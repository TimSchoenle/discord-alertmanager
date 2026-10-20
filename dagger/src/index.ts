/**
 * Dagger pipeline for discord-alertmanager.
 *
 * Reproduces what CI and the Dockerfile do today, as typed functions that run identically on a
 * laptop and on a runner:
 *
 *   dagger call test                          # cargo test --workspace --all-features
 *   dagger call build                         # stripped static musl release binary
 *   dagger call contract                      # contract.json + contract.labels
 *   dagger call docker                        # the runtime image (FROM scratch)
 *   dagger call smoke-test                    # the image starts and carries its contract
 *   dagger call publish --address=…           # push one or more platforms as one index
 *   dagger call cache-archive export --path=…  # the cargo caches as one tar.zst
 *   dagger call cache-restore --archive=…      # and back into a fresh engine
 *
 * Two kinds of caching are in play. Dagger caches every step by its inputs, so an unchanged
 * step is skipped outright. Inside a step that does run, cargo reuses three cache volumes:
 * `CARGO_HOME` (registry index, crate sources, git checkouts) and one target directory per
 * toolchain/libc/architecture. Both survive between calls on the same engine. CI starts every
 * run on a fresh engine and carries only the cargo volumes across, see `.github/workflows/ci.yml`.
 */

import {
  argument,
  CacheSharingMode,
  CacheVolume,
  Container,
  dag,
  Directory,
  File,
  func,
  object,
  Platform,
  ReturnType,
  Secret,
  Service,
} from "@dagger.io/dagger"

import {
  ALPINE_IMAGE,
  BINARY,
  CARGO_CHEF_VERSION,
  CONTRACT_PATH,
  DIND_IMAGE,
  EXECUTION_DIRECTORY,
  GROUP_ID,
  INGEST_PORT,
  PACKAGE,
  USER_ID,
  parseLabels,
  parseToolchainChannel,
  registryHost,
} from "./config"
import { targetFor, type TargetSpec } from "./platform"

/**
 * What the build reads, and nothing else. Everything is excluded, then the workspace is
 * re-included, so a new top-level file cannot invalidate the build cache, and a developer's
 * `config.toml` (which holds the bot token) can never reach a container.
 */
const SOURCE_FILTER: string[] = [
  "*",
  "!Cargo.toml",
  "!Cargo.lock",
  "!rust-toolchain.toml",
  "!.cargo",
  "!crates",
  "!xtask",
  "**/target",
  "**/*.snap.new",
]

/** `CARGO_HOME` on a cache volume, not the image's `/usr/local/cargo`. See `rustEnv`. */
const CARGO_HOME = "/cargo-home"
const TARGET_DIR = `${EXECUTION_DIRECTORY}/target`
const RECIPE = "/tmp/recipe.json"

/** Where `cacheArchive` and `cacheRestore` mount the cargo caches, one subdirectory each. */
const CACHE_ROOT = "/cache"
const CACHE_NAMES = ["cargo-home", "target-gnu", "target-musl"] as const

/** The two builder flavours: glibc for tests (as CI runs them), musl for the static release. */
type Libc = "gnu" | "musl"

@object()
export class DiscordAlertmanager {
  /** The filtered Cargo workspace every function builds from. */
  @func()
  source: Directory

  constructor(
    /** Repository root. Defaults to the directory holding `dagger.json`. */
    @argument({ defaultPath: "/", ignore: SOURCE_FILTER })
    source: Directory,
  ) {
    this.source = source
  }

  /**
   * Runs the workspace test suite with every feature, as the `Test` job in build.yaml does.
   *
   * The PostgreSQL conformance test starts its own database through testcontainers, so a Docker
   * daemon runs as a sidecar service and `DOCKER_HOST` points at it. The step is cached by its
   * inputs: re-running against an unchanged tree returns the previous output without testing.
   *
   * @returns cargo's output.
   */
  @func()
  async test(): Promise<string> {
    const env = await this.preparedEnv("gnu", await dag.defaultPlatform(), [
      "--all-targets",
      "--all-features",
    ])
    return env
      .withServiceBinding("docker", dockerDaemon())
      .withEnvVariable("DOCKER_HOST", "tcp://docker:2375")
      .withExec(["cargo", "test", "--workspace", "--all-features", "--locked"])
      .stdout()
  }

  /**
   * Builds the release binary: static musl, stripped, and UPX-packed where the target allows it.
   *
   * Dependencies are compiled first from a cargo-chef recipe; the recipe changes only when a
   * manifest or the lockfile does, so on a source-only change that step is skipped by Dagger and
   * only the workspace crates compile.
   *
   * @param platform Target platform. Defaults to the engine's own, so no emulation is involved.
   */
  @func()
  async build(platform?: Platform): Promise<File> {
    const resolved = platform ?? (await dag.defaultPlatform())
    const spec = targetFor(resolved)
    const env = await this.preparedEnv("musl", resolved, ["--release", "--target", spec.muslTriple])

    // Build and copy out in one exec. The target directory is a shared cache volume, and its
    // content between two execs is not guaranteed to be what the first one left there.
    const built = env.withExec([
      "sh",
      "-euc",
      'cargo build --release --locked --target "$1" -p "$2"\n' +
        'mkdir -p /out\n' +
        `cp "${TARGET_DIR}/$1/release/$3" "/out/$3"`,
      "sh",
      spec.muslTriple,
      PACKAGE,
      BINARY,
    ])

    const stripped = built.withExec(["strip", "--strip-all", `/out/${BINARY}`])
    const packed = spec.upx
      ? stripped.withExec(["upx", "--best", "--lzma", `/out/${BINARY}`])
      : stripped
    return packed.file(`/out/${BINARY}`)
  }

  /**
   * Generates the configuration contract and the image labels that describe it.
   *
   * Both files come out of one generator run on one source tree, which is the guarantee the
   * Dockerfile's `contract-builder` stage gives. Runs in the test environment, whose target
   * volume already holds the `config-schema` build from `test`.
   *
   * @param appVersion Release recorded in the document's `app.version`. Omitted, the field is
   *   absent, which keeps the output equal to the committed `docs/config.contract.json`.
   */
  @func()
  async contract(appVersion?: string): Promise<Directory> {
    const env = await this.preparedEnv("gnu", await dag.defaultPlatform(), [
      "--all-targets",
      "--all-features",
    ])
    const versionArgs =
      appVersion === undefined || appVersion === "" ? [] : ["--version", appVersion]
    const generate = (format: string): string[] => [
      "cargo",
      "run",
      "--quiet",
      "--locked",
      "-p",
      "discord-alertmanager-config",
      "--features",
      "config-schema",
      "--example",
      "config-schema",
      "--",
      "--format",
      format,
      ...versionArgs,
    ]
    return env
      .withExec(["mkdir", "-p", "/out"])
      .withExec(generate("contract"), { redirectStdout: "/out/contract.json" })
      .withExec(generate("labels"), { redirectStdout: "/out/contract.labels" })
      .directory("/out")
  }

  /**
   * Assembles the runtime image, equivalent to the Dockerfile's `runtime` target.
   *
   * `FROM scratch` with the static binary, the CA bundle, zoneinfo, a non-root user and the
   * contract. Labels are applied from the generator's output at build time rather than pasted
   * into a file, so they cannot drift from the document in the image.
   *
   * @param platform Target platform. Defaults to the engine's own.
   * @param appVersion Value of the `version` label, and the contract's `app.version` when set.
   * @param release Value of the `release` label.
   */
  @func()
  async docker(
    platform?: Platform,
    appVersion?: string,
    release = "unreleased",
  ): Promise<Container> {
    const resolved = platform ?? (await dag.defaultPlatform())
    const [binary, contract] = await Promise.all([this.build(resolved), this.contract(appVersion)])
    const labels = parseLabels(await contract.file("contract.labels").contents())

    const env = dag
      .container({ platform: resolved })
      .from(ALPINE_IMAGE)
      .withExec(["apk", "add", "--no-cache", "ca-certificates", "tzdata"])
      .withExec(["update-ca-certificates"])
      .withExec([
        "adduser",
        "--disabled-password",
        "--gecos",
        "",
        "--home",
        "/nonexistent",
        "--shell",
        "/sbin/nologin",
        "--no-create-home",
        "--uid",
        String(USER_ID),
        "appuser",
      ])

    let image = dag
      .container({ platform: resolved })
      .withLabel("version", appVersion ?? "unknown")
      .withLabel("release", release)

    for (const [key, value] of labels) {
      image = image.withLabel(key, value)
    }

    return image
      .withFile("/etc/passwd", env.file("/etc/passwd"))
      .withFile("/etc/group", env.file("/etc/group"))
      .withFile(
        "/etc/ssl/certs/ca-certificates.crt",
        env.file("/etc/ssl/certs/ca-certificates.crt"),
      )
      .withDirectory("/usr/share/zoneinfo", env.directory("/usr/share/zoneinfo"))
      .withFile(CONTRACT_PATH, contract.file("contract.json"))
      .withWorkdir(EXECUTION_DIRECTORY)
      .withFile(`${EXECUTION_DIRECTORY}/app`, binary, { permissions: 0o555 })
      .withUser(`${USER_ID}:${GROUP_ID}`)
      .withExposedPort(INGEST_PORT)
      .withEntrypoint(["./app"])
  }

  /**
   * Checks the image the way the `Docker Build and Test` job does, without a Docker daemon.
   *
   * 1. The binary starts: with no token or database it exits non-zero, but only after logging
   *    `starting version=`. A UPX stub that cannot run on this kernel prints nothing at all.
   * 2. The contract label names a path, and the file at that path is the generated document.
   *
   * @returns the binary's output.
   */
  @func()
  async smokeTest(platform?: Platform): Promise<string> {
    const image = await this.docker(platform, "smoke-test", "smoke-test")

    const [output, contractPath, contract] = await Promise.all([
      // `expect: ANY` because a non-zero exit is the expected outcome; the log line is the check.
      image.withExec(["./app"], { expect: ReturnType.Any }).combinedOutput(),
      image.label("dev.terrace.config.contract.path"),
      this.contract("smoke-test").then((dir) => dir.file("contract.json").contents()),
    ])

    if (!output.includes("starting version=")) {
      throw new Error(`the binary did not reach startup logging; output:\n${output}`)
    }
    if (contractPath === "") {
      throw new Error("the image carries no dev.terrace.config.contract.path label")
    }
    const inImage = await image.file(contractPath).contents()
    if (inImage !== contract) {
      throw new Error(`${contractPath} in the image is not the document the generator produced`)
    }
    return output
  }

  /**
   * Publishes the image for one or more platforms as a single multi-platform index.
   *
   * A platform other than the engine's own is built under emulation, which for a Rust release
   * build is an order of magnitude slower; the release workflow builds each architecture on a
   * native runner for that reason.
   *
   * @param address Image reference including the tag, e.g. `ghcr.io/owner/repo:1.2.3`.
   * @param platforms Platforms to include. Defaults to the engine's own.
   * @param username Registry user. Omit with `password` to push anonymously (e.g. to ttl.sh).
   * @param password Registry password or token.
   * @param appVersion See `docker`.
   * @param release See `docker`.
   * @returns the pushed reference, pinned to its digest.
   */
  @func()
  async publish(
    address: string,
    platforms: Platform[] = [],
    username?: string,
    password?: Secret,
    appVersion?: string,
    release = "unreleased",
  ): Promise<string> {
    if ((username === undefined) !== (password === undefined)) {
      throw new Error("username and password must be given together")
    }
    const targets = platforms.length > 0 ? platforms : [await dag.defaultPlatform()]
    const variants = await Promise.all(targets.map((p) => this.docker(p, appVersion, release)))

    let publisher = dag.container()
    if (username !== undefined && password !== undefined) {
      publisher = publisher.withRegistryAuth(registryHost(address), username, password)
    }
    return publisher.publish(address, { platformVariants: variants })
  }

  /**
   * Packs what cargo can reuse into one `tar.zst`, for CI to carry to the next run.
   *
   * Only the cargo caches go in: `CARGO_HOME` and the two target directories of the engine's
   * platform. Base images and Dagger's own step cache stay out; a fresh runner downloads the
   * images in seconds, and they were most of the engine state. Unpacked crate sources and git
   * working copies are left out too, since cargo recreates both from what is kept.
   *
   * Never cached: the result depends on the volumes' contents, which are not an input.
   */
  @func({ cache: "never" })
  async cacheArchive(): Promise<File> {
    const archive = "/out/cargo-cache.tar.zst"
    return (await this.cacheMounts())
      .withExec(["mkdir", "-p", "/out"])
      .withExec([
        "tar",
        "--create",
        "--file",
        archive,
        "--use-compress-program",
        "zstd -3 -T0",
        "--directory",
        CACHE_ROOT,
        "--exclude=./cargo-home/registry/src",
        "--exclude=./cargo-home/git/checkouts",
        "--exclude=./cargo-home/.package-cache*",
        ".",
      ])
      .file(archive)
  }

  /**
   * Unpacks an archive from `cacheArchive` into the cargo cache volumes.
   *
   * Run once on a fresh engine, before anything that builds. Files in the archive overwrite
   * those already in the volumes; nothing else is removed.
   *
   * @param archive The `tar.zst` produced by `cacheArchive`.
   * @returns the size of each restored cache.
   */
  @func({ cache: "never" })
  async cacheRestore(archive: File): Promise<string> {
    return (await this.cacheMounts())
      .withMountedFile("/in/cargo-cache.tar.zst", archive)
      .withExec([
        "tar",
        "--extract",
        "--file",
        "/in/cargo-cache.tar.zst",
        "--use-compress-program",
        "zstd -d -T0",
        "--directory",
        CACHE_ROOT,
      ])
      .withExec(["du", "-sh", ...CACHE_NAMES.map((name) => `${CACHE_ROOT}/${name}`)])
      .stdout()
  }

  /** The pinned toolchain from `rust-toolchain.toml`; names images and target volumes. */
  private async toolchainChannel(): Promise<string> {
    return parseToolchainChannel(await this.source.file("rust-toolchain.toml").contents())
  }

  /**
   * A small container with the cargo cache volumes mounted under `CACHE_ROOT`, for archiving.
   *
   * The volumes are the same objects `rustEnv` mounts, for the engine's own platform, which is
   * the one CI builds.
   */
  private async cacheMounts(): Promise<Container> {
    const channel = await this.toolchainChannel()
    const { arch } = targetFor(await dag.defaultPlatform())
    return (
      dag
        .container()
        .from(ALPINE_IMAGE)
        .withExec(["apk", "add", "--no-cache", "tar", "zstd"])
        // An archive or a restore must run every time it is asked for, never from a cached step.
        .withEnvVariable("CACHE_BUSTER", new Date().toISOString())
        .withMountedCache(`${CACHE_ROOT}/cargo-home`, cargoHomeVolume(), {
          sharing: CacheSharingMode.Shared,
        })
        .withMountedCache(`${CACHE_ROOT}/target-gnu`, targetVolume(channel, "gnu", arch), {
          sharing: CacheSharingMode.Shared,
        })
        .withMountedCache(`${CACHE_ROOT}/target-musl`, targetVolume(channel, "musl", arch), {
          sharing: CacheSharingMode.Shared,
        })
    )
  }

  /**
   * A builder container with the toolchain, the caches mounted, and dependencies precompiled.
   *
   * @param cookFlags Flags for `cargo chef cook`; they must select the same profile, target and
   *   features as the build that follows, or the precompiled artefacts are not the ones it uses.
   */
  private async preparedEnv(libc: Libc, platform: Platform, cookFlags: string[]): Promise<Container> {
    const env = await this.rustEnv(libc, platform)

    // The recipe is a function of the manifests and the lockfile only, so its content, and with
    // it the cook step's cache key, stays the same across source-only changes.
    const recipe = env
      .withDirectory(EXECUTION_DIRECTORY, this.source)
      .withExec(["cargo", "chef", "prepare", "--recipe-path", RECIPE])
      .file(RECIPE)

    return (
      env
        .withFile(RECIPE, recipe)
        .withExec(["cargo", "chef", "cook", "--locked", ...cookFlags, "--recipe-path", RECIPE])
        .withDirectory(EXECUTION_DIRECTORY, this.source)
        // cargo decides whether a workspace crate is stale by mtime. The target directory
        // outlives this checkout, so a file restored with an older mtime (a branch switch, an
        // archive) could be taken as already built. Touching the members makes them always
        // rebuild; dependencies, which dominate the build time, are unaffected. In CI every
        // checkout is newer than the cache anyway, so this costs nothing there.
        .withExec([
          "find",
          `${EXECUTION_DIRECTORY}/crates`,
          `${EXECUTION_DIRECTORY}/xtask`,
          "-type",
          "f",
          "-exec",
          "touch",
          "{}",
          "+",
        ])
    )
  }

  /**
   * The bare toolchain container for one libc and platform, with caches mounted.
   *
   * `CARGO_HOME` is a single volume instead of separate `registry` and `git` mounts. cargo
   * serialises access to those directories through a lock file in `CARGO_HOME` itself; with
   * only the subdirectories shared, each container has a private lock file and two concurrent
   * cargo processes corrupt the shared checkout. That is the `failed to clone into … no error`
   * the Dockerfile's `contract-builder` stage works around with separate cache ids.
   */
  private async rustEnv(libc: Libc, platform: Platform): Promise<Container> {
    const channel = await this.toolchainChannel()
    const spec = targetFor(platform)
    const image = libc === "musl" ? `rust:${channel}-alpine` : `rust:${channel}-trixie`

    let base = dag.container({ platform }).from(image)
    if (libc === "musl") {
      base = base.withExec(["apk", "add", "--no-cache", ...muslPackages(spec)])
    } else {
      // No debug info in the test build. It is most of a debug target directory, which is what
      // CI archives, and a failing test still reports its file and line: the panic location is
      // compiled in independently of debug info.
      base = base
        .withEnvVariable("CARGO_PROFILE_DEV_DEBUG", "0")
        .withEnvVariable("CARGO_PROFILE_TEST_DEBUG", "0")
    }

    return (
      base
        // Before the cache mounts, so the binary lands in an image layer that is cached once per
        // toolchain rather than on a volume shared with builds.
        .withExec([
          "cargo",
          "install",
          "--locked",
          "--root",
          "/usr/local",
          "--version",
          CARGO_CHEF_VERSION,
          "cargo-chef",
        ])
        // The image's toolchain, and not whatever `rust-toolchain.toml` would make rustup fetch:
        // the file also lists `rustfmt` and `clippy`, which a build does not need, and rustup
        // would download them on every cold cache.
        .withEnvVariable("RUSTUP_TOOLCHAIN", channel)
        .withEnvVariable("CARGO_HOME", CARGO_HOME)
        .withEnvVariable("CARGO_TARGET_DIR", TARGET_DIR)
        // Incremental artefacts are useless in CI and roughly double the target directory, which
        // is what CI has to archive and restore.
        .withEnvVariable("CARGO_INCREMENTAL", "0")
        .withEnvVariable("CARGO_NET_RETRY", "10")
        .withMountedCache(CARGO_HOME, cargoHomeVolume(), { sharing: CacheSharingMode.Shared })
        .withMountedCache(TARGET_DIR, targetVolume(channel, libc, spec.arch), {
          sharing: CacheSharingMode.Shared,
        })
        .withWorkdir(EXECUTION_DIRECTORY)
    )
  }
}

/** Alpine packages the musl builder needs on top of the official Rust image. */
function muslPackages(spec: TargetSpec): string[] {
  // `musl-dev` for the C code in `ring`, `binutils` for `strip`. No OpenSSL: the workspace is
  // rustls-only and deny.toml bans `openssl-sys`.
  const packages = ["musl-dev", "binutils"]
  return spec.upx ? [...packages, "upx"] : packages
}

/** Cache volumes are global to the engine, so every name carries the project prefix. */
function cacheVolume(name: string): CacheVolume {
  return dag.cacheVolume(`discord-alertmanager-${name}`)
}

/** Registry index, downloaded crates and git dependencies, shared by every build. */
function cargoHomeVolume(): CacheVolume {
  return cacheVolume("cargo-home")
}

/** One target directory per toolchain, libc and architecture: their artefacts never overlap. */
function targetVolume(channel: string, libc: Libc, arch: string): CacheVolume {
  return cacheVolume(`target-${channel}-${libc}-${arch}`)
}

/**
 * A Docker daemon for testcontainers.
 *
 * `/var/lib/docker` is a cache volume so the PostgreSQL image is pulled once rather than on every
 * run. `PRIVATE` sharing, because two daemons on one data root corrupt it: a second concurrent
 * daemon gets an empty volume of its own instead.
 */
function dockerDaemon(): Service {
  return dag
    .container()
    .from(DIND_IMAGE)
    .withEnvVariable("DOCKER_TLS_CERTDIR", "")
    .withMountedCache("/var/lib/docker", cacheVolume("dind-data"), {
      sharing: CacheSharingMode.Private,
    })
    .withExposedPort(2375)
    .asService({
      args: ["dockerd", "--host=tcp://0.0.0.0:2375", "--tls=false"],
      useEntrypoint: true,
      insecureRootCapabilities: true,
    })
}
