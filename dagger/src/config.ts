/**
 * Pinned versions, image layout, and the pure helpers that derive values from repository files.
 *
 * Nothing here talks to the engine, so every function is deterministic in its input.
 */

/*
 * Every pinned version below is updated by Renovate through the custom managers in
 * `renovate.json`. Images are written `name:tag@sha256:digest`, which is the form those managers
 * recognise; a tag without a digest is invisible to them. The Rust builder images are not here:
 * their tag is read from `rust-toolchain.toml`, which Renovate already updates.
 */

/** A release asset fetched by URL, pinned to its tag and verified against its SHA-256. */
export interface ReleaseAsset {
  readonly version: string
  readonly sha256: string
}

/**
 * `cargo-chef`, as the static musl binary its release publishes for each architecture, so no
 * builder compiles it. The two entries move together: Renovate bumps the tag and recomputes each
 * digest in one pull request, and `cargoChef` refuses a mismatched pair.
 */
export const CARGO_CHEF: Readonly<Record<"amd64" | "arm64", ReleaseAsset>> = {
  // renovate: datasource=github-release-attachments depName=LukeMathWalker/cargo-chef
  amd64: {
    version: "v0.1.78",
    sha256: "aca691abfbfbbe00d482e0ed2249eec3091b65e96a0ce92947fb5b254d48b16d",
  },
  // renovate: datasource=github-release-attachments depName=LukeMathWalker/cargo-chef
  arm64: {
    version: "v0.1.78",
    sha256: "cd59b90fce5fa84c4189648fec33404ec5491791eebdb812df5e7fca8336408d",
  },
}

/**
 * Supplies `/etc/passwd`, `/etc/group`, the CA bundle and zoneinfo to the `scratch` runtime.
 * The same reference as the Dockerfile's `env` stage, so both produce the same runtime files.
 */
export const ALPINE_IMAGE =
  "alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6"

/** Docker daemon the PostgreSQL conformance test starts its throwaway container on. */
export const DIND_IMAGE =
  "docker:29.9.1-dind@sha256:3e5756e2b6dad6a5f1f5deacf0a19f9eaa2581b76ae092b2d4a118162e1d4d4e"

/** The workspace's binary package and the binary it produces. */
export const PACKAGE = "discord-alertmanager"
export const BINARY = "discord-alertmanager"

/** Runtime identity and layout, identical to the Dockerfile's global build arguments. */
export const USER_ID = 1001
export const GROUP_ID = 1001
export const EXECUTION_DIRECTORY = "/app"
export const INGEST_PORT = 9099

/** Where the configuration contract lives inside the image. The generator's label names it too. */
export const CONTRACT_PATH = "/config/contract.json"

/**
 * Reads the pinned toolchain out of `rust-toolchain.toml`.
 *
 * The builder image tag is derived from this, so a Renovate bump of the toolchain file moves the
 * pipeline with it and there is no second version to keep in step.
 *
 * @throws Error when no channel is declared or the channel is not an exact `X.Y.Z` release; a
 *   floating channel such as `stable` has no matching image tag and would make builds drift.
 */
export function parseToolchainChannel(toml: string): string {
  const match = /^\s*channel\s*=\s*"([^"]+)"/m.exec(toml)
  const channel = match?.[1]
  if (channel === undefined) {
    throw new Error("rust-toolchain.toml declares no [toolchain] channel")
  }
  if (!/^\d+\.\d+\.\d+$/.test(channel)) {
    throw new Error(`rust-toolchain.toml must pin an exact release (X.Y.Z), found "${channel}"`)
  }
  return channel
}

/**
 * Parses the generator's `--format labels` output: one `key=value` per line.
 *
 * Split on the first `=` only, because a value may contain one.
 *
 * @throws Error on a non-empty line without a key, so a format change in the generator fails the
 *   build instead of producing an image with missing labels.
 */
export function parseLabels(text: string): ReadonlyArray<readonly [string, string]> {
  const labels: Array<readonly [string, string]> = []
  for (const raw of text.split("\n")) {
    const line = raw.trim()
    if (line === "") {
      continue
    }
    const eq = line.indexOf("=")
    if (eq <= 0) {
      throw new Error(`malformed contract label line: "${line}"`)
    }
    labels.push([line.slice(0, eq), line.slice(eq + 1)])
  }
  if (labels.length === 0) {
    throw new Error("the contract generator emitted no labels")
  }
  return labels
}

/**
 * The registry host an image reference pushes to, which is what registry auth is keyed by.
 *
 * Follows the Docker reference rule: the first path segment is a registry only when it contains
 * a `.` or a `:`, or is `localhost`. Otherwise the reference is a Docker Hub repository.
 */
export function registryHost(address: string): string {
  const first = address.split("/")[0] ?? ""
  const hasPath = address.includes("/")
  if (hasPath && (first.includes(".") || first.includes(":") || first === "localhost")) {
    return first
  }
  return "docker.io"
}
