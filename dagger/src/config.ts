/**
 * Pinned versions, image layout, and the pure helpers that derive values from repository files.
 *
 * Nothing here talks to the engine, so every function is deterministic in its input.
 */

/** `cargo-chef`, installed into the builder images. The Dockerfile takes it from a prebuilt image. */
export const CARGO_CHEF_VERSION = "0.1.78"

/**
 * Supplies `/etc/passwd`, `/etc/group`, the CA bundle and zoneinfo to the `scratch` runtime.
 * The same reference as the Dockerfile's `env` stage, so both produce the same runtime files.
 */
export const ALPINE_IMAGE =
  "alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6"

/** Docker daemon the PostgreSQL conformance test starts its throwaway container on. */
export const DIND_IMAGE = "docker:29.9.1-dind"

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
