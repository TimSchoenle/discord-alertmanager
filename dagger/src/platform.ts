/**
 * Per-architecture build parameters, the module's counterpart of the `chef-amd64` and
 * `chef-arm64` stages in the Dockerfile.
 *
 * A lookup table rather than string surgery on the platform, so an unsupported platform fails
 * before any container starts instead of producing an artefact for the wrong target.
 */

import type { Platform } from "@dagger.io/dagger"

/** Everything that differs between the architectures the image ships for. */
export interface TargetSpec {
  /** Short name used in cache volume names, matching the Dockerfile's `TARGETARCH`. */
  readonly arch: "amd64" | "arm64"
  /** The musl triple the release binary is built for. Always the host triple of the builder. */
  readonly muslTriple: string
  /**
   * Whether the binary is packed with UPX. Off on arm64: the UPX stub assumes a 4 KiB page size,
   * so a packed arm64 binary fails to start on 64 KiB-page kernels.
   */
  readonly upx: boolean
}

const TARGETS: Readonly<Record<string, TargetSpec>> = {
  "linux/amd64": { arch: "amd64", muslTriple: "x86_64-unknown-linux-musl", upx: true },
  "linux/arm64": { arch: "arm64", muslTriple: "aarch64-unknown-linux-musl", upx: false },
}

/**
 * Resolves the build parameters for a platform.
 *
 * The variant is dropped before the lookup: an arm64 engine reports `linux/arm64/v8`, which is
 * the same target as `linux/arm64`.
 *
 * @throws Error when the platform is not one the image is built for.
 */
export function targetFor(platform: Platform): TargetSpec {
  const [os, arch] = String(platform).split("/")
  const key = `${os}/${arch}`
  const spec = TARGETS[key]
  if (spec === undefined) {
    throw new Error(
      `unsupported platform "${platform}"; supported: ${Object.keys(TARGETS).join(", ")}`,
    )
  }
  return spec
}
