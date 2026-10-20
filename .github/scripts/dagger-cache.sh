#!/usr/bin/env bash
# Decides whether the cargo cache archive the Dagger module packed is saved.
#
# The archive is made and restored by the module (`cache-archive`, `cache-restore`) and moved by
# `actions/cache`. This script only enforces the size limit in between.
#
# Usage:
#   dagger-cache.sh guard <archive> <max-bytes>    writes `save=true|false` to $GITHUB_OUTPUT
#
# Invoked through `bash` rather than executed: the repository is authored on Windows, where git
# does not record the executable bit.

set -euo pipefail

die() {
  echo "::error::$*" >&2
  exit 1
}

human() {
  numfmt --to=iec-i --suffix=B "$1"
}

cmd_guard() {
  [ "$#" -eq 2 ] || die "usage: guard <archive> <max-bytes>"
  local archive="$1" limit="$2" size save=true

  [ -f "${archive}" ] || die "archive ${archive} does not exist"
  size="$(stat -c %s "${archive}")"
  echo "Cargo cache archive: $(human "${size}")."

  # Every saved archive counts against the repository's 10 GB cache quota. An oversized entry
  # would evict the others; refuse it instead.
  if [ "${size}" -gt "${limit}" ]; then
    echo "::warning::Cargo cache archive is $(human "${size}"), over the $(human "${limit}") limit; not saving it."
    save=false
  fi
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "save=${save}" >> "${GITHUB_OUTPUT}"
  fi
}

main() {
  [ "$#" -ge 1 ] || die "usage: dagger-cache.sh guard <archive> <max-bytes>"
  local command="$1"
  shift
  case "${command}" in
    guard) cmd_guard "$@" ;;
    *) die "unknown command: ${command}" ;;
  esac
}

main "$@"
