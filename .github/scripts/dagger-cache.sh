#!/usr/bin/env bash
# Measures the Dagger pipeline in CI and decides whether its cargo cache archive is saved.
#
# The archive itself is made and restored by the module (`cache-archive`, `cache-restore`) and
# moved by `actions/cache`; this script only times the phases around them, records sizes, and
# enforces the size limit before a save.
#
# Usage:
#   dagger-cache.sh mark <phase>                   end of a timed phase
#   dagger-cache.sh size <name> <file>             record a file's size for the report
#   dagger-cache.sh guard <archive> <max-bytes>    `save=true|false` to $GITHUB_OUTPUT
#   dagger-cache.sh report                         timings and sizes to the job summary
#
# Invoked through `bash` rather than executed: the repository is authored on Windows, where git
# does not record the executable bit.

set -euo pipefail

readonly METRICS="${RUNNER_TEMP:?RUNNER_TEMP is not set}/dagger-metrics.tsv"

die() {
  echo "::error::$*" >&2
  exit 1
}

# Appends one `kind<TAB>name<TAB>value` row. `kind` is `mark` (epoch ms) or `size` (bytes).
record() {
  printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "${METRICS}"
}

human() {
  numfmt --to=iec-i --suffix=B "$1"
}

cmd_mark() {
  [ "$#" -eq 1 ] || die "usage: mark <phase>"
  record mark "$1" "$(date +%s%3N)"
}

cmd_size() {
  [ "$#" -eq 2 ] || die "usage: size <name> <file>"
  record size "$1" "$(stat -c %s "$2")"
}

cmd_guard() {
  [ "$#" -eq 2 ] || die "usage: guard <archive> <max-bytes>"
  local archive="$1" limit="$2" size save=true

  size="$(stat -c %s "${archive}")"
  record size saved-archive "${size}"

  # Every saved archive counts against the repository's 10 GB cache quota, which build.yaml's
  # buildx cache shares. An oversized entry would evict the others; refuse it instead.
  if [ "${size}" -gt "${limit}" ]; then
    echo "::warning::Cargo cache archive is $(human "${size}"), over the $(human "${limit}") limit; not saving it."
    save=false
  fi
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "save=${save}" >> "${GITHUB_OUTPUT}"
  fi
}

cmd_report() {
  [ "$#" -eq 0 ] || die "usage: report"
  [ -f "${METRICS}" ] || die "no metrics recorded"

  {
    echo "### Dagger pipeline"
    echo
    echo "| Phase | Duration |"
    echo "| --- | ---: |"
    # Each mark closes the phase it names; its duration is the distance to the previous mark.
    awk -F '\t' '
      $1 == "mark" {
        if (prev != "") { printf "| %s | %.1f s |\n", $2, ($3 - prev) / 1000 }
        prev = $3
      }' "${METRICS}"
    echo
    echo "| Measure | Size |"
    echo "| --- | ---: |"
    awk -F '\t' '$1 == "size" { print $2 "\t" $3 }' "${METRICS}" |
      while IFS=$'\t' read -r name bytes; do
        printf '| %s | %s |\n' "${name}" "$(human "${bytes}")"
      done
  } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
}

main() {
  [ "$#" -ge 1 ] || die "usage: dagger-cache.sh <mark|size|guard|report> ..."
  local command="$1"
  shift
  case "${command}" in
    mark) cmd_mark "$@" ;;
    size) cmd_size "$@" ;;
    guard) cmd_guard "$@" ;;
    report) cmd_report "$@" ;;
    *) die "unknown command: ${command}" ;;
  esac
}

main "$@"
