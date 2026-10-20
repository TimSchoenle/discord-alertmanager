#!/usr/bin/env bash
# Runs the Dagger engine on a GitHub-hosted runner with its state directory persisted through
# `actions/cache`, and measures what that persistence costs.
#
# The engine keeps everything it caches (image layers, step results, cache volumes) under
# `/var/lib/dagger`. On an ephemeral runner that directory starts empty, so the engine is started
# here with it bind-mounted from the host, restored from an archive before the pipeline and
# archived again after the engine has stopped. `actions/cache` moves the archive; this script
# never talks to the cache service.
#
# The archive is made with `sudo tar`, not by pointing `actions/cache` at the directory: the state
# is root-owned, holds device nodes (overlayfs whiteouts) and `trusted.*` xattrs (overlayfs opaque
# directories), and `actions/cache` runs tar as the runner user, which can read none of it.
#
# Usage:
#   dagger-engine.sh mark <phase>                         end of a timed phase
#   dagger-engine.sh size <name> <file>                   record a file's size for the report
#   dagger-engine.sh restore <archive> <state-dir>        extract a restored archive, if any
#   dagger-engine.sh start <state-dir> <name> <image> <engine.json>
#   dagger-engine.sh prune                                 GC down to the configured budget
#   dagger-engine.sh stop <name>                           flush and stop the engine
#   dagger-engine.sh archive <state-dir> <archive> <max-archive-bytes>
#   dagger-engine.sh report                                timings and sizes to the job summary
#
# Invoked through `bash` rather than executed: the repository is authored on Windows, where git
# does not record the executable bit.

set -euo pipefail

readonly METRICS="${RUNNER_TEMP:?RUNNER_TEMP is not set}/dagger-engine-metrics.tsv"

die() {
  echo "::error::$*" >&2
  exit 1
}

now_ms() {
  date +%s%3N
}

# Appends one `kind<TAB>name<TAB>value` row. `kind` is `mark` (epoch ms) or `size` (bytes).
record() {
  printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "${METRICS}"
}

# Echoes a `key=value` line to a `$GITHUB_*` file when running under Actions, so the script
# stays runnable by hand.
github_file() {
  local file="$1" line="$2"
  if [ -n "${file}" ]; then
    printf '%s\n' "${line}" >> "${file}"
  fi
}

cmd_mark() {
  [ "$#" -eq 1 ] || die "usage: mark <phase>"
  record mark "$1" "$(now_ms)"
}

cmd_size() {
  [ "$#" -eq 2 ] || die "usage: size <name> <file>"
  record size "$1" "$(stat -c %s "$2")"
}

cmd_restore() {
  [ "$#" -eq 2 ] || die "usage: restore <archive> <state-dir>"
  local archive="$1" state="$2"

  sudo mkdir -p "${state}"
  if [ ! -f "${archive}" ]; then
    echo "No archive restored; the engine starts cold."
    record size restored-archive 0
    return
  fi

  record size restored-archive "$(stat -c %s "${archive}")"
  sudo tar --use-compress-program='zstd -d -T0' \
    --extract --preserve-permissions --numeric-owner --xattrs --xattrs-include='*' \
    --file "${archive}" --directory "${state}"
  # The archive is now redundant and the runner's disk is not large.
  rm -f "${archive}"
  record size restored-state "$(sudo du -sb "${state}" | cut -f1)"
}

cmd_start() {
  [ "$#" -eq 4 ] || die "usage: start <state-dir> <name> <image> <engine.json>"
  local state="$1" name="$2" image="$3" config="$4"

  [ -f "${config}" ] || die "engine config ${config} does not exist"

  # `--privileged` is what the engine requires to run containers of its own.
  docker run --detach --privileged \
    --name "${name}" \
    --volume "${state}:/var/lib/dagger" \
    --volume "${config}:/etc/dagger/engine.json:ro" \
    "${image}" > /dev/null

  # The CLI connects to this container instead of provisioning one of its own.
  github_file "${GITHUB_ENV:-}" "_EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://${name}"
  echo "Engine ${name} started from ${image}."
}

cmd_prune() {
  [ "$#" -eq 0 ] || die "usage: prune"
  # Applies the `gc` budget from engine.json now rather than whenever the engine's own timer
  # fires. The archive is what is uploaded, so anything left over costs upload time on every run.
  # A failed prune only makes the archive larger, and the size guard in `archive` still applies.
  if ! dagger core engine local-cache prune --use-default-policy; then
    echo "::warning::Engine cache prune failed; archiving the state as it is."
  fi
}

cmd_stop() {
  [ "$#" -eq 1 ] || die "usage: stop <name>"
  local name="$1"
  # A graceful stop flushes the engine's metadata databases. Archiving a running engine would
  # capture them mid-write, and the next run would start from a corrupt cache.
  docker stop --time 300 "${name}" > /dev/null
  docker logs --tail 50 "${name}" > "${RUNNER_TEMP}/dagger-engine.log" 2>&1 || true
  docker rm "${name}" > /dev/null
  echo "Engine ${name} stopped."
}

cmd_archive() {
  [ "$#" -eq 3 ] || die "usage: archive <state-dir> <archive> <max-archive-bytes>"
  local state="$1" archive="$2" limit="$3"

  [ -d "${state}" ] || die "state directory ${state} does not exist"
  record size final-state "$(sudo du -sb "${state}" | cut -f1)"

  # Level 3: the archive is dominated by compiled artefacts, where higher levels buy a few percent
  # of size for several times the CPU time on a two-core runner.
  sudo tar --use-compress-program='zstd -3 -T0' \
    --create --numeric-owner --xattrs --xattrs-include='*' --sparse \
    --file "${archive}" --directory "${state}" .
  sudo chown "$(id -u):$(id -g)" "${archive}"

  local size
  size="$(stat -c %s "${archive}")"
  record size saved-archive "${size}"

  # Every saved archive counts against the repository's 10 GB cache quota, and an oversized one
  # evicts the others, including the buildx cache build.yaml relies on. Refuse rather than evict.
  if [ "${size}" -gt "${limit}" ]; then
    rm -f "${archive}"
    echo "::warning::Engine state archive is $(numfmt --to=iec-i --suffix=B "${size}"), over the" \
      "$(numfmt --to=iec-i --suffix=B "${limit}") limit; not saving it."
    github_file "${GITHUB_OUTPUT:-}" "save=false"
    return
  fi
  github_file "${GITHUB_OUTPUT:-}" "save=true"
}

cmd_report() {
  [ "$#" -eq 0 ] || die "usage: report"
  [ -f "${METRICS}" ] || die "no metrics recorded"

  {
    echo "### Dagger engine cache"
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
        printf '| %s | %s |\n' "${name}" "$(numfmt --to=iec-i --suffix=B "${bytes}")"
      done
  } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
}

main() {
  [ "$#" -ge 1 ] || die "usage: dagger-engine.sh <mark|size|restore|start|prune|stop|archive|report> ..."
  local command="$1"
  shift
  case "${command}" in
    mark) cmd_mark "$@" ;;
    size) cmd_size "$@" ;;
    restore) cmd_restore "$@" ;;
    start) cmd_start "$@" ;;
    prune) cmd_prune "$@" ;;
    stop) cmd_stop "$@" ;;
    archive) cmd_archive "$@" ;;
    report) cmd_report "$@" ;;
    *) die "unknown command: ${command}" ;;
  esac
}

main "$@"
