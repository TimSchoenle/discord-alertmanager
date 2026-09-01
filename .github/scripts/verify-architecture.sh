#!/usr/bin/env bash
#
# Asserts the dependency rule `discord-alertmanager-core` states about itself.
#
#     bash .github/scripts/verify-architecture.sh
#
# The crate documentation says the domain has four dependencies and does not know about Discord,
# and `deny.toml` says the other half of its layering rule is checked here. Both are prose until
# something reads the graph, and a graph nobody reads is how `tokio` ends up in a state machine
# that was supposed to be testable in milliseconds.
#
# Two assertions, because they fail in different ways.
#
# The direct set is exact — a fifth entry is a failure even if it is harmless, because the point
# of the rule is that adding one is a decision somebody makes on purpose. The ban list is about
# the transitive graph, where a dependency arrives through something else's manifest and nobody
# who could have objected sees it.
#
# `--edges normal` throughout. `proptest`, `rstest` and `serde_json` are dev-dependencies: they
# compile the tests and reach no consumer of this crate, so a rule that counted them would be a
# rule about the test harness.
#
# Deliberately POSIX tools only, no `jq`: it is not present in a default Git for Windows shell,
# and a script that only runs on the CI runner is a script nobody checks their edit against.
set -euo pipefail

readonly CRATE="discord-alertmanager-core"

# The manifest's `[dependencies]` table, as the crate documentation states it.
readonly ALLOWED_DIRECT="
chrono
regex
serde
thiserror
"

# Reached transitively or not at all. Each name is a layer the domain is not allowed to learn
# about: an async runtime, a Discord client, a database driver, an HTTP client and the server it
# would be spoken to through.
readonly BANNED="
tokio
serenity
sqlx
reqwest
axum
hyper
"

die() {
    echo "verify-architecture: $*" >&2
    exit 1
}

# Every crate `cargo tree` prints for one root, one name per line, deduplicated.
#
# `--prefix none` drops the box-drawing characters, leaving `name vX.Y.Z` and sometimes a trailing
# `(*)` where cargo elided a repeated subtree. The first field is the name; nothing else on the
# line is read.
tree_names() {
    local depth_args=()
    [ "$#" -eq 0 ] || depth_args=(--depth "$1")

    cargo tree \
        --package "${CRATE}" \
        --edges normal \
        --prefix none \
        "${depth_args[@]}" |
        awk 'NF { print $1 }' |
        sort -u
}

command -v cargo >/dev/null 2>&1 || die "cargo is not on PATH, so nothing was checked"

# The root prints itself as the first line at every depth, and it is not its own dependency.
direct="$(tree_names 1 | grep -vx "${CRATE}" || true)"
[ -n "${direct}" ] || die "'cargo tree -p ${CRATE} --depth 1' listed no dependencies at all; the invocation is wrong, not the manifest"

# Word splitting is the point: the list above is one string of newline-separated names,
# and each name has to reach `printf` as an argument of its own.
# shellcheck disable=SC2086
expected="$(printf '%s\n' ${ALLOWED_DIRECT} | sort -u)"

status=0

if ! diff_output="$(diff <(printf '%s\n' "${expected}") <(printf '%s\n' "${direct}"))"; then
    echo "error: the direct dependencies of ${CRATE} are not the four its documentation names." >&2
    echo "       '<' is expected and missing, '>' is present and undeclared:" >&2
    printf '%s\n' "${diff_output}" >&2
    echo "       Either revert the manifest, or change the rule in crates/${CRATE}/src/lib.rs," >&2
    echo "       crates/${CRATE}/Cargo.toml and this script together." >&2
    status=1
fi

reachable="$(tree_names)"

for banned in ${BANNED}; do
    if printf '%s\n' "${reachable}" | grep -qx "${banned}"; then
        echo "error: ${CRATE} reaches '${banned}'. The domain crate performs no I/O and knows" >&2
        echo "       nothing about Discord; 'cargo tree -p ${CRATE} --edges normal -i ${banned}'" >&2
        echo "       names the edge that brought it in." >&2
        status=1
    fi
done

if [ "${status}" -eq 0 ]; then
    # shellcheck disable=SC2086 # word splitting, as above
    printf '%s: %s direct dependencies, and none of the %s banned crates is reachable.\n' \
        "${CRATE}" \
        "$(printf '%s\n' "${direct}" | wc -l | tr -d ' ')" \
        "$(printf '%s\n' ${BANNED} | wc -l | tr -d ' ')"
fi

exit "${status}"
