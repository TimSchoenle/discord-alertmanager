#!/usr/bin/env bash
#
# Emits this repository's half of the README render payload as strict JSON on stdout.
#
# It is the `extra` input of the `readme-variables` action in docs.yml, deep-merged over what that
# action derives. Two kinds of fact arrive this way.
#
# The first kind the action cannot see. It reads one manifest, and this is a virtual workspace:
# `crates/discord-alertmanager/Cargo.toml` names the package and its description, while the
# licence, the MSRV and the edition live in the root's `[workspace.package]` where ten other
# crates inherit them. Pointing the action at the root instead is not an option — a virtual
# manifest has no `[package]` table and the action refuses it by name.
#
# The second kind is this repository's own generated surface: the configuration loader's
# variables, the count of keys behind them, and the crate map. All of it is lifted out of files
# that are themselves generated and committed — `docs/config.md` comes from `cargo xtask
# config-docs` — so nothing here parses Rust or runs a build.
#
# The one number in two places is the version. `crates/discord-alertmanager/Cargo.toml` states it
# literally because the action refuses a manifest that inherits it, and the root states it for the
# crates that do inherit. `require_version_agreement` below is what stops that from becoming
# drift: the render fails rather than advertising a tag the workspace never built.
#
# Run it yourself to see what CI will render with:
#
#     bash .github/scripts/readme-variables.sh
#
# Deliberately POSIX tools only, no `jq`: it is not present in a default Git for Windows shell,
# and a script that only runs on the CI runner is a script nobody checks their edit against.
set -euo pipefail

readonly WORKSPACE_MANIFEST="Cargo.toml"
readonly PACKAGE_MANIFEST="crates/discord-alertmanager/Cargo.toml"
readonly TOOLCHAIN_FILE="rust-toolchain.toml"
readonly CONFIG_REFERENCE="docs/config.md"

# The header lines of the two tables in the reference, matched in full. `cargo xtask config-docs`
# renders both from terrace-config's schema formatter, so a column added there is a column this
# script has to be told about rather than one it should quietly render half of.
readonly LOADER_HEADER='| Variable | Role | Default | Purpose |'
readonly KEYS_HEADER='| TOML | Type | Environment | Default | Flags | Purpose |'

# The keys an operator has to supply before the process will start, in the order a reader meets
# them. Only the names are authored: every cell beside them is lifted out of the generated
# reference, so a renamed key fails this script instead of silently rendering a stale row.
readonly REQUIRED_KEYS="
discord.token
alertmanager.endpoints
storage.backend
ingest.bind
ingest.webhook_token
"

die() {
    echo "readme-variables: $*" >&2
    exit 1
}

# Reads a `key = "value"` from one TOML table, scanning line by line rather than parsing.
#
# Anchoring to the table header is what makes the shallow read safe: `version` appears in almost
# every inline table under `[workspace.dependencies]`, and none of those lines start with it.
# Stops at the first match, so a trailing comment is discarded with no second pass.
table_field() {
    local file="$1" table="$2" key="$3" value

    value="$(
        awk -v want="[${table}]" -v key="${key}" '
            { line = $0; gsub(/^[ \t]+|[ \t]+$/, "", line) }
            line ~ /^\[/ { current = line; next }
            current != want { next }
            index(line, key) != 1 { next }
            {
                rest = substr(line, length(key) + 1)
                if (rest !~ /^[ \t]*=/) next
                sub(/^[ \t]*=[ \t]*/, "", rest)
                if (rest !~ /^"/) next
                rest = substr(rest, 2)
                quote = index(rest, "\"")
                if (quote == 0) next
                print substr(rest, 1, quote - 1)
                exit
            }
        ' "${file}"
    )"

    [ -n "${value}" ] || die "no '${key}' in [${table}] of ${file}"
    printf '%s' "${value}"
}

# Reads a field and rejects anything the accepted alphabet does not cover.
#
# The constraint is what lets the `printf` at the bottom emit these without a JSON encoder: a
# version, an SPDX identifier and an edition are all drawn from a small alphabet, and a value
# outside it means the manifest changed shape rather than that the value needs escaping.
checked_field() {
    local file="$1" table="$2" key="$3" pattern="$4" value

    value="$(table_field "${file}" "${table}" "${key}")"

    printf '%s' "${value}" | grep -Eq "${pattern}" ||
        die "'${key} = \"${value}\"' in ${file} is not the shape a README can quote"

    printf '%s' "${value}"
}

# Escapes a string for a JSON document.
#
# Only the two characters JSON reserves inside a string, plus a refusal for anything carrying a
# control character. A crate description is prose written by hand, so it is the one input here
# whose alphabet is not constrained in advance — and prose that has picked up a tab or a newline
# is a manifest to fix rather than a string to encode.
json_string() {
    local value="$1"

    printf '%s' "${value}" | LC_ALL=C grep -q '[[:cntrl:]]' &&
        die "a control character in '${value}'"

    printf '%s' "${value}" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

# Fails when the two manifests state different versions.
#
# The whole reason the version is written twice is that the action cannot read an inherited one,
# and a duplicate nobody checks is the drift this generator exists to prevent. Checked here rather
# than in a workflow step so that running the script by hand catches it too.
require_version_agreement() {
    local workspace="$1" package="$2"

    [ "${workspace}" = "${package}" ] || die "$(
        printf '%s states version %s, %s states %s. They have to agree: the action reads the second and the crates inherit the first.' \
            "${WORKSPACE_MANIFEST}" "${workspace}" "${PACKAGE_MANIFEST}" "${package}"
    )"
}

# The body rows of the table one header line opens.
#
# Anchored to the header rather than to a position in the file. The reference grew an introduction
# the day it needed a title, and a range that started at line one silently produced nothing; a
# header that stops matching fails the caller instead, which is the failure worth having.
table_rows() {
    local header="$1"

    awk -v header="${header}" '
        !started { if ($0 == header) started = 1; next }
        # The |---|---| rule between the header and the first row.
        started == 1 { started = 2; next }
        /^\|/ { print; next }
        { exit }
    ' "${CONFIG_REFERENCE}"
}

# The reference row for one configuration key, by its TOML path.
key_row() {
    local key="$1" row

    row="$(grep -F -m1 "| \`${key}\` |" "${CONFIG_REFERENCE}" || true)"

    [ -n "${row}" ] || die "no row for '${key}' in ${CONFIG_REFERENCE}; was the key renamed?"

    printf '%s' "${row}"
}

# Emits a JSON array of one-line strings, one per line of stdin.
json_array_of_lines() {
    local first=1 line

    printf '['
    while IFS= read -r line; do
        [ -n "${line}" ] || continue
        [ "${first}" = 1 ] || printf ','
        first=0
        printf '"%s"' "$(json_string "${line}")"
    done
    printf ']'
}

# Emits the crate map as a JSON array, one object per member under `crates/`.
#
# Built from the glob rather than from a list, for the same reason the action walks `docs/`: a
# crate added in one pull request and added to the README's table in the next is a crate the map
# lies about in between. The library name is the second column because `dam_core` and
# `discord-alertmanager-core` are the same crate, and a reader meeting an import first has no way
# to know that.
crate_map() {
    local first=1 manifest name purpose library

    printf '['
    for manifest in crates/*/Cargo.toml; do
        name="$(table_field "${manifest}" "package" "name")"
        purpose="$(table_field "${manifest}" "package" "description")"
        # Absent for the binary, which is the composition root and is imported by nothing.
        library="$(table_field "${manifest}" "lib" "name" 2>/dev/null || true)"

        [ "${first}" = 1 ] || printf ','
        first=0
        printf '{"name":"%s","library":"%s","purpose":"%s"}' \
            "$(json_string "${name}")" \
            "$(json_string "${library}")" \
            "$(json_string "${purpose}")"
    done
    printf ']'
}

for required in "${WORKSPACE_MANIFEST}" "${PACKAGE_MANIFEST}" "${TOOLCHAIN_FILE}" "${CONFIG_REFERENCE}"; do
    [ -f "${required}" ] || die "${required} is missing; run this from the repository root"
done

# --- the facts the action reads from a manifest this workspace does not have ------------------
version="$(checked_field "${WORKSPACE_MANIFEST}" "workspace.package" "version" '^[0-9]+(\.[0-9]+){2}([.+-][0-9A-Za-z.+-]*)?$')"
license="$(checked_field "${WORKSPACE_MANIFEST}" "workspace.package" "license" '^[0-9A-Za-z.+-]+$')"
msrv="$(checked_field "${WORKSPACE_MANIFEST}" "workspace.package" "rust-version" '^[0-9]+(\.[0-9]+){0,2}$')"
edition="$(checked_field "${WORKSPACE_MANIFEST}" "workspace.package" "edition" '^[0-9]{4}$')"
channel="$(checked_field "${TOOLCHAIN_FILE}" "toolchain" "channel" '^[0-9]+(\.[0-9]+){0,2}$')"

# Assigned before it is passed, never substituted into the argument list. `set -e` aborts on a
# failed assignment; a substitution that fails inside an argument leaves the caller's own exit
# status, so the check would run against an empty string and report the wrong problem.
package_version="$(checked_field "${PACKAGE_MANIFEST}" "package" "version" '^[0-9]+(\.[0-9]+){2}([.+-][0-9A-Za-z.+-]*)?$')"
require_version_agreement "${version}" "${package_version}"

# --- this repository's own generated surface ---------------------------------------------------
loader_rows="$(table_rows "${LOADER_HEADER}")"
key_rows="$(table_rows "${KEYS_HEADER}")"

[ -n "${loader_rows}" ] || die "no rows under '${LOADER_HEADER}' in ${CONFIG_REFERENCE}"
[ -n "${key_rows}" ] || die "no rows under '${KEYS_HEADER}' in ${CONFIG_REFERENCE}"

loader="$(json_array_of_lines <<< "${loader_rows}")"
required_rows="$(for key in ${REQUIRED_KEYS}; do key_row "${key}"; echo; done | json_array_of_lines)"
crates="$(crate_map)"
keys="$(wc -l <<< "${key_rows}" | tr -d ' ')"

# `repo` and `toolchain` are objects, so they merge key by key with what the action derived. A
# string here would replace the whole object and take `repo.slug`, `repo.url` and
# `repo.description` with it.
printf '{"repo":{"license":"%s"},"toolchain":{"msrv":"%s","edition":"%s","channel":"%s"},"config":{"keyCount":%s,"loaderRows":%s,"requiredRows":%s},"crates":%s}\n' \
    "${license}" \
    "${msrv}" \
    "${edition}" \
    "${channel}" \
    "${keys}" \
    "${loader}" \
    "${required_rows}" \
    "${crates}"
