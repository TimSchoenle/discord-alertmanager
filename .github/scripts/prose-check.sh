#!/usr/bin/env bash
# The mechanical half of the prose and doc-comment standards in TimSchoenle/actions:
#
#   https://github.com/TimSchoenle/actions/blob/main/docs/readme/PROSE.md#checks
#   https://github.com/TimSchoenle/actions/blob/main/docs/doc-comments/GUIDE.md#checks
#
# Three budgets, each copied from those documents: no doc comment opens with a banned opener, a
# Markdown document carries at most five em dashes outside tables, code and quotes, and at most
# three consecutive bullets share the `**Bold** —` frame. Everything else in those standards is a
# review criterion and is not checked here.
#
# Usage: bash .github/scripts/prose-check.sh [markdown file...]   (default: README.md)
# The `readme` job in docs.yml runs the same command.
set -euo pipefail

cd "$(dirname "$0")/../.."

readonly EM_DASH_BUDGET=5
readonly BOLD_FRAME_BUDGET=3

failed=0

# GUIDE.md's opener check, in POSIX ERE so it needs nothing the runner image lacks. GUIDE.md's
# `rg` spelling also matches a bare ` * ` line, which is how a Java or TypeScript doc block continues;
# in Rust that line can sit inside an ordinary `/* */` comment, so the Rust forms are named instead:
# `///`, `//!`, and the opening line of a `/** */` or `/*! */` block. Like the `rg` spelling it skips
# `target/`, `dist/` and `node_modules/`.
opener='^[[:space:]]*(///|//!|/\*\*|/\*!)[[:space:]]*(This (function|method|struct|class|enum|trait|module|field)|Used to |Helper (for|that)|Responsible for|A utility|Note that|In other words|Under the hood|Simply |Basically )'
status=0
openers="$(grep -rnE "$opener" --include='*.rs' \
    --exclude-dir=target --exclude-dir=dist --exclude-dir=node_modules --exclude-dir=.git .)" \
    || status=$?
case "$status" in
    0)
        echo "error: doc comments open with a phrase GUIDE.md bans (budget 0):" >&2
        echo "$openers" >&2
        failed=1
        ;;
    1) ;; # No match, which is the pass.
    *)
        echo "error: the opener search itself failed (grep exit $status), so nothing was checked" >&2
        exit 2
        ;;
esac

# PROSE.md's counts, with two departures. The numbered-list filter is spelled `[0-9][0-9]*`
# because BSD sed, which macOS ships, does not read `\+` in a basic expression. The bullet run
# carries across a bullet's indented continuation lines, which PROSE.md's one-liner treats as the
# end of the run, so a run of wrapped bullets was never counted past one.
# The backticks below are literal Markdown, not a shell expansion.
# shellcheck disable=SC2016
em_dashes() {
    awk '/^```/{f=!f; next} f{next} /<!--/{c=1} c{if (/-->/) c=0; next} 1' "$1" \
        | sed -e '/^|/d' -e '/^>/d' -e '/^[0-9][0-9]*\. /d' -e 's/`[^`]*`//g' \
        | { grep -o '—' || true; } | wc -l
}

bold_frame_run() {
    awk '/^- \*\*.*\*\* —/{r++; if (r>m) m=r; next} /^[[:space:]][[:space:]]+[^[:space:]]/{next} {r=0} END{print m+0}' "$1"
}

if [ "$#" -eq 0 ]; then
    set -- README.md
fi

for file in "$@"; do
    dashes="$(em_dashes "$file")"
    if [ "$dashes" -gt "$EM_DASH_BUDGET" ]; then
        echo "error: $file carries $dashes em dashes outside tables, code and quotes (budget $EM_DASH_BUDGET)" >&2
        failed=1
    fi
    run="$(bold_frame_run "$file")"
    if [ "$run" -gt "$BOLD_FRAME_BUDGET" ]; then
        echo "error: $file has $run consecutive '**Bold** —' bullets (budget $BOLD_FRAME_BUDGET)" >&2
        failed=1
    fi
done

exit "$failed"
