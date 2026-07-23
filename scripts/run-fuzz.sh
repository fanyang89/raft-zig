#!/usr/bin/env bash
set -uo pipefail

target=${1:?fuzz target is required}
runs=${2:?fuzz run count is required}

rm -f .zig-cache/f/crash
zig build "$target" -Doptimize=ReleaseSafe --fuzz="$runs" --summary line
status=$?

if [[ -f .zig-cache/f/crash ]]; then
    exit 1
fi
exit "$status"
