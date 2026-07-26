#!/usr/bin/env bash
set -euo pipefail

zig_exe=$1
target_triple=$2
shift 2

args=()
for arg in "$@"; do
    if [[ "$arg" == -march=armv8-a+crc+crypto ]]; then
        args+=(-mcpu=generic+crc+crypto)
    else
        args+=("$arg")
    fi
done

exec "$zig_exe" c++ -target "$target_triple" "${args[@]}"
