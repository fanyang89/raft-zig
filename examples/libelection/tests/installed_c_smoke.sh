#!/usr/bin/env bash
set -euo pipefail

prefix=$1
source=$2
cc=${CC:-cc}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/libelection-consumer.XXXXXX")
cleanup() {
    rm -rf "$work_dir"
}
trap cleanup EXIT

common_flags=(
    -std=c11
    -Wall
    -Wextra
    -Werror
    -I"$prefix/include"
    "$source"
)

"$cc" "${common_flags[@]}" \
    "$prefix/lib/libelection.a" \
    -pthread \
    -ldl \
    -lrt \
    -lm \
    -lstdc++ \
    -o "$work_dir/static-smoke"
"$work_dir/static-smoke"

"$cc" "${common_flags[@]}" \
    -L"$prefix/lib" \
    -Wl,-rpath,"$prefix/lib" \
    -lelection \
    -pthread \
    -ldl \
    -lrt \
    -lm \
    -o "$work_dir/shared-smoke"
"$work_dir/shared-smoke"

symbol_count=0
while read -r _ _ symbol; do
    case "$symbol" in
        election_*@@LIBELECTION_1.0 | LIBELECTION_1.0) ;;
        *)
            printf 'unexpected shared-library export: %s\n' "$symbol" >&2
            exit 1
            ;;
    esac
    symbol_count=$((symbol_count + 1))
done < <(nm -D --defined-only "$prefix/lib/libelection.so.1.0.0")

if [[ $symbol_count -ne 10 ]]; then
    printf 'expected 10 shared-library exports, found %d\n' "$symbol_count" >&2
    exit 1
fi

if ! readelf --version-info "$prefix/lib/libelection.so.1.0.0" | grep 'Name: LIBELECTION_1.0' >/dev/null; then
    printf 'missing LIBELECTION_1.0 version definition\n' >&2
    exit 1
fi
