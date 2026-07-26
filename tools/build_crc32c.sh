#!/usr/bin/env bash
set -euo pipefail

source_dir=$1
build_dir=$2
build_type=$3
cc=$4
zig_exe=$5
target_triple=$6
cxx_wrapper=$7
sanitize_thread=$8
sanitize_c=$9

if [[ "$sanitize_thread" == true && "$sanitize_c" == true ]]; then
    printf 'ThreadSanitizer and UndefinedBehaviorSanitizer are mutually exclusive\n' >&2
    exit 1
fi

c_flags=()
if [[ "$sanitize_thread" == true ]]; then
    c_flags+=(-fsanitize=thread)
fi
if [[ "$sanitize_c" == true ]]; then
    c_flags+=(-fsanitize=undefined -fno-sanitize-recover=undefined)
else
    c_flags+=(-fno-sanitize=undefined)
fi
if [[ "$sanitize_thread" == true || "$sanitize_c" == true ]]; then
    c_flags+=(-fno-omit-frame-pointer -g)
fi

CC="$cc" CXX="$cxx_wrapper $zig_exe $target_triple" cmake \
    -S "$source_dir" \
    -B "$build_dir" \
    -G Ninja \
    "-DCMAKE_BUILD_TYPE=$build_type" \
    "-DCMAKE_C_FLAGS=${c_flags[*]}" \
    "-DCMAKE_CXX_FLAGS=${c_flags[*]}" \
    -DBUILD_SHARED_LIBS=OFF \
    -DCRC32C_BUILD_TESTS=OFF \
    -DCRC32C_BUILD_BENCHMARKS=OFF \
    -DCRC32C_USE_GLOG=OFF \
    -DCRC32C_INSTALL=OFF
cmake --build "$build_dir" --target crc32c
