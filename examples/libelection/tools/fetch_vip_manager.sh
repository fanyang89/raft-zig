#!/usr/bin/env bash
set -euo pipefail

version=5.0.0
destination=${1:-zig-out/vip-manager}

case $(uname -m) in
    x86_64)
        architecture=x86_64
        checksum=0f9d118d2c1e6a7e561406749be008a9a8fca40a59dfdc53897685b8da06d2ef
        ;;
    aarch64 | arm64)
        architecture=arm64
        checksum=396165193e3278d53e43be75f70474500d5c2a1c954ac1b316bd8c137272b68b
        ;;
    *)
        printf 'unsupported architecture: %s\n' "$(uname -m)" >&2
        exit 1
        ;;
esac

archive_name="vip-manager_${version}_Linux_${architecture}.tar.gz"
url="https://github.com/cybertec-postgresql/vip-manager/releases/download/v${version}/${archive_name}"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/vip-manager-download.XXXXXX")
cleanup() {
    rm -rf "$work_dir"
}
trap cleanup EXIT

curl --fail --location --silent --show-error \
    --output "$work_dir/$archive_name" "$url"
printf '%s  %s\n' "$checksum" "$work_dir/$archive_name" | sha256sum --check --status
mkdir -p "$destination"
tar -xf "$work_dir/$archive_name" -C "$destination" --strip-components=1
printf '%s\n' "$destination/vip-manager"
