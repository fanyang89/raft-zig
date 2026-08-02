#!/usr/bin/env bash
set -euo pipefail

bridge=$1
fencer=$2
fencer_client=$3
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/libelection-vip-smoke.XXXXXX")
base_port=$((20000 + $$ % 20000))
raft_port=$base_port
http_port=$((base_port + 1))
cluster_id=00112233445566778899aabbccddeeff
bridge_pid=
fencer_pid=

cleanup() {
    if [[ -n "$bridge_pid" ]]; then
        kill "$bridge_pid" 2>/dev/null || true
        wait "$bridge_pid" 2>/dev/null || true
    fi
    if [[ -n "$fencer_pid" ]]; then
        kill "$fencer_pid" 2>/dev/null || true
        wait "$fencer_pid" 2>/dev/null || true
    fi
    rm -rf "$work_dir"
}
trap cleanup EXIT

"$bridge" \
    1 \
    "$cluster_id" \
    "127.0.0.1:$raft_port" \
    "127.0.0.1:$http_port" \
    "$work_dir/fencer.sock" \
    "$work_dir/node" \
    "1=127.0.0.1:$raft_port" \
    >"$work_dir/bridge.log" 2>&1 &
bridge_pid=$!

for _ in {1..100}; do
    code=$(curl --silent --output /dev/null --write-out '%{http_code}' \
        "http://127.0.0.1:$http_port/healthz" || true)
    [[ "$code" == 200 ]] && break
    sleep 0.05
done
if [[ "${code:-}" != 200 ]]; then
    printf 'bridge did not become healthy\n' >&2
    exit 1
fi

sleep 3
code=$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "http://127.0.0.1:$http_port/leader" || true)
if [[ "$code" != 503 ]]; then
    printf 'expected leader endpoint to fail closed without fencer, got %s\n' "$code" >&2
    exit 1
fi

"$fencer" --dry-run \
    "$work_dir/fencer.state" \
    "$work_dir/fencer.sock" \
    1=dry1 \
    2=dry2 \
    >"$work_dir/fencer.log" 2>&1 &
fencer_pid=$!

for _ in {1..100}; do
    code=$(curl --silent --output /dev/null --write-out '%{http_code}' \
        "http://127.0.0.1:$http_port/leader" || true)
    [[ "$code" == 200 ]] && break
    sleep 0.05
done
if [[ "${code:-}" != 200 ]]; then
    printf 'bridge did not expose leadership after fencing grant\n' >&2
    exit 1
fi

status=$(curl --silent "http://127.0.0.1:$http_port/status")
if [[ "$status" != *'"leader_active":true'* ||
      "$status" != *'"fencing_granted":true'* ]]; then
    printf 'unexpected bridge status: %s\n' "$status" >&2
    exit 1
fi

read -r persisted_cluster persisted_term persisted_owner persisted_interface \
    <"$work_dir/fencer.state"
if [[ "$persisted_cluster" != "$cluster_id" || "$persisted_owner" != 1 ||
      "$persisted_term" -lt 1 || "$persisted_interface" != dry1 ]]; then
    printf 'unexpected fencing state: %s %s %s %s\n' \
        "$persisted_cluster" "$persisted_term" "$persisted_owner" \
        "$persisted_interface" >&2
    exit 1
fi

response=$("$fencer_client" \
    "$work_dir/fencer.sock" "$cluster_id" "$persisted_term" 1)
if [[ "$response" != "GRANTED $persisted_term 1" ]]; then
    printf 'idempotent fencing request failed: %s\n' "$response" >&2
    exit 1
fi

response=$("$fencer_client" \
    "$work_dir/fencer.sock" "$cluster_id" "$persisted_term" 2)
if [[ "$response" != "REJECTED term-owner-conflict" ]]; then
    printf 'same-term owner conflict was not rejected: %s\n' "$response" >&2
    exit 1
fi

next_term=$((persisted_term + 1))
response=$("$fencer_client" \
    "$work_dir/fencer.sock" "$cluster_id" "$next_term" 2)
if [[ "$response" != "GRANTED $next_term 2" ]]; then
    printf 'higher-term owner transition failed: %s\n' "$response" >&2
    exit 1
fi

response=$("$fencer_client" \
    "$work_dir/fencer.sock" "$cluster_id" "$persisted_term" 1)
if [[ "$response" != "REJECTED stale-term" ]]; then
    printf 'stale fencing request was not rejected: %s\n' "$response" >&2
    exit 1
fi

kill "$fencer_pid"
wait "$fencer_pid"
fencer_pid=
"$fencer" --dry-run \
    "$work_dir/fencer.state" \
    "$work_dir/fencer.sock" \
    1=dry1 \
    2=changed \
    >"$work_dir/fencer-restart.log" 2>&1 &
fencer_pid=$!
for _ in {1..100}; do
    ! kill -0 "$fencer_pid" 2>/dev/null && break
    sleep 0.05
done
if kill -0 "$fencer_pid" 2>/dev/null; then
    printf 'fencer started with a drifted persisted owner interface\n' >&2
    exit 1
fi
if wait "$fencer_pid"; then
    printf 'fencer accepted a drifted persisted owner interface\n' >&2
    exit 1
fi
fencer_pid=

"$fencer" --dry-run \
    "$work_dir/fencer.state" \
    "$work_dir/fencer.sock" \
    1=dry1 \
    2=dry2 \
    >"$work_dir/fencer-restart-valid.log" 2>&1 &
fencer_pid=$!
for _ in {1..100}; do
    [[ -S "$work_dir/fencer.sock" ]] && break
    sleep 0.05
done
response=$("$fencer_client" \
    "$work_dir/fencer.sock" "$cluster_id" "$next_term" 2)
if [[ "$response" != "GRANTED $next_term 2" ]]; then
    printf 'persisted fencing state did not restart idempotently: %s\n' \
        "$response" >&2
    exit 1
fi

if "$fencer" --dry-run \
    "$work_dir/duplicate.state" \
    "$work_dir/duplicate.sock" \
    1=duplicate 2=duplicate \
    >"$work_dir/duplicate.log" 2>&1; then
    printf 'duplicate fencing interfaces were accepted\n' >&2
    exit 1
fi
