#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    printf 'run this demo as root\n' >&2
    exit 1
fi
if [[ $# -ne 3 ]]; then
    printf 'usage: %s ELECTION_VIP_BRIDGE ELECTION_VIP_FENCER VIP_MANAGER\n' "$0" >&2
    exit 2
fi

bridge=$(realpath "$1")
fencer=$(realpath "$2")
vip_manager=$(realpath "$3")
for command in ip curl; do
    command -v "$command" >/dev/null || {
        printf 'missing command: %s\n' "$command" >&2
        exit 1
    }
done

printf -v suffix '%04x%04x' "$RANDOM" "$RANDOM"
control_bridge="lvc${suffix}"
service_bridge="lvs${suffix}"
client_namespace="lv${suffix}cl"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/libelection-vip-demo.XXXXXX")
cluster_id=00112233445566778899aabbccddeeff
vip=10.220.0.100
declare -a namespaces created_namespaces control_host service_host bridge_pids manager_pids
fencer_pid=
control_bridge_created=false
service_bridge_created=false

cleanup() {
    set +e
    for pid in "${bridge_pids[@]:-}" "${manager_pids[@]:-}"; do
        [[ -n "$pid" ]] && kill "$pid" 2>/dev/null
    done
    [[ -n "$fencer_pid" ]] && kill "$fencer_pid" 2>/dev/null
    for pid in "${bridge_pids[@]:-}" "${manager_pids[@]:-}"; do
        [[ -n "$pid" ]] && wait "$pid" 2>/dev/null
    done
    [[ -n "$fencer_pid" ]] && wait "$fencer_pid" 2>/dev/null
    for namespace in "${created_namespaces[@]:-}"; do
        [[ -n "$namespace" ]] && ip netns del "$namespace" 2>/dev/null
    done
    $control_bridge_created && ip link del "$control_bridge" 2>/dev/null
    $service_bridge_created && ip link del "$service_bridge" 2>/dev/null
    rm -rf "$work_dir"
}
trap cleanup EXIT

ip link add "$control_bridge" type bridge
control_bridge_created=true
ip link add "$service_bridge" type bridge
service_bridge_created=true
ip link set "$control_bridge" up
ip link set "$service_bridge" up

for node in 1 2 3; do
    namespace="lv${suffix}n${node}"
    control="lv${suffix}c${node}"
    service="lv${suffix}s${node}"
    ip netns add "$namespace"
    created_namespaces+=("$namespace")
    namespaces[$node]=$namespace
    control_host[$node]=$control
    service_host[$node]=$service
    ip link add "$control" type veth peer name raft0 netns "$namespace"
    ip link set "$control" master "$control_bridge"
    ip link set "$control" up
    ip -n "$namespace" link set lo up
    ip -n "$namespace" link set raft0 up
    ip -n "$namespace" address add "10.210.0.$((10 + node))/24" dev raft0

    ip link add "$service" type veth peer name svc0 netns "$namespace"
    ip link set "$service" master "$service_bridge"
    ip link set "$service" up
    ip -n "$namespace" link set svc0 up
    ip -n "$namespace" address add "10.220.0.$((10 + node))/24" dev svc0
done

ip netns add "$client_namespace"
created_namespaces+=("$client_namespace")
client_host="lv${suffix}sc"
ip link add "$client_host" type veth peer name eth0 netns "$client_namespace"
ip link set "$client_host" master "$service_bridge"
ip link set "$client_host" up
ip -n "$client_namespace" link set lo up
ip -n "$client_namespace" link set eth0 up
ip -n "$client_namespace" address add 10.220.0.50/24 dev eth0

"$fencer" \
    "$work_dir/fencer.state" \
    "$work_dir/fencer.sock" \
    "1=${service_host[1]}" \
    "2=${service_host[2]}" \
    "3=${service_host[3]}" \
    >"$work_dir/fencer.log" 2>&1 &
fencer_pid=$!
for _ in {1..100}; do
    kill -0 "$fencer_pid" 2>/dev/null || break
    [[ -S "$work_dir/fencer.sock" ]] && break
    sleep 0.05
done
if [[ ! -S "$work_dir/fencer.sock" ]]; then
    printf 'fencer did not start\n' >&2
    exit 1
fi

for node in 1 2 3; do
    ip netns exec "${namespaces[$node]}" "$vip_manager" \
        --ip "$vip" \
        --netmask 24 \
        --interface svc0 \
        --trigger-key /leader \
        --trigger-value 200 \
        --dcs-type patroni \
        --dcs-endpoints http://127.0.0.1:8008 \
        --interval 100 \
        --manager-type basic \
        >"$work_dir/vip-manager-$node.log" 2>&1 &
    manager_pids[$node]=$!
    kill -0 "${manager_pids[$node]}"
done

peers=(
    1=10.210.0.11:7101
    2=10.210.0.12:7102
    3=10.210.0.13:7103
)
for node in 1 2 3; do
    ip netns exec "${namespaces[$node]}" "$bridge" \
        "$node" \
        "$cluster_id" \
        "10.210.0.$((10 + node)):$((7100 + node))" \
        127.0.0.1:8008 \
        "$work_dir/fencer.sock" \
        "$work_dir/node-$node" \
        "${peers[@]}" \
        >"$work_dir/bridge-$node.log" 2>&1 &
    bridge_pids[$node]=$!
done

for node in 1 2 3; do
    code=
    for _ in {1..100}; do
        kill -0 "${bridge_pids[$node]}" 2>/dev/null || break
        kill -0 "${manager_pids[$node]}" 2>/dev/null || break
        code=$(ip netns exec "${namespaces[$node]}" \
            curl --noproxy '*' --silent --output /dev/null \
            --write-out '%{http_code}' http://127.0.0.1:8008/healthz || true)
        [[ "$code" == 200 ]] && break
        sleep 0.05
    done
    if [[ "$code" != 200 ]]; then
        printf 'node %s did not become healthy\n' "$node" >&2
        exit 1
    fi
done

vip_owner() {
    local found=0
    local node
    for node in 1 2 3; do
        if [[ -n "$(ip -n "${namespaces[$node]}" -o address show dev svc0 to "$vip/32")" ]]; then
            if [[ $found -ne 0 ]]; then
                printf 'multiple'
                return
            fi
            found=$node
        fi
    done
    printf '%s' "$found"
}

all_processes_alive() {
    kill -0 "$fencer_pid" 2>/dev/null || return 1
    local node
    for node in 1 2 3; do
        kill -0 "${bridge_pids[$node]}" 2>/dev/null || return 1
        kill -0 "${manager_pids[$node]}" 2>/dev/null || return 1
    done
}

wait_for_owner() {
    local excluded=${1:-0}
    local owner
    for _ in {1..300}; do
        all_processes_alive || return 1
        owner=$(vip_owner)
        if [[ "$owner" != 0 && "$owner" != multiple && "$owner" != "$excluded" ]]; then
            printf '%s' "$owner"
            return
        fi
        sleep 0.1
    done
    return 1
}

initial_owner=$(wait_for_owner) || {
    printf 'VIP did not acquire an initial owner\n' >&2
    exit 1
}
ip netns exec "$client_namespace" ping -c 1 -W 1 "$vip" >/dev/null
printf 'initial VIP owner: node %s\n' "$initial_owner"

ip link set "${control_host[$initial_owner]}" down
new_owner=$(wait_for_owner "$initial_owner") || {
    printf 'VIP did not fail over\n' >&2
    exit 1
}
if [[ "$(<"/sys/class/net/${service_host[$initial_owner]}/operstate")" != down ]]; then
    printf 'previous owner service link was not fenced\n' >&2
    exit 1
fi

for _ in {1..100}; do
    all_processes_alive || {
        printf 'a demo process exited during failover\n' >&2
        exit 1
    }
    [[ "$(vip_owner)" == "$new_owner" ]] && break
    sleep 0.1
done
if [[ "$(vip_owner)" != "$new_owner" ]]; then
    printf 'stale VIP remained configured after failover\n' >&2
    exit 1
fi

ip -n "$client_namespace" neighbour flush dev eth0
ip netns exec "$client_namespace" ping -c 1 -W 1 "$vip" >/dev/null
new_mac=$(ip netns exec "${namespaces[$new_owner]}" \
    sh -c 'cat /sys/class/net/svc0/address')
neighbour=$(ip -n "$client_namespace" neighbour show "$vip" dev eth0)
if [[ "$neighbour" != *"lladdr $new_mac"* ]]; then
    printf 'client neighbor entry did not move to new owner: %s\n' "$neighbour" >&2
    exit 1
fi

read -r persisted_cluster persisted_term persisted_owner persisted_interface \
    <"$work_dir/fencer.state"
if [[ "$persisted_cluster" != "$cluster_id" ||
      "$persisted_owner" != "$new_owner" ||
      "$persisted_interface" != "${service_host[$new_owner]}" ]]; then
    printf 'fencer persisted the wrong owner\n' >&2
    exit 1
fi
if ! all_processes_alive; then
    printf 'a demo process exited before verification completed\n' >&2
    exit 1
fi

printf 'failover VIP owner: node %s, term %s\n' "$new_owner" "$persisted_term"
printf 'three-node fenced VIP demo passed\n'
