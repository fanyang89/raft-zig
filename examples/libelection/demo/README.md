# Fenced VIP composition

This demo composes three processes:

```text
libelection -> election-vip-bridge -> vip-manager -> Linux VIP and GARP
                       |
                       v
              election-vip-fencer
```

The bridge drives `libelection` in external mode on a dedicated thread. Raft
callbacks only update local generation state. A separate worker requests a
fencing grant over a Unix sequenced-packet socket. `/leader` returns HTTP 200
only while all of these conditions hold:

- The node has active leadership after a current-term commit.
- The external driver heartbeat is fresh.
- The fencer granted the same node and Raft term.
- `election_node_get_status` still reports that term as active.

## Fencer

Start the fencer outside the candidate node network namespaces:

```sh
election-vip-fencer STATE_FILE SOCKET_PATH \
  1=NODE1_HOST_VETH 2=NODE2_HOST_VETH 3=NODE3_HOST_VETH
```

`STATE_FILE` and `SOCKET_PATH` must be in directories owned by the fencer user
and not writable by group or other users. The state file records the cluster
ID, highest granted term, owner node ID, and exact fenced interface. The fencer
holds an advisory state lock until exit.

For a higher-term owner transition, the fencer performs these operations:

1. Disable and verify the persisted previous-owner interface.
2. Atomically persist and sync the new term, owner, and interface.
3. Enable and verify the new-owner interface.
4. Return the grant.

Repeating the same term and owner is idempotent. A lower term, a different owner
in the same term, duplicate interface mappings, and changes to the current
owner's persisted interface are rejected. If persistence or link manipulation
fails, no grant is returned.

## Bridge

Start one bridge in each candidate node:

```sh
election-vip-bridge \
  NODE_ID CLUSTER_ID RAFT_LISTEN HTTP_LISTEN FENCER_SOCKET DATA_DIR \
  PEER_ID=ADDRESS...
```

The HTTP listener accepts IPv4 addresses such as `127.0.0.1:8008`. Configure
vip-manager v5 with:

```text
--dcs-type patroni
--dcs-endpoints http://127.0.0.1:8008
--trigger-key /leader
--trigger-value 200
```

The bridge should run without network-administration privileges. vip-manager
needs the privileges required to add the VIP and send ARP. The fencer needs
permission to change the host-side service links.

## Security boundary

The included fencer is a Linux network-namespace demonstration, not a general
production STONITH service. Its mode-0600 Unix socket trusts clients running as
the fencer user. A compromised client with that identity can submit an
arbitrary owner or term. NetworkManager or an administrator can also violate
the invariant by manually re-enabling a fenced interface.

A production deployment must put fencing in an independent management failure
domain, authenticate each requester, protect fencing state from loss or
rollback, and replace the veth operation with a switch-port, hypervisor NIC,
Redfish/IPMI, or cloud-network action. The grant must still be returned only
after the old owner's data path is confirmed isolated.
