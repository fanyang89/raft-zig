# Membership

raft-zig supports Raft joint-consensus changes and an optional durable
membership record that binds node IDs to network addresses and a stable cluster
identity.

## Durable and Legacy Modes

Set `RaftorConfig.cluster_id` to a non-zero 16-byte value to enable durable
membership. Durable state records:

- cluster identity
- current peer IDs and advertised addresses
- configuration membership index
- retired node IDs
- snapshot-time membership

Leaving `cluster_id` null explicitly selects legacy ID-only mode. Legacy mode
does not persist the address-aware membership model and is intended only for
existing integrations that have not migrated.

## Bootstrap

Fresh storage with `join = false` selects bootstrap.

When `initial_peers` is empty, Raftor creates a one-node voter configuration
from `raft.id` and `advertise_addr`. It falls back to `listen_addr` only when
the advertised address is empty.

For a multi-node bootstrap, every `Peer` in `initial_peers` is an initial voter.
`Peer.context` must contain that peer's advertised address. IDs must be non-zero
and addresses must be present and unambiguous.

All bootstrap nodes must use the same cluster ID and identical initial peer
mapping. Seed configuration is an initialization input; the persisted
membership becomes authoritative afterward.

## Join

Fresh storage with `join = true` selects join. `initial_peers` contains seed
nodes and must not include the local node ID. A fresh joining node starts
outside the durable configuration and cannot campaign.

An existing member adds it through `addLearner` or `addNode`. The joining node
learns the durable configuration by replicated log entries or snapshot
installation. It becomes promotable only after applying a configuration that
includes its local ID as a voter. Merely appearing in a snapshot as a learner
does not make it promotable.

Join is only selected for empty storage. Existing HardState, ConfState, or log
entries always select restart, regardless of the current `join` flag.

## Dynamic Changes

Raftor exposes these high-level operations:

| Operation | Effect |
| --- | --- |
| `addNode(id, addr)` | Propose a voting member and durable address. |
| `addLearner(id, addr)` | Propose a non-voting learner. |
| `updateNodeAddress(id, addr)` | Change an existing durable endpoint. |
| `removeNode(id)` | Remove a member and retire its ID. |

Operations are proposals: returning success means the configuration change was
submitted, not committed. Joint transitions preserve quorum overlap when the
change requires it.

Retired IDs are rejected by later membership operations. Do not reuse a removed
node's ID for a replacement process; allocate a new ID.

## Transport Identity

Durable mode and `GrpcLiteTransport` use the same cluster ID and local node ID.
Raftor rejects a transport whose identity conflicts with configuration or
persisted membership. Streams validate the protocol version, cluster ID, and
source and target node IDs. Connecting to an address that serves the wrong node
is therefore rejected.

These checks detect configuration mistakes. They are not authentication or
authorization; see [Transport](transport.md).

## Restart

Persisted membership is authoritative on restart. Raftor restores peer
addresses into the transport and rejects mismatched cluster identity, missing
membership, inconsistent configuration state, or a reused retired ID.

Changing `initial_peers` does not rewrite existing membership. Use committed
membership operations to modify a running cluster.

## Legacy Migration

Storage created before durable membership is not migrated automatically. To
upgrade it, set `cluster_id` and provide
`RaftorConfig.legacy_membership_migration` with operator-verified data:

| Field | Meaning |
| --- | --- |
| `peers` | Current peer ID to advertised-address mapping. |
| `retired_node_ids` | IDs removed before migration and forbidden from reuse. |
| `membership_index` | Log index at which this mapping became authoritative. |
| `snapshot` | Historical membership for an existing snapshot when it differs from current state. |

Raftor validates the supplied mapping against persisted ConfState. Missing or
ambiguous migration data fails closed instead of inventing addresses or
membership history.

Back up the complete data directory before migration and validate the mapping
against every surviving member. Migration is an operator-controlled one-time
transition, not a discovery protocol.
