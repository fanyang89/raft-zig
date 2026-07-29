# Durability

raft-zig separates the storage contract from its in-memory and persistent
implementations. Raftor uses the same Ready ordering with either backend.

## Storage Interfaces

`Storage` supplies initial Raft state, log ranges, terms, indexes, and
snapshots. `WritableStorage` adds the mutations required by Ready processing:
entries, HardState, snapshots, synchronization, membership, and compaction.

Custom implementations are supported through type-erased vtables. They must
preserve the same ownership, ordering, and error semantics as the built-in
backends. Dependencies passed to `Raftor.createWithDependencies` remain owned by
the caller and must outlive the Raftor.

## Backend Selection

`Raftor.create` and `createWithTransport` choose storage from
`RaftorConfig.data_dir`:

| `data_dir` | Backend | Restart behavior |
| --- | --- | --- |
| Empty | `MemoryStorage` | No process-restart durability. |
| Non-empty | `WALStorage` | Opens or creates persistent state in the directory. |

`MemoryStorage` is non-locking and must not be called concurrently. Use it only
on the Raft event loop or wrap it behind an application-owned serialized task.

`file_system` can replace the host filesystem when `data_dir` is set. The
filesystem value is borrowed and must outlive the Raftor. The built-in real
filesystem currently targets Linux.

## Segmented WAL

WALStorage keeps entries in segmented `segment-NNNNNN.wal` files. Each segment
header carries the `WAL1` magic and format version. Each record carries its
type, flags, payload length, alignment padding, payload, and CRC32C. The WAL
persists entries, HardState, ConfState, snapshots, and durable membership state.

Compaction removes segments fully covered by the retained snapshot and log
boundary. Truncation uses an in-memory index from Raft indexes to segment and
byte offsets.

WAL record checksums always protect the persistent record. Entry checksums are
a separate end-to-end field computed for replicated entries. Setting
`RaftorConfig.checksum_enabled` verifies entry CRC32C before apply; it does not
enable or disable WAL record integrity.

## Durable Ready Order

For each Ready, persistence follows this sequence:

1. Install an incoming snapshot as the new baseline.
2. Append entries after the snapshot index.
3. Persist changed HardState.
4. Synchronize when `Ready.must_sync` is set.
5. Restore the snapshot into the StateMachine.
6. Release persistence-dependent messages and apply committed entries.

Never send persistence-dependent messages or advance RawNode before the durable
boundary. Applications integrating `RawNode` directly are responsible for this
ordering; Raftor enforces it through `ReadyProcessor`.

## Snapshots

Snapshots contain Raft metadata and application payload. In durable-membership
mode they also contain the address-aware membership record; legacy ID-only
snapshots may omit it. The application implements payload creation and atomic
restore through `StateMachine`.

Raftor can trigger snapshots by applied-entry count,
`snapshot_entries_threshold`, or tick interval, `snapshot_interval_ticks`.
Both zero values disable their respective automatic trigger. Manual
`takeSnapshot` snapshots the current applied index.

A successful local snapshot is persisted and compacts covered entries. An
incoming snapshot is persisted before the application restore callback runs,
then RawNode advances from that durable baseline.

Snapshot creation and restore must represent application state at the supplied
applied index. `restore_snapshot` must be atomic: errors must leave the previous
application state unchanged.

## Restart

Raftor examines storage before selecting startup mode. Any existing HardState,
ConfState, or log entry selects restart regardless of `RaftorConfig.join`.

On restart, WALStorage reconstructs its indexes and returns persisted Raft state
and membership. Raftor restores a stored snapshot into the StateMachine before
applying subsequent committed entries. Operators must preserve the entire data
directory and use the same cluster and node identity.

Storage that predates durable membership is not guessed or migrated
automatically. See [Membership](membership.md) for the explicit migration
contract.

## Testing Durability

The repository tests ordinary WAL recovery, operation-level filesystem faults,
Marionette simulated disks, crash boundaries, and bounded WAL fuzzing. Run the
focused tasks described in [Testing](testing.md) before modifying persistence or
snapshot code.
