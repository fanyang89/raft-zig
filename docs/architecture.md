# Architecture

raft-zig separates pure consensus decisions from persistence, transport, and
application state. The split supports both a low-level Ready/Advance API and a
complete orchestration layer.

## Layers

```text
Application
  StateMachine, proposals, linearizable reads, membership operations
                              |
                           Raftor
  lifecycle, ingress queues, ReadyProcessor, status, snapshots
                              |
                           RawNode
  Ready ownership, persistence boundaries, advance cursors
                              |
                Raft + RaftLog + ProgressTracker
  elections, replication, quorum, ReadIndex, configuration changes
                              |
                Storage                 Transport
        MemoryStorage / WAL      Loopback / grpc-lite / custom
```

| Module | Role |
| --- | --- |
| `core/` | Entries, messages, state, snapshots, errors, and configuration-change values. |
| `raft.zig` | Consensus state machine and message handling. |
| `raft_log.zig`, `unstable_log.zig` | Stable and not-yet-persisted log views. |
| `progress*.zig`, quorum modules | Per-peer replication state and quorum calculations. |
| `raw_node.zig` | User-facing Ready/Advance boundary. |
| `ready_processor.zig` | Correct persistence, transport, apply, and advance ordering. |
| `raftor.zig` | Server lifecycle, request queues, snapshots, membership, and status. |
| `storage.zig`, `memory_storage.zig`, `wal.zig` | Pluggable storage and built-in backends. |
| `transport.zig`, `loopback_transport.zig`, `rpc/` | Pluggable message transport and peer lifecycle. |

## Ready Processing

Raft produces decisions but does not perform external side effects. `Ready`
describes the work an integration must complete. `ReadyProcessor` executes each
batch in this order:

1. Validate entry checksums when enabled.
2. Persist an incoming snapshot as the storage baseline.
3. Persist unstable entries following that snapshot.
4. Persist HardState and sync when required.
5. Restore the durable snapshot into the application state machine.
6. Send messages whose safety depends on persistence.
7. Apply committed entries and complete ReadIndex requests.
8. Advance RawNode and process LightReady entries and messages.

This order is a safety contract. In particular, vote responses and follower
append responses must not become externally visible before the state that
justifies them is durable.

`Ready.must_sync` indicates that the storage backend must establish its durable
boundary. `Ready.is_persisted_msg` distinguishes messages that become sendable
only after persistence; `Ready.messages()` hides those messages from an early
send phase.

## Raftor Tick

One `Raftor.tick` iteration serializes node mutation and performs bounded work:

1. Expire tracked requests and drain bounded proposal and ReadIndex queues.
2. Advance the Raft logical clock and process resulting Ready batches.
3. Poll bounded inbound transport messages and peer events.
4. Process additional Ready batches produced by inbound work.
5. Trigger automatic snapshots when configured thresholds are met.
6. Publish a synchronized status snapshot when leaving the event loop.

Budgets prevent one input source from monopolizing the event loop. Request data
is copied before entering the queues, so producer threads do not need to retain
their input buffers.

## Storage Boundary

The consensus core reads through `Storage`. `Raftor` needs `WritableStorage` to
append entries, persist HardState and snapshots, synchronize, and compact.
`Raftor.create` selects MemoryStorage for an empty `data_dir` and WALStorage for
a non-empty directory. `createWithDependencies` allows another implementation.

Storage and application snapshots form one recovery boundary: the storage
snapshot carries Raft metadata and, in durable mode, address-aware membership,
while the StateMachine owns the application payload and atomic restore behavior.

## Transport Boundary

`Transport` routes outbound messages by node ID and queues inbound work from
foreign threads. Raft callbacks run only when the event loop calls `pollOne`.
This keeps consensus mutation single-threaded even when a transport has network
worker threads.

Peer lifecycle events report unreachable peers, failed snapshot delivery, and
identity rejection. Raftor converts those events into the corresponding
RawNode reports.

## Failure Model

Apply and snapshot-restore errors are terminal because Raftor cannot safely
continue after application state diverges from the committed log. Persistence
failures retain the pending Ready phase so callers can inspect the terminal
error without incorrectly advancing the consensus cursors.

Fast internal invariants run by default in Debug and ReleaseSafe builds. The
test harness adds election-safety, committed-prefix, convergence, filesystem
fault, and crash-recovery checks; see [Testing](testing.md).
