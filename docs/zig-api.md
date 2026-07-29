# Zig API

[`src/root.zig`](../src/root.zig) is raft-zig's public module entry point. It
re-exports the supported integration types as well as lower-level consensus
types that may evolve before 1.0.

## Choose an Integration Level

| Level | Primary types | Application responsibility |
| --- | --- | --- |
| Orchestrated | `Raftor`, `RaftorConfig`, `StateMachine`, `Transport` | Configure the node, implement replicated state, and drive `tick` or `run`. |
| Ready/Advance | `RawNode`, `Ready`, `LightReady`, `Storage` | Persist state, order messages, apply entries, restore snapshots, and advance cursors. |
| Consensus core | `Raft`, `RaftLog`, quorum and progress types | Build a specialized integration and preserve every Raft safety contract. |

## Raftor

`Raftor` combines a `RawNode`, Ready processor, storage, proposal tracker,
bounded ingress queues, state machine, and transport.

### Construction

| Constructor | Ownership |
| --- | --- |
| `create` | Owns MemoryStorage or WALStorage and a single-node NoopTransport. |
| `createWithTransport` | Owns storage selected from `data_dir`; borrows the supplied transport. |
| `createWithDependencies` | Borrows storage, transport, and state machine; the caller supplies the startup mode. |

All borrowed dependencies and callback contexts must remain at stable addresses
until after `Raftor.destroy`.

### Event Loop

`tick` performs one event-loop iteration: it advances logical time, polls a
bounded amount of transport work, drains request queues, processes Ready
batches, and triggers snapshots. `run` repeatedly calls `tick` and blocks until
`stop` is requested.

Only one event-loop operation may mutate a Raftor at a time. Proposal and
read-index ingress queues are thread-safe. When callers use them concurrently,
the allocator passed to Raftor must also be thread-safe because request buffers
can be allocated and released on different threads. `getStatus` returns a
synchronized snapshot, which may lag mutations until the current tick publishes
status.

### Requests

`propose` and `readIndex` copy their input into bounded queues. Queue limits and
drain budgets are configured by `RaftorConfig`; saturation returns
`ProposalBackpressure` or `ReadIndexBackpressure`.

Callbacks are the only completion API. Proposal success data is borrowed from
the tracker until callback return. Accepted requests are completed exactly once
by apply/read confirmation, timeout, shutdown, or terminal failure.

Apply, read-confirmation, and timeout callbacks run on the event-loop thread.
`stop` invokes detached shutdown callbacks synchronously on the thread that
calls `stop`. Callbacks must therefore be thread-safe, must not block, and must
not assume one fixed execution thread.

`proposal_timeout_ticks` and `read_index_timeout_ticks` start after a request
leaves its ingress queue. Zero disables the corresponding timeout. Applications
serving leader-only requests can inspect `leaderServicePolicy` to ensure
check-quorum, disabled proposal forwarding, and bounded timeouts are all set.

### Lifecycle

`stop` is callback-safe and does not wait for an active `run` call. `destroy`
requests shutdown, waits for active event-loop work and shutdown callbacks, and
then releases owned resources. Calling `destroy` from a Raftor callback is
invalid and panics. Because `stop` invokes shutdown callbacks synchronously, a
blocking callback also blocks the caller of `stop`.

## StateMachine

`StateMachine` is a type-erased vtable borrowed by `Raftor`.

`apply` receives committed entries in order. The entry and its fields are
borrowed for the call; the StateMachine must not deinitialize or retain them.
Use `cloneEntry` or `shareEntry` with an application allocator when state must
outlive the callback. Returning an error is terminal and must leave application
state unchanged. An optional `ApplyResult.response` must be allocated with the
allocator passed to Raftor. raft-zig owns it after return, delivers it as a
borrowed slice to the proposal callback, and then frees it with that allocator.

`take_snapshot` receives the applied index, applied term, and current
`ConfState`. The configuration is borrowed for the call. It returns an owned
`Snapshot`; every owned field must use the allocator passed to `take_snapshot`.
Raftor persists and then deinitializes the snapshot and compacts the covered log
prefix.

`restore_snapshot` receives metadata and a streaming `SnapshotReader`. It must
atomically replace application state: returning an error must preserve the old
state.

`on_leadership_change` is optional and defaults to a no-op. Keep callback work
short because it runs on the Raft event-loop thread.

## RawNode

`RawNode` wraps the consensus state machine without performing I/O. A complete
integration repeatedly performs this sequence:

1. Call `tick`, `step`, `propose`, or another input method.
2. Check `hasReady` and obtain an owned `Ready` with `getReady`.
3. Persist its snapshot, entries, and HardState; honor `must_sync`.
4. Restore a persisted incoming snapshot, then send `Ready.light.messages`.
5. Apply committed entries and consume read states.
6. Call `advance`, persist and sync any advanced HardState, then send and apply the returned `LightReady`.
7. Call `advanceApply` and deinitialize every owned value.

The exact durable order is described in [Architecture](architecture.md). A
latency-optimized integration may send `Ready.messages()` before persistence,
but must send `Ready.light.messages` after persistence when
`Ready.is_persisted_msg` is true. Never send the same message in both phases.

## Ownership

raft-zig uses explicit allocators and deterministic `deinit` throughout the
public API.

`Entry.data` is immutable and reference-counted after it enters the Raft
pipeline. Owned `Entry` values are linear handles: do not duplicate them with a
plain assignment and then deinitialize both copies. Use `cloneEntry` for a deep
copy or `shareEntry` for another shared handle.

Owned `Ready` and `LightReady` values own their slices and nested values. Their
`deinit` methods release those allocations. Transport `MessageCallback` takes
ownership of the message even when the callback returns an error.

Messages passed to `Transport.send` are borrowed only for that call. An
asynchronous transport must clone or share every value it retains before
returning from `send`.

Borrowed values are valid only for the documented call duration. In particular,
proposal response bytes are valid only until callback return, and dependencies
passed through `RaftorDependencies` remain caller-owned.

## Threading

- Consensus mutation and Ready processing run on one event-loop thread.
- `Raftor.propose`, `Raftor.readIndex`, `Raftor.stop`, and status publication use synchronized state; concurrent ingress also requires a thread-safe Raftor allocator.
- `MemoryStorage` is not safe for concurrent direct calls.
- `GrpcLiteTransport.create` requires a thread-safe allocator because server callbacks and peer workers allocate concurrently.
- Transport implementations queue foreign-thread input and invoke Raft callbacks from `pollOne` on the event-loop thread.

Applications needing a different threading model should adapt `RawNode` rather
than concurrently invoking the consensus core.
