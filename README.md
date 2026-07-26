# raft-zig

A Zig implementation of the [RAFT](https://raft.github.io/) consensus
algorithm. Ported from the author's C++ project
[raftpp](https://github.com/fanyang89/raftpp); project layout, build, and
module conventions follow the author's Zig [gRPC runtime](https://github.com/fanyang89/grpc-lite).

> **Status:** scaffold. The core data types, error model, and build system are
> in place. The full consensus state machine, storage, WAL, and RPC layers are
> landing incrementally — see `AGENTS.md` for the porting roadmap.

## Features (planned)

- **Core consensus** — Follower, Candidate, PreCandidate, and Leader roles.
- **Log management** — `RaftLog` over a pluggable `Storage` interface.
- **Dynamic membership** — joint consensus configuration changes.
- **Linearizable reads** — `ReadOnly` queue with the Safe option.
- **High-level orchestration** — `Raftor` event loop, ready processing, and
  proposal tracking (callbacks, futures, sync variants).
- **Write-Ahead Log** — segmented log files with CRC32C checksums.
- **Pluggable RPC** — abstract `Transport` interface with a grpc-lite backend.

The grpc-lite backend uses persistent directed raw streams with bounded
application and gRPC buffers. Stream identity metadata detects cluster and
node-address misconfiguration; it is not authentication. The transport has no
TLS support and must run only on a trusted network or behind a separately
secured network boundary.

## Development

Requires Zig 0.16.0 and [mise](https://mise.jdx.dev/).

```bash
mise install
mise run build
mise run test
mise run check   # fmt-check + test
```

Useful tasks:

```bash
mise run test-release-safe
mise run test-tsan
mise run test-ubsan
mise run prepare-gperftools
mise run build-gperftools
mise run test-gperftools
mise run bench-raft
mise run profile-raft
mise run fuzz-smoke
mise run fuzz-codec
mise run fuzz-wal
mise run fuzz-confchange
mise run fuzz-sim
mise run vopr-smoke
mise run fmt
mise run fmt-check
```

Bounded fuzzing exits non-zero when Zig writes a reproducer to `.zig-cache/f/crash`.
The simulation target checks core Raft safety and post-partition convergence; application and snapshot-state convergence remain separate full-stack work.

Direct Zig invocations work too:

```bash
zig build
zig build test --summary all
zig fmt build.zig src examples tests benchmarks
```

Fast Raft invariant checks are enabled by default in Debug and ReleaseSafe builds. Override them with `-Dinvariant-checks=false` or `-Dinvariant-checks=true`.

Entry checksums use [google/crc32c](https://github.com/google/crc32c), with runtime dispatch to x86 SSE4.2 or ARM64 CRC instructions and a portable fallback.

On Linux, `-Dgperftools=true` replaces the process C allocator with tcmalloc and exposes CPU and heap profiling through the `raft_zig_gperftools` module. This option is incompatible with ThreadSanitizer.

`Entry.data` is immutable and reference-counted inside raft-zig. Borrowed payloads are copied once when entering the Raft pipeline, then shared across Unstable, Ready, storage, WAL, and internal transports. Owned entries are linear handles and must not be duplicated with plain assignment; `cloneEntry` creates a deep copy and `shareEntry` creates another shared handle.

## Examples

| Example | Description |
| --- | --- |
| [`examples/minimal_node.zig`](examples/minimal_node.zig) | Single-node bootstrap. Will grow into a self-electing demo as the consensus core lands. |

## Durable Membership

Set `RaftorConfig.cluster_id` to enable durable membership. For bootstrap,
`initial_peers` contains the initial voters and each `Peer.context` contains
that peer's advertised address. An empty list creates a one-node cluster using
`advertise_addr`, or `listen_addr` when no advertised address is set.

For a fresh joining node, set `join = true`, provide seed nodes in
`initial_peers`, and do not include the local node ID. The joining node remains
non-promotable until it installs a cluster snapshot containing its ID. Existing
storage is always detected as restart state, regardless of `join`.

Leaving `cluster_id` null explicitly selects the legacy ID-only startup mode.
Legacy storage is not migrated automatically.

## Architecture

```
┌─────────────────────────────────────────┐
│  Application (StateMachine, Proposals)  │
├─────────────────────────────────────────┤
│  Raftor  (event loop, ready processor,  │
│           proposal tracker)             │
├─────────────────────────────────────────┤
│  Core Raft  (RawNode → Raft → RaftLog)  │
├─────────────────────────────────────────┤
│  WAL + RPC Transport                    │
└─────────────────────────────────────────┘
```

The current scaffold covers the lowest layer (`src/core/`): plain Zig structs
for `Entry`, `Message`, `HardState`, `ConfState`, `Snapshot`, and the
configuration-change types, plus a single error model (`src/core/error.zig`)
that replaces raftpp's `Result<T, RaftError>` with idiomatic Zig error unions.

## License

MIT License. See [LICENSE](LICENSE).
