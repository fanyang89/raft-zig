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
mise run fuzz-smoke
mise run fuzz-codec
mise run fuzz-wal
mise run fuzz-confchange
mise run fmt
mise run fmt-check
```

Direct Zig invocations work too:

```bash
zig build
zig build test --summary all
zig fmt build.zig src examples tests
```

## Examples

| Example | Description |
| --- | --- |
| [`examples/minimal_node.zig`](examples/minimal_node.zig) | Single-node bootstrap. Will grow into a self-electing demo as the consensus core lands. |

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
