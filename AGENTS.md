# raft-zig Developer Guide

## Project Overview

raft-zig is a Zig implementation of the RAFT consensus algorithm. Project
layout, build system conventions, and module style follow the author's Zig
[gRPC runtime](https://github.com/fanyang89/grpc-lite).

## Language

- Chat communication: use Chinese (Simplified) when talking with the user or
  maintainer.
- Repo artifacts: use English for code, code comments, and documentation.

## Toolchain

- Zig 0.16.0
- mise for task runners

## Commands

```bash
mise run build
mise run test
mise run test-release-safe
mise run test-tsan
mise run test-ubsan
mise run fuzz-smoke
mise run fuzz-codec
mise run fuzz-wal
mise run fuzz-confchange
mise run fuzz-sim
mise run fuzz-wal-crash
mise run vopr-smoke
mise run fmt
mise run fmt-check
mise run ci-lint
mise run check
```

Direct Zig invocations work too:

```bash
zig build
zig build test --summary all
zig fmt build.zig src examples tests
```

## Reference Layout

A local snapshot of the [gRPC runtime](https://github.com/fanyang89/grpc-lite)
lives under `ref/grpc-lite/` and is intentionally excluded from version control
(see `.gitignore`). It is the reference for build and module conventions.

Do not commit anything under `ref/`.

## Architecture

raft-zig is organized in layered modules under `src/`:

- `core/` — plain data types, error model, role/state enums, and status
  snapshots.
- `log` — `RaftLog` + `Unstable`.
- `storage` — `Storage`/`WritableStorage` vtables and `MemoryStorage`.
- `progress` — `Progress`, `Inflights`, `ProgressTracker`, quorum structs.
- `raft` — `Raft` state machine, `Step*`, tick, and become-* transitions.
- `raw_node` — user-facing `RawNode` and `Ready` batching.
- `conf` — `JointConf`, `MajorityConf`, `TrackerConf`, `ConfChanger`.
- `read_only` — linearizable read-index queue.
- `raftor` — high-level orchestration loop, ready processor, proposal
  tracker, `StateMachine` interface.
- `wal` — segmented WAL with CRC32C.
- `rpc` — pluggable transport (with a grpc-lite backend as the default).

## Style

- Run `zig fmt` on every change; CI checks formatting with `zig fmt --check`.
- Prefer small modules, explicit ownership, and deterministic `deinit`.
- Public APIs take `std.mem.Allocator` explicitly; no hidden global allocators.
- No comments unless requested; when needed, place them above the declaration.
- Production code must not write to stdout/stderr directly — use `std.log`.

## Scope Decisions

The compatibility target is `raft-zig-core-v1`. Change this table before
implementing a feature outside the current decision.

| Capability                         | Decision      | Notes                                                                       |
| ---------------------------------- | ------------- | --------------------------------------------------------------------------- |
| Core consensus (Follower..Leader)  | Required      | Follower/Candidate/Leader transitions                                       |
| Pre-vote                           | Required      | Enabled by default                                                          |
| Joint consensus / conf changes     | Required      | `ConfChanger`, `JointConf`                                                  |
| Linearizable reads (Safe option)   | Required      | `ReadOnly` queue; new leaders postpone ReadIndex requests until they commit an entry in their own term, then replay (etcd-aligned) |
| `MemoryStorage`                    | Required      | Built-in default                                                            |
| Segmented WAL                      | Required      | Segmented WAL with CRC32C                                                   |
| grpc-lite RPC transport            | Required      | Default `rpc/` backend                                                      |
| Cap'n Proto wire format            | Out of scope  | Zig structs + grpc-lite framing                                             |
| Seastar integration                | Out of scope  | Not applicable in Zig                                                       |
| io_uring WAL backend               | Selected      | Linux-only, behind a build flag                                             |
| Multi-tenant raft groups           | Out of scope  | One node, one group for now                                                 |

## MCP usage

Use Context7 MCP when you need library or framework documentation, including
Zig standard library patterns and grpc-lite APIs.
