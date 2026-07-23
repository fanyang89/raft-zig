# raft-zig Developer Guide

## Project Overview

raft-zig is a Zig implementation of the RAFT consensus algorithm, ported from
the author's C++ project [raftpp](https://github.com/fanyang89/raftpp). Project
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

Upstream snapshots of raftpp and grpc-lite live under `ref/` and are
intentionally excluded from version control (see `.gitignore`). The `ref/`
tree is the source of truth while porting:

- `ref/raftpp/` — the C++ implementation being ported.
- `ref/grpc-lite/` — the Zig project whose build and module conventions we
  mirror.

Do not commit anything under `ref/`. When a port is complete, remove the
corresponding snapshot before opening a PR.

## Architecture

The port keeps raftpp's layered design, expressed as Zig modules:

- `src/core/` — plain data types, error model, role/state enums, and status
  snapshots. Currently ported.
- Future layers will land as top-level modules under `src/`:
  - `log` — `RaftLog` + `Unstable` (raftpp `core/raft_log.h`,
    `core/unstable_log.h`).
  - `storage` — `Storage`/`WritableStorage` vtables and `MemoryStorage`.
  - `progress` — `Progress`, `Inflights`, `ProgressTracker`, quorum structs.
  - `raft` — `Raft`/`RaftCore` state machine, `Step*`, tick, and become-*
    transitions.
  - `raw_node` — user-facing `RawNode` and `Ready` batching.
  - `conf` — `JointConf`, `MajorityConf`, `TrackerConf`, `ConfChanger`.
  - `read_only` — linearizable read-index queue.
  - `raftor` — high-level orchestration loop, ready processor, proposal
    tracker, `StateMachine` interface.
  - `wal` — segmented WAL with CRC32C.
  - `rpc` — pluggable transport (with a grpc-lite backend as the default).

## Design Mappings (C++ → Zig)

| raftpp                                       | raft-zig                                            |
| -------------------------------------------- | --------------------------------------------------- |
| `Result<T, RaftError>`                       | `Error!T` Zig error union (`src/core/error.zig`)    |
| Cap'n Proto generated structs                | Plain owned Zig structs (`src/core/types.zig`)      |
| `std::unique_ptr<MallocMessageBuilder>`      | Allocator-owned slices with explicit `deinit`       |
| `Storage` virtual interface                  | Vtable struct (to be added with `storage` module)   |
| `Map<K, V>` / `Set<K>`                       | `std.AutoHashMap` / `std.AutoHashMap(K, void)`      |
| `RAFTPP_LOG_*`                               | `std.log.*` scoped to `.raft_zig`                   |
| `nonstd::span<const Entry>`                  | `[]const Entry`                                     |
| Entry ctor `(index, term)` parameter order   | Same: `Entry{ .index = i, .term = t }`              |

## Style

- Run `zig fmt` on every change; CI checks formatting with `zig fmt --check`.
- Prefer small modules, explicit ownership, and deterministic `deinit`.
- Public APIs take `std.mem.Allocator` explicitly; no hidden global allocators.
- No comments unless requested; when needed, place them above the declaration.
- Production code must not write to stdout/stderr directly — use `std.log`.

## Porting Workflow

1. Read the raftpp header (and `.cc` if needed) under `ref/raftpp/`.
2. Map the C++ type to its Zig counterpart using the table above.
3. Port the unit tests from `ref/raftpp/tests/` into `tests/` or inline
   `test {}` blocks next to the implementation.
4. Keep field names stable so the public API test (`tests/public_api_test.zig`)
   continues to compile.
5. Run `mise run check` before declaring the port complete.

## Scope Decisions

The compatibility target is `raft-zig-core-v1`. Change this table before
implementing a feature outside the current decision.

| Capability                         | Decision      | Notes                                            |
| ---------------------------------- | ------------- | ------------------------------------------------ |
| Core consensus (Follower..Leader)  | Required      | Ported from `core/raft.h`                        |
| Pre-vote                           | Required      | Matches raftpp default                           |
| Joint consensus / conf changes     | Required      | `ConfChanger`, `JointConf`                       |
| Linearizable reads (Safe option)   | Required      | `ReadOnly` queue                                 |
| `MemoryStorage`                    | Required      | Built-in default                                 |
| Segmented WAL                      | Required      | Port `raftor/wal/`                               |
| grpc-lite RPC transport            | Required      | Default `rpc/` backend                           |
| Cap'n Proto wire format            | Out of scope  | Replace with Zig structs + grpc-lite framing     |
| Seastar integration                | Out of scope  | Not applicable in Zig                            |
| io_uring WAL backend               | Selected      | Linux-only, behind a build flag                  |
| Multi-tenant raft groups           | Out of scope  | One node, one group for now                      |

## MCP usage

Use Context7 MCP when you need library or framework documentation, including
Zig standard library patterns and grpc-lite APIs.
