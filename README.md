# raft-zig

[![CI](https://github.com/fanyang89/raft-zig/actions/workflows/ci.yml/badge.svg)](https://github.com/fanyang89/raft-zig/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/fanyang89/raft-zig/graph/badge.svg)](https://codecov.io/gh/fanyang89/raft-zig)
[![Zig](https://img.shields.io/badge/Zig-0.16.0-f7a41d?logo=zig&logoColor=white)](https://ziglang.org/)
[![License](https://img.shields.io/github/license/fanyang89/raft-zig)](LICENSE)

An embeddable [Raft](https://raft.github.io/) consensus library for Zig with a
Ready/Advance core, durable WAL, snapshots, and a grpc-lite transport.

raft-zig is a functional pre-1.0 implementation ported from
[raftpp](https://github.com/fanyang89/raftpp). Core consensus, `RawNode`,
`Raftor`, persistent storage, durable membership, and multi-node transport are
implemented and tested. Public APIs and persistent formats may still change
before 1.0.

## Highlights

| Consensus | Integration | Durability and transport |
| --- | --- | --- |
| Pre-vote and check-quorum options | Low-level `RawNode` Ready/Advance API | In-memory and segmented WAL storage |
| Joint-consensus membership changes | High-level `Raftor` event loop | CRC32C records and entry verification |
| Safe and lease-based ReadIndex | Callback-based proposals and reads | Snapshots, compaction, and restart recovery |
| Leadership transfer and learners | Pluggable state machine and storage | Loopback and grpc-lite transports |

The test suite includes deterministic network simulation, fault-injected filesystems,
Marionette crash testing, bounded fuzzing, sanitizers, and behavioral inventories from
five established Raft implementations.

## Quick Start

raft-zig requires Zig 0.16.0. [mise](https://mise.jdx.dev/) is recommended for
tool installation and project tasks, but direct `zig build` commands also work.

There are no release tags yet. Pin a commit when adding the package to
`build.zig.zon`, then import its public module:

```zig
const raft_dependency = b.dependency("raft_zig", .{
    .target = target,
    .optimize = optimize,
});
app.root_module.addImport("raft_zig", raft_dependency.module("raft_zig"));
```

```zig
const raft = @import("raft_zig");
```

Build and run the single-node example from this repository:

```bash
mise install
mise run build
./zig-out/bin/raft-zig-minimal-node
```

For a complete application, [`examples/raft-sqlite`](examples/raft-sqlite)
implements a durable replicated SQLite database with gRPC and a CLI. It remains
an independent package so the core build does not compile SQLite or protoc:

```bash
mise run test-raft-sqlite
mise -C examples/raft-sqlite run build
```

See [Getting Started](docs/getting-started.md) for dependency setup, logger
initialization, state-machine requirements, and a complete node lifecycle.

## Integration Model

| API | Use it when |
| --- | --- |
| `Raftor` | You want built-in Ready processing, proposal queues, snapshots, storage selection, and transport orchestration. |
| `RawNode` | You need to own the persistence, message dispatch, apply, and event-loop pipeline. |

`Raftor.create` uses `MemoryStorage` when `data_dir` is empty and `WALStorage`
when it is set. Applications always provide the replicated `StateMachine`.
Multi-node deployments additionally provide a `Transport`.

```text
Application state machine and requests
                 |
              Raftor
       queues, Ready processing
                 |
              RawNode
          Ready / Advance API
                 |
       Raft, RaftLog, Storage
```

See [Architecture](docs/architecture.md) and the [Zig API guide](docs/zig-api.md)
for the processing order and ownership contracts.

## Compatibility

| Capability | Status |
| --- | --- |
| Linux x86_64 and arm64 | Continuously tested in Debug and ReleaseSafe |
| Core Raft, ReadIndex, learners, joint consensus | Supported |
| MemoryStorage, WAL, snapshots, restart | Supported |
| Single Raft group per `Raftor` | Supported scope |
| grpc-lite authentication and TLS | Not provided by raft-zig |
| Multi-tenant group hosting, disaster-recovery import | Out of scope or not implemented |

The default filesystem and `Raftor.run` currently use Linux primitives. The
grpc-lite transport uses stream identity metadata to reject cluster or node
misconfiguration, but that metadata is not authentication. Run it only on a
trusted network or behind a separately secured network boundary.

Types re-exported by [`src/root.zig`](src/root.zig) are the public API surface.
Lower-level modules remain available for experimentation and may evolve before 1.0.

## Documentation

- [Getting Started](docs/getting-started.md): package setup and the first node
- [Zig API](docs/zig-api.md): Raftor, RawNode, StateMachine, ownership, and threading
- [Architecture](docs/architecture.md): layers and Ready processing order
- [Durability](docs/durability.md): storage, WAL, checksums, snapshots, and recovery
- [Membership](docs/membership.md): bootstrap, join, restart, and migration
- [Transport](docs/transport.md): transport contract and grpc-lite integration
- [Testing](docs/testing.md): suites, fuzzing, fault injection, and upstream inventory
- [Development](docs/development.md): tasks, build options, profiling, and coverage

## Development

```bash
mise install
mise run check
```

See the [development guide](docs/development.md) for focused test tasks and build
options.

## License

[MIT](LICENSE)
