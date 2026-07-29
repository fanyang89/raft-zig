# Getting Started

raft-zig provides a high-level `Raftor` server and a lower-level `RawNode`
integration API. Start with `Raftor` unless the application must control Ready
processing itself.

## Requirements

- Zig 0.16.0
- Linux for the default filesystem and `Raftor.run`
- mise is optional but recommended for the pinned Zig toolchain

The repository does not have release tags yet. Downstream projects should pin a
known commit in `build.zig.zon`:

```zig
.dependencies = .{
    .raft_zig = .{
        .url = "https://codeload.github.com/fanyang89/raft-zig/tar.gz/<commit>",
        .hash = "<hash returned by zig fetch>",
    },
},
```

Add the public module to the consuming target:

```zig
const target = b.standardTargetOptions(.{});
const optimize = b.standardOptimizeOption(.{});
const raft_dependency = b.dependency("raft_zig", .{
    .target = target,
    .optimize = optimize,
});

const app = b.addExecutable(.{
    .name = "app",
    .root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{
            .name = "raft_zig",
            .module = raft_dependency.module("raft_zig"),
        }},
    }),
});
```

Application code imports the module as follows:

```zig
const raft = @import("raft_zig");
```

## Initialize Logging

raft-zig shares grpc-lite's process-global asynchronous logger. Initialize it
before creating application threads and deinitialize it after every Raftor and
transport has stopped:

```zig
try raft.log.initGlobal(init.gpa, init.io, false);
defer raft.log.deinitGlobal(init.gpa);
```

Logs are discarded before initialization. Pass `true` as the final argument to
enable debug logs and source locations.

## Implement a State Machine

Every `Raftor` requires an application-supplied `raft.StateMachine`. Its vtable
contains these operations:

| Operation | Contract |
| --- | --- |
| `apply` | Apply one borrowed committed entry and optionally return response bytes allocated with the Raftor allocator. Errors are terminal and must leave application state unchanged. |
| `take_snapshot` | Return an owned snapshot allocated with the callback allocator at the supplied applied index, term, and configuration. |
| `restore_snapshot` | Atomically replace application state from a snapshot stream. |
| `on_leadership_change` | Optional notification after role, term, or leader changes. |

The callback context and vtable must outlive the `Raftor`. See
[`examples/minimal_node.zig`](../examples/minimal_node.zig) for a complete
implementation and [Zig API](zig-api.md) for the entry, response, and snapshot
ownership details.

## Create a Single-Node Cluster

`Raftor.create` owns either `MemoryStorage` or `WALStorage` and installs a
`NoopTransport`, which is suitable for one-node operation:

```zig
var config = raft.RaftorConfig{};
config.raft.id = 1;
config.cluster_id = .{1} ++ .{0} ** 15;
config.advertise_addr = "127.0.0.1:9000";

const node = try raft.Raftor.create(allocator, config, state_machine);
defer node.destroy();

try node.campaign();
```

`cluster_id` must be a stable, non-zero 16-byte value. An empty
`initial_peers` list bootstraps the local node as the only voter using
`advertise_addr`, or `listen_addr` when no advertised address is set.

## Drive the Node

Call `tick` from an application event loop or call blocking `run` on a dedicated
thread:

```zig
while (running) {
    _ = try node.tick();
}
```

```zig
try node.run(); // Returns after another thread calls node.stop().
```

`Raftor` mutation is single-threaded. Do not call event-loop operations such as
`tick`, `campaign`, or membership changes concurrently. `propose` and
`readIndex` enqueue copied request data and are intended for cross-thread
ingress. If they are called concurrently, the allocator passed to Raftor must be
thread-safe because request buffers can be allocated and freed on different
threads.

Proposals complete through callbacks:

```zig
try node.propose("command", .{
    .ctx = &request,
    .function = Request.onProposal,
});
```

A successful `propose` call means the request entered the bounded queue, not
that it committed. The callback receives the final apply result, timeout,
shutdown, or leadership error. A successful response slice is borrowed until
the callback returns. Normal completion and timeout run on the event-loop
thread; shutdown completion runs synchronously on the thread calling `stop`.
Callbacks must be short, non-blocking, and safe on either thread.

## Enable Persistence

Leave `data_dir` empty for `MemoryStorage`. Set it to a directory to open the
segmented WAL backend:

```zig
config.data_dir = "/var/lib/my-service/raft";
```

Storage containing HardState, ConfState, or log entries is automatically treated
as restart state even when `join` is set. The state machine must be able to
restore the persisted snapshot before new committed entries are applied. See
[Durability](durability.md) and [Membership](membership.md) before deploying
persistent nodes.

## Multi-Node Setup

Multi-node applications create a transport and pass its borrowed interface to
`Raftor.createWithTransport`. Durable mode records peer addresses with the Raft
configuration so restarts do not depend on seed configuration.

The grpc-lite backend needs a thread-safe allocator. Hostname peer addresses
also need one process-wide `raft.GrpcRuntime` that outlives every transport.
See [Transport](transport.md) for a complete setup and security constraints.

## Shutdown

`stop` is callback-safe and requests shutdown without waiting for `run` to
return. The owner must join application threads and then call `destroy`:

```zig
node.stop();
worker.join();
node.destroy();
```

Do not call `destroy` directly or indirectly from a Raftor callback. It waits
for the event loop and shutdown callbacks to quiesce.
