# Transport

Raft consensus operates on node IDs and messages. The `Transport` interface
maps those IDs to application-defined delivery and peer lifecycle behavior.

## Contract

The type-erased `Transport` vtable provides:

| Method | Responsibility |
| --- | --- |
| `start`, `stop` | Manage transport lifecycle. |
| `addPeer`, `removePeer` | Maintain the current ID-to-address routing table. |
| `send` | Queue or transmit messages according to each message's `to` field. |
| `setMessageCallback` | Register inbound message delivery. |
| `setPeerEventCallback` | Register unreachable, snapshot-failure, and identity-rejection events. |
| `pollOne` | Deliver at most one queued message or event on the Raft event loop. |
| `identity` | Optionally expose the transport's cluster and local-node identity. |

Foreign network threads must queue input rather than invoke Raft directly.
Callbacks run only through `pollOne` on the owner event loop. Message ownership
transfers to `MessageCallback` when invoked, including when it returns an error.
Messages passed to `send` remain borrowed; an asynchronous implementation must
clone or share retained data before `send` returns.

## Built-In Transports

| Implementation | Use |
| --- | --- |
| `NoopTransport` | Single-node `Raftor.create` and direct tests. |
| `LoopbackTransport` | Deterministic in-process multi-node tests and simulations. |
| `GrpcLiteTransport` | Persistent raw-stream network transport. |

Applications may provide another implementation through `Transport.VTable`.

## grpc-lite Setup

`GrpcLiteTransport.create` requires a thread-safe allocator because server
callbacks and peer workers encode, decode, and enqueue data concurrently.

```zig
const identity = raft.TransportIdentity{
    .cluster_id = cluster_id,
    .node_id = node_id,
};

const grpc_transport = try raft.GrpcLiteTransport.create(allocator, .{
    .identity = identity,
    .listen_addr = "0.0.0.0:9000",
});
defer grpc_transport.destroy();

const node = try raft.Raftor.createWithTransport(
    allocator,
    config,
    state_machine,
    grpc_transport.transport(),
);
defer node.destroy();
```

The transport is caller-owned and must outlive the Raftor. Destroy the Raftor
before destroying the transport.

`Raftor` starts and stops the borrowed transport. Durable membership populates
its peer table from bootstrap state, committed configuration changes, or
restart state.

## Runtime and Addresses

IPv4 literals require no global DNS setup. Hostname peer addresses need one
process-wide `raft.GrpcRuntime` passed through
`GrpcLiteTransportConfig.runtime`. Initialize it before application threads and
deinitialize it after every transport:

```zig
var runtime = try raft.GrpcRuntime.init();
defer runtime.deinit();

const grpc_transport = try raft.GrpcLiteTransport.create(allocator, .{
    .identity = identity,
    .listen_addr = "0.0.0.0:9000",
    .runtime = &runtime,
});
```

The current grpc-lite integration uses IPv4 addresses. `listen_addr` is the
local bind address; durable peer membership should contain addresses reachable
from other nodes.

## Backpressure and Recovery

The grpc-lite backend uses one persistent directed raw stream per peer and
bounded buffering at every ingress layer.

`stream_limits` controls grpc-lite message and stream queues.
`mailbox_max_messages` and `mailbox_max_bytes` bound decoded inbound Raft
messages. Outbound saturation returns `TransportBackpressure`. An inbound
mailbox overflow terminates the affected stream with `InboundMailboxFull` so
memory cannot grow without limit; peer recovery then follows reconnect policy.

Peer workers reconnect with bounded exponential delays configured by
`reconnect_initial_delay_ns` and `reconnect_max_delay_ns`. Transport recovery
does not make application proposals idempotent; applications remain responsible
for request identity and retry policy.

Peer lifecycle events report unreachable peers and failed snapshot delivery to
Raft. Identity rejection is surfaced separately so operators can distinguish a
configuration error from an ordinary connection failure.

## Security Boundary

The raft-zig grpc-lite backend does not expose TLS or peer authentication.
Stream metadata includes the protocol version, cluster ID, source node ID, and
target node ID to detect misconfiguration, but a network attacker can forge it.

Deploy the transport only on a trusted network or behind a separately secured
network boundary that provides encryption, authentication, authorization, and
traffic isolation. Do not treat successful identity validation as proof of peer
identity.

## Logging and Shutdown

The transport shares raft-zig's process-global logger. Initialize it before
starting transports; otherwise logs are discarded.

`stop` prevents new delivery and begins bounded graceful shutdown. The transport
must remain alive until Raftor event-loop work and callbacks have stopped.
Follow the lifecycle order in [Getting Started](getting-started.md).
