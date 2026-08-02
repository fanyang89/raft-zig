# libelection

`libelection` is a C SDK for durable, fixed-membership leader election. It uses
raft-zig for consensus, an internal grpc-lite transport, and a WAL for restart
recovery.

## Build

Zig 0.16.0 and a Linux C/C++ toolchain are required. ReleaseSafe is the default
optimization mode.

```sh
zig build test --summary all
zig build test-installed-c-sdk --summary all
zig build install-c-sdk --prefix /path/to/prefix
```

The SDK installs these files:

```text
include/libelection/libelection.h
lib/libelection.a
lib/libelection.so
lib/libelection.so.1
lib/libelection.so.1.0.0
```

Link a C program against the shared library:

```sh
cc app.c -I/path/to/prefix/include -L/path/to/prefix/lib \
  -lelection -pthread -ldl -lrt -lm -o app
```

The static library includes its Zig, grpc-lite, nghttp2, and c-ares objects.
It also needs the C++ runtime used by crc32c:

```sh
cc app.c -I/path/to/prefix/include /path/to/prefix/lib/libelection.a \
  -pthread -ldl -lrt -lm -lstdc++ -o app
```

## Configuration

Every node receives the same ordered-independent peer set and cluster ID. Each
node ID must be nonzero and unique. Peer addresses use `host:port` syntax, and
multi-node peer ports must be nonzero. The local peer address is advertised to
other nodes and may differ from `listen_address`, such as when listening on a
wildcard interface.

Each node needs a dedicated persistent `data_dir`. The SDK creates missing path
components, binds the directory to its node and cluster IDs, and holds an
exclusive process lock until destroy. Do not reuse a data directory with another
node ID, cluster ID, or membership list.

Initialize public structs with their macros so `struct_size` and defaults are
set correctly:

```c
election_node_options options = ELECTION_NODE_OPTIONS_INIT;
election_callbacks callbacks = ELECTION_CALLBACKS_INIT;
election_status status = ELECTION_STATUS_INIT;
```

`election_node_create` copies all option strings and peers. The caller may free
that input after the function returns.

## Drive Modes

Managed mode is the default. `election_node_start` creates a driver thread that
runs the Raft loop, advances logical ticks at `tick_interval_ms`, and invokes
callbacks.

External mode lets the host drive Raft from one thread:

```c
options.drive_mode = ELECTION_DRIVE_EXTERNAL;
election_node_start(node);

for (;;) {
  election_node_poll(node, NULL);
  election_node_tick(node, NULL);
}
```

`election_node_poll` processes pending Raft and transport work without advancing
logical time. Each `election_node_tick` advances exactly one logical tick. The
SDK still owns the grpc-lite network runtime in external mode. Calling `poll`
or `tick` on a managed node returns `ELECTION_ERROR_INVALID_STATE`.

Serialize lifecycle functions with all other calls for the same node. Call
`election_node_destroy` only after every other call has returned, and never use
the handle afterward. External `poll` and `tick` calls must run on one host
thread.

## Callbacks

`ELECTION_EVENT_LEADERSHIP_ACQUIRED` is emitted only after a leader commits an
entry in its current term. `ELECTION_EVENT_LEADERSHIP_LOST` follows loss of
active leadership, including a clean shutdown. `ELECTION_EVENT_FAILED` reports
a terminal driver failure.

Managed callbacks run on the SDK driver thread. External callbacks run
synchronously inside `poll`, `tick`, or `shutdown`. Callback data is borrowed
until the callback returns. A callback may call `election_node_get_status`, but
must not call lifecycle or drive functions for the originating node. No callback
runs after `election_node_shutdown` returns.

## Example

Build the managed-mode C node:

```sh
zig build example
```

Start three terminals with the same cluster ID and peer list:

```sh
zig-out/bin/election-node 1 00112233445566778899aabbccddeeff \
  127.0.0.1:7101 data/node-1 \
  1=127.0.0.1:7101 2=127.0.0.1:7102 3=127.0.0.1:7103
```

Use node IDs 2 and 3 with their matching listen addresses and data directories
for the other processes. Send `SIGINT` or `SIGTERM` for a clean shutdown.

See `include/libelection/libelection.h` for the complete ABI.
