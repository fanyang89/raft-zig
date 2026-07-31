# raft-sqlite

A complete raft-zig example implementing a bounded, Raft-replicated SQLite
database for Zig. It is an independent package under
`examples/raft-sqlite`, so the raft-zig default build stays lightweight.

SQLite stores the durable state machine in `<data-dir>/state.sqlite3`. Each SQL
transaction atomically persists user data, request deduplication records, and
the applied Raft index and term. Normal restarts reuse that cursor and replay
only a missing committed suffix. Raft WAL and image snapshots remain the
consensus authority and rebuild a missing or lagging SQLite file. Writes are
accepted by the leader and replicated as versioned protobuf commands. Leader
reads use Raft ReadIndex before querying the local state machine.

## Status

- One Raft group with a static initial membership
- Typed gRPC Execute, Query, and Status methods
- Atomic parameterized write batches with request ID deduplication
- Linearizable leader reads
- Durable SQLite restart, WAL suffix replay, image snapshots, and follower catch-up
- SQLite 3.53.4 amalgamation pinned by SHA3-256

Dynamic membership is not exposed yet. Restart nodes with the same node ID,
cluster ID, peer list, and data directory.

## Build

Zig 0.16.0 is required.

From the raft-zig repository root:

```sh
mise run test-raft-sqlite
mise -C examples/raft-sqlite run build
```

From this example directory:

```sh
mise run build
mise run check
```

Direct Zig commands also work:

```sh
zig build
zig build test --summary all
zig build test -Doptimize=ReleaseSafe --summary all
```

## Run A Node

The peer list defines the complete initial voting set. A single-node example:

```sh
zig build run -- serve \
  --node-id 1 \
  --cluster-id 0198f54d-5c2a-7000-8000-000000000001 \
  --api-listen 127.0.0.1:8001 \
  --raft-listen 127.0.0.1:9001 \
  --data-dir ./data/node-1
```

For a three-node cluster, pass the same three `--peer` values to every node:

```text
--peer 1=127.0.0.1:9001
--peer 2=127.0.0.1:9002
--peer 3=127.0.0.1:9003
```

Each node needs a distinct `--node-id`, API address, Raft address, and data
directory. All nodes need the same cluster ID.

## Storage

The data directory contains both the `raft-zig` WAL and `state.sqlite3`. Keep
and restore the directory as one unit, and stop the node before copying it.
SQLite uses a rollback journal with `synchronous=EXTRA`. User changes and the
durable applied cursor commit in one transaction.

Raft snapshots contain a checksummed SQLite image. Snapshot installation uses
the SQLite Online Backup API, so a failed installation rolls back without
changing the current database. Existing version 1 snapshots are migrated during
installation. A SQLite cursor ahead of the durable Raft commit, with a mismatched
term, or from another cluster is rejected at startup.

## CLI

```sh
zig-out/bin/raft-sqlite exec 127.0.0.1:8001 create-items \
  'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL) STRICT'

zig-out/bin/raft-sqlite exec 127.0.0.1:8001 insert-1 \
  'INSERT INTO items VALUES (?1, ?2)' int:1 text:first

zig-out/bin/raft-sqlite query 127.0.0.1:8001 \
  'SELECT id, name FROM items WHERE id = ?1' int:1

zig-out/bin/raft-sqlite status 127.0.0.1:8001
```

Parameter forms are `null`, `int:42`, `real:3.14`, `text:value`, and
`blob:00ff`. CLI responses are JSON. Followers return `failed_precondition`
instead of forwarding requests.

## Limits And SQL Policy

- Database image: 64 MiB
- SQL text: 1 MiB
- Write batch: 32 statements and 1024 parameters
- Query result: 1000 rows and 4 MiB of text/blob values
- User statement execution: 10 million SQLite virtual-machine opcodes
- API response frame: 8 MiB
- Request ID: 128 bytes, retained for a 10,000-entry deduplication window

User SQL cannot access internal tables or use transactions, savepoints,
ATTACH, DETACH, PRAGMA, temporary schema objects, triggers, virtual tables,
extensions, or nondeterministic random and time functions. Write batches run in
one SQLite transaction and roll back on the first SQL error.

`Runtime.create` may be called directly by embedders. Its allocator must support
concurrent use by the Raft driver and gRPC reactor threads; the executable uses
`std.heap.smp_allocator`.
