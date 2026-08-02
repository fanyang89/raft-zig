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

- One Raft group with bootstrap, learner join, promotion, removal, and readdressing
- Typed gRPC Execute, Query, Status, and Admin methods
- Atomic parameterized write batches with request ID deduplication
- Linearizable leader reads
- Durable SQLite restart, WAL suffix replay, image snapshots, and follower catch-up
- Leadership transfer and operator-triggered snapshots
- SQLite 3.53.4 amalgamation pinned by SHA3-256

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

## Dynamic Membership

Start a new node with `--join` and one or more existing members as seed peers.
Seed peers must not include the joining node ID:

```sh
zig-out/bin/raft-sqlite serve \
  --join \
  --node-id 4 \
  --cluster-id 0198f54d-5c2a-7000-8000-000000000001 \
  --api-listen 127.0.0.1:8004 \
  --raft-listen 127.0.0.1:9004 \
  --data-dir ./data/node-4 \
  --peer 1=127.0.0.1:9001
```

Add the node through the current leader, then wait for `status` to show
`promotion_ready: true` before promoting it with the same advertised address:

```sh
zig-out/bin/raft-sqlite add-learner 127.0.0.1:8001 4 127.0.0.1:9004
zig-out/bin/raft-sqlite status 127.0.0.1:8001
zig-out/bin/raft-sqlite promote 127.0.0.1:8001 4 127.0.0.1:9004
```

Membership Admin success means the change was submitted. The returned
`observed_membership_index` is the committed index before that submission. Poll
`status` until `membership_index` advances and the requested member state
appears. A joining learner may catch up by log replication or snapshot
installation.

`matched_index` and `promotion_ready` are leader observations. Followers report
zero and false for those fields.

Persisted membership is authoritative after the first start, even when the
process is restarted with `--join`. Removed node IDs are retired permanently
and cannot be reused.

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

zig-out/bin/raft-sqlite update-address 127.0.0.1:8001 4 127.0.0.1:9014
zig-out/bin/raft-sqlite status 127.0.0.1:8004
# Restart node 4 with --raft-listen 127.0.0.1:9014, then wait for catch-up.
zig-out/bin/raft-sqlite transfer-leader 127.0.0.1:8001 4
zig-out/bin/raft-sqlite status 127.0.0.1:8004
zig-out/bin/raft-sqlite remove 127.0.0.1:8004 1
zig-out/bin/raft-sqlite snapshot 127.0.0.1:8004
```

Parameter forms are `null`, `int:42`, `real:3.14`, `text:value`, and
`blob:00ff`. CLI responses are JSON. Followers return `failed_precondition`
instead of forwarding requests.

Apply an address update while the member is running, wait until that member's
status reports the new address and membership index, then restart it on the new
Raft listen address. Send membership changes and leadership transfers to the
current leader. Snapshots may be requested from any member.
Leadership transfer success means the request was accepted; poll `status` for
the new leader. Snapshot success means the local snapshot and compaction
completed.

The example API has no authentication or TLS. Keep it on loopback or a trusted
network. Add transport security and Admin authorization before deployment.

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
