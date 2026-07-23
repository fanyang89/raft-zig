# Upstream Test Suites

This directory tracks behavioral tests and invariants derived from established
Raft implementations. CI uses only the committed Zig tests and manifests; it
does not fetch upstream repositories.

Each inventory entry has one status:

- `adapted`: implemented from a permissively licensed upstream test.
- `reimplemented`: independently implemented from observable behavior.
- `covered_elsewhere`: an existing raft-zig test already covers the behavior.
- `excluded`: outside the raft-zig-core-v1 scope or implementation-specific.
- `blocked`: relevant, but the product or test harness lacks a prerequisite.
- `planned`: accepted for future implementation.

etcd/raft is the primary behavioral baseline. raft-rs contributes only
meaningful deltas and historical regressions. OpenRaft contributes stateful
invariants. HashiCorp Raft behavior is reimplemented clean-room because its
source is MPL-2.0; no HashiCorp test code is copied.
