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

## Inventory Format

Each source has a `cases.jsonl` file with one compact JSON object per line.
Records are sorted by `path`, then `id`, and use these fields:

- `id`: stable upstream case identifier, unique within the source.
- `path`: path relative to the pinned upstream revision.
- `category`: lower-kebab-case behavior group.
- `status`: one of the statuses above.
- `target`: raft-zig source or test file providing coverage.
- `rationale`: concise reason for the classification.

`target` is required for `adapted`, `reimplemented`, and
`covered_elsewhere`; it is omitted for all other statuses. Source manifests
pin the expected record and status counts so accidental inventory changes fail
the audit tests. A `covered_elsewhere` target must directly assert the recorded
behavior; executing related code or testing a broader component is not enough.

## Coverage Snapshot

| Source | Cases | Adapted | Reimplemented | Covered | Planned | Excluded | Blocked |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| etcd/raft | 299 | 30 | 0 | 72 | 167 | 30 | 0 |
| raft-rs | 263 | 3 | 0 | 109 | 138 | 7 | 6 |
| OpenRaft | 286 | 0 | 7 | 21 | 200 | 47 | 11 |
| HashiCorp Raft | 184 | 0 | 1 | 16 | 57 | 91 | 19 |
| Total | 1032 | 33 | 8 | 218 | 562 | 175 | 36 |
