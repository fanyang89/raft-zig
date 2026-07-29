# Testing

raft-zig combines direct unit tests with integration, simulation, fault,
sanitizer, fuzz, and upstream-derived behavioral suites.

## Default Suite

Run the same formatting and default test entry point used for local validation:

```bash
mise run check
```

`check` runs `fmt-check` followed by `zig build test --summary all`. The default
test build includes:

- module unit tests
- public API and storage-contract tests
- consensus, log, quorum, configuration, RawNode, and Raftor tests
- in-process and real grpc-lite multi-node integration tests
- deterministic cluster simulation
- filesystem and WAL fault tests
- adapted upstream suites and manifest audits
- Marionette integration smoke tests

It does not run sanitizer variants, extended WAL crash fuzzing, coverage, or
profiling.

## Focused Tests

```bash
mise run test-rpc
mise run test-grpc-raftor
mise run test-upstream
mise run test-upstream-etcd-raft
mise run test-upstream-raft-rs
mise run test-upstream-openraft
mise run test-upstream-hashicorp-raft
mise run test-upstream-dragonboat
```

Every task forwards extra arguments to the underlying Zig build command.

## Build Modes and Sanitizers

```bash
mise run test-release-safe
mise run test-tsan
mise run test-ubsan
```

CI runs Debug and ReleaseSafe on Linux x86_64 and arm64. TSan and C undefined
behavior detection run as separate Linux jobs.

Fast Raft invariant checks are enabled by default in Debug and ReleaseSafe.
Override them explicitly with `-Dinvariant-checks=false` or
`-Dinvariant-checks=true` when isolating invariant-check behavior.

## Simulation and Fault Injection

The deterministic network harness checks election safety, committed-prefix
agreement, and convergence while varying message delivery and partitions.

Marionette-backed VOPR tests exercise cluster behavior and filesystem crash
boundaries:

```bash
mise run vopr-smoke
mise run wal-durability
mise run fuzz-wal-crash
```

`wal-durability` runs focused simulated-disk durability cases.
`fuzz-wal-crash` runs the crash-recovery target for the task's bounded corpus.

## Fuzzing

```bash
mise run fuzz-smoke
mise run fuzz-codec
mise run fuzz-wal
mise run fuzz-confchange
mise run fuzz-sim
```

The focused tasks use `scripts/run-fuzz.sh` from a source checkout to run bounded
iterations. A Zig fuzz reproducer written to `.zig-cache/f/crash` causes a
non-zero exit so CI cannot silently accept a discovered crash.

## Upstream Behavioral Inventory

[`tests/upstream/README.md`](../tests/upstream/README.md) is the authoritative
inventory of cases derived from etcd/raft, raft-rs, OpenRaft, HashiCorp Raft,
and Dragonboat. It records whether each pinned upstream case is adapted,
reimplemented, covered elsewhere, planned, excluded, or blocked.

The committed manifests and source-audit tests prevent inventory counts or
source policy from drifting silently. `covered_elsewhere` requires a direct
behavioral assertion, not merely execution of related code.

Do not copy inventory totals into other documents. Link the authoritative table
so planned and implemented counts remain current.

## Coverage

```bash
mise run coverage
```

Coverage uses `kcov` and writes output under `zig-out/coverage`. Install kcov
and its platform dependencies first; the CI workflow contains the pinned build
procedure used by the project.

## Historical Audits

Date-stamped reports under [`docs/reports`](reports/) describe the repository at
a point in time. They are not current status documents. Always verify findings
against the current tests and upstream inventory before acting on a report.
