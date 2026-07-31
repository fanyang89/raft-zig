# Development

raft-zig uses Zig 0.16.0 and mise tasks for reproducible local commands. The
project also accepts direct Zig build invocations.

## Setup

```bash
mise install
mise run build
mise run check
```

`mise install` installs the versions pinned in `mise.toml`. `check` runs Zig
format validation and the default test suite.

Direct equivalents are available:

```bash
zig build
zig build test --summary all
zig fmt --check build.zig src examples/minimal_node.zig \
  examples/raft-sqlite/build.zig examples/raft-sqlite/src tests benchmarks
```

## Test Tasks

```bash
mise run test
mise run test-release-safe
mise run test-tsan
mise run test-ubsan
mise run test-rpc
mise run test-grpc-raftor
mise run test-raft-sqlite
mise run test-upstream
mise run vopr-smoke
mise run wal-durability
```

See [Testing](testing.md) for suite boundaries, per-upstream tasks, fault
injection, and fuzzing.

## Formatting and Workflow Validation

```bash
mise run fmt
mise run fmt-check
mise run ci-lint
```

`fmt` and `fmt-check` cover the core sources, the minimal example, the
raft-sqlite build and Zig sources, tests, and benchmarks. Explicit example paths
avoid traversing nested build caches. `ci-lint` validates GitHub Actions with
actionlint.

## Build Options

| Option | Purpose |
| --- | --- |
| `-Doptimize=Debug|ReleaseSafe|ReleaseFast` | Select Zig optimization and safety mode. |
| `-Dinvariant-checks=true|false` | Override fast Raft invariant checks. |
| `-Dsanitize-thread=true` | Enable ThreadSanitizer. |
| `-Dsanitize-c=true` | Enable full C undefined behavior detection. |
| `-Dcoverage=true` | Run tests through kcov. |
| `-Dgperftools=true` | Replace the process C allocator with tcmalloc and export profiling APIs. |

gperftools currently supports Linux and is incompatible with ThreadSanitizer.
The build retains frame pointers when sanitizers or gperftools need them.

## Fuzzing

```bash
mise run fuzz-smoke
mise run fuzz-codec
mise run fuzz-wal
mise run fuzz-confchange
mise run fuzz-sim
mise run fuzz-wal-crash
```

Focused fuzz tasks use bounded runs suitable for local and CI execution.
Reproducers are stored under `.zig-cache/f/crash`.

## Benchmarks and Profiling

```bash
mise run bench-raft
mise run prepare-gperftools
mise run build-gperftools
mise run test-gperftools
mise run profile-raft
```

`bench-raft` measures the single-node Raft pipeline. `profile-raft` builds the
benchmark in ReleaseFast with gperftools, writes the CPU profile under
`.zig-cache/profiles`, and renders a text report with `pprof`.

`prepare-gperftools` requires `zigfetch`, curl, and access to the pinned source
archive. It populates Zig's package cache and the ignored `zig-pkg` directory.

## Coverage

```bash
mise run coverage
```

The task requires kcov and writes its report to `zig-out/coverage`. The GitHub
Actions workflow pins the kcov source revision and documents its Ubuntu build
dependencies.

## Project Conventions

- Keep the consensus core deterministic and free of external I/O.
- Preserve explicit allocator ownership and deterministic `deinit` paths.
- Keep Raft mutation on one event-loop thread.
- Add focused failure and ownership tests when changing persistence or callbacks.
- Update the upstream inventory when adapting or superseding an upstream case.
- Treat `src/root.zig` as the public API boundary and document compatibility changes.
