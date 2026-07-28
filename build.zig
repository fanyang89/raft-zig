const std = @import("std");
const manifest = @import("build.zig.zon");
const grpc_lite_build = @import("grpc_lite");

pub fn grpcLiteDependency(
    dependency: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Dependency {
    return dependency.builder.dependency("grpc_lite", .{
        .target = target,
        .optimize = optimize,
        .protobuf = false,
    });
}

pub fn createProtocStep(
    dependency: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    options: grpc_lite_build.protobuf_codegen.RunProtocStep.Options,
) *grpc_lite_build.protobuf_codegen.RunProtocStep {
    return grpc_lite_build.createProtocStep(
        grpcLiteDependency(dependency, target, optimize),
        target,
        optimize,
        options,
    );
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    var coverage: Coverage = .{
        .enabled = b.option(bool, "coverage", "Generate test coverage with kcov") orelse false,
    };
    const sanitizers: Sanitizers = .{
        .thread = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer"),
        .c = if (b.option(bool, "sanitize-c", "Enable full C undefined behavior detection")) |enabled|
            if (enabled) .full else .off
        else
            null,
    };
    const enable_gperftools = b.option(
        bool,
        "gperftools",
        "Use tcmalloc and expose CPU and heap profiling APIs",
    ) orelse false;
    if (enable_gperftools and target.result.os.tag != .linux) {
        @panic("gperftools support is currently limited to Linux");
    }
    if (enable_gperftools and sanitizers.thread == true) {
        @panic("gperftools/tcmalloc is incompatible with ThreadSanitizer");
    }

    const raft_zig_options = b.addOptions();
    raft_zig_options.addOption([]const u8, "version", manifest.version);
    raft_zig_options.addOption(
        bool,
        "invariant_checks",
        b.option(bool, "invariant-checks", "Enable fast Raft invariant checks") orelse
            (optimize == .Debug or optimize == .ReleaseSafe),
    );
    raft_zig_options.addOption(bool, "sanitize_thread", sanitizers.thread orelse false);

    const raft_zig = b.addModule("raft_zig", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    applySanitizers(raft_zig, sanitizers);
    raft_zig.addOptions("raft_zig_options", raft_zig_options);

    const crc32c_dep = b.dependency("crc32c", .{});
    const crc32c = b.createModule(.{
        .root_source_file = b.path("src/crc32c.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    applySanitizers(crc32c, sanitizers);
    addCrc32c(crc32c, crc32c_dep, target);
    crc32c.link_libcpp = true;
    raft_zig.addImport("crc32c", crc32c);

    // grpc-lite RPC backend (optional dependency).
    const grpc_dep = b.dependency("grpc_lite", .{
        .target = target,
        .optimize = optimize,
        .@"sanitize-thread" = sanitizers.thread orelse false,
        .@"sanitize-c" = sanitizers.c == .full,
        .gperftools = enable_gperftools,
        .protobuf = false,
    });
    const grpc_lite = grpc_dep.module("grpc_lite");
    const nanozlog = b.dependency("nanozlog", .{
        .target = target,
        .optimize = optimize,
    }).module("nanozlog");
    grpc_lite.addImport("nanozlog", nanozlog);
    raft_zig.addImport("grpc_lite", grpc_lite);

    const raft_zig_gperftools = if (enable_gperftools)
        b.addModule("raft_zig_gperftools", .{
            .root_source_file = b.path("src/gperftools.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft_zig", .module = raft_zig },
                .{ .name = "grpc_lite_gperftools", .module = grpc_dep.module("grpc_lite_gperftools") },
            },
        })
    else
        null;
    if (enable_gperftools) {
        raft_zig.omit_frame_pointer = false;
        applySanitizers(raft_zig_gperftools.?, sanitizers);
        raft_zig_gperftools.?.omit_frame_pointer = false;
    }

    const library = b.addLibrary(.{
        .name = "raft-zig",
        .root_module = raft_zig,
    });
    b.installArtifact(library);

    const unit_tests = b.addTest(.{
        .root_module = raft_zig,
    });
    const run_unit_tests = addTestRun(b, unit_tests, &coverage);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
    const crc32c_tests = b.addTest(.{
        .name = "crc32c",
        .root_module = crc32c,
    });
    test_step.dependOn(&addTestRun(b, crc32c_tests, &coverage).step);
    if (raft_zig_gperftools) |gperftools| {
        const gperftools_test_module = b.createModule(.{
            .root_source_file = b.path("tests/gperftools_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "raft_zig_gperftools", .module = gperftools }},
        });
        applySanitizers(gperftools_test_module, sanitizers);
        gperftools_test_module.omit_frame_pointer = false;
        const gperftools_tests = b.addTest(.{
            .name = "gperftools-integration",
            .root_module = gperftools_test_module,
        });
        test_step.dependOn(&addTestRun(b, gperftools_tests, &coverage).step);
    }
    const rpc_test_step = b.step("test-rpc", "Run grpc transport tests");
    const grpc_raftor_test_step = b.step("test-grpc-raftor", "Run grpc Raftor integration tests");
    const test_specs = [_]TestSpec{
        .{ .name = "public-api", .source = "tests/public_api_test.zig" },
        .{ .name = "storage", .source = "tests/storage_test.zig" },
        .{ .name = "log", .source = "tests/log_test.zig" },
        .{ .name = "progress", .source = "tests/progress_test.zig" },
        .{ .name = "quorum", .source = "tests/quorum_test.zig" },
        .{ .name = "confchange", .source = "tests/confchange_test.zig" },
        .{ .name = "raft", .source = "tests/raft_test.zig" },
        .{ .name = "raw_node", .source = "tests/raw_node_test.zig" },
        .{ .name = "raftor", .source = "tests/raftor_test.zig" },
        .{ .name = "multi_node", .source = "tests/multi_node_test.zig" },
        .{ .name = "raftor_multi_node", .source = "tests/raftor_multi_node_test.zig" },
        .{ .name = "figure8", .source = "tests/figure8_test.zig" },
        .{ .name = "raft_snap", .source = "tests/raft_snap_test.zig" },
        .{ .name = "inflights", .source = "tests/inflights_test.zig" },
        .{ .name = "raft_paper", .source = "tests/raft_paper_test.zig" },
        .{ .name = "rpc", .source = "tests/rpc_test.zig" },
        .{ .name = "grpc-raftor", .source = "tests/grpc_raftor_test.zig" },
        .{ .name = "simulation", .source = "tests/simulation_test.zig" },
        .{ .name = "fs", .source = "tests/fs_test.zig" },
        .{ .name = "wal-fault", .source = "tests/wal_fault_test.zig" },
    };
    for (test_specs) |spec| {
        const module = b.createModule(.{
            .root_source_file = b.path(spec.source),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "raft_zig", .module = raft_zig }},
        });
        applySanitizers(module, sanitizers);
        const tests = b.addTest(.{ .name = spec.name, .root_module = module });
        const run_tests = addTestRun(b, tests, &coverage);
        test_step.dependOn(&run_tests.step);
        if (std.mem.eql(u8, spec.name, "rpc")) rpc_test_step.dependOn(&run_tests.step);
        if (std.mem.eql(u8, spec.name, "grpc-raftor")) grpc_raftor_test_step.dependOn(&run_tests.step);
    }

    const upstream_specs = [_]TestSpec{
        .{ .name = "upstream-manifest", .source = "tests/upstream/source_manifest.zig" },
        .{ .name = "upstream-source-audit", .source = "tests/upstream/source_audit_test.zig" },
        .{ .name = "upstream-etcd-raft", .source = "tests/upstream/etcd_raft/suite_test.zig" },
        .{ .name = "upstream-raft-rs", .source = "tests/upstream/raft_rs/suite_test.zig" },
        .{ .name = "upstream-openraft", .source = "tests/upstream/openraft/suite_test.zig" },
        .{ .name = "upstream-hashicorp-raft", .source = "tests/upstream/hashicorp_raft/suite_test.zig" },
        .{ .name = "upstream-dragonboat", .source = "tests/upstream/dragonboat/suite_test.zig" },
    };
    const upstream_manifest = b.createModule(.{
        .root_source_file = b.path("tests/upstream/source_manifest.zig"),
        .target = target,
        .optimize = optimize,
    });
    const upstream_network = b.createModule(.{
        .root_source_file = b.path("tests/harness/network.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "raft_zig", .module = raft_zig }},
    });
    const upstream_step = b.step("test-upstream", "Run all adapted upstream test suites");
    const upstream_source_steps = [_]*std.Build.Step{
        upstream_step,
        upstream_step,
        b.step("test-upstream-etcd-raft", "Run adapted etcd/raft tests"),
        b.step("test-upstream-raft-rs", "Run adapted raft-rs tests"),
        b.step("test-upstream-openraft", "Run adapted OpenRaft tests"),
        b.step("test-upstream-hashicorp-raft", "Run clean-room HashiCorp Raft tests"),
        b.step("test-upstream-dragonboat", "Run adapted Dragonboat tests"),
    };

    for (upstream_specs, upstream_source_steps) |spec, source_step| {
        const module = b.createModule(.{
            .root_source_file = b.path(spec.source),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft_zig", .module = raft_zig },
                .{ .name = "upstream_manifest", .module = upstream_manifest },
                .{ .name = "raft_test_network", .module = upstream_network },
            },
        });
        applySanitizers(module, sanitizers);
        const tests = b.addTest(.{ .name = spec.name, .root_module = module });
        const run_tests = addTestRun(b, tests, &coverage);
        source_step.dependOn(&run_tests.step);
        upstream_step.dependOn(&run_tests.step);
        test_step.dependOn(&run_tests.step);
    }

    const vopr_smoke_step = b.step("vopr-smoke", "Run Marionette integration smoke tests");
    const wal_durability_step = b.step("wal-durability", "Run Marionette WAL durability tests");
    if (b.lazyDependency("marionette", .{
        .target = target,
        .optimize = optimize,
    })) |marionette_dep| {
        const vopr_smoke_module = b.createModule(.{
            .root_source_file = b.path("tests/vopr/smoke_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft_zig", .module = raft_zig },
                .{ .name = "marionette", .module = marionette_dep.module("marionette") },
            },
        });
        applySanitizers(vopr_smoke_module, sanitizers);
        const vopr_smoke_tests = b.addTest(.{
            .name = "vopr-smoke",
            .root_module = vopr_smoke_module,
        });
        const run_vopr_smoke = addTestRun(b, vopr_smoke_tests, &coverage);
        vopr_smoke_step.dependOn(&run_vopr_smoke.step);
        test_step.dependOn(&run_vopr_smoke.step);

        const wal_durability_module = b.createModule(.{
            .root_source_file = b.path("tests/vopr/wal_fs_adapter.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "raft_zig", .module = raft_zig },
                .{ .name = "marionette", .module = marionette_dep.module("marionette") },
            },
        });
        applySanitizers(wal_durability_module, sanitizers);
        const wal_durability_tests = b.addTest(.{
            .name = "wal-durability",
            .root_module = wal_durability_module,
        });
        const run_wal_durability = b.addRunArtifact(wal_durability_tests);
        wal_durability_step.dependOn(&run_wal_durability.step);
        const fuzz_wal_crash_step = b.step("fuzz-wal-crash", "Fuzz WAL crash recovery on Marionette SimDisk");
        fuzz_wal_crash_step.dependOn(&run_wal_durability.step);
    }

    const fuzz_smoke_step = b.step("fuzz-smoke", "Run fuzz corpus smoke tests");
    const fuzz_specs = [_]FuzzSpec{
        .{ .name = "codec", .source = "src/codec.zig" },
        .{ .name = "wal", .source = "src/wal.zig" },
        .{ .name = "confchange", .source = "src/core/util.zig" },
    };
    for (fuzz_specs) |spec| {
        const module = b.createModule(.{
            .root_source_file = b.path(spec.source),
            .target = target,
            .optimize = optimize,
            .imports = if (std.mem.eql(u8, spec.name, "wal"))
                &.{
                    .{ .name = "crc32c", .module = crc32c },
                    .{ .name = "grpc_lite", .module = grpc_lite },
                }
            else if (std.mem.eql(u8, spec.name, "confchange"))
                &.{.{ .name = "crc32c", .module = crc32c }}
            else
                &.{},
        });
        const tests = b.addTest(.{
            .name = b.fmt("fuzz-{s}", .{spec.name}),
            .root_module = module,
        });
        const run_tests = b.addRunArtifact(tests);
        const fuzz_step = b.step(b.fmt("fuzz-{s}", .{spec.name}), b.fmt("Fuzz {s}", .{spec.name}));
        fuzz_step.dependOn(&run_tests.step);
        fuzz_smoke_step.dependOn(&run_tests.step);
    }

    const simulation_fuzz_module = b.createModule(.{
        .root_source_file = b.path("tests/simulation_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "raft_zig", .module = raft_zig }},
    });
    applySanitizers(simulation_fuzz_module, sanitizers);
    const simulation_fuzz_tests = b.addTest(.{
        .name = "fuzz-sim",
        .root_module = simulation_fuzz_module,
    });
    const run_simulation_fuzz = b.addRunArtifact(simulation_fuzz_tests);
    const simulation_fuzz_step = b.step("fuzz-sim", "Fuzz deterministic cluster simulation");
    simulation_fuzz_step.dependOn(&run_simulation_fuzz.step);
    fuzz_smoke_step.dependOn(&run_simulation_fuzz.step);

    const minimal_node = addExample(b, "raft-zig-minimal-node", "examples/minimal_node.zig", raft_zig);
    b.installArtifact(minimal_node);

    const raft_benchmark_module = b.createModule(.{
        .root_source_file = b.path("benchmarks/raft.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "raft_zig", .module = raft_zig }},
    });
    applySanitizers(raft_benchmark_module, sanitizers);
    raft_benchmark_module.omit_frame_pointer = raft_zig.omit_frame_pointer;
    const raft_benchmark = b.addExecutable(.{
        .name = "raft-zig-bench-raft",
        .root_module = raft_benchmark_module,
    });
    const run_raft_benchmark = b.addRunArtifact(raft_benchmark);
    if (b.args) |args| run_raft_benchmark.addArgs(args);
    const raft_benchmark_step = b.step("bench-raft", "Benchmark the single-node Raft pipeline");
    raft_benchmark_step.dependOn(&run_raft_benchmark.step);
    const install_raft_benchmark = b.addInstallArtifact(raft_benchmark, .{});
    const build_raft_benchmark_step = b.step("build-bench-raft", "Build the Raft benchmark");
    build_raft_benchmark_step.dependOn(&install_raft_benchmark.step);

    const fmt_step = b.step("fmt", "Format Zig sources");
    const fmt_run = b.addSystemCommand(&.{ "zig", "fmt", "build.zig", "src", "examples", "tests", "benchmarks" });
    fmt_step.dependOn(&fmt_run.step);

    const fmt_check_step = b.step("fmt-check", "Check Zig formatting");
    const fmt_check_run = b.addSystemCommand(&.{ "zig", "fmt", "--check", "build.zig", "src", "examples", "tests", "benchmarks" });
    fmt_check_step.dependOn(&fmt_check_run.step);
}

const Sanitizers = struct {
    thread: ?bool,
    c: ?std.zig.SanitizeC,

    fn enabled(self: Sanitizers) bool {
        return self.thread == true or self.c == .full;
    }
};

const TestSpec = struct {
    name: []const u8,
    source: []const u8,
};

const FuzzSpec = struct {
    name: []const u8,
    source: []const u8,
};

const Coverage = struct {
    enabled: bool,
    previous_run: ?*std.Build.Step = null,
};

fn addTestRun(
    b: *std.Build,
    tests: *std.Build.Step.Compile,
    coverage: *Coverage,
) *std.Build.Step.Run {
    if (coverage.enabled) {
        tests.use_llvm = true;
        tests.setExecCmd(&.{
            "kcov",
            "--include-path=src",
            "zig-out/coverage",
            null,
        });
    }
    const run = b.addRunArtifact(tests);
    if (coverage.enabled) {
        if (coverage.previous_run) |previous| run.step.dependOn(previous);
        coverage.previous_run = &run.step;
    }
    return run;
}

fn applySanitizers(module: *std.Build.Module, sanitizers: Sanitizers) void {
    module.sanitize_thread = sanitizers.thread;
    module.sanitize_c = sanitizers.c;
    if (sanitizers.enabled()) module.omit_frame_pointer = false;
}

fn addCrc32c(
    module: *std.Build.Module,
    dependency: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
) void {
    const b = module.owner;
    const arch = target.result.cpu.arch;
    const is_x86 = arch == .x86 or arch == .x86_64;
    const is_aarch64 = arch == .aarch64 or arch == .aarch64_be;
    const config_header = b.addConfigHeader(.{
        .style = .{ .cmake = dependency.path("src/crc32c_config.h.in") },
        .include_path = "crc32c/crc32c_config.h",
    }, .{
        .BYTE_ORDER_BIG_ENDIAN = arch.endian() == .big,
        .HAVE_BUILTIN_PREFETCH = true,
        .HAVE_MM_PREFETCH = is_x86,
        .HAVE_SSE42 = is_x86,
        .HAVE_ARM64_CRC32C = is_aarch64,
        .HAVE_STRONG_GETAUXVAL = is_aarch64 and target.result.os.tag == .linux,
        .HAVE_WEAK_GETAUXVAL = false,
        .CRC32C_TESTS_BUILT_WITH_GLOG = false,
    });
    module.addConfigHeader(config_header);
    module.addIncludePath(dependency.path("include"));
    module.addIncludePath(dependency.path("src"));
    module.addCSourceFiles(.{
        .root = dependency.path(""),
        .files = &.{
            "src/crc32c.cc",
            "src/crc32c_portable.cc",
        },
        .flags = &.{
            "-fno-exceptions",
            "-fno-rtti",
            "-fno-sanitize-coverage=inline-8bit-counters,pc-table,trace-cmp",
        },
    });
    if (is_x86) {
        module.addCSourceFile(.{
            .file = dependency.path("src/crc32c_sse42.cc"),
            .flags = &.{
                "-fno-exceptions",
                "-fno-rtti",
                "-fno-sanitize-coverage=inline-8bit-counters,pc-table,trace-cmp",
                "-Xclang",
                "-target-feature",
                "-Xclang",
                "+sse4.2",
                "-Xclang",
                "-target-feature",
                "-Xclang",
                "+crc32",
            },
        });
    }
    if (is_aarch64) {
        module.addCSourceFile(.{
            .file = dependency.path("src/crc32c_arm64.cc"),
            .flags = &.{
                "-fno-exceptions",
                "-fno-rtti",
                "-fno-sanitize-coverage=inline-8bit-counters,pc-table,trace-cmp",
                "-Xclang",
                "-target-feature",
                "-Xclang",
                "+crc",
                "-Xclang",
                "-target-feature",
                "-Xclang",
                "+aes",
            },
        });
    }
}

fn addExample(
    b: *std.Build,
    name: []const u8,
    source: []const u8,
    raft_zig: *std.Build.Module,
) *std.Build.Step.Compile {
    const module = b.createModule(.{
        .root_source_file = b.path(source),
        .target = raft_zig.resolved_target,
        .optimize = raft_zig.optimize,
        .imports = &.{.{ .name = "raft_zig", .module = raft_zig }},
    });
    module.sanitize_thread = raft_zig.sanitize_thread;
    module.sanitize_c = raft_zig.sanitize_c;
    module.omit_frame_pointer = raft_zig.omit_frame_pointer;
    return b.addExecutable(.{
        .name = name,
        .root_module = module,
    });
}
