const std = @import("std");
const manifest = @import("build.zig.zon");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const sanitizers: Sanitizers = .{
        .thread = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer"),
        .c = if (b.option(bool, "sanitize-c", "Enable full C undefined behavior detection")) |enabled|
            if (enabled) .full else .off
        else
            null,
    };

    const raft_zig_options = b.addOptions();
    raft_zig_options.addOption([]const u8, "version", manifest.version);
    raft_zig_options.addOption(
        bool,
        "invariant_checks",
        b.option(bool, "invariant-checks", "Enable fast Raft invariant checks") orelse
            (optimize == .Debug or optimize == .ReleaseSafe),
    );

    const raft_zig = b.addModule("raft_zig", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    applySanitizers(raft_zig, sanitizers);
    raft_zig.addOptions("raft_zig_options", raft_zig_options);

    // grpc-lite RPC backend (optional dependency).
    const grpc_dep = b.dependency("grpc_lite", .{
        .target = target,
        .optimize = optimize,
        .@"sanitize-thread" = sanitizers.thread orelse false,
        .@"sanitize-c" = sanitizers.c == .full,
    });
    const grpc_lite = grpc_dep.module("grpc_lite");
    raft_zig.addImport("grpc_lite", grpc_lite);

    const library = b.addLibrary(.{
        .name = "raft-zig",
        .root_module = raft_zig,
    });
    b.installArtifact(library);

    const unit_tests = b.addTest(.{
        .root_module = raft_zig,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
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
        const run_tests = b.addRunArtifact(tests);
        test_step.dependOn(&run_tests.step);
    }

    const upstream_specs = [_]TestSpec{
        .{ .name = "upstream-manifest", .source = "tests/upstream/source_manifest.zig" },
        .{ .name = "upstream-source-audit", .source = "tests/upstream/source_audit_test.zig" },
        .{ .name = "upstream-etcd-raft", .source = "tests/upstream/etcd_raft/suite_test.zig" },
        .{ .name = "upstream-raft-rs", .source = "tests/upstream/raft_rs/suite_test.zig" },
        .{ .name = "upstream-openraft", .source = "tests/upstream/openraft/suite_test.zig" },
        .{ .name = "upstream-hashicorp-raft", .source = "tests/upstream/hashicorp_raft/suite_test.zig" },
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
        const run_tests = b.addRunArtifact(tests);
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
        const run_vopr_smoke = b.addRunArtifact(vopr_smoke_tests);
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

    const fmt_step = b.step("fmt", "Format Zig sources");
    const fmt_run = b.addSystemCommand(&.{ "zig", "fmt", "build.zig", "src", "examples", "tests" });
    fmt_step.dependOn(&fmt_run.step);

    const fmt_check_step = b.step("fmt-check", "Check Zig formatting");
    const fmt_check_run = b.addSystemCommand(&.{ "zig", "fmt", "--check", "build.zig", "src", "examples", "tests" });
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

fn applySanitizers(module: *std.Build.Module, sanitizers: Sanitizers) void {
    module.sanitize_thread = sanitizers.thread;
    module.sanitize_c = sanitizers.c;
    if (sanitizers.enabled()) module.omit_frame_pointer = false;
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
