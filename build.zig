const std = @import("std");
const manifest = @import("build.zig.zon");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const raft_zig_options = b.addOptions();
    raft_zig_options.addOption([]const u8, "version", manifest.version);

    const raft_zig = b.addModule("raft_zig", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    raft_zig.addOptions("raft_zig_options", raft_zig_options);

    const library = b.addLibrary(.{
        .name = "raft-zig",
        .root_module = raft_zig,
    });
    b.installArtifact(library);

    const unit_tests = b.addTest(.{
        .root_module = raft_zig,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const public_api_tests = b.addTest(.{
        .name = "public-api",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/public_api_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "raft_zig", .module = raft_zig }},
        }),
    });
    const run_public_api_tests = b.addRunArtifact(public_api_tests);

    const storage_tests = b.addTest(.{
        .name = "storage",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/storage_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "raft_zig", .module = raft_zig }},
        }),
    });
    const run_storage_tests = b.addRunArtifact(storage_tests);

    const log_tests = b.addTest(.{
        .name = "log",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/log_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "raft_zig", .module = raft_zig }},
        }),
    });
    const run_log_tests = b.addRunArtifact(log_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_public_api_tests.step);
    test_step.dependOn(&run_storage_tests.step);
    test_step.dependOn(&run_log_tests.step);

    const minimal_node = addExample(b, "raft-zig-minimal-node", "examples/minimal_node.zig", raft_zig);
    b.installArtifact(minimal_node);

    const fmt_step = b.step("fmt", "Format Zig sources");
    const fmt_run = b.addSystemCommand(&.{ "zig", "fmt", "build.zig", "src", "examples", "tests" });
    fmt_step.dependOn(&fmt_run.step);

    const fmt_check_step = b.step("fmt-check", "Check Zig formatting");
    const fmt_check_run = b.addSystemCommand(&.{ "zig", "fmt", "--check", "build.zig", "src", "examples", "tests" });
    fmt_check_step.dependOn(&fmt_check_run.step);
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
    return b.addExecutable(.{
        .name = name,
        .root_module = module,
    });
}
