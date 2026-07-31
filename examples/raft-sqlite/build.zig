const std = @import("std");
const raft_build = @import("raft_zig");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const raft_dependency = b.dependency("raft_zig", .{
        .target = target,
        .optimize = optimize,
    });
    const grpc_dependency = raft_build.grpcLiteDependency(raft_dependency, target, optimize);
    const grpc_module = grpc_dependency.module("grpc_lite");
    const protobuf_module = grpc_module.import_table.get("protobuf").?;
    const grpc_protobuf_module = b.createModule(.{
        .root_source_file = grpc_dependency.path("src/protobuf_adapter.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "grpc_lite", .module = grpc_module },
            .{ .name = "protobuf", .module = protobuf_module },
        },
    });

    const generate_proto = raft_build.createProtocStep(raft_dependency, target, optimize, .{
        .destination_directory = b.path(".zig-cache/generated"),
        .source_files = &.{b.path("proto/raft/sqlite/v1/database.proto")},
        .include_directories = &.{b.path("proto")},
    });
    const generate_proto_step = b.step("gen-proto", "Generate Zig protobuf sources");
    generate_proto_step.dependOn(&generate_proto.step);

    const database_proto = b.createModule(.{
        .root_source_file = b.path(".zig-cache/generated/raft/sqlite/v1.pb.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "protobuf", .module = protobuf_module }},
    });

    const sqlite_c = b.createModule(.{
        .root_source_file = b.path("src/sqlite_c.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    sqlite_c.addIncludePath(b.path("vendor/sqlite"));
    sqlite_c.addCSourceFile(.{
        .file = b.path("vendor/sqlite/sqlite3.c"),
        .flags = &.{
            "-std=c11",
            "-DSQLITE_THREADSAFE=1",
            "-DSQLITE_DEFAULT_FOREIGN_KEYS=1",
            "-DSQLITE_DQS=0",
            "-DSQLITE_OMIT_LOAD_EXTENSION",
            "-DSQLITE_ENABLE_API_ARMOR",
            "-DSQLITE_TEMP_STORE=3",
            "-DSQLITE_MAX_LENGTH=67108864",
            "-DSQLITE_MAX_SQL_LENGTH=1048576",
        },
    });

    const raft_sqlite = b.addModule("raft_sqlite", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "database_proto", .module = database_proto },
            .{ .name = "grpc_lite", .module = grpc_module },
            .{ .name = "grpc_lite_protobuf", .module = grpc_protobuf_module },
            .{ .name = "raft_zig", .module = raft_dependency.module("raft_zig") },
            .{ .name = "sqlite_c", .module = sqlite_c },
        },
    });

    const library = b.addLibrary(.{ .name = "raft-sqlite", .root_module = raft_sqlite });
    library.step.dependOn(&generate_proto.step);
    b.installArtifact(library);

    const executable_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "raft_sqlite", .module = raft_sqlite }},
    });
    const executable = b.addExecutable(.{ .name = "raft-sqlite", .root_module = executable_module });
    executable.step.dependOn(&generate_proto.step);
    b.installArtifact(executable);

    const tests = b.addTest(.{ .root_module = raft_sqlite });
    tests.step.dependOn(&generate_proto.step);
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit and integration tests");
    test_step.dependOn(&run_tests.step);

    const run_executable = b.addRunArtifact(executable);
    if (b.args) |args| run_executable.addArgs(args);
    const run_step = b.step("run", "Run raft-sqlite");
    run_step.dependOn(&run_executable.step);

    const fmt_step = b.step("fmt", "Format Zig sources");
    const fmt_run = b.addSystemCommand(&.{ "zig", "fmt", "build.zig", "src" });
    fmt_step.dependOn(&fmt_run.step);

    const fmt_check_step = b.step("fmt-check", "Check Zig formatting");
    const fmt_check_run = b.addSystemCommand(&.{ "zig", "fmt", "--check", "build.zig", "src" });
    fmt_check_step.dependOn(&fmt_check_run.step);
}
