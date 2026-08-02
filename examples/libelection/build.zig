const std = @import("std");
const manifest = @import("build.zig.zon");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.option(
        std.builtin.OptimizeMode,
        "optimize",
        "Prioritize performance, safety, or binary size",
    ) orelse .ReleaseSafe;
    const raft_dependency = b.dependency("raft_zig", .{
        .target = target,
        .optimize = optimize,
        .@"sanitize-c" = false,
    });

    const options = b.addOptions();
    options.addOption([]const u8, "version", manifest.version);

    const election = b.addModule("election", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "raft_zig", .module = raft_dependency.module("raft_zig") },
            .{ .name = "election_options", .module = options.createModule() },
        },
    });

    const static_library = b.addLibrary(.{
        .name = "election",
        .root_module = election,
    });
    static_library.bundle_compiler_rt = true;
    const shared_library = b.addLibrary(.{
        .name = "election",
        .linkage = .dynamic,
        .root_module = election,
        .version = .{ .major = 1, .minor = 0, .patch = 0 },
    });
    if (target.result.os.tag == .linux) shared_library.setVersionScript(b.path("libelection.map"));
    const package_static_library = b.addSystemCommand(&.{"bash"});
    package_static_library.addFileArg(b.path("tools/package_static_library.sh"));
    package_static_library.addFileArg(static_library.getEmittedBin());
    const packaged_static_library = package_static_library.addOutputFileArg("libelection.a");
    package_static_library.addArg(b.graph.zig_exe);
    const install_static = b.addInstallFileWithDir(
        packaged_static_library,
        .lib,
        "libelection.a",
    );
    const install_shared = b.addInstallArtifact(shared_library, .{});
    const install_header = b.addInstallHeaderFile(
        b.path("include/libelection/libelection.h"),
        "libelection/libelection.h",
    );
    b.getInstallStep().dependOn(&install_static.step);
    b.getInstallStep().dependOn(&install_shared.step);
    b.getInstallStep().dependOn(&install_header.step);

    const install_c_sdk = b.step(
        "install-c-sdk",
        "Install the C header, static library, and shared library",
    );
    install_c_sdk.dependOn(&install_static.step);
    install_c_sdk.dependOn(&install_shared.step);
    install_c_sdk.dependOn(&install_header.step);

    const tests = b.addTest(.{ .root_module = election });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run libelection tests");
    test_step.dependOn(&run_tests.step);

    const c_smoke_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    c_smoke_module.addIncludePath(b.path("include"));
    c_smoke_module.addCSourceFile(.{
        .file = b.path("tests/c_api_smoke.c"),
        .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" },
    });
    c_smoke_module.linkLibrary(shared_library);
    const c_smoke = b.addExecutable(.{
        .name = "libelection-c-api-smoke",
        .root_module = c_smoke_module,
    });
    test_step.dependOn(&b.addRunArtifact(c_smoke).step);

    const example_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .link_libcpp = true,
    });
    example_module.addIncludePath(b.path("include"));
    example_module.addCSourceFile(.{
        .file = b.path("examples/election_node.c"),
        .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" },
    });
    example_module.addObjectFile(packaged_static_library);
    if (target.result.os.tag == .linux) {
        example_module.linkSystemLibrary("pthread", .{});
        example_module.linkSystemLibrary("dl", .{});
        example_module.linkSystemLibrary("rt", .{});
    }
    const example = b.addExecutable(.{
        .name = "election-node",
        .root_module = example_module,
    });
    const install_example = b.addInstallArtifact(example, .{});
    const example_step = b.step("example", "Build the C election node example");
    example_step.dependOn(&install_example.step);
    test_step.dependOn(&example.step);

    if (target.result.os.tag == .linux) {
        const installed_c_smoke = b.addSystemCommand(&.{"bash"});
        installed_c_smoke.addFileArg(b.path("tests/installed_c_smoke.sh"));
        installed_c_smoke.addArg(b.getInstallPath(.prefix, ""));
        installed_c_smoke.addFileArg(b.path("tests/c_api_smoke.c"));
        installed_c_smoke.step.dependOn(&install_static.step);
        installed_c_smoke.step.dependOn(&install_shared.step);
        installed_c_smoke.step.dependOn(&install_header.step);

        const test_installed_c_sdk = b.step(
            "test-installed-c-sdk",
            "Test the installed C SDK with the system compiler",
        );
        test_installed_c_sdk.dependOn(&installed_c_smoke.step);
    }
}
