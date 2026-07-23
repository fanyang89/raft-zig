const std = @import("std");
const raft = @import("raft_zig");
const scripted = @import("harness/scripted_wal_fs.zig");

const Fs = raft.WalFileSystem;

fn cleanupDirectory(allocator: std.mem.Allocator, path: [:0]const u8) void {
    const fs = raft.linuxWalFileSystem();
    var listing = fs.listDir(allocator, path) catch return;
    defer listing.deinit();
    var child_buffer: [512]u8 = undefined;
    for (listing.entries.items) |entry| {
        const child = std.fmt.bufPrintZ(&child_buffer, "{s}/{s}", .{ path, entry.name }) catch continue;
        fs.unlink(child) catch {};
    }
    _ = std.os.linux.rmdir(path.ptr);
}

test "ScriptedWalFs retries interrupted and short positional I/O" {
    const allocator = std.testing.allocator;
    const path = "/tmp/raft-zig-scripted-wal-io";
    const file_path = "/tmp/raft-zig-scripted-wal-io/data";
    const base = raft.linuxWalFileSystem();
    cleanupDirectory(allocator, path);
    defer cleanupDirectory(allocator, path);
    _ = try base.makeDir(path);
    const write_handle = try base.open(file_path, .write_truncate);

    var backend = scripted.ScriptedWalFs.init(base);
    const fs = backend.fileSystem();
    backend.setScript(&.{
        .{ .operation = .pwrite, .occurrence = 1, .effect = .interrupted },
        .{ .operation = .pwrite, .occurrence = 2, .effect = .{ .short = 2 } },
    });
    try fs.pwriteAll(write_handle, "abcdef", 0);
    backend.inject(.{ .operation = .pwrite, .occurrence = 1, .effect = .{ .short = 2 } });
    try fs.pwriteAll(write_handle, "ghijkl", 6);
    backend.inject(.{ .operation = .pwrite, .occurrence = 1, .effect = .zero });
    try std.testing.expectError(error.WriteFailed, fs.pwriteAll(write_handle, "x", 12));
    try base.close(write_handle);

    const read_handle = try base.open(file_path, .read_only);
    defer base.close(read_handle) catch {};
    var buffer: [12]u8 = undefined;
    backend.inject(.{ .operation = .pread, .occurrence = 1, .effect = .interrupted });
    try std.testing.expectEqual(buffer.len, try fs.preadAll(read_handle, &buffer, 0));
    try std.testing.expectEqualStrings("abcdefghijkl", &buffer);
    backend.inject(.{ .operation = .pread, .occurrence = 1, .effect = .{ .short = 3 } });
    try std.testing.expectEqual(buffer.len, try fs.preadAll(read_handle, &buffer, 0));
    try std.testing.expectEqualStrings("abcdefghijkl", &buffer);
}

test "ScriptedWalFs metadata faults recover an atomic incarnation" {
    const allocator = std.testing.allocator;
    const Case = struct {
        operation: scripted.Operation,
        occurrence: u32 = 1,
        effect: scripted.Effect,
        expected_incarnation: u64,
        succeeds: bool = false,
    };
    const cases = [_]Case{
        .{ .operation = .open, .effect = .interrupted, .expected_incarnation = 2, .succeeds = true },
        .{ .operation = .pwrite, .effect = .interrupted, .expected_incarnation = 2, .succeeds = true },
        .{ .operation = .sync_file, .occurrence = 2, .effect = .interrupted, .expected_incarnation = 2, .succeeds = true },
        .{ .operation = .rename, .effect = .interrupted, .expected_incarnation = 2, .succeeds = true },
        .{ .operation = .sync_dir, .effect = .interrupted, .expected_incarnation = 2, .succeeds = true },
        .{ .operation = .pwrite, .effect = .{ .short = 7 }, .expected_incarnation = 2, .succeeds = true },
        .{ .operation = .open, .effect = .fail_after, .expected_incarnation = 1 },
        .{ .operation = .pwrite, .effect = .fail_before, .expected_incarnation = 1 },
        .{ .operation = .pwrite, .effect = .fail_after, .expected_incarnation = 1 },
        .{ .operation = .pwrite, .effect = .zero, .expected_incarnation = 1 },
        .{ .operation = .sync_file, .occurrence = 2, .effect = .fail_before, .expected_incarnation = 1 },
        .{ .operation = .sync_file, .occurrence = 2, .effect = .fail_after, .expected_incarnation = 1 },
        .{ .operation = .close, .effect = .interrupted, .expected_incarnation = 1 },
        .{ .operation = .close, .effect = .fail_after, .expected_incarnation = 1 },
        .{ .operation = .rename, .effect = .fail_before, .expected_incarnation = 1 },
        .{ .operation = .rename, .effect = .fail_after, .expected_incarnation = 2 },
        .{ .operation = .sync_dir, .effect = .fail_before, .expected_incarnation = 2 },
        .{ .operation = .sync_dir, .effect = .fail_after, .expected_incarnation = 2 },
    };

    for (cases, 0..) |case, case_index| {
        const path = try std.fmt.allocPrintSentinel(allocator, "/tmp/raft-zig-scripted-wal-{d}", .{case_index}, 0);
        defer allocator.free(path);
        cleanupDirectory(allocator, path);
        defer cleanupDirectory(allocator, path);
        const base = raft.linuxWalFileSystem();

        var initial = try raft.WAL.open(allocator, .{ .dir = path, .fs = base });
        try std.testing.expectEqual(@as(u64, 1), try initial.reserveIncarnation());
        initial.deinit();

        var backend = scripted.ScriptedWalFs.init(base);
        var wal = try raft.WAL.open(allocator, .{ .dir = path, .fs = backend.fileSystem() });
        backend.inject(.{
            .operation = case.operation,
            .occurrence = case.occurrence,
            .effect = case.effect,
        });
        const result = wal.reserveIncarnation();
        if (case.succeeds) {
            try std.testing.expectEqual(@as(u64, 2), try result);
        } else {
            if (result) |_| return error.ExpectedInjectedFailure else |_| {}
        }
        wal.deinit();

        var recovered = try raft.WAL.open(allocator, .{ .dir = path, .fs = base });
        defer recovered.deinit();
        try std.testing.expectEqual(case.expected_incarnation, recovered.incarnation);
    }
}
