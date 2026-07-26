const std = @import("std");
const raft = @import("raft_zig");
const fault = @import("harness/fault_fs.zig");

const Fs = raft.Fs;

test "FaultFs retries interrupted and short positional I/O" {
    const allocator = std.testing.allocator;
    var fixture = try raft.FsTestFixture.init(allocator, .real);
    defer fixture.deinit();
    const path = fixture.walDir();
    const file_path = try std.fmt.allocPrintSentinel(allocator, "{s}/data", .{path}, 0);
    defer allocator.free(file_path);
    const base = fixture.fs();
    _ = try base.makeDir(path);
    const write_handle = try base.open(file_path, .write_truncate);

    var backend = fault.FaultFs.init(base);
    const fs = backend.fs();
    backend.setScript(&.{
        .{ .operation = .pwrite, .occurrence = 1, .effect = .interrupted },
        .{ .operation = .pwrite, .occurrence = 2, .effect = .{ .short = 2 } },
    });
    try fs.pwriteAll(write_handle, "abcdef", 0);
    try backend.assertConsumed();
    backend.inject(.{ .operation = .pwrite, .occurrence = 1, .effect = .{ .short = 2 } });
    try fs.pwriteAll(write_handle, "ghijkl", 6);
    try backend.assertConsumed();
    backend.inject(.{ .operation = .pwrite, .occurrence = 1, .effect = .zero });
    try std.testing.expectError(error.WriteFailed, fs.pwriteAll(write_handle, "x", 12));
    try backend.assertConsumed();
    try base.close(write_handle);

    const read_handle = try base.open(file_path, .read_only);
    defer base.close(read_handle) catch {};
    var buffer: [12]u8 = undefined;
    backend.inject(.{ .operation = .pread, .occurrence = 1, .effect = .interrupted });
    try std.testing.expectEqual(buffer.len, try fs.preadAll(read_handle, &buffer, 0));
    try std.testing.expectEqualStrings("abcdefghijkl", &buffer);
    try backend.assertConsumed();
    backend.inject(.{ .operation = .pread, .occurrence = 1, .effect = .{ .short = 3 } });
    try std.testing.expectEqual(buffer.len, try fs.preadAll(read_handle, &buffer, 0));
    try std.testing.expectEqualStrings("abcdefghijkl", &buffer);
    try backend.assertConsumed();
}

test "FaultFs metadata faults recover an atomic incarnation" {
    const allocator = std.testing.allocator;
    const Case = struct {
        operation: fault.Operation,
        occurrence: u32 = 1,
        effect: fault.Effect,
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

    for (cases) |case| {
        var fixture = try raft.FsTestFixture.init(allocator, .real);
        defer fixture.deinit();
        const path = fixture.walDir();
        const base = fixture.fs();

        var initial = try raft.WAL.open(allocator, .{ .dir = path, .fs = base });
        try std.testing.expectEqual(@as(u64, 1), try initial.reserveIncarnation());
        initial.deinit();

        var backend = fault.FaultFs.init(base);
        var wal = try raft.WAL.open(allocator, .{ .dir = path, .fs = backend.fs() });
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
        try backend.assertConsumed();
        wal.deinit();

        var recovered = try raft.WAL.open(allocator, .{ .dir = path, .fs = base });
        defer recovered.deinit();
        try std.testing.expectEqual(case.expected_incarnation, recovered.incarnation);
    }
}

test "FaultFs legacy migration recovers retryable or complete state" {
    const allocator = std.testing.allocator;
    const Case = struct {
        operation: fault.Operation,
        occurrence: u32,
        effect: fault.Effect,
    };
    const cases = [_]Case{
        .{ .operation = .pwrite, .occurrence = 1, .effect = .fail_before },
        .{ .operation = .sync_file, .occurrence = 1, .effect = .fail_before },
        .{ .operation = .rename, .occurrence = 1, .effect = .fail_before },
        .{ .operation = .sync_dir, .occurrence = 1, .effect = .fail_before },
        .{ .operation = .pwrite, .occurrence = 2, .effect = .fail_before },
        .{ .operation = .sync_file, .occurrence = 2, .effect = .fail_before },
        .{ .operation = .rename, .occurrence = 2, .effect = .fail_before },
        .{ .operation = .rename, .occurrence = 2, .effect = .fail_after },
        .{ .operation = .sync_dir, .occurrence = 2, .effect = .fail_before },
    };

    for (cases) |case| {
        var fixture = try raft.FsTestFixture.init(allocator, .real);
        defer fixture.deinit();
        const path = fixture.walDir();
        const base = fixture.fs();
        {
            var ws = try raft.WALStorage.openWithFs(allocator, path, base);
            defer ws.deinit();
            const storage = ws.asWritableStorage();
            try storage.append(allocator, &.{
                .{ .index = 1, .term = 1 },
                .{ .index = 2, .term = 1 },
            });
            try storage.setHardState(.{ .term = 1, .vote = 1, .commit = 2 });
            try storage.setConfState(allocator, .{ .voters = @constCast(&[_]u64{1}) });
            try storage.applyLocalSnapshot(allocator, .{
                .data = @constCast("legacy-state"),
                .metadata = .{
                    .index = 1,
                    .term = 1,
                    .conf_state = .{ .voters = @constCast(&[_]u64{1}) },
                },
            });
        }

        var backend = fault.FaultFs.init(base);
        var ws = try raft.WALStorage.openWithFs(allocator, path, backend.fs());
        var peers = [_]raft.PeerEndpoint{
            .{ .node_id = 1, .address = @constCast("node-1") },
        };
        const membership = raft.ClusterMembership{
            .cluster_id = .{1} ++ .{0} ** 15,
            .peers = &peers,
        };
        backend.inject(.{
            .operation = case.operation,
            .occurrence = case.occurrence,
            .effect = case.effect,
        });
        if (ws.asWritableStorage().migrateLegacyMembership(allocator, membership, 2, membership)) |_| {
            return error.ExpectedInjectedFailure;
        } else |_| {}
        try backend.assertConsumed();
        ws.deinit();

        var recovered = try raft.WALStorage.openWithFs(allocator, path, base);
        var state = try recovered.asWritableStorage().initialState(allocator);
        if (state.cluster_membership == null) {
            try recovered.asWritableStorage().migrateLegacyMembership(allocator, membership, 2, membership);
            state.deinit(allocator);
            state = try recovered.asWritableStorage().initialState(allocator);
        }
        try std.testing.expectEqual(@as(u64, 2), state.membership_index);
        try std.testing.expectEqualStrings("node-1", state.cluster_membership.?.addressOf(1).?);
        var snapshot = (try recovered.asWritableStorage().localSnapshot(allocator)).?;
        try std.testing.expect(snapshot.membership.len != 0);
        snapshot.deinit(allocator);
        state.deinit(allocator);
        recovered.deinit();
    }
}
