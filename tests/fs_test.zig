const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

fn runFsContract(backend: raft.FsTestBackend) !void {
    var fixture = try raft.FsTestFixture.init(allocator, backend);
    defer fixture.deinit();
    const fs = fixture.fs();
    const dir = fixture.walDir();
    try std.testing.expect(try fs.makeDir(dir));
    try std.testing.expect(!try fs.makeDir(dir));
    try fs.syncDir(fixture.root());

    const path = try std.fmt.allocPrintSentinel(allocator, "{s}/data", .{dir}, 0);
    defer allocator.free(path);
    const renamed_path = try std.fmt.allocPrintSentinel(allocator, "{s}/renamed", .{dir}, 0);
    defer allocator.free(renamed_path);

    const write_handle = try fs.open(path, .create_exclusive);
    var write_open = true;
    defer if (write_open) fs.close(write_handle) catch {};
    try std.testing.expectError(error.OpenFailed, fs.open(path, .create_exclusive));
    try fs.pwriteAll(write_handle, "abcdef", 0);
    try std.testing.expectEqual(@as(u64, 6), try fs.fileSize(write_handle));
    try fs.truncate(write_handle, 4);
    try fs.syncFile(write_handle);
    write_open = false;
    try fs.close(write_handle);

    const read_handle = try fs.open(path, .read_only);
    defer fs.close(read_handle) catch {};
    var data: [4]u8 = undefined;
    try std.testing.expectEqual(data.len, try fs.preadAll(read_handle, &data, 0));
    try std.testing.expectEqualStrings("abcd", &data);

    var listing = try fs.listDir(allocator, dir);
    defer listing.deinit();
    try std.testing.expectEqual(@as(usize, 1), listing.entries.items.len);
    try std.testing.expectEqualStrings("data", listing.entries.items[0].name);
    try std.testing.expectEqual(raft.FsDirEntryKind.file, listing.entries.items[0].kind);

    try fs.rename(path, renamed_path);
    try fs.syncDir(dir);
    try std.testing.expectError(error.FileNotFound, fs.open(path, .read_only));
    try fs.unlink(renamed_path);
    try fs.unlink(renamed_path);
    try fs.syncDir(dir);
}

fn runWalRoundTrip(backend: raft.FsTestBackend) !void {
    var fixture = try raft.FsTestFixture.init(allocator, backend);
    defer fixture.deinit();

    var wal = try raft.WAL.open(allocator, .{ .dir = fixture.walDir(), .fs = fixture.fs() });
    var wal_open = true;
    defer if (wal_open) wal.deinit();
    try std.testing.expectEqual(@as(u64, 1), try wal.reserveIncarnation());
    wal_open = false;
    wal.deinit();

    var reopened = try raft.WAL.open(allocator, .{ .dir = fixture.walDir(), .fs = fixture.fs() });
    defer reopened.deinit();
    try std.testing.expectEqual(@as(u64, 1), reopened.incarnation);
}

test "RealFs satisfies the filesystem contract" {
    try runFsContract(.real);
}

test "TmpFs satisfies the filesystem contract" {
    try runFsContract(.tmpfs);
}

test "WAL reopens on RealFs" {
    try runWalRoundTrip(.real);
}

test "WAL reopens on TmpFs" {
    try runWalRoundTrip(.tmpfs);
}
