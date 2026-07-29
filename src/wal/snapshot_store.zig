const std = @import("std");
const fs_mod = @import("../fs.zig");
const fs_testing = @import("../fs/testing.zig");

const types = @import("../core/types.zig");
const cluster_membership_mod = @import("../cluster_membership.zig");

const Snapshot = types.Snapshot;
const Crc32Iscsi = std.hash.crc.Crc32Iscsi;

const snapshot_magic: u32 = 0x534E4150;
const format_version_v1: u32 = 1;
const format_version: u32 = 2;
const header_size: usize = 64;
// v2 stores the membership blob length in the v1 reserved bytes 12..16.
const membership_length_offset: usize = 12;

pub const SnapshotStore = struct {
    allocator: std.mem.Allocator,
    dir: [:0]u8,
    fs: fs_mod.Fs,

    pub fn init(allocator: std.mem.Allocator, fs: fs_mod.Fs, dir: [:0]const u8) !SnapshotStore {
        return .{
            .allocator = allocator,
            .dir = try allocator.dupeSentinel(u8, dir, 0),
            .fs = fs,
        };
    }

    pub fn deinit(self: *SnapshotStore) void {
        self.allocator.free(self.dir);
        self.* = undefined;
    }

    pub fn save(self: *SnapshotStore, snapshot: Snapshot) !void {
        if (snapshot.metadata.index == 0) return error.MetadataCorrupt;
        try validateMembership(self.allocator, snapshot);
        const data = try encode(self.allocator, snapshot);
        defer self.allocator.free(data);
        const path = try makePath(self.allocator, self.dir, snapshot.metadata.index, snapshot.metadata.term, false);
        defer self.allocator.free(path);
        const tmp_path = try makePath(self.allocator, self.dir, snapshot.metadata.index, snapshot.metadata.term, true);
        defer self.allocator.free(tmp_path);

        const fd = try self.fs.open(tmp_path, .write_truncate);
        var is_open = true;
        errdefer {
            if (is_open) self.fs.close(fd) catch {};
            self.fs.unlink(tmp_path) catch {};
        }
        try self.fs.pwriteAll(fd, data, 0);
        try self.fs.syncFile(fd);
        const close_result = self.fs.close(fd);
        is_open = false;
        try close_result;
        try self.fs.rename(tmp_path, path);
        try self.fs.syncDir(self.dir);
    }

    pub fn load(self: *SnapshotStore, index: u64, term: u64) !Snapshot {
        const path = try makePath(self.allocator, self.dir, index, term, false);
        defer self.allocator.free(path);
        const fd = try self.fs.open(path, .read_only);
        defer self.fs.close(fd) catch {};
        const size = std.math.cast(usize, try self.fs.fileSize(fd)) orelse return error.StatFailed;
        const data = try self.allocator.alloc(u8, size);
        defer self.allocator.free(data);
        if (try self.fs.preadAll(fd, data, 0) != data.len) return error.ReadFailed;
        var snapshot = try decode(self.allocator, data);
        errdefer snapshot.deinit(self.allocator);
        if (snapshot.metadata.index != index or snapshot.metadata.term != term) return error.MetadataCorrupt;
        return snapshot;
    }

    pub fn remove(self: *SnapshotStore, index: u64, term: u64) !void {
        if (index == 0) return;
        const path = try makePath(self.allocator, self.dir, index, term, false);
        defer self.allocator.free(path);
        try self.fs.unlink(path);
        try self.fs.syncDir(self.dir);
    }
};

pub fn removeFiles(allocator: std.mem.Allocator, fs: fs_mod.Fs, dir: [:0]const u8) void {
    var listing = fs.listDir(allocator, dir) catch return;
    defer listing.deinit();
    for (listing.entries.items) |entry| {
        if (isSnapshotFilename(entry.name)) {
            const path = std.fmt.allocPrintSentinel(allocator, "{s}/{s}", .{ dir, entry.name }, 0) catch return;
            fs.unlink(path) catch {};
            allocator.free(path);
        }
    }
}

fn encode(allocator: std.mem.Allocator, snapshot: Snapshot) ![]u8 {
    const voters_len = std.math.cast(u32, snapshot.metadata.conf_state.voters.len) orelse return error.MetadataCorrupt;
    const learners_len = std.math.cast(u32, snapshot.metadata.conf_state.learners.len) orelse return error.MetadataCorrupt;
    const outgoing_len = std.math.cast(u32, snapshot.metadata.conf_state.voters_outgoing.len) orelse return error.MetadataCorrupt;
    const next_len = std.math.cast(u32, snapshot.metadata.conf_state.learners_next.len) orelse return error.MetadataCorrupt;
    const membership_len = std.math.cast(u32, snapshot.membership.len) orelse return error.MetadataCorrupt;
    const data_len = std.math.cast(u64, snapshot.data.len) orelse return error.MetadataCorrupt;
    var total = header_size;
    for ([_]usize{
        snapshot.metadata.conf_state.voters.len,
        snapshot.metadata.conf_state.learners.len,
        snapshot.metadata.conf_state.voters_outgoing.len,
        snapshot.metadata.conf_state.learners_next.len,
    }) |count| total = std.math.add(usize, total, std.math.mul(usize, count, 8) catch return error.MetadataCorrupt) catch return error.MetadataCorrupt;
    total = std.math.add(usize, total, snapshot.membership.len) catch return error.MetadataCorrupt;
    total = std.math.add(usize, total, snapshot.data.len) catch return error.MetadataCorrupt;

    const data = try allocator.alloc(u8, total);
    @memset(data, 0);
    std.mem.writeInt(u32, data[0..4], snapshot_magic, .little);
    std.mem.writeInt(u32, data[4..8], format_version, .little);
    std.mem.writeInt(u32, data[membership_length_offset..][0..4], membership_len, .little);
    std.mem.writeInt(u64, data[16..24], snapshot.metadata.index, .little);
    std.mem.writeInt(u64, data[24..32], snapshot.metadata.term, .little);
    std.mem.writeInt(u32, data[32..36], voters_len, .little);
    std.mem.writeInt(u32, data[36..40], learners_len, .little);
    std.mem.writeInt(u32, data[40..44], outgoing_len, .little);
    std.mem.writeInt(u32, data[44..48], next_len, .little);
    data[48] = @intFromBool(snapshot.metadata.conf_state.auto_leave);
    std.mem.writeInt(u64, data[56..64], data_len, .little);

    var offset = header_size;
    for ([_][]const u64{
        snapshot.metadata.conf_state.voters,
        snapshot.metadata.conf_state.learners,
        snapshot.metadata.conf_state.voters_outgoing,
        snapshot.metadata.conf_state.learners_next,
    }) |ids| {
        for (ids) |id| {
            std.mem.writeInt(u64, data[offset..][0..8], id, .little);
            offset += 8;
        }
    }
    @memcpy(data[offset .. offset + snapshot.membership.len], snapshot.membership);
    offset += snapshot.membership.len;
    @memcpy(data[offset..], snapshot.data);
    std.mem.writeInt(u32, data[8..12], Crc32Iscsi.hash(data[12..]), .little);
    return data;
}

fn decode(allocator: std.mem.Allocator, data: []const u8) !Snapshot {
    if (data.len < header_size) return error.MetadataCorrupt;
    if (std.mem.readInt(u32, data[0..4], .little) != snapshot_magic) return error.MetadataCorrupt;
    const version = std.mem.readInt(u32, data[4..8], .little);
    if (version != format_version_v1 and version != format_version) return error.MetadataCorrupt;
    if (Crc32Iscsi.hash(data[12..]) != std.mem.readInt(u32, data[8..12], .little)) return error.MetadataCorrupt;
    if (data[48] > 1 or !std.mem.allEqual(u8, data[49..56], 0)) return error.MetadataCorrupt;
    const membership_len: usize = std.mem.readInt(u32, data[membership_length_offset..][0..4], .little);
    if (version == format_version_v1 and membership_len != 0) return error.MetadataCorrupt;

    var result = Snapshot{ .metadata = .{
        .index = std.mem.readInt(u64, data[16..24], .little),
        .term = std.mem.readInt(u64, data[24..32], .little),
        .conf_state = .{ .auto_leave = data[48] == 1 },
    } };
    errdefer result.deinit(allocator);
    if (result.metadata.index == 0) return error.MetadataCorrupt;

    var offset = header_size;
    result.metadata.conf_state.voters = try readIds(allocator, data, &offset, std.mem.readInt(u32, data[32..36], .little));
    result.metadata.conf_state.learners = try readIds(allocator, data, &offset, std.mem.readInt(u32, data[36..40], .little));
    result.metadata.conf_state.voters_outgoing = try readIds(allocator, data, &offset, std.mem.readInt(u32, data[40..44], .little));
    result.metadata.conf_state.learners_next = try readIds(allocator, data, &offset, std.mem.readInt(u32, data[44..48], .little));
    const membership_end = std.math.add(usize, offset, membership_len) catch return error.MetadataCorrupt;
    if (membership_end > data.len) return error.MetadataCorrupt;
    if (membership_len > 0) result.membership = try allocator.dupe(u8, data[offset..membership_end]);
    offset = membership_end;
    const payload_len = std.math.cast(usize, std.mem.readInt(u64, data[56..64], .little)) orelse return error.MetadataCorrupt;
    const payload_end = std.math.add(usize, offset, payload_len) catch return error.MetadataCorrupt;
    if (payload_end != data.len) return error.MetadataCorrupt;
    if (payload_len > 0) result.data = try allocator.dupe(u8, data[offset..payload_end]);
    try validateMembership(allocator, result);
    return result;
}

fn validateMembership(allocator: std.mem.Allocator, snapshot: Snapshot) !void {
    if (snapshot.membership.len == 0) return;
    var membership = cluster_membership_mod.decode(allocator, snapshot.membership) catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.MetadataCorrupt,
    };
    defer membership.deinit(allocator);
    membership.validate(snapshot.metadata.conf_state) catch return error.MetadataCorrupt;
}

fn readIds(allocator: std.mem.Allocator, data: []const u8, offset: *usize, count: u32) ![]u64 {
    if (count == 0) return &.{};
    const byte_len = std.math.mul(usize, count, 8) catch return error.MetadataCorrupt;
    const end = std.math.add(usize, offset.*, byte_len) catch return error.MetadataCorrupt;
    if (end > data.len) return error.MetadataCorrupt;
    const ids = try allocator.alloc(u64, count);
    for (ids, 0..) |*id, i| id.* = std.mem.readInt(u64, data[offset.* + i * 8 ..][0..8], .little);
    offset.* = end;
    return ids;
}

fn makePath(allocator: std.mem.Allocator, dir: [:0]const u8, index: u64, term: u64, temporary: bool) ![:0]u8 {
    return std.fmt.allocPrintSentinel(allocator, "{s}/snapshot-{d}-{d}.{s}", .{ dir, index, term, if (temporary) "tmp" else "snap" }, 0);
}

fn isSnapshotFilename(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "snapshot-") and (std.mem.endsWith(u8, name, ".snap") or std.mem.endsWith(u8, name, ".tmp"));
}

test "snapshot store round-trips complete snapshots" {
    const allocator = std.testing.allocator;
    var fixture = try fs_testing.FsFixture.init(allocator, .real);
    defer fixture.deinit();
    const dir = fixture.walDir();
    const fs = fixture.fs();
    _ = try fs.makeDir(dir);

    var store = try SnapshotStore.init(allocator, fs, dir);
    defer store.deinit();
    var snapshot = Snapshot{
        .membership = blk: {
            var peers = [_]cluster_membership_mod.PeerEndpoint{
                .{ .node_id = 1, .address = @constCast("node-1") },
                .{ .node_id = 2, .address = @constCast("node-2") },
                .{ .node_id = 3, .address = @constCast("node-3") },
                .{ .node_id = 4, .address = @constCast("node-4") },
                .{ .node_id = 5, .address = @constCast("node-5") },
            };
            break :blk try (cluster_membership_mod.ClusterMembership{
                .cluster_id = .{1} ++ .{0} ** 15,
                .peers = &peers,
            }).encode(allocator);
        },
        .data = try allocator.dupe(u8, "state-image"),
        .metadata = .{
            .index = 9,
            .term = 4,
            .conf_state = .{
                .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }),
                .learners = try allocator.dupe(u64, &.{4}),
                .voters_outgoing = try allocator.dupe(u64, &.{ 1, 2 }),
                .learners_next = try allocator.dupe(u64, &.{5}),
                .auto_leave = true,
            },
        },
    };
    defer snapshot.deinit(allocator);

    try store.save(snapshot);
    var loaded = try store.load(9, 4);
    defer loaded.deinit(allocator);
    try std.testing.expectEqualStrings("state-image", loaded.data);
    try std.testing.expectEqualSlices(u8, snapshot.membership, loaded.membership);
    try std.testing.expect(loaded.metadata.conf_state.eql(snapshot.metadata.conf_state));
}

test "snapshot store decodes a v1 fixture without membership" {
    const allocator = std.testing.allocator;
    var fixture = try fs_testing.FsFixture.init(allocator, .real);
    defer fixture.deinit();
    const dir = fixture.walDir();
    const fs = fixture.fs();
    _ = try fs.makeDir(dir);

    var bytes = [_]u8{0} ** (header_size + 8 + "legacy-state".len);
    std.mem.writeInt(u32, bytes[0..4], snapshot_magic, .little);
    std.mem.writeInt(u32, bytes[4..8], format_version_v1, .little);
    std.mem.writeInt(u64, bytes[16..24], 7, .little);
    std.mem.writeInt(u64, bytes[24..32], 3, .little);
    std.mem.writeInt(u32, bytes[32..36], 1, .little);
    std.mem.writeInt(u64, bytes[56..64], "legacy-state".len, .little);
    std.mem.writeInt(u64, bytes[64..72], 1, .little);
    @memcpy(bytes[72..], "legacy-state");
    std.mem.writeInt(u32, bytes[8..12], Crc32Iscsi.hash(bytes[12..]), .little);

    const path = try makePath(allocator, dir, 7, 3, false);
    defer allocator.free(path);
    const fd = try fs.open(path, .write_truncate);
    try fs.pwriteAll(fd, &bytes, 0);
    try fs.close(fd);

    var store = try SnapshotStore.init(allocator, fs, dir);
    defer store.deinit();
    var loaded = try store.load(7, 3);
    defer loaded.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), loaded.membership.len);
    try std.testing.expectEqualSlices(u64, &.{1}, loaded.metadata.conf_state.voters);
    try std.testing.expectEqualStrings("legacy-state", loaded.data);
}

test "snapshot store rejects malformed and mismatched membership" {
    const allocator = std.testing.allocator;
    var fixture = try fs_testing.FsFixture.init(allocator, .real);
    defer fixture.deinit();
    const dir = fixture.walDir();
    const fs = fixture.fs();
    _ = try fs.makeDir(dir);
    var store = try SnapshotStore.init(allocator, fs, dir);
    defer store.deinit();

    try std.testing.expectError(error.MetadataCorrupt, store.save(.{
        .membership = @constCast("bad"),
        .metadata = .{ .index = 1, .term = 1 },
    }));

    var peers = [_]cluster_membership_mod.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
    };
    const membership = try (cluster_membership_mod.ClusterMembership{
        .cluster_id = .{1} ++ .{0} ** 15,
        .peers = &peers,
    }).encode(allocator);
    defer allocator.free(membership);
    try std.testing.expectError(error.MetadataCorrupt, store.save(.{
        .membership = membership,
        .metadata = .{
            .index = 2,
            .term = 1,
            .conf_state = .{ .voters = @constCast(&[_]u64{2}) },
        },
    }));

    const valid = Snapshot{
        .membership = membership,
        .metadata = .{
            .index = 3,
            .term = 1,
            .conf_state = .{ .voters = @constCast(&[_]u64{1}) },
        },
    };
    var malformed = try encode(allocator, valid);
    defer allocator.free(malformed);
    malformed[header_size + 8] ^= 0xff;
    std.mem.writeInt(u32, malformed[8..12], Crc32Iscsi.hash(malformed[12..]), .little);
    try std.testing.expectError(error.MetadataCorrupt, decode(allocator, malformed));

    var mismatch = try encode(allocator, valid);
    defer allocator.free(mismatch);
    std.mem.writeInt(u64, mismatch[header_size..][0..8], 2, .little);
    std.mem.writeInt(u32, mismatch[8..12], Crc32Iscsi.hash(mismatch[12..]), .little);
    try std.testing.expectError(error.MetadataCorrupt, decode(allocator, mismatch));
}

test "snapshot store rejects corruption and metadata mismatch" {
    const allocator = std.testing.allocator;
    var fixture = try fs_testing.FsFixture.init(allocator, .real);
    defer fixture.deinit();
    const dir = fixture.walDir();
    const fs = fixture.fs();
    _ = try fs.makeDir(dir);

    var store = try SnapshotStore.init(allocator, fs, dir);
    defer store.deinit();
    const snapshot = Snapshot{ .data = @constCast("payload"), .metadata = .{ .index = 3, .term = 2 } };
    try store.save(snapshot);
    try std.testing.expectError(error.FileNotFound, store.load(4, 2));

    const original_path = try makePath(allocator, dir, 3, 2, false);
    defer allocator.free(original_path);
    const mismatched_path = try makePath(allocator, dir, 4, 2, false);
    defer allocator.free(mismatched_path);
    try fs.rename(original_path, mismatched_path);
    try std.testing.expectError(error.MetadataCorrupt, store.load(4, 2));
    try fs.rename(mismatched_path, original_path);

    try store.remove(0, 0);
    try store.remove(3, 2);
    try std.testing.expectError(error.FileNotFound, store.load(3, 2));
    try store.save(snapshot);

    const path = try makePath(allocator, dir, 3, 2, false);
    defer allocator.free(path);
    const fd = try fs.open(path, .write_truncate);
    try fs.pwriteAll(fd, "corrupt", 0);
    try fs.close(fd);
    try std.testing.expectError(error.MetadataCorrupt, store.load(3, 2));
}
