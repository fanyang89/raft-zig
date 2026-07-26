const std = @import("std");
const fs_mod = @import("../fs.zig");
const fs_testing = @import("../fs/testing.zig");

const Crc32Iscsi = std.hash.crc.Crc32Iscsi;

const metadata_magic: u32 = 0x4D455441;
const format_version: u32 = 4;
const header_size: usize = 16;
const content_size_v1: usize = 24;
const content_size_v2: usize = 32;
const content_size_v3: usize = 40;
const content_size: usize = 48;
const max_metadata_size: usize = 16 * 1024 * 1024;

pub const Metadata = struct {
    version: u32 = format_version,
    first_index: u64 = 1,
    snapshot_index: u64 = 0,
    snapshot_term: u64 = 0,
    first_segment_id: u64 = 0,
    incarnation: u64 = 0,
    membership_index: u64 = 0,
    hard_state: []u8 = &.{},
    conf_state: []u8 = &.{},
    cluster_membership: []u8 = &.{},

    pub fn deinit(self: *Metadata, allocator: std.mem.Allocator) void {
        if (self.hard_state.len > 0) allocator.free(self.hard_state);
        if (self.conf_state.len > 0) allocator.free(self.conf_state);
        if (self.cluster_membership.len > 0) allocator.free(self.cluster_membership);
        self.* = .{};
    }
};

pub const MetadataStore = struct {
    allocator: std.mem.Allocator,
    dir: [:0]u8,
    path: [:0]u8,
    tmp_path: [:0]u8,
    fs: fs_mod.Fs,

    pub fn init(allocator: std.mem.Allocator, fs: fs_mod.Fs, dir: [:0]const u8) !MetadataStore {
        const dir_copy = try allocator.dupeSentinel(u8, dir, 0);
        errdefer allocator.free(dir_copy);
        const path = try makePath(allocator, dir, "metadata");
        errdefer allocator.free(path);
        const tmp_path = try makePath(allocator, dir, "metadata.tmp");
        return .{
            .allocator = allocator,
            .dir = dir_copy,
            .path = path,
            .tmp_path = tmp_path,
            .fs = fs,
        };
    }

    pub fn deinit(self: *MetadataStore) void {
        self.allocator.free(self.tmp_path);
        self.allocator.free(self.path);
        self.allocator.free(self.dir);
        self.* = undefined;
    }

    pub fn load(self: *MetadataStore) !?Metadata {
        const fd = self.fs.open(self.path, .read_only) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer self.fs.close(fd) catch {};

        const size = std.math.cast(usize, try self.fs.fileSize(fd)) orelse return error.StatFailed;
        if (size > max_metadata_size) return error.MetadataCorrupt;
        const data = try self.allocator.alloc(u8, size);
        defer self.allocator.free(data);
        if (try self.fs.preadAll(fd, data, 0) != data.len) return error.ReadFailed;
        return try decode(self.allocator, data);
    }

    pub fn save(self: *MetadataStore, metadata: Metadata) !void {
        const data = try encode(self.allocator, metadata);
        defer self.allocator.free(data);

        const fd = try self.fs.open(self.tmp_path, .write_truncate);
        var is_open = true;
        errdefer {
            if (is_open) self.fs.close(fd) catch {};
            self.fs.unlink(self.tmp_path) catch {};
        }
        try self.fs.pwriteAll(fd, data, 0);
        try self.fs.syncFile(fd);
        const close_result = self.fs.close(fd);
        is_open = false;
        try close_result;
        try self.fs.rename(self.tmp_path, self.path);
        try self.fs.syncDir(self.dir);
    }
};

pub fn removeFiles(allocator: std.mem.Allocator, fs: fs_mod.Fs, dir: [:0]const u8) void {
    const path = makePath(allocator, dir, "metadata") catch return;
    defer allocator.free(path);
    const tmp_path = makePath(allocator, dir, "metadata.tmp") catch return;
    defer allocator.free(tmp_path);
    fs.unlink(path) catch {};
    fs.unlink(tmp_path) catch {};
}

fn encode(allocator: std.mem.Allocator, metadata: Metadata) ![]u8 {
    const hard_state_len = std.math.cast(u32, metadata.hard_state.len) orelse return error.MetadataCorrupt;
    const conf_state_len = std.math.cast(u32, metadata.conf_state.len) orelse return error.MetadataCorrupt;
    const cluster_membership_len = std.math.cast(u32, metadata.cluster_membership.len) orelse return error.MetadataCorrupt;
    var total = try std.math.add(usize, header_size, content_size);
    total = try std.math.add(usize, total, 4 + metadata.hard_state.len);
    total = try std.math.add(usize, total, 4 + metadata.conf_state.len);
    total = try std.math.add(usize, total, 4 + metadata.cluster_membership.len);
    if (total > max_metadata_size) return error.MetadataCorrupt;

    const data = try allocator.alloc(u8, total);
    @memset(data, 0);
    std.mem.writeInt(u32, data[0..4], metadata_magic, .little);
    std.mem.writeInt(u32, data[4..8], format_version, .little);
    std.mem.writeInt(u64, data[16..24], metadata.first_index, .little);
    std.mem.writeInt(u64, data[24..32], metadata.snapshot_index, .little);
    std.mem.writeInt(u64, data[32..40], metadata.snapshot_term, .little);
    std.mem.writeInt(u64, data[40..48], metadata.first_segment_id, .little);
    std.mem.writeInt(u64, data[48..56], metadata.incarnation, .little);
    std.mem.writeInt(u64, data[56..64], metadata.membership_index, .little);

    var offset: usize = header_size + content_size;
    std.mem.writeInt(u32, data[offset..][0..4], hard_state_len, .little);
    offset += 4;
    @memcpy(data[offset .. offset + metadata.hard_state.len], metadata.hard_state);
    offset += metadata.hard_state.len;
    std.mem.writeInt(u32, data[offset..][0..4], conf_state_len, .little);
    offset += 4;
    @memcpy(data[offset .. offset + metadata.conf_state.len], metadata.conf_state);
    offset += metadata.conf_state.len;
    std.mem.writeInt(u32, data[offset..][0..4], cluster_membership_len, .little);
    offset += 4;
    @memcpy(data[offset .. offset + metadata.cluster_membership.len], metadata.cluster_membership);

    const crc = Crc32Iscsi.hash(data[12..]);
    std.mem.writeInt(u32, data[8..12], crc, .little);
    return data;
}

fn decode(allocator: std.mem.Allocator, data: []const u8) !Metadata {
    if (data.len < header_size + content_size_v1 + 8) return error.MetadataCorrupt;
    if (std.mem.readInt(u32, data[0..4], .little) != metadata_magic) return error.MetadataCorrupt;
    const version = std.mem.readInt(u32, data[4..8], .little);
    if (version < 1 or version > format_version) return error.MetadataCorrupt;
    const fixed_content_size = switch (version) {
        1 => content_size_v1,
        2 => content_size_v2,
        3 => content_size_v3,
        4 => content_size,
        else => unreachable,
    };
    const length_fields_size: usize = if (version >= 4) 12 else 8;
    const minimum_size = std.math.add(usize, header_size + fixed_content_size, length_fields_size) catch
        return error.MetadataCorrupt;
    if (data.len < minimum_size) return error.MetadataCorrupt;
    const expected_crc = std.mem.readInt(u32, data[8..12], .little);
    if (Crc32Iscsi.hash(data[12..]) != expected_crc) return error.MetadataCorrupt;

    var result = Metadata{
        .version = version,
        .first_index = std.mem.readInt(u64, data[16..24], .little),
        .snapshot_index = std.mem.readInt(u64, data[24..32], .little),
        .snapshot_term = std.mem.readInt(u64, data[32..40], .little),
        .first_segment_id = if (version >= 2) std.mem.readInt(u64, data[40..48], .little) else 0,
        .incarnation = if (version >= 3) std.mem.readInt(u64, data[48..56], .little) else 0,
        .membership_index = if (version >= 4) std.mem.readInt(u64, data[56..64], .little) else 0,
    };
    errdefer result.deinit(allocator);
    if (result.first_index == 0) return error.MetadataCorrupt;

    var offset: usize = header_size + fixed_content_size;
    const hard_state_len = try readLength(data, &offset);
    const hard_state_end = std.math.add(usize, offset, hard_state_len) catch return error.MetadataCorrupt;
    if (hard_state_end > data.len) return error.MetadataCorrupt;
    if (hard_state_len > 0) result.hard_state = try allocator.dupe(u8, data[offset..hard_state_end]);
    offset = hard_state_end;

    const conf_state_len = try readLength(data, &offset);
    const conf_state_end = std.math.add(usize, offset, conf_state_len) catch return error.MetadataCorrupt;
    if (conf_state_end > data.len) return error.MetadataCorrupt;
    if (conf_state_len > 0) result.conf_state = try allocator.dupe(u8, data[offset..conf_state_end]);
    offset = conf_state_end;
    if (version < 4) {
        if (offset != data.len) return error.MetadataCorrupt;
        return result;
    }

    const cluster_membership_len = try readLength(data, &offset);
    const cluster_membership_end = std.math.add(usize, offset, cluster_membership_len) catch return error.MetadataCorrupt;
    if (cluster_membership_end != data.len) return error.MetadataCorrupt;
    if (cluster_membership_len > 0) result.cluster_membership = try allocator.dupe(u8, data[offset..cluster_membership_end]);
    return result;
}

fn readLength(data: []const u8, offset: *usize) !usize {
    const end = std.math.add(usize, offset.*, 4) catch return error.MetadataCorrupt;
    if (end > data.len) return error.MetadataCorrupt;
    const len: usize = std.mem.readInt(u32, data[offset.*..][0..4], .little);
    offset.* = end;
    return len;
}

fn makePath(allocator: std.mem.Allocator, dir: [:0]const u8, basename: []const u8) ![:0]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir, basename });
    defer allocator.free(path);
    return allocator.dupeZ(u8, path);
}

test "metadata store round-trips and rejects corruption" {
    const allocator = std.testing.allocator;
    var fixture = try fs_testing.FsFixture.init(allocator, .real);
    defer fixture.deinit();
    const dir = fixture.walDir();
    const fs = fixture.fs();
    _ = try fs.makeDir(dir);

    var store = try MetadataStore.init(allocator, fs, dir);
    defer store.deinit();
    try std.testing.expect((try store.load()) == null);
    try store.save(.{
        .first_index = 7,
        .snapshot_index = 6,
        .snapshot_term = 3,
        .first_segment_id = 4,
        .incarnation = 11,
        .membership_index = 9,
        .hard_state = @constCast("hard"),
        .conf_state = @constCast("conf"),
        .cluster_membership = @constCast("cluster"),
    });

    var loaded = (try store.load()).?;
    defer loaded.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 7), loaded.first_index);
    try std.testing.expectEqual(@as(u64, 4), loaded.first_segment_id);
    try std.testing.expectEqual(@as(u64, 11), loaded.incarnation);
    try std.testing.expectEqual(@as(u64, 9), loaded.membership_index);
    try std.testing.expectEqualStrings("hard", loaded.hard_state);
    try std.testing.expectEqualStrings("conf", loaded.conf_state);
    try std.testing.expectEqualStrings("cluster", loaded.cluster_membership);

    const tmp_fd = try fs.open(store.tmp_path, .write_truncate);
    try fs.pwriteAll(tmp_fd, "stale", 0);
    try fs.close(tmp_fd);
    var loaded_with_stale_tmp = (try store.load()).?;
    defer loaded_with_stale_tmp.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 7), loaded_with_stale_tmp.first_index);

    const fd = try fs.open(store.path, .write_truncate);
    defer fs.close(fd) catch {};
    try fs.pwriteAll(fd, "corrupt", 0);
    try std.testing.expectError(error.MetadataCorrupt, store.load());
}

fn legacyMetadata(allocator: std.mem.Allocator, current: []const u8, version: u32, cluster_len: usize) ![]u8 {
    const legacy_content_size = switch (version) {
        1 => content_size_v1,
        2 => content_size_v2,
        3 => content_size_v3,
        else => unreachable,
    };
    const two_blobs_len = current.len - (header_size + content_size) - (4 + cluster_len);
    const legacy = try allocator.alloc(u8, header_size + legacy_content_size + two_blobs_len);
    @memcpy(legacy[0 .. header_size + legacy_content_size], current[0 .. header_size + legacy_content_size]);
    @memcpy(legacy[header_size + legacy_content_size ..], current[header_size + content_size ..][0..two_blobs_len]);
    std.mem.writeInt(u32, legacy[4..8], version, .little);
    std.mem.writeInt(u32, legacy[8..12], Crc32Iscsi.hash(legacy[12..]), .little);
    return legacy;
}

test "metadata store decodes v1 through v3 with membership defaults" {
    const allocator = std.testing.allocator;
    const current = try encode(allocator, .{
        .first_index = 7,
        .snapshot_index = 6,
        .snapshot_term = 3,
        .first_segment_id = 4,
        .incarnation = 11,
        .membership_index = 9,
        .hard_state = @constCast("hard"),
        .conf_state = @constCast("conf"),
        .cluster_membership = @constCast("cluster"),
    });
    defer allocator.free(current);
    for (1..4) |version_usize| {
        const version: u32 = @intCast(version_usize);
        const legacy = try legacyMetadata(allocator, current, version, "cluster".len);
        defer allocator.free(legacy);
        var metadata = try decode(allocator, legacy);
        defer metadata.deinit(allocator);
        try std.testing.expectEqual(if (version >= 2) @as(u64, 4) else 0, metadata.first_segment_id);
        try std.testing.expectEqual(if (version >= 3) @as(u64, 11) else 0, metadata.incarnation);
        try std.testing.expectEqual(@as(u64, 0), metadata.membership_index);
        try std.testing.expectEqual(@as(usize, 0), metadata.cluster_membership.len);
        try std.testing.expectEqualStrings("hard", metadata.hard_state);
        try std.testing.expectEqualStrings("conf", metadata.conf_state);
    }
}

test "metadata store rejects malformed cluster membership blob" {
    const allocator = std.testing.allocator;
    var encoded = try encode(allocator, .{
        .hard_state = @constCast("hard"),
        .conf_state = @constCast("conf"),
        .cluster_membership = @constCast("cluster"),
    });
    defer allocator.free(encoded);
    const cluster_length_offset = header_size + content_size + 4 + "hard".len + 4 + "conf".len;
    std.mem.writeInt(u32, encoded[cluster_length_offset..][0..4], 100, .little);
    std.mem.writeInt(u32, encoded[8..12], Crc32Iscsi.hash(encoded[12..]), .little);
    try std.testing.expectError(error.MetadataCorrupt, decode(allocator, encoded));
}
