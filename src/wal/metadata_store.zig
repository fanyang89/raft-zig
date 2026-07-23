const std = @import("std");
const linux = std.os.linux;

const Crc32Iscsi = std.hash.crc.Crc32Iscsi;

const metadata_magic: u32 = 0x4D455441;
const format_version: u32 = 1;
const header_size: usize = 16;
const content_size: usize = 24;
const max_metadata_size: usize = 16 * 1024 * 1024;

pub const Metadata = struct {
    first_index: u64 = 1,
    snapshot_index: u64 = 0,
    snapshot_term: u64 = 0,
    hard_state: []u8 = &.{},
    conf_state: []u8 = &.{},

    pub fn deinit(self: *Metadata, allocator: std.mem.Allocator) void {
        if (self.hard_state.len > 0) allocator.free(self.hard_state);
        if (self.conf_state.len > 0) allocator.free(self.conf_state);
        self.* = .{};
    }
};

pub const MetadataStore = struct {
    allocator: std.mem.Allocator,
    dir: [:0]u8,
    path: [:0]u8,
    tmp_path: [:0]u8,

    pub fn init(allocator: std.mem.Allocator, dir: [:0]const u8) !MetadataStore {
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
        };
    }

    pub fn deinit(self: *MetadataStore) void {
        self.allocator.free(self.tmp_path);
        self.allocator.free(self.path);
        self.allocator.free(self.dir);
        self.* = undefined;
    }

    pub fn load(self: *MetadataStore) !?Metadata {
        const fd = openRead(self.path) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer closeIgnore(fd);

        const size = try fileSize(fd);
        if (size > max_metadata_size) return error.MetadataCorrupt;
        const data = try self.allocator.alloc(u8, size);
        defer self.allocator.free(data);
        try readAll(fd, data);
        return try decode(self.allocator, data);
    }

    pub fn save(self: *MetadataStore, metadata: Metadata) !void {
        const data = try encode(self.allocator, metadata);
        defer self.allocator.free(data);

        const fd = try openTemporary(self.tmp_path);
        var is_open = true;
        errdefer {
            if (is_open) closeIgnore(fd);
            unlinkIgnore(self.tmp_path);
        }
        try writeAll(fd, data);
        try syncFile(fd);
        const close_result = closeFile(fd);
        is_open = false;
        try close_result;
        try renameFile(self.tmp_path, self.path);
        try syncDirectory(self.dir);
    }
};

pub fn removeFiles(allocator: std.mem.Allocator, dir: [:0]const u8) void {
    const path = makePath(allocator, dir, "metadata") catch return;
    defer allocator.free(path);
    const tmp_path = makePath(allocator, dir, "metadata.tmp") catch return;
    defer allocator.free(tmp_path);
    unlinkIgnore(path);
    unlinkIgnore(tmp_path);
}

fn encode(allocator: std.mem.Allocator, metadata: Metadata) ![]u8 {
    const hard_state_len = std.math.cast(u32, metadata.hard_state.len) orelse return error.MetadataCorrupt;
    const conf_state_len = std.math.cast(u32, metadata.conf_state.len) orelse return error.MetadataCorrupt;
    var total = try std.math.add(usize, header_size, content_size);
    total = try std.math.add(usize, total, 4 + metadata.hard_state.len);
    total = try std.math.add(usize, total, 4 + metadata.conf_state.len);
    if (total > max_metadata_size) return error.MetadataCorrupt;

    const data = try allocator.alloc(u8, total);
    @memset(data, 0);
    std.mem.writeInt(u32, data[0..4], metadata_magic, .little);
    std.mem.writeInt(u32, data[4..8], format_version, .little);
    std.mem.writeInt(u64, data[16..24], metadata.first_index, .little);
    std.mem.writeInt(u64, data[24..32], metadata.snapshot_index, .little);
    std.mem.writeInt(u64, data[32..40], metadata.snapshot_term, .little);

    var offset: usize = header_size + content_size;
    std.mem.writeInt(u32, data[offset..][0..4], hard_state_len, .little);
    offset += 4;
    @memcpy(data[offset .. offset + metadata.hard_state.len], metadata.hard_state);
    offset += metadata.hard_state.len;
    std.mem.writeInt(u32, data[offset..][0..4], conf_state_len, .little);
    offset += 4;
    @memcpy(data[offset .. offset + metadata.conf_state.len], metadata.conf_state);

    const crc = Crc32Iscsi.hash(data[12..]);
    std.mem.writeInt(u32, data[8..12], crc, .little);
    return data;
}

fn decode(allocator: std.mem.Allocator, data: []const u8) !Metadata {
    if (data.len < header_size + content_size + 8) return error.MetadataCorrupt;
    if (std.mem.readInt(u32, data[0..4], .little) != metadata_magic) return error.MetadataCorrupt;
    if (std.mem.readInt(u32, data[4..8], .little) != format_version) return error.MetadataCorrupt;
    const expected_crc = std.mem.readInt(u32, data[8..12], .little);
    if (Crc32Iscsi.hash(data[12..]) != expected_crc) return error.MetadataCorrupt;

    var result = Metadata{
        .first_index = std.mem.readInt(u64, data[16..24], .little),
        .snapshot_index = std.mem.readInt(u64, data[24..32], .little),
        .snapshot_term = std.mem.readInt(u64, data[32..40], .little),
    };
    errdefer result.deinit(allocator);
    if (result.first_index == 0) return error.MetadataCorrupt;

    var offset: usize = header_size + content_size;
    const hard_state_len = try readLength(data, &offset);
    const hard_state_end = std.math.add(usize, offset, hard_state_len) catch return error.MetadataCorrupt;
    if (hard_state_end > data.len) return error.MetadataCorrupt;
    if (hard_state_len > 0) result.hard_state = try allocator.dupe(u8, data[offset..hard_state_end]);
    offset = hard_state_end;

    const conf_state_len = try readLength(data, &offset);
    const conf_state_end = std.math.add(usize, offset, conf_state_len) catch return error.MetadataCorrupt;
    if (conf_state_end != data.len) return error.MetadataCorrupt;
    if (conf_state_len > 0) result.conf_state = try allocator.dupe(u8, data[offset..conf_state_end]);
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

fn openRead(path: [:0]const u8) !linux.fd_t {
    const flags: linux.O = .{ .ACCMODE = .RDONLY, .CLOEXEC = true };
    while (true) {
        const rc = linux.open(path.ptr, flags, 0);
        switch (linux.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .NOENT => return error.FileNotFound,
            else => return error.OpenFailed,
        }
    }
}

fn openTemporary(path: [:0]const u8) !linux.fd_t {
    const flags: linux.O = .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true };
    while (true) {
        const rc = linux.open(path.ptr, flags, 0o644);
        switch (linux.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => return error.OpenFailed,
        }
    }
}

fn fileSize(fd: linux.fd_t) !usize {
    const rc = linux.lseek(fd, 0, 2);
    if (linux.errno(rc) != .SUCCESS) return error.StatFailed;
    return std.math.cast(usize, rc) orelse error.StatFailed;
}

fn readAll(fd: linux.fd_t, data: []u8) !void {
    var offset: usize = 0;
    while (offset < data.len) {
        const rc = linux.pread(fd, data[offset..].ptr, data.len - offset, @intCast(offset));
        switch (linux.errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
            else => return error.ReadFailed,
        }
        if (rc == 0) return error.ReadFailed;
        offset += rc;
    }
}

fn writeAll(fd: linux.fd_t, data: []const u8) !void {
    var offset: usize = 0;
    while (offset < data.len) {
        const rc = linux.write(fd, data[offset..].ptr, data.len - offset);
        switch (linux.errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
            else => return error.WriteFailed,
        }
        if (rc == 0) return error.WriteFailed;
        offset += rc;
    }
}

fn syncFile(fd: linux.fd_t) !void {
    while (true) {
        const rc = linux.fsync(fd);
        switch (linux.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            else => return error.SyncFailed,
        }
    }
}

fn closeFile(fd: linux.fd_t) !void {
    if (linux.errno(linux.close(fd)) != .SUCCESS) return error.CloseFailed;
}

fn closeIgnore(fd: linux.fd_t) void {
    _ = linux.close(fd);
}

fn renameFile(old: [:0]const u8, new: [:0]const u8) !void {
    while (true) {
        const rc = linux.rename(old.ptr, new.ptr);
        switch (linux.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            else => return error.RenameFailed,
        }
    }
}

fn syncDirectory(path: [:0]const u8) !void {
    const flags: linux.O = .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true };
    const fd: linux.fd_t = while (true) {
        const rc = linux.open(path.ptr, flags, 0);
        switch (linux.errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            else => return error.OpenFailed,
        }
    };
    defer closeIgnore(fd);
    try syncFile(fd);
}

fn unlinkIgnore(path: [:0]const u8) void {
    _ = linux.unlink(path.ptr);
}

test "metadata store round-trips and rejects corruption" {
    const allocator = std.testing.allocator;
    const dir: [:0]const u8 = "/tmp/raft-zig-metadata-store-test";
    removeFiles(allocator, dir);
    _ = linux.mkdir(dir.ptr, 0o755);
    defer {
        removeFiles(allocator, dir);
        _ = linux.rmdir(dir.ptr);
    }

    var store = try MetadataStore.init(allocator, dir);
    defer store.deinit();
    try std.testing.expect((try store.load()) == null);
    try store.save(.{
        .first_index = 7,
        .snapshot_index = 6,
        .snapshot_term = 3,
        .hard_state = @constCast("hard"),
        .conf_state = @constCast("conf"),
    });

    var loaded = (try store.load()).?;
    defer loaded.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 7), loaded.first_index);
    try std.testing.expectEqualStrings("hard", loaded.hard_state);
    try std.testing.expectEqualStrings("conf", loaded.conf_state);

    const tmp_fd = try openTemporary(store.tmp_path);
    try writeAll(tmp_fd, "stale");
    try closeFile(tmp_fd);
    var loaded_with_stale_tmp = (try store.load()).?;
    defer loaded_with_stale_tmp.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 7), loaded_with_stale_tmp.first_index);

    const fd = try openTemporary(store.path);
    defer closeIgnore(fd);
    try writeAll(fd, "corrupt");
    try std.testing.expectError(error.MetadataCorrupt, store.load());
}
