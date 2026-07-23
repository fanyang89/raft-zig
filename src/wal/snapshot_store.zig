const std = @import("std");
const linux = std.os.linux;

const types = @import("../core/types.zig");

const Snapshot = types.Snapshot;
const Crc32Iscsi = std.hash.crc.Crc32Iscsi;

const snapshot_magic: u32 = 0x534E4150;
const format_version: u32 = 1;
const header_size: usize = 64;

pub const SnapshotStore = struct {
    allocator: std.mem.Allocator,
    dir: [:0]u8,

    pub fn init(allocator: std.mem.Allocator, dir: [:0]const u8) !SnapshotStore {
        return .{
            .allocator = allocator,
            .dir = try allocator.dupeSentinel(u8, dir, 0),
        };
    }

    pub fn deinit(self: *SnapshotStore) void {
        self.allocator.free(self.dir);
        self.* = undefined;
    }

    pub fn save(self: *SnapshotStore, snapshot: Snapshot) !void {
        if (snapshot.metadata.index == 0) return error.MetadataCorrupt;
        const data = try encode(self.allocator, snapshot);
        defer self.allocator.free(data);
        const path = try makePath(self.allocator, self.dir, snapshot.metadata.index, snapshot.metadata.term, false);
        defer self.allocator.free(path);
        const tmp_path = try makePath(self.allocator, self.dir, snapshot.metadata.index, snapshot.metadata.term, true);
        defer self.allocator.free(tmp_path);

        const fd = try openTemporary(tmp_path);
        var is_open = true;
        errdefer {
            if (is_open) closeIgnore(fd);
            unlinkIgnore(tmp_path);
        }
        try writeAll(fd, data);
        try syncFile(fd);
        const close_result = closeFile(fd);
        is_open = false;
        try close_result;
        try renameFile(tmp_path, path);
        try syncDirectory(self.dir);
    }

    pub fn load(self: *SnapshotStore, index: u64, term: u64) !Snapshot {
        const path = try makePath(self.allocator, self.dir, index, term, false);
        defer self.allocator.free(path);
        const fd = try openRead(path);
        defer closeIgnore(fd);
        const size = try fileSize(fd);
        const data = try self.allocator.alloc(u8, size);
        defer self.allocator.free(data);
        try readAll(fd, data);
        var snapshot = try decode(self.allocator, data);
        errdefer snapshot.deinit(self.allocator);
        if (snapshot.metadata.index != index or snapshot.metadata.term != term) return error.MetadataCorrupt;
        return snapshot;
    }

    pub fn remove(self: *SnapshotStore, index: u64, term: u64) !void {
        if (index == 0) return;
        const path = try makePath(self.allocator, self.dir, index, term, false);
        defer self.allocator.free(path);
        try unlinkFile(path);
        try syncDirectory(self.dir);
    }
};

pub fn removeFiles(allocator: std.mem.Allocator, dir: [:0]const u8) void {
    const flags: linux.O = .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true };
    const fd = openDirectory(dir, flags) catch return;
    defer closeIgnore(fd);
    var buffer: [4096]u8 = undefined;
    while (true) {
        const n = getDirectoryEntries(fd, &buffer) catch return;
        if (n == 0) break;
        var offset: usize = 0;
        while (offset < n) {
            const entry: *align(1) linux.dirent64 = @ptrCast(&buffer[offset]);
            const name_offset = @offsetOf(linux.dirent64, "name");
            if (entry.reclen <= name_offset or entry.reclen > n - offset) return;
            const name_ptr: [*]const u8 = &entry.name;
            const padded_name = name_ptr[0 .. entry.reclen - name_offset];
            const name_len = std.mem.findScalar(u8, padded_name, 0) orelse return;
            const name = name_ptr[0..name_len];
            if (isSnapshotFilename(name)) {
                const path = std.fmt.allocPrintSentinel(allocator, "{s}/{s}", .{ dir, name }, 0) catch return;
                unlinkIgnore(path);
                allocator.free(path);
            }
            offset += entry.reclen;
        }
    }
}

fn encode(allocator: std.mem.Allocator, snapshot: Snapshot) ![]u8 {
    const voters_len = std.math.cast(u32, snapshot.metadata.conf_state.voters.len) orelse return error.MetadataCorrupt;
    const learners_len = std.math.cast(u32, snapshot.metadata.conf_state.learners.len) orelse return error.MetadataCorrupt;
    const outgoing_len = std.math.cast(u32, snapshot.metadata.conf_state.voters_outgoing.len) orelse return error.MetadataCorrupt;
    const next_len = std.math.cast(u32, snapshot.metadata.conf_state.learners_next.len) orelse return error.MetadataCorrupt;
    const data_len = std.math.cast(u64, snapshot.data.len) orelse return error.MetadataCorrupt;
    var total = header_size;
    for ([_]usize{
        snapshot.metadata.conf_state.voters.len,
        snapshot.metadata.conf_state.learners.len,
        snapshot.metadata.conf_state.voters_outgoing.len,
        snapshot.metadata.conf_state.learners_next.len,
    }) |count| total = std.math.add(usize, total, std.math.mul(usize, count, 8) catch return error.MetadataCorrupt) catch return error.MetadataCorrupt;
    total = std.math.add(usize, total, snapshot.data.len) catch return error.MetadataCorrupt;

    const data = try allocator.alloc(u8, total);
    @memset(data, 0);
    std.mem.writeInt(u32, data[0..4], snapshot_magic, .little);
    std.mem.writeInt(u32, data[4..8], format_version, .little);
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
    @memcpy(data[offset..], snapshot.data);
    std.mem.writeInt(u32, data[8..12], Crc32Iscsi.hash(data[12..]), .little);
    return data;
}

fn decode(allocator: std.mem.Allocator, data: []const u8) !Snapshot {
    if (data.len < header_size) return error.MetadataCorrupt;
    if (std.mem.readInt(u32, data[0..4], .little) != snapshot_magic) return error.MetadataCorrupt;
    if (std.mem.readInt(u32, data[4..8], .little) != format_version) return error.MetadataCorrupt;
    if (Crc32Iscsi.hash(data[12..]) != std.mem.readInt(u32, data[8..12], .little)) return error.MetadataCorrupt;
    if (data[48] > 1 or !std.mem.allEqual(u8, data[49..56], 0)) return error.MetadataCorrupt;

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
    const payload_len = std.math.cast(usize, std.mem.readInt(u64, data[56..64], .little)) orelse return error.MetadataCorrupt;
    const payload_end = std.math.add(usize, offset, payload_len) catch return error.MetadataCorrupt;
    if (payload_end != data.len) return error.MetadataCorrupt;
    if (payload_len > 0) result.data = try allocator.dupe(u8, data[offset..payload_end]);
    return result;
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

fn openDirectory(path: [:0]const u8, flags: linux.O) !linux.fd_t {
    while (true) {
        const rc = linux.open(path.ptr, flags, 0);
        switch (linux.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => return error.OpenFailed,
        }
    }
}

fn getDirectoryEntries(fd: linux.fd_t, buffer: []u8) !usize {
    while (true) {
        const rc = linux.getdents64(fd, buffer.ptr, buffer.len);
        switch (linux.errno(rc)) {
            .SUCCESS => return rc,
            .INTR => continue,
            else => return error.ReadFailed,
        }
    }
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

fn unlinkFile(path: [:0]const u8) !void {
    while (true) {
        const rc = linux.unlink(path.ptr);
        switch (linux.errno(rc)) {
            .SUCCESS, .NOENT => return,
            .INTR => continue,
            else => return error.UnlinkFailed,
        }
    }
}

fn unlinkIgnore(path: [:0]const u8) void {
    _ = linux.unlink(path.ptr);
}

fn syncDirectory(path: [:0]const u8) !void {
    const flags: linux.O = .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true };
    const fd = try openDirectory(path, flags);
    defer closeIgnore(fd);
    try syncFile(fd);
}

test "snapshot store round-trips complete snapshots" {
    const allocator = std.testing.allocator;
    const dir: [:0]const u8 = "/tmp/raft-zig-snapshot-store-test";
    removeFiles(allocator, dir);
    _ = linux.mkdir(dir.ptr, 0o755);
    defer {
        removeFiles(allocator, dir);
        _ = linux.rmdir(dir.ptr);
    }

    var store = try SnapshotStore.init(allocator, dir);
    defer store.deinit();
    var snapshot = Snapshot{
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
    try std.testing.expect(loaded.metadata.conf_state.eql(snapshot.metadata.conf_state));
}

test "snapshot store rejects corruption and metadata mismatch" {
    const allocator = std.testing.allocator;
    const dir: [:0]const u8 = "/tmp/raft-zig-snapshot-store-corruption-test";
    removeFiles(allocator, dir);
    _ = linux.mkdir(dir.ptr, 0o755);
    defer {
        removeFiles(allocator, dir);
        _ = linux.rmdir(dir.ptr);
    }

    var store = try SnapshotStore.init(allocator, dir);
    defer store.deinit();
    const snapshot = Snapshot{ .data = @constCast("payload"), .metadata = .{ .index = 3, .term = 2 } };
    try store.save(snapshot);
    try std.testing.expectError(error.FileNotFound, store.load(4, 2));

    const path = try makePath(allocator, dir, 3, 2, false);
    defer allocator.free(path);
    const fd = try openTemporary(path);
    try writeAll(fd, "corrupt");
    try closeFile(fd);
    try std.testing.expectError(error.MetadataCorrupt, store.load(3, 2));
}
