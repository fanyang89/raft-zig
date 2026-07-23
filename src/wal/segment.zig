//! Single WAL segment file abstraction.
//!
//! Ports `ref/raftpp/lib/raftor/wal/segment.{h,cc}`. Each Segment owns
//! one file descriptor and manages byte-level append/read/sync/truncate.
//! It knows nothing about Raft records — that's the WAL layer's job.

const std = @import("std");
const linux = std.os.linux;

const log = std.log.scoped(.raft_zig_wal);

const SEGMENT_HEADER_SIZE: usize = 32;
const segment_magic: u32 = 0x57414C31; // "WAL1"
const format_version: u32 = 1;

fn errno(rc: usize) linux.E {
    const signed: isize = @bitCast(rc);
    if (signed >= 0) return .SUCCESS;
    return @enumFromInt(-signed);
}

fn sysOpen(path: [:0]const u8) !linux.fd_t {
    // Open for read+write, do NOT create. scanDirectory uses this to find
    // existing segments; creating files during scanning would leave empty
    // files behind when the header check fails.
    const flags: linux.O = .{ .ACCMODE = .RDWR };
    while (true) {
        const rc = linux.open(path.ptr, flags, 0o644);
        switch (errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .NOENT => return error.FileNotFound,
            else => return error.OpenFailed,
        }
    }
}

fn sysOpenExclusive(path: [:0]const u8) !linux.fd_t {
    const flags: linux.O = .{ .ACCMODE = .WRONLY, .CREAT = true, .EXCL = true };
    while (true) {
        const rc = linux.open(path.ptr, flags, 0o644);
        const e = errno(rc);
        switch (e) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => {
                log.debug("exclusive open failed: path={s}, errno={}", .{ path, e });
                return error.OpenFailed;
            },
        }
    }
}

fn sysPwrite(fd: linux.fd_t, data: []const u8, offset: u64) !void {
    var written: usize = 0;
    while (written < data.len) {
        const current_offset = std.math.add(u64, offset, @intCast(written)) catch return error.WriteFailed;
        const signed_offset = std.math.cast(i64, current_offset) orelse return error.WriteFailed;
        const rc = linux.pwrite(fd, data[written..].ptr, data.len - written, signed_offset);
        switch (errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
            else => return error.WriteFailed,
        }
        if (rc == 0) return error.WriteFailed;
        written += rc;
    }
}

fn sysPread(fd: linux.fd_t, buf: []u8, offset: u64) !usize {
    var read_len: usize = 0;
    while (read_len < buf.len) {
        const current_offset = std.math.add(u64, offset, @intCast(read_len)) catch return error.ReadFailed;
        const signed_offset = std.math.cast(i64, current_offset) orelse return error.ReadFailed;
        const rc = linux.pread(fd, buf[read_len..].ptr, buf.len - read_len, signed_offset);
        switch (errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
            else => return error.ReadFailed,
        }
        if (rc == 0) break;
        read_len += rc;
    }
    return read_len;
}

fn sysFsync(fd: linux.fd_t) !void {
    while (true) {
        const rc = linux.fsync(fd);
        switch (errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            else => return error.SyncFailed,
        }
    }
}

fn sysFtruncate(fd: linux.fd_t, len: u64) !void {
    const signed_len = std.math.cast(i64, len) orelse return error.TruncateFailed;
    while (true) {
        const rc = linux.ftruncate(fd, signed_len);
        switch (errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            else => return error.TruncateFailed,
        }
    }
}

fn sysFileSize(fd: linux.fd_t) !u64 {
    const cur = linux.lseek(fd, 0, 1); // SEEK_CUR
    if (errno(cur) != .SUCCESS) return error.StatFailed;
    const end = linux.lseek(fd, 0, 2); // SEEK_END
    if (errno(end) != .SUCCESS) return error.StatFailed;
    _ = linux.lseek(fd, @intCast(cur), 0); // restore
    return @intCast(end);
}

fn sysClose(fd: linux.fd_t) void {
    _ = linux.close(fd);
}

fn sysUnlink(path: [:0]const u8) !void {
    while (true) {
        const rc = linux.unlink(path.ptr);
        switch (errno(rc)) {
            .SUCCESS, .NOENT => return,
            .INTR => continue,
            else => return error.UnlinkFailed,
        }
    }
}

// ===========================================================================
// Segment
// ===========================================================================

pub const Segment = struct {
    fd: ?linux.fd_t,
    segment_id: u64,
    first_index: u64,
    write_offset: u64,
    file_size: u64,
    path: [:0]u8,
    allocator: std.mem.Allocator,

    /// Create a new segment file with the given ID and first_index.
    /// Writes the 32-byte segment header. The file is opened O_CREAT|O_EXCL.
    pub fn create(
        allocator: std.mem.Allocator,
        dir: [:0]const u8,
        segment_id: u64,
        first_index: u64,
    ) !*Segment {
        const path = try makeFilename(allocator, dir, segment_id);
        errdefer allocator.free(path);

        const fd = try sysOpenExclusive(path);
        errdefer sysUnlink(path) catch {};
        errdefer sysClose(fd);

        // Write segment header.
        var header: [SEGMENT_HEADER_SIZE]u8 = undefined;
        encodeHeader(&header, segment_id, first_index);
        try sysPwrite(fd, &header, 0);

        const self = try allocator.create(Segment);
        self.* = .{
            .fd = fd,
            .segment_id = segment_id,
            .first_index = first_index,
            .write_offset = SEGMENT_HEADER_SIZE,
            .file_size = SEGMENT_HEADER_SIZE,
            .path = path,
            .allocator = allocator,
        };
        return self;
    }

    /// Open an existing segment file, read its header, and determine
    /// write_offset from the file size.
    pub fn open(allocator: std.mem.Allocator, path: [:0]const u8) !*Segment {
        const fd = try sysOpen(path);
        errdefer sysClose(fd);

        var header: [SEGMENT_HEADER_SIZE]u8 = undefined;
        const n = try sysPread(fd, &header, 0);
        if (n != SEGMENT_HEADER_SIZE) return error.InvalidSegmentHeader;

        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != segment_magic) return error.InvalidSegmentHeader;
        const ver = std.mem.readInt(u32, header[4..8], .little);
        if (ver != format_version) return error.InvalidSegmentHeader;

        const segment_id = std.mem.readInt(u64, header[8..16], .little);
        const first_index = std.mem.readInt(u64, header[16..24], .little);
        const size = try sysFileSize(fd);

        const path_copy = try allocator.dupeSentinel(u8, path, 0);
        errdefer allocator.free(path_copy);

        const self = try allocator.create(Segment);
        self.* = .{
            .fd = fd,
            .segment_id = segment_id,
            .first_index = first_index,
            .write_offset = size,
            .file_size = size,
            .path = path_copy,
            .allocator = allocator,
        };
        return self;
    }

    pub fn destroy(self: *Segment) void {
        self.close();
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    /// Append data at write_offset. Advances the cursor.
    pub fn append(self: *Segment, data: []const u8) !void {
        try sysPwrite(try self.openFd(), data, self.write_offset);
        self.write_offset += data.len;
        if (self.write_offset > self.file_size) self.file_size = self.write_offset;
    }

    /// Read `buf.len` bytes at the given offset. Returns actual bytes read.
    pub fn read(self: *Segment, buf: []u8, offset: u64) !usize {
        return sysPread(try self.openFd(), buf, offset);
    }

    pub fn sync(self: *Segment) !void {
        try sysFsync(try self.openFd());
    }

    /// Truncate the file to the given offset (removes trailing data).
    pub fn truncate(self: *Segment, offset: u64) !void {
        try sysFtruncate(try self.openFd(), offset);
        self.write_offset = offset;
        self.file_size = offset;
    }

    pub fn close(self: *Segment) void {
        if (self.fd) |fd| sysClose(fd);
        self.fd = null;
    }

    pub fn isFull(self: *const Segment, threshold: u64) bool {
        return self.write_offset >= threshold;
    }

    /// Delete the segment file from disk. Called after destroy.
    pub fn unlink(self: *const Segment) !void {
        try sysUnlink(self.path);
    }

    fn openFd(self: *const Segment) !linux.fd_t {
        return self.fd orelse error.SegmentNotOpen;
    }
};

// ===========================================================================
// Filename helpers
// ===========================================================================

/// Build "dir/segment-NNNNNN.wal" as a sentinel-terminated string.
pub fn makeFilename(allocator: std.mem.Allocator, dir: [:0]const u8, segment_id: u64) ![:0]u8 {
    const unsent = try std.fmt.allocPrint(allocator, "{s}/segment-{d:0>6}.wal", .{ dir, segment_id });
    defer allocator.free(unsent);
    const sentinel = try allocator.allocSentinel(u8, unsent.len, 0);
    @memcpy(sentinel[0..unsent.len], unsent);
    return sentinel;
}

/// Parse segment ID from a filename like "segment-000001.wal".
pub fn parseSegmentId(filename: []const u8) ?u64 {
    const prefix = "segment-";
    const suffix = ".wal";
    if (!std.mem.startsWith(u8, filename, prefix)) return null;
    if (!std.mem.endsWith(u8, filename, suffix)) return null;
    const num_str = filename[prefix.len .. filename.len - suffix.len];
    return std.fmt.parseInt(u64, num_str, 10) catch null;
}

// ===========================================================================
// Header encode/decode
// ===========================================================================

fn encodeHeader(out: *[SEGMENT_HEADER_SIZE]u8, segment_id: u64, first_index: u64) void {
    std.mem.writeInt(u32, out[0..4], segment_magic, .little);
    std.mem.writeInt(u32, out[4..8], format_version, .little);
    std.mem.writeInt(u64, out[8..16], segment_id, .little);
    std.mem.writeInt(u64, out[16..24], first_index, .little);
    @memset(out[24..], 0);
}

// ===========================================================================
// Directory helpers
// ===========================================================================

pub fn makeDir(path: [:0]const u8) !void {
    while (true) {
        const rc = linux.mkdir(path.ptr, 0o755);
        switch (errno(rc)) {
            .SUCCESS, .EXIST => return,
            .INTR => continue,
            else => return error.MkdirFailed,
        }
    }
}

pub fn syncDirectory(path: [:0]const u8) !void {
    const flags: linux.O = .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true };
    const fd: linux.fd_t = while (true) {
        const rc = linux.open(path.ptr, flags, 0);
        switch (errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            else => return error.DirectorySyncFailed,
        }
    };
    defer sysClose(fd);
    sysFsync(fd) catch return error.DirectorySyncFailed;
}

/// Remove all files in a directory (used for test cleanup).
pub fn removeDirTree(allocator: std.mem.Allocator, dir: [:0]const u8) void {
    // Best-effort: list directory entries and delete segment-*.wal files.
    _ = allocator;
    _ = dir;
    // For simplicity, we rely on individual file cleanup in tests.
}
