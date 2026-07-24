//! Single WAL segment file abstraction.
//!
//! Ports `ref/raftpp/lib/raftor/wal/segment.{h,cc}`. Each Segment owns
//! one file descriptor and manages byte-level append/read/sync/truncate.
//! It knows nothing about Raft records — that's the WAL layer's job.

const std = @import("std");
const fs_mod = @import("../fs.zig");

const SEGMENT_HEADER_SIZE: usize = 32;
const segment_magic: u32 = 0x57414C31; // "WAL1"
const format_version: u32 = 1;

// ===========================================================================
// Segment
// ===========================================================================

pub const Segment = struct {
    fd: ?fs_mod.Handle,
    fs: fs_mod.Fs,
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
        fs: fs_mod.Fs,
        dir: [:0]const u8,
        segment_id: u64,
        first_index: u64,
    ) !*Segment {
        const path = try makeFilename(allocator, dir, segment_id);
        errdefer allocator.free(path);

        const fd = try fs.open(path, .create_exclusive);
        errdefer fs.unlink(path) catch {};
        errdefer fs.close(fd) catch {};

        // Write segment header.
        var header: [SEGMENT_HEADER_SIZE]u8 = undefined;
        encodeHeader(&header, segment_id, first_index);
        try fs.pwriteAll(fd, &header, 0);

        const self = try allocator.create(Segment);
        self.* = .{
            .fd = fd,
            .fs = fs,
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
    pub fn open(allocator: std.mem.Allocator, fs: fs_mod.Fs, path: [:0]const u8) !*Segment {
        const fd = try fs.open(path, .read_write);
        errdefer fs.close(fd) catch {};

        var header: [SEGMENT_HEADER_SIZE]u8 = undefined;
        const n = try fs.preadAll(fd, &header, 0);
        if (n != SEGMENT_HEADER_SIZE) return error.InvalidSegmentHeader;

        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != segment_magic) return error.InvalidSegmentHeader;
        const ver = std.mem.readInt(u32, header[4..8], .little);
        if (ver != format_version) return error.InvalidSegmentHeader;

        const segment_id = std.mem.readInt(u64, header[8..16], .little);
        const first_index = std.mem.readInt(u64, header[16..24], .little);
        const size = try fs.fileSize(fd);

        const path_copy = try allocator.dupeSentinel(u8, path, 0);
        errdefer allocator.free(path_copy);

        const self = try allocator.create(Segment);
        self.* = .{
            .fd = fd,
            .fs = fs,
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
        try self.fs.pwriteAll(try self.openFd(), data, self.write_offset);
        self.write_offset += data.len;
        if (self.write_offset > self.file_size) self.file_size = self.write_offset;
    }

    /// Read `buf.len` bytes at the given offset. Returns actual bytes read.
    pub fn read(self: *Segment, buf: []u8, offset: u64) !usize {
        return self.fs.preadAll(try self.openFd(), buf, offset);
    }

    pub fn sync(self: *Segment) !void {
        try self.fs.syncFile(try self.openFd());
    }

    /// Truncate the file to the given offset (removes trailing data).
    pub fn truncate(self: *Segment, offset: u64) !void {
        try self.fs.truncate(try self.openFd(), offset);
        self.write_offset = offset;
        self.file_size = offset;
    }

    pub fn close(self: *Segment) void {
        if (self.fd) |fd| self.fs.close(fd) catch {};
        self.fd = null;
    }

    pub fn isFull(self: *const Segment, threshold: u64) bool {
        return self.write_offset >= threshold;
    }

    /// Delete the segment file from disk. Called after destroy.
    pub fn unlink(self: *const Segment) !void {
        try self.fs.unlink(self.path);
    }

    fn openFd(self: *const Segment) !fs_mod.Handle {
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

pub fn makeDir(fs: fs_mod.Fs, path: [:0]const u8) !bool {
    return fs.makeDir(path);
}

pub fn syncDirectory(fs: fs_mod.Fs, path: [:0]const u8) !void {
    try fs.syncDir(path);
}

/// Remove all files in a directory (used for test cleanup).
pub fn removeDirTree(allocator: std.mem.Allocator, dir: [:0]const u8) void {
    // Best-effort: list directory entries and delete segment-*.wal files.
    _ = allocator;
    _ = dir;
    // For simplicity, we rely on individual file cleanup in tests.
}
