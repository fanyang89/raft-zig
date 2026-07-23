//! Manages a collection of WAL segment files in a directory.
//!
//! Ports `ref/raftpp/lib/raftor/wal/segment_manager.{h,cc}`. The manager
//! owns Segment instances, handles segment rolling, and supports deleting
//! old segments during compaction. Segments are stored in a sorted
//! ArrayList for deterministic iteration order.

const std = @import("std");
const segment_mod = @import("segment.zig");

const Segment = segment_mod.Segment;
const linux = std.os.linux;

fn errno(rc: usize) i32 {
    const signed: isize = @bitCast(rc);
    if (signed >= 0) return 0;
    return @intCast(-signed);
}

const linux_dirent64 = extern struct {
    inode: u64,
    off: i64,
    reclen: u16,
    type: u8,
    name: [256]u8,
};

pub const SegmentEntry = struct {
    id: u64,
    segment: *Segment,
};

pub const SegmentManager = struct {
    segments: std.ArrayList(SegmentEntry),
    current_segment_id: u64 = 0,
    directory_dirty: bool = false,
    dir: [:0]u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, dir: [:0]const u8) !SegmentManager {
        const dir_copy = try allocator.dupeSentinel(u8, dir, 0);
        var sm = SegmentManager{
            .segments = .empty,
            .dir = dir_copy,
            .allocator = allocator,
        };
        errdefer sm.deinit();
        try sm.scanDirectory();
        return sm;
    }

    pub fn deinit(self: *SegmentManager) void {
        for (self.segments.items) |entry| entry.segment.destroy();
        self.segments.deinit(self.allocator);
        self.allocator.free(self.dir);
    }

    fn scanDirectory(self: *SegmentManager) !void {
        // Scan segment files by trying consecutive IDs: segment-000001.wal,
        // segment-000002.wal, ... Stop at the first missing one. This avoids
        // the complexity of getdents64.
        var sid: u64 = 1;
        while (true) {
            const path = try segment_mod.makeFilename(self.allocator, self.dir, sid);
            defer self.allocator.free(path);
            const seg = Segment.open(self.allocator, path) catch |err| switch (err) {
                error.FileNotFound => break,
                else => return err,
            };
            self.segments.append(self.allocator, .{ .id = sid, .segment = seg }) catch |err| {
                seg.destroy();
                return err;
            };
            self.current_segment_id = sid;
            sid += 1;
        }
    }

    pub fn getCurrent(self: *SegmentManager) ?*Segment {
        if (self.segments.items.len == 0) return null;
        for (self.segments.items) |*entry| {
            if (entry.id == self.current_segment_id) return entry.segment;
        }
        return null;
    }

    pub fn rollToNew(self: *SegmentManager, first_index: u64) !*Segment {
        if (self.getCurrent()) |cur| try cur.sync();
        try self.segments.ensureUnusedCapacity(self.allocator, 1);
        const new_id = self.current_segment_id + 1;
        const seg = try Segment.create(self.allocator, self.dir, new_id, first_index);
        self.segments.appendAssumeCapacity(.{ .id = new_id, .segment = seg });
        self.current_segment_id = new_id;
        self.directory_dirty = true;
        return seg;
    }

    pub fn removeSegmentsBefore(self: *SegmentManager, before_id: u64) !void {
        var i: usize = 0;
        while (i < self.segments.items.len) {
            if (self.segments.items[i].id < before_id) {
                try self.segments.items[i].segment.unlink();
                self.segments.items[i].segment.destroy();
                _ = self.segments.orderedRemove(i);
                self.directory_dirty = true;
            } else {
                i += 1;
            }
        }
    }

    pub fn removeAllSegments(self: *SegmentManager) !void {
        while (self.segments.items.len > 0) {
            const entry = self.segments.items[0];
            try entry.segment.unlink();
            entry.segment.destroy();
            _ = self.segments.orderedRemove(0);
            self.directory_dirty = true;
        }
        self.current_segment_id = 0;
        try self.syncDirectoryIfDirty();
    }

    pub fn syncAll(self: *SegmentManager) !void {
        for (self.segments.items) |entry| try entry.segment.sync();
        try self.syncDirectoryIfDirty();
    }

    pub fn closeAll(self: *SegmentManager) void {
        for (self.segments.items) |entry| entry.segment.close();
    }

    pub fn count(self: SegmentManager) usize {
        return self.segments.items.len;
    }

    /// Returns segment IDs in ascending order (segments list is kept sorted).
    pub fn sortedIds(self: *SegmentManager, allocator: std.mem.Allocator) ![]u64 {
        const ids = try allocator.alloc(u64, self.segments.items.len);
        for (self.segments.items, 0..) |entry, i| ids[i] = entry.id;
        return ids;
    }

    pub fn get(self: *SegmentManager, id: u64) ?*Segment {
        for (self.segments.items) |*entry| {
            if (entry.id == id) return entry.segment;
        }
        return null;
    }

    fn syncDirectoryIfDirty(self: *SegmentManager) !void {
        if (!self.directory_dirty) return;
        try segment_mod.syncDirectory(self.dir);
        self.directory_dirty = false;
    }
};
