const std = @import("std");

pub const IndexEntry = struct {
    segment_id: u64,
    offset: u64,
    length: u32,
    term: u64,
};

pub const WALIndex = struct {
    entries: std.ArrayList(IndexEntry) = .empty,
    first_index: u64 = 1,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) WALIndex {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *WALIndex) void {
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn ensureUnusedCapacity(self: *WALIndex, count: usize) !void {
        try self.entries.ensureUnusedCapacity(self.allocator, count);
    }

    pub fn insertAssumeCapacity(self: *WALIndex, index: u64, entry: IndexEntry) !void {
        const expected = try std.math.add(u64, self.first_index, self.entries.items.len);
        if (index != expected) return error.Fatal;
        self.entries.appendAssumeCapacity(entry);
    }

    pub fn lookup(self: WALIndex, index: u64) ?IndexEntry {
        if (self.entries.items.len == 0 or index < self.first_index or index > self.lastIndex()) return null;
        return self.entries.items[@intCast(index - self.first_index)];
    }

    pub fn term(self: WALIndex, index: u64) ?u64 {
        return if (self.lookup(index)) |entry| entry.term else null;
    }

    pub fn truncateFrom(self: *WALIndex, index: u64) void {
        if (self.entries.items.len == 0 or index > self.lastIndex()) return;
        if (index <= self.first_index) {
            self.entries.clearRetainingCapacity();
            return;
        }
        self.entries.shrinkRetainingCapacity(@intCast(index - self.first_index));
    }

    pub fn truncateBefore(self: *WALIndex, index: u64) void {
        if (index <= self.first_index) return;
        if (self.entries.items.len == 0 or index > self.lastIndex()) {
            self.entries.clearRetainingCapacity();
            self.first_index = index;
            return;
        }
        const remove_count: usize = @intCast(index - self.first_index);
        std.mem.copyForwards(IndexEntry, self.entries.items[0..], self.entries.items[remove_count..]);
        self.entries.shrinkRetainingCapacity(self.entries.items.len - remove_count);
        self.first_index = index;
    }

    pub fn lastIndex(self: WALIndex) u64 {
        if (self.entries.items.len == 0) return self.first_index -| 1;
        return self.first_index + self.entries.items.len - 1;
    }

    pub fn setFirstIndex(self: *WALIndex, index: u64) void {
        self.first_index = index;
    }

    pub fn reset(self: *WALIndex, first_index: u64) void {
        self.entries.clearRetainingCapacity();
        self.first_index = first_index;
    }
};

test "WALIndex inserts and truncates locations" {
    var index = WALIndex.init(std.testing.allocator);
    defer index.deinit();
    index.setFirstIndex(3);
    try index.ensureUnusedCapacity(3);
    try index.insertAssumeCapacity(3, .{ .segment_id = 1, .offset = 32, .length = 48, .term = 1 });
    try index.insertAssumeCapacity(4, .{ .segment_id = 2, .offset = 32, .length = 48, .term = 2 });
    try index.insertAssumeCapacity(5, .{ .segment_id = 2, .offset = 80, .length = 48, .term = 2 });
    try std.testing.expectEqual(@as(u64, 2), index.term(4).?);

    index.truncateFrom(5);
    try std.testing.expectEqual(@as(u64, 4), index.lastIndex());
    index.truncateFrom(3);
    try std.testing.expectEqual(@as(usize, 0), index.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), index.lastIndex());

    try index.ensureUnusedCapacity(1);
    try index.insertAssumeCapacity(3, .{ .segment_id = 3, .offset = 32, .length = 48, .term = 3 });
    index.truncateFrom(2);
    try std.testing.expectEqual(@as(usize, 0), index.entries.items.len);
    index.truncateBefore(4);
    try std.testing.expect(index.lookup(3) == null);
    try std.testing.expectEqual(@as(u64, 4), index.first_index);
}
