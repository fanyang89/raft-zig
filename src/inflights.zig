//! In-flight message tracker for Raft replication.
//!
//! The tracker is a fixed-capacity ring buffer of entry indices that the
//! leader has sent to a follower but not yet seen acknowledged. Capacity can be
//! resized at runtime; the buffer is lazily allocated on the first `add`.

const std = @import("std");

/// Ring buffer of in-flight entry indices for one follower.
pub const Inflights = struct {
    start: usize,
    count: usize,
    /// High-water mark of slots ever written. Entries in `buffer.items[0..written]`
    /// are valid storage; the ring rotates within that range.
    buffer: std.ArrayList(u64),
    capacity: usize,
    /// Pending shrink applied lazily once the buffer drains. `null` when no
    /// resize is outstanding.
    incoming_capacity: ?usize,

    pub fn init(capacity: usize) Inflights {
        return .{
            .start = 0,
            .count = 0,
            .buffer = .empty,
            .capacity = capacity,
            .incoming_capacity = null,
        };
    }

    pub fn deinit(self: *Inflights, allocator: std.mem.Allocator) void {
        self.buffer.deinit(allocator);
        self.* = undefined;
    }

    /// Adjust capacity. Growth takes effect immediately; shrinkage is deferred
    /// until the buffer is empty unless `count == 0`.
    pub fn setCapacity(self: *Inflights, allocator: std.mem.Allocator, incoming_capacity: usize) !void {
        if (self.capacity == incoming_capacity) {
            self.incoming_capacity = null;
            return;
        }

        if (self.capacity < incoming_capacity) {
            if (self.start + self.count <= self.capacity) {
                if (self.buffer.capacity > 0) {
                    try self.buffer.ensureTotalCapacityPrecise(allocator, incoming_capacity);
                }
            } else {
                std.debug.assert(self.capacity == self.buffer.items.len);
                var buffer: std.ArrayList(u64) = .empty;
                try buffer.ensureTotalCapacityPrecise(allocator, incoming_capacity);
                try buffer.appendSlice(allocator, self.buffer.items[self.start..]);
                const tail_count = self.count - (self.capacity - self.start);
                try buffer.appendSlice(allocator, self.buffer.items[0..tail_count]);
                self.buffer.deinit(allocator);
                self.buffer = buffer;
                self.start = 0;
            }
            self.capacity = incoming_capacity;
            self.incoming_capacity = null;
            return;
        }

        // incoming_capacity < self.capacity
        if (self.count == 0) {
            self.capacity = incoming_capacity;
            self.incoming_capacity = null;
            self.start = 0;
            if (self.buffer.capacity > 0) {
                var fresh: std.ArrayList(u64) = .empty;
                try fresh.ensureTotalCapacityPrecise(allocator, incoming_capacity);
                self.buffer.deinit(allocator);
                self.buffer = fresh;
            }
        } else {
            self.incoming_capacity = incoming_capacity;
        }
    }

    pub fn reset(self: *Inflights, allocator: std.mem.Allocator) void {
        self.count = 0;
        self.start = 0;
        self.buffer.clearAndFree(allocator);

        if (self.incoming_capacity) |cap| {
            self.capacity = cap;
            self.incoming_capacity = null;
        }
    }

    /// Release all entries up to and including `to`.
    pub fn freeTo(self: *Inflights, allocator: std.mem.Allocator, to: u64) void {
        if (self.count == 0 or to < self.buffer.items[self.start]) {
            return;
        }

        var i: usize = 0;
        var idx = self.start;
        while (i < self.count) : (i += 1) {
            if (to < self.buffer.items[idx]) {
                break;
            }
            idx += 1;
            if (idx >= self.capacity) idx -= self.capacity;
        }

        self.count -= i;
        self.start = idx;

        if (self.count == 0) {
            if (self.incoming_capacity) |incoming_cap| {
                self.start = 0;
                self.capacity = incoming_cap;
                self.buffer.clearAndFree(allocator);
                _ = self.buffer.ensureTotalCapacityPrecise(allocator, incoming_cap) catch {};
                self.incoming_capacity = null;
            }
        }
    }

    pub fn freeFirstOne(self: *Inflights, allocator: std.mem.Allocator) void {
        if (self.count > 0) {
            const start = self.buffer.items[self.start];
            self.freeTo(allocator, start);
        }
    }

    /// Append `inflight` to the ring. Panics if the buffer is full — callers
    /// must check `full()` first.
    pub fn add(self: *Inflights, allocator: std.mem.Allocator, inflight: u64) !void {
        if (self.full()) {
            @panic("inflights full"); // KCOV_EXCL_LINE
        }

        if (self.buffer.capacity == 0) {
            std.debug.assert(self.count == 0);
            std.debug.assert(self.start == 0);
            std.debug.assert(self.incoming_capacity == null);
            try self.buffer.ensureTotalCapacityPrecise(allocator, self.capacity);
        }

        var next = self.start + self.count;
        if (next >= self.capacity) next -= self.capacity;
        std.debug.assert(next <= self.buffer.items.len);
        if (next == self.buffer.items.len) {
            self.buffer.appendAssumeCapacity(inflight);
        } else {
            self.buffer.items[next] = inflight;
        }
        self.count += 1;
    }

    pub fn full(self: Inflights) bool {
        if (self.count == self.capacity) return true;
        if (self.incoming_capacity) |cap| {
            return self.count >= cap;
        }
        return false;
    }

    pub fn count_(self: Inflights) usize {
        return self.count;
    }

    pub fn bufferSize(self: Inflights) usize {
        return self.buffer.capacity;
    }

    pub fn bufferIsAllocated(self: Inflights) bool {
        return self.buffer.capacity > 0;
    }
};

// KCOV_EXCL_START
test "add linear then wrap" {
    const allocator = std.testing.allocator;
    var inflight = Inflights.init(10);
    defer inflight.deinit(allocator);

    var i: u64 = 0;
    while (i < 5) : (i += 1) try inflight.add(allocator, i);
    try std.testing.expectEqual(@as(usize, 0), inflight.start);
    try std.testing.expectEqual(@as(usize, 5), inflight.count);
    try std.testing.expectEqualSlices(u64, &.{ 0, 1, 2, 3, 4 }, inflight.buffer.items);
    try std.testing.expectEqual(@as(usize, 10), inflight.capacity);
    try std.testing.expect(inflight.incoming_capacity == null);

    while (i < 10) : (i += 1) try inflight.add(allocator, i);
    try std.testing.expectEqual(@as(usize, 0), inflight.start);
    try std.testing.expectEqual(@as(usize, 10), inflight.count);
    try std.testing.expectEqualSlices(u64, &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, inflight.buffer.items);
}

test "add into preallocated slots rotates correctly" {
    const allocator = std.testing.allocator;
    var inflight = Inflights.init(10);
    defer inflight.deinit(allocator);

    // Manually set up a wrapped state: start at 5, 5 zero-filled slots allocated.
    inflight.start = 5;
    var filler: [5]u64 = .{ 0, 0, 0, 0, 0 };
    try inflight.buffer.appendSlice(allocator, &filler);

    var i: u64 = 0;
    while (i < 5) : (i += 1) try inflight.add(allocator, i);
    try std.testing.expectEqual(@as(usize, 5), inflight.start);
    try std.testing.expectEqual(@as(usize, 5), inflight.count);
    try std.testing.expectEqualSlices(u64, &.{ 0, 0, 0, 0, 0, 0, 1, 2, 3, 4 }, inflight.buffer.items);

    while (i < 10) : (i += 1) try inflight.add(allocator, i);
    try std.testing.expectEqual(@as(usize, 5), inflight.start);
    try std.testing.expectEqual(@as(usize, 10), inflight.count);
    try std.testing.expectEqualSlices(u64, &.{ 5, 6, 7, 8, 9, 0, 1, 2, 3, 4 }, inflight.buffer.items);
}

test "freeTo slides the window and wraps" {
    const allocator = std.testing.allocator;
    var inflight = Inflights.init(10);
    defer inflight.deinit(allocator);

    var i: u64 = 0;
    while (i < 10) : (i += 1) try inflight.add(allocator, i);

    inflight.freeTo(allocator, 4);
    try std.testing.expectEqual(@as(usize, 5), inflight.start);
    try std.testing.expectEqual(@as(usize, 5), inflight.count);

    inflight.freeTo(allocator, 8);
    try std.testing.expectEqual(@as(usize, 9), inflight.start);
    try std.testing.expectEqual(@as(usize, 1), inflight.count);

    while (i < 15) : (i += 1) try inflight.add(allocator, i);

    inflight.freeTo(allocator, 12);
    try std.testing.expectEqual(@as(usize, 3), inflight.start);
    try std.testing.expectEqual(@as(usize, 2), inflight.count);
    try std.testing.expectEqualSlices(u64, &.{ 10, 11, 12, 13, 14, 5, 6, 7, 8, 9 }, inflight.buffer.items);

    inflight.freeTo(allocator, 14);
    try std.testing.expectEqual(@as(usize, 5), inflight.start);
    try std.testing.expectEqual(@as(usize, 0), inflight.count);
}

test "freeFirstOne releases the oldest inflight" {
    const allocator = std.testing.allocator;
    var inflight = Inflights.init(10);
    defer inflight.deinit(allocator);

    var i: u64 = 0;
    while (i < 10) : (i += 1) try inflight.add(allocator, i);

    inflight.freeFirstOne(allocator);
    try std.testing.expectEqual(@as(usize, 1), inflight.start);
    try std.testing.expectEqual(@as(usize, 9), inflight.count);
}

test "setCapacity grows the buffer at multiple start offsets" {
    const allocator = std.testing.allocator;
    const starts = [_]usize{ 16, 112, 120 };
    for (starts) |start| {
        var inflight = Inflights.init(128);
        defer inflight.deinit(allocator);

        var i: u64 = 0;
        while (i < start) : (i += 1) try inflight.add(allocator, i);
        inflight.freeTo(allocator, start - 1);
        while (i < start + 16) : (i += 1) try inflight.add(allocator, i);

        try std.testing.expectEqual(@as(usize, 16), inflight.count);
        try std.testing.expectEqual(start, inflight.start);

        try inflight.setCapacity(allocator, 1024);
        try std.testing.expectEqual(@as(usize, 1024), inflight.capacity);
        try std.testing.expect(inflight.incoming_capacity == null);
        try std.testing.expectEqual(@as(usize, 1024), inflight.buffer.capacity);
        if (start != 120) {
            try std.testing.expect(inflight.start != 0);
        } else {
            try std.testing.expectEqual(@as(usize, 0), inflight.start);
        }
    }
}

test "ordinary boundaries and deferred shrink" {
    const allocator = std.testing.allocator;
    var inflight = Inflights.init(4);
    defer inflight.deinit(allocator);

    inflight.freeTo(allocator, 1);
    inflight.freeFirstOne(allocator);
    try inflight.setCapacity(allocator, 4);
    try inflight.setCapacity(allocator, 6);
    try std.testing.expectEqual(@as(usize, 6), inflight.capacity);
    try std.testing.expect(!inflight.bufferIsAllocated());

    try inflight.add(allocator, 3);
    try inflight.add(allocator, 5);
    inflight.freeTo(allocator, 2);
    try std.testing.expectEqual(@as(usize, 2), inflight.count_());

    try inflight.setCapacity(allocator, 1);
    try std.testing.expect(inflight.full());
    try std.testing.expectEqual(@as(?usize, 1), inflight.incoming_capacity);
    inflight.freeTo(allocator, 5);
    try std.testing.expectEqual(@as(usize, 0), inflight.count_());
    try std.testing.expectEqual(@as(usize, 1), inflight.capacity);
    try std.testing.expect(inflight.incoming_capacity == null);
}

test "reset applies a deferred shrink" {
    const allocator = std.testing.allocator;
    var inflight = Inflights.init(4);
    defer inflight.deinit(allocator);

    try inflight.add(allocator, 1);
    try inflight.setCapacity(allocator, 2);
    inflight.reset(allocator);

    try std.testing.expectEqual(@as(usize, 2), inflight.capacity);
    try std.testing.expectEqual(@as(usize, 0), inflight.count);
    try std.testing.expect(inflight.incoming_capacity == null);
}
// KCOV_EXCL_STOP
