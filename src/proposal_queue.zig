//! Thread-safe queues for cross-thread proposal and read-index submission.
//!
//! Users can call `push()` from any thread; the event loop thread drains via
//! `tryPop()`.
//!
//! Uses `std.atomic.Mutex` (spinlock) around O(1) deque push/pop operations.

const std = @import("std");

const proposal_tracker_mod = @import("proposal_tracker.zig");

const ProposalCallback = proposal_tracker_mod.ProposalCallback;
const ReadIndexCallback = proposal_tracker_mod.ReadIndexCallback;

pub const ProposalItem = struct {
    data: []u8,
    ctx: []u8,
    callback: ProposalCallback,
};

pub const ReadIndexItem = struct {
    ctx: []u8,
    callback: ReadIndexCallback,
};

pub const ReadIndexQueueLimits = struct {
    max_items: usize = std.math.maxInt(usize),
    max_bytes: usize = std.math.maxInt(usize),
};

pub const ProposalQueueLimits = struct {
    max_items: usize = std.math.maxInt(usize),
    max_bytes: usize = std.math.maxInt(usize),
};

pub const QueueStats = struct {
    count: usize,
    bytes: usize,
};

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {}
}

pub const ProposalQueue = struct {
    mutex: std.atomic.Mutex = .unlocked,
    items: std.Deque(ProposalItem),
    allocator: std.mem.Allocator,
    limits: ProposalQueueLimits,
    queued_bytes: usize = 0,

    pub fn init(allocator: std.mem.Allocator, limits: ProposalQueueLimits) ProposalQueue {
        return .{ .items = .empty, .allocator = allocator, .limits = limits };
    }

    pub fn deinit(self: *ProposalQueue) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        var iterator = self.items.iterator();
        while (iterator.next()) |item| {
            self.allocator.free(item.data);
            self.allocator.free(item.ctx);
        }
        self.items.deinit(self.allocator);
    }

    pub fn push(self: *ProposalQueue, data: []u8, ctx: []u8, callback: ProposalCallback) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.items.len >= self.limits.max_items) return error.ProposalBackpressure;
        const item_bytes = std.math.add(usize, data.len, ctx.len) catch return error.ProposalBackpressure;
        if (item_bytes > self.limits.max_bytes -| self.queued_bytes) return error.ProposalBackpressure;
        try self.items.pushBack(self.allocator, .{
            .data = data,
            .ctx = ctx,
            .callback = callback,
        });
        self.queued_bytes += item_bytes;
    }

    pub fn tryPop(self: *ProposalQueue) ?ProposalItem {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const item = self.items.popFront() orelse return null;
        self.queued_bytes -= item.data.len + item.ctx.len;
        return item;
    }

    pub fn takeAll(self: *ProposalQueue) std.Deque(ProposalItem) {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const items = self.items;
        self.items = .empty;
        self.queued_bytes = 0;
        return items;
    }

    pub fn empty(self: *ProposalQueue) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.items.len == 0;
    }

    pub fn stats(self: *const ProposalQueue) QueueStats {
        const mutex = @constCast(&self.mutex);
        spinLock(mutex);
        defer mutex.unlock();
        return .{ .count = self.items.len, .bytes = self.queued_bytes };
    }
};

pub const ReadIndexQueue = struct {
    mutex: std.atomic.Mutex = .unlocked,
    items: std.Deque(ReadIndexItem),
    allocator: std.mem.Allocator,
    limits: ReadIndexQueueLimits,
    queued_bytes: usize = 0,

    pub fn init(allocator: std.mem.Allocator, limits: ReadIndexQueueLimits) ReadIndexQueue {
        return .{ .items = .empty, .allocator = allocator, .limits = limits };
    }

    pub fn deinit(self: *ReadIndexQueue) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        var iterator = self.items.iterator();
        while (iterator.next()) |item| self.allocator.free(item.ctx);
        self.items.deinit(self.allocator);
    }

    pub fn push(self: *ReadIndexQueue, ctx: []u8, callback: ReadIndexCallback) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.items.len >= self.limits.max_items) return error.ReadIndexBackpressure;
        if (ctx.len > self.limits.max_bytes -| self.queued_bytes) return error.ReadIndexBackpressure;
        try self.items.pushBack(self.allocator, .{ .ctx = ctx, .callback = callback });
        self.queued_bytes += ctx.len;
    }

    pub fn tryPop(self: *ReadIndexQueue) ?ReadIndexItem {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const item = self.items.popFront() orelse return null;
        self.queued_bytes -= item.ctx.len;
        return item;
    }

    pub fn takeAll(self: *ReadIndexQueue) std.Deque(ReadIndexItem) {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const items = self.items;
        self.items = .empty;
        self.queued_bytes = 0;
        return items;
    }

    pub fn empty(self: *ReadIndexQueue) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.items.len == 0;
    }

    pub fn stats(self: *const ReadIndexQueue) QueueStats {
        const mutex = @constCast(&self.mutex);
        spinLock(mutex);
        defer mutex.unlock();
        return .{ .count = self.items.len, .bytes = self.queued_bytes };
    }
};

// KCOV_EXCL_START
test "proposal queue push and tryPop" {
    var q = ProposalQueue.init(std.testing.allocator, .{});
    defer q.deinit();

    try std.testing.expect(q.tryPop() == null);

    const Cb = struct {
        fn cb(_: *anyopaque, _: proposal_tracker_mod.ProposalResult) void {}
    };
    const data = try std.testing.allocator.dupe(u8, "hello");
    const ctx = try std.testing.allocator.dupe(u8, "ctx1");
    try q.push(data, ctx, .{ .ctx = undefined, .function = Cb.cb });

    const item = q.tryPop().?;
    try std.testing.expectEqualStrings("hello", item.data);
    try std.testing.expectEqualStrings("ctx1", item.ctx);
    std.testing.allocator.free(item.data);
    std.testing.allocator.free(item.ctx);

    try std.testing.expect(q.tryPop() == null);
}

test "proposal and read queues deinit pending owned items" {
    const Cb = struct {
        fn proposal(_: *anyopaque, _: proposal_tracker_mod.ProposalResult) void {}
        fn read(_: *anyopaque, _: proposal_tracker_mod.ReadIndexResult) void {}
    };

    var proposals = ProposalQueue.init(std.testing.allocator, .{});
    try proposals.push(
        try std.testing.allocator.dupe(u8, "data"),
        try std.testing.allocator.dupe(u8, "proposal"),
        .{ .ctx = undefined, .function = Cb.proposal },
    );
    proposals.deinit();

    var reads = ReadIndexQueue.init(std.testing.allocator, .{});
    try reads.push(
        try std.testing.allocator.dupe(u8, "read"),
        .{ .ctx = undefined, .function = Cb.read },
    );
    reads.deinit();
}

test "proposal queue preserves FIFO order across deque wrap-around" {
    var queue = ProposalQueue.init(std.testing.allocator, .{});
    defer queue.deinit();

    const Cb = struct {
        fn callback(_: *anyopaque, _: proposal_tracker_mod.ProposalResult) void {}
    };
    const callback = ProposalCallback{ .ctx = undefined, .function = Cb.callback };

    for (0..16) |i| {
        const value = try std.fmt.allocPrint(std.testing.allocator, "{}", .{i});
        errdefer std.testing.allocator.free(value);
        const ctx = try std.testing.allocator.dupe(u8, value);
        errdefer std.testing.allocator.free(ctx);
        try queue.push(value, ctx, callback);
    }
    for (0..8) |i| {
        const item = queue.tryPop().?;
        defer std.testing.allocator.free(item.data);
        defer std.testing.allocator.free(item.ctx);
        const expected = try std.fmt.allocPrint(std.testing.allocator, "{}", .{i});
        defer std.testing.allocator.free(expected);
        try std.testing.expectEqualStrings(expected, item.data);
    }
    for (16..32) |i| {
        const value = try std.fmt.allocPrint(std.testing.allocator, "{}", .{i});
        errdefer std.testing.allocator.free(value);
        const ctx = try std.testing.allocator.dupe(u8, value);
        errdefer std.testing.allocator.free(ctx);
        try queue.push(value, ctx, callback);
    }
    for (8..32) |i| {
        const item = queue.tryPop().?;
        defer std.testing.allocator.free(item.data);
        defer std.testing.allocator.free(item.ctx);
        const expected = try std.fmt.allocPrint(std.testing.allocator, "{}", .{i});
        defer std.testing.allocator.free(expected);
        try std.testing.expectEqualStrings(expected, item.data);
    }
    try std.testing.expect(queue.empty());
}

test "proposal queue enforces item and byte limits" {
    var queue = ProposalQueue.init(std.testing.allocator, .{ .max_items = 1, .max_bytes = 7 });
    defer queue.deinit();

    const Cb = struct {
        fn callback(_: *anyopaque, _: proposal_tracker_mod.ProposalResult) void {}
    };
    const callback = ProposalCallback{ .ctx = undefined, .function = Cb.callback };
    const data = try std.testing.allocator.dupe(u8, "data");
    const ctx = try std.testing.allocator.dupe(u8, "ctx");
    try queue.push(data, ctx, callback);
    try std.testing.expectEqual(QueueStats{ .count = 1, .bytes = 7 }, queue.stats());

    const rejected_data = try std.testing.allocator.dupe(u8, "x");
    defer std.testing.allocator.free(rejected_data);
    const rejected_ctx = try std.testing.allocator.dupe(u8, "y");
    defer std.testing.allocator.free(rejected_ctx);
    try std.testing.expectError(error.ProposalBackpressure, queue.push(rejected_data, rejected_ctx, callback));
    try std.testing.expectEqual(QueueStats{ .count = 1, .bytes = 7 }, queue.stats());

    const item = queue.tryPop().?;
    std.testing.allocator.free(item.data);
    std.testing.allocator.free(item.ctx);
    try std.testing.expectEqual(QueueStats{ .count = 0, .bytes = 0 }, queue.stats());

    const oversized_data = try std.testing.allocator.dupe(u8, "oversized");
    defer std.testing.allocator.free(oversized_data);
    const oversized_ctx = try std.testing.allocator.dupe(u8, "ctx");
    defer std.testing.allocator.free(oversized_ctx);
    try std.testing.expectError(error.ProposalBackpressure, queue.push(oversized_data, oversized_ctx, callback));
    try std.testing.expect(queue.empty());
}

test "proposal queue supports concurrent producers and consumption" {
    const allocator = std.heap.smp_allocator;
    const producer_count = 4;
    const items_per_producer = 128;

    var queue = ProposalQueue.init(allocator, .{});
    defer queue.deinit();
    var producers_done = std.atomic.Value(usize).init(0);

    const Cb = struct {
        fn callback(_: *anyopaque, _: proposal_tracker_mod.ProposalResult) void {}
    };
    const Producer = struct {
        queue: *ProposalQueue,
        done: *std.atomic.Value(usize),

        fn run(self: *@This()) void {
            for (0..items_per_producer) |_| {
                const data = allocator.dupe(u8, "data") catch @panic("OOM");
                const ctx = allocator.dupe(u8, "ctx") catch {
                    allocator.free(data);
                    @panic("OOM");
                };
                self.queue.push(data, ctx, .{ .ctx = self.queue, .function = Cb.callback }) catch unreachable;
            }
            _ = self.done.fetchAdd(1, .release);
        }
    };

    var producers: [producer_count]Producer = undefined;
    var threads: [producer_count]std.Thread = undefined;
    var started: usize = 0;
    errdefer for (threads[0..started]) |thread| thread.join();
    for (&producers, &threads) |*producer, *thread| {
        producer.* = .{ .queue = &queue, .done = &producers_done };
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{producer});
        started += 1;
    }

    var consumed: usize = 0;
    while (producers_done.load(.acquire) != producer_count or !queue.empty()) {
        if (queue.tryPop()) |item| {
            allocator.free(item.data);
            allocator.free(item.ctx);
            consumed += 1;
        } else {
            std.atomic.spinLoopHint();
        }
    }
    for (&threads) |*thread| thread.join();
    started = 0;

    try std.testing.expectEqual(producer_count * items_per_producer, consumed);
    try std.testing.expect(queue.empty());
}

test "proposal queue limits concurrent producers" {
    const allocator = std.heap.smp_allocator;
    const producer_count = 4;
    const items_per_producer = 64;
    const max_items = 64;
    const item_bytes = 7;

    var queue = ProposalQueue.init(allocator, .{ .max_items = max_items, .max_bytes = max_items * item_bytes });
    defer queue.deinit();
    var accepted = std.atomic.Value(usize).init(0);
    var rejected = std.atomic.Value(usize).init(0);

    const Cb = struct {
        fn callback(_: *anyopaque, _: proposal_tracker_mod.ProposalResult) void {}
    };
    const Producer = struct {
        queue: *ProposalQueue,
        accepted: *std.atomic.Value(usize),
        rejected: *std.atomic.Value(usize),

        fn run(self: *@This()) void {
            for (0..items_per_producer) |_| {
                const data = allocator.dupe(u8, "data") catch @panic("OOM");
                const ctx = allocator.dupe(u8, "ctx") catch @panic("OOM");
                if (self.queue.push(data, ctx, .{ .ctx = self.queue, .function = Cb.callback })) |_| {
                    _ = self.accepted.fetchAdd(1, .monotonic);
                } else |err| switch (err) {
                    error.ProposalBackpressure => {
                        allocator.free(data);
                        allocator.free(ctx);
                        _ = self.rejected.fetchAdd(1, .monotonic);
                    },
                    error.OutOfMemory => @panic("OOM"),
                }
            }
        }
    };

    var producers: [producer_count]Producer = undefined;
    var threads: [producer_count]std.Thread = undefined;
    var started: usize = 0;
    errdefer for (threads[0..started]) |thread| thread.join();
    for (&producers, &threads) |*producer, *thread| {
        producer.* = .{ .queue = &queue, .accepted = &accepted, .rejected = &rejected };
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{producer});
        started += 1;
    }
    for (&threads) |*thread| thread.join();
    started = 0;

    try std.testing.expectEqual(QueueStats{ .count = max_items, .bytes = max_items * item_bytes }, queue.stats());
    try std.testing.expectEqual(max_items, accepted.load(.monotonic));
    try std.testing.expectEqual(producer_count * items_per_producer - max_items, rejected.load(.monotonic));
    while (queue.tryPop()) |item| {
        allocator.free(item.data);
        allocator.free(item.ctx);
    }
    try std.testing.expectEqual(QueueStats{ .count = 0, .bytes = 0 }, queue.stats());
}

test "read index queue enforces item and byte limits" {
    var queue = ReadIndexQueue.init(std.testing.allocator, .{ .max_items = 1, .max_bytes = 3 });
    defer queue.deinit();

    const Cb = struct {
        fn callback(_: *anyopaque, _: proposal_tracker_mod.ReadIndexResult) void {}
    };
    const callback = ReadIndexCallback{ .ctx = undefined, .function = Cb.callback };
    const ctx = try std.testing.allocator.dupe(u8, "ctx");
    try queue.push(ctx, callback);
    try std.testing.expectEqual(QueueStats{ .count = 1, .bytes = 3 }, queue.stats());

    const rejected = try std.testing.allocator.dupe(u8, "x");
    defer std.testing.allocator.free(rejected);
    try std.testing.expectError(error.ReadIndexBackpressure, queue.push(rejected, callback));
    try std.testing.expectEqual(QueueStats{ .count = 1, .bytes = 3 }, queue.stats());

    const item = queue.tryPop().?;
    std.testing.allocator.free(item.ctx);
    try std.testing.expectEqual(QueueStats{ .count = 0, .bytes = 0 }, queue.stats());

    const oversized = try std.testing.allocator.dupe(u8, "four");
    defer std.testing.allocator.free(oversized);
    try std.testing.expectError(error.ReadIndexBackpressure, queue.push(oversized, callback));
}

test "read index queue supports concurrent producers and consumption" {
    const allocator = std.heap.smp_allocator;
    const producer_count = 4;
    const items_per_producer = 128;

    var queue = ReadIndexQueue.init(allocator, .{});
    defer queue.deinit();
    var producers_done = std.atomic.Value(usize).init(0);

    const Cb = struct {
        fn callback(_: *anyopaque, _: proposal_tracker_mod.ReadIndexResult) void {}
    };
    const Producer = struct {
        queue: *ReadIndexQueue,
        done: *std.atomic.Value(usize),

        fn run(self: *@This()) void {
            for (0..items_per_producer) |_| {
                const ctx = allocator.dupe(u8, "ctx") catch @panic("OOM");
                self.queue.push(ctx, .{ .ctx = self.queue, .function = Cb.callback }) catch unreachable;
            }
            _ = self.done.fetchAdd(1, .release);
        }
    };

    var producers: [producer_count]Producer = undefined;
    var threads: [producer_count]std.Thread = undefined;
    var started: usize = 0;
    errdefer for (threads[0..started]) |thread| thread.join();
    for (&producers, &threads) |*producer, *thread| {
        producer.* = .{ .queue = &queue, .done = &producers_done };
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{producer});
        started += 1;
    }

    var consumed: usize = 0;
    while (producers_done.load(.acquire) != producer_count or !queue.empty()) {
        if (queue.tryPop()) |item| {
            allocator.free(item.ctx);
            consumed += 1;
        } else {
            std.atomic.spinLoopHint();
        }
    }
    for (&threads) |*thread| thread.join();
    started = 0;

    try std.testing.expectEqual(producer_count * items_per_producer, consumed);
    try std.testing.expect(queue.empty());
}
// KCOV_EXCL_STOP
