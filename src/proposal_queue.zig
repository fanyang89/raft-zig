//! Thread-safe queues for cross-thread proposal and read-index submission.
//!
//! Ports the `ProposalQueue` and `ReadIndexQueue` from
//! `include/raftpp/raftor/proposal_tracker.h`. Users can call `push()` from
//! any thread; the event loop thread drains via `tryPop()`.
//!
//! Uses `std.atomic.Mutex` (spinlock) since Zig 0.16 removed
//! `std.Thread.Mutex`. Contention is brief (O(1) push/pop).

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

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {}
}

pub const ProposalQueue = struct {
    mutex: std.atomic.Mutex = .unlocked,
    items: std.ArrayList(ProposalItem),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ProposalQueue {
        return .{ .items = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *ProposalQueue) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        for (self.items.items) |*item| {
            self.allocator.free(item.data);
            self.allocator.free(item.ctx);
        }
        self.items.deinit(self.allocator);
    }

    pub fn push(self: *ProposalQueue, data: []u8, ctx: []u8, callback: ProposalCallback) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.items.append(self.allocator, .{
            .data = data,
            .ctx = ctx,
            .callback = callback,
        });
    }

    pub fn tryPop(self: *ProposalQueue) ?ProposalItem {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.items.items.len == 0) return null;
        return self.items.orderedRemove(0);
    }

    pub fn takeAll(self: *ProposalQueue) std.ArrayList(ProposalItem) {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const items = self.items;
        self.items = .empty;
        return items;
    }

    pub fn empty(self: *ProposalQueue) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.items.items.len == 0;
    }
};

pub const ReadIndexQueue = struct {
    mutex: std.atomic.Mutex = .unlocked,
    items: std.ArrayList(ReadIndexItem),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ReadIndexQueue {
        return .{ .items = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *ReadIndexQueue) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        for (self.items.items) |*item| self.allocator.free(item.ctx);
        self.items.deinit(self.allocator);
    }

    pub fn push(self: *ReadIndexQueue, ctx: []u8, callback: ReadIndexCallback) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.items.append(self.allocator, .{ .ctx = ctx, .callback = callback });
    }

    pub fn tryPop(self: *ReadIndexQueue) ?ReadIndexItem {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.items.items.len == 0) return null;
        return self.items.orderedRemove(0);
    }

    pub fn takeAll(self: *ReadIndexQueue) std.ArrayList(ReadIndexItem) {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const items = self.items;
        self.items = .empty;
        return items;
    }

    pub fn empty(self: *ReadIndexQueue) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.items.items.len == 0;
    }
};

test "proposal queue push and tryPop" {
    var q = ProposalQueue.init(std.testing.allocator);
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

test "proposal queue supports concurrent producers and consumption" {
    const allocator = std.heap.smp_allocator;
    const producer_count = 4;
    const items_per_producer = 128;

    var queue = ProposalQueue.init(allocator);
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
                const ctx = allocator.dupe(u8, "ctx") catch @panic("OOM");
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

test "read index queue supports concurrent producers and consumption" {
    const allocator = std.heap.smp_allocator;
    const producer_count = 4;
    const items_per_producer = 128;

    var queue = ReadIndexQueue.init(allocator);
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
