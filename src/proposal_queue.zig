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

    pub fn push(self: *ProposalQueue, data: []u8, ctx: []u8, callback: ProposalCallback) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        self.items.append(self.allocator, .{
            .data = data,
            .ctx = ctx,
            .callback = callback,
        }) catch {};
    }

    pub fn tryPop(self: *ProposalQueue) ?ProposalItem {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.items.items.len == 0) return null;
        return self.items.orderedRemove(0);
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

    pub fn push(self: *ReadIndexQueue, ctx: []u8, callback: ReadIndexCallback) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        self.items.append(self.allocator, .{ .ctx = ctx, .callback = callback }) catch {};
    }

    pub fn tryPop(self: *ReadIndexQueue) ?ReadIndexItem {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.items.items.len == 0) return null;
        return self.items.orderedRemove(0);
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
    q.push(data, ctx, .{ .ctx = undefined, .function = Cb.cb });

    const item = q.tryPop().?;
    try std.testing.expectEqualStrings("hello", item.data);
    try std.testing.expectEqualStrings("ctx1", item.ctx);
    std.testing.allocator.free(item.data);
    std.testing.allocator.free(item.ctx);

    try std.testing.expect(q.tryPop() == null);
}
