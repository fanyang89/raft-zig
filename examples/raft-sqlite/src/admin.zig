const std = @import("std");

pub const MemberAddress = struct {
    node_id: u64,
    address: []const u8,
};

pub const Operation = union(enum) {
    add_learner: MemberAddress,
    promote_member: MemberAddress,
    remove_member: u64,
    update_address: MemberAddress,
    transfer_leadership: u64,
    take_snapshot,
    inspect_membership,
};

pub const Member = struct {
    node_id: u64,
    address: []u8,
    voter: bool,
    learner: bool,
    outgoing_voter: bool,
    learner_next: bool,
    matched_index: u64,
    promotion_ready: bool,

    fn deinit(self: *Member, allocator: std.mem.Allocator) void {
        allocator.free(self.address);
        self.* = undefined;
    }
};

pub const Membership = struct {
    index: u64,
    members: []Member,
    retired_node_ids: []u64,

    pub fn deinit(self: *Membership, allocator: std.mem.Allocator) void {
        for (self.members) |*member| member.deinit(allocator);
        allocator.free(self.members);
        allocator.free(self.retired_node_ids);
        self.* = undefined;
    }
};

pub const Result = union(enum) {
    submitted: u64,
    membership: Membership,

    pub fn deinit(self: *Result, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .submitted => {},
            .membership => |*membership| membership.deinit(allocator),
        }
        self.* = undefined;
    }
};

pub const Pending = struct {
    operation: Operation,
    done: std.atomic.Value(bool) = .init(false),
    result: ?Result = null,
    failure: ?anyerror = null,

    pub fn wait(self: *Pending) void {
        while (!self.done.load(.acquire)) std.Thread.yield() catch {};
    }

    pub fn takeResult(self: *Pending) !Result {
        self.wait();
        if (self.failure) |failure| return failure;
        const result = self.result orelse return error.MissingAdminResult;
        self.result = null;
        return result;
    }

    pub fn complete(self: *Pending, result: Result) void {
        self.result = result;
        self.done.store(true, .release);
    }

    pub fn fail(self: *Pending, failure: anyerror) void {
        self.failure = failure;
        self.done.store(true, .release);
    }
};

pub const Queue = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    entries: []?*Pending,
    head: usize = 0,
    len: usize = 0,
    closed: bool = false,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Queue {
        if (capacity == 0) return error.InvalidConfig;
        const entries = try allocator.alloc(?*Pending, capacity);
        @memset(entries, null);
        return .{ .allocator = allocator, .entries = entries };
    }

    pub fn deinit(self: *Queue) void {
        std.debug.assert(self.closed and self.len == 0);
        self.allocator.free(self.entries);
        self.* = undefined;
    }

    pub fn submit(self: *Queue, pending: *Pending) !void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.closed) return error.ShuttingDown;
        if (self.len == self.entries.len) return error.AdminQueueFull;
        const index = (self.head + self.len) % self.entries.len;
        self.entries[index] = pending;
        self.len += 1;
    }

    pub fn tryPop(self: *Queue) ?*Pending {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.len == 0) return null;
        const pending = self.entries[self.head].?;
        self.entries[self.head] = null;
        self.head = (self.head + 1) % self.entries.len;
        self.len -= 1;
        return pending;
    }

    pub fn close(self: *Queue) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.closed) return;
        self.closed = true;
        while (self.len != 0) {
            const pending = self.entries[self.head].?;
            self.entries[self.head] = null;
            self.head = (self.head + 1) % self.entries.len;
            self.len -= 1;
            pending.fail(error.ShuttingDown);
        }
    }
};

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

test "admin queue is bounded and closes pending work" {
    var queue = try Queue.init(std.testing.allocator, 1);
    defer queue.deinit();
    var first: Pending = .{ .operation = .take_snapshot };
    var second: Pending = .{ .operation = .take_snapshot };
    try queue.submit(&first);
    try std.testing.expectError(error.AdminQueueFull, queue.submit(&second));
    try std.testing.expect(queue.tryPop() == &first);
    first.complete(.{ .submitted = 7 });
    var result = try first.takeResult();
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 7), result.submitted);

    try queue.submit(&second);
    queue.close();
    try std.testing.expectError(error.ShuttingDown, second.takeResult());
}
