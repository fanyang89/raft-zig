//! Bounded thread-safe inbound queue between grpc and the Raftor event loop.

const std = @import("std");
const Error = @import("../core/error.zig").Error;
const Message = @import("../core/types.zig").Message;

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub const InboundMailbox = struct {
    pub const Limits = struct {
        max_messages: usize,
        max_bytes: usize,

        pub fn validate(self: Limits) !void {
            if (self.max_messages == 0 or self.max_bytes == 0) return error.InvalidConfig;
        }
    };

    const Item = struct {
        message: Message,
        encoded_size: usize,
    };

    mutex: std.atomic.Mutex = .unlocked,
    inbox: std.ArrayList(Item) = .empty,
    head: usize = 0,
    encoded_bytes: usize = 0,
    limits: Limits,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, limits: Limits) !InboundMailbox {
        try limits.validate();
        var self = InboundMailbox{ .allocator = allocator, .limits = limits };
        try self.inbox.ensureTotalCapacity(allocator, limits.max_messages);
        return self;
    }

    pub fn deinit(self: *InboundMailbox) void {
        self.clear();
        self.inbox.deinit(self.allocator);
        self.* = undefined;
    }

    /// Ownership transfers only on success.
    pub fn push(self: *InboundMailbox, message: Message, encoded_size: usize) Error!void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const pending = self.inbox.items.len - self.head;
        if (pending >= self.limits.max_messages or
            encoded_size > self.limits.max_bytes -| self.encoded_bytes)
        {
            return error.TransportBackpressure;
        }
        if (self.head > 0 and self.inbox.items.len == self.inbox.capacity) self.compact();
        self.inbox.appendAssumeCapacity(.{ .message = message, .encoded_size = encoded_size });
        self.encoded_bytes += encoded_size;
    }

    pub fn canAccept(self: *InboundMailbox, encoded_size: usize) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.inbox.items.len - self.head < self.limits.max_messages and
            encoded_size <= self.limits.max_bytes -| self.encoded_bytes;
    }

    pub fn pop(self: *InboundMailbox) ?Message {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.head == self.inbox.items.len) return null;
        const item = self.inbox.items[self.head];
        self.head += 1;
        self.encoded_bytes -= item.encoded_size;
        if (self.head == self.inbox.items.len) {
            self.inbox.clearRetainingCapacity();
            self.head = 0;
        }
        return item.message;
    }

    pub fn clear(self: *InboundMailbox) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        for (self.inbox.items[self.head..]) |*item| item.message.deinit(self.allocator);
        self.inbox.clearRetainingCapacity();
        self.head = 0;
        self.encoded_bytes = 0;
    }

    pub fn empty(self: *InboundMailbox) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.head == self.inbox.items.len;
    }

    pub fn count(self: *InboundMailbox) usize {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.inbox.items.len - self.head;
    }

    pub fn bytes(self: *InboundMailbox) usize {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.encoded_bytes;
    }

    fn compact(self: *InboundMailbox) void {
        const pending = self.inbox.items.len - self.head;
        std.mem.copyForwards(Item, self.inbox.items[0..pending], self.inbox.items[self.head..]);
        self.inbox.shrinkRetainingCapacity(pending);
        self.head = 0;
    }
};

test "bounded mailbox tracks count and bytes" {
    const allocator = std.testing.allocator;
    var mailbox = try InboundMailbox.init(allocator, .{ .max_messages = 2, .max_bytes = 12 });
    defer mailbox.deinit();

    try mailbox.push(.{ .msg_type = .append, .index = 1 }, 5);
    try mailbox.push(.{ .msg_type = .append, .index = 2 }, 7);
    try std.testing.expectError(
        error.TransportBackpressure,
        mailbox.push(.{ .msg_type = .append, .index = 3 }, 1),
    );
    try std.testing.expectEqual(@as(usize, 2), mailbox.count());
    try std.testing.expectEqual(@as(usize, 12), mailbox.bytes());
    var first = mailbox.pop().?;
    defer first.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 7), mailbox.bytes());
}
