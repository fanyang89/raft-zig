//! Thread-safe inbound message queue bridging grpc-lite's libuv thread
//! and the Raftor event loop thread.
//!
//! grpc handlers push decoded Messages here; `poll()` drains them on the
//! raft event loop thread, feeding each to `raw_node.step()`.

const std = @import("std");
const types = @import("../core/types.zig");

const Message = types.Message;

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {}
}

pub const InboundMailbox = struct {
    mutex: std.atomic.Mutex = .unlocked,
    inbox: std.ArrayList(Message),
    head: usize = 0,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) InboundMailbox {
        return .{ .inbox = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *InboundMailbox) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        for (self.inbox.items[self.head..]) |*m| m.deinit(self.allocator);
        self.inbox.deinit(self.allocator);
    }

    /// Push a message from the grpc handler thread. Ownership transfers only
    /// when this function succeeds.
    pub fn push(self: *InboundMailbox, msg: Message) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.head >= 64 and self.head >= self.inbox.items.len - self.head) {
            const pending = self.inbox.items.len - self.head;
            std.mem.copyForwards(Message, self.inbox.items[0..pending], self.inbox.items[self.head..]);
            self.inbox.shrinkRetainingCapacity(pending);
            self.head = 0;
        }
        try self.inbox.append(self.allocator, msg);
    }

    pub fn pop(self: *InboundMailbox) ?Message {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.head == self.inbox.items.len) return null;
        const message = self.inbox.items[self.head];
        self.head += 1;
        if (self.head == self.inbox.items.len) {
            self.inbox.clearRetainingCapacity();
            self.head = 0;
        }
        return message;
    }

    /// Drain all pending messages. Returns an owned slice; caller must call
    /// `deinit` on each message and `allocator.free` the slice.
    pub fn drain(self: *InboundMailbox) ![]Message {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.head == 0) return self.inbox.toOwnedSlice(self.allocator);
        const messages = try self.allocator.dupe(Message, self.inbox.items[self.head..]);
        self.inbox.clearRetainingCapacity();
        self.head = 0;
        return messages;
    }

    pub fn empty(self: *InboundMailbox) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.head == self.inbox.items.len;
    }
};

test "inbound mailbox push and drain" {
    const allocator = std.testing.allocator;
    var mb = InboundMailbox.init(allocator);
    defer mb.deinit();

    try std.testing.expect(mb.empty());

    try mb.push(.{ .msg_type = .append, .to = 1, .from = 2, .term = 1 });
    try mb.push(.{ .msg_type = .heartbeat, .to = 1, .from = 3, .term = 1 });
    try std.testing.expect(!mb.empty());

    const msgs = try mb.drain();
    defer {
        for (msgs) |*m| m.deinit(allocator);
        allocator.free(msgs);
    }
    try std.testing.expectEqual(@as(usize, 2), msgs.len);
    try std.testing.expectEqual(types.MessageType.append, msgs[0].msg_type);
    try std.testing.expect(mb.empty());
}

test "inbound mailbox compacts consumed prefix" {
    const allocator = std.testing.allocator;
    var mb = InboundMailbox.init(allocator);
    defer mb.deinit();

    for (0..128) |index| try mb.push(.{ .msg_type = .append, .index = index });
    for (0..96) |index| {
        var message = mb.pop().?;
        defer message.deinit(allocator);
        try std.testing.expectEqual(index, message.index);
    }

    try mb.push(.{ .msg_type = .append, .index = 128 });
    try std.testing.expectEqual(@as(usize, 0), mb.head);
    try std.testing.expectEqual(@as(usize, 33), mb.inbox.items.len);

    for (96..129) |index| {
        var message = mb.pop().?;
        defer message.deinit(allocator);
        try std.testing.expectEqual(index, message.index);
    }
    try std.testing.expect(mb.empty());
}

test "inbound mailbox supports concurrent producers and draining" {
    const allocator = std.heap.smp_allocator;
    const producer_count = 4;
    const messages_per_producer = 128;

    var mailbox = InboundMailbox.init(allocator);
    defer mailbox.deinit();
    var producers_done = std.atomic.Value(usize).init(0);

    const Producer = struct {
        mailbox: *InboundMailbox,
        done: *std.atomic.Value(usize),
        id: u64,

        fn run(self: *@This()) void {
            for (0..messages_per_producer) |index| {
                self.mailbox.push(.{
                    .msg_type = .append,
                    .from = self.id,
                    .to = 1,
                    .index = index,
                }) catch unreachable;
            }
            _ = self.done.fetchAdd(1, .release);
        }
    };

    var producers: [producer_count]Producer = undefined;
    var threads: [producer_count]std.Thread = undefined;
    var started: usize = 0;
    errdefer for (threads[0..started]) |thread| thread.join();
    for (&producers, &threads, 0..) |*producer, *thread, index| {
        producer.* = .{ .mailbox = &mailbox, .done = &producers_done, .id = index + 1 };
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{producer});
        started += 1;
    }

    var received: usize = 0;
    while (producers_done.load(.acquire) != producer_count or !mailbox.empty()) {
        const messages = try mailbox.drain();
        received += messages.len;
        for (messages) |*message| message.deinit(allocator);
        allocator.free(messages);
        if (messages.len == 0) std.atomic.spinLoopHint();
    }
    for (&threads) |*thread| thread.join();
    started = 0;

    try std.testing.expectEqual(producer_count * messages_per_producer, received);
    try std.testing.expect(mailbox.empty());
}
