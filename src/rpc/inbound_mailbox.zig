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
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) InboundMailbox {
        return .{ .inbox = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *InboundMailbox) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        for (self.inbox.items) |*m| m.deinit(self.allocator);
        self.inbox.deinit(self.allocator);
    }

    /// Push a message from the grpc handler thread. Takes ownership of the
    /// message's heap-allocated fields.
    pub fn push(self: *InboundMailbox, msg: Message) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        self.inbox.append(self.allocator, msg) catch {};
    }

    /// Drain all pending messages. Returns an owned slice; caller must call
    /// `deinit` on each message and `allocator.free` the slice.
    pub fn drain(self: *InboundMailbox) ![]Message {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.inbox.toOwnedSlice(self.allocator);
    }

    pub fn empty(self: *InboundMailbox) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.inbox.items.len == 0;
    }
};

test "inbound mailbox push and drain" {
    const allocator = std.testing.allocator;
    var mb = InboundMailbox.init(allocator);
    defer mb.deinit();

    try std.testing.expect(mb.empty());

    mb.push(.{ .msg_type = .append, .to = 1, .from = 2, .term = 1 });
    mb.push(.{ .msg_type = .heartbeat, .to = 1, .from = 3, .term = 1 });
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
