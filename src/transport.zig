//! Pluggable transport interface for Raft message passing.
//!
//! Ports `include/raftpp/raftor/rpc/transport.h`. The transport routes
//! outbound messages by their `to` field and invokes a callback for inbound
//! messages. `NoopTransport` is a process-local implementation that collects
//! sent messages for test inspection.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");

const Error = error_model.Error;
const Message = types.Message;

/// Callback invoked for each inbound message.
pub const MessageCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, msg: Message) void,

    pub fn invoke(self: MessageCallback, msg: Message) void {
        self.function(self.ctx, msg);
    }
};

/// vtable interface the Raftor layer calls into.
pub const Transport = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        start: *const fn (ctx: *anyopaque) Error!void,
        stop: *const fn (ctx: *anyopaque) void,
        add_peer: *const fn (ctx: *anyopaque, id: u64, addr: []const u8) void,
        remove_peer: *const fn (ctx: *anyopaque, id: u64) void,
        send: *const fn (ctx: *anyopaque, messages: []const Message) void,
        set_message_callback: *const fn (ctx: *anyopaque, cb: MessageCallback) void,
        poll: *const fn (ctx: *anyopaque) void,
    };

    pub fn start(self: Transport) Error!void {
        return self.vtable.start(self.ctx);
    }
    pub fn stop(self: Transport) void {
        self.vtable.stop(self.ctx);
    }
    pub fn addPeer(self: Transport, id: u64, addr: []const u8) void {
        self.vtable.add_peer(self.ctx, id, addr);
    }
    pub fn removePeer(self: Transport, id: u64) void {
        self.vtable.remove_peer(self.ctx, id);
    }
    pub fn send(self: Transport, messages: []const Message) void {
        self.vtable.send(self.ctx, messages);
    }
    pub fn setMessageCallback(self: Transport, cb: MessageCallback) void {
        self.vtable.set_message_callback(self.ctx, cb);
    }
    pub fn poll(self: Transport) void {
        self.vtable.poll(self.ctx);
    }
};

/// Process-local transport that simply collects sent messages into a list.
/// Tests inspect `sent` to verify the Raft layer emitted expected messages.
/// Inbound messages can be injected via `deliver`.
pub const NoopTransport = struct {
    sent: std.ArrayList(Message),
    callback: ?MessageCallback = null,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) NoopTransport {
        return .{ .sent = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *NoopTransport) void {
        for (self.sent.items) |*m| m.deinit(self.allocator);
        self.sent.deinit(self.allocator);
        self.* = undefined;
    }

    /// Remove and return all collected messages. Caller owns each message
    /// and must call `deinit` on every element plus `allocator.free`.
    pub fn drainSent(self: *NoopTransport) ![]Message {
        return self.sent.toOwnedSlice(self.allocator);
    }

    /// Inject a message as if it arrived from the network.
    pub fn deliver(self: *NoopTransport, msg: Message) void {
        if (self.callback) |cb| cb.invoke(msg);
    }

    // ---- vtable impl ----

    fn startImpl(_: *anyopaque) Error!void {}
    fn stopImpl(_: *anyopaque) void {}
    fn addPeerImpl(_: *anyopaque, _: u64, _: []const u8) void {}
    fn removePeerImpl(_: *anyopaque, _: u64) void {}

    fn sendImpl(ctx: *anyopaque, messages: []const Message) void {
        const self: *NoopTransport = @ptrCast(@alignCast(ctx));
        for (messages) |m| {
            self.sent.append(self.allocator, m) catch return;
        }
    }

    fn setMessageCallbackImpl(ctx: *anyopaque, cb: MessageCallback) void {
        const self: *NoopTransport = @ptrCast(@alignCast(ctx));
        self.callback = cb;
    }

    fn pollImpl(_: *anyopaque) void {}

    pub const vtable: Transport.VTable = .{
        .start = startImpl,
        .stop = stopImpl,
        .add_peer = addPeerImpl,
        .remove_peer = removePeerImpl,
        .send = sendImpl,
        .set_message_callback = setMessageCallbackImpl,
        .poll = pollImpl,
    };

    pub fn transport(self: *NoopTransport) Transport {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

test "noop transport collects sent messages" {
    const allocator = std.testing.allocator;
    var t = NoopTransport.init(allocator);
    defer t.deinit();

    var msg = Message{ .msg_type = .append, .to = 2, .from = 1, .term = 1 };
    defer msg.deinit(allocator);

    const tp = t.transport();
    tp.send(&.{msg});

    const drained = try t.drainSent();
    defer {
        for (drained) |*m| m.deinit(allocator);
        allocator.free(drained);
    }
    try std.testing.expectEqual(@as(usize, 1), drained.len);
    try std.testing.expectEqual(@as(u64, 2), drained[0].to);
}

test "noop transport delivers to callback" {
    const allocator = std.testing.allocator;
    var t = NoopTransport.init(allocator);
    defer t.deinit();

    var received: ?Message = null;
    defer if (received) |*r| r.deinit(allocator);

    const Cb = struct {
        ptr: *?Message,
        pub fn invoke(ctx: *anyopaque, msg: Message) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.ptr.* = msg;
        }
    };
    var cb_obj = Cb{ .ptr = &received };
    const tp = t.transport();
    tp.setMessageCallback(.{ .ctx = &cb_obj, .function = Cb.invoke });

    var msg = Message{ .msg_type = .heartbeat, .to = 1 };
    defer msg.deinit(allocator);
    t.deliver(msg);

    try std.testing.expect(received != null);
    try std.testing.expectEqual(@import("core/types.zig").MessageType.heartbeat, received.?.msg_type);
}
