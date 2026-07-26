//! Pluggable transport interface for Raft message passing.
//!
//! The transport routes outbound messages by their `to` field. Implementations
//! queue inbound messages and peer events from foreign threads; callbacks are
//! invoked only by `pollOne` on the owning event-loop thread.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");

const Error = error_model.Error;
const Message = types.Message;

pub const TransportIdentity = struct {
    cluster_id: [16]u8,
    node_id: u64,
};

pub const PeerEventKind = enum {
    @"unreachable",
    snapshot_failure,
    identity_rejected,
};

pub const PeerEvent = struct {
    peer_id: u64,
    kind: PeerEventKind,
};

/// Callback invoked for each inbound message. Ownership transfers when the
/// callback is invoked by `pollOne`, including when it returns an error.
pub const MessageCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, msg: Message) Error!void,

    pub fn invoke(self: MessageCallback, msg: Message) Error!void {
        return self.function(self.ctx, msg);
    }
};

pub const PeerEventCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, event: PeerEvent) Error!void,

    pub fn invoke(self: PeerEventCallback, event: PeerEvent) Error!void {
        return self.function(self.ctx, event);
    }
};

/// vtable interface the Raftor layer calls into.
pub const Transport = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        start: *const fn (ctx: *anyopaque) Error!void,
        stop: *const fn (ctx: *anyopaque) void,
        add_peer: *const fn (ctx: *anyopaque, id: u64, addr: []const u8) Error!bool,
        remove_peer: *const fn (ctx: *anyopaque, id: u64) Error!void,
        send: *const fn (ctx: *anyopaque, messages: []const Message) Error!void,
        set_message_callback: *const fn (ctx: *anyopaque, cb: ?MessageCallback) void,
        set_peer_event_callback: *const fn (ctx: *anyopaque, cb: ?PeerEventCallback) void,
        poll_one: *const fn (ctx: *anyopaque) Error!bool,
        identity: ?*const fn (ctx: *anyopaque) TransportIdentity = null,
    };

    pub fn start(self: Transport) Error!void {
        return self.vtable.start(self.ctx);
    }
    pub fn stop(self: Transport) void {
        self.vtable.stop(self.ctx);
    }
    pub fn addPeer(self: Transport, id: u64, addr: []const u8) Error!bool {
        return self.vtable.add_peer(self.ctx, id, addr);
    }
    pub fn removePeer(self: Transport, id: u64) Error!void {
        return self.vtable.remove_peer(self.ctx, id);
    }
    pub fn send(self: Transport, messages: []const Message) Error!void {
        return self.vtable.send(self.ctx, messages);
    }
    pub fn setMessageCallback(self: Transport, cb: ?MessageCallback) void {
        self.vtable.set_message_callback(self.ctx, cb);
    }
    pub fn setPeerEventCallback(self: Transport, cb: ?PeerEventCallback) void {
        self.vtable.set_peer_event_callback(self.ctx, cb);
    }
    pub fn pollOne(self: Transport) Error!bool {
        return self.vtable.poll_one(self.ctx);
    }
    pub fn identity(self: Transport) ?TransportIdentity {
        const get_identity = self.vtable.identity orelse return null;
        return get_identity(self.ctx);
    }
};

/// Process-local transport that simply collects sent messages into a list.
/// Tests inspect `sent` to verify the Raft layer emitted expected messages.
/// Inbound messages can be injected via `deliver`.
pub const NoopTransport = struct {
    sent: std.ArrayList(Message),
    inbox: std.ArrayList(Message),
    peer_events: std.ArrayList(PeerEvent),
    callback: ?MessageCallback = null,
    peer_event_callback: ?PeerEventCallback = null,
    stopped: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) NoopTransport {
        return .{
            .sent = .empty,
            .inbox = .empty,
            .peer_events = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *NoopTransport) void {
        for (self.sent.items) |*m| m.deinit(self.allocator);
        self.sent.deinit(self.allocator);
        for (self.inbox.items) |*m| m.deinit(self.allocator);
        self.inbox.deinit(self.allocator);
        self.peer_events.deinit(self.allocator);
        self.* = undefined;
    }

    /// Remove and return all collected messages. Caller owns each message
    /// and must call `deinit` on every element plus `allocator.free`.
    pub fn drainSent(self: *NoopTransport) ![]Message {
        return self.sent.toOwnedSlice(self.allocator);
    }

    /// Clone and inject a borrowed message as if it arrived from the network.
    pub fn deliver(self: *NoopTransport, msg: Message) Error!bool {
        if (self.stopped.load(.acquire) or self.callback == null) return false;
        const cloned = try storage_mod.shareMessage(self.allocator, msg);
        errdefer {
            var owned = cloned;
            owned.deinit(self.allocator);
        }
        try self.inbox.append(self.allocator, cloned);
        return true;
    }

    pub fn deliverPeerEvent(self: *NoopTransport, event: PeerEvent) Error!bool {
        if (self.stopped.load(.acquire) or self.peer_event_callback == null) return false;
        try self.peer_events.append(self.allocator, event);
        return true;
    }

    // ---- vtable impl ----

    fn startImpl(ctx: *anyopaque) Error!void {
        const self: *NoopTransport = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return error.AlreadyStarted;
    }
    fn stopImpl(ctx: *anyopaque) void {
        const self: *NoopTransport = @ptrCast(@alignCast(ctx));
        self.stopped.store(true, .release);
    }
    fn addPeerImpl(_: *anyopaque, _: u64, _: []const u8) Error!bool {
        return true;
    }
    fn removePeerImpl(_: *anyopaque, _: u64) Error!void {}

    fn sendImpl(ctx: *anyopaque, messages: []const Message) Error!void {
        const self: *NoopTransport = @ptrCast(@alignCast(ctx));
        var cloned: std.ArrayList(Message) = .empty;
        defer {
            for (cloned.items) |*message| message.deinit(self.allocator);
            cloned.deinit(self.allocator);
        }
        try cloned.ensureTotalCapacity(self.allocator, messages.len);
        for (messages) |message| cloned.appendAssumeCapacity(try storage_mod.shareMessage(self.allocator, message));
        try self.sent.ensureUnusedCapacity(self.allocator, cloned.items.len);
        for (cloned.items) |message| self.sent.appendAssumeCapacity(message);
        cloned.clearRetainingCapacity();
    }

    fn setMessageCallbackImpl(ctx: *anyopaque, cb: ?MessageCallback) void {
        const self: *NoopTransport = @ptrCast(@alignCast(ctx));
        self.callback = cb;
    }

    fn setPeerEventCallbackImpl(ctx: *anyopaque, cb: ?PeerEventCallback) void {
        const self: *NoopTransport = @ptrCast(@alignCast(ctx));
        self.peer_event_callback = cb;
    }

    fn pollOneImpl(ctx: *anyopaque) Error!bool {
        const self: *NoopTransport = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return false;
        if (self.inbox.items.len > 0) {
            const cb = self.callback orelse return false;
            try cb.invoke(self.inbox.orderedRemove(0));
            return true;
        }
        if (self.peer_events.items.len > 0) {
            const cb = self.peer_event_callback orelse return false;
            try cb.invoke(self.peer_events.orderedRemove(0));
            return true;
        }
        return false;
    }

    pub const vtable: Transport.VTable = .{
        .start = startImpl,
        .stop = stopImpl,
        .add_peer = addPeerImpl,
        .remove_peer = removePeerImpl,
        .send = sendImpl,
        .set_message_callback = setMessageCallbackImpl,
        .set_peer_event_callback = setPeerEventCallbackImpl,
        .poll_one = pollOneImpl,
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
    try tp.send(&.{msg});

    const drained = try t.drainSent();
    defer {
        for (drained) |*m| m.deinit(allocator);
        allocator.free(drained);
    }
    try std.testing.expectEqual(@as(usize, 1), drained.len);
    try std.testing.expectEqual(@as(u64, 2), drained[0].to);
}

test "noop transport deeply copies sent messages" {
    const allocator = std.testing.allocator;
    var transport = NoopTransport.init(allocator);
    defer transport.deinit();

    var entries = [_]types.Entry{.{ .data = @constCast("payload") }};
    const message = Message{
        .entries = entries[0..],
        .snapshot = .{ .data = @constCast("snapshot") },
    };
    try transport.transport().send(&.{message});
    try std.testing.expect(transport.sent.items[0].entries.ptr != message.entries.ptr);
    try std.testing.expect(transport.sent.items[0].entries[0].data.ptr != message.entries[0].data.ptr);
    try std.testing.expect(transport.sent.items[0].snapshot.?.data.ptr != message.snapshot.?.data.ptr);
}

test "noop transport delivers to callback" {
    const allocator = std.testing.allocator;
    var t = NoopTransport.init(allocator);
    defer t.deinit();

    var received: ?Message = null;
    defer if (received) |*r| r.deinit(allocator);

    const Cb = struct {
        ptr: *?Message,
        pub fn invoke(ctx: *anyopaque, msg: Message) Error!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.ptr.* = msg;
        }
    };
    var cb_obj = Cb{ .ptr = &received };
    const tp = t.transport();
    tp.setMessageCallback(.{ .ctx = &cb_obj, .function = Cb.invoke });

    var msg = Message{ .msg_type = .heartbeat, .to = 1 };
    defer msg.deinit(allocator);
    try std.testing.expect(try t.deliver(msg));
    try std.testing.expect(try tp.pollOne());

    try std.testing.expect(received != null);
    try std.testing.expectEqual(@import("core/types.zig").MessageType.heartbeat, received.?.msg_type);
}
