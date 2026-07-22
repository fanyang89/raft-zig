//! Process-local multi-node message routing for testing.
//!
//! `LoopbackNetwork` holds N `LoopbackTransport` instances keyed by node_id.
//! `send()` deep-clones each message into the target node's inbox. `poll()`
//! drains the inbox and invokes the registered callback for each message.
//! This lets tests simulate multi-node clusters without real TCP.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const transport_mod = @import("transport.zig");
const storage_mod = @import("storage.zig");

const Error = error_model.Error;
const Message = types.Message;
const MessageType = types.MessageType;
const Transport = transport_mod.Transport;
const MessageCallback = transport_mod.MessageCallback;
const cloneEntry = storage_mod.cloneEntry;

const log = std.log.scoped(.raft_zig_loopback);

/// Central registry that routes messages between in-process nodes.
/// Must be heap-allocated (via `create`) so that `LoopbackTransport.network`
/// pointers remain valid across caller stack frames.
pub const LoopbackNetwork = struct {
    nodes: std.AutoHashMap(u64, *LoopbackTransport),
    allocator: std.mem.Allocator,
    /// Optional filter: returns true to drop a message. Simulates packet
    /// loss or network partitions. Signature: (from, to, msg_type) → drop?.
    drop_filter: ?*const fn (from: u64, to: u64, msg_type: MessageType) bool = null,

    /// Heap-allocate a LoopbackNetwork. The caller owns the returned pointer
    /// and must call `destroy`.
    pub fn create(allocator: std.mem.Allocator) !*LoopbackNetwork {
        const self = try allocator.create(LoopbackNetwork);
        self.* = .{
            .nodes = std.AutoHashMap(u64, *LoopbackTransport).init(allocator),
            .allocator = allocator,
        };
        return self;
    }

    pub fn destroy(self: *LoopbackNetwork) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |tp| {
            tp.*.deinit();
            self.allocator.destroy(tp.*);
        }
        self.nodes.deinit();
        self.allocator.destroy(self);
    }

    /// Create a transport for `node_id` and register it in the network.
    pub fn createTransport(self: *LoopbackNetwork, node_id: u64) !*LoopbackTransport {
        const tp = try self.allocator.create(LoopbackTransport);
        tp.* = LoopbackTransport.init(self.allocator, self, node_id);
        try self.nodes.put(node_id, tp);
        return tp;
    }

    pub fn getTransport(self: *LoopbackNetwork, node_id: u64) ?*LoopbackTransport {
        return self.nodes.get(node_id);
    }

    /// Route a single message to the target's inbox (deep clone).
    fn route(self: *LoopbackNetwork, msg: Message) void {
        if (self.drop_filter) |f| {
            if (f(msg.from, msg.to, msg.msg_type)) return;
        }
        const target = self.nodes.get(msg.to) orelse return;
        target.inbox.append(self.allocator, msg) catch return;
    }

    /// Poll every node's inbox. Returns true if any messages were delivered.
    pub fn pollAll(self: *LoopbackNetwork) bool {
        var had_work = false;
        var it = self.nodes.valueIterator();
        while (it.next()) |tp| {
            if (tp.*.poll()) had_work = true;
        }
        return had_work;
    }
};

/// In-process transport that routes messages through a `LoopbackNetwork`.
/// Implements the `Transport` vtable.
pub const LoopbackTransport = struct {
    network: *LoopbackNetwork,
    node_id: u64,
    inbox: std.ArrayList(Message),
    callback: ?MessageCallback = null,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, network: *LoopbackNetwork, node_id: u64) LoopbackTransport {
        return .{
            .network = network,
            .node_id = node_id,
            .inbox = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *LoopbackTransport) void {
        for (self.inbox.items) |*m| m.deinit(self.allocator);
        self.inbox.deinit(self.allocator);
        self.* = undefined;
    }

    /// Drain the inbox and invoke the callback for each message. Returns
    /// true if any messages were delivered.
    ///
    /// **Ownership**: the callback receives each Message by value and becomes
    /// the sole owner of its heap-allocated fields. The callback (or its
    /// callee) must call `msg.deinit(allocator)` exactly once. `poll()` does
    /// NOT free the messages after delivery.
    pub fn poll(self: *LoopbackTransport) bool {
        if (self.inbox.items.len == 0) return false;
        const cb = self.callback orelse return false;

        // Move inbox to a local list to avoid re-entrant mutation.
        var local = self.inbox;
        self.inbox = .empty;

        // Transfer ownership of each message to the callback.
        for (local.items) |msg| {
            cb.invoke(msg);
        }

        // Only free the ArrayList metadata (the messages themselves are now
        // owned by the callbacks).
        local.deinit(self.allocator);
        return true;
    }

    // ---- Transport vtable impl ----

    fn startImpl(_: *anyopaque) Error!void {}
    fn stopImpl(_: *anyopaque) void {}

    fn addPeerImpl(_: *anyopaque, _: u64, _: []const u8) void {}
    fn removePeerImpl(_: *anyopaque, _: u64) void {}

    fn sendImpl(ctx: *anyopaque, messages: []const Message) void {
        const self: *LoopbackTransport = @ptrCast(@alignCast(ctx));
        for (messages) |m| {
            // Deep clone so the recipient owns its copy.
            const cloned = cloneMessageShallow(self.allocator, m) catch continue;
            self.network.route(cloned);
        }
    }

    fn setMessageCallbackImpl(ctx: *anyopaque, cb: MessageCallback) void {
        const self: *LoopbackTransport = @ptrCast(@alignCast(ctx));
        self.callback = cb;
    }

    pub const vtable: Transport.VTable = .{
        .start = startImpl,
        .stop = stopImpl,
        .add_peer = addPeerImpl,
        .remove_peer = removePeerImpl,
        .send = sendImpl,
        .set_message_callback = setMessageCallbackImpl,
    };

    pub fn transport(self: *LoopbackTransport) Transport {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

/// Clone a Message including all owned buffers (entries, context). The
/// returned message is fully independent and must be deinit'd by the caller.
fn cloneMessageShallow(allocator: std.mem.Allocator, src: Message) !Message {
    var entries = try allocator.alloc(types.Entry, src.entries.len);
    for (src.entries, 0..) |e, i| {
        entries[i] = .{
            .entry_type = e.entry_type,
            .term = e.term,
            .index = e.index,
            .checksum = e.checksum,
            .data = if (e.data.len > 0) try allocator.dupe(u8, e.data) else &.{},
            .context = if (e.context.len > 0) try allocator.dupe(u8, e.context) else &.{},
        };
    }
    const context: []u8 = if (src.context.len > 0) try allocator.dupe(u8, src.context) else &.{};
    return .{
        .msg_type = src.msg_type,
        .to = src.to,
        .from = src.from,
        .term = src.term,
        .log_term = src.log_term,
        .index = src.index,
        .commit = src.commit,
        .commit_term = src.commit_term,
        .entries = entries,
        .request_snapshot = src.request_snapshot,
        .reject = src.reject,
        .reject_hint = src.reject_hint,
        .context = context,
        .priority = src.priority,
        .snapshot = null,
    };
}

// ===========================================================================
// Tests
// ===========================================================================

test "loopback: route delivers message to target inbox" {
    const allocator = std.testing.allocator;
    const net = try LoopbackNetwork.create(allocator);
    defer net.destroy();

    const tp1 = try net.createTransport(1);
    const tp2 = try net.createTransport(2);

    // Send a message from 1 to 2.
    tp1.transport().send(&.{.{ .msg_type = .append, .to = 2, .from = 1, .term = 1 }});

    // Node 2 should have it in its inbox.
    try std.testing.expectEqual(@as(usize, 1), tp2.inbox.items.len);
    try std.testing.expectEqual(@as(u64, 1), tp2.inbox.items[0].from);
}

test "loopback: poll invokes callback" {
    const allocator = std.testing.allocator;
    const net = try LoopbackNetwork.create(allocator);
    defer net.destroy();

    const tp1 = try net.createTransport(1);
    const tp2 = try net.createTransport(2);

    var received_count: usize = 0;
    const Cb = struct {
        count: *usize,
        alloc: std.mem.Allocator,
        fn invoke(ctx: *anyopaque, msg: Message) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.count.* += 1;
            var m = msg;
            m.deinit(self.alloc);
        }
    };
    var cb_obj = Cb{ .count = &received_count, .alloc = allocator };
    tp2.transport().setMessageCallback(.{ .ctx = &cb_obj, .function = Cb.invoke });

    tp1.transport().send(&.{.{ .msg_type = .heartbeat, .to = 2, .from = 1 }});
    try std.testing.expect(tp2.poll());
    try std.testing.expectEqual(@as(usize, 1), received_count);
}

test "loopback: drop filter simulates partition" {
    const allocator = std.testing.allocator;
    const net = try LoopbackNetwork.create(allocator);
    defer net.destroy();

    // Drop all append messages.
    const dropAppends = struct {
        fn filter(_: u64, _: u64, t: MessageType) bool {
            return t == .append;
        }
    };
    net.drop_filter = dropAppends.filter;

    const tp1 = try net.createTransport(1);
    const tp2 = try net.createTransport(2);

    tp1.transport().send(&.{.{ .msg_type = .append, .to = 2, .from = 1 }});
    tp1.transport().send(&.{.{ .msg_type = .heartbeat, .to = 2, .from = 1 }});

    // Append was dropped, heartbeat delivered.
    try std.testing.expectEqual(@as(usize, 1), tp2.inbox.items.len);
    try std.testing.expectEqual(MessageType.heartbeat, tp2.inbox.items[0].msg_type);
}
