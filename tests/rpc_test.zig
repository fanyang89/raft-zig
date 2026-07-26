//! End-to-end grpc-lite transport test.
//!
//! Creates a GrpcLiteTransport, connects to itself, sends a Raft message,
//! and verifies receipt via poll().

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

test "rpc: inbound mailbox push and drain" {
    // This is a pure unit test — no network needed.
    var mb = raft.InboundMailbox.init(allocator);
    defer mb.deinit();

    try mb.push(.{ .msg_type = .heartbeat, .to = 1, .from = 2, .term = 1 });
    try std.testing.expect(!mb.empty());

    const msgs = try mb.drain();
    defer {
        for (msgs) |*m| m.deinit(allocator);
        allocator.free(msgs);
    }
    try std.testing.expectEqual(@as(usize, 1), msgs.len);
    try std.testing.expectEqual(raft.MessageType.heartbeat, msgs[0].msg_type);
}

test "rpc: peer manager add and remove" {
    var pm = raft.PeerManager.init(allocator);
    defer pm.deinit();

    try std.testing.expect(try pm.addPeer(1, "127.0.0.1:9001"));
    try std.testing.expect(try pm.addPeer(2, "127.0.0.1:9002"));
    try std.testing.expectEqual(@as(usize, 2), pm.count());
    try std.testing.expect(pm.hasPeer(1));
    try std.testing.expect(pm.hasPeer(2));

    pm.removePeer(1);
    try std.testing.expectEqual(@as(usize, 1), pm.count());
    try std.testing.expect(!pm.hasPeer(1));
}

test "rpc: failed transport start releases resources" {
    const first = try raft.GrpcLiteTransport.create(allocator, "127.0.0.1:0");
    defer first.destroy();
    first.transport().start() catch return error.SkipZigTest;
    var address_buffer: [64]u8 = undefined;
    const address = try std.fmt.bufPrint(&address_buffer, "127.0.0.1:{}", .{try first.port()});

    const second = try raft.GrpcLiteTransport.create(allocator, address);
    defer second.destroy();
    try std.testing.expectError(error.BindFailed, second.transport().start());
}

test "rpc: grpc transport self-connect round-trip" {
    const tp = raft.GrpcLiteTransport.create(allocator, "127.0.0.1:0") catch |e| {
        // If we can't bind (port in use, etc.), skip this test.
        std.log.warn("skipping rpc test: {s}", .{@errorName(e)});
        return error.SkipZigTest;
    };
    defer tp.destroy();
    tp.transport().start() catch |e| {
        std.log.warn("skipping rpc test: {s}", .{@errorName(e)});
        return error.SkipZigTest;
    };
    try std.testing.expectError(error.AlreadyStarted, tp.transport().start());

    var address_buffer: [64]u8 = undefined;
    const address = try std.fmt.bufPrint(&address_buffer, "127.0.0.1:{}", .{try tp.port()});
    try std.testing.expect(try tp.transport().addPeer(1, address));

    // Set callback to record received messages.
    var received: ?raft.Message = null;
    const Cb = struct {
        ptr: *?raft.Message,
        fn invoke(ctx: *anyopaque, msg: raft.Message) raft.Error!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.ptr.* = msg;
        }
    };
    var cb_obj = Cb{ .ptr = &received };
    tp.transport().setMessageCallback(.{ .ctx = &cb_obj, .function = Cb.invoke });

    // Send a message to ourselves (callUnary is synchronous — blocks until done).
    try tp.transport().send(&.{.{ .msg_type = .heartbeat, .to = 1, .from = 1, .term = 1 }});

    for (0..1000) |_| {
        _ = try tp.transport().pollOne();
        if (received != null) break;
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }
    try std.testing.expect(received != null);

    var message = received.?;
    try std.testing.expectEqual(raft.MessageType.heartbeat, message.msg_type);
    message.deinit(allocator);

    received = null;
    try tp.transport().send(&.{.{ .msg_type = .heartbeat, .to = 1, .from = 1, .term = 2 }});
    tp.transport().stop();
    try std.testing.expect(!(try tp.transport().pollOne()));
    try std.testing.expect(received == null);
    try std.testing.expectError(
        error.ConnectionClosed,
        tp.transport().send(&.{.{ .msg_type = .heartbeat, .to = 1, .from = 1 }}),
    );
}
