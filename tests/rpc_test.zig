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

    mb.push(.{ .msg_type = .heartbeat, .to = 1, .from = 2, .term = 1 });
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

    try pm.addPeer(1, "127.0.0.1:9001");
    try pm.addPeer(2, "127.0.0.1:9002");
    try std.testing.expectEqual(@as(usize, 2), pm.count());
    try std.testing.expect(pm.hasPeer(1));
    try std.testing.expect(pm.hasPeer(2));

    pm.removePeer(1);
    try std.testing.expectEqual(@as(usize, 1), pm.count());
    try std.testing.expect(!pm.hasPeer(1));
}

test "rpc: failed transport start releases resources" {
    const first = raft.GrpcLiteTransport.create(allocator, "127.0.0.1:19101") catch return error.SkipZigTest;
    defer first.destroy();

    if (raft.GrpcLiteTransport.create(allocator, "127.0.0.1:19101")) |unexpected| {
        unexpected.destroy();
        return error.TestUnexpectedResult;
    } else |err| {
        try std.testing.expectEqual(error.ListenFailed, err);
    }
}

test "rpc: grpc transport self-connect round-trip" {
    // Create a transport listening on localhost.
    const tp = raft.GrpcLiteTransport.create(allocator, "127.0.0.1:19100") catch |e| {
        // If we can't bind (port in use, etc.), skip this test.
        std.log.warn("skipping rpc test: {s}", .{@errorName(e)});
        return error.SkipZigTest;
    };
    defer tp.destroy();

    // Add ourselves as a peer.
    tp.transport().addPeer(1, "127.0.0.1:19100");

    // Set callback to record received messages.
    var received: ?raft.Message = null;
    const Cb = struct {
        ptr: *?raft.Message,
        fn invoke(ctx: *anyopaque, msg: raft.Message) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.ptr.* = msg;
        }
    };
    var cb_obj = Cb{ .ptr = &received };
    tp.transport().setMessageCallback(.{ .ctx = &cb_obj, .function = Cb.invoke });

    // Send a message to ourselves (callUnary is synchronous — blocks until done).
    tp.transport().send(&.{.{ .msg_type = .heartbeat, .to = 1, .from = 1, .term = 1 }});

    for (0..1000) |_| {
        tp.transport().poll();
        if (received != null) break;
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }
    try std.testing.expect(received != null);

    var message = received.?;
    defer message.deinit(allocator);
    try std.testing.expectEqual(raft.MessageType.heartbeat, message.msg_type);
}
