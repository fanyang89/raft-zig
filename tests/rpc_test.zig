const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.heap.smp_allocator;
const cluster_a = [_]u8{1} ** 16;
const cluster_b = [_]u8{2} ** 16;

const Capture = struct {
    messages: std.ArrayList(raft.Message) = .empty,
    events: std.ArrayList(raft.PeerEvent) = .empty,
    callback_thread: ?std.Thread.Id = null,

    fn deinit(self: *Capture) void {
        for (self.messages.items) |*message| message.deinit(allocator);
        self.messages.deinit(allocator);
        self.events.deinit(allocator);
    }

    fn onMessage(context: *anyopaque, message: raft.Message) raft.Error!void {
        const self: *Capture = @ptrCast(@alignCast(context));
        errdefer {
            var owned = message;
            owned.deinit(allocator);
        }
        self.callback_thread = std.Thread.getCurrentId();
        try self.messages.append(allocator, message);
    }

    fn onEvent(context: *anyopaque, event: raft.PeerEvent) raft.Error!void {
        const self: *Capture = @ptrCast(@alignCast(context));
        self.callback_thread = std.Thread.getCurrentId();
        try self.events.append(allocator, event);
    }

    fn attach(self: *Capture, transport: raft.Transport) void {
        transport.setMessageCallback(.{ .ctx = self, .function = onMessage });
        transport.setPeerEventCallback(.{ .ctx = self, .function = onEvent });
    }
};

fn config(node_id: u64, cluster_id: [16]u8, address: []const u8) raft.GrpcLiteTransportConfig {
    return .{
        .identity = .{ .cluster_id = cluster_id, .node_id = node_id },
        .listen_addr = address,
        .reconnect_initial_delay_ns = 2 * std.time.ns_per_ms,
        .reconnect_max_delay_ns = 20 * std.time.ns_per_ms,
        .graceful_shutdown_timeout_ns = 20 * std.time.ns_per_ms,
    };
}

fn addressOf(transport: *raft.GrpcLiteTransport, buffer: []u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "127.0.0.1:{}", .{try transport.port()});
}

fn waitActive(transport: *raft.GrpcLiteTransport, peer_id: u64) !void {
    for (0..1000) |_| {
        if (transport.peerState(peer_id) == .active) return;
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }
    return error.TestTimeout;
}

fn pollUntil(transport: raft.Transport, count: *const usize, expected: usize) !void {
    for (0..1000) |_| {
        _ = try transport.pollOne();
        if (count.* >= expected) return;
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }
    return error.TestTimeout;
}

fn waitEvent(transport: raft.Transport, capture: *Capture, kind: raft.PeerEventKind) !void {
    for (0..1000) |_| {
        _ = try transport.pollOne();
        for (capture.events.items) |event| if (event.kind == kind) return;
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }
    return error.TestTimeout;
}

const Pair = struct {
    first: *raft.GrpcLiteTransport,
    second: *raft.GrpcLiteTransport,

    fn create() !Pair {
        const first = try raft.GrpcLiteTransport.create(allocator, config(1, cluster_a, "127.0.0.1:0"));
        errdefer first.destroy();
        const second = try raft.GrpcLiteTransport.create(allocator, config(2, cluster_a, "127.0.0.1:0"));
        errdefer second.destroy();
        try first.start();
        errdefer first.stop();
        try second.start();
        errdefer second.stop();
        var first_address_buffer: [64]u8 = undefined;
        var second_address_buffer: [64]u8 = undefined;
        const first_address = try addressOf(first, &first_address_buffer);
        const second_address = try addressOf(second, &second_address_buffer);
        try std.testing.expect(try first.transport().addPeer(2, second_address));
        try std.testing.expect(try second.transport().addPeer(1, first_address));
        try waitActive(first, 2);
        try waitActive(second, 1);
        return .{ .first = first, .second = second };
    }

    fn destroy(self: Pair) void {
        self.first.stop();
        self.second.stop();
        self.first.destroy();
        self.second.destroy();
    }
};

test "rpc: bounded inbound mailbox enforces count and encoded bytes" {
    var mailbox = try raft.InboundMailbox.init(allocator, .{ .max_messages = 2, .max_bytes = 10 });
    defer mailbox.deinit();
    try mailbox.push(.{ .msg_type = .heartbeat }, 6);
    try std.testing.expectError(
        error.TransportBackpressure,
        mailbox.push(.{ .msg_type = .heartbeat }, 5),
    );
    var message = mailbox.pop().?;
    message.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), mailbox.bytes());
}

test "rpc: persistent stream preserves FIFO and callbacks run only in pollOne" {
    const pair = try Pair.create();
    defer pair.destroy();
    var capture = Capture{};
    defer capture.deinit();
    capture.attach(pair.second.transport());
    const stream_open_count = pair.first.peerOpenCount(2);
    try std.testing.expect(stream_open_count > 0);

    const messages = [_]raft.Message{
        .{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 1 },
        .{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 2 },
        .{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 3 },
    };
    try pair.first.transport().send(&messages);
    try std.testing.io.sleep(.fromNanoseconds(5 * std.time.ns_per_ms), .awake);
    try std.testing.expectEqual(@as(usize, 0), capture.messages.items.len);
    const poll_thread = std.Thread.getCurrentId();
    try pollUntil(pair.second.transport(), &capture.messages.items.len, 3);
    try std.testing.expectEqual(poll_thread, capture.callback_thread.?);
    try std.testing.expectEqual(@as(u64, 1), capture.messages.items[0].term);
    try std.testing.expectEqual(@as(u64, 2), capture.messages.items[1].term);
    try std.testing.expectEqual(@as(u64, 3), capture.messages.items[2].term);
    try std.testing.expectEqual(stream_open_count, pair.first.peerOpenCount(2));
}

test "rpc: node pair uses two directed streams" {
    const pair = try Pair.create();
    defer pair.destroy();
    var first_capture = Capture{};
    defer first_capture.deinit();
    var second_capture = Capture{};
    defer second_capture.deinit();
    first_capture.attach(pair.first.transport());
    second_capture.attach(pair.second.transport());
    const first_stream_open_count = pair.first.peerOpenCount(2);
    const second_stream_open_count = pair.second.peerOpenCount(1);
    try std.testing.expect(first_stream_open_count > 0);
    try std.testing.expect(second_stream_open_count > 0);

    try pair.first.transport().send(&.{.{ .msg_type = .append, .from = 1, .to = 2, .term = 7 }});
    try pair.second.transport().send(&.{.{ .msg_type = .append_response, .from = 2, .to = 1, .term = 7 }});
    try pollUntil(pair.first.transport(), &first_capture.messages.items.len, 1);
    try pollUntil(pair.second.transport(), &second_capture.messages.items.len, 1);
    try std.testing.expectEqual(@as(u64, 2), first_capture.messages.items[0].from);
    try std.testing.expectEqual(@as(u64, 1), second_capture.messages.items[0].from);
    try std.testing.expectEqual(first_stream_open_count, pair.first.peerOpenCount(2));
    try std.testing.expectEqual(second_stream_open_count, pair.second.peerOpenCount(1));
}

test "rpc: wrong cluster is identity rejected without delivery" {
    const first = try raft.GrpcLiteTransport.create(allocator, config(1, cluster_a, "127.0.0.1:0"));
    defer first.destroy();
    const second = try raft.GrpcLiteTransport.create(allocator, config(2, cluster_b, "127.0.0.1:0"));
    defer second.destroy();
    var first_capture = Capture{};
    defer first_capture.deinit();
    var second_capture = Capture{};
    defer second_capture.deinit();
    first_capture.attach(first.transport());
    second_capture.attach(second.transport());
    try first.start();
    try second.start();
    var first_buffer: [64]u8 = undefined;
    var second_buffer: [64]u8 = undefined;
    _ = try first.transport().addPeer(2, try addressOf(second, &second_buffer));
    _ = try second.transport().addPeer(1, try addressOf(first, &first_buffer));
    try waitEvent(first.transport(), &first_capture, .identity_rejected);
    try std.testing.expectEqual(@as(usize, 0), second_capture.messages.items.len);
}

test "rpc: peer ID to address mismatch is identity rejected" {
    const first = try raft.GrpcLiteTransport.create(allocator, config(1, cluster_a, "127.0.0.1:0"));
    defer first.destroy();
    const third = try raft.GrpcLiteTransport.create(allocator, config(3, cluster_a, "127.0.0.1:0"));
    defer third.destroy();
    var capture = Capture{};
    defer capture.deinit();
    capture.attach(first.transport());
    try first.start();
    try third.start();
    var first_buffer: [64]u8 = undefined;
    var third_buffer: [64]u8 = undefined;
    _ = try first.transport().addPeer(2, try addressOf(third, &third_buffer));
    _ = try third.transport().addPeer(1, try addressOf(first, &first_buffer));
    try waitEvent(first.transport(), &capture, .identity_rejected);
}

test "rpc: wrong from destination and local messages are rejected before send" {
    const pair = try Pair.create();
    defer pair.destroy();
    var first_capture = Capture{};
    defer first_capture.deinit();
    var second_capture = Capture{};
    defer second_capture.deinit();
    first_capture.attach(pair.first.transport());
    second_capture.attach(pair.second.transport());

    try std.testing.expectError(
        error.MessageSourceMismatch,
        pair.first.transport().send(&.{.{ .msg_type = .heartbeat, .from = 99, .to = 2 }}),
    );
    try std.testing.expectError(
        error.MessageDestinationMismatch,
        pair.first.transport().send(&.{.{ .msg_type = .heartbeat, .from = 1, .to = 0 }}),
    );
    try std.testing.expectError(
        error.LocalMessageOnTransport,
        pair.first.transport().send(&.{.{ .msg_type = .hup, .from = 1, .to = 2 }}),
    );
    try waitActive(pair.first, 2);
    try std.testing.expectEqual(@as(usize, 0), second_capture.messages.items.len);
}

test "rpc: worker reconnects when peer server starts late" {
    const port = try unusedPort();
    var late_address_buffer: [64]u8 = undefined;
    const late_address = try std.fmt.bufPrint(&late_address_buffer, "127.0.0.1:{}", .{port});
    const first = try raft.GrpcLiteTransport.create(allocator, config(1, cluster_a, "127.0.0.1:0"));
    defer first.destroy();
    const second = try raft.GrpcLiteTransport.create(allocator, config(2, cluster_a, late_address));
    defer second.destroy();
    var first_capture = Capture{};
    defer first_capture.deinit();
    var second_capture = Capture{};
    defer second_capture.deinit();
    first_capture.attach(first.transport());
    second_capture.attach(second.transport());
    var first_address_buffer: [64]u8 = undefined;
    try first.start();
    _ = try second.transport().addPeer(1, try addressOf(first, &first_address_buffer));
    _ = try first.transport().addPeer(2, late_address);
    try waitEvent(first.transport(), &first_capture, .@"unreachable");

    try second.start();
    try waitActive(first, 2);
    try pairSendAndReceive(first, second, &second_capture, 11);
    try std.testing.expect(first.peerOpenCount(2) >= 1);
}

test "rpc: remove and address replacement fence active stream" {
    const pair = try Pair.create();
    defer pair.destroy();
    pair.second.stop();

    const replacement = try raft.GrpcLiteTransport.create(allocator, config(2, cluster_a, "127.0.0.1:0"));
    defer replacement.destroy();
    var capture = Capture{};
    defer capture.deinit();
    capture.attach(replacement.transport());
    try replacement.start();
    var first_address_buffer: [64]u8 = undefined;
    var replacement_address_buffer: [64]u8 = undefined;
    _ = try replacement.transport().addPeer(1, try addressOf(pair.first, &first_address_buffer));
    try pair.first.transport().removePeer(2);
    try std.testing.expectEqual(@as(?raft.PeerLifecycleState, null), pair.first.peerState(2));
    _ = try pair.first.transport().addPeer(2, try addressOf(replacement, &replacement_address_buffer));
    try waitActive(pair.first, 2);
    try pairSendAndReceive(pair.first, replacement, &capture, 12);
}

test "rpc: tiny outbound limit reports backpressure" {
    var first_config = config(1, cluster_a, "127.0.0.1:0");
    first_config.stream_limits = .{
        .max_message_size = 128,
        .max_inbound_buffer_size = 64 * 1024,
        .max_outbound_buffer_size = 133,
    };
    const first = try raft.GrpcLiteTransport.create(allocator, first_config);
    defer first.destroy();
    const second = try raft.GrpcLiteTransport.create(allocator, config(2, cluster_a, "127.0.0.1:0"));
    defer second.destroy();
    try first.start();
    try second.start();
    var first_buffer: [64]u8 = undefined;
    var second_buffer: [64]u8 = undefined;
    _ = try first.transport().addPeer(2, try addressOf(second, &second_buffer));
    _ = try second.transport().addPeer(1, try addressOf(first, &first_buffer));
    try waitActive(first, 2);

    var saw_backpressure = false;
    for (0..10000) |term| {
        first.transport().send(&.{.{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = term }}) catch |err| {
            if (err == error.TransportBackpressure) {
                saw_backpressure = true;
                break;
            }
            return err;
        };
    }
    try std.testing.expect(saw_backpressure);
}

test "rpc: bounded mailbox overflow terminates stream" {
    var second_config = config(2, cluster_a, "127.0.0.1:0");
    second_config.mailbox_max_messages = 1;
    second_config.mailbox_max_bytes = 1024;
    const first = try raft.GrpcLiteTransport.create(allocator, config(1, cluster_a, "127.0.0.1:0"));
    defer first.destroy();
    const second = try raft.GrpcLiteTransport.create(allocator, second_config);
    defer second.destroy();
    var first_capture = Capture{};
    defer first_capture.deinit();
    var second_capture = Capture{};
    defer second_capture.deinit();
    first_capture.attach(first.transport());
    second_capture.attach(second.transport());
    try first.start();
    try second.start();
    var first_buffer: [64]u8 = undefined;
    var second_buffer: [64]u8 = undefined;
    _ = try first.transport().addPeer(2, try addressOf(second, &second_buffer));
    _ = try second.transport().addPeer(1, try addressOf(first, &first_buffer));
    try waitActive(first, 2);
    try first.transport().send(&.{
        .{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 1 },
        .{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 2 },
    });
    try waitEvent(first.transport(), &first_capture, .@"unreachable");
    try pollUntil(second.transport(), &second_capture.messages.items.len, 1);
    try std.testing.expectEqual(@as(usize, 1), second_capture.messages.items.len);
}

test "rpc: stop is idempotent and rejects send" {
    const pair = try Pair.create();
    defer pair.destroy();
    pair.first.stop();
    pair.first.stop();
    try std.testing.expectError(
        error.ConnectionClosed,
        pair.first.transport().send(&.{.{ .msg_type = .heartbeat, .from = 1, .to = 2 }}),
    );
}

fn pairSendAndReceive(
    first: *raft.GrpcLiteTransport,
    second: *raft.GrpcLiteTransport,
    capture: *Capture,
    term: u64,
) !void {
    try first.transport().send(&.{.{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = term }});
    try pollUntil(second.transport(), &capture.messages.items.len, 1);
    try std.testing.expectEqual(term, capture.messages.items[0].term);
}

fn unusedPort() !u16 {
    const listen_address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try listen_address.listen(std.testing.io, .{});
    defer listener.deinit(std.testing.io);
    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        listener.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    return std.mem.bigToNative(u16, local_address.port);
}
