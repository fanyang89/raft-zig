const std = @import("std");
const api = @import("api_types.zig");
const Node = @import("node.zig").Node;

const allocator = std.heap.smp_allocator;
const cluster_id = [_]u8{1} ++ [_]u8{0} ** 15;

const Capture = struct {
    acquired: std.atomic.Value(u32) = .init(0),
    lost: std.atomic.Value(u32) = .init(0),
    failed: std.atomic.Value(u32) = .init(0),
    acquired_thread: std.atomic.Value(usize) = .init(0),
    lost_thread: std.atomic.Value(usize) = .init(0),
    failed_thread: std.atomic.Value(usize) = .init(0),

    fn onEvent(context: ?*anyopaque, event: *const api.Event) callconv(.c) void {
        const self: *Capture = @ptrCast(@alignCast(context.?));
        const thread_id: usize = @intCast(std.Thread.getCurrentId());
        switch (std.enums.fromInt(api.EventType, event.event_type).?) {
            .leadership_acquired => {
                self.acquired_thread.store(thread_id, .release);
                _ = self.acquired.fetchAdd(1, .monotonic);
            },
            .leadership_lost => {
                self.lost_thread.store(thread_id, .release);
                _ = self.lost.fetchAdd(1, .monotonic);
            },
            .failed => {
                self.failed_thread.store(thread_id, .release);
                _ = self.failed.fetchAdd(1, .monotonic);
            },
        }
    }

    fn callbacks(self: *Capture) api.Callbacks {
        return .{ .user_data = self, .on_event = onEvent };
    }
};

test "external drive elects a durable single-node leader" {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const data_dir = try temporary.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(data_dir);
    const address = "127.0.0.1:0";
    const peers = [_]api.Peer{.{
        .id = 1,
        .address = bytes(address),
    }};
    var capture: Capture = .{};
    var node: ?*Node = try Node.create(allocator, .{
        .drive_mode = @intFromEnum(api.DriveMode.external),
        .node_id = 1,
        .cluster_id = cluster_id,
        .listen_address = bytes(address),
        .data_dir = bytes(data_dir),
        .peers = &peers,
        .peer_count = peers.len,
        .tick_interval_ms = 10,
        .heartbeat_ticks = 1,
        .election_ticks = 5,
    }, capture.callbacks());
    defer if (node) |value| value.destroy();

    try node.?.start();
    for (0..20) |_| try std.testing.expect(!(try node.?.poll()));
    try std.testing.expectEqual(@as(u64, 0), node.?.getStatus().term);

    for (0..30) |_| {
        _ = try node.?.tick();
        if (node.?.getStatus().leader_active != 0) break;
    }
    const status = node.?.getStatus();
    try std.testing.expectEqual(@as(u32, @intFromEnum(api.Role.leader)), status.role);
    try std.testing.expectEqual(@as(u32, 1), status.leader_active);
    try std.testing.expectEqual(@as(u32, 1), capture.acquired.load(.acquire));
    try std.testing.expectEqual(@as(usize, @intCast(std.Thread.getCurrentId())), capture.acquired_thread.load(.acquire));

    try node.?.shutdown();
    try std.testing.expectEqual(@as(u32, 1), capture.lost.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), capture.failed.load(.acquire));
    node.?.destroy();
    node = null;

    var restarted_capture: Capture = .{};
    const restarted = try Node.create(allocator, .{
        .drive_mode = @intFromEnum(api.DriveMode.external),
        .node_id = 1,
        .cluster_id = cluster_id,
        .listen_address = bytes(address),
        .data_dir = bytes(data_dir),
        .peers = &peers,
        .peer_count = peers.len,
        .tick_interval_ms = 10,
        .heartbeat_ticks = 1,
        .election_ticks = 5,
    }, restarted_capture.callbacks());
    defer restarted.destroy();
    try restarted.start();
    try std.testing.expect(restarted.getStatus().term >= status.term);
    for (0..30) |_| {
        _ = try restarted.tick();
        if (restarted.getStatus().leader_active != 0) break;
    }
    try std.testing.expectEqual(@as(u32, 1), restarted.getStatus().leader_active);
    try std.testing.expectEqual(@as(u32, 1), restarted_capture.acquired.load(.acquire));
}

test "data directory is locked and bound to the local node" {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root_path);
    const data_dir = try std.fmt.allocPrint(allocator, "{s}/node", .{root_path});
    defer allocator.free(data_dir);
    const ports = try reserveUniquePorts(2);
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    const scratch = arena.allocator();
    const addresses = [_][]const u8{
        try std.fmt.allocPrint(scratch, "127.0.0.1:{}", .{ports[0]}),
        try std.fmt.allocPrint(scratch, "127.0.0.1:{}", .{ports[1]}),
    };
    const peers = [_]api.Peer{
        .{ .id = 1, .address = bytes(addresses[0]) },
        .{ .id = 2, .address = bytes(addresses[1]) },
    };

    const first = try Node.create(allocator, .{
        .drive_mode = @intFromEnum(api.DriveMode.external),
        .node_id = 1,
        .cluster_id = cluster_id,
        .listen_address = bytes(addresses[0]),
        .data_dir = bytes(data_dir),
        .peers = &peers,
        .peer_count = peers.len,
    }, .{});
    try std.testing.expectError(error.DataDirectoryInUse, Node.create(allocator, .{
        .drive_mode = @intFromEnum(api.DriveMode.external),
        .node_id = 1,
        .cluster_id = cluster_id,
        .listen_address = bytes(addresses[0]),
        .data_dir = bytes(data_dir),
        .peers = &peers,
        .peer_count = peers.len,
    }, .{}));
    first.destroy();

    try std.testing.expectError(error.IdentityMismatch, Node.create(allocator, .{
        .drive_mode = @intFromEnum(api.DriveMode.external),
        .node_id = 2,
        .cluster_id = cluster_id,
        .listen_address = bytes(addresses[1]),
        .data_dir = bytes(data_dir),
        .peers = &peers,
        .peer_count = peers.len,
    }, .{}));

    const identity_path = try std.fmt.allocPrint(allocator, "{s}/libelection.identity", .{data_dir});
    defer allocator.free(identity_path);
    try std.Io.Dir.deleteFileAbsolute(std.testing.io, identity_path);
    try std.testing.expectError(error.IdentityMissing, Node.create(allocator, .{
        .drive_mode = @intFromEnum(api.DriveMode.external),
        .node_id = 1,
        .cluster_id = cluster_id,
        .listen_address = bytes(addresses[0]),
        .data_dir = bytes(data_dir),
        .peers = &peers,
        .peer_count = peers.len,
    }, .{}));
}

test "managed three-node cluster re-elects after leader shutdown" {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root_path);
    const ports = try reserveUniquePorts(3);
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    const scratch = arena.allocator();

    var addresses: [3][]const u8 = undefined;
    var data_dirs: [3][]const u8 = undefined;
    var peers: [3]api.Peer = undefined;
    for (0..3) |index| {
        addresses[index] = try std.fmt.allocPrint(scratch, "127.0.0.1:{}", .{ports[index]});
        data_dirs[index] = try std.fmt.allocPrint(scratch, "{s}/node-{}", .{ root_path, index + 1 });
        peers[index] = .{ .id = index + 1, .address = bytes(addresses[index]) };
    }

    var captures: [3]Capture = .{ .{}, .{}, .{} };
    var nodes: [3]?*Node = .{ null, null, null };
    defer for (&nodes) |*maybe_node| {
        if (maybe_node.*) |node| {
            node.destroy();
            maybe_node.* = null;
        }
    };
    for (0..3) |index| {
        nodes[index] = try createNode(index, .managed, addresses, data_dirs, &peers, captures[index].callbacks());
        try nodes[index].?.start();
    }
    try std.testing.expectError(error.InvalidState, nodes[0].?.poll());

    const first_leader = try waitForLeader(&nodes);
    const first_term = nodes[first_leader].?.getStatus().term;
    try std.testing.expectEqual(@as(u32, 1), captures[first_leader].acquired.load(.acquire));

    const stopped = nodes[first_leader].?;
    try stopped.shutdown();
    stopped.destroy();
    nodes[first_leader] = null;
    try std.testing.expectEqual(@as(u32, 1), captures[first_leader].lost.load(.acquire));
    try std.testing.expectEqual(
        captures[first_leader].acquired_thread.load(.acquire),
        captures[first_leader].lost_thread.load(.acquire),
    );

    const second_leader = try waitForLeader(&nodes);
    try std.testing.expect(nodes[second_leader].?.getStatus().term > first_term);
    for (captures) |capture| try std.testing.expectEqual(@as(u32, 0), capture.failed.load(.acquire));
}

test "external drive processes multi-node transport traffic" {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root_path);
    const ports = try reserveUniquePorts(3);
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    const scratch = arena.allocator();

    var addresses: [3][]const u8 = undefined;
    var data_dirs: [3][]const u8 = undefined;
    var peers: [3]api.Peer = undefined;
    for (0..3) |index| {
        addresses[index] = try std.fmt.allocPrint(scratch, "127.0.0.1:{}", .{ports[index]});
        data_dirs[index] = try std.fmt.allocPrint(scratch, "{s}/node-{}", .{ root_path, index + 1 });
        peers[index] = .{ .id = index + 1, .address = bytes(addresses[index]) };
    }

    var captures: [3]Capture = .{ .{}, .{}, .{} };
    var nodes: [3]?*Node = .{ null, null, null };
    defer for (&nodes) |*maybe_node| {
        if (maybe_node.*) |node| {
            node.destroy();
            maybe_node.* = null;
        }
    };
    for (0..3) |index| {
        nodes[index] = try createNode(index, .external, addresses, data_dirs, &peers, captures[index].callbacks());
        try nodes[index].?.start();
    }

    var leader: ?usize = null;
    for (0..500) |_| {
        for (nodes) |maybe_node| {
            const node = maybe_node.?;
            _ = try node.tick();
            _ = try node.poll();
        }
        leader = activeLeader(&nodes);
        if (leader != null) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    const leader_index = leader orelse return error.LeaderTimeout;
    try std.testing.expectEqual(@as(u32, 1), captures[leader_index].acquired.load(.acquire));
    try std.testing.expectEqual(@as(usize, @intCast(std.Thread.getCurrentId())), captures[leader_index].acquired_thread.load(.acquire));
}

fn createNode(
    index: usize,
    mode: api.DriveMode,
    addresses: [3][]const u8,
    data_dirs: [3][]const u8,
    peers: *const [3]api.Peer,
    callbacks: api.Callbacks,
) !*Node {
    return Node.create(allocator, .{
        .drive_mode = @intFromEnum(mode),
        .node_id = index + 1,
        .cluster_id = cluster_id,
        .listen_address = bytes(addresses[index]),
        .data_dir = bytes(data_dirs[index]),
        .peers = peers,
        .peer_count = peers.len,
        .tick_interval_ms = 10,
        .heartbeat_ticks = 1,
        .election_ticks = 10,
    }, callbacks);
}

fn waitForLeader(nodes: *[3]?*Node) !usize {
    for (0..1000) |_| {
        var leader: ?usize = null;
        var leader_count: usize = 0;
        for (nodes, 0..) |maybe_node, index| {
            const node = maybe_node orelse continue;
            const status = node.getStatus();
            if (status.state == @intFromEnum(api.NodeState.failed)) return error.DriverFailed;
            if (status.leader_active != 0) {
                leader = index;
                leader_count += 1;
            }
        }
        if (leader_count == 1) return leader.?;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.LeaderTimeout;
}

fn activeLeader(nodes: *[3]?*Node) ?usize {
    var leader: ?usize = null;
    var count: usize = 0;
    for (nodes, 0..) |maybe_node, index| {
        const status = (maybe_node orelse continue).getStatus();
        if (status.leader_active != 0) {
            leader = index;
            count += 1;
        }
    }
    return if (count == 1) leader else null;
}

fn bytes(value: []const u8) api.BytesView {
    return .{ .data = value.ptr, .size = value.len };
}

fn reserveUniquePorts(comptime count: usize) ![count]u16 {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listeners: [count]std.Io.net.Server = undefined;
    var listener_count: usize = 0;
    defer for (listeners[0..listener_count]) |*listener| listener.deinit(std.testing.io);

    var ports: [count]u16 = undefined;
    for (&listeners, 0..) |*listener, index| {
        listener.* = try address.listen(std.testing.io, .{});
        listener_count += 1;
        var local_address: std.posix.sockaddr.in = undefined;
        var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
        if (std.posix.errno(std.posix.system.getsockname(
            listener.socket.handle,
            @ptrCast(&local_address),
            &address_length,
        )) != .SUCCESS) return error.AddressQueryFailed;
        ports[index] = std.mem.bigToNative(u16, local_address.port);
    }
    return ports;
}
