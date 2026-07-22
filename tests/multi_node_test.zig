//! Multi-node integration tests using LoopbackTransport.
//!
//! Each test creates 3 nodes (MemoryStorage + RawNode + LoopbackTransport)
//! connected through a LoopbackNetwork. The event loop drives tick →
//! Ready → persist → advance → send → poll on each cycle.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

const MemoryStorage = raft.MemoryStorage;
const RawNode = raft.RawNode;
const LoopbackNetwork = raft.LoopbackNetwork;
const StateRole = raft.StateRole;
const Message = raft.Message;
const MessageType = raft.MessageType;
const Entry = raft.Entry;
const Config = raft.Config;

const Node = struct {
    storage: MemoryStorage,
    raw_node: RawNode,
    transport: *raft.LoopbackTransport,
};

fn makeConfig(id: u64) Config {
    var c = raft.defaultConfig();
    c.id = id;
    c.election_tick = 10;
    c.heartbeat_tick = 1;
    c.election_timeout_seed = id * 999 + 1;
    return c;
}

const Cluster = struct {
    net: *raft.LoopbackNetwork,
    nodes: [3]Node,

    fn destroy(self: *Cluster) void {
        for (&self.nodes) |*nd| {
            nd.raw_node.deinit();
            nd.storage.deinit(allocator);
        }
        self.net.destroy();
    }
};

fn createCluster() !Cluster {
    const net = try raft.LoopbackNetwork.create(allocator);

    var transports: [3]*raft.LoopbackTransport = undefined;
    for (1..4) |i| {
        transports[i - 1] = try net.createTransport(@intCast(i));
    }

    const voter_ids = [_]u64{ 1, 2, 3 };
    var nodes: [3]Node = undefined;
    for (1..4) |i| {
        const idx = i - 1;
        nodes[idx] = Node{
            .storage = MemoryStorage.init(),
            .raw_node = undefined,
            .transport = transports[idx],
        };
        const voters = try allocator.dupe(u64, &voter_ids);
        var cs = raft.ConfState{ .voters = voters };
        try nodes[idx].storage.setRaftState(allocator, .{ .conf_state = cs });
        cs.deinit(allocator);
    }

    return .{ .net = net, .nodes = nodes };
}

/// Phase 2: create RawNode instances. MUST be called after the cluster struct
/// is at its final location.
fn initRawNodes(nodes: *[3]Node) !void {
    for (1..4) |i| {
        const idx = i - 1;
        nodes[idx].raw_node = try RawNode.init(allocator, makeConfig(@intCast(i)), nodes[idx].storage.asStorage());
    }
}

/// Phase 3: register message callbacks.
fn registerCallbacks(nodes: *[3]Node) void {
    for (nodes[0..]) |*nd| {
        nd.transport.transport().setMessageCallback(.{
            .ctx = nd,
            .function = &messageCallback,
        });
    }
}

fn destroyCluster(comptime n: usize, net: *LoopbackNetwork, nodes: *[n]Node) void {
    for (nodes[0..]) |*nd| {
        nd.raw_node.deinit();
        nd.storage.deinit(allocator);
    }
    net.deinit();
}

/// Callback invoked by LoopbackTransport when a message arrives. Forwards
/// the message to the node's RawNode.
fn messageCallback(ctx: *anyopaque, msg: Message) void {
    const node: *Node = @ptrCast(@alignCast(ctx));
    var m = msg;
    defer m.deinit(allocator);
    node.raw_node.step(m) catch {};
}

/// Drive one full event-loop cycle across all nodes:
///   1. Tick each node.
///   2. Process Ready (persist + advance + send).
///   3. Poll the network to deliver messages.
fn tickCluster(net: *raft.LoopbackNetwork, nodes: *[3]Node) !void {
    // 1. Tick.
    for (nodes[0..]) |*nd| _ = try nd.raw_node.tick();

    // 2. Process Ready for each node.
    for (nodes[0..]) |*nd| {
        while (nd.raw_node.hasReady()) {
            var rd = try nd.raw_node.getReady();
            defer rd.deinit(allocator);

            if (rd.entries.len > 0) try nd.storage.append(allocator, rd.entries);
            if (rd.light.committed_entries.len > 0) {
                // Apply to storage (simulated).
            }
            if (rd.light.messages.len > 0) nd.transport.transport().send(rd.light.messages);

            var light = try nd.raw_node.advance(rd);
            defer light.deinit(allocator);
            if (light.messages.len > 0) nd.transport.transport().send(light.messages);
            nd.raw_node.advanceApply();
        }
    }

    // 3. Deliver messages.
    _ = net.pollAll();
}

fn countLeaders(nodes: *[3]Node) usize {
    var count: usize = 0;
    for (nodes[0..]) |*nd| {
        if (nd.raw_node.raftConst().state == .leader) count += 1;
    }
    return count;
}

fn findLeader(nodes: *[3]Node) ?u64 {
    for (nodes[0..]) |*nd| {
        if (nd.raw_node.raftConst().state == .leader) return nd.raw_node.raftConst().id;
    }
    return null;
}

test "multi-node: cluster setup" {
    var cluster = try createCluster();
    defer cluster.destroy();
    try initRawNodes(&cluster.nodes);

    // All nodes should be followers.
    for (&cluster.nodes) |*nd| {
        try std.testing.expectEqual(StateRole.follower, nd.raw_node.raftConst().state);
    }
}

test "multi-node: 3-node leader election" {
    var cluster = try createCluster();
    try initRawNodes(&cluster.nodes);
    registerCallbacks(&cluster.nodes);
    defer cluster.destroy();

    // Campaign node 1.
    try cluster.nodes[0].raw_node.campaign();

    // Drive enough cycles for election to complete.
    var i: usize = 0;
    while (i < 30 and countLeaders(&cluster.nodes) == 0) : (i += 1) {
        try tickCluster(cluster.net, &cluster.nodes);
    }

    // Exactly one leader.
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster.nodes));
    const leader = findLeader(&cluster.nodes).?;
    try std.testing.expect(leader >= 1 and leader <= 3);
}

test "multi-node: partition prevents replication" {
    var cluster = try createCluster();
    try initRawNodes(&cluster.nodes);
    registerCallbacks(&cluster.nodes);
    defer cluster.destroy();

    // Drop all messages → simulates full network partition.
    const dropAll = struct {
        fn filter(_: u64, _: u64, _: MessageType) bool {
            return true;
        }
    };
    cluster.net.drop_filter = dropAll.filter;

    try cluster.nodes[0].raw_node.campaign();
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        try tickCluster(cluster.net, &cluster.nodes);
    }

    // With all messages dropped, no leader can be elected (no votes get through).
    // Each node stays as candidate or follower (single node can't reach quorum).
    // Node 1 started a campaign but can't get votes from 2 and 3.
    for (&cluster.nodes) |*nd| {
        // Nodes should NOT be leader since they can't get a quorum.
        // (Node 1 might briefly become candidate but not leader.)
        const state = nd.raw_node.raftConst().state;
        try std.testing.expect(state != .leader);
    }
}
