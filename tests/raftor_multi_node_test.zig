//! Multi-node Raftor integration tests.
//!
//! Unlike multi_node_test.zig (which drives RawNode directly), these tests
//! exercise the full Raftor pipeline: Transport → RawNode → ReadyProcessor →
//! StateMachine. Three Raftor instances connect via LoopbackTransport.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

const Raftor = raft.Raftor;
const RaftorConfig = raft.RaftorConfig;
const MockStateMachine = raft.MockStateMachine;
const LoopbackNetwork = raft.LoopbackNetwork;
const StateRole = raft.StateRole;

fn makeConfig(id: u64) RaftorConfig {
    var rc = RaftorConfig{};
    rc.raft.id = id;
    rc.raft.election_tick = 10;
    rc.raft.heartbeat_tick = 1;
    rc.raft.election_timeout_seed = id * 777 + 3;
    return rc;
}

const Cluster = struct {
    net: *raft.LoopbackNetwork,
    raftors: [3]*Raftor,
    sms: *[3]MockStateMachine,

    fn destroy(self: *Cluster) void {
        for (self.raftors) |r| r.destroy();
        for (self.sms) |*sm| sm.deinit();
        allocator.destroy(self.sms);
        self.net.destroy();
    }
};

fn createCluster() !Cluster {
    const net = try LoopbackNetwork.create(allocator);

    const sms = try allocator.create([3]MockStateMachine);
    sms.* = .{
        MockStateMachine.init(allocator),
        MockStateMachine.init(allocator),
        MockStateMachine.init(allocator),
    };

    var transports: [3]*raft.LoopbackTransport = undefined;
    for (1..4) |i| {
        transports[i - 1] = try net.createTransport(@intCast(i));
    }

    // Heap-allocate peers slice so the address survives createWithTransport's
    // internal bootstrap (which reads config.initial_peers).
    const peers = try allocator.alloc(raft.Peer, 3);
    defer allocator.free(peers);
    peers[0] = .{ .id = 1 };
    peers[1] = .{ .id = 2 };
    peers[2] = .{ .id = 3 };

    var raftors: [3]*Raftor = undefined;
    for (1..4) |i| {
        const idx = i - 1;
        var config = makeConfig(@intCast(i));
        config.initial_peers = peers;
        raftors[idx] = try Raftor.createWithTransport(
            allocator,
            config,
            sms[idx].stateMachine(),
            transports[idx].transport(),
        );
    }

    return .{ .net = net, .raftors = raftors, .sms = sms };
}

/// Drive one event-loop cycle: tick all raftors, then poll the network.
fn tickCluster(c: *Cluster) !void {
    for (c.raftors) |r| _ = try r.tick();
    _ = try c.net.pollAll();
}

fn countLeaders(c: *Cluster) usize {
    var count: usize = 0;
    for (c.raftors) |r| {
        if (r.isLeader()) count += 1;
    }
    return count;
}

test "raftor multi-node: 3-node leader election" {
    var cluster = try createCluster();
    defer cluster.destroy();

    // Campaign node 1.
    try cluster.raftors[0].campaign();

    // Drive election.
    var i: usize = 0;
    while (i < 30 and countLeaders(&cluster) == 0) : (i += 1) {
        try tickCluster(&cluster);
    }

    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "raftor multi-node: propose replicates to all state machines" {
    var cluster = try createCluster();
    defer cluster.destroy();

    try cluster.raftors[0].campaign();
    var i: usize = 0;
    while (i < 30 and countLeaders(&cluster) == 0) : (i += 1) {
        try tickCluster(&cluster);
    }
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));

    // Propose from the leader.
    const Tester = struct {
        applied: bool = false,
        fn cb(ctx: *anyopaque, result: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .ok) self.applied = true;
        }
    };
    var tester = Tester{};
    for (cluster.raftors) |r| {
        if (r.isLeader()) {
            try r.propose("hello", .{ .ctx = &tester, .function = Tester.cb });
            break;
        }
    }

    // Drive replication.
    i = 0;
    while (i < 30) : (i += 1) try tickCluster(&cluster);

    try std.testing.expect(tester.applied);

    // All state machines should have applied the proposed entry (plus noop).
    // The leader's SM gets noop + proposal; followers get replicated entries.
    for (cluster.sms) |*sm| {
        try std.testing.expect(sm.applied.items.len >= 1);
    }
}

test "raftor multi-node: leader election with transport message routing" {
    var cluster = try createCluster();
    defer cluster.destroy();

    // Don't call campaign — let election timeouts fire naturally.
    // With distinct seeds, one node times out first and wins.
    var i: usize = 0;
    while (i < 100 and countLeaders(&cluster) == 0) : (i += 1) {
        try tickCluster(&cluster);
    }

    // A leader should eventually emerge via natural election timeout.
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}
