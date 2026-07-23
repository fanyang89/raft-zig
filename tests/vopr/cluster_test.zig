const std = @import("std");
const mar = @import("marionette");
const raft = @import("raft_zig");
const adapter = @import("raft_adapter.zig");

const node_count = 3;

const Cluster = struct {
    sim: mar.Sim,
    pool: *adapter.PacketPool,
    peers: []raft.Peer,
    nodes: [node_count]*adapter.NodeProcess,
    completed_proposals: usize = 0,
    minimum_completed_proposals: usize = 0,

    pub fn deinit(self: *Cluster) void {
        const allocator = self.sim.env.allocator();
        for (self.nodes) |node| node.destroy();
        allocator.free(self.peers);
        self.pool.deinit();
        allocator.destroy(self.pool);
    }
};

const Case = mar.SimCase(Cluster);

fn initCluster(sim: mar.Sim) !Cluster {
    const allocator = sim.env.allocator();
    const pool = try allocator.create(adapter.PacketPool);
    pool.* = adapter.PacketPool.init(allocator);
    errdefer {
        pool.deinit();
        allocator.destroy(pool);
    }

    const peers = try allocator.alloc(raft.Peer, node_count);
    errdefer allocator.free(peers);
    for (peers, 1..) |*peer, id| peer.* = .{ .id = id };

    const endpoints = try sim.endpoints(adapter.PacketRef, node_count, 0);
    var nodes: [node_count]*adapter.NodeProcess = undefined;
    var initialized: usize = 0;
    errdefer for (nodes[0..initialized]) |node| node.destroy();
    for (&nodes, endpoints, 1..) |*node, endpoint, id| {
        var config = raft.RaftorConfig{};
        config.raft.id = id;
        config.raft.election_tick = 10;
        config.raft.heartbeat_tick = 1;
        config.raft.election_timeout_seed = id * 997;
        config.initial_peers = peers;
        node.* = try adapter.NodeProcess.create(allocator, config, endpoint, pool);
        try sim.registerProcess(@intCast(id - 1), node.*.lifecycle());
        initialized += 1;
    }
    return .{ .sim = sim, .pool = pool, .peers = peers, .nodes = nodes };
}

fn proposalCallback(ctx: *anyopaque, result: raft.ProposalResult) void {
    const cluster: *Cluster = @ptrCast(@alignCast(ctx));
    if (result == .ok) cluster.completed_proposals += 1;
}

fn drive(case: *Case, rounds: usize) !void {
    for (0..rounds) |_| {
        for (case.app.nodes) |node| {
            if (node.raftor) |raftor| _ = try raftor.tick();
        }
        try assertSafety(&case.app);
    }
}

fn scenario(case: *Case) !void {
    try case.app.nodes[0].raftor.?.campaign();
    try drive(case, 30);
    const leader = findLeader(&case.app) orelse return error.LeaderNotElected;

    try leader.propose("before-partition", .{ .ctx = &case.app, .function = proposalCallback });
    _ = try leader.tick();
    try drive(case, 60);
    try std.testing.expectEqual(@as(usize, 1), case.app.completed_proposals);

    const leader_index = findLeaderIndex(&case.app) orelse return error.LeaderNotElected;
    const minority_index: mar.NodeId = if (leader_index == 2) 1 else 2;
    var majority: [2]mar.NodeId = undefined;
    var majority_count: usize = 0;
    for (0..node_count) |index| {
        if (index == minority_index) continue;
        majority[majority_count] = @intCast(index);
        majority_count += 1;
    }
    const minority = [_]mar.NodeId{minority_index};
    try case.control().network.partition(&majority, &minority);
    const current_leader = findLeader(&case.app) orelse return error.LeaderNotElected;
    try current_leader.propose("during-partition", .{ .ctx = &case.app, .function = proposalCallback });
    _ = try current_leader.tick();
    try drive(case, 20);

    try case.app.sim.killProcess(minority_index);
    try case.app.sim.restartProcess(minority_index);
    try case.control().network.heal();
    try drive(case, 200);
    case.app.minimum_completed_proposals = 2;
}

fn findLeader(cluster: *Cluster) ?*raft.Raftor {
    for (cluster.nodes) |node| {
        const raftor = node.raftor orelse continue;
        if (raftor.isLeader()) return raftor;
    }
    return null;
}

fn findLeaderIndex(cluster: *Cluster) ?mar.NodeId {
    for (cluster.nodes, 0..) |node, index| {
        const raftor = node.raftor orelse continue;
        if (raftor.isLeader()) return @intCast(index);
    }
    return null;
}

fn assertSafety(cluster: *const Cluster) !void {
    for (cluster.nodes, 0..) |left_node, left_index| {
        const left = left_node.raftor orelse continue;
        if (!left.isLeader()) continue;
        const left_status = left.getStatus();
        for (cluster.nodes[left_index + 1 ..]) |right_node| {
            const right = right_node.raftor orelse continue;
            const right_status = right.getStatus();
            if (right.isLeader() and right_status.term == left_status.term) return error.MultipleLeadersInTerm;
        }
    }

    for (cluster.nodes, 0..) |left, left_index| {
        for (cluster.nodes[left_index + 1 ..]) |right| try assertCommittedPrefix(left, right);
    }
}

fn assertCommittedPrefix(left: *adapter.NodeProcess, right: *adapter.NodeProcess) !void {
    var left_state = try left.storage.initialState(left.allocator);
    defer left_state.deinit(left.allocator);
    var right_state = try right.storage.initialState(right.allocator);
    defer right_state.deinit(right.allocator);
    const common_commit = @min(left_state.hard_state.commit, right_state.hard_state.commit);
    if (common_commit == 0) return;

    const left_first = left.storage.core.firstIndex();
    const right_first = right.storage.core.firstIndex();
    const first = @max(left_first, right_first);
    if (first > common_commit) return;
    for (first..common_commit + 1) |index| {
        const left_entry = left.storage.core.entries.items[index - left_first];
        const right_entry = right.storage.core.entries.items[index - right_first];
        if (left_entry.term != right_entry.term or left_entry.entry_type != right_entry.entry_type) return error.CommittedLogMismatch;
        if (!std.mem.eql(u8, left_entry.data, right_entry.data)) return error.CommittedLogMismatch;
    }
}

fn checkConvergence(case: *const Case) !void {
    try assertSafety(&case.app);
    try std.testing.expect(case.app.completed_proposals >= case.app.minimum_completed_proposals);
    const expected = case.app.nodes[0].state_machine.applied.items;
    for (case.app.nodes[1..]) |node| {
        const actual = node.state_machine.applied.items;
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |left, right| try std.testing.expectEqualStrings(left, right);
    }
}

fn chaosScenario(case: *Case) !void {
    try case.app.nodes[0].raftor.?.campaign();
    try drive(case, 30);

    const proposal_values = [_][]const u8{ "chaos-0", "chaos-1", "chaos-2", "chaos-3" };
    for (0..100) |step| {
        const action = randomLessThan(case.env().io(), u8, 9);
        const node_index = randomLessThan(case.env().io(), usize, node_count);
        try case.env().record("raft_vopr.action step={} action={} node={}", .{ step, action, node_index });
        switch (action) {
            0 => if (case.app.nodes[node_index].raftor) |node| {
                _ = try node.tick();
            },
            1 => {
                try drive(case, 1);
            },
            2 => if (case.app.nodes[node_index].raftor != null) {
                _ = try case.app.nodes[node_index].transport.transport().pollOne();
            },
            3 => if (case.app.nodes[node_index].raftor) |node| {
                _ = try node.processReadyStep();
            },
            4 => {
                try case.control().network.heal();
                const minority = [_]mar.NodeId{@intCast(node_index)};
                var majority: [node_count - 1]mar.NodeId = undefined;
                var count: usize = 0;
                for (0..node_count) |index| {
                    if (index == node_index) continue;
                    majority[count] = @intCast(index);
                    count += 1;
                }
                try case.control().network.partition(&majority, &minority);
            },
            5 => try case.control().network.heal(),
            6 => if (aliveCount(&case.app) > 1 and case.app.nodes[node_index].raftor != null) {
                try case.app.sim.killProcess(@intCast(node_index));
            },
            7 => if (case.app.nodes[node_index].raftor == null) {
                try case.app.sim.restartProcess(@intCast(node_index));
            },
            8 => if (findLeader(&case.app)) |leader| {
                try leader.propose(proposal_values[step % proposal_values.len], .{
                    .ctx = &case.app,
                    .function = proposalCallback,
                });
                _ = try leader.tick();
            },
            else => unreachable,
        }
        try assertSafety(&case.app);
    }

    try case.app.sim.transitionToLiveness(&.{ 0, 1, 2 });
    var attempts: usize = 0;
    while (findLeader(&case.app) == null and attempts < 10) : (attempts += 1) {
        try case.app.nodes[0].raftor.?.campaign();
        try drive(case, 20);
    }
    const leader = findLeader(&case.app) orelse return error.LeaderNotElected;
    const completed_before = case.app.completed_proposals;
    try leader.propose("final-marker", .{ .ctx = &case.app, .function = proposalCallback });
    _ = try leader.tick();
    try drive(case, 300);
    case.app.minimum_completed_proposals = completed_before + 1;
}

fn aliveCount(cluster: *const Cluster) usize {
    var count: usize = 0;
    for (cluster.nodes) |node| {
        if (node.raftor != null) count += 1;
    }
    return count;
}

fn randomLessThan(io: std.Io, comptime T: type, less_than: T) T {
    var source: std.Random.IoSource = .{ .io = io };
    return @intCast(source.interface().intRangeLessThan(u64, 0, less_than));
}

const checks = [_]mar.StateCheck(Case){
    .{ .name = "cluster preserves safety and converges", .check = checkConvergence },
};

const simulate_options = mar.World.SimulateOptions{
    .network = .{
        .nodes = node_count,
        .service_nodes = node_count,
        .path_capacity = 4096,
    },
};

test "Marionette full-stack Raft cluster converges" {
    try mar.expectSimPass(.{
        .allocator = std.testing.allocator,
        .seed = 0xA11CE,
        .tick_ns = mar.default_tick_ns,
        .simulate = simulate_options,
        .init = initCluster,
        .scenario = scenario,
        .checks = &checks,
    });
}

test "Marionette full-stack Raft seed sweep" {
    try mar.expectSimFuzz(.{
        .allocator = std.testing.allocator,
        .seed = 0xA11CE,
        .seeds = 8,
        .tick_ns = mar.default_tick_ns,
        .simulate = simulate_options,
        .init = initCluster,
        .scenario = scenario,
        .checks = &checks,
    });
}

test "Marionette full-stack chaos seed sweep" {
    try mar.expectSimFuzz(.{
        .allocator = std.testing.allocator,
        .seed = 0xC4A05,
        .seeds = 16,
        .tick_ns = mar.default_tick_ns,
        .simulate = simulate_options,
        .init = initCluster,
        .scenario = chaosScenario,
        .checks = &checks,
    });
}
