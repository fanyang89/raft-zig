const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.heap.smp_allocator;
const cluster_id: raft.ClusterId = .{0x47} ++ .{0x52} ** 15;
const node_count = 4;
const initial_node_count = 3;

const TestStateMachine = struct {
    state: std.ArrayList(u8) = .empty,
    last_applied_index: u64 = 0,
    restore_count: usize = 0,

    fn deinit(self: *TestStateMachine) void {
        self.state.deinit(allocator);
        self.* = undefined;
    }

    fn cast(ctx: *anyopaque) *TestStateMachine {
        return @ptrCast(@alignCast(ctx));
    }

    fn apply(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self = cast(ctx);
        try self.state.appendSlice(allocator, entry.data);
        self.last_applied_index = entry.index;
        return .{};
    }

    fn takeSnapshot(
        ctx: *anyopaque,
        snapshot_allocator: std.mem.Allocator,
        applied_index: u64,
        applied_term: u64,
        conf_state: raft.ConfState,
    ) raft.Error!raft.Snapshot {
        const self = cast(ctx);
        return .{
            .data = if (self.state.items.len == 0) &.{} else try snapshot_allocator.dupe(u8, self.state.items),
            .metadata = .{
                .index = applied_index,
                .term = applied_term,
                .conf_state = try raft.cloneConfState(snapshot_allocator, conf_state),
            },
        };
    }

    fn restoreSnapshot(ctx: *anyopaque, metadata: raft.SnapshotMetadata, reader: raft.SnapshotReader) raft.Error!void {
        const self = cast(ctx);
        var restored: std.ArrayList(u8) = .empty;
        errdefer restored.deinit(allocator);
        var buffer: [256]u8 = undefined;
        while (true) {
            const count = try reader.read(&buffer);
            if (count == 0) break;
            try restored.appendSlice(allocator, buffer[0..count]);
        }
        self.state.deinit(allocator);
        self.state = restored;
        self.last_applied_index = metadata.index;
        self.restore_count += 1;
    }

    fn stateMachine(self: *TestStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable: raft.StateMachine.VTable = .{
        .apply = apply,
        .take_snapshot = takeSnapshot,
        .restore_snapshot = restoreSnapshot,
    };
};

const Completion = struct {
    completed: usize = 0,
    failure: ?raft.Error = null,

    fn callback(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *Completion = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => self.completed += 1,
            .err => |err| self.failure = err,
        }
    }

    fn proposalCallback(self: *Completion) raft.ProposalCallback {
        return .{ .ctx = self, .function = callback };
    }
};

const ReadCompletion = struct {
    machine: *TestStateMachine,
    required_applied_index: u64,
    completed: bool = false,
    observed_applied_index: u64 = 0,
    failure: ?raft.Error = null,

    fn callback(ctx: *anyopaque, result: raft.ReadIndexResult) void {
        const self: *ReadCompletion = @ptrCast(@alignCast(ctx));
        self.observed_applied_index = self.machine.last_applied_index;
        switch (result) {
            .ok => self.completed = true,
            .err => |err| self.failure = err,
        }
    }
};

const Cluster = struct {
    address_buffers: [node_count][64]u8 = undefined,
    addresses: [node_count][]const u8 = undefined,
    storages: [node_count]raft.MemoryStorage = undefined,
    machines: [node_count]TestStateMachine = undefined,
    transports: [node_count]?*raft.GrpcLiteTransport = .{null} ** node_count,
    raftors: [node_count]?*raft.Raftor = .{null} ** node_count,
    storage_count: usize = 0,
    machine_count: usize = 0,

    fn create(ports: [node_count]u16) !*Cluster {
        const self = try allocator.create(Cluster);
        self.* = .{};
        errdefer self.destroy();

        for (0..node_count) |index| {
            self.addresses[index] = try std.fmt.bufPrint(
                &self.address_buffers[index],
                "127.0.0.1:{}",
                .{ports[index]},
            );
            self.storages[index] = raft.MemoryStorage.init();
            self.storage_count += 1;
            self.machines[index] = .{};
            self.machine_count += 1;
            self.transports[index] = try raft.GrpcLiteTransport.create(allocator, .{
                .identity = .{ .cluster_id = cluster_id, .node_id = index + 1 },
                .listen_addr = self.addresses[index],
                .reconnect_initial_delay_ns = std.time.ns_per_ms,
                .reconnect_max_delay_ns = 20 * std.time.ns_per_ms,
                .graceful_shutdown_timeout_ns = 50 * std.time.ns_per_ms,
            });
        }

        for (0..initial_node_count) |index| try self.startBootstrapNode(index);
        return self;
    }

    fn destroy(self: *Cluster) void {
        for (&self.raftors) |*raftor| {
            if (raftor.*) |value| value.destroy();
            raftor.* = null;
        }
        for (&self.transports) |*transport| {
            if (transport.*) |value| value.destroy();
            transport.* = null;
        }
        for (0..self.storage_count) |index| self.storages[index].deinit(allocator);
        for (0..self.machine_count) |index| self.machines[index].deinit();
        allocator.destroy(self);
    }

    fn config(self: *Cluster, index: usize) raft.RaftorConfig {
        var result = raft.RaftorConfig{};
        result.raft.id = index + 1;
        result.raft.election_tick = 50;
        result.raft.heartbeat_tick = 1;
        result.raft.election_timeout_seed = (index + 1) * 7919;
        result.cluster_id = cluster_id;
        result.listen_addr = self.addresses[index];
        result.advertise_addr = self.addresses[index];
        result.snapshot_entries_threshold = 0;
        result.transport_poll_budget = 512;
        return result;
    }

    fn startBootstrapNode(self: *Cluster, index: usize) !void {
        var peers: [initial_node_count]raft.Peer = undefined;
        for (&peers, 0..) |*peer, peer_index| {
            peer.* = .{ .id = peer_index + 1, .context = self.addresses[peer_index] };
        }
        var node_config = self.config(index);
        node_config.initial_peers = &peers;
        self.raftors[index] = try raft.Raftor.createWithDependencies(allocator, node_config, .bootstrap, .{
            .storage = self.storages[index].asWritableStorage(),
            .transport = self.transports[index].?.transport(),
            .state_machine = self.machines[index].stateMachine(),
        });
    }

    fn startJoiningNode(self: *Cluster) !void {
        const index = node_count - 1;
        var seeds: [initial_node_count]raft.Peer = undefined;
        for (&seeds, 0..) |*seed, seed_index| {
            seed.* = .{ .id = seed_index + 1, .context = self.addresses[seed_index] };
        }
        var node_config = self.config(index);
        node_config.join = true;
        node_config.initial_peers = &seeds;
        self.raftors[index] = try raft.Raftor.createWithDependencies(allocator, node_config, .join, .{
            .storage = self.storages[index].asWritableStorage(),
            .transport = self.transports[index].?.transport(),
            .state_machine = self.machines[index].stateMachine(),
        });
    }

    fn drive(self: *Cluster, active_nodes: usize) !void {
        for (self.raftors[0..active_nodes]) |raftor| _ = try raftor.?.tick();
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }

    fn leaderIndex(self: *Cluster, active_nodes: usize) ?usize {
        var leader: ?usize = null;
        for (self.raftors[0..active_nodes], 0..) |raftor, index| {
            if (!raftor.?.isLeader()) continue;
            if (leader != null) return null;
            leader = index;
        }
        return leader;
    }

    fn waitForStableLeader(self: *Cluster, active_nodes: usize) !usize {
        var stable_leader: ?usize = null;
        var stable_rounds: usize = 0;
        for (0..3000) |_| {
            try self.drive(active_nodes);
            const leader = self.leaderIndex(active_nodes);
            if (leader) |index| {
                const leader_id = index + 1;
                var all_agree = true;
                for (self.raftors[0..active_nodes]) |raftor| {
                    if (raftor.?.getLeaderId() != leader_id) all_agree = false;
                }
                if (all_agree and stable_leader == index) {
                    stable_rounds += 1;
                } else if (all_agree) {
                    stable_leader = index;
                    stable_rounds = 1;
                } else {
                    stable_leader = null;
                    stable_rounds = 0;
                }
                if (stable_rounds >= 20) return index;
            } else {
                stable_leader = null;
                stable_rounds = 0;
            }
        }
        return error.TestTimeout;
    }

    fn waitForInitialStreams(self: *Cluster) !void {
        for (0..5000) |_| {
            var active = true;
            for (0..initial_node_count) |from| {
                for (0..initial_node_count) |to| {
                    if (from == to) continue;
                    if (self.transports[from].?.peerState(to + 1) != .active) active = false;
                }
            }
            if (active) return;
            try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
        }
        return error.TestTimeout;
    }

    fn expectInitialStreamsOpenedOnce(self: *Cluster) !void {
        for (0..initial_node_count) |from| {
            for (0..initial_node_count) |to| {
                if (from == to) continue;
                try std.testing.expectEqual(@as(u64, 1), self.transports[from].?.peerOpenCount(to + 1));
            }
        }
    }
};

test "grpc raftor: persistent three-node replication and live learner snapshot join" {
    const ports = try reserveUniquePorts();
    const cluster = try Cluster.create(ports);
    defer cluster.destroy();

    try cluster.waitForInitialStreams();
    try cluster.expectInitialStreamsOpenedOnce();

    try cluster.raftors[0].?.campaign();
    const leader_index = try cluster.waitForStableLeader(initial_node_count);
    try std.testing.expectEqual(@as(usize, 0), leader_index);
    try std.testing.expectEqual(@as(usize, 1), countLeaders(cluster, initial_node_count));

    var proposals = Completion{};
    const values = [_][]const u8{ "alpha", "beta", "gamma", "delta" };
    for (values) |value| try cluster.raftors[leader_index].?.propose(value, proposals.proposalCallback());
    for (0..3000) |_| {
        try cluster.drive(initial_node_count);
        if (proposals.completed == values.len and allStateEquals(cluster, initial_node_count, "alphabetagammadelta")) break;
    }
    try std.testing.expect(proposals.failure == null);
    try std.testing.expectEqual(values.len, proposals.completed);
    try expectConverged(cluster, initial_node_count, "alphabetagammadelta");

    const leader = cluster.raftors[leader_index].?;
    const required_applied_index = leader.getStatus().commit_index;
    var read = ReadCompletion{
        .machine = &cluster.machines[leader_index],
        .required_applied_index = required_applied_index,
    };
    try leader.readIndex("safe-read", .{ .ctx = &read, .function = ReadCompletion.callback });
    for (0..3000) |_| {
        try cluster.drive(initial_node_count);
        if (read.completed or read.failure != null) break;
    }
    try std.testing.expect(read.failure == null);
    try std.testing.expect(read.completed);
    try std.testing.expect(read.observed_applied_index >= read.required_applied_index);
    try cluster.expectInitialStreamsOpenedOnce();

    try leader.addLearner(4, cluster.addresses[3]);
    for (0..3000) |_| {
        try cluster.drive(initial_node_count);
        if (allHaveLearner(cluster, initial_node_count, 4)) break;
    }
    try std.testing.expect(allHaveLearner(cluster, initial_node_count, 4));
    try leader.takeSnapshot();
    var leader_snapshot = (try cluster.storages[leader_index].localSnapshot(allocator)).?;
    defer leader_snapshot.deinit(allocator);
    const snapshot_index = leader_snapshot.metadata.index;
    var snapshot_membership = try raft.decodeClusterMembership(allocator, leader_snapshot.membership);
    defer snapshot_membership.deinit(allocator);
    try std.testing.expectEqualStrings(cluster.addresses[3], snapshot_membership.addressOf(4).?);
    try std.testing.expect(contains(leader_snapshot.metadata.conf_state.learners, 4));
    try std.testing.expect(!contains(leader_snapshot.metadata.conf_state.voters, 4));
    try std.testing.expect((try cluster.storages[leader_index].firstIndex()) > snapshot_index);

    try cluster.startJoiningNode();
    for (0..5000) |_| {
        try cluster.drive(node_count);
        if (joinedFromSnapshot(cluster, snapshot_index)) break;
    }
    try std.testing.expect(joinedFromSnapshot(cluster, snapshot_index));
    try expectDurableLearner(cluster, 3, 4);
    try std.testing.expect(!cluster.raftors[3].?.isLeader());

    const four_node_leader_index = try cluster.waitForStableLeader(node_count);
    var post_join = Completion{};
    try cluster.raftors[four_node_leader_index].?.propose("epsilon", post_join.proposalCallback());
    for (0..3000) |_| {
        try cluster.drive(node_count);
        if (post_join.completed == 1 and allStateEquals(cluster, node_count, "alphabetagammadeltaepsilon")) break;
    }
    try std.testing.expect(post_join.failure == null);
    try std.testing.expectEqual(@as(usize, 1), post_join.completed);
    try expectConverged(cluster, node_count, "alphabetagammadeltaepsilon");
    try expectDurableLearner(cluster, 3, 4);
    try cluster.expectInitialStreamsOpenedOnce();
}

fn countLeaders(cluster: *Cluster, active_nodes: usize) usize {
    var count: usize = 0;
    for (cluster.raftors[0..active_nodes]) |raftor| if (raftor.?.isLeader()) {
        count += 1;
    };
    return count;
}

fn allStateEquals(cluster: *Cluster, active_nodes: usize, expected: []const u8) bool {
    for (cluster.machines[0..active_nodes]) |*machine| {
        if (!std.mem.eql(u8, machine.state.items, expected)) return false;
    }
    return true;
}

fn expectConverged(cluster: *Cluster, active_nodes: usize, expected: []const u8) !void {
    try std.testing.expect(allStateEquals(cluster, active_nodes, expected));
    const first = cluster.raftors[0].?.getStatus();
    try std.testing.expectEqual(first.commit_index, first.applied_index);
    for (cluster.raftors[1..active_nodes]) |raftor| {
        const status = raftor.?.getStatus();
        try std.testing.expectEqual(first.commit_index, status.commit_index);
        try std.testing.expectEqual(first.applied_index, status.applied_index);
    }
}

fn allHaveLearner(cluster: *Cluster, active_nodes: usize, learner_id: u64) bool {
    for (cluster.storages[0..active_nodes]) |*storage| {
        const state = &storage.core.raft_state;
        if (!contains(state.conf_state.learners, learner_id)) return false;
        if (contains(state.conf_state.voters, learner_id)) return false;
        const membership = state.cluster_membership orelse return false;
        if (membership.addressOf(learner_id) == null) return false;
    }
    return true;
}

fn joinedFromSnapshot(cluster: *Cluster, snapshot_index: u64) bool {
    const index = node_count - 1;
    return cluster.machines[index].restore_count > 0 and
        cluster.storages[index].core.snapshot_data.metadata.index >= snapshot_index and
        allHaveLearner(cluster, node_count, 4);
}

fn expectDurableLearner(cluster: *Cluster, node_index: usize, learner_id: u64) !void {
    var state = try cluster.storages[node_index].initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expect(contains(state.conf_state.learners, learner_id));
    try std.testing.expect(!contains(state.conf_state.voters, learner_id));
    try std.testing.expect(state.cluster_membership != null);
    try std.testing.expect(state.cluster_membership.?.addressOf(learner_id) != null);
}

fn contains(values: []const u64, value: u64) bool {
    return std.mem.indexOfScalar(u64, values, value) != null;
}

fn reserveUniquePorts() ![node_count]u16 {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listeners: [node_count]std.Io.net.Server = undefined;
    var listener_count: usize = 0;
    defer for (listeners[0..listener_count]) |*listener| listener.deinit(std.testing.io);

    var ports: [node_count]u16 = undefined;
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
        for (ports[0..index]) |port| try std.testing.expect(port != ports[index]);
    }
    return ports;
}
