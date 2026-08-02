const std = @import("std");
const linux = std.os.linux;

const grpc = @import("grpc_lite");
const raft = @import("raft_zig");
const admin = @import("admin.zig");
const config_mod = @import("config.zig");
const service_mod = @import("service.zig");
const sqlite = @import("sqlite.zig");
const state_machine = @import("state_machine.zig");

pub const Options = struct {
    tick_interval_ms: u64 = 100,
    election_tick: usize = 20,
    heartbeat_tick: usize = 2,
    proposal_timeout_ticks: u64 = 100,
    read_index_timeout_ticks: u64 = 100,
    snapshot_entries_threshold: u64 = 10_000,
    api_reactor_count: usize = 4,
    max_queued_admin_commands: usize = 64,
    graceful_timeout_ns: u64 = 5 * std.time.ns_per_s,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    options: Options,
    machine: state_machine.SqliteStateMachine,
    transport: *raft.GrpcLiteTransport,
    raftor: *raft.Raftor,
    admin_queue: admin.Queue,
    service: service_mod.DatabaseService,
    registration: service_mod.Registration,
    api_server: grpc.Server,
    driver_thread: std.Thread,
    driver_exited: std.atomic.Value(bool) = .init(false),
    driver_failed: std.atomic.Value(bool) = .init(false),
    driver_stop: std.atomic.Value(bool) = .init(false),
    running: bool = false,

    /// The allocator must support concurrent use by Raft and gRPC threads.
    pub fn create(
        allocator: std.mem.Allocator,
        config: *const config_mod.ServerConfig,
        options: Options,
    ) !*Runtime {
        if (options.api_reactor_count == 0 or options.max_queued_admin_commands == 0 or
            options.graceful_timeout_ns == 0) return error.InvalidConfig;
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.options = options;
        self.driver_exited = .init(false);
        self.driver_failed = .init(false);
        self.driver_stop = .init(false);
        self.running = false;

        const data_dir = try std.fmt.allocPrintSentinel(allocator, "{s}", .{config.data_dir}, 0);
        defer allocator.free(data_dir);
        const fs = raft.realFileSystem();
        _ = try fs.makeDir(data_dir);
        const database_path = try std.fmt.allocPrintSentinel(allocator, "{s}/state.sqlite3", .{config.data_dir}, 0);
        defer allocator.free(database_path);
        self.machine = try state_machine.SqliteStateMachine.initFile(allocator, database_path, config.cluster_id);
        errdefer self.machine.deinit();
        try fs.syncDir(data_dir);

        const max_transport_message = state_machine.max_snapshot_bytes + 1024 * 1024;
        const stream_buffer_bytes = max_transport_message + 5;
        self.transport = try raft.GrpcLiteTransport.create(allocator, .{
            .identity = .{ .cluster_id = config.cluster_id, .node_id = config.node_id },
            .listen_addr = config.raft_listen,
            .stream_limits = .{
                .max_message_size = max_transport_message,
                .max_inbound_buffer_size = stream_buffer_bytes,
                .max_outbound_buffer_size = stream_buffer_bytes,
            },
            .mailbox_max_bytes = max_transport_message * 2,
        });
        errdefer self.transport.destroy();

        var raft_config: raft.RaftorConfig = .{};
        raft_config.raft.id = config.node_id;
        raft_config.raft.election_tick = options.election_tick;
        raft_config.raft.heartbeat_tick = options.heartbeat_tick;
        raft_config.raft.check_quorum = true;
        raft_config.raft.pre_vote = true;
        raft_config.raft.disable_proposal_forwarding = true;
        raft_config.cluster_id = config.cluster_id;
        raft_config.listen_addr = config.raft_listen;
        raft_config.advertise_addr = config.raft_advertise;
        raft_config.initial_peers = config.peers;
        raft_config.join = config.join;
        raft_config.data_dir = config.data_dir;
        raft_config.tick_interval_ms = options.tick_interval_ms;
        raft_config.proposal_timeout_ticks = options.proposal_timeout_ticks;
        raft_config.read_index_timeout_ticks = options.read_index_timeout_ticks;
        raft_config.snapshot_entries_threshold = options.snapshot_entries_threshold;
        raft_config.checksum_enabled = true;
        self.raftor = try raft.Raftor.createWithTransport(
            allocator,
            raft_config,
            self.machine.stateMachine(),
            self.transport.transport(),
        );
        errdefer self.raftor.destroy();

        self.admin_queue = try admin.Queue.init(allocator, options.max_queued_admin_commands);
        errdefer {
            self.admin_queue.close();
            self.admin_queue.deinit();
        }
        self.service = try service_mod.DatabaseService.init(allocator, self.raftor, &self.machine, &self.admin_queue);
        self.registration = self.service.registration();
        errdefer self.registration.deinit();
        self.api_server = try grpc.Server.init(allocator, .{
            .host = config.api_host,
            .port = config.api_port,
            .reactor_count = options.api_reactor_count,
            .max_request_size = state_machine.max_command_bytes,
            .stream_limits = .{
                .max_message_size = sqlite.max_api_response_bytes,
                .max_inbound_buffer_size = sqlite.max_api_response_bytes + 5,
                .max_outbound_buffer_size = sqlite.max_api_response_bytes + 5,
            },
        });
        errdefer self.api_server.deinit();
        try self.registration.register(&self.api_server);

        self.driver_thread = try std.Thread.spawn(.{}, runDriver, .{self});
        errdefer {
            self.driver_stop.store(true, .release);
            self.admin_queue.close();
            self.raftor.stop();
            self.driver_thread.join();
        }
        try self.api_server.start();
        self.running = true;
        return self;
    }

    pub fn shutdown(self: *Runtime) !void {
        if (!self.running) return;
        self.api_server.shutdownGracefully(self.options.graceful_timeout_ns);
        self.driver_stop.store(true, .release);
        self.admin_queue.close();
        self.raftor.stop();
        self.api_server.wait();
        self.driver_thread.join();
        self.running = false;
        if (self.driver_failed.load(.acquire)) return error.RaftDriverFailed;
    }

    pub fn deinit(self: *Runtime) void {
        std.debug.assert(!self.running);
        self.api_server.deinit();
        self.registration.deinit();
        self.admin_queue.deinit();
        self.raftor.destroy();
        self.transport.destroy();
        self.machine.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn driverExited(self: *const Runtime) bool {
        return self.driver_exited.load(.acquire);
    }

    pub fn apiAddress(self: *const Runtime) !grpc.ServerLocalAddress {
        return self.api_server.localAddress();
    }

    pub fn status(self: *const Runtime) raft.NodeStatus {
        return self.raftor.getStatus();
    }

    fn runDriver(self: *Runtime) void {
        while (!self.driver_stop.load(.acquire)) {
            _ = self.raftor.tick() catch |err| {
                if (!(err == error.ShuttingDown and self.driver_stop.load(.acquire))) {
                    self.driver_failed.store(true, .release);
                    raft.log.err(@src(), "Raft driver stopped: {s}", .{@errorName(err)});
                }
                break;
            };
            if (self.admin_queue.tryPop()) |pending| self.processAdmin(pending);
            if (self.driver_stop.load(.acquire)) break;
            sleepUntilNextTick(self);
        }
        self.admin_queue.close();
        if (self.driver_failed.load(.acquire)) self.raftor.stop();
        self.driver_exited.store(true, .release);
    }

    fn processAdmin(self: *Runtime, pending: *admin.Pending) void {
        const result: admin.Result = switch (pending.operation) {
            .add_learner => |member| result: {
                if (!self.raftor.isLeader()) return pending.fail(error.NotLeader);
                const membership = self.raftor.getClusterMembership() orelse return pending.fail(error.MissingClusterMembership);
                if (membership.addressOf(member.node_id) != null) return pending.fail(error.MemberAlreadyExists);
                if (addressInUse(membership, member.node_id, member.address)) return pending.fail(error.DuplicatePeerAddress);
                self.raftor.addLearner(member.node_id, member.address) catch |err| return pending.fail(err);
                break :result .{ .submitted = self.raftor.getMembershipIndex() };
            },
            .promote_member => |member| result: {
                if (!self.raftor.isLeader()) return pending.fail(error.NotLeader);
                if (!self.raftor.getRawNode().raftConst().progress_tracker.conf.learners.contains(member.node_id)) {
                    return pending.fail(error.StepPeerNotFound);
                }
                const raft_node = self.raftor.getRawNode().raftConst();
                const progress = raft_node.progress_tracker.progress.get(member.node_id) orelse
                    return pending.fail(error.StepPeerNotFound);
                if (progress.matched < raft_node.raft_log.committed) return pending.fail(error.LearnerNotCaughtUp);
                self.raftor.addNode(member.node_id, member.address) catch |err| return pending.fail(err);
                break :result .{ .submitted = self.raftor.getMembershipIndex() };
            },
            .remove_member => |node_id| result: {
                if (!self.raftor.isLeader()) return pending.fail(error.NotLeader);
                const membership = self.raftor.getClusterMembership() orelse return pending.fail(error.MissingClusterMembership);
                if (membership.addressOf(node_id) == null) {
                    if (contains(membership.retired_node_ids, node_id)) return pending.fail(error.NodeRetired);
                    return pending.fail(error.StepPeerNotFound);
                }
                const conf = &self.raftor.getRawNode().raftConst().progress_tracker.conf;
                if (conf.voters.incoming.contains(node_id) and conf.voters.incoming.count() == 1) {
                    return pending.fail(error.RemovedAllVoters);
                }
                self.raftor.removeNode(node_id) catch |err| return pending.fail(err);
                break :result .{ .submitted = self.raftor.getMembershipIndex() };
            },
            .update_address => |member| result: {
                if (!self.raftor.isLeader()) return pending.fail(error.NotLeader);
                const membership = self.raftor.getClusterMembership() orelse return pending.fail(error.MissingClusterMembership);
                if (addressInUse(membership, member.node_id, member.address)) return pending.fail(error.DuplicatePeerAddress);
                self.raftor.updateNodeAddress(member.node_id, member.address) catch |err| return pending.fail(err);
                break :result .{ .submitted = self.raftor.getMembershipIndex() };
            },
            .transfer_leadership => |node_id| result: {
                if (!self.raftor.isLeader()) return pending.fail(error.NotLeader);
                const raft_node = self.raftor.getRawNode().raftConst();
                if (node_id == raft_node.id) return pending.fail(error.TransferToSelf);
                if (!raft_node.progress_tracker.conf.voters.incoming.contains(node_id)) {
                    return pending.fail(error.StepPeerNotFound);
                }
                const progress = raft_node.progress_tracker.progress.get(node_id) orelse return pending.fail(error.StepPeerNotFound);
                if (progress.matched < raft_node.raft_log.committed) return pending.fail(error.LearnerNotCaughtUp);
                self.raftor.transferLeader(node_id) catch |err| return pending.fail(err);
                break :result .{ .submitted = self.raftor.getMembershipIndex() };
            },
            .take_snapshot => result: {
                self.raftor.takeSnapshot() catch |err| return pending.fail(err);
                break :result .{ .submitted = self.raftor.getMembershipIndex() };
            },
            .inspect_membership => .{ .membership = self.inspectMembership() catch |err| return pending.fail(err) },
        };
        pending.complete(result);
    }

    fn inspectMembership(self: *Runtime) !admin.Membership {
        const membership = self.raftor.getClusterMembership() orelse return error.MissingClusterMembership;
        const raft_node = self.raftor.getRawNode().raftConst();
        var conf_state = try raft_node.progress_tracker.conf.toConfState(self.allocator);
        defer conf_state.deinit(self.allocator);
        const members = try self.allocator.alloc(admin.Member, membership.peers.len);
        errdefer self.allocator.free(members);
        var initialized: usize = 0;
        errdefer for (members[0..initialized]) |*member| self.allocator.free(member.address);
        for (membership.peers, 0..) |peer, index| {
            const progress = raft_node.progress_tracker.progress.get(peer.node_id);
            const matched_index = if (raft_node.state == .leader and progress != null) progress.?.matched else 0;
            const learner = contains(conf_state.learners, peer.node_id);
            members[index] = .{
                .node_id = peer.node_id,
                .address = try self.allocator.dupe(u8, peer.address),
                .voter = contains(conf_state.voters, peer.node_id),
                .learner = learner,
                .outgoing_voter = contains(conf_state.voters_outgoing, peer.node_id),
                .learner_next = contains(conf_state.learners_next, peer.node_id),
                .matched_index = matched_index,
                .promotion_ready = learner and raft_node.state == .leader and matched_index >= raft_node.raft_log.committed,
            };
            initialized += 1;
        }
        return .{
            .index = self.raftor.getMembershipIndex(),
            .members = members,
            .retired_node_ids = try self.allocator.dupe(u64, membership.retired_node_ids),
        };
    }
};

fn contains(values: []const u64, expected: u64) bool {
    return std.mem.containsAtLeastScalar(u64, values, 1, expected);
}

fn addressInUse(membership: *const raft.ClusterMembership, node_id: u64, address: []const u8) bool {
    for (membership.peers) |peer| {
        if (peer.node_id != node_id and std.mem.eql(u8, peer.address, address)) return true;
    }
    return false;
}

fn sleepUntilNextTick(runtime: *const Runtime) void {
    var remaining = runtime.options.tick_interval_ms *| std.time.ns_per_ms;
    while (remaining > 0 and !runtime.driver_stop.load(.acquire)) {
        const duration = @min(remaining, 10 * std.time.ns_per_ms);
        sleepNanoseconds(duration);
        remaining -= duration;
    }
}

fn sleepNanoseconds(nanoseconds: u64) void {
    var request = linux.timespec{
        .sec = std.math.cast(isize, nanoseconds / std.time.ns_per_s) orelse std.math.maxInt(isize),
        .nsec = @intCast(nanoseconds % std.time.ns_per_s),
    };
    var remaining: linux.timespec = undefined;
    while (true) {
        const rc = linux.nanosleep(&request, &remaining);
        switch (linux.errno(rc)) {
            .SUCCESS => return,
            .INTR => request = remaining,
            else => return,
        }
    }
}
