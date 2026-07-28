//! Top-level Raftor orchestration: ties RawNode, ReadyProcessor, Transport,
//! and StateMachine into a complete Raft server.
//!
//! Raft mutation is single-threaded on the event loop. Proposal and read-index
//! ingress queues are thread-safe, and storage and transport are pluggable.

const std = @import("std");
const linux = std.os.linux;

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");
const memory_storage_mod = @import("memory_storage.zig");
const fs_mod = @import("fs.zig");
const wal_mod = @import("wal.zig");
const raft_config_mod = @import("raft_config.zig");
const raft_mod = @import("raft.zig");
const raw_node_mod = @import("raw_node.zig");
const state_machine_mod = @import("state_machine.zig");
const transport_mod = @import("transport.zig");
const proposal_tracker_mod = @import("proposal_tracker.zig");
const proposal_queue_mod = @import("proposal_queue.zig");
const request_context_mod = @import("request_context.zig");
const ready_processor_mod = @import("ready_processor.zig");
const raftor_config_mod = @import("raftor_config.zig");
const state_role_mod = @import("core/state_role.zig");
const cluster_membership_mod = @import("cluster_membership.zig");

const Error = error_model.Error;
const Entry = types.Entry;
const Message = types.Message;
const ConfState = types.ConfState;
const ConfChangeV2 = types.ConfChangeV2;

const Config = raft_config_mod.Config;
const Raft = raft_mod.Raft;
const RawNode = raw_node_mod.RawNode;
const MemoryStorage = memory_storage_mod.MemoryStorage;
const WALStorage = wal_mod.WALStorage;
const StateMachine = state_machine_mod.StateMachine;
const Transport = transport_mod.Transport;
const NoopTransport = transport_mod.NoopTransport;
const PeerEvent = transport_mod.PeerEvent;
const ProposalTracker = proposal_tracker_mod.ProposalTracker;
const ProposalQueue = proposal_queue_mod.ProposalQueue;
const ReadIndexQueue = proposal_queue_mod.ReadIndexQueue;
const ProposalItem = proposal_queue_mod.ProposalItem;
const ReadIndexItem = proposal_queue_mod.ReadIndexItem;
const DetachedCallbacks = proposal_tracker_mod.DetachedCallbacks;
const ReadyProcessor = ready_processor_mod.ReadyProcessor;
const ReadyPhase = ready_processor_mod.ReadyPhase;
const RaftorConfig = raftor_config_mod.RaftorConfig;
const LegacySnapshotMembership = raftor_config_mod.LegacySnapshotMembership;
const StateRole = state_role_mod.StateRole;
const ClusterMembership = cluster_membership_mod.ClusterMembership;
const ClusterId = cluster_membership_mod.ClusterId;
const PeerEndpoint = cluster_membership_mod.PeerEndpoint;
const Peer = raw_node_mod.Peer;

const log = @import("grpc_lite").log;

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
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

fn currentThreadId() usize {
    return @intCast(std.Thread.getCurrentId());
}

const Lifecycle = enum {
    active,
    stopping,
    stopped,
    terminating,
    terminal,
    destroying,
};

const ShutdownBatch = struct {
    proposals: std.Deque(ProposalItem),
    reads: std.Deque(ReadIndexItem),
    tracked: DetachedCallbacks,
    allocator: std.mem.Allocator,

    fn invoke(self: *ShutdownBatch, proposal_error: Error, read_error: Error) void {
        var proposal_iterator = self.proposals.iterator();
        while (proposal_iterator.next()) |proposal| {
            self.allocator.free(proposal.data);
            self.allocator.free(proposal.ctx);
            proposal.callback.invoke(.{ .err = proposal_error });
        }
        self.proposals.deinit(self.allocator);
        var read_iterator = self.reads.iterator();
        while (read_iterator.next()) |read| {
            self.allocator.free(read.ctx);
            read.callback.invoke(.{ .err = read_error });
        }
        self.reads.deinit(self.allocator);
        self.tracked.invoke(proposal_error, read_error);
        self.* = undefined;
    }
};

/// Storage owned by the convenience constructors.
const StorageBackend = union(enum) {
    memory: MemoryStorage,
    wal: *WALStorage,

    fn asWritableStorage(self: *StorageBackend) storage_mod.WritableStorage {
        return switch (self.*) {
            .memory => |*m| m.asWritableStorage(),
            .wal => |ws| ws.asWritableStorage(),
        };
    }

    fn deinit(self: *StorageBackend, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .memory => |*m| m.deinit(allocator),
            .wal => |ws| ws.deinit(),
        }
    }
};

pub const StartupMode = enum {
    bootstrap,
    restart,
    join,
};

/// Dependencies are borrowed for the lifetime of the Raftor.
pub const RaftorDependencies = struct {
    storage: storage_mod.WritableStorage,
    transport: Transport,
    state_machine: StateMachine,
};

pub const NodeStatus = struct {
    id: u64 = 0,
    role: StateRole = .follower,
    term: u64 = 0,
    leader_id: u64 = 0,
    commit_index: u64 = 0,
    applied_index: u64 = 0,
    pending_proposals: usize = 0,
    queued_proposals: usize = 0,
    queued_proposal_bytes: usize = 0,
    queued_read_indexes: usize = 0,
    queued_read_index_bytes: usize = 0,
    incarnation: u64 = 0,
};

pub const LeaderServicePolicy = struct {
    check_quorum: bool,
    disable_proposal_forwarding: bool,
    proposal_timeout_ticks: u64,
    read_index_timeout_ticks: u64,

    pub fn isSafe(self: LeaderServicePolicy) bool {
        return self.check_quorum and
            self.disable_proposal_forwarding and
            self.proposal_timeout_ticks > 0 and
            self.read_index_timeout_ticks > 0;
    }
};

test "leader service policy requires bounded leader-only requests" {
    try std.testing.expect(!(LeaderServicePolicy{
        .check_quorum = true,
        .disable_proposal_forwarding = true,
        .proposal_timeout_ticks = 0,
        .read_index_timeout_ticks = 10,
    }).isSafe());
    try std.testing.expect((LeaderServicePolicy{
        .check_quorum = true,
        .disable_proposal_forwarding = true,
        .proposal_timeout_ticks = 10,
        .read_index_timeout_ticks = 10,
    }).isSafe());
}

/// A complete Raftor instance. Because the internal MemoryStorage's address
/// is captured by the Storage vtable, this struct must not be moved after
/// `init` returns. Callers should heap-allocate it via `create`.
pub const Raftor = struct {
    allocator: std.mem.Allocator,
    config: RaftorConfig,

    // Subsystems — order matters for initialization.
    owned_storage: ?StorageBackend,
    storage: storage_mod.WritableStorage,
    // Owned only when created internally via `create` (single-node).
    // `createWithTransport` leaves this null and borrows externally.
    noop_transport: ?NoopTransport = null,
    transport: Transport,
    raw_node: RawNode,
    proposal_tracker: ProposalTracker,
    proposal_queue: ProposalQueue,
    read_index_queue: ReadIndexQueue,
    ready_processor: ReadyProcessor,
    request_context_generator: request_context_mod.Generator,

    tick_count: u64 = 0,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    lifecycle: Lifecycle = .active,
    stop_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    run_active: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    event_loop_active: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    event_loop_thread_id: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    shutdown_callback_thread_id: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    status_mutex: std.atomic.Mutex = .unlocked,
    status_snapshot: NodeStatus = .{},
    terminal_error: ?Error = null,
    transport_stopped: bool = false,

    // Snapshot triggering state.
    last_snapshot_index: u64 = 0,
    last_snapshot_attempt_index: u64 = 0,
    last_snapshot_attempt_tick: u64 = 0,
    last_snapshot_tick: u64 = 0,

    const PendingProposal = struct {
        data: []u8,
        ctx: []u8,
        callback: proposal_tracker_mod.ProposalCallback,
    };

    const PendingRead = struct {
        ctx: []u8,
        callback: proposal_tracker_mod.ReadIndexCallback,
    };

    /// Create a Raftor with an internal NoopTransport (single-node mode).
    pub fn create(
        allocator: std.mem.Allocator,
        config: RaftorConfig,
        state_machine: StateMachine,
    ) Error!*Raftor {
        const self = try allocator.create(Raftor);
        errdefer allocator.destroy(self);

        self.owned_storage = try openStorage(allocator, config);
        errdefer if (self.owned_storage) |*storage| storage.deinit(allocator);
        self.noop_transport = NoopTransport.init(allocator);
        errdefer if (self.noop_transport) |*transport| transport.deinit();

        const storage = self.owned_storage.?.asWritableStorage();
        const startup_mode = try detectStartupMode(allocator, storage, config);
        try self.initInternal(allocator, config, startup_mode, .{
            .storage = storage,
            .transport = self.noop_transport.?.transport(),
            .state_machine = state_machine,
        });
        return self;
    }

    /// Create a Raftor with an externally-owned Transport (multi-node mode).
    /// The caller must keep `transport` alive for the Raftor's lifetime.
    pub fn createWithTransport(
        allocator: std.mem.Allocator,
        config: RaftorConfig,
        state_machine: StateMachine,
        transport: Transport,
    ) Error!*Raftor {
        const self = try allocator.create(Raftor);
        errdefer allocator.destroy(self);

        self.owned_storage = try openStorage(allocator, config);
        errdefer if (self.owned_storage) |*storage| storage.deinit(allocator);
        self.noop_transport = null;

        const storage = self.owned_storage.?.asWritableStorage();
        const startup_mode = try detectStartupMode(allocator, storage, config);
        try self.initInternal(allocator, config, startup_mode, .{
            .storage = storage,
            .transport = transport,
            .state_machine = state_machine,
        });
        return self;
    }

    pub fn createWithDependencies(
        allocator: std.mem.Allocator,
        config: RaftorConfig,
        startup_mode: StartupMode,
        dependencies: RaftorDependencies,
    ) Error!*Raftor {
        const self = try allocator.create(Raftor);
        errdefer allocator.destroy(self);

        self.owned_storage = null;
        self.noop_transport = null;
        try self.initInternal(allocator, config, startup_mode, dependencies);
        return self;
    }

    /// Stop and destroy the Raftor after active run/event-loop calls and
    /// shutdown callbacks quiesce. The owner must first stop all other API
    /// callers. Calling this directly or indirectly from a callback is invalid.
    pub fn destroy(self: *Raftor) void {
        self.assertDestroyAllowed();
        self.stop();
        self.waitForQuiescence();

        spinLock(&self.lifecycle_mutex);
        if (self.lifecycle == .destroying) @panic("concurrent Raftor.destroy calls");
        std.debug.assert(self.lifecycle == .stopped or self.lifecycle == .terminal);
        self.lifecycle = .destroying;
        self.lifecycle_mutex.unlock();

        const allocator = self.allocator;
        self.deinitInternal();
        allocator.destroy(self);
    }

    fn initInternal(
        self: *Raftor,
        allocator: std.mem.Allocator,
        config: RaftorConfig,
        startup_mode: StartupMode,
        dependencies: RaftorDependencies,
    ) Error!void {
        self.allocator = allocator;
        self.config = config;
        self.storage = dependencies.storage;
        self.transport = dependencies.transport;
        self.lifecycle_mutex = .unlocked;
        self.lifecycle = .active;
        self.stop_requested = std.atomic.Value(bool).init(false);
        self.run_active = std.atomic.Value(bool).init(false);
        self.event_loop_active = std.atomic.Value(bool).init(false);
        self.event_loop_thread_id = std.atomic.Value(usize).init(0);
        self.shutdown_callback_thread_id = std.atomic.Value(usize).init(0);
        self.status_mutex = .unlocked;
        self.status_snapshot = .{};
        self.terminal_error = null;
        self.transport_stopped = false;

        try config.raft.validate();
        if (config.transport_poll_budget == 0) return error.InvalidConfig;
        if (config.max_queued_proposals == 0 or config.max_queued_proposal_bytes == 0) return error.InvalidConfig;
        if (config.proposal_drain_budget == 0) return error.InvalidConfig;
        if (config.read_index_drain_budget == 0) return error.InvalidConfig;
        if (config.max_queued_read_indexes == 0 or config.max_queued_read_index_bytes == 0) return error.InvalidConfig;
        try self.prepareStorage(startup_mode);
        var initial_state = try self.storage.initialState(allocator);
        defer initial_state.deinit(allocator);
        if (initial_state.cluster_membership) |membership| {
            membership.validate(initial_state.conf_state) catch return error.InvalidClusterMembership;
        } else if (initial_state.membership_index != 0) {
            return error.MissingClusterMembership;
        }
        if (self.transport.identity()) |identity| {
            if (identity.node_id != config.nodeId()) return error.TransportIdentityMismatch;
            if (initial_state.cluster_membership) |membership| {
                if (!std.mem.eql(u8, &identity.cluster_id, &membership.cluster_id)) {
                    return error.TransportIdentityMismatch;
                }
            } else return error.MissingClusterMembership;
        }
        const incarnation = try self.storage.reserveIncarnation();
        self.request_context_generator = request_context_mod.Generator.init(config.nodeId(), incarnation);
        const initial_applied_index = if (startup_mode == .restart)
            try self.restoreLocalSnapshot(dependencies.state_machine, config.raft.applied)
        else
            config.raft.applied;

        self.proposal_tracker = ProposalTracker.init(allocator);
        self.proposal_queue = ProposalQueue.init(allocator, .{
            .max_items = config.max_queued_proposals,
            .max_bytes = config.max_queued_proposal_bytes,
        });
        self.read_index_queue = ReadIndexQueue.init(allocator, .{
            .max_items = config.max_queued_read_indexes,
            .max_bytes = config.max_queued_read_index_bytes,
        });
        errdefer {
            self.proposal_queue.deinit();
            self.read_index_queue.deinit();
            self.proposal_tracker.deinit();
        }
        self.tick_count = 0;
        self.last_snapshot_index = initial_applied_index;
        self.last_snapshot_attempt_index = 0;
        self.last_snapshot_attempt_tick = 0;
        self.last_snapshot_tick = 0;

        // Build RawNode AFTER storage is at its final address.
        var raft_config = config.raft;
        raft_config.load_state_on_startup = startup_mode != .bootstrap;
        raft_config.applied = initial_applied_index;
        self.raw_node = try RawNode.init(allocator, raft_config, self.storage.asStorage());
        errdefer self.raw_node.deinit();

        // Build ReadyProcessor AFTER raw_node is at its final address.
        const initial_membership = initial_state.cluster_membership;
        initial_state.cluster_membership = null;
        self.ready_processor = ReadyProcessor.init(
            allocator,
            &self.raw_node,
            self.storage,
            dependencies.state_machine,
            self.transport,
            &self.proposal_tracker,
            config.nodeId(),
            config.checksum_enabled,
            initial_applied_index,
            initial_membership,
            initial_state.membership_index,
        );
        errdefer self.ready_processor.deinit();
        try self.ready_processor.hydrateTransport();
        self.status_snapshot = self.currentCoreStatus();

        // Register callbacks only after transport peers reflect durable membership.
        self.transport.setMessageCallback(.{
            .ctx = self,
            .function = onMessage,
        });
        self.transport.setPeerEventCallback(.{
            .ctx = self,
            .function = onPeerEvent,
        });
        self.transport.start() catch |err| {
            self.transport.setMessageCallback(null);
            self.transport.setPeerEventCallback(null);
            self.transport.stop();
            self.transport_stopped = true;
            return err;
        };
    }

    fn restoreLocalSnapshot(self: *Raftor, state_machine: StateMachine, fallback_applied_index: u64) Error!u64 {
        var snapshot = (try self.storage.localSnapshot(self.allocator)) orelse return fallback_applied_index;
        defer snapshot.deinit(self.allocator);
        if (snapshot.metadata.index == 0) return fallback_applied_index;
        var reader = state_machine_mod.BufferSnapshotReader.init(snapshot.data);
        try state_machine.restoreSnapshot(snapshot.metadata, reader.reader());
        log.info(@src(), "restored local snapshot: node_id={}, index={}, term={}", .{
            self.config.nodeId(),
            snapshot.metadata.index,
            snapshot.metadata.term,
        });
        return snapshot.metadata.index;
    }

    fn prepareStorage(self: *Raftor, startup_mode: StartupMode) Error!void {
        var state = try self.storage.initialState(self.allocator);
        defer state.deinit(self.allocator);

        switch (startup_mode) {
            .bootstrap => {
                if (self.config.legacy_membership_migration != null) return error.InvalidConfig;
                if (!state.hard_state.isEmpty() or !confStateIsEmpty(state.conf_state) or try self.storage.lastIndex() != 0) {
                    return error.IncompatibleStorage;
                }
                if (self.config.cluster_id) |_| {
                    var initial = try buildInitialMembership(self.allocator, self.config, false);
                    defer initial.deinit(self.allocator);
                    try self.storage.setMembershipState(
                        self.allocator,
                        .{ .voters = initial.voters },
                        initial.membership,
                        0,
                    );
                } else {
                    const voters = if (self.config.initial_peers.len > 0)
                        try buildVoterIds(self.allocator, self.config)
                    else
                        try self.allocator.dupe(u64, &.{self.config.nodeId()});
                    defer self.allocator.free(voters);
                    try self.storage.setConfState(self.allocator, .{ .voters = voters });
                }
                try self.storage.sync();
            },
            .restart => {
                if (confStateIsEmpty(state.conf_state)) return error.IncompatibleStorage;
                if (state.cluster_membership != null and self.config.legacy_membership_migration != null) {
                    return error.InvalidConfig;
                }
                if (state.cluster_membership == null and self.config.cluster_id != null) {
                    const migration = self.config.legacy_membership_migration orelse
                        return error.LegacyMembershipMigrationRequired;
                    var current_membership = try buildLegacyMembership(self.allocator, self.config.cluster_id.?, .{
                        .peers = migration.peers,
                        .retired_node_ids = migration.retired_node_ids,
                    });
                    defer current_membership.deinit(self.allocator);
                    current_membership.validate(state.conf_state) catch return error.InvalidClusterMembership;

                    var local_snapshot = try self.storage.localSnapshot(self.allocator);
                    defer if (local_snapshot) |*snapshot| snapshot.deinit(self.allocator);
                    var snapshot_membership: ?ClusterMembership = null;
                    defer if (snapshot_membership) |*membership| membership.deinit(self.allocator);
                    if (local_snapshot) |snapshot| {
                        const historical = migration.snapshot orelse return error.LegacySnapshotMigrationRequired;
                        snapshot_membership = try buildLegacyMembership(self.allocator, self.config.cluster_id.?, historical);
                        snapshot_membership.?.validate(snapshot.metadata.conf_state) catch return error.InvalidClusterMembership;
                    } else if (migration.snapshot) |historical| {
                        snapshot_membership = try buildLegacyMembership(self.allocator, self.config.cluster_id.?, historical);
                    }

                    try self.storage.migrateLegacyMembership(
                        self.allocator,
                        current_membership,
                        migration.membership_index,
                        snapshot_membership,
                    );
                    state.deinit(self.allocator);
                    state = try self.storage.initialState(self.allocator);
                } else if (state.cluster_membership == null and self.config.legacy_membership_migration != null) {
                    return error.ClusterIdRequired;
                }
                if (state.cluster_membership) |membership| {
                    if (self.config.cluster_id) |cluster_id| {
                        if (!std.mem.eql(u8, &cluster_id, &membership.cluster_id)) return error.ClusterIdMismatch;
                    }
                    if (containsSorted(membership.retired_node_ids, self.config.nodeId())) return error.NodeRetired;
                } else if (self.config.cluster_id != null) {
                    return error.MissingClusterMembership;
                }
            },
            .join => {
                if (self.config.legacy_membership_migration != null) return error.InvalidConfig;
                if (!state.hard_state.isEmpty() or !confStateIsEmpty(state.conf_state) or try self.storage.lastIndex() != 0) {
                    return error.IncompatibleStorage;
                }
                var initial = try buildInitialMembership(self.allocator, self.config, true);
                defer initial.deinit(self.allocator);
                try self.storage.setMembershipState(
                    self.allocator,
                    .{ .voters = initial.voters },
                    initial.membership,
                    0,
                );
                try self.storage.sync();
            },
        }
    }

    fn deinitInternal(self: *Raftor) void {
        self.transport.setMessageCallback(null);
        self.transport.setPeerEventCallback(null);
        self.ready_processor.deinit();
        self.proposal_queue.deinit();
        self.read_index_queue.deinit();
        self.proposal_tracker.deinit();
        self.raw_node.deinit();
        if (self.noop_transport) |*nt| nt.deinit();
        if (self.owned_storage) |*storage| storage.deinit(self.allocator);
    }

    fn detachRequestsLocked(self: *Raftor) ShutdownBatch {
        return .{
            .proposals = self.proposal_queue.takeAll(),
            .reads = self.read_index_queue.takeAll(),
            .tracked = self.proposal_tracker.detachAll(),
            .allocator = self.allocator,
        };
    }

    fn markTransportStoppedLocked(self: *Raftor) bool {
        if (self.transport_stopped) return false;
        self.transport_stopped = true;
        return true;
    }

    /// Inbound message callback. Transfers message ownership to `step()`.
    fn onMessage(ctx: *anyopaque, msg: Message) Error!void {
        const self: *Raftor = @ptrCast(@alignCast(ctx));
        std.debug.assert(self.event_loop_active.load(.acquire));
        if (self.driverError()) |err| {
            var owned = msg;
            owned.deinit(self.allocator);
            return err;
        }
        self.raw_node.step(msg) catch |err| switch (err) {
            error.StepLocalMsg, error.StepPeerNotFound => {},
            else => return err,
        };
    }

    fn onPeerEvent(ctx: *anyopaque, event: PeerEvent) Error!void {
        const self: *Raftor = @ptrCast(@alignCast(ctx));
        std.debug.assert(self.event_loop_active.load(.acquire));
        if (self.driverError()) |err| return err;
        switch (event.kind) {
            .@"unreachable", .identity_rejected => try self.raw_node.reportUnreachable(event.peer_id),
            .snapshot_failure => {
                try self.raw_node.reportUnreachable(event.peer_id);
                try self.raw_node.reportSnapshot(event.peer_id, .failure);
            },
        }
    }

    // -----------------------------------------------------------------------
    // Event loop
    // -----------------------------------------------------------------------

    /// Advance the event loop by one tick. Returns true if there was work.
    pub fn tick(self: *Raftor) Error!bool {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        return self.tickImpl();
    }

    fn tickImpl(self: *Raftor) Error!bool {
        if (self.driverError()) |err| return err;
        if (self.ready_processor.phase() != null) {
            _ = try self.processReady();
            return true;
        }
        self.tick_count += 1;

        self.proposal_tracker.expireTimeouts(self.tick_count);

        // Drain pending proposals from the thread-safe queue.
        var had_work = false;
        const proposal_timeout = if (self.config.proposal_timeout_ticks > 0) self.config.proposal_timeout_ticks else 0;
        for (0..self.config.proposal_drain_budget) |_| {
            spinLock(&self.lifecycle_mutex);
            if (self.lifecycle != .active) {
                self.lifecycle_mutex.unlock();
                break;
            }
            const item = self.proposal_queue.tryPop() orelse {
                self.lifecycle_mutex.unlock();
                break;
            };
            const track_error: ?Error = if (self.proposal_tracker.track(
                item.ctx,
                item.callback,
                self.tick_count,
                proposal_timeout,
            )) |_| null else |err| err;
            self.lifecycle_mutex.unlock();
            var data = item.data;
            defer self.allocator.free(data);
            defer self.allocator.free(item.ctx);
            if (track_error) |err| {
                item.callback.invoke(.{ .err = err });
                continue;
            }
            self.raw_node.proposeOwned(item.ctx, &data) catch |e| {
                self.proposal_tracker.fail(item.ctx, e);
                if (e != error.ProposalDropped) return e;
            };
            had_work = true;
        }

        // Drain pending reads.
        const read_timeout = if (self.config.read_index_timeout_ticks > 0) self.config.read_index_timeout_ticks else 0;
        for (0..self.config.read_index_drain_budget) |_| {
            spinLock(&self.lifecycle_mutex);
            if (self.lifecycle != .active) {
                self.lifecycle_mutex.unlock();
                break;
            }
            const item = self.read_index_queue.tryPop() orelse {
                self.lifecycle_mutex.unlock();
                break;
            };
            const track_error: ?Error = if (self.proposal_tracker.trackRead(
                item.ctx,
                item.callback,
                self.tick_count,
                read_timeout,
            )) |_| null else |err| err;
            self.lifecycle_mutex.unlock();
            defer self.allocator.free(item.ctx);
            if (track_error) |err| {
                item.callback.invoke(.{ .err = err });
                continue;
            }
            self.raw_node.readIndex(item.ctx) catch |err| {
                self.proposal_tracker.failRead(item.ctx, err);
                return err;
            };
            had_work = true;
        }

        _ = try self.raw_node.tick();
        had_work = true;

        while (try self.processReady()) {
            had_work = true;
        }

        // Bound transport work so a sustained burst cannot starve later ticks.
        for (0..self.config.transport_poll_budget) |_| {
            if (!try self.transport.pollOne()) break;
            had_work = true;
        }

        // Process any additional readys generated by inbound messages.
        while (try self.processReady()) {
            had_work = true;
        }

        self.maybeAutoSnapshot() catch |e| switch (e) {
            error.SnapshotOutOfDate => log.debug(@src(), "snapshot attempt skipped: {s}", .{@errorName(e)}),
            else => log.warn(@src(), "snapshot attempt failed: {s}", .{@errorName(e)}),
        };

        return had_work;
    }

    /// Run the event loop until `stop()` is called. Blocks the caller.
    pub fn run(self: *Raftor) Error!void {
        spinLock(&self.lifecycle_mutex);
        if (self.lifecycle != .active) {
            const err = self.terminal_error orelse error.ShuttingDown;
            self.lifecycle_mutex.unlock();
            return err;
        }
        if (self.run_active.load(.monotonic)) {
            self.lifecycle_mutex.unlock();
            return error.AlreadyStarted;
        }
        self.run_active.store(true, .release);
        self.lifecycle_mutex.unlock();
        defer self.run_active.store(false, .release);
        while (!self.stop_requested.load(.acquire)) {
            _ = self.tick() catch |err| {
                spinLock(&self.lifecycle_mutex);
                const clean_shutdown = err == error.ShuttingDown and self.terminal_error == null and self.lifecycle != .active;
                self.lifecycle_mutex.unlock();
                if (clean_shutdown) return;
                return err;
            };
            if (self.stop_requested.load(.acquire)) break;
            self.sleepUntilNextTick();
        }
    }

    /// Request shutdown and complete accepted callbacks exactly once. This is
    /// callback-safe and does not wait for an active `run` call to return.
    pub fn stop(self: *Raftor) void {
        var batch: ?ShutdownBatch = null;
        spinLock(&self.lifecycle_mutex);
        self.stop_requested.store(true, .release);
        if (self.lifecycle == .active) {
            self.lifecycle = .stopping;
            batch = self.detachRequestsLocked();
        }
        const stop_transport = self.markTransportStoppedLocked();
        self.lifecycle_mutex.unlock();
        const owns_shutdown = batch != null;

        if (stop_transport) self.transport.stop();
        if (batch) |*detached| self.invokeShutdownBatch(detached, error.ShuttingDown, error.ShuttingDown);

        if (owns_shutdown) {
            spinLock(&self.lifecycle_mutex);
            if (self.lifecycle == .stopping) self.lifecycle = .stopped;
            self.lifecycle_mutex.unlock();
        }
    }

    /// Whether a `run` call can still access this Raftor.
    pub fn isRunning(self: *const Raftor) bool {
        return self.run_active.load(.acquire);
    }

    // -----------------------------------------------------------------------
    // Proposals
    // -----------------------------------------------------------------------

    pub fn propose(self: *Raftor, data: []const u8, callback: proposal_tracker_mod.ProposalCallback) !void {
        const queued_bytes = std.math.add(usize, data.len, request_context_mod.header_size) catch return error.ProposalBackpressure;
        if (queued_bytes > self.config.max_queued_proposal_bytes) return error.ProposalBackpressure;
        const data_copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(data_copy);
        const ctx_copy = try self.request_context_generator.next(self.allocator, .proposal, "");
        errdefer self.allocator.free(ctx_copy);
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.lifecycle != .active) return error.ShuttingDown;
        try self.proposal_queue.push(data_copy, ctx_copy, callback);
    }

    pub fn readIndex(self: *Raftor, ctx: []const u8, callback: proposal_tracker_mod.ReadIndexCallback) !void {
        const queued_bytes = std.math.add(usize, ctx.len, request_context_mod.header_size) catch return error.ReadIndexBackpressure;
        if (queued_bytes > self.config.max_queued_read_index_bytes) return error.ReadIndexBackpressure;
        const ctx_copy = try self.request_context_generator.next(self.allocator, .read_index, ctx);
        errdefer self.allocator.free(ctx_copy);
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.lifecycle != .active) return error.ShuttingDown;
        try self.read_index_queue.push(ctx_copy, callback);
    }

    // -----------------------------------------------------------------------
    // Cluster management
    // -----------------------------------------------------------------------

    pub fn campaign(self: *Raftor) Error!void {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        if (self.driverError()) |err| return err;
        try self.raw_node.campaign();
        while (try self.processReady()) {}
    }

    pub fn transferLeader(self: *Raftor, target_id: u64) Error!void {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        if (self.driverError()) |err| return err;
        try self.raw_node.transferLeader(target_id);
    }

    pub fn addNode(self: *Raftor, id: u64, addr: []const u8) Error!void {
        return self.proposeNodeAddressChange(.add_node, id, addr);
    }

    pub fn addLearner(self: *Raftor, id: u64, addr: []const u8) Error!void {
        return self.proposeNodeAddressChange(.add_learner_node, id, addr);
    }

    pub fn updateNodeAddress(self: *Raftor, id: u64, addr: []const u8) Error!void {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        if (self.driverError()) |err| return err;
        const membership = self.ready_processor.getClusterMembership() orelse return error.MissingClusterMembership;
        try rejectRetiredId(membership.*, id);
        if (addr.len == 0) return error.PeerAddressMissing;
        const current_addr = membership.addressOf(id) orelse return error.StepPeerNotFound;
        if (std.mem.eql(u8, current_addr, addr)) return error.ConflictingPeerAddress;
        try self.proposeNodeAddressChangeImpl(.update_node, id, addr, true);
    }

    fn proposeNodeAddressChange(self: *Raftor, change_type: types.ConfChangeType, id: u64, addr: []const u8) Error!void {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        if (self.driverError()) |err| return err;
        try self.proposeNodeAddressChangeImpl(change_type, id, addr, false);
    }

    fn proposeNodeAddressChangeImpl(
        self: *Raftor,
        change_type: types.ConfChangeType,
        id: u64,
        addr: []const u8,
        durable_required: bool,
    ) Error!void {
        if (id == 0) return error.InvalidNodeId;
        const membership = self.ready_processor.getClusterMembership();
        if (durable_required and membership == null) return error.MissingClusterMembership;
        if (membership) |current| try rejectRetiredId(current.*, id);

        var cc = ConfChangeV2{ .changes = try self.allocator.alloc(types.ConfChangeSingle, 1) };
        defer self.allocator.free(cc.changes);
        cc.changes[0] = .{ .change_type = change_type, .node_id = id };
        if (membership) |current| {
            if (addr.len == 0) return error.PeerAddressMissing;
            const current_addr = current.addressOf(id);
            if (change_type != .update_node and current_addr != null and !std.mem.eql(u8, current_addr.?, addr)) {
                return error.ConflictingPeerAddress;
            }
            var endpoints = [_]PeerEndpoint{.{ .node_id = id, .address = @constCast(addr) }};
            const context_endpoints: []PeerEndpoint = if (change_type == .update_node or current_addr == null) &endpoints else &.{};
            cc.context = (cluster_membership_mod.MembershipContext{ .endpoints = context_endpoints }).encode(self.allocator) catch |err| return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                error.InvalidNodeId => error.InvalidNodeId,
                error.EmptyAddress => error.PeerAddressMissing,
                else => error.InvalidClusterMembership,
            };
        } else {
            cc.context = try self.allocator.dupe(u8, addr);
        }
        defer self.allocator.free(cc.context);
        try self.raw_node.proposeConfChange(addr, cc);
    }

    pub fn removeNode(self: *Raftor, id: u64) Error!void {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        if (self.driverError()) |err| return err;
        if (self.ready_processor.getClusterMembership()) |membership| try rejectRetiredId(membership.*, id);
        var cc = ConfChangeV2{ .changes = try self.allocator.alloc(types.ConfChangeSingle, 1) };
        defer self.allocator.free(cc.changes);
        cc.changes[0] = .{ .change_type = .remove_node, .node_id = id };
        try self.raw_node.proposeConfChange("", cc);
    }

    // -----------------------------------------------------------------------
    // Snapshot triggering
    // -----------------------------------------------------------------------

    /// Manually trigger a snapshot at the current applied_index. The
    /// StateMachine's `takeSnapshot` is called, then the result is persisted
    /// via `storage.applyLocalSnapshot` (which also compacts old entries).
    pub fn takeSnapshot(self: *Raftor) Error!void {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        return self.takeSnapshotImpl();
    }

    fn takeSnapshotImpl(self: *Raftor) Error!void {
        if (self.driverError()) |err| return err;
        const applied_index = self.ready_processor.getAppliedIndex();
        if (applied_index == 0) return;

        const init_state = try self.storage.initialState(self.allocator);
        var is_copy = init_state;
        defer is_copy.deinit(self.allocator);

        const applied_term = self.storage.term(applied_index) catch 0;

        var snap = try self.ready_processor.state_machine.takeSnapshot(
            self.allocator,
            applied_index,
            applied_term,
            is_copy.conf_state,
        );
        defer snap.deinit(self.allocator);

        if (self.ready_processor.getClusterMembership()) |membership| {
            const encoded_membership = membership.encode(self.allocator) catch |err| return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidClusterMembership,
            };
            if (snap.membership.len != 0) self.allocator.free(snap.membership);
            snap.membership = encoded_membership;
        }

        log.info(@src(), "node {} taking snapshot at index {} term {}", .{ self.raw_node.raftConst().id, applied_index, applied_term });
        try self.storage.applyLocalSnapshot(self.allocator, snap);

        self.last_snapshot_tick = self.tick_count;
        self.last_snapshot_index = applied_index;
        self.last_snapshot_attempt_index = applied_index;
        self.last_snapshot_attempt_tick = self.tick_count;
    }

    /// Check if a snapshot should be automatically triggered based on the
    /// configured thresholds. Called at the end of each `tick()`.
    fn maybeAutoSnapshot(self: *Raftor) Error!void {
        const cfg = self.config;
        const entries_threshold = cfg.snapshot_entries_threshold;
        const interval_ticks = cfg.snapshot_interval_ticks;

        // Both zero → auto-snapshot disabled.
        if (entries_threshold == 0 and interval_ticks == 0) return;

        const applied_index = self.ready_processor.getAppliedIndex();
        if (applied_index == 0) return;

        // Rate limiting: if applied_index hasn't advanced and we tried
        // recently, skip.
        if (applied_index <= self.last_snapshot_attempt_index and
            (self.tick_count - self.last_snapshot_attempt_tick) < cfg.snapshot_retry_min_ticks)
        {
            return;
        }

        // Condition 1: entry count threshold.
        const snapshot_index = self.last_snapshot_index;
        if (entries_threshold > 0 and applied_index > snapshot_index and
            (applied_index - snapshot_index) >= entries_threshold)
        {
            self.last_snapshot_attempt_index = applied_index;
            self.last_snapshot_attempt_tick = self.tick_count;
            try self.takeSnapshotImpl();
            return;
        }

        // Condition 2: time interval.
        if (interval_ticks > 0 and
            (self.tick_count - self.last_snapshot_tick) >= interval_ticks)
        {
            self.last_snapshot_attempt_index = applied_index;
            self.last_snapshot_attempt_tick = self.tick_count;
            try self.takeSnapshotImpl();
            return;
        }
    }

    // -----------------------------------------------------------------------
    // Status
    // -----------------------------------------------------------------------

    /// Return the most recently published core state plus live ingress stats.
    /// Core state is published at event-loop boundaries and may be one active
    /// tick behind. This may run concurrently with `run`, but not `destroy`.
    pub fn getStatus(self: *const Raftor) NodeStatus {
        var result = self.observedCoreStatus();
        const queue_stats = self.proposal_queue.stats();
        const read_queue_stats = self.read_index_queue.stats();
        result.pending_proposals = self.proposal_tracker.pendingCount();
        result.queued_proposals = queue_stats.count;
        result.queued_proposal_bytes = queue_stats.bytes;
        result.queued_read_indexes = read_queue_stats.count;
        result.queued_read_index_bytes = read_queue_stats.bytes;
        return result;
    }

    pub fn isLeader(self: *const Raftor) bool {
        return self.observedCoreStatus().role == .leader;
    }

    pub fn getLeaderId(self: *const Raftor) u64 {
        return self.observedCoreStatus().leader_id;
    }

    pub fn leaderServicePolicy(self: *const Raftor) LeaderServicePolicy {
        return .{
            .check_quorum = self.config.raft.check_quorum,
            .disable_proposal_forwarding = self.config.raft.disable_proposal_forwarding,
            .proposal_timeout_ticks = self.config.proposal_timeout_ticks,
            .read_index_timeout_ticks = self.config.read_index_timeout_ticks,
        };
    }

    /// Return a mutable escape hatch for event-loop-thread-only use.
    /// Mutations become visible to status readers after the next event-loop
    /// boundary; concurrent access is unsupported.
    pub fn getRawNode(self: *Raftor) *RawNode {
        return &self.raw_node;
    }

    pub fn getClusterMembership(self: *const Raftor) ?*const ClusterMembership {
        return self.ready_processor.getClusterMembership();
    }

    pub fn getMembershipIndex(self: *const Raftor) u64 {
        return self.ready_processor.getMembershipIndex();
    }

    pub fn getReadyPhase(self: *const Raftor) ?ReadyPhase {
        return self.ready_processor.phase();
    }

    pub fn processReadyStep(self: *Raftor) Error!bool {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        if (self.driverError()) |err| return err;
        const did_work = self.ready_processor.processStep() catch |err| {
            self.latchReadyError();
            return err;
        };
        if (self.ready_processor.terminalError()) |err| {
            self.enterTerminal(err);
            return err;
        }
        return did_work;
    }

    fn processReady(self: *Raftor) Error!bool {
        const did_work = self.ready_processor.process() catch |err| {
            self.latchReadyError();
            return err;
        };
        if (self.ready_processor.terminalError()) |err| {
            self.enterTerminal(err);
            return err;
        }
        return did_work;
    }

    fn latchReadyError(self: *Raftor) void {
        if (self.ready_processor.terminalError()) |err| self.enterTerminal(err);
    }

    fn enterTerminal(self: *Raftor, err: Error) void {
        var batch: ?ShutdownBatch = null;
        spinLock(&self.lifecycle_mutex);
        if (self.terminal_error == null) self.terminal_error = err;
        self.stop_requested.store(true, .release);
        if (self.lifecycle == .active) {
            self.lifecycle = .terminating;
            batch = self.detachRequestsLocked();
        }
        const stop_transport = self.markTransportStoppedLocked();
        self.lifecycle_mutex.unlock();
        const owns_shutdown = batch != null;

        if (stop_transport) self.transport.stop();
        if (batch) |*detached| self.invokeShutdownBatch(detached, err, err);

        if (owns_shutdown) {
            spinLock(&self.lifecycle_mutex);
            if (self.lifecycle == .terminating) self.lifecycle = .terminal;
            self.lifecycle_mutex.unlock();
        }
    }

    fn driverError(self: *const Raftor) ?Error {
        const mutex = @constCast(&self.lifecycle_mutex);
        spinLock(mutex);
        defer mutex.unlock();
        if (self.terminal_error) |err| return err;
        if (self.lifecycle != .active) return error.ShuttingDown;
        return null;
    }

    fn enterEventLoop(self: *Raftor) Error!void {
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.terminal_error) |err| return err;
        if (self.lifecycle != .active) return error.ShuttingDown;
        if (self.event_loop_active.cmpxchgStrong(false, true, .acquire, .monotonic) != null) {
            return error.EventLoopBusy;
        }
        self.event_loop_thread_id.store(currentThreadId(), .release);
    }

    fn leaveEventLoop(self: *Raftor) void {
        self.publishStatus();
        self.event_loop_thread_id.store(0, .release);
        self.event_loop_active.store(false, .release);
    }

    fn currentCoreStatus(self: *const Raftor) NodeStatus {
        const raft = self.raw_node.raftConst();
        return .{
            .id = raft.id,
            .role = raft.state,
            .term = raft.term,
            .leader_id = raft.leader_id,
            .commit_index = raft.raft_log.committed,
            .applied_index = self.ready_processor.applied_index,
            .incarnation = self.request_context_generator.incarnation,
        };
    }

    fn observedCoreStatus(self: *const Raftor) NodeStatus {
        if (self.event_loop_thread_id.load(.acquire) == currentThreadId()) {
            return self.currentCoreStatus();
        }
        return self.publishedStatus();
    }

    fn publishStatus(self: *Raftor) void {
        const snapshot = self.currentCoreStatus();
        spinLock(&self.status_mutex);
        self.status_snapshot = snapshot;
        self.status_mutex.unlock();
    }

    fn publishedStatus(self: *const Raftor) NodeStatus {
        const mutex = @constCast(&self.status_mutex);
        spinLock(mutex);
        defer mutex.unlock();
        return self.status_snapshot;
    }

    fn invokeShutdownBatch(self: *Raftor, batch: *ShutdownBatch, proposal_error: Error, read_error: Error) void {
        const thread_id = currentThreadId();
        if (self.shutdown_callback_thread_id.cmpxchgStrong(0, thread_id, .acq_rel, .acquire) != null) {
            @panic("concurrent Raftor shutdown callback batches");
        }
        defer self.shutdown_callback_thread_id.store(0, .release);
        batch.invoke(proposal_error, read_error);
    }

    fn assertDestroyAllowed(self: *const Raftor) void {
        const thread_id = currentThreadId();
        if (self.event_loop_thread_id.load(.acquire) == thread_id or
            self.shutdown_callback_thread_id.load(.acquire) == thread_id)
        {
            @panic("Raftor.destroy cannot be called from a Raftor callback");
        }
    }

    fn waitForQuiescence(self: *Raftor) void {
        while (true) {
            self.assertDestroyAllowed();
            const active = self.run_active.load(.acquire) or self.event_loop_active.load(.acquire);
            spinLock(&self.lifecycle_mutex);
            const transitioning = self.lifecycle == .stopping or self.lifecycle == .terminating;
            self.lifecycle_mutex.unlock();
            if (!active and !transitioning) return;
            sleepNanoseconds(std.time.ns_per_ms);
        }
    }

    fn sleepUntilNextTick(self: *const Raftor) void {
        var remaining = self.config.tick_interval_ms *| std.time.ns_per_ms;
        const max_sleep = 10 * std.time.ns_per_ms;
        while (remaining > 0 and !self.stop_requested.load(.acquire)) {
            const duration = @min(remaining, max_sleep);
            sleepNanoseconds(duration);
            remaining -= duration;
        }
    }
};

// ===========================================================================
// Bootstrap helpers
// ===========================================================================

fn openStorage(allocator: std.mem.Allocator, config: RaftorConfig) Error!StorageBackend {
    if (config.data_dir.len == 0) return .{ .memory = MemoryStorage.init() };

    const wal_dir = try allocator.allocSentinel(u8, config.data_dir.len, 0);
    defer allocator.free(wal_dir);
    @memcpy(wal_dir[0..config.data_dir.len], config.data_dir);
    return .{ .wal = try WALStorage.openWithFs(
        allocator,
        wal_dir,
        config.file_system orelse fs_mod.realFileSystem(),
    ) };
}

fn detectStartupMode(
    allocator: std.mem.Allocator,
    storage: storage_mod.WritableStorage,
    config: RaftorConfig,
) Error!StartupMode {
    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    if (!state.hard_state.isEmpty() or !confStateIsEmpty(state.conf_state) or try storage.lastIndex() != 0) return .restart;
    return if (config.join) .join else .bootstrap;
}

fn confStateIsEmpty(conf_state: ConfState) bool {
    return conf_state.voters.len == 0 and
        conf_state.learners.len == 0 and
        conf_state.voters_outgoing.len == 0 and
        conf_state.learners_next.len == 0;
}

fn buildVoterIds(allocator: std.mem.Allocator, config: RaftorConfig) ![]u64 {
    var ids = try allocator.alloc(u64, config.initial_peers.len);
    for (config.initial_peers, 0..) |peer, i| ids[i] = peer.id;
    return ids;
}

const InitialMembership = struct {
    voters: []u64,
    membership: ClusterMembership,

    fn deinit(self: *InitialMembership, allocator: std.mem.Allocator) void {
        allocator.free(self.voters);
        self.membership.deinit(allocator);
    }
};

fn buildInitialMembership(
    allocator: std.mem.Allocator,
    config: RaftorConfig,
    join: bool,
) Error!InitialMembership {
    const cluster_id = config.cluster_id orelse return error.ClusterIdRequired;
    if (std.mem.eql(u8, &cluster_id, &(@as(ClusterId, .{0} ** 16)))) return error.ClusterIdRequired;

    const peer_count = if (config.initial_peers.len == 0) @as(usize, 1) else config.initial_peers.len;
    if (join and config.initial_peers.len == 0) return error.InvalidConfig;
    var peers = try allocator.alloc(PeerEndpoint, peer_count);
    var initialized: usize = 0;
    errdefer {
        for (peers[0..initialized]) |*peer| peer.deinit(allocator);
        allocator.free(peers);
    }

    if (config.initial_peers.len == 0) {
        const address = localAddress(config);
        if (address.len == 0) return error.PeerAddressMissing;
        peers[0] = try PeerEndpoint.init(allocator, config.nodeId(), address);
        initialized = 1;
    } else {
        for (config.initial_peers) |peer| {
            if (peer.id == 0) return error.InvalidNodeId;
            const address = peer.context orelse return error.PeerAddressMissing;
            if (address.len == 0) return error.PeerAddressMissing;
            peers[initialized] = try PeerEndpoint.init(allocator, peer.id, address);
            initialized += 1;
        }
    }
    std.mem.sort(PeerEndpoint, peers, {}, struct {
        fn lessThan(_: void, lhs: PeerEndpoint, rhs: PeerEndpoint) bool {
            return lhs.node_id < rhs.node_id;
        }
    }.lessThan);

    var local_present = false;
    for (peers, 0..) |peer, index| {
        if (index > 0 and peers[index - 1].node_id == peer.node_id) return error.DuplicatePeerId;
        if (peer.node_id == config.nodeId()) local_present = true;
    }
    if ((!join and !local_present) or (join and local_present)) return error.NodeIdNotInInitialPeers;
    if (join and localAddress(config).len == 0) return error.PeerAddressMissing;

    const voters = try allocator.alloc(u64, peers.len);
    errdefer allocator.free(voters);
    for (peers, 0..) |peer, index| voters[index] = peer.node_id;
    return .{
        .voters = voters,
        .membership = .{ .cluster_id = cluster_id, .peers = peers },
    };
}

fn buildLegacyMembership(
    allocator: std.mem.Allocator,
    cluster_id: ClusterId,
    source: LegacySnapshotMembership,
) Error!ClusterMembership {
    if (std.mem.eql(u8, &cluster_id, &(@as(ClusterId, .{0} ** 16)))) return error.ClusterIdRequired;
    var peers = try allocator.alloc(PeerEndpoint, source.peers.len);
    var initialized: usize = 0;
    errdefer {
        for (peers[0..initialized]) |*peer| peer.deinit(allocator);
        if (peers.len != 0) allocator.free(peers);
    }
    for (source.peers) |peer| {
        if (peer.id == 0) return error.InvalidNodeId;
        const address = peer.context orelse return error.PeerAddressMissing;
        if (address.len == 0) return error.PeerAddressMissing;
        peers[initialized] = try PeerEndpoint.init(allocator, peer.id, address);
        initialized += 1;
    }
    std.mem.sort(PeerEndpoint, peers, {}, struct {
        fn lessThan(_: void, lhs: PeerEndpoint, rhs: PeerEndpoint) bool {
            return lhs.node_id < rhs.node_id;
        }
    }.lessThan);
    for (peers, 0..) |peer, index| {
        if (index != 0 and peer.node_id == peers[index - 1].node_id) return error.DuplicatePeerId;
    }

    const retired = if (source.retired_node_ids.len == 0)
        @as([]u64, &.{})
    else
        try allocator.dupe(u64, source.retired_node_ids);
    errdefer if (retired.len != 0) allocator.free(retired);
    std.mem.sort(u64, retired, {}, std.sort.asc(u64));
    for (retired, 0..) |id, index| {
        if (id == 0 or (index != 0 and retired[index - 1] == id)) return error.InvalidClusterMembership;
    }

    return .{
        .cluster_id = cluster_id,
        .peers = peers,
        .retired_node_ids = retired,
    };
}

fn localAddress(config: RaftorConfig) []const u8 {
    return if (config.advertise_addr.len != 0) config.advertise_addr else config.listen_addr;
}

fn containsSorted(ids: []const u64, id: u64) bool {
    var low: usize = 0;
    var high = ids.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (ids[mid] < id) {
            low = mid + 1;
        } else if (ids[mid] > id) {
            high = mid;
        } else {
            return true;
        }
    }
    return false;
}

fn rejectRetiredId(membership: ClusterMembership, id: u64) Error!void {
    if (containsSorted(membership.retired_node_ids, id)) return error.NodeRetired;
}

// ===========================================================================
// Tests
// ===========================================================================

const MockStateMachine = state_machine_mod.MockStateMachine;

fn makeRaftorConfig(id: u64) RaftorConfig {
    var rc = RaftorConfig{};
    rc.raft.id = id;
    rc.raft.election_tick = 10;
    rc.raft.heartbeat_tick = 1;
    rc.raft.election_timeout_seed = id * 999;
    return rc;
}

test "raftor: single-node campaign and propose" {
    const allocator = std.testing.allocator;
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const config = makeRaftorConfig(1);
    var r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    try r.campaign();
    try std.testing.expect(r.isLeader());

    const Tester = struct {
        applied: bool = false,
        fn cb(ctx: *anyopaque, result: proposal_tracker_mod.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .ok) self.applied = true;
        }
    };
    var tester = Tester{};
    try r.propose("hello", .{ .ctx = &tester, .function = Tester.cb });

    var i: usize = 0;
    while (i < 10) : (i += 1) _ = try r.tick();

    try std.testing.expect(tester.applied);
    try std.testing.expectEqual(@as(usize, 2), sm.applied.items.len);
    try std.testing.expectEqualStrings("hello", sm.applied.items[1]);
}

test "raftor: getStatus returns correct fields" {
    const allocator = std.testing.allocator;
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.create(allocator, makeRaftorConfig(1), sm.stateMachine());
    defer r.destroy();

    const status = r.getStatus();
    try std.testing.expectEqual(@as(u64, 1), status.id);
    try std.testing.expectEqual(@as(u64, 1), status.incarnation);
    try std.testing.expectEqual(StateRole.follower, status.role);
    try std.testing.expectEqual(@as(usize, 0), status.pending_proposals);
}

test "raftor: request context sequence exhaustion does not enqueue" {
    const allocator = std.testing.allocator;
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.create(allocator, makeRaftorConfig(1), sm.stateMachine());
    defer r.destroy();
    r.request_context_generator.sequence.store(std.math.maxInt(u64), .monotonic);

    const Callback = struct {
        fn proposal(_: *anyopaque, _: proposal_tracker_mod.ProposalResult) void {}
        fn read(_: *anyopaque, _: proposal_tracker_mod.ReadIndexResult) void {}
    };
    try std.testing.expectError(
        error.ContextSequenceExhausted,
        r.propose("data", .{ .ctx = undefined, .function = Callback.proposal }),
    );
    try std.testing.expectError(
        error.ContextSequenceExhausted,
        r.readIndex("read", .{ .ctx = undefined, .function = Callback.read }),
    );
    try std.testing.expect(r.proposal_queue.empty());
    try std.testing.expect(r.read_index_queue.empty());
}

test "raftor: stop terminates tracked requests" {
    const allocator = std.testing.allocator;
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.create(allocator, makeRaftorConfig(1), sm.stateMachine());
    defer r.destroy();

    const Callback = struct {
        proposal_error: ?Error = null,
        read_error: ?Error = null,

        fn proposal(ctx: *anyopaque, result: proposal_tracker_mod.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .err) self.proposal_error = result.err;
        }
        fn read(ctx: *anyopaque, result: proposal_tracker_mod.ReadIndexResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .err) self.read_error = result.err;
        }
    };
    var callback = Callback{};
    try r.proposal_tracker.track("proposal", .{ .ctx = &callback, .function = Callback.proposal }, 0, 0);
    try r.proposal_tracker.trackRead("read", .{ .ctx = &callback, .function = Callback.read }, 0, 0);
    r.stop();
    try std.testing.expectEqual(error.ShuttingDown, callback.proposal_error.?);
    try std.testing.expectEqual(error.ShuttingDown, callback.read_error.?);
}
