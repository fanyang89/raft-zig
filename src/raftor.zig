//! Top-level Raftor orchestration: ties RawNode, ReadyProcessor, Transport,
//! and StateMachine into a complete Raft server.
//!
//! Ports `include/raftpp/raftor/raftor.h` and `lib/raftor/raftor.cc` with
//! major simplifications:
//!   * Uses MemoryStorage instead of WAL (durable storage arrives later).
//!   * Accepts any Transport implementation (NoopTransport for single-node,
//!     LoopbackTransport for multi-node testing, future TCP for production).
//!   * Single-threaded: no mutexes, no cross-thread proposal queues.
//!   * No telemetry / OpenTelemetry.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");
const memory_storage_mod = @import("memory_storage.zig");
const wal_mod = @import("wal.zig");
const raft_config_mod = @import("raft_config.zig");
const raft_mod = @import("raft.zig");
const raw_node_mod = @import("raw_node.zig");
const state_machine_mod = @import("state_machine.zig");
const transport_mod = @import("transport.zig");
const proposal_tracker_mod = @import("proposal_tracker.zig");
const proposal_queue_mod = @import("proposal_queue.zig");
const ready_processor_mod = @import("ready_processor.zig");
const raftor_config_mod = @import("raftor_config.zig");
const state_role_mod = @import("core/state_role.zig");

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
const ProposalTracker = proposal_tracker_mod.ProposalTracker;
const ProposalQueue = proposal_queue_mod.ProposalQueue;
const ReadIndexQueue = proposal_queue_mod.ReadIndexQueue;
const ReadyProcessor = ready_processor_mod.ReadyProcessor;
const ReadyPhase = ready_processor_mod.ReadyPhase;
const RaftorConfig = raftor_config_mod.RaftorConfig;
const StateRole = state_role_mod.StateRole;
const Peer = raw_node_mod.Peer;

const log = std.log.scoped(.raft_zig_raftor);

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
};

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

    tick_count: u64 = 0,
    proposal_sequence: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    running: bool = false,
    terminal_error: ?Error = null,

    // Snapshot triggering state.
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
        const startup_mode = try detectStartupMode(allocator, storage);
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
        const startup_mode = try detectStartupMode(allocator, storage);
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

    pub fn destroy(self: *Raftor) void {
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

        try config.raft.validate();
        try self.prepareStorage(startup_mode);

        self.proposal_tracker = ProposalTracker.init(allocator);
        self.proposal_queue = ProposalQueue.init(allocator);
        self.read_index_queue = ReadIndexQueue.init(allocator);
        errdefer {
            self.proposal_queue.deinit();
            self.read_index_queue.deinit();
            self.proposal_tracker.deinit();
        }
        self.tick_count = 0;
        self.proposal_sequence = std.atomic.Value(u64).init(0);
        self.running = false;
        self.terminal_error = null;
        self.last_snapshot_attempt_index = 0;
        self.last_snapshot_attempt_tick = 0;
        self.last_snapshot_tick = 0;

        // Build RawNode AFTER storage is at its final address.
        var raft_config = config.raft;
        raft_config.load_state_on_startup = startup_mode == .restart;
        self.raw_node = try RawNode.init(allocator, raft_config, self.storage.asStorage());
        errdefer self.raw_node.deinit();

        // Register inbound message callback: transport → raw_node.step().
        self.transport.setMessageCallback(.{
            .ctx = self,
            .function = onMessage,
        });

        // Build ReadyProcessor AFTER raw_node is at its final address.
        self.ready_processor = ReadyProcessor.init(
            allocator,
            &self.raw_node,
            self.storage,
            dependencies.state_machine,
            self.transport,
            &self.proposal_tracker,
            config.nodeId(),
            config.checksum_enabled,
            config.raft.applied,
        );
    }

    fn prepareStorage(self: *Raftor, startup_mode: StartupMode) Error!void {
        var state = try self.storage.initialState(self.allocator);
        defer state.deinit(self.allocator);

        switch (startup_mode) {
            .bootstrap => {
                if (!state.hard_state.isEmpty() or !confStateIsEmpty(state.conf_state) or try self.storage.lastIndex() != 0) {
                    return error.IncompatibleStorage;
                }
                const voters = if (self.config.initial_peers.len > 0)
                    try buildVoterIds(self.allocator, self.config)
                else
                    try self.allocator.dupe(u64, &.{self.config.nodeId()});
                defer self.allocator.free(voters);
                try self.storage.setConfState(self.allocator, .{ .voters = voters });
                try self.storage.sync();
            },
            .restart => {
                if (confStateIsEmpty(state.conf_state)) return error.IncompatibleStorage;
            },
        }
    }

    fn deinitInternal(self: *Raftor) void {
        self.transport.setMessageCallback(null);
        self.proposal_queue.deinit();
        self.read_index_queue.deinit();
        self.proposal_tracker.deinit();
        self.ready_processor.deinit();
        self.raw_node.deinit();
        if (self.noop_transport) |*nt| nt.deinit();
        if (self.owned_storage) |*storage| storage.deinit(self.allocator);
    }

    /// Inbound message callback. Transfers message ownership to `step()`.
    fn onMessage(ctx: *anyopaque, msg: Message) Error!void {
        const self: *Raftor = @ptrCast(@alignCast(ctx));
        if (self.terminal_error) |err| {
            var owned = msg;
            owned.deinit(self.allocator);
            return err;
        }
        self.raw_node.step(msg) catch |err| switch (err) {
            error.StepLocalMsg, error.StepPeerNotFound => {},
            else => return err,
        };
    }

    // -----------------------------------------------------------------------
    // Event loop
    // -----------------------------------------------------------------------

    /// Advance the event loop by one tick. Returns true if there was work.
    pub fn tick(self: *Raftor) Error!bool {
        if (self.terminal_error) |e| return e;
        if (self.ready_processor.phase() != null) {
            _ = try self.ready_processor.process();
            return true;
        }
        self.tick_count += 1;

        self.proposal_tracker.expireTimeouts(self.tick_count);

        // Drain pending proposals from the thread-safe queue.
        var had_work = false;
        const proposal_timeout = if (self.config.proposal_timeout_ticks > 0) self.config.proposal_timeout_ticks else 0;
        while (self.proposal_queue.tryPop()) |p| {
            defer {
                self.allocator.free(p.data);
                self.allocator.free(p.ctx);
            }
            self.proposal_tracker.track(p.ctx, p.callback, self.tick_count, proposal_timeout) catch |err| {
                p.callback.invoke(.{ .err = err });
                continue;
            };
            self.raw_node.propose(p.ctx, p.data) catch |e| {
                self.proposal_tracker.fail(p.ctx, e);
                if (e != error.ProposalDropped) return e;
            };
            had_work = true;
        }

        // Drain pending reads.
        const read_timeout = if (self.config.read_index_timeout_ticks > 0) self.config.read_index_timeout_ticks else 0;
        while (self.read_index_queue.tryPop()) |r| {
            defer self.allocator.free(r.ctx);
            self.proposal_tracker.trackRead(r.ctx, r.callback, self.tick_count, read_timeout) catch |err| {
                r.callback.invoke(.{ .err = err });
                continue;
            };
            self.raw_node.readIndex(r.ctx) catch |err| {
                self.proposal_tracker.failRead(r.ctx, err);
                return err;
            };
            had_work = true;
        }

        _ = try self.raw_node.tick();
        had_work = true;

        while (try self.ready_processor.process()) {
            had_work = true;
        }

        // Drain inbound transport messages (triggers onMessage → step()).
        if (try self.transport.pollOne()) had_work = true;

        // Process any additional readys generated by inbound messages.
        while (try self.ready_processor.process()) {
            had_work = true;
        }

        self.maybeAutoSnapshot() catch |e| switch (e) {
            error.SnapshotOutOfDate => log.debug("snapshot attempt skipped: {s}", .{@errorName(e)}),
            else => log.warn("snapshot attempt failed: {s}", .{@errorName(e)}),
        };

        return had_work;
    }

    /// Run the event loop until `stop()` is called. Blocks the caller.
    pub fn run(self: *Raftor) Error!void {
        self.running = true;
        while (self.running) {
            if (self.terminal_error) |e| return e;
            _ = try self.tick();
            // Sleep for the configured tick interval to avoid busy-looping.
            const ns: u64 = self.config.tick_interval_ms * std.time.ns_per_ms;
            std.Thread.sleep(ns);
        }
    }

    pub fn stop(self: *Raftor) void {
        self.running = false;
    }

    pub fn isRunning(self: *const Raftor) bool {
        return self.running;
    }

    // -----------------------------------------------------------------------
    // Proposals
    // -----------------------------------------------------------------------

    pub fn propose(self: *Raftor, data: []const u8, callback: proposal_tracker_mod.ProposalCallback) !void {
        if (self.terminal_error != null) return error.ShuttingDown;
        const data_copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(data_copy);
        var ctx_buf: [32]u8 = undefined;
        const sequence = self.proposal_sequence.fetchAdd(1, .monotonic);
        const ctx = try std.fmt.bufPrint(&ctx_buf, "p{}", .{sequence});
        const ctx_copy = try self.allocator.dupe(u8, ctx);
        errdefer self.allocator.free(ctx_copy);
        // Push to thread-safe queue; tick() will drain it.
        try self.proposal_queue.push(data_copy, ctx_copy, callback);
    }

    pub fn readIndex(self: *Raftor, ctx: []const u8, callback: proposal_tracker_mod.ReadIndexCallback) !void {
        if (self.terminal_error != null) return error.ShuttingDown;
        const ctx_copy = try self.allocator.dupe(u8, ctx);
        errdefer self.allocator.free(ctx_copy);
        try self.read_index_queue.push(ctx_copy, callback);
    }

    // -----------------------------------------------------------------------
    // Cluster management
    // -----------------------------------------------------------------------

    pub fn campaign(self: *Raftor) Error!void {
        try self.raw_node.campaign();
        while (try self.ready_processor.process()) {}
    }

    pub fn transferLeader(self: *Raftor, target_id: u64) Error!void {
        try self.raw_node.transferLeader(target_id);
    }

    pub fn addNode(self: *Raftor, id: u64, addr: []const u8) Error!void {
        const inserted = try self.transport.addPeer(id, addr);
        errdefer if (inserted) self.transport.removePeer(id) catch {};
        var cc = ConfChangeV2{ .changes = try self.allocator.alloc(types.ConfChangeSingle, 1) };
        defer self.allocator.free(cc.changes);
        cc.changes[0] = .{ .change_type = .add_node, .node_id = id };
        cc.context = try self.allocator.dupe(u8, addr);
        defer self.allocator.free(cc.context);
        try self.raw_node.proposeConfChange(addr, cc);
    }

    pub fn removeNode(self: *Raftor, id: u64) Error!void {
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

        log.info("node {} taking snapshot at index {} term {}", .{ self.raw_node.raftConst().id, applied_index, applied_term });
        try self.storage.applyLocalSnapshot(self.allocator, snap);

        self.last_snapshot_tick = self.tick_count;
        self.last_snapshot_attempt_index = applied_index;
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
        const snapshot_index = self.last_snapshot_attempt_index;
        if (entries_threshold > 0 and applied_index > snapshot_index and
            (applied_index - snapshot_index) >= entries_threshold)
        {
            try self.takeSnapshot();
            return;
        }

        // Condition 2: time interval.
        if (interval_ticks > 0 and
            (self.tick_count - self.last_snapshot_tick) >= interval_ticks)
        {
            try self.takeSnapshot();
            return;
        }

        // Record the attempt so rate limiting works even if no condition fired.
        self.last_snapshot_attempt_index = applied_index;
        self.last_snapshot_attempt_tick = self.tick_count;
    }

    // -----------------------------------------------------------------------
    // Status
    // -----------------------------------------------------------------------

    pub fn getStatus(self: *const Raftor) NodeStatus {
        const r = self.raw_node.raftConst();
        return .{
            .id = r.id,
            .role = r.state,
            .term = r.term,
            .leader_id = r.leader_id,
            .commit_index = r.raft_log.committed,
            .applied_index = self.ready_processor.applied_index,
            .pending_proposals = self.proposal_tracker.pendingCount(),
        };
    }

    pub fn isLeader(self: *const Raftor) bool {
        return self.raw_node.raftConst().state == .leader;
    }

    pub fn getLeaderId(self: *const Raftor) u64 {
        return self.raw_node.raftConst().leader_id;
    }

    pub fn getRawNode(self: *Raftor) *RawNode {
        return &self.raw_node;
    }

    pub fn getReadyPhase(self: *const Raftor) ?ReadyPhase {
        return self.ready_processor.phase();
    }

    pub fn processReadyStep(self: *Raftor) Error!bool {
        return self.ready_processor.processStep();
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
    return .{ .wal = try WALStorage.open(allocator, wal_dir) };
}

fn detectStartupMode(allocator: std.mem.Allocator, storage: storage_mod.WritableStorage) Error!StartupMode {
    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    if (!state.hard_state.isEmpty() or !confStateIsEmpty(state.conf_state) or try storage.lastIndex() != 0) return .restart;
    return .bootstrap;
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
    try std.testing.expectEqual(StateRole.follower, status.role);
    try std.testing.expectEqual(@as(usize, 0), status.pending_proposals);
}
