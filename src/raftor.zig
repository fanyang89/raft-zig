//! Top-level Raftor orchestration: ties RawNode, ReadyProcessor, Transport,
//! and StateMachine into a complete single-node Raft server.
//!
//! Ports `include/raftpp/raftor/raftor.h` and `lib/raftor/raftor.cc` with
//! major simplifications:
//!   * Uses MemoryStorage instead of WAL (durable storage arrives later).
//!   * Uses NoopTransport instead of CapnpTransport (RPC arrives later).
//!   * Single-threaded: no mutexes, no cross-thread proposal queues.
//!   * No telemetry / OpenTelemetry.
//!
//! The event loop is driven by `tick()` (non-blocking) or `run()` (blocking).

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");
const memory_storage_mod = @import("memory_storage.zig");
const raft_config_mod = @import("raft_config.zig");
const raft_mod = @import("raft.zig");
const raw_node_mod = @import("raw_node.zig");
const state_machine_mod = @import("state_machine.zig");
const transport_mod = @import("transport.zig");
const proposal_tracker_mod = @import("proposal_tracker.zig");
const ready_processor_mod = @import("ready_processor.zig");
const raftor_config_mod = @import("raftor_config.zig");
const state_role_mod = @import("core/state_role.zig");

const Error = error_model.Error;
const Entry = types.Entry;
const Message = types.Message;
const ConfState = types.ConfState;
const ConfChangeV2 = types.ConfChangeV2;
const Snapshot = types.Snapshot;

const Config = raft_config_mod.Config;
const Raft = raft_mod.Raft;
const RawNode = raw_node_mod.RawNode;
const MemoryStorage = memory_storage_mod.MemoryStorage;
const WritableStorage = storage_mod.WritableStorage;
const StateMachine = state_machine_mod.StateMachine;
const Transport = transport_mod.Transport;
const NoopTransport = transport_mod.NoopTransport;
const ProposalTracker = proposal_tracker_mod.ProposalTracker;
const ReadyProcessor = ready_processor_mod.ReadyProcessor;
const RaftorConfig = raftor_config_mod.RaftorConfig;
const StateRole = state_role_mod.StateRole;
const Peer = raw_node_mod.Peer;

const log = std.log.scoped(.raft_zig_raftor);

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
    storage: MemoryStorage,
    transport_impl: NoopTransport,
    raw_node: RawNode,
    proposal_tracker: ProposalTracker,
    ready_processor: ReadyProcessor,

    // Pending proposals submitted via `propose()` but not yet fed to RawNode.
    pending_proposals: std.ArrayList(PendingProposal),

    // Pending reads submitted via `readIndex()` but not yet fed to RawNode.
    pending_reads: std.ArrayList(PendingRead),

    tick_count: u64 = 0,
    running: bool = false,

    const PendingProposal = struct {
        data: []u8,
        ctx: []u8,
        callback: proposal_tracker_mod.ProposalCallback,
    };

    const PendingRead = struct {
        ctx: []u8,
        callback: proposal_tracker_mod.ReadIndexCallback,
    };

    /// Create a Raftor on the heap. The caller owns the returned pointer and
    /// must call `destroy` to free it.
    pub fn create(
        allocator: std.mem.Allocator,
        config: RaftorConfig,
        state_machine: StateMachine,
    ) Error!*Raftor {
        const self = try allocator.create(Raftor);
        errdefer allocator.destroy(self);

        try self.initInternal(allocator, config, state_machine);
        return self;
    }

    pub fn destroy(self: *Raftor) void {
        const allocator = self.allocator;
        self.deinitInternal();
        allocator.destroy(self);
    }

    fn initInternal(self: *Raftor, allocator: std.mem.Allocator, config: RaftorConfig, state_machine: StateMachine) Error!void {
        self.allocator = allocator;
        self.config = config;
        self.storage = MemoryStorage.init();
        self.transport_impl = NoopTransport.init(allocator);
        self.proposal_tracker = ProposalTracker.init(allocator);
        self.pending_proposals = .empty;
        self.pending_reads = .empty;
        self.tick_count = 0;
        self.running = false;

        // Bootstrap: seed storage with the initial ConfState.
        const voters = if (config.initial_peers.len > 0)
            buildVoterIds(allocator, config) catch return error.OutOfMemory
        else
            allocator.dupe(u64, &.{config.nodeId()}) catch return error.OutOfMemory;
        defer allocator.free(voters);
        // setRaftState clones the ConfState internally; no need to deinit cs.
        const cs = ConfState{ .voters = voters };
        try self.storage.setRaftState(allocator, .{ .conf_state = cs });

        // Build RawNode AFTER storage is at its final address.
        self.raw_node = try RawNode.init(allocator, config.raft, self.storage.asStorage());

        // Build ReadyProcessor AFTER raw_node is at its final address.
        self.ready_processor = ReadyProcessor.init(
            allocator,
            &self.raw_node,
            self.storage.asWritableStorage(),
            state_machine,
            self.transport_impl.transport(),
            &self.proposal_tracker,
            config.nodeId(),
            config.checksum_enabled,
            config.raft.applied,
        );
    }

    fn deinitInternal(self: *Raftor) void {
        for (self.pending_proposals.items) |*p| {
            self.allocator.free(p.data);
            self.allocator.free(p.ctx);
        }
        self.pending_proposals.deinit(self.allocator);
        for (self.pending_reads.items) |*r| self.allocator.free(r.ctx);
        self.pending_reads.deinit(self.allocator);
        self.proposal_tracker.deinit();
        self.raw_node.deinit();
        self.transport_impl.deinit();
        self.storage.deinit(self.allocator);
    }

    // -----------------------------------------------------------------------
    // Event loop
    // -----------------------------------------------------------------------

    /// Advance the event loop by one tick. Returns true if there was work.
    pub fn tick(self: *Raftor) Error!bool {
        self.tick_count += 1;

        // Expire timed-out proposals and reads.
        self.proposal_tracker.expireTimeouts(self.tick_count);

        // Drain pending proposals into RawNode.
        var had_work = false;
        while (self.pending_proposals.items.len > 0) {
            const p = self.pending_proposals.swapRemove(self.pending_proposals.items.len - 1);
            defer {
                self.allocator.free(p.data);
                self.allocator.free(p.ctx);
            }
            self.proposal_tracker.track(p.ctx, p.callback, self.tick_count, 0) catch {};
            self.raw_node.propose(p.ctx, p.data) catch |e| {
                if (e == error.ProposalDropped) {
                    self.proposal_tracker.fail(p.ctx, error.ProposalDropped);
                } else {
                    return e;
                }
            };
            had_work = true;
        }

        // Drain pending reads into RawNode.
        while (self.pending_reads.items.len > 0) {
            const r = self.pending_reads.swapRemove(self.pending_reads.items.len - 1);
            defer self.allocator.free(r.ctx);
            self.proposal_tracker.trackRead(r.ctx, r.callback, self.tick_count, 0) catch {};
            self.raw_node.readIndex(r.ctx) catch {};
            had_work = true;
        }

        // Tick the raft FSM.
        _ = try self.raw_node.tick();
        had_work = true;

        // Process ready cycles until none remain.
        while (try self.ready_processor.process()) {
            had_work = true;
        }

        return had_work;
    }

    /// Run the event loop until `stop()` is called. Blocks the caller.
    pub fn run(self: *Raftor) Error!void {
        self.running = true;
        while (self.running) {
            _ = try self.tick();
            // In a real implementation, we'd sleep for tick_interval_ms here.
            // For now, yield to avoid busy-looping in tests.
            std.Thread.yield() catch {};
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
        const data_copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(data_copy);
        // Generate a unique context from the tick count + pending count.
        var ctx_buf: [32]u8 = undefined;
        const ctx = try std.fmt.bufPrint(&ctx_buf, "p{}-{}", .{ self.tick_count, self.pending_proposals.items.len });
        const ctx_copy = try self.allocator.dupe(u8, ctx);
        errdefer self.allocator.free(ctx_copy);
        try self.pending_proposals.append(self.allocator, .{
            .data = data_copy,
            .ctx = ctx_copy,
            .callback = callback,
        });
    }

    pub fn readIndex(self: *Raftor, ctx: []const u8, callback: proposal_tracker_mod.ReadIndexCallback) !void {
        const ctx_copy = try self.allocator.dupe(u8, ctx);
        errdefer self.allocator.free(ctx_copy);
        try self.pending_reads.append(self.allocator, .{
            .ctx = ctx_copy,
            .callback = callback,
        });
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
        self.transport_impl.transport().addPeer(id, addr);
        var cc = ConfChangeV2{ .changes = try self.allocator.alloc(types.ConfChangeSingle, 1) };
        defer self.allocator.free(cc.changes);
        cc.changes[0] = .{ .change_type = .add_node, .node_id = id };
        // Use addr as context for transport discovery.
        cc.context = try self.allocator.dupe(u8, addr);
        defer self.allocator.free(cc.context);
        try self.raw_node.proposeConfChange(addr, cc);
    }

    pub fn removeNode(self: *Raftor, id: u64) Error!void {
        self.transport_impl.transport().removePeer(id);
        var cc = ConfChangeV2{ .changes = try self.allocator.alloc(types.ConfChangeSingle, 1) };
        defer self.allocator.free(cc.changes);
        cc.changes[0] = .{ .change_type = .remove_node, .node_id = id };
        try self.raw_node.proposeConfChange("", cc);
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

    pub fn getTransport(self: *Raftor) *NoopTransport {
        return &self.transport_impl;
    }
};

// ===========================================================================
// Bootstrap helpers
// ===========================================================================

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

    // Campaign to become leader.
    try r.campaign();
    try std.testing.expect(r.isLeader());

    // Propose data.
    const Tester = struct {
        applied: bool = false,
        fn cb(ctx: *anyopaque, result: proposal_tracker_mod.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .ok) self.applied = true;
        }
    };
    var tester = Tester{};
    try r.propose("hello", .{ .ctx = &tester, .function = Tester.cb });

    // Tick a few times to process the proposal through the Ready pipeline.
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = try r.tick();
    }
    try std.testing.expect(tester.applied);
    // The noop entry (from becomeLeader) and the proposed entry are both applied.
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
