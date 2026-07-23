//! Ready processing pipeline: drives RawNode → Ready → persist → apply → advance.
//!
//! Ports `lib/raftor/ready_processor.{h,cc}`. Each `process()` call pulls one
//! Ready from the RawNode, runs it through an 8-step pipeline, and returns
//! whether there was work to do:
//!
//!   1. Validate entries (optional CRC32C checksum)
//!   2. Persist unstable entries to storage
//!   3. Persist HardState (if changed)
//!   4. Apply snapshot (if present)
//!   5. Send outbound messages via transport
//!   6. Apply committed entries to StateMachine
//!   7. Enqueue read-index states
//!   8. Advance RawNode + process light ready (more committed entries + messages)

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const util = @import("core/util.zig");
const storage_mod = @import("storage.zig");
const raw_node_mod = @import("raw_node.zig");
const state_machine_mod = @import("state_machine.zig");
const transport_mod = @import("transport.zig");
const proposal_tracker_mod = @import("proposal_tracker.zig");
const state_role_mod = @import("core/state_role.zig");

const Error = error_model.Error;
const Entry = types.Entry;
const EntryType = types.EntryType;
const Message = types.Message;
const ConfChangeV2 = types.ConfChangeV2;
const Snapshot = types.Snapshot;
const SnapshotMetadata = types.SnapshotMetadata;
const ReadState = @import("read_only.zig").ReadState;

const RawNode = raw_node_mod.RawNode;
const Ready = raw_node_mod.Ready;
const LightReady = raw_node_mod.LightReady;
const WritableStorage = storage_mod.WritableStorage;
const StateMachine = state_machine_mod.StateMachine;
const Transport = transport_mod.Transport;
const ProposalTracker = proposal_tracker_mod.ProposalTracker;
const StateRole = state_role_mod.StateRole;

const log = std.log.scoped(.raft_zig_ready_processor);

pub const ReadyPhase = enum {
    validate,
    persist_entries,
    persist_hard_state,
    restore_snapshot,
    persist_snapshot,
    sync,
    send_messages,
    apply_committed,
    complete_reads,
    advance,
    persist_advanced_hard_state,
    sync_advanced_hard_state,
    send_advanced_messages,
    apply_advanced_committed,
    advance_apply,
};

const PendingReady = struct {
    ready: Ready,
    light_ready: LightReady = .{},
    phase: ReadyPhase = .validate,

    fn deinit(self: *PendingReady, allocator: std.mem.Allocator) void {
        self.light_ready.deinit(allocator);
        self.ready.deinit(allocator);
    }
};

pub const ReadyProcessor = struct {
    raw_node: *RawNode,
    storage: WritableStorage,
    state_machine: StateMachine,
    transport: Transport,
    proposal_tracker: *ProposalTracker,
    node_id: u64,
    applied_index: u64,
    prev_role: StateRole,
    prev_leader: u64,
    prev_term: u64,
    fatal_error: ?Error,
    fatal_after_ready: ?Error,
    pending: ?PendingReady,
    checksum_enabled: bool,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        raw_node: *RawNode,
        storage: WritableStorage,
        state_machine: StateMachine,
        transport: Transport,
        proposal_tracker: *ProposalTracker,
        node_id: u64,
        checksum_enabled: bool,
        initial_applied_index: u64,
    ) ReadyProcessor {
        const ss = raw_node.raftConst().softState();
        return .{
            .raw_node = raw_node,
            .storage = storage,
            .state_machine = state_machine,
            .transport = transport,
            .proposal_tracker = proposal_tracker,
            .node_id = node_id,
            .applied_index = initial_applied_index,
            .prev_role = ss.role,
            .prev_leader = ss.leader_id,
            .prev_term = raw_node.raftConst().term,
            .fatal_error = null,
            .fatal_after_ready = null,
            .pending = null,
            .checksum_enabled = checksum_enabled,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ReadyProcessor) void {
        if (self.pending) |*pending| pending.deinit(self.allocator);
        self.pending = null;
    }

    pub fn isLeader(self: ReadyProcessor) bool {
        return self.prev_role == .leader;
    }

    pub fn getLeaderId(self: ReadyProcessor) u64 {
        return self.prev_leader;
    }

    pub fn getAppliedIndex(self: ReadyProcessor) u64 {
        return self.applied_index;
    }

    pub fn phase(self: ReadyProcessor) ?ReadyPhase {
        return if (self.pending) |pending| pending.phase else null;
    }

    pub fn terminalError(self: ReadyProcessor) ?Error {
        return self.fatal_error;
    }

    /// Process one Ready cycle. Returns true if there was work to do.
    pub fn process(self: *ReadyProcessor) Error!bool {
        if (self.fatal_error) |e| return e;
        if (!try self.processStep()) return false;
        while (self.pending != null) _ = try self.processStep();
        return true;
    }

    /// Advance one phase of the current Ready cycle.
    pub fn processStep(self: *ReadyProcessor) Error!bool {
        if (self.fatal_error) |e| return e;
        if (self.pending == null) {
            if (!self.raw_node.*.hasReady()) return false;
            const ready = try self.raw_node.*.getReady();
            self.checkLeadershipChange(ready);
            self.pending = .{ .ready = ready };
            return true;
        }

        const pending = &self.pending.?;
        switch (pending.phase) {
            .validate => {
                if (self.checksum_enabled) try self.validateEntries(pending.ready.entries);
                pending.phase = .persist_entries;
            },
            .persist_entries => {
                if (pending.ready.entries.len > 0) {
                    try self.storage.append(self.allocator, pending.ready.entries);
                }
                pending.phase = .persist_hard_state;
            },
            .persist_hard_state => {
                if (pending.ready.hs) |hs| {
                    try self.storage.setHardState(hs);
                }
                pending.phase = .restore_snapshot;
            },
            .restore_snapshot => {
                if (pending.ready.snapshot) |snapshot| {
                    if (snapshot.metadata.index > 0) try self.restoreSnapshot(snapshot);
                }
                pending.phase = .persist_snapshot;
            },
            .persist_snapshot => {
                if (pending.ready.snapshot) |snapshot| {
                    if (snapshot.metadata.index > 0) try self.persistSnapshot(snapshot);
                }
                pending.phase = .sync;
            },
            .sync => {
                if (pending.ready.must_sync) try self.storage.sync();
                pending.phase = .send_messages;
            },
            .send_messages => {
                self.sendMessages(pending.ready.light.messages);
                pending.phase = .apply_committed;
            },
            .apply_committed => {
                if (pending.ready.light.committed_entries.len > 0) {
                    try self.applyCommittedEntries(pending.ready.light.committed_entries);
                }
                pending.phase = .complete_reads;
            },
            .complete_reads => {
                for (pending.ready.read_states) |read_state| {
                    self.proposal_tracker.markReadReady(read_state.request_ctx, read_state.index);
                }
                self.proposal_tracker.completeReadyReads(self.applied_index);
                pending.phase = .advance;
            },
            .advance => {
                pending.light_ready = self.raw_node.*.advance(pending.ready) catch |err| {
                    self.fatal_error = err;
                    return err;
                };
                pending.phase = .persist_advanced_hard_state;
            },
            .persist_advanced_hard_state => {
                if (pending.light_ready.commit_index != null) {
                    try self.storage.setHardState(self.raw_node.*.raftConst().hardState());
                }
                pending.phase = .sync_advanced_hard_state;
            },
            .sync_advanced_hard_state => {
                if (pending.light_ready.commit_index != null) try self.storage.sync();
                pending.phase = .send_advanced_messages;
            },
            .send_advanced_messages => {
                self.sendMessages(pending.light_ready.messages);
                pending.phase = .apply_advanced_committed;
            },
            .apply_advanced_committed => {
                if (pending.light_ready.committed_entries.len > 0) {
                    try self.applyCommittedEntries(pending.light_ready.committed_entries);
                }
                pending.phase = .advance_apply;
            },
            .advance_apply => {
                self.raw_node.*.advanceApply();
                pending.deinit(self.allocator);
                self.pending = null;
                self.fatal_error = self.fatal_after_ready;
                self.fatal_after_ready = null;
            },
        }
        return true;
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn sendMessages(self: *ReadyProcessor, messages: []const Message) void {
        for (messages) |*message| {
            self.transport.send(message[0..1]) catch |err| {
                log.warn("failed to send Raft message to {}: {s}", .{ message.to, @errorName(err) });
                self.raw_node.*.reportUnreachable(message.to) catch {};
                if (message.msg_type == .snapshot) {
                    self.raw_node.*.reportSnapshot(message.to, .failure) catch {};
                }
            };
        }
    }

    fn checkLeadershipChange(self: *ReadyProcessor, rd: Ready) void {
        if (rd.ss) |ss| {
            if (ss.role != self.prev_role or ss.leader_id != self.prev_leader) {
                self.state_machine.onLeadershipChange(ss.role == .leader, self.raw_node.*.raftConst().term, ss.leader_id);
                self.prev_role = ss.role;
                self.prev_leader = ss.leader_id;
            }
        }
        const current_term = self.raw_node.*.raftConst().term;
        if (current_term != self.prev_term) {
            self.prev_term = current_term;
        }
    }

    fn validateEntries(self: *ReadyProcessor, entries: []const Entry) Error!void {
        for (entries) |entry| {
            if (util.isChecksumExemptEntry(entry)) continue;
            const expected = entry.checksum;
            if (expected == 0) continue;
            const actual = util.computeEntryChecksum(entry);
            if (actual != expected) {
                if (entry.context.len > 0) {
                    self.proposal_tracker.fail(entry.context, error.ChecksumMismatch);
                }
                self.fatal_error = error.ChecksumMismatch;
                return error.ChecksumMismatch;
            }
        }
    }

    fn restoreSnapshot(self: *ReadyProcessor, snap: Snapshot) Error!void {
        log.info("applying snapshot at index {} term {}", .{ snap.metadata.index, snap.metadata.term });

        var reader = state_machine_mod.BufferSnapshotReader.init(snap.data);
        try self.state_machine.restoreSnapshot(snap.metadata, reader.reader());
    }

    fn persistSnapshot(self: *ReadyProcessor, snap: Snapshot) Error!void {
        try self.storage.applySnapshot(self.allocator, snap);

        self.applied_index = snap.metadata.index;
        self.proposal_tracker.completeReadyReads(self.applied_index);
    }

    fn applyCommittedEntries(self: *ReadyProcessor, entries: []Entry) Error!void {
        for (entries) |entry| {
            self.applyEntry(entry) catch |e| {
                if (entry.context.len > 0) self.proposal_tracker.fail(entry.context, e);
                self.fatal_error = e;
                return e;
            };
            self.applied_index = entry.index;
            self.proposal_tracker.completeReadyReads(self.applied_index);
        }
    }

    fn applyEntry(self: *ReadyProcessor, entry: Entry) Error!void {
        if (self.checksum_enabled) try self.validateEntries(&.{entry});

        switch (entry.entry_type) {
            .normal => {
                var result = try self.state_machine.apply(entry);
                defer result.deinit(self.allocator);
                if (result.response) |resp| {
                    if (entry.context.len > 0) {
                        self.proposal_tracker.complete(entry.context, resp);
                    }
                } else {
                    if (entry.context.len > 0) {
                        self.proposal_tracker.complete(entry.context, "");
                    }
                }
            },
            .conf_change, .conf_change_v2 => {
                var cc = util.decodeConfChangeV2(self.allocator, entry.data) catch return error.ConfChangeParseError;
                defer cc.deinit(self.allocator);

                var applied_cs = try self.raw_node.*.applyConfChange(cc);
                defer applied_cs.deinit(self.allocator);
                try self.storage.setConfState(self.allocator, applied_cs);
                for (cc.changes) |change| switch (change.change_type) {
                    .add_node, .add_learner_node => _ = self.transport.addPeer(change.node_id, cc.context) catch |err| {
                        log.warn("failed to add transport peer {}: {s}", .{ change.node_id, @errorName(err) });
                        self.fatal_after_ready = err;
                        continue;
                    },
                    .remove_node => self.transport.removePeer(change.node_id) catch |err| {
                        log.warn("failed to remove transport peer {}: {s}", .{ change.node_id, @errorName(err) });
                        self.fatal_after_ready = err;
                    },
                };
            },
        }
    }
};
