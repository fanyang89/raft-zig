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
            .checksum_enabled = checksum_enabled,
            .allocator = allocator,
        };
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

    /// Process one Ready cycle. Returns true if there was work to do.
    pub fn process(self: *ReadyProcessor) Error!bool {
        if (self.fatal_error) |e| return e;
        if (!self.raw_node.*.hasReady()) return false;

        var rd = try self.raw_node.*.getReady();
        defer rd.deinit(self.allocator);

        self.checkLeadershipChange(rd);

        // 1. Validate checksums (optional).
        if (self.checksum_enabled) try self.validateEntries(rd.entries);

        // 2. Persist entries.
        if (rd.entries.len > 0) {
            self.storage.append(self.allocator, rd.entries) catch |e| {
                log.warn("failed to persist entries: {s}", .{@errorName(e)});
                return error.ProposalDropped;
            };
        }

        // 3. Persist HardState.
        if (rd.hs) |hs| {
            self.storage.setHardState(hs) catch |e| {
                log.warn("failed to persist hard state: {s}", .{@errorName(e)});
                return error.ProposalDropped;
            };
            if (rd.must_sync) {
                self.storage.sync() catch {};
            }
        }

        // 4. Apply snapshot.
        if (rd.snapshot) |snap| {
            if (snap.metadata.index > 0) try self.applySnapshot(snap);
        }

        // 5. Send messages (from rd.light — pre-advance messages).
        self.sendMessages(rd.light.messages);

        // 6. Apply committed entries from rd.light.
        if (rd.light.committed_entries.len > 0) try self.applyCommittedEntries(rd.light.committed_entries);

        // 7. Enqueue read states.
        for (rd.read_states) |rs| {
            self.proposal_tracker.completeRead(rs.request_ctx);
        }

        // Also check rd.light for committed entries and messages already
        // pulled by getReady's getLightReady.
        // rd.light.committed_entries was applied above.
        // rd.light.messages were sent above.

        // 8. Advance and process the light ready.
        var light_rd = try self.raw_node.*.advance(rd);
        defer light_rd.deinit(self.allocator);

        // Send messages from advance's light ready.
        self.sendMessages(light_rd.messages);

        // Apply committed entries from advance's light ready.
        if (light_rd.committed_entries.len > 0) try self.applyCommittedEntries(light_rd.committed_entries);

        self.raw_node.*.advanceApply();

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
                log.warn("checksum mismatch at index {}: expected {x}, got {x}", .{ entry.index, expected, actual });
                if (entry.context.len > 0) {
                    self.proposal_tracker.fail(entry.context, error.ChecksumMismatch);
                }
                return error.ChecksumMismatch;
            }
        }
    }

    fn applySnapshot(self: *ReadyProcessor, snap: Snapshot) Error!void {
        log.info("applying snapshot at index {} term {}", .{ snap.metadata.index, snap.metadata.term });

        // Restore to state machine first.
        const reader = BufferSnapshotReader.init(snap.data);
        try self.state_machine.restoreSnapshot(snap.metadata, .{
            .ctx = @constCast(&reader),
            .vtable = &buffer_snapshot_reader_vtable,
        });

        // Then apply to storage.
        self.storage.applySnapshot(self.allocator, snap) catch |e| {
            log.warn("failed to apply snapshot to storage: {s}", .{@errorName(e)});
            return error.ProposalDropped;
        };

        self.applied_index = snap.metadata.index;
    }

    fn applyCommittedEntries(self: *ReadyProcessor, entries: []Entry) Error!void {
        for (entries) |entry| {
            self.applyEntry(entry) catch |e| {
                log.warn("failed to apply entry at index {}: {s}", .{ entry.index, @errorName(e) });
                if (e == error.ChecksumMismatch) {
                    self.fatal_error = e;
                    return e;
                }
                // Non-fatal: continue applying remaining entries.
            };
            self.applied_index = entry.index;
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
                var cc = util.decodeConfChangeV2(self.allocator, entry.data) catch {
                    log.warn("failed to decode ConfChangeV2 at index {}", .{entry.index});
                    return error.ConfChangeParseError;
                };
                defer cc.deinit(self.allocator);

                var applied_cs = self.raw_node.*.applyConfChange(cc) catch |e| {
                    log.warn("ApplyConfChange failed: {s}", .{@errorName(e)});
                    return e;
                };
                defer applied_cs.deinit(self.allocator);
                self.storage.setConfState(self.allocator, applied_cs) catch |err| {
                    log.warn("failed to persist configuration: {s}", .{@errorName(err)});
                    self.fatal_error = err;
                };
                for (cc.changes) |change| switch (change.change_type) {
                    .add_node, .add_learner_node => _ = self.transport.addPeer(change.node_id, cc.context) catch |err| {
                        log.warn("failed to add transport peer {}: {s}", .{ change.node_id, @errorName(err) });
                        self.fatal_error = err;
                        continue;
                    },
                    .remove_node => self.transport.removePeer(change.node_id) catch |err| {
                        log.warn("failed to remove transport peer {}: {s}", .{ change.node_id, @errorName(err) });
                        self.fatal_error = err;
                    },
                };
            },
        }
    }
};

// ===========================================================================
// Buffer-backed SnapshotReader
// ===========================================================================

const BufferSnapshotReader = struct {
    data: []const u8,
    offset: usize = 0,

    fn init(data: []const u8) BufferSnapshotReader {
        return .{ .data = data };
    }

    fn readImpl(ctx: *anyopaque, out: []u8) Error!usize {
        const self: *BufferSnapshotReader = @ptrCast(@alignCast(ctx));
        if (self.offset >= self.data.len or out.len == 0) return 0;
        const remaining = self.data.len - self.offset;
        const n = @min(out.len, remaining);
        @memcpy(out[0..n], self.data[self.offset .. self.offset + n]);
        self.offset += n;
        return n;
    }
};

const buffer_snapshot_reader_vtable: state_machine_mod.SnapshotReader.VTable = .{
    .read = BufferSnapshotReader.readImpl,
};
