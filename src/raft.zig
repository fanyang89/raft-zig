//! Raft consensus state machine.
//!
//! The state machine is single-threaded by contract. Callers drive it via
//! `step(message)` (push an inbound message) and `tick()` (advance time by one
//! tick). Outbound messages accumulate in `self.messages`; the integrator is
//! responsible for draining and dispatching them.

const std = @import("std");

const error_model = @import("core/error.zig");
const primitives = @import("core/primitives.zig");
const types = @import("core/types.zig");
const util = @import("core/util.zig");
const storage_mod = @import("storage.zig");
const raft_log_mod = @import("raft_log.zig");
const read_only_mod = @import("read_only.zig");
const raft_config_mod = @import("raft_config.zig");
const progress_mod = @import("progress.zig");
const progress_tracker_mod = @import("progress_tracker.zig");
const tracker_conf_mod = @import("tracker_conf.zig");
const conf_changer_mod = @import("conf_changer.zig");
const conf_restore_mod = @import("conf_restore.zig");

const Error = error_model.Error;
const errorName = error_model.name;
const invalid_index = primitives.invalid_index;
const invalid_id = primitives.invalid_id;

const Entry = types.Entry;
const EntryType = types.EntryType;
const MessageType = types.MessageType;
const Message = types.Message;
const HardState = types.HardState;
const ConfState = types.ConfState;
const Snapshot = types.Snapshot;
const ConfChangeV2 = types.ConfChangeV2;
const ConfChangeSingle = types.ConfChangeSingle;
const ConfChangeTransition = types.ConfChangeTransition;
const ConfChangeType = types.ConfChangeType;

const Storage = storage_mod.Storage;
const shareEntry = storage_mod.shareEntry;
const cloneSnapshot = storage_mod.cloneSnapshot;
const GetEntriesContext = storage_mod.GetEntriesContext;
const RaftState = storage_mod.RaftState;

const RaftLog = raft_log_mod.RaftLog;

const ReadOnly = read_only_mod.ReadOnly;
const ReadOnlyOption = read_only_mod.ReadOnlyOption;
const ReadState = read_only_mod.ReadState;

const Config = raft_config_mod.Config;

const Progress = progress_mod.Progress;
const ProgressState = progress_mod.ProgressState;
const ProgressTracker = progress_tracker_mod.ProgressTracker;
const VoteResult = @import("ack_indexer.zig").VoteResult;
const TrackerConfiguration = tracker_conf_mod.TrackerConfiguration;
const ConfChanger = conf_changer_mod.ConfChanger;
const restoreTracker = conf_restore_mod.restore;

const StateRole = @import("core/state_role.zig").StateRole;
const invariant = @import("invariant.zig");
const SoftState = @import("core/state_role.zig").SoftState;

const log = std.log.scoped(.raft_zig);

// ===========================================================================
// Campaign type strings.
// ===========================================================================

pub const campaign_pre_election = "CampaignPreElection";
pub const campaign_election = "CampaignElection";
pub const campaign_transfer = "CampaignTransfer";

pub const CampaignType = enum { pre_election, election, transfer };

// ===========================================================================
// UncommittedState
// ===========================================================================

/// Tracks the total bytes of uncommitted entries so a leader doesn't accept
/// an unbounded backlog.
pub const UncommittedState = struct {
    max_uncommitted_size: u64,
    uncommitted_size: u64 = 0,
    last_log_tail_index: u64 = 0,

    pub fn isNoLimit(self: UncommittedState) bool {
        return self.max_uncommitted_size == std.math.maxInt(u64);
    }

    pub fn maybeIncreaseUncommittedSize(self: *UncommittedState, entries: []const Entry) bool {
        if (self.isNoLimit()) return true;

        var size: u64 = 0;
        for (entries) |e| size += e.data.len;

        if (size == 0 or self.uncommitted_size == 0 or
            size + self.uncommitted_size <= self.max_uncommitted_size)
        {
            self.uncommitted_size += size;
            return true;
        }
        return false;
    }

    pub fn maybeReduceUncommittedSize(self: *UncommittedState, entries: []const Entry) bool {
        if (self.isNoLimit() or entries.len == 0) return true;

        var size: u64 = 0;
        for (entries) |e| {
            if (e.index > self.last_log_tail_index) size += e.data.len;
        }

        if (size > self.uncommitted_size) {
            self.uncommitted_size = 0;
            return false;
        }
        self.uncommitted_size -= size;
        return true;
    }
};

// ===========================================================================
// Raft
// ===========================================================================

pub const Raft = struct {
    // Identity & current role.
    id: u64,
    term: u64,
    vote: u64,
    state: StateRole,
    leader_id: u64,
    lead_transferee: ?u64,
    promotable: bool,

    // Subsystems.
    raft_log: RaftLog,
    progress_tracker: ProgressTracker,
    read_only: ReadOnly,
    read_states: std.ArrayList(ReadState),
    pending_read_index_messages: std.ArrayList(Message),
    messages: std.ArrayList(Message),

    // Configuration-derived knobs.
    max_inflight: usize,
    max_message_size: u64,
    check_quorum: bool,
    pre_vote: bool,
    skip_broadcast_commit: bool,
    batch_append: bool,
    disable_proposal_forwarding: bool,

    // Timing.
    election_elapsed: usize,
    heartbeat_elapsed: usize,
    heartbeat_timeout: usize,
    election_timeout: usize,
    randomized_election_timeout: usize,
    min_election_timeout: usize,
    max_election_timeout: usize,

    // Misc bookkeeping.
    pending_conf_index: u64,
    pending_request_snapshot: u64,
    priority: i64,
    uncommitted_state: UncommittedState,
    max_committed_size_per_ready: u64,

    // PRNG for randomized election timeouts.
    prng: std.Random.DefaultPrng,

    allocator: std.mem.Allocator,
    config: Config,

    pub fn init(allocator: std.mem.Allocator, config: Config, store: Storage) Error!Raft {
        try config.validate();

        const seed = config.election_timeout_seed orelse blk: {
            // Zig 0.16 dropped std.crypto.random; use a stack address + the
            // configured id as a cheap per-instance seed mix. Tests should
            // pass `election_timeout_seed` explicitly to make timeouts
            // deterministic.
            var marker: u8 = undefined;
            break :blk @as(u64, @intFromPtr(&marker)) ^ (@as(u64, config.id) << 32);
        };
        var raft_log = try RaftLog.init(allocator, store, config.max_apply_unpersisted_log_limit);
        errdefer raft_log.deinit();

        var progress_tracker = ProgressTracker.init(allocator, config.max_inflight_messages);
        errdefer progress_tracker.deinit();

        // Restore the cluster configuration from the storage's initial state.
        const raft_state = try raft_log.getInitialState();
        var rs_copy = raft_state;
        defer rs_copy.deinit(allocator);
        try restoreTracker(&progress_tracker, raft_log.lastIndex() + 1, rs_copy.conf_state);
        var initial_cs = try progress_tracker.conf.toConfState(allocator);
        defer initial_cs.deinit(allocator);

        var r = Raft{
            .id = config.id,
            .term = 0,
            .vote = 0,
            .state = .follower,
            .leader_id = invalid_id,
            .lead_transferee = null,
            .promotable = false,

            .raft_log = raft_log,
            .progress_tracker = progress_tracker,
            .read_only = ReadOnly.init(allocator, config.read_only_option),
            .read_states = .empty,
            .pending_read_index_messages = .empty,
            .messages = .empty,

            .max_inflight = config.max_inflight_messages,
            .max_message_size = config.effectiveMaxSizePerMessage(),
            .check_quorum = config.check_quorum,
            .pre_vote = config.pre_vote,
            .skip_broadcast_commit = config.skip_broadcast_commit,
            .batch_append = config.batch_append,
            .disable_proposal_forwarding = config.disable_proposal_forwarding,

            .election_elapsed = 0,
            .heartbeat_elapsed = 0,
            .heartbeat_timeout = config.heartbeat_tick,
            .election_timeout = config.election_tick,
            .randomized_election_timeout = 0,
            .min_election_timeout = config.minElectionTick(),
            .max_election_timeout = config.maxElectionTick(),

            .pending_conf_index = 0,
            .pending_request_snapshot = invalid_index,
            .priority = 0,
            .uncommitted_state = .{ .max_uncommitted_size = config.max_uncommitted_size },
            .max_committed_size_per_ready = config.max_committed_size_per_ready,

            .prng = std.Random.DefaultPrng.init(seed),

            .allocator = allocator,
            .config = config,
        };

        if (config.load_state_on_startup and !raft_state.hard_state.isEmpty()) {
            r.loadState(raft_state.hard_state);
        }

        if (config.applied > 0) {
            r.commitApplyInternal(config.applied, true);
        }

        r.becomeFollower(r.term, invalid_id);

        // Post-confchange bookkeeping: figure out whether we're a voter.
        r.postConfChange(initial_cs);

        log.info(
            "node {} initialized: term={} commit={} applied={} last_index={} last_term={}",
            .{ r.id, r.term, r.raft_log.committed, r.raft_log.applied, r.raft_log.lastIndex(), r.raft_log.lastTerm() catch 0 },
        );

        invariant.assertRaft(&r);
        return r;
    }

    pub fn deinit(self: *Raft) void {
        self.raft_log.deinit();
        self.progress_tracker.deinit();
        self.read_only.deinit();
        for (self.read_states.items) |*rs| rs.deinit(self.allocator);
        self.read_states.deinit(self.allocator);
        for (self.pending_read_index_messages.items) |*m| m.deinit(self.allocator);
        self.pending_read_index_messages.deinit(self.allocator);
        for (self.messages.items) |*m| m.deinit(self.allocator);
        self.messages.deinit(self.allocator);
        self.* = undefined;
    }

    // -----------------------------------------------------------------------
    // Public accessors
    // -----------------------------------------------------------------------

    pub fn hardState(self: *const Raft) HardState {
        return .{
            .term = self.term,
            .vote = self.vote,
            .commit = self.raft_log.committed,
        };
    }

    pub fn softState(self: *const Raft) SoftState {
        return .{ .leader_id = self.leader_id, .role = self.state };
    }

    pub fn isLeader(self: *const Raft) bool {
        return self.state == .leader;
    }
    pub fn isFollower(self: *const Raft) bool {
        return self.state == .follower;
    }
    pub fn isCandidate(self: *const Raft) bool {
        return self.state == .candidate;
    }
    pub fn isPreCandidate(self: *const Raft) bool {
        return self.state == .pre_candidate;
    }

    pub fn snapshot(self: *Raft) ?Snapshot {
        if (self.raft_log.unstable.snapshot) |s| return cloneSnapshot(self.allocator, s) catch null;
        return null;
    }

    // -----------------------------------------------------------------------
    // State transitions
    // -----------------------------------------------------------------------

    pub fn becomeFollower(self: *Raft, term: u64, leader_id: u64) void {
        defer invariant.assertRaft(self);
        const pending_request_snapshot = self.pending_request_snapshot;
        self.reset(term);
        self.leader_id = leader_id;
        self.state = .follower;
        self.pending_request_snapshot = pending_request_snapshot;
        self.raft_log.max_apply_unpersisted_log_limit = 0;
        log.info("node {} became follower at term {}", .{ self.id, term });
    }

    pub fn becomePreCandidate(self: *Raft) void {
        defer invariant.assertRaft(self);
        std.debug.assert(self.state != .leader);
        self.state = .pre_candidate;
        self.progress_tracker.resetVotes();
        self.leader_id = invalid_id;
        log.info("node {} became pre-candidate at term {}", .{ self.id, self.term });
    }

    pub fn becomeCandidate(self: *Raft) void {
        defer invariant.assertRaft(self);
        std.debug.assert(self.state != .leader);
        const term = self.term + 1;
        self.reset(term);
        self.vote = self.id;
        self.state = .candidate;
        self.promotable = self.progress_tracker.conf.voters.contains(self.id);
        log.info("node {} became candidate at term {}", .{ self.id, term });
    }

    pub fn becomeLeader(self: *Raft) Error!void {
        defer invariant.assertRaft(self);
        std.debug.assert(self.state != .follower);

        self.reset(self.term);
        self.leader_id = self.id;
        self.state = .leader;

        const last_index = self.raft_log.lastIndex();
        self.uncommitted_state.uncommitted_size = 0;
        self.uncommitted_state.last_log_tail_index = last_index;

        // Self-progress starts in Replicate, matched to last_index.
        if (self.progress_tracker.getPtr(self.id)) |self_pr| {
            self_pr.becomeReplicate();
        }

        self.pending_conf_index = last_index;

        // Append a no-op entry so the new leader can commit past entries
        // from earlier terms (Figure 8 safety).
        const noop = Entry{};
        if (!try self.appendEntry(&.{noop})) {
            @panic("appending an empty entry should never be dropped");
        }

        log.info("node {} became leader at term {}", .{ self.id, self.term });
    }

    // -----------------------------------------------------------------------
    // Tick
    // -----------------------------------------------------------------------

    pub fn tick(self: *Raft) Error!bool {
        defer invariant.assertRaft(self);
        return switch (self.state) {
            .follower, .candidate, .pre_candidate => try self.tickElection(),
            .leader => try self.tickHeartbeat(),
        };
    }

    fn tickElection(self: *Raft) Error!bool {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;

        var has_ready = false;
        if (self.election_elapsed >= self.randomized_election_timeout) {
            self.election_elapsed = 0;
            var m = Message{
                .msg_type = .hup,
                .to = invalid_id,
                .from = self.id,
            };
            has_ready = true;
            try self.step(&m);
            m.deinit(self.allocator);
        }

        if (self.state != .leader) return has_ready;

        if (self.heartbeat_elapsed >= self.heartbeat_timeout) {
            self.heartbeat_elapsed = 0;
            has_ready = true;
            var m = Message{
                .msg_type = .beat,
                .to = invalid_id,
                .from = self.id,
            };
            try self.step(&m);
            m.deinit(self.allocator);
        }
        return has_ready;
    }

    fn tickHeartbeat(self: *Raft) Error!bool {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;

        var has_ready = false;
        if (self.election_elapsed >= self.randomized_election_timeout) {
            self.election_elapsed = 0;
            if (self.check_quorum) {
                var m = Message{
                    .msg_type = .check_quorum,
                    .to = invalid_id,
                    .from = self.id,
                };
                has_ready = true;
                try self.step(&m);
                m.deinit(self.allocator);
            }
            if (self.state == .leader and self.lead_transferee != null) {
                self.abortLeaderTransfer();
            }
        }

        if (self.state != .leader) return has_ready;

        if (self.heartbeat_elapsed >= self.heartbeat_timeout) {
            self.heartbeat_elapsed = 0;
            has_ready = true;
            var m = Message{
                .msg_type = .beat,
                .to = invalid_id,
                .from = self.id,
            };
            try self.step(&m);
            m.deinit(self.allocator);
        }
        return has_ready;
    }

    // -----------------------------------------------------------------------
    // Step dispatch
    // -----------------------------------------------------------------------

    pub fn step(self: *Raft, m_in: *Message) Error!void {
        defer invariant.assertRaft(self);
        // m_in may be moved into the messages list; clone for safety where
        // we still need to read after a `send`. We treat the caller-owned
        // buffer as mutable.
        const m = m_in;
        if (m.term == 0) {
            // Local message; fall through.
        } else if (m.term > self.term) {
            const is_vote_request = m.msg_type == .request_vote or m.msg_type == .request_pre_vote;
            if (is_vote_request) {
                const force = (m.context.len > 0 and std.mem.eql(u8, m.context, campaign_transfer));
                const in_lease = self.check_quorum and self.leader_id != invalid_id and
                    self.election_elapsed < self.election_timeout;
                if (!force and in_lease) {
                    log.debug("node {} ignored vote from {}: lease is not expired", .{ self.id, m.from });
                    return;
                }
            }

            const is_prevote_request = m.msg_type == .request_pre_vote;
            const is_prevote_resp_ok = m.msg_type == .request_pre_vote_response and !m.reject;
            if (is_prevote_request or is_prevote_resp_ok) {
                // Never change our term in response to a pre-vote.
            } else {
                log.info("node {} received a message with higher term from {}", .{ self.id, m.from });
                if (m.msg_type == .append or m.msg_type == .heartbeat or m.msg_type == .snapshot) {
                    self.becomeFollower(m.term, m.from);
                } else {
                    self.becomeFollower(m.term, invalid_id);
                }
            }
            // Fall through.
        } else if (m.term < self.term) {
            if ((self.check_quorum or self.pre_vote) and
                (m.msg_type == .heartbeat or m.msg_type == .append))
            {
                try self.send(.{
                    .msg_type = .append_response,
                    .to = m.from,
                });
            } else if (m.msg_type == .request_pre_vote) {
                try self.send(.{
                    .msg_type = .request_pre_vote_response,
                    .to = m.from,
                    .term = self.term,
                    .reject = true,
                });
            } else {
                log.debug("node {} ignored a message with lower term from {}", .{ self.id, m.from });
            }
            return;
        }

        // m.term == self.term.
        switch (m.msg_type) {
            .hup => {
                if (self.promotable) {
                    self.hup(false);
                } else {
                    log.debug("node {} received MsgHup but is not promotable", .{self.id});
                }
            },
            .request_vote, .request_pre_vote => try self.handleVoteRequest(m),
            else => switch (self.state) {
                .pre_candidate, .candidate => try self.stepCandidate(m),
                .follower => try self.stepFollower(m),
                .leader => try self.stepLeader(m),
            },
        }
    }

    fn handleVoteRequest(self: *Raft, m: *Message) Error!void {
        const can_vote = (self.vote == m.from) or
            (self.vote == invalid_id and self.leader_id == invalid_id) or
            (m.msg_type == .request_pre_vote and m.term > self.term);

        const up_to_date = try self.raft_log.isUpToDate(m.index, m.log_term);
        const priority_ok = m.index > self.raft_log.lastIndex() or self.priority <= m.priority;

        if (can_vote and up_to_date and priority_ok) {
            try self.send(.{
                .msg_type = voteRespMsgType(m.msg_type),
                .to = m.from,
                .term = m.term,
                .reject = false,
            });

            if (m.msg_type == .request_vote) {
                // Only record real votes.
                self.election_elapsed = 0;
                self.vote = m.from;
            }
        } else {
            const commit_info = try self.raft_log.commitInfo();
            try self.send(.{
                .msg_type = voteRespMsgType(m.msg_type),
                .to = m.from,
                .term = self.term,
                .reject = true,
                .commit = commit_info.index,
                .commit_term = commit_info.term,
            });
            try self.maybeCommitByVote(m);
        }
    }

    fn stepCandidate(self: *Raft, m: *Message) Error!void {
        switch (m.msg_type) {
            .propose => return error.ProposalDropped,
            .append => {
                self.becomeFollower(m.term, m.from);
                try self.handleAppendEntries(m);
            },
            .heartbeat => {
                self.becomeFollower(m.term, m.from);
                try self.handleHeartbeat(m);
            },
            .snapshot => {
                self.becomeFollower(m.term, m.from);
                try self.handleSnapshot(m);
            },
            .request_pre_vote_response, .request_vote_response => {
                if ((self.state == .pre_candidate and m.msg_type != .request_pre_vote_response) or
                    (self.state == .candidate and m.msg_type != .request_vote_response))
                {
                    return;
                }
                _ = self.poll(m.from, !m.reject);
                try self.maybeCommitByVote(m);
            },
            .timeout_now => log.debug("candidate ignored MsgTimeoutNow from {}", .{m.from}),
            .read_index => log.debug("node {} has no leader; dropping read index message", .{self.id}),
            else => {},
        }
    }

    fn stepFollower(self: *Raft, m: *Message) Error!void {
        switch (m.msg_type) {
            .propose => {
                if (self.leader_id == invalid_id) return error.ProposalDropped;
                if (self.disable_proposal_forwarding) return error.ProposalDropped;
                m.to = self.leader_id;
                try self.send(m.*);
                // Mark as consumed so caller doesn't double-free.
                m.entries = &.{};
                m.context = &.{};
                m.snapshot = null;
            },
            .append => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                try self.handleAppendEntries(m);
            },
            .heartbeat => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                try self.handleHeartbeat(m);
            },
            .snapshot => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                try self.handleSnapshot(m);
            },
            .transfer_leader => {
                if (self.leader_id == invalid_id) {
                    log.debug("node {} has no leader; dropping transfer message", .{self.id});
                    return;
                }
                m.to = self.leader_id;
                try self.send(m.*);
                m.entries = &.{};
                m.context = &.{};
                m.snapshot = null;
            },
            .timeout_now => {
                if (self.promotable) {
                    self.hup(true);
                } else {
                    log.debug("node {} received MsgTimeoutNow but is not promotable", .{self.id});
                }
            },
            .read_index => {
                if (self.leader_id == invalid_id) {
                    log.debug("node {} has no leader; dropping read index message", .{self.id});
                    return;
                }
                m.to = self.leader_id;
                try self.send(m.*);
                m.entries = &.{};
                m.context = &.{};
                m.snapshot = null;
            },
            .read_index_resp => {
                if (self.leader_id == invalid_id or m.from != self.leader_id) {
                    log.debug("ignored MsgReadIndexResp from {}", .{m.from});
                    return;
                }
                if (m.entries.len != 1) {
                    log.warn("invalid MsgReadIndexResp from {} entries={}", .{ m.from, m.entries.len });
                    return;
                }
                const ctx_bytes = m.entries[0].data;
                const ctx_copy = self.allocator.dupe(u8, ctx_bytes) catch return error.OutOfMemory;
                try self.read_states.append(self.allocator, .{
                    .index = m.index,
                    .request_ctx = ctx_copy,
                });
                _ = self.raft_log.maybeCommit(m.index, m.term) catch null;
            },
            else => {},
        }
    }

    fn stepLeader(self: *Raft, m: *Message) Error!void {
        switch (m.msg_type) {
            .beat => {
                try self.broadcastHeartbeat();
                return;
            },
            .check_quorum => {
                const active = self.progress_tracker.quorumRecentlyActive(self.id) catch true;
                if (!active) {
                    log.warn("node {} stepped down because quorum is inactive", .{self.id});
                    self.becomeFollower(self.term, invalid_id);
                }
                return;
            },
            .propose => {
                if (m.entries.len == 0) @panic("stepped empty MsgProp");
                if (!self.progress_tracker.progress.contains(self.id)) return error.ProposalDropped;
                if (self.lead_transferee != null) return error.ProposalDropped;

                // Detect configuration proposals; reject if one is pending or
                // if multiple arrive in a single batch.
                var conf_change_position: ?usize = null;
                for (m.entries, 0..) |e, i| {
                    if (e.entry_type == .conf_change or e.entry_type == .conf_change_v2) {
                        if (self.hasPendingConf()) {
                            log.debug("node {} dropped configuration change while another is pending", .{self.id});
                            return error.ProposalDropped;
                        }
                        if (conf_change_position != null) {
                            log.debug("node {} dropped multiple configuration changes", .{self.id});
                            return error.ProposalDropped;
                        }
                        conf_change_position = i;
                        if (e.data.len == 0) {
                            log.debug("node {} dropped configuration change without data", .{self.id});
                            return error.ProposalDropped;
                        }
                    }
                }

                const old_last_index = self.raft_log.lastIndex();
                if (!try self.appendEntry(m.entries)) return error.ProposalDropped;

                if (conf_change_position) |pos| {
                    self.pending_conf_index = old_last_index + @as(u64, @intCast(pos)) + 1;
                }

                try self.broadcastAppend();
                return;
            },
            .read_index => {
                if (!self.commitToCurrentTerm()) {
                    log.debug("node {} has not committed in its term; postponing read index", .{self.id});
                    var cloned = try storage_mod.shareMessage(self.allocator, m.*);
                    errdefer cloned.deinit(self.allocator);
                    try self.pending_read_index_messages.append(self.allocator, cloned);
                    return;
                }
                try self.serveReadIndex(m);
                return;
            },
            else => {},
        }

        switch (m.msg_type) {
            .append_response => try self.handleAppendResponse(m),
            .heartbeat_response => try self.handleHeartbeatResponse(m),
            .snap_status => try self.handleSnapshotStatus(m),
            .unreachable_peer => try self.handleUnreachable(m),
            .transfer_leader => try self.handleTransferLeader(m),
            else => {
                if (self.progress_tracker.getPtr(m.from) == null) {
                    log.debug("no progress available for {}", .{m.from});
                }
            },
        }
    }

    // -----------------------------------------------------------------------
    // Election & voting
    // -----------------------------------------------------------------------

    pub fn poll(self: *Raft, from: u64, vote: bool) VoteResult {
        self.progress_tracker.recordVote(from, vote) catch {};
        const r = self.progress_tracker.countVotes();
        if (from != self.id) {
            log.debug("received vote response from {} vote={}", .{ from, vote });
        }

        switch (r.result) {
            .pending => {},
            .lost => self.becomeFollower(self.term, invalid_id),
            .won => {
                if (self.state == .pre_candidate) {
                    self.campaign(.election) catch {};
                } else {
                    self.becomeLeader() catch {};
                    self.broadcastAppend() catch {};
                }
            },
        }
        return r.result;
    }

    pub fn campaign(self: *Raft, campaign_type: CampaignType) Error!void {
        var vote_msg: MessageType = undefined;
        var term: u64 = undefined;

        switch (campaign_type) {
            .pre_election => {
                self.becomePreCandidate();
                vote_msg = .request_pre_vote;
                term = self.term + 1;
            },
            .election, .transfer => {
                self.becomeCandidate();
                vote_msg = .request_vote;
                term = self.term;
            },
        }

        if (self.poll(self.id, true) == .won) return;

        const commit_info = try self.raft_log.commitInfo();
        const voter_ids = self.progress_tracker.conf.voters.ids() catch return error.OutOfMemory;
        defer self.allocator.free(voter_ids);

        for (voter_ids) |voter_id| {
            if (voter_id == self.id) continue;

            var context: []u8 = &.{};
            if (campaign_type == .transfer) {
                context = self.allocator.dupe(u8, campaign_transfer) catch return error.OutOfMemory;
            }
            try self.send(.{
                .msg_type = vote_msg,
                .to = voter_id,
                .term = term,
                .index = self.raft_log.lastIndex(),
                .log_term = self.raft_log.lastTerm() catch 0,
                .commit = commit_info.index,
                .commit_term = commit_info.term,
                .context = context,
            });
        }
    }

    pub fn hup(self: *Raft, transfer_leader: bool) void {
        if (self.state == .leader) {
            log.debug("ignoring MsgHup; already leader", .{});
            return;
        }

        const low: u64 = if (self.raft_log.unstable.maybeFirstIndex()) |i| i else self.raft_log.applied + 1;
        const high = self.raft_log.committed + 1;
        if (self.hasUnappliedConfChanges(low, high)) {
            log.debug("node {} cannot campaign at term {}; configuration changes are pending", .{ self.id, self.term });
            return;
        }

        log.info("node {} starting a new election at term {}", .{ self.id, self.term });
        if (transfer_leader) {
            self.campaign(.transfer) catch {};
        } else if (self.pre_vote) {
            self.campaign(.pre_election) catch {};
        } else {
            self.campaign(.election) catch {};
        }
    }

    fn hasUnappliedConfChanges(self: *Raft, low: u64, high: u64) bool {
        if (self.raft_log.applied >= self.raft_log.committed) return false;

        const Finder = struct {
            found: *bool,
            pub fn scan(self_: *@This(), entries: []const Entry) Error!bool {
                for (entries) |e| {
                    if (e.entry_type == .conf_change or e.entry_type == .conf_change_v2) {
                        self_.found.* = true;
                        return false;
                    }
                }
                return true;
            }
        };
        var found = false;
        var finder = Finder{ .found = &found };
        self.raft_log.scan(low, high, self.max_committed_size_per_ready, GetEntriesContext{ .transfer_leader = {} }, Finder, &finder) catch return true;
        return found;
    }

    pub fn maybeCommit(self: *Raft) Error!bool {
        const max_commit_index = self.progress_tracker.maxCommittedIndex().index;
        if (try self.raft_log.maybeCommit(max_commit_index, self.term)) {
            const committed = self.raft_log.committed;
            if (self.progress_tracker.getPtr(self.id)) |self_pr| {
                self_pr.updateCommitted(committed);
            }
            try self.releasePendingReadIndexMessages();
            return true;
        }
        return false;
    }

    pub fn commitToCurrentTerm(self: *const Raft) bool {
        const t = self.raft_log.term(self.raft_log.committed) catch return false;
        return t == self.term;
    }

    pub fn maybeCommitByVote(self: *Raft, m: *const Message) Error!void {
        if (m.commit == 0 or m.commit_term == 0) return;
        const last_commit = self.raft_log.committed;
        if (m.commit <= last_commit or self.state == .leader) return;
        if (!try self.raft_log.maybeCommit(m.commit, m.commit_term)) return;

        log.info(
            "fast-forwarded commit to vote request: index={} term={}",
            .{ m.commit, m.commit_term },
        );

        if (self.state != .candidate and self.state != .pre_candidate) return;

        const low = last_commit + 1;
        const high = self.raft_log.committed + 1;
        if (self.hasUnappliedConfChanges(low, high)) {
            self.becomeFollower(self.term, invalid_id);
        }
    }

    pub fn shouldBroadcastCommit(self: *const Raft) bool {
        return !self.skip_broadcast_commit or self.hasPendingConf();
    }

    pub fn hasPendingConf(self: *const Raft) bool {
        return self.pending_conf_index > self.raft_log.applied;
    }

    // -----------------------------------------------------------------------
    // AppendEntries / heartbeat handlers
    // -----------------------------------------------------------------------

    pub fn handleAppendEntries(self: *Raft, m: *Message) Error!void {
        if (self.pending_request_snapshot != invalid_index) {
            try self.sendRequestSnapshot();
            return;
        }

        if (m.index < self.raft_log.committed) {
            try self.send(.{
                .msg_type = .append_response,
                .to = m.from,
                .index = self.raft_log.committed,
                .commit = self.raft_log.committed,
            });
            return;
        }

        const result = try self.raft_log.maybeAppend(m.index, m.log_term, m.commit, m.entries);

        if (result.term_matched) {
            try self.send(.{
                .msg_type = .append_response,
                .to = m.from,
                .index = result.last_index,
                .commit = self.raft_log.committed,
            });
        } else {
            const probe_idx = @min(m.index, self.raft_log.lastIndex());
            const fc = try self.raft_log.findConflictByTerm(probe_idx, m.log_term);
            const hint_term = fc.term orelse 0;
            try self.send(.{
                .msg_type = .append_response,
                .to = m.from,
                .index = m.index,
                .reject = true,
                .reject_hint = fc.index,
                .log_term = hint_term,
                .commit = self.raft_log.committed,
            });
        }
    }

    pub fn handleAppendResponse(self: *Raft, m: *Message) Error!void {
        var next_probe_index = m.reject_hint;
        if (m.reject and m.log_term > 0) {
            const fc = try self.raft_log.findConflictByTerm(m.reject_hint, m.log_term);
            next_probe_index = fc.index;
        }

        const pr = self.progress_tracker.getPtr(m.from) orelse {
            log.warn("no progress available for {}", .{m.from});
            return;
        };

        pr.recent_active = true;
        pr.updateCommitted(m.commit);

        if (m.reject) {
            if (pr.maybeDecTo(m.index, next_probe_index, m.request_snapshot)) {
                if (pr.state == .replicate) pr.becomeProbe();
                try self.sendAppend(m.from);
            }
            return;
        }

        const old_paused = pr.isPaused();
        if (!pr.maybeUpdate(m.index)) return;

        switch (pr.state) {
            .probe => pr.becomeReplicate(),
            .replicate => {
                pr.inflights.freeTo(self.allocator, m.index);
                if (pr.isSnapshotCaughtUp()) pr.becomeProbe();
            },
            .snapshot => {
                if (pr.isSnapshotCaughtUp()) pr.becomeProbe();
            },
        }

        if (try self.maybeCommit()) {
            if (self.shouldBroadcastCommit()) try self.broadcastAppend();
        } else if (old_paused) {
            try self.sendAppend(m.from);
        }

        try self.sendAppendAggressively(m.from);

        if (self.lead_transferee) |lt| {
            if (lt == m.from) {
                if (self.progress_tracker.getPtr(m.from)) |p| {
                    if (p.matched == self.raft_log.lastIndex()) {
                        log.info("sent MsgTimeoutNow to {} after MsgAppResp", .{m.from});
                        try self.sendTimeoutNow(m.from);
                    }
                }
            }
        }
    }

    pub fn handleHeartbeat(self: *Raft, m: *Message) Error!void {
        _ = self.raft_log.commitTo(m.commit) catch null;
        if (self.pending_request_snapshot != invalid_index) {
            try self.sendRequestSnapshot();
            return;
        }

        var context: []u8 = &.{};
        if (m.context.len > 0) {
            context = self.allocator.dupe(u8, m.context) catch return error.OutOfMemory;
        }
        try self.send(.{
            .msg_type = .heartbeat_response,
            .to = m.from,
            .context = context,
            .commit = self.raft_log.committed,
        });
    }

    pub fn handleHeartbeatResponse(self: *Raft, m: *Message) Error!void {
        const pr = self.progress_tracker.getPtr(m.from) orelse {
            log.info("no progress available for {}", .{m.from});
            return;
        };

        pr.updateCommitted(m.commit);
        pr.recent_active = true;
        pr.resume_();
        if (pr.state == .replicate and pr.inflights.full()) {
            pr.inflights.freeFirstOne(self.allocator);
        }

        if (pr.matched < self.raft_log.lastIndex() or pr.pending_request_snapshot != invalid_index) {
            try self.sendAppendById(m.from);
        }

        if (self.read_only.option != .safe or m.context.len == 0) return;

        const acks = (try self.read_only.recvACK(m.from, m.context)) orelse return;
        if (!try self.hasQuorum(acks)) return;

        const statuses = try self.read_only.advance(m.context);
        defer self.allocator.free(statuses);
        for (statuses) |*st| {
            if (try self.handleReadyReadIndexMsg(st.req, st.index)) |resp| {
                self.send(resp) catch |err| {
                    var owned = resp;
                    owned.deinit(self.allocator);
                    return err;
                };
            }
            // Advance transferred ownership of the status (incl. req).
            var mut = st.*;
            mut.deinit(self.allocator);
        }
    }

    pub fn handleSnapshot(self: *Raft, m: *Message) Error!void {
        var snap_owned = if (m.snapshot) |s| try cloneSnapshot(self.allocator, s) else return;
        defer snap_owned.deinit(self.allocator);
        const last_index = if (try self.restoreSnapshot(snap_owned)) blk: {
            break :blk self.raft_log.lastIndex();
        } else blk: {
            break :blk self.raft_log.committed;
        };

        try self.send(.{
            .msg_type = .append_response,
            .to = m.from,
            .index = last_index,
        });
        // no-op marker for clarity
    }

    pub fn handleSnapshotStatus(self: *Raft, m: *Message) Error!void {
        const pr = self.progress_tracker.getPtr(m.from) orelse return;
        if (pr.state != .snapshot) return;

        if (m.reject) {
            pr.snapshotFailure();
            pr.becomeProbe();
        } else {
            pr.becomeProbe();
        }

        pr.pause();
        pr.pending_request_snapshot = invalid_index;
    }

    pub fn handleUnreachable(self: *Raft, m: *Message) Error!void {
        const pr = self.progress_tracker.getPtr(m.from) orelse return;
        if (pr.state == .replicate) pr.becomeProbe();
        log.info("peer {} reported unreachable", .{m.from});
    }

    pub fn handleTransferLeader(self: *Raft, m: *Message) Error!void {
        const from = m.from;
        const pr = self.progress_tracker.getPtr(from) orelse return;
        if (self.progress_tracker.conf.learners.contains(from)) return;

        if (self.lead_transferee) |existing| {
            if (existing == from) return;
            self.abortLeaderTransfer();
        }
        if (from == self.id) return;

        self.election_elapsed = 0;
        self.lead_transferee = from;

        if (pr.matched == self.raft_log.lastIndex()) {
            try self.sendTimeoutNow(from);
        } else {
            try self.sendAppendById(from);
        }
    }

    pub fn handleReadyReadIndex(self: *Raft, req: *const Message, index: u64) Error!?Message {
        if (req.from == invalid_id or req.from == self.id) {
            const ctx_bytes = if (req.entries.len > 0) req.entries[0].data else "";
            const ctx_copy = self.allocator.dupe(u8, ctx_bytes) catch return error.OutOfMemory;
            try self.read_states.append(self.allocator, .{
                .index = index,
                .request_ctx = ctx_copy,
            });
            return null;
        }

        // Build a ReadIndexResp that echoes the request's ctx in its entry data.
        var entries = try self.allocator.alloc(Entry, req.entries.len);
        var initialized: usize = 0;
        errdefer {
            for (entries[0..initialized]) |*entry| entry.deinit(self.allocator);
            self.allocator.free(entries);
        }
        for (req.entries) |entry| {
            entries[initialized] = try shareEntry(self.allocator, entry);
            initialized += 1;
        }
        return Message{
            .msg_type = .read_index_resp,
            .to = req.from,
            .index = index,
            .entries = entries,
        };
    }

    fn handleReadyReadIndexMsg(self: *Raft, req: Message, index: u64) Error!?Message {
        return self.handleReadyReadIndex(&req, index);
    }

    pub fn pendingReadIndexCount(self: *const Raft) usize {
        return self.pending_read_index_messages.items.len;
    }

    // Serve a ReadIndex request that is now eligible (leader has committed an
    // entry in its own term). Used both by the step path and by the replay of
    // postponed requests after a fresh-term commit.
    fn serveReadIndex(self: *Raft, m: *Message) Error!void {
        if (self.progress_tracker.isSingleton()) {
            const read_index = self.raft_log.committed;
            if (try self.handleReadyReadIndex(m, read_index)) |resp| {
                self.send(resp) catch |err| {
                    var owned = resp;
                    owned.deinit(self.allocator);
                    return err;
                };
            }
            return;
        }

        switch (self.read_only.option) {
            .safe => {
                if (m.entries.len > 0) {
                    // AddRequest clones the message internally.
                    try self.read_only.addRequest(self.raft_log.committed, m.*, self.id);
                    try self.broadcastHeartbeatWithCtx(m.entries[0].data);
                }
            },
            .lease_based => {
                const read_index = self.raft_log.committed;
                if (try self.handleReadyReadIndex(m, read_index)) |resp| {
                    self.send(resp) catch |err| {
                        var owned = resp;
                        owned.deinit(self.allocator);
                        return err;
                    };
                }
            },
        }
    }

    // Replay postponed ReadIndex requests once the leader has committed an
    // entry in its current term. Mirrors etcd's releasePendingReadIndexMessages.
    fn releasePendingReadIndexMessages(self: *Raft) Error!void {
        if (self.pending_read_index_messages.items.len == 0) return;
        if (!self.commitToCurrentTerm()) return;

        var local = self.pending_read_index_messages;
        self.pending_read_index_messages = .empty;
        defer local.deinit(self.allocator);

        var i: usize = 0;
        while (i < local.items.len) : (i += 1) {
            var owned = local.items[i];
            defer owned.deinit(self.allocator);
            try self.serveReadIndex(&owned);
        }
    }

    fn hasQuorum(self: *Raft, acks: *std.AutoHashMap(u64, void)) Error!bool {
        return self.progress_tracker.hasQuorum(acks.*);
    }

    // -----------------------------------------------------------------------
    // Snapshot
    // -----------------------------------------------------------------------

    pub fn restoreSnapshot(self: *Raft, snap_in: Snapshot) Error!bool {
        const meta = snap_in.metadata;
        if (meta.index == std.math.maxInt(u64)) return false;
        if (self.pending_request_snapshot != invalid_index and meta.index < self.pending_request_snapshot) return false;
        if (meta.index < self.raft_log.committed) return false;
        if (meta.index == self.raft_log.committed and self.pending_request_snapshot == invalid_index) return false;

        if (self.state != .follower) {
            log.warn("non-follower attempted to restore snapshot", .{});
            self.becomeFollower(self.term + 1, invalid_id);
            return false;
        }

        const member_sets = [_]struct { members: []const u64, role: u8 }{
            .{ .members = meta.conf_state.voters, .role = 1 },
            .{ .members = meta.conf_state.learners, .role = 2 },
            .{ .members = meta.conf_state.voters_outgoing, .role = 4 },
            .{ .members = meta.conf_state.learners_next, .role = 8 },
        };
        var member_roles = std.AutoHashMap(u64, u8).init(self.allocator);
        defer member_roles.deinit();
        for (member_sets) |set| {
            for (set.members) |id| {
                if (id == invalid_id) {
                    log.warn("invalid snapshot ConfState member {}", .{id});
                    return false;
                }
                const result = try member_roles.getOrPut(id);
                const previous = if (result.found_existing) result.value_ptr.* else 0;
                const combined = previous | set.role;
                if (previous & set.role != 0) {
                    log.warn("duplicate snapshot ConfState member {}", .{id});
                    return false;
                }
                switch (combined) {
                    1, 2, 4, 5, 8, 12 => result.value_ptr.* = combined,
                    else => {
                        log.warn("conflicting snapshot ConfState roles for member {}", .{id});
                        return false;
                    },
                }
            }
        }
        if (meta.conf_state.voters_outgoing.len == 0 and meta.conf_state.auto_leave) {
            log.warn("invalid snapshot ConfState: auto-leave requires a joint configuration", .{});
            return false;
        }
        for (meta.conf_state.learners_next) |id| {
            if (member_roles.get(id).? & 4 == 0) {
                log.warn("invalid snapshot ConfState: learner-next {} is not staged correctly", .{id});
                return false;
            }
        }

        const local_roles = member_roles.get(self.id) orelse {
            log.warn("restored snapshot but node id not in ConfState", .{});
            return false;
        };
        if (local_roles & 7 == 0) return false;

        if (self.pending_request_snapshot == invalid_index and
            self.raft_log.matchTerm(meta.index, meta.term) catch false)
        {
            log.info("fast-forwarded commit to snapshot", .{});
            self.raft_log.commitTo(meta.index) catch {};
            return false;
        }

        var restored_tracker = ProgressTracker.init(self.allocator, self.config.max_inflight_messages);
        defer restored_tracker.deinit();

        restoreTracker(&restored_tracker, meta.index + 1, meta.conf_state) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                log.warn("failed to restore tracker from snapshot: {s}", .{@errorName(err)});
                return false;
            },
        };
        var restored_cs = try restored_tracker.conf.toConfState(self.allocator);
        defer restored_cs.deinit(self.allocator);

        self.raft_log.restore(snap_in) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                log.warn("failed to restore raft log from snapshot: {s}", .{@errorName(err)});
                return false;
            },
        };

        // Swap the tracker in.
        const old_tracker = self.progress_tracker;
        self.progress_tracker = restored_tracker;
        // Re-init restored_tracker so the defer'd deinit doesn't double-free;
        // we moved it.
        restored_tracker = ProgressTracker.init(self.allocator, self.config.max_inflight_messages);
        var old = old_tracker;
        old.deinit();

        self.postConfChange(restored_cs);
        self.pending_request_snapshot = invalid_index;
        log.info("restored snapshot at index {}", .{meta.index});
        return true;
    }

    pub fn requestSnapshot(self: *Raft) Error!void {
        if (self.state == .leader) {
            log.debug("node {} cannot request a snapshot while leader", .{self.id});
        } else if (self.leader_id == invalid_id) {
            log.debug("node {} has no leader; dropping snapshot request", .{self.id});
        } else if (self.snapshot() != null or self.pending_request_snapshot != invalid_index) {
            log.debug("node {} already has a pending snapshot", .{self.id});
        } else {
            const request_index = self.raft_log.lastIndex();
            const request_index_term = self.raft_log.term(request_index) catch 0;
            if (self.term == request_index_term) {
                self.pending_request_snapshot = request_index;
                try self.sendRequestSnapshot();
                return;
            }
            log.debug("node {} dropped snapshot request due to term mismatch", .{self.id});
        }
        return error.RequestSnapshotDropped;
    }

    fn sendRequestSnapshot(self: *Raft) Error!void {
        const reject_hint = self.raft_log.lastIndex();
        const lt = self.raft_log.term(reject_hint) catch 0;
        try self.send(.{
            .msg_type = .append_response,
            .index = self.raft_log.committed,
            .reject = true,
            .reject_hint = reject_hint,
            .to = self.leader_id,
            .request_snapshot = self.pending_request_snapshot,
            .log_term = lt,
        });
    }

    // -----------------------------------------------------------------------
    // Sending helpers
    // -----------------------------------------------------------------------

    fn send(self: *Raft, m_in: Message) Error!void {
        var m = m_in;
        if (m.from == invalid_id) m.from = self.id;

        switch (m.msg_type) {
            .request_pre_vote, .request_pre_vote_response, .request_vote, .request_vote_response => {
                if (m.term == 0) @panic("term should be set when sending vote message");
            },
            else => {
                if (m.term != 0) @panic("term should not be set when sending non-vote message");
                if (m.msg_type != .propose and m.msg_type != .read_index) {
                    m.term = self.term;
                }
            },
        }

        if (m.msg_type == .request_vote or m.msg_type == .request_pre_vote) {
            m.priority = self.priority;
        }

        try self.messages.append(self.allocator, m);
    }

    pub fn sendTimeoutNow(self: *Raft, to: u64) Error!void {
        try self.send(.{ .msg_type = .timeout_now, .to = to });
    }

    fn sendAppendById(self: *Raft, to: u64) Error!void {
        const pr = self.progress_tracker.getPtr(to) orelse return;
        try self.sendAppendInternal(to, pr, true);
    }

    fn sendAppend(self: *Raft, to: u64) Error!void {
        const pr = self.progress_tracker.getPtr(to) orelse return;
        try self.sendAppendInternal(to, pr, true);
    }

    fn sendAppendAggressively(self: *Raft, to: u64) Error!void {
        const pr = self.progress_tracker.getPtr(to) orelse return;
        while (try self.maybeSendAppend(to, pr, false)) {}
    }

    fn sendAppendInternal(self: *Raft, to: u64, pr: *Progress, allow_empty: bool) Error!void {
        _ = try self.maybeSendAppend(to, pr, allow_empty);
    }

    fn maybeSendAppend(self: *Raft, to: u64, pr: *Progress, allow_empty: bool) Error!bool {
        if (pr.isPaused()) return false;
        try self.messages.ensureUnusedCapacity(self.allocator, 1);

        var m = Message{ .to = to };

        if (pr.pending_request_snapshot != invalid_index) {
            if (!try self.prepareSendSnapshot(&m, pr, to)) return false;
        } else {
            const ctx = GetEntriesContext{ .send_append = .{
                .to = to,
                .term = self.term,
                .aggressively = !allow_empty,
            } };
            const ents = self.raft_log.getEntries(pr.next_idx, self.max_message_size, ctx) catch |e| switch (e) {
                error.LogTemporarilyUnavailable => return false,
                error.Compacted => {
                    if (!try self.prepareSendSnapshot(&m, pr, to)) return false;
                    try self.send(m);
                    return true;
                },
                else => return e,
            };
            defer {
                for (ents) |*e| e.deinit(self.allocator);
                self.allocator.free(ents);
            }

            if (!allow_empty and ents.len == 0) return false;

            const term_result = self.raft_log.term(pr.next_idx - 1) catch null;
            if (term_result) |t| {
                if (self.batch_append and try self.tryBatching(to, pr, ents)) {
                    return true;
                }
                try self.prepareSendEntries(&m, pr, t, ents);
            } else {
                if (!try self.prepareSendSnapshot(&m, pr, to)) return false;
            }
        }

        try self.send(m);
        return true;
    }

    fn tryBatching(self: *Raft, to: u64, pr: *Progress, new_entries: []const Entry) Error!bool {
        var i: usize = 0;
        while (i < self.messages.items.len) : (i += 1) {
            const msg = &self.messages.items[i];
            if (msg.msg_type == .append and msg.to == to) {
                if (new_entries.len > 0) {
                    if (!util.isContinuousEntries(msg.*, new_entries)) return false;
                    // Append the new entries into the existing message.
                    const combined = try self.allocator.alloc(Entry, msg.entries.len + new_entries.len);
                    var initialized: usize = 0;
                    errdefer {
                        for (combined[0..initialized]) |*entry| entry.deinit(self.allocator);
                        self.allocator.free(combined);
                    }
                    for (msg.entries) |entry| {
                        combined[initialized] = try shareEntry(self.allocator, entry);
                        initialized += 1;
                    }
                    for (new_entries) |entry| {
                        combined[initialized] = try shareEntry(self.allocator, entry);
                        initialized += 1;
                    }
                    for (msg.entries) |*entry| entry.deinit(self.allocator);
                    self.allocator.free(msg.entries);
                    msg.entries = combined;
                    const last_idx = combined[combined.len - 1].index;
                    pr.updateState(last_idx) catch {};
                }
                msg.commit = self.raft_log.committed;
                return true;
            }
        }
        return false;
    }

    fn prepareSendEntries(self: *Raft, msg: *Message, pr: *Progress, term_: u64, entries: []const Entry) Error!void {
        msg.msg_type = .append;
        msg.index = pr.next_idx - 1;
        msg.log_term = term_;
        msg.entries = try storage_mod.shareEntries(self.allocator, entries);
        msg.commit = self.raft_log.committed;
        if (entries.len > 0) pr.updateState(entries[entries.len - 1].index) catch {};
    }

    fn prepareSendSnapshot(self: *Raft, msg: *Message, pr: *Progress, to: u64) Error!bool {
        if (!pr.recent_active) return false;

        msg.msg_type = .snapshot;
        const snap = self.raft_log.getSnapshot(pr.pending_request_snapshot, to) catch |err| switch (err) {
            error.SnapshotTemporarilyUnavailable => return false,
            else => return err,
        };
        if (snap.metadata.index == 0) @panic("need non-empty snapshot");
        pr.becomeSnapshot(snap.metadata.index);
        msg.snapshot = snap;
        return true;
    }

    pub fn broadcastAppend(self: *Raft) Error!void {
        const ids = try self.collectPeerIds();
        defer self.allocator.free(ids);
        for (ids) |id| {
            if (id == self.id) continue;
            const pr = self.progress_tracker.getPtr(id) orelse continue;
            try self.sendAppendInternal(id, pr, true);
        }
    }

    fn broadcastHeartbeat(self: *Raft) Error!void {
        const ctx = self.read_only.lastPendingRequestCtx();
        try self.broadcastHeartbeatWithCtxOpt(ctx);
    }

    fn broadcastHeartbeatWithCtx(self: *Raft, ctx: []const u8) Error!void {
        try self.broadcastHeartbeatWithCtxOpt(ctx);
    }

    fn broadcastHeartbeatWithCtxOpt(self: *Raft, ctx: ?[]const u8) Error!void {
        const ids = try self.collectPeerIds();
        defer self.allocator.free(ids);
        for (ids) |id| {
            if (id == self.id) continue;
            const pr = self.progress_tracker.getPtr(id) orelse continue;
            try self.sendHeartbeat(id, pr, ctx);
        }
    }

    fn sendHeartbeat(self: *Raft, to: u64, pr: *Progress, ctx: ?[]const u8) Error!void {
        var context: []u8 = &.{};
        if (ctx) |c| {
            context = self.allocator.dupe(u8, c) catch return error.OutOfMemory;
        }
        try self.send(.{
            .msg_type = .heartbeat,
            .to = to,
            .commit = @min(pr.matched, self.raft_log.committed),
            .context = context,
        });
    }

    fn collectPeerIds(self: *Raft) Error![]u64 {
        var ids: std.ArrayList(u64) = .empty;
        errdefer ids.deinit(self.allocator);
        var it = self.progress_tracker.progress.map.keyIterator();
        while (it.next()) |k| try ids.append(self.allocator, k.*);
        std.mem.sort(u64, ids.items, {}, std.sort.asc(u64));
        return ids.toOwnedSlice(self.allocator);
    }

    // -----------------------------------------------------------------------
    // AppendEntry (proposals)
    // -----------------------------------------------------------------------

    pub fn appendEntry(self: *Raft, entries: []const Entry) Error!bool {
        if (entries.len == 0) {
            _ = try self.raft_log.append(&.{});
            return true;
        }

        // Re-tag each entry with our current term and the next available index.
        const last_index = self.raft_log.lastIndex();
        const entry_count = std.math.cast(u64, entries.len) orelse return error.Fatal;
        const last_new_index = std.math.add(u64, last_index, entry_count) catch return error.Fatal;
        if (last_new_index == std.math.maxInt(u64)) return error.Fatal;
        const previous_uncommitted_size = self.uncommitted_state.uncommitted_size;
        if (!self.uncommitted_state.maybeIncreaseUncommittedSize(entries)) return false;
        errdefer self.uncommitted_state.uncommitted_size = previous_uncommitted_size;

        const owned = try storage_mod.shareEntries(self.allocator, entries);
        for (owned, 0..) |*entry, i| {
            entry.term = self.term;
            entry.index = last_index + @as(u64, @intCast(i)) + 1;
        }
        defer {
            for (owned) |*e| e.deinit(self.allocator);
            self.allocator.free(owned);
        }
        _ = try self.raft_log.append(owned);
        return true;
    }

    pub fn reduceUncommittedSize(self: *Raft, ents: []const Entry) void {
        if (self.state != .leader) return;
        if (!self.uncommitted_state.maybeReduceUncommittedSize(ents)) {
            log.warn("try to reduce uncommitted size less than 0", .{});
        }
    }

    // -----------------------------------------------------------------------
    // ConfChange
    // -----------------------------------------------------------------------

    pub fn applyConfChange(self: *Raft, cc: ConfChangeV2) Error!ConfState {
        defer invariant.assertRaft(self);
        var changer = ConfChanger.init(&self.progress_tracker);
        var result: ?conf_changer_mod.ConfChangeResult = null;
        defer if (result) |*r| r.deinit(self.allocator);

        if (leaveJoint(cc)) {
            log.info("ApplyConfChange: LeaveJoint", .{});
            result = try changer.leaveJoint();
        } else {
            if (enterJoint(cc)) |auto_leave| {
                log.info("ApplyConfChange: EnterJoint auto_leave={}", .{auto_leave});
                result = try changer.enterJoint(auto_leave, cc.changes);
            } else {
                log.info("ApplyConfChange: Simple num_changes={}", .{cc.changes.len});
                result = try changer.simple(cc.changes);
            }
        }

        const r = result.?;
        var cs = try r.conf.toConfState(self.allocator);
        errdefer cs.deinit(self.allocator);
        try self.progress_tracker.applyConf(r.conf, r.changes, self.raft_log.lastIndex());
        self.postConfChange(cs);
        return cs;
    }

    fn postConfChange(self: *Raft, cs: ConfState) void {
        defer invariant.assertRaft(self);
        log.info("switched to configuration", .{});
        const is_voter = self.progress_tracker.conf.voters.contains(self.id);
        self.promotable = is_voter;

        if (!is_voter and self.state == .leader) return;
        if (self.state != .leader or cs.voters.len == 0) return;

        const committed_now = self.maybeCommit() catch false;
        if (committed_now) {
            self.broadcastAppend() catch {};
        } else {
            const ids = self.collectPeerIds() catch return;
            defer self.allocator.free(ids);
            for (ids) |id| {
                if (id == self.id) continue;
                if (self.progress_tracker.getPtr(id)) |pr| {
                    _ = self.maybeSendAppend(id, pr, false) catch {};
                }
            }
        }

        if (self.read_only.lastPendingRequestCtx()) |ctx| {
            const acks_opt = self.read_only.recvACK(self.id, ctx) catch null;
            if (acks_opt) |acks| {
                if (self.progress_tracker.hasQuorum(acks.*)) {
                    const statuses = self.read_only.advance(ctx) catch &[_]@import("read_only.zig").ReadIndexStatus{};
                    defer self.allocator.free(statuses);
                    for (statuses) |*st| {
                        if (self.handleReadyReadIndexMsg(st.req, st.index) catch null) |resp| {
                            self.send(resp) catch {
                                var owned = resp;
                                owned.deinit(self.allocator);
                            };
                        }
                        var mut = st.*;
                        mut.deinit(self.allocator);
                    }
                }
            }
        }

        if (self.lead_transferee) |lt| {
            if (!self.progress_tracker.conf.voters.contains(lt)) self.abortLeaderTransfer();
        }
    }

    // -----------------------------------------------------------------------
    // State reload & misc
    // -----------------------------------------------------------------------

    pub fn loadState(self: *Raft, hs: HardState) void {
        defer invariant.assertRaft(self);
        if (hs.commit < self.raft_log.committed or hs.commit > self.raft_log.lastIndex()) {
            log.warn(
                "hs.commit {} out of range [{}, {}]",
                .{ hs.commit, self.raft_log.committed, self.raft_log.lastIndex() },
            );
            @panic("hs.commit out of range");
        }
        self.raft_log.committed = hs.commit;
        self.term = hs.term;
        self.vote = hs.vote;
    }

    pub fn commitApply(self: *Raft, applied: u64) void {
        defer invariant.assertRaft(self);
        self.commitApplyInternal(applied, false);
    }

    fn commitApplyInternal(self: *Raft, applied: u64, skip_check: bool) void {
        const old_applied = self.raft_log.applied;
        if (!skip_check) {
            self.raft_log.appliedTo(applied);
        } else {
            std.debug.assert(applied > 0);
            self.raft_log.appliedToUnchecked(applied);
        }

        if (self.progress_tracker.conf.auto_leave and
            old_applied <= self.pending_conf_index and
            applied >= self.pending_conf_index and
            self.state == .leader)
        {
            const data = util.encodeConfChangeV2(self.allocator, .{}) catch {
                @panic("failed to encode auto-leave ConfChangeV2");
            };
            var entry = Entry{ .entry_type = .conf_change_v2 };
            entry.adoptData(self.allocator, data) catch {
                self.allocator.free(data);
                @panic("failed to retain auto-leave ConfChangeV2");
            };
            defer entry.deinit(self.allocator);
            _ = self.appendEntry(&.{entry}) catch {
                @panic("appending an empty EntryConfChangeV2 should never be dropped");
            };
            self.pending_conf_index = self.raft_log.lastIndex();
        }
    }

    pub fn ping(self: *Raft) void {
        if (self.state == .leader) self.broadcastHeartbeat() catch {};
    }

    pub fn setPriority(self: *Raft, p: i64) void {
        self.priority = p;
    }

    pub fn onPersistEntries(self: *Raft, index: u64, term_: u64) Error!void {
        defer invariant.assertRaft(self);
        const update = try self.raft_log.maybePersist(index, term_);
        if (update and self.state == .leader) {
            if (self.term != term_) {
                log.warn("leader persisted term {} != current {}", .{ term_, self.term });
            }
            if (self.progress_tracker.getPtr(self.id)) |pr| {
                if (pr.maybeUpdate(index) and try self.maybeCommit() and self.shouldBroadcastCommit()) {
                    try self.broadcastAppend();
                }
            }
        }
    }

    pub fn onPersistSnapshot(self: *Raft, index: u64) void {
        defer invariant.assertRaft(self);
        _ = self.raft_log.maybePersistSnapshot(index);
    }

    pub fn enableGroupCommit(self: *Raft, enable: bool) Error!void {
        self.progress_tracker.enableGroupCommit(enable);
        if (self.state == .leader and !enable and try self.maybeCommit()) {
            try self.broadcastAppend();
        }
    }

    pub fn assignCommitGroups(self: *Raft, ids: []const [2]u64) Error!void {
        for (ids) |pair| {
            std.debug.assert(pair[1] > 0);
            if (self.progress_tracker.getPtr(pair[0])) |pr| pr.commit_group_id = pair[1];
        }
        if (self.state == .leader and self.progress_tracker.groupCommitEnabled() and try self.maybeCommit()) {
            try self.broadcastAppend();
        }
    }

    pub fn groupCommit(self: *const Raft) bool {
        return self.progress_tracker.group_commit;
    }

    // -----------------------------------------------------------------------
    // Reset & helpers
    // -----------------------------------------------------------------------

    fn reset(self: *Raft, term: u64) void {
        if (self.term != term) {
            self.term = term;
            self.vote = invalid_id;
        }
        self.leader_id = invalid_id;
        self.resetRandomizedElectionTimeout();
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.abortLeaderTransfer();
        self.progress_tracker.resetVotes();
        self.pending_conf_index = 0;

        // Reset ReadOnly queue.
        const opt = self.read_only.option;
        self.read_only.deinit();
        self.read_only = ReadOnly.init(self.allocator, opt);

        // Drop postponed ReadIndex requests: a new term/role cannot serve them.
        for (self.pending_read_index_messages.items) |*m| m.deinit(self.allocator);
        self.pending_read_index_messages.clearRetainingCapacity();

        self.pending_request_snapshot = invalid_index;

        const last_index = self.raft_log.lastIndex();
        const committed = self.raft_log.committed;
        const persisted = self.raft_log.persisted;
        var it = self.progress_tracker.progress.map.valueIterator();
        while (it.next()) |pr| {
            pr.reset(last_index + 1);
        }
        if (self.progress_tracker.getPtr(self.id)) |self_pr| {
            self_pr.matched = persisted;
            self_pr.committed_index = committed;
        }
    }

    fn resetRandomizedElectionTimeout(self: *Raft) void {
        const lo = self.min_election_timeout;
        const hi = self.max_election_timeout;
        // [lo, hi - 1] inclusive.
        const span = if (hi > lo) hi - lo else 1;
        const r = self.prng.random().uintLessThan(usize, span);
        const t = lo + r;
        const prev = self.randomized_election_timeout;
        self.randomized_election_timeout = t;
        log.debug("reset election timeout {} -> {}", .{ prev, t });
    }

    fn abortLeaderTransfer(self: *Raft) void {
        self.lead_transferee = null;
    }
};

// ===========================================================================
// Free helpers
// ===========================================================================

fn voteRespMsgType(mt: MessageType) MessageType {
    return switch (mt) {
        .request_vote => .request_vote_response,
        .request_pre_vote => .request_pre_vote_response,
        else => @panic("not a vote message"),
    };
}

fn leaveJoint(cc: ConfChangeV2) bool {
    return cc.transition == .auto_ and cc.changes.len == 0;
}

fn enterJoint(cc: ConfChangeV2) ?bool {
    if (cc.transition != .auto_ or cc.changes.len > 1) {
        return switch (cc.transition) {
            .auto_, .implicit => true,
            .explicit => false,
        };
    }
    return null;
}

// ===========================================================================
// Smoke tests
// ===========================================================================

const MemoryStorage = @import("memory_storage.zig").MemoryStorage;

fn newThreePeerSetup(allocator: std.mem.Allocator, id: u64) !struct { storage: MemoryStorage, raft: Raft } {
    var storage = MemoryStorage.init();
    var cs = ConfState{ .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }) };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);

    var config = raft_config_mod.defaultConfig();
    config.id = id;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.election_timeout_seed = id * 1000;

    const raft = try Raft.init(allocator, config, storage.asStorage());
    return .{ .storage = storage, .raft = raft };
}

test "raft constructs and starts as follower" {
    const allocator = std.testing.allocator;
    var setup = try newThreePeerSetup(allocator, 1);
    defer setup.storage.deinit(allocator);
    defer setup.raft.deinit();

    try std.testing.expectEqual(StateRole.follower, setup.raft.state);
    try std.testing.expectEqual(@as(u64, 1), setup.raft.id);
    try std.testing.expect(setup.raft.promotable); // node 1 is in voters
}

test "raft single-node self-elects via hup" {
    const allocator = std.testing.allocator;
    var storage = MemoryStorage.init();
    var cs = ConfState{ .voters = try allocator.dupe(u64, &.{1}) };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);

    var config = raft_config_mod.defaultConfig();
    config.id = 1;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.election_timeout_seed = 42;
    var r = try Raft.init(allocator, config, storage.asStorage());
    defer r.deinit();
    defer storage.deinit(allocator);

    // Single-node should immediately win on hup.
    var m = Message{ .msg_type = .hup, .from = 1 };
    try r.step(&m);
    m.deinit(allocator);

    try std.testing.expectEqual(StateRole.leader, r.state);
    try std.testing.expectEqual(@as(u64, 1), r.term);
}
