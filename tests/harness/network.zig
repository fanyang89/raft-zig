//! Deterministic simulated network for Raft integration tests.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;
const default_step_limit: usize = 10_000;

const Raft = raft.Raft;
const MemoryStorage = raft.MemoryStorage;
const Message = raft.Message;
const MessageType = raft.MessageType;

const Link = struct {
    from: u64,
    to: u64,
};

pub const Delivery = enum {
    delivered,
    dropped,
    unknown_target,
};

pub const LogicalDigest = struct {
    term: u64,
    leader_id: u64,
    committed: u64,
    last_index: u64,
    log_hash: u64,
    conf_hash: u64,

    pub fn eql(self: LogicalDigest, other: LogicalDigest) bool {
        return std.meta.eql(self, other);
    }
};

pub const Peer = struct {
    raft: Raft,
    storage: MemoryStorage,

    pub fn deinit(self: *Peer) void {
        self.raft.deinit();
        self.storage.deinit(allocator);
    }
};

pub const Network = struct {
    peers: std.AutoHashMap(u64, *Peer),
    ignored: std.AutoHashMap(MessageType, void),
    blocked: std.AutoHashMap(Link, void),
    observed_leaders: std.AutoHashMap(u64, u64),
    pending: std.ArrayList(Message),
    step_count: usize,

    pub fn init() Network {
        return .{
            .peers = std.AutoHashMap(u64, *Peer).init(allocator),
            .ignored = std.AutoHashMap(MessageType, void).init(allocator),
            .blocked = std.AutoHashMap(Link, void).init(allocator),
            .observed_leaders = std.AutoHashMap(u64, u64).init(allocator),
            .pending = .empty,
            .step_count = 0,
        };
    }

    pub fn deinit(self: *Network) void {
        for (self.pending.items) |*message| message.deinit(allocator);
        self.pending.deinit(allocator);
        var it = self.peers.valueIterator();
        while (it.next()) |peer| {
            peer.*.deinit();
            allocator.destroy(peer.*);
        }
        self.peers.deinit();
        self.ignored.deinit();
        self.blocked.deinit();
        self.observed_leaders.deinit();
        self.* = undefined;
    }

    pub fn ignoreMessageType(self: *Network, message_type: MessageType) !void {
        try self.ignored.put(message_type, {});
    }

    pub fn clearIgnored(self: *Network) void {
        self.ignored.clearRetainingCapacity();
    }

    pub fn cut(self: *Network, one: u64, other: u64) !void {
        try self.blocked.ensureUnusedCapacity(2);
        self.blocked.putAssumeCapacity(.{ .from = one, .to = other }, {});
        self.blocked.putAssumeCapacity(.{ .from = other, .to = one }, {});
    }

    pub fn isolate(self: *Network, id: u64) !void {
        try self.blocked.ensureUnusedCapacity(self.peers.count() * 2);
        var it = self.peers.keyIterator();
        while (it.next()) |peer_id| {
            if (peer_id.* == id) continue;
            self.blocked.putAssumeCapacity(.{ .from = id, .to = peer_id.* }, {});
            self.blocked.putAssumeCapacity(.{ .from = peer_id.*, .to = id }, {});
        }
    }

    pub fn recover(self: *Network) void {
        self.blocked.clearRetainingCapacity();
    }

    pub fn getPeer(self: *Network, id: u64) ?*Peer {
        return self.peers.get(id);
    }

    pub fn pendingCount(self: *const Network) usize {
        return self.pending.items.len;
    }

    pub fn enqueue(self: *Network, messages: []const Message) !void {
        var cloned: std.ArrayList(Message) = .empty;
        defer {
            for (cloned.items) |*message| message.deinit(allocator);
            cloned.deinit(allocator);
        }
        try cloned.ensureTotalCapacity(allocator, messages.len);
        for (messages) |message| {
            cloned.appendAssumeCapacity(try cloneMessage(allocator, message));
        }
        try self.pending.ensureUnusedCapacity(allocator, cloned.items.len);
        for (cloned.items) |message| self.pending.appendAssumeCapacity(message);
        cloned.clearRetainingCapacity();
    }

    pub fn dropPending(self: *Network, index: usize) !void {
        if (index >= self.pending.items.len) return error.InvalidPendingIndex;
        var message = self.pending.orderedRemove(index);
        message.deinit(allocator);
        self.step_count +|= 1;
        try self.checkSafety();
    }

    pub fn deliverOne(self: *Network) !?Delivery {
        if (self.pending.items.len == 0) return null;
        return try self.deliverAt(self.pending.items.len - 1);
    }

    pub fn deliverAt(self: *Network, index: usize) !Delivery {
        if (index >= self.pending.items.len) return error.InvalidPendingIndex;
        var message = self.pending.orderedRemove(index);
        defer message.deinit(allocator);
        self.step_count +|= 1;

        const target = routeTarget(message) orelse {
            try self.checkSafety();
            return .unknown_target;
        };
        if (self.isBlocked(message)) {
            try self.checkSafety();
            return .dropped;
        }

        const peer = self.peers.get(target) orelse {
            try self.checkSafety();
            return .unknown_target;
        };
        peer.raft.step(&message) catch |err| switch (err) {
            error.ProposalDropped, error.RequestSnapshotDropped => {},
            else => return err,
        };
        try persistPeer(peer);
        try self.collectOutput(peer);
        try self.checkSafety();
        return .delivered;
    }

    pub fn tickPeer(self: *Network, id: u64) !bool {
        const peer = self.peers.get(id) orelse return error.UnknownPeer;
        const has_ready = try peer.raft.tick();
        try persistPeer(peer);
        try self.collectOutput(peer);
        self.step_count +|= 1;
        try self.checkSafety();
        return has_ready;
    }

    pub fn stepLocal(self: *Network, target: u64, input: Message) !void {
        const peer = self.peers.get(target) orelse return error.UnknownPeer;
        var message = try cloneMessage(allocator, input);
        defer message.deinit(allocator);
        peer.raft.step(&message) catch |err| switch (err) {
            error.ProposalDropped, error.RequestSnapshotDropped => {},
            else => return err,
        };
        try persistPeer(peer);
        try self.collectOutput(peer);
        self.step_count +|= 1;
        try self.checkSafety();
    }

    pub fn runUntilIdle(self: *Network, max_steps: usize) !usize {
        var steps: usize = 0;
        while (self.pending.items.len > 0) {
            if (steps == max_steps) return error.StepLimitExceeded;
            _ = try self.deliverOne();
            steps += 1;
        }
        return steps;
    }

    pub fn send(self: *Network, messages: []const Message) !void {
        try self.enqueue(messages);
        _ = try self.runUntilIdle(default_step_limit);
    }

    pub fn checkSafety(self: *Network) !void {
        var peers_it = self.peers.valueIterator();
        while (peers_it.next()) |peer_ptr| {
            const peer = peer_ptr.*;
            if (peer.storage.core.snapshot_metadata.index != 0) return error.SnapshotSafetyUnsupported;
            if (raft.checkRaftInvariants(&peer.raft)) |violation| {
                std.log.err(
                    "node {} invariant failed: {s}, role={s}, term={}, peer={}, expected={}, actual={}",
                    .{
                        peer.raft.id,
                        @tagName(violation.kind),
                        @tagName(peer.raft.state),
                        peer.raft.term,
                        violation.peer_id,
                        violation.expected,
                        violation.actual,
                    },
                );
                return error.RaftInvariantViolation;
            }
            if (peer.raft.state == .leader) {
                const observed = try self.observed_leaders.getOrPut(peer.raft.term);
                if (observed.found_existing and observed.value_ptr.* != peer.raft.id) {
                    std.log.err(
                        "term {} has leaders {} and {}",
                        .{ peer.raft.term, observed.value_ptr.*, peer.raft.id },
                    );
                    return error.ElectionSafetyViolation;
                }
                observed.value_ptr.* = peer.raft.id;
            }
        }

        var left_it = self.peers.valueIterator();
        while (left_it.next()) |left_ptr| {
            var right_it = self.peers.valueIterator();
            while (right_it.next()) |right_ptr| {
                if (left_ptr.*.raft.id >= right_ptr.*.raft.id) continue;
                try checkCommittedOverlap(left_ptr.*, right_ptr.*);
            }
        }
    }

    pub fn convergenceDigest(self: *Network) !LogicalDigest {
        if (self.pending.items.len != 0) return error.NetworkNotIdle;

        var first: ?*Peer = null;
        var leader: ?*Peer = null;
        var peers_it = self.peers.valueIterator();
        while (peers_it.next()) |peer_ptr| {
            const peer = peer_ptr.*;
            if (first == null) first = peer;
            if (peer.raft.state == .leader) {
                if (leader != null) return error.MultipleLeaders;
                leader = peer;
            }
            if (peer.raft.raft_log.unstable.entries.items.len != 0 or
                peer.raft.raft_log.unstable.snapshot != null or
                peer.raft.raft_log.persisted != peer.raft.raft_log.lastIndex() or
                peer.raft.messages.items.len != 0 or
                peer.raft.read_states.items.len != 0 or
                peer.raft.read_only.pendingReadCount() != 0 or
                peer.raft.lead_transferee != null or
                peer.raft.pending_request_snapshot != raft.invalid_index)
            {
                return error.PersistenceNotIdle;
            }
        }

        const baseline = first orelse return error.EmptyNetwork;
        const current_leader = leader orelse return error.NoLeader;
        const expected_term = baseline.raft.term;
        const expected_commit = baseline.raft.raft_log.committed;
        const expected_last = baseline.raft.raft_log.lastIndex();
        var expected_conf = try baseline.raft.progress_tracker.conf.toConfState(allocator);
        defer expected_conf.deinit(allocator);

        peers_it = self.peers.valueIterator();
        while (peers_it.next()) |peer_ptr| {
            const peer = peer_ptr.*;
            if (peer.raft.term != expected_term or
                peer.raft.leader_id != current_leader.raft.id or
                peer.raft.raft_log.committed != expected_commit or
                peer.raft.raft_log.lastIndex() != expected_last)
            {
                return error.ClusterNotConverged;
            }
            var conf = try peer.raft.progress_tracker.conf.toConfState(allocator);
            defer conf.deinit(allocator);
            if (!expected_conf.eql(conf)) return error.ConfigurationNotConverged;
            try checkLogsEqual(baseline, peer);
        }

        var progress_it = current_leader.raft.progress_tracker.progress.map.valueIterator();
        while (progress_it.next()) |progress| {
            if (progress.matched != expected_last or progress.inflights.count != 0 or progress.state == .snapshot) {
                return error.ProgressNotConverged;
            }
        }

        return .{
            .term = expected_term,
            .leader_id = current_leader.raft.id,
            .committed = expected_commit,
            .last_index = expected_last,
            .log_hash = hashEntries(baseline.storage.core.entries.items),
            .conf_hash = hashConf(expected_conf),
        };
    }

    pub fn converge(self: *Network, max_rounds: usize, max_steps: usize) !LogicalDigest {
        self.recover();
        self.clearIgnored();
        _ = try self.runUntilIdle(max_steps);

        var round: usize = 0;
        while (round < max_rounds) : (round += 1) {
            if (self.highestTermLeader()) |leader| {
                leader.raft.ping();
                try self.collectOutput(leader);
                _ = try self.runUntilIdle(max_steps);
                if (self.convergenceDigest()) |before| {
                    leader.raft.ping();
                    try self.collectOutput(leader);
                    _ = try self.runUntilIdle(max_steps);
                    const after = self.convergenceDigest() catch |err| {
                        if (isTransientConvergenceError(err)) continue;
                        return err;
                    };
                    if (!before.eql(after)) return error.FixedPointChanged;
                    return after;
                } else |err| {
                    if (!isTransientConvergenceError(err)) return err;
                }
            }

            const ids = try self.sortedPeerIds();
            defer allocator.free(ids);
            for (ids) |id| _ = try self.tickPeer(id);
            _ = try self.runUntilIdle(max_steps);
        }
        return error.ConvergenceTimeout;
    }

    fn isBlocked(self: *const Network, message: Message) bool {
        if (self.ignored.contains(message.msg_type)) return true;
        if (message.to == 0) return false;
        return self.blocked.contains(.{ .from = message.from, .to = message.to });
    }

    fn collectOutput(self: *Network, peer: *Peer) !void {
        try self.pending.ensureUnusedCapacity(allocator, peer.raft.messages.items.len);
        for (peer.raft.messages.items) |message| self.pending.appendAssumeCapacity(message);
        peer.raft.messages.clearRetainingCapacity();
    }

    fn highestTermLeader(self: *Network) ?*Peer {
        var result: ?*Peer = null;
        var it = self.peers.valueIterator();
        while (it.next()) |peer_ptr| {
            const peer = peer_ptr.*;
            if (peer.raft.state != .leader) continue;
            if (result == null or peer.raft.term > result.?.raft.term) result = peer;
        }
        return result;
    }

    fn sortedPeerIds(self: *Network) ![]u64 {
        const ids = try allocator.alloc(u64, self.peers.count());
        var i: usize = 0;
        var it = self.peers.keyIterator();
        while (it.next()) |id| : (i += 1) ids[i] = id.*;
        std.mem.sort(u64, ids, {}, std.sort.asc(u64));
        return ids;
    }
};

fn routeTarget(message: Message) ?u64 {
    if (message.to != 0) return message.to;
    return switch (message.msg_type) {
        .hup, .beat, .propose, .check_quorum, .read_index => message.from,
        else => null,
    };
}

fn isTransientConvergenceError(err: anyerror) bool {
    return switch (err) {
        error.NoLeader,
        error.MultipleLeaders,
        error.ClusterNotConverged,
        error.ConfigurationNotConverged,
        error.LogNotConverged,
        error.ProgressNotConverged,
        => true,
        else => false,
    };
}

fn cloneMessage(alloc: std.mem.Allocator, src: Message) !Message {
    var entries = try alloc.alloc(raft.Entry, src.entries.len);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    for (src.entries) |entry| {
        entries[initialized] = try raft.cloneEntry(alloc, entry);
        initialized += 1;
    }

    var snapshot: ?raft.Snapshot = null;
    if (src.snapshot) |value| snapshot = try raft.cloneSnapshot(alloc, value);
    errdefer if (snapshot) |*value| value.deinit(alloc);

    const context: []u8 = if (src.context.len == 0) &.{} else try alloc.dupe(u8, src.context);
    errdefer if (context.len != 0) alloc.free(context);

    return .{
        .msg_type = src.msg_type,
        .to = src.to,
        .from = src.from,
        .term = src.term,
        .log_term = src.log_term,
        .index = src.index,
        .entries = entries,
        .commit = src.commit,
        .commit_term = src.commit_term,
        .snapshot = snapshot,
        .request_snapshot = src.request_snapshot,
        .reject = src.reject,
        .reject_hint = src.reject_hint,
        .context = context,
        .priority = src.priority,
    };
}

fn persistPeer(peer: *Peer) !void {
    const raft_node = &peer.raft;
    if (raft_node.raft_log.unstable.snapshot) |snapshot| {
        const index = snapshot.metadata.index;
        try peer.storage.applySnapshot(allocator, snapshot);
        raft_node.raft_log.stableSnapshot(index);
        raft_node.onPersistSnapshot(index);
    }

    const unstable_entries = raft_node.raft_log.unstable.entries.items;
    if (unstable_entries.len != 0) {
        try peer.storage.append(allocator, unstable_entries);
        const last = unstable_entries[unstable_entries.len - 1];
        raft_node.raft_log.stableEntries(last.index, last.term);
        try raft_node.onPersistEntries(last.index, last.term);
    }
    try peer.storage.setHardState(raft_node.hardState());
}

fn checkCommittedOverlap(left: *Peer, right: *Peer) !void {
    const high = @min(left.raft.raft_log.committed, right.raft.raft_log.committed);
    if (high == 0) return;

    const low = @max(left.storage.core.firstIndex(), right.storage.core.firstIndex());
    if (low > high) return;
    var index = low;
    while (true) {
        const left_entry = entryAt(left, index) orelse return error.MissingCommittedEntry;
        const right_entry = entryAt(right, index) orelse return error.MissingCommittedEntry;
        if (!entriesEqual(left_entry, right_entry)) {
            std.log.err("committed entry {} differs on nodes {} and {}", .{ index, left.raft.id, right.raft.id });
            return error.CommittedLogViolation;
        }
        if (index == high) break;
        index += 1;
    }
}

fn checkLogsEqual(left: *Peer, right: *Peer) !void {
    const left_entries = left.storage.core.entries.items;
    const right_entries = right.storage.core.entries.items;
    if (left_entries.len != right_entries.len) return error.LogNotConverged;
    for (left_entries, right_entries) |left_entry, right_entry| {
        if (!entriesEqual(left_entry, right_entry)) return error.LogNotConverged;
    }
}

fn entryAt(peer: *Peer, index: u64) ?raft.Entry {
    const entries = peer.storage.core.entries.items;
    if (entries.len == 0 or index < entries[0].index) return null;
    const offset = index - entries[0].index;
    if (offset >= entries.len) return null;
    return entries[offset];
}

fn entriesEqual(left: raft.Entry, right: raft.Entry) bool {
    return left.entry_type == right.entry_type and
        left.term == right.term and
        left.index == right.index and
        left.checksum == right.checksum and
        std.mem.eql(u8, left.data, right.data) and
        std.mem.eql(u8, left.context, right.context);
}

fn hashEntries(entries: []const raft.Entry) u64 {
    var hash = std.hash.Wyhash.init(0);
    hashLength(&hash, entries.len);
    for (entries) |entry| {
        hash.update(std.mem.asBytes(&entry.entry_type));
        hash.update(std.mem.asBytes(&entry.term));
        hash.update(std.mem.asBytes(&entry.index));
        hash.update(std.mem.asBytes(&entry.checksum));
        hashLength(&hash, entry.data.len);
        hash.update(entry.data);
        hashLength(&hash, entry.context.len);
        hash.update(entry.context);
    }
    return hash.final();
}

fn hashConf(conf: raft.ConfState) u64 {
    var hash = std.hash.Wyhash.init(0);
    hashLength(&hash, conf.voters.len);
    hash.update(std.mem.sliceAsBytes(conf.voters));
    hashLength(&hash, conf.learners.len);
    hash.update(std.mem.sliceAsBytes(conf.learners));
    hashLength(&hash, conf.voters_outgoing.len);
    hash.update(std.mem.sliceAsBytes(conf.voters_outgoing));
    hashLength(&hash, conf.learners_next.len);
    hash.update(std.mem.sliceAsBytes(conf.learners_next));
    hash.update(std.mem.asBytes(&conf.auto_leave));
    return hash.final();
}

fn hashLength(hash: *std.hash.Wyhash, len: usize) void {
    const value: u64 = @intCast(len);
    hash.update(std.mem.asBytes(&value));
}

fn createPeer(id: u64, peer_ids: []const u64) !*Peer {
    const peer = try allocator.create(Peer);
    errdefer allocator.destroy(peer);
    peer.storage = MemoryStorage.init();
    errdefer peer.storage.deinit(allocator);

    const voters = try allocator.dupe(u64, peer_ids);
    var conf_state = raft.ConfState{ .voters = voters };
    defer conf_state.deinit(allocator);
    try peer.storage.setRaftState(allocator, .{ .conf_state = conf_state });

    var config = raft.defaultConfig();
    config.id = id;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.election_timeout_seed = id *% 0xDEAD;
    peer.raft = try Raft.init(allocator, config, peer.storage.asStorage());
    return peer;
}

pub fn newNetwork(peer_ids: []const u64) !Network {
    var network = Network.init();
    errdefer network.deinit();

    for (peer_ids) |id| {
        if (network.peers.contains(id)) return error.DuplicatePeer;
        const peer = try createPeer(id, peer_ids);
        network.peers.put(id, peer) catch |err| {
            peer.deinit();
            allocator.destroy(peer);
            return err;
        };
    }
    try network.checkSafety();
    return network;
}
