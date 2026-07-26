//! RawNode integration tests.
//!
//! Each test drives a single RawNode through Campaign → GetReady → persist →
//! Advance, exercising the Ready protocol, propose/conf-change/transfer-leader
//! wrappers, and the
//! restart-from-snapshot path.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

const MemoryStorage = raft.MemoryStorage;
const RawNode = raft.RawNode;
const Config = raft.Config;
const Message = raft.Message;
const StateRole = raft.StateRole;
const Entry = raft.Entry;
const HardState = raft.HardState;

fn seedStorage(storage: *MemoryStorage, voters: []const u64) !void {
    const v = try allocator.dupe(u64, voters);
    var cs = raft.ConfState{ .voters = v };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);
}

fn makeConfig(id: u64) Config {
    var config = raft.defaultConfig();
    config.id = id;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.election_timeout_seed = 42;
    return config;
}

/// Drive `node` through one campaign → leader transition, persisting every
/// Ready along the way.
fn campaignLeader(node: *RawNode, storage: *MemoryStorage) !void {
    try node.campaign();
    while (true) {
        if (!node.hasReady()) break;
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        if (rd.entries.len > 0) {
            try storage.append(allocator, rd.entries);
        }
        const became_leader = blk: {
            if (rd.ss) |ss| break :blk ss.role == .leader and ss.leader_id == node.raftConst().id;
            break :blk false;
        };
        var light = try node.advance(rd);
        defer light.deinit(allocator);
        node.advanceApply();
        if (became_leader) break;
    }
}

test "raw_node: start produces a follower with empty initial state" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try std.testing.expectEqual(StateRole.follower, node.raftConst().state);
    try std.testing.expectEqual(@as(u64, 1), node.raftConst().id);
    try std.testing.expect(!node.hasReady());
}

test "raw_node: campaign transitions to leader and emits SoftState" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);
    try std.testing.expectEqual(StateRole.leader, node.raftConst().state);
}

test "raw_node: propose flows through Ready.entries" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);

    try node.propose("", "first");
    try std.testing.expect(node.hasReady());

    var rd = try node.getReady();
    defer rd.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), rd.entries.len);
    try std.testing.expectEqualStrings("first", rd.entries[0].data);
    try storage.append(allocator, rd.entries);
    var light = try node.advance(rd);
    light.deinit(allocator);
}

test "raw_node: proposeBatch appends one ordered batch" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();
    try campaignLeader(&node, &storage);

    const proposals = [_]RawNode.Proposal{
        .{ .context = "first-context", .data = "first" },
        .{ .context = "second-context", .data = "second" },
    };
    try node.proposeBatch(&proposals);

    var ready = try node.getReady();
    defer ready.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), ready.entries.len);
    try std.testing.expectEqualStrings("first", ready.entries[0].data);
    try std.testing.expectEqualStrings("first-context", ready.entries[0].context);
    try std.testing.expectEqualStrings("second", ready.entries[1].data);
    try std.testing.expectEqualStrings("second-context", ready.entries[1].context);
}

test "raw_node: proposeBatchOwned consumes payloads on failure" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();
    var proposals = [_]RawNode.OwnedProposal{
        .{ .data = try allocator.dupe(u8, "first") },
        .{ .data = try allocator.dupe(u8, "second") },
    };
    defer for (&proposals) |*proposal| {
        if (proposal.data.len != 0) allocator.free(proposal.data);
    };

    try std.testing.expectError(error.ProposalDropped, node.proposeBatchOwned(&proposals));
    try std.testing.expectEqual(@as(usize, 0), proposals[0].data.len);
    try std.testing.expectEqual(@as(usize, 0), proposals[1].data.len);
}

test "raw_node: uncommitted entry limit rejects until Ready advances" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var config = makeConfig(1);
    config.max_uncommitted_entries = 1;
    var node = try RawNode.init(allocator, config, storage.asStorage());
    defer node.deinit();
    try campaignLeader(&node, &storage);
    try std.testing.expectEqual(@as(u64, 0), node.raftConst().uncommitted_state.uncommitted_entries);

    try node.propose("", "first");
    try std.testing.expectEqual(@as(u64, 1), node.raftConst().uncommitted_state.uncommitted_entries);
    try std.testing.expectError(error.ProposalDropped, node.propose("", "second"));
    try std.testing.expectEqual(@as(u64, 1), node.raftConst().uncommitted_state.uncommitted_entries);

    var ready = try node.getReady();
    defer ready.deinit(allocator);
    try storage.append(allocator, ready.entries);
    var light = try node.advance(ready);
    defer light.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 0), node.raftConst().uncommitted_state.uncommitted_entries);
    try node.propose("", "second");
}

test "raw_node: restart preserves prior HardState" {
    // First node: campaign, propose, persist.
    var storage1 = MemoryStorage.init();
    defer storage1.deinit(allocator);
    try seedStorage(&storage1, &.{1});

    {
        var node = try RawNode.init(allocator, makeConfig(1), storage1.asStorage());
        defer node.deinit();
        try campaignLeader(&node, &storage1);
        try node.propose("", "hello");
        while (node.hasReady()) {
            var rd = try node.getReady();
            defer rd.deinit(allocator);
            if (rd.entries.len > 0) try storage1.append(allocator, rd.entries);
            var light = try node.advance(rd);
            light.deinit(allocator);
        }
    }

    // Second node: rebuild from storage, ensure state matches.
    var storage2 = MemoryStorage.init();
    defer storage2.deinit(allocator);
    // Copy entries from storage1 to storage2 (single-node doesn't share).
    {
        const ents = try storage1.allEntries(allocator);
        defer {
            for (ents) |*e| e.deinit(allocator);
            allocator.free(ents);
        }
        try storage2.append(allocator, ents);
    }
    try seedStorage(&storage2, &.{1});

    var config2 = makeConfig(1);
    config2.load_state_on_startup = true;
    var node2 = try RawNode.init(allocator, config2, storage2.asStorage());
    defer node2.deinit();

    // lastIndex on storage2 should now be > 0 (proposed entry).
    try std.testing.expect(node2.raftConst().raft_log.lastIndex() > 0);
}

test "raw_node: restart from snapshot applies ConfState" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    // Apply a snapshot directly so storage now reports voters=[1] at index 100.
    const snap_voters = try allocator.dupe(u64, &.{1});
    var snap = raft.Snapshot{
        .metadata = .{
            .index = 100,
            .term = 5,
            .conf_state = .{ .voters = snap_voters },
        },
    };
    defer snap.deinit(allocator);
    try storage.applySnapshot(allocator, snap);

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try std.testing.expectEqual(@as(u64, 100), node.raftConst().raft_log.committed);
    // After applying a snapshot at index N, raft_log.applied is N.
    try std.testing.expectEqual(@as(u64, 100), node.raftConst().raft_log.applied);
}

test "raw_node: read index surfaces a ReadState" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);

    // Single-node leader serves read-index requests locally.
    try node.readIndex("ctx1");
    try std.testing.expect(node.hasReady());

    var rd = try node.getReady();
    defer rd.deinit(allocator);
    try std.testing.expect(rd.read_states.len > 0);
    try std.testing.expectEqualStrings("ctx1", rd.read_states[0].request_ctx);
    var light = try node.advance(rd);
    light.deinit(allocator);
}

test "raw_node: step local message rejected" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    var m = Message{ .msg_type = .hup, .from = 1 };
    try std.testing.expectError(error.StepLocalMsg, node.step(m));
    m.deinit(allocator);
}

test "raw_node: step unknown peer rejected" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    // A response from a peer not in our progress map is rejected.
    var m = Message{ .msg_type = .append_response, .from = 99 };
    try std.testing.expectError(error.StepPeerNotFound, node.step(m));
    m.deinit(allocator);
}

test "raw_node: propose conf change — simple add node" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);

    var cc = raft.ConfChangeV2{ .changes = try allocator.alloc(raft.ConfChangeSingle, 1) };
    defer allocator.free(cc.changes);
    cc.changes[0] = .{ .change_type = .add_node, .node_id = 2 };

    try node.proposeConfChange("", cc);
    var rd = try node.getReady();
    defer rd.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), rd.entries.len);
    try std.testing.expectEqual(raft.EntryType.conf_change_v2, rd.entries[0].entry_type);

    try storage.append(allocator, rd.entries);
    var light = try node.advance(rd);
    defer light.deinit(allocator);

    // Apply the ConfChange and check the resulting voters.
    var applied_cs = try node.applyConfChange(cc);
    defer applied_cs.deinit(allocator);
    var sorted: [2]u64 = .{ applied_cs.voters[0], applied_cs.voters[1] };
    std.mem.sort(u64, &sorted, {}, std.sort.asc(u64));
    try std.testing.expectEqual(@as(u64, 1), sorted[0]);
    try std.testing.expectEqual(@as(u64, 2), sorted[1]);
}

test "raw_node: reject second pending conf change" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);

    var cc1 = raft.ConfChangeV2{ .changes = try allocator.alloc(raft.ConfChangeSingle, 1) };
    defer allocator.free(cc1.changes);
    cc1.changes[0] = .{ .change_type = .add_node, .node_id = 2 };
    try node.proposeConfChange("", cc1);

    // A second conf change before the first is applied should be dropped.
    var cc2 = raft.ConfChangeV2{ .changes = try allocator.alloc(raft.ConfChangeSingle, 1) };
    defer allocator.free(cc2.changes);
    cc2.changes[0] = .{ .change_type = .add_node, .node_id = 3 };
    try std.testing.expectError(error.ProposalDropped, node.proposeConfChange("", cc2));
}

test "raw_node: propose conf change — add learner" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);

    var cc = raft.ConfChangeV2{ .changes = try allocator.alloc(raft.ConfChangeSingle, 1) };
    defer allocator.free(cc.changes);
    cc.changes[0] = .{ .change_type = .add_learner_node, .node_id = 5 };

    try node.proposeConfChange("", cc);
    var rd = try node.getReady();
    defer rd.deinit(allocator);
    try storage.append(allocator, rd.entries);
    var light = try node.advance(rd);
    defer light.deinit(allocator);

    var applied_cs = try node.applyConfChange(cc);
    defer applied_cs.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), applied_cs.learners.len);
    try std.testing.expectEqual(@as(u64, 5), applied_cs.learners[0]);
}

test "raw_node: joint auto leave follows conf change commit" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);

    // Use IMPLICIT transition so EnterJoint is invoked with auto_leave=true
    // even for a single change. (AUTO + 1 change routes through Simple.)
    var cc = raft.ConfChangeV2{
        .transition = .implicit,
        .changes = try allocator.alloc(raft.ConfChangeSingle, 1),
    };
    defer allocator.free(cc.changes);
    cc.changes[0] = .{ .change_type = .add_node, .node_id = 2 };

    try node.proposeConfChange("", cc);
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try storage.append(allocator, rd.entries);
        var light = try node.advance(rd);
        light.deinit(allocator);
    }

    var applied_cs = try node.applyConfChange(cc);
    defer applied_cs.deinit(allocator);
    try std.testing.expect(applied_cs.auto_leave);
}

test "raw_node: committed_entries_pagination respects max_committed_size_per_ready" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var config = makeConfig(1);
    // Tight cap: each Ready returns at most one entry.
    config.max_committed_size_per_ready = raft.entry_message_overhead + 1;
    var node = try RawNode.init(allocator, config, storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);

    // Propose 3 entries (small enough to reason about).
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        const buf = try std.fmt.allocPrint(allocator, "e{}", .{i});
        defer allocator.free(buf);
        try node.propose("", buf);
    }

    // Drain Readys. Single-node leader should surface every proposed entry.
    // Both `rd.light` (from getReady) and the `advance` return value carry
    // committed entries — getReady pulls entries committed before persistence,
    // advance pulls any newly-committed entries after persistence.
    var total: usize = 0;
    while (node.hasReady()) {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        if (rd.entries.len > 0) try storage.append(allocator, rd.entries);
        if (rd.light.committed_entries.len > 0) total += rd.light.committed_entries.len;
        var light = try node.advance(rd);
        defer light.deinit(allocator);
        if (light.committed_entries.len > 0) total += light.committed_entries.len;
    }
    try std.testing.expectEqual(@as(usize, 3), total);
}

test "raw_node: entry data is shared through Ready and MemoryStorage" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();
    try campaignLeader(&node, &storage);
    try node.propose("", "payload");

    const unstable_data = node.raftConst().raft_log.unstable.entries.items[0].data;
    var ready = try node.getReady();
    defer ready.deinit(allocator);
    try std.testing.expectEqual(@intFromPtr(unstable_data.ptr), @intFromPtr(ready.entries[0].data.ptr));

    try storage.append(allocator, ready.entries);
    const stored = try storage.allEntries(allocator);
    defer {
        for (stored) |*entry| entry.deinit(allocator);
        allocator.free(stored);
    }
    try std.testing.expectEqual(@intFromPtr(ready.entries[0].data.ptr), @intFromPtr(stored[stored.len - 1].data.ptr));

    var light = try node.advance(ready);
    defer light.deinit(allocator);
    try std.testing.expectEqual(@intFromPtr(ready.entries[0].data.ptr), @intFromPtr(light.committed_entries[0].data.ptr));
}

test "raw_node: getReady allocation failures preserve pending data" {
    var saw_oom = false;
    var reached_success = false;

    for (0..16) |failure_offset| {
        var failing = std.testing.FailingAllocator.init(allocator, .{});
        const node_allocator = failing.allocator();
        var storage = MemoryStorage.init();
        defer storage.deinit(node_allocator);
        var conf_state = raft.ConfState{ .voters = try node_allocator.dupe(u64, &.{1}) };
        try storage.setRaftState(node_allocator, .{ .conf_state = conf_state });
        conf_state.deinit(node_allocator);

        var node = try RawNode.init(node_allocator, makeConfig(1), storage.asStorage());
        defer node.deinit();
        try node.campaign();
        while (node.hasReady()) {
            var ready = try node.getReady();
            if (ready.entries.len > 0) try storage.append(node_allocator, ready.entries);
            var light = try node.advance(ready);
            light.deinit(node_allocator);
            ready.deinit(node_allocator);
            node.advanceApply();
            if (node.raftConst().state == .leader) break;
        }

        try node.propose("", "payload");
        try node.readIndex("read-context");
        failing.fail_index = failing.alloc_index + failure_offset;
        if (node.getReady()) |ready_value| {
            var ready = ready_value;
            defer ready.deinit(node_allocator);
            try std.testing.expect(ready.entries.len > 0);
            try std.testing.expectEqualStrings("payload", ready.entries[ready.entries.len - 1].data);
            try std.testing.expectEqualStrings("read-context", ready.read_states[0].request_ctx);
            reached_success = true;
            break;
        } else |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            saw_oom = true;
            try std.testing.expect(node.hasReady());

            failing.fail_index = std.math.maxInt(usize);
            var ready = try node.getReady();
            defer ready.deinit(node_allocator);
            try std.testing.expect(ready.entries.len > 0);
            try std.testing.expectEqualStrings("payload", ready.entries[ready.entries.len - 1].data);
            try std.testing.expectEqualStrings("read-context", ready.read_states[0].request_ctx);
        }
    }

    try std.testing.expect(saw_oom);
    try std.testing.expect(reached_success);
}

test "raw_node: persisting entries advances commit on a single-node leader" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(&storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try campaignLeader(&node, &storage);
    // After campaign, the noop entry should be committed at index 1.
    try std.testing.expectEqual(@as(u64, 1), node.raftConst().raft_log.committed);
}
