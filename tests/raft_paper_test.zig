//! Raft paper test scenarios.
//!
//! The formal Raft safety properties (Section 5 of the paper): term handling,
//! vote granting, quorum commit, and election timeout distribution.

const std = @import("std");
const raft = @import("raft_zig");
const network_mod = @import("harness/network.zig");

const allocator = std.testing.allocator;
const MemoryStorage = raft.MemoryStorage;
const Config = raft.Config;
const StateRole = raft.StateRole;
const Entry = raft.Entry;
const Message = raft.Message;
const MessageType = raft.MessageType;

fn makeConfig(id: u64) Config {
    var c = raft.defaultConfig();
    c.id = id;
    c.election_tick = 10;
    c.heartbeat_tick = 1;
    c.election_timeout_seed = id * 17;
    return c;
}

fn newStorage(voters: []const u64) !MemoryStorage {
    var storage = MemoryStorage.init();
    const v = try allocator.dupe(u64, voters);
    var cs = raft.ConfState{ .voters = v };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);
    return storage;
}

fn freeMessages(node: *raft.Raft) void {
    for (node.messages.items) |*m| m.deinit(allocator);
    node.messages.clearRetainingCapacity();
}

// ===========================================================================
// Term update (Section 5.1)
// ===========================================================================

test "paper: follower updates term from higher-term message" {
    var storage = try newStorage(&.{1});
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    node.term = 1;
    try std.testing.expectEqual(StateRole.follower, node.state);

    // Receive an append from a higher-term leader.
    var msg = Message{ .msg_type = .append, .to = 1, .from = 2, .term = 3 };
    try node.step(&msg);
    msg.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 3), node.term);
    try std.testing.expectEqual(StateRole.follower, node.state);
    try std.testing.expectEqual(@as(u64, 2), node.leader_id);
}

test "paper: candidate falls back on higher-term append" {
    var storage = try newStorage(&.{ 1, 2, 3 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    node.term = 2;
    node.state = .candidate;

    // Higher-term append causes candidate → follower.
    var msg = Message{ .msg_type = .append, .to = 1, .from = 2, .term = 5 };
    try node.step(&msg);
    msg.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 5), node.term);
    try std.testing.expectEqual(StateRole.follower, node.state);
}

// ===========================================================================
// Vote granting (Section 5.4.1)
// ===========================================================================

test "paper: voter rejects stale-term vote request" {
    var storage = try newStorage(&.{ 1, 2, 3 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    var entries = [_]Entry{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 2 },
        .{ .index = 3, .term = 3 },
    };
    try storage.append(allocator, &entries);
    _ = try node.raft_log.append(&entries);
    node.term = 3;

    // Vote request from term 2 (stale, lower than our term 3) → silently
    // ignored (no response sent). Raft doesn't respond to stale-term votes.
    var msg = Message{
        .msg_type = .request_vote,
        .to = 1,
        .from = 2,
        .term = 2,
        .log_term = 2,
        .index = 2,
    };
    try node.step(&msg);
    msg.deinit(allocator);

    // No messages generated for a stale-term RequestVote.
    try std.testing.expectEqual(@as(usize, 0), node.messages.items.len);
}

test "paper: voter grants vote for up-to-date candidate" {
    var storage = try newStorage(&.{ 1, 2, 3 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    // Node 1 has entries up to index 3, term 3.
    var entries = [_]Entry{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 2 },
        .{ .index = 3, .term = 3 },
    };
    try storage.append(allocator, &entries);
    _ = try node.raft_log.append(&entries);
    node.term = 3;
    node.vote = 0; // hasn't voted yet

    // Candidate at term 4 with last_log_term=3, last_index=3 → up-to-date.
    var msg = Message{
        .msg_type = .request_vote,
        .to = 1,
        .from = 2,
        .term = 4,
        .log_term = 3,
        .index = 3,
    };
    try node.step(&msg);
    msg.deinit(allocator);

    // Should grant (reject=false).
    try std.testing.expectEqual(@as(usize, 1), node.messages.items.len);
    try std.testing.expect(!node.messages.items[0].reject);
    try std.testing.expectEqual(@as(u64, 2), node.vote);
    freeMessages(&node);
}

test "paper: voter rejects candidate with stale log" {
    var storage = try newStorage(&.{ 1, 2, 3 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    // Node 1 has entries up to index 3, term 3.
    var entries = [_]Entry{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 2 },
        .{ .index = 3, .term = 3 },
    };
    try storage.append(allocator, &entries);
    _ = try node.raft_log.append(&entries);
    node.term = 3;
    node.vote = 0;

    // Candidate at term 4 with last_log_term=2 (stale), last_index=5.
    // isUpToDate: term 2 < term 3 → not up to date → reject.
    var msg = Message{
        .msg_type = .request_vote,
        .to = 1,
        .from = 2,
        .term = 4,
        .log_term = 2,
        .index = 5,
    };
    try node.step(&msg);
    msg.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), node.messages.items.len);
    try std.testing.expect(node.messages.items[0].reject);
    freeMessages(&node);
}

// ===========================================================================
// Quorum commit (Section 5.4.2)
// ===========================================================================

test "paper: leader commits after majority acknowledgment" {
    var storage = try newStorage(&.{ 1, 2, 3 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    // Force leader state.
    node.term = 1;
    node.state = .leader;
    node.leader_id = 1;

    // Append entry at index 1, term 1.
    _ = try node.appendEntry(&.{.{ .term = 1, .index = 1 }});

    // Self matched = 0 initially (after reset). Update self + one follower.
    if (node.progress_tracker.getPtr(1)) |pr| pr.matched = 1;
    if (node.progress_tracker.getPtr(2)) |pr| pr.matched = 1;

    // maybeCommit: maxCommitIndex from tracker should be 1 (majority of 3 = 2,
    // sorted desc: 1, 1, 0 → 2nd = 1). maybeCommit(1, 1) should succeed.
    const committed = try node.maybeCommit();
    try std.testing.expect(committed);
    try std.testing.expectEqual(@as(u64, 1), node.raft_log.committed);
}

// ===========================================================================
// Election timeout distribution (Section 5.4.4)
// ===========================================================================

test "paper: randomized election timeout is in expected range" {
    var storage = try newStorage(&.{1});
    defer storage.deinit(allocator);

    // Create 100 nodes and check their randomized election timeouts.
    const N: usize = 100;
    var i: usize = 0;
    while (i < N) : (i += 1) {
        var config = makeConfig(1);
        config.election_tick = 10;
        config.min_election_tick = 10;
        config.max_election_tick = 20;
        config.election_timeout_seed = i;

        var node = try raft.Raft.init(allocator, config, storage.asStorage());
        defer node.deinit();

        // The randomized timeout should be in [min, max).
        try std.testing.expect(node.randomized_election_timeout >= 10);
        try std.testing.expect(node.randomized_election_timeout < 20);
    }
}

test "paper: start as follower" {
    var storage = try newStorage(&.{1});
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try std.testing.expectEqual(StateRole.follower, node.state);
    try std.testing.expectEqual(@as(u64, 0), node.term);
    try std.testing.expectEqual(@as(u64, 0), node.leader_id);
}
