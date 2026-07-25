// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;
const max_uncommitted_size: u64 = 12;
const proposal_data = "hello world!";

pub const inventory_target = "tests/upstream/raft_rs/cases/uncommitted_test.zig";

fn newLimitedNetwork() !network.Network {
    var net = try network.newNetwork(&.{ 1, 2, 3, 4, 5 });
    var peers = net.peers.valueIterator();
    while (peers.next()) |peer| {
        peer.*.raft.uncommitted_state.max_uncommitted_size = max_uncommitted_size;
    }
    return net;
}

fn newProposal(from: u64) !raft.Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, proposal_data) };
    return .{
        .msg_type = .propose,
        .from = from,
        .to = from,
        .entries = entries,
    };
}

fn sendProposal(net: *network.Network, from: u64) !void {
    var message = try newProposal(from);
    defer message.deinit(allocator);
    try net.send(&.{message});
}

fn seedCommittedEntries(storage: *raft.MemoryStorage) !void {
    var conf_state = raft.ConfState{ .voters = try allocator.dupe(u64, &.{ 1, 2, 3, 4, 5 }) };
    defer conf_state.deinit(allocator);
    try storage.setRaftState(allocator, .{
        .hard_state = .{ .term = 1, .commit = 3 },
        .conf_state = conf_state,
    });

    var entries = [_]raft.Entry{
        .{ .term = 1, .index = 1 },
        .{ .term = 1, .index = 2 },
        .{ .term = 1, .index = 3 },
    };
    defer for (&entries) |*entry| entry.deinit(allocator);
    entries[1].data = try allocator.dupe(u8, proposal_data);
    entries[2].data = try allocator.dupe(u8, proposal_data);
    try storage.setEntries(allocator, &entries);
}

fn newRawNode(storage: *raft.MemoryStorage) !raft.RawNode {
    var config = raft.defaultConfig();
    config.id = 2;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.applied = 1;
    config.max_uncommitted_size = max_uncommitted_size;
    config.load_state_on_startup = true;
    config.election_timeout_seed = 42;
    return raft.RawNode.init(allocator, config, storage.asStorage());
}

fn grantVote(node: *raft.RawNode, from: u64) !void {
    try node.step(.{
        .msg_type = .request_vote_response,
        .from = from,
        .to = 2,
        .term = 2,
    });
}

fn persistReady(storage: *raft.MemoryStorage, ready: raft.Ready) !void {
    if (ready.hs) |hard_state| try storage.setHardState(hard_state);
    if (ready.entries.len > 0) try storage.append(allocator, ready.entries);
}

test "raft-rs: uncommitted entry after leader election" {
    var net = try newLimitedNetwork();
    defer net.deinit();

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try net.cut(1, 3);
    try net.cut(1, 4);
    try net.cut(1, 5);
    try sendProposal(&net, 1);

    try net.isolate(1);
    try net.ignoreMessageType(.append);
    try net.send(&.{.{ .msg_type = .hup, .from = 2, .to = 2 }});

    const leader = net.getPeer(2).?;
    try std.testing.expectEqual(raft.StateRole.leader, leader.raft.state);
    try std.testing.expectEqual(@as(u64, 0), leader.raft.uncommitted_state.uncommitted_size);

    var accepted = try newProposal(2);
    defer accepted.deinit(allocator);
    try leader.raft.step(&accepted);
    try std.testing.expectEqual(max_uncommitted_size, leader.raft.uncommitted_state.uncommitted_size);

    var dropped = try newProposal(2);
    defer dropped.deinit(allocator);
    try std.testing.expectError(error.ProposalDropped, leader.raft.step(&dropped));
    try std.testing.expectEqual(max_uncommitted_size, leader.raft.uncommitted_state.uncommitted_size);
}

test "raft-rs: uncommitted state advance Ready from last term" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedCommittedEntries(&storage);

    var node = try newRawNode(&storage);
    defer node.deinit();
    try node.campaign();
    try grantVote(&node, 1);
    try grantVote(&node, 3);
    try std.testing.expectEqual(raft.StateRole.leader, node.raftConst().state);

    try node.propose("", proposal_data);
    try std.testing.expectEqual(max_uncommitted_size, node.raftConst().uncommitted_state.uncommitted_size);

    var ready = try node.getReady();
    defer ready.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), ready.light.committed_entries.len);
    try std.testing.expectEqual(@as(u64, 2), ready.light.committed_entries[0].index);
    try std.testing.expectEqual(@as(u64, 3), ready.light.committed_entries[1].index);
    try std.testing.expectEqual(max_uncommitted_size, node.raftConst().uncommitted_state.uncommitted_size);

    try persistReady(&storage, ready);
    var light = try node.advance(ready);
    defer light.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), node.raftConst().raft_log.applied);
    try std.testing.expectEqual(max_uncommitted_size, node.raftConst().uncommitted_state.uncommitted_size);
}
