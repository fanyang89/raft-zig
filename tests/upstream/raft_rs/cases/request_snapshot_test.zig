// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/request_snapshot_test.zig";

const Prepared = struct {
    net: network.Network,
    previous_snapshot: raft.Snapshot,

    fn deinit(self: *Prepared) void {
        self.previous_snapshot.deinit(allocator);
        self.net.deinit();
    }
};

fn installSnapshot(peer: *network.Peer) !void {
    var snapshot = raft.Snapshot{ .metadata = .{
        .index = 11,
        .term = 11,
        .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }) },
    } };
    defer snapshot.deinit(allocator);

    try std.testing.expect(try peer.raft.restoreSnapshot(snapshot));
    const pending = peer.raft.raft_log.unstable.snapshot.?;
    try peer.storage.applySnapshot(allocator, pending);
    peer.raft.raft_log.stableSnapshot(pending.metadata.index);
    peer.raft.onPersistSnapshot(pending.metadata.index);
    peer.raft.commitApply(pending.metadata.index);
    peer.raft.becomeFollower(11, raft.invalid_id);
    try peer.storage.setHardState(peer.raft.hardState());
}

fn propose(net: *network.Network) !void {
    var entries = [_]raft.Entry{.{ .data = @constCast("testdata") }};
    try net.send(&.{.{
        .msg_type = .propose,
        .from = 1,
        .to = 1,
        .entries = &entries,
    }});
}

fn prepareRequestSnapshot() !Prepared {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    errdefer net.deinit();

    for (1..4) |id| try installSnapshot(net.getPeer(id).?);
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try propose(&net);
    try propose(&net);

    const leader = net.getPeer(1).?;
    try std.testing.expectEqual(@as(u64, 14), leader.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 14), net.getPeer(2).?.raft.raft_log.committed);
    leader.raft.commitApply(14);

    var previous_snapshot = try leader.storage.getSnapshot(allocator, 0, leader.raft.id);
    errdefer previous_snapshot.deinit(allocator);
    try propose(&net);

    return .{ .net = net, .previous_snapshot = previous_snapshot };
}

fn takeSnapshotRequest(peer: *network.Peer) !raft.Message {
    try peer.raft.requestSnapshot();
    return peer.raft.messages.pop() orelse error.MissingSnapshotRequest;
}

fn expectSnapshotRequest(message: raft.Message, request_index: u64) !void {
    try std.testing.expectEqual(raft.MessageType.append_response, message.msg_type);
    try std.testing.expect(message.reject);
    try std.testing.expectEqual(request_index, message.request_snapshot);
}

test "raft-rs: test_follower_request_snapshot" {
    var prepared = try prepareRequestSnapshot();
    defer prepared.deinit();

    const leader = prepared.net.getPeer(1).?;
    const follower = prepared.net.getPeer(2).?;
    const previous_snapshot_index = prepared.previous_snapshot.metadata.index;
    const request_index = leader.raft.raft_log.committed;
    try std.testing.expect(previous_snapshot_index < request_index);

    var request = try takeSnapshotRequest(follower);
    defer request.deinit(allocator);
    try expectSnapshotRequest(request, request_index);
    try leader.raft.step(&request);

    try propose(&prepared.net);
    try std.testing.expectEqual(@as(u64, 16), leader.raft.raft_log.committed);
    try std.testing.expectEqual(raft.ProgressState.snapshot, leader.raft.progress_tracker.getPtr(2).?.state);
    try std.testing.expectEqual(@as(u64, 15), follower.raft.raft_log.committed);

    try prepared.net.send(&.{.{ .msg_type = .snap_status, .from = 2, .to = 1 }});
    try prepared.net.send(&.{.{ .msg_type = .heartbeat_response, .from = 2, .to = 1 }});
    try propose(&prepared.net);

    try std.testing.expectEqual(@as(u64, 17), leader.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 17), follower.raft.raft_log.committed);
}

fn requestSnapshotUnavailable() !void {
    var prepared = try prepareRequestSnapshot();
    defer prepared.deinit();

    const leader = prepared.net.getPeer(1).?;
    const follower = prepared.net.getPeer(2).?;
    const previous_snapshot_index = prepared.previous_snapshot.metadata.index;
    const request_index = leader.raft.raft_log.committed;
    try std.testing.expect(previous_snapshot_index < request_index);

    var request = try takeSnapshotRequest(follower);
    defer request.deinit(allocator);
    try expectSnapshotRequest(request, request_index);

    leader.storage.triggerSnapshotUnavailable();
    try leader.raft.step(&request);
    try std.testing.expectEqual(raft.ProgressState.probe, leader.raft.progress_tracker.getPtr(2).?.state);

    leader.storage.triggerSnapshotUnavailable();
    try leader.raft.step(&request);
    try std.testing.expectEqual(raft.ProgressState.probe, leader.raft.progress_tracker.getPtr(2).?.state);

    try leader.raft.step(&request);
    try std.testing.expectEqual(raft.ProgressState.snapshot, leader.raft.progress_tracker.getPtr(2).?.state);
}

test "raft-rs: test_request_snapshot_unavailable" {
    try requestSnapshotUnavailable();
}

test "raft-rs: test_request_snapshot_matched_change" {
    var prepared = try prepareRequestSnapshot();
    defer prepared.deinit();

    const leader = prepared.net.getPeer(1).?;
    const follower = prepared.net.getPeer(2).?;
    follower.raft.raft_log.committed -= 1;

    var request = try takeSnapshotRequest(follower);
    defer request.deinit(allocator);
    try leader.raft.step(&request);
    try std.testing.expectEqual(raft.ProgressState.replicate, leader.raft.progress_tracker.getPtr(2).?.state);

    leader.raft.ping();
    var heartbeat_index: ?usize = null;
    for (leader.raft.messages.items, 0..) |message, index| {
        if (message.to == 2 and message.msg_type == .heartbeat) {
            heartbeat_index = index;
            break;
        }
    }
    var heartbeat = leader.raft.messages.orderedRemove(heartbeat_index orelse return error.MissingHeartbeat);
    defer heartbeat.deinit(allocator);
    try follower.raft.step(&heartbeat);

    var retried_request = follower.raft.messages.pop() orelse return error.MissingSnapshotRequest;
    defer retried_request.deinit(allocator);
    try leader.raft.step(&retried_request);
    try std.testing.expectEqual(raft.ProgressState.snapshot, leader.raft.progress_tracker.getPtr(2).?.state);
}

test "raft-rs: test_request_snapshot_none_replicate" {
    var prepared = try prepareRequestSnapshot();
    defer prepared.deinit();

    const leader = prepared.net.getPeer(1).?;
    const follower = prepared.net.getPeer(2).?;
    leader.raft.progress_tracker.getPtr(2).?.state = .probe;

    var request = try takeSnapshotRequest(follower);
    defer request.deinit(allocator);
    try leader.raft.step(&request);
    try std.testing.expect(leader.raft.progress_tracker.getPtr(2).?.pending_request_snapshot != raft.invalid_index);
}

test "raft-rs: test_request_snapshot_step_down" {
    var prepared = try prepareRequestSnapshot();
    defer prepared.deinit();

    try prepared.net.isolate(2);
    try propose(&prepared.net);
    try prepared.net.send(&.{.{ .msg_type = .hup, .from = 3, .to = 3 }});
    try std.testing.expectEqual(raft.StateRole.leader, prepared.net.getPeer(3).?.raft.state);

    prepared.net.recover();
    try prepared.net.getPeer(2).?.raft.requestSnapshot();
    try prepared.net.send(&.{.{ .msg_type = .beat, .from = 3, .to = 3 }});
    try std.testing.expectEqual(raft.invalid_index, prepared.net.getPeer(2).?.raft.pending_request_snapshot);
}

test "raft-rs: test_request_snapshot_on_role_change" {
    var prepared = try prepareRequestSnapshot();
    defer prepared.deinit();

    const leader = prepared.net.getPeer(1).?;
    const follower = prepared.net.getPeer(2).?;
    try follower.raft.requestSnapshot();

    follower.raft.becomeFollower(leader.raft.term, leader.raft.id);
    try std.testing.expect(follower.raft.pending_request_snapshot != raft.invalid_index);

    follower.raft.becomeCandidate();
    try std.testing.expectEqual(raft.invalid_index, follower.raft.pending_request_snapshot);
}

test "raft-rs: test_request_snapshot_after_term_change" {
    var prepared = try prepareRequestSnapshot();
    defer prepared.deinit();

    const follower = prepared.net.getPeer(2).?;
    try follower.raft.requestSnapshot();
    try std.testing.expect(follower.raft.pending_request_snapshot != raft.invalid_index);

    const previous_term = follower.raft.term;
    follower.raft.becomeCandidate();
    try std.testing.expectEqual(previous_term + 1, follower.raft.term);
    try std.testing.expectEqual(raft.invalid_index, follower.raft.pending_request_snapshot);
}

test "raft-rs: test_raft_snap::test_request_snapshot" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    const node = net.getPeer(1).?;

    var snapshot = raft.Snapshot{ .metadata = .{
        .index = 11,
        .term = 11,
        .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2 }) },
    } };
    defer snapshot.deinit(allocator);
    try std.testing.expect(try node.raft.restoreSnapshot(snapshot));
    node.raft.becomeFollower(snapshot.metadata.term, raft.invalid_id);
    const pending = node.raft.raft_log.unstable.snapshot.?;
    try node.storage.applySnapshot(allocator, pending);
    node.raft.raft_log.stableSnapshot(pending.metadata.index);
    node.raft.onPersistSnapshot(pending.metadata.index);

    try std.testing.expectError(error.RequestSnapshotDropped, node.raft.requestSnapshot());

    const term = node.raft.term;
    node.raft.becomeFollower(term + 1, 2);
    try std.testing.expectError(error.RequestSnapshotDropped, node.raft.requestSnapshot());

    node.raft.becomeCandidate();
    try node.raft.becomeLeader();
    try std.testing.expectError(error.RequestSnapshotDropped, node.raft.requestSnapshot());

    var matched = raft.Message{ .msg_type = .append_response, .from = 2, .to = 1, .index = 11 };
    try node.raft.step(&matched);
    try std.testing.expectEqual(raft.ProgressState.replicate, node.raft.progress_tracker.getPtr(2).?.state);

    const request_snapshot_index = node.raft.raft_log.committed;
    var request = raft.Message{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .index = 11,
        .reject = true,
        .request_snapshot = request_snapshot_index,
    };

    var out_of_order = request;
    out_of_order.index = 9;
    try node.raft.step(&out_of_order);
    try std.testing.expectEqual(raft.ProgressState.replicate, node.raft.progress_tracker.getPtr(2).?.state);

    try node.raft.step(&request);
    const progress = node.raft.progress_tracker.getPtr(2).?;
    try std.testing.expectEqual(raft.ProgressState.snapshot, progress.state);
    try std.testing.expectEqual(@as(u64, 11), progress.pending_snapshot);
    try std.testing.expectEqual(@as(u64, 12), progress.next_idx);
    try std.testing.expect(progress.isPaused());

    var sent_snapshot = node.raft.messages.pop() orelse return error.MissingSnapshot;
    defer sent_snapshot.deinit(allocator);
    try std.testing.expectEqual(raft.MessageType.snapshot, sent_snapshot.msg_type);
    try std.testing.expectEqual(request_snapshot_index, sent_snapshot.snapshot.?.metadata.index);

    try node.raft.step(&matched);
    try std.testing.expectEqual(raft.ProgressState.snapshot, progress.state);
    try std.testing.expectEqual(@as(u64, 11), progress.pending_snapshot);
    try std.testing.expectEqual(@as(u64, 12), progress.next_idx);
    try std.testing.expect(progress.isPaused());

    var status = raft.Message{ .msg_type = .snap_status, .from = 2, .to = 1 };
    try node.raft.step(&status);
    try std.testing.expectEqual(raft.ProgressState.probe, progress.state);
    try std.testing.expectEqual(@as(u64, 0), progress.pending_snapshot);
    try std.testing.expectEqual(@as(u64, 12), progress.next_idx);
    try std.testing.expect(progress.isPaused());
}
