const std = @import("std");
const raft = @import("raft_zig");
const network_mod = @import("harness/network.zig");

const Message = raft.Message;
const Network = network_mod.Network;

const Action = enum(u8) {
    campaign,
    tick,
    propose,
    cut,
    isolate,
    recover,
    deliver,
    drop,
    drain,
};

const TraceAction = struct {
    action: Action,
    id: u64,
    pending: usize,
    detail: u64,
};

fn campaign(network: *Network, id: u64) !void {
    const message = Message{ .msg_type = .hup, .from = id };
    try network.send(&.{message});
}

fn enqueueCampaign(network: *Network, id: u64) !void {
    const message = Message{ .msg_type = .hup, .from = id };
    try network.enqueue(&.{message});
}

fn propose(network: *Network, id: u64, data: []const u8) !void {
    var entries = [_]raft.Entry{.{ .data = @constCast(data) }};
    const message = Message{
        .msg_type = .propose,
        .from = id,
        .to = id,
        .entries = entries[0..],
    };
    try network.send(&.{message});
}

fn enqueueProposal(network: *Network, id: u64, data: []const u8) !void {
    var entries = [_]raft.Entry{.{ .data = @constCast(data) }};
    const message = Message{
        .msg_type = .propose,
        .from = id,
        .to = id,
        .entries = entries[0..],
    };
    try network.enqueue(&.{message});
}

test "simulation: scheduler exposes one message at a time" {
    var network = try network_mod.newNetwork(&.{ 1, 2, 3 });
    defer network.deinit();

    try enqueueCampaign(&network, 1);
    try std.testing.expectEqual(@as(usize, 1), network.pendingCount());
    try std.testing.expectEqual(network_mod.Delivery.delivered, (try network.deliverOne()).?);
    try std.testing.expectEqual(raft.StateRole.candidate, network.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(usize, 2), network.pendingCount());
    try std.testing.expectEqual(@as(u64, 2), network.pending.items[0].to);
    try std.testing.expectEqual(@as(u64, 3), network.pending.items[1].to);

    _ = try network.runUntilIdle(100);
    try std.testing.expectEqual(raft.StateRole.leader, network.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(usize, 0), network.pendingCount());
}

test "simulation: runUntilIdle enforces its step limit" {
    var network = try network_mod.newNetwork(&.{ 1, 2, 3 });
    defer network.deinit();

    try enqueueCampaign(&network, 1);
    try std.testing.expectError(error.StepLimitExceeded, network.runUntilIdle(0));
    try std.testing.expectEqual(@as(usize, 1), network.pendingCount());
}

test "simulation: local feedback requires an explicit target" {
    var network = try network_mod.newNetwork(&.{ 1, 2, 3 });
    defer network.deinit();
    try campaign(&network, 1);

    const feedback = Message{ .msg_type = .unreachable_peer, .from = 2 };
    try network.enqueue(&.{feedback});
    try std.testing.expectEqual(network_mod.Delivery.unknown_target, (try network.deliverOne()).?);

    try network.stepLocal(1, feedback);
    try std.testing.expectEqual(@as(usize, 0), network.pendingCount());
}

test "simulation: minority proposal commits after partition heals" {
    var network = try network_mod.newNetwork(&.{ 1, 2, 3 });
    defer network.deinit();

    try campaign(&network, 1);
    try network.isolate(1);
    try propose(&network, 1, "minority");

    try std.testing.expectEqual(@as(u64, 1), network.getPeer(1).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 2), network.getPeer(1).?.raft.raft_log.lastIndex());
    try std.testing.expectEqual(@as(u64, 1), network.getPeer(2).?.raft.raft_log.committed);

    const digest = try network.converge(64, 1_000);
    try std.testing.expectEqual(@as(u64, 2), digest.committed);
    try std.testing.expectEqual(@as(u64, 2), digest.last_index);
}

test "simulation: lagging follower catches up after recovery" {
    var network = try network_mod.newNetwork(&.{ 1, 2, 3 });
    defer network.deinit();

    try campaign(&network, 1);
    try network.cut(1, 3);
    try propose(&network, 1, "majority");

    try std.testing.expectEqual(@as(u64, 2), network.getPeer(1).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 2), network.getPeer(2).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 1), network.getPeer(3).?.raft.raft_log.committed);

    const digest = try network.converge(32, 1_000);
    try std.testing.expectEqual(@as(u64, 1), digest.leader_id);
    try std.testing.expectEqual(@as(u64, 2), digest.committed);
}

test "simulation: isolated candidate rejoins a converged cluster" {
    var network = try network_mod.newNetwork(&.{ 1, 2, 3 });
    defer network.deinit();

    try campaign(&network, 1);
    try network.isolate(3);
    try campaign(&network, 3);
    try std.testing.expectEqual(raft.StateRole.candidate, network.getPeer(3).?.raft.state);

    const digest = try network.converge(64, 1_000);
    try std.testing.expectEqual(digest.term, network.getPeer(1).?.raft.term);
    try std.testing.expectEqual(digest.term, network.getPeer(2).?.raft.term);
    try std.testing.expectEqual(digest.term, network.getPeer(3).?.raft.term);
}

test "simulation: delayed traffic converges safely" {
    for ([_]u64{ 1, 3 }) |other| {
        var network = try network_mod.newNetwork(&.{ 1, 2, 3 });
        defer network.deinit();

        try enqueueCampaign(&network, 2);
        _ = try network.runUntilIdle(10_000);
        _ = try network.tickPeer(2);
        try network.cut(2, other);
        _ = try network.deliverOne();
        try enqueueProposal(&network, 3, "a");
        try enqueueProposal(&network, 1, "b");
        try enqueueCampaign(&network, 2);
        try network.isolate(2);
        try enqueueProposal(&network, 2, "c");
        _ = try network.tickPeer(1);
        try enqueueProposal(&network, 2, "d");
        try enqueueCampaign(&network, 3);
        _ = try network.runUntilIdle(10_000);
        try network.checkSafety();
        _ = try network.converge(64, 10_000);
    }
}

test "fuzz: deterministic cluster simulation" {
    try std.testing.fuzz({}, fuzzSimulation, .{ .corpus = &.{
        "",
        "campaign-propose",
        "partition-recover",
    } });
}

fn fuzzSimulation(_: void, smith: *std.testing.Smith) !void {
    var network = try network_mod.newNetwork(&.{ 1, 2, 3 });
    defer network.deinit();

    var trace: [16]TraceAction = undefined;
    var trace_len: usize = 0;
    errdefer {
        for (trace[0..trace_len], 0..) |item, i| {
            std.log.err(
                "simulation action {}: {s}, node={}, pending={}, detail={}",
                .{ i, @tagName(item.action), item.id, item.pending, item.detail },
            );
        }
    }

    const action_count = smith.valueRangeAtMost(u8, 1, 16);
    for (0..action_count) |_| {
        const id = smith.valueRangeAtMost(u64, 1, 3);
        const action = smith.value(Action);
        trace[trace_len] = .{ .action = action, .id = id, .pending = network.pendingCount(), .detail = 0 };
        const trace_index = trace_len;
        trace_len += 1;
        switch (action) {
            .campaign => try enqueueCampaign(&network, id),
            .tick => _ = try network.tickPeer(id),
            .propose => {
                var payload: [8]u8 = undefined;
                const value = smith.value(u64);
                trace[trace_index].detail = value;
                std.mem.writeInt(u64, &payload, value, .little);
                try enqueueProposal(&network, id, &payload);
            },
            .cut => {
                var other = smith.valueRangeAtMost(u64, 1, 3);
                if (other == id) other = id % 3 + 1;
                trace[trace_index].detail = other;
                try network.cut(id, other);
            },
            .isolate => try network.isolate(id),
            .recover => network.recover(),
            .deliver => if (network.pendingCount() > 0) {
                const max_index: u16 = @intCast(network.pendingCount() - 1);
                const index = smith.valueRangeAtMost(u16, 0, max_index);
                trace[trace_index].detail = index;
                _ = try network.deliverAt(index);
            },
            .drop => if (network.pendingCount() > 0) {
                const max_index: u16 = @intCast(network.pendingCount() - 1);
                const index = smith.valueRangeAtMost(u16, 0, max_index);
                trace[trace_index].detail = index;
                try network.dropPending(index);
            },
            .drain => _ = try network.runUntilIdle(10_000),
        }
        try network.checkSafety();
    }

    _ = try network.converge(256, 10_000);
}
