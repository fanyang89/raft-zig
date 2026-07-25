//! MPL-2.0 clean-room reimplementation of observable commitment behavior.
//!
//! The scenarios translate only externally observable outcomes into raft-zig's
//! public APIs. Their fixture and assertions are independently authored, and no
//! upstream source text is copied.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/hashicorp_raft/cases/commitment_test.zig";

const Fixture = struct {
    storage: raft.MemoryStorage,
    node: raft.Raft,

    fn init(
        self: *Fixture,
        voters: []const u64,
        last_index: usize,
        current_term: u64,
        current_term_start: u64,
    ) !void {
        std.debug.assert(last_index > 0);
        std.debug.assert(current_term > 0);
        std.debug.assert(current_term_start > 0 and current_term_start <= last_index);

        self.storage = raft.MemoryStorage.init();
        errdefer self.storage.deinit(allocator);

        const entries = try allocator.alloc(raft.Entry, last_index);
        defer allocator.free(entries);
        for (entries, 0..) |*entry, offset| {
            const index: u64 = @intCast(offset + 1);
            entry.* = .{
                .index = index,
                .term = if (index < current_term_start) current_term - 1 else current_term,
            };
        }
        try self.storage.append(allocator, entries);

        var conf_state = raft.ConfState{ .voters = try allocator.dupe(u64, voters) };
        defer conf_state.deinit(allocator);
        try self.storage.setRaftState(allocator, .{
            .hard_state = .{ .term = current_term },
            .conf_state = conf_state,
        });

        var config = raft.defaultConfig();
        config.id = voters[0];
        config.election_timeout_seed = 1;
        config.load_state_on_startup = true;
        self.node = try raft.Raft.init(allocator, config, self.storage.asStorage());

        for (voters) |id| self.node.progress_tracker.at(id).matched = 0;
    }

    fn deinit(self: *Fixture) void {
        self.node.deinit();
        self.storage.deinit(allocator);
    }

    fn acknowledge(self: *Fixture, id: u64, index: u64) !bool {
        const progress = self.node.progress_tracker.getPtr(id) orelse return false;
        _ = progress.maybeUpdate(index);
        return self.node.maybeCommit();
    }

    fn changeVoter(self: *Fixture, change_type: raft.ConfChangeType, id: u64) !void {
        var changes = [_]raft.ConfChangeSingle{.{
            .change_type = change_type,
            .node_id = id,
        }};
        var conf_state = try self.node.applyConfChange(.{ .changes = &changes });
        conf_state.deinit(allocator);
    }
};

test "HashiCorp Raft: TestCommitment_match_max" {
    var fixture: Fixture = undefined;
    try fixture.init(&.{ 1, 2, 3, 4, 5 }, 8, 1, 1);
    defer fixture.deinit();

    try std.testing.expect(!try fixture.acknowledge(1, 8));
    try std.testing.expect(!try fixture.acknowledge(2, 8));
    try std.testing.expect(!try fixture.acknowledge(2, 1));
    try std.testing.expectEqual(@as(u64, 8), fixture.node.progress_tracker.at(2).matched);
    try std.testing.expect(try fixture.acknowledge(3, 8));
    try std.testing.expectEqual(@as(u64, 8), fixture.node.raft_log.committed);
}

test "HashiCorp Raft: TestCommitment_match_nonVoting" {
    var fixture: Fixture = undefined;
    try fixture.init(&.{ 1, 2, 3, 4, 5 }, 10, 1, 1);
    defer fixture.deinit();

    try std.testing.expect(!try fixture.acknowledge(1, 8));
    try std.testing.expect(!try fixture.acknowledge(2, 8));
    try std.testing.expect(try fixture.acknowledge(3, 8));

    try std.testing.expect(!try fixture.acknowledge(90, 10));
    try std.testing.expect(!try fixture.acknowledge(91, 10));
    try std.testing.expect(!try fixture.acknowledge(92, 10));
    try std.testing.expectEqual(@as(u64, 8), fixture.node.raft_log.committed);
}

test "HashiCorp Raft: TestCommitment_recalculate" {
    var fixture: Fixture = undefined;
    try fixture.init(&.{ 1, 2, 3, 4, 5 }, 30, 1, 1);
    defer fixture.deinit();

    try std.testing.expect(!try fixture.acknowledge(1, 30));
    try std.testing.expect(!try fixture.acknowledge(2, 20));
    try std.testing.expectEqual(@as(u64, 0), fixture.node.raft_log.committed);

    try std.testing.expect(try fixture.acknowledge(3, 10));
    try std.testing.expectEqual(@as(u64, 10), fixture.node.raft_log.committed);
    try std.testing.expect(try fixture.acknowledge(4, 15));
    try std.testing.expectEqual(@as(u64, 15), fixture.node.raft_log.committed);

    try fixture.changeVoter(.remove_node, 5);
    try fixture.changeVoter(.remove_node, 4);
    try std.testing.expect(try fixture.node.maybeCommit());
    try std.testing.expectEqual(@as(u64, 20), fixture.node.raft_log.committed);

    try fixture.changeVoter(.add_node, 4);
    try std.testing.expect(!try fixture.node.maybeCommit());
    try std.testing.expect(!try fixture.acknowledge(2, 25));
    try std.testing.expectEqual(@as(u64, 20), fixture.node.raft_log.committed);
    try std.testing.expect(try fixture.acknowledge(4, 23));
    try std.testing.expectEqual(@as(u64, 23), fixture.node.raft_log.committed);
}

test "HashiCorp Raft: TestCommitment_recalculate_startIndex" {
    var fixture: Fixture = undefined;
    try fixture.init(&.{ 1, 2, 3, 4, 5 }, 4, 2, 4);
    defer fixture.deinit();

    try std.testing.expect(!try fixture.acknowledge(1, 3));
    try std.testing.expect(!try fixture.acknowledge(2, 3));
    try std.testing.expect(!try fixture.acknowledge(3, 3));
    try std.testing.expectEqual(@as(u64, 0), fixture.node.raft_log.committed);

    try std.testing.expect(!try fixture.acknowledge(1, 4));
    try std.testing.expect(!try fixture.acknowledge(2, 4));
    try std.testing.expect(try fixture.acknowledge(3, 4));
    try std.testing.expectEqual(@as(u64, 4), fixture.node.raft_log.committed);
}
