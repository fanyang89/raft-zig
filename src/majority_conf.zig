//! Majority quorum configuration and algorithms.
//!
//! `MajorityConfig` is a set of voter IDs plus two quorum computations:
//!
//!   * `committedIndex` — sort matched indexes descending, pick the
//!     majority-th. Optional `use_group_commit` mixes in group-id awareness
//!     to coordinate commits across groups.
//!   * `getVoteResult` — count yes/no/missing votes against the majority
//!     threshold.

const std = @import("std");

const ack_indexer_mod = @import("ack_indexer.zig");

const Index = ack_indexer_mod.Index;
const VoteResult = ack_indexer_mod.VoteResult;
const AckedIndexer = ack_indexer_mod.AckedIndexer;

/// Quorum size for `total` voters: `total/2 + 1`. Empty collections return 0
/// (callers handle the empty case separately before invoking this).
pub fn majority(total: usize) usize {
    return total / 2 + 1;
}

pub const CommittedIndexResult = struct {
    index: u64,
    use_group_commit: bool,
};

pub const MajorityConfig = struct {
    voters: std.AutoHashMap(u64, void),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) MajorityConfig {
        return .{
            .voters = std.AutoHashMap(u64, void).init(allocator),
            .allocator = allocator,
        };
    }

    /// Build a MajorityConfig pre-populated from `ids`. Ownership of `ids`
    /// stays with the caller.
    pub fn fromIds(allocator: std.mem.Allocator, ids: []const u64) !MajorityConfig {
        var out = MajorityConfig.init(allocator);
        errdefer out.deinit();
        for (ids) |id| try out.voters.put(id, {});
        return out;
    }

    pub fn deinit(self: *MajorityConfig) void {
        self.voters.deinit();
        self.* = undefined;
    }

    /// Deep clone using the same allocator.
    pub fn clone(self: MajorityConfig) !MajorityConfig {
        var out = MajorityConfig.init(self.allocator);
        errdefer out.deinit();
        var it = self.voters.keyIterator();
        while (it.next()) |k| try out.voters.put(k.*, {});
        return out;
    }

    pub fn add(self: *MajorityConfig, id: u64) !void {
        try self.voters.put(id, {});
    }

    pub fn remove(self: *MajorityConfig, id: u64) bool {
        return self.voters.remove(id);
    }

    pub fn contains(self: MajorityConfig, id: u64) bool {
        return self.voters.contains(id);
    }

    pub fn count(self: MajorityConfig) usize {
        return self.voters.count();
    }

    pub fn isEmpty(self: MajorityConfig) bool {
        return self.voters.count() == 0;
    }

    pub fn clear(self: *MajorityConfig) void {
        self.voters.clearRetainingCapacity();
    }

    /// Sort matched indexes descending and pick the majority-th.
    /// Empty config returns `(max(u64), false)` — treated as
    /// "everything is committed".
    pub fn committedIndex(
        self: MajorityConfig,
        use_group_commit: bool,
        indexer: AckedIndexer,
    ) CommittedIndexResult {
        if (self.isEmpty()) {
            return .{ .index = std.math.maxInt(u64), .use_group_commit = false };
        }

        var matched = self.allocator.alloc(Index, self.voters.count()) catch
            return .{ .index = 0, .use_group_commit = false };
        defer self.allocator.free(matched);

        var i: usize = 0;
        var it = self.voters.keyIterator();
        while (it.next()) |k| : (i += 1) {
            matched[i] = indexer.ackedIndex(k.*) orelse Index{ .index = 0, .group_id = 0 };
        }

        std.mem.sort(Index, matched, {}, struct {
            fn lessThan(_: void, lhs: Index, rhs: Index) bool {
                return lhs.index > rhs.index;
            }
        }.lessThan);

        const quorum = majority(matched.len);
        const quorum_index = matched[quorum - 1];
        if (!use_group_commit) {
            return .{ .index = quorum_index.index, .use_group_commit = false };
        }

        const quorum_commit_index = quorum_index.index;
        var checked_group_id = quorum_index.group_id;
        var single_group = true;
        for (matched) |m| {
            if (m.group_id == 0) {
                single_group = false;
                continue;
            }
            if (checked_group_id == 0) {
                checked_group_id = m.group_id;
                continue;
            }
            if (checked_group_id == m.group_id) continue;
            return .{ .index = @min(m.index, quorum_commit_index), .use_group_commit = true };
        }

        if (single_group) {
            return .{ .index = quorum_index.index, .use_group_commit = false };
        }
        return .{ .index = matched[matched.len - 1].index, .use_group_commit = false };
    }

    /// Tally votes via `checker.check(id)` returning `?bool`
    /// (true=yes, false=no, null=missing).
    pub fn getVoteResult(
        self: MajorityConfig,
        comptime Checker: type,
        checker: *const Checker,
    ) VoteResult {
        if (self.isEmpty()) return .won;

        var yes: usize = 0;
        var missing: usize = 0;

        var it = self.voters.keyIterator();
        while (it.next()) |k| {
            if (checker.check(k.*)) |r| {
                if (r) yes += 1;
            } else {
                missing += 1;
            }
        }

        const q = majority(self.voters.count());
        if (yes >= q) return .won;
        if (yes + missing >= q) return .pending;
        return .lost;
    }
};

test "empty majority wins votes and returns max index" {
    var c = MajorityConfig.init(std.testing.allocator);
    defer c.deinit();

    const Checker = struct {
        pub fn check(_: @This(), _: u64) ?bool {
            return null;
        }
    };
    const checker = Checker{};
    try std.testing.expectEqual(VoteResult.won, c.getVoteResult(Checker, &checker));

    var idx = ack_indexer_mod.AckIndexer.init(std.testing.allocator);
    defer idx.deinit();
    const r = c.committedIndex(false, idx.indexer());
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), r.index);
}

test "vote counting across majority sizes" {
    const Case = struct {
        voters: []const u64,
        votes: []const u8, // 'y' / 'n' / '_'
        want: VoteResult,
    };
    const cases = [_]Case{
        .{ .voters = &.{1}, .votes = &.{'_'}, .want = .pending },
        .{ .voters = &.{1}, .votes = &.{'n'}, .want = .lost },
        .{ .voters = &.{123}, .votes = &.{'y'}, .want = .won },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ '_', '_' }, .want = .pending },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ 'n', '_' }, .want = .lost },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ 'y', '_' }, .want = .pending },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ 'n', 'y' }, .want = .lost },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ 'y', 'y' }, .want = .won },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ '_', '_', '_' }, .want = .pending },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'n', '_', '_' }, .want = .pending },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'y', '_', '_' }, .want = .pending },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'n', 'n', '_' }, .want = .lost },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'y', 'n', '_' }, .want = .pending },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'y', 'y', '_' }, .want = .won },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'y', 'y', 'n' }, .want = .won },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'n', 'y', 'n' }, .want = .lost },
    };

    for (cases) |c| {
        var mc = try MajorityConfig.fromIds(std.testing.allocator, c.voters);
        defer mc.deinit();

        // Build a vote map: positions in c.voters correspond to positions in c.votes.
        // The checker captures the slice pair.
        const Checker = struct {
            voters: []const u64,
            votes: []const u8,
            pub fn check(self: @This(), id: u64) ?bool {
                for (self.voters, 0..) |v, i| {
                    if (v == id) {
                        return switch (self.votes[i]) {
                            'y' => @as(?bool, true),
                            'n' => @as(?bool, false),
                            '_' => @as(?bool, null),
                            else => unreachable,
                        };
                    }
                }
                return null;
            }
        };
        const checker = Checker{ .voters = c.voters, .votes = c.votes };
        try std.testing.expectEqual(c.want, mc.getVoteResult(Checker, &checker));
    }
}

test "committed index picks the majority-th match" {
    const allocator = std.testing.allocator;
    var idx = ack_indexer_mod.AckIndexer.init(allocator);
    defer idx.deinit();
    // Voters {1, 2, 3} with matched indexes 10, 20, 30. Sorted desc: 30,20,10.
    // Majority of 3 is 2, so we pick index 1 (the 2nd highest) = 20.
    try idx.set(1, .{ .index = 10, .group_id = 0 });
    try idx.set(2, .{ .index = 20, .group_id = 0 });
    try idx.set(3, .{ .index = 30, .group_id = 0 });

    var c = try MajorityConfig.fromIds(allocator, &.{ 1, 2, 3 });
    defer c.deinit();

    const r = c.committedIndex(false, idx.indexer());
    try std.testing.expectEqual(@as(u64, 20), r.index);
    try std.testing.expectEqual(false, r.use_group_commit);
}
