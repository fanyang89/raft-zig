//! Joint consensus configuration: pair of MajorityConfigs.
//!
//! `JointConfiguration` holds an incoming majority (the new config) and an
//! outgoing majority (the previous config). Joint consensus requires both
//! majorities to agree during a membership transition.

const std = @import("std");

const majority_conf_mod = @import("majority_conf.zig");
const ack_indexer_mod = @import("ack_indexer.zig");

const MajorityConfig = majority_conf_mod.MajorityConfig;
const CommittedIndexResult = majority_conf_mod.CommittedIndexResult;
const AckedIndexer = ack_indexer_mod.AckedIndexer;
const VoteResult = ack_indexer_mod.VoteResult;

pub const JointConfiguration = struct {
    incoming: MajorityConfig,
    outgoing: MajorityConfig,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) JointConfiguration {
        return .{
            .incoming = MajorityConfig.init(allocator),
            .outgoing = MajorityConfig.init(allocator),
            .allocator = allocator,
        };
    }

    /// `voters` becomes the incoming set; outgoing starts empty (simple
    /// configuration, not joint).
    pub fn fromVoters(allocator: std.mem.Allocator, voters: []const u64) !JointConfiguration {
        var out = JointConfiguration.init(allocator);
        errdefer out.deinit();
        for (voters) |id| try out.incoming.add(id);
        return out;
    }

    pub fn fromIncomingOutgoing(
        allocator: std.mem.Allocator,
        incoming: []const u64,
        outgoing: []const u64,
    ) !JointConfiguration {
        var out = JointConfiguration.init(allocator);
        errdefer out.deinit();
        for (incoming) |id| try out.incoming.add(id);
        for (outgoing) |id| try out.outgoing.add(id);
        return out;
    }

    pub fn deinit(self: *JointConfiguration) void {
        self.incoming.deinit();
        self.outgoing.deinit();
        self.* = undefined;
    }

    pub fn clone(self: JointConfiguration) !JointConfiguration {
        return .{
            .incoming = try self.incoming.clone(),
            .outgoing = try self.outgoing.clone(),
            .allocator = self.allocator,
        };
    }

    /// Committed index is the smaller of incoming/outgoing majorities.
    /// `use_group_commit` is set only when both sides agree on group commit.
    pub fn committedIndex(
        self: JointConfiguration,
        use_group_commit: bool,
        indexer: AckedIndexer,
    ) CommittedIndexResult {
        const i = self.incoming.committedIndex(use_group_commit, indexer);
        const o = self.outgoing.committedIndex(use_group_commit, indexer);
        return .{
            .index = @min(i.index, o.index),
            .use_group_commit = i.use_group_commit and o.use_group_commit,
        };
    }

    /// Joint vote: won only if both sides win; lost if either loses; otherwise
    /// pending.
    pub fn getVoteResult(
        self: JointConfiguration,
        comptime Checker: type,
        checker: *const Checker,
    ) VoteResult {
        const in = self.incoming.getVoteResult(Checker, checker);
        const out = self.outgoing.getVoteResult(Checker, checker);

        if (in == .won and out == .won) return .won;
        if (in == .lost or out == .lost) return .lost;
        return .pending;
    }

    pub fn clear(self: *JointConfiguration) void {
        self.incoming.clear();
        self.outgoing.clear();
    }

    pub fn contains(self: JointConfiguration, id: u64) bool {
        return self.incoming.contains(id) or self.outgoing.contains(id);
    }

    pub fn isSingleton(self: JointConfiguration) bool {
        return self.outgoing.isEmpty() and self.incoming.count() == 1;
    }

    /// Caller owns the returned sorted slice.
    pub fn ids(self: JointConfiguration) ![]u64 {
        var out = try self.allocator.alloc(u64, self.incoming.count() + self.outgoing.count());
        errdefer self.allocator.free(out);
        var seen = std.AutoHashMap(u64, void).init(self.allocator);
        defer seen.deinit();
        var n: usize = 0;
        var it = self.incoming.voters.keyIterator();
        while (it.next()) |k| {
            const gop = try seen.getOrPut(k.*);
            if (gop.found_existing) continue;
            out[n] = k.*;
            n += 1;
        }
        var ot = self.outgoing.voters.keyIterator();
        while (ot.next()) |k| {
            const gop = try seen.getOrPut(k.*);
            if (gop.found_existing) continue;
            out[n] = k.*;
            n += 1;
        }
        std.mem.sort(u64, out[0..n], {}, std.sort.asc(u64));
        // Shrink the allocation to the actual element count so callers can
        // free with the returned length.
        return self.allocator.realloc(out, n) catch out[0..n];
    }
};

// KCOV_EXCL_START
test "joint committed index takes min of two majorities" {
    const allocator = std.testing.allocator;
    var idx = ack_indexer_mod.AckIndexer.init(allocator);
    defer idx.deinit();
    try idx.set(1, .{ .index = 10, .group_id = 0 });
    try idx.set(2, .{ .index = 20, .group_id = 0 });
    try idx.set(3, .{ .index = 30, .group_id = 0 });

    // Incoming {1,2,3} → sorted desc 30,20,10 → majority 2nd = 20.
    // Outgoing {2,3} → sorted desc 30,20 → majority 1st = 30.
    // Joint = min(20, 30) = 20.
    var jc = try JointConfiguration.fromIncomingOutgoing(allocator, &.{ 1, 2, 3 }, &.{ 2, 3 });
    defer jc.deinit();
    const r = jc.committedIndex(false, idx.indexer());
    try std.testing.expectEqual(@as(u64, 20), r.index);
}

test "joint vote result requires both sides" {
    const allocator = std.testing.allocator;

    const Checker = struct {
        yes: []const u64,
        no: []const u64,
        pub fn check(self: @This(), id: u64) ?bool {
            for (self.yes) |y| if (y == id) return true;
            for (self.no) |n| if (n == id) return false;
            return null;
        }
    };

    // Both sides {1,2} vote yes → won.
    {
        var jc = try JointConfiguration.fromIncomingOutgoing(allocator, &.{ 1, 2 }, &.{ 1, 2 });
        defer jc.deinit();
        const ch = Checker{ .yes = &.{ 1, 2 }, .no = &.{} };
        try std.testing.expectEqual(VoteResult.won, jc.getVoteResult(Checker, &ch));
    }
    // Incoming wins, outgoing missing → pending.
    {
        var jc = try JointConfiguration.fromIncomingOutgoing(allocator, &.{ 1, 2 }, &.{ 1, 2 });
        defer jc.deinit();
        const ch = Checker{ .yes = &.{1}, .no = &.{} };
        try std.testing.expectEqual(VoteResult.pending, jc.getVoteResult(Checker, &ch));
    }
    // Either side loses → lost.
    {
        var jc = try JointConfiguration.fromIncomingOutgoing(allocator, &.{ 1, 2 }, &.{ 1, 2 });
        defer jc.deinit();
        const ch = Checker{ .yes = &.{1}, .no = &.{2} };
        try std.testing.expectEqual(VoteResult.lost, jc.getVoteResult(Checker, &ch));
    }
}

test "joint contains and isSingleton" {
    const allocator = std.testing.allocator;
    var jc = try JointConfiguration.fromIncomingOutgoing(allocator, &.{1}, &.{});
    defer jc.deinit();
    try std.testing.expect(jc.contains(1));
    try std.testing.expect(!jc.contains(2));
    try std.testing.expect(jc.isSingleton());

    try jc.outgoing.add(2);
    try std.testing.expect(!jc.isSingleton());
}

test "joint ids unions incoming and outgoing without duplicates" {
    const allocator = std.testing.allocator;
    var jc = try JointConfiguration.fromIncomingOutgoing(allocator, &.{ 1, 2 }, &.{ 2, 3 });
    defer jc.deinit();
    const got = try jc.ids();
    defer allocator.free(got);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, got);
}
// KCOV_EXCL_STOP
