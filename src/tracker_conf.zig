//! Top-level cluster configuration wrapper.
//!
//! `TrackerConfiguration` layers learners on top of `JointConfiguration`:
//!   * `voters` (incoming/outgoing majorities)
//!   * `learners` (current learner set)
//!   * `learners_next` (peers becoming learners after leaving the joint state)
//!   * `auto_leave` (joint state leaves automatically once committed)

const std = @import("std");

const joint_conf_mod = @import("joint_conf.zig");
const types = @import("core/types.zig");

const JointConfiguration = joint_conf_mod.JointConfiguration;
const ConfState = types.ConfState;

pub const TrackerConfiguration = struct {
    voters: JointConfiguration,
    learners: std.AutoHashMap(u64, void),
    learners_next: std.AutoHashMap(u64, void),
    auto_leave: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TrackerConfiguration {
        return .{
            .voters = JointConfiguration.init(allocator),
            .learners = std.AutoHashMap(u64, void).init(allocator),
            .learners_next = std.AutoHashMap(u64, void).init(allocator),
            .auto_leave = false,
            .allocator = allocator,
        };
    }

    pub fn fromVotersLearners(
        allocator: std.mem.Allocator,
        voters: []const u64,
        learners: []const u64,
    ) !TrackerConfiguration {
        var out = TrackerConfiguration.init(allocator);
        errdefer out.deinit();
        for (voters) |id| try out.voters.incoming.add(id);
        for (learners) |id| try out.learners.put(id, {});
        return out;
    }

    pub fn deinit(self: *TrackerConfiguration) void {
        self.voters.deinit();
        self.learners.deinit();
        self.learners_next.deinit();
        self.* = undefined;
    }

    pub fn clone(self: TrackerConfiguration) !TrackerConfiguration {
        var out = TrackerConfiguration.init(self.allocator);
        errdefer out.deinit();
        var it = self.voters.incoming.voters.keyIterator();
        while (it.next()) |k| try out.voters.incoming.add(k.*);
        var ot = self.voters.outgoing.voters.keyIterator();
        while (ot.next()) |k| try out.voters.outgoing.add(k.*);
        var lt = self.learners.keyIterator();
        while (lt.next()) |k| try out.learners.put(k.*, {});
        var nt = self.learners_next.keyIterator();
        while (nt.next()) |k| try out.learners_next.put(k.*, {});
        out.auto_leave = self.auto_leave;
        return out;
    }

    pub fn clear(self: *TrackerConfiguration) void {
        self.voters.clear();
        self.learners.clearRetainingCapacity();
        self.learners_next.clearRetainingCapacity();
        self.auto_leave = false;
    }

    /// Build a wire-format ConfState. Caller owns the resulting slices.
    pub fn toConfState(self: TrackerConfiguration, allocator: std.mem.Allocator) !ConfState {
        const voters = try collectSorted(allocator, self.voters.incoming.voters);
        errdefer allocator.free(voters);
        const learners = try collectSorted(allocator, self.learners);
        errdefer allocator.free(learners);
        const voters_outgoing = try collectSorted(allocator, self.voters.outgoing.voters);
        errdefer allocator.free(voters_outgoing);
        const learners_next = try collectSorted(allocator, self.learners_next);
        return .{
            .voters = voters,
            .learners = learners,
            .voters_outgoing = voters_outgoing,
            .learners_next = learners_next,
            .auto_leave = self.auto_leave,
        };
    }
};

/// Copy keys from a `std.AutoHashMap(u64, void)` into a sorted, freshly
/// allocated slice. The caller owns the slice.
fn collectSorted(allocator: std.mem.Allocator, set: std.AutoHashMap(u64, void)) ![]u64 {
    const out = try allocator.alloc(u64, set.count());
    errdefer allocator.free(out);
    var i: usize = 0;
    var it = set.keyIterator();
    while (it.next()) |k| : (i += 1) out[i] = k.*;
    std.mem.sort(u64, out, {}, std.sort.asc(u64));
    return out;
}

// KCOV_EXCL_START
test "tracker configuration round-trips through ConfState" {
    const allocator = std.testing.allocator;
    var tc = try TrackerConfiguration.fromVotersLearners(allocator, &.{ 3, 1, 2 }, &.{ 5, 4 });
    defer tc.deinit();

    var cs = try tc.toConfState(allocator);
    defer cs.deinit(allocator);

    // Voters are sorted by toConfState for stable wire format.
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, cs.voters);
    try std.testing.expectEqualSlices(u64, &.{ 4, 5 }, cs.learners);
    try std.testing.expectEqualSlices(u64, &.{}, cs.voters_outgoing);
    try std.testing.expectEqualSlices(u64, &.{}, cs.learners_next);
    try std.testing.expectEqual(false, cs.auto_leave);
}

test "tracker configuration clone is deep" {
    const allocator = std.testing.allocator;
    var tc = try TrackerConfiguration.fromVotersLearners(allocator, &.{1}, &.{2});
    defer tc.deinit();
    try tc.voters.outgoing.add(3);
    try tc.learners_next.put(4, {});
    tc.auto_leave = true;

    var copy = try tc.clone();
    defer copy.deinit();

    try copy.voters.incoming.add(99);
    try std.testing.expect(tc.voters.incoming.count() == 1);
    try std.testing.expect(copy.voters.incoming.count() == 2);
    try std.testing.expect(copy.auto_leave);
}

test "tracker configuration clear resets everything" {
    const allocator = std.testing.allocator;
    var tc = try TrackerConfiguration.fromVotersLearners(allocator, &.{1}, &.{2});
    defer tc.deinit();
    try tc.learners_next.put(3, {});
    tc.auto_leave = true;

    tc.clear();
    try std.testing.expect(tc.voters.incoming.isEmpty());
    try std.testing.expect(tc.learners.count() == 0);
    try std.testing.expect(tc.learners_next.count() == 0);
    try std.testing.expect(!tc.auto_leave);
}

test "toConfState cleans up allocation failures" {
    const Helper = struct {
        fn run(allocator: std.mem.Allocator, conf: *const TrackerConfiguration) !void {
            var state = try conf.toConfState(allocator);
            defer state.deinit(allocator);
        }
    };

    var conf = try TrackerConfiguration.fromVotersLearners(std.testing.allocator, &.{ 1, 2 }, &.{3});
    defer conf.deinit();
    try conf.voters.outgoing.add(4);
    try conf.learners_next.put(4, {});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helper.run, .{&conf});
}
// KCOV_EXCL_STOP
