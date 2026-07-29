//! Vote outcome enumeration and acknowledged-index lookup interface.
//!
//! `AckedIndexer` is the read-only interface used by quorum math in
//! `majority_conf` / `joint_conf`; the concrete `AckIndexer` is a thin map
//! wrapper, while `ProgressMap` (added in
//! the progress module) implements the same vtable backed by live Progress
//! state.

const std = @import("std");

/// Outcome of a vote tally.
pub const VoteResult = enum(u8) { pending, lost, won };

/// A voter's latest acknowledged (match) index plus the group it belongs to.
pub const Index = struct {
    index: u64,
    group_id: u64,
};

/// Type-erased `?Index` lookup over a voter set. Implementations store their
/// own progress data and expose it through this vtable so quorum functions
/// stay generic.
pub const AckedIndexer = struct {
    ctx: *const anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        acked_index: *const fn (ctx: *const anyopaque, voter: u64) ?Index,
    };

    pub fn ackedIndex(self: AckedIndexer, voter: u64) ?Index {
        return self.vtable.acked_index(self.ctx, voter);
    }
};

/// Simple `voter -> Index` map for tests and standalone quorum checks.
pub const AckIndexer = struct {
    map: std.AutoHashMap(u64, Index),

    pub fn init(allocator: std.mem.Allocator) AckIndexer {
        return .{ .map = std.AutoHashMap(u64, Index).init(allocator) };
    }

    pub fn deinit(self: *AckIndexer) void {
        self.map.deinit();
        self.* = undefined;
    }

    pub fn set(self: *AckIndexer, voter: u64, idx: Index) !void {
        try self.map.put(voter, idx);
    }

    pub fn ackedIndex(self: *const AckIndexer, voter: u64) ?Index {
        return self.map.get(voter);
    }

    fn ackedIndexImpl(ctx: *const anyopaque, voter: u64) ?Index {
        const self: *const AckIndexer = @ptrCast(@alignCast(@constCast(ctx)));
        return self.ackedIndex(voter);
    }

    pub const vtable: AckedIndexer.VTable = .{ .acked_index = ackedIndexImpl };

    /// Borrow `self` as a generic indexer. The returned value is invalidated
    /// when `self` is moved or destroyed.
    pub fn indexer(self: *const AckIndexer) AckedIndexer {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

// KCOV_EXCL_START
test "ack indexer hit and miss" {
    var ack = AckIndexer.init(std.testing.allocator);
    defer ack.deinit();

    try std.testing.expect(ack.ackedIndex(1) == null);
    try ack.set(1, .{ .index = 5, .group_id = 7 });
    try ack.set(2, .{ .index = 9, .group_id = 7 });

    const found = ack.ackedIndex(1).?;
    try std.testing.expectEqual(@as(u64, 5), found.index);
    try std.testing.expectEqual(@as(u64, 7), found.group_id);
    try std.testing.expect(ack.ackedIndex(3) == null);
}

test "ack indexer vtable dispatch matches direct call" {
    var ack = AckIndexer.init(std.testing.allocator);
    defer ack.deinit();
    try ack.set(1, .{ .index = 4, .group_id = 0 });

    const generic = ack.indexer();
    const direct = ack.ackedIndex(1).?;
    const via_vtable = generic.ackedIndex(1).?;
    try std.testing.expectEqual(direct.index, via_vtable.index);
    try std.testing.expectEqual(direct.group_id, via_vtable.group_id);
    try std.testing.expect(generic.ackedIndex(99) == null);
}
// KCOV_EXCL_STOP
