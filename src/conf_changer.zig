//! Membership change logic: enter/leave joint consensus, simple add/remove.
//!
//! `ConfChanger` is given a `ProgressTracker` reference and produces a new
//! `(TrackerConfiguration, MapChange)` pair without mutating the tracker.
//! The caller invokes `ProgressTracker.applyConf` to commit the result.
//!
//! `IncrChangeMap` layers a sequence of add/remove operations on top of an
//! existing `ProgressMap` so we can compute the final diff while the original
//! tracker stays untouched.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const tracker_conf_mod = @import("tracker_conf.zig");
const progress_mod = @import("progress.zig");
const progress_tracker_mod = @import("progress_tracker.zig");

const Error = error_model.Error;
const ConfChangeSingle = types.ConfChangeSingle;
const ConfChangeType = types.ConfChangeType;
const TrackerConfiguration = tracker_conf_mod.TrackerConfiguration;
const ProgressMap = progress_mod.ProgressMap;
const ProgressTracker = progress_tracker_mod.ProgressTracker;
const MapChangeEntry = progress_tracker_mod.MapChangeEntry;
const MapChangeKind = progress_tracker_mod.MapChangeKind;

const log = std.log.scoped(.raft_zig_conf_changer);

/// True when the configuration has both incoming and outgoing voter sets
/// (i.e. a joint consensus is active).
pub fn joint(cfg: TrackerConfiguration) bool {
    return !cfg.voters.outgoing.isEmpty();
}

/// Layered view of `base` plus a sequence of (id, Add/Remove) edits.
/// `contains` walks the latest edit for an id first, falling back to base.
pub const IncrChangeMap = struct {
    base: *const ProgressMap,
    changes: std.ArrayList(MapChangeEntry),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, base: *const ProgressMap) IncrChangeMap {
        return .{
            .base = base,
            .changes = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *IncrChangeMap) void {
        self.changes.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn contains(self: IncrChangeMap, id: u64) bool {
        // Walk changes in reverse to find the latest entry for `id`.
        if (self.changes.items.len > 0) {
            var i: usize = self.changes.items.len;
            while (i > 0) {
                i -= 1;
                const c = self.changes.items[i];
                if (c.id == id) return c.kind == .add;
            }
        }
        return self.base.contains(id);
    }

    pub fn appendChange(self: *IncrChangeMap, id: u64, kind: MapChangeKind) !void {
        try self.changes.append(self.allocator, .{ .id = id, .kind = kind });
    }

    /// Borrow the accumulated changes. The slice is invalidated by any
    /// subsequent `appendChange` on this IncrChangeMap.
    pub fn toChanges(self: IncrChangeMap) []const MapChangeEntry {
        return self.changes.items;
    }
};

/// Validate a proposed configuration against these invariants:
///   * every voter/learner has a Progress entry
///   * a learner can't also be a voter
///   * learners_next must remain within outgoing voters
///   * non-joint configs must have empty learners_next and auto_leave=false
pub fn checkInvariants(cfg: TrackerConfiguration, prs: IncrChangeMap) Error!void {
    const voters_ids = cfg.voters.ids() catch return error.OutOfMemory;
    defer cfg.allocator.free(voters_ids);
    for (voters_ids) |id| {
        if (!prs.contains(id)) {
            log.warn("no progress for voter {}", .{id});
            return error.ConfChangeError;
        }
    }

    var lit = cfg.learners.keyIterator();
    while (lit.next()) |k| {
        const id = k.*;
        if (!prs.contains(id)) {
            log.warn("no progress for learner {}", .{id});
            return error.ConfChangeError;
        }
        if (cfg.voters.outgoing.contains(id)) {
            log.warn("{} is in learners and outgoing voters", .{id});
            return error.ConfChangeError;
        }
        if (cfg.voters.incoming.contains(id)) {
            log.warn("{} is in learners and incoming voters", .{id});
            return error.ConfChangeError;
        }
    }

    var nit = cfg.learners_next.keyIterator();
    while (nit.next()) |k| {
        const id = k.*;
        if (!prs.contains(id)) {
            log.warn("no progress for learner(next) {}", .{id});
            return error.ConfChangeError;
        }
        if (!cfg.voters.outgoing.contains(id)) {
            log.warn("{} is in learners_next but not outgoing voters", .{id});
            return error.ConfChangeError;
        }
    }

    if (!joint(cfg)) {
        if (cfg.learners_next.count() != 0) return error.LearnersNextMustBeEmpty;
        if (cfg.auto_leave) return error.AutoLeaveMustBeFalse;
    }
    return;
}

/// Owned configuration + change list returned from ConfChanger methods.
/// The caller is responsible for `deinit`.
pub const ConfChangeResult = struct {
    conf: TrackerConfiguration,
    changes: []MapChangeEntry,

    pub fn deinit(self: *ConfChangeResult, allocator: std.mem.Allocator) void {
        self.conf.deinit();
        allocator.free(self.changes);
        self.* = undefined;
    }
};

pub const ConfChanger = struct {
    tracker: *ProgressTracker,

    pub fn init(tracker: *ProgressTracker) ConfChanger {
        return .{ .tracker = tracker };
    }

    /// Transition a simple config into a joint one: incoming becomes the
    /// auto_leave hint and a copy of incoming is added to outgoing.
    pub fn enterJoint(
        self: ConfChanger,
        auto_leave: bool,
        ccs: []const ConfChangeSingle,
    ) Error!ConfChangeResult {
        if (joint(self.tracker.conf)) return error.ConfigAlreadyJoint;

        var pair = try self.checkAndCopy();
        defer pair.deinit();
        const cfg = &pair.cfg;
        const prs = &pair.prs;

        if (cfg.voters.incoming.isEmpty()) return error.ZeroVoterConfigJoint;

        // Promote incoming into outgoing as well.
        var it = cfg.voters.incoming.voters.keyIterator();
        while (it.next()) |k| try cfg.voters.outgoing.add(k.*);

        try applyChanges(cfg, prs, ccs);
        cfg.auto_leave = auto_leave;
        try checkInvariants(cfg.*, prs.*);

        return try pair.finalize();
    }

    pub fn leaveJoint(self: ConfChanger) Error!ConfChangeResult {
        if (!joint(self.tracker.conf)) return error.LeaveNonJointConfig;

        var pair = try self.checkAndCopy();
        defer pair.deinit();
        const cfg = &pair.cfg;
        const prs = &pair.prs;

        if (cfg.voters.outgoing.isEmpty()) {
            log.warn("configuration is not joint", .{});
            return error.ConfChangeError;
        }

        // learners_next graduates into learners.
        var ln_it = cfg.learners_next.keyIterator();
        while (ln_it.next()) |k| try cfg.learners.put(k.*, {});

        cfg.learners_next.clearRetainingCapacity();

        // Drop outgoing-only voters.
        var out_it = cfg.voters.outgoing.voters.keyIterator();
        while (out_it.next()) |k| {
            const id = k.*;
            if (!cfg.voters.incoming.contains(id) and !cfg.learners.contains(id)) {
                try prs.appendChange(id, .remove);
            }
        }

        cfg.voters.outgoing.clear();
        cfg.auto_leave = false;
        try checkInvariants(cfg.*, prs.*);

        return try pair.finalize();
    }

    /// Apply a single atomic change to a non-joint config. Refuses to mutate
    /// more than one voter at a time.
    pub fn simple(self: ConfChanger, ccs: []const ConfChangeSingle) Error!ConfChangeResult {
        if (joint(self.tracker.conf)) return error.CannotApplySimpleInJointConfig;

        var pair = try self.checkAndCopy();
        defer pair.deinit();
        const cfg = &pair.cfg;
        const prs = &pair.prs;

        try applyChanges(cfg, prs, ccs);

        // Symmetric difference between new and old incoming voters must have
        // length ≤ 1, otherwise we'd be changing multiple voters without a
        // joint phase.
        const new_voters = try collectKeys(self.tracker.allocator, cfg.voters.incoming.voters);
        defer self.tracker.allocator.free(new_voters);
        const old_voters = try collectKeys(self.tracker.allocator, self.tracker.conf.voters.incoming.voters);
        defer self.tracker.allocator.free(old_voters);

        std.mem.sort(u64, new_voters, {}, std.sort.asc(u64));
        std.mem.sort(u64, old_voters, {}, std.sort.asc(u64));

        var diff_count: usize = 0;
        var i: usize = 0;
        var j: usize = 0;
        while (i < new_voters.len and j < old_voters.len) {
            if (new_voters[i] < old_voters[j]) {
                diff_count += 1;
                i += 1;
            } else if (new_voters[i] > old_voters[j]) {
                diff_count += 1;
                j += 1;
            } else {
                i += 1;
                j += 1;
            }
        }
        diff_count += (new_voters.len - i) + (old_voters.len - j);
        if (diff_count > 1) return error.MultipleVotersChangedWithoutJoint;

        try checkInvariants(cfg.*, prs.*);
        return try pair.finalize();
    }

    /// Snapshot the tracker's config + progress into a working copy.
    pub fn checkAndCopy(self: ConfChanger) Error!CheckAndCopyResult {
        var prs = IncrChangeMap.init(self.tracker.allocator, &self.tracker.progress);
        errdefer prs.deinit();
        var cfg = try self.tracker.conf.clone();
        errdefer cfg.deinit();
        try checkInvariants(self.tracker.conf, prs);
        return .{ .cfg = cfg, .prs = prs, .allocator = self.tracker.allocator };
    }

    /// Apply each ConfChangeSingle to `cfg`/`prs`.
    pub fn applyChanges(cfg: *TrackerConfiguration, prs: *IncrChangeMap, ccs: []const ConfChangeSingle) Error!void {
        for (ccs) |cc| {
            if (cc.node_id == 0) continue;
            switch (cc.change_type) {
                .add_node => try makeVoter(cfg, prs, cc.node_id),
                .remove_node => try remove(cfg, prs, cc.node_id),
                .add_learner_node => try makeLearner(cfg, prs, cc.node_id),
            }
        }
        if (cfg.voters.incoming.isEmpty()) return error.RemovedAllVoters;
    }

    fn initProgress(cfg: *TrackerConfiguration, prs: *IncrChangeMap, id: u64, is_learner: bool) Error!void {
        if (!is_learner) {
            try cfg.voters.incoming.add(id);
        } else {
            try cfg.learners.put(id, {});
        }
        try prs.appendChange(id, .add);
    }

    fn makeVoter(cfg: *TrackerConfiguration, prs: *IncrChangeMap, id: u64) Error!void {
        if (!prs.contains(id)) {
            try initProgress(cfg, prs, id, false);
            return;
        }
        try cfg.voters.incoming.add(id);
        _ = cfg.learners.remove(id);
        _ = cfg.learners_next.remove(id);
    }

    fn makeLearner(cfg: *TrackerConfiguration, prs: *IncrChangeMap, id: u64) Error!void {
        if (!prs.contains(id)) {
            try initProgress(cfg, prs, id, true);
            return;
        }
        if (cfg.learners.contains(id)) return;

        _ = cfg.voters.incoming.remove(id);
        _ = cfg.learners.remove(id);
        _ = cfg.learners_next.remove(id);

        if (cfg.voters.outgoing.contains(id)) {
            try cfg.learners_next.put(id, {});
        } else {
            try cfg.learners.put(id, {});
        }
    }

    fn remove(cfg: *TrackerConfiguration, prs: *IncrChangeMap, id: u64) Error!void {
        if (!prs.contains(id)) return;

        _ = cfg.voters.incoming.remove(id);
        _ = cfg.learners.remove(id);
        _ = cfg.learners_next.remove(id);

        // Keep the Progress entry if the peer is still a voter in the
        // outgoing config (joint transition).
        if (!cfg.voters.outgoing.contains(id)) {
            try prs.appendChange(id, .remove);
        }
    }
};

fn collectKeys(allocator: std.mem.Allocator, set: std.AutoHashMap(u64, void)) ![]u64 {
    const out = try allocator.alloc(u64, set.count());
    errdefer allocator.free(out);
    var i: usize = 0;
    var it = set.keyIterator();
    while (it.next()) |k| : (i += 1) out[i] = k.*;
    return out;
}

const CheckAndCopyResult = struct {
    cfg: TrackerConfiguration,
    prs: IncrChangeMap,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *CheckAndCopyResult) void {
        self.cfg.deinit();
        self.prs.deinit();
    }

    pub fn finalize(self: *CheckAndCopyResult) Error!ConfChangeResult {
        // Transfer changes ownership to the result. prs.changes becomes empty.
        const changes = try self.prs.changes.toOwnedSlice(self.allocator);
        // Move cfg out, leaving an empty replacement that is cheap to deinit
        // when the caller's defer pair.deinit() runs.
        const out_cfg = self.cfg;
        self.cfg = TrackerConfiguration.init(self.allocator);
        return .{
            .conf = out_cfg,
            .changes = changes,
        };
    }
};

// =========================================================================
// Tests
// =========================================================================

fn newConfChange(change_type: ConfChangeType, node_id: u64) ConfChangeSingle {
    return .{ .change_type = change_type, .node_id = node_id };
}

fn applySimple(tracker: *ProgressTracker, ccs: []const ConfChangeSingle) ![]const u8 {
    const allocator = tracker.allocator;
    var result = try ConfChanger.init(tracker).simple(ccs);
    defer result.deinit(allocator);
    try tracker.applyConf(result.conf, result.changes, 0);
    return "";
}

test "conf changer: simple add/remove rejects multi-voter changes" {
    const allocator = std.testing.allocator;
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();

    // Start from {1}.
    {
        const cc = [_]ConfChangeSingle{newConfChange(.add_node, 1)};
        var r = try ConfChanger.init(&tr).simple(&cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }

    // Adding two voters at once must fail.
    {
        const cc = [_]ConfChangeSingle{ newConfChange(.add_node, 2), newConfChange(.add_node, 3) };
        try std.testing.expectError(error.MultipleVotersChangedWithoutJoint, ConfChanger.init(&tr).simple(&cc));
    }

    // Removing all voters fails with RemovedAllVoters.
    {
        const cc = [_]ConfChangeSingle{newConfChange(.remove_node, 1)};
        try std.testing.expectError(error.RemovedAllVoters, ConfChanger.init(&tr).simple(&cc));
    }
}

test "conf changer: enter then leave joint" {
    const allocator = std.testing.allocator;
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();

    // Bootstrap with {1, 2} via two simple adds (enterJoint refuses empty incoming).
    {
        const cc = [_]ConfChangeSingle{newConfChange(.add_node, 1)};
        var r = try ConfChanger.init(&tr).simple(&cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }
    {
        const cc = [_]ConfChangeSingle{newConfChange(.add_node, 2)};
        var r = try ConfChanger.init(&tr).simple(&cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }

    // Now enter joint to add voter 3 — incoming {1,2,3} graduates to outgoing {1,2}.
    {
        const cc = [_]ConfChangeSingle{newConfChange(.add_node, 3)};
        var r = try ConfChanger.init(&tr).enterJoint(false, &cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }

    try std.testing.expect(joint(tr.conf));

    // Leave joint: outgoing cleared, incoming {1,2,3} remains.
    {
        var r = try ConfChanger.init(&tr).leaveJoint();
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }
    try std.testing.expect(!joint(tr.conf));
    try std.testing.expectEqual(@as(usize, 3), tr.conf.voters.incoming.count());
}

test "conf changer: enter joint twice fails" {
    const allocator = std.testing.allocator;
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();

    // Bootstrap with simple.
    {
        const cc = [_]ConfChangeSingle{newConfChange(.add_node, 1)};
        var r = try ConfChanger.init(&tr).simple(&cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }
    {
        const cc = [_]ConfChangeSingle{newConfChange(.add_node, 2)};
        var r = try ConfChanger.init(&tr).enterJoint(false, &cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }
    // Try to add another via EnterJoint again — should refuse because we're already joint.
    {
        const cc = [_]ConfChangeSingle{newConfChange(.add_node, 3)};
        try std.testing.expectError(error.ConfigAlreadyJoint, ConfChanger.init(&tr).enterJoint(false, &cc));
    }
}

test "incr change map contains reflects latest edit" {
    var pm = ProgressMap.init(std.testing.allocator);
    defer pm.deinit();
    try pm.put(1, progress_mod.Progress.init(std.testing.allocator, 1, 4));

    var im = IncrChangeMap.init(std.testing.allocator, &pm);
    defer im.deinit();

    try std.testing.expect(im.contains(1)); // present in base
    try std.testing.expect(!im.contains(2));

    try im.appendChange(2, .add);
    try std.testing.expect(im.contains(2));

    try im.appendChange(2, .remove);
    try std.testing.expect(!im.contains(2));

    try im.appendChange(1, .remove);
    try std.testing.expect(!im.contains(1)); // removed overrides base
}
