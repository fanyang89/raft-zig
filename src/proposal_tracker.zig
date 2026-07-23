//! Proposal and read-index tracking with callbacks.
//!
//! Ports `include/raftpp/raftor/proposal_tracker.h`. When a user proposes
//! data or requests a read-index, the tracker registers a callback keyed by
//! the entry's context bytes. When the corresponding entry is applied (or
//! the read-index is confirmed), the callback fires.
//!
//! Thread safety is omitted: Zig 0.16's single-threaded event loop model
//! means proposals and completions happen on the same thread. Cross-thread
//! submission would use a queue external to this struct.

const std = @import("std");

const error_model = @import("core/error.zig");

const Error = error_model.Error;

/// Result delivered to a proposal callback.
pub const ProposalResult = union(enum) {
    ok: []const u8,
    err: Error,
};

/// Result delivered to a read-index callback.
pub const ReadIndexResult = union(enum) {
    ok,
    err: Error,
};

/// Type-erased callback for proposal completion. The `result` is a
/// `ProposalResult` by value; the `ok` slice is borrowed from the tracker
/// and remains valid until the callback returns.
pub const ProposalCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, result: ProposalResult) void,

    pub fn invoke(self: ProposalCallback, result: ProposalResult) void {
        self.function(self.ctx, result);
    }
};

/// Type-erased callback for read-index completion.
pub const ReadIndexCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, result: ReadIndexResult) void,

    pub fn invoke(self: ReadIndexCallback, result: ReadIndexResult) void {
        self.function(self.ctx, result);
    }
};

const PendingProposal = struct {
    callback: ProposalCallback,
    deadline_tick: u64,
};

const PendingRead = struct {
    callback: ReadIndexCallback,
    deadline_tick: u64,
    ready_index: ?u64 = null,
};

pub const ProposalTracker = struct {
    proposals: std.StringHashMap(PendingProposal),
    reads: std.StringHashMap(PendingRead),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ProposalTracker {
        return .{
            .proposals = std.StringHashMap(PendingProposal).init(allocator),
            .reads = std.StringHashMap(PendingRead).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ProposalTracker) void {
        // Free owned key bytes.
        var pi = self.proposals.keyIterator();
        while (pi.next()) |k| self.allocator.free(k.*);
        self.proposals.deinit();
        var ri = self.reads.keyIterator();
        while (ri.next()) |k| self.allocator.free(k.*);
        self.reads.deinit();
        self.* = undefined;
    }

    /// Register a proposal. `ctx_bytes` is duped internally; the caller
    /// retains ownership of the input. `timeout_ticks` of 0 means no timeout.
    pub fn track(self: *ProposalTracker, ctx_bytes: []const u8, callback: ProposalCallback, current_tick: u64, timeout_ticks: u64) !void {
        if (self.proposals.contains(ctx_bytes)) return error.DuplicateRequest;
        const key = try self.allocator.dupe(u8, ctx_bytes);
        errdefer self.allocator.free(key);
        try self.proposals.put(key, .{
            .callback = callback,
            .deadline_tick = if (timeout_ticks == 0) std.math.maxInt(u64) else current_tick + timeout_ticks,
        });
    }

    /// Complete a proposal successfully. The response slice is passed to
    /// the callback and need not survive after the callback returns.
    pub fn complete(self: *ProposalTracker, ctx_bytes: []const u8, response: []const u8) void {
        const kv = self.proposals.fetchRemove(ctx_bytes) orelse return;
        self.allocator.free(kv.key);
        kv.value.callback.invoke(.{ .ok = response });
    }

    /// Fail a proposal with an error.
    pub fn fail(self: *ProposalTracker, ctx_bytes: []const u8, err: Error) void {
        const kv = self.proposals.fetchRemove(ctx_bytes) orelse return;
        self.allocator.free(kv.key);
        kv.value.callback.invoke(.{ .err = err });
    }

    /// Fail every pending proposal (e.g. on leadership loss or shutdown).
    pub fn failAll(self: *ProposalTracker, err: Error) void {
        var it = self.proposals.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.callback.invoke(.{ .err = err });
        }
        // Free keys and clear.
        var ki = self.proposals.keyIterator();
        while (ki.next()) |k| self.allocator.free(k.*);
        self.proposals.clearRetainingCapacity();
    }

    /// Fail every pending read.
    pub fn failAllReads(self: *ProposalTracker, err: Error) void {
        var it = self.reads.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.callback.invoke(.{ .err = err });
        }
        var ki = self.reads.keyIterator();
        while (ki.next()) |k| self.allocator.free(k.*);
        self.reads.clearRetainingCapacity();
    }

    /// Register a read-index request.
    pub fn trackRead(self: *ProposalTracker, ctx_bytes: []const u8, callback: ReadIndexCallback, current_tick: u64, timeout_ticks: u64) !void {
        if (self.reads.contains(ctx_bytes)) return error.DuplicateRequest;
        const key = try self.allocator.dupe(u8, ctx_bytes);
        errdefer self.allocator.free(key);
        try self.reads.put(key, .{
            .callback = callback,
            .deadline_tick = if (timeout_ticks == 0) std.math.maxInt(u64) else current_tick + timeout_ticks,
        });
    }

    pub fn completeRead(self: *ProposalTracker, ctx_bytes: []const u8) void {
        const kv = self.reads.fetchRemove(ctx_bytes) orelse return;
        self.allocator.free(kv.key);
        kv.value.callback.invoke(.ok);
    }

    pub fn markReadReady(self: *ProposalTracker, ctx_bytes: []const u8, index: u64) void {
        const pending = self.reads.getPtr(ctx_bytes) orelse return;
        pending.ready_index = if (pending.ready_index) |current| @max(current, index) else index;
    }

    pub fn completeReadyReads(self: *ProposalTracker, applied_index: u64) void {
        while (true) {
            var ready_ctx: ?[]const u8 = null;
            var it = self.reads.iterator();
            while (it.next()) |entry| {
                const ready_index = entry.value_ptr.ready_index orelse continue;
                if (ready_index <= applied_index) {
                    ready_ctx = entry.key_ptr.*;
                    break;
                }
            }
            self.completeRead(ready_ctx orelse return);
        }
    }

    pub fn failRead(self: *ProposalTracker, ctx_bytes: []const u8, err: Error) void {
        const kv = self.reads.fetchRemove(ctx_bytes) orelse return;
        self.allocator.free(kv.key);
        kv.value.callback.invoke(.{ .err = err });
    }

    pub fn pendingCount(self: ProposalTracker) usize {
        return self.proposals.count();
    }

    pub fn pendingReadCount(self: ProposalTracker) usize {
        return self.reads.count();
    }

    pub fn isReadPending(self: ProposalTracker, ctx_bytes: []const u8) bool {
        return self.reads.contains(ctx_bytes);
    }

    /// Expire proposals and reads whose deadline has passed.
    pub fn expireTimeouts(self: *ProposalTracker, current_tick: u64) void {
        // Proposals.
        var to_remove_p: std.ArrayList([]const u8) = .empty;
        defer to_remove_p.deinit(self.allocator);
        var pi = self.proposals.iterator();
        while (pi.next()) |entry| {
            if (current_tick >= entry.value_ptr.deadline_tick) {
                to_remove_p.append(self.allocator, entry.key_ptr.*) catch break;
            }
        }
        for (to_remove_p.items) |key| {
            if (self.proposals.fetchRemove(key)) |kv| {
                kv.value.callback.invoke(.{ .err = error.Timeout });
                self.allocator.free(kv.key);
            }
        }

        // Reads.
        var to_remove_r: std.ArrayList([]const u8) = .empty;
        defer to_remove_r.deinit(self.allocator);
        var ri = self.reads.iterator();
        while (ri.next()) |entry| {
            if (current_tick >= entry.value_ptr.deadline_tick) {
                to_remove_r.append(self.allocator, entry.key_ptr.*) catch break;
            }
        }
        for (to_remove_r.items) |key| {
            if (self.reads.fetchRemove(key)) |kv| {
                kv.value.callback.invoke(.{ .err = error.Timeout });
                self.allocator.free(kv.key);
            }
        }
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const Tester = struct {
    result: ?ProposalResult = null,

    fn proposalCb(ctx: *anyopaque, result: ProposalResult) void {
        const self: *Tester = @ptrCast(@alignCast(ctx));
        self.result = result;
    }

    fn proposalCallback(self: *Tester) ProposalCallback {
        return .{ .ctx = self, .function = proposalCb };
    }
};

const ReadTester = struct {
    result: ?ReadIndexResult = null,

    fn readCb(ctx: *anyopaque, result: ReadIndexResult) void {
        const self: *ReadTester = @ptrCast(@alignCast(ctx));
        self.result = result;
    }

    fn readCallback(self: *ReadTester) ReadIndexCallback {
        return .{ .ctx = self, .function = readCb };
    }
};

test "proposal tracker track and complete" {
    const allocator = std.testing.allocator;
    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();

    var tester = Tester{};
    try tracker.track("ctx1", tester.proposalCallback(), 0, 0);
    try std.testing.expectEqual(@as(usize, 1), tracker.pendingCount());

    tracker.complete("ctx1", "response_data");
    try std.testing.expect(tester.result != null);
    try std.testing.expectEqualStrings("response_data", tester.result.?.ok);
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());
}

test "proposal tracker rejects duplicate contexts" {
    const allocator = std.testing.allocator;
    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();

    var proposal = Tester{};
    try tracker.track("proposal", proposal.proposalCallback(), 0, 0);
    try std.testing.expectError(error.DuplicateRequest, tracker.track("proposal", proposal.proposalCallback(), 0, 0));

    var read = ReadTester{};
    try tracker.trackRead("read", read.readCallback(), 0, 0);
    try std.testing.expectError(error.DuplicateRequest, tracker.trackRead("read", read.readCallback(), 0, 0));
}

test "proposal tracker fail and failAll" {
    const allocator = std.testing.allocator;
    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();

    var t1 = Tester{};
    var t2 = Tester{};
    try tracker.track("a", t1.proposalCallback(), 0, 0);
    try tracker.track("b", t2.proposalCallback(), 0, 0);

    tracker.fail("a", error.ProposalDropped);
    try std.testing.expectEqual(error.ProposalDropped, t1.result.?.err);

    tracker.failAll(error.LostLeadership);
    try std.testing.expectEqual(error.LostLeadership, t2.result.?.err);
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());
}

test "proposal tracker expire timeouts" {
    const allocator = std.testing.allocator;
    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();

    var t = Tester{};
    try tracker.track("ctx", t.proposalCallback(), 100, 50);
    // Deadline is tick 150. At tick 140, not expired.
    tracker.expireTimeouts(140);
    try std.testing.expectEqual(@as(usize, 1), tracker.pendingCount());

    // At tick 150, expired.
    tracker.expireTimeouts(150);
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());
    try std.testing.expectEqual(error.Timeout, t.result.?.err);
}

test "proposal tracker completes ready reads after apply" {
    const allocator = std.testing.allocator;
    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();

    var read = ReadTester{};
    try tracker.trackRead("read", read.readCallback(), 0, 0);
    tracker.markReadReady("read", 5);
    tracker.completeReadyReads(4);
    try std.testing.expect(read.result == null);
    try std.testing.expectEqual(@as(usize, 1), tracker.pendingReadCount());

    tracker.completeReadyReads(5);
    switch (read.result.?) {
        .ok => {},
        .err => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingReadCount());
}

test "proposal tracker ignores ready state after read timeout" {
    const allocator = std.testing.allocator;
    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();

    var read = ReadTester{};
    try tracker.trackRead("read", read.readCallback(), 0, 1);
    tracker.expireTimeouts(1);
    try std.testing.expectEqual(error.Timeout, read.result.?.err);

    tracker.markReadReady("read", 5);
    tracker.completeReadyReads(5);
    try std.testing.expectEqual(error.Timeout, read.result.?.err);
}
