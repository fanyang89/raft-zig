//! Proposal and read-index tracking with callbacks.
//!
//! When a user proposes data or requests a read-index, the tracker registers a
//! callback keyed by the entry's context bytes. When the corresponding entry is applied (or
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

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub const DetachedCallbacks = struct {
    proposals: std.StringHashMap(PendingProposal),
    reads: std.StringHashMap(PendingRead),
    allocator: std.mem.Allocator,

    pub fn invoke(self: *DetachedCallbacks, proposal_error: Error, read_error: Error) void {
        invokeProposals(self.allocator, &self.proposals, proposal_error);
        invokeReads(self.allocator, &self.reads, read_error);
        self.* = undefined;
    }
};

pub const ProposalTracker = struct {
    proposals: std.StringHashMap(PendingProposal),
    reads: std.StringHashMap(PendingRead),
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(allocator: std.mem.Allocator) ProposalTracker {
        return .{
            .proposals = std.StringHashMap(PendingProposal).init(allocator),
            .reads = std.StringHashMap(PendingRead).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ProposalTracker) void {
        spinLock(&self.mutex);
        // Free owned key bytes.
        var pi = self.proposals.keyIterator();
        while (pi.next()) |k| self.allocator.free(k.*);
        self.proposals.deinit();
        var ri = self.reads.keyIterator();
        while (ri.next()) |k| self.allocator.free(k.*);
        self.reads.deinit();
        self.mutex.unlock();
        self.* = undefined;
    }

    /// Register a proposal. `ctx_bytes` is duped internally; the caller
    /// retains ownership of the input. `timeout_ticks` of 0 means no timeout.
    pub fn track(self: *ProposalTracker, ctx_bytes: []const u8, callback: ProposalCallback, current_tick: u64, timeout_ticks: u64) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.proposals.contains(ctx_bytes)) return error.DuplicateRequest;
        const key = try self.allocator.dupe(u8, ctx_bytes);
        errdefer self.allocator.free(key);
        try self.proposals.put(key, .{
            .callback = callback,
            .deadline_tick = if (timeout_ticks == 0) std.math.maxInt(u64) else current_tick +| timeout_ticks,
        });
    }

    /// Complete a proposal successfully. The response slice is passed to
    /// the callback and need not survive after the callback returns.
    pub fn complete(self: *ProposalTracker, ctx_bytes: []const u8, response: []const u8) void {
        spinLock(&self.mutex);
        const kv = self.proposals.fetchRemove(ctx_bytes) orelse {
            self.mutex.unlock();
            return;
        };
        self.mutex.unlock();
        self.allocator.free(kv.key);
        kv.value.callback.invoke(.{ .ok = response });
    }

    /// Fail a proposal with an error.
    pub fn fail(self: *ProposalTracker, ctx_bytes: []const u8, err: Error) void {
        spinLock(&self.mutex);
        const kv = self.proposals.fetchRemove(ctx_bytes) orelse {
            self.mutex.unlock();
            return;
        };
        self.mutex.unlock();
        self.allocator.free(kv.key);
        kv.value.callback.invoke(.{ .err = err });
    }

    /// Fail every pending proposal (e.g. on leadership loss or shutdown).
    pub fn failAll(self: *ProposalTracker, err: Error) void {
        spinLock(&self.mutex);
        var detached = self.proposals;
        self.proposals = std.StringHashMap(PendingProposal).init(self.allocator);
        self.mutex.unlock();
        invokeProposals(self.allocator, &detached, err);
    }

    /// Fail every pending read.
    pub fn failAllReads(self: *ProposalTracker, err: Error) void {
        spinLock(&self.mutex);
        var detached = self.reads;
        self.reads = std.StringHashMap(PendingRead).init(self.allocator);
        self.mutex.unlock();
        invokeReads(self.allocator, &detached, err);
    }

    pub fn detachAll(self: *ProposalTracker) DetachedCallbacks {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const detached = DetachedCallbacks{
            .proposals = self.proposals,
            .reads = self.reads,
            .allocator = self.allocator,
        };
        self.proposals = std.StringHashMap(PendingProposal).init(self.allocator);
        self.reads = std.StringHashMap(PendingRead).init(self.allocator);
        return detached;
    }

    /// Register a read-index request.
    pub fn trackRead(self: *ProposalTracker, ctx_bytes: []const u8, callback: ReadIndexCallback, current_tick: u64, timeout_ticks: u64) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.reads.contains(ctx_bytes)) return error.DuplicateRequest;
        const key = try self.allocator.dupe(u8, ctx_bytes);
        errdefer self.allocator.free(key);
        try self.reads.put(key, .{
            .callback = callback,
            .deadline_tick = if (timeout_ticks == 0) std.math.maxInt(u64) else current_tick +| timeout_ticks,
        });
    }

    pub fn completeRead(self: *ProposalTracker, ctx_bytes: []const u8) void {
        spinLock(&self.mutex);
        const kv = self.reads.fetchRemove(ctx_bytes) orelse {
            self.mutex.unlock();
            return;
        };
        self.mutex.unlock();
        self.allocator.free(kv.key);
        kv.value.callback.invoke(.ok);
    }

    pub fn markReadReady(self: *ProposalTracker, ctx_bytes: []const u8, index: u64) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const pending = self.reads.getPtr(ctx_bytes) orelse return;
        pending.ready_index = if (pending.ready_index) |current| @max(current, index) else index;
    }

    pub fn completeReadyReads(self: *ProposalTracker, applied_index: u64) void {
        while (true) {
            spinLock(&self.mutex);
            var ready_ctx: ?[]const u8 = null;
            var it = self.reads.iterator();
            while (it.next()) |entry| {
                const ready_index = entry.value_ptr.ready_index orelse continue;
                if (ready_index <= applied_index) {
                    ready_ctx = entry.key_ptr.*;
                    break;
                }
            }
            const ctx = ready_ctx orelse {
                self.mutex.unlock();
                return;
            };
            const kv = self.reads.fetchRemove(ctx).?;
            self.mutex.unlock();
            self.allocator.free(kv.key);
            kv.value.callback.invoke(.ok);
        }
    }

    pub fn failRead(self: *ProposalTracker, ctx_bytes: []const u8, err: Error) void {
        spinLock(&self.mutex);
        const kv = self.reads.fetchRemove(ctx_bytes) orelse {
            self.mutex.unlock();
            return;
        };
        self.mutex.unlock();
        self.allocator.free(kv.key);
        kv.value.callback.invoke(.{ .err = err });
    }

    pub fn pendingCount(self: *const ProposalTracker) usize {
        const mutex = @constCast(&self.mutex);
        spinLock(mutex);
        defer mutex.unlock();
        return self.proposals.count();
    }

    pub fn pendingReadCount(self: *const ProposalTracker) usize {
        const mutex = @constCast(&self.mutex);
        spinLock(mutex);
        defer mutex.unlock();
        return self.reads.count();
    }

    pub fn isReadPending(self: *const ProposalTracker, ctx_bytes: []const u8) bool {
        const mutex = @constCast(&self.mutex);
        spinLock(mutex);
        defer mutex.unlock();
        return self.reads.contains(ctx_bytes);
    }

    /// Expire proposals and reads whose deadline has passed.
    pub fn expireTimeouts(self: *ProposalTracker, current_tick: u64) void {
        while (detachExpiredProposal(self, current_tick)) |kv| {
            self.allocator.free(kv.key);
            kv.value.callback.invoke(.{ .err = error.Timeout });
        }
        while (detachExpiredRead(self, current_tick)) |kv| {
            self.allocator.free(kv.key);
            kv.value.callback.invoke(.{ .err = error.Timeout });
        }
    }
};

fn invokeProposals(allocator: std.mem.Allocator, proposals: *std.StringHashMap(PendingProposal), err: Error) void {
    while (proposals.count() > 0) {
        var iterator = proposals.iterator();
        const entry = iterator.next().?;
        const removed = proposals.fetchRemove(entry.key_ptr.*).?;
        allocator.free(removed.key);
        removed.value.callback.invoke(.{ .err = err });
    }
    proposals.deinit();
}

fn invokeReads(allocator: std.mem.Allocator, reads: *std.StringHashMap(PendingRead), err: Error) void {
    while (reads.count() > 0) {
        var iterator = reads.iterator();
        const entry = iterator.next().?;
        const removed = reads.fetchRemove(entry.key_ptr.*).?;
        allocator.free(removed.key);
        removed.value.callback.invoke(.{ .err = err });
    }
    reads.deinit();
}

fn detachExpiredProposal(self: *ProposalTracker, current_tick: u64) ?std.StringHashMap(PendingProposal).KV {
    spinLock(&self.mutex);
    defer self.mutex.unlock();
    var iterator = self.proposals.iterator();
    while (iterator.next()) |entry| {
        if (current_tick >= entry.value_ptr.deadline_tick) return self.proposals.fetchRemove(entry.key_ptr.*);
    }
    return null;
}

fn detachExpiredRead(self: *ProposalTracker, current_tick: u64) ?std.StringHashMap(PendingRead).KV {
    spinLock(&self.mutex);
    defer self.mutex.unlock();
    var iterator = self.reads.iterator();
    while (iterator.next()) |entry| {
        if (current_tick >= entry.value_ptr.deadline_tick) return self.reads.fetchRemove(entry.key_ptr.*);
    }
    return null;
}

// ===========================================================================
// Tests
// ===========================================================================

// KCOV_EXCL_START
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

test "proposal tracker failRead fails only the matching read" {
    var tracker = ProposalTracker.init(std.testing.allocator);
    defer tracker.deinit();
    var first = ReadTester{};
    var second = ReadTester{};
    try tracker.trackRead("first", first.readCallback(), 0, 0);
    try tracker.trackRead("second", second.readCallback(), 0, 0);

    tracker.failRead("missing", error.LostLeadership);
    tracker.failRead("first", error.LostLeadership);

    try std.testing.expectEqual(error.LostLeadership, first.result.?.err);
    try std.testing.expect(second.result == null);
    try std.testing.expectEqual(@as(usize, 1), tracker.pendingReadCount());
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

test "proposal tracker ignores contexts from an old incarnation" {
    const allocator = std.testing.allocator;
    const request_context = @import("request_context.zig");
    var old_generator = request_context.Generator.init(1, 1);
    var new_generator = request_context.Generator.init(1, 2);
    const old_proposal = try old_generator.next(allocator, .proposal, "");
    defer allocator.free(old_proposal);
    const new_proposal = try new_generator.next(allocator, .proposal, "");
    defer allocator.free(new_proposal);
    const old_read = try old_generator.next(allocator, .read_index, "same");
    defer allocator.free(old_read);
    const new_read = try new_generator.next(allocator, .read_index, "same");
    defer allocator.free(new_read);

    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();
    var proposal = Tester{};
    try tracker.track(new_proposal, proposal.proposalCallback(), 0, 0);
    tracker.complete(old_proposal, "old");
    try std.testing.expect(proposal.result == null);
    tracker.complete(new_proposal, "new");
    try std.testing.expectEqualStrings("new", proposal.result.?.ok);

    var read = ReadTester{};
    try tracker.trackRead(new_read, read.readCallback(), 0, 0);
    tracker.markReadReady(old_read, 1);
    tracker.completeReadyReads(1);
    try std.testing.expect(read.result == null);
    tracker.markReadReady(new_read, 2);
    tracker.completeReadyReads(2);
    try std.testing.expect(read.result != null);
}

test "proposal tracker detaches batches before invoking callbacks" {
    const allocator = std.testing.allocator;
    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();

    const State = struct {
        tracker: *ProposalTracker,
        callbacks: usize = 0,
        add_request: bool = false,

        fn callback(ctx: *anyopaque, _: ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.callbacks += 1;
            if (self.add_request) {
                self.add_request = false;
                self.tracker.track("new", .{ .ctx = self, .function = callback }, 0, 0) catch unreachable;
            }
            self.tracker.fail("second", error.ProposalDropped);
        }
    };
    var state = State{ .tracker = &tracker, .add_request = true };
    try tracker.track("first", .{ .ctx = &state, .function = State.callback }, 0, 0);
    try tracker.track("second", .{ .ctx = &state, .function = State.callback }, 0, 0);
    tracker.failAll(error.ShuttingDown);
    try std.testing.expectEqual(@as(usize, 2), state.callbacks);
    try std.testing.expectEqual(@as(usize, 1), tracker.pendingCount());
    tracker.failAll(error.ShuttingDown);
    try std.testing.expectEqual(@as(usize, 3), state.callbacks);
}

test "proposal tracker completion races batch failure exactly once" {
    const allocator = std.heap.smp_allocator;
    var tracker = ProposalTracker.init(allocator);
    defer tracker.deinit();
    var callbacks = std.atomic.Value(usize).init(0);
    const Callback = struct {
        fn invoke(ctx: *anyopaque, _: ProposalResult) void {
            const count: *std.atomic.Value(usize) = @ptrCast(@alignCast(ctx));
            _ = count.fetchAdd(1, .monotonic);
        }
    };
    try tracker.track("request", .{ .ctx = &callbacks, .function = Callback.invoke }, 0, 0);

    const Race = struct {
        fn complete(value: *ProposalTracker) void {
            value.complete("request", "ok");
        }
        fn fail(value: *ProposalTracker) void {
            value.failAll(error.ShuttingDown);
        }
    };
    const complete_thread = try std.Thread.spawn(.{}, Race.complete, .{&tracker});
    const fail_thread = try std.Thread.spawn(.{}, Race.fail, .{&tracker});
    complete_thread.join();
    fail_thread.join();
    try std.testing.expectEqual(@as(usize, 1), callbacks.load(.monotonic));
}

test "proposal tracker timeout deadline saturates" {
    var tracker = ProposalTracker.init(std.testing.allocator);
    defer tracker.deinit();
    var proposal = Tester{};
    try tracker.track("request", proposal.proposalCallback(), std.math.maxInt(u64) - 2, 10);
    tracker.expireTimeouts(std.math.maxInt(u64) - 1);
    try std.testing.expect(proposal.result == null);
    tracker.expireTimeouts(std.math.maxInt(u64));
    try std.testing.expectEqual(error.Timeout, proposal.result.?.err);
}
// KCOV_EXCL_STOP
