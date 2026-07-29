//! Linearizable read-index queue.
//!
//! The leader tracks pending read-index requests keyed by their opaque context
//! bytes; quorum ACKs are recorded per request; `advance(ctx)` releases all
//! earlier requests in insertion order.
//!
//! Memory model: `queue` owns the context byte slices. `pending` borrows those
//! slices as map keys. `advance` and `deinit` free the slices via `queue`
//! after removing the corresponding map entry.

const std = @import("std");

const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");

const Message = types.Message;

pub const ReadOnlyOption = enum(u8) { safe, lease_based };

/// Per-tick read-index result waiting to be applied.
pub const ReadState = struct {
    index: u64,
    request_ctx: []u8 = &.{},

    pub fn deinit(self: *ReadState, allocator: std.mem.Allocator) void {
        if (self.request_ctx.len != 0) allocator.free(self.request_ctx);
        self.request_ctx = &.{};
    }
};

/// A pending read-index request together with the ACKs it has collected.
pub const ReadIndexStatus = struct {
    /// Owned copy of the original read-index request message.
    req: Message,
    /// Commit index at the time the request was registered.
    index: u64,
    /// Set of voter IDs that have acknowledged the heartbeat covering this
    /// request. The leader's own id is inserted at registration time.
    acks: std.AutoHashMap(u64, void),

    pub fn deinit(self: *ReadIndexStatus, allocator: std.mem.Allocator) void {
        self.req.deinit(allocator);
        self.acks.deinit();
        self.* = undefined;
    }
};

pub const ReadOnly = struct {
    option: ReadOnlyOption,
    /// Owned context bytes. The slice at index `i` is the key for
    /// `pending.items[i]`. `advance(ctx)` pops from the front until it reaches
    /// the matching context.
    queue: std.ArrayList([]u8),
    /// Borrowed-key map: keys point into `queue.items`.
    pending: std.StringHashMap(ReadIndexStatus),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, option: ReadOnlyOption) ReadOnly {
        return .{
            .option = option,
            .queue = .empty,
            .pending = std.StringHashMap(ReadIndexStatus).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ReadOnly) void {
        // Free owned context bytes.
        for (self.queue.items) |ctx| self.allocator.free(ctx);
        self.queue.deinit(self.allocator);
        // Free owned statuses.
        var it = self.pending.valueIterator();
        while (it.next()) |status| status.deinit(self.allocator);
        self.pending.deinit();
        self.* = undefined;
    }

    /// Register a new read-index request. Silently no-ops on duplicate context
    /// or on a message with no entries (the context bytes come from
    /// `req.entries[0].data`).
    pub fn addRequest(self: *ReadOnly, index: u64, req: Message, self_id: u64) !void {
        if (req.entries.len == 0) return;
        const ctx = req.entries[0].data;
        if (self.pending.contains(ctx)) return;

        const owned_ctx = try self.allocator.dupe(u8, ctx);
        errdefer self.allocator.free(owned_ctx);
        var cloned_req = try storage_mod.shareMessage(self.allocator, req);
        errdefer cloned_req.deinit(self.allocator);

        var acks = std.AutoHashMap(u64, void).init(self.allocator);
        errdefer acks.deinit();
        try acks.put(self_id, {});

        try self.queue.append(self.allocator, owned_ctx);
        errdefer {
            // Remove the slot we just added so deinit doesn't double-free.
            _ = self.queue.pop();
        }
        try self.pending.put(owned_ctx, .{
            .req = cloned_req,
            .index = index,
            .acks = acks,
        });
    }

    pub fn lastPendingRequestCtx(self: ReadOnly) ?[]const u8 {
        if (self.queue.items.len == 0) return null;
        return self.queue.items[self.queue.items.len - 1];
    }

    pub fn pendingReadCount(self: ReadOnly) usize {
        return self.queue.items.len;
    }

    /// Record an ACK and return the current ack set for the request. Returns
    /// `null` when `ctx` does not match any pending request.
    pub fn recvACK(self: *ReadOnly, id: u64, ctx: []const u8) !?*std.AutoHashMap(u64, void) {
        const status = self.pending.getPtr(ctx) orelse return null;
        try status.acks.put(id, {});
        return &status.acks;
    }

    /// Release all pending requests up to and including the one identified by
    /// `ctx`. Returns an owned list of statuses that the caller must deinit.
    /// Unknown `ctx` yields an empty (but still owned) result.
    pub fn advance(self: *ReadOnly, ctx: []const u8) ![]ReadIndexStatus {
        var target_idx: ?usize = null;
        for (self.queue.items, 0..) |item, i| {
            if (std.mem.eql(u8, item, ctx)) {
                target_idx = i;
                break;
            }
        }
        if (target_idx == null) return self.allocator.alloc(ReadIndexStatus, 0);

        const end = target_idx.? + 1;
        for (self.queue.items[0..end]) |key| {
            if (!self.pending.contains(key)) return error.Fatal;
        }

        const result = try self.allocator.alloc(ReadIndexStatus, end);
        var i: usize = 0;
        while (i < end) : (i += 1) {
            const key = self.queue.items[0];
            const kv = self.pending.fetchRemove(key) orelse unreachable;
            result[i] = kv.value;
            self.allocator.free(key);
            _ = self.queue.orderedRemove(0);
        }
        return result;
    }
};

// KCOV_EXCL_START
test "read only construction and basic add" {
    var ro = ReadOnly.init(std.testing.allocator, .safe);
    defer ro.deinit();

    try std.testing.expectEqual(@as(usize, 0), ro.pendingReadCount());
    try std.testing.expect(ro.lastPendingRequestCtx() == null);

    var entries = [_]types.Entry{.{ .data = try std.testing.allocator.dupe(u8, "ctx1") }};
    defer std.testing.allocator.free(entries[0].data);
    const msg = Message{ .msg_type = .read_index, .entries = &entries };

    try ro.addRequest(10, msg, 1);
    try std.testing.expectEqual(@as(usize, 1), ro.pendingReadCount());
    try std.testing.expectEqualStrings("ctx1", ro.lastPendingRequestCtx().?);
}

test "read only ignores duplicate and empty" {
    var ro = ReadOnly.init(std.testing.allocator, .safe);
    defer ro.deinit();

    const empty_msg = Message{ .msg_type = .read_index };
    try ro.addRequest(10, empty_msg, 1);
    try std.testing.expectEqual(@as(usize, 0), ro.pendingReadCount());

    const data1 = try std.testing.allocator.dupe(u8, "dup");
    defer std.testing.allocator.free(data1);
    var entries1 = [_]types.Entry{.{ .data = data1 }};
    const msg1 = Message{ .msg_type = .read_index, .entries = &entries1 };
    try ro.addRequest(10, msg1, 1);

    const data2 = try std.testing.allocator.dupe(u8, "dup");
    defer std.testing.allocator.free(data2);
    var entries2 = [_]types.Entry{.{ .data = data2 }};
    const msg2 = Message{ .msg_type = .read_index, .entries = &entries2 };
    try ro.addRequest(20, msg2, 1);

    try std.testing.expectEqual(@as(usize, 1), ro.pendingReadCount());
}

test "read only advance releases earlier requests in order" {
    var ro = ReadOnly.init(std.testing.allocator, .safe);
    defer ro.deinit();

    const ctxs = [_][]const u8{ "alpha", "beta", "gamma", "delta" };
    var owned: [4][]u8 = undefined;
    for (ctxs, 0..) |c, i| {
        owned[i] = try std.testing.allocator.dupe(u8, c);
        var entries = [_]types.Entry{.{ .data = owned[i] }};
        const msg = Message{ .msg_type = .read_index, .entries = &entries };
        try ro.addRequest(100 + @as(u64, @intCast(i)) * 100, msg, 1);
    }
    for (owned) |o| std.testing.allocator.free(o);

    const statuses = try ro.advance("delta");
    defer {
        for (statuses) |*s| {
            var mut = s.*;
            mut.deinit(std.testing.allocator);
        }
        std.testing.allocator.free(statuses);
    }
    try std.testing.expectEqual(@as(usize, 4), statuses.len);
    try std.testing.expectEqual(@as(u64, 100), statuses[0].index);
    try std.testing.expectEqual(@as(u64, 200), statuses[1].index);
    try std.testing.expectEqual(@as(u64, 300), statuses[2].index);
    try std.testing.expectEqual(@as(u64, 400), statuses[3].index);
    try std.testing.expectEqual(@as(usize, 0), ro.pendingReadCount());
}

test "read only advance allocation failure preserves pending requests" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var ro = ReadOnly.init(allocator, .safe);
    defer ro.deinit();

    var entries = [_]types.Entry{.{ .data = "ctx" }};
    try ro.addRequest(10, .{ .msg_type = .read_index, .entries = &entries }, 1);

    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, ro.advance("ctx"));
    try std.testing.expectEqual(@as(usize, 1), ro.pendingReadCount());
    try std.testing.expectEqualStrings("ctx", ro.lastPendingRequestCtx().?);

    failing.fail_index = std.math.maxInt(usize);
    const statuses = try ro.advance("ctx");
    defer {
        for (statuses) |*status| status.deinit(allocator);
        allocator.free(statuses);
    }
    try std.testing.expectEqual(@as(usize, 1), statuses.len);
}

test "read only ack set accumulates per request" {
    var ro = ReadOnly.init(std.testing.allocator, .safe);
    defer ro.deinit();

    const data = try std.testing.allocator.dupe(u8, "ctx1");
    defer std.testing.allocator.free(data);
    var entries = [_]types.Entry{.{ .data = data }};
    const msg = Message{ .msg_type = .read_index, .entries = &entries };
    try ro.addRequest(10, msg, 1);

    try std.testing.expect(try ro.recvACK(2, "ctx1") != null);
    try std.testing.expect(try ro.recvACK(3, "ctx1") != null);
    try std.testing.expect(try ro.recvACK(2, "ctx1") != null); // idempotent

    const acks = (try ro.recvACK(4, "ctx1")).?;
    try std.testing.expectEqual(@as(u32, 4), acks.count());
    try std.testing.expect(acks.contains(1));
    try std.testing.expect(acks.contains(2));
    try std.testing.expect(acks.contains(3));
    try std.testing.expect(acks.contains(4));

    try std.testing.expect(try ro.recvACK(2, "unknown") == null);
}
// KCOV_EXCL_STOP
