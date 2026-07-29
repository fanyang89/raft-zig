//! Application-supplied state machine interface.
//!
//! Users implement this to receive committed entries, create/restore
//! snapshots, and react to leadership changes. The interface is a vtable so
//! any Zig type with the
//! right methods can plug in.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");

const Error = error_model.Error;
const Entry = types.Entry;
const Snapshot = types.Snapshot;
const SnapshotMetadata = types.SnapshotMetadata;
const ConfState = types.ConfState;

/// Optional response data returned by `StateMachine.apply`. The response must
/// use the Raftor allocator; Raftor takes ownership and frees it after invoking
/// the proposal callback.
pub const ApplyResult = struct {
    response: ?[]u8 = null,

    pub fn deinit(self: *ApplyResult, allocator: std.mem.Allocator) void {
        if (self.response) |r| allocator.free(r);
        self.response = null;
    }
};

/// Streaming sink for snapshot payload bytes. Implementations write chunks
/// until the snapshot is fully serialized.
pub const SnapshotWriter = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        write: *const fn (ctx: *anyopaque, chunk: []const u8) Error!void,
    };

    pub fn write(self: SnapshotWriter, chunk: []const u8) Error!void {
        return self.vtable.write(self.ctx, chunk);
    }
};

/// Streaming source for snapshot payload bytes. `read` returns 0 on EOF.
pub const SnapshotReader = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        read: *const fn (ctx: *anyopaque, out: []u8) Error!usize,
    };

    pub fn read(self: SnapshotReader, out: []u8) Error!usize {
        return self.vtable.read(self.ctx, out);
    }
};

pub const BufferSnapshotReader = struct {
    data: []const u8,
    offset: usize = 0,

    pub fn init(data: []const u8) BufferSnapshotReader {
        return .{ .data = data };
    }

    pub fn reader(self: *BufferSnapshotReader) SnapshotReader {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn readImpl(ctx: *anyopaque, out: []u8) Error!usize {
        const self: *BufferSnapshotReader = @ptrCast(@alignCast(ctx));
        if (self.offset >= self.data.len or out.len == 0) return 0;
        const count = @min(out.len, self.data.len - self.offset);
        @memcpy(out[0..count], self.data[self.offset .. self.offset + count]);
        self.offset += count;
        return count;
    }

    const vtable: SnapshotReader.VTable = .{ .read = readImpl };
};

/// vtable interface the Raftor orchestration layer calls into.
pub const StateMachine = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// The entry is borrowed for this call. Apply must be atomic: returning
        /// an error must leave application state unchanged. Raftor treats every
        /// apply error as terminal.
        apply: *const fn (ctx: *anyopaque, entry: Entry) Error!ApplyResult,
        /// The returned Snapshot and its owned fields must use `allocator`.
        take_snapshot: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, applied_index: u64, applied_term: u64, conf_state: ConfState) Error!Snapshot,
        /// Restore must be atomic: returning an error must leave application
        /// state unchanged.
        restore_snapshot: *const fn (ctx: *anyopaque, metadata: SnapshotMetadata, reader: SnapshotReader) Error!void,
        on_leadership_change: *const fn (ctx: *anyopaque, is_leader: bool, term: u64, leader_id: u64) void = noopOnLeadershipChange,
    };

    pub fn apply(self: StateMachine, entry: Entry) Error!ApplyResult {
        return self.vtable.apply(self.ctx, entry);
    }

    pub fn takeSnapshot(self: StateMachine, allocator: std.mem.Allocator, applied_index: u64, applied_term: u64, conf_state: ConfState) Error!Snapshot {
        return self.vtable.take_snapshot(self.ctx, allocator, applied_index, applied_term, conf_state);
    }

    pub fn restoreSnapshot(self: StateMachine, metadata: SnapshotMetadata, reader: SnapshotReader) Error!void {
        return self.vtable.restore_snapshot(self.ctx, metadata, reader);
    }

    pub fn onLeadershipChange(self: StateMachine, is_leader: bool, term: u64, leader_id: u64) void {
        self.vtable.on_leadership_change(self.ctx, is_leader, term, leader_id);
    }
};

fn noopOnLeadershipChange(_: *anyopaque, _: bool, _: u64, _: u64) void {}

/// In-memory StateMachine for tests: stores every applied entry's data in a
/// list and echoes back the data as the ApplyResult response.
pub const MockStateMachine = struct {
    applied: std.ArrayList([]u8),
    allocator: std.mem.Allocator,
    last_applied_index: u64 = 0,
    snapshot_count: usize = 0,
    last_snapshot_index: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) MockStateMachine {
        return .{ .applied = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *MockStateMachine) void {
        for (self.applied.items) |d| self.allocator.free(d);
        self.applied.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn applyImpl(ctx: *anyopaque, entry: Entry) Error!ApplyResult {
        const self: *MockStateMachine = @ptrCast(@alignCast(ctx));
        const data_copy = try self.allocator.dupe(u8, entry.data);
        errdefer self.allocator.free(data_copy);
        const resp = try self.allocator.dupe(u8, entry.data);
        errdefer self.allocator.free(resp);
        try self.applied.ensureUnusedCapacity(self.allocator, 1);
        self.applied.appendAssumeCapacity(data_copy);
        self.last_applied_index = entry.index;
        return .{ .response = resp };
    }

    pub fn takeSnapshotImpl(ctx: *anyopaque, allocator: std.mem.Allocator, applied_index: u64, applied_term: u64, conf_state: ConfState) Error!Snapshot {
        const self: *MockStateMachine = @ptrCast(@alignCast(ctx));
        self.snapshot_count += 1;
        self.last_snapshot_index = applied_index;
        return .{
            .data = try allocator.dupe(u8, ""),
            .metadata = .{
                .index = applied_index,
                .term = applied_term,
                .conf_state = try @import("storage.zig").cloneConfState(allocator, conf_state),
            },
        };
    }

    pub fn restoreSnapshotImpl(ctx: *anyopaque, metadata: SnapshotMetadata, reader: SnapshotReader) Error!void {
        _ = ctx;
        _ = metadata;
        var buf: [4096]u8 = undefined;
        while (true) {
            const n = try reader.read(&buf);
            if (n == 0) break;
        }
    }

    pub const vtable: StateMachine.VTable = .{
        .apply = applyImpl,
        .take_snapshot = takeSnapshotImpl,
        .restore_snapshot = restoreSnapshotImpl,
    };

    pub fn stateMachine(self: *MockStateMachine) StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

test "mock state machine applies entries and returns response" {
    const allocator = std.testing.allocator;
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const msm = sm.stateMachine();
    var entry = Entry{ .index = 1, .term = 1, .data = try allocator.dupe(u8, "hello") };
    defer entry.deinit(allocator);

    var result = try msm.apply(entry);
    defer result.deinit(allocator);
    try std.testing.expectEqualStrings("hello", result.response.?);
    try std.testing.expectEqual(@as(usize, 1), sm.applied.items.len);
    try std.testing.expectEqualStrings("hello", sm.applied.items[0]);
}

test "mock state machine leaves state unchanged on allocation failure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var sm = MockStateMachine.init(failing.allocator());
    defer sm.deinit();

    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, sm.stateMachine().apply(.{ .index = 1, .term = 1, .data = @constCast("data") }));
    try std.testing.expectEqual(@as(u64, 0), sm.last_applied_index);
    try std.testing.expectEqual(@as(usize, 0), sm.applied.items.len);
}
