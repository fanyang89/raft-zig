const std = @import("std");

const pb = @import("database_proto");
const raft = @import("raft_zig");
const sqlite = @import("sqlite.zig");

pub const command_format_version: u32 = 1;
pub const max_command_bytes: usize = 4 * 1024 * 1024;
pub const max_snapshot_bytes: usize = sqlite.max_database_bytes + 64;

pub const SqliteStateMachine = struct {
    allocator: std.mem.Allocator,
    database: sqlite.Database,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(allocator: std.mem.Allocator) !SqliteStateMachine {
        return .{
            .allocator = allocator,
            .database = try sqlite.Database.init(allocator),
        };
    }

    pub fn initFile(
        allocator: std.mem.Allocator,
        path: [:0]const u8,
        cluster_id: sqlite.ClusterId,
    ) !SqliteStateMachine {
        return .{
            .allocator = allocator,
            .database = try sqlite.Database.openFile(allocator, path, cluster_id),
        };
    }

    pub fn deinit(self: *SqliteStateMachine) void {
        self.database.deinit();
        self.* = undefined;
    }

    pub fn stateMachine(self: *SqliteStateMachine) raft.StateMachine {
        return .{
            .ctx = self,
            .vtable = if (self.database.persistent) &durable_vtable else &volatile_vtable,
        };
    }

    pub fn query(self: *SqliteStateMachine, allocator: std.mem.Allocator, request: pb.QueryRequest) sqlite.Error!pb.QueryResponse {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.database.query(allocator, request);
    }

    pub fn appliedIndex(self: *SqliteStateMachine) sqlite.Error!u64 {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.database.appliedIndex();
    }

    pub fn databaseBytes(self: *SqliteStateMachine) sqlite.Error!u64 {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.database.serializedSize();
    }

    fn applyImpl(context: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self: *SqliteStateMachine = @ptrCast(@alignCast(context));
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (entry.data.len == 0) {
            self.database.advance(entry.index, entry.term) catch |err| return mapSqliteError(err);
            return .{};
        }
        if (entry.data.len > max_command_bytes) return error.Fatal;

        var arena: std.heap.ArenaAllocator = .init(self.allocator);
        defer arena.deinit();
        var reader: std.Io.Reader = .fixed(entry.data);
        const envelope = pb.CommandEnvelope.decode(&reader, arena.allocator()) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.Fatal,
        };
        if (envelope.format_version != command_format_version or
            envelope.sqlite_version_number != sqlite.sqlite_version_number)
        {
            return error.Fatal;
        }
        const request = envelope.execute orelse return error.Fatal;
        var request_hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(entry.data, &request_hash, .{});
        const response = self.database.apply(self.allocator, request, request_hash, entry.index, entry.term) catch |err| {
            return mapSqliteError(err);
        };
        return .{ .response = response };
    }

    fn takeSnapshotImpl(
        context: *anyopaque,
        allocator: std.mem.Allocator,
        applied_index: u64,
        applied_term: u64,
        conf_state: raft.ConfState,
    ) raft.Error!raft.Snapshot {
        const self: *SqliteStateMachine = @ptrCast(@alignCast(context));
        lock(&self.mutex);
        defer self.mutex.unlock();
        const data = self.database.takeSnapshot(allocator, applied_index, applied_term) catch |err| return mapSqliteError(err);
        errdefer allocator.free(data);
        return .{
            .data = data,
            .metadata = .{
                .index = applied_index,
                .term = applied_term,
                .conf_state = raft.cloneConfState(allocator, conf_state) catch return error.OutOfMemory,
            },
        };
    }

    fn restoreSnapshotImpl(
        context: *anyopaque,
        metadata: raft.SnapshotMetadata,
        reader: raft.SnapshotReader,
    ) raft.Error!void {
        const self: *SqliteStateMachine = @ptrCast(@alignCast(context));
        var data: std.ArrayList(u8) = .empty;
        defer data.deinit(self.allocator);
        var buffer: [64 * 1024]u8 = undefined;
        while (true) {
            const count = try reader.read(&buffer);
            if (count == 0) break;
            if (data.items.len > max_snapshot_bytes -| count) return error.Fatal;
            data.appendSlice(self.allocator, buffer[0..count]) catch return error.OutOfMemory;
        }
        lock(&self.mutex);
        defer self.mutex.unlock();
        self.database.restore(metadata.index, metadata.term, data.items) catch |err| return mapSqliteError(err);
    }

    fn durableAppliedImpl(context: *anyopaque) raft.Error!raft.DurableApplied {
        const self: *SqliteStateMachine = @ptrCast(@alignCast(context));
        lock(&self.mutex);
        defer self.mutex.unlock();
        const position = self.database.appliedPosition() catch |err| return mapSqliteError(err);
        return .{ .index = position.index, .term = position.term };
    }

    const volatile_vtable: raft.StateMachine.VTable = .{
        .apply = applyImpl,
        .take_snapshot = takeSnapshotImpl,
        .restore_snapshot = restoreSnapshotImpl,
    };

    const durable_vtable: raft.StateMachine.VTable = .{
        .apply = applyImpl,
        .take_snapshot = takeSnapshotImpl,
        .restore_snapshot = restoreSnapshotImpl,
        .durable_applied = durableAppliedImpl,
    };
};

pub fn encodeExecuteCommand(allocator: std.mem.Allocator, request: pb.ExecuteRequest) ![]u8 {
    const envelope: pb.CommandEnvelope = .{
        .format_version = command_format_version,
        .sqlite_version_number = sqlite.sqlite_version_number,
        .execute = request,
    };
    var output: std.Io.Writer.Allocating = .init(allocator);
    defer output.deinit();
    try envelope.encode(&output.writer, allocator);
    if (output.written().len > max_command_bytes) return error.CommandTooLarge;
    return output.toOwnedSlice();
}

pub fn decodeExecuteResponse(allocator: std.mem.Allocator, encoded: []const u8) !pb.ExecuteResponse {
    var reader: std.Io.Reader = .fixed(encoded);
    return pb.ExecuteResponse.decode(&reader, allocator);
}

fn mapSqliteError(err: sqlite.Error) raft.Error {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.Fatal,
    };
}

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

const SliceSnapshotReader = struct {
    data: []const u8,
    offset: usize = 0,

    fn reader(self: *SliceSnapshotReader) raft.SnapshotReader {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn read(context: *anyopaque, output: []u8) raft.Error!usize {
        const self: *SliceSnapshotReader = @ptrCast(@alignCast(context));
        if (self.offset == self.data.len) return 0;
        const count = @min(output.len, self.data.len - self.offset);
        @memcpy(output[0..count], self.data[self.offset .. self.offset + count]);
        self.offset += count;
        return count;
    }

    const vtable: raft.SnapshotReader.VTable = .{ .read = read };
};

test "state machine applies commands and restores a snapshot" {
    const allocator = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    var request: pb.ExecuteRequest = .{ .request_id = "state-machine-1" };
    try request.statements.append(arena.allocator(), .{
        .sql = "CREATE TABLE values_table (value INTEGER NOT NULL) STRICT",
    });
    try request.statements.append(arena.allocator(), .{
        .sql = "INSERT INTO values_table VALUES (42)",
    });
    const command = try encodeExecuteCommand(allocator, request);
    defer allocator.free(command);

    var machine = try SqliteStateMachine.init(allocator);
    defer machine.deinit();
    var result = try machine.stateMachine().apply(.{
        .index = 4,
        .term = 2,
        .data = command,
    });
    defer result.deinit(allocator);
    var response = try decodeExecuteResponse(allocator, result.response.?);
    defer response.deinit(allocator);
    try std.testing.expectEqual(pb.ExecuteCode.EXECUTE_CODE_OK, response.code);

    var snapshot = try machine.stateMachine().takeSnapshot(allocator, 4, 2, .{});
    defer snapshot.deinit(allocator);
    var restored = try SqliteStateMachine.init(allocator);
    defer restored.deinit();
    var snapshot_reader: SliceSnapshotReader = .{ .data = snapshot.data };
    try restored.stateMachine().restoreSnapshot(snapshot.metadata, snapshot_reader.reader());
    var query_response = try restored.query(allocator, .{ .sql = "SELECT value FROM values_table" });
    defer query_response.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 42), query_response.rows.items[0].values.items[0].kind.?.integer_value);
}
