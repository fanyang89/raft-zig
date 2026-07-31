const std = @import("std");

const grpc = @import("grpc_lite");
const grpc_pb = @import("grpc_lite_protobuf");
const pb = @import("database_proto");
const raft = @import("raft_zig");
const sqlite = @import("sqlite.zig");
const state_machine = @import("state_machine.zig");

pub const ServiceError = error{
    OutOfMemory,
    NotLeader,
    InvalidRequest,
    ResourceExhausted,
    Unavailable,
    Internal,
};

pub const DatabaseApi = pb.DatabaseService(DatabaseService, ServiceError);
pub const Registration = grpc_pb.ServiceRegistration(DatabaseApi);

pub const DatabaseService = struct {
    allocator: std.mem.Allocator,
    raftor: *raft.Raftor,
    machine: *state_machine.SqliteStateMachine,

    pub fn init(
        allocator: std.mem.Allocator,
        raftor: *raft.Raftor,
        machine: *state_machine.SqliteStateMachine,
    ) ServiceError!DatabaseService {
        if (!raftor.leaderServicePolicy().isSafe()) return error.Internal;
        return .{ .allocator = allocator, .raftor = raftor, .machine = machine };
    }

    pub fn registration(self: *DatabaseService) Registration {
        return Registration.init(
            self.allocator,
            self,
            .{
                .Execute = execute,
                .Query = query,
                .Status = status,
            },
            .{ .map_error = mapError },
        );
    }

    fn execute(self: *DatabaseService, request: pb.ExecuteRequest) ServiceError!pb.ExecuteResponse {
        if (!self.raftor.isLeader()) return error.NotLeader;
        const command = state_machine.encodeExecuteCommand(self.allocator, request) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.CommandTooLarge => error.ResourceExhausted,
            else => error.InvalidRequest,
        };
        defer self.allocator.free(command);

        var pending: ProposalPending = .{ .allocator = self.allocator };
        defer pending.deinit();
        self.raftor.propose(command, pending.callback()) catch |err| return mapRaftError(err);
        pending.wait();
        if (pending.failure) |failure| return mapRaftError(failure);
        const encoded = pending.response orelse return error.Internal;
        return state_machine.decodeExecuteResponse(self.allocator, encoded) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.Internal,
        };
    }

    fn query(self: *DatabaseService, request: pb.QueryRequest) ServiceError!pb.QueryResponse {
        if (!self.raftor.isLeader()) return error.NotLeader;
        var pending: ReadPending = .{};
        self.raftor.readIndex("sql-query", pending.callback()) catch |err| return mapRaftError(err);
        pending.wait();
        if (pending.failure) |failure| return mapRaftError(failure);
        return self.machine.query(self.allocator, request) catch |err| return mapSqliteError(err);
    }

    fn status(self: *DatabaseService, _: pb.StatusRequest) ServiceError!pb.StatusResponse {
        const node_status = self.raftor.getStatus();
        var response: pb.StatusResponse = .{
            .node_id = node_status.id,
            .leader_id = node_status.leader_id,
            .role = self.allocator.dupe(u8, @tagName(node_status.role)) catch return error.OutOfMemory,
            .term = node_status.term,
            .applied_index = node_status.applied_index,
        };
        errdefer response.deinit(self.allocator);
        response.database_bytes = self.machine.databaseBytes() catch return error.Internal;
        response.sqlite_version = self.allocator.dupe(u8, "3.53.4") catch return error.OutOfMemory;
        return response;
    }
};

const ProposalPending = struct {
    allocator: std.mem.Allocator,
    done: std.atomic.Value(bool) = .init(false),
    response: ?[]u8 = null,
    failure: ?raft.Error = null,

    fn deinit(self: *ProposalPending) void {
        if (self.response) |response| self.allocator.free(response);
    }

    fn callback(self: *ProposalPending) raft.ProposalCallback {
        return .{ .ctx = self, .function = complete };
    }

    fn complete(context: *anyopaque, result: raft.ProposalResult) void {
        const self: *ProposalPending = @ptrCast(@alignCast(context));
        switch (result) {
            .ok => |response| self.response = self.allocator.dupe(u8, response) catch {
                self.failure = error.OutOfMemory;
                self.done.store(true, .release);
                return;
            },
            .err => |failure| self.failure = failure,
        }
        self.done.store(true, .release);
    }

    fn wait(self: *ProposalPending) void {
        while (!self.done.load(.acquire)) std.Thread.yield() catch {};
    }
};

const ReadPending = struct {
    done: std.atomic.Value(bool) = .init(false),
    failure: ?raft.Error = null,

    fn callback(self: *ReadPending) raft.ReadIndexCallback {
        return .{ .ctx = self, .function = complete };
    }

    fn complete(context: *anyopaque, result: raft.ReadIndexResult) void {
        const self: *ReadPending = @ptrCast(@alignCast(context));
        switch (result) {
            .ok => {},
            .err => |failure| self.failure = failure,
        }
        self.done.store(true, .release);
    }

    fn wait(self: *ReadPending) void {
        while (!self.done.load(.acquire)) std.Thread.yield() catch {};
    }
};

fn mapRaftError(err: anyerror) ServiceError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.ProposalBackpressure, error.ReadIndexBackpressure => error.ResourceExhausted,
        error.ProposalDropped, error.LostLeadership => error.NotLeader,
        error.ShuttingDown, error.Timeout, error.ConnectionClosed => error.Unavailable,
        else => error.Internal,
    };
}

fn mapSqliteError(err: sqlite.Error) ServiceError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.InvalidRequest => error.InvalidRequest,
        error.QueryLimitExceeded, error.ResultTooLarge, error.SnapshotTooLarge => error.ResourceExhausted,
        else => error.Internal,
    };
}

fn mapError(err: ServiceError) grpc.Status {
    return switch (err) {
        error.OutOfMemory, error.Internal => .init(.internal, "internal error"),
        error.NotLeader => .init(.failed_precondition, "not leader"),
        error.InvalidRequest => .init(.invalid_argument, "invalid request"),
        error.ResourceExhausted => .init(.resource_exhausted, "resource exhausted"),
        error.Unavailable => .init(.unavailable, "service unavailable"),
    };
}

test "service error mapping is stable" {
    try std.testing.expectEqual(grpc.StatusCode.failed_precondition, mapError(error.NotLeader).code);
    try std.testing.expectEqual(grpc.StatusCode.resource_exhausted, mapError(error.ResourceExhausted).code);
}
