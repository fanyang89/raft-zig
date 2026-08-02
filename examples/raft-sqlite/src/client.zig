const std = @import("std");

const grpc = @import("grpc_lite");
const grpc_pb = @import("grpc_lite_protobuf");
const pb = @import("database_proto");
const sqlite = @import("sqlite.zig");

const ClientApi = pb.DatabaseService(void, error{});
const TypedClient = grpc_pb.ServiceClient(ClientApi);

pub const ExecuteResult = grpc_pb.TypedResult(pb.ExecuteResponse);
pub const QueryResult = grpc_pb.TypedResult(pb.QueryResponse);
pub const StatusResult = grpc_pb.TypedResult(pb.StatusResponse);
pub const AdminResult = grpc_pb.TypedResult(pb.AdminResponse);

pub const Client = struct {
    channel: grpc.Channel,

    pub fn init(allocator: std.mem.Allocator, endpoint: []const u8) !Client {
        return .{ .channel = try grpc.Channel.init(allocator, endpoint, .{}) };
    }

    pub fn deinit(self: *Client) void {
        self.channel.deinit();
        self.* = undefined;
    }

    pub fn execute(self: *Client, allocator: std.mem.Allocator, request: pb.ExecuteRequest) !ExecuteResult {
        var client = TypedClient.init(&self.channel);
        return client.callUnary(allocator, "Execute", request, .{
            .timeout_ns = 10 * std.time.ns_per_s,
            .max_response_size = sqlite.max_api_response_bytes,
        });
    }

    pub fn query(self: *Client, allocator: std.mem.Allocator, request: pb.QueryRequest) !QueryResult {
        var client = TypedClient.init(&self.channel);
        return client.callUnary(allocator, "Query", request, .{
            .timeout_ns = 10 * std.time.ns_per_s,
            .max_response_size = sqlite.max_api_response_bytes,
        });
    }

    pub fn status(self: *Client, allocator: std.mem.Allocator) !StatusResult {
        var client = TypedClient.init(&self.channel);
        return client.callUnary(allocator, "Status", .{}, .{ .timeout_ns = 5 * std.time.ns_per_s });
    }

    pub fn admin(self: *Client, allocator: std.mem.Allocator, request: pb.AdminRequest) !AdminResult {
        var client = TypedClient.init(&self.channel);
        return client.callUnary(allocator, "Admin", request, .{ .timeout_ns = 10 * std.time.ns_per_s });
    }
};
