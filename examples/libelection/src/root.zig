const c_api = @import("c_api.zig");

comptime {
    @export(&c_api.election_abi_version, .{ .name = "election_abi_version" });
    @export(&c_api.election_library_version, .{ .name = "election_library_version" });
    @export(&c_api.election_error_string, .{ .name = "election_error_string" });
    @export(&c_api.election_node_create, .{ .name = "election_node_create" });
    @export(&c_api.election_node_start, .{ .name = "election_node_start" });
    @export(&c_api.election_node_poll, .{ .name = "election_node_poll" });
    @export(&c_api.election_node_tick, .{ .name = "election_node_tick" });
    @export(&c_api.election_node_get_status, .{ .name = "election_node_get_status" });
    @export(&c_api.election_node_shutdown, .{ .name = "election_node_shutdown" });
    @export(&c_api.election_node_destroy, .{ .name = "election_node_destroy" });
}

test {
    _ = @import("api_types.zig");
    _ = @import("integration_test.zig");
    _ = c_api;
}
