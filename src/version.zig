const build_options = @import("raft_zig_options");

pub const string = build_options.version;

test "version string parses as semantic version" {
    const std = @import("std");
    _ = try std.SemanticVersion.parse(string);
}
