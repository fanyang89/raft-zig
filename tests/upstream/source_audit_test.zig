const std = @import("std");
const manifest = @import("upstream_manifest");

test "upstream source manifests are valid" {
    try manifest.auditAll(std.testing.allocator, &.{
        @import("etcd_raft/source.zig").upstream,
        @import("raft_rs/source.zig").upstream,
        @import("openraft/source.zig").upstream,
        @import("hashicorp_raft/source.zig").upstream,
    });
}
