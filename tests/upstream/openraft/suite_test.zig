const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");

test "OpenRaft source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{});
}
