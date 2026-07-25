const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");
const membership = @import("cases/membership_test.zig");

test "HashiCorp Raft source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{membership.inventory_target});
}

test {
    _ = membership;
}
