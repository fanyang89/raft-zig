const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");
const invariants = @import("cases/invariants_test.zig");
const learner = @import("cases/learner_test.zig");
const replication = @import("cases/replication_test.zig");

test "OpenRaft source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{
        invariants.inventory_target,
        learner.inventory_target,
        replication.inventory_target,
    });
}

test {
    _ = invariants;
    _ = learner;
    _ = replication;
}
