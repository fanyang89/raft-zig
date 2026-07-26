const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");
const election = @import("cases/election_test.zig");
const invariants = @import("cases/invariants_test.zig");
const learner = @import("cases/learner_test.zig");
const membership = @import("cases/membership_test.zig");
const replication = @import("cases/replication_test.zig");

test "OpenRaft source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{
        election.inventory_target,
        invariants.inventory_target,
        learner.inventory_target,
        membership.inventory_target,
        replication.inventory_target,
    });
}

test {
    _ = election;
    _ = invariants;
    _ = learner;
    _ = membership;
    _ = replication;
}
