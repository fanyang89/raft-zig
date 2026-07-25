const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");
const learner = @import("cases/learner_test.zig");
const configuration = @import("cases/configuration_test.zig");
const election = @import("cases/election_test.zig");
const replication = @import("cases/replication_test.zig");
const snapshot = @import("cases/snapshot_test.zig");

test "raft-rs source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{
        learner.inventory_target,
        configuration.inventory_target,
        election.inventory_target,
        replication.inventory_target,
        snapshot.inventory_target,
    });
}

test {
    _ = learner;
    _ = configuration;
    _ = election;
    _ = replication;
    _ = snapshot;
}
