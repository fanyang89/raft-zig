const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");
const async_ready = @import("cases/async_ready_test.zig");
const learner = @import("cases/learner_test.zig");
const configuration = @import("cases/configuration_test.zig");
const election = @import("cases/election_test.zig");
const extensions = @import("cases/extensions_test.zig");
const pagination = @import("cases/pagination_test.zig");
const read_index = @import("cases/read_index_test.zig");
const replication = @import("cases/replication_test.zig");
const request_snapshot = @import("cases/request_snapshot_test.zig");
const snapshot = @import("cases/snapshot_test.zig");
const uncommitted = @import("cases/uncommitted_test.zig");
const vote_commit = @import("cases/vote_commit_test.zig");

test "raft-rs source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{
        async_ready.inventory_target,
        learner.inventory_target,
        configuration.inventory_target,
        election.inventory_target,
        extensions.inventory_target,
        pagination.inventory_target,
        read_index.inventory_target,
        replication.inventory_target,
        request_snapshot.inventory_target,
        snapshot.inventory_target,
        uncommitted.inventory_target,
        vote_commit.inventory_target,
    });
}

test {
    _ = async_ready;
    _ = learner;
    _ = configuration;
    _ = election;
    _ = extensions;
    _ = pagination;
    _ = read_index;
    _ = replication;
    _ = request_snapshot;
    _ = snapshot;
    _ = uncommitted;
    _ = vote_commit;
}
