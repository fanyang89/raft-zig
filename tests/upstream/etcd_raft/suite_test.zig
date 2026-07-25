const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");
const pre_vote = @import("cases/pre_vote_test.zig");
const leadership_transfer = @import("cases/leadership_transfer_test.zig");
const read_index = @import("cases/read_index_test.zig");
const learner = @import("cases/learner_test.zig");

test "etcd/raft source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{
        pre_vote.inventory_target,
        leadership_transfer.inventory_target,
        read_index.inventory_target,
        learner.inventory_target,
    });
}

test {
    _ = pre_vote;
    _ = leadership_transfer;
    _ = read_index;
    _ = learner;
}
