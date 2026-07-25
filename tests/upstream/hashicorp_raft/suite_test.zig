const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");
const commitment = @import("cases/commitment_test.zig");
const consensus = @import("cases/consensus_test.zig");
const membership = @import("cases/membership_test.zig");
const snapshot = @import("cases/snapshot_test.zig");

test "HashiCorp Raft source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{
        commitment.inventory_target,
        consensus.inventory_target,
        membership.inventory_target,
        snapshot.inventory_target,
    });
}

test {
    _ = commitment;
    _ = consensus;
    _ = membership;
    _ = snapshot;
}
