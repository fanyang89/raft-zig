const std = @import("std");
const manifest = @import("upstream_manifest");
const source = @import("source.zig");
const election = @import("cases/election_test.zig");
const leadership_transfer = @import("cases/leadership_transfer_test.zig");
const log_safety = @import("cases/log_safety_test.zig");
const ownership = @import("cases/ownership_test.zig");
const raw_node = @import("cases/raw_node_test.zig");
const read_index = @import("cases/read_index_test.zig");
const snapshot = @import("cases/snapshot_test.zig");

test "Dragonboat source metadata" {
    try manifest.audit(std.testing.allocator, source.upstream);
    try manifest.auditConsumedTargets(std.testing.allocator, source.upstream, &.{
        election.inventory_target,
        leadership_transfer.inventory_target,
        log_safety.inventory_target,
        ownership.inventory_target,
        raw_node.inventory_target,
        read_index.inventory_target,
        snapshot.inventory_target,
    });
}

test {
    _ = election;
    _ = leadership_transfer;
    _ = log_safety;
    _ = ownership;
    _ = raw_node;
    _ = read_index;
    _ = snapshot;
}
