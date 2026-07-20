//! Minimal single-node bootstrap.
//!
//! Ported from `examples/minimal_node/main.cc` in raftpp. The full Raftor
//! event loop is not yet ported, so this example currently boots the module
//! tree and exits once it can read back the node id. It will grow as the
//! consensus core lands.

const std = @import("std");
const raft = @import("raft_zig");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    const node_id: u64 = 1;

    var entry = raft.Entry{
        .term = 1,
        .index = 1,
    };
    defer entry.deinit(allocator);

    std.log.info(
        "raft-zig {s}: node {d} ready (role={s})",
        .{ raft.version, node_id, raft.roleName(.follower) },
    );
}
