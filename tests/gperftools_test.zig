const std = @import("std");
const perf = @import("raft_zig_gperftools");

test "grpc-lite gperftools is linked through raft-zig" {
    const memory = try perf.allocator.alloc(u8, 4096);
    defer perf.allocator.free(memory);

    try std.testing.expect(perf.owns(memory.ptr));
    try std.testing.expect(perf.getNumericProperty("generic.current_allocated_bytes") != null);
}
