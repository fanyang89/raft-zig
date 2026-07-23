const manifest = @import("upstream_manifest");
const source = @import("source.zig");

test "etcd/raft source metadata" {
    try manifest.audit(source.upstream);
}

test {
    _ = @import("cases/pre_vote_test.zig");
    _ = @import("cases/leadership_transfer_test.zig");
    _ = @import("cases/read_index_test.zig");
}
