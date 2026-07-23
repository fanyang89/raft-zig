const manifest = @import("upstream_manifest");
const source = @import("source.zig");

test "etcd/raft source metadata" {
    try manifest.audit(source.upstream);
}
