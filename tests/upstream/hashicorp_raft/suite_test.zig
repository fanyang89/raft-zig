const manifest = @import("upstream_manifest");
const source = @import("source.zig");

test "HashiCorp Raft source metadata" {
    try manifest.audit(source.upstream);
}
