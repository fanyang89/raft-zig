const manifest = @import("upstream_manifest");
const source = @import("source.zig");

test "raft-rs source metadata" {
    try manifest.audit(source.upstream);
}
