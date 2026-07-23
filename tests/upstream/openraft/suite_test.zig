const manifest = @import("upstream_manifest");
const source = @import("source.zig");

test "OpenRaft source metadata" {
    try manifest.audit(source.upstream);
}
