# HashiCorp Raft

- Repository: <https://github.com/hashicorp/raft>
- Revision: `dd30865f162c68ee31130c7f8ee1047e9122f2ec`
- License: MPL-2.0

Only externally observable behavior is used. Tests are independently designed
against raft-zig APIs without copying HashiCorp source, helpers, constants,
assertions, or test structure.

The inventory contains 94 high-level families, 16 commitment and configuration
cases, 15 snapshot-store exclusions, and 59 stable parameterized subcases. The
upstream license text is preserved in `LICENSE.upstream` for provenance; its
presence does not change the clean-room policy.
