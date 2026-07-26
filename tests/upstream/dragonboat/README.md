# Dragonboat

- Repository: <https://github.com/lni/dragonboat>
- Revision: `076c7f6497dcc18880aed6323246d5079661942c`
- License: Apache-2.0

The inventory contains the 407 top-level `func Test` declarations from the 13
`internal/raft/*_test.go` files at the pinned revision. Only Dragonboat core
deltas and historical regressions are adapted. Tests derived from etcd/raft
are delegated to the primary baseline.

NodeHost and multi-group behavior, witnesses, quiesce, rate limiting, delayed
snapshot acknowledgements, log queries, and implementation-specific Update or
LogDB contracts are excluded. The upstream license and the two notices from
`internal/raft` are preserved locally.
