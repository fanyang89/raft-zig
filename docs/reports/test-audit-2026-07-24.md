# 测试 Soundness 与 Completeness 审计报告

> Historical report: this document records the repository as of 2026-07-24.
> Current test coverage and inventory status are documented in
> [`../testing.md`](../testing.md) and
> [`../../tests/upstream/README.md`](../../tests/upstream/README.md).

日期：2026-07-24
范围：仓库内全部测试套件，以及 `tests/upstream/*/cases.jsonl` 中的上游清单
（etcd/raft、raft-rs、OpenRaft、HashiCorp Raft 四个固定 revision，共 1032 条）。
方法：四路并行只读审计，分别覆盖故障注入 soundness、核心测试 soundness、
上游覆盖 completeness、不变量检查器强度。审计期间未修改任何生产或测试代码。

## 总体结论

当前测试套件**既不 sound 也不 complete**。

- Soundness：多个命名测试在被测行为损坏甚至不存在时仍会通过，共 5 个高严重度发现。
- Completeness：608 条上游用例仍为 `planned`，43 条为 `blocked`；多条 Raft
  安全不变量没有跨时间检查器；部分产品层没有任何有意义的测试。

## 1. Soundness 发现

### 1.1 高严重度

#### S1. `tests/raft_test.zig:126-142` — "leader steps down when quorum lost" 是空测试

注入 `ignoreMessageType(.append/.heartbeat)` 之后没有任何 tick 或消息驱动。
断言的状态（`state == .leader`）就是 setup 选举完成后的状态。即使完全删除
check-quorum / 退位逻辑，该测试依然通过。

#### S2. 崩溃故障测试缺少故障见证（fault witness）

`tests/vopr/wal_fs_adapter.zig:439-471`（reordered suffix with gaps）、
`tests/vopr/wal_fs_adapter.zig:505-597`（fuzz crash recovery）、
`tests/vopr/cluster_test.zig:309-337`（整盘断电）只断言崩溃后的不变量，
而这些不变量宽到把"故障根本没发生"的世界也包含进去：

- 若 marionette 的 reorder 速率被静默丢弃，所有 pending write 按序落地，
  断言依然通过。
- fuzz crash 测试从不校验已提交 entry 的**数据内容**，只验证 `term(i)` 可取。
- 断电场景在崩溃前没有记录已提交前缀的任何锚点。如果三个节点一致地丢失
  已提交 entry `"before-power-loss"`，跨节点收敛检查依然通过——因为它比较
  的是节点彼此之间，而不是与历史比较。

建议修复：统一引入 fault-fired witness（断言 marionette `disk.crash` trace
计数 `lost/torn/reordered/metadata_lost > 0`），并在重启后断言崩溃前记录的
已提交前缀仍然存在。

#### S3. `tests/upstream/etcd_raft/cases/pre_vote_test.zig:44-88` — 相比上游丢失响应断言

etcd 原版（`TestPreVoteFromAnyState`）断言恰好产生一条 pre-vote-response
且 `reject == false`。Zig 移植版从不检查响应消息；一个静默丢弃或拒绝所有
pre-vote 请求的实现也能通过。

#### S4. `tests/upstream/etcd_raft/cases/read_index_test.zig:82-96` — 与上游语义相反

上游 `TestReadOnlyForNewLeader` 断言请求被**暂缓（postponed）**，并在 leader
提交本任期 entry 后被服务。Zig 版断言 `pendingReadCount() == 0`——即请求被
**丢弃**——且从未测试"提交后成功服务"的后半段。这把与上游相反的行为固化为
预期，可能指向一个真实的产品 bug（新 leader 上的 ReadIndex 请求未排队）。

#### S5. 一族"setup 即保证断言"的测试

- `tests/raft_test.zig:144-160` — "follower vote rejects stale candidate"：
  candidate 从未发起竞选，断言由构造直接保证。
- `tests/raft_test.zig:162-187` — "heartbeat advances follower commit"：
  心跳循环之后没有任何断言；且绕过 harness（直接调 `p1.raft.tick()`），
  跳过了持久化与安全检查。
- `tests/raft_flow_control_test.zig`（全文件）— 直接摆弄 `Inflights` 内部状态，
  再断言刚写入的值；从未走 Raft 消息路径。实际是 Inflights 单元测试，
  冠以 flow-control 之名。
- `tests/raft_snap_test.zig:115-143` — "pending snapshot pauses replication"：
  节点从未成为 leader，消息集合为空，断言循环体一次都不执行。

### 1.2 中严重度

- `tests/wal_fault_test.zig:98` — 19 个失败 case 接受**任意**错误类型，
  应按 case 断言具体的预期 `FsError`。
- `tests/wal_fault_test.zig:56-75` — 12 种 FaultFs 操作中只注入了 7 种；
  `make_dir`、`list_dir`、`file_size`、`truncate`、`unlink` 的故障路径零覆盖。
  `reserveIncarnation` 的第一次 `sync_file`（段 fsync）失败路径从未注入。
- `tests/vopr/cluster_test.zig:186-196` — VOPR 选举安全只对当前存活的 leader
  做瞬时两两比较；term T 的 leader 被 kill 或退位后，另一节点在同 term 成为
  leader 不会被发现。Network harness 的累积 term→leader 映射
  （`tests/harness/network.zig:239-249`）是正确做法，VOPR 侧没有等价物。
- `tests/vopr/cluster_test.zig:284` — chaos 动作空间在所有 16 个 seed 中
  排除了节点 0 的 kill/restart。
- `tests/vopr/cluster_test.zig:129-138` — 分区期间发出的第二条 proposal 可能
  被静默丢失；`minimum_completed = 1` 允许 `completed=1, failed=1` 通过。
- `tests/harness/fault_fs.zig:197-206` — close 的 `fail_before`/`fail_after`/
  `interrupted` 效果塌缩为同一行为；`assertConsumed` 无法发现脚本中重复的
  死条目，也无法校验跨操作的触发顺序。
- `tests/raw_node_test.zig:106-147` — "restart preserves prior HardState"
  只断言 `lastIndex() > 0`。

### 1.3 已确认 sound 的部分

- `Network.checkSafety` 在每次状态变更（deliver、tick、stepLocal、drop、
  create）时运行，累积检查单 term 单 leader，并逐条比较任意两节点已提交
  区间内的 entry（type/term/index/checksum/data/context）。不是摆设。
- `src/invariant.zig`（`applied <= committed` 等）在 Debug 和 ReleaseSafe
  构建中每次 step/tick 执行，VOPR 运行同样经过。
- VOPR chaos 与 simulation fuzz 测试有强终结断言：跨节点 applied 序列有序
  相等；收敛 digest 要求全量日志相等、inflights 清空且达到不动点。
- 所有 seed 均为硬编码确定性常量，未发现 CI 抖动风险。

## 2. Completeness 发现

数据来源：1032 条清单，其中 608 条 `planned`、43 条 `blocked`。

### 2.1 planned 用例主题分布（按风险排序）

1. **成员变更（约 112 条）** — 脑裂风险最高的协议区域。OpenRaft membership
   matrix 与 etcd `configuration` 用例基本未落地。
2. **选举与投票安全（约 99 条）** — term 单调性、pre-vote 边界、
   disruptive-server 回归。
3. **日志复制核心（约 133 条）** — 数量最大但边际风险较低；
   `log_test.zig`/`raft_paper_test.zig` 已覆盖基础路径。OpenRaft 的 log
   purge/truncation 边界是最值得优先的子集。
4. **快照与压缩（约 57 条）** — 同时受下文 harness 缺口阻塞。
5. **领导权转移（33 条）** — 与快照交互的用例 blocked。
6. **活性策略（约 38 条）** — check-quorum、flow control、
   uncommitted-limit（产品尚无对应功能）。
7. **生命周期/重启/bootstrap（约 29 条，主要来自 OpenRaft）**。
8. **Ready/RawNode 语义（约 31 条）、ReadIndex（12 条）、提案管线（14 条）**。

### 2.2 blocked 用例及所需能力

| 分组 | 数量 | 所需能力 |
| --- | ---: | --- |
| A. 确定性网络缺少快照安全校验 | 7 | `tests/harness/network.zig:223` 当前直接返回 `SnapshotSafetyUnsupported`；比较逻辑需跳过已压缩前缀。单点改动，解锁杠杆最大。 |
| B. 灾备恢复 API（全部 HashiCorp） | 19 | RecoverCluster 式管理恢复（6 条）与 UserRestore 式外部快照导入（13 条）。同时也是生产必需的灾备能力。 |
| C. 模拟器缺少 applied-SM 内省（全部 OpenRaft） | 11 | 暴露快照覆盖前缀、将持久日志与存活节点分离建模、产出可比对的状态机摘要、follower apply 交接。属架构性改动。 |
| D. 流量控制 API（raft-rs） | 2 | 动态 max-inflight 调整与 inflight 资源回收。 |
| E. 异步取日志（raft-rs） | 4 | `RawNode.onEntriesFetched` 及 stale-term 防护。 |

### 2.3 缺失的跨时间不变量

当前已验证：选举安全（仅 Network 侧，累积式）、log matching（已提交前缀
横截面对比）、状态机安全（收敛时 applied 序列相等）、`applied <= committed`
（每步检查）。

完全缺失：单节点 commit index 单调不回退、已提交前缀时间稳定性（已提交
entry 之后不被改写）、leader append-only、leader completeness（仅在最终
收敛时近似检查）、VOPR 侧累积 term→leader 映射。

### 2.4 清单未覆盖的产品区域

| 区域 | 现状 |
| --- | --- |
| `src/ready_processor.zig` | 0 测试，是 raftor 编排的关键粘合层。 |
| `src/rpc/` | 仅 4 条回环测试；无多 peer、断线重连、背压、传输层故障注入。 |
| `wal_io_uring` | AGENTS.md 范围表标记 Selected，但零代码、零构建选项、零测试。需决策：实现或修改范围表。 |
| `examples/minimal_node.zig` | 仅编译，无 smoke run。 |
| `tests/public_api_test.zig` | 仅符号钉扎，属预期用途。 |

### 2.5 清单本身的缺口

- etcd `testdata/*.txt` datadriven rafttest 语料基本未入库（41+ 个文件仅登记
  1 条）。这是 etcd 最系统的安全场景语料，移植成本低、价值高。
- 无 Dragonboat / braft / NuRaft 来源。Dragonboat 是最值得补充的第五个来源。
- raft-zig 自有层（rpc、raftor、WAL、loopback）没有"计划覆盖"的登记机制。

## 3. 优先级建议

P0 — 先修 soundness，再扩覆盖；否则新测试建立在假阳性之上：

1. 首先调查 S4（疑似真实 ReadIndex 产品 bug：新 leader 上请求被丢弃而非暂缓）。
2. 重写或改名 S1/S5 空转测试，使其真实驱动所声称的行为。
3. 为 pre-vote 移植版补上响应消息断言（S3）。
4. 为崩溃/断电测试加入 fault-fired witness 与崩溃前已提交前缀锚点（S2）。

P0 — 让 `Network.checkSafety` 支持快照（A 组）：单点 harness 改动，解锁
7 条 blocked 与 57 条 snapshot 主题的落地路径。

P1 — 落地成员变更与选举安全的 planned 用例；为 VOPR 增加累积 term→leader
与单调 commit 历史检查。

P1 — 决策 RecoverCluster/UserRestore 范围（解锁 19 条 HashiCorp blocked）与
wal_io_uring（实现或更新 AGENTS.md）。

P2 — ReadyProcessor 单元测试；rpc 多 peer/重连/故障注入测试；etcd datadriven
语料入库；VOPR seed 数量改为构建选项（如 `-Dvopr-seeds=N`），CI smoke 与
nightly 大扫描分离。

P3 — 视 raft-rs 兼容性目标决定异步取日志（4 条）与流量控制 API（2 条）；
补充 Dragonboat 作为第五个清单来源。

## 4. 审计时的验证状态

- Debug / ReleaseSafe / TSan / UBSan：359/359 通过。
- wal-durability：11/11；vopr-smoke：19/19；fuzz-wal-crash：1060 runs，
  无 crash reproducer。
- 本报告所有发现针对的是测试**证明了什么**，而非它们今天是否通过。
