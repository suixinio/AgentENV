# D7：Go 契约测试（Rust `paused_registry` → `registry.Store` 语义移植）

> 2026-08-19 · 研发 agent 产物。对应 [`_impl-plan-control-plane-phase2.md`](_impl-plan-control-plane-phase2.md) §2 🅑 **B2**。
> 规格来源只有两处：[`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md)（语义规格书）与
> `tests/paused_registry.rs`（29 个 Rust 集成测原文）。
> **全程未读 Go 实现**（`store_postgres.go` / `migrate.go` / `grace.go` / `registry_service.go` 及其测试）——
> 唯一例外见 §0.2，那是装配 seam，不是语义。

**交付文件**（全部我拥有，未碰任何其它文件）：

| 文件 | 内容 |
|---|---|
| `services/scheduler/internal/registry/contract_test.go` | harness + pause 生命周期 + 读路径 + cluster 作用域 + fail-closed 补测 |
| `services/scheduler/internal/registry/contract_claim_test.go` | 三分法 `ClaimForResume` + `MarkRunning` claim guard + `ReleaseClaim` |
| `services/scheduler/internal/registry/contract_lease_test.go` | `RenewLease` + `ReleaseNodeHoldings` + `ReclaimExpiredHoldings` + `WithLeaseTTL` |

---

## 0. 方法论与两条纪律

### 0.1 期望值只从 Rust 侧推导

每条断言的来源是 R2 规格里的**逐字 SQL 谓词**或 Rust 测试的注释所声明的不变式，
不是 Go 实现的行为。因此本文件里「红」有三种含义，§3 逐条分类：
实现未完成 / 疑似实现缺陷 / 我的期望可能写错。

### 0.2 唯一一次接触实现文件（已披露）

`store.go` 冻结时**没有构造入口**，测试无法编译。我 SendMessage 问了 team-lead，
在等待期间用 `grep -nE '^func New'` 取了**一行函数签名**（`store_postgres.go:120`），
未看任何函数体。随后 team-lead 直接改了 `store.go`（新增 `NewStore` 签名文档、
`Migrate`、`WithLeaseTTL`、`Close`，删掉 `BeginPauseInput.LeaseTTL`），测试已按新契约重写，
那一行 grep 得到的 `NewPostgresStore` 最终**没有被使用**。

📌 **一处待你确认的口径不一致**（不影响测试，但会影响 D6）：你的裁决说
「构造函数**自己跑 migration 建表**」，而 `store.go:284` 的文档写的是
「It does not connect and it does not migrate; call Migrate for that.」。
我按**接口文档**走：harness 先按 `SCHEMA_DDL` 逐字建表，再显式调一次 `store.Migrate(ctx)`
——两种口径下都成立，而且顺带把计划 §5.1 那条硬门禁变成可执行的（见 §4 S10）。

### 0.3 harness 的两个刻意偏离（Rust 侧的缺点不照抄）

1. **幂等建表 + 只删自己的行，永不 DROP**：按 `SCHEMA_DDL` 逐字建表（幂等），
   每个测试一个随机 `cluster_id`，cleanup 只
   `DELETE FROM paused_sandboxes WHERE cluster_id = ANY($1::uuid[])`（含 `otherCluster` 注册的那个）。
   Rust harness 只分区、从不清理，本地反复跑会让表无限增长。
   > 我第一版写成了「进来时表不存在就建、结束 DROP」，为的是绕开同包
   > `postgres_integration_test.go` 当时那条「表已存在就 `t.Fatalf`」的守卫。
   > team-lead 裁定那条守卫本身是地雷（它意味着套件跑第二遍必挂）并把它拆了，
   > 我已按新姿势重写。**实测连跑两遍都过，且跑完表里剩 0 行**（§5）。
2. **防假绿**：`SCHEDULER_REGISTRY_TEST_REQUIRED=1` 且无 DSN ⇒ `t.Fatal`，不是 `t.Skip`。
   与 Rust 的 `AENV_PAUSED_REGISTRY_TEST_REQUIRED` 同形。

### 0.4 时钟参数

`contractLeaseTTL = 1s`（Rust `TEST_LEASE_SECS = 1.0`）、
`contractPastLease = 1600ms`（Rust `PAST_LEASE`）。全套 sleep 合计约 20s。

---

## 1. 移植对照表

### 1.1 `tests/paused_registry.rs` 29 个集成测

| # | Rust 测试 | Go 测试 | 钉住的不变式 | R2 优先级 |
|---|---|---|---|---|
| 1 | `a_failed_publish_keeps_the_snapshot_the_sandbox_already_had` | `TestContractAFailedPublishKeepsTheSnapshotTheSandboxAlreadyHad` | `BeginPause` 的 upsert **不清 `snapshot_id`**；`BeganPause.PreviousSnapshotID` 必须来自 `previous` CTE；失败的发布仍能从上一个快照被别处认领 | **P0** |
| 2 | `a_live_holder_cannot_have_its_sandbox_taken_away` | `TestContractALiveHolderCannotHaveItsSandboxTakenAway` | `running` 行永不可抢 ⇒ `Conflict{origin}`（第二副本 bug） | **P0** |
| 3 | `a_live_sandbox_is_never_taken_over_on_a_lapsed_lease` | `TestContractALiveSandboxIsNeverTakenOverOnALapsedLease` | 租约过期**也不**抢活行：租约只证明够不到 DB，不证明持有者死了 | **P0** |
| 4 | `a_parked_sandbox_moves_on_once_its_holder_stops_renewing` | `TestContractAParkedSandboxMovesOnOnceItsHolderStopsRenewing` | 三分法第二支：`publishing` 租约活着 ⇒ `NotReady{origin}`；过期后 ⇒ `Claimed` 且 `PreviousState == publishing` | **P0** |
| 5 | `a_successor_process_releases_what_the_previous_one_was_running` | `TestContractASuccessorProcessReleasesWhatThePreviousOneWasRunning` | `ReleaseNodeHoldings` released 出口：有快照 ⇒ 回 `paused`、快照保留、任意节点可认领 | **P0** |
| 6 | `a_successor_process_discards_live_rows_that_never_published` | `TestContractASuccessorProcessDiscardsLiveRowsThatNeverPublished` | discarded 出口：无快照 ⇒ DELETE，不留永远无人能认领的行 | **P0** |
| 7 | `releasing_holdings_touches_nothing_but_this_nodes_live_rows` | `TestContractReleasingHoldingsTouchesNothingButThisNodesLiveRows` | 作用域三重收窄：别人的活行 + 自己的 parked 行 + **另一个集群同名节点的活行**都不碰 | **P0** |
| 8 | `an_interrupted_resume_is_released_by_the_node_that_claimed_it` | `TestContractAnInterruptedResumeIsReleasedByTheNodeThatClaimedIt` | `resuming` 按 `claimed_by_node_id` 判持有者，不是 `origin_node_id`；释放后 claimer 清空 | **P0** |
| 9 | `only_the_holder_can_renew_its_lease` | `TestContractOnlyTheHolderCanRenewItsLease` | `RenewLease` 谓词由 SQL 决定而不是调用方：非持有者续 0 行，持有者续 1 行 | **P0** |
| 10 | `a_paused_sandbox_is_claimable_immediately` | `TestContractAPausedSandboxIsClaimableImmediately` | 三分法第一支不看租约；且 claim **不动 `origin_node_id`**、写 `claimed_by`、`generation +1`（=2） | **P0** |
| 11 | `an_ordinary_claim_reports_no_takeover` | `TestContractAnOrdinaryClaimReportsNoTakeover` | 🔴 `PreviousState` 必须来自 `previous` CTE 而不是 `RETURNING` 的行（潜伏数月那条 bug） | **P0** |
| 12 | `marking_a_sandbox_running_cannot_erase_another_nodes_claim` | `TestContractMarkingASandboxRunningCannotEraseAnotherNodesClaim` | `MarkRunning` claim guard；被拒方**返回 false**（Rust 未断言，我加了）；claimer 完成后 `origin` 换人、`claimed_by` 清空 | **P0** |
| 13 | `one_cluster_cannot_reach_anothers_sandboxes` | `TestContractOneClusterCannotReachAnothersSandboxes` | cluster 作用域四条：`Get` 看不到 / claim 得 `NotFound` / `Remove` 删不掉 / `BeginPause` 劫持被拒 | **P0** |
| 14 | `a_downgrade_that_matches_nothing_is_reported` | `TestContractADowngradeThatMatchesNothingIsReported` | `MarkLocalOnly` 的 CAS 失败必须上报 `ErrGenerationConflict`，且行保持 `publishing` | P1 |
| 15 | `a_sandbox_that_never_published_is_never_claimable` | `TestContractASandboxThatNeverPublishedIsNeverClaimable` | `snapshot_id IS NOT NULL` 前置：租约过期多久都只给 `NotReady{origin}` | **P0** |
| 16 | `releasing_a_claim_puts_the_sandbox_back` | `TestContractReleasingAClaimPutsTheSandboxBack` | `ReleaseClaim` ⇒ 回 `paused`、`claimed_by` 清空、可被第三个节点重新认领 | P1 |
| 17 | `marking_an_untracked_sandbox_running_reports_that_it_is_untracked` | `TestContractMarkingAnUntrackedSandboxRunningReportsThatItIsUntracked` | `false` 语义 + 🔴 **never creates a row**（Rust 未断言建行，我加了 `Get` 复核） | **P0** |
| 18 | `marking_a_tracked_sandbox_running_reports_the_node_as_holder` | `TestContractMarkingATrackedSandboxRunningReportsTheNodeAsHolder` | `true` 语义 | P1 |
| 19 | `marking_running_reports_a_refusal_when_another_node_holds_the_claim` | `TestContractMarkingRunningReportsARefusalWhenAnotherNodeHoldsTheClaim` | 拒绝必须可与成功区分 | **P0** |
| 20 | `a_batch_read_reports_only_the_sandboxes_that_have_rows` | `TestContractABatchReadReportsOnlyTheSandboxesThatHaveRows` | 全有或全无：present=present，absent=「没有行」，永不是「我没查」 | **P0** |
| 21 | `a_batch_read_cannot_see_another_clusters_sandboxes` | `TestContractABatchReadCannotSeeAnotherClustersSandboxes` | `GetMany` 的 cluster 作用域（reconciliation 就是拿它删本地） | **P0** |
| 22 | `a_batch_read_of_nothing_asks_nothing` | `TestContractABatchReadOfNothingAsksNothing` | 空输入 ⇒ 空 map 且不报错 | P2 |
| 23 | `a_sandbox_that_outlived_its_deadline_on_a_silent_node_is_reclaimed` | `TestContractASandboxThatOutlivedItsDeadlineOnASilentNodeIsReclaimed` | 回收两条件同时成立才动手；released 出口保留快照、清 claimer；**另一个集群同样过期的行不动** | **P0** |
| 24 | `a_sandbox_still_within_its_deadline_survives_a_silent_node` | `TestContractASandboxStillWithinItsDeadlineSurvivesASilentNode` | 只有租约过期不够（那是分区，抢了就双活） | **P0** |
| 25 | `an_expired_sandbox_stays_with_a_node_that_is_still_reporting` | `TestContractAnExpiredSandboxStaysWithANodeThatIsStillReporting` | 只有 deadline 过期不够（还在续租的节点自己驱逐更优） | **P0** |
| 26 | `a_sandbox_with_no_deadline_is_never_reclaimed` | `TestContractASandboxWithNoDeadlineIsNeverReclaimed` | `sandbox_expires_at IS NULL` 永不匹配（「永不过期」与「未知」都按安全方向） | **P0** |
| 27 | `reclamation_leaves_parked_rows_alone` | `TestContractReclamationLeavesParkedRowsAlone` | 回收只作用于 `running`/`resuming`；顺带钉住 **`paused` 不在续租谓词里**（续 0 行） | P1 |
| 28 | `reclamation_discards_expired_rows_with_nothing_to_rebuild_from` | `TestContractReclamationDiscardsExpiredRowsWithNothingToRebuildFrom` | 回收 discarded 出口 | P1 |
| 29 | `a_renewal_moves_the_deadline_the_row_is_judged_against` | `TestContractARenewalMovesTheDeadlineTheRowIsJudgedAgainst` | deadline 来自持有者上报而不是行里的 metadata | **P0** |

**29/29 全部移植**（P0 20 项全覆盖，P1 8 项、P2 1 项一并做了 —— 它们成本极低且都是变异检测器）。

### 1.2 Rust 侧缺失、Go 侧补的（全部 fail-closed 方向）

| Go 测试 | 钉住的不变式 | 来源 |
|---|---|---|
| `TestContractBeginPauseRefusesAnotherClustersRow` | 跨集群 upsert 被拒 ⇒ `ErrInvalidRecord`，且原行的 state/generation/origin/snapshot **一个都没动** | 任务点名（但见 §4.0：Rust 侧其实有一半覆盖） |
| `TestContractCompletePauseRefusesAStaleGeneration` | `CompletePause` 的 generation CAS 失败 ⇒ `ErrGenerationConflict`，且**不写 `snapshot_id`** | 任务点名 |
| `TestContractCompletePauseRefusesARowThatIsNoLongerPublishing` | 谓词里 `AND state='publishing'` 这一半：generation 没变时只有它能挡住迟到的第二次发布把 `running` 行拖回 `paused` | R2 §2.2 逐字 SQL |
| `TestContractAPausedRowWithNoSnapshotIsRefusedNotSkipped` | `paused && snapshot_id IS NULL` ⇒ `ErrInvalidRecord`；**且 `GetMany` 里一条坏行必须让整批报错**，不能悄悄缩短 map | 任务点名 + R2 §2.4/§2.5 |
| `TestContractAnInFlightResumeIsNeverTakenOverOnALapsedLease` | 三分法第三支的 **`resuming` 那一半**：Rust 只测了 `running`，另一半状态从没被覆盖 | R2 §2.6 谓词 |
| `TestContractARenewalKeepsAParkedRowWithItsHolder` | 🔴 续租**真的移动了 `lease_expires_at`**。Rust 只断言返回的行数——行数在「匹配到但没写时钟」时同样正确 | R2 §2.8 |
| `TestContractARenewalCannotReachAnotherClustersRows` | 续租的 cluster 作用域（node_id 是机器名，跨集群会重名） | R2 §2.8 谓词 |
| `TestContractMarkingRunningCannotReachAnotherClustersRow` | `MarkRunning` 的 cluster 作用域 | R2 §2.10 谓词 |
| `TestContractRemoveDeletesOnlyTheRowItNames` | `Remove` 只删指名那行；删不存在的行不报错（重试可幂等） | R2 §2.12 |
| `TestContractAReadCarriesTheLeaseColumnsAndTheMetadataItWasGiven` | Go 的 `Entry` 比 Rust `ENTRY_COLUMNS` 多两个租约列，读路径必须真的填它们；metadata 不丢字段 | `registry.go:67-89` 类型文档 + 计划 §3.2 |
| `TestContractTheLeaseTTLTheCallerReportsIsWhatStampsTheRow` | `WithLeaseTTL` 是 Go 侧新增语义：调用方报的 TTL 必须写进行里，而不是被进程默认值顶掉 | `store.go` `WithLeaseTTL` 文档 |
| `contractStore` 里的 `Migrate` 调用 | 🔴 **migration 必须对「node 已经建好的表」幂等**——这是计划 §5.1 那条硬门禁唯一的可执行形式 | 计划 §5.1 |

合计 **40 个 Go 测试**（`contract_test.go` 12 + `contract_claim_test.go` 13 + `contract_lease_test.go` 15）。

---

## 2. 没有移植的项 + 理由

R2 §5.6 统计的 P0 是 **36 项**，其中只有 **20 项**（§5.1 的集成测）落在 `Store` 接口上。
其余 16 项全部是**节点侧 Rust 逻辑**，阶段 2 并不把它们搬到 Go，接口上没有任何东西可以挂载：

| R2 章节 | 项数 | 内容 | 不移植的理由 |
|---|---|---|---|
| §5.2 `src/cfg.rs` | 2 个 P0（+2 P1） | `lease_ttl ≥ 3×reconcile_interval`、`reconcile_interval ≥ 1s` 的下限校验 | 这是**节点配置**的校验，仍留在 Rust。⚠️ 但 `WithLeaseTTL` 把 TTL 变成了 per-call 参数，这条下限在 Go 侧**变成了新的空白**——见 §4 的 **S3**，这是我认为最需要你裁决的一条 |
| §5.3 `paused_coordinator.rs` | 7 个 P0 | 孤儿快照三分法、`running_registrations` 语义、`live_elsewhere` 守卫 | 都是 node 进程内的状态与快照仓库的交互，`Store` 接口既看不到快照仓库也看不到进程内注册表。阶段 2 不搬这层 |
| §5.4 `paused_recovery.rs` | 3 组纯函数（`supersession` / `arbitration` / `running_supersession`） | 「什么时候删用户数据」的判定表 | 同上，node 侧纯函数。⚠️ R2 建议中央裁决层照抄这个「纯函数 + 表驱动」形状——那属于阶段 3，今天 Go 侧没有对应物 |
| §5.5 `orchestrator/tests.rs` | 1 个 P0 | `disabled_registry_is_not_cluster_backed` | Rust 侧 `Disabled` 后端的属性。Go 侧对应物是 `registry.Disabled()`（读路径，已有 `registry_test.go` 覆盖），写路径没有 disabled 形态 |

另外**刻意不断言**的两处：

1. **`ReleaseClaim` 之后的 `lease_expires_at` 具体值**。R2 §2.7 明写三处落到 `paused` 的租约写法不一致
   （`release_claim` 写 `now()+ttl`，`release_node_holdings` / `reclaim` 写 `now()`），
   并判定这是**观测口径问题不是正确性问题**。我不把任何一种写法固化成契约，
   否则你后面统一它们时会被我的测试挡住。**这条留给你裁决**（见 §4 S2）。
2. **`paused_at` / `updated_at` 的时钟源**。计划 §4 要求 B3 顺手把 `begin_pause` 的这两列改成 DB `now()`
   （Rust 写的是节点进程时钟）。我只断言两列非零，不断言来自哪个时钟——因为测试进程和 PG 在同一台机器上，
   这个断言在这里**无法证伪**，写了就是假绿。

---

## 3. 运行结果

### 3.1 全绿，一条不红

```
SCHEDULER_REGISTRY_TEST_DSN='postgres://aenv:verify@127.0.0.1:15499/aenv?sslmode=disable' \
SCHEDULER_REGISTRY_TEST_REQUIRED=1 GOWORK=off go test -count=1 ./scheduler/internal/registry/

ok  agentenv/services/scheduler/internal/registry  25.4s
```

- **40/40 PASS，0 FAIL，0 SKIP**（`grep -cE '^--- (SKIP|FAIL)'` = 0，整包 verbose 跑，含同包既有测试）。
- 我先写完全部断言、`go vet` 通过之后才第一次跑；期间实现还在编译不过
  （`undefined: DiscardBreaker` 等），我用一个「等它编译通过就自动跑」的后台轮询等到了绿。

**这与「测试写完时大概率是红的」的预期不符，所以我没有就此收工** —— 一套永远不会红的测试
和没有测试是一回事。§3.2 是我补的证伪。

### 3.2 变异验证：9 次变异，8 次被抓、1 次退化

方法：把整个 `services` 模块**拷进 scratchpad**，只在拷贝里用 `sed` / `perl` 做**盲替换**
（锚点全部来自 R2 规格书里的逐字 SQL，不是来自读实现），跑测试，再还原。
**仓库文件全程未被修改**（每轮结束 `cmp` 校验）。

| # | 变异（在 scratch 拷贝上） | 结果 | 被哪些断言抓住 |
|---|---|---|---|
| M1 | 三分法第二支扩大到 `running` / `resuming`（活行可抢） | ✅ 2 红 | `ALiveSandboxIsNeverTakenOverOnALapsedLease`、`AnInFlightResumeIsNeverTakenOverOnALapsedLease`：`got "claimed", want "conflict"` |
| M2 | 回收去掉 `AND sandbox_expires_at < now()`（只凭租约回收） | ✅ 3 红 | `AStillWithinItsDeadline…`、`AWithNoDeadline…`、`ARenewalMovesTheDeadline…`：`got {Released:1}` |
| M3 | `MarkRunning` 去掉 claim guard | ✅ 2 红 | `MarkingASandboxRunningCannotEraseAnotherNodesClaim`、`MarkingRunningReportsARefusal…` |
| M4 | `previous_state` 改成取 UPDATE **之后**的值（那条潜伏数月的 bug 的复现） | ✅ 3 红 | `AnOrdinaryClaimReportsNoTakeover`（`got "resuming", want "paused"`）、`AParkedSandboxMovesOn…`、`AFailedPublishKeepsTheSnapshot…` |
| M5 | 全局删 `cluster_id = $n` 作用域 | ⚠️ **退化**：语句参数对不上（`mismatched param and argument count`），7 个测试红在错误的理由上，不算有效变异 |
| M6 | `begin_pause` 的 upsert 里补上 `snapshot_id = NULL`（清掉被顶掉的快照） | ✅ 2 红 | `AFailedPublishKeepsTheSnapshot…`（`got "", want <snapshot>`）、`AParkedSandboxMovesOn…`（`got "not_ready"`——没有快照就永远不可认领） |
| M7 | 代替 M5：把两个 cluster id 塌成同一个 row-space（**从外部看等价于实现忽略 cluster 作用域**） | ✅ 7 红 | 七个作用域测试全红：`Get` / `GetMany` / `BeginPause` 劫持 / `MarkRunning` / `RenewLease` / `ReleaseNodeHoldings` / `ReclaimExpiredHoldings` 各一 |
| M8 | `snapshot_id IS NOT NULL` 极性翻转（released/discarded 两个出口互换 + claim 前置反转） | ✅ 5 红 | `ASuccessorProcessReleases…`、`ASuccessorProcessDiscards…`、`ASandboxThatNeverPublishedIsNeverClaimable`、`AOutlivedItsDeadline…`、`ReclamationDiscards…` |
| M9 | 两个 CAS 去掉 `AND state = 'publishing'`（只留 generation） | ✅ 1 红 | 只有 `CompletePauseRefusesARowThatIsNoLongerPublishing` 红 |

> 🔁 你裁决落地（`store.go` 改 `WithLeaseTTL` + 拆掉建表守卫、实现侧加 `grace.go` / `Logger`）之后，
> 这套变异 **原样重跑了一遍，八条红的条数逐条一致**（2/3/2/3/2/7/5/1）。

**M9 的红只有一条，这本身是个发现**：CAS 谓词里「状态」那一半
**只被一个测试覆盖，而那个测试是 Rust 侧从来没有的**（§1.2 第 3 行）。
generation 那一半有两个测试兜着，状态那一半在移植之前是裸的。

### 3.3 结论与它的边界

就本轮而言：**`Store` 的 PG 实现与 Rust `paused_registry` 在这 40 条语义上等价，且我的断言是能红的。**

三条边界，别把结论用过头：

1. **未覆盖并发**。全部是单线程顺序调用。Rust 侧那把 advisory lock（`postgres.rs:151-181`）
   与 generation CAS 的真实竞态（两个节点同时 claim / 同时 `mark_running`）本套没有测。
   R2 §5.1 那 29 个测同样没测——这是**两边共同的**空白，不是移植缺口。
2. **未覆盖失败注入**。护栏 §3.1「任何后端错误 ⇒ error 而不是空 map」只在
   「一条坏行让整批报错」这一个方向上被证明（`APausedRowWithNoSnapshotIsRefusedNotSkipped`）。
   连接断开、context 超时、部分结果这几种，需要能掐连接的 harness，本轮没做。
3. **独立性只保证我这一侧**。我没读实现；实现是否读过我的测试文件我无从保证，
   两边是并行推进的。若要更硬的独立性证据，§3.2 的变异结果比「谁先写」更有说服力。

---

## 4. 🔴 我从 Rust 规格里读出、但 Go 接口无法表达的语义

按重要性排序。这一节是给你的主要输入。

### S0（先更正规格书一处）：R2 §5.1 末尾那三条「Rust 侧缺失」，实际只缺两条

R2 说 `tests/paused_registry.rs` 里**没有** `begin_pause` 跨集群 upsert 被拒的测试。
实际上 `one_cluster_cannot_reach_anothers_sandboxes`（`tests/paused_registry.rs:609`，最后三分之一）
就断言了 `matches!(hijack, Err(PausedRegistryError::InvalidRecord { .. }))`。
真正缺失的是另外两条（`complete_pause` 的 generation 冲突、`paused && snapshot IS NULL` 不变式）。
我两种都补了（一条并入 cluster 作用域测试，一条独立），不影响结论，但规格书那句该修。

### S1：`Remove` 没有 `expect_generation` —— 全接口唯一一个无条件破坏性写

R2 §2.12 已经点名：「Go 侧的 `TransitionSandbox` 若要覆盖 `remove`，**必须补上 `expect_generation`**」。
冻结的接口里没有。

为什么阶段 2 比今天更危险：Rust 侧的守卫在**调用方**
（`paused_coordinator.rs:294-306` 的 `forget_sandbox` 先 `get` 再判 `live_elsewhere` 才删），
这是个 TOCTOU 守卫，而且它现在位于**信任边界之外**——阶段 2 的整个论点是「节点不再是决策者」。
一个陈旧节点（刚从分区里回来、还没 reconcile）对一台已经在别处 resume 的沙箱调 `remove`，
中央会照删：那台活沙箱瞬间失去可恢复的快照，且**全程零报错**。

**建议**：`Remove(ctx, clusterID, sandboxID string, expectGeneration *int64) error`，
`nil` 保留今天的无条件语义供运维用，节点侧一律必须带。
⚠️ 我的 `TestContractRemoveDeletesOnlyTheRowItNames` 钉的是**今天的**无条件语义，你改签名时要同步改它。

### S2：`ReleaseClaim` 的「0 行静默成功」在 Go 侧变得不可观测

Rust 侧 `release_claim` 不检查 `rows_affected`（R2 §2.7），0 行静默成功。
Go 签名 `ReleaseClaim(...) error` 忠实照抄了这一点——**但代价变了**：
中央化之后，「一次 release 什么都没匹配到」正是「这个节点报的 generation 已经过期 / 认领权早被别人拿走」的
唯一信号，而现在没有任何东西能把它计数或告警。

**建议**：返回 `(bool, error)` 或 `(uint64, error)`。语义不变（0 行仍不是错误），只是让它可观测。

### S3：🔴 `WithLeaseTTL` 把租约下限校验的归属**弄丢了**（TTL 单一来源已由你修掉，**下限这半仍然悬空**）

> 更新：我最初报的是「`BeginPauseInput.LeaseTTL` 与构造 TTL 构成两个 TTL 源」，
> 你已核对 Rust（`lease_ttl_secs` 是单一字段、7 条写路径共用）并删掉了那个字段，
> 改成 `WithLeaseTTL` 由调用方报——**那一半已解决**，我的 `TestContractTheLeaseTTLTheCallerReportsIsWhatStampsTheRow` 就是它的钉子。
> 下面这半**没有**被那次修改解决，反而因为 TTL 变成 per-call 而更明确了。

Rust 侧 TTL 是**连接级**的（`connect(..., lease_ttl_secs)`），且 `cfg.rs:455-458` 强制
`lease_ttl ≥ 3 × reconcile_interval`——**同一个进程同时拥有续租节奏和 TTL**，所以下限一定成立。
R2 §5.2 把这两条列成 P0，论证是「比续租节奏还短的租约会在健康节点上过期，
让停在好节点上的沙箱被无理由地从旧快照在别处重建」。

Go 侧 `WithLeaseTTL(ttl)` 让**调用方**报 TTL，`StoreConfig.LeaseTTL` 只是默认值。
于是：
- 控制面**接受节点报来的任意 TTL**，包括比该节点自己的续租周期还短的值；
- 控制面**看不到节点的 `reconcile_interval`**，因此**无处校验**这条下限；
- 这个错误的表现是「健康节点上的 parked 行被别处抢走、回退一个快照」——
  正是 R2 §4.5 说的、租约唯一有实质后果的那条路径。

`store.go` 里 `WithLeaseTTL` 的论证（「TTL 属于节点，不属于本进程」）我同意**方向**，
但它把校验一并交出去了，而校验需要的两个量（cadence 与 TTL）现在**分居两个进程**。

**建议三选一**：
1. RPC 顺带上报节点的 `reconcile_interval`，controller 侧执行 `ttl = max(reported, 3×interval)` 并打 metric；
2. controller 侧只做 `ttl = max(reported, floor)`（floor 来自 controller 配置），不需要节点多报字段；
3. 明确写下「下限只在节点侧保证」，并在 §3.2 grace 的验收里加一条「TTL 低于 floor 的上报要告警」。

我没有为这条写测试——接口上没有可以挂载的地方。

### S4：`GetMany` 的「全有或全无」在类型上无法自证

护栏 §3.1 的最后一道闸是 `GetMany` 的 `error` 返回。但接口没有任何东西表达
「我问了 N 个、我确实**查了** N 个」：一次被截断的分页、一段部分流、一个丢了后半批的 chunk 循环，
都会表现为「这些 id 没有行」，而调用方对「没有行」的反应是**删用户工作区**。

Rust 侧靠单进程 + `?` 传播来保证；跨 gRPC 之后这个保证没有了载体。

**建议**：`GetMany` 的响应（以及 `GetSandboxesResponse`）带上「本次实际覆盖的 id 集合」或
至少 `requested_count`，让 node 侧客户端在把缺失当成删除授权之前先做一次
`len(requested) == len(covered)` 的断言。这是护栏 §3.1 目前唯一没有机械保证的一环。

### S5：读路径没有数据库时钟，但 `Sandbox.LeaseExpired` 要求用它

`registry.Listing` 刻意携带 `Now`（DB 时钟），`registry.go:105-112` 的文档写死
「`now` 必须是读这批行时的数据库时钟，不是本进程的墙钟：两者已知会漂移」。

`Store.Get` / `GetMany` 返回的 `Entry` **没有**这个时钟。任何拿写路径读回来的行做租约判断的代码，
只能用本进程墙钟——正好是那段文档禁止的事。

**建议**：`GetMany` 返回 `(map[string]Entry, time.Time, error)`，或复用 `Listing` 那种信封。

### S6：`Entry` 带了两个租约列，但没有任何东西说它们**会被填**

Rust `ENTRY_COLUMNS` 不含这两列。如果 Go 实现照抄 `ENTRY_COLUMNS`，
`Entry.LeaseExpiresAt` / `SandboxExpiresAt` 会静默为零值，
而 `Sandbox.LeaseDeadline()` 在 `LeaseExpiresAt == nil` 时**回退到 `UpdatedAt`**——
于是「租约过期与否」会被用另一列的时间悄悄回答，且永远不报错。

我用 `TestContractAReadCarriesTheLeaseColumnsAndTheMetadataItWasGiven` 钉住了这条。
**建议**在 `Entry` 的文档里把它写成契约（现在只在 `Sandbox` 的类型文档里暗示）。

### S7：`MarkRunning` 的 `bool` 合并了两个事实，而 `store.go` 自己说调用方两个都要

`store.go` 的注释原文：「the difference between "not tracked" and "somebody else's" —
**and the caller needs both**」。签名只给一个 `bool`。

Rust 侧用「0 行时重读一次、只为打 warn」补偿（R2 §2.10），那次重读在阶段 2 之后发生在 controller，
**这个区分永远不会跨过网络**。计划 §3.3 的 `TransitionSandbox` 应答也没有为它留字段。

**建议**：返回 `(MarkRunningOutcome, error)`，三态：`confirmed` / `untracked` / `held_by{node}`。
（我的测试只断言 `false`，两种情况都覆盖到了，但无法区分——正是这条缺口的证据。）

### S8：`ClaimOutcomeConflict` 一个变体承担两种事实，`OriginNodeID` 一个字段承担三种含义

R2 §2.6 已经点名这是拆点。今天：
- `Conflict` = 「活在别处」**或**「刚输掉一次竞态（行已回 `paused`）」；
- `OriginNodeID` 在 `resuming` 时其实是 `claimed_by_node_id`，在 `running` 时是 `origin_node_id`，
  在「输掉竞态」时是 `origin_node_id` 而那台机器**什么都没在跑**。

调用方 `arbitration()`（`paused_recovery.rs:820-835`）用「答案指向我自己就算 Proceed」来消化这个歧义，
这在节点侧成立，中央化之后 controller 自己也要做同样判断，而它拿到的是同一个被压平的字段。

我按现状写了测试（`TestContractAnInFlightResumeIsNeverTakenOverOnALapsedLease` 断言 resuming 时
`OriginNodeID == claimer`）。**如果你要拆，我的断言需要同步改。**

### S9：护栏 §3.3「丢弃熔断」在接口上没有落脚点

`ReclaimExpiredHoldings` 与 `ReleaseNodeHoldings` 的 `Discarded` 出口是**不可逆 DELETE**。
接口没有 dry-run、没有上限参数、没有「这一轮会丢弃 N 行，超阈值就停手」的表达。
调用方只能先做后看。

**建议**：给这两个方法一个 `maxDiscard uint64`（0 = 不限），超限则整个事务回滚并返回一个可识别的错误。
熔断从「服务层的一段 if」变成「存储层的一个不变式」，成本很低。

### S10：`Migrate` 与「node 也会建表」的共存没有被表达

计划 §5.1 是硬门禁：`SCHEMA_DDL` 里的 `DROP CONSTRAINT / ADD CONSTRAINT` 是无条件的，
每台跑 `postgres` 后端的 node 每次启动都跑。controller 一旦加了新状态，
下一台启动的 node 会 `ADD CONSTRAINT` 失败并**永远起不来**。

接口只有 `Migrate(ctx) error`，没有任何东西表达「第一版必须是逐字复制、零新列」。
我能做的只有把它变成可执行的：**contract harness 先按 `SCHEMA_DDL` 逐字建表，再调 `Migrate`**，
于是任何偏离节点形状的 migration 会在这里炸。这是那条门禁目前唯一的机械保证。

### S11：「没有 metadata」这件事，两侧正在往不同方向走（本轮进行中的漂移）

我写测试期间，另一路把 Rust 侧的 `PausedSandboxEntry.metadata` 改成了
`Option<SandboxMetadata>`（`tests/paused_registry.rs:98` 的 `metadata: Some(...)` 即此改动的连带）。
而 `metadata` 列是 **`JSONB NOT NULL`**，Go 侧 `BeginPauseInput.Metadata` 是
`json.RawMessage`，接口文档里**没有说 nil 是什么意思**。

我在 scratch 拷贝里探了一次（探针已删，不在交付物里）：

| 传入 | 今天的行为 |
|---|---|
| `nil` | `ErrInvalidRecord: … has no valid metadata document`（fail-closed ✅） |
| `[]byte{}` | 同上 ✅ |
| `[]byte("null")` | **接受**，落库为 JSONB `null`，读回来还是 `"null"` |

三条都不算错，但**「没有 metadata」现在有两种表示**（拒绝 / 存成 `null`），
而 Rust 那边的 `Option` 会把 `None` 序列化成哪一种，取决于那一路怎么写。
一旦两侧选择不同，症状是：一台沙箱在 Go 侧存得下、在 Rust 侧 decode 成 `InvalidRecord`，
**而 `InvalidRecord` 会让那台机器整批 `get_many` 报错、reconciliation 全线停摆**（R2 §6.4 最后一段）。

**建议**：在 `BeginPauseInput.Metadata` 的文档里把三种输入的含义写死，并让改 Rust 的那一路确认
`None` 的线上表示。这条不需要新接口，只需要一次对齐——但没人对齐的话，它会在下一次 resume 才暴露。

---

## 5. 门槛输出

全部在 `apps/AgentENV/services` 下、`GOWORK=off`：

```
$ gofmt -l .
（空）

$ go build ./...
build OK

$ go vet ./...
vet OK

$ SCHEDULER_REGISTRY_TEST_DSN='postgres://aenv:verify@127.0.0.1:15499/aenv?sslmode=disable' \
  SCHEDULER_REGISTRY_TEST_REQUIRED=1 go test -count=1 -timeout 300s ./scheduler/internal/registry/
ok   agentenv/services/scheduler/internal/registry   25.443s
（-v 下 --- SKIP / --- FAIL 计数 = 0）

# 防假绿守卫本身也验了一遍：REQUIRED 设了、DSN 不设
$ SCHEDULER_REGISTRY_TEST_REQUIRED=1 go test -run TestContractAPausedSandboxIsClaimableImmediately ./scheduler/internal/registry/
--- FAIL: TestContractAPausedSandboxIsClaimableImmediately
    contract_claim_test.go:21: SCHEDULER_REGISTRY_TEST_REQUIRED is set but
    SCHEDULER_REGISTRY_TEST_DSN is not: these contract tests would have been skipped
```

**连跑两遍 + 无残留**（`t.Cleanup` 的 DELETE 是否真的覆盖了 `otherCluster` 注册的那个 cluster）：

```
$ go test -count=1 ./scheduler/internal/registry/   # 第一遍
ok   25.813s
$ go test -count=1 ./scheduler/internal/registry/   # 第二遍，表由第一遍留下
ok   25.865s
$ SELECT count(*) FROM paused_sandboxes
rows left in paused_sandboxes: 0
```

⚠️ 整包跑（不只 `-run Contract`）也是绿的，说明 §0.3 那条「建表/清理」的处理
**没有把同包的 `postgres_integration_test.go` 染红**——那正是它存在的理由。

📌 **给 CI（计划 §2 🅐 A3）的建议**：Go 侧这一 job 必须同时设
`SCHEDULER_REGISTRY_TEST_DSN` 与 `SCHEDULER_REGISTRY_TEST_REQUIRED=1`，
并且**断言 skip 数为 0**（仓内 `agent-worker` 那个 job 已经是这个做法）。
只设 DSN 不设 REQUIRED，等于把这 40 条安全绳挂在「有没有人记得配环境变量」上。
