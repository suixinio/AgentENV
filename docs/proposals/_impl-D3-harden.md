# D3：按 V1 审查报告加固 + 补 lookup 四分类测试

> 2026-08-19 · 研发 agent 交付报告。只动 Go（`services/`），Rust `src/` 零改动，
> 未连集群、未 `make k8s-apply`、未 commit / push。
> 上位：[`_review-V1-phase0.md`](_review-V1-phase0.md)、[`_impl-plan-control-plane-phase01.md`](_impl-plan-control-plane-phase01.md)、
> [`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md) §6。

---

## 0. 一页速览

- 审查报告 **13 条：修 11 条、按裁决不修 2 条**（P2-11 窗口微秒级接受现状；P2-12 的"建只读 PG 角色"是运维动作，本轮不碰集群）。
- 任务 B 的 13 个用例**全部有直接单测**。开工时发现 `lookup_test.go` 已由 D2 落盘（521 行）并覆盖其中 11 个，
  我补齐了缺的 2 处（用例 9 的冷 bindings 半边、用例 10/11 的"被调用就 t.Fatal"reader），未重写该文件。
- **23 个变异全部被挡住**（4 个时钟 + 9 个 lookup 判定 + 10 个本轮修复点），明细见 §3。
- 四条命令 + 真 PG 集成测全绿，集成测 **0 skip**（见 §4）。
- 顺手多修了一个报告没提但会污染新 gauge 的缺陷（节点身份别名），见 §1 附注与 §5。

改动文件：
`scheduler/internal/reconcile.go`（重写）、`metrics.go`、`node_registry.go`、
`registry/{registry.go,postgres.go}`、`scheduler/cmd/main.go`；
测试：`reconcile_test.go`、`metrics_test.go`（新建）、`lookup_test.go`、
`node_registry_roster_test.go`、`registry/postgres_integration_test.go`。

---

## 1. 审查报告逐条处置

### P0-1 节点整台消失时其序列被 Reset 删光 —— ✅ 修（两件都做了）

① 新增 `agentenv_scheduler_registry_rows_without_roster{node}`：**从登记表那一侧算**——
每行取 `Holder()`（resuming 取 claimer），该 holder 没有新鲜 roster 就计一笔，按 holder 分组
（`reconcile.go` 阶段 2）。标签来自表而不是 `observed`，所以节点被 discovery 摘掉时它**变大**而不是消失。

> 一个偏离字面的地方（**故意的**）：对**有新鲜 roster 的节点**我额外播一个 0 值种子。
> 理由是验收计划 V0-2 要求 `/metrics` 上能看到这条序列；纯按表侧算的话，健康集群里
> 这个 GaugeVec 一个子序列都没有，`grep` 不到会被判成"没实现"。计数仍然只来自表：
> 有新鲜 roster 的节点按定义就是 0。

② `roster_stale` 的标签全集换成 discovery 已知节点全集。实现方式不是给
`computeRegistryReconcile` 再塞一个 `knownNodes` 参数，而是把
`NodeRegistry.Rosters()` 换成 `RostersInCluster(clusterID)`，让它**把"discovery 知道但从没心跳过"
的节点也吐出来**（`LastSeen` 零值、无 sandbox）。这样：

- 从没心跳过的节点渲染成 `roster_stale=1` 而不是不存在；
- 顺带把 P1-8 的根子问题解决了：`Roster{LastSeen: 零值}` 从"生产不可达的构造"变成
  **这个方法的正常产物**，那条分支不再是死代码。

### P1-2 roster 侧不按 cluster 过滤 —— ✅ 修

`RostersInCluster(clusterID)` 按 `record.node.GetClusterId()` 过滤（trim + 小写归一，
两侧 UUID 文本分别经 config 文件与环境变量而来，大小写差一位会把 roster 侧整个清空）。

作用域**不是**新加一个配置项，而是 `Reader` 新增 `ClusterID() string`，
由 `Service.registryReconcileInput` 直接取用：**SQL 过滤什么，roster 就过滤什么**，
两边不可能被配歪。空 cluster_id = 两侧都不过滤，语义与 reader 一致。

### P1-3 `untracked`/`staleCopy` 拿陈 roster 当证据 —— ✅ 修

这两个计数移到独立一轮（`reconcile.go` 阶段 4），入口即 `if !rosterFresh { continue }`，
与 `ghost` / `holderConflict` 同一把尺子。陈 roster 的节点**仍然出现**在这三条序列里、值为 0，
配 `roster_stale=1` 说明原因——不是把节点整条抹掉。

### P1-4 `parkedLeaseExpiring` 算进了永远抢不走的行 —— ✅ 修

`publishing`/`local_only` 分两支：
- `snapshot_id` 为空 ⇒ 记入新增的 `agentenv_scheduler_registry_stranded_rows`，**不进租约口径**；
- 否则才按 `COALESCE(lease_expires_at, updated_at) < now()+window` 计 `parked_lease_expiring`。

依据（读了 Rust，未改）：`claim_for_resume` 的 WHERE 有 `snapshot_id IS NOT NULL`
（`postgres.rs:548`）；`release_node_holdings`（`:877,897`）与 `reclaim_expired_holdings`（`:733,752`）
的 `LIVE_HOLDINGS_OF_NODE` / `state IN ('running','resuming')` 都只碰 live 行。
所以这类行**谁也抢不走、谁也清不掉**，只有 origin 自己还能本地 resume
（`arbitration()` 对 `Conflict{origin == 本节点}` 判 `Proceed`，`paused_recovery.rs:829`）。

dev 集群那行（`local_only` + `snapshot_id NULL`）现在的读数：`parked_lease_expiring=0`、`stranded_rows=1`，
与验收计划 V0-3 的期望一致。

> Help 文案里写明了一个噪声源：每次 pause 都会短暂经过 `publishing` + 无 snapshot，
> 所以这条要按"持续多久"告警，不是"出现即告警"。

### P1-5 `holderConflict` 与 `staleCopy` 不互斥 —— ✅ 修（按裁决改逻辑）

冲突判定提到 staleCopy **之前**（阶段 3）算出 `unattributable` 集合，阶段 4 遇到该集合里的
sandbox 直接跳过 staleCopy 计数。现在同一个 sandbox 要么"能归属⇒输家计 staleCopy"，
要么"归不了⇒只计一次 holderConflict"，注释所声称的互斥成立。

`untracked` **保留**：登记表没有这行、与两个节点说不清谁持有，是两件不同的事，都成立。

### P1-6 时钟混用测试假绿 —— ✅ 修，并复跑了那 4 个变异

不止改一行。按报告给的 `+90m` 改完只能挡住 3 个：`in.now.Sub(LastSeen) → in.listing.Now.Sub(...)`
这一个在 `+90m` 下**反而挡不住**（roster 年龄变成负数，"新鲜"和"不陈"两个结论都不变）。
所以额外做了两件事：

1. 加一个 `node-drifted` roster，`LastSeen` 打在**数据库时钟**上 —— 本机时钟看它 90 分钟没心跳（陈），
   数据库时钟看它刚刚心跳（不陈），两个时钟给出相反结论；
2. 加一行 `local_only` + snapshot 的 parked 行，把 `parked_lease_expiring` 的窗口比较也纳入断言
   （原 fixture 只有 running 行，第 3 个变异根本不经过那条分支）。

4/4 变异实测被挡，见 §3。

### P1-7 `metrics.go` 零覆盖 —— ✅ 修（新建 `metrics_test.go`）

用 `prometheus.NewRegistry()` 注册这批 collector 后 `Gather()` 读回（不引 `testutil`，
避免把 `client_model` / `godebug` 从 indirect 提成 direct 依赖，CI 的 tidy job 会因此漂移）。
三条不变式各一个测试：

- `TestRegistryMetricsSurviveAReadFailure` —— 成功轮后 gauge 有值；紧接一轮读失败后
  **每条序列的形状与数值都不变**，只有 `read_failures_total` +1；
- `TestRegistryMetricsKeepADepartedNodesRowsVisible` —— 节点消失后它的 roster 类序列消失、
  `rows_without_roster{它}` 仍在且等于它名下的行数；从没心跳过的 node-b 读成 `roster_stale=1`；
- `TestRegistryRowsForgetsStatesThatNoLongerExist` —— P2-13 的守卫。

### P1-8 `Roster{LastSeen: 零值}` 是生产不可达的构造 —— ✅ 修

- 删掉 fixture 里那条捏造的 `Roster{NodeID: "node-never"}`；
- 改为经 `Heartbeat` + discovery（`Set`）构造：
  `TestRostersInClusterReportsNodesThatHaveNeverReported`（registry 层）
  与 `TestRegistryMetricsKeepADepartedNodesRowsVisible`（端到端到指标）；
- 断言方向按报告：**从没心跳过的节点仍然出现在 `rosterStale` 里且为 1**；
  被 discovery 驱逐的节点则从 roster 视图彻底消失——这正是需要 `rows_without_roster` 的原因，
  两个测试互为对照。

### P2-9 `Get` 遇 22P02 把 reader 标成 ready —— ✅ 修（按裁决当必修）

删掉那支 `r.ready.Store(true)`，并在注释里写死理由（Ready 是"可以把缺行当权威答案"的开关，
下游那个答案会删工作区）。真 PG 集成测 `TestPostgresReaderMalformedIDDoesNotMarkTheReaderReady`
钉住：畸形 id 之后 `Ready()` 仍为 false，紧接一次跑得通的查询才翻 true。

> 副作用（可接受，方向是 fail-closed）：reader 还没 warm 时，一个畸形 sandbox id 现在拿到
> 503 而不是 404；warm 之后仍是 404。

### P2-10 feature off 与"从未成功过"不可区分 —— ✅ 修

新增 `agentenv_scheduler_registry_enabled` 0/1，`scheduler.SetRegistryEnabled()` 在
`createRegistryReader` 里对**主副本与 query-only 副本都**调用一次。Help 里写明：
其余 registry 告警表达式必须用它当门。

### P2-12 —— ✅ 修一半（按裁决）

DSN 非空但 `cluster_id` 为空时打 WARN，点名环境变量名与后果（"每次读覆盖库里所有集群"）。
**没做**给 scheduler 单独建只读 PG 角色——运维动作，本轮不碰集群。

### P2-13 `schedulerRegistryRows` 从不 Reset —— ✅ 修

`recordRegistryReconcile` 先 `Reset()` 再写。注意 `rowsByState` 会把**新版本节点写的未知 state**
也带出来（这是刻意的，看见它是唯一的发现途径），所以这条 Reset 不只是为了将来第 6 个 state，
今天就有意义。

### P2-11 Reset→Set 非原子 —— ⬜ 不修（用户已裁决）

### 附注：报告没提、但我一并修了的一条

登记表的 `origin_node_id` 是节点**自报的身份**（fleet 升级期间是 pod 名），
而 roster 以 discovery 给的身份为键。两边不做解析直接比，会让升级窗口内该节点的**每一行**
同时读成"holder 没有 roster"（我新加的 gauge 首当其冲）和"持有者自己的 stale_copy"，
而这个窗口按 `node_registry.go` 的注释"can be hours"。

修法：`registryReconcileInput.resolveNodeID` 走 `Service.canonicalNodeID`（即
`nodes.Resolve`，与 `lookup.go` 的 preferNode 解析同一条路径）；纯函数里为 nil 时退化成恒等，
测试照旧不受影响。守卫：`TestReconcileResolvesRowsWrittenUnderAPreviousNodeIdentity`
+ `TestReconcileInputIsScopedByTheReadersOwnCluster`（变异 FIX-10）。

---

## 2. 任务 B：13 个用例落在哪个测试函数

开工时 `services/scheduler/internal/lookup_test.go` **已存在**（D2 于 10:59 落盘，521 行），
13 条里 11 条已被覆盖。我的处置是在其上补齐缺口而不是重写（重写会丢掉已有的 8 个额外用例，
且与 D2 的产物冲突）。全部用**真 `Service` + 假 registry reader + 真 node registry（经 `Heartbeat` 喂 roster）**。

| # | 用例 | 测试函数（`scheduler/internal/lookup_test.go`） | 来源 |
|---|---|---|---|
| 1 | 读失败 ⇒ `Unavailable`（非 NotFound） | `TestLookupNeverTurnsAnUnreadableRegistryIntoNotFound/the read failed` | 已有 |
| 2 | 未 warm（`Ready()==false` 且无行）⇒ `Unavailable` | `TestLookupNeverTurnsAnUnreadableRegistryIntoNotFound/the reader has never read` | 已有 |
| 3 | 未装配（`ErrDisabled`）⇒ `NotFound` | `TestLookupWithoutARegistryKeepsTheOldAnswer` | 已有 |
| 4 | `paused` + origin 可调度 ⇒ PLACED 且选中 origin | `TestLookupPrefersTheOriginForAPausedSandbox`（连测 3 次，排除轮询巧合） | 已有 |
| 5 | `paused` + origin DRAINING ⇒ PLACED 选别的节点 | `TestLookupPlacesAPausedSandboxAwayFromADrainingOrigin` | 已有 |
| 6 | `local_only` + origin DRAINING ⇒ `FailedPrecondition` | `TestLookupRefusesToPinToANodeThatWillNotServe/origin is draining`（另含 `origin is not reporting`） | 已有 |
| 7 | `local_only` + origin 正常 ⇒ PINNED 到 origin | `TestLookupPinsAParkedSandboxToItsOrigin/local_only`（同表覆盖 `publishing`） | 已有 |
| 8 | `resuming` ⇒ 路由到 `claimed_by_node_id` | `TestLookupRoutesAResumingSandboxToItsClaimer`（对照组 `TestLookupRoutesARunningSandboxToItsOrigin`） | 已有 |
| 9 | `running` + holder 不在新鲜 roster ⇒ `FailedPrecondition`，**且 warm 时才这样** | warm：`TestLookupLiveSandboxOnASilentNodeIsFailedPrecondition`；冷：**`TestLookupWithholdsALiveSandboxJudgementWhileBindingsAreCold`（本轮新增）** | 补齐 |
| 10 | binding 命中 ⇒ BOUND 且**根本不查 registry** | `TestLookupBindingHitNeverReadsTheRegistry`（**本轮改用 `forbiddenRegistryReader`，被调用即 `t.Fatal`**） | 加强 |
| 11 | binding miss + roster 命中 ⇒ BOUND 且不查 registry | `TestLookupFallsBackToTheRosterWithoutReadingTheRegistry`（同上，且断言选中的是 roster 说的 node-a 而非行里的 node-b） | 加强 |
| 12 | 未知 state ⇒ `FailedPrecondition`（fail-closed） | `TestLookupRefusesAnUnrecognisedRegistryState` | 已有 |
| 13 | `QueryOnlyService` 有行但无法 place ⇒ `Unavailable` | `TestQueryOnlyLookupRunsTheSameLadder/a registered sandbox it cannot place is unavailable` | 已有 |

同文件另有 8 个用例不在这 13 条里但一并跑：陈 roster 被忽略、paused 无可用节点 ⇒ Unavailable、
pod 名写入的 origin 能解析、可读且空 ⇒ NotFound、冷 bindings 扣住 NotFound、空 sandbox_id、
binding store 故障、`Schedule` 不吃 preferNode。

---

## 3. 变异验证表

方法：把工作区整份复制到 scratchpad（`/tmp/.../scratchpad/services-base`），每个变异
在**独立副本**上打、跑 `GOWORK=off go test`，工作区零改动。全部 23 个变异**都被挡住**。

### 3.1 时钟混用（P1-6 点名的那 4 个，`reconcile.go`）

| # | 变异 | 结果 | 报错断言 |
|---|---|---|---|
| CLOCK-1 | `LeaseExpired(dbNow)` → `LeaseExpired(in.now)` | **FAIL（挡住）** | `expected the live lease to be judged against the database clock, got 1` |
| CLOCK-2 | `dbNow.Sub(UpdatedAt)` → `in.now.Sub(UpdatedAt)` | **FAIL（挡住）** | `expected the young row to be judged against the database clock, got 1 ghosts` |
| CLOCK-3 | `dbNow.Add(leaseWarnWindow)` → `in.now.Add(...)` | **FAIL（挡住）** | `expected the parked lease window to be measured from the database clock, got 1` |
| CLOCK-4 | `in.now.Sub(LastSeen)` → `in.listing.Now.Sub(...)` | **FAIL（挡住）** | `expected a roster last seen 90 minutes ago to be stale on the local clock` |

（对照：审查报告实测原测试只挡住第 4 个；现在 4/4。）

### 3.2 lookup 四分类判定（任务 B，`lookup.go`）

| # | 变异 | 结果 | 被哪个测试挡住 |
|---|---|---|---|
| LOOKUP-1 | 读失败分支 `Unavailable` → `NotFound` | **FAIL** | `TestLookupNeverTurnsAnUnreadableRegistryIntoNotFound/the read failed` + `TestQueryOnlyLookupRunsTheSameLadder` |
| LOOKUP-2 | 摘掉 `!reader.Ready()` 冷 reader 卫兵 | **FAIL** | `.../the reader has never read` |
| LOOKUP-3 | `place(entry.OriginNodeID)` → `place("")`（丢亲和性） | **FAIL** | `TestLookupPrefersTheOriginForAPausedSandbox` + `TestLookupIgnoresAStaleRoster` |
| LOOKUP-4 | `entry.Holder()` → `entry.OriginNodeID` | **FAIL** | `TestLookupRoutesAResumingSandboxToItsClaimer` |
| LOOKUP-5 | pin 前 `schedulableNode` → `liveNode`（不查 DRAINING） | **FAIL** | `TestLookupRefusesToPinToANodeThatWillNotServe/origin is draining` |
| LOOKUP-6 | 未知 state `FailedPrecondition` → `NotFound` | **FAIL** | `TestLookupRefusesAnUnrecognisedRegistryState` |
| LOOKUP-7 | 无 placer 分支 `Unavailable` → `NotFound` | **FAIL** | `TestQueryOnlyLookupRunsTheSameLadder/a registered sandbox it cannot place is unavailable` |
| LOOKUP-8 | 在 binding 之前插一次 `registry.Get`（热路径落库） | **FAIL** | `TestLookupBindingHitNeverReadsTheRegistry` + `TestLookupFallsBackToTheRosterWithoutReadingTheRegistry` |
| LOOKUP-9 | 摘掉 roster 兜底那一步 | **FAIL** | `TestLookupFallsBackToTheRosterWithoutReadingTheRegistry` |

### 3.3 本轮每个修复点各一发

| # | 变异（= 把修复退回去） | 结果 | 被哪个测试挡住 |
|---|---|---|---|
| FIX-1（P1-3） | 阶段 4 去掉 `!rosterFresh → continue` | **FAIL** | `TestReconcileStopsCountingAgainstAStaleRoster` |
| FIX-2（P1-4） | 去掉 `snapshot_id == ""` 分流 | **FAIL** | `TestReconcileSeparatesStrandedParkedRowsFromClaimableOnes` |
| FIX-3（P1-5） | 去掉 `unattributable` 跳过 | **FAIL** | `TestReconcileHolderConflictNeedsTwoFreshRostersTheRegistryCannotSettle/registry names neither reporter` |
| FIX-4（P0-1①） | 去掉 `rowsWithoutRoster` 累加 | **FAIL** | `TestRegistryMetricsSurviveAReadFailure` + `TestRegistryMetricsKeepADepartedNodesRowsVisible` |
| FIX-5（P2-13） | 去掉 `schedulerRegistryRows.Reset()` | **FAIL** | `TestRegistryRowsForgetsStatesThatNoLongerExist` |
| FIX-6（P1-7） | 读失败分支里插一次 `recordRegistryReconcile(空)` | **FAIL** | `TestRegistryMetricsSurviveAReadFailure` |
| FIX-7（P2-9） | 22P02 分支恢复 `ready.Store(true)` | **FAIL**（真 PG） | `TestPostgresReaderMalformedIDDoesNotMarkTheReaderReady` |
| FIX-8（P1-2） | `RostersInCluster` 忽略 cluster 过滤 | **FAIL** | `TestRostersInClusterScopesToOneCluster` + `TestRostersInClusterReportsNodesThatHaveNeverReported` |
| FIX-9（P0-1②） | `RostersInCluster` 不吐从没心跳过的节点 | **FAIL** | `TestRegistryMetricsKeepADepartedNodesRowsVisible` + `TestUnregisterClearsTheRoster` |
| FIX-10（附注） | `resolveNodeID` 接成恒等（不解析 pod 名） | **FAIL** | `TestReconcileInputIsScopedByTheReadersOwnCluster` |

---

## 4. 全绿门槛的实际输出

```
$ cd apps/AgentENV/services && export GOWORK=off

$ go build ./...
(no output)

$ go vet ./...
(no output)

$ gofmt -l .
(no output)

$ go test -count=1 ./...
?   	agentenv/services/api/proto	[no test files]
?   	agentenv/services/gateway/cmd	[no test files]
ok  	agentenv/services/gateway/internal	0.033s
?   	agentenv/services/scheduler/cmd	[no test files]
ok  	agentenv/services/scheduler/internal	0.081s
ok  	agentenv/services/scheduler/internal/registry	0.005s
ok  	agentenv/services/shared/config	0.008s
ok  	agentenv/services/shared/logging	0.003s
?   	agentenv/services/shared/observability	[no test files]
```

真 PG 集成测（**0 skip**，即真的连上库跑了，不是假绿）：

```
$ SCHEDULER_REGISTRY_TEST_DSN='postgres://aenv:verify@127.0.0.1:15499/aenv?sslmode=disable' \
    go test -count=1 ./scheduler/internal/registry/
ok  	agentenv/services/scheduler/internal/registry	0.175s

$ ... -v | grep -c SKIP
0
```

额外确认 CI 的 `tidy` job 不会因这轮改动报漂移（新测试只用了已有的 direct 依赖
`prometheus/client_golang`，没有把任何 indirect 提成 direct）：

```
$ GOFLAGS=-mod=mod GOPROXY=off go mod tidy   # 在 scratchpad 副本里跑，工作区未动
go.mod: no drift
go.sum: no drift
```

集成测里本轮新增两个：`TestPostgresReaderMalformedIDDoesNotMarkTheReaderReady`（P2-9）、
`TestPostgresReaderReportsItsClusterScope`（P1-2 的作用域来源）。

---

## 5. 我发现但没做的问题

1. **`rows_without_roster` 不区分"救得回"与"救不回"。** 一台节点没了，它名下的 `paused`
   行（有快照）别的节点能接管，`local_only`/`publishing` 行不能——这条 gauge 把两类合在一起。
   看的时候要配 `registry_rows{state}` 一起读。要分开的话是加一个 `state` 维度，
   属于口径扩张，本轮没做。
2. **`holder_conflict` / `invalid_rows` / `stranded_rows` 都没有节点维度。** 数字告诉你有几行坏了，
   但要定位到哪台机器还得去查 `/registry/sandboxes`。同样属于口径扩张。
3. **`ListRegistrySandboxes` 的 `node_id` 过滤没做身份解析**
   （`service.go` 里 `sandbox.Holder() != nodeFilter` 直比）。fleet 升级窗口内，按 discovery 的
   节点名过滤会漏掉那台节点用 pod 名写下的行。对账口径我已经修了，这个只读 API 没跟上——
   一行的事（复用 `Service.canonicalNodeID`），但它是 D2 的代码面，我没动，避免与并行改动撞车。
4. **`registry_reconcile_duration_seconds` 只量了读，没量派生。**
   `recordRegistryReconcileDuration(start)` 在 `List` 返回后立刻调用，之后的 compute + 写指标不计入，
   而 Help 写的是 "Duration of one reconciliation round"。要么挪到轮末，要么改 Help。
5. **本轮改了 `NodeRegistry` 接口**（`Rosters()` → `RostersInCluster(clusterID)`）。仓内没有第二个
   实现（测试全用 `AtomicNodeRegistry`），所以零成本；但如果外部有 mock，这是一个 breaking change。
6. **并发编辑风险已解除但值得记一笔**：开工时 `lookup.go` 正在被 D2 改动（mtime 距我读文件 23 秒），
   我等它稳定 45 秒后才落笔，并在收尾时确认 `lookup.go` / `service.go` / `gateway/internal/server.go`
   的 mtime 未再变化。我全程没有修改 `lookup.go` 与 `service.go`（`lookup_test.go` 只做增量）。
