# 阶段 4 Go 侧测试基线

移植 `services/scheduler` 进 Rust `--role api` 之前，"今天全绿"到底是什么。
**测量树**：worktree detached 在 `56de492`（`Merge branch 'fix/orphan-after-node-death' into feat/phase4-scheduler-fold`）。
**测量日期**：2026-08-25。工具：`go1.26.1`、Docker 29.3.1、`redis-server` 7.0.15、`postgres:16-alpine`。

> ⚠️ 本文所有数字都来自本次实测。凡是引用本文断言"某测试不存在/未被覆盖"的，请先
> `git log --oneline -1` 复核所在树 —— 一份落后 228 个提交的基线曾把 `catalog/`、
> `sweep.go`、`applyProjectionDelete` 全部报成"不存在"。

---

## 1. 两个 make 目标的实测结果

| 目标 | 退出码 | 墙钟 | 结果 |
| --- | --- | --- | --- |
| `make -C services test` | 0 | **9 s** | 全部包 `ok` |
| `make -C services test-with-postgres` | 0 | **44 s** | 全部包 `ok` |

两个目标都会把 `shared/` 和 `api/` 跑两遍（gateway 子目标一遍、scheduler 子目标一遍），
所以 make 输出里的包行数不是唯一测试数。下面所有计数用 `go test ./... -json` 从
`services/` 根跑一遍得出，不重复。

`make test` 结尾自带的告警（`report-skipped-suites`）在本机只打印了 PostgreSQL 那一条，
因为本机 PATH 上有 `redis-server`。**在没装 redis-server 的干净 CI 机器上会多打印一条**，
且依然退出 0。

---

## 2. 假绿量化

计数口径：`pass`/`skip` 均按 **顶层 `func TestXxx`** 统计（括号内是含子测试的数）。

| 环境 | 通过 | skip | 合计 |
| --- | --- | --- | --- |
| **A. `make test` 等价**（无 DSN，本机有 redis-server） | 719 (1260) | **257 (271)** | 976 (1531) |
| **B. `test-with-postgres` 等价**（DSN + 两个 `_REQUIRED=1` + `REDIS_SERVER_BIN`） | **976 (1597)** | **0 (0)** | 976 (1597) |
| **C. 干净 CI 机器**（无 DSN，PATH 上无 `redis-server`） | 695 (1213) | **281 (318)** | 976 (1531) |

### 2.1 A 档 skip 的按包分布（257 顶层）

| 包 | 顶层 pass | 顶层 skip |
| --- | ---: | ---: |
| `scheduler/internal/registry` | 22 | **166** |
| `scheduler/internal/catalog` | 27 | **83** |
| `scheduler/internal`（仅 `catalog_service_test.go`） | 342 | **7** |
| `scheduler/cmd`（仅 `catalog_gate_test.go`） | 9 | **1** |
| `gateway/internal`, `gateway/internal/resume`, `shared/*` | 全部 | 0 |

按文件：

| 文件 | 顶层测试 | A 档 skip |
| --- | ---: | ---: |
| `registry/store_postgres_test.go` | 86 | 81 |
| `registry/execution_fencing_test.go` | 28 | 28 |
| `registry/contract_lease_test.go` | 18 | 18 |
| `registry/contract_claim_test.go` | 13 | 13 |
| `registry/contract_test.go` | 13 | 13 |
| `registry/grace_test.go` | 14 | 6 |
| `registry/postgres_integration_test.go` | 6 | 6 |
| `registry/metadata_golden_test.go` | 3 | 1 |
| `registry/registry_test.go` | 7 | 0 |
| `registry/legacy_schema_test.go` | 0（只有 helper） | – |
| `catalog/store_postgres_test.go` | 70 | 70 |
| `catalog/schema_test.go` | 8 | 8 |
| `catalog/migrate_test.go` | 14 | 5 |
| `catalog/queries_test.go` | 13 | 0 |
| `catalog/pin_test.go` | 5 | 0 |
| `catalog_service_test.go` | 28 | 7 |
| `cmd/catalog_gate_test.go` | 1 | 1 |

### 2.2 `CLAUDE.md` 的"125 个"—— 不准，已过时约一倍

`CLAUDE.md:74` 写的是「`make test` 跑暂停沙箱注册表套件时……每个碰 SQL 的测试都 skip
—— **125 个**」。这句由 `4f9c70b`（2026-08-20，距 HEAD **237 个提交**）引入。
当时 `scheduler/internal/registry` 共 133 个测试函数，其中约 125 个需要 DSN —— 数字是对的。

今天：

* `registry` 包本身已涨到 **188 个顶层测试，166 个 skip**（+41）。
* 而且 `125` 只覆盖 registry 一个包。`catalog` 包（83）、`catalog_service_test.go`（7）、
  `cmd/catalog_gate_test.go`（1）在那之后加入，本身也全部 DSN-gated。
* **今天 `make test` 下静默跳过的实际是 257 个顶层测试（含子测试 271 个）**，
  是文档所写的 2.06 倍。

建议把该句改成「**257 个**（含子测试 271 个），分布在 `registry`、`catalog`、
`catalog_service`、`cmd/catalog_gate` 四处」，并注明数字随提交漂移。

### 2.3 完整版是否归零 —— **是，归零**

B 档 skip = **0**。没有第三类被静默跳过的依赖。
唯一一个非 DSN/非 Redis 的 `t.Skip`（`shared/config/snapshot_storage_manifest_test.go:411`
`"no manifest sets AENV_CONFIG_OVERLAY_PATH"`）在当前 manifest 下不触发，三档环境里都没跳过。

### 2.4 受限 PATH：**多 24 个顶层 skip（含子测试 47 个）**，且仍然退出 0

Redis 那组确实靠 `exec.LookPath("redis-server")` 找二进制
（`scheduler/internal/redis_store_test.go:186`、`shared/routing/reader_test.go:140`）。
`SCHEDULER_REDIS_TEST_REQUIRED` 未设时走 `t.Skip`，**绿色跳过**。

实测（把 `/usr/bin` 等目录做成不含 `redis-server` 的 symlink farm 后重跑）：

| 包 | 多出的顶层 skip |
| --- | ---: |
| `scheduler/internal` | **20** |
| `shared/routing` | **4** |

具体 24 个：

* `scheduler/internal/redis_store_test.go` (3)：`TestRedisBindingStore{Get,Record,Reconcile}Cases`
* `scheduler/internal/redis_projection_test.go` (7)：`TestRedisDeleteGuardRules`、
  `TestRedisDeleteRemovesAnUndecodableRecord`、`TestRedisRecordHonoursTheNodeBudget`、
  `TestRedisHeartbeat{RefreshKeepsTheDeadline,RepairSetsTheDeadline,RefreshStillRewritesTheRecord}`、
  `TestRedisRosterBudgetsStayAlignedWithTheirSandboxes`
* `scheduler/internal/projection_restart_test.go` (3)：`TestARestartedSchedulerAnswersFromASurvivingProjection`、
  `TestARestartedSchedulerWithoutTheProjectionReproducesTheMeasured404`、
  `TestAProjectionThatOutlivedTheOutageStillExpires`
* `scheduler/internal/ha_lookup_test.go` (3)：`TestQueryOnlyReplicaAnswersWithTheExecutionOverRedis`、
  `TestTwoPrimariesSharingOneRedisConvergeOnTheNewerExecution`、
  `TestTheReplicaSeesALegacyRosterAsBindingsRatherThanAsNone`
* `scheduler/internal/projection_deadline_test.go` (1)：`TestRedisRefreshGivesADeadlineToARecordThatHasNone`
* `scheduler/internal/binding_arbitration_test.go` (3)：`TestRedisReconcileTakesOneRoundTrip`、
  `TestRedisRecordTakesOneRoundTrip`、`TestTheRedisRecordShapeIsTheGoTypes`
* `shared/routing/reader_test.go` (4)：`TestReaderGet{Hit,MissIsNotAnError,FailureIsAnError,HonoursCallerCancellation}`

**这 24 个正是唯一碰 Redis binding store / routing reader 的测试**，也正是每个 HA 部署跑的那条路径。
`services/Makefile` 的 `test-with-postgres` 用一句 `command -v redis-server || exit 1` 前置挡住了这个洞 ——
但 `make test`（CI 的 `unit-tests` 也跑它）没有这道闸。

---

## 3. 按 Stage 归类

`sweep.go` **存在**（`services/scheduler/internal/sweep.go`，383 行，心跳超时清扫路由投影），归 **Stage D**。

| Stage | 顶层测试 | `make test` 下 skip | 干净 CI 下 skip |
| --- | ---: | ---: | ---: |
| A 节点清册 | 87 | 0 | 0 |
| B catalog | 142 | **91** | 91 |
| C paused registry | 253 | **166** | 166 |
| D 剩余（binding/routing/projection/sweep/cmd） | 175 | 0 | **20** |
| E gateway + shared | 319 | 0 | **4** |
| **合计** | **976** | **257** | **281** |

A+B+C+D 在 `scheduler/internal` 内加起来正好 349 = 该包顶层测试总数，无重叠、无遗漏。

下列命令**全部实跑验证过**。前置（B/C 需要，D/E 建议）：

```bash
cd <worktree>/services
docker run -d --rm --name pg-baseline -e POSTGRES_PASSWORD=verify \
  -e POSTGRES_DB=aenv_registry -p 15499:5432 postgres:16-alpine
export SCHEDULER_REGISTRY_TEST_DSN=postgres://postgres:verify@127.0.0.1:15499/aenv_registry
export SCHEDULER_REGISTRY_TEST_REQUIRED=1 SCHEDULER_REDIS_TEST_REQUIRED=1
export REDIS_SERVER_BIN="$(command -v redis-server)"
runs(){ grep -hoE '^func Test[A-Za-z0-9_]*' "$@" | sed 's/func //' \
        | paste -sd'|' | sed 's/^/^(/;s/$/)$/'; }
```

### Stage A — 节点清册（87，无外部依赖，~2 s）

`node_registry.go` `kubernetes_discovery.go` `filter.go` `strategy.go` `warmup.go` `cpu_template.go`

文件：`node_registry_test.go` (20)、`node_registry_roster_test.go` (8)、
`kubernetes_discovery_test.go` (19)、`filter_test.go` (17)、`strategy_test.go` (2)、
`warmup_test.go` (7)、`cpu_template_test.go` (14)

```bash
A=$(runs scheduler/internal/{node_registry,node_registry_roster,kubernetes_discovery,filter,strategy,warmup,cpu_template}_test.go)
go test -count=1 -run "$A" ./scheduler/internal/     # 87 pass
```

### Stage B — catalog（142，其中 91 需 PostgreSQL，~26 s）

`internal/catalog/*`（`store_postgres.go` `queries_*.go` `pin.go` `migrate.go` + 3 个 migration SQL）、
`catalog_service.go`、`cmd/catalog_gate_test.go`、`cmd/build_queue_test.go`

```bash
go test -count=1 ./scheduler/internal/catalog/                       # 110 顶层 (211 含子测试)
B=$(runs scheduler/internal/catalog_service_test.go)
go test -count=1 -run "$B" ./scheduler/internal/                     # 28
BC=$(runs scheduler/cmd/{catalog_gate,build_queue}_test.go)
go test -count=1 -run "$BC" ./scheduler/cmd/                         # 4
```

### Stage C — paused registry（253，其中 166 需 PostgreSQL，~39 s）

`internal/registry/*`、`internal/reconcile.go`、`registry_service.go`、`service_registry_test.go`

```bash
go test -count=1 ./scheduler/internal/registry/                      # 188 顶层 (259 含子测试)
C=$(runs scheduler/internal/{reconcile,registry_service,service_registry}_test.go)
go test -count=1 -run "$C" ./scheduler/internal/                     # 65
```

### Stage D — 剩余（175，其中 20 需 Redis，~1 s + Redis 启停）

`lookup.go` `store.go` `redis_store.go` `metrics.go` `service.go`（含 projection 四件套）
`sweep.go` `cmd/main.go`

```bash
D=$(runs scheduler/internal/{lookup,lookup_execution,ha_lookup,store,redis_store,projection_deadline,projection_restart,projection_service,projection_store,redis_projection,binding_arbitration,heartbeat_lease_renewal,metrics,service,routing_golden,sweep}_test.go)
go test -count=1 -run "$D" ./scheduler/internal/                     # 169
DC=$(runs scheduler/cmd/health_test.go)
go test -count=1 -run "$DC" ./scheduler/cmd/                         # 6
```

### Stage E — gateway + shared（319，其中 4 需 Redis，~1 s）

```bash
go test -count=1 ./gateway/... ./shared/...                          # 319 顶层 (553 含子测试)
```

---

## 4. 两个高风险点的覆盖

### 4.1 `ReportSandboxEvent` → `applyProjectionDelete`

`services/scheduler/internal/service.go:576`（RPC）→ `:602`（`applyProjectionDelete`）→
`s.store.Delete(sandboxID, execution, now)`。
开关 `SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE=on` 在
`deploy/k8s/base/kustomization.yaml:199` 默认打开 —— 生产走的就是这条。

| 层 | 覆盖 | 后端 |
| --- | ---: | --- |
| **RPC 端到端**（`projection_service_test.go`） | **6** 个 `TestReportSandboxEvent*` | **只有 in-memory** |
| in-memory store `Delete` 守卫（`projection_store_test.go`） | 1 顶层 / 8 子测试 `TestInMemoryDeleteGuardRules` | in-memory |
| Redis store `Delete` 守卫（`redis_projection_test.go`） | 2：`TestRedisDeleteGuardRules`、`TestRedisDeleteRemovesAnUndecodableRecord` | **Redis Lua** |
| projection 四件套合计 | 30 顶层 / 55 含子测试 | 见上 |
| Redis 孪生文件 `redis_projection_test.go` | 7 顶层 | Redis |
| gateway 侧 `execution_fencing_test.go` / `projection_test.go` | 30 / 15 顶层 | 与后端无关 |

**结论 —— Redis 后端被测到了，但只在 store 层，不在 RPC 层。**

* 好消息：`redis_projection_test.go:15` 明确自称是 `TestInMemoryDeleteGuardRules` 的
  "Lua twin"，注释直接写「两者都存在，且互不替代」。四个 outcome
  （`absent` / `deleted` / `rejected_stale` / `deleted_unknown_incumbent`）+ 空 execution 拒绝，
  两个后端都断言了。这一条不是"改了内存版忘了 Redis 版"的形状。
* 风险点：`projection_service_test.go` 里 **每一个** `Service` 都用
  `NewInMemoryBindingStore(...)`（`:66,:82,:112,:146,:189,:279,:329,:341,:353,:377`），
  `newProjectionService(t, store BindingStore, ...)` 虽然收接口，但没有任何测试用 Redis store
  构造 Service 再调 `ReportSandboxEvent`。
  （`projection_restart_test.go` 和 `ha_lookup_test.go` 确实用 Redis store 建了 `NewService`，
  但只驱动 `Lookup`/`RecordAssignment`/`Heartbeat`。）
* 因此 **`applyProjectionDelete` 里的守卫顺序**（switch-off → 空 sandboxID →
  `normalizeExecutionIDReason` 空 execution → `store.Delete` → outcome 归类 / 指标标签）
  只在 in-memory 路径上跑过。一个只在 Redis 上出现的返回码映射错误（比如 Lua 返回值到
  `BindingDeleteOutcome` 的转换）会被 store 层测试抓住；一个"服务层把 Redis 的 error 当成
  deleted 计数"这类接线错误不会。

### 4.2 `paused_sandboxes` 的 claim / lease / fencing / reclaim 谓词

`services/scheduler/internal/registry/store_postgres.go`（2151 行）。

按测试体内实际调用的方法统计（`ClaimForResume`/`ReleaseClaim`；`RenewLease`/`RenewParkedLeases`/
`RenewLiveLeases`/`RenewSandboxDeadline`；`MarkRunning`/`BeginPause`/`CompletePause`/`expectGeneration`；
`ReclaimExpiredHoldings`/`ReleaseNodeHoldings`）：

| 谓词组 | 顶层测试 | `make test` 下 skip |
| --- | ---: | ---: |
| claim | 24 | **22** |
| lease | 30 | **30** |
| fencing（generation / incarnation） | 42 | **42** |
| reclaim | 12 | **11** |
| **并集** | **91** | **88（96.7 %）** |

整个 `registry` 包：188 顶层测试，`make test` 下 **166 个 skip（88.3 %）**。

活下来的 22 个**没有一个碰 SQL 谓词**：

* `grace_test.go` (8)：`Grace` 闸门两段开启、`DiscardBreaker` 上限取严 —— 纯算术
* `registry_test.go` (7)：状态枚举解析、`Holder` 恒等于 origin、DSN 校验 —— 纯逻辑
* `store_postgres_test.go` (5)：`TestReclamationNeedsBothClocks`、
  `TestALiveSandboxIsNeverTakenOverOnALapsedLease`、
  `TestAParkedSandboxMovesOnOnceItsHolderStopsRenewing`、
  `TestTheSchemaLockIsTheOneTheNodesTake`、`TestMigrateRetryingDeadlockRefusesEverythingElse`
  —— 对 SQL **文本**的 golden 断言，不连库
* `metadata_golden_test.go` (2)：fixture 可读性 + 与 Rust `REQUIRED_FIELDS` 对齐

**所以：没有那个一次性 PostgreSQL，claim/lease/fencing/reclaim 的行为覆盖实际为零，
只剩"SQL 字符串长这样"的形状检查。** 移植期间任何在 `make test` 上"删一块还是绿的"
判据，对 Stage C 完全无效。

---

## 5. Rust 侧要复刻的等价覆盖（一句话）

**照抄 `src/orchestrator/store/` 已有的模式：一份 backend-agnostic 的 `contract.rs` 让
in-memory 与 Redis/Postgres 两个后端各跑一遍全部断言，配 `harness.rs` 那种
`AENV_*_TEST_REQUIRED=1` → 缺依赖即 fail（否则打 `SKIPPED[...]` 到 stderr）的闸门，
并让 `make` 目标 grep 该标记后置失败；**额外补上 Go 侧现在缺的那一格 —— 把
service/RPC 层（`ReportSandboxEvent` 等价物）也参数化到两个后端上跑，而不是只在
store 层做孪生测试。**

配套三条：

1. Go 侧 `make test` 缺 `redis-server` 的静默 skip（24 个）**不要在 Rust 侧复现** ——
   Rust 的 `make test-with-redis` 已经用 `SKIPPED[redis]` grep + `AENV_REDIS_TEST_REQUIRED=1`
   把它变成失败，把 paused registry / catalog 的 Postgres 依赖接进同一套机制。
2. 移植验收判据必须以 **`make -C services test-with-postgres`** 为准，永远不是 `make test`：
   Stage B/C 的 91 + 166 个测试在后者下全部绿色跳过。
3. `CLAUDE.md:74` 的 "125" 已过时（今天是 257），顺手改掉。

---

## 6. 本次未做

* 未跑 Rust 侧任何测试（`make test-unit` / `make test-with-redis`），本文只建立 Go 侧基线。
* 未跑 `make -C services vet` / `fmt-check`（不在任务范围）。
* 未测 `--query-only` 副本的进程级 HA 行为（需要真起两个 scheduler 进程）；
  `ha_lookup_test.go` 的 3 个进程内等价物已计入 Stage D。
