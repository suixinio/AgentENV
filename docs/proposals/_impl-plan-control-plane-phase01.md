# 实施计划：中央控制面 阶段 0 + 阶段 1

> 2026-08-19 · 主 agent 裁决，供研发 agent 执行。
> 上位文档：[`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md)
> 侦察产物（**开工前必读对应章节**）：
> - [`_recon-R1-readpath.md`](_recon-R1-readpath.md) — Go 侧改造点清单（file:line）
> - [`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md) — Rust registry 语义规格书
> - [`_recon-R3-cluster-runbook.md`](_recon-R3-cluster-runbook.md) — pve-sg dev 集群 + 发布 runbook

---

## 0. 本轮范围

| 阶段 | 做不做 | 理由 |
|---|---|---|
| 阶段 0（中央影子对账） | ✅ | 零风险，产出后续决策依据 |
| 阶段 1（读路径上收） | ✅ | 删掉 §1.1 四层补丁 + 修 §1.2 亲和性缺陷 |
| Rust 单测 → Go **写路径**契约测试 | ❌ 推迟到阶段 2 | Go 侧此刻**没有写路径实现**，测试无被测对象。安全绳的载体本轮改为 `_recon-R2-registry-spec.md` 规格书（已落仓）+ 只读路径的真 PG 测试 |
| 阶段 2（PG 写权上收） | ✅ **已解锁** | 🚦 **闸门 A 于 2026-08-19 通过**：用户裁决**长期自维护 fork**，手动从上游拉取检查合并。上游追平成本不再是约束，允许改动 Rust 主干。详见 `_impl-plan-control-plane-phase2.md` |
| 阶段 3 / 4 | ❌ | 仍卡在闸门 B（fencing 方案选定 + 与 agent-platform 敲定 ExecutionID 跨仓契约） |

---

## 1. 🔴 侦察推翻/修正了方案的四处，以修正版为准

### 1.1 阶段 0 的指标口径要重写（R2 §6）

方案 §4 阶段 0 列的四个口径 `orphan` / `ghost` / `holder_conflict` / `lease_expiring`
**有两个算不准、一个会把两件事混起来、还有一个（`sync_ok`）本阶段根本不可得**：

- **`orphan` 算不出来。** `mark_running` 的契约是 "Never creates a row"
  （`mod.rs:204-205`），所以「本节点创建、从没 pause 过」的沙箱**本来就没有行**，
  这在稳态下是多数情况。roster ∖ registry 是 orphan 的**超集**，健康集群里恒非零。
  ⇒ 改名 `untracked`，并在文档/告警说明里写死"这不是 orphan"。
- **`holder_conflict` 在健康集群里也非零。** 跨节点接管的过渡态里 origin 仍持有 paused
  记录、claimer 已经有活的，两边 roster 都报同一个 id。⇒ 拆出 `stale_copy`（能用登记表
  归到单一权威持有者的那一侧），`holder_conflict` 只留给登记表也说不清的。
- **`lease_expiring` 混了两件事。** `publishing`/`local_only` 租约将过期 = **可被抢走、
  会丢最后一次 pause 的工作**（可告警）；`running`/`resuming` 租约过期 = 本身零后果。
  ⇒ 拆成三个：`parked_lease_expiring`（告警）/ `live_lease_lapsed`（信息）/
  `reclaimable_now`（两条件同时成立，最该盯的数）。
- **`sync_ok` 本阶段不可得。** heartbeat 是 node→scheduler 单向推，没有"controller 问、
  node 答"这一步。阶段 0 只能上报 `roster_stale`（≈ e2b 的 `answered`）。
  **不许把 `sync_ok` 写进阶段 0 验收清单。**

**新增一个方案没提但更该有的**：`invalid_rows` = `state='paused' AND snapshot_id IS NULL`。
这类行今天会让 Rust 侧 `get_many` **整批报错**（`postgres.rs:236-241`），
即一条坏行静默冻结一台机器的全部对账。中央只读对账是第一次能看见它的地方。**应当恒为 0。**

### 1.2 阶段 1 的「查 roster 兜底」今天没有数据源（R1 坑 1）

方案写「回落读必须同时查一次 `ListObservedNodes` 的 roster」——
`ObservedNode`（`proto:157-167`）**没有 per-sandbox 列表**。heartbeat 里的 `sandbox_ids`
（`proto:177`）唯一去处是 `service.go:265` 的 `ReconcileNode`，写进 BindingStore 后即丢弃。
所以「roster」今天 == BindingStore 本身，查它救不了 BindingStore 的 miss。

⇒ **阶段 0 必须先把 heartbeat roster 留存到 `observedNodeRecord`**
（`node_registry.go:38-42`）。这本来就是对账的必需输入，阶段 1 直接复用。

### 1.3 `--query-only` 副本会把阶段 1 整个绕过去（R1 坑 2）

gateway 的数据面 `LookupNode` 走 `s.queryOnlyScheduler`（`server.go:219`），
`Schedule` 走 `s.scheduler`（`:257`）——两条不同 gRPC 连接（`gateway/cmd/main.go:53-63`）。
`QueryOnlyService`（`service.go:77-94`）只有 `store` 一个字段。
若只把 registry reader 接到 `Service` 上，一旦有人配了 `gateway.query_only_scheduler_addr`，
**所有 resume 的 lookup 都会拿到裸 `NotFound`，新逻辑一次都不生效**——
而当前 k8s base 恰好没配这个地址，本地测不出来。

⇒ **`QueryOnlyService` 必须同样装 registry reader。**

### 1.4 删掉 reroute 后，Rust 侧的 503 会直通客户端（R1 坑 3）

`x-agentenv-reroute: schedule` 的产生方在 `src/api/isolation.rs:60-68`（本轮**不动**的 Rust
代码），节点 `scheduling_disabled()` 且沙箱曾登记过就会发。
`paused` 走软偏好那条安全（`FilterUnschedulable` 会排掉 DRAINING）；
**`publishing`/`local_only` 硬钉 origin 那两条不安全**——硬钉到一台 draining 的 origin，
节点 gate 判「可在别处重建」→ 发 503 → gateway 没有重放能力 → 用户吃 503。

⇒ 裁决：**controller 侧先判 origin 可调度性**。origin 处于 DRAINING / 不在新鲜 roster 里时，
LookupNode 答一个**专门的分类**，gateway 转 **503 + 明确 body + 专门指标**，
而不是让请求打过去再吃一个语义模糊的 503。

> 注：今天走 reroute 重放到另一台，那台的 `claim_for_resume` 对租约未过期的
> `local_only` 会答 `NotReady` → 409。所以删掉重放对这个场景**不是功能倒退**，
> 只是错误码从「409 打了一趟」变成「503 一次答完」，且语义更准。

---

## 2. 阶段 0 详细设计

### 2.1 新增依赖

`services/go.mod` 加 `github.com/jackc/pgx/v5`（modcache 已有，网络可达 proxy.golang.org）。
用 `pgxpool`，**只读**，`max_conns` 小（默认 4）。

### 2.2 配置（照抄 `shared/config/config.go` 现有写法：字段 + UnmarshalJSON + env override + validate）

```jsonc
"scheduler": {
  "registry": {
    "dsn": "",                      // 空 = 关闭整个阶段 0，一切降级为今天的行为
    "cluster_id": "",               // 空 = 不按 cluster 过滤（dev 集群是全零 UUID）
    "max_connections": 4,
    "reconcile_interval": "30s",    // 对账周期
    "query_timeout": "5s",
    "lease_warn_window": "30s"      // parked_lease_expiring 的前瞻窗口
  }
}
```

env override：`SCHEDULER_REGISTRY_DSN`（**DSN 只走 env / Secret，不进 ConfigMap**）、
`SCHEDULER_REGISTRY_CLUSTER_ID`、`SCHEDULER_REGISTRY_RECONCILE_INTERVAL`。

`validate`：DSN 非空时才校验其余项；DSN 为空一律放行（feature off）。

### 2.3 新包 `services/scheduler/internal/registry/`

```go
// 只读读模型 —— 🔴 必须包含租约列，Rust 的 ENTRY_COLUMNS 不含它们（R2 歧义 2）
type Sandbox struct {
    SandboxID        string
    ClusterID        string
    State            State      // publishing|paused|resuming|local_only|running
    Generation       int64
    OriginNodeID     string
    ClaimedByNodeID  string     // 空 = NULL
    SnapshotID       string     // 空 = NULL
    PausedAt         time.Time
    UpdatedAt        time.Time
    LeaseExpiresAt   *time.Time
    SandboxExpiresAt *time.Time
}

// Holder 返回「权威持有者」：resuming 看 claimed_by_node_id，其余看 origin_node_id。
// 🔴 R2 歧义 4：resuming 行的 origin_node_id 仍指向持有本地 artifacts 的旧节点，
// 任何按 origin_node_id 与 roster 比对的口径对 resuming 行都会得出错误结论。
func (s Sandbox) Holder() string

type Reader interface {
    Get(ctx context.Context, sandboxID string) (Sandbox, bool, error)
    List(ctx context.Context) ([]Sandbox, error)
    // Ready 报告 reader 是否曾成功查过一次（warm）。false ⇒ 调用方必须答 UNAVAILABLE，
    // 🔴 绝不能答 NOT_FOUND（方案 §3.1）
    Ready() bool
}
```

SQL 固定为 R2 §6.4 的最小读模型：

```sql
SELECT sandbox_id, cluster_id, state, generation,
       origin_node_id, claimed_by_node_id, snapshot_id,
       paused_at, updated_at, lease_expires_at, sandbox_expires_at
  FROM paused_sandboxes
 [WHERE cluster_id = $1]
```

**只读纪律**：本包只允许 `SELECT`。连接串附加 `default_transaction_read_only=on`
（在 pool 的 `AfterConnect` 里 `SET default_transaction_read_only = on`），
让"误写"在数据库层就失败，而不是靠代码评审。

**降级语义**：DSN 为空 ⇒ 装 `disabledReader`，`Ready()` 恒 false、`List` 返回
`ErrRegistryDisabled`。调用方据此区分「关了」与「坏了」——前者走今天的行为，后者答 UNAVAILABLE。

### 2.4 heartbeat roster 留存（阶段 1 的前置）

`node_registry.go` 的 `observedNodeRecord` 增加 `sandboxIDs []string` + `lastSeen`。
`Heartbeat` 处理时同时写 BindingStore（现有）与这份 roster（新增）。
新增查询：
- `RosterOf(nodeID) ([]string, freshAt time.Time, ok bool)`
- `NodesHolding(sandboxID) []string` — 反向索引，对账的 `holder_conflict` 与阶段 1 的兜底都要用

新鲜度判定统一用 `scheduler.report_ttl`（默认 30s）的 3 倍作为 `roster_stale` 阈值。

### 2.5 对账循环 + 指标

新文件 `services/scheduler/internal/reconcile.go`，`RunRegistryReconcile(ctx, interval)`
（照抄 `RunObservedNodesMetrics` 的形状，`service.go:287`）。

每轮：`registry.List()` + 全部节点 roster → 算下表 → 写 Prometheus gauge。
读失败**不清零**旧 gauge（避免误报"一切归零"），而是把 `..._read_failures_total` 加 1，
并把 `..._last_success_timestamp_seconds` 停在原处。

| 指标（前缀照 `metrics.go` 现有约定） | 类型 | 口径 |
|---|---|---|
| `registry_rows{state}` | gauge | 按 state 分布 |
| `registry_untracked{node}` | gauge | `roster(N) ∖ registry` — 🔴 **不是 orphan**，健康集群非零 |
| `registry_ghost{node}` | gauge | `state='running' AND origin=N AND id ∉ roster(N)`，且 N 的 roster 新鲜、行 `updated_at` 超过 1 个 report_ttl + margin。**不算 resuming** |
| `registry_stale_copy{node}` | gauge | `id ∈ roster(N)` 但 `Holder() != N` |
| `registry_holder_conflict` | gauge | id 出现在 ≥2 个新鲜 roster **且** 无法用登记表归到单一持有者 |
| `registry_parked_lease_expiring` | gauge | `state IN ('publishing','local_only') AND lease_expires_at < now()+window` ← **可告警** |
| `registry_live_lease_lapsed` | gauge | `state IN ('running','resuming') AND LEASE_EXPIRED` ← 信息性 |
| `registry_reclaimable_now` | gauge | `live_lease_lapsed AND sandbox_expires_at < now()` ← **最该盯** |
| `registry_roster_stale{node}` | gauge 0/1 | `now-last_seen > 3×report_ttl` |
| `registry_invalid_rows` | gauge | `state='paused' AND snapshot_id IS NULL` ← **应恒为 0** |
| `registry_read_failures_total` | counter | 读失败 |
| `registry_reconcile_duration_seconds` | histogram | 单轮耗时 |
| `registry_last_success_timestamp_seconds` | gauge | 最后一次成功对账 |

`LEASE_EXPIRED` 的 Go 实现必须逐字对齐 Rust：`COALESCE(lease_expires_at, updated_at) < now()`
（`postgres.rs:85-87`）。时间基准统一取**数据库的 `now()`**（随 List 一起 `SELECT now()` 返回），
不要用 scheduler 进程时钟 —— R2 歧义 3 指出 `begin_pause` 写的是节点进程时钟，
本来就已经有一处漂移源，别再引入第二处。

### 2.6 只读 API

- scheduler gRPC 新增 `ListRegistrySandboxes(ListRegistrySandboxesRequest) returns (...)`：
  可选 `state` 过滤、可选 `node_id` 过滤、`page_size` + `page_token`（照抄仓内已有分页习惯）。
  registry 未装配 ⇒ `FAILED_PRECONDITION`；未 warm / PG 不可达 ⇒ `UNAVAILABLE`。
- gateway 新增 `GET /registry/sandboxes`（路由与 `/nodes` 同级）：
  转调上面的 gRPC，JSON 输出。字段名用 camelCase，与 gateway 现有 `/nodes` 输出风格一致。

  🔴 **更正（2026-08-19，集群验证 F1）**：本条原写「走现有 API key 中间件」，**事实上 gateway 侧
  没有任何鉴权中间件** —— `/nodes` 与本端点都由 gateway 自己应答，谁都不校验；`/sandboxes` 那条
  401 是**转发到的节点**给的，不是 gateway。本端点因此**自带一条非空 `X-API-Key` 检查**
  （头名与节点侧 `src/api/impls/auth.rs` 一致），缺失或空 ⇒ 401，请求不出 gateway。
  这只是一道门不是锁：它挡住"裸 curl 就能拿到全集群沙箱 ID + 归属节点"，不替代 gateway
  真正的凭据校验体系（独立决策，不在本轮）。`/nodes` **刻意不动** —— Agent-Console /
  AENV-Panel 今天就是不带凭据读它的。

  查询参数是封闭集合 `state` / `nodeID` / `limit` / `nextToken`，**其余一律 400**；
  `state` 不在五个合法值内也是 400（校验在 scheduler 侧，gateway 把 `InvalidArgument` 映射成
  400）。理由同一条：过滤器静默不生效比没有过滤器更糟，因为返回的东西看上去仍然像答案。

### 2.7 部署清单

- `deploy/k8s/base/scheduler-deployment.yaml`：加
  ```yaml
  env:
    - name: SCHEDULER_REGISTRY_DSN
      valueFrom:
        secretKeyRef: { name: agentenv-postgres, key: dsn, optional: true }
  ```
  🔴 **`optional: true` 是硬要求** —— 没有这个 Secret 的集群（上游默认部署）必须照常启动。
- `deploy/k8s/base/scheduler-service.yaml`：把 `:9101` metrics 端口加进 Service
  （今天只开了 grpc 9090，观测不到，R3 B7）。

---

## 3. 阶段 1 详细设计

### 3.1 proto

```proto
enum SandboxLocation {
  SANDBOX_LOCATION_UNSPECIFIED = 0;  // 旧行为：node 就是答案（binding 命中）
  SANDBOX_LOCATION_BOUND       = 1;  // binding / roster 命中，node = 持有者
  SANDBOX_LOCATION_PLACED      = 2;  // 登记表 paused：placement 结果，origin 是软偏好
  SANDBOX_LOCATION_PINNED      = 3;  // 登记表 publishing/local_only：硬钉 origin
}

message LookupNodeResponse {
  Node node = 1;
  SandboxLocation location = 2;     // 新增
  string origin_node_id = 3;        // 新增，PLACED/PINNED 时携带，便于观测与排错
}
```

**向后兼容**：旧 gateway 只读 `node`，行为等价于今天走完 recovery 分支的结果（且更好，
因为带 origin 亲和）。新 gateway 读 `location` 决定日志/指标与 `recordAssignment` 策略。

`ScheduleRequest` / `ScheduleRequestHint` **不改** —— 删掉 `scheduleRecoveryNode` 后没有
外部消费方，origin 软偏好在 controller 内部 placement 直接表达（"如无必要，勿增实体"）。

### 3.2 `lookupNode` 的新判定顺序

```
1. BindingStore 命中且未过期        → BOUND，返回该 node
2. roster 反查（阶段 0 留存的）命中新鲜节点 → BOUND，返回该 node
                                       （补 binding TTL 抖动的窗口，方案 §4「404 的前提条件」第三条）
3. registry 未装配（DSN 空）        → 走今天的行为：NOT_FOUND
4. registry 装配但 !Ready() 或读失败 → 🔴 UNAVAILABLE（绝不 NOT_FOUND）
5. registry 有行：
   a. state=paused        → placement(preferNode=origin) → PLACED
                            origin 可调度就是它；不可调度就换一台（软偏好）
   b. state=publishing
      | state=local_only  → origin 可调度 → PINNED(origin)
                            origin 不可调度 → 🔴 FAILED_PRECONDITION（坑 3）
   c. state=running
      | state=resuming    → Holder() 节点（resuming 取 claimed_by_node_id）→ BOUND
                            该节点不在新鲜 roster 里 → FAILED_PRECONDITION
6. registry 无行                    → NOT_FOUND（此时是权威答案）
```

「可调度」的判定复用 `filter.go` 的 `FilterUnschedulable`（会排掉 DRAINING），
外加"在新鲜 roster 里"。

### 3.3 placement 的 origin 软偏好（修 §1.2）

`strategy.go` / `Schedule` 内部路径加一个 `preferNodeID string` 入参：
候选集过滤后，若 `preferNodeID` 仍在候选集里就直接选它，否则退回原策略。
**不改 `Schedule` RPC 的对外签名**，只加内部函数参数。

### 3.4 gateway 删补丁

删除：`scheduleRecoveryNode`、`captureReplayBody`、`restoreReplayBody`、
`maxReplayBodyBytes`、`rerouteToScheduledNode`、`proxyRequestOptions.allowReroute`
及其在 `proxyRequest` 里的 reroute 分支。

错误映射：
| scheduler gRPC code | gateway HTTP |
|---|---|
| `NotFound` | 404 |
| `Unavailable` | **503**（不是 404 —— 这正是要修的 bug 的镜像） |
| `FailedPrecondition` | 503 + body 说明 origin 不可用 |

指标：`route_source` 增加区分 PLACED / PINNED 的口径（R1 附注指出今天 recovery 与普通
resume 在指标上无法区分，拿不到 baseline）。

### 3.5 测试改造

`reroute_test.go`（3 个）与 `server_test.go`（3 个）依赖三段补丁，需重写为对新分类的断言。
**必须新增**的用例：
- registry 不可达 ⇒ gateway 返回 **503 而不是 404**（方案 §3.1 的验收条件，最重要的一条）
- registry 未 warm（刚启动）⇒ 同上
- `local_only` + origin DRAINING ⇒ 503 且不打到 origin
- `paused` + origin 可调度 ⇒ 选中 origin（亲和性生效，§1.2 的回归防线）
- `paused` + origin DRAINING ⇒ 选中别的节点
- `resuming` ⇒ 路由到 `claimed_by_node_id` 而不是 `origin_node_id`
- binding miss + roster 命中 ⇒ BOUND，不打 PG

---

## 4. 通用纪律

1. **注释中文**（仓内 AgentENV 是英文注释风格 —— 🔴 以 `apps/AgentENV/` 现有代码为准，
   Go 侧 `services/` 现有注释是英文，**保持英文**，不要引入中英混排）
2. Conventional Commit 前缀：`feat:` / `fix:` / `refactor:` / `chore:`
3. 每个改动必须 `make -C services test` + `make -C services vet` + `make -C services fmt-check` 全绿
4. **不许动 `src/`（Rust）** —— 本轮阶段 0/1 的定义就是"node 侧代码零改动"
5. **不许跑 `make k8s-apply`** —— 它会静默删掉 `[orchestrator.paused_registry]` 配置节，
   把 PG registry 关掉且不报错（R3 §3.2）。发布只用 `kubectl set image`
6. 发布镜像一律用**不可变 tag**，绝不用 `:latest`（203 上缓存着过期的 latest，会静默跑旧代码）
