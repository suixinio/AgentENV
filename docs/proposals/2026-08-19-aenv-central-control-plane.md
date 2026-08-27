# AgentENV 中央控制面改造：与 e2b 的架构对照分析

> 2026-08-19 · 分析文档（非实施方案）
> 结论先行：**AgentENV 让 node 直连 PG 不是取舍，是"系统里没有第二个能持有状态的进程"的必然结果。**
> 根因比"PG 放哪"更深一层 —— AgentENV 的用户级 API 是 **per-node** 的，
> 而 e2b 的用户级 API 只有中央一份，node 永远只暴露内部 gRPC。

---

## 1. 两家服务映射矩阵

### 1.1 服务对照

| AgentENV | e2b | 职责本质 |
|---|---|---|
| **gateway**（Go，:8080）| `client-proxy` **+ `api` 的 HTTP 入口** | AgentENV 把"数据面路由"和"控制面入口"合成了一个纯转发器 |
| **scheduler**（Go gRPC，:9090）| `api/internal/orchestrator/placement` + `nodemanager`（**api 内部包，不是服务**）| 只有"选节点"这一半；缺"持有状态 + 状态机 + 裁决"那一半 |
| **node**（Rust，:8000）| `orchestrator` **+ `api` 的全部状态职责** | 一台机器 = 一个完整的 AgentENV |
| — | **`api`（中央，PG+Redis+ClickHouse）** | 🔴 **AgentENV 没有这一层** |
| —（node 内 `/proxy` + sandbox_proxy_domains）| `client-proxy` | 数据面路由在 AgentENV 混进 gateway + node |
| —（node 内 `image/` + template 构建）| `orchestrator` 里的 `template-manager` | |
| `envd`（thirdparty，VM 内）| `envd` | 一致 |
| — | `dashboard-api` | 我们用 Agent-Console 顶了 |

### 1.2 最本质的差异：API 在哪一层

AgentENV 的 node 直接对外暴露完整的**用户级 REST API**（`src/api/generated/src/server/mod.rs`）：

```
/sandboxes  POST GET       /sandboxes/{id}/pause      /snapshots
/sandboxes/{id} GET DELETE /sandboxes/{id}/resume     /templates
/sandboxes/{id}/fork       /sandboxes/{id}/timeout    /nodes/{id}
```

gateway 做的事情就是**把这些请求转发给某台 node**。也就是说：

> **每台 AgentENV 机器都是一个功能完整、可独立使用的 AgentENV。**
> gateway + scheduler 是后来贴上去的"让 N 台机器看起来像一台"的补丁。

对照 e2b —— node 侧 `orchestrator.proto` 全部接口只有 6 个内部 gRPC：

```protobuf
service SandboxService {
  rpc Create / Update / List / Delete / Pause / Checkpoint
}
```

**没有 Resume。** 因为在 e2b 里 resume 根本不是节点操作，而是"中央拿着快照重新走一遍 Create，顺便决定放在哪台机器"。用户永远碰不到 orchestrator，它只有一个客户端：中央 api。


🔴 **这才是 PG 下沉到 node 的真正原因**：当"谁能决定一个沙箱的命运"这件事分散在 N 台机器上时，它们只能找一个共享的地方打架 —— 那个地方就是 PG。

---

## 2. e2b 的设计理念

### 2.1 一句话纲领（源码原文）

`packages/api/internal/sandbox/store.go:140`：

```go
func (s *Store) Reconcile(ctx context.Context, sandboxes []NodeSandbox, nodeID string) {
	// Redis is the source of truth — divergent sandboxes are orphans running
	// on the node but not present in the store. Kill them.
	orphans := s.storage.Reconcile(ctx, sandboxes, nodeID)
	... KillOrphanSandbox(ctx, sbx)
}
```

**中央是 desired state，节点是 actual state。节点上跑着但中央不认的，一律杀掉。**

对照 AgentENV 的架构文档原文：

> Runtime heartbeats include the node's full sandbox ID roster.
> **Scheduler treats that roster as the source of truth for that node.**

方向完全相反：AgentENV 里节点报什么就是什么，控制面只是缓存。

### 2.2 三层数据模型

| 层 | 存什么 | 谁写 | 丢了会怎样 |
|---|---|---|---|
| **PostgreSQL** | templates / snapshots / teams / builds / volumes / aliases —— **持久资产** | 只有 `api`、`dashboard-api`、`auth`（全是中央服务）| 灾难，要备份恢复 |
| **Redis** | 运行态 sandbox catalog、状态转换锁、team 配额预留、过期索引、状态变更 pubsub | `api` 多副本共享 | 可从 node roster 重建 |
| **node 本地** | VM、块设备、网络 —— **无跨重启的权威状态** | node 自己 | 沙箱没了，但不影响集群一致性 |

`packages/orchestrator/go.mod` 里 **没有任何 PG 驱动**。node 侧的 Redis 只做三件软状态的事：P2P peer registry（TTL 短、上传窗口内有效）、sandbox 事件流、网络回收 —— 而且 Redis 还允许 disabled（`ErrRedisDisabled`）。

### 2.3 中央拉取式 reconcile（关键机制）

`api/internal/orchestrator/cache.go` + `nodemanager/sync.go`，每 20s 一轮：

```go
nodeInfo, err := client.Info.ServiceInfo(ctx, &emptypb.Empty{})   // ① node 答不答
...
orphanCandidates, err := n.GetOrphanCandidates(ctx)               // ② 要它的沙箱清单
store.Reconcile(ctx, orphanCandidates, n.ID)                      // ③ 中央裁决
```

注意注释里刻意分开的两个变量：

```go
// Tracked separately from success because the two answer different questions.
// A sync can fail on a node this replica did reach ...
// and a node that answered is not unreachable however the rest of the cycle went.
answered := false
syncRetrySuccess := false
```

**中央能直接区分"节点没答"和"节点答了但没这个沙箱"。** 这正是 AgentENV 用租约永远换不来的信息 —— 我们代码里那段最长的注释（`LEASE_EXPIRED`）说的就是这个：

> It proves the holder cannot reach PostgreSQL. It does **not** prove the
> holder's process is dead.

因为节点之间**互相不可见**，它们只能通过 PG 间接推断彼此死活，而这个推断天生不成立。中央 poll 一下就有答案。

### 2.4 ExecutionID：沙箱"化身"的 fencing token

```go
// ExpectExecutionID pins the removal to one incarnation of the sandbox.
// ... the record can be removed and the ID reused by a resume before the
// removal runs, and the transition would then land on a live sandbox that
// was never in scope.
ExpectExecutionID string
```

sandbox ID 是稳定的，但**每次启动/恢复是一次新的 execution**。所有会造成破坏的操作都可以钉在某一次化身上。`SandboxInfo` 结构体里 `ExecutionID` 与 `OrchestratorID`/`OrchestratorIP` 并列，catalog 的 `DeleteSandbox(ctx, sandboxID, executionID)` 强制带上它。

我们的 `paused_sandboxes.generation` 是同一个思路的弱化版 —— 但它只在 aenv 内部有效，**平台侧（agent-platform）完全看不到它**，所以平台的判断只能退化成"HTTP 状态码猜"。

> 🔧 **深挖订正（2026-08-19 逐调用点考古 @ `6938cbb`）：ExecutionID 不是 e2b 防双活的主闸。**
>
> 1. **`ExpectExecutionID` 生产调用点为零** —— enforcement 写得很讲究（刻意放进 Redis Lua 而不是
>    Go 侧，`storage/redis/scripts.go:33-39` 逐字：`Add is lockless, so a resume can install a new
>    incarnation between a Go-side comparison and this write`），但全仓只有 `execution_pin_test.go`
>    在设它，evictor（`evictor/evict.go:158`）只传 `{Action, Eviction: true}`。
>    ⚠️ **但"未上膛"是误读**：`states.go:82-92` 的注释把范围写死了 ——
>    「`For callers that decide from a snapshot taken earlier — a background scan, a queued batch`」
>    才需要 pin，而「`Empty means "remove whatever is stored", which is correct for callers
>    acting on a fresh read or on user intent`」。`errors.go:47` 复述为 "caller **opted in**"。
>    ⇒ **pin 是刻意设计成 opt-in 的**，只对"从陈旧快照做决定"的调用方上膛；
>    那类调用方在 e2b 生产代码里还不存在，所以调用点为零。**不是没做完，是故意划了范围。**
>    引入它的提交是 `c29ee2622`（2026-08-10，"let a removal pin the sandbox incarnation it
>    meant to remove"，带 `GitOrigin-RevId` ⇒ 从内部 monorepo 导出，**无公开设计讨论，
>    裁决理由全在注释里**）。
>    📌 对我们的含义：**agent-platform 的重试队列正是注释点名的 "queued batch" 类别** ——
>    这从反面确认了 G3（平台传 execution）该在平台侧改造时做，阶段 3 不必。
> 2. **主闸是「Running 记录在库即 409」**（`handlers/sandbox_resume.go:96-106`），且**那条记录没有租约、
>    不会自动过期释放** —— `UnreachableSince`（`nodemanager/status.go:118-135`）**全仓零生产消费者**。
>    ⇒ e2b 在节点分区期间对 resume 是**彻底 fail-closed 的：等，不接管**。
>    ⚠️ 对照我们：reclaim 是「租约过期 + deadline 过期」双条件**自动接管** ——
>    **我们比 e2b 激进，因此比 e2b 更需要 fencing。**
> 3. **保护快照链的是"发布权集中"，不是 ExecutionID**：节点无自主 pause 权
>    （`orchestrator.proto:56-59`：`the orchestrator itself does not act on it — the API evictor does`）
>    + 每次 pause 造**全新 build UUID** + build 翻 `ready` 只由 API 在 RPC 成功后做
>    （`pause_instance.go:71-77`）+ resume 只选 `status_group='ready'`（`get_last_snapshot.sql:8`）。
>    ⇒ **分区旧节点即使把快照字节传完，没有中央翻牌就永远进不了快照链。**
> 4. **存储层零排他**：GCS / S3 / Azure 后端 grep `IfGenerationMatch` / `precondition` / `IfNoneMatch`
>    **零命中**；唯一的"锁"是节点本地 `O_EXCL` + 10s TTL 的 NFS 读缓存去重锁，
>    自认可多持有（`storage/lock/file_lock.go:47-49`）。
> 5. **envd token 不绑 execution**：`sandbox_envd_secret.go:27-35` token = `HMAC(sandboxID)`，
>    新旧化身相同。数据面路由则是**每请求实时查 catalog、无缓存**
>    （缓存曾存在、被 PR #2636 / #2315 刻意删掉）—— 是**收敛**不是**拒绝**。
> 6. 🔴 **e2b 自己的一个现成缺口**：`Reconcile` 判 orphan **只比对 sandboxID 存在性、不比对
>    ExecutionID / NodeID**（`storage/redis/main.go:205-217` 只看 `raw != nil`）。
>    同 ID 在 B 节点重建后，A 节点回来的旧化身查库会命中**新化身的记录** ⇒ 不判 orphan、不杀，
>    成为无路由僵尸。**我们的阶段 3 `Reconcile → KillOrphan` 正是照它抄的，别把缺口一起抄过来。**

### 2.5 显式状态机

```go
var AllowedTransitions = map[State]map[State]bool{
	StateRunning:      {StatePausing: true, StateKilling: true, StateSnapshotting: true},
	StatePausing:      {StateKilling: true},
	StateSnapshotting: {StateRunning: true, StateKilling: true, StatePausing: true},
}
```

加上 `TransitionEffect`（terminal / transient）、`KillReason` 分类（request/timeout/admin/orphaned/base_template_missing）。所有转换在**一个进程**里判定。

### 2.6 API 副本之间也要协调（reservations）

`api/internal/sandbox/reservations/redis/README.md`：Reserve 用一个 Lua 脚本原子地做完"清理过期 pending + 查重 + 用 `SCARD + ZCARD` 校配额 + 入 pending zset"，创建完成写 TTL result key 并 pubsub 通知。

⇒ **中央控制面是无状态多副本的**，它们靠 Redis 达成一致，而不是靠"每个副本一份内存"。

---

## 3. 对照结论

e2b 证明了中央控制面持有状态、节点只暴露内部执行接口的形状能够横向扩展。
AgentENV 当前把用户级 API 与状态职责下沉到每个节点，才需要所有节点通过共享 PG
竞争同一份生命周期状态。重构目标不是照抄组件，而是把发布权与仲裁权收回中央。

---

## 4. 改造后能获得什么

### 4.1 直接消灭的结构性缺陷

| # | 现状缺陷 | 改造后 | 依据 |
|---|---|---|---|
| **G1** | **租约过期 ≠ 进程已死**，live 状态永远不敢回收，只能等"同机器后继进程"或"沙箱自己到寿" | 中央 poll node，`answered` 与 `sync success` 分开判 —— 节点死活是**直接观测**而非推断 | e2b `nodemanager/sync.go` |
| **G2** | **孤儿沙箱无人回收**：节点上跑着、登记表里没有的沙箱会一直占内存和盘 | `Reconcile` → `KillOrphanSandbox`，中央每 20s 对账一次 | e2b `store.go:140` |
| **G3** | **404 有三来源且不可区分**，平台侧只能靠状态码猜，猜错就重建工作区（我们真出过事故）| 中央持有 catalog + ExecutionID，"这个沙箱存不存在/是不是同一次化身"是**查询**不是猜 | e2b `sandbox-catalog` |
| **G4** | **并发 resume 互相不知情**（节点各自 claim，靠 DB CAS 兜底，失败方拿到含义模糊的错误）| resume 变成"中央选节点 + Create with snapshot"，天然串行化 | e2b 的 orchestrator **根本没有 Resume RPC** |
| **G5** | **scheduler 单副本 + 绑定在内存，重启丢绑定**（EKS 方案 §5.3 明写"变更窗口要避开有活沙箱的时候"）| catalog 进 Redis，控制面变无状态多副本，随便重启 | e2b |
| **G6** | **schema 由节点自建**（`ensure_schema` + advisory lock，注释记着首次两节点滚动就撞 `pg_type` 唯一索引挂了一台）| schema 有 owner，走正常 migration | e2b `packages/db/migrations` |
| **G7** | **PG DSN 下发到每台跑用户代码的 KVM 机器** | node 只拿 Redis 凭据；PG 凭据只在控制面 Deployment。爆炸半径 N 台 → 1 处 | 你提的约束，与 e2b 实践一致 |
| **G8** | **PG 连接数随节点数线性增长**（每 node 默认 8 条，20 台 = 160 条常驻）| 恒定（控制面副本数 × pool） | — |
| **G9** | **平台 PG 与 aenv PG 两套真相，无人对账**：`sessions.sandbox_id/sandbox_engine` 在我们库，`sandbox → node + state + lease` 在 aenv 库，中间只有 HTTP 状态码 | 中央控制面可以对账，也可以直接被 agent-platform 查询 | 见 §5 选项 |
| **G10** | **登记表只能直连 PG 看**（Agent-Console 只能开 PG 直连）| 控制面有 API，运维面走 API | e2b 的 dashboard-api |

### 4.2 改造**不能**解决的（别抱幻想）

| 问题 | 为什么改控制面没用 |
|---|---|
| rustfs / S3 并发 multipart 503（≥3 并发必挂）| 存储服务端问题，换谁写都一样 |
| 层 >64MiB 的暂停降级 `local_only` | 快照上传耐久性问题；**e2b 在同样场景下是直接丢数据**，我们的降级反而更保守 |
| 沙箱 MTU 黑洞、npm audit 慢、预览冷启动 | 纯数据面 |
| 跨节点恢复仍依赖快照进共享存储 | 中央只能决定"去哪台"，搬不动字节 |
| 上游追平成本 | 见下 |

### 4.3 一个额外收益：可观测性口径统一

e2b 把 `indexHealed` 这类指标当**首要告警信号**（"healthy steady state is zero"）—— 中央持有状态后，"登记表和现实不一致"变成一个可计数、可告警的量。我们现在这个量根本无处可测。

---

## 5. 改造形态

🔴 **已裁决（2026-08-19）**：改造只能在 AgentENV 内部完成。
明确否决"让 agent-platform 持有 sandbox→node 绑定与状态机、AgentENV 退回 local registry"
这一选项 —— agent-platform 是 AgentENV 的**消费方**，让消费方代持被消费系统的内部状态会
打穿职责边界：AgentENV 从此失去独立多节点能力、只能自用，且 e2b SDK 兼容面脱节。

具体重构方案见 [`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md)。

## 6. 什么时候不改会真的痛

按现在 EKS 两节点 + 灰度规模，`local` 换 `postgres` 的补丁能撑住。会失效的时刻：

1. **KVM 节点扩到 5 台以上** —— 连接数、schema 自建竞态、租约误判的概率都随 N² 级的节点对增长
2. **节点开始频繁滚动**（镜像升级常态化）—— 每次滚动都有一个 §G1 的窗口，我们已经实测出 10s 量级
3. **要给沙箱做超卖 / 主动迁移** —— 这类决策必须有全局视图，节点各自看 PG 做不出来
4. **要对外提供 aenv 能力**（不只自用）—— 用户能直连 node API 这件事本身不可接受

---

## 附：本文结论的源码依据

- e2b：`packages/api/internal/sandbox/{store.go,sandboxtypes/states.go,storage/redis/,reservations/redis/}`、`packages/api/internal/orchestrator/{cache.go,pause_instance.go,create_instance.go,placement/,nodemanager/sync.go}`、`packages/shared/pkg/sandbox-catalog/`、`packages/orchestrator/{orchestrator.proto,go.mod,pkg/factories/run.go}`、`packages/client-proxy/internal/proxy/proxy.go`
- AgentENV：`src/orchestrator/paused_registry/postgres.rs`、`src/cfg.rs`、`src/api/generated/src/server/mod.rs`、`services/{go.mod,shared/config/config.go,api/proto/scheduler.proto}`、`docs/src/internals/architecture.md`

> 决策落点见 [`2026-08-19-control-plane-refactor-outcome.md`](2026-08-19-control-plane-refactor-outcome.md) §3。
