# AgentENV 中央控制面重构方案

> 2026-08-19 · 实施方案（v3，**阶段 0 / 1 / 2 已实施并通过 pve-sg dev 集群验证**）
> 背景与三家架构对照见 [`aenv-central-control-plane.md`](2026-08-19-aenv-central-control-plane.md)
> 实施收口、闸门 B 决策材料与遗留项清单见
> [`2026-08-19-control-plane-refactor-outcome.md`](2026-08-19-control-plane-refactor-outcome.md)
>
> 🔴 **本文写于实施之前，多处已被侦察或实测推翻。** 订正一律就地标成「🔧 实施订正」
> 并注明证据出自哪份文档；**原判断保留不删** —— 它是当时的推理链，删掉就看不出哪里想错了。
>
> | 订正处 | 一句话 | 证据 |
> |---|---|---|
> | §3 → §3.5 | 四条护栏漏了反方向，补成**五条** | `_recon-R4-rust-client.md` §7 风险 1 |
> | §4 阶段 0 | 四个指标口径**两个算不准、一个混了两件事、一个不可得** | `_impl-plan-control-plane-phase01.md` §1.1 |
> | §4 阶段 1 | 「查 roster 兜底」当时**没有数据源** | 同上 §1.2 |
> | §4 阶段 2 | 13 → 5 个 RPC **已实施**，实际形状见订正块 | `_impl-plan-control-plane-phase2.md` §3.3 |
> | §7 | 🚦 **闸门 A 已通过**（2026-08-19 用户裁决） | `_impl-plan-control-plane-phase2.md` 抬头 |
> | §4 阶段 3-C | fencing 三候选的判断要改，**且主次对调**（写路径 fencing 才是本体）| 2026-08-19 e2b / Cube 源码考古 |
> | §4 阶段 3 | **不需要向后兼容** ⇒ schema 重画、RPC 直接重写、execution 内部必填 | 用户裁决 2026-08-19 |
> | §4 阶段 4 | **并入阶段 3**，不再"拖到最后" | 同上考古（fencing 从"检查"降级成"拓扑"）|
> | §5.3 | ExecutionID 是**内部** token；跨仓契约**退出阶段 3 关键路径** | 范围裁决 2026-08-19 |
> | §7 | 🚦 **闸门 B 已通过**（2026-08-19 用户裁决）⇒ 阶段 3 可开工 | 见 outcome §3.4 |
>
> 🔴 **本文 §5 重估该文 §4.1 的 G 列表**（G1 的性质、G3 的可达性、G5/G6 的归属四条已变），
> 以本文为准。
>
> **裁决前提**：改造在 AgentENV 内部完成，agent-platform 是消费方，不代持任何沙箱状态。
> 但见 §5.3 —— **阶段 3 会破这个前提**，那是一个必须提前知道的跨仓契约变更，不是意外。
>
> 🔧 **实施订正（2026-08-19 范围裁决）：上面那句"阶段 3 会破这个前提"作废 —— 前提不会被破。**
> 本轮设计**只做 AgentENV**，agent-platform 后期再改。ExecutionID 被重新定位成**内部** fencing
> token（e2b 的原始形态就是这样，见 §5.3 订正），阶段 3 因此**不需要任何跨仓协同**。
> 另一条前提也变了：**AgentENV 尚未上生产，无向后兼容包袱**，允许按最优架构直接重画。

---

## 0. 一句话

把 `scheduler` 从"无状态选节点器"升级成 **controller**（e2b `api` / Cube `CubeMaster` 的角色）：
唯一持有 PG 的进程、唯一的状态裁判；node 退回纯执行器；gateway 退回纯入口。

**但顺序反过来走**：先建**观测面**，再收**读路径**，最后才动**写路径与裁决权**。
每一步都能单独上线、单独回退、单独兑现价值 —— 而不是先把旧语义固化成网络契约、再推翻它。

---

## 1. 根因：系统里没有第二个能持有状态的进程

`src/orchestrator/paused_registry/mod.rs`，`reclaim_expired_holdings` 的文档注释：

> This is where e2b puts the same decision. Its eviction runs in the control plane
> off a cluster-wide expiry index, entirely independent of node state
> (`e2b/packages/api/internal/orchestrator/evictor/evict.go`), and it drops the
> sandbox from its store even when the node cannot be reached to be told.
> **Our eviction lives on the node instead, which is why losing the node used to
> mean losing the eviction with it.**

⇒ 这不是"我们否定上游设计"，是**上游作者已知的、因缺少控制面而不得不接受的妥协**。

### 1.1 补丁堆叠的现场：gateway 的 resume 路径

`services/gateway/internal/server.go`，为了在"没有权威决策者"的前提下做对一次 resume：

1. `:218` `LookupNode` → NotFound 且是 resume ⇒ `:224-241` `scheduleRecoveryNode()` 挑一台，让它去抢
2. 挑中的节点可能拒绝（isolated）⇒ `:286` `captureReplayBody()` 提前缓冲请求体准备重放
3. 拒绝了 ⇒ `:315` `rerouteToScheduledNode()` 重放到另一台
4. body 太大缓冲不下 ⇒ **放弃重路由**（注释原文 "simply forfeits the reroute"）

同一件事在 e2b 里是：查 catalog → 未命中查 snapshots → placement 选节点 → `Create` with snapshot。
**一次决策，无试错，无重放，无请求体缓冲。**

### 1.2 同一处埋着一个现成的性能缺陷

`services/gateway/internal/server.go:733`：

```go
resp, err := s.scheduler.Schedule(ctx, &schedulerv1.ScheduleRequest{})   // 空 hint
```

登记表里明明有 `origin_node_id`，gateway 却随机挑一台 —— **跨节点 resume 主动丢弃了 origin 亲和性**，
必然走 OSS 全量拉层（EKS 上实测比同节点复用差 15–30x）。

这不是"重构完才能修"的东西：**阶段 1 一步就能修掉**（§4「阶段 1」），顺带把上面四层补丁全删掉。

---

## 2. 目标形态

```
                     ┌──────────────────────────────────────┐
   客户端 / SDK ────► │ gateway（Go）                        │  ← e2b client-proxy + api 入口
                     │ REST 入口 / 鉴权 / 数据面直连沙箱     │  ← Cube CubeAPI + CubeProxy
                     └───────┬───────────────────┬──────────┘
                控制面 gRPC  │                   │ 数据面 HTTP（直连，不经 controller）
                             ▼                   │
                     ┌──────────────────────┐    │
                     │ controller（Go）      │    │   ← e2b api / Cube CubeMaster
   ┌── PostgreSQL ──►│ 状态机 · placement    │    │
   │  （唯一持有者） │ reconcile · evictor   │    │
   │      Redis     │ catalog               │    │
   └─────────────────└───────┬──────────────┘    │
                  内部 gRPC  │                   │
                             ▼                   ▼
                     ┌──────────────────────────────────────┐
                     │ node（Rust，DaemonSet）               │  ← e2b orchestrator / Cube Cubelet
                     │ VM 生命周期 · 块设备 · 网络           │
                     │ 本地 store（只记本机 artifacts 事实） │
                     │ 🔴 无 PG · 最多 Redis（订阅态）       │
                     └──────────────────────────────────────┘
```

| 组件 | 现在 | 改造后 |
|---|---|---|
| **gateway** | 无脑把用户请求转发给某台 node，自己兜 recovery/reroute/replay | 控制面请求 → controller gRPC；数据面仍直连 node。**删除全部 recovery/reroute/replay 补丁** |
| **scheduler → controller** | 内存/Redis binding + 只会"选节点" | 唯一持 PG；沙箱全生命周期状态机、placement、中央 reconcile、evictor、catalog |
| **node** | 完整用户级 REST + 直连 PG + 自己续租/抢占/驱逐 | 内部接口，只接受 controller 调用；本地 registry 只记"本机有哪些 artifacts" |

### 2.1 数据分层：这是**第三种**形态，不是"对齐 e2b"

必须说清楚，否则后面每个决策都会拿错参照：

| | live 沙箱 | paused 沙箱 |
|---|---|---|
| **e2b** | Redis catalog（`AllowedTransitions` 里只有 running/pausing/killing/snapshotting，**没有 paused**，`sandboxtypes/states.go:93`）| PG `snapshots` 表（带 `origin_node_id`）|
| **Cube** | Redis（元数据 + 生命周期事件唯一可信源）| MySQL/PG `snapshot_runtime_ref` |
| **本方案** | **PG**（由 `paused_sandboxes` 演进成 `sandboxes`）| **PG**（同一张表）|

选 PG 承载全生命周期是**刻意偏离**，理由只有一条，但足够：

> **e2b 的沙箱是一次性执行环境，丢了不算事故；我们的沙箱是用户工作区，丢了就是事故。**

这条理由在下面反复出现，是本方案与"照抄 e2b"的分界线（见 §3.4、§5.1 的 G1）。

| 层 | 内容 | 谁写 | 丢了会怎样 |
|---|---|---|---|
| **PostgreSQL** | `sandboxes`（全生命周期）、node 注册、（后续）template/snapshot 元数据 | **只有 controller** | 灾难，需备份 |
| **Redis** | 数据面路由 catalog（热路径）、controller 多副本协调锁 | controller | 可从 node roster 重建 |
| **node 本地** | 本机 artifacts、块设备、网络 | node 自己 | 该机沙箱丢，集群一致性不受影响 |

### 2.2 Redis 不是"扩副本才要"，是数据面热路径

**旧版本的方案把 Redis 的定位写反了。** 在 e2b 里 Redis 首先服务于数据面：
`packages/client-proxy/internal/proxy/proxy.go:76` `catalogResolution()` —— **每个用户请求**查一次 catalog
拿 node IP，miss 才回调 api 触发 auto-resume（`:109`）。

我们这边同一条路早就走上了：`services/shared/config/config.go:459` —— gateway 用的 `--query-only`
scheduler **强制要求 `scheduler.redis_addr`**，`services/scheduler/internal/redis_store.go` 的
`RedisBindingStore` 已实现并接线（`cmd/main.go:164`）。

⇒ **Redis 从阶段 1 就在链路里，不是阶段 3 的可选项。** 尤其预览多端口（`{port}-{sandboxID}`）
每个 HTTP 请求都要一次 sandbox→node 解析，把它压到 PG 上是错的。

### 2.3 node 的 Redis 边界（与两家一致）

允许：订阅事件流、P2P peer registry 这类**短 TTL 软状态**。
禁止：任何"丢了就影响集群一致性"的东西。
> e2b orchestrator 的 Redis 可以直接 disabled（`ErrRedisDisabled`）；Cubelet 只**订阅** redis stream。

---

## 3. 🔴 五条不可协商的护栏

> 🔧 **实施订正（原文写"四条"）**：阶段 2 的 Rust 接入面侦察找出了这四条共同的盲区 ——
> 它们**全部**针对"停手 vs 当作空"这一个方向，而 `arbitrate_resume`
> （`src/api/impls/paused_recovery.rs`）是**反方向**的：registry 报错就放行。
> 补成 §3.5。证据见 [`_recon-R4-rust-client.md`](_recon-R4-rust-client.md) §7 风险 1，
> 采用的收窄版验收条件见 [`_impl-plan-control-plane-phase2.md`](_impl-plan-control-plane-phase2.md) §1。

写在阶段之前，因为它们不是某一阶段的任务，是**每一阶段都必须满足的验收条件**。
前三条针对同一个事实：**把本地 PG 调用换成跨进程调用，语义不可能"逐字不变"** ——
它引入了本地调用不存在的第三种答案："我不知道"。

### 3.1 全有或全无：`get_many` 的空结果是删除指令

trait 契约写死了（`paused_registry/mod.rs`）：

> A sandbox missing from the returned map has no row — the same answer `get` gives
> as `None`, and **never "we did not look"**.

消费方严格按这条行事（`src/api/impls/paused_recovery.rs:587` 与 `:676`）：

```rust
None => Superseded::Gone,   // → discard_local_paused_record / discard_superseded_sandbox
```

**"行不存在" 直接等于 "删掉本地 paused artifacts / 拆掉正在跑的沙箱"。**
本地 SQL 要么返回全集要么返回 `Err`，中间态不存在。跨网络之后中间态必然存在。

**验收条件（三条同时满足才算过）**：

1. `GetMany` 全有或全无：controller 侧任何后端错误 → **RPC error**，永不返回部分或空 map
2. controller 未完成 migration / PG 不可达 / 刚启动未 warm → 一律 `UNAVAILABLE`，**不是空结果**
3. node 侧客户端：gRPC 层任何非 `OK`（含 deadline、EOF、部分流）→ 映射成 `PausedRegistryError::Backend`，
   走现有的 `warn!("registry unreachable; stopping reconciliation"); return` 分支

e2b 在完全相同的位置有明写的护栏（`storage/redis/main.go:193`）：

```go
// Pipeline error — skip entirely to avoid mass kills.
```

### 3.2 租约冻结，而不是加速

`src/cfg.rs:435`：`lease_ttl_secs` 默认 **90s**，`reconcile_interval_secs` **30s**（容忍 2 次漏续，
且 TTL 被强制 ≥ 3×interval）。
`deploy/k8s/base/scheduler-deployment.yaml:8`：**`replicas: 1`**。

今天 scheduler 挂掉：只坏路由，登记表照常经 PG 工作 —— **两件事是解耦的**。
把登记表放到 controller 后面之后，controller 停机 **>90s**（镜像拉取 + 启动跑 migration，
CrashLoop 必然超）会同时发生两件事：

- 路由不可用
- **全集群 parked 行同时进入可抢状态** → 下一个 resume 落到别的节点，
  `postgres.rs:552` 的 `state IN ('publishing','local_only') AND LEASE_EXPIRED` 成立，
  claim 成功，**静默回退到上一个已发布快照，丢掉最后一次 pause 的工作**

**验收条件**：

1. controller 启动后进入 **grace 期**：先把本集群所有租约延长 `(观测到的停机时长 + lease_ttl)`，
   再开放 `claim_for_resume` / `reclaim_expired_holdings`。停机时长从 PG 里的 `max(updated_at)`
   与当前时间推算，取不到就按一个完整 TTL 算
2. grace 期内 `claim_for_resume` 对 `publishing`/`local_only` 一律拒绝（`paused` 不受影响 ——
   那条路径不依赖租约）
3. grace 期状态必须可观测（metric + `/healthz` 里区分 ready 与 serving）

> 一句话：**控制面不可用时，租约时钟必须停摆，不能继续跑。**

### 3.3 丢弃熔断

即便 3.1/3.2 都做到，仍然要有最后一道：**单轮 reconcile 丢弃的本地记录数超过阈值
（绝对值或占比，取严）就停手 + 告警，不执行**。

这不是防某个已知 bug，是防"我们还没想到的那个"。e2b 的 `orphanGracePeriod = 1 分钟`
（`storage/redis/main.go:158`，新沙箱一律不判孤儿）是同类东西的弱化版；我们的记录带用户数据，
需要更强的那一版。

### 3.4 resume 的三分法必须显式建模，不能被"照抄 e2b"抹平

`postgres.rs:538-556` 现在是三分法：

```sql
AND snapshot_id IS NOT NULL
AND (state = 'paused'                                            -- 任意节点可接，正常路径
  OR (state IN ('publishing','local_only') AND LEASE_EXPIRED))   -- 仅租约过期后，且是「回退到上个快照」的降级
--   'running' / 'resuming' 永不可抢 —— 租约过期只证明够不到 DB，不证明进程已死
```

**e2b 没有 `local_only` 这个状态，是因为它压根不做这个保证**（`delete_instance.go:104`）：

```go
// Once we start the removal process, we want to make sure it gets removed from the store
defer o.sandboxStore.Remove(context.WithoutCancel(ctx), teamID, sandboxID)
err = o.removeSandboxFromNode(...)   // ← 失败也照删
```

节点不可达 → pause 失败 → 记录照删 → 快照从未生成 → 节点回来后当孤儿杀掉。
在 e2b 这是可接受的泄漏，**在我们这里是用户工作区凭空消失**。而 rustfs 并发 multipart 503
让 `local_only` 在我们这里是常态而非边角。

**验收条件**：controller 侧的 resume 决策必须保留三个分支，且第二个分支
（`publishing`/`local_only` 接管）必须是一个**显式的、可告警的降级事件**
（现在 Rust 侧那条 `claim_outcome = "rewound"` 的 warn 不能丢）。

**实测**：T3 C2 在真集群上抓到了这条 warn 的现场，且 `previous_state = local_only`
（来自 `previous` CTE 而不是 `RETURNING`）—— 说明"是不是一次回退"判对了。
见 [`_verify-T3-phase2.md`](_verify-T3-phase2.md) §4 C2。

### 3.5 🔧（实施新增）registry 不可达时，resume 必须失败，不能放行

前四条是"**别把'我不知道'当成'不存在'**"。这一条是它的镜像：**别把'我不知道'当成'可以上'**。

`arbitrate_resume`（`src/api/impls/paused_recovery.rs`）今天在 registry 报错时返回
`ResumeArbitration::Proceed` —— 放行，不做任何集群检查。而
「`running`/`resuming` 永不可抢」这条不变式，在实现层的最后一道闸就是 `claim_for_resume`
返回 `Conflict`。registry 一不可达，这道闸整个消失；**同一次故障还会让
`discard_if_superseded` 静默不删** —— 两道防线同源同时失效，后果是双活：
两台 VM 从同一快照分叉、各写各的 rootfs 层、gateway 在两者间抖。

**为什么阶段 2 才变严重**：今天这个错误 = node 到集群内 PG 的连接故障（同集群、连接池常驻、
发生率极低）。阶段 2 之后 = node 到 **scheduler** 的 gRPC 故障，而 scheduler `replicas: 1`、
无 PDB、无 `maxSurge` —— 滚动升级、拉镜像、OOM、驱逐**都会**制造窗口。

**验收条件（收窄版，不是一刀切）**：

1. 本地有 paused 记录 **且** 该记录是 `ClusterRegistration::As(_)`（曾登记到集群）
   ⇒ registry 不可达时 resume 返回 **500/503（可重试）**，不再 `Proceed`
2. 其余情况（从未上过集群的沙箱）保持 `Proceed` —— 它们的本地副本就是唯一副本，
   登记表答不出来对它们本就不携带信息
3. 两条路径必须有 metric 分得开

> 取舍：registry 抖动时 resume 会短暂失败。那是**可重试的失败**；双活**不可逆**。

**实测**（[`_verify-T3-phase2.md`](_verify-T3-phase2.md) §4 C3）：同一进程、同一次故障、
同一台节点，只差"有没有登记过" ⇒ 登记过的 500、没登记过的 201，
`agentenv_paused_registry_resume_unarbitrated_total{outcome}` 两个标签值各 1。
"scheduler 没了"与"scheduler 在但它的 PG 没了"两种故障都收口。

---

## 4. 分阶段迁移

五阶段各自独立可上线、可回退。**顺序的核心原则：先拿观测，再拿读，最后动写与裁决。**

### 阶段 0：中央影子对账（只观测，不裁决）　✅ 已实施（`7e6f790` + `c35f5ec`）

**做什么**

1. controller（就是现在的 scheduler）加**只读** PG 连接，读 `paused_sandboxes`
2. 用已有的 Heartbeat roster + 登记表做对账，产出 e2b 口径的差异指标：
   - `orphan`（节点上跑着、登记表不认）
   - `ghost`（登记表说 running、节点 roster 里没有）
   - `holder_conflict`（两个节点同时报同一个 sandbox）
   - `lease_expiring`（租约将过期的 live 行数）
   - `answered` 与 `sync_ok` **分开计**（照抄 `nodemanager/sync.go` 里刻意分开的两个变量）
3. 一个只读 API 暴露登记表（Agent-Console 不再需要 PG 直连）

> 🔧 **实施订正：上面这四个口径，两个算不准、一个混了两件事、一个本阶段根本不可得。**
> 证据与逐条论证见 [`_impl-plan-control-plane-phase01.md`](_impl-plan-control-plane-phase01.md) §1.1，
> 实现见 [`_impl-D3-harden.md`](_impl-D3-harden.md)，集群取样见 [`_verify-T1-results.md`](_verify-T1-results.md) §2。
>
> | 原口径 | 为什么不成立 | 改成了什么 |
> |---|---|---|
> | `orphan` | `mark_running` 的契约是 **"Never creates a row"**，所以"本节点创建、从没 pause 过"的沙箱**本来就没有行**，稳态下还是多数。`roster ∖ registry` 是 orphan 的**超集**，健康集群恒非零 | **`untracked`**（改名 + 文档写死"这不是 orphan"） |
> | `holder_conflict` | 跨节点接管的过渡态里 origin 仍持 paused 记录、claimer 已经有活的，两边 roster 都报同一个 id ⇒ **健康集群也非零** | 拆出 **`stale_copy`**（能用登记表归到单一权威持有者的那一侧）；`holder_conflict` 只留给登记表也说不清的 |
> | `lease_expiring` | 混了两件事：`publishing`/`local_only` 租约将过期 = 会丢最后一次 pause 的工作（该告警）；`running`/`resuming` 租约过期 = **本身零后果** | 拆成三个：**`parked_lease_expiring`**（告警）/ **`live_lease_lapsed`**（信息）/ **`reclaimable_now`**（两条件同时成立，最该盯） |
> | `sync_ok` | heartbeat 是 node→scheduler **单向推**，没有"controller 问、node 答"这一步，本阶段**不可得** | 只上报 **`roster_stale`**（≈ e2b 的 `answered`）。`sync_ok` 要等阶段 3 的中央 poll |
>
> **另外补了方案没提、但更该有的四个**：
> - **`invalid_rows`** = `state='paused' AND snapshot_id IS NULL`。这类行会让 Rust 侧 `get_many`
>   **整批报错**，即一条坏行静默冻结一台机器的全部对账。**应恒为 0**。
>   ⚠️ 它至今**没有分辨力**（从未造出非零样本，见 [`_verify-T2-final.md`](_verify-T2-final.md) N3）
> - **`stranded_rows` / `rows_without_roster`** —— 从登记表那一侧按 `Holder()` 算，
>   节点整台从 discovery 消失时它**变大**而不是随序列一起被删掉
>   （审查 [`_review-V1-phase0.md`](_review-V1-phase0.md) P0-1 找出的：原实现会让最该响的场景静默 resolve）
> - **`registry_enabled`** + `read_failures_total` + `last_success_timestamp_seconds` 三件套 ——
>   让「关了」「坏了」「一切正常且真的是 0」在监控上分得开。
>   `LEASE_EXPIRED` 逐字对齐 Rust 的 `COALESCE(lease_expires_at, updated_at) < now()`，
>   时间基准统一取**数据库的 `now()`**（随 List 一起返回），不用进程时钟

**为什么必须是第一步**：阶段 2/3 的全部收益现在都是**估计值**。
没有这组数据，"中央裁决能消灭多少不一致"是论证，不是事实。
e2b 把 `indexHealed` 当首要告警信号（"healthy steady state is zero"）就是这个东西。

**兑现**：G10（登记表有 API 可查）+ **把后续阶段的收益从估计值变成实测值**
**风险**：零。不写 PG、不改语义、不在任何决策路径上
**回退**：关掉

### 阶段 1：读路径上收（干掉 §1.1 的全部补丁）　✅ 已实施（`7e6f790` + `c35f5ec`）

**做什么**

1. controller 的 `LookupNode` 在 binding 未命中时**回落读登记表**（复用阶段 0 已建好的只读连接），
   把答案分成四类返回，而不是一个裸的 node id：

   | 登记表 state | controller 答什么 | gateway 怎么走 |
   |---|---|---|
   | `paused`（快照已发布）| placement 结果，**origin 作软偏好**；origin 不可接单就换一台 | 一次决策，直接转发 |
   | `publishing` / `local_only` | **硬钉 origin node**（快照没进共享存储，别处起不来）| 直接转发到 origin；能不能接管仍由该节点的 `claim_for_resume` 判 |
   | `running` / `resuming` | 持有者 node | 直接转发 |
   | 无行 | `NOT_FOUND` | 404 |

   🔴 **`paused` 用软偏好、`local_only` 用硬钉，这两条不能合并** —— 前者合并会牺牲层复用，
   后者合并会把请求送到一台根本没有 artifacts 的机器上。

2. gateway **删除** `scheduleRecoveryNode` / `captureReplayBody` / `rerouteToScheduledNode` 三段
3. `Schedule` 增加 origin 亲和 hint，修掉 §1.2 那个丢亲和性的缺陷

**🔴 404 的前提条件**（否则这一步会把"我不知道"洗成"不存在"，正是要修的那个 bug 的镜像）：

- 只有 controller **明确答出"无行"且登记表是 cluster-backed** 时才 404
- controller 不可用 / 未完成 warm-up ⇒ 按 §3.1 返回 `UNAVAILABLE`，gateway 转 **503 而不是 404**
- 登记表里没有行 ≠ 沙箱不存在：**从未 pause 过的 running 沙箱本来就没有行**
  （`mark_running` 的契约是 "Never creates a row"）。这类沙箱只可能在 binding 命中时被找到，
  所以 binding TTL（30s）内的心跳抖动会把它推进 404 分支 ⇒ **回落读必须同时查一次
  `ListObservedNodes` 的 roster，roster 里有就按 roster 走**

  > 🔧 **实施订正：写这句话的时候没有这个数据源。** `ObservedNode` **没有 per-sandbox 列表**；
  > heartbeat 里的 `sandbox_ids` 唯一去处是 `ReconcileNode`，写进 BindingStore 后即丢弃 ——
  > 也就是说"roster"当时 == BindingStore 本身，查它救不了 BindingStore 的 miss。
  > ⇒ **阶段 0 顺带把 heartbeat roster 留存进 `observedNodeRecord`**（`sandboxIDs` + `lastSeen`
  > + 反向索引 `NodesHolding`），阶段 1 直接复用。证据见
  > [`_impl-plan-control-plane-phase01.md`](_impl-plan-control-plane-phase01.md) §1.2。
  >
  > 侦察同时找出另外两处：**① `--query-only` 副本会把阶段 1 整个绕过去**
  > （gateway 的 `LookupNode` 走 `queryOnlyScheduler`，是另一条 gRPC 连接，
  > registry reader 必须同样装到 `QueryOnlyService` 上，否则新逻辑一次都不生效）；
  > **② 删掉 reroute 后 Rust 侧的 503 会直通客户端** ⇒ 裁决为 controller 侧先判 origin 可调度性，
  > 答一个专门分类让 gateway 一次答完 503（同文 §1.3 / §1.4）。

**兑现**：§1.1 四层补丁全删 + 跨节点 resume 不再无谓走 OSS（同节点复用层）
**风险**：低。**写路径一行不动**，node 侧代码零改动
**回退**：`LookupNode` 关掉回落分支，gateway 回滚到旧路径

> 这一步单独就值得做，即便后面三个阶段全部不做。

### 阶段 2：PG 写权上收　✅ 已实施（`4a5e3ef` / `339d1f2` / `40a4526` / `224b70d`）

**做什么**

1. `services/api/proto/scheduler.proto` 新增 `PausedRegistryService`
2. node 侧新增 `PausedRegistryBackendKind::Central`，走 gRPC，复用已有的 `[cluster].scheduler_endpoint`
3. controller 侧把 `postgres.rs`（942 行 Rust，其中 SQL 与状态判定约 700 行）翻成 Go，
   **generation CAS、lease 判定、三分法的边界条件逐条对齐**
4. schema 从"节点自建"改为 controller 启动时 migration
5. **§3.1 / §3.2 / §3.3 三条护栏在本阶段落地，是验收条件不是 nice-to-have**

**🔴 接口按目标形状设计，不做 13 方法 1:1**

旧版本方案要把 `PausedSandboxRegistry` 的 13 个方法 1:1 映射成 RPC。不这么做，两个理由：

- **它把"节点是决策者"从实现细节升格成跨进程网络契约**，而阶段 3 第一件事就是删掉这个契约。
  先固化再推翻，正是要避免的那种补丁堆叠
- 逐字翻译出来的 700 行 Go，阶段 3 要按中央语义重写一遍 —— 这份翻译的全部寿命只有观察期

**做法**：RPC 面按**阶段 3 的目标语义**定义，阶段 2 只是先用"节点仍是发起方"的方式实现它。
具体地，把 13 个方法收敛成 5 个：

| RPC | 覆盖旧方法 | 阶段 3 是否保留 |
|---|---|---|
| `GetSandboxes`（批量读，全有或全无）| `get` / `get_many` | ✅ 保留 |
| `TransitionSandbox`（带 `expect_generation` 的状态转换）| `begin_pause` / `complete_pause` / `mark_local_only` / `mark_running` / `release_claim` / `remove` | ✅ 保留（改由 controller 主动发起）|
| `AcquireSandbox`（resume 授权，三分法在服务端判）| `claim_for_resume` | ⚠️ 阶段 3 改由 controller 内部调用，RPC 面消失 |
| `RenewNodeLease`（批量续租）| `renew_lease` | ❌ 阶段 3 被中央 poll 取代 |
| `ReleaseNodeHoldings`（进程启动时释放前任）| `release_node_holdings` | ⚠️ 见「阶段 3-B」的证据等级讨论 |

`reclaim_expired_holdings` **不进 RPC 面** —— 它从阶段 2 起就由 controller 自己按定时器跑
（它本来就是"集群兜底"，不该由节点发起）。

> 🔧 **实施订正：这 5 个 RPC 已经落地，服务名是 `PausedRegistry`（不是 `PausedRegistryService`），
> 定义在 `services/api/proto/scheduler.proto`。** 契约细节与取舍见
> [`_impl-plan-control-plane-phase2.md`](_impl-plan-control-plane-phase2.md) §3.1–§3.3，
> 实现见 [`_impl-D6-scheduler.md`](_impl-D6-scheduler.md)（Go）/ [`_impl-D8-node-rust.md`](_impl-D8-node-rust.md)（Rust）。
> 方案没写、但实施时**必须**这么定的几条：
>
> - **`GetSandboxes` 不带 metadata**。`entry.metadata` 在 Rust 侧只有一个消费方（跨节点重建的
>   `restore_request`，其 entry 来自 `claim_for_resume`），`get_many` 的两个消费方完全不碰它。
>   ⇒ metadata 只出现在 `TransitionSandbox(begin_pause)` 与 `AcquireSandbox` 两处，
>   顺带消掉 tonic 4 MiB 解码上限风险，并把"字节级往返"的验证面缩到两个点
> - **metadata 线上表示是 `bytes metadata_json`，不是 `google.protobuf.Struct`**。
>   Struct 会把整数塌成 double、重排键、归一化，而 `SandboxMetadata` 没有 `deny_unknown_fields`，
>   **丢字段时不报错**，且只在下一次 resume 才暴露。Go 侧全程 `json.RawMessage` 直接 bind 成 JSONB
> - **`state` 用字符串不用 enum** —— 表的 CHECK 约束是真相源，本 build 不认识的值必须能原样传到
>   运维眼前，不能塌成 `UNSPECIFIED`
> - **时间用 `unix_micros`**：PG `TIMESTAMPTZ` 微秒 vs `chrono` 纳秒，微秒是两侧的公约数
> - **每个请求都带 `cluster_id` 与 `node_id`** —— Rust 侧的 cluster 作用域写在每条 SQL 的 `WHERE` 里，
>   不是连接级的，不许降级成隐式
> - **`AcquireSandboxResponse` 显式建模四个变体**（`claimed` / `not_found` / `not_ready` / `conflict`），
>   `claimed` 带的 `previous_state` 来自 `previous` CTE **而不是 `RETURNING` 的行** ——
>   从 `RETURNING` 读会把每次普通 resume 都报成租约接管（那个 bug 潜伏了数月）
> - 🔴 **`expect_generation` 是 3 个转换要求，不是 4 个**：`postgres.rs` 里带
>   `AND generation = $2` 的只有 `complete_pause` / `mark_local_only` / `release_claim`
>   （HEAD `224b70d` 上分别在 `:416` / `:447` / `:771`）。`claim_for_resume` 与 `mark_running`
>   的谓词只在 state 与 holder 上。见 [`_impl-D6-scheduler.md`](_impl-D6-scheduler.md) §5.1
>
> **一处刻意的语义收紧**：`begin_pause` 的 `paused_at` / `updated_at` 从**节点进程时钟**改成 DB `now()`。
> 原因是 §3.2 的 grace 期要靠 `max(updated_at)` 推算停机时长，留着节点时钟会让它在时钟漂移时
> **算错且方向不确定**。

**兑现**：G6（schema 有 owner）、G7（PG 凭据不再下沉到 KVM 节点）、G8（连接数恒定）
+ controller 重启不丢 registry
**不兑现**：G1/G2/G4 —— 语义还是节点视角，只是换了条路访问 PG
**回退**：`backend` 配置切回 `postgres`。Rust 侧 `PostgresPausedSandboxRegistry` **保留一个观察期**再删
**切换窗口**：所有 node 同时切（混跑期两种 backend 写同一张表，generation CAS 仍能仲裁，
但租约 TTL 参数来源不同，且 `ensure_schema` 的 advisory lock 会与 controller migration 抢）
⇒ 建议在无活沙箱窗口切

> 🔧 **实测（[`_verify-T3-phase2.md`](_verify-T3-phase2.md)，pve-sg dev 双节点）**：
>
> - **G7/G8 是用否定证据兑现的**：切换前两台 node 各持 2 条 PG 连接，切换后节点 IP
>   **从 `pg_stat_activity` 完全消失**，而**同一时刻登记表那行仍在被续租**。
>   `local` 后端什么都不写 ⇒ 这条同时排除了"静默回落 local"这个最难分辨的失败模式（§3 B3）
> - **schema 交接硬门禁**在真集群上带对照探针验过：先证明 Go 的 `SchemaDDL` 与 Rust 的
>   `SCHEMA_DDL` **逐字节相同**，再证明 node 真的每次启动重跑 DDL（删索引 → 重启 → 索引回来）（§2 A2）
> - **正常链路在三种配置下各跑一遍全绿**（postgres → central → postgres），
>   `generation` 序列 1→4 逐格一致（begin_pause=1 / complete_pause=2 / claim=3 / mark_running=4）
> - **跨节点 resume 成功**，且它是 metadata 字节级往返的唯一真实取样（§3 B5）
> - **切换窗口 ≈ 5s**（两节点并行删 Pod）；**回退 13 秒、零数据损失**，
>   且回退路径不依赖任何新代码 —— 只是把 env 改回去（§5 D1/D2）
> - `backend` 是 `OnceLock`，**没有热加载**：切换与回退都必须重启 node

### 阶段 3：语义上收（真正的重构）

> 🔧 **实施订正（2026-08-19）：两条前提变了，阶段 3 的做法比原文激进。**
>
> **前提一：AgentENV 尚未上生产，不需要向后兼容。**
>
> | 原文受兼容约束的做法 | 修订后 |
> |---|---|
> | ExecutionID 作附加**可选**字段 | **内部必填**。可选的 fencing token 是 fencing 剧场 —— "字段缺失"必然要留一条 fail-open 分支，而那条分支就是全部攻击面 |
> | 登记表 schema 走兼容迁移 | **直接重画**（dev 存量行可丢）。`execution_id` NOT NULL；并定清**身份轴与版本轴的分工** —— `generation` 今天只服务 3 处 CAS（`complete_pause` / `mark_local_only` / `release_claim`），要么被 execution 吸收成一条轴，要么明确拆成「execution = 化身身份 / version = 行版本」。**现在是唯一能改的窗口**；且 agent-platform 侧对 `generation` **零消费**（grep `apps/agent-platform/internal/sandbox/aenv/*.go` 无命中），怎么改都不外溢 |
> | 阶段 2 的 5 个 RPC 形状在其上演进 | **直接删掉重写成中央语义**。本文下方已写明这份翻译"全部寿命只有观察期" —— 现在寿命可以归零：node→controller 收敛成「上报事实」，controller→node 收敛成「下发命令」 |
>
> **前提二：范围只做 AgentENV**（§5.3 订正）⇒ ExecutionID 内部化，跨仓契约退出关键路径。
>
> **建议与「暂停必然落 OSS」(v3) 并批**（主仓 `docs/proposals/2026-08-19-aenv-pause-publish-durability.md`，
> 不在本 submodule 内，勿加相对链接）：
> 那份方案把 `publishing` 从瞬时态改成长驻重试态、`local_only` 只在预算耗尽才写。
> 它落地后，placement 里"硬钉 origin"的分支基本消失，**中央 placement 才真正自由**。
> 两者动的是同一批状态流转，分开做等于把同一处代码改两遍。

| 现有机制（node 视角） | 改造后（中央视角） | 参照 |
|---|---|---|
| `renew_lease` 节点自己续租 | **controller 每 N 秒主动 poll node**，`answered` 与 `sync_ok` 分开判 | e2b `nodemanager/sync.go` |
| 无孤儿回收 | controller `Reconcile(roster, nodeID)` → 中央不认的 → `KillOrphan`（**必须带 grace period + §3.3 熔断**）| e2b `store.go:140` |
| `claim_for_resume` 节点互抢 | controller placement 决策 + 对目标 node 下 `Create with snapshot`，**三分法按 §3.4 显式建模** | e2b 没有 Resume RPC |
| `generation` 仅 aenv 内可见 | 引入 **ExecutionID**（每次启动/恢复一次化身），破坏性操作带 `expect_execution_id` | e2b `RemoveOpts.ExpectExecutionID` |
| 状态散落在 SQL 的 WHERE 条件里 | 显式 `AllowedTransitions` 表 + `TransitionEffect` + 结构化 `KillReason` | e2b `sandboxtypes/states.go` |

**🔴 三处不能照抄，必须自己想清楚**

#### 阶段 3-A：`reclaim_expired_holdings` ≠ e2b evictor（旧版本方案在这里犯了类别错误）

它们是两件事，**都要有，不能合并**：

| | 触发条件 | 效果 | e2b 的对应物 |
|---|---|---|---|
| **evictor** | 沙箱**自己的 deadline** 到了 | 驱逐（kill 或 auto-pause）| `evictor/evict.go` 跑在 `store.ExpiredItems()` 上，与节点状态无关 ✅ |
| **reclaim** | 租约过期 **且** deadline 也过了 | **释放登记表所有权**，让别的节点能接管 | e2b 没有对应物 —— 它靠 `Reconcile` + orphan kill，而 orphan kill 只杀 store **完全不认**的 |

现有注释已经把"两个条件都要"的理由写死了：

> 🔴 The deadline, not the lease, is what makes this safe. A lapsed lease alone
> says only that the holder cannot reach the database.

中央化之后 evictor 变得更强（节点不可达也能执行到期），但 **reclaim 的两条件不能放宽成一条**。

#### 阶段 3-B：`release_node_holdings`：换成 `service_instance_id` 是证据等级**下降**，要显式论证

现有论证（`paused_registry/mod.rs`）：

> a node's ID names the machine, not the process, so a row saying "running on this node"
> that is being read by a process which has just started and holds nothing can only have
> been written by a previous process on this same machine. That process is gone, and its
> sandboxes went with it — **the VMs are its children, in its PID namespace**.

controller 观察到 `service_instance_id` 变了，只证明**有个新进程自称是这个 node id**。
要用这条替代，必须先论证：

- node id 与物理机/VM 一一对应，且**永不复用**（VM 重建复用 ID 时，老机器可能还在分区里跑着）
- controller 收到新 instance_id 与老进程实际死亡之间，没有可观测的重叠窗口

**论证不成立就保留"本机后继进程"这条路** —— 它成本极低（进程启动时一次调用），
而且是这个系统里唯一能**证明**（而非推断）VM 已消失的证据。

#### 阶段 3-C：G1 的诚实版本：中央 poll 换的是推断的**种类**，不是消灭推断

中央 poll 把"够不到 PG"换成"够不到 controller"，**仍然是推断**。
controller ↔ node 网络分区时，node 活得好好的、还在被 gateway 直连路由数据面流量。
e2b 敢直接裁决，是因为它接受泄漏（孤儿等节点回来再杀）+ 沙箱是一次性的。

**真正消灭双活要在数据面 fencing，控制面做不到。** 三个候选，阶段 3 动手前必须选一个：

| 方案 | 机制 | 代价 |
|---|---|---|
| **存储层写锁** | overlaybd / 块设备层持有排他写租约，第二份实例起不来 | 最彻底；要动 ublk/overlaybd 那层 |
| **envd token 绑 execution** | 每次化身换 token，旧 execution 的 envd 请求一律拒 | 中等；杀不掉已在跑的进程，但切断了它的对外影响 |
| **路由层拒旧 execution** | gateway/proxy 按 ExecutionID 路由，旧化身收到的流量归零 | 最便宜；VM 仍在跑（浪费资源），但用户视角单活 |

**倾向**：先做「路由层拒旧 execution」（与 ExecutionID 同一批工作，几乎零额外成本），
把「存储层写锁」列为长期项。**在选定之前，`running`/`resuming` 永不可抢这条不许放宽。**

> 🔧 **实施订正（2026-08-19 源码考古）：三个候选的判断要改，而且主次对调。**
> 证据来自对 e2b（`/home/debian/e2b-infra` @ `6938cbb`）与 CubeSandbox
> （`/home/debian/CubeSandbox-latest` @ `50d9a3e7`，比另一份检出新 660 commits）的逐调用点考古。
> **结论：两家都没有"解决"这个问题 —— 它们各自靠一条我们不具备的业务前提把问题消解掉了。**
>
> **e2b 的四条硬事实**
>
> 1. **存储层零排他**：GCS / S3 / Azure 三个后端 grep `IfGenerationMatch` / `precondition` /
>    `IfNoneMatch` **零命中**，对象全是无条件 Put。唯一的"锁"
>    （`shared/pkg/storage/lock/file_lock.go:47-49`）是**节点本地** `O_EXCL` + 10s TTL 的
>    NFS 读缓存去重锁，注释自认 `The worst that can happen is more than one node will acquire the lock`
>    —— 它保护的是**不可变、内容逐字相同**的缓存 chunk（temp 文件 + rename 原子落位），
>    锁坏了只是重复下载一次。**⇒ 候选 1 在 e2b 零先例。**
> 2. **envd token 不绑 execution**：`api/internal/sandbox/sandbox_envd_secret.go:27-35`
>    token = `HMAC(sandboxID)`，新旧化身完全相同。**⇒ 候选 2 在 e2b 也零先例。**
> 3. **`ExpectExecutionID` 基建完整，但生产调用点为零**：enforcement 刻意放进 Redis Lua
>    （`storage/redis/scripts.go:33-39`：`Add is lockless, so a resume can install a new incarnation
>    between a Go-side comparison and this write`），而全仓只有 `execution_pin_test.go` 在设它，
>    evictor 不带 pin。⚠️ **但这不是"没做完"，是刻意划范围**：`states.go:82-92` 注释写死
>    「陈旧快照决策方（background scan / queued batch）才需要 pin；`Empty means "remove whatever
>    is stored", which is correct for callers acting on a fresh read or on user intent`」，
>    `errors.go:47` 复述为 "caller **opted in**"。引入提交 `c29ee2622`（2026-08-10，带
>    `GitOrigin-RevId` ⇒ 内部 monorepo 导出，**裁决理由只存在于注释**）。
>    ⇒ **pin 是 opt-in 的补充手段，不是 e2b 防双活的主闸。**
> 4. **真正防双活的主闸是别的东西**：resume 入口查到 Running 记录直接 409
>    （`handlers/sandbox_resume.go:96-106`），而**那条记录没有租约、不会自动过期释放**
>    （`UnreachableSince` 全仓零生产消费者）⇒ e2b 在分区期间对 resume 是**彻底 fail-closed 的：
>    等，不接管**。⚠️ 对照我们：reclaim 是「租约过期 + deadline 过期」双条件**自动接管** ——
>    **我们在接管上比 e2b 激进，这正是我们比 e2b 更需要 fencing 的原因。**
>
> **e2b 的写路径 fencing 本体 = 发布权集中**（不是 ExecutionID、更不是存储锁）：
> 节点无自主 pause 权（`orchestrator.proto:56-59`：`the orchestrator itself does not act on it —
> the API evictor does`）+ 每次 pause 造**全新 build UUID**（写目标天然不相交）+ build 翻 `ready`
> **只由 API 在 RPC 成功返回后做**（`pause_instance.go:71-77`），而 resume 只选
> `status_group = 'ready'` 的最新 build。⇒ **分区旧节点就算把快照字节传完，没有中央翻牌，
> 这份数据永远进不了快照链。**
>
> **CubeSandbox**：**根本没有跨节点 resume**（roadmap 未来项，`docs/zh/guide/lifecycle.md:259`
> 逐字「后续版本将支持跨节点恢复」），沙箱身份终身钉死单节点、快照落本地 cubecow reflink 盘
> ⇒ fencing 降维成 cubelet **进程内** per-sandbox 互斥锁（`services/cubebox/update.go:77`）——
> 因为一个沙箱的全部生命周期操作都汇聚到唯一节点，**这把进程锁就是全局锁**。
> 全仓 grep `fencing|fence|epoch` **零命中**。
>
> **⇒ 修订后的三候选判断**
>
> | 候选 | 修订判断 |
> |---|---|
> | 1 存储层写锁 | **两家零先例** —— 连把快照放中央对象存储的 e2b 都没做条件写。维持长期项的结论更有底气 |
> | 2 envd token 绑 execution | **e2b 也没做**。维持「`secure` 沙箱加强项、seed 统一后再补」 |
> | 3 路由层拒旧 execution | 方向被 e2b **半**验证：它把路由缓存刻意删成每请求实时查 catalog（PR #2636 / #2315），买的就是收敛速度 —— 但它只做**收敛**，不做**拒绝** |
>
> **🔴 主次对调（本订正最重要的一条）**：原文把「旧 execution 禁 pause/publish」当作候选 3 的
> *配套措施* —— 考古证明**它才是 fencing 本体**。路由拒绝保护的是分区期间的交互流量（**可恢复**），
> 快照链保护的是用户工作区（**不可逆**）。
> ⇒ 闸门 B 的答案应表述为：**写路径 fencing 为主 + node API 收窄同批（原阶段 4）+ 路由层拒旧 execution 为辅。**

**兑现**：G2（孤儿回收）、G4（并发 resume 天然串行）、G1 的**一半**（见上）；末尾引 Redis 后控制面可无状态多副本

### 阶段 4：API 收窄（边界闭合）

node 的用户级 REST（`/sandboxes` POST/DELETE、`/pause`、`/resume`、`/fork`、`/templates`、`/nodes/{id}`…）
改为**只接受 controller 调用**（mTLS 或内部 token），gateway 的控制面路径不再直接打 node。
数据面（`{port}-{sandboxID}` 反代）保持直连 node 不变。

外部 e2b SDK 兼容面不受影响 —— 外部本来就只经 gateway。

**兑现**：绕过 controller 变得不可能；controller 的"唯一裁判"身份才真正成立

> 🔧 **实施订正（2026-08-19）：阶段 4 并入阶段 3，不再"拖到最后"。**
>
> 理由来自考古：e2b 的 fencing 本体是「发布权集中」，而它成立的前提是**节点没有自主发起路径**
> （`orchestrator.proto:56-59`）。node 的用户级 REST 只要还开着，"旧化身自己发起 pause/publish"
> 这条路径就**在物理上存在**，我们只能靠一道道 `expect_execution_id` 检查去堵；
> 同批收窄则让这条路径**根本不存在** —— fencing 从"检查"降级成"拓扑"，是同一件事的更强形态。
>
> 分两阶段做反而更贵：阶段 3 要为"node 仍可被直接调用"写一整套校验，阶段 4 再把这套校验证明为多余。
> 顺带关掉 [`_impl-D11-pg-removal.md`](_impl-D11-pg-removal.md) §5 的 **L1**
> （节点面鉴权只查 header 存在、不查值）与原 A2/A3。
>
> 🔍 **侦察项（动手前必须确认，否则收窄边界画错）**：agent-platform 今天打的是
> `http://<node-ip>:30800`（主仓 `apps/agent-platform/internal/sandbox/aenv/client.go:91-100`，
> 单一 `baseURL`），**要先确认它落在 gateway 还是 node 自身 API**。
> 这正是 §2.1 教训（"写方案时引用的每一个数据源，动手前都要确认它真的存在"）的适用场景。
>
> ⚠️ **另一个 in-house 消费方：Agent-Console**（主仓 `apps/Agent-Console`）。它**直连 PG**
> 读登记表（`internal/aenv/pausedregistry/reader.go`），而阶段 0 产出的只读 API
> `GET /registry/sandboxes` 本来就是为了让它不再直连。**schema 重画会打掉这条直连路径**
> ⇒ 把 Console 切 API 排进同批发布（它不在本 submodule 内，但属于本轮的下游收尾）。

---

## 5. 收益重估（取代背景文 §4.1 的 G 列表）

### 5.1 只有中央控制面能拿的

| # | 缺陷 | 哪一阶段兑现 | 说明 |
|---|---|---|---|
| **G7** | PG DSN 下发到每台跑用户代码的 KVM 机器 | 阶段 2 | 爆炸半径 N 台 → 1 处。**这是最硬的那条理由** |
| **G8** | PG 连接数随节点数线性增长（每 node 8 条，20 台 = 160 条常驻）| 阶段 2 | 恒定 |
| **G2** | 孤儿沙箱无人回收 | 阶段 3 | 必须带 grace period + §3.3 熔断 |
| **G4** | 并发 resume 互相不知情 | 阶段 3 | 中央决策天然串行 |
| **G1** | 租约过期 ≠ 进程已死 | 阶段 3 **的一半** | 🔴 见「阶段 3-C」：中央 poll 只换推断种类；消灭双活要 fencing |

### 5.2 今天就能拿，不算重构收益

| # | 原表述 | 实情 |
|---|---|---|
| **G5** | "scheduler 绑定在内存，重启丢" | `RedisBindingStore` 已实现并接线（`redis_store.go`，`cmd/main.go:164`），`--query-only` 模式甚至**强制**要 Redis。改配置的事 |
| **G6** | "schema 由节点自建" | 一个 migration job 就能解决，不需要中央控制面。阶段 2 顺带做，别拿它当理由 |
| **G10** | "登记表只能直连 PG 看" | **阶段 0** 就兑现，零风险 |

### 5.3 拿不到 / 需要跨仓契约变更

**G3（404 三来源不可区分）—— 不能靠"照抄 e2b"拿到。**

我核过 `e2b-infra/spec/openapi.yml`：**公开 API 零处暴露 execution id**，它只活在
`shared/pkg/sandbox-catalog/catalog.go:15` 这个内部结构里。e2b 的 SDK 使用者同样只能看状态码。

要让 agent-platform 不再猜，必须：
1. 在 aenv 的外部契约上**增加**一个 execution 身份字段（附加字段，不破坏 e2b SDK 兼容）
2. **改 agent-platform** 消费它

⇒ **"改造只在 AgentENV 内部完成"这个前提，在阶段 3 就不成立了。** 这不是意外，是必须提前
知道并接受的跨仓协同点。

另外：平台侧已经修过一轮（只有 resume 的 404 授权重建），**G3 的剩余价值明显低于原表述**。

> 🔧 **实施订正：阶段 1 又替 G3 消掉了一部分，剩余价值还要再打折。**
> 平台侧记的 404 三个来源（`apps/agent-platform/internal/sandbox/aenv/client.go:58-71`）里，
> 「registry 不可达」现在答 **503 而不是 404**（[`_verify-T1-results.md`](_verify-T1-results.md) V1-1 实测），
> 「scheduler 没有 binding」也被 roster + 登记表回落覆盖掉了大部分。
> ⇒ 那段注释描述的是阶段 1 上线**之前**的行为。**阶段 0/1 上线后应重新取样**，
> 再决定 ExecutionID 里有多少份额是冲着 G3 去的。

**G9（两套真相无人对账）**：阶段 0 就能开始对账，但"平台库 ↔ aenv 库"的对账天然是跨仓的。

> 🔧 **实施订正（2026-08-19，两条裁决叠加）：G3 的跨仓部分退出阶段 3 的关键路径。**
>
> **① 范围裁决（用户 2026-08-19）**：本轮设计**只做 AgentENV**，agent-platform 后期再改。
> **② ExecutionID 是内部 fencing token，不是跨仓契约。** 这不是妥协 —— 这恰是 e2b 的原始形态：
> 上面已核过"公开 API 零处暴露 execution id"，外部调用方只说"pause 沙箱 X"，
> 由 **API 层自己**把 X 解析成当前化身，再把下游每一步钉死在这次化身上。
>
> 改造后的边界：
>
> ```text
> 外部（gateway，e2b 兼容面）   不要求传 execution ⇒ agent-platform 一行不用改
>          ↓ controller 在每个破坏性操作入口解析 sandbox_id → 当前 execution
> 内部（controller → node → 登记表 → 发布）   全链路钉死该 execution
>          ↓ node 拒绝任何 execution ≠ 自己活着的化身的命令
> ```
>
> **能力边界（诚实说明，别高估单侧能拿到什么）**
>
> | 威胁 | AgentENV 单侧 |
> |---|---|
> | 分区旧节点发布快照 → 第二条快照分支（**不可逆，用户工作区**）| ✅ 完全能解决 |
> | 旧化身继续收流量 | ✅ 完全能解决 |
> | 操作执行期间被 resume 插队（内部竞态）| ✅ —— 正是 e2b `ExpectExecutionID` 注释描述的原场景 |
> | 平台重试队列里的陈旧 delete 落到新化身（= **G3 本体**）| ❌ 需平台传 execution，**后期做** |
>
> 最后一条推迟**有先例**：e2b 的 `ExpectExecutionID` 生产调用点为零，它自己也还没做。
> 而在外部**只读**暴露 execution 字段（`GET /sandboxes/{id}` 与 resume 响应）是 additive、
> 无人依赖、成本近乎为零 ⇒ **阶段 3 顺手加上**，平台哪天要用就有。
>
> ⇒ **闸门 B 的前置二（跨仓 ExecutionID 契约）从阻塞项里移除。闸门 B 只剩一个决定：选定 fencing 方案。**

### 5.4 改造**不能**解决的（别抱幻想）

| 问题 | 为什么改控制面没用 |
|---|---|
| rustfs / S3 并发 multipart 503（≥3 并发必挂）| 存储服务端问题 |
| 层 >64MiB 的暂停降级 `local_only` | 快照上传耐久性；e2b 在同样场景下是**直接丢数据**，我们的降级更保守 |
| 沙箱 MTU 黑洞、npm audit 慢、预览冷启动 | 纯数据面 |
| 跨节点恢复仍依赖快照进共享存储 | 中央只能决定"去哪台"，搬不动字节 |

### 5.5 一个方案原本漏掉的必要输入：层局部性

中央 placement 一旦不知道"哪台机器有哪些层"，跨节点 resume 就要从 OSS 全量拉。
Cube 为此有 `artifact_node_placement` / `template_replica` 两张表；
我们其实**已经有原料** —— `services/api/proto/scheduler.proto:16-18` 的
`RecordP2pArtifact` / `ForgetP2pArtifact` / `LookupP2pArtifact`，但 `Schedule` 不消费它。

⇒ 阶段 3 的 placement 必须把 P2P artifact registry 接进打分，否则中央化会**换来一次性能倒退**。

---

## 6. 成本与风险

| 项 | 评估 |
|---|---|
| **上游追平成本** | 阶段 0/1 侵入面几乎为零（只加读路径）。**阶段 2 起动主干**，与 `suixinio/AgentENV` fork 上游的分叉显著加深 ⇒ 阶段 2 是"是否长期自维护 fork"的决策闸门。🔧 **已裁决（2026-08-19）：长期自维护 fork，手动从上游拉取检查合并** ⇒ 本行不再是约束 |
| **SQL 翻译风险** | 942 行 Rust → Go，generation CAS / lease / 三分法边界必须逐条对齐。**先把 Rust 侧现有单测移植成 Go 契约测试**，作为阶段 2/3 的安全绳 —— 这件事在阶段 1 的观察期内就可以并行做 |
| **单点** | 今天 gateway 与 scheduler 都已是 `replicas: 1`。但阶段 2 之后 controller 停机会**同时**打掉路由与租约时钟（§3.2）—— 这是新出现的相关性故障，靠 §3.2 的 grace 期兜住，阶段 3 末尾引 Redis 后可多副本 |
| **切换窗口** | 阶段 2 切 backend 需所有 node 同时切，建议在无活沙箱窗口 |
| **跨仓协同** | ~~阶段 3 的 ExecutionID 需要 agent-platform 配合（§5.3）~~ 🔧 **已消除**：ExecutionID 内部化 + 范围只做 AgentENV ⇒ 阶段 3 零跨仓依赖（§5.3 订正）。仅剩下游收尾：Agent-Console 从 PG 直连切只读 API |

---

## 7. 落子顺序与闸门

1. **阶段 0**（观测）—— 立刻可做，零风险，产出后续所有决策的依据
2. **阶段 1**（读路径）—— 便宜、可回退，吃掉 §1.1 全部痛点 + 修 §1.2 的性能缺陷
3. 阶段 1 观察期内并行：**把 Rust 侧 paused_registry 的现有单测移植成 Go 契约测试**
4. 🚦 **闸门 A** ✅ **已通过（2026-08-19，用户裁决）**：**长期自维护 fork，手动从上游拉取检查合并。**
   ⇒ 上游追平成本不再是约束，允许改动 Rust 主干。
   （原文："决定 fork 长期维护策略 —— 业务决策，不是技术决策。不过闸不进阶段 2"）
5. **阶段 2**（写路径）—— §3.1/§3.2/§3.3 **以及实施中新增的 §3.5** 是验收条件
6. 🚦 **闸门 B** ✅ **已通过（2026-08-19，用户裁决）**：
   **写路径 fencing 为主 + node API 收窄同批 + 路由层拒旧 execution 为辅。**
   前置二（跨仓 ExecutionID 契约）随 ExecutionID 内部化 + 范围裁决**已移出阻塞项**（§5.3 订正）。
   ⇒ 决策材料与依据见 [`2026-08-19-control-plane-refactor-outcome.md`](2026-08-19-control-plane-refactor-outcome.md) §3 / §3.4
   ⇒ 🔴 **闸门放行的是"开工"，不是"提前放宽不变式"**：在 A3 落地并验证前，
   `running`/`resuming` 永不可抢这条不许放宽
7. **阶段 3**（语义 + 裁决权）+ **原阶段 4**（API 收窄）—— 🔧 **同批做**，理由见阶段 4 订正
8. ~~阶段 4~~ —— 已并入第 7 步

---

## 8. 🔧 阶段 3 开发清单（2026-08-19 订正后的完整形态）

> 本节是上面各订正块的汇总视图，供任务书直接展开。**批次 A 是闸门 B 的解锁工程本身。**

### 批次 A：身份轴 + 边界闭合

| # | 事 | 关键约束 |
|---|---|---|
| A1 | **ExecutionID 一等公民（内部）** | start / resume 换代，快照 / checkpoint **不**换代（抄 e2b 边界，`checkpoint_instance.go:22-23`）。把现有 `SandboxInstanceId`（今天只私有流向 custom extension hook，`src/sandbox/custom_extension/client.rs`）升格进 orchestrator 元数据、登记表、node Runtime |
| A2 | **登记表 schema 重画** | `execution_id` NOT NULL；定清身份轴与版本轴分工（见阶段 3 前提块）。dev 存量行可丢 |
| A3 | **写路径 fencing（主角）** | 所有破坏性转换内部必带 execution，旧化身 pause / publish / remove 一律拒。🔴 **校验必须在 controller 的 SQL 事务内原子完成** —— 抄 e2b 把 enforcement 放进 Lua 的教训：Go 侧"先查后写"之间正是 resume 插队的窗口。同时关掉 §4.2 的 **S1**（全接口唯一无条件破坏性写）|
| A4 | **node API 收窄**（原阶段 4）| node 用户级 REST 只接受 controller 调用；数据面 `{port}-{sandboxID}` 反代保持直连。先做侦察项（平台的 `:30800` 落在哪一面）|
| A5 | **路由层拒旧 execution（配角）** | `LookupNode` 答案携带 execution 身份，gateway / proxy 对旧化身流量归零 |
| A6 | **外部只读暴露 execution** | additive、无人依赖，为平台后期留口 |

### 批次 B：语义上收本体（**批次 A 落地后**才许放宽"永不可抢"）

| # | 事 | 关键约束 |
|---|---|---|
| B1 | 中央 poll 取代节点自续租 | `answered` 与 `sync_ok` 分开判；`RenewNodeLease` RPC 退役 |
| B2 | evictor 与 reclaim **分离** | 见「阶段 3-A」：两个都要、不能合并；reclaim 双条件不许放宽成单条件 |
| B3 | 孤儿回收 | `Reconcile(roster) → KillOrphan` + grace period + §3.3 熔断。🔴 **按 execution 身份判孤儿，不按 sandboxID 存在性判** —— e2b 的现成缺口：`storage/redis/main.go:205-217` 只看 `raw != nil`，同 ID 异节点重建后旧化身查库会命中**新化身的记录**，不判 orphan、永远杀不掉 |
| B4 | 中央 placement | resume 改「controller 选节点 + 下发 Create with snapshot」；`AcquireSandbox` 变内部调用；三分法（§3.4）在中央显式建模 |
| B5 | 显式状态机 | `AllowedTransitions` + `TransitionEffect` + 结构化 `KillReason` |
| B6 | RPC 面重写 | node→controller = 上报事实；controller→node = 下发命令。阶段 2 的 5 RPC 形状直接丢 |
| B7 | `release_node_holdings` **保留** | 见「阶段 3-B」：除非能论证 node id 永不复用且无重叠窗口 —— 它是系统里唯一能**证明**而非推断 VM 已死的证据 |

### 批次 C：随行收口

- T3 **F6** 永久孤儿行（`sandbox_expires_at` 为 NULL 永不匹配 reclaim）—— 随中央 poll 一并消掉
- **S 系列**接口语义缺口（随 RPC 重写消化）：**S4** 响应带实际覆盖的 id 集合（护栏 §3.1 目前唯一无机械保证的一环，而缺行的下游反应是**删用户工作区**）、**S2** release 0 行静默成功要可计数、**S5** 读路径带 DB 时钟、**S7** `MarkRunning` 的 bool 拆成两个事实、**S3** 节点 `reconcile_interval` 校验归属
- **D8**：`Code::Aborted ⇒ GenerationConflict`（一行，Go 侧已备好）

### 🔴 写进任务书的两条反面教材

1. **绝不"跳过 RPC 直删元数据"** —— Cube 的 `sandbox_remove.go:180-204`：节点不在内存缓存里就跳过
   Destroy RPC、直接抹掉 Redis 元数据 ⇒ 中央认为已删、分区节点上 VM 还在跑还在写盘，
   且 cubelet 无本地 TTL 自杀，孤儿跑到人工干预为止。这正是我们护栏哲学（"我不知道"≠"不存在"）要防的。
2. **分区期间 fail-closed 等待，不抄 e2b 的"清库不等 ack"**（`delete_instance.go:104-105`
   无条件 `defer Remove`）—— e2b 敢这么做是因为沙箱可弃，我们的是用户工作区。

---

## 附：关键源码锚点

**AgentENV**
- `src/orchestrator/paused_registry/mod.rs`（trait 契约与三处 🔴 论证）
- `src/orchestrator/paused_registry/postgres.rs`：`claim_for_resume:491`（三分法 SQL `:538-556`）、
  `renew_lease:646`、`reclaim_expired_holdings:698`、`release_node_holdings:854`
- `src/api/impls/paused_recovery.rs:587` / `:676`（`get_many` 的两个消费方 —— 空结果即删除）
- `src/cfg.rs:395-460`（lease 90s / reconcile 30s 及其论证）
- `services/gateway/internal/server.go:218-320`、`scheduleRecoveryNode:731`
- `services/scheduler/internal/{redis_store.go,store.go}`、`cmd/main.go:164`
- `services/shared/config/config.go:459`（query-only 强制 Redis）
- `services/api/proto/scheduler.proto:16-18`（P2P artifact registry）
- `deploy/k8s/base/scheduler-deployment.yaml:8`（replicas: 1）

**e2b**
- `packages/orchestrator/orchestrator.proto:235-241`（6 个 RPC，无 Resume）
- `packages/api/internal/sandbox/store.go:140`（Reconcile）、`sandboxtypes/states.go:93`（AllowedTransitions）
- `packages/api/internal/sandbox/storage/redis/main.go:142`（Reconcile 实现）、`:158`（grace）、`:193`（mass-kill 护栏）
- `packages/api/internal/orchestrator/nodemanager/sync.go`（answered / syncRetrySuccess 分开）
- `packages/api/internal/orchestrator/evictor/evict.go`（跑在 `ExpiredItems` 上）
- `packages/api/internal/orchestrator/create_instance.go:331-341`（origin 软偏好）、`:452-506`（remap）
- `packages/api/internal/orchestrator/delete_instance.go:104`（无条件 `defer Remove`）
- `packages/client-proxy/internal/proxy/proxy.go:76`（数据面每请求查 catalog）、`:109`（miss → resume）
- `spec/openapi.yml`（**零处** execution id）

**CubeSandbox**
- `docs/zh/architecture/overview.md`、`CubeMaster/pkg/base/db/models/snapshot_runtime_ref.go`、`Cubelet/go.mod`（无 CubeDB）
