# 阶段四 Stage D/E 尽调：`services/scheduler/` 搬完 A/B/C 之后还剩什么

调研日期 2026-08-25，对照 `dev` @ `1022b3a`。只读调研，未改动任何代码。

## 0. 前置纠偏：A/B/C 今天并未真的"搬完"

任务前提是"在 Stage A/B/C 搬完之后"盘点剩余部分，但**实测代码现状是 A/B/C 都还没搬**：
`services/scheduler/internal/catalog/`、`internal/registry/` 与它们各自的 gRPC 处理器
（`catalog_service.go`、`registry_service.go`）仍然是运行中的 Go 服务；Rust 侧
（`src/snapshot/repository/backends/central/`、`src/orchestrator/paused_registry/central.rs`）
是这两个服务的**gRPC 客户端**，不是替代实现——DSN、连接池、schema 仍然只在 scheduler 进程里。
这与 `phase4-scheduler-decommission` 记忆条目一致：用户裁决的是"最终都搬进 api"，但落子顺序
是 Go→Rust 的**逐块移植**（含 SQL 谓词原样带走），不是先在 Redis 里重新发明一遍再删代码。

本文按任务要求，**假设 A/B/C 按此裁决完成移植**（node_registry 移植进 api 的节点清册、
catalog 移植进 api 的 PG catalog、paused registry 移植进 api 的 PG registry），来回答
"届时 `services/scheduler/` 还剩什么"。这是一次**面向目标态的盘点**，不是"今天已经删了什么"。

## 1. 结论摘要

`services/scheduler/` 非测试 Go 代码合计 **17,188 行**（`1022b3a`，比记忆条目记录的
16,991 多 197 行——期间的 paused-registry 修复行为提交仍在增长这份代码）。三块已裁决要走的
部分：

| 归属 | 行数 | 备注 |
|---|---|---|
| Stage A（节点清册） | 1,481（官方口径）+ 300（`cpu_template.go`，官方口径漏计）+ 25（`types.go`，共享类型） = **1,806** | |
| Stage B（catalog） | 3,613（`internal/catalog/`）+ 975（`catalog_service.go`，官方口径漏计的 RPC 处理器）= **4,588** | |
| Stage C（paused registry） | 4,191（`internal/registry/`，比记忆条目的 4,044 多 147 行）+ 978（`registry_service.go`）+ 648（`reconcile.go`，心跳驱动的租约续期，语义上属于 registry）= **5,817** | |
| **Stage D（本文范围：剩余）** | **4,977** | `cmd/main.go` `lookup.go` `metrics.go` `redis_store.go` `service.go` `store.go` `sweep.go` |

（1,806+4,588+5,817+4,977 = 17,188，与实测总量吻合，逐文件表见 §2。）

**Stage D 的 4,977 行里，能直接删的约 827 行（17%）**——`cmd/main.go` 的 Go 进程自举/参数解析/
gRPC 服务注册，在 Rust 里没有逐行对应物，会被已经存在的 `assemble_api`（`src/bin/aenv-api.rs`）
吸收，而不是被"翻译"过去。**其余约 4,150 行（83%）必须移植**，且移植后大多数不是原样的
"Go 翻译成 Rust"，而是**因为进程边界消失而被真实简化**（gRPC 往返变成进程内函数调用，见 §3、§4）
——但状态机本身（哪个来源优先、什么时候答 Unavailable 而不是 NotFound、TTL 语义）必须原样保留，
这部分逻辑不能删，只能搬。

三个最大的风险，见 §11。

## 2. 全量盘点表（骨架）

`services/scheduler/` 除 `Makefile` 外只有 Go 代码（`cmd/`、`internal/`）与
`internal/catalog/migrations/*.sql`（3 个文件，随 Stage B 迁移，不单独计行）。下表为
**非测试 `.go` 文件**，逐文件给出行数、归属 Stage、去留裁决。

| 文件 | 非测试行数 | 归属 | 去留裁决 |
|---|---:|---|---|
| `cmd/main.go` | 827 | **Stage D** | 删——Go 进程自举/flag 解析/gRPC 服务装配，Rust 侧由既有的 `assemble_api` 承接，不是逐行翻译对象 |
| `internal/catalog/migrate.go` | 703 | Stage B | 随 B 迁移（不在本文范围） |
| `internal/catalog/pin.go` | 53 | Stage B | 同上 |
| `internal/catalog/queries_admin.go` | 426 | Stage B | 同上 |
| `internal/catalog/queries_resolved.go` | 350 | Stage B | 同上 |
| `internal/catalog/store.go` | 578 | Stage B | 同上 |
| `internal/catalog/store_postgres.go` | 1,354 | Stage B | 同上 |
| `internal/catalog/values.go` | 149 | Stage B | 同上 |
| `internal/catalog_service.go` | 975 | **Stage B（官方口径漏计）** | `SnapshotCatalog` gRPC 处理器，物理上不在 `internal/catalog/` 目录下但逻辑上是该服务的唯一 RPC 面，必须随 B 一起移植，见 §3 脚注 |
| `internal/cpu_template.go` | 300 | **Stage A（官方口径漏计）** | 唯一调用方是 `node_registry.go:334` 的 `computeIntersectionLocked`（心跳聚合的 CPU 配置交集），纯函数库，随 A 走；1,481 的官方估算未把它算进去 |
| `internal/filter.go` | 118 | Stage A | 官方 1,481 已含 |
| `internal/kubernetes_discovery.go` | 379 | Stage A | 官方 1,481 已含 |
| `internal/lookup.go` | 574 | **Stage D** | 三段式路由，本文 §3 详述；必须移植，逻辑不可删 |
| `internal/metrics.go` | 842 | **Stage D（内容上跨 C/D）** | 41 个 Prometheus 指标定义；约 20 个带 `registry` 前缀，语义上属于 Stage C，但物理上同一个文件——移植时建议按 C/D 拆到两处注册点，而不是整体搬一份，见 §5 |
| `internal/node_registry.go` | 824 | Stage A | 官方 1,481 已含；同时是 P2P 端点列表 (`ListP2pPeers`)、observed-node 视图、CPU 交集、roster 的唯一持有者，Stage C/D 的多处逻辑靠读它，见 §0 附注 |
| `internal/reconcile.go` | 648 | **Stage C（官方口径未提及）** | 心跳驱动的 `paused_sandboxes` 租约续期（`RunRegistryReconcile`/`renewParkedLeasesFromHeartbeats`/`renewLiveLeasesFromHeartbeats`），纯粹操作 registry 的租约状态，应随 C 走，不属于本文 Stage D |
| `internal/redis_store.go` | 663 | **Stage D** | binding store 的 Redis 实现 + Lua 仲裁脚本，§4 详述；必须移植 |
| `internal/registry/catalog_tx.go` | 175 | Stage C | 随 C 迁移 |
| `internal/registry/grace.go` | 513 | Stage C | 同上 |
| `internal/registry/migrate.go` | 249 | Stage C | 同上 |
| `internal/registry/postgres.go` | 295 | Stage C | 同上 |
| `internal/registry/registry.go` | 220 | Stage C | 同上 |
| `internal/registry/store.go` | 588 | Stage C | 同上 |
| `internal/registry/store_postgres.go` | 2,151 | Stage C | 同上 |
| `internal/registry_service.go` | 978 | **Stage C（官方口径漏计）** | `PausedRegistry` gRPC 处理器，与 `catalog_service.go` 同理 |
| `internal/service.go` | 999 | **Stage D（含约 150 行应随 C 走）** | `Scheduler` 服务的全部 RPC 处理器：`Schedule` `ListNodes` `LookupNode` `RecordAssignment` `Heartbeat` `ReportSandboxEvent` `ListObservedNodes` 四个 P2P 方法 `GetNode` `UnregisterNode`，以及 `ListRegistrySandboxes`（约行 848–997，约 150 行，纯读 `pausedregistry.Reader`，逻辑上是 registry 的只读视图，应随 C 走而非留在 D） |
| `internal/store.go` | 689 | **Stage D** | `BindingStore` 接口 + `InMemoryBindingStore` + `InMemoryArtifactStore`（P2P artifact 索引），§4/§4.4 详述 |
| `internal/strategy.go` | 60 | Stage A | 官方 1,481 已含；`round_robin`/`random` 两个策略实现 |
| `internal/sweep.go` | 383 | **Stage D** | binding store 的第三条移除路径（heartbeat-timeout sweep），纯粹操作 `BindingStore`，不依赖 registry |
| `internal/types.go` | 25 | Stage A（共享） | `Node`/`RichNode` 类型别名，`Node = routing.Node`（与 gateway 共享同一个 Go 结构体），随 A 走 |
| `internal/warmup.go` | 100 | Stage A（官方口径已含，但功能上是 D 的从属） | 官方 1,481 把它计入 A，但它的唯一消费者是 `lookup.go` 的 warm-up gate；移植顺序上必须与 D 的 lookup 逻辑同批落地，不能只搬 A 就装完 |

**合计校验**：1,481(A 官方) + 300 + 25 + 3,613(B 官方) + 975 + 4,191(C 实测) + 978 + 648 + 4,977(D) = 17,188，与实测总量一致，无遗漏、无重复计数。

## 3. 路由三段式 lookup（`internal/lookup.go`）

三段（`lookup.go:141-330`，函数 `lookupNode`）：

1. **binding**（`:150-169`）：`deps.store.Get(sandboxID, now)`，热路径，命中即返回。数据源是
   `BindingStore`（内存或 Redis，见 §4），命中率假设是"绝大多数请求"——这是唯一在**每个代理请求**
   上运行的一段，其余三段只在未命中时才跑。失败即答 `Unavailable`（"binding store unavailable"），
   从不把存储故障读成"没有这个沙箱"。
2. **roster**（`:171-188`）：`deps.placer.rosterHolder(...)`，覆盖两个窗口——心跳还没到但
   binding TTL 已过期；以及别的节点的对账把 binding 删了但这台机器仍在心跳里报告持有。数据源是
   `AtomicNodeRegistry`（Stage A）里按节点心跳最近一次上报的 roster，用带化身感知的仲裁
   （`rosterPrefers`）+ 新鲜度 tie-break 选出最优持有者。
3. **registry**（`:190-292`）：`deps.registry.Get(...)`（`pausedregistry.Reader`，Stage C 的
   PG `paused_sandboxes` 表），按五个状态分叉：`paused`（可在任意节点重建，偏好 origin）、
   `publishing`/`local_only`（唯一副本在 origin 磁盘上，钉死 origin 且要求 origin
   schedulable）、`running`/`resuming`（读 `Holder()` == `origin_node_id`，从不读
   `claimed_by_node_id`——后者是仲裁者进程 id，结构上不可能出现在心跳 roster 里）。第 4 步之前
   还有一个 warm-up 门（`deps.warmup`，Stage A 文件里的从属逻辑）：binding 与 roster 由同一次
   心跳写入，一个刚启动、什么都没被告知过的 scheduler 答 NotFound 是在断言它无法断言的事，所以
   冷启动窗口内答 `Unavailable` 而不是 404。

三段的失败语义是设计核心：**NotFound 只在第 4 步（`lookupAbsent`）产生**，且要求 binding/roster
都已 warm——其余每一步凡是"没查成"（存储不可用、registry 未 ready、node 不可达）一律答
`Unavailable`（503，可重试），因为下游把 404 当成"这个沙箱已经不存在了"来处理，误判成本远高于
一次多余的重试。

**搬进 api 之后，三段是否还成立**：

- **第一段确实"消失"，但不是因为 api 自己持有 Redis 这个理由本身，而是因为 gateway 已经绕过它
  了**——阶段 1②"gateway 直读"已经落地并在 `deploy/k8s/base/kustomization.yaml`
  （`routing-projection-config`：`GATEWAY_ROUTING_PROJECTION_READ=on`、
  `GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE=on`）里默认打开：gateway 直接读同一个 Redis
  key 空间（`services/shared/routing`），命中就转发，未命中才落到 `LookupNode`。所以**今天
  第一段在 gateway 的调用路径上已经是"读 Redis"而不是"call scheduler"**——scheduler 自己的
  `lookupNode` 第一段（`deps.store.Get`）只在 gateway 未命中回落、或 api 自己需要放置时才跑。
  搬进 api 后，这条回落调用从"gRPC 到 scheduler"变成"api 里的一次函数调用"，**逻辑不消失，
  只是不再是 RPC**。
- **第二段（roster）依赖 Stage A 的 `AtomicNodeRegistry`**，一旦 A 先于 D 落地，这段在 api
  进程内直接可用，无需改动语义。
  但要注意：它的必要性来自"binding 的 TTL 比心跳间隔长得多"这一事实——阶段 1 第 5 点的
  `KEEPTTL` 修复以及寿命上界钳制已经把这个窗口收窄了很多，但没有消灭它，第二段仍然要搬。
- **第三段（registry）依赖 Stage C**，语义完全不变，只是从 gRPC 客户端读变成本地 PG 查询——
  相应地，`pausedregistry.ErrDisabled`/`Ready()` 这套"没配置 vs 配置了但读不出来"的区分必须原样
  保留，因为它是"never invent NotFound"这条硬约束的落点。
- 三段合并进同一个进程后，**新增的简化空间**是：第 1/2/3 段目前是三次独立的、各自处理失败的调用
  （因为它们原本可能来自不同的存储/服务），搬进 api 之后可以合并成一次事务性的读（比如
  一次 Redis pipeline + 一次 PG 查询），但**这是一次可选的性能优化，不是移植的必要条件**——
  按原语义原样搬三段，行为不变，是更安全的第一步。

## 4. binding store

- **接口**：`BindingStore`（`internal/store.go:14-30`）——`Get`/`Record`/`ReconcileNode`/`Delete`
  （guarded by execution id）。
- **内存实现**：`InMemoryBindingStore`（`store.go:195-501`），仅用于单副本/开发场景。
- **Redis 实现**：`RedisBindingStore`（`internal/redis_store.go`），键空间与记录格式来自
  **共享包 `services/shared/routing`**（`services/shared/routing/record.go` 等，295 行非测试，
  不在 `services/scheduler/` 计数内，因为 gateway 也直接 import 它读同一批 key）——这是一份
  **跨语言契约**：Redis 里的 JSON 记录格式和 Lua 脚本的仲裁语义，gateway（Go，保留）今天直接
  读它，搬进 api 之后 Rust 必须写出 gateway 能解析的同一份格式，不能"翻译成 Rust 自己的
  结构"。
- **谁写谁读**：写者是 `RecordAssignment`（gateway 的同步写 + api 自己的 `SchedulerNodePlacement`
  在 cold-start 分发之后的写，见 `src/node_client/scheduler_placement.rs`）与
  `ReconcileNode`（心跳驱动，`redisReconcileNodeScriptBody`，`阶段 1 第 5 点`修的 `KEEPTTL`
  vs `PX` 分岔）；读者是 `lookup.go` 第一段与 gateway 的直读路径。

**搬进 api 之后**：

1. **`api` 已经持有 Redis（`src/orchestrator/store/redis/`），但 binding 不能直接并进那个
   store**——那是 api 自己的沙箱元数据/状态机存储（`SandboxMetadata`，CAS 事务、状态转换锁），
   binding 是一份**给 gateway 读的、独立于 api 是否在线的路由缓存**。两者的失效模型不同：
   binding 的存在理由（阶段 1 的核心兑现）恰恰是"控制面（scheduler/api）挂掉不拖死数据面
   （gateway 转发）"——如果把 binding 折进 api 自己那份状态存储的 key 空间，gateway
   还是要读同一个 Redis，语义结果一样，但把两份不同生命周期的数据混进同一套 CAS/锁语义
   反而增加耦合。**建议：binding 保持独立的 Redis key 空间与 Lua 脚本（沿用
   `services/shared/routing` 的格式），只是"谁来写"从 Go scheduler 换成 Rust api**——
   这是移植（把 `redis_store.go` 的 Lua 脚本与仲裁常量对照
   `src/orchestrator/store/redis/scripts.rs` 的 `lazy_script!` 模式重写一份），不是合并。
2. **`--query-only` 这套 HA 模式随 scheduler 一起消失**。它存在的唯一理由是"scheduler 的
   写路径（PG 仲裁 + Redis 写）单副本，但 `LookupNode` 读路径可以从 Redis 横向扩展"；一旦
   scheduler 进程本身消失、`LookupNode` 的等价物变成 api 里的一个函数，"query-only 副本"这个
   概念不再有独立存在的意义——**api 本来就是 N 副本**，每个副本都能读同一个 Redis 回答
   binding 命中，天然就是过去"query-only 副本"想要的效果，不需要再造一种"只读子集角色"。
3. **`gateway.query_only_scheduler_addr`**（Go 侧字段名
   `Gateway.QueryOnlySchedulerAddr`，env `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR`）：
   `services/gateway/internal/server.go:633` 是它唯一的调用点，且已经有杀关（commit
   `e289f0e`，`GATEWAY_SCHEDULER_FALLBACK_DISABLED`/`GATEWAY_SCHEDULER_FALLBACK_TIMEOUT`，
   已在 `deploy/k8s/base/gateway-deployment.yaml` 声明、default 未设为强制关闭）。
   **Stage E 下线时的动作是：确认 `GATEWAY_RESUME_ADDR` 指向 api
   （`agentenv-api:8002`）且 `GATEWAY_SCHEDULER_FALLBACK_DISABLED=true`，再删掉
   `QueryOnlySchedulerClient`/`GATEWAY_QUERY_ONLY_SCHEDULER_ADDR` 这条代码路径与配置项**——
   这条杀关是本仓已经为这一步准备好的钩子，不用新造。

## 5. 其余 RPC 逐个裁决

| RPC | 调用方 | 裁决 |
|---|---|---|
| `Schedule` | gateway（create 路径，`server.go:517`，仅当 `GATEWAY_REST_UPSTREAM_ADDR` 未指向 api 时）；api 自己（`SchedulerNodePlacement`，cold-start 分发与 resume 放置） | **搬**——`selectNode` + `Strategy`（Stage A）+ `FilterUnschedulable`/`FilterByResourceLimit`（Stage A）在 api 里变成一次进程内函数调用。一旦 `GATEWAY_REST_UPSTREAM_ADDR=http://agentenv-api:8000` 落地（已有开关，见 `deploy/k8s/base/kustomization.yaml` 的 `api-upstream-config`），gateway 侧这个调用点直接消失，只剩 api 自己内部用 |
| `RecordAssignment` | 同上两个调用方 | **搬**——一旦成为 api 内部调用，退化成"选完节点后往 binding Redis 写一条记录"，不再是 RPC |
| `ListNodes` / `GetNode` | gateway（`GET /nodes`、`GET /nodes/{id}`，`cluster_list.go:112`、`node_list.go:183`） | **搬**——数据源是 Stage A 的 `AtomicNodeRegistry`/observed-node 视图，搬进 api 后 gateway 改成调 api 的等价 REST/gRPC 端点，或者 api 直接暴露一个聚合端点替代 |
| `UnregisterNode` | 目前无 Rust/Go 调用方证据之外的地方（节点自身优雅关闭时调用，具体调用点未在本轮核实，见 §12 待确认） | **搬**（随 Stage A） |
| `Heartbeat` | 每个节点的 `ObservabilityReporter`（`src/observability/reporter.rs`），目标地址已由 `4456481` 做成可热改（见 §7） | **搬**——目标改指 api 之后，这个 RPC 处理器（`AtomicNodeRegistry::Heartbeat` 的解析/roster 更新/CPU 交集）成为 api 收到的第一手数据源，Stage A/D 都靠它 |
| `ReportSandboxEvent` | 同一个 `ObservabilityReporter` | **可以直接删，但要分两半看**：Go 侧处理器（`service.go:576-600`）今天逐字是"记一条 debug 日志，返回空响应"，`applyProjectionDelete` 的真正删除逻辑目前只在**阶段 1 第 3 点交付之后**才会被启用（当前 PAUSE/DELETE 事件走这条通道，但读代码可见处理器主体仍是接收即丢弃的占位）——如果搬的时点在阶段 1 那一半（PAUSE/DELETE 按 execution 守卫删 binding）已经实现之后，这个 RPC 的处理器就不是"纯日志"了，必须带着阶段 1 第 3 点的逻辑一起搬，不能因为记忆里"现在只是记日志"就整体删掉调用；如果阶段 1 第 3 点始终没做，那这条 RPC 从节点侧的发送方（`reporter.rs`）到 scheduler 侧的接收方都可以一起砍掉。**结论：裁决取决于阶段 1 第 3 点是否已经交付，不能脱离这个前置条件单独回答，本轮未能确认阶段 1 第 3 点的交付状态（见 §12）** |
| `ListObservedNodes` | gateway/运维查询（具体消费者未核实，见 §12） | **搬**（随 Stage A 的 observed-node 视图） |
| `ListP2pPeers` | node 侧 P2P 发现（`src/p2p/discovery/scheduler.rs`） | **搬**（随 Stage A——数据源是 `AtomicNodeRegistry` 里每个节点心跳携带的 `P2pEndpoint`，不是独立状态） |
| `RecordP2pArtifact` / `ForgetP2pArtifact` / `LookupP2pArtifact` | node 侧 P2P 传输层（写入/查询 artifact-to-node 提示索引） | **必须搬，但语义会退化**：索引本体是 `internal/store.go` 里的 `InMemoryArtifactStore`（LRU，纯内存，**没有 Redis 或任何持久化实现**）。scheduler 今天单副本，索引是全局唯一一份；搬进 N 副本的 api 后，除非另起一个共享后端（Redis 或类似），否则**每个 api 副本只掌握经由它自己被调用过的那部分索引，天然只是一份不完整的本地缓存**。CLAUDE.md 原文把它定位成"accelerates artifact lookup before falling back to broad peer polling"——即它设计上就是一个可以缺失的加速器，缺失时退化为广播式 peer 轮询，不是正确性依赖。**建议**：按现状原样搬（每副本一份本地 LRU），显式接受命中率下降，不在这一阶段重做成 Redis 共享索引；如果后续观察到轮询兜底成本过高再单独立项 |
| `ListRegistrySandboxes` | 运维/管理面只读视图 | 物理上在 `service.go`，**语义上应随 Stage C 一起搬**（纯读 `pausedregistry.Reader`），不要跟着 D 的其余部分走 |
| 调度策略 `round_robin`/`random` | `Schedule` 内部 | 已在 Stage A 的官方 1,481 行估算内（`strategy.go`），**不属于本文 Stage D**，随 A 搬 |

## 6. 指标

`internal/metrics.go` 定义 41 个 Prometheus 指标（`agentenv_scheduler_*` 前缀），经由独立的
`:9101` metrics 监听端口暴露（`scheduler-deployment.yaml`/`scheduler-service.yaml`）。**本仓
未追踪任何 Prometheus scrape 配置或 Grafana dashboard**（`deploy/` 下无 ServiceMonitor、无
`prometheus.yml`、无 `grafana/` 目录）——`/metrics` 是否真的被外部 Prometheus 抓取、告警规则是否
存在，**无法在本仓范围内确认，标为待确认**（见 §12）。

指标大致三类：
- **registry 相关**（约 20 个，`schedulerRegistry*`）：行数上物理属于 `metrics.go`，语义上属于
  Stage C，应随 C 一起搬。
- **Schedule/lookup/binding/heartbeat/sweep 相关**（`schedulerScheduleDuration`
  `schedulerObservedNodes` `schedulerLookupResults` `schedulerBindingExecution`
  `schedulerLookupExecutionAuthority` `schedulerHeartbeatLegacyRoster`
  `schedulerHeartbeatRosterDropped` `schedulerSandboxEvent` `schedulerProjectionTTLSource`
  `schedulerBindingSweep*` `schedulerRoutingExecutionArbitration`）：真正属于本文 Stage D，
  应随 D 搬。
- **纯 gRPC 传输层的**（`agentenv_scheduler_rpc_duration_seconds`）：随 gRPC 面消失，**这个
  指标本身该删**——api 已经在用 `metrics::histogram!`（`src/observability/prometheus.rs`）
  记录 HTTP 路由耗时，进程内函数调用不再需要一个"RPC 耗时"维度。

**下线后的等价物**：api 侧已经有一个 `/metrics` 端点（`src/observability/prometheus.rs`，挂在
主 HTTP 端口 `:8000` 上，不像 scheduler/gateway 那样有独立监听端口——
`agentenv-api-deployment.yaml:342` 的注释明确写了"No metrics port"），但**目前只有通用的
HTTP 路由耗时和存储阶段耗时指标，没有任何 scheduler 语义的指标**（lookup 结果分类、binding
仲裁决策、registry 租约续期健康度、P2P 索引命中率等，一个都没有）。这些需要在移植 D/C 逻辑的
同时用 `metrics::counter!`/`gauge!`/`histogram!` 宏在 Rust 里逐个新增，复用同一个 `/metrics`
端点，不需要新端口/新 Service。

## 7. gateway 侧的全部耦合

`services/gateway/internal/`：

| 耦合点 | 位置 | Stage E 之后 |
|---|---|---|
| `Schedule` gRPC 调用 | `server.go:517` | 一旦 `GATEWAY_REST_UPSTREAM_ADDR` 指向 api，这条调用点整体消失（REST create 走 api，不再经 gateway 转发到 scheduler 选节点） |
| `LookupNode`（query-only 客户端） | `server.go:633`，受 `GATEWAY_SCHEDULER_FALLBACK_DISABLED`/`_TIMEOUT` 杀关保护（`e289f0e`） | 确认 `GATEWAY_RESUME_ADDR` 已指向 api 后关闭杀关、删代码，见 §4 |
| `RecordAssignment` | `server.go:1024` | 同 `Schedule`，随 REST 上游切到 api 一起消失 |
| `ListNodes` | `cluster_list.go:112`（`GET /nodes` 聚合） | 需要 api 提供等价查询面（节点清册 Stage A 的产物），否则这个端点在 scheduler 下线后无数据源 |
| `GetNode` | `node_list.go:183`（`GET /nodes/{id}`） | 同上 |
| Redis 直读（binding） | `server.go`（`GATEWAY_REDIS_ADDR`，已默认开启） | **保留不变**——这是唯一一条"不经过 api/scheduler 进程"的路径，是阶段 1 的核心价值，Stage E 不应该动它，只是 Redis 里数据的写者换成 api |
| 配置项 `gateway.scheduler_addr` / `SCHEDULER_ADDR` | `services/gateway/cmd/main.go:50` | 名字保留还是重命名待定（如果 gateway 从此不再直连一个叫"scheduler"的东西，这个配置名会显得名不副实，但这是收尾细节，不是阻塞项） |
| 配置项 `gateway.query_only_scheduler_addr` | 同上 | 删（见 §4 第 3 点） |
| `GATEWAY_SCHEDULER_FALLBACK_DISABLED`/`_TIMEOUT` | `deploy/k8s/base/gateway-deployment.yaml` | 落地为永久 `true` 后可以连同这两个配置键与它们保护的调用路径一起删 |

**结论**：gateway 本身保留，但它目前"直接调 scheduler gRPC"的调用点（`Schedule`
`RecordAssignment` `ListNodes` `GetNode` `LookupNode` 回落）要么已经有开关能切到 api
（`GATEWAY_REST_UPSTREAM_ADDR`/`GATEWAY_RESUME_ADDR`/`GATEWAY_SCHEDULER_FALLBACK_DISABLED`），
要么需要 api 新增等价端点才能切（`ListNodes`/`GetNode` 这两个目前没有对应的 api 端点，本轮
未在 `src/api/` 找到聚合节点列表的现成实现——**待确认，见 §12**）。

## 8. Stage E 的心跳目标改指（commit `4456481`）

`4456481c5f8d33`（`feat(observability): let a node's heartbeat target change without a
DaemonSet roll`）做的事：

- **只让 `ObservabilityReporter` 的心跳目标可热改**，新增
  `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT_FILE`（`src/cfg.rs` 新字段
  `scheduler_endpoint_file`）：`SchedulerChannelSource`
  （`src/observability/reporter.rs`）每次心跳 tick 重新读这个文件，内容变化就重建 gRPC
  channel 并原地替换——下一次心跳和 sandbox-event 批次立即跟随新地址，无需重启/滚动。
  未配置该文件时行为与之前逐字节一致（启动时读一次静态值，之后终身不变）。
- **部署侧**：DaemonSet 的字面量 `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` 值挪进新 ConfigMap
  `node-heartbeat-config`（`deploy/k8s/base/kustomization.yaml`），同一个 key 被两处消费：
  作为环境变量（启动时读一次的 bootstrap 值）与作为**不带 `subPath` 的整卷挂载**
  （`/etc/agentenv/heartbeat/scheduler-endpoint`，kubelet 会定期同步刷新；`subPath`
  挂载不会被刷新，这是本次修改选文件而不是继续用环境变量的唯一原因）。
- **怎么切**：编辑 `node-heartbeat-config` 这个 ConfigMap 的
  `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` 值，等 kubelet 同步卷（约一分钟内），下一次心跳
  tick 自动切走，**不需要 `kubectl set env`、不需要重启 Pod、不需要滚动 DaemonSet**。
- **怎么切回**：把 ConfigMap 值改回旧地址，同样无需重启，一次心跳周期内自动切回。
- **🔴 关键限制，Stage E 检查清单必须覆盖**：这个机制**只覆盖心跳这一个消费者**。
  `[cluster].scheduler_endpoint`（`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`，静态值）还有
  三个其他消费者，`4456481` 的注释原文点名了它们且明确说"这些仍然读静态值，仍然需要重启才能
  改"：
  1. P2P 发现（`src/p2p/discovery/scheduler.rs`）
  2. paused registry 的 `central` 后端（`src/orchestrator/paused_registry/central.rs`，
     以及 catalog 的 `central` 后端 `src/snapshot/repository/backends/central/mod.rs`，
     都读同一个静态端点）
  3. resume/create 的放置客户端（`src/node_client/scheduler_placement.rs`，
     `SchedulerNodePlacement`，仅 `--role api` 使用）
  也确认了 `agentenv-api-deployment.yaml:304` 的
  `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT=http://agentenv-scheduler:9090` 是静态值，
  api 半边的注释原文写着"Still needed, and not for heartbeats: the `central` paused
  registry above speaks to the scheduler over this endpoint"——**api 自己都还没有热改
  这个端点的机制**。

  ⇒ **`4456481` 只是 Stage E 回退安全网的第一片，不是全部**。真正做"scheduler 进程下线"
  这一刀时，P2P 发现、paused registry central 客户端、`SchedulerNodePlacement`
  三处如果也要跟着回退（先切到 api 再切回 scheduler），今天仍然是"改配置 + 重启 Pod"，
  不是热改。这与 CLAUDE.md 里"每一块独立可回退"的硬约束有直接冲突，需要在 Stage E
  的排期前决定：要么把另外三个消费者也补齐热改（工作量与 `4456481` 相当，×3 且各有不同的
  重连语义），要么接受这三处的回退代价是一次 Pod 重启（这本身不算太重，因为 api 是
  N 副本、可以滚动重启，比 scheduler 只有 1 副本时的整卡顿要好，但仍然不是"零停顿"）。

## 9. 部署侧

`deploy/k8s/base/` 里与 scheduler 直接相关的对象：

- `scheduler-deployment.yaml`——单副本 Deployment，`replicas: 1` 且注释明确写死不能加副本
  （"observed-node state and the P2P artifact index are still per-process memory"）。
  下线时**整体删除**。
- `scheduler-service.yaml`（gRPC `:9090` + metrics `:9101`）——下线时删除。
- `scheduler-pdb.yaml`（`minAvailable: 1`，专门防止唯一副本被 drain 带走）——下线时删除。
- `config/scheduler.json`（挂载为 `scheduler-k8s-config` ConfigMap，`grpc_listen_addr`
  `strategy` `discovery.kubernetes.*` `registry.write_enabled`
  `registry.write_fencing` `routing.execution_arbitration`）——下线时删除，但**其中
  `discovery.kubernetes.*` 与 `strategy` 这两块内容要迁移成 api 的等价配置**（Stage A 的
  节点发现与调度策略在 api 里需要一份新的配置面，不是简单删除了事）。
- Secret `agentenv-postgres`（`dsn` key，供 `SCHEDULER_REGISTRY_DSN` 使用）——**api 需要
  新增对同一个 Secret 的挂载**，因为 Stage C 移植后 api 会直连这个 Postgres。CLAUDE.md
  已经点名了这个机制：`AENV_CONFIG_OVERLAY_PATH` 就是为"一个免凭据的跟踪文件 + 一个挂载的
  Secret 合成 `[backend.oss]`"这种模式而生的，`deploy/k8s` 已经在用它（见
  `agentenv-api-deployment.yaml:213-217` 的 `AENV_CONFIG_OVERLAY_PATH` 挂载）——PG DSN
  可以照抄同一个模式：一个不含凭据的 overlay TOML 片段（host/port/dbname/schema）+
  从 `agentenv-postgres` Secret 挂载凭据，两者深合并出 api 需要的完整 PG 连接配置，而不是
  把 DSN 整个塞进一个环境变量。
- ConfigMap `execution-fencing-config`（`SCHEDULER_REGISTRY_WRITE_FENCING`/
  `SCHEDULER_ROUTING_EXECUTION_ARBITRATION`）、`routing-projection-config`
  （`SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE`）——这两个 ConfigMap 同时被 gateway 侧的
  对应键消费（`GATEWAY_ROUTING_EXECUTION_FENCING` 等），下线时只删 scheduler 那一半键，
  gateway 那一半（如果还需要）要看 Stage C/D 移植后 api 是否还需要对称的仲裁开关。
- `cluster-identity-config`（`CLUSTER_ID`）——不属于 scheduler 专有，DaemonSet 和 api 都读，
  保留。
- `redis.yaml`（`agentenv-redis`）——**保留**，binding store 与 gateway 直读都要继续用它；
  只是它的写者从 scheduler 换成 api。
- `node-heartbeat-config`（`4456481` 新增）——**保留**，值从
  `http://agentenv-scheduler:9090` 改成 api 的地址（如 `http://agentenv-api:8000` 或
  专门的心跳接收端点，取决于 api 侧怎么暴露 `Heartbeat` 的等价接口）。
- `scheduler-fallback-config`（`GATEWAY_SCHEDULER_FALLBACK_DISABLED`/`_TIMEOUT`）——下线时
  把值锁定为"永久关闭回落"，可以在下一个 release 里连同它保护的代码路径一起删除。

## 10. `--role all` 回退路径

`assemble_all`（`src/bin/server.rs`）是"拆分之前跑的那个进程"的定义，**它本身对 Go
scheduler 进程没有硬依赖**：

- `cluster_placement()`（需要 `[cluster].scheduler_endpoint`，否则直接 `bail!`）**只在
  `assemble_api` 里被调用**，`assemble_all` 从不调用它——`--role all` 自己在本机决定放哪个
  节点，不需要问任何调度器。
- `assemble_all` 里唯一会用到 `[cluster].scheduler_endpoint` 的地方是**可选的**心跳上报
  （`core.reporter`，`observability.scheduler_report.enabled` 关闭时整个上报任务不启动）
  与可选的 paused registry `central` 后端——两者缺省都是"没配置就优雅降级/不启动"，不是
  硬性 `bail!`。
- 源码扫描测试 `only_the_split_roles_bind_a_second_listener`（`src/bin/server.rs:1449` 起）
  验证的是一个**完全不同的轴**：AgentENV 内部的 node↔api "唤醒" gRPC 面
  （`services/api/proto/node.proto`，`spawn_grpc_surface`），不是 Go 的
  `services/api/proto/scheduler.proto`。这两个 proto 文件在 `src/proto.rs` 里分属
  `pub(crate) mod node` 与 `pub(crate) mod scheduler`，`node.proto` 的文档注释原文写着
  "Unlike the scheduler proto next to it, this one has no Go consumer"——它们是两条独立的线。

**建议**：`--role all` 这条回退路径**在 scheduler 进程下线之后仍然完全有效，不需要修改**，
因为它从一开始就不经过 Go scheduler。`only_the_split_roles_bind_a_second_listener` 这个
安全网测试**也不需要因为 Stage E 而改**——它守的是 node↔api 唤醒面的进程拓扑，与 Stage
D/E 要下线的 Scheduler/PausedRegistry/SnapshotCatalog 三个 Go gRPC 服务毫无关系。

唯一需要新增覆盖的地方（不是改这条已有测试，而是补一条新的）：**`--role all` 在
`[cluster].scheduler_endpoint` 完全未配置、且 Stage C/D 已经把 catalog/registry/binding
都吸收进 api 自己的情况下，是否还能正确工作**——今天 `--role all` 的 paused registry
默认是 `local` 后端（不依赖任何 scheduler），这条路径本来就是独立验证过的；真正的新风险是
Stage A/B/C/D 移植进 `--role api`/`--role node` 之后，会不会有新代码**误把某个只在
`api`/`node` 分离场景下才存在的假设（比如"catalog 一定是远程 gRPC"）硬编码进公共路径**，
从而让 `assemble_all` 意外开始依赖一个它本不该依赖的东西。这是移植 A/B/C/D 时要盯的回归面，
而不是这条测试本身需要改。

## 11. 三个最大的风险

1. **P2P artifact 索引（`InMemoryArtifactStore`）从"单副本全局唯一"变成"N 副本各自一份不完整
   缓存"，且没有共享后端**——不是移植能解决的，是移植之后必然发生的语义退化，唯一的缓解是它
   本来就设计成"缺失时退化为广播轮询"，但退化幅度（缓存命中率下降多少、轮询开销涨多少）
   在实测之前无法量化。
2. **`4456481` 的热改机制只覆盖心跳一个消费者，`[cluster].scheduler_endpoint` 的另外三个
   消费者（P2P 发现、paused registry/catalog 的 `central` 客户端、
   `SchedulerNodePlacement`）仍然是"改配置要重启 Pod"**——如果 Stage E 的排期假设"回退是
   纯配置切换、不用滚动"，这个假设在这三处不成立，需要要么补齐、要么在排期里显式承认这个
   代价。
3. **`ReportSandboxEvent` 的裁决依赖阶段 1 第 3 点（PAUSE/DELETE 按 execution 守卫删
   binding）是否已经交付**，而本轮未能确认这一点的真实交付状态（`services/scheduler/internal/service.go` 里
   `ReportSandboxEvent` 的处理器主体读起来仍是"记日志、返回空"的占位形态，但方案文档写的是
   "这条通道已经打通，只是接收端在丢弃"——如果这两者的判断不一致，会导致"能不能直接删"这个
   结论整个反转，属于必须先核实再排期的前置项。

## 12. 待确认（未能证实，不要当结论用）

- `阶段 1 第 3 点`（PAUSE/DELETE 事件走 `ReportSandboxEvent` 并按 execution 守卫删除
  binding）**在当前代码里是否已经真正交付**——`applyProjectionDelete`
  （`service.go:602`）函数体本身存在，但本轮未逐行确认它是否已经被 `ReportSandboxEvent`
  处理器实际调用、并在生产配置里打开。这直接决定 §5 中 `ReportSandboxEvent` 能否直接删。
- `ListObservedNodes` 与 `UnregisterNode` 的**实际调用方**——本轮在 gateway 与 Rust
  代码里未找到明确调用点，不能排除是只有运维脚本/`grpcurl`/未来功能预留的可能性。
- `agentenv_scheduler_*` 系列 Prometheus 指标**是否真的被外部 Prometheus/Grafana 抓取和
  使用**——本仓没有追踪任何 scrape 配置或 dashboard 定义，无法证实也无法证伪。
- gateway 的 `ListNodes`/`GetNode`（`GET /nodes`、`GET /nodes/{id}`）在 Stage A 移植进 api
  之后，**api 侧是否已有等价的聚合端点**——本轮未在 `src/api/` 找到对应实现，需要在排期
  Stage D 的这两个 RPC 时单独确认是否要新增。
- `internal/metrics.go` 里"约 20 个 registry 相关指标"的**精确行区间**——本文只按指标名前缀
  估算了归属，未逐行切分文件内的物理边界，移植时需要重新核对。
- `services/scheduler` 非测试行数与 `phase4-scheduler-decommission`/
  `deletion-batch-not-unlocked` 两条记忆记录的数字（16,991 / registry 4,044）之间约
  150-200 行的差异，**是否全部来自 2026-08-25 当天的 paused-registry 修复提交**——本轮只
  确认了实测值（17,188 / 4,191），未逐提交核对差值构成。
