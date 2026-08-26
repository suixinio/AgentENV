# Phase 4 遗留问题逐条查实

调研方法说明：本 agent 运行在独立 worktree（分支 `worktree-agent-a4e1b39b0a891855c`），
与承载 Stage A–DE 四份文档和阶段 1 实现的 `feat/phase4-scheduler-fold`
（tip `56de492`，是 `dev` tip `04d5f00` 之上再合并 `fix/orphan-after-node-death` 的结果）
是两条独立分支。为了看到 Stage A–DE 文档本身查实的最新代码状态，本文档所有引用一律基于
`git show 56de492:<path>`（不改动、不 checkout 该分支，只读取该 commit 下的文件内容），
下文简写为 `56de492:`。四份既有分块调研也是从这个 commit 读取的
（`docs/proposals/_sd-phase4-stage{A,B,C,DE}-*.md`）。

## 一句话结论汇总

- **Q1**：`ReportSandboxEvent` **不能**直接删——阶段 1 第 3 点（PAUSE/DELETE 按 execution
  守卫删 binding）**已经实现且已经在部署配置里打开**（`projection_authoritative=on`），
  该 RPC 现在是真正做事的路径，删除会让 pause/delete 退回到只能靠 TTL/心跳回收 binding。
- **Q2**：`cluster_list.go` 是 gateway 的**沙箱清单**扇出端点（`GET /sandboxes`），跟节点清册无关；
  "消掉重复清册"指的是 Stage A 完成后到 Stage E scheduler 下线前那段**过渡期**里 api 新清册与
  scheduler 旧清册并存的状态，不是今天已经存在的重复——Stage A 自己的推断成立，予以确认。
- **Q3**：`--role node` 通过 `SnapshotManager` **只读**目录（`load_runnable`→`get`/`get_scoped`/
  `resolve_alias_scoped`），从不写目录行——`stage`/`stage_captured` 只写字节，目录行的
  commit 全部在 api 侧完成；链路确认为 `central/mod.rs` → gRPC → `catalog_service.go` → PG；
  node→api 反向面**不存在**，建议优先让 api 把已解析的记录带下去，而不是新建一条 RPC 面。
- **Q4**：`4456481` 只让 `ObservabilityReporter`（心跳 + 事件上报）热改；实际点验发现还有
  **5 处**（不是 3 处）各自独立 `connect_lazy` 一次、进程存活期持有：P2P 发现、paused-registry
  central 客户端、snapshot catalog central 客户端、创建放置客户端（`SchedulerNodePlacement`）、
  resume 放置客户端（`SchedulerPlacementSource`）。已抽出的 `SchedulerChannelSource` 具备直接复用条件。
- **Q5**：P2P artifact 索引退化只表现为**变慢**，不会**错**——`lookup_with_hints` 对索引结果本来
  就是"命中就用，未命中或出错就退回全量/hint 轮询"，索引从来不是正确性的必要条件；建议按 Stage D
  原判断接受退化、记录延迟回归，不必落 Redis。

---

## Q1 — `ReportSandboxEvent` 能不能直接删

### 结论
不能直接删。阶段 1 第 3 点已经交付，且在 `deploy/k8s/base/kustomization.yaml` 里默认打开，
`ReportSandboxEvent` 目前是 PAUSE/DELETE 事件唯一的"立即释放 binding"路径，删除会让集群退回到
只能靠 30s TTL 或下一次心跳回收——这正是这条 RPC存在的价值所在。

### 证据
- `56de492:services/scheduler/internal/service.go:576-598`（`ReportSandboxEvent`）对 PAUSE/DELETE
  调用 `applyProjectionDelete`，其余事件类型才是"只记一条 debug 日志"：
  ```go
  case SANDBOX_EVENT_TYPE_PAUSE, SANDBOX_EVENT_TYPE_DELETE:
      if s.applyProjectionDelete(event, now) { applied++ }
  default:
      recordSandboxEvent(...) // 仅打点，不落状态
  ```
- `service.go:602-643`（`applyProjectionDelete`）在 `projectionAuthoritative` 开关打开时，
  从事件里取出 execution id（`normalizeExecutionIDReason`），调用
  `s.store.Delete(sandboxID, execution, now)`；`store.go:14-58` 的 `BindingStore.Delete` 接口注释
  明确写"the guard is the whole point"——记录名字对不上事件带来的 execution id 就拒绝
  （`BindingDeleteRejectedStale`），对上了才真删（`BindingDeleteDeleted`）。
- `store.go:283-314`（`InMemoryBindingStore.Delete`）与 `redis_store.go:139-162`
  （`RedisBindingStore.Delete`，用 Lua 脚本做同样的比较）两个后端都实现了这个守卫——in-memory 和
  生产用的 Redis 后端一致。
- 开关默认值与生产配置：`service.go:44-54` 注释写"It defaults to false"；
  `deploy/k8s/base/kustomization.yaml:199-200`：
  ```
  SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE=on
  GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE=on
  ```
  即部署时默认已经打开，`main.go:114-116` 把它接进 `WithAuthoritativeProjection`。
- Rust 发送侧：`56de492:src/observability/reporter.rs:360-386`（`send_sandbox_events`）失败只
  `warn!` 丢弃，不重试（`reporter.rs:200-207`）；`service.go:561-565` 的注释也确认这一点是设计
  假设（"best effort... the heartbeat reconciliation is the repair path"）。

### 对施工的影响
折叠 scheduler 进 api 时，`ReportSandboxEvent` 的整个 `applyProjectionDelete` 逻辑（含 execution
守卫、TTL 解析 `resolveProjectionTTL`）必须原样搬进 api，而不是当作日志占位删掉；Stage D 之前
"可能是纯日志"的裁决前提已经被推翻，需要把这条工作量重新计入排期。

---

## Q2 — `cluster_list.go` 的语义与"重复清册"指什么

### 结论
`cluster_list.go` 是 gateway 对**沙箱清单**（`GET /sandboxes`、`GET /v2/sandboxes`）的扇出实现，
先调 `ListNodes` 拿节点表，再对每个节点发 REST 请求汇总沙箱列表，与节点身份/健康清册无关。
"消掉重复清册"这句话确认是**前瞻性**的：api 完成 Stage A（自建节点清册）之后、scheduler 在
Stage E 下线之前，api 的新清册与 scheduler 原有 `NodeRegistry` 会短暂并存，这段并存才是要消掉的
"重复"；今天 `--role api` 没有任何本地节点状态，字面意义上不存在重复。

### 证据
- `56de492:services/gateway/internal/cluster_list.go:63-99`（`isClusterListRequest`/
  `fansOutClusterList`）：路由是 `/sandboxes`、`/v2/sandboxes`；`handleClusterList`
  （110-117 行）调 `s.scheduler.ListNodes(...)` 拿节点表后 `fetchClusterList` 逐节点扇出。
  `listedSandbox` 结构体（23-47 行）字段是 `TemplateID`/`SandboxID`/`State`/`ExecutionID` 等——
  确认这是沙箱清单，不是节点清单。
- `56de492:src/bin/aenv-api.rs:939-944`（`assemble_api`）：
  `debug_assert!(!role.sends_heartbeats())` —— `--role api` 今天完全不维护本地节点状态。
- `56de492:docs/proposals/2026-08-20-service-decomposition.md:171-178`：
  > 目标形态里 `api` 需要节点清册来解析 node endpoint，`scheduler` 也需要节点清册来放置 ——
  > **两个进程各自维护同一份状态**……正确答案是阶段 4 把 `scheduler` 折叠进 `api`
  ——这与 Stage A 文档 §2.4（`_sd-phase4-stageA-node-inventory.md:144-158`）给出的"前瞻性重复"
  读法完全对应：api 的 Stage A 新实现 + scheduler 原实现，在切换开关打开之前会短暂并存，
  Stage E 才真正消掉。

### 对施工的影响
`cluster_list.go` 的删除时机挂在 Stage B（catalog 进 PG，沙箱清单变成一条 SQL）而不是 Stage A；
"重复清册"不是 Stage A 要立刻解决的问题，是 Stage A 到 Stage E 之间的一个已知、有意为之、
behind 开关的过渡状态，不需要为它单独排期消除工作。

---

## Q3 — `--role node` 与 catalog：具体操作、链路、node↔api 现有面

### 结论
`--role node` 对 catalog 的操作**全部是读**（`SnapshotManager::load_runnable` →
`get`/`get_scoped`/`resolve_alias_scoped`），用于两个场景：(a) 从快照引用创建沙箱、
(b) 远程模板构建时解析 base 快照引用。写操作（`stage`/`stage_captured`）只把字节写进共享仓库，
从不提交目录行；目录行的 `begin_snapshot`/`commit_snapshot`/`try_start_build` 等提交动作全部由
api 侧自己的 `SnapshotManager` 完成。链路确认为
`central/mod.rs`（Rust gRPC 客户端）→ gRPC → `catalog_service.go`（Go gRPC 服务）→ PG。
node→api 反向 RPC 面**目前不存在**——node 除了被 api 通过 `node_service_addr` 驱动之外，
没有任何配置项知道 api 的地址。**建议**：不要为这两个只读场景新建一条 node→api 面，而是把
api 已经在 dispatch 时经手的快照引用改成"api 先解析、把解析结果连同现有 RPC 消息一起发给
node"，让 node 端完全不用碰 catalog——这只需要扩两个已有的 proto 消息字段和挪两处调用点，
比新建一整套服务发现+客户端+鉴权的量级小得多。

### 证据
- **凭据不落地的规定**：`56de492:src/snapshot/repository/backends/mod.rs:51-63`
  （`build_snapshot_backend`）：目录写模式为 `postgres` 直接 `bail!`（293 行不允许这个后端提供
  服务）；`build_central_catalog`（285-314 行）连的是
  `config.cluster.scheduler_endpoint`（gRPC），不是数据库 DSN。
- **node 侧调用清单**（`56de492:src/node_server/service.rs`）：
  - 840 行、306 行：`.snapshots.load_runnable(...)`——创建沙箱、模板构建解析 base ref，均为读。
  - 193-239 行（`stage_for_caller`）、341-371 行（`build_template_impl`）：调
    `stage_captured`/`stage`，注释明写"an operator reconciling `snapshots/` against the catalog
    has nothing"、"only the caller's `try_start_build` row does"——只写字节，不写目录行。
- **SnapshotManager 的读写分工**（`56de492:src/snapshot/manager.rs`）：
  - `211-230` 行：`publish`/`publish_captured` 才调用 `commit_and_advertise`（写目录行）；
  - `265-295` 行：`stage_captured` 只调 `self.repository.stage(...)`，不碰目录；
  - `620-646` 行：`try_start_build`/`renew_build_lease`/`mark_build_error` 是目录写方法，node 侧
    代码里没有任何调用点（已在 `node_server/service.rs` 全文 grep 确认）。
- **暂停发布不经过 node 的 catalog**：`56de492:src/api/impls/paused_coordinator.rs:436-438`：
  ```go
  if !self.registry.is_cluster_backed() {
      return self.unrecorded(sandbox_id, Unrecorded::NoClusterRegistry);
  }
  ```
  `--role node` 用的是 `DisabledPausedSandboxRegistry`（`56de492:src/bin/server.rs:743-747`），
  `is_cluster_backed()` 为 false，所以 `publish_captured`（目录写）在 node 角色下**永远不会被调用**——
  与 `assemble_node` 744 行注释"a pause here stays durable through the node-local persister"一致。
- **链路对应**：`56de492:services/scheduler/internal/catalog_service.go:137-531` 的方法名
  （`BeginSnapshot`/`CommitSnapshot`/`FailSnapshot`/`DeleteSnapshot`/`GetSnapshot`/`ListSnapshots`/
  `ResolveAlias`/`StartBuild`/`RenewBuildLease`/`GetBuild`）与 `central/mod.rs` 的方法一一对应。
- **node→api 面不存在**：`56de492:src/cfg.rs` 的 `ClusterConfig` 只有
  `scheduler_endpoint`（node/api → scheduler）与 `node_service_addr`（api → node），没有任何
  "api 的地址"配置项；`56de492:src/orchestrator/service.rs` 全文没有 create 前解析快照记录的
  代码（`RemoteSandboxBackendFactory::build` 直接把原始快照引用透传给 node，见
  `56de492:src/node_client/factory.rs:49-59`），说明 api 今天确实还没有做这层预解析，改造量
  不是零，但只涉及现有 api→node 消息扩字段，不涉及新服务/新端口/新发现机制。

### 对施工的影响
Stage B 不需要在范围内新建 node→api RPC 面这项额外工作量（原本被视为"范围内的额外工作量"），
只需要把两个读取点（创建、远程构建）的目录解析责任从 node 挪到 api 的 dispatch 代码里，
是一次中等大小、局部的改动，不是新起一条网络面的工作量。

---

## Q4 — `scheduler_endpoint` 的消费者与热改覆盖面

### 结论
`4456481` 只让 `ObservabilityReporter`（心跳 + `ReportSandboxEvent` 批量上报共用同一个
`SchedulerChannelSource`）热改。逐点找全后，实际还有 **5 处**独立的、"进程启动时 `connect_lazy`
一次、之后一直持有"的消费者（原始设想是 3 处，实测是 5 处，因为"resume 放置客户端"和
"paused-registry/catalog 的 central 客户端"这两个类别里各自都是两个独立对象）。`cfg.rs` 里
`scheduler_endpoint_file` 字段的文档注释已经原生列出了这个缺口范围，与本次点验结果一致。

### 证据（逐点，均为 `56de492:` 下路径）
1. **心跳 + 事件上报**（已覆盖）——`src/observability/reporter.rs:545-663`
   （`SchedulerChannelSource`）：`current()` 每次心跳 tick 都会 `stat` 一次
   `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT_FILE`，文件变了就重建 `Channel` 并原子替换。
2. **P2P 发现** —— `src/p2p/discovery/scheduler.rs:30-70`（`SchedulerPeerDiscovery::start`）：
   `channel` 在 `start()` 里建一次，塞进 `SchedulerClient`，`refresh_scheduler_peers` 后台循环
   （150-185 行）复用同一个 `client`，从未重新读 endpoint。调用点：`src/p2p/mod.rs:43-59`
   （`peer_discovery_from_config`），进程启动时调一次。
3. **paused-registry central 客户端** —— `src/orchestrator/paused_registry/mod.rs:583-609`
   （`build_paused_registry`）：`CentralPausedSandboxRegistry::connect_lazy(endpoint, ...)`，
   `build_paused_registry` 只在进程装配时调一次。
4. **snapshot catalog central 客户端** —— `src/snapshot/repository/backends/mod.rs:285-314`
   （`build_central_catalog`）：`CentralSnapshotCatalog::connect_lazy(endpoint, ...)`，由
   `build_snapshot_backend`→`SnapshotManager::new()` 在进程启动时调一次（`src/bin/server.rs:461`
   `assemble_node_core` 与 `1002` 行 `assemble_api` 各自建一个实例，两边都不会重连）。
5. **创建放置客户端** —— `src/bin/aenv-api.rs:1146-1166`（`cluster_placement`）：
   `SchedulerNodePlacement::connect_lazy(endpoint, ...)`，`assemble_api` 装配时调一次，覆盖
   `Schedule`/`LookupNode`（一跳解析）/`RecordAssignment`/`GetNode`。
6. **resume 放置客户端** —— `src/api/impls/resume_surface.rs:243-301`
   （`ResumeWiring::from_config`/`cluster_from_config`）：`Endpoint::from_shared(...).connect_lazy()`
   建一个 `channel`，包进 `SchedulerPlacementSource`（358-366 行），只在装配时建一次，是与
   `cluster_placement` 完全独立的第二个 `Channel`。
- **官方确认**：`src/cfg.rs:890-906`（`scheduler_endpoint_file` 字段文档）：
  > Only this field's own consumer, `crate::observability::reporter`, reads it. It is not a
  > second way to reach the scheduler for `[cluster].scheduler_endpoint`'s other consumers
  > (P2P, the paused sandbox registry's `central` backend, resume placement) — those still read
  > the static value and still require a restart to change.
  这段注释列出的三类与本次点验一致，只是没有单独点名 snapshot catalog 客户端（第 4 项）和
  创建放置客户端（第 5 项，与 resume 放置客户端是两个不同对象）——这是本次调研比该注释更细
  的地方，供排期时不要漏项。
- 非消费者（排除）：`src/sandbox/access.rs:126` 只是 `.is_some()` 布尔判断，用于决定是否打印
  envd seed 警告，不建立任何到 scheduler 的连接。

### 最小改动方案（未实现，仅估算）
把 `SchedulerChannelSource`（`reporter.rs:545-663`，逻辑自包含，只依赖
`tonic::transport::{Channel,Endpoint}`/`Mutex`/`PathBuf`/`SystemTime`/一个 metrics 计数器）从
`reporter.rs` 提出到一个共享模块（如 `src/cluster_channel.rs`），把 `struct` 和 `fn current`
标 `pub`，metric 名加一个 `consumer` label 区分来源。之后 5 个消费者分别把"启动时建好的
`Channel`/`SchedulerClient`"字段换成 `Arc<SchedulerChannelSource>`，在每次真正发起 RPC 前调
`.current()` 换取当前 channel 再 `SchedulerClient::new(channel)`——这个模式在 reporter.rs 自己
（373 行）和 `resume_surface.rs`（371 行 `SchedulerPlacementSource::locate`）里已经在用，唯一要
新增的是 P2P 发现的后台循环要把"每次 tick 用当前 client"这件事接进 `refresh_scheduler_peers`。
五处改动都是同一个模式的重复应用，量级是"一个新公共类型 + 5 处把字段类型换掉 + 补 5 组测试"，
不是重新设计。

### 对施工的影响
如果阶段 4 的"每块独立可回退"要求真的适用于全部 5 个消费者而不只是心跳，需要把这条最小改动
方案也排进阶段 4 的工作量里；如果决定只有心跳/事件必须热改（回退到旧 scheduler 时最需要立刻
切换的正是这两个），可以把另外 4 处标注为"重启生效，可接受"并明确记录，但需要在排期文档里
显式写清楚是哪种决定，而不是留白。

---

## Q5 — P2P artifact 索引在 N 副本 api 下的退化

### 结论
索引退化只会让请求**变慢**（更多情况落到全量/hint 轮询），不会让功能**错**——scheduler 侧的索引
查找从设计上就是一个可以失败、可以为空的加速路径，Rust 侧的调用方在索引未命中或报错时无条件
回退到全量 peer 轮询，而全量 peer 名单来自独立于索引的心跳/发现机制。建议按 Stage D 原判断接受
退化、只记录延迟回归指标，不必把索引搬进 Redis。

### 证据
- **实现位置与结构**：`56de492:services/scheduler/internal/store.go:502-514`
  （`InMemoryArtifactStore`）：纯内存 `map[artifactIndexKey]map[string]struct{}` + 一个容量
  1,000,000 的 LRU 淘汰策略（`516-532` 行），无持久化、无跨进程共享。`182-187` 行
  `ArtifactStore` 接口全仓库只有这一个实现——`redis_store.go` 里没有 Redis 版本
  （`BindingStore` 有 Redis 实现，`ArtifactStore` 没有），`main.go:100` 生产环境也只
  `WithArtifactStore(scheduler.NewInMemoryArtifactStore(...))`。
- **消费者与回退路径**：`56de492:src/p2p/iroh/transport.rs:451-483`（`lookup_with_hints`）：
  ```rust
  match self.peer_discovery.peers_for_key(key).await {
      Ok(peers) => { if let Some(d) = self.lookup_peers(peers, key).await { return Ok(Some(d)); } }
      Err(err) => debug!(..., "falling back to peer discovery"),
  }
  let peers = self.peer_discovery.peers_with_hints(hints).await?;
  if let Some(d) = self.lookup_peers(peers, key).await { return Ok(Some(d)); }
  Ok(None)
  ```
  索引查找（`peers_for_key`，走 `LookupP2pArtifact` RPC）失败或没找到，无条件退到
  `peers_with_hints`；其默认实现（`56de492:src/p2p/discovery/mod.rs:19-21`）直接调
  `self.peers()`——即完整的已知 peer 集合（来自 `ListP2pPeers`/心跳发现，与 artifact 索引是两套
  完全独立的数据）。
- **索引不是正确性前提**：`peers_for_key`/`record_key`/`forget_key` 全部是"best effort"
  （`56de492:src/p2p/discovery/mod.rs:24-30` trait 注释），即便一直返回空，`lookup_with_hints`
  最终答案由 `peers_with_hints` 决定，不受索引状态影响。
- **外层还有一层回退**：按 CLAUDE.md 描述，P2P 传输本身在整个系统里就是"能加速就加速、失败就走
  对象存储/镜像"的可选层（"OSS resolver consumes fixed artifacts P2P-first with backend
  fallback"、"failed P2P publish does not roll back the repository"），即便
  `IrohBlobsP2pTransport::lookup_with_hints` 整体返回 `None`，上层调用方也不会因此功能失败。

### 对施工的影响
Stage D 可以维持"接受退化"的结论，不需要为 P2P artifact 索引单独设计 Redis 迁移；折叠 scheduler
进 api 之后，N 副本各自持有不完整索引会表现为镜像/快照层的 P2P 命中率下降、更多请求退回对象
存储或全量 peer 轮询,这是一个观测/告警项（建议监控索引命中率随副本数的变化），不是阻塞项。
