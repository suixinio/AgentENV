# Stage A：节点清册搬进 `api` —— 调研与施工计划

> 2026-08-25 · 只读调研产出。范围：五段折叠计划（A 节点清册 / B catalog / C paused registry /
> D 路由三段式+binding store+指标 / E 心跳目标切换+scheduler 下线）里的 **Stage A**。
> 背景文档：[`2026-08-20-service-decomposition.md`](2026-08-20-service-decomposition.md) §7 阶段 4、
> [`2026-08-20-module-responsibilities.md`](2026-08-20-module-responsibilities.md) §4.2/§4.3/§5。
> 硬约束：每块独立可回退、`--role all` 字节等价、不再往 `services/scheduler/` 加新 gRPC 面、
> 新集群级事实优先落 api 已持有的 Redis。

## 结论摘要

Stage A 比"~1,481 行"这个数字暗示的要大，但仍然是五块里最独立、风险最集中在**新依赖**而不是
**语义正确性**的一块。1,481 是 `2026-08-20-service-decomposition.md` v7 给的估算，只覆盖五个文件
（`node_registry.go` 824 ＋ `kubernetes_discovery.go` 379 ＋ `filter.go` 118 ＋ `strategy.go` 60 ＋
`warmup.go` 100，逐行核对后精确等于 1,481，本次复核无误）。但这个估算漏了两块 Stage A 绕不开的代码：
`node_registry.go` 硬依赖的 `cpu_template.go`（300 行，CPU 交集算法，用户点名"不能断"的那条链路）从未被
数进五个文件；`service.go` 里挂 `Heartbeat`/`ListNodes`/`GetNode`/`ListObservedNodes`/`UnregisterNode`
这五个 RPC 方法体本身（约 130 行）也不在内。把这些加回去，Stage A 的真实非测试代码量在 **1,900 行上下**，
配套 Go 测试约 2,000 行。

能不能独立做：**能**，接口边界干净——`scheduler.Service` 的构造函数把 `NodeRegistry` / `Strategy` /
`BindingStore` 当成三个正交参数，Stage A 只碰第一个和 `strategy.go`；`BindingStore`（Stage D）、
`internal/registry`（Stage C，4,044 行）、`internal/catalog`（Stage B，3,613 行）字面上一行都不用碰。
Rust 侧的落点也已经就位：`services/api/proto/scheduler.proto` 是 Go 和 Rust 共享的唯一 proto 源，
Rust 的 `build.rs` 已经从同一份文件生成 `crate::proto::scheduler`，所以 `Node`/`NodeStatus`/
`HeartbeatRequest`/`ObservedNode`/`MachineInfo` 这些线上类型不用手搬，直接复用生成代码。

最大的风险不是"port 错了算法"，是三件事：
1. **新依赖**——`kubernetes_discovery.go` 用的是 in-cluster `client-go` informer，Rust 侧目前
   `Cargo.toml` 里没有任何 k8s 客户端 crate，`agentenv-api-deployment.yaml` 也没有 `serviceAccountName`
   （用默认 SA，无 RBAC）。这是 Stage A 最大的单项工作量，不是算法搬运。
2. **心跳还没改目标**——Stage E 才切心跳目标到 api（见任务背景），但 Stage A 要让 api 的节点清册在
   `--role all` 之外的部署里真正"热"起来，就需要在心跳数据到位之前想清楚：api 的新清册在 Stage A
   期间靠什么喂数据（本文档 §5 给出的建议是双心跳，但这是一个需要用户裁决的开放决策，不是既定事实）。
3. **两个入口都要改，不只是 api**——`ListP2PPeers`/artifact RPC 不止 api 在用，`--role node` 自己的
   `src/p2p/discovery/scheduler.rs`（265 行）也直连 scheduler 做 P2P 对等发现。这条路径与 Stage D 的
   `store.go`（`ArtifactStore`）纠缠，Stage A 明确不动它，但必须在文档里说清楚，否则容易被误当作
   "顺手一起搬了"。

---

## 1. Go 侧现状：节点清册的真实边界

### 1.1 逐文件行数核对（`services/scheduler/internal/`，`wc -l`，非测试）

| 文件 | 非测试行数 | 对应测试文件 | 测试行数 | 计入 1,481？ |
|---|---:|---|---:|---|
| `node_registry.go` | 824 | `node_registry_test.go` + `node_registry_roster_test.go` | 582 + 258 = 840 | 是 |
| `kubernetes_discovery.go` | 379 | `kubernetes_discovery_test.go` | 505 | 是 |
| `filter.go` | 118 | `filter_test.go` | 263 | 是 |
| `strategy.go` | 60 | `strategy_test.go` | 25 | 是 |
| `warmup.go` | 100 | `warmup_test.go` | 154 | 是 |
| **小计** | **1,481** | | **1,787** | |
| `cpu_template.go` | 300 | `cpu_template_test.go` | 302 | **否**（`node_registry.go:334` 硬依赖 `IntersectCpuConfigs`） |
| `service.go`（仅 5 个 RPC 方法体 + `RunObservedNodesMetrics`/`refreshObservedNodesMetrics`/`isKnownNode`） | 约 130 | `service_registry_test.go`（部分）+ `service_test.go`（部分） | 未单独拆分 | **否**（RPC 挂载点，见 1.2） |
| `metrics.go`（仅 `schedulerObservedNodes` 与 `recordObservedNodes`） | 约 20 | 含在 `metrics_test.go` | 未单独拆分 | **否** |
| `types.go`（`Node`/`RichNode` 别名） | 25 | —— | —— | 否（Rust 侧无需搬，见 §2.5） |

`types.go` 的 `Node = routing.Node` 是从 `services/shared/routing` 借来的类型别名——Rust 侧不用管，
因为 Rust 从 `.proto` 直接生成对应结构体。

### 1.2 五个文件之外，Stage A 绕不开的代码

- **`cpu_template.go`**：`node_registry.go:334` 的 `computeIntersectionLocked` 直接调用
  `IntersectCpuConfigs`（`cpu_template.go:44`）。这正是用户点名的第二条已知风险——心跳里的
  `MachineInfo.cpu_config_json`，全集群按位 AND 交集后回填到心跳响应，节点用它调 Firecracker
  `PUT /cpu-config`。不搬这 300 行，CPU 交集这条链路就断在 Stage A 里。算法本身是纯函数
  （JSON in → JSON out，无状态），是全篇风险最低、可以逐行照搬的一块。

- **`service.go` 里的五个 RPC 方法体**：
  - `ListNodes`（`service.go:440-450`，约 11 行）：`s.nodes.Snapshot(true)` 直接转 proto，无副作用。
  - `Heartbeat`（`service.go:514-552`，约 39 行）：**这是 Stage A 与 Stage D 唯一的真实耦合点**——
    见 §7 风险 1，`s.nodes.Heartbeat(...)` 之后紧跟着 `s.store.ReconcileNode(node, roster, now)`，
    后者是 `BindingStore` 接口方法（`store.go:17`），Stage D 的地盘。
  - `ListObservedNodes`（`service.go:722-727`，约 6 行）＋ `RunObservedNodesMetrics`/
    `refreshObservedNodesMetrics`（`service.go:700-720`，约 17 行，周期性把 `ListObserved` 结果灌进
    Prometheus）。
  - `GetNode`（`service.go:800-812`，约 13 行）：`s.nodes.GetObserved(...)`，无副作用。
  - `UnregisterNode`（`service.go:814-847`，约 34 行）：同样先调 `s.nodes.UnregisterObserved(...)`
    （Stage A），再调 `s.store.ReconcileNode(Node{ID: nodeID}, nil, now)` 清路由绑定 ＋
    `s.artifacts.ForgetNode(nodeID)` 清 P2P 索引（两者都是 Stage D 的 `store.go`）。
  - `isKnownNode`（`service.go:997-999`，约 3 行）：`Schedule` 用来给候选节点做存在性检查，Stage A
    的 `NodeRegistry.Resolve` 之上一层薄封装。

- **`metrics.go`** 里只有 `schedulerObservedNodes`（Gauge，按派生状态计数）和 `recordObservedNodes`
  两处属于节点清册；`metrics.go` 剩下 800 多行是 lookup/binding/registry/legacy-roster 的指标，
  明确不属于 Stage A。

### 1.3 明确排除的文件（Stage B / C / D 的地盘，Stage A 不碰）

| 文件 | 行数 | 属于 |
|---|---:|---|
| `internal/catalog/`（整包） | 3,613 | Stage B |
| `internal/registry/`（整包） | 4,044 | Stage C |
| `store.go`（`BindingStore`/`ArtifactStore` 接口与内存实现） | 689 | Stage D |
| `redis_store.go`（`BindingStore` 的 Redis 实现，含 `ReconcileNode` 的 Lua 脚本） | 663 | Stage D |
| `lookup.go`（`LookupNode` 三段式：binding → roster → registry） | 574 | Stage D |
| `reconcile.go`（paused registry 对账，`RunRegistryReconcile` 等） | 648 | Stage C |
| `registry_service.go` | 978 | Stage C |
| `catalog_service.go` | 975 | Stage B |
| `sweep.go`（心跳超时后的路由绑定清理） | 383 | Stage D |

`reconcile.go` 尤其容易和"节点清册"混淆——文件名像，且确实读心跳数据——但它对账的是
`paused_sandboxes` 表（`internal/registry`）与心跳 roster 的差异，属于 Stage C。Stage A 的
`node_registry.go` 只留存 roster 本身（`RosterOf`/`NodesHolding`/`RostersInCluster`），不做任何
对账判定。

---

## 2. Rust 侧现状

### 2.1 `src/observability/`

`reporter.rs`（发心跳的一侧，跑在 `--role node`/`--role all`）用 `tonic::transport::Channel` 直连
`[cluster].scheduler_endpoint`，调 `scheduler::scheduler_client::SchedulerClient::heartbeat`。
`ObservabilitySchedulerReportConfig.scheduler_endpoint_file` 已经支持"心跳目标可热改"——这是
Stage E "心跳目标改指 api" 要用的机制，Stage A 不用碰，但值得记录：**改心跳目标从来不需要滚动重启**，
已经是现成能力。

`src/api/impls/admin.rs` 的 `nodes_get`/`nodes_node_id_get`（`ObservabilityService::node_snapshot()`）
是**单机自描述**接口——`nodes_get` 永远只返回 `vec![models::Node::from(node)]` 一条，`nodes_node_id_get`
里 `path_params.node_id != observability.node_id()` 直接拒绝查询别的节点。这不是集群清册，是每个
`--role api`/`--role all`/`--role node` 副本自己的 `/nodes` 自描述端点，Stage A 不用改它，但不要
把它误当成"api 已经有的重复清册"（详见 §2.4）。

### 2.2 `src/node_client/`（api → scheduler / api → node 的出口）

- `placement.rs`：`NodePlacement` trait，四个方法——`place_new`（新建放哪）、`place_existing`
  （已存在的沙箱去哪，走 binding cache）、`resolve_node`（已知身份、只问地址，直接查 discovery 不查
  binding cache）、`record_placement`（写"沙箱去了哪"）。
- `scheduler_placement.rs`（749 行）：`SchedulerNodePlacement`，`NodePlacement` 唯一的当前实现，
  四个方法分别对应 `Schedule`、`LookupNode`、`GetNode`、`RecordAssignment` 四个 RPC。**Stage A 的
  落点就是这个文件里的 `resolve_node`**（`scheduler_placement.rs:159`）——它是唯一一个纯粹只依赖
  节点清册、不碰 binding/routing 语义的方法。`place_existing`/`place_new`/`record_placement` 三个
  都要碰 binding store 或 strategy 选择，横跨 Stage A/D，Stage A 单独动这一个方法时要小心别把
  trait 签名拆散。
- `stub.rs`（1608 行）：`resolve_node` 的两处调用点——`reopen`（origin 节点回退路径）与
  `reresolve_placement`（连接失败后的重连，见 §7 风险 1 展开）。

### 2.3 `src/node_server/`

api → node 的驱动侧（`SandboxService`），与节点清册无关，Stage A 不碰。

### 2.4 api 是否已有"重复清册"

**没有字面意义上的重复。** 现状是 `--role api` 完全没有本地节点状态——`assemble_api`
（`src/bin/server.rs:939`）里 `debug_assert!(!role.sends_heartbeats())`，`ObservabilityService`
只描述自己（§2.1），`cluster_placement`（`src/bin/aenv-api.rs:1148`）无条件构造
`SchedulerNodePlacement`，每次都要打一次 RPC。

用户任务描述里"消掉重复清册"更准确的读法是**前瞻性的**，出自
`2026-08-20-service-decomposition.md` 阶段 4 一节："阶段 3 之后 api 已经需要节点清册来解析
node endpoint。保留 scheduler 意味着两个进程各自维护同一份状态"——这句话描述的是**折叠完成后**
如果不做 Stage A，api 为了给用户答复"节点在哪"就必须自己长出一份对节点身份/健康的理解（哪怕只是
一层缓存），而 scheduler 那份还在，于是变成两份。Stage A 的产出物本身，在切换开关打开之前，
会短暂制造一份**有意为之、behind 一个开关、只为回退保留**的第二份清册（api 的新实现 + scheduler
原有实现同时存在）；这份"重复"在 Stage E scheduler 下线时才真正消失。这是本次调研对这句话的最佳
解读，不是已确认的既定设计——已列入 §11 待确认。

### 2.5 proto 复用

`services/api/proto/scheduler.proto` 是唯一的 schema 源，Go 和 Rust 都从它生成代码
（Rust 侧 `build.rs:5-20` 用 tonic-build，生成到 `crate::proto::scheduler`）。`Node`、`NodeStatus`、
`ObservedNode`、`MachineInfo`、`HeartbeatRequest`、`HeartbeatResponse` 这些线上类型 Rust 已经有，
Stage A 不需要手写等价结构体，只需要决定新代码内部用生成类型还是再包一层。

### 2.6 `src/orchestrator/store/redis/`（新集群级事实的落点）

`keys.rs` 现有的 keyspace 完全是沙箱生命周期记录——`sbx:<id>`、`index`、`expiry`（ZSET）、
`txn:<id>`、`lock:sbx:<id>`、`pending`（ZSET）、`reserve:<id>`、`:notify`（pub/sub 频道），
全部共享 `{prefix}:{global}:` 前缀和 `{global}` hash tag（为将来上 Redis Cluster 铺路）。
**目前一个字节都没有关于节点身份的 key。** 如果 Stage A（或 §5 的双心跳方案）要把心跳产生的
`ObservedNode`/CPU 交集写进 Redis，需要新增一组 key（例如 `node:<id>`、`nodes:index`、
`cpu_intersection:<cluster_id>`），建议挂在同一个 `KeySpace` 结构体上、复用同一个 `{global}`
tag——不要另起一个 Redis 连接池或前缀方案。

---

## 3. 一跳 RPC 指的是什么

不止一条，一共三条独立的直连 scheduler 路径，Stage A 只处理第一条：

1. **`--role api` → scheduler，`GetNode`**（`node_client/scheduler_placement.rs:159`，被
   `stub.rs:476` 的 `reopen` 和 `stub.rs:726` 的 `reresolve_placement` 调用）。这是节点清册意义上
   真正的"一跳"——已知节点身份、只想问当前地址，绕过 binding cache 直接查 discovery。§7 风险 1
   详细展开了这条路径存在的原因（binding cache 在滚动重启期间会给出旧地址）。
2. **`--role api` → scheduler，`Schedule`/`LookupNode`/`RecordAssignment`**（同一个
   `SchedulerNodePlacement` struct 的另外三个方法）。这三个都要读 binding store 或跑
   placement 策略，Stage A **不处理**，属于 Stage D（binding store）与 Stage A/D 的交界——
   `Schedule` 内部的 `selectNode`/`FilterUnschedulable`/`FilterByResourceLimit`/策略选择虽然算法在
   Stage A 范围（`filter.go`/`strategy.go`），但 RPC 入口和 `RecordAssignment` 写路径在 Stage D。
3. **`--role node` → scheduler，`ListP2PPeers`/`RecordP2PArtifact`/`ForgetP2PArtifact`/
   `LookupP2PArtifact`**（`src/p2p/discovery/scheduler.rs`，265 行，`SchedulerPeerDiscovery`）。
   这条路径跑在 **node** 角色上，不是 api，且 `ListP2PPeers` 虽然读的是 `node_registry.go`
   的数据（`ListP2pPeers`/`FilterP2pPeers`，在 1,481 行范围内），但 `RecordP2PArtifact`/
   `ForgetP2PArtifact`/`LookupP2PArtifact` 读写的是 `store.go` 的 `ArtifactStore`（Stage D）。
   Stage A **明确不碰这条路径**——见 §10。

---

## 4. gateway 侧的耦合

`services/gateway/internal/node_list.go`（221 行）实现 `GET /nodes`（调 `ListObservedNodes`）
和 `GET /nodes/{id}`（调 `GetNode`）；`cluster_list.go`（433 行）是另一个端点，先调 `ListNodes`
拿节点表，再对每个节点发 REST 扇出请求（大概率是集群级沙箱列表，未在本次调研中确认具体路由，
见 §11）。两者都是 gateway 自己的 Go 代码，独立于 scheduler 的 `AtomicNodeRegistry`，纯粹通过
RPC 转发。

目标架构（`2026-08-20-module-responsibilities.md` §4.3）写明：**"`阶段 2` 之后集群列表是一条
SQL，不再扇出"**，并把 `internal/{node,cluster,registry}_list` 标为**删除**。但这句话的直接前提
是 Stage B（catalog 进 PG），不是 Stage A——`node_list.go` 依赖的是心跳/发现状态，不是目录数据，
不会因为 catalog 进 PG 就变成"一条 SQL"。这里目标架构文档本身可能对 `node_list.go` 和
`cluster_list.go`（大概率是沙箱清单，目录数据）一视同仁地写了"删除"，但没有分开论证——**这是
一处需要在排期前澄清的文档缺口**，本次调研判断：

- `cluster_list.go`（如果确实是沙箱清单）在 Stage B 之后可能真的变成一条 SQL，可以在那时删除。
- `node_list.go`（`GET /nodes`/`GET /nodes/{id}`，节点身份/健康）**不会因为 catalog 进 PG 而消失**，
  它依赖的数据（心跳、发现状态）永远不会落进 PG catalog；Stage A 完成后，它的落点应该从
  "转发到 scheduler 的 `ListObservedNodes`/`GetNode`" 改成"转发到 api 的等价 REST/gRPC 端点"，
  而不是被删除。

**建议不在 Stage A 里改 gateway。** Stage A 的开关只切 api 内部怎么解析节点地址（§5），
不改变 api 对外暴露的接口；gateway 继续按老样子打 scheduler 的 `ListObservedNodes`/`GetNode`
（scheduler 进程仍在跑，行为不变）。等 Stage E 决定"scheduler 真的要下线"时，`node_list.go`
两个 handler 的转发目标才需要跟着切到 api（届时 api 需要暴露一个等价的 `ListObservedNodes`/
`GetNode` REST 或 gRPC 面）。这样可以避免 Stage A 引入 gateway 侧的额外风险面。

---

## 5. 放置开关的设计

现有可复用的模式：

- **Go `--query-only` + `scheduler.redis_addr`**（`services/scheduler/cmd/main.go:35`）：一个
  flag 决定这个副本是否运行"只读、只答 `LookupNode`"模式，要求 Redis。这是 Stage 3（binding
  store 折 Redis）的先例，不是节点清册的先例——`QueryOnlyService` 完全没有 `NodeRegistry`
  （`cmd/main.go:315` 的注释直说"a query-only replica never reaches this. It has no node
  registry, no ..."）。可以类比但不能照搬。
- **Rust `[cluster].scheduler_endpoint` + `cluster_placement`**（`src/bin/aenv-api.rs:1148`）：
  这是 Stage A 真正要改的挂载点。今天它无条件构造 `SchedulerNodePlacement`；Stage A 要在这里
  加一个分支。

**建议的开关设计**：新增 `[cluster].node_placement_source`（或类似命名），取值
`"scheduler"`（默认，行为不变）/ `"native"`。

- `env = "AENV_NODE_PLACEMENT_SOURCE"`，加在 `ClusterConfig`（`src/cfg.rs:926`）上——**这个字段
  不需要 `#[config(nested)]`**，因为它是 `ClusterConfig` 自己的标量字段，不是新的嵌套段，所以
  可以直接给它 `env =` 属性，不会掉进"`[backend.oss]` 那种嵌套段只能靠
  `AENV_CONFIG_OVERLAY_PATH`"的坑——那个坑只影响新增 **`#[config(nested)]` 的整个子结构体**，
  不影响在已有的、本来就有 `env =` 字段的结构体里加一个新标量字段。
- `cluster_placement` 按这个值构造 `SchedulerNodePlacement`（现状）或新的
  `NativeNodePlacement`（Stage A 新增，实现同一个 `NodePlacement` trait，`resolve_node` 读本地
  注册表 + Redis，其余三个方法——`place_new`/`place_existing`/`record_placement`——在 Stage A
  阶段**继续委托给 `SchedulerNodePlacement`**，即 `NativeNodePlacement` 内部持有一个
  `SchedulerNodePlacement` 做 fallback，只有 `resolve_node` 真正走本地。这样开关粒度做到
  "只切一个方法"，风险面最小，且天然可回退——把配置值改回 `"scheduler"`，或者干脆不部署
  `NativeNodePlacement` 需要的发现/心跳前置条件，都能立刻回到今天的行为。

**回退动作**：改一个 TOML/环境变量、重启 api 副本（api 本来就是无状态多副本滚动更新，重启成本低，
不像 Stage 4 原方案里"节点心跳目标要热改否则得滚全部 node"那么昂贵）。scheduler 二进制本身完全
不需要动，天然满足"保留 scheduler 二进制一个 release"。

**心跳数据从哪来（本节最大的开放问题）**：`NativeNodePlacement`/api 本地注册表要回答
`resolve_node`，需要知道"节点当前在哪个地址"——这来自发现（k8s/static），不需要心跳。但如果
Stage A 还想验证"CPU 交集"这类心跳衍生数据的等价性（§7 风险 2），或者想让开关真正在生产流量上
被验证过而不是纸面正确，就需要心跳数据在 Stage E 之前就到 api。两个选项，都不是本次调研能替
用户拍板的：

- **方案一（推荐，但需要用户确认）**：给 `ObservabilitySchedulerReportConfig` 加一个可选的第二
  上报目标（小改动，`reporter.rs` 已经有 `SchedulerChannelSource` 这层抽象，加一路并发上报不难），
  节点在 Stage A 期间同时心跳 scheduler（继续权威）和 api（新清册，只读验证用）。Stage E 只是
  "去掉双报里的 scheduler 那一路"，不再是一次协同大切换。
- **方案二**：Stage A 只交付发现部分（k8s/static discovery 港进 api，且真实跑在生产里，风险已经
  验证），把"清册里的心跳衍生字段"（`ObservedNode`/`NodeSnapshot`/CPU 交集）整个留到 Stage E
  一次性接上——`resolve_node` 在 Stage A 期间即使切到 `"native"` 也只依赖发现，不依赖心跳新鲜度。
  代价是 Stage A 的"独立可回退"验证不到心跳链路，把风险推迟到 Stage E。

本文档倾向方案一，但这是一个需要用户/架构裁决的点，已列入 §11。

---

## 6. 测试面

### 6.1 Go 侧（`make -C services test`，覆盖 Stage A 的部分）

`node_registry_test.go`（582）、`node_registry_roster_test.go`（258）、
`kubernetes_discovery_test.go`（505）、`filter_test.go`（263）、`strategy_test.go`（25）、
`warmup_test.go`（154）、`cpu_template_test.go`（302）——全部不依赖数据库，`make -C services test`
（不需要 `test-with-postgres`）就能跑，已经确认这七个文件里没有 `t.Skip`/postgres 依赖。
`service_registry_test.go`（333）与 `service_test.go`（791，只有一部分与 Heartbeat/
ListNodes/GetNode/ListObservedNodes/UnregisterNode 相关，其余测 Schedule/RecordAssignment，
Stage D 范围）同样不需要数据库。

Stage A 落地后，这些测试**保留在 Go 侧不动**——只要 scheduler 二进制还在跑（回退目标），它们就
要继续证明 scheduler 自己的实现是对的。只有 Stage E scheduler 真正下线时才谈得上删除它们。

### 6.2 Rust 侧（`make test-unit`）

现有覆盖：`src/node_client/tests.rs`（4,545 行，`cargo test -p agentenv --lib` 的一部分）里对
`resolve_node` 有若干针对性用例，但用的是手写的 `NodePlacement` fake（`resolving_to`、
`answers_get_node` 之类），不是真的 `SchedulerNodePlacement` struct；`scheduler_placement.rs`
自己的 `#[cfg(test)] mod tests`（421 行起）跑一个内嵌的假 `SchedulerServer` 实现，对
`SchedulerNodePlacement::resolve_node` 做端到端（走真 tonic 序列化）验证，包含
`answers_get_node` 三种脚本化响应（成功两种 + `NotFound` 一种）。

Stage A 需要新增：
- `NativeNodePlacement`（或最终定名）自己的单元测试，仿照 `scheduler_placement.rs` 的模式——
  对本地注册表/Redis 读路径做端到端验证，覆盖"节点在/不在/身份重写过（alias）"三种情形
  （对应 Go 侧 `canonicalIDLocked` 的语义，见 §7 风险 4）。
- discovery（k8s/static）的移植测试，对应 Go 的 `kubernetes_discovery_test.go` 覆盖的场景——
  `Conditions.Serving` 门控、`Terminating` → lingering、pod selector 过滤、地址选择。这块如果用
  `kube` crate 的 mock/fixture 机制，工作量不小，是本 Stage 测试面里最重的一块。
- `cpu_template.go` 移植的黄金用例（`cpu_template_test.go` 302 行）——纯函数，直接把 Go 测试的
  输入/输出对逐条搬成 Rust 用例即可，性价比最高的一块测试移植。

`make test-unit` 跑 `--lib --bins`，覆盖 `agentenv` crate 全部——新代码只要放进 `src/` 就自动被
它捡到，不需要额外接线。

---

## 7. 风险与坑

1. **`Heartbeat`/`UnregisterNode` 都在同一次 RPC 里横跨 Stage A 和 Stage D**——
   `service.go:545` 的 `Heartbeat` 在 `s.nodes.Heartbeat(...)`（Stage A）成功后立刻调
   `s.store.ReconcileNode(node, roster, now)`（Stage D）；`lookup.go:150-169` 的热路径第一步
   直接返回 binding store 里的地址，这条地址**只有** `ReconcileNode` 会覆写。而
   `service.go:522-529` 的 `Heartbeat` 在 `errors.Is(err, ErrNodeNotInRegistry)` 时直接
   `return`，**跳过** `ReconcileNode`——滚动升级期间新 Pod 在 EndpointSlice 里还没被
   `kubernetes_discovery.go:312`（`endpoint.Conditions.Serving`）判成 serving 时，心跳就是这个
   下场：discovery 还不认这个节点，心跳打不进 registry，binding 也就刷新不了，`resolve_node`
   （走 discovery，不走 binding）能看到新地址但 `place_existing`（走 binding）看不到——这正是
   `stub.rs` 里 `reresolve_placement` 存在的理由。**Stage A 单独移植 `resolve_node`/发现逻辑时，
   这条竞态本身不会消失，只是移到了 api 内部**——`NativeNodePlacement` 如果也在"发现还没收敛"
   的窗口里被查询，答案依然可能是旧的或空的，只是不再需要一跳 RPC 才能看到。这不是 Stage A
   引入的新风险，是把已有风险从"scheduler 内部竞态"搬成"api 内部竞态"，测试要覆盖到。

2. **CPU 交集链路不能断**——心跳带 `MachineInfo.cpu_config_json`，`node_registry.go:334` 的
   `computeIntersectionLocked` 调 `cpu_template.go` 的 `IntersectCpuConfigs` 算全集群按位 AND，
   写回心跳响应的 `CpuConfigJson` 字段，节点收到后调 Firecracker `PUT /cpu-config`。这条链路
   完全在 Stage A 范围内（心跳处理 + node_registry 状态），但**不在 1,481 行的原始估算里**
   （§1.2）。移植时必须把 `cpu_template.go` 一起搬，且 `allConfigsReadyLocked`（只有当集群里
   "每个节点都上报过 cpu_config_json"才计算交集，避免用不完整集合算出错误的交集）这条门槛逻辑
   容易在重写时被简化掉——一旦被简化，行为就从"等到大家都报完"退化成"边到边算"，交集结果会
   随节点上报顺序抖动。

3. **`ListP2PPeers` 不要被顺手一起搬了**——它读的数据（`node_registry.go` 的
   `ListP2pPeers`/`FilterP2pPeers`）在 Stage A 范围内，但它的调用方是 `--role node` 的
   `src/p2p/discovery/scheduler.rs`，而且同一个 RPC 分组里的
   `RecordArtifact`/`ForgetArtifact`/`LookupArtifact` 读写的是 `store.go` 的 `ArtifactStore`
   （Stage D）。如果 Stage A 顺手把 `ListP2PPeers` 也切到 api，就会在 node 角色上新增一条
   "node → api"的 P2P 发现路径，同时 artifact 三个 RPC 还留在 scheduler，一次心跳分组的 P2P
   发现从"一个后端"变成"两个后端各管一半"，比不动它更容易出错。建议整组（`ListP2PPeers` +
   三个 artifact RPC）留到 Stage D 一起搬。

4. **身份别名（`canonicalIDLocked`）的语义容易在重写里丢掉**——`node_registry.go:173` 的
   `canonicalIDLocked` 把"调用方给的任意身份"（可能是 pod name，可能是节点自己上报的
   `AENV_NODE_ID`）映射到 discovery 当前认定的规范 ID，且顺序是"先查真实节点表，查不到才查
   alias 表"——这个顺序本身是保证："一个真实存在的节点永远解析成它自己，alias 表不可能反过来
   遮蔽一个真实节点"。`scheduler_placement.rs` 的文档注释里也提到这个场景（"a scheduler row was
   written by the node itself and may name the identity it reported under before a fleet
   upgrade renamed it"）。这类身份轴改动历史上在本仓库出过事故（见
   `identity-axis-consumers.md`/`benign-test-fixtures.md` 两条项目记忆：换了身份的写入方，
   会让每一个拿这个身份去比较的读取方悄悄失真，而写入侧自己的测试全绿）——Stage A 的测试夹具
   必须像那两条记忆建议的那样，claimant/holder（这里是"调用方给的身份" vs "discovery 规范身份"）
   用**结构不同**的字符串（一个 pod-name 形状，一个真实节点名形状），不能图省事让两边长得一样，
   否则测试测不出别名解析被写反的情况。

5. **k8s RBAC 缺口**——`deploy/k8s/base/agentenv-api-deployment.yaml` 没有 `serviceAccountName`
   字段（用默认 SA，零权限）；`endpointslices`/`pods` 的 get/list/watch 权限目前只挂在
   `agentenv-scheduler` 这个 SA 上（`role.yaml`/`rolebinding.yaml`）。Stage A 如果要让 api
   在 kubernetes 模式下真正跑 discovery，必须给 api 的 Deployment 配一个新 SA（或复用
   `agentenv-scheduler` 的 SA，但那样等于 api 和 scheduler 共享一个身份，回退期间两个进程用
   同一个 SA 读同一批 EndpointSlice，读权限本身没有冲突风险，但部署拓扑上更清晰的做法是给
   api 建一个新的、权限对等的 SA/Role/RoleBinding）——这是一个部署清单变更，不是代码变更，
   容易在"只看 Rust 代码"的评审里被漏掉。

6. **Rust 目前零 k8s 客户端依赖**——`Cargo.toml` 没有 `kube`/`k8s-openapi` 之类的 crate。
   Go 用的是 client-go 的 `SharedIndexInformer`（本地缓存 + 增量 watch + resync）；Rust 生态的
   对应物是 `kube` crate 的 `watcher`/`reflector`。这不是简单的逐行翻译，是选一个新依赖、验证它
   在这个 workspace 的构建/许可证/供应链约束下能用，工作量在"移植 kubernetes_discovery.go 的
   379 行代码"之外。

7. **`warmup.go` 的消费方在 Stage D，不在 Stage A**——`warmupGate` 由 `Heartbeat` 喂
   （`reportedIn`），但只被 `lookupNode`（Stage D）读。Stage A 移植 `warmup.go` 时，如果只是把
   数据结构搬过去而没有 Stage D 的消费方，这段代码在 api 里会是"写了没人读"的死代码，直到
   Stage D 落地才用得上。建议 Stage A 把 `warmupGate`-等价物做成一个独立、可被 Stage D 直接
   引用的类型（类似 Go 里已经做到的"`NodeRegistry` 是个独立接口"），但不强求 Stage A 自己接上
   消费方；在计划里明确写"这段代码 Stage A 完成时是死代码，属于预期"，避免评审时被当成 bug。

---

## 8. 迁移映射表

| Go 文件 | 行数 | Rust 落点（建议） | 处理方式 |
|---|---:|---|---|
| `node_registry.go` | 824 | `src/node_registry/registry.rs`（新模块） | 重写——数据结构和并发原语要按 Rust 习惯来（`Arc<RwLock<..>>` 或 `DashMap`），但 `Snapshot`/`Resolve`/`Heartbeat`/`ListObserved`/`GetObserved`/`PeekObserved`/`RosterOf`/`NodesHolding`/`RostersInCluster`/`UnregisterObserved` 这组方法签名和别名解析顺序（§7 风险 4）要逐条对应搬 |
| `kubernetes_discovery.go` | 379 | `src/node_registry/kubernetes_discovery.rs`（新模块，新依赖 `kube` crate） | 重写——`Conditions.Serving`/`Terminating` 门控、pod selector 过滤、地址选择这几条判定逻辑要保真；informer 换成 `kube::runtime::watcher`/`reflector` |
| `filter.go` | 118 | `src/node_registry/filter.rs` | 照搬——两个纯函数（`FilterUnschedulable`/`FilterByResourceLimit`），逻辑简单直接 |
| `strategy.go` | 60 | `src/node_registry/strategy.rs` | 照搬——`RoundRobinStrategy`/`RandomStrategy`，`AtomicU64` 换 Rust `AtomicU64` |
| `warmup.go` | 100 | `src/node_registry/warmup.rs` | 照搬数据结构，**消费方留给 Stage D**（§7 风险 7） |
| `cpu_template.go` | 300 | `src/node_registry/cpu_template.rs` | 照搬——纯函数，`cpu_template_test.go` 的用例可直接迁移做黄金测试 |
| `service.go`（5 个 RPC 方法体 + `RunObservedNodesMetrics`/`isKnownNode`） | ~130 | 视 §5 开关设计而定：`NativeNodePlacement`（`src/node_client/native_placement.rs` 或类似）的 `resolve_node`，以及（如果方案一心跳双报被采纳）一个新的、api 侧接收心跳的入口 | 重写，且**必须**在 `Heartbeat`/`UnregisterNode` 两处显式留一个 no-op 或 TODO 标记，指向 Stage D 要接上的 `ReconcileNode`/`ArtifactStore.ForgetNode` 调用（§7 风险 1） |
| `metrics.go`（`schedulerObservedNodes`/`recordObservedNodes`） | ~20 | 挂在现有 `metrics::gauge!` 体系下，新文件或并入 `src/observability/prometheus.rs` | 照搬语义，宏替换 |
| `types.go` | 25 | 不需要——直接用 `crate::proto::scheduler::Node` | 丢弃 |
| `store.go`/`redis_store.go`/`lookup.go`/`sweep.go` | 689+663+574+383 | —— | **不动**，Stage D |
| `reconcile.go`/`registry_service.go` | 648+978 | —— | **不动**，Stage C |
| `catalog_service.go` | 975 | —— | **不动**，Stage B |
| `internal/registry/`、`internal/catalog/`（整包） | 4,044+3,613 | —— | **不动**，Stage C / B |

---

## 9. 分步施工清单（每步独立可编译、独立可回退）

1. **纯函数先行**：移植 `cpu_template.go` → `src/node_registry/cpu_template.rs`，逐条搬
   `cpu_template_test.go` 的用例。不接入任何调用方，先让它作为一个独立、通过 `cargo test`
   验证的库函数存在。**回退**：删掉这个模块即可，没有任何东西依赖它。

2. **`filter.go`/`strategy.go` 移植**：同样先落地为独立、有测试覆盖的纯函数模块，不接调用方。
   **回退**：同上。

3. **`NodeRegistry`-等价物的数据结构**（`node_registry.go` 的核心部分，不含 discovery 和 RPC
   挂载）：定义 trait/struct，用静态节点列表（对应 Go 的"static discovery"分支）先跑通
   `Snapshot`/`Resolve`/`Heartbeat`/`ListObserved`/`GetObserved`/`UnregisterObserved` 这套接口，
   测试用手写的心跳请求驱动，不接真实网络。**回退**：新模块整体不参与任何现有装配路径，是否
   编译进二进制都不影响现有行为。

4. **k8s discovery**：引入 `kube` crate，实现 `kubernetes_discovery.rs`，独立于 §3 的注册表跑一遍
   watch/list，先只做"打日志、不接注册表"的空跑验证（类似 dry-run），在一个真实测试集群
   （k3s 开发集群）上确认 RBAC、EndpointSlice 过滤、`Serving`/`Terminating` 语义符合预期。
   同批需要的 `deploy/k8s/` 变更：给 api 的 Deployment 加 `serviceAccountName` + 对应
   `ServiceAccount`/`Role`（`endpointslices`/`pods` get/list/watch）/`RoleBinding`。
   **回退**：这一步只加代码和权限，不改变任何现有行为（api 仍然打 `SchedulerNodePlacement`），
   不部署新 SA/Role 就等于没发生过。

5. **接上 §3 和 §4**：discovery 的输出喂进注册表的 `Set`（对应 Go 的 `AtomicNodeRegistry.Set`）。
   仍然不接入任何用户可见路径。**回退**：同上，新代码路径未被引用。

6. **心跳数据来源（依赖 §5 的用户裁决）**：
   - 若选方案一（双心跳）：给 `ObservabilitySchedulerReportConfig` 加第二上报目标，`reporter.rs`
     并发发一路给 api 的新入口（本步骤需要 api 侧先有一个接收心跳的 RPC/HTTP 端点，实现
     `Heartbeat` 里 Stage A 范围的那部分逻辑——`s.nodes.Heartbeat(...)` 等价物，**不实现**
     `ReconcileNode` 那一半）。**回退**：第二上报目标配置项留空即可关闭，节点继续只报
     scheduler，行为与今天完全一致。
   - 若选方案二（延后到 Stage E）：跳过本步骤，`resolve_node` 在 Stage A 完成时只依赖发现，
     不依赖心跳新鲜度；`ObservedNode`/CPU 交集在 api 侧留空实现或直接不提供，直到 Stage E。

7. **`NativeNodePlacement` 实现 `resolve_node`**：`src/node_client/` 下新增实现，`resolve_node`
   读 §3/§5/§6 搭好的本地注册表；其余三个 `NodePlacement` 方法委托给内部持有的
   `SchedulerNodePlacement`。加 `[cluster].node_placement_source` 开关（默认 `"scheduler"`），
   `cluster_placement`（`src/bin/aenv-api.rs:1148`）按开关分流。**回退**：开关改回
   `"scheduler"`（或干脆不设置，用默认值），行为与 Stage A 之前完全一致，无需重新部署代码——
   只改配置、重启 api 副本（api 本来就滚动重启）。

8. **灰度验证**：在一个 api 副本上打开 `"native"`，其余保持 `"scheduler"`，对比两者对同一批
   `resolve_node` 请求的应答（可以先只做影子调用/日志对比，不真正采信 native 的答案），确认
   discovery 收敛速度、别名解析结果、CPU 交集内容（如果方案一）与 scheduler 一致。

9. **全量切换**：确认灰度无误后，`node_placement_source` 默认值改成 `"native"`（或在部署配置里
   显式打开）。**scheduler 二进制仍然继续跑**——因为 `--role node`（`ObservabilityReporter`，
   以及若采用方案一则继续跑到 Stage E）和 `--role node` 的 `SchedulerPeerDiscovery` 都还要用它；
   Stage A 结束不等于 scheduler 可以下线。

**每一步的编译/回退性**：第 1-6 步全部是"加新代码，不接入任何现有调用路径"，天然不影响现有构建
和运行时行为，`cargo build`/`make test-unit` 全程保持绿。第 7-9 步引入唯一的运行时开关，回退是
一次配置变更而非代码回滚。

---

## 10. 这次不做什么

- **不动 `BindingStore`/`RedisBindingStore`（Stage D）**：`ReconcileNode`、`Heartbeat` 里喂
  binding 的那一半、`LookupNode` 三段式、`sweep.go` 的心跳超时清理，全部留在 scheduler 不动。
  `Heartbeat`/`UnregisterNode` 在 Stage A 完成后，Rust 侧的等价实现里**不实现**
  `store.ReconcileNode`/`artifacts.ForgetNode` 对应的逻辑——这两半继续只在 Go scheduler 里跑，
  Rust 侧的心跳/注销入口（如果按方案一实现）只更新 Stage A 范围内的注册表状态。
- **不动 `internal/registry`（paused registry，Stage C）与 `internal/catalog`（Stage B）**，
  一行都不碰。
- **不搬 `ListP2PPeers`/artifact RPC 三兄弟**（§7 风险 3）——整组留给 Stage D 与 `store.go`
  一起搬，`src/p2p/discovery/scheduler.rs` 在 Stage A 结束时仍然直连 scheduler。
- **不改 gateway**（§4）——`services/gateway/internal/node_list.go`/`cluster_list.go` 继续打
  scheduler 的 RPC，不因为 api 有了本地注册表就跟着切。
- **不改节点的心跳目标**（除非采纳 §5 方案一的"双报"，且双报的第二路是新增而非替换）——单一心跳
  目标切换到 api、彻底停止心跳 scheduler，是 Stage E 的动作。
- **不给 `services/scheduler/` 加任何新 gRPC 方法**，符合硬约束——本计划里所有新代码都在 Rust
  一侧新增，Go 侧 scheduler 的 `.proto` 实现面保持不变。
- **不下线 scheduler 进程或删除它的 Deployment**——Stage A 结束时 scheduler 仍然是
  `--role node`（心跳，及可能的双报场景下的一路）和 P2P 发现的唯一后端，不能缩容。

---

## 11. 待确认事项（未能在本次调研中确认，不要当成既定结论）

1. **§5 心跳数据来源的方案一 vs 方案二**——本文档倾向双心跳（方案一），但这需要用户/架构裁决，
   涉及给 `ObservabilitySchedulerReportConfig` 新增能力，不是纯粹的"照搬 Go 代码"范围，需要
   明确排期前拍板。
2. **`cluster_list.go`（433 行）的确切路由和语义**——本次调研没有读它的完整实现，只从函数名和
   `ListNodes` 的用法推断它是"先拿节点表、再扇出 REST"的集群级列表（大概率是沙箱清单）。
   `2026-08-20-module-responsibilities.md` 把它和 `node_list.go` 一起标记"阶段 2 之后删除"，
   但没有分开论证——§4 给出的"`node_list.go` 不会随 catalog 迁移消失"的判断需要与
   `cluster_list.go` 的真实实现核对后再确认。
3. **`§2.4` 对"消掉重复清册"的解读**——本文档给出的是最贴合上下文的推断（Stage A 完成后到
   Stage E 之前，api 的新实现与 scheduler 的旧实现是"有意为之的重复，靠开关切换，回退用"），
   但用户任务原文本身没有明确这是不是就是想问的东西，值得在启动施工前用一句话向用户复核。
4. **api 新 SA 还是复用 `agentenv-scheduler` 的 SA**（§7 风险 5）——本文档建议新建一个权限对等
   的 SA，但两个选项都可行，需要部署侧拍板。
5. **`kube` crate 的具体版本/许可证/供应链评审**——本次调研只确认了"目前没有这个依赖"，没有做
   选型评审（`kube` vs 手写一个基于 `k8s-openapi` 的最小 watch 客户端）。
6. **Stage A 结束时是否需要给 api 暴露一个 gRPC/REST 面供 gateway 未来切换用**（§4 提到的
   "等 Stage E 时 `node_list.go` 需要转发目标从 scheduler 换成 api"）——本文档判断这个面
   应该留到 Stage E/D 再建，但没有和用户确认这条时间线是否可接受。
