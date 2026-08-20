# 模块职责与关联：e2b 地图 ＋ AgentENV 设计

> 2026-08-20 · 配套 [`2026-08-20-service-decomposition.md`](2026-08-20-service-decomposition.md)。
> 那份定的是**进程怎么拆、分几步**；这份定的是**每个模块负责什么、模块之间怎么连**。
>
> 考古基准：e2b `/home/debian/e2b-infra`。所有 `packages/...` 路径都指它。
> 行数均为**非测试**代码。
>
> **v4（2026-08-20）**：随拆分方案第三轮对抗审查同步更新。
> 新增 **D11（路由投影与活跃态 store 是两个结构）**；
> **D10 改了一半** —— CREATE / FORK 的投影写今天已经同步，不该改走事件通道；
> **§8.4 的配额那一支我们暂时用不上**（没有租户模型）；
> `src/cache/` 从阶段 3 提前到阶段 2；`gateway/internal/resume` 从阶段 1 推到阶段 3。

---

## 0. 三十秒版

e2b 的模块划分不是按「功能」切的，是按**谁拥有哪份真相**切的。读懂这一点，
它的进程边界、副本数、以及哪些东西必须放 Redis 就全是推论了。

从地图里读出六条原则（§3），其中三条我们今天是反的：

| 原则 | e2b | AgentENV 今天 |
|---|---|---|
| P2 真相在共享存储，进程只有缓存 | ✅ api 无本地权威状态 | ❌ `InMemoryMetadataStore` 是权威 |
| P3 跨副本协调走 Redis pub/sub | ✅ `publisher.go` ＋ `subscription_manager.go` | ❌ 进程内 broadcast channel |
| P6 对账是拉的 | ✅ `nodemanager.Sync → List → store.Reconcile` | ❌ 只有推（心跳），没有拉 |

§4–§5 是 AgentENV 的目标模块表与连边表；§6 是十一个设计决策；
§8 是活跃态 store 的四个缺口。

---

## 1. e2b 模块地图

### 1.1 `api` —— 决策层（N 副本，无本地权威状态）

| 模块 | 行数 | 负责领域 | 拥有的真相 |
|---|---|---|---|
| `internal/api/` | 17,910 | oapi-codegen 生成的 HTTP 骨架 | —— |
| `internal/handlers/` | 8,671 | 公开 REST 的每个端点实现 | —— |
| `internal/orchestrator/` | 5,427 | 集群编排总装（见下） | —— |
| └ `placement/` | 515 | 选节点：best-of-K ＋ **试到有人接为止** | —— |
| └ `nodemanager/` | 1,187 | 每个节点在 api 侧的代理对象：连接、状态、机器信息、labels、可达性、`Sync` 循环 | 进程内缓存（可重建） |
| └ `discovery/` | 441 | 节点发现：`local` / `nomad` / `kubernetes` / `merged` | —— |
| └ `evictor/` | 206 | 超时驱逐（**唯一的驱逐发起方**） | —— |
| └ 顶层 | —— | `lifecycle`（写路由 catalog）、`create_instance`、`pause_instance`（**发布翻牌**）、`autoresume`、`routing`、`snapshot_template`、`analytics` | —— |
| `internal/sandbox/` | 3,175 | **活跃沙箱 store**：四态状态机、预留、envd secret、别名 | **Redis** |
| └ `storage/redis/` | —— | Lua CAS、pub/sub、过期索引、锁、扫描、heal | Redis |
| └ `sandboxtypes/` | —— | `running` / `pausing` / `killing` / `snapshotting`——**没有 paused** | —— |
| └ `reservations/redis/` | —— | 并发配额预留 | Redis |
| `internal/clusters/` | 2,596 | 到 node 的 gRPC 客户端束（Info / Sandbox / Volume / Template 四个服务）＋ 远程集群同步 | —— |
| `internal/secretsstore/` | 1,369 | 密钥 | —— |
| `internal/analytics_collector/` | 969 | 事件采集 → ClickHouse | —— |
| `internal/template-manager/` | 952 | 构建调度（**api 侧**：队列、并发上限、状态查询） | PG `builds` |
| `internal/cache/` | 907 | **Redis 支撑的共享缓存**：templates / aliases / snapshots / sandboxcounts | Redis（派生） |
| `internal/template/` | 401 | 模板解析 | —— |
| `internal/middleware/` | 328 | 鉴权、限流（Redis） | —— |
| `internal/team/` `oauth/` `db/` `pause/` | 445 | 团队、OAuth、PG 连接、暂停辅助 | PG |

外加 `packages/db`（PG 目录：teams / envs / env_builds / snapshots / env_aliases）。

### 1.2 `orchestrator` —— 执行层（每节点，无数据库）

| 模块 | 行数 | 负责领域 |
|---|---|---|
| `pkg/sandbox/` | 32,551 | VM 生命周期、块设备、内存、网络、模板缓存、P2P peerclient、uploads |
| `pkg/template/` | 9,700 | 模板构建执行（`template-manager` 角色） |
| `pkg/nfsproxy/` | 4,600 | NFS 缓存代理 |
| `pkg/server/` | 2,849 | gRPC 服务端：`SandboxService` ＋ `ChunkService` |
| `pkg/factories/` | 1,240 | 依赖装配 |
| `pkg/volumes/` | 1,013 | 卷（`VolumeService`） |
| `pkg/tcpfirewall/` `chrooted/` `portmap/` | 1,858 | 网络与隔离 |
| `pkg/hyperloopserver/` | 621 | VM 内回调入口 |
| `pkg/metrics/` `healthcheck/` `service/` | 946 | 自我观测与角色声明 |
| `pkg/startupreclaim/` | 282 | **启动时回收残留 Firecracker 进程** |
| `pkg/proxy/` | 273 | 本机反向代理（沙箱数据面入口，端口 5007） |
| `pkg/scheduling/` | 110 | 产出 `SchedulingMetadata`（build 链亲和性提示）**上报给 api** |
| `pkg/events/` | 94 | 事件上报 |

### 1.3 `client-proxy` —— 转发层（N 副本，无状态）

| 模块 | 负责领域 |
|---|---|
| `internal/proxy/proxy.go` | `catalogResolution`：直读 Redis → 拿 `OrchestratorIP` → 转发到 `nodeIP:5007` |
| `internal/proxy/paused_*` | catalog 未命中 → 调 api 的 `ResumeSandbox` |

---

## 2. e2b 的八条边

| # | 边 | 协议 | 性质 | 谁是真相 |
|---|---|---|---|---|
| E1 | client-proxy → Redis | RESP | **每请求直读**，1s 超时，**无缓存** | Redis |
| E2 | client-proxy → api | gRPC，**只有 `ResumeSandbox` 一个 RPC**（`shared/pkg/grpc/proxy/proxy.proto:23`） | 冷路径 | —— |
| E3 | api → node | gRPC ×4：`SandboxService` / `InfoService` / `VolumeService` / `TemplateService`（`clusters/client.go`） | 命令 | 命令的接收方裁决 |
| E4 | api ← node | **拉**：`nodemanager.Node.Sync` 定期调 `List`，喂 `store.Reconcile(orphanCandidates, nodeID)` | 对账 | node 的实际持有 |
| E5 | node ↔ node | gRPC `ChunkService`（peer 供 build 块，绕过远端存储） | 数据 | 内容寻址 |
| E6 | api ↔ Redis | 活跃态 store（Lua CAS）、**pub/sub 跨副本状态变更**、共享缓存、限流、预留 | 权威 | Redis |
| E7 | api ↔ PG | 目录：teams / envs / builds / snapshots / aliases | 权威 | PG |
| E8 | node → 对象存储 | 字节（build 层、内存快照） | 内容寻址 | —— |

外加发现边：api → `nomad` / `kubernetes` / static（`discovery/`）。

🔴 **注意 E4 的方向。** 不是节点推心跳，是 **api 拉**。同一条 gRPC 连接既下命令又做对账，
且 api 副本重启后立刻能对账，不必等下一个心跳。

🔴 **E2 只有一个 RPC。** 数据面对控制面的全部依赖，就是「这个沙箱暂停了，帮我恢复」。
其余一切数据面流量不经过控制面。

---

## 3. 从地图里读出的六条原则

### P1 节点是容量的最终裁决者，控制面只做优选

`placement/placement.go` 的 `PlaceSandbox` **不是**「算出最优解然后提交」，而是
**逐个候选节点真的去 `Create`，直到有人接**：

- 容量不足的节点返回**快速 `ResourceExhausted` 拒绝**（`placement.go:85`：
  「Nothing but capacity refusals before the deadline is capacity, not a slow placement」）；
- 成功之后 api **乐观更新**本地节点指标（`placement.go:142-143`：
  「Optimistic update: assume resources are occupied after successful creation」）。

⇒ **多副本放置竞态不需要共享状态解决。** 两个副本同时选中节点 X，X 接一个、拒一个，
被拒的那个换下一个候选。api 侧的节点指标只是提示，错了不会导致超配。

### P2 真相在共享存储，进程只有缓存

`api` 没有任何本地权威状态：活跃态在 Redis，目录在 PG，节点视图（`nodemanager`）
是可从 `discovery` ＋ `List` 重建的缓存。**这是它能有 N 副本的前提**，
不是它多副本之后才做的适配。（前提，不等于充分 —— 还要 §8 那四个原语。）

### P3 跨副本协调走 Redis pub/sub，不走进程内 channel

`sandbox/storage/redis/publisher.go` ＋ `subscription_manager.go`：
「maintains a Redis PubSub connection and fans out storage notifications to
registered in-process waiters」。`WaitForStateChange` 因此能跨副本工作 ——
副本 A 上的等待者能被副本 B 的状态变更唤醒。

### P4 控制面对数据面只暴露一个 RPC

见 E2。**接口面越小，数据面对控制面的可用性依赖就越短。**

### P5 节点不持有数据库

`packages/orchestrator` 全仓 grep `pgxpool` / `sqlc` / `queries.` **零命中**。

> ⚠️ **一处诚实的例外**：`pkg/sandbox/uploads.go` 有 `redis.UniversalClient`，
> 用途仅是 `uploadDoneChannel(buildID)` 的 pub/sub —— 一条**通知**信道，不写权威状态。
> 所以 e2b 的原则准确表述是「节点不拥有真相」，而不是「节点碰不到共享设施」。

### P6 对账是拉的，不是推的

见 E4。推（心跳）表达的是「节点声称自己有什么」，拉表达的是「控制面去问节点实际有什么」。
孤儿判定必须基于后者。

---

## 4. AgentENV 的模块设计

### 4.1 `api` 角色（Rust，N 副本，镜像 `agentenv-runtime --role api`）

| 模块 | 状态 | 负责领域 | 拥有的真相 | 对应 e2b |
|---|---|---|---|---|
| `src/api/generated` ＋ `src/api/impls/` | 已有 28,520 | 公开 REST 全量 | —— | `internal/api` ＋ `handlers` |
| `src/orchestrator/service.rs` | 已有，改造 | 生命周期状态机、fork、counters | 委托给 store | `internal/orchestrator` 顶层 |
| `src/orchestrator/store/redis.rs` | 🆕 **新建，且是唯一生产实现** | 活跃沙箱 store：状态机 CAS（Lua）、TTL、过期索引 | **Redis** | `sandbox/storage/redis/` |
| ~~`src/orchestrator/store/in_memory.rs`~~ | ❌ **删除**（D7） | —— | —— | e2b 无对应物 |
| `src/orchestrator/events/` | 已有，改造 | 生命周期事件；传输改 Redis pub/sub，进程内 broadcast 降为最后一跳 fan-out | Redis | `storage/redis/publisher.go` |
| `src/orchestrator/store/redis_cas.rs` | 🆕 **新建** | `update_state_if_state` / `update_if_state` / `wait_while_in_states` 的 Redis 实现（Lua ＋ pub/sub 唤醒） | Redis | `storage/redis/scripts.go`、`state_change.go` |
| `src/orchestrator/store/lock.rs` | 🆕 **新建，范围比想象小** | 只用于 CAS 表达不了的两处：跨多次往返的读-判断-写、TTL 有界的崩溃恢复（见 D9） | Redis | `sandbox/storage/redis/lock.go` |
| `src/orchestrator/store/reserve.rs` | 🆕 **新建** | 并发 create 去重：已存在时返回「等第一个的结果」而不是报错（🔴 我们只有**三态**，见 8.4） | Redis | `sandbox/store.go:157`、`reservations/redis/` |
| `src/orchestrator/routing_projection.rs` | 🆕 **新建** | 🔴 **路由投影** —— 五字段扁平记录，随 store 插入**同步**写；`gateway` 唯一读的东西。**与活跃态 store 是两个结构**（拆分方案 §4.2.1） | Redis（派生） | `shared/pkg/sandbox-catalog/` ＋ `orchestrator/lifecycle.go:15` |
| `src/orchestrator/evictor.rs` | 已有，提出来 | 超时驱逐（唯一发起方） | —— | `evictor/` |
| `src/orchestrator/placement/` | 🆕 **新建**（Go port） | 优选 ＋ **试到有人接为止** | —— | `placement/` |
| `src/orchestrator/node_manager/` | 🆕 **新建** | 每节点代理对象：连接、状态、机器信息、`Sync` 循环 | 进程内缓存 | `nodemanager/` |
| `src/orchestrator/discovery/` | 🆕 **新建**（Go port） | `static` / `kubernetes` | —— | `discovery/` |
| `src/node_client/` | 🆕 **新建** | `RemoteSandboxBackendFactory` ＋ gRPC 客户端束 | —— | `clusters/client.go` |
| `src/snapshot/repository/pg/` | 🆕 **新建** | 目录：templates / builds / snapshots / aliases | **PG** | `packages/db` |
| `src/snapshot/` 其余 | 已有 14,154 | 发布流程、运行时解析 | —— | `snapshot_template.go` |
| `src/template/builder.rs` | 已有 | 构建调度 ＋ **并发上限**（今天没有） | PG `builds` | `template-manager/` |
| `src/cache/` | 🆕 **新建（阶段 2，与目录进 PG 同批）** | 目录派生的共享缓存（Redis）＋ 失效 —— e2b 三个子包全部 Redis ＋ DB 回落，所以没有「跨副本失效协议」这回事 | Redis（派生） | `internal/cache/` |
| `src/observability/` | 已有，改造 | 心跳接收端（今天是发送端）、节点指标 | —— | `nodemanager/metrics.go` |
| `src/api/impls/auth.rs` | 已有 | 鉴权（阶段 3 之后再补真凭据） | —— | `middleware/` |

### 4.2 `node` 角色（Rust，每节点，镜像 `agentenv-runtime --role node`）

| 模块 | 状态 | 负责领域 | 对应 e2b |
|---|---|---|---|
| `src/sandbox/` | 已有 15,103 | VM 生命周期、netns、ublk、envd、MMDS、custom extension hook | `pkg/sandbox/` |
| 沙箱句柄表 ＋ 控制面不透明配置 | 已有，改造 | `RwLock<HashMap<SandboxId, SandboxHandle>>` —— **节点自己的真相**，不是 `MetadataStore`；每项附带 `api` 下发的不透明配置，`ListSandboxes` 原样回传 | `sandboxFactory.Sandboxes` ＋ `APIStoredConfig` |
| `src/node_server/` | 🆕 **新建** | gRPC 服务端：`SandboxService` ＋ **`ListSandboxes`** | `pkg/server/` |
| `src/node_server/admission.rs` | 🆕 **新建** | **容量裁决**：接不下就快速拒绝（P1 的节点侧） | orchestrator 的 `ResourceExhausted` |
| `src/node_reclaim/` | 🆕 **新建** | 启动时回收残留 Firecracker / ublk / netns | `pkg/startupreclaim/` |
| `src/image/` ＋ `storage/*` | 已有 | 镜像解析、overlaybd、ublk 设备 | `pkg/sandbox/block` 等 |
| `src/p2p/` | 已有 | 层 / 块 peer 分发 ＋ 本地持有清单 | `chunks.proto` ＋ peerclient |
| `src/api/proxy.rs` | 已有，**改造** | 本机反向代理（沙箱数据面入口）。🔴 **`try_auto_resume`（`:878` `:891`）必须摘掉** —— 它今天让 node 自主发起 resume，与「纯执行器」冲突；唤醒改由 gateway 未命中触发 `api` | `pkg/proxy/`（不含唤醒决策） |
| `src/orchestrator/persistence/` | 已有 | 本地产物与 paused state | —— |
| `src/template/runner.rs` `step_executor.rs` | 已有 | 构建**执行**（调度在 api） | `pkg/template/` |
| `uvm-ublk-daemon` | 已有 | ublk 设备进程 | —— |

🔴 **`node` 角色不持有任何共享存储凭据**（比 e2b 的 P5 更严，理由见 D6）。

### 4.3 `gateway`（Go，N 副本）

| 模块 | 状态 | 负责领域 |
|---|---|---|
| `internal/proxy` | 改造 | 直读 Redis 解析路由 → 转发到 node 的本机反代 |
| `internal/resume` | 🆕 新建 | 未命中 → 调 `api` 的恢复 RPC。**它接的是从 `src/api/proxy.rs` 摘下来的那个决策**，不是新功能 |
| `internal/execution_fencing` | 已有 | 路由层拒旧 execution |
| `internal/{node,cluster,registry}_list` | **删除** | 阶段 2 之后集群列表是一条 SQL，不再扇出 |

---

## 5. AgentENV 的连边设计

| # | 边 | 协议 | 性质 | 对应 e2b |
|---|---|---|---|---|
| A1 | gateway → Redis | RESP | 每请求直读，无缓存 | E1 |
| A2 | gateway → api | gRPC，**只暴露恢复一个 RPC** | 冷路径 | E2 |
| A3 | api → node | gRPC `SandboxService`（含容量拒绝） | 命令 | E3 |
| A4a | api ← node（推·事件） | gRPC `ReportSandboxEvent` | **PAUSE / DELETE 删路由投影**，稀疏。🔴 CREATE / RESUME / FORK **不走这条**（见 D10） | E6 写入侧（e2b 由 api 自己写） |
| A4b | api ← node（推·周期） | gRPC `Heartbeat` | 节点清册与指标；**路由记录的对账修复路径** | 无对应（e2b 只有拉） |
| A5 | api → node（拉） | gRPC **`ListSandboxes`** | 对账 / 孤儿判定 | E4 |
| A6 | node ↔ node | iroh P2P ＋ overlaybd registryfs | 数据 | E5 |
| A7 | api ↔ Redis | 活跃态 store（Lua CAS）、pub/sub、共享缓存 | 权威 | E6 |
| A8 | api ↔ PG | 目录 | 权威 | E7 |
| A9 | node → 对象存储 | 字节 | 内容寻址 | E8 |
| A10 | api → k8s / static | 节点发现 | —— | discovery |

---

## 6. 十一个设计决策

### D1 推与拉都要，但各管各的

e2b 只有拉（E4），AgentENV 今天只有推（心跳）。**两者不是二选一**：

- **推（A4b）保留**：一次心跳带全部节点状态，成本 O(M)；拉是 O(副本数 × M)。
  我们已经有它，且 `ListP2pPeers` / 节点清册都建立在上面。
- **拉（A5）必须新增**：孤儿判定要问「节点实际有什么」，而心跳是「节点声称有什么」；
  `api` 副本冷启动也需要一次立刻的对账，不能等下一个心跳周期。

⇒ 心跳喂节点清册与指标；`ListSandboxes` 喂对账与孤儿判定。**不要用心跳的 roster 做孤儿判定** ——
那正是上一轮踩过的坑（outcome §2.1：roster 当时根本没被留存）。

### D2 缓存按「是否内容寻址」分层

| 缓存 | 放哪 | 理由 |
|---|---|---|
| 镜像层、overlaybd commit、premerged index、P2P blob | **node 本地**（现状不变） | 内容寻址、不可变，跨副本一致性无意义 |
| 模板元数据、别名解析、快照摘要、沙箱计数 | **api 共享（Redis）** | 从 PG 目录派生，必须跨副本一致失效 |

这条解决了拆分方案 §8 陷阱 2 的悬而未决。e2b 的 `internal/cache/` 正好只装第二类。

### D3 生命周期事件改走 Redis pub/sub

`src/orchestrator/service.rs` 今天的 `sandbox_event_tx` 是**进程内** broadcast。
拆成 N 副本之后它只覆盖本副本 —— 而消费者（observability reporter、将来的
`WaitForStateChange` 类等待）需要跨副本。

⇒ 抄 P3：Redis pub/sub 做传输，进程内 broadcast 保留为**最后一跳 fan-out**。
今天「无订阅者就丢弃」的最佳努力语义可以保留，但丢弃的判定要在本地那一跳，不在传输层。

### D4 放置抄「试到有人接为止」，容量裁决落在节点

抄 P1：`api` 的 `placement` 逐个候选真去 `Create`，节点侧 `admission.rs` 快速拒绝，
`api` 成功后乐观更新本地指标。

🔴 **这与上一轮删掉 gateway 那三段补丁不矛盾**，两者的差别是本质的：

| | 上一轮删掉的 | 这里要加的 |
|---|---|---|
| 路径 | resume（已有沙箱） | create（新沙箱） |
| 在哪 | **gateway**，要缓冲并重放请求体 | **api**，参数在手，无需重放 |
| 语义 | 挑一台让它去抢 —— 试错 | 候选集逐个问 —— 优选 |

上一轮删它是对的（一次决策不该靠重放）；这里加它也是对的（容量只有节点自己知道）。

### D5 缓存亲和性：我们已经有一半

e2b 的节点用 `pkg/scheduling/FromHeaders` 产出 `SchedulingMetadata`（build 链的
rootfs / memfile build id 与字节数）上报给 api ——
**但 api 侧今天还没有消费方**（`grep SchedulingMetadata packages/api/` 零命中），
是一条建好待用的通道。

我们的对应物已经存在且更完整：P2P 的 artifact 索引（`RecordP2pArtifact` /
`LookupP2pArtifact`）知道哪个节点持有哪个层。⇒ **`placement` 的候选排序直接查这个索引**，
不必新造一条元数据通道。这是我们领先的地方，别丢掉。

### D6 `node` 角色一律不拿共享存储凭据 —— 比 e2b 更严

e2b 的节点有一处 Redis（`pkg/sandbox/uploads.go` 的 upload-done pub/sub）。我们不抄这一处：

- 上一轮把 PG 凭据从每台跑用户代码的 KVM 机器上摘掉，被 outcome §1.3 称为
  「整个重构最硬的那条理由」（G7）。发 Redis 凭据下去等于把它撤销；
- 我们的等价需求（「层下载完了」）本来就有别的通道：P2P 的
  `/p2p-control/publish-layer` ＋ 目录索引，不需要一条额外的 pub/sub。

⇒ 如果将来确实需要节点级通知，走 `api` 中转（A4/A5 已经有连接），不新开一条到共享设施的边。

### D7 活跃态只有一个后端，测试跑真 Redis

用户裁决（2026-08-20）：**`InMemoryMetadataStore` 作为权威状态整个拿掉**，
不保留「本地/dev 用内存、生产用 Redis」的双后端。

**三条依据：**

1. **e2b 就是单后端。** `packages/api/internal/sandbox/storage/` 下只有 `redis/` 一个子目录；
   测试用 testcontainer 起真 Redis（`packages/shared/pkg/redis/tests.go:17`，`redis:8-alpine`）。
2. **我们自己付过双实现的代价。** `CLAUDE.md` 逐字记着 `test-with-postgres` 存在的理由：
   「a change made to the in-memory store and forgotten for Redis is invisible everywhere else」。
   同一个陷阱，换个 store 再踩一次没有道理。
3. **双后端会让「两个副本各持一份」这条最危险的路径永远不被生产验证** ——
   而它恰恰是这次重构要消灭的那一条。

**边界（不要拿掉的）：**

- `MetadataStore` **trait 保留** —— 它现在只有一个生产实现，外加
  `src/orchestrator/tests.rs` 里那四个测试替身。替身是 mock，不是第二个后端。
- **节点侧的沙箱句柄表保留**，见 §4.2。它不是 `MetadataStore`，是活着的 VM 句柄，
  且 e2b 的 `List` 正是从它来的（`pkg/server/sandboxes.go:568`）。
- 现有的 `make -C services test-with-postgres` harness（起临时 Redis ＋
  `SCHEDULER_REDIS_TEST_REQUIRED=1`）就是这类测试的模板，照它扩到 Rust 侧。

**影响面**：全仓 60 处引用 / 8 个文件；生产路径只有 `src/orchestrator/service.rs:94`
（默认类型参数）、`:143` `:155`（便捷构造）与 `src/orchestrator/mod.rs:29`（再导出）。

### D8 🔴 节点上有一类沙箱不属于控制面

e2b `packages/orchestrator/pkg/server/sandboxes.go:577-582` 逐字：

> Build sandboxes are not owned by the API and must never show up here, or the API
> would treat them as orphans and kill them. They are the only sandboxes created
> without an `APIStoredConfig`.

我们的 `src/template/runner.rs` 在节点上跑构建沙箱，拆分之后必然撞上同一件事。

⇒ **`ListSandboxes` 只报控制面拥有的沙箱**，所有权用**显式标记**表达
（跟着那份不透明配置走：有配置 = 控制面的），不要靠「它长得像不像用户沙箱」去推断。

顺带：这个不透明配置还解决了 `api` 副本重启的重建问题 —— 节点原样回传控制面当初下发的东西，
`api` 不需要把节点内部状态翻译回自己的模型。

### D9 互斥有两把锁，别搞混

> 🔧 **本节 v2 写错过一次。** 原文说「我们今天这三层由进程内 `RwLock` … 承担」，
> 由此得出「N 副本之后第一层直接失效，要新造分布式锁」。**读代码之后这个判断要改。**

**AgentENV 的生命周期互斥今天就不靠 `RwLock`。** `src/orchestrator/store/mod.rs`
的 `MetadataStore` trait 已经带着 e2b 三层里的两层半：

```rust
async fn update_state_if_state(&self, id, new_state, expected_states) -> Result<SandboxState>;  // :61 状态 CAS
async fn update_if_state<F>(&self, id, expected_states, update: F) -> ...;                      // :73 带闭包的 CAS
async fn wait_while_in_states(&self, id, transitional_states) -> ...;                           // :96 等过渡态结束
```

它们是 **trait 方法** —— 阶段 3 把 `S` 换成 `RedisMetadataStore`，**自动就跨副本了**。
过渡态（`Pausing` / `Snapshotting` / `Forking`）本身就是锁：谁先 CAS 成功谁持有。

**另一把锁守的是完全不同的东西。** `service.rs:41` `:101`：

```rust
type SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>;
sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>,
```

活着的 Firecracker VM 的**进程内句柄**：不可序列化，**永远不上 Redis**，
拆分后留在 `node` 角色。e2b 同形 —— 它的 `List` 正是从这张表来的
（`pkg/server/sandboxes.go:568`）。

#### 那还缺什么

| 缺口 | 为什么 CAS 覆盖不到 | 落点 |
|---|---|---|
| 🔴 **`update_if_state<F>` 的闭包契约** | `store/mod.rs:73` 注释逐字：「store implementations **may run it while holding their metadata lock**」——**这是为进程内锁写的**。Redis 实现只能二选一：翻成 Lua，或改成 GET→改→CAS-SET 的乐观重试。**是语义变更，不是换后端**，每个调用点都要审 | 阶段 3 必做第 2 条 |
| 🔴 **跨多次往返的读-判断-写** | e2b `StartRemoving`（`state_change.go:41`）先 `Obtain(lockKey, lockTimeout)` **然后才** GET / unmarshal / 判状态机 / 生成 transition id / 写回 —— 复杂到写不进单个 Lua | 阶段 3 必做第 3 条 |
| 🔴 **崩溃恢复** | 副本在 `Pausing` 中途死掉，Redis 里的状态**没有 TTL，永远卡住**。今天单进程重启清空内存 store，问题不存在；**上 Redis 之后是新问题** | 同上，靠锁的 `lockTimeout` |
| **并发 create 去重** | e2b `store.go:157` 的 `Reserve`：已存在时返回 `waitForStart` 让第二个调用方**等结果**而不是报错 | 阶段 3 必做第 4 条 |

⇒ **锁不是 CAS 的替代，是 CAS 表达不了的那部分的兜底。** 这比「e2b 有所以我们也要有」
是更硬的理由，也说明它的范围比 v2 想的小 —— 不是「所有状态变更都要先拿锁」。

### D10 路由投影：从心跳派生升格为权威记录 —— 但不是所有事件都走事件通道

拆分方案 §7 阶段 1 的①。在这份文档里的意义是：**它把 A4 这条边劈成了两条**。

- **今天**：路由记录由心跳 roster 派生，30s TTL；
  `ReportSandboxEvent` 在 `services/scheduler/internal/service.go:425-432`
  收到即丢弃（逐字：「scheduler ignored sandbox event batch」）。
- **目标**：记录的存活不再依赖 scheduler 在线；**心跳对账降级为修复路径**。

🔧 **哪条写走哪条路，v4 修正过一次**：

| 事件 | 走哪 | 理由 |
|---|---|---|
| **CREATE / FORK** | **不改** —— gateway 响应路径上的同步写（`server.go:557-561`，已带化身） | e2b `sandbox/store.go:43` 明确要求投影写**同步**：「to prevent race conditions where we would know where to route the sandbox」。改走尽力而为的事件流是降级 |
| **RESUME** | 补进同一个同步机制 | `shouldRecordAssignment`（`server.go:655-670`）今天不匹配它 —— **这是投影真正缺的那条写** |
| **PAUSE / DELETE** | 事件通道（`ReportSandboxEvent`），按 execution 守卫删除 | DELETE 连方法都不匹配那个同步路径；守卫对标 `catalog_redis.go` 的 `DeleteSandbox`：`if info.ExecutionID != executionID { return nil }` |

⇒ 与 D1 的关系：D1 说「推与拉都要，各管各的」，D10 把「推」再拆成
**事件（稀疏、只管删）** 与 **心跳（对账、周期）**，两条的失败模式不同 ——
事件丢了由心跳修，心跳晚了不影响记录存活。

🔴 **TTL 写成什么，今天没有答案。** e2b 用 `MaxLengthInHours`（建时确定、之后不变），
我们没有这个量（`grep max_instance_length` 全仓无匹配），只有一个可被 `SetTimeout`
反复推后的 deadline。要么引入上界，要么每次改 timeout 都续期 ——
后者是阶段 3 的机制。取舍见拆分方案 §7 阶段 1 第 4 点。

---

### D11 🔴 路由投影与活跃态 store 是两个结构

e2b 是两个，而且刻意隔离 —— `grep -rn "api/internal/sandbox" packages/client-proxy/` **零命中**：

| | key | 内容 | 谁读 |
|---|---|---|---|
| 投影 `sandbox:catalog:<id>` | 扁平 | 五字段：`OrchestratorID` / `OrchestratorIP` / `ExecutionID` / `StartedAt` / `MaxLengthInHours` | `client-proxy` |
| store `sandbox:storage:{team}:sandboxes:<id>` | 分片 ＋ transition / lock / ZSET | 完整状态机 | **只有 `api`** |

**对我们比对 e2b 更硬**：gateway 是 Go，`api` 是 Rust。让 gateway 反序列化活跃态 store
的记录，等于把一个随状态机每次改动而变的 Rust 类型变成跨语言 wire contract ——
正是拆分方案 §2.2 反对的东西。投影是五字段冻结契约，状态机怎么改都不动它。

🔴 **记录留下，写它的人会换。** 阶段 1 由 gateway 在响应路径上写（因为今天 REST 入口是
node，gateway 是唯一同时看得见「请求」和「哪台节点答的」的地方）；
阶段 3 之后 REST 入口就是 `api` 本身，投影改由 `api` 随 store 插入同步写 ——
和 e2b 的 `Callbacks.AddSandboxToRoutingTable` 逐字同形。
**换的是写入方，不是记录结构**，所以阶段 1 的产出不作废。

不必照抄的一处：e2b 按 team 分片（`storage/redis/utils.go:62` 的 `SameSlot(teamID)`）
是 Redis Cluster hash slot 的需求。我们没有租户模型，store key 可以扁平。
**隔离的理由是跨语言契约，不是键推导。**

---

## 7. 新建模块与阶段的对应

| 模块 | 阶段 | 备注 |
|---|---|---|
| `gateway/internal/resume` | **3** | 🔴 与「从 `src/api/proxy.rs` 摘掉 `try_auto_resume`」同批。阶段 1／2 期间未命中仍回落 `LookupNode`，node 自己唤醒 |
| `scheduler` 的 `ReportSandboxEvent` 实现（今天丢弃） | **1** | D10，阶段 1 的① |
| proto 加 `SandboxEvent.execution_id` | **1** | D10 的 execution 守卫删除需要它 |
| `shouldRecordAssignment` 覆盖 RESUME | **1** | D10 —— 投影真正缺的那条同步写 |
| 🔴 裁决沙箱寿命上界（`max_sandbox_lifetime`） | **1** | D10 末尾；不定就写不出投影的 TTL |
| `src/snapshot/repository/pg/` | **2** | 目录进 PG |
| `src/template` 的并发上限 ＋ `builds` 表 | **2** | 建表时留形状，别等阶段 3 |
| `src/orchestrator/store/redis.rs` | **3** | `api` N 副本的**必要条件**（不是充分 —— 还要 8.5 那六件） |
| `src/orchestrator/routing_projection.rs` | **3** | D11；写入方从 gateway 换成 `api`，记录结构不变 |
| 摘掉 `src/api/proxy.rs` 的 `try_auto_resume` | **3** | 🔴 不摘，`--role node` 之后 node 仍自主发起 resume |
| `src/orchestrator/events/`（pub/sub 化） | **3** | D3 |
| `src/orchestrator/store/redis_cas.rs` | **3** | 三个 CAS/wait 原语的 Redis 实现；`update_if_state` 的闭包契约要重写 |
| `src/orchestrator/store/lock.rs` | **3** | D9：只补崩溃恢复与跨往返读-判断-写 |
| `src/orchestrator/store/reserve.rs` | **3** | D9：并发 create 去重 |
| `src/cache/` | **2** | D2 第二类；🔧 v4 从阶段 3 提前 —— 目录在哪，目录缓存就在哪 |
| `src/node_client/` | **3** | `RemoteSandboxBackendFactory` |
| `src/node_server/` ＋ `ListSandboxes` | **3** | D1 的拉侧 |
| `src/node_server/admission.rs` | **3** | D4 的节点侧 |
| `src/node_reclaim/` | **3** | node 角色一旦独立就必须有 |
| 删除 `src/orchestrator/store/in_memory.rs` | **3** | D7，与 Redis store 同批 |
| `ListSandboxes` 的所有权标记 | **3** | D8，漏了会被当孤儿杀掉 |
| `src/orchestrator/placement/` | **4** | 从 Go port，D4 ＋ D5 |
| `src/orchestrator/node_manager/` | **4** | 从 Go port |
| `src/orchestrator/discovery/` | **4** | 从 Go port |
| 删除 `gateway/internal/{node,cluster,registry}_list` | **2 之后** | 集群列表变成一条 SQL |

---

## 8. 活跃态 store 的四个缺口 —— 照 e2b 补齐

D9 指出了四个缺口。这一节给每个缺口写下 e2b 的具体机制与我们的落点。
🔴 **其中两个的解法和 D9 初稿写的不一样** —— 读实现之后改的。

### 8.1 闭包式 update：契约不变，只是锁换成分布式锁

**我们的**：`src/orchestrator/store/mod.rs:73` 的 `update_if_state<F>`，注释写着
「store implementations **may run it while holding their metadata lock**」。

**e2b 的同形物**：`store.go:128` 的 `Update(ctx, teamID, sandboxID, updateFunc)`。
Redis 实现（`storage/redis/operations.go:159-215`）：

```go
lock, _ := s.locker.Obtain(ctx, lockKey, lockTimeout)   // ① 分布式锁
defer lock.Release(...)
data, _ := s.redisClient.Get(ctx, key).Bytes()          // ② 读
json.Unmarshal(data, &sbx)
updatedSbx, err := updateFunc(sbx)                      // ③ 闭包在锁内跑
newData, _ := json.Marshal(updatedSbx)
s.redisClient.Set(ctx, key, newData, redis.KeepTTL)     // ④ 写，保留 TTL
if !updatedSbx.EndTime.Equal(sbx.EndTime) {             // ⑤ EndTime 变了就重打分
    s.redisClient.ZAdd(ctx, globalExpirationSet, ...)
}
```

⇒ **闭包保留，「持有锁跑闭包」这条契约逐字保留**，变的只是锁的作用域：进程 → 集群。

> 🔧 **这推翻了 D9 初稿的说法**（「要么翻成 Lua，要么改成乐观重试循环」）。
> 不需要。真正要加的是两件小事：
> 1. `redis.KeepTTL` 的等价物 —— 写回不能抹掉沙箱寿命 TTL；
> 2. 🔴 **给闭包一个执行时限**。进程内锁没有 TTL，分布式锁有 —— 一个慢闭包会活过
>    `lockTimeout`，此后它的写入就不再受保护。「不许在闭包里做 async 工作」这条
>    从「实现细节」升格成「正确性要求」。

### 8.2 转换的崩溃恢复：transition key 三件套

**缺口**：`update_state_if_state(Running → Pausing)` 只 claim 了状态，
**没有 TTL、没有 owner、没有结果通道**。副本在转换中途死掉，Redis 里的 `Pausing`
永远不会自己解开。今天单进程重启就清空内存 store，所以这个问题不存在；上 Redis 之后是新问题。

**e2b 的机制**（`storage/redis/state_change.go`）—— 不是「给锁一个 TTL」，是三件套：

| 键 | TTL | 作用 |
|---|---|---|
| `transitionKey` | `transitionKeyTTL` | 谁在做这个转换，值是 `transitionID`（uuid） |
| `resultKey(transitionID)` | `transitionResultKeyTTL` | 转换的结果，等待者从这里取 |
| **完成回调** | —— | 调用方**必须**调：删 `transitionKey` ＋ 写 `resultKey` |

状态与 `transitionKey` 由**同一个 Lua 脚本**原子写入（`startTransitionScript`）。

🔴 **分布式锁只覆盖「读-判断-写」那一小段，不覆盖整个操作。**
`state_change.go` 在进入等待之前显式 `releaseFunc()`。
**跨越整个操作的是 `transitionKey`，它的 TTL 才是崩溃恢复的边界。**

`handleExistingTransition` 的三分支，我们要照搬：

- **同一目标态在途** ⇒ 等它完成，返回它的结果（不是报错「忙」）；
- **不同目标态在途** ⇒ 等它结束，然后重试；
- **无在途** ⇒ 开始。

配套两个语义，我们的状态机也需要：
- `AllowedTransitions[from][to]` —— 显式的转换表，非法转换返回结构化错误；
- `TransitionTransient` vs `TransitionExpires` —— 瞬态转换（如 snapshotting）成功后
  由回调**恢复到 Running**；终态转换则顺手把 `EndTime` 拨到现在。

### 8.3 多副本驱逐：全局过期 ZSET ＋ healer

**缺口**：我们今天的 `MetadataStore::list_expired(now)` 是**全表扫**。
N 个副本各扫一遍自己的 store 是今天的形状；上 Redis 之后，N 个副本扫同一份数据，
既浪费又没有「谁负责这一批」的概念。

**e2b 的机制**：

| 部件 | 落点 | 说明 |
|---|---|---|
| `globalExpirationSet` | Redis ZSET | score ＝ `EndTime.UnixMilli()`，member 含 sandboxID ＋ **executionID** |
| `ExpiredItems` | `items.go:20-32` | `ZRangeByScore(-inf, now, Count: 256)` —— **每轮有界**，不是全表扫 |
| 重打分 | `operations.go:209-214` | `Update` 里 `EndTime` 变了就 `ZAdd` |
| 锁内重校验 | `state_change.go` 的 `opts.Eviction` 分支 | 驱逐前在锁内**再判一次是否真的过期**，防与 `SetTimeout` 竞态；已有转换在途则直接 `ErrEvictionInProgress` |
| **healer** | `heal.go` | 每 5 分钟带 jitter，`ZMSCORE` 找缺失成员，`ZADD NX` 补回 |

healer 的三个设计点值得逐条抄：
1. **跑在每个副本上**，`ZADD NX` 让并发轮次幂等无害；
2. **grace period（1 分钟）跳过刚启动的沙箱**，避免清掉在途的 Add/Remove；
3. **feature flag 每轮重新求值**，注释逐字：「acts as a kill switch without redeploy」。

`heal.go` 里那段 TOCTOU 论证是本轮考古里最值得学的一段：

> A TOCTOU with a concurrent Remove can only plant an orphan member, which
> `ExpiredItems` sweeps once its score passes — garbage, never a false eviction
> (eviction re-checks the stored JSON and re-validates expiry under the lock in
> `StartRemoving`).

**把「修复动作可能出错」的后果论证到「只会产生垃圾，不会误删」**，而不是论证它不会出错。

### 8.4 并发 create：`Reserve` 的四态返回

**缺口**：同一个 sandbox id 被并发创建（客户端重试是常态），今天没有任何去重。

**e2b 的机制**（`reservations/redis/reservation.go:50-83`）—— 一个 Lua 脚本，三个键
（`storageIndexKey` 已存在的 / `pendingSetKey` 在途的 / `resultKey` 结果），四种返回：

| 返回 | 含义 | 调用方拿到 |
|---|---|---|
| `reserved` | 我抢到了 | `finishStart(sandbox, err)` 回调 |
| `alreadyInStorage` | 已经建好了 | `ErrAlreadyExists` |
| **`alreadyPending`** | **别人正在建** | **`waitForStart(ctx)` —— 等它的结果，不是报错** |
| `limitExceeded` | 团队配额满 | `LimitExceededError`（🔴 我们暂时用不上，见下） |

🔴 **`alreadyPending` 那一支是关键**：并发创建的第二个调用方**等第一个的结果**，
而不是收到 409。这是「客户端重试」与「用户真的建了两次」的分水岭。

**崩溃恢复也在这里**：脚本带 `staleCutoff = now - staleTTL`，
在途集合里的陈旧条目自动失效 —— 建到一半死掉的副本不会永久占住这个 id。

⇒ 这一个原语在 e2b 那里同时解决三件事：并发去重、团队配额、在途创建的崩溃恢复。

> 🔧 **v4 修正：我们只拿得到其中两件。**
> ```
> $ grep -rn "team_id|TeamId" src/ --include=*.rs
> src/api/generated/src/models.rs      # 只是 E2B 兼容 schema 的字段，没有实现
> ```
> `src/api/impls/auth.rs` 自述 "**presence, not validity**"。**我们没有租户模型**，
> 所以 `limitExceeded` 没有可对照的配额主体 —— 阶段 3 实际实现的是**三态**
> （`reserved` / `alreadyInStorage` / `alreadyPending`⇒`waitForStart`）。
> 并发去重与在途创建的崩溃恢复照拿，配额那一支留好返回值位置，等租户模型落地再接。

### 8.5 落到阶段 3 的必做清单

| # | 做什么 | e2b 参照 |
|---|---|---|
| 1 | `update_if_state` 保留闭包，锁换分布式锁 ＋ 闭包执行时限 ＋ KeepTTL 等价物 | `operations.go:159-215` |
| 2 | transition key ＋ result key ＋ 完成回调；`handleExistingTransition` 三分支 | `state_change.go` |
| 3 | 显式 `AllowedTransitions` 表；瞬态 / 终态两种 effect | `sandboxtypes` |
| 4 | 全局过期 ZSET 替掉 `list_expired` 全表扫；每轮有界；锁内重校验 | `items.go:20`、`state_change.go` |
| 5 | healer：每副本跑、`ZADD NX`、grace period、可热关 | `heal.go` |
| 6 | `Reserve` **三态**返回（配额那一支留位不实现，见 8.4），含 `waitForStart` 与 `staleCutoff` | `reservation.go:50` |

---

## 附：本文新增的证据

| 事实 | 位置 |
|---|---|
| 放置是「试到有人接为止」＋ 乐观更新 | `packages/api/internal/orchestrator/placement/placement.go:85` `:142-143` |
| api 对数据面只暴露一个 RPC | `packages/shared/pkg/grpc/proxy/proxy.proto:23` |
| api → node 的四个 gRPC 服务 | `packages/api/internal/clusters/client.go` |
| 对账是拉的 | `packages/api/internal/orchestrator/nodemanager/sync.go:17` `:69` |
| 跨副本状态变更靠 Redis pub/sub | `packages/api/internal/sandbox/storage/redis/publisher.go`、`subscription_manager.go` |
| 节点无 PG；唯一的 Redis 是 upload-done 通知 | `packages/orchestrator/pkg/sandbox/uploads.go:69` `:196` `:217` |
| 节点产出亲和性元数据，api 侧尚无消费方 | `packages/orchestrator/pkg/scheduling/`；`grep SchedulingMetadata packages/api/` 零命中 |
| 发现的四种实现 | `packages/api/internal/orchestrator/discovery/{local,nomad,kubernetes,merged}.go` |
| 节点启动残留回收 | `packages/orchestrator/pkg/startupreclaim/` |
| 分布式锁保护的是跨多次往返的读-判断-写 | `packages/api/internal/sandbox/storage/redis/lock.go`、`state_change.go:41` `:190` |
| 并发 create 去重 | `packages/api/internal/sandbox/store.go:157` |
| 路由记录 TTL＝沙箱寿命，由 api 显式写／删 | `packages/api/internal/orchestrator/lifecycle.go:38`、`catalog_redis.go` 的 `DeleteSandbox` |
| 活跃态只有 Redis 一个后端 | `packages/api/internal/sandbox/storage/`（仅 `redis/`） |
| 测试跑 testcontainer 里的真 Redis | `packages/shared/pkg/redis/tests.go:17` |
| 节点的 `List` 来自进程内句柄表 | `packages/orchestrator/pkg/server/sandboxes.go:568` |
| 🔴 构建沙箱必须排除，否则被当孤儿杀掉 | `packages/orchestrator/pkg/server/sandboxes.go:577-582` |
| 🔴 路由投影是独立结构；client-proxy 从不引用 api 的 store 包 | `packages/shared/pkg/sandbox-catalog/catalog.go:11-18`；`grep -rn "api/internal/sandbox" packages/client-proxy/` 零命中 |
| 🔴 投影写必须**同步**，是 store 的插入回调 | `packages/api/internal/sandbox/store.go:43-44`、`orchestrator/lifecycle.go:15` |
| store key 按 team 分片；投影 key 扁平 | `storage/redis/utils.go:62-67`、`catalog_redis.go:111-112` |
| 🔴 过期索引 member 按 execution 作用域 ⇒ 每次 ZREM "structurally safe" | `packages/api/internal/sandbox/storage/redis/utils.go:32-38` |
| 目录缓存三个子包全部 Redis ＋ DB 回落 | `packages/api/internal/cache/{templates,snapshots,sandboxcounts}` |
| 冷路径：catalog miss ⇒ 调 api 唤醒 | `packages/client-proxy/internal/proxy/proxy.go:109` |
| 单条全局 pub/sub 通道，路由键在 payload 内 | `packages/api/internal/sandbox/storage/redis/utils.go:22-25` |

**AgentENV 侧新增**

| 事实 | 位置 |
|---|---|
| 🔴 CREATE / FORK 的投影写**已经是同步的**，且带化身 | `services/gateway/internal/server.go:557-561` |
| 投影的记录结构已存在：扁平 key ＋ 节点 ＋ 化身 | `services/scheduler/internal/redis_store.go:17` |
| 🔴 没有「沙箱最大寿命」这个量 | `config/default.toml:215`；`grep max_instance_length` 全仓无匹配 |
| 🔴 auto-resume 在 node 的数据面反代路径上 | `src/api/proxy.rs:878` `:891` |
| 🔴 没有租户模型；鉴权是 presence-only | `src/api/impls/auth.rs`；`grep team_id src/` 仅命中 generated |
