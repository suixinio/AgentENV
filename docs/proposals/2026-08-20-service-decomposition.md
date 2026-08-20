# 服务拆分：抄 e2b 的边界与进程表，但不抄它的语言分配

> 2026-08-20 · **写给决定要不要动这一刀的人**。
> 上游背景：[`2026-08-19-control-plane-refactor-outcome.md`](2026-08-19-control-plane-refactor-outcome.md)（阶段 0/1/2 收口）
> 三家架构对照：[`2026-08-19-aenv-central-control-plane.md`](2026-08-19-aenv-central-control-plane.md)
>
> 考古基准：e2b `/home/debian/e2b-infra`。本文所有 `packages/...` 路径都指它。
>
> **配套**：[`2026-08-20-module-responsibilities.md`](2026-08-20-module-responsibilities.md)
> —— 本文定「进程怎么拆、分几步」，那份定「每个模块负责什么、模块之间怎么连」。
>
> **v2（2026-08-20）**：经一轮对抗审查后重写。修正了三处事实错误、两条无法执行的阶段，
> 并按「`api` 必须是 N 副本」这个目标重排了阶段。v1 的关键结论有一条被推翻 —— 见 §2.3。
>
> **v3（2026-08-20）**：第二轮对抗审查。改掉两处自相矛盾、一处被高估的兑现、
> 一处会导致回退已发布代码的时态错误，并补上三个 e2b 有而本文没有的机制
> （分布式锁 / `Reserve` / 事件驱动的权威 binding）。**阶段 1 的定义变了** —— 见 §7.1。
> 两轮审查的处置分别在 §10 与 §11。
>
> **v4（2026-08-20）**：第三轮对抗审查，重点在「照着能不能执行」。
> 阶段 1 的 ① 被证明是把一条**已经同步**的写降级成异步（§7 阶段 1）；
> 路由投影与活跃态 store 被证明必须是**两个**结构（§4.2.1）；
> auto-resume 的归属是全文最大的空缺（§4.3、阶段 3 第 5 条）。处置在 §12。

---

## 0. 三十秒版

上一轮阶段 2 把 PG 写权收进了 scheduler，但**只收了账本，没收职责**。e2b `api` 真正定义
它身份的四件事 —— REST 入口、目录所有权、驱逐权、发布翻牌 —— 在 AgentENV 里一件都不在
scheduler 手上，全在每台 node 的 Rust 二进制里。

本文提议按 e2b 的边界（决策 / 执行 / 转发）重新划分。**最终进程表会收敛到 e2b 的三个**
（`gateway` / `api` / `node`），我们不照抄的是它的**语言分配** —— 新的 `api` 层用 Rust，
从现有 workspace 以 `--role` 切出来，而不是在 `services/` 里用 Go 重写那 61k 行。

🔴 **两条硬约束贯穿全文**：

1. **先动存储，再拆进程。** e2b 的进程边界是它存储边界的*结果*，不是原因。
   先拆进程再动存储，只会把一个单点变成三个单点。
2. **`node` 角色不持有共享存储凭据。** 上一轮把 PG 凭据从每台跑用户代码的 KVM 机器上摘掉
   （G7，outcome §1.3 称之为「整个重构最硬的那条理由」）。把 Redis 凭据发下去等于把它撤销。
   ⇒ **活跃态进 Redis 必须与 role 拆分同批**，不能提前。

   > ⚠️ v2 把这条写成「**只有 `api` 持有**」，与阶段 1（gateway 直读 Redis）自相矛盾 ——
   > gateway 是控制面组件，不跑用户代码，约束对象从来就只是 `node`。登记在 §11 G8。

| 阶段 | 做什么 | 兑现 | 回退 |
|---|---|---|---|
| **1** | **路由投影**变成权威记录 ＋ gateway 直读 Redis | 控制面挂掉不再打死数据面 | 读/写两个开关分别回落 |
| **2** | catalog 进 PG，对象存储降为纯字节 | 拿到可原子提交的地方；暂停态有地方落 | 双写保留到阶段 3 之后 |
| **3** | 活跃态折叠进 Redis ＋ `--role` 拆 `api`/`node` ＋ `api` 直接 N 副本 | 三分变二分；−13,640 行；node 成纯执行器 | `--role all` |
| **4** | 折叠 `scheduler` 进 `api` | 消除双节点清册；进程表收敛到 e2b 的三个 | 保留 `scheduler` 二进制一个 release |

> **阶段 1 与阶段 2 无相互依赖，可并行。** 阶段 1 排第一是因为它没有任何前置，
> 且消除的是今天唯一一个每次发版都会打开的故障窗口。
> 🔧 **v3 扩大了它的范围**：只做「gateway 直读」那一半，买到的是 30 秒宽限而不是标题
> 所说的东西 —— 理由与证据在 §7 阶段 1。
> 🔧 **v4 又把它收窄了**：CREATE / FORK 的投影写**今天已经是同步的**，不需要改造，
> 真正缺的只有 RESUME 与 PAUSE / DELETE。规模回到 400–500 行含测试。

> **术语**：本文用「**本文阶段 N**」指上表；用「**上一轮阶段 N**」指
> [`控制面重构收口`](2026-08-19-control-plane-refactor-outcome.md) 的阶段 0/1/2/3。
> 两套编号无关，混淆会导致排期读错。

---

## 1. 命名约定（先定，全文按此书写）

| 层 | 规则 | 现有 | 变化 |
|---|---|---|---|
| 服务角色 | **裸名** | `gateway` `scheduler` | 新增 `api` `node`；`scheduler` 在阶段 4 消失 |
| 部署对象 | **`agentenv-` 前缀** | `agentenv-gateway` `agentenv-scheduler` `agentenv-node` | 新增 `agentenv-api` |
| 容器镜像 | **`agentenv-` 前缀** | `agentenv-runtime` `agentenv-gateway` `agentenv-scheduler` | `api` 与 `node` **共用 `agentenv-runtime`** |
| 操作者 CLI | **`aenv-` 前缀** | `aenv` `aenv-snapshot-image` | 不变 |
| 存储子系统 | **`uvm-` 前缀** | `uvm-ublk-daemon` | 不变 |

🔴 **`api` 和 `node` 不是两个二进制，是一个二进制的两个角色。** 仍然只有
`src/bin/server.rs`，靠 `--role` 选装配。部署层是两个对象（`agentenv-api` Deployment ＋
`agentenv-node` DaemonSet）跑**同一个镜像**。e2b 就是这么做的：orchestrator 与
template-manager 共用一个镜像，靠 `ORCHESTRATOR_SERVICES` 选角色
（`packages/orchestrator/pkg/cfg/service.go:17`）。

🔴 **不要引入第三种前缀 `aenv-<service>`。** `aenv-` 今天专属面向操作者的 CLI
（`crates/aenv`、`src/bin/aenv-snapshot-image.rs`）；把服务塞进这一族，会让 `aenv-api`
和 `aenv` 看起来像同一个东西的两个子命令。

---

## 2. 抄什么、不抄什么

### 2.1 对应关系不是一对一的

一个常见的误读是「e2b `api` ↔ 我们的 `scheduler`」。差得很远 —— e2b 没有独立的
scheduler 进程，放置逻辑就在 `api` 里：`packages/api/internal/orchestrator/{placement,nodemanager,discovery}/`。

```
packages/api/internal 非测试代码            44,214 行
  减去 internal/api/（oapi-codegen 生成，
      "DO NOT EDIT"）                      -17,910 行
                                          ---------
  手写部分                                  26,304 行

  其中对应 AgentENV scheduler 的部分：
    orchestrator/placement                     515
    orchestrator/nodemanager                 1,187
    orchestrator/discovery                     441
                                          ---------
                                             2,143 行  ≈ 手写 api 的 8.1%
```

> ⚠️ **v1 在这里写的是「44,214 行 / 5%」，把 17,910 行生成代码算进了分母，
> 且把 `packages/api/internal` 标成了 `packages/api`。** 修正后方向不变，
> 但那个偏差正好偏向作者想要的结论，登记在 §10 F3。

剩下 91.9% 在 AgentENV 里**不在 scheduler**，而在 node 的 Rust 二进制里：

| e2b `api` 的组成 | 行数 | AgentENV 里住在哪 |
|---|---|---|
| `handlers/`（REST 全量） | 8,671 | node `src/api/impls/` |
| `sandbox/`（活跃态 store） | 3,175 | **每台 node 的 `InMemoryMetadataStore`** ＋ scheduler bindings |
| `template-manager/`（构建调度） | 952 | node `src/template/` |
| `cache/`（**三个子包全部 Redis ＋ DB 回落**） | 907 | **无对应物** —— 我们没有跨副本目录缓存，见 §8 陷阱 3 |
| `db/` ＋ queries（目录 / 构建 / 快照） | —— | **对象存储** `catalog/records/` |
| `orchestrator/evictor/` | 206 | node `src/orchestrator/service.rs:2159` |
| `pause_instance.go` 发布翻牌 | —— | node 自己 publish |
| `orchestrator/{placement,nodemanager,discovery}` | 2,143 | **scheduler** ✅ |

对照另一侧：`services/scheduler` 10,077 行、`services/gateway` 3,086 行；
node 的 `src/api` 28,520 ＋ `src/orchestrator` 16,016 ＋ `src/snapshot` 14,154 ＋ `src/template` 2,447。

### 2.2 不抄的是语言分配

把 e2b `api` 的职责搬进 `services/` 意味着把上面那 61k 行 Rust 移植成 Go。
除了工作量，还有一个更硬的理由：

🔴 **同一条接缝不要跨两次语言。** `Orchestrator` 的状态机、`SnapshotManager` 的发布流程、
`TemplateBuilder` 的步骤执行，与 `src/sandbox`、`src/image`、`storage/overlaybd` 共享大量类型。
跨语言拆会在 Rust 侧留下一整套只为序列化而存在的镜像类型 —— 上一轮阶段 2 已经付过一次这个代价：
`_impl-D7-contract-tests.md` §4 的 S0–S11，十二条「Rust 语义在 Go 接口上表达不出来」。

### 2.3 🔧 但进程表本身，最终确实要收敛到 e2b 的三个

> **这一条推翻了 v1 的标题论断。** v1 说「不照抄它的进程表」，理由是我们要保留
> `scheduler`。审查（§10 F8）指出：目标形态里 `api` 需要节点清册来解析 node endpoint，
> `scheduler` 也需要节点清册来放置 —— **两个进程各自维护同一份状态**，正是 §0 第一条
> 硬约束批评的形状。
>
> 在「不背债、要最优」的前提下，正确答案是**阶段 4 把 `scheduler` 折叠进 `api`**：
> port `placement` / `filter` / `strategy` / `kubernetes_discovery` / `node_registry`
> 的等价物（Go 侧 2,143 行量级），换掉一个进程、一份重复清册、一跳 RPC。
>
> ⇒ 最终进程表 = `gateway` / `api` / `node`，**和 e2b 逐个对得上**。
> 不抄的只剩一件事：`api` 是 Rust 写的。

---

## 3. 切口在代码里 —— 但缺一块

### 3.1 `SandboxBackend` 对上了 e2b `SandboxService` 的大部分

`src/sandbox/backend.rs:215` 的 trait 和 `packages/orchestrator/orchestrator.proto:235`：

| `SandboxBackend`（Rust，进程内） | e2b `SandboxService` | 对得上吗 |
|---|---|---|
| `SandboxBackendFactory::build` ＋ `start` | `Create` | ✅（注意入口在 factory，不在 backend） |
| `pause` | `Pause` | ✅ |
| `snapshot` | `Checkpoint` | ✅ |
| `stop` | `Delete` | ✅ |
| `update_network_policy` | `Update` 的 `egress` | ✅ |
| `fork` | —— | 我们多出来的 |
| `update_custom_extension_params` | —— | 我们多出来的 |
| —— | `Update` 的 `end_time` | ❌ 我们的 timeout 在 orchestrator 元数据里，不在 backend 上 |
| —— | `List`（`repeated RunningSandbox`，**整台节点**） | 🔴 **缺** |

### 3.2 🔴 缺的那块：节点级枚举 RPC

`SandboxBackend` 的 `runtime_info()` 是**单个沙箱**的事实；
`SandboxBackendFactory` 的四个方法（`build` / `build_from_snapshot` / `decode_paused_state` /
`build_from_paused_state`）**没有任何节点级枚举**。

而 e2b 的 `List` 恰恰是控制面赖以存活的那个 RPC：
`packages/api/internal/sandbox/store.go:140` 的 `Reconcile(ctx, sandboxes []NodeSandbox, nodeID)`
就是拿它的结果对账的，`api` 重启后靠它重新同步（§6.1 的论证依赖这一点）。

⇒ **阶段 3 必须新造一个 `ListSandboxes` 节点级 RPC。** 文档不再宣称「切口已经全在代码里了」——
接缝有 90%，这 10% 得新写。

> ⚠️ v1 把 `runtime_info` 对成了 `List`，而 §6.1 又依赖 `List` 存在。登记在 §10 F6。

### 3.3 `Orchestrator` 的泛型参数就是接缝

```rust
// src/orchestrator/service.rs:93
pub struct Orchestrator<
    S: MetadataStore          = InMemoryMetadataStore,
    F: SandboxBackendFactory  = FirecrackerSandboxFactory,
    P: SandboxPersister       = FileBackedSandboxPersister,
>
```

- `F` → `RemoteSandboxBackendFactory`（gRPC 打到 node）⇒ 从「本机编排器」变成「集群编排器」；
- `S` → **Redis 后端，并且是唯一后端**（见 §4.2、模块文档 D7）⇒ 这是 `api` 能有 N 副本的
  **必要条件，不是充分条件** —— 换完 `S` 还要补齐四个原语（阶段 3 第 2 条）才谈得上多副本正确；
- `P` 留在 node 侧。

🔴 **`S` 今天只有一个生产实现**：`src/orchestrator/store/in_memory.rs:95`。
其余四个 `impl MetadataStore` 全在 `src/orchestrator/tests.rs` 里，是测试替身。
⇒ 不换 `S` 就谈 `api` 多副本，等于两个副本各持一张互不知情的沙箱表。

🔴 **本次重构把 `InMemoryMetadataStore` 作为生产实现整个拿掉，不保留「dev 模式」双后端。**
e2b 的 `packages/api/internal/sandbox/storage/` 下**只有 `redis/` 一个子目录**，
测试跑 testcontainer 里的真 Redis（`packages/shared/pkg/redis/tests.go:17`）。
我们自己也已经付过双实现的代价 —— `CLAUDE.md` 逐字记着：
「a change made to the in-memory store and forgotten for Redis is invisible everywhere else」。
落点见模块文档 §6 D7。

`src/api/impls/mod.rs:65` 的 `ApiImpl` 八个字段里六个是 `Arc<...>`，另外两个是 HTTP 客户端与
代理域名 —— **没有任何一个是对 Firecracker、netns 或 ublk 的直接引用**。这一层已经可以独立装配。

### 3.4 🔴 trait 本身还不能直接当 RPC 面

`pause` 返回 `PausedSandboxCapture`、`snapshot` 返回 `CapturedSandboxSnapshot`、
`startup_artifacts` 返回 `RuntimeArtifactSet` —— 都是**持有本地临时目录的进程内句柄**，过不了线。
`src/sandbox/backend.rs:138-142` 自己写着：「Concrete backends may use it to keep temporary
artifact directories alive until publication finishes.」

这不是要绕开的障碍，它恰好指向正确的解法（见 §5.1）。

---

## 4. 目标形态

### 4.1 进程

```
┌──────────────────────────────────────────────────────────────────┐
│ gateway    (Go,   N 副本)                                         │
│   直读 Redis 活跃态解析路由；冷路径（目标已暂停）才调 api           │
│   ← e2b client-proxy                                              │
├──────────────────────────────────────────────────────────────────┤
│ api        (Rust, N 副本)   镜像 agentenv-runtime --role api      │
│   REST 全量 + 目录(PG) + 活跃态(Redis) + 放置 + evictor + 发布翻牌 │
│   ← e2b api                                                       │
├──────────────────────────────────────────────────────────────────┤
│ node       (Rust, 每节点)   镜像 agentenv-runtime --role node     │
│   SandboxService + ListSandboxes gRPC，纯执行器；本机反向代理      │
│   + uvm-ublk-daemon                                               │
│   ← e2b orchestrator                                              │
└──────────────────────────────────────────────────────────────────┘
```

### 4.2 🔴 存储：三分折叠成二分

**e2b 是二分的**，而且边界很利落 —— `packages/api/internal/sandbox/sandboxtypes/states.go:108-113`
的状态只有四个：

```go
StateRunning      State = "running"
StatePausing      State = "pausing"
StateKilling      State = "killing"
StateSnapshotting State = "snapshotting"
```

**没有 `paused`。** 一个沙箱被暂停之后就**离开 Redis**，变成 PG 里的一行快照/构建记录
（`getPausedSandboxes` 走的是 `GetSnapshotsWithCursor`）。于是：

| | 活着的沙箱 | 暂停的沙箱 |
|---|---|---|
| **e2b** | Redis，api 拥有，N 副本共享 | PG 目录行 |
| **AgentENV 今天** | 每台 node 的 `InMemoryMetadataStore`（不共享）＋ scheduler 的 binding | PG `paused_sandboxes`（**外加 running 行**） |
| **AgentENV 目标** | Redis，`api` 拥有 | PG 目录行（阶段 2 建的表） |

⇒ **`paused_sandboxes` 表不是「搬到 Redis」，是整个消失。** 暂停态本来就是目录的一部分。
今天把活跃态、路由 binding、租约拆成三个存储、三个所有者、三种耐久度，
是 AgentENV 独有的形状，两家参考都没有。

> 🔧 **但折叠之后不是一条记录，是两条。** v3 在这里写过「同一条记录的三个字段」——
> 那句话把路由投影和活跃态 store 混成了一个东西。见 §4.2.1。

这一步同时解决三件事：`api` 能有 N 副本（F1）、13,640 行跨语言登记表消失、
以及第一轮就发现的「中央登记表不是沙箱表，所以 `GET /sandboxes` 只能扇出」。

**租约去哪了 —— 🔴 不是「折叠进 TTL」，这条要单独设计。**

v2 写的是「节点死了 ⇒ 记录到期 ⇒ 不再被路由，这也是 e2b 的做法」。**归因错了**：
e2b 的记录 expiration 是 `MaxLengthInHours`（`packages/api/internal/orchestrator/lifecycle.go:38`）——
**沙箱的最大寿命，小时级**。节点死了记录**不会**很快到期；清理靠
`nodemanager.Sync` → `store.Reconcile(orphanCandidates, nodeID)`。
它的 `UnreachableSince` 消费者为零，恰恰说明它**不靠 TTL 自动接管**。

⇒ TTL 只负责「记录不会永久泄漏」，**不负责「节点失联了要不要接管」**。后者是独立决策，
而且是上一轮那条铁律的落点（`running` / `resuming` 永不可抢）。两种可选形态：

| 形态 | 语义 | 代价 |
|---|---|---|
| **fail-closed（e2b）** | 节点失联 ⇒ 等它回来对账，期间沙箱不可 resume | 一台机器长时间失联 = 那批沙箱长时间不可用 |
| **有条件接管（我们今天）** | 租约过期 ＋ deadline 过期 ⇒ 允许别处认领 | 比 e2b 激进，**正因如此更依赖 fencing**（outcome §3.1 已登记） |

**本文不替这条做决定** —— 它必须与 §6 的 fencing 方案一起裁决。但**必须在阶段 3 建 Redis
记录结构之前定下来**，因为两种形态需要的字段不同（fail-closed 只要 `execution_id` ＋
`node_id`；有条件接管还要 `lease_expires_at` ＋ `sandbox_expires_at`）。

> 🔴 **一个必须同批决定的前提：`local_only` 怎么办。**
> 今天 pause 可能发布失败，留下只有原节点能恢复的沙箱 —— 而目录行是全集群可见的。
> 最优解是让**暂停必然落共享存储**（主仓 `docs/proposals/2026-08-19-aenv-pause-publish-durability.md`，
> 不在本 submodule 内），`local_only` 随之消失，模型精确落在 e2b 上。
> 若该项不能同期完成，退路是目录行上带 `origin_node_id` ＋ `published` 两列，
> resume 时对未发布的行硬钉 origin。**这是阶段 2 建表时就要定的，不能拖到阶段 3。**

### 4.2.1 🔴 路由投影与活跃态 store 是**两个**结构，不是一个

**e2b 是两个，而且刻意隔离**：

| | key | 内容 | 谁写 | 谁读 |
|---|---|---|---|---|
| **路由投影** | `sandbox:catalog:<id>`（扁平） | 五个字段：`OrchestratorID` / `OrchestratorIP` / `ExecutionID` / `StartedAt` / `MaxLengthInHours` | `api` —— 函数名就叫 `addSandboxToRoutingTable`（`lifecycle.go:15`） | `client-proxy` |
| **活跃态 store** | `sandbox:storage:{team}:sandboxes:<id>` ＋ transition / lock / 全局 ZSET | 完整状态机 | `api` | **只有 `api`** |

```
$ grep -rn "api/internal/sandbox" packages/client-proxy/
（零命中）
```

**对我们来说这条比对 e2b 更硬**：gateway 是 Go，`api` 是 Rust。让 gateway 直接反序列化
活跃态 store 的记录，等于把一个**随状态机每次改动而变的 Rust 类型**变成跨语言 wire
contract —— 正是 §2.2 反对的「只为序列化而存在的镜像类型」。
投影是五个字段的冻结契约，状态机怎么改都不动它。

⇒ 三条推论：

1. **`api` 写两次**：一次进 store（权威），一次进投影（派生）。e2b 把后者做成 store 的
   插入回调（`sandbox/store.go:44` 的 `Callbacks.AddSandboxToRoutingTable`），
   **同步调用**，理由逐字写在 `:43`。
2. **阶段 1 建的就是这个投影** —— `services/scheduler/internal/redis_store.go:17` 的
   `redisBindingRecord{Node, ExecutionID}` ＋ 扁平 key `agentenv:scheduler:bindings:<id>`
   已经是它的雏形。⇒ **记录结构在阶段 3 保留，换的是写入方**：
   阶段 1 由 gateway 在响应路径上写（今天 REST 入口是 node，gateway 是唯一同时看得见
   「请求」和「哪台节点答的」的地方）；阶段 3 之后入口就是 `api` 本身，改由它随 store
   插入同步写 —— 和 e2b `Callbacks.AddSandboxToRoutingTable` 逐字同形。
   **所以阶段 1 的产出不作废**（模块文档 D11）。
3. **gateway 永远不读 store**。§4.3 的职责表按这条拆开。

> 一处**不必照抄**：e2b 的 store 按 team 分片（`storage/redis/utils.go:62` 的
> `SameSlot(teamID)`）是 Redis Cluster hash slot 的需求。我们没有租户模型（§4.4），
> store key 可以是扁平的。**隔离的理由是跨语言契约，不是键推导。**

### 4.3 职责边界，逐条

| 职能 | 归属 | 依据 |
|---|---|---|
| 用户 REST 入口 | `api` | e2b `packages/api/internal/handlers/` |
| 鉴权 / 配额 | `api` | e2b `packages/api/internal/team/`（🔴 我们**没有租户模型**，见 §4.4） |
| 模板 / 快照 / 别名目录 | `api`（PG owner） | e2b `packages/db/queries/` |
| 活跃沙箱状态（含租约） | `api` **独占**，不出 `api` 进程 | e2b `packages/api/internal/sandbox/store.go` |
| **路由投影（写）** | `api`，随 store 插入**同步**写 | e2b `orchestrator/lifecycle.go:15` |
| 路由解析（**只读投影**） | `gateway`（直读 Redis） | e2b `client-proxy/main.go:130` |
| 🔴 **暂停沙箱的按需唤醒** | `api`（冷路径，由 gateway 未命中触发） | e2b `proxy.go:109` ＋ `paused_sandbox_resumer_grpc.go` |
| 目录缓存（需跨副本一致） | `api`（Redis） | e2b `packages/api/internal/cache/` |
| 放置（选节点）、节点清册、心跳 | `api`（阶段 4 起） | e2b `placement/` `nodemanager/` `discovery/` |
| 超时驱逐 | `api` | e2b `evictor/evict.go` |
| **发布翻牌** | `api` | e2b `pause_instance.go:71-82` |
| VM 生命周期执行 ＋ 节点级枚举 | `node` | e2b `orchestrator.proto:235` |
| 启动残留回收 | `node` | e2b `packages/orchestrator/pkg/startupreclaim/` |
| 模板构建执行 | `node`（并发由 `api` 控） | e2b `template-manager` 角色 |
| 层 / 块 P2P 分发 | `node` | e2b `chunks.proto` `ChunkService` |
| 本机反向代理（**不含唤醒决策**） | `node` | —— |

### 4.4 🔴 一个贯穿全文的事实：我们没有租户模型

```
$ grep -rn "team_id|TeamId" src/ --include=*.rs
src/api/generated/src/models.rs      # 只是 E2B 兼容 schema 的字段，没有实现
```

`src/api/impls/auth.rs` 自述是 "**presence, not validity**" —— 任何非空值都接受。

影响两处，都要在阶段 3 之前明确：

- **e2b store 的 team 分片对我们不适用**（§4.2.1 末尾）；
- 🔴 **`Reserve` 的四态里，`limitExceeded` 没有可对照的配额主体。**
  模块文档 §8.4 说它「同时带来团队配额」、上表把「鉴权 / 配额」归 `api` 并引 e2b
  `team/` —— 这两处**目前是无对应物**。阶段 3 实际拿到的是**三态**
  （reserved / alreadyInStorage / alreadyPending⇒`waitForStart`）：
  并发去重与在途创建的崩溃恢复照拿，配额那一支等租户模型落地再接。

---

## 5. 🔴 硬前置：存储必须先动

### 5.1 为什么 §3.4 的句柄问题决定了顺序

`SandboxBackend` 过不了线，是因为 pause / snapshot 的产物「写完字节」和「宣布生效」
今天是同一步。拆开它们，就是 e2b 的原样：

```
node 侧   写字节（OSS / POSIX），路径带 build-unique 前缀，返回 snapshot id
   ↓
api  侧   在 PG 里写目录行 —— 这一步才是「这个快照生效了」
```

e2b `pause_instance.go:71-82`：orchestrator 的 `Pause` RPC 成功之后，API 才写
`UpdateEnvBuildStatus(Success)`。**而未翻牌的 build 选不中** —— `status_group = 'ready'`
这个谓词在快照与模板的解析查询里（`get_snapshots_with_cursor.sql:26`、
`get_team_template.sql:32`、`get_team_templates_with_cursor.sql:46`），
不在 `sandbox_resume.go` 里。

> ⚠️ v1 把这个谓词说成是 resume 路径的直接性质。结论不变、机理写错了。登记在 §10 F5。

⇒ **要拆进程，`api` 必须先有一个可以原子提交的地方。而今天没有** ——
快照目录在对象存储里，`src/snapshot/repository/backends/oss/repository.rs` 的 `bind_alias`
自述是「weaker than a true CAS」的读-改-写-回读，`list()` 是
`list_keys_recursive("catalog/records/")` 加每条一次 GET，
`src/api/impls/snapshots.rs:90` 在内存里分页。

### 5.2 顺带把 fencing 的**落点**收敛（不是拿掉）

上一轮阶段 3 的 execution fencing 要解决的是「被取代的 node 发布陈旧快照压掉活的」。
（我们代码里的字段名是 `execution_id`／`execution_authority`，**没有 `expect_` 前缀** ——
`ExpectExecutionID` 是 e2b 的名字，别混用，会 grep 不到。）
目录进 PG 之后，被取代的 node 照样能把字节写完，但它写在一个**没人引用的路径**上，
因为它没有 PG 的提交权。这正是 outcome §3.4 认定为「真正被实证有效」的那条：e2b 的发布权集中。

🔴 **但这不等于 fencing 可以整体不做。** 决策权收进 `api` 之后，竞态没有消失，
它上移了一层 —— `api` 是 N 副本，副本手里的在途操作仍然是一次陈旧提交，而它有提交权。
详见 §6。

### 5.3 存储分层今天是反的

| | 高频、可重建的活跃态 | 低频、不可逆的目录 |
|---|---|---|
| e2b | Redis（`sandbox/store.go` ＋ `sandbox-catalog/catalog_redis.go`） | PostgreSQL（`get_snapshots_with_cursor.sql`，真 keyset 分页） |
| AgentENV 今天 | **进程内存 ＋ PostgreSQL** ＋ generation CAS ＋ 熔断器 ＋ grace ＋ reclaim 定时器 | **对象存储全量扫描** |
| AgentENV 目标 | Redis | PostgreSQL |

dev 集群上 `paused_sandboxes` 的熔断阈值「2/8 就跳闸」（outcome §4.1 A6）——
最严格的一致性机制守着一张几百行、可完全重建的表；
最贵、不可逆的用户数据连条件写都没有。

---

## 6. fencing 收敛到哪里 —— 不是拿掉，是从五处收到两处

### 6.1 🔴 拓扑消灭的是「node 自主发起」，不是竞态本身

一个很自然的推论是：node 变成纯执行器、发布权收进 `api` 之后，
上一轮阶段 3 那套 execution fencing 就可以整体不做了。**这个推论是错的**，
而最硬的反证是 e2b 自己 —— 它已经是这个形态，却仍然在做 execution fencing。

`packages/api/internal/sandbox/storage/redis/scripts.go:33-40` 逐字写着理由：

> When `ARGV[5]` is set, the write only happens if the stored record is still that
> execution. This is the enforcement point for `RemoveOpts.ExpectExecutionID`, and
> **it has to live in here rather than in Go: Add is lockless, so a resume can install
> a new incarnation between a Go-side comparison and this write**, and the SET below
> would then overwrite the new live record with the stale one.

翻成我们的处境：**决策权收进 `api` 层，但 `api` 是 N 副本。
副本 A 手里那次在途的 pause，它的结果仍然是一次陈旧提交 —— 而 A 有提交权。**

**第二个独立证据**：e2b 的 evictor 是裸的 `go sandboxEvictor.Start(ctx)`
（`orchestrator.go:192-196`），**每个 api 进程一个，没有 leader election**；
`evict.go` 里的 `activeEvictions sync.Map` 只是**进程内**去重。
N 个副本跑 N 个 evictor 而不打架，靠的完全是 store 的原子状态转换。
⇒ **多副本决策层必须有原子 CAS，这不是可选项。**

单副本也躲不掉：`api` 重启后要靠 `ListSandboxes`（§3.2）重新同步，
同步窗口内到达的任何操作，都是基于陈旧读做出的决定。

### 6.2 另外三件拓扑管不了的

**① `execution_id` 不消失，它换了工作** —— 从「要检查的谓词」变成「要写进去的路径名」。
e2b 每次 pause 造一个全新 `BuildID`（`packages/api/internal/orchestrator/snapshot_template.go`），
字节落在那个 id 下面，`api` 才翻牌。**这本身就是 fencing，只是以命名的形式存在。**
想连 execution 身份一起拿掉，等于让两次发布写同一个路径 —— 发布权集中当场失效。

**② 孤儿判定必须按 execution 判**，而这和拆不拆进程完全无关。
e2b 的现成缺口在 `packages/api/internal/sandbox/storage/redis/main.go:205-217`：

```go
if raw != nil {
    // Sandbox exists in store, not an orphan.
    continue
}
```

只看 sandbox id 存在性。同 ID 在 B 节点重建之后，A 节点回来的旧化身查库命中的是
**新化身的记录**，于是不判 orphan、永远杀不掉，成为无路由僵尸。

🔴 **我们今天还没有 `KillOrphan`** —— 全仓只有两处注释提到它，都是把它登记为上一轮阶段 3
的待办（`services/scheduler/internal/registry/store_postgres.go:1055`）。
所以这条不是「要修的缺陷」，是**写它的时候别照抄**。

> ⚠️ v1 把它写成了既有代码（「我们的 `Reconcile(roster) → KillOrphan` 正是照它抄的」）。
> 登记在 §10 F4。

**③ 数据面路由那一半本来就不归拓扑管。** 分区期间旧化身还在跑、还在本机反代上应答。
路由层拒旧 execution 保护的是**交互流量（可恢复）**，写路径 fencing 保护的是
**用户工作区（不可逆）** —— outcome §3.1 末尾那条分工，不因为拆进程而改变。

### 6.3 于是：砍一半，留一半

> 🔴 **先纠正一个时态。** v2 这里写的是「**可以不写**」，读起来像在给尚未开工的工作提建议。
> 实际上上一轮阶段 3 的批次 A **已经落地并合并了**：
>
> ```
> f7eef4c feat(config): settle the three switches this guard is rolled out behind
> 7851bb4 feat(node): mint an execution at the claim and refuse a superseded one
> 04c5d37 feat(registry): refuse a write from an incarnation the row has moved past
> bff4993 feat(gateway): tell a node which incarnation the route meant
> ```
>
> 代码里也在：`store_postgres.go:462` `:501` 的 `execution_id` 谓词、`classifyRefusedPause`、
> `ErrExecutionFenced`；`scheduler.proto` 的 `execution_id` / `ExecutionAuthority`；
> `gateway/internal/execution_fencing.go` 528 行 ＋ 1,622 行测试。
>
> ⇒ **它们在本文阶段 3 落地之前是唯一防线，必须留着。** 下面这张表说的是
> 「阶段 3 之后随 `paused_sandboxes` 一起退役」，**不是**「现在别写」，
> 更不是「回退已发布的代码」。登记在 §11 G3。

**阶段 3 之后退役的（现在必须留着）：**

| 项 | 何时退役 | 理由 |
|---|---|---|
| node 侧为「我可能被用户直接调用」写的那套校验 | 阶段 3 | `--role node` 之后用户级 REST 不存在，路径没了 |
| `TransitionSandbox` 上为「node 是决策者」设计的谓词 | 阶段 3 的删除 release | 整个 RPC 面随 `paused_sandboxes` 一起消失 |
| gateway 三态开关里 `observe` 模式的一半 | 阶段 3 | 它当初是为观察「node 会不会自己发起」，那件事不再可能 |

**必须留的（阶段 3 之后仍然承重）：**

| 项 | 落点 | 承重 |
|---|---|---|
| 状态转换时的 execution CAS | Redis Lua 脚本内，一处 | ✅ |
| 提交目录行时的 execution 谓词 | `api` 的 PG 事务内，一处 | ✅ |
| `execution_id` 作为发布路径名 | 快照 / build 路径 | ✅ |
| 孤儿按 execution 判 | `KillOrphan`（新写） | ✅ |
| **过期索引的 member 按 execution 作用域** | 全局过期 ZSET 的 member | ✅ |
| 路由层拒旧 execution | gateway，一处 | 保护另一样东西（交互流量） |

🔴 **两处承重的 CAS 都必须在事务 / 脚本内原子完成**，不能是「先查后写」。
这是 e2b 把 enforcement 放进 Lua 而不是 Go 的全部理由。

🔴 **过期索引那一行容易漏。** ZSET 的 member 如果只是 sandbox id，一次针对死化身的 `ZREM`
会把同 ID 的活化身从驱逐索引里摘掉 —— 沙箱从此永不过期。e2b 的
`storage/redis/utils.go:32-38` 逐字给了理由：member 写成 `team:sandbox:execution`，
「makes every ZREM **structurally safe**: removing a dead execution's member can never
unindex a live one, even when a lockless Add for the same sandbox ID races a Remove
or the evictor's stale sweep.」⇒ 这是 fencing 的第六个落点，排期时别只对着前五行。

### 6.4 一句话

**从五处收到两处，并且其中一处不再承重。**

今天要对齐的五处：登记表 SQL 谓词 / `TransitionSandbox` / 路由层 / node 侧拒绝 /
gateway 三态开关。拆完之后是两处：状态转换与目录提交的 CAS（承重）
＋ 路由层（不承重，保护交互流量）。

写成「fencing 可以拿掉」会导致排期砍错东西 —— 砍掉承重的那一半，留下不承重的那一半。

---

## 7. 分阶段

每一阶段单独上线、单独回退、单独兑现价值 —— 沿用上一轮阶段 0/1/2 已验证有效的做法
（outcome §5.4：**如果一个阶段的回退需要跑新写的回滚逻辑，那它就没有真的可回退**）。

🔴 **删除动作一律不与切换动作同批。** 表和代码的删除放在下一个 release，
让上一批在集群上跑过一个观察期。v1 在阶段 4 里同时写了「删表」和「env 切回」，
那是一句自相矛盾的回退声明（§10 F2）。

### 阶段 1 —— 路由投影变成权威记录 ＋ gateway 直读

> 🔧 **v3 改了这一阶段的定义。** v2 只有「gateway 直读」那一半，而那一半单独做
> **买不到它宣称的东西**（下面的表）。登记在 §11 G2。

**前置**：`scheduler.redis_addr` 要配上。**这已经支持了**
（`services/scheduler/cmd/main.go:266`：为空则回落内存 bindings），不需要任何新代码。

#### ① 让路由投影从「心跳派生的缓存」变成「权威记录」

> 🔧 **v4 收窄了这一条。** v3 写的是「CREATE / RESUME / FORK 都改走事件」。
> 其中 CREATE 与 FORK **今天已经是同步写的**，改走事件是降级 —— 见下面第 1 点。

**记录结构已经在了**，`services/scheduler/internal/redis_store.go:17`：

```go
type redisBindingRecord struct {
	Node        Node   `json:"node"`
	ExecutionID string `json:"execution_id,omitempty"`
}
```

扁平 key `agentenv:scheduler:bindings:<id>`，值是「节点 ＋ 化身」。**这就是 e2b 的
`SandboxInfo`**（`packages/shared/pkg/sandbox-catalog/catalog.go:11-18`）少两个字段。
⇒ 阶段 1 不是新建一个过渡结构，是**把已有结构补成 §4.2.1 的投影**。四件事：

**1. CREATE / FORK：不动，它已经是同步的。**
`services/gateway/internal/server.go:557-561` 在**响应路径上**写，还从响应头取到了化身
（`bff4993` 落的）：

```go
executionID := executionIDFromResponse(resp.Header)
if sandboxID, ok := sandboxIDFromHeaders(resp.Header); ok {
    s.recordAssignment(recordCtx, sandboxID, node, executionID, "response_header")
```

e2b 明确要求这条同步 —— `packages/api/internal/sandbox/store.go:43`：
「`AddSandboxToRoutingTable` should be called **sync** to prevent race conditions where we
would know where to route the sandbox」。把它挪到尽力而为的事件流上，等于自己造出
「create 已返回、gateway 还查不到」的窗口。

**2. RESUME：补进同一个同步机制。**
`shouldRecordAssignment`（`server.go:655-670`）只匹配 `POST /sandboxes`、`/sandboxes-cold`、
`/{id}/fork` —— resume 不在其中。**这是投影今天真正缺的那条写。**

**3. PAUSE / DELETE：这两条才需要事件通道。**
DELETE 连方法都不匹配（`shouldRecordAssignment` 首行 `if r.Method != http.MethodPost`）。
通道已经是通的，只是接收端在丢弃 —— `services/scheduler/internal/service.go:425-432` 逐字：

```go
func (s *Service) ReportSandboxEvent(...) (*schedulerv1.ReportSandboxEventResponse, error) {
	s.logger.Debug("scheduler ignored sandbox event batch", ...)
	return &schedulerv1.ReportSandboxEventResponse{}, nil
}
```

删除要**按 execution 守卫**，对标 e2b `catalog_redis.go` 的 `DeleteSandbox`：
`if info.ExecutionID != executionID { return nil }`。
⇒ proto 加一个字段 `SandboxEvent.execution_id`（今天只有 `SandboxRosterEntry` 有它，
`scheduler.proto:309`）。本仓无兼容包袱，additive 直接加。

**4. 🔴 TTL 从 30s 改成什么 —— 这一条今天没有答案，开工前要拍板。**
e2b 用 `MaxLengthInHours`（`orchestrator/lifecycle.go:38`）：**建时确定、之后不变**的上界，
所以能直接当 Redis TTL。我们没有这个量：

```
$ grep -rn "max_instance_length|MaxLength|max_sandbox_lifetime" config/default.toml src/cfg.rs
（无匹配）
config/default.toml:215:  default_sandbox_timeout_secs = 15   # 可被 SetTimeout 反复推后
```

| 选项 | 做法 | 代价 |
|---|---|---|
| **(a) 引入 `max_sandbox_lifetime` 上界**（建议） | 对标 e2b，TTL 一次写定，永不续期 | 给用户一个新的硬上限 |
| (b) TTL ＝ 当前 deadline ＋ 宽限 | 每次改 timeout 都要 `EXPIRE` 续期 | 正是 e2b 在 `operations.go:159-215` 里靠 `redis.KeepTTL` ＋ ZAdd 重打分处理的那类问题 —— **属于阶段 3 的机制** |

⇒ **建议 (a)**。阶段 1 的价值全在「投影的存活不依赖 scheduler 在线」，
而 (b) 让它重新依赖一条周期性写路径。

🔴 **心跳对账保留为修复路径，不是主路径。** 事件是尽力而为的（节点侧广播无订阅者即丢弃），
丢了必须有东西把投影修回来。e2b 是同一形状：`StoreSandbox` 在 create 时写，
`Reconcile` 兜底修。

#### ② gateway 直读

把 `RedisBindingStore.Get` 提到 `services/shared/`；gateway 先读 Redis，命中直接转发，
未命中才 `LookupNode`。这就是 e2b `client-proxy` 的形状
（`paused_sandbox_resumer_grpc.go`：只有需要 resume 才调控制面）。

🔴 **两件不能省的**：
1. gateway 侧要**合成完整的 `LookupNodeResponse` 语义** —— `decideFencing` 依赖
   `location` / `origin_node_id` / `execution_id` / `execution_authority`，
   还要复刻 `authorityFor` 的判定；
2. **roster 回落不能丢**。`lookup.go:170-173` 写明它覆盖两个窗口：「心跳迟到」
   和「别的节点的对账把 binding 删了而这台还在报」，`rosterHolder`（`lookup.go:450`）
   本身带 execution 感知的仲裁与 freshness tie-break。Redis 未命中必须回落，不能答 404。

#### 为什么只做②不够

投影**写在 create，但只靠心跳续期**，TTL 30s。`shouldRecordAssignment`（`services/gateway/internal/server.go:655-670`）
只在 `POST /sandboxes`、`/sandboxes-cold`、`/{id}/fork` 上为真 ——
**普通数据面流量不续 binding**，而心跳打的是 scheduler。所以只做②的话：

| scheduler 停机时长 | 结果 |
|---|---|
| < 30s（binding TTL，`store.go:11`） | ✅ 直读命中，流量正常 |
| **> 30s** | binding 全部过期 → 直读全部未命中 → 回落 `LookupNode` → 打到已停机的 scheduler → **503** |

⇒ 只做②买到的是**30 秒宽限**，不是「控制面挂掉不再打死数据面」。
而①之后 binding 的存活不再依赖 scheduler 在线，标题才成立。

**规模**：400–500 行含测试。
（v2 写的 300–400 只覆盖②；v3 写的 600–800 假设 CREATE / FORK 也要改造，而它们不用。）

**兑现**：控制面挂掉不再打死数据面 —— 今天
`services/gateway/cmd/main.go:27` 的 `grpc.NewClient` 没有 retry policy、没有 `WaitForReady`，
`server.go:375` 把 `Unavailable` 直接翻成 503 交给客户端；scheduler 每一次重启、OOM、
被驱逐，窗口内所有运行中沙箱的交互流量同时 503，而 gateway 不重试。

**回退**：两个开关分开 —— 读侧回落 `LookupNode`；写侧回落「心跳派生 ＋ 30s TTL」。

**验证判据（要有分辨力）**：
scheduler 缩到 0 **持续 5 分钟**（远超 binding TTL），运行中沙箱代理请求成功率不变。
🔴 **必须带对照组**：把写侧开关切回心跳派生，同样 5 分钟 —— 成功率应在 ~30s 后掉到 0。
两个相位给出相同答案则探针作废（outcome §5.3 的教训）。

### 阶段 2 —— catalog 进 PG

**前置**：无。与阶段 1 可并行。

**做**：PG 新建 `templates` / `builds` / `snapshots` / `aliases`；
`SnapshotRepository` 的目录读写走 PG；对象存储只留字节；`list()` 换成 keyset 分页 SQL。
**目录缓存同批进 Redis** —— 理由在 §8 陷阱 3：e2b 的
`packages/api/internal/cache/` 三个子包全部是 Redis ＋ DB 回落，所以它压根没有
「跨副本失效」这个问题。目录在哪，目录缓存就在哪，两件事一起做才不用做两次。

🔴 **建表时就要定的两件**：
1. **暂停态的落点**（§4.2 的红框）：`local_only` 是消掉还是用
   `origin_node_id` ＋ `published` 两列表达。拖到阶段 3 会变成一次 schema 重做。
2. **`builds` 表的形状要能承载构建队列** —— e2b 有
   `get_concurrent_template_builds` / `active_template_builds` 做并发控制，我们今天没有。

🔴 **它的主要理由不是性能。** 今天目录小，`list_keys_recursive` ＋ N 次 GET 的成本
在 dev 上量不出来 —— 当性能优化去论证会论证不动。真正的理由是 §5.1：
`api` 层需要一个能原子提交的地方，而对象存储不是。

**回退**：双写。**双写保留到阶段 3 上线并稳定之后再拆** —— 在此之前，
回退是把读侧开关切回对象存储，零数据损失。

**验证判据**：目录里塞 10k 条记录，`GET /snapshots?limit=10` 的对象存储请求数为 0。

### 阶段 3 —— 活跃态折叠进 Redis ＋ `--role` 拆分 ＋ `api` N 副本

**前置**：阶段 2。

🔴 **这三件事是同一批，不能拆。** 理由是 §0 的第二条硬约束：只有 `api` 持有 Redis 凭据。
先搬活跃态、后拆 role，等于把 Redis 凭据发到每台跑用户代码的 KVM 机器上，
把上一轮兑现的 G7 撤销掉；先拆 role、后搬活跃态，等于让 `api` 以单副本上线一段时间，
而那期间它是一个比今天更集中的新单点。

**它们本来也是同一件事**：store 后端是 role 的函数 —— `--role all` 用进程内 store，
`--role api` 用 Redis store。所以回退是一个 flag。

**做**：

```
server --role api|node|all
  all  = 今天的行为（进程内 store + 本机执行），默认值，零变化
  api  = ApiImpl + Orchestrator<RedisMetadataStore, RemoteSandboxBackendFactory>
  node = SandboxService + ListSandboxes gRPC + ublk + 本机反向代理，不起用户 REST
```

抄 e2b 的角色开关（`packages/orchestrator/pkg/cfg/service.go:17`：
orchestrator 与 template-manager 共用一个二进制）。

**同批必须做的六件**：

1. **新造 `ListSandboxes` 节点级 RPC**（§3.2）。没有它 `api` 重启后无法对账，
   §6.1 的整个论证也落空。
2. 🔴 **活跃态 store 的四个原语要照 e2b 补齐。** 逐条设计（含 e2b 的具体机制与我们的落点）
   在**模块文档 §8**，这里只列清单：
   - **闭包式 update**：`update_if_state<F>` 的闭包**保留**，「持有锁跑闭包」这条契约也
     **逐字保留** —— 变的只是锁的作用域（进程 → 集群）。e2b 的
     `operations.go:159-215` 就是这么写的。要加的是 KeepTTL 等价物 ＋ **闭包执行时限**
     （分布式锁有 TTL，慢闭包会活过它）。
   - **transition key 三件套**：`transitionKey`（带 TTL 与 owner id）＋ `resultKey`
     ＋ **完成回调**。🔴 这才是崩溃恢复的边界 —— 分布式锁只覆盖「读-判断-写」那一小段，
     e2b 在进入等待前就 `releaseFunc()` 了。副本在 `Pausing` 中途死掉，
     今天单进程重启会清空内存 store，**上 Redis 之后状态会永远卡住**。
   - **全局过期 ZSET**：替掉 `list_expired` 的全表扫，`ZRangeByScore` 每轮有界；
     驱逐前在锁内**重校验过期**；配一个 healer（每副本跑、`ZADD NX`、grace period、可热关）。
   - **`Reserve` 四态返回**：`alreadyPending` 那一支返回 `waitForStart` 让第二个调用方
     **等结果而不是报 409** —— 这是「客户端重试」与「用户真的建了两次」的分水岭。
     它同时带来团队配额与在途创建的崩溃恢复。
3. **`node` 角色不再暴露用户级 REST** —— 这就是上一轮阶段 4 的「node API 收窄」，
   outcome §3.3 论证过它必须与 fencing 同批。`src/api/control_plane_gate.rs`
   的 `GET /sandboxes` 豁免随之删掉（集群列表现在是一条 SQL）。
4. **`proxy_routes` 与 `SandboxHandle` 分家。** 两者今天在 `Orchestrator` 里同生共死，
   拆 role 时必须分开：
   - `SandboxHandle` ＝ `Arc<Mutex<Box<dyn SandboxBackend>>>`（`service.rs:41`），
     活 VM 的**进程内句柄**：不可序列化、**永远不上 Redis**，留在 `node` 角色
     （e2b 同形 —— `pkg/server/sandboxes.go:568` 从 `sandboxFactory.Sandboxes.Items()` 取）。
     `api` 侧拿到的是它的**远程存根**（`RemoteSandboxBackendFactory`）。
   - `proxy_routes` 服务 node 的**本机反向代理**，同样留在 `node` 角色。

   不分开的话，`api` 会为了一张路由表而持有一堆它根本构造不出来的句柄。

5. 🔴 **auto-resume 必须从 `node` 迁到 `api` —— 这是全文最大的一处空缺。**

   ```
   src/api/proxy.rs:878   Ok(ProxyLookupResult::Paused { auto_resume: true }) => {
   src/api/proxy.rs:891       try_auto_resume(api_impl, sandbox_id).await?;
   ```

   它**不在 `src/api/impls/` 的用户级 REST 里，在数据面反代路径上** —— 而 §4.3 把
   「本机反向代理」留给 `node`。⇒ 什么都不做的话，`--role node` 之后它照样跑，
   **node 依然自主发起 resume**：§6.1「拓扑消灭的是 node 自主发起」的前提当场被架空，
   而且 `try_auto_resume(api_impl, ...)` 还要求 node 保留一个能做决定的 orchestrator，
   与「纯执行器」直接冲突。

   照 e2b 的形状搬 —— **gateway 未命中投影 ⇒ 调 `api` 唤醒**
   （`packages/client-proxy/internal/proxy/proxy.go:109` 逐字：
   「catalog miss, attempting resume via api」，走 `paused_sandbox_resumer_grpc.go`）。
   `node` 侧只保留「已经在跑的沙箱」的反代，不保留唤醒决策。

6. **启动残留回收归 `node`** —— 对标 e2b `packages/orchestrator/pkg/startupreclaim/`。
   node 重启后本地残留由它自己扫，不要等 `api` 对账。

🔴 **但不要把 fencing 整体砍掉。** 该砍的和该留的逐条列在 §6.3。

**回退**：`--role all`。进程内 store 与 Redis store 两条代码路径在这一批期间**都保留**。

**删除动作放下一个 release**（观察期之后）：

```
services/scheduler/internal/registry     10,076 行（3,483 非测试）
src/orchestrator/paused_registry           3,564 行
                                         --------
                                          13,640 行
```

外加 **`InMemoryMetadataStore` 的生产实现**（`src/orchestrator/service.rs:94` 的默认类型
参数、`:143` 与 `:155` 的便捷构造；全仓 60 处引用，生产路径只有 `service.rs` ＋
`mod.rs` 的再导出）。

> 🔧 **v3 把它从「同批必做」移到了这里。** v2 让它与切换同批，
> 而回退声明写的是「`--role all` ＋ 两条 store 路径都保留」—— 删掉了就没有可回退的路径，
> 且它违反本节抬头刚立的规矩。登记在 §11 G1。

外加 `paused_sandboxes` 表、`DiscardBreaker`、`Grace`、`paused_sandboxes_reclaim_idx`，
以及 `_impl-D7-contract-tests.md` §4 里 S1 / S2 / S4 / S5 / S7 / S8 ——
这六条是「一个语义两个语言实现」的产物，接缝没了它们就没了。

> **诚实对照**：e2b 的 Redis 沙箱 store 非测试约 2,100 行，和我们 Go 侧的 3,483 行同量级。
> **省的不是行数，是那条跨语言接缝本身。**

**成本**：Redis 从可选变成硬依赖，且它现在承载活跃沙箱状态 —— 必须 HA，必须持久化策略明确。
这一条要单独过一遍。

**验证判据**（三条，第三条是这一阶段真正的风险）：
1. `api` 副本数 2，创建一个沙箱，从**另一个副本**读到它；
2. 杀掉写入的那个副本，沙箱仍可 pause / resume；
3. 🔴 **两个副本同时对同一沙箱发相反操作**（如 pause 与 delete），
   结果必须是其中一个成功、另一个收到明确冲突 —— 不能两个都「成功」。
   对照组：把**这一项**的分布式锁关掉重跑，必须出现双执行，否则探针没有分辨力。

### 阶段 4 —— 折叠 `scheduler` 进 `api`

**前置**：阶段 3。

**做**：把 `placement` / `filter` / `strategy` / `kubernetes_discovery` / `node_registry`
的等价物 port 进 `api`（Go 侧 2,143 行量级）；心跳直接打到 `api`；`scheduler` 进程下线。

🔴 **硬前提是阶段 3 第 5 条已经落地。** `scheduler` 下线意味着 `LookupNode` 消失，
而它今天正是 gateway 的未命中回落。冷路径此时必须已经改成「调 `api` 唤醒」，
否则每一个暂停沙箱的第一次访问都无人接管。

**为什么值得**：阶段 3 之后 `api` 已经需要节点清册来解析 node endpoint。
保留 `scheduler` 意味着**两个进程各自维护同一份状态**，正是 §0 第一条硬约束批评的形状。
折叠掉之后进程表收敛到 e2b 的三个，且少一跳 RPC。

**回退**：🔴 **不是一个开关。** 这一刀同时把心跳搬到了 `api`，
回退时 scheduler 手里没有节点清册，`Schedule` 会对着空集群做决定。
所以回退是一次**协同变更**：`api` 侧的放置开关切回 `scheduler` ＋
**节点侧的心跳目标同时改回去**，两者必须同批。
`scheduler` 二进制与清单保留一个 release；节点的心跳目标做成可热改的配置项，
否则回退需要重启每一台 node。

**验证判据**：`scheduler` Deployment 缩到 0 副本，创建 / 恢复 / 列表全通。

---

## 8. 已知陷阱

1. 🔴 **别把两把锁搞混。** 生命周期互斥**今天就不靠 `RwLock`** ——
   靠的是 `MetadataStore` 的 `update_state_if_state` / `update_if_state` /
   `wait_while_in_states`（`src/orchestrator/store/mod.rs:61` `:73` `:96`）。
   它们是 trait 方法，换成 Redis 实现就自动跨副本了，**不需要新造锁**。
   而 `RwLock<HashMap<SandboxId, SandboxHandle>>`（`service.rs:101`）守的是活 VM 的
   进程内句柄，**不可序列化、留在 `node`**。真正要补的是
   `update_if_state` 的闭包契约（阶段 3 第 2 条）与 TTL 有界的崩溃恢复锁（第 3 条）。

2. **多副本 `api` 的 evictor 会重复驱逐**，而且**驱逐的数据源也要换**。
   e2b 没有 leader election（`orchestrator.go:192-196` 是裸的 `go Start(ctx)`），
   它靠的是三件配套：**全局过期 ZSET**（`items.go:20`，`ZRangeByScore` 每轮有界）
   ＋ **锁内重校验过期**（防与 `SetTimeout` 竞态，已有转换在途则 `ErrEvictionInProgress`）
   ＋ **healer**（`heal.go`，补回丢失的索引成员）。
   ⇒ 我们今天的 `list_expired(now)` 是全表扫，N 个副本各扫一遍既浪费又没有「谁负责这一批」
   的概念。设计见模块文档 §8.3。

3. **多副本 `api` 的目录缓存 —— e2b 的答案是「缓存本身进 Redis」。**
   `packages/api/internal/cache/` 的三个子包（`templates` / `snapshots` / `sandboxcounts`）
   **全部是 `redis.UniversalClient` ＋ DB 回落**，所以 `pause_instance.go:85` 的
   `snapshotCache.Invalidate` 就是一次 Redis 删除 —— **根本不存在「跨副本失效协议」
   这个问题**，它是被存储选型消掉的，不是被协议解决的。
   ⇒ 这不是「阶段 3 之前要定」的开放问题，是**阶段 2 的一部分**：目录进 PG 的同时，
   目录缓存进 Redis。留在 `node` 的只有块 / 层这类**内容寻址、天然可各自为政**的缓存
   （模块文档 D2）。

4. 🔴 **构建沙箱必须被 `ListSandboxes` 排除，否则 `api` 会把它们当孤儿杀掉。**
   e2b 在 `packages/orchestrator/pkg/server/sandboxes.go:577-582` 逐字写了这条：
   「Build sandboxes are not owned by the API and must never show up here, or the API
   would treat them as orphans and kill them.」它用 `APIStoredConfig == nil` 做标记。
   我们的 `src/template/runner.rs` 正是在节点上跑构建沙箱 —— 拆分之后必然撞上。
   ⇒ `ListSandboxes` 必须只报**控制面拥有的**沙箱，所有权标记要显式，不能靠推断。

5. **`RuntimeArtifactSet` / `PausedSandboxCapture` 的所有权。** 见 §3.4。
   建议把 `SandboxBackend` 拆成两个 trait —— 一个可远程（返回 id 与事实），
   一个纯本机（返回句柄）—— 而不是在同一个 trait 上加 `#[cfg]`。

6. **envd access token seed 必须集群统一。**
   `src/sandbox/access.rs` 的 seed 没配时来自节点本地
   `{home_path}/secrets/sandbox-access-token-hash-seed`。`api` 角色一旦要签发 token，
   两台节点推不出同一个值，**而且做错了是静默失效**。排进阶段 3 的前置。

7. **`GetMany` 的全有或全无（S4）语义在阶段 3 之后会变形。**
   今天节点拿「缺行」当删除本地产物的授权。Redis pipeline 的部分失败长得一模一样。
   ⇒ 新 store 必须把「本次实际覆盖的 id 集合」带在响应里，
   让调用方在把缺失当成删除授权之前先断言一次。这条在 PG 版本里就是遗留项，别原样继承。

8. **Redis 的持久化与 HA 策略。** 阶段 3 之后它承载的是活跃沙箱状态，
   丢了等于整个集群失忆。e2b 的对应组件是托管 Redis。我们要么同等对待，
   要么明确接受「Redis 全丢 ⇒ 靠 `ListSandboxes` 从所有 node 重建」并验证这条路径。

9. **探针要先自证。**
   outcome §5.3 记录过一次真实翻车：合成行在任何相位都认领不了，
   「看着像被拒了」其实是没有分辨力。本轮每条验证判据都要有 A/B/A 或对照组。

---

## 9. 明确不做

- **不把 `api` 层写成 Go。** 理由见 §2.2。注意这与 §2.3「进程表收敛到 e2b 的三个」
  不矛盾：收敛的是边界，不是语言。
- **不上多集群 / edge pool。** e2b 的 `packages/api/internal/clusters/` 与 edge pool 是它
  多区域部署的产物。我们的 scheduler 对多集群刻意 fail-closed（`_impl-D6-scheduler.md` §6.5）。
- **不在阶段 3 之前碰 `src/api/impls/auth.rs`。** 那句 "presence, not validity" 是既有问题，
  而阶段 3 的边界收窄会让它不再是主要防线。先收边界，再决定要不要签发真凭据。
- **不引入 ClickHouse。** e2b 用它承载指标与事件；我们今天只有 Prometheus。
  这是一个独立的取舍，不应该搭在服务拆分这一刀上。

---

## 10. v1 对抗审查的处置

> ⚠️ **这张表是历史记录，不是当前状态。** 其中 F7（「阶段 1 改为 300–400 行」）与
> F10（「阶段 3 同批四件事之一」）已被 v3／v4 覆盖 —— **以正文为准**。

| # | 审查发现 | 处置 |
|---|---|---|
| **F1** | 阶段 3 无法以 N 副本部署：`MetadataStore` 生产实现只有 `InMemoryMetadataStore` | **重排阶段**。`api` N 副本成为目标，活跃态折叠进 Redis 与 role 拆分并为一批（§4.2、阶段 3） |
| **F2** | 阶段 4 同时写「删表」与「env 切回」，回退声明自相矛盾 | §7 抬头立规：**删除不与切换同批**；阶段 2 双写保留到阶段 3 之后 |
| **F3** | 「44,214 行 / 5%」含 17,910 行生成代码，分母标签也写错 | §2.1 改为 26,304 行 / 8.1%，并标注偏差方向 |
| **F4** | `KillOrphan` 被写成既有代码，实际不存在 | §6.2② 改为「还没有，写的时候别照抄」 |
| **F5** | `status_group='ready'` 挂在 `sandbox_resume.go` 上，实际在解析查询里 | §5.1 改正落点 |
| **F6** | §3.1 把 `List` 对成 `runtime_info`，而 §6.1 依赖 `List` 存在 | §3.2 独立成节：**缺一个 `ListSandboxes` 节点级 RPC**，阶段 3 同批新造 |
| **F7** | v1 §7.0「150 行」低估 | 阶段 1 改为 300–400 行，并列出两件不能省的 |
| **F8** | 保留 `scheduler` 会造成双节点清册，只算了移植成本没算耦合成本 | §2.3 推翻 v1 论断；新增阶段 4 折叠 `scheduler` |
| **F9** | 阶段编号暗示了不存在的依赖 | gateway 直读 Redis 提为阶段 1，并注明与阶段 2 可并行 |
| **F10** | 启动残留回收归谁没写 | §4.3 归 `node`；阶段 3 同批四件事之一 |
| **F11** | 多副本缓存失效没提 | §8 陷阱 2 |

**经受住审查、未改动的**：§2.2（不写 Go）、§5（存储必须先动）、§6（fencing 收敛）、
§1（命名约定）。其中 §6 反而拿到了第二个独立证据 —— e2b 的 evictor 无 leader election，
其安全性完全来自 store 的原子性（§6.1）。

---

## 11. v2 对抗审查的处置

> ⚠️ 同上。G11 的「重排为六件」在 v3 一度收敛成五件，v4 因为补入 auto-resume 又回到六件 ——
> 数字巧合，组成不同。**以正文为准**。

| # | 审查发现 | 处置 |
|---|---|---|
| **G1** | 阶段 3 同批「删掉 `InMemoryMetadataStore`」与回退声明「`--role all` ＋ 两条 store 路径都保留」自相矛盾，且违反本节抬头刚立的「删除不与切换同批」 | 移进「删除动作放下一个 release」块；必做清单回到不含删除动作 |
| **G2** | 阶段 1 的兑现被高估：binding **只靠心跳续期**（`server.go:655-670` 的 `shouldRecordAssignment` 只覆盖三条 POST 路径），scheduler 停机 >30s 一样全断；验证判据前 30 秒会假通过 | **重定义阶段 1**（§7 阶段 1）：加①「事件驱动的权威 binding」；验证判据改为停机 5 分钟 ＋ 对照组 |
| **G3** | §6.3「可以不写的」是时态错误 —— 上一轮阶段 3 批次 A 已合并（`f7eef4c` `7851bb4` `04c5d37` `bff4993`），照此执行等于回退已发布代码 | §6.3 改为「阶段 3 之后退役，现在必须留着」，并列出提交与代码位置 |
| **G4** | 全文用 `expect_execution_id`，我们代码里叫 `execution_id`（`expect_` 是 e2b 的名字） | §5.2 加澄清，正文改名 |
| **G5** | 「租约折叠进 TTL，节点死了记录到期，这也是 e2b 的做法」—— 归因错误（e2b 的 TTL 是沙箱寿命，清理靠 `Reconcile`），且真正的问题「节点失联要不要接管」被掩盖 | §4.2 重写：TTL 只防泄漏；接管形态列成两选一，**必须在阶段 3 建结构前裁决** |
| **G6** | 跨副本每沙箱互斥完全没提，而 e2b 有专门的 `lock.go` | 阶段 3 必做第 2 条 ＋ §8 陷阱 1 |
| **G7** | 并发 create 去重（e2b `Reserve` 的 `waitForStart`）没提 | 阶段 3 必做第 3 条 |
| **G8** | §0 硬约束 2「只有 `api` 持有共享存储凭据」与阶段 1（gateway 直读 Redis）自相矛盾 | 改为「`node` 角色不持有」，并注明约束对象从来只是 `node` |
| **G9** | 阶段 4 回退声明不成立 —— 心跳也搬了，scheduler 回退后没有节点清册 | 改为「协同变更」，并要求节点心跳目标可热改 |
| **G10** | 阶段 3 验证判据不测最危险的路径（两副本同时操作同一沙箱） | 改成三条，第三条带关锁对照组 |
| **G11** | 「同批必须做的四件」下面列了 5 条 | 随 G1／G6／G7 一并重排为六件 |
| **G12** | §2.1 把 e2b 的 `internal/cache/`（Redis 共享目录缓存）映射成「node 内存 ＋ 本地 RocksDB」 | 改为「无对应物」——我们今天没有跨副本目录缓存 |

**这一轮经受住审查的**：§2.3（进程表收敛）、§3.2（缺 `ListSandboxes`）、
§4.2 的二分模型本身、§5（存储先动）、§6.1–6.2 的论证、构建沙箱那条陷阱。
§6.1 又多一个佐证 —— `lock.go` 的存在说明 e2b 认为「原子 CAS ＋ 分布式锁」两件都要，
不是二选一。

---

## 12. v3 对抗审查的处置

这一轮的着眼点是「照着能不能执行」，所以 findings 比前两轮更靠近实施面。

| # | 审查发现 | 处置 |
|---|---|---|
| **H1** | 阶段 1 的「TTL ＝ 沙箱寿命」**无量可取** —— `grep max_instance_length` 全仓无匹配，我们只有一个可被 `SetTimeout` 反复推后的 deadline | 阶段 1 第 4 点：列成 (a) 引入上界 / (b) 续期 两选一，**建议 (a)**，并注明 (b) 等于把阶段 3 的机制提前引进来 |
| **H2** | 阶段 1 ① 把 CREATE / FORK 这条**今天已经同步、且已带化身**的写（`server.go:557-561`）降级成异步事件；e2b `store.go:43` 明确要求它同步 | 阶段 1 ① 重写成四点：CREATE/FORK 不动、RESUME 补进同一机制、PAUSE/DELETE 才走事件、TTL 单独裁决。规模从 600–800 回到 400–500 |
| **H3** | **auto-resume 没有被任何一张职责表分配。** 它在 `src/api/proxy.rs:878` `:891` 的数据面反代路径上，而 §4.3 把本机反代留给 `node` ⇒ `--role node` 之后 node 仍自主发起 resume，§6.1 的前提被架空；阶段 4 删掉 `LookupNode` 后冷路径无人接管 | 新增阶段 3 第 5 条（迁到 `api`，照 `proxy.go:109` ＋ `paused_sandbox_resumer_grpc.go`）；§4.3 加「暂停沙箱的按需唤醒」一行；阶段 4 加硬前提 |
| **H4** | 路由投影与活跃态 store 被写成「同一条记录的三个字段」，而 **e2b 是两个结构且刻意隔离**（`grep api/internal/sandbox packages/client-proxy/` 零命中） | 新增 §4.2.1 与模块文档 D11；§4.3 表拆成四行；并指出**阶段 1 建的就是这个投影 —— 记录结构在阶段 3 保留，换的只是写入方**（gateway → `api`），不是过渡投入 |
| **M5** | §3.3「Redis store 是 N 副本的**充要条件**」逻辑过强 —— 文档自己另列了四个原语 | 改为「必要条件，不是充分条件」 |
| **M6** | 阶段 3 第 4 条句子被编辑破坏：`proxy_routes` 出现在标题后再未出现，末尾「前者……后者……」悬空 | 重写成两个子项 ＋ 一句「不分开会怎样」 |
| **M7** | 阶段 3 判据 3 的对照组指向「第 2 项」，而第 2 项与分布式锁无关 | 改指本项 |
| **M8** | §6.3 承重表缺一行：过期索引的 member 必须按 execution 作用域 | 加第六行 ＋ 逐字引 `storage/redis/utils.go:32-38` 的 "structurally safe" 论证 |
| **M9** | §10 F7／F10、§11 G11 的处置栏与正文已对不上 | 两节抬头加「历史记录，以正文为准」 |
| **M10** | §8 陷阱 3 描述对了问题却没给 e2b 的答案 —— e2b 的 `cache/` 三个子包**全部是 Redis ＋ DB 回落**，失效问题是被存储选型消掉的 | 陷阱 3 重写；并把目录缓存**并进阶段 2**（目录在哪缓存就在哪） |
| **新事实** | **我们没有租户模型**（`grep team_id src/` 只命中 generated；`auth.rs` 是 presence-only） | 新增 §4.4：e2b 的 team 分片不适用；`Reserve` 阶段 3 实际只拿到**三态**，`limitExceeded` 无配额主体 |

**这一轮经受住审查的**：§2.3（进程表收敛到三个）、§4.2 的两分模型、§5（存储先动）、
§6.1–6.2 的全部论证、§8 陷阱 4（构建沙箱必须排除）、阶段 1 ② 的两件不能省。

§6.1 又添一条佐证：`storage/redis/utils.go:22-25` 说明 e2b 连 pub/sub 都只用**一条全局通道
＋ payload 内路由键**（"one connection per API pod is sufficient"）——
它在多副本上花的心思是系统性的，不是零散补丁。

---

## 附：证据索引

**AgentENV**

| 事实 | 位置 |
|---|---|
| node 的 PG 后端已被物理删除，遇到 `postgres` 直接 `bail!` | `src/orchestrator/paused_registry/mod.rs:436` |
| `MarkRunning` 从不建行 ⇒ 中央登记表不是沙箱表 | `services/scheduler/internal/registry/store.go` |
| gateway 每请求一次 `LookupNode`，无缓存、无重试 | `services/gateway/internal/server.go:252`、`cmd/main.go:27`、`server.go:375` |
| roster 回落覆盖的两个窗口 | `services/scheduler/internal/lookup.go:170-173`、`:450` |
| binding TTL 30s 对心跳 5s | `services/scheduler/internal/store.go:11`、`config/default.toml:401` |
| scheduler `replicas: 1` 及其理由 | `deploy/k8s/base/scheduler-deployment.yaml` |
| 快照目录全量扫描 ＋ 每条一次 GET；别名是「weaker than a true CAS」 | `src/snapshot/repository/backends/oss/repository.rs` |
| 列表在内存里分页 | `src/api/impls/snapshots.rs:90` |
| 驱逐在 node 上 | `src/orchestrator/service.rs:2159` |
| 拆分接缝：trait / 泛型 | `src/sandbox/backend.rs:215` `:313`、`src/orchestrator/service.rs:93` |
| 🔴 生产 `MetadataStore` 只有一个实现 | `src/orchestrator/store/in_memory.rs:95` |
| `KillOrphan` 尚不存在，仅在注释里作为待办 | `services/scheduler/internal/registry/store_postgres.go:1055` |
| API 层已可独立装配 | `src/api/impls/mod.rs:65` |
| 🔴 `ReportSandboxEvent` 收到就丢弃 | `services/scheduler/internal/service.go:425-432` |
| 五种沙箱事件已定义、节点已在发 | `services/api/proto/scheduler.proto:316-324` |
| 🔴 普通数据面流量不续 binding | `services/gateway/internal/server.go:655-670` |
| execution fencing 批次 A 已合并 | `f7eef4c` `7851bb4` `04c5d37` `bff4993`；`store_postgres.go:462` `:501` |
| 🔴 CREATE / FORK 的投影写**已经是同步的**，且带化身 | `services/gateway/internal/server.go:557-561` |
| 路由投影的记录结构已存在：扁平 key ＋ 节点 ＋ 化身 | `services/scheduler/internal/redis_store.go:17` |
| 🔴 没有「沙箱最大寿命」这个量 | `config/default.toml:215`；`grep max_instance_length` 全仓无匹配 |
| 🔴 auto-resume 在 node 的数据面反代路径上 | `src/api/proxy.rs:878` `:891` |
| 🔴 没有租户模型；鉴权是 presence-only | `src/api/impls/auth.rs`；`grep team_id src/` 仅命中 generated |

**e2b**（`/home/debian/e2b-infra`）

| 事实 | 位置 |
|---|---|
| `SandboxService` 六个 RPC（含节点级 `List`） | `packages/orchestrator/orchestrator.proto:235` |
| 「节点不自主驱逐，API evictor 才做」 | `packages/orchestrator/orchestrator.proto:59` |
| 一个二进制多角色 | `packages/orchestrator/pkg/cfg/service.go:17` |
| 🔴 活跃态状态机只有 running/pausing/killing/snapshotting，**没有 paused** | `packages/api/internal/sandbox/sandboxtypes/states.go:108-113` |
| 活跃态 store 的完整接口（含 `Reconcile(nodeSandboxes, nodeID)`） | `packages/api/internal/sandbox/store.go:73-157` |
| 暂停态从 PG 目录读 | `packages/api/internal/handlers/sandboxes_list.go:33` |
| 🔴 evictor 无 leader election，安全性全靠 store 原子性 | `packages/api/internal/orchestrator/orchestrator.go:192-196`、`evictor/evict.go` |
| 放置在 api 里 | `packages/api/internal/orchestrator/placement/` |
| 发布翻牌在 api 里 | `packages/api/internal/orchestrator/pause_instance.go:71-82` |
| 未翻牌的 build 选不中（谓词在解析查询里） | `packages/db/queries/get_snapshots_with_cursor.sql:26` 等 |
| 目录在 PG，真 keyset 分页 | `packages/db/queries/get_snapshots_with_cursor.sql` |
| 路由态在 Redis；client-proxy 直读 | `packages/shared/pkg/sandbox-catalog/catalog_redis.go`、`packages/client-proxy/main.go:130` |
| 只在 resume 冷路径调 api | `packages/client-proxy/internal/proxy/paused_sandbox_resumer_grpc.go` |
| execution CAS 必须在 Lua 内，不能在 Go 里先查后写 | `packages/api/internal/sandbox/storage/redis/scripts.go:33-40` |
| 孤儿判定只看 sandbox id 存在性（现成缺口，别抄） | `packages/api/internal/sandbox/storage/redis/main.go:205-217` |
| 每次 pause 造全新 BuildID | `packages/api/internal/orchestrator/snapshot_template.go` |
| 节点启动残留回收 | `packages/orchestrator/pkg/startupreclaim/` |
| 节点间供块 | `packages/orchestrator/chunks.proto` |
| 🔴 跨副本的每沙箱分布式锁 | `packages/api/internal/sandbox/storage/redis/lock.go`、`state_change.go:41` `:190` |
| 并发 create 去重：`ErrAlreadyExists` ⇒ `waitForStart` | `packages/api/internal/sandbox/store.go:157` |
| 路由记录的 TTL 是沙箱寿命，不是租约 | `packages/api/internal/orchestrator/lifecycle.go:38` |
| 🔴 路由投影是独立结构，client-proxy 从不引用 api 的 store 包 | `packages/shared/pkg/sandbox-catalog/catalog.go:11-18`；`grep -rn "api/internal/sandbox" packages/client-proxy/` 零命中 |
| 🔴 投影写必须**同步**，且是 store 的插入回调 | `packages/api/internal/sandbox/store.go:43-44`、`orchestrator/lifecycle.go:15` |
| store key 按 team 分片；投影 key 扁平 | `storage/redis/utils.go:62-67`、`catalog_redis.go:111-112` |
| 🔴 过期索引 member 按 execution 作用域 ⇒ 每次 ZREM "structurally safe" | `packages/api/internal/sandbox/storage/redis/utils.go:32-38` |
| 目录缓存三个子包全部 Redis ＋ DB 回落 | `packages/api/internal/cache/{templates,snapshots,sandboxcounts}` |
| 冷路径：catalog miss ⇒ 调 api 唤醒 | `packages/client-proxy/internal/proxy/proxy.go:109` |
| 单条全局 pub/sub 通道，路由键在 payload 内 | `packages/api/internal/sandbox/storage/redis/utils.go:22-25` |
