# 阶段 3 结构半：`--role api|node|all` 的落地规格

> 2026-08-20 · **写给要照着敲键盘的人**。设计文档，不含生产代码。
>
> 上游：[`2026-08-20-service-decomposition.md`](2026-08-20-service-decomposition.md) §3 / §4 / §7 阶段 3 / §8；
> [`2026-08-20-module-responsibilities.md`](2026-08-20-module-responsibilities.md) §4 / §7 / D1 D6 D8 D9；
> 落地环境：[`_sd-recon-env.md`](_sd-recon-env.md) §6 / §9 SD-B2 SD-B3。
> 考古基准：e2b `/home/debian/e2b-infra`（本文引用的每一条都自己核过，核对结果在附录）。
>
> **范围。** 阶段 3 有两半。本文只写**进程/角色分解**这一半：
> `--role` 开关、`ListSandboxes`、`SandboxBackend` 拆两 trait、`proxy_routes` 与句柄分家、
> auto-resume 迁移、node 收窄 REST、启动残留回收、envd seed、部署与回退。
>
> **不写**另一半（Redis `MetadataStore`、分布式锁、transition key、过期 ZSET、`Reserve`）——
> 那份单独设计。本文在接缝处按名引用它，标注为 **[R]**（Redis 半的交付物）。
>
> 🔴 **仓库边界（用户裁决，2026-08-20）**：全部工作只落在 `/home/debian/AgentENV`。
> 拆分方案 §4.2.1 所依赖的**跨仓** pause-publish-durability 交付物按**不可用**处理 ——
> 本文不把它列为前置、不设计成等它，一律按 §4.2.1 自己写下的退路（`origin_node_id` ＋
> `published`，未发布的行 resume 硬钉 origin）设计。落点在 **§6.6**。
>
> **可执行性**：本文自带全部证据行号，不需要回读父提案。

---

## 0. 三十秒版

`--role` 这一刀落在 `src/bin/server.rs`。今天 `grep -rn "role" src/bin/server.rs` **零命中**（SD-B2），
所以这是一张白纸 —— 但它落在一个**不是白纸**的类型系统上，那才是本文的主要内容。

**四个关键决定：**

| # | 问题 | 决定 |
|---|---|---|
| **A** | `Orchestrator` 泛型三参数，两个角色是两个具体类型 | **不把 `ApiImpl` 泛型化，也不 box `S`/`P`（做不到）**。新增一个对象安全的门面 trait `SandboxOrchestration`，`ApiImpl` 持 `Arc<dyn SandboxOrchestration>`，角色只决定往里塞哪个 `Orchestrator<S,F,P>` |
| **B** | `ListSandboxes` 的所有权标记 | **显式不透明标记 `control_plane_config`**，随创建请求从 `api` 下发、node 原样存、`ListSandboxes` 只报有标记的。🔴 但真正会杀错人的消费者不是 `ListSandboxes`，是 `node_reclaim` —— 见 §3.5 |
| **C** | `SandboxBackend` 拆两 trait | **`SandboxBackend` 原样不动**（本机、句柄、node 独占）；新增 `NodeSandboxService`（线上、id ＋ 事实）。api 侧用一个 `RemoteSandboxStub` 把后者适配成前者，从而**保住 `Orchestrator` 的泛型接缝** |
| **E** | auto-resume | 从 `src/api/proxy.rs:878/:891` 整体摘掉，改成 gateway 投影未命中 ⇒ 调 `api` 的**单 RPC** `ResumeSandbox`，照抄 e2b `proxy.proto` 的形状（一个 RPC，token 走 gRPC metadata） |

**🔴 读完代码之后，我认为父提案里有六处必须更正**（完整论证在 §14）：

1. §3.3「`ApiImpl` … 没有任何一个是对 Firecracker、netns 或 ublk 的直接引用。这一层已经可以独立装配」—— **错**。
2. §3.3「`S` → Redis 后端」写得像换类型参数就行 —— `MetadataStore` / `SandboxPersister` **不是对象安全的**，换不了。
3. §8 陷阱 4（构建沙箱会被当孤儿杀掉）—— 在 `ListSandboxes` 这一侧是**假警报**，在 `node_reclaim` 那一侧是**真的**。
4. §5.1 的 bytes-then-commit —— 是**阶段 2 的硬前置**，而阶段 2 的描述没有覆盖它。不劈开，阶段 3 的远程 pause 无处落地。
5. 阶段 3「回退：`--role all`」—— **被低估**。它是三个部署对象的协同回退，且慢的那一步是 3600 秒 grace 的 DaemonSet 串行滚动。
6. 🔴 §4.2.1 / D12「节点是亲和提示，不是所有者」—— **它挂的那条跨仓依赖不可用**（用户裁决，2026-08-20）。
   ⇒ 退路生效：目录行带 `origin_node_id` ＋ `published`，未发布的行 resume **硬钉 origin**。
   `api` 必须同时会 **pin** 和 **prefer** 两档落点，D12 的三条模块级后果各要加一个例外分支。见 **§6.6** 与 §14.8。

外加一条 D7 的收窄（§14.6）和一条部署侧的静默死锁（§7.3）。

> 🔧 **2026-08-21 回填：本文写在阶段 2 开工之前，2a/2b/2c 落地之后有一节新增与七处订正 —— 见 §15。**
> 最要紧的三条：**读作用域是「面」的属性**（§15.1）、**「不存在」这个答案要三态**（§15.2）、
> **先落地后接线的代码在接线前要过一次审计**（§15.3）。
> 就地写错的七处汇总在 §15.5，其中 §7.3 的 preStop 修法与 §4.1／§4.2 的 `StagedSnapshot` 形状
> **按本文原样做会做错**。

> 🔴 **一条被本文按「不可用」处理的上游依赖。** 拆分方案 §4.2.1 的整个论证
> （「折叠之后没有需要接管的东西」）依赖主仓的 pause-publish-durability 消掉 `local_only`，
> 它自己标注为**跨仓**（「不在本 submodule 内」）。本文**不把它列为前置，也不设计成等它** ——
> 全部工作只落在 `/home/debian/AgentENV` 内，并按 §4.2.1 自己写下的退路设计。

---

## 1. 今天的装配全景 —— `src/bin/server.rs` 逐项归属

刀要落在这里，所以先把它切完。下表是 `main()` 从第 80 行到第 219 行构造的每一样东西，
以及拆分后它归谁。**「两者」列的意思是这一项在 `api` 和 `node` 里都要有一份，
不是共享一份。**

| 行 | 构造物 | api | node | 备注 |
|---|---|:--:|:--:|---|
| `:80-81` | `require_runtime_capabilities` / `clear_ambient_capabilities` | ❌ | ✅ | 🔴 api 跑在普通 Deployment 里，没有 `CAP_*`。**这两行必须进 node 分支**，否则 api Pod 起不来 |
| `:83` | `API_ADDR` 监听地址 | ✅ | ✅ | 两者都开 HTTP，但服务的路由不同（§7） |
| `:84-87` | `NodeIdentity::from_config` | ✅ | ✅ | api 的 `node_id` 是副本身份，不是机器身份 |
| `:88-91` | `p2p::transport_from_config` ＋ `OverlaybdP2pRuntime` | ❌ | ✅ | 层/块分发是 node 的事（§4.3「层 / 块 P2P 分发 → node」） |
| `:93` | `setup::ensure_environment` | ❌ | ✅ | 下载 firecracker / kernel / tools drive、生成 overlaybd 配置 |
| `:96-100` | `UblkDeviceManager::init_global_*` | ❌ | ✅ | 起 `uvm-ublk-daemon` |
| `:102-104` | `FirecrackerPool::prime` | ❌ | ✅ | 预热 VMM 进程 |
| `:106-110` | `SnapshotManager::new` | ✅ | ✅ | 🔴 **两者都要，但用途相反**：node 写字节（stage），api 提交目录行（commit）。见 §4.4 |
| `:111-133` | `cluster_cpu_arc` / `applied_cpu_arc` | 半 | ✅ | api 收集并下发交集，node 应用到 booting microVM |
| `:134-136` | `TemplateBuilder` | ✅ | ✅ | 调度在 api（并发上限，阶段 2），执行在 node（`src/template/runner.rs`） |
| `:137` | `ImageResolver` | ✅ | ✅ | api 解析用户 image ref，node 解析成本机 overlaybd |
| `:138` | `FirecrackerSandboxFactory` | ❌ | ✅ | **`--role api` 绝不构造它** —— 它会去摸 `/dev/kvm` 相关配置 |
| `:139` | `Orchestrator::with_file_backed_store_and_factory` | ✅ | ✅ | 🔴 **两个不同的具体类型**。这是 §2 的全部内容 |
| `:140-153` | `ObservabilityService` | ✅ | ✅ | api 侧是**接收端**（阶段 4），node 侧是**发送端**（今天） |
| `:154-167` | `ObservabilityReporter` | ❌ | ✅ | 心跳发送方 |
| `:169-182` | `build_paused_registry` ＋ `PausedSandboxWiring` | ✅ | ❌ | 🔴 **api 独占**。它是「暂停沙箱归谁」的仲裁，正是 §4.3 的「发布翻牌」。阶段 3 之后这整块被 [R] 的 Redis 记录 ＋ PG 目录行取代 |
| `:183-191` | `ApiImpl::new` | ✅ | 半 | node 只需要它的数据面一半（反代 ＋ `/health` ＋ `/nodes`），见 §7 |
| `:202-217` | `release_stale_node_holdings` / `renew_paused_leases` / `reconcile_local_records` / `spawn_paused_record_upkeep` | ✅ | ❌ | 🔴 **全部是决策**。node 角色一个都不跑。node 换成 `src/node_reclaim/`（§8） |
| `:219` | `server::new(api_impl)` | ✅ | ✅ | 路由集合不同 |
| `:286-295` | `set_scheduling_disabled(true)` ＋ drain 传播 | ❌ | ✅ | 节点退出轮换 |
| `:236-272` | 关停顺序（reporter → upkeep → orchestrator → pool → ublk → overlaybd p2p → p2p） | 裁剪 | ✅ | api 的关停只有 orchestrator 一项 |

> 🔧 **2026-08-21：本表引的 `src/bin/server.rs` 行号在阶段 1／2 之后整体下移。**
> 对照表在 §15.5 末尾（`:139` → `:152`、`:169-182` → `:181-195`、`:202-217` → `:215-231`…）。
> 各项的**归属**与**理由**不变，只有行号变了。

**读法**：右两列同为 ✅ 的行，就是「同一个二进制两个角色」这句话的成本 ——
它们不是共享代码，是**两份配置不同的同名构造**。同为 ❌ / ✅ 的行才是真正被拆掉的东西。

---

## 2. A. `--role` 这把刀

### 2.1 CLI 与配置

```rust
// src/bin/server.rs 的 ServerCli 新增
/// Which half of the split this process runs.
#[arg(long, value_enum, default_value_t = ServerRole::All, env = "AENV_ROLE")]
role: ServerRole,

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum ServerRole { Api, Node, All }
```

🔴 **三条硬规则：**

1. **`all` 是默认值，且是今天逐字的行为。** 不是「api ＋ node 的并集」，是**现在这份 `main()`**。
   任何「顺便在 all 里也开一下」的想法都会让回退失去意义（§11）。
2. **`env = "AENV_ROLE"`**，因为部署侧只能改 env / args，而 `deploy/k8s/run.sh:30` 会用
   `config/default.toml` 覆盖 ConfigMap 里的 toml（`_sd-recon-env.md` §6 的 D-1/D-3）——
   把 role 写进 toml 等于把它交给一个每次 apply 都会被重写的文件。**role 不进 toml。**
3. **`--role` 与 `--setup-only` / `--setup-host` 互不冲突但要检查**：`--role api --setup-host`
   是无意义组合（api 不需要 KVM/ublk 主机预置），应当在 `main()` 早期 `bail!`。

### 2.2 🔴 真正的问题不是三个泛型参数，是 trait 的对象安全

父提案 §3.3 把接缝描述成「换 `S`、换 `F`、留 `P`」。读代码之后这个描述**不够用**，
理由有两条，第二条是致命的。

**证据一：`ApiImpl` 今天是编译期钉死在 Firecracker 上的。**

```rust
// src/orchestrator/service.rs:93-97
pub struct Orchestrator<
    S: MetadataStore          = InMemoryMetadataStore,
    F: SandboxBackendFactory  = FirecrackerSandboxFactory,
    P: SandboxPersister       = FileBackedSandboxPersister,
>

// src/api/impls/mod.rs:66
    orchestrator: Arc<Orchestrator>,   // ← 裸名 = 三个默认参数
```

`Arc<Orchestrator>` 展开就是
`Arc<Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory, FileBackedSandboxPersister>>`。
⇒ 父提案 §3.3 末尾那句「`ApiImpl` 八个字段里六个是 `Arc<...>` …… **没有任何一个是对 Firecracker、
netns 或 ublk 的直接引用**。这一层已经可以独立装配」是**错的**：第一个字段就是。
八个字段、六个 `Arc` 这两个数字对（`src/api/impls/mod.rs:65-76`），结论不对。

**证据二（致命的）：`S` 和 `P` 换不成 `Box<dyn ...>`。**

```rust
// src/orchestrator/store/mod.rs:73        ← 泛型方法
async fn update_if_state<F>(&self, id, expected_states, update: F) -> Result<MetadataUpdateResult>
where F: FnOnce(&mut SandboxMetadata) + Send;

// src/orchestrator/store/mod.rs:84        ← 泛型方法
async fn list_with_callback<F>(&self, callback: F) -> Result<()> where F: FnMut(&SandboxMetadata) + Send;

// src/orchestrator/persistence/mod.rs:85  ← 泛型方法
async fn load_all<F>(&self, factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
where F: SandboxBackendFactory;
```

**带泛型方法的 trait 不是对象安全的。** `Box<dyn MetadataStore>` / `Box<dyn SandboxPersister>`
**编译不过**。所以「角色选后端」不能在 `S`/`P` 这一层用 trait object 实现。

对照：`SandboxBackendFactory`（`src/sandbox/backend.rs:313-354`）四个方法全部非泛型、
全部 `&self`、返回 `Box<dyn SandboxBackend>` / `Arc<dyn PausedSandboxState>` ⇒ **它是对象安全的**，
`Box<dyn SandboxBackendFactory>` 今天就能用。`SandboxBackend`（`:215`）也是（它已经以
`Box<dyn SandboxBackend>` 存在）。

⇒ **三个参数里只有 `F` 能被 box。** 这决定了解法必须在**更高一层**。

### 2.3 决策：门面 trait `SandboxOrchestration` ＋ 两个装配函数

**做法。** 新建 `src/orchestrator/facade.rs`：

```rust
/// The orchestrator surface everything outside `src/orchestrator/` uses.
///
/// 🔴 Object-safe on purpose. `Orchestrator` is generic over three parameters
/// and the two roles instantiate different concrete types; `MetadataStore` and
/// `SandboxPersister` have generic methods and therefore cannot be boxed, so
/// the only place the two assemblies can converge is here — above all three.
#[async_trait]
pub trait SandboxOrchestration: Send + Sync + 'static {
    // …29 个方法，逐条对应 service.rs 今天的 pub fn…
}

impl<S, F, P> SandboxOrchestration for Orchestrator<S, F, P>
where S: MetadataStore + 'static, F: SandboxBackendFactory, P: SandboxPersister + 'static
{ /* 全部转发 */ }
```

方法集合就是 `Orchestrator` 今天的 29 个 `pub fn`（`service.rs:364` 起，构造函数除外）：

```
create_sandbox        restore_sandbox      fork_sandbox         get_sandbox
list_sandboxes        list_sandbox_ids     list_sandbox_roster  list_sandboxes_filtered
get_envd_access_token validate_envd_access_token                live_execution_id
proxy_lookup_for      keep_alive_for       delete_sandbox       discard_superseded_sandbox
set_paused_publisher  paused_record_cluster_registration        discard_local_paused_record
shutdown              pause_sandbox        resume_sandbox       capture_snapshot
replace_sandbox_network_policy             patch_sandbox_custom_extension_params
metrics_snapshot      subscribe_sandbox_events                  scheduling_disabled
set_scheduling_disabled                    scheduling_disabled_changed_at_ms
```

**改动面小得可以逐个点名。** 全仓在 `src/orchestrator/` 之外持有具体 `Orchestrator` 类型的
生产代码只有三处：

| 位置 | 改成 |
|---|---|
| `src/api/impls/mod.rs:66` `:80` `:100` | `Arc<dyn SandboxOrchestration>` |
| `src/api/isolation.rs:96` | `&Arc<dyn SandboxOrchestration>` |
| `src/observability/service.rs:25` `:36` | `Arc<dyn SandboxOrchestration>` |

其余全是测试（`src/api/impls/paused_recovery.rs:1232`、`src/api/proxy.rs:1942`、
`tests/integration/orchestrator.rs:75` `:165` `:246`），它们构造具体类型再往上转即可。

**于是 `--role` 变成一句话：**

```rust
let orchestration: Arc<dyn SandboxOrchestration> = match cli.role {
    ServerRole::All | ServerRole::Node =>
        assemble_local(config).await?,   // Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory, FileBackedSandboxPersister>
    ServerRole::Api =>
        assemble_remote(config).await?,  // Orchestrator<RedisMetadataStore, RemoteSandboxBackendFactory, DisabledSandboxPersister>
};
```

### 2.4 代价，逐条

| 代价 | 量 | 判断 |
|---|---|---|
| 一次 vtable 跳转 / 一次 `Box<dyn Future>` 分配（`#[async_trait]`）每次编排调用 | ns 级 | **可忽略**：这 29 个方法里最快的一个也要过一次 Redis 往返或一次 `RwLock`；最慢的要开一台 VM |
| 29 个方法的手写转发，加方法时要写两遍 | ~320 行，其中 ~120 行是机械转发 | **接受**。可以用一个 `forward!` 宏收掉大半 |
| trait 与 `impl` 漂移的风险 | —— | 由「`Orchestrator` 的 `pub fn` 只能通过门面被外部调用」这条约束兜底：把 `service.rs` 的 `pub fn` 降级为 `pub(crate) fn`，外部就只能走 trait，漏一个是编译错误 |

**被否掉的三条替代方案，以及否掉的理由：**

| 方案 | 为什么不 |
|---|---|
| `ApiImpl<S, F, P>` 泛型化 | 病毒式扩散：`src/api/impls/` 五个 `impl apis::*::* for ApiImpl`、`src/api/server.rs:12` 的 `I: AsRef<A> + AsRef<ApiImpl>` 约束、`src/api/proxy.rs` 里几十个 `&ApiImpl` 自由函数、`src/api/isolation.rs` 的中间件、`ObservabilityService` —— 28,520 行 `src/api` 全部要带上三个参数。而且两套实例化会把这一坨代码**单态化两遍**，编译时间与产物同时翻倍。换来的是 0 ns |
| `Box<dyn MetadataStore>` / `Box<dyn SandboxPersister>` | **编译不过**（§2.2）。要让它编译得过，必须把 `update_if_state<F>` 的闭包改成 `Box<dyn FnOnce(&mut SandboxMetadata) + Send>`、`list_with_callback<F>` 同理、`load_all<F>` 改成收 `&dyn SandboxBackendFactory`。这是**语义变更**，且 `update_if_state` 的闭包契约正是 [R] 那一半要重写的东西 ⇒ 两半会在同一个签名上打架 |
| `#[cfg(feature = "node")]` 分两次编译 | §8 陷阱 5 已经点名反对；更硬的理由是**它让 `--role all` 变成另一个构建产物**，回退就不再是一个 flag，而是一次重新构建加一次镜像推送（`_sd-recon-env.md` §5.2：在 204 上构建一次 runtime 镜像约 11 分钟） |

### 2.5 每个角色构造什么、不构造什么

`main()` 拆成三个函数：`assemble_api` / `assemble_node` / `assemble_all`，共享一个
`assemble_common`（logging、config、identity、prometheus）。

```
--role all   ：今天的 main() 逐字。零变化。
               🔴 包括：不起 node gRPC 服务端、不跑 node_reclaim、不装 RoleGate。

--role api   ：
   ✅ ConfigManager / NodeIdentity / prometheus
   ✅ SnapshotManager（commit 侧）、ImageResolver、TemplateBuilder（调度侧）
   ✅ Orchestrator<RedisMetadataStore[R], RemoteSandboxBackendFactory, DisabledSandboxPersister>
   ✅ ApiImpl 全量 + 生成路由全量 + ControlPlaneGate
   ✅ ResumeSandbox gRPC 服务端（§6）
   ✅ ObservabilityService（接收侧；阶段 4 起接管心跳）
   ❌ privileges::require_runtime_capabilities   ← 🔴 漏了 api Pod 起不来
   ❌ p2p transport / OverlaybdP2pRuntime / setup::ensure_environment
   ❌ UblkDeviceManager / FirecrackerPool / FirecrackerSandboxFactory
   ❌ ObservabilityReporter（心跳发送端）
   ❌ node_reclaim
   ❌ set_scheduling_disabled 的 drain 传播（api 没有「节点」这个身份）

--role node  ：
   ✅ 上表 node 列的全部
   ✅ Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory, FileBackedSandboxPersister>
        —— 🔴 作为**节点自己的账**，不是集群权威。见 §14.6
   ✅ node gRPC 服务端（§3）+ admission（容量拒绝）
   ✅ node_reclaim（§8）
   ✅ ApiImpl 的数据面一半 + RoleGate（§7）
   ❌ 用户级 REST（sandboxes / snapshots / templates 三组）
   ❌ paused_registry / PausedSandboxWiring / 四个 upkeep 任务
   ❌ try_auto_resume（§6）
   ❌ TTL 驱逐（api 独占，§4.3）
```

🔴 **`--role node` 仍然构造一个完整的 `Orchestrator`。** 这一条会引起争论，理由写在 §14.6：
它是「node 是纯执行器」与「不重写 3,017 行 `service.rs`」之间唯一站得住的落点。

---

## 3. B. 🔴 缺的那块：`ListSandboxes` 节点级 RPC

### 3.1 传输选型

**今天 Rust 侧没有生产用的 gRPC 服务端。** 全仓 `tonic::transport::Server` 只有一处，
在 `src/orchestrator/paused_registry/central.rs:863` `:1053`，是**测试**里的假控制面。

但工具链已经在了，且 `build.rs:13-20` 已经开着 `build_server(true)`：

```rust
tonic_prost_build::configure()
    .build_server(true)     // ← 已经是 true
    .build_client(true)
    .compile_protos(&["services/api/proto/scheduler.proto"], &["services/api/proto"])
```

⇒ **选 tonic gRPC，不新造传输。** 三条理由：
1. `RemoteSandboxBackendFactory` 要下**命令**并等结果，语义与 `scheduler.proto` 的既有客户端同形；
2. 生成器、include path、`src/proto.rs:10` 的 `tonic::include_proto!` 惯例全都在；
3. 与 e2b 的 `orchestrator.proto` 逐条对得上（`SandboxService` 六个 RPC，
   `packages/orchestrator/orchestrator.proto:235` —— **已核**）。

**proto 落点**：`services/api/proto/node.proto`，package `agentenv.node.v1`。

🔴 **只加进 `build.rs`，不加进 `services/Makefile` 的 `PROTO_SRC`**
（`services/Makefile:4` 是显式文件列表）。node.proto 没有 Go 消费者，
生成一份没人用的 `.pb.go` 只会让 `make -C services build` 多一个要维护的产物。

### 3.2 消息形状

```proto
service NodeSandboxService {
  rpc Create        (SandboxCreateRequest)   returns (SandboxCreateResponse);
  rpc Delete        (SandboxDeleteRequest)   returns (google.protobuf.Empty);
  rpc Pause         (SandboxPauseRequest)    returns (SandboxPauseResponse);
  rpc Checkpoint    (SandboxCheckpointRequest) returns (SandboxCheckpointResponse);
  rpc Fork          (SandboxForkRequest)     returns (SandboxForkResponse);
  rpc UpdateNetwork (SandboxNetworkRequest)  returns (google.protobuf.Empty);
  rpc UpdateParams  (SandboxParamsRequest)   returns (google.protobuf.Empty);
  // 🔴 §3.2 缺的那一条。
  rpc ListSandboxes (google.protobuf.Empty)  returns (SandboxListResponse);
}

message SandboxListResponse { repeated NodeSandbox sandboxes = 1; }

message NodeSandbox {
  string sandbox_id    = 1;
  string execution_id  = 2;   // 🔴 必填，孤儿判定按它判（拆分方案 §6.2 ②）
  string node_id       = 3;
  int64  started_at_ms = 4;
  int64  expires_at_ms = 5;   // 0 = 无
  uint32 vcpu          = 6;
  uint64 ram_mb        = 7;
  string host_interaction_ip = 8;
  uint64 rootfs_virtual_size = 9;
  // 🔴 §3.4 的所有权标记。空 ⇒ 这条本来就不该出现在响应里。
  bytes  control_plane_config = 10;
}
```

对照 e2b 的 `RunningSandbox`（`orchestrator.proto:218-229`，字段：`Config` /
`ClientId` / `StartTime` / `EndTime` / `SandboxId` / `TeamId` / `ExecutionId` / `Vcpu` / `RamMb`）——
逐条对得上，去掉 `TeamId`（§4.4：我们没有租户模型），把它的 `Config`（就是 `APIStoredConfig`）
显式化成 `control_plane_config`。

### 3.3 数据来源 —— 🔴 必须是句柄表，不能是账本

e2b：`packages/orchestrator/pkg/server/sandboxes.go:568`（**已核**）

```go
items := s.sandboxFactory.Sandboxes.Items()
```

我们的同形物是 `src/orchestrator/service.rs:101`：

```rust
sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>,   // SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>  (:41)
```

🔴 **不要用 `list_sandbox_roster`。** 它今天从 store 读（`service.rs:795-803`：
`self.store.list()`），也就是模块文档 D1 说的「节点**声称**有什么」。
`ListSandboxes` 要回答的是「节点**实际**有什么」，而那只有句柄表知道。
把两者混起来，孤儿判定就退化成「拿账本对账本」—— 正是上一轮踩过的坑（outcome §2.1）。

**具体实现**：在 `SandboxOrchestration` 上加一个方法
`list_live_sandboxes() -> Vec<NodeSandbox>`，实现是遍历 `self.sandboxes`，
对每个句柄取 `execution_id()` / `host_interaction_ip()` / `runtime_info()`，
其余字段（started_at / expires_at / resources / 标记）从 store 里同 id 的 metadata 补齐。
**句柄表决定成员集合，store 只提供属性。** 缺 metadata 的句柄仍然要报（它是真实存在的 VM），
`control_plane_config` 为空则被过滤掉。

### 3.4 🔴 所有权标记：显式、不透明、双向原样

**e2b 逐字**（`packages/orchestrator/pkg/server/sandboxes.go:576-579`，**已核**）：

> Build sandboxes are not owned by the API and must never show up here, or the API
> would treat them as orphans and kill them. They are the only sandboxes created
> without an `APIStoredConfig`.

**我们的落法：**

| 项 | 定义 |
|---|---|
| 字段 | `SandboxMetadata::control_plane_config: Option<Vec<u8>>`（`src/orchestrator/store/metadata.rs:31` 起新增，`#[serde(default)]`） |
| 语义 | **不透明**。node 不解析、不校验、不生成。`api` 在 `Create` 请求里下发什么，`ListSandboxes` 就原样回什么 |
| 判定 | `Some(_)` ⇒ 控制面拥有；`None` ⇒ 不拥有，**永不出现在 `ListSandboxes` 响应里** |
| 谁能置位 | 只有 node gRPC 的 `Create` / `Fork`。用户级 REST（`--role all` 下）**永远置 `None`** |
| 内容 | ~~阶段 3 里放 `api` 重建自己那份记录所需的最小集：`{execution_id, snapshot_id, timeout_action, expires_at, auto_resume, secure, user_metadata}` 的紧凑编码。~~ 🔧 **2026-08-21 更正（§15.5 第 6 条）：这七项不够，必须是 `SandboxMetadata` 本体的版本化 serde 编码（去掉 `paused_state`）** —— 它是 Redis 全丢之后重建路径的唯一数据源（[R] §13.3）。**node 不需要知道这里面是什么**，这一性质不变 |

🔴 **`#[serde(default)]` 而不是必填**，与 `execution_id` 相反（`metadata.rs:33-42` 那段注释
论证了 `execution_id` 为什么不能有默认值）。这里可以有默认值，而且**必须**有：
默认值是 `None` ＝ 「不属于控制面」，也就是 **fail-closed**——
一条来路不明的旧记录不会被误认成控制面的沙箱而被孤儿逻辑碰到。
`execution_id` 的默认值方向相反（默认会 fail-open），所以两者的处理不同不是不一致。

顺带（D8 末尾）：这个不透明配置同时解决 `api` 副本重启的重建问题 ——
node 原样回传 `api` 当初下发的东西，`api` 不需要把节点内部状态翻译回自己的模型。

### 3.5 🔴 我对 §8 陷阱 4 的更正：警报响错了地方

**读代码之后：构建沙箱在 `ListSandboxes` 这一侧是假警报。**

```rust
// src/template/runner.rs:166
move || FirecrackerSandbox::new_with_id(config, sandbox_id, execution_id)
// src/template/runner.rs:189
FirecrackerSandbox::from_snapshot(&base_snapshot, &launch_config, execution_id)
```

`run_template_build`（`runner.rs:193-266`）在一个**自己起的线程 ＋ 自己的 current-thread runtime**
里构造、`start()`、`pause_to_dir()`、`stop()` 这个 `FirecrackerSandbox`。
它**从头到尾不经过 `Orchestrator`**，因此**从来没进过 `service.rs:101` 的句柄表**。

e2b 的坑成立，是因为它的构建沙箱和 API 沙箱**共用同一张 `sandboxFactory.Sandboxes`**
（`sandboxes.go:568` 与构建路径同源）。**我们不共用。**
⇒ 只要 §3.3 的规矩守住（数据来源是句柄表），构建沙箱结构上就进不来。

**但标记照做，而且必须做**，理由有三条，且第三条才是真危险：

1. **规矩本身**：「所有权用显式标记表达，不要靠推断」。「node 上只有 api 会调 `Create`」
   本身就是一次推断，而且是会随代码演化而失效的那种。
2. **`--role all`**：回退形态下，本机 REST 创建的沙箱**会**进句柄表，而 `api` 不拥有它们。
   没有标记，回退期间的 `api` 会把它们当孤儿。
3. 🔴 **真正会杀错人的消费者是 `node_reclaim`（§8），不是 `ListSandboxes`。**
   启动残留回收扫的是**宿主机残留** —— firecracker 进程、netns、ublk 设备、临时目录 ——
   而**构建沙箱的残留和用户沙箱的残留在宿主机上长得一模一样**：
   netns 名字是 `{NETNS_PREFIX}{uuid_v7}`（`src/sandbox/network/slot.rs:93`），
   不带沙箱 id、不带来源、不带所有权。
   ⇒ **陷阱 4 的真身在这里**，而父提案把它挂在了 `ListSandboxes` 上。§8 给它一个能站住的安全论证。

---

## 4. C. `SandboxBackend` 拆两个 trait

### 4.1 三个句柄，逐个说清楚远程形态返回什么

`src/sandbox/backend.rs` 里过不了线的三样：

| 句柄 | 定义 | 为什么过不了线 |
|---|---|---|
| `PausedSandboxCapture`（`:155-162`） | `{ state: Arc<dyn PausedSandboxState>, publishable: Option<CapturedSandboxSnapshot> }` | `state` 是 trait object；`publishable` 持有临时目录 |
| `CapturedSandboxSnapshot`（`:143-145`） | `Box<dyn Any + Send>` | 文档逐字（`:138-142`）：「Concrete backends may use it to keep temporary artifact directories alive until publication finishes」 |
| `RuntimeArtifactSet`（`:105-107`） | `Vec<PathBuf>`（overlaybd image.json 路径） | **本机路径**。序列化了也没意义 —— 另一台机器上不存在 |

**远程形态：**

| 本机 | 远程返回 | 说明 |
|---|---|---|
| `PausedSandboxCapture.state` | `bytes paused_state_json` | `PausedSandboxState::encode() -> Value`（`backend.rs:32`）**今天就有**。node 编码，api 存进 [R] 的记录，resume 时原样发回给（可能是另一台）node |
| `PausedSandboxCapture.publishable` | `StagedSnapshot { snapshot_id, staged_manifest_digest, repository_uri }` | 🔴 **node 已经把字节写完了**。api 拿到的是「字节在哪」，不是句柄。见 §4.4 |
| `CapturedSandboxSnapshot` | 同上 `StagedSnapshot` | —— |
| `RuntimeArtifactSet` | **不暴露** | image-liveness 是 node 独有的关切。api 侧的 `RuntimeArtifactSet::empty()`（`backend.rs:111`） |
| `SandboxRuntimeInfo`（`:132-136`） | 只过 `rootfs_virtual_size` | 它的另一个字段就是 `runtime_artifacts` |

> 🔧 **2026-08-21：上表与下表里的 `StagedSnapshot { snapshot_id, staged_manifest_digest, repository_uri }`
> 是本文发明的形状，而阶段 2b 已经交付了一个不长这样的。** 实际的 `StagedSnapshot`
> （`src/snapshot/repository/interfaces.rs:371`）是 `commit: SnapshotCommit` ＋ `staged_at_unix_ms`
> ＋ `origin_node_id` ＋ `execution_id` 的 `Serialize + Deserialize` 纯值。
> 🔴 **proto 要承载这个值本身，不是三字段摘要** —— 详见 §15.5 第 3 条。

### 4.2 两个 trait 面

**Trait 1 —— `SandboxBackend`（`src/sandbox/backend.rs:215`）：原样不动。**

🔴 **不改名。** 它由 `FirecrackerSandbox`（`src/sandbox/firecracker/sandbox.rs:165`）和
`MockBackend`（`src/sandbox/mock.rs`）实现，被 `Box<dyn SandboxBackend>` 装进句柄表。
改名是零信息量的 churn，而它的文档已经写明自己是「Lifecycle interface for a **single** sandbox instance」。
拆分之后它多了一条不变量，写进文档即可：

> 🔴 This trait never crosses a process boundary. Three of its return types hold
> live local state (`PausedSandboxCapture`, `CapturedSandboxSnapshot`,
> `RuntimeArtifactSet`); the remoteable surface is `NodeSandboxService`.

**Trait 2 —— `NodeSandboxService`（新，`src/node_server/service.rs` 定义，`src/node_client/` 消费）：
线上面，只有 id 与事实。**

| `SandboxBackend`（本机） | `NodeSandboxService`（线上） | 差在哪 |
|---|---|---|
| `SandboxBackendFactory::build` + `start()` | `Create(spec) -> CreateAck { execution_id, host_interaction_ip, rootfs_virtual_size }` | 🔴 `start` / `start_nowait` / `wait_for_ready` 三个方法**折成一个 RPC**。「已构造未启动」是一个**进程内**中间态，把它留在线上就得给它一个远程生命周期，而没人需要 |
| `pause(artifact_root)` | `Pause { sandbox_id, execution_id, publish } -> PauseResult { paused_state_json, staged: Option<StagedSnapshot> }` | `artifact_root` 参数消失 —— 目录由 node 自己的 `SandboxPersister` 分配（`persistence/file_backed.rs:388`） |
| `resume()` | **不映射** | api 侧的 resume ＝ `decode_paused_state` ＋ 一次新的 `Create`。`SandboxBackend::resume` 是「同一台机器上原地恢复」，那是 node 内部的事 |
| `snapshot()` | `Checkpoint { sandbox_id, execution_id } -> StagedSnapshot` | —— |
| `fork(&[spec])` | `Fork { specs } -> repeated ForkChildResult { sandbox_id, execution_id, oneof { CreateAck ack, string error } }` | 子句柄留在 node。**保留「一个 spec 一个结果、顺序一致」的契约**（`backend.rs:267-281`） |
| `stop()` | `Delete { sandbox_id, execution_id }` | —— |
| `host_interaction_ip()` | `CreateAck` / `NodeSandbox` 的字段 | 从方法变字段 |
| `runtime_info()` | `NodeSandbox.rootfs_virtual_size` | 丢掉 `runtime_artifacts` |
| `startup_artifacts()` | **不暴露** | 纯本机 |
| `update_network_policy()` | `UpdateNetwork` | 对应 e2b `Update` 的 `egress` |
| `update_custom_extension_params()` | `UpdateParams` | 我们多出来的 |
| `execution_id()` | 每个请求/响应都带 | 从方法变**每条消息的必填字段**——这正是 fencing 的载体 |
| —— | 🔴 `ListSandboxes()` | §3 |

🔴 **错误分类必须过线。** `SandboxCaptureError::{Recoverable, Terminal}`（`backend.rs:49-55`）
在本机是 Rust enum，在线上必须是**结构化的**，不能是 message 字符串：
`Recoverable` ⇒ api 回滚到 `Running`；`Terminal` ⇒ api 必须拆掉沙箱。
判错方向就是丢一个用户工作区或者留一个僵尸 VM。
落法：gRPC `Status` 的 `details` 里放一个 `SandboxCaptureFailure { bool terminal; string reason; }`，
**不要靠 `code`**（`Internal` 两边都会用）。

### 4.3 🔴 §3.3 的「`F` → `RemoteSandboxBackendFactory`」为什么不够，以及怎么补

父提案说换掉 `F` 就把「本机编排器」变成「集群编排器」。**方向对，但少了两块。**

**少的第一块：placement。** `SandboxBackendFactory::build`（`backend.rs:320`）是**同步**的，
返回 `Box<dyn SandboxBackend>`；异步的 `start()` 在 backend 上。
⇒ `RemoteSandboxBackendFactory::build` 只能返回一个**不做 I/O 的惰性存根**，
真正的「选节点 ＋ gRPC Create」发生在 `RemoteSandboxStub::start()` 里。
**这是可行的，而且是这个接缝之所以撑得住的原因**，但它意味着：

🔴 **阶段 3 的 `api` 仍然要问 `scheduler` 要节点** —— `placement/` 在阶段 4（模块文档 §7）。
⇒ **阶段 3 结束时进程表是四个**（gateway / api / scheduler / node），不是三个。
父提案没有明说这个中间态，排期时要按四个进程做容量与告警。

🔴 **而且「问 scheduler 要节点」是两个不同的 RPC，不是一个。**
create 走 `Schedule`（「哪台机器有空」），**resume 走 `LookupNode`**
（「这个暂停沙箱该去哪，以及它到底能不能去别处」）。
理由是暂停沙箱有**钉死**和**偏好**两档落点 —— 完整论证与它对 [R] 记录结构的要求在 **§6.6**。

**少的第二块：`P` 也得换。** `api` 用 `DisabledSandboxPersister` ——
`allocate_artifact_root` 返回 `None`（本机没有产物要落），
`persist_paused` 变成空操作。**暂停沙箱的耐久记录是 [R] 的 Redis 记录 ＋ 阶段 2 的目录行**，
不是 api 本地的文件。这与 §4.2「暂停沙箱离开活跃态、变成目录行」一致。

**存根的三个句柄方法怎么实现：**

```
RemoteSandboxStub::pause(_)   -> PausedSandboxCapture {
        state: Arc::new(RemotePausedState(json)),      // impl PausedSandboxState
        publishable: None,                             // 🔴 见 §4.4
    }
RemoteSandboxStub::snapshot() -> CapturedSandboxSnapshot::new(StagedSnapshotRef { .. })
RemoteSandboxStub::startup_artifacts() -> RuntimeArtifactSet::empty()
```

`RemotePausedState::runtime_artifacts()` 返回 `RuntimeArtifactSet::empty()`
（`backend.rs:37` 要求它，`:111` 提供空值）——**api 上没有本机产物要保活**，这是事实，不是妥协。

### 4.4 🔴 与 §5.1 的接线：`publish` 必须先劈成两半，而那是阶段 2 的活

这是本文找到的**最硬的一条跨阶段依赖**，父提案没有写。

> 🔧 **2026-08-21：✅ 阶段 2b 已交付这一对，本节的结论成立、论证要改引。**
> `SnapshotRepository` **不再是 trait**（2b 拆成 `SnapshotCatalog` ＋ `SnapshotArtifactStore`，
> 它本身成了组合两者的结构体）⇒ 下面引的 `interfaces.rs:138-142` 那份契约**已经不存在**，
> 改引 `composite.rs:128`（`stage`）与 `:178`（`commit_staged`）。
> 🔴 **推论 2（「`--role api` 之后每次 pause 都会在 downcast 上失败」）也不再成立** ——
> downcast 现在在 `SnapshotManager::stage_captured`（`manager.rs:177`），**那是 node 侧**。
> 逐条见 §15.5 第 1／2 条。**下文按历史保留，不改写。**

```rust
// src/snapshot/manager.rs:102-119
pub async fn publish_captured(&self, metadata, captured_snapshot: CapturedSandboxSnapshot) -> ... {
    let manifest = captured_snapshot
        .downcast_ref::<FirecrackerCapturedSnapshot>()          // ① 只认 Firecracker 的具体类型
        .map(|s| s.manifest().clone())
        .ok_or(RepositoryError::Unsupported { .. })?;
    let record = self.repository.publish(metadata, manifest).await?;   // ② 字节 + 目录行，一步
    self.publish_p2p_artifacts(&record, &manifest).await;
    Ok(record)
}
```

而 `manifest` 里全是**本机路径**：`manifest.vm_state.path`、`manifest.rootfs.image_config_path`、
`manifest.memory.image_config_path`（`manager.rs:139-158` 逐个用到）。
`SnapshotRepository::publish` 的契约（`src/snapshot/repository/interfaces.rs:138-142`）
逐字要求实现「reading build artifacts from the provided local artifact description」
**并且**「committing a durable snapshot record」—— **字节和翻牌在同一个调用里**。

⇒ **`api` 进程无论如何拿不到这些文件。** 三条推论：

1. **阶段 2 必须交付 `publish` 的两半**，不能只做「目录读写走 PG」：
   - `stage_artifacts(metadata, manifest) -> StagedSnapshot`：**node 侧**，写字节到 OSS，
     路径带 build-unique 前缀（＝ `execution_id`，拆分方案 §6.2 ①），**不写任何目录行**；
   - `commit_staged(metadata, StagedSnapshot) -> SnapshotRecord`：**api 侧**，一条 PG 事务，
     带 execution 谓词（§6.3「必须留的」第二行）。
2. **不做这一步的失败形态是明确的**：`--role api` 之后每一次 pause/snapshot 都会在
   `manager.rs:106` 的 downcast 上失败，返回 `RepositoryError::Unsupported`。
   **这是响亮的失败**（好事），但它意味着**阶段 3 无法上线**，不是「先上再补」。
3. **P2P 发布留在 node。** `publish_p2p_artifacts`（`manager.rs:124`）读的是本机 overlaybd 层文件
   （`SnapshotP2pArtifact::local_overlaybd_layers`）。它跟着 `stage_artifacts` 走，
   在 node 上做，与「OSS resolver 消费 P2P、POSIX resolver 不消费」的既有分工不冲突。

🔴 **排期动作**：在开工阶段 3 结构半之前，去阶段 2 的交付清单里确认 `stage/commit` 这一对存在。
不存在就先补，因为**它决定的是 proto 的 `StagedSnapshot` 长什么样**，
而 proto 一旦发出去就改不动了。

#### 4.4.1 阶段 2b 交付时留下的两条，阶段 3 必须处理（不要重新发现）

> 🔧 **2026-08-21 复核：下面两条仍然全部开着**，行号已漂移，且**还漏了第三条**。
> 更新后的行号，以及新增的那条（commit 之后的 P2P 广告需要一条从 `api` 回到 node 的「已提交」信号），
> 在 §15.5 第 4／5 条。

`stage / commit_staged` 这一对已经在阶段 2b 交付了（`src/snapshot/repository/composite.rs`）。
QA 在验收 2b 时挖出两条**在 2b 里无害、在 `--role api` 下变成 bug** 的东西，
经判断都属于阶段 3 的活，故留在这里而不是就地修掉。

**① `StagedSnapshot::origin_node_id` 过了线就被丢掉。**

`stage` 把它填成本机 node id（`composite.rs:147`），
值随 `StagedSnapshot` 序列化过 gRPC，`commit_staged` 收到它——然后**不用**。
中心目录的写入自己算一个：`begin_snapshot` / `commit_snapshot` 里
`origin_node_id` 取的是 `self.node_id`
（`backends/central/mod.rs:295-297`、`:361-363`），也就是**发起提交的那台机器**。

2b 里两者是同一台，所以看不出来。`--role api` 之后 stage 在 node、commit 在 api，
写进去的就是 api 进程的 id ——
而 §3.1 把这一列当作硬 pin 的依据，§5.5 明说「origin 由 stage 决定」。
⇒ **拆角色的那一批必须让 `commit_snapshot` 用 staged 值里的 origin，而不是客户端自己的 node id。**
这同时意味着 `CommitSnapshotRequest.origin_node_id` 的语义要在 proto 上写清楚是「谁 stage 的」。

**② `commit_staged` 的文档承诺比它做的事窄。**

它的注释说自己 "cannot consult the artifact store about what it wrote"
（`composite.rs:159-165`），而它的 `Err` 分支就在下面调 `roll_back_publish`
（`:180`），后者走 `self.artifacts.delete_artifacts`（`:250`）。
2b 里这没造成问题——同进程，字节就在手边——但**它正是那句注释想要挡住的形态**：
一个只有 `StagedSnapshot` 的 api 进程，回滚时会去删一批它根本看不见的文件。
⇒ 拆角色时要么把回滚挪回 node（由 node 在收到 commit 失败的回执后做），
要么让 `commit_staged` 明确返回「该回滚了」而不是自己动手。
不要靠改注释了事。

**③ （小）P3 的并发形态在 2b 补齐了，别再当缺口。**
`tests/snapshot_catalog.rs::two_commits_racing_for_one_name_produce_exactly_one_winner`
现在真的把两个 `publish_commit` 同时挂在飞行中，靠服务端的唯一索引决胜负。

---

## 5. D. `proxy_routes` 与 `SandboxHandle` 分家

今天两者同生共死在一个结构里：

```rust
// src/orchestrator/service.rs:101-103
sandboxes:   RwLock<HashMap<SandboxId, SandboxHandle>>,   // SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>  (:41)
proxy_routes: RwLock<ProxyRouteTable>,
next_proxy_route_version: AtomicU64,
```

**拆法（两者都留在 node，但从此不再互为前提）：**

| 项 | 归属 | 不变量 |
|---|---|---|
| `sandboxes`（句柄表） | **node** | 🔴 **永不序列化、永不上 Redis**。它是活 VM 的进程内句柄。`ListSandboxes` 的**唯一**数据源（§3.3）。e2b 同形（`sandboxes.go:568`） |
| `proxy_routes` | **node** | 服务本机反向代理。`ProxyRouteTable`（`src/orchestrator/proxy.rs:34`）已经是独立结构，只是被 `Orchestrator` 持有 |
| api 侧的对应物 | `RemoteSandboxStub` | api 拿到的是句柄的**远程存根**，不是句柄 |

🔴 **必须同批做的一件事：收窄 node 侧的 `proxy_lookup_for`。**

```rust
// src/orchestrator/service.rs:852-882
pub async fn proxy_lookup_for(&self, sandbox_id: &SandboxId) -> Result<ProxyLookupResult> {
    if let Some(route) = self.proxy_routes.read().await.route(sandbox_id).cloned() { return Ok(Ready(..)); }
    let metadata = self.store.get(sandbox_id).await?;      // ← 🔴 拆分后这一半在 node 上没有意义
    Ok(match metadata { None => NotFound, Running => RouteMissing, Paused => Paused{..}, _ => Unavailable(..) })
}
```

拆分后 node 的 `store` 只是它自己的账，**不是集群真相**。一个在别处暂停的沙箱，
在这台 node 上查出来是 `NotFound` —— 而那是错的答案，正确答案「它暂停了，去叫醒它」
只有 `api` 知道。⇒ **node 角色下 `proxy_lookup_for` 只回两态**：

```
Ready(target)   —— 路由表命中
RouteMissing    —— 路由表未命中。node 不再解释原因，把判断交给 gateway 的投影未命中路径（§6）
```

`NotFound` / `Paused` / `Unavailable` 三个分支在 `--role node` 下**不可达**。
这不是删代码 —— `--role all` 还要它们 —— 是让 node 的 lookup 走一条更短的分支。

**不分开的后果**（父提案原话）：「`api` 会为了一张路由表而持有一堆它根本构造不出来的句柄」。
补充一句更具体的：`ProxyRoute` 里带 `execution_id`（`proxy.rs:31`），
而那个字段的注释逐字说明它是「这台节点上活着」的定义
（`:26-30`：「a route exists from the moment a VM is reachable until the moment it stops being」）——
**这是一个只有 node 能回答的问题**，把表挪到 api 会让这句注释变成假话。

**模块边界：**

```
src/node_server/          🆕  gRPC 服务端（NodeSandboxService 的 impl）
  ├─ mod.rs                   tonic Server 装配、监听、优雅关停
  ├─ service.rs               八个 RPC → SandboxOrchestration 调用的翻译层
  ├─ admission.rs         🆕  容量裁决：接不下就快速拒绝（gRPC ResourceExhausted）
  └─ ownership.rs         🆕  control_plane_config 的置位 / 过滤（§3.4）
src/node_client/          🆕  api 侧
  ├─ mod.rs                   连接束（每 node 一条，复用 scheduler 客户端的连接管理惯例）
  ├─ factory.rs               RemoteSandboxBackendFactory : SandboxBackendFactory
  ├─ stub.rs                  RemoteSandboxStub : SandboxBackend（§4.3 的三个句柄方法）
  └─ paused_state.rs          RemotePausedState : PausedSandboxState
```

---

## 6. E. 🔴 auto-resume 从 `node` 迁到 `api`

父提案称这是「全文最大的一处空缺」。它是对的，而且比它写的还要具体一点。

### 6.1 今天的路径，逐行

```rust
// src/api/proxy.rs:869-895（resolve_proxy_request 内）
match api_impl.orchestrator().proxy_lookup_for(&sandbox_id).await {
    Ok(ProxyLookupResult::Ready(target)) => break target,
    ...
    Ok(ProxyLookupResult::Paused { auto_resume: true }) => {        // :878
        if auto_resume_attempted { return Err(AutoResumeFailed); }
        authorize_secure_envd_auto_resume(api_impl, sandbox_id, target_port, &parts.headers).await?;  // :884
        try_auto_resume(api_impl, sandbox_id).await?;               // :891
        auto_resume_attempted = true;
        continue;
    }
```

`try_auto_resume`（`proxy.rs:996-1060`）做的是**三件决策**，不是一件转发：

1. `api_impl.arbitrate_resume(sandbox_id)` —— 跨节点仲裁（`ResumeArbitration::{Proceed, Held, Blocked, NotReady, Unavailable}`）；
2. `orchestrator().resume_sandbox(sandbox_id, NewTimeout::EnsureMinimum(..), claimed)` —— 真的把 VM 开起来；
3. 失败时 `api_impl.abandon_claim(sandbox_id, generation)` —— 归还认领。

外加 `authorize_secure_envd_auto_resume`（`proxy.rs:953-994`）验 envd token：
`api_impl.orchestrator().validate_envd_access_token(sandbox_id, candidate)`（`proxy.rs:977`）。

⇒ **什么都不做的话，`--role node` 之后这四段照跑**：node 依然自主发起 resume，
§6.1「拓扑消灭的是 node 自主发起」的前提当场被架空；
而且 `try_auto_resume(api_impl, ...)` 要求 node 保留一个**能做决定**的 orchestrator
（`arbitrate_resume` 要读 `PausedSandboxCoordinator`，也就是 §1 表里标 api 独占的那块），
与「纯执行器」直接冲突。

### 6.2 e2b 的形状（本文核过）

```
client-proxy: catalog 未命中
  → packages/client-proxy/internal/proxy/proxy.go:109
      logger.L().Info(ctx, "catalog miss, attempting resume via api", ...)
      nodeIP, err := pausedChecker.Resume(ctx, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)
  → paused_sandbox_resumer_grpc.go:74-82：token 走 gRPC **metadata**，不是 proto 字段
  → api: packages/api/internal/handlers/proxy_grpc.go
      :248-255   验 envd access token（不匹配 ⇒ PermissionDenied）
      :257-266   startSandboxInternal(...)  ← 真正的 resume
      :271-276   return SandboxResumeResponse{ OrchestratorIp: nodeIP }
```

接口面（`packages/shared/pkg/grpc/proxy/proxy.proto:22-24`，**已核**）：

```proto
service SandboxService {
  rpc ResumeSandbox(SandboxResumeRequest) returns (SandboxResumeResponse);
}
message SandboxResumeRequest  { string sandbox_id = 1; reserved 2; }
message SandboxResumeResponse { string orchestrator_ip = 1; }
```

🔴 **一个 RPC，两个字段。** 这就是模块文档 P4「控制面对数据面只暴露一个 RPC」的全部实现。

### 6.3 我们的新 API 面

**proto**：`services/api/proto/apiproxy.proto`，package `agentenv.apiproxy.v1`。
🔴 **这一份要同时进 `build.rs`（Rust 服务端）和 `services/Makefile` 的 `PROTO_SRC`（Go 客户端）**，
与 node.proto 相反（§3.1）。

```proto
service SandboxResumeService {
  rpc ResumeSandbox(SandboxResumeRequest) returns (SandboxResumeResponse);
}
message SandboxResumeRequest  { string sandbox_id = 1; }
message SandboxResumeResponse {
  string node_id      = 1;
  string node_address = 2;   // gateway 直接拿去转发，省一次 LookupNode
  string execution_id = 3;   // 🔴 gateway 的 execution fencing 要它（见下）
}
```

**metadata（照抄 e2b，不进 proto 字段）：**

| key | 内容 | 谁验 |
|---|---|---|
| `x-agentenv-target-port` | 目标端口 | api：决定这是不是 envd 流量 |
| `x-access-token` | envd access token | api：`secure` 沙箱必验（§9） |

**谁调**：`services/gateway/internal/resume/`（模块文档 §4.3 的 `internal/resume`）。
调用条件：**投影未命中**。不是「404 之后重试」，是路由解析这一步的显式分支。

**api 侧落点**：`src/api/grpc/resume.rs`，一个 tonic 服务，
内部调用与 REST `POST /sandboxes/{id}/resume` **完全相同**的那条路径 ——
今天 `src/api/impls/mod.rs:29` 的注释已经把这条规矩写下了：

> The data-plane auto-resume takes the same decision the REST resume does; both
> reach it through this one point.

⇒ 迁移之后这句话仍然成立，只是「data-plane」那一侧从**本机函数调用**变成**一次 gRPC 入站**。
`ResumeArbitration`（`src/api/impls/paused_recovery.rs`）**整块跟着搬到 api**，不重写。

**错误映射**（照抄 e2b `proxy_grpc.go` 的四种，`proxy.go:110-126` 是消费端）：

| 情况 | gRPC code | gateway 行为 |
|---|---|---|
| token 不匹配 | `PermissionDenied` | 403，不重试 |
| 沙箱不存在 / 不允许 auto-resume | `NotFound` | 走原本的未命中处理（今天的 404/410） |
| 在途转换未结束 | `FailedPrecondition`（reason `transition_in_progress`） | 503 ＋ `Retry-After` |
| 🔴 **钉在 origin，而 origin 不上报** | `FailedPrecondition`（reason `origin_not_reporting`） | 503。**不重试到别的节点** —— 别处没有这份字节。见 §6.6 |
| 🔴 **钉在 origin，而 origin 不收活** | `FailedPrecondition`（reason `origin_not_accepting_work`） | 503。同上 |
| 集群装不下 | `ResourceExhausted` | 503 |
| 其余 | `Internal` | 502 |

🔴 **三种 `FailedPrecondition` 必须靠结构化 reason 区分，不能只靠 message。**
前一种是「等一会儿会好」，后两种是「等到 origin 回来才会好，或者永远不会」——
gateway 的退避策略与告警面完全不同，而今天 scheduler 侧同类失败**只有计数器没有日志**
（`_sd-recon-env.md` §8 待办 (1)：「scheduler 侧被 fence 的 `begin_pause` 零日志」）。
这一刀正好是补上它的地方。

### 6.4 `node` 保留什么

| 保留 | 摘掉 |
|---|---|
| 本机反向代理（`src/api/proxy.rs` 的转发与 WebSocket 升级） | `try_auto_resume`（`:996-1060`） |
| `proxy_lookup_for` 的**两态**版本（§5） | `authorize_secure_envd_auto_resume`（`:953-994`）—— 它验的 token 现在由 api 在唤醒时验 |
| 🔴 **node_proxy 侧的 execution fencing**（`proxy.rs:380` 的 `fencing_stage = "node_proxy"`） | `resolve_proxy_request` 里的 auto-resume 循环（`:868-895` 的 loop 结构塌成一次查表） |

🔴 **fencing 那一行要单独说。** §6.3 的「阶段 3 之后退役」表里列了「node 侧为『我可能被用户直接调用』
写的那套校验」，理由是「用户级 REST 不存在，路径没了」。
**`node_proxy` 这一段不在那个范围内** —— 它在数据面反代路径上，不是 REST，
而且它是流量到达一个被取代的 VM 之前的**最后一道**。§6.3「必须留的」表里只写了
「路由层拒旧 execution | gateway，一处」。⇒ **补一行：node_proxy 也留着，两处。**
砍掉它等于把 §6.2 ③「分区期间旧化身还在跑、还在本机反代上应答」这条重新打开。

另有一处顺带的净收益：`proxy.rs:398-409` 那段 `execution_that_served` 的注释
（「This path resumes paused sandboxes by itself, so a sandbox with no live incarnation
when the request came in has one by the time it is served」）在迁移之后**不再成立** ——
node 不会再自己把沙箱叫醒，`on_arrival` 与「服务时」不可能再不同。
⇒ 这个函数可以塌成一次读取，同时它的存在本身就是「node 自主发起」的化石证据。

### 6.6 🔴 暂停沙箱的落点：**pin 还是 prefer** —— `api` 必须两种都会

**为什么这一节存在。** 拆分方案 §4.2.1 把「原节点失联怎么办」这个问题**取消提问**，
论证挂在一条硬依赖上：pause 必然发布共享存储 ⇒ `local_only` 消失 ⇒ 原节点从「必需」降为「偏好」。
它自己也逐字标注了这是**跨仓**依赖（「不在本 submodule 内」）。

🔴 **本文按「该交付物不可用」设计。** ⇒ §4.2.1 的干净答案不成立，
退路生效：**目录行带 `origin_node_id` ＋ `published` 两列，resume 对未发布的行硬钉 origin。**
`api` 因此必须同时会两种落点，而不是「任何节点都能恢复任何沙箱」。

**好消息：这套两档逻辑今天已经在，而且已经在生产路径上。**

```go
// services/scheduler/internal/lookup.go
case pausedregistry.StatePaused:                                    // ← 已发布
    ...
    zap.Bool("origin_preferred", placed.ID == entry.OriginNodeID)   // :260  提示，落空即换人
    return deps.answer(placed, SANDBOX_LOCATION_PLACED, entry.OriginNodeID, "", PENDING)

case pausedregistry.StatePublishing, pausedregistry.StateLocalOnly: // ← 未发布  :270
    // 逐字：「No snapshot in shared storage: the only copy is on origin's disk,
    //         so this is that node or nothing.」
    origin, schedulability := deps.placer.schedulableNode(entry.OriginNodeID, now)
    switch schedulability {
    case nodeNotReporting:      return nil, FailedPrecondition("... is not reporting")      // :284-292
    case nodeNotAcceptingWork:  return nil, FailedPrecondition("... is not accepting work") // :294-302
    }
    return deps.answer(origin, SANDBOX_LOCATION_PINNED, entry.OriginNodeID, "", PENDING)    // :311-317
```

⇒ **这不是要新写的东西，是要"搬过来而不是删掉"的东西。**
`SANDBOX_LOCATION_{PLACED,PINNED}` 这个二分，就是「偏好」与「必需」的线上表达。

**对本半的四条具体后果：**

| # | 后果 | 落点 |
|---|---|---|
| 1 | 🔴 **`api` 的 resume 走 `LookupNode` 而不是 `Schedule`** | `Schedule` 只回「哪台机器有空」，不知道 `origin_node_id` / `published`。阶段 3 的 `RemoteSandboxBackendFactory` 在 **create** 路径上用 `Schedule`，在 **resume** 路径上用 `LookupNode` —— **两条不同的放置入口**，§4.3 那句「向 scheduler 要节点」要按这一条拆开读 |
| 2 | 🔴 **[R] 的 Redis 记录必须带 `origin_node_id` ＋ `published`** | 拆分方案 §4.2.1 的结论「Redis 记录**只需要** `execution_id` ＋ `node_id`」**在本文的前提下不成立**。这是给 [R] 那一半的接缝要求：**记录结构一旦建起来改不动**（父提案自己的话），所以这一条要在 [R] 开工前就传达到 |
| 3 | **`Pause` RPC 的响应已经带着那一位，不用加字段** | §4.2 定的 `PauseResult { paused_state_json, staged: Option<StagedSnapshot> }`：`staged == None` **就是**「字节没进共享存储」。它和 `PausedSandboxCapture::local_only()`（`src/sandbox/backend.rs:166-171`，`publishable: None`）是同一件事的两种表示。⇒ **api 从 `Pause` 的返回值直接推出 `published`，不需要 node 再报一次** |
| 4 | **阶段 4 port `placement/` 时，两档一起 port** | 模块文档 D12 第 1 条只写了「接受一个可空的 preferred node，不可用时**静默降级**」。🔴 **那只是 `PLACED` 那一档。** `PINNED` 那一档的正确行为是**不降级、显式失败**（上面的两个 `FailedPrecondition`）。只 port D12 写的那一半，未发布的暂停沙箱会被放到一台没有它字节的机器上，然后在 resume 时以一个说不清的错误失败 |

🔴 **第 4 条是这条修正里最容易漏的。** D12 那三条模块级后果全部是按「§4.2.1 成立」写的：
「落空就放弃，走通用放置」「记录只要 `execution_id` ＋ `node_id`」「提示落空要改写提示」。
**前两条在本文前提下要各加一个例外分支**（未发布 ⇒ 不放弃、不改写、显式失败）；
第三条（`maybeRemapResumeOriginNode` 那个自愈回路）**只适用于已发布的行** ——
把一个未发布行的 origin 改写掉，等于把唯一那份字节的地址擦了。

**兜底不变**：这套逻辑今天在 `scheduler` 里。阶段 3 期间它留在原地，`api` 调 `LookupNode` 消费它；
阶段 4 才把它 port 进 `src/orchestrator/placement/`。⇒ **本半不需要重写它，只需要不假设它会消失。**

### 6.5 🔴 排序约束

```
阶段 3 第 5 条（本节）  ──必须早于──▶  阶段 4（scheduler 下线）
```

理由（父提案阶段 4 逐字）：「`scheduler` 下线意味着 `LookupNode` 消失，
而它今天正是 gateway 的未命中回落」。
⇒ **本节不落地就开阶段 4 的话，每一个暂停沙箱的第一次访问都无人接管。**

反向也有一条：**本节不能早于 gateway 直读投影（阶段 1 ②）**——
「投影未命中」这个分支得先存在，才有地方挂 resume 调用。
模块文档 §7 已经把 `gateway/internal/resume` 从阶段 1 推到阶段 3，本文确认这个排法。

---

## 7. F. `node` 不再暴露用户级 REST

### 7.1 🔴 用 RoleGate 层，不要删路由

**为什么不能删。** 生成的路由是一个**单体函数**：

```rust
// src/api/generated/src/server/mod.rs:24-46+
pub fn new<I, A, E, C>(api_impl: I) -> Router {
    Router::new()
        .route("/health", ...)
        .route("/nodes", ...)
        .route("/sandboxes", ...)
        ...
}
```

它是机器管理的（CLAUDE.md：「Treat generated code in `src/api/generated/` as machine-managed」）。
挑着挂需要改生成模板，那是给 `--role` 这一刀背上一份 codegen 债。

**为什么「不挂就等于 404」也不成立。** `src/api/server.rs:62-66` 的 `assemble`：

```rust
generated.layer(require_control_plane).merge(data_plane)
```

而 `data_plane`（`proxy::router`）**带 fallback**（承载 host 路由的沙箱流量，
`server.rs:99-103` 的测试替身逐字复刻了这个形状）。
⇒ 不挂 `/sandboxes`，请求会掉进**数据面的 fallback**，不是 404。
那会让「node 拒绝用户 REST」这件事以一种最难排查的方式失败。

**做法：`src/api/role_gate.rs`，与 `control_plane_gate.rs` 同形、同位置、同测试风格。**

```rust
// src/api/server.rs::assemble
generated
    .layer(middleware::from_fn_with_state(role, refuse_outside_role))  // 🆕 内层，先跑
    .layer(middleware::from_fn_with_state(gate, require_control_plane))
    .merge(data_plane)
```

🔴 **挂在 `merge` 之前**，和 `require_control_plane` 一样，理由逐字相同
（`server.rs:51-61`：`Router::merge` 保留各自的层）——**数据面不受这一层管辖是装配顺序的事实，
不是这个函数记得去判断路径**。

**返回什么**：`404 Not Found`，空 body。不是 403、不是 405。
理由：404 让 `--role node` 与「这条路由压根没编译进来」**不可区分**，
而 403 会说「存在但你不能用」，招来带凭据的重试。

### 7.2 允许清单（`--role node` 下仍然可达的生成路由）

| 路径 | 方法 | 为什么留 |
|---|---|---|
| `/health` | GET | kubelet 三个探针（daemonset `:199-217`）。与 `control_plane_gate.rs:64` 的豁免同理 |
| `/nodes` | GET | 节点自述 |
| `/nodes/{nodeID}` | GET / POST | 🔴 **preStop 要 POST 它来 drain**（daemonset preStop 脚本），且 §7.3 之后还要 GET 它拿 `sandboxCount` |

**其余全部 404**：`sandboxes` 组 16 条、`snapshots` 组 2 条、`templates` 组 8 条
（`src/api/openapi.yml:1331-2222` 的 tag 分组）。

### 7.3 🔴 同批必须改的一件事：preStop 会静默死锁

> 🔧 **2026-08-21：本节的诊断对，射程写窄了，而它给的修法会做错。**
> ① 卡住的不是 rollout，是 **preStop 钩子本身** —— **任何一次 node Pod 删除都会跑它**
> （set image / rollout restart / delete pod / drain / 节点重启），循环里没有超时也没有逃生口。
> ② 🔴 下面提的 `GET /nodes/{id}` 的 `sandboxCount` **只数 VM 还活着的那几个状态**，
> 暂停的记在另一个字段 `sandboxPausedCount` ⇒ 判据必须是**两者之和**。
> 实测：pause 掉一台之后 `sandboxCount` 读 1、`/v2/sandboxes` 读 2。逐条见 §15.5 第 7 条。

`deploy/k8s/base/agentenv-daemonset.yaml` 的 preStop 脚本，drain 循环逐字：

```sh
count=$(curl -sf -H 'X-API-Key: preStop' \
  -H "x-agentenv-control-plane: ${CONTROL_PLANE_TOKEN}" \
  http://localhost:8000/sandboxes | jq 'length') || count=""
if [ -z "$count" ]; then
  echo "preStop: failed to query sandbox count, retrying..."
  sleep 3
  continue        # ← 🔴 永不退出
fi
```

`--role node` 之后 `GET /sandboxes` 返回 404 ⇒ `curl -sf` 失败 ⇒ `count=""` ⇒ `continue` ⇒
**无限循环，直到 `terminationGracePeriodSeconds: 3600` 到期被 SIGKILL**
（daemonset `:29`）。而 `maxSurge: 0` / `maxUnavailable: 1`（`:21-23`）意味着
**每台 node 依次卡满一小时**。一个 5 节点集群的一次滚动 = 5 小时。

**修法**（同批，不可延后）：把 drain 计数改成读 `GET /nodes/${AENV_NODE_ID}` 的
`sandboxCount` —— 它在 `Node` / `NodeDetail` schema 里（`src/api/openapi.yml:1190` `:1219`
`:1248` `:1277`，两个 schema 各有一个 required 的 `sandboxCount`），
而 `/nodes/{id}` 在 §7.2 的允许清单里。

```sh
count=$(curl -sf -H 'X-Admin-Token: preStop' \
  -H "x-agentenv-control-plane: ${CONTROL_PLANE_TOKEN}" \
  "http://localhost:8000/nodes/${AENV_NODE_ID}" | jq '.sandboxCount') || count=""
```

🔴 **注意 header 换成 `X-Admin-Token`** —— `/nodes/{id}` 声明的是 `AdminApiKeyAuth`
（`openapi.yml:2253`），`X-API-Key` 会拿到 401，而 401 也会被 `-f` 吞成失败，
症状与上面完全一样。这个坑 preStop 脚本自己的注释里已经踩过一次（drain 那一段的 `X-Admin-Token, not X-API-Key`）。

### 7.4 `control_plane_gate.rs` 的 `GET /sandboxes` 豁免

```rust
// src/api/control_plane_gate.rs:76-82
fn is_exempt(method: &Method, path: &str) -> bool {
    if path == "/health" { return true; }
    method == Method::GET && matches!(path, "/sandboxes" | "/v2/sandboxes")
}
```

豁免存在的理由（`:65-71` 逐字）是「gateway 建集群列表时扇出到它，
用的是 gateway 自己的 HTTP 客户端而不是反向代理，所以不经过打凭据的钩子」。

⇒ **阶段 2 之后集群列表是一条 SQL，扇出消失；阶段 3 之后这两条路由在 node 上根本不存在。**
删掉这两条 `matches!`，`is_exempt` 塌成 `path == "/health"`。

🔴 **删除时机**：与 `gateway/internal/{node,cluster,registry}_list` 的删除同批
（模块文档 §7：「删除 `gateway/internal/{node,cluster,registry}_list` | **2 之后**」）。
**先删豁免、后删扇出** ⇒ 扇出全部 403，集群列表变 502。**顺序是：先停扇出，再删豁免。**

---

## 8. G. `src/node_reclaim/` —— 启动残留回收

对标 e2b `packages/orchestrator/pkg/startupreclaim/`（**已核**：`reclaim.go` ＋ `firecracker.go`，282 行）。

### 8.1 e2b 的形状，逐条

```go
// reclaim.go:Run  —— 顺序是硬的，注释逐字：
// "Order matters: firecracker runs first so the VMMs are killed before the
//  network reclaim tears down the slots they used."
reclaimers := []reclaimer{
    {resourceFirecracker, reclaimFirecrackers(ctx, config.ProcDir)},  // 扫 /proc 杀进程组
    {resourceNBD,         nbd.ReclaimLeaked},
    {resourceNetwork,     network.ReclaimLeakedSlots(NetnsDir, ..)},
    {resourceCgroup,      cgroup.ReclaimLeaked(..)},
    {resourceFile,        storage.ReclaimSandboxFiles(TempDir, SandboxCacheDir)},
}
```

两个设计点值得逐字抄：
- **best-effort，从不 fatal**（`reclaimer` 的文档逐字：「reclaim is best-effort and never fatal」）；
- **每类资源两个计数器**（`reclaimed` / `failed`，带 `resource_type` 标签），
  而不是一个布尔。这直接决定了 §12 的探针有没有分辨力。

### 8.2 我们的映射

| e2b | 我们 | 备注 |
|---|---|---|
| `reclaimFirecrackers`（扫 `/proc`） | 扫 `/proc`，匹配 `firecracker` 可执行名 ＋ `{firecracker.work_dir}` 前缀的 cwd | 🔴 **必须同时匹配 work_dir**，否则会杀掉这台机器上别人的 firecracker |
| `nbd.ReclaimLeaked` | ublk 设备 | 见 §8.3 的顺序约束 |
| `network.ReclaimLeakedSlots` | `{netns_dir}` 下的 `{NETNS_PREFIX}*`（`src/sandbox/network/slot.rs:93`） | 一并清 veth / iptables |
| `cgroup.ReclaimLeaked` | 沙箱 cgroup | daemonset postStart 已经动过 `memory.oom.group`，落点相同 |
| `storage.ReclaimSandboxFiles` | `{firecracker.work_dir}` 下的临时目录、`serial_output_base_dir/{sandbox_id}/` | 🔴 **不碰** `persisted_sandbox_store_path` —— 那是暂停沙箱的产物，`Orchestrator::new` 要读它（`service.rs:195`） |

### 8.3 🔴 安全论证 —— 为什么「全杀」不会杀掉构建沙箱

§3.5 指出：宿主机残留里，构建沙箱和用户沙箱**长得一模一样**，
netns 名不带来源（`slot.rs:93` 的 `{NETNS_PREFIX}{uuid_v7}`）。
所以 node_reclaim 的正确性**不能**建立在「能分辨」上，只能建立在**时机**上：

> 🔴 node_reclaim 在**监听器打开之前、`FirecrackerPool::prime` 之前**运行，前提是
> **这台机器上的上一个进程已经彻底没了**。在那个瞬间，机器上不存在任何本进程负责的
> firecracker / netns / ublk 设备 —— 包括构建沙箱和预热池 —— 所以「全部回收」
> 与「精确回收」在结果上等价。

**这个前提今天已经被两件事保证，而且都已经写在树里：**

1. DaemonSet `maxSurge: 0`，注释逐字（`agentenv-daemonset.yaml:11-18`）：
   「A node releases the sandboxes its previous process died holding by node identity,
   at startup, on the premise that the previous process on this machine is gone —
   which is exactly what "old pod fully terminated before the new one starts" guarantees.」
   **同一个前提，第二个消费者。**
2. ublk daemon 客户端 `wait_for_socket_available`（`storage/ublk-daemon/src/client.rs:240-265`）：
   旧 socket 还连得上就等，超时则**启动失败**。也就是「上一个 daemon 还在 ⇒ 我不启动」。

🔴 **两条推论，必须写进代码：**

- **顺序**：node_reclaim 的 ublk 一步要排在
  `UblkDeviceManager::init_global_*`（`server.rs:96`）**之后**，
  因为正是那一步断言了旧 daemon 已经消失。其余几步排在它之前。
  ⇒ 落点是 `server.rs:93`（`ensure_environment`）与 `:102`（`FirecrackerPool::prime`）之间，
  拆成两段。
- **`--role all` 默认关**。`all` 是「今天的行为」（§2.1 规则 1），而且开发机上
  同一台机器跑两个 server 是常态，前提不成立。配置项
  `[orchestrator] startup_reclaim_enabled`，`--role node` 下默认 `true`，
  `--role all` 下默认 `false`。

### 8.4 与 `api` 对账的关系

模块文档 §7：「`src/node_reclaim/` | **3** | node 角色一旦独立就必须有」。
补一句为什么「必须」：`api` 的对账（`ListSandboxes` → `Reconcile` → `KillOrphan`）
**只能看见 node 报出来的东西**，而残留的定义就是「node 自己都不知道它还在」。
⇒ 两者不重叠：node_reclaim 管**没人知道的**，`api` 对账管**双方记账不一致的**。

---

## 9. H. 🔴 envd access-token seed 必须集群统一 —— 阶段 3 前置

### 9.1 今天的形状

```rust
// src/sandbox/access.rs:50-70
pub(crate) fn load_or_create(config: &AppConfig, managed_seed_must_exist: bool) -> Result<Self> {
    if let Some(seed) = config.sandbox.access_token_hash_seed.as_deref() { return Self::new(seed); }
    let managed_seed_path = config.home_path.join("secrets/sandbox-access-token-hash-seed");
    let seed = resolve_seed(&managed_seed_path, managed_seed_must_exist)?;   // 没有就**随机生成一个**
    if config.sandbox.access_token_hash_seed.is_none() && config.cluster.scheduler_endpoint.is_some() {
        warn!(... "using a node-local managed envd access-token seed; configure AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED with the same value on every node ...");
    }
    Self::new(&seed)
}
```

token 是 `HMAC-SHA256(seed, sandbox_id)`（`access.rs:72-77`）。

### 9.2 🔴 失效面比 §8 陷阱 6 说的宽

陷阱 6 说的是「`api` 角色一旦要签发 token，两台节点推不出同一个值」。
读代码之后，问题**不在两台节点之间，在两个 `api` 副本之间**，而且不只 resume：

| 调用点 | 做什么 | 副本不一致的后果 |
|---|---|---|
| `service.rs:425` | create 时铸 token，写进 VM 配置 | —— |
| `service.rs:636` | fork 子沙箱铸 token | 子沙箱的 token 由**执行 fork 的副本**决定 |
| `service.rs:1676` | **resume 时重新推导**，塞给恢复出来的 VM | 🔴 副本 B 恢复副本 A 创建的沙箱 ⇒ VM 里的 token 被换成 B 的，用户手上的旧 token 立刻失效 |
| `service.rs:817-821` `get_envd_access_token` ← `src/api/impls/sandbox.rs:231` `:245` | **`GET /sandboxes/{id}` 每次都重新推导后返回给用户** | 🔴 **同一个沙箱，从 A 查和从 B 查会拿到两个不同的 token。用户拿到哪个取决于负载均衡。** |
| `service.rs:823-825` `validate_envd_access_token` ← `proxy.rs:977` | 验 token | 迁到 api 之后（§6.3），验的那个副本必须和铸的那个推出同一个值 |

⇒ **失效是静默的**：没有异常、没有日志、没有指标，只有用户偶发地拿到一个 envd 401。

**而且今天唯一那条 warn 的触发条件在阶段 4 之后会消失** ——
它挂在 `config.cluster.scheduler_endpoint.is_some()` 上（`access.rs:61-62`），
而阶段 4 的目标就是 scheduler 下线。⇒ 最后一根提示线也断了。

### 9.3 落法

| # | 做什么 | 在哪 |
|---|---|---|
| 1 | 🔴 **`--role api` 下 `[sandbox].access_token_hash_seed` 从「可选」升为「必填」** —— 未配置直接 `bail!`，不再回落到节点本地文件 | `src/sandbox/access.rs::load_or_create` 加一个 `role` 参数；`--role node` / `all` 行为不变 |
| 2 | 报错信息要能直接照做：给出 env 名 `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` 和「每个副本必须相同」这句话 | 同上 |
| 3 | 部署侧：`agentenv-api` Deployment 从**同一个 Secret 的同一个 key** 读，与 DaemonSet 现有的那条完全一样 | `agentenv-daemonset.yaml:39-44` 已经有 `secretKeyRef{name: agentenv-runtime-secrets, key: sandbox-access-token-hash-seed, optional: true}` ⇒ api 侧照抄，但 **`optional: false`** |
| 4 | 🔴 **加一个可观测的自证**：把 `SHA256(seed)[..8]` 作为 `agentenv_access_token_seed_fingerprint{fingerprint="..."}` 这个 gauge 的标签，恒为 1 | 这是 §12 那条探针的全部基础 —— 没有它，「seed 一致」在集群上**不可观测** |

🔴 **第 4 条不能省。** 陷阱 6 说「做错了是静默失效」；
「加一条必填校验」只解决了配置**缺失**，解决不了配置**不同**
（两个副本各配了一个非空但不同的值，校验全过）。指纹 gauge 是唯一能看见的地方。
指纹是 seed 的哈希前 8 字节，不是 seed 本身 —— 不泄露凭据。

**时机**：🔴 **阶段 3 前置**，与父提案一致，但要更早一点 ——
第 3、4 条（部署 Secret ＋ 指纹指标）**可以在阶段 3 开工之前单独上线**，
在今天的 `--role all` 形态下就能验证「两台 node 的指纹相同」，
而那正是 §12 探针要的对照面。

---

## 10. I. 部署

### 10.1 两个部署对象，一个镜像

```
agentenv-api    Deployment   replicas: 2   image: agentenv-runtime   args: ["--role","api"]
agentenv-node   DaemonSet    每节点        image: agentenv-runtime   args: ["--role","node"]
```

抄 e2b（`packages/orchestrator/pkg/cfg/service.go:15-18`，**已核**：
`Orchestrator` 与 `TemplateManager` 两个 `ServiceType` 共用一个二进制，
由 `ORCHESTRATOR_SERVICES` 环境变量选）。

**`agentenv-api` Deployment 的关键差异（相对 DaemonSet）：**

| 项 | 值 | 理由 |
|---|---|---|
| `securityContext.privileged` | **不设** | api 不碰 `/dev/kvm`、不建 netns。🔴 §1 表里那两行 `require_runtime_capabilities` 必须进 node 分支，否则这里起不来 |
| volumes | 只有 `agentenv-config`（ConfigMap） | 🔴 **不挂 `hostPath: /var/lib/aenv`**、不挂 `/dev`。挂了就等于把 node 的本地状态目录暴露给一个不该写它的进程 |
| `terminationGracePeriodSeconds` | 60 | api 没有沙箱要 drain。**不要照抄 3600** |
| `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` | `secretKeyRef`，🔴 **`optional: false`** | §9 |
| `AENV_REDIS_ADDR` | 指向 `agentenv-redis` Service | [R]。🔴 **只在这里，永不进 DaemonSet**（§0 硬约束 2 / D6） |
| `AENV_ROLE` | `api` | 也可以走 `args`；两条都要能用，因为 §11 的回退要能只改一处 |
| ports | `http: 8000`、`grpc: 9095`（ResumeSandbox）、`metrics` | —— |
| `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED` | `false` | api 不发心跳 |
| PodDisruptionBudget | `minAvailable: 1` | 照 `scheduler-pdb.yaml` |

**DaemonSet 的增量改动：**

| 项 | 改成 | 理由 |
|---|---|---|
| `args` | `["--role","node"]` | —— |
| 新增 port | `node-grpc: 9094` | `NodeSandboxService` |
| preStop drain 循环 | 读 `/nodes/{id}` 的 `sandboxCount` | 🔴 §7.3，否则每台卡 3600s |
| `AENV_STARTUP_RECLAIM_ENABLED` | `"true"` | §8.3 |
| 🔴 **不新增任何 Redis 相关 env** | —— | §0 硬约束 2 |

外加：`agentenv-api-service.yaml`（ClusterIP，8000 ＋ 9095），
`agentenv-api-pdb.yaml`，以及 `gateway.json` / `GATEWAY_*` 里的两条新配置
（REST 上游从「扇出到 node」改成「转发到 `agentenv-api:8000`」，
resume 上游指向 `agentenv-api:9095`）。

### 10.2 🔴 怎么引入而不触发 §6 的漂移

`_sd-recon-env.md` §9 SD-B3 逐字：「**拆分要新增 `agentenv-api` 工作负载 ⇒ 迟早绕不开一次 apply。**」
而 `kubectl apply -k` **不 prune**，会「一次 apply 抹掉一半漂移、留下另一半，且大多数不报错」（§6）。

集群上有 11 处漂移，其中会被 apply 静默退回的至少三处：

| 漂移 | apply 后 | 后果 |
|---|---|---|
| **D-11** `execution-fencing-config` 集群是 `enforce`/`true`/`enforce`，仓内 literal 是 `true`/`observe`/`off` | 🔴 静默退回 | 化身闸门整体回到 observe/off |
| **D-1/D-2** 集群 `agentenv.toml` 有 `[backend.oss]` 段（含明文凭据），仓内由 `run.sh:30` 从 `config/default.toml` 重写 | 🔴 静默覆盖 | OSS 后端配置丢失 |
| **D-3** node 的 backend 靠 DS 的 `AENV_PAUSED_REGISTRY_BACKEND=central`，仓内是 `configMapKeyRef` 指向一个**集群里不存在**的 CM（`optional`） | 🔴 静默 | 回落 `local`，中央登记表悄悄关掉 |

SD-B3 给了三条按性价比排的做法。**本文的选择：② 为主，① 为必须，③ 为验收。**

**① 必做（因为它是唯一会响亮失败的那条，做了就有兜底）**
给 `deploy/k8s/run.sh` 加 `IMAGE_REGISTRY` / `IMAGE_TAG` 两个 env，
在渲染时注入 `kustomization.yaml` 的 `images:` 三条（现在写死
`newName: agentenv-gateway` 等，没有 registry 前缀，`kustomization.yaml` 末尾）。
D-7 是 apply 之后会**响亮失败**（ImagePullBackOff）的唯一一条 ——
**先把它变成不会失败的，再动其余的**，否则 apply 之后的第一屏输出会被它淹没。

**② 主线：在主仓 `deploy/agentenv-sg/` 补一个 overlay**，承接今天靠 out-of-band 维持的五项：
`agentenv.toml`（D-1/D-2）、regctl 挂载与 HOME（D-5）、30800 NodePort（D-8）、
PG（D-9）、化身开关（D-11）。
🔴 **overlay 里 `execution-fencing-config` 写终态**（`enforce`/`true`/`enforce`），
不是 base 里的发布起点 —— base 的 literal 是「release 的起点」这件事，
`kustomization.yaml` 自己的注释已经写明了，overlay 正是表达「这个集群已经到终点了」的地方。

**引入 `agentenv-api` 的具体动作序（每一步都可停）：**

```
0. 备份（SD-B3 逐字给的三条）
   kubectl -n agentenv-system get cm  agentenv-k8s-config      -o yaml > /tmp/cm-agentenv.bak.yaml
   kubectl -n agentenv-system get cm  execution-fencing-config -o yaml > /tmp/cm-fencing.bak.yaml
   kubectl -n agentenv-system get svc agentenv-gateway-nodeport -o yaml > /tmp/svc-30800.bak.yaml

1. 先把 ① 与 ② 落地，**不新增任何工作负载**，跑一次 `make k8s-render`，
   与集群现状逐项 diff。🔴 这一步的产出是一份 diff，不是一次 apply。
   验收：diff 为空（或只剩已知的、写进 overlay 的那几项）。

2. 只有 diff 干净了，才 apply。apply 完**立刻**跑 `_sd-recon-env.md` §7.4 的六组复核探针。

3. 再单独 apply `agentenv-api` 的三份新清单（Deployment / Service / PDB）。
   它们是纯新增，不与任何漂移相交。

4. 🔴 **DaemonSet 的 `--role node` 是最后一步，且单独一发**（§11 的分批 3b）。
```

**③ 验收**：把 §7.4 的六组探针做成 `make k8s-drift-check`。
🔴 它必须**在步骤 1 之后、步骤 2 之前就已经能跑**，否则步骤 2 之后你没有基线可比。

---

## 11. J. 回退

### 11.1 🔴 「回退：`--role all`」被低估了

父提案阶段 3 的回退声明是一行：「`--role all`。进程内 store 与 Redis store 两条代码路径在这一批期间**都保留**」。
**代码侧这句话是对的。部署侧它是三个对象的协同变更，而且慢的那一步很慢。**

回退要同时改：

| 对象 | 改什么 | 代价 |
|---|---|---|
| `agentenv-node` DaemonSet | `--role node` → `--role all` | 🔴 **滚动重启**。`maxSurge: 0` ＋ `maxUnavailable: 1`（`:21-23`）⇒ 逐台串行；每台 `terminationGracePeriodSeconds: 3600`（`:29`），preStop 要等沙箱 drain 干净 |
| gateway | REST 上游从 `agentenv-api:8000` 改回扇出到 node；resume 上游拆掉 | ConfigMap ＋ 一次 gateway 滚动，秒级 |
| `agentenv-api` | `replicas: 0` | 秒级 |

而且**顺序是硬的**：node 必须先重新开始服务 REST，gateway 才能指回去。
⇒ 回退的关键路径就是那次 DaemonSet 滚动。
**在事故中，这不是回退，是把故障时间乘以节点数。**

### 11.2 于是：把阶段 3 结构半劈成 3a / 3b

沿用 outcome §5.4 的规矩：**如果一个阶段的回退需要跑新写的回滚逻辑，那它就没有真的可回退。**
这里的问题不是「新写的回滚逻辑」，是「回退本身要花一小时乘以节点数」。

| | 做什么 | 回退动作 | 回退耗时 |
|---|---|---|---|
| **3a**（影子） | DaemonSet 保持 **`--role all`**；上线 `agentenv-api --role api` 副本 2；**gateway 的 REST 上游切到 api**；resume 走 api | **gateway 一个 ConfigMap 切回去 ＋ gateway 滚动** | 秒级，**不碰 DaemonSet** |
| **3b**（收窄） | DaemonSet 切 `--role node`（关掉本机 REST、启用 node_reclaim） | 3a 的动作 ＋ DaemonSet 滚回 `all` | 分钟～小时 |

3a 期间 node 是 `--role all`：它**同时**持有自己的句柄表和自己的 REST，
而 `api` 通过新的 node gRPC 驱动它。两套记账并存 ——
这正是 §3.4 的所有权标记要解决的事：`api` 只认它自己下发过 `control_plane_config` 的那些，
node 本机 REST 建的（3a 期间应当为零）它一个都不碰。

🔴 **3a 的验收判据就是「node 本机 REST 建的沙箱数恒为 0」，而且要按 §12 的规矩把它顶起来一次。**

### 11.3 「保留什么才叫真的可回退」

回退到 `--role all` 要成立，下列东西**在整个阶段 3 期间一行都不能删**：

| 必须留 | 在哪 | 删了会怎样 |
|---|---|---|
| `InMemoryMetadataStore` 生产实现 | `src/orchestrator/store/in_memory.rs`（960 行） | `--role all` 与 `--role node` 都没有 store。🔴 见 §14.6 —— 我认为它根本不该在阶段 3 删 |
| `FileBackedSandboxPersister` | `src/orchestrator/persistence/file_backed.rs` | 同上 |
| 生成路由的**全部** 26 条 | `src/api/generated/` | RoleGate 是**层**，不是删路由，所以这条天然满足 —— 这也是 §7.1 选层的第三个理由 |
| `try_auto_resume` 及其四段决策 | `src/api/proxy.rs:953-1060` | 🔴 **这一条是例外，见下** |
| `paused_registry` / `PausedSandboxWiring` / 四个 upkeep 任务 | `src/orchestrator/paused_registry/`（3,564 行）、`server.rs:169-217` | `--role all` 的跨节点恢复没了 |
| 上一轮阶段 3 的 execution fencing 四件 | `services/scheduler/internal/registry/store_postgres.go:462` `:501`、`gateway/internal/execution_fencing.go` | §6.3 逐字：「它们在本文阶段 3 落地之前是唯一防线」 |

🔴 **`try_auto_resume` 的例外要说清楚。** §6 要把它「摘掉」，而这里说不能删 —— 两句话不矛盾：

- **`--role node` 下不再有人调它**（`resolve_proxy_request` 的那个分支塌掉）；
- **`--role all` 下它照跑**。

⇒ 落法是**把调用点变成角色分支**，不是把函数删掉：

```rust
Ok(ProxyLookupResult::Paused { auto_resume: true }) if role.serves_wake_decisions() => { /* 今天的四段 */ }
Ok(ProxyLookupResult::Paused { .. }) => return Err(proxy_error_response(&SandboxUnavailable(..))),
```

删除动作**统一放到阶段 3 之后的那个 release**（父提案「删除动作一律不与切换动作同批」）。

---

## 12. K. 验证探针 —— 每条自带控制面

> 🔧 **2026-08-21：阶段 2 又顶出五条验证手法上的教训，本节六组探针要逐条继承 —— 见 §15.4，
> 那里还给了一组新增的 P7（休眠拒绝分支的顶起来）。**

沿用 `_sd-recon-env.md` §8 的四条方法论。🔴 尤其是第 2 条：
「**某指标恒 0 本身不是证据** —— 必须先把它顶起来一次，证明 0 是事实而不是探针瞎了。」
以及那次真实翻车：合成行在任何相位都认领不了，`409` 看着像被拒，实则毫无分辨力。

### P1 —— `--role node` 真的拒绝用户 REST

| | 动作 | 期望 |
|---|---|---|
| **正面** | 带合法控制面凭据 `POST /sandboxes` 打 node 的 8000 | **404** |
| **控制面 A（必须仍然工作）** | 同一凭据 `POST /nodes/${NODE_ID}` `{"status":"draining"}` | **200** |
| **控制面 B（必须仍然工作）** | `GET /health` 无凭据 | **204** |
| **控制面 C（排除更弱的实现）** | 沙箱数据面：`GET /proxy/...` 带 `x-agentenv-sandbox-id` 打一个**正在跑**的沙箱 | **200** —— 证明 404 不是「整个 8000 端口被关了」 |

🔴 **控制面 C 是关键的那个。** 只有 A/B 的话，一个把整个生成路由都 404 掉的实现和
一个把整个进程都打挂的实现，探针看不出区别。

### P2 —— `ListSandboxes` 排除构建沙箱

| | 动作 | 期望 |
|---|---|---|
| **准备** | 在目标 node 上**故意跑一次模板构建**（`POST /v3/templates` 触发 `src/template/runner.rs`），并在构建沙箱活着的窗口内取样 | —— |
| **正面** | 同一窗口内 `ListSandboxes` gRPC | 🔴 构建沙箱的 id **不出现** |
| **对照** | 同一窗口内，在同一台 node 上有一个普通沙箱在跑 | 🔴 它**必须出现** |
| **顶起来（§8 第 2 条）** | 临时把 `ownership.rs` 的过滤关掉重跑 | 🔴 构建沙箱**必须出现** —— 否则这条探针只是在观察一件本来就不会发生的事 |

🔴 **「顶起来」这一发不能省**，而且 §3.5 已经预告了它的结果：
今天构建沙箱**根本不进句柄表**，所以关掉过滤它**照样不会出现**。
⇒ 这一发的真实作用是**证明这一点**，而不是证明过滤有效。
如果它真的出现了，说明有人把构建路径接进了 `Orchestrator`，那才是要拦的变更。
把这一发写成一个**长期跑的回归测试**，而不是一次性探针。

**配套的第二发（真正有牙的那个）**：node_reclaim 的构建沙箱豁免。
在一台 node 上起一个构建，然后**在构建进行中重启 node 进程**，
断言 `agentenv_node_reclaim_reclaimed_total{resource_type="firecracker"}` ≥ 1
（构建沙箱**应该**被回收 —— §8.3 论证的是「那个时刻它不该存在」，不是「要放过它」），
且新进程正常起来。对照面：不起构建直接重启，该计数器为 0。

### P3 —— auto-resume 走冷路径，node 不再自主发起

| | 动作 | 期望 |
|---|---|---|
| **正面** | 建一个 `autoResume: true` 的沙箱 → pause → 从 gateway 打一次数据面 | 200，且 `agentenv_api_resume_grpc_total{result="ok"}` +1 |
| **控制面 A（node 侧必须恒 0，且要顶起来）** | 同一次操作期间，node 侧 `agentenv_proxy_auto_resume_total` | 🔴 **0**。**顶起来的做法**：把同一台 node 临时切回 `--role all` 并直接打它的 8000（绕过 gateway），该计数器必须 +1 |
| **控制面 B（排除「gateway 自己吞了」）** | 断言 gateway 的 `resume_attempt_total` 与 api 的 `resume_grpc_total` **逐条相等** | 🔴 `_sd-recon-env.md` §8 第 3 条：两侧互证，对不上就是有一侧算错了（上一轮是 46 = 46） |
| **控制面 C（排除「其实是别的路径把它叫醒的」）** | 把 api 的 `ResumeSandbox` 服务端临时返回 `Unimplemented`，重跑正面 | 🔴 数据面必须**失败**。它成功了，说明还有第二条唤醒路径 |

🔴 控制面 A 的「顶起来」正是 `_sd-recon-env.md` §8 第 2 条那个故事的形状：
「刻意 pause 一台沙箱再打数据面 ⇒ `unfenced_node_silent` 长出 3」。

### P4 —— envd seed 集群统一（§9）

| | 动作 | 期望 |
|---|---|---|
| **正面** | 抓两个 api 副本的 `agentenv_access_token_seed_fingerprint` 标签 | 🔴 **相同** |
| **对照（证明探针有分辨力）** | 起第三个副本，**故意**给它一个不同的 seed | 🔴 指纹**必须不同** —— 否则这个 gauge 只是在报一个常量 |
| **端到端** | 副本 A 建一个 `secure: true` 沙箱拿到 token → pause → 从 gateway 打数据面（由**任一**副本处理唤醒）→ 用**同一个** token | 200 |
| **端到端对照** | 用一个改了一位的 token | 🔴 401 |

### P5 —— 门面重构没有改变行为（A 的回归面）

`SandboxOrchestration` 是纯重构，所以它的探针是**既有测试全绿**：
`make test`、`sudo -E cargo test -p agentenv --test orchestrator_integration orchestrator::`。
🔴 **对照面**：在 blanket impl 里故意把 `pause_sandbox` 转发到 `delete_sandbox`，
断言 `tests/integration/orchestrator.rs` 有测试失败 —— 证明这套测试对这一层的转发**有分辨力**。
（这一发做完立刻改回来，它不是要提交的代码。）

### P6 —— 钉死的暂停沙箱不会被放到别处（§6.6）

🔴 **这一条是本轮范围修正新增的，而且它是最容易在「全绿」里蒙混过去的一条** ——
如果集群里恰好没有未发布的暂停沙箱，pin 逻辑在任何相位都不会被触发，
探针会全绿而毫无分辨力（正是 `_sd-recon-env.md` §8 第 1 条那次翻车的形状）。

| | 动作 | 期望 |
|---|---|---|
| **准备（顶起来）** | 🔴 **刻意造一个未发布的暂停沙箱**：pause 时让共享存储写入失败（临时把 OSS endpoint 指到黑洞），确认登记表里长出一行 `local_only` / `publishing` | 该行存在，`origin_node_id` ＝ 造它的那台 node |
| **正面 A** | origin 正常时，从 gateway 打数据面唤醒它 | 200，且**落在 origin 上**（比对 `NodeSandbox.node_id`） |
| **正面 B** | 把 origin 置 `draining`（`POST /nodes/{id}`），再打一次 | 🔴 `FailedPrecondition` / reason `origin_not_accepting_work`，**且集群里没有第二台节点被尝试过**（断言其余节点的 `Create` 计数器增量为 0） |
| **对照 A（证明不是"一律拒绝"）** | 同一时刻，对一个**已发布**的暂停沙箱做同样的事 | 🔴 **成功，且落在 origin 以外的节点上** —— 证明 `PLACED` 那一档确实降级了 |
| **对照 B（证明不是"一律钉死"）** | 把上面那个未发布沙箱补发布成功后重跑正面 B | 🔴 成功，落在别处 |

**对照 A 是这一组的关键。** 只有正面 A/B 的话，一个「所有暂停沙箱都钉死在 origin」的
实现和一个正确的两档实现看不出区别 —— 而前者会让每一次节点滚动都变成一批不可恢复的沙箱。

---

## 13. 任务顺序与规模

### 13.1 依赖序（本半之内）

```
                        ┌─ T0 envd seed 指纹 + Secret（§9 第 3/4 条）  ← 🔴 可以今天就做，与一切无关
                        │
T1 SandboxOrchestration 门面（§2.3）  ← 唯一的真前置，其余全部挂在它下面
  ├─ T2 node.proto + src/node_server/（§3）              ← 可与 [R] 并行
  │    └─ T3 所有权标记 control_plane_config（§3.4）
  ├─ T4 src/node_client/ + RemoteSandboxStub（§4）
  │    └─ 🔴 依赖 阶段 2 的 stage/commit 劈开（§4.4）—— 本半之外
  ├─ T5 proxy_routes / SandboxHandle 分家 + proxy_lookup 收窄（§5）
  ├─ T6 apiproxy.proto + src/api/grpc/resume.rs + gateway/internal/resume（§6）
  │    └─ 依赖 阶段 1 ② gateway 直读投影 —— 本半之外，已排在前
  ├─ T7 RoleGate（§7.1/7.2）+ preStop 改写（§7.3）
  ├─ T8 src/node_reclaim/（§8）
  └─ T9 --role 三个装配函数（§2.5）                      ← 🔴 收口，最后做
       └─ T10 部署清单 + overlay + 漂移基线（§10）
            ├─ 3a 上线（§11.2）
            └─ 3b 上线
```

### 13.2 哪些可以在 [R]（Redis store 半）就绪之前做

| 任务 | 能否先行 | 理由 |
|---|---|---|
| T0 seed | ✅ **应该先行** | 与 role 完全无关，且它是 P4 探针的基础 |
| T1 门面 | ✅ | 纯重构，`--role all` 下行为不变 |
| T2/T3 node_server ＋ 标记 | ✅ | node 侧的东西，不碰 store |
| T5 分家 | ✅ | 同上 |
| T7 RoleGate | ✅ | 需要 `--role` 这个值存在，但不需要 api 装配 |
| T8 node_reclaim | ✅ | 完全本机 |
| T6 resume 迁移 | 🟡 **半** | gateway ↔ api 的那一半可以先做；api 侧真正把沙箱唤醒需要 [R] 的记录。🔴 **并且要给 [R] 传一条硬要求**：记录必须带 `origin_node_id` ＋ `published`（§6.6 第 2 条）—— 这条越早传越好，记录结构建起来就改不动了 |
| T4 node_client | ❌ | `RemoteSandboxBackendFactory` 只在 `Orchestrator<RedisMetadataStore, ...>` 里有意义 |
| T9 装配 | ❌ | `--role api` 分支要 [R] 的 `RedisMetadataStore` 才能编译 |
| T10 部署 | ❌ | 依赖 T9 |

⇒ **本半有六个任务（T0/T1/T2/T3/T5/T7/T8）可以在 [R] 完全没动之前做完并合入**，
且它们在 `--role all` 下全部是零行为变化。这是两半能并行的实际证据。

### 13.3 规模估算（含测试）

| 模块 | 性质 | LOC |
|---|---|---|
| `src/orchestrator/facade.rs`（trait ＋ blanket impl ＋ `forward!` 宏） | 🆕 | 320 |
| `src/api/impls/mod.rs` / `isolation.rs` / `observability/service.rs` 改 dyn | 改 | +40 / −20 |
| `src/bin/server.rs`：role 解析 ＋ 三个装配函数 | 改 | +260 / −70 |
| `services/api/proto/node.proto` | 🆕 | 220 |
| `src/node_server/`（mod / service / admission / ownership） | 🆕 | 1,400 |
| `src/node_client/`（mod / factory / stub / paused_state） | 🆕 | 1,150 |
| `SnapshotManager::commit_staged` ＋ `StagedSnapshot` 类型 | 改 | 260 |
| `services/api/proto/apiproxy.proto` ＋ `src/api/grpc/resume.rs` | 🆕 | 480 |
| `services/gateway/internal/resume/` | 🆕 | 460 |
| `src/api/proxy.rs`：摘 auto-resume ＋ 收窄 lookup ＋ role 分支 | 改 | +130 / −210 |
| `src/api/role_gate.rs` | 🆕 | 420 |
| `src/node_reclaim/`（mod / firecracker / netns / ublk / files） | 🆕 | 900 |
| `src/sandbox/access.rs`：role 感知 ＋ 指纹 gauge | 改 | 180 |
| `src/orchestrator/store/metadata.rs`：`control_plane_config` | 改 | 60 |
| `deploy/k8s/base/api-{deployment,service,pdb}.yaml` ＋ kustomization ＋ overlay | 🆕 | 330 |
| `deploy/k8s/base/agentenv-daemonset.yaml`：preStop ＋ args ＋ port | 改 | 50 |
| `deploy/k8s/run.sh`：`IMAGE_REGISTRY` / `IMAGE_TAG`（§10.2 ①） | 改 | 40 |
| **合计** | | **≈ 6,700 行**（其中新增 ≈ 5,940，删除 ≈ 300） |

**对照**：本半**不减少**任何行数。父提案阶段 3 承诺的 −13,640 行
（`services/scheduler/internal/registry` 10,076 ＋ `src/orchestrator/paused_registry` 3,564）
在**下一个 release** 的删除批里，而且它们大部分挂在 [R] 那一半上（`paused_sandboxes` 表退役）。
⇒ 🔴 **不要把 −13,640 写进本半的验收判据**，那会导致有人为了对上数字提前删东西，
而 §11.3 刚论证过其中一部分是回退的前提。

---

## 14. 🔴 对抗性结论：我认为父提案写错、或写了但走不通的地方

### 14.1 §3.3 「`ApiImpl` 已经可以独立装配」—— 错

原文：「`src/api/impls/mod.rs:65` 的 `ApiImpl` 八个字段里六个是 `Arc<...>`，另外两个是 HTTP 客户端与
代理域名 —— **没有任何一个是对 Firecracker、netns 或 ublk 的直接引用**。这一层已经可以独立装配。」

字段数对（八个、六个 `Arc`），结论错。第一个字段 `orchestrator: Arc<Orchestrator>`
（`mod.rs:66`）用的是默认类型参数，展开含 `FirecrackerSandboxFactory`（`service.rs:95`）。
**这就是对 Firecracker 的直接、编译期引用。** `ApiImpl` 今天无法与任何别的 factory 一起装配。

**影响**：这句话如果被当成「这一层不用改」，`--role api` 会在第一次 `cargo build` 时撞墙。
本文 §2.3 给的门面 trait 就是为了把这句话**变成真的**。

### 14.2 §3.3 「`S` → Redis 后端」写得像换个类型参数 —— 换不了

`MetadataStore::update_if_state<F>`（`store/mod.rs:73`）、
`MetadataStore::list_with_callback<F>`（`:84`）、
`SandboxPersister::load_all<F>`（`persistence/mod.rs:85`）都是**泛型方法**
⇒ 两个 trait 都**不是对象安全的** ⇒ `Box<dyn MetadataStore>` / `Box<dyn SandboxPersister>` 编译不过。

「换 `S`」在**单态**世界里成立（写两个具体的 `Orchestrator<A,B,C>`），
在「一个 `ApiImpl` 装两种」的世界里不成立。**接缝比提案想的高一层。**

### 14.3 §8 陷阱 4：警报响错了地方

见 §3.5。`src/template/runner.rs:166` `:189` 直接构造 `FirecrackerSandbox`，
从不进 `Orchestrator::sandboxes`（`service.rs:101`）。
e2b 的坑成立是因为它两类沙箱共用一张表（`sandboxes.go:568`），我们不共用。
⇒ `ListSandboxes` 这一侧是假警报；**`node_reclaim` 那一侧是真的**，而提案没把两者连起来。
标记照做（理由是规矩本身 ＋ `--role all` 期间的双记账），但**验收探针要按 §12 P2 的第二发写**。

### 14.4 §5.1 的 bytes-then-commit 是阶段 2 的硬前置，而阶段 2 没写

见 §4.4。`SnapshotManager::publish_captured`（`manager.rs:102-119`）downcast 到
`FirecrackerCapturedSnapshot` 再 `repository.publish(metadata, manifest)`，
而 `SnapshotRepository::publish` 的契约（`interfaces.rs:138-142`）**同时**要求
「reading build artifacts from the provided local artifact description」和
「committing a durable snapshot record」，且 manifest 里全是本机路径。

阶段 2 的描述只有「`SnapshotRepository` 的目录读写走 PG；对象存储只留字节」——
这句话可以在**不劈开这个调用**的前提下满足（同一个进程里先写 OSS 再写 PG）。
⇒ **必须在阶段 2 的交付清单里显式加一条：`publish` 拆成 `stage_artifacts`（node）＋
`commit_staged`（api）两个可分别调用的半。** 不加，阶段 3 的远程 pause 无处落地，
而失败点在 proto 已经定稿之后才会暴露。

### 14.5 阶段 3 的「回退：`--role all`」被低估

见 §11.1。它是三个部署对象的协同回退，关键路径是一次 `maxSurge: 0` ＋
`terminationGracePeriodSeconds: 3600` 的 DaemonSet 串行滚动
（`agentenv-daemonset.yaml:21-23` `:29`）。
⇒ 本文给的 3a/3b 分批（§11.2）把真正的回退落在 gateway 的一个 ConfigMap 上。

### 14.6 D7「把 `InMemoryMetadataStore` 整个拿掉」与 `node` 角色冲突 —— 必须收窄

模块文档 D7 和阶段 3 的删除清单都写了要删掉 `InMemoryMetadataStore` 的生产实现
（`src/orchestrator/store/in_memory.rs`，960 行）。同时模块文档 §4.2 说 node 的
「沙箱句柄表 …… **节点自己的真相**，不是 `MetadataStore`」。

**两者放在一起做不出来。** node 角色要执行 create / pause / resume / fork / snapshot，
而这五条路径的全部逻辑在 `Orchestrator`（`service.rs`，3,017 行）里，
而 `Orchestrator` 的每一步都建在 `MetadataStore` 上（状态机、超时、`execution_id`、
`paused_state`、`image_refs` 引用计数）。
⇒ 「node 只有句柄表、没有 `MetadataStore`」等于**在 node 侧重写 `service.rs`**。
那不是阶段 3 能装下的工作量，也不在任何一份清单里。

**收窄成这样：**

> `InMemoryMetadataStore` **不再是集群权威**，但**保留为 node 角色的本地账本**。
> 集群里唯一的权威活跃态 store 是 [R] 的 Redis 实现，`--role api` 只用它。

D7 的三条依据仍然全部成立 —— 它们论证的是「**权威**状态不要有两个后端」，
而 node 的账本不是权威（`api` 用 `ListSandboxes` 去对它的账，正是因为不信它）。
D7 第三条「双后端会让『两个副本各持一份』这条最危险的路径永远不被生产验证」
在收窄之后仍然满足：`--role api` 只有 Redis 一条路径。

🔴 **诚实登记的代价**：`update_if_state` 之类的语义会有两个实现，
正是 `CLAUDE.md` 里「a change made to the in-memory store and forgotten for Redis is
invisible everywhere else」警告的形状。缓解是**两个实现服务两个不同的调用面**
（node 的账本从不并发决策，api 的 store 是唯一的决策者），
所以「改了一个忘了另一个」不会静默 —— node 侧不需要 CAS 正确性，api 侧的测试跑真 Redis。
**但这条要写进 D7，不能不说。**

### 14.7 §6.3 的退役表漏了一处承重的 fencing

§6.3「必须留的」表里 fencing 落点写了六处，路由层那一处标注为「gateway，一处」。
但 node 的**数据面反代**上还有一处：`src/api/proxy.rs:380` 的 `fencing_stage = "node_proxy"`
（`:431-439` 的注释说明它必须跑在 auto-resume 之前，逐字：
「Resolving first would let traffic addressed to a replaced incarnation wake the sandbox up」）。

auto-resume 迁走之后，那条「必须跑在前面」的理由消失了，但**拒绝本身**没有消失 ——
它是流量到达一个被取代的 VM 之前的最后一道。
⇒ **改成「路由层拒旧 execution | gateway ＋ node_proxy，两处」。**
`node_proxy` 那一处不能随「node 侧为『我可能被用户直接调用』写的那套校验」一起退役 ——
它不在用户 REST 路径上。

### 14.8 🔴 §4.2.1 / D12 的干净答案不可用 —— 退路必须被当成主路设计

拆分方案 §4.2.1 把「原节点失联要不要接管」这个问题**取消提问**，
论证是「折叠之后没有需要接管的东西」。它成立的条件只有一个，而 §4.2.1 自己逐字写下了：

> 🔴 **硬依赖，而且跨仓。** 整个论证挂在「pause 必然发布」上 ——
> 主仓 `docs/proposals/2026-08-19-aenv-pause-publish-durability.md`，**不在本 submodule 内**。
> 它不落地，`local_only` 就还在，origin 就还是「必需」，接管问题原样回来。

**用户裁决（2026-08-20）：该交付物不可用，全部工作只落在 `/home/debian/AgentENV` 内。**
⇒ 括号里那句退路生效，而且不是「万一」，是**当前状态**：

> 目录行带 `origin_node_id` ＋ `published` 两列，resume 时对未发布的行硬钉 origin。

**这条修正的影响面比它看起来大，因为 §4.2.1 的结论被三处引用过：**

| 引用处 | 原文 | 在本文前提下 |
|---|---|---|
| 拆分方案 §4.2.1 末尾 | 「阶段 3 的 Redis 记录**只需要** `execution_id` ＋ `node_id`」 | ❌ 还要 `origin_node_id` ＋ `published`。🔴 **这是给 [R] 那一半的接缝要求**，且父提案自己说「记录结构一旦建起来改不动」 |
| 拆分方案 §7 阶段 3 开头 | 「🔴 外加一条跨仓前置：主仓的 pause-publish-durability …… **这一项的状态要在本阶段开工前确认，不能开工后再问**」 | ✅ 已确认：不可用。⇒ 按「有条件接管」那一支排期，不要留悬置 |
| 模块文档 D12 的三条模块级后果 | ① placement 接受可空 preferred node，落空**静默降级**；② 记录只要两个字段；③ 提示落空**改写提示** | ①③ **各要加一个例外分支**（未发布 ⇒ 不降级、不改写、显式失败）；② 见上 |

**但代价比想象小 —— 因为这套两档逻辑今天已经在生产路径上**
（`services/scheduler/internal/lookup.go:260` 的 `origin_preferred` 与 `:270-317` 的
`SANDBOX_LOCATION_PINNED` 三分支）。⇒ 本半要做的**不是重写，是不假设它会消失**：
`api` 的 resume 走 `LookupNode` 而不是 `Schedule`，阶段 4 port `placement/` 时两档一起 port。
完整落法在 §6.6。

🔴 **顺带一条排期含义**：阶段 3 的删除批里那 13,640 行包含
`services/scheduler/internal/registry`（10,076 行）。**其中承载 pin/prefer 的那部分不能只删不搬** ——
`lookup.go` 的三个分支是这条语义今天唯一的实现。§13.3 已经警告过不要把 −13,640 当验收判据，
这里是它的第二个理由。

### 14.9 阶段 3 结束时进程表是四个，不是三个

§4.1 的目标形态图画了三个进程。阶段 3 之后 `placement/` 还在阶段 4（模块文档 §7），
所以 `api` 的 `RemoteSandboxBackendFactory` 仍要向 `scheduler` 要节点。
⇒ **阶段 3 结束状态：gateway / api / scheduler / node 四个进程。**
不是错误，是一个没写出来的中间态 —— 但它影响容量规划、告警面和 §12 探针的取样点，
应当在排期文档里显式画出来。

---

## 15. 🔧 阶段 2 落地之后回填的事实与订正（2026-08-21）

> 本文写在阶段 2 开工之前。2a（目录 schema ＋ 服务）、2b（trait 拆分、`stage` / `commit_staged`、双写）、
> 2c（`read = postgres`，今天在 dev 集群上跑着）在验收与 QA 里挖出的东西**改变了本文的若干前提**。
> 本节回填，**不重写前面**：每条要么给一条新规矩，要么就地点名一处过时（订正汇总在 §15.5）。

### 15.1 🔴 读作用域是**面**的属性，不是后端的、也不是客户端的

**发生了什么。** 2c 把中心目录 `SnapshotCatalog` 的**整个读面**钉死成
`CatalogReadScope::Resolvable` ⇒ `status_group = 'ready'`
（`src/snapshot/repository/backends/central/mod.rs:942` `:947` `:973`；
SQL 侧 `services/scheduler/internal/catalog/queries_resolved.go:26` 的 `readyPredicate`，
用在 `:116` 与 `:236`）。

**这条规则对快照是对的** —— 它挡住一个字节还在上传的快照被拿去开 VM。
**它被套到别的面上，造成了两次性质不同的失败：**

| # | 失败 | 机理 |
|---|---|---|
| 1 | **模板整体不可见**：`GET /templates/{id}` 404、两个列表里都没有、`POST …/builds/{id}` 连着 404 二十次、按名字解析不出来、delete 找不到行还回一个客气的 204 | 模板行从创建到首次构建提交为止一直是 `waiting`，**永远到不了 `ready`**。修法是**按调用点传作用域**：模板端点要 `AnyStatus`，一切「解析出一个快照去跑」的路径保持 resolvable（`251c839`） |
| 2 | 🔴 **一台暂停沙箱的集群记录被销毁** | 跨节点 resume 读登记表指名的那个快照，**读不到就删掉登记行** —— 而那一行是集群里唯一记着这台沙箱存在的东西。在 resolvable 作用域下，「读不到」把**真的没了**和**行在、只是还没翻成 ready** 混成了同一个答案。一面短暂落后的镜像因此销毁了记录，而它的字节在对象存储里完好无损 |

🔴 **规矩（本半要照着用）：**

> **读作用域是「面」的属性。** 同一个后端、同一个客户端，在不同的面上要答不同的问题：
> 「这个快照能不能拿去开 VM」和「这个快照存不存在」不是一个问题，
> 而把前者的谓词当默认值装在后端上，就等于让每一个问后者的调用点悄悄拿到前者的答案。
> 🔴 **任何在否定答案上执行删除的分支，需要的作用域必须分得清「还没到」和「从来没有」。**

**为什么这条对本半特别重要：阶段 3 给 `api` 装的是一整套新的读面**，
而其中好几处的否定答案后面挂着销毁动作：

| 本文的新读面 | 否定答案意味着什么 | 挂在后面的动作 |
|---|---|---|
| `ListSandboxes`（§3） | 「这台 node 上没有这个沙箱」 | 🔴 `api` 对账 ⇒ `KillOrphan`（§8.4） |
| `control_plane_config == None` 的过滤（§3.4） | 「这台 VM 不属于控制面」 | **不碰它** —— 这一处方向是对的，fail-closed |
| `node_reclaim` 的宿主机扫描（§8） | 「这份残留没人认领」 | 杀进程 / 拆 netns / 删目录 |
| 收窄后的 `proxy_lookup_for`（§5） | `RouteMissing` | 🔴 **只表示「本机路由表没有」**，不表示沙箱不存在 —— §5 已经写对了，这里补一句它为什么重要：把 `RouteMissing` 读成「没这个沙箱」，就是上表第 2 行的形状换了个位置 |

🔴 **⇒ 本半的每一处「读不到 ⇒ 动手」，在写之前先回答一个问题：这个否定答案，
分得清「还没到」和「从来没有」吗？** 分不清就换问法 —— 换法在 §15.2。

### 15.2 🔴 「不存在」这个答案需要三态，不是两态

**2c 的最终修法（`dbd6fa9`）不是把作用域调宽，是让那条销毁分支根本不问「读」。**
理由逐字：**两边都没有行的时候，任何读都分不清「一次还没落地的写」与「一个从来不存在的快照」。**

落成的形状（`SnapshotCatalog::absence_of`，`src/snapshot/repository/interfaces.rs:719`；
语义类型 `SnapshotAbsence` 在 `:499-517`）：

```
absence_of(id) -> Settled                     // 不存在，而且这是定论
              -> Unsettled { because }        // 有东西反驳了它（队列里欠着一笔写，或另一个 store 持有）
              -> Err(..)                      // 🔴 够不着的 store 以错误传播，绝不表现为「不存在」
```

三条要点，**本半新增的每一个远程读都照搬**：

1. **答案来自两个来源，缺一不可**：本节点**欠着的持久写队列**（只有它知道一笔两个 store 都没收下的写）
   ＋ **两个 store 的交叉读**（把对象存储已经持有的东西带给一个从没欠过它的节点）。
2. 🔴 **错误不是不存在。** trait 注释逐字：`An error is never an absence`。
   这就是 §15.1 那条混淆**上升一层**的样子 —— 那边混的是「还没到 / 从来没有」，
   这边混的是「我不知道 / 没有」。
3. **读与销毁问的是两个问题。** 读答「谁现在持有」；销毁问「不持有是不是定论」。

**本半的三个落点（现在就要按这个形状写，不要事后补）：**

| 落点 | 🔴 必须做到 |
|---|---|
| `ListSandboxes`（§3）→ `api` 对账（§8.4） | **一台 gRPC 调不通的 node，不等于这台 node 上没有沙箱。** 一轮对账里只要有一台 node 的 `ListSandboxes` 失败，**整轮放弃**，不许按「拿到的那部分」判孤儿。这与 [R] §9.2 第 3 条（`get_many` 报错 ⇒ 整轮跳过）是**同一条规矩的两端**：一端是 node 侧的事实读不到，另一端是 store 侧的账读不到 |
| `KillOrphan`（本半写它，[R] §10.3 给判定表） | 判定要能同时消费「记录不存在」与「记录还在创建中」（pending）两种否定，**而不是把它们都读成孤儿** |
| `RemoteSandboxStub` 的每一次 gRPC（§4.3） | 超时／不可达 ⇒ **错误**，不是「沙箱没了」。特别是 `Delete` 与 `Pause`：把一次不可达读成「已经没了」，就是在一台还活着的 VM 上撤掉集群对它的记账 |

### 15.3 🔴 惰性代码不是正确的代码 —— 本半有四样东西会先落地、后接线

**2a 的实例。** 2a 提前交付了 `StartBuild` / `RenewBuildLease` / `ReapExpiredBuilds`，
一道 QA 门**正确地**判定它们无害 —— 理由是**没有任何东西在调它们**。
2c 把驱动接上之后，这三样里有**三个真缺陷**，每一个都以别的样子出现：

| 缺陷 | 症状 | 现在的落点 |
|---|---|---|
| 心跳由**节点**盖戳、拿去和**reaper 的时钟**比 | 一台走慢的节点，它跑的每一个构建都在半途被杀，而错误说的是「心跳失效」—— 一件从没发生过的事 | 两端都取数据库的时钟；两个输入类型**根本没有地方放第二个时钟**（`services/scheduler/internal/catalog/store.go:364-378` `:380-392`，`ReapInput` 收的是**时长**不是时刻） |
| **成功不把构建移出队列** | 每个**成功**的构建继续占着一个名额；攒够天花板那么多次成功之后，全集群拒绝一切新构建，而当时**没有任何东西在构建** | `CommitSnapshot` 在翻牌的同一个事务里结束构建 |
| reaper **没有预热** | 一次滚动让所有心跳同时看起来陈旧（不是因为构建停了，是因为没人在听），第一轮把它们全杀了 | 闸门打开后先按住一整个 TTL；闸门再关就重新计时 |

外加 `69c131d`：`start_build` 把同一个 id 同时当 `build_id` 和 `template_id` 发出去，
而 `builds.id` 是主键 ⇒ **一个模板一辈子只能构建一次**（第二次撞上第一条构建行，回一个
「a row with this id already exists」的 400，而那正是「重试一次失败的构建」的样子）。
🔴 **而如果它没有撞上**，两个共用 id 的构建会**互相续对方的租约**。

**⇒ 规矩：**

> 🔴 **一样东西「还没有人调它」，只证明它现在无害，不证明它是对的。**
> 提前落地的代码在**接上驱动之前**要过一次审计，而不是接的时候翻一个开关就算数。

**本半按这条要审的四样（都是本文自己安排的「先落地、后接线」）：**

| # | 东西 | 什么时候才第一次真的跑 | 🔴 接线前要审什么 |
|---|---|---|---|
| 1 | `--role all` 下保留的**两条 store 路径**（§11.3） | Redis 那条在 `--role api` 上线之前**一行都不会执行** | 接线前跑一遍 [R] §11.2 的 L1 契约套**对着两个后端**，而不是只对着 in-memory |
| 2 | 休眠的**读侧闸门** | —— | 🔴 **这一条现在就有活样本**：`guard_read_side` 的三条拒绝分支**除了单元测试之外从未执行过**；`(both, postgres)` 那一支里 `diverged > 0` 这个析取项**至今仍未执行过**。⇒ 本半新增的任何「拒绝启动」分支，验收里必须有一发**把它顶起来** |
| 3 | `ListSandboxes` 的所有权标记 `control_plane_config`（§3.4） | 标记从第一天就写进每条记录，但**只有 `api` 对账时才被消费** | §12 P2 的第二发（node_reclaim 那一发）就是为它写的；另加一发：**故意置空**一台 node 的标记，断言那台上的沙箱被判为「不属于控制面」而**不是被杀掉**（[R] §14 R7 对照面 2 与这一发是同一发） |
| 4 | `RemoteSandboxStub` / `RemoteSandboxBackendFactory`（§4） | 只有 `Orchestrator<RedisMetadataStore, …>` 装配起来才有意义 | 它的每个方法都是「本机语义的远程替身」；接线前逐个对照 §4.2 的映射表核一遍，特别是 §4.2 末尾那条**结构化的 `SandboxCaptureError` 分类**——判错方向就是丢一个工作区或留一台僵尸 VM |

### 15.4 🔧 验证手法：本半探针要继承的五条

`_sd-recon-env.md` §8 的四条仍然全部适用。阶段 2 又加了五条，**每条都是被真事顶出来的**：

**① 跳过的测试报成 `ok`，而且是整批。**
`make -C services test` 曾经**静默跳过 152 个测试**并报绿，
其中包括**唯一两个**抓到 `KEEPTTL` 被改回去的测试。
`-count=1` 现在写在 `services/gateway/Makefile:19` 与 `services/scheduler/Makefile:19`
（顶层 `services/Makefile:29-32` 靠委派继承它，自己那一行上没有写；
`report-skipped-suites` 会在输出末尾把**这一轮跳过了什么**说出来）。
🔴 **本半的对应物**：任何需要外部依赖的新测试面，缺依赖时必须**失败**而不是跳过
（`AENV_*_TEST_REQUIRED=1` 的形状），并且**要有一个 CI job 真的跑它**。

**② 一个 1,100 行的套件可以从来没被 CI 跑过。**
`tests/snapshot_catalog.rs`（发现时 1,161 行，现在 2,673 行）是**唯一**演练双写的地方，
而它**不在任何一个 CI workflow 里**，且没有环境变量时**提前返回并打印 `ok`**。
现在挂在 `.github/workflows/integration-tests.yml:52-66`，带 `AENV_SNAPSHOT_CATALOG_TEST_REQUIRED=1`。
🔴 **本半的对应物**：§12 的六组探针，逐条问一遍「谁会跑它、跑不跑得起来、跑不起来时是红还是绿」。

**③ 🔴 一个恒 0 的计数器，「做过了」和「从来没做」读起来一模一样。**
镜像回填在这套集群上约 **100 ms** 跑完 ⇒ 「切读侧之前先看 `mirror_lag` 归零」这条判据，
在「跑过并结清」与「根本没跑」两种相位下**给出完全相同的读数**（实测撞过两次）。
**定下来的做法：判据钉在「直接总体比对」上，用 `mirror_repaired_total` 佐证，不拿 lag 当主判据。**
🔴 **已在集群上证过它有分辨力**：PG 31 / 对象存储 32 时，四个 gauge 全读 0，
总体判据照样拒绝，并把缺的那个 id 点了名。
⇒ **本半凡是写「某计数器为 0」的判据，都要先回答：这个 0，是「事情做了且结清」还是「事情没发生」？**

**④ 🔴 指标是**每节点**的，而集群级的判据要求和，产品里没有任何东西在求这个和。**
一台节点离场，会把它那份欠账**从总和里带走，不留痕迹**。
⇒ 本半的 §12 P3 控制面 B（gateway 与 api 的计数逐条相等）、
P1 的「其余节点 `Create` 计数增量为 0」、§15.3 表里第 3 行的对账探针，
**全部要显式说清楚：谁在求和，求和的那一刻有几台在报。**

**⑤ 两个落在同一毫秒里的时间戳，让一条测试变得不可能失败。**
本轮抓到一个断言，它要区分的两个时刻由两次紧邻的取时产生，在快机器上落在同一毫秒 ⇒ 恒绿。
🔴 **任何基于时间先后的断言，都要由测试自己制造一个可靠大于时钟粒度的间隔**，
不能指望执行耗时把它们分开。§12 P6 的「pause → resume → 比对落点」正是这种形状，逐条检查。

**对 §12 的具体增补：**

| 探针 | 增补 |
|---|---|
| **P1** | 控制面 C 已经排除「整个端口关了」。🔴 再加一条：`POST /sandboxes` 打 node 的 404，与「路由压根不存在」**必须不可区分** —— 用一条**从未定义过**的路径（如 `/does-not-exist`）做对照，两者响应体应当一致 |
| **P2** | 「顶起来」那一发写成**长期回归测试**这条不变。🔴 补 §15.3 表第 3 行那一发：**故意置空标记 ⇒ 不属于控制面 ⇒ 不出现在列表里，而不是被杀掉** |
| **P3** | 控制面 B 的「两侧逐条相等」🔴 要按 ④ 写清楚求和口径 |
| **P4** | seed 指纹 gauge 也是**每节点**的 ⇒ 判据是「把所有副本的指纹**收齐**并去重后只剩一个」，不是「随便抓两个相同」。🔴 并且要说清楚收齐的那一刻有几个副本 |
| **P5** | 门面重构的对照面（故意把 `pause_sandbox` 转发到 `delete_sandbox`）保留 |
| **P6** | 🔴 按 ⑤ 检查：pin/prefer 的判据里若含「哪次更晚」，要显式制造时间间隔 |
| **🆕 P7** | 🔴 **休眠拒绝分支的顶起来**：本半新增的每一条「拒绝启动 / 拒绝接管」分支（§2.1 规则 3 的 `bail!`、§9.3 第 1 条的 seed 必填、admission 的容量拒绝），各要有一发**制造它拒绝的那个条件**并断言它真的拒绝。理由是 §15.3 第 2 行那个活样本 |

### 15.5 🔧 就地订正：本文里现在写错或过时的七处

| # | 位置 | 现在的事实 |
|---|---|---|
| **1** | §4.4 引的「`SnapshotRepository::publish` 的契约（`interfaces.rs:138-142`）同时要求读本机产物与提交目录行」 | 🔴 **该 trait 已经不存在。** 2b 把它拆成 `SnapshotCatalog`（行）＋ `SnapshotArtifactStore`（字节），`SnapshotRepository` 变成组合两者的**结构体**（`src/snapshot/repository/mod.rs:8`）。§4.4 的**结论**（字节与翻牌必须分开）已经兑现，**论证里引的那段契约要改引** `composite.rs:128`（`stage`）与 `:178`（`commit_staged`） |
| **2** | §4.4 推论 2：「`--role api` 之后每一次 pause/snapshot 都会在 `manager.rs:106` 的 downcast 上失败」 | 🔴 **不再成立，而且方向反了。** downcast 现在在 `SnapshotManager::stage_captured` 里（`manager.rs:177`），**那是 node 侧**。`--role api` 根本不调它。这条推论描述的失败形态已经被 2b 消掉了 |
| **3** | 🔴 §4.1 / §4.2 里发明的 `StagedSnapshot { snapshot_id, staged_manifest_digest, repository_uri }` | **实际交付的 `StagedSnapshot` 不长这样**（`src/snapshot/repository/interfaces.rs:371`）：`commit: SnapshotCommit` ＋ `staged_at_unix_ms` ＋ `origin_node_id` ＋ `execution_id`，是一个 `Serialize + Deserialize` 的**纯值**，自带一条硬约束（`:359-366`）：不许含 `PathBuf` / `Arc` / 临时目录 guard / `FirecrackerSnapshotManifest`。⇒ 🔴 **`node.proto` 里的 `StagedSnapshot` 要承载这个值本身**（编码后的字节，或与它逐字段对应的 message），**不是一份三字段摘要**。§4.4 自己说过「proto 一旦发出去就改不动」，这一条就是它警告的那种改不动 |
| **4** | 🔴 §4.4.1 之外**新增的一条 2b 遗留**（本文没有） | `SnapshotManager::advertise_committed`（`manager.rs:252`）读本机文件 ⇒ 只能在 node 上跑，而它**必须排在 commit 之后**。今天这个顺序只是 `commit_and_advertise`（`manager.rs:257`）里的**语句顺序**；拆开之后需要一条**从 `api` 回到 node 的「已提交」信号**。该函数注释已经逐字把这条登记为「the piece the next phase has to move」。⇒ **本半的 `Pause` / `Checkpoint` RPC 要么带一个提交回执，要么 P2P 广告改成由 node 轮询目录**，二选一，现在就要选 |
| **5** | §4.4.1 ① 与 ② 的行号 | ① `stage` 填 origin：`composite.rs:154`（原写 `:147`）；中心侧仍取 `self.node_id`：`central/mod.rs:389-392`（`commit_snapshot`）与 `:323-326`（`begin_snapshot`）（原写 `:361-363` / `:295-297`）。🔴 **这条缺陷仍然开着**，本半必须修。② `commit_staged` 的注释在 `composite.rs:164-172`，`Err` 分支的 `roll_back_publish` 在 `:188`，它走的 `delete_artifacts` 在 `:330`（原写 `:159-165` / `:180` / `:250`）。🔴 **这条也仍然开着** |
| **6** | 🔴 §3.4 表格「内容」一行：`control_plane_config` 装七个字段的紧凑编码 | **不够。** [R]（`_sd-impl-phase3-redis.md` §2.1 末尾与其附录第一条）从重建路径倒推出：它必须是 **`SandboxMetadata` 本体的版本化 serde 编码**（去掉 `paused_state`）。七个字段重建不出 `resources` / `created_at` / `max_lifetime` / `network_policy` / `custom_extension_params` / `runtime_versions` / `image_configs` / `virtualization_mode` 等十余项。**本半接受这条更正**；「对 node 不透明」这一性质不变 —— 它是 `api` 与**未来的自己**的契约 |
| **7** | 🔴 §7.3 提的 preStop 修法：改读 `GET /nodes/{id}` 的 `sandboxCount` | **这个修法会把一个已知会漏的读法写进钩子里。** `sandboxCount` **只数 VM 还活着的那几个状态**，暂停的记在另一个字段 `sandboxPausedCount`（`src/api/openapi.yml:1232` `:1250`；`NodeDetail` 侧 `:1290` `:1309`；两个 schema 里**都是 required**：`:1203` `:1208` / `:1261` `:1265`。§7.3 原引的 `:1190` `:1219` `:1248` `:1277` 已漂移）。实测：pause 掉一台之后 `sandboxCount` 读 1、`/v2/sandboxes` 读 2。⇒ 🔴 **判据改成两个字段之和**：`.sandboxCount + .sandboxPausedCount`。<br>🔴 **而且 §7.3 把这件事的射程写窄了**：卡住的不是 rollout，是 **preStop 钩子本身**，**任何一次 node Pod 删除都会跑它**（set image / rollout restart / delete pod / drain / 节点重启），而循环里没有超时也没有逃生口。⇒ §7.3 的「否则每台卡 3600s」要改成「**否则每一次删 node Pod 都会卡满 grace**」 |

**外加一条不算错、但会误导排期的**：§1 的装配全景表引的 `src/bin/server.rs` 行号
在阶段 1／2 之后整体下移了。对照（表里的 → 实际）：
`:139` → **`:152`**（`Orchestrator`）、`:140-153` → **`:154-166`**（`ObservabilityService`）、
`:154-167` → **`:167-180`**（`ObservabilityReporter`）、`:169-182` → **`:181-195`**（`build_paused_registry` ＋ `PausedSandboxWiring`）、
`:183-191` → **`:196-213`**（`ApiImpl::new`）、`:202-217` → **`:215-231`**（四个 upkeep）、
`:219` → **`:232`**（`server::new`）。`:80-81` / `:83` / `:84` / `:88-91` / `:93` / `:96` / `:102` / `:110` / `:111-133` / `:134-138` 未变。
🔴 沿用 [R] §16.10 的规矩：**引用时带符号名，不要只带行号** —— 这些文件正在被多个人同时改。
（另：§0 与附录里「`grep -rn "role" src/bin/server.rs` 零命中」**今天仍然成立**，已复核。）

### 15.6 🔧 当前集群基线（阶段 3 从这里开始）

| 项 | 值 |
|---|---|
| 运行中的一批 | `sd2c-*` |
| `snapshot.catalog.write` / `read` | **`both`** / **`postgres`** |
| 目录总体 | 30 条 `p0-seed-*` ＋ 2 条模板 = **N = 32**（两侧一致） |
| `mirror_lag` / `mirror_diverged` | 两台 node 上**都是 0**（🔴 按 §15.4 ③，这不是一致的证据） |
| 三个路由投影开关 | **全 on** |
| F4 sweep 开关 | **off** |
| 镜像 store | **空** |

🔴 **三条从这份基线直接来的部署纪律**（详见 `_sd-recon-env.md` §11.8）：

1. **删任何一个 node Pod 之前，`/v2/sandboxes` 必须读 0** —— 不只是滚动，任何一次删除都算。
2. **要造「中心不可达」，探针必须走 node 直连** —— `scheduler` 副本数归零会把 gateway 一起废掉，
   那一发测到的是 gateway 502，不是节点在中心不可达时的行为。
3. **快照删除现在有操作员路径了**（`DELETE /snapshots/{snapshotID}`，admin 鉴权，走目录，按 `AnyStatus` 读）——
   在此之前它是 405，dev 集群上因此攒了三条孤儿。
   🔴 **它不接任何自动路径、也不问谁依赖这个快照**：一台暂停沙箱的快照就是那台沙箱唯一的耐久副本。

---

## 附：本文核对过的引用

**e2b（`/home/debian/e2b-infra`），任务要求逐条自证的五条：**

| 断言 | 位置 | 核对结果 |
|---|---|---|
| 一个二进制多角色，靠 `ORCHESTRATOR_SERVICES` 选 | `packages/orchestrator/pkg/cfg/service.go:15-18`（const 块）、`:33` `GetServices` | ✅ 提案写 `:17`，落在 const 块内 |
| `SandboxService` 六个 RPC，含节点级 `List` | `packages/orchestrator/orchestrator.proto:235-242` | ✅ 逐字：`Create` / `Update` / `List` / `Delete` / `Pause` / `Checkpoint` |
| `List` 的数据来自进程内句柄表 | `packages/orchestrator/pkg/server/sandboxes.go:568` | ✅ `items := s.sandboxFactory.Sandboxes.Items()` |
| 构建沙箱必须排除，含逐字注释 | 同上 `:576-579`（提案写 `:577-582`） | ✅ 注释逐字一致；标记是 `sbx.APIStoredConfig == nil` |
| `Reconcile(ctx, sandboxes []NodeSandbox, nodeID)` | `packages/api/internal/sandbox/store.go:140` | ✅ 签名逐字一致 |
| 节点启动残留回收 | `packages/orchestrator/pkg/startupreclaim/{reclaim.go,firecracker.go}` | ✅ 五类资源，顺序注释逐字「Order matters: firecracker runs first…」 |
| 冷路径 resume：catalog miss ⇒ 调 api | `packages/client-proxy/internal/proxy/proxy.go:109`、`paused_sandbox_resumer_grpc.go:74-82` | ✅ 日志逐字「catalog miss, attempting resume via api」；token 走 gRPC metadata |
| 控制面对数据面只暴露一个 RPC | `packages/shared/pkg/grpc/proxy/proxy.proto:22-24` | ✅ 只有 `ResumeSandbox`；响应只有 `orchestrator_ip` |
| api 侧在唤醒时验 envd token | `packages/api/internal/handlers/proxy_grpc.go:248-255` | ✅ 不匹配 ⇒ `PermissionDenied` |

**AgentENV 侧，本文新增的证据：**

| 事实 | 位置 |
|---|---|
| 🔴 `grep -rn "role" src/bin/server.rs` 零命中 | 已复核，SD-B2 成立 |
| 🔴 `ApiImpl` 编译期钉死 Firecracker | `src/api/impls/mod.rs:66` ＋ `src/orchestrator/service.rs:93-97` |
| 🔴 `MetadataStore` / `SandboxPersister` 不是对象安全的 | `src/orchestrator/store/mod.rs:73` `:84`、`src/orchestrator/persistence/mod.rs:85` |
| `SandboxBackendFactory` / `SandboxBackend` **是**对象安全的 | `src/sandbox/backend.rs:313-354`、`:215` |
| `Orchestrator` 的 29 个 `pub fn`（门面 trait 的大小） | `src/orchestrator/service.rs:364` … `:2823` |
| 全仓在 `src/orchestrator/` 之外持有具体 `Orchestrator` 的生产代码只有三处 | `src/api/impls/mod.rs:66`、`src/api/isolation.rs:96`、`src/observability/service.rs:25` |
| 🔴 构建沙箱不进句柄表 | `src/template/runner.rs:166` `:189` `:193-266` |
| 🔴 `publish_captured` downcast ＋ 本机路径 | `src/snapshot/manager.rs:102-119` `:139-158`；`src/snapshot/repository/interfaces.rs:138-142` |
| `proxy_lookup_for` 先查路由表再查 store | `src/orchestrator/service.rs:852-882` |
| `ProxyRoute.execution_id` 的注释定义了「这台节点上活着」 | `src/orchestrator/proxy.rs:24-31` |
| auto-resume 的四段决策 | `src/api/proxy.rs:878` `:884` `:891`、`:953-994`、`:996-1060` |
| 🔴 node_proxy 侧的 fencing | `src/api/proxy.rs:380`、`:398-409`、`:431-439` |
| 生成路由是单体函数，且数据面带 fallback | `src/api/generated/src/server/mod.rs:24-46`、`src/api/server.rs:62-66` `:99-103` |
| `GET /sandboxes` 豁免及其理由 | `src/api/control_plane_gate.rs:56-82` |
| 🔴 preStop 用 `GET /sandboxes` 数沙箱，失败即无限重试 | `deploy/k8s/base/agentenv-daemonset.yaml` preStop 脚本 |
| `sandboxCount` 在 Node / NodeDetail schema 里 | `src/api/openapi.yml:1190` `:1219` `:1248` `:1277` |
| 🔴 DaemonSet `maxSurge: 0` 的前提，与 node_reclaim 共用 | `deploy/k8s/base/agentenv-daemonset.yaml:11-18` `:21-23` `:29` |
| ublk 客户端断言旧 daemon 已消失 | `storage/ublk-daemon/src/client.rs:240-265` |
| envd seed 的五个调用点 | `src/sandbox/access.rs:50-70`；`src/orchestrator/service.rs:425` `:636` `:817-825` `:1676`；`src/api/impls/sandbox.rs:231` `:245`；`src/api/proxy.rs:977` |
| seed warn 的触发条件挂在 scheduler_endpoint 上 | `src/sandbox/access.rs:61-62` |
| DaemonSet 已有 seed 的 secretKeyRef（`optional: true`） | `deploy/k8s/base/agentenv-daemonset.yaml:39-44` |
| netns 名不带来源 | `src/sandbox/network/slot.rs:93` |
| tonic 服务端生成已开启 | `build.rs:13-20`；`src/proto.rs:10` |
| Go 侧 proto 是显式文件列表 | `services/Makefile:4` |
| `shouldRecordAssignment` 不匹配 RESUME / DELETE | `services/gateway/internal/server.go:655-670` |
| 🔴 **pin / prefer 两档落点今天已经在生产路径上** | `services/scheduler/internal/lookup.go:260`（`origin_preferred`）、`:270-317`（`PUBLISHING`/`LOCAL_ONLY` ⇒ `SANDBOX_LOCATION_PINNED`，两个 `FailedPrecondition` 分支） |
| `local_only` ＝ `publishable: None`，同一件事的两种表示 | `src/sandbox/backend.rs:155-171`；`src/api/impls/paused_coordinator.rs:361` 的 `mark_local_only` |
| 未发布行的两个既有告警指标 | `services/scheduler/internal/metrics.go:122` `:147`（逐字：「nobody but the origin node can ever resume them」） |
| 🔴 Redis 清单已在树里并已进 `resources:`（本会话另一位在做，未提交）⇒ SD-B1 的部署对象即将就位，但**它本身就是一次结构性 apply**，与 §10.2 的动作序共用同一次窗口 | `deploy/k8s/base/redis.yaml`、`deploy/k8s/base/kustomization.yaml:11` |
