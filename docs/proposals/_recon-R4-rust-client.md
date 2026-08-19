# R4：Rust 侧接入 gRPC 登记表后端的全部接入面

> 配套 [`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md)（13 个方法的**语义**规格）。
> 本文只写**接线与类型**：后端在哪装配、已有的 gRPC 客户端长什么样、
> 哪些类型必须逐字对齐、错误怎么落到调用方、测试怎么跑、怎么切怎么退。
> 语义部分一律引用 R2，不重复。
>
> 代码基线：分支 `central-control-plane-phase2`，`c35f5ec`。

---

## 0. 一页速览

| 问题 | 答案 |
|---|---|
| Rust 侧有 tonic 吗 | **有**，`tonic 0.14.2` + `tonic-prost-build`（`Cargo.toml:74-77,128`） |
| 消费的是哪份 proto | **就是 `services/api/proto/scheduler.proto` 本体**，不是副本（`build.rs:10-15`） |
| codegen 链路要新建吗 | **不要**。加 RPC = 往 proto 里加 message + service method，`cargo build` 自动重生成 |
| 客户端怎么连 | `Endpoint::from_shared(...).connect_lazy()`，无 TLS、无重试层、无 keepalive（`reporter.rs:246-251`） |
| 端点配置 | `[cluster].scheduler_endpoint`，env `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`（`cfg.rs:647-654`），DaemonSet 已注入（`deploy/k8s/base/agentenv-daemonset.yaml:63-64`） |
| registry 在哪造 | `src/bin/server.rs:148` 一处，`main` 里，async，`?` 直接决定进程能否启动 |
| `Arc<dyn PausedSandboxRegistry>` 被谁持有 | **只有 `PausedSandboxCoordinator` 一个**（`paused_coordinator.rs:87`），其余全经 `coordinator.registry()` |
| metadata 要过网吗 | **只过两条 RPC**：`begin_pause`（写）与 `claim_for_resume`（读，单行）。`get_many` 的消费方**完全不看 metadata** |
| 后端切换要重启吗 | **要**。配置是 `OnceLock`，无热加载（`cfg.rs:1237-1242`） |

### 🔴 我认为阶段 2 最大的三个技术风险

见 [§7](#7-三个最大技术风险)。一句话版本：

1. **`arbitrate_resume` 的 fail-open 是四条护栏的盲区**，且它的失败方向与 `get_many` 相反 —— scheduler 每次滚动升级都会开一个"双活防线消失"的窗口。
2. **启动语义会从 fail-closed 翻成 fail-open**：今天 PG 不可达 ⇒ node 起不来；照抄 `connect_lazy` ⇒ node 起得来但 `release_stale_node_holdings` 静默失败，且**永不重试**。
3. **metadata JSONB 的往返 + schema owner 交接**：Go 侧今天完全不碰 metadata 列，阶段 2 第一次要碰；且只要还有一个 node 跑 `postgres` 后端，它每次启动都会**无条件重新断言旧的 CHECK 约束**。

---

## 1. A：后端装配

### 1.1 `PausedRegistryBackendKind` 完整定义（`src/cfg.rs:381-391`）

```rust
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PausedRegistryBackendKind {
    Local,      // 节点本地，DisabledPausedSandboxRegistry
    Postgres,   // 集群级，PostgresPausedSandboxRegistry
}
```

加 `Central` 只要加一个变体 —— `#[serde(rename_all = "snake_case")]` 让 TOML 里写 `backend = "central"` 即可，
**不需要动 confique 的任何东西**。

### 1.2 `[orchestrator.paused_registry]` 全部配置项（`src/cfg.rs:393-435`）

| 字段 | 类型 | 默认 | env 覆盖 |
|---|---|---|---|
| `backend` | `PausedRegistryBackendKind` | `"local"` | **无** |
| `dsn` | `Option<String>` | 无 | `AENV_PAUSED_REGISTRY_DSN`（`parse_trimmed_string`） |
| `max_connections` | `u32` | `8` | **无** |
| `reconcile_interval_secs` | `u64` | `30` | **无** |
| `lease_ttl_secs` | `u64` | `90` | **无** |

🔴 **只有 `dsn` 有 env 覆盖**。confique 的 `#[config(env = ...)]` 是逐字段 opt-in 的，
其它四个字段**只能改 TOML**。这直接决定了 §6.2 的回退代价。

两个强制夹紧（`cfg.rs:437-459`，R2 §4.2 已展开语义）：
`reconcile_interval() = max(secs, 1)`、`lease_ttl_secs() = max(ttl, interval*3)`。
**注意**：传给 registry 的是 `config.lease_ttl_secs()`（方法，夹紧后），不是字段
—— `mod.rs:318` `config.lease_ttl_secs() as f64`。Central 后端如果把 TTL 交给 controller 决定，
这两条夹紧就**从节点侧消失了**，必须在 controller 侧重建（护栏 §3.2 的前提）。

### 1.3 唯一的构造点：`src/orchestrator/paused_registry/mod.rs:298-329`

```rust
pub async fn build_paused_registry(
    config: &PausedRegistryConfig,
    identity: &NodeIdentity,
) -> anyhow::Result<Arc<dyn PausedSandboxRegistry>>
```

- `Local` ⇒ `Arc::new(DisabledPausedSandboxRegistry)`（`mod.rs:303`）
- `Postgres` ⇒ 校验 DSN 非空（缺就 `anyhow` 失败，注释明写"不静默降级"），
  然后 `PostgresPausedSandboxRegistry::connect(dsn, identity.cluster_id, max_connections, lease_ttl_secs()).await?`

**调用点上下文**（`src/bin/server.rs:148-149`）：

```rust
let paused_registry =
    build_paused_registry(&config.orchestrator.paused_registry, &identity_for_registry).await?;
let paused_wiring = PausedSandboxWiring::new(paused_registry, Arc::clone(&snapshot_manager), &identity_for_registry);
```

拿得到的依赖：
- `&AppConfig`（含 `config.cluster.scheduler_endpoint` —— **Central 后端要的端点就在手上**，
  但目前签名只收 `&PausedRegistryConfig`，需要把 `&ClusterConfig` 或整个 `&AppConfig` 加进签名）
- `identity_for_registry: NodeIdentity`（`src/identity.rs:16-22`）
  = `{ id: String, cluster_id: Uuid, service_instance_id: String, commit: String, version: String }`
  —— `cluster_id` / `node_id` 都齐了
- **是 async 上下文**（`#[tokio::main] async fn main`，`server.rs:55`）
- `lease_ttl_secs` 来自配置夹紧后的值，见上

位置：在 `ObservabilityReporter::new(...) + reporter.start()` 之后（`server.rs:131-146`），
也就是**心跳客户端已经建好了**。理论上可以复用 `reporter` 的 `Channel`，但 `ObservabilityReporter`
没有暴露 channel 的方法，且 `observability.enabled = false` 时整个 reporter 是 `None`
—— **不要把登记表挂在 observability 的开关下**，自己建一条 `Channel` 更干净（`Channel` 本身就是连接池，两条不算浪费）。

### 1.4 `Arc<dyn PausedSandboxRegistry>` 的持有者

只有一个：

```
src/api/impls/paused_coordinator.rs:87    registry: Arc<dyn PausedSandboxRegistry>   ← PausedSandboxCoordinator 字段
src/api/impls/paused_coordinator.rs:107   pub fn registry(&self) -> &Arc<dyn PausedSandboxRegistry>
```

链路：
```
build_paused_registry  →  PausedSandboxWiring::new (impls/mod.rs:40-53)
                              └─ Arc<PausedSandboxCoordinator>
                                    ├─ ApiImpl.paused          (impls/mod.rs:66)
                                    └─ orchestrator.set_paused_publisher(wiring.publisher())  (server.rs:155)
```

**持有它的 task**（`server.rs:292-322` `spawn_paused_record_upkeep`，两个 `tokio::spawn`）：

| task | 周期 | 调的方法 |
|---|---|---|
| renew | `reconcile_interval()` | `api_impl.renew_paused_leases()` → `renew_lease` |
| reconcile | `reconcile_interval()` | `reconcile_local_records()` → `get_many` ×2；`reclaim_expired_sandboxes()` → `reclaim_expired_holdings` |

外加**启动期三连（在 listener 打开之前，顺序 load-bearing，`server.rs:176-178`）**：
`release_stale_node_holdings()` → `renew_paused_leases()` → `reconcile_local_records()`。

另一处构造：`src/api/proxy.rs:1724-1725`，测试装配里塞 `DisabledPausedSandboxRegistry`，不影响生产。

---

## 2. B：现成的 gRPC 客户端 🔴

### 2.1 结论先说：codegen 链路**已经建好了，且吃的就是那份 proto**

`build.rs:8-16`：

```rust
tonic_prost_build::configure()
    .build_server(false)
    .build_client(true)
    .compile_protos(
        &["services/api/proto/scheduler.proto"],
        &["services/api/proto"],
    )
    .expect("failed to compile scheduler proto for Rust gRPC client");
```

- 库：`tonic = "0.14.2"` / `tonic-prost = "0.14.2"` / `prost = "0.14.3"`（`Cargo.toml:74-77`），
  build-dep `tonic-prost-build = "0.14.2"`（`Cargo.toml:128`）
- 输入：**`services/api/proto/scheduler.proto` 本体**。全仓只有这一个 `.proto`
  （`find . -name '*.proto'` 排除 target/thirdparty 后唯一命中）。
  **不存在第二份 Rust 侧副本，不存在手写 HTTP。**
- 生成物：`OUT_DIR`，经 `src/proto.rs` 挂进 crate：
  ```rust
  pub(crate) mod scheduler {
      tonic::include_proto!("scheduler.v1");
  }
  ```
  🔴 `pub(crate)` —— 生成类型只在 crate 内可见。Central 后端住在 `src/orchestrator/paused_registry/` 里，
  同 crate，**没问题**；但集成测（`tests/*.rs` 是外部 crate）**看不到这些类型**，见 §5.2。
- 增量：`cargo:rerun-if-changed=services/api/proto/scheduler.proto`（`build.rs:5`），改 proto 自动重编。

**所以阶段 2 在 Rust 侧是"加几个 RPC"，不是"先建 codegen 链路"。**

唯一要动 `build.rs` 的地方：`.build_server(false)`（`build.rs:9`）。
如果想在 Rust 侧写一个 in-process 假 scheduler 来测传输层错误映射，必须翻成 `true`，见 §5.2。

### 2.2 客户端在哪构造、连接怎么管

**两个现存消费方，风格一致：**

#### (a) `src/observability/reporter.rs` —— 心跳 + 沙箱事件 + UnregisterNode

```rust
// reporter.rs:246-251
fn build_scheduler_channel(scheduler_endpoint: &str) -> Result<Channel> {
    let endpoint = Endpoint::from_shared(raw_endpoint.clone())
        .with_context(|| format!("invalid scheduler endpoint: {raw_endpoint}"))?;
    Ok(endpoint.connect_lazy())
}
```

- **`connect_lazy()`** —— 构造期不连，第一次 RPC 才连；**端点不可达不会让构造失败**，
  只有 `from_shared` 的 URI 解析会失败。
- `Channel` 存在 `ObservabilityReporter.scheduler_channel`（`reporter.rs:40`），
  两个 task 各 `clone()` 一份（`Channel` clone 廉价，共享同一连接池）。
- 每次调用**新建一个 client**：`SchedulerClient::new(scheduler_channel.clone()).heartbeat(request)`
  （`reporter.rs:275`、`:346`、`:469`）—— 等价写法，`Channel` 才是连接。
- **超时**：`const GRPC_CALL_TIMEOUT: Duration = Duration::from_secs(10)`（`reporter.rs:21`），
  逐调用 `request.set_timeout(GRPC_CALL_TIMEOUT)`（`:270`、`:348`、`:468`）。
- **重连**：没有显式逻辑。tonic 的 `Channel` 在下一次请求时自行重连；
  **没有 tower retry 层、没有 HTTP/2 keepalive、没有 `connect_timeout`、没有 TLS**。
- **退避**：只在应用层的心跳循环里 —— `backoff` 从 `config.interval` 起，失败翻倍，
  上限 `MAX_REPORT_BACKOFF = 60s`（`reporter.rs:20`、`:100`、`:141-151`）。成功即复位。
  🔴 这套退避**属于心跳循环，不属于 channel**。登记表后端拿不到它，要自己写或不写。

#### (b) `src/p2p/discovery/scheduler.rs` —— P2P peer 发现 + artifact registry

- 同样 `GrpcEndpoint::from_shared(...).connect_lazy()`（`:154-155`），
  端点非法时**降级成 `NoopP2pPeerDiscovery`**（`:157-163`），不让进程死。
- 持一个长期 `SchedulerClient<Channel>`（`:24`、`:58`），每次调用 `self.client.clone()`。
- 两档超时：`REFRESH_RPC_TIMEOUT = 10s`、`ARTIFACT_RPC_TIMEOUT = 5s`（`:137-138`）。
- 刷新循环有**启动抖动**（`rand::rng().random_range(Duration::ZERO..refresh_interval)`，`:161`），
  防惊群 —— Central 后端的 reconcile 循环值得抄这一条（今天 `spawn_paused_record_upkeep` 没有抖动）。

### 2.3 `[cluster].scheduler_endpoint`（`src/cfg.rs:646-654`）

```rust
#[derive(Debug, Config, Clone)]
pub struct ClusterConfig {
    /// Shared gRPC scheduler endpoint for cluster-level services.
    #[config(env = "AENV_OBSERVABILITY_SCHEDULER_ENDPOINT", parse_env = parse_trimmed_string)]
    pub scheduler_endpoint: Option<String>,
}
```

- 类型 `Option<String>`，**无默认值**。
- env 名保留了历史包袱：`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`（字段已搬到 `[cluster]`，env 名没改）。
- `normalize()`（`cfg.rs:1423-1432`）把空白串归一成 `None`。
- 形状：`http://agentenv-scheduler:9090`（`deploy/k8s/base/agentenv-daemonset.yaml:64`），
  也就是 ClusterIP Service。**没有客户端负载均衡**（`Endpoint::from_shared` 单端点），
  但 scheduler `replicas: 1`（`deploy/k8s/base/scheduler-deployment.yaml:8`），今天无所谓。

**没配时的行为**（三处不同，值得注意）：
| 消费方 | 没配 ⇒ |
|---|---|
| `ObservabilityReporter` | `ReporterConfig::resolve` 返回 `None` + `warn!`，reporter 整个不建（`reporter.rs:486-497`） |
| P2P discovery | 端点非法 ⇒ Noop discovery（`scheduler.rs:157-163`） |
| **Central 后端（待定）** | 🔴 必须**像 `Postgres` 缺 DSN 一样硬失败**，理由与 `mod.rs:291-296` 的注释逐字相同：静默降级成节点本地语义，只有在丢节点时才暴露 |

### 2.4 需要额外注意的 tonic 默认值

- `max_decoding_message_size` 默认 **4 MiB**（客户端解码上限）。
  `GetSandboxes` 批量读**不带 metadata** 的话（见 §3.5），1000 行 × 约 200 B ≈ 200 KB，安全。
  一旦把 metadata 塞进批量响应就有风险。
- `Request::set_timeout` 写的是 gRPC `grpc-timeout` 头，服务端也会看到 —— Go 侧 handler 应当尊重 `ctx`。

---

## 3. C：类型与序列化

### 3.1 `SandboxId` / `SnapshotId`

| 类型 | 定义 | 底层 | serde |
|---|---|---|---|
| `SandboxId` | `src/types/id.rs:6-7` | `struct SandboxId(Uuid)`（私有字段） | `#[derive(Serialize, Deserialize)]` 无 `transparent` |
| `SnapshotId` | `src/snapshot/types/value.rs:7-12` | `struct SnapshotId(pub(crate) Uuid)` | 同上 |

🔴 **两者都是 newtype 且都没有 `#[serde(transparent)]`。**
serde 对 `struct X(T)` 的 derive 默认按 **newtype struct** 处理，JSON 里就是内层值本身
（`"0199aa..."`），所以在 `SandboxMetadata.id` 里表现为一个普通 UUID 字符串 —— 与 `transparent` 同形。
但**不要**据此以为可以随便加字段。

生成方式都是 `Uuid::now_v7()`（`id.rs:10-12`、`value.rs:16-18`），
所以 sandbox_id 是**时间有序**的 —— 对 Go 侧分页/游标是个可用性质。

Display 一律小写带连字符（`value.rs:9-11` 明确注明：PosixFs 后端按目录名查找，大写会 miss）。
proto 里已经全用 `string sandbox_id` + `::text` 转换（`services/scheduler/internal/registry/postgres.go:31,37`），继续沿用即可。

DB 列类型是 `UUID`（`postgres.rs:39,44`），Rust 侧 bind `sandbox_id.into_inner()`（裸 `Uuid`）。

### 3.2 `SandboxMetadata`（`src/orchestrator/store/metadata.rs:30-64`）

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]   // ← 无 rename_all，无 deny_unknown_fields
pub struct SandboxMetadata {
    pub id: SandboxId,
    pub snapshot_id: String,
    pub snapshot_alias: Option<String>,
    pub state: SandboxState,
    pub created_at: SystemTime,
    pub timeout: Option<Duration>,
    pub timeout_action: SandboxTimeoutAction,
    pub expires_at: Option<SystemTime>,
    pub auto_resume: bool,
    #[serde(default)]
    pub virtualization_mode: VirtualizationMode,
    pub runtime_versions: SnapshotRuntimeVersions,
    pub resources: SandboxResources,
    pub context: CommandContext,
    pub startup: Option<StartupCommand>,
    #[serde(default, skip_serializing_if = "ImageConfigs::is_empty")]
    pub image_configs: ImageConfigs,
    pub user_metadata: Option<HashMap<String, String>>,
    pub network_policy: SandboxNetworkPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<CustomExtensionParams>,
    #[serde(default)]
    pub secure: bool,
    #[serde(skip)]
    pub paused_state: Option<Arc<dyn PausedSandboxState>>,
}
```

#### 逐条 serde 属性

| 属性 | 出现在哪 | 含义 |
|---|---|---|
| `#[serde(deny_unknown_fields)]` | **全仓零处**（`grep -rn deny_unknown_fields src/` 无命中） | 未知字段被静默忽略 |
| `rename_all` | `SandboxMetadata` 本身**没有** | 字段名即 Rust 名（已是 snake_case） |
| `flatten` | 无 | — |
| `#[serde(default)]` | `virtualization_mode` / `image_configs` / `custom_extension_params` / `secure` | 缺失即默认，**这四个是"会随版本增删"的观察窗口** |
| `skip_serializing_if` | `image_configs`（空数组时省略）、`custom_extension_params`（`None` 时省略） | 写出的 JSON **字段数不固定** |
| `#[serde(skip)]` | `paused_state` | 从不落 JSON，读时恒为 `None` |

#### 嵌套类型的 tag 方式（全是**外部 tag 的 unit variant**，即裸字符串）

| 类型 | 定义 | JSON 形状 |
|---|---|---|
| `SandboxState` | `src/orchestrator/types.rs:57-67`，**无 rename_all** | `"Creating"` / `"Resuming"` / `"Running"` / `"Snapshotting"` / `"Forking"` / `"Pausing"` / `"Paused"` / `"Killing"` |
| `SandboxTimeoutAction` | `metadata.rs:16-20`，无 rename_all | `"Pause"` / `"Delete"` |
| `VirtualizationMode` | `src/virtualization.rs:4-10`，`rename_all = "lowercase"` | `"kvm"` / `"pvm"` |
| `BaseSandboxNetworkPolicy` | `src/sandbox/network/policy.rs:16-23`，无 rename_all | `"Default"` / `"Allow"` / `"Deny"` |

🔴 **两套大小写词汇并存，且指的是同一个概念**：
`metadata.state` 是 **PascalCase**（`"Paused"`），DB 的 `state` 列是 **lowercase**（`'paused'`，
CHECK 约束在 `postgres.rs:51-52`）。而且两者**取值集合都不同**
（`SandboxState` 有 8 个，`PausedRegistryState` 有 5 个）。
Go 侧任何"把 metadata.state 和行 state 当同一个枚举"的代码都是错的。

#### 嵌套结构体

```
SnapshotRuntimeVersions  (snapshot/types/version.rs:10-17)
    kernel_version: String
    firecracker_version: String
    envd_version: String
    tools_drive_version: String     #[serde(default)]

SandboxResources         (types/resources.rs:7-12)   Copy
    cpu_count: u32, memory_mib: u32, disk_size_mib: u32

CommandContext           (snapshot/types/snapshot.rs:156-172)
    env_vars: HashMap<String,String>      ← 无 skip，恒存在
    workdir: String                       ← 无 skip，恒存在
    user: Option<String>                  #[serde(default, skip_serializing_if="Option::is_none")]
    exposed_ports: Vec<String>            #[serde(default, skip_serializing_if="Vec::is_empty")]
    entrypoint: Option<Vec<String>>       同上
    cmd: Option<Vec<String>>              同上
    volumes: Vec<String>                  同上
    labels: HashMap<String,String>        #[serde(default, skip_serializing_if="HashMap::is_empty")]

StartupCommand           (snapshot/types/snapshot.rs:277-282)
    start_cmd: String, ready_cmd: String, context: CommandContext

SandboxNetworkPolicy     (sandbox/network/policy.rs:76-80)
    base_policy: BaseSandboxNetworkPolicy
    egress: SandboxNetworkEgressPolicy { allowed_cidrs, allowed_domains, denied_cidrs : Vec<String> }
        ← 三个 Vec 都**没有** skip_serializing_if，恒存在

ImageConfigs             (types/image_configs.rs:24-26)   #[serde(transparent)] → 裸数组
    └─ ImageConfigEntry  (types/image_configs.rs:5-17)
         driveId    ← #[serde(rename = "driveId", default, skip_serializing_if="Option::is_none")]  🔴 camelCase
         mountPath  ← #[serde(rename = "mountPath")]                                                 🔴 camelCase
         config     ← serde_json::Value（原样透传的 OCI config，任意 JSON）

CustomExtensionParams    (sandbox/custom_extension/client.rs:353)
    = serde_json::Map<String, serde_json::Value>   → 任意 JSON 对象
```

🔴 **`driveId` / `mountPath` 是整个 metadata 里仅有的两个 camelCase 键**，
其余全部 snake_case。任何"整体驼峰化 / 整体蛇形化"的 Go tag 策略都会踩这里。

#### 实际 JSON 样例

**推导依据**：`SandboxMetadata::default()`（`metadata.rs:66-96`）逐字段展开，
配合上表的 serde 属性；`SystemTime` 与 `Duration` 用 serde 的标准表示
（`SystemTime` → `{"secs_since_epoch":u64,"nanos_since_epoch":u32}`，
`Duration` → `{"secs":u64,"nanos":u32}`）。
仓内**没有**任何 JSON fixture 可直接引（`grep` 全仓 `secs_since_epoch` / `timeout_action` 零命中），
所以这份样例是从类型推导的，**没有跑过 `serde_json::to_string`**。
👉 建议实施时第一件事就是补一个 Rust 测试把真值 dump 成 fixture，见 §5.4。

`SandboxMetadata::default()`（最小形态，注意 `image_configs` / `custom_extension_params` / `paused_state` 都不出现）：

```json
{
  "id": "0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07",
  "snapshot_id": "unknown",
  "snapshot_alias": null,
  "state": "Creating",
  "created_at": { "secs_since_epoch": 1755561600, "nanos_since_epoch": 123456789 },
  "timeout": null,
  "timeout_action": "Pause",
  "expires_at": null,
  "auto_resume": false,
  "virtualization_mode": "kvm",
  "runtime_versions": {
    "kernel_version": "unknown",
    "firecracker_version": "unknown",
    "envd_version": "unknown",
    "tools_drive_version": "unknown"
  },
  "resources": { "cpu_count": 1, "memory_mib": 128, "disk_size_mib": 1024 },
  "context": { "env_vars": {}, "workdir": "/" },
  "startup": null,
  "user_metadata": null,
  "network_policy": {
    "base_policy": "Default",
    "egress": { "allowed_cidrs": [], "allowed_domains": [], "denied_cidrs": [] }
  },
  "secure": false
}
```

一个更接近生产的形态（有超时、有镜像配置、有 secure）：

```json
{
  "id": "0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07",
  "snapshot_id": "0199c8ff-1122-7000-8000-aabbccddeeff",
  "snapshot_alias": "tpl-node22",
  "state": "Paused",
  "created_at": { "secs_since_epoch": 1755561600, "nanos_since_epoch": 0 },
  "timeout": { "secs": 900, "nanos": 0 },
  "timeout_action": "Pause",
  "expires_at": { "secs_since_epoch": 1755562500, "nanos_since_epoch": 0 },
  "auto_resume": true,
  "virtualization_mode": "kvm",
  "runtime_versions": { "kernel_version": "6.1.102", "firecracker_version": "1.13.1",
                        "envd_version": "0.4.2", "tools_drive_version": "2026.08.01" },
  "resources": { "cpu_count": 2, "memory_mib": 4096, "disk_size_mib": 20480 },
  "context": { "env_vars": { "PATH": "/usr/local/bin:/usr/bin" }, "workdir": "/home/user",
               "user": "user", "exposed_ports": ["5173/tcp"] },
  "startup": { "start_cmd": "…", "ready_cmd": "…",
               "context": { "env_vars": {}, "workdir": "/home/user" } },
  "image_configs": [
    { "mountPath": "/", "config": { "Env": ["PATH=/usr/bin"], "Cmd": ["/bin/sh"] } },
    { "driveId": "drive-1", "mountPath": "/data", "config": {} }
  ],
  "user_metadata": { "sandboxId": "…", "workspaceId": "42" },
  "network_policy": { "base_policy": "Default",
                      "egress": { "allowed_cidrs": [], "allowed_domains": [], "denied_cidrs": [] } },
  "custom_extension_params": { "anything": true },
  "secure": true
}
```

#### 读侧的硬约束（Go 写坏了会怎样）

写路径：`postgres.rs:295` `serde_json::to_value(&entry.metadata)` → bind 成 JSONB。
读路径：`postgres.rs:220-228` `serde_json::from_value::<SandboxMetadata>` →
失败即 `PausedRegistryError::InvalidRecord { reason: "metadata is not a sandbox record" }`。

`InvalidRecord` 会把这一行**在所有读路径上变成永久错误**：`get` / `get_many` / `claim_for_resume`
都走同一个 `decode`，所以一旦 Go 侧写坏一行，那台沙箱**从此既读不出也认领不了**，
只能人工改库。

**缺失即报错的字段**（非 `Option`、且无 `#[serde(default)]`）：
`id` / `snapshot_id` / `state` / `created_at` / `timeout_action` / `auto_resume` /
`runtime_versions` / `resources` / `context` / `network_policy`。
（`Option<T>` 字段缺失时 serde 走 `missing_field` 的 `deserialize_option` 分支得到 `None`，
所以 `snapshot_alias` / `timeout` / `expires_at` / `startup` / `user_metadata` 缺了也没事。）

👉 **结论：Go 侧绝对不要把 metadata 反序列化成 Go struct 再序列化回去。**
用 `json.RawMessage`（或 `pgtype.JSONB` 的 raw 形态）原样透传，proto 里用 `bytes metadata_json`。
不要用 `google.protobuf.Struct` —— 它把所有数字塌成 `double`，且会重排/归一化。

补充：~~`serde_json::to_value` 用 `BTreeMap` 建对象（Cargo.toml 没开 `preserve_order`），键序已排序~~；
而 PG JSONB 本身就重排键。**键序不是契约，字节级往返只需要"键集合 + 值"一致。**

> 🔧 **勘误（2026-08-19，实施期实证）：删去的那半句是错的。**
> `storage/overlaybd/Cargo.toml:28` 开了 `serde_json = { features = ["preserve_order"] }`，
> **Cargo 的 feature 统一让 `agentenv` 也吃到** —— 于是 `serde_json::Map` 在本 workspace 是
> **插入序（IndexMap）而不是 BTreeMap**，`SandboxMetadata` 里 `HashMap` 字段
> （`env_vars` / `user_metadata`）**每进程随机的迭代序会直接漏进 JSON**。
>
> 后果：不处理的话 golden fixture 每次跑出来都不一样，根本钉不住。
> 处理办法是渲染前递归按键排序（`src/orchestrator/store/metadata.rs` 的 `sort_keys` / `canonical()`），
> 连跑 5 次确认稳定。**结论那句仍然成立**（键序不是契约，PG jsonb 自己会重排），
> 错的只是"为什么它稳定"的机理。见 [`_impl-D5-sliceA.md`](_impl-D5-sliceA.md) §2。

### 3.3 `PausedSandboxEntry` ↔ DB 列映射（`types.rs:56-79` / `postgres.rs:204-267`）

| Rust 字段 | 来源 | 说明 |
|---|---|---|
| `sandbox_id: SandboxId` | 列 `sandbox_id UUID` | **解码**：`SandboxId::from_uuid(uuid)`（`:206-208`） |
| `cluster_id: Uuid` | 列 `cluster_id UUID` | 直读 |
| `state: PausedRegistryState` | 列 `state TEXT` | **解码**：`PausedRegistryState::parse`（`types.rs:41-48`），未知值 ⇒ `InvalidRecord`（`postgres.rs:190-195`） |
| `generation: i64` | 列 `generation BIGINT` | 直读 |
| `origin_node_id: String` | 列 `origin_node_id TEXT` | 直读 |
| `claimed_by_node_id: Option<String>` | 列 `claimed_by_node_id TEXT` | 直读（可空） |
| `snapshot_id: Option<SnapshotId>` | 列 `snapshot_id UUID` | **解码**：`snapshot_uuid.map(SnapshotId::from_uuid)`（`:258`）；且 `state == Paused && NULL` ⇒ `InvalidRecord`（`:232-238`） |
| `metadata: SandboxMetadata` | 列 `metadata JSONB` | **解码**：`serde_json::from_value`（`:222`） |
| `paused_at: DateTime<Utc>` | 列 `paused_at TIMESTAMPTZ` | 直读 |
| `updated_at: DateTime<Utc>` | 列 `updated_at TIMESTAMPTZ` | 直读 |

`ENTRY_COLUMNS`（`postgres.rs:21-22`）**不含** `lease_expires_at` / `sandbox_expires_at`
—— 这两列节点侧只写不读（判定全在 SQL 的 `LEASE_EXPIRED` 谓词里，`postgres.rs:87`），
所以 `PausedSandboxEntry` 里根本没有它们。
Go 侧读路径反而**多读了这两列**（`services/scheduler/internal/registry/postgres.go:29-41`，注释明说"deliberately wider"）。
阶段 2 的 `GetSandboxes` 响应要不要带它们：**节点侧用不上，别带**（少一份跨进程的时钟解释）。

### 3.4 时间类型与精度

| 场景 | 表示 | 精度 |
|---|---|---|
| `paused_at` / `updated_at` / `lease_expires_at` / `sandbox_expires_at` 在 **DB** | `TIMESTAMPTZ` | **微秒**（PG 硬上限） |
| 同上在 **sqlx ↔ Rust** | `chrono::DateTime<Utc>`（`sqlx` features 含 `chrono`，`Cargo.toml:57-64`） | chrono 支持纳秒，但**经 PG 一定被截到微秒** |
| 同上在 **serde** | 这几个字段**不过 serde** —— 它们是 DB 列不是 JSON | — |
| `metadata.created_at` / `expires_at` 在 **serde/JSONB** | `SystemTime` → `{"secs_since_epoch","nanos_since_epoch"}` | **纳秒**（JSON 里是整数，不丢） |
| `metadata.timeout` 在 **serde/JSONB** | `Duration` → `{"secs","nanos"}` | 纳秒 |
| `HeldSandbox.expires_at` | `Option<DateTime<Utc>>`，由 `metadata.expires_at: Option<SystemTime>` 转来（`paused_recovery.rs:372` `metadata.expires_at.map(DateTime::<Utc>::from)`） | 转换无损，落库截到微秒 |

**跨 gRPC 的选型建议**：

- 现有 proto 全用 `int64 ..._unix_ms`（`scheduler.proto` `RegistrySandbox` 的四个时间字段）
  —— 那是**只读观测**面，毫秒够。
- 阶段 2 的写路径**不要沿用毫秒**。`begin_pause` 会把节点给的 `now` 同时写进 `paused_at` 和 `updated_at`
  （`postgres.rs:339` `VALUES (..., $5, $5, ...)`），而 `updated_at` 参与
  `COALESCE(lease_expires_at, updated_at) < now()`（`postgres.rs:87`）这条**升级期的兼容判据**。
  毫秒截断本身不致命（比 90s TTL 小 5 个数量级），但它会让"Rust 直写"和"经 Go 写"的同一行
  产生可观测的差异，**排障时会浪费时间**。
- 建议：用 `google.protobuf.Timestamp`（秒 + 纳秒），Go 侧 `pgx` 绑 `time.Time`，
  由 PG 自己截到微秒 —— 与今天 Rust 直写的结果**逐字相同**。
- 🔴 更重要的一条：`renew_lease` / `claim_for_resume` / `reclaim_expired_holdings` 里
  **所有租约判定都用 DB 的 `now()`**（`postgres.rs:672` `now() + make_interval(...)`、`:87` `< now()`），
  从不用节点时钟。阶段 2 必须保持这一条 —— controller 一旦改成"用自己的时钟算 lease_expires_at"，
  就把一个跨进程时钟偏差引进了唯一一处仲裁判据。

---

## 4. D：错误语义

### 4.1 `PausedRegistryError` 全部变体（`mod.rs:44-64`）

```rust
pub enum PausedRegistryError {
    Backend { operation: &'static str, source: anyhow::Error },   // 后端故障
    InvalidRecord { sandbox_id: String, reason: String, source: Option<anyhow::Error> },  // 行读不懂
    GenerationConflict { sandbox_id: String, expected: i64 },     // CAS 失败
}
```

🔴 **生产代码里没有任何一处按变体 match**（`grep PausedRegistryError` 在 `src/` 内除定义文件外零命中；
只有 `tests/paused_registry.rs:638,667` 断言变体）。
**所有调用方一律 `Err(err) => …` 一把抓。**
这意味着 Central 后端把 gRPC 错误映射成哪个变体，对现有行为**完全没有影响** ——
但也意味着**没有任何一处能区分"后端不可达"和"这一行坏了"**。
护栏 §3.1 要求"任何非 OK ⇒ `Backend`"，落地上是对的，但要清楚它换不来行为差异，
真正的行为差异在下面这张表里。

### 4.2 逐个调用点：错误当"停手"还是当"空"

| # | 调用点 | 方法 | 错误处理 | 方向 |
|---|---|---|---|---|
| 1 | `paused_recovery.rs:587-595` | `get_many`（running 侧） | `warn!("registry unreadable; stopping running-sandbox reconciliation"); return` | ✅ 停手 |
| 2 | `paused_recovery.rs:676-685` | `get_many`（paused 侧） | `warn!("registry unreadable; stopping paused-record reconciliation"); return` | ✅ 停手 |
| 3 | `paused_recovery.rs:173-181` | `claim_for_resume` | `warn!("could not reach the registry to arbitrate a resume; proceeding locally"); return ResumeArbitration::Proceed` | 🔴 **放行** |
| 4 | `paused_recovery.rs:216-221` | `get`（`resolve_missing_local_resume`） | `return MissingLocalResume::Undecided(err)` | ✅ 不下结论 |
| 5 | `paused_recovery.rs:778-785` | `get`（`superseded_by_cluster`） | `return Err(())` ⇒ 调用方 `discard_if_superseded` 的 `let Ok(Some(_)) else { return false }` ⇒ 不删 | ✅ 停手 |
| 6 | `paused_recovery.rs:298-300` | `remove` | `warn!` 后继续（行留着当 dangling） | 中性 |
| 7 | `paused_recovery.rs:375-386` | `renew_lease` | `warn!("…may be rebuilt elsewhere from an older one")`，不重试 | ⚠️ 见 §4.4 |
| 8 | `paused_recovery.rs:430-437` | `release_node_holdings` | `warn!("…stay unclaimable until a later start")`，**永不重试** | ⚠️ 见 §7 风险 2 |
| 9 | `paused_recovery.rs:481-483` | `reclaim_expired_holdings` | `warn!`，下个 tick 再来 | ✅ 安全 |
| 10 | `paused_coordinator.rs:143-153` | `begin_pause` | `warn!("…stays resumable on this node only"); return None` ⇒ 整个 publish 放弃 | ✅ 保守 |
| 11 | `paused_coordinator.rs:183-197` | `complete_pause` | `warn!` + `discard_unreferenced_snapshot`（**删刚上传的快照**） | ⚠️ 见 §4.5 |
| 12 | `paused_coordinator.rs:213-219` | `mark_local_only` | `warn!`，行停在 `publishing` | ⚠️ 见 §4.5 |
| 13 | `paused_coordinator.rs:241-251` | `mark_running` | `warn!` + `confirmed = false`（不登记 running registration） | ✅ 保守 |
| 14 | `paused_coordinator.rs:283-291` | `get`（forget 路径） | `warn!`，`return` | ✅ 停手 |
| 15 | `paused_coordinator.rs:308-310` | `remove`（forget 路径） | `warn!` 继续 | 中性 |
| 16 | `paused_recovery.rs:~800` | `release_claim` | `warn!` 继续 | 中性 |

**方案 §3.1 引的那句注释在 `#1` 和 `#2`。**
实际文案不是 `"registry unreachable; stopping reconciliation"`，是
`"registry unreadable; stopping running-sandbox reconciliation"`（`:592`）与
`"registry unreadable; stopping paused-record reconciliation"`（`:681`）
—— 两处两条，写验收断言时按实际文案。

### 4.3 「当成停手」的完整清单

除了 `get_many` 两处，还有 **#4 / #5 / #10 / #14**，一共 6 处。
其中 **#5 是隐式的**：`superseded_by_cluster` 返回 `Err(())`，靠调用方
`let Ok(Some(superseded)) = … else { return false }`（`paused_recovery.rs:732-734`）吃掉，
文档注释在 `:726-730`（"Any doubt — registry disabled, registry unreachable, row still ours —
leaves the local record alone"）。这一处**没有日志**，Central 后端下变成"读不到就静默不删"，
排障时会看不见。建议实施时补一条 `debug!`。

### 4.4 `renew_lease` 失败不是"停手"也不是"放行"

`#7` 失败后什么都不做，下个 `reconcile_interval`（30s）再试。
`lease_ttl_secs()` ≥ `interval * 3`（`cfg.rs:452-457`），所以**连续漏 3 次**才过期。
Central 后端把这条链路从"本地 PG"变成"gRPC → scheduler → PG"之后，
连续 3 次失败的概率**显著上升**（scheduler 滚动升级就够）。
护栏 §3.2 的 grace 期正是为这个 —— 但注意它只在 controller 侧生效；
**node 侧连续漏续这件事本身，Central 后端无法感知**，除非把"连续失败计数"暴露成 metric。
建议：Central 后端为 `renew_lease` 加一个 `agentenv_paused_registry_renew_consecutive_failures` gauge。

### 4.5 `complete_pause` / `mark_local_only` 失败会**删快照**

`#11`：快照已经传上去了，但 `complete_pause` 报错 ⇒ `discard_unreferenced_snapshot`
（`paused_coordinator.rs:194-196`）。今天这个错误只可能是"PG 写失败"或 `GenerationConflict`。
Central 之后多了一种：**写其实成功了，但响应在网络上丢了**（gRPC deadline / EOF）。
那时快照是**被引用的**，而节点会把它删掉 ⇒ 行指向一个不存在的快照 ⇒
下次 resume 走 `paused_recovery.rs:293-302` 的 "snapshot is missing from the repository" 分支 ⇒ **`remove` 掉整行** ⇒ 沙箱没了。

🔴 **这是护栏 §3.1/§3.2/§3.3 都没覆盖的第四个洞**，且是纯粹由"本地调用变跨进程调用"引入的。
规避：`TransitionSandbox` 必须**幂等 + 可重放**（同 `expect_generation` 重发得到相同结果，
而不是第二次报 `GenerationConflict`），或者节点侧在 `complete_pause` 失败后先 `get` 一次确认再决定删不删。
后者更便宜，且今天 `paused_coordinator.rs:335` 已经有一个 `let reread = self.registry.get(sandbox_id).await;`
的先例可以照抄。

### 4.6 `arbitrate_resume` 的 fail-open（`paused_recovery.rs:160-185`）

```rust
Err(err) => {
    warn!(error = %err, %sandbox_id,
          "could not reach the registry to arbitrate a resume; proceeding locally");
    return ResumeArbitration::Proceed;      // ← registry 报错 ⇒ 放行
}
```

**影响面**：`ResumeArbitration` 的四个变体在 `sandbox.rs:1262-1280` 决定 HTTP 结果：

| 变体 | 结果 |
|---|---|
| `Blocked` | 409 "sandbox is held by node X" |
| `NotReady` | 409 "snapshot is still being published by node X" |
| `Held(entry)` | 拿到认领权，可走跨节点重建（`restore_from_cluster`，用 `entry.metadata`） |
| **`Proceed`** | **不做任何集群检查，直接 `orchestrator.resume_sandbox` 本地拉起** |

也就是说：`claim_for_resume` 一报错，这一发 resume 就**完全跳过双活防线**。
本地有副本 ⇒ 直接拉起（另一节点可能正在跑同一台）；本地没有 ⇒ 落到
`resolve_missing_local_resume`（`:202-260`，这个是安全的，会回 409 而不是 404）。

所以**危险的是"本地有陈旧副本 + registry 不可达"**这一种组合 ——
而 `discard_if_superseded`（`sandbox.rs:1254`，在 arbitrate 之前跑）在同一次故障下也会
因为 `#5` 的 `Err(())` 而不删。两道防线**同一个故障源，同时失效**。

这条在今天可以接受：registry 报错 = node 到集群内 PG 的连接故障，罕见。
Central 之后，registry 报错 = node 到 scheduler 的 gRPC 故障，而 scheduler
`replicas: 1`、无 PDB、无 surge（`deploy/k8s/base/scheduler-deployment.yaml:8`），
**每次发版都开一个窗口**。见 §7 风险 1。

---

## 5. E：测试基建

### 5.1 `tests/paused_registry.rs` 的 harness

| 项 | 做法 | 行 |
|---|---|---|
| DSN | `std::env::var("AENV_PAUSED_REGISTRY_TEST_DSN")`，trim + 非空过滤 | `:35-39` |
| 防假绿 | `require_db!()` 宏：无 DSN 时先 `assert!(env::var("AENV_PAUSED_REGISTRY_TEST_REQUIRED").is_err(), ...)` 再 `eprintln!` + `return` | `:48-62` |
| 建实例 | `PostgresPausedSandboxRegistry::connect(dsn, cluster_id, 4, TEST_LEASE_SECS)`，`TEST_LEASE_SECS = 1.0` | `:64-68`、`:28` |
| **隔离** | **每个测试自己 `Uuid::new_v4()` 当 cluster_id**，靠 `cluster_id` 列天然分区 | 各测试首行 |
| **清理** | **没有清理**。不 DELETE、不 TRUNCATE、不 DROP（全文零命中） | — |
| 加速 | `PAST_LEASE = 1600ms` 用来"睡过一个租约" | `:30` |
| 数量 | 29 个 `#[tokio::test]` | — |

对比 Go 侧（`services/scheduler/internal/registry/postgres_integration_test.go`）：
用 `SCHEDULER_REGISTRY_TEST_DSN`，**自己建表 + 结束时 DROP**（`:56-62` 注释），
schema DDL 是从 `postgres.rs` **逐字复制**过来的（`:12-37`，注释明写"a paraphrase would pass while the real thing failed"）。
🔴 **Go 侧没有 `_REQUIRED` 防假绿开关**，只有 `t.Skip`（`:65-68`）。

### 5.2 建议：Rust 侧**不要**起 Go 二进制

**理由（按分量排序）：**

1. **`src/proto.rs:1` 是 `pub(crate)`** —— `tests/*.rs` 是独立 crate，看不见生成的 gRPC 类型。
   要在集成测里搭假 scheduler，得先把 proto 模块导出成 `pub`，
   而那等于把 scheduler 的 wire 类型变成 agentenv 的公开 API 面。不值。
2. **`build.rs:9` 是 `.build_server(false)`** —— 今天根本没有服务端 stub。
   翻成 `true` 之后，**crate 内 `#[cfg(test)]` 单测**可以起一个
   `tonic::transport::Server` 绑 `127.0.0.1:0`，实现一个可编程的假 `PausedRegistryService`，
   注入 `UNAVAILABLE` / deadline / 半截流 / 部分 map，**逐条验证 §4.2 那 16 个调用点的方向**。
   这才是 Central 后端真正要测的东西 —— 它是个传输层壳子，语义在 SQL 里。
3. **`cargo test` 与 Go 构建耦合是纯负债**：CI 的 Rust job（`ci.yml:56-65`）没有 Go toolchain，
   Go job（`services-ci.yml`）没有 Rust toolchain，两边 `paths:` 过滤器也不重叠
   （`ci.yml` 只在 `**.rs` 变时跑，`services-ci.yml` 只在 `services/**` 变时跑）。
   要跑 Go 二进制就得让两边都装两套 toolchain，代价远大于收益。
4. **真语义测试该在 Go 侧**：R2 §5 已经把 29 个 Rust 集成测列成了 Go 契约测的源材料，
   Go 侧的 harness（自建表 + DROP）已经就位，把它们移植过去是最短路径。

**建议的三层：**

| 层 | 在哪 | 测什么 | 需要什么 |
|---|---|---|---|
| L1 传输层单测 | Rust `#[cfg(test)]`，crate 内 | 状态码 → `PausedRegistryError` 映射；`GetSandboxes` 全有或全无；deadline；连接拒绝 | `build.rs` 翻 `build_server(true)` |
| L2 语义契约测 | Go，`services/scheduler/internal/registry/` | R2 §5 的 29 条移植版，真 PG | 已有 harness；**补 `SCHEDULER_REGISTRY_TEST_REQUIRED`** |
| L3 跨语言 golden | Rust dump fixture → Go 读 fixture | metadata JSONB 字节级往返 | 见 §5.4 |

**唯一需要真 scheduler 的**是端到端（node → scheduler → PG），
那个属于 k3s 上的验收（`docs/topics/agentenv-pve-k3s-cluster.md` 的 dev 集群），不属于 `cargo test`。

### 5.3 这些测试今天在 CI 里怎么被跑到 —— 答案：**根本没跑**

| Makefile 目标 | 命令 | 覆盖 `tests/paused_registry.rs`？ |
|---|---|---|
| `make test-unit`（`Makefile:117-123`） | `cargo test -p agentenv … --lib` | ❌ 只有 lib target |
| `make test-agent`（`Makefile:129-134`） | `cargo test -p agentenv`（全 target） | ✅ 会跑（但没 DSN ⇒ 全 skip） |
| `make test-agent-integration`（`Makefile:136-146`） | `--test integration --test orchestrator_integration` | ❌ 显式点名，不含 |
| `make test`（`Makefile:115`） | `test-agent + test-envd + test-ublk` | ✅ 间接 |

CI 里的全部 `make` 调用：

```
ci.yml:65                make test-unit PROFILE=debug          ← 只跑 --lib
integration-tests.yml:49 make test-agent-integration           ← 点名两个 target
ublk-tests.yml:49        make test-ublk
envd-tests.yml:54        make test-envd
coverage.yml:46          make coverage                          ← cargo adev coverage
benchmark.yml:29         make bench
services-ci.yml:37       make test                              ← Go 侧
```

🔴 **没有任何 workflow 跑 `make test` 或 `make test-agent`，也没有任何 workflow 设置
`AENV_PAUSED_REGISTRY_TEST_DSN`**（全 workflow grep 零命中）。
`services-ci.yml:33-35` 只装 redis，不装 postgres，所以 Go 侧的 PG 集成测**也是全 skip**。

**结论：登记表的 29 个 Rust 测 + Go 的 PG 集成测，今天在 CI 里一次都没跑过。**
`AENV_PAUSED_REGISTRY_TEST_REQUIRED` 这个防假绿机制**从来没有被启用过**。
阶段 2 要动这块 SQL，第一件事应该是给 CI 加一个带 `services: postgres` 的 job
并把 `_REQUIRED=1` 打开 —— 否则所有"契约对齐"的验收都是空转。

### 5.4 建议补的 golden fixture（§3.2 的落点）

一个 Rust 测试（放 `src/orchestrator/store/metadata.rs` 的 `#[cfg(test)]` 里，
或新建 `tests/metadata_golden.rs`）：

- 构造一个**把所有 `skip_serializing_if` 都跨过去**的 `SandboxMetadata`
  （非空 `image_configs`、`Some(custom_extension_params)`、`CommandContext` 的六个可选项全填），
  `serde_json::to_string_pretty` 写进 `services/scheduler/internal/registry/testdata/metadata_golden.json`，
  并断言与仓内已有文件一致（不一致就失败，等于把 fixture 钉住）。
- Go 侧一个测试读同一个文件，走**写路径的完整链路**（RPC 解码 → 落 JSONB → 读回），
  断言 `jsonb` 读回的值与原文 `JSONB` 相等（用 PG 的 `=` 比较 jsonb，天然忽略键序）。

这是唯一能在两侧都不启动对方的前提下，证明"原样往返"的办法。

---

## 6. F：切换与回退

### 6.1 `ensure_schema` 的 advisory lock：混跑期的冲突形态

**现状**（`postgres.rs:151-181`）：

```rust
const SCHEMA_LOCK_KEY: i64 = 0x0A6E_7653_4348_4D41;   // = 751668287800626497
```

- 在**池里 pin 一条连接**上 `pg_advisory_lock` → `sqlx::raw_sql(SCHEMA_DDL)` → `pg_advisory_unlock`。
- `SCHEMA_DDL`（`postgres.rs:37-58`）是多语句、走 simple query 协议 ⇒ **一个隐式事务**。
- 触发时机：**每个 node 每次启动**，`PostgresPausedSandboxRegistry::connect` 里无条件跑（`:127`）。
- 注释里记录的历史事故：两个 node 同时首启，`CREATE TABLE IF NOT EXISTS` 在 `pg_type_typname_nsp_index` 上撞唯一键（`:139-145`）。

**混跑期（一部分 node 跑 `postgres`、controller 跑 migration）的冲突形态，按危险度排序：**

1. 🔴 **node 会无条件"回滚" CHECK 约束。**
   DDL 里这一对是无条件的（`postgres.rs:51-53`）：
   ```sql
   ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_state_check;
   ALTER TABLE paused_sandboxes ADD CONSTRAINT paused_sandboxes_state_check
       CHECK (state IN ('publishing','paused','resuming','local_only','running'));
   ```
   controller 的 migration 若给 CHECK 加了新状态（阶段 3 引 ExecutionID 时很可能要加），
   下一个跑 `postgres` 后端的 node 一启动就把它**改回旧集合**。
   如果此时库里已经有新状态的行，`ADD CONSTRAINT` 会因
   `check constraint "…" is violated by some row` **失败** ⇒ `ensure_schema` 返回 `Err`
   ⇒ `build_paused_registry` 的 `?` ⇒ **node 起不来，CrashLoopBackOff**。
   这是最尖锐的一种：故障表现是"某台 node 永远起不来"，而根因在另一个进程的迁移里。

2. ⚠️ **锁不互斥。** node 用 `pg_advisory_lock(751668287800626497)`；
   golang-migrate 用 `pg_advisory_lock(hash(database_name))`，goose 用 `schema_migrations` 表 + 自己的锁。
   **两边不会互相阻塞**。虽然 PG 自己的 `ACCESS EXCLUSIVE` 表锁会把 DDL 串起来，
   但串起来的顺序是随机的 —— 第 1 条的"回滚"就是这么发生的。

3. ⚠️ **node 启动被 migration 阻塞。** controller 跑长事务 DDL（比如加索引不带 `CONCURRENTLY`）
   时持 `ACCESS EXCLUSIVE`，node 的 `ensure_schema` 会一直等。
   `PgPoolOptions` 没设 `acquire_timeout` 之外的语句超时，也没设 `statement_timeout`
   ⇒ node 可能**无限期卡在启动**（不是崩，是不动），比崩更难查。

**规避办法（按推荐度）：**

- **A（推荐）：混跑期不存在。** 方案 §4 阶段 2 已经写了"建议在无活沙箱窗口切"。
  把它升级成硬要求：**所有 node 同时切到 `central`，且切换前 controller 不跑任何 DDL**。
  切完之后 controller 才接管 schema。这样第 1/2/3 条全部不成立。
- **B：controller 的 migration 必须与 node 的 DDL 幂等等价。** 也就是 controller 的
  第一个 migration 就是 `SCHEMA_DDL` 逐字复制（Go 侧已有这个先例：
  `postgres_integration_test.go:12-37` 就是逐字复制的），且**在阶段 2 期间不加任何新列/新状态**。
  这条能容忍混跑，但把 schema 演进冻结到阶段 3。
- **C：controller 复用同一个 advisory lock key。** 便宜、能解决第 2/3 条，
  但解决不了第 1 条（回滚 CHECK 是逻辑问题不是并发问题）。**必须与 B 合用**。
- **D：让 `Central` 后端跳过 `ensure_schema`。** 这是自然结果（Central 后端不连 PG），
  但它只保证"切过去的 node 不再跑 DDL"，**保护不了还没切的 node**。

我的判断：**A + C**。A 是主手段，C 是廉价保险（万一有 node 没切干净）。

### 6.2 配置回退（`backend = "postgres"`）需要重启吗

**需要，而且比想象的贵。**

1. **无热切换**：`ConfigManager` 是 `OnceLock<ConfigManager>`（`cfg.rs:1237-1242`），
   `init_global` 只在 `main` 开头跑一次（`server.rs:61-66`），全仓无 watcher / reload / notify
   （`grep -rn "fn reload\|watcher\|notify::" src/cfg*` 零命中）。
   `Arc<dyn PausedSandboxRegistry>` 在 `PausedSandboxCoordinator` 里是 immutable 字段，
   没有 `ArcSwap` 之类的换手机制。**改后端 = 重启进程。**

2. 🔴 **`backend` 没有 env 覆盖**（§1.2）。只能改 TOML。
   TOML 来自 ConfigMap `agentenv-k8s-config` 的 `agentenv.toml`
   （`deploy/k8s/base/agentenv-daemonset.yaml:37-38` `AENV_CONFIG_PATH=/workspace/config/agentenv.toml`）。

3. 🔴 **仓内 manifests 里根本没有 `paused_registry` 配置**：
   `grep -rn paused_registry deploy/ config/` **零命中**。
   而 `deploy/k8s/run.sh:30` 每次都把 `config/default.toml` **覆盖**成 ConfigMap 的内容，
   `config/default.toml` 里没有 `[orchestrator.paused_registry]` 段
   （只有 `[orchestrator]` 的三个别的键，`:196-204`）。
   R3 §2.2 观察到的 dev 集群那份带 `backend = "postgres"` 的 ConfigMap，
   是**手工 patch 上去的**，`AENV_PAUSED_REGISTRY_DSN` 的 secretKeyRef 也不在仓内 DaemonSet 里。
   👉 **回退动作今天等于"手工编辑集群 ConfigMap"**，没有 git 记录、没有 review、
   而且下一次 `deploy/k8s/run.sh apply` 会把它**冲掉**。

4. **重启本身很慢**：DaemonSet 是 `maxSurge: 0 / maxUnavailable: 1`（`agentenv-daemonset.yaml:20-23`）
   + `terminationGracePeriodSeconds: 3600`（`:29`）。逐台滚，每台最坏等一小时排水。

**建议**：阶段 2 的**第一个 PR** 就把 `[orchestrator.paused_registry]` 段
（含 `backend` / `max_connections` / `reconcile_interval_secs` / `lease_ttl_secs`）
写进 `config/default.toml`，把 `AENV_PAUSED_REGISTRY_DSN` 的 secretKeyRef 写进
`deploy/k8s/base/agentenv-daemonset.yaml`，**并给 `backend` 补一个 env 覆盖**
（`#[config(env = "AENV_PAUSED_REGISTRY_BACKEND")]`）。
有了 env 覆盖，回退就能靠 `kubectl set env daemonset/agentenv-node` 一条命令完成，
不用碰 ConfigMap。这是整个阶段 2 里性价比最高的一个改动。

### 6.3 启动顺序依赖

#### 今天（`backend = "postgres"`，PG 没起来）

```
build_paused_registry (mod.rs:304-328)
  └─ PostgresPausedSandboxRegistry::connect (postgres.rs:114-136)
       ├─ PgPoolOptions::new().max_connections(n).connect(dsn).await   ← 🔴 eager，会实际建连
       │     失败 ⇒ anyhow!("connect to paused sandbox registry database: {e}")
       └─ Self::ensure_schema(&pool).await?                            ← 🔴 acquire + DDL，阻塞
             失败 ⇒ anyhow!("ensure paused sandbox registry schema: {e}")
```

`server.rs:148` 是 `.await?`，`main` 返回 `Err` ⇒ **进程退出非零 ⇒ Pod CrashLoopBackOff**。
**这是 fail-closed，而且是好的**：node 不会带着"半聋的登记表"上线。
代价是 PG 与 node 变成同级依赖 —— PG 挂了所有 node 都起不来。

#### Central 后端如果照抄 `connect_lazy`（`reporter.rs:250`）

`Endpoint::from_shared(...).connect_lazy()` **不会失败**（只有 URI 解析会）。
于是 node **启动成功**，然后立刻按顺序跑（`server.rs:176-178`，**在 listener 打开之前**）：

```
api_impl.release_stale_node_holdings()   ← ① 全部失败 ⇒ warn，且【永不重试】
api_impl.renew_paused_leases()           ← ② 失败 ⇒ warn，30s 后重试
api_impl.reconcile_local_records()       ← ③ get_many 失败 ⇒ 停手，30s 后重试
```

②③ 会自愈，**① 不会**。`release_stale_node_holdings` 的注释（`paused_recovery.rs:391-395`）
说得很清楚：**只能在启动期、listener 打开前调**，因为它按 node 身份释放行，
一旦本进程开始跑沙箱，它要释放的就是自己的行。
所以它**一辈子只有一次机会**。失败后的注释（`:430-437`）：

> Nothing else releases these rows, so the sandboxes stay stranded until a later start succeeds.

也就是：**scheduler 晚起 30 秒，这台 node 上前一个进程留下的 `running` / `resuming` 行就永久卡死到下次重启。**
而 `claim_for_resume` 对 `running` / `resuming` **永不可抢**（`postgres.rs:516` 起的论证，R2 §2.6），
所以那些沙箱的 resume 会一直 409，直到有人再滚一次这台 node。

今天这个问题**不存在**，因为 PG 不可达时进程压根起不来，`release_stale_node_holdings` 也就没有"失败过"。

**Kubernetes 侧没有任何顺序保证**：DaemonSet `agentenv-node` 与 Deployment `agentenv-scheduler`
互相独立，无 initContainer 探测、无 `dependsOn`。
scheduler `replicas: 1`（`scheduler-deployment.yaml:8`），滚动升级期间必然有空窗。

**建议（三选一，我推荐 3）：**

1. **eager 连**：`Endpoint::connect().await?`，保持今天的 fail-closed。
   缺点：把 node 启动绑死在 scheduler 上，且 scheduler 也在 k8s 里滚 —— 会造成级联 CrashLoop。
2. **lazy + 启动期探活**：`connect_lazy` 之后先打一发轻量 RPC（比如
   `GetSandboxes` 空列表），失败就重试 N 次 / 直到超时，仍失败则**退出进程**。
   保住 fail-closed，又不把 channel 建立绑死。
3. **lazy + 只把 ① 变成可重试**：`connect_lazy`，但把 `release_stale_node_holdings`
   从"一次性"改成"带围栏的可重试" —— 用一个只在**本进程 mark_running 过任何沙箱之前**
   有效的窗口（`PausedSandboxCoordinator.running_registrations` 为空即可判定），
   在这个窗口内每 5s 重试直到成功或窗口关闭。
   这条同时修掉了今天就存在的一个弱点（PG 短暂抖动也会让 ① 永久失败），
   且不引入启动顺序依赖。

---

## 7. 三个最大技术风险

### 风险 1：`arbitrate_resume` 的 fail-open 是四条护栏的盲区，而 scheduler 每次发版都会触发它

**证据链：**

| # | 事实 | 位置 |
|---|---|---|
| 1 | `claim_for_resume` 报错 ⇒ `ResumeArbitration::Proceed`（放行，不做任何集群检查） | `src/api/impls/paused_recovery.rs:173-181` |
| 2 | `Proceed` ⇒ 直接 `orchestrator.resume_sandbox`，跳过 `Blocked`/`NotReady` 两个 409 | `src/api/impls/sandbox.rs:1262-1280` |
| 3 | 同一次故障也会让 `discard_if_superseded` 静默不删（`superseded_by_cluster` 返回 `Err(())`） | `src/api/impls/paused_recovery.rs:778-785` + `:732-734` |
| 4 | scheduler `replicas: 1`，无 PDB、无 `maxSurge` | `deploy/k8s/base/scheduler-deployment.yaml:8` |
| 5 | 方案 §3 四条护栏全部针对"停手 vs 当作空"这一个方向；`arbitrate_resume` 是**反方向**（放行），没有一条覆盖 | `docs/proposals/2026-08-19-agentenv-control-plane-refactor.md:144-235` |

**危害**：`running`/`resuming` 永不可抢这条不变式，在实现层的最后一道闸就是 `claim_for_resume`
返回 `Conflict`。registry 一不可达，这道闸整个消失，两道防线（3 与 1）**同源同时失效**。
后果是双活 —— 两台 VM 从同一快照分叉、各写各的 rootfs 层、gateway 在两者间抖。
这正是 `postgres.rs:69-79` 那段 🔴 注释花整整十行论证要避免的东西。

**为什么阶段 2 才变严重**：今天 `claim_for_resume` 报错 = node 到集群内 PG 的连接故障
（同集群、`max_connections=8`、连接池常驻），实际发生率极低。
阶段 2 之后它 = node 到 scheduler 的 gRPC 故障，而 scheduler 单副本滚动升级、
镜像拉取、OOM、节点驱逐**都会**制造窗口。

**我的建议**：把"registry 不可达 ⇒ resume 返回 503 而不是 Proceed"加进阶段 2 的验收条件
（等价于给护栏加第 3.5 条）。代价是 registry 抖动时 resume 会短暂失败 ——
但那是**可重试的失败**，而双活不可逆。
若担心影响面，可以退一步：只在**本地有 paused 记录且该记录 `ClusterRegistration::As(_)`**
（即曾被登记过）时改成 503，其余保持 Proceed —— 这正好覆盖危险组合，不影响从未上过集群的沙箱。

---

### 风险 2：启动语义从 fail-closed 翻成 fail-open，而 `release_stale_node_holdings` 只有一次机会

**证据链：**

| # | 事实 | 位置 |
|---|---|---|
| 1 | 今天：`PgPoolOptions::connect` 是 eager 的，`ensure_schema` 也阻塞；任一失败 ⇒ `?` ⇒ 进程退出 | `postgres.rs:121-127` + `mod.rs:318-325` + `server.rs:148` |
| 2 | 现有 gRPC 客户端一律 `connect_lazy()`，端点不可达**不会**让构造失败 | `reporter.rs:246-251`、`p2p/discovery/scheduler.rs:154-155` |
| 3 | 启动期三连在 listener 打开前跑，顺序 load-bearing | `server.rs:170-178` |
| 4 | `release_stale_node_holdings` **只能在启动期调**，注释明写"call it a second later and it hands live sandboxes to whoever resumes them next" | `paused_recovery.rs:391-395` |
| 5 | 它失败后**永不重试**：`"…stay unclaimable until a later start"` | `paused_recovery.rs:430-437` |
| 6 | 而 `claim_for_resume` 对 `running`/`resuming` 永不可抢，所以这些行**没有任何别的出路** | `postgres.rs:516` 起、R2 §2.6 |
| 7 | DaemonSet 与 scheduler Deployment 无启动顺序约束；scheduler `replicas: 1` | `agentenv-daemonset.yaml` / `scheduler-deployment.yaml:8` |

**危害**：node 重启时 scheduler 恰好不可达（发版、拉镜像、被驱逐）⇒
上一个进程持有的所有 `running` / `resuming` 行**永久卡死**，对应沙箱的 resume 一直 409，
直到有人再手工滚一次那台 node。而且**没有告警**——只有一条 `warn!`，
在 `kubectl logs` 里活不过一次重启。

这是一个**纯粹由架构变更引入的新故障模式**：今天不存在，因为 PG 不可达时进程根本起不来。

**我的建议**：见 §6.3 的方案 3 —— 把 ① 改成"带围栏的可重试"，
围栏条件用 `running_registrations` 为空（`paused_coordinator.rs` 里已有这个结构）。
这个改动**独立于阶段 2**，可以先做、先上线、先观察，等于把风险前置消化掉。
另外补一条 metric：`agentenv_paused_registry_stale_release_failed_total`。

---

### 风险 3：metadata JSONB 的原样往返 + schema owner 交接，两件事都没有"部分正确"

#### 3a. metadata 往返

**证据链：**

| # | 事实 | 位置 |
|---|---|---|
| 1 | Go 侧读路径**从不 select `metadata` 列**（`selectColumns` 里没有它，注释还说自己"deliberately wider"却唯独漏了 metadata） | `services/scheduler/internal/registry/postgres.go:29-41` |
| 2 | 所以阶段 2 是 Go 侧**第一次**碰这一列 | — |
| 3 | `SandboxMetadata` 无 `deny_unknown_fields`、4 个 `#[serde(default)]`、2 个 `skip_serializing_if`（字段数不固定）、2 个 camelCase 键混在全 snake_case 里 | `metadata.rs:30-64`、`image_configs.rs:8,11` |
| 4 | 读侧解码失败 ⇒ `InvalidRecord`，而 `get`/`get_many`/`claim_for_resume` **共用同一个 `decode`** | `postgres.rs:204-267`（`:220-228` 是 metadata 那段） |
| 5 | 10 个字段缺失即报错（非 Option 且无 default）：`id`/`snapshot_id`/`state`/`created_at`/`timeout_action`/`auto_resume`/`runtime_versions`/`resources`/`context`/`network_policy` | §3.2 |
| 6 | 仓内**没有任何 metadata JSON fixture**（全仓 grep `secs_since_epoch` / `timeout_action` 零命中），两侧没有共同的真值来源 | — |

**危害**：Go 侧只要把 metadata 过一遍 Go struct 再写回，就会静默丢掉它不认识的字段
（没有 `deny_unknown_fields` 保护，丢的时候不报错）。丢掉的若是那 10 个必需字段之一，
那台沙箱**在所有读路径上永久变成 `InvalidRecord`** —— 既读不出，也认领不了，只能人工改库。
而这个错误**只在下一次 resume 时才暴露**，可能是几天以后。

**好消息（也是本次调研最有用的一条）**：`entry.metadata` 在整个 Rust 侧
**只有一个消费方** —— `paused_recovery.rs:312` `restore_request(&entry.metadata, snapshot, timeout)`，
即跨节点重建路径，它的 entry 来自 `ResumeArbitration::Held`（即 `claim_for_resume`）。
`get_many` 的两个消费方（`supersession` / `running_supersession`，`:850-945`）
**只看 `state` / `origin_node_id` / `claimed_by_node_id` / `generation`，完全不碰 metadata**。

👉 **所以 metadata 只需要过两条 RPC**：
- `TransitionSandbox(begin_pause)`：node → controller，**写方向**
- `AcquireSandbox`：controller → node，**读方向，且只有一行**

`GetSandboxes`（批量）**完全不需要带 metadata**。这同时消掉了 tonic 4 MiB 解码上限的风险，
并把"字节级往返"的验证面缩到两个点。

**我的建议**：
- proto 里用 `bytes metadata_json`（原始 JSON 字节），**不要** `google.protobuf.Struct`
  （数字塌成 double、重排、归一化）。
- Go 侧全程 `json.RawMessage`，直接 bind 成 JSONB，**不定义 Go struct**。
- 补 §5.4 的 golden fixture 测试，两侧各一个。

#### 3b. schema owner 交接

**证据链：**

| # | 事实 | 位置 |
|---|---|---|
| 1 | `SCHEMA_DDL` 里 `DROP CONSTRAINT IF EXISTS … / ADD CONSTRAINT …` 是**无条件**的 | `postgres.rs:51-53` |
| 2 | 每个跑 `postgres` 后端的 node **每次启动**都跑它 | `postgres.rs:127`（`connect` 里）|
| 3 | advisory lock key `0x0A6E_7653_4348_4D41` (= 751668287800626497) 只在 node 之间互斥，与任何 Go migration 工具的 key 都不同 | `postgres.rs:153` |
| 4 | `ensure_schema` 失败 ⇒ `?` ⇒ 进程退出 | `postgres.rs:179` + `mod.rs:325` + `server.rs:148` |
| 5 | 方案已经写了"建议在无活沙箱窗口切"，但措辞是**建议**不是硬要求 | 方案 `:335-337` |

**危害**：controller 一旦给 CHECK 加了新状态，且库里已有该状态的行，
下一台跑 `postgres` 后端的 node 启动时 `ADD CONSTRAINT` 会因
`check constraint is violated by some row` 失败 ⇒ **那台 node 永远起不来**，
而根因在另一个进程的 migration 里。

**我的建议**：把"所有 node 同时切、切换前 controller 不跑 DDL"从建议升级成**硬门禁**，
并让 controller 复用同一个 advisory lock key 作为廉价保险（§6.1 的 A + C）。
另外，阶段 2 期间 controller 的 migration **第一版就是 `SCHEMA_DDL` 逐字复制、不加任何新列**
—— Go 侧已有逐字复制的先例可循（`postgres_integration_test.go:12-37`）。

---

## 8. 实施清单（按依赖顺序，供排期）

| # | 动作 | 文件 | 依赖 |
|---|---|---|---|
| 1 | `[orchestrator.paused_registry]` 段写进 `config/default.toml`；`AENV_PAUSED_REGISTRY_DSN` secretKeyRef 写进 DaemonSet；给 `backend` 加 `#[config(env = "AENV_PAUSED_REGISTRY_BACKEND")]` | `config/default.toml`、`deploy/k8s/base/agentenv-daemonset.yaml`、`src/cfg.rs:394-395` | — |
| 2 | CI 加带 `services: postgres` 的 job，跑 `cargo test -p agentenv --test paused_registry` 并设 `AENV_PAUSED_REGISTRY_TEST_REQUIRED=1`；Go 侧补 `SCHEDULER_REGISTRY_TEST_REQUIRED` | `.github/workflows/ci.yml`、`services-ci.yml`、`postgres_integration_test.go:65-68` | — |
| 3 | `release_stale_node_holdings` 改成带围栏可重试 + metric | `src/api/impls/paused_recovery.rs:411-443` | — |
| 4 | metadata golden fixture（Rust dump + Go 读） | 新增 | — |
| 5 | proto 加 `PausedRegistryService`（5 个 RPC，`bytes metadata_json`，`GetSandboxes` 不带 metadata） | `services/api/proto/scheduler.proto` | 1 |
| 6 | `build.rs` 翻 `build_server(true)`（为 L1 传输层单测） | `build.rs:9` | 5 |
| 7 | `PausedRegistryBackendKind::Central` + `build_paused_registry` 分支（签名加 `&ClusterConfig`） | `src/cfg.rs:381-391`、`src/orchestrator/paused_registry/mod.rs:298-329`、`src/bin/server.rs:148` | 5 |
| 8 | `central.rs` 后端实现 + L1 单测（状态码映射、全有或全无、deadline） | 新增 `src/orchestrator/paused_registry/central.rs` | 6,7 |
| 9 | resume fail-open 收口（风险 1） | `src/api/impls/paused_recovery.rs:173-181` | 8 |
| 10 | `complete_pause` 失败后先 `get` 再决定删不删快照（§4.5） | `src/api/impls/paused_coordinator.rs:183-197` | 8 |
| 11 | controller 侧 Go 实现 + L2 语义契约测（R2 §5 移植） | `services/scheduler/internal/registry/` | 5 |
| 12 | schema owner 交接（A+C）+ 切换窗口演练 | `deploy/` | 11 |

---

## 附：本文没有覆盖、需要另查的

- controller 侧的 Go 实现细节（属于 R2 的语义规格 + 阶段 2 的实施）
- 护栏 §3.2 的 grace 期具体算法（需要 controller 侧设计，本文只指出节点侧无法感知）
- 护栏 §3.3 丢弃熔断的阈值取值（需要 R1 读路径的实测分布）
- 阶段 3 的 ExecutionID 契约（闸门 B）
