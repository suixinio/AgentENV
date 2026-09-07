# 放置资源打分（shadow 先行）— 实现规格

**状态**：已通过对抗审查（GPT-5.6-Sol，2026-08-30，零 BLOCKING），可进入实现
**仓库基线**：AgentENV `55a3b6a`
**参照实现**：e2b-dev/infra `fdc33599b983095284f97e47852a02bdf2a4a73f`（本地 `/home/debian/e2b-infra`）

> 本文是实现规格。§4 中的所有数值（夹具比值、变异期望、wire 字节）**是规范值**，
> 已独立验算，实现必须逐字满足；不得因"看起来差不多"而改写。

---

## 1. 目标与范围

AgentENV 中**需要策略决策的**放置路径当前**完全不读节点资源**：候选经 `filter_unschedulable`
过滤后由 `RoundRobinStrategy` 轮询选出。本期引入资源打分，但**不让它决定放置**——它**仅在真实
`RoundRobinStrategy` 被调用之后**以 shadow 形式执行，只产出全新指标。
（`prefer_node_id` 命中的路径根本不执行策略，因此也不执行 shadow，见 K-2。）
翻默认是另一个方案（§7）。

| 交付 | 内容 |
|---|---|
| **S1 shadow 打分** | 分类 + 投放后压力 + 无放回采样，纯计算，不影响返回值 |
| **S2 `NewSandboxHint` 补资源字段** | proto additive 变更，让请求大小对打分可见 |
| **S3 门控调查两项** | P2 沙箱指标（§6.1）、P3 overlaybd 检查工具（§6.2），只出结论不出实现 |

**明确不做**：`Strategy` trait、策略选择配置、任何可启用 best-of-K 的开关。本期唯一新增配置是
`shadow_k`。理由见 §7。

---

## 2. 不可违反的约束

每条都附证据；违反其中任何一条，改动即为不正确，而不仅是不优雅。

**K-1 真实放置必须逐字节不变。** shadow 的任何结果都不得进入返回路径，不得推进第二个轮询游标，
不得 panic，不得引入 IO。

**K-2 `prefer_node_id` 命中时策略根本不执行**（`src/binding_store/lookup.rs:112-126`），
因此该路径**不执行 shadow**。把 shadow 提到 prefer 之前会把有意的 origin affinity 记成策略分歧，
污染未来的 cutover 证据。

**K-3 两类流量共用 `select_node`。** `Schedule`（`src/node_registry/grpc_service.rs:766-771`）与
paused fallback（`src/binding_store/lookup.rs:423-444`，后者 `hint=None`）。指标必须按**内部闭集
`ShadowSource`** 分流，**不得**由 `hint.is_some()` 推断。

**K-4 freshness 按每条记录自己的 TTL 判。** `ObservedNodeRecord` 自带 `report_ttl` 与 **API 接收侧**
`last_seen`（`src/node_registry/registry.rs:260-270`），现有派生用记录自身 TTL、为零才回退默认值
（`:672-680`），Redis 跨副本同时保存两者（`:278-423`）。硬编码 `DEFAULT_OBSERVED_REPORT_TTL` 会在
非默认 TTL 下把 registry 已判过期的节点算成健康。`peek_observed` 明确**不**做派生（`:224-229`）。

**K-5 墙钟可能倒退。** 跨副本可能 `last_seen > now`；任何 `duration_since().unwrap()` 都会违反 K-1。

**K-6 指标宏不是零成本。** `counter!/histogram!` 每次调用走 recorder 注册，Prometheus recorder 的
`register_*` 走 `get_or_create_*` 且内部持 `RwLock`
（`metrics-exporter-prometheus-0.18.3/src/recorder.rs:382-394`、`:7,47`）。逐 candidate 发指标会把锁
与注册放大到 O(N)。

**K-7 内存单位不一致。** 心跳 `allocated_memory_bytes` 是**字节**，请求侧
`SandboxResources::memory_mib` 是 **MiB**（`src/orchestrator/metrics.rs:119-127`、
`src/node_client/placement.rs:93-100`）。直接相加会把 512 MiB 当成 512 字节。

**K-8 心跳盲窗，且没有 pending 记账。** `Schedule` 只选节点（`grpc_service.rs:761-792`），
create 在之后（`src/node_client/stub.rs:1089-1101`），心跳默认 5 秒（`src/cfg.rs:972-979`），
api 多副本，进程内计数不完整。**这是本期不翻默认的根本原因**：E2B 的 best-of-K 依赖
`placement.go:129-151` 的 `StartPlacing` + 成功后 optimistic update，我们没有等价物。

**K-9 发布契约（MUST）。** `deploy/k8s/base/agentenv-api-deployment.yaml:88-107`：api 与 node 必须
**同 commit 发布、滚到同一 tag**——`node.proto` 有 11 个字段的载荷是 Rust `serde` 编码，由一个全局
`SERIALIZED_VALUE_SCHEMA_VERSION` 守卫，skew 会让所有跨半 RPC 同时失败。且
`imagePullPolicy: IfNotPresent`（`:108-109`），可变 tag 会让节点继续跑缓存镜像。

### 2.1 两项既存事实（不是本期引入，也不在本期修）

**E-1 恢复路径发生两次独立放置。** `lookup.rs:423-444` 偏好 origin；跨副本无本地 handle 时从
catalog 重建（`resume_surface.rs:780-796`、`paused_recovery.rs:495-536`、`factory.rs:110-195`），
随后 `stub.rs:1064-1088` **再调用一次 `place_new`**，可能选中另一台；而 wake 响应返回**第一次**的
选择（`resume_surface.rs:827-879`），gateway 直接信任它转发首请求
（`services/gateway/internal/resume/client.go:148-165`、`server.go:422-437`）。
本期不修，理由与处理见 §4.6。

**E-2 集群无法证明排序质量。** pve-mf 两台均 8 CPU，内存 capacity 不同（实查 16375592Ki /
15886248Ki）；`scripts/tests/e2e/suites/10_cluster_deployment.sh:49-57` 的放置/分布断言在当前
split 部署下被跳过。**集群回归只能证明"没有弄坏"**，排序正确性只能由 §4 的单元测试证明。

---

## 3. 设计

### 3.1 模块

```
src/node_registry/placement/mod.rs     // shadow 入口、ShadowSource、指标句柄
src/node_registry/placement/score.rs   // 分类与压力，纯函数
src/node_registry/placement/sample.rs  // 无放回采样，RNG 可注入
```

`src/node_registry/strategy.rs` **不动、不迁移、不留 shim**；`ScheduleDeps.strategy`
（`lookup.rs:68-75`）与 `grpc_service.rs:173-198` 继续持 `RoundRobinStrategy` /
`Arc<RoundRobinStrategy>` 的**具体类型**，共享游标的不变式（两条路径必须共用同一实例）原样保留。
best-of-K 是独立、无副作用的函数，只被 shadow 调用。

### 3.2 候选载体与 freshness（全程一次 registry 读取）

候选构造现在每节点读一次 snapshot（`lookup.rs:99-109`），`RichNode` 只存 `node`/`snapshot`
（`src/node_registry/types.rs:40-44`），shadow 阶段拿不到 freshness。因此 registry 新增：

```rust
fn peek_observed_with_freshness(&self, node_id: &str, now: SystemTime)
    -> Option<(NodeSnapshot, SnapshotFreshness)>;   // Fresh | Stale | ClockSkew
```

- TTL 判定**复用 registry 私有 helper**，与 `derive_observed_node_view`（`registry.rs:672-680`）同源，
  避免两套规则漂移（K-4）。
- `last_seen > now` → `ClockSkew`，绝不 `unwrap`（K-5）。

候选以并列 sidecar 承载：

```rust
struct ShadowCandidate { rich: RichNode, freshness: Option<SnapshotFreshness> }
```

- 🔴 **不变式：`freshness.is_none()` 当且仅当 `rich.snapshot.is_none()`。**
- discovery 已发现但尚未心跳的节点，accessor 返回 `None`，它**必须继续以 `RichNode { snapshot: None }`
  进入未改动的过滤与轮询**（现有代码刻意保留这类节点，`filter.rs:23-28`、`types.rs:34-50`），
  shadow 侧分类为 `Unknown{no_snapshot}`。
- 🔴 **禁止**把派生出的 `UNHEALTHY` 写回 `RichNode` 再进 `filter_unschedulable`——那会改变真实候选集，
  违反 K-1。
- 🔴 **禁止**在 shadow 阶段二次调用 registry（每节点会多一次 `RwLock` 读）。
- 每次 selection 只捕获**一个 `now`**，分类与 freshness 共用它。

给 `NodeRegistry` trait 加方法会波及测试替身，**必须由编译器确认全集**，已知三处：
`src/orchestrator/paused_registry/mod.rs:812-875`、
`crates/aenv-api/src/orchestrator/paused_registry/mod.rs:42-98`、
`crates/aenv-api/src/orchestrator/paused_registry/postgres/contract.rs:1516-1578`。

### 3.3 压力（`score.rs`，纯函数、total、infallible）

```
req_mem_bytes = memory_mib.checked_mul(1_048_576)?                                   // K-7
after_cpu     = (allocated_cpu.checked_add(requested_cpu)?) as f64 / cpu_count as f64
after_mem     = (allocated_memory_bytes.checked_add(req_mem_bytes)?) as f64
                / memory_total_bytes as f64
pressure      = max(after_cpu, after_mem)        // 瓶颈维度，越小越优
```

`Classification::{ Scored { pressure }, Unknown { reason } }`，`reason` 是闭集：
`no_snapshot | stale | clock_skew | zero_denominator | overflow`。

- **不使用 starting-count 惩罚**：`Creating`/`Resuming` 已同时计入 `sandbox_starting_count`
  **和**完整的 `allocated_cpu`/`allocated_memory_bytes`（`src/orchestrator/metrics.rs:77-127`），
  再按 starting 数扣分是重复计费，且会对 1-vCPU 与 16-vCPU 的 start 给出相同惩罚。
- **`paused_allocated_*` 不计入**：paused 沙箱不占 CPU/内存，只占磁盘
  （`services/api/proto/scheduler.proto:251-255`）。磁盘维度本期不打分（`disks` 是 mount 点数组，
  语义需单独定义）。
- **`pressure > 1.0` 合法且仍是 `Scored`**：过量分配不是错误状态，本期不做硬 fit。
- 它衡量的是**声明的资源压力**，不是实际 `cpu_percent` / RSS。
- 分母为 0 → `Unknown{zero_denominator}`；任一 checked 运算失败 → `Unknown{overflow}`。

### 3.4 请求资源的 total 映射（闭集，无未定义分支）

| 情形 | requested_cpu | requested_mem_bytes | 记 missing |
|---|---|---|---|
| `PausedLookup`（不看 hint） | 0 | 0 | 否 |
| `Schedule` + `hint = None` | 0 | 0 | 是 |
| `Schedule` + `Some(hint { kind: None })` | 0 | 0 | 是 |
| `Schedule` + `NewSandbox{cpu:None, mem:None}` | 0 | 0 | 是 |
| `Schedule` + `NewSandbox` 仅一个字段存在（CPU-only / memory-only 两向） | 存在者取值，缺失者 0 | 同左 | 是 |
| `Schedule` + `NewSandbox{cpu:Some, mem:Some}` | 取值 | `mib * 1_048_576` | 否 |
| `Schedule` + `NewColdSandbox{cpu_count, memory_mb}` | `cpu_count` | `memory_mb * 1_048_576` | 否 |

`Some(hint { kind: None })` 是可达形状：`ScheduleRequest.hint` 是 `Option<ScheduleRequestHint>`，
其内部 `oneof kind` **自身也是 `Option`**（`scheduler.proto:39-69`）。空 hint、以及滚动期间只携带
未来未知 oneof tag 的消息都会落在这里。

- **`Some(0)` 是 present**，按 0 参与计算，**不计 missing**——这正是采用 `optional` presence 的原因。
- 🔴 **"missing 恒为 0"只对更新后的 `NativeNodePlacement::place_new` producer 测试成立**，
  不对所有 `Schedule` 流量成立：外部/legacy 调用方可以合法地不带字段。
- `NewColdSandboxHint.memory_mb` 的单位是一条**契约裁决**：仓库内没有任何 producer 构造过该 hint
  （生产的 `NativeNodePlacement` 始终构造 `NewSandbox`，`native_placement.rs:261-280`），而 HTTP 冷
  请求把 `memoryMB` 解释为 MiB（`src/api/impls/sandbox.rs:326-343`：
  `let memory_mib = body.memory_mb.unwrap_or(default_mem)` → `SandboxResources { memory_mib }`）。
  故规定其值按 MiB 解释，换算与热 hint 一致。冷 hint 的历史字段名本期不改。

### 3.5 采样与配置

**配置 ABI（钉死，不留给实现者决定）**：

| 项 | 值 |
|---|---|
| 字段路径 | `[cluster].placement_shadow_k` |
| 环境变量 | `AENV_CLUSTER_PLACEMENT_SHADOW_K` |
| 类型 | `u32` |
| 默认值 | `3` |
| 校验 | `0` 在加载时即拒绝（附默认值测试与拒绝测试） |

**采样规则**：

- **无放回**；生产用 per-call 线程 RNG（不共享、不加锁），测试注入 scripted RNG。
- 样本内有 `Scored` → 取 `pressure` 最小者；**并列时取采样随机顺序在先者**
  （按 node id 排序会让同构/新集群的 shadow 永远指向同一台，产出无意义的 agreement 证据）。
- 样本全为 `Unknown` → 取采样顺序第一个。全集群 all-unknown 时仍产出 shadow 选择。
- 已知限定：**`N <= K` 时 best-of-K 就是全局最优**，此时它不缓解羊群；缓解羊群需 `N > K` 或
  pending 记账（K-8）。

### 3.6 shadow 执行与指标

**触发点**：真实轮询策略实际被调用之后（K-2 的早返回路径不执行）。签名为
`select_node(..., source: ShadowSource)`，`source` 由两个真实调用点分别传入，**不存进共享的
`ScheduleDeps`**：

| 来源 | 调用点 |
|---|---|
| `ShadowSource::Schedule` | `src/node_registry/grpc_service.rs:766-771` |
| `ShadowSource::PausedLookup` | `src/binding_store/lookup.rs:428` |

**指标**（全新名字；现有 `strategy` label 与 series 一律不动）：

| 指标 | 标签 | 基数 |
|---|---|---|
| `agentenv_api_placement_shadow_agreement_total` | `source`(2) × `agrees`(2) | 4 |
| `agentenv_api_placement_shadow_classification_total` | `source`(2) × `class`(6) | 12 |
| `agentenv_api_placement_shadow_pressure_spread` | `source`(2) | 2 |
| `agentenv_api_placement_missing_request_resources_total` | `source`(2) | 2 |

`class` 为 6 值闭集：`scored` + `no_snapshot` / `stale` / `clock_skew` / `zero_denominator` / `overflow`。

**热路径纪律（K-6）**：

- 全部指标句柄在**服务构造期**创建并缓存（`grpc_service.rs:173-198` 附近），调用路径不再走
  `counter!/histogram!` 宏；
- 分类计数先在栈上聚合，**每类每次调用最多一次 increment**；
- shadow 函数 total/infallible：不用 `?`、不 `unwrap`、**不按原始 K 预分配**；
- `pressure_spread` **仅当样本内 ≥2 个 `Scored`** 才记录；
- 交付 N=2 / 100 / 1000 三档 shadow on/off 的 microbenchmark，或写明可接受的 P99 增量上限并给出实测。

### 3.7 proto 变更（S2）

- `NewSandboxHint` 增 `optional uint32 cpu_count = 2;`、`optional uint64 memory_mib = 3;`
  （`metadata` 占 field 1，`scheduler.proto:61-65`）。
- 采用 proto3 显式 presence，使"缺失"与"显式 0"可区分（§3.4 依赖该区分）。
  prost 0.14.3 对 proto3 optional scalar 生成 `Option<T>`
  （`prost-build-0.14.3/src/code_generator.rs:417-538,1083-1095`）；Go 侧 generator 为
  protoc-gen-go v1.36.11 / protoc 3.21.12（`services/api/proto/scheduler.pb.go:1-5`），生成指针 presence。
- 命名用 `memory_mib`（来源就是 `SandboxResources::memory_mib`）。
- `NativeNodePlacement::place_new` 填充已收到的 `_resources`，不再丢弃
  （`src/node_client/native_placement.rs:266-281`）。
- **生成方式**：Rust 绑定由 `build.rs:5-35` 的 `tonic_prost_build` 编进 `OUT_DIR`，**不入库**；
  入库的是 Go 绑定 `scheduler.pb.go` / `scheduler_grpc.pb.go`，由 `make -C services proto` 产出。
- 兼容性现状：gateway 不构造也不消费 `NewSandboxHint`，aenv-node 无该 hint 的运行时消费者，
  故 wire 上是 additive 的。

### 3.8 不改动清单

`filter_unschedulable` 语义、`prefer_node_id` 优先级（过滤之后、shadow 与策略之前）、
`ScheduleRequest` 以外的 proto、aenv-node 代码、部署清单（镜像引用除外）。

---

## 4. 测试

### 4.1 现有行为守卫

`filter_unschedulable`（`src/node_registry/filter.rs:37-44`）的**完整 keep/drop 集**逐值断言：
keep `{UNSPECIFIED, READY}`，drop `{CONNECTING, UNHEALTHY, LINGERING, DRAINING}`
（其语义是"保留 `UNSPECIFIED`，其余只保留 `can_accept_new_requests()` 为真者"，
`src/proto.rs:32-35`；心跳会把缺省状态改写为 `CONNECTING`，`registry.rs:1116-1122`）。
新增状态时该守卫必须失败。

### 4.2 正交夹具（数值为规范值；`K=N`；每例重置 scripted RNG；每例跑一遍输入顺序反转）

内存以 GiB 书写，实现按字节；请求记作 `(cpu, mem_mib)`。

| 夹具 | 节点 | cpu / alloc_cpu | mem / alloc_mem | 请求 | after_cpu / after_mem | pressure | 正确答案 |
|---|---|---|---|---|---|---|---|
| **F-1a** 内存维度决定 | `node-a` | 8 / 1 | 8 GiB / 7 GiB | (1, 1024) | 0.25 / **1.0** | 1.0 | **`node-z`** |
| | `node-z` | 8 / 6 | 64 GiB / 8 GiB | | **0.875** / 0.140625 | 0.875 | |
| **F-1b** CPU 维度决定 | `node-a` | 8 / 2 | 16 GiB / 12 GiB | (1, 1024) | 0.375 / **0.8125** | 0.8125 | **`node-a`** |
| | `node-z` | 8 / 7 | 64 GiB / 8 GiB | | **1.0** / 0.140625 | 1.0 | |
| **F-2** 容量 vs 占用率 | `node-a` | 64 / 56 | 256 GiB / 224 GiB | (1, 1024) | **0.890625** / 0.87890625 | 0.890625 | **`node-z`** |
| | `node-z` | 4 / 1 | 8 GiB / 2 GiB | | **0.5** / 0.375 | 0.5 | |
| **F-3 小** 请求反转 | `node-a` | 8 / 4 | 8 GiB / 1 GiB | (1, 1024) | 0.625 / 0.25 | 0.625 | **`node-a`** |
| | `node-z` | 8 / 6 | 64 GiB / 8 GiB | | 0.875 / 0.140625 | 0.875 | |
| **F-3 大** 同一对节点 | `node-a` | 同上 | 同上 | (1, 7168) | 0.625 / **1.0** | 1.0 | **`node-z`** |
| | `node-z` | 同上 | 同上 | | **0.875** / 0.234375 | 0.875 | |
| **F-8** 单位（内存必须主导） | 单节点 | 8 / 0 | 1 GiB / 256 MiB | (2, 512) | 0.25 / **0.75** | **0.75** | 精确等于 0.75 |

各夹具承担的证伪职责：F-1a 杀"只看 CPU"；F-1b 杀"只看内存"；F-2 杀"取绝对空闲量最大"
（`node-a` 空闲 8 vCPU 多于 `node-z` 的 3）；F-3 两次答案不同，杀"忽略 requested_*"；
F-1a 与 F-1b 的正确答案分别是 `node-z` 与 `node-a`，"永远取首个"与"永远取末个"同时被杀。

| 夹具 | 构造 | 正确答案 |
|---|---|---|
| **F-4** 分类边界 | stale / clock_skew（`last_seen > now`）/ zero_denominator（`cpu_count=0`）/ overflow 各一例，**外加 overallocated 一例** | 前四例为对应 `Unknown{reason}`；🔴 **overallocated 是 `Scored{pressure>1}`，不是 Unknown** |
| **F-5** unknown 混合 | 部分 unknown、全 unknown | 全 unknown 时仍产出 shadow 选择 |
| **F-6** K 边界 | K=0（配置拒绝）、K=1、K=N、K>N、默认值=3 | 符合 §3.5 |
| **F-7** TTL | `report_ttl=5s`（非默认）与默认各一例 | 用记录自身 TTL 判定（K-4） |

### 4.3 变异证据（每条必须**确定性**失败，PR 中贴失败输出）

| 变异 | 确定性 killer 与期望 |
|---|---|
| **M-1** `pressure` 返回常量 | **F-1a**（唯一钉死了采样顺序 `[node-a, node-z]` 的夹具）：常量体全部同分 → 落到"采样顺序在先者" → 选 `node-a`，正确答案 `node-z`。🔴 其他夹具是否同时杀死常量体取决于各自采样顺序，**不作为 M-1 的验收依据，也不要为此扩充采样脚本** |
| **M-2** 删 `after_mem`（只看 CPU） | **F-1a**：只看 CPU 时 `node-a`(0.25) < `node-z`(0.875) → 选 `node-a`，正确 `node-z` |
| **M-3** `max()` → `min()` | **{F-1b, F-3 小}**：F-1b 取 min 后 `node-a`=0.375 > `node-z`=0.140625 → 选 z，正确 a；F-3 小取 min 后 `node-a`=0.25 > `node-z`=0.140625 → 同样失败。（F-1a / F-2 / F-3 大取 min 后答案不变，杀不掉本条） |
| **M-4** 忽略 `requested_*` | **F-3**：两次答案变相同 |
| **M-5** stale 改用节点自报时间 | 同一 `now=T`、`report_ttl=5s`：① `last_seen=T-6s` 且 `reported_at=T` → 正确 `Stale`，变异体判 `Fresh`；② 反向控制 `last_seen=T-1s` 且 `reported_at=T-6s` → 正确 `Fresh` |
| **M-6** 采样改"永远取前 K 个" | 直测 sampler：`[a,b,c,d]`、`K=2`、scripted shrinking-pool draws `[3,0]` → 期望 `[d,a]`；变异体得 `[a,b]` |
| **M-7** 采样改**有放回** | `[a,b]`、`K=2`、draws `[0,0]` → 正确得 `[a,b]`（第二次在剩余池 `[b]` 取 0）；变异体得 `[a,a]`。**同时断言输出长度=2 且集合基数=2** |
| **M-8** `placement_shadow_k = 0` 不再被拒绝 | **F-6** |
| **M-9** shadow 影响返回值 | 两节点**均** mem 64 GiB / alloc 8 GiB，请求 `(1, 1)` → 两者 after_mem 同为 8193/65536 = **0.1250152587890625**；`node-a` cpu 8/alloc 7 → after_cpu 1.0 → **pressure 1.0**；`node-z` cpu 8/alloc 0 → after_cpu 0.125 → **pressure 0.1250152587890625**（内存维度决定）。RR 初始游标 0（候选按 id 排序后首个是 `node-a`），`K=2`、scripted 采样顺序 `[node-a, node-z]` → 断言**返回 `node-a`** 且 `agreement{source="schedule", agrees="false"}` +1 |
| **M-10** 删除生产里的 shadow 调用 | M-9 的指标断言失败 |
| **M-11** MiB 未换算 / 换算两次 | **F-8** 的 `pressure == 0.75` 断言：未换算得 ≈0.2500004768，双重换算得 ≈524288.25 |
| **M-12** `place_new` 仍丢弃 `_resources` | §4.4 producer 测试失败 |

### 4.4 接线与热路径测试

- **producer 测试**：从 `NativeNodePlacement::place_new(cpu=2, memory_mib=512)` 进入，断言 shadow 实际
  收到资源、换算正确、`missing_request_resources_total` **不增**。
  （只从 `NodeRegistryGrpcService::schedule` 手造 hint 进入不够——那样 producer 仍丢资源也会全绿。）
- **hint 形状矩阵**：§3.4 七种情形逐一断言 requested 值与 missing 计数，其中"仅一个字段"拆成
  **CPU-only** 与 **memory-only** 两例，另加 **`Some(0)`/`Some(0)`** 一例（按 0 计算且不计 missing）。
  `Some(hint { kind: None })` 用 raw 字节 **`12 02 1a 00`** 构造，断言 outer hint 为 `Some`、
  `kind` 为 `None`。
- **source 分流**：`schedule` 与 `paused_lookup` 各自打到自己的 `source`；`prefer_node_id` 命中时
  **不产生** agreement 样本。
- **no-snapshot 守卫**：无心跳节点仍出现在真实候选集中、shadow 分类为 `no_snapshot`、
  全程只有一次 registry 读取。
- **共享游标不变式**：`Schedule` 与 `lookup_node` Paused 分支交替调用，轮询序列连续。
- **并发 burst**：同一心跳窗口内并发 `Schedule`，返回分布与改动前一致。

### 4.5 legacy wire golden

在 `src/proto.rs` 测试中定义一个**只含 `metadata` field 1** 的本地 legacy prost 形状（两端都用新类型
证明不了滚动兼容），钉死三组字节：

| 用例 | 字节 |
|---|---|
| 字段全缺 | `12 02 12 00` |
| `cpu_count=2, memory_mib=512` | `12 07 12 05 10 02 18 80 04` |
| 显式零 | `12 06 12 04 10 00 18 00` |

断言：新类型把第一组解成 `None`/`None`、第三组解成 `Some(0)`/`Some(0)`；**legacy 形状解第二组时仍
识别 `new_sandbox` 且忽略 tag 2/3**。Go 侧指针字段由 `make -C services proto` 与 Go 编译覆盖。

### 4.6 E-1 的处理

本期**不交付 E-1 的任何硬测试**：`#[ignore]` 的期望不变式测试不会被 §4.7 任何命令执行；
"断言当前错误行为"又会把未来的正确修复变成测试失败；且四方一致性跨 Rust api 与 Go gateway，
无现有 harness 可承载。

改为建立**独立缺陷记录**，含确定性 reproducer、严重性、修复验收条件（修复批次同时落**非 ignored**
的四方一致性测试：实际 create 落点、wake response、binding、gateway 首次转发目标四者一致）。

延期成立的三个前提在本规格中同时成立：真实决策硬编码轮询、shadow 不影响返回值、
不存在可启用 best-of-K 的配置。

### 4.7 回归面

```
make fmt
make clippy
make test-unit
make test-with-redis
make check-crate-boundaries
make -C services test
make -C services proto && git diff --exit-code     # 生成物 diff-clean 门
```

---

## 5. 集群回归（pve-mf，10.1.0.200 / 10.1.0.201）

**定位：证明"没有弄坏"。E-2 已说明它无法证明排序质量。**

### 5.1 发布

1. 从**干净、已提交的同一个 commit** 构建 **runtime / api / gateway 三个镜像**。gateway 必须重建：
   proto codegen 必然改 `services/api/proto/scheduler.pb.go`，而 `deploy/docker/Dockerfile.gateway:5-8`
   复制整个 `services/`。本机构建（Dockerfile 需 BuildKit 1.7 前端），传
   `--build-arg AENV_GIT_COMMIT=<sha>`。
2. push 到 `10.1.0.201:5000`，记录三个 registry digest。
3. **按 digest 滚动**（`repository@sha256:...`），不用 tag（K-9：`IfNotPresent` + 可变 tag 会让节点
   继续跑缓存镜像）。
4. 逐 workload 核验：gateway 1/1、api 2/2、node DaemonSet 每个 replica 的 imageID digest 非空、
   **等于该 workload 自己那个镜像的 digest**（不是三个镜像互相相等）。
5. 保存旧 digest 作为回滚输入。
6. 🔴 api 与 node 同 commit、一起滚（K-9），不申请豁免。

### 5.2 门禁取证（逐 Pod）

- Secret `agentenv-control-plane-token` 被读的两键（gateway 读 `token`，api 与 node 都读
  `node-gate-token`）非空且字节相等（不打印值）。三处挂载均 `optional: true`
  （`deploy/k8s/base/gateway-deployment.yaml:56-71`、`agentenv-api-deployment.yaml:751-758`、
  `agentenv-daemonset.yaml:588-595`），缺失即门禁全关。
- 🔴 gauge=1 **不**证明加载的 token 等于当前 Secret：读文件失败会保留上一次成功值
  （`src/api/control_plane_gate.rs:235-244`）；gateway 的 token 是 Pod 启动时的 env。
- 因此：**每个 api Pod** 直连 `GET /sandboxes` 验四态（仅 API key→403 / 错 control token→403 /
  仅正确 control token→401 / 两者正确→200）——api 有 2 副本
  （`agentenv-api-deployment.yaml:46-50`），只打 Service 可能四次落同一副本；
  **每个 node Pod** 直连 `GET /nodes`（带 `X-Admin-Token`）同样四态；
  最后从 **gateway** 只带普通 API key 请求一个必定 200 的路由，证明正在运行的 gateway 确实盖戳。
- 全部通过后才跑 e2e。

### 5.3 e2e 与判读

```bash
KUBECONFIG=$HOME/.kube/config-aenv-mf \
E2E_DEV_CLUSTER_GATEWAY_NODEPORT_URL=http://10.1.0.200:30800 \
bash scripts/tests/e2e/run_dev_cluster.sh
```

- 🔴 **不要用 `make test-e2e-k8s`**：它走 delete/apply 路径，等于拆掉长期集群。
  `run_dev_cluster.sh:11-23,85-94` 强制 apply/delete/load 为 none——**它只测"当前正在跑的东西"**，
  这也是 §5.1 的发布核验不可省略的原因。
- 判读基线（钉在 `55a3b6a`）：14/14 套件、`[PASS]` 117 行（含 8 行 skip-as-pass）、`[FAIL]` 0
  → **109 条非 skip PASS 行**。不称"真断言"：runner 没有 skip 计数器，若干 `_pass` 分支只表示
  "覆盖不可观测"。
- **失败条件**：suite 清单必须不变；**不得新增 skip 或空跑套件**；**非 skip PASS 行数不得减少**。
  只看"14/14 + 0 FAIL"会让新的空跑套件绿色蒙混。
- 已知空跑套件：`12_template_shared_repository:25-35`（node-REST 门）、
  `14_code_interpreter:14-17`（无条件退出）。它们的 exit 0 不携带任何集群信息。
- `09_e2b_compat:18-29,139-177` 的 PASS/skip 随本机 CLI/Python/npm/tsx 状态变化 →
  **记录工具与版本矩阵**并保存逐 suite 输出。`git diff -- scripts/tests/e2e/` 为空只能排除脚本漂移，
  不能解释依赖环境变化。
- 🔴 禁止在 node pod 内做全盘递归扫描（`grep -r /`、`find /`、`du -sh /`）。

---

## 6. 门控调查（只出结论，不出实现）

### 6.1 P2 沙箱级指标

E2B 的沙箱指标来自 orchestrator 通过 host IP + envd 端口拉取 **envd 的 `GET /metrics`**
（`e2b-infra/packages/orchestrator/pkg/sandbox/metrics.go:35`），host 侧只做聚合导出
（`pkg/metrics/sandboxes.go` 的 `SandboxObserver` + `exportInterval`）。我们的 envd 同源：
`thirdparty/envd/http-client/envd.yaml:18` 有该端点，且**生成的 Rust 客户端已提供 `metrics_get`**
（`thirdparty/envd/http-client/src/apis/default_api.rs:162-200`，含 `/metrics` URL、
`X-Access-Token` 与 JSON 解码）。因此门控**不是**"缺客户端要重新生成"，而是要拿运行时证据——
客户端存在不能替代生产 guest 真的答这个端点。

**"看到一次 200 就照抄"不成立**：vendored schema 字段全部非 required
（`envd.yaml:334-369`），返回 `{}` 也满足；E2B 实现带 envd 版本门槛、100ms 超时、5 秒周期与并发限制
（`sandboxes.go:27-42,160-190`）。

门控动作：经数据面路由（端口 49983）探测一个 secure sandbox，携带 envd token；记录 envd 版本、
HTTP 状态与返回体；校验 `ts` / `cpu_count` / `cpu_used_pct` / `mem_total` / `mem_used` /
`mem_cache` / `disk_total` / `disk_used` 的类型与取值范围；间隔后再取一次，要求 `ts` 前进。
**结论只能写"生产 guest endpoint 可用/不可用"**，不得声称 observer 或导出管道已验证。

### 6.2 P3 overlaybd 离线检查工具

E2B 有 12 个 `orchestrator/cmd/*` 排障工具（`inspect-build`、`show-build-diff`、
`mount-build-rootfs` 等），我们整个仓库只有 3 个 bin，而 LSMT / segment mapping / zfile 跳表这套格式
没有任何 dump 手段。

门控动作：**先钉死语料**——列出最近三次存储类排障各自的**不可变标识**（issue / session / commit sha），
或给出仓库内可复现的查询与截止时间（否则不同执行者会选到不同的"三次"并得出相反结论）。
逐次回答：①要回答什么问题 ②能否被 `inspect` / `diff` / `mount` 之一覆盖。
**结束条件**：≥2 次可被覆盖则立项，否则就地关闭并记录结论。

---

## 7. 明确不做的项与理由

| 项 | 理由 |
|---|---|
| **`Strategy` trait / 策略选择配置 / 可启用的 best-of-K** | 一个能接受 `best_of_k` 的配置会让操作者绕过 K-8（pending 记账）、E-1（单一放置权威）与 metric cutover 三项前置；一个只接受 `round_robin` 的配置则是惰性配置。真实决策只有一个有状态实现时，引入 trait 是投机抽象 |
| **在途预留表** | 本期默认仍是轮询且无可启用 best-of-K 的配置，故可延后；翻默认时它是阻断项（K-8）。另：过量分配是否真实发生过，目前没有数据 |
| **事件持久化** | best-effort 且无重试，失败批次记录后丢弃（`src/observability/reporter.rs:22-23,181-208,377-383`）；收敛发生在**下一次成功的 heartbeat reconciliation**（`grpc_service.rs:923-930`），而 heartbeat 失败会**指数退避到 60 秒**（`reporter.rs:140-175`），binding store 持续不可用时 reconciliation 同样失败（`grpc_service.rs:580-632`）。🔴 因此**不存在"一个心跳窗口"的上界**；期间是 stale routing projection，属可用性/路由缺口（首请求可能失败），不只是可观测性。结论仍是不持久化 |
| **PG schema 统一** | `preflight` 在 ledger 非空时立即返回（`crates/aenv-api/src/snapshot/repository/backends/postgres/migrate.rs:201-208`），加 v2 **不会**普遍启动失败；真实风险是反向的——**静默采用一张不兼容的既有表**，且 paused registry 另有自己的 execution-axis preflight（`paused_registry/postgres/schema.rs:121-170`）。需要显式 verify/adopt 路径 |
| **按调用方限额** | `src/api/impls/auth.rs:19-53` 只检查 header 存在性，`Claims` 不携带可信调用方身份。键在可伪造 header 上的限额是安全剧场；若要做只能做集群级准入上限，属另一次决策 |
| **多集群 / hyperloop 回调 / 跨沙箱持久卷 / 模板分层缓存** | 产品定位差异或缺乏频次数据 |

### 7.1 翻默认（让 best-of-K 真正决定放置）的前置条件

① shadow 指标显示分歧率与 pressure spread 有意义；② 跨副本 pending 记账落地；
③ 并发 burst 覆盖心跳盲窗；④ 引入 `Strategy` trait 与策略选择；
⑤ 盘点并改造所有按 `strategy="round_robin"` 过滤的 dashboard / recording rule / alert，并同步
`CLAUDE.md:193-200`、`services/README.md:42-48`、`config/default.toml:154-165`；
⑥ 登记"恢复路径的策略选择不产生 Schedule strategy metric"（`grpc_service.rs:761-783,816-840`）；
⑦ E-1 的单一放置权威修复。

---

## 8. 交付物清单

1. `src/node_registry/placement/{mod,score,sample}.rs`（不动 `strategy.rs`，不引入 trait）
2. `registry` 新增 `peek_observed_with_freshness`（复用私有 TTL helper）+ 编译器确认的**全部**
   `NodeRegistry` 测试替身更新（已知三处见 §3.2）
3. `ShadowCandidate` sidecar 与一次读取路径（`lookup.rs:99-109` 附近）
4. shadow 调用点与 `source` 传参：`grpc_service.rs:766-771`、`lookup.rs:428`
5. 构造期缓存的 4 + 12 + 2 + 2 个指标句柄
6. proto：`optional cpu_count = 2` / `optional memory_mib = 3`；`make -C services proto` 重生成 Go
   绑定并提交；`native_placement.rs:266-281` 填充资源
7. 配置 `[cluster].placement_shadow_k`（env / u32 / 默认 3 / 0 拒绝）+ 默认值与拒绝测试
8. §4.1 守卫、§4.2 全部夹具（含顺序反转）、§4.3 M-1..M-12 变异证据（贴失败输出）、
   §4.4 全部接线测试、§4.5 wire golden、microbenchmark 或延迟实测
9. §4.7 全部回归输出，含 `make -C services proto` 的 diff-clean 证明
10. §5 发布记录（三镜像 digest、逐 replica imageID、旧 digest）、逐 Pod 门禁四态结果、
    e2e 逐 suite 输出与 117/8/0 判读 + 工具矩阵
11. E-1 独立缺陷记录（reproducer / 严重性 / 修复验收条件）
12. §6.1 envd `/metrics` 实测记录与 P2 结论；§6.2 P3 语料标识、逐次判定与结论

---

## 9. 实现者最容易做错的三件事

> 以下由对抗审查方（GPT-5.6-Sol）在通过评审时给出，原文保留。

1. **真实放置绝不能变**：先保存 `RoundRobinStrategy` 的返回值，再做 shadow；`prefer_node_id` 命中
   立即返回，不执行 shadow；`Schedule` 必须传 `ShadowSource::Schedule`，Paused 分支必须在
   `lookup.rs:428` 直接传 `PausedLookup`，任何 shadow 错误、分类或采样结果都不得进入返回路径或推进
   第二个 RR 游标。
2. **freshness 与资源只解码一次**：候选构造时用同一个 `now`、一次 registry 读取，原始
   `RichNode.snapshot` 原样供现有 filter/RR，`Option<SnapshotFreshness>` 只供 shadow；按每条记录
   自己的 TTL 分类。严格区分 optional 的 `None` 与 `Some(0)`，覆盖 `hint=None`、`Some(kind=None)`、
   CPU-only、memory-only、NewCold，并对所有 MiB 值只做**一次** `checked_mul(1_048_576)`。
3. **不要把观测变成热路径锁**：构造期缓存全部 metric handles，调用时先在栈上聚合、每类最多
   increment 一次；生产使用 per-call RNG，不共享带锁 RNG，不按原始 K 分配。完成后必须跑
   producer/source/hint 矩阵、M-1..M-12、legacy wire golden、N=2/100/1000 延迟证据及 Go proto
   diff-clean 门，**不能只测纯 score 函数**。

---

## 附录：审查轨迹

本规格经六轮对抗审查（GPT-5.6-Sol，只读模式，逐条要求 file:line 证据），BLOCKING 数
**16 → 12 → 4 → 3 → 1 → 0**，共解决 36 条。过程中被推翻的关键前提包括：放置侧曾以为存在的资源上限
（实为已删除的未接线 stand-in）、`preflight` 会导致启动失败（实为 ledger 非空即早返回）、
事件通道"已知会卡死"（实为失败即丢批次、指数退避）、`NewSandboxHint` 不带资源（实为已到手后被丢弃）、
以及 `filter_unschedulable` 只过滤 `DRAINING`（实为 keep/drop 集）。
§4 的全部数值与 §4.5 的字节序列均经独立验算。
