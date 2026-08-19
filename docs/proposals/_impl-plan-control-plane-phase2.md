# 实施计划：中央控制面 阶段 2（PG 写权上收）

> 2026-08-19 · 主 agent 裁决，供研发 agent 执行。
> 🚦 **闸门 A 已通过**（2026-08-19，用户裁决）：**长期自维护 fork，手动从上游拉取检查合并。**
> ⇒ 上游追平成本不再是约束，**允许改动 Rust 主干**。
>
> 上位：[`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md) §3 §4 阶段 2
> 前序：[`_impl-plan-control-plane-phase01.md`](_impl-plan-control-plane-phase01.md)（阶段 0/1，已上线验证）
> 侦察：[`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md)（**语义规格书 + 测试移植清单**）、
> [`_recon-R4-rust-client.md`](_recon-R4-rust-client.md)（**Rust 接入面 + 三个技术风险**）

---

## 0. 目标与非目标

**目标**：node 不再直连 PG。登记表的**全部读写**经 gRPC 打到 controller（= scheduler），
由 controller 独占 PG 与 schema。

**兑现**（方案 §5.1）：
- **G7** PG DSN 不再下发到每台跑用户代码的 KVM 机器 —— 爆炸半径 N 台 → 1 处（**最硬的理由**）
- **G8** PG 连接数恒定（今天每 node 8 条，20 台 = 160 条常驻）
- **G6** schema 有唯一 owner
- controller 重启不丢 registry

**明确不兑现**：G1 / G2 / G4 —— 语义仍是**节点视角**，只是换了条路访问 PG。
那些要等阶段 3（闸门 B：fencing 方案 + 跨仓 ExecutionID 契约）。

**明确不做**：任何 `AllowedTransitions` 表、ExecutionID、中央 poll、evictor 重构。
阶段 2 的 RPC 面**按阶段 3 的目标形状设计**，但实现仍是"节点是发起方"。

---

## 1. 🔴 护栏：方案的四条 + 本轮新增的第五条

前四条见方案 §3（全有或全无 / 租约冻结 / 丢弃熔断 / resume 三分法显式建模），
**每一条都是验收条件，不是 nice-to-have**。以下是本轮新增的：

### 🔴 §3.5（新增）：registry 不可达时，resume 必须失败，不能放行

**这是 R4 找出的、方案漏掉的反方向盲区。**

方案 §3 的四条全部针对「**停手 vs 当作空**」这一个方向。但 `arbitrate_resume`
（`src/api/impls/paused_recovery.rs:173-181`）是**反方向**的：

```rust
registry 报错 ⇒ ResumeArbitration::Proceed   // 放行，不做任何集群检查
```

`running`/`resuming` 永不可抢这条不变式，在实现层的最后一道闸就是 `claim_for_resume`
返回 `Conflict`。registry 一不可达这道闸整个消失；**同一次故障还会让 `discard_if_superseded`
静默不删**（`:778-785` + `:732-734`）—— 两道防线同源同时失效，后果是双活：
两台 VM 从同一快照分叉、各写各的 rootfs 层、gateway 在两者间抖。
这正是 `postgres.rs:69-79` 花十行论证要避免的东西。

**为什么阶段 2 才变严重**：今天该错误 = node 到集群内 PG 的连接故障（同集群、连接池常驻、
发生率极低）。阶段 2 之后 = node 到 **scheduler** 的 gRPC 故障，而 scheduler `replicas: 1`、
无 PDB、无 `maxSurge` —— **滚动升级、拉镜像、OOM、驱逐都会制造窗口**。

**验收条件（采用 R4 建议的收窄版，不是一刀切）**：
- 本地有 paused 记录 **且** 该记录是 `ClusterRegistration::As(_)`（即曾被登记到集群）
  ⇒ registry 不可达时 resume 返回 **503（可重试）**，不再 `Proceed`
- 其余情况（从未上过集群的沙箱）保持 `Proceed` —— 它们的本地副本就是唯一副本，
  登记表答不出来对它们本就不携带信息（`paused_recovery.rs:759-763`）
- 必须有 metric 区分这两条路径

> 取舍说明：registry 抖动时 resume 会短暂失败。那是**可重试的失败**；双活**不可逆**。

---

## 2. 分片：四片，第一片独立可上线

### 🅐 Slice A：前置加固（**不依赖阶段 2 任何东西，先做、先上线、先观察**）

把 R4 的风险 2 与风险 3 在架构变更之前就消化掉，并把安全绳架起来。

| # | 动作 | 为什么现在做 |
|---|---|---|
| **A1** | `release_stale_node_holdings` 改成**带围栏可重试** + 失败 metric | R4 风险 2。今天它只有一次机会、失败后永不重试、只有一条 `warn!`；阶段 2 会把它的失败概率从"PG 不可达"提到"scheduler 在滚动"。围栏条件用 `running_registrations` 为空（`paused_coordinator.rs` 已有该结构） |
| **A2** | metadata **golden fixture**（Rust dump 一份 → Go 读同一份断言原样往返） | R4 风险 3a。两侧今天**没有共同的真值来源**，全仓 grep `timeout_action` 零命中 |
| **A3** | CI 补带真 PG 的 job，跑 `cargo test -p agentenv --test paused_registry` 并设 `AENV_PAUSED_REGISTRY_TEST_REQUIRED=1`；Go 侧补等价的 `SCHEDULER_REGISTRY_TEST_REQUIRED` 防假绿 | 🔴 R4 §5.3：**那 29 个集成测今天根本没在 CI 里跑**。它们是阶段 2 要对齐的语义基准，基准本身没人守 |
| **A4** | `[orchestrator.paused_registry]` 段写进 `config/default.toml`；DaemonSet 的 DSN secretKeyRef 也进 base 清单 | 修 R3 §3.2 那颗地雷的**根因**：今天这段只活在集群 ConfigMap 里，`make k8s-apply` 会让它整节消失、静默回落 `local`、pause 变节点本地且**不报错** |
| **A5** | `complete_pause` / `mark_local_only` 失败后**先 `get` 再决定删不删快照** | R4 §4.5：今天这两个失败会直接删快照，而阶段 2 里"失败"多了一种"够不到 controller"的含义 |

**A 片全部只动 Rust + CI + 清单，不碰 Go 控制面。**独立提交、独立验证、独立上线。

---

### 🅑 Slice B：controller 侧（Go）—— 契约测试先行

**顺序不许颠倒**：先 proto，再**契约测试**，最后实现。方案 §7 步骤 3 的理由是
「942 行 Rust → Go，generation CAS / lease / 三分法边界必须逐条对齐」，
测试是这次翻译唯一的安全绳。

| # | 动作 |
|---|---|
| **B1** | `services/api/proto/scheduler.proto` 新增 `PausedRegistryService`，**5 个 RPC**（见 §3） |
| **B2** | **契约测试先写**：按 [`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md) §5 的移植清单，把 **P0 的 36 项**移植成 Go 测试，对着接口写、此时必然全红 |
| **B3** | Go 写实现：`postgres.rs` 的 SQL 与状态判定翻成 Go，**逐条对齐**（三分法、generation CAS、lease、两条件回收） |
| **B4** | schema migration 移到 controller 启动时；DDL **第一版逐字复制 `SCHEMA_DDL`，不加任何新列** |
| **B5** | 护栏 §3.1 / §3.2 / §3.3 落地（见 §4） |
| **B6** | `reclaim_expired_holdings` **不进 RPC 面** —— 从本阶段起由 controller 自己按定时器跑（它本来就是"集群兜底"，不该由节点发起） |

---

### 🅒 Slice C：node 侧（Rust）Central 后端

| # | 动作 |
|---|---|
| **C1** | `PausedRegistryBackendKind::Central` + `build_paused_registry` 分支（签名加 `&ClusterConfig`） |
| **C2** | 新增 `src/orchestrator/paused_registry/central.rs`，实现 13 个 trait 方法，映射到 5 个 RPC |
| **C3** | 🔴 **错误映射**：gRPC 层任何非 `OK`（含 deadline、EOF、部分流）⇒ `PausedRegistryError::Backend`，走现有的 `warn!("registry unreachable; stopping reconciliation"); return` 分支。**绝不允许把任何 gRPC 错误翻译成"空结果"** |
| **C4** | 护栏 §3.5 落地（resume fail-open 收口，见 §1） |
| **C5** | `build.rs` 翻 `build_server(true)`，为传输层单测提供服务端桩 |

---

### 🅓 Slice D：切换、验证、回退

见 §5。

---

## 3. RPC 面（**按阶段 3 的目标形状设计，不做 13 方法 1:1**）

方案 §4 阶段 2 已裁定不做 1:1 映射，理由是「它把'节点是决策者'从实现细节升格成跨进程网络契约，
而阶段 3 第一件事就是删掉这个契约」。

| RPC | 覆盖旧方法 | 阶段 3 是否保留 |
|---|---|---|
| `GetSandboxes` | `get` / `get_many` | ✅ |
| `TransitionSandbox`（带 `expect_generation`）| `begin_pause` / `complete_pause` / `mark_local_only` / `mark_running` / `release_claim` / `remove` | ✅（改由 controller 主动发起）|
| `AcquireSandbox`（三分法在服务端判）| `claim_for_resume` | ⚠️ 阶段 3 改由 controller 内部调用，RPC 面消失 |
| `RenewNodeLease`（批量）| `renew_lease` | ❌ 阶段 3 被中央 poll 取代 |
| `ReleaseNodeHoldings` | `release_node_holdings` | ⚠️ 见方案「阶段 3-B」的证据等级讨论 |

### 3.1 🔴 metadata 只走两条 RPC（R4 §3.2 的关键发现）

`entry.metadata` 在整个 Rust 侧**只有一个消费方** —— `paused_recovery.rs:312`
`restore_request(&entry.metadata, ...)`，即跨节点重建路径，其 entry 来自 `claim_for_resume`。
`get_many` 的两个消费方只看 `state` / `origin_node_id` / `claimed_by_node_id` / `generation`，
**完全不碰 metadata**。

⇒ **`GetSandboxes` 不带 metadata。** metadata 只出现在：
- `TransitionSandbox(begin_pause)` — 写方向
- `AcquireSandbox` — 读方向，且只有一行

这同时消掉 tonic 4 MiB 解码上限的风险，并把"字节级往返"的验证面缩到两个点。

### 3.2 🔴 metadata 的线上表示：`bytes`，不是 `Struct`

```proto
bytes metadata_json = N;   // 原始 JSON 字节，逐字往返
```

**不许用 `google.protobuf.Struct`** —— 它会把整数塌成 double、重排键、归一化，
而 `SandboxMetadata` 没有 `deny_unknown_fields`，丢字段时**不报错**。

**Go 侧全程 `json.RawMessage`，直接 bind 成 JSONB，不定义 Go struct。**
丢掉 10 个必需字段之一 ⇒ 那台沙箱在所有读路径上永久 `InvalidRecord`，
既读不出也认领不了，只能人工改库 —— 而这个错误**只在下一次 resume 时才暴露**。

### 3.3 proto 草案（B1 的靶子）

```proto
service PausedRegistry {
  // 批量读。🔴 全有或全无：任何后端错误 ⇒ RPC error，永不返回部分结果或空 map。
  // 不带 metadata（见 §3.1）。
  rpc GetSandboxes(GetSandboxesRequest) returns (GetSandboxesResponse);

  // 带 expect_generation 的状态转换。覆盖 begin_pause / complete_pause /
  // mark_local_only / mark_running / release_claim / remove。
  rpc TransitionSandbox(TransitionSandboxRequest) returns (TransitionSandboxResponse);

  // resume 授权。三分法在服务端判（方案 §3.4）。带 metadata（单行）。
  rpc AcquireSandbox(AcquireSandboxRequest) returns (AcquireSandboxResponse);

  // 批量续租。
  rpc RenewNodeLease(RenewNodeLeaseRequest) returns (RenewNodeLeaseResponse);

  // 进程启动时释放前任持有的行。
  rpc ReleaseNodeHoldings(ReleaseNodeHoldingsRequest) returns (ReleaseNodeHoldingsResponse);
}
```

**每个请求都必须带 `cluster_id` 与 `node_id`** —— Rust 侧的 cluster 作用域是写在每条 SQL 的
`WHERE` 里的（R2 §2.5/§2.13），不是连接级的，Go 侧不许把它降级成隐式。

**`RegistryEntry`（读回的行，不含 metadata）**：
```proto
message RegistryEntry {
  string sandbox_id = 1;
  string cluster_id = 2;
  string state      = 3;   // 五态之一，字符串不是 enum —— 表的 CHECK 约束是真相源，
                           // 本 build 不认识的值必须能原样传到运维眼前，不能塌成 UNSPECIFIED
  int64  generation = 4;
  string origin_node_id     = 5;
  string claimed_by_node_id = 6;   // 空 = NULL
  string snapshot_id        = 7;   // 空 = NULL
  int64  paused_at_unix_micros  = 8;
  int64  updated_at_unix_micros = 9;
}
```
🔴 **时间用 `unix_micros`**：PG `TIMESTAMPTZ` 是微秒精度，`chrono::DateTime<Utc>` 是纳秒精度。
用纳秒会让往返出现"写进去 123456789ns、读回来 123456000ns"的静默截断；
用秒会让同一秒内的多次转换无法排序。**微秒是两侧的公约数。**
（租约两列**不进** `RegistryEntry` —— Rust 侧的 `ENTRY_COLUMNS` 不含它们，
消费方也看不到；阶段 0 的只读对账另有自己的读模型。）

**`AcquireSandboxResponse` 必须显式建模三分法 + 四个变体**（方案 §3.4 验收条件）：
```proto
message AcquireSandboxResponse {
  oneof outcome {
    AcquiredSandbox claimed = 1;   // 含 entry + metadata_json + previous_state
    Empty           not_found = 2;
    OriginRef       not_ready = 3;  // publishing/local_only 且租约还活着
    OriginRef       conflict  = 4;  // 活在别处，或刚输掉一次竞态
  }
}
message AcquiredSandbox {
  RegistryEntry entry = 1;
  bytes  metadata_json = 2;
  // 🔴 UPDATE 之前的状态，来自 `previous` CTE，不是 RETURNING 的行。
  // 从 entry 读会让每次普通 resume 都被报成租约接管（那个 bug 潜伏了数月）。
  // previous_state ∈ {publishing, local_only} ⇒ 这是一次"回退到上个快照"的降级，
  // 必须是显式的、可告警的事件（方案 §3.4）。
  string previous_state = 3;
}
```

**`TransitionSandbox` 的 CAS 语义**：`expect_generation` 用 `optional int64`
（`mark_running` 与 `remove` 不带 CAS，R2 §3.4 统计只有 4 个方法是真 CAS）。
CAS 失败必须是**可与"行不存在"区分**的应答 —— Rust 侧
`mark_local_only` 的 CAS 失败要上报（R2 §2.3 的测试 `a_downgrade_that_matches_nothing_is_reported` 钉死）。

---

## 4. 三条护栏的落地要点

### §3.1 全有或全无
- controller 侧 `GetSandboxes`：任何后端错误 ⇒ **RPC error**，永不返回部分或空 map
- controller 未完成 migration / PG 不可达 / 刚启动未 warm ⇒ 一律 `UNAVAILABLE`，**不是空结果**
- node 侧：见 C3

### §3.2 租约冻结（controller 停机 >90s 是**阶段 2 新引入**的相关性故障）
controller 启动后进入 **grace 期**：
1. 先把本集群所有租约延长 `(观测到的停机时长 + lease_ttl)`，再开放 `AcquireSandbox` / reclaim
2. 停机时长从 PG 里的 `max(updated_at)` 与当前时间推算，取不到就按一个完整 TTL 算
3. grace 期内对 `publishing`/`local_only` 一律拒绝认领（`paused` 不受影响 —— 那条路不依赖租约）
4. grace 期状态必须可观测（metric + `/healthz` 区分 ready 与 serving）

> ⚠️ R2 歧义 3：`begin_pause` 写的 `updated_at` 是**节点进程时钟**，其余写路径是 DB `now()`。
> grace 推算会因此在节点时钟漂移时算错，**且方向不确定**。
> ⇒ B3 顺手把 `begin_pause` 的 `paused_at`/`updated_at` 改成 DB `now()`
> （`paused_at` 的调用方传值本来就被忽略）。

### §3.3 丢弃熔断
单轮 reconcile 丢弃的本地记录数超过阈值（绝对值或占比，**取严**）⇒ 停手 + 告警，不执行。
这不是防某个已知 bug，是防"我们还没想到的那个"。

---

## 5. 切换、回退与硬门禁

### 5.1 🔴 schema owner 交接是**硬门禁**，不是建议
- `SCHEMA_DDL` 里 `DROP CONSTRAINT IF EXISTS … / ADD CONSTRAINT …` 是**无条件**的，
  每个跑 `postgres` 后端的 node **每次启动**都跑（`postgres.rs:51-53,127`）
- controller 一旦给 CHECK 加了新状态且库里已有该状态的行，下一台跑 `postgres` 后端的 node
  启动时 `ADD CONSTRAINT` 会失败 ⇒ **那台 node 永远起不来**，而根因在另一个进程里
- ⇒ **阶段 2 期间 controller 的 migration 第一版必须是 `SCHEMA_DDL` 逐字复制、零新列**
- ⇒ controller 复用同一个 advisory lock key（`0x0A6E_7653_4348_4D41`）作为廉价保险

### 5.2 切换窗口
所有 node **同时**切。混跑期两种 backend 写同一张表：generation CAS 仍能仲裁，
但租约 TTL 参数来源不同，且 `ensure_schema` 的 advisory lock 会与 controller migration 抢。
⇒ **在无活沙箱窗口切**。

### 5.3 回退
`backend` 配置切回 `postgres`。**需要 node 重启**（配置是 `OnceLock`，无热加载）。
Rust 侧 `PostgresPausedSandboxRegistry` **保留一个观察期**再删。

### 5.4 启动顺序
今天 PG 不可达 ⇒ node 起不来（fail-closed）。Central 后端若照抄 `connect_lazy()`
⇒ node 起得来但 A1 那条静默失败。**A1 先做掉就是为了让这里可以安全地 fail-open。**

---

## 6. 通用纪律

1. Go 注释**英文**（`services/` 现有风格）；Rust 注释**英文**（仓内现有风格）。
   ⚠️ 本仓 `apps/AgentENV/` 的注释语言与外层 uns-swe 的"注释必须中文"**不同**，以本仓为准
2. Conventional Commit：`feat:` / `fix:` / `refactor:` / `chore:`
3. Rust 侧门槛：`make fmt` + `make clippy`（`-D warnings`）+ `make test-unit`
4. Go 侧门槛（`GOWORK=off`）：`go build ./...` + `go vet ./...` + `gofmt -l .` 空 + `go test -count=1 ./...`
5. **每条改动都要有测试，且做变异验证**（把修复/实现退回去，确认测试 FAIL）。
   这是阶段 0/1 已经建立的标准，阶段 2 风险更高，不许降低
6. 不许 `make k8s-apply`
7. 分支 `central-control-plane-phase2`
