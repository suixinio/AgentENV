# Stage C 调研：paused registry（`services/scheduler/internal/registry`）→ `--role api`

> 调研快照：`dev` @ `1022b3a`（2026-08-25）。只读调研，未改任何代码。

## 0. 结论摘要

- **走的是新路**：`paused_sandboxes` 表连同它的 PG schema 一起搬进 `api`，claim / lease /
  fencing / reclaim 继续是 SQL 谓词，仲裁继续在 PostgreSQL 里做——只是换了哪个进程持有连接池。
  **不在 Redis 上重建这套协议。** 本文档从头到尾建立在这个前提上。
- **规模**：`services/scheduler/internal/registry` 非测试代码，`wc -l` 实测 **4,191 行**
  （`cloc` 去空行去注释后 2,181 行；本包被大段设计性 doc comment 占据）。
  `2026-08-20-service-decomposition.md` 引用的「4,044 行」是同一测量口径在今天
  `1022b3a`（10:19 UTC，修复 POST /timeout 的 deadline 传播）落地**之前**的行数——
  两者可对账：`1022b3a` 净增 147 行（grace.go +4、registry.go +7、store.go +50、
  store_postgres.go +88 − 2 净），4044 + 147 = 4191，逐文件核对精确吻合。**不是两个不同的数字，
  是同一数字在两个时间点的快照**，本文档以当前 HEAD 的 4,191 为准。
  测试代码另有 5,110 行（`cloc`），全部依赖真实 PostgreSQL。
- **不止 `internal/registry`**：`RunRegistryReconcile` 心跳续租逻辑在 `internal/reconcile.go`
  （648 行，含 ~400 行纯函数 `computeRegistryReconcile`），gRPC 转译层在
  `internal/registry_service.go`（978 行），指标定义在 `internal/metrics.go`（842 行），
  进程装配在 `cmd/main.go`（约 150/827 行与 registry 相关）。这四个文件**不算进 4,191**，
  但心跳续租的纯逻辑（`computeRegistryReconcile`）必须一起搬，否则 Fix A 的续租机制无处安放。
- **依赖**：Rust 侧目前**没有任何** PG 客户端——`sqlx` / `tokio-postgres` / `deadpool` 在
  `Cargo.toml` 和全部 `src/` 中零命中。Stage C 会是第一个引入者。且 Go 侧 registry 与
  catalog（Stage B，3,613 行，同口径实测）**共享同一个 PG 连接池，并且共享同一个 PG 事务**——
  `catalog` 的 `PausedHalfAdapter` 在自己开的 `pgx.Tx` 内调用
  `BeginPauseTx`/`CompletePauseTx`/`MarkLocalOnlyTx`，让「暂停行状态迁移」与「快照目录提交」
  原子落地。这不是并列关系，是硬耦合——见 §10 风险 1。
- **最大风险（三条）**：
  1. **两个后台定时任务的单实例假设**。`cmd/main.go` 里 `createRegistryStore` 的注释原话：
     「Never on a query-only replica... there is exactly one owner of this table's shape and
     of the reclamation timer」。`RunRegistryReconcile`（心跳续租）和 `RunReclaim`（回收）
     今天无条件在唯一一个 scheduler 进程里跑 `go func`。搬进 N 副本 `api` 后必须选主，
     否则 `Grace.ExtendLeases`（重启后把租约整体前移一段推断停机时间）会在多副本同时启动时
     被叠加 N 次。**建议：PG advisory lock 选主**，理由与方案见 §6。
  2. **Stage B/C 的 PG 事务耦合**。如果 Stage C 先于 Stage B 落地 Rust，`pause` 的 registry
     行迁移与 catalog 快照提交的原子性会跨进程丢失——见 §10。
  3. **单条 SQL 谓词本身在 N 副本下几乎全部安全**（generation/execution_id CAS + PG 行锁天然
     序列化），真正的坑不在语句本身，在两个定时任务的"谁来跑"和 Grace 的"跑几次"。

---

## 1. Go 侧全貌：`services/scheduler/internal/registry/`

### 1.1 逐文件行数（`wc -l`，非测试，当前 HEAD）

| 文件 | 行数 | 内容 |
|---|---:|---|
| `store_postgres.go` | 2,151 | 全部 SQL 语句常量 + `PostgresStore` 的每个方法实现 |
| `store.go` | 588 | `Store`/`Reader` 接口定义、`Entry`/`Sandbox` 类型、`State` 五态 |
| `grace.go` | 513 | 重启宽限期 `Grace`、`ExtendLeases`、`DiscardBreaker` 熔断器 |
| `postgres.go` | 295 | **只读** `PostgresReader`（`SET default_transaction_read_only`），供 `ListRegistrySandboxes` 管理端点与 `RunRegistryReconcile` 读取 |
| `registry.go` | 220 | 只读视图的 `Sandbox`/`State`/`Reader` 类型（`postgres.go` 用的那套，与 `store.go` 的 `Entry` 是两套并行的读模型——见 §7 坑 1） |
| `migrate.go` | 249 | `SchemaDDL`、`Migrate()`（PG advisory lock 串行化多控制器） |
| `catalog_tx.go` | 175 | `BeginPauseTx`/`CompletePauseTx`/`MarkLocalOnlyTx`/`ObserveGenerationTx`——供 catalog 在自己的事务内调用 |
| **合计** | **4,191** | |

测试文件（`cloc` 代码行，全部要求真实 PG）：`store_postgres_test.go` 2,934 行/86 个
`TestXxx`、`execution_fencing_test.go` 1,294 行/36 个、`contract_test.go` 885 行、
`contract_claim_test.go` 392 行、`contract_lease_test.go` 640 行（`contract_*` 三个合计 86 个
`TestXxx`）、`grace_test.go` 435 行、`postgres_integration_test.go` 405 行、
`metadata_golden_test.go` 322 行、`registry_test.go` 245 行、`legacy_schema_test.go` 59 行。

### 1.2 关联但不计入 4,191 的文件

| 文件 | 行数 | 与 Stage C 的关系 |
|---|---:|---|
| `internal/registry_service.go` | 978 | gRPC `PausedRegistry` 服务实现（Store → proto 转译）。搬进 `api` 后**这层整体消失**（同进程内直接调用，无需 RPC），但 `registryErrorCode` 的错误分类语义、`checkRenewalCadence` 的告警语义要保留（Rust 侧已有对应的 `PausedRegistryError` 分类，见 §4） |
| `internal/reconcile.go` | 648 | `computeRegistryReconcile`（纯函数，~400 行：比对 registry 行与心跳 roster，算出待续租/可回收候选）+ `RunRegistryReconcile`（定时器）+ `renewParkedLeasesFromHeartbeats`/`renewLiveLeasesFromHeartbeats`（Fix A 的落点）。**这是 Fix A 的核心逻辑所在**，必须整体移植 |
| `internal/metrics.go` | 842 | Prometheus 指标定义 |
| `cmd/main.go` | ~150/827 | registry 相关的装配：`createRegistryStore`、`Migrate`+`Grace.Enter` 时序、`go svc.RunRegistryReconcile(...)`、`go registrySvc.RunReclaim(...)` 的无条件启动 |

### 1.3 SQL 语句清单（按语义分组）

**Fencing / 状态迁移（每条都是单行 UPDATE/INSERT，WHERE 里带 generation 或 execution_id CAS）：**

| 语句 | 方法 | 迁移 | 关键谓词 |
|---|---|---|---|
| `beginPauseFencedSQL` / `beginPauseUnfencedSQL` | `BeginPause` | 任意态 → `publishing` | `INSERT ... ON CONFLICT DO UPDATE ... WHERE cluster_id = EXCLUDED.cluster_id [AND execution_id = EXCLUDED.execution_id]`；两个常量是同一语句的 fencing 开关两态，故意不用 `OR $flag` 折成一条 |
| `completePauseSQL` | `CompletePause` | `publishing` → `paused` | `WHERE generation = $2 AND state = 'publishing'` |
| `markLocalOnlySQL` | `MarkLocalOnly` | `publishing` → `local_only` | 同上 |
| `claimForResumeSQL` / `claimForResumeDurableOnlySQL` | `ClaimForResume` | `paused`→`resuming`，或 `publishing`/`local_only` 租约过期→`resuming` | `snapshot_id IS NOT NULL AND (state='paused' OR (state IN ('publishing','local_only') AND lease_expired))`；`running`/`resuming` 永不可通过此路径抢占 |
| `releaseClaimSQL` | `ReleaseClaim` | `resuming` → `paused` | `WHERE generation = $2 AND state = 'resuming'` |
| `markRunningFencedSQL` / `markRunningUnfencedSQL` | `MarkRunning` | `resuming`/`paused`/`publishing`/`local_only`/`running` → `running` | 三分支 CASE/WHERE，见 §5——**claimant（$2）与 holder（$7）两个身份**，是 Fix「双角色参数」已经拆开后的形态 |
| `renewSandboxDeadlineSQL` | `RenewSandboxDeadline` | `running` 行内更新（仅 `sandbox_expires_at`） | `WHERE state='running' AND execution_id=$2::uuid`——今天最新的一条（`1022b3a`），无身份谓词 |
| `removeSQL` | `Remove` | 任意态 → 删除 | `WHERE generation = $3` |

**Lease（续租，三条互不相同的语句，全部是「谁的心跳/身份」在续谁的租）：**

| 语句 | 方法 | 谁调用 | 覆盖状态 | 身份谓词 |
|---|---|---|---|---|
| `renewLeaseSQL` | `RenewLease` | 节点/api 副本自己上报「我持有这些」 | `running`/`publishing`/`local_only`（按 origin_node_id）、`resuming`（按 claimed_by_node_id） | `WHERE (state IN (...) AND origin_node_id=$5) OR (state='resuming' AND claimed_by_node_id=$5)`——$5 是调用者自称的身份，**Fix A 之前 api 副本传自己的 pod 名，running 臂永远打不中** |
| `renewParkedLeaseSQL` | `RenewParkedLeases` | `RunRegistryReconcile`（心跳驱动） | 仅 `publishing`/`local_only` | 调用方给一批 `(sandbox, node)`，语句自己核对 `origin_node_id = v.node_id` |
| `renewLiveLeaseSQL` | `RenewLiveLeases` | `RunRegistryReconcile`（心跳驱动，**Fix A 新增**） | 仅 `running` | 同上，覆盖 `renewLeaseSQL` 打不中的 running 臂 |

**Reclaim（回收，四条 + 两条计数）：**

| 语句 | 覆盖 | 双条件 |
|---|---|---|
| `reclaimReleasedRunningSQL` | `running`→`paused` | 租约过期 **且** `sandbox_expires_at < now()` |
| `reclaimReleasedResumingSQL` | `resuming`→`paused`（**Fix B 新增，拆出来的**） | 仅租约过期（`sandbox_expires_at` 首次 resume 从不写，NULL 永不匹配第二条件） |
| `reclaimDiscardedSQL` | `running`/`resuming` 且 `snapshot_id IS NULL` → 删除 | 租约过期 且 deadline 已过 |
| `countReclaimDiscardableSQL` / `countClusterRowsSQL` | `DiscardBreaker` 的分子/分母 | 熔断阈值：`MaxRows=10`、`MaxRatio=10%`、`MinRatioRows=3` |
| `releaseHoldingsReleasedSQL` / `releaseHoldingsDiscardedSQL` | `ReleaseNodeHoldings(nodeID)`：本机上一进程死掉时持有的行 | `liveHoldingsOfNode`：`(state='running' AND origin_node_id=$2) OR (state='resuming' AND claimed_by_node_id=$2)` |

**Grace（重启宽限期，独立于上面所有语句）：**

`extendLeasesSQL`——一次性把集群里**所有**行的 `lease_expires_at` 按推断停机时间**加性**前移，
详见 §6.3，这是唯一一个**不是**行级 CAS、而是**全表批量写**的语句，也是 N 副本下唯一真正
需要重新设计的一条。

### 1.4 gRPC 面（`services/api/proto/scheduler.proto`，`service PausedRegistry`）

五个 RPC：`GetSandboxes`（批量读，chunk=1000）、`TransitionSandbox`（覆盖
begin_pause/complete_pause/mark_local_only/mark_running/release_claim/remove/
renew_sandbox_deadline，`1022b3a` 新增了 `TRANSITION_KIND_RENEW_DEADLINE`）、
`AcquireSandbox`（= ClaimForResume）、`RenewNodeLease`、`ReleaseNodeHoldings`。
实现类 `PausedRegistryService`（`internal/registry_service.go`）。

调用方：Rust 侧 `src/orchestrator/paused_registry/central.rs`（`CentralPausedSandboxRegistry`，
2,621 行，`impl PausedSandboxRegistry`，`tonic` 客户端）。**搬进 `api` 之后这五个 RPC 的语义
全部变成同进程内的 trait 方法调用，RPC 帧本身消失**——但 trait 的方法签名（`mod.rs`）不必变，
因为它本来就是照 Go `Store` 接口逐方法对应设计的（`store.go` 顶部注释原话：「deliberately
mirrors the node-side Rust trait method for method」），只是反过来——现在轮到 Go 那套镜像 Rust。

---

## 2. PG schema

表 `paused_sandboxes`（DDL 见 `services/scheduler/internal/registry/migrate.go:30-96`）：

```
sandbox_id           UUID PRIMARY KEY
cluster_id           UUID NOT NULL
state                TEXT NOT NULL  CHECK (state IN ('publishing','paused','resuming','local_only','running'))
generation           BIGINT NOT NULL
origin_node_id       TEXT NOT NULL   -- 持有本地字节的机器；resuming 期间仍指旧持有者
claimed_by_node_id   TEXT            -- 仅 resuming 期间非空：谁在做这次 resume
snapshot_id          UUID            -- 仅 paused 时非空
metadata             JSONB NOT NULL
paused_at            TIMESTAMPTZ NOT NULL
updated_at           TIMESTAMPTZ NOT NULL
lease_expires_at     TIMESTAMPTZ
sandbox_expires_at   TIMESTAMPTZ     -- running 用户设置的寿命上界；resuming 从不写（Fix B 的病根）
execution_id         UUID            -- CHECK: 非空 ⟺ state ∈ (running, publishing, resuming)
execution_started_at TIMESTAMPTZ     -- 非空 ⟺ execution_id 非空
```

约束：`paused_sandboxes_state_check`（五态）、`paused_sandboxes_execution_check`（身份轴，
`(state IN (running,publishing,resuming)) = (execution_id IS NOT NULL)`）。

索引：`paused_sandboxes_origin_node_idx`、`paused_sandboxes_updated_at_idx`、
`paused_sandboxes_reclaim_idx`（partial，`state IN (running,resuming) AND sandbox_expires_at
IS NOT NULL`，服务两条 reclaim 语句）、`paused_sandboxes_resuming_reclaim_idx`（partial，
`state='resuming'`，**Fix B 新增**，专门服务 `reclaimReleasedResumingSQL` 因为 stuck 的
resuming 行 `sandbox_expires_at` 恰恰是 NULL、走不进上一条索引）。

状态机（五态）：

```
            begin_pause                complete_pause
  (any) ────────────────► publishing ───────────────► paused
                              │  mark_local_only          │ claim_for_resume
                              ▼                            ▼
                          local_only ──claim_for_resume──► resuming ──mark_running──► running
                              ▲                                                          │
                              └──────────── reclaim（租约过期，双条件）───────────────────┘
                                            release_claim → paused
                                            reclaim（resuming：仅租约过期）→ paused
                                            release_node_holdings → paused（有快照）/删除（无快照）
```

Migration 文件：仅 `migrate.go` 一处，`Migrate()` 用 `pg_advisory_lock(0x0A6E_7653_4348_4D41)`
串行化并发建表/加约束，`preflight()` 在加约束前先扫描存量违规行并拒绝自动回填
（要求人工 `DROP TABLE` 走 runbook）。**这套已经是 N-controller 安全的**（注释原话：
「two controllers rolling over each other are the pair that has to serialise now」），
port 到 Rust 时原样照抄这个模式，不要重新设计。

---

## 3. Rust 侧现状

`src/orchestrator/paused_registry/`：

| 文件 | 行数 | 现状 |
|---|---:|---|
| `mod.rs` | 880 | `PausedSandboxRegistry` trait（14 个方法，逐方法对应 Go `Store` 接口）+ `PausedRegistryError`（已完整覆盖 Go 的错误分类：`Backend`/`InvalidRecord`/`GenerationConflict`/`ExecutionFenced`）+ `build_paused_registry()`（读 `PausedRegistryBackendKind`，`Local`→disabled，`Central`→gRPC，**`Postgres`→ 直接 `bail!`，见下**） |
| `central.rs` | 2,621 | `CentralPausedSandboxRegistry`：`tonic` 客户端，实现 trait，把每个方法转成一次 `PausedRegistry` gRPC 调用。**这是回退路径，保持不动**（硬约束 1） |
| `disabled.rs` | 117 | 空实现，`--role node` 恒用它 |
| `types.rs` | 284 | `PausedSandboxEntry`/`PausedRegistryState`/`ResumeClaim`/`MarkRunningOutcome`/`DeadlineRenewalOutcome`/`ConflictReason` 等——已经是 Go `Entry`/`State` 的镜像 |

`src/cfg.rs:597-620`：`PausedRegistryBackendKind::Postgres` **已经是保留名**——doc comment 原话
「Removed. Kept as a name so a deployment still carrying it is told what to do instead of being
told the word is unknown」，`build_paused_registry` 里对应分支直接 `bail!("...has been removed:
the node no longer connects to the registry database...")`。这是 D11（`_impl-D11-pg-removal.md`）
从 `--role node`/前代码里删掉的旧直连行为的遗留名。

**这是 Stage C 的天然落点**：`Postgres` 这个 enum 值不需要新造，只需要把 `build_paused_registry`
里那个 `bail!` 分支换成真正构造一个新的 `PostgresPausedSandboxRegistry`（新文件，例如
`src/orchestrator/paused_registry/postgres.rs`）。语义上也说得通——旧 `Postgres` 变体原本描述
「进程直连 registry 数据库」，只是原来那个进程是 `node`（D11 判定不安全，删除），现在换成 `api`
（Stage C 判定安全，因为 api 本来就不运行用户代码）。

调用点：`src/bin/server.rs` 只有 `assemble_all`（1 副本）和 `assemble_api`（N 副本）会调
`build_paused_registry`；`assemble_node` 从不调，恒用 `DisabledPausedSandboxRegistry`（硬编码，
不读配置）——这一点不受 Stage C 影响。

**上层消费者**（不在 4,191 行范围内，但要一起看）：

- `src/api/impls/paused_coordinator.rs`（2,933 行）——`PausedSandboxPublisher`：orchestrator 驱动
  的发布路径，调用 trait 的 `begin_pause`/`complete_pause`/`mark_local_only`/`mark_running`/
  `renew_deadline`。**不需要改**，因为它只认 trait，不认后端。
- `src/api/impls/paused_recovery.rs`（2,905 行）——跨节点 resume + 本机记录对账：
  `renew_paused_leases`（Fix A 的 Rust 侧调用点，见 §5）、`release_stale_node_holdings`、
  `reconcile_local_records`、`retry_stale_node_holdings_release`。**不需要改**，同理。
- `src/node_client/paused_state.rs`（218 行）——resume 路由用的 `RemotePausedState` 值类型，
  与后端无关。**不需要改**。

`mark_running` 的双身份参数——trait 签名已经是
`mark_running(&self, sandbox_id, node_id: &str, holder_node_id: &str, execution_id, expires_at)`
（`mod.rs:275-282`）——**在 Rust 侧已经是拆开的两参数**，与 Go `259d0de` 拆分后的形态一致。
详见 §5。

---

## 4. PG 客户端选型

`Cargo.toml` 与全部 `src/*.rs`：`sqlx`、`tokio-postgres`、`deadpool` **零命中**。工作区目前
没有任何 PG 客户端依赖。

Stage B（`internal/catalog`，3,613 行，Go 侧与 registry 共享同一 `pgxpool.Pool`）目前也**没有**
对应的 Rust 落点（`src/snapshot/repository/backends/central/` 是走 gRPC 的旧客户端，不是 PG
直连）。也就是说，**截至本次调研，Stage B 尚未在 Rust 侧引入 PG 客户端**——Stage C 如果先落地，
它的选型就是事实上的工作区标准，Stage B 落地时必须复用，不能各自选一个。

**建议**：`sqlx`（`postgres` + `runtime-tokio` features）。理由：
1. 本仓库全异步、Tokio 原生，`sqlx` 与 `tokio-postgres` 相比省掉一层连接池自己搭（`sqlx::PgPool`
   内置），`deadpool-postgres` 则是给 `tokio-postgres` 补连接池但仍要手写更多样板。
2. 本迁移的所有语句都是**静态字符串常量**（照抄 Go 的写法），不依赖 `sqlx::query!` 的编译期
   校验（宏需要连一个真实数据库做类型推断，这在 CI 里是额外负担）——用 `sqlx::query(...)`/
   `query_as(...)` 的运行时变体即可，不强制上 `sqlx::query!` 宏。
3. `Cargo.toml` 里已有 `chrono`、`uuid`、`serde_json`，`sqlx` 对三者都有官方 feature（
   `chrono`、`uuid`、`json`），类型映射不需要额外转换层。

**必须与 Stage B 协调的一点**：Go 侧 `catalog.NewStoreWithPool(registryWriter.Pool(), ...)`
让 catalog 和 registry 共享**同一个连接池**，进而共享**同一个事务**（`BeginPauseTx` 之类，
见 §10 风险 1）。Rust 侧选 `sqlx::PgPool` 之后，这个池必须是 Stage B/C 两边都能拿到同一个
`Arc<PgPool>`（或等价的共享句柄）的东西——具体是挂在 `AppConfig`/`assemble_api` 里造一次
再传给两边，还是各自 `PgPool::connect` 再指向同一个 DSN（两个池，牺牲跨表事务），是一个
必须显式决策、而不是各自实现时顺手决定的问题。本文档建议前者（单一共享池），因为后者会
直接丢掉 §10 风险 1 描述的原子性。

配置新增（在 `PausedRegistryConfig` 下加字段，`#[config(nested)]` 的禁令只管 `[backend.oss]`/
`[backend.posix_fs]` 那层，`PausedRegistryConfig.backend` 今天已经在用 `env =`，所以新增字段
同样可以用 `env =`）：至少需要 `postgres_dsn: Option<String>`（对应 Go 的
`SCHEDULER_REGISTRY_DSN`，建议命名 `AENV_PAUSED_REGISTRY_POSTGRES_DSN`，与已有的
`AENV_PAUSED_REGISTRY_BACKEND` 同族）、连接池大小（Go 默认 8）、语句超时（Go 默认 30s）。

---

## 5. A/B 两个修复 + `mark_running` 双角色参数：怎么处理

### Fix A（`151d00b`，running 行租约续期）

**建议：不要移植"坏了再修"的历史，直接按修复后的最终形态实现。** 具体说：Rust 侧的
`PostgresPausedSandboxRegistry`（新后端）应该从第一天起就提供 `renew_live_leases` 方法
（对应 Go `RenewLiveLeases`/`renewLiveLeaseSQL`），并且 `RunRegistryReconcile`
的 Rust 版本（见 §9 施工清单）从第一天起就同时调用「续 publishing/local_only 租约」和
「续 running 租约」两条路径。`src/api/impls/paused_recovery.rs::renew_paused_leases`
（Rust 侧 Fix A 涉及的调用点）**本身不需要改**——它调用的 `registry().renew_lease(...)`
对应 `renewLeaseSQL`，这条语句本来就只覆盖调用者自己身份下的行（`resuming` 用
`claimed_by_node_id`，其余用 `origin_node_id`），在 api 副本身份下这条对 `resuming` 行仍然
正确（api 副本自己 claim 的行，`claimed_by_node_id` 就是它自己）。running 行的续租从来不该
靠这条路径——Fix A 的本质是"新增一条心跳驱动的路径"，不是"修另一条路径的 bug"，所以 Rust 移植
不存在"先复现 bug 再修"的必要，直接实现两条路径即可。

### Fix B（`7335219`，resuming 行永久搁浅）

**同样建议直接实现最终形态**：`reclaim_released_running` 与 `reclaim_released_resuming` 从
第一天起就是两条独立语句（对应两个独立的 Rust 方法或者内部两条 SQL 常量），resuming 分支只
凭租约过期，不要求 `sandbox_expires_at`。这不是"可选的优化"，是**正确性要求**——如果 Rust
移植时图省事把两态合并成一条 `WHERE state IN ('running','resuming') AND lease_expired AND
sandbox_expires_at < now()`（即 Fix B 之前的 Go 形态），会立刻重新引入"首次 resume 的 claim
方进程死掉、后继 api 副本永远不来"这个已知死锁——而这个死锁在 N 副本 `api` 场景下**比 Go
单实例场景更容易触发**，因为 `claimed_by_node_id` 存的是 api Pod 名，Deployment 滚动后旧
Pod 名永远不会再出现（这一点 Go commit message 自己点破：「a Kubernetes Deployment pod name
never is again after a reschedule」）。

配套的 `paused_sandboxes_resuming_reclaim_idx` partial index 也要在 Rust 侧的 schema
bootstrap 里一起建（不是 Go 独有的优化，索引服务的是"stuck resuming 行"这个查询模式，
Rust 后端跑同一张表、同一批查询，同样需要）。

### `mark_running` 双角色参数

**已经拆开，不是待修复项**——Go 侧 `259d0de`「split mark_running's identity into a claimant
guard and a holder write」已经把单一 `$2` 拆成 claimant（`$2`，比较，guard）和 holder（`$7`，
只写不比较，除了 branch ③ 的一处例外，见 `store_postgres.go:1196` 附近大段注释）。Rust 侧
trait 签名 `mod.rs:275-282` 已经镜像了这个拆分（`node_id: &str` + `holder_node_id: &str`
两个独立参数）。**Stage C 的任务是移植时不要把这两个参数重新揉回一个**——具体来说，新的
`PostgresPausedSandboxRegistry::mark_running` 实现必须原样保留 `markRunningFencedSQL` 的
三分支 WHERE（① `resuming AND claimed_by_node_id=$2 AND execution_id=$6`，② `paused/
publishing/local_only AND origin_node_id=$2 AND claimed_by_node_id IS NULL`，③ `running AND
origin_node_id=$7 AND execution_id=$6`——**唯一一处比较 holder 而非 claimant 的分支**，
原因是同一次成功 resume 上，orchestrator 自己的 mark_running 和 resume 路径的幂等跟随调用
会对同一行写两次，第二次落地时 origin_node_id 已经被第一次改成了 holder，若仍按 claimant
比较，每一次跨节点 resume 的第二次写都会失败）。这段逻辑复杂、注释比代码长，**建议直接照抄
SQL 文本本身**（含注释），不要凭理解重写谓词——凭理解重写正是这一条历史上出错的方式。

### `reconcile.go` 的监控缺口

`strandedRows`/`parkedLeaseExpiring` 只数 `publishing`/`local_only`，不数 `running`（Fix A
之后 running 行有独立的续租路径和独立的 `liveLeaseLapsed`/`liveDeadlinePassed`，但缺口具体是
"这两个聚合指标没有把 running 也算进同一口径"——本次调研未在 `reconcile.go` 里找到已经排期修
这个的记录）。**建议在移植时一并补上**：这不是新功能，是把已经算出来的 `liveLeaseLapsed`/
`liveDeadlinePassed`（`computeRegistryReconcile` 已经在算，只是没有并进 `strandedRows`/
`parkedLeaseExpiring` 那两个聚合指标）在 Rust 版本里从一开始就用统一口径暴露，成本接近零，
拖到之后修等于要再读一遍这段逻辑。

---

## 6. N 副本并发下每条 SQL 谓词的正确性审查（最重要的一节）

### 6.1 背景：今天的单实例假设

`cmd/main.go` 里创建写存储的注释：

> Never on a query-only replica. Those exist so sandbox lookups survive a primary restart;
> **there is exactly one owner of this table's shape and of the reclamation timer that
> deletes rows from it**, and a replica that migrated and reclaimed alongside the primary
> would be a second one.

`go svc.RunRegistryReconcile(...)` 和 `go registrySvc.RunReclaim(...)` 都是**无条件**
`go func`——没有选主，因为今天只有一个进程会执行到这两行。这个假设在搬进 N 副本 `api` 之后
不再成立，是本节要处理的核心问题。

### 6.2 逐语句审查

| 语句/操作 | 触发方 | N 副本下是否安全 | 理由 |
|---|---|:---:|---|
| `beginPauseFencedSQL`/`Unfenced` | 单次 API 请求 | ✅ 安全 | `INSERT ... ON CONFLICT DO UPDATE ... WHERE cluster_id=... [AND execution_id=...]`，PG 对同一行的并发 upsert 天然串行化，第二个到达的要么因 execution_id 不匹配报 0 行、要么等第一个提交后重新评估 WHERE |
| `completePauseSQL`/`markLocalOnlySQL` | 单次 API 请求 | ✅ 安全 | `generation` CAS，行锁序列化 |
| `claimForResumeSQL`/`DurableOnlySQL` | 单次 API 请求 | ✅ 安全（但见下方"选哪条语句"的警告） | 同上，`snapshot_id IS NOT NULL AND (state=...)` |
| `releaseClaimSQL` | 单次 API 请求 | ✅ 安全 | `generation` CAS |
| `markRunningFencedSQL`/`Unfenced` | 单次 API 请求 | ✅ 安全 | 三分支 WHERE 已经是为"两个不同身份的调用者可能对同一行写两次"设计的（见 §5），本来就是为并发场景写的谓词 |
| `renewSandboxDeadlineSQL` | 单次 API 请求（POST /timeout） | ✅ 安全 | 无身份谓词，`execution_id` CAS 即可，多个副本处理不同请求天然无冲突 |
| `removeSQL` | 单次 API 请求 | ✅ 安全 | `generation` CAS |
| `renewLeaseSQL` | 副本自己的心跳/记录对账循环，携带**自己的身份** | ✅ 安全 | 每个副本只能续到 `origin_node_id`/`claimed_by_node_id` 等于自己身份的行——天然按身份分区，不会有两个副本抢续同一行的租约（除非两个副本谎报同一身份，这不在信任模型内） |
| `renewParkedLeaseSQL`/`renewLiveLeaseSQL` | **`RunRegistryReconcile` 定时器** | ⚠️ 单条语句本身安全，**循环本身不该 N 份跑** | UPDATE 本身是幂等的行级 CAS（两个副本同一 tick 都执行，先提交的生效，后到的 0 行受影响，不会产生错误结果）。但 N 副本各自跑一遍`computeRegistryReconcile`（对全表 + 全部 roster 做比对，O(行数)）是 N 倍数据库读 + N 倍 CPU，且每个副本各自的 Prometheus 指标（`recordRegistryReconcile`）会重复上报，讲述"发生了 N 次"而不是"发生了一次" |
| `reclaimReleasedRunningSQL`/`reclaimReleasedResumingSQL`/`reclaimDiscardedSQL` | **`RunReclaim` 定时器** | ⚠️ 单条语句本身安全，**循环本身不该 N 份跑** | 同上，UPDATE/DELETE 本身幂等；但 `DiscardBreaker` 的"数候选/数总数/决定放不放行"这三步是**非原子的三步判断**，N 个副本各自独立做这个判断——不会产生错误删除（每一步仍然是行级 CAS 保护），但 `registryReclaimBreakerTripped` 计数器可能为同一次"确实该跳闸"的事件重复递增 N 次，误导告警的严重程度判断 |
| `releaseHoldingsReleasedSQL`/`releaseHoldingsDiscardedSQL` | 副本自己启动时，用**自己的身份** | ✅ 安全，但对 `running` 行**从不匹配** | `liveHoldingsOfNode` 的 running 分支要求 `origin_node_id=$2`（调用者自己），而 `origin_node_id` 对 running 行永远是真实机器 ID，从不是 api Pod 名——所以这条语句对 api 副本而言实际只对自己曾经 claim 过的 resuming 行有效，是 Fix B 之外的"锦上添花"路径而非主要安全网 |
| `Migrate()` / `pg_advisory_lock(schemaLockKey)` | 每个副本自己启动时 | ✅ **已经是 N 副本安全的** | 本来就是为"两个控制器同时滚动"设计的，原样照抄 |
| `ExtendLeases`/`Grace.Enter` | 每个副本自己启动时 | 🔴 **不安全，需要重新设计** | 见 §6.3 |

### 6.3 唯一需要重新设计的一条：`Grace`/`ExtendLeases`

`extendLeasesSQL` 是**加性**的：`lease_expires_at = COALESCE(lease_expires_at, updated_at) +
downtime + ttl`，`downtime` 是从 `now() - max(updated_at)` 推断出来的、这次"停机"多久。
设计意图是"这个唯一的写入进程重启了，它自己造成的空档期不该被算进任何节点的失联时长"。

在 N 副本模型下，如果每个副本各自在自己的进程启动时跑一遍 `Grace.Enter`（照抄现状），
会有两个问题：

1. **多副本同时启动时的可加性叠加**：`updated_at` 这一列在 `ExtendLeases` 里被故意不写
   （注释："it is the evidence this pass reads to infer the downtime, and overwriting it...
   would leave the next restart measuring its outage against this one's clean-up"）。
   于是副本 A 先跑，推断出停机 D 并把每行租约加上 `D+ttl`；副本 B 紧接着跑（`updated_at`
   没变，因为 A 没碰它），**推断出几乎同样的停机 D**，再加一次 `D+ttl`——N 个副本同时冷启动
   （初次部署、或一次触及全部副本的滚动重启）会让每行租约被叠加 N 次。方向是保守的（租约
   被推得更远，不会提前失效，不会导致误抢占），但会让"这个宽限机制到底在保护什么"失去意义，
   而且没有上界——重复的滚动发布会让某些行的租约值持续膨胀。
2. **"谁的停机"这个问题本身在 N 副本下问法就变了**。单实例时"这个进程的停机"就是"这张表的
   写权停摆了多久"，两者是一回事。N 副本时，只要**至少一个副本**在正常跑定时器，写权就没有
   真正停摆——个别副本的重启不该触发全局宽限。

**建议**：把 `Grace.Enter` 从"每个副本自己启动时跑"改成"**新当选为定时任务 leader 时跑**"
（见 §6.4），即与 §6.4 的选主机制绑定成一个状态转换：一个副本获得 leader advisory lock 后，
先跑一次 `ExtendLeases`（此时它才代表"这张表的写权刚刚回来"这个语义），再开始跑
`RunRegistryReconcile`/`RunReclaim` 的循环。非 leader 副本完全不跑 `Grace.Enter`。

这个改动同时解决了叠加问题（同一时刻只有一个 leader 在跑）和语义问题（"谁的停机"变成
"leader 交接的空档"，这才是 N 副本下这张表写权真正可能出现空档的地方）。

### 6.4 选主：用什么

**建议：PostgreSQL advisory lock（`pg_try_advisory_lock`），不是 Redis 分布式锁。**

理由：
1. **本仓库已有这个精确的先例**：`migrate.go` 的 `schemaLockKey`（`pg_advisory_lock`/
   `pg_advisory_unlock`，session-scoped）就是为"N 个控制器同时启动，谁先跑 DDL"这个问题
   设计的，只是它是一次性的（跑完 DDL 就释放），而选主要长期持有。机制原理相同，不需要
   引入新的分布式锁依赖（Redis、etcd、k8s Lease 都要新增依赖或新增 RBAC）。
2. **advisory lock 是 session 作用域的，连接一断自动释放**——比 Redis 锁更适合"leader 挂了
   要让别人接班"这个场景：不需要 TTL、不需要续租心跳、不需要处理"锁过期了但持有者还活着"
   的经典分布式锁难题（Redis 锁必须靠续租 goroutine + 网络分区下的误判处理，PG advisory
   lock 靠 TCP 连接本身的生死，出问题时行为更好预测）。
3. **实现模式**：leader 候选用一个**独立的、不进 `sqlx::PgPool` 复用池**的长连接（advisory
   lock 是 session 级的，进池子里会被随意复用的连接抢走语义就乱了——这也是 Go
   `migrate.go` 里特意从 `pool.Acquire` 拿一个"钉住"的连接、而不是直接 `pool.Exec` 的原因，
   见 `migrate.go:204-213` 的注释）。每个副本启动时用这个专用连接尝试
   `pg_try_advisory_lock(leaderKey)`（非阻塞，立即返回成败）；成功的副本跑
   `Grace.Enter` → `RunRegistryReconcile`/`RunReclaim`；失败的副本每隔一小段时间
   （例如与 reconcile interval 同量级）重试一次抢锁，用于原 leader 挂掉后的接班。
4. `leaderKey` 与 `schemaLockKey` 一样，取一个固定的 64 位常量，两者必须不同（不能复用
   同一个 key，否则"建表锁"和"选主锁"会互相阻塞）。

**不需要选主的部分**：所有单次 API 请求触发的写（§6.2 表格里标 ✅ 的那些）继续按请求到达
哪个副本就由哪个副本执行，**不经过 leader**——这些语句的正确性从来不依赖"只有一个进程在写"，
是靠 SQL 本身的 CAS 谓词保证的，这正是 `store_postgres.go` 顶部注释说的：「Every mutating
statement carries its own precondition in the WHERE clause, so two callers racing on the
same sandbox resolve through the database rather than through application-level locking」。
选主只管两件事：`RunRegistryReconcile`/`RunReclaim` 两个定时器循环，以及绑定在其上的
`Grace.Enter`。

---

## 7. 已知坑（本次调研新发现，非 A/B/mark_running 之外的）

1. **两套并行的读模型**。`store.go` 的 `Entry`/`State`（写路径用，14 列，被
   `entryColumns` 常量选出）与 `registry.go`+`postgres.go` 的 `Sandbox`/`State`
   （只读路径用，`selectColumns` 常量选出，多两列 lease 字段但少 `metadata`）是**两份独立
   维护的列表**，`postgres.go:32-36` 自己的注释承认："Adding a column to one and not the
   other does not fail: it makes that column read as empty on whichever paths use the list
   that was missed"。移植到 Rust 时，如果直接调用方（trait 内部）本来就该统一成一套读模型，
   建议借这次机会把两套合一，而不是照搬两份并行常量——但要注意 Go 侧只读路径的
   `SET default_transaction_read_only=on` 这个防御在 Rust 侧完全不需要了（api 本来就是
   合法写方），不要把这个约束也搬过去。
2. **`ObserveGenerationTx` 只在 catalog 事务内使用**，是 catalog 写冲突后重读 generation
   用的辅助方法，如果 Stage C 先于 Stage B 落地，这个方法在 Rust 侧暂时没有调用方——不算错，
   但施工清单里不要漏掉它（trait 完整性要求）。
3. `checkRenewalCadence`（`registry_service.go:504`）在 Go 侧是 RPC 层的"节点上报的 lease TTL
   / renewal interval 与服务器配置不一致时警告"逻辑，属于 gRPC transport 层的校验，**搬进
   同进程调用后这类校验大概率不再有意义**（不再是两个独立配置的进程互相校验，是同一个配置
   来源）——建议在施工清单里明确标注为丢弃，而不是遗漏后才发现找不到对应位置。

---

## 8. 测试面

Go 侧：`internal/registry` 的测试**没有数据库会 skip 而不是 fail**——`legacy_schema_test.go`
之外的 9 个测试文件、86+36+... 上百个 `TestXxx`，必须用 `make -C services test-with-postgres`
（起一个 Docker 里的一次性 PG，端口 15499）才会真正跑。`contract_test.go`/
`contract_claim_test.go`/`contract_lease_test.go`（合计 86 个测试）是"一套断言，多个场景"
的模式，专门覆盖 claim/lease 语义；`execution_fencing_test.go`（36 个）专门覆盖身份轴；
`store_postgres_test.go`（86 个）是逐方法的行为测试。

**Rust 侧建议直接照抄 `src/orchestrator/store/` 的两个既有模式**（`CLAUDE.md` 已经把这个
模式点名为可复用的）：

1. **契约测试**（对应 `src/orchestrator/store/contract.rs`）：把 Go 的 `contract_*_test.go`
   翻译成一组 `pub(crate) async fn xxx<R: PausedSandboxRegistry>(registry: &R)` 纯断言函数，
   新的 `PostgresPausedSandboxRegistry` 跑全套；`DisabledPausedSandboxRegistry`
   和 `CentralPausedSandboxRegistry`（回退路径）如果语义上也该满足同一份契约，可以视情况
   决定是否也跑（`central.rs` 本身通过 gRPC 转发到 Go 实现，理论上应该已经满足，跑一遍
   是双重保险但需要一个真实的 scheduler+PG 才能跑，成本另算）。
2. **硬依赖门禁**（对应 `src/orchestrator/store/redis/harness.rs` +
   `AENV_REDIS_TEST_REQUIRED=1` + `make test-with-redis`）：新增
   `AENV_PG_TEST_REQUIRED=1`（命名待定，需与是否已有 Stage B 引入的等价开关协调，避免
   一个仓库里出现两个名字不同、语义相同的"没有 PG 就报错"开关）+ 一个新的 Makefile 目标
   （建议直接复用 `services/Makefile` 里已经写好的一次性 PG 容器配方——同一个数据库、
   同一张表，没有理由起两套），跑 `cargo test -p agentenv --lib orchestrator::paused_registry::`。
   `make test-unit` 是否要把这个也纳入强制项，参照 `AENV_REDIS_TEST_REQUIRED` 今天在
   `make test-unit` 里的地位（`CLAUDE.md`："Anything that changes `InMemoryMetadataStore`
   has to run it"）——这里对应的不变量是"改了 `PostgresPausedSandboxRegistry` 就必须跑
   Postgres 契约测试"，理由类比：`DisabledPausedSandboxRegistry`/`CentralPausedSandboxRegistry`
   与新 `PostgresPausedSandboxRegistry` 是同一份契约的三个实现，改一个忘了跑另一个的风险与
   `InMemoryMetadataStore`/Redis 的风险完全同构。

---

## 9. 分步施工清单（每步独立编译通过、独立可回退）

1. **Schema + 迁移**：新增 `src/orchestrator/paused_registry/schema.rs`（或类似），照抄
   `migrate.go` 的 `SchemaDDL`/`preflight`/`Migrate`（advisory lock 模式原样保留）。不接入
   任何调用方，只保证独立编译 + 单元测试（可以针对一次性 PG 跑"建表两次不报错"这类测试）。
2. **只读路径**：新增 `PostgresPausedSandboxRegistry::get`/`get_many`（`entryColumns` 对应的
   14 列，§7 坑 1 提到的读模型统一在这一步做）。不接入 `build_paused_registry`，只有单元测试
   覆盖。可独立回退（删文件即可，无调用方）。
3. **Fencing 写路径**：`begin_pause`/`complete_pause`/`mark_local_only`/`claim_for_resume`/
   `release_claim`/`mark_running`/`renew_sandbox_deadline`/`remove`——按 §1.3 表格逐条照抄
   SQL 文本，契约测试（§8）跟着这一步一起写，每加一个方法就跑一遍对应契约测试。这是最大的
   一步，可以再拆成"7 个方法各自一个可独立回退的子提交"。
4. **Lease 写路径**：`renew_lease`/`renew_parked_leases`/`renew_live_leases`——Fix A 的形态
   直接实现，不复现历史 bug（§5）。
5. **Reclaim 路径**：`reclaim_expired_holdings`（拆开的 running/resuming 两条，Fix B 形态直接
   实现）、`release_node_holdings`、`DiscardBreaker` 等价物。
6. **Grace + 选主**：`ExtendLeases` 等价物、advisory-lock leader election（§6.4）、
   `RunRegistryReconcile`/`RunReclaim` 等价的 Rust 后台任务，绑定 leader 状态转换（§6.3）。
   这一步依赖 Stage A 已经把节点心跳 roster 搬进 `api`（`computeRegistryReconcile` 需要
   `s.nodes.RostersInCluster()` 等价数据源）——**如果 Stage A 尚未落地，这一步要么阻塞，
   要么先接一个"读 Go scheduler 的心跳数据"的临时桥接**，需要与 Stage A 的实际进度对齐后
   再排期，本文档不替 Stage A 的状态做假设。
7. **接入 `build_paused_registry`**：把 `PausedRegistryBackendKind::Postgres` 的 `bail!`
   换成真正构造第 1-6 步的 `PostgresPausedSandboxRegistry`。到这一步为止，`Central`/`Local`
   两个既有分支保持字节不动——切换只是新增一个可选分支，`--role all` 和 `--role api` 默认
   仍然读 `AENV_PAUSED_REGISTRY_BACKEND`（默认值 `local`，见 `cfg.rs:711`，改哪个集群用
   新后端是运维操作，不是代码变更）。
8. **配置项**：`postgres_dsn` 等新字段接入 `PausedRegistryConfig`（§4）。
9. **测试门禁**：`AENV_PG_TEST_REQUIRED=1` + 新 Makefile 目标接入 `make test-unit`（§8）。
10. **集群验证**：在 dev 集群上把某个 `api` 副本切到 `AENV_PAUSED_REGISTRY_BACKEND=postgres`，
    观察多副本下 leader 选举、reconcile/reclaim 指标是否符合预期（不再是 N 倍），A/B 两个
    修复对应的场景（running 行租约续期、首次 resume 卡住）跑一遍集群级验证——**这是任务里
    提到的"还没有集群验证记录"要在这一步补上**，而不是假设 Rust 移植后自动继承 Go 侧已经
    做过的验证。
11. **退役开关**：确认 `Central` 分支仍可用（硬约束 1），`services/scheduler` 二进制保留一个
    release 不删除。

每一步都不改 `Central`/`Local` 分支的现有行为，`--role all` 字节等价性（硬约束 2）全程不受
影响，因为整个改动只发生在 `build_paused_registry` 内部新增一个 `match` 分支，不触碰
`assemble_all`/`spawn_grpc_surface` 的现有文本。

---

## 10. 这次不做什么

- **不碰 catalog（Stage B）本身**——但见下方风险 1，Stage C 的施工顺序必须考虑 Stage B 的
  落地状态，而不是假装两者无关。
- **不在 Redis 上重建 claim/lease/fencing/reclaim**——用户已裁决，本文档全程假设这一点。
- **不改 `--role node`**——node 恒用 `DisabledPausedSandboxRegistry`，不读
  `PausedRegistryBackendKind` 配置，这一点 Stage C 不涉及。
- **不删除或修改 `central.rs`（gRPC 客户端）与 `services/scheduler` 的
  `PausedRegistry`/`SnapshotCatalog` gRPC 服务**——保留作为回退路径（硬约束 1），退役是
  Stage E 的事。
- **不在这一阶段重新设计 `reconcile.go` 的完整指标体系**——§5 提到的"补 running 到
  strandedRows/parkedLeaseExpiring 口径"是顺手做，但指标命名/维度的整体重新设计（是否要
  从 `agentenv_scheduler_registry_*` 改名成 `agentenv_api_registry_*`）留给施工时的实际
  PR 讨论，本文档不替这个决定拍板。
- **不解决"Deployment Pod 名不稳定导致 `release_node_holdings` 覆盖面有限"这个结构性问题**
  ——§6.2 已指出这条语句对 running 行从不生效、对 resuming 行是 Fix B 之外的锦上添花路径，
  这是 Kubernetes 身份模型本身的限制，不是这次移植能修的，继续依赖 Fix B 的租约兜底。

---

## 11. 待确认（未能核实，标注待确认，不要采信为结论）

- **Stage A（节点清册/心跳 roster）在本次调研时的实际落地进度**——`computeRegistryReconcile`
  依赖的心跳 roster 数据源今天在 Go `scheduler` 里（`s.nodes.RostersInCluster`），Rust
  `api` 是否已经有等价的、可供 Stage C 直接调用的心跳 roster——本次调研只读了代码结构，
  没有找到确凿证据说明 Stage A 是否已经完整落地到可用状态。施工清单第 6 步显式标注了这个
  依赖，但具体排期需要向 Stage A 的负责人确认。
- **Stage B（catalog）是否已经在排期上先于 Stage C**——本文档按用户给出的 Stage 顺序
  A→B→C→D→E 假设 Stage B 会先/同时落地 Rust，但代码库里目前没有任何 Rust 侧 catalog PG
  代码的痕迹，无法确认这是"尚未开始"还是"另有安排、不走这个顺序"。§10 风险 1（PG 事务
  跨进程原子性丢失）的严重程度直接取决于这一点，需要与 Stage B 负责人对齐后才能定案是否
  要求"Stage C 必须等 Stage B"这个硬性排期约束，还是可以接受一个有文档记录的临时降级窗口。
- **`reconcile.go` 的监控缺口（strandedRows/parkedLeaseExpiring 不含 running）是否已经有
  单独的 issue/任务在跟踪**——本次调研在 `reconcile.go` 源码注释里没有找到明确的"待办"标记
  （不同于 `store_postgres.go:1055` 那种明确写了 `KillOrphan` 待办的注释），只能确认现状
  如此，无法确认是否已经有人计划要修，还是这次调研是第一次发现。
- **`AENV_PG_TEST_REQUIRED`（或类似名字）是否应该与 Stage B 共用同一个环境变量名**——如果
  Stage B 先落地并已经定义了自己的门禁变量名，Stage C 应该复用而不是新造一个；本次调研没有
  找到 Stage B 侧的先例可以核实。
- **选主用的 advisory lock key 常量值**——§6.4 建议新增一个独立于 `schemaLockKey`
  （`0x0A6E_7653_4348_4D41`）的常量，具体取值本文档未指定，留给实现时选定（只要求与
  `schemaLockKey` 不同，避免互相阻塞）。
