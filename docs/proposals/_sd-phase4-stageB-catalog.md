# Phase 4 / Stage B：catalog 搬进 `--role api`（含拆双写并入）

调研快照：`dev` @ `1022b3a`（2026-08-25）。只读调研，无代码改动。

---

## 0. 结论摘要

- **范围**：`services/scheduler/internal/catalog/` 非测试代码 **3,613 行**，与
  `docs/proposals/2026-08-20-service-decomposition.md` §7 阶段4 v7 注记的数字**精确一致**（逐文件核对见 §1）。
  但这不是 Stage B 的全部边界：gRPC 转译层 `services/scheduler/internal/catalog_service.go`
  另有 **975 行非测试代码**（9 个 RPC），不在 3,613 这个数字里，却是同一次迁移必须一起处理的部分——不搬这层，Go 侧катalog RPC 服务端就没法退役。
- **最大的意外发现**：Go 侧为「暂停即时提交快照」设计的跨表原子事务机制
  （`PausedHalf` / `catalog_tx.go`，把 `snapshots` 行和 `paused_sandboxes` 行绑进同一个
  `pgx.Tx`）**从未被 Rust 客户端实际使用**——`CentralSnapshotCatalog`
  （`src/snapshot/repository/backends/central/mod.rs`）在三处调用点全部把
  `paused_transition` 硬编码成 `None`，代码注释明说「这批不做，谁把 pause 路径接上谁来设」。
  今天暂停快照的落地路径是 `src/api/impls/paused_coordinator.rs` 里两次**独立**、
  靠 generation 防护 + 失败重读兜底的 RPC（`registry.begin_pause` → `snapshot_manager.publish_captured`
  → `registry.complete_pause`），不是一次数据库事务。**这意味着 Stage B 不需要把
  `PausedHalf` 那套机制搬进 Rust**——可以整体丢弃，用 Rust 已有的、经过验证的两段式代替。
- **拆双写现状**：这不是一个要新建的东西——`f1579fd`／`09dac5f`（均已在 `dev`）已经把
  「读侧准入」「双写」「自愈式比对」全部实现在 Rust
  （`src/snapshot/repository/mirror/`），只是今天比的是「对象存储 catalog」vs
  「经 gRPC 连到 scheduler 的『central』catalog」。用户描述的「已作废的 `GetCatalogReadAdmission`
  gRPC 方案」在整个仓库和 `docs/proposals/` 里**没有任何代码或文档痕迹**——它大概率是被
  `admit_read_side`（复用已有的 `list`，不需要新 RPC）取代的更早期口头方案，可视为已经用「不加新
  RPC」的方式解决过一次。Stage B 要做的不是「拆双写」本身，而是把这套已经跑通的机制的
  **传输层从「gRPC 到 scheduler」换成「进程内直连 PG」**，并且把目前卡在
  `write = "postgres"` 上的桩代码填上。
- **最大风险**（详见 §9）：
  1. `write="postgres"` 未实现的桩就在
     `src/snapshot/repository/backends/mod.rs:292-293`
     （`anyhow::bail!("snapshot.catalog.write = \"postgres\" is not served by this build")`），
     且 `build_central_catalog` 只在 `write == Both` 时才会构造「central catalog」客户端——直连 PG 的实现要嵌进同一个位置，不能另起一套装配路径，否则又是「分开做等于做两遍」。
  2. CrashLoopBackOff 根因是「读侧是否已确认一致」这个**集群事实**被记在
     `MirrorBacklog`（RocksDB，`LocalKvStore`，路径
     `$AENV_HOME/snapshot-catalog-mirror`）——而 api 副本的 `$AENV_HOME`
     是 `emptyDir`，每个新副本都是一张白纸，把「switching now」误判为每次都要重新比对
     （`09dac5f`）。今天的修复是「比对失败就重放再比一次」，是**行为补丁**，不是**结构修复**。
     用户给的方向——把这个事实记到 PG 自己里——才是结构修复，本文档 §5 会给出具体落点。
  3. Postgres 连接池：N 个 `--role api` 副本各开一个池，今天 Go 侧 `scheduler` 只有
     **一个进程**连这个库（`pgxpool.MaxConns`，`defaultStoreMaxConnections`）。api 是多副本，
     必须显式设置每副本连接数上限，并核算 `副本数 × 上限 ≤ Postgres max_connections`。

---

## 1. catalog 是什么：逐文件行数核对

`services/scheduler/internal/catalog/`（Go 包 `catalog`）：

| 文件 | 非测试行数 | 内容 | Rust 落点建议 |
|---|---:|---|---|
| `store_postgres.go` | 1354 | `PostgresStore`：9 个 RPC 对应的 SQL 执行、`pgxpool` 装配、fencing/CAS 逻辑 | 重写。目标是一个 `PostgresSnapshotCatalog: impl SnapshotCatalog`，直接执行下面的 SQL |
| `migrate.go` | 703 | 手写版本化 migration applier（advisory lock + 版本表，**故意不用** migration 框架，见文件头注释） | 照搬思路，重写实现。见 §7 步骤 2 |
| `queries_resolved.go` | 350 | 读路径 SQL：`GetSnapshot`/`ListSnapshots`（keyset 分页）/`ResolveAlias`，全部带 `status_group='ready'` 谓词 | 照搬 SQL 文本，重写 Rust 绑定 |
| `store.go` | 578 | `Store` 接口、`PausedHalf` 接口定义、`StoreConfig`、错误类型 | **`PausedHalf` 部分丢弃**（见 §0、§5.3）；`Store` trait 形状对应 Rust 已有的 `SnapshotCatalog` trait（`src/snapshot/repository/interfaces.rs`），不需要重新设计接口，只需新实现 |
| `queries_admin.go` | 426 | 写路径 SQL：`insertSnapshotSQL`/`commitSnapshotSQL`/构建队列（`StartBuild`/`RenewBuildLease`/reaper 扫描） | 照搬 SQL 文本，重写 Rust 绑定 |
| `values.go` | 149 | 状态常量、`status`/`status_group` 校验（不下判断，只做「发送前 Go 侧先查一遍表的 CHECK 约束」） | 照搬为 Rust enum + 校验函数 |
| `pin.go` | 53 | origin pinning 的两列（`published`/`origin_node_id`）取值计算 | 照搬 |
| **合计** | **3613** | | 与 v7 line-count 注记一致 |

验证：`1354+350+703+578+426+149+53 = 3613`（已用 `python3` 复核）。

**不在这 3,613 行里，但同一次迁移必须处理**：

| 文件 | 非测试行数 | 说明 |
|---|---:|---|
| `services/scheduler/internal/catalog_service.go` | 975 | gRPC 转译层：9 个 RPC 方法体、`catalogGate`（见 §6）、build 相关 Prometheus 指标（`catalogRPCs`/`catalogRejections`/`catalogBuildsReaped`/`catalogReaperWarmup`/`catalogClockSkew`）、`RunBuildReaper` 后台协程、`NewPausedHalfAdapter` | RPC 方法体本身在 api 直连 PG 之后不再需要（api 进程内直接调用 store，没有 wire 层）；**但 `RunBuildReaper` 后台任务、build 相关 4 个指标、`catalogGate` 的语义必须在 Rust 侧重建**，否则构建队列的僵尸清理和可观测性会静默消失 |

三张迁移表（`0001_snapshots.sql` / `0002_templates_builds_aliases.sql` /
`0003_disk_size_known_at_ready.sql`）本身是纯 SQL，**逐字照搬**，不需要重写（除非改动 Rust 侧的 migration
applier 机制本身要求换格式，见 §7 步骤 2）。

**PG schema 摘要**（完整定义见迁移文件，此处只列迁移中要点，供实现时对照）：
- `snapshots`：主表，`id UUID PK`，`cluster_id`，`source_kind ∈ {template,sandbox}`
  （与 `source_sandbox_id` 是等价约束轴），`cpu_count`/`memory_mib`/`disk_size_mib`，
  `status ∈ {waiting,building,ready,error}` 与派生列 `status_group`（触发器维护，**从不由调用方写**），
  origin pinning 两列 `published`/`origin_node_id`（设计上要能整体 `DROP COLUMN`，见 0001 注释），
  `committed_payload BYTEA` + `committed_schema INTEGER`（Rust 序列化的不透明 blob，Go 侧从不解码），
  `build_error JSONB`，`publishing_execution_id UUID`（写入但当前未被判断，是留给多写者阶段的 fencing 位）。
  三个索引：`snapshots_list_idx`（keyset 分页专用，谓词 `status_group='ready'`）、
  `snapshots_source_sandbox_idx`、`snapshots_unpublished_idx`（origin pinning 专用，设计上要能整体删除）。
- `templates`：几乎是 `snapshots` 的影子表（FK 级联），当前无 template-only 列，
  `active_templates` 视图隐藏 `deleted_at_ms`。
- `builds`：构建队列。`node_id`（**发起/心跳的 admitting 进程，不一定是真正跑 build 的机器**——
  见 `builds.node_id` 列注释，与 `src/node_client/build.rs` 的 dispatch 语义对应）、
  `heartbeat_at_ms`（reaper 依据）、唯一索引 `builds_one_active_per_template`
  （partial unique index 做「每模板一个在途构建」，取代 Go 侧原本的 read-modify-write 竞态）。
- `aliases`：`(cluster_id, alias)` PK + `snapshot_id` 上的唯一索引
  `aliases_one_per_snapshot`，`ON DELETE CASCADE`。
- `0003` 迁移：`disk_size_mib` 的 CHECK 从「插入即校验」搬到「`ready` 才校验」——因为 v3
  模板在创建时确实不知道磁盘大小（`0` 是「未知」哨兵值，不是缺省错误），这条业务语义
  **必须在 Rust 侧的 SQL/binding 里原样保留**，否则每一个 v3 模板创建都会在 `ready`
  之前被拒。

---

## 2. 双写现状：状态机

配置轴（`src/cfg.rs:483-556`，`SnapshotCatalogConfig`）：

```
write ∈ { object_store, both, postgres }   AENV_SNAPSHOT_CATALOG_WRITE
read  ∈ { object_store, postgres }         AENV_SNAPSHOT_CATALOG_READ
```

合法组合由 `AppConfig::validate_snapshot_catalog`（`src/cfg.rs:1524-1570`）在启动时强制：

| write \ read | object_store | postgres |
|---|---|---|
| `object_store` | ✅ 今天的默认 | ❌ 拒绝：读一张没人写的表 |
| `both` | ✅ 双写，读对象存储 | ✅ **唯一合法的「读 PG」状态**，见 §5 |
| `postgres` | ❌ 拒绝：写一张没人读的表 | ❌ **本 build 拒绝**（`write=postgres` 未实现，见下） |

读侧从 `object_store` 切到 `postgres` 时经过的完整链路（都在
`src/snapshot/repository/backends/mod.rs::build_snapshot_backend` 里，
`45-215` 行区间）：

1. 装配对象存储侧的 `SnapshotRepository`。
2. `build_central_catalog`（`285-315`）：只有 `write == Both` 才会连
   `cluster.scheduler_endpoint`（`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`）构造
   `CentralSnapshotCatalog::connect_lazy`；`write == ObjectStore` 返回 `None`；
   `write == Postgres` **直接 `bail!`**。
3. `backlog.queue_history_toward_central`：把对象存储里「双写打开之前就存在」的历史行
   排进本地 `MirrorBacklog` 的待补队列（否则双写只覆盖「今后」的写入，见 `mod.rs:83-98`
   注释里记录的真实事故：PG 0 行、对象存储 32 行）。
4. `backlog.settle_before_reading_from`（`backlog.rs:1025`）：读侧从 postgres
   回退到 object_store 之前，先把对象存储欠 central 的写重放掉——否则回滚这个动作本身
   会被 `guard_read_side` 拒绝，形成「回不去」。
5. `admit_read_side`（`population.rs:407-443`）：**这就是用户描述的「读侧准入」**。
   逻辑：
   - `backlog.guard_read_side(configured)`：读欠债计数器（`mirror_lag`/`mirror_diverged`）。
   - 如果 `configured == Postgres` 且这个节点的本地存储从未记录过读侧（`recorded_read_side()
     != Some(Postgres)`）：视为「正在切换」，`CatalogPopulations::compare` 直接各拉一次
     `every_snapshot_id()`（对象存储 vs central，全量 `list`，**复用已有 RPC，没有新增
     `GetCatalogReadAdmission` 这类专用接口**）。
   - 不一致：`repair_toward_central` 重放欠central的队列，重放到东西就再比一次。
   - 仍不一致：`bail!` 出一条列出具体 id 差集的错误——**这就是 CrashLoopBackOff 的直接来源**，见 §4。
   - `backlog.record_read_side(configured)`：**问题就在这一步的落点**——写进本地
     `LocalKvStore`，不是 PG。见 §5。
6. 通过后才真正把 `CatalogReadSide::Postgres` 接进 `SnapshotRepository` 的读路径。

---

## 3. `write="postgres"` 为什么没实现（文件行号证据）

`src/snapshot/repository/backends/mod.rs:285-294`：

```rust
fn build_central_catalog(config: &AppConfig) -> Result<Option<Arc<CentralSnapshotCatalog>>> {
    match config.snapshot.catalog.write {
        SnapshotCatalogWrite::ObjectStore => return Ok(None),
        SnapshotCatalogWrite::Both => {}
        // Refused by `validate_snapshot_catalog` before this is reached; the
        // arm exists so adding the mode later is a compile error here rather
        // than a silent no-op.
        SnapshotCatalogWrite::Postgres => {
            anyhow::bail!("snapshot.catalog.write = \"postgres\" is not served by this build")
        }
    }
    ...
```

并且 `src/cfg.rs:1565-1570`（`validate_snapshot_catalog` 的
`(Postgres, Postgres)` 分支）在配置校验层就已经先行拒绝：

> "snapshot.catalog: write = \"postgres\" drops the object-store copy, which
> is the only way back from the central catalog. It is allowed once the read
> side has been served from PostgreSQL for an observation period and the
> mirror lag has been 0 throughout; it is not allowed in this build. Set
> write = \"both\"."

即：这不是一个 bug，是一个**故意留白的安全阀**——设计上要求「先在 `write=both,
read=postgres` 跑一段观察期、`mirror_lag` 持续为 0」才允许收掉对象存储副本，而这个「收尾」
从未被实现（两处都写死拒绝，`cfg.rs` 拒绝在前，`mod.rs` 的 `bail!` 是防呆冗余）。

**CrashLoopBackOff 的根因**（一句话）：`write="postgres"` 本身从未导致过 CrashLoop——
它在 `both/postgres` 状态就已经启动失败，从未上线过。真正让线上 Pod 反复重启的是
`write=both, read=postgres` 状态下 `admit_read_side` 的比对逻辑：`09dac5f`
之前，每个新副本的 `MirrorBacklog`（RocksDB on `emptyDir`）都是空的，空存储被
`admit_read_side` 读作「正在切换」而触发全量比对；比对时两个 catalog 有 5 个快照的差异
（对象存储领先），而当时补差的 `MirrorCompensator` 是在 `admit_read_side` **之后**才
spawn 的——于是比对必拒、拒了就 `bail!`、`bail!` 让 `build_snapshot_backend` 返回
`Err`、进程退出、k8s 重启、新副本又是一张白纸，无限循环，只有比差异更老的 Pod 还在服务。
`09dac5f` 的修复是把「比对失败就重放欠债、重放到东西就再比一次」塞进 `admit_read_side`
内部，让它能在**没有 compensator 先跑一轮**的情况下自愈——是行为补丁，见 §0 与 §5。

---

## 4. api Pod 启动门：实现位置与解封逻辑

- **触发点**：`build_snapshot_backend`（`src/snapshot/repository/backends/mod.rs:45`）
  是 `SnapshotManager::new`（`src/snapshot/manager.rs:167`）唯一的装配路径，
  在 `--role api`（`assemble_api`，`src/bin/server.rs:939`）与 `--role all`
  （`assemble_node_core` 内部共享路径，`server.rs:418,461`）启动时同步调用；返回
  `Err` 会让 `async_main` 直接失败退出（K8s 视为 Pod 启动失败 → CrashLoopBackOff）。
- **比什么**：`CatalogPopulations::compare`（`population.rs:69-88`）——对象存储的
  全量快照 id 集合 vs central catalog 的全量快照 id 集合，只比身份（id 存在与否），
  **不比内容**（同 id 不同 `created_at_ms`/alias/payload 会被判定为一致，这是文档里
  明确写出的已知局限，`population.rs:20-23`）。
- **失败后果**：`admit_read_side` 直接 `anyhow::bail!(populations.refusal(...))`，
  错误信息里带最多 8 个具体缺失 id（`IDS_IN_REFUSAL`），冒泡到 `build_snapshot_backend`
  → `SnapshotManager::new` → `async_main` → 进程退出。
- **今天的解封**（`09dac5f`，不下线节点）：不一致时**不立即拒绝**，先
  `repair_toward_central`（重放 `MirrorBacklog` 里欠 central 的写），重放有效果就再比一次；
  只有「重放之后仍然不一致」才真正拒绝——这时候的差异是「central 有、对象存储没有」的行，
  这种方向的差异确实没有自动修复路径，需要人工介入。k8s 清单里同步补了一段解释性注释
  （`deploy/k8s/base/agentenv-api-deployment.yaml`，`scratch` volume 定义下方，
  `09dac5f` 新增约 28 行），说明 `snapshot-catalog-mirror` 这个目录**名义上要求「node-local
  and durable」但在这个角色上实际是 `emptyDir`**，这是一处已知但暂未解决的持久化缺口。

**这个门今天的结构性问题**（不是这次要修的 bug，而是 Stage B 应该顺手修掉的设计债）：
它把一个**集群级事实**（「PostgreSQL 是否已经追上对象存储」）记在**节点级存储**里，
所以「一次确认，处处生效」变成了「每个副本各自确认一次」。§5 给出移到 PG 里的具体方案。

---

## 5. 「拆双写」怎么并入 Stage B

### 5.1 读侧准入这条事实记在 PG 哪里

新增一张小表（暂定名 `catalog_migration_state`，单行或按 `cluster_id` 一行）：

```sql
CREATE TABLE IF NOT EXISTS catalog_migration_state (
    cluster_id           UUID PRIMARY KEY,
    read_side_confirmed  BOOLEAN NOT NULL DEFAULT false,
    confirmed_at_ms       BIGINT NULL,
    confirmed_by_node_id  TEXT NULL   -- 审计用，不参与判断
);
```

`admit_read_side` 的等价逻辑改成：先查这一行；`read_side_confirmed = true` 直接放行，
**不再需要每个副本各自拉全量 id 比对一次**。第一次把 `read` 切到 `postgres`
的那个副本负责跑一次 `CatalogPopulations::compare`（逻辑照搬 `population.rs`），
成功后 `UPSERT ... SET read_side_confirmed = true`；失败则不写，下一个尝试切换的副本
（可能还是同一个副本重启后）重新跑。这条 UPSERT 本身要在**同一个 PG 事务**里做
「比对通过」和「标记确认」，避免「比对通过但没来得及标记」和「已经标记但比对其实没通过」
的窗口（Go 侧 `queries_admin.go` 已经有类似「读一次、判断一次、原子写一次」的先例可以照抄这个模式）。

这样修复了 §4 提到的结构性问题：CrashLoopBackOff 的触发条件（「本地存储是空的」）从物理上
不存在了——因为要查的存储不再是节点本地的 `emptyDir`，是所有副本共享的 PG。

### 5.2 `write="postgres"` 怎么实现

在 Stage B 语境下，「postgres」不再意味着「只经 gRPC 连 scheduler、不留对象存储副本」，
而是「`SnapshotCatalog` 的唯一实现是进程内直连 PG 的 `PostgresSnapshotCatalog`,
不再有 central-gRPC 这一跳」。具体地：

- `SnapshotCatalogWrite::Both` 与 `SnapshotCatalogWrite::Postgres` 在 Stage B
  之后应该都指向**同一个** `PostgresSnapshotCatalog` 实现，区别只是「object store
  是否还保留一份」——这正是今天 `Both`/`Postgres` 两个变体的本意，没有变。
- `build_central_catalog`（改名为 `build_direct_catalog` 更贴切，但改名与否是实现期
  决定）在 `write != ObjectStore` 时构造 `PostgresSnapshotCatalog::connect(dsn,
  cluster_id)`，而不是 `CentralSnapshotCatalog::connect_lazy(scheduler_endpoint, ...)`。
  这一步把 `SnapshotCatalogWrite::Postgres` 分支里的 `anyhow::bail!` 替换成真正的构造调用——
  桩填上了，`write="postgres"` 就实现了。
- `validate_snapshot_catalog` 里 `(Postgres, Postgres)` 分支的拒绝文案提到的前提
  （「先在 `write=both, read=postgres` 跑一段观察期、`mirror_lag` 为 0」）在 Stage B
  语境下已经不成立——一旦 `PostgresSnapshotCatalog` 是唯一实现，"双写" 和 "观察期"
  的意义是「object store 是否保留归档副本」，不再是「验证一个新的、可能有 bug 的直连
  实现是否可信」（因为 Stage B 会为这份新代码补一整套契约测试，见 §8）。**这条拒绝规则
  是否放开，是 Stage B 施工期要重新做的产品判断，不能照搬旧文案**——保守起见，本计划
  建议 Stage B 交付时仍然拒绝 `(Postgres, Postgres)`，把「收掉对象存储副本」单独作为
  一个后续变更（观察 Stage B 稳定运行一段时间之后），而不是随 Stage B 一起放开。

### 5.3 `PausedHalf` 不搬——但要显式记录这个决定

Go 侧 `catalog.PausedHalf` 接口（`store.go:497-508`）与 `catalog_tx.go`
（175 行，在 `internal/registry` 包里，不在 3,613 行统计内）实现「暂停这个动作
= 写 `paused_sandboxes` 一行 + 写 `snapshots` 一行，绑在一个 `pgx.Tx` 里」。
`services/scheduler/cmd/main.go:174-175`：
`catalogStore := catalog.NewStoreWithPool(registryWriter.Pool(),
catalogStoreConfig(logger, cfg, scheduler.NewPausedHalfAdapter(registryWriter)))`——
catalog store 直接借用 registry 的连接池，并且总是接一个真实的 `PausedHalf`。

但 **Rust 客户端从未使用这条能力**：`src/snapshot/repository/backends/central/mod.rs`
的三处 RPC 调用（`begin_snapshot` ~L375、`commit_snapshot` ~L434、`fail_snapshot`
附近）全部把 `paused_transition: None` 写死，注释原文：

> "🔴 Absent, in this batch, always. Filling it in would make this RPC write
> `paused_sandboxes` as well — and the pause path still drives that table
> through `PausedRegistry`, so both halves would be writing the same row from
> two calls. The field is the whole point of the service and the batch that
> rewires the pause path is the one that sets it."

即：Go 服务端支持原子联合写，但至今没有一个批次把它接上；今天真实的暂停落盘路径是
`src/api/impls/paused_coordinator.rs:487-514`：`registry.begin_pause` →
`snapshot_manager.publish_captured`（走 catalog 的 `CommitSnapshot`,
`paused_transition=None`）→ `registry.complete_pause`，三次独立调用，靠
`began.generation` 防护 + 「`complete_pause` 失败先重读当前状态再决定」的幂等处理
兜底不一致窗口（`paused_coordinator.rs:907-1150` 一整段专门处理这个）。

**结论**：`PausedHalf`/`catalog_tx.go` 这一整块（Go 侧约 175 行 + `store.go` 里
`PausedHalf` 接口定义与相关分支）**不需要在 Stage B 里port**。理由：
1. 它服务的能力从未被启用过，是纯粹的死重量。
2. Rust 已经有一套跑通、测试覆盖（见 `paused_coordinator.rs` 里
   `a_failed_complete_pause_rereads_the_row_before_touching_the_snapshot` 等测试）
   的两段式非原子协议，重新引入一次原子写反而是给一个没人依赖的能力找负担。
3. 如果未来真的要把 `paused_transition` 接上（把两次调用合并成一次事务），那必然要等
   **Stage C 把 `paused_sandboxes` 也搬进 api 的同一个 PG 连接**之后才有意义——
   两张表都在同一个 Rust 进程、同一个 PG 连接池里，才谈得上「一个事务写两张表」；
   在 Stage B 单独做，事务的另一半（`paused_sandboxes`）还在 Go/scheduler 手里，
   物理上做不到。**如果以后真做，应该算在 Stage C 或 Stage C 之后的一个独立小改动里，
   不要算进 Stage B 的交付范围**，本计划把这一点显式标注出来，避免被以后的人当成
   Stage B 遗漏的功能。

---

## 6. Rust 侧现有对接与需要新增什么

### 6.1 现有对接

- `SnapshotCatalog` trait（`src/snapshot/repository/interfaces.rs`）是唯一的抽象点，
  今天有至少 4 种实现：
  - `CentralSnapshotCatalog`（`src/snapshot/repository/backends/central/mod.rs`,
    1447+538 行）：gRPC 到 scheduler，本次要被取代/退居 fallback 的对象。
  - 对象存储自带的 catalog 实现（在 `oss`/`posixfs`/`common` 目录下，POSIX/OSS
    仓库各自的目录实现）。
  - 若干测试用 fake（`OneRowCatalog`/`PendingTemplateCatalog`/`JournallingCatalog`/
    `UncommittedSnapshotCatalog`，分布在 `api/impls/*.rs` 的测试模块里）。
- gRPC 客户端本身走 `tonic`（`Cargo.toml:67-70`，`tonic 0.14.2`/`prost 0.14.3`），
  proto 由 `crate::proto::scheduler`（`thirdparty`/生成代码）提供。
- `--role node` 也持有一份 `SnapshotManager`（`src/bin/server.rs` 内
  `assemble_node_core`，被 `assemble_node` 与 `assemble_all` 共用），用于：
  1. `PausedSandboxWiring`（暂停快照发布）；
  2. `node_server::serve_on` 对外暴露的构建分发 gRPC（`agentenv::node_server`）。
  这两处都通过 `SnapshotCatalog` trait 使用目录，**不直接持有 PG 连接**——`central/mod.rs`
  文件头注释明确说明这是有意为之：「DSN、连接预算、schema 都不落在跑用户代码的机器上」。
  **这条安全边界在 Stage B 之后必须保留**：`--role node` 绝不能拿到 PG DSN。

### 6.2 需要新增

- **Cargo 依赖**：仓库目前**没有任何** Postgres 客户端依赖（`sqlx`/`tokio-postgres`/
  `deadpool-postgres`/`bb8-postgres` 全部搜索为空,已核实）。需要新增一个。建议
  `tokio-postgres` + `deadpool-postgres`（或 `bb8-postgres`）而非 `sqlx`：
  - Go 侧的 migration 是刻意手写的版本表 + advisory lock（`migrate.go` 头部注释明确
    拒绝引入 migration 框架的理由），Stage B 应该延续这个判断而不是引入 `sqlx::migrate!`
    的自动化机制——三份 `.sql` 文件本来就要逐字照搬,用 `tokio-postgres` 手动执行
    这些语句 + 手写版本表逻辑,是对 Go 侧既有决定的最小改动式移植。
  - `tokio-postgres` 是 `deadpool-postgres`/`bb8-postgres` 的底层依赖，两者都是成熟的
    连接池方案，可以直接对齐「每副本连接数上限」的配置需求（§0 风险 3）。
  - 若团队更偏好 `sqlx`（编译期 SQL 检查、生态更大），也可行，但要接受这是对 Go 侧刻意
    决定的偏离，需要单独写明理由。**这是一个需要在动工前拍板的开放决策**，本文档
    不代为决定，列入 §10 待确认。
- **PG 连接池装配**：一个新的（暂定）`src/snapshot/repository/backends/postgres/`
  模块，装配逻辑要点：
  - DSN 从 Secret 注入（复用 k8s 里已经存在的 `agentenv-postgres` Secret，见
    `deploy/k8s/base/scheduler-deployment.yaml:91-94` 的
    `SCHEDULER_REGISTRY_DSN` 先例——**同一个 DSN**，因为 Go 侧 catalog 和 registry
    本来就共用一个 pool）。
  - 按 CLAUDE.md 的 `cfg.rs` 坑（`#[config(nested)]` 段不能有 `env =`）：如果新增的
    PG 配置放进一个 nested struct（比如仿照 `[backend.oss]` 建一个
    `[backend.postgres]`），DSN 就只能通过 `AENV_CONFIG_OVERLAY_PATH`
    的文件设置，**不能**指望一个 `AENV_...` 环境变量直接生效。如果需要环境变量注入
    （Secret 挂载成 env var 是 k8s 里最简单的方式），DSN 字段就不能挂在
    `#[config(nested)]` 的段下面，得挂在一个顶层非 nested 的配置项上（类似
    `snapshot.catalog.write`/`read` 今天的做法，它们不在 nested 段里，所以能用
    `env = "AENV_SNAPSHOT_CATALOG_WRITE"`）。这一条必须在设计 PG 配置 struct 的
    第一步就定下来，写错了要等一次配置改版才能修。
  - 连接数上限：新增配置项（如 `snapshot.catalog.postgres_max_connections`，
    类比 Go 侧 `defaultStoreMaxConnections`），文档里写明「乘以副本数不能超过
    Postgres `max_connections`」，并在校验逻辑里给出参考默认值。
- **Migration runner**：port `migrate.go` 的版本表 + advisory lock 思路（不引入框架），
  三份 `.sql` 文件本身照搬（可以直接引用/复制 `services/scheduler/internal/catalog/
  migrations/` 下的文件，保持两侧 schema 定义单一来源，或在 Rust 侧建一份物理副本并
  用测试固定「两边字节相同」防止漂移——两种做法各有取舍，留给实现者按仓库惯例决定）。
- **`RunBuildReaper` 等价物**：一个 Tokio 后台任务，周期扫描
  `builds_active_idx`（谓词 `status_group IN (pending,in_progress)`），
  对心跳过期的行标记失败——模式与 `src/orchestrator/`
  已有的「自动过期沙箱回收」后台任务（CLAUDE.md 提到的 auto-eviction task）一致，
  照抄该模式即可，不需要重新设计。
- **Prometheus 指标**：`agentenv_scheduler_catalog_rpc_total`、
  `agentenv_scheduler_catalog_rejected_total`、
  `agentenv_scheduler_catalog_builds_reaped_total`、
  `agentenv_scheduler_catalog_build_reaper_warmup_passes_total`、
  `agentenv_scheduler_catalog_build_clock_skew_total` 五个指标要在 Rust 侧重建
  （改名或保留原名是运维层面的决定，需要和现有 dashboard/告警对齐，本文档不代为决定）。

---

## 7. 施工清单（每步独立编译通过、独立可回退）

1. **加依赖，不改行为**：往 `Cargo.toml` 加 PG 客户端依赖（`tokio-postgres` +
   连接池，或拍板后的 `sqlx`），新建空的 `postgres` 子模块骨架，`cargo build` 通过，
   不接入任何调用路径。回退：删依赖、删文件。
2. **Migration runner + schema 落地**：port `migrate.go` 的版本表/advisory lock
   applier，三份迁移 SQL 照搬进来（新增第 4 份 `0004_catalog_migration_state.sql`
   落 §5.1 的确认表）。有独立测试：对一个空库跑两遍都成功、版本表内容正确、
   `DROP TABLE ... CASCADE` 回退命令验证过。不接入 `build_snapshot_backend`。
3. **`PostgresSnapshotCatalog` 只读路径**：实现 `SnapshotCatalog` trait 的
   `get`/`list`/`resolve_alias`（对应 `queries_resolved.go`），配套单元测试（可以
   先用现有的 fake catalog 测试模式打底,再补一套针对真实 PG 的集成测试，见 §8）。
   不接入配置装配，只在测试里手工构造使用。
4. **`PostgresSnapshotCatalog` 写路径**：`begin`/`commit`/`fail`/`delete`/
   `start_build`/`renew_build_lease`/`get_build`（对应 `queries_admin.go`），
   fencing/CAS 逻辑照抄 Go 侧 SQL 的 `WHERE` 条件与 partial unique index 依赖。
   仍不接入配置装配。
5. **Build reaper 后台任务** + 五个 Prometheus 指标。独立可测（构造一个过期心跳的
   build 行,断言被标记失败）。
6. **接入 `build_snapshot_backend`**：`build_central_catalog` 改为在
   `write != ObjectStore` 时构造 `PostgresSnapshotCatalog`（而不是
   `CentralSnapshotCatalog`），删掉 `SnapshotCatalogWrite::Postgres` 分支的
   `bail!`。**这一步是切口**——之前的所有步骤都不改变现网行为，这一步开始才会真正
   有流量走新代码。建议先在 `write=object_store`（默认值）下合入并跑通全部 CI，
   确保这条新代码路径「存在但默认不生效」。
7. **读侧准入落 PG（§5.1）**：新增 `catalog_migration_state` 表读写逻辑，替换
   `admit_read_side` 的本地 `MirrorBacklog` 判断依据。**这一步单独灰度**：先在一个
   非生产集群上把 `write=both, read=object_store` 切到 `write=both,
   read=postgres`（此时 central 仍是 `CentralSnapshotCatalog`，走 gRPC 到
   scheduler，只是准入判断换了落点），验证新 Pod 启动不再因为 `emptyDir`
   而反复重放比对。
8. **切换 central 实现来源**：把 `write=both`/`postgres` 时构造的「central catalog」
   从 `CentralSnapshotCatalog::connect_lazy(scheduler_endpoint)` 换成
   `PostgresSnapshotCatalog::connect(dsn)`。回退开关：配置层面把 `write` 切回
   `object_store`（对象存储副本仍在，双写保留到这一切都稳定之后才考虑拆——与
   `docs/proposals/2026-08-20-service-decomposition.md` 阶段2 的「双写保留到阶段3
   上线并稳定之后再拆」是同一条原则,继续沿用）。
9. **`--role node` 的路由**：确认 `--role node` 场景下 `SnapshotManager` 走的是
   哪条路径（§6.1 提到它今天也用 `SnapshotCatalog` trait），确保它**不会**因为这次改动
   意外拿到 PG DSN——它应该继续走一条网络跳转（可以是仍然连 scheduler 的
   `CentralSnapshotCatalog`，直到 Stage E scheduler 真正下线为止；或者改连 api
   暴露的等价 gRPC 面，如果决定提前切换）。**这一步需要先回答 §10 里的未确认问题
   「node 侧目录访问在 Stage B 完成后指向哪里」**，本计划不代为决定,只标注这是
   必须显式处理的一步，不能被隐式遗漏。
10. **`services/scheduler` 侧收尾**：`catalog_service.go` 的 9 个 RPC 方法体保留
    （给还没切换的节点/回退路径用），但停止把它当作「唯一实现」来维护；
    `internal/catalog` 包本身在这一阶段**不删除**（回退需要它）。真正的删除留给
    Stage E。
11. **契约测试落地**（可以穿插在 3/4 步之后做，不必等到最后）：见 §8。

---

## 8. 测试面

- **Go 侧现状**：`internal/catalog` 目录下 7 个测试文件，5535+1546=约 7,081
  非我统计范围但供参考的测试行数。`schema_test.go`/`pin_test.go`/`queries_test.go`
  不依赖真实数据库（纯 Go 单元测试,`make -C services test` 不需要 PG 就能跑）；
  `migrate_test.go`/`store_postgres_test.go`（3,238 行，全仓库最大的单个测试文件）
  依赖真实 Postgres，**复用与 `internal/registry` 相同的环境变量**
  `SCHEDULER_REGISTRY_TEST_DSN`/`SCHEDULER_REGISTRY_TEST_REQUIRED`（已核实,变量名
  虽然带 `REGISTRY` 字样，但 catalog 包的测试同样读它）——这意味着 CLAUDE.md 里说的
  「`make -C services test-with-postgres` 覆盖 `internal/registry`」这条陈述,
  **对 catalog 包同样成立**，只是变量命名容易让人误以为它只测 registry。
  `catalog_service_test.go`（1,546 行）在 `internal/` 包下测 gRPC 转译层，同样部分
  依赖 DB。
- **`make -C services test`（无 DB）**：catalog 的 SQL 语义测试全部跳过（打印
  `SKIPPED[redis]` 类似的跳过提示，具体文案以实际运行为准，未逐条核实每条打印文本）,
  只有不依赖 DB 的部分（schema 校验、pin 逻辑、query 参数构造）真正跑。
- **Rust 侧要补的**：
  1. 一套**跨两个后端共用的契约测试**，模式直接照抄 `src/orchestrator/store/
     contract.rs`（CLAUDE.md 已经点名这是「可以照抄的模式」）——用同一份测试断言
     `PostgresSnapshotCatalog` 和现有的 `CentralSnapshotCatalog`（在它还没退役之前）
     或者一个内存 fake 满足相同的 `SnapshotCatalog` trait 契约,防止「改了一个后端、
     忘了另一个」的类型漂移（CLAUDE.md 明确警告过这类 bug 是这个代码库反复踩过的坑）。
  2. 针对 `admit_read_side` 落 PG 之后的新行为：模拟「多副本同时启动、其中一个在跑比对、
     其他副本应该直接读到确认结果而不用各自比对一次」的并发场景。
  3. `builds_one_active_per_template` partial unique index 的并发插入测试
     （两个并发 `StartBuild` 只有一个成功）——这是 Go 侧特意用数据库约束替代
     read-modify-write 竞态的地方（`0002` 迁移注释里写明），Rust 侧如果用错误处理
     方式（比如先 SELECT 再 INSERT）会重新引入这个竞态,必须直接测数据库约束本身生效。
  4. `0003` 迁移那条「`disk_size_mib` 只在 `ready` 时校验」的业务规则,专门测 v3
     模板在 `waiting`/`building` 阶段 `disk_size_mib=0` 能正常创建。
  5. `make test-with-redis` 类比的一个新目标（例如 `make test-with-postgres`
     的 Rust 等价物，如果还不存在的话）：把「无 PG 则跳过、`AENV_*_REQUIRED=1`
     则跳过变失败」这条 Go 侧已经验证过的模式在 Rust 侧重建，CI 里显式跑一次真实
     Postgres 的契约测试,不能让它变成「本地跑不出来所以永远绿」的假象。

---

## 9. 风险与坑

1. **`write="postgres"` 的桩位置是唯一装配入口**——`build_central_catalog` 是
   `SnapshotCatalogWrite::Postgres` 唯一被处理的地方（`cfg.rs` 的配置校验层已经先一步
   拒绝了 `(Postgres, Postgres)`），改动必须精确落在这一个函数，不要在别处新建平行的
   装配路径，否则未来又要靠人力保持两处同步。
2. **`central` 模块名字具有误导性**——现有 `crate::snapshot::repository::backends::central`
   模块名叫 `central`，文档注释解释的是「controller-owned」这个设计意图，不是
   「一定经过 gRPC」。Stage B 加入 `PostgresSnapshotCatalog` 之后，如果决定保留
   `central` 这个词描述新实现（因为它同样是「谁持有 DB 谁负责事务」的意图），要更新
   模块头注释，否则新读者会误以为 PG 版本也一定要经过某个「controller」进程。
3. **`--role node` 的目录访问路径没有在这次调研里得到明确答案**（见 §10）——
   这是 Stage B 唯一一个「不确定是否要动」的高风险点：如果 Stage B 简单粗暴地把所有
   `write != ObjectStore` 场景统一换成直连 PG,`--role node` 会意外拿到 PG DSN,
   直接违反 `central/mod.rs` 文件头写明的安全边界（「DSN 不落在跑用户代码的机器上」）。
   施工前必须先回答：`--role node` 在 Stage B 交付时到底连谁。
4. **build reaper 是唯一实例后台任务**——Go 侧只有一个 `scheduler` 进程在跑
   `RunBuildReaper`；Rust 侧是 N 个 `--role api` 副本，如果直接照搬「每个进程自己起一个
   定时任务」，会变成 N 个副本同时扫描、同时争抢同一批过期 build 行。虽然
   `builds_active_idx` 的 SQL 本身是幂等的（UPDATE 一行已经被别人标记失败的 build
   不会造成数据损坏),但 N 倍的扫描频率和潜在的行锁竞争是不必要的开销,需要一个
   leader-election 或者「谁抢到就谁做」的机制（可以参考 orchestrator 自己的
   auto-eviction 任务在多副本场景下是怎么处理的，如果它已经处理过的话——本次调研
   没有核实这一点，列入 §10）。
5. **`disk_size_mib=0` 哨兵值语义必须原样保留**——如果 Rust 侧的 binding 层图省事,
   给 `disk_size_mib` 加一个「非零」的 Rust 类型级别约束（比如 `NonZeroU32`),
   会在 v3 模板创建时直接编译期/运行期崩掉,这条业务语义只在 SQL CHECK
   约束里生效，Rust 类型系统不能替代这个决定。
6. **`created_at_ms`/`updated_at_ms` 是应用层时钟不是数据库时钟**——0001 迁移注释
   明确写了「双写阶段要求镜像的两行携带相同的时刻，服务端时间戳会让每一行都因为 RPC
   延迟而不一致」。Rust 侧直连 PG 之后，这条约束的理由（避免 gRPC 往返延迟导致的比对
   误报）理论上不再成立（同进程内直接执行 SQL,延迟趋近于 0），但迁移文件本身的触发器
   逻辑（`updated_at` 只在调用方留空时才用服务端时钟）**不应该因为这个理由改动**——
   改行为等于改 schema 语义，属于 Stage B 范围之外的独立决策。
7. **`published`/`origin_node_id` 这两列设计上要能整体 `DROP COLUMN`**——0001
   迁移注释列了 6 条「保持这两列不被缠绕进其他逻辑」的规则。Rust 侧实现 binding 时
   如果图方便把这两列塞进某个复合索引或者和 `status_group` 合并判断,会破坏这个
   「以后能一次性删掉」的设计预留,需要在 code review 时对照这 6 条规则检查。
8. **`publishing_execution_id` 目前写入但不校验**——Go 侧注释明确这是留给「多写者阶段」
   的 fencing 位，今天永远是「写了但从不在 WHERE 里用」。Stage B 如果引入新的写入路径
   （比如允许多个 api 副本同时对同一行发起 commit),必须先补上这条 fencing 校验,
   否则「多写者」的场景在 Rust 侧比 Go 侧更容易触发（Go 只有一个 scheduler 进程,
   Rust 是 N 个 api 副本,天然就是多写者)。

---

## 10. 这次不做什么

- **不实现 `PausedHalf`/跨表原子事务**——见 §5.3，这个能力从未被启用，不在 Stage B
  范围内；如果以后要做，前提是 Stage C 完成之后。
- **不放开 `write=postgres` 之后「收掉对象存储副本」**——桩会填上、状态会可达,
  但保守起见,双写继续保留到独立观察期结束,不作为 Stage B 交付的一部分自动打开。
- **不改变 `disk_size_mib=0`/`created_at_ms` 应用层时钟等既有 schema 业务语义**——
  逐字照搬，不借这次机会「顺手优化」。
- **不在 `services/scheduler` 里删除或废弃 `internal/catalog` 包**——回退路径需要它,
  真正删除是 Stage E 的职责。
- **不改 `services/gateway`**——已核实 gateway 完全不引用 `SnapshotCatalog`
  相关的任何 gRPC 调用，这次不涉及它。
- **不新建 `GetCatalogReadAdmission` 或任何新 gRPC 方法**——用户已经裁决作废，本计划
  也没有找到需要新增 wire 层接口的理由：`admit_read_side` 复用现有 `list`,
  §5.1 的读侧准入表是直接 SQL 读写，都不需要新 RPC。

---

## 11. 待确认（未在本次调研中确证，禁止假设）

1. **`--role node` 在 Stage B 完成后应该连谁做目录访问**——是继续连
   `CentralSnapshotCatalog`（此时它连的是仍在运行的 `services/scheduler`，等 Stage E
   才切换）,还是提前改connect 到 api 新暴露的等价 gRPC 面？这两个选项各自的成本
   （前者：node 侧代码零改动，但意味着 scheduler 在 Stage E 之前必须继续为 catalog
   写路径保留 gRPC 服务端；后者：api 需要新暴露一个 gRPC 面服务 node，等于给 api
   增加了 Stage E 之前就要交付的新 wire 层）没有在本次调研中权衡,需要施工前拍板。
2. **build reaper 在多副本 `--role api` 下的并发策略**——是否已经有一个通用的
   「N 副本选一个做周期任务」机制（orchestrator 的 auto-eviction 是否已经解决过
   这个问题、能否直接复用）,本次没有读 `src/orchestrator/` 里 auto-eviction
   任务的具体实现来确认。
3. **PG 客户端库选型：`tokio-postgres`/`deadpool-postgres` vs `sqlx`**——
   本文档给出了倾向性建议（延续 Go 侧「不用 migration 框架」的判断，选前者更贴合）,
   但这是团队决策,没有在代码或文档里找到已经拍板的证据。
4. **`catalog_migration_state` 表是否应该按 `cluster_id` 分行还是全局单行**——
   本文档假设是按 `cluster_id`（因为 `snapshots` 表本身是多 cluster 的),但没有
   找到「一个 Postgres 库是否会同时服务多个 cluster_id」的明确证据来验证这个假设
   是否必要（也可能整个 catalog 库天生就是单 cluster 一个库,这张表就不需要
   `cluster_id` 这一列）。
5. **Go 侧 `catalogGate`（`catalog_service.go` 里的 `type catalogGate interface{
   Require() error }`）与 Rust 侧 `admit_read_side` 是否需要在 Stage B 里统一成
   一套「schema 未就绪则拒绝」的机制**——本文档把两者当成概念上独立的两道门
   （§4 vs §6 的 `migrateCatalog`/`registryGrace`）分别讨论,但没有确认 Rust 侧
   是否已经有等价于 Go 的 `Grace`/`grace.Enter` 的「本 build 自己的 schema
   还没跑完之前拒绝服务」机制,还是要在 Stage B 里新建一个。
6. **`snapshot.catalog.postgres_max_connections` 之类新配置项的默认值**——
   本文档只指出需要这个配置项并给出方向,没有给出具体数字,需要结合实际 Postgres
   实例规格和副本数在实现期确定。
