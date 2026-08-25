# Phase 4 / Stage B：catalog 搬进 `--role api`（含拆双写并入）

调研快照：`dev` @ `1022b3a`（2026-08-25）。只读调研，无代码改动。

🔴 **订正记录**：本文档在 `1022b3a` 之后又经过两轮改动才是 Stage B 施工时会看到的
起点——一次窄范围验收（发现 Step 0 与 Stage A 之间的前提已经变化），加上验收本身修复
F1-F3 时顺带推进了 `src/cfg.rs`/`src/bin/server.rs` 的行号（例如
`validate_snapshot_catalog` 因为同一个 PR 里 `ClusterConfig` 新增字段又往下移了
~46 行）。本次订正（`feat/phase4-scheduler-fold` 分支，本次订正提交前的 HEAD
为 `a625959`）里出现的所有行号都是对着这个 HEAD 重新核对过的，**不是**照抄验收
agent 交下来的数字——两者在个别位置不一致，以本文档为准，但下一次订正前仍然建议
重新 `grep`/`wc -l` 核对一遍，而不是假设行号在此后没有继续漂移。

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
| `migrate.go` | 703 | 手写版本化 migration applier（advisory lock + 版本表，**故意不用** migration 框架，见文件头注释） | 照搬思路，重写实现。见 §7 步骤 3 |
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
applier 机制本身要求换格式，见 §7 步骤 3）。

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

配置轴（`SnapshotCatalogConfig`：字段声明 `src/cfg.rs:508`，结构体本身
`:519`，`SnapshotCatalogWrite`/`SnapshotCatalogRead` 两个 enum `:564-584`）：

```
write ∈ { object_store, both, postgres }   AENV_SNAPSHOT_CATALOG_WRITE
read  ∈ { object_store, postgres }         AENV_SNAPSHOT_CATALOG_READ
```

合法组合由 `AppConfig::validate_snapshot_catalog`（`src/cfg.rs:1816-1864`，
本次订正时点重新核对——注意这个函数的绝对行号会随 `cfg.rs` 里更早的任何改动漂移，
下次订正前建议先 `grep -n "fn validate_snapshot_catalog"` 重新核对而不是照抄本文档）
在启动时强制：

| write \ read | object_store | postgres |
|---|---|---|
| `object_store` | ✅ 今天的默认 | ❌ 拒绝：读一张没人写的表 |
| `both` | ✅ 双写，读对象存储 | ✅ **唯一合法的「读 PG」状态**，见 §5 |
| `postgres` | ❌ 拒绝：写一张没人读的表 | ❌ **本 build 拒绝**（`write=postgres` 未实现，见下） |

读侧从 `object_store` 切到 `postgres` 时经过的完整链路（都在
`src/snapshot/repository/backends/mod.rs::build_snapshot_backend` 里，
`45-207` 行区间）：

1. 装配对象存储侧的 `SnapshotRepository`。
2. `build_central_catalog`（`285-318`）：只有 `write == Both` 才会连
   `cluster.scheduler_endpoint`（`AENV_OBSERVABILITY_SCHEDULER_ENDPOINT`）构造
   `CentralSnapshotCatalog::connect_hot_reloadable`；`write == ObjectStore` 返回
   `None`；`write == Postgres` **直接 `bail!`**。🔴 **不再是 `connect_lazy`**——Step
   0.5（`6d09c50`）把这一跳换成了热重载版本，`cluster.scheduler_endpoint_file`
   变了不用重启就能生效。这不是无关细节：§7 步骤 9「切换 central 实现来源」把这一跳换成
   `PostgresSnapshotCatalog::connect(dsn)` 时，必须显式决定直连 PG 的这份连接/池是否也要
   继承这个热重载能力，还是接受“换 DSN 需要重启副本”这个更弱的保证——旧文档写的
   `connect_lazy` 会让实现者以为这从来就不是一个要考虑的问题。
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

并且 `src/cfg.rs:1857-1862`（`validate_snapshot_catalog` 的
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
  是 `SnapshotManager::new`（`src/snapshot/manager.rs:166`）唯一的装配路径，
  在 `--role api`（`assemble_api`，`src/bin/server.rs:956`）与 `--role all`
  （`assemble_node_core` 内部共享路径，`server.rs:434`，调用点 `:477`）启动时同步调用；
  返回 `Err` 会让 `async_main` 直接失败退出（K8s 视为 Pod 启动失败 → CrashLoopBackOff）。
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
  cluster_id)`，而不是 `CentralSnapshotCatalog::connect_hot_reloadable(scheduler_endpoint,
  ...)`（Step 0.5 之后的现状，不再是本文档最初写的 `connect_lazy`——见 §2 订正，这一步
  同时必须显式决定直连 PG 是否也要保留热重载能力）。这一步把
  `SnapshotCatalogWrite::Postgres` 分支里的 `anyhow::bail!` 替换成真正的构造调用——
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
（该文件本身现为 **1484** 行，非测试+测试合计——旧数字 1447 已随文件增长漂移；
`convert.rs` 那 538 行是同目录另一个文件，未受影响）的三处 RPC 调用
（`begin_snapshot` `:374`、`commit_snapshot` `:440`、`fail_snapshot` `:496`）
全部把 `paused_transition: None` 写死，注释原文：

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
    **1484**+538 行——`mod.rs` 本身、`convert.rs`；旧数字 1447 已随 `mod.rs`
    增长漂移）：gRPC 到 scheduler，本次要被取代/退居 fallback 的对象。
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

- **Cargo 依赖：已拍板，不是本节要做的决定**。🔴 这条与本文档最初的调研结论矛盾，订正于此：
  `sqlx` 0.8 已经是仓库的直接依赖（`Cargo.toml:128`，`7caaf95` 引入），而不是
  「没有任何 Postgres 客户端依赖」。选型理由与 Go 侧「不用 migration 框架」的判断
  **没有冲突**：`Cargo.toml:120-121` 的依赖注释明确写着「Runtime-query API only
  (`sqlx::query`/`query_as`), never the `query!` compile-time macros」——也就是说，
  团队选了 `sqlx`,但**只用它的运行期查询 API,不用编译期宏也不用 `sqlx::migrate!`
  的自动化迁移机制**,这正是本文档原本想通过选 `tokio-postgres` 而非 `sqlx` 来保住的
  那条判断,只是保住的方式不同（限制用法，而不是换库）。Stage B 不需要再讨论选型，
  直接用已经在依赖树里的 `sqlx`（`runtime-tokio-native-tls` + `postgres` feature）。
- **PG 连接池：已经建好，不在 `backends/` 下，且零生产调用方**。`src/pg/pool.rs`
  的 `PgPoolSettings`/`connect`（`src/pg/mod.rs` re-export）已经是「一个新的
  PG 连接池模块」——不是待办事项，是已经存在的代码：
  - DSN 走 `[pg].dsn`（`src/cfg.rs:600` 起的 `PgConfig`），与 `[backend.oss]`/
    `[backend.posix_fs]` 同一种约束下的同一种解法——`PgConfig` 只 `#[derive(Deserialize)]`
    不进 confique 的 `Config` 派生，DSN **只能**通过 `AENV_CONFIG_OVERLAY_PATH`
    的文件设置，不支持 `AENV_...` 环境变量直接注入。这正是本文档原本担心「如果放进
    `#[config(nested)]` 段就不能用 env var」的那个坑，但它已经在设计 `[pg]` 的时候
    被**主动选择**了（不是漏做），k8s 侧通过 Secret 投影成的 overlay 文件供给
    （与 `[backend.oss]`/`[backend.posix_fs]` 同一套机制，CLAUDE.md 的 `cfg.rs` 一节
    已经点名）。
  - 连接数上限**不需要新配置项**：`[pg].max_connections`（`Option<u32>`，默认
    `src/pg/pool.rs:30` 的 `DEFAULT_MAX_CONNECTIONS = 8`）已经存在，文档字符串里已经写明
    「乘以副本数不能超过 Postgres `max_connections`」——不需要重新发明
    `snapshot.catalog.postgres_max_connections`。
  - **但这个池今天没有任何生产调用方**（`src/pg/pool.rs` 自己的文档字符串写着
    "nothing yet consumes this pool"）——Stage B 缺的不是"建池"这一步，是"把这个已经建好的池
    接进 catalog 的装配路径"这一步。这一步在旧的施工清单里完全没有出现，本次订正把它
    补进 §7（新增步骤，插在原步骤 1 和 2 之间）。
- **Migration runner**：port `migrate.go` 的版本表 + advisory lock 思路（不引入框架），
  三份 `.sql` 文件本身照搬（可以直接引用/复制 `services/scheduler/internal/catalog/
  migrations/` 下的文件，保持两侧 schema 定义单一来源，或在 Rust 侧建一份物理副本并
  用测试固定「两边字节相同」防止漂移——两种做法各有取舍，留给实现者按仓库惯例决定）。
  🔴 **advisory lock 的 key 必须逐字复用 Go 的 `schemaLockKey = 0x0A6E_7653_4348_4D41`**
  （`src/pg/lock_keys.rs` 顶部文档注释已经记录了这个值和两个 Go 侧定义它的位置：
  `services/scheduler/internal/registry/migrate.go:183` 与
  `services/scheduler/internal/catalog/migrate.go:113`），**不能**在
  `AdvisoryLockKey` 枚举里新加一个变体给它——这两个 Go 常量与 `AdvisoryLockKey`
  的每个变体共享同一个 64 位 PostgreSQL advisory lock 键空间（单参数 `bigint` 形式），
  `lock_keys_never_collide_with_the_go_advisory_locks` 这条测试守的前提正是
  「`AdvisoryLockKey` 的取值范围与两个 Go 常量互不相交」，把 schema 锁也塞进这个枚举
  会破坏这个前提而不是遵守它。
- **`RunBuildReaper` 等价物**：一个 Tokio 后台任务，周期扫描
  `builds_active_idx`（谓词 `status_group IN (pending,in_progress)`），
  对心跳过期的行标记失败。🔴 **不要照抄 `src/orchestrator/` 的 auto-eviction 任务作为
  多副本并发模式**——那个任务恰恰是反例：它没有做 leader election，`--role api`/
  `--role all` 的每个副本各自独立跑一份，之所以安全是因为它操作的是每个副本自己持有的
  内存态而不是共享的 PG 表。build reaper 操作的是所有副本共享的 `builds` 表，如果照抄
  auto-eviction 的「每副本各自起一个定时任务」，会变成 N 个副本同时扫描、同时争抢同一批
  过期 build 行（§9 风险 4 已经点出这个问题）。真正该复用的原语是
  `src/pg/election.rs:124` 的 `spawn_singleton_task`——它已经用 PostgreSQL
  session-scoped advisory lock 实现了「集群内只有一个副本真正执行」，且
  `AdvisoryLockKey::CatalogBuildReaper = 1`（`src/pg/lock_keys.rs:58`）已经为这个用途
  按名预留好了键，不需要再设计新的选主机制。
- **Prometheus 指标**：`agentenv_scheduler_catalog_rpc_total`、
  `agentenv_scheduler_catalog_rejected_total`、
  `agentenv_scheduler_catalog_builds_reaped_total`、
  `agentenv_scheduler_catalog_build_reaper_warmup_passes_total`、
  `agentenv_scheduler_catalog_build_clock_skew_total` 五个指标要在 Rust 侧重建
  （改名或保留原名是运维层面的决定，需要和现有 dashboard/告警对齐，本文档不代为决定）。

### 6.3 可复用的 Stage A 模式（新增小节）

Stage A（`docs/proposals/_sd-phase4-stageA-node-inventory.md`）已经踩过几个 Stage B
会再踩一遍的坑，模式可以直接借，但有的地方借的时候必须带上限制条件一起借，不能只抄形状：

- **开关驱动的子系统构造**：`assemble_api`（`src/bin/server.rs:956`）里
  `match config.cluster.node_placement_source { Native => Some(...), Scheduler => None }`
  这段（`server.rs:987-1012`）是「一个配置开关决定要不要装配一整套子系统（发现客户端、
  gRPC 服务、后台任务）」的现成形状，`write != ObjectStore` 时要不要装配
  `PostgresSnapshotCatalog`/build reaper 可以直接照这个形状写。**但**有一个具体细节
  不能照抄：这段代码把子系统的后台任务句柄汇入 `paused_upkeep`
  （`Vec<tokio::task::JoinHandle<()>>`，`server.rs:95`），进程关停时对这个 `Vec`
  里每一项调用 `.abort()`。build reaper 如果用 `spawn_singleton_task`
  （§6.2/§7 步骤 5），拿到的是一个 `SingletonTaskHandle`，它的优雅关停方法是
  `async fn shutdown(self)`（`election.rs:100`，**消费 `self`，不是
  `JoinHandle`**）——直接把它硬塞进 `paused_upkeep` 然后当成 `JoinHandle` 一样
  `.abort()`，会跳过 `shutdown()` 里释放 PostgreSQL advisory lock 的那部分，把锁
  留到底层连接被物理断开（进程退出、连接池整体销毁）才释放，而不是干净地在
  `shutdown()` 里主动 `pg_advisory_unlock`——这不是编译错误，`.abort()`
  对什么类型的句柄都能编，是一个只有跑起来才会看见的资源泄漏。`SingletonTaskHandle`
  需要自己的关停路径，与 `paused_upkeep` 分开管理，不能直接 `push` 进那个 `Vec`。
- **dump 观测钩子的两条纪律**：`node_registry::dump` 遵守的两条纪律（两个数并排给，
  不是只给好看的那个；没有诚实答案就留空，不是编一个凑数的）同样适用于 Stage B。
  对应的数据结构已经存在：`CatalogPopulations`（`mirror/population.rs:69`
  的 `compare` 方法），装的正是「对象存储有多少行、central 有多少行、双向各自缺哪些
  id」——两个数并排的形状已经在结构体里了。但**今天它只出现在
  `build_snapshot_backend`（`backends/mod.rs:139-151`）里的一行 `tracing::info!`**，
  没有对外的调试端点。旧版 §7 施工清单完全没有「把 `CatalogPopulations` 暴露成一个
  可查询的观测钩子」这一步——本次订正把这个缺口标注出来，但不代为决定要不要现在补：
  如果 Stage B 要补，应该新增一步，产出形状类似 `node_registry::dump`（同样两个数
  并排、同样没有诚实答案就留空），而不是塞进现有步骤里顺手做。
- **挂载新调试路由必须用 `new_with_control_plane_routes`，不能 `.route()`**：这是本轮
  （F1）刚修过的坑，`/debug/node-registry` 曾经因为在 `server::new`
  返回的路由器上直接 `.route(...)` 而完全绕过控制面网关和角色网关。如果 Stage B
  要新增任何调试/观测端点（例如上面那条 `CatalogPopulations` 的查询钩子），必须走
  `agentenv::api::server::new_with_control_plane_routes` 的 `extra_control_plane_routes`
  参数，在 `assemble` 附加网关**之前**合并进生成的路由器——`src/api/server.rs`
  自己的模块文档和
  `extra_control_plane_routes_require_the_control_plane_credential` 测试记录了完整
  原因，不要重新踩一遍。

---

## 7. 施工清单（每步独立编译通过、独立可回退）

🔴 **本节相对最初调研已重排**：`src/pg`（连接池 `pool.rs`、选主原语
`election.rs`、锁 key 登记表 `lock_keys.rs`）在本文档写完之后已经先行落地
（Step 0.5 附带工作），但**零生产调用方**——`src/pg/pool.rs` 自己的文档字符串写着
"nothing yet consumes this pool"。这改变了两件事：(a) 「加依赖」不再是第一步要做的
决定，因为依赖已经加了；(b) 两个原本排在后面、依赖「先有 catalog trait 实现」的步骤
（读侧准入落 PG、build reaper）其实只需要「池 + 一张表」，不需要等
`PostgresSnapshotCatalog` 的读写路径写完——移到前面能更早交付价值（尤其是读侧准入落
PG，它单独就是 CrashLoopBackOff 的结构性修复）。步骤编号已按这个新顺序重排，不是在旧
编号上打补丁。

1. **确认依赖，新建骨架**：`sqlx` 0.8 已经是直接依赖（`Cargo.toml:128`，
   `runtime-tokio-native-tls` + `postgres` feature，`7caaf95`），这一步不用再加依赖或
   重新选型——上一版文档「需要新增 Postgres 客户端依赖」的结论已经过期，见 §6.2 订正。
   新建（如果还没有）`src/snapshot/repository/backends/postgres/` 模块骨架用于放
   `PostgresSnapshotCatalog` 本身，`cargo build` 通过，不接入任何调用路径。
2. **接池（新增步骤，原文档没有）**：`src/pg` 目前零生产调用方——`build_snapshot_backend`
   （`src/bin/server.rs:956` 的 `assemble_api` → `SnapshotManager::new(None)`
   `:1058` → `manager.rs:166` → `build_snapshot_backend`
   `backends/mod.rs:45` → `build_central_catalog` `:285`）只收
   `Option<Arc<dyn P2pTransport>>`，配置靠 `ConfigManager::global_config()` 读，没有
   任何一层把 `PgPool` 传下去。这一步要么把 `PgPool` 穿过
   `build_snapshot_backend`/`build_central_catalog` 两层签名，要么做成进程全局——
   **这个签名改动会同时落在 `--role all` 的路径上**
   （`assemble_node_core` `server.rs:434` → `SnapshotManager::new`
   `:477` 同一条 `build_snapshot_backend`），而**那里不能建池**：`--role node`
   （`assemble_node`/`assemble_all` 共用 `assemble_node_core`）绝不能拿到 PG DSN
   （见步骤 10），所以池必须在 `--role api`/`--role all` 各自的调用点按需传入，不能
   变成 `build_snapshot_backend` 内部无条件构造的东西。不接入任何 catalog 逻辑，只是
   让池能在正确的角色下被正确地传递到位，`cargo build` 通过即可。
3. **Migration runner + schema 落地**：port `migrate.go` 的版本表/advisory lock
   applier，三份迁移 SQL 照搬进来（新增第 4 份 `0004_catalog_migration_state.sql`
   落 §5.1 的确认表）。🔴 advisory lock 的 key **必须逐字复用** Go 的
   `schemaLockKey = 0x0A6E_7653_4348_4D41`（`src/pg/lock_keys.rs` 已记录），**不能**
   在 `AdvisoryLockKey` 里新加变体——那会破坏
   `lock_keys_never_collide_with_the_go_advisory_locks` 守住的「这个枚举与两个 Go
   常量的取值范围互不相交」这个前提（见 §6.2）。有独立测试：对一个空库跑两遍都成功、
   版本表内容正确、`DROP TABLE ... CASCADE` 回退命令验证过。不接入
   `build_snapshot_backend`。
4. **读侧准入落 PG（§5.1，原步骤 7，前移到这里）**：只需要步骤 2 的池和步骤 3 的
   `catalog_migration_state` 表，**不需要等 `PostgresSnapshotCatalog` 的读写路径写完**
   ——新增该表的读写逻辑，替换 `admit_read_side` 的本地 `MirrorBacklog` 判断依据。
   这一步独立交付 CrashLoopBackOff 的结构性修复（§4 的结构性问题）。**这一步单独
   灰度**：先在一个非生产集群上把 `write=both, read=object_store` 切到
   `write=both, read=postgres`（此时 central 仍是 `CentralSnapshotCatalog`，走 gRPC
   到 scheduler，只是准入判断换了落点），验证新 Pod 启动不再因为 `emptyDir`
   而反复重放比对。
5. **Build reaper 后台任务（原步骤 5，前移到这里）+ 五个 Prometheus 指标**：同样只需要
   步骤 2 的池和步骤 3 的 `builds` 表，不需要等 catalog trait 的读写路径。用
   `src/pg/election.rs:124` 的 `spawn_singleton_task` 而不是照抄
   `src/orchestrator/` 的 auto-eviction 任务——那个任务**是反例，不是可复用的模式**：
   它没有做选主，`--role api`/`--role all` 每个副本各自独立跑一份，之所以安全是因为
   操作的是每个副本自己的内存态；build reaper 操作的是所有副本共享的 `builds`
   表，照抄「每副本各自起一个定时任务」会变成 N 个副本同时扫描、同时争抢同一批过期
   build 行（§9 风险 4）。`AdvisoryLockKey::CatalogBuildReaper = 1`
   （`src/pg/lock_keys.rs:58`）已经按名预留好键，直接用。**两个注意事项**：
   - `SingletonTaskHandle::shutdown()`（`election.rs:100`）是 `async` 且消费
     `self`，但**不会抢占正在执行中的任务体**——如果 reaper 单趟扫描/更新耗时较长，
     graceful shutdown 会等这一趟跑完才返回；reaper 单趟必须自限时长（例如一次只处理
     有限行数、设置语句超时），否则一次关停可能被拖到超出 Pod 的
     `terminationGracePeriodSeconds`。
   - leader 任期内 `spawn_singleton_task` 独占一条连接（见 `election.rs` 自己的文档：
     "`body` runs on the pool connection this function is holding the lock on ...
     for as long as this replica remains leader"）——`[pg].max_connections`
     默认 8（`src/pg/pool.rs:30`）要为这条常驻连接留出余量，不能全部当成短查询的
     周转容量来算。
   独立可测（构造一个过期心跳的 build 行,断言被标记失败）。
6. **`PostgresSnapshotCatalog` 只读路径**：实现 `SnapshotCatalog` trait 的
   `get`/`list`/`resolve_alias`（对应 `queries_resolved.go`），配套单元测试（可以
   先用现有的 fake catalog 测试模式打底,再补一套针对真实 PG 的集成测试，见 §8）。
   不接入配置装配，只在测试里手工构造使用。
7. **`PostgresSnapshotCatalog` 写路径**：`begin`/`commit`/`fail`/`delete`/
   `start_build`/`renew_build_lease`/`get_build`（对应 `queries_admin.go`），
   fencing/CAS 逻辑照抄 Go 侧 SQL 的 `WHERE` 条件与 partial unique index 依赖。
   仍不接入配置装配。
8. **接入 `build_snapshot_backend`**：`build_central_catalog` 改为在
   `write != ObjectStore` 时构造 `PostgresSnapshotCatalog`（而不是
   `CentralSnapshotCatalog`），删掉 `SnapshotCatalogWrite::Postgres` 分支的
   `bail!`。**这一步是切口**——之前的所有步骤都不改变现网行为，这一步开始才会真正
   有流量走新代码。建议先在 `write=object_store`（默认值）下合入并跑通全部 CI，
   确保这条新代码路径「存在但默认不生效」。
9. **切换 central 实现来源**：把 `write=both`/`postgres` 时构造的「central catalog」
   从 `CentralSnapshotCatalog::connect_hot_reloadable(scheduler_endpoint, ...)`
   （Step 0.5 之后的现状，**不再是** `connect_lazy`）换成
   `PostgresSnapshotCatalog::connect(dsn)`。🔴 这不是一次简单的
   「`connect_lazy` → `connect(dsn)`」替换：要显式决定直连 PG 这条新路径是否也要保留
   `scheduler_endpoint_file`/DSN 热重载的能力，还是接受「换 DSN 需要重启副本」这个更弱
   的保证——旧文档写的是已经过时的 `connect_lazy`，没有这个问题需要决定，订正后必须
   把这个决定显式做出来（见 §2 订正）。回退开关：配置层面把 `write` 切回
   `object_store`（对象存储副本仍在，双写保留到这一切都稳定之后才考虑拆——与
   `docs/proposals/2026-08-20-service-decomposition.md` 阶段2 的「双写保留到阶段3
   上线并稳定之后再拆」是同一条原则,继续沿用）。
10. **`--role node` 的路由**：`central/mod.rs` 文件头写明的安全边界（「DSN 不落在跑
    用户代码的机器上」）今天**已经在启动期被强制**，不再只是一条需要人工小心遵守的
    约定——`ServerRole::check_pg_dsn`（`src/role.rs:239-251`）在 `--role node`
    配了 `[pg].dsn` 时直接拒绝启动，`src/bin/server.rs:309` 在任何角色专属装配开始前
    调用它，且有源码扫描守卫钉住这个调用没被移除或绕过。所以步骤 2「接池」即使写错、
    把池不小心传给了 `assemble_node_core`，**结果是进程启动失败**而不是「node 悄悄
    拿到了数据库凭据」——这把原来「一个纯人工纪律」的风险降级成了「一个编译期就能类型
    检查、启动期兜底拒绝」的风险。至于 `--role node` 场景下 `SnapshotManager`
    该走哪条路径这个问题本身（§6.1 提到它今天也用 `SnapshotCatalog` trait 但只读），
    已经在 `_sd-phase4-open-questions-resolved.md` 的 Q3 里给出建议答案：不新建
    node→api RPC 面，让 api 在 dispatch 时把已解析的快照记录连同现有 RPC 消息一起
    发给 node，node 侧完全不碰 catalog——这一步降级为「施工时确认并按 Q3 记录的方案
    实施」，不再是一个需要临场拍板的未知数。
11. **`services/scheduler` 侧收尾**：`catalog_service.go` 的 9 个 RPC 方法体保留
    （给还没切换的节点/回退路径用），但停止把它当作「唯一实现」来维护；
    `internal/catalog` 包本身在这一阶段**不删除**（回退需要它）。真正的删除留给
    Stage E。
12. **契约测试落地**（可以穿插在 4-7 步之后做，不必等到最后）：见 §8。
    🔴 **陷阱**：`make test-with-postgres`（`Makefile:189-207`）执行的是
    `cargo test -p agentenv --lib pg:: -- --nocapture`（`Makefile:199`）——**这是一个
    名字过滤器**，只跑模块路径匹配 `pg::` 的测试。Stage B 新增的 catalog 契约测试如果
    放在 `src/snapshot/repository/backends/postgres/` 之类的模块路径下（这是这份文档
    自己建议的落点），**不会被这个过滤器选中**——不会报错、不会失败,只会在
    `AENV_PG_TEST_REQUIRED=1` 的必需模式下被静默排除，永远显示为 skip（CI 的
    `.github/workflows/ci.yml:111,134` 两步都靠这个目标兜底,同样会被静默绕过）。
    这些测试要么改名/挪进 `pg::` 这个模块路径下（哪怕只是 `mod pg_contract`
    这样一层壳），要么新建一个独立的 `make` 目标并同步更新 CI，两者选一，**不能什么都
    不做就假设 `make test-with-postgres` 会覆盖它们**。

---

## 8. 测试面

- **Go 侧现状**：`internal/catalog` 目录下 **5**（不是 7——原文档计数有误，行数和
  5,535 这个数字本身是对的，数错的是文件数：实际是
  `migrate_test.go`/`pin_test.go`/`queries_test.go`/`schema_test.go`/
  `store_postgres_test.go` 五个文件）测试文件，5535+1546=约 7,081
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
  5. 🔴 `make test-with-redis` 类比的目标**已经存在，不是待新增项**——原文档写作时它
     还没有，现在已经落地：`make test-with-postgres`（`Makefile:189-207`）把「无 PG
     则跳过、`AENV_PG_TEST_REQUIRED=1` 则跳过变失败」这条 Go 侧已经验证过的模式在
     Rust 侧重建了，CI 也已经接进去（`.github/workflows/ci.yml:111` 设置
     `AENV_PG_TEST_REQUIRED: "1"`、`:134` 调用 `make test-with-postgres`）。
     Stage B 要做的不是新建这个目标，而是**确保新增的 catalog 契约测试真的落在这个
     目标能看见的地方**——见 §7 步骤 12 的 `pg::` 过滤器陷阱，这才是这个目标今天唯一
     还没被验证过的地方：它只被 `src/pg/` 自己的测试练过，还没有被“一个新模块的测试
     依赖它才能通过”这种用法验证过。

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
3. 🔴 **已订正——这不再是「不确定是否要动」的高风险点，已降级**。原文档担心「如果
   Stage B 简单粗暴地把所有 `write != ObjectStore` 场景统一换成直连 PG，`--role node`
   会意外拿到 PG DSN」，并要求「施工前必须先回答 `--role node` 到底连谁」。两点都已经
   有答案：(a) `central/mod.rs` 文件头写明的安全边界今天**已经在启动期被强制**——
   `ServerRole::check_pg_dsn`（`src/role.rs:239-251`）在 `--role node` 配了
   `[pg].dsn` 时直接拒绝启动，`src/bin/server.rs:309` 在任何角色专属装配开始前调用它，
   有源码扫描守卫钉住这条调用没被移除——把这条风险从「一个纯人工纪律，实现时可能
   疏忽」降级成了「一个启动期就会兜底拒绝的配置错误」；(b) `--role node`
   到底该连谁这个问题本身，已经在 `_sd-phase4-open-questions-resolved.md` 的 Q3
   里给出建议答案（不新建 node→api RPC 面，让 api 预解析后把记录带下去）。剩下要做的
   是施工时**确认并按 Q3 记录的方案实施**，不是在施工前重新决策——见 §7 步骤 10、
   §11 开头的订正说明。
4. **build reaper 是唯一实例后台任务**——Go 侧只有一个 `scheduler` 进程在跑
   `RunBuildReaper`；Rust 侧是 N 个 `--role api` 副本，如果直接照搬「每个进程自己起一个
   定时任务」，会变成 N 个副本同时扫描、同时争抢同一批过期 build 行。虽然
   `builds_active_idx` 的 SQL 本身是幂等的（UPDATE 一行已经被别人标记失败的 build
   不会造成数据损坏),但 N 倍的扫描频率和潜在的行锁竞争是不必要的开销,需要一个
   leader-election 或者「谁抢到就谁做」的机制。🔴 **已订正**：原文档建议「参考
   orchestrator 自己的 auto-eviction 任务在多副本场景下是怎么处理的」并把这一点列为
   未核实的待确认项（旧 §11.2）——现在核实过了，**这个参考方向是错的**：
   orchestrator 的 auto-eviction 任务**没有做选主**，`--role api`/`--role all`
   每个副本各自独立跑一份，之所以安全是因为它操作的是每个副本自己的内存态而不是共享的
   PG 表，跟 build reaper 面对的问题不是同一类。真正该用的原语是
   `src/pg/election.rs:124` 的 `spawn_singleton_task`，用 PostgreSQL
   session-scoped advisory lock 做选主，`AdvisoryLockKey::CatalogBuildReaper = 1`
   （`src/pg/lock_keys.rs:58`）已经按名预留好键——这个原语已经存在，不需要 Stage B
   自己设计一个。具体的两个实现注意事项见 §7 步骤 5。
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

🔴 本节相对最初调研删掉了四项——不是因为它们不重要，是因为它们已经有答案了，留着会让
下一个读这份文档的人重新纠结一遍已经决定过的事：

- ~~`--role node` 在 Stage B 完成后应该连谁做目录访问~~——已在
  `_sd-phase4-open-questions-resolved.md` Q3 答复：不新建 node→api RPC 面，让 api
  预解析后把记录带下去。降级为「施工时确认并按 Q3 记录的方案实施」，见 §7 步骤 10、
  §9 风险 3。
- ~~build reaper 在多副本 `--role api` 下的并发策略~~——原语已有：
  `src/pg/election.rs:124` 的 `spawn_singleton_task`，键
  `AdvisoryLockKey::CatalogBuildReaper = 1`（`src/pg/lock_keys.rs:58`）已按名预留。
  文档原本建议参考的 orchestrator auto-eviction 任务是反例（没有选主），见 §9 风险 4。
- ~~PG 客户端库选型：`tokio-postgres`/`deadpool-postgres` vs `sqlx`~~——已拍板，
  `sqlx` 0.8（`7caaf95`），见 §6.2。
- ~~`snapshot.catalog.postgres_max_connections` 之类新配置项的默认值~~——不需要新
  配置项，`[pg].max_connections` 已存在，默认 8（`src/pg/pool.rs:30`），见 §6.2。

还真正待确认的两项：

1. **`catalog_migration_state` 表是否应该按 `cluster_id` 分行还是全局单行**——
   本文档假设是按 `cluster_id`（因为 `snapshots` 表本身是多 cluster 的),但没有
   找到「一个 Postgres 库是否会同时服务多个 cluster_id」的明确证据来验证这个假设
   是否必要（也可能整个 catalog 库天生就是单 cluster 一个库,这张表就不需要
   `cluster_id` 这一列）。
2. **Go 侧 `catalogGate`（`catalog_service.go` 里的 `type catalogGate interface{
   Require() error }`）与 Rust 侧 `admit_read_side` 是否需要在 Stage B 里统一成
   一套「schema 未就绪则拒绝」的机制**——本文档把两者当成概念上独立的两道门
   （§4 vs §6 的 `migrateCatalog`/`registryGrace`）分别讨论,但没有确认 Rust 侧
   是否已经有等价于 Go 的 `Grace`/`grace.Enter` 的「本 build 自己的 schema
   还没跑完之前拒绝服务」机制,还是要在 Stage B 里新建一个。
