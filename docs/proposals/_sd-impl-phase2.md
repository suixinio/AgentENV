# 阶段 2 实施规格：目录进 PG，对象存储降为纯字节

> 2026-08-20 · **写给照着这份文件动手的人**。读完这一份就够，不需要回头翻父提案。
>
> 父提案：[`2026-08-20-service-decomposition.md`](2026-08-20-service-decomposition.md) §5 / §7 阶段 2 / §8 陷阱 3
> 模块归属：[`2026-08-20-module-responsibilities.md`](2026-08-20-module-responsibilities.md) D2 / D6 / D12 / §7
> 落地环境：[`_sd-recon-env.md`](_sd-recon-env.md) §4（依赖就绪度）/ §8（验证方法论）/ §9（阻塞项）
>
> e2b 考古基准：`/home/debian/e2b-infra`（HEAD `6938cbb72`）。本文所有 `packages/...` 路径都指它。
>
> 🔴 **本文对父提案提出 10 处更正，其中 3 处会改变阶段 2 能不能照着做。** 全部在 §12。
> 最重的一条：父提案 §4.2.1 把「消掉 `local_only`」挂在一个**本仓之外的**交付物上。
> **那条依赖出了范围**（用户裁决 2026-08-20，全部工作只在 `/home/debian/AgentENV` 内）。
> ⇒ 父提案说的"退路"不是退路，是**唯一的设计，而且现在就建**。见 §3.1。

---

## 0. 三十秒版

阶段 2 要做的事一句话说得清：**把快照目录从对象存储搬进 PostgreSQL，让 `api` 层有一个
能原子提交的地方**（父提案 §5.1）。但落到代码上，它撞上三堵墙：

| 墙 | 事实 | 后果 |
|---|---|---|
| **Rust 连不上 PG** | `grep sqlx\|tokio-postgres\|diesel --include=Cargo.toml .` **零命中**；`Cargo.lock` 843 个 crate **零 db 依赖** | 「`SnapshotRepository` 的目录读写走 PG」在阶段 2 **字面上无法执行** |
| **阶段 2 时代码全跑在 node 上** | `grep -rn "role" src/bin/server.rs` 零命中（recon SD-B2）；`--role` 拆分是阶段 3 | 谁拿 PG 凭据 = 每台跑用户代码的 KVM 机器拿 PG 凭据 ⇒ **违反 §0 硬约束 2** |
| **集群里没有 Redis** | recon §4.5：`kubectl get pods -A \| grep -i redis` 零命中；两台机器都没有 `redis-server`（阶段 1 的 `deploy/k8s/base/redis.yaml` 已写好但**未 apply**） | §8 陷阱 3 要求「目录缓存同批进 Redis」，而那个 Redis 是配成 `noeviction` 的**耐久存储**，装不得缓存 ⇒ §7.2 把目录缓存降级成条件项 |

**本文的答案**：目录归 **scheduler 的 Go 侧**，node 通过 gRPC 访问，与今天
`service PausedRegistry`（`services/api/proto/scheduler.proto:498-516`）**逐字同形**。
决定性理由不是"守规矩"，而是：**只有这样，`complete_pause` 与目录提交才可能是同一个事务**
—— 而那正是父提案 §5.1 要买的东西。让 Rust 直连 PG 反而买不到它（§1.4）。

**并且这不是一个阶段。** 8,000–9,500 行、跨两种语言、一个新 proto service、一套新的迁移机制、
一个新的基础设施依赖（Redis）、外加一个**不能改形状的公开 API 字段**（`x-next-token`）。
拆成 2a / 2b / 2c，各自可上线可回退（§11）。

---

## 1. 🔴 中心问题：谁在什么时候连 PG

父提案 §7 阶段 2 只写了一句「`SnapshotRepository` 的目录读写走 PG」，没说是谁的进程在连。
这是整个阶段的分水岭，必须先答。

### 1.1 事实基线

| 事实 | 位置 |
|---|---|
| Rust workspace **零 PG 依赖**（21 个 `Cargo.toml`，843 个锁定 crate） | `grep -rn "sqlx\|tokio-postgres\|postgres\|diesel\|deadpool-postgres" --include=Cargo.toml .` 空；`Cargo.lock` 同 |
| PG 只在 Go 侧，唯一驱动 `pgx/v5`，无 ORM 无 sqlc | `services/go.mod:7`；`services/scheduler/internal/registry/store_postgres.go:1-15` |
| Rust 的 PG 后端是**被刻意物理删除的**，不是"还没写" | 提交 `4208f47 refactor(paused-registry): take PostgreSQL off the node` |
| 遇到 `backend = "postgres"` 直接 `bail!`，错误信息逐字指向 scheduler | `src/orchestrator/paused_registry/mod.rs:436-443` |
| 删除的理由写在模块文档里 | `src/orchestrator/paused_registry/central.rs:1-21`：「the DSN, the connection budget and the schema stop being the business of every machine that runs user code」 |
| `--role` 在代码里不存在 | `grep -rn "role" src/bin/server.rs` 零命中（recon SD-B2） |
| node DaemonSet **今天没有任何 PG env** | `deploy/k8s/base/agentenv-daemonset.yaml:58-62` 只有 `AENV_PAUSED_REGISTRY_BACKEND` |
| scheduler 的 DSN 来自 Secret `agentenv-postgres` / key `dsn`，`optional: true` | `deploy/k8s/base/scheduler-deployment.yaml:58-63` |

🔴 **一条必须同时说清的反面事实**：node **今天已经持有共享存储凭据** —— OSS 的
`access_key_id` / `access_key_secret` 明文写在 `agentenv-k8s-config` 的 `[backend.oss]` 段里
（recon §4.2）。所以"node 一尘不染"不是现状。
但这**不改变结论**：G7 摘掉的是**数据库 DSN**，而 `central.rs` 点名的三样东西
（DSN / 连接预算 / schema）都只对数据库成立。一个作用域为快照桶的对象存储密钥，
和一个能连上控制面唯一真相源的连接串，不是同一类东西。

### 1.2 三个选项，逐条评估

#### (a) Rust 加 PG 客户端，"只给未来的 `api` 角色用"

**它在阶段 2 违反硬约束 2 吗？违反。** 不需要绕弯：

阶段 2 落地时 `--role` 不存在，`src/bin/server.rs` 只有一种装配，跑在 DaemonSet 上。
「目录读写走 PG」⇒ DaemonSet 上出现 `AENV_SNAPSHOT_CATALOG_DSN` ⇒ 四个提交之前
`4208f47` 摘掉的东西原样装回去。outcome §1.3 把那次摘除称作「整个重构最硬的那条理由」。

「先加进来、等阶段 3 再收走」也不成立 —— 阶段 3 的前置里有 Redis（recon SD-B1 未解），
有 `--role`（SD-B2 未开工），还有 §3.1.b 那三步。**"暂时"的长度是不可控的。**

🔴 **而且它买不到阶段 2 要买的东西。** 见 §1.4。

#### (b) 目录走 scheduler 的 gRPC，阶段 3/4 再收进 `api` 进程内

代价诚实地摆出来：**一份 Go 实现活一到两个阶段，然后随 `scheduler` 一起删掉**
（父提案 §7 阶段 4：`scheduler` 进程下线）。约 4,000 行 Go 含测试是**明确的一次性投入**。

父提案 §2.2 反对「同一条接缝跨两次语言」，理由是
`_impl-D7-contract-tests.md` §4 的 S0–S11 十二条「Rust 语义在 Go 接口上表达不出来」。
**这条反对在这里只成立一半**，差别是可度量的：

| | `paused_sandboxes`（S0–S11 的来源） | 快照目录 |
|---|---|---|
| 状态机 | 5 态 × generation CAS × 租约 × execution fencing，**两侧都要推理** | 4 态（`Waiting/Building/Ready/Error`）＋ 一次 CAS |
| 载荷 | `SandboxMetadata` 要被 Go 侧解释（`metadata_golden_test.go` 322 行专门锁它） | `CommittedSnapshot` **可以完全不透明** |
| 可查询面 | 与载荷交织 | 10 个标量列，与载荷完全可分 |

⇒ **契约设计的硬要求**：wire 上只出现「可查询的标量列 ＋ 一段 Go 侧永不解析的 `bytes`」。
`CommittedSnapshot` / `OverlaybdLayerRef` / `ManagedLayer` / `CommittedAttachedDrive` /
`PersistedDiskImagePublication` / `ImageConfigs` / `CustomExtensionParams`
（`src/snapshot/types/snapshot.rs:284-303` 等）**一个都不镜像到 Go**。
这和 §4.2.2 里路由投影「五个字段的冻结契约」是同一招。

#### (c) 阶段 2 相对 `--role` 拆分重排

即：先把 `--role` 切出来，让 `api` 存在，再让它连 PG。

**不推荐，理由两条**：
1. 它反转父提案 §5 的硬前置（存储先动），而 §5 是两轮对抗审查都没推翻的部分；
2. `--role` 拆分（阶段 3）**当前被 Redis 阻塞**（recon SD-B1：集群里没有 Redis，
   阶段 3 的活跃态折叠一步都验不了）。用一个可解的约束换一个被阻塞的前置。

存在一个收窄变体 **(c′)**：只切出一个"薄 `api`"，只服务目录读端点，其余原样转发。
它能让 (a) 合法，但代价是 REST 面在整个阶段 2 期间**劈成两个进程**，
gateway 要按路径前缀分流（`/snapshots` 与 `/templates` 去 `api`，`/sandboxes` 去 node）。
**这比 (b) 的 4,000 行 Go 更贵，且回退面更大**（回退要同时改 gateway 路由与部署对象）。

### 1.3 ✅ 推荐：(b)

**目录归 scheduler 的 Go 侧，Rust 通过一个新的 `service SnapshotCatalog` 访问，
载荷不透明。** 阶段 3 `api` 角色出现后，它可以选择继续走 RPC 或直连；
阶段 4 `scheduler` 折叠进 `api` 时，Go 侧实现随之删除，Rust 侧换成直连 —— 表结构不变。

**四条理由，按重要性**：

1. 🔴 **只有它能兑现阶段 2 声称要买的东西**（§1.4，这是决定性的一条）。
2. **有现成的、已部署的先例**：`service PausedRegistry`
   （`services/api/proto/scheduler.proto:498-516`）就是「node 说要什么，控制器执行语句」。
   接线方式（lazy pool、注册在迁移之前、迁移完成前答 `UNAVAILABLE`、DSN 为空即整体关闭）
   在 `services/scheduler/cmd/main.go:117,131,436-465,518-537` 已经跑通。
   **加的是一个 proto service，不是一套新架构。**
3. **(a) 是严格意义上的一次性工作** —— 它必须被撤销；(b) 的 RPC 面**不是**：
   在目标形态里 node 本来就不读目录（e2b 的 node 从 Create RPC 拿 `templateID`/`buildID`
   然后读**存储**，不读 DB）。阶段 2 让 node 的目录访问变成一次 RPC，
   与它最终要变成「`api` 把解析好的产物递下来」是同一个方向。
4. **凭据面不动**：node 的 DaemonSet env 一行不加，D-9 漂移检查（recon §7.4）继续成立。

### 1.4 🔴 决定性的一条：只有 (b) 能让 pause 变成一个事务

父提案 §5.1 说「`api` 必须先有一个可以原子提交的地方」。**在 (a) 下这句话是假的。**

今天一次 pause 的持久化分两处落，中间没有任何原子性：

```
paused_coordinator.rs:288   registry.begin_pause()          → gRPC → scheduler → PG paused_sandboxes（state='publishing'）
paused_coordinator.rs:301   snapshot_manager.publish_captured() → 对象存储写字节 ＋ 写 catalog/records/{id}.json
paused_coordinator.rs:309   registry.complete_pause()       → gRPC → scheduler → PG（state='paused', snapshot_id=…）
```

| 方案 | `complete_pause` 在哪 | 目录提交在哪 | 能同事务吗 |
|---|---|---|---|
| 今天 | scheduler / PG | 对象存储 | ❌ |
| **(a)** | scheduler / PG（gRPC） | node 自己的 PG 连接 | ❌ **两条独立连接，跨进程** |
| **(b)** | scheduler / PG | **同一个 scheduler 的同一个 pool** | ✅ |

⇒ (a) 把目录搬进了 PG，却**没有**把两件事搬到同一个事务边界里 —— 它买到了 keyset 分页和
唯一索引，没买到「原子提交」。而后者才是父提案 §5.1 给出的、要在拆进程之前动存储的**唯一理由**。

**这条同时给出了一个父提案没写的兑现**：阶段 2 之后，
`complete_pause` ＋ 目录行提交可以合成一条多 CTE 的 data-modifying statement
（照 e2b `create_new_snapshot.sql` 的形状，见 §5.2），
"暂停成功但目录里查不到"这个窗口从此不存在。

### 1.5 契约边界：Go 侧允许知道什么

🔴 **这张表是 (b) 不退化成 S0–S11 的全部保证。写代码时逐条对照。**

| Go 侧 | 允许 | 禁止 |
|---|---|---|
| 标量列（§4 的 DDL 全部列） | ✅ 读、写、索引、谓词 | —— |
| `committed_payload bytea` | ✅ 原样存取、按长度校验 | ❌ 反序列化、按内容分支、定义 Go struct |
| `build_error jsonb` | ✅ 存取 | ❌ 解释字段（`TemplateBuildErrorReason` 有自定义 `Deserialize`，`types/snapshot.rs:86-107`） |
| `status` 四态 | ✅ CHECK 约束、CAS 谓词、`status_group` 派生 | ❌ 在 Go 里为它写状态机 |
| 别名 | ✅ 唯一索引、冲突返回 | ❌ 解析 `SnapshotAlias` 的字符集规则（那在 `value.rs:63-74`，Rust 侧校验后才上线） |

一句话：**Go 是这张表的 DBA，不是它的领域模型。**

---

## 2. 今天的目录长什么样

搬走之前要知道搬的是什么。以下全部实测。

### 2.1 目录的唯一实体是 `SnapshotRecord`

🔴 **父提案与任务书里说的 `SnapshotMetadata` 不存在** ——
`grep -rn "SnapshotMetadata" --include=*.rs .` 零命中。目录行是 `SnapshotRecord`
（`src/snapshot/types/snapshot.rs:328-337`）：

```rust
pub struct SnapshotRecord {
    pub id: SnapshotId,                        // Uuid v7 newtype, value.rs:7-12
    pub alias: Option<SnapshotAlias>,          // 🔴 至多一个别名，不是集合
    pub source: SnapshotSource,                // Template{build: TemplateBuildInfo} | Sandbox{source_sandbox_id}
    pub resources: SandboxResources,           // cpu_count u32 / memory_mib u32 / disk_size_mib u32
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub committed: Option<CommittedSnapshot>,  // None ⇒ 还没构建出来
}
```

`SnapshotId` 已经预备好了进 PG：`value.rs:26` 的 `to_uuid()` 注释逐字写着
「for callers that store snapshot IDs in a UUID-typed column」。

### 2.2 目录读写今天的形状（这是要被替换掉的东西）

| 关注点 | 今天 | 位置 |
|---|---|---|
| 行存储 | 一行一个 JSON 对象 / 文件 | `oss/layout.rs:17-19`、`posixfs/layout.rs:46-48` |
| 别名索引 | **另一个** JSON 对象，与行里的 `alias` 字段重复，无事务关联 | `oss/layout.rs:13-15`、`posixfs/layout.rs:38-40` |
| `list()` | `list_keys_recursive("catalog/records/")` ＋ **每条一次 GET**（16 路并发）＋ 内存过滤 ＋ 内存排序 | `oss/repository.rs:369-397`、`oss/client.rs:163-186` |
| POSIX `list()` | `read_dir` ＋ 每条一次读文件 ＋ 同样的内存过滤排序 | `posixfs/catalog.rs:209-252` |
| 分页 | **trait 上没有分页概念**；在 HTTP 层对全量结果做内存分页 | `interfaces.rs:148`（`list` 返回 `Vec`，`SnapshotListFilter` 无 limit/cursor）；`snapshots.rs:91`；`template.rs:382`；`pagination.rs:91-120` |
| 别名唯一性 | **无保证**。OSS 自述「weaker than a true CAS」的读-改-写-回读，≤5 次重试 | `oss/repository.rs:590-682`（注释在 `:606-609`） |
| POSIX 互斥 | `create_new` 建锁文件 ＋ 10s 超时 ＋ **60s 陈旧即抢占** —— 不是 `flock`，NFS 上不可靠 | `posixfs/catalog.rs:17-19, 521-628` |
| `Waiting→Building` CAS | POSIX 有记录锁；**OSS 是裸的读-改-写** ⇒ 两个并发 `POST` 都看到 `Waiting`、都开一台构建 VM | `posixfs/catalog.rs:329-353`；`oss/repository.rs:469-493` |
| 构建并发 | **完全无控制**：`tokio::spawn` 裸起，无信号量、无去重、无收割器 | `src/api/impls/template.rs:652` |
| 发布可见性 | **别名先于行写入**（两个后端都是），中间窗口里别名指向一个不存在的行 | `oss/repository.rs:299-310`；`posixfs/catalog.rs:109-111` |

🔴 **最后一行直接违反 trait 自己的契约** ——
`interfaces.rs:112-113` 逐字写着「publish should only make aliases visible after the snapshot
record … are durable」。而读路径的"陈旧别名回收"（`oss/repository.rs:455-464`、
`posixfs/catalog.rs:295-301`）会**删掉一个正在发布中的快照的别名**。
这是阶段 2 顺带修掉的既有缺陷，不是新引入的要求。

### 2.3 🔴 `SnapshotRepository` 今天把目录和字节焊死了

父提案 §7 阶段 2 说「对象存储只留字节」，读起来像是把一半拿走。**代码不支持这个动作**：

| 后端 | 目录与字节是否已分离 | 证据 |
|---|---|---|
| POSIX | ✅ 已经分了 | `backends/posixfs/catalog.rs`（997 行）vs `artifacts.rs`（1114 行） |
| OSS | ❌ **完全交织** | `backends/oss/repository.rs` 1394 行，`publish()` 在 `:191-348` 里从导出磁盘镜像一路做到写目录行 |

⇒ **阶段 2 的第一刀是 trait 拆分**，把 `SnapshotRepository`（`interfaces.rs:80-176`，10 个方法）
劈成两个：

```
trait SnapshotCatalog       // 行：create / publish_commit / get / list_page / delete /
                            //     resolve_alias / try_start_build / mark_build_error
trait SnapshotArtifactStore // 字节：import_built_artifacts / export / delete_prefix / …
```

`publish()` 从"一个方法"变成"先 `artifacts.import(...)` 拿到事实，再 `catalog.publish_commit(...)`"
—— 这正是父提案 §5.1 那张图（node 写字节 → `api` 写目录行）在**进程内**的预演。
🔴 **`SnapshotManager` 那一层的对应拆分（`stage` / `commit_staged`）是同一处手术的上半截，
也在阶段 2 —— 见 §5.5。**
**这一刀是阶段 2 Rust 侧最大的单项，父提案没有为它计过预算**（§12 E7）。

影响面：3 个 trait 实现（`mock.rs:30`、`oss/repository.rs:150`、`posixfs/backend.rs:232`）
＋ 5 个调用点（`manager.rs:212`、`snapshots.rs:76`、`template.rs:331`、`template.rs:370`、测试）。

---

## 3. 🔴 建表时必须定的两件事

父提案 §7 阶段 2 点名这两件「拖到阶段 3 会变成一次 schema 重做」。逐条裁决。

### 3.1 暂停态的落点 —— ✅ 已定：现在就建 `published` ＋ `origin_node_id`

**裁决（2026-08-20，用户）**：父提案 §4.2.1 把「消掉 `local_only`」挂在一个**本仓之外的**
交付物上。**那条依赖出了范围** —— 本轮全部工作只在 `/home/debian/AgentENV` 内，
外部交付物按**不可用、也不等待**处理。

⇒ **按 §4.2.1 边界第 2 点的那套建表，而且现在就建，不留到以后**：

| | 规则 |
|---|---|
| 目录行带两列 | `published boolean` ＋ `origin_node_id text` |
| `published = false` | resume **硬钉 origin**；别的节点一律拒绝 |
| `published = true` | **没有任何节点亲和要求**，任何节点都可以起 |

🔴 **`published` 就是 bool，而且刻意是 bool。** 我在上一稿里主张过三态
（`publishing` / `local_only` / `durable`）。**收回。** 理由是本仓自己的代码，不是外部文档：

那三个值里，「还在传」与「已放弃」的区别**已经有一个归属了** ——
`paused_sandboxes.state`（`services/scheduler/internal/registry/migrate.go` 的
`CHECK (state IN ('publishing','paused','resuming','local_only','running'))`）。
把它复制进目录，等于**同一个状态机在两张表里各写一份**，
而那正是父提案 §0 第一条硬约束批评的形状。

目录只回答一个问题 —— **「任何节点都能起它吗」** —— 而那个问题的答案确实是 yes/no。
`publishing` 与 `local_only` 对这个问题给出的答案**完全一样**，而且今天的代码本来就
一视同仁地对待它们：

| 事实 | 位置 |
|---|---|
| `claim_for_resume` 对两态用同一个谓词分支 | `services/scheduler/internal/registry/store_postgres.go:784-785` |
| 路由对两态都答 `SANDBOX_LOCATION_PINNED` ＋ 钉 `OriginNodeID` | `services/scheduler/internal/lookup.go:270-313` |
| 认领失败时对两态返回同一个 `NotReady{origin}` | `store_postgres.go:900-903`（注释逐字：「both mean parked on its origin node」） |

⇒ **两列，两个职责，不重叠**：目录说"能不能在别处起"，`paused_sandboxes` 说"为什么不能"。

🔴 **`published` 与 `status_group` 是两个轴，别让它们看起来重复。** 这一点决定了这两列
到底承不承重，写代码前必须先想清楚：

| | `status_group = 'ready'` | `published = true` |
|---|---|---|
| 问的问题 | **捕获/构建完成了吗？有没有一个能跑的快照？** | **它的字节到共享仓库了吗？** |
| `local_only` 的答案 | ✅ **是**（快照是完整的、能跑的） | ❌ 否（只在 origin 上） |
| 谁消费它 | 4 条解析查询的 `WHERE`（§5.3） | 一个钉选函数，**不进 `WHERE`**（下方 V5） |

一个发布失败的快照**是一个完整可用的快照**，只不过只有 origin 起得来。
把它 `status` 压成非 `ready`，等于对用户宣称"这个快照不存在"，而它明明能在原机器上恢复 ——
今天 `claim_for_resume` 就是这么放行 origin 的（`store_postgres.go:900-903` 的
`NotReady{origin}` ＋ `paused_recovery.rs:1031-1033` 的自身节点分支 ⇒ `Proceed`）。

⇒ **发布失败的终态是 `status='ready'` ＋ `published=false` ＋ `origin_node_id=<node>`**，
不是"停在 building"。这也是这两列不会退化成 `status_group` 的同义词的原因。

**列定义（完整 DDL 在 §4.2）**：
```sql
published       BOOLEAN NOT NULL DEFAULT true,
origin_node_id  TEXT    NULL,       -- published=false 时必填；published=true 时是纯亲和提示
CONSTRAINT snapshots_origin_axis CHECK (published OR origin_node_id IS NOT NULL)
```

`published = true` 的行**也可以带** `origin_node_id` —— 那时它是父提案 §4.2.1 第 3 条
说的「亲和提示」（对标 e2b `snapshots.origin_node_id` ＋ `UpdateSnapshotOriginNode` 的
提示自愈）。**提示可以为空，落空时不报错、不重试原机器。**

### 3.1.a 🔴 设计成"将来可以整列删掉"

用户的要求：如果哪天 `local_only` 在 AgentENV 内部被消掉，这两列要能**变成废列直接删**，
不引发其余 schema 的重做。这不是自动成立的，它靠下面六条约束换来。**逐条遵守。**

| # | 约束 | 违反了会怎样 |
|---|---|---|
| **V1** | `published` / `origin_node_id` **不得出现在任何 PRIMARY KEY、UNIQUE 约束或外键里** | 删列会连带重建约束，进而重写依赖它的索引 |
| **V2** | 🔴 **`published` 不得与 `status_group` 合并成一列。** 它们是两个轴：`status_group` 答"捕获/构建成功了吗"，`published` 答"别处起得来吗" | 合并之后删 `published` 变成"从一个混合列里重新推导另一个语义"，必然要回填 |
| **V3** | 两列在**索引里只出现一次**，且是一条**专用的部分索引** `snapshots_unpublished_idx`，删列时整条索引一起删 | 散落进多条复合索引 ⇒ 删列要逐条重建 |
| **V4** | 关联两列的 CHECK 必须是**具名**的（`snapshots_origin_axis`），不能写成匿名 CHECK | 匿名约束名由 PG 生成，删除脚本无法幂等 |
| **V5** | 🔴 两列**永远不进任何 `WHERE`**（那条专用部分索引除外）。4 条解析查询只把它们**投影出来**，判定收敛成**一个** Go 函数 `pinOriginIfUnpublished` | 一旦进了 `WHERE`，删列就要逐条改查询；而且过滤掉未发布行等于对用户谎称快照不存在（§3.1） |
| **V6** | `origin_node_id` **永远是 NULLable**，任何时候都不要 `SET NOT NULL` | 一旦 NOT NULL，删列前要先解开约束，且中间态会拒写 |

**将来真要删时，全部动作就是这一个迁移文件加三处代码删除**：

```sql
-- migrations/000N_drop_origin_pinning.sql
-- +aenv NO TRANSACTION
-- 前置：SELECT count(*) FROM snapshots WHERE NOT published;  必须为 0
DROP INDEX CONCURRENTLY IF EXISTS snapshots_unpublished_idx;
ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS snapshots_origin_axis;
ALTER TABLE snapshots DROP COLUMN IF EXISTS origin_node_id;
ALTER TABLE snapshots DROP COLUMN IF EXISTS published;
```

代码侧只有三处删除，**没有一条 `WHERE` 要改**（这正是 V5 买到的）：
从 4 条解析查询的 `SELECT` 列表里去掉两列、
删掉 `pinOriginIfUnpublished` 及其唯一调用点、
删掉 proto 响应里的 `published` / `origin_node_id` 字段（用 `reserved`，不重编号）。

🔴 **`ALTER TABLE … DROP COLUMN` 在 PostgreSQL 里是 O(1) 的纯元数据操作，不重写表** ——
这才是"整列删掉不痛"这句话的依据。上面六条约束存在的全部意义，
就是保证删除时**只需要碰这一个文件**，而不会牵动 `snapshots` 的主键、
keyset 索引（`snapshots_list_idx`）或 `committed_payload` 的任何一部分。

**触发条件**（判定标准，不是感觉）：`SELECT count(*) FROM snapshots WHERE NOT published` 
连续 30 天为 0，且 `paused_sandboxes` 里 `state = 'local_only'` 的行数为 0。

### 3.1.b `local_only` 能不能在本仓内部消掉 —— 一个定量的判断，不是本阶段的活

用户点名这可以评估。**结论：能显著降低频率，但"消掉"是三步，约 2,000–3,000 行，
自成一个阶段，不进阶段 2。** 依据全部在本仓：

**第一步 · 把已经存在的旋钮接上（便宜，~80 行）。**
`ObjectStoreOperatorConfig` **已经有** `timeout` / `max_retries` 两个 `Option` 字段
（`crates/object-store-operator/src/operator.rs:35-36`），
而 OSS backend 硬传 `None`（`src/snapshot/repository/backends/oss/client.rs:83-84`），
于是吃到默认值 `DEFAULT_MAX_RETRIES = 3`（`operator.rs:13`，用在 `:96`）
＋ `TimeoutLayer(30s)` ＋ `RetryLayer`（`:102-106`）。
`UPLOAD_CONCURRENCY = 8` 也还是编译期常量（`oss/client.rs:29`）。
⇒ 把这三个变成配置项，是**接线不是设计**。它降低 `local_only` 的频率，但消不掉它。

**第二步 · 发布异步化（~400–600 行，且改变 pause 的语义）。**
今天发布在 pause 请求里同步 await —— `src/orchestrator/service.rs:1524` 的
`publish_paused_sandbox(...).await`，注释里写着它「runs after the point of no return on purpose」。
要重试就得先让它离开请求路径，而这会把 pause 的返回含义从"已持久到共享仓库"
改成"已本地冻结"。⇒ 是一次**对外语义变更**，需要自己的灰度。

**第三步 · 预算跨进程持久化 ＋ 自环重试（~1,200–1,800 行，跨 Rust／Go／proto）。**
今天失败即终态：`src/api/impls/paused_coordinator.rs:346-364` 在 `publish_captured`
返回 `Err` 时直接 `mark_local_only`，**全仓没有任何补发路径**。
要自环就需要：`paused_sandboxes` 加"首次尝试时刻"一类的列（Go 侧 DDL ＋ proto ＋ Rust 客户端）、
一条重启接手的扫描、一个人工再试的入口。
🔴 而且它有一个连锁反应：让 `publishing` 从"几秒"变成"整个上传时长"，
会撞上今天靠"它只活几秒"压着的两处 —— 路由对 `publishing` 行硬钉 origin 且原机器不在就答
`FailedPrecondition`（`lookup.go:270-313`），
以及 `mark_local_only` 之后仍然返回 `Some(node_id)` ⇒ 上游照样 `mark_cluster_registered`
（`paused_coordinator.rs:346-364` → `src/orchestrator/service.rs:1180-1183`）。

**⇒ 排期建议**：第一步可以随阶段 2 的任意一批捎带（它不改语义、不改 schema）；
第二、三步单独立项。**在它们完成之前，§3.1 的两列是承重的，不是过渡的。**

### 3.1.c 🔴 两分折叠有一个洞：一部分暂停沙箱**没有快照行可以变成**

父提案 §4.2：「`paused_sandboxes` 表不是「搬到 Redis」，是整个消失。暂停态本来就是目录的一部分。」

**对绝大多数行成立，对一类行不成立**：

```
state = 'local_only' AND snapshot_id IS NULL
```

即"begin_pause 成功、发布从未成功过一次"的行。它们的处境（逐行核对）：

| 事实 | 位置 |
|---|---|
| `claim_for_resume` 硬要求 `snapshot_id IS NOT NULL` ⇒ 这类行**谁也认领不了，永远** | `services/scheduler/internal/registry/store_postgres.go:783-785` |
| 两条 reclaim 路径都只碰活行，租约过期什么也不动 | `store_postgres.go` 的 `paused_sandboxes_reclaim_idx` 谓词 |
| scheduler 自己把它们叫 `strandedRows`，并且告警的是"它一直在"而不是"它出现了" | `services/scheduler/internal/reconcile.go:117-125`；`metrics.go:147` |
| 上一轮验证留下的待办第 (4) 条：`local_only` 不在 `paused_sandboxes_reclaim_idx` 覆盖面 ⇒ 过期的 `local_only` 行**永不回收**，且「这不是一行 SQL，是设计问题」 | recon §8 待办表 |

⇒ **这类行没有 `snapshots` 行可以承载它们 —— 它们的快照不存在。**
折叠之后它们要么变成一条只有 `origin_node_id` 没有 `snapshot_id` 的目录行（那就不是快照目录了），
要么留在 `paused_sandboxes`。

**阶段 2 的裁决**：不解决它，但把两条结论登记下来：

1. `paused_sandboxes` 在这类行清零之前**不能删**（写进阶段 3 的前置）；
2. 🔴 **本仓今天没有清零手段** —— 没有补发路径（§3.1.b 第三步），也没有运维再试入口。
   ⇒ 在 §3.1.b 的第三步落地之前，这类行**只能靠人工删沙箱**清掉。
   这同时也是 §3.1.a 那个"删列触发条件"迟迟不满足的原因，排期时要认这笔账。

### 3.1.d 阶段 2 期间 `paused_sandboxes` 仍是暂停态的权威

**不要在阶段 2 切换暂停态的读路径。** 阶段 2 只做两件与暂停态有关的事：

1. `snapshots` 表带上 `source_sandbox_id` / `origin_node_id` / `published` / `sandbox_started_at` 四列，
   并**在发布路径上如实写入**（写而不读）；
2. 让 `complete_pause` 与目录提交进同一个语句（§5.2）。

仲裁（`claim_for_resume`、`lookup.go:270-313` 的 pinned 路由、reconcile）**一律不动**。
理由就是父提案 §7 抬头那条：删除动作不与切换同批。

### 3.2 `builds` 表要能承载构建队列

**e2b 怎么做的**（逐条实测，全部可抄）：

| 机制 | 位置 |
|---|---|
| `status_group` —— 7 个原始状态压成 4 组（`pending`/`in_progress`/`ready`/`failed`），**触发器维护的 text 列**，不是 `GENERATED`、不是 enum | `packages/db/migrations/20260210120002_add_status_group_column.sql:3,6-21` |
| 部分索引，只索引**活着的**构建 ⇒ 索引大小与历史无关 | `migrations/20260305120000`：`idx_env_builds_team_active ON env_builds (team_id) WHERE status_group IN ('pending','in_progress')` |
| 窄侧表 `active_template_builds`，热计数查询不碰胖表；`created_at > NOW() - INTERVAL '1 day'` 当**崩溃恢复 TTL**，泄漏行自己退出配额 | `migrations/20260305130000_create_active_template_builds.sql`；`queries/builds/get_inprogress_builds.sql` |
| 终态转换与出队**原子**（一条 CTE 语句） | `update_template_build_status.sql.go:16-26` 的 `WITH deactivated AS (DELETE …) UPDATE …` |
| 同模板同 tag 的并发构建查询 | `queries/builds/get_concurrent_template_builds.sql`（`status_group IN ('pending','in_progress') AND eb.id != @current_build_id`） |
| 配额主体是 tier 表的一列 | `tiers.concurrent_template_builds bigint NOT NULL DEFAULT 20 CHECK (> 0)`，`migrations/20250901161352` |

**我们的处境不同，两处**：

1. 🔴 **没有租户模型**（父提案 §4.4），所以没有 `tiers` 可挂配额。
   ⇒ 配额主体只能是**集群**（一个全局上限）和**模板**（每模板互斥）。
2. 🔴 **今天 `template_id == build_id` 是被 API 强制的** ——
   `src/api/impls/template.rs:417` 直接校验两者相等。
   所以我们今天没有"一个模板多次构建"这回事。

**这反而让我们能做一件 e2b 做不到的事**：e2b 因为允许多 tag 而只能"先查后判"，
我们可以直接上**部分唯一索引**：

```sql
CREATE UNIQUE INDEX builds_one_active_per_template
    ON builds (template_id)
    WHERE status_group IN ('pending','in_progress');
```

一条索引同时买到三样：每模板构建互斥（比 e2b 的查询强）、
**修掉 `oss/repository.rs:469-493` 那个"两个并发 POST 都开一台 VM"的裸 RMW**、
以及构建队列的准入点。

🔴 **但它有一个陷阱，必须同批解掉**：今天 `tokio::spawn`（`template.rs:652`）是脱管的，
进程死掉时记录**永远卡在 `Building`**，全仓无收割器。
加了这条唯一索引之后，**一条僵死的 `Building` 行会永久阻塞该模板的每一次后续构建** ——
今天的泄漏会升级成故障。

⇒ **`builds` 表必须同批带 `heartbeat_at` ＋ 一个收割器**（§4.3）。
「加索引不加收割器」是本阶段最容易掉进去的坑。

**集群级并发上限**用 PG 事务级 advisory lock，不用"先数后插"：

```sql
BEGIN;
SELECT pg_advisory_xact_lock(3405691582);            -- BUILD_ADMISSION_KEY，常量
SELECT count(*) FROM builds WHERE status_group IN ('pending','in_progress');
-- 超限 ⇒ ROLLBACK，返回 429
INSERT INTO builds (...) VALUES (...);
COMMIT;
```

READ COMMITTED 下"先插后数"是错的（两个事务互相看不见对方未提交的行）。
advisory lock 是这里唯一正确又便宜的做法，我们的量级下它的串行化成本可以忽略。

---

## 4. Schema

🔴 **命名**：表名不加前缀（与 `paused_sandboxes` 一致）。四张表：
`templates` / `builds` / `snapshots` / `aliases`。

🔴 **时间列一律 `bigint` 毫秒，不用 `timestamptz`。** 理由不是口味：
`SnapshotRecord.created_at_unix_ms: i64` 是既有的序列化形状，
而分页游标的 wire 格式是从它渲染出来的（`pagination.rs:127-134`）。
`i64 ms → timestamptz(µs) → RFC3339 nanos` 往返会在游标边界上引入一整类精度 bug。
代价诚实说：失去 PG 的日期函数与 `NOW()` 默认值，运维查询要自己除 1000。**这个交换值得。**

### 4.1 `templates`

模板与快照在我们这里是**同一个实体的两种 source**（`SnapshotRecord.source`），
不像 e2b 分成 `envs` / `snapshots` 两张表。**保持这个形状**，
`templates` 只承载模板独有的、快照没有的东西。

```sql
CREATE TABLE IF NOT EXISTS templates (
    id                 UUID        PRIMARY KEY,          -- == snapshots.id，同一个 SnapshotId
    cluster_id         UUID        NOT NULL,
    created_at_ms      BIGINT      NOT NULL,
    updated_at_ms      BIGINT      NOT NULL,
    deleted_at_ms      BIGINT      NULL,                 -- 软删除，见下
    CONSTRAINT templates_id_fk FOREIGN KEY (id)
        REFERENCES snapshots (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS templates_cluster_live_idx
    ON templates (cluster_id, created_at_ms DESC, id)
    WHERE deleted_at_ms IS NULL;
```

**软删除照抄 e2b**（`migrations/20260628120000_add_env_deleted_at.sql`），
并且照抄它的读视图技巧 —— 视图**不暴露 `deleted_at_ms`**，调用方无从二次过滤：

```sql
CREATE OR REPLACE VIEW active_templates AS
SELECT id, cluster_id, created_at_ms, updated_at_ms
FROM templates WHERE deleted_at_ms IS NULL;
```

🔴 软删除是必需的，不是可选的：今天 `delete()` 会连同别名一起硬删
（`oss/repository.rs:399-440`），而快照删除**可能连带删掉一个已经被
`aenv-snapshot-image` 导出到 registry 的引用**（`CLAUDE.md` 里记着这条警告）。
软删除给了阶段 3 一个可回溯的边界。

### 4.2 `snapshots` —— 目录主表

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    id                   UUID        PRIMARY KEY,        -- SnapshotId（Uuid v7），value.rs:26 的 to_uuid()
    cluster_id           UUID        NOT NULL,

    -- source（SnapshotSource 的判别式 ＋ 只属于 Sandbox 分支的字段）
    source_kind          TEXT        NOT NULL
                                     CHECK (source_kind IN ('template','sandbox')),
    source_sandbox_id    TEXT        NULL,               -- source_kind='sandbox' 时非空
    CONSTRAINT snapshots_source_axis
        CHECK ((source_kind = 'sandbox') = (source_sandbox_id IS NOT NULL)),

    -- resources（SandboxResources，三个 u32）
    cpu_count            INTEGER     NOT NULL CHECK (cpu_count      > 0),
    memory_mib           INTEGER     NOT NULL CHECK (memory_mib     > 0),
    disk_size_mib        INTEGER     NOT NULL CHECK (disk_size_mib  > 0),

    -- 生效状态：committed IS NOT NULL 的 PG 表达
    status               TEXT        NOT NULL
                                     CHECK (status IN ('waiting','building','ready','error')),
    status_group         TEXT        NOT NULL
                                     CHECK (status_group IN ('pending','in_progress','ready','failed')),

    -- 🔴 §3.1：暂停态的落点。published=false ⇒ 只有 origin 起得来
    -- 🔴 V2：这两列与 status_group 是两个轴，永远不要合并（§3.1.a）
    published            BOOLEAN     NOT NULL DEFAULT true,
    origin_node_id       TEXT        NULL,       -- published=true 时是纯亲和提示，可为 NULL（V6）
    CONSTRAINT snapshots_origin_axis
        CHECK (published OR origin_node_id IS NOT NULL),

    sandbox_started_at_ms BIGINT     NULL,               -- 暂停快照的沙箱起始时刻（e2b snapshots.sandbox_started_at）
    created_at_ms        BIGINT      NOT NULL,
    updated_at_ms        BIGINT      NOT NULL,
    deleted_at_ms        BIGINT      NULL,

    -- 🔴 Go 侧永不解析：CommittedSnapshot 的 serde 输出
    committed_payload    BYTEA       NULL,
    committed_schema     INTEGER     NULL,               -- 载荷版本，Rust 侧拒绝未知版本
    CONSTRAINT snapshots_committed_axis
        CHECK ((committed_payload IS NULL) = (committed_schema IS NULL)),
    CONSTRAINT snapshots_ready_is_committed
        CHECK (status <> 'ready' OR committed_payload IS NOT NULL),

    build_error          JSONB       NULL,               -- TemplateBuildErrorReason，Go 侧不解释
    CONSTRAINT snapshots_error_axis
        CHECK (status <> 'error' OR build_error IS NOT NULL)
);
```

`status_group` 用**触发器**维护（照抄 e2b `compute_status_group()`，
`migrations/20260210120002:6-21`），不用 `GENERATED ALWAYS AS` ——
后者在 PG 里不能被部分索引的谓词用到早期版本上，且改映射要重写整表：

```sql
CREATE OR REPLACE FUNCTION snapshots_status_group() RETURNS TRIGGER AS $$
BEGIN
  NEW.status_group := CASE NEW.status
    WHEN 'waiting'  THEN 'pending'
    WHEN 'building' THEN 'in_progress'
    WHEN 'ready'    THEN 'ready'
    ELSE 'failed' END;
  NEW.updated_at_ms := (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT;
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER snapshots_status_group_trg
  BEFORE INSERT OR UPDATE OF status ON snapshots
  FOR EACH ROW EXECUTE FUNCTION snapshots_status_group();
```

索引：

```sql
-- 🔴 keyset 分页的支撑索引，方向必须与 ORDER BY 逐字一致（§6）
CREATE INDEX IF NOT EXISTS snapshots_list_idx
    ON snapshots (cluster_id, source_kind, created_at_ms DESC, id)
    WHERE deleted_at_ms IS NULL AND status_group = 'ready';

-- GET /snapshots?sandboxID=… 的过滤
CREATE INDEX IF NOT EXISTS snapshots_source_sandbox_idx
    ON snapshots (cluster_id, source_sandbox_id)
    WHERE source_sandbox_id IS NOT NULL AND deleted_at_ms IS NULL;

-- 暂停态运维视图 ＋ §3.1.a 的 stranded 判定
-- 🔴 V3：这是这两列**唯一**出现的索引。将来删列时整条一起删，别的索引一处不动
CREATE INDEX IF NOT EXISTS snapshots_unpublished_idx
    ON snapshots (cluster_id, origin_node_id)
    WHERE NOT published;
```

> 🔴 **上面这条 DDL 写错了（2026-08-20 修正，实现里已经不是这样）。**
> 谓词漏了 `deleted_at_ms IS NULL` —— 它上面两条索引都带。
> 后果不在读路径，而在 §3.1.a 那条退休判据：「`count(*) WHERE NOT published`
> 连续 30 天为 0 ⇒ 可以删掉 origin 两列」。软删除的行永远留在这条索引里，
> 那个计数就永远到不了 0，两列于是永远删不掉 —— 一条只会在几个月之后、
> 以「为什么这个数不降」的形式暴露的错。
> `0001_snapshots.sql` 里的实际 DDL 是：
>
> ```sql
> CREATE INDEX IF NOT EXISTS snapshots_unpublished_idx
>     ON snapshots (cluster_id, origin_node_id)
>     WHERE NOT published AND deleted_at_ms IS NULL;
> ```
>
> 相应地，§3.1.a 的运维查询也要带上 `AND deleted_at_ms IS NULL`。

🔴 **`snapshots_list_idx` 里的 `status_group = 'ready'` 是部分索引谓词，不是列上的普通索引。**
这一条把 §5.3 的"未翻牌的行选不中"从"每次查询都要写对谓词"降级成"写错谓词会立刻变慢"，
是一道有反馈的护栏。

### 4.3 `builds`

🔴 **今天 `template_id == build_id`（`api/impls/template.rs:417` 强制）。**
表**不要**沿用这个恒等式 —— 它是要被拆开的，而拆表比拆索引贵得多。
阶段 2 里 `builds.id` 可以恰好等于 `template_id`，但列是分开的。

```sql
CREATE TABLE IF NOT EXISTS builds (
    id                UUID        PRIMARY KEY,
    template_id       UUID        NOT NULL,
    cluster_id        UUID        NOT NULL,

    status            TEXT        NOT NULL
                                  CHECK (status IN ('waiting','building','ready','error')),
    status_group      TEXT        NOT NULL
                                  CHECK (status_group IN ('pending','in_progress','ready','failed')),

    node_id           TEXT        NULL,                  -- 哪台机器在跑（e2b env_builds.cluster_node_id）
    -- 🔴 收割器的依据。没有它，下面那条部分唯一索引会把泄漏升级成故障
    heartbeat_at_ms   BIGINT      NULL,

    created_at_ms     BIGINT      NOT NULL,
    started_at_ms     BIGINT      NULL,
    finished_at_ms    BIGINT      NULL,
    error_reason      JSONB       NULL,                  -- Go 侧不解释

    CONSTRAINT builds_template_fk FOREIGN KEY (template_id)
        REFERENCES snapshots (id) ON DELETE CASCADE,
    CONSTRAINT builds_started_axis
        CHECK ((status = 'waiting') = (started_at_ms IS NULL)),
    CONSTRAINT builds_finished_axis
        CHECK ((status_group IN ('ready','failed')) = (finished_at_ms IS NOT NULL))
);

-- 🔴 每模板构建互斥。比 e2b 的 get_concurrent_template_builds 强，因为我们没有 tag 维度
CREATE UNIQUE INDEX IF NOT EXISTS builds_one_active_per_template
    ON builds (template_id)
    WHERE status_group IN ('pending','in_progress');

-- 集群级并发计数（配合 §3.2 的 advisory lock）与收割器扫描，只索引活着的构建
CREATE INDEX IF NOT EXISTS builds_active_idx
    ON builds (cluster_id, heartbeat_at_ms)
    WHERE status_group IN ('pending','in_progress');
```

同批的**收割器**（跑在 scheduler 的现有 reconcile 循环里，与 `reconcile.go` 同一个 ticker）：

```sql
UPDATE builds
   SET status = 'error',
       finished_at_ms = $now,
       error_reason = '{"message":"build heartbeat lapsed","step":null}'::jsonb
 WHERE cluster_id = $1
   AND status_group IN ('pending','in_progress')
   AND heartbeat_at_ms IS NOT NULL
   AND heartbeat_at_ms < $now - $build_heartbeat_ttl_ms
RETURNING id, template_id;
```

Rust 侧对应：`api/impls/template.rs:652` 那个 `tokio::spawn` 里加一个心跳 tick
（复用 `paused_coordinator.rs` 已有的 renew 循环形状），周期取 TTL 的 1/3。
`build_heartbeat_ttl_ms` 默认 **300_000**（5 分钟）—— 构建可以跑很久，但心跳不该断 5 分钟。

### 4.4 `aliases`

```sql
CREATE TABLE IF NOT EXISTS aliases (
    cluster_id   UUID   NOT NULL,
    alias        TEXT   NOT NULL,
    snapshot_id  UUID   NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (cluster_id, alias),
    CONSTRAINT aliases_snapshot_fk FOREIGN KEY (snapshot_id)
        REFERENCES snapshots (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS aliases_snapshot_idx ON aliases (snapshot_id);
```

**主键即唯一约束**，一次 `INSERT … ON CONFLICT (cluster_id, alias) DO NOTHING RETURNING`
就把 `oss/repository.rs:590-682` 那 93 行"weaker than a true CAS"整个替掉。

🔴 **`ON DELETE CASCADE` 是刻意的，与今天的行为不同。** 今天删快照时只有当别名
"还指着被删的 id"才顺带删（`oss:410-418`）—— 那是因为没有外键、只能事后核对。
有了外键就不需要那种防御，也不再有"别名指向不存在的行"这个状态，
读路径上两处陈旧别名回收（`oss/repository.rs:455-464`、`posixfs/catalog.rs:295-301`）
在 `catalog_read = postgres` 之后**必须删掉** —— 留着它们会在 PG 上误删合法别名。

🔴 **不抄 e2b 的 `namespace` ＋ `NULLS NOT DISTINCT`**（`migrations/20260127120000`）——
那是它的多租户命名空间需求，我们没有租户模型（父提案 §4.4）。
`cluster_id` 已经提供了我们需要的全部作用域。

---

## 5. 发布翻牌：字节先落，目录后翻

### 5.1 🔴 e2b 没有跨 RPC 的事务，别去造一个

父提案 §5.1 那张图（node 写字节 → `api` 在 PG 里写目录行 —— 这一步才是生效）
读起来像"一个事务"。**去 e2b 里核对，它不是**：

```
pause_instance.go:36   UpsertSnapshot(...)          ← 一条 4-CTE data-modifying statement，autocommit
                                                      建 envs 行 ＋ upsert snapshots 行 ＋ 建 env_builds(status='snapshotting')
                                                      ＋ 建 env_build_assignments 边，返回 build_id/template_id
pause_instance.go:58   node 的 Sandbox.Pause gRPC   ← 完全不在事务里
pause_instance.go:72   UpdateEnvBuildStatus(Success) ← 一条裸 UPDATE，autocommit
pause_instance.go:84   snapshotCache.Invalidate      ← 一次 Redis 删除
```

`grep -n "WithTx\|BEGIN" packages/api/internal/orchestrator/pause_instance.go` 无命中。
**三条独立的自动提交语句。**

原子性从哪来？两处，都不是事务：

1. `UpsertSnapshot` 本身是**一条**语句 —— 4 个 CTE 一起成功或一起失败
   （`packages/db/queries/snapshots/create_new_snapshot.sql:1-106`）；
2. 崩在 `:58` 与 `:72` 之间留下的孤儿行是 `status='snapshotting'` ⇒ `status_group='in_progress'`，
   **而所有解析查询都带 `status_group = 'ready'`**（§5.3）—— 孤儿被谓词挡住，不需要清理。

⇒ **照抄这个形状。谁想在 gRPC 调用两端持一个 PG 事务，就是在造一个比原设计更差的东西**
（长事务、连接被占、node 一慢就把 pool 拖垮）。

### 5.2 我们的落法：两条语句 ＋ 一个谓词

**发布路径（pause）**：

```
① scheduler 事务 A（一条多 CTE 语句）
     INSERT snapshots (id, …, status='building', status_group='in_progress',
                       published=false, origin_node_id=$node)
     ＋ UPDATE paused_sandboxes SET state='publishing', generation=generation+1, …
     ⇒ 🔴 §1.4 的兑现：begin_pause 与目录建行成为同一条语句
② node 写字节（对象存储 / POSIX，路径带 snapshot_id 前缀）
③ scheduler 事务 B（一条多 CTE 语句）
     UPDATE snapshots SET status='ready', published=true,
                          committed_payload=$1, committed_schema=$2
       WHERE id=$id AND status='building'
     ＋ INSERT aliases … ON CONFLICT DO NOTHING          （有别名时）
     ＋ UPDATE paused_sandboxes SET state='paused', snapshot_id=$id
       WHERE sandbox_id=$sbx AND generation=$gen AND state='publishing'
     ⇒ 三件事一起成或一起败；complete_pause 与翻牌不再可能只成功一半
```

**发布失败**：事务 C 一条语句做两件事 ——
`paused_sandboxes.state` → `local_only`，**并且**目录行
`status='ready'` ＋ `published=false` ＋ `origin_node_id=$node`。
🔴 **注意 `status` 是 `ready` 不是 `building`**：快照是完整可跑的，只是只有 origin 起得来（§3.1）。
「还在传」与「已放弃」的区别归 `paused_sandboxes`，不进目录。**行不删** —— 与
`store_postgres.go:697-737` 的 `MarkLocalOnly` 语义一致（那里有一条测试
`TestMarkLocalOnlyKeepsTheRow` 专门锁住"不删"）。

🔴 **上面的 ② 与 ③ 就是 §5.5 的 `stage` 与 `commit_staged`** —— 两者是同一件事的两种写法，
读的时候当成一件事。

**模板构建路径**：`v3_templates_post` 建 `waiting` 行；
`try_start_build` 变成一条带 advisory lock 的事务（§3.2）；
`builder.rs:128-147` 的 `publish` 走上面的事务 B（无 `paused_sandboxes` 那一支）。

🔴 **事务 B 的 `WHERE … AND status='building'` 是承重的**，不是防御性写法。
它是 execution fencing 在目录侧的落点（父提案 §6.3「提交目录行时的 execution 谓词」）。
阶段 2 里它按 `status` 判；阶段 3 `api` 变成 N 副本之后，
这里要**再加一个 `execution_id` 谓词** —— 建表时就把列留出来：

```sql
ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS publishing_execution_id UUID NULL;
```

阶段 2 只写不判，阶段 3 加谓词。**现在不留，阶段 3 是一次带回填的 schema 变更。**

### 5.3 🔴 `status_group='ready'` 的谓词放在哪 —— 解析查询，不是 resume 路径

父提案 §10 F5 已经改正过一次（v1 把它挂在 `sandbox_resume.go` 上）。**核对属实，并且能补全**：
e2b 里带这个谓词的查询有**四条**，父提案只列了三条。

| 查询 | 位置 | 我们的对应 |
|---|---|---|
| `GetSnapshotsWithCursor` | `packages/db/queries/get_snapshots_with_cursor.sql:26` | `list_snapshots_page`（`GET /snapshots`） |
| `GetTeamTemplate` | `packages/db/queries/get_team_template.sql:32` | `get_template`（`GET /templates/{id}`） |
| `GetTeamTemplatesWithCursor` | `packages/db/queries/get_team_templates_with_cursor.sql:46` | `list_templates_page`（`v2 GET /templates`） |
| 🔴 **`GetLastSnapshot`** —— **resume 路径实际用的那条**，父提案漏了 | `packages/db/queries/snapshots/get_last_snapshot.sql:8`（`eb_inner.status_group = 'ready'`） | `resolve_runnable` / `load_runnable`（`manager.rs:243-266`） |

⇒ **谓词确实不在 resume 的控制流里，但它在 resume 读的那条查询里。**
父提案的措辞（「不在 `sandbox_resume.go` 里」）字面正确，
但如果读成"恢复路径不带这个谓词"就会漏掉最重要的一条。**这是我们最不能漏的一条** ——
少了它，一个字节还没传完的快照会被拿去起 VM。

**必须带谓词的查询（4 条）**：
`list_snapshots_page` / `list_templates_page` / `get_template` / `resolve_runnable`。
另外 `resolve_alias` 走 `aliases` 表 join `snapshots`，同样带。

🔴 **必须不带谓词的查询（2 条）**，写错了会让构建状态永远查不到：
- `get_build_status`（`templates_template_id_builds_build_id_status_get`，`api/impls/template.rs:409-449`）
  —— 它存在的意义就是看一个 `building` / `error` 的构建；
- 收割器与运维视图。

**落地建议**：把两组分成两个 Go 文件（`queries_resolved.go` / `queries_admin.go`），
并在 §4.2 的部分索引里把谓词写死 —— 写错谓词的查询会立刻掉出索引，慢给你看。

### 5.4 🔴 `published` 不是谓词，是投影 —— 它在 `resolve_runnable` 上怎么用

§5.3 那 4 条查询过滤的是 `status_group`。**`published` / `origin_node_id` 一律不进 `WHERE`**
（V5），它们被 `SELECT` 出来交给一个函数判定：

```go
// 唯一的判定点。将来 §3.1.a 删列时，删掉这个函数和它的调用点即可。
func pinOriginIfUnpublished(row snapshotRow, target string) (allowed bool, pin string) {
    if row.Published {
        return true, row.OriginNodeID      // 亲和提示，可为空；落空不报错、不重试原机器
    }
    return row.OriginNodeID == target, row.OriginNodeID   // 硬钉
}
```

三条调用契约：

1. **`resolve_runnable` / `load_runnable`**（`manager.rs:243-266`）：
   `allowed == false` ⇒ 拒绝，错误里带上 `pin`，语义与今天
   `api/impls/sandbox.rs:760-766` `:1336-1342` 的
   「sandbox snapshot is still being published by node '{origin}'」对齐 —— **不要新造一个错误形状**。
2. **`list_snapshots_page` / `list_templates_page`**：**不过滤**。
   未发布的快照照常列出，只是响应里带上它被钉在哪台机器
   （新增一个可选响应字段；`published=true` 时不下发）。
   过滤掉它等于对用户宣称一个他明明能在原机器上恢复的快照不存在。
3. **`get_template`**：同 2。

🔴 **`published = true` 的行上那个 `origin_node_id` 是提示，不是约束。**
落空时**静默降级走通用放置**，并且照 e2b `UpdateSnapshotOriginNode` 的做法
**把提示改写成新节点**（父提案 §4.2.1 第 3 条、模块文档 D12 第 3 条）。
阶段 2 只要保证"写得进、读得出、落空不报错"，改写回路本身属于阶段 4 的 placement。

---

### 5.5 🔴 bytes-then-commit：`stage` / `commit_staged` —— 归阶段 2，同意阶段 3 的判断

**背景**：阶段 3 的结构设计（其 §4.4）指出 §5.1 的 bytes-then-commit 是阶段 2 的硬前置，
而阶段 2 的任务描述没把它派给任何人。**核对属实，我接下来。**

`--role api` 的远程 pause 无处落地，根因在 `src/sandbox/backend.rs:137-142` 自己写着的那句：
`CapturedSandboxSnapshot`「may use it to keep temporary artifact directories alive until
publication finishes」—— 它是一个**持有本地临时目录的进程内句柄**，过不了线（父提案 §3.4）。

#### 结论：属于阶段 2 的 2b 批，不属于阶段 3

三条理由，第三条是决定性的：

1. **不做它，阶段 2 只兑现了自己理由的一半。** §5.1 说「`api` 必须先有一个可以原子提交的地方」。
   PG 的表是那个**地方**；`stage`/`commit_staged` 才是让那个地方**能被另一个进程使用**的东西。
2. **它和我已经在做的手术是同一处。** §2.3 的 trait 拆分要把 OSS 的 `publish()`
   （`oss/repository.rs:191-348`）拆成"先字节、后目录"。`SnapshotManager` 这一层的拆分
   骑在它上面，**增量约 260–350 行**；等阶段 3 再做，就是在双写机器已经跑起来之后
   重新动 publish，贵得多。（POSIX 侧本来就已经是这个形状 ——
   `backend.rs:149-197` 的 `begin_publish` → `import_built_artifacts` → `commit_publish`。）
3. 🔴 **在 (b) 方案下，阶段 2 里这条缝就已经有真实的跨进程消费者了。**
   目录提交在 scheduler 的 Go 进程里（§1.3），所以 2b 一上线，
   "字节落在一处、提交发生在另一处"**就是真的**，不是为阶段 3 预留的空接口。
   这消解了"给一个还没有 wire 的类型设计 wire 格式"这个通常的反对意见 ——
   **wire 在阶段 2 就存在。** 这一条同时说明 §1 的 (b) 与本节是互相加强的，不是各自独立的选择。

#### 缝在哪：`oss/repository.rs:296` 与 `:299` 之间

今天 OSS 的 `publish()` 顺序是：导出磁盘镜像 `:230-241` → 内存层 `:243-245` →
`vm_state.bin` `:249-266` → manifest `:270-277` → 附加盘 `:280-282` →
**构建 `CommittedSnapshot` `:285-296`** → `bind_alias` `:299` → `write_committed_record` `:310`。

⇒ **缝就在 `:296` 之后、`:299` 之前** —— `CommittedSnapshot` 已经构造完成
（所有产物引用都已解析），而目录还一个字节都没写。

#### `StagedSnapshot` 携带什么

```rust
/// 一次暂存的产物：字节已经落库，目录还没提交。
/// 🔴 纯值类型 —— 不含 PathBuf、不含 Arc、不含临时目录守卫。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StagedSnapshot {
    pub id: SnapshotId,
    pub alias: Option<SnapshotAlias>,
    pub source: SnapshotPublishSource,
    pub resources: SandboxResources,
    /// 已完整构造的提交载荷 —— 就是 §4.2 的 committed_payload
    pub committed: CommittedSnapshot,
    pub staged_at_unix_ms: i64,
    /// §3.1 的两列之一；published 由 commit 决定，origin 由 stage 决定
    pub origin_node_id: String,
    /// 阶段 3 的提交谓词（§5.2 末尾预留的列）。阶段 2 只写不判
    pub execution_id: Option<ExecutionId>,
}
```

🔴 **两处 serde 缺口，动手时先补**：
- `SnapshotPublishMetadata`（`types/snapshot.rs:21-35`）是 `#[derive(Clone, Debug)]`，
  **没有 Serialize/Deserialize**；`SnapshotPublishSource`（`:60-64`）同样要确认。
  它们的成员（`CommandContext` / `StartupCommand` / `SnapshotRuntimeVersions` /
  `ImageConfigs` / `CustomExtensionParams`）都已经是可序列化的（它们在 `CommittedSnapshot` 里），
  所以这是加 derive，不是改设计。
- `FirecrackerSnapshotManifest` 的路径字段全部 `#[serde(skip)]`
  （`src/sandbox/firecracker/manifest.rs:17-63`）。**这正是我们想要的** ——
  `commit_staged` 不该、也不能回头去读本地路径。⇒ **`StagedSnapshot` 不携带 manifest**，
  只携带从它派生出来的 `CommittedSnapshot`。

#### 暂存字节落在哪 —— ✅ 已经是 fencing，用的是 `snapshot_id` 不是 `execution_id`

父提案 §6.2① 说 `execution_id` 「从「要检查的谓词」变成「要写进去的路径名」」，
并警告「想连 execution 身份一起拿掉，等于让两次发布写同一个路径」。

🔴 **在我们的代码里，那个属性已经成立了，而且不是靠 `execution_id`。**

| 事实 | 位置 |
|---|---|
| 每一次 pause 都**新铸一个 `SnapshotId`** | `src/api/impls/paused_coordinator.rs:695-698` 的 `publish_metadata()`：`id: SnapshotId::generate()` |
| 每一次模板构建同样新铸 | `src/template/builder.rs:58`；`src/api/impls/template.rs:537` |
| 快照产物按 id 分前缀 | OSS `artifacts/{id}/…`（`oss/layout.rs`）；POSIX `snapshots/{id}/…`（`posixfs/layout.rs`） |

⇒ `SnapshotId` 已经扮演了 e2b `BuildID` 的角色（父提案引的 `snapshot_template.go`）。
**两次发布不可能写同一个 `artifacts/{id}/` 前缀**，因为 id 是每次现铸的。

⇒ **暂存路径就用 `artifacts/{snapshot_id}/`，不要再往路径里塞 `execution_id`**：
它是冗余的，而且会让阶段 3 的排期多出一项本来不存在的工作。

> **一个不能顺推的例外**：`managed-layers/{digest}` 是**内容寻址**的，
> 不在 per-snapshot 前缀下，两个写者会写同一个 key。**这是安全的**，
> 因为同 digest 即同字节；OSS 代码也已经据此把它排除在回滚之外
> （`oss/repository.rs:301-302, 324-325`：「shared across snapshots and require separate GC」）。
> ⇒ 别为了"路径隔离"把内容寻址的层也塞进 per-snapshot 前缀，那会让去重失效、存储翻倍。

**两个 fencing 落点因此是分开的，别混**：

| | 靠什么 | 状态 |
|---|---|---|
| **命名隔离**（两次发布不撞路径） | `snapshot_id`，每次现铸 | ✅ **今天已经成立** |
| **提交排他**（被取代的化身不能提交） | `snapshots.publishing_execution_id` 谓词 | 阶段 2 预留列、只写不判；阶段 3 加谓词（§5.2 末尾） |

#### 提交事务坐在哪

`commit_staged` **就是 §5.2 的事务 B**，一一对应：

```
stage(metadata, captured) -> StagedSnapshot        # 在 node 上：写字节，消费掉 capture 句柄
                                                    # 🔴 消费所有权 ⇒ 临时目录在这里释放，§3.4 的句柄问题就此消失
commit_staged(StagedSnapshot) -> SnapshotRecord     # 走 gRPC 到 scheduler，跑 §5.2 事务 B：
                                                    #   UPDATE snapshots SET status='ready', published=true,
                                                    #                        committed_payload=serde(staged.committed)
                                                    #   ＋ INSERT aliases … ON CONFLICT DO NOTHING
                                                    #   ＋ UPDATE paused_sandboxes SET state='paused', snapshot_id=…
```

| 阶段 | `stage` 跑在 | `commit_staged` 的事务跑在 |
|---|---|---|
| **2b/2c** | node（进程内） | **scheduler 的 Go pool**（已经跨进程） |
| **3** | node，由 `api` 通过 RPC 触发，返回序列化的 `StagedSnapshot` | `api` 调用，事务仍在拥有 PG 的那一侧 |
| **4** | 同上 | `api` 进程内直连 |

**三个阶段里 `StagedSnapshot` 的形状一行不变** —— 变的只有谁调用它。

#### 与发布翻牌、与 §5.3 谓词的组合

1. `stage` **不写任何目录状态**。行由 §5.2 事务 A 在 `begin_pause` 时就建好了，
   状态是 `status='building'` ⇒ `status_group='in_progress'`。
   ⇒ **暂存中的快照被 §5.3 的 4 条解析查询天然挡住**，不需要任何额外谓词。
   这就是 e2b 用 `status_group='ready'` 掩住孤儿的同一招（§5.1）。
2. `commit_staged` 是**唯一**把 `status` 推到 `ready` 的地方。翻牌只有这一个写者。
3. `stage` 成功、`commit_staged` 失败 ⇒ 字节在、行停在 `in_progress`、任何解析查询都看不到它。
   处置沿用今天的 `discard_unreferenced_snapshot`（`paused_coordinator.rs:344-346`）。
4. 🔴 **`published` 由 `commit_staged` 决定，`origin_node_id` 由 `stage` 决定。**
   发布失败走 §5.2 的事务 C：`status='ready'` ＋ `published=false` ＋ origin 硬钉（§3.1）。

#### 🔴 一个必须移交给阶段 3 的连带项：P2P 广告

今天 P2P 广告在提交**之后**跑（`manager.rs:118` 的 `publish_p2p_artifacts`，
CLAUDE.md 也把「after commit」写成约定），而它**需要本地字节**。

⇒ 阶段 3 里 `api` 执行提交，但字节在 node 上，**`api` 广告不了它没有的东西**。

**阶段 2 的处置**：`publish_p2p_artifacts` 留在 `commit_staged` 里，行为不变（同进程，没问题）。
**移交阶段 3**：需要一个 node 侧的"提交完成"回调，否则要么 P2P 广告消失，
要么就得把广告提到 `stage` 里 —— 而后者会广告一个尚未提交的快照，违反现有约定。
**这一条请阶段 3 的规格显式接手，我在这里只登记，不设计。**

---

## 6. Keyset 分页

### 6.1 🔴 游标的 wire 格式不许变

`x-next-token` 是 OpenAPI 里的公开响应头。改了它，
(a) 在途客户端的翻页会断，(b) **双写回退（§9）不再是零损失** —— 回退后旧格式的游标解不开。

今天的格式（`src/api/impls/pagination.rs:127-134`）：

```
base64url( "{created_at RFC3339 with nanos}__{value}" )     # 分隔符 "__"，:57-59
排序        created_at DESC, id ASC                          # compare_desc, :141-148
开区间起点  SnapshotId::max()（Uuid::max 哨兵）                # snapshots.rs:72, value.rs:38-40
```

⇒ **`PaginationCursor` 一行不改。** 变的只是"谁来消费它"：
从 `paginate_sorted(全量 Vec)` 变成"拆成 `(time, id)` 传给 SQL"。

### 6.2 SQL

照抄 e2b 的**操作数互换**技巧（`get_snapshots_with_cursor.sql:34`）——
它用一次行比较表达了混合方向的排序，比拆成 `OR` 更能用上索引：

```sql
-- list_snapshots_page
SELECT s.id, s.cpu_count, s.memory_mib, s.disk_size_mib,
       s.created_at_ms, s.updated_at_ms, s.committed_payload, s.committed_schema,
       s.published, s.origin_node_id,          -- 🔴 投影，不进 WHERE（§5.4 / V5）
       a.alias
  FROM snapshots s
  LEFT JOIN aliases a ON a.snapshot_id = s.id AND a.cluster_id = s.cluster_id
 WHERE s.cluster_id   = @cluster_id
   AND s.source_kind  = 'sandbox'
   AND s.deleted_at_ms IS NULL
   AND s.status_group = 'ready'                              -- 🔴 §5.3；published 不在此处
   AND (@source_sandbox_id::text IS NULL OR s.source_sandbox_id = @source_sandbox_id)
   AND (@alias::text          IS NULL OR a.alias = @alias)
   -- 🔴 操作数互换：等价于 created_at_ms < cur_ms OR (= AND cur_id < id)
   AND (s.created_at_ms, @cursor_id::text) < (@cursor_ms, s.id::text)
 ORDER BY s.created_at_ms DESC, s.id ASC
 LIMIT @limit_plus_one;
```

三处必须逐字对齐，错一个就退化成全表扫：

1. `ORDER BY` 的方向必须与 `snapshots_list_idx` 的 `(created_at_ms DESC, id)` 一致；
2. `id` 两侧都转 `text` 比较 —— 因为游标里的 `id` 是字符串，
   而 `Uuid` 的**二进制序**与它的**文本序**不同。今天 `compare_desc`
   （`pagination.rs:141-148`）比的是 `SnapshotId` 的 `to_string()`，SQL 必须比一样的东西。
   🔴 **这一条最容易错，错了不报错，只是翻页时静默跳记录。**
3. `LIMIT n+1` 取代今天的 `items.len() > limit`（`pagination.rs:110`）——
   多取一条就是"还有下一页"的判据，第 n 条渲染成 `next_token`。

`SnapshotId::max()` 哨兵在 SQL 里是 `'ffffffff-ffff-ffff-ffff-ffffffffffff'`，
文本序上大于任何真 UUID ⇒ 第一页的 `cursor_id` 用它，行比较自然全通过。**不需要特判。**

### 6.3 trait 改动

`SnapshotListFilter`（`interfaces.rs:17-35`）今天**没有 limit / cursor / 排序**，
`list()` 返回 `Vec<SnapshotRecord>` 全量。要加：

```rust
pub struct SnapshotListPage {
    pub items: Vec<SnapshotRecord>,
    pub next: Option<PaginationCursor<SnapshotId>>,
}

// SnapshotListFilter 新增
pub limit:  Option<u32>,
pub cursor: Option<PaginationCursor<SnapshotId>>,
```

`list()` → `list_page(filter) -> RepositoryResult<SnapshotListPage>`。
3 个实现 ＋ 5 个调用点（§2.3 已列）。
对象存储实现保留今天的全扫 ＋ 内存分页语义（双写期间读侧还可能切回去），
只是把 `paginate_sorted` 从 HTTP 层**下沉**进后端。

🔴 **同批修掉两个 API 层缺陷**：
- `templates_get`（v1，`api/impls/template.rs:322-346`）**完全没有分页**，返回全量模板。
  它是 10k 条记录下最先炸的端点，而验证判据只盯着 `GET /snapshots`。
- `limit` 是 `Option<u32>`，为 `None` 时今天返回全部且 `next_token` 恒 `None`
  （`pagination.rs:110-116`）。**PG 版本必须有硬上限**（建议 `limit.unwrap_or(100).min(1000)`），
  否则一条 `GET /snapshots` 就能把 10k 行拉进内存。

---

## 7. 目录缓存进 Redis

### 7.1 🔴 前置：集群里没有 Redis

recon §4.5 实测：两台机器都没有 `redis-server`，`kubectl get pods/svc -A | grep -i redis` 零命中，
scheduler 启动日志坐实 `binding_store="memory"`。SD-B1 把它列为阻塞项。

⇒ **父提案 §7 阶段 2 写的「前置：无」是错的**（§12 E2）。

> 🔧 **在途（2026-08-20）**：`deploy/k8s/base/redis.yaml` 已经写好但**尚未 apply**
> （`kubectl get pods,svc -n agentenv-system | grep -i redis` 仍零命中）。
> 它是阶段 1 的产出，落地即解掉 SD-B1。**动手前先复核它是否真在集群上**，
> 别照着这一节的"没有"去做规划。

**但这不是阶段 2 独有的成本**：阶段 1（gateway 直读 Redis）与阶段 3（活跃态折叠）
都以它为前提。SD-B1 是三个阶段共享的一次性部署工作（Deployment ＋ Service ＋ 可选 PVC
＋ 把 `redis_addr` 注进 scheduler），**顺带解掉滚 scheduler 的 14s 数据面 503 窗口**。
⇒ 把它写成**阶段 2 的前置**，而不是阶段 2 的内容。

### 7.2 🔴 缓存的凭据问题和 PG 一模一样，父提案没注意到

如果目录缓存跑在 Rust 进程里，阶段 2 时那就是 node ⇒ node 拿 Redis 凭据 ⇒
**同样违反硬约束 2**，而且模块文档 D6 特意写过「我们比 e2b 更严，e2b 的节点有一处 Redis
（`pkg/sandbox/uploads.go`），我们不抄这一处」。

⇒ **缓存与目录同侧**：Redis 客户端在 scheduler 的 Go 进程里，
包在目录查询外层。这恰好是 e2b 的形状 —— `cache.RedisCache[V]`
（`packages/shared/pkg/cache/redis.go:99-140`）就是"Redis L1 ＋ DataCallback L2 ＋
singleflight ＋ redislock"，callback 就是 DB 查询。
`services/go.mod` 已经有 `github.com/redis/go-redis/v9 v9.7.3`，**不新增依赖**。

🔴 **但和阶段 1 的 Redis 共用一个实例有一个必须先解的冲突。**
`deploy/k8s/base/redis.yaml` 把这个实例明确配成**耐久存储而非缓存**，两条设置逐字写在注释里：
`appendonly yes` 与 **`maxmemory-policy noeviction`** —— 后者的理由是
「the only policy that fails *loudly*」，因为阶段 3 之后它装的是"哪些沙箱存在"。

把一个**有 TTL、会无界增长、丢了也没关系**的目录缓存放进一个 `noeviction` 的实例，
后果是缓存把内存吃满之后，**耐久数据的写入开始报错** —— 缓存的容量问题变成控制面的可用性问题。
三条出路，按性价比：

| # | 做法 | 代价 |
|---|---|---|
| 1 | **目录缓存用独立的 Redis 实例/db**（`allkeys-lru`，不开 AOF） | 多一个部署对象；但语义正确，推荐 |
| 2 | 同实例，给缓存 key 前缀配 `maxmemory` 之外的硬上限（应用侧自己限条数） | 应用侧要维护逐出，等于自己写一遍 LRU |
| 3 | 干脆不做目录缓存（§7.3 已经建议 C-3 先不做；C-1/C-2 若索引够快也可先不做） | 少一个组件、少一处失效点 |

🔴 **建议先走 3，量出 §10 P10 的 P99 之后再决定要不要 1。**
「目录在哪缓存就在哪」（§8 陷阱 3）说的是**归属**，不是"必须同时上线"——
归属正确的前提下，缓存本身是一个可以推迟的优化，而 `noeviction` 那条约束不是。

### 7.3 三个缓存面

🔴 **父提案说 e2b 那三个子包"全部是 Redis ＋ DB 回落"—— 只有两个是**（§12 E4）。
逐条核对之后，我们该抄的和不该抄的：

| e2b | 实际形状 | 我们抄不抄 |
|---|---|---|
| `cache/templates`（`TemplateCache` / `AliasCache` / `TemplateMetadataCache` / `TemplatesBuildCache`） | ✅ Redis ＋ `*sqlcdb.Client` 回落。TTL 5m / refresh 1m | ✅ 抄 |
| `cache/snapshots`（`SnapshotCache`） | ✅ Redis ＋ `GetLastSnapshot` 回落。TTL 5m / refresh 1m | ✅ 抄 |
| `cache/sandboxcounts`（`CountsCache`） | ❌ **没有 DB**。`NewCountsCache(source Source, redisClient)`（`counts_cache.go:38`），source 接的是 orchestrator 的 **Redis 沙箱 store**（`handlers/store.go:283`）。**连 `Invalidate` 方法都没有**，纯 TTL 30s | ❌ **不属于阶段 2** |

⇒ 我们的三个缓存面是（把第三个换掉）：

| # | key | 值 | TTL | 回落 |
|---|---|---|---|---|
| **C-1 别名解析** | `aenv:cat:alias:{cluster}:{alias}` | `snapshot_id`（16 字节） | 5m / refresh 1m | `resolve_alias` SQL |
| **C-2 快照/模板行** | `aenv:cat:rec:{cluster}:{id}` | 投影列 ＋ `committed_payload` | 5m / refresh 1m | `get` SQL |
| **C-3 首页列表** | `aenv:cat:page1:{cluster}:{source_kind}:{filter_hash}` | 第一页的 id 序列 ＋ next_token | **30s，只缓存第一页** | `list_*_page` SQL |

🔴 **C-3 只缓存第一页**，因为深页的 key 空间是游标，命中率趋零而失效面无限。
e2b 也不缓存列表 —— 它靠 keyset ＋ 索引让查询本身够快。
**如果 §6 的索引写对了，C-3 可以直接不做。** 建议：先不做，用 §10 的探针量出 P99，
超过 50ms 再加。少一个缓存面就少一处失效点。

### 7.4 失效点

🔴 **每一条目录写路径都必须失效，一条都不能漏。** e2b 的失效调用点共 20 处
（`snapshotCache.Invalidate` 5 处、`templateCache.Invalidate` 3 处、
`InvalidateAllTags` 4 处、`InvalidateAlias*` 6 处、`buildCache.Invalidate` 2 处）。

我们的写路径少得多，逐条对齐：

| 写路径 | 落点 | 失效 |
|---|---|---|
| `create`（`v3_templates_post` → `api/impls/template.rs:549`） | 事务：INSERT snapshots(waiting) ＋ INSERT aliases | C-1(alias)、C-3 |
| `try_start_build`（`template.rs:634`） | 事务：advisory lock ＋ INSERT builds ＋ UPDATE snapshots→building | C-2(id)、C-3 |
| `publish_commit`（`builder.rs:128-147` / `paused_coordinator.rs:309`） | §5.2 事务 B | C-1(alias)、C-2(id)、C-3 |
| `mark_build_error`（`template.rs:671/718/733/750/782`） | UPDATE snapshots→error | C-2(id)、C-3 |
| `mark_local_only`（`paused_coordinator.rs:361`） | 只动 `paused_sandboxes.state`；目录行不变（§5.2） | **无** —— 目录没变就不该失效 |
| `delete`（`template.rs:459`、`snapshot_manager.delete`） | UPDATE deleted_at_ms（软删）＋ CASCADE 删 aliases | C-1(alias)、C-2(id)、C-3 |
| **构建收割器**（§4.3，新增） | UPDATE builds→error | C-2(template_id)、C-3 |

🔴 **最后一行是最容易漏的**：它是**唯一不由用户请求触发**的写路径。
e2b 的等价物（`template_status.go:305,322`）也在失效名单里。

**实现约束**：失效必须在**事务提交之后**做，且失败只 `warn` 不回滚 ——
失效失败的后果是最多 5 分钟的陈旧读，而回滚一个已提交的目录事务是数据损失。
写成 `defer` 里的 `context.WithoutCancel`，与 e2b 逐字同形（`pause_instance.go:84`）。

---

## 8. 迁移机制

### 8.1 今天怎么做的

**全仓没有任何迁移工具**：无 golang-migrate / goose / atlas / sqlc / dbmate，
`services/go.mod` 的 10 个直接依赖里一个都没有；`find . -name "*.sql"` **零文件**；
`Makefile` 里 `grep migrat` 零命中；`deploy/` 里零个 `kind: Job`。

| 环节 | 位置 |
|---|---|
| 整个 schema 是一个 Go 字符串常量 `SchemaDDL` | `services/scheduler/internal/registry/migrate.go:30-85` |
| 幂等靠 `CREATE TABLE IF NOT EXISTS` ＋ `ALTER … ADD COLUMN IF NOT EXISTS` ＋ `DROP CONSTRAINT IF EXISTS` 再 `ADD` | 同上 |
| 串行化靠 PG advisory lock `0x0A6E_7653_4348_4D41` | `migrate.go:172, 205` |
| **没有版本表** | 全文无 `schema_migrations` |
| 有一个 preflight：数据违反新 CHECK 时**拒绝启动**而不是回填 | `migrate.go:95-103, 135-164`（拒绝信息是中文，指向 runbook） |
| 入口：scheduler 启动时的一个永远重试的 goroutine，迁移失败**不致命** | `services/scheduler/cmd/main.go:147 → :447 → store_postgres.go:235 → migrate.go:188` |
| gRPC 服务先注册、迁移完成前答 `UNAVAILABLE` | `main.go:117, 131, 119-124` |
| `Migrate(ctx) error` 是 `Store` 接口的方法，理由写在 `store.go:162-169`：「There is exactly one owner of this table's shape and it is this process」 | `store.go:169` |

### 8.2 🔴 目录表不能沿用这套

`SchemaDDL` 那套对 `paused_sandboxes` 是够的 —— 那是一张几百行、可完全重建的表。
**目录不是**：它是最贵、不可逆的用户数据（父提案 §5.3 的原话）。
`IF NOT EXISTS` 风格表达不了四类必需操作：

| 操作 | 为什么 `IF NOT EXISTS` 做不到 |
|---|---|
| `CREATE INDEX CONCURRENTLY` | 不能在事务里跑，而 `conn.Exec(ctx, SchemaDDL)` 是一次多语句提交 |
| 列改类型 / 改约束 | 需要"加 NOT VALID → VALIDATE → SET NOT NULL"三步，且步骤间要能中断续跑 |
| 分批回填 | 需要循环里 `COMMIT`（e2b 的 `20260210120002` 用 50k/批 ＋ `pg_sleep(10)`） |
| 回滚一次错误的变更 | 无版本号 ⇒ 无 down |

### 8.3 ✅ 落法：给目录引入版本化迁移，**不动** `paused_sandboxes` 的机制

**保留** `migrate.go` 原样跑 `paused_sandboxes`（一个在跑的东西不要碰），
**新增** `services/scheduler/internal/catalog/migrate.go`：

```
services/scheduler/internal/catalog/
  migrations/
    0001_create_snapshots.sql
    0002_create_templates_builds_aliases.sql
    0003_status_group_trigger.sql
    0004_indexes_concurrently.sql          -- 带 NO TRANSACTION 标记
  migrate.go        // ~150 行 applier，embed.FS
  schema.go
```

约定（**抄 goose 的两个惯例，不抄 goose 本身**）：

- **版本表** `catalog_schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms BIGINT NOT NULL)`；
- 每个文件默认在**一个事务**里跑，首行 `-- +aenv NO TRANSACTION` 则不开事务
  （给 `CREATE INDEX CONCURRENTLY` 与分批回填用）；
- **复用同一把 advisory lock** `0x0A6E_7653_4348_4D41` —— 两套迁移不能并发，
  用两把锁会在同一进程内制造一个可以死锁的顺序；
- 沿用 `migrate.go` 已经验证过的三条形状：register-before-migrate、
  迁移前 preflight 拒绝而非回填、失败永远重试且不致命（`main.go:436-465`）；
- 入口挂在同一个 goroutine，`store.Migrate(ctx)` 之后立刻 `catalog.Migrate(ctx, pool)`。

**为什么不上 goose**：`services/go.mod` 的直接依赖是 10 个人尽皆知的库，
零构建工具、零 `go tool` 依赖；引入 goose 会改构建链与 CI 镜像。
150 行 applier 与仓库现有的手写口味一致，且这四个文件之后大概率半年不动。
🔴 **这是一个可以被推翻的判断** —— 如果之后目录 schema 变更变频繁（>1 次/月），
换成 goose（e2b 的选择，`packages/db/Makefile:5-6`，版本表 `_migrations`）是对的。

### 8.4 🔴 迁移的执行顺序与部署顺序

`snapshots` 有指向自身的外键（`templates.id → snapshots.id`，`builds.template_id → snapshots.id`，
`aliases.snapshot_id → snapshots.id`）⇒ 建表顺序固定：`snapshots` → 其余三张。

部署顺序：**scheduler 先，node 后**（与 recon §7.2 的 B2 一致）。
scheduler 起来跑完迁移、RPC 从 `UNAVAILABLE` 变成可用之后，node 才滚。
反过来 node 会在启动窗口里对着一个没有表的 RPC 报错 —— 双写模式下那是**发布失败**。

---

## 9. 双写与回退

### 9.1 两个开关，分开

抄本仓已验证的三开关做法（`f7eef4c feat(config): settle the three switches this guard is rolled out behind`）：

```toml
[snapshot.catalog]
# 写侧：object_store（今天）| both（双写）| postgres（拆掉双写，阶段 3 之后才允许）
write = "object_store"
# 读侧：object_store（今天）| postgres
read  = "object_store"
```

对应 env：`AENV_SNAPSHOT_CATALOG_WRITE` / `AENV_SNAPSHOT_CATALOG_READ`
（🔴 env 覆盖 config —— 与 `AENV_PAUSED_REGISTRY_BACKEND` 同样的理由，
recon §6 的 ConfigMap 漂移会把配置文件里的值退回去）。

**合法组合矩阵**，非法组合在启动时 `bail!`（照 `paused_registry/mod.rs:436` 的形状）：

| write | read | 合法 | 阶段 |
|---|---|---|---|
| `object_store` | `object_store` | ✅ | 今天 / 2b 之前 |
| `both` | `object_store` | ✅ | **2b 的稳态** |
| `both` | `postgres` | ✅ | **2c 的稳态** |
| `postgres` | `postgres` | ✅ | **阶段 3 之后** |
| `postgres` | `object_store` | ❌ 启动失败 | 读一个不再被写的地方 |
| `object_store` | `postgres` | ❌ 启动失败 | 读一个空表 |

### 9.2 双写不变式

🔴 **PG 先写，对象存储后写。** 顺序不能反：PG 是那个能原子提交的地方，
它成功即"已发布"；对象存储是**从 PG 派生**的镜像。

| # | 不变式 | 违反时 |
|---|---|---|
| **I1** | 每一次目录写（create / publish_commit / try_start_build / mark_build_error / mark_local_only / delete / 收割）都写两侧 | 回退时丢记录 |
| **I2** | PG 事务失败 ⇒ 整个操作失败，**不写对象存储** | 对象存储里出现 PG 没有的行，回退方向反了 |
| **I3** | PG 成功、对象存储失败 ⇒ **操作仍然成功**，记一次 `catalog_mirror_failed_total`，行进入待补 | 让镜像失败杀掉一次真实发布，等于把可用性做成两个系统的与 |
| **I4** | 一个**只朝一个方向**的补偿器（PG → 对象存储）持续把待补行重放，并导出 `catalog_mirror_lag` gauge | I3 的漏洞变成永久分歧 |
| **I5** | 🔴 **`read` 切回 `object_store` 时，启动做一次 preflight：`catalog_mirror_lag > 0` 则拒绝启动**，并说明差多少行 | 「零数据损失」这句承诺失效 |

I5 是让父提案「回退是把读侧开关切回对象存储，**零数据损失**」这句话真正成立的东西 ——
没有它，那只是一句期望。preflight 的形状照抄 `migrate.go:135-164`（拒绝而不是自作主张）。

**补偿器的方向只能是 PG → 对象存储**，因为对象存储的目录行可以从 PG 的列 ＋
`committed_payload` 完整重建（那正是它今天的内容），反向不成立（PG 有 `status_group`、
`heartbeat_at_ms`、`published` 这些对象存储里没有的东西）。

### 9.3 什么时候拆掉双写

父提案：**阶段 3 上线并稳定之后**。翻译成可判定的条件：

1. 阶段 3 已发布，且 `--role api` 在集群上跑满一个观察期；
2. `catalog_mirror_lag` 连续 7 天为 0；
3. `catalog_read = postgres` 下 `agentenv_snapshot_object_store_requests_total{op="list"}` 连续 7 天为 0。

三条都满足才允许 `write = "postgres"`。**删除对象存储目录里的历史 `catalog/records/`
再往后放一个 release**（父提案 §7 抬头：删除不与切换同批）。

---

## 10. 验证探针

🔴 **每条探针必须自带对照面。** recon §8 记着上一轮的真实翻车：
「合成行在任何相位都认领不了，"看着像被拒了"其实是没有分辨力」。
下面每条的"对照"列都是**必然给出相反结果**的输入。

### 10.1 🔴 先补一个不存在的指标

头号判据是「`GET /snapshots?limit=10` 的对象存储请求数为 0」。**今天没法观测**：

```
$ grep -rn "object_store\|oss_request\|opendal" src/ crates/object-store-operator/src --include=*.rs | grep -i "metric\|counter"
（零命中）
```

设施是有的：`metrics = "0.24"` 门面（`Cargo.toml:78`）＋ prometheus recorder
（`src/bin/server.rs:58` 的 `init_prometheus_recorder()`）＋ `/metrics` 路由
（`src/api/server.rs:35`），全仓已有 20 余处 `metrics::counter!`。

⇒ **阶段 2 的一部分**：在 `OssClient` 的 8 个请求方法上各埋一次
（`backends/oss/client.rs:114 get_bytes` / `:127 get_to_file` / `:150 exists` /
`:164 list_keys_recursive` / `:189 put_bytes` / `:222 put_file` / `:271 delete` / `:284 delete_prefix`），
POSIX 侧在 `catalog.rs` 的 `read_json` / `write_json` / `read_dir` 上对等埋点：

```rust
metrics::counter!(
    "agentenv_snapshot_object_store_requests_total",
    "op"      => "list" | "get" | "put" | "delete" | "head",
    "surface" => "catalog" | "artifact",     // 🔴 必须区分，否则判据被字节流量淹没
    "outcome" => "ok" | "error",
).increment(1);
```

🔴 **`surface` 标签是判据能不能成立的关键。** 阶段 2 之后对象存储仍然承载全部字节，
`op="put"` 会一直很高。判据说的是 `surface="catalog"` 那一支为 0。

### 10.2 探针清单

| # | 判据 | 做法 | 🔴 对照面（必须给出相反结果） |
|---|---|---|---|
| **P1** | **头号：10k 条目录，`GET /snapshots?limit=10` 的对象存储请求数为 0** | 造 10k 行；记录 `…object_store_requests_total{surface="catalog"}` 基线；发 20 次请求；增量必须 **= 0** | 同一发请求在 `catalog_read = object_store` 下重跑：增量必须 **≥ 20 × (1 + 10000)**（1 次 LIST ＋ 10k 次 GET，`oss/repository.rs:369-397`）。两相同答 ⇒ 探针瞎了（recon §8 第 2 条） |
| **P2** | keyset 翻页无重无漏 | 10k 行，`limit=10` 翻到底，收集 id 集合 | ① 集合大小恰为 10,000 且无重复；② 翻页中途插入一条 `created_at` 落在**已翻过**区间的行 ⇒ 它**不出现**；③ 插一条 `created_at` 最新的 ⇒ 也**不出现**。offset 分页在 ② 会漏一条、在 ③ 会重一条 |
| **P3** | 别名唯一性 | 两个并发 publish 争同一别名 | PG 侧：恰好一个 2xx、一个 `AliasConflict`。**对照**：同一测试打 `catalog_write = object_store` ⇒ 必须能复现出**两个都成功**或丢失更新（`oss/repository.rs:590-682` 的读-改-写-回读）。复现不出来 ⇒ 并发度不够，加压 |
| **P4** | 构建准入 | 对同一 template 并发发 N=20 次 `POST …/builds/{id}` | 恰好 1 次进入 `building`，19 次拿到明确冲突；`builds` 表里 `status_group IN ('pending','in_progress')` 的行数 = 1。**对照**：临时 drop `builds_one_active_per_template` 重跑 ⇒ 必须出现 >1（对应 `oss/repository.rs:469-493` 今天的行为） |
| **P5** | 🔴 **翻牌的原子性** | 在 §5.2 的 ② 与 ③ 之间 `SIGKILL` 进程 | ① 行停在 `status='building'`；② `GET /snapshots` **不列出它**；③ `resolve_runnable` **拒绝它**；④ `paused_sandboxes` 停在 `publishing`。**对照**：手工把 `status` 改成 `ready` ⇒ 三条读路径立刻都能看到它。看不到 ⇒ 探针没走到解析查询 |
| **P6** | 🔴 **收割器与唯一索引的联动**（§3.2 的陷阱） | 起一次构建，`SIGKILL` node；等过 `build_heartbeat_ttl`；再对同一 template 发起构建 | 第二次必须**成功**。**对照**：把收割器关掉重跑 ⇒ 第二次必须**永久失败**。这一条不测，加了唯一索引就是把泄漏升级成故障 |
| **P7** | 双写一致 | `write=both` 跑完一整套 CRUD | PG 行数 == 对象存储 `catalog/records/` 对象数，逐 id 对齐。**对照**（recon §8 第 3 条，两侧互证）：`GET /snapshots` 翻页到底的条数 == `SELECT count(*) FROM snapshots WHERE status_group='ready' AND source_kind='sandbox' AND deleted_at_ms IS NULL`，**逐条对得上**，不是数量级相近 |
| **P8** | 回退是零损失 | `write=both`，把 OSS endpoint 指到一个死地址，做 10 次提交 | `catalog_mirror_lag` == 10；此时把 `read` 切回 `object_store` ⇒ **启动被 I5 的 preflight 拒绝**，错误信息里带缺失行数。**对照**：让补偿器跑完、lag 归 0 ⇒ 同样的切换成功启动 |
| **P9** | 缓存失效不漏 | 对 §7.4 表里**每一行**：读一次（填缓存）→ 走那条写路径 → 立刻再读 | 第二次读必须看到新值。**对照**：把该路径的失效调用注释掉重跑 ⇒ 必须读到旧值。🔴 收割器那一行最容易漏，单独跑 |
| **P10** | 目录规模不再影响延迟 | 100 / 1k / 10k 行三档，各测 `GET /snapshots?limit=10` 的 P99 | 三档之间的 P99 差异 < 20%。**对照**：`catalog_read=object_store` 下同样三档，必须呈线性增长。不增长 ⇒ 数据没造进去 |
| **P11** | 🔴 **origin 硬钉（§3.1 / §5.4）** | 造一个 `published=false` ＋ `origin_node_id=A` 的快照（把 OSS endpoint 指死，让一次 pause 的发布失败）；从**节点 B** 发起 resume | B 必须被拒，错误里点名 A（与 `api/impls/sandbox.rs:760-766` 的既有措辞一致）；从 **A** 发起必须成功。**对照面**（recon §8 第 1 条，要一个必然为假的输入）：把同一行 `UPDATE snapshots SET published=true` ⇒ **B 必须立刻成功**。改一列就翻转结论，才说明钉的是这一列而不是别的东西（比如恰好 binding 还指着 A） |
| **P12** | 🔴 **暂存与提交可分离（§5.5）** | 调 `stage(...)` 拿到 `StagedSnapshot`，**不调** `commit_staged`；然后 ① 把它 `serde_json` 往返一次，② 查目录 | ① 往返后 `commit_staged` 仍然成功 ⇒ 证明它是纯值、过得了线（阶段 3 的前提）；② 只 stage 不 commit 时，`GET /snapshots` 与 `resolve_runnable` **都看不到它**，而 `artifacts/{id}/` 下的字节**在**。**对照面**：补上 `commit_staged` ⇒ 两条读路径立刻都能看到。字节在而目录看不见，正是 §5.1 要的形状 |

### 10.3 射程边界（照实写，recon §8 第 4 条）

- P1–P10 全部证明的是**目录这一侧**。它们**不证明** node 写字节的路径没有回归 ——
  那条路径阶段 2 不变，但双写会改变它的错误处理时序（I3）。
- P5 是单进程 kill，**不证明**多副本下的翻牌竞态 —— 那是阶段 3 的 `execution_id` 谓词
  （§5.2 末尾留的列）要保护的东西，阶段 2 无法验证。
- 🔴 **P1 的 10k 行必须是真造出来的，不是 `INSERT INTO snapshots SELECT …` 灌进 PG 的合成行** ——
  合成行没有对应的对象存储目录对象，`catalog_read=object_store` 的对照面会假通过（增量为 0），
  于是 P1 的两个相位给出相同答案，探针作废。**造法**：`write=both` 下跑 10k 次
  `v3_templates_post` ＋ 直接写 `catalog/records/` 的辅助脚本，两侧都要有。

---

## 11. 工作量与阶段拆分

### 11.1 逐区估算（含测试）

| 区域 | 新增 | 改写/搬移 | 说明 |
|---|---|---|---|
| **Go · 迁移机制 ＋ 4 张表 DDL** | 450 | 0 | applier ~150 ＋ 4 个 `.sql` ~200 ＋ 测试 ~100 |
| **Go · 目录 store（查询层）** | 900 | 0 | 12 条语句 ＋ 事务 A/B/C ＋ advisory lock ＋ 收割器 |
| **Go · `service SnapshotCatalog` 服务实现** | 450 | 0 | 照 `registry_service.go` 的形状 |
| **Go · Redis 目录缓存（C-1/C-2）** | 400 | 0 | 🔴 **条件项** —— §7.2 建议先不做，由 P10 的 P99 决定。做的话要配独立实例（`noeviction` 冲突） |
| **Go · 测试** | 1,800 | 0 | 对标 `store_postgres_test.go` 2,217 行的密度；需要 `test-with-postgres` |
| **proto** | 350 | 0 | 一个 service ＋ 消息；additive，无兼容包袱 |
| 🔴 **Rust · trait 拆分**（§2.3） | 300 | **1,200** | `SnapshotRepository` → `SnapshotCatalog` ＋ `SnapshotArtifactStore`；OSS 的 1,394 行要真拆 |
| 🔴 **Rust · `stage` / `commit_staged`**（§5.5） | 320 | 60 | `StagedSnapshot` ＋ 拆 `manager.rs:88-120` ＋ 补两处 serde derive。**增量骑在上一行的手术上**；阶段 3 的估算是 ~260，我算 320，差在两个 serde 缺口 |
| **Rust · `backends/central/` 目录客户端** | 700 | 0 | 照 `paused_registry/central.rs` 的形状（那个文件 2,460 行，但含大量测试） |
| **Rust · 双写包装 ＋ 补偿器 ＋ lag gauge** | 600 | 0 | I1–I5 |
| **Rust · 分页下沉** | 250 | 150 | `SnapshotListFilter` ＋ `list_page` ＋ 5 个调用点；`templates_get` 补分页 |
| **Rust · 对象存储请求指标** | 150 | 0 | 8 + 3 个埋点 ＋ `surface` 标签 |
| **Rust · 构建心跳** | 200 | 0 | `template.rs:652` 的 spawn 里加 tick |
| **Rust · 配置 ＋ 启动 preflight** | 120 | 0 | 两个开关 ＋ 矩阵校验 ＋ I5 |
| **Go/Rust · origin 钉选** | 180 | 0 | §5.4 的 `pinOriginIfUnpublished` ＋ 两列的投影/写入 ＋ 复用既有拒绝措辞。**按 V1–V6 写，将来整块可删** |
| **Rust · 测试** | 1,500 | 0 | |
| **部署** | 200 | 0 | Redis 清单（SD-B1，三阶段共享）＋ scheduler env |
| **合计** | **≈ 8,870** | **≈ 1,410** | |

对照：上一轮阶段 2/3 的 `services/scheduler/internal/registry` 整个是 10,076 行（3,483 非测试）。
**这一刀与那一刀同量级。**

### 11.2 🔴 这不是一个阶段

三条独立的理由：

1. **回退面不是一个**。schema、双写、读切换各自的回退动作完全不同
   （删表 / 关写开关 / 关读开关），父提案 §7 抬头的规矩要求它们分批。
2. **它有一个跨语言的中点**。Go 侧建好而 Rust 侧未接，是一个**可以稳定停留的状态**；
   把它和 Rust 侧捆一起发，等于放弃一个免费的观察期。
3. **它有一个基础设施前置**（Redis，SD-B1），而那个前置是和阶段 1 共享的。
   捆在一起会让阶段 1 和阶段 2 互相阻塞。

### 11.3 ✅ 拆法

| | 内容 | 前置 | 兑现 | 回退 | 规模 |
|---|---|---|---|---|---|
| **2a · 承载层** | 迁移机制；4 张表；Go 目录 store ＋ `service SnapshotCatalog`；对象存储请求指标（Rust 侧） | 无（🔴 **不再包含 Redis** —— 见下） | 指标能观测；RPC 能答；表在。**行为零变化** | 删表 ＋ 回滚 scheduler 镜像 | Go ~3,200 ＋ Rust ~150 |
| **2b · 双写** | Rust trait 拆分；🔴 **`stage` / `commit_staged`（§5.5）**；`backends/central/`；双写 ＋ 补偿器 ＋ lag；`write = both` | 2a | 目录有了第二份、可校验的副本；I1–I5 全部生效；**bytes-then-commit 的缝在这里真正劈开，且立刻被跨进程消费** | `write = object_store` | Rust ~3,120 ＋ 搬移 ~1,410 |
| **2c · 读切换** | keyset 分页下沉；`read = postgres`；`templates_get` 补分页；构建准入 ＋ 心跳 ＋ 收割器。**目录缓存不在此列**（条件项，§7.2） | 2b 稳定 ≥1 个观察期 | **P1 头号判据在这里通过** | `read = object_store`（受 I5 preflight 保护） | Go ~400 ＋ Rust ~1,700 |

各自的判据：**2a → P0（指标非零，见下）；2b → P3 P7 P8 P12；2c → P1 P2 P4 P5 P6 P10 P11。**
**P9（缓存失效）只在真做了目录缓存时才跑**，与它同批。

> **P0**（2a 专属）：埋点上线后，跑一次今天的 `GET /snapshots`，
> `…object_store_requests_total{surface="catalog"}` 必须**长出 1 + N**。
> 恒 0 说明埋点没生效 —— recon §8 第 2 条，"某指标恒 0 本身不是证据"。

🔴 **Redis 不再是阶段 2 的前置。** §7.2 的结论把目录缓存降级成条件项之后，
2a/2b/2c 三批都不依赖 Redis：目录读走 PG，缓存先不做。
⇒ **E2 的第一半（前置写成"无"）实际上可以成立**，但要靠"先不做缓存"这个决定去成立，
而不是靠父提案原文那句「目录缓存同批进 Redis」。**第二半（凭据归属）依然成立且不可协商** ——
真要做缓存时，它必须在 scheduler 的 Go 进程里，不能在 node 的 Rust 进程里。

🔴 **2b 与 2c 之间的观察期不能省。** 双写期是唯一能在真流量上发现
"PG 与对象存储对不上"的窗口；一旦切了读，分歧就变成静默的正确性问题。

---

## 12. 🔴 对父提案的更正

三轮对抗审查各查出真错误；这是第四轮，着眼于"阶段 2 照着能不能做"。
**E1 / E2 / E3 会改变能不能执行，其余是事实订正。**

| # | 父提案的说法 | 事实 | 影响 |
|---|---|---|---|
| **E1** 🔴 | §4.2.1 / D12 / §7 阶段 2：目标是「消掉 `local_only`」，依赖一个**本仓之外**的交付物；「退路是目录行带 `origin_node_id` ＋ `published` 两列」 | 那条依赖**出了本轮范围**（用户裁决 2026-08-20）。而在本仓内部消掉它是**三步、约 2,000–3,000 行、跨 Rust／Go／proto 的独立工作**，且第二步会改变 pause 的对外语义 —— 定量依据在 §3.1.b（`operator.rs:13,35-36,96,102-106`；`oss/client.rs:29,83-84`；`service.rs:1524`；`paused_coordinator.rs:346-364`） | **"退路"是唯一设计，且现在就建**（§3.1）。同时按 §3.1.a 的 V1–V6 设计成"将来可整列删除"，并给出那次删除的完整迁移文件 |
| **E2** 🔴 | §7 阶段 2：「**前置**：无。与阶段 1 可并行。」 | 两处不成立：① §8 陷阱 3 把 Redis 目录缓存并进本批，而集群里**没有 Redis**（recon §4.5 / SD-B1）；② 更重要的是，**目录缓存的凭据问题和 PG 一模一样** —— 缓存跑在 Rust 进程里就意味着 node 拿 Redis 凭据，直接违反 §0 硬约束 2，而模块文档 D6 恰好写过「e2b 的节点有一处 Redis，我们不抄这一处」 | 前置改为「Redis 已部署（SD-B1，与阶段 1 共享）」；缓存与目录**同侧**，落在 scheduler 的 Go 进程里（§7.2） |
| **E3** 🔴 | §7 阶段 2：「`SnapshotRepository` 的目录读写走 PG」 | 字面无法执行：Rust 零 PG 依赖（`Cargo.toml` ＋ `Cargo.lock` 都零命中），且 `--role` 不存在（SD-B2）⇒ 代码全跑在 node 上 | 见 §1。答案是走 scheduler gRPC；**关键论据是 §1.4：让 Rust 直连 PG 反而买不到"原子提交"** |
| **E4** | §2.1 表格 ＋ §8 陷阱 3 ＋ §12 M10：e2b `packages/api/internal/cache/` 的「**三个子包全部**是 Redis ＋ DB 回落」 | **两个是**。`sandboxcounts` 的构造函数是 `NewCountsCache(source Source, redisClient)`（`counts_cache.go:38`），`source` 接的是 orchestrator 的 **Redis 沙箱 store**（`handlers/store.go:283` → `orchestrator/admin.go:13-15` → `sandbox/store.go:124-126`），**没有 `*sqlcdb.Client`，也没有 `Invalidate` 方法**，纯 30s TTL | 第三个缓存面在我们这里**不属于阶段 2**（沙箱计数来自 node 的内存 store，不是目录派生）。§7.3 用"首页列表"顶替，并建议先不做 |
| **E5** | §5.1 与附录：`pause_instance.go:71-82` 的翻牌、`:85` 的 `snapshotCache.Invalidate` | 翻牌在 `:72-77`（`:71` 是 `now := time.Now()`）；`Invalidate` 在 **`:84`**，`:85` 是空行 | 引用订正 |
| **E6** | §5.1 那张图读起来像「node 写字节 → `api` 在一个事务里写目录行」 | e2b **没有跨 RPC 的事务**：`UpsertSnapshot`（4-CTE 单语句）→ Pause RPC → `UpdateEnvBuildStatus`（裸 UPDATE），三条 autocommit，`grep WithTx\|BEGIN` 零命中。原子性来自"单条多 CTE 语句"＋"`status_group='ready'` 挡住孤儿" | §5.1 明说别去造跨 RPC 的事务；§5.2 给出两条语句的落法 |
| **E7** | §7 阶段 2：「对象存储只留字节」，读起来像拿走一半 | `SnapshotRepository` 今天把目录与字节**焊死**：POSIX 已分（`catalog.rs` 997 / `artifacts.rs` 1,114），**OSS 完全交织**（`oss/repository.rs` 1,394 行，`publish()` 从 `:191` 到 `:348` 一路做到底） | trait 拆分是阶段 2 Rust 侧最大单项（~1,200 行搬移），父提案未计入。§2.3 ＋ §11.1 |
| **E8** | §4.2：「`paused_sandboxes` 表不是「搬到 Redis」，是整个消失。暂停态本来就是目录的一部分」 | 对绝大多数行成立，对 `state='local_only' AND snapshot_id IS NULL` 这类行**不成立** —— 它们**没有快照可以变成目录行**。而这类行谁也认领不了（`store_postgres.go:783-785` 要求 `snapshot_id IS NOT NULL`）、两条 reclaim 都不碰、永久滞留（`reconcile.go:117-125` 的 `strandedRows`），且 recon §8 待办 (4) 已把它登记为「不是一行 SQL，是设计问题」 | §3.1.c：折叠有一个洞。`paused_sandboxes` 在这类行清零之前不能删，而 🔴 **本仓今天没有清零手段** —— 补发路径不存在（§3.1.b 第三步），只能人工删沙箱 |
| **E9** | §10 F5 的处置：`status_group='ready'` 的谓词在「快照与模板的解析查询里」，列了三条 | 属实，但**漏了最重要的第四条**：`GetLastSnapshot`（`packages/db/queries/snapshots/get_last_snapshot.sql:8`）—— 那正是 resume 路径经 `snapshotCache` 读的查询 | §5.3 补齐四条，并额外指定**两条必须不带谓词**的查询，写错会让构建状态永远查不到 |
| **E10** | 附录证据索引：「列表在内存里分页 `src/api/impls/snapshots.rs:90`」 | 分页调用在 `:91`（`:90` 是空行）。另：`src/api/impls/templates.rs` 不存在，文件名是 `template.rs`；`SnapshotMetadata` 这个类型全仓不存在（目录实体是 `SnapshotRecord`） | 引用订正 |
| **E11** 🔴 | §7 阶段 2 的任务分解里**没有**把 `SnapshotManager` 的 bytes-then-commit 拆分派给任何人，尽管 §5.1 把它当成整个阶段的理由；阶段 3 的结构设计（其 §4.4）把它列为阶段 2 的硬前置 | **同意阶段 3 的判断，本文接手**（§5.5）。顺带一条独立更正：§6.2① 说 `execution_id` 要「变成路径名」，**在我们的代码里命名隔离已经成立了，靠的是每次现铸的 `SnapshotId`**（`paused_coordinator.rs:695-698`、`builder.rs:58`、`template.rs:537`），不需要把 `execution_id` 塞进路径；`execution_id` 只承担**提交排他**那一半 | §5.5 归入 2b 批（增量 ~320 行）。🔴 别为路径隔离把内容寻址的 `managed-layers/{digest}` 也塞进 per-snapshot 前缀 —— 那会让去重失效 |

**经受住这一轮的**：§5（存储必须先动 —— §1.4 反而给了它第二个、更硬的论据）、
§5.1 的「拆开写字节与宣布生效」这个动作本身、§5.3（存储分层今天是反的）、
§7 抬头「删除不与切换同批」、§8 陷阱 3 的**结论**（目录在哪缓存就在哪）——
只是它的 e2b 依据要按 E4 收窄，落点要按 E2 改到 Go 侧。

### 12.1 顺带发现、不属于阶段 2 但要登记

| 发现 | 位置 | 处置 |
|---|---|---|
| **发布时别名先于记录写入**，直接违反 trait 自己在 `interfaces.rs:112-113` 的契约；读路径的陈旧别名回收会删掉正在发布的快照的别名 | `oss/repository.rs:299-310`；`posixfs/catalog.rs:109-111`；回收在 `oss:455-464`、`posix:295-301` | 阶段 2 顺带修掉（PG 侧外键 ＋ 单事务）；🔴 切到 PG 之后那两处回收**必须删掉**，留着会误删合法别名 |
| **content-addressed managed layers 全仓没有 GC**，且发布回滚显式跳过它们 | `oss/repository.rs:301-302, 324-325` | 不在阶段 2 范围。但目录进 PG 之后，"哪些 layer 还被引用"第一次变成一条可写的 SQL ⇒ 登记为阶段 2 之后的一个便宜收益 |
| `pagination.rs:112` 的 `items.get(limit - 1)` 在 `limit == 0` 时下溢 panic，今天靠 `:101` 的早返回挡着 | `src/api/impls/pagination.rs:101, 112` | 分页下沉时顺手消掉 |
| `POST …/builds/{id}` 的 `tokio::spawn` 脱管，进程死掉记录永卡 `Building`，全仓无收割器 | `src/api/impls/template.rs:652` | 🔴 **必须在阶段 2 修**，否则 `builds_one_active_per_template` 把泄漏升级成故障（§3.2、P6） |
| `templates_get`（v1）完全没有分页，返回全量模板 | `src/api/impls/template.rs:322-346` | 阶段 2 同批补上，否则 10k 记录下它先炸而判据看不到 |
| PG / RustFS 的 PVC 都是 `local-path`、钉死 204、无备份 | recon SD-B5 | 🔴 目录进 PG 之后 PG 成为不可逆用户数据的唯一真相源。**2a 上线前必须谈一次持久化**，这是 recon 自己提的 |

---

## 附：证据索引（本文新增，不重复父提案已有的）

**AgentENV**

| 事实 | 位置 |
|---|---|
| 🔴 Rust workspace 零 PG 依赖（21 个 `Cargo.toml` / 843 个锁定 crate） | `grep -rn "sqlx\|tokio-postgres\|postgres\|diesel\|deadpool-postgres" --include=Cargo.toml .` 空；`Cargo.lock` 同 |
| Rust 的 PG 后端是被物理删除的，理由写在模块文档里 | 提交 `4208f47`；`paused_registry/central.rs:1-21`；`mod.rs:436-443` |
| 目录实体是 `SnapshotRecord`；**`SnapshotMetadata` 不存在** | `src/snapshot/types/snapshot.rs:328-337` |
| `SnapshotId::to_uuid()` 注释已经在为 UUID 列做准备 | `src/snapshot/types/value.rs:26` |
| 🔴 `SnapshotRepository` 把目录与字节焊死；POSIX 已分而 OSS 未分 | `interfaces.rs:80-176`；`posixfs/catalog.rs`(997) vs `artifacts.rs`(1114)；`oss/repository.rs:191-348` |
| trait 上**没有任何分页概念**，`list()` 返回全量 `Vec` | `interfaces.rs:17-35, 148` |
| OSS `list()` = 一次 LIST ＋ 每条一次 GET ＋ 内存过滤 ＋ 内存排序 | `oss/repository.rs:369-397`；`oss/client.rs:163-186` |
| 🔴 OSS `try_start_build` 是裸的读-改-写，两个并发 POST 都会开构建 | `oss/repository.rs:469-493` |
| 别名绑定自述「weaker than a true CAS」 | `oss/repository.rs:590-682`（注释 `:606-609`） |
| POSIX 的锁是 `create_new` 锁文件 ＋ 60s 陈旧抢占，不是 `flock` | `posixfs/catalog.rs:17-19, 521-628` |
| 🔴 发布时别名先于记录写入，违反 `interfaces.rs:112-113` 自己的契约 | `oss/repository.rs:299-310`；`posixfs/catalog.rs:109-111` |
| 分页在 HTTP 层做，游标格式 base64url(`rfc3339__value`)，`created_at DESC, id ASC` | `src/api/impls/pagination.rs:91-120, 127-134, 141-148` |
| `templates_get`(v1) 无分页；`limit=None` 时返回全量 | `src/api/impls/template.rs:322-346`；`pagination.rs:110-116` |
| 🔴 构建 `tokio::spawn` 脱管，无收割器 | `src/api/impls/template.rs:652` |
| `template_id == build_id` 由 API 强制 | `src/api/impls/template.rs:417` |
| pause 的发布窗口：`begin_pause` → `publish_captured` → `complete_pause` | `src/api/impls/paused_coordinator.rs:288, 301-304, 309-312` |
| `mark_local_only` 只匹配 `state='publishing'`，且**刻意不删行** | `services/scheduler/internal/registry/store_postgres.go:697-737`；测试 `TestMarkLocalOnlyKeepsTheRow` |
| 🔴 `local_only + snapshot_id IS NULL` 谁也认领不了，永久滞留 | `store_postgres.go:783-785`；`services/scheduler/internal/reconcile.go:117-125`；`metrics.go:147` |
| 迁移是一个 Go 字符串常量 ＋ advisory lock，**无版本表**；全仓零 `.sql` 文件、零迁移工具 | `services/scheduler/internal/registry/migrate.go:30-85, 172, 188`；`services/go.mod` |
| 迁移入口在启动的永久重试 goroutine 里，失败不致命 | `services/scheduler/cmd/main.go:147, 447, 436-465` |
| gRPC 先注册、迁移完成前答 `UNAVAILABLE` | `services/scheduler/cmd/main.go:117, 119-124, 131` |
| `service PausedRegistry` —— (b) 方案的现成先例 | `services/api/proto/scheduler.proto:498-516` |
| scheduler 的 DSN 来源；node DaemonSet **无任何 PG env** | `deploy/k8s/base/scheduler-deployment.yaml:58-63`；`agentenv-daemonset.yaml:58-62` |
| 🔴 **全仓没有对象存储请求计数指标**；但 `metrics` 门面 ＋ prometheus recorder ＋ `/metrics` 都在 | `Cargo.toml:78`；`src/bin/server.rs:58`；`src/api/server.rs:35`；`grep object_store.*metric src/` 零命中 |
| OSS 请求面共 8 个方法 | `oss/client.rs:114, 127, 150, 164, 189, 222, 271, 284` |
| 🔴 **每次 pause / 每次构建都新铸 `SnapshotId`** ⇒ 命名隔离已成立，不需要把 `execution_id` 塞进路径 | `src/api/impls/paused_coordinator.rs:695-698`；`src/template/builder.rs:58`；`src/api/impls/template.rs:537` |
| bytes-then-commit 的缝：`CommittedSnapshot` 构造完成之后、目录第一次写入之前 | `oss/repository.rs:285-296`（构造）vs `:299`（bind_alias）/ `:310`（write_record） |
| POSIX 侧本来就已经是"先字节后目录"的三段式 | `posixfs/backend.rs:149-197`（`begin_publish` → `import_built_artifacts` → `commit_publish`） |
| `CapturedSandboxSnapshot` 是持有本地临时目录的进程内句柄，过不了线 | `src/sandbox/backend.rs:137-142` |
| 🔴 `SnapshotPublishMetadata` **没有 Serialize/Deserialize**，拆 `StagedSnapshot` 时要补 | `src/snapshot/types/snapshot.rs:21-35`（`#[derive(Clone, Debug)]`） |
| manifest 的路径字段全部 `#[serde(skip)]` ⇒ 提交侧不该回头读本地路径 | `src/sandbox/firecracker/manifest.rs:17-63` |
| P2P 广告在提交之后、且需要本地字节 ⇒ 阶段 3 需要 node 侧回调 | `src/snapshot/manager.rs:118, 124-193` |
| 内容寻址的 managed layers 共享且刻意不回滚 | `oss/repository.rs:301-302, 324-325` |
| 重试预算的旋钮**已存在但未接线**：`timeout` / `max_retries` 是 `Option` 字段，OSS backend 硬传 `None` ⇒ 吃默认 `DEFAULT_MAX_RETRIES = 3` | `crates/object-store-operator/src/operator.rs:13, 35-36, 96, 102-106`；`oss/client.rs:83-84` |
| `UPLOAD_CONCURRENCY = 8` 是编译期常量 | `oss/client.rs:29` |
| 发布在 pause 请求里同步 await，注释说明这是刻意的 | `src/orchestrator/service.rs:1524`（及 `:1521-1523` 的注释） |
| 🔴 发布失败即终态，**全仓无补发路径** | `src/api/impls/paused_coordinator.rs:346-364` |
| 🔴 `claim_for_resume` / 路由 / 认领失败三处对 `publishing` 与 `local_only` **一视同仁** ⇒ 目录侧一个 bool 就够 | `store_postgres.go:784-785, 900-903`（「both mean parked on its origin node」）；`lookup.go:270-313` |
| `paused_sandboxes.state` 的五态 CHECK —— 细分状态已有归属，不要复制进目录 | `services/scheduler/internal/registry/migrate.go`（`CHECK (state IN (...))`） |

**e2b**（`/home/debian/e2b-infra`，HEAD `6938cbb72`）

| 事实 | 位置 |
|---|---|
| keyset 分页的**操作数互换**技巧（混合方向排序用一次行比较表达） | `packages/db/queries/get_snapshots_with_cursor.sql:34-35`；索引 `migrations/20250923103546` |
| 有下一页的探针是 `LIMIT limit_plus_one` | `get_team_templates_with_cursor.sql:54` |
| 🔴 `status_group='ready'` 的**第四条**解析查询 —— resume 路径实际用的那条 | `packages/db/queries/snapshots/get_last_snapshot.sql:8` |
| `status_group` 是**触发器维护的 text 列**，不是 `GENERATED`、不是 enum；配 4 组映射 | `migrations/20260210120002_add_status_group_column.sql:3, 6-21` |
| 只索引活构建的部分索引 | `migrations/20260305120000`（`WHERE status_group IN ('pending','in_progress')`） |
| 窄侧表 `active_template_builds` ＋ `created_at > NOW() - INTERVAL '1 day'` 作为崩溃恢复 TTL | `migrations/20260305130000`；`queries/builds/get_inprogress_builds.sql` |
| 终态转换与出队原子（一条 CTE 语句） | `queries/update_template_build_status.sql.go:16-26` |
| 并发构建查询 | `queries/builds/get_concurrent_template_builds.sql` |
| 配额主体是 tier 表的列（我们无对应物） | `migrations/20250901161352_add_concurrent_template_builds_to_tier.sql` |
| 🔴 翻牌**没有跨 RPC 的事务**，三条 autocommit | `pause_instance.go:36, 58, 72`；`grep WithTx\|BEGIN` 零命中 |
| `UpsertSnapshot` 是一条 4-CTE data-modifying statement | `queries/snapshots/create_new_snapshot.sql:1-106` |
| 缓存失效在 `:84`，`context.WithoutCancel` | `pause_instance.go:84` |
| 🔴 `sandboxcounts` **没有 DB 回落、没有 Invalidate**，是 Redis-over-Redis | `cache/sandboxcounts/counts_cache.go:28-50`；`handlers/store.go:283`；`orchestrator/admin.go:13-15` |
| 两级缓存的通用实现（Redis L1 ＋ callback L2 ＋ singleflight ＋ redislock） | `packages/shared/pkg/cache/redis.go:99-140` |
| 软删除 ＋ **不暴露 `deleted_at` 的读视图** | `migrations/20260628120000_add_env_deleted_at.sql:7-22` |
| 迁移工具是 goose，版本表 `_migrations`，`NO TRANSACTION` 用于 `CREATE INDEX CONCURRENTLY` | `packages/db/Makefile:5-6` |
| `snapshots.origin_node_id text NOT NULL`，且**可被改写**（亲和自愈） | `migrations/20250708135401`、`20250824185634`；`queries/snapshots/update_snapshot_origin_node.sql:5` |
| 别名唯一靠 `NULLS NOT DISTINCT`（我们不需要，因为没有 namespace） | `migrations/20260127120000` |
