# D11 · 把 PG 从 node 上摘干净

> 2026-08-19 · 阶段 2 的收尾任务书。上游权威：
> [`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md)
> · 收口记录：[`2026-08-19-control-plane-refactor-outcome.md`](2026-08-19-control-plane-refactor-outcome.md)
>
> **裁决依据**：本轮取舍对照 e2b（`/home/debian/e2b-infra`）的实际实现，不凭直觉。

---

## 0. 一句话

阶段 2 让 node **可以不走 PG**，这一轮让它**不能走 PG** —— 删掉 `postgres` 后端本身。

摘除不是删一个文件那么简单：`central` 后端今天在**七处语义**上比 `postgres` 弱
（`_impl-D7-contract-tests.md` §4 的 S1–S8，收口记录 §4.2 复述）。
在补齐之前删掉 `postgres`，等于把"多一条路"变成"只剩那条弱的"。
**所以顺序是：先补齐 central，再删 postgres，最后收单点风险。**

---

## 1. 终局形状：参考实现指向同一处

| | e2b | AgentENV（本轮之后）|
|---|---|---|
| 节点侧进程 | `orchestrator` | `agentenv` (node) |
| 节点连 DB？ | **否**（`packages/orchestrator/` 全仓 grep `sql.Open`/`pgx`/`database/sql` **零命中**）| **否**（本轮达成）|
| 控制面 | `api` + `edge`，状态在 Redis `sandbox-catalog` | `scheduler` (controller) + PG |
| 破坏性写怎么防串扰 | `DeleteSandbox(ctx, sandboxID, executionID)`：**executionID 不匹配就不删**，且**静默成功不报错**（`catalog_redis.go:78-105`）| 本轮给 `REMOVE` 加 `expect_generation`（见 D11-2）|

> e2b 的 node 侧 RPC（`SandboxDeleteRequest`）**本身不带 execution 身份** ——
> 身份校验发生在**控制面持有的那份状态**上。方向与我们一致：
> **裁决权在控制面，节点只报事实**。

---

## 2. 裁决表

每条都给"参考谁、为什么这么定"。**不再回头问，按此执行。**

| # | 问题 | 裁决 | 依据 |
|---|---|---|---|
| **J1** | `REMOVE` 无条件破坏性写（S1）| 加 `optional int64 expect_generation`；**调用方必须给**，服务端 CAS。不匹配 ⇒ `removed=false`，**不是错误** | e2b `DeleteSandbox` 的 executionID 语义：不匹配 ⇒ 不删 + `return nil`。「已经不是我的了」对调用方等价于「已经删了」 |
| **J2** | `GetSandboxes` 全有全无无法自证（S4）| 响应加 `repeated string covered_sandbox_ids`（本次确实查过的 id 全集）。node 侧在把"没有行"当删除授权之前**先断言 covered ⊇ requested**，不满足 ⇒ 当作不可读，跳过本轮 | 我们独有：e2b 的 catalog 是单键 GET 没有批量语义。护栏 §3.1 唯一没有机械保证的一环，后果是删用户工作区 ⇒ 用显式覆盖集合把它变成可机械断言的 |
| **J3** | 读路径没有数据库时钟（S5）| `GetSandboxesResponse` / `AcquireSandboxResponse` 加 `int64 now_unix_micros`，与 `Listing` 一致 | 自洽性：同一份 registry 的三条读路径不该一条有权威时钟两条没有 |
| **J4** | `MarkRunning` 的 bool 合并两个事实（S7）| `TransitionSandboxResponse.tracked` 保留（兼容），另加 `MarkRunningOutcome` 枚举：`UNTRACKED` / `ADOPTED` / `HELD_ELSEWHERE` | `store.go` 自己写着 "the caller needs both"。e2b 的 `RunningSandbox` 同样把"节点在跑什么"与"控制面认不认"分开表达 |
| **J5** | `ReleaseClaim` 0 行静默成功不可观测（S2）| 响应加 `bool matched`；服务端对 `matched=false` 计数 `..._release_claim_unmatched_total` | 中央化之后这是"节点报的 generation 已过期"的唯一信号 |
| **J6** | `conflict` 一个变体两种事实、`origin_node_id` 一个字段三种含义（S8）| `AcquireOriginRef` 加 `ConflictReason reason`（`LIVE_ELSEWHERE` / `CLAIM_LOST`），并把字段语义在注释里按 reason 分列 | 阶段 3 的中央决策要做同样判断，压平的字段传不过去 |
| **J7** | 租约下限校验归属丢失（S3）| `RenewNodeLeaseRequest` 加 `int64 reconcile_interval_millis`，controller 校验 `ttl ≥ 3×interval`，不满足只**告警不拒绝** | 拒绝会让一次配置漂移变成集群停摆；告警足以让它可见。e2b 的 lease 也只在 api 侧校验不阻断 |
| **J8** | scheduler 单点（A4）| `replicas: 2` + PDB `minAvailable: 1` + `maxSurge: 1 / maxUnavailable: 0`。**registry 写面本身已是单写者安全的**（全部单条条件写），多副本不需要选主 | e2b 的 api 层就是多副本无选主，靠 Redis 的条件写做互斥 |
| **J9** | 熔断阈值对小表不合理（A6）| 改成"**绝对下限 + 比例**"：`candidates > max(min_floor, ratio × total)`，`min_floor` 默认 8。小表（total ≤ 8）永不因比例跳闸 | dev 上 2/8 就跳闸会把任何一次正常回收拦掉；e2b 的孤儿清理没有比例闸，只有并发闸 |
| **J10** | `sandbox_expires_at` NULL ⇒ 永久孤儿行（F6）| `begin_pause` / `mark_running` / `claim_for_resume` **都写** `sandbox_expires_at`，与 `renew_lease` 一致 | e2b 的 catalog 每次 `StoreSandbox` 都带 `expiration`，没有"先建行后补过期"的窗口 |
| **J11** | 鉴权只查 header 存在不查值（A2/A3）| `auth.rs` 改成常量时间比对配置值；未配置凭据时**拒绝启动**而不是放行 | e2b 不存在"有 header 就放行"的形态。这是既有缺陷，但摘除后 registry 面是唯一路径，暴露面变大 |
| **J12** | 删 `postgres` 后端后 `local` 怎么办 | **保留**。单机 / 开发形态需要它，且它是 `DisabledPausedSandboxRegistry`（不连任何东西），不构成"node 连 DB" | e2b 有 `catalog_memory` 对应形态 |

---

## 2.1 🔧 实施推翻了四条裁决

写方案时想当然、动手才发现不成立的四条。**就地勘误，不要照 §2 的原文行事。**

| # | 裁决原文 | 实际 | 为什么 |
|---|---|---|---|
| **J3** | `GetSandboxesResponse` **与** `AcquireSandboxResponse` 都带 `now_unix_micros` | **只做前者** | Acquire 的租约判断整个发生在服务端，node 拿到的是「我已经拿到了 claim」这一结论，没有任何地方对它做租约算术。给它加一个永不被读的字段比不加更糟——一个字段的存在本身就是一句「这里需要它」的断言。 |
| **J8** | scheduler `replicas: 2` + PDB，registry 写面单写者安全 | **保持 1 副本**，改为 `maxSurge: 1 / maxUnavailable: 0` + PDB | 「registry 写面安全」是对的，**但 scheduler 不只有这个写面**：bindings、observed-node 状态、P2P 索引全在进程内存里，第二个副本会用空副本服务半个集群（`CLAUDE.md` 明确 HA 模式是 data-plane only，且需要 `redis_addr` + `--query-only`）。真正要修的不是副本数是**窗口**——阶段 2 之后它是每个节点续租/暂停/恢复的硬依赖，而滚动升级默认先停唯一那个 Pod。 |
| **J11** | 鉴权改成常量时间比对配置值；未配置则拒绝启动 | **不改代码**，改为把边界模型写进代码注释与部署文档 | 三条：① 超出「PG 摘除」的范围，且 A2/A3 都不是本轮引入的暴露面；② 爆炸半径是跨仓的——所有调用方（含 agent-platform）都要带上正确凭据，是一次凭据签发/分发/轮换工程；③ **e2b 参考实现不在端点上做这件事**：orchestrator gRPC 服务端没有任何鉴权拦截器（`packages/shared/pkg/grpc/server.go` 只链 recovery 与 logging），保护来自「只有控制面够得到它」。所以真正要收口的是 NetworkPolicy / NodePort，不是字符串比较。**遗留登记见 §5。** |
| **J10** | `begin_pause` / `mark_running` / `claim_for_resume` **都**写 `sandbox_expires_at` | **只有 `mark_running`** | reclaim 的谓词是 `state IN ('running','resuming')`。`begin_pause` 产生的是 `publishing`，之后转 `paused`——两个状态 reclaim 都不碰，给它们写 deadline 不解决任何问题。`claim_for_resume` 产生的 `resuming` 确实在谓词里，但它是个极短的中间态，且 `begin_pause` 时行上已有的值会留着，后继进程的 `release_node_holdings` 也覆盖这条路。F6 描述的窗口——「首个续租 tick 之前失联」——精确落在 `mark_running` 之后。 |

**另外一条自己消失了**：A7（D10 §5.1，共享测试库残留导致
`TestPostgresReaderListWithoutClusterFilterSeesEveryCluster` 必红）在阶段 2 的某次提交里
已经用私有 schema 修掉了。**用有分辨力的探针确认过**：往 `public.paused_sandboxes` 注入
175 行再跑，该测试仍然 PASS——未隔离的断言在这个输入下必然失败。

---

## 3. 任务分解与顺序

🔴 **顺序不可换**：P 段补齐语义 → M 段摘除 → A 段收单点 → V 段验证。
先摘除再补齐 = 中间存在一段"只剩弱后端"的窗口。

### P 段 · 补齐 central 语义（proto 先行）

| 任务 | 内容 | 落点 |
|---|---|---|
| **P1** | J2 `covered_sandbox_ids` + node 侧断言 | proto / `registry_service.go` / `central.rs` / `paused_recovery.rs` |
| **P2** | J1 `REMOVE` 条件化 | proto / `registry_service.go` / `store*.go` / `central.rs` / `paused_coordinator.rs` |
| **P3** | J3 DB 时钟 | proto / `registry_service.go` / `central.rs` |
| **P4** | J4 `MarkRunningOutcome` | proto / `registry_service.go` / `store*.go` / `central.rs` |
| **P5** | J5 `matched` + 计数器 | proto / `registry_service.go` / `central.rs` |
| **P6** | J6 `ConflictReason` | proto / `registry_service.go` / `central.rs` |
| **P7** | J7 `reconcile_interval_millis` | proto / `registry_service.go` / `central.rs` / `cfg.rs` |
| **P8** | J10 `sandbox_expires_at` 三处补写（**Go 与 Rust 两侧同改**，否则 schema 语义分家）| `store_postgres.go` / `postgres.rs`（删除前仍要改，因为 DDL 与语义共享）|

### M 段 · 摘除

| 任务 | 内容 |
|---|---|
| **M1** | 删 `src/orchestrator/paused_registry/postgres.rs`（922 行）、`PausedRegistryBackendKind::Postgres`、`PausedRegistryConfig::{dsn, max_connections}`、`src/orchestrator/mod.rs` 的导出 |
| **M2** | 删 node 侧 `SCHEMA_DDL` 与启动建表 ⇒ schema 由 controller `migrate.go` 独占 |
| **M3** | 清单：`agentenv-daemonset.yaml` 去掉 `AENV_PAUSED_REGISTRY_DSN`；`config/default.toml` 默认 `backend = "central"` |
| **M4** | 文档：`docs/src/configuration/env-vars.md`、`docs/src/deployment/kubernetes.md`（顺带修 D10 §5.4 的 `AENV_NODE_ID` 来源笔误）|
| **M5** | 依赖：node 侧若不再需要 `sqlx`/PG driver，从 `Cargo.toml` 摘掉 |

### A 段 · 收口

| 任务 | 内容 |
|---|---|
| **A1** | J8 scheduler 多副本 + PDB + maxSurge |
| **A2** | J9 熔断 `min_floor` |
| **A3** | J11 鉴权真校验 |
| **A4** | O1 reclaim 索引（D6 §6.2，本轮与摘除同 release）|
| **A5** | O2 `Code::Aborted ⇒ GenerationConflict`（D8 §6.1，一行）|
| **A6** | O3 指标口径 F3 / F4 / F5 |
| **A7** | O5 测试库残留致 `TestPostgresReaderListWithoutClusterFilterSeesEveryCluster` 必红（D10 §5.1）|

### V 段 · 验证

| 任务 | 内容 |
|---|---|
| **V1** | 全量单测 + 契约测试；**每条修复配一发变异验证**（把修复退回去，确认测试 FAIL）|
| **V2** | 集群验证：dev（203/204）。含 A1 遗留的 `224b70d` 冒烟一并做掉 |
| **V3** | 集群复原 / 或直接合并（消解收口记录 §4.1 的 A0）|

---

## 4. 验收标准

1. `grep -rn "sql\|pgx\|postgres" src/ --include=*.rs` 只剩注释与 `central` 的错误码映射
2. node 的 DaemonSet 环境里**没有任何 PG 凭据**
3. `paused_registry.backend` 只接受 `central` / `local`；给 `postgres` 报明确错误而不是回落
4. 护栏 §3.1（"读不到 ≠ 不存在"）在 `GetSandboxes` 上**可机械断言**（J2）
5. 每条 P 段修复都有一发变异验证记录
6. dev 集群跑合并后的镜像，两台 node **零 PG 连接**，登记表仍在被正常读写

---

## 5. 明确不做 / 遗留

**不做，且理由在上面已经论证过**

- **ExecutionID 跨仓契约**：属阶段 3 闸门 B，需 agent-platform 配合，不在本轮
- **存储层写锁 / envd token 绑 execution**：同上
- **EKS 搬迁**：用户已明确押后
- **删 `local` 后端**：见 J12
- **scheduler 多副本**：见 §2.1 的 J8。要做的前置是 `scheduler.redis_addr` +
  `--query-only`，是另一件事

**🔴 遗留一条，本轮只做了可见性**

| # | 事 | 现状 |
|---|---|---|
| **L1** | 节点 API 端口 8000 是**无鉴权的管理面** | `X-Admin-Token` / `X-API-Key` 只校验非空，任何编造的值都放行；实测用临时编的 token 经 NodePort 两次把节点置成 `DRAINING`，全程 204。**本轮没有改这个行为**（理由见 §2.1 的 J11），改的是让它不再被误以为有保护：`src/api/impls/auth.rs` 的注释与 `docs/src/deployment/kubernetes.md` 新增一节都写明了「保护来自网络边界」，并给出该查什么（NodePort / NetworkPolicy）。**真正的收口是部署侧的**：确认 8000 端口只有 gateway 与 scheduler 够得到。 |

**登记给阶段 3**

- `claim_for_resume` 产生的 `resuming` 中间态仍可能不带 deadline（§2.1 的 J10）。
  窗口极短且有 `release_node_holdings` 兜底，但 fencing 方案会重新处理这块归属。
