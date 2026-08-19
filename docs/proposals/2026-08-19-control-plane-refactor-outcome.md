# 中央控制面重构：阶段 0 / 1 / 2 收口

> 2026-08-19 · **写给三个月后回来接着做的人**。
> 权威方案（含实施订正）：[`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md)
> 背景与三家架构对照：[`2026-08-19-aenv-central-control-plane.md`](2026-08-19-aenv-central-control-plane.md)
>
> 本文能独立读懂：需要细节时再按文末索引去翻过程文档，不必先读它们。

---

## 0. 三十秒版

把 `scheduler` 从"无状态选节点器"升级成 **controller**（唯一持 PG 的进程、唯一的状态裁判），
node 退回纯执行器。**顺序是反的**：先建观测面，再收读路径，最后才动写路径 ——
每一步单独上线、单独回退、单独兑现价值。

| 阶段 | 做了什么 | 状态 | 分支 / 提交 |
|---|---|---|---|
| **0** 中央影子对账 | controller 加只读 PG，产出差异指标 + 只读 API | ✅ 实施 + 集群验证 | `central-control-plane-phase01`：`7e6f790` `c35f5ec` |
| **1** 读路径上收 | `LookupNode` 回落读登记表，gateway 删掉三段补丁 | ✅ 实施 + 集群验证 | 同上（两阶段同批交付） |
| **2** PG 写权上收 | node 不再直连 PG，登记表读写全经 gRPC 打到 controller | ✅ 实施 + 集群验证 | `central-control-plane-phase2`：`4a5e3ef` `339d1f2` `40a4526` `224b70d` |
| **3** 语义 + 裁决权 | 中央 poll / 孤儿回收 / ExecutionID / 中央 placement | ⛔ **卡在闸门 B** | 见 §3 |
| **4** API 收窄 | node 的用户级 REST 只接受 controller 调用 | ⛔ 未开始 | 可拖到最后 |

**🚦 闸门 A 已通过**（2026-08-19 用户裁决）：长期自维护 fork，手动从上游拉取检查合并。
⇒ 上游追平成本不再是约束，允许改动 Rust 主干。

**🚦 闸门 B 未过**，它是阶段 3 的硬前置：选定 fencing 方案 + 与 agent-platform 敲定
ExecutionID 跨仓契约。**在此之前 `running` / `resuming` 永不可抢这条不许放宽。**
完整决策材料见 §3。

**🔴 三件事现在就要有人决定**，见 §4.1 的 A0 / A1 / A6。

---

## 1. 三个阶段各自做了什么

### 1.1 阶段 0：中央影子对账（只观测，不裁决）

controller 加一条**只读** PG 连接（在数据库层就拒写，不是靠代码评审），
每 30s 用「heartbeat roster × 登记表」对一次账，把差异写成 Prometheus 指标；
另外开一个只读 API（`GET /registry/sandboxes`），让 Agent-Console 不再需要 PG 直连。

**它为什么必须是第一步**：阶段 2/3 的全部收益在动手前都是**估计值**。
没有这组数据，"中央裁决能消灭多少不一致"是论证，不是事实。

**方案原本给的四个指标口径，落地时被推翻了三个半**（详见方案 §4 阶段 0 的订正块）：
`orphan` 算不出来（`mark_running` 从不建行 ⇒ roster∖registry 是超集，健康集群恒非零）⇒ 改名
`untracked`；`holder_conflict` 健康集群也非零 ⇒ 拆出 `stale_copy`；`lease_expiring` 混了两件事
⇒ 拆成 `parked_lease_expiring` / `live_lease_lapsed` / `reclaimable_now`；`sync_ok` 本阶段
根本不可得（heartbeat 是单向推）⇒ 只上报 `roster_stale`。
另补了 `invalid_rows` / `stranded_rows` / `rows_without_roster` / `registry_enabled`。

**这一步的可回退性是配置级的**：`SCHEDULER_REGISTRY_DSN` 置空 ⇒ 完全回到改造前的行为，
且「关了」与「坏了」在监控上分得开（`registry_enabled` + `read_failures_total` +
`last_success_timestamp_seconds` 三件套）。

### 1.2 阶段 1：读路径上收

`LookupNode` 在 binding 未命中时**回落读登记表**，把答案分成四类
（`BOUND` / `PLACED` / `PINNED` / 无行）而不是一个裸的 node id：

- `paused`（快照已发布）⇒ placement 结果，**origin 作软偏好**，origin 不可接单就换一台
- `publishing` / `local_only` ⇒ **硬钉 origin**（快照没进共享存储，别处起不来）
- `running` / `resuming` ⇒ 持有者节点
- 无行 ⇒ 404，**但只有在 controller 明确答得出来时**；registry 不可达一律 503

gateway 随之**删掉了三段补丁**：`scheduleRecoveryNode`（挑一台让它去抢）、
`captureReplayBody`（提前缓冲请求体准备重放）、`rerouteToScheduledNode`（被拒了重放到另一台）。
同一件事现在是一次决策，无试错、无重放、无请求体缓冲。

顺带修掉一个现成的性能缺陷：跨节点 resume 此前用空 hint 调 `Schedule`，
**主动丢弃了 origin 亲和性**，必然走 OSS 全量拉层（EKS 上实测比同节点复用差 15–30x）。

**侦察在开工前推翻了方案的三处**（方案 §4 阶段 1 的订正块）：
① 「查 roster 兜底」当时**没有数据源**（`ObservedNode` 无 per-sandbox 列表，heartbeat 的
`sandbox_ids` 写进 BindingStore 后即丢弃）⇒ 阶段 0 顺带把 roster 留存下来；
② `--query-only` 副本走的是另一条 gRPC 连接，registry reader 不装到 `QueryOnlyService` 上
新逻辑一次都不生效，而当时的 k8s 清单恰好没配这个地址、**本地测不出来**；
③ 删掉 reroute 后 Rust 侧的 503 会直通客户端 ⇒ 改成 controller 先判 origin 可调度性，一次答完。

### 1.3 阶段 2：PG 写权上收

node 不再直连 PG。登记表的**全部读写**经 gRPC 打到 controller，由 controller 独占 PG 与 schema。

**RPC 面按阶段 3 的目标形状设计，不做 13 方法 1:1** —— 1:1 会把"节点是决策者"
从实现细节升格成跨进程网络契约，而阶段 3 第一件事就是删掉这个契约。
13 个 trait 方法收敛成 **5 个 RPC**（`service PausedRegistry`，
`services/api/proto/scheduler.proto`）：`GetSandboxes` / `TransitionSandbox` /
`AcquireSandbox` / `RenewNodeLease` / `ReleaseNodeHoldings`。
`reclaim_expired_holdings` **不进 RPC 面** —— 它从阶段 2 起由 controller 自己按定时器跑。

**兑现的**：G7（PG 凭据不再下发到每台跑用户代码的 KVM 机器，爆炸半径 N 台 → 1 处 ——
这是整个重构最硬的那条理由）、G8（PG 连接数恒定）、G6（schema 有唯一 owner）、
controller 重启不丢 registry。
**明确不兑现**：G1 / G2 / G4 —— 语义仍是**节点视角**，只是换了条路访问 PG。

**五条护栏全部落地并在真集群上验过**（第五条是本轮新增的，见 §2.2）。

### 1.4 代码在哪、验证到什么程度、有没有合并

| | 阶段 0 + 1 | 阶段 2 | 阶段 2.5（摘除）|
|---|---|---|---|
| 提交 | `7e6f790` + `c35f5ec` | `4a5e3ef` `339d1f2` `40a4526` `224b70d` | `82abc95` … `c7797ff` |
| 集群验证 | `_verify-T1-results.md` + `_verify-T2-final.md` | `_verify-T3-phase2.md` | 见 §1.5 |
| **合并状态** | 🟢 **已并入 `origin/dev`**（HEAD `c7797ff`）| 🟢 同上 | 🟢 同上 |

**T3 交付时那两处落差都已消解**：

1. ~~`224b70d` 没有经过集群验证~~ —— D11 的集群验证跑的是含它在内的完整分支镜像
   （`d11-9a8fd88`）。
2. ~~过程文档没有进版本库~~ —— `18c3571` 已把整个 `docs/proposals/` 提交。

三份验证报告的合并结论都是「能合并」，判据分别见 T1 §8 / T2 §6 / T3 §8；
摘除那一轮的判据见 §1.5。

### 1.5 集群现状（pve-sg dev，203/204）

**跑的是已合并的 `dev`**（镜像 `d11-9a8fd88`，两台 node + scheduler 同批）。
T3 交付时那 4 项"未复原的未合并代码"随合并一起消失了——A0 不再存在。

摘除那一轮在这个集群上验的九件事：

| 探针 | 判据 | 结果 |
|---|---|---|
| **迁移拒绝**（对照探针）| 新 node 镜像配 `backend=postgres` | ✅ 拒绝启动，exit 1，日志逐字给出 `central` 的迁移指引 |
| **G7 兑现**（否定证据）| `pg_stat_activity` 按 `client_addr` 分组 | ✅ 4 条连接**全部**来自 scheduler 的 Pod IP，两台 node **各 0 条** |
| **没有静默回落 local**（正面反证）| 装配日志 + RPC 计数 | ✅ 两台都打 `backend="central"` + endpoint；`GetSandboxes` 7 / `TransitionSandbox` 5 / `AcquireSandbox` 1 / `RenewNodeLease` 4 / `ReleaseNodeHoldings` 2 全在涨 |
| **migration 扩展生效** | `pg_indexes` | ✅ `paused_sandboxes_reclaim_idx` 已建，`WHERE state IN (…) AND sandbox_expires_at IS NOT NULL` 部分索引 |
| **端到端** | 建 → pause → resume → 删 | ✅ 全通（201 / 204 / 201 / 204）|
| **J10 deadline** | resume 后立刻查行 | ✅ `running` 行带 `sandbox_expires_at`（第一次续租之前），`paused` 态为 NULL——与设计精确对应 |
| **J2 覆盖断言无误伤** | node 日志 `off contract` 计数 | ✅ 两台各 0 |
| **F4 指标分来源** | `grace_refusals_total` | ✅ `{source="reclaim"} 3`，`rpc` 一次都没有——正是 T3 F4 预测的"健康重启的自噪声"，现在可以和真实拒绝分开告警 |
| **存量行不受伤** | 老行 `01a01853` | ✅ `local_only` / gen 3 / origin 不变 |

> `01a01853` 仍是 `local_only`——那是 [`aenv-pause-publish-durability`](2026-08-19-aenv-pause-publish-durability.md)
> 要修的东西，不在本轮范围内。

---

## 2. 实施过程中被改写的三条结论

这三条不是细节，是**下次做类似事情时会再撞上的东西**。

### 2.1 「查 roster 兜底」是一句写不出来的话

方案 §4 阶段 1 要求"回落读必须同时查一次 `ListObservedNodes` 的 roster"。
写这句话的时候**没有这个数据源** —— 那份 roster 从来没被留存过。
教训：写方案时引用的每一个数据源，都要在动手前确认它真的存在，
而不是"名字听起来像有"。（`_impl-plan-control-plane-phase01.md` §1.2）

### 2.2 四条护栏漏了反方向，补成五条

方案 §3 的四条全部针对同一个方向：**别把"我不知道"当成"不存在"**。
但 `arbitrate_resume` 是**反方向**的：registry 报错 ⇒ `Proceed`，放行，不做任何集群检查。
而「`running`/`resuming` 永不可抢」这条不变式，实现层的最后一道闸就是
`claim_for_resume` 返回 `Conflict` —— registry 一不可达这道闸整个消失，
**同一次故障还会让 `discard_if_superseded` 静默不删**，两道防线同源同时失效，后果是双活。

阶段 2 之前它不严重（= node 到集群内 PG 的连接故障，发生率极低）；
阶段 2 之后它 = node 到 scheduler 的 gRPC 故障，而 scheduler `replicas: 1`、无 PDB、
无 `maxSurge` —— **滚动升级、拉镜像、OOM、驱逐都会制造窗口**。

⇒ 补成护栏 §3.5，采用**收窄版**：只有"本地有 paused 记录**且**该记录曾登记到集群"
才拒绝放行，从未上过集群的沙箱保持原行为。
（`_recon-R4-rust-client.md` §7 风险 1 → `_impl-plan-control-plane-phase2.md` §1）

### 2.3 侦察产物自己也会错，而且是安静地错

两份侦察报告各被实施推翻一处，两处都已就地勘误：

- `_recon-R2-registry-spec.md` §0：「13 个方法中有 **4** 个带 generation CAS」——
  **实际 3 个**（`complete_pause` / `mark_local_only` / `release_claim`，HEAD `224b70d` 上在
  `postgres.rs:416` / `:447` / `:771`）。`claim_for_resume` 与 `mark_running` 的谓词
  只在 state 与 holder 上。同文 §3.4 的表格本身列的就是 3 个 —— 错的只有速览那句，
  而 proto 注释按速览抄成了"四个"，一路传下去。
- `_recon-R4-rust-client.md` §3.2：「Cargo.toml 没开 `preserve_order`，键序已排序」——
  **`storage/overlaybd/Cargo.toml:28` 开了**，Cargo 的 feature 统一让 `agentenv` 也吃到，
  于是 `serde_json::Map` 在本 workspace 是**插入序**，`HashMap` 字段每进程随机的迭代序
  会直接漏进 JSON。不处理的话 golden fixture 每次跑都不一样。
  （结论"键序不是契约"仍成立，错的是它成立的机理 —— 这种错最难发现。）

---

## 3. 🚦 闸门 B 的决策材料

阶段 3 有两件前置，缺一件都不能开工。

### 3.1 前置一：选定 fencing 方案

**为什么必须选**：中央 poll 把"够不到 PG"换成"够不到 controller"，**仍然是推断**。
controller ↔ node 网络分区时，node 活得好好的、还在被 gateway 直连路由数据面流量。
e2b 敢直接裁决，是因为它接受泄漏（孤儿等节点回来再杀）+ 沙箱是一次性的；
**我们的沙箱是用户工作区，丢了就是事故**。

⇒ **真正消灭双活要在数据面 fencing，控制面做不到。**

#### 候选 1：存储层写锁

**机制**：overlaybd / 块设备层持有排他写租约，第二份实例根本起不来。

**落点与代价**：`storage/ublk-daemon` 的 `UblkDeviceManager` 是 process-wide singleton，
但它只在**本机**范围内互斥 —— 跨节点双活是两台机器各自的 daemon，谁也不知道对方。
真正的排他必须落在**共享存储那一层**（OSS / rustfs 后端，或 POSIX 共享目录），
而对象存储没有租约原语，要自己用条件写 / 租约对象做出来。
而 rustfs 在这条路上的并发行为已知不好（并发 multipart ≥3 必 503）。

**判断**：最彻底，但成本最高，且踩在本轮已知最不稳的组件上。**列为长期项，不作为闸门 B 的答案。**

#### 候选 2：envd token 绑 execution

**机制**：每次化身换 token，旧 execution 的 envd 请求一律拒。

**现状（已核对代码）**：`SandboxAccessTokenGenerator::generate(sandbox_id)` =
`HMAC(seed, sandbox_id)`（`src/sandbox/access.rs`）—— **只跟 sandbox_id 有关，与执行化身无关**；
且只在 `metadata.secure` 为真时启用（`orchestrator/service.rs:791`）；
seed 来自 `[sandbox].access_token_hash_seed`，没配就用节点本地
`{home_path}/secrets/sandbox-access-token-hash-seed`。

**改法**：派生输入改成 `(sandbox_id, execution_id)`，旧 execution 的请求验签必然失败。

**代价与洞**：
1. **只对 `secure` 沙箱有效** —— 非 secure 的一路完全不受保护，这不是"覆盖率低"，是"有一半没锁"；
2. seed 若是节点本地的，两台节点根本推不出同一个 token，**必须先把 seed 统一成集群级**，
   这是一次部署变更，且做错了是静默失效；
3. token 由平台侧持有并注入，换 token 意味着调用方要能**重新取到**新值；
4. **杀不掉已在跑的进程**，只切断它的对外影响。

**判断**：中等成本，但覆盖面有洞、依赖一次 seed 统一。**不作为唯一手段。**

#### 候选 3：路由层拒旧 execution 　← **推荐**

**机制**：gateway / proxy 按 ExecutionID 路由，旧化身收到的流量归零。

**为什么推荐**：
1. **它与 ExecutionID 是同一批工作**，几乎零额外成本 —— 而 ExecutionID 本来就是阶段 3
   要引入的东西（破坏性操作带 `expect_execution_id`）；
2. **aenv 内部已经有半个了**：`SandboxInstanceId`（`src/sandbox/custom_extension/client.rs`）
   每次 start / resume 生成一个新的，代码注释写的理由就是
   "sandbox ids are reused across pause/resume，扩展要能忽略过期实例的乱序 stop 通知"。
   它今天只流向 custom extension hook，**不进登记表、不进 API** —— 把它接出来即可；
3. 它把损失的性质改了：从"两份 rootfs 分叉后无法合并"（不可逆）
   降成"一份在跑但没人理"（可回收的资源浪费）。

**🔴 但它有一个残留风险必须配套堵住**：VM 还在跑，就还在写自己的 overlaybd 上层。
那台机器如果随后把这台沙箱 pause 并发布快照，就会产生**第二条快照分支** ——
用户视角的单活被破坏，只是晚了一步。
⇒ 选候选 3 就必须同时做：**旧 execution 的 pause / publish 一律拒绝**。
这在阶段 2 已有的 `TransitionSandbox` 上加一个 `expect_execution_id` 就能做，成本很低。

#### 建议

**候选 3 + "旧 execution 不许 pause/publish"** 作为闸门 B 的答案；
候选 2 作为 `secure` 沙箱的加强项，在 seed 统一之后再补；
候选 1 列长期项，不阻塞阶段 3。

### 3.2 前置二：跨仓 ExecutionID 契约

🔴 **"改造只在 AgentENV 内部完成"这个裁决前提，从阶段 3 起就不成立了**（方案 §5.3）。
这不是意外，是必须提前接受的跨仓协同点。

**aenv 侧要出的**：在外部契约上**增加**一个 execution 身份字段。
注意这是我们**加**的，不是抄 e2b 的 —— e2b 的公开 API（`spec/openapi.yml`）
**零处**暴露 execution id，它只活在 `sandbox-catalog` 这个内部结构里。
作为附加字段不破坏 e2b SDK 兼容。落点是 `GET /sandboxes/{id}` 与
`POST /sandboxes/{id}/resume` 的响应体。

**agent-platform 侧要配合的三件**：

1. **存**：沙箱记录上多一列存 execution id，每次 resume 之后更新；
2. **带**：破坏性操作（delete / pause / 写文件）把它带上，让 aenv 有东西可以 fence；
3. **换掉启发式**：今天平台侧靠一套"只信 resume 的 404、不信 Get 的 404"的分层判据
   （`apps/agent-platform/internal/sandbox/aenv/client.go:58-71` 与
   `service.go:464-490`，注释里写了 404 的三个来源）。有了 execution 身份之后
   这套启发式可以退役。

**时序**：aenv 先出字段（向后兼容，平台不读也不坏），平台再消费。
**不要**等平台改完再发 aenv —— 那会把一次单向兼容变更变成一次双向同步发布。

**一条现在就该重新取样的事**：阶段 1 已经**部分**改善了那三个 404 来源
——「registry 不可达」现在答 **503 而不是 404**（T1 V1-1 实测），
「binding miss」也被 roster + 登记表回落覆盖掉了大部分。
⇒ 平台侧那段注释描述的是阶段 1 上线**之前**的行为。阶段 0/1 合并上线后应该重新取一次样，
再决定 G3 剩下多少价值 —— 有可能它比原表述小得多。

---

## 4. 遗留项清单

### 4.0 ✅ 已处置（阶段 2.5，见 [`_impl-D11-pg-removal.md`](_impl-D11-pg-removal.md)）

| # | 结局 |
|---|---|
| **A0** dev 集群上未合并代码持有 DELETE 权 | **消失**：分支已合并进 `origin/dev`，集群跑的就是它 |
| **A1** `224b70d` 从未在集群上跑过 | **已跑**：D11 的集群验证用的是含它的完整分支 |
| **A2/A3** 鉴权只查 header 存在不查值 | **不改代码**，改为写清边界模型 + 给出该查的部署项。理由：两家参考都把节点面的保护放在网络边界（e2b 的 orchestrator gRPC 服务端零鉴权拦截器），改成真凭据是一次跨仓的签发/分发/轮换工程。**遗留登记见 D11 §5 的 L1** |
| **A4** scheduler 单点 | **更正后处置**：不加副本（bindings / observed-node / P2P 索引都在进程内存里），改为 `maxSurge:1 / maxUnavailable:0` + PDB，把"缺席窗口"从滚动升级的默认行为里拿掉 |
| **A5** 生产 Secret 有没有 `cluster_id` | `224b70d` 已把值搬进 base 层 `configMapGenerator`；**生产集群的实际状态仍需单独确认** |
| **A6** 熔断阈值对小表不合理 | **已修**：比例臂加了绝对下限，小表不再因百分比跳闸 |
| S1 / S2 / S4 / S5 / S7 / S8 | **已修**（D11 的 J1–J6），每条配变异验证 |
| S3 | **已修**：节点上报 `reconcile_interval`，controller 校验但不拒绝 |
| D6 §6.2 reclaim 全表扫 | **已修**：部分索引，随摘除同 release |
| D8 §6.1 `GenerationConflict` 表达不出来 | **已修**：`Aborted` ⇒ `GenerationConflict` |
| T3 F3 / F4 / F5 | **已修**：指标改名 / 加 `source` 标签 / HELP 说清它不是进程停机时长 |
| T3 F6 `sandbox_expires_at` 永久孤儿行 | **已修**：`mark_running` 写 deadline（落点比原方案收窄，理由见 D11 §2.1）|
| D10 §5.1 共享测试库致必红 | **早已修**：私有 schema。用注入 175 行的对照探针确认过 |
| T2 N3 `invalid_rows` 没有分辨力 | 仍未造出非零样本 |

### 4.1 🔴 上生产前必须有结论

> 以下为**原始清单**，现状见 §4.0。

| # | 事 | 出处 | 说明 |
|---|---|---|---|
| **A0** | **dev 集群上未合并代码正持有 DELETE 权** | T3 §6.1 | scheduler 跑着 `cp2-40a4526` 且写面开着，reclaim 定时器每 30s 会 DELETE 行。要么把分支合了，要么按 T3 §6.1 的四条命令复原 |
| **A1** | **`224b70d` 从未在集群上跑过** | D10 抬头 | T3 验的是 `40a4526`。合并前至少补一轮冒烟 |
| **A2** | `POST /nodes/{id}` 是**写操作**，却只查 header 存在不查值 | T2 §N2 | 实测坐实：用临时编的 `X-Admin-Token`、经对宿主机开放的 NodePort **两次**把节点置成 DRAINING，全程 204。根因在 `src/api/impls/auth.rs:19` 的 `TODO: Validate configured authentication credentials instead of only checking that they are present.`。**既有问题，非本轮引入**，但优先级高于 A3 |
| **A3** | `/registry/sandboxes` 的 `X-API-Key` 只是**一道门**不是一把锁 | T1 §6 F1 / T2 §N1 | 任意非空值放行。本轮新增端点把「全集群沙箱 ID + 归属节点 + 租约时间」加进了这个面。要么给这组补真凭据校验，要么确认 gateway 只在内网可达 |
| **A4** | **scheduler 是单点，阶段 2 把它从"读侧降级"提升成"写侧硬依赖"** | T3 §8 | `replicas: 1`、无 PDB、无 `maxSurge`。实测：它一停机，节点侧的续租、对账、`begin_pause`、**以及所有已登记沙箱的 resume**（按 §3.5 设计如此）全部停摆。护栏做对了它该做的事（失败可重试、不双活），但**故障窗口的宽度现在由滚动升级策略决定，而没有任何清单约束住它** |
| **A5** | 生产的 `agentenv-postgres` Secret 有没有 `cluster_id` | T3 §7 F1 | 缺了 ⇒ 写面永远 `PhaseCold` ⇒ 每个 RPC `UNAVAILABLE`，**现象与"scheduler 挂了"完全一样**，而进程健康、`/healthz` 200、探针也通过。`224b70d` 已把值搬进 base 层 `configMapGenerator`，但生产集群的实际状态要单独确认 |
| **A6** | 熔断阈值（10 行 / 10%）对生产表规模是否合理 | T3 §8 | dev 上 **2/8 就跳闸**。如果生产登记表长期只有个位数行，这个熔断会把**任何**一次正常回收都拦掉。`max_rows` 臂只有单测覆盖，没在集群上单独取样过 |

### 4.2 🟡 建议做

**部署 / 观测口径**

- T3 F3：`grace_takeovers_withheld_total` 统计的是"grace 期内服务的 claim 数"，
  **不是"真被拦下的接管数"** —— `paused` 那条臂根本不看租约，不存在"被 withheld"这回事。
  一次重启后如果有大量普通跨节点 resume，这个计数器会让运维**高估 grace 的代价**。
- T3 F4：`grace_refusals_total` 把"客户端 RPC 被拒"和"controller 自己的 reclaim 定时器被闸住"
  混在一个计数器里 ⇒ 一次完全健康、零客户端流量的重启也会稳定产出 2~3 次 refusal。加个
  `source="rpc"|"reclaim"` 标签即可。
- T3 F5：`inferred_downtime = now() - max(updated_at)`，而节点每 30s 才写一次 ⇒
  健康重启会高估最多一个对账周期（实测 14s 的重启报 20.1s）。方向是安全的（高估 ⇒ 租约多延），
  但那个数**不是进程停机时长**，值得在 HELP 里点一句。
- T3 F6：`sandbox_expires_at` **只由 `renew_lease` 写**，`begin_pause` 不写；
  而 reclaim 的两条语句都要求它 `< now()`，`NULL` 永不匹配。⇒ 一台在**首个续租周期内**
  就失联的节点会留下**永久孤儿行**。既有语义（两侧一致），登记给阶段 3 的 fencing 方案。
- T2 N3：`invalid_rows` 至今**没有分辨力** —— 全程恒 0，从没造出过非零样本。
  它恰恰是"一条坏行静默冻结一台机器全部对账"的那种行。建议单独授权插一行
  `paused` + `snapshot_id IS NULL` 验一次即删。
- T1 F7 / T2：gateway 自身 metrics 已补进 Service，但 scheduler 侧的
  `registry_reconcile_duration_seconds` 仍把"读 + 派生"当一整段量（T2 N5）。

**接口与实现**（`_impl-D7-contract-tests.md` §4 里仍然成立的那些）

D7 从 Rust 规格里读出 12 条「Go 接口表达不出来」的语义。
其中 **S0 / S6 / S9 / S10 / S11 已在实施中收口或降级**
（S0 是规格书笔误，本轮已勘误；S6 有测试钉住；S9 的熔断在服务层实现并实测；
S10 由 contract harness 机械保证 + T3 A2 集群验证；S11 由 D8 的契约①钉死）。
**仍然成立的**：

| # | 一句话 | 为什么值得做 |
|---|---|---|
| **S4** | `GetMany` 的「全有或全无」在类型上**无法自证** | 护栏 §3.1 目前**唯一**没有机械保证的一环。一次被截断的分页 / 部分流 / 丢了后半批的 chunk 循环，都表现为"这些 id 没有行"，而调用方对"没有行"的反应是**删用户工作区**。建议响应带上"本次实际覆盖的 id 集合"或 `requested_count`，让 node 侧在把缺失当成删除授权之前先断言一次 |
| **S1** | `Remove` 没有 `expect_generation`，是全接口**唯一一个无条件破坏性写** | 守卫在**调用方**（`forget_sandbox` 先 `get` 再判），是 TOCTOU 守卫，而阶段 2 的整个论点是"节点不再是决策者"。一个刚从分区回来、还没 reconcile 的陈旧节点对一台已在别处 resume 的沙箱调 `remove`，中央会照删、全程零报错 |
| **S3** | 租约下限校验的**归属**丢了（半条） | TTL 已裁决属于 node（`WithLeaseTTL`），`lease_ttl_floor` 补了粗粒度兜底（挡 0 和单位搞错）。但 controller 看不到节点的 `reconcile_interval`，`ttl ≥ 3×interval` 这条**无处校验** |
| **S5** | 读路径没有数据库时钟 | `Listing` 刻意携带 DB 的 `now()`，而 `Get`/`GetMany` 返回的 `Entry` 没有 ⇒ 拿它做租约判断只能用进程墙钟，正是文档禁止的事 |
| **S7** | `MarkRunning` 的 `bool` 合并了两个事实 | `store.go` 自己写着"the difference between 'not tracked' and 'somebody else's' — **and the caller needs both**"。Rust 侧靠"0 行时重读一次只为打 warn"补偿，中央化之后那次重读发生在 controller，**这个区分永远不会跨过网络** |
| **S2** | `ReleaseClaim` 的「0 行静默成功」变得不可观测 | 中央化之后，"一次 release 什么都没匹配到"正是"这个节点报的 generation 已过期"的唯一信号，而现在没有任何东西能把它计数 |
| **S8** | `ClaimOutcomeConflict` 一个变体承担两种事实，`OriginNodeID` 一个字段承担三种含义 | 阶段 3 的中央决策要做同样的判断，而它拿到的是同一个被压平的字段 |

**其它**

- D6 §6.2：reclaim 的两条语句是**全表扫**（没有可用索引）。没做是因为阶段 2 的硬门禁要求
  migration 第一版逐字复制 `SCHEMA_DDL`。建议和"删掉 `postgres` 后端"那个 release 一起做。
- D8 §6.1：`GenerationConflict` 在 Central 后端下**表达不出来**（Go 侧给 CAS 失败留了
  `Aborted`，Rust 侧一律翻成 `Backend`）。**今天没有行为差异**（生产代码没有任何一处按
  `PausedRegistryError` 变体 match），已裁决阶段 3 再说。要加是一行：
  `Code::Aborted ⇒ GenerationConflict`，Go 侧已经备好了。
- D10 §5.1：本机共享测试库 `aenv` 的 `paused_sandboxes` 有 180 行 Rust 集成测试残留，
  导致 `TestPostgresReaderListWithoutClusterFilterSeesEveryCluster` **必红**（非本轮引入）。
  取证方式：新建空库跑同一个包全绿。建议把那条断言改成"至少包含这 6 行"，或给它自己的库。

### 4.3 🟢 可以不做（都已论证过或刻意如此）

- **`AENV_PAUSED_REGISTRY_BACKEND=""`（显式空串）仍回落 `local` 且不报错**（D10 §5.2）：
  confique 全局约定"空串 = 未设置"，为它破例要写一个专用 `parse_env`，与全 crate 其它 env
  项行为分家。后果现在**可见**了 —— 装配日志会打 `backend=local`，那正是 `224b70d` 解决的分辨力。
- **scheduler 侧 cluster id 仍是 `optional: true`**（D10 §5.3）：刻意。部署时的分辨力由
  CI 清单测试 + `/healthz` 承担，而不是靠让 Pod 起不来。要改等于推翻 D6 §5.2，应另开裁决。
- **写面缺 cluster id 是"注册但留冷"而不是 Fatal**（D6 §5.2）：让配置校验失败 ⇒
  scheduler CrashLoop ⇒ 路由、发现、binding 全停 —— 用一个刚打开的子系统换掉整个集群的数据面。
  爆炸半径搞反了。现在的行为是进程正常启动、RPC 一律 `UNAVAILABLE`（可重试语义），
  `/healthz` 报 `phase: cold`（不是 `off` —— 它被配置开了，报 off 是撒谎）。
- **`get_many` 仍把 metadata 读回控制面**（D6 §6.3）：它**不上线**（`GetSandboxes` 的 proto
  里没有 metadata 字段），与今天的 Rust 完全同量，不是回归。要优化得动冻结接口。
- **一个 controller 只服务一个集群**（D6 §6.5）：proto 形状允许多集群，实现按单集群 fail-closed。
  放开会让 grace 期的正确性依赖"发现所有 cluster"这件事本身不出错。不建议在阶段 2 做。
- **`local_only` 沙箱在 scheduler 重启后有 ~5s 的 503 窗口**（T1 F6）：`schedulableNode`
  刻意的 fail-closed，503 可重试。对比之下 `paused` 沙箱在同一窗口是 fail-open 的
  （照样 PLACED 成功）—— 两条路一开一闭是设计选择，不是缺陷。
- **cluster id 没有 apply 期覆盖钩子**（D10 §5.5）：故意不加。多一个覆盖入口就多一处能和
  `config/default.toml` 的回落值分家的地方。
- `docs/src/deployment/kubernetes.md` 里 `AENV_NODE_ID` 的来源写错（写成 `metadata.name`，
  实际是 `fieldRef: spec.nodeName`）—— 文档小错，顺手改（D10 §5.4）。

---

## 5. 这轮在方法论上有效的做法

下次做同类改造（跨语言、跨进程、动用户数据）时值得照搬的四条。

### 5.1 契约测试与实现分离，且**测试作者不读实现**

阶段 2 的核心风险是"942 行 Rust 翻成 Go，边界条件必须逐条对齐"。做法是：
**先 proto，再契约测试，最后实现**，且写测试的那一棒**只从 Rust 规格推导期望值**，
不看 Go 实现（唯一一次接触实现文件被显式披露在 `_impl-D7-contract-tests.md` §0.2）。

价值不只是抓 bug —— 它逼出了 **12 条"Rust 语义在 Go 接口上表达不出来"**（S0–S11，见 §4.2）。
这些不是测试能覆盖的东西，是**接口本身的缺口**，而只有"照着规格写测试却发现无处挂载"
才会把它们显形。

### 5.2 每条修复配一发**变异验证**

标准是：把修复/实现退回去，确认测试真的 FAIL。全程累计
D3 的 23 发、D6 的 82 条、D8 的 37 发、D10 的 10 条 —— **全部有牙**。

它的价值在于抓"假绿"：审查阶段就用变异实测揪出 3 个看着通过、实际什么都没断言的测试
（`_review-V1-phase0.md` #6 / #7 / #8）；D8 发现 `ResumeArbitration::Unavailable`
的 HTTP 状态码**此前零覆盖**（把 500 改成 404，测试全绿存活）—— 而 404 在下游
agent-platform 是"授权重建工作区"的意思。

### 5.3 探针必须**先自证**，否则"没有分辨力"会伪装成"护栏生效"

这轮有一次真实的翻车，值得记住：验 grace 期"拒绝接管"时，第一发探针用的是一行
`local_only` + `snapshot_id IS NULL` 的合成行 —— grace 期内它被答 409，**看着像被拒了**。
但 grace 结束后**同一发请求得到一模一样的答复**。读 SQL 才发现两条认领臂都带
`AND snapshot_id IS NOT NULL`，那行**任何相位都认领不了**。探针作废，重做。
（`_verify-T3-phase2.md` §4 C2 如实登记了这一次。）

有分辨力的探针长什么样，本轮有三个样板：

- **A/B/A**：同一行、同一发请求，只改相位 ⇒ serving 抢得到 → grace 抢不到 → serving 又抢得到；
- **否定证据 + 正面反证同时给**：节点侧 PG 连接从 2+2 掉到 0（否定），
  **而同一时刻登记表仍在被续租**（正面）—— 后者排除了"静默回落 local"这个最难分辨的失败模式；
- **天然对照组**：同一个 reconcile 循环，能读到答案时删过一条，读不到时一条不删。

对照探针不是可选项：T1 里 `origin_unschedulable` 的 7 次 503，真实原因是"节点没心跳"，
而错误文案说的是"不接单" —— 差点被记成 PASS。

### 5.4 分阶段的价值在**回退**，不在"分批交付"

"先观测、再读、最后写"这个顺序的真正回报是：每一步的回退都不依赖任何新代码。
阶段 0/1 回退 = 把一个 env 置空；阶段 2 回退 = 把一个 env 改回去，
实测 **13 秒、零数据损失**。
反过来说，**如果一个阶段的回退需要跑新写的回滚逻辑，那它就没有真的可回退**。

---

## 附：过程文档索引

本轮产物都在 `docs/proposals/`，以 `_` 开头。它们是本文的引用来源，按需要翻。

**侦察（动手前）**
- `_recon-R1-readpath.md` — Go 侧改造点清单（file:line）
- `_recon-R2-registry-spec.md` — Rust `paused_registry` 语义规格书（13 个方法逐个 + 状态机全图 + 测试清单）🔧 含一处勘误
- `_recon-R3-cluster-runbook.md` — pve-sg dev 集群与发布 runbook
- `_recon-R4-rust-client.md` — Rust 侧接入面 + 三个技术风险 🔧 含一处勘误

**计划（裁决产物）**
- `_impl-plan-control-plane-phase01.md` — 阶段 0/1 任务书（§1 是四处侦察推翻）
- `_impl-plan-control-plane-phase2.md` — 阶段 2 任务书（§1 第五条护栏、§3 RPC 面）

**实现记录**
- `_impl-D3-harden.md` / `_impl-D4-fixes.md` — 阶段 0/1 加固与缺陷修复
- `_impl-D5-sliceA.md` — 阶段 2 前置加固（可重试的 stale release、metadata golden fixture、CI 补真 PG）
- `_impl-D6-scheduler.md` — 阶段 2 controller 侧 Go 实现（§1 是与 `postgres.rs` 的逐条对齐表）
- `_impl-D7-contract-tests.md` — Go 契约测试（§4 是 S0–S11）
- `_impl-D8-node-rust.md` — 阶段 2 node 侧 Rust Central 后端
- `_impl-D10-f1f2.md` — T3 两条静默失败缺口的修复（= `224b70d`）

**审查 / 验证**
- `_review-V1-phase0.md` — 阶段 0 对抗审查（13 条）
- `_verify-plan-phase01.md` — 阶段 0/1 验证计划
- `_verify-T1-results.md` — 阶段 0/1 首轮集群验证
- `_verify-T2-final.md` — 六条修复复验 + T1 两条 BLOCKED 关闭
- `_verify-T3-phase2.md` — 阶段 2 集群验证（五条护栏 + 回退）
