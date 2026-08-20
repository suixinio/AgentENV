# 阶段 3 任务书：语义上收 + 边界闭合（含原阶段 4）

> 2026-08-19 · **裁决产物**，写在闸门 B 通过之后、动手之前。
> 🔧 **2026-08-19 二次修订（回填侦察）**：R1–R5 **全部完成**，结论与随之做出的裁决已写进正文 ——
> §1 由"要查什么"换成"结论 + 它改变了什么"，§2 / §3 / §5 / §6 / §7 按结论改写，新增 §9 汇总。
> **🔴 动手前先读 [§9](#9-侦察改变了什么)**：它一眼列出哪些原定义已被推翻，别照着过期的定义动手。
> 🔧 **2026-08-19 三次修订（设计裁决回填）**：三份设计文档（node / scheduler / gateway）共 **22 条未决项已全部裁决**，
> 逐条标在各设计文档的原处，收口表在 **[§10 设计裁决记录](#10-设计裁决记录)**。
> §3 的批次 A 表格已按裁决同步改写（**A2 不再是字面 `NOT NULL`**、A4 豁免清单多一条、A5 的错误码链路定稿）。
> **🔴 动手前 §9 与 §10 都要读**：§9 是"侦察推翻了什么"，§10 是"设计里的分叉最后选了哪一支"。
> 🔧 **2026-08-19 四次修订（设计阶段收尾 · 五份文档收敛成一份无歧义实施规格）**：
> - **[§10.2](#102--n1n5-的裁决收口冻结契约)（新）**：命名裁决 N1–N5 收口 + **冻结契约表**（HTTP 头 / 错误码链路 / 三个开关 / 日志字段）；
> - **[§10.3](#103--scheduler-a5-设计新增未决项的裁决2026-08-19)（新）**：`_design-phase3-scheduler-a5.md` 新提的 U1–U6 与两条口径纠正的裁决；
> - **[§6](#6-总发布-runbook-n5三份设计的顺序合成一条) 整节重写**：三份设计的发布顺序合成**一条 13 步的线性 runbook**，
>   含破坏性步骤的独立确认点、30800 out-of-band 资源、症状→开关对照表、混版本窗口标注、集群探针；
> - **[§11](#11-实现者速查三个实现-agent-各取一份)（新）**：**实现者速查** —— 按 Rust node / Go scheduler / Go gateway 三块分，
>   各自的文件清单、验收命令、以及"依赖别人先做完什么"；**§11.1 的冻结契约表是三个实现 agent 的唯一契约来源**；
> - §3 的 **A5 行判据已按 A5-U3 重写**（原措辞"被拒（非仅仅改道）"夸大了 A5 的射程，已照实改）；
> - §0 的 P1 补了**边界说明**（"无向后兼容包袱"≠"滚动升级期间没有混版本共存"）。
> 🔧 **2026-08-19 五次修订（设计阶段最后 7 条遗留项落定）**：
> - 🔴 **[§6.1](#61-依赖图为什么是这个线性顺序) 追认「node 只滚一次」**：A4 gate / A5 接收端 / A6 node 响应字段 / preStop 带头
>   全部提前到**步骤 3** 与 A1 同镜像（惰性），**启用点仍在 A3 闸门之后**；步骤表新增
>   🟡「本步部署但未启用」标注；**步骤 9 从"node 第二次滚"变成一次配置热翻转**
>   （前提：node token 走挂载文件，`_design-phase3-node.md` §3.2）。步骤数仍是 **13**。
> - 🔴 **[§6.3](#63--步骤-2破坏性步骤唯一一步会打掉存量沙箱有独立确认点) 写清服务窗口的范围**：
>   它是 **aenv 整体窗口**（控制面 + **数据面**同时不可用），跨过整个步骤 3，典型 **5–15 分钟**；
>   并新增一条前置：**步骤 2 之前必须先清干净 `Running` 沙箱**（否则它们在步骤 3 无快照地消失）。
> - 🔴 **A6 的 node 侧归 impl-node**（`openapi.yml` + codegen + 三个 schema），gateway 只管它自己那三个 DTO ——
>   见 `_design-phase3-node.md` **§3.8**、`_design-phase3-gateway.md` §9 的归属块、本文 §11.2 / §11.4 / §11.5。
> - **node 侧新增指标章节**（`_design-phase3-node.md` §3.9，封闭标签集），`execution_ahead` 从此有名字。
> - scheduler 设计补 `T-A2-10`（releaseClaim 清轴）、`AcquireSandboxRequest.execution_id = 5` 的归属两处标注、
>   §2.3 表与 §3.4 SQL 的**块内**作废标注；node 设计补 `T-A4-7/8/9`。
> - 若干锚点行号订正（daemonset / auth.rs / server.go / central.rs / redis_store_test.go）。
>
> 🔧 **2026-08-20 六次修订（集群验证开工前的实地校正 · pve-sg dev 实测）**：
> - **[§6.0](#60--本轮的部署方式定点更新不走-make-k8s-apply不许抹掉集群侧的-out-of-band-配置)（新）**：
>   **本轮改判为定点更新，不走 `make k8s-apply`** —— 这两套集群与仓内清单有 **10 处 out-of-band 漂移**，
>   全量 apply 会把它们一起抹掉且大多数不报错。含：漂移清单（6.0.2）、要显式创建的资源（6.0.3）、
>   **可直接执行的定点更新命令序列**（6.0.4）、每步的漂移复核探针（6.0.5，全部带对照面）、
>   **control-plane token Secret 拆成两个 key** 的方案与"key 缺失时行为"的查实（6.0.6）、
>   **哪几步要滚服务、哪一步是真热生效**（6.0.7）。
> - **§6.3 的 2.4 / 2.5 已订正**：`$AENV_HOME` 在容器里**是空串**（真变量是 `AENV_HOME_PATH=/workspace/env`），
>   原命令是**静默无操作 + 打印 `0` 伪装成"每台都清干净了"**；同时把"条目数"口径改成
>   `artifacts/` 子目录数或启动日志 `loaded=/retained=`。原表述保留为问题陈述。
> - **§6.5 补齐**：原先只点名 30800 一条，现在引用 §6.0.2 的完整十条清单。
> - **§12.5（新）**：技术债「部署清单与集群实际长期漂移」。
> - **仓内清单改动**：`deploy/k8s/base/agentenv-daemonset.yaml` 的 `control-plane-token` 卷加
>   `items: [{key: node-gate-token, path: token}]`（gateway 仍读 `token`），
>   新增测试 `the_gateway_and_the_node_read_different_keys_of_the_credential_secret` 钉住该不变式。
>
> **🔴 三个实现 agent 只需读：§0 P1 边界 → §3 自己那行 → §6 → §10.2/§10.3 → §11 自己那块 → 自己那份设计文档。**
> **🔴 上集群执行的人另加一份必读：§6.0（部署方式与漂移）。**
> 权威方案：[`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md)（§4 阶段 3 / §8 清单）
> 决策材料：[`2026-08-19-control-plane-refactor-outcome.md`](2026-08-19-control-plane-refactor-outcome.md) §3
> 三家对照：[`2026-08-19-aenv-central-control-plane.md`](2026-08-19-aenv-central-control-plane.md)

---

## 0. 裁决与前提

**🚦 闸门 B ✅ 已通过（2026-08-19 用户裁决）**：

> **写路径 fencing 为主 + node API 收窄同批（原阶段 4）+ 路由层拒旧 execution 为辅。**

候选 2（envd token 绑 execution）留作 `secure` 沙箱加强项，seed 统一后再补；
候选 1（存储层写锁）留长期项，不阻塞本阶段。

**三条前提（都在 2026-08-19 裁定）**

| # | 前提 | 对做法的影响 |
|---|---|---|
| P1 | **AgentENV 尚未上生产，无向后兼容包袱** | schema 直接重画；阶段 2 的 RPC 面直接删掉重写；execution **内部必填**而非可选附加 |
| P2 | **本轮只做 AgentENV**，agent-platform 后期再改 | ExecutionID 内部化，阶段 3 **零跨仓依赖** |
| P3 | 闸门 A 已过：长期自维护 fork | 允许改 Rust 主干 |

> 🔴 **P1 的边界（2026-08-19 追加，A5-U2 裁决的直接产物，写在这里防止后来者再犯）：**
>
> **P1「无向后兼容包袱」指的是「没有生产存量数据 / 没有外部客户端锁死我们的契约」，
> 它 ***不*** 等于「滚动升级期间没有混版本共存」。**
>
> node 是 DaemonSet、scheduler / gateway 是 Deployment，**三者各自独立滚动，且滚动不是原子的**。
> 任何"新版本读不懂旧版本发的东西 ⇒ 当成空"的字段处置，都会在那个窗口里产生真实故障。
> 实证：`HeartbeatRequest.sandbox_ids` 若按 P1 字面直接 `reserved 8`，新 scheduler 从旧 node 读到**空 roster**，
> 两个 binding 存储会把该节点名下的 binding **全删**（`store.go:90-97`、`redis_store.go:283-291`/`:306-308`），
> 后果是**所有从未 pause 过的沙箱在整个滚动窗口里数据面 404**（[§10.3 A5-U2](#103--scheduler-a5-设计新增未决项的裁决2026-08-19)）。
>
> ⇒ **判据**：删/重画一个 proto 字段之前，先问「**滚动窗口里，新的那一侧读到这个字段缺失时会做什么**」。
> 答案若是"当成空集合并据此删数据"，就必须并存一个发布周期。

🟡 **P1 与回填后 A2 的张力（先说清，免得被读成自相矛盾）**：P1 说"schema 直接重画"，
但 R3（`migrate.go:28-32` 自认 bootstrap 对无默认值 NOT NULL 列不安全）+ P4（Console 仍直连 PG）
让**纯加法**成为本轮更优路径。⇒ **P1 依然成立** —— 我们**有权**重画、没有兼容包袱 ——
但**本轮不必行使**；真要行使，就要付 §1.4「本轮不做的代价」那笔账并在 PR 里显式登记。

**🔴 P4（2026-08-19 追加）范围裁定：Agent-Console 相关工作整块移出本轮。**
本轮只做 AgentENV 仓内的架构重构；主仓 `apps/Agent-Console` 的"切只读 API / 断 PG 直连 / 修三个既有缺陷"
后续单独更新。R4 的侦察事实全部保留（见 §1.4），但它**不再产生本轮的工作项**，
只产生 A2 的一条**新约束**（§3 A2 行 + §1.4「本轮不做的代价」）。

**🔴 闸门放行的是"开工"，不是"提前放宽不变式"**：在 A3 落地并通过集群验证之前，
`running` / `resuming` 永不可抢这条**不许放宽**。

---

## 1. 侦察结论（R1–R5 ✅ 2026-08-19 全部完成）

§2.1 的教训是"写方案时引用的每一个数据源，都要在动手前确认它真的存在"。本阶段的五条**已全部收口**。
下表保留当初的问题（说明要查什么、为什么），结论逐条展开在 §1.1–§1.5。

| # | 当初要查什么 | 判据 / 为什么 | 结论 |
|---|---|---|---|
| **R1** | agent-platform 打的 `http://<node-ip>:30800`（主仓 `apps/agent-platform/internal/sandbox/aenv/client.go:91-100`）**落在 gateway 还是 node 自身 API** | 决定 A4 收窄的边界画在哪。查不清就动手 = 要么收窄无效，要么打掉平台 | ✅ **二分本身错了**：寻址=gateway，执行=node REST ⇒ **A4 改读法**（§1.1） |
| **R2** | `SandboxInstanceId`（`src/sandbox/custom_extension/client.rs`）**今天的生成时机与生命周期** | A1 要把它升格成 ExecutionID | ✅ **它在任何已部署环境里一次都没被生成过** ⇒ **A1 按"新建"排期，不是"迁移"**（§1.2） |
| **R3** | `generation` 的**全部**读写点 | A2 要定"身份轴 vs 版本轴" | ✅ **CAS 是 4 处不是 3 处**，且原引用的 `postgres.rs` 已被删除；**两轴必须分开**（§1.3） |
| **R4** | Agent-Console 读登记表的**全部字段**（主仓 `apps/Agent-Console/internal/aenv/pausedregistry/reader.go`） | A2 重画 schema 会打掉 PG 直连 | ✅ 只读 API 早已存在、只缺 3 个字段；**但按 P4 本轮不做**，改为 A2 的一条风险约束（§1.4） |
| **R5** | 节点侧**是否存在** controller 之外可触发 pause / publish 的路径 | A3 的前提 | ✅ **存在 9 条，其中 4 条零外部请求即可自主写共享状态**；与 e2b 结构性相反 ⇒ A4 收窄打不到它们，**A3 扩范围**（§1.5） |

---

### 1.1 R1：`:30800` 落 gateway 还是 node？—— **二分本身是错的**

**结论：寻址=gateway，执行=node REST。**

- `agentenv-gateway-nodeport` NodePort 30800 → gateway Pod 8080（实地 `kubectl` 验证）。
- 但 gateway 是**默认透传**：只截 4 类路径自处理（`services/gateway/internal/server.go:101-134`、`:176-195`），
  控制面路径**不改写**、原样转发（`server.go:674-695` `upstreamTargetPath`）。
- 平台 6 个调用（主仓 `apps/agent-platform/internal/sandbox/aenv/client.go`）里 **5 个被逐字转发到 node `:8000`**，
  只有 `GET /v2/sandboxes` 是 gateway 自聚合（`services/gateway/internal/cluster_list.go:52-62`、`:72-119`）。
- node REST 只在 8000，**无任何 NodePort / hostPort**（`deploy/k8s/base/agentenv-daemonset.yaml:109-113`，headless ClusterIP）。
- node 鉴权是 **presence-only**：`src/api/impls/auth.rs:15-55`（`impl ApiKeyAuthHeader` 整块）注释自认 "Presence, not validity"；实测无头 401 / 任意值 200。
- dev 集群**零 NetworkPolicy**（实测 `No resources found`），而 AgentENV 自己的
  `docs/src/deployment/kubernetes.md:174-199` 明写 "protected by the network, not by its headers"
  —— **那张网根本没铺**。
- 🔴 **controller 没有下行命令通道**：`services/api/proto/scheduler.proto:387-401` 五个 RPC 全是 node 当 client；
  `grep 'http.Client|http.NewRequest|Dial(' services/scheduler` **零命中**。
- 🔴 30800 那个 Service **不在仓内清单里**（仓内 `deploy/k8s/base/gateway-service.yaml` 只有 ClusterIP 8080+9102），
  是集群里手工 `kubectl apply` 的 **out-of-band 资源**。

**它改变了什么**

| 原定义 | 新定义 |
|---|---|
| A4「node 用户级 REST 只接受 **controller**」 | 改读法：**只接受来自 gateway / scheduler 的调用**，把 gateway 认定为 controller 的前端（§3 A4 行 + §9 第 14/15/16 条） |
| §2 §3.6「只能由 controller 做」当作 A 批次可兑现 | 拆开：controller 无下行通道 ⇒ "只能由 controller 发起"移到 **B6**（§2） |
| 发布 / 回退按 `kubectl apply -k` 还原 | **runbook 必须点名 30800 这个手工 Service**，`apply -k` 还原不了（§6） |

---

### 1.2 R2：`SandboxInstanceId` 生命周期 —— **它从未被生成过**

**结论：语义正确，但机制是死的。**

- 它是 `Uuid::now_v7()`，**模块私有、零持久化，且只在 `[custom_extension].url` 配置时才生成**。
  `config/default.toml:130-136` 该项被注释掉、`deploy/` 零命中、主仓零消费方
  ⇒ **在任何已部署环境里一次都没被生成过**。
  🔴 outcome 文档 §3 那句"aenv 内部已经有半个 execution 了"要**打折**：可复用的是**语义与单测**，不是运行中的机制。
- 语义**已经**是 A1 要的：start / resume 换代（`src/sandbox/custom_extension/client.rs:271-272`、`:300-301`），
  snapshot / fork **不**换代（`src/sandbox/firecracker/sandbox.rs:330-361`、`:363-416` 走 FC 原地 pause+resume，guard 未动）。
  有 **9 个单测**钉住（`client.rs:588-813`）。
- **四条冲突使它不能直接升格**：
  1. 它是"本节点这一次 VM 运行"，不是"沙箱化身" —— 跨节点不可比对；
  2. 铸造时机**晚于**登记表首次写入（`src/orchestrator/service.rs:1558` 翻 `Resuming` 时它还不存在）；
  3. 可选存在 + mock backend 没有 ⇒ P1 要求的"内部必填"做不到；
  4. 由**节点**铸，而 B4 要 **controller** 铸。
- 🔴 **"从模板 / 快照创建新沙箱"走的是 `LaunchMode::Resume`**（`src/sandbox/firecracker/sandbox.rs:551-561`）
  ⇒ 换代判定**必须按 `LaunchPlan` 变体判**；按 hook 类型或 `LaunchMode` 判会把"创建"误判成"resume"。
- 🟢 **建议实现形状**：在 `LaunchPlan`（`src/orchestrator/launch_plan.rs`）的 Create / Resume 两个变体上
  **各带一个 `execution_id` 字段**，让"各换一次代"由类型系统保证；`SandboxInstanceId` 改成它的**派生**，
  对外 JSON 字段名 `sandboxInstanceId` 不变（openapi 契约零破坏）。
- 🟢 **仓内已有一处上线中的同构 fencing 先例**：`NodeIdentity.service_instance_id`
  （`src/identity.rs:19,41-45`，同为 UUID v7），scheduler 已在校验
  （`services/scheduler/internal/service.go:345`）且有测试钉住
  （`services/scheduler/internal/service_test.go:644` `TestUnregisterObservedNodeRejectsServiceInstanceMismatch`）。
  ⇒ **A1 / A3 的 proto 字段命名、校验位置、错误语义直接对齐它**，别另发明一套。
- 🟡 envd access token = `HMAC-SHA256(seed, sandbox_id)`（`src/sandbox/access.rs:73-78`），**不绑化身**
  ⇒ 旧化身的 token 在新化身起来后依然有效。这坐实了"候选 2 留作后续加强项"的判断，
  同时也意味着**数据面这道门今天完全敞开**。
- 🟡 节点崩溃时**零信号**（`client.rs:336-347` 的 Drop 兜底只在进程活着时有效）
  ⇒ **A1 不替代 B7**（`release_node_holdings` 保留）。

**它改变了什么**：A1 从"迁移既有字段"变成"**新建字段 + 复用语义与单测**"，排期按新建算；
换代判定的位置被钉死在 `LaunchPlan` 变体上；命名与校验语义对齐 `service_instance_id` 先例。

---

### 1.3 R3：`generation` 的全部读写点 —— **CAS 是 4 处，且两轴必须分开**

**写点**

| 类别 | 位置（全部在 `services/scheduler/internal/registry/store_postgres.go`）|
|---|---|
| **CAS（4 处，原任务书写的 3 处漏了 `Remove`）** | `:484` completePause · `:519` markLocalOnly · `:786` releaseClaim · 🔴 `:1179` remove（`DELETE … AND generation = $3`）|
| **递增（6 处）** | `:396` beginPause(ON CONFLICT) · `:594` claimForResume · `:618` claimForResumeDurableOnly · `:834` markRunning · `:998` reclaimReleased · `:1110` releaseHoldingsReleased |
| 新行初值 | `:388,392` 硬编码 `generation=1`。**无 DB 默认值、无 sequence、无 trigger**，全靠应用层 |
| 读侧 | **全部是透传 / 日志，无一处做值比较** |

- 🔴 **`postgres.rs` 已不存在**，被 `4208f47 refactor(paused-registry): take PostgreSQL off the node` 删除。
  原任务书引用的 `postgres.rs:416/:447/:771` 指向**已删文件**（那些行号在 `4208f47^` 上才是对的）。
  **全部 CAS 现在只在 Go 侧 `store_postgres.go`。**
- 完整 DDL 真相源：`services/scheduler/internal/registry/migrate.go:39-71`（`const SchemaDDL`；
  无独立 `.sql`、无 migration 工具，`Migrate()` 用 advisory lock `0x0A6E76534348_4D41` 串行化）。

**🔴 裁决：身份轴（execution）与版本轴（generation）必须分开。三条独立证明：**

1. **一次跨节点 resume 里，沙箱还没开始服务 generation 就涨两次**（ClaimForResume +1 → MarkRunning +1），
   再 pause 又涨一次 ⇒ 合并则单化身生命周期内身份变 3 次，与 A1 验收判据「snapshot 前后 execution 不变」
   **自相矛盾**，且 A3 会把化身**自己的**后续写当成旧化身拒掉。
2. `ReclaimExpiredHoldings`(`:998`) 与 `ReleaseNodeHoldings`(`:1110`) 在**无任何化身参与**下递增 generation
   （控制面定时器 / 死节点上的后继进程）⇒ 合并等于为"没有活进程的沙箱"**凭空签发身份**。
3. 已批准的 v3「暂停必然落 OSS」（主仓 `docs/proposals/2026-08-19-aenv-pause-publish-durability.md`）
   把 `publishing` 改成长驻重试态后，`CompletePause` 会在**同一 execution、同一 generation** 下自环重试 k 次
   —— 方案明写"零 generation 抖动"是其收益。

**随手带出的三条**

- 🟡 **§5 的 D8 已经做完了**，不是"一行待补"：Rust `src/orchestrator/paused_registry/central.rs:432`
  + Go `services/scheduler/internal/registry_service.go:688` 映射齐备，还有负向测试（`central.rs:2090-2126`）
  ⇒ **从 §5 清单里划掉**。
- 🟡 proto 里还留着 `TRANSITION_KIND_REMOVE_UNCONDITIONAL`，服务端已 fail-closed 拒服务
  （`registry_service.go:325-332`）⇒ **A2 / A3 重画 RPC 面时把枚举值一并删掉**。
- 🟡 `migrate.go:28-32` 注释**明写**这套 `ADD COLUMN IF NOT EXISTS` bootstrap 对"新增无默认值 NOT NULL 列"
  不安全（原话 "Indexes qualify; a NOT NULL column still would not"），且 dev / test 两套集群有存量行
  ⇒ **A2 的 `execution_id NOT NULL` 不能一步到位**：要么给默认值 + 回填，要么按 P1 直接 drop 重建
  （drop 重建的代价见 §1.4）。
- 🟢 **A2 爆炸半径比想象小**：`agent-platform` 对 generation **零命中**；
  外部读者只有 Console PG 直连（纯展示）与 gateway JSON 透传。

---

### 1.4 R4：Console 读登记表哪些字段？——**侦察成立，但按 P4 本轮不做**

**🔴 侦察结论（保留，作为后续 PR 的现成输入）**

- **只读 API 已经存在，不用新建**：`GET /registry/sandboxes`（`services/gateway/internal/registry_list.go`，
  X-API-Key + `state` / `nodeID` / `limit` / `nextToken` + `databaseTimeUnixMs`，后端 scheduler gRPC `ListRegistrySandboxes`）。
  阶段 0 建它就是为了让 Console 别再直连。Console 的 `/api/**` 反代也**已在无条件注入 `X-API-Key`** 打到 gateway
  （主仓 `apps/Agent-Console/internal/aenv/proxy/control.go:30-37,62`）⇒ **Console 侧不需要新增任何网络配置**。
- Console 直连 PG **只有一处**（主仓 `apps/Agent-Console/internal/aenv/pausedregistry/reader.go`，
  DSN 来自 `AGENTENV_PAUSED_REGISTRY_DSN`），且**零写操作**（纯 SELECT + `pgx.ReadOnly` 事务 +
  连接级 `default_transaction_read_only=on`）。
- **缺口只有 3 个 metadata 派生字段**：`snapshot_alias`、`resources.cpu_count`、`resources.memory_mib`。
  另 5 个（`disk_size_mib` / `auto_resume` / `timeout_action` / `expires_at` / `user_metadata`）
  Console 只放进 DTO **从不渲染** ⇒ 平移时直接删，**不要加进 API**。
- 🔴 **排序**：Console 现用 `ORDER BY (state='running') ASC, updated_at DESC`（活行垫底，
  防截断吃掉"停着的行"—— 那正是这一页存在的理由）。API 要么加 `order=parkedFirst`，
  要么给"取全量"的明确保证。
- 前端（主仓 Agent-Console `web/src/**`）**零改动**（只要 BFF 响应形状不变）。

**Console 侧三个既有缺陷（本轮不修，登记在案，Console 后续更新时一并修）**

| # | 缺陷 | 证据 |
|---|---|---|
| C1 | `Reconcile` 拿 `origin_node_id` 比 roster，而 AgentENV `Sandbox.Holder()` 明写 **resuming 行的权威持有者是 `claimed_by_node_id`** ⇒ 跨节点搬迁中的行判错 | `services/scheduler/internal/registry/registry.go:98` |
| C2 | 租约判定用 **Console 进程墙钟**（主仓 `apps/Agent-Console/internal/aenv/server/server.go:292`，`Reconcile(..., time.Now().UTC())`）而非 DB 时钟 —— **正是 §5 的 S5**；而 API 已返回 `databaseTimeUnixMs`，平移即修复 | —— |
| C3 | `leaseExpired` 谓词两边**各手抄一份**（主仓 `apps/Agent-Console/internal/aenv/pausedregistry/reconcile.go` vs AgentENV `services/scheduler/internal/registry/registry.go:113`），必然漂移 | —— |

> **S5 留在 AgentENV 侧**：它是 `Get` / `GetMany` 读路径拿不到 DB 时钟这个**接口缺口**，
> 属于 AgentENV，与 Console 是否平移无关。**不要因为 Console 出局就把 S5 一起划掉**（见 §5）。

**🔴 本轮不做的代价（必须写明，不能变成没人知道的意外）**

Console 今天**仍在直连 PG** 读 `paused_sandboxes`，本轮既然不改它：

- 它的 SQL 是**一整条语句**（主仓 `apps/Agent-Console/internal/aenv/pausedregistry/reader.go:41-64`），**任一列失效就整体失败**，
  整页变成 `source=unavailable`（该文件的 `Read` 刻意不返回 error，只报"读不到"）。
- Console 正在 SELECT 的列（A2 动它们就会失明）：
  `sandbox_id` / `cluster_id` / `state` / `generation` / `origin_node_id` / `claimed_by_node_id` /
  `snapshot_id` / `lease_expires_at` / `sandbox_expires_at` / `paused_at` / `updated_at` /
  `metadata`（`->>'snapshot_alias'`、`->'resources'->>{cpu_count,memory_mib,disk_size_mib}`、
  `->>'auto_resume'`、`->>'timeout_action'`、`->'expires_at'`、`->'user_metadata'`）。
- ⇒ **A2 应尽量做纯加法**（只加 `execution_id`）。若按 P1 确实要 drop 重建表，
  **必须在 PR 描述与本任务书里显式记下"Console 会同步失明，等后续 PR 修"** —— 这是缩小范围的已知代价。

---

### 1.5 R5：controller 之外有无自主 pause / publish 路径？—— **有 9 条**

**口径先说清**（9 条不是同一种东西）：
**4 条是"零外部请求即可自主写共享状态"**（#2 #3 #4 #5）—— **A4 的网络收窄对这 4 条完全打不到**；
另有 #6（自主，但**只写本地**）与 #9（外部流量触发、**决策在节点**、写 PG 且无 generation 校验）；
其余是外部 HTTP 入口（#1 #7 #8）—— 那 3 组才是 A4 能收的。

**🔴 结论与 e2b 结构性相反**：e2b 的 `orchestrator.proto:56-59` 保证节点不自己动手；
**我们的节点是快照链每一次写入的唯一发起方**，controller 今天是纯被动 RPC 服务端
（唯一自主动作是 `RunReclaim`，`services/scheduler/internal/registry_service.go:735`）。

| # | 路径 | 类型 | 位置 | 是否写共享状态 |
|---|---|---|---|---|
| 1 | `POST /sandboxes/{id}/pause` | 外部 HTTP，无鉴权 | `src/api/impls/sandbox.rs:1056` → `src/orchestrator/service.rs:1240` | 写 PG + OSS + **删旧快照** |
| 2 | 🔴 **TTL 到期自动 pause，默认 1 秒一跳** | **节点自主** | `service.rs:2094` `evict_expired_sandboxes` → `:2108`；间隔 `config/default.toml:198` = 1000ms | 全套 |
| 3 | 🔴 TTL 到期自动 delete | **节点自主** | `service.rs:2115` → `:1087` → `src/api/impls/paused_coordinator.rs:497` | 删 PG 行 + 删 OSS 快照 |
| 4 | 🔴 优雅关机批量 pause（SIGTERM，最多 3 轮，所有非 Paused 沙箱全 pause）| **节点自主** | `service.rs:2604` → `:2636`，触发点 `src/bin/server.rs:228`/`:253` | 全套 |
| 5 | 🔴 启动时 `release_node_holdings` + 失败后 5s 一次后台重试 | **节点自主** | `src/bin/server.rs:181`/`:192`、`src/api/impls/paused_recovery.rs:468`/`:497` | 写 PG |
| 6 | 周期 reconcile（30s）丢本地 paused / 拆本地 running | **节点自主** | `src/api/impls/paused_recovery.rs:597`/`:719`/`:638` | ⚠️ **只写本地，不写 PG/OSS** |
| 7 | `POST /sandboxes/{id}/snapshots` 显式 checkpoint | 外部 HTTP，无鉴权 | `src/api/impls/sandbox.rs:1095` → `:1157` | **只写 OSS，完全不碰 `paused_sandboxes`** ⇒ 不进快照链，**A3 不必管，A4 仍要收** |
| 8 | `POST .../resume` / `DELETE /sandboxes/{id}` / `POST .../fork` | 外部 HTTP，无鉴权 | `sandbox.rs:1234` / `:775` / `:816` | 写 PG |
| 9 | 🔴 **数据面反代 auto-resume** | **节点自主**（外部流量触发，决策在节点）| `src/api/proxy.rs:686` → `:800` `try_auto_resume` → `orchestrator.resume_sandbox` | 写 PG `mark_running`（**无 generation 校验**）|

**风险排序与关键机理**

1. 🔴 **真正裸奔的只有 `begin_pause` 与 `mark_running`，而且是 RPC 契约层主动拒收 generation 字段**
   （`registry_service.go:192` 与 `:267` 的 `rejectFields`；`:191`/`:266` 是它们各自的 `case` 行）。其余四个 CAS 都已带守卫。
   ⇒ **A3 不是「给三种操作加校验」，而是要改 proto 契约 + 改两条 SQL 的 WHERE。**
2. 🔴 **#2 不是罕见竞态而是必然序列**：`pause_sandbox_inner` 在 `begin_pause` 之前**零次读集群行**
   （`service.rs:1253` → `:1475` → `paused_coordinator.rs:263` → `:284`），
   而 `beginPauseSQL`（`store_postgres.go:381-409`）是 `ON CONFLICT DO UPDATE` 且 WHERE 只比 `cluster_id`。
   配上我们比 e2b 激进的 reclaim：reclaim 把 running 翻 paused（`store_postgres.go:995-1004`）→ B 节点接管
   → 老节点恢复联系 → **1 秒内**自动 pause 打回来，把行改成自己的 publishing、落陈旧快照，
   最后 `src/api/impls/paused_coordinator.rs:313-320` 把**活节点正依赖的 durable 副本从 OSS 删掉**。
3. 🔴 `mark_running` 的 SQL 守卫只有 `claimed_by_node_id IS NULL OR = $2`（`store_postgres.go:831-839`，守卫在 `:839`），
   而 **running 行的该列正是 NULL** ⇒ 一次数据面流量就能抢走别人的 running 行，两台 VM 同时活着。
   触发条件是代码注释自承认的常态（`src/bin/server.rs:295-300`：scheduler binding 在两节点间抖）。
4. `publishing → paused` 的翻转者：**SQL 在 controller，决策与触发 100% 在节点** ——
   节点自己排的三步 `src/api/impls/paused_coordinator.rs:284 begin_pause` → `:299 publish_captured`（上传 OSS）
   → `:307 complete_pause`；controller 不知道有沙箱在 publishing，不会超时、不会重推。
   **e2b `pause_instance.go:71-77` 的等价物在我们这里不存在。**
5. "build 可被 resume 选中"的判定有**两处**：
   (a) 集群判定 `claimForResumeSQL`（`store_postgres.go:585-604`，判据 = `snapshot_id IS NOT NULL` + state）；
   (b) 🔴 **节点本地判定完全绕开集群**（`service.rs:1541`/`:1600`，只要本地 metadata 是 Paused
   就直接从本地 artifacts 拉起）。REST resume 前面有两道闸
   （`sandbox.rs:1254 discard_if_superseded` + `:1261 arbitrate_resume`），
   **数据面 auto-resume 两道都没有**。

**🟢 减轻因素（缩小了 A3 必须守住的面）**

每次 pause 都 `SnapshotId::generate()` 全新 UUID（`src/api/impls/paused_coordinator.rs:685-690`），
OSS `artifacts/{uuid}/` 天然不相交、层内容寻址 ⇒ **旧化身写 OSS 本身无害，只留垃圾**。
**全部不可逆损失只在两点**：`completePauseSQL` 翻牌，和 `paused_coordinator.rs:313-320` 删 `previous_snapshot_id`。
**A3 守住这两点就够** —— 这为「候选 1 存储层写锁留长期项」提供独立佐证。

**🟢 / ⚠️ 另外两条**

- 🟢 **今天没有后台重发 publish 的东西**（失败只降级 `mark_local_only`）。
  ⚠️ 但 v3「暂停必然落 OSS」要把 `publishing` 改成长驻重试态
  ⇒ **会新造一条节点自主写路径，必须与 A3 同批设计**，否则一边收权一边开新口子。
- 🟢 方案 §7.4 的判断被证实：内部化后 execution **必填**成立 ——
  9 条路径全在节点侧、都能拿到本次化身身份，不存在 e2b 那种 fresh-read 语义。

---

## 2. 本阶段的不可协商项

沿用方案 §3 的五条护栏，**新增三条**（原 §3.6 按 R1 结论拆成 §3.6a / §3.6b）：

- **§3.6a（A 批次兑现）写路径必带身份**：任何进入快照链的状态翻转
  （`publishing → paused`、build 可被 resume 选中）**必须携带并在 SQL 事务内校验 execution 身份**。
  兑现物 = **A3**。
- **§3.6b（B6 兑现）发布权集中**：上述翻转**只能由 controller 发起**。
  这是 e2b 被实证有效的那条（`pause_instance.go:71-77` + `get_last_snapshot.sql:8`）。
  🔴 **今天在我们的架构里无对应实现** —— R1 证明 controller **没有下行命令通道**
  （`services/api/proto/scheduler.proto:387-401` 五个 RPC 全是 node 当 client；
  `grep 'http.Client|http.NewRequest|Dial(' services/scheduler` 零命中），
  需要 B6 新增命令下发面才谈得上。**兑现物 = B6，不是 A3。**
- **§3.7 孤儿判据必须带身份**：`KillOrphan` 按 **execution** 判，不按 sandboxID 存在性判。
  抄自 e2b 的现成缺口（`storage/redis/main.go:205-217` 只看 `raw != nil`）—— 别把缺口一起抄。

> **为什么必须拆开**：原 §3.6 把"带身份校验"与"只能 controller 做"写进同一句话，
> 而后者依赖一个还不存在的下行通道 ⇒ 不拆，A3 **永远做不完**（每次评审都会因为"还没集中发布权"被判未达标）。

---

## 3. 批次 A：身份轴 + 边界闭合（闸门 B 的解锁工程）

| # | 事 | 验收判据 | 变异验证（把修复退回去，测试必须 FAIL）|
|---|---|---|---|
| **A1** | ExecutionID 一等公民（内部）。**按"新建字段"排期**（R2：`SandboxInstanceId` 从未在任何部署环境生成过），复用它的语义与 9 个单测。在 `LaunchPlan` 的 **Create / Resume 两个变体上各带 `execution_id`**（`src/orchestrator/launch_plan.rs`），让"各换一次代"由类型系统保证；`SandboxInstanceId` 改成其派生，对外 `sandboxInstanceId` 字段名不变。命名 / 校验位置 / 错误语义**对齐 `NodeIdentity.service_instance_id` 先例**（`src/identity.rs:19,41-45`、`services/scheduler/internal/service.go:345`）。🔴 ✅ **裁决（§10 第 1 条）：Resume 的化身在 claim 时预分配（E-A）** ⇒ 铸造点从 VM start 前移到 resume 决策点，`for_resume` 由"铸造者"改成"**消费一个只有 claim 能构造的 token**"，「拿到 `LaunchPlan` ⟺ 已铸恰好一个新 execution」的编译期保证**不许丢** | 同一沙箱 pause→resume 后 execution 变化；snapshot / fork 前后不变；**"从模板/快照创建"判为 Create（不是 resume）** | ① 让 resume 复用旧 execution ⇒ A3 的拒绝用例必须转 FAIL；② 把换代判定改回按 `LaunchMode` / hook 类型判 ⇒ "模板创建算新化身"用例必须 FAIL |
| **A2** | 登记表 schema：**加** `execution_id UUID`（**nullable**）+ `execution_started_at TIMESTAMPTZ`。🔴 ✅ **裁决（§10 第 3 / 22 条）：不按本任务书原先的字面 `NOT NULL` 写** —— 一刀切 NOT NULL 会逼出哨兵 UUID，而哨兵迟早被人拿去比相等、fencing 无声破掉（第二个理由仍是 §9 第 5 条那条：`services/scheduler/internal/registry/migrate.go:28-32` 自认这套 `ADD COLUMN IF NOT EXISTS` bootstrap 对"无默认值 NOT NULL 列"不安全）。改用**按状态的 CHECK**（严格更强，还管住"不该有的时候没有"）：<br>`CHECK ((state IN ('running','publishing','resuming')) = (execution_id IS NOT NULL) AND (execution_id IS NULL) = (execution_started_at IS NULL))`<br>（`resuming` 在集合内，是裁决 §10 第 1 条走 E-A 的直接结果；`execution_started_at` 是 §10 第 22 条 —— `updated_at` 被 `renewLeaseSQL:925` 每心跳刷新，B3 的 grace 不能拿它当起点）。迁移走 **P1 drop 重建**：运维一次 `DROP TABLE`，🔴 **与 node 侧清空本地 paused 记录是同一个 runbook 步骤**（§10 第 4 条），并保留 `Migrate` 的 fail-fast 自检。**身份轴 / 版本轴分开**（generation 保持版本轴，语义不动）。🟢 **只加列、不删列/不改名 ⇒ Console 的 PG 直连不会失明**（表清零只让那一页变空，`source` 仍是 `ok`，与"读不到"是两个显示结果） | CHECK 生效：`running`/`publishing`/`resuming` 行缺 execution 写不进去（`23514`），`paused`/`local_only` 行带 execution 也写不进去；三条读路径（`Get` / `GetMany` / `ListRegistrySandboxes`）都带出 execution；`Migrate` 遇到阶段 3 之前的存量行**报错并指明 `DROP TABLE`**，不是 PostgreSQL 的约束原文 | 去掉 CHECK（或只写单向）⇒ "缺 execution 的 running 行被拒" 与 "parked 行不许带 execution" 两发用例必须 FAIL；去掉自检 ⇒ 错误退化成约束原文，`TestMigrateRefusesAPrePhase3Table` 必须 FAIL |
| **A3** | **写路径 fencing（主角）**。范围按 R5 扩大为三件事：① **改 proto 契约**让 `begin_pause` / `mark_running` **接受并要求** execution（今天是 `registry_service.go:192`/`:267` 的 `rejectFields` **主动拒收**）；② 改 `beginPauseSQL`（`store_postgres.go:381-409`）与 `markRunningSQL`（`:831-839`）的 **WHERE**；③ **数据面 auto-resume 那条 PG 写也必须校验**（`src/api/proxy.rs:686` → `:800` → `mark_running`）。🔴 校验在 **SQL 事务内**原子完成。<br>✅ **裁决收口三条**：① **化身谓词本轮只加这两条**（`begin_pause` / `mark_running`）——另外四条转换（`complete_pause` / `mark_local_only` / `release_claim` / `remove`）保持 **generation-only**，且契约层**拒收** `execution_id`（§10 第 13 / 19 条）；② 🔴 **三条夺权路径（reclaim / releaseHoldings / releaseClaim）必须清空 `execution_id`**，不清则"reclaim 后老节点 1 秒自动 pause 打回来"那条必然序列**第一步就成立**（§10 第 13 条）；③ 拒绝语义**两分**：`ErrExecutionFenced → codes.PermissionDenied`（**永不重试**）与既有 `GenerationConflict → codes.Aborted`（重读再试）**严格分开**，合并会让节点重读拿新 generation 再发一次而**绕过 fencing**（§10 第 2 条） | 旧 execution 的 pause / publish / remove 全部被拒且**零副作用**（行未变、无文件写出）；**必须守住的两点**：`completePauseSQL` 翻牌 与 `paused_coordinator.rs:313-320` 删 `previous_snapshot_id` | ① 把校验挪到事务外的 handler ⇒ 并发插队用例必须 FAIL；② 去掉校验 ⇒ 拒绝用例必须 FAIL。🔴 **第一发变异指定打 `begin_pause`**（R5 风险 #1 那条必然序列）|
| **A4** | node API 收窄（原阶段 4）。🔴 **改读法**：node 用户级 REST **只接受来自 gateway / scheduler 的调用**，把 gateway 认定为 controller 的前端（R1：controller 无下行通道，按字面"只接受 controller"等于谁都进不来）。**数据面反代不变**。切点在 node 的 generated 控制面 router（`src/api/server.rs:24-41`），**不碰** `.merge(proxy::router(...))`。**同批收 `POST /nodes/{id}`**（gateway 无条件透传 + node presence-only 鉴权 ⇒ 任何能打到 30800 的人都能把节点置 DRAINING；无代码调用方，收窄零打击面）。<br>🔴 ✅ **裁决（§10 第 9 条）：豁免清单是两条，不是一条** —— 除 `/health` 外，**必须放行 `GET /sandboxes`（含 `GET /v2/sandboxes`）**：gateway 的集群列表是自聚合扇出、不经 `ReverseProxy.Rewrite`（拿不到注入的 token），而该端点 all-or-nothing（`cluster_list.go:84-96`）⇒ 不放行就是集群列表整体 502。🟡 **只豁免只读 GET，`POST /sandboxes` 仍收**。<br>✅ **裁决（§10 第 17 条）：node 同批实现 A5 的接收端** —— 比对 `x-agentenv-expect-execution-id`、不匹配回 **412** + `x-agentenv-refusal`、并**始终回声** `x-agentenv-execution-id`（落点在数据面 `proxy.rs`，且**必须在 auto-resume 之前**）| 直连 node 的破坏性调用被拒；经 gateway 的同一调用成功；**平台 6 个调用一行不改全部仍通**；`POST /nodes/{id}` 直连被拒；🔴 **无 token 的 `GET /sandboxes` 仍通、无 token 的 `POST /sandboxes` 被拒**（后半句是对照面，缺了它"整条路径前缀豁免"的变异会假绿） | 放开鉴权 ⇒ 直连拒绝用例必须 FAIL；把 `/sandboxes` 整条前缀豁免 ⇒ `POST` 那发必须 FAIL |
| **A5** | 路由层拒旧 execution：`LookupNode` 携带身份。🔴 ✅ **裁决 A5-U3（§10.3）改写了本项的定位，照实写、不粉饰**：**A5 的主体是「让路由答案变正确」**（binding / roster 持久化 execution + 仲裁改成 UUID v7 大者胜，治的是**旧化身活着时每个心跳把 binding 抢回去**这条必然序列，`store.go:99-102` / `redis_store.go:293-300` 两处实证）；**闸 2（回程比对 node 回声）提供检测与自证**；**闸 1（下发 expect 由 node 拒）只覆盖「node 比中央旧」这一种情形，本阶段真阳性集合接近空集** —— 因为路由与 expect 来自同一个 lookup 答案，不匹配只可能是节点比中央新。🔴 **不要把 A5 说成「能拦截飞行中的旧化身流量」，那是 A3（SQL 事务内 fencing）与 reclaim 顺序的职责。**闸 1 保留是为 B4/B6 上膛（那时 execution 由 controller 铸，`live < expect` 才变成可达状态）。🔴 **硬约束：拒绝码绝不能是 404**（见 §7.5）—— 用 **409**（与 resume 撞 running 的既有语义一致）并配独立错误码。<br>✅ **裁决收口五条**：① 拒绝链路定稿 **node 412 + `x-agentenv-refusal` → gateway 409 + `code=sandbox_execution_superseded`**，🔴 **也不许复用 410**（node `/proxy` 已用它表示 not proxyable，`src/api/proxy.rs:869-872`）（§10 第 2 条）；② 🔴 **binding 存储必须持久化 execution 并在 `LookupNode` 的 binding 命中分支回出**，且**必须在 HA / query-only 副本形态下验证**（本地单 scheduler 测不出它失效）（§10 第 5 条）；③ 冲突仲裁 = **UUID v7 字典序大者胜** + 对反向覆盖打 warn，**登记表在被查询时是真相**（§10 第 6 条）；④ **闸 3（长连接周期性撤销）本轮不做**，残留缺口标注"已知、有意推迟"（§10 第 10 条）；⑤ 开关**默认 `enforce`**，发布时先跑一轮 `observe`（§10 第 11 条）。<br>🟢 **`GET /v2/sandboxes` 的非确定性排序缺陷并入本轮**（双活时 `startedAt` 与 `sandboxID` 全同 ⇒ `sort.Slice` 不稳定 ⇒ 胜者随机；`cluster_list.go:248-249` 的 TODO 自己写了正解 = ExecutionID）（§10 第 12 条）| 🔴 ✅ **判据按 A5-U3 重写（旧措辞「被拒（非仅仅改道）」已作废，它夸大了 A5 的射程）**：① **路由不再指向旧化身** —— 旧化身节点的心跳**抢不回** binding（内存 + Redis 两个实现，且必须在 **HA / query-only 副本**形态下验证）；② **仲裁确定** —— 同 `sandboxID` 冲突稳定选 v7 较大的那条（连跑 20 次一致），`GET /v2/sandboxes` 去重同理；③ **闸 1/闸 2 已装配且可自证** —— node 始终回声 `x-agentenv-execution-id`；gateway 的 `unfenced_node_silent` 与 scheduler 的 `lookup_execution_authority_total{authority="unknown"}` **逐条对得上**（对不上就是有一侧算错了）；④ **拒绝形状正确** —— 一旦真的产生拒绝，HTTP 码是 **409** 且 **≠ 404、≠ 410、≠ 503**；⑤ **成功证据是路由指标，不是拒绝率** —— `binding_execution_total{decision="rejected_older"}` 在健康集群恒 0，`registry_execution_mismatch` 恒 0；🔴 **闸 1 的真阳性率为 0 不算失败**（A5-U3）。⚠️ **已建立的长连接本轮不兑现**（闸 3 推迟，已登记）| ① 退回"只按 sandbox 路由" ⇒ 拒绝用例必须 FAIL；② 🔴 把拒绝码改成 404 ⇒ **"平台不得把化身过期读成沙箱不存在"用例必须 FAIL** |
| **A6** | 外部**只读**暴露 execution（`GET /sandboxes/{id}`、resume 响应）| 字段存在且与内部一致；**不作为入参** | —— |

**A3 的 SQL 事务内校验为什么是硬要求**：抄 e2b 的教训 ——
`storage/redis/scripts.go:33-39` 明写 enforcement 必须在 Lua 内而不是 Go 侧，
因为 `Add is lockless, so a resume can install a new incarnation between a Go-side comparison
and this write`。**Go 侧"先查后写"之间就是 resume 插队的窗口。**

**🔴 A4 不能靠关端口 / NetworkPolicy 实现**：控制面与数据面**共用一个 listener**
（`src/api/server.rs:27-29`，`API_ADDR` 默认 `0.0.0.0:8000`），数据面是 `.fallback()` 兜底
（`src/api/proxy.rs:148`）⇒ **只能在路由层做，或拆端口**。
另外 dev 集群实测**零 NetworkPolicy**，AgentENV 自己文档写的
"protected by the network"（`docs/src/deployment/kubernetes.md:174-199`）**那张网根本没铺**。

**🔴 A4 的网络收窄对 R5 的 #2 / #4（TTL 自动 pause、关机批量 pause）完全无效** ——
它们不需要任何外部请求。⇒ **唯一解是 A3 的 SQL 层校验，这坐实了 A3 必须先于一切。**

**随 A2 / A3 顺手清掉**：proto 里残留的 `TRANSITION_KIND_REMOVE_UNCONDITIONAL`
（服务端已 fail-closed 拒服务，`registry_service.go:325-332`）—— 重画 RPC 面时把枚举值一并删掉。

---

## 4. 批次 B：语义上收本体（A3 验证通过后才许放宽"永不可抢"）

| # | 事 | 关键约束 / 验收 |
|---|---|---|
| **B1** | 中央 poll 取代节点自续租 | `answered` 与 `sync_ok` **分开判**；`RenewNodeLease` RPC 退役；`sync_ok` 指标此时才可得（阶段 0 曾因 heartbeat 单向推而不可得）|
| **B2** | evictor 与 reclaim **分离**（阶段 3-A）| 两个都要、不能合并。reclaim 的「租约过期 **且** deadline 过期」**双条件不许放宽成单条件** |
| **B3** | 孤儿回收 | `Reconcile(roster) → KillOrphan` + grace period + §3.3 熔断；**按 execution 判**（§3.7）。变异：改成按 sandboxID 判 ⇒ "同 ID 异节点重建后旧化身被杀"用例必须 FAIL |
| **B4** | 中央 placement | resume = controller 选节点 + 下发 Create with snapshot；`AcquireSandbox` 变内部调用；三分法（§3.4）显式建模。🔴 R2：execution 最终应由 **controller** 铸（A1 先落在节点侧是过渡形态，B4 要把铸造点搬走）|
| **B5** | 显式状态机 | `AllowedTransitions` + `TransitionEffect` + 结构化 `KillReason` |
| **B6** | RPC 面重写 | node→controller = 上报事实；controller→node = **下发命令**。阶段 2 的 5 RPC 形状直接丢（P1 允许）。🔴 **§3.6b「发布权集中」在这里兑现** —— R1 证明 controller 今天连下行通道都没有（`scheduler.proto:387-401` 全是 node 当 client），B6 的新增命令面是它的前提；R5 的 9 条节点自主路径也只有到这一步才谈得上"根本不存在" |
| **B7** | `release_node_holdings` **保留** | 除非能论证「node id 永不复用」且「新 instance_id 与老进程死亡无可观测重叠窗口」—— 它是系统里唯一能**证明**而非推断 VM 已死的证据。🔴 **A1 不替代它**：R2 实证节点崩溃时 execution 侧**零信号**（`client.rs:336-347` 的 Drop 兜底只在进程活着时有效）|

---

## 5. 批次 C：随行收口

- **T3 F6**：首个续租周期内失联的节点留**永久孤儿行**（`sandbox_expires_at` 为 NULL 永不匹配 reclaim）—— 随 B1 消掉
- **S 系列**（随 B6 重写消化）：
  - **S4** 响应带"本次实际覆盖的 id 集合" —— 护栏 §3.1 目前**唯一**无机械保证的一环，而缺行的下游反应是**删用户工作区**
  - **S2** release 命中 0 行要可计数（中央化后它是"节点报的 generation 已过期"的唯一信号）
  - **S5** 读路径带 DB 时钟（`Get`/`GetMany` 目前拿不到，只能用进程墙钟 —— 正是文档禁止的）。
    🔴 **留在 AgentENV 侧**：这是接口缺口，与 Console 是否平移无关（Console 的同类缺陷 C2 见 §1.4，本轮不做）
  - **S7** `MarkRunning` 的 bool 拆成两个事实（"没跟踪" vs "是别人的"，调用方两个都要）
  - **S3** 节点 `reconcile_interval` 校验归属（`ttl ≥ 3×interval` 目前无处校验）
- ~~**D8**：`Code::Aborted ⇒ GenerationConflict`~~ ✅ **已完成，划掉**（R3 实证：Rust
  `src/orchestrator/paused_registry/central.rs:432` + Go `registry_service.go:688` 映射齐备，
  另有负向测试 `central.rs:2090-2126`）
- **T2 N3**：`invalid_rows` 至今零分辨力 —— 授权造一行 `paused` + `snapshot_id IS NULL` 验一次即删
- **（新）proto 清理**：删 `TRANSITION_KIND_REMOVE_UNCONDITIONAL`（服务端已 fail-closed，`registry_service.go:325-332`），随 A2/A3 重画 RPC 面一并做

---

## 6. 总发布 runbook（N5：三份设计的顺序合成一条）

> 🔴 **2026-08-19 · 本节整节重写（命名裁决 N5）。**
> 三份设计各自写了发布顺序，**三条都对但方向不同**：
> A1/A3（scheduler §5.1）要 **node 先**（旧 controller 忽略未知字段）；
> A4（node §3.2）要 **gateway 先**（否则 gateway→node 的调用先被自己拒掉）；
> A5（gateway §10 / scheduler-a5 §4.2）是 **node → scheduler → gateway observe → enforce**。
> 依赖方向不同，不矛盾 —— 但**必须线性化**，否则发布日一定有人按其中一份文档做而打断另一条。
>
> 🔴 **执行以本节为准**；各设计文档里的局部顺序段保留为"这一项自己的依赖方向"，是本节的推导材料，不是执行清单。
> 🟡 **适用范围**：dev（VM 203/204）与 test（VM 201/202）两套 k3s 集群，**各完整跑一遍**。
> AgentENV 尚未上生产（P1），本 runbook 含**破坏性步骤与服务窗口**，只在这两套集群执行。

---

### 6.0 🔴 本轮的部署方式：**定点更新，不走 `make k8s-apply`**（不许抹掉集群侧的 out-of-band 配置）

> **2026-08-20 新增。集群验证开工前的实地侦察产物，`pve-sg dev`（VM 203/204）逐条实测。**
> 原文（§6.2 表头）写的是"所有部署动作一律走 `make k8s-apply`"，**已在 §6.2 就地订正并保留原表述**。

#### 6.0.1 判决与理由

**判决：本轮 dev / test 两套 k3s 集群一律走定点更新（`kubectl patch` / `kubectl set image` + 显式建 CM/Secret），不跑 `make k8s-apply`。**

理由不是 `run.sh` 有毛病，而是**这两套集群与仓内清单已经长期漂移**：快照 OSS 后端、
`AENV_PAUSED_REGISTRY_BACKEND=central`、带 registry 前缀的镜像引用、regctl 挂载 ——
全部是 out-of-band 的（清单见 6.0.2）。一次全量 apply 会把它们**一起抹掉，而且大部分抹掉后不报错**，
症状分别是"快照不再落 OSS"、"中央登记表关掉"、"模板镜像拉不动"、"三个工作负载 ImagePullBackOff"。

🟡 **这些漂移一条都不是阶段 3 引入的** —— 它们比本轮早。所以本轮的处置是：

- **不消灭它们**。要消灭就得把 RustFS 的端点与凭据搬进仓库，超出本轮范围且危险（凭据入库）。
- **登记 + 保护**：漂移清单写在 6.0.2，每一步执行完都跑一遍 6.0.5 的复核探针确认它们还在。
- **单开一条技术债**记「部署清单与集群实际长期漂移」这件事本身，见 [§12.5](#125-部署清单与集群实际长期漂移本轮只登记不治)。

🔴 **`make k8s-render` 本轮照用**（只渲染不 apply）：定点补丁的内容就是从它的输出里摘出来的，
这样补丁不会与仓内清单漂移。**只有 `apply` 被禁**。

#### 6.0.2 🔴 out-of-band 漂移清单（`pve-sg dev`，2026-08-20 实测）

> §6.5 此前**只点名了 30800 一条**，那是不完整的。完整清单在这里，§6.5 的 R3 已改为引用本表。

| # | 集群里 out-of-band 的东西 | 仓内清单说的是什么 | 🔴 被 apply 抹掉后的症状 |
|---|---|---|---|
| **D-1** | CM `agentenv-k8s-config` 的 `agentenv.toml`：`repository_backend = "oss"` + 整段 `[backend.oss]`（RustFS 端点 + 凭据） | `config/default.toml` 是 `posix_fs`，无 `[backend.oss]` | 🔴 **静默**：`run.sh:30` 每次 apply 从 `config/default.toml` 重生成该 CM（`disableNameSuffixHash: true` ⇒ 同名覆盖）⇒ 快照不再落 OSS，pause 全退化 `local_only`，**不报错** |
| **D-2** | 同一份 toml 的缓存预算：`image.cache.capacity_gb=24` / `remote_blocks.max_size_gb=12` / oss cache（合 44G，**按 master 96G 根盘定尺**） | `100` / `100`（合 200G+） | 🟡 预算回到 200G+ 而 master 只有 ~90G 可用 ⇒ GC 高水位（capacity 的 95%）**永远触发不到**，盘先满 |
| **D-3** | 同一份 toml 的 `[orchestrator.paused_registry] backend = "postgres"` —— **一个新版已经删掉的值** | `backend = "local"` | 🔴 **看它跟 D-4 一起丢还是单独丢**：<br>① D-3+D-4 一起（= 一次完整 apply）⇒ 静默回落 `local`，中央登记表关掉、不报错；<br>② **只丢 D-4、CM 还是老的** ⇒ 新 node 读到 `postgres` 会**拒绝启动**（`src/orchestrator/paused_registry/mod.rs` 的 `bail!`，两节点一起 CrashLoop）。今天靠 D-4 的 env 压着，**env 赢文件**（confique：env > file > default；实测节点日志 `paused sandbox registry ready backend="central"`） |
| **D-4** | DS `agentenv-node` 的 `env AENV_PAUSED_REGISTRY_BACKEND=central`（**硬写字面值**） | `configMapKeyRef: paused-registry-config`（`optional: true`），而该 CM **在 dev 集群不存在**（实测 `NotFound`） | 🔴 **静默**：apply 后该 env 变成"引用一个不存在 CM 的 optional key" ⇒ env 消失 ⇒ 与 D-3 合流成"中央登记表悄悄关掉" |
| **D-5** | DS 的 `env HOME=/root` + volumeMount `regctl-config` → `/root/.regctl/config.json` + CM `regctl-config`（内容：`10.10.10.204:5000` 走明文 HTTP） | 仓内 DS **完全没有** regctl 的 env / volume / volumeMount | 🔴 apply 不 prune ⇒ **CM 还在**，但 **DS 的挂载被抹掉** ⇒ node 上拉模板镜像失败（症状出现在建沙箱时，不在 apply 时）|
| **D-6** | DS 的 `env AENV_PAUSED_REGISTRY_DSN` ←`Secret agentenv-postgres/dsn` | 仓内 DS 已删（D11 把 PG 从 node 摘除） | 🟢 抹掉无害，新 node 根本不读它。**登记它只是为了排错时别把它当成"新版还在直连 PG"的证据** |
| **D-7** | 三个工作负载的 image 全是 `10.10.10.204:5000/agentenv-*:<不可变 tag>` | `deploy/k8s/base/kustomization.yaml` 的 `images:` 钉成 `agentenv-{gateway,scheduler,runtime}:latest`（**无 registry 前缀**），且 `run.sh` **没有镜像覆盖入口**（grep 零命中） | 🔴 apply 后三个工作负载引用 `docker.io/library/agentenv-*:latest` ⇒ **ImagePullBackOff**。🔴 更阴的是 203 的 containerd 里那份 `…:latest` 是**脏的**（R3 §1.3）⇒ 配合 `IfNotPresent` 有可能**不报错地跑旧代码** |
| **D-8** | Service `agentenv-gateway-nodeport`（NodePort **30800**） | 仓内**没有这个文件**（`gateway-service.yaml` 只有 ClusterIP `8080` + `9102`） | 🟡 apply **不删也不建**它（不 prune）。危险动作是 `run.sh delete` 之后再 apply ⇒ 30800 没了，平台侧全线打不通。**备份已在步骤 1 落盘** |
| **D-9** | `agentenv-postgres`（StatefulSet + Service + Secret）、RustFS 全套、`agent-console` | 仓内无清单（RustFS 在**主仓** `deploy/agentenv-sg/rustfs.yaml`；PG 与 agent-console 只活在集群里）| 🟡 apply 不动它们。登记是因为**重建集群会漏**（R3 的 B3）|
| **D-10** | scheduler 的 `SCHEDULER_REGISTRY_CLUSTER_ID` ← `Secret agentenv-postgres/cluster_id` | 仓内改成 ← CM `cluster-identity-config/CLUSTER_ID`（本轮新增的 CM，集群里还没有）| 🟢 **唯一良性的一条**：两处的值**实测相同**（都是全零 UUID，且与 toml 的 `[node_identity].cluster_id`、节点日志里的 `cluster_id=00000000-…` 三方一致）⇒ 本轮**不需要**创建 `cluster-identity-config` |

🔴 **抹掉这十条里的大多数，`kubectl` 都会回你一句 `configured`。**"apply 成功"与"apply 把这套集群拆了"在终端上是同一行字。

#### 6.0.3 本轮要**显式创建**的资源（仓内清单里有，集群里没有）

实测四个都是 `NotFound`：`paused-registry-config` / `execution-fencing-config` / `cluster-identity-config` / `secret agentenv-control-plane-token`。其中：

| 资源 | 本轮建吗 | 为什么 |
|---|---|---|
| CM `execution-fencing-config` | ✅ **必须建**（步骤 4 之前） | 三个开关都靠它注入。🔴 不建 ⇒ 三个 env 全落空 ⇒ **回落代码默认值**，而代码默认是**终态**（`write_fencing=true` / `arbitration=enforce` / gateway `enforce`）⇒ 步骤 4 直接跳过 observe 期，步骤 5/6/10/11 的闸门全部失去意义 |
| Secret `agentenv-control-plane-token` | ✅ **分两次建**（步骤 8 建 key `token`，步骤 9 补 key `node-gate-token`）| 见 6.0.4 步骤 8/9 与 §6.0.6 |
| CM `cluster-identity-config` | ❌ **不建** | D-10：集群侧已从 `Secret agentenv-postgres/cluster_id` 拿到同一个全零 UUID，node 侧从 toml 落到同一个值。建它不会错，但会多一份需要跟另外两处保持一致的真相源 |
| CM `paused-registry-config` | ❌ **不建** | D-4：DS 里已硬写 `central`。定点更新不碰这条 env，建这个 CM 只是给"以后某次 apply"埋一个看起来没问题的伏笔 |

#### 6.0.4 🔢 定点更新命令序列（可直接执行；步骤号对齐 §6.2 步骤表）

```bash
# ── 公共变量（每个新 shell 都要重设）
export KUBECONFIG=~/.kube/config-aenv-sg
NS=agentenv-system
REG=10.10.10.204:5000
TAG=cp3-bff4993                 # 🔴 不可变 tag，绝不用 :latest（R3 §3.3：203 的 :latest 是脏的）
GW=http://10.10.10.203:30800

# 容器名（已查实，patch/set image 必须写对，写错是静默 no-op）
#   ds/agentenv-node          → 容器名 agentenv
#   deploy/agentenv-gateway   → 容器名 gateway
#   deploy/agentenv-scheduler → 容器名 scheduler

# 三个镜像已构建并推到 registry（实测 tags/list 里都有 cp3-bff4993），且已预拉到两节点 containerd
```

---

**步骤 3 —— node（🔴 全程唯一一次滚 DaemonSet，所以 image 与四处新增必须在同一个 patch 里）**

🔴 **不能只 `set image`**。集群里那份 DS 是漂移版，**缺**新版 node 需要的四样东西
（实测 `live-ds` 里全部不存在）：

| 缺什么 | 不补的后果 |
|---|---|
| `env AENV_API_CONTROL_PLANE_TOKEN_FILE` | 🔴 **步骤 9 直接做不成** —— gate 没有文件可读，A4 永远启用不了 |
| volume + volumeMount `control-plane-token` | 同上 |
| preStop 脚本里的 `x-agentenv-control-plane` 头 | 🔴 步骤 9 之后 preStop 的 `POST /nodes/{id}` 被 gate 拒 403，而脚本 `|| echo` 把失败吞掉 ⇒ **节点静默不再 drain**，沙箱继续被排到一台正在关机的机器上 |
| image | —— |

🔴 **分两次做 = 滚两次 DaemonSet = 两次全集群 pause 风暴（§6.1）。必须一个 patch。**
补丁**从 `make k8s-render` 的输出里摘**，这样它不会与仓内清单漂移：

```bash
cd <AgentENV 仓库根>
bash deploy/k8s/run.sh render > /tmp/aenv-cv/rendered-$TAG.yaml    # 🔴 render，不是 apply

python3 - "$REG" "$TAG" <<'PY' > /tmp/aenv-cv/patch-node.yaml
import sys, yaml
reg, tag = sys.argv[1], sys.argv[2]
docs = [d for d in yaml.safe_load_all(open(f"/tmp/aenv-cv/rendered-{tag}.yaml")) if d]
ds = next(d for d in docs if d["kind"] == "DaemonSet")
spec = ds["spec"]["template"]["spec"]
c = spec["containers"][0]
assert c["name"] == "agentenv", c["name"]
env = [e for e in c["env"] if e["name"] == "AENV_API_CONTROL_PLANE_TOKEN_FILE"]
vm  = [m for m in c["volumeMounts"] if m["name"] == "control-plane-token"]
vol = [v for v in spec["volumes"] if v["name"] == "control-plane-token"]
# 自证：三样都必须摘到，摘不到就是仓内清单变了，停下来看
assert len(env) == 1 and len(vm) == 1 and len(vol) == 1, (env, vm, vol)
assert vol[0]["secret"].get("items"), "node 卷必须只投影 node-gate-token（见 §6.0.6）"
print(yaml.safe_dump({"spec": {"template": {"spec": {
    "containers": [{
        "name": "agentenv",
        "image": f"{reg}/agentenv-runtime:{tag}",
        "env": env,
        "volumeMounts": vm,
        "lifecycle": {"preStop": c["lifecycle"]["preStop"]},
    }],
    "volumes": vol,
}}}}, allow_unicode=True, sort_keys=False))
PY

# 🔴 先肉眼过一遍补丁：它只应含 image / 一条 env / 一条 volumeMount / preStop / 一个 volume
cat /tmp/aenv-cv/patch-node.yaml

kubectl -n "$NS" patch ds/agentenv-node --type=strategic --patch-file /tmp/aenv-cv/patch-node.yaml
kubectl -n "$NS" rollout status ds/agentenv-node --timeout=900s
```

> 🟢 **为什么 strategic merge 不会误删 out-of-band 的东西**：`env` 按 `name` 合并、
> `volumeMounts` 按 `mountPath` 合并、`volumes` 按 `name` 合并、`containers` 按 `name` 合并
> （都是 k8s API 类型自带的 `patchMergeKey`）⇒ D-4 的 `AENV_PAUSED_REGISTRY_BACKEND`、
> D-5 的 `HOME` 与 regctl 挂载、D-6 的 DSN env **全部原样保留**。
> 🔴 `lifecycle` 不是列表，是**整体替换**——所以 preStop 必须从 render 里整段摘，不能手抄。
> **patch 完立刻跑 6.0.5 的复核**，用命令确认上面这句是真的，而不是相信它。

---

**步骤 4 —— scheduler（一次 patch：image + 两个开关 env）**

🔴 集群里那份 scheduler **没有** `SCHEDULER_REGISTRY_WRITE_FENCING` / `SCHEDULER_ROUTING_EXECUTION_ARBITRATION`
（实测只有 `SCHEDULER_REGISTRY_DSN` / `SCHEDULER_REGISTRY_CLUSTER_ID` / `SCHEDULER_REGISTRY_WRITE_ENABLED`）。
**不补 env ⇒ 回落代码默认 `enforce` ⇒ 步骤 4 一起来就是 enforce，步骤 5/6 的 observe 闸门形同虚设。**

```bash
# 4.0 先建开关 CM（🔴 必须在 patch 之前，否则 Pod 起来时 optional key 落空 = 回落终态默认值）
kubectl -n "$NS" create configmap execution-fencing-config \
  --from-literal=SCHEDULER_REGISTRY_WRITE_FENCING=true \
  --from-literal=SCHEDULER_ROUTING_EXECUTION_ARBITRATION=observe \
  --from-literal=GATEWAY_ROUTING_EXECUTION_FENCING=off
kubectl -n "$NS" get cm execution-fencing-config -o jsonpath='{.data}{"\n"}'   # 三个键都要在

# 4.1 一次 patch：image + 两条 env
kubectl -n "$NS" patch deploy/agentenv-scheduler --type=strategic -p "$(cat <<EOF
spec:
  template:
    spec:
      containers:
        - name: scheduler
          image: ${REG}/agentenv-scheduler:${TAG}
          env:
            - name: SCHEDULER_REGISTRY_WRITE_FENCING
              valueFrom:
                configMapKeyRef: {name: execution-fencing-config, key: SCHEDULER_REGISTRY_WRITE_FENCING, optional: true}
            - name: SCHEDULER_ROUTING_EXECUTION_ARBITRATION
              valueFrom:
                configMapKeyRef: {name: execution-fencing-config, key: SCHEDULER_ROUTING_EXECUTION_ARBITRATION, optional: true}
EOF
)"
kubectl -n "$NS" rollout status deploy/agentenv-scheduler --timeout=300s

# 4.2 自证：env 真进去了、值是 observe 而不是 enforce
kubectl -n "$NS" exec deploy/agentenv-scheduler -- env 2>/dev/null | grep -a "^SCHEDULER_ROUTING_EXECUTION_ARBITRATION=" || \
kubectl -n "$NS" logs deploy/agentenv-scheduler --tail=200 | grep -a "arbitration"
# 指标口径（权威）：agentenv_scheduler_routing_execution_arbitration_enabled == 1（observe）/ 2（enforce）
```

> 🟡 **步骤 6 翻 `enforce`**：改 CM 之后**必须滚 Deployment**，见 §6.0.7。
> ```bash
> kubectl -n "$NS" patch cm execution-fencing-config --type=merge \
>   -p '{"data":{"SCHEDULER_ROUTING_EXECUTION_ARBITRATION":"enforce"}}'
> kubectl -n "$NS" rollout restart deploy/agentenv-scheduler
> kubectl -n "$NS" rollout status  deploy/agentenv-scheduler --timeout=300s
> ```

---

**步骤 8 —— gateway（一次 patch：image + 两条 env）+ 建 Secret 的 key A**

🔴 集群里那份 gateway **只有** `GATEWAY_SANDBOX_PROXY_DOMAINS` 一条 env ——
`GATEWAY_CONTROL_PLANE_TOKEN` 与 `GATEWAY_ROUTING_EXECUTION_FENCING` **都没有**。
只 `set image` 的话：注入不会发生（步骤 8 的验收直接挂），fencing 回落代码默认 `enforce`（跳过步骤 10 的 observe）。

```bash
# 8.0 🔴 只建 key A（gateway 的那半）。key B 留到步骤 9 —— 这是顺序约束能成立的全部原因，见 §6.0.6
# 🔴 凭据只在这一条命令里出现一次，之后一律从 Secret 里取（含步骤 9 的 key B）
kubectl -n "$NS" create secret generic agentenv-control-plane-token \
  --from-literal=token="$(head -c 48 /dev/urandom | base64 | tr -d '=+/\n' | head -c 40)"
# 🔴 单行内联，不落变量 —— 别写成 TOKEN=... 再引用：那样它会留在 shell 历史与环境里

# 自证：此刻 Secret 只有一个键，node 侧那半还不存在
kubectl -n "$NS" get secret agentenv-control-plane-token -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}'
# 期望恰好一行：token   （出现 node-gate-token = 顺序已经破了，见 §6.0.6）

# 8.1 一次 patch：image + 两条 env
kubectl -n "$NS" patch deploy/agentenv-gateway --type=strategic -p "$(cat <<EOF
spec:
  template:
    spec:
      containers:
        - name: gateway
          image: ${REG}/agentenv-gateway:${TAG}
          env:
            - name: GATEWAY_CONTROL_PLANE_TOKEN
              valueFrom:
                secretKeyRef: {name: agentenv-control-plane-token, key: token, optional: true}
            - name: GATEWAY_ROUTING_EXECUTION_FENCING
              valueFrom:
                configMapKeyRef: {name: execution-fencing-config, key: GATEWAY_ROUTING_EXECUTION_FENCING, optional: true}
EOF
)"
kubectl -n "$NS" rollout status deploy/agentenv-gateway --timeout=300s

# 8.2 🔴 node 侧此刻必须还是全放行（对照面：证明建 Secret 没顺手点亮 node）
kubectl -n "$NS" logs -l app.kubernetes.io/name=agentenv-node --tail=200 --prefix \
  | sed 's/\x1b\[[0-9;]*m//g' | grep -a "no control-plane credential is configured" 
# 或直接读 gauge：agentenv_api_control_plane_gate_enabled 必须为 0
```

---

**步骤 9 —— 启用 A4：给 Secret 补上 key B（🔴 唯一一步真热生效，不滚 DaemonSet）**

```bash
# 🔴 key B 的值从 key A 里派生，不许手敲 —— 敲错一个字符 = 平台每一个请求 403
kubectl -n "$NS" patch secret agentenv-control-plane-token --type=merge -p "$(python3 - <<'PY'
import base64, json, subprocess
raw = subprocess.check_output([
    "kubectl","-n","agentenv-system","get","secret","agentenv-control-plane-token",
    "-o","jsonpath={.data.token}"])
print(json.dumps({"data": {"node-gate-token": raw.decode()}}))   # 逐字节同一份 base64，不解码不重编
PY
)"

# 自证 ①：两个键的 base64 逐字节相同（不打印明文）
kubectl -n "$NS" get secret agentenv-control-plane-token \
  -o jsonpath='{.data.token}{"\n"}{.data.node-gate-token}{"\n"}' | uniq | wc -l   # 必须是 1

# 自证 ②：🔴 Pod 没重启（这就是走文件不走 env 的全部理由）
kubectl -n "$NS" get pod -l app.kubernetes.io/name=agentenv-node \
  -o custom-columns=NAME:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp
# 与步骤 3 之后记下的那份逐行相同。Secret 卷刷新有 ≤60s 延迟，等 gate 指标翻 1 再验，别急着重试

# 自证 ③：gate 真的开了
#   agentenv_api_control_plane_gate_enabled 由 0 → 1
#   然后跑 §6.6 的 P-A4-1 / P-A4-2 / P-A4-3
```

🔴 **步骤 9 的回退不是"删掉 key B"，是"把 key B 写成空串"**：

```bash
kubectl -n "$NS" patch secret agentenv-control-plane-token --type=merge \
  -p '{"stringData":{"node-gate-token":""}}'
```

理由见 §6.0.6 的"key 缺失时行为"——**删键之后文件消失，而 node 对"读失败"是刻意保留上一个 good 值的**，
gate 会**继续开着**，回退看起来做了、其实没做。空串是一次**成功读到零个凭据**，那才是设计好的关闭开关。

---

**回滚（任何一步）**

```bash
kubectl -n "$NS" rollout undo deploy/agentenv-gateway      # 回上一个 ReplicaSet
kubectl -n "$NS" rollout undo deploy/agentenv-scheduler
kubectl -n "$NS" rollout undo ds/agentenv-node             # 🔴 又一次滚 = 又一次 pause 风暴，先 drain
# 或显式钉回当前已知好版本（实测集群现值）：
#   ds/agentenv-node          10.10.10.204:5000/agentenv-runtime:d11-9a8fd88
#   deploy/agentenv-scheduler 10.10.10.204:5000/agentenv-scheduler:d11-9a8fd88
#   deploy/agentenv-gateway   10.10.10.204:5000/agentenv-gateway:cp1-c35f5ec
```

#### 6.0.5 🔴 每一步做完都要跑的「out-of-band 还在吗」复核（带对照面）

> 探针必须先自证（§8.3）。下面每条都配了一个**必然为假的对照输入**：对照不返回"假"的那一刻，
> 说明这条探针没有分辨力，它给出的"还在"是无意义的。

```bash
export KUBECONFIG=~/.kube/config-aenv-sg; NS=agentenv-system

# ① D-1：OSS 后端与 [backend.oss] 段还在（🔴 只数行，不打印内容 —— 那段里有 RustFS 凭据）
kubectl -n "$NS" get cm agentenv-k8s-config -o jsonpath='{.data.agentenv\.toml}' | grep -c '^repository_backend = "oss"'   # 期望 1
kubectl -n "$NS" get cm agentenv-k8s-config -o jsonpath='{.data.agentenv\.toml}' | grep -c '^\[backend\.oss\]'              # 期望 1
#    对照面（必然不存在的串）：
kubectl -n "$NS" get cm agentenv-k8s-config -o jsonpath='{.data.agentenv\.toml}' | grep -c '^repository_backend = "no_such_backend"'  # 必须 0

# ② D-4：中央登记表开关还在
kubectl -n "$NS" get ds agentenv-node -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="AENV_PAUSED_REGISTRY_BACKEND")].value}{"\n"}'   # 期望 central
#    对照面（不存在的 env 名）：下面必须打印空行，否则这条 jsonpath 是在瞎匹配
kubectl -n "$NS" get ds agentenv-node -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="AENV_NO_SUCH_ENV")].value}{"\n"}'
#    🔴 运行时口径（比 spec 更硬，spec 对了不代表进程读到了）：
kubectl -n "$NS" logs -l app.kubernetes.io/name=agentenv-node --tail=2000 --prefix | sed 's/\x1b\[[0-9;]*m//g' \
  | grep -a 'paused sandbox registry ready'     # 每台都必须是 backend="central"，不是 "local"

# ③ D-5：regctl 挂载与 HOME 还在
kubectl -n "$NS" get ds agentenv-node -o jsonpath='{.spec.template.spec.containers[0].volumeMounts[?(@.name=="regctl-config")].mountPath}{"\n"}'  # /root/.regctl/config.json
kubectl -n "$NS" get ds agentenv-node -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="HOME")].value}{"\n"}'                        # /root

# ④ D-7：三个镜像都带 registry 前缀 + 不可变 tag
for r in ds/agentenv-node deploy/agentenv-gateway deploy/agentenv-scheduler; do
  kubectl -n "$NS" get "$r" -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
done
#    🔴 三行都必须以 10.10.10.204:5000/ 开头。出现裸 agentenv-*:latest = 有人 apply 过，立刻停下
#    🔴 换完镜像要比 imageID，不是比 image 名（203 的 :latest 是脏的）：
kubectl -n "$NS" get pod -l app.kubernetes.io/name=agentenv-node -o jsonpath='{range .items[*]}{.spec.nodeName}{" "}{.status.containerStatuses[0].imageID}{"\n"}{end}'

# ⑤ D-8：30800 还在
kubectl -n "$NS" get svc agentenv-gateway-nodeport -o jsonpath='{.spec.ports[0].nodePort}{"\n"}'   # 30800
#    没了就用步骤 1 的备份：kubectl apply -f /tmp/aenv-cv/nodeport-30800.bak.yaml
```

#### 6.0.6 🔴 Secret 拆成两个 key：为什么，以及"key 缺失时到底发生什么"

**问题**：`GATEWAY_CONTROL_PLANE_TOKEN`（gateway 的 env）与 node 的 `/etc/agentenv/control-plane/token`
（挂载卷）原本**共用一个 Secret 且共用同一个 key** ⇒ **创建这个 Secret 这一个动作，会同时点亮两侧**。
于是步骤 8→9 那条"gateway 先注入、node 后开 gate"的顺序约束**形同虚设**：Secret 一建，
node 的 gate 立刻开始生效，而 gateway 的新 Pod 还没滚完 —— 正好是**唯一不许出现的那个顺序**
（node 拒掉一切平台流量）。

**改法（已落地仓内清单）**：一个 Secret、两个 key。

| 谁 | 读哪个 key | 怎么读 | 在哪一步被点亮 |
|---|---|---|---|
| gateway | `token` | `secretKeyRef`（env） | **步骤 8** 建 Secret |
| node | `node-gate-token` | 卷投影成文件 `token`（`items:` + `optional: true`） | **步骤 9** `kubectl patch` 补这个 key |

- `deploy/k8s/base/agentenv-daemonset.yaml`：`control-plane-token` 卷加 `items: [{key: node-gate-token, path: token}]`，`optional: true` 保留；挂载路径与 `AENV_API_CONTROL_PLANE_TOKEN_FILE` 都**不变**。
- `deploy/k8s/base/gateway-deployment.yaml`：仍读 `key: token`，注释改写说明两半的关系与顺序。
- 🔴 不变式由测试钉住：`src/api/control_plane_gate.rs` 的
  `the_gateway_and_the_node_read_different_keys_of_the_credential_secret`
  （删掉 `items:` 那四行 ⇒ 测试变红，已用变异验证过分辨力）。

**为什么不用"两个独立 Secret"**：两侧必须是**同一个凭据**，而两个对象之间的"值相等"没有任何东西看得住；
放在一个对象里，`kubectl get secret -o yaml` 一眼能比，而且步骤 9 的命令可以**直接从 key A 派生 key B**
（见 6.0.4），从构造上消掉"两边不一致"这个失败模式。改动面也更小：只动 node 卷的四行。

##### 🔴 查实：`items` 里的 key 不存在时，卷会怎样

**依据一 · Kubernetes 自己的 API 契约**（read-only 取自本集群的 OpenAPI，
`kubectl explain daemonset.spec.template.spec.volumes.secret.items`）：

> "If a key is specified which is not present in the Secret, **the volume setup will error unless it is marked optional**."
> 以及 `…volumes.secret.optional`："optional field specify whether **the Secret or its keys** must be defined"。

⇒ 卷上已经有的 `optional: true` **同时覆盖"Secret 不存在"与"列出的 key 不存在"**两种情况：
**不报错、不阻塞 Pod 启动、该文件干脆不出现**。这就是本方案要的确定行为。

**依据二 · node 侧对"文件不存在"的处置**（`src/api/control_plane_gate.rs` 的 `file_tokens()`）：

| 情形 | 代码走哪条路 | gate 结果 |
|---|---|---|
| 文件**从未**被成功读过（key 一直没给 / 卷没挂上） | `read_to_string` 失败 → `state.tokens` 还是 `None` → 返回空 | 🟢 **关**（`GateDecision::Disabled`，等价于 gate 出现之前的行为）|
| 文件**曾被成功读过**，之后消失（删 key、卷抽走、磁盘抖） | `read_to_string` 失败 → `state.tokens` 是 `Some(...)` → **保留上一个 good 值** | 🔴 **仍然开着**，并打一条 warn |
| 文件存在且**内容为空** | 读**成功**，得到零个凭据 | 🟢 **关**（这是设计好的关闭开关）|

⇒ 上一轮的裁决"读失败保留上一个 good 值、空文件是有意的关闭开关"，
**"文件不存在"落在两边都有**：**第一次成功读之前**落在"关"这一边（所以步骤 8 安全），
**成功读过之后**落在"保留上一个 good 值"那一边（所以**步骤 9 的回退必须写空串，不能删 key**）。
🔴 这一条不写清楚，步骤 9 的回退就是不可靠的 —— 而它失败的方式是**静默的**：删完 key，`kubectl` 说 patched，gate 还开着。

已有的两个测试就是这条结论的可执行版本，本轮**未改动**它们：
`src/api/server.rs` 的 `the_gate_picks_up_a_token_written_after_startup`（T-A4-8：没挂上 ⇒ 放行；写入 ⇒ 拒；清空 ⇒ 放行）
与 `an_unreadable_token_file_keeps_the_last_known_value`（T-A4-9：**删文件 ⇒ 仍然拒**；写空 ⇒ 放行）。

#### 6.0.7 🔴 哪几步要滚服务，哪一步是真热生效（别把两种机制混着记）

| 步骤 | 动作 | 机制 | 要滚吗 |
|---|---|---|---|
| 3 | node：image + token 文件接线 + preStop | DaemonSet pod template | 🔴 **滚 DaemonSet（全程唯一一次）** |
| 4 | scheduler：image + 两条开关 env | Deployment pod template | ✅ 滚 Deployment（patch 自带） |
| 6 | `SCHEDULER_ROUTING_EXECUTION_ARBITRATION` → `enforce` | 🔴 **`configMapKeyRef` 注入的 env，不会热刷新** | 🔴 **必须 `rollout restart deploy/agentenv-scheduler`**。只改 CM 不滚 = 什么都没发生，而指标会照旧显示 observe —— 看起来像"翻了没生效"，其实是"根本没翻" |
| 8 | gateway：image + 两条 env（+ 建 Secret key A） | Deployment pod template | ✅ 滚 Deployment（patch 自带） |
| **9** | **node gate 开启：Secret 补 key B** | 🔴 **挂载卷里的文件，进程按请求重读** | 🟢 **真热生效，绝不许滚 DaemonSet**（滚一次 = 一次全集群 pause 风暴，§6.1）。kubelet 刷新卷有 **≤60s** 延迟，等指标翻 1，别急着重试 |
| 10 / 11 | `GATEWAY_ROUTING_EXECUTION_FENCING` → `observe` / `enforce` | 🔴 **`configMapKeyRef` 注入的 env，不会热刷新** | 🔴 **必须 `rollout restart deploy/agentenv-gateway`** |

🔴 **一句话记法**：**三个开关都是 env ⇒ 改完必须滚；只有步骤 9 的 node token 是文件 ⇒ 改完不滚。**
把这两种机制混起来，会得到两个方向都错的结论：要么"改了 CM 就等于翻了开关"（其实没翻），
要么"翻 node token 也得滚一次 DaemonSet"（白付一次 pause 风暴）。

---

### 6.1 依赖图（为什么是这个线性顺序）

🔴 **依赖图分两条轴看：代码落地（编译进哪个镜像）与功能启用（哪个配置翻了）。**
本轮的全部顺序约束都落在**启用轴**上；把它们误读成代码落地轴，就会得出"node 必须滚两次"的结论。

```
【代码落地轴】
A1(node 铸 execution + roster + claim token)
   │  ├─▶ 必须早于 ── A2/A3(scheduler)      ：新 controller 必填 execution，旧 node 不发 ⇒ 全线 InvalidArgument
   │  └─▶ 必须早于 ── A5(scheduler binding) ：仲裁要有可比对的对象
A2(schema)  ─▶ 必须早于 ── A3(fencing)      ：谓词要有列可比
A4 的 gate / A5 的接收端 / A6 的 node 响应字段
            ─▶ 与 A1 **同一个 node 镜像**    ：三者在 token 为空、gateway 不下发 expect 时都是惰性的

【启用轴】（本轮的真正顺序约束都在这里）
A3 集群验证（闸门）─▶ 必须早于 ── A4 启用 / A5 启用 / B 批次
A4: gateway 先注入 ─▶ 再 node 开 gate        ：反过来 node 会拒掉所有平台流量
A5: node 接收端已在位 ─▶ scheduler observe→enforce ─▶ gateway observe→enforce
```

#### ✅ **裁决（2026-08-19 主 agent 追认）：node 只滚一次**

原稿写的是"node 要滚两次（第一次带 A1，第二次带 A4 的 gate 与 A5 的接收端）"，并把合并列为"需追认的偏离"。
**已追认：采纳合并。** A4 的 gate、A5 的接收端、A6 的 node 响应字段、preStop 带头**全部提前到步骤 3**
与 A1 同镜像发布，`AENV_API_CONTROL_PLANE_TOKEN` / token 文件留空、gateway 尚未下发 expect ⇒ **三者都是惰性的**。

**为什么这不是绕过闸门**：闸门约束的是**启用**（"A4/A5 在 A3 集群验证通过后才生效"），不是**代码落地**。
合并后 A4 的启用点仍在步骤 9（A3 闸门 = 步骤 7 之后），A5 的启用点仍在步骤 10/11 ——
顺序一分没变，变的只是"惰性代码在哪个镜像里躺着"。而**部署惰性代码 + 配置翻转启用**，
本就比"到点了再滚一次带新代码的镜像"安全：后者把"新代码首次运行"与"新功能首次生效"压在同一时刻，
出问题时分不清是哪一个；前者让新代码先跟着步骤 3–8 空跑一整段（含闸门 7 的三发探针）再启用。

**代价侧的证据（合并要省下的到底是什么）**：node 是 DaemonSet，**优雅关机会把该节点上所有非 Paused
沙箱 pause 掉**（`src/orchestrator/service.rs:2604` → `:2636`，`terminationGracePeriodSeconds: 3600`
+ `maxSurge: 0` ⇒ **逐节点串行**）⇒ **每滚一次 node = 一次全集群 pause 风暴**，
而 pause 正是删 `previous_snapshot_id` 那个不可逆点所在（R5 实证）。
🔴 **两次滚的风暴代价并不对称**：

| | 风暴代价 |
|---|---|
| 步骤 3 的滚 | ≈ **0** —— 它紧跟步骤 2（登记行清零 + 本地 paused 记录清空 + 存量沙箱按 §6.3 已清干净），**集群是空的** |
| 原步骤 9 的第二次滚 | 🔴 **真代价** —— 那时集群已经从步骤 3 一路用回满（步骤 4–8 的每一发验证都在建沙箱），一滚就是一次满载 pause 风暴 |

⇒ 合并消掉的正是**唯一一次有代价的滚**。

🔴 **合并成立有一个前提，必须一起做**：A4 的启用点（步骤 9）**不能是 env 变量**。
`AENV_API_CONTROL_PLANE_TOKEN` 走 env ⇒ 改它要滚 DaemonSet ⇒ 那次滚发生在集群满载时，
**刚省下的风暴原样还回来，合并等于白做**（回退演练 §6.5 R1/R2 还要再各付一次）。
⇒ node 侧的 control-plane token **必须支持从挂载文件热读**（`_design-phase3-node.md` §3.2 已按此定稿）：
启用 = 写 Secret 文件，回退 = 清空该文件，**两个方向都零重启**。
🟡 若主 agent 否决这条新增机制，则合并的收益减半：步骤 9 与 §6.5 的 R1/R2 各要付一次满载 pause 风暴，
且每次翻转前**必须先主动 drain**（把全集群沙箱按正常路径 pause 干净）才能把风暴压回可控 ——
这条替代路径同样要写进 runbook，不许默认"翻个 env 而已"。

---

### 6.2 🔢 步骤表（按编号顺序执行，不许跳步）

> 🔴 **读法**：「部署什么」列里凡标 🟡 **本步部署但未启用** 的，都是**惰性代码** ——
> 它这一步只是被编译进镜像并跟着跑，**功能生效点在另一步**（该行会点名是哪一步）。
> 这是本轮 node 只滚一次的代价分摊方式，见 §6.1 的追认块。
> 🔴 **node 全程只滚一次（步骤 3）**。步骤 9 是**配置热翻转，不滚 DaemonSet** ——
> 任何让 node 重启的动作都会触发一次全集群 pause 风暴（§6.1）。
> 🔴 **本表只说"部署什么、验什么"；具体怎么部署一律看 [§6.0.4](#604--定点更新命令序列可直接执行步骤号对齐-62-步骤表) 的定点更新命令序列**
> —— 这两套集群与仓内清单已长期漂移，**不许用 `make k8s-apply`**（§6.0）。
> 🔴 **别只 `set image`**：集群里那三份工作负载**缺**本轮要的 env / volume / preStop
> （node 缺 4 样、scheduler 缺 2 条开关 env、gateway 缺 2 条 env），只换镜像的后果逐条列在 §6.0.4。
> 🔴 **哪几步要滚服务、哪一步是真热生效**，看 [§6.0.7](#607--哪几步要滚服务哪一步是真热生效别把两种机制混着记)：
> **三个开关都是 `configMapKeyRef` 注入的 env ⇒ 改完必须滚 Deployment；只有步骤 9 的 node token 是挂载文件 ⇒ 改完不滚。**

> ⚠️ **本段原表述已订正（2026-08-20，pve-sg dev 实测）**，原文保留在下面作为问题陈述：
> ~~🔴 **本表所有"部署/滚服务"的动作一律走 `make k8s-apply`（= `bash deploy/k8s/run.sh apply`），
> 不要直接 `kubectl apply -k deploy/k8s/base`。**~~
> ✅ **本轮（dev / test 两套 k3s）一律不走 `make k8s-apply`，改走 §6.0 的定点更新。**
> 理由不是 `run.sh` 有问题，而是**这两套集群已经与仓内清单长期漂移**：OSS 快照后端、
> `AENV_PAUSED_REGISTRY_BACKEND=central`、registry 前缀镜像、regctl 挂载全是 out-of-band 的，
> 全量 apply 会把它们**一起抹掉且不报错**（完整清单 + 抹掉后的症状见 **[§6.0](#60--本轮的部署方式定点更新不走-make-k8s-apply不许抹掉集群侧的-out-of-band-配置)**）。
> 🟡 **这些漂移都不是阶段 3 引入的** —— 本轮既不消灭它们，也不假装没有：登记 + 在每一步的验证里保护它们。
>
> 下面这条关于 `run.sh` 的事实**仍然成立且仍然要知道**（`make k8s-render` 本轮照用，用来核对定点补丁写对了没）：
> `deploy/k8s/base/config/agentenv.toml` **不在仓库里**：`run.sh` 会把整个 `deploy/k8s` 复制到临时目录，
> 再从 `config/default.toml` 生成它（`run.sh:30`），之后才调 kustomize。绕过 `run.sh` ⇒ kustomize 在
> `configMapGenerator` 上直接报文件不存在。
> 🔴 而这条机制正是漂移不可 apply 的**头号原因**：`generatorOptions.disableNameSuffixHash: true` ⇒
> 同名覆盖，一次 apply 就把集群里那份手工调过的 `agentenv.toml` 换成 `config/default.toml`。
> 🟡 **这不是阶段 3 引入的**：该引用自开源首版提交（`8f028b1`，2026-07-25）就在，`agentenv.toml` 从未被提交过，
> 也不在任何 `.gitignore` 里 —— 它就是个渲染期产物。而且 `base` 本来也不是 kustomize 入口，
> 入口是 `deploy/k8s/overlays/{default,local-dev}`，`run.sh` 用 `K8S_OVERLAY` 选。

| # | 部署什么 | 前置条件 | 验证方法（可执行）| 回退动作 |
|---|---|---|---|---|
| **1** | **什么都不部署 —— 基线与前置盘点** | 无 | ① `kubectl -n <ns> get svc agentenv-gateway-nodeport -o yaml > /tmp/nodeport-30800.bak.yaml`（🔴 **out-of-band 资源，必须先备份**，见 §6.5）；② `psql -c "SELECT state, count(*) FROM paused_sandboxes GROUP BY 1"` **存档**（顺带取 T2 N3 那条 `invalid_rows` 零分辨力的样本）；③ 记下每个节点本地 paused 目录的条目数（⚠️ **口径已订正**：数 `/workspace/env/persisted-sandboxes/artifacts/` 的**子目录数**，或读节点启动日志的 `loaded=N retained=N`；**数 `persisted-sandboxes/` 顶层条目永远得 2**，见 §6.3）；④ 确认三个开关在**旧 build 里都不存在**（grep 配置，避免"以为翻了其实没读"）；⑤ 🔴 **清点仍在 `Running` 的沙箱**（`GET /v2/sandboxes`）—— 它们在步骤 3 会被无登记表的关机 pause 打掉，见 §6.3 | —— |
| **2** | 🔴 **破坏性步骤 + 🔴 服务窗口开始 —— 见 §6.3，有独立确认点**<br>🔴 **窗口不只是控制面**：scheduler 停 ⇒ gateway 的 `LookupNode` 也答不出 ⇒ **数据面同时不可用** | 步骤 1 全部完成且**存档已落盘** | 见 §6.3 的逐条验证 | 见 §6.3（**不可逆：登记行清零**；本步的"回退"只是把旧镜像放回去，数据回不来）|
| **3** | 🔴 **node 镜像（全程唯一一次滚）**<br>**立即生效**：A1（`ExecutionId` 类型 + `LaunchPlan` 两变体各带 execution + `ClaimedExecution` token + `SandboxMetadata.execution_id` 必填 + 心跳带 `roster` **且保留 `sandbox_ids`** + `AcquireSandbox`/`TransitionSandbox` 带 execution）+ A6 的 node 响应字段（`Sandbox` / `SandboxDetail` / `ListedSandbox` 三个 schema，additive）<br>🟡 **本步部署但未启用**：<br>· **A4 的 gate**（token 与 token 文件**都留空** ⇒ 全放行 = 今天的行为）⇒ 启用点在**步骤 9**<br>· **A5 的接收端**（gateway 尚未下发 expect 头 ⇒ 恒放行）⇒ 启用点在**步骤 10**<br>· **preStop 带 `x-agentenv-control-plane` 头**（node 还没开 gate ⇒ 头被忽略）⇒ 与步骤 9 同时变成必需 | 步骤 2 已完成（本地 paused 目录已清空，否则本步会因缺 `execution_id` **响亮报错**）<br>🔴 **执行方式见 §6.0.4 步骤 3**：一个 `kubectl patch` 同时带 image + `AENV_API_CONTROL_PLANE_TOKEN_FILE` + `control-plane-token` 卷与挂载 + 新 preStop。**分两次做 = 滚两次 DaemonSet = 两次全集群 pause 风暴**| ① `kubectl rollout status ds/agentenv-node -n <ns>`（🔴 DaemonSet 名是 **`agentenv-node`**，不是 `agentenv`）；② 每个节点日志**不得**出现"paused 记录加载失败"（出现 = 步骤 2 的 node 那一半漏做）；③ 🔴 **此刻不要试 pause/resume** —— scheduler 仍是 0 副本、表已 DROP，登记表路径必然失败，那不是回归；能做的是 `GET /health` 与 `GET /sandboxes` 通、进程不 crash-loop、心跳失败日志是"连不上 scheduler"而不是别的；④ 🔴 **A4 未启用的对照探针**（本步的关键自证）：`kubectl port-forward pod/<node-pod> 18000:8000` 后**无 token** `POST /sandboxes/{id}/pause` ⇒ **不是 403**（是 4xx/5xx 的业务错都行）。看到 403 说明 token 被误配了，步骤 8 之前 node 会拒掉一切 | 换回旧 node 镜像 + **再清一次本地 paused 目录**（旧 build 读不懂新记录里的 `execution_id`？—— serde 未知字段是忽略的，**能读**；但 A1 之后写下的记录在旧 build 上会丢失化身，属可接受）|
| **4** | **scheduler 镜像：A2 + A3 + A5 的 scheduler 侧**（新 `SchemaDDL` + fail-fast 自检 + 两条 SQL 谓词 + 三条夺权路径清轴 + binding/roster 带 execution + 仲裁 + `LookupNode` 两个新字段 + `rosterFromHeartbeat` 回落）<br>**开关**：`scheduler.registry.write_fencing=true`（默认）、🔴 `scheduler.routing.execution_arbitration=observe`（**发布纪律，不是默认值**）<br>🔴 **服务窗口在本步 Ready 时关闭**（§6.3）| 步骤 3 的 DaemonSet **rollout 已 Ready**（`desiredNumberScheduled == numberReady`）<br>🔴 **必须先建 CM `execution-fencing-config`**（集群里没有，实测 `NotFound`）：不建 ⇒ 两条 env 落空 ⇒ **回落代码默认 `enforce`**，步骤 5/6 的 observe 闸门直接被跳过。执行方式见 §6.0.4 步骤 4 | ① `Migrate` 成功、`kubectl logs` 无 fail-fast 报错；② `\d paused_sandboxes` 有 14 列与两条 CHECK；③ pause 一台沙箱 → `SELECT state, execution_id FROM paused_sandboxes` ⇒ `publishing`/非空；④ resume → `running`/**同一个** execution；⑤ 🔴 **必然序列探针**（§6.6 P-A3）| **配置级**：`SCHEDULER_REGISTRY_WRITE_FENCING=false` + `SCHEDULER_ROUTING_EXECUTION_ARBITRATION=off`，滚 Deployment。**镜像不必回退** |
| **5** | 🟡 **混版本窗口的关闭点（不部署，只观察）** | 步骤 4 已上 | 盯 `agentenv_scheduler_heartbeat_legacy_roster_total{node}` —— 🔴 **必须归零**。非零 = 集群里还有没带 A1 的 node（步骤 3 漏了某台，或有节点 cordoned）| —— |
| **6** | **翻 `scheduler.routing.execution_arbitration=enforce`** | 步骤 5 归零 **且** `agentenv_scheduler_binding_execution_total{decision="rejected_older"}` 在 observe 期间**恒 0** | ① 指标 `agentenv_scheduler_routing_execution_arbitration_enabled == 2`；② `agentenv_scheduler_lookup_execution_authority_total{authority="registry"}` 占比达到预期；③ 数据面照常 | 翻回 `observe`（或 `off`），滚 Deployment |
| **7** | 🚦 **闸门：A3 集群验证（不部署）** | 步骤 4–6 全部完成 | §6.6 的 **P-A3-1 / P-A3-2 / P-A3-3** 三发探针**全过**，且每发都带对照面。🔴 **不过就不许进步骤 8**，更不许开 B 批次 | —— |
| **8** | **gateway 镜像：A4 的注入 + A5 的 gateway 侧 + A6 的三个 DTO + `GET /v2/sandboxes` 去重按 execution**<br>**开关**：`GATEWAY_CONTROL_PLANE_TOKEN=<token>`（**开始注入**）、🔴 `gateway.routing.execution_fencing=off`<br>🟡 **本步部署但未启用**：A5 的 gateway 侧（`off` 在 `decideFencing` 入口 early return ⇒ 不下发 expect、不比对回声）⇒ 启用点在**步骤 10/11** | 步骤 7 闸门通过<br>🔴 **建 Secret 时只建 key `token`**（gateway 那半），**key `node-gate-token` 留到步骤 9** —— 否则一个动作同时点亮两侧，步骤 8→9 的顺序约束形同虚设（§6.0.6）。集群里那份 gateway 只有一条 env，两条新 env 都要 patch 进去，见 §6.0.4 步骤 8 | ① `kubectl rollout status deploy/agentenv-gateway`；② 🔴 **注入生效探针**：在 node 上抓一次经 gateway 的控制面请求，确认带 `x-agentenv-control-plane`（或用一个临时 echo 上游）；③ **对照面**：客户端自己塞 `x-agentenv-control-plane: forged` ⇒ 上游收到的是 gateway 的值，不是 forged；④ `GET /v2/sandboxes` 仍 200，且此刻**已能看到 `executionID` 字段**（node 的 A6 从步骤 3 就在报了，本步是 gateway 侧 DTO 不再把它吃掉）| `GATEWAY_CONTROL_PLANE_TOKEN=""`（停止注入**并删除**入站同名头），滚 Deployment |
| **9** | 🔴 **启用 A4（配置热翻转，不滚 DaemonSet、不换镜像）**：把 token 写进 node 挂载的 Secret 文件（`_design-phase3-node.md` §3.2）⇒ 步骤 3 就躺在那儿的 gate 开始生效<br>🔴 **绝不要改 `AENV_API_CONTROL_PLANE_TOKEN` 这个 env** —— 改 env 要重启 Pod，等于在满载集群上再付一次全集群 pause 风暴（§6.1） | 步骤 8 的**注入已验证生效**（🔴 顺序颠倒 ⇒ node 会拒掉所有平台流量）<br>🔴 **本步的动作 = 给 Secret 补上 key `node-gate-token`，值从 key `token` 派生（不许手敲）**，见 §6.0.4 步骤 9 / §6.0.6 | ① 先确认**热生效**：改文件后 `kubectl get pod -l app.kubernetes.io/name=agentenv-node` 的 `RESTARTS` 与 `AGE` **一个都没变**（Secret 卷刷新有 ≤60s 延迟，等指标/日志出现 gate 启用记录再验）；② §6.6 的 **P-A4-1/2/3**；③ 🔴 **无 token 的 `GET /sandboxes` 仍通、无 token 的 `POST /sandboxes` 被拒**（缺后半句，"整条路径前缀豁免"的变异会假绿）；④ `GET /v2/sandboxes` 集群列表**仍然 200**（D6 豁免生效，否则整体 502）；⑤ preStop：`kubectl drain` 一台节点，确认它进 DRAINING 而不是静默失败（🔴 这一发会把该节点的沙箱 pause 掉，**放在本步最后做**）| 🔴 **node 先**：把该 Secret 文件**清空**（空 = 全放行 = 今天的行为），同样**不滚 DaemonSet**。**绝不能先回退 gateway**<br>🔴 **"清空"字面意思是把 key `node-gate-token` 写成空串，不是删掉这个 key**：删 key ⇒ 文件消失 ⇒ node 对"读失败"是刻意**保留上一个 good 值**的（`control_plane_gate.rs`，测试 T-A4-9 钉住）⇒ **gate 还开着，而 `kubectl` 会回你 patched**。命令见 §6.0.4 步骤 9 末尾，机理见 §6.0.6 |
| **10** | **翻 `gateway.routing.execution_fencing=observe`** | 步骤 9 的热翻转**已验证生效**（P-A4-1/2/3 全过）| ① `agentenv_gateway_execution_fencing_total{decision="unfenced_node_silent"}` **必须为 0**（非 0 = 还有 node 没装 A5 接收端）；② ⚠️ **本判据已订正（2026-08-20，对齐 gateway 设计 §10 的裁决 A5-U5）**：~~`refused_preflight` / `refused_echo` 在健康集群上**恒 0**（🔴 非 0 是**先查再开**，不是"翻了再说"）~~ —— 两个系列必须**分开读**。✅ **`refused_echo == 0` 才是真判据**（🔴 非 0 是**先查再开**，不是“翻了再说”）。🔴 **`refused_preflight` 在 observe 期恒 0 是必然，不是证据**：observe **不下发 expect 头** ⇒ 闸 1 根本没上膛 ⇒ node 无从拒 ⇒ 这个系列**只可能**是 0。拿它当“闸 1 正常”的证据是**假绿**。它**非 0 反而说明有别的东西在往 node 发 expect 头** —— 另一个跑在 `enforce` 上的 gateway、翻转瞬间仍在途的请求、或中间件重放；③ `unfenced_no_authority` 与 scheduler 的 `lookup_execution_authority_total{authority="unknown"}` **逐条对得上**（跨服务互证，对不上就是有一侧算错了）| 翻回 `off` |
| **11** | **翻 `gateway.routing.execution_fencing=enforce`** | 步骤 10 的三条判据全过 | ① 数据面照常；② `refused_*` 仍恒 0（🔴 **闸 1 真阳性为 0 不算失败**，见 §10.3 A5-U3）；③ `agentenv_scheduler_registry_execution_mismatch` 恒 0 | 翻回 `observe` / `off` |
| **12** | **回退演练（不改代码，必须真跑一遍）** | 步骤 11 完成 | §6.5 逐条 | —— |
| **13** | 🚦 **解锁 B 批次** | 步骤 1–12 全过 | —— | —— |

---

### 6.3 🔴 步骤 2：破坏性步骤（**唯一一步会打掉存量沙箱**，有独立确认点）

> **这一步的两半 —— scheduler 的 `DROP TABLE` 与 node 的"清空本地 paused 记录" —— 是同一次破坏性操作，
> 必须在同一个步骤里做完**（裁决 §10 第 4 条）。
> 分开写，就一定会有集群只做了其中一半，症状是**"中央说没有、节点说有"的半清状态，比两边都不清更难查**。

#### 🔴 服务窗口的**范围**（别把它当成"控制面短暂不可用"）

**窗口 = 步骤 2.2（`scale deploy/agentenv-scheduler --replicas=0`）→ 步骤 4 的 scheduler Ready。**
它**跨过了整个步骤 3 的 node 滚动**（步骤 2.6 明写 controller 要等 node 滚完才起）。

| 面 | 窗口内的状态 | 为什么 |
|---|---|---|
| **控制面**（create / pause / resume / delete） | 🔴 **全不可用** | 登记表已 DROP；`Migrate` 要等新 scheduler 起来才重建 |
| 🔴 **数据面**（`{port}-{sandboxID}.<domain>` 的沙箱流量、预览） | 🔴 **同样不可用** | gateway 每条数据面请求都要先 `LookupNode`（`services/gateway/internal/server.go:177-195` 之后的唯一决策），scheduler 是 0 副本 ⇒ `Unavailable` ⇒ 502/503。🔴 **任何"只关写"的开关都救不了它**（包括 `SCHEDULER_REGISTRY_WRITE_FENCING=false`）—— `DROP TABLE` 之后**读路径也是 `42P01`**，而且此刻 scheduler 进程根本不在 |
| **沙箱 VM 本身** | 🟡 **分两段看**（下方） | |
| Agent-Console | 那一页变**空**（`source` 仍 `ok`），不是"读不到" | §6.7 |

🔴 **"窗口内沙箱 VM 本身仍在跑"这句话只对前半段成立**：

| 时段 | VM 状态 |
|---|---|
| 2.2 → 步骤 3 开始 | ✅ **VM 全在跑**，只是没人能路由到它们（数据面经 gateway ⇒ 不可达；直连 node 的 `:8000` 仍可达，可用 `kubectl port-forward` 自证） |
| 步骤 3 的 node 滚动期间 | 🔴 **凡是这时候还活着的，就没了**（= 下面那条前置没做干净的情形）：DaemonSet 优雅关机会尝试 pause 该节点上每一台非 Paused 沙箱（`service.rs:2604`→`:2636`），而此刻登记表不存在、scheduler 是 0 副本 ⇒ **每一次 pause 都会失败 3 轮**（`MAX_SHUTDOWN_PASSES`），随后进程按 `terminationGracePeriodSeconds` 退出 ⇒ **这些 VM 直接没了，且没有可用快照**。按前置清干净后本行为空集 |

⇒ 🔴 **所以步骤 1 的第 ⑤ 项与下面确认点里的"清 Running"不是可选的**：
把存量 `Running` 沙箱在步骤 2 之前**主动收干净**（`DELETE /sandboxes/{id}`，或先 pause —— 反正它们的
paused 记录马上要在 2.4 被清掉），否则"步骤 2 只毁 paused"这句话是假的，步骤 3 会顺手把 Running 的也毁掉，
**而这一段今天在文档里完全没写**。清干净还有第二个好处：步骤 3 的滚动此时**无沙箱可 pause**
（`list_filtered` 返回空 ⇒ 直接 break）⇒ 该次滚动的 pause 风暴代价为 0，这正是 §6.1 合并方案成立的前提之一。

**预估窗口时长**（dev / test 各两节点）：

| 分段 | 典型 | 决定因素 |
|---|---|---|
| 2.2–2.5（停 scheduler + DROP + 逐节点清目录） | **1–3 分钟** | 节点数 |
| 步骤 3（node 滚动，**已按上面清空沙箱**）| **3–8 分钟** | 镜像拉取 + 逐节点串行（`maxSurge: 0`）+ 启动自检 |
| 步骤 4（scheduler 起 + `Migrate`）| **1–2 分钟** | —— |
| **合计** | 🟡 **5–15 分钟** | 🔴 **若没清 Running 沙箱，步骤 3 会变成"每台沙箱 3 轮失败 pause"，上限受 `terminationGracePeriodSeconds: 3600` 约束，可以退化到小时级** |

#### ✅ 进入前的独立确认点（**逐条勾，不勾满不许开始**）

- [ ] 我知道**这一步会打掉 dev/test 集群上全部登记行**，且**不可逆**（`SELECT state,count(*)` 已存档在步骤 1）
- [ ] 我知道**存量沙箱会失去"换节点恢复"的能力**；它们**仍可在自己的节点上被唤醒**，且**下一次 pause 会把行重新写回来**（`beginPause` 的 INSERT 分支）
- [ ] 我知道**节点本地 `$AENV_HOME/persisted-sandboxes` 会被清空**，那些 paused 沙箱**就地消失**
- [ ] 🔴 我已经把**仍在 `Running` 的沙箱清干净了**（步骤 1 第 ⑤ 项清点过），我知道不清的话它们会在步骤 3 的关机 pause 里**无快照地消失**，且会把服务窗口拖长一个数量级
- [ ] 这是 **dev 或 test 集群**，不是生产（AgentENV 本轮无生产，但这条勾是给未来的人看的）
- [ ] 🔴 我准备好接受一个 **aenv 服务整体窗口**（**不是**"控制面短暂不可用"）：**控制面与数据面同时不可用**，从 2.2 一直持续到步骤 4 的 scheduler Ready，典型 **5–15 分钟**（范围与时长见上一小节）
- [ ] 本集群的 `agentenv-gateway-nodeport`（30800）已在步骤 1 备份

#### 执行（6 个子步，顺序不可换）

```bash
NS=<agentenv 命名空间>
# 2.1 存档（若步骤 1 没做，现在补）
psql "$DSN" -c "SELECT state, count(*) FROM paused_sandboxes GROUP BY 1;" | tee /tmp/aenv-registry-before.txt

# 2.1b 🔴 先清 Running 沙箱（**服务窗口之前**，此刻控制面还活着）
#      不清的后果见本节「服务窗口的范围」：步骤 3 的关机 pause 会在无登记表的情况下失败 3 轮，
#      这些 VM 无快照地消失，且窗口从分钟级退化到小时级。
curl -s "http://<gateway>:30800/v2/sandboxes" | jq -r '.[] | select(.state=="running") | .sandboxID' \
  | while read -r id; do curl -s -X DELETE "http://<gateway>:30800/sandboxes/$id"; done
curl -s "http://<gateway>:30800/v2/sandboxes" | jq 'length'    # 应为 0（或只剩 paused，它们下一步也会没）

# 2.2 停 controller 的写入面（本轮直接停进程）
#     🔴 服务窗口从这里开始，且是 **aenv 整体窗口**：控制面 + 数据面同时不可用，
#        一直到步骤 4 的 scheduler Ready 才关闭（跨过整个步骤 3 的 node 滚动）。
kubectl -n "$NS" scale deploy/agentenv-scheduler --replicas=0
kubectl -n "$NS" rollout status deploy/agentenv-scheduler --timeout=120s   # 应显示 0 available

# 2.3 DROP TABLE（🔴 前半）
psql "$DSN" -c "DROP TABLE IF EXISTS paused_sandboxes;"

# 2.4 清空【每一个】节点的本地 paused 记录（🔴 后半，与 2.3 是同一次操作）
#     ⚠️ 本小节两条命令已订正（2026-08-20，pve-sg dev 实测），原文见本代码块下方的「已订正」块。
#     🔴 路径是 /workspace/env/persisted-sandboxes，不是 "$AENV_HOME"/persisted-sandboxes：
#        容器里 AENV_HOME **是空串**，真正的变量叫 AENV_HOME_PATH=/workspace/env
#        （镜像里写死：deploy/docker/Dockerfile.agentenv 的 `ENV AENV_HOME_PATH=/workspace/env`；
#         /workspace 是 hostPath /var/lib/aenv，所以宿主机上是 /var/lib/aenv/env/persisted-sandboxes）。
#     🔴 selector 必须是 app.kubernetes.io/name=agentenv-node（DaemonSet 名 agentenv-node，
#        `deploy/k8s/base/agentenv-daemonset.yaml:3-6`）。写错 label 的后果是**静默无操作**：
#        for 循环遍历空列表 ⇒ 什么都没清，而下面 2.5 的检查同样遍历空列表 ⇒ 一行输出都没有，
#        看起来像"每台都是 0"。⇒ 先断言 Pod 数与节点数相等，再动手。
NODES=$(kubectl get node --no-headers | wc -l)
PODS=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=agentenv-node --no-headers | wc -l)
[ "$NODES" = "$PODS" ] || { echo "selector 选出 $PODS 个 pod，节点有 $NODES 个 —— 停下来查，别继续"; exit 1; }

PSD=/workspace/env/persisted-sandboxes

# 2.4a 🔴 清空之前先自证目标目录【存在】（这是本轮第二次踩"空列表伪装成已清空"，见下方失败形态）
for n in $(kubectl -n "$NS" get pod -l app.kubernetes.io/name=agentenv-node -o name); do
  echo "== $n"
  kubectl -n "$NS" exec "$n" -- sh -c '
    D='"$PSD"'
    [ -d "$D" ] || { echo "FAIL: $D 不存在 —— 路径写错了，停下来查"; exit 9; }
    echo "OK  $D 存在；artifacts 子目录 $(ls -1 "$D/artifacts" 2>/dev/null | wc -l) 个"
  ' || exit 9
done

# 2.4b 🔴 对照面：同一条命令指向一个【必然不存在】的路径，必须报错并 exit 9，而不是打印 0
kubectl -n "$NS" exec "$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=agentenv-node -o name | head -1)" -- sh -c '
  D=/workspace/env/persisted-sandboxes-NOPE
  [ -d "$D" ] || { echo "FAIL: $D 不存在"; exit 9; }
  echo "artifacts 子目录 $(ls -1 "$D/artifacts" 2>/dev/null | wc -l) 个"
'; echo "对照面 rc=$?  # 必须是 9。是 0 就说明这条探针没有分辨力，上面那些 OK 都不算数"

# 2.4c 真正清空
for n in $(kubectl -n "$NS" get pod -l app.kubernetes.io/name=agentenv-node -o name); do
  kubectl -n "$NS" exec "$n" -- sh -c 'rm -rf /workspace/env/persisted-sandboxes/artifacts/* || true'
done

# 2.5 确认两半都做了
psql "$DSN" -c "\dt paused_sandboxes"     # 应为 "Did not find any relation"
# 🔴 口径是 artifacts/ 的【子目录数】，不是 persisted-sandboxes/ 的条目数：
#    persisted-sandboxes/ 下永远是 artifacts/ + records.db 两个固定条目，与 paused 沙箱数【无关】——
#    数它永远得 2，既不会因为清空而变 0，也不会因为有 5 台 paused 而变 5。
for n in $(kubectl -n "$NS" get pod -l app.kubernetes.io/name=agentenv-node -o name); do
  echo -n "$n: "
  kubectl -n "$NS" exec "$n" -- sh -c '
    D=/workspace/env/persisted-sandboxes
    [ -d "$D/artifacts" ] || { echo "FAIL: $D/artifacts 不存在"; exit 9; }
    ls -1 "$D/artifacts" | wc -l
  '   # 每台都应为 0
done   # 🔴 输出行数必须等于 $NODES；一行都没有 = selector 写错了，不是"都清干净了"

# 2.5b 🔴 权威口径（比数目录更硬）：步骤 3 node 起来后看启动日志的 loaded=/retained=
#      这两个数才是"节点认为自己手上有几条 paused 记录"，两台都必须是 loaded=0 retained=0
kubectl -n "$NS" logs -l app.kubernetes.io/name=agentenv-node --tail=2000 --prefix \
  | sed 's/\x1b\[[0-9;]*m//g' | grep -a "loaded paused sandbox records"

# 2.6 controller 先【不要】起 —— 它要等步骤 3 的 node 滚完，见步骤表
#     🔴 原因（也是服务窗口为什么这么长的全部原因）：步骤 4 那版 scheduler 对
#        begin_pause / mark_running 的 execution_id 是【必填】的，而没滚到 A1 的
#        旧 node 根本不发 ⇒ 提前起 = 全集群 pause/resume 全线 InvalidArgument。
#        起旧 scheduler 更糟：它会用旧形状重建表并在滚动期间写进无 execution 的行，
#        步骤 4 的 fail-fast 自检会因此拦住 Migrate，逼你再 DROP 一次。
```

🔴 **2.5 的两条命令都必须跑**。只跑一条 = 只确认了一半，而"半清状态"正是这一步最想避免的东西。

##### ⚠️ 2.4 / 2.5 的原命令（保留为问题陈述，**实测不符，勿照抄**）

```bash
# ❌ 原文 2.4
kubectl -n "$NS" exec "$n" -- sh -c 'rm -rf "$AENV_HOME"/persisted-sandboxes/* || true'
# ❌ 原文 2.5
kubectl -n "$NS" exec "$n" -- sh -c 'ls -1 "$AENV_HOME"/persisted-sandboxes | wc -l'   # 每台都应为 0
```

**为什么它们是静默失败的**（2026-08-20 在 dev 两台节点上逐条实测）：

| # | 事实 | 原命令实际做了什么 |
|---|---|---|
| 1 | 容器里 **`AENV_HOME` 是空串**，真变量是 `AENV_HOME_PATH=/workspace/env`（镜像里 `ENV` 写死，见 `deploy/docker/Dockerfile.agentenv`）| `"$AENV_HOME"/persisted-sandboxes` 展开成 **`/persisted-sandboxes`** |
| 2 | `/persisted-sandboxes` 不存在 | 2.4 的 `rm -rf /persisted-sandboxes/*`：glob 无匹配 ⇒ 传字面串 ⇒ **无操作**（`\|\| true` 再把一切吞掉）|
| 3 | 同上 | 2.5 的 `ls -1 /persisted-sandboxes \| wc -l`：`ls` 的报错走 **stderr**，`wc -l` 数到 0 行 ⇒ **stdout 打印 `0`、退出码 `0`** |
| 4 | 合起来 | 🔴 **每台节点都打印 `0`，看起来"两台都清干净了"，实际一台都没清** —— 而真正的 1.8G paused 沙箱还在 worker-01 上 |

> 🔴 **这是本轮第二次踩同一个失败形态：「空列表 / 空目录被伪装成『已经清干净了』」。**
> 第一次是 **selector label 写错**（`for` 遍历空 Pod 列表 ⇒ 一行输出都没有 ⇒ 读成"每台都是 0"），
> 已经在 2.4 的注释里写过；第二次就是这里的**路径写错**（目录不存在 ⇒ `ls` 报到 stderr、`wc` 数到 0）。
> **两次的形状完全一样：一个本该是「查不到」的结果，被打印成了「查到了，结果是零」。**
>
> ⇒ **本 runbook 的通用纪律**：凡是「数一个数，期望它是 0」的检查，
> **必须先用一个必然失败的对照输入证明它分得清「0」与「没查成」**
> —— 目录检查就先 `[ -d ]`，列表检查就先断言条数，探针就配一个必然不存在的输入（2.4b 就是这么写的）。

##### ⚠️ 2.5「条目数」口径的订正

~~记下每个节点本地 paused 目录的**条目数**~~ —— **口径错了**。
`persisted-sandboxes/` 下**永远**只有 `artifacts/` 与 `records.db` 两个固定条目，
**与 paused 沙箱数量无关**（dev 实测：master-01 有 0 台 paused、worker-01 有 1 台，两台的顶层条目数**都是 2**）。

✅ **正确口径两个，取其一或都取**：

1. **`artifacts/` 的子目录数** —— dev 实测 master-01 = `0`、worker-01 = `1`，与真实 paused 数一致；
2. **节点启动日志的 `loaded=N retained=N`**（`loaded paused sandbox records`）—— 这是节点自己认的数，最权威。

步骤 1 的第 ③ 项「记下每个节点本地 paused 目录的条目数」按此口径执行。

#### 🔴 这一步做错的三种症状（供排错）

| 只做了 | 症状 |
|---|---|
| 只 `DROP TABLE`，没清节点 | 步骤 3 的 node 起来时**加载本地 paused 记录失败**（缺 `execution_id`），日志有响亮报错 + 处置命令（node 设计 §2.3 的裁决要求）。**这是设计好的护栏，照它说的做即可** |
| 只清节点，没 `DROP TABLE` | 步骤 4 的 `Migrate` **fail-fast 自检**报错：一句中文 + `DROP TABLE paused_sandboxes` 命令（scheduler 设计 §2.8）。**不是** PostgreSQL 的 `23514` 约束原文 |
| 两个都没做 | 同上，`Migrate` 直接拦住，controller 起不来 |

---

### 6.4 症状 → 该拉哪个开关

> 🔴 **本轮有三个开关，"fencing 关了吗"这个问题没有单一答案**，必须点名是哪一个（命名裁决 N2，冻结契约见 §10.2）。
> **拉错开关的代价**：以为止住了血，实际关的是另一半，而那一半的失效是**无声的**。

| 症状 | 该拉哪个 | 拉到什么 | 🔴 拉了之后失去什么 |
|---|---|---|---|
| pause / resume 大面积 `PermissionDenied`（Rust 侧 `ExecutionFenced`）；或 `23514` check violation 刷屏 | `scheduler.registry.write_fencing` | `false` | **A3 整个没了** —— 旧化身可以覆盖新化身的行、可以删掉活节点依赖的 durable 快照。🔴 **这是本轮最致命的那半，拉之前先确认不是 node 送错了值**（最容易写错的是 `mark_running` 送了新铸的而不是 claim 时那个）|
| 跨节点 resume 全线失败（`mark_running` 分支 ① 谓词不匹配） | **先别拉开关** | —— | 这多半是 **E-A 的 claim/mark_running 值不一致**（node 侧 `ClaimedExecution` token 没接上），关 fencing 只是掩盖 |
| 数据面路由在两个节点之间来回抖；`binding_execution_total{decision="rejected_older"}` 持续非 0 | `scheduler.routing.execution_arbitration` | 先 `observe` 看清楚，**别直接 `off`** | `off` = 退回"最近上报者胜"，**缺陷一原样回来** |
| 从未 pause 过的沙箱数据面大面积 404（多半在滚动窗口） | **先别拉开关** | —— | 这是 `sandbox_ids`/`roster` 回落没生效（A5-U2）。查 `heartbeat_legacy_roster_total` 与 `heartbeat_roster_dropped_total`，别拿开关掩盖 |
| 客户端大量 `409 sandbox_execution_superseded` | `gateway.routing.execution_fencing` | `observe`（先看，别直接 `off`）| ⚠️ **本格原文两处都错，已订正（2026-08-20）**：~~`observe` 仍下发 expect 头与计数、只是不拒；🔴 **注意**：若拒绝来自 node 的 412，`observe` 也会把它翻成放行~~<br>✅ **正确读法**：(a) `observe` **不下发 expect 头**（裁决 A5-U5），它只读回声、只计数、不拒；(b) node 的 412 在 `observe` 下**不是被翻成放行**，而是被翻成 **409 —— 仍然是拒绝**，只是换成唯一的对外形状。但正因为没盖章，node 本来就无从产生 412，这条分支在正常接线下不可达。<br>🟢 **改判之后这条运维建议本身反而更成立**：409 的两个来源在 `observe` 下**全部止住** —— 闸 1（412→409）因没盖章而不可达，闸 2（回声拒绝）因 `refuse=false` 只记不拒。🔴 **但别拿旧解释去解释它**：止血不是因为把拒绝翻成了放行，而是因为一条不可达、一条只观察 |
| 每一次同机 pause→resume 之后第一批请求 409 | **先别拉开关** | —— | 这是**比对写成等值了**（应为有序，`live > expect` 必须放行，裁决 A5-U4）。拉开关只掩盖，改代码才对 |
| 平台开始"重建工作区"、用户工作区蒸发 | 🔴 **立即** `gateway.routing.execution_fencing=off` | `off` | 说明某处把拒绝落成了 **404**。**这是本轮唯一一个"先止血再查"的症状** |
| 平台 create/pause/resume 全被 node 拒（403） | node 侧 control-plane token **文件**（**node 先**）| **清空该文件**（热生效）——🔴 具体是把 Secret 的 key `node-gate-token` **写成空串**，**不是删这个 key**（删 key = 读失败 = 保留上一个 good 值 = gate 还开着，§6.0.6）。🔴 **不要改 `AENV_API_CONTROL_PLANE_TOKEN` 这个 env、不要滚 DaemonSet** —— 滚一次 = 一次全集群 pause 风暴（§6.1）| A4 整个没了，回到 presence-only。🔴 **绝不能先回退 gateway 的注入** —— 那会让 node 拒掉一切 |
| 集群列表 `GET /v2/sandboxes` 整体 502 | 同上（清空 node 侧 token 文件）| 空 | 说明 D6 的 `GET /sandboxes` 豁免没生效（`cluster_list.go:84-96` all-or-nothing）|
| 🔴 **告警守卫（不是症状，是读表前必须先知道的一条）**：`agentenv_scheduler_registry_write_fencing_enabled == 0` | **先别拉任何开关** | —— | 🔴 **这个 gauge 读 0 有两种完全不同的含义，它自己分不开**：① fencing 真的被关了；② **写面根本没装配** —— `SetRegistryWriteFencingEnabled` 只在 `createRegistryStore` 建出写 store 之后才被调用（`services/scheduler/cmd/main.go:394`），而该函数在 `queryOnly \|\| !registryWriteEnabled(cfg)` 时**提前返回**，gauge 就停在默认的 0 上。query-only 副本、没配 DSN 的集群、`write_enabled=false` 的集群**全都读 0**。✅ **任何基于该 gauge 的告警必须用 `agentenv_scheduler_registry_enabled == 1` 做守卫**（该 gauge 在 `createRegistryReader` 里**无条件**设置，见 `main.go:292`）。⚠️ **守卫之后仍有一处残留歧义**：`registry_enabled=1`（配了 DSN）但 `write_enabled=false` 或 `--query-only` 的副本，两个 gauge 会是 `1 / 0`，与“写面开着但 fencing 关了”同形 —— 这类副本要么按实例排除，要么就别在它上面挂这条告警。<br>✅ **已订正（2026-08-20）**：上面这一整段是**问题陈述**，不是现状。残留歧义已由第三个 gauge `agentenv_scheduler_registry_write_surface_enabled` 消掉：**三个 gauge 在每一条启动路径上都被显式设值**（装配写面设 1、没装配设 0，含 `fencing` 那个 —— 它不再停在“没人写过”的默认 0 上）。读法见本表下方「🔴 三个 registry gauge 的组合读法」。旧的“用 `registry_enabled` 守卫”仍然是对的，但已不必够用 |

#### 🔴 三个 registry gauge 的组合读法（2026-08-20 新增）

> 一个 gauge 回答不了「这台在不在写、写的时候有没有带谓词」，因为“没装配”和“关了”会落到同一个 0 上。
> 三个一起读，四种启动形态互不同形；**每一条启动路径都显式设这三个值**，所以读到的 0 一定是事实，不是默认值。

| `registry_enabled` | `registry_write_surface_enabled` | `registry_write_fencing_enabled` | 这台是什么 | 该不该有人管 |
|---|---|---|---|---|
| 0 | 0 | 0 | 没配 DSN，整个 registry 功能关着 | 否（受支持的形态）|
| 1 | 0 | 0 | 配了 DSN 只读：`write_enabled=false` 的集群，**或 `--query-only` 副本** | 否 |
| 1 | 1 | 1 | 在写，且身份轴谓词开着 —— 正常生产形态 | 否 |
| 1 | 1 | **0** | 🔴 在写，但谓词关了：旧化身可以覆盖新化身的行 | **是，唯一需要人管的一格** |

- **告警条件写成**：`registry_write_surface_enabled == 1 and registry_write_fencing_enabled == 0`。
  只用 `registry_enabled` 守卫会把上面第二行（只读副本）一起点着，那是全队最多的那类 Pod。
- 🔴 `write_surface_enabled == 1` 只说明**装配了**，不说明**在服务**：没有 cluster id 的写面是「registered and cold」，
  照样读 1。「冷/热」只有 `/healthz` 的 `phase` 能说（`cold` / `grace` / `serving`），别拿 gauge 去问这件事。
- 代码位置：`services/scheduler/internal/registry_service.go`（三个 setter 与 tuple 注释）、
  `services/scheduler/cmd/main.go` 的 `createRegistryStore`（两条返回路径各设一次）；
  形态互不同形由 `services/scheduler/cmd/health_test.go` 的
  `TestTheRegistryGaugesTellTheStartupShapesApart` 钉住（每格先把三个 gauge 反着设一遍，
  再要求启动路径把它们全写对 —— 漏写一个就红）。

---

### 6.5 🔴 回退演练（步骤 12）—— 必须点名 30800

**为什么单列**：`agentenv-gateway-nodeport`（NodePort **30800**）**不在仓内清单里** ——
`deploy/k8s/base/gateway-service.yaml` **全文 18 行、无 `type:` 字段（即 ClusterIP）、只有 `http:8080` + `metrics:9102`**，
且 `grep -rn "30800\|NodePort" deploy/` **零命中**。它是集群里手工 `kubectl apply` 的 **out-of-band 资源**。

> ⚠️ **本节原先只点名了 30800 一条，这是不完整的（已订正 2026-08-20）**：
> 实测这两套集群共有 **十条** out-of-band 漂移（OSS 快照后端 + 凭据段、缓存预算、
> `[orchestrator.paused_registry] backend`、DS 里硬写的 `AENV_PAUSED_REGISTRY_BACKEND=central`、
> regctl 挂载 + `HOME`、node 的 PG DSN env、三个带 registry 前缀的镜像引用、30800、
> PG/RustFS/agent-console 三套无清单资源、scheduler 的 cluster id 来源）。
> **完整清单与"抹掉后的症状"见 [§6.0.2](#602--out-of-band-漂移清单pve-sg-dev2026-08-20-实测)，
> 复核命令见 [§6.0.5](#605--每一步做完都要跑的out-of-band-还在吗复核带对照面)。**
> 🔴 30800 之所以还单列一行，是因为它是**唯一一个"apply 不会删、但 delete 之后 apply 不会重建"**的，
> 失效方式与其余九条不同。

| # | 演练项 | 做法 | 🔴 自证 / 陷阱 |
|---|---|---|---|
| R1 | 三个开关各自回退一次 | 逐个翻到最保守值、滚服务、跑一遍冒烟；再翻回来 | 每次只翻**一个**，翻完确认**另外两个的指标没变** —— 否则你验的是"一起关了"，不是"能分别关"。🟢 三个开关都在 scheduler / gateway（Deployment）上，**演练本身不碰 node** |
| R2 | node 侧 A4 回退 | **清空 node 的 control-plane token 文件** → 直连 node 的 `POST /sandboxes/{id}/pause` **不再 403** → 再写回去 | 🔴 **顺序**：回退必须 **node 先**；先回退 gateway 会让 node 拒掉一切。<br>🔴 **不许用"改 env + 滚 DaemonSet"来做这一发** —— 演练要来回翻两次，那就是**两次满载全集群 pause 风暴**（§6.1）；token 走挂载文件的全部理由就在这里，本发同时是**该机制的验收**：翻两次之后 `kubectl get pod` 的 `RESTARTS`/`AGE` 必须一动不动 |
| R3 | 🔴 **out-of-band 十条全体存活确认**（含 30800）| 跑一遍 **§6.0.5** 的五组探针（每组自带对照面）| 🔴 **`kubectl apply -k deploy/k8s/...` 不会重建 30800，也不会删它** —— 以为"重新部署一遍就恢复原状"的人会踩空。若它没了：`kubectl apply -f /tmp/aenv-cv/nodeport-30800.bak.yaml`（步骤 1 的备份）。<br>🔴 其余九条**大多数是 apply 会静默抹掉**的（D-1/D-3/D-4/D-5/D-7），所以本演练不只是"看看 30800 在不在"，而是**每一步之后都要跑**（§6.0.2）|
| R4 | 🔴 **别用 30800 验"直连被拒"** | 用 `kubectl port-forward pod/<node-pod> 18000:8000` 直连 | 30800 落在 **gateway**（gateway 默认透传），经它打永远成功。看到 200 会被误读成"收窄失效" |
| R5 | A2 的 schema 不回退 | 确认旧 build 能跑（`entryColumns` / `selectColumns` 是显式列清单，多两列不会被选中）| 若必须彻底回退：再 `DROP TABLE` 一次 + 跑旧 build 的 `Migrate`。**两个方向都只要一次运维动作，不依赖新写的回滚代码** |

---

### 6.6 集群验收探针（每发都必须带对照面）

> 方法论 §8.3：**探针必须先自证** —— 用一个必然失败的对照输入证明它有分辨力。
> 本轮已经翻过一次车：grace 期"拒绝接管"的第一发探针用了一行任何相位都认领不了的合成行，
> `409` 看着像被拒，实则毫无分辨力。

| 探针 | 属于 | 做法 | 🔴 对照面 |
|---|---|---|---|
| **P-A3-1**「必然序列」 | 步骤 7 | 造 `running/E_A/nodeA` → 推租约与死线到过去 → `ReclaimExpiredHoldings` → nodeB `ClaimForResume` + `MarkRunning(E_B)` → **nodeA 的 `BeginPause(E_A)` 必须 `ExecutionFenced` 且行逐列未变** | 先跑 `BeginPause(originNode=nodeB, exec=E_B)` 必须**成功**。同一沙箱、同一条语句、**只换 execution** —— 证明拒绝来自新谓词，而不是 `cluster_id`/`state` |
| **P-A3-2**「零副作用」 | 步骤 7 | 上一发被拒之后，逐列比对行（含 `generation` / `updated_at` / `lease_expires_at` / `execution_id`）+ 确认 OSS 上**没有**新 `artifacts/{uuid}/`、`previous_snapshot_id` **没被删** | 只比 `state` 不够 —— 一次"状态没变但 `updated_at` 被刷新"的写会把 grace 推迟一整轮 |
| **P-A3-3**「两种拒绝分得开」 | 步骤 7 | 同一行两次：`complete_pause` 带错 generation ⇒ `GenerationConflict`（`Aborted`）；`begin_pause` 带错 execution ⇒ `ExecutionFenced`（`PermissionDenied`）；两者 `errors.Is` 互不成立 | 若合并成一个码，节点会重读拿新 generation 再发一次，**绕过 fencing** |
| **P-A4-1**「直连被拒」 | 步骤 9 | `kubectl port-forward pod/<node-pod> 18000:8000` → `curl -X POST localhost:18000/sandboxes/{id}/pause` ⇒ **403** | 同一命令**带上正确 token** ⇒ 非 403。没有对照就分不清"403"与"路由不存在" |
| **P-A4-2**「经 gateway 成功」 | 步骤 9 | 经 30800 打同一个 pause ⇒ 2xx | 临时把 gateway 的 token 改错 ⇒ **必须 403**。不做这步就分不清"注入生效"与"node 根本没开 gate" |
| **P-A4-3**「只读豁免精确」 | 步骤 9 | 无 token 的 `GET /sandboxes` ⇒ **非 403** | 无 token 的 `POST /sandboxes` ⇒ **403**。🔴 缺这半句，"把 `/sandboxes` 整条前缀豁免"的变异会假绿 |
| **P-A5-1**「路由不再被抢回」 | 步骤 10/11 | 让 nodeA 的旧化身继续心跳上报同一沙箱，nodeB 持有新化身 ⇒ 连续 20 次 `LookupNode` **恒指向 nodeB** | `binding_execution_total{decision="rejected_older"}` 在这期间应 +N（证明拒绝真的发生过，而不是 A 恰好没上报）|
| **P-A5-2**「HA 形态」 | 步骤 10/11 | 🔴 **必须用 Redis binding store + query-only 副本**跑一遍 P-A5-1 | 先对一个"只有登记表行、没有 binding"的沙箱在副本上 `LookupNode` ⇒ 必须 `Unavailable`（证明确实跑在**没有 placer 的副本**上，而不是 primary）|
| **P-A5-3**「聚合端点稳定」 | 步骤 11 | 造同 `sandboxID`、同 `startedAt`、不同 execution 的两条行 ⇒ `GET /v2/sandboxes` **连跑 20 次返回同一条**（v7 较大者）| 退回 keep-first ⇒ 20 次里出现两种结果 |
| **P-A5-4**「不误拒同机换代」 | 步骤 11 | 一台沙箱 TTL 自动 pause 后由数据面 auto-resume 拉起（本机换代，`live > expect`）⇒ **第一批请求必须 200，不是 409** | 🔴 这一发直接打裁决 A5-U4：把比对写成等值，这里必挂 |

---

### 6.7 与其它工作的关系

- 🔴 **Agent-Console 不在本轮**（P4）：它后续独立更新，**既不是步骤 2 的前置也不是后置**。
  A2 是**纯加法**（只加两列，不删列/改名）⇒ Console 的 PG 直连**不会失明**；
  步骤 2 的表清零只让它那一页变**空**（`source` 仍是 `ok`），与"读不到"（`source=unavailable`）是两个显示结果。
- **建议与「暂停必然落 OSS」(v3) 并批**（主仓 `docs/proposals/2026-08-19-aenv-pause-publish-durability.md`）：
  它把 `publishing` 改成长驻重试态后，placement 的"硬钉 origin"分支基本消失，B4 才真正自由。
  🔴 **而且它会新造一条节点自主写路径**（长驻重试的 publish），**必须与 A3 同批设计** ——
  否则一边收权、一边开新口子（R5 §🟢 最后一条）。
- **回退总原则**（沿用阶段 0/1/2）：**一个阶段的回退不许依赖新写的回滚逻辑**。
  A 批次的回退面 = 三个配置开关 + 一个空 token，全部配置级；
  唯一不可配置级回退的是**步骤 2 的数据清零**，它已在 §6.3 单独列为破坏性步骤并配了确认点。


## 7. 已知陷阱（写在前面，别再踩一遍）

1. **绝不"跳过 RPC 直删元数据"** —— Cube 的 `sandbox_remove.go:180-204`：节点不在内存缓存里就跳过
   Destroy RPC、直接抹 Redis 元数据 ⇒ 中央认为已删、分区节点上 VM 还在跑还在写盘，
   且 cubelet 无本地 TTL 自杀，孤儿跑到人工干预为止。
2. **分区期间 fail-closed 等待**，不抄 e2b 的"清库不等 ack"（`delete_instance.go:104-105` 无条件
   `defer Remove`）—— e2b 敢这么做是因为沙箱可弃，我们的是用户工作区。
3. **我们的 reclaim 比 e2b 激进**：e2b 的 Running 记录**永不自动释放**（`UnreachableSince` 零消费者），
   我们是租约 + deadline 双条件自动接管。⇒ 同样的分区场景，我们的暴露面比 e2b 大，
   A3 是补上这个差额的东西，不是锦上添花。
4. **`ExpectExecutionID` 在 e2b 是 opt-in**（`states.go:82-92`：空值对"刚读完就动手 / 用户直接下令"
   的调用方是**正确**行为）。我们内部化后是**必填**，因为我们的内部调用方全部是"controller 决定、
   node 执行"，不存在 e2b 那类 fresh-read 直接动手的语义。这个差异要在实现里写清楚，
   否则后来者会照 e2b 抄成 opt-in。
5. 🔴 **拒绝码绝不能是 404 —— 404 在平台侧的语义是"授权重建工作区"。**
   平台判「沙箱没了 ⇒ 重建」靠的就是 resume 的 404，而那个 404 的**产地是 gateway**
   （`services/gateway/internal/server.go:328-344` 把 scheduler `codes.NotFound` 翻成 404（`:337-338`），
   请求**从未到达 node**）。A5 若把"化身过期"落成 404，平台会读成"沙箱没了"然后**重建
   ⇒ 用户工作区蒸发**。⇒ 用 **409** + 独立错误码（A5 硬约束）。
   同理，A3 / A4 的任何新增拒绝路径都要先自问"这个码会不会被平台读成不存在"。
   🔴 **2026-08-19 追加（§10 第 2 条）：也不许复用 410** —— node 的 `/proxy` 已经用它表示
   "sandbox is not proxyable in its current state"（`src/api/proxy.rs:869-872`），再占一个含义就分不开了。
   node → gateway 那一跳用 **412 + `x-agentenv-refusal`** 作内部信号（`/proxy` 码表上的空位），
   由 gateway 翻译成对外的 **409 + `code=sandbox_execution_superseded`**，**不许直通客户端**。
6. 🔴 **别把 `SandboxInstanceId` 当成"已经在跑的机制"**：它只在 `[custom_extension].url` 配置时生成，
   而该项在 `config/default.toml:130-136` 是注释掉的、`deploy/` 零命中
   ⇒ **任何已部署环境里它一次都没被生成过**。可复用的是语义与单测，不是运行中的机制。
7. 🔴 **换代判定不许按 `LaunchMode` 判**："从模板 / 快照创建新沙箱"走的正是 `LaunchMode::Resume`
   （`src/sandbox/firecracker/sandbox.rs:551-561`）⇒ 必须按 `LaunchPlan` 变体判，否则"创建"被误判成"resume"。

---

## 8. 方法论要求（沿用本轮已验证有效的四条）

1. **契约测试与实现分离，测试作者不读实现** —— 只从规格推导期望值。它逼出过 12 条
   "语义在接口上表达不出来"（S0–S11），那不是测试能覆盖的东西，是**接口本身的缺口**。
2. **每条修复配一发变异验证** —— 把修复退回去，确认测试真的 FAIL。本轮累计 152 发全部有牙，
   抓出过 3 个"看着通过、实际什么都没断言"的测试。
3. **探针必须先自证** —— 用必然失败的对照输入验证探针有分辨力。本轮翻过一次车：
   grace 期"拒绝接管"的第一发探针用了一行任何相位都认领不了的合成行，
   `409` 看着像被拒，实则毫无分辨力。
4. **无声上限要 log** —— 任何 top-N / 不重试 / 采样导致的覆盖收窄，必须打日志，
   否则"没报错"会被读成"全覆盖"。

---

## 9. 侦察改变了什么

**一张表收口**：哪些原定义被推翻、为什么、改成了什么。**动手前先扫这张表。**

| # | 原定义（回填前）| 侦察发现 | 新定义 / 新约束 | 来源 |
|---|---|---|---|---|
| 1 | A1 = 把现有 `SandboxInstanceId` **升格** | 它只在 `[custom_extension].url` 配置时生成，而该项被注释掉、`deploy/` 零命中 ⇒ **任何已部署环境从未生成过** | A1 按**新建字段**排期；复用的是**语义 + 9 个单测**，不是机制。outcome 文档 §3「aenv 内部已经有半个 execution 了」**要打折** | R2 |
| 2 | 换代判定按 start / resume 分 | **"从模板/快照创建"走的是 `LaunchMode::Resume`**（`firecracker/sandbox.rs:551-561`）| 必须按 **`LaunchPlan` 变体**判；建议在 Create / Resume 两个变体上各带 `execution_id`，由类型系统保证"各换一次代" | R2 |
| 3 | ExecutionID 的字段命名 / 校验位置待定 | 仓内已有**上线中的同构 fencing 先例** `NodeIdentity.service_instance_id`（`src/identity.rs:19,41-45`；`services/scheduler/internal/service.go:345`；测试 `service_test.go:644`）| A1/A3 的 proto 字段命名、校验位置、错误语义**直接对齐它** | R2 |
| 4 | A2「身份轴 / 版本轴分工按 R3 结论定」（悬而未决）| 三条独立证明（跨节点 resume 一次涨两代 / reclaim 与 releaseHoldings 在无化身参与下涨代 / v3 长驻重试要求"零 generation 抖动"）| 🔴 **两轴必须分开**；generation 保持版本轴语义不动，execution 是**新增**的身份轴 | R3 |
| 5 | A2「`execution_id` NOT NULL」一步到位 | `migrate.go:28-32` 注释自认这套 `ADD COLUMN IF NOT EXISTS` bootstrap 对"新增无默认值 NOT NULL 列"不安全；dev/test 有存量行 | 给默认值 + 回填，或按 P1 drop 重建。**优先纯加法**（见第 6 条） | R3 |
| 6 | Console 切只读 API 与 A2 **同批发布** | 只读 API 早已存在（`registry_list.go`）、Console 反代已注入 X-API-Key、缺口只有 3 个 metadata 派生字段 | 🔴 **按 P4 整块移出本轮**，Console 后续独立更新。代价：它仍直连 PG，**A2 若 drop/改名它 SELECT 的任一列 ⇒ 整页 `source=unavailable`** ⇒ A2 优先纯加法，否则 PR 里显式登记失明 | R4 + P4 |
| 7 | §5 的 S5 与 Console 绑定 | S5 是 `Get`/`GetMany` 拿不到 DB 时钟这个**接口缺口** | **S5 留在 AgentENV 侧**，不随 Console 出局；Console 的同类缺陷 C2 单独记账 | R4 |
| 8 | §5 的 D8「一行待补」 | Rust `central.rs:432` + Go `registry_service.go:688` 映射齐备，还有负向测试 | ✅ **已完成，从 §5 划掉** | R3 |
| 9 | §2 §3.6 一句话：「只能由 controller 做，且必须携带并校验 execution」 | controller **没有下行命令通道**（`scheduler.proto:387-401` 五个 RPC 全是 node 当 client；scheduler 侧 grep http/Dial 零命中）| 🔴 **拆成 §3.6a（A3 兑现"带身份 + SQL 内校验"）+ §3.6b（B6 兑现"发布权集中"）**。不拆 A3 永远做不完 | R1 |
| 10 | A3 =「给三种操作加校验」 | 真正裸奔的只有 `begin_pause` / `mark_running`，**且是 RPC 契约层 `rejectFields` 主动拒收 generation**（`registry_service.go:192`/`:267`）；其余 4 个 CAS 已带守卫 | 🔴 A3 = **改 proto 契约 + 改两条 SQL 的 WHERE + 覆盖数据面 auto-resume 那条写**（`proxy.rs:686`→`:800`）| R5 |
| 11 | A3 的 CAS 面 = 3 处，引用 `postgres.rs:416/:447/:771` | **CAS 是 4 处**（漏 `Remove`）；`postgres.rs` 已被 `4208f47` 删除 | 全部 CAS 只在 `services/scheduler/internal/registry/store_postgres.go:484/:519/:786/:1179`；**旧引用作废** | R3 |
| 12 | A3 的第一发变异未指定 | R5 证明 TTL 自动 pause（1s 一跳）与 reclaim 组合出的**必然序列**会删掉活节点依赖的 durable 副本 | **第一发变异指定打 `begin_pause`** | R5 |
| 13 | A3 要守的面 = 全部旧化身写 | 每次 pause 造全新 `SnapshotId`、OSS 路径天然不相交、层内容寻址 ⇒ 旧化身写 OSS 只留垃圾 | **不可逆损失只有两点**：`completePauseSQL` 翻牌 + `paused_coordinator.rs:313-320` 删 `previous_snapshot_id`。守住这两点即可（也佐证候选 1 可留长期项）| R5 |
| 14 | A4 =「只接受 **controller**」 | 寻址=gateway、执行=node REST；controller 无下行通道 ⇒ 按字面执行等于**谁都进不来**，平台 create/pause/resume/timeout/delete 全被拒，且 §6 把 A4 排在 B 之前 ⇒ 顺序表与原定义**互斥** | 🔴 **改读法：只接受来自 gateway / scheduler 的调用**，gateway = controller 的前端。零跨仓改动（守住 P2）、平台一行不动、§6 自洽 | R1 |
| 15 | A4 靠网络层收窄即可 | 控制面与数据面**共用一个 listener**（`src/api/server.rs:27-29`），数据面是 `.fallback()` 兜底（`proxy.rs:148`）；dev 集群**零 NetworkPolicy** | 只能切在 generated 控制面 router（`src/api/server.rs:24-41`），**不碰** `.merge(proxy::router(...))`；不能靠关端口 / netpol | R1 + R5 |
| 16 | A4 只收沙箱类端点 | `POST /nodes/{id}` 经 gateway 无条件透传 + node presence-only 鉴权 ⇒ 任何能打到 30800 的人都能把节点置 DRAINING；无代码调用方 | **A4 同批收 `POST /nodes/{id}`**（收窄零打击面）| R1 |
| 17 | A4 能覆盖节点自主路径 | TTL 自动 pause（#2）与关机批量 pause（#4）**不需要任何外部请求** | 🔴 网络收窄对它们**完全无效**；**唯一解是 A3 的 SQL 层校验 ⇒ A3 必须先于一切** | R5 |
| 18 | A5 的拒绝码未定 | 平台把 resume 的 **404 读成"沙箱没了 ⇒ 授权重建工作区"**，而那个 404 产地是 gateway（`server.go:328-344` 翻 `codes.NotFound`），请求从未到 node | 🔴 **硬约束：绝不用 404**，用 **409** + 独立错误码；并入 §7.5 陷阱 | R1 |
| 19 | 发布 / 回退按仓内清单 | 30800 的 `agentenv-gateway-nodeport` **不在仓内**（`gateway-service.yaml` 只有 ClusterIP 8080+9102），是手工 apply 的 out-of-band 资源 | **runbook 必须点名它**，`kubectl apply -k` 还原不了 | R1 |
| 20 | B7 可能被 A1 取代 | 节点崩溃时 execution 侧**零信号**（`client.rs:336-347` Drop 兜底只在进程活着时有效）| **A1 不替代 B7**，`release_node_holdings` 保留 | R2 |
| 21 | 与 v3「暂停必然落 OSS」并批只是省工 | v3 把 `publishing` 改成长驻重试态 = **新造一条节点自主写路径** | 🔴 **必须与 A3 同批设计**，否则一边收权一边开新口子 | R5 |
| 22 | envd 数据面这道门的状态未知 | envd token = `HMAC-SHA256(seed, sandbox_id)`（`src/sandbox/access.rs:73-78`），**不绑化身** ⇒ 旧化身 token 在新化身起来后仍有效 | 坐实"候选 2 留后续加强项"，但要认账：**数据面这道门今天完全敞开** | R2 |
| 23 | proto 面待重画（细节未定）| 残留 `TRANSITION_KIND_REMOVE_UNCONDITIONAL`，服务端已 fail-closed 拒服务（`registry_service.go:325-332`）| 随 A2/A3 重画 RPC 面**把枚举值一并删掉** | R3 |

---

## 10. 设计裁决记录

**2026-08-19 · 主 agent 对三份设计文档（[node](_design-phase3-node.md) / [scheduler](_design-phase3-scheduler.md) / [gateway](_design-phase3-gateway.md)）
共 22 条未决项的裁决。**

口径先说清，免得与 §9 混淆：

- **§9 是侦察记录** —— "动手前的事实核查推翻了哪些原定义"，**不再改动**；
- **§10 是设计裁决记录** —— "设计阶段暴露出的分叉，最后选了哪一支、为什么"。
- 每条裁决同时**就地标注在对应设计文档的原未决项处**（原问题陈述一个字不删，它记录了"当初为什么是个问题"）。
  🔴 **两处冲突时以本表为准**，本表是唯一真相源。

| # | 出处文档 | 问题 | ✅ 裁决 | 理由 | 影响到谁 |
|---|---|---|---|---|---|
| 1 | scheduler §6-U1 / node §5-O1 | `resuming` 行的 `execution_id` 是恒空（E-B）还是认领时预分配（E-A）| 🔴 **走 E-A（预分配）**：claim 时分配，与 `claimed_by_node_id` **同事务**写入。node 侧铸造点从 VM start 前移到 resume 决策点，`for_resume` 改为**消费一个只有 claim 能构造的 token**（`ClaimedExecution`），「拿到 `LaunchPlan` ⟺ 已铸恰好一个新 execution」的**编译期保证不许丢** | ① 让 `mark_running` 成为真正的"校验 execution"而非"校验 claim 持有者"；② E-B 要求 B3 记得给 `resuming` 开特例，而"注释承诺别处会做但没做"正是本仓踩过的坑；③ 它使 gateway 的 S5 可满足 —— resume 窗口内可正常设防而不必选"不设防" | A1（铸造点 + token 类型）、A2（CHECK 状态集含 `resuming`）、A3（`markRunningSQL` 分支 ①）、A5（`resuming` 报 `REGISTRY`）、B3（不必开特例） |
| 2 | node §5-O5 + scheduler §3.5 + gateway §13-D5 | 化身冲突用什么错误码；三段链路各自的形状 | **三件套定稿**：① scheduler RPC 新增 `ErrExecutionFenced → codes.PermissionDenied → Rust `ExecutionFenced`，**永不重试**，与 `GenerationConflict → codes.Aborted`（重读再试）**严格分开**；② node → gateway：**412** + `x-agentenv-refusal` 作内部信号；③ gateway → 客户端：**409** + `code=sandbox_execution_superseded`。🔴 任何一环**不许 404**、**不许复用 410** | 合并成一个码会让节点重读拿到新 generation 再发一次，**绕过 fencing**；404 是平台"授权重建工作区"的唯一判据，落成 404 = 用户工作区蒸发；410 已被 node `/proxy` 占用（not proxyable）| A3、A5、node 的 `/proxy` 接收端、平台侧语义（本轮平台零改动）|
| 3 | scheduler §0.2 / §2.4（相对任务书的偏离 #2）| 任务书字面写 `execution_id NOT NULL`，设计改成 nullable + 按状态 CHECK | **采纳偏离**：`CHECK ((state IN ('running','publishing','resuming')) = (execution_id IS NOT NULL) AND (execution_id IS NULL) = (execution_started_at IS NULL))` | 一刀切 NOT NULL 会逼出哨兵 UUID，而哨兵迟早被人拿去比相等，fencing 无声破掉；CHECK 严格更强，还管住"不该有的时候没有" | A2；🔴 **§3 的 A2 行已同步改写**，否则实现 agent 会照 `NOT NULL` 写 |
| 4 | node §5-O2 + scheduler §2.7/§2.8（#7）| 存量形态怎么清：node 的本地 paused 记录、scheduler 的登记表，各自怎么处置 | **两处"清干净"是同一次破坏性操作的两半，必须写进 runbook 的同一个步骤**；node 的 `serde(default)` **不加**；但**加载失败必须响亮报错并指明处置命令**，不许静默丢弃；`Migrate` 的 fail-fast 自检**保留** | 分开写就一定会有集群只做一半，"中央说没有、节点说有"的半清状态比两边都不清更难查；`serde(default)` 等于让"缺化身"静默通过，正是要防的东西 | A2 的发布 runbook、node 的持久化加载、运维 |
| 5 | gateway §13-D1 / §5-S1 | binding 存储要不要持久化 execution | 🔴 **做。它是 A5 的成败点，不是优化项。** 附加硬要求：**必须在 HA / query-only 副本形态下验证** | 热路径命中 binding 就 return、从不读登记表；HA 下数据面走 query-only 副本（连 placer 都没有）⇒ 除 binding 外无路可走。**本地单 scheduler 测不出该失效** | A5、scheduler 的 binding store（内存 + Redis）、验收环境形态 |
| 6 | gateway §13-D2 / §5-S2·S3 | 同 sandbox 两个化身冲突时怎么仲裁 | **热路径按 UUID v7 字典序大者胜** + 对"较小者覆盖较大者"打 **warn**（时钟回拨信号）；**登记表在被查询时是真相** | 热路径不能为每次冲突付一次 DB 读；v7 单调依赖墙钟，没有那条 warn 就发现不了回拨 | A5、`ReconcileNode` / `rosterHolder`（今天是到达顺序仲裁）|
| 7 | gateway §13-D3 | A2 要不要给 running 沙箱建行（`mark_running` 今天不建行）| **不建。`BeginPause` 保持唯一建行者。** 缺口按 gateway 的论证是安全的（无行 ⇒ 物理上单化身），但要**写成显式的「已知覆盖缺口 + 为什么安全」**，不许留成隐含假设 | 建行会把"曾被暂停过的沙箱"这条表语义漂成"所有沙箱"，写入面变大；缺口可论证为安全且**大小可测**（`unfenced_no_authority`）| A2（不动）、A5（永久 `UNKNOWN` 缺口 + 指标）|
| 8 | gateway §13-D4 / §5-S5 | resume 窗口内是"可能误拒"还是"不设防" | **走 E-A 使 S5 成立 ⇒ 窗口内正常设防**，不选"不设防"。仅当**登记表答不出权威化身**时报 `PENDING` 并放行 | E-A 让化身与 holder 同事务落表，误拒的根源（holder 变了而化身还是旧的）消失；数据面读路径 fail-open 可接受，因为**破坏性写已由 A3 在 SQL 层挡住** | A5、A2/A3（E-A 的连带改动）|
| 9 | gateway §13-D6 / §12-R-9 | A4 收窄会不会打死 gateway 的集群列表扇出 | 🔴 **A4 必须放行 gateway → node 的 `GET /sandboxes`**（含 `GET /v2/sandboxes`）；**只放行只读 GET，`POST /sandboxes` 仍收**。**已同步写进 node 设计 §3.3 的 A4 豁免清单**（那份原先只列 `/health`）| 该端点 all-or-nothing（`cluster_list.go:84-96`），任一 node 失败整体 502；而扇出走 gateway 自己的 client，拿不到注入的 token | A4（豁免清单 + 一发对照测试）、A5/A6 的聚合端点 |
| 10 | gateway §13-D7 | 闸 3（长连接周期性撤销）本轮做不做 | **本轮不做。** 残留缺口**照实保留在文档里**，标注"已知、有意推迟" | 不过度设计；闸 1/闸 2 是本轮价值主体，闸 3 是唯一要在 gateway 起后台 goroutine 的部分 | A5（判据"被拒非仅改道"对**已建立的长连接**本轮不兑现）、`requestContextForProxy` 与它那发既有测试**本轮不动** |
| 11 | gateway §13-D8 | `execution_fencing` 默认值 | **默认 `enforce`**，发布时**先跑一轮 `observe`** | 默认值该指向终态；把保守写进默认值等于永远有集群停在 observe 上没人翻 | A5 的配置与发布 runbook |
| 12 | gateway §13-D9 / §8 | `GET /v2/sandboxes` 的非确定性排序缺陷算不算本轮 | **并入本轮** | 双活时 `startedAt` 与 `sandboxID` 全同 ⇒ `sort.Slice` 不稳定 ⇒ 胜者随机；`cluster_list.go:248-249` 的 TODO 自己写了正解（就是 ExecutionID），分开做要改两遍 | A5/A6、`listedSandbox` DTO、重复计数指标 |
| 13 | node §5-O3 | 另外四条转换（`complete_pause` / `mark_local_only` / `release_claim` / `remove`）要不要同批带 execution | **本轮不加**（generation CAS 已够）。🔴 **但三条夺权路径（reclaim / releaseHoldings / releaseClaim）必须清空 execution** —— 这条**写死在 node 与 scheduler 两份文档里** | 那四条上化身谓词是纵深不是主闸，每多一个必填字段就多一处"忘了传"的失败面；而不清空的话，"reclaim → B 接管 → 老节点 1 秒自动 pause 打回来"那条**必然序列第一步就成立** | A3（谓词只加两条）、A2（清轴）、契约层（四个 kind 拒收 execution）、`T-A3-5` 要重排成"两个 kind 各产一种拒绝" |
| 14 | node §5-O4 | wire 名 `sandboxInstanceId` 改不改成 execution | **不改。** 在 openapi 的 `description` 与 Rust 侧**各加一行注释**说明内部名与 wire 名的关系 | A1 的一个明确收益就是不动 `openapi.yml`、不重新 codegen，改名会把这收益吐回去；如无必要勿增实体 | A1（`custom_extension_api` 零 codegen）|
| 15 | node §5-O6 | pre-merge layer 依赖 axum `Router::merge` 保留各自 layer 这一语义 | **接受现状**，但那发行为测试要**显式命名并加注释说明它守的是什么不变式** | 它长得像"测框架行为"的冗余测试，不写清楚就会被后人当噪声删掉，而它是整个 pre-merge 方案唯一的守卫 | A4（`T-A4-1`）|
| 16 | node §5-O7 | gate 与 `resume_isolation_gate` 的顺序 ⇒ 隔离节点上无 token 的 resume 拿 503 而非 403 | **接受**，记为**已知偏差** | 不构成安全问题（503 不泄露、不执行），仅诊断信息略差；把 gate 提到最外层就要在 gate 里再判路径，那是明确反对的写法 | A4 的验收 runbook（探针前置条件）|
| 17 | node §5-O8 | node 侧缺 A5 的接收端 | **通道已由 gateway 设计解决，但接收端是 node 侧的新增工作**：实现 `x-agentenv-expect-execution-id` 比对与 **412** 拒绝、并**始终回声** `x-agentenv-execution-id`。**已加进 node 设计的 A1/A4 工作清单（新增 §3.7）** | 它正好落在两份设计的接缝上，不点名就会两边都以为是对方的活 | A4/A5、`src/api/proxy.rs`（比对必须在 auto-resume **之前**）|
| 18 | scheduler §6-U2 | `ExecutionFenced` 用 `codes.PermissionDenied` 合不合适 | **采纳。** 附注：**A4 若给该 gRPC channel 加 mTLS/token，必须复查**语义是否与传输层鉴权失败混淆 | 它逐字就是"你不是有资格做这件事的那个实体"；今天 scheduler gRPC 无 auth interceptor，不撞语义 | A3、A4（加鉴权时复查）|
| 19 | scheduler §6-U3 | `remove` 要不要带 execution | **采纳"不带"**：保持 generation-only，契约层明确拒收 | 真正的收益（平台重试队列里的陈旧 delete = G3）要平台传 execution，P2 明确本轮不做；提前开口只会引入一条 fail-open 分支 | A3、后续 G3 那一批 |
| 20 | scheduler §6-U4 | 要不要引入 migration 框架 / 版本表 | **采纳"不引入"** | 本阶段唯一的非幂等动作是一次由运维执行、有 runbook 的 `DROP TABLE`；为一次性动作造永久设施是增熵 | A2；B 批次真需要多步迁移时再引（那时表已全新）|
| 21 | scheduler §6-U5 | `beginPauseSQL` 要不要为 `local_only` 开 fail-open 例外 | **采纳"不开"** | 任何 fail-open 分支就是全部攻击面；代价不是数据丢失（沙箱本机仍可唤醒，只是这次快照没进登记表），且该窗口已被护栏 §3.5 收窄 | A3 |
| 22 | scheduler §2.5（相对任务书的增项 #8）| 要不要顺手加 `execution_started_at` | **采纳，本轮就加** | `updated_at` 被 `renewLeaseSQL:925` **每心跳**刷新，B3 的 `KillOrphan` grace 不能拿它当起点；现在加是一次 DDL，B3 再加是第二次改表 | A2、B3 |

### 10.1 ✅ 命名/口径不一致（N1–N5，2026-08-19 已全部裁决）

> 这几条是回填裁决时在三份设计之间发现的**同一个东西不同叫法**。
> **下表的问题陈述一个字不删**（它记录了当初三处各写了什么）；
> 🔴 **裁决在 [§10.2](#102--n1n5-的裁决收口冻结契约)，实现以那一份为准**，五份设计文档正文已按裁决同步改写。

| # | 不一致 | 三处各自的写法 | 为什么必须先统一 |
|---|---|---|---|
| N1 | **header 名的大小写风格** | node A4：`X-AgentENV-Control-Plane`；gateway A5：`x-agentenv-expect-execution-id` / `x-agentenv-execution-id` / `X-Agentenv-Refusal`；仓内既有：`x-agentenv-sandbox-id` / `x-agentenv-target-port` | HTTP 头大小写不敏感，**不会**出运行时故障，但三种拼法（`AgentENV` / `Agentenv` / `agentenv`）会让 grep、文档与测试断言各自对不上 |
| N2 | **同名开关、不同类型** | scheduler：`scheduler.registry.execution_fencing` = **bool**（默认 true）；gateway：`gateway.execution_fencing` = **三态字符串** `off\|observe\|enforce` | 名字一样、类型不同、作用面不同（一个关 SQL 谓词、一个关路由拒绝）。运维读到"execution_fencing 关了"会以为两边都关了 |
| N3 | **同一事实的三个词** | scheduler：`ExecutionFenced` / `fenced`；gateway 对外机器码：`sandbox_execution_superseded` / 头值 `stale_execution` | 三个词描述同一件事（化身被取代）。链路排错要在日志里跨三段串起来，词不统一就串不起来。**注意：码本身已裁决（§10 第 2 条），这里说的只是措辞** |
| N4 | **S1/S2/S3/S6 的归属文档** | gateway §5 提要求；`_design-phase3-scheduler.md` §0 的范围声明**只含** registry + `PausedRegistryService`，**不含** `store.go` / `lookup.go` / heartbeat roster | 它们是 A5 的成败点（§10 第 5 条已裁决"做"），但在现有两份设计里**无归属章节** —— 需确认由 scheduler-A5 那份设计承接 |
| N5 | **发布顺序有三份，方向不同** | A1/A3（scheduler §5.1）：**node 先**；A4（node §3.2）：**gateway 先**；A5（gateway §10）：node → scheduler → gateway observe → enforce | 三条各自都对（依赖方向不同），但**必须合成一条总 runbook**，否则发布日会有人按其中一条推翻另一条 |

### 10.2 ✅ N1–N5 的裁决收口（冻结契约）

**2026-08-19 · 主 agent 裁决。五份设计文档正文已按本节同步改写；两处不一致时以本节为准。**

| # | ✅ 裁决 | 一句话理由 | 已改到哪 |
|---|---|---|---|
| **N1** | 🔴 **HTTP 头一律小写 `x-agentenv-*`**，对齐仓内既有的 `x-agentenv-sandbox-id` / `x-agentenv-target-port`。`X-AgentENV-Control-Plane` ⇒ `x-agentenv-control-plane`；`X-Agentenv-Refusal` ⇒ `x-agentenv-refusal` | 大小写不敏感不出运行时故障，但三种拼法会让 grep / 文档 / 断言三处对不上，排错纯损耗 | node §3.2 / §3.4 / §3.7 / §4.2；gateway §6.3 / §11.1 |
| **N2** | 🔴 **三个开关全部改成作用域显式的名字，只改名、不合并**（见下方「冻结契约 · 开关」表）| 今天两个同名不同类型，运维读到"execution_fencing 关了"会以为两边都关了，而**写路径那个才是致命的**；`_design-phase3-scheduler-a5.md` §12 冲突点 2 的分家理由成立 —— 必须能分别关，共用会让一次止血顺手关掉另一半且失效无声 | scheduler §5.2 / §7；scheduler-a5 §0 / §4.2 / §9 / §11 / §12 / §14；gateway §10 |
| **N3** | 🔴 **上 wire 的字符串只留一个：`sandbox_execution_superseded`**（对外错误码与 `x-agentenv-refusal` 头值**都用它**）。删掉 `stale_execution` 这个第三名。**内部 Go / Rust 错误类型叫 `ExecutionFenced`**（不上 wire）。日志字段名按下方「冻结契约 · 日志」表跨三段统一 | 三个词描述同一件事，链路排错要在日志里跨三段串起来，词不统一就串不起来。码本身早已裁决（§10 第 2 条），这里定的是措辞 | node §3.7 / §4.2；scheduler §3.5；gateway §3.1 / §6.3 / §11.1 |
| **N4** | ✅ **S1/S2/S3/S6 归 [`_design-phase3-scheduler-a5.md`](_design-phase3-scheduler-a5.md)**（该文已存在并承接）。已在 `_design-phase3-scheduler.md` §0 的范围声明里加指针指过去 | 它们落在 `store.go` / `lookup.go` / heartbeat roster，不在 A2/A3 那份的范围声明内；不点名就会掉在两份文档中间 | scheduler §0；scheduler-a5 全文 |
| **N5** | 🔴 **合成一条总发布 runbook，写在 [§6](#6-顺序回退与发布总-runbook)**。三份设计各自的顺序都对但方向不同（A1/A3 要 node 先、A4 要 gateway 先、A5 是 node→scheduler→gateway），必须线性化，否则实施时必然有人按某一份文档的顺序做而打断另一条 | 三条依赖方向不同，不矛盾，但不线性化就没法执行 | **§6 整节重写**；各设计文档的局部顺序段保留为"这一项自己的依赖方向"，**执行以 §6 为准** |

---

#### 🧊 冻结契约 · HTTP 头（三段链路，全部小写）

| 头 | 方向 | 值 | 语义 |
|---|---|---|---|
| `x-agentenv-control-plane` | gateway → node（**控制面**，A4）| 共享 secret token | 证明这条请求来自 gateway/scheduler。gateway 必须**先 `Del` 再 `Set`**；token 为空时必须 `Del`（否则成了客户端 token 的透传管道）|
| `x-agentenv-expect-execution-id` | gateway → node（**数据面**，A5 闸 1）| 小写 canonical UUID v7 | "中央认为该沙箱当前的化身是这个"。**只在 `authority=REGISTRY` 时下发**；gateway 必须先剥掉客户端塞的同名头 |
| `x-agentenv-execution-id` | node → gateway（响应，A5 闸 2）| 小写 canonical UUID v7 | 本机该沙箱**此刻活着**的化身。**放行与拒绝两种响应都要带**（它同时是 node "已装配 A5" 的能力信号）|
| `x-agentenv-refusal` | node → gateway（拒绝，与 **412** 同发）| **`sandbox_execution_superseded`** | 🔴 **内部信号，绝不直通客户端**。gateway 见到 `412` + 本头 ⇒ 改写成 **409** + `code=sandbox_execution_superseded` |
| `x-agentenv-sandbox-id` / `x-agentenv-target-port` | 既有，不动 | —— | 仓内既有路由入参头，N1 的大小写基准就是它们 |

#### 🧊 冻结契约 · 错误码链路（三段，谁也不许复用谁的码）

```
scheduler RPC        ErrExecutionFenced → codes.PermissionDenied → Rust PausedRegistryError::ExecutionFenced   【永不重试】
                     （与 ErrGenerationConflict → codes.Aborted【重读再试】严格分开）
node → gateway       412 Precondition Failed + x-agentenv-refusal: sandbox_execution_superseded               【内部信号】
gateway → 客户端      409 Conflict + {"code":"sandbox_execution_superseded", ...}                              【对外唯一形状】
```

🔴 **任何一环都不许 404**（平台据此"授权重建工作区"，落成 404 = 用户工作区蒸发）、
**不许复用 410**（node `/proxy` 已用它表示 not proxyable）、**不许 503 / 502**。

#### 🧊 冻结契约 · 开关（三个，作用面不同，**只改名不合并**）

| 开关（配置键） | 环境变量 | 类型 / 默认 | 关掉什么 | 落点文档 |
|---|---|---|---|---|
| `scheduler.registry.write_fencing` | `SCHEDULER_REGISTRY_WRITE_FENCING` | bool / `true` | **A3 的两条 SQL 谓词**（`beginPauseSQL` / `markRunningSQL` 的 execution 比较）。实现是"两条 SQL 常量二选一，store 构造时选定" | scheduler §5.2 |
| `scheduler.routing.execution_arbitration` | `SCHEDULER_ROUTING_EXECUTION_ARBITRATION` | `off\|observe\|enforce` / `enforce` | **S1/S2 的 binding 仲裁与 `LookupNode` 应答形状**。实现是"两份 Lua / 两个 `arbitrate` 函数值，构造时选定" | scheduler-a5 §11 |
| `gateway.routing.execution_fencing` | `GATEWAY_ROUTING_EXECUTION_FENCING` | `off\|observe\|enforce` / `enforce` | **gateway 路由层拒绝**（闸 1 下发 expect + 闸 2 回程比对）。`off` 必须是 `decideFencing` 入口第一行 early return | gateway §10 |

> 🔴 **"fencing 关了吗"这个问题在本轮没有单一答案**，必须点名是哪一个。
> 症状 → 该拉哪一个的对照表在 [§6.4](#64-症状--该拉哪个开关)。
> 命名规则：`{服务}.{作用面}.{被关掉的东西}`；环境变量 = 配置键全大写 + `.` 换 `_`。

#### 🧊 冻结契约 · 日志字段（跨三段统一，一次 grep 能串起 gateway→node→scheduler）

| 字段 | 取值 | 三段都要打 |
|---|---|---|
| `sandbox_id` | UUID | ✅ |
| `expected_execution_id` | 小写 canonical UUID v7 或空 | 中央/权威侧认为的那个（gateway 下发的 expect、scheduler 行上的 execution、node 收到的 expect）|
| `observed_execution_id` | 小写 canonical UUID v7 或空 | 事实侧实际的那个（node 本机活化身、scheduler 请求里带上来的 execution、gateway 收到的回声）|
| `refusal_code` | **`sandbox_execution_superseded`** | 只在拒绝时打。**与 wire 上的机器码逐字相同**，不许另起日志专用措辞 |
| `fencing_stage` | `registry_write` / `node_proxy` / `gateway_route` | 哪一段拒的。三个值封闭 |
| `node_id` | 节点 id | scheduler / gateway 侧必打（node 侧是自己）|

🟡 **措辞统一，不是把三个词合并成一个东西**：Go / Rust 的**类型名**仍叫 `ExecutionFenced`
（它是内部符号，不上 wire、不进日志值），指标标签仍用各自已定的封闭集
（`decision="rejected_older"` 等）—— 统一的是**上 wire 的字符串**与**日志字段名**。

---

### 10.3 ✅ scheduler-A5 设计新增未决项的裁决（2026-08-19）

> `_design-phase3-scheduler-a5.md` 是在 §10 那 22 条裁决之后写出来的，它自己又提了 6 条未决（U1–U6）
> 与两条口径纠正。**下表是对它们的裁决**，编号沿用该文的 U 编号（**注意与 `_design-phase3-scheduler.md` 的
> U1–U5 不是同一套**，那一套已在 §10 第 1/18/19/20/21 条收口）。

| # | 出处 | ✅ 裁决 | 理由 | 影响到谁 |
|---|---|---|---|---|
| **A5-U1** | scheduler-a5 §10.0 / §13-U1 | ✅ **采纳**：新增 `SCHEDULER_REDIS_TEST_REQUIRED=1`，与既有 `SCHEDULER_REGISTRY_TEST_REQUIRED` **同风格**，并写进 `services/Makefile` 的 `test-with-postgres` | `redis_store_test.go:156-165` 今天找不到 `redis-server` 就静默 `t.Skip` ⇒ **HA 那一发（M3 唯一的捕手）会消失**，而"漏起 redis"与"全绿"长得一模一样 | scheduler-a5 §10.0 / §14；**§11 的验收命令四个变量全带** |
| **A5-U2** | scheduler-a5 §S2.2 / §13-U2 | 🔴 ✅ **采纳，推翻 gateway §4.1**：`HeartbeatRequest.sandbox_ids = 8` **保留一个发布周期**（加 `[deprecated = true]`），**不立刻 `reserved`**；B1 那批再删 | 新 scheduler + 旧 node ⇒ `roster` 为空 ⇒ 两个 binding 存储都把该节点名下的 binding **删光**（`store.go:90-97`、`redis_store.go:283-291`/`:306-308`）⇒ **所有从未 pause 过的沙箱在整个滚动窗口数据面 404**。🔴 **P1「无向后兼容包袱」指的是没有生产存量，不等于滚动升级期间没有混版本共存** —— 已写进 §0 的 P1 条目下 | gateway §4.1（**作废那句 `reserved 8`**）、scheduler-a5 §S2.2 / §4.1 / §4.2、**§6 runbook 的混版本窗口标注** |
| **A5-U3** | scheduler-a5 §2.2 / §13-U3 | 🔴 ✅ **认账并改判据**：闸 1 的真阳性集合接近空集（路由与 expect 来自同一个 lookup 答案 ⇒ 不匹配只可能是"节点比中央新"）。**A5 的验收判据按 §3 A5 行的新措辞重写**，不粉饰 | 把 A5 说成"能拦截飞行中的旧化身流量"是夸大 —— **那是 A3 与 reclaim 顺序的职责**。A5 的主体是 **binding 带 execution + 仲裁**（治的是"旧化身每个心跳把 binding 抢回去"），闸 2 提供检测与自证，闸 1 只覆盖"node 比中央旧"这一种情形 | **§3 的 A5 行已重写**；scheduler-a5 §2.2 / §13-R1；gateway §7 |
| **A5-U4** | scheduler-a5 §2.2 / §13-U4 | 🔴 ✅ **采纳**：node 侧比对改**有序** —— `live < expect ⇒ 拒`，`live >= expect ⇒ 放行`（小写 canonical UUID v7 字典序即时间序）。**不用等值** | 等值会在「同机 pause→resume」这个**常规事件**（TTL 自动 pause 默认 1s 一跳 + 数据面 auto-resume）上**批量制造 409**，而误拒发生在用户正等着唤醒的时刻。🔴 **必须传导进 node 与 gateway 两份设计**，不能只写在 scheduler-a5 里 | **node §3.7 已改**、**gateway §3.1/§3.2 已改**、scheduler-a5 §2.2 |
| **A5-U5** | scheduler-a5 §S3.3 / §13-U5 | ✅ **采纳，本轮做**：加 `agentenv_scheduler_registry_execution_mismatch{node}` 指标 | 复用 30s shadow reconcile（`reconcile.go:410` 已同时读表与 roster）⇒ **零新增 IO**；它是**双活的直接信号**，比闸 1 的真阳性率有用得多（见 A5-U3） | scheduler-a5 §9 / §14 |
| **A5-U6** | scheduler-a5 §12 / §13-U6 | ✅ **采纳**：`_design-phase3-scheduler.md` 里 E-A **已是裁决态**（§10 第 1 条），该文 §2.2 的 CHECK 与 §2.7 DDL 全文、§3.3 分支①注释**已同步改写**；scheduler-a5 §12 里"A2/A3 文档尚未更新"那句已作废 | 实现 agent 会直接 copy §2.7 的 DDL 全文，那一份原来是 E-B 形状（`resuming` 必须为空）⇒ 照抄会让**每一次跨节点 resume 的 claim 写不进去** | scheduler §2.2 / §2.7 / §6-U1；scheduler-a5 §12 |
| **A5-S1'** | scheduler-a5 §0 第 2 条 | ✅ **采纳口径纠正**：binding 存 execution 的**第一价值是「S2 的仲裁没它写不出来」**，不是"喂 expect 头"。gateway 文档 §5-S1 的表述已改 | 按旧口径读，S1 会被当成闸 1 的配套而随闸 1 一起被质疑价值；实际上它是**缺陷一（到达顺序仲裁）**唯一的修法，与闸 1 的成败无关 | gateway §5-S1；scheduler-a5 §S1 |
| **A5-S8'** | scheduler-a5 §S8 | 🔴 ✅ **采纳并加机械保证**：`PermissionDenied` 在 gateway `writeSchedulerError`（`server.go:334-343`）**没有分支 ⇒ 落 `default` ⇒ 502**。我们给 `ExecutionFenced` 选的正是 `PermissionDenied`，好在它属于 `PausedRegistryService` 而非 `Scheduler` service（node 直连，不经 gateway）。**加一发 `TestSchedulerServiceNeverReturnsPermissionDenied` 钉住「`Scheduler` service 永不返回 `PermissionDenied`」** | 否则将来有人把某个方法挪到 `Scheduler` service 上，一个精确的 fencing 事实就**静默变成 502**（"上游坏了"，一个错误的诊断） | scheduler-a5 §S8 / §10.B / M14；gateway §6.2 |

---

## 11. 实现者速查（三个实现 agent 各取一份）

> 🔴 **本节是设计阶段的收尾产物**：三份设计 + 本任务书里散落的裁决，在这里收成"每个服务要做什么"。
> **§11.1 的冻结契约表是三个实现 agent 的唯一契约来源** —— 任何一处与设计文档正文冲突，以本表为准。
> 各服务的详细论证仍看各自的设计文档，本节只做导航与清单。

### 11.1 🧊 冻结的跨服务契约（唯一真相源）

#### (a) ExecutionID 的表示

| 项 | 冻结值 |
|---|---|
| 生成 | `Uuid::now_v7()`（Rust）/ 与 `NodeIdentity.service_instance_id`、`SandboxId` 三处现有形状一致 |
| 字符串形态 | **小写 canonical**（36 位带连字符）。🔴 **入口必须归一化为小写** —— `isCanonicalUUID`（`store_postgres.go:1395`）接受大写，而 §6 的仲裁整个建立在**字典序**上，`'0'-'9'(0x30) < 'A'-'F'(0x41) < 'a'-'f'(0x61)` 会让顺序反转 |
| proto / JSON 类型 | `string` |
| DB 列类型 | `uuid`（nullable） |
| 比较语义 | **有序**（字典序 = v7 时间序）。**不是等值** —— 见 (d) |
| Rust 类型 | `ExecutionId(Uuid)`，`Copy`，🔴 **不实现 `Default`** |
| Go 侧 | `string`（已归一化的小写 canonical） |

#### (b) HTTP 头（全部小写 `x-agentenv-*`，命名裁决 N1）

| 头 | 方向 | 谁写 | 谁读 |
|---|---|---|---|
| `x-agentenv-control-plane` | gateway → node（控制面）| **gateway**（`Rewrite` 钩子，先 `Del` 再 `Set`；token 空时必须 `Del`）| **node** A4 gate（常数时间比较）|
| `x-agentenv-expect-execution-id` | gateway → node（数据面）| **gateway**（只在 `authority=REGISTRY` 时；必须先剥客户端的同名头）| **node** `proxy.rs`，**必须在 `try_auto_resume` 之前** |
| `x-agentenv-execution-id` | node → gateway（响应）| **node**，🔴 **放行与拒绝两种响应都要带** | **gateway** `ModifyResponse`（闸 2 + 能力探测）|
| `x-agentenv-refusal` | node → gateway（拒绝）| **node**，值恒为 `sandbox_execution_superseded` | **gateway**（翻译成 409）|

#### (c) 错误码链路（三段，谁也不许复用谁的码）

| 段 | 形状 | 语义 |
|---|---|---|
| scheduler RPC | `ErrExecutionFenced` → `codes.PermissionDenied` → Rust `PausedRegistryError::ExecutionFenced` | **永不重试** |
| （对照）版本轴 | `ErrGenerationConflict` → `codes.Aborted` → Rust `GenerationConflict` | 重读再试 |
| node → gateway | **412** + `x-agentenv-refusal: sandbox_execution_superseded` | 内部信号，**不直通客户端** |
| gateway → 客户端 | **409** + `{"code":"sandbox_execution_superseded", ...}` | 对外唯一形状 |

🔴 **任何一环不许 404 / 410 / 503 / 502**。
🔴 **`Scheduler` service（不是 `PausedRegistryService`）永不返回 `PermissionDenied`** ——
gateway 的 `writeSchedulerError`（`server.go:328-344`）没有该分支，会落 `default` ⇒ **502**。
配 `TestSchedulerServiceNeverReturnsPermissionDenied` 钉住（裁决 A5-S8'）。

#### (d) 比对规则（裁决 A5-U4，node 与 gateway 两侧同一份）

设 `live` = 落点上该沙箱**此刻活着**的化身，`E` = 中央下发的 expect：

| 关系 | 动作 |
|---|---|
| `live < E` | 🔴 **拒**（node 412 → gateway 409）|
| `live == E` | 放行 |
| `live > E` | ✅ **放行**并计数 —— 本机比中央新是**常规的同机换代**。🔴 **指标名两侧各有一个、都已定稿，别再另起第三个**：node = `agentenv_proxy_execution_fencing_total{decision="pass_ahead"}`（node §3.9.2）、gateway = `agentenv_gateway_execution_fencing_total{decision="unfenced_node_ahead"}`（gateway §11.8）|
| 落点没有这台沙箱 | ✅ **放行**，落回既有 404 / auto-resume（**只凭正面证据拒绝**）|

#### (e) proto 字段号（🔴 **已按源码核实、零撞号；下表是唯一分配表**）

文件：`services/api/proto/scheduler.proto`

| message / enum | 新增 | 字段号 | **归属 PR** | 核实结论 |
|---|---|---|---|---|
| `TransitionSandboxRequest` | `string execution_id` | **10** | **A2/A3** | 现最大 9，下一可用 10 ✅ |
| `TransitionKind` | 删 `= 6` 改 `reserved 6` + `reserved "TRANSITION_KIND_REMOVE_UNCONDITIONAL"` | （6 保留）| **A2/A3** | 6 现被 `REMOVE_UNCONDITIONAL [deprecated]` 占；`= 7 REMOVE` 不受影响 ✅ |
| `AcquireSandboxRequest` | `string execution_id` | **5** | 🔴 **A2/A3**（**本次定稿**）| 现最大 4，下一可用 5 ✅。**两份设计都会用到它**（E-A 的 claim 预分配），scheduler-a5 §4.1 也列了它 ⇒ **归 A2/A3 加一次**，理由：写入它的是 `claimForResumeSQL`，在 `registry` 包内 |
| `RegistryEntry`（`:405-430`）| `string execution_id` | **10** | **A2/A3** | 现最大 9，下一可用 10 ✅ |
| `RegistrySandbox`（`:318-344`）| `string execution_id` | **13** | **A2/A3** | 现最大 12（`holder_node_id`），下一可用 13 ✅ |
| `LookupNodeResponse` | `string execution_id` | **4** | **A5**（scheduler 侧）| 现最大 3，下一可用 4 ✅ |
| `LookupNodeResponse` | `ExecutionAuthority execution_authority` | **5** | **A5** | ✅ |
| （新）`enum ExecutionAuthority` | `UNSPECIFIED=0 / UNKNOWN=1 / REGISTRY=2 / PENDING=3` | —— | **A5** | 新类型 ✅ |
| `HeartbeatRequest` | `repeated SandboxRosterEntry roster` | **10** | **A5** | 现最大 9（`p2p_endpoint`），下一可用 10 ✅ |
| `HeartbeatRequest` | `sandbox_ids = 8` 加 `[deprecated = true]` | （8 保留）| **A5** | 🔴 **不许 `reserved`**（裁决 A5-U2）；B1 那批再删 |
| （新）`SandboxRosterEntry` | `sandbox_id = 1` / `execution_id = 2` | —— | **A5** | 新类型 ✅ |
| `RecordAssignmentRequest` | `string execution_id` | **3** | **A5** | 现最大 2，下一可用 3 ✅ |

🔴 **`BeginPauseRequest` / `MarkRunningRequest` 这两个 message 在 proto 里不存在** ——
它们是 `TransitionSandboxRequest` 上的 `TransitionKind` 分支（`BEGIN_PAUSE=1` / `MARK_RUNNING=4`）。
A3 要给这两条转换加 execution 谓词，**只能加在 `TransitionSandboxRequest` 上并按 kind 分流**，
proto 层做不到逐 kind 分字段 —— 这正是 `requireExecution` / `rejectFields(…|fieldExecution)` 要逐 kind 明示的原因。

#### (f) 开关（三个，**只改名不合并**，命名裁决 N2）

| 配置键 | 环境变量 | 类型 / 默认 | 关掉什么 |
|---|---|---|---|
| `scheduler.registry.write_fencing` | `SCHEDULER_REGISTRY_WRITE_FENCING` | bool / `true` | A3 的两条 SQL 谓词 |
| `scheduler.routing.execution_arbitration` | `SCHEDULER_ROUTING_EXECUTION_ARBITRATION` | `off\|observe\|enforce` / `enforce` | binding 仲裁 + `LookupNode` 应答 |
| `gateway.routing.execution_fencing` | `GATEWAY_ROUTING_EXECUTION_FENCING` | `off\|observe\|enforce` / `enforce` | gateway 路由层拒绝 |

三者的实现形态统一是 **"两条常量二选一，构造时选定"**，🔴 **不许在语句/脚本里塞 `if flag`**。

#### (g) 日志字段（跨三段统一，一次 grep 串起 gateway→node→scheduler）

`sandbox_id` / `expected_execution_id` / `observed_execution_id` /
`refusal_code`（恒 `sandbox_execution_superseded`）/
`fencing_stage`（`registry_write` \| `node_proxy` \| `gateway_route`）/ `node_id`

> 🔴 **冻结的是"字段集"，不是 message 文本**（2026-08-20 澄清）。跨三段一次 grep 串起来靠的是上面这些
> **字段**；message 是给人读的一句话，各段可以按自己的语义写、也可以在同一段里按结果分成多条。
>
> ⚠️ **gateway 侧就必须分成两条**：`observe` 与 `enforce` 走的是**同一行**日志、带**同一组字段**，
> 但 observe 那一轮请求是 **200 被放行**的。一条统一写着 "refused" 的 message ⇒ observe 期
> `grep refused` 捞回来的全是实际 200 的请求 —— 而 observe 期响应与健康态一模一样，日志正是那时候
> 仅剩的两件观测物之一。落地：`logMsgExecutionRefused` / `logMsgExecutionObserved` 两条常量，
> 字段集一字不动；测试 `TestTheObserveLineDoesNotClaimToHaveRefused` 同时钉住"两条 message 不同"
> 与"六个字段一个不少"。

---

### 11.2 🦀 Rust node（A1 / A4 / A5 接收端 / 🔴 A6 的**全部** openapi 面）

**权威设计**：[`_design-phase3-node.md`](_design-phase3-node.md)（§2 = A1，§3 = A4，§3.7 = A5 接收端，§3.8 = A6，§3.9 = 指标）

🔴 **一次性交付、只滚一次**：A1 + A4 gate + A5 接收端 + A6 响应字段 + preStop 带头**同一个镜像**（runbook 步骤 3），后三者靠配置保持惰性 —— 依据与代价见 [§6.1 的追认块](#61-依赖图为什么是这个线性顺序)。

**要改的文件**

| 文件 | 改什么 |
|---|---|
| `src/types/id.rs` + `mod.rs` | 新增 `ExecutionId`（`Copy`、`Display`、**无 `Default`**）|
| `src/orchestrator/launch_plan.rs` | 两个变体各加**私有** `execution_id`；三个 `for_*` 构造函数；新增 `execution_id()` 访问器；🔴 新增 `ClaimedExecution` token（私有字段 + 只有 claim 路径可构造 + 无 `Default`/`Clone`），`for_resume` **按值消费**它 |
| `src/orchestrator/service.rs` | `build_sandbox` 传参；`transitional_metadata` / `update_if_state` 写字段；`mark_running` 调用带上；fork 子沙箱（`:624-631`、`:701-706` 那行 clone 之后必须覆盖）|
| `src/orchestrator/store/metadata.rs` | `pub execution_id: ExecutionId` 必填、**无 `#[serde(default)]`**；加载失败**响亮报错并指明处置命令** |
| `src/sandbox/backend.rs` | `SandboxBackendFactory` 三方法加必填参；`SandboxBackend` 加 `fn execution_id()`；`SandboxForkSpec` 加必填字段 |
| `src/sandbox/firecracker/sandbox.rs` | 存字段 + 传参；🔴 **绝不放进 `FirecrackerCommonConfig`**（会 serde 进 paused state ⇒ 静默复活旧化身）|
| `src/sandbox/custom_extension/client.rs` | 删 `SandboxInstanceId` 类型；guard 与三个 hook 改签名；9 个单测同步。**wire 字段名 `sandboxInstanceId` 不变、`src/custom_extension_api/openapi.yml` 不改结构、不重新 codegen**，只加 `description` 一行。🔴 **别把这句读成"本轮不碰 openapi"** —— A6 要改的是**另一份**（`src/api/openapi.yml`，见下面两行）|
| `src/sandbox/mock.rs` | 🔴 **必须把 execution 记下来**（否则 A1 的验收只能靠需要 root + `/dev/kvm` 的集成测试，CI 跑不了、变异验证没牙）|
| `src/orchestrator/paused_registry/{mod,central,disabled}.rs` | `mark_running` / `claim_for_resume` 加 execution 参数；`central.rs` 加 `PermissionDenied ⇒ ExecutionFenced` 映射 + 负向测试（照 `central.rs:2090-2126` 的形状）|
| `src/api/impls/{sandbox,paused_recovery,paused_coordinator}.rs` | claim 路径铸 execution 并产出 token；`ExecutionFenced` 的**终态**处理（不重试、不发布、不删快照）|
| `src/api/server.rs` | 🔴 A4 gate 挂在 **generated router 上、`.merge(proxy::router(...))` 之前**；豁免**两条**：`/health` 与 **`GET /sandboxes`（含 `GET /v2/sandboxes`）**，`POST /sandboxes` **不豁免** |
| `src/api/proxy.rs` | A5 接收端：**有序**比对（见 §11.1(d)）+ 412 + **始终回声**；🔴 **必须在 `try_auto_resume` 之前** |
| `src/cfg.rs` | `[api] control_plane_tokens: Vec<String>`（`AENV_API_CONTROL_PLANE_TOKEN`，静态）**加** 🔴 `[api] control_plane_token_file`（挂载文件，**热读**）；两者取并集，**都空 = 全放行 = 回退路径**。热读是 runbook 步骤 9 能不滚 DaemonSet 的唯一前提（§6.1）|
| `src/api/openapi.yml` + `make agentenv-server` | 🔴 **A6 归 node**：`Sandbox` / `SandboxDetail` / `ListedSandbox` 三个 schema 各加 `executionID`，**要动 openapi、要重新 codegen**（与 A1 的 `custom_extension_api` 零 codegen 是两回事，别混）|
| `src/api/impls/sandbox.rs` | A6 的三处 `From<SandboxMetadata>`（`:115` / `:134` / `:185`）各填一行 `execution_id: m.execution_id.to_string()` |
| `deploy/k8s/base/agentenv-daemonset.yaml` | preStop 两条 curl 加 `x-agentenv-control-plane` 头（🔴 preStop 失败是静默的，配 `T-A4-6` 清单校验）；🔴 **加一个 Secret 卷**承载 control-plane token 文件（env `secretKeyRef` **不会**热更新，卷才会）|

**它依赖别的服务先做完什么**

- 🟢 **A1 不依赖任何人**：新字段先写进 `metadata_json`（JSONB 原样透传），旧 controller 认不认都无害 ⇒ **A1 可以先合**。
- 🔴 **A4 的 gate 生效依赖 gateway 先注入**（runbook 步骤 8 → 9）。代码在步骤 3 就上，但 **token 文件与 env 必须留空**到 gateway 注入验证通过。
- 🟢 **A6 不依赖任何人**：三个响应字段是 additive，gateway 侧 DTO 还没改时会被静默丢弃（= 今天的行为），改了就看得见。
- 🔴 **A5 接收端依赖 gateway 下发 expect 头**，而 gateway 只在 `authority=REGISTRY` 时下发 ⇒ 依赖 scheduler 的 S1/S4 先落地。缺头必须**放行**。
- 🔴 **E-A 的硬耦合**：`mark_running` 送上去的**必须是 claim 时那一个**（`AcquiredSandbox.entry.execution_id`），**不得自己再铸**。送新铸的值 ⇒ `markRunningSQL` 分支 ① 谓词不匹配 ⇒ **每一次跨节点 resume 都失败**。🔴 **这是本轮最容易写错的一处，变异验证要专打它（`T-A1-6`）**。

**验收命令**

```bash
cd /home/debian/.orbitmux/projects/pj-8v5-AE/worktrees/sandbox-all/apps/AgentENV
cargo test -p agentenv --lib                    # A1 的 T-A1-1..9 + A4 的 T-A4-1..5 / 7..9 + A5 的 T-A5N-1..7 + A6 的 T-A6-1/2 全在这里
                                                # （T-A4-6 是 deploy 清单校验，不在 cargo test 里）
cargo test -p agentenv --lib -- a_resume_runs_under_a_new_execution   # 单发
# 集成层（需 root + /dev/kvm，不承担变异验证，只做一次落地确认）
sudo -E cargo test --test orchestrator_integration
```

---

### 11.3 🐹 Go scheduler（A2 / A3 + A5 的 scheduler 侧）

**权威设计**：[`_design-phase3-scheduler.md`](_design-phase3-scheduler.md)（A2/A3）
+ [`_design-phase3-scheduler-a5.md`](_design-phase3-scheduler-a5.md)（S1–S8）
🔴 **两份是同一个服务的两块互不重叠的地界**：前者管 `registry` 包 + `PausedRegistryService`（写路径 + DDL），
后者管 binding / lookup / roster（读路径 + 路由答案）。**唯一共用面是 proto，按 §11.1(e) 的归属分**。

**要改的文件（A2/A3 那半）**

| 文件 | 改什么 |
|---|---|
| `services/api/proto/scheduler.proto` | §11.1(e) 里归属 **A2/A3** 的全部字段（含 `AcquireSandboxRequest.execution_id = 5`）|
| `registry/migrate.go` | 新 `SchemaDDL`（🔴 CHECK 的状态集**含 `'resuming'`**）+ **fail-fast 自检**（必须早于 `ADD CONSTRAINT`，报"一句中文 + 一条命令"）|
| `registry/store_postgres.go` | `entryColumns` 加两列；`beginPauseSQL` / `markRunningSQL` 按设计重写；`completePause`/`markLocalOnly`/`releaseClaim`/`reclaimReleased`/`releaseHoldingsReleased` **清轴**（🔴 三条夺权路径清轴是**硬要求**，不清则必然序列第一步就成立）；`claimForResumeSQL` **写入** execution；`MarkRunning`/`BeginPause` 的 0 行再读**包进 `tx`** |
| `registry/store.go` | 接口签名加 execution；`Sandbox` 加两字段；新增 `ErrExecutionFenced` |
| `registry/postgres.go` | `selectColumns` 加 `execution_id`（🔴 **两份列清单漏一份的症状是"某条路径恒空" = fencing 恒放行**）|
| `registry_service.go` | `fieldExecution` + `requireExecution`（🔴 **只挂 `BEGIN_PAUSE` / `MARK_RUNNING`**，其余四个 kind `rejectFields(…\|fieldExecution)`）；`registryErrorCode` 加 `ErrExecutionFenced ⇒ PermissionDenied` 与 `23514 ⇒ ErrInvalidRecord`；删 `:325-332` 的 `REMOVE_UNCONDITIONAL` case（但**保留**断言 `kind=6` 仍 `InvalidArgument` 的测试）|
| `services/shared/config/config.go` | `SchedulerRegistryConfig.WriteFencing bool`（默认 `true`）+ `SCHEDULER_REGISTRY_WRITE_FENCING` |
| `registry/legacy_schema_test.go`（新）| `legacyNodeSchemaDDL` 化石常量（合并两份测试拷贝），注释写明"**它是化石，永不跟随 `SchemaDDL` 更新**" |

**要改的文件（A5 那半）**

| 文件 | 改什么 |
|---|---|
| `services/api/proto/scheduler.proto` | §11.1(e) 里归属 **A5** 的全部字段 |
| `internal/store.go` | `Binding` / `RosterEntry` 类型；`BindingStore` 三方法签名；`arbitrate` + 两个策略值；`upsertLockedWithExpiry` 前置仲裁 |
| `internal/redis_store.go` | `redisBindingRecord.ExecutionID`；`parse_node_id` → `parse_binding`；🔴 **仲裁必须进 Lua**（Go 侧"GET→比较→SET"就是 e2b 那条教训的同构）；两段脚本各出 fenced/unfenced **两份常量**并返回 per-sandbox `decision` |
| `internal/lookup.go` | `lookupResponse` 扩成五参、五个出口**显式**传值（不给默认值，让"新增出口忘了分类"变编译错误）；`:230-294` 两分支硬编码 `("", PENDING)`；`rosterHolder` 改按 execution 仲裁、平手退回 `lastSeen` |
| `internal/service.go` | `rosterFromHeartbeat` 收口两代字段 + legacy 计数；`RecordAssignment` 🔴 **必须走同一条仲裁**（否则是绕过 S2 的后门）|
| `internal/node_registry.go` / `reconcile.go` | roster 类型；`normalizeExecutionID`（trim + 校验 + **小写化**）；`execution_mismatch` 统计 |
| `internal/metrics.go` | scheduler-a5 §9 的六个 series |
| `services/shared/config/config.go` | `SchedulerConfig.Routing.ExecutionArbitration`（默认 `"enforce"`，**非法值启动即报错**）|
| `services/Makefile` | `test-with-postgres` 加 `SCHEDULER_REDIS_TEST_REQUIRED=1`（裁决 A5-U1）|
| `internal/redis_store_test.go` | `startRedisServerForTest` 的 `t.Skip`（`:164`）受 `SCHEDULER_REDIS_TEST_REQUIRED` 门控 |

**它依赖别的服务先做完什么**

- 🔴 **A2/A3 依赖 node 的 A1 先发布**（否则旧 node 不发 `execution_id` ⇒ 必填校验 ⇒ 全集群 pause/resume `InvalidArgument`）。**代码可以并行写，发布顺序不可交换**（runbook 步骤 3 → 4）。
- 🔴 **A2 依赖运维执行 runbook 步骤 2 的破坏性步骤**（`DROP TABLE` + 清节点本地记录，**同一步的两半**）。
- 🔴 **A5 的 `sandbox_ids` 回落必须实现**（裁决 A5-U2），否则滚动窗口里所有从未 pause 过的沙箱数据面 404。
- 🟡 gateway 侧不依赖 scheduler 先合代码（proto 是 additive），但 `enforce` 的翻转依赖 `legacy_roster_total` 归零。

**验收命令（🔴 四个变量一个都不能少）**

```bash
cd /home/debian/.orbitmux/projects/pj-8v5-AE/worktrees/sandbox-all/apps/AgentENV/services

# 规范入口（首选）：自己拉 throwaway postgres + 检查 redis-server
make -C /home/debian/.orbitmux/projects/pj-8v5-AE/worktrees/sandbox-all/apps/AgentENV/services test-with-postgres

# 手跑单发
GOWORK=off \
SCHEDULER_REGISTRY_TEST_DSN='postgres://postgres:verify@127.0.0.1:15499/aenv_registry?sslmode=disable' \
SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
SCHEDULER_REDIS_TEST_REQUIRED=1 \
REDIS_SERVER_BIN="$(command -v redis-server)" \
  go test -count=1 -run TestAReclaimedSandboxCannotBePausedBackByItsOldNode ./scheduler/internal/registry/

# 若本机没 PG
docker run -d --rm --name aenv-pg -e POSTGRES_PASSWORD=verify \
    -e POSTGRES_DB=aenv_registry -p 15499:5432 postgres:16-alpine
```

| 变量 | 少带的后果 |
|---|---|
| `GOWORK=off` | 直接报 `directory prefix . does not contain modules listed in go.work`（父 worktree 的 `go.work` 未列入 `apps/AgentENV/services`）|
| `SCHEDULER_REGISTRY_TEST_DSN` | **125 个 SQL 用例静默 skip**，裸跑只剩 40 个纯逻辑测试 |
| `SCHEDULER_REGISTRY_TEST_REQUIRED=1` | "漏起 PG"与"全绿"长得一模一样 |
| 🔴 `SCHEDULER_REDIS_TEST_REQUIRED=1` + `REDIS_SERVER_BIN` | **HA 那一发（M3 唯一的捕手）静默消失**，而 M3 的后果不是本地红、是 **HA 生产静默失效** |

---

### 11.4 🐹 Go gateway（A4 注入 / A5 三段闸 / A6 **只有那三个 DTO**）

**权威设计**：[`_design-phase3-gateway.md`](_design-phase3-gateway.md)

🔴 **A6 的边界**：gateway 只负责**自己拼的**三个响应 DTO（`GET /sandboxes`、`GET /v2/sandboxes`、`GET /registry/sandboxes`）**别把 node 新加的字段吃掉**；`GET /sandboxes/{id}` 与 resume 响应是 node 直通，**openapi 与 codegen 全归 node**（§11.2），gateway 一个字都不改。

**要改的文件**

| 文件 | 改什么 |
|---|---|
| `internal/server.go` | ① **A4**：`Rewrite` 钩子里 **无条件 `Set`** `x-agentenv-control-plane`，token 空时 **`Del`**（🔴 "没有才加"= 把 presence-only 换了个头名）；② **A5 闸 1**：`decideFencing` **纯函数**（`off` 是入口第一行 early return），数据面 + `authority=REGISTRY` 时**先剥后盖** `x-agentenv-expect-execution-id`；③ **A5 闸 2**：`ModifyResponse` 里**有序**比对回声（§11.1(d)），`<` 才拒；④ 拒绝构造函数 `writeStaleExecutionRefusal` ⇒ **409** + 标准 body；⑤ `RecordAssignment` 带上从响应头拿到的 execution |
| `internal/cluster_list.go` | `listedSandbox` 加 `ExecutionID`（🔴 不加 ⇒ node 报了也被 DTO **静默吃掉**）；去重冲突时**按 execution 字典序取大**，两边空则退回 keep-first；`cluster_list_duplicate_total` 计数 |
| `internal/registry_list.go` | `registrySandboxItem` 加 `ExecutionID`。🔴 **`registryListQueryParams` 那个封闭参数集不许加 `executionID` 过滤**（一能过滤，下一步就有人拿它当入参）|
| `internal/metrics.go` | gateway §11.8 的指标（标签集**封闭**）|
| `services/shared/config/config.go` | `GatewayConfig.Routing.ExecutionFencing`（默认 `"enforce"`）+ `GATEWAY_ROUTING_EXECUTION_FENCING`；`GATEWAY_CONTROL_PLANE_TOKEN` |
| **不改** | `requestContextForProxy`（`server.go:771-776`）与 `TestRequestContextNotCanceledWhenStreamingCancelCalled` —— 🔴 **闸 3 本轮不做，这两处本轮一个字都不要动** |

**它依赖别的服务先做完什么**

- 🔴 **闸 1 依赖 scheduler 的 S1/S4**（`LookupNodeResponse` 带 `execution_id` + `authority`）。拿不到 ⇒ 全是 `UNKNOWN` ⇒ 不下发、不拒、计 `unfenced_no_authority`。
- 🔴 **闸 2 依赖 node 的 A5 接收端**（始终回声）。收不到回声 ⇒ 放行 + 计 `unfenced_node_silent`。**翻 `enforce` 前该指标必须为 0。**
- 🔴 **A4 的注入必须早于 node 开 gate**（runbook 步骤 8 → 9）；**回退时反过来，node 先**。
- 🔴 **A4 的 node 侧收窄必须放行 `GET /sandboxes`**（含 `/v2`）：`handleClusterList`（`cluster_list.go:72-119`）是 **all-or-nothing**（`:84-96`，`http.Error` 在 `:94`），任一 node 失败**整体 502**；而扇出走 gateway 自己的 client，**不经 `ReverseProxy.Rewrite`**，拿不到注入的 token。

**验收命令**

```bash
cd /home/debian/.orbitmux/projects/pj-8v5-AE/worktrees/sandbox-all/apps/AgentENV/services
# gateway 的测试全部 stub 掉 scheduler client，不需要 PG / Redis
GOWORK=off go test -count=1 ./gateway/...
GOWORK=off go test -count=1 -run TestFencingRefusalIsNeverFourOhFour ./gateway/internal/

# 🟡 但只要同一条命令行会被复制去跑 scheduler 包，四个变量就照带（少带一次就是一次假绿）
GOWORK=off \
SCHEDULER_REGISTRY_TEST_DSN='postgres://postgres:verify@127.0.0.1:15499/aenv_registry?sslmode=disable' \
SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
SCHEDULER_REDIS_TEST_REQUIRED=1 \
REDIS_SERVER_BIN="$(command -v redis-server)" \
  go test -count=1 ./...
```

---

### 11.5 三个 agent 的交界处（**最容易两边都以为是对方的活**）

| 交界 | 谁做 | 判据 |
|---|---|---|
| A5 的 expect 头**发**与**收** | 发 = gateway，收 = **node**（`proxy.rs`，不是 A4 的 gate）| node §3.7；A4 的 gate 结构性地不覆盖 `/proxy` 与 fallback |
| `RegistryEntry` / `RegistrySandbox` / `AcquireSandboxRequest` 的 execution 字段 | **A2/A3 的 PR 加一次** | §11.1(e)；A5 那份只消费 |
| E-A 的 claim 预分配值 | scheduler 写（`claimForResumeSQL`），**node 复用**（不得再铸）| 写错 ⇒ 每一次跨节点 resume 都失败 |
| 三条夺权路径清 `execution_id` | scheduler（`reclaimReleased` / `releaseHoldingsReleased` / `releaseClaim`）| 不清 ⇒ A3 整条 fencing 白做 |
| `sandbox_ids` 回落 | scheduler（`rosterFromHeartbeat`）| 不做 ⇒ 滚动窗口全线 404 |
| `GET /sandboxes` 豁免 | **node**（A4 gate 的豁免清单），需求方是 gateway | 不做 ⇒ 集群列表整体 502 |
| A6 的 `executionID` | 🔴 **两边都要做，做的不是同一件事**：node 改 `openapi.yml` + codegen + 三个 schema（`Sandbox`/`SandboxDetail`/**`ListedSandbox`**）；gateway 只改自己那三个 DTO | node 漏 `ListedSandbox` ⇒ gateway 的 DTO 拿不到值，改了也恒空；gateway 漏改 ⇒ node 报了被 `json.Decode` **静默吃掉**。**两种失败长得一模一样：字段恒空、零报错** |
| 有序比对 | **node**（主）+ gateway（闸 2 同规则）| 写成等值 ⇒ 每次同机 pause→resume 都 409 |
| `Scheduler` service 不返回 `PermissionDenied` | scheduler（A5 那半配测试）| 违反 ⇒ gateway 落 502，诊断错误 |

---

## 12. 已知同步债（**只登记，本轮不动**）

> 🔴 **为什么单列一节**：本轮修掉的那条（Go 侧 `requiredMetadataFields` ↔ Rust `REQUIRED_FIELDS`）
> 不是孤例，它只是**唯一一条已经咬过人的**（`execution_id` 漏登记、单向无牙、全绿）。
> §12.1–§12.3 是同一类：**两侧（甚至三侧）手抄同一个事实，靠注释承诺"保持同步"，没有任何机制会在漂移时变红**。
> §12.4 是本轮已还的那一条（留作模板）。**§12.5 是 2026-08-20 新立的，类别不同**
> —— 手抄的两侧不是两份代码，而是**仓内部署清单与集群实际状态**，但同样属于"只登记、本轮不动"。
>
> 🔴 **本轮不动它们**：三处都不属于阶段 3 的改动面，现在改会把爆炸半径从"新加的东西"扩大到"既有主路径"。
> 登记在这里，是为了下一个碰它们的人不必再考古一遍。
>
> **本节所说的"有牙的做法"只有两个模板，都在仓内、都已验证**：
> 1. `services/shared/config/manifest_test.go` —— 直接解析对方的产物（`deploy/k8s/base/*.yaml`、`config/default.toml`），
>    不复述。`CLUSTER_ID` 一漂就红。
> 2. 本轮为第 1 条建的机制（2026-08-20）—— **一侧把事实导出成 fixture、另一侧读同一份文件做集合相等**：
>    Rust `src/orchestrator/store/metadata.rs` 的 `the_required_field_list_is_published_for_the_other_side`
>    把 `REQUIRED_FIELDS` 写进 `tests/fixtures/sandbox_metadata_required_fields.json`（`UPDATE_METADATA_GOLDEN=1` 重生成、不重生成就红），
>    Go `services/scheduler/internal/registry/metadata_golden_test.go` 的 `TestTheRequiredFieldListMatchesTheNodes`
>    读同一份文件与自己的列表做**集合相等**（不是包含 —— 包含就是那条单向无牙的旧机制）。
>    🔴 **判据**：两个方向各有一发变异能让它红。做不到这一点的"同步"都只是注释。

### 12.1 `LeaseExpired` —— 租约谓词，**三处**手抄（不是两处）

| 项 | 内容 |
|---|---|
| **位置** | `services/scheduler/internal/registry/registry.go:121`（`func (s Sandbox) LeaseExpired`）|
| **它同步的是什么** | `COALESCE(lease_expires_at, updated_at) < now()` —— "这条行的租约算不算过期"。同一个判据在仓内还有 SQL 版：`services/scheduler/internal/registry/store_postgres.go:50` 的 `const leaseExpired`；`services/scheduler/internal/reconcile.go:249` 也按同一口径读 |
| 🔴 **第三个手抄方** | **主仓 Agent-Console**：`apps/Agent-Console/internal/aenv/pausedregistry/reconcile.go:28 / :90 / :218`，注释自称"与上游 `LEASE_EXPIRED` 逐字一致"。Console 本轮已裁定**后续独立更新**（§6.7），所以这条谓词实际是**三处**手抄，且**跨仓库** —— 三处里没有任何一处会在另外两处改动时变红 |
| **漂移会怎样** | 判据只要有一处宽一点（比如把 `COALESCE` 落成 `lease_expires_at < now()`），**没写过租约列的行就永远不过期**：该行既不会被回收、也不会被别的节点认领，卡在那里没人管；反过来收紧一点，则是把活行判成过期 —— 那是**双活**的入口。Console 那一份漂了不会动数据（它只读），但会让运维在一个**与控制面结论不一致的界面**上做决定，这类分歧最难查 |
| **有牙的做法** | 谓词是 SQL，天然可导出：让 Go 侧的 `leaseExpired` 常量成为唯一真相源（或反过来），由测试把它写进一份 fixture，`LeaseExpired` 与 Console 各自读它、断言逐字相等。跨仓那一段没有共享文件系统，所以 Console 那份的现实做法是**读一个由 scheduler 暴露的常量端点**，或至少在 Console 侧写一发"对着同一批行跑两种判据，结论必须一致"的对拍测试 |
| ⚠️ **登记时发现的新事实（未改判、未动代码）** | `registry.go:121` 的注释写的是"mirrors **the node's** LEASE_EXPIRED predicate verbatim"，**但 Rust 侧已经没有这个东西了**：`grep -rn "LEASE_EXPIRED\|COALESCE" src/` 零命中，它在 `4208f47 refactor(paused-registry): take PostgreSQL off the node` 里随 node 侧 SQL 一起删掉了。⇒ 这条注释指向的对面**不存在**，真相源实际已经是 Go 侧自己的 SQL 常量。这不影响本条债的性质（仍是三处手抄），但**修它的时候别再去 Rust 里找对面** |

### 12.2 `Store` 接口 —— 逐方法镜像 Rust trait

| 项 | 内容 |
|---|---|
| **位置** | `services/scheduler/internal/registry/store.go:15`（`type Store interface`）|
| **它同步的是什么** | Rust 的 `PausedRegistry` trait（`src/orchestrator/paused_registry/mod.rs:106-306`）。注释明说是**逐方法镜像**而不是照 5 个 RPC 的形状，理由也写在那里：证明这层翻译正确的测试是 node 自己的测试，它们是照这组操作写的。当前对照：Rust trait 12 个方法（`begin_pause` / `complete_pause` / `mark_local_only` / `get` / `get_many` / `claim_for_resume` / `release_claim` / `renew_lease` / `reclaim_expired_holdings` / `mark_running` / `release_node_holdings` / `remove`），Go 侧同名 12 个 + `Migrate`（这一个是 Go 侧独有，node 从不迁移表）|
| **漂移会怎样** | 一侧加了方法、另一侧没有 ⇒ 编译期毫无反应（两个语言、两个模块），只在**跑到那条路径时**才暴露；更隐蔽的是**语义漂移**：方法名一样、返回值含义变了（`mark_running` 从两态答案变成三态就是本轮刚发生过的一次），Go 侧照旧编译、照旧绿 |
| **有牙的做法** | 方法名集合可以像 12.4 那样导出成 fixture 对拍（Rust 侧一个 `const TRAIT_METHODS` + 一发 golden 测试即可，成本极低，能挡住"加了/删了方法"）；**语义**挡不住，那要靠契约测试 —— 仓内已经有形态：`services/scheduler/internal/registry/contract_*_test.go` 就是照 node 的测试逐条重写的一套。缺的不是机制而是**登记**：哪条 Go 契约测试对应哪条 Rust 测试，现在只存在于当初写它的人脑子里 |

### 12.3 `TEST_LEASE_SECS` / `PAST_LEASE` —— 手抄 Rust 测试常量

| 项 | 内容 |
|---|---|
| **位置** | `services/scheduler/internal/registry/contract_test.go:40`（`contractLeaseTTL = 1 * time.Second`，注释"mirrors TEST_LEASE_SECS in the Rust suite"）与 `:43`（`contractPastLease = 1600 * time.Millisecond`，"mirrors PAST_LEASE"）|
| **它同步的是什么** | 契约测试套的两个时间常量：一条租约的长度，以及"睡多久算铁定过期"。它们成对才有意义 —— `contractPastLease` 必须**明显大于** `contractLeaseTTL`，否则在负载高的机器上会抖 |
| **漂移会怎样** | 🔴 这一条漂了**不会红，会假绿或间歇红**，这是最坏的一类：Rust 侧把 `TEST_LEASE_SECS` 调大而 Go 侧没跟，`contractPastLease` 就不再"铁定过期"，于是本该测"过期后可被接管"的用例，在**租约还活着**的情况下跑完并通过 —— 它测的东西凭空消失了，且从此每次都绿。间歇失败那一版还算幸运，至少有人会看 |
| **有牙的做法** | 与 12.2 同源，但更简单：这两个数是纯值，Rust 侧一发 golden 测试写进 fixture（同 §12 开头模板 2），Go 侧读同一份，两边都用 fixture 里的值**而不是各自的字面量**。⚠️ 注意方向：这里要同步的**不是"两侧数字相等"**，而是"两侧用的是同一个数" —— 断言相等仍然允许两边一起被改错，读同一份文件才不会 |

### 12.4 本轮已还的那一条（留作模板，勿删）

`requiredMetadataFields`（`services/scheduler/internal/registry/metadata_golden_test.go`）↔ `REQUIRED_FIELDS`
（`src/orchestrator/store/metadata.rs`）。**2026-08-20 已改成"导出 fixture + 集合相等"**，两个方向各有一发变异证明：

| 变异 | 该红的地方 |
|---|---|
| Go 侧列表**删**一个字段 | `TestTheRequiredFieldListMatchesTheNodes`（Go） |
| Rust 侧 `REQUIRED_FIELDS` **加**一个字段而不重生成 fixture | `the_required_field_list_is_published_for_the_other_side`（Rust） |
| Rust 侧加字段**并**重生成 fixture（`execution_id` 当年的真实形态） | `TestTheRequiredFieldListMatchesTheNodes`（Go）—— 这一格正是旧机制漏掉的那一格 |

### 12.5 部署清单与集群实际**长期漂移**（本轮只登记，不治）

> 🟡 **类别不同**：12.1–12.4 是"两侧手抄同一个事实"，这一条是"**清单与现实**两侧手抄同一套部署"。
> 但归属同一本账：**只登记，本轮不动**。2026-08-20 在 pve-sg dev 实测后立此条。

| 项 | 内容 |
|---|---|
| **现象** | `deploy/k8s/`（仓内 kustomize）与 dev / test 两套 k3s 集群的实际状态，有 **10 处 out-of-band 差异**（完整清单 [§6.0.2](#602--out-of-band-漂移清单pve-sg-dev2026-08-20-实测)）。它们**都不是阶段 3 引入的**，其中最早的可以追到集群搭起来那天 |
| **为什么会漂** | 三个成因，性质不同：<br>① **凭据类**（D-1 的 RustFS 端点 + access key）—— 仓内清单不该带凭据，所以它只能活在集群里；<br>② **本环境类**（D-2 的缓存预算按 master 的 96G 根盘定尺、D-5 的 registry 明文 HTTP、D-7 的 `10.10.10.204:5000` 前缀）—— 是"这套集群的事实"，写进 `deploy/k8s/base` 会污染上游；<br>③ **纯遗漏**（D-3 的 `backend = "postgres"` 是新版已删的值、D-8 的 30800 从来没有清单文件、D-9 的 PG/RustFS/agent-console） |
| **🔴 为什么危险** | `deploy/k8s/run.sh` 把 `config/default.toml` **逐字覆盖**到 `agentenv-k8s-config`（`disableNameSuffixHash: true`），而 `kubectl apply -k` **不 prune**。合起来的效果是：**一次 apply 会抹掉一半漂移、留下另一半**，且抹掉的那一半**大多数不报错**（D-1/D-3/D-4/D-5）。终端上你只会看到一串 `configured` |
| **🔴 最坏的一格** | **D-3 单独丢**（CM 还是老的、只有 DS 的 env 没了）⇒ 新 node 读到 `backend = "postgres"` **拒绝启动**，两台一起 CrashLoop。这是唯一一条"漂移半边"会**响亮**炸掉的组合 —— 其余的都是安静的 |
| **本轮怎么处理** | **不治**。治它要么把 RustFS 凭据搬进仓库（不许），要么给主仓 `deploy/agentenv-sg/` 加一层承接 `agentenv.toml` 与镜像引用的 overlay（是活儿，且属于部署工程不属于阶段 3）。本轮的处置是**登记（§6.0.2）+ 每步复核（§6.0.5）+ 本轮不 apply（§6.0.1）** |
| **有牙的做法（留给下一个人）** | 三件事，按性价比排：<br>1. 🔴 **给 `deploy/k8s/base/kustomization.yaml` 的 `images:` 补 registry 前缀入口**，或给 `run.sh` 加一个 `IMAGE_REGISTRY` / `IMAGE_TAG` 环境变量 —— D-7 是十条里**唯一一条 apply 后会立刻响亮失败**（ImagePullBackOff）的，也是最容易根治的；<br>2. 主仓 `deploy/agentenv-sg/` 补一个 overlay，承接 `agentenv.toml`（D-1/D-2/D-3）、regctl 挂载（D-5）、30800（D-8）、PG（D-9）。凭据走 Secret 引用，不落盘进仓库；<br>3. 补一条**漂移探测**（就是 §6.0.5 那五组探针）进定时任务或 `make` 目标，让"集群被 apply 拆了"这件事在**下一次有人看的时候**就红，而不是在下一次有人 pause 的时候才发现快照没落 OSS |
| **🔴 反模式（别做）** | 把 `patch-node-config.sh` 那种"apply 完再补一刀"的脚本继续加长。R3 已经记过：那个脚本**只认识 OSS 与缓存预算，不认识 `[orchestrator.paused_registry]`** —— 补刀脚本与漂移清单是两份要手动保持同步的东西，正是本节这本账要消灭的形态 |
