# 阶段 3 任务书：语义上收 + 边界闭合（含原阶段 4）

> 2026-08-19 · **裁决产物**，写在闸门 B 通过之后、动手之前。
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

**🔴 闸门放行的是"开工"，不是"提前放宽不变式"**：在 A3 落地并通过集群验证之前，
`running` / `resuming` 永不可抢这条**不许放宽**。

---

## 1. 动手前必须查清的（侦察项）

§2.1 的教训是"写方案时引用的每一个数据源，都要在动手前确认它真的存在"。本阶段有五条：

| # | 要查什么 | 判据 / 为什么 |
|---|---|---|
| **R1** | agent-platform 打的 `http://<node-ip>:30800`（主仓 `internal/sandbox/aenv/client.go:91-100`）**落在 gateway 还是 node 自身 API** | 决定 A4 收窄的边界画在哪。查不清就动手 = 要么收窄无效，要么打掉平台 |
| **R2** | `SandboxInstanceId`（`src/sandbox/custom_extension/client.rs`）**今天的生成时机与生命周期** | A1 要把它升格成 ExecutionID。必须确认：start / resume 各生成一次？pause 后 resume 是否换值？fork 的子沙箱怎么取值？ |
| **R3** | `generation` 的**全部**读写点 | A2 要定"身份轴 vs 版本轴"。已知 3 处 CAS（`complete_pause` / `mark_local_only` / `release_claim`，`postgres.rs:416/:447/:771`），**必须确认没有第四处**，以及是否存在"同一 execution 内需要多次 CAS"的场景（有 ⇒ 两条轴不能合并） |
| **R4** | Agent-Console 读登记表的**全部字段**（主仓 `apps/Agent-Console/internal/aenv/pausedregistry/reader.go`） | A2 重画 schema 会打掉 PG 直连。要知道只读 API 得补哪些字段才能让 Console 平移 |
| **R5** | 节点侧**是否存在** controller 之外可触发 pause / publish 的路径 | A3 的前提。e2b 靠 `proto:56-59` 那条结构性保证；我们要逐条列出 orchestrator 里所有 pause / snapshot / publish 的调用者 |

**R2 / R3 / R5 是 A1–A3 的直接输入，没查清不许写代码。**

---

## 2. 本阶段的不可协商项

沿用方案 §3 的五条护栏，**新增两条**：

- **§3.6（新）写路径单一发布权**：任何进入快照链的状态翻转（`publishing → paused`、
  build 可被 resume 选中）**只能由 controller 做**，且必须携带并校验 execution 身份。
  这是 e2b 被实证有效的那条（`pause_instance.go:71-77` + `get_last_snapshot.sql:8`）。
- **§3.7（新）孤儿判据必须带身份**：`KillOrphan` 按 **execution** 判，不按 sandboxID 存在性判。
  抄自 e2b 的现成缺口（`storage/redis/main.go:205-217` 只看 `raw != nil`）—— 别把缺口一起抄。

---

## 3. 批次 A：身份轴 + 边界闭合（闸门 B 的解锁工程）

| # | 事 | 验收判据 | 变异验证（把修复退回去，测试必须 FAIL）|
|---|---|---|---|
| **A1** | ExecutionID 一等公民（内部）。start / resume 换代；快照 / checkpoint **不**换代 | 同一沙箱 pause→resume 后 execution 变化；snapshot 前后不变 | 让 resume 复用旧 execution ⇒ A3 的拒绝用例必须转 FAIL |
| **A2** | 登记表 schema 重画：`execution_id` NOT NULL；身份轴 / 版本轴分工按 R3 结论定 | migration 后无 NULL 行；Console 只读 API 覆盖 R4 字段 | 把列改成 nullable ⇒ "缺 execution 的行被拒"用例必须 FAIL |
| **A3** | **写路径 fencing（主角）**：破坏性转换必带 execution，旧化身 pause / publish / remove 一律拒。🔴 校验在 **SQL 事务内**原子完成 | 旧 execution 的三种操作全部被拒且**零副作用**（行未变、无文件写出）| ① 把校验挪到事务外的 handler ⇒ 并发插队用例必须 FAIL；② 去掉校验 ⇒ 拒绝用例必须 FAIL |
| **A4** | node API 收窄（原阶段 4）：用户级 REST 只接受 controller；数据面反代不变 | 直连 node 的破坏性调用被拒；经 controller 的同一调用成功 | 放开鉴权 ⇒ 直连拒绝用例必须 FAIL |
| **A5** | 路由层拒旧 execution：`LookupNode` 携带身份，旧化身流量归零 | 新化身起来后，打向旧化身的请求**被拒**（非仅仅改道）| 退回"只按 sandbox 路由" ⇒ 拒绝用例必须 FAIL |
| **A6** | 外部**只读**暴露 execution（`GET /sandboxes/{id}`、resume 响应）| 字段存在且与内部一致；**不作为入参** | —— |

**A3 的 SQL 事务内校验为什么是硬要求**：抄 e2b 的教训 ——
`storage/redis/scripts.go:33-39` 明写 enforcement 必须在 Lua 内而不是 Go 侧，
因为 `Add is lockless, so a resume can install a new incarnation between a Go-side comparison
and this write`。**Go 侧"先查后写"之间就是 resume 插队的窗口。**

---

## 4. 批次 B：语义上收本体（A3 验证通过后才许放宽"永不可抢"）

| # | 事 | 关键约束 / 验收 |
|---|---|---|
| **B1** | 中央 poll 取代节点自续租 | `answered` 与 `sync_ok` **分开判**；`RenewNodeLease` RPC 退役；`sync_ok` 指标此时才可得（阶段 0 曾因 heartbeat 单向推而不可得）|
| **B2** | evictor 与 reclaim **分离**（阶段 3-A）| 两个都要、不能合并。reclaim 的「租约过期 **且** deadline 过期」**双条件不许放宽成单条件** |
| **B3** | 孤儿回收 | `Reconcile(roster) → KillOrphan` + grace period + §3.3 熔断；**按 execution 判**（§3.7）。变异：改成按 sandboxID 判 ⇒ "同 ID 异节点重建后旧化身被杀"用例必须 FAIL |
| **B4** | 中央 placement | resume = controller 选节点 + 下发 Create with snapshot；`AcquireSandbox` 变内部调用；三分法（§3.4）显式建模 |
| **B5** | 显式状态机 | `AllowedTransitions` + `TransitionEffect` + 结构化 `KillReason` |
| **B6** | RPC 面重写 | node→controller = 上报事实；controller→node = 下发命令。阶段 2 的 5 RPC 形状直接丢（P1 允许）|
| **B7** | `release_node_holdings` **保留** | 除非能论证「node id 永不复用」且「新 instance_id 与老进程死亡无可观测重叠窗口」—— 它是系统里唯一能**证明**而非推断 VM 已死的证据 |

---

## 5. 批次 C：随行收口

- **T3 F6**：首个续租周期内失联的节点留**永久孤儿行**（`sandbox_expires_at` 为 NULL 永不匹配 reclaim）—— 随 B1 消掉
- **S 系列**（随 B6 重写消化）：
  - **S4** 响应带"本次实际覆盖的 id 集合" —— 护栏 §3.1 目前**唯一**无机械保证的一环，而缺行的下游反应是**删用户工作区**
  - **S2** release 命中 0 行要可计数（中央化后它是"节点报的 generation 已过期"的唯一信号）
  - **S5** 读路径带 DB 时钟（`Get`/`GetMany` 目前拿不到，只能用进程墙钟 —— 正是文档禁止的）
  - **S7** `MarkRunning` 的 bool 拆成两个事实（"没跟踪" vs "是别人的"，调用方两个都要）
  - **S3** 节点 `reconcile_interval` 校验归属（`ttl ≥ 3×interval` 目前无处校验）
- **D8**：`Code::Aborted ⇒ GenerationConflict`（一行，Go 侧已备好）
- **T2 N3**：`invalid_rows` 至今零分辨力 —— 授权造一行 `paused` + `snapshot_id IS NULL` 验一次即删

---

## 6. 顺序、回退与发布

1. **R1–R5 侦察** → 出结论后才动 A1
2. **A1 → A2 → A3**（身份轴先立，schema 再改，fencing 最后上）—— A3 是闸门 B 的兑现物
3. **A3 集群验证通过**（含对照探针）→ 才允许开始 B 批次
4. **A4 / A5 / A6** 可与 A3 并行开发，但**发布顺序在 A3 之后**
5. **B1–B7** → **C**
6. **Console 切只读 API** 与 A2 同批发布（R4 的结论决定它要等哪些字段）

**回退**：沿用阶段 0/1/2 的原则 —— **一个阶段的回退不许依赖新写的回滚逻辑**。
A 批次的回退面是"关掉 fencing 校验 + 放开 node API"，必须是配置级；
做不到配置级回退的改动要在任务书里单独标注并给出理由。

**建议与「暂停必然落 OSS」(v3) 并批**（主仓 `docs/proposals/2026-08-19-aenv-pause-publish-durability.md`）：
它把 `publishing` 改成长驻重试态后，placement 的"硬钉 origin"分支基本消失，B4 才真正自由。
两者动同一批状态流转，分开做等于改两遍。

---

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
