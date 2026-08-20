# 阶段 3 · Go scheduler 侧设计：A2（身份轴入表）与 A3（写路径 fencing）

> 2026-08-19 · **设计产物，本轮不改任何源码**。
> 任务书：[`_impl-plan-control-plane-phase3.md`](_impl-plan-control-plane-phase3.md)
> 权威方案：[`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md) §3 / §4 / §8
> 闸门 B 决策材料：[`2026-08-19-control-plane-refactor-outcome.md`](2026-08-19-control-plane-refactor-outcome.md) §3
>
> **范围**：`services/scheduler/`（登记表 DDL、`registry` 包、`PausedRegistryService`）+ `services/api/proto/scheduler.proto`。
> **不在范围**：Rust 节点侧化身分配（A1，见 node 侧设计）、node API 收窄（A4）、路由层拒旧化身（A5）、
> 发布权集中（B6）、存储层写锁（长期项）、Agent-Console 切只读 API（同批发布但不在本仓）。
>
> 🔴 **N4 指针（2026-08-19 命名/归属裁决，任务书 §10.2）**：A5 需要的
> **S1（binding 存储持久化 execution）/ S2（heartbeat roster 带 execution + `ReconcileNode` 仲裁）/
> S3（UUID v7 大者胜的仲裁规则）/ S6（`RecordAssignment` 走同一条仲裁）**
> 落在 `services/scheduler/internal/store.go` / `redis_store.go` / `lookup.go` / `node_registry.go` / `service.go`，
> **不在本文的范围声明之内** —— 它们归 [`_design-phase3-scheduler-a5.md`](_design-phase3-scheduler-a5.md)。
> 本文与那份是**同一个服务的两块互不重叠的地界**：本文管 `registry` 包与 `PausedRegistryService`（写路径 + DDL），
> 那份管 binding / lookup / roster（读路径 + 路由答案）。
> **唯一的共用面是 proto**：`RegistryEntry` / `RegistrySandbox` 的 `execution_id` 字段**由本文这个 PR 加一次**，
> 那份只消费（字段号见任务书 §11 冻结契约表）。

---

## 0. 三十秒版

> 🚦 **2026-08-19 裁决回填**：本文的全部未决项（§6 U1–U5）与相对任务书的偏离（#2 / #5 / #7 / #8）
> **已由主 agent 逐条裁决**，结论就地标注在各节的 ✅ 块里，收口表在
> [`_impl-plan-control-plane-phase3.md` §10](_impl-plan-control-plane-phase3.md#10-设计裁决记录)。
> 🔴 **最大的一条：U1 走 E-A（`resuming` 预分配化身）** —— 它改写了 §2.2 的 CHECK、§2.3 的 #2/#3/#9、§3.3 的分支 ①。

1. **两条轴分开**：`generation` = 行版本（谁写在后），`execution_id` = 化身身份（哪个 VM 实例）。
   合并会自相矛盾 —— 一次跨节点 resume 里 generation 涨两次而化身只诞生一次。
2. ✅ **【裁决：采纳偏离】`execution_id` 是 nullable 的，语义上的"必填"由一条 CHECK 按状态表达**：
   `running` / `publishing` 必须有化身，`paused` / `local_only` / `resuming` 必须没有。
   这比一刀切 NOT NULL 更强 —— 后者会逼出一个哨兵 UUID，而哨兵是迟早会被人拿去比相等的东西。
   （🔴 **E-A 裁决后 `resuming` 改为"必须非空"**，见 §2.2；任务书 §3 A2 行的字面 `NOT NULL` 已同步改写）
3. **A3 只改两条 SQL 的 WHERE**：`beginPauseSQL` 加 `execution_id = EXCLUDED.execution_id`，
   `markRunningSQL` 把"claim 守卫"换成"持有者 + 化身"三分支谓词。**都在同一条语句里，不需要显式事务。**
   （✅ 裁决 O3：**本轮只有这两条加化身谓词**，另外四条转换保持 generation-only，见 §3.4）
4. ✅ **【裁决：采纳】拒绝语义要两种**：`ErrGenerationConflict → Aborted`（版本旧，重读再试）与
   **新增** `ErrExecutionFenced → PermissionDenied`（化身已死，**永不重试**）。
5. ✅ **【裁决：采纳】迁移走 P1 drop 重建**（运维一次性 `DROP TABLE`，代码里零一次性逻辑）；
   纯加法作为备选并写清它放弃了什么。**A2 只加列不删列/改名 ⇒ Console 的 PG 直连不会失明。**
   🔴 **它与 node 侧"清本地 paused 记录"是同一个 runbook 步骤**，不许分开写（§2.7）。
6. **PG 测试环境不是问题**：CI 与本机都有真 PG，套件自带反假绿开关。A3 的变异验证可以做，且必须做。

---

## 1. 起点事实（全部已实证，行号可点开）

### 1.1 登记表只有一处真相源，外加三份手抄

| 位置 | 是什么 | 谁跑它 |
|---|---|---|
| `services/scheduler/internal/registry/migrate.go:39-71` | `const SchemaDDL` —— **唯一真相源** | controller 每次启动，advisory lock `0x0A6E76534348_4D41` 串行 |
| `services/scheduler/internal/registry/contract_test.go:59` | `contractSchemaDDL` | 契约测试建"节点建出来的表" |
| `services/scheduler/internal/registry/postgres_integration_test.go:17` | `schemaDDL` | 集成测试同上 |

🟢 **节点侧那份已经不存在了**：`src/orchestrator/paused_registry/` 现在只剩
`central.rs` / `disabled.rs` / `mod.rs` / `types.rs`，`postgres.rs` 与它的 `SCHEMA_DDL` 在 D11 被删干净
（全仓 grep `SCHEMA_DDL` / `CREATE TABLE IF NOT EXISTS paused_sandboxes` 在 `src/` 下零命中）。
⇒ **controller 是这张表唯一的 DDL 施加者与唯一的写入者**，`migrate.go:28-32` 里"另一个写者会被卡死"
那条约束**已经解除**；两份测试拷贝从"另一个活写者的形状"降级成**化石**（见 §2.10）。

🔴 但 `migrate.go:28-32` 的另一半仍然成立，而且正是 A2 要撞的那面墙：

> Indexes qualify; a **NOT NULL column still would not**.

而且 dev（VM 203/204）与 test（VM 201/202）两套 k3s 集群里**有存量行**。

### 1.2 `generation` 的十个写点，读侧零比较

| # | 行号 | 语句 | 动作 |
|---|---|---|---|
| 1 | `store_postgres.go:396` | `beginPauseSQL` ON CONFLICT | 递增 |
| 2 | `:594` | `claimForResumeSQL` | 递增 |
| 3 | `:618` | `claimForResumeDurableOnlySQL` | 递增 |
| 4 | `:834` | `markRunningSQL` | 递增 |
| 5 | `:998` | `reclaimReleasedSQL` | 递增 |
| 6 | `:1110` | `releaseHoldingsReleasedSQL` | 递增 |
| 7 | `:484` | `completePauseSQL` | **CAS** `AND generation = $2` |
| 8 | `:519` | `markLocalOnlySQL` | **CAS** |
| 9 | `:786` | `releaseClaimSQL` | **CAS** |
| 10 | `:1179` | `removeSQL` | **CAS**（`DELETE … AND generation = $3`）|

新行由 `:388,392` 硬编码 `generation = 1`。**无 DB 默认值 / sequence / trigger**，全在应用层。
读侧（`Get` / `GetMany` / `ListRegistrySandboxes` / Console）**一处都不做值比较**，只透传与打日志。

🔴 **已裁决：两条轴不能合并。** 三条证明（照抄任务书侦察结论，此处只补行号）：

1. 一次跨节点 resume 里 generation 涨两次（`:594` ClaimForResume +1 → `:834` MarkRunning +1），
   再 pause 又涨一次（`:396`）—— 合并则单化身内身份变三次，与「snapshot 前后 execution 不变」直接矛盾，
   且 fencing 会把化身**自己的**后续写当旧化身拒掉。
2. `:998` 与 `:1110` 在**无任何化身参与**下递增（控制面定时器 / 死节点上的后继进程）——
   合并等于为"没有活进程的沙箱"凭空签发身份。
3. v3「暂停必然落 OSS」把 `publishing` 改成长驻重试态之后，`CompletePause` 会在
   **同一 execution、同一 generation** 下自环重试 k 次。

### 1.3 A3 的攻击面：真正裸奔的只有两条

| 转换 | 今天的守卫 | 评价 |
|---|---|---|
| `complete_pause` `:484` | `generation = $2 AND state='publishing'` | 🟢 有 |
| `mark_local_only` `:519` | 同上 | 🟢 有 |
| `release_claim` `:786` | `generation = $2 AND state='resuming'` | 🟢 有 |
| `remove` `:1179` | `generation = $3` | 🟢 有（D11 补上） |
| **`begin_pause` `:381-409`** | **`WHERE paused_sandboxes.cluster_id = EXCLUDED.cluster_id` —— 只比集群** | 🔴 **裸奔** |
| **`mark_running` `:831-839`** | **`claimed_by_node_id IS NULL OR = $2` —— 而 running 行该列正是 NULL** | 🔴 **裸奔** |

而且不是"忘了加"，是**契约层主动拒收**：`registry_service.go:191-192` 与 `:266-267` 的
`rejectFields(req, fieldGeneration|…)` 明确把 `expect_generation` 从这两个 kind 上打回。
proto 注释（`scheduler.proto` `TransitionSandboxRequest.expect_generation`）逐字写着
"Four, not five: mark_running's predicate is on state and holder, never on generation"。

⇒ **A3 不是"给三种操作加校验"，是改 proto 契约 + 改两条 SQL 的 WHERE。**

配套事实：节点侧 `pause_sandbox_inner` 在 `begin_pause` 之前**零次读集群行**
（`src/api/impls/paused_coordinator.rs:263-284`：`publish()` 进来第一件事就是 `begin_pause`），
所以"节点自己先判一下"这条防线根本不存在。

### 1.4 必然序列（A3 第一发变异验证要打的就是这条）

TTL 自动 pause 是节点后台定时器，**默认 1 秒一跳**
（`src/orchestrator/service.rs:2093-2115` `evict_expired_sandboxes`；间隔 `config/default.toml:198`
`auto_evict_interval_ms = 1000`）。配上我们比 e2b 激进的 reclaim：

```
reclaim（store_postgres.go:995-1004）把 running 翻 paused，generation+1
        ↓
B 节点 ClaimForResume（:585-604）拿走 → resuming
        ↓
老节点 A 恢复联系，1 秒内自动 pause 触发
        ↓
A 的 begin_pause（:381-409，WHERE 只比 cluster）把行改成自己的 publishing + originA
        ↓
A 发布陈旧快照 → complete_pause 翻牌
        ↓
paused_coordinator.rs:313-320 把 B 正依赖的 previous_snapshot_id 从 OSS 删掉
```

### 1.5 减轻因素：不可逆损失只有两点

每次 pause 都 `SnapshotId::generate()` 全新 UUID（`src/api/impls/paused_coordinator.rs:685-690`
`publish_metadata` 里的 `id: SnapshotId::generate()`），OSS `artifacts/{uuid}/` 天然不相交、层内容寻址
⇒ **旧化身写 OSS 本身无害，只留垃圾**。全部不可逆损失只在两点：

- `completePauseSQL:479-484` 的**翻牌**（把 `snapshot_id` 指向旧化身产出的快照）
- `paused_coordinator.rs:313-320` 的 **`delete_snapshot(previous)`**

**守住这两点就够，不需要存储层写锁。** 而这两点都在 `begin_pause` 成功之后才可能发生
（`paused_coordinator.rs:284` `begin_pause` → `:296` `publish_captured` → `:305` `complete_pause` → `:313` 删旧），
所以**把 `begin_pause` 挡住 = 把两点一起挡住**，这是 A3 零副作用保证的结构基础（§3.6）。

---

## 2. A2：登记表 schema

### 2.1 两条轴的定义

| 轴 | 列 | 语义 | 谁比较它 |
|---|---|---|---|
| **版本轴** | `generation BIGINT` | **这一行**被改过多少次。回答"你读到的那一版还是不是最新" | 4 处 CAS（§1.2 #7-10）|
| **身份轴** | `execution_id UUID` | **这台沙箱当前的活化身**（一个 VM 实例的一生）。回答"你是不是还是那个我认的进程" | A3 的两条新谓词 + §3.4 的三条防御性谓词 |

**化身生命周期**（与 A1 的契约，形状对齐 `NodeIdentity.service_instance_id`）：

- UUID **v7**（`src/identity.rs:41-45` 与 `src/sandbox/custom_extension/client.rs:56-60` 都是 `Uuid::now_v7()`）
- proto / JSON 上是 `string`；Go 侧 `requireUUID` 解析后按 `uuid` 绑定，与 `sandbox_id` / `cluster_id` 同规格
- **start / resume 各生成一个新的；pause / snapshot / checkpoint 不换**

### 2.2 列定义与不变式

```sql
-- 新增两列（都 nullable）
execution_id         UUID,
execution_started_at TIMESTAMPTZ,

-- 不变式由 CHECK 表达，而不是由 NOT NULL 表达
-- 🔴 状态集含 'resuming'：裁决 U1 = E-A（认领时预分配化身）。实现照抄这一份。
CONSTRAINT paused_sandboxes_execution_check CHECK (
    (state IN ('running', 'publishing', 'resuming')) = (execution_id IS NOT NULL)
    AND (execution_id IS NULL) = (execution_started_at IS NULL)
)
```

读作：

| state | `execution_id` | 含义 |
|---|---|---|
| `running` | **必须非空** | 正在跑的化身。§3.7 KillOrphan 的判据就是它 |
| `publishing` | **必须非空** | VM 已停，但这条发布链归它；`complete_pause` / `mark_local_only` 必须由它收尾 |
| `paused` | **必须为空** | 集群认为无人持有。任何"我还持有它"的写都应当被拒 |
| `local_only` | **必须为空** | 同上，只是仅 origin 节点能救回来 |
| `resuming` | ✅ **裁决 U1 = E-A ⇒ 必须非空**（原选项 E-B「必须为空」作废，问题陈述保留在 §6-U1）| 接管中，化身由认领者**预分配** |

> ### ✅ 裁决（U1 = E-A）：CHECK 的定稿全文，实现照抄这一份
>
> ```sql
> CONSTRAINT paused_sandboxes_execution_check CHECK (
>     (state IN ('running', 'publishing', 'resuming')) = (execution_id IS NOT NULL)
>     AND (execution_id IS NULL) = (execution_started_at IS NULL)
> )
> ```
>
> 一句话理由：E-A 让 `mark_running` 成为真正的"校验 execution"而不只是"校验 claim 持有者"，
> 并且**不欠 B3 一个"记得给 `resuming` 开孤儿判定特例"的债** —— 而"注释承诺别处会做、结果没做"正是本仓踩过的坑。
>
> 🔴 **随之被固定的四件事**（实现不许再二选一）：
> 1. `AcquireSandboxRequest` / `claimForResumeSQL` **携带并写入** execution，与 `claimed_by_node_id` **同一条语句/同一事务**；
> 2. `markRunningSQL` 的分支 ① **保留** `AND execution_id = $6::uuid`（§3.3 里标"仅方案 E-A"的那一行**生效**）；
> 3. `releaseClaimSQL`（`resuming → paused`）**必须清空** `execution_id` / `execution_started_at`，否则违反本 CHECK；
> 4. node 侧铸造点从 VM start 前移到 claim（`_design-phase3-node.md` §2.2.1 裁决 D-1），
>    `mark_running` 送上来的**必须是 claim 时那一个**。
>
> ⚠️ **落地影响（测试）**：§4.1 的 `T-A2-3 TestAParkedRowCannotCarryAnExecution` 只能用
> `paused` / `local_only` 造反例，**不能再用 `resuming`** —— 用 `resuming` 会在 E-A 下变成合法行，测试自己就错了。

🔴 **`paused` 必须为空，是整个 A3 成立的前提**。三条"夺权"路径
（`reclaimReleasedSQL:995-1004`、`releaseHoldingsReleasedSQL:1107-1114`、`releaseClaimSQL:781-786`）
全都把行落到 `paused`；只要它们**清空 `execution_id`**，§1.4 那条必然序列在第一步就断了 ——
老节点 A 的 `begin_pause` 拿 `E_A` 去比一个 NULL，SQL 三值逻辑直接不匹配。

### 2.3 十个写点逐个归位

> ⚠️ **读法（2026-08-19 补）**：下表是**裁决前**写成的，`execution_id` 那一列有 **6 行**（#2 #3 #4 #7 #8 #9）仍保留着
> **E-A / E-B 双写形态**或已被裁决 O3 驳回的"加谓词"。按"问题陈述不删"的纪律原文保留，
> 但已在**格子内部**逐个标了 ⚠️ —— 🔴 **只扫表格不读旁边散文的人，看格子里的标注即可；定稿一律以紧随本表的
> 「✅ 裁决后本表的定稿」为准**。

| # | 行号 | 语句 | `generation` | `execution_id` | `execution_started_at` | 为什么 |
|---|---|---|---|---|---|---|
| 1 | `:396` | `beginPause` ON CONFLICT | **+1** | **要求相等**，写入 = 幂等确认（running→publishing 是同一个 VM）| 不动 | 新一轮发布链要新版本号；化身**不变**，这正是「snapshot 前后 execution 不变」 |
| 1' | `:388,392` | `beginPause` INSERT（新行）| `1` | **安装** `$exec` | `now()` | 首次 pause，行第一次出现 |
| 2 | `:594` | `claimForResume` | **+1** | ⚠️ **本格保留的是裁决前的双写形态，只扫表格的人别照抄** —— E-B 那一支**已作废**，定稿见下方裁决表 #2/#3：**安装 claimer 预分配值**。~~E-B：置 `NULL`（已是 NULL，恒等）~~／✅ E-A：安装 claimer 预分配值 | ✅ `now()`（E-A） | 换手；未来化身尚未存在 |
| 3 | `:618` | `claimForResumeDurableOnly` | **+1** | ⚠️ 同上（双写形态，定稿见下方裁决表 #2/#3）| 同上 | 同上 |
| 4 | `:834` | `markRunning` | **+1** | ⚠️ **本格已被裁决改写**（E-A 之后 claim 才是安装点）：~~换代：安装 `$exec`~~ ⇒ 定稿见下方裁决表 #4：**分支①校验并确认同一个值**、分支②（本机 parked 就地唤醒）才是安装点 | ⚠️ ~~`now()`~~ **已订正**：仅分支 ①/② `now()`；**分支 ③（同化身重发）不重新盖戳**，见 §3.3 的 2026-08-20 裁决改判块 | ~~resume 完成 = 新化身诞生。**唯一的安装点**~~ ⇒ 已作废，见裁决表 |
| 5 | `:998` | `reclaimReleased` | **+1** | 🔴 **置 `NULL`** | 置 `NULL` | 无化身参与的夺权；不清就是 §1.4 |
| 6 | `:1110` | `releaseHoldingsReleased` | **+1** | 🔴 **置 `NULL`** | 置 `NULL` | 同上 |
| 7 | `:484` | `completePause` CAS | 读 `= $2` | ⚠️ ~~加谓词 `= $exec`~~ **已被裁决 O3 驳回，本轮不加**；✅ 成功后**置 `NULL`**（转 `paused`）—— 定稿见下方裁决表 #7/#8 | 置 `NULL` | 发布收尾，VM 已死 |
| 8 | `:519` | `markLocalOnly` CAS | 读 `= $2` | ⚠️ ~~加谓词 `= $exec`~~ **已被裁决 O3 驳回，本轮不加**；✅ 成功后**置 `NULL`** —— 定稿见下方裁决表 #7/#8 | 置 `NULL` | 同上 |
| 9 | `:786` | `releaseClaim` CAS | 读 `= $2` | ⚠️ **双写形态 + 一处已被裁决驳回**：~~E-B：不涉及~~／~~E-A：加谓词 `= $exec`~~ ⇒ 定稿见下方裁决表 #9：**必须置 `NULL`，但不加谓词**（夺权路径，裁决 O3）| 置 `NULL` | 交还未 resume 的 claim |
| 10 | `:1179` | `remove` CAS | 读 `= $3` | 🟡 **保持 generation-only**，见下 | —— | 删行，无残留 |

> ### ✅ 裁决后本表的定稿（只列被裁决改动的行，其余照上表）
>
> | # | 语句 | `execution_id` 定稿 | 依据 |
> |---|---|---|---|
> | 2 / 3 | `claimForResume` / `claimForResumeDurableOnly` | 🔴 **安装认领者预分配的化身**（E-A 分支生效，E-B 分支作废）；`execution_started_at = now()` | U1 = E-A |
> | 4 | `markRunning` | **校验并确认同一个值**（不是换代）：分支 ① 带 `AND execution_id = $6`；分支 ②（本机 parked 就地唤醒）仍是**安装点** | U1 = E-A |
> | 5 / 6 | `reclaimReleased` / `releaseHoldingsReleased` | 🔴 **必须置 NULL**（连同 `execution_started_at`）| 裁决 #3（夺权路径） |
> | 7 / 8 | `completePause` / `markLocalOnly` | 成功后**置 NULL**（CHECK 强制，非可选）；🔴 **本轮不加 `= $exec` 谓词** | 裁决 O3 |
> | 9 | `releaseClaim` | 🔴 **必须置 NULL**（夺权路径第三条）；🔴 **不加 `= $exec` 谓词** | 裁决 #3 + O3 |
> | 10 | `remove` | **保持 generation-only**，且契约层**明确拒收** `execution_id` | 裁决 U3 |
>
> 🔴 **"三条夺权路径必须清空 execution"是硬要求，不是纵深**：不清，§1.4 那条必然序列**第一步就成立** ——
> 行还带着 `E_A`，老节点 A 的 `begin_pause` 拿 `E_A` 去比就**匹配得上**，整条 fencing 白做。
> 这一条同时写死在 `_design-phase3-node.md` §2.9，两份文档不许只改一份。

🟡 **#10 `remove` 为什么不加 execution**：`remove` 的语义是"**用户**要删这台沙箱"，不是
"这个化身要删"。e2b 在同一位置把 `ExpectExecutionID` 定成 opt-in，理由逐字写在
`sandboxtypes/states.go:82-92`：`Empty means "remove whatever is stored", which is correct for
callers acting on a fresh read or on user intent`。而 D11 已经把它从无条件删改成 generation CAS，
那个窗口已经关上。**真正需要 execution 的是"平台重试队列里的陈旧 delete"（G3 本体），
而那要平台传 execution，P2 明确本轮不做。** ⇒ 列入未决项 §6-U3。
✅ **裁决（U3）：采纳 —— `remove` 保持 generation-only，契约层明确拒收 `execution_id`**；留给 G3 那一批一起做。

🟢 **`renewLease` `:921-931` 两条轴都不动**（它只刷 `lease_expires_at` / `sandbox_expires_at` / `updated_at`），
`get` / `getMany` / `ListRegistrySandboxes` 只读。

### 2.4 为什么不是"一刀切 NOT NULL"

任务书 A2 的字面写法是"`execution_id` NOT NULL"。**本设计有意偏离，理由三条**：

> ✅ **裁决（2026-08-19 主 agent）：采纳这次偏离 —— 用 nullable + CHECK，不用任务书字面的 `NOT NULL`。**
> 一句话理由：一刀切 NOT NULL 会逼出哨兵 UUID，而哨兵迟早被人拿去比相等，fencing 无声破掉；
> 按状态的 CHECK 严格更强，还管住"不该有的时候没有"。
> 🔴 **任务书 §3 的 A2 行已同步改写**（`_impl-plan-control-plane-phase3.md` §3 + §10 第 3 条），
> 免得实现 agent 照着旧字面写 `NOT NULL`。


1. **"没有活化身"是一个真实且必须可表达的状态**。`paused` 行就是没有。一刀切 NOT NULL 会逼出一个
   哨兵值（全零 UUID 之类），而哨兵是迟早会被某处代码拿去 `= ` 比较的东西 —— 那一刻 fencing 就破了，
   而且破得**无声**。
2. **按状态的 CHECK 严格强于 blanket NOT NULL**：后者只保证"有值"，前者保证
   "**该有的时候有、不该有的时候没有**"。§1.4 那条必然序列要靠的恰恰是后半句。
3. 变异验证仍然成立，只是打的靶子换了（§4.1 T-A2-2）：
   把 CHECK 去掉（= 允许 `running` 行 `execution_id IS NULL`）⇒ "缺 execution 的行被拒"用例必须 FAIL。

🔴 **CHECK 是本设计里唯一的机械保证**。没有它，A2 的验收退化成"我们现在这几条 SQL 写对了"——
而这张表未来还会长出新的写点（B1/B3/B4/B5 都会动它）。

### 2.5 `execution_started_at`：为 §3.7 预留的那一列

护栏 §3.7 要求 `KillOrphan` 按 execution 判。B3 实现它时需要两样东西：

1. 行上的活化身身份 → `execution_id` ✅
2. **grace period 的起点**：一个刚 `mark_running` 的化身，在 node 的下一次 roster 上报到达之前
   必然"对不上"，直接判孤儿就会杀掉每一次正常 resume。

🔴 **`updated_at` 不能当 #2 用**：`renewLeaseSQL:921-931` 的 `SET … updated_at = now()` 每个心跳
都刷它。`running` 行的 `updated_at` 永远很新 ⇒ 任何以它为起点的 grace 永不到期 ⇒ 孤儿永远杀不掉。
（这与 Console 的排序注释是同一个观察：主仓 `apps/Agent-Console/internal/aenv/pausedregistry/reader.go`
里写着"`renew_lease` 每次心跳都给活行写 `updated_at = now()`"。）

⇒ **A2 顺手加 `execution_started_at TIMESTAMPTZ`，与 `execution_id` 同生同灭**（CHECK 里绑死）。
成本一列，收益是 B3 不必回头再改一次 schema。

> ✅ **裁决：采纳，本轮就加 `execution_started_at`。**
> 一句话理由：`updated_at` 被 `renewLeaseSQL:925` **每个心跳**刷新，B3 的 `KillOrphan` grace period 不能拿它当起点；
> 这一列现在加是一次 DDL，B3 时再加是第二次改表。

### 2.6 索引：不加

`execution_id` **不建索引**。每一条会用到它的语句都已经落在主键上
（`sandbox_id = $1::uuid` 是 PK，`beginPause` 走 `ON CONFLICT (sandbox_id)`），
谓词只是行内比较。B3 的 `KillOrphan` 按 `(cluster_id, state)` 取集合、在 Go 侧比对身份，
也用不到它。**加一个只被写入维护、从不被查询选中的索引，是给每次 pause 加一次写。**

### 2.7 迁移：纯加法+回填 vs P1 drop 重建

| | 方案 α：纯加法（+ 不加 CHECK）| 方案 β：纯加法 + CHECK + 回填 | **方案 γ：P1 drop 重建（推荐）** |
|---|---|---|---|
| DDL | `ADD COLUMN IF NOT EXISTS`×2 | α + `ADD CONSTRAINT` | 运维 `DROP TABLE` 一次 + 新 `CREATE TABLE` |
| 存量 `running` 行 | 保留，`execution_id` 恒 NULL | 🔴 **`ADD CONSTRAINT` 会扫全表并失败** ⇒ controller 起不来 | 一起没了 |
| 修复存量行的办法 | 无（等它们自然 pause/删除）| `UPDATE … SET state='paused' WHERE state IN ('running','resuming')` —— 🔴 **把活沙箱降级成可抢，等于人为制造双活** | —— |
| `NOT VALID` 变体 | —— | 🔴 也不行：`NOT VALID` 仍在 UPDATE 时强制，而 `renewLease` 每心跳都 UPDATE 存量 running 行 ⇒ 全集群续租崩 | —— |
| 代码里的一次性逻辑 | 无 | 有（回填语句常驻 DDL = 一把上了膛的枪）| **无** |
| 机械保证 | 🔴 **没有**（§2.4 #3）| 有 | 有 |
| 回退 | 换镜像即可 | 换镜像 + 手工 `DROP CONSTRAINT` | 换镜像 + 再 `DROP TABLE` 一次 |
| 代价 | 放弃 CHECK | 不可行 | dev/test 两套集群的登记行清零 |

**推荐 γ**，理由：

- P1 明说"AgentENV 尚未上生产，dev 存量行可丢"；
- 登记行清零的**真实影响面很小**：登记表只记录"曾被暂停过的沙箱"的**跨节点**可恢复性。
  节点侧的本地 paused 记录（`persisted_sandbox_store_path`）**不受影响**
  ⇒ 存量沙箱仍然可以在**它自己的节点上**被唤醒，只是暂时失去"换节点恢复"的能力，
  而下一次 pause 就会把行重新写回来（`beginPause` 的 INSERT 分支）。
- 它是**唯一不需要在常驻 DDL 里塞数据修改语句**的方案。

> ### ✅ 裁决：采纳方案 γ（drop 重建），但那次 `DROP TABLE` 不是一个独立动作
>
> 🔴 **它与 node 侧"清空节点上的 `$AENV_HOME/persisted-sandboxes`"（`_design-phase3-node.md` §2.3）
> 是同一次破坏性操作的两半，必须写进 runbook 的同一个步骤**，不许分成两条各自独立的指令。
> 一句话理由：分开写，就一定会有集群只做了其中一半 —— 中央说没有、节点说有的半清状态，比两边都不清更难查。
>
> 该步骤的最小形状（两套集群各执行一次，顺序固定）：
> 1. 存档：`SELECT state, count(*) FROM paused_sandboxes GROUP BY 1`（顺带取 T2 N3 的样本）；
> 2. 停 controller 写入面（或先滚 node，见 §5.1 的发布顺序）；
> 3. `DROP TABLE IF EXISTS paused_sandboxes;`
> 4. **同一步骤内**：清空**每个节点**的本地 paused 记录目录；
> 5. 起新 controller（`Migrate` 建新表）＋滚 node。
>
> ✅ **2026-08-19 已就地修正（裁决 A5-U6）**：下面代码块里的 CHECK 原先是 E-B 形状（状态集不含 `'resuming'`），
> 已改成 E-A 形状。**现在可以照抄了。**
> 🔴 留这条记录是因为：实现 agent 会直接 copy 这段 DDL 全文，而旧的那一份会让**每一次跨节点 resume 的 claim
> 写不进去**（`23514`），症状是"跨节点 resume 全线失败"而不是"少了个约束"。

**具体 DDL 序列（γ）**

运维一次（两套集群各一次，写进 runbook；执行前先 `SELECT state, count(*) FROM paused_sandboxes GROUP BY 1` 存档）：

```sql
DROP TABLE IF EXISTS paused_sandboxes;
```

代码里的 `SchemaDDL` 改成（保持"对全新库一次建好、对已是新形状的库全 no-op"的性质）：

```sql
CREATE TABLE IF NOT EXISTS paused_sandboxes (
    sandbox_id           UUID        PRIMARY KEY,
    cluster_id           UUID        NOT NULL,
    state                TEXT        NOT NULL,
    generation           BIGINT      NOT NULL,
    origin_node_id       TEXT        NOT NULL,
    snapshot_id          UUID,
    metadata             JSONB       NOT NULL,
    paused_at            TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL,
    claimed_by_node_id   TEXT,
    lease_expires_at     TIMESTAMPTZ,
    sandbox_expires_at   TIMESTAMPTZ,
    execution_id         UUID,
    execution_started_at TIMESTAMPTZ
);
-- 以下全部对"已经是新形状"的表 no-op，对"controller 刚建好又重启"幂等
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS claimed_by_node_id   TEXT;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS lease_expires_at     TIMESTAMPTZ;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS sandbox_expires_at   TIMESTAMPTZ;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS execution_id         UUID;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS execution_started_at TIMESTAMPTZ;

ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_state_check;
ALTER TABLE paused_sandboxes ADD  CONSTRAINT paused_sandboxes_state_check
    CHECK (state IN ('publishing', 'paused', 'resuming', 'local_only', 'running'));

ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_execution_check;
ALTER TABLE paused_sandboxes ADD  CONSTRAINT paused_sandboxes_execution_check
    -- 🔴 状态集含 'resuming'（裁决 U1 = E-A）。少了它，每一次跨节点 resume 的 claim 都写不进去。
    CHECK ( (state IN ('running', 'publishing', 'resuming')) = (execution_id IS NOT NULL)
        AND (execution_id IS NULL) = (execution_started_at IS NULL) );

CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx  ON paused_sandboxes (updated_at);
CREATE INDEX IF NOT EXISTS paused_sandboxes_reclaim_idx
    ON paused_sandboxes (cluster_id, sandbox_expires_at)
    WHERE state IN ('running', 'resuming') AND sandbox_expires_at IS NOT NULL;
```

（注：把历史 `ALTER ADD COLUMN` 保留在 `CREATE TABLE` 之后是刻意的 —— 它们对新表全是 no-op，
删掉它们等于让"从旧形状升上来"这条路彻底消失，而运维忘了 `DROP TABLE` 时我们希望是**报错**，
不是**建不出列**。报错由下一段的自检负责。）

### 2.8 `migrate.go` 的 bootstrap 机制扛不扛得住

**扛得住加列，扛不住加 CHECK。** 分两句说：

- `ADD COLUMN IF NOT EXISTS execution_id UUID`（nullable）是安全幂等的，`migrate.go:28-32` 的告诫
  针对的是"无默认值的 NOT NULL 列"，我们两列都 nullable ⇒ 不触发。
- `ADD CONSTRAINT … CHECK`（不带 `NOT VALID`）会**扫全表校验**，存量 `running` 行 `execution_id IS NULL`
  必然违反 ⇒ `Migrate` 返回错误 ⇒ controller 起不来。而 `NOT VALID` 也救不了（§2.7 表里那一行）。

🔴 **所以 `Migrate` 必须增加一道 fail-fast 自检，而且必须早于 `ADD CONSTRAINT`**：

```go
// 伪代码，放在 Exec(SchemaDDL) 之前、拿到 advisory lock 之后
var legacy int64
err := conn.QueryRow(ctx, `
    SELECT count(*) FROM paused_sandboxes
     WHERE state IN ('running','publishing') AND execution_id IS NULL`).Scan(&legacy)
// 表不存在（42P01）⇒ 全新库，跳过
// 🔴 列不存在（42703）⇒ 这是**阶段 3 之前的旧形状表**，不是全新库，绝不能跳过：
//    改用 `SELECT count(*) FROM paused_sandboxes`，>0 就按下面同样的措辞拒绝
//    （T-A2-4 的化石表就是这个形状；只判 42P01 会让 Migrate 抛一个未处理的 driver 错误，
//     症状退回"没人认识的报错"，正是本自检要消灭的东西）
if legacy > 0 {
    return fmt.Errorf(
        "paused_sandboxes 里有 %d 行是阶段 3 之前的形状（state 为 running/publishing 但没有 execution_id）。"+
        "本 build 不做自动回填 —— 把 running 行降级成 paused 会让它们变成可抢，等于人为制造双活。"+
        "按 runbook 处置（dev/test：DROP TABLE paused_sandboxes 后重启本进程）", legacy)
}
```

**这条自检不是锦上添花。** `migrate.go:28-32` 那段注释本身写的就是这个教训的另一半：
> The failure lands on a machine whose own configuration is unchanged and whose logs name a constraint
> nobody there touched, which is the worst combination a schema owner can hand somebody.

不加自检，运维忘了 `DROP TABLE` 的症状是 PostgreSQL 抛
`check constraint "paused_sandboxes_execution_check" is violated by some row` —— 指向一个刚出生的约束名，
没有任何一句话告诉人该干什么。加了自检，症状是一句中文加一条命令。

🟢 **不引入 migration 框架、不引入 schema_version 表**。理由：本阶段唯一的非幂等动作是一次
`DROP TABLE`，而它由运维执行、有 runbook、只做一次。为它引入一套版本机制，是给"一次性动作"
造一个永久设施 —— 等 B 批次真的需要多步迁移时再引，那时表已经是全新的，没有历史包袱。
**这一条列入未决项 §6-U4 供裁决。**

> ✅ **裁决（对应 U4 与本节的 fail-fast 自检）**：
> - **不引入 migration 框架 / schema_version 表** —— 采纳本节的理由，不为一次性动作造永久设施；
> - 🔴 **`Migrate` 的 fail-fast 自检保留，且必须先于 `ADD CONSTRAINT`** —— 它是"运维忘了 `DROP TABLE`"时
>   唯一能把症状从"PostgreSQL 抛一个没人认识的约束名"翻译成"一句中文加一条命令"的东西。
>   这与 node 侧"加载失败必须响亮报错并指明处置命令"（`_design-phase3-node.md` §2.3）是同一条纪律的两侧。

### 2.9 对 dev/test 存量行、对 Console 直连的影响

**存量行**：方案 γ 下清零。影响面见 §2.7（节点本地记录不受影响，下一次 pause 自动重建行）。
执行前必须先 `SELECT state, count(*) … GROUP BY 1` 存档 —— 这也是 T2 N3 那条
"`invalid_rows` 至今零分辨力"顺手能取的样本。

**Agent-Console 直连 PG**（主仓 `apps/Agent-Console/internal/aenv/pausedregistry/reader.go`）：

- 它的 `selectSQL` 是一条**显式列清单**的 `SELECT`（`sandbox_id::text, cluster_id::text, state, generation,
  origin_node_id, COALESCE(claimed_by_node_id,''), COALESCE(snapshot_id::text,''), lease_expires_at, %s,
  paused_at, updated_at, metadata->>…`），无 `SELECT *`。
- 🟢 **A2 是纯加法（只加两列，不删列、不改名、不改类型）⇒ Console 的语句仍然成立，不会失明。**
- 🟡 **方案 γ 的表清零会让 Console 那一页变空**（`source` 仍是 `ok`，只是零行），
  这与"读不到"（`source=unavailable`）是两个显示结果，不会被误读成故障。
- 🔴 **明确记录这条已知代价**：如果将来有人在 A2 之上顺手删列或改名（例如把 `generation` 并进
  `execution_id`），Console 会**整页 `source=unavailable`** —— 任一列失效整条语句失败。
  本设计**不做**任何删列/改名，正是为了不欠这笔债。Console 切只读 API
  （`GET /registry/sandboxes` → `ListRegistrySandboxes` → `PostgresReader`）仍按任务书 §6.6 同批发布，
  但它不再是 A2 的**阻塞前置**。
- Console 已有 `columnExistsSQL` 探测模式（为 `sandbox_expires_at` 而写）。若后续要让 Console 展示
  `execution_id`，**照同一个模式加**，不要无条件 SELECT 新列。

**读侧 proto 是否要带 execution**：A6（外部只读暴露）落在 `RegistrySandbox`（`services/api/proto/scheduler.proto:318-344`）
与 `RegistryEntry`（同文件 `:405-430`）上，各加一个 `string execution_id`（**号已定死：13 / 10**，见任务书 §11 冻结契约表）。
🔴 **行号已于 2026-08-19 按源码订正**（原写 `:318-343` / `:405-427`，末行各差 1 / 3 行）；
🔴 **并注意 `RegistryEntry` 不在 `registry/registry.go` 里**（那个文件只有 195 行、行结构体叫 `Sandbox`，`registry.go:73-89`）—— 它是 **proto message**。
🔴 **两个 message 都要加**，否则 `GetSandboxes` 的批量读拿不到身份，B3 的 `Reconcile` 就无从比对。
`PostgresReader.selectColumns`（`postgres.go:30-40`）与 `PostgresStore.entryColumns`
（`store_postgres.go:30-41`）**两份列清单都要加** —— 这是本仓已知的"同一张表两份读模型"，
漏一份的症状是"某条路径上 execution 恒为空"，也就是 fencing 恒放行。

### 2.10 三份 DDL 手抄怎么同步

现状：`migrate.go:39`（真相源）+ `contract_test.go:59` + `postgres_integration_test.go:17`。
两份测试拷贝逐字相同（都没有 reclaim 索引），且注释都写着"copied verbatim from
`src/orchestrator/paused_registry/postgres.rs`"—— 而那个文件**已经不存在了**（§1.1）。

**方案（三步，全部只动测试）**：

1. **合并两份拷贝**成一个常量，挪进一个共享的测试文件（例如
   `services/scheduler/internal/registry/legacy_schema_test.go`），改名 `legacyNodeSchemaDDL`，
   注释改写成事实：**"这是 dev/test 集群里由旧节点建出来的表形状，冻结于 D11 之前。
   它是化石，**永不跟随 `SchemaDDL` 更新** —— 它存在的意义就是不跟。"**
2. **把 `TestMigrateAddsNoColumnOfItsOwn`（`store_postgres_test.go:350-380`）改名为
   `TestMigratePinsTheTableShape`**，并从"列集合等于节点的列集合"改成"列集合 + 约束定义
   等于本 build 钉住的清单"。它就是漂移告警器：**任何人给 `SchemaDDL` 加一列而不改这份清单，
   这个测试立刻红。**
3. 🔴 **两个"混合模式"测试要重新定性**：`TestMigrateIsIdempotentOverATableTheNodeCreated`（`:284`）
   与 `TestATableTheControllerCreatedAcceptsTheNodesBootstrap`（`:324`）测的是
   "节点和 controller 交替施加 DDL"，而**节点已经不施加 DDL 了**。
   - 前者**保留并改造**成"从化石形状升上来会被 §2.8 的自检明确拒绝"（见 T-A2-4）；
   - 后者**删除**：它断言的那个方向（节点在 controller 之后跑自己的 DDL）在这个 build 里不存在，
     留着只会让读者以为节点还会建表。

🟡 为什么不干脆让测试 `import` `SchemaDDL`：因为契约测试的价值恰恰是**不读实现**
（`contract_test.go:1-19` 与 `:100-110` 把这条写死了）。化石常量不是"实现的拷贝"，
它是**历史的快照**，两者的更新规则相反 —— 一个必须跟，一个必须不跟。把它们**命名区分开**
比让它们共享一个符号更能防漂移。

---

## 3. A3：写路径 fencing

### 3.1 proto 契约变更

```protobuf
message TransitionSandboxRequest {
  // …既有字段 1-9 不变…

  // 🔴 本次操作是哪个化身发起的。UUID v7，字符串形态与 sandbox_id / cluster_id 同规格。
  //
  // **内部必填**，不是 opt-in。e2b 的 ExpectExecutionID 是 opt-in（states.go:82-92：
  // 空值对"刚读完就动手 / 用户直接下令"的调用方是正确行为），我们不适用：
  // 我们的内部调用方全部是「controller 决定、node 执行」，不存在 fresh-read 直接动手的语义。
  // 而一个可选的 fencing token 必然要留一条"字段缺失就放行"的分支，那条分支就是全部攻击面。
  string execution_id = 10;
}
```

**契约层怎么改**（`registry_service.go`）：

| kind | 今天 | 目标 |
|---|---|---|
| `BEGIN_PAUSE` | `rejectFields(req, fieldGeneration\|fieldSnapshot)`（`:191-192`）| 加 `requireExecution(req)`；`fieldGeneration` 继续拒（begin_pause 无版本可引） |
| `MARK_RUNNING` | `rejectFields(req, fieldGeneration\|fieldMetadata\|fieldSnapshot)`（`:266-267`）| 加 `requireExecution(req)`；`fieldGeneration` 继续拒 |
| `COMPLETE_PAUSE` / `MARK_LOCAL_ONLY` / `RELEASE_CLAIM` | 已有 `requireGeneration` | ~~**加** `requireExecution`~~ ⇒ 🔴 ✅ **裁决 O3：本轮不加**，保持 generation-only（三条一并 `rejectFields(…\|fieldExecution)`，与 `REMOVE` 同规格）|
| `REMOVE` | 已有 `requireGeneration` | `rejectFields(req, …\|fieldExecution)` —— **明确拒收**，见 §2.3 #10 |

`requireExecution` 与 `requireGeneration`（`:433-440`）同形：

```go
func requireExecution(req *schedulerv1.TransitionSandboxRequest) (string, error) {
    v := strings.TrimSpace(req.GetExecutionId())
    if v == "" {
        return "", fmt.Errorf("%w: %v carries no execution id, and every write this build fences does",
            pausedregistry.ErrInvalidArgument, req.GetKind())
    }
    return v, nil
}
```

UUID 形态校验**放在 store 层**（`requireUUID("execution_id", …)`），与 `sandbox_id` / `cluster_id` 一致 ——
这样"格式不对"与"值不对"在同一层被拒，错误码也一致（`InvalidArgument`）。
形状与 `NodeIdentity.service_instance_id` 的现有先例对齐：Rust 生成
（`src/identity.rs:41-45`）→ 上 wire → Go 侧 trim + 非空校验（`services/scheduler/internal/service.go:345`
的 `Heartbeat` 分支）→ 单测（`service_test.go:644`）。

`AcquireSandboxRequest` 是否要带 execution：**取决于 §6-U1 的裁决**（方案 E-A 要，E-B 不要）。
✅ **裁决 U1 = E-A ⇒ 要带**：`AcquireSandboxRequest` 加 `string execution_id`，**必填**，
与 `claimed_by_node_id` 同一条 SQL 写入（§2.2 裁决块）。

> 🔴 **归属定稿（2026-08-19，任务书 §11.1(e)）：`AcquireSandboxRequest.execution_id = 5` 由 A2/A3 这个 PR 加，只加一次。**
> 理由：**写入它的是 `claimForResumeSQL`，在 `registry` 包内**，属于本设计的地界；
> [`_design-phase3-scheduler-a5.md`](_design-phase3-scheduler-a5.md) §4.1 **只消费、不加**（那份也已就地标注）。
> 🟡 点名是因为它同时出现在两份设计的字段清单里，不点名就会**两个实现 agent 都以为是对方的活**（撞号或漏加）。

> ✅ **裁决 O3 的两条落地后果（写在这里免得实现时漏）**：
> 1. **`execution_id` 只对 `BEGIN_PAUSE` / `MARK_RUNNING` 两个 kind 必填**（外加 `ACQUIRE`），
>    其余四个 kind **拒收**该字段 —— 于是"必填"这件事在契约层是**逐 kind 明示**的，没有"有就校验、没有就放行"的中间态。
>    这与 proto 注释里"内部必填、不是 opt-in"不矛盾：必填的是**被 fence 的那两条写**。
> 2. ⚠️ **`T-A3-5 TestTwoRefusalsAreToldApart` 要重排**：本轮不存在"同一条语句能同时产出两种拒绝"的 kind
>    （`begin_pause` / `mark_running` 拒收 generation，另外四条不收 execution）。
>    ⇒ 该用例改成**同一行、同一用例内两次不同 kind**：
>    (a) `complete_pause` 带错 generation ⇒ `ErrGenerationConflict`；
>    (b) `begin_pause` 带错 execution ⇒ `ErrExecutionFenced`；断言两者 `errors.Is` 互不成立。
>    **断言内容不变，只是产出两种拒绝的入口不同。**

### 3.2 `beginPauseSQL` 目标全文

```sql
WITH previous AS (
    SELECT snapshot_id FROM paused_sandboxes
     WHERE sandbox_id = $1::uuid AND cluster_id = $2::uuid
),
upserted AS (
    INSERT INTO paused_sandboxes (
        sandbox_id, cluster_id, state, generation, origin_node_id,
        claimed_by_node_id, snapshot_id, metadata, paused_at, updated_at,
        lease_expires_at, execution_id, execution_started_at
    )
    VALUES ($1::uuid, $2::uuid, 'publishing', 1, $3, NULL, NULL, $4::jsonb, now(), now(),
            now() + make_interval(secs => $5::double precision), $6::uuid, now())
    ON CONFLICT (sandbox_id) DO UPDATE SET
        state              = 'publishing',
        generation         = paused_sandboxes.generation + 1,
        origin_node_id     = EXCLUDED.origin_node_id,
        claimed_by_node_id = NULL,
        metadata           = EXCLUDED.metadata,
        paused_at          = EXCLUDED.paused_at,
        updated_at         = EXCLUDED.updated_at,
        lease_expires_at   = EXCLUDED.lease_expires_at
        -- execution_id / execution_started_at 刻意不在更新列表里：
        -- 谓词已经保证它们相等，写一遍只会让读者以为这里可以换代
    WHERE paused_sandboxes.cluster_id   = EXCLUDED.cluster_id
      AND paused_sandboxes.execution_id = EXCLUDED.execution_id   -- 🔴 A3
    RETURNING generation
)
SELECT upserted.generation          AS generation,
       previous.snapshot_id::text   AS previous_snapshot_id
  FROM upserted
  LEFT JOIN previous ON TRUE
```

**逐条谓词挡什么**

| 谓词 | 挡住什么 |
|---|---|
| `cluster_id = EXCLUDED.cluster_id`（既有）| 一个集群夺走另一个集群的行 |
| **`execution_id = EXCLUDED.execution_id`（新）** | ① §1.4 那条必然序列的第 4 步 —— 行已被 reclaim 清成 `NULL`，`E_A = NULL` 求值为 `NULL`，**不匹配**；② 行已被 B 的 `mark_running` 装上 `E_B`，`E_A ≠ E_B`，**不匹配**；③ 同一台节点上"旧化身的 in-flight pause"落到"新化身已经起来"的行上 |
| （隐含）`state` 不在谓词里 | **刻意的**：`running → publishing` 与 `publishing → publishing`（v3 重试）都合法，而两者的化身都对得上。用 execution 判比用 state 判更准 |

🔴 **`NULL` 的三值逻辑就是 fail-closed，这是本条谓词最值钱的性质**：任何"集群认为没有活化身"的行
（`paused` / `local_only` / `resuming`）都不可能被任何 `begin_pause` 命中。

**已知的诚实代价**：若某台沙箱的行是 `local_only`（`execution_id IS NULL`）而节点在**registry 不可达**
的窗口里本地 resume 成功（因而 `mark_running` 没跑成），随后的 pause 会被这条谓词拒。
后果**不是数据丢失** —— pause 本身仍然成功、沙箱在本机仍可唤醒，只是这一次的快照没能进登记表，
行继续停在 `local_only`（这正是 `local_only` 的定义）。
而这个窗口本身已经被护栏 §3.5 收窄：**登记过的沙箱在 registry 不可达时 resume 直接 500/503，不再 Proceed**
（`src/api/impls/paused_recovery.rs` 的 `arbitrate_resume`，T3 C3 实测）。
⇒ 我们**不为它加 fail-open 分支**。加一条 `OR (execution_id IS NULL AND state='local_only' AND origin_node_id=…)`
就等于承认"没有身份的行也能被声称拥有"，那正是任务书 §7.4 说的 fencing 剧场。
✅ **裁决（U5）：采纳 —— 不为 `local_only` 开这条 fail-open 例外**；理由如上：任何 fail-open 分支就是全部攻击面，
而这条的代价不是数据丢失（沙箱在本机仍可唤醒，只是这次快照没进登记表）。

### 3.3 `markRunningSQL` 目标全文

> ⚠️ **读法（2026-08-20 补）**：下面这段 SQL 里 `execution_started_at = now()` 这一行是**裁决前**写成的，
> 已被下方「✅ 裁决改判」订正。按"问题陈述不删"的纪律原文保留。

```sql
UPDATE paused_sandboxes
   SET state                = 'running',
       origin_node_id       = $2,
       claimed_by_node_id   = NULL,
       execution_id         = $6::uuid,
       execution_started_at = now(),        -- ⚠️ 已订正：分支 ③ 不重新盖戳，见下方裁决块
       generation           = generation + 1,
       updated_at           = now(),
       lease_expires_at     = now() + make_interval(secs => $3::double precision),
       sandbox_expires_at   = $5
 WHERE sandbox_id = $1::uuid
   AND cluster_id = $4::uuid
   AND (
         -- ① 正常跨节点 resume：本节点持有 claim
         (state = 'resuming' AND claimed_by_node_id = $2
                             AND execution_id = $6::uuid)      -- ✅ 裁决 U1 = E-A ⇒ 这一行【保留】，E-B 分支作废
         -- ② 本机自有 parked 行的就地 resume（claim 拿不到或未走 claim）
      OR (state IN ('paused', 'publishing', 'local_only')
          AND origin_node_id = $2
          AND claimed_by_node_id IS NULL)
         -- ③ 幂等重发：同一化身重复宣告自己在跑
      OR (state = 'running' AND origin_node_id = $2 AND execution_id = $6::uuid)
       )
```

**逐条谓词挡什么**

| 分支 | 允许什么 | 挡住什么 |
|---|---|---|
| ① `state='resuming' AND claimed_by_node_id=$2` | 只有拿到 claim 的那个节点能把 resume 收口 | 别的节点插队宣告；也挡住"没 claim 就宣告" |
| ① 的 `execution_id = $6`（E-A）| 只有 claim 时预分配的那个化身能收口 | 同一节点上"另一次 resume 的化身"抢走这次 claim |
| ② `origin_node_id=$2 AND claimed_by_node_id IS NULL` | 本机自有 parked 行就地唤醒（`local_only` 的唯一救回路径）| 别的节点唤醒不属于它的 parked 行；**行正被别人 claim 时（`claimed_by_node_id` 非空）一律不匹配** |
| ③ `state='running' AND execution_id=$6` | 同一化身重发（网络重试）幂等 | 🔴 **别的化身的 running 行** —— 这一条就是补 R5 那个洞：今天 `claimed_by_node_id IS NULL OR = $2` 对 running 行恒真，`state='running'` 分支现在要求 **`execution_id` 相等** |
| **`state='running'` 不在 ② 里** | —— | 🔴 **一次数据面流量抢走别人 running 行**（R5 实证的攻击）：`origin_node_id` 是别人的，分支 ③ 的 execution 也对不上，两条都不匹配 |

> ### ✅ 裁决改判（2026-08-20 主 agent）：`execution_started_at` 对同一化身**不可变** —— 分支 ③ **不重新盖戳**
>
> **改判的是什么**：上面 SQL 的 `execution_started_at = now()` 是无条件的，落到分支 ③（同一化身重复宣告 =
> 重试的 RPC）就是**每次重试都把起点往前推一次**。实现按字面写了；本裁决把它改成
>
> ```sql
> execution_started_at = CASE
>     WHEN state = 'running' AND execution_id = $6::uuid
>          THEN execution_started_at        -- 分支 ③：同一化身重发，起点不动
>     ELSE now()                            -- 分支 ①/②：真正的换代/安装
> END
> ```
>
> （`UPDATE` 的 `SET` 表达式读的是**更新前**的行值；WHERE 已经把行限死在三个分支里，
> `state = 'running'` 就唯一识别分支 ③ —— 分支 ② 排除了 `running`，分支 ① 要求 `resuming`。）
>
> 🔴 **理由**：这一列存在的**全部**理由就是 §2.5 那条 —— `updated_at` 被 `renewLeaseSQL` 每个心跳刷新，
> 不能当 §3.7 / B3 `KillOrphan` 的 grace 起点。**重新盖戳等于把刚逃开的抖动原样搬回来**：
> grace 会在每次重试时重启，而重试得最凶的恰恰是那台哪儿也去不了的孤儿。
> 今天 `mark_running` 不是周期性调用，所以这个缺陷**不可见**；B3 才是它咬人的地方，而那时已经没有任何东西
> 指得回这一行。
>
> 🟡 **分支 ①/② 仍然盖戳**：② 是安装点；① 是 claim 预分配的化身**真正把 VM 拉起来**的时刻，
> 比 claim 那一刻更准。
>
> 🟡 ~~**`markRunningUnfencedSQL`（write_fencing=false）不跟这次改判**：那条语句没有分支可分辨"重试"与"夺权"
> —— 关掉写面 fencing 正是放弃这个分辨力本身。化身可被任何人覆盖的行，其"化身起点"也就无从谈起。~~
>
> ### ✅ 上面这条 🟡 已改判（2026-08-20 收尾轮）：**两条语句都要满足这个不变式**
>
> **改判理由**：`execution_started_at` 的不可变性**不是一个 fencing 特性，是数据属性**。
> 若它随开关位置而变，B3 的 grace 起点就取决于"该行最后一次被写时开关在哪一档"，
> **翻一次开关就改变了既有数据的语义** —— 而且是对那些之后没人碰过的行也一样。
> `write_fencing` 该关掉的是**谓词**（拒不拒），不是**列的语义**。
>
> 另外，那条"没有分支可分辨"的理由本身不成立：判据 `state = 'running' AND execution_id = $6::uuid`
> 读的是更新前的行，**不需要任何谓词**就可判定 —— 它就是"这行已经叫着调用方要宣告的那个化身"，
> 两档开关下都是重试。所以 `markRunningUnfencedSQL` 现在带**同一个 CASE**。
>
> **测试**（两条语句各一发，共用同一组断言 `assertAReassertionDoesNotMoveTheStart`）：
> `TestReassertingTheSameExecutionDoesNotMoveItsStart`（fencing 开）与
> `TestReassertingTheSameExecutionDoesNotMoveItsStartWithFencingOff`（fencing 关），
> 均在 `execution_fencing_test.go`。
> 变异：把**任一条**语句改回无条件 `now()` ⇒ 对应那发必红（已实测：改 unfenced ⇒ 只有 `WithFencingOff` 那发红，
> fenced 那发照绿 —— 这正是上一版漏掉这半边的原因）。

> ✅ **裁决 U1 = E-A 之下，分支 ① 的语义定稿**：`$6` 是 **claim 时预分配、行上已经存在**的那个化身
> ⇒ 这条谓词是**校验**（"你还是我认领时那个吗"），不是安装。安装点只剩分支 ②（本机 parked 就地唤醒）。
> 🔴 node 侧必须送**同一个值**（`_design-phase3-node.md` §2.2.1 裁决 D-1 的 token 就是保证这一点的机制）；
> 送一个新铸的值 ⇒ 分支 ① 不匹配 ⇒ 正常的跨节点 resume 全线失败。**这是本轮最容易写错的一处，变异验证要专打它。**

**保持不变的两条既有性质**：

- 🔴 **绝不 INSERT**（`store.go:91-97` 与 `store_postgres.go:842-851` 都写死了）：
  从没被暂停过的沙箱没有行，`MarkRunningUntracked` 就是在说这件事。
- 0 行之后的**再读分类**（`:886-899`）仍然保留，且要多认一种结果（§3.5）。

### 3.4 其余四条 CAS

> ### ✅ 裁决 O3：本节的「加谓词」本轮**不做**，「清轴」本轮**必做**
>
> | 动作 | 裁决 | 一句话理由 |
> |---|---|---|
> | `completePauseSQL` / `markLocalOnlySQL` **加 `AND execution_id = $6`** | 🔴 **本轮不加**（下面两段 SQL 里那两行注掉）| 这两条是**纵深**不是主闸；`generation` CAS 已经够，而每多一个必填字段就多一处"忘了传"的失败面。不过度设计 |
> | `completePauseSQL` / `markLocalOnlySQL` **成功后置 NULL** | ✅ **必做** | 不是可选优化：目标态是 `paused` / `local_only`，§2.2 的 CHECK **强制**它们无化身；不清空语句直接报 `23514` |
> | `reclaimReleasedSQL` / `releaseHoldingsReleasedSQL` / `releaseClaimSQL` **置 NULL** | 🔴 ✅ **必做（裁决 #3）** | 三条夺权路径不清空 ⇒ §1.4 的必然序列**第一步就成立**，整条 fencing 白做 |
> | 上述三条**加谓词** | ❌ **不加** | 它们本来就不代表任何化身，加谓词等于让夺权动作需要被夺权者的同意 |
>
> 🔴 **v3「暂停必然落 OSS」把 `publishing` 变成长驻重试态之后要重新评估**：那时同一 generation 会存活很久，
> generation 的"新鲜度"作用被削弱，`completePause` 的化身谓词可能要补回来。**登记在案，随 v3 一起看，不在本轮。**

⚠️ **下面两段 SQL 里那两行 `-- AND execution_id = …` 是裁决前的形态，已作废、本轮不实现** ——
保留它们只为记录当初的权衡（见本节顶部裁决块与段末的 (a)/(b) 复盘）。**定稿：这两条只清轴、不加谓词。**

```sql
-- completePauseSQL 目标
UPDATE paused_sandboxes
   SET state = 'paused', snapshot_id = $3::uuid, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $4::double precision),
       execution_id = NULL, execution_started_at = NULL          -- 轴归位：paused = 无活化身
 WHERE sandbox_id = $1::uuid AND cluster_id = $5::uuid
   AND generation = $2 AND state = 'publishing'
-- AND execution_id = $6::uuid   -- ⚠️ 已作废形态，本轮【不实现】；定稿见 §3.4 顶部裁决块（O3：纵深非主闸）
```

```sql
-- markLocalOnlySQL 目标
UPDATE paused_sandboxes
   SET state = 'local_only', updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision),
       execution_id = NULL, execution_started_at = NULL
 WHERE sandbox_id = $1::uuid AND cluster_id = $4::uuid
   AND generation = $2 AND state = 'publishing'
-- AND execution_id = $5::uuid   -- ⚠️ 已作废形态，本轮【不实现】；定稿见 §3.4 顶部裁决块（O3：纵深非主闸）
```

🟡 **这两条的 execution 谓词是纵深防御，不是主闸**：`begin_pause` 已经把生成号推到最新，
任何在此之后夺权的转换都会再推一次，所以陈旧化身握着的 generation 早就过期了。
~~加上去的理由有两个~~ ⇒ 🔴 **本段两条理由已被裁决 O3 驳回，本轮不加谓词**（保留原文只为记录当初的权衡）：
(a) v3「暂停必然落 OSS」把 `publishing` 变成长驻重试态之后，**同一 generation 会存活很久**，
generation 的"新鲜度"作用被削弱 —— ✅ **成立，但那是 v3 那一批的事**，本条已按裁决 O3 登记为"随 v3 一起重新评估"；
(b) 字段本来就必填，让六个 kind 里五个都真正校验 —— ❌ **不成立**：裁决 O3 之后
`execution_id` **只对 `BEGIN_PAUSE` / `MARK_RUNNING`（+ acquire）必填**，其余四个 kind 在契约层**拒收**它，
所以"字段本来就必填"这个前提在本轮不成立。
⇒ **实现按本节顶部的 ✅ 裁决块：这两条只清轴、不加谓词。**

`reclaimReleasedSQL` / `releaseHoldingsReleasedSQL` / `releaseClaimSQL` 三条只加 `SET execution_id = NULL,
execution_started_at = NULL`（§2.3 #5/#6/#9），**不加谓词** —— 它们是夺权动作，本来就不代表任何化身。

`removeSQL` 不动（§2.3 #10 / §6-U3）。

### 3.5 拒绝语义：`ExecutionFenced` ≠ `GenerationConflict`

> ### ✅ 裁决：**采纳**，并且它是「错误码三件套」的第一件（答复 node O5 + 本节 + gateway D5）
>
> 三件套是**三段互不相同的链路**，谁也不许复用谁的码：
>
> | 段 | 形状 | 语义 |
> |---|---|---|
> | **scheduler RPC**（本节）| `ErrExecutionFenced → codes.PermissionDenied → Rust `PausedRegistryError::ExecutionFenced` | **永不重试**；与 `GenerationConflict → codes.Aborted`（重读再试）**严格分开** |
> | **node → gateway**（数据面）| **412** + `x-agentenv-refusal: sandbox_execution_superseded` | **内部信号**，不直通客户端（`_design-phase3-node.md` §3.7）|
> | **gateway → 客户端** | **409** + `code=sandbox_execution_superseded` | 对外唯一形状（本文姊妹篇 `_design-phase3-gateway.md` §6.3）|
>
> 一句话理由：合并会让节点**重读拿到新 generation 再发一次**，正好**绕过 fencing**。
>
> 🔴 **贯穿三段的硬约束（一处都不许破）**：
> - 任何一环**都不许用 404** —— 平台据此"授权重建工作区"，落成 404 就是用户工作区蒸发；
> - 也**不许复用 410** —— node 的 `/proxy` 已用它表示 not proxyable（`src/api/proxy.rs:869-872`）。

**新增一个 sentinel error 与一个 gRPC code**：

| 情况 | Go error | gRPC code | Rust 侧 | 调用方的正确反应 |
|---|---|---|---|---|
| 版本过期 | `ErrGenerationConflict`（`store.go:348`）| `Aborted`（`registry_service.go:688`）| `PausedRegistryError::GenerationConflict`（`central.rs:432`）| **重读 → 可以重试** |
| **化身过期** | **`ErrExecutionFenced`（新）** | **`PermissionDenied`（新）** | **`PausedRegistryError::ExecutionFenced`（新）** | 🔴 **永不重试**。这个化身对集群已经死了：停手、不发布、不删任何快照、把本地记录标成待孤儿回收 |
| 行不认识 | `ErrInvalidRecord` | `FailedPrecondition` | `Malformed` | 人工介入 |

🔴 **为什么必须分开**：两者的正确反应相反。把化身过期flatten 成 `Aborted`，节点会**重读再试** ——
而重读拿到的是新化身的 generation，用它再发一次 `begin_pause` 就会**绕过 fencing**。
这正是 `registryErrorCode`（`:682-700`）里已有的那句注释所说的同一类错误的镜像：
> Collapsing them would send a node into a re-read loop over a row that will never change.

**为什么选 `PermissionDenied`**：它逐字就是"你不是有资格做这件事的那个实体"。
`FailedPrecondition` 已经被 `ErrInvalidRecord` 占了（且语义是"这行没人能修"，与"你不是它的主人"不同），
`NotFound` 会与"没有行"混淆（而没有行是 `MarkRunningUntracked` 的正常答案，绝不能变成错误）。
🟡 **上线前要确认的一件事**：scheduler 的 gRPC server 目前**没有** auth interceptor
（全模块唯一的 gRPC 拦截器是 `MetricsUnaryInterceptor`，`services/scheduler/internal/metrics.go:259`，装配在 `services/scheduler/cmd/main.go:66`，**无任何鉴权拦截器**），所以 `PermissionDenied`
不会与传输层鉴权失败撞语义。**A4（node API 收窄）如果给这条 channel 加了 mTLS/token，
必须复查这一条** —— 列入未决项 §6-U2。

**0 行之后怎么知道是哪一种**：`UPDATE` 只告诉你 0 行。所以分类靠**再读**，而再读
🔴 **必须与写在同一个事务里**，否则分类本身会读到第三者写入后的行，把"化身过期"报成"版本过期"。
今天 `MarkRunning` 的再读（`store_postgres.go:886`）走的是 `s.fetch` —— **池上的另一条连接**，
已经是一个（良性的、只影响日志分类的）竞态；A3 让分类结果变成**调用方行为的分叉点**之后，
它就不再良性了。⇒ §3.7。

**Rust 侧映射**（node 侧实现，此处只定契约）：`central.rs:420-435` 的 `transition_failure`
现在只认 `(Code::Aborted, Some(expected))` 一条；要加
`(Code::PermissionDenied, _) => PausedRegistryError::ExecutionFenced { sandbox_id, execution_id }`，
并配一发与 `central.rs:2090-2126` 同形的负向测试（那里已经有 `Aborted ⇒ GenerationConflict` 的先例）。

### 3.6 零副作用：怎么保证、怎么验证

**保证（分两层，都是结构性的，不靠纪律）**

1. **DB 层**：所有 fencing 谓词都在**同一条** `UPDATE` / `INSERT … ON CONFLICT` 的 `WHERE` 里。
   单条语句在 PostgreSQL 里就是一个事务，谓词不匹配 ⇒ 0 行 ⇒ **一个字节都没写**，
   `generation` / `updated_at` / `lease_expires_at` 全都不动。
   这也是本仓既有的做法：`store_postgres.go` 全文只有 **2 处显式事务**
   （`reclaimExpiredHoldings:1043`、`ReleaseNodeHoldings:1135`，都是多行回收），
   其余全部靠单语句原子性。**A3 不改变这个形状。**
2. **文件层**：节点侧的调用顺序保证了"被拒 ⇒ 零字节写出"：
   `paused_coordinator.rs:284` `begin_pause`（失败即 `return None`，`:288-294`）
   → `:296-299` `publish_captured`（上传）
   → `:305-310` `complete_pause`
   → `:313-320` `delete_snapshot(previous)`。
   ⇒ **`begin_pause` 被拒 = 不上传、不翻牌、不删旧快照**，也就是 §1.5 那两个不可逆点一个都碰不到。

**验证（每条都要有断言，不能靠"看日志没报错"）**

- **行未变**：一个 `assertRowUnchanged(t, conn, sandboxID, before)` 助手，
  **逐列**比对（含 `generation` / `updated_at` / `lease_expires_at` / `execution_id` / `execution_started_at`）。
  🔴 只比 `state` 是不够的 —— 一次"状态没变但 `updated_at` 被刷新"的写，会把 §3.2 的 grace 推迟一整轮。
- **无文件写出**：Rust 侧用现成的测试替身断言 `publish_captured` 调用次数 = 0、
  `snapshot_manager.delete` 调用次数 = 0（`paused_coordinator.rs` 的 `test_support::CountingRegistry`
  已经是这个形状，`:1056-1090` 有 `mark_running_fails` 之类的开关先例）。**这条属于 node 侧设计的活**，
  本文只把它写成 A3 的验收项。

### 3.7 事务边界：走哪条路

| 路径 | 需要显式事务？ | 理由 |
|---|---|---|
| `beginPause` 的谓词校验 | **不需要** | 单条 `INSERT … ON CONFLICT … WHERE`。校验与写在同一条语句里，**没有"先查后写"的窗口** —— 这正是 e2b `packages/api/internal/sandbox/storage/redis/scripts.go:33-39` 那条教训的正解：`Add is lockless, so a resume can install a new incarnation between a Go-side comparison and this write` |
| `markRunning` 的谓词校验 | **不需要** | 同上，单条 `UPDATE` |
| `completePause` / `markLocalOnly` / `releaseClaim` | **不需要** | 同上 |
| 🔴 **0 行之后的"再读分类"** | **需要** | 见 §3.5。`MarkRunning` 今天的 `s.fetch`（`:886`）在另一条连接上跑，分类结果可能来自第三者写入之后的行。A3 让这个分类决定调用方"重试还是停手"，就必须与写同快照。做法：把 `MarkRunning`（以及新增分类的 `BeginPause`）的"写 + 再读"包进一个 `tx`，用**默认的 READ COMMITTED 即可** —— 需要的不是可串行化，只是"分类读到的是我这条写没匹配上的那一版" |

🟡 这会把显式事务从 2 处增加到 4 处。可接受：两条都是单行路径，锁范围就是那一行的行锁，
且 `withTimeout`（`:xxx` `defaultStoreQueryTimeout = 30s`）已经封顶。

### 3.8 `REMOVE_UNCONDITIONAL` 枚举清理

现状：`scheduler.proto` 里 `TRANSITION_KIND_REMOVE_UNCONDITIONAL = 6 [deprecated = true]`，
服务端 `registry_service.go:325-332` 已经 fail-closed 拒服务。

**目标**：

```protobuf
enum TransitionKind {
  TRANSITION_KIND_UNSPECIFIED    = 0;
  TRANSITION_KIND_BEGIN_PAUSE    = 1;
  TRANSITION_KIND_COMPLETE_PAUSE = 2;
  TRANSITION_KIND_MARK_LOCAL_ONLY= 3;
  TRANSITION_KIND_MARK_RUNNING   = 4;
  TRANSITION_KIND_RELEASE_CLAIM  = 5;
  TRANSITION_KIND_REMOVE         = 7;

  // 🔴 6 曾是 TRANSITION_KIND_REMOVE_UNCONDITIONAL —— 这个接口上唯一一处无守卫的破坏性写。
  // 保留而不是回收：一个还在发 6 的旧节点，如果 6 被新 kind 占用，它的"无条件删"就会
  // 静默变成另一件事。占着它，protoc 会替我们拒绝任何复用。
  reserved 6;
  reserved "TRANSITION_KIND_REMOVE_UNCONDITIONAL";
}
```

服务端：**删掉 `:325-332` 那个 case 分支**。删掉之后 `kind = 6` 解码成未知枚举值，落到
`default:`（`:334-336`）→ 同样的 `InvalidArgument`。⇒ 行为不变，分支少一个。
**但必须保留一发测试**断言 `kind = 6` 仍然是 `InvalidArgument`（§4.2 T-A3-8）——
否则"少一个分支"就变成"少一道闸"这件事没人守。

### 3.9 本设计**不做**什么（防范围蔓延）

| 不做 | 在哪做 | 为什么不在这里 |
|---|---|---|
| **发布权集中**（e2b `pause_instance.go:71-77` 那一形态：节点无自主 pause 权、翻牌只由中央做）| **B6** | controller 今天是纯被动 RPC 服务端，**没有下行命令通道**（`grep -E 'http\.Client\|http\.NewRequest\|Dial\(' services/scheduler` 零命中）。做它要先有命令下发面 |
| 存储层写锁 | 长期项 | 两家零先例；且踩在已知最不稳的 rustfs 上（并发 multipart ≥3 必 503）|
| envd token 绑 execution | `secure` 沙箱加强项，seed 统一后 | 只覆盖 `secure` 一半 |
| node API 收窄 | **A4**（同批，但独立 PR）| 边界画在哪要等 R1 结论 |
| 路由层拒旧化身 | **A5** | 保护的是可恢复的交互流量，不是不可逆的工作区 |
| `KillOrphan` 本体 | **B3** | A2 只负责把字段留好（§2.5）|
| Console 切只读 API | 主仓，同批发布 | A2 是纯加法，不阻塞它 |

---

## 4. 测试计划与变异验证

### 4.0 PG 测试环境（硬结论）

🟢 **本机与 CI 都具备真 PostgreSQL，A3 的变异验证可以做，没有阻塞。**

| 事实 | 证据 |
|---|---|
| gate 是环境变量 `SCHEDULER_REGISTRY_TEST_DSN` | `contract_test.go:84-96`、`postgres_integration_test.go:64-75` |
| 不给 DSN ⇒ **125 个测试静默 skip**（84 个 `store_postgres_test.go` + 41 个 `contract_*_test.go`），裸跑只剩 40 个纯逻辑测试，**一行 SQL 都不碰** | 基线 agent 实测 |
| 给了 DSN ⇒ **184 pass / 0 skip**，全套 49s | 同上 |
| 🔴 **反假绿开关 `SCHEDULER_REGISTRY_TEST_REQUIRED=1`** 把 skip 变成 `t.Fatal` | `contract_test.go:88-91`；已用"设 REQUIRED 但不给 DSN"这个必然失败的对照输入自证过 |
| CI 已经在跑真 PG | `.github/workflows/services-ci.yml:24-38`（`postgres:16-alpine` service）+ `:56-60`（注入 DSN 与 `REQUIRED: "1"`）|
| 无 testcontainers | 全仓无该依赖；容器由 CI service、`make test-with-postgres` 或人工 `docker run` 提供 |
| 夹具形态：**每个用例独立 schema + 独立 cluster id**，天然并行安全、可重复 | `contract_test.go:134-170` `contractSetup` / `contractSchemaName(t)` |
| `cargo-mutants` 未安装 | 不影响：A3 的变异验证是**手工变异**（把修复退回去看测试是否 FAIL）|

**规范入口（首选，本轮新增）** —— `services/Makefile:41-56` 的 `test-with-postgres`：
它自己拉一个 throwaway `postgres:16-alpine`（端口 `REGISTRY_TEST_PORT ?= 15499`）、注入 DSN 与
`SCHEDULER_REGISTRY_TEST_REQUIRED=1`，并额外要求 `redis-server` 在 PATH（否则 binding-store 测试
同样会静默 skip）。

```bash
make -C apps/AgentENV/services test-with-postgres      # 本设计的全部验收都跑这一条
```

🔴 **手跑单个用例时，三条硬要求一个都不能少（缺一条就是假绿或跑不起来）**：

```bash
# 必须在 services/ 目录下；必须三个都带
cd apps/AgentENV/services
GOWORK=off \
SCHEDULER_REGISTRY_TEST_DSN='postgres://postgres:verify@127.0.0.1:15499/aenv_registry?sslmode=disable' \
SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
  go test -count=1 -run TestAReclaimedSandboxCannotBePausedBackByItsOldNode ./scheduler/internal/registry/
```

1. **`GOWORK=off` 是硬前提** —— 父 worktree 的 `go.work` 未列入 `apps/AgentENV/services`，
   不设它直接报 `directory prefix . does not contain modules listed in go.work`。
2. **`SCHEDULER_REGISTRY_TEST_DSN` 必带** —— 否则 125 个用例静默跳过。
3. **`SCHEDULER_REGISTRY_TEST_REQUIRED=1` 必带** —— 否则"漏起 PG"与"全绿"长得一模一样。

4. 🟡 **`redis-server` 也要在 PATH** —— 否则 binding-store 测试是另一批静默 skip。
   `make test-with-postgres` 会先检查它并直接失败（`services/Makefile:42-44`），手跑时没人替你查。

若哪天连 docker 都没有，最小补救路径（`contract_test.go:14-18` 注释里就是这条）：

```bash
docker run -d --rm --name aenv-pg -e POSTGRES_PASSWORD=verify \
    -e POSTGRES_DB=aenv_registry -p 15499:5432 postgres:16-alpine
```

⇒ **PG 测试环境这一条对 A3 没有任何阻塞**：规范入口、CI job、手跑命令三条路都通，
且三条都带反假绿开关。复现脚本另有一份在 `/tmp/baseline/run-baseline.sh`。

### 4.1 A2 用例

| # | 用例 | 断言 | 🔴 变异（把修复退回去，本用例必须 FAIL）|
|---|---|---|---|
| **T-A2-1** | `TestMigratePinsTheTableShape`（改自 `store_postgres_test.go:350`）| `information_schema.columns` 等于钉住的 14 列；`pg_get_constraintdef` 含两条 CHECK | 从 `SchemaDDL` 去掉 `execution_id` 或那条 CHECK |
| **T-A2-2** | 🔴 **`TestARunningRowCannotExistWithoutAnExecution`** | 直连 SQL `INSERT … state='running', execution_id=NULL` 必须被拒（SQLSTATE `23514`）| **把 CHECK 去掉 / 改成只单向**（= 任务书那一发"把 `execution_id` 改成 nullable"）⇒ INSERT 成功 ⇒ FAIL |
| **T-A2-3** | `TestAParkedRowCannotCarryAnExecution` | `INSERT … state='paused', execution_id=<uuid>` 必须被拒（`23514`）| CHECK 只写 `state IN ('running','publishing') → NOT NULL` 的单向 ⇒ FAIL |
| **T-A2-4** | `TestMigrateRefusesAPrePhase3Table`（改自 `:284`）| 先用 `legacyNodeSchemaDDL` 建化石表 + 插一行 `state='running'`，`Migrate` 必须返回**含 "DROP TABLE" 与行数**的错误，且**不是** `23514` 原文 | 去掉 §2.8 的自检 ⇒ 错误变成 PostgreSQL 的约束原文 ⇒ 断言 FAIL |
| **T-A2-5** | `TestASnapshotDoesNotChangeTheExecution` | `MarkRunning(E)` → `BeginPause(E)` → 读回：`execution_id` 仍是 `E`，`generation` 涨 1 | 让 `beginPause` 的更新列表包含 `execution_id = EXCLUDED.execution_id` **并**允许不等 ⇒ 化身被换 ⇒ FAIL |
| **T-A2-6** | 🔴 **`TestReclaimClearsTheExecution`** | 造"租约过期 + 死线过期"的 running 行（沿用 `TestReclamationNeedsBothClocks:1539` 的造钟手法），`ReclaimExpiredHoldings` 后 `execution_id IS NULL` | 去掉 `reclaimReleasedSQL` 的 `execution_id = NULL` ⇒ FAIL（**这也是 T-A3-1 的前置**）|
| **T-A2-7** | `TestReleaseNodeHoldingsClearsTheExecution` | 同上，走 `ReleaseNodeHoldings` | 去掉 `releaseHoldingsReleasedSQL` 的清空 ⇒ FAIL |
| **T-A2-8** | `TestCompletePauseParksTheRowWithoutAnExecution` | `complete_pause` 成功后 `state='paused' AND execution_id IS NULL` | 去掉清空 ⇒ 违反 CHECK ⇒ 语句直接报错（也 FAIL，方向不同但同样有牙）|
| **T-A2-9** | `TestTheReadModelCarriesTheExecution`（扩 `:866`）| `Get` / `GetMany` / `ListRegistrySandboxes` 三条读路径都返回 `execution_id` | 只在 `entryColumns` 加、忘了 `postgres.go:30-40` 的 `selectColumns` ⇒ 某一条路径恒空 ⇒ FAIL |
| 🔴 **T-A2-10** | 🔴 **`TestReleaseClaimClearsTheExecution`**（**2026-08-19 补编号**：三条夺权路径此前只有 reclaim / releaseHoldings 进了编号表，releaseClaim 那条只在 §4.5 有名字没编号）| 造 `resuming` 行（claim 已预分配 `E`）→ `ReleaseClaim` 成功后 `execution_id IS NULL` **且** `execution_started_at IS NULL` **且** `state` 已回退、`claimed_by_node_id IS NULL` | 去掉 `releaseClaimSQL` 的 `execution_id = NULL` ⇒ 行还带着 `E`，**§1.4 的必然序列第一步就成立** ⇒ FAIL。🔴 与 T-A2-6/7 同族：**三条夺权路径缺一条清空，A3 整条 fencing 就白做** |

### 4.2 A3 用例

| # | 用例 | 断言 | 🔴 变异 |
|---|---|---|---|
| **T-A3-1** | 🔴 **`TestAReclaimedSandboxCannotBePausedBackByItsOldNode`** —— **必然序列**，详见 §4.3 | 老节点 `BeginPause(E_A)` 返回 `ErrExecutionFenced`；行**逐列**未变 | 删掉 `beginPauseSQL` 的 `AND paused_sandboxes.execution_id = EXCLUDED.execution_id` ⇒ 返回成功、行被改成 `publishing`/`originA` ⇒ FAIL |
| **T-A3-2** | 🔴 **`TestAResumeCannotSlipInBetweenTheCheckAndTheWrite`** —— **并发插队**，详见 §4.4 | 在"外部事务持行锁 + 期间装上新化身"的交错下，`BeginPause(E_A)` 必须被拒且行未变 | **把校验挪到事务外的 handler**（`registry_service.go` 里先 `store.Get` 比 execution 再调无谓词 SQL）⇒ 读到旧值通过、UPDATE 覆盖新化身 ⇒ **确定性 FAIL** |
| **T-A3-3** | `TestAStaleExecutionCannotStealARunningRow` | 行 `running`/`originB`/`E_B`；`MarkRunning(nodeA, E_A)` ⇒ `MarkRunningHeldElsewhere`，**不是** `Adopted`；行未变 | 把 `markRunningSQL` 谓词换回 `claimed_by_node_id IS NULL OR = $2` ⇒ 返回 `Adopted`、行被抢 ⇒ FAIL |
| **T-A3-4** | `TestTheSameExecutionCanReassertItself` | 行 `running`/`originA`/`E_A`；`MarkRunning(nodeA, E_A)` 幂等成功 | 去掉分支 ③ ⇒ 正常重试被误拒 ⇒ FAIL（**这是"探针有分辨力"的对照面**：证明 T-A3-3 的拒绝不是因为把所有 running 行都拒了）|
| **T-A3-5** | 🔴 **`TestTwoRefusalsAreToldApart`** | 同一行、同一用例内两次：(a) **正确 execution + 错 generation** ⇒ `ErrGenerationConflict`；(b) **正确 generation + 错 execution** ⇒ `ErrExecutionFenced`。断言两者 `errors.Is` 互不成立 | 把 `ErrExecutionFenced` 定义成 `ErrGenerationConflict` 的包装 ⇒ (b) 的 `errors.Is(err, ErrGenerationConflict)` 为真 ⇒ FAIL |
| **T-A3-6** | `TestExecutionFencedIsPermissionDenied`（`registry_service_test.go`）| `registryErrorCode(ErrExecutionFenced) == codes.PermissionDenied`，且 `!= codes.Aborted` | 映射成 `Aborted` ⇒ FAIL |
| **T-A3-7** | `TestATransitionWithoutAnExecutionIsRefused` | 六个 kind 中五个（除 `REMOVE`）缺 `execution_id` ⇒ `InvalidArgument`；`REMOVE` **携带** `execution_id` ⇒ `InvalidArgument` | 把必填改成"缺就放行" ⇒ FAIL（这一发直接打任务书 §7.4 的 opt-in 陷阱）|
| **T-A3-8** | `TestTheUnconditionalRemoveIsStillRefused` | `kind = 6` ⇒ `InvalidArgument` | 在 proto 里去掉 `reserved 6` 并让新 kind 占用 6 ⇒ 请求被当成新语义服务 ⇒ FAIL |
| **T-A3-9** | `TestARefusedPauseWritesNothing` | `assertRowUnchanged` 逐列（含 `updated_at` / `generation` / `lease_expires_at`）| 把 fencing 谓词从 `WHERE` 挪到 `SET`（写了再判）⇒ `updated_at` 变 ⇒ FAIL |

### 4.3 T-A3-1「必然序列」怎么写、需要什么夹具

**沿用现有夹具模式**（每个用例独立 schema + 独立 cluster id，`contractSetup`/`newStoreFixture`），
**不另造一套**。造两个时钟的手法直接抄 `TestReclamationNeedsBothClocks`（`store_postgres_test.go:1539`）。

```
前置：f := newStoreFixture(t)；E_A, E_B := uuid v7 ×2；S := sandboxUUID(n)

① 让行以 running/E_A/originA 存在
   f.beginPause(S, nodeA, E_A)                       // 行诞生：publishing/E_A
   f.store.CompletePause(…, gen, snapshotID)         // → paused / execution NULL
   claim := f.store.ClaimForResume(…, nodeA)         // → resuming
   f.store.MarkRunning(…, nodeA, E_A, expiresAt=过去) // → running/E_A，且 sandbox_expires_at 已过期

② 把租约推到过去（直连 conn，抄 :1539 的手法）
   UPDATE paused_sandboxes SET lease_expires_at = now() - interval '1 hour'

③ reclaim
   f.store.ReclaimExpiredHoldings(cluster)
   断言：state='paused'，execution_id IS NULL     ← T-A2-6 的复用

④ B 接管
   f.store.ClaimForResume(…, nodeB)                  // → resuming
   f.store.MarkRunning(…, nodeB, E_B, …)             // → running/E_B
   before := f.readRow(S)                            // 逐列快照

⑤ 🟢 探针自证（必须先跑，否则本用例毫无分辨力）
   f.store.BeginPause(…, originNode=nodeB, exec=E_B) // 必须【成功】
   回滚到 ④ 的状态（或用另一个 sandbox id 跑这一步）

⑥ 老节点 A 的 1 秒自动 pause
   err := f.store.BeginPause(…, originNode=nodeA, exec=E_A)
   断言：errors.Is(err, ErrExecutionFenced)
   断言：assertRowUnchanged(t, conn, S, before)      ← 逐列
```

🔴 **第 ⑤ 步是本轮方法论里翻过车的那一条**（grace 期"拒绝接管"的第一发探针用了一行任何相位都
认领不了的合成行，`409` 看着像被拒、实则毫无分辨力）。这里的对照输入是"**同一个 sandbox、
同一条语句、只换 execution**"，能证明拒绝确实来自新谓词，而不是来自 `cluster_id`、`state` 或别的东西。

**需要的新夹具**（三个小助手，都放测试文件里）：

| 助手 | 作用 |
|---|---|
| `f.readRow(sandboxID) rowSnapshot` | 直连 conn 读**全部**列（含 `execution_id` / `execution_started_at` / `updated_at`），返回可比较的结构体 |
| `assertRowUnchanged(t, conn, id, before)` | 逐列 diff，报告第一处不同的列名与前后值 |
| `f.expireLease(sandboxID)` / `f.expireDeadline(sandboxID)` | 两条直连 UPDATE，把两个时钟分别推到过去（抄 `:1539`）|

### 4.4 T-A3-2「事务内原子性」的确定性交错夹具

**问题**：正式实现是单条语句，无法在中间插桩；而"并发压测 + 概率复现"不是可判据。

**解法：用一个外部事务持有行锁，把交错变成确定的。**

```
t 起一个直连事务 tx：
    BEGIN;
    SELECT 1 FROM paused_sandboxes WHERE sandbox_id = $S FOR UPDATE;   -- 行锁在手

goroutine G 调用 store.BeginPause(cluster, S, originNode=nodeA, exec=E_A)
    → 单条 INSERT…ON CONFLICT 需要写这一行，被行锁挡住，【阻塞】

主协程在 tx 里执行"装上新化身"的等价写：
    UPDATE paused_sandboxes
       SET state='running', origin_node_id=$nodeB,
           execution_id=$E_B, execution_started_at=now(), generation=generation+1,
           updated_at=now()
     WHERE sandbox_id=$S;
    COMMIT;                                        -- 锁释放

G 解除阻塞：
    ✅ 正式实现：谓词在【解锁之后】才求值，看到的是 E_B ⇒ 0 行 ⇒ ErrExecutionFenced
    ❌ 变异实现（校验在 handler，先 Get 再无谓词 UPDATE）：
       它的 SELECT 【不会被行锁阻塞】（PG 的 MVCC 读不阻塞写锁），
       所以它在 tx 提交【之前】就读到了旧值 E_A、判定通过；
       随后它的无谓词 UPDATE 阻塞、解锁、【覆盖】掉 running/E_B
       ⇒ 返回成功 + 行变成 publishing/originA ⇒ 断言 FAIL
```

🔴 **这个夹具的价值在于：两侧都不需要 `sleep`，失败是确定的。**
它精确复现 e2b 那句注释描述的场景 ——
`a resume can install a new incarnation between a Go-side comparison and this write`。

**同一个夹具复用一次**：把 `BeginPause` 换成 `MarkRunning(nodeA, E_A2)`，外部事务里装上 `E_B`，
验证 `markRunningSQL` 的分支 ③ 同样不可插队。

### 4.5 目前**裸奔**的写点（A2/A3 动到它们就必须补测）

基线实测：只有 4 个测试直接点名 stale generation
（`TestCompletePauseRefusesAStaleGeneration:637`、`TestRemoveRefusesAGenerationTheRowHasMovedPast:1870`
及 2 个 Contract 对应），而 SQL 里有 **10 处** generation 写点（§1.2）。

| 写点 | 现有覆盖 | A2/A3 会动它吗 | 要补什么 |
|---|---|---|---|
| `:484` completePause CAS | ✅ | 是 —— 🔴 **只清轴，不加 execution 谓词**（裁决 O3；本行原写"加 execution 谓词"已作废）| T-A2-8 / T-A3-5 |
| `:1179` remove CAS | ✅ | 否 | —— |
| 🔴 `:519` markLocalOnly **CAS** | ❌ **无直接 stale-generation 用例**（`:683` 测的是"匹配不到就报告"，不是版本过期）| 是 | 补 `TestMarkLocalOnlyRefusesAStaleGeneration` |
| 🔴 `:786` releaseClaim **CAS** | ❌ 同上（`:1158` 测的是"匹配不到算成功"）| 是 —— 🔴 **只清轴，不加 execution 谓词**（裁决 O3 已把"E-A 下加谓词"这句驳回；`releaseClaim` 是三条夺权路径之一，加谓词等于让夺权动作需要被夺权者同意）| 补 `TestReleaseClaimRefusesAStaleGeneration` **加** `TestReleaseClaimClearsTheExecution`（✅ 后者已在 §4.1 编号为 **T-A2-10**）|
| 🔴 `:396`/`:594`/`:618`/`:834`/`:998`/`:1110` **六个递增点** | ❌ **没有任何测试断言"涨了几次"** | **全部**（每个都要写/清 execution）| 补一发 `TestTheVersionAxisMovesExactlyOncePerWrite`：一条完整 pause→resume→pause 链路上逐步断言 `generation` 的绝对值序列（1→2→3→4…），这是"两条轴分工"唯一可机械验证的形式 |

---

## 5. 发布顺序与回退

### 5.1 发布顺序（不可交换）

> 🔴 **本节只是「A1/A3 这一项自己的依赖方向」，不是执行清单。**
> 三份设计各写了一条顺序、方向不同（A4 要 gateway 先、A5 是 node→scheduler→gateway），
> 已按命名裁决 **N5** 合成一条 13 步的线性 runbook ——
> **执行以 [`_impl-plan-control-plane-phase3.md` §6](_impl-plan-control-plane-phase3.md#6-总发布-runbook-n5三份设计的顺序合成一条) 为准**。
> 本节对应那条 runbook 的**步骤 2（破坏性步骤）→ 步骤 3（node）→ 步骤 4（scheduler）**。

```
1. node 先发：带上 execution_id 字段（旧 controller 把未知字段当 unknown field 忽略，行为不变）
2. controller 后发：A2 迁移 + A3 校验一起上
```

🔴 **反过来会全线挂**：新 controller + 旧 node ⇒ 旧 node 不发 `execution_id` ⇒ 必填校验
⇒ 全集群 pause / resume 全部 `InvalidArgument`。
🟢 **正向是安全的**：proto3 的未知字段在旧 controller 的 generated struct 里进 `unknownFields`，被忽略。

运维前置（在 controller 发布之前）：两套集群各执行一次 `DROP TABLE paused_sandboxes`（§2.7）。

🔴 **一个只有合成 runbook 才看得见的后果**：`DROP TABLE`（步骤 2）与新 controller 起来（步骤 4）之间，
**整个 aenv 没有 scheduler**（步骤 2 把它 scale 到 0，且要等 node 滚完才起）⇒ 那一段是
**控制面 + 数据面同时不可用**的服务窗口，**不是**"控制面短暂不可用"。范围、时长与进入前置见
[§6.3](_impl-plan-control-plane-phase3.md#63--步骤-2破坏性步骤唯一一步会打掉存量沙箱有独立确认点)。
本节的两步顺序不变，但**不要据此以为"scheduler 停一下就好"**。

### 5.2 回退（配置级）

任务书 §6：**一个阶段的回退不许依赖新写的回滚逻辑**。

**主回退面 = 一个配置开关**，沿用本仓已有的先例
`SCHEDULER_REGISTRY_WRITE_ENABLED`（`services/shared/config/config.go:538-544`，
`strconv.ParseBool` + 默认值表 `:493-504`）：

```
scheduler.registry.write_fencing = true|false          # 默认 true
SCHEDULER_REGISTRY_WRITE_FENCING=false
```

> 🔴 **命名裁决 N2（2026-08-19 主 agent）**：这个开关**不叫** `execution_fencing`。
> 本轮一共有**三个**作用面不同的开关，名字必须自带作用域，否则运维读到「execution_fencing 关了」
> 会以为两边都关了 —— 而写路径那个才是致命的那半：
>
> | 开关 | 类型 | 关掉什么 |
> |---|---|---|
> | `scheduler.registry.write_fencing` | bool（默认 `true`）| **本节** —— A3 的两条 SQL 谓词 |
> | `scheduler.routing.execution_arbitration` | `off\|observe\|enforce`（默认 `enforce`）| binding 仲裁与 `LookupNode` 应答（`_design-phase3-scheduler-a5.md` §11）|
> | `gateway.routing.execution_fencing` | `off\|observe\|enforce`（默认 `enforce`）| gateway 路由层拒绝（`_design-phase3-gateway.md` §10）|
>
> ✅ **三个分家是刻意的，只改名、不合并**（理由见 `_design-phase3-scheduler-a5.md` §12 冲突点 2：
> 必须能分别关，共用会让一次止血顺手关掉另一半，而那一半的失效是无声的）。

实现形态**必须是"两条 SQL 常量二选一，在 store 构造时选定"**，不是"在语句里加一个
`OR $flag` 的分支"：

- `beginPauseSQL` / `beginPauseSQLUnfenced`
- `markRunningSQL` / `markRunningSQLUnfenced`（~~= 今天的形状~~）
  > ✅ **已订正（2026-08-20）**：unfenced 那条**不是**逐字的"今天的形状"，它比今天多两处，且两处都不是可选的：
  > ① `execution_id` 的**写入**（CHECK 是 DDL，不跟开关走，不写就 `23514`）；
  > ② `execution_started_at` 的 **CASE**（同一化身重发不重新盖戳 —— 见 §3.3 那条改判：
  > 这是列的语义，不是 fencing 的特性）。关掉的只有**谓词**。

理由：`OR` 分支会让"关掉时"的语句与"开着时"的语句是**同一条**，那条语句就永远无法被单独测试，
而且谓词求值顺序对 planner 是不可控的。两条常量各自被测试覆盖（关掉时的行为 = 今天的行为 = 已有测试）。

**关掉时必须是刺耳的**：
- 启动日志 `Warn` 一行，写清"registry write fencing 已关闭，旧化身可以覆盖新化身的行"
- `/healthz` 的 phase 输出里带 `write_fencing=disabled`（`PausedRegistryService.Phase()` 已有这个出口）
- 常驻 metric `agentenv_scheduler_registry_write_fencing_enabled{} = 0`
  > ✅ **已订正（2026-08-20）**：这一个 gauge **单独读不出结论** —— "写面根本没装配"（query-only 副本、
  > `write_enabled=false`、没配 DSN）也读 0。已新增 `agentenv_scheduler_registry_write_surface_enabled`，
  > 三个 gauge 在**每一条启动路径**上都被显式设值；告警条件写成
  > `write_surface_enabled == 1 and write_fencing_enabled == 0`。
  > 组合读法权威表见 `_impl-plan-control-plane-phase3.md` §6.4 下方「三个 registry gauge 的组合读法」。

**A2 的回退**：schema 不回退（多两个 nullable 列对旧 build 无影响 —— 旧 build 的
`entryColumns` / `selectColumns` 是显式列清单，多出的列不会被选中）。
若必须彻底回退：`DROP TABLE` 再跑一次旧 build 的 `Migrate`。**两个方向都只需要一次运维动作，
不依赖任何新写的回滚代码。**

🟡 **注意**：`write_fencing=false` 不会让 A2 的 CHECK 失效（CHECK 是 DDL 层的）。
所以关掉 fencing 之后，节点仍然必须发 `execution_id`（否则 `running` 行写不进去）。
⇒ **这个开关关的是"校验"，不是"字段"。** 这一点要写进开关的注释，否则有人会以为关掉它就能回到
"节点不用改"的世界。

---

## 6. 风险与未决项（✅ 2026-08-19 U1–U5 已全部裁决，见表后「裁决收口」）

| # | 未决项 | 选项 | 影响面 | 倾向 |
|---|---|---|---|---|
| **U1** | 🔴 ✅ **已裁决 = E-A**（见表后收口 + 任务书 §10 第 1 条；正文 §2.2 / §2.7 / §3.3 已同步）。原问题：**`resuming` 状态下 `execution_id` 是空还是预分配**（方案 E-B / E-A）| **E-A**：`AcquireSandbox` 带上 claimer 预分配的 execution，写进行。`markRunning` 分支 ① 变成真正的"校验 execution"，且 §3.7 KillOrphan 不必给 `resuming` 开特例。**代价：A1 必须把化身分配提前到 resume 决策点**（今天在 `src/sandbox/custom_extension/client.rs:271,300` 的 VM start 里）。<br>**E-B**：`resuming` 恒空，`markRunning` 分支 ① 只靠 `claimed_by_node_id`。**代价：B3 必须给 `resuming` 行开 grace，否则每一次正常 resume 在 roster 到达前都会被判孤儿** | 跨 A1 / A2 / A3 / B3 四项 | **E-A**。它把一个"以后要记得开特例"的债换成一次接口调整，而特例正是最容易在 B3 实现时漏掉的东西 |
| **U2** | `ErrExecutionFenced` 用 `codes.PermissionDenied` | 备选：新增一个 message-level 的结构化 detail 而复用 `Aborted` | A4 若给这条 gRPC channel 加 mTLS/token 鉴权，`PermissionDenied` 会与传输层拒绝撞语义 | `PermissionDenied` + 在 A4 落地时复查。（今天 scheduler gRPC **无** auth interceptor，已核）|
| **U3** | `remove` 要不要带 execution | 现设计：不带（generation-only）。备选：带，且"行有 execution 时必须相等" | 备选会引入一条 fail-open 分支；且真正的收益（平台重试队列里的陈旧 delete = G3 本体）要平台传 execution，P2 明确本轮不做 | **不带**，把它留给 G3 那一批一起做 |
| **U4** | `Migrate` 要不要引入版本表 / migration 框架 | 现设计：不引入，靠一次运维 `DROP TABLE` + 一道 fail-fast 自检 | B 批次（B1/B3/B4/B5）还会动这张表；到时若需要多步迁移，会重新面对这个问题 | 现在不引入。届时表已全新、无历史包袱，引入成本更低 |
| **U5** | `beginPauseSQL` 是否为 `local_only` 开一条 fail-open 例外 | 现设计：**不开**（§3.2 末尾）。代价：registry 不可达窗口里本地 resume 过的 `local_only` 沙箱，之后的 pause 进不了登记表 | 该窗口已被护栏 §3.5 收窄（登记过的沙箱在 registry 不可达时 resume 直接 5xx）| **不开**。任何 fail-open 分支就是全部攻击面 |
| **R1** | 🟡 A2 的 CHECK 是一条会**拒绝写入**的约束。将来某个新写点忘了维护身份轴，症状是**语句直接报错**而不是静默漂移 | —— | 这是刻意的（§2.4 #3），但要确保 `fail()`（`registry_service.go:666-679`）把 `23514` 映射成一个可读的错误而不是 `Unavailable` | 落地时补一条 `check_violation ⇒ ErrInvalidRecord`（`FailedPrecondition`）的映射 |
| **R2** | 🟡 显式事务从 2 处增到 4 处（§3.7），锁范围是单行行锁 | —— | 与 `reclaimExpiredHoldings` 的集群级事务不同量级，但仍是新的锁面 | 可接受；`defaultStoreQueryTimeout = 30s` 已封顶 |
| **R3** | 🟡 dev/test 两套集群的登记行清零，短期内**失去跨节点恢复**（本机唤醒不受影响，下次 pause 自动重建行）| —— | 需要在 runbook 里写清，并在执行前存档 `SELECT state, count(*) … GROUP BY 1` | 接受（P1）|

### ✅ 裁决收口（2026-08-19 主 agent，逐条对应上表）

> 上表的问题陈述与选项**一个字都不删**（它记录了"当初为什么是个问题"）；下表是最终裁决，**实现以下表为准**。

| # | ✅ 裁决 | 一句话理由 | 正文落点 |
|---|---|---|---|
| **U1** | 🔴 **E-A（预分配）**：claim 时分配 execution，与 `claimed_by_node_id` **同事务**写入 | ① 让 `mark_running` 成为真正的"校验 execution"；② E-B 要求 B3 记得给 `resuming` 开特例，而"注释承诺别处会做但没做"正是本仓踩过的坑；③ 它使 gateway 的 S5 可满足 —— resume 窗口内**正常设防**而不必选"不设防" | §2.2 裁决块、§2.3、§3.1、§3.3；node 侧 §2.2.1 |
| **U2** | **采纳** `codes.PermissionDenied` | 逐字就是"你不是有资格做这件事的那个实体"；今天 scheduler gRPC 无 auth interceptor，不撞语义。⚠️ **A4 若给这条 channel 加 mTLS/token，必须复查是否与传输层鉴权失败混淆** | §3.5 |
| **U3** | **采纳**：`remove` 保持 generation-only（并**拒收** execution）| 真正需要 execution 的是"平台重试队列里的陈旧 delete"（G3），而那要平台传，P2 明确本轮不做；提前开口只会引入一条 fail-open 分支 | §2.3 #10、§3.1 |
| **U4** | **采纳**：不引入 migration 框架 / 版本表 | 唯一的非幂等动作是一次由运维执行、有 runbook 的 `DROP TABLE`；为一次性动作造永久设施是增熵 | §2.8 |
| **U5** | **采纳**：`beginPauseSQL` **不为 `local_only` 开 fail-open 例外** | 任何 fail-open 分支就是全部攻击面；且该窗口已被护栏 §3.5 收窄，代价不是数据丢失（沙箱本机仍可唤醒）| §3.2 末 |

---

## 7. 落地清单（文件级，供实施 agent 用）

| 文件 | 改什么 |
|---|---|
| `services/api/proto/scheduler.proto` | 🔴 **字段号已定死，见任务书 §11.1(e)（唯一分配表，已按源码核实零撞号）**：`TransitionSandboxRequest.execution_id = 10`；`TransitionKind` 删 `= 6` 改 `reserved 6` + `reserved "TRANSITION_KIND_REMOVE_UNCONDITIONAL"`；`RegistryEntry.execution_id = 10`（message 在 `:405-430`）；`RegistrySandbox.execution_id = 13`（`:318-344`）；`AcquireSandboxRequest.execution_id = 5`（U1 = E-A **已裁决 ⇒ 条件成立，必加**）。🔴 **以上五项全部由本 PR 加一次**，`_design-phase3-scheduler-a5.md` 那个 PR 只消费、不重复加 |
| `services/scheduler/internal/registry/migrate.go` | `SchemaDDL` 换成 §2.7 的新形状；`Migrate` 前加 §2.8 的 fail-fast 自检 |
| `services/scheduler/internal/registry/store_postgres.go` | `entryColumns:30-41` 加两列；`beginPauseSQL:381`、`markRunningSQL:831` 按 §3.2/§3.3 重写；`completePauseSQL:479`、`markLocalOnlySQL:514`、`releaseClaimSQL:781`、`reclaimReleasedSQL:995`、`releaseHoldingsReleasedSQL:1107` 按 §2.3/§3.4 加谓词与清轴；`MarkRunning`/`BeginPause` 的 0 行再读包进 `tx`；`scanEntry`/`scanClaim` 解两列 |
| `services/scheduler/internal/registry/store.go` | ⚠️ ~~`Store` 接口**五个**方法签名加 execution 参数~~ **已订正（2026-08-20）：只有 3 个**。带 execution 入参的是 `BeginPause`（经 `BeginPauseInput.ExecutionID` 字段）、`ClaimForResume`、`MarkRunning`；另外三个写点（`CompletePause` / `MarkLocalOnly` / `ReleaseClaim`）**只在 SQL 里把身份轴清成 NULL**，行为由 `expectGeneration` 决定，**签名一个字都不用改**（裁决 O3：这三条本轮不加 `= $exec` 谓词）。🔴 别照“五个”去给后三个硬塞参数 —— 那等于把被 O3 驳回的谓词从后门加回来 |
| `services/scheduler/internal/registry/postgres.go` | `selectColumns:30-40` 加 `execution_id`（读侧）|
| `services/scheduler/internal/registry_service.go` | 新增 `fieldExecution` + `requireExecution`；六个 kind 的 `rejectFields`/`require*` 调整；`registryErrorCode:682` 加 `ErrExecutionFenced ⇒ PermissionDenied` 与 `23514 ⇒ ErrInvalidRecord`；删 `:325-332` 的 `REMOVE_UNCONDITIONAL` case；`registryEntryToProto:709` 带上 execution |
| `services/scheduler/internal/service.go` | `ListRegistrySandboxes` 的行转 proto 带上 execution |
| `services/shared/config/config.go` | `SchedulerRegistryConfig` 加 `WriteFencing bool`（默认 true）+ `SCHEDULER_REGISTRY_WRITE_FENCING` env（抄 `:538-544` 的 `WRITE_ENABLED` 形态）。🔴 **名字按 N2 裁决，不叫 `ExecutionFencing`**（§5.2）|
| `services/scheduler/internal/registry/legacy_schema_test.go`（新）| `legacyNodeSchemaDDL` 化石常量，合并 `contract_test.go:59` 与 `postgres_integration_test.go:17` |
| `store_postgres_test.go` / `contract_*_test.go` / `registry_service_test.go` | §4.1 / §4.2 / §4.5 的全部用例 |
| （node 侧，非本设计范围）| `central.rs` 送 execution 上 wire + `PermissionDenied ⇒ ExecutionFenced` 映射与负向测试；`paused_coordinator.rs` 对 `ExecutionFenced` 的终态处理（不重试、不发布、不删快照）|

**✅ 裁决后对上表的增量修正（实现时以这几条为准）：**

- `scheduler.proto`：`AcquireSandboxRequest` 加 `string execution_id` 的条件**已成立**（U1 = E-A），不再是"若…时"；
- `migrate.go`：`SchemaDDL` 里的 CHECK 用 **§2.2 裁决块那一份**（状态集含 `'resuming'`）；
- `store_postgres.go`：`claimForResumeSQL` / `claimForResumeDurableOnlySQL` **写入** execution；
  `completePauseSQL` / `markLocalOnlySQL` **只清轴、不加谓词**（O3）；
  `releaseClaimSQL` **只清轴、不加谓词**；`reclaimReleasedSQL` / `releaseHoldingsReleasedSQL` **必须清轴**；
- `registry_service.go`：`requireExecution` 只挂 `BEGIN_PAUSE` / `MARK_RUNNING`（+ acquire 路径），
  `COMPLETE_PAUSE` / `MARK_LOCAL_ONLY` / `RELEASE_CLAIM` / `REMOVE` 一律 `rejectFields(…|fieldExecution)`；
- 测试：`T-A2-3` 不许用 `resuming` 造反例（E-A 下它合法）；`T-A3-5` 按 §3.1 的 ✅ 块重排成"两个 kind 各产一种拒绝"。
- 🔴 **不在本文范围、但 A5 依赖的那一块**（gateway 设计 §5 的 S1/S2/S3/S6：binding 存储持久化 execution、
  heartbeat roster 带 execution、冲突仲裁改 v7 大者胜、`RecordAssignment` 可选带 execution）
  落在 `services/scheduler/internal/store.go` / `lookup.go`，**本文 §0 的范围声明不含它们** ——
  归属另一份 scheduler-A5 设计，别让它掉在两份文档中间。

---

## 附：本设计引用的全部证据锚点

| 断言 | 证据 |
|---|---|
| 登记表唯一真相源 | `services/scheduler/internal/registry/migrate.go:39-71` |
| NOT NULL 列不安全 | `migrate.go:28-32` |
| 节点侧 DDL 已删 | `src/orchestrator/paused_registry/` 只剩 `central.rs`/`disabled.rs`/`mod.rs`/`types.rs` |
| generation 十个写点 | `store_postgres.go:396,484,519,594,618,786,834,998,1110,1179` |
| begin_pause 只比 cluster | `store_postgres.go:381-409` |
| mark_running 守卫失效 | `store_postgres.go:831-839`（守卫本身在 `:839`）|
| 契约层主动拒收 generation | `registry_service.go:191-192`、`:266-267`、`:411-431` |
| TTL 自动 pause 1 秒一跳 | `src/orchestrator/service.rs:2093-2115` + `config/default.toml:198` |
| reclaim 把 running 翻 paused | `store_postgres.go:995-1004` |
| 删 previous 快照 | `src/api/impls/paused_coordinator.rs:313-320` |
| 每次 pause 新 snapshot UUID | `src/api/impls/paused_coordinator.rs:685-690` |
| publish 的调用顺序 | `paused_coordinator.rs:284 → :296 → :305 → :313` |
| renew_lease 刷 updated_at | `store_postgres.go:921-931`（`:925`）|
| ExecutionID = UUID v7 先例 | `src/identity.rs:41-45`、`src/sandbox/custom_extension/client.rs:56-60` |
| 身份字段的 Go 侧校验先例 | `services/scheduler/internal/service.go:345`、`service_test.go:644` |
| Aborted ⇒ GenerationConflict（D8 已完成）| `src/orchestrator/paused_registry/central.rs:432`、`registry_service.go:688`、负向测试 `central.rs:2090-2126` |
| REMOVE_UNCONDITIONAL 已 fail-closed | `registry_service.go:325-332` |
| e2b：enforcement 必须在 Lua 内 | `packages/api/internal/sandbox/storage/redis/scripts.go:33-39` |
| e2b：ExpectExecutionID 是 opt-in | `sandboxtypes/states.go:82-92`、`errors.go:47` |
| e2b：发布权集中 | `pause_instance.go:71-77`、`orchestrator.proto:56-59` |
| e2b：孤儿判据只看 `raw != nil`（现成缺口）| `storage/redis/main.go:205-217` |
| CI 已跑真 PG | `.github/workflows/services-ci.yml:24-38`、`:56-60` |
| 测试 PG gate 与反假绿开关 | `contract_test.go:84-96`、`postgres_integration_test.go:64-75` |
| 配置开关先例 | `services/shared/config/config.go:493-504`、`:538-544` |
| Console 直连的列清单 | 主仓 `apps/Agent-Console/internal/aenv/pausedregistry/reader.go` `selectSQL` |
