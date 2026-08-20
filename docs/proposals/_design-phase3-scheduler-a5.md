# 阶段 3 · A5 设计（Go scheduler 侧）：让路由答案带化身，并把答案变成权威推导的

> 2026-08-19 · **设计产物，本轮不改任何源码**。
> 范围：`services/scheduler/`（binding 存储、`lookupNode`、`NodeRegistry` roster、`RecordAssignment`）
> + `services/api/proto/scheduler.proto` 的读路径消息。
> **不在范围**：gateway 的三段闸与拒绝形状（见 `_design-phase3-gateway.md`）、node 侧比对与 A1 化身铸造
> （见 `_design-phase3-node.md`）、登记表 DDL 与写路径 fencing（见 `_design-phase3-scheduler.md`，本文简称 **A2/A3 设计**）。
>
> 需求来源：[`_design-phase3-gateway.md`](_design-phase3-gateway.md) §5「我需要 scheduler 提供什么」S1–S8 + §2.2/§2.3 两个实证缺陷。
> 任务书：[`_impl-plan-control-plane-phase3.md`](_impl-plan-control-plane-phase3.md) §3 A5 / §9 第 18 条。
> 姊妹文档：[`_design-phase3-scheduler.md`](_design-phase3-scheduler.md)（A2/A3）、[`_design-phase3-node.md`](_design-phase3-node.md)（A1/A4）。
>
> **前提（主 agent 已裁决，非待议项）**：E-A 预分配 / S1 必做且必须在 HA 形态下验证 /
> `ErrExecutionFenced → PermissionDenied` 与 `GenerationConflict → Aborted` 严格分家 /
> 不给 running 沙箱建行 / 热路径 UUID v7 大者胜 / 闸 3 本轮不做 / 默认 `enforce` 但发布先跑 `observe` /
> D6 与 D9 并入本轮。
>
> 🚦 **2026-08-19 二次裁决回填**：本文 §13 的 **U1–U6 已全部裁决**，两条口径纠正（S1 / S8）也已采纳，
> 收口表在 [`_impl-plan-control-plane-phase3.md` §10.3](_impl-plan-control-plane-phase3.md#103--scheduler-a5-设计新增未决项的裁决2026-08-19)
> 与本文 §13 表后的 ✅ 块。**两处冲突时以任务书 §10.3 为准。**
> 🔴 **另外三条命名裁决（N1 头名小写 / N2 三个开关改名 / N3 wire 字符串只留一个）已就地改写本文正文**，
> 冻结契约表在任务书 §10.2。

---

## 0. 三十秒版

1. **两个实证缺陷都成立，而且比 gateway 说的更宽**：无条件覆盖不只在内存实现
   （`store.go:99-114`），**Redis 实现的 Lua 里是同一个洞**（`redis_store.go:293-300`）——
   而 HA 生产跑的正是 Redis 那条。
2. 🔴 **S1 必做，但 gateway 给的理由要改口径**：binding 带 execution 的第一价值**不是**喂 expect 头，
   而是**没有它就做不了 S2 的仲裁**。我复核下来，**闸 1 在本阶段的真阳性集合接近空集**（§2），
   A5 的实质保护来自「路由答案变正确」，不是来自「落点拒绝」。这条要报上去，否则会做出 fencing 剧场。
3. 🔴 **仲裁必须在存储内部原子完成** —— 内存版在锁内，**Redis 版必须在 Lua 脚本里**。
   Go 侧「GET → 比较 → SET」就是 A3 设计 T-A3-2 那条 e2b 教训的同构
   （`packages/api/internal/sandbox/storage/redis/scripts.go:33-39`），且有确定性测法（数 Redis 往返，§10.A）。
4. 🔴 **推翻 gateway §4.1 的一句话**：`HeartbeatRequest.sandbox_ids` **不能立刻 `reserved 8`**。
   DaemonSet 滚动不是原子的，新 scheduler + 旧 node 会读到**空 roster**，两个 binding 存储都会
   把该节点名下的 binding **删光**（`store.go:90-97`、`redis_store.go:306-308`），
   后果是**所有从未 pause 过的沙箱在整个滚动窗口里数据面 404**。⇒ 并存一个发布周期（§4.2、§13-U2）。
5. **`PLACED` / `PINNED` 必须报 `PENDING`** 成立，而且我能给出比 gateway 更硬的理由：
   按 A2 的 CHECK，`paused` / `local_only` 行的 `execution_id` **恒为 NULL**，只有 `publishing` 非空，
   而那个非空值指向的是**一台已经停机的 VM**（§5.2）。
6. **「不给 running 建行 ⇒ 无行 ⇒ 无双活」成立，而且可以加强**：只有登记表行能授权在另一台机器上重建，
   无行 ⇒ **集群没有任何机制造得出第二个化身**（§7），并可配一发机械保证。
7. **回退是配置级三态开关** `scheduler.routing.execution_arbitration` = `off|observe|enforce`
   （env `SCHEDULER_ROUTING_EXECUTION_ARBITRATION`），与 A2/A3 的 `scheduler.registry.write_fencing`
   （写路径 bool，env `SCHEDULER_REGISTRY_WRITE_FENCING`）**刻意分家**，理由见 §12 冲突点 2。
   🔴 **两个名字都是 2026-08-19 命名裁决 N2 定的**（原稿是 `SCHEDULER_EXECUTION_ROUTING` /
   `SCHEDULER_REGISTRY_EXECUTION_FENCING`）：三个开关的名字必须自带作用域，只改名、**不合并**。

---

## 1. 起点事实：两个实证缺陷的独立复核

### 1.1 🔴 缺陷一：解析结果本身会指向旧化身 —— **成立，且覆盖面比 gateway 描述的大一倍**

**内存实现**（gateway §2.2 点名的那处）：

```go
// services/scheduler/internal/store.go:99-102（InMemoryBindingStore.ReconcileNode）
expiresAt := now.Add(s.bindingTTL)
for sandboxID := range normalized {
    s.upsertLockedWithExpiry(sandboxID, node, expiresAt)   // ← 无条件覆盖
}
```

`upsertLockedWithExpiry`（`store.go:122-135`）在 `existing.node.ID != node.ID` 时把反向索引从旧节点摘走，
然后**无条件**写入新记录。⇒ 谁的心跳最后到，谁就是 binding 的主人。

**Redis 实现**（gateway **没有**点到，而这是 HA 生产路径）：

```lua
-- services/scheduler/internal/redis_store.go:293-300（redisReconcileNodeScriptSource）
for sandbox_id, _ in pairs(desired) do
  local old_node_id = parse_node_id(redis.call("GET", binding_key(sandbox_id)))
  if old_node_id and old_node_id ~= node_id then
    redis.call("SREM", node_key_for(old_node_id), sandbox_id)
  end
  redis.call("SET", binding_key(sandbox_id), value, "PX", ttl_ms)   -- ← 同样无条件
  redis.call("SADD", node_key, sandbox_id)
end
```

脚本**已经读了**旧值（`parse_node_id`），只是读来摘反向索引，**从不据此拒绝写入**。
⇒ 仲裁所需的读已经在脚本里了，加谓词是**零额外往返**。

`Record`（`store.go:65-75` / `redis_store.go:89-115`，`RecordAssignment` 的落点）同样无条件。

**roster 侧**（`lookup.go:373-399`）：

```go
// 注释逐字，lookup.go:375-378
// More than one node listing the same sandbox is normal mid-takeover: the
// origin keeps its paused record until its own reconciliation drops it. The
// most recent report wins, ...
if !found || lastSeen.After(bestSeen) { best, bestSeen, found = node, lastSeen, true }
```

**三处都是到达顺序仲裁**。gateway §2.2 的必然序列成立：reclaim 把 X 判给 B，A 恢复联系后
每个心跳都把 binding 抢回去，流量在两个化身之间来回抖。

🟡 **一条对 gateway 描述的收窄（要说清，免得后来者按错的模型排错）**：租约续期
（`RenewNodeLease`）与心跳（`Heartbeat`）都打向同一个 primary 进程。一个**续不上租约**的节点
（reclaim 的前提）通常也**心跳不上**，所以它的 binding 会先按 30s TTL 自然过期。
⇒ 缺陷一的真正杀伤时刻不是「分区期间」，而是**旧节点恢复联系之后**：它一回来就把 binding 抢走，
而且**每个心跳周期抢一次，永不收敛**。这才是必须修的那一半；而「binding 冷启动时旧节点先上报」
是另一个**有界**窗口（§13-R2）。

### 1.2 🔴 缺陷二：热路径从不读登记表 —— **成立，并且现有测试对 HA 形态零覆盖**

```go
// services/scheduler/internal/lookup.go:143-145（注释逐字）
// 1. The binding. This is the hot path — every proxied request lands here —
// so nothing below it may run on a hit.
```

`lookup.go:151-160` 命中即 `return lookupResponse(node, BOUND, "")`，登记表是第 3 步（`:185-216`）。
HA 下数据面走 query-only 副本（gateway `server.go:226` 用 `s.queryOnlyScheduler`），
而该副本**没有 placer**（`service.go:153-167` 注释逐字 "No placer either"），
一旦落到 `running` / `resuming` 分支就直接 `Unavailable`（`lookup.go:220-228`）。
⇒ **query-only 副本上能成功回答数据面的路径实际只有 binding 命中一条。**

🔴 **我要补 gateway 没查到的那一条**：现有的 query-only 测试
（`lookup_test.go:527-586` `TestQueryOnlyLookupRunsTheSameLadder`）**全部用 `NewInMemoryBindingStore`**，
而配置校验明写 query-only **必须**配 Redis（`shared/config/config.go:826-829`）。
⇒ **HA 真正跑的 Redis 序列化路径（JSON value + 两段 Lua）在测试里零覆盖。**
一个「只给 InMemory 加了 execution、忘了 Redis 的 JSON/Lua」的实现，**今天所有测试都会绿**。
这就是主 agent 要求「必须有一发钉住 HA / query-only 形态」的实证依据（§10.C）。

### 1.3 🟡 缺陷三（本文新发现）：`isCanonicalUUID` 接受大写 ⇒ 字典序仲裁不安全

```go
// services/scheduler/internal/registry/store_postgres.go:1395
isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
```

`requireUUID`（`:1372-1381`）**原样返回 trim 后的字符串**，不做大小写归一。
ASCII 序里 `'0'–'9'(0x30) < 'A'–'F'(0x41) < 'a'–'f'(0x61)`，
⇒ 同一时刻铸造的两个 v7，一个写成 `7F2A…`、一个写成 `7f2a…`，**字典序会反转**。
Rust 的 `Uuid::to_string()` 今天输出小写，所以实际不会发生 —— 但**这条不变式今天无人守**，
而 §6 的仲裁规则整个建立在它上面。⇒ 必须在入口归一化并配一发变异（§10.A）。

### 1.4 现状盘点：本设计要动的每个点

| # | 位置 | 今天 | A5 之后 |
|---|---|---|---|
| 1 | `store.go:14-18` `BindingStore` 接口 | 三方法只认 `Node` / `[]string` | 认 `Binding{Node, ExecutionID}` / `[]RosterEntry` |
| 2 | `store.go:65-135` InMemory | 无条件覆盖 | 锁内仲裁 |
| 3 | `redis_store.go:89-156` + 两段 Lua | 无条件覆盖 | **Lua 内**仲裁 |
| 4 | `lookup.go:151-160` binding 命中 | 只回 node + BOUND | 回 execution + authority |
| 5 | `lookup.go:166-177` roster 命中 | 同上 | 同上 |
| 6 | `lookup.go:230-294` PLACED / PINNED | 同上 | authority=PENDING，execution 留空 |
| 7 | `lookup.go:296-327` running / resuming | 同上 | 回行上的 execution，authority=REGISTRY |
| 8 | `lookup.go:365-371` `lookupResponse` | 三字段 | 五字段 |
| 9 | `lookup.go:379-399` `rosterHolder` | 最近上报者胜 | execution 大者胜，平手退回最近上报者 |
| 10 | `service.go:361` `Heartbeat` | 传 `req.GetSandboxIds()` | 传 roster（带 execution）+ 兼容回落 |
| 11 | `service.go:305-341` `RecordAssignment` | `store.Record` 无仲裁 | 走同一条仲裁 |
| 12 | `node_registry.go:29-38,54-58,255,450-484` roster | `[]string` | `[]RosterEntry` |
| 13 | `reconcile.go:196-197,:279` roster 消费点 | 读 `SandboxIDs` | 读 `Entries[].SandboxID`（本轮不做判定） |
| 14 | `service.go:648-663` / `registry_service.go:709` 行转 proto | 无 execution | 带 execution |

---

## 2. 🔴 闸 1 的真正射程：我复核 gateway 论证后必须提出的修正

这一节是本设计里最重要的一段，因为它决定「我们是在做 fencing 还是在做 fencing 剧场」
（任务书 §7.4）。

### 2.1 路由与 expect 永远来自同一个答案

把 `lookupNode` 的五条出口逐个摊开：

| 出口 | 路由到哪台 | execution 来自哪 | 两者同源？ |
|---|---|---|---|
| binding 命中（`:151-160`） | `binding.node` | `binding.execution` | ✅ 同一条记录 |
| roster 命中（`:166-177`） | `rosterHolder` 选中的节点 | 该节点 roster 里的 execution | ✅ 同一次心跳 |
| PLACED（`:230-252`） | `placer.place()` 选的节点 | —（PENDING） | —— |
| PINNED（`:254-294`） | `entry.OriginNodeID` | —（PENDING） | —— |
| registry BOUND（`:296-327`） | `entry.Holder()` | 同一行的 `execution_id` | ✅ 同一行 |

⇒ **gateway 下发的 expect，永远是它要路由到的那台机器自己（直接或经登记表）报上来的值。**

### 2.2 推论：闸 1 的真阳性集合接近空集

设 gateway 路由到节点 N、下发 expect = E。N 上该沙箱的活化身记为 L。

- **E == L**：放行。这是绝大多数请求。
- **E ≠ L**：只可能因为「N 自己在上报 E 之后又换了代」⇒ **L 比 E 新**。
  这是**合法的同机换代**（TTL 自动 pause 默认 1 秒一跳 + 数据面 auto-resume 铸新化身，
  `src/orchestrator/service.rs:2093-2115`、`src/api/proxy.rs:105-107`），
  按等值比对会被**误拒**，而且误拒发生在用户正等着唤醒的时刻。
- **L 比 E 旧**：需要「中央知道一个比 N 上活着的更新的化身，却仍然把流量路由给 N」。
  由 §2.1，路由与 expect 同源 ⇒ **这个组合在本阶段的 `lookupNode` 里构造不出来**。

⇒ 🔴 **两条硬结论，都要传导给 gateway / node 设计**（✅ **两条都已裁决采纳**：
第 1 条 = A5-U4，已传导进 [`_design-phase3-node.md`](_design-phase3-node.md) §3.7 第 3/3b 条
与 [`_design-phase3-gateway.md`](_design-phase3-gateway.md) §3.1/§3.2；
第 2 条 = A5-U3，任务书 §3 的 A5 行判据已按它重写）：

1. **node 的比对必须是有序的，不是等值的**：
   `L < E ⇒ 拒`，`L >= E ⇒ 放行`（UUID v7 小写 canonical 字典序即时间序）。
   语义读作：「中央知道一个比我新的化身 ⇒ 我已被取代」vs「我比中央知道的新 ⇒ 中央只是落后了，
   而它路由到的就是我，不存在第二份活的要防」。
   **不改成有序，A5 会在最常见的合法事件（同机 pause→resume）上批量制造 409。**
2. **A5 在本阶段的实质保护是「路由变正确」，不是「落点拒绝」**。
   任务书 A5 判据「打向旧化身的请求被拒（非仅仅改道）」，在 scheduler 侧只能兑现成
   「打向旧化身的请求**不再产生**」。闸 1 保留，但它的真阳性率**预期恒为 0**，
   而 **0 不是成功的证据** —— 成功的证据是 §9 的路由指标。
   ⇒ 列入 §13-U3 请主 agent 认账或调整判据措辞。

### 2.3 那闸 1 为什么还要做

三条，都成立：

- **它是为 B4/B6 上膛的**。B4 把 execution 的铸造点搬到 controller 之后，expect 的来源
  不再是节点自报，`L < E` 立刻变成可达状态。那时闸 1 从空转变成主闸，而**接口那时已经在**。
- **它把「中央落后」从静默错路由变成可观测事件**（配上有序比对后，`L > E` 也应当计数而非放行沉默）。
- **闸 2（回声）是本轮唯一能自证 S1–S4 真的接上了的东西**：gateway 的
  `unfenced_no_authority` 与本文 §9 的 `lookup_execution_authority_total{authority="unknown"}`
  应当**逐条对得上**，对不上就是有一侧算错了。这是跨服务的探针自证。

---

## 3. S1–S8 逐条回应

### 速览

| # | 结论 | 一句话 |
|---|---|---|
| **S1** | ✅ **做，强制** | 两个实现都存 execution；**理由改口径**：它是 S2 仲裁的前提，不是 expect 头的饲料 |
| **S2** | ✅ **做，强制**，但**改做法** | roster 带 execution + 仲裁**在存储内部原子完成**；🔴 `sandbox_ids` 不得立刻 `reserved` |
| **S3** | ✅ **做**，附两条硬约束 | 小写归一 + 时钟偏移边界可推导（≥ `lease_ttl_floor` = 30s）；warn + 指标 |
| **S4** | ✅ **做** | `LookupNodeResponse` 加两字段；PLACED/PINNED→PENDING 复核**成立且理由更硬** |
| **S5** | ✅ **做（E-A）**，附一条新耦合 | claim 预分配同事务写入；🔴 node **必须复用** claim 给的 execution，否则每次跨节点 resume 都失败 |
| **S6** | ✅ **做，但升级为不可降级的一半** | 字段可选；**「走同一条仲裁」不可选**，否则它是绕过 S2 的后门 |
| **S7** | ✅ **做**，由 A2/A3 那份负责加字段 | 两个 message + 两份列清单 + 两处转换，漏一处的症状是「某条路径恒空」 |
| **S8** | ✅ **不改，并加机械保证** | `NotFound` 只从 `lookupAbsent` 产生；🔴 顺手发现 `PermissionDenied` 在 gateway 落 502，明令 `LookupNode` 不得用它 |

---

### S1 — binding 存储持久化 `execution_id`

**做/不做**：🔴 **做，强制。内存与 Redis 两个实现都要，且必须由同一份契约测试驱动。**

**改哪里**

| 文件 | 函数 / 常量 | 改什么 |
|---|---|---|
| `scheduler/internal/store.go:14-18` | `BindingStore` 接口 | `Get(sandboxID, now) (Binding, bool, error)`；`Record(sandboxID, Binding, now) error`；`ReconcileNode(node Node, roster []RosterEntry, now) error` |
| `store.go:27-30` | `bindingRecord` | 加 `executionID string` |
| `store.go:50-63,65-75,77-116,118-135` | InMemory 四个方法 | 读写 execution + §6 仲裁 |
| `redis_store.go:17-19` | `redisBindingRecord` | 加 `ExecutionID string \`json:"execution_id,omitempty"\`` |
| `redis_store.go:199-212` | `parseRedisBindingBytes` | 解出 execution；**缺字段 ⇒ 空 ⇒ unknown，不报错** |
| `redis_store.go:214-229` | `redisLuaHelpers` | `parse_node_id` → `parse_binding`，同时取 `node_id` 与 `execution_id` |
| `redis_store.go:231-249` / `:251-310` | 两段 Lua | 加仲裁谓词（见 S2） |
| `lookup.go:145,151-160` | binding 命中分支 | 把 execution 与 authority 带进 `lookupResponse` |

**新类型（放 `store.go`，与 `Node` 同文件）**

```go
// Binding 是「这台沙箱现在在哪台机器上、是哪一个化身」。
// 两半必须一起搬：节点 id 说往哪转发，化身 id 说这次转发的是不是集群认的那一份。
type Binding struct {
    Node Node
    // ExecutionID 是小写 canonical UUID v7。空字符串 = 不知道，**不是**「没有化身」——
    // 它只会出现在 A1 之前的节点写下的记录上，语义是「这条记录不参与仲裁」。
    ExecutionID string
}

// RosterEntry 是心跳 roster 的一项。
type RosterEntry struct {
    SandboxID   string
    ExecutionID string
}
```

**proto**：见 §4。

**不做的后果**
🔴 **不是「A5 少一层保护」，是缺陷一永久存在**：没有存下来的 execution，`ReconcileNode`
就没有可比对的对象，S2 的仲裁**根本写不出来** ⇒ 活着的旧化身每个心跳把 binding 抢回去，
流量在两个化身之间来回抖，A5 的其余部分全部无意义。
HA 下更糟：Redis 是**所有副本共享**的，一次错误覆盖对整个集群可见。

---

### S2 — roster 带 execution + `ReconcileNode` 不再按到达顺序覆盖

**做/不做**：🔴 **做，强制**，但**两处改做法**。

#### S2.1 仲裁必须在存储内部原子完成

- 内存版：在 `s.mu` 持有期间比对 + 写入（今天已经在锁内，加谓词即可）。
- 🔴 **Redis 版必须在 Lua 里**。`redis_store.go:285`/`:294` 的 `GET` 已经在脚本里，
  加一个 `if` 是零额外往返。**绝不允许**把仲裁写成 Go 侧「`Get` → 比较 → `Record`」：
  那正是 A3 设计 §3.7 引用的 e2b 教训的同构 ——
  > `Add is lockless, so a resume can install a new incarnation between a Go-side comparison and this write`
  —— 只不过那边的窗口在 SQL 前，这边的窗口在 Redis 前。
  有确定性测法（数 Redis 往返，§10.A `TestRedisReconcileTakesOneRoundTrip`），不靠并发压测。

#### S2.2 🔴 `HeartbeatRequest.sandbox_ids` **不能立刻 `reserved 8`** —— 推翻 gateway §4.1

gateway §4.1 写「P1 说无向后兼容包袱 ⇒ 直接 `reserved` 旧字段号重画，不做双写过渡」。
**这条对本字段不成立**，理由是实证的：

1. P1 的「无兼容包袱」说的是**外部**契约。node↔scheduler 是两个**可独立滚动**的进程，
   node 是 DaemonSet（`deploy/k8s/base/agentenv-daemonset.yaml`），滚动**不是原子的**。
2. 新 scheduler + 旧 node ⇒ 旧 node 发字段 8，新 struct 里没有该字段 ⇒ 落 `unknownFields` ⇒
   `req.GetRoster()` 为空 ⇒ **两个存储都会把该节点名下的 binding 删光**：
   - 内存：`store.go:90-97`，`len(normalized) == 0` 分支遍历 `nodeBinding[node.ID]` 全删；
   - Redis：`redis_store.go:283-291` 把 `current` 里不在 `desired` 的全 `DEL`，
     `:306-308` 再 `DEL node_key`。
   同时 `node_registry.go:255` 的 `normalizeRoster(req.GetSandboxIds())` 也变空 ⇒
   **roster 兜底同时失明**（`lookup.go:166-177` 与 `rosterHolder` 一起哑）。
3. 落到第 3 步登记表：**从未 pause 过的沙箱没有行**（§7）⇒ `lookupAbsent` ⇒ **404**。
   ⇒ **整个滚动窗口里，所有从未 pause 过的沙箱数据面 404。**

   🟢 澄清爆炸半径，不夸大：这**不会**触发平台的「授权重建工作区」——
   平台只认 `POST /sandboxes/{id}/resume` 的 404（主仓 `client.go:58-71`），
   而**被 resume 的沙箱一定 pause 过、一定有行**，会走 PLACED 正常返回。
   代价是「活着的沙箱在窗口内不可达」，不是工作区蒸发。但仍然足够严重。

**目标做法（并存一个发布周期）**

```proto
message HeartbeatRequest {
  // …1-7 不变…
  // 🔴 保留而不是 reserved：node 与 controller 独立滚动，窗口内必然新旧并存，
  // 而这个字段的「空」在 scheduler 侧不是降级，是「这台节点上一台沙箱都没有」，
  // 会触发批量删 binding。B1 退役 RenewNodeLease 时一并删掉它。
  repeated string sandbox_ids = 8 [deprecated = true];
  P2pEndpoint p2p_endpoint = 9;
  // node 报「我这台上活着的沙箱和它们各自的化身」。
  repeated SandboxRosterEntry roster = 10;
}

message SandboxRosterEntry {
  string sandbox_id = 1;
  // 小写 canonical UUID v7。空值 = 这条 roster 项不参与仲裁（见 scheduler 侧 §6）。
  string execution_id = 2;
}
```

scheduler 侧（`service.go:343-372` + `node_registry.go:255`）**一个函数收口**：

```go
// rosterFromHeartbeat 把两代字段收敛成一种形状，并让「用了旧字段」这件事可见。
func rosterFromHeartbeat(req *schedulerv1.HeartbeatRequest) ([]RosterEntry, bool /*legacy*/)
//   roster 非空            → 用 roster
//   roster 空 && sandbox_ids 非空 → 用 sandbox_ids，execution 全空（unknown），legacy=true
//   两者都空              → 真空 roster
```

`legacy=true` 时 `Warn` 一行 + `agentenv_scheduler_heartbeat_legacy_roster_total{node}` +1。
**这不是兼容代码的开脱，是让「集群里还有旧 node」在指标上可见**，
也是 gateway 翻 `enforce` 前必须为 0 的那条前置（R-4）。

#### S2.3 execution 缺失的 roster 项怎么处置 —— 与 gateway 措辞的一处刻意软化

gateway §4.1 写「🔴 必填。空值不等于『未知』，而是这条 roster 项**不可用于绑定仲裁**」。

**我按「保留为 unknown」实现，而不是丢弃**：

| 处置 | 后果 |
|---|---|
| 丢弃该项 | 该沙箱在这台节点上失去 binding **和** roster ⇒ 回落登记表 ⇒ 从未 pause 过的就是 404。**为了不给一条未受保护的路由，赔上一条能用的路由**，不划算 |
| **保留为 unknown（选）** | 可以在**无 incumbent 时**安装 binding；**不得顶掉**任何带 execution 的 incumbent。等价于 gateway 要的「不可用于仲裁」，但不丢路由 |

⇒ 语义与 gateway 一致，行为更保守。计数：
`agentenv_scheduler_binding_execution_total{decision="installed_unknown"|"rejected_unknown"}`。
方法论 §8.4（无声收窄要 log）由这两个 series 兑现。

#### S2.4 `rosterHolder` 也要改

`lookup.go:379-399` 的「最近上报者胜」换成：

```
优先按 execution 大者胜（两边都有 execution 时）
两边 execution 相同或都为空 → 退回今天的「最近上报者胜」
一有一无 → 有 execution 的胜
```

🟢 **保留「最近上报者胜」作为平手规则是刻意的**：它是一条正确的旧行为，
删掉会让「同一化身被两台节点上报」（别名/滚动更新期间的同一台机器）失去答案。

**不做的后果**：解析结果本身指向旧化身，闸 1/闸 2 加多少都白搭（gateway §7 的结论）。

---

### S3 — 冲突仲裁规则：UUID v7 字典序大者胜

**做/不做**：✅ **做**，附两条硬约束。

#### S3.1 归一化（缺陷三的修法）

- 在**入口**归一化为**小写 canonical**：`service.go` 的 Heartbeat / RecordAssignment 处理里
  加 `normalizeExecutionID(raw) (string, bool)` —— trim + 校验 36 位 canonical + `strings.ToLower`。
- 校验不通过 ⇒ 该 roster 项按 unknown 处理 + `Warn` + `…{reason="bad_uuid"}` 计数，
  **不失败整个心跳**（一条坏 roster 项不该让整台节点被判 UNHEALTHY）。
- 🟡 不改 `registry/store_postgres.go:1383-1402` 的 `isCanonicalUUID`（那是 A2/A3 的地界，
  且 PG 的 `uuid` 类型自己会归一）；scheduler 读路径自己归一即可。

#### S3.2 时钟偏移边界 —— 主 agent 要的那条论证

**结论：可用，边界是 `lease_ttl_floor`（默认 30s），有近三个数量级的余量。**

推导：v7 前 48 bit 是毫秒墙钟，跨节点比较只在**同一沙箱的两个化身同时被上报**时发生。
枚举能造出这种局面的路径：

| 路径 | 两个化身的铸造时刻相隔多久 | 会不会比较 |
|---|---|---|
| 正常跨节点 resume（`paused` → claim → mark_running） | 旧化身在新化身诞生**之前**已经死了（pause 先把 VM 停掉再传快照，`paused_coordinator.rs:284→:296`） | ❌ 不会同时出现在两个 roster 里 |
| **reclaim 接管**（唯一真正产生双活的路径） | 必须先「租约过期」：`lease_expires_at` 由 `RenewNodeLease` 刷新，TTL 默认 **90s**（`config.go:24 defaultSchedulerRegistryLeaseTTL`），下限 **30s**（`:28 defaultSchedulerRegistryLeaseTTLFloor`）；且 reclaim 还要求 deadline 也过期（双条件） | ✅ 会。但两个化身的铸造时刻**至少相隔 30s** |
| fork / 从模板创建 | 铸新 `sandbox_id`，不是同一台 | ❌ |

⇒ 要让字典序反转，**跨节点墙钟偏移必须超过 30 秒**。
NTP/chrony 同步的节点偏移在毫秒级；即使 NTP 完全失效，Linux TSC 漂移量级是 ppm，
积累到 30s 需要数月不校时。⇒ **规则安全，余量 ≈ 10⁵**。

🔴 **但余量不等于免检**：反向覆盖尝试必须 `Warn` + 计数
（`…{decision="rejected_older"}`），因为**一旦它非零，要么是时钟回拨、要么是我们的模型错了**，
两者都必须被人看到。这也正是主 agent 决策 5 里那句「对反向覆盖打 warn（时钟回拨信号）」。

#### S3.3 「登记表在被查询时是真相」怎么落地

热路径**不查表**（缺陷二的约束）。但 shadow reconcile 已经在周期性同时读表与 roster
（`reconcile.go:410` `s.nodes.RostersInCluster(...)`，默认 30s），
⇒ 🟢 **零新增 IO** 地加一个 series：

```
agentenv_scheduler_registry_execution_mismatch{node}
  节点 roster 报的 execution 与登记表该行 execution 不一致的沙箱数
```

它是**双活的直接信号**，也是唯一由权威表驱动的那个（比闸 1 的真阳性率有用得多，见 §2.2）。
本轮只做**观测**，不做动作（KillOrphan 是 B3）。

**不做的后果**：需要退回「冲突时查登记表」，而热路径不许查 ⇒ 实际是退回到达顺序仲裁 = 缺陷一未修。

---

### S4 — `LookupNodeResponse` 加 `execution_id` + `execution_authority`

**做/不做**：✅ **做**，字段与 gateway §4.1 提案一致。

```proto
message LookupNodeResponse {
  Node node = 1;
  SandboxLocation location = 2;
  string origin_node_id = 3;
  // 本次答案所指向的那个化身：node 那台机器上，中央认为当前该活着的化身。
  // 小写 canonical UUID v7。空 = 中央答不出来，与 execution_authority=UNKNOWN 等价。
  string execution_id = 4;
  // 调用方可以拿 execution_id 做多硬的事。只有 REGISTRY 一档允许用于拒绝。
  ExecutionAuthority execution_authority = 5;
}
```

`ExecutionAuthority` 枚举取值与语义见 §5。落点：`lookup.go:365-371` 的 `lookupResponse`
签名扩成 `lookupResponse(node, location, originNodeID, executionID, authority)`，
五个出口逐个显式传值 —— **不给默认参数**，让「新增一条出口忘了分类」变成编译错误而不是静默 UNKNOWN
（照 `lookupResult` 的封闭标签集写法，`lookup.go:17-53`）。

**不做的后果**：gateway 拿不到化身，A5 整体不存在。

---

### S5 — `resuming` 行的 `execution_id` 语义（E-A 已裁决）

**做/不做**：✅ **做（E-A 预分配）**，`resuming` 行报 `REGISTRY`（**不**降级 PENDING）。

**改哪里（写侧属 A2/A3，本文只对账 + 提一条新耦合）**

| 位置 | 改什么 |
|---|---|
| `scheduler.proto` `AcquireSandboxRequest` | 加 `string execution_id = 5`（认领者预分配） |
| `registry/store_postgres.go:585-604` `claimForResumeSQL` | `SET … execution_id = $5::uuid, execution_started_at = now()`，与 `claimed_by_node_id` **同一条语句** |
| `:609-627` `claimForResumeDurableOnlySQL` | 同上 |
| A2 §2.2 的 CHECK | `resuming` 从「必须为空」改成「必须非空」（E-A 分支，A2 文档已预留两写法） |
| `lookup.go:296-327` | `StateResuming` 分支照常回 `entry.ExecutionID`，authority=REGISTRY |

🔴 **E-A 引入一条新耦合，A1 必须知道，否则每次跨节点 resume 都会失败**：

`markRunningSQL` 的分支 ①（A3 设计 §3.3）要求 `execution_id = $6`，
而 `$6` 是**节点启动 VM 时用的那个化身**。E-A 下这个值**在 claim 时就定了**，
⇒ **节点必须使用 `AcquiredSandbox.entry.execution_id` 去启动 VM，不得自己再铸一个**。
- 传导路径：`AcquireSandboxResponse.claimed.entry`（`RegistryEntry`）必须带 `execution_id`（S7 的同批字段）；
- A1 的 `LaunchPlan::Resume` 变体上那个 `execution_id` 字段，在「controller 给了值」时**必须接受外部注入**，
  只在「本机自有 parked 行就地 resume（没走 claim）」时才自己铸。
- 🔴 **这条不做的后果不是降级，是硬故障**：claim 写 `E_pre`，node 用 `E_own` 启动，
  `mark_running` 分支 ① 谓词不匹配 ⇒ 每一次跨节点 resume 都拿不到 ADOPTED。

🔴 **第二条传导（给 node 设计）：拒绝必须只凭正面证据**。
`resuming` 窗口里 gateway 会带着 `E_pre` 把数据面流量打到认领者 B，而 B 的 VM 可能**还没起来**。
⇒ node 的比对必须是「**本机确有该沙箱的活化身，且它比 expect 旧**才拒」；
「本机没有这台沙箱」必须落回既有的 404 / auto-resume 路径。
否则 resume 窗口内每一个数据面请求都会被拒 —— 正是 gateway R-5 担心的那个体验最差的场景。

**不做的后果（退回 E-B）**：`resuming` 行 execution 恒空 ⇒ 该分支只能报 PENDING ⇒
resume 窗口内不设防（可接受），但 B3 必须给 `resuming` 行开 grace 特例（A2 设计 §6-U1 已列）。

---

### S6 — `RecordAssignmentRequest` 加可选 `execution_id`

**做/不做**：✅ **做**，但**把 gateway 标成「可降级」的那一半升级为不可降级**。

```proto
message RecordAssignmentRequest {
  string sandbox_id = 1;
  Node node = 2;
  // gateway 从 node 响应头拿到的化身。拿不到就留空 = unknown。
  string execution_id = 3;
}
```

- 🟢 **字段本身可降级**：gateway 在 create/fork 成功后写 binding（`server.go:505-518`），
  拿不到化身就留空。影响只是「新建后到第一次心跳」这段窗口 authority=UNKNOWN（≤ `report_ttl` = 30s），
  而这段窗口里沙箱是全新的、单化身，没东西可 fence。
  ⚠️ 补一条 gateway 没说的：**fork 也铸新 `sandbox_id`**（任务书 §9 第 2 条），
  所以「新建/fork 时不存在 incumbent」这条对两者都成立 ⇒ 空 execution 不会被仲裁挡住。
- 🔴 **「走同一条仲裁」不可降级**：`service.go:327` 今天直接 `s.store.Record(...)`。
  若 A5 只给 `ReconcileNode` 加仲裁而 `Record` 照旧无条件覆盖，
  **`RecordAssignment` 就成了绕过 S2 的后门** —— 一次落在旧节点上的 create 响应，
  能把一个更旧（或 unknown）的 binding 盖到一个带新 execution 的 incumbent 上。
  ⇒ `Record` 与 `ReconcileNode` **必须共用同一个仲裁函数**，且由同一份契约测试覆盖。

**不做的后果**：字段不做 ⇒ ≤30s 的 UNKNOWN 窗口（可接受）；**仲裁不做 ⇒ S2 白做**。

---

### S7 — `RegistrySandbox` proto 加 `execution_id`

**做/不做**：✅ **做**，但**字段由 A2/A3 那份 PR 加**（它的落地清单 §7 已列 `RegistryEntry` / `RegistrySandbox` 各加一个），
本文只消费，**避免两个 PR 各加一遍字段号打架**（§12 冲突点 3）。

字段号（现有最大号已核）：

| message | 现有最大字段号 | 新字段 |
|---|---|---|
| `RegistrySandbox`（`scheduler.proto:318-344`） | 12（`holder_node_id`） | `string execution_id = 13;` |
| `RegistryEntry`（`:405-430`） | 9（`updated_at_unix_micros`） | `string execution_id = 10;` |

🔴 **一处加、四处漏都会静默**（A2 设计 §2.9 已警告，本文再钉一次落点）：

| 落点 | 文件:行 | 漏了的症状 |
|---|---|---|
| 写模型列清单 | `registry/store_postgres.go:30-41` `entryColumns` | `GetSandboxes` / claim 返回的 entry 恒空 ⇒ **S5 的 node 复用链断**，跨节点 resume 全失败 |
| 读模型列清单 | `registry/postgres.go:31-41` `selectColumns` | `Get`/`List` 恒空 ⇒ **`lookup.go:296-327` 的 authority 恒 UNKNOWN**，A5 在登记表路径上失效 |
| 行转 proto（读） | `service.go:648-663` `registrySandboxToProto` | `/registry/sandboxes` 看不到化身 |
| 行转 proto（写服务） | `registry_service.go:709` `registryEntryToProto` | 同上 + S5 断链 |

⇒ 配一发**同时**断言四条路径的测试（§10.D `TestEveryReadPathCarriesTheExecution`），
只测一条的话另外三条是纯纸面。

**不做的后果**：运维在唯一能按行看到化身的端点上看不到它，双活时无处对账。

---

### S8 — `LookupNode` 的错误码语义不许变

**做/不做**：✅ **不改**，并加一发机械保证。

- `NotFound` 今天**只从 `lookupAbsent`（`lookup.go:349-363`）产生**，且被 warmup 门控。
  A5 新增的任何分支**不得**新增 `codes.NotFound` 返回点。
- 机械保证：`TestExecutionAxisAddsNoNewNotFound` —— 表驱动跑遍
  「binding 有/无 execution × roster 有/无 × 五种 state × authority 三档」，
  断言只有「登记表可读且无行 + 已 warm」这一种输入产生 `NotFound`。
  变异：给任何一条新分支加 `NotFound` ⇒ FAIL。
  对照组：复用既有的 `TestLookupAnswersNotFoundWhenTheRegistryIsReadableAndEmpty`
  （`lookup_test.go:463`）证明 `NotFound` **仍然能**被产生 —— 没有它，一个「把所有 NotFound 都删了」
  的变异会让上面那发假绿。

🔴 **顺手发现的一条真冲突，必须写下来**：
`gateway/internal/server.go:334-343` 的 `writeSchedulerError` **没有 `PermissionDenied` 分支**，
它会落 `default:` ⇒ **502 Bad Gateway**。
而 A3 设计 §3.5 新增的 `ErrExecutionFenced → codes.PermissionDenied` 走的是
`PausedRegistry` gRPC（node 直连 scheduler，**不经 gateway**），今天撞不上。
⇒ **本设计明令**：`LookupNode`（以及任何 `Scheduler` service 的方法）**不得返回 `PermissionDenied`**。
理由：gateway 设计 §6.2 已把 502 明确排除（「语义是上游坏了。上游没坏」），
而这条路径会把一个精确的 fencing 事实渲染成一个错误的诊断。
配一发 `TestSchedulerServiceNeverReturnsPermissionDenied`（穷举 `Scheduler` 的返回码集合）。

**不做的后果**：gateway 的 404 直通平台的「授权重建工作区」（gateway §6.1）⇒ 用户工作区蒸发。

---

## 4. proto 变更清单与发布顺序

### 4.1 逐字段

| message | 字段 | 类型 / 号 | 加法还是重画 | 兼容性 |
|---|---|---|---|---|
| `LookupNodeResponse` | `execution_id` | `string = 4` | 纯加 | 旧 gateway 忽略；新 gateway 见空 ⇒ UNKNOWN |
| `LookupNodeResponse` | `execution_authority` | `ExecutionAuthority = 5` | 纯加 | 旧 scheduler 不发 ⇒ 解码为 `UNSPECIFIED(0)`，语义等同 UNKNOWN（§5） |
| （新）`ExecutionAuthority` | 枚举 | 0/1/2/3 | 新增 | 见 §5 |
| `HeartbeatRequest` | `roster` | `repeated SandboxRosterEntry = 10` | 纯加 | 旧 node 不发 ⇒ 回落 `sandbox_ids` |
| `HeartbeatRequest` | `sandbox_ids` | `= 8` | 🔴 **保留 + `[deprecated = true]`**，**不 reserved** | 见 §S2.2；B1 那批再删 |
| （新）`SandboxRosterEntry` | `sandbox_id` / `execution_id` | `string = 1` / `= 2` | 新增 | `execution_id` 空 ⇒ unknown（§S2.3） |
| `RecordAssignmentRequest` | `execution_id` | `string = 3` | 纯加 | 旧 gateway 不发 ⇒ unknown |
| `AcquireSandboxRequest` | `execution_id` | `string = 5` | 纯加（E-A **已裁决 ⇒ 必加**）。🔴 **归属：由 A2/A3 那个 PR 加，本设计一个字都不加**（任务书 §11.1(e) 定稿 + `_design-phase3-scheduler.md` §3.1 已就地标注同一句）—— 写入它的是 `claimForResumeSQL`，在 `registry` 包内；本设计只消费 | 旧 node 不发 ⇒ claim 写不进 execution ⇒ **CHECK 会拒**（A2 §2.2）⇒ 必须与 A1 同批发布 |
| `RegistrySandbox` | `execution_id` | `string = 13` | 纯加，**由 A2/A3 PR 加** | 只读暴露 |
| `RegistryEntry` | `execution_id` | `string = 10` | 纯加，**由 A2/A3 PR 加** | S5 的 node 复用链靠它 |

🟢 **全部是 additive**（唯一的非加法是 `sandbox_ids` 加 `deprecated` 注记，不改号不改型）。

### 4.2 发布顺序（不可交换，比 A2/A3 那份多一条）

> 🔴 **本节只是「A5 这一项自己的依赖方向」。执行以
> [`_impl-plan-control-plane-phase3.md` §6](_impl-plan-control-plane-phase3.md#6-总发布-runbook-n5三份设计的顺序合成一条)
> 的 13 步线性 runbook 为准**（命名裁决 N5）——
> 它把本节与 node 设计 §3.2（A4 要 gateway 先）、A2/A3 设计 §5.1（A1/A3 要 node 先）合成了一条。
> 对应关系：本节第 1 步 = 总 runbook 步骤 3；第 2 步 = 步骤 4；第 3 步 = 步骤 5–6；第 4 步 = 步骤 8 + 10 + 11。
> 🔴 **2026-08-19 追认后的一处改动**：**node 全程只滚一次（步骤 3）** —— A5 的**接收端**（node 侧）随 A1 同镜像发布，
> 在 gateway 下发 expect 头之前是惰性的；原来夹在中间的"node 第二次滚"变成了步骤 9 的一次**配置热翻转**（那是 A4，不是 A5）。
> ⇒ 本节第 4 步不再与任何 node 滚动交替，它纯粹是 gateway 侧的两次开关翻转。

```
1. node 先发（A1）：铸 execution + 心跳发 roster + 保留 sandbox_ids
   ↑ 旧 scheduler 把 roster 当未知字段忽略，行为不变
2. scheduler 后发（A2 + A3 + 本设计），SCHEDULER_ROUTING_EXECUTION_ARBITRATION=observe 上线
   ↑ 看 legacy_roster / authority 分布两组指标
3. legacy_roster 归零（全集群 node 已带 A1） → 翻 enforce
4. gateway 最后发（A5 gateway 侧），先 observe 再 enforce
   ↑ node 侧的接收端从第 1 步就在位（惰性），这里不需要再滚 node
5. 下一批（B1）：删 HeartbeatRequest.sandbox_ids
```

🔴 **顺序反了的后果**（`sandbox_ids` 若被 reserved）：见 §S2.2 —— 所有从未 pause 过的沙箱
在整个滚动窗口里数据面 404。这就是为什么并存那一步不能省。

---

## 5. `execution_authority` 枚举：取值、语义、以及为什么 PLACED/PINNED 必须 PENDING

### 5.1 枚举

```proto
// ExecutionAuthority 说明 execution_id 的来源强度。
// 🔴 它不是「置信度」，是「允许被用来拒绝流量吗」。只有 REGISTRY 一档允许。
enum ExecutionAuthority {
  // 更旧的 scheduler。等同 UNKNOWN —— 这条等价关系是硬的：任何把 0 单独处置的调用方
  // 都会在滚动升级窗口里对同一件事给出两种行为。
  EXECUTION_AUTHORITY_UNSPECIFIED = 0;
  // 中央答不出当前化身：沙箱从未进过登记表（§7），或 binding/roster 是 A1 之前写下的。
  // 调用方必须放行，并把这次放行计成「未受保护」。
  EXECUTION_AUTHORITY_UNKNOWN = 1;
  // execution_id 来自权威来源，且它现在就应当活在 node 字段那台机器上。可用于拒绝。
  EXECUTION_AUTHORITY_REGISTRY = 2;
  // node 即将铸造一个新化身（PLACED / PINNED）。execution_id 一律留空。
  EXECUTION_AUTHORITY_PENDING = 3;
}
```

### 5.2 五个出口的归档表

| `lookupNode` 出口 | location | execution 来源 | authority | 说明 |
|---|---|---|---|---|
| binding 命中（`:151-160`） | BOUND | `binding.ExecutionID` | 非空⇒`REGISTRY`；空⇒`UNKNOWN` | 热路径，99% 请求 |
| roster 命中（`:166-177`） | BOUND | 该节点 roster 项 | 同上 | binding TTL 与心跳之间的缝 |
| `paused` → 置放（`:230-252`） | PLACED | —— | 🔴 **`PENDING`，execution 留空** | 见下 |
| `publishing`/`local_only` → 钉 origin（`:254-294`） | PINNED | —— | 🔴 **`PENDING`，execution 留空** | 见下 |
| `running`/`resuming` → holder（`:296-327`） | BOUND | `entry.ExecutionID` | 非空⇒`REGISTRY`；空⇒`UNKNOWN` | E-A 下 `resuming` 也非空 |

### 5.3 🔴 复核：PLACED / PINNED 为什么**必须**报 PENDING（比 gateway 的理由更硬）

gateway §1.2 的理由：node 的 `/proxy` 对 paused 沙箱会自动唤醒并铸新化身，
拿旧化身 expect 会把每一次数据面触发的自动唤醒拒死。**成立。**
我复核时找到**两条更硬的**，都直接来自 A2 的 schema：

1. **`paused` / `local_only` 行的 `execution_id` 按 CHECK 恒为 NULL**（A2 设计 §2.2 的状态表）。
   ⇒ PLACED 与 PINNED 的一半，`execution_id` 想报也报不出来。
   如果这时候报 `REGISTRY` + 空值，就成了「权威地说没有化身」——
   而 gateway 的 `decideFencing` 对 `authority == REGISTRY` 是要下发 expect 的，
   下发一个空 expect 等于让 node 去比对一个空串。**必须由 authority 挡住，不能靠「值恰好是空」挡。**
2. 🔴 **PINNED 的另一半 `publishing` 行 `execution_id` 恰恰是非空的**，而那个值指向的是
   **一台已经停机的 VM**：pause 的三步是先停 VM、再传快照、最后翻牌
   （`paused_coordinator.rs:284 begin_pause → :296 publish_captured → :305 complete_pause`），
   `begin_pause` 之后 VM 就不在了。
   ⇒ 同一个 PINNED 分支里，`local_only` 给空值、`publishing` 给一个**指向死 VM 的非空值**。
   把 authority 交给「值空不空」去推断，就会让同一条 location 给出两种权威度，
   而其中一种（`publishing`）**必然**让数据面自动唤醒 100% 被拒。
   **只有在分支里显式统一报 `PENDING` 才自洽。**

⇒ 实现要求：`lookup.go:230-294` 两个分支**显式传 `("", PENDING)`**，不允许「让 entry 的值自然流下去」。
配一发对照测试（§10.B），变异：让 PINNED 把 `entry.ExecutionID` 流下去 ⇒ FAIL。

**不满足的后果**：数据面触发的自动唤醒全线 409（gateway S4 原话），
而自动唤醒是 aenv 预览链路的常规路径（`src/api/proxy.rs:105-107` `PROXY_AUTO_RESUME_TIMEOUT`）。

---

## 6. 仲裁规则：一个函数，两个实现，一份契约

### 6.1 规则（写在一处，两个存储各实现一遍，由同一份契约测试驱动）

设 incumbent = 存储里现有记录（可能不存在/已过期），challenger = 本次要写入的。

| # | 条件 | 动作 | metric `decision` |
|---|---|---|---|
| 1 | 无 incumbent 或已过期 | 安装 | `installed`（challenger 有 exec）/ `installed_unknown`（没有） |
| 2 | challenger.exec == incumbent.exec（含都为空 + 同节点） | 覆盖（刷 TTL / endpoint） | `refreshed` |
| 3 | challenger.exec 非空，incumbent.exec 为空 | 覆盖 | `installed`（从 unknown 升级） |
| 4 | challenger.exec 为空，incumbent.exec 非空 | 🔴 **拒绝** | `rejected_unknown` |
| 5 | 两者非空且 challenger > incumbent | 覆盖 | `superseded` |
| 6 | 两者非空且 challenger < incumbent | 🔴 **拒绝** + `Warn` | `rejected_older` |

比较：**小写 canonical 字符串字典序**（§S3.1 保证形态；等价于 v7 时间序，§S3.2 保证边界）。

🔴 **拒绝时必须彻底跳过**：不写 binding、**也不写反向索引**（`nodeBinding` / Redis 的 `node_key` 集合）。
否则被拒的节点会在自己的反向索引里留下一条它并不拥有的沙箱，
下一次它上报空 roster 时会把**别人的** binding 删掉。

### 6.2 内存实现

`upsertLockedWithExpiry`（`store.go:122-135`）前置一个 `func arbitrate(incumbent bindingRecord, ok bool, challenger Binding, now time.Time) (accept bool, decision string)`，
全部在 `s.mu` 内。`ReconcileNode` 的删除侧（`:104-114`）不变。

### 6.3 Redis 实现 —— 仲裁进 Lua

`redisLuaHelpers` 的 `parse_node_id` 扩成 `parse_binding(raw) → node_id, execution_id`；
两段脚本各插一段：

```lua
local function accepts(raw, challenger_exec)
  if not raw then return true, "installed" end
  local _, incumbent_exec = parse_binding(raw)
  if not incumbent_exec or incumbent_exec == "" then return true, "installed" end
  if not challenger_exec or challenger_exec == "" then return false, "rejected_unknown" end
  if challenger_exec == incumbent_exec then return true, "refreshed" end
  if challenger_exec > incumbent_exec then return true, "superseded" end
  return false, "rejected_older"
end
```

- Redis 的 `>` 对字符串是字节序比较，与 Go 的 `>` 一致。✅
- **过期即不存在**：`GET` 对过期 key 返回 nil，落分支 1。✅
- **决策要回给 Go 侧**：脚本改成返回一个 `{sandbox_id, decision}` 数组，Go 侧据此打指标与 Warn。
  🔴 不返回决策 = 无声收窄（方法论 §8.4），而它吞掉的恰好是「集群里有双活」这个最值得知道的事实。

🟢 **零额外往返**：脚本本来就在 `GET`（`:285`/`:294`）。

### 6.4 `off` / `observe` 怎么实现（抄 A2/A3 的「两条常量二选一」）

- Redis：**两份脚本常量**（`redisReconcileNodeScriptSource` / `…FencedSource`），
  构造 `RedisBindingStore` 时选定，**不在一份脚本里塞 `if flag`**。
  理由与 A3 设计 §5.2 逐字相同：塞 `if` 会让「关掉时」和「开着时」是同一条脚本，
  那条脚本就永远无法被单独测试。
- 内存：同理，两个 `arbitrate` 函数值，构造时选定。
- `observe`：跑**开着的那条**求出 `decision`，**但按关着的行为写入**（即恒覆盖），只打指标与 Warn。
  ⇒ 三态各自是一条独立可测的代码路径。

---

## 7. 复核：「不给 running 沙箱建行」的覆盖缺口 —— 论证成立，且可以加强

主 agent 决策 4：`BeginPause` 保持唯一建行者，`MarkRunning` 仍 no-insert。
gateway §1.2 据此论证「无行 ⇒ 物理上单化身 ⇒ 没东西可 fence」。**我复核的结论：成立，且能加强。**

### 7.1 建行者确实只有一个

- `MarkRunning` **绝不 INSERT**：`registry/store.go:91-110` 的接口注释与
  `store_postgres.go:842-851` 的 0 行分类都写死了（`MarkRunningUntracked` 就是在说这件事）。
- 其余九个写点全是 UPDATE/DELETE（A2 设计 §1.2 的十点表，唯一 INSERT 在 `beginPauseSQL:388,392`）。

### 7.2 加强版不变式：**无行 ⇒ 集群没有任何机制造得出第二个化身**

gateway 说的是「目前只有一个」。更强的说法是「**造不出第二个**」，逐条穷举第二份的来源：

| 造第二份的路径 | 前提 | 无行时成立吗 |
|---|---|---|
| `ClaimForResume`（跨节点接管） | SQL 谓词 `snapshot_id IS NOT NULL` **且** 有行（`store_postgres.go:596-600`） | ❌ 无行 ⇒ 0 行更新 |
| `ReclaimExpiredHoldings` / `ReleaseNodeHoldings`（夺权） | 作用于**行**（`:995-1004` / `:1107-1114`） | ❌ 无行 ⇒ 不作用 |
| 节点本地 resume（绕开集群，`service.rs:1541`/`:1600`） | 本地 metadata 是 `Paused` | ❌ 进入 `Paused` 必走 `pause_sandbox_inner → begin_pause`，而 `begin_pause` 失败即 `return None`（`paused_coordinator.rs:288-294`）⇒ **「本地 Paused 但集群无行」不可达** |
| 数据面 auto-resume（`proxy.rs:686→:800`） | 同上（沙箱得先是 paused） | ❌ 同上 |
| fork / 从模板创建 | 铸**新 `sandbox_id`**（`orchestrator/service.rs:704` 是 `created_at` 唯一赋值点；任务书 §9 第 2 条） | ✅ 但那是另一台沙箱，不是同 id 双活 |

⇒ **不变式：只有登记表行能授权在另一台机器上重建一台沙箱。无行 ⇒ 无重建路径 ⇒ 无双活。**

### 7.3 写成显式的「已知覆盖缺口 + 为什么安全」

> 🟢 **已知缺口（可接受，必须可测量）**：从未 pause 过的沙箱在 `paused_sandboxes` 里无行，
> 其 binding/roster 的 execution 虽然存在（A1 之后 roster 总是带），但**登记表对它没有权威判断**。
> 这类答案在本设计里仍然报 `REGISTRY`（因为 binding 的 execution 是真的），
> 但它受保护的强度实际等于「节点自报」。
> **为什么安全**：§7.2 的不变式 —— 无行 ⇒ 集群造不出第二个化身 ⇒ 没有东西需要 fence。
> **大小怎么量**：gateway 的 `unfenced_no_authority` 计的是 authority 缺失，量不到这一类；
> 用本文 §9 的 `agentenv_scheduler_registry_untracked`（**已存在**，`metrics.go:83-89`，
> 语义逐字就是「节点报的、登记表没有行的沙箱」）作为这一缺口的直接大小。
> 🟢 顺带：这条已有指标的 Help 文本已经写着「NOT an orphan count」，正是这个缺口的另一半。

**机械保证**：`TestOnlyBeginPauseEverInsertsARow`（需 PG）——
对一个从未 pause 过的 sandbox 依次跑 `MarkRunning` / `CompletePause` / `MarkLocalOnly` /
`ClaimForResume` / `ReleaseClaim` / `RenewNodeLease` / `Remove`，断言 `paused_sandboxes` **恒零行**；
再跑一次 `BeginPause` 断言变成 1 行（**探针自证的对照面**：证明这发测试的夹具确实能建出行）。
变异：给 `markRunningSQL` 加 INSERT 分支 ⇒ FAIL。

---

## 8. D6 与 D9：本设计要替另外两份复述 / 出的东西

### 8.1 🔴 D6 复述（给 A4 实现者，务必看到）

> **A4 的 node 收窄必须放行 gateway → node 的 `GET /sandboxes`。**
> `handleClusterList`（`gateway/internal/cluster_list.go:72-119`）先 `ListNodes`（`:74`），
> 再对**每一台** node 扇出 `fetchClusterList`（`:121`），**任一台失败整体 502**（`:94`）。
> ⇒ 收窄若把这条只读 GET 一起挡掉，`GET /sandboxes` 与 `GET /v2/sandboxes` **整体不可用**，
> 而不是「少一台节点的数据」。
> 这条对 A4 是**必须放行清单**的一项，与「收 `POST /nodes/{id}`」（任务书 §9 第 16 条）同批。

### 8.2 D9：`GET /v2/sandboxes` 非确定性排序 —— scheduler 侧的贡献与**一条禁令**

**scheduler 侧要改的：零。** 这条路径完全不经 scheduler 的沙箱语义：
`handleClusterList` 只调 `ListNodes`（`cluster_list.go:74`），去重与排序全在 gateway
（`sortListedSandboxes:234-241` / `dedupListedSandboxes:243-260`，那条 TODO 在 `:248-249`）。
修复手段（按 ExecutionID 取大者）落在 gateway + node 的响应体上。

🔴 **但要留一条禁令，因为它是一个极容易被"顺手优化"引入的缺陷**：

> **不许把 `GET /v2/sandboxes` 的去重改成「查 scheduler 的登记表来决定谁是权威」。**
> 登记表对**从未 pause 过的沙箱没有行**（§7）。用登记表当去重/过滤依据，
> 会把这些沙箱**从集群列表里整体抹掉** —— 而它们恰恰是集群里最常见的一类（刚创建、还没暂停过）。
> 症状是「列表少了一半，没有任何报错」。
> ⇒ 去重的权威只能是**行内自带的 ExecutionID**，不能是外部一张表。

scheduler 侧唯一相关的产出是 **S7**：`RegistrySandbox.execution_id` 让
`GET /registry/sandboxes` 能按行看到化身，作为双活时的对账面。

---

## 9. 指标与日志（沿用 `agentenv_scheduler_*` 前缀与封闭标签集写法）

| 指标 | 标签 | 用途 |
|---|---|---|
| `agentenv_scheduler_binding_execution_total` | `decision` = `installed` / `installed_unknown` / `refreshed` / `superseded` / `rejected_older` / `rejected_unknown`；`source` = `heartbeat` / `assignment` | 🔴 **缺陷一是否真的修好，只看这一个**。健康集群上 `rejected_older` 应恒 0；非 0 = 时钟回拨或旧化身在抢 |
| `agentenv_scheduler_lookup_execution_authority_total` | `authority` = `registry` / `pending` / `unknown` | 覆盖率。**与 gateway 的 `unfenced_no_authority` 逐条对账**（跨服务互证，§2.3） |
| `agentenv_scheduler_heartbeat_legacy_roster_total` | `node` | 还有多少台节点没带 A1。🔴 **gateway 翻 `enforce` 的前置是它归零**（gateway R-4） |
| `agentenv_scheduler_heartbeat_roster_dropped_total` | `reason` = `no_execution` / `bad_uuid` | 无声收窄可见（方法论 §8.4） |
| `agentenv_scheduler_registry_execution_mismatch` | `node` | ✅ **裁决 A5-U5：本轮做**。🟢 零新增 IO（复用 30s shadow reconcile，`reconcile.go:410` 已同时读表与 roster）。**双活的直接信号**，比闸 1 的真阳性率有用得多（闸 1 真阳性≈0，见 §2.2 / A5-U3）|
| `agentenv_scheduler_routing_execution_arbitration_enabled` | —— | 常驻 gauge，`off`=0 / `observe`=1 / `enforce`=2。照 A3 §5.2 的「关掉时必须是刺耳的」 |

日志（都带 `sandbox_id` / 双方 `node_id` + `execution_id`）：

- `Warn "scheduler binding refused an older execution"` —— 规则 6
- `Warn "scheduler binding refused a challenger without an execution"` —— 规则 4
- `Warn "scheduler heartbeat used the legacy sandbox_ids roster"` —— 每节点限频

---

## 10. 测试计划与变异验证

### 10.0 跑法（三条硬要求一条都不能少，外加本设计新增的第四条）

**规范入口（首选）**：

```bash
make -C /home/debian/.orbitmux/projects/pj-8v5-AE/worktrees/sandbox-all/apps/AgentENV/services test-with-postgres
```

**手跑单发**（例：HA 那一发）：

```bash
cd /home/debian/.orbitmux/projects/pj-8v5-AE/worktrees/sandbox-all/apps/AgentENV/services
GOWORK=off \
SCHEDULER_REGISTRY_TEST_DSN='postgres://postgres:verify@127.0.0.1:15499/aenv_registry?sslmode=disable' \
SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
SCHEDULER_REDIS_TEST_REQUIRED=1 \
REDIS_SERVER_BIN="$(command -v redis-server)" \
  go test -count=1 -run TestQueryOnlyReplicaAnswersWithTheExecutionOverRedis ./scheduler/internal/
```

```bash
# 仲裁契约（两个实现各跑一遍）
cd /home/debian/.orbitmux/projects/pj-8v5-AE/worktrees/sandbox-all/apps/AgentENV/services
GOWORK=off \
SCHEDULER_REGISTRY_TEST_DSN='postgres://postgres:verify@127.0.0.1:15499/aenv_registry?sslmode=disable' \
SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
SCHEDULER_REDIS_TEST_REQUIRED=1 \
REDIS_SERVER_BIN="$(command -v redis-server)" \
  go test -count=1 -run 'TestBindingStore.*Execution' ./scheduler/internal/
```

```bash
# 登记表侧（S7 四条读路径 + §7 的建行不变式）
cd /home/debian/.orbitmux/projects/pj-8v5-AE/worktrees/sandbox-all/apps/AgentENV/services
GOWORK=off \
SCHEDULER_REGISTRY_TEST_DSN='postgres://postgres:verify@127.0.0.1:15499/aenv_registry?sslmode=disable' \
SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
SCHEDULER_REDIS_TEST_REQUIRED=1 \
  go test -count=1 -run 'TestEveryReadPathCarriesTheExecution|TestOnlyBeginPauseEverInsertsARow' ./scheduler/internal/registry/
```

| 变量 | 为什么必须带 |
|---|---|
| `GOWORK=off` | 父 worktree 的 `go.work` 未列入 `apps/AgentENV/services`，不设直接报 `directory prefix . does not contain modules listed in go.work` |
| `SCHEDULER_REGISTRY_TEST_DSN` | 不给 ⇒ 125 个 SQL 用例静默 skip（A3 设计 §4.0 实测） |
| `SCHEDULER_REGISTRY_TEST_REQUIRED=1` | 把 skip 变成 `t.Fatal`。**对 `./scheduler/internal/` 这个包没作用也照带** —— 同一条命令行会被复制去跑 `./scheduler/internal/registry/`，少带一次就是一次假绿 |
| 🔴 `SCHEDULER_REDIS_TEST_REQUIRED=1` + `REDIS_SERVER_BIN` | **本设计新增的硬要求**。`redis_store_test.go:156-165` 今天在找不到 `redis-server` 时 `t.Skip` ⇒ **HA 那一发会静默消失**，而它正是唯一能抓住 S1 失效的用例。`make test-with-postgres` 在 make 层已有 `command -v redis-server \|\| exit 1`（`services/Makefile:42-44`），但**手跑单发时没人替你查**。⇒ ✅ **裁决 A5-U1：采纳** —— 新增 `SCHEDULER_REDIS_TEST_REQUIRED`，与 `SCHEDULER_REGISTRY_TEST_REQUIRED` **同形**，并写进 `services/Makefile` 的 `test-with-postgres`（§13-U1、§14）|

夹具风格沿用现有两套，**不另造**：登记表侧用 `contractSetup` / `contractSchemaName(t)`
（每用例独立 schema + 独立 cluster id，`contract_test.go:134-170`）；
Redis 侧用 `startRedisServerForTest`（`redis_store_test.go:156-223`，每用例独立端口的 throwaway 进程）。

---

### 10.A 仲裁契约（一份表驱动，两个实现各跑一遍）

🔴 **必须写成 `bindingStoreContract(t, factory func(ttl time.Duration) BindingStore)`，
用 `t.Run("in-memory")` / `t.Run("redis")` 各跑一遍。**
理由是 §1.2 的实证：现有 query-only 测试全用 InMemory，Redis 路径零覆盖；
把契约写成一份是唯一能让「只改一个实现」当场变红的结构。

| 测试 | 断言 | 🔴 变异（必须 FAIL） |
|---|---|---|
| `TestBindingStoreKeepsTheNewerExecution` | A 报 `(X,E1)`、B 报 `(X,E2>E1)` ⇒ 读回 B。**两种到达顺序都测** | 去掉规则 5/6 ⇒ 后到者胜 ⇒ 顺序 (B,A) 那半 FAIL |
| 🔴 `TestBindingStoreRefusesToGoBackToAnOlderExecution` | **缺陷一的钉子**：binding 是 `(B,E2)`，A 的心跳报 `(X,E1)` ⇒ 读回仍是 B；反向索引里 A 名下**不含** X | 删掉规则 6 ⇒ FAIL |
| 🟢 `TestBindingStoreAcceptsTheSameNodeReportingANewerExecution` | **探针自证的对照面**：binding 是 `(A,E1)`，A 再报 `(X,E2>E1)` ⇒ 读回 `(A,E2)` | 没有它，一个「一律拒绝覆盖」的实现能让上面那发全绿 |
| `TestBindingStoreTreatsAMissingExecutionAsUnknown` | 空 execution ⇒ 无 incumbent 时安装；有带 execution 的 incumbent 时**被拒** | 规则 4 改成放行 ⇒ FAIL |
| 🔴 `TestBindingStoreNormalisesExecutionCase` | 写入 `7F2A…`（大写）⇒ 存的是小写；随后 `7f2b…` 能覆盖它 | 去掉归一化 ⇒ `"7f2b…" > "7F2A…"` 恰好还成立，**所以对照要反过来构造**：incumbent 大写 `7F2B…`、challenger 小写 `7f2a…`（真值更旧）⇒ 无归一化时 `"7f2a…" > "7F2B…"` 判为更新 ⇒ 旧化身赢 ⇒ FAIL |
| 🔴 `TestRedisReconcileTakesOneRoundTrip`（Redis 专发） | 用 `redis.Hook` 统计一次 `ReconcileNode` 发出的命令：只有一条 `EVAL`/`EVALSHA`，**零 `GET`** | 把仲裁挪到 Go 侧（先 `Get` 再 `Record`）⇒ 出现 `GET` ⇒ FAIL。**确定性，无 sleep、无并发概率** |
| `TestRecordAssignmentGoesThroughTheSameArbitration` | binding 是 `(B,E2)`，`RecordAssignment(X, A, E1)` ⇒ 不覆盖 | `service.go:327` 绕过仲裁直接 `Record` ⇒ FAIL（S6 那条后门） |

### 10.B lookup 与 authority（不需要 PG / Redis，`stubRegistryReader` 即可）

| 测试 | 断言 | 🔴 变异 |
|---|---|---|
| `TestLookupCarriesTheExecutionFromTheBinding` | authority=`REGISTRY`，execution=binding 的值 | `lookupResponse` 不传 ⇒ FAIL |
| `TestLookupReportsUnknownForABindingWithoutAnExecution` | authority=`UNKNOWN`，execution 空 | 把空值也报 REGISTRY ⇒ FAIL |
| 🔴 `TestPlacedAndPinnedAlwaysReportPending` | `paused`（PLACED）、`local_only` 与 **`publishing`**（PINNED）三种输入 ⇒ authority 恒 `PENDING` 且 execution 恒空。**`publishing` 那条行上带一个非空 execution**（§5.3 的硬点） | 让 PINNED 把 `entry.ExecutionID` 流下去 ⇒ `publishing` 那条 FAIL |
| 🟢 `TestRunningRowReportsRegistry` | **对照面**：`running` 行 ⇒ `REGISTRY` + 行上的 execution | 没有它，「一律 PENDING」的实现能让上面那发全绿 |
| `TestResumingRowReportsRegistryUnderPreallocation` | E-A：`resuming` 行带 execution ⇒ `REGISTRY` | 改报 PENDING ⇒ FAIL（若主 agent 改判 E-B，本发连同 S5 一并改） |
| `TestRosterHolderPrefersTheNewerExecution` | 两台节点都报 X，**先上报的那台 execution 更大** ⇒ 选它（钉死不是「最近上报者胜」） | 保留旧 `lastSeen.After` ⇒ FAIL |
| 🟢 `TestRosterHolderStillPrefersTheFresherReportOnATie` | **对照面**：两边 execution 相同/都空 ⇒ 退回最近上报者 | 证明 §S2.4 没把旧行为整条删掉 |
| `TestExecutionAxisAddsNoNewNotFound`（S8） | 表驱动穷举，只有「registry 可读且无行 + warm」产生 `NotFound` | 任一新分支返回 `NotFound` ⇒ FAIL |
| 🟢 `TestLookupAnswersNotFoundWhenTheRegistryIsReadableAndEmpty`（**已存在**，`lookup_test.go:463`） | **反向对照**：`NotFound` 仍然能被产生 | 没有它，「把所有 NotFound 都删了」的变异让上面那发假绿 |
| `TestSchedulerServiceNeverReturnsPermissionDenied`（S8 附） | 穷举 `Scheduler` service 的错误码集合，`PermissionDenied` 不在其中 | 给 `LookupNode` 加一条 `PermissionDenied` ⇒ FAIL（它在 gateway 落 502） |

### 10.C 🔴 HA / query-only 专发（S1 的失效只在这里暴露）

```
TestQueryOnlyReplicaAnswersWithTheExecutionOverRedis

装配（一个进程内，但形状与 HA 一致）：
  store := NewRedisBindingStore(startRedisServerForTest(t), 30s)     // 🔴 必须 Redis
  primary := NewService(logger, nodeRegistry, strategy, store, WithPausedRegistry(reader, …))
  replica := NewQueryOnlyService(logger, store, WithQueryOnlyPausedRegistry(reader))

① 🟢 探针自证（必然失败的对照输入，必须先跑）
   对一个「只有登记表行、没有 binding」的沙箱在 replica 上 LookupNode
   ⇒ 断言 codes.Unavailable（"this scheduler replica cannot place sandboxes"，lookup.go:220-228）
   ⇒ 这一步证明本用例确实跑在**没有 placer 的副本**上。
      若有人把它写成 primary，这里会拿到一个成功答案而不是 Unavailable，探针当场暴露。
      （本轮翻过一次车的正是这个：grace 期"拒绝接管"的探针用了一行任何相位都认领不了的
        合成行，409 看着像被拒、实则毫无分辨力。）

② primary.Heartbeat(node=A, roster=[(X, E1)])

③ replica.LookupNode(X)
   ⇒ node=A，execution=E1，authority=REGISTRY

④ primary.Heartbeat(node=B, roster=[(X, E2>E1)])
   replica.LookupNode(X) ⇒ node=B，execution=E2

⑤ primary.Heartbeat(node=A, roster=[(X, E1)])        // 旧化身回来抢
   replica.LookupNode(X) ⇒ **仍然** node=B，execution=E2
```

| 🔴 变异 | 必须 FAIL 的步骤 |
|---|---|
| **M-HA**：只给 InMemory 加 execution，Redis 的 JSON `redisBindingRecord` 与 Lua 不动 | ③ FAIL（execution 空、authority=UNKNOWN）。§10.A/§10.B 的绝大多数用例仍会全绿 —— **这就是「本地单 scheduler 测不出」的那个失效** |
| **M-HA2**：Redis 存了 execution，但 Lua 不仲裁 | ⑤ FAIL（binding 被抢回 A） |
| **M-HA3**：把仲裁写在 Go 侧 | ⑤ 大概率仍绿 ⇒ 由 §10.A 的 `TestRedisReconcileTakesOneRoundTrip` 确定性抓住 |

**第二发（跨副本）**：`TestTwoPrimariesSharingOneRedisConvergeOnTheNewerExecution` ——
两个 `Service` 实例共享同一 Redis（模拟滚动升级中的新旧 controller 进程），
A 与 B 的心跳交替灌入，断言 replica 读到的恒是较大 execution。
变异：仲裁缺失 ⇒ 最终值取决于最后一次写 ⇒ FAIL。

### 10.D 登记表读路径与建行不变式（需 PG）

| 测试 | 断言 | 🔴 变异 |
|---|---|---|
| `TestEveryReadPathCarriesTheExecution` | **同一行**上，四条路径全部带 execution：`Get` / `List`（`postgres.go` 读模型）、`GetSandboxes`（`entryColumns` 写模型）、`ListRegistrySandboxes`（`registrySandboxToProto`） | 只在 `entryColumns` 加、忘了 `selectColumns`（或反过来）⇒ 某一条恒空 ⇒ FAIL |
| 🔴 `TestOnlyBeginPauseEverInsertsARow`（§7） | 从未 pause 的 sandbox 上跑遍其余七个操作 ⇒ 表恒零行；再跑 `BeginPause` ⇒ 1 行（**对照面**） | 给 `markRunningSQL` 加 INSERT 分支 ⇒ FAIL |

### 10.E 回退

| 测试 | 断言 | 🔴 变异 |
|---|---|---|
| `TestExecutionArbitrationOffMatchesLegacyBehaviour` | `off` ⇒ ①旧化身的心跳**确实**抢回 binding（= 今天的行为，**这条正向断言是探针的分辨力来源**）；②`LookupNodeResponse` 的两个新字段为零值；③新指标零增长 | `off` 仍仲裁 ⇒ ① FAIL |
| `TestExecutionArbitrationObserveKeepsRoutingButCounts` | `observe` ⇒ 路由结果与 `off` 逐字节一致，但 `…{decision="rejected_older"}` +1、Warn 出现 | `observe` 也仲裁 ⇒ 路由与 `off` 不一致 ⇒ FAIL |
| `TestLegacyRosterFallbackIsCountedNotSilent` | 只发 `sandbox_ids` 的心跳 ⇒ roster 生效（binding 不被删）**且** `legacy_roster_total` +1 | 去掉回落 ⇒ binding 被删 ⇒ FAIL；去掉计数 ⇒ 计数断言 FAIL |

### 10.F 变异清单汇总

| # | 变异 | 必须 FAIL |
|---|---|---|
| M1 | `ReconcileNode` 退回无条件覆盖（内存） | `TestBindingStoreRefusesToGoBackToAnOlderExecution`(in-memory) |
| M2 | 同上（Redis Lua） | 同上(redis)、`TestQueryOnlyReplicaAnswers…` ⑤ |
| M3 | 只给 InMemory 加 execution | 🔴 `TestQueryOnlyReplicaAnswers…` ③ |
| M4 | 仲裁挪到 Go 侧 | `TestRedisReconcileTakesOneRoundTrip` |
| M5 | 去掉大小写归一 | `TestBindingStoreNormalisesExecutionCase` |
| M6 | `rosterHolder` 保留「最近上报者胜」 | `TestRosterHolderPrefersTheNewerExecution` |
| M7 | PLACED/PINNED 报 REGISTRY | 🔴 `TestPlacedAndPinnedAlwaysReportPending`（`publishing` 那条） |
| M8 | 空 execution 可顶掉带 execution 的 incumbent | `TestBindingStoreTreatsAMissingExecutionAsUnknown` |
| M9 | `RecordAssignment` 绕过仲裁 | `TestRecordAssignmentGoesThroughTheSameArbitration` |
| M10 | `sandbox_ids` 直接 `reserved 8` | `TestLegacyRosterFallbackIsCountedNotSilent` |
| M11 | `off` 仍仲裁 | `TestExecutionArbitrationOffMatchesLegacyBehaviour` |
| M12 | `selectColumns` / `entryColumns` 漏一份 | `TestEveryReadPathCarriesTheExecution` |
| M13 | `MarkRunning` 开始建行 | `TestOnlyBeginPauseEverInsertsARow` |
| M14 | `LookupNode` 返回 `PermissionDenied` | `TestSchedulerServiceNeverReturnsPermissionDenied` |
| M15 | 任一新分支返回 `NotFound` | `TestExecutionAxisAddsNoNewNotFound` |

🔴 **M3 是清单里唯一一条「变异后果不是本地红、而是 HA 生产静默失效」的**，
所以它对应的用例必须是 §10.C 那一发，且必须带 `SCHEDULER_REDIS_TEST_REQUIRED=1`。

---

## 11. 回退方案（配置级）

```
scheduler.routing.execution_arbitration = "off" | "observe" | "enforce"   # 默认 enforce
SCHEDULER_ROUTING_EXECUTION_ARBITRATION=observe
```

| 取值 | 行为 |
|---|---|
| `off` | 🔴 **完整回退**：仲裁不生效（恒覆盖，= 今天）、`LookupNodeResponse` 的两个新字段留零值、不打新指标。**必须是仲裁函数选择点的一次性构造决策**（§6.4 的「两条常量二选一」），不是散在 5 个调用点的 `if` |
| `observe` | 求出仲裁决策 **但按 `off` 的行为写入**；打全量指标 + Warn。**发布时先跑一轮**（主 agent 决策 7） |
| `enforce` | 默认。仲裁生效 |

**为什么不复用 A2/A3 的 `scheduler.registry.write_fencing`**（§12 冲突点 2）：
那个开关关的是**写路径**（两条 SQL 的 WHERE），归属 `scheduler.registry.*`；
本开关关的是**读路径**（binding 仲裁 + lookup 答案形状），归属 `scheduler.*`。
两者必须能**分别**关：关掉写路径 fencing 不应该让路由退回「最近上报者胜」，反之亦然；
一个共用开关会让一次止血动作顺手关掉另一半，而那一半的失效是无声的。

**落点**：`shared/config/config.go` 的 `SchedulerConfig`（`:238` 一带）+ 它的 `UnmarshalJSON`
+ `overrideWithEnv`（抄 `SCHEDULER_REGISTRY_WRITE_ENABLED` 的形态，`:538-544`），
但**取值是枚举字符串不是 bool**：非法值 ⇒ 启动即报错（不是静默默认），照 `parseSchedulerDuration` 的先例。

**关掉时必须是刺耳的**：启动 `Warn` 一行 +
`agentenv_scheduler_routing_execution_arbitration_enabled` 常驻 gauge（§9）+ `/healthz` 带 `routing.execution_arbitration=off`。

**A2 的 schema 不回退**（多两个 nullable 列对本设计的读路径无影响 —— `selectColumns` 是显式列清单）。

---

## 12. 与 A2/A3 设计的接口对账表

| 我用到的东西 | A2/A3 设计里的定义位置 | 一致？ |
|---|---|---|
| `execution_id` 列（UUID，nullable，按状态 CHECK） | §2.1 / §2.2 | ✅ 一致。本设计**只读**该列 |
| `execution_started_at` 列 | §2.5 | ✅ **不使用**（路由不需要 grace 起点，那是 B3） |
| `paused` / `local_only` 行 execution 恒 NULL | §2.2 状态表 | ✅ 一致 —— 这正是 §5.3 PLACED/PINNED 必须 PENDING 的一半理由 |
| `publishing` 行 execution 非空且指向已停机的 VM | §2.2 + §1.5（publish 调用顺序） | ✅ 一致 —— 另一半理由 |
| `running` 行 execution 必须非空 | §2.2 | ✅ 一致，`REGISTRY` 档靠它 |
| **E-A（`resuming` 预分配）** | §2.2 CHECK + §2.3 #2 表 + §2.7 DDL 全文 + §3.3 分支① + §6-U1 | ✅ **冲突点 1 已消解（裁决 A5-U6，2026-08-19）**：A2/A3 文档的 §2.2 CHECK、**§2.7 的 DDL 全文**、§3.3 分支①、§6-U1 行**已全部改成 E-A 裁决态**。🔴 之前最危险的是 §2.7 那段 DDL —— 实现 agent 会整段 copy，而旧的 E-B 形状会让**每一次跨节点 resume 的 claim 写不进去**（`23514`）|
| `ErrExecutionFenced → codes.PermissionDenied` | §3.5 | ✅ 语义一致；🔴 **补一条那份没写的**：`PermissionDenied` 在 gateway 的 `writeSchedulerError`（`server.go:334-343`）落 `default → 502`。A3 的用法走 `PausedRegistry` gRPC 不经 gateway 所以今天不撞，但**必须写下来**，且本设计明令 `Scheduler` service 不得返回它（§S8） |
| `ErrGenerationConflict → Aborted` | §3.5 | ✅ 与身份轴严格分家，本设计不产生任一 |
| 开关命名 `scheduler.registry.write_fencing`（bool，写路径） | §5.2 | ✅ **冲突点 2 已裁决（N2）：三个开关分家是刻意的，只改名不合并**。本设计的是 `scheduler.routing.execution_arbitration`（三态，读路径），理由见 §11。第三个是 `gateway.routing.execution_fencing`。**请勿在实施时合并任意两个** |
| `RegistrySandbox` / `RegistryEntry` 加 `execution_id` | §2.9 + §7 落地清单 | ✅ **冲突点 3 已定稿（任务书 §11 冻结契约表）**：**`RegistrySandbox.execution_id = 13`、`RegistryEntry.execution_id = 10`，由 A2/A3 那个 PR 加一次**，本设计与 gateway 设计都只消费。🔴 字段号以任务书 §11 那张表为唯一来源，三份设计里任何别处的号都以它为准 |
| 两份列清单（`entryColumns` / `selectColumns`）都要加 | §2.9 | ✅ 一致；本设计在 §S7 补了「漏一份的症状分别是什么」的落点表 |
| 测试入口 `make -C services test-with-postgres` + 三变量 | §4.0 | ✅ 沿用；🔴 **新增第四条** `SCHEDULER_REDIS_TEST_REQUIRED`（§13-U1） |
| 夹具风格（每用例独立 schema + 独立 cluster id） | §4.0 / §4.3 | ✅ 沿用，未另造 |
| 「两条常量二选一，构造时选定」的开关实现形态 | §5.2 | ✅ 沿用（Redis 落成两份 Lua 脚本常量） |
| 变异验证「把修复退回去，指定用例必须 FAIL」 | §4.1 / §4.2 | ✅ 沿用；探针自证的对照输入照 §4.3 第 ⑤ 步的形状写 |
| `remove` 不带 execution | §2.3 #10 / §6-U3 | ✅ 无关（读路径不涉及） |
| `HeartbeatRequest` 字段处置 | —— | ⚠️ **与 gateway §4.1 冲突（不是与 A2/A3）**：那份说直接 `reserved 8`，本设计推翻，见 §S2.2 / §13-U2 |

---

## 13. 风险与未决项

### 风险

| # | 风险 | 严重度 | 缓解 | 残留 |
|---|---|---|---|---|
| **R1** | 🔴 **闸 1 在本阶段真阳性≈0**（§2.2）。任务书 A5 判据「被拒（非仅仅改道）」在 scheduler 侧只能兑现成「不再产生」 | 🔴 高（是判据本身的问题，不是实现问题） | 有序比对避免误拒；用 §9 的路由指标而不是拒绝率作为成功判据；闸 1 为 B4/B6 上膛 | ✅ 有：判据措辞需要主 agent 认账（U3） |
| **R2** | **binding 冷启动窗口**：binding 存储为空（scheduler 重启且用内存实现，或 Redis 被清）时，旧持有者若先上报，会安装一个无对手的 binding；此后一个报告间隔内流量打到旧化身且闸 1 放行 | 🟡 中 | 下一个心跳（默认 5s，backoff 最长 60s）B 的更大 execution 覆盖它，自愈；`…{decision="superseded"}` 可观测 | ✅ 有：≤ 一个 report interval。**不做 high-water 记忆**（见「评估过但不做」） |
| **R3** | **跨节点时钟回拨反转 v7 序** | 🟢 低 | 边界 = `lease_ttl_floor` 30s（§S3.2），余量 ≈10⁵；`rejected_older` 非零即告警 | ✅ 明确声明：规则可用，异常靠指标发现 |
| **R4** | 🔴 **发布顺序反了 ⇒ 从未 pause 过的沙箱全线 404** | 🔴 高 | `sandbox_ids` 并存一个周期（§S2.2）+ `legacy_roster_total` 指标 | ⚠️ 若 U2 被否，风险回到 🔴 且无缓解 |
| **R5** | **接口重画的连带面**：`BindingStore` 三个方法签名全变，`NodeRegistry.Roster` / `RosterOf` 变，`reconcile.go:196,279` 两处消费点跟着变 | 🟡 中 | 都是编译期错误，不是运行期漂移 | 🟢 无 |
| **R6** | **`RunRegistryReconcile` 新增的 mismatch 指标依赖 roster 带 execution** | 🟢 低 | 旧 node 期间恒 0，与 `legacy_roster_total` 一起读才有意义 | 需写进指标 Help |
| **R7** | **query-only 副本的 registry reader 在本设计里更重要了**：`resuming`/`running` 分支的 authority 全靠它，而该副本无 placer ⇒ 那条分支恒 `Unavailable`（`lookup.go:220-228`） | 🟡 中 | ⇒ HA 下 authority 实际**只能**来自 binding/roster。这不是新缺陷（`service.go:124-128` 的注释已警告过一次），但 A5 让它的后果从「答不出」变成「答得出但只有一档权威」 | ✅ 有：HA 下 `registry` 档的覆盖率会低于单实例，`lookup_execution_authority_total` 能量出来 |

### 未决项（✅ 2026-08-19 主 agent 已逐条裁决，见表后「裁决收口」）

| # | 事项 | 选项 | 倾向 |
|---|---|---|---|
| **U1** | 🔴 **新增 `SCHEDULER_REDIS_TEST_REQUIRED=1`**（把 `redis_store_test.go:156-165` 的 `t.Skip` 变成 `t.Fatal`），并写进 `services/Makefile` 的 `test-with-postgres` | 不加 ⇒ §10.C 那一发在手跑时会静默 skip，而它是唯一能抓 M3 的用例 | **加**。与 `SCHEDULER_REGISTRY_TEST_REQUIRED` 同形，改动 <10 行 |
| **U2** | 🔴 **推翻 gateway §4.1**：`HeartbeatRequest.sandbox_ids` 保留一个发布周期而不是立刻 `reserved 8` | 立刻 reserved ⇒ 滚动窗口里所有从未 pause 过的沙箱数据面 404（§S2.2，两个存储的删除分支都已复核） | **保留一个周期**，B1 那批删 |
| **U3** | 🔴 **A5 验收判据措辞**：「打向旧化身的请求被拒（非仅仅改道）」在本阶段实际兑现为「不再产生」（§2.2） | ① 认账并把判据改成「旧化身收不到流量 + 闸 1/闸 2 已装配且可自证」；② 坚持要一发真拒绝 ⇒ 需要构造「中央知道更新的化身却仍路由到旧节点」的人造场景，只能靠测试替身，不是集群可验证的 | **①** |
| **U4** | **node 侧比对改成有序（`live < expect ⇒ 拒`）** | 等值比对会在「同机 pause→resume」这个**常规**事件上批量误拒（TTL 自动 pause 1s 一跳 + 数据面 auto-resume） | **改有序**，需传导给 `_design-phase3-node.md` 与 gateway §3.1 |
| **U5** | **`agentenv_scheduler_registry_execution_mismatch` 是否本轮做**（§S3.3） | 零新增 IO（复用 30s shadow reconcile），是双活的直接信号 | **做**。它比闸 1 的拒绝率更能证明 A5 有效 |
| **U6** | **A2/A3 文档的 E-A 段落谁来改** | 那份把 E-A 写成未决（§6-U1），裁决后 §2.2 的 CHECK 与 §3.3 的分支①注释要改 | 由 A2/A3 实施 agent 在动手前顺手改，本设计只对账不代改 |

### ✅ 裁决收口（2026-08-19 主 agent，逐条对应上表）

> 上表的问题陈述与选项**一个字都不删**（它记录了"当初为什么是个问题"）；下表是最终裁决，**实现以下表为准**。
> 编号在任务书里是 `A5-U*`（[§10.3](_impl-plan-control-plane-phase3.md#103--scheduler-a5-设计新增未决项的裁决2026-08-19)），
> 🔴 **注意与 `_design-phase3-scheduler.md` 的 U1–U5 不是同一套**。

| # | ✅ 裁决 | 一句话理由 | 正文落点 |
|---|---|---|---|
| **U1** | ✅ **采纳**：新增 `SCHEDULER_REDIS_TEST_REQUIRED=1`（`redis_store_test.go:156-165` 的 `t.Skip` 受它门控），并写进 `services/Makefile` 的 `test-with-postgres` | 今天找不到 `redis-server` 就静默 skip ⇒ **HA 那一发（M3 唯一的捕手）会消失**，而"漏起 redis"与"全绿"长得一模一样 | §10.0、§14 |
| **U2** | 🔴 ✅ **采纳，推翻 gateway §4.1**：`HeartbeatRequest.sandbox_ids = 8` **保留一个发布周期**（`[deprecated = true]`），B1 那批再删 | 新 scheduler + 旧 node ⇒ 空 roster ⇒ 两个存储把该节点 binding 删光 ⇒ 所有从未 pause 过的沙箱在整个滚动窗口数据面 404。🔴 **P1「无向后兼容包袱」= 没有生产存量，≠ 滚动升级期间没有混版本共存**（已写进任务书 §0 的 P1 条目下）| §S2.2、§4.1、§4.2；**gateway §4.1 的 `reserved 8` 已作废** |
| **U3** | 🔴 ✅ **认账并改判据**：A5 的主体是「路由答案变正确」+「闸 2 自证」，闸 1 只覆盖"node 比中央旧"且本阶段真阳性≈0 | 把 A5 说成"能拦截飞行中的旧化身流量"是夸大 —— **那是 A3 与 reclaim 顺序的职责**。判据措辞已在任务书 §3 A5 行按此重写，**不粉饰** | §2.2、§13-R1；任务书 §3 A5 行、§10.3 |
| **U4** | 🔴 ✅ **采纳**：node 侧比对改**有序**（`live < expect ⇒ 拒`，`live == expect ⇒ 放行`，`live > expect ⇒ 放行并计数`，`本机没有 ⇒ 放行`）| 等值会在「同机 pause→resume」这个常规事件（TTL 自动 pause 1s 一跳 + 数据面 auto-resume）上**批量制造 409**，而误拒发生在用户正等着唤醒的时刻。🔴 **已传导进 node 与 gateway 两份设计**，不是只写在本文里 | §2.2；**node §3.7 第 3/3b 条 + T-A5N-6/7**；**gateway §3.1 / §3.2** |
| **U5** | ✅ **采纳，本轮做**：`agentenv_scheduler_registry_execution_mismatch{node}` | 复用 30s shadow reconcile ⇒ 零新增 IO；是**双活的直接信号**，比闸 1 的真阳性率有用（U3 之后尤其如此）| §S3.3、§9、§14 |
| **U6** | ✅ **采纳**：A2/A3 文档的 E-A 段落**已改**（§2.2 CHECK、**§2.7 DDL 全文**、§3.3 分支①、§6-U1 行）| 实现 agent 会整段 copy §2.7 的 DDL，旧的 E-B 形状会让每一次跨节点 resume 的 claim 写不进去 | `_design-phase3-scheduler.md` §2.2 / §2.7 / §6-U1；本文 §12 冲突点 1 |

**另外两条口径纠正也已采纳**（不在上表，因为它们是本文主动提出的修正而非提问）：

| # | ✅ 裁决 | 落点 |
|---|---|---|
| **S1'** | **采纳口径纠正**：binding 存 execution 的第一价值是「**S2 的仲裁没它写不出来**」，不是"喂 expect 头"。**gateway 文档 §5-S1 的表述已改** | §S1；gateway §5-S1 两处 |
| **S8'** | **采纳并加机械保证**：`PermissionDenied` 在 gateway `writeSchedulerError`（`server.go:334-343`）落 **502**；本轮 `ExecutionFenced` 走 `PausedRegistryService`（不经 gateway）所以不撞，但**必须加 `TestSchedulerServiceNeverReturnsPermissionDenied` 钉住「`Scheduler` service 永不返回 `PermissionDenied`」** | §S8、§10.B、M14；**gateway §6.2 已补一行** |

---

### 评估过但**不做**（防范围蔓延，也防后来者重新提一遍）

| 不做 | 为什么 |
|---|---|
| **controller 在 `mark_running` 成功时直接写 binding** | 一度以为能消掉 R2。复核后否掉：`mark_running` 之前的 binding 要么已随 pause 从 roster 消失、要么已随分区过期 ⇒ **它能关的窗口几乎不存在**，却新增一个 binding 写者。不划算 |
| **per-sandbox「见过的最大 execution」高水位记忆** | 能消掉 R2 的全部。但要么是无界 map（内存），要么是一批新 Redis key + TTL（Redis），为一个 ≤5s 自愈、有指标可见的窗口造一套永久设施 |
| **热路径冲突时回查登记表** | 违反 `lookup.go:143-145` 的硬约束，且 query-only 副本上那条路走不通（无 placer） |
| **专门的 `ValidateExecution` RPC** | gateway §5 已明确不需要；多一次 RPC 就多一个热路径依赖 |
| **让 scheduler 参与 `GET /v2/sandboxes` 的去重** | §8.2 的禁令：会把从未 pause 过的沙箱整体抹掉 |
| **binding 里存 `execution_started_at`** | 路由不需要时间，只需要序。A2 留那一列是给 B3 的 grace 用的 |

---

## 14. 落地清单（文件级，供实施 agent 用）

| 文件 | 改什么 |
|---|---|
| `services/api/proto/scheduler.proto` | **本 PR 加**：`LookupNodeResponse.execution_id = 4` / `execution_authority = 5`；新增 `enum ExecutionAuthority`；`HeartbeatRequest.roster = 10` + `sandbox_ids = 8` 标 `[deprecated = true]`（🔴 **不 reserved**，裁决 A5-U2）；新增 `SandboxRosterEntry`；`RecordAssignmentRequest.execution_id = 3`。<br>🔴 **不由本 PR 加**（归 A2/A3，任务书 §11.1(e)）：`RegistrySandbox = 13` / `RegistryEntry = 10` / **`AcquireSandboxRequest.execution_id = 5`** / `TransitionSandboxRequest.execution_id = 10` / `TransitionKind reserved 6` |
| `services/scheduler/internal/store.go` | `BindingStore` 三方法签名换成 `Binding` / `[]RosterEntry`；新增 `Binding` / `RosterEntry` 类型；`bindingRecord` 加 execution；`arbitrate` + 两个策略值（`off`/`enforce`）；`upsertLockedWithExpiry` 前置仲裁 |
| `services/scheduler/internal/redis_store.go` | `redisBindingRecord` 加 `ExecutionID`；`parseRedisBindingBytes` 解它；`redisLuaHelpers` 的 `parse_node_id` → `parse_binding`；两段脚本各出 fenced/unfenced 两份常量并返回 per-sandbox `decision` |
| `services/scheduler/internal/lookup.go` | `lookupResponse` 扩成五参；五个出口显式传 `(execution, authority)`；`:230-294` 两分支硬编码 `("", PENDING)`；`rosterHolder`（`:379-399`）改按 execution 仲裁、平手退回 `lastSeen` |
| `services/scheduler/internal/service.go` | `Heartbeat`（`:343-372`）：新增 `rosterFromHeartbeat` 收口两代字段 + legacy 计数；`RecordAssignment`（`:305-341`）带 execution 并走仲裁；`registrySandboxToProto`（`:648`）带 execution |
| `services/scheduler/internal/node_registry.go` | `Roster.SandboxIDs []string` → `Entries []RosterEntry`；`RosterOf` 返回带 execution；`:255` 的 `normalizeRoster` 换成带 execution 的归一化（含小写化 + 非法值计数） |
| `services/scheduler/internal/reconcile.go` | `:196-197` / `:279` 两处 roster 消费点跟签名改；新增 `execution_mismatch` 统计（U5） |
| `services/scheduler/internal/metrics.go` | §9 的六个 series |
| `services/scheduler/internal/registry_service.go` | `registryEntryToProto`（`:709`）带 execution（与 A2/A3 PR 协调，只加一次） |
| `services/scheduler/internal/registry/postgres.go` / `store_postgres.go` | 两份列清单加 `execution_id`（属 A2/A3 PR，本设计对账） |
| `services/shared/config/config.go` | `SchedulerConfig` 加嵌套 `Routing SchedulerRoutingConfig{ ExecutionArbitration string }`（默认 `"enforce"`）+ `UnmarshalJSON` + `SCHEDULER_ROUTING_EXECUTION_ARBITRATION` env（非法值启动即报错）。🔴 名字按 N2 裁决 |
| `services/scheduler/cmd/main.go` | 把 `cfg.Scheduler.Routing.ExecutionArbitration` 传进 `createBindingStore`（`:232-246`）；启动 Warn + gauge |
| `services/Makefile` | `test-with-postgres` 加 `SCHEDULER_REDIS_TEST_REQUIRED=1`（U1） |
| `services/scheduler/internal/redis_store_test.go` | `startRedisServerForTest`（`:155-165`）的 skip 受 `SCHEDULER_REDIS_TEST_REQUIRED` 门控（U1） |
| `store_test.go` / `redis_store_test.go`（新增共享契约文件） | §10.A 的 `bindingStoreContract` 表驱动 |
| `lookup_test.go` | §10.B 全部用例 |
| （新）`ha_lookup_test.go` | §10.C 两发 |
| `registry/store_postgres_test.go` | §10.D 两发 |
| （node 侧，非本设计范围） | A1 必须复用 `AcquiredSandbox.entry.execution_id` 启动 VM（§S5）；比对改成有序且只凭正面证据（§S5、§13-U4） |

---

## 附：本设计引用的全部证据锚点

| 断言 | 证据 |
|---|---|
| 内存 binding 无条件覆盖 | `services/scheduler/internal/store.go:99-102`、`:122-135` |
| 内存空 roster 删光该节点 binding | `store.go:90-97` |
| **Redis binding 同样无条件覆盖** | `redis_store.go:293-300`（`SET` 无谓词），且脚本已在 `:285`/`:294` 读旧值 |
| Redis 空 roster 删光 | `redis_store.go:283-291`、`:306-308` |
| 热路径不读登记表 | `lookup.go:143-145`（注释）、`:151-160`（命中即 return） |
| roster「最近上报者胜」 | `lookup.go:375-378`（注释）、`:394-396`（实现） |
| query-only 无 placer ⇒ 登记表分支 Unavailable | `service.go:153-167`（注释逐字 "No placer either"）、`lookup.go:220-228` |
| **query-only 现有测试全用 InMemory** | `lookup_test.go:527-586`（四个子用例，`NewInMemoryBindingStore` / `missingBindingStore`） |
| query-only 必须配 Redis | `shared/config/config.go:826-829` |
| gateway 数据面打 query-only 副本 | `gateway/internal/server.go:226`、`cmd/main.go:54-70` |
| `NotFound` 只从一处产生 | `lookup.go:343-363` `lookupAbsent` |
| `PermissionDenied` 在 gateway 落 502 | `gateway/internal/server.go:334-343`（无该分支，落 `default`） |
| `isCanonicalUUID` 接受大写 | `registry/store_postgres.go:1395`；`requireUUID:1372-1381` 不归一 |
| `MarkRunning` 绝不 INSERT | `registry/store.go:91-110`、`store_postgres.go:842-851` |
| `ClaimForResume` 只接受 paused / 租约过期的 publishing·local_only | `store_postgres.go:596-600`、`:620-623` |
| 租约 TTL 默认 90s / 下限 30s | `shared/config/config.go:24`、`:28` |
| 心跳 report TTL / binding TTL 默认 30s | `shared/config/config.go:479-480` |
| shadow reconcile 30s 且已读 roster + 表 | `shared/config/config.go:18`、`reconcile.go:410` |
| `registry_untracked` 指标语义（缺口大小的现成量尺） | `scheduler/internal/metrics.go:83-89` |
| 两份列清单 | `registry/store_postgres.go:30-41`、`registry/postgres.go:31-41` |
| 行转 proto 两处 | `service.go:648-663`、`registry_service.go:709` |
| 心跳 roster 的两个消费点 | `service.go:361`、`node_registry.go:255` |
| roster 类型与反向索引 | `node_registry.go:29-38`、`:54-58`、`:450-484` |
| `GET /v2/sandboxes` all-or-nothing + keep-first TODO | `gateway/internal/cluster_list.go:72-119`（all-or-nothing 的 502 在 `:84-96`，`http.Error` 那行是 `:94`）、`:234-241`、`:243-260`（TODO 在 `:248-249`） |
| gateway 写 binding 的落点 | `gateway/internal/server.go:505-518` |
| 配置开关先例（bool + env） | `shared/config/config.go:103-111`、`:538-544` |
| 测试三变量与反假绿开关 | `registry/contract_test.go:84-96`、`services/Makefile:41-58` |
| redis-server 缺失即静默 skip | `scheduler/internal/redis_store_test.go:156-165` |
| e2b：enforcement 必须在脚本内 | `packages/api/internal/sandbox/storage/redis/scripts.go:33-39`（经 A3 设计 §3.7 引用） |
| pause 三步：先停 VM 再传再翻牌 | `src/api/impls/paused_coordinator.rs:284 → :296 → :305 → :313`（经 A3 设计 §3.6 引用） |
| `begin_pause` 失败即 return | `src/api/impls/paused_coordinator.rs:288-294`（同上） |
| TTL 自动 pause 1 秒一跳 | `src/orchestrator/service.rs:2093-2115` + `config/default.toml:198`（经任务书 §1.5 引用） |
| 数据面 auto-resume 会铸新化身 | `src/api/proxy.rs:105-107`、`:686 → :800`（经任务书 §1.5 / gateway §1.2 引用） |
| 平台只认 resume 的 404 授权重建 | 主仓 `apps/agent-platform/internal/sandbox/aenv/client.go:58-71`（经 gateway §6.1 引用） |
