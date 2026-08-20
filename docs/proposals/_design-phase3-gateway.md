# 阶段 3 · A5 设计：路由层拒旧化身（gateway 侧）

> 2026-08-19 · **设计文档，未实施**。范围：`services/gateway/` + 它对 `LookupNode` 的契约要求。
> 任务书：[`_impl-plan-control-plane-phase3.md`](_impl-plan-control-plane-phase3.md) §3 A5 / A6
> 决策材料：[`2026-08-19-control-plane-refactor-outcome.md`](2026-08-19-control-plane-refactor-outcome.md) §3.1 候选 3、§3.4
> 姊妹文档：`_design-phase3-scheduler.md`（A2/A3）、`_design-phase3-node.md`（A1/A4）
>
> **A5 是配角**：写路径 fencing（A3）保护不可逆的用户工作区，A5 只保护可恢复的交互流量
> （outcome §3.4 逐字：「路由拒绝保护**分区期间的交互流量（可恢复）**；写路径 fencing 保护**用户工作区（不可逆）**」）。
> 但它是**当前唯一**能挡住旧化身数据面流量的东西 —— envd access token =
> `HMAC-SHA256(seed, sandbox_id)`（`src/sandbox/access.rs:72-78`），**不绑化身**，
> 候选 2 已推迟到 `secure` 加强项 ⇒ 旧化身手上的 token 在新化身起来后依然验签通过。

---

## 0. 结论速览

| # | 结论 | 强度 |
|---|---|---|
| C1 | **A5 只作用于数据面**。控制面一条都不拒 —— 拒了会打死 `resume`（它就是铸造新化身的那个操作）| 🔴 硬结论，有 outcome §3.4 原文背书 |
| C2 | **不要求客户端带化身**。gateway 从 `LookupNode` 拿权威化身，**盖章下发**给它要转发的那台 node，由 node 比对自己活着的化身 ⇒ 拒的是"路由结果不匹配"，不是"客户端声明不匹配"。🔴 **盖章只发生在 `enforce`**：`observe` 只拿回声比对、不盖章（裁决 A5-U5，§10）| 🔴 P2 硬约束的唯一解 |
| C3 | **拒绝码 = 409**，带独立机器码 `sandbox_execution_superseded`。🔴 **绝不能是 404**，也不能是 410 / 503（三者在本链路上都已被占用且语义相反）| 🔴 最不能出错的一点 |
| C4 | 现状是**收敛**且是**有缺陷的收敛** —— `ReconcileNode` 按心跳到达顺序覆盖 binding（`store.go:99-114`），`rosterHolder` 按"最近上报者胜"（`lookup.go:377`）⇒ **一台还活着的旧化身每个心跳周期都会把 binding 抢回去**，gateway 于是把流量打回旧化身 | 🔴 实证，见 §2.2 |
| C5 | **热路径（binding 命中）从不读登记表**（`lookup.go:143-160` 注释逐字："nothing below it may run on a hit"）⇒ **binding 与 heartbeat roster 必须自带 execution，否则 A5 在 99% 的数据面请求上是死的** | 🔴 对 scheduler 的第一位要求 |
| C6 | 长连接（WS / streaming）建立后**永不重估**（`server.go:771-776` 直接复用 `r.Context()`，无 deadline、无撤销点）⇒ 需要一个**撤销器**，且必须只凭正面证据撤销 | 🟡 A5 的第二半。⏸️ **裁决 D-7：撤销器本轮不做**，缺口保留为"已知、有意推迟"（§3.3） |
| C7 | `GET /v2/sandboxes` 扇出到每台 node，**双活时两条行都会回来**，去重靠 `sort.Slice` + keep-first，而两条行的 `startedAt` 与 `sandboxID` **完全相同** ⇒ 胜者不确定 | 🔴 实证缺陷，见 §8 |
| C8 | A6 的 gateway 工作量比想象中大：`GET /sandboxes/{id}` / resume **gateway 侧零改动**（node 自己的响应体直通；🔴 **node 侧的 openapi + codegen 要改，归 impl-node**，见 §9 的归属块），但 `GET /sandboxes`、`GET /v2/sandboxes`、`GET /registry/sandboxes` 三个端点的 DTO 是 **gateway 自己的**，不改就会把 node 新加的 `executionID` **静默吃掉** | 🟡 易漏 |

---

## 1. A5 到底拒什么

### 1.1 三类流量的处置

| 类别 | 入口判定 | 今天怎么走 | A5 的处置 | 为什么 |
|---|---|---|---|---|
| **控制面**（pause / resume / delete / fork / timeout / network / custom-extension-params）| `isSandboxControlPlaneRequest`（`server.go:627-650`），路径取 id（`sandboxIDFromPath`，`server.go:597-625`），`routeSourcePath`，**原样转发**（`upstreamTargetPath`，`server.go:682-687`）| `LookupNode` → 转发 | 🔴 **一条都不拒**。只在日志/指标上记录解析到的化身，**不上线（不进 HTTP 头）** | ① `resume` 按定义就是化身要变的那一刻，前置拒绝必然打死它；② A3 在 SQL 事务里做的是权威判据，gateway 的那份必然更陈旧（来自一个可能落后一个心跳周期的 lookup 答案），**两道语义不同的闸只会制造"哪个说了算"的歧义**；③ outcome §3.4 已把控制面归给 A3 |
| **数据面 · header 入口** | `x-agentenv-sandbox-id` / `e2b-sandbox-id`（`sandboxIDFromHeaders`，`server.go:570-578`），`routeSourceHeader`，加 `/proxy` 前缀 | `LookupNode` → `/proxy` 转发 | ✅ **拒**（条件见 §1.2）| 这就是「交互流量」本体 |
| **数据面 · host 入口** | `{port}-{sandboxID}.{proxy_domain}`（`host_route.go:22-73`），`routeSourceHost`，加 `/proxy` 前缀 | 同上；host 值覆盖冲突的 header（`server.go:646-651`）| ✅ **拒**，与 header 入口**同一段代码** | 两个入口在 `handleProxy` 里已经收敛成同一个 `sandboxID + routeSource`（`server.go:196-213`），拒绝逻辑挂在收敛点之后，天然覆盖两者 |
| **不带任何化身信息** | **今天 100% 的请求**（含以上全部）| —— | **这不是一个需要单独处置的类别**，见 §1.3 | —— |

### 1.2 数据面的拒绝条件（精确定义）

「旧化身的流量」= **这次请求要落到的那台 node 上，该 sandbox 的活化身 ≠ 中央认定的当前化身**。

| `LookupNode` 答案 | 权威度 | 下发 expect 头？ | 拒不拒 |
|---|---|---|---|
| `BOUND`，且化身来自**登记表行**或**带化身的 binding/roster** | `REGISTRY` | ✅ 下发 | ✅ 不匹配就拒 |
| `BOUND`，但化身字段为空（从未 pause 过的沙箱、旧 binding） | `UNKNOWN` | ❌ 不下发 | ❌ 不拒，计入 `unfenced` 指标 |
| `PLACED`（paused，任意节点可重建）| `PENDING` | ❌ 不下发 | ❌ **不拒** |
| `PINNED`（publishing / local_only，钉 origin）| `PENDING` | ❌ 不下发 | ❌ **不拒** |
| ✅ **`resuming`（跨节点接管中，登记表行有 `claimed_by_node_id`）** | 🔴 **`REGISTRY`** | ✅ 下发（认领者**预分配**的那个化身）| ✅ **正常设防** —— 见 §5-S5 的裁决 |

> ✅ **裁决（对应 §13-D4 / §5-S5，2026-08-19 主 agent）：走 E-A（`resuming` 行预分配化身）⇒ S5 成立 ⇒ resume 窗口内正常设防，不选"不设防"。**
> 一句话理由：E-A 让认领者的化身与 `claimed_by_node_id` **同事务**落进登记表，gateway 拿到的就是**新 holder 的新化身**，
> 不存在"holder 变了而化身还是旧的"那个误拒源。
> 🟡 **唯一的降级出口**：**仅当登记表答不出权威化身时**报 `PENDING` 并放行 ——
> 数据面读路径 fail-open 是可接受的，因为**破坏性写已由 A3 在 SQL 层挡住**（A5 是配角，不是最后一道闸）。

🔴 **`PLACED` / `PINNED` 为什么必须不拒**：node 的 `/proxy` 对 paused 沙箱会**自动唤醒**
（`src/api/proxy.rs:105-107` `PROXY_AUTO_RESUME_TIMEOUT`、`ProxyRequestError::AutoResumeFailed`），
唤醒过程**铸造的是一个新化身**。登记表行里那个 `execution_id` 是**上一次**的。
拿它去 expect，等于让每一次"数据面触发的自动唤醒"都被自己拒死。

🟢 **`UNKNOWN` 的覆盖缺口为什么可接受（但必须计数）**：双活的**唯一**产生路径是
「从快照恢复出第二份」，而那需要一条登记表行。一个从未 pause 过的沙箱在集群里
**物理上只可能有一个化身**（id 在创建时铸造，fork 铸的是新 id）。
所以 `UNKNOWN` ⇒ 无行 ⇒ 无双活 ⇒ 没东西可 fence。
⚠️ 这条论证**依赖 A2 的 schema 决策**：若 A2 让 `mark_running` 也建行，覆盖率升到 100%；
若不建行，缺口就是"从未 pause 过的沙箱"，**大小可测**（见 §11 的 `unfenced_total`）。
→ 列入 §13 待裁决。

> ### ✅ 裁决（§13-D3）：**A2 不给 running 沙箱建行**，`BeginPause` 保持唯一建行者
>
> 一句话理由：建行会让"曾被暂停过的沙箱"这条表语义漂成"所有沙箱"，为一个**可论证为安全**的覆盖缺口，
> 去换一张写入面更大的表 —— 不划算，也不符合"不过度设计"。
>
> 🔴 **于是这条缺口是永久的，必须写成显式的「已知覆盖缺口 + 为什么安全」，不许留成隐含假设：**
>
> | 项 | 内容 |
> |---|---|
> | **缺口** | 从未 pause 过的沙箱在登记表里**没有行** ⇒ `LookupNode` 只能答 `UNKNOWN` ⇒ **A5 对它不设防** |
> | **为什么安全** | 双活的**唯一**产生路径是"从快照恢复出第二份"，而那必须先有一条登记表行；无行 ⇒ **物理上只可能有一个化身** ⇒ 没有东西可 fence |
> | **这条论证何时失效** | 若将来出现**不经登记表**就能复制出第二个活化身的路径（例如某种直接从本地 artifacts 拉起的旁路），本条立刻作废 —— 那时要么给 running 建行，要么堵那条旁路 |
> | **怎么知道它有多大** | `agentenv_gateway_execution_fencing_total{decision="unfenced_no_authority"}` 的绝对值就是缺口大小（§11.8）。🔴 **它不是噪声指标，是这条裁决的账单**，不许在指标清理里被删掉 |

### 1.3 🔴 核心难点：请求不带化身信息 —— 为什么这不是缺口

**方案：gateway 从不向客户端索取化身。**

```
客户端  ──(只说 sandbox_id，和今天一模一样)──▶  gateway
                                                  │  ① LookupNode(sandbox_id)
                                                  │     ← node + location + execution_id + authority
                                                  │  ② 剥掉客户端可能塞的 expect 头
                                                  │  ③ 盖自己的章：x-agentenv-expect-execution-id: E
                                                  ▼
                                               node（A1/A4 后）
                                                  │  ④ 比对 E 与"我这台上该沙箱活着的化身"
                                                  │     不等 ⇒ 拒，不执行任何副作用
                                                  ▼ 响应带 x-agentenv-execution-id: <本机活化身>
                                               gateway ⑤ 回程再比对一次（探测 + 自证）
```

三条性质：

1. **零跨仓改动**（满足 P2）：agent-platform / agent-worker / e2b SDK 一行不用改。
2. **拒的是路由结果**：gateway 说的是"我认为这台该是 E，所以才发给你"，
   不是 E 的那台自己举手。判据的两端（中央权威 / 节点事实）**都不在客户端手上**。
3. 🔴 **必须显式拒绝"把化身做成入参"这条路**。理由有两层：
   - 违反 P2（跨仓）；
   - 更要命的是**语义**：客户端持有的化身天然会陈旧。agent-platform 的重试队列里
     压着一条 pause，中间沙箱合法地 pause→resume 换了代 —— 若客户端带旧化身，
     这条 pause 会被拒。而 outcome §3.2 已经裁定：那个场景（G3）**需要平台配合，后期做**。
     现在把入参开出来，等于提前引入一个**只会误伤**的轴。
   - ⇒ 实现要求：gateway **必须剥掉**入站请求里客户端塞的 `x-agentenv-expect-execution-id`
     与 `x-agentenv-execution-id`，再盖自己的。今天 `ReverseProxy.Rewrite`（`server.go:377-390`）
     是**全量透传**入站头的，不剥就是一个可被客户端伪造的 fencing token。

**有没有更好的方向？** 评估过三个，都不选：

| 备选 | 为什么不选 |
|---|---|
| 让 gateway 缓存"上次见过的化身"，变化即拒 | 那是**变更检测**不是**权威**。gateway 无状态（多副本、可重启），缓存本身会漂；且第一次见到就没有基线，缺省 fail-open ⇒ 等于没有 |
| 只做回程比对（node 回声，不下发 expect） | 🔴 **请求已经送达旧化身并执行完了**。envd 的写文件、执行命令都已发生。回程拒绝是"检测"不是"预防"，不能当主闸 |
| gateway 直接查登记表 | 打掉阶段 2 刚建立的「controller 独占 PG」（G7，整个重构最硬的那条理由）。不考虑 |

---

## 2. 现状体检：这道门今天敞开到什么程度

### 2.1 gateway 是默认透传

`Handler()`（`server.go:101-134`）只对 `/health`、`/metrics` 做本地处理，其余全进
`handleProxy`（`server.go:158-298`）。`handleProxy` 里只截四类自处理：
`isClusterListRequest` / `isNodeListRequest` / `isRegistryListRequest` / `isNodeAdminRequest`
（`server.go:177-195`），**其余一律转发**。转发前的唯一决策就是一次 `LookupNode`
（`server.go:226`）。⇒ A5 的落点是**唯一的**：`server.go:226`（拿到答案）到 `server.go:277`
（算 upstream 路径）之间。这一点是好消息 —— 不需要在 N 个分支上重复插桩。

### 2.2 🔴 实证缺陷一：解析结果本身会指向旧化身

```go
// services/scheduler/internal/store.go:99-114（ReconcileNode）
expiresAt := now.Add(s.bindingTTL)
for sandboxID := range normalized {
    s.upsertLockedWithExpiry(sandboxID, node, expiresAt)   // ← 无条件覆盖
}
```

```go
// services/scheduler/internal/lookup.go:375-377（rosterHolder 注释逐字）
// More than one node listing the same sandbox is normal mid-takeover: the
// origin keeps its paused record until its own reconciliation drops it. The
// most recent report wins, ...
```

两处都是**到达顺序仲裁**，不是权威仲裁。把 A5 存在的那个场景代进去：

1. node A 上沙箱 X 活着，租约因一次 GC / 慢盘 / 短暂网络抖动过期；
2. reclaim（租约过期 **且** deadline 过期，双条件）把 X 判给 node B，B 从快照恢复 ⇒ 新化身；
3. **A 还活着**，它的 roster 里 X 还在。A 的下一次心跳一到，
   `ReconcileNode` 就把 binding 覆盖成 A；
4. gateway 下一个数据面请求 `LookupNode` 命中 binding ⇒ **打到 A（旧化身）**；
5. B 的心跳到了又抢回去 —— 流量在两个化身之间**来回抖**。

⇒ 这不是"只做了改道没做拒绝"，这是**改道本身会改到错的那一侧**。
🔴 **A5 的第一件事不是加拒绝，是让解析结果变成权威推导的。**

### 2.3 🔴 实证缺陷二：热路径根本不读登记表

```go
// services/scheduler/internal/lookup.go:143-145（注释逐字）
// 1. The binding. This is the hot path — every proxied request lands here —
// so nothing below it may run on a hit.
```

登记表是第 3 步（`lookup.go:185-216`），binding 命中直接 return（`lookup.go:151-160`）。
再叠加 HA 模式：数据面走 `gateway.query_only_scheduler_addr`
（`server.go:226` 用的是 `s.queryOnlyScheduler`；README:150），
而 `QueryOnlyService` **没有 placer**（`service.go:153-166` 注释逐字："No placer either"）——
它一旦要读登记表并落到 `running`/`resuming` 分支，`lookup.go:220-227` 直接答 `Unavailable`。
⇒ **query-only 副本上，能成功回答数据面的路径实际只有 binding 命中这一条。**

⇒ **binding 里没有 execution，A5 就在数据面 99% 的请求上不存在。**
这是 §5 接口清单的第 1 条，也是整个 A5 的成败点。

### 2.4 缺陷三：长连接建立后永不重估

```go
// services/gateway/internal/server.go:771-776
func requestContextForProxy(r *http.Request, routingCtx context.Context, streaming bool) (context.Context, context.CancelFunc) {
	if streaming {
		return r.Context(), func() {}      // ← 无 deadline，cancel 是空操作
	}
	return routingCtx, func() {}
}
```

WS（`isWebSocketRequest`，`server.go:798-801`）与 streaming（gRPC / Connect / SSE / TE:trailers，
`server.go:778-796`）走这一支。envd 的 PTY、进程流、`/process.Process/StreamInput`
（`server.go:445-447` 有专门分支）**全是长连接**。
⇒ 接管发生前建立的连接，会一直流向旧化身，直到客户端自己断或 VM 被杀。
**这条是"打向旧化身的请求"里最长命的一类，也是任务书验收判据「被拒，非仅仅改道」直指的东西。**

---

## 3. 机制设计：三段闸 + 一个撤销器

### 3.1 闸 1（预防）：下发 expect，由 node 拒　—— 🔴 **只在 `enforce` 下生效（裁决 A5-U5）**

> 🔴 ✅ **裁决 A5-U5（2026-08-20）：`observe` 不下发 expect 头**，理由与三态对照表见 §10。
> 一句话：下发 = 把拒绝权**委托**给 node，而委托是**单向**的 —— node 一旦回了 412，
> gateway 只能按 §6 翻成 409 交给客户端，**撤不回来**。所以"只看不拒"的那一档不能下发。

- gateway 在 `handleProxy` 里，数据面 + `authority == REGISTRY`
  **且 `gateway.routing.execution_fencing == enforce`** 时，
  在 `proxyRequestOptions` 上带 `expectExecutionID`，由 `Rewrite`
  （`server.go:377-390`，即 host 路由今天注入 `x-agentenv-sandbox-id` 的同一处）
  写入 `x-agentenv-expect-execution-id`。
- **先剥后盖**：`req.Out.Header.Del(...)` 两个化身头，再 `Set`。
- node（A1/A4 之后）在 `/proxy` 入口比对；**判为旧**则拒且**零副作用**。
  > 🔴 ✅ **裁决 A5-U4（2026-08-19）：node 的比对是有序的，不是等值的。**
  > 记本机活化身 `live`、下发的 expect `E`（都是小写 canonical UUID v7，字典序即时间序）：
  > **`live < E` ⇒ 拒**；**`live == E` ⇒ 放行**；**`live > E` ⇒ 放行并计数**（本机比中央新 = 中央落后一个心跳，
  > 而它路由到的就是本机，不存在第二份活的要防）；**本机没有该沙箱 ⇒ 放行**（落回既有 404 / auto-resume）。
  > 理由：`live > E` 是**常规事件** —— TTL 自动 pause 默认 1s 一跳叠加数据面 auto-resume，
  > 「同机 pause→resume」每天都在发生；等值比对会在这条最常见的合法路径上批量制造 409。
  > 论证见 [`_design-phase3-scheduler-a5.md`](_design-phase3-scheduler-a5.md) §2.2，接收端实现见
  > [`_design-phase3-node.md`](_design-phase3-node.md) §3.7 第 3 / 3b 条。
  > ⚠️ **本文 §1.2 与 §11.1 里凡写"不匹配"的地方，一律读作"`live < E`"**，不是"≠"。
- 🔴 **node 回什么码由 node 侧设计定，但 gateway 必须翻译**：不允许 node 的拒绝码直通客户端。
  理由见 §6.3 —— node 的 `/proxy` 今天已经会吐 404（`src/api/proxy.rs:868`
  `SandboxNotFound => NOT_FOUND`）和 410（`:869-872` `SandboxUnavailable => GONE`），
  这两个码已经有别的含义，混进来就分不开了。
  建议 node 用 **412 Precondition Failed + `x-agentenv-refusal: sandbox_execution_superseded`**
  作为内部信号（`/proxy` 现有码表里 412 是空位），gateway 见到这对组合就改写成 §6 的标准形状。

### 3.2 闸 2（检测 + 自证）：回程比对 node 的回声　—— **`observe` 与 `enforce` 都跑**

> 🔴 **闸 2 不需要闸 1 铺路**（裁决 A5-U5 的全部立足点）：node 的回声是**无条件**的
> （下一行；node 设计 §3.7 明写"放行与拒绝两种响应都带"），所以
> **不下发 expect 也拿得到全部可观测性**。`observe` = 闸 1 关、闸 2 开但不拒 ⇒
> 计数、日志、跨服务对账一条不少，而客户端一个 409 都收不到。

- node 在 `/proxy` 响应上**始终**回 `x-agentenv-execution-id: <本机该沙箱的活化身>`
  （🔴 **包括没收到 expect 头的时候** —— 这正是 `observe` 能只看不拒的前提）。
- gateway 在 `ModifyResponse`（`server.go:373-400` 已有该钩子）里比对，**同样是有序比对（裁决 A5-U4）**：
  - 回声 `== expect` ⇒ 放行，计 `enforced_pass`；
  - 回声 **`> expect`** ⇒ **放行**，计 `…execution_fencing_total{decision="unfenced_node_ahead"}` —— node 比中央新是常规的同机换代，
    🔴 **不许拒**（拒了就是把每一次同机 pause→resume 变成 409）；
  - 回声 **`< expect`** ⇒ **不把响应交给客户端**，改写成 §6 的 409，计 `refused_echo`；
  - **头不存在** ⇒ 说明这台 node 没参与 fencing（旧镜像 / 回退），计
    `unfenced{reason="node_silent"}` 并放行。
- 🔴 **闸 2 永远不能当主闸**：它拒的时候请求已经在旧化身上执行完了。
  它的价值是两条：
  1. **探针自证**（方法论 §8.3）——闸 1 若被回退或 node 没装配，
     `refused_echo` / `node_silent` 会立刻非零，而不是"看着一片绿"；
  2. 抓 node 侧比对逻辑本身的 bug。
- ⚠️ `ModifyResponse` 对**已升级的 WS** 不生效（升级后 ReverseProxy 走 hijack）。
  WS 的闸 2 只覆盖握手响应（101 之前）。这是已知不对称，见 §12 R-3。

### 3.3 闸 3（撤销）：长连接周期性重估　—— 🔴 ✅ **裁决（§13-D7）：本轮不做**

> ✅ **裁决：本轮不实现闸 3**（不起后台 goroutine、不改 `requestContextForProxy`、不加
> `gateway.execution_revalidate_interval`）。一句话理由：**不过度设计** —— 闸 1 / 闸 2 是本轮的价值主体，
> 闸 3 是唯一需要在 gateway 引入后台 goroutine 的部分，砍它比砍闸 1/2 划算得多。
>
> 🔴 **残留缺口照实保留，标注「已知、有意推迟」，不许当成已解决：**
>
> | 残留 | 后果 | 观测手段 |
> |---|---|---|
> | 接管**之前**建立的 WS / streaming 连接，会一直流向旧化身，直到客户端自己断或 VM 被杀（§2.4）| 任务书 A5 判据"被拒，非仅仅改道"对**已建立的长连接**这一类**本轮不兑现** | 无专门指标（`revocation_total` 随闸 3 一起推迟）；靠 node 侧 VM 被回收时连接自然断 |
> | envd 的 PTY / 进程流 / `StreamInput` 全属这一类 | 一次接管后，旧连接上的写仍会落到旧化身的 VM 里 | 🔴 **不可逆损失由 A3 在 SQL 层挡住**（旧化身的 pause/publish 写不进登记表），A5 缺的这一半只影响**交互流量**（可恢复），与 §0 "A5 是配角"的定位一致 |
>
> **推迟不等于作废**：下面的设计原样保留，B 批次或后续小 PR 直接照做；
> 🔴 尤其 `TestRequestContextNotCanceledWhenStreamingCancelCalled` 的处置（§11.4）**在真正实现闸 3 时**才动，
> 本轮**不要**去改那个测试。
>
> **以下为推迟内容（保留设计，本轮不实现）：**

- 仅对 **数据面 + 长连接 + 建链时 `authority == REGISTRY`** 的连接启用。
- 实现形态：**不引入 gateway 全局状态**。每条这样的连接在 `proxyRequest` 之前
  起一个随连接生命周期结束的 goroutine，按 `gateway.execution_revalidate_interval`
  重跑 `LookupNode`；命中"化身变了"就 cancel 该连接的 context。
- 🔴 **只凭正面证据撤销**（本仓反复吃过亏的那条）：
  | lookup 结果 | 动作 |
  |---|---|
  | 成功 + `authority == REGISTRY` + `execution != 建链时的 E` | ✅ 撤销 |
  | 成功 + `authority != REGISTRY` | ❌ 不撤销 |
  | `Unavailable` / `FailedPrecondition` / 超时 / 连接错误 | ❌ **不撤销**，计 `lookup_failed_ignored` |
  | `NotFound` | ❌ **不撤销**（404 的三个来源都不足以证明化身换了）|
  否则一次 scheduler 滚动升级 = 全集群长连接同时断。
- 需要一处小改动：`requestContextForProxy`（`server.go:771-776`）的 streaming 分支
  改成 `context.WithCancel(r.Context())`。
  🔴 它会打破 `TestRequestContextNotCanceledWhenStreamingCancelCalled`（`server_test.go:1247-1259`）
  的**字面断言**（"cancel 必须是空操作"），但那个测试的**意图**是
  "路由超时不许切断流"。⇒ 见 §11.4：按意图重写，不许直接删。

### 3.4 控制面：只记录，不上线

控制面请求把解析到的 `execution_id` / `authority` 写进
`s.logger.Debug("gateway routed request", ...)`（`server.go:266-274`）的字段
和 `agentenv_gateway_execution_fencing_total{plane="control",decision="observed"}`。
**不加任何 HTTP 头**。理由：一旦上线，后来者会很自然地"顺手让 node 也校验一下"，
于是就有了两道真相源不同的闸，而 gateway 那道是更陈旧的那道。

---

## 4. `LookupNode` 契约怎么扩

### 4.1 proto 变更建议

```proto
message LookupNodeResponse {
  Node node = 1;
  SandboxLocation location = 2;
  string origin_node_id = 3;

  // 新增：本次答案所指向的那个化身。
  //
  // 语义严格是"node 字段那台机器上，中央认为当前该活着的化身"，
  // 不是"客户端应该带的值"、也不是"曾经存在过的化身"。
  // 空字符串 = 中央答不出来，与 execution_authority=UNKNOWN 等价。
  //
  // 形状对齐 NodeIdentity.service_instance_id：UUID v7 的字符串形式，
  // 服务端校验（先例：services/scheduler/internal/service.go:345）。
  string execution_id = 4;

  // 新增：调用方可以拿 execution_id 做多硬的事。
  ExecutionAuthority execution_authority = 5;
}

// ExecutionAuthority 说明 execution_id 的来源强度。
//
// 🔴 它不是"置信度"，是"允许被用来拒绝流量吗"。只有 REGISTRY 一档允许。
enum ExecutionAuthority {
  // 更旧的 scheduler，或本次答案没有化身来源。等同 UNKNOWN。
  EXECUTION_AUTHORITY_UNSPECIFIED = 0;
  // 中央答不出当前化身：沙箱从未进过登记表，或 binding/roster 是化身字段
  // 出现之前写下的。调用方必须放行，并把这次放行计成"未受保护"。
  EXECUTION_AUTHORITY_UNKNOWN = 1;
  // execution_id 来自权威来源，且它现在就应当活在 node 上。可用于拒绝。
  EXECUTION_AUTHORITY_REGISTRY = 2;
  // node 即将铸造一个新化身（PLACED / PINNED，以及任何"正在换代"的中间态）。
  // execution_id 若非空，指的是上一代，绝不可用于拒绝。
  EXECUTION_AUTHORITY_PENDING = 3;
}
```

`HeartbeatRequest` 也要改（否则 §2.3 的洞堵不上）。
🔴 ✅ **本段的原提案（下方灰块）已被裁决 A5-U2 推翻**，定稿以裁决块为准。

> ### ❌ 原提案（保留作记录，**不许照抄**）
>
> ```proto
> message HeartbeatRequest {
>   // ...
>   reserved 8;  // formerly: repeated string sandbox_ids = 8
>   // node 报"我这台上活着的沙箱和它们各自的化身"。
>   repeated SandboxRosterEntry roster = 10;
> }
>
> message SandboxRosterEntry {
>   string sandbox_id = 1;
>   // 🔴 必填。空值不等于"未知"，而是这条 roster 项不可用于绑定仲裁 ——
>   // 让它可空，A2 的"execution_id NOT NULL"就会在这里被绕过去。
>   string execution_id = 2;
> }
> ```
>
> 原理由：P1 说无向后兼容包袱 ⇒ 直接 `reserved` 旧字段号重画，不做双写过渡。

> ### 🔴 ✅ 裁决 A5-U2（2026-08-19 主 agent）：`sandbox_ids = 8` **保留一个发布周期**，不立刻 `reserved`
>
> ```proto
> message HeartbeatRequest {
>   // …1-7 不变…
>   // 🔴 保留而不是 reserved：node 与 controller 独立滚动，窗口内必然新旧并存，
>   // 而这个字段的"空"在 scheduler 侧不是降级，是"这台节点上一台沙箱都没有"，
>   // 会触发批量删 binding。B1 退役 RenewNodeLease 时一并删掉它。
>   repeated string sandbox_ids = 8 [deprecated = true];
>   P2pEndpoint p2p_endpoint = 9;
>   // node 报"我这台上活着的沙箱和它们各自的化身"。
>   repeated SandboxRosterEntry roster = 10;
> }
>
> message SandboxRosterEntry {
>   string sandbox_id = 1;
>   // 小写 canonical UUID v7。空值 = 这条 roster 项**不参与仲裁**（unknown），
>   // 但**保留路由**（不丢弃该项）—— 见 scheduler-a5 §S2.3。
>   string execution_id = 2;
> }
> ```
>
> **推翻理由（实证）**：新 scheduler + 旧 node ⇒ `roster` 为空 ⇒ 两个 binding 存储都会把该节点名下的
> binding **删光**（`store.go:90-97`、`redis_store.go:283-291`/`:306-308`）+ roster 兜底同时失明
> ⇒ **所有从未 pause 过的沙箱在整个滚动窗口里数据面 404**。
> 🔴 **P1「无向后兼容包袱」指的是没有生产存量，不等于滚动升级期间没有混版本共存**
> （已写进任务书 §0 的 P1 条目下）。
>
> 🟡 **本文原写的"execution_id 必填、空值 = 不可用于绑定仲裁"这句语义保留**，
> 但**实现按 scheduler-a5 §S2.3 的更保守形态**：空 execution 的 roster 项**保留为 unknown**
> （无 incumbent 时可安装 binding，**不得顶掉**任何带 execution 的 incumbent），
> **不丢弃**该项 —— 丢弃等于"为了不给一条未受保护的路由，赔上一条能用的路由"。
>
> 完整论证与回落函数 `rosterFromHeartbeat` 见 [`_design-phase3-scheduler-a5.md`](_design-phase3-scheduler-a5.md) §S2.2。

`RegistrySandbox` 与 `RegistryEntry` 各加一个 `string execution_id`（A6 用，见 §9）。
🔴 **字段号与归属见任务书 §11 的冻结契约表**：`RegistrySandbox.execution_id = 13`、
`RegistryEntry.execution_id = 10`，**由 A2/A3 那个 PR 加一次**，本设计只消费
（防两个 PR 各选一个号打架）。

### 4.2 gateway 侧消费

```
resp := LookupNode(sandbox_id)
fence := decideFencing(mode, routeSource, resp)   // 唯一决策函数
  mode==off                            → {send:false, enforce:false}   // 早退，零新代码路径
  !isDataPlaneRouteSource(routeSource) → {send:false, enforce:false, log:true}
  authority != REGISTRY                → {send:false, enforce:false, metric:"unfenced"}
  otherwise                            → {send:true,  enforce: mode==enforce, expect: resp.ExecutionId}
```

`decideFencing` 是**纯函数**（入参全是值，无 IO）⇒ 可以逐条穷举做表驱动测试，
也是变异验证唯一需要动的地方。

---

## 5. 我需要 scheduler 提供什么（接口清单）

> 供主 agent 与 `_design-phase3-scheduler.md` 对账。按重要性排序，**S1 不满足则 A5 整体失效**。

| # | 要求 | 为什么 | 不满足的后果 |
|---|---|---|---|
| **S1** | 🔴 **binding 存储（内存 + Redis 两种实现）必须持久化 `execution_id`，并在 `LookupNode` 的 binding 命中分支原样回出，`authority=REGISTRY`** | 🔴 ✅ **口径已按裁决 A5-S1' 纠正**：第一价值**不是**"喂 expect 头"，而是**「没有它就写不出 S2 的仲裁」** —— 存储里没有可比对的对象，`ReconcileNode` 只能按到达顺序覆盖，缺陷一永久存在。第二价值才是：热路径命中 binding 就 return、**从不读登记表**（`lookup.go:143-160`）；HA 模式下数据面走 query-only 副本，它连 placer 都没有（`service.go:153-166`）⇒ 除 binding 外无路可走 | 🔴 **不是"A5 少一层保护"，是缺陷一永久存在**：活着的旧化身每个心跳把 binding 抢回去，流量在两个化身之间来回抖。附带后果：A5 在 ~99% 数据面请求上是 `UNKNOWN` |
| **S2** | 🔴 **`HeartbeatRequest` 的 roster 带 execution**（`sandbox_ids` → `SandboxRosterEntry`），且 `ReconcileNode` **不再按到达顺序覆盖** | `store.go:99-114` 无条件覆盖 + `lookup.go:377`"最近上报者胜" ⇒ 活着的旧化身每个心跳都把 binding 抢回去（§2.2）| 解析结果本身指向旧化身，加多少拒绝逻辑都白搭 |
| **S3** | **同 sandbox 两个化身冲突时的仲裁规则**。建议：**ExecutionID 是 UUID v7 ⇒ 字典序即时间序 ⇒ 大者胜**，并对"较小者覆盖较大者"的尝试打 warn 日志 | 避免每次冲突都回查登记表（一次 DB 读/冲突项）；v7 单调是任务书已冻结的属性 | 需要退回"冲突时查登记表"，成本高但可行。⚠️ v7 依赖墙钟，跨机时钟回拨会反转顺序 —— 需要那条 warn 日志才能发现 |
| **S4** | **`LookupNodeResponse` 加 `execution_id` + `execution_authority`**（§4.1），且 `PLACED` / `PINNED` **必须**报 `PENDING` | 数据面对 paused 沙箱会触发 node 自动唤醒并铸新化身（`src/api/proxy.rs:105-107`），用旧化身 expect 会拒死自动唤醒 | 自动唤醒全线 409 |
| **S5** | **`resuming` 行的 `execution_id` 语义**：必须与 `claimed_by_node_id` **同一个 SQL 事务**写入，且指向**认领者的新化身**。做不到就一律报 `PENDING` | `lookup.go:295-325` 的 `running/resuming` 分支按 `entry.Holder()` 路由；holder 变了而化身还是旧的，gateway 会拿旧化身去 expect 新 holder ⇒ 每一次跨节点 resume 期间的数据面流量全被拒 | 必须降级到 `PENDING`，等于 resume 窗口内不设防（可接受，但要显式声明） |
| **S6** | `RecordAssignmentRequest` 加**可选** `execution_id` | gateway 在 create/fork 成功后写 binding（`server.go:505-518`）。若能从 node 响应头拿到化身就带上，拿不到就留空 | 仅影响"新建后到第一次心跳"这段窗口的权威度（≤ `report_ttl`）。该窗口内沙箱是全新的、单化身 ⇒ 🟢 **可降级，不阻塞** |
| **S7** | `RegistrySandbox` proto 加 `execution_id` | A6 要在 `GET /registry/sandboxes` 上只读暴露 | 运维看不到 per-row 化身，双活时无处对账 |
| **S8** | `LookupNode` 的**错误码语义不许变**：`NotFound` 仍然且只能表示"登记表可读且无此行" | gateway 的 404 直通平台的"授权重建工作区"（§6.1）| 用户工作区蒸发 |

### ✅ 裁决收口（2026-08-19 主 agent）：本清单逐条的答复

| # | ✅ 裁决 | 一句话理由 |
|---|---|---|
| **S1** | 🔴 **做**（binding 存储持久化 `execution_id`，内存 + Redis 两种实现都要）。**它是 A5 的成败点，不是优化项**。附加硬要求：🔴 **必须在 HA / query-only 副本形态下验证** | 热路径命中 binding 就 return、从不读登记表；**本地单 scheduler 测不出该失效** —— 那正是 outcome §1.2 订正②踩过的同一个坑。🔴 **口径纠正（A5-S1'）：第一价值是「S2 的仲裁没它写不出来」** —— 按旧口径读，S1 会被当成闸 1 的配套而随闸 1 一起被质疑价值（闸 1 真阳性≈0，见任务书 §10.3 A5-U3），实际它与闸 1 的成败无关 |
| **S2** | **做**：roster 带 execution，`ReconcileNode` 不再按到达顺序覆盖 | 解析结果本身指向旧化身时，加多少拒绝逻辑都白搭（§2.2） |
| **S3** | **热路径按 UUID v7 字典序大者胜**，并对"较小者覆盖较大者"的尝试**打 warn**（时钟回拨信号）；🔴 **登记表在被查询时是真相**（冲突仲裁的最终解释权在它，不是缓存） | 热路径不能为每次冲突付一次 DB 读；v7 单调是已冻结属性，而它依赖墙钟 ⇒ 必须有那条 warn 才发现得了回拨 |
| **S4** | **做**（`LookupNodeResponse` 加 `execution_id` + `execution_authority`；`PLACED` / `PINNED` 必须报 `PENDING`）| 不然数据面自动唤醒会被自己拒死 |
| **S5** | **做，且按 E-A**：`resuming` 行的化身 = **认领者预分配**的新化身，与 `claimed_by_node_id` 同事务写入 ⇒ **resume 窗口内正常设防**（§1.2 的 ✅ 块）| 见 `_design-phase3-scheduler.md` §6-U1 裁决；"窗口内不设防"这个降级**不采纳**，只在登记表答不出时才降级为 `PENDING` |
| **S6** | 保持"可降级、不阻塞"（有就带，没有就留空）| 该窗口内沙箱是全新的、单化身 |
| **S7** | **做**（`RegistrySandbox` 加 `execution_id`）| 双活时唯一能按行对账的地方 |
| **S8** | **不许变**（`NotFound` 仍然且只能表示"登记表可读且无此行"）| 平台据此授权重建工作区 |

🔴 **归属提醒**：S1/S2/S3/S6 落在 `services/scheduler/internal/store.go` / `lookup.go`，
**不在 `_design-phase3-scheduler.md` 的范围声明（registry + PausedRegistryService）之内** ——
它们归 scheduler-A5 那份设计，别让它掉在两份文档中间。

**我不需要 scheduler 提供的（避免过度设计）**：
- 不需要"当前化身的完整历史"；
- 不需要 gateway 能写化身；
- 不需要专门的 `ValidateExecution` RPC —— `LookupNode` 一次答完就够，多一次 RPC 就多一个热路径依赖。

---

## 6. 拒绝的响应形状

### 6.1 🔴 为什么绝不能是 404

**产地就在 gateway**：`writeSchedulerError`（`server.go:328-344`，`NotFound → 404` 在 `:337-338`）把 scheduler 的
`codes.NotFound` 翻成 404，请求**从未到过 node**。而主仓 agent-platform 逐字写着：

```go
// apps/agent-platform/internal/sandbox/aenv/client.go:58-71
// 🔴 **它有多重含义，不能一律读成"沙箱不存在"**。... 三个地方会产生它：
//   沙箱确实不存在        —— 唯一能授权重建的读法
//   ...
// ⇒ 判"沙箱没了、可以重建"**只准用 resume 的 404**
```

⇒ 「授权重建工作区」这个动作，判据就是 **resume 的 404**。
A5 若把"化身过期"落成 404，平台会读成"沙箱没了" → 重建 → **用户工作区蒸发，全程不报错**。

有人会说："A5 只拒数据面，平台的 404 判据在控制面 resume 上，撞不上啊。"
**不够**。三条理由让这条禁令必须是绝对的：

1. **共用函数**：`writeSchedulerError` 是控制面/数据面**共享**的错误出口
   （`handleProxy` 的 `LookupNode` 失败与 `Schedule` 失败都走它，`server.go:229`/`:261`）。
   任何"顺手复用一下"的拒绝实现都会继承 `NotFound → 404`。
2. **共用路径**：控制面与数据面在 `handleProxy` 里是同一个函数体，
   拒绝逻辑写成一个 helper 后，后来者把它挪到收敛点之前是**一行 diff** 的事。
3. **数据面本身也有 404 语义了**：node 的 `/proxy` 对不认识的沙箱回 404
   （`src/api/proxy.rs:868`）。fencing 拒绝若也是 404，
   "这台没有这个沙箱" 与 "这台有但是旧的" 在客户端眼里完全一样 ——
   而这两件事的正确处置**相反**（前者应重解析，后者应重解析后重试；
   但任何把 404 当"沙箱没了"的启发式会对两者都做错事）。

⇒ **规则：gateway 的任何 fencing 拒绝，在任何层、任何 plane，都不得是 404。**
用一发机械保证钉死（§11.3）。

### 6.2 候选码逐条论证

| 码 | 结论 | 论证 |
|---|---|---|
| **404 Not Found** | 🔴 **禁** | §6.1 |
| **410 Gone** | 🔴 **禁** | ① 语义上"永久消失、别再来" —— 而我们要的恰恰是"再来一次就对了"；② 中间层/客户端普遍把 410 与 404 归一类处置；③ **本链路上已被占用**：node 的 `/proxy` 用 410 表示 "sandbox is not proxyable in its current state"（`src/api/proxy.rs:869-872`），再占一个含义就分不开了；④ 平台侧落进 `resp.StatusCode >= 300` 的泛化错误分支（`client.go:300-302`），不可重试 |
| **503 Service Unavailable** | 🔴 **禁** | `writeSchedulerError` 的注释逐字定义了 503 的含义：`Unavailable` = "scheduler 说它没法看"，`FailedPrecondition` = "唯一能服务的那台不肯服务"，两者都是"**过一会儿可能就变了**"（`server.go:320-327`）。fencing 拒绝是一个**确定的事实**，不是"看不了"。混进去就毁掉这段注释存在的全部理由 |
| **502 Bad Gateway** | ❌ | 语义是上游坏了。上游没坏，是我们不让它服务。<br>🔴 ✅ **裁决 A5-S8'（2026-08-19）顺手钉住的一条**：`writeSchedulerError`（`server.go:334-343`）**没有 `PermissionDenied` 分支** ⇒ 它落 `default` ⇒ **502**。而错误码三件套里 `ExecutionFenced` 选的正是 `codes.PermissionDenied` —— 今天撞不上，因为它走 `PausedRegistryService` gRPC（node 直连 scheduler，**不经 gateway**）。⇒ **明令：`Scheduler` service 的任何方法都不得返回 `PermissionDenied`**，并配一发 `TestSchedulerServiceNeverReturnsPermissionDenied` 穷举钉住（`_design-phase3-scheduler-a5.md` §S8 / §10.B / M14）。否则将来有人把它挪过去，一个精确的 fencing 事实就**静默变成 502** |
| **421 Misdirected Request** | 🟡 语义最贴，但不选 | RFC 9110 逐字："the request was directed at a server that is not able to produce a response … the client MAY retry over a different connection" —— 字面就是我们的场景。**但**：① Go / 浏览器 / HTTP2 栈对 421 有**自动重连重试**的内建行为，会把"拒绝"变成我们不控制的重试风暴；② 我们的下游客户端（agent-worker、e2b SDK）都不认它，会落进泛化错误 |
| **409 Conflict** | ✅ **推荐** | ① 语义成立："资源当前状态与请求冲突"；② **重试即对**——重试会重新解析并落到新化身；③ **平台已经认它**：`client.go:298-299` 映射成 `ErrConflict`，注释逐字"语义是**稍后再来**，不是'出错了'"；④ 🔴 **数据面码表上是空位**：node 的 `/proxy` 用了 400/401/404/410/500/502/504，**没有 409**（`src/api/proxy.rs:851-890`）⇒ 客户端见到 409 就唯一地知道是 gateway 的 fencing，零歧义 |

**409 的唯一代价**：控制面 resume 撞 running 时也回 409（`ErrConflict` 的注释即以此为例）。
两者都是"退避重试"⇒ **行为上不冲突**，只是**诊断上**会混。
⇒ 用机器码 + 独立指标 + 独立响应头把它们分开，见 §6.3。

### 6.3 标准拒绝形状

```
HTTP/1.1 409 Conflict
Content-Type: application/json
x-agentenv-refusal: sandbox_execution_superseded

{
  "code": "sandbox_execution_superseded",
  "message": "this request reached an execution of sandbox <id> that the control plane has superseded; retry to be routed to the current one",
  "sandboxID": "…",
  "expectedExecutionID": "…",        // gateway 从 LookupNode 拿到的当前化身
  "observedExecutionID": "…",        // node 回声；闸 1 拒绝时缺省为空
  "refusedBy": "node" | "gateway"    // 闸 1 / 闸 2
}
```

- `x-agentenv-refusal` 是**响应专用头**，不与任何路由入参头重名。
- `code` 是稳定机器码，**不随文案变**。
- **化身 id 出现在 body 里是可以的**：A6 已经把 execution 定为外部只读可见字段。

> ✅ **裁决（§13-D5）：本节形状全盘采纳，并与另外两段一起定稿成「错误码三件套」**：
>
> | 段 | 形状 |
> |---|---|
> | scheduler RPC | `ErrExecutionFenced → codes.PermissionDenied → Rust `ExecutionFenced`，**永不重试**；与 `GenerationConflict → Aborted`（重读再试）**严格分开** |
> | **node → gateway** | **412** + `x-agentenv-refusal: sandbox_execution_superseded`（**内部信号**，node 侧接收端见 `_design-phase3-node.md` §3.7）|
> | **gateway → 客户端** | **409** + `code=sandbox_execution_superseded`（本节形状）|
>
> 🔴 **硬约束不变**：任何一环**都不许用 404**（平台据此重建工作区），
> 也**不许复用 410**（node `/proxy` 已用它表示 not proxyable，`src/api/proxy.rs:869-872`）。

### 6.4 给 agent-platform 看的语义（本轮平台不改，但语义现在必须定对）

| 平台看到 | 含义 | 平台应当怎么做 | 🔴 绝不能怎么做 |
|---|---|---|---|
| **404**（`GET`/`SetTimeout` 等） | 三义（不存在 / 落错节点 / 无 binding）| 不作判断 | 不能据此重建 |
| **404**（`POST /sandboxes/{id}/resume`）| 登记表可读且无此行 = 沙箱真没了 | **唯一**可授权重建工作区的信号（**语义不变**）| —— |
| **409 + `code=sandbox_execution_superseded`** | **数据面**：你这条连接/请求打到了一个已被取代的化身。沙箱**活着**，只是不在那台上 | 丢弃这条连接，**重新解析后重试**（对 WS 即重连）| 🔴 **绝不可读成"沙箱没了"**；也不该无限快重试，退避 |
| **409（其它 / 无该头）** | 既有语义：状态冲突（如 resume 撞 running）| 退避重试（现状不变）| —— |
| **503** | 中央看不了 / 唯一能服务的节点不肯服务 | 退避重试 | 不能据此重建 |
| **连接被对端关闭（无状态码）** | 可能是化身撤销（§3.3），也可能是普通网络断 | 重连（与今天处理网络断一致）| —— |

🟢 **本轮平台零改动即可正确工作**：平台的 aenv Client 只走控制面（`client.go` 全部方法），
A5 只拒数据面 ⇒ 平台永远见不到这个 409。数据面消费方是 agent-worker → envd，
它对 409 的既有行为是报错并重连，与期望处置一致。

---

## 7. 「拒绝」而非「改道」：现状差在哪、怎么补

任务书 A5 验收判据逐字：**"新化身起来后，打向旧化身的请求被拒（非仅仅改道）"**。

| 维度 | 现状 | 是"收敛"还是"拒绝" | 补法 |
|---|---|---|---|
| 每请求重解析 | ✅ 有。`handleProxy` 每次都调 `LookupNode`（`server.go:226`），**没有任何路由缓存** —— 与 e2b PR #2636/#2315 把路由缓存刻意删成实时查 catalog 是同一形态 | **收敛** | 保留 |
| 解析结果正确性 | ❌ 到达顺序仲裁（§2.2）| **连收敛都不成立** | S2 + S3（roster 带化身 + v7 大者胜）|
| 请求被拒 | ❌ 零拒绝。旧化身若被命名，拿到全量流量 | —— | 闸 1（§3.1）。🔴 **但见下方裁决块：本阶段闸 1 的真阳性集合接近空集** |
| 拒绝被证明有效 | ❌ 无从判断"node 校验了还是忽略了" | —— | 闸 2（§3.2）+ `node_silent` 指标 |
| 已建立的长连接 | ❌ 永不重估（§2.4）| **既不收敛也不拒绝** | 闸 3（§3.3）⏸️ **本轮不做（D7）** |

> ### 🔴 ✅ 裁决 A5-U3（2026-08-19 主 agent）：本节的"一句话"要照实改写，不许粉饰
>
> 原稿写的是"A5 = 把答案变成权威推导的 + 让答错时的落点自己拒绝 + 让已在路上的也重新问一次 + 让前三条可被证伪"。
> **后两半在本轮都要打折**，改成：
>
> **A5 在本阶段 = ① 把路由答案变成权威推导的（S1/S2/S3，这是价值主体）
> + ② 让这件事可被证伪（闸 2 的回声 + 跨服务指标对账）
> + ③ 闸 1 装配好但真阳性预期为 0（为 B4/B6 上膛）。
> ④ 已建立的长连接本轮不管（闸 3 推迟）。**
>
> 🔴 **为什么闸 1 的真阳性≈0**：`lookupNode` 的五个出口里，**路由目标与 expect 永远来自同一条记录/同一次心跳/同一行**
> （`_design-phase3-scheduler-a5.md` §2.1 逐出口摊开）⇒ "中央知道一个更新的化身却仍然把流量路由给旧节点"
> 这个组合在本阶段**构造不出来**。剩下的 `live ≠ expect` 只可能是"节点比中央新"，
> 而那是**合法的同机换代**，按裁决 A5-U4 必须放行。
>
> 🔴 **于是"打向旧化身的请求被拒（非仅仅改道）"这条判据在 gateway 侧只能兑现成"打向旧化身的请求不再产生"** ——
> 任务书 §3 的 A5 行判据已按此重写（§10.3 A5-U3）。
> **拦截飞行中的旧化身破坏性写，是 A3（SQL 事务内 fencing）与三条夺权路径清 execution 的职责，不是 A5 的。**

---

## 8. `GET /v2/sandboxes` 聚合端点

**结论：受影响，且影响是一个既有的、当前不可见的缺陷。**

- 它**不走 `LookupNode`**：`handleClusterList` 先 `ListNodes`（`cluster_list.go:74`），
  再对**每一台** node 扇出 `fetchNodeClusterList`（`cluster_list.go:132-147` / `:172-209`），
  然后合并。⇒ 化身轴在这条路上完全不存在。
- 双活时**两条行都会回来**（A 的旧化身 + B 的新化身，同一个 `sandboxID`）。
- 去重逻辑：`sortListedSandboxes`（`cluster_list.go:234-241`）按 `StartedAt` 降序、
  同秒再按 `SandboxID` 升序；`dedupListedSandboxes`（`:243-260`）keep-first。
- 🔴 **两条行的 `startedAt` 与 `sandboxID` 完全相同**：`startedAt` 来自
  `started_at(m.created_at)`（`src/api/impls/sandbox.rs:93` / `:122` / `:200`），
  而 `created_at` **只在 fork 时重置**（`src/orchestrator/service.rs:704` 是唯一赋值点），
  resume 沿用持久化下来的值。⇒ 比较函数两个方向都返回 `false`，
  而 `sort.Slice` **不稳定** ⇒ **胜者不确定，同一集群连续两次调用可能给出不同的那一行**
  （状态、endAt、metadata 都可能来自旧化身）。
- 那段 TODO 自己就写了正解（`cluster_list.go:248-249` 逐字）：
  > `When sandbox migration is supported, replace this "keep first" fallback with a deterministic winner based on authoritative ownership or versioning.`
  **A1 的 ExecutionID 就是那个 versioning。**

> ✅ **裁决（§13-D9）：本节的修复并入本轮 A5/A6，不单独排期。**
> 一句话理由：这是一个**独立于化身的既有缺陷**（双活时 `startedAt` 与 `sandboxID` 全同 ⇒ `sort.Slice` 不稳定 ⇒ 胜者随机），
> 但**修复手段就是 ExecutionID** —— `cluster_list.go:248-249` 那条 TODO 自己写的正解就是它；分开做等于把同一处代码改两遍。

**A5/A6 的处置（✅ 已裁决：本轮做，成本极低）**：

1. `listedSandbox`（`cluster_list.go:23-36`）加 `ExecutionID string \`json:"executionID,omitempty"\``
   —— 否则 node 加的字段会被这个 DTO **静默吃掉**（见 §9）。
2. 去重比较键从 `sandboxID` 变成 `(sandboxID)`，冲突时**按 ExecutionID 字典序取大**
   （UUID v7 ⇒ 后铸造者胜），两边都为空则退回 keep-first。
3. 🔴 **重复本身要计数**：`agentenv_gateway_cluster_list_duplicate_total{resolution="by_execution"|"keep_first"}`。
   今天重复是**被静默吞掉**的 —— 这正是方法论 §8.4 禁止的"无声收窄"，
   而它吞掉的恰好是"集群里有双活"这个最值得知道的事实。
4. ⚠️ **与 A4 的交叉点**：这个端点是 **all-or-nothing** 的
   （`cluster_list.go:84-96`，README:155：任一 node 失败整体失败）。
   A4 收窄 node 的用户级 REST 时，**必须保证 gateway 到 node 的这条 `GET /sandboxes`
   仍然被允许**，否则整个集群列表直接 502。→ 列入 §13 给主 agent 对账。
   ✅ **裁决（§13-D6）：A4 必须放行 `GET /sandboxes`（含 `GET /v2/sandboxes`）** ——
   已写进 `_design-phase3-node.md` §3.3 的 A4 豁免清单（那份原先只列了 `/health`）。
   🟡 **只豁免只读 GET**，`POST /sandboxes` 仍在收窄范围内。

`GET /sandboxes`（v1）走同一段代码，同样处置。

---

## 9. A6 的 gateway 部分（外部只读暴露 execution）

> ### 🔴 归属定稿（2026-08-19）：**本节只管 gateway 那三个 DTO，node 那半不在本文**
>
> | 归谁 | 做什么 |
> |---|---|
> | **impl-node**（[`_design-phase3-node.md`](_design-phase3-node.md) **§3.8**）| `src/api/openapi.yml` 的改动 + `make agentenv-server` 重新 codegen + 响应体字段：`SandboxDetail`（`GET /sandboxes/{id}`）、`Sandbox`（resume / create / connect / fork）、🔴 **`ListedSandbox`（`GET /sandboxes`、`GET /v2/sandboxes`）** |
> | **impl-gateway**（本节）| **只**改 gateway 自己拼的三个 DTO：`listedSandbox`、`registrySandboxItem`，以及 `/v2/sandboxes` 的去重 —— **别把 node 新加的字段吃掉**，也别去动 node 的 openapi |
>
> 🔴 **依赖方向**：gateway 的 `listedSandbox` 是**照着 node 的 `ListedSandbox` schema 抄的**，
> node 那边不加，gateway 这边加了也恒空。两种失败（node 漏加 / gateway 漏加）**症状完全一样：字段恒空、零报错** ——
> 所以两侧各配一发测试（node T-A6-1 / 本文 §11.6）。
>
> 🟡 **字段名以 node 的 openapi 为准：`executionID`**（对齐同一份 schema 里既有的
> `templateID` / `sandboxID` / `clientID` 惯例，不是自创风格）。gateway 的 Go tag 照抄这个字符串。

| 端点 | 谁拼的响应 | gateway 要改吗 | 说明 |
|---|---|---|---|
| `GET /sandboxes/{id}` | **node**（控制面路径直通，`upstreamTargetPath` 原样转发，`server.go:682-687`）| 🟢 **零改动** | node 在自己的响应体里加 `executionID` 即可（`SandboxDetail`，归 impl-node，node 设计 §3.8），gateway 是透明反代 |
| `POST /sandboxes/{id}/resume` | **node** | 🟢 **零改动** | 同上。⚠️ 唯一注意：`recordAssignmentFromResponse`（`server.go:462-503`）在 `locationNeedsAssignment` 为真时会**缓冲并重写响应体**，但它保持 body 内容不变，只重设 Content-Length ⇒ 新字段能穿过去 |
| `GET /sandboxes` / `GET /v2/sandboxes` | 🔴 **gateway 自己的 DTO** `listedSandbox`（`cluster_list.go:23-36`），`json.Decode` 到结构体 ⇒ **未知字段直接丢弃** | 🔴 **必须改**：加 `ExecutionID`（`json:"executionID,omitempty"`）| 不改的话，node 明明报了，**运维在最可能用来发现双活的那个端点上却看不到** —— 而且没有任何报错。🔴 **前置**：node 必须先给 `ListedSandbox` schema 加上该字段（node 设计 §3.8.2），否则这里改了也恒空 |
| `GET /registry/sandboxes` | 🔴 **gateway 自己的 DTO** `registrySandboxItem`（`registry_list.go:25-38`），从 `RegistrySandbox` proto 逐字段搬 | 🔴 **必须改**：proto 加 `execution_id`（S7）+ DTO 加 `ExecutionID` | 这是唯一能按行看到化身的地方。Agent-Console 本轮不动（用户已裁定），但字段是 additive，Console 哪天要用就有 |
| `GET /nodes` / `GET /nodes/{id}` | scheduler 观测 / node 直通 | 🟢 零改动 | 化身是沙箱的属性不是节点的 |

🔴 **A6 的红线**：以上全部是**响应字段**。
`registryListQueryParams`（`registry_list.go:59-64`）那个封闭参数集**不许**加 `executionID` 过滤——
一旦能按化身过滤，下一步就有人拿它当入参了。

---

## 10. 配置与回退（必须配置级）

```json
"gateway": {
  "routing": {
    "execution_fencing": "enforce"
  }
}
```

| 键 | 取值 | 含义 |
|---|---|---|
| `gateway.routing.execution_fencing`（env `GATEWAY_ROUTING_EXECUTION_FENCING`）| `off` | 🔴 **完整回退**：行为与改造前**逐字节一致** |
| | `observe` | 🔴 **不下发 expect 头** + 回程比对 + 全量指标，**真正零拒绝**（客户端拿不到 409）。集群验证期用它证明覆盖率与误判率。裁决 A5-U5，见下 |
| | `enforce` | 默认。闸 1 + 闸 2 生效 |
| ~~`gateway.routing.execution_revalidate_interval`~~（env `GATEWAY_ROUTING_EXECUTION_REVALIDATE_INTERVAL`）| duration，`"0s"` = 关 | 闸 3 的周期。🔴 ✅ **裁决 D-7：闸 3 本轮不做 ⇒ 这个键本轮也不加**（等真正实现撤销器时再引入，免得留一个配了也没人读的键）|

> 🔴 ✅ **裁决 A5-U5（2026-08-20 主 agent）：`observe` 不下发 expect 头。**
>
> **原表述（自相矛盾，保留作问题陈述，不抹掉）**：
>
> > | | `observe` | 下发 expect 头 + 回程比对 + 全量指标，但**永不拒绝**。集群验证期用它证明覆盖率与误判率 |
>
> **这两条互斥**：expect 头一旦下发，node（§3.1 闸 1）就**真的会**回 412，
> 而 gateway **撤不回一个已经发生的拒绝** —— 它只能按 §6 把 412 翻成 409 交给客户端
> （不翻就是把内部信号漏到外面，§6.3 明令禁止）。
> 于是"永不拒绝"在 `observe` 下**不成立**，总 runbook 步骤 10「先 observe 跑一轮」
> 那句"验证接线而不影响用户"的**意义随之落空**。
>
> **裁决后的三态语义**：
>
> | mode | 闸 1（下发 expect，node 拒）| 闸 2（回程比对 node 回声）| 客户端可见的拒绝 |
> |---|---|---|---|
> | `off` | ❌ 不下发（且**剥掉**客户端塞的头，见下）| ❌ 不读回声 | 无 |
> | `observe` | 🔴 **❌ 不下发** | ✅ **照常比对 + 计数 + 打日志** | 🔴 **无（真正零拒绝）** |
> | `enforce` | ✅ 下发 | ✅ 比对 + 拒绝 | 409 |
>
> 🔴 **观测性一点没少**：node 的回声是**无条件**的（node 设计 §3.7：放行与拒绝两种响应都带
> `x-agentenv-execution-id`），闸 2 不靠闸 1 铺路。`observe` 照样算得出
> `refused_echo` / `unfenced_node_ahead` / `unfenced_node_silent` / `unfenced_no_authority`
> 的全部水位 —— 步骤 10 的三条判据一条不缺。
>
> ⚠️ **唯一少掉的是 `refused_preflight`**：它按定义只可能由 node 产生，而 `observe` 没给 node 上膛。
> 该系列在 observe 期**恒为 0 是必然而非证据**；🔴 **非 0 说明有别的东西在下发 expect 头**
> （另一台还停在 `enforce` 的 gateway、请求飞行中翻了开关、中间盒重放），要当异常查。
>
> ⚠️ **代价照实登记，不粉饰**：`observe` 那一轮**不再实测「下发 → node 拒」这条链**，
> 它第一次真正跑起来是在翻 `enforce` 的那一刻。缓解不是零：
> 回声与闸 1 的比对**在同一次 node 滚动里一起上**（node 设计 §3.7，步骤 3 只滚一次），
> 所以 `unfenced_node_silent == 0` 就是"全集群都装了 A5 接收端"的证据；
> 且 node 侧 `agentenv_proxy_execution_fencing_total{decision="pass_no_expect"}`
> 在 observe 期应≈数据面全量、翻 `enforce` 后应≈0 —— **这对指标就是闸 1 接线的对账**。
> 接受这个代价的理由：`observe` 的第一职责是"不影响用户"，
> 一个会给用户 409 的 `observe` 连自己的名字都对不上。
>
> 🔴 **保留的另一半**：node 的 412 + `x-agentenv-refusal` **任何 mode 下都不直通客户端**，
> 一律翻成 409 + `sandbox_execution_superseded`（§6.3）。改判后这条在 `observe` 下成为
> **纯防御分支**（不可达但保留）：不可达的翻译只值一次比较，缺了它则一旦别处产生 412
> 就把内部信号漏给客户端。实现见 `execution_fencing.go: fenceProxyResponse` 的注释，
> 钉在 `TestObserveModeStillTranslatesARogueNodeRefusal`。
>
> 🔴 **`off` / `observe` 都必须剥掉客户端塞的化身头**（`off` 的这条偏离是刻意的，不要"修"）：
> 开关只回滚 **gateway 自己**的 fencing，**不回滚 node 已装的闸**。
> 透传伪造的 expect 头 = 任何调用方都能让 node 拒绝任意请求；
> 在 `observe` 下更直接 —— 客户端伪造一个 expect 就能制造出 `observe` 承诺不会出现的 409。
> 钉在 `TestGatewayStripsClientSuppliedExecutionHeaders`（四例，含 `observe` / `off`）。

> 🔴 **命名裁决 N2（2026-08-19 主 agent）：这个开关叫 `gateway.routing.execution_fencing`，不叫 `gateway.execution_fencing`。**
> 本轮一共有**三个**作用面不同的开关，名字必须自带作用域：
>
> | 开关 | 类型 | 关掉什么 |
> |---|---|---|
> | `scheduler.registry.write_fencing` | bool（默认 `true`）| A3 的两条 SQL 谓词（`_design-phase3-scheduler.md` §5.2）|
> | `scheduler.routing.execution_arbitration` | `off\|observe\|enforce`（默认 `enforce`）| binding 仲裁与 `LookupNode` 应答（`_design-phase3-scheduler-a5.md` §11）|
> | `gateway.routing.execution_fencing` | `off\|observe\|enforce`（默认 `enforce`）| **本节** —— gateway 路由层拒绝 |
>
> ✅ **三个分家是刻意的，只改名、不合并**：必须能分别关，共用会让一次止血顺手关掉另一半，而那一半的失效是无声的。
> 🔴 **对运维的直接后果**：「fencing 关了吗」这个问题在本轮**没有单一答案**，必须点名是哪一个 ——
> 总 runbook（任务书 §6）的症状→开关对照表就是为此而写。

> ✅ **裁决（§13-D8）：默认值就是 `enforce`，但发布时先跑一轮 `observe`。**
> 一句话理由：P1 无生产包袱，默认值该指向我们想要的终态；而"先 observe 一轮"是**发布纪律**，不是默认值 ——
> 把保守写进默认值，等于永远有集群停在 observe 上没人翻，且没人知道。
> 🔴 **`observe` 那一轮的通过判据**：`refused_*` 在健康集群上**恒为 0** 且 `unfenced_*` 落在 §1.2 裁决块预期的水位；
> 非 0 就是**先查再开**，不是"翻了再说"。
> ⚠️ **裁决 A5-U5 后要分开读**：`refused_echo == 0` 是真判据；`refused_preflight == 0` 是**必然**（observe 不盖章
> ⇒ node 无从拒），拿它当"闸 1 正常"的证据是假绿，它非 0 反而说明有别的东西在下发 expect 头。

🔴 **回退不许依赖新写的回滚逻辑**（任务书 §6）：
`off` 必须是 `decideFencing` **函数入口的第一行 early return**，
且 `off` 时**不 spawn 撤销 goroutine、不改任何 header、不读任何响应头**。
不允许把开关判断散进 5 个调用点 —— 那样"关掉"本身就成了一段需要被验证的新代码。
配一发测试钉死（§11.5 `TestFencingOffMatchesLegacyBehaviour`）。

配置解析落点：`GatewayConfig`（`services/shared/config/config.go:351-362`）加嵌套
`Routing GatewayRoutingConfig{ ExecutionFencing string }` +
它的 `UnmarshalJSON`（`:364-412`）+ env 覆盖（`:529-532` / `:614-627` 的既有模式）。
⚠️ duration 必须走字符串解析（该文件对 `request_timeout` 的处理 `:414-437` 就是先例：
数字值要报错而不是当纳秒）。

**发布顺序**（本项自己的依赖方向；🔴 **执行以
[`_impl-plan-control-plane-phase3.md` §6](_impl-plan-control-plane-phase3.md#6-总发布-runbook-n5三份设计的顺序合成一条)
的 13 步线性 runbook 为准** —— 命名裁决 N5）：
1. 🔴 **全集群 node 升到唯一一次那版镜像**（A1 + A4 gate + **A5 接收端** + A6 响应字段，后三者惰性）；（= 总 runbook 步骤 3）
   🟡 **2026-08-19 追认：node 只滚一次**，A5 的接收端从这一步就在位 —— 它在 gateway 下发 expect 头之前恒放行
2. scheduler 升到带 S1–S5 的版本，观察 `unfenced` 指标降到预期水位；（= 步骤 4–6）
3. gateway 以 `off` 上线并开始注入 A4 的控制面 token；（= 步骤 8）
4. **启用 A4**：把 token 写进 node 的挂载文件（**热生效，不滚 DaemonSet**）；（= 步骤 9）
   🔴 **这一步在原稿里缺失，是三份设计合流时补上的**；它属于 A4 不属于 A5，本设计只需知道"它在第 5 步之前"
5. gateway 翻 `observe`，确认 `refused_echo` 恒 0 且 `unfenced_node_silent` 为 0（**探针自证**：
   若非 0，说明化身语义有误或还有 node 没装 A5，**先查再开**）；（= 步骤 10）
   🔴 **裁决 A5-U5 后这一步的判据要按新语义读**：`refused_preflight` 在 observe 期**恒 0 是必然**
   （没下发 expect ⇒ node 无从拒），**不能**拿它当"闸 1 正常"的证据；它非 0 反而是异常（见 §10 裁决块）。
   有意义的三条是 `refused_echo == 0`、`unfenced_node_silent == 0`、
   `unfenced_no_authority` 与 scheduler 的 `lookup_execution_authority_total{authority="unknown"}` 对得上。
6. 翻 `enforce`。（= 步骤 11）

---

## 11. 测试计划 + 变异验证

> 全部落在 `services/gateway/internal/`，沿用既有的 `stubSchedulerClient`（`server_test.go:27-42`）
> 与 `newTestServer`（`:163-179`）+ `testServerOption` 模式（新增 `withExecutionFencing(mode)`）。
> 命名沿用仓内既有风格（`TestUnreadableRegistryIsFiveOhThreeAndNotFourOhFour`）。
>
> 🟢 **本文列出的全部 gateway 测试都不需要数据库**（stub 掉 scheduler client 即可），
> `make -C services test` 就能跑全。⚠️ 但 §5 那几条 scheduler 侧要求（S1/S2/S5）的测试
> **必须走 `make -C services test-with-postgres`** —— 不带库时 `scheduler/internal/registry`
> 的 SQL 测试是 **skip 而不是 fail**（125 个 skip 全报成 pass），
> 在那里做变异验证会拿到假绿。

### 11.1 闸 1 / 闸 2 的拒绝用例

| 测试函数 | 断言 |
|---|---|
| `TestDataPlaneRequestCarriesTheExpectedExecutionHeader` | `authority=REGISTRY` 时，落到 node 的请求带 `x-agentenv-expect-execution-id: E` |
| `TestDataPlaneRequestRefusesWhenNodeEchoesAnOlderExecution` | node 回 `x-agentenv-execution-id: E'`（≠E）⇒ 409 + `code=sandbox_execution_superseded` + `refusedBy=gateway`；**node 的响应体不得出现在客户端响应里** |
| `TestDataPlaneRequestTranslatesNodePreconditionRefusal` | node 回 412 + `x-agentenv-refusal: sandbox_execution_superseded` ⇒ gateway 改写成标准 409 形状，`refusedBy=node` |
| `TestControlPlaneRequestIsNeverRefusedOnExecutionMismatch` | 同样的化身不匹配，`POST /sandboxes/{id}/pause` **和** `/resume` 都必须 200 直通，且**不带** expect 头 |
| `TestPlacedAndPinnedSandboxesAreNeverFenced` | `PLACED` / `PINNED`（`authority=PENDING`）⇒ 不下发 expect，不拒（防"自动唤醒被自己拒死"）|
| `TestGatewayStripsClientSuppliedExecutionHeaders` | 客户端塞 `x-agentenv-expect-execution-id: 伪造值` ⇒ 到 node 的是 gateway 自己的值；`authority=UNKNOWN` 时该头**必须不存在**（而不是透传伪造值）。🔴 **裁决 A5-U5 追加 mode 轴**：`observe` / `off` 下即使 `authority=REGISTRY` 该头也**必须不存在** —— 否则客户端伪造一个 expect 就能造出 `observe` 承诺不会有的 409 |
| 🔴 `TestObserveModeCountsTheMismatchWithoutRefusing`（**裁决 A5-U5 改写自** `TestObserveModeCountsTheRefusalButDoesNotRefuse`）| `observe` + node 活化身比权威化身旧 ⇒ ① 客户端拿到 **200 + node 原样响应体**；② 到 node 的请求**不带** expect 头；③ `refused_echo` +1；④ `refused_preflight` **不动**；⑤ Warn 日志按字段带全 `sandbox_id`/`node_id`/`expected_`/`observed_execution_id`/`refusal_code`/`refused_by`。🔴 stub node **实装 node 侧的闸**（收到更新的 expect 就回 412），所以"observe 又盖章"这个变异是以**客户端 409** 的形式让它红，而不是只差一个头 |
| 🔴 `TestObserveModeStillTranslatesARogueNodeRefusal`（**裁决 A5-U5 新增**）| `observe` 下 node **无端**回 412 + `x-agentenv-refusal` ⇒ 客户端拿到 **409**（不是 412、不含 node 响应体、`refusedBy=node`）+ `refused_preflight` +1。这是"纯防御分支"的执行版：改判后它在正常接线下不可达，**但不许当死代码删** |
| `TestHostRoutedDataPlaneIsFencedLikeHeaderRouted` | host 入口（`{port}-{id}.{domain}`）与 header 入口行为一致 |

### 11.2 🔴 探针自证（方法论 §8.3：先用必然失败的对照输入验能力）

| 测试函数 | 作用 |
|---|---|
| `TestMatchingExecutionPassesThrough` | **对照组**：化身一致 ⇒ 200 + 响应体逐字透传。没有它，一个"拒绝一切"的实现能让 §11.1 全绿 |
| 🔴 `TestNodeAheadOfTheControlPlanePassesThrough`（**裁决 A5-U4 新增**）| **第二个对照组**：node 回声 **`> expect`**（同机 pause→resume 刚换代，中央落后一个心跳）⇒ **200 放行** + `…{decision="unfenced_node_ahead"}` +1。没有它，一个"回声不等就拒"的实现会让上一行全绿，而它在生产上会把**每一次同机 pause→resume** 变成 409 |
| `TestUnfencedRequestIsCountedNotSilentlyAllowed` | `authority=UNKNOWN` ⇒ 放行**且** `…execution_fencing_total{decision="unfenced_no_authority"}` +1 |
| `TestNodeWithoutEchoHeaderIsCountedAsUnfenced` | node 不回声 ⇒ 放行 + `decision="unfenced_node_silent"` +1。**这是"闸 1 被回退/node 没装配"的唯一可观测信号** |

### 11.3 🔴 钉死"拒绝码不是 404"（防工作区蒸发的唯一机械保证）

```go
// TestFencingRefusalIsNeverFourOhFour
//
// 🔴 这一发的存在理由不是"409 好看"，而是：主仓 agent-platform 判"沙箱没了、
// 可以重建工作区"的唯一依据就是 404（apps/agent-platform/internal/sandbox/aenv/
// client.go:58-71）。任何一次手滑把 fencing 拒绝改成 404，代价是用户工作区蒸发
// 且全程不报错。这一发是这条不变式在 CI 上的全部保证。
func TestFencingRefusalIsNeverFourOhFour(t *testing.T)
```

断言三层，缺一不可：

1. **枚举所有拒绝路径**（闸 1 翻译 / 闸 2 回程 / 未来新增的任何 reason），
   表驱动断言 `code == 409` 且 `code != 404` 且 `code != 410` 且 `code != 503`；
2. **对拒绝构造函数本身**做单元断言（`writeStaleExecutionRefusal` 返回的 status），
   这样即使有人换了调用点，函数本身仍被钉住；
3. `TestSchedulerNotFoundStillMapsToFourOhFour` —— **反向对照**：
   `codes.NotFound` 必须**仍然**是 404（复用既有 `TestResumeOfAnUnknownSandboxIsFourOhFour`
   的形状，`server_test.go:2489-2504`）。没有这一发，
   "把所有 404 都改成 409"这个变异会让第 1 层假绿。

### 11.4 闸 3（撤销器）　—— ⏸️ **裁决 D-7：本轮不实现 ⇒ 本小节整块推迟**

> 🔴 **本轮不要写这几发，也不要动 `TestRequestContextNotCanceledWhenStreamingCancelCalled`**
> （§3.3 的裁决块已说明：那个测试守的是"路由超时不许切断流"，在闸 3 真正落地时才按意图重写）。
> 下表原样保留，实现闸 3 的那个 PR 直接照做。

| 测试函数 | 断言 |
|---|---|
| `TestLongLivedDataPlaneConnectionIsRevokedWhenExecutionChanges` | 建链时 E，第二次 lookup 回 E' ⇒ 连接被关闭 |
| `TestLongLivedConnectionSurvivesSchedulerUnavailable` | 🔴 **反向守卫**：重估 lookup 回 `Unavailable` / `NotFound` / 超时 ⇒ 连接**必须存活** |
| `TestLongLivedConnectionSurvivesUnknownAuthority` | 重估回 `authority=UNKNOWN` ⇒ 存活 |
| `TestControlPlaneAndShortRequestsSpawnNoRevalidator` | 非长连接 / 控制面不起 goroutine（用 lookup 调用计数断言）|
| `TestStreamingProxyContextIsNotCutByRoutingTimeout` | 🔴 **改写自** `TestRequestContextNotCanceledWhenStreamingCancelCalled`（`server_test.go:1247-1259`）。原测试断言"cancel 是空操作"，闸 3 需要真 cancel ⇒ **按意图重写，不许删**：routing deadline 到期后，流的 context 仍未取消 |
| `TestStreamingProxyContextIsCancelableForRevocation` | 新能力：撤销 cancel 能真正切断 |

### 11.5 回退

| 测试函数 | 断言 |
|---|---|
| `TestFencingOffMatchesLegacyBehaviour` | `mode=off` ⇒ 不发任何新头、不读响应头、不起 goroutine、不拒任何东西。用"lookup 返回化身完全不匹配"作输入，断言 200 直通 |

### 11.6 聚合端点

| 测试函数 | 断言 |
|---|---|
| `TestClusterListPrefersTheCurrentExecutionOnDuplicates` | 两台 node 各回一条同 `sandboxID`、**同 `startedAt`** 的行，execution 不同 ⇒ 稳定返回 v7 较大的那条（跑 20 次结果一致，钉死 §8 的 `sort.Slice` 不确定性）|
| `TestClusterListCountsDuplicates` | 重复被计数而不是静默吞掉 |
| `TestClusterListExposesExecutionID` | node 回的 `executionID` 出现在合并结果里（防 DTO 静默丢字段）。🟡 **本发只能证明 gateway 那半**：stub 的上游是测试自己拼的 JSON，**证明不了 node 真的会报** —— node 那半由 `T-A6-1`（node 设计 §3.8.5）覆盖，两发缺一不可 |
| `TestRegistryListExposesExecutionID` | 同上，`/registry/sandboxes` |

### 11.7 变异验证清单（把修复退回去，指定测试必须 FAIL）

| # | 变异 | 必须 FAIL 的测试 |
|---|---|---|
| **M1** | 任务书指定的那发：`decideFencing` 退回"只按 sandbox 路由"（永远返回 `{send:false, enforce:false}`）| `TestDataPlaneRequestCarriesTheExpectedExecutionHeader`、`TestDataPlaneRequestRefusesWhenNodeEchoesAnOlderExecution`、`TestDataPlaneRequestTranslatesNodePreconditionRefusal` |
| **M2** | 拒绝码 409 → 404 | `TestFencingRefusalIsNeverFourOhFour`（三层全 FAIL）|
| **M3** | 拒绝码 409 → 503 | `TestFencingRefusalIsNeverFourOhFour` 第 1 层 |
| **M4** | 闸 2 fail-open（不比对回声，直接放行）| `TestDataPlaneRequestRefusesWhenNodeEchoesAnOlderExecution` |
| **M5** | 不剥客户端头 | `TestGatewayStripsClientSuppliedExecutionHeaders` |
| **M6** | 撤销器改成 lookup 出错也撤销 | `TestLongLivedConnectionSurvivesSchedulerUnavailable` ⏸️ **随闸 3 推迟（裁决 D-7）** |
| **M7** | `PENDING` 也下发 expect | `TestPlacedAndPinnedSandboxesAreNeverFenced` |
| **M8** | 控制面也 enforce | `TestControlPlaneRequestIsNeverRefusedOnExecutionMismatch` |
| **M9** | 去掉 `unfenced` 计数 | `TestUnfencedRequestIsCountedNotSilentlyAllowed`、`TestNodeWithoutEchoHeaderIsCountedAsUnfenced` |
| **M10** | 聚合去重退回 keep-first | `TestClusterListPrefersTheCurrentExecutionOnDuplicates` |
| **M11** | `off` 模式仍下发 expect 头 | `TestFencingOffMatchesLegacyBehaviour`、`TestGatewayStripsClientSuppliedExecutionHeaders/off_…` |
| **M12** | 闸 2 的**有序**比对退回**等值**比对（回声 ≠ expect 就拒）| `TestNodeAheadOfTheControlPlanePassesThrough`、`TestIncarnationsAreComparedAfterBeingLowerCased` |
| 🔴 **M13**（**裁决 A5-U5 新增**）| `observe` **也下发** expect 头 | `TestObserveModeCountsTheMismatchWithoutRefusing`（以客户端 409 的形式红）、`TestGatewayStripsClientSuppliedExecutionHeaders/observe_…` |
| 🔴 **M14**（**裁决 A5-U5 新增**）| 闸 2 改回"没下发就不读回声"（`fenceProxyResponse` 的入口从 `!plan.compare` 退回 `!plan.stamp`）| `TestObserveModeCountsTheMismatchWithoutRefusing`、`TestObserveModeStillTranslatesARogueNodeRefusal` —— 这是**把 observe 变成哑巴**的那条变异：客户端表现不变，指标与日志全没了 |

🔴 **M2 是清单里唯一一条"变异后果不是测试红、而是生产事故"的**，
所以它对应的测试是三层的，且第 3 层（反向对照）专门防"整体改 404→409"这种能让前两层假绿的变异。

### 11.8 指标（新增，沿用 `agentenv_gateway_*` 前缀）

| 指标 | 标签 | 用途 |
|---|---|---|
| `agentenv_gateway_execution_fencing_total` | `plane`=data/control，`decision`=`enforced_pass`/`refused_preflight`/`refused_echo`/`unfenced_no_authority`/`unfenced_node_silent`/🔴 `unfenced_node_ahead`（**裁决 A5-U4 新增**：回声比 expect 新 ⇒ 放行）/`pending`/`off`/`observed` | 覆盖率与拒绝率。🔴 标签集封闭（照 `gatewaySandboxLocationLabel`，`metrics.go:169-183` 的"未知归 other"写法）|
| `agentenv_gateway_execution_revocation_total` | `reason`=`superseded`/`lookup_failed_ignored`/`unknown_authority_ignored` | 撤销器行为；`lookup_failed_ignored` 涨说明 scheduler 在抖，但没误伤 |
| `agentenv_gateway_cluster_list_duplicate_total` | `resolution`=`by_execution`/`keep_first` | 双活的直接信号 |

**健康集群上 `refused_*` 与 `duplicate_total` 都应恒为 0** ⇒ 它们直接可做告警，
且 `unfenced_*` 的绝对值就是**覆盖缺口的大小**（方法论 §8.4：无声收窄必须可见）。

---

## 12. 风险与未决缺口（不掩盖）

| # | 风险 | 严重度 | 现状/缓解 | 是否有残留 |
|---|---|---|---|---|
| **R-1** | **`UNKNOWN` 覆盖缺口**：从未 pause 过的沙箱无登记表行 ⇒ 不受保护 | 🟢 低 | §1.2 论证：无行 ⇒ 物理上单化身 ⇒ 无东西可 fence。**但这条论证依赖 A2 的 schema 决策** | ⚠️ 若 A2 不给 running 建行，缺口永久存在；大小由 `unfenced_no_authority` 指标量化 |
| **R-2** | **闸 2 无法撤回已送达的请求** | 🟡 中 | 设计上明确它是"检测 + 自证"不是主闸；主闸是闸 1 | ✅ 有残留：闸 1 失效（node 未装配/回退）时，第一个打到旧化身的请求**会被执行**，只是不返回给客户端。**副作用已发生** |
| **R-3** | **WS 撤销是 TCP 断，不是带码的拒绝** | 🟡 中 | ReverseProxy 升级后走 hijack，gateway 拿到的是裸字节流，无法合成 WS close frame | ✅ 有残留：任务书判据"被拒"对 WS 只能兑现成"被断"。客户端行为一致（重连），但**可观测性差** —— 只能靠 `revocation_total` 分辨"被撤销"与"网络断" |
| **R-4** | **node 忽略 expect 头 ⇒ 静默不设防** | 🔴 高 | 回声头兼作能力信号，落到 `unfenced_node_silent` | ✅ 有残留：**缓解不是消除**。🔴 发布纪律：gateway 翻 `enforce` 前必须确认全集群 node 已带 A1，否则是"部分覆盖 + 看着绿" |
| **R-5** | **合法 resume 期间的数据面请求被误拒** | 🟡 中 | 若 S5 满足（`resuming` 行的化身 = 认领者新化身），窗口很窄；若不满足，降级 `PENDING` ⇒ 窗口内不设防 | ⚠️ 二选一：要么误拒（客户端退避重试可自愈），要么窗口内不设防。**建议选后者**（`PENDING`），因为误拒发生在用户正在等唤醒的时刻，体验最差 |
| **R-6** | **gateway 信任 node 回声** | 🟢 低 | 不是新的信任边界（gateway 本来就把 node 的整个响应交给客户端）| ✅ 明确声明：**A5 fence 的是"陈旧"，不是"恶意"**。被攻陷的 node 可以回声任意值 |
| **R-7** | **HA（query-only）路径权威度** | 🔴 高 | query-only 副本有 registry reader（`service.go:129-136` 注释专门强调过这点），但**没有 placer**，且热路径 binding 命中根本不读 registry | ✅ 完全取决于 **S1**（binding 带化身）。S1 不做 ⇒ HA 模式下 A5 全线失效，且**本地单 scheduler 开发环境测不出来**（正是 outcome §1.2 订正②踩过的同一个坑）|
| **R-8** | **闸 3 的 RPC 放大** | 🟢 低 | N 条活跃长连接 / interval。200 条 @30s ≈ 7 rps，且打的是 query-only 副本的 binding 热路径 | 若量大，可加 per-sandbox 去重（多条连接共享一次 lookup）。**本轮不做**（不过度设计），但把 interval 做成可配以便应急 |
| **R-9** | **聚合端点 all-or-nothing × A4 收窄** | 🟡 中 | `GET /sandboxes` 扇出到每台 node，任一失败整体 502（`cluster_list.go:84-96`）| ⚠️ **跨设计依赖**：A4 必须放行 gateway → node 的这条只读 GET |
| **R-10** | **数据面自动唤醒会铸新化身** | 🟡 中 | `src/api/proxy.rs` 的 auto-resume 使得**一个数据面请求就能创造一个化身** | ⚠️ 这条超出 A5 范围（并发唤醒的仲裁是 A3 / 闸门 B 的事），但它解释了为什么 `PENDING` 不能 enforce。**记录在此以免被当成 A5 的洞** |

### ✅ 裁决对上表的更新（2026-08-19）

| # | 裁决后的状态 |
|---|---|
| **R-1** | 🔴 **缺口确认为永久**（D3：A2 不给 running 建行）。已按裁决写成显式的「已知覆盖缺口 + 为什么安全」（§1.2 的 ✅ 块），并由 `unfenced_no_authority` 计量 —— **不许再当成隐含假设** |
| **R-3 / R-8** | ⏸️ **随闸 3 一起推迟**（D7）。R-3 的"WS 只能被断不能被拒"本轮**不兑现**；这属于"已知、有意推迟"的残留，不是被解决的风险 |
| **R-5** | 🟢 **消解**：E-A 让 `resuming` 行带认领者的新化身（D4/S5）⇒ resume 窗口内**正常设防**，既不误拒也不裸奔。仅当登记表答不出权威化身时降级 `PENDING` 放行 |
| **R-7** | 🔴 **仍是最高优先级的落地风险**：S1 已裁决为"做"，但**必须在 HA / query-only 副本形态下验证** —— 本地单 scheduler 测不出它的失效 |
| **R-9** | 🟢 **消解**：D6 已裁决 A4 放行 `GET /sandboxes`，并写进 node 设计的 A4 豁免清单 |

---

## 13. 需要主 agent 裁决 / 跨设计对账的（✅ 2026-08-19 D1–D9 已全部裁决，见表后「裁决收口」）

| # | 事项 | 影响谁 |
|---|---|---|
| **D1** | 🔴 **S1（binding 带 execution）做不做**。不做 ⇒ A5 在数据面 ~99% 请求上是空转，且 HA 模式全线失效。这是 A5 的成败点，不是优化项 | scheduler 设计 |
| **D2** | 🔴 **S2/S3 的冲突仲裁规则**：`ReconcileNode` 从"到达顺序覆盖"改成"UUID v7 大者胜"。若 scheduler 侧另有方案（如冲突时回查登记表），两边必须一致 | scheduler 设计 |
| **D3** | **A2 的 schema 是否给 running 沙箱建行**（`mark_running` 今天不建行）。建 ⇒ A5 覆盖率 100%；不建 ⇒ 永久留 R-1 那个（可论证为安全的）缺口 | scheduler 设计 |
| **D4** | **S5：`resuming` 行的 `execution_id` 是认领者的新化身还是原化身**。决定 resume 窗口内是"可能误拒"还是"不设防"。**本文推荐后者（报 `PENDING`）** | scheduler 设计 |
| **D5** | **node 侧拒绝用什么码**。本文建议 412 + `x-agentenv-refusal` 作为**内部信号**，由 gateway 翻译成 409。🔴 硬约束只有一条：**不能是 404，也不能复用 node `/proxy` 已占的 410** | node 设计（A1/A4）|
| **D6** | **A4 必须放行 gateway → node 的 `GET /sandboxes`**，否则集群列表整体 502（R-9）| node 设计（A4）|
| **D7** | **是否本轮就做闸 3（长连接撤销）**。它是"被拒非仅仅改道"判据里最长命的那一类流量，但也是本设计里唯一需要在 gateway 起后台 goroutine 的部分。若要砍，砍它比砍闸 1/2 划算 | 主 agent |
| **D8** | **`execution_fencing` 默认值**：本文推荐 `enforce`（P1 无生产包袱），发布时先用 `observe` 跑一轮集群验证再翻。若倾向更保守，可默认 `observe` 一个发布周期 | 主 agent |
| **D9** | **§8 的聚合端点修复是否算进 A5/A6**。它是一个独立于化身的既有缺陷（`sort.Slice` 不确定），但**修复手段就是 ExecutionID**，分开做等于把同一处代码改两遍 | 主 agent |

### ✅ 裁决收口（2026-08-19 主 agent，逐条对应上表）

> 上表的问题陈述**一个字都不删**（它记录了"当初为什么是个问题"）；下表是最终裁决，**实现以下表为准**。

| # | ✅ 裁决 | 一句话理由 | 正文落点 |
|---|---|---|---|
| **D1** | 🔴 **做**（S1：binding 存储持久化 execution）。附加硬要求：**必须在 HA / query-only 副本形态下验证** | 它是 A5 的成败点不是优化项；本地单 scheduler 测不出该失效 | §5 裁决表、§12 R-7 |
| **D2** | **热路径 UUID v7 字典序大者胜** + 对反向覆盖打 **warn**（时钟回拨信号）；**登记表在被查询时是真相** | 热路径不能为每次冲突付一次 DB 读；v7 依赖墙钟，没有那条 warn 就发现不了回拨 | §5-S2/S3 |
| **D3** | **不建行** —— `BeginPause` 保持唯一建行者；缺口写成**显式的「已知覆盖缺口 + 为什么安全」** | 无行 ⇒ 物理上单化身 ⇒ 没东西可 fence；为一个可论证安全的缺口去换一张写入面更大的表不划算 | §1.2 ✅ 块、§12 R-1 |
| **D4** | **走 E-A 使 S5 成立**，resume 窗口内**正常设防**；仅当登记表答不出权威化身时报 `PENDING` 放行 | 数据面读路径 fail-open 可接受 —— **破坏性写已由 A3 在 SQL 层挡住** | §1.2 ✅ 块、§5-S5 |
| **D5** | **采纳**：node **412** + `x-agentenv-refusal` 作内部信号 → gateway **409** + `code=sandbox_execution_superseded`；🔴 任何一环**不许 404**、**不许复用 410** | 404 = 平台授权重建工作区；410 已被 node `/proxy` 占用（not proxyable） | §6.3 ✅ 块；node 侧 §3.7 |
| **D6** | **A4 必须放行 gateway → node 的 `GET /sandboxes`**（只读 GET，`POST` 不放行）| 否则 `cluster_list.go:84-96` 的 all-or-nothing 让集群列表整体 502 | §8 第 4 条；**已同步写进 node 设计 §3.3 的豁免清单** |
| **D7** | 🔴 **本轮不做闸 3**（长连接周期性撤销）| 不过度设计；闸 1/闸 2 是本轮价值主体。**残留缺口照实保留，标注"已知、有意推迟"** | §3.3 ✅ 块、§11.4、§12 R-3/R-8 |
| **D8** | **默认 `enforce`**，发布时**先跑一轮 `observe`** | 默认值该指向终态；"先 observe"是发布纪律不是默认值 | §10 ✅ 块 |
| **D9** | **并入本轮**（§8 聚合端点的非确定性排序缺陷）| 修复手段就是 ExecutionID，`cluster_list.go:248-249` 的 TODO 自己写了正解；分开做要改两遍 | §8 ✅ 块 |
