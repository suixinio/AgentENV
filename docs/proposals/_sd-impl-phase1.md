# 阶段 1 实施规格：路由投影升格为权威记录 ＋ gateway 直读

> **本文是可执行规格，不是提案。** 读它的人不需要回去读
> [`2026-08-20-service-decomposition.md`](2026-08-20-service-decomposition.md) §7 阶段 1
> 或 [`2026-08-20-module-responsibilities.md`](2026-08-20-module-responsibilities.md) D10/D11 ——
> 需要的结论都抄进来了，需要纠正的也都标出来了。
>
> 🔴 **本文推翻了母提案的三处事实判断**（§1），其中两处直接决定这一阶段能不能成立。
> 与母提案冲突时**以本文为准**，并把冲突登记回母提案。
>
> 🔧 **更新（2026-08-20 晚）：阶段 1 已在 dev 集群通过验收，记录在 §13。**
> 规格本体一字未改，但 §13 里有三件事会改变别的东西：
> ① **一个新缺陷 SD-D1** —— 把写侧开关 `off → on` 会在活着的沙箱上打出一段 404（§13.4），
> ⇒ 这一翻要当维护事件排期，**回滚方向反而是免费的**；
> ② **F4 的框架订正** —— 制造 F4 的是**写**开关，读开关既不制造也不加宽它（§13.5），
> ⇒ 要门控就门控写开关；
> ③ **射程边界**（§13.6）—— 七条没有被这一轮证明的东西，其中「真正的整机猝死」
> 在这套集群上目前**没有可用手法**（[`_sd-recon-env.md`](_sd-recon-env.md) §11.2）。
>
> 🔧 **第二轮补记（2026-08-20 夜）：§13.9。** F4 的心跳超时清扫已在集群上跑过一次真节点猝死，
> 🔴 **顺带把 F4 的严重度改了**：陈旧记录**不只是一条坏路由，它还挡住了修复**
> —— 一次本该成功的 resume 被路由到尸体上、返回 502。**凡描述 F4 处都按这个严重度改写**，
> 包括下面 §13.5 自己。制造猝死的可用手法（以及一条读起来像通过的错误手法）在
> [`_sd-recon-env.md`](_sd-recon-env.md) §11.6 ⇒ §13.6 第 1 条那条缺口**已经关闭**。

---

## 0. 三十秒版

母提案说阶段 1 是「①四件事 ＋ ②一件事，400–500 行含测试」。核查代码之后：

- ① 的第 1 点（「CREATE / FORK 不动」）**不成立**：CREATE 的投影写不带化身，
  FORK 的投影写**根本没有发生过**（响应体是顶层数组，gateway 的抽取函数解不了）。
- ① 的第 4 点（TTL）**漏了一条会让整件事失效的路径**：心跳对账脚本每 5 秒
  用 `PX 30s` 重写一次记录 —— 建时写的长 TTL 活不过一个心跳。
- ①.2（RESUME）**不是 Go 的一行改动**：resume 响应既无 sandbox-id 头也无化身头，
  而在集群当前的 `enforce` 仲裁下，一次不带化身的写会被 Lua 静默拒绝。
- 规模实际约 **1,450 行非测试 ＋ 1,330 行测试 ＋ ~800 行生成代码 ＋ ~180 行 YAML**，
  是提案估计的 6–8 倍（§10）。

真正的产出不变，判据也不变：**scheduler 停机 5 分钟，数据面不掉。**

---

## 1. 🔴 本文推翻的三处

| # | 母提案的说法 | 位置 | 实际 | 影响 |
|---|---|---|---|---|
| **E1** | 「CREATE / FORK 的投影写已经是同步的，**且带化身**」 | §7 阶段 1 ①.1、附证据索引 | 同步 ✅（`server.go:431` `ModifyResponse` 内），**带化身 ❌**。`executionIDFromResponse` 读的 `x-agentenv-execution-id` 只由 `src/api/proxy.rs:352-364` 的 `echo_execution` 写出，三个调用点（`proxy.rs:449` `:462` `:473`）全在 `proxy_request` 内 —— **控制面响应从不带它**。`server.go:552-555` 的注释本身就承认它可能缺席 | CREATE 的 binding 永远以空化身写入，实际化身由 5 秒后的心跳补 |
| **E2** | 同上，FORK 一并「不动」 | 同上 | **FORK 的投影写从未发生**。fork 201 的响应体是**顶层 JSON 数组**（`src/api/openapi.yml:1634-1638`，`Vec<models::SandboxForkResult>`），而 `extractSandboxIDsFromResponse`（`server.go:924-926`）第一句就是 `json.Unmarshal(body, &map[string]any)` —— 顶层数组直接报错返回 `nil`。唯一的测试（`server_test.go:665`）喂的是 `{"sandboxes":[…]}` 信封，**AgentENV 没有任何路由产出这种形状** | fork 子沙箱的 binding 只能等心跳；阶段 1 若不修，子沙箱在 ≤5s 窗口内直读全部未命中 |
| **E3** | 「续期时拒绝 —— 越界 ⇒ `errMaxInstanceLengthExceeded` ⇒ HTTP 400」 | §7 阶段 1 ①.4 表、模块文档 D10 | e2b 的 `keep_alive.go:28` 是 **`getMaxAllowedTTL` = `min(timeLeft, duration)`，即钳制**；400 只在 `time.Since(StartTime) > MaxInstanceLength`（**沙箱已经越界**）时抛出（`keep_alive.go:31-32` `:55`）。请求一个过长的 timeout **不会被拒绝，会被截短** | 照 E3 实现会给每个传大 timeout 的客户端一个新的 400。见 §6.3 |

另有两处**引用位置**需要更正（不影响结论）：
- e2b 的 `keep_alive.go` 在 `packages/api/internal/**orchestrator**/`，不在 `handlers/`。
- `SandboxEvent` 在 `services/api/proto/scheduler.proto:327-333`，不是 `:316-324`；
  `SandboxRosterEntry.execution_id` 在 `:303-311`。

**经受住核查、可以照做的**：投影与活跃态 store 是两个结构（D11）；投影写必须同步；
PAUSE/DELETE 走事件通道并按化身守卫；roster 回落不能丢；②的两件不能省。

---

## 2. 事实基线

改动前必须当作已知的事实，全部现场核过：

| 事实 | 位置 |
|---|---|
| gateway 每请求一次 `LookupNode`，无缓存无重试 | `services/gateway/internal/server.go:252`；`gateway/cmd/main.go:27` |
| `Unavailable` / `FailedPrecondition` → 503 直接交给客户端 | `services/gateway/internal/server.go:375` |
| 投影写在 `ModifyResponse` 内，对客户端同步 | `services/gateway/internal/server.go:431` `:449` |
| `shouldRecordAssignment` 只匹配 `POST /sandboxes`、`/sandboxes-cold`、`/{id}/fork` | `services/gateway/internal/server.go:655-670` |
| resume / connect 被识别为控制面路径（`routeSourcePath`），因此**已经**做了 `LookupNode` | `services/gateway/internal/server.go:743` |
| 投影记录结构：扁平 key `agentenv:scheduler:bindings:sandbox:<id>`，值 `{node,execution_id}` | `services/scheduler/internal/redis_store.go:17-22` `:236-241` |
| binding TTL 30s，心跳 5s | `services/scheduler/internal/store.go:11`；`src/cfg.rs:735` |
| 🔴 **心跳对账脚本每次都 `SET … PX ttl_ms`** —— 长 TTL 活不过一个心跳 | `services/scheduler/internal/redis_store.go`，`redisReconcileNodeScriptBody` 的 accept 分支 |
| 🔴 空 roster ⇒ 删掉该节点名下所有 binding ＋ `DEL` 反向索引 | 同上，`desired_count == 0` 分支 |
| 🔴 `accepts` 在 `fenced` 模式下：challenger 为空且已有 incumbent ⇒ **拒绝**（`rejected_unknown`） | `services/scheduler/internal/redis_store.go`，`redisArbitrationFenced` |
| 集群当前三个化身开关全在终态（`enforce`/`true`/`enforce`） | [`_sd-recon-env.md`](_sd-recon-env.md) §3.3 |
| `ReportSandboxEvent` 收到即丢弃 | `services/scheduler/internal/service.go:425-432` |
| `RecordAssignment` 会拒绝未知节点（`InvalidArgument`） | `services/scheduler/internal/service.go:350-357` |
| binding 命中时 `answer(node, BOUND, "", exec, authorityFor(exec))` | `services/scheduler/internal/lookup.go:166-167` |
| roster 回落覆盖两个窗口，带化身仲裁 ＋ freshness tie-break | `services/scheduler/internal/lookup.go:170-188` `:450-470` |
| `authorityFor`：空 ⇒ `UNKNOWN`，非空 ⇒ `REGISTRY` | `services/scheduler/internal/lookup.go:424-435` |
| 🔴 `answer` 有 `silentExecution` 回退分支，会把化身字段清零 | `services/scheduler/internal/lookup.go:405-412` |
| `go-redis/v9` 已是 `services` 模块的直接依赖 | `services/go.mod:10` |
| 沙箱 deadline 是 `SandboxMetadata::expires_at`，起点是 `created_at` | `src/orchestrator/store/metadata.rs:47` `:50` |
| deadline 的唯一算式在 `_set_timeout`：`from + ttl`，**不看 `created_at`** | `src/orchestrator/store/metadata.rs:119-122` |
| `keep_alive_for` 是 `/timeout` 与 `/refreshes` 的唯一收口 | `src/orchestrator/service.rs:884-972` |
| `/timeout` `/refreshes` 的响应枚举**没有 400 变体** | `src/api/generated/src/apis/sandboxes.rs:242-251` `:196-205` |
| 🔴 `OrchestratorError::InvalidSandboxState` 今天落到 catch-all ⇒ **HTTP 500，body 里的 `code` 却是 400** | `src/api/impls/sandbox.rs:61` `:1528-1530` |
| resume 保留 `created_at`、重算 `expires_at`；fork **重置** `created_at`；本地 restore 原样保留；跨节点 restore 重置 | `src/orchestrator/service.rs:2403-2416`、`:713`、`:212-214`、`:463-480` |
| 驱逐只看 `expires_at`，且只驱逐 `Running` | `src/orchestrator/service.rs:2160-2197` |
| 生命周期事件只有三个字段，`Copy` | `src/orchestrator/types.rs:41-55` |
| 五个 publish 点**都**能拿到 `execution_id`，都没传 | `service.rs:556` `:743` `:1118` `:1518` `:1682` |
| 心跳 roster 由 `store.list()` 全量派生 ⇒ **暂停沙箱也在 roster 里** | `src/orchestrator/service.rs:795-803`；`src/orchestrator/store/in_memory.rs:252-261` |
| 持久化沙箱的 restore 在 `Orchestrator::new` 内同步完成，早于 `reporter.start()` | `src/bin/server.rs:139` vs `:162` |
| `SandboxForkResult` 内嵌完整 `Sandbox`，而 `Sandbox` 已带 `executionID` | `src/api/openapi.yml:713-714`；`src/api/impls/sandbox.rs:134` |
| create/cold 的 201 已有 `x-agentenv-sandbox-id` 头；resume/fork/connect 没有 | `src/api/openapi.yml:1388` `:1427`；`:1588-1594` `:1630-1638` |
| Rust 侧 proto 由 `build.rs:5,17` 从同一份 `scheduler.proto` 生成 | `build.rs` |
| 🔧 集群里的 Redis **已部署**（`deploy/k8s/base/redis.yaml`，提交 `77aa98f`），scheduler 已接、**gateway 未接** | [`_sd-recon-env.md`](_sd-recon-env.md) §4.5、SD-B1 |
| 🔴 scheduler 缺席 > `binding_ttl` 的表现是 **404 ~13 秒**，不是 503 ⇒ **只数 503 的探针会假通过** | [`_sd-recon-env.md`](_sd-recon-env.md) §9.1、SD-B6 |

---

## 3. ①.1 CREATE / FORK —— 触发条件不动，写入内容要动

**结论：母提案「不动」只对了三分之一。**

| 环节 | 现状 | 阶段 1 是否要改 |
|---|---|---|
| 何时触发写 | `shouldRecordAssignment` 命中 create / cold / fork（`server.go:655-670`） | ❌ 不改 |
| 写是否同步 | 是，`ModifyResponse` 内（`server.go:431` `:449`） | ❌ 不改 |
| 写的化身 | create：永远空（E1）；fork：**根本没写**（E2） | ✅ 必须改 |
| 写的 TTL | 恒 `binding_ttl` = 30s，且**下一个心跳会重置它** | ✅ 必须改 |

### 3.1 CREATE / COLD

节点在 201 上补两个响应头（§6.4 的载体表）：

- `x-agentenv-execution-id`：`metadata.execution_id`
- `x-agentenv-projection-ttl-secs`：`metadata.projection_ttl_secs(now)`（§6.2）

gateway 侧 `recordAssignmentFromResponse`（`server.go:548-599`）在 header 快路径上
多读一个头，塞进 `RecordAssignmentRequest.projection_ttl_secs`。
**不引入 body 缓冲** —— create 的快路径今天靠 `x-agentenv-sandbox-id` 避开了
`readBodyWithLimit`，这个性质要保住。

### 3.2 FORK

fork 是一次响应、N 个子沙箱、N 个化身，**头装不下**，只能走 body。要改两处：

1. `extractSandboxIDsFromResponse`（`server.go:924-964`）改成先尝试
   `[]any`、失败再尝试 `map[string]any`，并返回三元组
   `(sandboxID, executionID, projectionTTLSecs)` 而不是裸 id 列表。
   fork 元素的化身在 `result.sandbox.executionID`（已存在），
   TTL 在新增的 `result.projectionTtlSecs`。
2. 删掉 `server.go:588-593` 那段「body 路径一律不记化身」的注释与行为 ——
   它当初成立的理由是「一次 echo、多个 id，把父的化身盖到子身上会命名一个从未在那里跑过的化身」。
   **现在每个子元素自带自己的化身，这个理由消失了。**
   🔴 但保留它的**保守版本**：只有当某个元素**自身**带了非空 `executionID` 时才记，
   否则该元素以空化身写入（等心跳补）。禁止跨元素借用。

🔴 **`SandboxForkResult` 加 `projectionTtlSecs`，不要加到 `Sandbox` 上。**
`Sandbox` 是用户可见模型，`SandboxForkResult` 已经是基础设施味道的包装
（"Result of one requested fork"）。这样单沙箱路由用头、fork 用 body，
两条路径各自最省，而用户可见 schema 一个字段都不动。

---

## 4. ①.2 RESUME —— 补进同一机制，但它是三处改动

### 4.1 路由事实

`POST /sandboxes/{id}/resume`（以及 `POST /sandboxes/{id}/connect`）：

- `isSandboxControlPlaneRequest` 命中（`server.go:743` 的 `case` 列表里两个都在）
  ⇒ `routeSource = routeSourcePath`、`hasSandbox = true`
  ⇒ **已经做了一次 `LookupNode`，gateway 手里已有 `sandboxID`**（来自路径，`server.go:227`）。
- `shouldRecordAssignment`（`server.go:669`）末行只认 `parts[2] == "fork"` ⇒ **resume/connect 为假**。

### 4.2 Go 侧

```go
// server.go:669 附近
switch parts[2] {
case "fork":
    return true
case "resume", "connect":
    // 🔴 两条都是 resume 入口：connect 走的是同一个 resume 路径
    //（src/api/impls/sandbox.rs:787 与 :1317 都给 NewTimeout，落到同一个
    // resume_sandbox_inner）。只收 resume 会在 connect 上留同一个洞。
    return s.projectionAuthoritative
default:
    return false
}
```

（`shouldRecordAssignment` 目前是包级函数，要么加一个 `authoritative bool` 参数，
要么提成 `Server` 方法。取后者，调用点只有 `server.go` 一处。）

`recordAssignmentFromResponse` 增加一条**路由 id 快路径**：控制面记录路由
（resume/connect）不查 header 里的 sandbox id、不缓冲 body，直接用
`opts.sandboxID`（`proxyRequestOptions` 里已经有这个字段，`server.go:386-388`）。

### 4.3 🔴 Rust 侧 —— 不做的话这条写会被静默拒绝

resume 的 201 响应**既没有 `x-agentenv-sandbox-id` 也没有 `x-agentenv-execution-id`**
（`src/api/openapi.yml:1588-1594` 没有 `headers:` 块；
`SandboxesSandboxIdResumePostResponse::Status201_…(models::Sandbox)`
是元组变体，`src/api/generated/src/apis/sandboxes.rs:210-221` —— 处理函数**想塞也塞不进去**）。

sandbox id 可以从路径拿（§4.2），**化身不行**。而 resume 会铸一个**新**化身
（`src/orchestrator/launch_plan.rs:155-168`）。于是一次不带化身的 resume 写：

```
accepts(raw, "")  →  incumbent 非空且 challenger == ""  →  false, "rejected_unknown"
```

（`redisArbitrationFenced`，集群当前正是 `enforce`，见 [`_sd-recon-env.md`](_sd-recon-env.md) §3.3）

⇒ **记录不会被写，而且是静默的** —— 只有一个 `rejected_unknown` 计数器。
①.2 的全部价值当场归零，而验证探针在只做 ② 的相位下看不出差别。

**必须做的 Rust 改动**（§6.4 一并规格化）：给 resume / connect 的 201 加
`x-agentenv-sandbox-id`、`x-agentenv-execution-id`、`x-agentenv-projection-ttl-secs`
三个响应头。这是 `src/api/openapi.yml` 改 ＋ `make agentenv-server` 重生成 ＋
`src/api/impls/sandbox.rs` 填值，**不是手改生成代码**（CLAUDE.md 的约束）。

---

## 5. ①.3 PAUSE / DELETE —— 事件通道

### 5.1 proto（加法式）

```protobuf
message SandboxEvent {
  string sandbox_id = 1;
  SandboxEventType event_type = 2;
  uint32 requested_cpu = 3;
  uint64 requested_memory_bytes = 4;
  uint64 requested_disk_bytes = 5;
  // 事件所属的化身。删除投影必须按它守卫：一次 pause 事件迟到、
  // 而同 id 已经在别处以新化身跑起来，无守卫的删除会摘掉活记录。
  // 空 = 报告方太旧，不参与守卫（见 scheduler 侧规则）。
  string execution_id = 6;
}
```

消费者只有两个，都在本仓：Go（`services/api/proto/*.pb.go`，`make -C services` 重生成）
与 Rust（`build.rs:5,17` 用 tonic 从同一份文件生成）。**无外部消费者**，
`services/api/proto/` 也没有发布成独立 Go module（`services/go.mod` 是唯一 module）。
⇒ 加字段安全。

### 5.2 Rust 侧

`SandboxLifecycleEvent`（`src/orchestrator/types.rs:50-55`）加
`pub execution_id: ExecutionId`。`ExecutionId` 是 `Uuid` newtype ⇒ 仍是 `Copy`，
结构体不变性保住。

🔴 **不要给它 `Option`。** 五个 publish 点在发事件的那一行都持有化身：

| 事件 | 位置 | 化身来源 |
|---|---|---|
| Create | `service.rs:556-560` | `metadata.execution_id` |
| Fork | `service.rs:743-747` | 同一循环里 `:731-732` 刚用过 `metadata.execution_id` |
| Delete | `service.rs:1118-1122` | `store.remove` 返回的 `metadata.execution_id`（`:1115`） |
| Pause | `service.rs:1518` | `paused_metadata` / `persisted_metadata` 在 `:1505-1507` 就在作用域内 |
| Resume | `service.rs:1682-1686` | `resumed.as_ref()` 的 `metadata.execution_id` |

`publish_sandbox_event`（`service.rs:2016-2028`）加一个参数，五个调用点各补一个表达式。

`src/observability/reporter.rs:455-461` 的映射加一行
`execution_id: event.execution_id.to_string()`。

🔴 **事件是尽力而为的，这一点不要试图修**：`recv_sandbox_event_batch`
对 `Lagged` 只打日志（`reporter.rs:303-305` `:319-321`），
`send_sandbox_events` 失败只 `warn!`（`reporter.rs:180`），无重试。
这正是「心跳对账保留为修复路径」的理由 —— 不要在阶段 1 给事件加重试队列。

### 5.3 Scheduler 侧

`BindingStore` 加一个方法：

```go
// Delete removes a sandbox's routing projection, but only if the record still
// names the incarnation the event came from.
Delete(sandboxID string, executionID string, now time.Time) (deleted bool, err error)
```

`ReportSandboxEvent`（`service.go:425-432`）实现：

```go
for _, ev := range req.GetEvents() {
    switch ev.GetEventType() {
    case PAUSE, DELETE:
        if !s.projectionAuthoritative { recordSandboxEvent(ev, "ignored_switch_off"); continue }
        exec := normalizeExecutionID(ev.GetExecutionId())
        if exec == "" { recordSandboxEvent(ev, "ignored_unknown_execution"); continue }
        deleted, err := s.store.Delete(ev.GetSandboxId(), exec, now)
        ...
    default: // CREATE / RESUME / FORK 仍然只记日志
    }
}
```

🔴 **三条守卫规则，其中两条**不是**照抄 e2b：**

| 情形 | e2b `catalog_redis.go` `DeleteSandbox` | 本规格 | 理由 |
|---|---|---|---|
| 记录不存在 | 直接返回 nil | 同 | —— |
| 记录里的化身 ≠ 事件的化身 | 返回 nil，不删 | 同，并计 `rejected_stale` | 这是守卫本身 |
| 🔴 记录里的化身**为空** | `"" != exec` ⇒ **不删** | **删**，计 `deleted_unknown_incumbent` | 空 incumbent 是「未知」不是「别人的」。写路径的 `accepts` 对空 incumbent 的规则就是「让位给任何具名 challenger」（`redisArbitrationFenced`）；删除路径用相反规则会造成：create 写了空化身 ⇒ 2 秒后删掉沙箱 ⇒ 记录带着 24h TTL 活下来，指向一个已经不存在的沙箱 |
| 🔴 事件的化身为空（旧节点） | 不适用 | **不删**，计 `ignored_unknown_execution` | 无守卫删除会重新引入竞态；而旧节点也不发 TTL，它的记录本来就 30s 到期，不删无害 |
| 🔴 原子性 | Go 侧 `GET` → 比较 → `DEL`（**有 TOCTOU**：resume 可以在两步之间装入新化身，`DEL` 会删掉活记录） | **Lua 脚本内 `GET`＋比较＋`DEL`** | e2b 自己在 store 侧把同类判定放进 Lua 并逐字写了理由（`storage/redis/scripts.go:33-40`）；catalog 侧那份是它的现成缺口，别抄。本仓已经跑着两个 Lua 脚本，加第三个成本为零 |

删除时同步 `SREM {prefix}:node:<node_id> <sandbox_id>`，否则反向索引里留悬挂成员，
下一次空 roster 的 `desired_count == 0` 分支会去 `GET` 一个已删 key（无害但脏）。

内存 `BindingStore`（`store.go`，`redis_addr` 未配时的默认实现）必须实现同一套规则 ——
`make -C services test` 默认跑的就是它。

### 5.4 🔴 关于 ①.3 的实际价值，要照实说

`list_sandbox_roster`（`src/orchestrator/service.rs:795-803`）走 `store.list()`，
**不过滤状态** ⇒ **暂停的沙箱仍然出现在心跳 roster 里**。于是：

- **PAUSE 事件删掉的记录，会在 ≤5 秒后被下一次心跳对账重新装回。**
  ⇒ ①.3 中 PAUSE 那一半在阶段 1 里**基本不承重**。
  它也不有害：装回来的记录指向的正是持有该暂停沙箱的节点，
  数据面到那里会 auto-resume（`src/api/proxy.rs:878` `:891`）或答 410 ——
  和今天 `LookupNode` 的 roster 回落给出的答案完全一致。
- **DELETE 那一半承重**：`store.remove`（`service.rs:1115`）当场把沙箱移出 roster，
  所以心跳本来就会在 5 秒内删掉 binding。事件买到的是**那 5 秒**，
  以及一个真实的洞：**节点在 delete 之后立刻死掉**，心跳再也不来，
  而记录带着 24 小时 TTL 留在那里指向一台不存在的节点。

⇒ 两半都实现（同一段代码），但**不要在验收里宣称 PAUSE 那一半的效果**。
把 roster 收窄成「只报本节点能服务的沙箱」是一个**独立的、有风险的**改动
（`rosterHolder` 回落、`NodesHolding`、暂停沙箱接管都依赖当前语义），
**明确不在阶段 1 范围内**，登记到 §12。

---

## 6. ①.4 沙箱寿命上界与投影 TTL

### 6.1 config

`config/default.toml` 的 `[orchestrator]`（当前 `:228-236`）新增：

```toml
# Hard ceiling on a sandbox's total wall-clock life, measured from creation.
# A create/resume/fork request asking for more is clamped, not refused, and a
# later SetTimeout can never push the deadline past it. 0 disables the ceiling
# (and with it the long-lived routing projection: records fall back to
# scheduler.binding_ttl).
max_sandbox_lifetime_secs = 86400
# Slack added to the routing projection's TTL so the record outlives the
# sandbox rather than dying just before it.
projection_ttl_grace_secs = 60
```

`src/cfg.rs` 的 `OrchestratorConfig`（`:759-785`），照 `:781` 的形状：

```rust
#[config(default = 86400u64, env = "AENV_MAX_SANDBOX_LIFETIME_SECS")]
pub max_sandbox_lifetime_secs: u64,
#[config(default = 60u64, env = "AENV_PROJECTION_TTL_GRACE_SECS")]
pub projection_ttl_grace_secs: u64,
```

**默认取 24 小时，理由三条：**

1. **阶段 1 引入这个量的目的是让投影 TTL 可推导且有界，不是引入配额。**
   e2b 的 1 小时是 `tiers.max_length_hours` 这张**配额表**的默认值，
   背后有 tier / project 覆盖两级逃生口（`20240219190940`、`20260728163016`）。
   §4.4 已定：**我们没有租户模型**，抄那个数字等于抄来限制、抄不来逃生口。
2. **24 小时对现有行为是零改变。** 今天 `/refreshes` 单次上限就是 3600 秒
   （`src/api/openapi.yml:674`），要撞到 24 小时得连续续期 24 次。
   把一次产品行为变更夹带进一次基础设施阶段是 §7 抬头「删除不与切换同批」的同类错误。
3. **最坏泄漏被 24 小时封顶**：事件丢了 **且** 节点再也不心跳，记录最多苟活一天。

`0` 必须是「关闭」而不是「无上界」。🔴 关闭时节点发 `projection_ttl_secs = 0`，
scheduler 落回 `binding_ttl` —— **这就是写侧回退路径，且它等于今天的行为**。

#### 🔴 已订正（QA F3）：上界约束的是「运行时长」，不是「自创建起的墙钟」

本节初稿把上界写成「自 `created_at` 起的墙钟寿命」，理由只列到「`/refreshes`
单次上限 3600 秒，要撞到 24 小时得连续续期 24 次」——
**这条推理只覆盖了一直在跑的沙箱，完全没算暂停期间流逝的墙钟。**
实测后果：创建 → 暂停 → 25 小时后 resume，resume 返回 201，
但 `_set_timeout` 算出的 `expires_at = min(now + timeout, created_at + 24h)`
**已经是过去时刻**；`auto_evict_interval_ms`（默认 1000ms）内驱逐循环就会看到
一台 Running 且已过期的沙箱，按 `timeout_action` 把它重新暂停（默认）
或者直接删除。客户端看到的是「resume 成功，随即沙箱死亡」。

**订正后的语义：上界约束的是沙箱的累计运行时长，暂停期间不计入。**

判据不是口味问题，是本阶段的纪律问题：

- 本阶段抄的 e2b **根本没有 paused 状态**（`sandboxtypes/states.go:108-113`
  只有 running / pausing / killing / snapshotting，暂停的沙箱离开活动集合、
  变成 catalog 行），所以它的 `MaxInstanceLength` 只可能约束运行时长 ——
  这个问题在那边提不出来。
- 「暂停也计入上界」是 AgentENV 多出一个状态之后**新造**出来的产品行为变更。
  §7 的纪律（「删除动作一律不与切换动作同批」，以及每个阶段必须可独立回滚、
  不得夹带行为变更）明确禁止把它塞进一次基础设施阶段。

对外可见的规则因此表述为：**一台沙箱累计运行不得超过 `max_sandbox_lifetime_secs`**，
而不是「创建 24 小时后不能再 resume」。

### 6.2 (a) 建时钳制 —— 一个字段管住所有入口

🔴 **不要在 create 调用点钳制。** `expires_at` 的入口有六条
（create-from-snapshot `service.rs:487`、create-fresh `:547`、resume `:2414-2415`、
fork `:715`、connect、auto-resume `src/api/proxy.rs:1032`），
逐个钳制必漏。所有六条最终都汇进 `SandboxMetadata::_set_timeout`
（`src/orchestrator/store/metadata.rs:119-122`），钳制放那里。

`SandboxMetadata` 加三个字段 —— 一个预算，一个已花，一个本次运行的起点：

```rust
/// The lifetime ceiling this sandbox was created under, or None when the node
/// had no ceiling configured. A budget of *running* time.
#[serde(default)]
pub max_lifetime: Option<Duration>,
/// Running time already spent, summed over the runs that have finished.
#[serde(default)]
pub running_elapsed: Duration,
/// When the run in progress started, or None while the sandbox is paused.
#[serde(skip)]
pub running_since: Option<SystemTime>,
```

🔴 `serde(default)` 在前两个字段上都是承重的，与 `execution_id` 相反：
持久化的暂停沙箱记录里有早先构建写下的行，而 `persister.load_all` 跑在
`Orchestrator::new`（`service.rs:195`）里面 —— 这里写成必填字段
等于「升级之后节点起不来」。

🔴 `running_since` 反过来**故意不持久化**。它描述的是「本进程这一次运行」的区间，
而记录只在 pause 时离开本进程，那时 `pause_sandbox_inner` 已经把区间结算进
`running_elapsed` 了。持久化它只会新增一种失败模式：一个跨越节点宕机的未闭合区间，
把停机时间算成运行时间 —— 正是这套机制要消除的那个 bug。

```rust
impl SandboxState {
    /// 除 Paused 外全都在花预算：暂停的沙箱没有 VM、没有 vCPU、没有网络槽位。
    pub fn spends_lifetime(self) -> bool { !matches!(self, SandboxState::Paused) }
}

impl SandboxMetadata {
    /// 预算耗尽的时刻。None 表示没有上界。
    pub fn lifetime_deadline(&self, now: SystemTime) -> Option<SystemTime> {
        let remaining = self.max_lifetime?.saturating_sub(self.running_elapsed);
        // 在跑：锚在本次运行的起点，所以整段运行期间答案固定，且**可以是过去时刻**
        //       —— 这正是 §6.3 那条 400 报告的东西。
        // 暂停：什么都没在花，锚随 now 后退 —— 暂停一周回来预算原封不动。
        let anchor = match self.running_since {
            Some(since) if self.state.spends_lifetime() => since,
            _ => now,
        };
        anchor.checked_add(remaining)
    }

    /// 幂等，且只由 state 驱动。
    pub fn sync_running_clock(&mut self, now: SystemTime) { /* 开/结算本次区间 */ }

    /// fork 子沙箱专用：预算清零重开（与 `created_at = now` 配对）。
    pub fn restart_lifetime_clock(&mut self, now: SystemTime) { /* … */ }

    fn _set_timeout(&mut self, timeout: Option<Duration>, from: SystemTime) {
        self.timeout = timeout;
        let deadline = timeout.and_then(|ttl| from.checked_add(ttl));
        self.expires_at = match (deadline, self.lifetime_deadline(from)) {
            (Some(d), Some(cap)) => Some(d.min(cap)),
            (d, _) => d,
        };
    }
}
```

**记账放在哪：`InMemoryMetadataStore` 的四个写入口**（`add`、`update`、
`update_state_if_state`、`update_if_state`）在发布结果前各调一次
`sync_running_clock`。理由与「`_set_timeout` 是唯一钳制点」完全同构：
`metadata.state = …` 的赋值点有八九处，逐点挂钩必漏一处，而漏掉的症状是
某台沙箱悄悄多花或少花预算。时钟是 state 的函数，所以放在写入口收敛最省事 ——
`update_if_state` 的回调可以随便改 state，时钟自己跟上。

两个例外要显式调用，且都因为幂等而无害：

- `pause_sandbox_inner`（`service.rs:1489` 附近）在 `state = Paused` 之后、
  `persist_paused` **之前**结算。持久化的那份才是重启后读回来的那份，
  把结算留给 store 等于每次节点重启白送一段运行时间。
- fork 子沙箱（`service.rs:715` 附近）在 `created_at = now` 旁边
  `restart_lifetime_clock(now)`。克隆自带父的 `running_elapsed`，
  不清零就等于「从跑了 23 小时的父沙箱 fork 出来的孩子只有 1 小时命」。

**为什么是 `max_lifetime: Option<Duration>` 而不是存一个绝对 `lifetime_deadline`：**
fork 清零时钟，所以子沙箱拿到全新窗口；resume 保留已花预算，
所以恢复后接着花同一份 —— 两者都是「最大运行时长」的正确读法，
而且**不需要在任何调用点写一行钳制代码**。

写入 `max_lifetime` 的位置只有 `SandboxMetadata` 的构造点：
`service.rs:463-480`（snapshot 分支）、`:524-540`（cold 分支）、
`:706-715`（fork 子沙箱，克隆父的即可）。跨节点 restore
（`src/api/impls/paused_recovery.rs:1167-1189` → `create_sandbox_inner`）
走的是普通 create 路径，自动拿到新值（预算也随之清零）。
本地 restore 原样保留旧值（含 `None`），`running_elapsed` 一并读回。

### 6.3 (b) 续期 —— 钳制为主，400 只给「已经越界」

🔴 **这一条与母提案相反，见 §1 E3。** 照 e2b 实际行为实现：

- **过长的续期请求被截短，不被拒绝。** 截短由 (a) 自动完成 ——
  `keep_alive_for` 里的 `metadata.set_timeout(Some(valid_timeout))`
  （`src/orchestrator/service.rs:953`）走的就是 `_set_timeout`。**这一行不用改。**
- **只有沙箱已经越过上界时返回 400。** 在 `keep_alive_for` 的
  `metadata.state != Running` 检查之后（`service.rs:931` 之后）插入：

  ```rust
  let now = SystemTime::now();
  if let Some(cap) = metadata.lifetime_deadline(now) {
      if now >= cap {
          return Err(OrchestratorError::SandboxLifetimeExceeded { sandbox_id, cap });
      }
  }
  ```

  新增 `OrchestratorError::SandboxLifetimeExceeded`。

- 🔴 **HTTP 400 不会自己出现，有两个坑：**
  1. `/timeout` 与 `/refreshes` 的响应枚举**没有 400 变体**
     （`src/api/generated/src/apis/sandboxes.rs:242-251` `:196-205`）。
     必须给 `src/api/openapi.yml:1711-1719` 与 `:1847-1855` 各加
     `"400": $ref: "#/components/responses/400"`（组件已存在于 `:113-118`），
     再 `make agentenv-server`。
  2. `src/api/impls/sandbox.rs:1528-1530` / `:1296-1298` 的 catch-all 会把任何新错误
     变成 **HTTP 500，而 body 里的 `code` 写着 400**（`sandbox.rs:61` 的 `From` 实现）。
     必须在两个 impl 里**显式**匹配新变体并返回
     `Status400_BadRequest(Self::error(400, …))`（形状照 `sandbox.rs:600-606`）。

- resume / connect 请求里的 timeout **不需要 400**：它们走 (a) 的钳制，
  和 create 一样 —— 一次要求超过剩余寿命的 resume 被截短，不被拒绝。

### 6.4 🔴 (c) 投影 TTL：算式、载体、契约

#### 算式（节点侧）

```rust
pub fn projection_ttl_secs(&self, now: SystemTime) -> u32 {
    let Some(cap) = self.lifetime_deadline(now) else { return 0 };   // 0 = 交给对端默认
    let remaining = cap.duration_since(now).unwrap_or(Duration::ZERO);
    // ceil to whole seconds, then add the grace, then floor at 1.
    let secs = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
    (secs + grace_secs).clamp(1, u64::from(u32::MAX)) as u32
}
```

🔴 **母提案标出的整数除法坑，实际是两个坑，第二个更致命：**

| 坑 | e2b 代码 | 后果 |
|---|---|---|
| 截断 | `int64(MaxInstanceLength / time.Hour)`（`lifecycle.go:36`） | 90 分钟 ⇒ 1 ⇒ 60 分钟：**投影比沙箱先过期**，沙箱还活着路由就查不到 |
| 🔴 **归零** | 同上，< 1 小时 ⇒ `0` ⇒ `lifetime = 0` ⇒ go-redis `Set(…, 0)` = **永不过期** | 上界越短，泄漏越严重。e2b 只因为默认就是整 1 小时才没踩到 |

⇒ 本规格的三条硬规则：
1. **单位是整秒，向上取整**（`ceil`），不是截断。
2. **下限钳到 1**，绝不出现 0 以外的非正值。
3. 🔴 **对端收到 `<= 0` 一律解释为「使用 `binding_ttl`」，永远不解释为「不过期」。**
   Redis `PX 0` / `PX <0` 是错误，`SET` 不带过期是永久 —— 两者都不能是默认行为。

#### 载体（三条，全部加法式）

| 路径 | 载体 | 新增 |
|---|---|---|
| create / cold / resume / connect 的 201 | HTTP 响应头 `x-agentenv-projection-ttl-secs`（配 `x-agentenv-sandbox-id` / `x-agentenv-execution-id`） | `src/api/openapi.yml` 四个操作的 `headers:` 块 ＋ 重生成 |
| fork 的 201（数组） | body：`SandboxForkResult.projectionTtlSecs`（化身用已有的 `result.sandbox.executionID`） | `src/api/openapi.yml:706-716` |
| gateway → scheduler | `RecordAssignmentRequest.projection_ttl_secs = 4`（uint32） | `services/api/proto/scheduler.proto:172-181` |
| 节点 → scheduler（心跳修复路径） | `SandboxRosterEntry.projection_ttl_secs = 3`（uint32） | `services/api/proto/scheduler.proto:303-311` |

#### 🔴 契约：Go 侧**不持有**这个值的任何副本

**决定：值从 Rust 配置出发，全程走线，Go 侧只搬运不定义。**

被否掉的两条：

- **scheduler 配置项**（`scheduler.max_sandbox_lifetime`）。
  这会在 ConfigMap 里造出寿命上界的第二份副本。
  [`_sd-recon-env.md`](_sd-recon-env.md) §3.3 已经记录这套集群的 ConfigMap literal
  **会被 `make k8s-apply` 静默退回**（D-11），§6 的漂移表已有十条。
  再加一条的失败模式是「投影比沙箱先过期」—— 表现为路由未命中，
  和冷缓存一模一样，**没有任何告警能把两者分开**。
- **只用响应头**。响应头到不了**心跳修复路径**，而修复路径正是「事件丢了记录还在」
  的全部依据。只有响应头 ⇒ 修复写出来的记录 TTL 是 30 秒 ⇒ 一次丢事件就把记录降级回今天。

**skew 分析 —— 只有版本 skew，没有配置 skew，而且两个方向都落回今天的行为：**

| 组合 | 结果 |
|---|---|
| 旧节点 ＋ 新 scheduler | 字段缺席 ⇒ `0` ⇒ 用 `binding_ttl`（30s）＝ **今天** |
| 新节点 ＋ 旧 scheduler | 字段被忽略 ⇒ `binding_ttl`（30s）＝ **今天** |
| 新 ＋ 新、开关 off | scheduler 忽略字段 ⇒ 30s ＝ **今天** |
| 新 ＋ 新、开关 on | 长 TTL 生效 |

⇒ **滚动升级期间的任何中间态都等于当前系统，不等于某个新的中间系统。**
这是选这条路的主要理由，比「少一个配置项」重要得多。

**一个上限，不是第二个真相源**：scheduler 侧加
`scheduler.max_projection_ttl`（默认 `24h`），对收到的值做上钳。
它是**存储所有者对写入者的限额**，不是寿命的定义。
钳到之后投影比沙箱先过期 ⇒ 直读未命中 ⇒ 回落 `LookupNode` ⇒ roster 命中 ⇒ 仍然可路由。
**降级，不是故障。**

### 6.5 🔴🔴 (c) 的另一半：心跳对账必须停止重置 TTL

**母提案完全没提这条，而它单独就能让 ①.4 全部失效。**

`redisReconcileNodeScriptBody`（`services/scheduler/internal/redis_store.go`）
的 accept 分支现在是：

```lua
redis.call("SET", binding_key(sandbox_id), value, "PX", ttl_ms)
```

`ttl_ms` 恒等于 `binding_ttl`。心跳 5 秒一次（`src/cfg.rs:735`）
⇒ **建时写的 24 小时 TTL 在 5 秒后变回 30 秒。**

改成：

```lua
if raw and decision == "refreshed" then
  -- 同一化身再次报到：这是周期性的心跳，绝不能给记录续命，
  -- 否则投影的存活重新依赖一条周期写路径，本阶段的兑现被抵消。
  redis.call("SET", binding_key(sandbox_id), value, "KEEPTTL")
else
  -- installed / installed_unknown（修复丢失的写）与 superseded（新化身，
  -- 携带新的寿命预算）：这两种都是真实的生命周期事件，不是周期性 tick。
  redis.call("SET", binding_key(sandbox_id), value, "PX", entry_ttl_ms)
end
```

其中 `entry_ttl_ms` 来自该 roster 条目的 `projection_ttl_secs`，
缺席或 `<= 0` 时落回 `binding_ttl`。参数照脚本现有约定，
以**第三条平行参数串**传入（脚本已经用两条平行串传 id 与化身，
理由逐字写在 `ReconcileNode` 的注释里 —— 扁平配对「一个索引错误就把每个沙箱绑到邻居的化身上」）。

`redisRecordBindingScriptBody` 同样接受 `ARGV[8] = projection_ttl_ms`，
`<= 0` 落回 `ARGV[4]`（`binding_ttl`）。

🔴 **`{prefix}:node:<id>` 反向索引的 1 小时 TTL（`defaultRedisNodeIndexTTL`）不要动。**
它是索引不是投影；把它拉长会让一台永久消失的节点的索引集合活得比它的记录还久。

🔴 **`desired_count == 0` 的整体清空分支保留。** 核过启动顺序：
持久化沙箱的 restore 在 `Orchestrator::new` 内**同步**完成
（`src/bin/server.rs:139` 里的 `.await?`，落到 `src/orchestrator/service.rs:212-214`），
`reporter.start()` 在 `:162`，第一次心跳再延迟 100ms（`reporter.rs:100`）
⇒ **第一发心跳就带着完整 roster**，不存在「重启时空 roster 把自己的记录清空」。
但这条现在是承重的时序不变量，而没有任何注释或测试保护它。
⇒ 在 `src/bin/server.rs:139` 与 `:162` 之间加一段注释说明，并加一个断言测试。

---

## 7. ② gateway 直读

### 7.1 共享读端

新建 `services/shared/routing/`（gateway 不能 import `services/scheduler/internal/…` ——
Go 的 `internal` 规则把它锁在 `services/scheduler/` 子树内）：

| 文件 | 内容 |
|---|---|
| `record.go` | `Node`（`node_id` / `endpoint` / `pod_name,omitempty`，json tag 必须与 `services/scheduler/internal/types.go:5-19` 逐字一致）、`Record{Node, ExecutionID}`、`BindingKey(prefix, sandboxID)`、`ParseRecord([]byte)` |
| `authority.go` | `NormalizeExecutionID`、`AuthorityFor` —— 🔴 从 `lookup.go:424-435` **移动**过来，`lookup.go` 改为调用共享版本并**删掉本地副本**。两份必然漂移 |
| `reader.go` | `Reader`：`redis.Client` ＋ `Get(ctx, sandboxID) (Record, bool, error)`，2 秒超时（照 `defaultRedisOperationTimeout`） |

`services/scheduler/internal` 侧：`type Node = routing.Node`、
`type redisBindingRecord = routing.Record`（别名，不是复制），
`parseRedisBindingBytes` 转调 `routing.ParseRecord`。

🔴 **一个跨包 golden 测试**：断言
`routing.Synthesize(rec)` 与 `lookupDeps.answer(rec.Node, BOUND, "", rec.ExecutionID, AuthorityFor(rec.ExecutionID))`
产出**逐字段相等**的 `*schedulerv1.LookupNodeResponse`。
没有这个测试，两侧漂移是必然而不是可能。

### 7.2 🔴 gateway 合成 `LookupNodeResponse` —— 逐字段清点

`decideFencing`（`gateway/internal/execution_fencing.go`）用到四个字段，
`server.go:252-280` 另外用到两个。扁平记录带了哪些、缺的怎么来：

| 字段 | 谁要 | 记录里有吗 | 怎么得到 |
|---|---|---|---|
| `node`（`node_id` / `endpoint` / `pod_name`） | 转发目标；`recordAssignment` 的参数 | ✅ `record.node` | 原样 |
| `execution_id` | `decideFencing` 的 `REGISTRY` 分支、`plan.expect` | ✅ `record.execution_id` | 原样，过 `NormalizeExecutionID` |
| `execution_authority` | `decideFencing` 的 `switch` | ❌ | **纯函数导出**：`AuthorityFor(execution_id)`（空 ⇒ `UNKNOWN`，非空 ⇒ `REGISTRY`），`lookup.go:430-435` |
| `location` | `recordGatewaySandboxLocation`、`locationNeedsAssignment` 的日志分支 | ❌ | **常量 `SANDBOX_LOCATION_BOUND`** —— binding 命中的出口只有 `lookup.go:166` 一处，恒传这个 |
| `origin_node_id` | 只进日志（`server.go:270`） | ❌ | **常量 `""`** —— 同一出口恒传空 |

⇒ **没有任何一个字段是拿不到的**：两个是常量，一个是三行纯函数，两个原样。
母提案「要合成完整语义」的判断对，但难度被高估了；真正的风险是**这三样东西
在两个包里各存一份**，所以 §7.1 要求移动而不是复制，并要求 golden 测试。

🔴 **一处直读绕开了的东西：`silentExecution` 回退。**
`lookupDeps.answer`（`lookup.go:405-412`）在
`SCHEDULER_ROUTING_EXECUTION_ARBITRATION=off` 时会把化身字段**清零**返回。
直读的 gateway 看不到 scheduler 的这个设置，会继续返回带化身的答案 ——
**一次针对化身仲裁的紧急回退，在直读打开时只回退了一半。**
处置：不加新机制，改为**成对翻转的运维约束** ——
`SCHEDULER_ROUTING_EXECUTION_ARBITRATION=off` 必须同时
`GATEWAY_ROUTING_PROJECTION_READ=off`。写进 §8 的回退表，
并加进 [`_sd-recon-env.md`](_sd-recon-env.md) §7.4 的 D-11 漂移复核清单。

### 7.3 读路径

`server.go:246-280` 的 `if hasSandbox` 块改成：

```go
var resp *schedulerv1.LookupNodeResponse
source := routeResolutionScheduler

if s.projectionReader != nil {           // 读侧开关关闭时为 nil
    rec, ok, err := s.projectionReader.Get(routingCtx, sandboxID)
    switch {
    case err != nil:
        // 🔴 Redis 出错不得成为新的失败模式。本阶段的兑现是"少一个依赖"，
        // 把 Redis 变成第二个能打死数据面的东西是反的。
        recordRouteResolution(routeResolutionRedisError)
        s.logger.Warn("routing projection read failed, falling back to scheduler", ...)
    case ok:
        resp = routing.Synthesize(rec)
        source = routeResolutionRedisHit
    default:
        recordRouteResolution(routeResolutionRedisMiss)
    }
}

if resp == nil {
    rpcStart := time.Now()
    var err error
    resp, err = s.queryOnlyScheduler.LookupNode(routingCtx, &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
    recordGatewaySchedulerRPC("LookupNode", rpcStart, err)
    if err != nil {
        s.writeSchedulerError(w, err)      // 未命中的 404/503 仍然只由这里产生
        return
    }
}
recordRouteResolution(source)
// 下面 node/location/fencing 的取用逻辑一字不改
```

🔴 **两条不可违反的：**
1. **未命中不是答案。** Redis miss 与 Redis error 都只能**落到 `LookupNode`**，
   gateway 自己**永远不产生** 404 / 503。`writeSchedulerError`（`server.go:364-380`）
   仍是这两个状态码的唯一出口。
2. **roster 回落因此结构性地保住了** —— 回落到 `LookupNode` 就走完
   `lookup.go` 的 1→2→3（binding → roster → 登记表），
   `rosterHolder`（`:450-470`）的化身仲裁与 freshness tie-break 一并保留。
   要有一个测试直接钉住这一点（§9 P1-b）。

### 7.4 观测

直读会把 scheduler 侧的 `recordSchedulerLookup` / `recordLookupExecutionAuthority`
打到接近 0，**两侧互证**（[`_sd-recon-env.md`](_sd-recon-env.md) §8 第 3 条）会失效。
补一个 gateway 侧计数器：

```
agentenv_gateway_route_resolution_total{source="redis_hit"|"redis_miss"|"redis_error"|"scheduler"}
```

互证等式（同一窗口）：
`Δ{redis_miss} + Δ{redis_error} ≈ Δ agentenv_scheduler_lookup_total`。
对不上就是有一侧算错了。

scheduler 侧补：

```
agentenv_scheduler_sandbox_event_total{event_type=…, outcome="deleted"|"deleted_unknown_incumbent"|"rejected_stale"|"ignored_unknown_execution"|"ignored_switch_off"|"noop_absent"}
agentenv_scheduler_projection_ttl_source_total{source="event"|"default"|"clamped"}
```

---

## 8. 开关与回退

**两个逻辑开关，三个环境变量** —— 写侧横跨两个进程，各持一半。

| 逻辑开关 | 进程 | 环境变量 | 取值 | 代码默认 | ConfigMap 起点 |
|---|---|---|---|---|---|
| 读侧 | gateway | `GATEWAY_ROUTING_PROJECTION_READ` | `off` / `on` | **`off`** | `off` |
| 写侧 | gateway | `GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE` | `off` / `on` | **`off`** | `off` |
| 写侧 | scheduler | `SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE` | `off` / `on` | **`off`** | `off` |

解析照 `services/shared/config/config.go:822-843` 现有三个开关的形状。

**各自关掉什么：**

| 开关 | `off` 时 |
|---|---|
| `GATEWAY_…_READ` | `projectionReader = nil`，每请求仍走 `LookupNode`（今天的行为） |
| `GATEWAY_…_AUTHORITATIVE` | `shouldRecordAssignment` 不匹配 resume/connect；不读、不转发 TTL 头（TTL 字段发 0） |
| `SCHEDULER_…_AUTHORITATIVE` | 忽略收到的 `projection_ttl_secs`，一律用 `binding_ttl`；`ReportSandboxEvent` 继续丢弃；reconcile 脚本走 `PX binding_ttl` 而不是 `KEEPTTL` |

🔴 **写侧两个环境变量不需要原子翻转，两个顺序都安全：**
- 先 scheduler 后 gateway：resume 的记录暂时由心跳修复路径装入（迟 ≤5 秒），TTL 正确。
- 先 gateway 后 scheduler：多写一次 resume 的 assignment，TTL 仍是 30 秒 —— 今天的行为 ＋ 一次冗余写。

🔴 **代码默认取 `off`，与既有三个化身开关相反，理由要写进注释。**
既有三个默认终态是因为「那次发布的存在理由就是打开它们，停在 observe 才是它们怕的失败」
（`deploy/k8s/base/kustomization.yaml:78-81`）。这两个不同：
写侧开关一打开，`ReportSandboxEvent` 就从「保证是 no-op」变成会改状态的 RPC，
而**所有在跑的节点今天就在发这些事件**。一个升级了 scheduler 但还没配开关的集群，
会在重启的那一刻拿到一个它没要求的行为。⇒ `off` 是唯一安全的代码默认。

🔴 **必须放环境变量，不能放挂载文件。**
[`_sd-recon-env.md`](_sd-recon-env.md) §7.7 记录的「删 key 与写空串结果相反、且失败一侧静默」
**是挂载文件的性质**（`src/api/control_plane_gate.rs` 对读失败刻意保留上一个 good 值，
kubelet 卷刷新还跨节点偏斜 12s/60s）。`configMapKeyRef` 的环境变量不是这样：
进程只在启动时读一次，删 key（`optional: true`）与写空串**都要等 Pod 重启才生效**，
且删 key 落回代码默认。⇒ 回退动作是

```bash
kubectl -n $NS set env deploy/agentenv-gateway   GATEWAY_ROUTING_PROJECTION_READ=off
kubectl -n $NS set env deploy/agentenv-scheduler SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE=off
```

`set env` 会触发 rollout，是**响亮的**。不要用 `kubectl patch cm` 然后等它自己生效 —— 它不会。

**回退的完整性**：读侧关掉 ⇒ 回到每请求 `LookupNode`；写侧关掉 ⇒ 下一个心跳
（≤5 秒）就把所有记录用 `PX binding_ttl` 重写成 30 秒 TTL。
**回退不需要跑任何新写的回滚逻辑**（outcome §5.4 的判据），因为回退路径就是
`else` 分支本身。

🔴 **成对约束一条**：`SCHEDULER_ROUTING_EXECUTION_ARBITRATION=off` 必须
同时 `GATEWAY_ROUTING_PROJECTION_READ=off`（§7.2 的 `silentExecution` 绕过）。

---

## 9. 验证探针

每一发都带对照面（[`_sd-recon-env.md`](_sd-recon-env.md) §8 第 1、2 条：
**一个必然为假的输入**，以及**恒 0 的指标要先顶起来一次**）。

造沙箱时 `timeout` 与 `autoResume` 两个字段必传（§7.5 的坑）。

### P1 —— 头号判据：控制面挂掉不打死数据面

| | 动作 | 断言 |
|---|---|---|
| **准备** | 10 个 running 沙箱，`timeout=3600`、`autoResume=true`；对每个打稳定的数据面流量（1 rps） | 基线成功率 100% |
| **A（处理组）** | 两个开关全 `on`，`kubectl scale deploy/agentenv-scheduler --replicas=0`，**持续 5 分钟** | 成功率不变；`route_resolution_total{source="redis_hit"}` 持续增长；`{source="scheduler"}` 平坦；503 计数 **0** |
| **B（对照组，必须失败）** | `SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE=off` ＋ rollout，等 ≥2 个心跳让记录被重写回 30s TTL，再 scale 到 0，**同样 5 分钟** | 成功率在 **~30–35 秒后掉到 0**；503 出现在 `server.go:375` 那条路径 |

🔴 **A 与 B 给出相同答案则本探针作废**（outcome §5.3）。
🔴 **B 之前必须先确认记录 TTL 真的回到 30 秒**（`PTTL`），否则你测的是 A 的残留。

### P1-b —— roster 回落没有丢（A 相位自己证明不了）

A 相位从不未命中，所以它无法证明回落还在。补一发：
两个开关 `on`、**scheduler 在线**，手工 `DEL agentenv:scheduler:bindings:sandbox:<id>`，
立刻打一次数据面请求。

- 断言：请求成功；`route_resolution_total{source="redis_miss"}` +1；
  scheduler 侧 `lookup_result="roster"` +1。
- 🔴 **对照面（必须失败）**：对一个**从不存在**的 sandbox id 打同一请求 ⇒ **404**。
  这排除「未命中一律放行」这种更弱的实现。

### P2 —— 建时钳制有牙

- 探针：`AENV_MAX_SANDBOX_LIFETIME_SECS=300`，`POST /sandboxes {timeout: 3600}`，
  `GET /sandboxes/{id}` ⇒ `endAt - startedAt <= 300s`。
- 🔴 对照面（必须失败）：同一请求，`AENV_MAX_SANDBOX_LIFETIME_SECS=0` ⇒
  `endAt - startedAt == 3600s`。**没有这一面，「300」可能只是默认 timeout 而不是钳制。**

### P3 —— 续期推不过上界

- 探针：上界 300s；create `timeout=60`；t=30s 时 `POST /sandboxes/{id}/timeout {timeout: 3600}`
  ⇒ **204**（不是 400），且 `endAt <= startedAt + 300s`。
- 对照面（必须失败）：上界 0 ⇒ 同一调用后 `endAt ≈ now + 3600s`，越过 300s 线。
- 🔴 **400 那一支不做集群探针，照实登记射程边界**
  （[`_sd-recon-env.md`](_sd-recon-env.md) §8 第 4 条）：
  它只在 `now >= lifetime_deadline` 且驱逐器尚未动手时可达，
  而 `auto_evict_interval_ms = 1000` ⇒ 窗口 ≤1 秒，端到端不可稳定复现。
  **用 `keep_alive_for` 的单元测试证明**（构造一个 `running_since` 在过去、
  已经跑满预算的记录 —— 🔴 订正：不是 `created_at` 在过去，
  见 §6.1「上界约束的是运行时长」），
  并在验收记录里写明「400 是单元测试覆盖，不是集群取证」。

### P3-b —— 🔴 暂停久于上界的沙箱仍然能 resume（QA F3 的回归探针）

- 探针：`AENV_MAX_SANDBOX_LIFETIME_SECS=300`；create（`timeout=60`）⇒ pause ⇒
  等待 > 300 秒 ⇒ resume ⇒ **等待 ≥ 2 个 `auto_evict_interval_ms`** ⇒
  `GET /sandboxes/{id}` 仍是 `running`，且 `endAt > now`。
- 🔴 **必须等驱逐周期**，只看 resume 的 201 什么都证明不了：
  这个缺陷的表现就是「201 之后一秒内被驱逐」。
- 对照面（必须失败）：同一台沙箱累计**运行**满 300 秒之后 resume ⇒
  一个驱逐周期内回到 `paused`（`timeout_action=Pause`）。
  没有这一面，「没被驱逐」可能只是上界被整个关掉了。
- 单元测试覆盖同一对判据：
  `a_sandbox_paused_past_the_ceiling_resumes_and_survives_the_evictor`
  与 `a_sandbox_that_has_spent_its_running_budget_is_still_evicted_after_a_resume`。

### P4 —— 事件删除是按化身守卫的

- **先把指标顶起来**（§8 第 2 条）：`DELETE /sandboxes/{id}` ⇒
  `sandbox_event_total{event_type="delete",outcome="deleted"}` 从 0 长出 1，
  且 Redis key **在下一个心跳之前**消失。
- 🔴 对照面（必须失败）：**用真行、真化身**，不要合成 ——
  ① 建沙箱 A，记下化身 E1；② `DELETE` 它；③ 用同一个 sandbox id 重新建，得化身 E2；
  ④ 直接对 scheduler 发一次 `ReportSandboxEvent{sandbox_id: A, event_type: DELETE, execution_id: E1}`。
  断言：**key 存活**，`outcome="rejected_stale"` +1。
  （§8 第 1 条明确警告过：一行任何相位都认领不了的合成记录，"409 看着像被拒，实则毫无分辨力"。）
- 第三面：`execution_id=""` 的同一事件 ⇒ key 存活，`outcome="ignored_unknown_execution"` +1。

### P5 —— TTL 写一次、不被续期（这是唯一能抓到 §6.5 的探针）

- 探针：上界 86400。create 后立刻 `PTTL <key>` ⇒ ≈ 86,400,000 ± grace。
  等 30 秒（≥6 个心跳）再 `PTTL` ⇒ **比上次少了 ~30,000 ms**。
- 🔴 对照面（必须失败）：写侧开关 `off`，等 2 个心跳，同样两次采样相隔 30 秒 ⇒
  两次都在 **~30,000 ms 附近徘徊**（每个心跳重置）。
- 没有这一发，`redisReconcileNodeScriptBody` 的 `KEEPTTL` 改动即使被回退掉也没人发现，
  而 P1 的 A 相位在 5 分钟内**照样通过**（记录被心跳一直续着，只要 scheduler 还没停）。
  ⇒ **P5 必须在 P1 之前跑。**

### P6 —— resume 的写真的落地了（②的价值证明）

- 探针：create → pause → `POST /sandboxes/{id}/resume`；
  在**下一个心跳到达之前**（<5s）读 Redis ⇒ key 存在，`execution_id` 等于 resume 后
  `GET /sandboxes/{id}` 返回的 `executionID`。
- 🔴 对照面（必须失败）：把节点回滚到不发 `x-agentenv-execution-id` 头的镜像
  （`cp3-bff4993`，registry 里现存）⇒ 同一序列下
  `binding_arbitration` 指标出现 `rejected_unknown`，且 5 秒窗口内 key 不存在。
  **这一面直接证明 §4.3 的判断**（不带化身的写会被静默拒），
  也证明本探针不是在测「resume 之后早晚会有记录」。

---

## 10. 逐文件改动与规模

🔴 **母提案的「400–500 行含测试」低估约 6–8 倍。**

### Go

| 文件 | 动作 | 非测试 | 测试 |
|---|---|---|---|
| `services/shared/routing/record.go` | 新建：`Node`/`Record`/key/parse/`Synthesize` | 140 | 130 |
| `services/shared/routing/authority.go` | 新建：从 `lookup.go:424-435` 移动 | 35 | 60 |
| `services/shared/routing/reader.go` | 新建：Redis 读端 | 90 | 120 |
| `services/scheduler/internal/redis_store.go` | 别名到共享类型；`Delete` ＋ 第三个 Lua 脚本；两个脚本收 TTL 参数；🔴 reconcile 改 `KEEPTTL` | 150 | 210 |
| `services/scheduler/internal/store.go` | `BindingStore.Delete`；内存实现的同规则 ＋ 每记录 TTL | 80 | 120 |
| `services/scheduler/internal/service.go` | `ReportSandboxEvent` 实体化；`RecordAssignment` 透传 TTL | 90 | 140 |
| `services/scheduler/internal/lookup.go` | 改调共享 `AuthorityFor`，删本地副本 | 15 | 0 |
| `services/scheduler/internal/metrics.go` | 2 个计数器 | 40 | 20 |
| `services/gateway/internal/projection.go` | 新建：reader 装配 ＋ 合成 ＋ 回落 | 120 | 180 |
| `services/gateway/internal/server.go` | 读路径；`shouldRecordAssignment`；路由 id 快路径；🔴 `extractSandboxIDsFromResponse` 支持顶层数组 ＋ 返回三元组 | 140 | 200 |
| `services/gateway/internal/metrics.go` | 1 个计数器 | 25 | 20 |
| `services/gateway/cmd/main.go` | 构造 reader | 35 | 0 |
| `services/shared/config/config.go` | 3 个开关 ＋ gateway redis addr ＋ scheduler TTL 上限 | 110 | 90 |
| `services/api/proto/scheduler.proto` | 3 个字段 | 25 | 0 |
| **Go 小计** | | **~1,095** | **~1,290** |

（`services/api/proto/*.pb.go` 为生成代码，另计。）

### Rust

| 文件 | 动作 | 非测试 | 测试 |
|---|---|---|---|
| `config/default.toml` ＋ `src/cfg.rs` | 2 个配置项 | 20 | 0 |
| `src/orchestrator/store/metadata.rs` | `max_lifetime` ＋ `running_elapsed` / `running_since` ＋ `lifetime_deadline(now)` ＋ `sync_running_clock` / `restart_lifetime_clock` ＋ `_set_timeout` 钳制 ＋ `projection_ttl_secs()` | 70 | 150 |
| `src/orchestrator/types.rs` | 事件加 `execution_id` | 10 | 0 |
| `src/orchestrator/error.rs` | `SandboxLifetimeExceeded` | 10 | 0 |
| `src/orchestrator/service.rs` | 3 处构造点写 `max_lifetime`；`keep_alive_for` 的 400 分支；`publish_sandbox_event` ＋ 5 个调用点 | 90 | 160 |
| `src/observability/reporter.rs` | 事件化身；roster 带 TTL | 40 | 60 |
| `src/observability/service.rs` | roster 三元组 | 25 | 30 |
| `src/bin/server.rs` | 🔴 启动时序注释 | 10 | 40 |
| `src/api/openapi.yml` | 4 个操作的 `headers:`；2 个操作的 `400`；`SandboxForkResult.projectionTtlSecs` | 90 | 0 |
| `src/api/impls/sandbox.rs` | 4 个 handler 填头；2 个 handler 显式映射 400 | 110 | 90 |
| **Rust 小计** | | **~475** | **~530** |

生成代码：`src/api/generated/**` 重生成后 diff **约 600–900 行**；
`services/api/proto/*.pb.go` 约 **150 行**。两者都不手改。

### 部署

| 文件 | 动作 | 行 |
|---|---|---|
| `deploy/k8s/base/redis-*.yaml` | 🔴 **新建** Deployment ＋ Service（＋可选 PVC） | 110 |
| `deploy/k8s/base/gateway-deployment.yaml` | redis addr ＋ 2 个开关 env | 30 |
| `deploy/k8s/base/scheduler-deployment.yaml` | redis addr ＋ 1 个开关 env | 20 |
| `deploy/k8s/base/kustomization.yaml` | configMapGenerator literals ＋ `images:` | 25 |

### 合计

**~1,570 行非测试 ＋ ~1,820 行测试 ＋ ~1,050 行生成 ＋ ~185 行 YAML。**

**被低估的四处，按贡献排序：**
1. 提案假定 create/fork 已带化身（E1/E2），因此**给 Rust API 层记了 0 行** —— 实际约 675 行 ＋ 一轮 codegen。
2. §6.5 的 `KEEPTTL` 问题完全没进视野 —— 它单独带来 Lua ＋ 两条 proto 参数串 ＋ P5 探针。
3. 寿命上界被写成「一个 config 值即可」—— 实际是一个 metadata 字段、一处钳制、
   一个错误变体、两个 OpenAPI 响应、一轮 codegen。
4. Redis 本身没有部署对象（SD-B1），提案里不算成本但它是硬前置。

即使只算 ② 那一半（母提案 v2 估 300–400），实际也是 **~600 非测试 ＋ ~700 测试**。

---

## 11. 🔴 不能加法式改的地方

| # | 位置 | 为什么不是加法 | 处置 |
|---|---|---|---|
| **N1** | `redisReconcileNodeScriptBody` 的 TTL 语义（§6.5） | 这是**改行为**不是加分支。一个跑新脚本（`KEEPTTL`）的 scheduler 与一个跑旧脚本（`PX 30s`）的 scheduler 对着**同一个 Redis** 会互相打架：旧的每 5 秒把 TTL 打回 30 秒 | scheduler 今天 `replicas: 1`；滚动更新期间会短暂两个 primary。**接受这个 ≤30 秒的降级窗口**（只是 TTL 被重置，不是数据损坏），不要为它上 leader election。`--query-only` 副本不写，无影响 |
| **N2** | `SandboxMetadata.max_lifetime` / `running_elapsed` 的 serde | `persister.load_all` 在 `Orchestrator::new` 内（`service.rs:195`），旧记录缺字段 ⇒ **节点起不来** | 必须 `#[serde(default)]`。🔴 注意本文件的既有先例相反：`execution_id`（`metadata.rs:33-42`）**刻意不给** `serde(default)`。别照抄邻居 |
| **N3** | `ReportSandboxEvent` 从「保证 no-op」变成有副作用 | 所有在跑的节点**今天就在发**这些事件。scheduler 一升级就会开始改投影，而节点侧没有任何开关 | 写侧开关代码默认 `off`（§8）。这是它必须默认 `off` 的**主要**理由 |
| **N4** | `publish_sandbox_event` 加参数 | 签名变更，5 个调用点必须同批改 | 单 crate 内，编译器兜底 |
| **N5** | `authorityFor` 移到 `services/shared/routing` | 移动会删掉 `lookup.go` 现在拥有的符号 | 移动而非复制；两处不得并存；加跨包 golden 测试（§7.1） |
| **N6** | `/timeout` `/refreshes` 新增 400 | 客户端会收到一个它以前收不到的状态码 | 可达窗口 ≤1 秒（§9 P3），风险可接受；写进 CHANGELOG |
| **N6-b** | 寿命上界的单位（§6.1 订正） | 与 N6 同批进 CHANGELOG。对外规则是「累计**运行**不得超过 `max_sandbox_lifetime_secs`」；暂停时间不计入 | 只影响本阶段新引入的行为，没有更早的版本对它有依赖 |
| **N7** | `extractSandboxIDsFromResponse` 的返回类型 | 从 `[]string` 变成三元组 | 包内私有函数，调用点只有 `server.go:596`；其唯一测试（`server_test.go:665`）用的信封形状本仓不产出，🔴 **顺手改成真实的 fork 数组形状** |
| **N8** | proto 三个新字段 | 加法安全：消费者只有本仓 Go（`services/go.mod` 是唯一 module，`services/api/proto/` 未独立发布）与本仓 Rust（`build.rs:5,17`）。无外部消费者 | 直接加。`make -C services build` ＋ `cargo build` 各重生成一次 |

---

## 12. 前置、范围外、以及登记回母提案

### 硬前置

| # | 项 | 状态 |
|---|---|---|
| **B1** | 🔧 **已全解**（原文：「已解一半」）。`deploy/k8s/base/redis.yaml`（提交 `77aa98f`）建了 Deployment ＋ Service ＋ PVC，`scheduler-deployment.yaml:82-83` 注了 `SCHEDULER_REDIS_ADDR`，日志已是 `binding_store="redis"`；🔧 **gateway 侧的 addr 也已接上**（`gateway-deployment.yaml:100-101`，集群实测同值，见 §13.7 C2）—— ②（gateway 直读）已在集群上验过（§13.2 A 相位）| 🔴 **仍然成立**：**不要把 Redis 凭据发到 node** —— 那会撤销上一轮 G7 摘掉 node 侧 PG 凭据的成果 |
| **B2** | `make k8s-apply` 会把化身开关退回 `observe`/`off`（D-11） | 本阶段新增三个开关会成为 D-12 / D-13 / D-14。发布只用 `set image` ＋ `set env`，不碰 ConfigMap（§7.2 的纪律） |
| **B3** | scheduler 滚动更新的数据面 503 窗口（binding 在内存里）。🔧 **窗口是 2.3–3.3 秒，不是 14 秒** —— 同法重测，见 [`_sd-recon-env.md`](_sd-recon-env.md) §4.5 | 一旦 Redis 到位，**这条顺带解掉**（重测确认：配上 `SCHEDULER_REDIS_ADDR` 后同一次滚动 0 次 503）—— 也是先做 B1 的额外理由。🔴 **但它不覆盖「缺席」** —— scheduler 缺席 > `binding_ttl` 的表现是 **404 而不是 503**，Redis 不修，见 `_sd-recon-env.md` SD-B6／§9.1。**②的验证探针必须按状态码分类计数** |

### 明确不在阶段 1 范围内

1. 🔴 **把心跳 roster 收窄成「只报本节点能服务的沙箱」**（§5.4）。
   它会让 PAUSE 事件真正承重，但 `rosterHolder` 回落、`NodesHolding`、
   暂停沙箱接管都依赖当前语义，改它是一次独立的、要单独验证的手术。
2. **给事件通道加重试 / 有序保证。** 事件是尽力而为的，心跳是修复路径 ——
   这是设计，不是缺陷。
3. **`x-agentenv-execution-id` 在数据面之外的其它语义。**
   本阶段只是把它从「数据面 echo」扩成「控制面 201 也带」，
   fencing 的判定规则（`decideFencing`）一行不改。
4. **`silentExecution` 的自动联动**（§7.2）—— 用运维约束顶着，不加机制。

### 登记回母提案的修改

| 目标文件 | 修改 |
|---|---|
| `2026-08-20-service-decomposition.md` §7 阶段 1 ①.1 | 「CREATE / FORK 不动」改为「触发条件不动，写入内容要改」，并写明 E1/E2 |
| 同上 ①.4 表第二行 | 「续期时拒绝 ⇒ 400」改为「续期时**钳制**；400 只给已越界」，并更正 `keep_alive.go` 的包路径 |
| 同上 ①.4 | 增补「心跳对账必须停止重置 TTL」一条（§6.5） |
| 同上 ①.4 | 🔴 上界的单位改为**累计运行时长**（暂停不计入），见 §6.1 订正块。CHANGELOG 里对外的说法是「一台沙箱累计运行不得超过 `max_sandbox_lifetime_secs`」，**不是**「创建 24 小时后不能再 resume」 |
| 同上「规模」 | 400–500 → ~1,570 非测试 ＋ ~1,820 测试 ＋ ~1,050 生成 |
| 附证据索引 | 「CREATE / FORK 的投影写已经是同步的，**且带化身**」删去后半句，改指 `src/api/proxy.rs:95` `:352-364` |
| `2026-08-20-module-responsibilities.md` D10 表 | RESUME 一行补「需要 Rust 侧响应头，否则在 `enforce` 仲裁下被静默拒绝」 |
| `_sd-recon-env.md` §7.4 | D-11 复核清单加三个新开关；补一条「`SCHEDULER_ROUTING_EXECUTION_ARBITRATION` 与 `GATEWAY_ROUTING_PROJECTION_READ` 必须成对」 |
| 🔧 `_sd-recon-env.md` §7.4 | **已做**（2026-08-20 晚）：三个开关落地成 **D-12 / D-13 / D-14**，成对约束也补了。见 `_sd-recon-env.md` §7.4 ⑦ 与 §11.4 |
| 🔴 `2026-08-20-service-decomposition.md` §7 阶段 1「开关与回退」 | **新增 SD-D1**：写侧开关 `off → on` 会打出一段 404（§13.4）。母提案里「开关可以随时翻」这个前提要改成「**翻开是维护事件、翻回是免费的**」 |
| 🔴 `2026-08-20-service-decomposition.md` / 模块文档里凡引用 F4 的地方 | F4 的框架订正（§13.5）：制造 F4 的是**写**开关，读开关既不制造也不加宽它。**凡是「因为 F4 所以门控读开关」的说法都要改成门控写开关** |

---

## 13. 🔧 集群验收记录（2026-08-20 晚，`pve-sg dev` 203/204）

> **阶段 1 已在 dev 集群通过验收**：P1 / P1-b / P2 / P3 / P3-b / P4 / P5 / P6 全部 PASS，**无 VOID**。
> 本节是验收回执，**不改前面的规格**；现场与规格不一致的两处单列在 §13.7。
>
> 🔴 **本节里重要的不是那些 PASS**，是三件事：
> **§13.4 的新缺陷**（翻开关会打出一段 404）、**§13.5 对 F4 的框架订正**（该被门控的是**写**开关）、
> 以及 **§13.6「这一轮没有证明什么」**。
>
> 证据目录 `$WD = /tmp/claude-1000/-home-debian-AgentENV/6fffa6a8-45b2-4e85-acd7-d41688c06560/scratchpad/p1/`
> —— 🔴 **临时目录，会被清掉；下面表里的数字就是它的全部内容**。时间戳一律 UTC。
> 环境侧的发现（哪些手法在这套集群上根本不生效）在
> [`_sd-recon-env.md`](_sd-recon-env.md) §11，**本节不重复**。

### 13.1 基线

| 项 | 值 | 出处 |
|---|---|---|
| 三个服务的镜像 | `10.10.10.204:5000/agentenv-{runtime,gateway,scheduler}:sd1-1f79e8f`，**三个同 tag** | `$WD/baseline-images.txt` |
| 源码 | `1f79e8f`（tag 里的就是它） | `git rev-parse --short HEAD` |
| 开关（验收前） | 三个全 `off` | `$WD/baseline-cm-routing-projection.yaml` |
| **开关（验收后）** | **三个全 `on`，留在 `routing-projection-config` 里没有翻回去** | 同上 CM |
| Redis 接线 | scheduler 与 **gateway 都有** `…_REDIS_ADDR=agentenv-redis:6379` ⇒ §12 硬前置 **B1 已全解** | `$WD/baseline-env-agentenv-{gateway,scheduler}.json` |
| 沙箱 | 10 台 running，`timeout=3600`、`autoResume.enabled=true`，两台节点各 5 台 | `$WD/placement.txt`、`master-fleet.txt`、`worker-fleet.txt` |
| 负载 | 每台 1 rps 的数据面代理请求（`204` 为成功） | `$WD/probe.py` |

### 13.2 头号判据（P1）：控制面停 5 分钟，数据面不掉

| 相位 | 动作 | 数据面 | gateway `route_resolution_total` |
|---|---|---|---|
| 基线 | 不动，60 秒 | **600 / 600 × 204** | — |
| **A（处理组）** | 三个开关全 `on`，`scale deploy/agentenv-scheduler --replicas=0`，**300 秒**（18:37:51.11Z → 18:42:51.61Z） | 🟢 **4200 / 4200 × 204**，非 204 **0 次** | `{redis_hit}` **+4200**；`{redis_miss}` +0；`{scheduler}` +0 |
| **B（对照组，必须失败）** | `SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE=off` ＋ rollout ＋ 等记录 TTL 回到 30 秒，再 scale 0，**同样 300 秒**（18:47:30.47Z → 18:52:30.96Z） | 🔴 **1190 / 4200 × 204（28.33%）**，**3010 次 503**，连续覆盖 18:47:56.54Z–18:52:56.55Z（301 个采样秒） | `{redis_hit}` **+1190**、`{redis_miss}` **+3010**、`{scheduler}` **+0** |

- 🔴 **A ≠ B，探针有分辨力**（[`_sd-recon-env.md`](_sd-recon-env.md) §8 第 1 条）。
- 🔴 **两侧互证**（同 §8 第 3 条）：B 相位 `redis_miss` 的 **+3010** 与数据面 503 的 **3010** 逐个对上。
- 🟢 B 相位 scheduler 侧指标一条都没动（进程 0 副本）—— 对照面自己也自证了。
- 🔴 **B 相位失败的形状是 503，不是 404**（`codes.Unavailable` ⇒ `services/gateway/internal/server.go:433-434`）。
  这**没有**推翻 SD-B6：SD-B6 说的是「scheduler 回来之后、心跳还没到」那一段，
  本轮的 B 相位停在 5 分钟窗口内取样，**没有覆盖恢复段**。见 §13.6 第 4 条。

证据：`$WD/p1-baseline.csv`、`p1-A.csv`、`p1-B.csv`、`p1-A-marks.txt`、`p1-B-marks.txt`、
`m-{A,B}-{a,b}-{gw,sched}.txt`（用 `$WD/mdiff.py` 取差值）。

### 13.3 逐探针

| 探针 | 结果 | 正面 | 🔴 对照面（必须失败的那一面） |
|---|---|---|---|
| **P1** | ✅ | 见 §13.2 A | 见 §13.2 B |
| **P1-b** | ✅ | 手工 `DEL` 一把 binding key 后立刻打一次数据面：**成功**；gw `{redis_miss}` +1、`{scheduler}` +1；sched `lookup_node_total{result="bound_roster"}` +1（**两侧 1 = 1**） | 对一个**从不存在**的 sandbox id 打同一请求 ⇒ **404**；gw `{redis_miss}` +1、**`{scheduler}` +0**；sched `{result="not_found"}` +1 |
| **P2** | ✅ | 上界 300s ＋ `timeout=3600` ⇒ `endAt - startedAt = **300.06s**`（`p2-probe.txt`） | 上界 0 ⇒ 同一请求 `= **3600.23s**`（`p2-control.txt`） |
| **P3** | ✅ | 上界 300s、create `timeout=60`、t=30s 时续 3600 ⇒ **HTTP 204（不是 400）**，`endAt - startedAt = **300.22s**`（`p3-probe.txt`） | 上界 0 ⇒ 同一调用后 **3630.63s**，越过 300s 线（`p3-control.txt`） |
| **P3-b** | ✅ | 暂停 **330 秒**（> 300s 上界）后 resume ⇒ 201，**等到 +15s 仍是 `running`**（`p3b-probe.log`） | 同一台把 **300 秒运行预算跑满** ⇒ resume 之后约 1 秒回到 `paused`（`p3b-control.log`） |
| **P4** | ✅ **四发** | ① 正常 `DELETE` ⇒ `sandbox_event_total{event_type="delete",outcome="deleted"}` **+1**（`m-p4-a/b`） | ② **真行真化身**的旧化身 E1 事件 ⇒ `rejected_stale` **+1**、key 存活（`m-p4c-a/b`）；③ `execution_id=""` ⇒ `ignored_unknown_execution` **+1**、key 存活（`m-p4c-b/c`）；④ 🔴 **收尾一发**：换成正确化身 ⇒ `deleted` **+1**（`m-p4c-c/d`）—— 这一发证明 ②③ 是**真拒**，不是守卫整体瞎了 |
| **P5** | ✅ | 上界 86400：create 后 `PTTL = 86,459,425 ms`；40 秒后 `86,419,630 ms`，**少了 39,795 ms**，全程 **0 次回升**（`p5-ttl.csv`，10 Hz × 40s）。载体也当场坐实：201 响应头 `X-Agentenv-Projection-Ttl-Secs: 86460`（= 86400 ＋ 60 grace，`p5-hdr.txt`） | 写侧 `off` ⇒ 同样采样 60 秒，PTTL 在 **25.0–30.0 秒**之间来回，**0 次缺席**（`p5c-ttl.csv`，599 采样）—— 每个心跳都在重置 |
| **P6** | ✅（**替代对照面**） | 5 次 trial：从 resume 的 201 到「记录出现且带**新**化身」= **0.007 – 0.18 秒**，全部早于下一发心跳；首见 PTTL ≈ **86.0×10⁶ ms**（长 TTL，不是 30 秒）（`p6-on.txt`、`p6-on-{1..5}-keys.csv`） | 🔴 规格写的对照面（把 node 回滚到 `cp3-bff4993`）**没有做**。用的是替代面，射程小得多，见 §13.6 第 2 条 |

### 13.4 🔴 新缺陷 **SD-D1**：把写侧开关 `off → on`，会在活着的沙箱上打出一段 404

**测出来的，带因果闭合。** 10 台沙箱 / 1 rps 的规模上：

| 时刻（UTC） | 发生了什么 | 证据 |
|---|---|---|
| 18:56:22.76 / :27.86 / :32.86 | 记录仍被每 5 秒一次的心跳按 `PX ~30s` 续期（PTTL 三次跳回 ~29.9 秒） | `f1-onflip.csv` |
| **18:56:32.86 之后** | 🔴 **再没有任何一次续期**：新 scheduler 接手对账，`KEEPTTL` **忠实地保留了 OFF 时期装进去的那个短 deadline**。此后 PTTL 严格单调递减 | 同上 |
| 18:57:00.39 | 一台节点漏掉一发心跳（滚动期竞争），退避 5 秒 | 当场读的 node 日志，**未存档** |
| 18:57:02.79 | 短 deadline 到期，记录消失（最后一次可见是 18:57:02.743Z，`PTTL = 48 ms`） | `f1-onflip.csv` |
| **18:57:02.84 – 18:57:05.54** | **记录持续缺席 2.7 秒**（28 个采样 @ 10 Hz） | 同上 |
| 18:57:03.13 – 18:57:05.14 | 🔴 **数据面 15 次 404，落在 5 台活着的、健康的沙箱上** | `f1-onflip-dp.csv` |
| 18:57:05.64 | 下一发心跳把记录按**长 TTL** 重新装入（`PTTL = 85,178,945 ms` ≈ 23.66 小时），自愈 | `f1-onflip.csv` |

> ⚠️ 一处**报告与采样的小出入**，不影响结论：新 scheduler 报 Ready 是 **18:56:25Z**（当场读的 rollout 状态），
> 而最后一次 `PX ~30s` 续期落在 **18:56:32.86Z** —— 差的这 7.5 秒是滚动更新期旧 Pod 还在对账。
> 「**从某一刻起再也不续期**」这件事本身是采样直接给出的（此后 PTTL 严格单调递减），
> 起点取 18:56:32.86 是保守的那一头。

🔴 **因果闭合**：那 5 台 404 的沙箱**正是漏心跳那台节点上的 5 台**
（`master-fleet.txt` / `placement.txt` 逐个对得上）；另一台节点的 5 台在同一窗口
**500 / 500 × 204**。总计 985 / 1000 × 204 ＋ 15 × 404。

🟢 **反方向是免费的**：同法测的 `on → off`（18:45:22.14Z `set env`，18:45:34.01Z rollout 完成）——
TTL 在 rollout 完成后 **2.42 秒**从 ~23.9 小时塌回 **29,920 ms**（18:45:36.43Z），
90 秒采样窗口内 **0 次缺席**（`f1-collapse.csv`，899 采样）。记录一次都没消失过 ⇒ 404 那个形状**没有成因**。

> ⚠️ `on → off` 这一发**没有留数据面 trace**，「0 次失败」是从「记录一次都没缺席」推出来的，不是直接测的。

**机理**（决定它能不能修）——`services/scheduler/internal/redis_store.go:587-606`：

```lua
if keeps_deadline(incumbent, execution_id, entry_ttl_n) and redis.call("PTTL", key) > 0 then
  redis.call("SET", key, value, "KEEPTTL")
else
  redis.call("SET", key, value, "PX", entry_ttl_ms)
end
```

对**已存在**的记录保留旧 deadline，这在稳态正是 §6.5 要的；但在 `off → on` 的那一刻，
「旧 deadline」恰好是 OFF 时期装的那个 30 秒。长 TTL 只有走 `else` 分支才写得进去，
而在 `KEEPTTL` 一直命中的情况下，**只有记录先死一次、变成「不存在」，下一发心跳才会给它长 TTL**。
⇒ **缺席是拿到长 TTL 的必经之路。**

🔴 **由此得出的运维规则，写进发布计划**：

1. **`off → on` 这一翻要当成一次维护事件排期。** 回滚（`on → off`）便宜，
   **重新打开才是要付钱的那一次** —— 这与「开关哪个方向危险」的常识相反。
2. 窗口宽度由「短 deadline 到期」到「下一发心跳」决定（本轮 **2.7 秒**）；
   **受影响的沙箱数 = 翻开关时带着短 TTL 的存量记录数**。空集群上翻不会有任何症状，
   **别拿空集群的一次成功当证据**。
3. 🔴 **窗口宽度实际由「节点退避多久」决定，而漏心跳是这一翻自带的**：翻开关**必然**滚 scheduler，
   滚动期节点就会漏心跳并进入退避（本轮正是如此，退避 5 秒）。最坏是退避封顶 **60 秒**
   （`src/observability/reporter.rs:20`）⇒ **别在同一个时间窗口里再叠别的滚动**（滚 node、滚 gateway），
   叠上去就是把退避叠上去。

**没修，登记为 SD-D1。** 两个方向的缓解手法都**没有验证过，别当结论**：
① 翻完立刻对每台在跑的沙箱触发一次带 TTL 的写；
② 在 Lua 里把条件收紧成「PTTL 还剩的比这次带来的预算短很多就重设」——
但那会重新引入「心跳能延长记录寿命」的语义，与 §6.5 正面冲突，要单独想。

### 13.5 🔴 F4 的框架订正：造成 F4 的是**写**开关，不是读开关

**此前记的是**：「读开关打开之后，一次整机猝死会让一条路由被钉住整个 TTL」。
**这个框架是错的，而且它会让人去门控错的那个开关。**

| 事实 | 证据 |
|---|---|
| 读开关 `off` 时，gateway 去问 scheduler，**scheduler 读的是同一条 Redis 记录**，答出来的是**同一个死节点** | `services/scheduler/internal/lookup.go:150-169`：第 1 步就是 `store.Get`，命中即 `return`，注释逐字写着 *"This is the hot path — every proxied request lands here — so nothing below it may run on a hit."* |
| 现场坐实 | 写一条指向**错误节点**的 binding，再直接对 scheduler 打一次 `LookupNode`：答的是 `bound_binding` / `SANDBOX_LOCATION_BOUND` ＋ **那个错误 endpoint**（`$WD/m-f4b-{a,b}-sched.txt`：`binding_execution_total{decision="refreshed",source="assignment"}` +1 ⇒ `lookup_node_total{result="bound_binding"}` +1） |
| 🔴 **两条读路径都没有任何东西去交叉核对一条陈旧 binding** | gateway 直读：`services/gateway/internal/projection.go:53-58`，命中即 `routing.Synthesize(record)` 返回；scheduler：同上 `lookup.go:150-169` |

⇒ **真正的严重度差在写开关上**：

| 位置 | 一条指向死节点的记录能活多久 |
|---|---|
| 写开关 `off`（两个读开关任意） | **30 秒**（`binding_ttl`）到期，之后由**带活性判定**的心跳 roster 兜底 |
| **写开关 `on`** | **最长 ~24 小时**（投影 TTL ＝ 沙箱寿命），**与读开关无关** |

🔴 **如果要为 F4 门控某个开关，门控的是写开关**（`*_ROUTING_PROJECTION_AUTHORITATIVE`），
不是 `GATEWAY_ROUTING_PROJECTION_READ`。读开关既不制造 F4，也不加宽它。

🟢 **节点回来之后的修复是 ≤5 秒（一发心跳），实测**：§13.4 那次缺席就是被下一发心跳
在 2.7 秒内重新装回去的。F4 疼的是**节点不回来**那一支。

🔴 **注意射程**：本条订正的是**框架**（哪个开关制造了它、有没有活性判定），
这两件都是**代码取证 ＋ 现场坐实**。而「~24 小时」这个**窗口本身仍然是推出来的**，
不是从一台真死掉的节点上量出来的 —— 见 §13.6 第 1 条。

🔧 **升级（2026-08-20 夜，§13.9）：上面这张表把后果说小了。**
一条指向死节点的记录**不只是「一条会活满预算的坏路由」** ——
它还**挡住了修复本身**：现场那台沙箱已经在另一台健康机器上以新化身救回来了，
而 resume 请求仍被这条陈旧记录送去尸体，**502**。
🔴 **F4 ＝ 对修复的拒绝**，不是「一段时间的坏路由」。取证、清扫的双向对照面、
以及那一次必须照实登记的人工 `redis-cli DEL`，全部在 §13.9。

### 13.6 🔴 射程边界：这一轮**没有**证明什么

**这一节比上面所有 PASS 都重要。**

1. **一次真正的整机猝死。** F4 的窗口是「实测的『TTL 再也不被续期』＋ 实测的『读路径上没有任何活性判定』」
   两件事推出来的，**不是从一台真死掉的节点上量出来的**。
   🔴 这套集群目前**没有**制造整机猝死的可用手法，原因与唯一的建议路线见
   [`_sd-recon-env.md`](_sd-recon-env.md) §11.2。
   🔧 **已关闭（2026-08-20 夜）**：手法找到了（recon §11.6），猝死也真的做了一次。
   现在的正面证据是**节点死后两条记录都还在、且带着 ~24 小时的 TTL**；
   🔴 但「一条记录真的活满 24 小时」**仍然没有实测**（§13.9 末尾第 4 条）。
2. **P6 规格里写的那个对照面。** 没有把 node 回滚到 `cp3-bff4993`，用的是替代面
   （对着**活着的在位者**发一次**不带化身**的 `RecordAssignment`，被静默拒绝 ⇒
   `binding_execution_total{decision="rejected_unknown",source="assignment"}` +1）。
   `cp3-bff4993` 那个镜像的行为**只有静态取证**：
   `git show bff4993:src/api/openapi.yml | grep -c x-agentenv-execution-id` = **0**，
   而 HEAD（`1f79e8f`）= **5**。⇒ 「那个镜像不发这个头」是真的，
   「所以那个序列会落成 `rejected_unknown` 且 5 秒内没有记录」**没有在集群上跑过**。
3. **P3 的 400 分支。** 只有单元测试，**存在性确认、没有重跑**（§9 P3 早已写明它端到端不可稳定复现）。
4. **「缺席」这条路径在当前开关位置下还会不会出 404。** B 相位整整 301 秒**全程是 503**，
   没有覆盖 SD-B6 说的恢复段。本轮唯一一次 404 是**翻开关**打出来的（§13.4），
   不是「scheduler 缺席」打出来的。⇒ **SD-B6 的兑现判据尚未被这一轮证伪或证实。**
5. **任何超过「10 台沙箱 × 1 rps × 5 分钟」的负载。** 两台节点全程都远没吃满
   —— 这句话对容量**什么都没说**，本轮也没有做任何容量取样。
6. 🔴 **Redis 自己出故障。** `route_resolution_total{source="redis_error"}` 全程 **0**，
   而且**一次都没有被顶起来过** ⇒ 按方法论第 2 条（[`_sd-recon-env.md`](_sd-recon-env.md) §8），
   这条 series 是 **UNVERIFIED，不是「零错误」的证据**。
   读失败的回落分支（`projection.go:43-52`）**在集群上从未被执行过**。
7. **多副本 scheduler / HA（`--query-only`）。** `P-A5-2` 仍然**结构性未跑**。
   Redis 到位之后它已经**跑得了**，只是这一轮没跑。

### 13.7 现场对规格的两处修正

| # | 规格怎么写的 | 现场 | 处置 |
|---|---|---|---|
| **C1** | §8：「删 key（`optional: true`）与写空串都要等 Pod 重启才生效，且删 key 落回代码默认」 | 仓内清单**确实**是 `optional: true`（`deploy/k8s/base/gateway-deployment.yaml:129-144`、`scheduler-deployment.yaml:169-174`），**但集群里在跑的那份 Deployment 上没有这一行**（`$WD/baseline-env-agentenv-{gateway,scheduler}.json`：三条 `configMapKeyRef` 都不带 `optional`，而既有的 fencing 三条都带） | 🔴 **在这套集群上「删 key」不是落回代码默认，是 Pod 起不来**（`CreateContainerConfigError`）。回退**一律 `kubectl set env`**，§8 的那条命令本身仍然正确 |
| **C2** | §12 硬前置 **B1**：「还差 gateway 侧的 addr」 | ✅ **已解**：`gateway-deployment.yaml:100-101` 与集群里在跑的 Deployment 都有 `GATEWAY_REDIS_ADDR=agentenv-redis:6379`；且它是**读开关还关着的时候就先配上的**（读开关 `on` 而没有 addr 时 gateway 拒绝启动） | B1 关闭 |

### 13.8 交叉核对时挖出来的两个坑（都会让下一发探针骗人）

**坑 1 —— `route_resolution_total{source="scheduler"}` 只在回落「成功」时才 +1。**

失败的那一次只被 `redis_miss` 记到。代码：`services/gateway/internal/server.go:305-313` ——
`LookupNode` 出错就 `writeSchedulerError` 直接 `return`，
而 `recordRouteResolution(source)` 在它**后面**一行，够不着。
现场两处坐实：P1-b 的对照面（不存在的 id）`redis_miss` +1 而 `scheduler` **+0**；
§13.2 的 B 相位 3010 次失败回落，`scheduler` 同样 **+0**。

⇒ 🔴 **不要把 `source="scheduler"` 读成「回落尝试次数」，它是「回落成功次数」。**
要数尝试，用 `redis_miss + redis_error`。
（`metrics.go:80` 那条对账等式 `Δ{redis_miss} + Δ{redis_error} ≈ Δ scheduler_lookup_total` 不受影响，
它对的是 scheduler 那一侧，不是 gateway 的 `{scheduler}` 标签。）

**坑 2 —— 不带化身的写「可以装上」，它只是不能「顶掉」在位者。**

| 做法 | 结果 |
|---|---|
| 先 `DEL` key，再发一次不带化身的 `RecordAssignment`（`$WD/m-p6c2-{a,b}-sched.txt`） | `binding_execution_total{decision="installed_unknown",…}` +1 —— 🔴 **装上了** |
| 对着**活着的在位者**发同一个写（`$WD/m-p6c3-{a,b}-sched.txt`） | `binding_execution_total{decision="rejected_unknown",…}` +1 —— **被拒** |

⇒ 🔴 **一个「先删 key 再写」的探针什么都证明不了**：它删掉的正是守卫要保护的那个东西。
这是 [`_sd-recon-env.md`](_sd-recon-env.md) §8 第 1 条在本轮的又一个具体形态。

---

### 13.9 🔧 第二轮补记（2026-08-20 夜，＝ recon 的第三轮）：F4 的清扫在集群上跑起来了，**顺带把 F4 自己的严重度改了**

> 这一轮的主题是阶段 2a（记录在 [`_sd-impl-phase2.md`](_sd-impl-phase2.md) §13），
> 但同一发镜像里带上了 F4 的心跳超时清扫，于是 F4 第一次有了**真节点猝死**的现场。
> 证据目录 `$WD2 = …/scratchpad/sd2a/`（同样是临时目录）。
> 制造猝死的手法本身、以及那条**读起来像通过的错误手法**，在
> [`_sd-recon-env.md`](_sd-recon-env.md) §11.6，**本节不重复**。

#### 🔴 F4 比 §13.5 记的更重：陈旧记录**不只是一条坏路由，它还挡住了修复**

§13.5（以及母提案与模块文档里凡引用 F4 的地方）的框架是
「一条指向死节点的记录会活满它的预算（写开关 `on` 时最长 ~24 小时）」——
**这个框架是对的，但它把后果说小了。**

现场发生的事：一台节点猝死，它上面暂停过的沙箱 **S** 在**另一台活着的节点上**
被重新拉起（新化身）。这次 resume **本该成功**，因为承接它的那台机器是健康的。
🔴 **但请求被路由到了那具尸体上，返回 `502 upstream unavailable`。**

⇒ **陈旧记录不是「一条过期的路由」，它是「对修复本身的拒绝」**：
沙箱越是能被救回来，这条记录越是挡在救援路上。
🔴 **凡是描述 F4 的地方，都要按这个严重度改写** —— 包括本文 §13.5、
[`_sd-recon-env.md`](_sd-recon-env.md) SD-B7、以及母提案/模块文档里的引用。

#### 清扫在集群上的双向取证（含对照面）

清扫按「沙箱 id ＋ 死节点自己报过的那个化身」这一对去退休记录，
守卫是 `BindingStore.Delete`（与 pause 事件走的是同一个守卫）。
三台沙箱同时在场，覆盖两个方向 ＋ 一个不该被碰的对照面：

| 沙箱 | 处境 | 清扫之后 |
|---|---|---|
| **S** | 暂停在死节点上，**已在另一台机器上以新化身救回** | 🔴 记录**原封不动**，数据面 **204** |
| **U** | 节点死时正在运行，**从未被救回** | 记录**被删除**，之后 **404** |
| **W** | 全程活在**幸存**的那台节点上，从来不在死节点的 roster 里 | 记录**原封不动**，**204** |

计数器（`agentenv_scheduler_binding_sweep_total`）：

| series | 增量 | 意义 |
|---|---|---|
| `{outcome="rejected_stale"}` | **1** | 🔴 **最吃劲的一条**：守卫拒绝退休 S 的记录，因为它已经指向新化身。这一条是 0 才该告警 |
| `{outcome="deleted"}` | **1** | U 的记录被退休 |
| `agentenv_scheduler_binding_sweep_nodes_total{outcome="swept"}` | **1** | 死的那一台被判定并处理了 |

节点静默 **302.55 秒**越过 300 秒阈值触发（`scheduler.binding_sweep_silence` 默认 5 分钟、
巡视间隔 30 秒：`services/shared/config/config.go:58-59`）。
🟢 **随后两个巡视周期计数器一动不动** ⇒ 「每份报告只处理一次」的簿记成立，
不会每 30 秒重删一遍。

#### 🔴 必须照实登记的一次人工干预

因为陈旧记录把 S 的 resume 路由到了尸体上（上面那条 502），
为了让真正的 resume 走下去，**对 S 的那条陈旧 key 手工发过一次 `redis-cli DEL`**
（**U 的没有碰**）。之后判定的一切都是真的，但**这次 `DEL` 属于本轮记录的一部分**：
读这份记录的人有权知道 S 那条链路上有过一次人手。

#### 🔴 这一轮**没有**证明什么

1. **只有 N=2 台节点。** 整机群守卫（`agentenv_scheduler_binding_sweep_nodes_total{outcome="suppressed_all_silent"}`，
   即「所有报到过的节点同时静默 ⇒ 更可能是 scheduler 自己的网络问题，不动路由表」）
   **一次都没有开火** —— 它在集群上属于 UNVERIFIED，不是「没问题」（§13.6 第 6 条同形）。
2. 🔴 **改名路径从未走到。** 一台机器换了名字回来，是**唯一一种「判错就会退掉一台健康机器的记录」**
   的情形，而它没有被演过。
3. **只测了 `binding_store=redis`。** 内存 store 的 `Delete` 守卫在集群上从未被执行过。
4. **窗口的上界仍然不是量出来的。** 现在有的正面证据是：节点死后**两条记录都还在**，
   并且带着 **~24 小时的 TTL** —— 这比 §13.6 第 1 条当时的「纯推」硬了一档，
   但「一条记录真的活满 24 小时」**依然没有实测**。

---

## 附：本文核过的 e2b 引用

| 母提案的引用 | 核查结果 |
|---|---|
| `sandbox-catalog/catalog.go:11-18` 五字段投影 | ✅ 逐字符合（`OrchestratorID`/`OrchestratorIP`/`ExecutionID`/`StartedAt`/`MaxLengthInHours`） |
| `catalog_redis.go` `DeleteSandbox` 的化身守卫 | ✅ 存在，🔴 但是 Go 侧 `GET`→比较→`DEL`，**有 TOCTOU**，别照抄（§5.3） |
| `api/internal/sandbox/store.go:43-44` 投影写必须同步 | ✅ 逐字：「should be called sync to prevent race conditions where we would know where to route the sandbox」 |
| `store.go:82-83` 入库钳制 | ✅ `if endTime.Sub(StartTime) > MaxInstanceLength { EndTime = StartTime.Add(MaxInstanceLength) }` |
| `keep_alive.go:28` `:31-32` 续期拒绝 ⇒ 400 | ❌ **半错**：`:28` 是 `getMaxAllowedTTL` = `min(timeLeft, duration)`，**钳制**；`:31-32` 的 400 只在沙箱**已经**越界时。且文件在 `internal/orchestrator/` 不是 `internal/handlers/`（§1 E3） |
| `orchestrator/lifecycle.go:15` `:36` `:38-39` 投影 TTL | ✅ `MaxLengthInHours: int64(MaxInstanceLength / time.Hour)`，`lifetime := Duration(MaxLengthInHours) * Hour`。🔴 除截断外还有**归零⇒永不过期**（§6.4） |
| `client-proxy/main.go:130` 直读 | ✅ `catalog := e2bcatalog.NewRedisSandboxCatalog(redisClient)` 在 `:130` |
| `client-proxy/internal/proxy/proxy.go:109` 冷路径 | ✅ `handlePausedSandbox` 的 `:109`：「catalog miss, attempting resume via api」 |
