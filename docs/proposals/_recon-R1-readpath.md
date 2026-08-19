# R1 侦察：阶段 0 + 阶段 1 的 Go 侧改造点清单

> 2026-08-19 · 只读侦察产物，配合 [`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md) §4「阶段 0」「阶段 1」阅读。
> 所有锚点为 `apps/AgentENV/` 下相对路径 + 行号，逐个文件读源码核对过。

---

## 0. 一页速查：要动的文件

| 文件 | 阶段 | 动作 |
|---|---|---|
| `services/api/proto/scheduler.proto` | 1 | `LookupNodeResponse` 加分类字段；`ScheduleRequestHint` 加 origin 亲和 oneof（二选一，见 §B.4） |
| `services/scheduler/internal/service.go` | 0+1 | `lookupNode()` 自由函数上收成方法；加登记表回落；加对账循环 |
| `services/scheduler/internal/store.go` | 0 | 新增 `RegistryReader` 接口（只读 PG），与 `BindingStore` 并列 |
| `services/scheduler/internal/{strategy.go,filter.go}` | 1 | origin 软偏好注入点 |
| `services/scheduler/internal/metrics.go` | 0 | 加 orphan/ghost/holder_conflict/lease_expiring/answered/sync_ok |
| `services/scheduler/cmd/main.go` | 0 | 装配只读 PG；query-only 分支也要装（🔴 见坑 2） |
| `services/shared/config/config.go` | 0 | 加 PG DSN 配置项 + env override + validate |
| `services/gateway/internal/server.go` | 1 | 删三段补丁 |
| `services/gateway/internal/schedule_hint.go` | 1 | 删 `maxReplayBodyBytes` / `captureReplayBody` / `restoreReplayBody` |
| `services/gateway/internal/reroute_test.go` | 1 | 3 个测试要重写或删 |
| `services/gateway/internal/server_test.go` | 1 | 3 个测试要重写或删 |
| `services/go.mod` | 0 | 加 PG driver（当前**零** PG 依赖） |
| `deploy/k8s/base/{scheduler-deployment,config/scheduler}.{yaml,json}` | 0 | 注入 DSN Secret；scheduler 当前**没有任何 Secret / env** |

---

## A. gateway 侧

### A.1 `handleProxy` 主流程与三段补丁

`services/gateway/internal/server.go:160-321` 是唯一入口（`Handler()` 在 `:103-137` 把除本地 `/health`·`/metrics` 外的一切都丢进来）。

**路由源判定**（`:195-211`）优先级：host 路由 → 控制面路径 → header；都没有则 `routeSourceSchedule`。
`setGatewayRouteSource(w, routeSource)` 在 `:211` 写的**不是响应头**，是 metrics label（`metrics.go:109-113`，写进 `statusRecorder.routeSource`，后写覆盖先写）。

#### 补丁 ①：recovery 分支 `server.go:217-243`

```go
resp, err := s.queryOnlyScheduler.LookupNode(routingCtx, ...)   // :219
recordGatewaySchedulerRPC("LookupNode", rpcStart, err)          // :220
switch {
case err == nil:                                   node = resp.GetNode()          // :222-223
case status.Code(err) == codes.NotFound && isPausedSandboxRecoveryRequest(r):     // :224
    recovery, recoveryErr := s.scheduleRecoveryNode(routingCtx, sandboxID)        // :231
    if recoveryErr != nil { s.writeSchedulerError(w, err); return }               // :232-237  ← 报的是原 lookup 错误
    node = recovery; recoveredSandbox = true                                      // :238-239
default: s.writeSchedulerError(w, err); return                                    // :240-242
}
```

- **触发条件**：`LookupNode` 返回 gRPC `NotFound` **且** 请求是 `POST /sandboxes/{id}/resume`。
  scheduler 侧只有 binding miss 且 warm-up 已过才会答 `NotFound`（`scheduler/internal/service.go:187-200`）。
- **写了什么**：无响应头。metrics 上多出一次 `agentenv_gateway_scheduler_rpc_duration_seconds{rpc="Schedule"}`
  （`scheduleRecoveryNode` 内 `:734`）。`route_source` label **仍是 `path`** —— 该分支不改 routeSource，
  所以今天从指标上**分不出**一次 resume 是否走了 recovery。日志有 Info `"routing resume of an unassigned sandbox to a scheduled node"`（`:745-748`）。
- **删掉会丢**：
  1. 「沙箱活得比节点久」—— 无 binding 的 resume 直接 404（`writeSchedulerError` 把 `NotFound`→404，`:387-388`）。
  2. `recoveredSandbox=true` 顺带在 `:310` 强制 `recordAssignment`，让新持有者**立刻**拿到 binding，
     不用等下一次心跳（binding TTL 30s，`store.go:11`）。删了这条，即使换别的方式选中节点，
     也必须补等价的 `RecordAssignment`，否则接管后 30s 内该沙箱继续 404。

#### 补丁 ②：`captureReplayBody` `server.go:285-298` + `schedule_hint.go:156-195`

```go
replayBody, canReplay := []byte(nil), false
if !recoveredSandbox && isPausedSandboxRecoveryRequest(r) {          // :291
    replayBody, canReplay, err = captureReplayBody(r)                // :293
    if err != nil { http.Error(w, "failed to read request body", 400); return }   // :294-297
}
```

- **触发条件**：是 resume **且不是**刚刚 recovery 选出来的节点（即走 binding 命中的那条）。
- `captureReplayBody`（`schedule_hint.go:165-183`）：缓冲 ≤ `maxReplayBodyBytes = 64<<10`（`:159`）。
  超限时把已读前缀用 `prefixedBody`（`:87-92`）缝回流上，返回 `canReplay=false` —— 即方案 §1.1 说的 "forfeits the reroute"。
  空 body（`http.NoBody`）返回 `canReplay=true`（`:166-168`）。
- **写了什么**：只在读 body 失败时写 400。无指标。
- **删掉会丢**：`allowReroute` 的唯一来源（`:313`）。删了它 ③ 就自动失效。

#### 补丁 ③：`rerouteToScheduledNode` `server.go:316-320` + `:323-376`

```go
if !rerouted { return }                                              // :316-318
s.rerouteToScheduledNode(w, r, routingCtx, sandboxID, replayBody, longLived)  // :320
```

- **触发条件**：`proxyRequest` 返回 `true`（判定见 §A.2）。
- **写了什么**：`setGatewayRouteSource(w, routeSourceSchedule)`（`:354`）—— 覆盖成 `schedule`，
  这是**唯一**能从指标上看出重路由发生过的痕迹（`gatewayHTTPDuration{route_source="schedule"}`）。
  外加第二次 `Schedule` RPC 指标 + Info 日志 `"rerouting declined resume to a scheduled node"`（`:355-359`）。
  第二跳固定 `recordAssignment: true`（`:372`）。
- **🔴 一个现成的观测缺口**：`proxyRequest` 在 `:521-523` 检测到 rerouted 就 `return true`，
  **跳过** `recordGatewayUpstreamProxy`（`:524`）。所以被拒绝的第一跳在
  `agentenv_gateway_upstream_proxy_duration_seconds` 里完全不存在。
- **删掉会丢**：隔离节点（draining）上 paused 沙箱的 resume 迁移能力。
  ⚠️ **node 侧不会自动停止发这个信号**（见 §A.2），删 gateway 这段等于把 503 直接透给客户端。

### A.2 `proxyRequest` 的 `rerouted=true` 判定

`server.go:440-451`（`ModifyResponse`）：

```go
if options.allowReroute &&
   resp.StatusCode == http.StatusServiceUnavailable &&
   strings.EqualFold(resp.Header.Get(headerReroute), rerouteReasonSchedule) {
    _ = resp.Body.Close(); rerouteRequested = true; return errRerouteRequested
}
```

三个条件全满足：`allowReroute` 为真（= 是 resume 且 body 缓冲成功）、上游 **503**、响应头
`x-agentenv-reroute: schedule`（常量 `server.go:35-36`）。`errRerouteRequested` 在 `ErrorHandler`
`:466-475` 被吞掉，**一个字节都不写给客户端**。

**信号的产生方（Rust 侧）**：`src/api/isolation.rs:41-79` 的 `resume_isolation_gate` 中间件。
仅当 ①`orchestrator.scheduling_disabled()`（节点被 admin API 隔离 = DRAINING）
且 ②`recoverable_elsewhere()` 为真（`:96-116`，`ClusterRegistration != Never`，即该沙箱**曾**被登记到 registry）。

> 🔴 **删补丁时这个信号会去哪**：`isolation.rs` 是 Rust 侧、本次不动的代码，
> 它**照发不误**。阶段 1 只有在「controller 选节点时就排除 DRAINING 节点」的前提下才安全 ——
> 而 `FilterUnschedulable`（`scheduler/internal/filter.go:23-33`）确实排除 DRAINING，
> 所以 `paused` 软偏好这条路是闭合的。
> **但硬钉 origin 的两条（`publishing` / `local_only`）不闭合**：origin 正在 draining 时，
> 请求被硬钉过去，节点 gate 判 `ClusterRegistration != Never` → 发 503+reroute →
> gateway 已经没有重放能力 → 客户端吃 503。
> 这是阶段 1 必须显式处理的边界（要么 Rust 侧 gate 对 `local_only` 放行，要么 controller 对
> 硬钉目标额外要求 `!draining`，二者都得写进方案）。

### A.3 各匹配函数覆盖的 URL / Host 形态

| 函数 | 位置 | 匹配 |
|---|---|---|
| `isPausedSandboxRecoveryRequest` | `server.go:719-727` | 仅 `POST /sandboxes/{id}/resume`（`strings.Trim(path,"/")` 后恰好 3 段，所以尾部斜杠也算） |
| `isSandboxControlPlaneRequest` | `server.go:753-776` | 2 段：`GET`/`DELETE /sandboxes/{id}`；3 段：`POST` 于 `pause,resume,fork,connect,timeout,refreshes,snapshots`；`PUT /sandboxes/{id}/network`；`GET`/`PATCH /sandboxes/{id}/custom-extension-params` |
| `shouldRecordAssignment` | `server.go:636-651` | 仅 POST。无 sandbox 时：`/sandboxes` 或 `/sandboxes-cold`；有 sandbox 且 `routeSource==path` 时：只有 `/sandboxes/{id}/fork` |
| `sandboxIDFromPath` | `server.go:687-708` | 前缀 `/sandboxes/`，取下一段；找不到前缀时退化为在**任意位置**搜 `/sandboxes/`（`:691`）—— 实际只被 `isSandboxControlPlaneRequest` 守卫后调用，那条 fallback 走不到 |
| `sandboxIDFromHeaders` | `server.go:653-661` | `x-agentenv-sandbox-id` → `e2b-sandbox-id`，取先非空者 |
| `parseHostRoute` | `host_route.go:22-73` | `{port}-{sandboxID}.{configured-domain}`。label **必须含 `-`**（`:43`），`strings.Cut(label,"-")` 首段作端口（`:50`），余下全是 sandboxID。端口非数字/越界/sandboxID 非法 DNS label → 返回 error（→ 400）。**不支持裸 `{sandboxID}.{domain}`**：UUID 自带 `-`，会被当成端口解析失败。测试见 `host_route_test.go:19-76` |
| `isNodeListRequest` / `isNodeAdminRequest` / `nodeIDFromPath` | `node_list.go:72-113` | `GET /nodes`；`GET`/`POST /nodes/{id}` |
| `isClusterListRequest` | `cluster_list.go:52-62` | `GET /sandboxes`、`GET /v2/sandboxes`（**扇出到全部节点**，`fetchClusterList:121`） |

数据面（header/host）路由会给上游路径加 `/proxy` 前缀（`server.go:800-820`），控制面/调度不加。

### A.4 `schedule_hint.go` 现状

- `buildScheduleHint`（`:19-47`）**只在 `!hasSandbox` 分支被调用**（`server.go:245`）。
  只认两条路径：`POST /sandboxes-cold` → `NewColdSandbox` hint、`POST /sandboxes` → `NewSandbox` hint；其余返回 `(nil, nil)`。
- oneof 结构：`scheduler.proto:32-54`
  - `ScheduleRequestHint.kind` = `new_cold_sandbox(1)` | `new_sandbox(2)`
  - `NewColdSandboxHint{cpu_count, memory_mb, images[], metadata}`；`NewSandboxHint{metadata}`
  - `ScheduleRequest{ reserved 1; hint = 2 }`（`:56-59`）
- body 读取/回填：`captureRequestBody`（`:63-82`）缓冲 ≤ `maxHintBodyBytes = 64KiB`（`:55`），
  正常路径 `io.NopCloser(bytes.NewReader(buf))` 回填并修正 `ContentLength`；
  超限走 `prefixedBody`（`:87-92`）拼回流并**跳过** hint 提取。解析失败一律降级为空 hint（`:111-132`、`:143-154`）。

> **阶段 1 的 origin 亲和 hint 往哪加**：注意 resume 路径上 gateway **今天根本不带 hint**
> —— `scheduleRecoveryNode` 发的是 `&schedulerv1.ScheduleRequest{}`（`server.go:733`），
> 空到连 oneof 都没有。这就是方案 §1.2 那个缺陷的字面证据。
> 如果阶段 1 按方案「controller 内部做 placement」实现，gateway 这条 `Schedule` 调用整个消失，
> hint 也就不需要加（见 §B.4 的两种方案对比）。

### A.5 gateway 现有指标

`services/gateway/internal/metrics.go:19-43`，统一前缀 `agentenv_gateway_`：

| 指标 | 类型 | labels |
|---|---|---|
| `agentenv_gateway_http_request_duration_seconds` | Histogram | `method, route, route_source, status` |
| `agentenv_gateway_upstream_proxy_duration_seconds` | Histogram | `route, status` |
| `agentenv_gateway_scheduler_rpc_duration_seconds` | Histogram | `rpc, status` |

- label 值全部**低基数化**：`method` 白名单（`:152-171`）、`route` 模板化（`:204-237`，未命中 → `unmatched`）、
  `status` 分桶为 `1xx..5xx`/`client_closed`/`other`（`:179-202`），`route_source` 空则 `unknown`（`:98-103`）。
- Buckets 统一取 `services/shared/observability.DurationBuckets`。
- 指标监听是**独立端口**（`gateway.metrics_listen_addr`，默认 `:9102`），公开 HTTP 上的 `/metrics` 恒 404（`server.go:126-131`）。

### A.6 依赖三段补丁的测试（删补丁必须一并处理）

| 测试 | 位置 | 依赖哪段 | 处置 |
|---|---|---|---|
| `TestResumeOfUnassignedSandboxRoutesToScheduledNode` | `server_test.go:2261-2303` | ① | 重写：改成断言 controller 直接给出 placement，`Schedule` 调用数应为 **0** |
| `TestPauseOfUnassignedSandboxDoesNotReschedule` | `server_test.go:2308-2330` | ①的对照 | 保留（阶段 1 后仍应 404 且不 Schedule） |
| `TestIsPausedSandboxRecoveryRequest` | `server_test.go:2232-2256` | 函数本身 | 函数删则测试删 |
| `TestDeclinedResumeIsReroutedToScheduledNode` | `reroute_test.go:22-87` | ②③ | 删；换成「503+reroute 头透传给客户端」的新契约测试 |
| `TestPlainServiceUnavailableIsNotRerouted` | `reroute_test.go:92-125` | ③ | 与上条合并 |
| `TestDeclineMarkerOnNonResumeIsNotRerouted` | `reroute_test.go:130-160` | ③ | 同上 |
| `TestNodeStatusChangeIsProxiedToTheNode` / `TestOtherMethodsOnNodePathAreNotProxiedToTheNode` | `reroute_test.go:164-242` | 无关（node admin） | 不动，但文件若改名要跟着搬 |

**测试脚手架（新测试直接复用这套）**：

- fake scheduler：`server_test.go:27-151` 的 `stubSchedulerClient`，13 个 `xxxFunc` 字段，
  未设置的方法调用返回 `fmt.Errorf("unexpected X call")` —— 天然的「不该被调用」断言。
- 构造 server：`newTestServer(t, client, timeout, maxRespSize, opts...)`（`:155-171`），
  option: `withSandboxProxyDomains`（`:173`）、`withDebugMode`（`:179`）、`withQueryOnlyScheduler`（`:185`）。
- fake upstream：`httptest.NewServer(http.HandlerFunc(...))` + `httptest.NewRecorder()`，
  或需要真 TCP 时 `httptest.NewServer(server.Handler())`（见 `:491`）。
- scheduler 侧：`scheduler/internal/service_test.go:16-28` 的 `failingBindingStore`（三个方法全返回 error）、
  `registerObservedNodeForTest`（`:30-52`，一次心跳把节点推成 READY）。

---

## B. scheduler 侧

### B.1 `LookupNode` → `lookupNode()` 完整语义

- `Service.LookupNode`：`service.go:168-170`，转调**包级自由函数** `lookupNode(logger, store, req, s.warmup)`。
- `QueryOnlyService.LookupNode`：`service.go:90-94`，同一函数但 `warmup` 传 `nil`。
- `lookupNode` 本体：`service.go:177-207`

| 情况 | 返回 |
|---|---|
| `sandbox_id` 空白 | `InvalidArgument`（`:178-180`） |
| `store.Get` 报错 | `Unavailable "binding store unavailable"`（`:183-186`） |
| miss + `warmup != nil` 且未 warm | `Unavailable "scheduler is still seeding sandbox assignments"`（`:188-197`） |
| miss + 已 warm（或 warmup 为 nil） | **`NotFound "sandbox assignment not found"`**（`:198-199`） |
| hit | `LookupNodeResponse{Node}`（`:201-206`） |

`warmupGate`（`warmup.go:41-101`）：warm 的定义 = 「至少一个节点报到过 **且** 当前非 lingering 的每个已知节点都有 observed 记录」（`:85-96`），
或超过 `deadline`（默认 15s，`:12`；配置 `scheduler.warmup_timeout`）。一旦 warm 就**永久锁存**（`:44-46`, `:98`）。
`reportedIn` 在 `Heartbeat` 里 **`ReconcileNode` 成功之后**才调（`service.go:265-274`），刻意的顺序。

> 🔴 **`NotFound` 的语义就是「binding 表里没有」**，不是「沙箱不存在」。
> 方案 §4 阶段 1 的 404 前提正是要修这一点。

### B.2 `BindingStore` 与心跳 reconcile

- 接口：`store.go:14-18`
  ```go
  Get(sandboxID string, now time.Time) (Node, bool, error)
  Record(sandboxID string, node Node, now time.Time) error
  ReconcileNode(node Node, sandboxIDs []string, now time.Time) error
  ```
- TTL：`defaultBindingTTL = 30 * time.Second`（`store.go:11`）；配置 `scheduler.binding_ttl`。
  内存实现按 `expiresAt` 惰性过期（`store.go:56-61`），Redis 实现交给 `PX`（`redis_store.go:245`、`:298`）。
- **reconcile 语义**（`InMemoryBindingStore.ReconcileNode`，`store.go:77-116`）：
  - roster 为空 ⇒ **删掉该节点名下全部 binding**（`:90-97`）。
  - roster 非空 ⇒ 名单内全部 upsert 并续期到 `now+TTL`（`:99-102`）；
    再把该节点名下**不在名单里**的删掉（`:104-114`）。
  - Redis 版语义等价，用两个 Lua 脚本保证原子（`redis_store.go:231-310`）；
    抢占换主时会从旧节点的反查 set 里 `SREM`（`:242-244`、`:293-297`）。
- 调用点：`Heartbeat`（`service.go:265`，roster 来自 `HeartbeatRequest.sandbox_ids`）、
  `UnregisterNode`（`service.go:424`，传 `nil` = 清空）。
- `RecordAssignment`（`service.go:209-245`）会先把 node.ID 归一到 registry 当前身份（`:217-222`），
  未知节点直接 `InvalidArgument`（`:223-230`）。

### B.3 `ListObservedNodes` / `ObservedNode` 的新鲜度

- 数据结构：`scheduler.proto:157-167`
  `ObservedNode{node_id, endpoint, cluster_id, service_instance_id, version, commit, machine_info, snapshot, last_seen_unix_ms}`。
- `NodeSnapshot`（`proto:127-150`）有 `sandbox_count` / `paused_sandbox_count` 等**计数**。
- `ListObserved`（`node_registry.go:285-300`）返回**全部** observed 记录（可按 cluster_id 过滤），
  **不做剔除**；新鲜度体现在派生出的 `status` 上：
  `deriveObservedNodeViewLocked`（`:411-445`）—— `now - last_seen > reportTTL`（默认 30s，`:35`）⇒ `UNHEALTHY`；
  不在 discovery ⇒ `CONNECTING`；lingering ⇒ `LINGERING`；否则保留节点自报状态。
- `PeekObserved`（`:375-387`）只给原始 `NodeSnapshot`，不派生状态，专供调度用。

> 🔴 **方案 §4 阶段 1 那条「回落读必须同时查一次 `ListObservedNodes` 的 roster」今天做不到**：
> `ObservedNode` **没有沙箱列表字段**。`HeartbeatRequest.sandbox_ids`（`proto:177`）的唯一消费者是
> `service.go:265` 的 `ReconcileNode`，用完即丢，从不留存。
> 也就是说「roster」和「binding 表」目前是同一份数据的同一个副本，查 roster 不能救 binding miss。
> 要实现方案那条，必须三选一：
> (a) 在 `observedNodeRecord`（`node_registry.go:38-42`）里存一份 roster + proto 加字段；
> (b) controller 主动 fan-out `GET /sandboxes` 到各节点（gateway 的 `fetchClusterList`，`cluster_list.go:121-160` 已有此模式）；
> (c) 承认 binding TTL 内的心跳抖动窗口无法消除，靠 `Unavailable`（而非 404）兜底。
> **这条必须在写代码前先定，否则阶段 1 的 404 前提是空的。**

### B.4 `Schedule` 打分链路与 origin 软偏好注入点

`Service.Schedule`（`service.go:96-140`）：

```
nodes.Snapshot(allowLingering=false)          // :102  丢弃 lingering
  → 每个节点配 PeekObserved 快照 → []RichNode // :103-109
  → FilterUnschedulable(rich)                 // :113  丢弃自报 DRAINING（filter.go:23-33）
  → FilterByResourceLimit(..., resourceLimit) // :113  资源阈值（filter.go:38-118）
  → strategy.Select(eligible, req.GetHint())  // :115
```

- `NodeResourceLimit`：`shared/config/config.go:42-58`，9 个可选阈值；
  判定在 `filter.go:58-118`（含 "including paused" 三条 `:99-116`）。无快照的节点**一律保留**（`filter.go:45-49`）。
- `Strategy` 接口：`strategy.go:13-16`，`Select(nodes []RichNode, hint *ScheduleRequestHint) (RichNode, error)`。
  **`RoundRobinStrategy`（`:22`）和 `RandomStrategy`（`:40`）都把 hint 参数丢弃（`_`）** —— 目前 hint 只进日志（`service.go:119`、`:144-154`）。

**注入点建议（最自然的签名）**：在 `service.go:113` 与 `:115` 之间加一层重排，而**不是**改 Strategy 实现：

```go
// scheduler/internal/affinity.go（新文件）
// preferNodes 把偏好节点提到候选队首；偏好节点不在 eligible 里时原样返回。
func preferNodes(eligible []RichNode, preferred []string) []RichNode
```

理由：① 两个现有 Strategy 都忽略 hint，改它们等于给每个策略重复实现一遍亲和；
② 软偏好的语义就是「候选集不变、顺序变」，与「选择算法」正交；
③ `RoundRobinStrategy` 的 `atomic.AddUint64` 取模（`strategy.go:26-27`）对入参顺序敏感，
放在 Select 之前重排即可自然生效，不用碰并发状态。

**两种阶段 1 形态的取舍**（方案 §4 没有定，实现前必须定）：

| 形态 | LookupNode 返回 | gateway 侧 | 代价 |
|---|---|---|---|
| **A：controller 内部 placement** | 直接返回最终 node（+ 一个「是否硬钉」的标记） | 只保留一次 `LookupNode`，`Schedule` 从 resume 路径彻底消失 | `lookupNode()` 必须从自由函数上收为 `*Service` 方法（要用 `s.nodes`/`s.strategy`/`s.resourceLimit`）；**`QueryOnlyService` 没有这些字段**（`service.go:77-81`），见坑 2 |
| **B：controller 只答分类，gateway 再 Schedule** | 返回 `state` + `origin_node_id` | gateway 按分类决定硬钉还是发带 hint 的 `Schedule` | 保持 query-only 可用，但 resume 变两次 RPC，且把语义决策留在了 gateway —— 与方案「gateway 退回纯入口」的目标相反 |

推荐 A，并把 query-only 副本的降级路径显式写进方案（见坑 2）。

### B.5 P2P artifact index：`Schedule` 确实不消费它

- RPC：`proto:16-18` + `service.go:326-385`（`RecordP2PArtifact` / `ForgetP2PArtifact` / `LookupP2PArtifact`）。
- 存储：`ArtifactStore` 接口 `store.go:20-25`；实现 `InMemoryArtifactStore`（`store.go:163-306`）——
  `map[artifactIndexKey]map[nodeID]struct{}` 正查 + `nodeKeys` 反查 + LRU 容量上限（默认 100 万，`store.go:12`），
  key = `(cluster_id, backend, key)` 三元组（`:308-315`）。**纯内存、进程重启即失**。
- **不消费的证据**：`s.artifacts` 在 `service.go` 全文只出现在 `:41`（构造）、`:73`（option）、
  `:339`（Record）、`:361`（Forget）、`:376`（Lookup）、`:431`（UnregisterNode 清理）。
  `Schedule`（`:96-140`）**一次都没引用 `s.artifacts`**，链路里也没有任何 artifact 相关过滤器
  （`filter.go` 只 import `schedulerv1` 和 `config`）。方案 §5.5 的判断成立。

### B.6 `cmd/main.go` 装配顺序

`services/scheduler/cmd/main.go`：

```
flag: -config, -query-only                              // :30-32
config.LoadScheduler(path, queryOnly)                   // :34
logging.New                                             // :39
signal.NotifyContext(SIGINT, SIGTERM)                   // :45
createBindingStore(logger, cfg) → BindingStore, closeFn // :48-49  （:164-178）
grpc.NewServer(UnaryInterceptor(MetricsUnaryInterceptor)) // :51
if query-only:  NewQueryOnlyService(logger, store)      // :52-55
else:           NewAtomicNodeRegistry(nil, ReportTTL)   // :57
                discovery: kubernetes goroutine / static registry.Set  // :58-67
                NewService(logger, registry, strategy, store, opts...)  // :69-80
                go svc.RunObservedNodesMetrics(sigCtx, 15s)             // :81
health server（"" 与 Scheduler 服务名两条）             // :85-88
net.Listen(GRPCListenAddr)                              // :90
metrics HTTP server（promhttp.Handler，独立端口）       // :101-110
graceful stop：health→NOT_SERVING，GracefulStop，10s 超时强停  // :131-151
```

**`--query-only` 下不装的组件**：`AtomicNodeRegistry`、discovery（k8s / static 全不跑）、
`Strategy`、`ArtifactStore`、`NodeResourceLimit`、`warmupGate`、`RunObservedNodesMetrics`。
只装 `BindingStore` + `QueryOnlyService`。

**Redis 接线**：`createBindingStore`（`:164-178`）—— `cfg.Scheduler.RedisAddr` 为空则内存，
否则 `NewRedisBindingStore`（`redis_store.go:28-61`，支持 `host:port` 与 `redis://` URL 两种，构造时 Ping 探活），
失败 `logger.Fatal`。`--query-only` 强制要求 Redis（`config.go:458-462`）。

**metrics server**：`promhttp.Handler()` 挂在 `cfg.Scheduler.MetricsListenAddr`（默认 `:9101`），
`:105-110` 起 goroutine，`ListenAndServe` 非 `ErrServerClosed` 即 `Fatal`。
⚠️ `deploy/k8s/base/scheduler-deployment.yaml` 里**没有暴露 9101 的 containerPort**，也没有 Service —— 阶段 0 加指标要顺手补。

### B.7 `shared/config/config.go` 的写法约定（阶段 0 加 PG DSN 照抄这套）

四段式，每段都要动：

1. **结构体 + `json` tag**：`SchedulerConfig`（`:60-73`）。
2. **自定义 `UnmarshalJSON`**（`:75-147`）：`wire` 影子结构全用**指针 / `json.RawMessage`**，
   逐字段 `if parsed.X != nil { s.X = *parsed.X }` —— 目的是让「未出现的 key」保留默认值，
   而不是被零值覆盖。`time.Duration` 一律走 `json.RawMessage` + `parseSchedulerDuration`（`:149-165`），
   **只接受字符串 `"30s"`，数字显式报错**。
3. **默认值**：`defaultConfig()`（`:286-319`）+ `applyDefaults()`（`:405-424`，兜底非法/空值）。
4. **env 覆盖**：`overrideWithEnv()`（`:321-391`）。字符串走 `set(key, *string)` 闭包（`:322-326`，空串不覆盖）；
   非字符串各自 `strconv` / `time.ParseDuration` 并**返回带原值的 error**（如 `:342-348`）。
5. **校验**：`validate(schedulerQueryOnly bool)`（`:430-506`），按 `c.Service` 分支；
   query-only 在校验 Redis 后**提前 return**（`:458-463`），跳过 discovery/artifact 校验。

> PG DSN 加法建议（保持一致性）：
> - `SchedulerConfig.RegistryDSN string \`json:"registry_dsn"\`` + wire 里 `*string`；
> - env `SCHEDULER_REGISTRY_DSN`（走 `set()`）—— 与 Rust 侧 `AENV_PAUSED_REGISTRY_DSN`（`src/cfg.rs:398`）指向**同一个库**；
> - `validate` 里：**非 query-only 且开启对账时**才要求非空，默认关闭（阶段 0 「关掉即回退」）；
> - 🔴 DSN 带凭据，**不能进 ConfigMap**（现在 `deploy/k8s/base/config/scheduler.json` 是明文 ConfigMap），
>   必须走 Secret + env，正好 `overrideWithEnv` 这条路径天然支持。

### B.8 阶段 0 要读的表（PG 侧现状）

`src/orchestrator/paused_registry/postgres.rs:38-58` 的 `SCHEMA_DDL`（**由 node 启动时自建，无 migration 工具**）：

```sql
paused_sandboxes(
  sandbox_id UUID PK, cluster_id UUID NOT NULL, state TEXT NOT NULL,
  generation BIGINT NOT NULL, origin_node_id TEXT NOT NULL,
  claimed_by_node_id TEXT, snapshot_id UUID, metadata JSONB NOT NULL,
  paused_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
  lease_expires_at TIMESTAMPTZ, sandbox_expires_at TIMESTAMPTZ)
CHECK (state IN ('publishing','paused','resuming','local_only','running'))
INDEX: (origin_node_id), (updated_at)
```

- 读路径列清单常量：`ENTRY_COLUMNS`（`postgres.rs:21-22`）。
- 租约过期判据：`LEASE_EXPIRED = "COALESCE(lease_expires_at, updated_at) < now()"`（`:88`），
  **用数据库时钟**，Go 侧对账必须照抄这个表达式，不能用 Go 的 `time.Now()`。
- `running` 行由 `origin_node_id` 指认持有者，`resuming` 行由 `claimed_by_node_id` 指认（`:94-95`）。
- 🔴 `mark_running` 的契约是 **"Never creates a row"**（`paused_registry/mod.rs:204`）——
  **从未 pause 过的沙箱在这张表里没有行**。阶段 0 的 `ghost` 口径必须建立在这个前提上，
  否则会把「一直在跑、从没暂停过」的正常沙箱全部误报成 orphan。
- 阶段 0 的 Go 侧连接必须是**只读**：库的 schema owner 是 Rust node（启动即 `CREATE TABLE IF NOT EXISTS` + `ALTER`），
  Go 侧再建一次 migration 会与之打架。建议直接用只读 role。

---

## C. 依赖与构建

### C.1 `services/go.mod`

- `module agentenv/services`，**`go 1.25.0`**（`go.mod:3`）。
- 直接依赖 9 个（`:5-15`）：`hashicorp/golang-lru/v2`、`prometheus/client_golang`、`redis/go-redis/v9`、
  `zap`、`grpc`、`protobuf`、`k8s.io/{api,apimachinery,client-go} v0.29.4`。
- **没有任何 PostgreSQL driver**，**没有任何 migration 库** —— `go.sum` 里 `pgx` / `lib/pq` / `sqlx` / `golang-migrate` / `goose` 零命中。
  阶段 0 是这个 module 的**第一次引入数据库依赖**（建议 `jackc/pgx/v5`，只读用 `pgxpool`；不引 ORM、不引 migration）。

### C.2 Makefile

- 顶层 `services/Makefile`：`proto`（`:8-12`，`protoc --go_out --go-grpc_out paths=source_relative`）、
  `tidy`、`build`/`test`/`fmt`/`fmt-check`/`vet`（`:17-35`，全部转发给两个子 Makefile）、
  `run-scheduler`/`run-gateway`（`:37-41`，用 `services/config/local.json`）。
- 子 Makefile 完全对称（`gateway/Makefile`、`scheduler/Makefile`）：
  `build` → `go build -o bin/{name} ./{name}/cmd`；`test` → `go test ./{name}/...`；
  `vet` → `go vet ./{name}/... ./shared/... ./api/...`。
  ⚠️ **`make test` 不覆盖 `./shared/...`** —— config 包的测试（`shared/config/config_test.go`，630 行）
  只有直接 `go test ./...` 才会跑。阶段 0 改 config 时注意本地要显式跑。
- CI：`.github/workflows/services-ci.yml`，触发路径 `services/**` + `deploy/**`。
  步骤：`make build` → **`apt-get install redis-server`**（`:32-35`，scheduler 测试要真 Redis）→ `make test` → `make fmt-check` → `make vet`。
  ⇒ 阶段 0 如果写需要真 PG 的测试，CI 里得照这个模式加 service container 或 `postgresql` 包。

### C.3 `deploy/k8s/` 现状

| 对象 | 文件 | 关键点 |
|---|---|---|
| scheduler Deployment | `base/scheduler-deployment.yaml` | **`replicas: 1`**（`:8`）；`serviceAccountName: agentenv-scheduler`；`args: -config /config/scheduler.json`；只暴露 `grpc:9090`（`:27-29`，**9101 metrics 未暴露**）；readiness/liveness 走 `/grpc_health_probe`（`:30-43`）；**零 env、零 Secret**（`:44-52` 只挂 ConfigMap） |
| gateway Deployment | `base/gateway-deployment.yaml` | `replicas: 1`（`:8`）；唯一 env 是 `GATEWAY_SANDBOX_PROXY_DOMAINS`（`:29-35`，`optional: true` 从 `sandbox-proxy-config` ConfigMap 取）；只暴露 `http:8080`（**9102 metrics 未暴露**） |
| 配置注入 | `base/kustomization.yaml:19-31` | `configMapGenerator` 三个：`agentenv-k8s-config`/`scheduler-k8s-config`/`gateway-k8s-config`，外加 literal `sandbox-proxy-config`；`disableNameSuffixHash: true`（`:45`）⇒ **改 ConfigMap 不会自动滚动 Pod**，得手动 `rollout restart` |
| scheduler 配置内容 | `base/config/scheduler.json` | 只有 `grpc_listen_addr`/`strategy`/`discovery{kubernetes}`；**没有 redis_addr、没有 binding_ttl、没有 node_resource_limit**（全吃默认值） |
| gateway 配置内容 | `base/config/gateway.json` | `scheduler_addr: agentenv-scheduler:9090`、`request_timeout: 90s`；**`query_only_scheduler_addr` 未配** ⇒ 当前部署下 gateway 的 `queryOnlyScheduler` 就是主 scheduler（`server.go:82-85` 的 fallback） |
| Secret | — | **`deploy/k8s/` 下没有任何 Secret 资源** —— 阶段 0 要新建 |
| PG / Redis | — | **base 里都没有** |

---

## D. 三个最容易踩的坑

### 坑 1：「查 roster 兜底」在今天的数据结构上不存在

方案 §4 阶段 1 写的「回落读必须同时查一次 `ListObservedNodes` 的 roster，roster 里有就按 roster 走」
—— `ObservedNode`（`proto:157-167`）**没有 sandbox 列表**。心跳里的 `sandbox_ids`（`proto:177`）
唯一去处是 `service.go:265` 的 `ReconcileNode`，写进 BindingStore 后即丢弃。
所以「roster」就是 BindingStore 本身，查它救不了 BindingStore 的 miss。
**不先补 proto 字段 / 不先在 registry 里留存 roster，阶段 1 的 404 前提是空的。**
（`node_registry.go:38-42` 的 `observedNodeRecord` 是留存 roster 最自然的位置。）

### 坑 2：`--query-only` 副本会把新逻辑整个绕过去

数据面的 `LookupNode` 走的是 `s.queryOnlyScheduler`（`server.go:219`），
而 `Schedule` 走 `s.scheduler`（`:257`、`:733`）—— 两个不同的 gRPC 连接（`gateway/cmd/main.go:53-63`）。
`QueryOnlyService`（`service.go:77-94`）**只有 `store` 一个字段**：没有 registry、没有 strategy、没有 resourceLimit、warmup 传 nil。
如果阶段 0/1 只把 PG 只读连接和登记表回落接到 `Service` 上，那么一旦有人配了 `gateway.query_only_scheduler_addr`，
所有 resume 的 lookup 都会打到那个副本，拿到裸 `NotFound`，新逻辑一次都不生效——
而**当前 k8s base 恰好没配这个地址**（`config/gateway.json`），本地测不出来，上了别的环境才炸。
⇒ 要么 `QueryOnlyService` 同样装 PG 只读读者，要么在 config 校验里显式禁止「开了登记表回落 + 配了 query-only 地址」的组合。

### 坑 3：删掉 reroute，但 Rust 侧照发 503

`x-agentenv-reroute: schedule` 的产生方在 `src/api/isolation.rs:60-68`，是本次**不动**的 Rust 代码，
只要节点处于 `scheduling_disabled()` 且沙箱曾登记过就会发。
gateway 一旦删掉 `allowReroute`（`server.go:445-451`）与 `rerouteToScheduledNode`，这个 503 就**直通客户端**。
`paused` 走软偏好那条是安全的（`FilterUnschedulable`，`filter.go:23-33`，会排掉 DRAINING）；
**`publishing` / `local_only` 硬钉 origin 那两条不安全** —— 硬钉到一台正在 draining 的 origin，
节点 gate 判定「可在别处重建」（`isolation.rs:96-116` 对非 `Never` 的登记一律返回 true）→ 发 503 →
gateway 没有重放能力 → 用户吃 503。
⇒ 阶段 1 方案必须补一条：硬钉目标为 DRAINING 时怎么办（等它？答 503 但语义明确？还是 Rust 侧对 `local_only` 放行）。

---

## 附：其它值得记一笔的观察

- **被拒绝的第一跳在 upstream 指标里不存在**：`proxyRequest` 在 `:521-523` 提前 return，跳过 `:524` 的
  `recordGatewayUpstreamProxy`。阶段 1 若想量化「删补丁前后」，只能靠 `route_source="schedule"` 这个间接口径。
- **recovery 分支不改 `route_source`**：`:211` 设的 `path` 一直保留到最后，所以 recovery 与普通 resume 在指标上无法区分。
  阶段 0 想拿 baseline，得先补一个 counter（或临时把 route_source 改掉）。
- **`scheduleRecoveryNode` 的错误处理是「报旧错」**：`:232-237` 在 Schedule 失败时上报的是原始 `LookupNode` 的
  `NotFound`（→ 404），而不是 Schedule 的 `Unavailable`（→ 503）。这正是方案 §4 阶段 1 想根治的「把『我不知道』洗成『不存在』」的一个现成实例。
- **`schedulerRPCLabel`（`scheduler/internal/metrics.go:93-116`）漏了 4 个 P2P RPC 与 `ListP2pPeers`**，
  返回 `""` 时拦截器直接跳过计时（`:51-53`），所以这些 RPC 今天完全无指标。
- **`ReportSandboxEvent` 是纯 no-op**（`service.go:278-285`，只打一条 Debug 日志）。
  阶段 0 想拿「事件流」口径的对账数据，这里是现成的接入点，且 node 侧已经在发。
- **`InMemoryArtifactStore.Lookup` 在 `RLock` 下调 `s.lru.Get`**（`store.go:229-247`）：
  `Get` 会把条目挪到 LRU 队首（由 lru 自己的锁保护，不触发 evict 回调），所以今天不是竞态；
  但它**不是只读操作** —— 若日后给 P2P index 加读路径（阶段 3 的 placement 要用），
  别照着这段假设「Lookup 可以只持读锁」。
