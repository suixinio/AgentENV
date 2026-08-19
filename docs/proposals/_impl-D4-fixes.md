# D4：集群验证缺陷修复（F1 / F2 / F3 / F4 / F5 / F7 + F6 注释）

分支 `central-control-plane-phase01`，基线 commit `7e6f790`。
任务书来源：`docs/proposals/_verify-T1-results.md` §6。
**未 commit、未 push、未连集群、未 `make k8s-apply`；`src/` 下 Rust 代码零改动。**

---

## 0. 一页速览

| # | 结论 | 改动面 | 对应测试 | 变异验证 |
|---|------|--------|----------|----------|
| F1 | `/registry/sandboxes` 需非空 `X-API-Key`，否则 401 | gateway | `TestRegistryListRequiresAnAPIKey` | ✅ FAIL |
| F2 | 未知 `state` ⇒ 400，消息列出五个合法值 | scheduler + registry 包 | `TestListRegistrySandboxesRejectsAnUnknownState` 等 4 个 | ✅ FAIL（3 发） |
| F3 | 未知查询参数 ⇒ 400，指名道姓 | gateway | `TestRegistryListRejectsUnknownQueryParameters` 等 3 个 | ✅ FAIL（3 发） |
| F4 | "没心跳" 与 "不接单" 在日志 / metric label / HTTP 文案三处分开 | scheduler | `TestLookupRefusesToPinToANodeThatWillNotServe` | ✅ FAIL（2 发） |
| F5 | 失败轮不再进直方图（取消的轮也不进） | scheduler | `TestRegistryReconcileDurationOnlyTimesSuccessfulRounds` + 1 | ✅ FAIL |
| F6 | **不修**，只在 `lookup.go` 注释里写明窗口存在与量级 | scheduler 注释 | 复用 F4 的两个用例 | n/a |
| F7 | gateway 补 `containerPort: 9102` + Service `metrics` 端口 | `deploy/k8s/base/` | `TestMetricsListenersAreDeclaredAndExposed` | ✅ FAIL（2 发） |

四条命令 + 集成测全绿，见 §3。

---

## 1. 逐条

### F1 —— `/registry/sandboxes` 无鉴权

**改了什么**

- `services/gateway/internal/registry_list.go`
  - 新增 `const headerRegistryAPIKey = "X-API-Key"`（头名与节点侧
    `src/api/impls/auth.rs` 一致；`http.Header.Get` 自带大小写规范化，
    `x-api-key` / `X-Api-Key` 都认）。
  - `handleRegistryList` 第一件事：`strings.TrimSpace(r.Header.Get(...)) == ""` ⇒
    `401` + body `X-API-Key is required`，**请求不出 gateway**（scheduler 不被调用）。
- `docs/proposals/_impl-plan-control-plane-phase01.md` §2.6：把"走现有 API key 中间件"
  那句改成事实描述 —— gateway 侧没有任何鉴权中间件，`/sandboxes` 的 401 是转发到的**节点**
  给的；本端点自带一条非空 key 检查；`/nodes` 刻意不动。同时把 F2/F3 的参数校验口径写进去。

**行为变化**

| 请求 | 之前 | 之后 |
|---|---|---|
| `GET /registry/sandboxes`（无头） | 200 + 全量 | **401** |
| `GET /registry/sandboxes` + `X-API-Key: <任意非空>` | 200 | 200（不变） |
| `GET /nodes`（无头） | 200 | **200（刻意不变）** |
| `GET /sandboxes`（无头） | 401（节点给的） | 401（不变，没碰） |

**范围**：只这一个端点。`/nodes` 与 `/nodes/{id}` 一律不动 —— Agent-Console / AENV-Panel
今天就是不带凭据读 `/nodes` 的，动它是另一个决策。这条由
`TestRegistryListRequiresAnAPIKey/does_not_spread_to_the_node_list` 反向钉住。

**对应测试**：`services/gateway/internal/registry_list_test.go`
`TestRegistryListRequiresAnAPIKey`（4 个子用例：无头 / 空值 / 纯空白 / 带 key 走通，
外加"不外溢到 /nodes"）。所有既有 registry 用例改走新 helper `registryListRequest()`，
凭据只写一处。

**这不是锁，是一道门**：它挡住"裸 curl 拿到全集群沙箱 ID + 归属节点 + 租约时间"，
不校验 key 的内容。gateway 真正的凭据体系是独立决策，本轮不做（裁决如此）。

---

### F2 —— `?state=bogus` 静默返回空列表

**改了什么**

- `services/scheduler/internal/registry/registry.go`：新增 `KnownStates()` 与
  `ParseState(raw) (State, bool)`。合法值集合定义在**声明这五个状态的那个包**里，
  校验方不再另抄一份列表。
- `services/scheduler/internal/service.go`：`listRegistrySandboxes` 新增
  `parseRegistryStateFilter`，未知值 ⇒ `codes.InvalidArgument`，消息形如
  `unknown state "bogus", must be one of publishing, paused, resuming, local_only, running`。
  过滤改用解析后的规范值做等值比较（原来是对原始输入 `EqualFold`）。
- gateway 侧不需要改：`writeSchedulerError` 已经把 `InvalidArgument` 映射成 400。

**校验放在 scheduler 而不是 gateway**，两个理由：gRPC 直连的调用方（T1 用过的直连探针、
将来的 Agent-Console）同样受保护；以及合法集合就在 scheduler 这一侧，放 gateway 得再抄一份。

**顺序**：`page_size` → `state` → 再看 reader 是否装配。所以"打错 state"在装配了 registry 的
集群和没装配的集群上是**同一个答案**（400），不会因为这台恰好没配 DSN 就变成 501。
由 `TestListRegistrySandboxesJudgesTheStateBeforeTheConfiguration` 钉住。

**行为变化**

| 请求 | 之前 | 之后 |
|---|---|---|
| `?state=bogus` | 200 `{"sandboxes":[]}` | **400** `unknown state "bogus", must be one of publishing, paused, resuming, local_only, running` |
| `?state=LOCAL_ONLY` / `?state=local_only` / `?state=" local_only "` | 200 过滤 | 200 过滤（大小写与前后空白照旧宽容） |
| `?nextToken=zzz` | 200 空列表 | 200 空列表（**按裁决不改**：游标排在所有 UUID 之后，空就是正确答案） |

**对应测试**
- `services/scheduler/internal/service_registry_test.go`：
  `TestListRegistrySandboxesRejectsAnUnknownState`（含"消息必须列出五个值"和
  "registry 一次都没被读"两条断言）、
  `TestListRegistrySandboxesJudgesTheStateBeforeTheConfiguration`、
  `TestListRegistrySandboxesAcceptsEveryKnownState`（五个状态 × 三种拼法，
  防"用拒绝一切来满足拒绝未知"）。
- `services/scheduler/internal/registry/registry_test.go`：
  `TestParseStateAcceptsOnlyTheFiveKnownStates`、`TestKnownStatesCoversEveryDeclaredState`
  （后者把 `KnownStates()` 和五个常量声明绑死，将来加第六个状态漏登记会红）。

---

### F3 —— `?nodeId=`（小写 d）被静默忽略

**改了什么**

`services/gateway/internal/registry_list.go`：
- 新增 `var registryListQueryParams`，封闭集合 = `state` / `nodeID` / `limit` / `nextToken`。
- 新增 `rejectUnknownRegistryListParams(r)`：任何不在集合里的参数 ⇒ 400，
  消息形如 `unknown query parameter(s) nodeId; supported: limit, nextToken, nodeID, state`。
  未知参数名与 supported 列表都排序后输出（map 遍历顺序不确定，消息必须可复现）。
- supported 列表**从同一张 map 派生**，不写第二遍字面量。
  （这条是变异验证逼出来的：第一版消息里的 supported 是硬编码字符串，
  把 `nodeID` 从 map 删掉之后，400 的消息仍然在推荐 `nodeID` —— 一个自相矛盾的 400。）

**行为变化**

| 请求 | 之前 | 之后 |
|---|---|---|
| `?nodeId=node-a` | 200 **全量**（过滤被吞） | **400** 指出 `nodeId` |
| `?State=paused` | 200 全量 | **400** 指出 `State` |
| `?zzz=1&aaa=2` | 200 全量 | **400** 指出 `aaa, zzz` |
| `?state=&nodeID=&limit=&nextToken=` | 200 | 200（不变） |

**对应测试**：`services/gateway/internal/registry_list_test.go`
- `TestRegistryListRejectsUnknownQueryParameters`（4 组，含多参数排序）
- `TestRegistryListAcceptsEveryDocumentedParameter`（防"拒绝一切"）
- `TestRegistryListRefusalNamesTheSetItEnforces`（把 400 消息里 advertise 的每个参数
  真的再打一遍，必须 200 —— 消息与实现不许分家）

---

### F4 —— `origin_unschedulable` 把两种原因说成同一句

**改了什么**

`services/scheduler/internal/lookup.go`：
- 新增 `nodeSchedulability` 三值枚举：`nodeSchedulable` / `nodeNotReporting` /
  `nodeNotAcceptingWork`。
- `nodePlacer.schedulableNode` 的返回从 `(Node, bool)` 改成 `(Node, nodeSchedulability)`；
  `(*Service).schedulableNode` 里 `liveNode` 失败 ⇒ `nodeNotReporting`，
  `FilterUnschedulable` 过滤掉 ⇒ `nodeNotAcceptingWork`。
- `publishing` / `local_only` 分支按原因分成两路。

**三处都分开了**

| | 没有新鲜 roster（节点没心跳） | DRAINING（节点说不接单） |
|---|---|---|
| **metric** `lookup_node_total{result=}` | `origin_not_reporting`（**新**） | `origin_unschedulable`（沿用） |
| **日志** | `scheduler cannot pin a sandbox to an origin node that is not reporting` | `... that is not accepting work` |
| **HTTP body** | `sandbox is local_only on node "x", which is not reporting` | `... which is not accepting work` |
| **gRPC code** | `FailedPrecondition`（**未改**） | `FailedPrecondition`（未改） |

HTTP body 也分开了：两句话本来就不一样长不一样意思，让 401/503 的排障者在
`curl` 那一层就能分辨，比只改日志便宜。`FailedPrecondition` 一个字节没动，
gateway 的 503 映射与前端契约不受影响。

"没心跳"这句与 `running`/`resuming` 分支既有的 `holder_unreachable`
（`... which is not reporting`）用词一致 —— 同一件事在两处必须是同一句话。

**T1 那 7 次 503 之后会怎样**：`result=origin_not_reporting`，日志说 "not reporting"，
排障从 admin API 的 DRAINING 转向节点心跳，这才是真现场。

**对应测试**：`services/scheduler/internal/lookup_test.go`
`TestLookupRefusesToPinToANodeThatWillNotServe` 重写为两组各自断言
「metric label 增加 1」+「另一个 label 纹丝不动」+「HTTP 消息子串」。
新增 helper `lookupResultCounts(t)` 通过私有 registry 读回
`agentenv_scheduler_lookup_node_total`。

---

### F5 —— 失败轮混进 `registry_reconcile_duration_seconds`

**改了什么**

- `services/scheduler/internal/reconcile.go`：删掉失败分支里的
  `recordRegistryReconcileDuration(start)`。成功轮末尾那一次保留不动。
- `services/scheduler/internal/metrics.go`：Help 改成
  "Duration of one **successful** ... round, read included. Failed rounds are not
  observed here — their duration is how long it took to give up ... see
  registry_read_failures_total."，并给 `recordRegistryReconcileDuration` 补了函数注释。

**行为变化**：3 轮读失败之后 `_count` 不再 19→22，仍是 19；
`registry_read_failures_total` 照常 +3。**顺带修好的一半**：原来的
`recordRegistryReconcileDuration(start)` 排在 `ctx.Err()` 检查之前，
所以**进程关闭时被取消的那一轮也在计时**，现在也不计了 —— 关机路径不会成为直方图里
最响的那个贡献者。

**对应测试**：`services/scheduler/internal/metrics_test.go`
- `TestRegistryReconcileDurationOnlyTimesSuccessfulRounds`
  （成功 +1 → 失败不动 + read_failures +1 → 恢复后又 +1，三段）
- `TestRegistryReconcileIgnoresACancelledRound`（取消轮既不计时也不算读失败）
- 新增 helper `histogramCount()`；`newRegistryGatherer` 补上直方图这个 collector。

---

### F6 —— 不修，补注释（按裁决）

`services/scheduler/internal/lookup.go` 的 `(*Service).schedulableNode` 注释里加了一段，
写明：

- 这个窗口存在：scheduler 每次重启，`publishing` / `local_only` 沙箱在
  origin 的第一条心跳落地之前一律被拒；
- 量级：**一个节点上报周期，默认 5s**（`AENV_OBSERVABILITY_REPORT_INTERVAL_SECS`，
  见 `src/cfg.rs:642`）；**scheduler 停机够久让节点上报退避涨满时最长 60s**
  （`MAX_REPORT_BACKOFF`，见 `src/observability/reporter.rs:20`）；
- 为什么值这个价：拒绝是 `FailedPrecondition` ⇒ gateway 503 ⇒ 可重试，代价是一次重试；
  fail-open 的代价是把沙箱钉到一台可能已经消失几小时的机器上，代价是这台沙箱；
- 为什么与 `paused` 不对称：`paused` 有已发布快照、任何节点都能重建，所以走 place 不走 pin。

没有为它单独写用例 —— F4 的 `origin is not reporting` 那一组走的就是这条 fail-closed 路径。

---

### F7 —— gateway `:9102` 够不到

**改了什么**

- `deploy/k8s/base/gateway-deployment.yaml`：container `ports` 加
  `- name: metrics / containerPort: 9102`。
- `deploy/k8s/base/gateway-service.yaml`：`ports` 加
  `- name: metrics / port: 9102 / targetPort: metrics`。

形状照抄本轮 scheduler 那两处（`scheduler-deployment.yaml` 的 `metrics: 9101` +
`scheduler-service.yaml` 的 `targetPort: metrics`），包括注释口径。

**对应测试**：`services/shared/config/manifest_test.go`（新文件）
`TestMetricsListenersAreDeclaredAndExposed`：
对 gateway 与 scheduler 各做一遍 ——
用 `config.Load()` 读**清单里真正挂进 Pod 的那份 JSON**（`deploy/k8s/base/config/*.json`）
解析出 metrics 监听端口，再解析同目录的 Deployment / Service YAML，
断言该端口被声明为 containerPort、且 Service 有端口指向它（按 `targetPort` 匹配，
所以改 Service 的对外端口号仍合法，指向空气则不合法）。
把"代码里的端口"和"清单里的端口"绑在一起，双向漂移都会红。scheduler 那半是回归护栏。

> 依赖说明：测试只用了 `services/go.mod` 里**已经是直接依赖**的 `k8s.io/api` 与
> `k8s.io/apimachinery`，`go.mod` / `go.sum` 零改动。

---

## 2. 变异验证表

变异全部打在 scratchpad 的副本
`/tmp/claude-1000/…/scratchpad/mut/{services,deploy}` 上，**工作区零改动**；
每发验完立即还原，最后整棵副本重跑 `go test ./...` 确认回到全绿。

| # | 变异（把修复退回去 / 削弱） | 跑的用例 | 结果 |
|---|---|---|---|
| F1 | 删掉 `handleRegistryList` 里那段 401 检查 | `TestRegistryListRequiresAnAPIKey` | **FAIL** ×3 子用例：`expected an unauthenticated request never to reach the scheduler` |
| F1b | 把同一段检查**加到** `handleNodeList`（模拟"外溢") | 同上 | **FAIL**：`expected the node list to stay open, got 401` |
| F2 | 去掉 `parseRegistryStateFilter`，恢复对原始输入 `EqualFold` 过滤 | `TestListRegistrySandboxes*` | **FAIL** ×2：`expected no response, got 0 rows` / `expected InvalidArgument, got FailedPrecondition` |
| F2b | 保留 400 但消息里不列五个值 | `…RejectsAnUnknownState` | **FAIL**：`expected the message to list "publishing", got "unknown state \"bogus\""` |
| F2c | `ParseState` 改成来者不拒 | `TestParseStateAcceptsOnlyTheFiveKnownStates` | **FAIL**：`expected "" to be rejected` |
| F2d | `KnownStates()` 少列一个 `running` | `TestKnownStatesCoversEveryDeclaredState` | **FAIL**：`KnownStates is missing "running"` |
| F3 | 删掉 `rejectUnknownRegistryListParams` 的调用 | `…RejectsUnknownQueryParameters` | **FAIL**：`expected an unknown query parameter never to reach the scheduler` |
| F3b | 从合法集合里删 `nodeID` | `…AcceptsEveryDocumentedParameter` + `…RendersRowsWithNullLeases` | **FAIL** ×2：`expected 200, got 400` |
| F3c | 400 消息里的 supported 改回硬编码字面量（含 `nodeId` 笔误） | `…RefusalNamesTheSetItEnforces` | **FAIL**：`the message advertises "nodeId" but the endpoint answers 400 for it` |
| F4 | 把两个 case 合成一个，只留 `origin_unschedulable` + "not accepting work" | `…RefusesToPinToANodeThatWillNotServe` | **FAIL**：`expected the message to say "which is not reporting", got "... which is not accepting work"` |
| F4b | 只把 metric label 合回去（两句文案保持不同） | 同上 | **FAIL**：`expected one origin_not_reporting, got 0` |
| F5 | 把 `recordRegistryReconcileDuration(start)` 加回失败分支 | `…DurationOnlyTimesSuccessfulRounds` + `…IgnoresACancelledRound` | **FAIL** ×2：`expected a failed round not to be timed, got 2 after 1` / `expected a cancelled round not to be timed, got 3 after 2` |
| F7 | 同时撤掉 gateway 的 containerPort 与 Service 端口 | `TestMetricsListenersAreDeclaredAndExposed` | **FAIL**：`gateway serves metrics on port 9102 but the Deployment declares [{http 0 8080 }]` |
| F7b | 只撤掉 Service 端口（containerPort 留着） | 同上 | **FAIL**：`gateway declares its metrics port as "metrics" but the Service exposes [{http <nil> 8080 {1 0 http} 0}]` |

14 发全部 FAIL，无一漏网。F1b / F3b / F3c 三发是**反向探针**：它们验的不是"修复在"，
而是"修复没有过头"（没外溢到 `/nodes`、没把合法参数一起拒了、消息没和实现分家）。

---

## 3. 门槛命令实际输出

工作目录 `apps/AgentENV/services`，`export GOWORK=off`：

```
### go build ./...
(no output — exit 0)

### go vet ./...
(no output — exit 0)

### gofmt -l .
(no output — exit 0)

### go test -count=1 ./...
?   	agentenv/services/api/proto	[no test files]
?   	agentenv/services/gateway/cmd	[no test files]
ok  	agentenv/services/gateway/internal	0.030s
?   	agentenv/services/scheduler/cmd	[no test files]
ok  	agentenv/services/scheduler/internal	0.079s
ok  	agentenv/services/scheduler/internal/registry	0.004s
ok  	agentenv/services/shared/config	0.010s
ok  	agentenv/services/shared/logging	0.003s
?   	agentenv/services/shared/observability	[no test files]
```

registry 包集成测（真库）：

```
$ SCHEDULER_REGISTRY_TEST_DSN='postgres://aenv:verify@127.0.0.1:15499/aenv?sslmode=disable' \
    go test -count=1 -v ./scheduler/internal/registry/
=== RUN   TestPostgresReaderListReadsEveryStateWithLeases
--- PASS: TestPostgresReaderListReadsEveryStateWithLeases (0.03s)
=== RUN   TestPostgresReaderListWithoutClusterFilterSeesEveryCluster
--- PASS: TestPostgresReaderListWithoutClusterFilterSeesEveryCluster (0.03s)
=== RUN   TestPostgresReaderGet
--- PASS: TestPostgresReaderGet (0.03s)
=== RUN   TestPostgresReaderPoolRefusesWrites
    --- PASS: TestPostgresReaderPoolRefusesWrites/update (0.01s)
    --- PASS: TestPostgresReaderPoolRefusesWrites/delete (0.00s)
    --- PASS: TestPostgresReaderPoolRefusesWrites/insert (0.00s)
    --- PASS: TestPostgresReaderPoolRefusesWrites/ddl (0.00s)
=== RUN   TestPostgresReaderMalformedIDDoesNotMarkTheReaderReady
--- PASS: TestPostgresReaderMalformedIDDoesNotMarkTheReaderReady (0.03s)
=== RUN   TestPostgresReaderReportsItsClusterScope
--- PASS: TestPostgresReaderReportsItsClusterScope (0.02s)
...
=== RUN   TestParseStateAcceptsOnlyTheFiveKnownStates
--- PASS: TestParseStateAcceptsOnlyTheFiveKnownStates (0.00s)
=== RUN   TestKnownStatesCoversEveryDeclaredState
--- PASS: TestKnownStatesCoversEveryDeclaredState (0.00s)
PASS
ok  	agentenv/services/scheduler/internal/registry	0.161s
```

**skip 数 = 0**（`--- SKIP` 计数为 0，即真的连上了库，不是假绿）。

改动清单（`git diff --stat`，全部未 commit）：

```
 deploy/k8s/base/gateway-deployment.yaml            |   5 +
 deploy/k8s/base/gateway-service.yaml               |   5 +
 services/gateway/internal/registry_list.go         |  68 ++++++++
 services/gateway/internal/registry_list_test.go    | 181 ++++++++++++++++++++-
 services/scheduler/internal/lookup.go              |  77 +++++++--
 services/scheduler/internal/lookup_test.go         |  51 +++++-
 services/scheduler/internal/metrics.go             |   5 +-
 services/scheduler/internal/metrics_test.go        | 110 +++++++++++++
 services/scheduler/internal/reconcile.go           |   7 +-
 services/scheduler/internal/registry/registry.go   |  26 +++
 .../scheduler/internal/registry/registry_test.go   |  52 ++++++
 services/scheduler/internal/service.go             |  34 +++-
 .../scheduler/internal/service_registry_test.go    |  74 +++++++++
 13 files changed, 675 insertions(+), 20 deletions(-)
```

外加两个未跟踪的新文件：`services/shared/config/manifest_test.go`、本文件。
`docs/proposals/_impl-plan-control-plane-phase01.md` §2.6 已按 F1 要求改成事实描述。
`go.mod` / `go.sum` 未动；`src/` 下 Rust 零改动。

---

## 4. 与任务书不一致的地方

1. **F4 的 HTTP body 也分开了**（任务书说"分不分随你判断"）。
   理由：两条消息本来就是两句不同的话，把区分留在 body 里，
   `curl` 那一层就能分辨，不必等拿到 pod 日志。gRPC code 仍是 `FailedPrecondition`，
   一个字节没改。

2. **F2 的校验放在 scheduler，不在 gateway**（任务书没指定位置，只说"⇒ 400"）。
   理由：合法集合定义在 scheduler 侧的 registry 包里；放 gateway 得抄第二份，
   且直连 gRPC 的调用方（T1 用过的探针）就保护不到。gateway 已有的
   `InvalidArgument → 400` 映射让最终 HTTP 行为与要求一致。
   代价：一次 gRPC 往返 —— 这是运维端点，不是数据面。

3. **F3 的校验放在 gateway**（与 F2 分处两侧）。
   理由：查询参数名是 HTTP 层的概念，gRPC 请求里根本没有"未知参数"这回事。

4. **F1 只认 `X-API-Key` 一个头**。节点侧 `auth.rs` 实际上是
   `X-API-Key` **或** `X-Team-ID` **或** `X-Admin-Token` 三选一；按裁决"头名与节点侧一致
   （`X-API-Key`）"，这里只实现了这一个。若之后发现有运维工具只带 `X-Admin-Token`，
   放宽是一行的事，先按裁决执行。

5. **F7 的测试落在 `services/shared/config/`**，不在 gateway 包里，并且**同时覆盖
   scheduler**。理由：断言的是"代码里的监听端口 ↔ 清单里的端口"这条关系，
   两侧都读得到的地方只有 config 包；顺带把本轮已经做对的 scheduler 那半锁住防回归。

6. **F5 顺带修了取消轮**。原代码把计时排在 `ctx.Err()` 检查之前，
   所以关机时被取消的那一轮也在计时。任务书只点了"失败轮"，但两者同一行代码，
   分开修反而奇怪。

7. **F3 的 400 消息实现改过一版**：第一版把 supported 列表写成硬编码字面量，
   变异验证时暴露出"400 消息推荐一个自己会拒绝的参数"，改成从同一张 map 派生，
   并补了 `TestRegistryListRefusalNamesTheSetItEnforces` 钉住。

---

## 5. 发现但没做的问题

1. **🔴 `X-API-Key` 只查在不查值，本轮之后仍然如此。**
   任何非空字符串都能读到全集群沙箱 ID + 归属节点 + 租约时间。
   按裁决"不新造 gateway 鉴权体系"，这条留着。上生产前仍需要 T1 §6 F1 的那个结论：
   要么 gateway 长出真凭据校验，要么确认 gateway 只在内网可达。
   **注意本轮把风险口径改小了但没归零** —— 别因为现在返回 401 就以为这条已经关掉。

2. **`/nodes` 与 `/nodes/{id}` 仍然完全不鉴权**（刻意保留，见 F1）。
   `/nodes` 泄露的是节点清单 + 资源水位 + 沙箱计数；`POST /nodes/{id}` 更要命 ——
   那是**把节点设成 DRAINING 的写操作**，今天任何人都能打。
   这条比 F1 原本那条严重，不在本轮范围内，建议单独立项。

3. **`nextToken` 是明文 sandbox_id，且不校验格式。**
   `?nextToken=zzz` 返回空列表是对的（裁决已确认），但游标同时也是"上一页最后一行的
   sandbox_id"，翻页语义泄露给了调用方。不是缺陷，是口径 —— 将来要换游标格式会破坏兼容。

4. **`page_size` 没有上限。**
   `?limit=0` / 不传 = 全量。今天登记表只有个位数行，无所谓；行数上千之后
   一次 List 会把整表读进 gateway 的内存再序列化。
   注意 `reader.List(ctx)` 本身就是全表读，分页是**读完之后在内存里切的** ——
   加 `limit` 上限只能省网络，省不了库和内存。真要治得把过滤下推到 SQL。

5. **`registry_reconcile_duration_seconds` 仍然只量"读 + 派生"这一整段。**
   D3 §5.4 自己记过：读和派生分不开，所以看到 p99 涨了不知道是库慢了还是集群大了。
   本轮只把失败轮摘出去，没拆这两段。

6. **T1 §5.1 那条根因没动，也不该在本轮动**：paused 沙箱永远留在 heartbeat roster 里，
   所以日常 pause/resume 根本走不到登记表分支。本轮六条修的都是登记表分支之外的东西
   （鉴权、参数校验、文案、指标口径、清单），**不改变"稳态流量零影响"这个结论**。

7. **T1 §5.2 的 DRAINING 对照组仍然取不到**（需要往 `paused_sandboxes` 插合成行，
   属于集群操作，本轮纪律禁止）。不过 F4 的两个方向现在都有单测覆盖，
   且两个变异都被挡住 —— 真集群复验时只要看 `origin_not_reporting` 这个新 label
   有没有出现，就能一眼分辨那 7 次 503 到底是哪种。
