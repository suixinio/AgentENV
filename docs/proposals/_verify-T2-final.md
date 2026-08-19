# T2 最终轮集群复验：6 条修复 + T1 两条 BLOCKED（pve-sg dev，203/204）

> 2026-08-19 · 测试 agent 交付。被测代码 `central-control-plane-phase01` @ `c35f5ec`
> （= `7e6f790` 阶段 0+1 主体 + `c35f5ec` 缺陷修复）。
> 上位：[`_verify-T1-results.md`](_verify-T1-results.md)、[`_impl-D4-fixes.md`](_impl-D4-fixes.md)、
> [`_verify-plan-phase01.md`](_verify-plan-phase01.md)。
> 全程未跑 `make k8s-apply`，未动 node DaemonSet，未改任何 ConfigMap，未用过 `:latest`。

---

## 0. 一页速览

| 条目 | 结论 |
|---|---|
| **任务 A** | |
| F1 `/registry/sandboxes` 需 API key | ✅ **PASS**（含反向对照：`/nodes` 仍 200，修复没外溢）|
| F2 `?state=bogus` ⇒ 400 且列出五个合法值 | ✅ **PASS**（五个合法值逐个复验，防"拒绝一切"）|
| F3 `?nodeId=` ⇒ 400 指名参数 | ✅ **PASS**（并拿到 T1 缺的那条：正确拼法真的在过滤）|
| F4 "没心跳" vs "不接单" 三处分开 | ✅ **PASS —— 本轮最硬的一条**，同一行沙箱在**同一个进程**里跑出三种结局，见 §3.4 |
| F5 读失败轮不进直方图 | ✅ **PASS**（整个指标族**只有** `read_failures_total` 一格在动）|
| F7 gateway `:9102` 可从 Service 抓 | ✅ **PASS**（改前 Service 上超时、改后 200，探针自证）|
| **任务 B（T1 BLOCKED 的两条）** | |
| V1-3 第 4 步：origin DRAINING ⇒ 改选另一台 | ✅ **PASS**，`origin_preferred` 从 `true` 翻成 `false`，落点从 worker-01 换成 master-01 |
| V1-4：`local_only` + origin DRAINING ⇒ 503 且零转发 | ✅ **PASS**，503 文案对得上，转发计数**逐字不变** |
| 附带补上：P1-4 两分支（`stranded` vs `parked`）真的分得开 | ✅ **PASS** —— T1 §2 说这半边"没有分辨力"，本轮拿到了 |

**合并判断：可以合并。** 判据见 §6。

**T1 的两条 BLOCKED 全部关闭，无一条剩下。** 关键方法上的发现：
**这两条根本不需要冷 scheduler 窗口** —— 合成行的 sandbox_id 不在任何节点的 roster 里、
也没有 binding，所以 `lookupNode` 第 1/2 步必然落空、**每次都直落登记表分支**。
T1 §5.1 那个死锁是"真沙箱永远留在 roster 里"造成的，合成行天然绕开它。
冷窗口本轮只用了一次，且只为 F4 的"没心跳"那一支（§3.4）。

---

## 1. 发布记录

### 1.1 构建

```
构建机 10.10.10.204:/opt/AgentENV
  sudo git -c safe.directory=/opt/AgentENV fetch fork central-control-plane-phase01
    7e6f790..c35f5ec  central-control-plane-phase01 -> fork/central-control-plane-phase01
  sudo git -c safe.directory=/opt/AgentENV checkout --detach FETCH_HEAD
  HEAD = c35f5ecf632882537d055836e0328362b7250422    （tracked 文件零 dirty）

TAG = cp1-c35f5ec     （不可变 tag，全程未用 :latest）
  docker build -f deploy/docker/Dockerfile.scheduler -t 10.10.10.204:5000/agentenv-scheduler:$TAG .   rc=0
  docker build -f deploy/docker/Dockerfile.gateway   -t 10.10.10.204:5000/agentenv-gateway:$TAG   .   rc=0
  docker push …/agentenv-scheduler:$TAG   digest sha256:3b72466acff6eb2a59fe46fd107d1905b821119000eb4f1e31f2e7f57ac468d2
  docker push …/agentenv-gateway:$TAG     digest sha256:8856011d5eb75f50d729f0a0d28791d3f1627b8cf0d5d174db4341cef0d19326
```

**Rust 侧未重建**（`c35f5ec` 的 diff 里 `src/` 零改动，见下），两个 `agentenv-node` Pod 全程未动。

```
$ git diff --stat 7e6f790..c35f5ec
 deploy/k8s/base/gateway-deployment.yaml            |   5 +
 deploy/k8s/base/gateway-service.yaml               |   5 +
 services/gateway/internal/registry_list.go         |  68 ++++
 services/gateway/internal/registry_list_test.go    | 181 ++++++++-
 services/scheduler/internal/lookup.go              |  77 +++--
 services/scheduler/internal/lookup_test.go         |  51 +++-
 services/scheduler/internal/metrics.go             |   5 +-
 services/scheduler/internal/metrics_test.go        | 110 +++++++
 services/scheduler/internal/reconcile.go           |   7 +-
 services/scheduler/internal/registry/registry.go   |  26 +++
 services/scheduler/internal/registry/registry_test.go |  52 +++
 services/scheduler/internal/service.go             |  34 ++-
 services/scheduler/internal/service_registry_test.go |  74 +++++
 services/shared/config/manifest_test.go            | 128 +++++++++
 14 files changed, 803 insertions(+), 20 deletions(-)
```

### 1.2 发布

只用 `kubectl patch` + `kubectl set image`，**没有 apply 任何清单**。
scheduler 侧 T1 已经注入过 env、补过 metrics 端口，本轮只换镜像；
gateway 侧按 `c35f5ec` 的 base 清单补 F7 那两处。

```bash
# 备份（/tmp/claude-1000/aenv-verify-t2/ 下，4 个对象 + 全 namespace ConfigMap）
bak-agentenv-scheduler-deploy.yaml / bak-agentenv-gateway-deploy.yaml
bak-agentenv-scheduler-svc.yaml    / bak-agentenv-gateway-svc.yaml / bak-configmaps.yaml

# F7：gateway Deployment 补 containerPort
kubectl -n agentenv-system patch deploy agentenv-gateway --type=json -p '[
  {"op":"add","path":"/spec/template/spec/containers/0/ports/-",
   "value":{"name":"metrics","containerPort":9102,"protocol":"TCP"}}]'
# F7：gateway Service 补 metrics 端口（targetPort 用名字，照 scheduler 那半的形状）
kubectl -n agentenv-system patch svc agentenv-gateway --type=json -p '[
  {"op":"add","path":"/spec/ports/-",
   "value":{"name":"metrics","port":9102,"targetPort":"metrics","protocol":"TCP"}}]'
# 换镜像
kubectl -n agentenv-system set image deploy/agentenv-scheduler scheduler=10.10.10.204:5000/agentenv-scheduler:cp1-c35f5ec
kubectl -n agentenv-system set image deploy/agentenv-gateway   gateway=10.10.10.204:5000/agentenv-gateway:cp1-c35f5ec
```

两个都 `successfully rolled out`。

**imageID 对账（不是只看 image 名）：**

```
agentenv-scheduler-59b5c9c4b8-25fr8  …/agentenv-scheduler@sha256:3b72466a…  == push digest ✅
agentenv-gateway-67fffbf86-772n8     …/agentenv-gateway@sha256:8856011d…    == push digest ✅
```

装配自证（新进程）：

```
{"level":"warn","caller":"cmd/main.go:216","msg":"scheduler paused registry has no cluster id; …"}
{"level":"info","caller":"cmd/main.go:231","msg":"scheduler paused registry enabled","cluster_id":"","max_connections":4,"reconcile_interval":30}
{"level":"info","caller":"cmd/main.go:108","msg":"scheduler gRPC server listening","addr":":9090","strategy":"round_robin","binding_store":"memory","query_only":false,"paused_registry":true}
{"level":"info","caller":"cmd/main.go:121","msg":"scheduler metrics server listening","addr":":9101"}
gateway /health ⇒ 204
```

---

## 2. 任务 A：六条修复逐条

> 所有 "改前" 读数都是本轮**在换镜像之前**、在同一套集群上实测的（不是抄 T1），
> 所以每条都自带反向对照。

### F1 —— `/registry/sandboxes` 需要 `X-API-Key` ✅ PASS

**改前（cp0-7e6f790，本轮实测）：**

```
GET /registry/sandboxes            无 key   ⇒ 200（返回全量）
GET /registry/sandboxes            带 key   ⇒ 200
GET /nodes                         无 key   ⇒ 200
```

**改后（cp1-c35f5ec）：**

```
$ curl -s -w ' HTTP:%{http_code}\n' http://10.10.10.203:30800/registry/sandboxes
X-API-Key is required     HTTP:401                       ← 无 key
$ curl … -H 'X-API-Key: dummy'      …/registry/sandboxes  ⇒ HTTP:200
$ curl … -H 'X-API-Key: '           …/registry/sandboxes  ⇒ X-API-Key is required  HTTP:401   ← 空值
$ curl … -H 'x-api-key: dummy'      …/registry/sandboxes  ⇒ HTTP:200                          ← 小写头也认
```

**🔴 反向对照（修复不许外溢）：**

| 请求 | 期望 | 实测 |
|---|---|---|
| `GET /nodes` 无 key | 仍 200 | **200** ✅ |
| `GET /sandboxes` 无 key | 仍 401（既有） | 401 ✅ |
| `GET /nodes/aenv-worker-01` 无 key | —— | **401** |

最后一行我停下来查了源码，确认**不是本轮引入的**：
`git diff 7e6f790..c35f5ec -- services/gateway/internal/` **只动了 `registry_list.go`**，
`node_list.go` 一个字节没改；而且 gateway 里**根本没有** `/nodes/{id}` 的本地 handler ——
它是被代理到节点的（gateway 指标里这条的 `route_source="path"`，与 `/nodes` 的
`route_source="gateway"` 分得开），401 是**那台节点自己**给的。属于既有行为。

### F2 —— `?state=` 未知值 ⇒ 400 ✅ PASS

**改前：** `?state=bogus` ⇒ `{"sandboxes":[],…}` HTTP **200**（静默空列表）

**改后：**

```
$ curl … '…/registry/sandboxes?state=bogus'
unknown state "bogus", must be one of publishing, paused, resuming, local_only, running
HTTP:400
```
消息里**五个合法值一个不缺** ✅

**防"用拒绝一切来满足拒绝未知"** —— 五个合法值逐个打：

| 查询 | HTTP |
|---|---|
| `?state=publishing` / `paused` / `resuming` / `local_only` / `running` | 全部 **200** ✅ |
| `?state=LOCAL_ONLY`（大小写宽容） | 200，且**返回 1 行**（与 `local_only` 一致）✅ |
| `?state=`（空值） | 200 全量 ✅ |

### F3 —— 未知查询参数 ⇒ 400 指名道姓 ✅ PASS

**改前：** `?nodeId=aenv-master-01`（小写 d）⇒ HTTP **200**，且**返回了 origin 是
`aenv-worker-01` 的那一行** —— 过滤被整个吞掉，答案看起来却像"master-01 名下有这行"。
这正是 T1 差点误判成 PASS 的陷阱。

**改后：**

```
$ curl … '…/registry/sandboxes?nodeId=aenv-master-01'
unknown query parameter(s) nodeId; supported: limit, nextToken, nodeID, state
HTTP:400                                                          ← 指名 nodeId ✅
$ curl … '…/registry/sandboxes?zzz=1&aaa=2'
unknown query parameter(s) aaa, zzz; supported: limit, nextToken, nodeID, state
HTTP:400                                                          ← 多参数已排序 ✅
$ curl … '…/registry/sandboxes?state=paused&nodeID=n&limit=1&nextToken=s0'
HTTP:200                                                          ← 四个合法参数同发 ✅
```

**🔴 T1 缺的那半：正确拼法真的在过滤**（否则"400 了"也可能只是把过滤整个关了）：

| 查询 | 返回行数 |
|---|---|
| `?nodeID=aenv-worker-01` | **1** ✅ |
| `?nodeID=aenv-master-01` | **0** ✅ |

改前 `nodeId=aenv-master-01` 返回 1 行、改后 `nodeID=aenv-master-01` 返回 0 行 ——
同一个语义意图，改前给的是错答案，改后给的是对答案。

### F4 —— "没心跳" 与 "不接单" 三处分开 ✅ **PASS（本轮最硬）**

完整取证在 §3.4（它同时是任务 B 的一部分）。结论先放这里 ——
**同一行沙箱（合成行 Y，`local_only`，origin `aenv-worker-01`），只改 worker-01 的处境，
在同一个 scheduler 进程里跑出三种结局：**

| worker-01 的处境 | HTTP body | metric `lookup_node_total{result=}` | 日志 |
|---|---|---|---|
| ready 且在心跳 | （转发过去了，节点答 404） | `pinned` = 1 | `scheduler pinned a sandbox to its origin node` |
| ready 但**没心跳**（冷窗口） | `sandbox is local_only on node "aenv-worker-01", which is **not reporting**` | `origin_not_reporting` = 12 | `… origin node that is **not reporting**` |
| 在心跳但 **DRAINING** | `… which is **not accepting work**` | `origin_unschedulable` = 1 | `… origin node that is **not accepting work**` |

三个 label 在**同一个进程的同一张指标表里并存**（见 §3.4 末尾那次实测），
排除了"代码里只有一支可达、另一支是死分支"这个替代解释。

T1 那 7 次 503 的真实原因是"节点没心跳"却被记成 `origin_unschedulable` + "不接单"；
本轮同样的处境记的是 `origin_not_reporting` + "not reporting"。**F4 要修的就是这个，修对了。**

### F5 —— 读失败轮不进直方图 ✅ PASS

**探针先自证**（否则"count 不涨"可能只是这个计数器根本不动）：

```
12:45:28  reconcile_duration_seconds_count = 4   last_success = 1.787143507e+09
（等一个 30s 对账周期，PG 正常）
12:46:0x  reconcile_duration_seconds_count = 5   last_success = 1.787143537e+09   ← 健康轮确实会 +1
```

**正式实验**（`kubectl scale sts/agentenv-postgres --replicas=0`，同一个 scheduler 进程）：

```
12:46:15  scale 之前最后一读：count = 6   read_failures_total = 0   last_success = 1.787143567e+09
--- scale sts/agentenv-postgres --replicas=0，等 110s（≥3 个对账周期）---
12:48:27  重新抓全量 agentenv_scheduler_registry_* 与 before 逐行 diff：

  9c9
  < agentenv_scheduler_registry_read_failures_total 0
  ---
  > agentenv_scheduler_registry_read_failures_total 4
```

**整个指标族（43 个序列）只有 `read_failures_total` 这一格变了。** 具体地：

- `reconcile_duration_seconds_count` **停在 6**（T1 在旧代码上量到的是 19 → 22）✅ **F5 修好了**
- `reconcile_duration_seconds_sum` 与**每一个 bucket** 也全部不动 ✅
- `last_success_timestamp_seconds` 停在 `1.787143567e+09` 不前进 ✅（V0-5 那条不变式仍成立）
- `rows{…}` / `stranded_rows` / `invalid_rows` / `untracked` / `ghost` / `stale_copy` /
  `roster_stale` / `rows_without_roster` 全部保留上一轮真实值、没被清零 ✅

scheduler 侧坐实走的是失败分支：

```
{"level":"warn","caller":"internal/reconcile.go:377","msg":"scheduler registry read failed",
 "error":"begin registry read: failed to connect to `user=aenv database=aenv`: …
  dial tcp 10.43.224.84:5432: connect: connection refused"}
（每 30s 一条）
```

验完 `kubectl scale sts/agentenv-postgres --replicas=1`，读立刻恢复。

### F7 —— gateway `:9102` 可从 Service 抓到 ✅ PASS

**探针自证（先证明这条探针分得开）**，在 patch 之前，从集群内一个普通 Pod 打：

```
$ kubectl exec agentenv-postgres-0 -- wget -q -T 5 -O - \
    http://agentenv-gateway.agentenv-system.svc.cluster.local:9102/metrics
wget: download timed out                              ← 改前：Service 上抓不到

$ kubectl exec agentenv-postgres-0 -- wget -q -T 5 -O - \
    http://agentenv-scheduler.agentenv-system.svc.cluster.local:9101/metrics | grep -c '^agentenv_scheduler_registry_'
48                                                    ← 同一条探针对 scheduler（T1 已补）是通的
```
⇒ 探针有分辨力：不是"wget 用不了"，是 gateway 那个 Service 端口不存在。

**改后**（`kubectl get` 直接读清单 + 集群内实抓）：

```
svc/agentenv-gateway    ports = [{http 8080 → http}, {metrics 9102 → targetPort "metrics"}]
deploy/agentenv-gateway containerPorts = [{http 8080}, {metrics 9102}]

$ kubectl exec agentenv-postgres-0 -- wget -q -T 8 -O - \
    http://agentenv-gateway.agentenv-system.svc.cluster.local:9102/metrics | grep '^agentenv_gateway_'
agentenv_gateway_http_request_duration_seconds_count{method="GET",route="/nodes",route_source="gateway",status="2xx"} 32
agentenv_gateway_http_request_duration_seconds_count{method="GET",route="/nodes/{node_id}",route_source="path",status="2xx"} 14
agentenv_gateway_http_request_duration_seconds_count{method="GET",route="/sandboxes/{sandbox_id}",route_source="path",status="2xx"} 2
…
```

走的是 **Service DNS + Service 端口**，正是 Prometheus 抓取会走的那条路，
不是 `kubectl port-forward`。✅
（`targetPort` 用的是名字 `metrics`，与 `c35f5ec` 的 base 清单一致，
所以将来改对外端口号仍合法、指向空气则不合法 —— 这正是 `manifest_test.go` 钉住的那条关系。）

---

## 3. 任务 B：三行合成登记表行

### 3.0 方法与安全性（我自己复核过一遍，不是照抄授权理由）

插入前独立读了 Rust 侧 `src/orchestrator/paused_registry/postgres.rs`：

```rust
const LIVE_HOLDINGS_OF_NODE: &str = "((state = 'running'  AND origin_node_id     = $2)
              OR (state = 'resuming' AND claimed_by_node_id = $2))";
```

- `reclaim_expired_holdings`：`UPDATE … WHERE state IN ('running','resuming') …` +
  `DELETE … WHERE snapshot_id IS NULL AND state IN ('running','resuming') …`
- `release_node_holdings`：两条语句都带 `{LIVE_HOLDINGS_OF_NODE}` ⇒ 同样只碰 running/resuming
- `renew_lease`：`WHERE p.sandbox_id = v.sandbox_id`，`v` 来自**本机持有的沙箱** VALUES 列表
- `get_many`：入参是**本地 sandbox id 集合**

⇒ 三行都是 `paused` / `local_only`，且在**任何**节点上都没有本地副本，
**对节点侧完全惰性**。唯一能改动它们的是 `claim_for_resume`，只在有人 resume 该 id 时触发。

**因此本轮探测一律用 `GET /sandboxes/{id}` 而不是 `resume`** ——
它走的是**同一条 `lookupNode` 判定路径**（gateway 对任何带 sandbox id 的路径都先打
`LookupNode`，`server.go:226`），但不会触发 `claim_for_resume`。
代价是零：要验的判定发生在 lookup，转发之后的事本来就被假快照污染了。
**副作用：三行全程 `generation` 保持 1、`claimed_by_node_id` 保持 NULL，
X 行没有像预期那样自己消失**（因为我没让它走 resume）。

### 3.1 基线（插入之前）

```
$ SELECT count(*) FROM paused_sandboxes;
1
$ SELECT state, count(*), count(snapshot_id) FROM paused_sandboxes GROUP BY state;
   state    | count | with_snap
------------+-------+-----------
 local_only |     1 |         0          ← 接手前就有的 01a01853

节点：aenv-worker-01 ready   aenv-master-01 ready

指标基线：
  registry_rows{local_only}=1  {paused|publishing|resuming|running}=0
  stranded_rows=1   parked_lease_expiring=0   invalid_rows=0
  live_lease_lapsed=0  holder_conflict=0  reclaimable_now=0
  rows_without_roster{aenv-master-01}=0  {aenv-worker-01}=0
  lookup_node_total{bound_binding}=3      （其余 label 尚未出现）
  gateway sandbox_location_total{bound}=3
  gateway upstream_proxy_duration_seconds_count{/sandboxes/{sandbox_id},2xx}=2
```

### 3.2 插入

```sql
INSERT INTO paused_sandboxes (sandbox_id, cluster_id, state, generation, origin_node_id,
                              snapshot_id, metadata, paused_at, updated_at,
                              claimed_by_node_id, lease_expires_at, sandbox_expires_at)
VALUES
 ('deadbeef-0000-0000-0000-000000000001','0000…0000','paused',    1,'aenv-worker-01',
  'deadbeef-0000-0000-0000-0000000000f1','{"synthetic":"T2-verify-row-X"}', now(), now(), NULL, NULL, NULL),
 ('deadbeef-0000-0000-0000-000000000002','0000…0000','local_only',1,'aenv-worker-01',
  NULL,                                  '{"synthetic":"T2-verify-row-Y"}', now(), now(), NULL, NULL, NULL),
 ('deadbeef-0000-0000-0000-000000000003','0000…0000','local_only',1,'aenv-worker-01',
  'deadbeef-0000-0000-0000-0000000000f3','{"synthetic":"T2-verify-row-Z"}', now(), now(), NULL,
  now() - interval '1 hour', NULL);
INSERT 0 3
```

UUID 一眼可辨（`deadbeef-…0001/2/3`），`origin_node_id` 用真节点名，
`metadata` 打了 `"synthetic":"T2-verify-row-*"` 标记（该列 NOT NULL）。
`count(*)` 1 → **4**。

### 3.3 🔴 顺带补上 T1 §2 说"没有分辨力"的那半：P1-4 两分支真的分得开

等一个对账周期后与基线 diff，**四处变化，与预测逐格一致**：

```
  parked_lease_expiring       0 → 1
  rows{state="local_only"}    1 → 3
  rows{state="paused"}        0 → 1
  stranded_rows               1 → 2
（其余 9 个序列逐字不变，含 invalid_rows=0、rows_without_roster 两条=0）
```

**为什么这就是分辨力**：Y 行与 Z 行**只差一个 `snapshot_id`** ——

| 行 | state | snapshot_id | 租约 | 落进哪个桶 |
|---|---|---|---|---|
| Y | local_only | **NULL** | 无（`COALESCE` 退化到 `updated_at`）| `stranded_rows` |
| Z | local_only | **非空** | **1 小时前就过期** | `parked_lease_expiring` |

两行的租约口径都是"已过期"。若 P1-4 没修（即不先看快照就进租约分支），
**Y 也会落进 `parked_lease_expiring`，读数会是 2 而不是 1**。
实测是 1 ⇒ 两个分支真的按 `snapshot_id` 分开了。
这正是 T1 §2 里"`parked_lease_expiring=0` 证明不了什么"那半边的补齐。

对照代码 `reconcile.go:226-238`（`SnapshotID == ""` ⇒ `strandedRows++` 后 `break`，
不进租约检查）—— 行为与代码一致。

### 3.4 三行 × 两种节点状态

判定证据以 **scheduler 的判定日志 + `lookup_node_total`** 为准（不看客户端最终状态码）；
"零转发"另用 gateway 自己的 `upstream_proxy_duration_seconds_count` 取证。

#### A. worker-01 **READY**（两节点都 ready）

四条请求（含一条必然失败的对照），逐条结果：

```
X    deadbeef-0000  -> {"code":404,"message":"sandbox deadbeef-…0001 not found"}   HTTP:404
Y    deadbeef-0000  -> {"code":404,"message":"sandbox deadbeef-…0002 not found"}   HTTP:404
Z    deadbeef-0000  -> {"code":404,"message":"sandbox deadbeef-…0003 not found"}   HTTP:404
CTRL 00000000-0000  -> sandbox assignment not found                                HTTP:404
```

> ⚠️ 四个都是 404，但**body 形状把它们分开了**：X/Y/Z 拿到的是**节点的 JSON 404**
> （说明请求真的转发到了某台机器），CTRL 拿到的是 **gateway/scheduler 自己的纯文本 404**
> （说明根本没转发）。这也是 T1 用过的那条判据。

scheduler 判定日志：

```
scheduler placed a paused sandbox              sandbox=deadbeef-…0001  node=aenv-worker-01  origin=aenv-worker-01  origin_preferred=true
scheduler pinned a sandbox to its origin node  sandbox=deadbeef-…0002  node=aenv-worker-01  state=local_only
scheduler pinned a sandbox to its origin node  sandbox=deadbeef-…0003  node=aenv-worker-01  state=local_only
```

`lookup_node_total` diff（before → after，**四次请求四次计数，无背景流量污染**）：

```
+ result="placed"    1        ← X
+ result="pinned"    2        ← Y、Z
+ result="not_found" 1        ← CTRL
  result="bound_binding" 3    ← 不动
```

gateway 侧独立佐证：

```
routing a sandbox the scheduler resolved from the paused registry  sandbox=deadbeef-…0001 location=placed node=aenv-worker-01 origin=aenv-worker-01
routing a sandbox the scheduler resolved from the paused registry  sandbox=deadbeef-…0002 location=pinned node=aenv-worker-01 origin=aenv-worker-01
routing a sandbox the scheduler resolved from the paused registry  sandbox=deadbeef-…0003 location=pinned node=aenv-worker-01 origin=aenv-worker-01

+ sandbox_location_total{location="placed"} 1
+ sandbox_location_total{location="pinned"} 2
  upstream_proxy_duration_seconds_count{route="/sandboxes/{sandbox_id}",status="4xx"}  3 ← 三次都真的转发出去了
```

| 行 | 期望 | 实测 | 判定 |
|---|---|---|---|
| X | PLACED，`origin_preferred=true` | PLACED，node=worker-01=origin，**`origin_preferred=true`** | ✅ |
| Y | PINNED → worker-01 | PINNED → worker-01 | ✅ |
| Z | PINNED → worker-01（顺带进 `parked_lease_expiring`）| 同上；`parked_lease_expiring=1` | ✅ |

> 与任务书的一处差异：任务书预期 Y 行 READY 时会 "claim_for_resume 答 NotFound ⇒ 客户端 404"。
> 我用的是 GET 不是 resume，所以 404 来自"节点上没有这台沙箱"，不是 claim 失败。
> 要验的 PINNED 判定一样拿到了，且三行都毫发无损地留到了删除那一步。

#### B. worker-01 **DRAINING**

```bash
curl -X POST -H 'X-Admin-Token: …' -d '{"status":"draining"}' http://10.10.10.203:30800/nodes/aenv-worker-01
HTTP:204
# 5s 后 scheduler 视角已更新：
aenv-worker-01=draining   aenv-master-01=ready
```

同样三行：

```
X -> {"code":404,"message":"sandbox deadbeef-…0001 not found"}                            HTTP:404
Y -> sandbox is local_only on node "aenv-worker-01", which is not accepting work          HTTP:503
Z -> sandbox is local_only on node "aenv-worker-01", which is not accepting work          HTTP:503
```

scheduler 判定日志：

```
[info] scheduler placed a paused sandbox                                       sandbox=deadbeef-…0001  node=aenv-master-01  origin=aenv-worker-01  origin_preferred=false
[warn] scheduler cannot pin a sandbox to an origin node that is not accepting work  sandbox=deadbeef-…0002  origin=aenv-worker-01  state=local_only
[warn] scheduler cannot pin a sandbox to an origin node that is not accepting work  sandbox=deadbeef-…0003  origin=aenv-worker-01  state=local_only
```

`lookup_node_total` diff：

```
  result="placed"                1 → 2      ← X（这次落在 master-01）
+ result="origin_unschedulable"  0 → 2      ← Y、Z
```

**🔴 零转发取证（自证过的探针）**：同一窗口发了 **3** 次请求，
gateway 的上游转发计数只涨了 **1**：

```
upstream_proxy_duration_seconds_count{route="/sandboxes/{sandbox_id}",status="4xx"}  3 → 4
sandbox_location_total{location="placed"}                                            1 → 2
sandbox_location_total{location="pinned"}                                            不动
```
⇒ 那 1 次是 X（转发到 master-01），Y/Z 那 **2 次 503 一次上游转发都没发生**。
这条探针有分辨力：**同一个计数器**对"确实转发了"的请求非零、对"被拒的"为零。

| 行 | 期望 | 实测 | 判定 |
|---|---|---|---|
| X | PLACED 选中 **master-01**，`origin_preferred=false` | PLACED → **aenv-master-01**，**`origin_preferred=false`** | ✅ **T1 §5.2 那条 BLOCKED 关闭** |
| Y | FailedPrecondition ⇒ 503，**零转发** | 503 + 原话 body；`origin_unschedulable`+1；转发计数不动 | ✅ **T1 V1-4 那条 BLOCKED 关闭** |
| Z | 同 Y（并验 P1-4 两分支拆分）| 同 Y；两分支已在 §3.3 拆开 | ✅ |

X 行这一条正是 T1 V1-3 计划原文第 4 步："把 origin 置 DRAINING 后期望选中另一台"。
**同一行沙箱、同一个 scheduler 进程，只改 worker-01 的状态，
`origin_preferred` 从 `true` 翻成 `false`、落点从 worker-01 换成 master-01** ——
这排除了"总是选中 origin 只是因为只有一台可选"，也排除了"origin 亲和性是硬绑定"。

#### C. 可逆性对照（证明 503 是 DRAINING 造成的，不是行本身坏了）

把 worker-01 改回 `ready`，5 秒后同一行 Y：

```
Y -> {"code":404,"message":"sandbox deadbeef-…0002 not found"}   HTTP:404
lookup_node_total{result="pinned"}                2 → 3
lookup_node_total{result="origin_unschedulable"}  停在 2 不动
```
⇒ 503 随 DRAINING 出现、随 DRAINING 消失。

#### D. F4 的另一支：`origin_not_reporting`（冷 scheduler 窗口）

用 T1 §5.1 那四行命令开窗口；为绕开 gateway 的 gRPC 重连退避，
用 T1 附录那个直连 gRPC 的 `LookupNode` 探针（20 行 Go，`GOWORK=off go build`）。

**探针先自证**（warm scheduler，已知答案）：

```
id=deadbeef-0000  OK node=aenv-worker-01  location=SANDBOX_LOCATION_PLACED  origin="aenv-worker-01"   ← X
id=deadbeef-0000  OK node=aenv-worker-01  location=SANDBOX_LOCATION_PINNED  origin="aenv-worker-01"   ← Y
id=deadbeef-0000  OK node=aenv-worker-01  location=SANDBOX_LOCATION_PINNED  origin="aenv-worker-01"   ← Z
id=00000000-0000  code=NotFound  msg="sandbox assignment not found"                                  ← 对照老实答 NotFound
```

开窗口：

```
12:55:31  kubectl scale deploy/agentenv-scheduler --replicas=0
          sleep 165                                   # 让 node 上报退避涨到 MAX_REPORT_BACKOFF
12:58:16  kubectl scale deploy/agentenv-scheduler --replicas=1
12:58:18.32  scheduler 进程启动（日志时间戳）
12:58:28  Pod Ready → 立刻 port-forward 9090 直连
```

冷窗口内连打 6 轮，每轮 4 个 id（合成 Y / **既有的真行 01a01853** / 合成 X / 全零对照）：

```
12:58:37.975 id=deadbeef-0000  code=FailedPrecondition  msg="sandbox is local_only on node \"aenv-worker-01\", which is not reporting"
12:58:38.344 id=01a01853-30a6  code=FailedPrecondition  msg="sandbox is local_only on node \"aenv-worker-01\", which is not reporting"
12:58:38.568 id=deadbeef-0000  OK node=aenv-worker-01  location=SANDBOX_LOCATION_PLACED  origin="aenv-worker-01"
12:58:38.901 id=00000000-0000  code=NotFound  msg="sandbox assignment not found"
…（6 轮，逐轮相同）…
```

新进程的计数器（从零开始，账目完全对得上：6×2 / 6×1 / 6×1）：

```
agentenv_scheduler_lookup_node_total{result="origin_not_reporting"} 12
agentenv_scheduler_lookup_node_total{result="placed"}                6
agentenv_scheduler_lookup_node_total{result="not_found"}             6
（origin_unschedulable 一次都没出现）

日志：x12  scheduler cannot pin a sandbox to an origin node that is not reporting  state=local_only origin=aenv-worker-01
```

**同一条消息也出现在既有的真行 `01a01853` 上**，所以这不是合成行的伪影。

**最后一步：让两个 label 在同一个进程里并存**（排除"只有一支可达"）——
节点回热后，在**同一个 scheduler 进程**里再置一次 DRAINING：

```
Y warm       -> HTTP:404（PINNED，转发过去了）
Y draining   -> sandbox is local_only on node "aenv-worker-01", which is not accepting work   HTTP:503

agentenv_scheduler_lookup_node_total{result="pinned"}                1
agentenv_scheduler_lookup_node_total{result="origin_not_reporting"} 12
agentenv_scheduler_lookup_node_total{result="origin_unschedulable"}  1     ← 三个 label 同表并存 ✅
upstream_proxy_duration_seconds_count{route="/sandboxes/{sandbox_id}"}  逐字不变 ⇒ 零转发 ✅
```

附带确认 T1 F6 的观察（不是缺陷，是设计选择）：冷窗口里
`paused` 的 X 行是 **fail-open**（照样 PLACED 成功），`local_only` 的 Y/Z 是 **fail-closed**。

> 顺带一条与 T1 的表述差异：T1 在冷窗口里看到全零 UUID 被答
> `Unavailable "scheduler is still seeding sandbox assignments"`，本轮看到的是 `NotFound`。
> 不矛盾 —— warmup 是 15s，我第一次探测在进程启动后 **19.6s**（12:58:18.32 → 12:58:37.97），
> warmup 已过。两者都对，只是取样时刻不同。

### 3.5 删除与复核回基线

```sql
DELETE FROM paused_sandboxes
 WHERE sandbox_id IN ('deadbeef-…0001','deadbeef-…0002','deadbeef-…0003');
DELETE 3
```

```
$ SELECT count(*) FROM paused_sandboxes;                         →  1     （= 基线）
$ SELECT count(*) … WHERE sandbox_id::text LIKE 'deadbeef%';     →  0     （一行不剩）
$ SELECT … FROM paused_sandboxes;
      id       |   state    | origin_node_id | has_snap
---------------+------------+----------------+----------
 01a01853-30a6 | local_only | aenv-worker-01 | f                 （接手前那行，原封不动）
```

等一个对账周期后，指标与**最初基线逐字 diff**：

```
$ diff b-base-reg.txt b-final-reg.txt
（无输出）   ✅ 13 个序列逐字回到基线
  parked_lease_expiring  1 → 0
  rows{local_only}       3 → 1
  rows{paused}           1 → 0
  stranded_rows          2 → 1
```

只读 API 复核，也回到只剩那一行：

```json
{"sandboxes":[{"sandboxID":"01a01853-30a6-75a3-8b25-eb87b3db0265","state":"local_only",
 "originNodeID":"aenv-worker-01","claimedByNodeID":"","snapshotID":"","holderNodeID":"aenv-worker-01",
 …}],"databaseTimeUnixMs":1787144450584}
```

**三行全程没有被节点侧动过**：删除前复查 `generation` 仍为 1、`claimed_by_node_id` 仍为 NULL，
与插入时逐字一致 ⇒ §3.0 那条"对节点惰性"的判断经实测成立。

---

## 4. 回归冒烟（我改过 DRAINING 两次、重启过 scheduler，必须确认没留伤）

用模板 `uns-scaffold-v3` 走一整轮，**两个必传字段都带上**：

| 步骤 | 结果 |
|---|---|
| `POST /sandboxes`（`timeout:600` + `autoResume.enabled:true`）| **201**，`sandboxID=01a01a1c-97ef-…` |
| `GET /sandboxes/{id}` | **200** |
| `POST /sandboxes/{id}/pause` | **204**，登记表出现 `paused` + **snapshot 非空**（真发布到了 RustFS，没触发 kb 那条并发 503 降级）|
| `POST /sandboxes/{id}/resume` | **201** |
| `GET /sandboxes/{id}` | **200** |
| `DELETE /sandboxes/{id}` | **204**，登记表行随删除清掉 |

交付态：`GET /sandboxes` ⇒ `[]`，两节点 `ready`、`sandboxes=0`，`/health` ⇒ 204。

---

## 5. 集群改动登记表 + 复原情况

| # | 改动 | 性质 | 现状 |
|---|---|---|---|
| 1 | `deploy/agentenv-scheduler` image → `…/agentenv-scheduler:cp1-c35f5ec` | **本轮发布，刻意保留** | 在跑，imageID 已对账 |
| 2 | `deploy/agentenv-gateway` image → `…/agentenv-gateway:cp1-c35f5ec` | **本轮发布，刻意保留** | 在跑，imageID 已对账 |
| 3 | gateway 容器新增 `metrics` 端口 9102（F7）| **本轮发布，刻意保留** | 在，与 `c35f5ec` base 清单一致 |
| 4 | gateway Service 新增 `metrics` 端口 9102 → `targetPort: metrics`（F7）| **本轮发布，刻意保留** | 在，与 base 清单一致 |
| 5 | scheduler 的 env / metrics 端口 / Service 端口（T1 留下的）| 沿用 | 未动，已确认仍在 |
| 6 | `sts/agentenv-postgres` scale 0 → 1（F5）| 临时 | ✅ 已复原（`--replicas=1`，Running，PVC 数据没丢，`count(*)` 与实验前一致）|
| 7 | `deploy/agentenv-scheduler` scale 0/1 一次（F4 冷窗口）| 临时 | ✅ 已复原（replicas=1，Running 12m）|
| 8 | `paused_sandboxes` INSERT 3 行合成行 | 临时（经裁决）| ✅ **已 DELETE 3，`count(*)` 回 1，`LIKE 'deadbeef%'` 为 0，指标逐字回基线** |
| 9 | `aenv-worker-01` 置 DRAINING **两次** | 临时 | ✅ 两次都已改回 `ready` 并复验（`/nodes` 显示 `ready`）|
| 10 | 建了 1 台沙箱 `01a01a1c-…`（冒烟）| 临时 | ✅ 已 DELETE，`GET /sandboxes` ⇒ `[]`，登记表行已清 |
| 11 | 数次短暂 `kubectl port-forward`（scheduler 9090）| 无状态 | ✅ 全部已 kill，`ps` 查零残留 |
| 12 | 工作区临时建 `services/lookupprobe/`（gRPC 探针源码）| 临时 | ✅ 已 `rm -rf`，`git status` 干净（仅剩本来就未跟踪的 `docs/proposals/`）|

**未做的事**（明确登记）：未跑 `make k8s-apply`；未改任何 ConfigMap；未碰 node DaemonSet；
未重建 Rust 镜像（`c35f5ec` 的 `src/` 零改动）；未用过 `:latest`；
**未对 `paused_sandboxes` 做过除那 3 行 INSERT + DELETE 以外的任何写操作**。

**交付时的集群状态**（与我接手前一致）：

```
health 204 | aenv-worker-01 ready sandboxes=0 | aenv-master-01 ready sandboxes=0
GET /sandboxes ⇒ []
paused_sandboxes ⇒ 1 行（01a01853-…，local_only，origin aenv-worker-01，snapshot NULL —— 接手前就有的那行）
registry_enabled=1，对账循环在跑，指标与接手时逐字一致
pods: gateway / scheduler / 2×node / postgres / rustfs / agent-console 全部 1/1 Running
```

备份文件在本机 `/tmp/claude-1000/aenv-verify-t2/`：
`bak-agentenv-{scheduler,gateway}-{deploy,svc}.yaml` + `bak-configmaps.yaml`。

---

## 6. 发现的缺陷 + 能不能合并

### 结论：**能合并。**

判据：

1. **六条修复全部在真集群上验到，且每条都有反向对照** ——
   F1 没外溢到 `/nodes`、F2 没变成"拒绝一切"、F3 的正确拼法真的在过滤、
   F5 的计数器在健康轮确实会动、F7 改前的 Service 真的抓不到。
2. **F4 是本轮质量最高的一条**：同一行沙箱在同一个进程里跑出
   `pinned` / `origin_not_reporting` / `origin_unschedulable` 三种结局，
   日志 / metric label / HTTP 文案三处一致地分开。T1 那 7 次被误标的 503，
   在新代码上会被标成 `origin_not_reporting` —— 排障方向从"查 admin API 的 DRAINING"
   纠正为"查节点心跳"，这才是真现场。
3. **T1 的两条 BLOCKED 全部关闭**，且是用比 T1 更强的对照关掉的：
   X 行的 `origin_preferred` 在同一行沙箱上被观测到从 `true` 翻成 `false`；
   Y/Z 的 503 有"同一计数器对转发的请求非零、对被拒的为零"这条自证探针。
4. **顺带补上了 T1 §2 自认"没有分辨力"的 P1-4 那半边**：Y/Z 两行只差一个
   `snapshot_id`，落进了两个不同的桶。这条不变式现在有真凭据了。
5. **正常链路不回归**：create/pause/resume/delete 全绿，pause 真发布了快照。
6. **集群完全复原**，指标与 DB 逐字回到接手前。

### 发现的问题（都不阻塞合并）

#### N1 🟡 F1 是"一道门"不是"一把锁"，风险口径改小了但没归零

`X-API-Key: dummy` 就能读到全集群沙箱 ID + 归属节点 + 租约时间 —— 实测通过。
这是 D4 §5.1 自己登记过的、按裁决保留的口径。**别因为现在返回 401 就以为这条关掉了。**
上生产前仍需要 T1 F1 那个结论：要么 gateway 长出真凭据校验，要么确认它只在内网可达。

#### N2 🔴 `POST /nodes/{id}` 仍然是"任何非空 token 就能把节点摘出轮转"—— 本轮实测坐实

D4 §5.2 提过这条，本轮把它**做实了**：我用一个临时编的 `X-Admin-Token`，
经**对宿主机开放的 NodePort 30800**，两次把 `aenv-worker-01` 置成 DRAINING，全程 204。
节点侧 `src/api/impls/auth.rs` 只查头存在不查值（代码里有 `TODO: Validate configured
authentication credentials instead of only checking that they are present.`）。

这比 F1 原本那条严重得多（**写操作**，能让一台机器停止接单），
但**不是本轮引入的，也不在本轮范围内**。建议单独立项，且优先级高于 N1。

#### N3 🟡 `invalid_rows` 至今没有分辨力（本轮**没有**补上）

全程恒为 0。要给它分辨力需要插一行 `paused` + `snapshot_id IS NULL` 的行 ——
**这超出了本次裁决授权的三行范围，我没做**。而且它恰恰是文档说会让节点侧
`get_many` 整批报错、静默冻结一台机器对账的那种行；虽然合成 id 永远进不了
`get_many` 的批次（§3.0），但这条值得单独报请裁决再验。

**建议**：下次授权时加第四行 `deadbeef-…0004`（`paused` + `snapshot_id NULL`），
期望 `invalid_rows` 0 → 1 而 `rows{paused}` 也 +1，验完即删。
这是目前唯一还没被真集群证明有分辨力的指标。

#### N4 🟢 `/nodes` 上有稳定的背景轮询流量，量指标要挑对序列

实测 `upstream_proxy_duration_seconds_count{route="/nodes/{node_id}",2xx}` 在 3 分钟内
从 103 涨到 110（agent-console 在轮询）。`/sandboxes/{sandbox_id}` 那条序列则完全干净，
本轮所有计数 diff 都精确对得上请求数。后续做类似取证时**别用 `/nodes` 那条序列做基线**。

#### N5 🟢 `reconcile_duration_seconds` 仍只量"读 + 派生"一整段

D4 §5.5 自己记过。本轮只验了失败轮已被摘出去（F5 PASS），没拆读/派生两段。不影响合并。

### 合并前建议顺手做的

无。六条修复本身没发现需要返工的地方。

### 上生产前必须先有结论的

- **N2**（`POST /nodes/{id}` 无实质鉴权的写操作）—— 建议排在 N1 前面。
- **N1**（`X-API-Key` 只查在不查值）。

两条都是**既有**问题，不是 `c35f5ec` 引入的。

---

## 附：本轮新增的可复用工具

1. **合成登记表行 = 登记表分支的通用取样办法**（比冷窗口好用得多）：
   合成 id 不在任何 roster、也没有 binding ⇒ `lookupNode` 每次都直落第 3 步。
   T1 §5.1 那个"paused 沙箱永远留在 roster 里"的死锁对它不成立。
   配合 `GET /sandboxes/{id}`（而不是 `resume`）探测，可以反复打而不改动任何行。
2. **直连 gRPC 的 `LookupNode` 探针**（T1 附录那 20 行，本轮实证可用）：
   注意客户端构造函数是 `schedulerv1.NewSchedulerClient`（**不是** `NewSchedulerServiceClient`），
   `Node` 的 getter 是 `GetNodeId()`。放在 `services/` 下 `GOWORK=off go build` 即可，用完记得删。
3. **集群内抓 Service 指标**：`agentenv-postgres-0` 与 `agent-console` 两个 Pod 里都有
   `wget`（没有 `curl`、没有 `python3`），`wget -q -T 8 -O - http://<svc>.<ns>.svc.cluster.local:<port>/metrics`
   就是 Prometheus 会走的那条路，比 `port-forward` 更能证明"Service 上抓得到"。
