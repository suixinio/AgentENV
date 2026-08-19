# T1 集群验证报告：阶段 0 + 阶段 1（pve-sg dev，203/204）

> 2026-08-19 · 测试 agent 交付。被测代码 `central-control-plane-phase01` @ `7e6f790`。
> 上位：[`_verify-plan-phase01.md`](_verify-plan-phase01.md)、[`_recon-R3-cluster-runbook.md`](_recon-R3-cluster-runbook.md)、
> [`_impl-D3-harden.md`](_impl-D3-harden.md)、[`_impl-plan-control-plane-phase01.md`](_impl-plan-control-plane-phase01.md)。
> 全程未跑 `make k8s-apply`，未动 node DaemonSet，未改任何 ConfigMap，未对 `paused_sandboxes` 做过任何写操作。

---

## 0. 一页速览

| 条目 | 结论 |
|---|---|
| V0-1 装配自证 | ✅ PASS |
| V0-2 指标齐全 | ✅ PASS（16 个指标族一个不缺，含新拆的 `stranded_rows`）|
| V0-3 与 PG 真值对账 | ✅ PASS（两轮，逐格一致；用独立 SQL 复算了 `stranded` / `invalid` / `live_lease_lapsed`）|
| V0-4 `invalid_rows` 恒 0 | ✅ PASS |
| **V0-5 🔴 对照探针（读失败不清零）** | ✅ **PASS —— 本轮最硬的一条，见 §4.1** |
| V0-6 只读 API | ✅ PASS（含过滤器自证；NULL→`null` 也实测到了）|
| **V1-1 🔴 registry 不可达 ⇒ 503 不是 404** | ✅ **PASS**（走等价探针，理由见 §3.1）|
| V1-2 不存在的沙箱 ⇒ 404 | ✅ PASS |
| V1-3 origin 亲和性 | ✅ **正向 PASS，且拿到了比计划更强的对照**（两个不同 origin 各选各的，见 §4.2）；**计划原文第 4 步（DRAINING）BLOCKED** |
| V1-4 `local_only` + origin 不可服务 ⇒ 503 且不转发 | 🟡 **PARTIAL**：`local_only` ⇒ FailedPrecondition ⇒ gateway 503 ⇒ **零转发**（有硬证据）；但触发原因是「origin 无新鲜 roster」而不是计划要求的 DRAINING —— **DRAINING 这个具体触发条件 BLOCKED** |
| V1-5 补丁真的删了 | ✅ PASS（带反向对照）|
| V1-6 正常链路不回归 | ✅ PASS（两节点各跑 create/pause/resume/delete + 数据面 exec）|
| 额外：阶段 0 回滚（DSN 置空）| ✅ PASS（`enabled=0` / 501 / 404 老行为）|

**BLOCKED 的两条只有一个共同原因**，且不是代码缺陷，是**取样条件在这套集群里取不到**：
paused 沙箱**永远留在 heartbeat roster 里** ⇒ lookup 第 1/2 步必然命中 ⇒ 登记表分支走不到；
而「知道 origin 在 DRAINING」和「roster 里没有这台沙箱」由**同一条 heartbeat** 决定，互斥。
详见 §5.1。解法（造两行合成登记表行）已两次报请裁决，截至交付未收到答复。

**合并判断：建议合并。** 判据见 §7。

---

## 1. 发布记录

### 1.1 构建

```
构建机 10.10.10.204:/opt/AgentENV
  sudo git -c safe.directory=/opt/AgentENV fetch fork central-control-plane-phase01
  sudo git -c safe.directory=/opt/AgentENV checkout --detach FETCH_HEAD
  HEAD = 7e6f7904ade0c14fef9e791354262d0edb30b0ba   （tracked 文件零 dirty）

TAG = cp0-7e6f790     （不可变 tag，全程未用 :latest）
  docker build -f deploy/docker/Dockerfile.scheduler -t 10.10.10.204:5000/agentenv-scheduler:$TAG .   rc=0
  docker build -f deploy/docker/Dockerfile.gateway   -t 10.10.10.204:5000/agentenv-gateway:$TAG   .   rc=0
  docker push …/agentenv-scheduler:$TAG  digest sha256:e841d925649d3e13c657d40d840f27c0b75ed2cadca6e6141d78b8b3c835a3c0
  docker push …/agentenv-gateway:$TAG    digest sha256:e45b9c448b3d724b7b0189eeff18ce3e8cc764ace04e5b3a8d6020ab5f6a3b6c
```

**Rust 侧未重建**（本轮 `src/` 零改动），两个 `agentenv-node` Pod 保持 `merge-abe1bbd`（imageID `947f9a…`）不变。

### 1.2 发布

只用 `kubectl patch` + `kubectl set image`，**没有 apply 任何清单**。

```bash
# 备份（/tmp/claude-1000/aenv-verify/ 下）
bak-scheduler-deploy.yaml / bak-gateway-deploy.yaml / bak-scheduler-svc.yaml / bak-configmaps.yaml

# 1) scheduler Deployment 注入 env（照 7e6f790 的 base 清单）+ metrics 容器端口
kubectl -n agentenv-system patch deploy agentenv-scheduler --type=json -p '[
  {"op":"add","path":"/spec/template/spec/containers/0/env","value":[
    {"name":"SCHEDULER_REGISTRY_DSN","valueFrom":{"secretKeyRef":{"name":"agentenv-postgres","key":"dsn","optional":true}}},
    {"name":"SCHEDULER_REGISTRY_CLUSTER_ID","valueFrom":{"secretKeyRef":{"name":"agentenv-postgres","key":"cluster_id","optional":true}}}]},
  {"op":"add","path":"/spec/template/spec/containers/0/ports/-","value":{"name":"metrics","containerPort":9101,"protocol":"TCP"}}]'
# 2) scheduler Service 补 metrics 端口（R3 B7）
kubectl -n agentenv-system patch svc agentenv-scheduler --type=json -p '[
  {"op":"add","path":"/spec/ports/-","value":{"name":"metrics","port":9101,"targetPort":"metrics","protocol":"TCP"}}]'
# 3) 换镜像
kubectl -n agentenv-system set image deploy/agentenv-scheduler scheduler=10.10.10.204:5000/agentenv-scheduler:cp0-7e6f790
kubectl -n agentenv-system set image deploy/agentenv-gateway   gateway=10.10.10.204:5000/agentenv-gateway:cp0-7e6f790
```

rollout 结果：两个都 `successfully rolled out`。

**imageID 对账（不是只看 image 名）：**

```
agentenv-scheduler-6756ccc85c-bbghz  10.10.10.204:5000/agentenv-scheduler@sha256:e841d925…  == push digest ✅
agentenv-gateway-6d5bdb9d6b-k9xc6    10.10.10.204:5000/agentenv-gateway@sha256:e45b9c44…    == push digest ✅
```

> `SCHEDULER_REGISTRY_CLUSTER_ID` 用的 Secret key `cluster_id` **在集群里不存在**，`optional:true` 生效，
> 值为空。这不是配错：dev 只有一个 cluster（全零 UUID）。它顺带把 D3 的 P2-12 修复触发了（见 V0-1）。

---

## 2. 阶段 0 逐条

### V0-1 装配自证 —— ✅ PASS

```
$ kubectl -n agentenv-system logs deploy/agentenv-scheduler
{"level":"warn","caller":"cmd/main.go:216","msg":"scheduler paused registry has no cluster id;
  every read covers every cluster in the database","env":"SCHEDULER_REGISTRY_CLUSTER_ID"}
{"level":"info","caller":"cmd/main.go:231","msg":"scheduler paused registry enabled",
  "cluster_id":"","max_connections":4,"reconcile_interval":30}
{"level":"info","caller":"cmd/main.go:108","msg":"scheduler gRPC server listening","addr":":9090",
  "strategy":"round_robin","binding_store":"memory","query_only":false,"paused_registry":true}
{"level":"info","caller":"cmd/main.go:121","msg":"scheduler metrics server listening","addr":":9101"}
```

三件事一次拿到：装配日志 ✅、启动行 `paused_registry=true` ✅、**D3 的 P2-12（cluster_id 空要 WARN 并点名环境变量）实测生效** ✅。

### V0-2 指标可达且齐全 —— ✅ PASS

`kubectl port-forward deploy/agentenv-scheduler 9101:9101` 后 `curl localhost:9101/metrics`。
计划要求的 15 条 + `enabled` 全部存在，**一个不缺**：

```
agentenv_scheduler_registry_enabled 1
agentenv_scheduler_registry_rows{state="local_only"} 1
agentenv_scheduler_registry_rows{state="paused"|"publishing"|"resuming"|"running"} 0
agentenv_scheduler_registry_untracked{node="aenv-master-01"|"aenv-worker-01"} 0
agentenv_scheduler_registry_ghost{node=…} 0
agentenv_scheduler_registry_stale_copy{node=…} 0
agentenv_scheduler_registry_rows_without_roster{node=…} 0
agentenv_scheduler_registry_holder_conflict 0
agentenv_scheduler_registry_parked_lease_expiring 0
agentenv_scheduler_registry_stranded_rows 1
agentenv_scheduler_registry_live_lease_lapsed 0
agentenv_scheduler_registry_reclaimable_now 0
agentenv_scheduler_registry_roster_stale{node=…} 0
agentenv_scheduler_registry_invalid_rows 0
agentenv_scheduler_registry_read_failures_total 0
agentenv_scheduler_registry_reconcile_duration_seconds_{bucket,count,sum}
agentenv_scheduler_registry_last_success_timestamp_seconds 1.787139107e+09
```

**按 D3 §1 的新口径核过**：`parked_lease_expiring` 与 `stranded_rows` 是两条独立序列，
Help 文案里写明了「无快照的行不进租约口径」和「每次 pause 都会短暂经过，要按持续时长告警」。

附带发现（不在计划里，PASS）：D3 §1 P0-1① 说的「给有新鲜 roster 的节点播 0 值种子」确实生效 ——
健康集群里 `rows_without_roster` 有两条 0 值子序列而不是一条都没有，`grep` 得到。

### V0-3 与 PG 真值对账 —— ✅ PASS（做了两轮）

**第一轮**（集群原始状态，1 行）：

```
$ psql -c "SELECT state, count(*), count(snapshot_id) FROM paused_sandboxes GROUP BY state;"
   state    | count | with_snapshot
------------+-------+---------------
 local_only |     1 |             0
```
⇒ `rows{local_only}=1`，其余四个 state 全 0。逐格一致 ✅

**第二轮**（我造完样本后，3 行）：

```
PG:  local_only 1 (snap 0)   |   running 2 (snap 2)
指标: rows{local_only}=1  rows{running}=2  rows{paused|publishing|resuming}=0     ✅
```

同时用**独立 SQL 复算**了三个派生量（不是照抄 Go 的结论）：

```sql
SELECT count(*) FILTER (WHERE state='paused' AND snapshot_id IS NULL)                        AS invalid_rows,
       count(*) FILTER (WHERE state IN ('publishing','local_only') AND snapshot_id IS NULL)  AS stranded,
       count(*) FILTER (WHERE state IN ('running','resuming')
                        AND COALESCE(lease_expires_at, updated_at) < now())                  AS live_lease_lapsed;
 invalid_rows | stranded | live_lease_lapsed
--------------+----------+-------------------
            0 |        1 |                 0
```
对应指标 `invalid_rows=0` / `stranded_rows=1` / `live_lease_lapsed=0` ✅

#### 🔴 P1-4 那条：`stranded_rows=1` ✅，但 `parked_lease_expiring=0` **这半边没有分辨力**

`stranded_rows=1` 是真凭据 —— 这条序列是 P1-4 修复**新增**的，旧口径根本没有它。

但 `parked_lease_expiring=0` 证明不了什么，理由是实测出来的：那行 `local_only` 的**租约一直在被 origin 续**，

```
updated_at        2026-08-19 11:29:02+00
lease_expires_at  2026-08-19 11:30:32+00     (= updated_at + 90s，每 ~30s 刷一次)
db now()          2026-08-19 11:29:32+00
```

⇒ `COALESCE(lease_expires_at, updated_at) < now() + 30s` **恒为 false**。
就算 P1-4 没修，这行今天也落不进那个桶。**要真正分开两个分支，需要两行租约已过期的对照样本
（一行有快照、一行没有）—— 这需要往 `paused_sandboxes` 写合成数据，已报请裁决，未获答复。**

### V0-4 `invalid_rows` 恒为 0 —— ✅ PASS

两轮都是 0，并与上面那句独立 SQL 一致。

分辨力说明：这条**当前也是弱证据**（库里从来没出现过 `paused` + 无快照的行，所以 0 是唯一可能的读数）。
它的价值在"以后出现时能看见"，本轮只能证明它没有假阳性。

### V0-5 🔴 对照探针 —— ✅ **PASS**（本报告的核心，展开见 §4.1）

### V0-6 只读 API —— ✅ PASS

```
$ curl -H 'X-API-Key: dummy' 'http://10.10.10.203:30800/registry/sandboxes'
{"sandboxes":[{"sandboxID":"01a01853-…","clusterID":"00000000-…","state":"local_only","generation":3,
 "originNodeID":"aenv-worker-01","claimedByNodeID":"","snapshotID":"","holderNodeID":"aenv-worker-01",
 "pausedAtUnixMs":1787116421789,"updatedAtUnixMs":1787139122083,
 "leaseExpiresAtUnixMs":1787139212083,"sandboxExpiresAtUnixMs":1787118191277}],
 "databaseTimeUnixMs":1787139125464}     HTTP 200
```

- 字段齐（`holderNodeID` / 两个租约字段 / `databaseTimeUnixMs`）✅
- `snapshotID` NULL ⇒ 空串、`claimedByNodeID` NULL ⇒ 空串 ✅
- **NULL ⇒ JSON `null`（不是 0）✅ 实测到了**：我 pause 出来的那行
  `"sandboxExpiresAtUnixMs": null`（原始的那行两个租约列都非 NULL，看不到这个）
- **过滤器自证**（否则"过滤了"和"没过滤"分不开）：

  | 查询 | 结果 |
  |---|---|
  | `?state=local_only` | 1 行 ✅ |
  | `?state=paused`（当时无该态）| `[]` ✅ |
  | `?nodeID=aenv-worker-01` | 1 行 ✅ |
  | `?nodeID=aenv-master-01`（必然落空的对照）| `[]` ✅ |
  | `?limit=1` | 1 行 ✅ |
  | `?limit=-1` | `invalid limit: must not be negative` HTTP 400 ✅ |
  | `POST /registry/sandboxes` | `route not found` 404 ✅（只挂 GET）|

  ⚠️ 我第一次用的是 `?nodeId=`（小写 d），**静默被忽略、返回全量**。参数名是 `nodeID`（`registry_list.go:66`），
  大小写敏感且不校验未知参数 —— 差点让我把"过滤没生效"当成 PASS。

---

## 3. 阶段 1 逐条

### V1-1 🔴 registry 不可达 ⇒ 503 而不是 404 —— ✅ PASS（等价探针）

**先按计划要求做了正常状态的自证**（否则 503 可能来自别的原因）：

| 状态 | 请求 | 结果 |
|---|---|---|
| registry 可读 | `POST /sandboxes/0000…0000/resume` | **404** `sandbox assignment not found` |
| registry 可读 | `POST /sandboxes/<真沙箱>/resume` | **201**（正常恢复）|
| **registry 不可读**（PG scale 到 0）| `POST /sandboxes/0000…0000/resume` | **503** `paused registry unavailable` |
| **registry 不可读** | `GET /sandboxes/0000…0000` | **503** 同上 |
| **registry 不可读** | `GET /registry/sandboxes` | **503** 同上 |

scheduler 侧日志坐实走的是哪条分支：

```
{"level":"warn","caller":"internal/lookup.go:171","msg":"scheduler lookup registry read failed",
 "sandbox_id":"00000000-0000-0000-0000-000000000000",
 "error":"query registry row: failed to connect to `user=aenv database=aenv`: … connection refused"}
```
`lookup.go:171` = `case registryErr != nil` 分支；计数器 `agentenv_scheduler_lookup_node_total{result="unavailable_registry"} 2`。

**为什么用「不存在的沙箱」而不是计划写的「有登记表行但 binding 已过期的沙箱」：**

1. 「有行」这个条件在 `lookupNode` 里**根本到不了** —— `reader.Get()` 先返回 error，
   `registryErr != nil` 分支在 `found` 被读之前就 return 了（`lookup.go:168-176`）。
   有没有行对这条分支毫无影响。
2. 反过来，「不存在的沙箱」才是**唯一会退化成 404 的那个输入** —— 也就是要修的 bug 的原形。
   同一条路径，registry 可读时答 404、不可读时答 503，这正是要证的命题。
3. 我**确实按计划原文试过**一次（冷 scheduler + PG 挂 + 对一台有 `paused` 行的沙箱发 resume）：
   没做成，因为 roster 抢先命中了（根因见 §5.1），请求被转到 node，node 自己因为读不到 PG 而卡了 30s：
   ```
   11:46:00 WARN sandboxes_sandbox_id_resume_post: failed to read the paused registry row
            error=… pool timed out … sandbox_id=01a019d1-…
   ```

### V1-2 不存在的沙箱 ⇒ 404 —— ✅ PASS

```
$ curl -X POST -H 'Content-Type: application/json' -d '{"timeout":300}' \
    http://10.10.10.203:30800/sandboxes/00000000-0000-0000-0000-000000000000/resume
sandbox assignment not found          HTTP 404
```

顺带这就是 **V1-5 的反向对照**：改之前同一条请求返回的是 **node 的** JSON
`{"code":404,"message":"sandbox 00000000-… not found"}`（gateway 先 `scheduleRecoveryNode` 挑了一台机器再转发过去，
由那台机器回 404）；现在返回的是 **gateway/scheduler 自己的** 纯文本 `sandbox assignment not found`，
说明 recovery 那一跳没了。

⚠️ 顺带发现：`POST /sandboxes/{id}/resume` **不带 `Content-Type: application/json` 会拿到 415**，
计划里的 curl 命令照抄会失败。

### V1-3 origin 亲和性 —— ✅ 正向 PASS（对照比计划更强）；🔴 计划第 4 步 BLOCKED

见 §4.2。一句话：**同一个冷窗口内，两台不同 origin 的 paused 沙箱各被选回各自的 origin，5/5 无交叉**，
在 `round_robin` 策略下这排除了"只有一台可选"和"轮询巧合"两种解释。
计划原文第 4 步（把 origin 置 DRAINING 后期望选中另一台）**取不到样本**，理由见 §5.1。

### V1-4 `local_only` + origin 不接单 ⇒ 503 且不打到 origin —— 🟡 PARTIAL

**做到了的部分（有硬证据）：**

`01a01853`（`local_only`，origin `aenv-worker-01`，无快照）在 origin 不可服务时：

```
12:09:38  sandbox is local_only on node "aenv-worker-01", which is not accepting work   HTTP:503
12:09:40  （同上）  HTTP:503        ← 连续 7 次
…
12:09:48  {"…","state":"paused",…}                                                       HTTP:200   ← 条件消失后立刻恢复
```

- gateway **503**，body 就是 scheduler 的原话 ✅（`FailedPrecondition → 503` 映射，`server.go:339`）
- scheduler 日志：`{"level":"warn","caller":"internal/lookup.go:239","msg":"scheduler cannot pin a sandbox to its origin node","sandbox_id":"01a01853-…","state":"local_only","origin_node_id":"aenv-worker-01"}`
- 计数器：`lookup_node_total{result="origin_unschedulable"} 8`

**「请求根本没转发过去」—— 我换了证据，因为计划给的那条探针是无效的：**

计划说「期望 node Pod 日志里没有这次 resume 的记录」。我先自证了这条探针：
**在那 25 次真的被转发并返回 200 的请求期间，node Pod 日志同样一行都没有**
（`kubectl logs agentenv-node-gspxc --since-time=…` 输出 0 行）。
⇒ node 对这类请求根本不记日志，「没有日志」与「没转发」**分不开**，这条探针零分辨力，不能用。

改用 gateway 自己的计数器（同一个 gateway 进程，计数窗口正好覆盖这 7+25 次）：

```
agentenv_gateway_http_request_duration_seconds_count{route="/sandboxes/{sandbox_id}",status="5xx"} 7
agentenv_gateway_http_request_duration_seconds_count{route="/sandboxes/{sandbox_id}",status="2xx"} 25
agentenv_gateway_upstream_proxy_duration_seconds_count{route="/sandboxes/{sandbox_id}",status="2xx"} 25   ← 只有 25
agentenv_gateway_scheduler_rpc_duration_seconds_count{rpc="LookupNode",status="failed_precondition"} 7
agentenv_gateway_sandbox_location_total{location="bound"} 25
```

`upstream_proxy` 只记了 25 次，7 次 503 **一次上游转发都没发生** ✅。
这条探针是自证的：同一计数器对"确实转发了"的请求非零，对"被拒的"为零。

**没做到的部分：** 触发 `origin_unschedulable` 的原因是 **origin 没有新鲜 roster**，
不是计划要求的 **DRAINING**。两者进的是同一个 `schedulableNode` 分支、同一句错误文案，
但严格说 DRAINING 这条路径本轮**没有在集群上跑到**。原因见 §5.1。

### V1-5 补丁真的删了 —— ✅ PASS（带反向对照）

```
$ kubectl -n agentenv-system logs deploy/agentenv-gateway --tail=500 | grep -i "reroute|recovery|replay"
（无输出）

$ grep -rn 'scheduleRecoveryNode|captureReplayBody|restoreReplayBody|maxReplayBodyBytes|rerouteToScheduledNode|allowReroute' services/ --include=*.go
（无输出）

# 反向对照：同一条 grep 打在上一个提交上
$ git grep -n '同上' abe1bbd -- services/
abe1bbd:services/gateway/internal/schedule_hint.go:156,159,161,165,171,175,180,185,187
abe1bbd:services/gateway/internal/server.go:231,293,313,320,323,329,337,361,400,403,445
```
⇒ grep 本身有分辨力，"现在查不到"是真的删了，不是 grep 写错了 ✅

`x-agentenv-reroute` 常量**保留**（`server.go:38`），但只作为原样透传的观测标记，
不再有任何 reroute 分支消费它。运行期行为改变已由 V1-2 的 404 文案变化间接坐实。

### V1-6 正常链路不回归 —— ✅ PASS

两个节点各来一遍，全绿：

| # | 沙箱 | 落点 | create | pause | resume | delete | 数据面 |
|---|---|---|---|---|---|---|---|
| 1 | `01a019cb-…` | `aenv-worker-01` | 201 | 204 | 201（两次）| 204 | `aenv exec` ⇒ `hostname=instance` / `uname -r=6.1.175` ✅ |
| 2 | `01a019d1-…` | `aenv-master-01` | 201 | 204 | 201 | 204 | `aenv exec` ⇒ ✅ |

- 两次 pause 都**成功发布到 RustFS**（`state=paused` + `snapshot_id` 非空），没触发 kb 里那条并发 503 降级
- resume 耗时 ~1s（两台都是）
- `ssh supos@10.10.10.203 '~/.local/bin/aenv list'` 正常返回 JSON
- 删完 `GET /sandboxes` ⇒ `[]`，`/registry/sandboxes` 只剩原来那一行 ⇒ 登记表行随删除一起清掉了 ✅

### 额外（不在计划里）：阶段 0 回滚路径 —— ✅ PASS

计划 §4 说「阶段 0 单独回滚 = 把 `SCHEDULER_REGISTRY_DSN` 置空」。实测：

```
patch env[0] -> {"name":"SCHEDULER_REGISTRY_DSN","value":""} ; rollout

启动行:  "paused_registry":false          ✅（且没有 "paused registry enabled" 那行）
指标:    agentenv_scheduler_registry_enabled 0     ✅ ← P2-10 的自证：与 feature-on 时的 1 分得开
         registry_rows / roster_stale 等序列整体消失（对账循环没跑）
GET /registry/sandboxes            ⇒ 501 "paused registry is not configured"   ✅（与"读不到"的 503 分得开）
POST /sandboxes/0000…/resume       ⇒ 404 "sandbox assignment not found"        ✅ 老行为
GET  /sandboxes/<真沙箱>            ⇒ 200                                       ✅ roster 路径不受影响
```

改回 secretKeyRef 后一切恢复。**feature 开关是干净的，回滚可用。**

---

## 4. 两处对照探针（报告核心）

### 4.1 V0-5：读失败不清零旧 gauge —— ✅ PASS

**为什么不能按计划原文做**：计划说「把 DSN 改成必然连不上的地址，滚一次」。
但**滚 Pod 会换进程，所有指标从零开始** —— 新进程从来没成功读过，所有 gauge 天然是空的/零的，
这恰恰**测不出**「成功过的进程遇到读失败时不清零」这条不变式。

**改用的做法**：让**同一个进程**的读从成功变成失败 —— `kubectl scale sts/agentenv-postgres --replicas=0`。

```
T0（PG 在）：
  registry_rows{local_only}=1  {paused}=1  {running}=1  {publishing}=0  {resuming}=0
  stranded_rows=1   invalid_rows=0   parked_lease_expiring=0   live_lease_lapsed=0   reclaimable_now=0
  untracked{×2}=0   ghost{×2}=0   stale_copy{×2}=0   rows_without_roster{×2}=0   roster_stale{×2}=0
  enabled=1         read_failures_total=0
  last_success_timestamp_seconds = 1.787139617e+09
  reconcile_duration_seconds_count = 19

--- scale sts/agentenv-postgres --replicas=0，等 60s（3 个对账周期）---

scheduler 日志（每 30s 一条）：
  {"level":"warn","caller":"internal/reconcile.go:372","msg":"scheduler registry read failed",
   "error":"begin registry read: … dial tcp 10.43.224.84:5432: connect: connection refused"}

T1（PG 没了）—— 与 T0 逐行 diff，只有两处变化：
  read_failures_total                0 → 3          ✅ 增长
  reconcile_duration_seconds_count  19 → 22         （失败轮也计入直方图，见 §6-F5）

  其余 **全部逐字不变**：
    registry_rows{…} 三条值原样 ✅          stranded_rows 1 ✅        invalid_rows 0 ✅
    untracked/ghost/stale_copy/rows_without_roster/roster_stale 全部子序列都在、值都在 ✅
    enabled 1 ✅
    last_success_timestamp_seconds 1.787139617e+09  **停在原处、没有前进** ✅
```

⇒ **「全 0」与「读不到」分得开**：读不到时 gauge 保留上一轮真实值，
判据是 `read_failures_total` 在涨 + `last_success_timestamp_seconds` 不动。
这条是 D3 §1 P1-7 / FIX-6 在真集群上的复现，也是本次唯一能证明 V0-2/V0-3 那些 0 有意义的实验。

PG 恢复（`--replicas=1`）后读立刻恢复，`last_success` 重新前进。

**副产品（PG 挂掉那 6 分钟的 node 侧行为，供参考）**：node 不 panic、不退出，
每 30s 打 WARN（`renew_lease` / `get_many` / `reclaim_expired_holdings` 三种），
`registry unreadable; stopping paused-record reconciliation` 是软停不是崩溃。恢复后自愈。

### 4.2 V1-3 的对照探针 —— ✅ PASS（比计划要求的更强）

计划的担心是：「没有它，'总是选中 origin' 也可能只是因为只有一台可选」。
计划给的对照是「把 origin 置 DRAINING 后期望选中另一台」——**这个取不到样本（§5.1）**。
我用了另一个能取到、而且**同样能排除那个替代解释**的对照：

**同一个冷窗口内，同时查两台 origin 不同的 paused 沙箱。**

前置：`01a019cb` pause 在 `aenv-worker-01`，`01a019d1` pause 在 `aenv-master-01`，
两行都是 `state=paused` + `snapshot_id` 非空；策略是 `round_robin`；两台节点都 ready 且都在候选集里。

```
11:59:35  id=01a019cb…  OK node=aenv-worker-01  location=SANDBOX_LOCATION_PLACED  origin=aenv-worker-01
11:59:35  id=01a019d1…  OK node=aenv-master-01  location=SANDBOX_LOCATION_PLACED  origin=aenv-master-01
11:59:36  id=0000…0000  code=NotFound  msg="sandbox assignment not found"
11:59:39  id=01a019cb…  OK node=aenv-worker-01  location=SANDBOX_LOCATION_PLACED  origin=aenv-worker-01
11:59:40  id=01a019d1…  OK node=aenv-master-01  location=SANDBOX_LOCATION_PLACED  origin=aenv-master-01
…（共 5 轮，5/5 各回各家，一次交叉都没有）…
11:59:54  id=01a019cb…  OK node=aenv-worker-01  location=SANDBOX_LOCATION_BOUND   origin=          ← 第一条 heartbeat 到达，窗口关闭
```

scheduler 日志（前一个窗口，`origin_preferred` 字段直接可读）：

```
{"msg":"scheduler placed a paused sandbox","sandbox_id":"01a019d1-…",
 "node_id":"aenv-master-01","origin_node_id":"aenv-master-01","origin_preferred":true}   ← 连续 8 条
```
计数器：`lookup_node_total{result="placed"} 8`。

**这个对照排除了什么：**
- 「只有一台可选」—— 两台都被选中过，就在同一个窗口、同一批调用里
- 「round_robin 巧合」—— 轮询会交替，实测 10 次调用零交替，每个 id 恒定选自己的 origin
- 「凡是 PLACED 都答同一台」—— 两个 id 答案不同
- 探针本身有分辨力 —— 同批次里 `0000…0000` 老老实实答 `NotFound`，不是"什么都答 OK"

**没能排除的**：origin 不可调度时是否真的会换一台（那是 §5.1 BLOCKED 的部分）。

### 4.3 附加对照探针（我加的，都自证过）

| 探针 | 对照输入 | 结果 |
|---|---|---|
| `rows_without_roster` 是「从表侧算、节点没了会涨」 | 冷窗口（无任何 roster）| `{aenv-worker-01}=2 {aenv-master-01}=1`（正好是各自名下行数）；warm 时两条都是 0 ✅ 这就是 D3 P0-1① |
| `roster_stale` 覆盖 discovery 全集 | 同上 | 冷窗口 `{两台}=1`，warm `{两台}=0` ✅ |
| `untracked`/`ghost`/`stale_copy` 被陈 roster 门控（P1-3）| 同上 | 冷窗口里这三条**仍在且为 0**，不是消失也不是爆炸 ✅ |
| 冷 bindings 不许答 404 | 冷窗口 + 15s warmup 未到 | `0000…0000` ⇒ `Unavailable "scheduler is still seeding sandbox assignments"`；同一 id 在 warm 后 ⇒ `NotFound` ✅ 两个答案分得开 |
| `enabled` 分开 "feature off" 与 "从没成功过" | DSN 置空 | `enabled 0` vs `enabled 1` ✅（P2-10）|
| `/registry/sandboxes` 分开"没配"与"读不到" | DSN 置空 / PG 挂 | **501** vs **503** ✅ |

---

## 5. 取样受阻的两条：根因

### 5.1 🔴 paused 沙箱**永远留在 heartbeat roster 里**，登记表分支被 roster 短路

**实测**：worker-01 上建沙箱 → pause → 等 45s（`binding_ttl=30s`）→ resume ⇒
`lookup_node_total{result="bound_binding"}` +1，`placed`/`pinned`/`bound_registry` 全 0。

直连 gRPC 复核（三台沙箱，warm scheduler）：

```
id=01a019d1…（paused）      OK node=aenv-master-01  location=SANDBOX_LOCATION_BOUND  origin=
id=01a019cb…（running）     OK node=aenv-worker-01  location=SANDBOX_LOCATION_BOUND  origin=
id=01a01853…（local_only）  OK node=aenv-worker-01  location=SANDBOX_LOCATION_BOUND  origin=
```
`origin=""` ⇒ 都是 binding/roster 答的，登记表一次都没被读。

**根因**（读 Rust 确认，未改）：heartbeat 的 `sandbox_ids` 来自
`observability/service.rs:82 → orchestrator.list_sandbox_ids() → store.list_ids()`，
而 store 里**包含 Paused 沙箱**（`Orchestrator::new` 启动时把持久化的 Paused 也 restore 回 store）。
所以只要 origin 节点还在心跳，它的 roster 就一直报着这台 paused 沙箱，
binding 被 heartbeat 无限续期 ⇒ `lookupNode` 第 1 步（binding）就命中。

⇒ **验证计划 V1-3 第 2 步「等 binding TTL 过期，确认 LookupNode 会走登记表回落」前提不成立。**
登记表分支只在三种情况下被走到：**scheduler 冷启动**（binding + roster 都空）／
**origin 节点停止心跳超 90s**（3×`report_ttl`）／**origin 从 discovery 里消失**。

> 这**不一定是缺陷** —— 那三种正是这个 feature 要救的场景（尤其是 scheduler 重启／多副本新起）。
> 但它意味着"日常 pause/resume"根本不经过新代码。评估合并风险时值得知道：
> **本轮改动对稳态流量是零影响的**，只在控制面自己刚重启或节点失联时才生效。

**我用来打开取样窗口的办法（可复现）**：

```bash
kubectl -n agentenv-system scale deploy/agentenv-scheduler --replicas=0
sleep 160          # 让两个 node 的 heartbeat 退避涨到 60s（MAX_REPORT_BACKOFF）
kubectl -n agentenv-system scale deploy/agentenv-scheduler --replicas=1
# pod Ready 后立刻 port-forward 9090 直连 gRPC 打 LookupNode
# 窗口 ≈ 20~25s（到第一条 heartbeat 落地为止）
```
⚠️ 经 gateway 打这个窗口会被 **gateway 自己的 gRPC 重连退避**吃掉（实测一次 ~30s，窗口全没了）；
要么直连 scheduler，要么在 scheduler Ready 之后立刻重启 gateway Pod 拿一条新连接（我两种都用过）。

### 5.2 由 5.1 导致的死锁：DRAINING 对照组取不到

V1-3 第 4 步 / V1-4 都要求「**登记表分支被走到** 且 **scheduler 知道 origin 是 DRAINING**」。
但 DRAINING 状态和 roster **是同一条 heartbeat 带来的**：

- heartbeat 还没到 ⇒ 登记表分支走得到，但 `FilterUnschedulable` 对 `Snapshot == nil` 的节点
  **fail-open**（`service.go:213-221`），scheduler 不知道 origin 在 draining ⇒ 照样选中 origin
- heartbeat 到了 ⇒ 知道 draining 了，但 roster 同时命中（`rosterHolder` 只看 roster 新鲜度，
  **不看可调度性**，`lookup.go:338-356`）⇒ 第 2 步就 return 了

无论怎么排时序都取不到「知道 draining + roster 未命中」这个组合。我逐条排除过：
换节点（对称）、只让一台先心跳（origin 那台要么没报要么报了 roster）、
杀 node Pod（`terminationGracePeriodSeconds=3600`，且违反纪律 4）、
让沙箱超时被驱逐（`local_only` 那行 `sandbox_expires_at` 已过期 6 小时仍在 store 里，不会掉）。

**唯一可行解**：往 `paused_sandboxes` 插两行**不在任何节点 roster 里**的合成行
（`origin_node_id` 用真节点名、`sandbox_id` 用不存在的 UUID），验完删掉。
副作用为零（V1-4 期望的 503 发生在转发之前，请求根本不出 gateway；
Rust 侧 `reclaim_expired_holdings` / `release_node_holdings` 的 WHERE 都限定 `state IN ('running','resuming')`，
`claim_for_resume` 只在有人 resume 该 id 时触发）。
**已两次报请裁决，截至交付未获答复，故未执行。**

同一批合成行还能一并补上 §2 V0-3 里 `parked_lease_expiring` 那半边缺失的分辨力。

---

## 6. 发现的缺陷 / 风险

按严重度排。**没有一条是我判定为阻塞合并的**。

### F1 🟡 `/registry/sandboxes` 与 `/nodes` 一样**完全不鉴权**

```
$ curl -o /dev/null -w '%{http_code}\n' http://10.10.10.203:30800/registry/sandboxes    # 不带任何 key
200
$ curl -o /dev/null -w '%{http_code}\n' http://10.10.10.203:30800/nodes                 # 对照
200
$ curl -o /dev/null -w '%{http_code}\n' http://10.10.10.203:30800/sandboxes             # 对照
401
```

实施计划 §2.6 写的是「走现有 API key 中间件，与 `/nodes` 同级」。**"与 /nodes 同级"做到了，
"走 API key 中间件"没做到** —— 因为 `/nodes` 本身就不走。这是 gateway 既有行为（不是本轮引入），
但新端点把**全集群沙箱 ID + 节点归属 + 租约时间**加进了这个无鉴权面。

复现：上面三条 curl。
建议：合并可以，但上生产前要么给这一组补鉴权，要么确认 gateway 只在内网可达。

### F2 🟡 `state` / `nextToken` 传垃圾值静默返回空列表，不是 400

```
$ curl '…/registry/sandboxes?state=bogus'      ⇒ {"sandboxes":[],…}  HTTP 200
$ curl '…/registry/sandboxes?nextToken=zzz'    ⇒ {"sandboxes":[],…}  HTTP 200
```
`limit=-1` 是好的（400）。`state` 打错一个字母会读成"没有这种行"，运维排错时容易被带偏。
（`nextToken` 那条可以争辩是对的 —— 游标排在所有 UUID 之后，空是正确答案。）

### F3 🟡 `nodeID` 查询参数大小写敏感且不校验未知参数

`?nodeId=`（小写 d）被静默忽略、返回全量。我差点把"过滤没生效"记成 PASS。
同 F2，属于"错误输入读成合法结果"这一类。

### F4 🟢 `origin_unschedulable` 的错误文案把两种原因说成同一句

`schedulableNode` 失败有两个原因：**没有新鲜 roster**（`liveNode` 失败）和 **DRAINING**
（`FilterUnschedulable` 过滤掉）。两者共用同一句
`sandbox is %s on node %q, which is not accepting work`。

实测那 7 次 503 的真实原因是"节点没心跳"，但文案说的是"不接单"。
排障时会把人引到 admin API 去看 DRAINING 状态，而那里是正常的。
建议在日志（不必在 HTTP body）里分开这两个原因。

### F5 🟢 `registry_reconcile_duration_seconds` 把**失败轮**也计进直方图

V0-5 实测：3 轮读失败，`_count` 从 19 涨到 22。
Help 写的是 "Duration of one reconciliation round"，但失败轮的"耗时"其实是"失败前的耗时"，
混在成功轮的分布里会让 p99 失真。（D3 §5.4 已自己记了一条相邻的问题：只量了读、没量派生。）

### F6 🟢 `local_only` 沙箱在 scheduler 重启后有 ~5s 窗口拿 503

冷窗口实测：`01a01853`（`local_only`）在 scheduler 刚起、还没收到 heartbeat 时，
被 `origin_unschedulable` 拒成 503，尽管它的 origin 节点完全健康。
这是 `schedulableNode` 刻意的 fail-closed（代码注释写明了），且 503 可重试，**不是缺陷**，
但要知道：**scheduler 每次重启，`local_only` 沙箱有一个几秒的不可恢复窗口**。
对比之下 `paused` 沙箱在同一窗口是 fail-open 的（照样 PLACED 成功）。两条路一开一闭是设计选择。

### F7 🟢 gateway 自身 metrics（`:9102`）够不到 —— 既没进 Service，也没声明 containerPort

本轮给 gateway 新增了 `agentenv_gateway_sandbox_location_total`（我用它做了 V1-4 的关键取证），
但 `:9102` 这个 listener 一直是**既有**的、且从来没暴露过：

```
svc/agentenv-gateway            ports = [{http 8080}]
deploy/agentenv-gateway         containerPorts = [{http 8080}]        ← 连 9102 都没声明
```
跟 R3 B7 说 scheduler 的情况完全一样。**scheduler 那条本轮补了（Service + containerPort），gateway 这条没补。**
要看 gateway 指标只能 `kubectl port-forward deploy/agentenv-gateway 9102:9102`（不声明 containerPort 也能转，
但 Prometheus 抓不到）。属于既有问题被本轮扩大了价值，不是本轮引入。

---

## 7. 我改动过的集群状态 + 复原情况

| # | 改动 | 性质 | 现状 |
|---|---|---|---|
| 1 | `deploy/agentenv-scheduler` image → `10.10.10.204:5000/agentenv-scheduler:cp0-7e6f790` | **本轮发布，刻意保留** | 在跑 |
| 2 | `deploy/agentenv-gateway` image → `…/agentenv-gateway:cp0-7e6f790` | **本轮发布，刻意保留** | 在跑 |
| 3 | scheduler Deployment 新增 env `SCHEDULER_REGISTRY_DSN` / `SCHEDULER_REGISTRY_CLUSTER_ID`（均 secretKeyRef + optional）| **本轮发布，刻意保留** | 已确认 spec 与 7e6f790 的 base 清单一致 |
| 4 | scheduler 容器新增 `metrics` 端口 9101 | **本轮发布，刻意保留** | 在 |
| 5 | scheduler Service 新增 `metrics` 端口 9101 | **本轮发布，刻意保留** | 在 |
| 6 | `sts/agentenv-postgres` scale 0 → 1（V0-5）| 临时 | ✅ 已复原（`--replicas=1`，Running，数据在 PVC 上没丢）|
| 7 | `deploy/agentenv-scheduler` scale 0/1 共 4 次（撑冷窗口）| 临时 | ✅ 已复原（replicas=1）|
| 8 | `deploy/agentenv-gateway` 删 Pod 1 次（拿新 gRPC 连接）| 临时 | ✅ 已复原（新 Pod Running）|
| 9 | scheduler env DSN 临时置空（回滚验证）| 临时 | ✅ 已改回 secretKeyRef 并复验 |
| 10 | 建了 2 台沙箱 `01a019cb-…`（worker）/ `01a019d1-…`（master）| 临时 | ✅ 已 DELETE，`GET /sandboxes` ⇒ `[]` |
| 11 | 上述 2 台在 `paused_sandboxes` 留下的行 | 临时 | ✅ 随删除自动清掉，表回到 1 行 |
| 12 | 十余次短暂 `kubectl port-forward`（scheduler 9101/9090、gateway 9102）| 无状态 | 全部已 kill |

**未做的事**（明确登记）：未跑 `make k8s-apply`；未改任何 ConfigMap；未碰 node DaemonSet；
未重建 Rust 镜像；**未对 `paused_sandboxes` 做过任何 INSERT/UPDATE/DELETE**；未用过 `:latest` tag。

**交付时的集群状态**（与我接手前一致）：

```
health 204 | aenv-worker-01 ready running=0 paused=1 | aenv-master-01 ready running=0 paused=0
GET /sandboxes ⇒ []
paused_sandboxes ⇒ 1 行（01a01853-…，local_only，origin aenv-worker-01，snapshot NULL —— 接手前就有的那行）
registry_enabled=1，对账循环在跑
```

备份文件仍在构建/验证机本机 `/tmp/claude-1000/aenv-verify/`：
`bak-scheduler-deploy.yaml` / `bak-gateway-deploy.yaml` / `bak-scheduler-svc.yaml` / `bak-configmaps.yaml`
（后者是我额外多备的，全 namespace 的 ConfigMap，防 §3.2 那三条回退）。

---

## 8. 能不能合并

**能，建议合并。** 判据：

1. **阶段 0 全部通过，且最要紧的那条不变式是在真集群上用同进程读失败验的**（§4.1），
   不是"看起来是 0 所以对"。`enabled` / `read_failures` / `last_success` 三件套让
   「关了」「坏了」「一切正常且真的是 0」在监控上彻底分得开。
2. **要修的那个 bug 的镜像面已经证实翻过来了**：同一条路径，registry 可读 ⇒ 404，
   registry 不可读 ⇒ 503（§3.1）；冷 bindings ⇒ 503 而不是 404（§4.3）。
   这是本轮的核心承诺，做到了。
3. **origin 亲和性在真集群上生效**，且对照探针排除了所有我能想到的替代解释（§4.2）。
4. **gateway 的三段补丁真删了**，且删掉之后不存在的沙箱的 404 由 gateway 自己给出，
   不再有"挑一台机器转发过去让它回 404"这一跳（§3.5 + §3.2）。
5. **正常链路两节点各跑一遍全绿，数据面可用**（§3.6）。
6. **feature 开关干净**：DSN 置空 ⇒ 完全回到今天的行为，且 501/503 可区分（§3 额外一条）。
   ⇒ 万一线上出问题，回滚成本是改一个 env。
7. 发现的 7 条问题里，最重的 F1 是**既有行为的暴露面扩大**而不是本轮引入的错误逻辑，
   其余都是文案/校验/观测口径。

**合并前建议顺手做的（都不大）**：F2 + F3（`state`/`nodeID` 参数校验，一起改）、F4（日志分开两种原因）。
**上生产前必须先有结论的**：F1（无鉴权暴露面）。

**BLOCKED 的两条（V1-3 第 4 步、V1-4 的 DRAINING 触发）我的看法**：
它们测的是 `place(prefer)` 和 `schedulableNode` 在 origin 不可调度时的行为。
`schedulableNode` 这一半我已经用"origin 无新鲜 roster"跑到了同一个分支、同一句错误、同一个计数器；
`place(prefer)` 那一半只有单测覆盖（D3 §3.2 LOOKUP-3/LOOKUP-5 两个变异都被挡住）。
**我不认为它们该阻塞合并**，但建议在批准合成行之后补跑一次，
因为它们是"跨节点恢复"这条路上唯一没被真集群验过的判定。

---

## 附：本次用到的两个可复用工具

1. **冷 scheduler 窗口**（§5.1 末尾的四行命令）—— 目前**唯一**能让登记表分支被走到的办法。
2. **直连 gRPC 的 LookupNode 探针** —— 绕开 gateway 的重连退避，且能直接读到
   `location` / `origin_node_id` 两个新字段。源码 20 行，放在 `services/` 下用
   `GOWORK=off go build` 即可：

   ```go
   resp, err := cli.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: id})
   // 打印 resp.GetNode().GetNodeId() / resp.GetLocation() / resp.GetOriginNodeId()
   // 出错时打印 status.FromError(err) 的 Code() 与 Message()
   ```
   （注意 `Node` 的 getter 是 `GetNodeId()` 不是 `GetId()`。）
