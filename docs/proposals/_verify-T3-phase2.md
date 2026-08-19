# T3 集群验证报告：阶段 2（PG 写权上收）—— pve-sg dev，203/204

> 2026-08-19 · 分支 `central-control-plane-phase2`，HEAD `40a4526`（三个提交 `4a5e3ef` / `339d1f2` / `40a4526`）
> 上位：[`_impl-plan-control-plane-phase2.md`](_impl-plan-control-plane-phase2.md)（任务书）
> 前序：[`_verify-T1-results.md`](_verify-T1-results.md)（阶段 0/1）、[`_verify-T2-final.md`](_verify-T2-final.md)（六条修复 + 合成行方法）
> 实现记录：[`_impl-D6-scheduler.md`](_impl-D6-scheduler.md)（Go）、[`_impl-D8-node-rust.md`](_impl-D8-node-rust.md)（Rust）

---

## 0. 一页速览

| 阶段 | 条目 | 结论 |
|---|---|---|
| A | A1 基线取样 | ✅ 完成 |
| A | **A2 schema 交接硬门禁** | ✅ **PASS**，且带一条有分辨力的对照探针（见 §2 A2） |
| B | B1 node 新镜像 + `backend=central` | ✅ PASS（imageID 对账） |
| B | B2 两节点同时切 | ✅ PASS（无活沙箱窗口，5s 内两 Pod 全换） |
| B | **B3 node 不再连 PG** | ✅ **PASS**，最硬的一条：切前两节点各 2 条 PG 连接，切后节点 IP 从 `pg_stat_activity` 完全消失，**而登记表仍在被续租** |
| B | B4 正常链路（两节点各一遍，真发布快照） | ✅ PASS |
| B | B5 跨节点 resume | ✅ PASS，`origin` 从 worker 翻到 master，且 worker 侧自行丢弃了本地副本 |
| C | **C1 §3.1 全有或全无** | ✅ **PASS**（两种故障模式各验一遍：scheduler 没了 / scheduler 在但 PG 没了） |
| C | **C2 §3.2 grace 期** | ✅ **PASS**，做成了 A/B/A（serving 抢得到 → grace 抢不到 → serving 又抢得到） |
| C | **C3 §3.5 收窄版** | ✅ **PASS**，两条路径同进程同故障同节点，只差"有没有登记过" |
| C | C4 §3.3 丢弃熔断 | ✅ **PASS**（造出来了：ratio 臂跳闸 + 阈值以下同一机制真的删；rows 臂未在集群上取样） |
| D | D1/D2 回退 | ✅ PASS，**13 秒**，零数据损失 |

**能不能合并：能。** 判据与保留意见见 §8。

发现 6 条问题，**没有一条是本轮引入的错误逻辑**：1 条部署缺口（🟡）、1 条可观测性缺口（🟡）、
3 条指标口径（🟢）、1 条既有语义（🟢）。见 §7。

---

## 1. 发布记录

### 1.1 构建

```
构建机 10.10.10.204:/opt/AgentENV
  sudo git -c safe.directory=/opt/AgentENV fetch fork central-control-plane-phase2
  sudo git -c safe.directory=/opt/AgentENV checkout --detach FETCH_HEAD
  HEAD = 40a45266114106bbd449ca304656e9ded4a45cff
  tracked 文件 dirty 数 = 0（只剩 12 个既有 untracked：*.prealign / build-*.log / .cargo-test/）

TAG = cp2-40a4526     （不可变 tag，全程未用 :latest）
  docker build -f deploy/docker/Dockerfile.scheduler -t 10.10.10.204:5000/agentenv-scheduler:$TAG .   rc=0
  docker build -f deploy/docker/Dockerfile.agentenv  -t 10.10.10.204:5000/agentenv-runtime:$TAG   .   rc=0   （Rust，~7min）
  docker push …/agentenv-scheduler:$TAG   digest sha256:8159f51737435baf4365838d24914930eb589c83ce372b6d2965ff9a9c14795e
  docker push …/agentenv-runtime:$TAG     digest sha256:d19f6ef9386a15058c5045ae43911b3126a3b771901d993e1c84392d200698e4
```

**gateway 未重建**，保持 `cp1-c35f5ec`。理由：`git diff --name-only c35f5ec..40a4526 | grep services/gateway`
零命中，改动只落在 `services/scheduler` / `services/shared/config` / `services/api/proto`。
（`shared/config` 与 `api/proto` 也链进 gateway 二进制，所以严格说重建后字节会变，
但没有任何 gateway 行为面被改；本轮全链路是拿旧 gateway 跑通的。）

### 1.2 发布

只用 `kubectl patch` / `set image` / `scale` / `delete pod`，**没有 apply 任何清单，没跑 `make k8s-apply`**。

```bash
# 备份（本机 /home/debian/.claude/jobs/ec286011/tmp/t3/）
bak-node-ds.yaml / bak-scheduler-deploy.yaml / bak-gateway-deploy.yaml
bak-configmaps.yaml / bak-svcs.yaml / bak-secret-agentenv-postgres.yaml

# 1) 🔴 写面需要 cluster 作用域：base 清单引用的 Secret key 在集群里不存在（见 §7 F1）
kubectl -n agentenv-system patch secret agentenv-postgres --type=json \
  -p '[{"op":"add","path":"/data/cluster_id","value":"<base64 of 00000000-0000-0000-0000-000000000000>"}]'

# 2) scheduler 打开写面 + 换镜像
kubectl -n agentenv-system patch deploy agentenv-scheduler --type=json -p '[
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"SCHEDULER_REGISTRY_WRITE_ENABLED","value":"true"}}]'
kubectl -n agentenv-system set image deploy/agentenv-scheduler \
  scheduler=10.10.10.204:5000/agentenv-scheduler:cp2-40a4526

# 3) node 换镜像 + 切 backend（同一个 patch，随后两个 Pod 一起删 —— 见 B2）
kubectl -n agentenv-system patch ds agentenv-node --type=json -p '[
  {"op":"replace","path":"/spec/template/spec/containers/0/image",
   "value":"10.10.10.204:5000/agentenv-runtime:cp2-40a4526"},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"AENV_PAUSED_REGISTRY_BACKEND","value":"central"}}]'
kubectl -n agentenv-system get pods -l app.kubernetes.io/name=agentenv-node -o name \
  | xargs -P4 -I{} kubectl -n agentenv-system delete {} --wait=false
```

**imageID 对账（比 digest，不是只看 image 名）：**

```
agentenv-scheduler-64bbc868cc-7t9cs  …/agentenv-scheduler@sha256:8159f517…  == push digest ✅
agentenv-node-7c92t (master-01)      …/agentenv-runtime@sha256:d19f6ef9…    == push digest ✅
agentenv-node-h4mnq (worker-01)      …/agentenv-runtime@sha256:d19f6ef9…    == push digest ✅
```

> ⚠️ 与 base 清单的两处形状差异（都是刻意的，见 §7 F2）：
> - base 用 `configMapKeyRef{paused-registry-config, AENV_PAUSED_REGISTRY_BACKEND, optional}`，
>   我用了字面值 env —— 任务书要求"不要动 ConfigMap"，而集群里根本没有 `paused-registry-config` 这个 ConfigMap。
> - base 用 `secretKeyRef{agentenv-runtime-secrets, paused-registry-dsn}` 拿 DSN，
>   集群实际用的是 `secretKeyRef{agentenv-postgres, dsn}`。**我没动它** —— 留着它反而让 B3 的否定结论更硬：
>   DSN 还在环境里、连接却一条都没有。

---

## 2. 阶段 A：切换前

### A1 基线 —— ✅ 完成

```
health 204 | aenv-worker-01 ready sandboxes=0 | aenv-master-01 ready sandboxes=0
GET /sandboxes ⇒ []

paused_sandboxes：1 行
 01a01853-30a6-75a3-8b25-eb87b3db0265 | cluster 0000…0000 | local_only | gen 3
   origin aenv-worker-01 | claimed NULL | snapshot NULL | 租约在被续（updated_at 每 30s 前进）
 ⇒ 与 T1/T2 交付时逐字一致（接手前就有的那行）

各 state 分布：local_only=1，其余四态=0

scheduler 指标（cp1-c35f5ec，写面尚不存在）：
  registry_enabled 1
  rows{local_only}=1  {paused|publishing|resuming|running}=0
  stranded_rows=1  parked_lease_expiring=0  invalid_rows=0
  live_lease_lapsed=0  holder_conflict=0  reclaimable_now=0
  untracked/ghost/stale_copy/rows_without_roster{两节点}=0
  read_failures_total=0  reconcile_duration_seconds_count=472
  lookup_node_total{bound_binding}=5 {not_found}=6 {origin_not_reporting}=12

node → PG 连接（B3 的基线，关键）：
  10.42.0.55  (master node Pod) = 2
  10.42.1.164 (worker node Pod) = 2
  10.42.1.162 (scheduler Pod)   = 2
```

全量指标存在 `/home/debian/.claude/jobs/ec286011/tmp/t3/A1-metrics-baseline.txt`（543 行）。

### A2 🔴 schema 交接硬门禁 —— ✅ **PASS**

任务书 §5.1 的那条最危险路径：controller 一旦改了 CHECK，下一台跑 `postgres` 后端的 node
启动时无条件 `ADD CONSTRAINT` 会失败，**而根因在另一个进程里**。

**第一步：静态对账（先做，因为它比任何运行时观察都强）**

```python
# 抽出 Go 的 SchemaDDL 与 Rust 的 SCHEMA_DDL 逐字节比
GO len 1087   RS len 1087   IDENTICAL: True
```
advisory lock key 两侧也一致：`0x0A6E_7653_4348_4D41`
（`migrate.go:69` vs `postgres.rs:154`）。⇒ 任务书 §5.1 的"逐字复制、零新列"**成立**。

**第二步：controller 先跑 Migrate**

```
{"msg":"scheduler paused registry enabled","cluster_id":"00000000-…-000000000000","max_connections":4,…}
{"msg":"scheduler paused registry write surface enabled","cluster_id":"00000000-…","lease_ttl":90,
 "lease_ttl_floor":30,"reclaim_interval":30}
{"msg":"paused registry write surface entering its restart grace period",
 "inferred_downtime":20.106494,"leases_extended":1,"grace":90}
{"msg":"paused registry write surface open","grace_until":"2026-08-19T17:00:52.247Z"}
```

migrate 前后 `\d+ paused_sandboxes` + `pg_constraint` + `pg_indexes` 三份 dump **diff 为空**。

**第三步（门禁本体）：两台 node 仍跑 `postgres` 后端，逐台重启**

```
17:00:14 delete pod agentenv-node-gspxc (worker-01) → 17:00:32 新 Pod Ready
  …paused_registry::postgres: paused sandbox registry ready cluster_id=0000…0000 lease_ttl_secs=90.0
17:00:57 delete pod agentenv-node-stcmb (master-01) → 17:01:05 新 Pod Ready
  …paused_registry::postgres: paused sandbox registry ready cluster_id=0000…0000 lease_ttl_secs=90.0
/nodes ⇒ 两台 ready，CHECK 约束逐字未变
```

**🔴 对照探针（这一条才让上面的"起来了"有意义）**

"node 起来了"本身证明不了 node 真的跑过 DDL —— 也许它根本没碰 schema。
所以我先把 DDL 里的一个索引删掉，再重启 node：

```sql
DROP INDEX paused_sandboxes_updated_at_idx;     -- 确认 pg_indexes 里为 0
```
```
delete pod agentenv-node-52xfv → Ready
SELECT indexname FROM pg_indexes WHERE tablename='paused_sandboxes';
  paused_sandboxes_origin_node_idx
  paused_sandboxes_pkey
  paused_sandboxes_updated_at_idx      ← 回来了
```

⇒ **node 确实在每次启动重新宣告整套 schema**（`CREATE INDEX IF NOT EXISTS` 那一句真的跑了），
这正是 §5.1 描述的机制；它这次没炸，**唯一的原因是两侧 DDL 逐字节相同**。
门禁过，后续才继续。

---

## 3. 阶段 B：切到 Central

### B1 node 新镜像 + `backend=central` —— ✅ PASS

env 走 `AENV_PAUSED_REGISTRY_BACKEND`，**没有动 ConfigMap**（集群 `agentenv-k8s-config`
里的 `[orchestrator.paused_registry] backend = "postgres"` 原样保留）。
confique 的 `.env()` 在 `.file()` 之前，env 优先级更高 —— 这条由后面 B3 的结果反证。

### B2 两节点同时切 —— ✅ PASS

`maxUnavailable:1 / maxSurge:0` 的 DaemonSet 默认是**一台一台滚**，会留下混跑窗口。
做法是 patch 完立刻并行删两个 Pod：

```
17:03:40 patch ds（image + env 一起）
17:03:41 两个 Pod 同时删
17:03:43 两个新 Pod 都已在 Running
17:03:48 两个都 Ready，restartCount=0
```
切换窗口 ≈ 5s，且 `GET /sandboxes ⇒ []`（无活沙箱窗口，符合 §5.2）。

### B3 🔴 node 侧不再连 PG —— ✅ **PASS（本报告最硬的一条）**

```
切换前：
 client_addr | count        10.42.0.55  = master node Pod
-------------+-------       10.42.1.164 = worker node Pod
 10.42.0.55  |     2        10.42.1.162 = scheduler Pod
 10.42.1.162 |     2
 10.42.1.164 |     2

切换后（新 Pod IP：master 10.42.0.56 / worker 10.42.1.165）：
 client_addr | count
-------------+-------
 10.42.1.162 |     3        ← 只剩 scheduler
```

**为什么这不是"节点静默回落到 local"**：同一时刻登记表那行仍在被续租 ——

```
 01a01853-… | local_only | lease_expires_at 17:05:44 | updated_at 17:04:14 | now() 17:04:18
```
`local` 后端**什么都不写**。写还在发生、而没有任何 node 持有 DB 连接 ⇒ 写只能是经 gRPC 打到
controller 的。scheduler 侧的 RPC 计数器同时给出正面证据：

```
agentenv_scheduler_registry_write_rpc_total{code="OK",rpc="GetSandboxes"}        2
agentenv_scheduler_registry_write_rpc_total{code="OK",rpc="ReleaseNodeHoldings"} 2
agentenv_scheduler_registry_write_rpc_total{code="OK",rpc="RenewNodeLease"}      2
```
（`ReleaseNodeHoldings` 各 1 次 = 两台节点启动时释放前任持有 —— Slice A1 那条路径也顺带验到了。）

**G8 兑现**：PG 常驻连接从 3 个来源掉到 1 个；节点侧恒为 0，不随机器数增长。

### B4 正常链路 —— ✅ PASS（两节点各一遍）

先在切换**之前**跑了一遍 postgres 后端的对照（同模板、同参数）：
`create → pause(paused+snapshot) → resume(running, gen 4) → delete(行消失)`，全绿。

切到 Central 之后（`"timeout":1800` + `"autoResume":{"enabled":true}` 都带了）：

| 步骤 | master-01 | worker-01 |
|---|---|---|
| create | `01a01afb-8906-…` 201 | `01a01afb-a173-…` 201 |
| **pause（真发布快照）** | `paused` gen 1，`snapshot_id` 非空 | `paused` gen 1，`snapshot_id` 非空 |
| resume | `running` **gen 4** | `running` **gen 4** |
| delete | 行消失 | 行消失 |

gen 1→4 与 postgres 后端逐格一致（begin_pause=1 / complete_pause=2 / claim=3 / mark_running=4），
说明 5 个 RPC 覆盖的 6 个旧方法在真链路上映射对了。
node 日志给出发布确认：`paused sandbox is recoverable cluster-wide … snapshot_id=…`。

回退之后（阶段 D）又跑了第三遍，同样全绿 —— 三遍分别在 postgres / central / postgres 上。

### B5 跨节点 resume —— ✅ PASS

T1 §5.1 记过：paused 沙箱永远留在 origin 的 heartbeat roster 里，所以经 gateway 打 resume
一定被路由回 origin，登记表分支走不到。本轮换了个更直接也更强的取样方式：
**`kubectl port-forward` 直连非 origin 节点的 `:8000`，绕开 gateway 与 scheduler 路由**。
这恰好就是 B5 要验的那件事（非 origin 节点能不能认领并从共享仓库重建）。

先做对照，确认探针有分辨力：对不存在的沙箱打同一条路径 ⇒ `404 sandbox … not found`。

```
沙箱 01a01afb-a173-…，origin = aenv-worker-01，state = paused（snapshot 已发布）
POST http://<master-01 node Pod>:8000/sandboxes/01a01afb-a173-…/resume   ⇒ 201
```

```
切换前：state paused    gen 5  origin aenv-worker-01
切换后：state running   gen 7  origin aenv-master-01
```

- gen 5→7 = claim(6) + mark_running(7)。
- `origin_node_id` 被 `MarkRunning` 重指到 master ⇒ 认领 + 重建都真的发生了。
- **这条同时是 `metadata` 字节级往返的唯一真实取样**：跨节点重建走
  `restore_request(&entry.metadata, …)`，metadata 只经 `AcquireSandbox` 这一条 RPC 回来
  （任务书 §3.1）。它成功了 ⇒ R4 风险 3a（两侧没有共同真值来源）在真集群上没有兑现。
- worker-01 随后自己收场（下一轮对账）：
  ```
  discarded stranded local paused record sandbox_id=01a01afb-a173-…
  discarded superseded paused record … reason=node 'aenv-master-01' holds it now
  discarded paused records the cluster has moved past discarded=1
  ```
  **这条日志是 C1 的天然对照组**：同一个 reconcile 循环、同一台机器，
  registry 答得出来的时候它**会**删本地副本。

---

## 4. 阶段 C：三条护栏（本轮重点）

### C1 §3.1 全有或全无 —— ✅ **PASS（两种故障模式）**

先把代码里那条真正危险的分支钉清楚（`paused_recovery.rs:756`）：

```rust
let superseded = match rows.get(&sandbox_id) {
    None => Superseded::Gone,      // ← 行不在 = 集群不认识它了 = 删本地副本
```
⇒ **一个"空 map"就是一次删除**。所以"错误绝不能翻译成空"不是洁癖，是这条路径的全部安全边际。

#### 模式一：scheduler 整个没了（`scale --replicas=0`，17:09:42）

两台 node 每 30s 一轮，逐轮都停手；**两个 reconcile 循环都停**：

```
master-01:
 WARN registry unreadable; stopping paused-record  reconciliation
      error=paused sandbox registry backend failed during get_many:
            The service is currently unavailable: tcp connect error
 WARN registry unreadable; stopping running-sandbox reconciliation
      error=… during get_many: The service is currently unavailable: tcp connect error
worker-01:
 WARN registry unreadable; stopping paused-record reconciliation   （它没有已登记的 running 沙箱）
```

> 📝 实际日志文案是 `registry unreadable; stopping paused-record / running-sandbox reconciliation`，
> 与任务书里写的 `registry unreachable; stopping reconciliation` 不同（两句、"unreadable"）。
> 内容一致，登记一下免得下一棒 grep 不到。

**本地 artifacts 逐条对账（停机前 vs 停机后）：**

| 节点 | 停机前 | 停机 75s 后 |
|---|---|---|
| master-01 | `01a01afb-8906…`, `01a01afb-a173…`，173M | **完全一致**，173M |
| worker-01 | `01a01853-30a6…`，1.8G | **完全一致**，1.8G |

`discarded` 一次都没出现，master 上那台 running 沙箱也没被拆。
⇒ **分辨力来自 B5**：同一循环在能读到答案时删过一条（`discarded=1`），读不到时一条没删。

#### 模式二：scheduler 在，但它的 PG 没了（更阴险的那种）

`scale sts/agentenv-postgres --replicas=0` 之后重启 scheduler。进程正常起来、gRPC 照常监听
（路由 / 发现 / binding 不受影响），但写面进不去：

```
{"level":"error","msg":"paused registry write surface is not open; every registry request is
  refused until it is","error":"acquire connection for registry schema bootstrap: … connection
  refused","retry_in":1}      …retry_in 2 / 4 / 8 / 16（指数退避，不 Fatal）
GET /healthz ⇒ {"registry_write":{"phase":"cold","ready":false,"serving":false},"status":"ok"}   HTTP 200
agentenv_scheduler_registry_write_phase 0
agentenv_scheduler_registry_write_rpc_total{code="Unavailable",rpc="GetSandboxes"}   2
agentenv_scheduler_registry_write_rpc_total{code="Unavailable",rpc="RenewNodeLease"} 1
```

node 侧收到的是错误、不是空：

```
17:23:13（闸门还没关，PG 已经没了）
 WARN registry unreadable; stopping … error=… during get_many: The service is currently
      unavailable: registry get_many: failed to connect to `user=aenv database=aenv`…
17:23:43（重启后进 cold，闸门挡在 store 之前）
 WARN registry unreadable; stopping … error=… during get_many: The service is currently
      unavailable: paused registry is not ready
```
两句错误文案不同，说明**两道闸各自都在工作**（一道是 store 报错透传，一道是相位闸）。

**PG 拉回来之后：写面自己开了，scheduler 没有重启**（`restartCount=0`，`startTime` 不变）：

```
17:24:05 sts scale 1 → 17:24:11 PG Ready
17:24:15 {"msg":"paused registry write surface open","inferred_downtime":91.04,"leases_extended":3}
```
⇒ D6 §2.1 "migration + grace pass 后台带退避重试，不 Fatal"在集群上兑现。

### C2 §3.2 grace 期 —— ✅ **PASS（做成了 A/B/A）**

#### 停机推算与"加法而不是绝对形式"

```
17:09:42 scheduler → 0
17:12:54 scheduler → 1；表里 max(updated_at)=17:09:14.662
17:12:56 {"msg":"…entering its restart grace period","inferred_downtime":221.56304,
          "leases_extended":3,"grace":90}
```
221.563s = 17:12:56.227 − 17:09:14.662，**逐位吻合**。

租约算术逐行核对（以 `01a01853` 为例）：
```
延长前 lease = 17:10:44.662  (= updated_at 17:09:14.662 + 90s)
延长后 lease = 17:15:56.225  = 17:10:44.662 + 221.563 + 90     ✅ 加法
updated_at   = 17:09:14.662  ← **没被改写**（D6 说的"它正是这一趟读的证据"）
```

后面那次 107s 停机里还顺带验到了加法形式的第二个性质：一行**停机之前就已经死透**的租约
（我造的合成行，`lease = now() - 1h`）延长同样的量之后**仍然是过期的**：
```
16:16:11.15 (= 17:16:11.15 − 1h)  + 107.14 + 90  =  16:19:28.29   ⇒ still_expired = t
```
绝对形式 `now() + downtime + ttl` 会把它硬冻到未来，加法不会。**这条是设计取舍的现场证据。**

#### `/healthz` 区分 ready 与 serving —— ✅

```
cold    : {"phase":"cold",   "ready":false,"serving":false,"grace_remaining_seconds":0,     …}  HTTP 200
grace   : {"phase":"grace",  "ready":true, "serving":false,"grace_remaining_seconds":13.27,
           "inferred_downtime_seconds":221.56}                                                 HTTP 200
serving : {"phase":"serving","ready":true, "serving":true, "grace_remaining_seconds":0,     …}  HTTP 200
```
`write_phase` 对应 0 / 1 / 2。恒 200 是刻意的（D6 §2.2 的注释），验到了。

#### grace 期内拒绝认领 —— A/B/A

**🔴 我的第一发探针没有分辨力，这里如实登记。**
第一次我用的是一行 `local_only` + `snapshot_id IS NULL` 的合成行：grace 期内它被答
`409 sandbox snapshot is still being published by node 'aenv-ghost-01'`，看着像"被拒了"。
但 grace 结束后**同一发请求得到一模一样的答复**，行也没动。
读 SQL 才发现两条臂都带 `AND snapshot_id IS NOT NULL`（`store_postgres.go:577/601`）
—— 那行**任何时候都认领不了**，"被拒"与 grace 无关。**探针作废，重做。**

换成 `local_only` + **snapshot 非空** + 租约 1 小时前过期的行（`deadbeef-…0012`），三段：

| 时刻 | 相位 | resume（master-01 node API） | 登记表行 |
|---|---|---|---|
| 17:15:38 | **serving** | 409（后续二段错误，见下） | **`resuming` gen 2 claimed_by=aenv-master-01** ⇒ 抢到了 |
| 17:18:16 | **grace** | `409 sandbox snapshot is still being published by node 'aenv-ghost-01'` | **`local_only` gen 1 claimed NULL** ⇒ 原样没动 |
| 17:19:52 | **serving** | 409 | **`resuming` gen 2 claimed_by=aenv-master-01** ⇒ 又抢到了 |

⇒ 同一行、同一发请求、只差相位，**结果真的翻**。这是 §3.2 第 3 条的现场证据。

第一段 serving 抢到时，controller 侧还给出了 §3.4 要求的那条告警：
```
{"level":"warn","msg":"took over a sandbox parked on a node that stopped renewing its lease;
  restoring from the last snapshot that reached the repository, so any work since that snapshot
  is lost","sandbox_id":"deadbeef-…0012","node_id":"aenv-master-01",
  "claim_outcome":"rewound","previous_state":"local_only","previous_holder":"aenv-ghost-01"}
agentenv_scheduler_registry_write_claim_rewound_total 1
```
**`previous_state` = `local_only` 而不是 `resuming`** ⇒ 它真的来自 `previous` CTE 而不是
`RETURNING`（任务书 §3.3 点名的那个潜伏数月的 bug 的镜像面，在真集群上是对的）。

> ⚠️ 上表 HTTP 全是 409，不要拿它当判据。原因：我的合成行 `metadata` 是
> `{"synthetic":"…"}`，不是完整的 `SandboxMetadata`，节点拿到认领之后解不开，
> 于是走 `InvalidRecord` → `Proceed` → 本地没有副本 → `resolve_missing_local_resume`
> 等 5s 后据实回答"另一发 resume 正在做"。**判据是 PG 里那行动没动。**

#### `paused` 那一臂 grace 期不受影响 —— ✅

同一个 grace 窗口内，另一行合成 `paused`（snapshot 非空、租约同样过期）被**正常认领**：
```
deadbeef-…0011  paused gen 1  →  resuming gen 2  claimed_by aenv-master-01
```
⇒ D6 §2.2 "冻它只会让每次重启后所有普通跨节点 resume 白等一个租约"这条取舍成立。

#### reclaim grace 期不跑 —— ✅（见 C4，跳闸日志的时间戳都落在 serving 相位）

### C3 §3.5（新增护栏）—— ✅ **PASS，两条都验了**

这是收窄版的关键：**同一进程、同一次故障、同一台节点**，只差"这台沙箱有没有被登记到集群"。

| | 沙箱 | 本地记录 | scheduler | resume 结果 |
|---|---|---|---|---|
| **A 登记过** | `01a01afb-a173-…` | `ClusterRegistration::As(aenv-master-01)` | **DOWN** | **500** |
| **B 从未登记** | `01a01afb-8906-…` | `Never` | **DOWN** | **201（放行，本地拉起来了）** |

**A（17:11:19）**
```json
{"code":500,"message":"cannot determine whether the sandbox is live elsewhere:
  paused sandbox registry backend failed during claim_for_resume:
  The service is currently unavailable: tcp connect error"}
```
node 日志：
```
WARN could not reach the registry to arbitrate a resume for a sandbox this node announced to
     the cluster; refusing rather than risking a second live copy
```

**B 的构造**（这一步本身也是取证）：scheduler 已经 DOWN 的情况下，直连节点 API 把一台 running
沙箱 pause 掉 —— `begin_pause` 打不通，publish 整个放弃，于是本地记录的
`cluster_registered` 停在 false：
```
17:11:28  INFO  sandbox paused sandbox_id=01a01afb-8906-…
17:11:28  WARN  failed to register paused sandbox; it stays resumable on this node only
                error=… during begin_pause: The service is currently unavailable: tcp connect error
```
随后 17:11:39 对它 resume ⇒ **201**，沙箱在本地正常起来。

**metric 区分两条路径（D8 §6.3 的"一个 counter 两个标签值"）—— 实测正好各 1：**
```
agentenv_paused_registry_resume_unarbitrated_total{outcome="proceeded"} 1
agentenv_paused_registry_resume_unarbitrated_total{outcome="refused"}   1
agentenv_paused_registry_renew_consecutive_failures                     5   ← C6b，随失败轮递增
```

**第二种故障模式下重跑 A**（scheduler 在、PG 没了，17:23:57）：
```json
{"code":500,"message":"cannot determine whether the sandbox is live elsewhere: … during
  claim_for_resume: The service is currently unavailable: paused registry is not ready"}
```
⇒ 收窄版对"够不到 controller"与"controller 够不到库"两种都收口。

### C4 §3.3 丢弃熔断 —— ✅ **PASS（造出来了）**

`reclaimDiscardedSQL` 的谓词是
`snapshot_id IS NULL AND state IN ('running','resuming') AND lease 过期 AND sandbox_expires_at < now()`。
用 `origin_node_id = 'aenv-ghost-01'`（不存在的节点名）的合成行去撞它，对两台真节点完全惰性
（`release_node_holdings` / `renew_lease` / `get_many` 的入参与谓词都碰不到它们）。

**跳闸（只让 ratio 臂越线，rows 臂不越）**

插 2 行可丢弃，表内共 8 行 ⇒ 2 ≤ max_rows(10) 但 25% > max_ratio(0.10)：

```
{"level":"error","caller":"registry/grace.go:439","msg":"refusing to reclaim: this pass would
  discard more rows than anything here can explain, and a discarded row has no snapshot to come
  back from","candidates":2,"cluster_rows":8,"max_rows":10,"max_ratio":0.1}

agentenv_scheduler_registry_write_reclaim_breaker_tripped_total 2   （两个 tick 各一次）
agentenv_scheduler_registry_write_reclaim_discarded_total       0
count(*) 仍是 8，两行都还在
agentenv_scheduler_registry_reclaimable_now                     2   ← 阶段 0 的读侧口径也认同
```
⇒ **两条件"取严"成立**：rows 没越线，光 ratio 越线就够停手。

**对照组（阈值以下，同一机制真的会删）**

删掉一行、补 9 行惰性行 ⇒ 1 行可丢弃 / 共 16 行 = 6.25% < 10%，且 1 ≤ 10：

```
{"level":"warn","msg":"reclaimed sandboxes that outlived their deadline on a node that stopped
  reporting; released ones resume from their last published snapshot",
  "released":0,"discarded":1}
count(*) 16 → 15，那行没了
agentenv_scheduler_registry_write_reclaim_discarded_total       1
agentenv_scheduler_registry_write_reclaim_breaker_tripped_total 2   ← 没再涨
```
⇒ 熔断不是"永远拦着"，它是**按阈值**拦的。

**没做到的半条（明确登记）**：`max_rows` 臂**没在集群上单独取样** —— 要让 rows 越线而 ratio 不越线，
需要 11 行可丢弃 + 表内 ≥110 行，代价与风险都不划算，且这两个阈值**没有 env 覆盖**
（`config.go` 只给了 `WRITE_ENABLED` / `LEASE_TTL` / `LEASE_TTL_FLOOR` / `RECLAIM_INTERVAL` 四个）。
该臂由 D6 §3.2 的单测 + 变异覆盖。

---

## 5. 阶段 D：回退

### D1 —— ✅ PASS

```
17:27:27  kubectl patch ds …/env/12/value → "postgres"（带 op:test 断言 name，防索引漂移）
17:27:28  两个 Pod 并行删
17:27:40  两个新 Pod 都 Ready
```

```
两台 node 都打出： paused sandbox registry ready cluster_id=0000…0000 lease_ttl_secs=90.0
worker-01：       loaded paused sandbox records loaded=1 retained=1
PG 连接回来了：    10.42.0.57 = 2   10.42.1.170 = 2   10.42.1.168(scheduler) = 3
登记表：           01a01853-… local_only gen 3 origin aenv-worker-01，租约继续被续
```

回退后又跑了一遍完整链路（create → pause(`paused`+snapshot) → resume → delete），全绿。

**注意**：`backend` 是 `OnceLock`，**没有热加载** —— 回退必须重启 node，这条和 D8 §7.5 说的一致。
我用的是"改 env + 删 Pod"，DaemonSet 自己滚也可以，但那样会有一台一台的混跑窗口。

### D2 回退耗时与数据损失

| 项 | 值 |
|---|---|
| **回退耗时** | **13 秒**（patch 发出 → 两台 node 都 Ready） |
| 期间数据面 | 无活沙箱（我把两台测试沙箱都删了再回退），未取样"带活沙箱回退" |
| **数据损失** | **零**。`paused_sandboxes` 回到 1 行、gen 3、origin/state/snapshot 逐字未变；worker-01 的本地 paused 记录 `loaded=1 retained=1`；两节点 artifacts 目录清单与容量未变 |
| 回退窗口内的写 | 有 ~12s 空窗（两侧都不在写），期间租约不续。租约 TTL 90s ⇒ 空窗远小于 TTL，无行进入可接管状态 |

---

## 6. 集群改动登记表 + 复原情况

| # | 改动 | 性质 | 现状 |
|---|---|---|---|
| 1 | `deploy/agentenv-scheduler` image → `…/agentenv-scheduler:cp2-40a4526` | 本轮发布 | **见 §6.1** |
| 2 | scheduler Deployment 新增 env `SCHEDULER_REGISTRY_WRITE_ENABLED=true` | 本轮发布 | **见 §6.1** |
| 3 | Secret `agentenv-postgres` 新增 key `cluster_id`（全零 UUID） | 本轮发布 | **见 §6.1**；原 Secret 只有 `POSTGRES_PASSWORD` / `dsn` 两个 key |
| 4 | `ds/agentenv-node` image → `…/agentenv-runtime:cp2-40a4526` | 本轮发布 | **见 §6.1**；原值是 ctr 本地名 `agentenv-runtime:latest` |
| 5 | `ds/agentenv-node` 新增 env `AENV_PAUSED_REGISTRY_BACKEND` | 本轮发布 | **值已回退为 `postgres`**；env 项本身仍在 |
| 6 | node Pod 删除重建 **6 轮**（A2×3、B2、C 无、D1） | 临时 | ✅ 每轮都等到两台 Ready、`restartCount=0` |
| 7 | `deploy/agentenv-scheduler` scale 0/1 **2 次** + 删 Pod 1 次 | 临时 | ✅ 已复原（replicas=1，Running） |
| 8 | `sts/agentenv-postgres` scale 0 → 1 **1 次** | 临时 | ✅ 已复原（Running，PVC 数据没丢，`count(*)` 与实验前一致） |
| 9 | `paused_sandboxes` INSERT 合成行 **12 行**（`deadbeef-…`，经 T2 §3.0 裁决沿用） | 临时 | ✅ **已全部 DELETE**，`LIKE 'deadbeef%'` 为 0，表回到 1 行 |
| 10 | `DROP INDEX paused_sandboxes_updated_at_idx`（A2 对照探针） | 临时 | ✅ 已由 node 启动 DDL 自行重建，`pg_indexes` 三条齐 |
| 11 | UPDATE 合成行 `deadbeef-…0012` 复位 1 次（C2 A/B/A 之间） | 临时 | ✅ 该行已删 |
| 12 | 建了 4 台沙箱（1 台切换前对照 + 2 台 B4/B5/C + 1 台 D 冒烟） | 临时 | ✅ 全部 DELETE，`GET /sandboxes ⇒ []`，登记表行随删除清掉 |
| 13 | 十余次短暂 `kubectl port-forward`（scheduler 9101、node 8000） | 无状态 | ✅ 全部已 kill（每次都是同一条命令里 `kill $PF`） |

**未做的事**（明确登记）：未跑 `make k8s-apply`；**未改任何 ConfigMap**；未碰 gateway 镜像；
未用过 `:latest`；未用过 `--force --grace-period=0`；
**除第 9/10/11 项外未对 `paused_sandboxes` 做过任何写操作**（真沙箱产生的行是被测系统自己写的）。

备份文件在本机 `/home/debian/.claude/jobs/ec286011/tmp/t3/`：
`bak-node-ds.yaml` / `bak-scheduler-deploy.yaml` / `bak-gateway-deploy.yaml` /
`bak-svcs.yaml` / `bak-configmaps.yaml` / `bak-secret-agentenv-postgres.yaml`，
外加 `A1-metrics-baseline.txt` / `A2-schema-{before,after}.txt` / `FINAL-metrics.txt`。

### 6.1 交付时的集群状态

```
health 204 | aenv-worker-01 ready sandboxes=0 | aenv-master-01 ready sandboxes=0
GET /sandboxes ⇒ []
paused_sandboxes ⇒ 1 行（01a01853-…，local_only，gen 3，origin aenv-worker-01，snapshot NULL）
node backend = postgres（回退完成），两台各持 2 条 PG 连接
registry_enabled=1，读侧对账循环在跑，指标与 A1 基线一致
（read_failures_total=2 是 §3.1 模式二里我自己造的 PG 停机，属预期）
pods: gateway / scheduler / 2×node / postgres / rustfs / agent-console 全部 1/1 Running
```

**尚未复原的 4 项（第 1/2/3/4 条）**：镜像与写面开关是否保留、要不要连同 Secret key 一起撤，
已发消息请主 agent 裁决。**如果要全量复原到我接手前的样子**：

```bash
kubectl -n agentenv-system set image deploy/agentenv-scheduler \
  scheduler=10.10.10.204:5000/agentenv-scheduler:cp1-c35f5ec
kubectl -n agentenv-system patch deploy agentenv-scheduler --type=json \
  -p '[{"op":"remove","path":"/spec/template/spec/containers/0/env/2"}]'   # WRITE_ENABLED
kubectl -n agentenv-system patch secret agentenv-postgres --type=json \
  -p '[{"op":"remove","path":"/data/cluster_id"}]'
kubectl -n agentenv-system patch ds agentenv-node --type=json -p '[
  {"op":"replace","path":"/spec/template/spec/containers/0/image","value":"agentenv-runtime:latest"},
  {"op":"remove","path":"/spec/template/spec/containers/0/env/12"}]'       # BACKEND
```

> ⚠️ 保留现状（scheduler 写面开着 + node 在 postgres）**不是零成本**：
> controller 的 reclaim 定时器每 30s 跑一次、会 DELETE 行，node 侧的 reclaim 也在跑。
> 两侧谓词相同、都是单条条件写，互相不会写坏，但这是"未合并代码在无人值守下持有删除权"。

---

## 7. 发现的缺陷 / 风险

### F1 🟡 写面的 cluster 作用域**没有任何清单提供**，缺了会永久留冷且只有一条 error 日志

`deploy/k8s/base/scheduler-deployment.yaml` 用
`secretKeyRef{agentenv-postgres, cluster_id, optional: true}` 取 `SCHEDULER_REGISTRY_CLUSTER_ID`，
但**没有任何东西创建这个 key** —— 本集群的 Secret 原本只有 `POSTGRES_PASSWORD` / `dsn`
（T1 §1.2 已经记过"这个 key 在集群里不存在"，当时读面允许为空所以无害）。
阶段 2 之后后果变了（D6 §5.2 是刻意设计）：写面被开、cluster id 为空 ⇒ 相位永远 `PhaseCold`
⇒ **每个 RPC 都 UNAVAILABLE**，节点侧的表现就是"registry 永远够不到"。

现象与"scheduler 挂了"**完全一样**，而 scheduler 进程是健康的、`/healthz` 200、
grpc_health_probe 也通过。唯一的线索是启动时那一条 error 日志。

**这属于"全新集群 seed 缺口"那一类**（参见 `project_fresh_cluster_seed_gaps`）。
建议：把 `cluster_id` 写进创建 Secret 的地方，或让写面在 cluster id 为空时**拒绝启动**
（D6 §5.2 论证过不该 Fatal，那就至少把它挂进一个专门的 readiness 信号，别只留日志）。

### F2 🟡 `central` 后端**没有任何装配日志**，"切成功了"与"静默回落 local"从日志上分不开

`postgres` 后端启动时有：
```
INFO agentenv::orchestrator::paused_registry::postgres: paused sandbox registry ready
     cluster_id=… lease_ttl_secs=90.0
```
`central` 分支（`mod.rs:356-372`，`connect_lazy`）**一个字都不打**。本轮我不得不靠
`pg_stat_activity` + scheduler 侧 RPC 计数器**反推**它切成功了。

这正好抵消掉 `AENV_PAUSED_REGISTRY_BACKEND` 这个 env 存在的理由 ——
它是为了对抗"ConfigMap 被 apply 覆盖 ⇒ 静默回落 local ⇒ 不报错"，
结果 `central` 自己也没有正面确认信号。

建议：`Central` 分支加一条 `info!(endpoint, cluster_id, lease_ttl_secs, "paused sandbox registry
ready (central)")`。一行的事，且它是运维唯一能自证的东西。

### F3 🟢 `grace_takeovers_withheld_total` 统计的是"grace 期内服务的 claim 数"，不是"真被拦下的接管数"

`grace.go:279-287` 的 `allowsLeaseTakeover()` 在 grace 相位下**无条件 `Inc()` 后返回 false**。
实测：grace 期内 2 发 claim ⇒ 计数 +2，而其中一发是 `paused` 行（那条臂根本不看租约，
不存在"被withheld"这回事）。

后果：一次重启后如果有大量普通跨节点 resume（全是 `paused` 行），这个计数器会给出
"我拦下了 N 次接管"的读数，而真实影响是 0。指标名与语义不符会让运维高估 grace 的代价。
建议：把 `Inc()` 挪到实际因缺了那条臂而落空的路径上，或者改名成 `..._claims_without_takeover_arm_total`。

### F4 🟢 `grace_refusals_total` 把"客户端 RPC 被拒"和"controller 自己的 reclaim 定时器被闸住"混在一个计数器里

`Require()`（RPC 面）与 `RequireServing()`（reclaim）都 `Inc()` 同一个 counter。
实测逐位对得上：某个 pod 上 13 = 客户端 8（`Unavailable` 的 GetSandboxes 5 + RenewNodeLease 2 +
AcquireSandbox 1）+ reclaim 自己 5。

后果：一次**完全健康、零客户端流量**的 controller 重启也会稳定产出 2~3 次 refusal。
拿它做告警会有恒定底噪。建议加一个 `source="rpc"|"reclaim"` 标签。

### F5 🟢 健康重启时 `inferred_downtime` 会高估最多一个节点对账周期

`inferred_downtime = now() - max(updated_at)`，而节点每 30s 才写一次。
实测：一次 14s 的滚动重启报 `inferred_downtime=20.1s`。

方向是安全的（高估 ⇒ 租约多延一点），但日志 / metric / `/healthz` 上那个数**不是进程停机时长**，
容易被当成"控制面挂了 20 秒"。值得在字段注释或指标 HELP 里点一句。

### F6 🟢 `sandbox_expires_at` 只由 `renew_lease` 写；一台在首个续租周期内就失联的节点会留下永久孤儿行

两侧一致（`postgres.rs:651` / `store_postgres.go:872`），`begin_pause` 不写这一列。
而 controller 的 reclaim 两条语句都要求 `sandbox_expires_at < now()`，
`NULL` 永不匹配。⇒ 一行 `running` 如果在 origin 节点的第一个续租 tick（≤30s）之前就失去了它的
节点，`sandbox_expires_at` 永远是 NULL，reclaim 永远不碰它。

本轮顺手撞到过这个不对称（同批建的两台沙箱，一台有一台没有，差别只是续租 tick 的落点）。
**这是既有语义，不是本轮引入的**，也不阻塞合并；登记下来给阶段 3 的 fencing 方案。

---

## 8. 能不能合并

**能，建议合并。** 判据：

1. **最危险的那条路径（schema 交接）是在真集群上、带对照探针验的**（§2 A2）：
   先证明两侧 DDL 逐字节相同，再证明 node 真的每次启动重跑 DDL（删索引 → 重启 → 索引回来），
   最后两台 node 在 controller 迁移之后都正常起来。不是"看着没炸所以对"。
2. **G7/G8 是用否定证据兑现的**（§3 B3）：节点侧 PG 连接从 2+2 掉到 0，而**同一时刻登记表仍在被写**。
   这一条同时排除了"静默回落 local"这个最难分辨的失败模式。
3. **三条护栏都有反向对照，不是一笔带过**：
   - §3.1：同一个 reconcile 循环，能读到答案时删过一条（B5 `discarded=1`），读不到时两台节点
     两个循环一条不删、artifacts 逐字节清单不变；且"scheduler 没了"与"scheduler 在但库没了"
     两种模式各验一遍，后者 RPC 明确答 `Unavailable` 而不是空 map。
   - §3.2：同一行、同一发请求，**serving 抢得到 → grace 抢不到 → serving 又抢得到**；
     租约算术两处逐位吻合；`/healthz` 三相位分得开。
   - §3.5：同进程、同故障、同节点，只差"有没有登记过"⇒ 500 / 201，metric 各 1。
4. **§3.3 熔断造出来了**，且有阈值以下的对照组证明它不是"永远拦着"。
5. **正常链路在三种配置下各跑一遍全绿**（postgres → central → postgres），
   generation 序列逐格一致，跨节点 resume 把 `metadata` 的字节级往返也带过了。
6. **回退 13 秒、零数据损失**，且回退路径不依赖任何本轮新代码（只是把 env 改回去）。
7. 发现的 6 条里，**没有一条是本轮引入的错误逻辑**：F1 是部署缺口、F2 是可观测性缺口、
   F3/F4/F5 是指标口径、F6 是既有语义。

### 合并前建议顺手做的（都不大）

- **F2**（`central` 装配日志一行）—— 最值得做的一条，它是运维唯一的自证手段。
- **F1**（把 `cluster_id` 落进创建 Secret 的地方）—— 否则下一个全新集群会重演一次
  "registry 永远够不到、进程却健康"的排错。
- F3 / F4 的指标口径（加标签或改名）。

### 上生产前必须先有结论的

1. **scheduler 是 `replicas: 1`、无 PDB、无 `maxSurge`。** 本轮实测：它一停机，
   节点侧的续租、对账、pause 的 `begin_pause`、以及**所有已登记沙箱的 resume**（§3.5，按设计）
   全部停摆。阶段 2 把这个单点从"读侧降级"提升成了"写侧硬依赖"。
   护栏做对了它该做的事（失败可重试、不双活），但**故障窗口的宽度现在由 scheduler 的
   滚动升级策略决定**，这件事没有被任何清单约束住。
2. **F1 的部署缺口**在生产集群上会不会重演（生产的 `agentenv-postgres` Secret 有没有 `cluster_id`）。
3. `max_rows` 熔断臂只有单测覆盖，生产阈值（10 行 / 10%）相对于生产表的规模是否合理 ——
   本 dev 集群只有 1 行，ratio 臂在小表上极易误触发（本轮 2/8 就跳了）。
   生产上如果登记表长期只有个位数行，这个熔断会把**任何**一次正常回收都拦掉。

---

## 附：本轮新增的可复用工具

1. **直连节点 `:8000` 的 API 探针**（`node.sh`）—— 绕开 gateway 与 scheduler 路由，
   是本轮跨节点 resume（B5）与 registry 不可达场景（C1/C3）唯一可行的驱动方式：
   scheduler 一停，gateway 就路由不了，任何经 gateway 的取样都拿不到。
   鉴权是空壳（`auth.rs:31`：任意非空 `X-API-Key` 放行）。
   ```bash
   kubectl -n agentenv-system port-forward pod/<node-pod> 18000:8000
   curl -X POST http://127.0.0.1:18000/sandboxes/<id>/resume -H 'X-API-Key: dummy' \
        -H 'Content-Type: application/json' -d '{"timeout":1800}'
   ```
2. **合成行造熔断**：`origin_node_id='aenv-ghost-01'` + `sandbox_expires_at` 过去 + `snapshot_id IS NULL`
   + `state='running'|'resuming'` 精确命中 `reclaimDiscardedSQL`，对真节点完全惰性
   （`release_node_holdings` 要求 `origin`/`claimed_by` 匹配真实节点名）。
3. **`pg_stat_activity` 按 `client_addr` 分组**：判"某个 Pod 还连不连库"的最直接探针，
   自带基线（切换前的读数就是对照组）。
4. 🔴 **合成行做 claim 相关探针时必须带 `snapshot_id`** —— 两条认领臂都有
   `AND snapshot_id IS NOT NULL`，NULL 的行任何相位都认领不了，会把"探针没分辨力"
   伪装成"护栏生效"。本轮踩过一次，见 §4 C2。
