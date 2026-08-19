# 阶段 0 + 阶段 1 集群验证计划（pve-sg dev，203/204）

> 2026-08-19 · 主 agent 拟定。执行前先读 [`_recon-R3-cluster-runbook.md`](_recon-R3-cluster-runbook.md)
> §3.3（发布 runbook）与 §4（验证入口）。

---

## 0. 环境与纪律

- 集群：`10.10.10.203`（k3s master）+ `10.10.10.204`（worker + 构建机 + registry `:5000`）
- kubeconfig：`~/.kube/config-aenv-sg`，namespace `agentenv-system`
- gateway：`http://10.10.10.203:30800`（`/health` 返回 **204** 才正常，API key 任意非空）
- PG：`agentenv-postgres-0`（StatefulSet，ClusterIP `agentenv-postgres:5432`，库 `aenv`）

🔴 **三条纪律**
1. **只用 `kubectl set image` 发布**，绝不 `make k8s-apply` —— 后者会把 ConfigMap 里的
   `[orchestrator.paused_registry]` 整节删掉，PG registry 静默关闭且不报错（R3 §3.2）
2. **镜像用不可变 tag**，绝不 `:latest` —— 203 的 containerd 缓存着过期的 latest，会静默跑旧代码
3. 改 Deployment（本轮要加 env）只能用 `kubectl patch`/`edit` 精确改那一处，不要整份 apply

---

## 1. 发布

```bash
# 构建机对齐源码（本轮代码尚未 push，先把 services/ 与 deploy/ 同步过去，或先 push 分支再 fetch）
ssh supos@10.10.10.204
cd /opt/AgentENV && sudo git -c safe.directory=/opt/AgentENV fetch fork <branch> \
  && sudo git -c safe.directory=/opt/AgentENV checkout --detach FETCH_HEAD

TAG=cp0-$(sudo git -c safe.directory=/opt/AgentENV rev-parse --short HEAD)
sudo docker build -f deploy/docker/Dockerfile.scheduler -t 10.10.10.204:5000/agentenv-scheduler:$TAG .
sudo docker build -f deploy/docker/Dockerfile.gateway   -t 10.10.10.204:5000/agentenv-gateway:$TAG .
sudo docker push 10.10.10.204:5000/agentenv-scheduler:$TAG
sudo docker push 10.10.10.204:5000/agentenv-gateway:$TAG
```

```bash
export KUBECONFIG=~/.kube/config-aenv-sg
# 先备份两份可能被动到的对象
kubectl -n agentenv-system get deploy agentenv-scheduler -o yaml > /tmp/bak-scheduler-deploy.yaml
kubectl -n agentenv-system get svc    agentenv-scheduler -o yaml > /tmp/bak-scheduler-svc.yaml

# 注入 DSN env（阶段 0 需要）+ metrics 端口
kubectl -n agentenv-system patch deploy agentenv-scheduler --type=json -p '[...]'   # 见 §1.1
kubectl -n agentenv-system set image deploy/agentenv-scheduler scheduler=10.10.10.204:5000/agentenv-scheduler:$TAG
kubectl -n agentenv-system set image deploy/agentenv-gateway   gateway=10.10.10.204:5000/agentenv-gateway:$TAG
kubectl -n agentenv-system rollout status deploy/agentenv-scheduler --timeout=180s
kubectl -n agentenv-system rollout status deploy/agentenv-gateway   --timeout=180s
# 比 imageID 确认真换了，别只看 image 名
```

### 1.1 DSN env 从哪来
现成 Secret `agentenv-postgres` 的 `dsn` key（node DaemonSet 已在用同一份）。
patch 成 `secretKeyRef{name: agentenv-postgres, key: dsn, optional: true}`。

---

## 2. 阶段 0 验收

### V0-1 装配自证
```bash
kubectl -n agentenv-system logs deploy/agentenv-scheduler | grep -i "paused registry"
# 期望：scheduler paused registry enabled ... + 启动行 paused_registry=true
```

### V0-2 指标可达且齐全
```bash
kubectl -n agentenv-system port-forward deploy/agentenv-scheduler 9101:9101 &
curl -s localhost:9101/metrics | grep -E '^agentenv_scheduler_registry_'
```
期望看到全部：`registry_enabled` / `rows` / `untracked` / `ghost` / `stale_copy` /
`rows_without_roster` / `holder_conflict` / `parked_lease_expiring` / `stranded_rows` /
`live_lease_lapsed` / `reclaimable_now` / `roster_stale` / `invalid_rows` /
`read_failures_total` / `reconcile_duration_seconds` / `last_success_timestamp_seconds`

### V0-3 与 PG 真值对账
```bash
kubectl -n agentenv-system exec agentenv-postgres-0 -- \
  psql -U aenv -d aenv -c "SELECT state, count(*), count(snapshot_id) AS with_snapshot
                             FROM paused_sandboxes GROUP BY state;"
```
`registry_rows{state=...}` 必须与这张表逐格一致。

🔴 **重点核对 P1-4 那条**：集群里现存那行是 `local_only` 且 `snapshot_id IS NULL`
⇒ `parked_lease_expiring` 必须为 **0**（它谁也抢不走），`stranded_rows` 必须为 **1**。
若 `parked_lease_expiring=1`，说明 P1-4 没修对。

### V0-4 `invalid_rows` 恒为 0
不为 0 说明库里有 `paused` 但无 snapshot 的行 —— 那会让 Rust 侧 `get_many` 整批报错、
静默冻结一台机器的对账。这是本次对账第一次能看见它。

### V0-5 🔴 对照探针（否则"全 0"与"读不到"分不开）
把 DSN 改成一个必然连不上的地址，滚一次，确认：
- `registry_read_failures_total` **增长**
- 其它 gauge **保持上一轮的值不被清零**（方案 §3.1 的核心不变式）
- `LookupNode` 对 binding miss 的沙箱答 **503 而不是 404**（见 V1-1）
验完立刻改回。**不做这一步，V0-2/3 全过也证明不了任何东西。**

### V0-6 只读 API
```bash
curl -s -H 'X-API-Key: dummy' 'http://10.10.10.203:30800/registry/sandboxes' | jq .
```
期望：返回那一行，字段含 `holderNodeID` / `leaseExpiresAtUnixMs` / `sandboxExpiresAtUnixMs` /
`databaseTimeUnixMs`；`snapshotID` 为空串、两个租约字段按 NULL 渲染成 `null`（不是 0）。

---

## 3. 阶段 1 验收

前置：造样本。建沙箱**两个必传字段**（漏了不报错但会把结论带偏）：
`"timeout": <秒>`、`"autoResume":{"enabled":true}`。

### V1-1 🔴 registry 不可达 ⇒ 503 而不是 404（最重要的一条）
沿用 V0-5 的坏 DSN 状态，对一个**有登记表行但 binding 已过期**的沙箱发 resume：
```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'X-API-Key: dummy' \
  http://10.10.10.203:30800/sandboxes/<id>/resume
```
期望 **503**。得到 404 就是这一轮最该修的 bug 的镜像 —— 直接判定不通过。

### V1-2 不存在的沙箱 ⇒ 404
```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'X-API-Key: dummy' \
  http://10.10.10.203:30800/sandboxes/00000000-0000-0000-0000-000000000000/resume
```
期望 **404**（registry 可读且无行时才是权威答案）。

### V1-3 origin 亲和性生效（§1.2 的性能修复）
1. 在 node A 上建沙箱 → pause（等它 publish 成 `paused` 且 `snapshot_id` 非空）
2. 等 binding TTL（30s）过期，确认 `LookupNode` 会走登记表回落
3. resume，看 scheduler 日志：
```bash
kubectl -n agentenv-system logs deploy/agentenv-scheduler | grep "scheduler placed a paused sandbox"
# 期望 origin_preferred=true 且 node_id == origin_node_id
```
4. 对照：把 origin 隔离（admin API 置 DRAINING）后再 resume ⇒ 期望选中**另一台**，
   `origin_preferred=false`。**这一步是探针自证** —— 没有它，"总是选中 origin"也可能是
   因为只有一台可选。

### V1-4 `local_only` + origin DRAINING ⇒ 503 且不打到 origin
库里那行正好是 `local_only`（origin=`aenv-worker-01`）。把该节点置 DRAINING 后 resume：
- 期望 gateway **503**，body 说明 origin 不接单
- 期望 node Pod 日志里**没有**这次 resume 的记录（证明请求根本没转发过去）

### V1-5 补丁真的删了
```bash
kubectl -n agentenv-system logs deploy/agentenv-gateway | grep -i "reroute\|recovery"
```
期望：无 reroute 相关日志；`x-agentenv-reroute: schedule` 的 503 直通客户端。

### V1-6 正常链路不回归
建 / pause / resume / delete 各跑一轮，两节点各来一遍，确认与改前行为一致。
`aenv` CLI 在 203 上：`ssh supos@10.10.10.203 '~/.local/bin/aenv list'`（**要写全路径**）。

---

## 4. 回滚

```bash
kubectl -n agentenv-system rollout undo deploy/agentenv-scheduler
kubectl -n agentenv-system rollout undo deploy/agentenv-gateway
# 或钉回已知好版本 tag：merge-abe1bbd
```
阶段 0 单独回滚 = 把 `SCHEDULER_REGISTRY_DSN` 置空（feature 干净关闭，回到今天的行为）。
阶段 1 单独回滚 = 同上（DSN 空 ⇒ `lookupNode` 走 `ErrDisabled` 分支 ⇒ 旧的 NotFound 行为）。
🔴 但阶段 1 已经删了 gateway 的 recovery/reroute/replay，那部分**只能靠镜像回滚**。
