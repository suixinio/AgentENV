# SD 侦察：服务拆分的落地环境（`pve-sg dev`，203/204）

> 2026-08-20 · **全程只读**（动作清单见文末）。为
> [`2026-08-20-service-decomposition.md`](2026-08-20-service-decomposition.md) 的验收做环境摸底：
> 拆 `api` / `node` 角色之后，所有功能测试要在这套 k3s 上跑完。
>
> 上游：[`_recon-R3-cluster-runbook.md`](_recon-R3-cluster-runbook.md)（2026-08-19 的集群侦察）、
> [`_impl-plan-control-plane-phase3.md`](_impl-plan-control-plane-phase3.md) §6.0–§6.8（上一轮 13 步的定点更新序列与验证结果）。
> **本文不重复它们，只写「2026-08-20 今天的样子」与「差在哪」**；凡与 R3 冲突之处以本文为准并标注 🔧。
>
> 🔴 **凭据一律占位符。** 集群里 `agentenv-k8s-config` 的 `[backend.oss]` 段带明文 RustFS access key，
> 复核它时**只数行、不打印内容**（§7 探针 ①）。
>
> 🔧 **更新（2026-08-20 当日晚些）**，三处，均已就地标 🔧：
> ① **Redis 已部署**（`deploy/k8s/base/redis.yaml`，提交 `77aa98f`），scheduler 已接上、
> gateway 还没 —— §4.5 与 SD-B1 改为「解掉一半」，**不删除它挡住过什么的记录**；
> ② **滚 scheduler 的数据面 503 窗口重测为 2.3–3.3 秒**，上一轮记的 **14 秒**不要再引用（§4.5）；
> ③ **新增 SD-B6** —— scheduler **缺席**超过 `binding_ttl` 的表现**不是 503 而是 404**，
> 会让只数 503 的验证探针假通过（§9、§8 第 2 条）。
>
> ⚠️ **①②不是本次只读侦察的产物**：Redis 清单是另一批工作落的，
> 2.3–3.3 秒是一次带 `rollout restart` 的**写操作**测量。
> §10 的「全部只读」只描述本文最初那一轮。
>
> 🔧 **第二轮更新（2026-08-20 晚，阶段 1 验收当轮）：新增 §11，另有三处就地更新。**
> 🔴 **那一轮不是只读**（改开关、scale、applied 一个 NetworkPolicy、DEL 过 Redis key、建删过沙箱）。
> 里面有两条**会让未来探针整发作废**的：**这套集群不执行 NetworkPolicy**、
> **`kill -STOP 1` 对 PID 1 是静默无效**（§11.1）⇒ 目前**没有制造整机猝死的可用手法**（§11.2）；
> 还有一条清理沙箱的真坑：**`/sandboxes` 不列暂停的沙箱**（§11.3、§7.5）。
> 就地更新三处：§7.4 ⑦（三个开关落地成 **D-12 / D-13 / D-14**）、§7.5（收尾那一步）、§9 **SD-B1**（已全解）。
> 验收本身的记录不在本文，在 [`_sd-impl-phase1.md`](_sd-impl-phase1.md) §13。
>
> 🔧 **第三轮更新（2026-08-20 夜，阶段 2a 验收当轮）：新增 §11.6，另有四处就地更新。**
> 🔴 **同样不是只读**（taint 过节点、宿主机 `kill -9` 过、删过 Pod、`DEL` 过一次 Redis key、
> 建删过沙箱、播了 30 条快照并**故意留着**）。这一轮最重要的两条：
> ① **SD-B7 关闭** —— 猝死做得出来了（§11.6 ③ 的三步配方），
> 🔴 **但最省事的那条路（有沙箱在跑时直接 `delete pod`）是陷阱：节点自己把路由记录删干净，
> 探针扫无可扫、然后报"通过"**；
> ② 🔴 **`/nodes` 的 `sandboxCount` 不含暂停的沙箱，而心跳 roster 含** ——
> 拿它确认"这台机器是空的"会被骗（§11.3 第二个坑）。
> 就地更新四处：§7.1 A4 ＋ 新增 §7.1.a（registry 的 `Accept` 坑 —— 🔴 **头写错与 tag 不存在
> 都是 404，只有 body 分得出**，而本项目已经有一发探针栽在这上面）、
> §7.5（resume 要带 body ＋ `limit` 上限）、§9 **SD-B7**（已关闭）、§11.2（已被 §11.6 取代）。
> 2a 的验收记录在 [`_sd-impl-phase2.md`](_sd-impl-phase2.md) §13，清扫的在
> [`_sd-impl-phase1.md`](_sd-impl-phase1.md) §13.9。

---

## 0. 三十秒版

| 项 | 结论 |
|---|---|
| 访问 | 本机 `kubectl --kubeconfig ~/.kube/config-aenv-sg` **直连可用**；`ssh supos@10.10.10.203/204` 免密可用；gateway `http://10.10.10.203:30800` 本机可直达 |
| 拓扑 | 203 = k3s server（`aenv-master-01`，8C/16G/96G）；204 = agent + 构建机 + registry（`aenv-worker-01`，16C/32G/290G）。**两台都有 `/dev/kvm`**，都跑 `agentenv-node` |
| 在跑的版本 | 三个服务全是 `cp3-bff4993`（= commit `bff4993`），本地 HEAD `5d2dadd` **领先 6 个 commit**，其中 3 个是纯文档，另 3 个动了 `deploy/k8s/base` 与 `config/default.toml` |
| PG | ✅ 就绪（`agentenv-postgres-0`，`paused_sandboxes` **14 列 / 0 行**） |
| 对象存储 | ✅ RustFS 就绪（`rustfs` svc + NodePort 30900/30901，控制台 200） |
| Registry | ✅ `10.10.10.204:5000` 明文 HTTP，两节点 k3s 已信任 |
| **Redis** | 🔧 侦察时全集群没有；**同日已部署**（`deploy/k8s/base/redis.yaml`，提交 `77aa98f`）。scheduler 已接上（`binding_store="redis"`）；🔧 **当晚 gateway 也接上了**（`GATEWAY_REDIS_ADDR`）⇒ SD-B1 全解。见 §4.5、§9 |
| 镜像链路 | 在 **204** 上 `sudo docker build -f deploy/docker/Dockerfile.<svc> -t 10.10.10.204:5000/agentenv-<svc>:<不可变 tag> .` → `push` → `kubectl set image`。**绝不在本机构建再传**（上行实测 190 KB/s） |

**离「能部署测试服务拆分」还差什么**：见 §9 的阻塞项。**Redis 那条已经解掉一半**
（部署对象在了、scheduler 接上了、gateway 还没接）；剩下最硬的是
**`--role` 在代码里还不存在**（`grep -rn "role" src/bin/server.rs` 零命中）。
🔴 **另有一条新发现的、会让验证探针假通过的**：见 §9 **SD-B6** ——
scheduler 缺席超过 `binding_ttl` 的表现**不是 503，是 404**。

---

## 1. 怎么访问

### 1.1 SSH

`~/.ssh/config` 里**没有** 203/204 的条目 —— 它们靠默认 key（`~/.ssh/id_ed25519`）+ `known_hosts`
里的 hash 条目工作，直接写 IP 即可：

```bash
ssh -o BatchMode=yes -o ConnectTimeout=8 supos@10.10.10.203 'hostname; uname -a'
ssh -o BatchMode=yes -o ConnectTimeout=8 supos@10.10.10.204 'hostname; uname -a'
```

实测输出：

```
aenv-master-01   Linux 6.8.0-137-generic  x86_64   uid=1000(supos) groups=…,27(sudo)
aenv-worker-01   Linux 6.8.0-137-generic  x86_64   uid=1000(supos) groups=…,27(sudo),988(docker)
```

- 用户 `supos`，**两台都有 `sudo`**；🔴 **只有 204 在 `docker` 组** —— 这是「构建必须在 204」的机制原因之一。
- 🔴 `aenv` CLI **只在 203 上**：`/home/supos/.local/bin/aenv`（`aenv 0.1.2`），
  **不在非交互 SSH 的 PATH 里**，必须写全路径：
  ```bash
  ssh supos@10.10.10.203 '~/.local/bin/aenv list'
  ```
  凭据在 `~/.config/aenv/credentials`（TOML：`url = "http://10.10.10.203:30800"` + `api_key = <REDACTED>`）。

### 1.2 kubectl（本机直连，**不必 ssh 过去**）

```bash
export KUBECONFIG=~/.kube/config-aenv-sg
kubectl get nodes -o wide
```

本机还有 `~/.kube/config-aenv-test-sg.retired`（test 集群 201/202 的，**已退役，不要用**）、
`config-k3s-agentenv`、`cube-k3s.yaml` 等历史文件。🔴 **每个新 shell 都要显式 `export KUBECONFIG`** ——
默认 `~/.kube/config` 指的是别的集群。

### 1.3 HTTP 入口（本机可直达，无需跳板）

| 入口 | 地址 | 实测 |
|---|---|---|
| gateway | `http://10.10.10.203:30800` | `/health` ⇒ **204** ✅、`/nodes` ⇒ 200、`/sandboxes` ⇒ 200 |
| gateway `/metrics` | `30800/metrics` | 🟡 **404**（gateway 自身指标在 Pod `:9102`，见 §4.4） |
| RustFS 控制台 | `http://10.10.10.204:30901/rustfs/console/` | **200** ✅ |
| RustFS S3 | `http://10.10.10.204:30900/` | **403**（未签名请求的正常回应）✅ |
| agent-console | `http://10.10.10.203:30895` | **200**（无登录，只作本地排障）|
| registry | `http://10.10.10.204:5000/v2/_catalog` | 200 ✅ |

gateway 鉴权是空壳：**任意非空 `X-API-Key` 都放行**；节点 Pod `:8000` 才有真 gate（§4.4）。

---

## 2. 集群拓扑与机器能力

```
$ kubectl get nodes -o wide
NAME             STATUS  ROLES          AGE  VERSION       INTERNAL-IP    OS-IMAGE           KERNEL              RUNTIME
aenv-master-01   Ready   control-plane  6d   v1.36.3+k3s1  10.10.10.203   Ubuntu 24.04.4 LTS 6.8.0-137-generic  containerd://2.3.2-k3s2
aenv-worker-01   Ready   <none>         6d   v1.36.3+k3s1  10.10.10.204   Ubuntu 24.04.4 LTS 6.8.0-137-generic  containerd://2.3.2-k3s2
```

| | `aenv-master-01`（203） | `aenv-worker-01`（204） |
|---|---|---|
| 角色 | k3s **server** | k3s **agent** + **构建机** + **registry** |
| `/dev/kvm` | ✅ `crw-rw---- root kvm` | ✅ 同 |
| CPU | 8 vCPU，AMD EPYC 9354（AMD-V，x86_64） | 16 vCPU，同型号 |
| 内存 | 15G total / 13G available | 31G total / 28G available |
| 根盘 `/dev/sda1` | 96G，用 12G，**余 85G** | 290G，用 125G，**余 165G** |
| `docker` | ❌ 无（只有 k3s containerd） | ✅ 有，且在 `docker` 组 |
| 跑什么 | gateway、scheduler、`agentenv-node` | `agentenv-node`、PG、RustFS、agent-console、`aenv-registry` |

🔴 **两台都能跑真沙箱**（都有 `/dev/kvm`，DaemonSet 两副本都 Running），但 **203 的根盘只有 96G**，
所以集群里 `agentenv.toml` 的缓存预算被按 96G 定尺（`image.cache.capacity_gb=24` /
`remote_blocks.max_size_gb=12` / `oss cache_max_size_gb=8`，合 44G），而**仓内 `config/default.toml` 是 100/100**。
这是漂移 D-2（§6），一次 `make k8s-apply` 会把它抹回去。

🟡 **所有 PVC 都是 `local-path` 且钉在 204**（`data-agentenv-postgres-0` 5Gi、`rustfs-data` 100Gi、
`agent-console-audit` 1Gi）⇒ **204 挂掉，登记表和快照桶一起丢**。

---

## 3. `agentenv-system` 现状（2026-08-20 实测）

### 3.1 工作负载

| 工作负载 | 类型 | 副本 | 镜像 | RESTARTS / AGE |
|---|---|---|---|---|
| `agentenv-node` | **DaemonSet** | 2/2 | `10.10.10.204:5000/agentenv-runtime:cp3-bff4993` | worker 1 (9h) / master 0 |
| `agentenv-gateway` | Deployment | 1/1 | `10.10.10.204:5000/agentenv-gateway:cp3-bff4993` | 0 / 9h |
| `agentenv-scheduler` | Deployment | 1/1 | `10.10.10.204:5000/agentenv-scheduler:cp3-bff4993` | 0 / 9h |
| `agentenv-postgres` | StatefulSet | 1/1 | `postgres:17-alpine` | 0 / 9h |
| `rustfs` | Deployment | 1/1 | `rustfs/rustfs:latest` | 0 / 9h |
| `agent-console` | Deployment | 1/1 | `…/agent-console:sandbox-all-c35eaf2e-d061830` | 0 / 9h |

Service：

| Service | 类型 | 端口 |
|---|---|---|
| `agentenv-gateway` | ClusterIP | 8080 / **9102（metrics）** |
| `agentenv-gateway-nodeport` | NodePort | **8080:30800** 🔴 无清单文件（D-8）|
| `agentenv-scheduler` | ClusterIP | 9090（grpc）/ **9101（metrics）** |
| `agentenv-nodes` | Headless | 8000（scheduler 的 k8s discovery 靠它）|
| `agentenv-ublk-daemon-metrics` | Headless | 9103 |
| `agentenv-postgres` | ClusterIP | 5432 |
| `rustfs` / `rustfs-nodeport` | ClusterIP / NodePort | 9000,9001 / 30900,30901 |
| `agent-console` | NodePort | 8095:30895 |

🔧 **对 R3 的订正**：R3 的 B7 说「scheduler `:9101` metrics 未进 Service」—— **已经不成立**，
`agentenv-scheduler` 现在同时暴露 9090 与 9101，gateway 也暴露了 9102。两者都可以直接
`kubectl port-forward svc/…` 抓，不必再 port-forward Pod。

### 3.2 ConfigMap / Secret / PVC

| ConfigMap | 内容 | 谁读 |
|---|---|---|
| `agentenv-k8s-config` | 节点 `agentenv.toml`（🔴 带 RustFS 明文凭据） | node DS，挂 `/workspace/config/agentenv.toml` |
| `gateway-k8s-config` | `gateway.json` | gateway |
| `scheduler-k8s-config` | `scheduler.json` | scheduler |
| `execution-fencing-config` | 三个化身开关，**当前全是终态** | gateway + scheduler |
| `sandbox-proxy-config` | `SANDBOX_PROXY_DOMAINS=`（空） | gateway + node |
| `regctl-config` | `10.10.10.204:5000` 走明文 HTTP | node DS，挂 `/root/.regctl/config.json`（D-5）|

🔴 **`cluster-identity-config` 在集群里不存在** —— 仓内 `deploy/k8s/base/kustomization.yaml:35`
新增了它，集群侧仍从 `Secret agentenv-postgres/cluster_id` 拿（两处值实测相同，全零 UUID）。这就是漂移 D-10。

| Secret | key | 用途 |
|---|---|---|
| `agentenv-postgres` | `POSTGRES_PASSWORD` / `dsn` / `cluster_id` | PG 凭据 + 集群名 |
| `agentenv-control-plane-token` | **`token`** / **`node-gate-token`** | 前者 gateway 读 env，后者 node 读挂载文件。🔴 **两个 key 是顺序约束能成立的全部原因**（§6.0.6） |
| `agentenv-runtime-secrets` | `sandbox-access-token-hash-seed` | node |
| `rustfs-credentials` | `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` | RustFS 自身 |

### 3.3 三个化身开关：**当前全在终态**

```
$ kubectl -n agentenv-system get cm execution-fencing-config -o jsonpath='{.data}'
{"GATEWAY_ROUTING_EXECUTION_FENCING":"enforce",
 "SCHEDULER_REGISTRY_WRITE_FENCING":"true",
 "SCHEDULER_ROUTING_EXECUTION_ARBITRATION":"enforce"}
```

与 gateway 启动日志 `execution_fencing":"enforce","control_plane_token_configured":true` 一致。

🔴 **仓内 `kustomization.yaml` 里这三个 literal 是「发布起点」而不是终态**
（`SCHEDULER_ROUTING_EXECUTION_ARBITRATION=observe` / `GATEWAY_ROUTING_EXECUTION_FENCING=off`）。
⇒ **一次 `make k8s-apply` 会把这套集群的化身闸门整体退回 observe/off**，而且不报错。
这是 R3 之后**新增的**一条漂移，§6 的 D 表里没有 —— 本文把它登记为 **D-11**。

### 3.4 运行时接线（谁跟谁说话）

```
client ──30800──> gateway ──gRPC 9090──> scheduler ──pgx──> agentenv-postgres
                     │                       ▲
                     └──HTTP 8000──> node ───┘ (Heartbeat / 登记表 RPC，AENV_PAUSED_REGISTRY_BACKEND=central)
                                       └──S3──> rustfs
```

node DS 的关键 env（实测）：

| env | 值 |
|---|---|
| `AENV_PAUSED_REGISTRY_BACKEND` | **`central`**（硬写字面值，D-4）|
| `AENV_PAUSED_REGISTRY_DSN` | ← `Secret agentenv-postgres/dsn`（🟢 **新版 node 不读它**，D-6 残留）|
| `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED` / `_ENDPOINT` | `true` / `http://agentenv-scheduler:9090` |
| `AENV_API_CONTROL_PLANE_TOKEN_FILE` | `/etc/agentenv/control-plane/token`（卷只投影 `node-gate-token`）|
| `AENV_VIRTUALIZATION_MODE` / `AENV_NODE_ID` | `kvm` / ← `spec.nodeName` |
| `HOME` | `/root`（D-5，regctl 靠它找 config）|

`privileged: true`，🔴 **`terminationGracePeriodSeconds: 3600`** —— 节点上有 running 沙箱时
滚 DaemonSet 会长时间卡在 `Terminating`（§8 纪律 4）。

心跳间隔默认 **5s**（`src/cfg.rs:735` `AENV_OBSERVABILITY_REPORT_INTERVAL_SECS`）；
scheduler `binding_ttl` 默认 **30s**（`services/shared/config/config.go:655`），集群未覆盖。

---

## 4. 依赖组件就绪度

### 4.1 PostgreSQL ✅

`agentenv-system/agentenv-postgres-0`（`postgres:17-alpine`，nodeSelector 钉 `aenv-worker-01`，PVC 5Gi local-path）。

```bash
kubectl -n agentenv-system exec agentenv-postgres-0 -- \
  psql -U aenv -d aenv -c "SELECT state, count(*) FROM paused_sandboxes GROUP BY state;"
```

实测：**`(0 rows)`** —— 表在、**14 列**（上一轮 A2 加的两列已落地）、**当前零行**（上一轮步骤 2 的 DROP + 重建之后没有存量沙箱）。

🔧 **对 R3 的订正**：R3 §2.5「`services/` 无任何 DB 代码」**已不成立** ——
scheduler 现在自己连 PG（`SCHEDULER_REGISTRY_DSN` ← 同一个 Secret 的 `dsn` key），
启动日志 `scheduler paused registry enabled` / `write surface open`。**PG 写权已经在 scheduler 手上。**

DSN 形状（凭据遮蔽）：`postgres://<USER>:<PASSWORD>@agentenv-postgres.agentenv-system.svc.cluster.local:5432/aenv`。

🟡 **9h 前有过一次冷启动窗口**：scheduler 05:57 起来时 PG 还没 ready，
连报 ~80s `connection refused`，05:58:16 `write surface open`（`inferred_downtime=106s`，续了 3 条租约），
06:00 grace 结束。**这是正常的重启形态，不是故障** —— 但它说明 **PG 与 scheduler 之间没有启动序**，
拆 `api` 之后副本数变多，这个窗口会变宽。

### 4.2 对象存储 RustFS ✅

- 集群内端点：`http://rustfs.agentenv-system.svc.cluster.local:9000`，bucket `agentenv-snapshots`，prefix `snapshots/`。
- 凭据：**写死在 `agentenv-k8s-config` 的 `[backend.oss]` 明文段里**（不是从 Secret 引），
  另有一份 `Secret rustfs-credentials` 给 RustFS 自身。🔴 **这就是 D-1 抹不掉又不能进仓库的原因。**
- 节点 toml：`[snapshot] repository_backend = "oss"` + `p2p_enabled = true`（但 `[p2p] enabled = false`）。

### 4.3 Registry ✅

```
$ curl http://10.10.10.204:5000/v2/_catalog
{"repositories":["agent-console","agentenv-gateway","agentenv-runtime","agentenv-scheduler",…]}
$ curl http://10.10.10.204:5000/v2/agentenv-scheduler/tags/list
{"tags":["cp3-bff4993","isolation","d11-9a8fd88","cp1-c35f5ec","cp0-7e6f790","merge-abe1bbd",
          "isolation3","latest","isolation2","cp2-40a4526"]}
```

- registry 本体：204 上的 docker 容器 `aenv-registry`（`registry:2`），Up 5 天。
- 两节点 k3s 已信任：`/etc/rancher/k3s/registries.yaml` 里 `10.10.10.204:5000` → 明文 endpoint + `insecure_skip_verify`。
- 204 的 `/etc/docker/daemon.json` = `{"insecure-registries":["10.10.10.204:5000"]}`。
- 🔴 **本机的 `/etc/docker/daemon.json` 指的是 `10.1.0.106:5000`（另一套环境）**
  ⇒ **本机 docker 直接 push 到 204 会因 HTTPS 校验失败**。见 §5 为什么这不重要。

### 4.4 metrics 端点

| 端点 | 地址 | 状态 |
|---|---|---|
| gateway | Pod/Svc `:9102` | ✅ 已进 Service（`30800/metrics` 是 **404**，别拿它当 gateway 指标）|
| scheduler | Pod/Svc `:9101` | ✅ 已进 Service |
| node 运行时 | Pod `:8000/metrics` | ✅ 无 NodePort，且**结构性地在 control-plane gate 覆盖面之外**（`src/api/server.rs` 里 `/metrics` 加在 `assemble()` 之后）⇒ 开 gate 不打断抓取 |
| ublk daemon | Pod `:9103`，headless svc | ✅ |

### 4.5 🔧 Redis —— 本节写于「不存在」时，**当天晚些时候已经部署**

> **原文保留，因为它记录的是这条阻塞项挡住了什么。当前状态在本节末尾。**

初次侦察时（2026-08-20 上午）：

```
$ kubectl get pods -A | grep -i redis        # 零命中
$ kubectl get svc  -A | grep -i redis        # 零命中
$ ssh supos@10.10.10.203 'which redis-server' # 空
$ ssh supos@10.10.10.204 'which redis-server' # 空
```

scheduler 启动日志坐实：

```
scheduler gRPC server listening addr=":9090" strategy="round_robin"
  binding_store="memory" query_only=false paused_registry=true
```

配置入口存在但没配：`scheduler.redis_addr` / env `SCHEDULER_REDIS_ADDR`
（`services/shared/config/config.go:260`），`--query-only` 副本**强依赖它**
（`services/scheduler/cmd/main.go:33,266`）。

**这一条的后果分三层，别只记第一层**：

1. `P-A5-2「HA 形态」`探针在这套集群上**结构性地跑不了**（上一轮已把它显式登记为未验证项，
   见 `_impl-plan-control-plane-phase3.md` §6.8.7 第二条边界）。
2. **滚 scheduler 要付一个数据面 503 窗口**（binding 在内存里，滚动即丢）。
   拆 `api` 期间要反复滚控制面 ⇒ 这个窗口会被反复付。
   🔧 **窗口大小已重新实测，见下方「② 的数字改了」。**
3. 🔴 **服务拆分的阶段 1 与阶段 3 都以 Redis 为前提** —— 阶段 1 是「gateway 直读 Redis」，
   阶段 3 是「活跃态折叠进 Redis」。**没有 Redis，这两个阶段在集群上一步都验不了。**

---

#### 🔧 当前状态（同日，提交 `77aa98f`）

| 项 | 状态 |
|---|---|
| Redis 部署对象 | ✅ **在了** —— `deploy/k8s/base/redis.yaml`（Deployment ＋ Service ＋ PVC），已进 `kustomization.yaml:11` 的 `resources:` |
| scheduler | ✅ **已接上** —— `deploy/k8s/base/scheduler-deployment.yaml:82-83` 的 `SCHEDULER_REDIS_ADDR=agentenv-redis:6379`；启动日志 `binding_store="redis"` |
| gateway | 🔴 **还没接** —— `deploy/k8s/base/gateway-deployment.yaml` 里没有任何 redis env。阶段 1 的②（gateway 直读）**还差这一步** |
| `P-A5-2「HA 形态」` | 🟡 现在**跑得了**了，但还没跑 |

🔴 **持久化取向要记住**：`redis.yaml` 是按**耐久存储**配的，不是按缓存
—— `appendonly yes` ＋ `maxmemory-policy noeviction`，理由逐字写在清单抬头
（拆分方案阶段 3 要把活跃沙箱状态折进来，「a lost write here will eventually mean a lost
sandbox」）。**不要把这两项「整理」成缓存形状的默认值。**
🔴 而 HA 仍是敞口：单副本 ＋ 单个 local-path 卷，且落在**已经背着 PG 与 RustFS 的那台**
（204，见 SD-B5）。

#### 🔧 ② 的数字改了：14s → **2.3–3.3s**

本节原文引的 **14s** 来自上一轮，**今天在这套集群上按同一形状重测过**
（一个运行中沙箱、成对探针、3 次/秒打数据面代理路径，一次 `kubectl rollout restart`）——
量出来小一个数量级：

| 场景 | 数据面 503 窗口 |
|---|---|
| `binding_store="memory"`，滚 scheduler | **连续 2.3–3.3 秒** |
| `SCHEDULER_REDIS_ADDR` 已配，同一次滚动 | **0 次 503** |

⇒ **「14 秒」这个数不要再引用了**，引用会把一个 3 秒级的窗口说成一次事故。
方向不变：滚动即丢 binding 的问题是真的，Redis 也确实把它关到 0。

🔴 **但这条测的只是「滚动」，不是「缺席」** —— 后者的表现完全不同，
而且**不是 503**。见 §9 的 **SD-B6**。

---

## 5. 镜像构建与发布链路

### 5.1 一句话

**在 204 上用仓库根做 context 构建 → push 到 `10.10.10.204:5000` → `kubectl set image`。**
`api` 与 `node` 共用 `agentenv-runtime` 镜像（拆分提案 §1 的命名约定），所以拆 role 之后
**镜像还是三个，不是四个**。

### 5.2 🔴 为什么必须在 204 上构建（实测数据）

| 方向 | 实测 |
|---|---|
| 204 → 本机（下行） | 20 MB / 3.7s ≈ **5.4 MB/s** |
| 本机 → 204（**上行**） | 20 MB / **107.8s** ≈ **190 KB/s** |
| RTT | **110 ms** |

`agentenv-runtime` 镜像 476 MB（压缩 121 MB）⇒ 从本机 push 一次要 **~11 分钟**，而且是每一发。
🔧 R3 写的「~70KB/s」量级方向是对的，**具体数字按本文**（190 KB/s，且**只有上行慢**）。
**下行 5.4 MB/s 意味着 `kubectl logs` / `exec` / 拉配置都很顺**，慢的只有推镜像。

### 5.3 Dockerfile 与 context

| 服务 | Dockerfile | 构建时长量级 |
|---|---|---|
| gateway | `deploy/docker/Dockerfile.gateway`（golang:1.25 多阶段 → distroless） | 快（Go 侧 ~42 个文件） |
| scheduler | `deploy/docker/Dockerfile.scheduler`（同构 + `grpc-health-probe` 阶段） | 快 |
| runtime（`api`/`node`） | `deploy/docker/Dockerfile.agentenv`（cargo-chef） | 🔴 首次 5–8 min，改 `src/` 后增量也不便宜 |

🔴 **context 是仓库根**（要 `services/` 和 `deploy/docker/config/`），
写 `-f deploy/docker/Dockerfile.gateway .`，**不能只送 `services/`**。

### 5.4 构建机源码状态（204，2026-08-20 实测）

```
/opt/AgentENV   HEAD = 32ffc6a (detached)
remotes: fork=https://github.com/suixinio/AgentENV.git   origin=https://github.com/kvcache-ai/AgentENV.git
```

本地 HEAD `5d2dadd` 比它多 3 个**纯文档** commit（`a4c303a` / `d65b843` / `5d2dadd`）
⇒ **构建机的代码面与本地一致**，改 `services/` 或 `src/` 之前 fetch 一次即可。

🟡 12 个未跟踪文件仍在（`*.prealign`、`build-*.log`、`.cargo-test/`），不影响构建。
🔴 **`tests/fixtures` 是 root 属主，`git checkout` 会静默漏文件** —— checkout 后必须
`sudo git status -s` 复核，缺文件就 `checkout -- <path>` 补。

### 5.5 🔴 三条不许省的纪律

1. **不可变 tag，绝不用 `:latest`。** `imagePullPolicy: IfNotPresent` + 203 的 containerd 里缓存着
   一份**过期的** `…:latest` ⇒ 用 `:latest` 在 203 上会**静默跑旧代码**。
   命名沿用现有惯例：`<批次>-<短 commit>`，如 `sd1-$(git rev-parse --short HEAD)`。
2. **验证换没换要比 `imageID`，不是比 image 名。**
3. **首选 `kubectl set image` / `kubectl patch`，不要 `make k8s-apply`** —— 理由见 §6。

---

## 6. 🔴 `make k8s-apply` 的漂移风险（**在 R3 基础上又多了一条**）

机制未变：`deploy/k8s/run.sh:30` 把 `config/default.toml` 逐字拷成
`base/config/agentenv.toml`，`configMapGenerator` 带 `disableNameSuffixHash: true` ⇒ **同名覆盖**；
而 `kubectl apply -k` **不 prune** ⇒ **一次 apply 抹掉一半漂移、留下另一半，且大多数不报错**。

完整的 D-1 … D-10 十条清单在 [`_impl-plan-control-plane-phase3.md` §6.0.2](_impl-plan-control-plane-phase3.md)，
**不在此重复**。本文只补今天新看到的两条：

| # | 集群 out-of-band 的东西 | 仓内清单说的是什么 | apply 后的症状 |
|---|---|---|---|
| **D-11**（新） | CM `execution-fencing-config` = `enforce` / `true` / `enforce`（**终态**） | `kustomization.yaml` 的 literal 是 `true` / **`observe`** / **`off`**（发布**起点**） | 🔴 **静默**：化身闸门整体退回 observe/off ⇒ 路由仲裁不再拒、gateway 不再拒。终端上只有一行 `configured` |
| **D-3 已变形** | node toml 里**已经没有** `[orchestrator.paused_registry]` 段了；backend 完全靠 DS 的 `AENV_PAUSED_REGISTRY_BACKEND=central` | 仓内 DS 是 `configMapKeyRef: paused-registry-config`（`optional`），而该 CM 集群里**不存在** | 🔴 **静默**：env 消失 ⇒ 回落 `local` ⇒ 中央登记表悄悄关掉。🟢 **好消息**：R3 记的「CM 里 `postgres` 会让新 node 拒绝启动」这个组合**已经消失**，因为那一段已经不在 CM 里了 |

**规避（按性价比）**

1. ✅ **改 Go / 改 Rust 的发布路径完全不需要 apply** —— §7 的 runbook 全程只 `set image` / `patch`。
2. 万一必须 apply（改了 base 清单结构，比如新增 `agentenv-api` Deployment）：
   ```bash
   kubectl -n agentenv-system get cm agentenv-k8s-config      -o yaml > /tmp/cm-agentenv.bak.yaml
   kubectl -n agentenv-system get cm execution-fencing-config -o yaml > /tmp/cm-fencing.bak.yaml
   kubectl -n agentenv-system get svc agentenv-gateway-nodeport -o yaml > /tmp/svc-30800.bak.yaml
   ```
   apply 完**立刻**跑 §7.4 的五组复核探针，逐条把漂移补回去。
3. 🔴 **反模式**：继续加长 `patch-node-config.sh` 那种「apply 完再补一刀」的脚本
   （它只认识 OSS 与缓存预算，不认识 `paused_registry`，更不认识 `execution-fencing-config`）。

---

## 7. 🔧 Runbook：**如何在这个集群上部署并验证一个改动**

> 这一节是本文的交付物。每条命令都可以直接粘。
> `<TAG>` 一律用不可变 tag；下面统一记作 `$TAG`。

### 7.0 公共变量（每个新 shell 都要重设）

```bash
export KUBECONFIG=~/.kube/config-aenv-sg
NS=agentenv-system
GW=http://10.10.10.203:30800
REG=10.10.10.204:5000
```

### 7.1 步骤 A：把改动推到构建机并构建

```bash
# A1. 本地：推到 fork（构建机从 fork 拉）
git push fork <your-branch>

# A2. 构建机对齐源码
ssh supos@10.10.10.204 '
  cd /opt/AgentENV
  sudo git -c safe.directory=/opt/AgentENV fetch fork <your-branch>
  sudo git -c safe.directory=/opt/AgentENV checkout --detach FETCH_HEAD
  sudo git -c safe.directory=/opt/AgentENV status -s          # 🔴 缺文件就 checkout -- <path> 补
  sudo git -c safe.directory=/opt/AgentENV rev-parse --short HEAD
'

# A3. 构建 + push（只建改了的那个；Go 两个都改就都建）
ssh supos@10.10.10.204 '
  cd /opt/AgentENV
  TAG=sd1-$(sudo git -c safe.directory=/opt/AgentENV rev-parse --short HEAD)
  sudo docker build -f deploy/docker/Dockerfile.gateway   -t 10.10.10.204:5000/agentenv-gateway:$TAG   . &&
  sudo docker push  10.10.10.204:5000/agentenv-gateway:$TAG
  # scheduler： Dockerfile.scheduler / agentenv-scheduler
  # api+node ： Dockerfile.agentenv  / agentenv-runtime   （🔴 5-8 min）
  echo "TAG=$TAG"
'

# A4. 确认 registry 里真有这个 tag（比"push 没报错"硬）
curl -s http://$REG/v2/agentenv-gateway/tags/list | grep -o "$TAG"
# 🔴 存在性一律用 /tags/list —— 它不吃 Accept 头，裸查就是 200。
#    只有需要 digest 时才去碰 /manifests/<tag>，那条路有个会骗人的坑：见 §7.1.a
```

#### 7.1.a 🔴 registry 的 `Accept` 坑：两个 404 长得一模一样

**2026-08-20 夜实测，逐字留证**（`10.10.10.204:5000`，repo `agentenv-runtime`，
tag `sd2a-7cf9c30` —— **一个确实存在的 tag**）：

| # | 请求带的 `Accept` | 结果 |
|---|---|---|
| **A** | **不带** | `404`，`content-type=application/json`，body：`{"errors":[{"code":"MANIFEST_UNKNOWN","message":"OCI index found, but accept header does not support OCI indexes"}]}` |
| **B** | `application/vnd.docker.distribution.manifest.v2+json` | 🔴 **仍然 404** |
| **C** | `application/vnd.oci.image.index.v1+json` | ✅ `200`，`Content-Type: application/vnd.oci.image.index.v1+json`，`Docker-Content-Digest: sha256:3623b89ee89c…375ed7` |
| **D**（对照面）| 正确的 Accept ＋ **不存在的 tag** `sd2a-deadbeef` | `404` |

🔴 **B 说明这不是"v1 还是 v2"的问题**：docker v2 这个类型**本身**就被拒。
这些镜像是 **OCI index**，只有点名 index 类型的 `Accept` 才拿得到。

🔴 **而真正让它成为陷阱的是 A 与 D：两发都是 404 ——
状态码分不出「我的头写错了」和「这个 tag 根本不在」。**
只有 **body** 分得出：头写错的那一发带着
`"OCI index found, but accept header does not support OCI indexes"`，
真的不存在的那一发**没有这一句**（两者的 `code` 都是 `MANIFEST_UNKNOWN`）。

⇒ 这正是 §8 第 1 条那类失效，而且**本项目已经踩过**：一发探针拿真 tag 与假 tag
各查一次 manifest、两次都收到 404，于是判成"这个 tag 不在" —— **它毫无分辨力**。

**两条规矩：**

1. **查存在性用 `/v2/<repo>/tags/list`**（不吃 Accept，裸查 200，本轮再次确认）。
2. **只有要 digest 时才碰 `/manifests/<tag>`**，且必须带 OCI index 类型，
   并且 🔴 **看 body，不只看状态码**：

```bash
# 🔴 别加 -o /dev/null：body 是这里唯一的信息源
curl -s -D- \
  -H 'Accept: application/vnd.oci.image.index.v1+json' \
  http://$REG/v2/agentenv-runtime/manifests/$TAG | head -20
# 期望 200 ＋ Docker-Content-Digest。拿到 404 就看 body 里有没有
# "accept header does not support OCI indexes" —— 有，就是头的问题，不是 tag 不在。
```

### 7.2 步骤 B：发布

```bash
# B1. 先记基线（回滚与"热生效"判据都要它）
kubectl -n $NS get pods -o wide > /tmp/sd-baseline-pods.txt
kubectl -n $NS get ds/agentenv-node deploy/agentenv-gateway deploy/agentenv-scheduler \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.template.spec.containers[0].image}{"\n"}{end}' \
  > /tmp/sd-baseline-images.txt

# B2. 只换镜像（🔴 不碰 ConfigMap ⇒ 天然规避 §6 的所有漂移）
kubectl -n $NS set image deploy/agentenv-gateway   gateway=$REG/agentenv-gateway:$TAG
kubectl -n $NS set image deploy/agentenv-scheduler scheduler=$REG/agentenv-scheduler:$TAG
kubectl -n $NS rollout status deploy/agentenv-gateway   --timeout=180s
kubectl -n $NS rollout status deploy/agentenv-scheduler --timeout=180s

# 🔴 滚 node（DaemonSet）之前：先确认没有 running 沙箱，否则 grace=3600 会把 rollout 卡死
curl -s -H 'X-API-Key: dummy' $GW/sandboxes | python3 -c 'import json,sys;print(len(json.load(sys.stdin)))'   # 期望 0
# 🔧 🔴 这一行只数 running（§11.3）。**暂停的那几台它看不见，而 rollout 会把它们恢复出来**
#    ⇒ 判断"这台机器是空的"一律照 /v2/sandboxes 数，`/nodes` 的 sandboxCount 同样漏（§11.3 第二个坑）
kubectl -n $NS set image ds/agentenv-node agentenv=$REG/agentenv-runtime:$TAG
kubectl -n $NS rollout status ds/agentenv-node --timeout=600s
# 🔴 绝不 --force --grace-period=0：强删只删 API 对象、容器还活着，
#    新 Pod 抢不到 records.db/LOCK 会 CrashLoop

# B3. 验证真的换了 —— 🔴 比 imageID，不是比 image 名
kubectl -n $NS get pod -l app.kubernetes.io/name=agentenv-node \
  -o jsonpath='{range .items[*]}{.spec.nodeName}{" "}{.status.containerStatuses[0].imageID}{"\n"}{end}'
kubectl -n $NS get pod -l app.kubernetes.io/name=agentenv-gateway \
  -o jsonpath='{.items[*].status.containerStatuses[*].imageID}{"\n"}'
```

🟡 **滚 scheduler 要付一个数据面 503 窗口**（binding 在内存里；🔧 **重测为 2.3–3.3 秒**，
不是上一轮记的 14s —— §4.5）。窗口内的 503 **既不是失败也不是"没有窗口"**。
🟢 **`SCHEDULER_REDIS_ADDR` 配上之后同一次滚动是 0 次 503**，而集群今天已经配上了。
滚 gateway 不用付。
🔧 **再次复现（2026-08-20 夜，2a 发布当轮）**：带着 `SCHEDULER_REDIS_ADDR` 滚 scheduler，
数据面**零错误**。⇒ 这条现在有两次独立取证，可以当作稳定结论用。

### 7.3 步骤 C：冒烟（每一发都跑，30 秒）

```bash
curl -s -o /dev/null -w 'health=%{http_code}\n' $GW/health                             # 期望 204
curl -s -H 'X-API-Key: dummy' $GW/nodes | python3 -c \
  'import json,sys;[print(n["id"],n["status"],n["sandboxCount"]) for n in json.load(sys.stdin)]'
# 期望两行 ready

kubectl -n $NS logs deploy/agentenv-scheduler --tail=50 | grep -E 'listening|registry'
kubectl -n $NS logs -l app.kubernetes.io/name=agentenv-node --tail=500 --prefix \
  | sed 's/\x1b\[[0-9;]*m//g' | grep -a 'paused sandbox registry ready'
# 🔴 两台都必须是 backend="central"，不是 "local"
```

### 7.4 步骤 D：漂移复核（**每次发布后都跑，带对照面**）

```bash
# ① D-1：OSS 后端与 [backend.oss] 段还在（🔴 只数行，那段里有明文凭据）
kubectl -n $NS get cm agentenv-k8s-config -o jsonpath='{.data.agentenv\.toml}' | grep -c '^repository_backend = "oss"'  # 1
kubectl -n $NS get cm agentenv-k8s-config -o jsonpath='{.data.agentenv\.toml}' | grep -c '^\[backend\.oss\]'            # 1
kubectl -n $NS get cm agentenv-k8s-config -o jsonpath='{.data.agentenv\.toml}' | grep -c '^repository_backend = "no_such_backend"'  # 对照面：必须 0

# ② D-4：中央登记表开关还在
kubectl -n $NS get ds agentenv-node -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="AENV_PAUSED_REGISTRY_BACKEND")].value}{"\n"}'  # central
kubectl -n $NS get ds agentenv-node -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="AENV_NO_SUCH_ENV")].value}{"\n"}'              # 对照面：必须空行

# ③ D-5：regctl 挂载与 HOME 还在
kubectl -n $NS get ds agentenv-node -o jsonpath='{.spec.template.spec.containers[0].volumeMounts[?(@.name=="regctl-config")].mountPath}{"\n"}'    # /root/.regctl/config.json
kubectl -n $NS get ds agentenv-node -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="HOME")].value}{"\n"}'                          # /root

# ④ D-7：三个镜像都带 registry 前缀 + 不可变 tag
for r in ds/agentenv-node deploy/agentenv-gateway deploy/agentenv-scheduler; do
  kubectl -n $NS get "$r" -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
done   # 🔴 三行都必须以 10.10.10.204:5000/ 开头；出现裸 agentenv-*:latest = 有人 apply 过，立刻停

# ⑤ D-8：30800 还在
kubectl -n $NS get svc agentenv-gateway-nodeport -o jsonpath='{.spec.ports[0].nodePort}{"\n"}'   # 30800

# ⑥ D-11（本文新增）：三个化身开关还在终态
kubectl -n $NS get cm execution-fencing-config -o jsonpath='{.data}{"\n"}'
# 期望 enforce / true / enforce。出现 observe 或 off = 被 apply 退回了

# ⑦ 🔧 D-12 / D-13 / D-14（**已落地，原文写的「尚未进 deploy/k8s/base」已过时**）：
#    三个路由投影开关现在有清单了 —— gateway-deployment.yaml:129-144、
#    scheduler-deployment.yaml:169-174，CM literal 在 kustomization.yaml:120-124，
#    集群里的 CM 叫 routing-projection-config（不是 execution-fencing-config，故意分开的）。
kubectl -n $NS get cm routing-projection-config -o jsonpath='{.data}'; echo
# 期望三个全 on（2026-08-20 阶段 1 验收后的终态）：
#   GATEWAY_ROUTING_PROJECTION_READ / GATEWAY_ROUTING_PROJECTION_AUTHORITATIVE
#   / SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE
# 🔴 与 D-11 的形状一样、方向相反：仓内 literal 是 **off**（kustomization.yaml:122-124），
#    集群终态是 **on** ⇒ 一次 apply 把三个全部**静默**退回 off。这就是 D-12/13/14。
# 🔴 这一条比 D-11 更贵：退回 off 不只是「新行为不生效」，**再翻回 on 要付一次 404 窗口**
#    （_sd-impl-phase1.md §13.4 的 SD-D1；机理与代价见本文 §11.4）。
# 🔴 成对约束：SCHEDULER_ROUTING_EXECUTION_ARBITRATION=off 必须同时
#    GATEWAY_ROUTING_PROJECTION_READ=off。
# 🔴 回退只用 `kubectl set env`，**绝不删 key**：集群里在跑的 Deployment 上这三条
#    configMapKeyRef **没有** optional: true（仓内清单有，集群那份没有），
#    删 key ⇒ Pod CreateContainerConfigError，不是「落回代码默认」。
```

### 7.5 步骤 E：功能验证（造真沙箱）

```bash
# 🔴 建沙箱两个必传字段，漏了不报错但会把你带偏：
#   timeout：不传吃 15s 默认 TTL（config: default_sandbox_timeout_secs = 15），沙箱几十秒内自己消失
#   autoResume：不传则 paused 后数据面恒 410
curl -s -X POST -H 'X-API-Key: dummy' -H 'Content-Type: application/json' $GW/sandboxes -d '{
  "templateID": "uns-sandbox-runtime-v3",
  "timeout": 1800,
  "autoResume": {"enabled": true}
}' | tee /tmp/sd-sandbox.json

SB=$(python3 -c 'import json;print(json.load(open("/tmp/sd-sandbox.json"))["sandboxID"])')

# pause / resume（登记表这条路径的端到端）
curl -s -o /dev/null -w 'pause=%{http_code}\n'  -X POST -H 'X-API-Key: dummy' $GW/sandboxes/$SB/pause
kubectl -n $NS exec agentenv-postgres-0 -- psql -U aenv -d aenv \
  -c "SELECT state, origin_node_id, execution_id FROM paused_sandboxes WHERE sandbox_id='$SB';"

# 🔧 🔴 订正（2026-08-20 夜实测）：resume **必须带 Content-Type ＋ body**，
#    原来这里写的裸 POST 回的是 **415**，不是 201。
#    requestBody 是 required: true / schema ResumedSandbox（src/api/openapi.yml:1629-1634，
#    字段 timeout 在 :581-588）。
curl -s -o /dev/null -w 'resume=%{http_code}\n' -X POST -H 'X-API-Key: dummy' \
  -H 'Content-Type: application/json' -d '{"timeout": 1800}' $GW/sandboxes/$SB/resume
# 🔴 这条坑危险在于**它看起来是无害的**：autoResume 开着时，紧接着的任何数据面请求
#    都会把沙箱自动 resume，状态读出来就是 running ⇒ 那个 415 看着只像"头没写对"，
#    实际含义是**这一发根本没有验到 resume API**。判据里含 resume 的探针必须单独校这个码。

# 收尾
curl -s -o /dev/null -w 'delete=%{http_code}\n' -X DELETE -H 'X-API-Key: dummy' $GW/sandboxes/$SB

# 🔴 🔧 收尾的真坑（2026-08-20 实测踩到）：`/sandboxes` **不列暂停的沙箱**。
#    它 deprecated，语义就是「列 running」（src/api/openapi.yml:1344-1348）；
#    `/v2/sandboxes` 才列全部，并且带 state 过滤（:1489-1512）。
#    ⚠️ 别照 /v2/sandboxes 那个 200 的描述文案判断语义 —— 它仍写着
#    "all running sandboxes"（:1521），和它自己的 summary 冲突，是个陈旧文案。
#    照 /sandboxes 数着清，暂停的那几台会留下来，而且不会安静地留着：见 §11.3。
curl -s -H 'X-API-Key: dummy' $GW/v2/sandboxes \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d)); [print(" ", x["sandboxID"], x.get("state")) for x in d]'

# 🔧 🟡 顺带（2026-08-20 夜）：列表接口的 limit 上限是 **100**
#    （src/api/openapi.yml:93-103，maximum: 100）⇒ /snapshots?limit=200 直接 **400**（range error）。
#    写脚本别用 200。另：limit 今天**不省任何对象存储请求**，见 _sd-impl-phase2.md §13.1。
```

也可以用 203 上的 CLI（**必须写全路径**）：

```bash
ssh supos@10.10.10.203 '~/.local/bin/aenv list'
ssh supos@10.10.10.203 '~/.local/bin/aenv template list'
```

现有模板（两个都 `ready`）：`uns-sandbox-runtime-v3`（`01a008fd-3c28-…`）、`uns-scaffold-v3`（`01a008fd-3c39-…`），
均 2 vCPU / 4096 MB / 磁盘 64 GB / envd `0.5.15`。
🟡 `spawnCount` 随 node 进程重启归零，**不能拿它推断"没人用过"**。

### 7.6 步骤 F：抓指标

```bash
kubectl -n $NS port-forward svc/agentenv-scheduler 9101:9101 &
curl -s localhost:9101/metrics | grep -E 'agentenv_scheduler_(registry_write_rpc_total|binding_execution_total|lookup_execution_authority_total)'
kubectl -n $NS port-forward svc/agentenv-gateway 9102:9102 &
curl -s localhost:9102/metrics | grep -E 'agentenv_gateway_'
# node 侧（无 NodePort，要从集群内或 port-forward Pod）
kubectl -n $NS port-forward pod/<node-pod> 18000:8000 &
curl -s localhost:18000/metrics | head        # 🟢 /metrics 在 control-plane gate 覆盖面之外，不需要 token
```

### 7.7 步骤 G：回滚

```bash
kubectl -n $NS rollout undo deploy/agentenv-gateway            # 回上一个 ReplicaSet
# 或钉回已知好版本（今天三个服务跑的都是它）：
kubectl -n $NS set image deploy/agentenv-gateway   gateway=$REG/agentenv-gateway:cp3-bff4993
kubectl -n $NS set image deploy/agentenv-scheduler scheduler=$REG/agentenv-scheduler:cp3-bff4993
kubectl -n $NS set image ds/agentenv-node          agentenv=$REG/agentenv-runtime:cp3-bff4993
```

退路 tag（registry 里现存）：`cp3-bff4993`（当前）/ `cp2-40a4526` / `cp1-c35f5ec` / `cp0-7e6f790` / `d11-9a8fd88` / `merge-abe1bbd`。

🔴 **配置级回滚有一个反直觉的坑（上一轮实测）**：关 control-plane gate 时
**「删 key」与「写空串」结果完全相反，而失败那一侧是静默的**——

| 做法 | `kubectl` | 挂载文件 | gate gauge | 直连 `POST /pause` |
|---|---|---|---|---|
| **删 key** | `patched` | 12s 后消失 | 🔴 **仍然 1** | 🔴 **仍然 403** |
| **写空串** | `patched` | 60s 后 0 字节 | ✅ 0 | ✅ 204 放行 |

⇒ **回退 gate 只能把 `node-gate-token` 写成空串，绝不能删 key。**
（node 对"读失败"是刻意保留上一个 good 值的，`src/api/control_plane_gate.rs`。）
⚠️ kubelet 卷刷新**跨节点不同步**（实测 12s / 60s 偏斜）⇒ **止血按最慢那台计时**。

---

## 8. 上一轮验证方法论（服务拆分要照抄的四条）

来源：[`_impl-plan-control-plane-phase3.md`](_impl-plan-control-plane-phase3.md) §6.6 / §6.8 与
[`_verify-plan-phase01.md`](_verify-plan-phase01.md) / [`_verify-T2-final.md`](_verify-T2-final.md)。
**这四条是上一轮 13 步全过的全部原因，不是格式要求。**

1. 🔴 **每发探针必须自带对照面** —— 一个**必然为假**的输入，证明这条探针有分辨力。
   上一轮翻过车：grace 期"拒绝接管"的第一发用了一行任何相位都认领不了的合成行，
   `409` 看着像被拒，实则毫无分辨力。
   *形态举例*：断言「无 token `POST /sandboxes` ⇒ 403」，对照面是「**带错误 token** ⇒ 仍 403」
   —— 排除"有这个头就放行"这种更弱的实现。
2. 🔴 **"某指标恒 0"本身不是证据** —— 必须先把它**顶起来**一次，证明 0 是事实而不是探针瞎了。
   上一轮做法：刻意 pause 一台沙箱再打数据面 ⇒ `unfenced_node_silent` 长出 3。
   🔴 **本轮的具体形态（SD-B6）**：一个只数 503 的探针，在「scheduler 缺席 5 分钟」
   这一相位上会**假通过** —— 因为真正的失败长成 404。
   **探针必须按状态码分类计数，不能只判 `!= 200` 或只判 503。**
3. 🔴 **两侧互证** —— gateway 侧增量与 scheduler 侧同义计数器的增量必须**逐条对得上**
   （上一轮：46 = 46）。对不上就是有一侧算错了。
4. 🔴 **射程边界要照实写** —— 合成行探针证明的是"登记表这一侧的谓词对"，
   **不证明**"真节点收到拒绝之后克制住了没动快照链"。**别把一半的证据当成全覆盖引用。**

**上一轮验完之后留下的、服务拆分会正面撞上的 4 条待办**（`_impl-plan-control-plane-phase3.md` §12.6）：

| # | 内容 | 谁会撞上 |
|---|---|---|
| (1) | scheduler 侧被 fence 的 `begin_pause` **零日志**，只有计数器 | 拆 `api` 后排错链更长，这一断点更疼 |
| (2) | 唯一那条 fencing 日志缺 `refusal_code`；`fencing_stage` 冒出第 4 个取值 `binding_arbitration` | 任何要按 `refusal_code` 串三段的排错 |
| (3) | `agentenv_scheduler_registry_reclaimable_now` **恒 0**，不能当预警 | 想拿它做 `api` 副本健康判据的人 |
| (4) | 🔴 `local_only` 不在 `paused_sandboxes_reclaim_idx` 覆盖面 ⇒ 过期的 `local_only` 行**永不回收** | B2（evictor 与 reclaim 分离）——**这不是一行 SQL，是设计问题** |

**上一轮验证的"射程外"两条**（拆分验收要么补跑、要么继续显式登记）：

- `P-A5-2「HA 形态」`（Redis binding store + query-only 副本）——**dev 上跑不了，没跑**。
- `P-A5-1`（旧化身心跳抢不回 binding）与 `P-A5-3`（聚合端点连跑 20 次稳定）——
  步骤 10/11 是按**指标判据**过的，这两发**没有单独取证记录**。

---

## 9. 🔴 阻塞项清单（离"能部署测试服务拆分"还差什么）

| # | 阻塞项 | 严重度 | 说明与处置 |
|---|---|---|---|
| **SD-B1** | ~~集群里没有 Redis~~ 🔧 **已部署，gateway 侧未接线** | 🟡 **不再阻塞阶段 1 的①；仍差②的接线** | **它挡住过什么（记录保留）**：阶段 1（gateway 直读 Redis）与阶段 3（活跃态折叠进 Redis）全部以它为前提；`P-A5-2「HA 形态」`探针因此结构性地跑不了；滚 scheduler 要付一个数据面 503 窗口。🔧 **2026-08-20 当日进展**：`deploy/k8s/base/redis.yaml`（提交 `77aa98f`）建了 Deployment ＋ Service ＋ PVC 并进了 `kustomization.yaml:11`；`scheduler-deployment.yaml:82-83` 注了 `SCHEDULER_REDIS_ADDR`，日志已是 `binding_store="redis"`。🔴 **还差**：`gateway-deployment.yaml` **没有任何 redis env** ⇒ 阶段 1 的②（gateway 直读）还不能验。🔴 **仍然成立的两条**：**不要把 Redis 凭据发到 node**（那会撤销上一轮 G7 摘掉 node 侧 PG 凭据的成果，拆分提案 §0 硬约束 2）；Redis 是**单副本 ＋ 单 local-path 卷、落在 204**，HA 是敞口（见 SD-B5 与 `redis.yaml` 抬头的 OPEN ITEM）。🔧 **顺带解掉的那条数字改了**：滚动窗口是 **2.3–3.3 秒**，不是 14 秒（§4.5）。🔧 **2026-08-20 晚：本条全解** —— gateway 侧的 addr 已接（`gateway-deployment.yaml:100-101`，集群实测同值），阶段 1 的②已在集群上验过（[`_sd-impl-phase1.md`](_sd-impl-phase1.md) §13.2）；`P-A5-2「HA 形态」`现在**跑得了但仍未跑**（同上 §13.6 第 7 条）|
| **SD-B2** | **`--role` 在代码里不存在** | 🔴 阻塞阶段 3 | `grep -rn "role" src/bin/server.rs` **零命中**。拆 `api`/`node` 的第一刀还没落。集群侧对应的空缺：**没有 `agentenv-api` 的 Deployment 清单**（`deploy/k8s/base/` 只有 gateway/scheduler Deployment + node DaemonSet） |
| **SD-B3** | **`deploy/k8s/base` 与集群实际有 11 处漂移，且 apply 会静默拆掉一半** | 🔴 每次发布都要防 | 十条在 `_impl-plan-control-plane-phase3.md` §6.0.2，本文补了 **D-11**（化身开关会被退回 observe/off）。**拆分要新增 `agentenv-api` 工作负载 ⇒ 迟早绕不开一次 apply。** 有牙的做法（按性价比）：① 给 `kustomization.yaml` 的 `images:` 补 registry 前缀入口，或给 `run.sh` 加 `IMAGE_REGISTRY`/`IMAGE_TAG` env —— D-7 是唯一一条 apply 后会**响亮**失败的；② 主仓 `deploy/agentenv-sg/` 补一个 overlay 承接 `agentenv.toml`（D-1/D-2）、regctl 挂载（D-5）、30800（D-8）、PG（D-9）、化身开关（D-11）；③ 把 §7.4 那六组探针做成 `make` 目标或定时任务 |
| **SD-B4** | **本机 docker 无法直接 push 到 `10.10.10.204:5000`**，且上行只有 190 KB/s | 🟡 已有规避 | `/etc/docker/daemon.json` 的 `insecure-registries` 指的是另一套环境（`10.1.0.106:5000`）。**规避是既定的：全部构建在 204 上做**（§7.1）。要改本机也行（加 insecure registry + 重启 docker），但上行 190 KB/s ⇒ 推一次 runtime 镜像 ~11 分钟，**不值得** |
| **SD-B5** | **PG / RustFS 的 PVC 都是 `local-path` 且钉死 204，5Gi / 100Gi，无备份** | 🟡 | 204 挂掉 = 登记表 + 快照桶一起丢。拆分之后 `api` 是 N 副本、登记表是唯一真相源 ⇒ **这条的暴露面会变大**。至少要在阶段 2（catalog 进 PG）之前谈一次持久化 |
| **SD-B6** | 🔴 **scheduler 缺席超过 `binding_ttl` 的表现是 404，不是 503** | 🔴 **会让阶段 1 的验证探针假通过** | scheduler 缺席 **> 30 秒**（`binding_ttl`，`services/scheduler/internal/store.go:11`）之后恢复，会出现一段 **~13 秒的 404 窗口**，其间 gateway 对一个**活着、健康**的沙箱回答「不存在」，然后自愈。**Redis 不修这条**，`binding_ttl` 才是它的开关。机理与两条后果在 §9.1 |
| **SD-B7** | 🔴 **这套集群做不出「整机猝死」** | 🔴 **一整类探针在这里拿不到取证** | 四条路三条堵死：deny-all NetworkPolicy **不执行**、`kill -STOP 1` 对 PID 1 **静默无效**、`--force --grace-period=0` 被 §7.2 明令禁止、优雅 `delete pod` 撞上 `terminationGracePeriodSeconds: 3600`。⇒ 判据里含「节点真的死了」的探针（F4 的窗口、陈旧路由存活时长、节点侧接管）**只能推，不能实测**。唯一建议路线（**也还没验证**）：挑一台**持零个沙箱**的节点做优雅 delete。机理与证据在 §11.1 / §11.2。🔧 **2026-08-20 夜：本条关闭** —— 猝死做得出来了（§11.6 ③：`taint NoSchedule` → 宿主机 `kill -9` 容器 init → `delete pod --grace-period=1`），并有正面证据（endpoint 掉了、`/nodes` 掉到一条、死节点的路由记录留在 Redis 里）。🔴 **但上面那条"唯一建议路线"本身是陷阱**：节点上还有沙箱时它会自己删干净路由记录，探针扫无可扫、读起来像通过（§11.6 ②） |

### 9.1 🔴 SD-B6 的机理 —— 三段都在代码里

阶段 1 的验证判据是「scheduler 缩到 0 **持续 5 分钟**，运行中沙箱代理请求成功率不变」。
🔴 **那个相位里失败的形状不是 503。** 逐段：

| # | 环节 | 证据 |
|---|---|---|
| ① | 缺席期间没有心跳对账 ⇒ Redis 里的 binding 按自己的 TTL 到期。**Redis 只让记录活过 scheduler 进程，不让它活过 `binding_ttl`** | `services/scheduler/internal/store.go:11`（30s）；对账写在 `redis_store.go:448` |
| ② | 节点侧心跳失败后**指数退避**：5 → 10 → 20 → 40 → 60 秒封顶 ⇒ scheduler 回来之后第一发心跳可能迟到几十秒 | `src/observability/reporter.rs:20`（`MAX_REPORT_BACKOFF`）、`:142` `:151` |
| ③ | scheduler 的 warm-up 闸门**只等 15 秒**，到点就 latch 成 warm；此后 `lookupAbsent` 直接给 `codes.NotFound`，gateway 把它翻成 **404**（而 `Unavailable` 才翻 503） | `services/scheduler/internal/warmup.go:12` `:79-82`；`lookup.go:388-390`；`services/gateway/internal/server.go:373-374` vs `:375-376` |

⇒ ③ 让闸门在 15 秒时开，② 让第一发心跳更晚才到，**中间那段就是 404**。

🔧 **行号复核（2026-08-20 晚，阶段 1 改过这两个文件）** —— 结论一字不变，只是三处引用漂了：

| 原文写的 | 现在在哪（`1f79e8f`） |
|---|---|
| 对账写在 `redis_store.go:448` | 🔧 `:448` 现在是 `redisKeepsDeadlineEphemeral` 那段前言；对账脚本体在 **`redis_store.go:560-618`**，其中 TTL 的那一刀在 **`:587-606`** |
| `lookup.go:388-390` | 🔧 **`lookup.go:391`**（`codes.NotFound`） |
| `gateway/internal/server.go:373-374` vs `:375-376` | 🔧 都搬进了 `writeSchedulerError`：**404 在 `:431-432`，503 在 `:433-434`**（函数从 `:422` 起） |
| `store.go:11`（30s）、`warmup.go:12` `:79-82`、`reporter.rs:20` | ✅ 未动 |

🔴 **两条后果，第二条对本轮更致命：**

1. **客户端侧**：把 404 当终态的客户端会**放弃一个还在跑的沙箱**。
   `warmup.go:22-24` 的注释逐字预见过这件事
   （「tells a client its sandbox no longer exists」），闸门也确实挡住了大部分 ——
   **但它挡不住退避比它长的那一段**。
2. 🔴 **验证侧**：**只数 503 的探针会假通过。** 这正是 §8 第 1 条「探针要有分辨力」
   在本轮的具体形态：探针**必须按状态码分类计数**，不能只判 `!= 200` 或只判 503。
   否则它测的是一个恰好不发生的现象。

**真正的修法是阶段 1 的①**（投影 TTL ＝ 沙箱寿命，不再靠心跳续期）——
⇒ **SD-B6 不是一个待办，它是阶段 1 ① 的兑现判据之一**：
①落地之后，「缺席 5 分钟」这一相位既不该出 503，也不该出 404。

---

**🟢 已经不再是阻塞的**（R3 记过、今天复核已解决）：

| 原编号 | 内容 | 现状 |
|---|---|---|
| R3-B1 | `services/` 无任何 DB 代码 | ✅ scheduler 已直连 PG，写权已上收 |
| R3-B7 | scheduler `:9101` metrics 未进 Service | ✅ 9101 与 gateway 的 9102 都已进 Service |
| R3-B4 | 登记表只有一行 `local_only` | ✅ 表已重建，**0 行**，取样不再受存量污染 |

---

## 10. 本次执行的动作清单（透明起见）

**全部是只读命令**：`kubectl get / logs / exec … psql SELECT`、`curl GET`、`ssh` 读取、`git log/diff`。

- 两次 `kubectl exec agentenv-postgres-0 -- psql … SELECT`（一次 `GROUP BY state`，一次 `information_schema.columns` 计数）。
- 两次 20 MB 的 `ssh + dd` 吞吐测量（`dd if=/dev/zero … | cat > /dev/null`，**两端都写 `/dev/null`**，不落盘）。
- **未执行**任何 `apply` / `delete` / `restart` / `scale` / `set image` / `edit` / `patch` / `port-forward`；
  **未对 `paused_sandboxes` 做任何写操作**；**未创建任何临时 Pod**。

---

## 11. 🔧 第二轮：阶段 1 验收当轮的环境发现（2026-08-20 晚）

> 🔴 **这一轮不是只读** —— 翻过三个开关、`scale` 过 scheduler、`apply` 过一个 NetworkPolicy、
> `DEL` 过 Redis key、建过也删过沙箱。§10 只描述本文最初那一轮。
>
> **本节只写环境**：哪些手法在这套集群上根本不生效、清理时会踩什么。
> 阶段 1 的验收结论、探针结果、新缺陷与射程边界在
> [`_sd-impl-phase1.md`](_sd-impl-phase1.md) §13，**不在这里重复**。

### 11.1 🔴 两条「以为能用、其实是空转」的手法

两条都是想制造「节点失联」时踩到的。**它们不是失败，是无效** —— 用它们跑出来的
「零失败」看着像通过，实际上探针根本没有开火。

| # | 手法 | 实测 | 结论 |
|---|---|---|---|
| **SD-E1** | 给某个 node Pod 打标签、挂一条 deny-all `NetworkPolicy`（`Ingress` ＋ `Egress` 全空），制造「这台节点被网络隔离」 | 覆盖 **40.7 秒**（19:04:14.15Z 挂上 → 19:04:54.86Z 摘掉）：**心跳零失败**，scheduler 侧 `binding_execution_total{decision="refreshed",source="heartbeat"}` 在指标窗口（61 秒，19:04:08–19:05:09Z，完整覆盖这一段）里 **+125**，与「两台 × 5 条记录 / 5 秒心跳 ≈ 2/s ⇒ ≈122」吻合 ⇒ **两台都在报，一台都没掉队**；数据面 **620/620 × 204**；被采样的 binding key 全程 **0 次缺席** | 🔴 **这套 k3s 不执行 NetworkPolicy**：**任何以 netpol 做隔离的探针在这里恒为空转** —— 对象建得出来、`kubectl get netpol` 看得见，就是没人执行。（大概率是起 k3s 时关掉了 netpol 控制器，**没有去核实原因** —— 结论只依赖「挂上去没有任何效果」这一条实测） |
| **SD-E2** | `kubectl exec <node-pod> -- kill -STOP 1`，冻住节点进程 | PID 1 状态**仍是 `Ssl`**（不是 `T`）；心跳零失败 | 🔴 **内核不向同一 PID 命名空间内的 PID 1 投递「默认动作」的信号** —— PID 1 没给 `SIGSTOP` 装 handler，于是信号被直接丢弃。**用它跑的那一发是 no-op，不是证据** |

证据：`$WD/f4-netpol.yaml`、`f4-marks.txt`、`f4-dp.csv`、`f4-ttl.csv`、`m-f4-{a,b}-*.txt`、`m-f4b-{a,b}-*.txt`
（`$WD` 见 [`_sd-impl-phase1.md`](_sd-impl-phase1.md) §13 抬头；**临时目录，会被清掉**）。

🔴 **方法论上这是 §8 第 2 条的又一个形态**：「某指标恒 0」不是证据。
这两发里恒 0 的是**心跳失败数**，而它恒 0 的原因不是系统扛住了，是**手法没生效**。
⇒ **任何声称「隔离/冻结了一台节点」的探针，必须先给出这台节点确实失联的正面证据**
（心跳失败计数涨了、`ListObservedNodes` 里它掉了、或者 PID 状态变成 `T`），
再去看被测对象的表现。

### 11.2 🔴 由此产生的缺口：这套集群**没有**制造「整机猝死」的可用手法

四条路，三条堵死：

| 路 | 状态 |
|---|---|
| deny-all NetworkPolicy | ❌ **不执行**（SD-E1） |
| `kill -STOP 1` | ❌ **静默无效**（SD-E2） |
| `kubectl delete pod --force --grace-period=0` | ❌ **runbook §7.2 明令禁止** —— 强删只删 API 对象、容器还活着，新 Pod 抢不到 `records.db`/`LOCK` 会 CrashLoop |
| 优雅 `delete pod` | 🟡 **有沙箱在跑时不可用**：`terminationGracePeriodSeconds: 3600`（`deploy/k8s/base/agentenv-daemonset.yaml:29`）⇒ 一次删要等一小时 |

🔴 **登记为开放缺口**：凡是判据里含「一台节点真的死了」的探针（F4 的窗口、
节点侧接管、陈旧路由的存活时长），**在这套集群上目前都拿不到取证**，
只能推。**别把推出来的窗口写成实测的**。

🟢 **唯一建议的路线**：挑一台**当前持有零个沙箱**的节点做优雅 `delete pod` ——
3600 秒的 grace 只对「还有沙箱要保」的时候才咬人，空节点上它是无害的。
先用 `/v2/sandboxes` ＋ `placement` 确认它真的是空的（§11.3 的坑正好在这里咬人），
再删。**这条路线本身也还没验证过。**

🔧 **已被 §11.6 取代（2026-08-20 夜）。** 上面这段**两半各对了一半**，原文保留以便对照：
- ✅ 「空节点上 `delete pod` 是无害的」**成立**：实测 1.9 秒返回、53 秒起来、`restarts=0`。
- 🔴 「**这样就能制造猝死**」**不成立** —— 节点上有沙箱时它自己会把路由记录删干净，
  于是探针**扫无可扫、读起来像通过**。真正做得出猝死的三步配方在 §11.6 ③。
- 🔴 「先确认它是空的」这一步的读法也要改：`/v2/sandboxes` ＋ `placement` 是对的，
  **但别用 `/nodes` 的 `sandboxCount`**（§11.3 第二个坑）。

### 11.3 🔴 清理沙箱的坑：`/sandboxes` **不列暂停的沙箱**

- `/sandboxes` 是 deprecated 的，语义就是「列 **running**」（`src/api/openapi.yml:1344-1348`）；
  `/v2/sandboxes` 才是「列全部」，并且带 `state` 过滤（`:1489-1512`）。
- ⚠️ **别照 `/v2/sandboxes` 那个 200 的描述文案判断语义** —— 它仍写着
  `"Successfully returned all running sandboxes"`（`:1521`），与它自己的 summary 冲突，是陈旧文案。

**实际踩到的样子**：一轮清理按 `/sandboxes` 数着删，**三台暂停的沙箱因此活了下来**；
随后一次 node rollout 把它们从持久化存储里恢复成 `Paused`，
数据面一碰就**自动 resume** 了 —— 于是「已经清干净」的集群上凭空出现三台 running 沙箱。

⇒ 已写进 §7.5 的收尾步骤。🔴 **收尾一律照 `/v2/sandboxes` 数**，
并且**删完再数一次** —— 这是唯一能把「暂停的那几台」也算进去的读法。

#### 🔧 第二个坑（2026-08-20 夜新增）：`/nodes` 的 `sandboxCount` 也漏暂停的，**而心跳 roster 不漏**

**实测**：pause 掉一台之后，`/nodes` 的 `sandboxCount` 读 **1**，
同一时刻 `/v2/sandboxes` 是 **2**（一台 running、一台 paused）。

| 面 | 数的是什么 | 出处 |
|---|---|---|
| `/nodes` 的 `sandboxCount` | **只数 VM 还活着的那几个状态**（`Running` ＋ `Pausing`/`Snapshotting`/`Forking`/`Killing`），暂停的记在另一个字段 `sandboxPausedCount` | `src/observability/service.rs:81`；`src/orchestrator/metrics.rs:60-66, 88-96`；`src/api/openapi.yml:1232-1235`（描述逐字「running on the node」）与 `:1250-1253` |
| 🔴 心跳 roster（`sandbox_ids` / `sandbox_roster`） | **整个内存 store，不区分状态** —— 暂停的也在里面 | `src/observability/service.rs:82-83` → `src/orchestrator/service.rs:795`、`:805-823` |

🔴 **两件事因此会被骗**：
1. **「这台机器是空的」** —— 拿 `sandboxCount == 0` 下结论会偏小。删节点、做猝死（§11.6）、
   滚 DaemonSet 之前，**一律照 `/v2/sandboxes` 数**。
2. 🔴 **清扫读的正是 roster 那一份**（含暂停的）⇒ 一个按 `sandboxCount` 设计判据的清扫探针，
   **期望值从一开始就是错的**。



### 11.4 🔧 三个路由投影开关落地了 ⇒ 漂移多出 D-12 / D-13 / D-14

§7.4 ⑦ 原本写的是「尚未进 `deploy/k8s/base`，先别加进例行复核」，**已过时**：
清单在 `gateway-deployment.yaml:129-144` / `scheduler-deployment.yaml:169-174`，
CM literal 在 `kustomization.yaml:120-124`，集群里的 CM 叫 `routing-projection-config`
（**和 `execution-fencing-config` 故意分开**：不同的改动、不同的日子）。

| # | 集群 out-of-band 的东西 | 仓内清单说的是什么 | apply 后的症状 |
|---|---|---|---|
| **D-12 / D-13 / D-14**（新） | CM `routing-projection-config` 三个键 **全 `on`**（2026-08-20 阶段 1 验收后的终态） | `kustomization.yaml:122-124` 的 literal **全 `off`**（发布**起点**，也是代码默认） | 🔴 **静默**：三个开关整体退回 `off`。形状与 D-11 一样、方向相反 |

🔴 **它比 D-11 贵**：退回 `off` 不只是「新行为不生效」——
**再翻回 `on` 要付一次数据面 404 窗口**（实测 2.7 秒 / 15 次 404，
[`_sd-impl-phase1.md`](_sd-impl-phase1.md) §13.4 的 **SD-D1**）。
⇒ 在这套集群上，**一次 `make k8s-apply` 的代价里现在含一次可测量的数据面故障**。

另外两条一并记住（§7.4 ⑦ 已就地写进复核清单）：

- 🔴 **成对约束**：`SCHEDULER_ROUTING_EXECUTION_ARBITRATION=off` 必须同时
  `GATEWAY_ROUTING_PROJECTION_READ=off`。没有任何机制强制它，是运维约束。
- 🔴 **回退只用 `kubectl set env`，绝不删 key**：集群里在跑的 Deployment 上这三条
  `configMapKeyRef` **没有** `optional: true`（仓内清单有，集群那份没有，
  见 `$WD/baseline-env-agentenv-{gateway,scheduler}.json`）⇒
  删 key 是 **Pod 起不来**，不是「落回代码默认」。

### 11.5 「14s → 2.3–3.3s」这条订正的复核

本轮顺手核过一遍，**三处一致、没有内部矛盾**：本文 §4.5（重测表）、
§9 **SD-B1**（顺带解掉的那条）、[`_sd-impl-phase1.md`](_sd-impl-phase1.md) §12 硬前置 **B3**。
`_impl-plan-control-plane-phase3.md` 里的 14s 是**上一轮自己那次测量的记录**，
属于历史，不动它。

🟡 **仍有一处未订正的引用**：`_sd-impl-phase2.md:1116` 还写着
「顺带解掉滚 scheduler 的 **14s** 数据面 503 窗口」。**不归本文改**，登记在这里。

🟢 **销案（2026-08-20 夜）**：那处已就地订正为 2.3–3.3 秒，并在
[`_sd-impl-phase2.md`](_sd-impl-phase2.md) §13.5 **C2** 留了订正记录。全仓再无 14s 的活引用
（`_impl-plan-control-plane-phase3.md` 里那处是历史测量，按原议不动）。

### 11.6 🔧 第三轮（2026-08-20 夜，阶段 2a 验收当轮）：**SD-B7 关闭** —— 猝死做得出来了，但最省事的那条路是陷阱

> 🔴 **这一轮同样不是只读**：taint 过节点、`kill -9` 过宿主进程、删过 Pod、`DEL` 过一次 Redis key、
> 建删过沙箱、播了 30 条快照（**故意留着**，见 [`_sd-impl-phase2.md`](_sd-impl-phase2.md) §13.1）。
> 本节只写**手法**；清扫的验收结论在 [`_sd-impl-phase1.md`](_sd-impl-phase1.md) §13.9，
> 2a 的验收在 [`_sd-impl-phase2.md`](_sd-impl-phase2.md) §13。

§11.2 把 `kubectl delete pod --grace-period=1` 记为「唯一建议、**也还没验证**」的路线。
**验了，而它同时给出两个答案。**

#### ① 🟢 安全性：这条命令本身是安全的（§7.2 的恐惧是专指另一条命令的）

在一台**持零个沙箱**的节点上：

| 项 | 实测 |
|---|---|
| `delete pod --grace-period=1` 返回 | **1.9 秒**（不是 3600 —— `terminationGracePeriodSeconds` 是**上限**，不是等待时间）|
| 替代 Pod ready | **53 秒** |
| `restarts` | **0** |
| `records.db` / `LOCK` 抢占导致的 CrashLoop | **没有出现** |

⇒ 🔧 **§7.2 那条「绝不强删」的纪律，射程是 `--force --grace-period=0`**（只删 API 对象、
容器还活着、新 Pod 抢不到锁），**不覆盖 `--grace-period=1`** —— 后者是正常的优雅删除，
只是把宽限期设成 1 秒。原纪律不变，但别把它读成"任何小 grace 都危险"。

#### ② 🔴 但它**做不出猝死**，而且失败的样子读起来像通过

**节点上有 running 沙箱时**，SIGTERM 一到，节点**反应得够快**（**~4 秒**）就开始它的
关机暂停流程，并且顺手**把自己的路由记录删干净**：

- Redis **被清空**（不是"留下一条陈旧记录"，是**一条都不剩**）；
- 那台沙箱停在 `paused_sandboxes.state='publishing'`。

🔴 **于是用这种方式布置的清扫探针，扫无可扫，然后报"通过"。**
它测的是一个**恰好不发生**的现象 —— 与上一轮记录过的「一个在每个相位都通过的探针」
是同一个失效类（§8 第 1、2 条）。**这条要当陷阱记，不是当选项记。**

#### ③ ✅ 真正做得出猝死的配方（三步，实测有效）

```bash
# 1) 先把这台机器踢出调度，但 🔴 绝不用 NoExecute
kubectl taint nodes <node> k=down:NoSchedule
# 🔴 NoExecute 会把 postgres / redis / rustfs / gateway / scheduler 一起赶走 ——
#    worker-01（204）上跑着这些，一条 NoExecute 就是把整个控制面端了。
#    其中 postgres / rustfs / redis 是**钉死**在 204 的（PVC 全是 local-path，§2 与 §9 SD-B1）；
#    gateway / scheduler 当晚也在 204 上（本轮实测的落点，仓内清单没有 nodeSelector ⇒
#    🔴 动手前自己 `kubectl -n $NS get pod -o wide` 复核一遍，别照抄这句）。
#    用完记得摘： kubectl taint nodes <node> k-

# 2) 从这台节点的【宿主机】上，SIGKILL 容器里的 /server init
#    定位：遍历宿主 PID，在 /proc/<pid>/cgroup 里匹配这个 Pod 的 UID
ssh supos@<node-ip> 'sudo kill -9 <host PID of the container /server init>'

# 3) 再删 Pod（此时容器里已经没有活着的 init，走的就是 ① 那条 1.9 秒的路）
kubectl -n $NS delete pod <node-pod> --grace-period=1
```

🔴 **第 2 步为什么有效，而 §11.1 的 SD-E2 无效**：内核不向**同一 PID 命名空间内**的 PID 1
投递「默认动作」的信号（所以 Pod 内 `kill -STOP 1` 被静默丢弃），
**而祖先命名空间不受这条保护** —— 从宿主机发出的 `SIGKILL` **是会被投递的**。
⇒ SD-E2 那条结论不用改，它只对"在 Pod 内部动手"成立。

🔴 **第 1 步之前怎么确认这台机器"是空的"**：照 `/v2/sandboxes` ＋ `placement` 数，
**不要看 `/nodes` 的 `sandboxCount`** —— 它把暂停的沙箱漏掉了（§11.3 的第二个坑）。

#### ④ 正面证据（§11.1 要求的那种：先证明它真的死了）

| 证据 | 值 |
|---|---|
| `agentenv-nodes` 的 endpoints | **少了这一个** |
| `/nodes` | 从两条**掉到一条** |
| 🔴 死节点的两条路由记录 | **都还在 Redis 里**，TTL **~24 小时** |

第三条同时是两件事：**猝死成功的证据**，以及 **F4 的现场**
（陈旧记录活过了它的节点 —— 严重度见 [`_sd-impl-phase1.md`](_sd-impl-phase1.md) §13.9）。
