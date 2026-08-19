# R3 侦察：pve-sg **dev** 集群现状 + 发布 runbook

调研时间 2026-08-19。**全程只读**（唯一例外见文末「本次执行的动作清单」）。
本文所有结论都附实测命令输出。凭据一律占位符，**不写明文**。

入口：`kubectl --kubeconfig ~/.kube/config-aenv-sg`、`ssh supos@10.10.10.203/204`、
gateway `http://10.10.10.203:30800`。

---

## 1. 集群现状

### 1.1 节点

```
$ kubectl --kubeconfig ~/.kube/config-aenv-sg get nodes -o wide
NAME             STATUS   ROLES           AGE     VERSION        INTERNAL-IP    KERNEL              CONTAINER-RUNTIME
aenv-master-01   Ready    control-plane   4d19h   v1.36.3+k3s1   10.10.10.203   6.8.0-137-generic   containerd://2.3.2-k3s2
aenv-worker-01   Ready    <none>          4d19h   v1.36.3+k3s1   10.10.10.204   6.8.0-137-generic   containerd://2.3.2-k3s2
```

资源余量（gateway `GET /nodes`，AgentENV 自己上报的口径）：

| 节点 | vCPU | 内存 | `/workspace`（hostPath 所在盘） | 沙箱数 |
|---|---|---|---|---|
| `aenv-worker-01` | 16 | 2.9G / 31.3G | 124G used / 290G（**余 165G**） | 0 |
| `aenv-master-01` | 8 | 1.6G / 15.6G | 19G used / 96G（**余 77G**） | 0 |

两节点 `status: ready`，`sandboxCount: 0`，**当前没有任何活沙箱**（`GET /sandboxes` ⇒ `[]`）。
gateway `/health` ⇒ **204** ✅。

### 1.2 `agentenv-system` 工作负载清单

```
$ kubectl get all -n agentenv-system
pod/agent-console-5ffcbb496f-k94dn       1/1 Running   worker
pod/agentenv-gateway-6556d8b9d8-4mz5x    1/1 Running   worker
pod/agentenv-node-gspxc                  1/1 Running   worker
pod/agentenv-node-stcmb                  1/1 Running   master
pod/agentenv-postgres-0                  1/1 Running   worker    ← 🔴 见 §2
pod/agentenv-scheduler-6b7c5f988-nb7tw   1/1 Running   worker
pod/rustfs-7b5555c7f4-ffcbb              1/1 Running   worker
```

| 工作负载 | 类型 | 副本 | 镜像（spec 里写的） | pullPolicy |
|---|---|---|---|---|
| `agentenv-node` | DaemonSet | 2/2 | `agentenv-runtime:latest` | IfNotPresent |
| `agentenv-gateway` | Deployment | 1/1 | `agentenv-gateway:latest` | IfNotPresent |
| `agentenv-scheduler` | Deployment | 1/1 | `agentenv-scheduler:latest` | IfNotPresent |
| `agentenv-postgres` | **StatefulSet** | 1/1 | `postgres:17-alpine` | — |
| `rustfs` | Deployment | 1/1 | `rustfs/rustfs:latest` | — |
| `agent-console` | Deployment | 1/1 | `10.10.10.204:5000/agent-console:sandbox-all-732fe241` | — |

Service：`agentenv-gateway`(CI 8080) / `agentenv-gateway-nodeport`(30800) /
`agentenv-nodes`(headless 8000) / `agentenv-scheduler`(9090) /
`agentenv-postgres`(CI 5432) / `rustfs`(9000,9001) / `rustfs-nodeport`(30900,30901) /
`agentenv-ublk-daemon-metrics`(headless 9103) / `agent-console`(30895)。

ConfigMap：`agentenv-k8s-config`（节点 `agentenv.toml`）、`gateway-k8s-config`、
`scheduler-k8s-config`、`sandbox-proxy-config`、`regctl-config`。
Secret：`agentenv-postgres`（key `POSTGRES_PASSWORD` + `dsn`）、
`agentenv-runtime-secrets`（`sandbox-access-token-hash-seed`）、`rustfs-credentials`。

PVC：`data-agentenv-postgres-0` 5Gi / `rustfs-data` 100Gi / `agent-console-audit` 1Gi，
全是 `local-path`（**节点本地盘**，都钉在 worker）。

### 1.3 🔴 镜像来源：registry **可用**，但当前跑的这版是 ctr 侧的本地名

两条路都通，**当前用的是本地名**：

- **registry 是活的**，且本机就能直连：
  ```
  $ curl http://10.10.10.204:5000/v2/_catalog
  {"repositories":["agent-console","agentenv-gateway","agentenv-runtime","agentenv-scheduler",
   "mock-bff","mock-tier0","probe/fresh","uns-agent-platform","uns-agent-worker",
   "uns-build-deploy","uns-swe/sandbox-runtime","uns-swe/sandbox-scaffold","uns-test-console"]}
  $ curl http://10.10.10.204:5000/v2/agentenv-gateway/tags/list
  {"tags":["isolation","merge-abe1bbd","isolation3","latest","isolation2"]}
  ```
- **两个节点的 k3s 都已信任它**（备忘里那条 2026-08-18 的 `registries.yaml` 是真的、还在）：
  ```
  $ ssh supos@10.10.10.203 sudo cat /etc/rancher/k3s/registries.yaml   # 204 输出逐字相同
  mirrors:
    "10.10.10.204:5000":
      endpoint: ["http://10.10.10.204:5000"]
  configs:
    "10.10.10.204:5000":
      tls: {insecure_skip_verify: true}
  ```
- 构建机 204 的 docker 也信任它：`/etc/docker/daemon.json` = `{"insecure-registries":["10.10.10.204:5000"]}`；
  registry 本体 `aenv-registry`（`registry:2`）Up 3 天，`10.10.10.204:5000->5000/tcp`。

**当前实际在跑的镜像**（`status.containerStatuses`，按 config digest 对账）：

| Pod | 上报的 image | imageID (config digest) |
|---|---|---|
| gateway | `docker.io/library/agentenv-gateway:latest` | `sha256:3e6669…` |
| scheduler | `docker.io/library/agentenv-scheduler:latest` | `sha256:a5b462…` |
| node（worker） | `docker.io/library/agentenv-runtime:latest` | `sha256:947f9a…` |
| node（master） | `10.10.10.204:5000/agentenv-runtime:merge-abe1bbd` | `sha256:947f9a…` |

两个 node Pod **image 名不同但 imageID 相同** —— 同一份内容，只是 kubelet 报了不同的引用名
（DaemonSet spec 里写的是 `agentenv-runtime:latest`）。

对账到 registry（`ctr images ls` 的 manifest digest）：
`docker.io/library/agentenv-{runtime,gateway,scheduler}:latest`
= `749b4791` / `18fd7541` / `a1e4512e`
= registry 的 `10.10.10.204:5000/agentenv-*:merge-abe1bbd` 逐个相同
⇒ **三个服务跑的都是 `merge-abe1bbd`，与 submodule HEAD `abe1bbd` 一致**。

🔴 **`:latest` 在 203 上是脏的**：203 的 containerd 里
`10.10.10.204:5000/agentenv-gateway:latest` = manifest `4329f73c`，而 registry 现在的 `latest`
是 `18fd7541`。配合 `imagePullPolicy: IfNotPresent`，**任何用 `:latest` 的 `set image` 在 203 上
都会静默用到旧内容**。⇒ 发布一律用不可变 tag（见 §3）。

---

## 2. 🔴 PostgreSQL 现状 —— **已经有了，而且 paused registry 已经在用**

这是本次最重要的发现：**不需要补 PG，阶段 0 的读侧数据源今天就在跑**。

### 2.1 PG 在哪

集群内，`agentenv-system` 命名空间的 StatefulSet：

```
statefulset.apps/agentenv-postgres   1/1   43h   postgres:17-alpine
  env POSTGRES_USER = aenv
  env POSTGRES_DB   = aenv
  env POSTGRES_PASSWORD <- secretKeyRef agentenv-postgres/POSTGRES_PASSWORD
  env PGDATA        = /var/lib/postgresql/data/pgdata
  nodeSelector: kubernetes.io/hostname=aenv-worker-01     ← 🔴 PVC 是 local-path，钉死 worker
  PVC data-agentenv-postgres-0  5Gi  local-path
Service agentenv-postgres  ClusterIP 10.43.224.84:5432
```

版本 `PostgreSQL 17.11 on x86_64-pc-linux-musl`，库大小 7726 kB，当前 5 个连接。

### 2.2 DSN 配置项与接线（源码 → ConfigMap → Secret）

- 源码：`src/cfg.rs:382-396` `PausedRegistryBackendKind{Local,Postgres}`，
  `#[config(default = "local")]`；实现在 `src/orchestrator/paused_registry/postgres.rs`。
- 集群 ConfigMap `agentenv-k8s-config` 的 `agentenv.toml` **末尾**：

  ```toml
  [orchestrator.paused_registry]
  backend = "postgres"
  max_connections = 8
  reconcile_interval_secs = 30
  lease_ttl_secs = 90
  # DSN carries credentials and comes from AENV_PAUSED_REGISTRY_DSN.
  ```
- DaemonSet `agentenv-node` 注入：
  `env AENV_PAUSED_REGISTRY_DSN <- secretKeyRef {name: agentenv-postgres, key: dsn}`
- DSN 形状（凭据已遮蔽）：
  `postgres://<USER>:<PASSWORD>@agentenv-postgres.agentenv-system.svc.cluster.local:5432/aenv`

**运行时自证**（两个 node Pod 启动日志都有）：

```
INFO agentenv::orchestrator::paused_registry::postgres:
     paused sandbox registry ready cluster_id=00000000-…-0000 lease_ttl_secs=90.0
```

### 2.3 `paused_sandboxes` 表：存在，1 行

```
$ kubectl exec -n agentenv-system agentenv-postgres-0 -- psql -U aenv -d aenv -c '\dt'
 public | paused_sandboxes | table | aenv      ← 全库只有这一张表

$ ... -c '\d paused_sandboxes'
 sandbox_id uuid NOT NULL (PK) | cluster_id uuid NOT NULL | state text NOT NULL
 generation bigint NOT NULL | origin_node_id text NOT NULL | snapshot_id uuid
 metadata jsonb NOT NULL | paused_at timestamptz NOT NULL | updated_at timestamptz NOT NULL
 claimed_by_node_id text | lease_expires_at timestamptz | sandbox_expires_at timestamptz
 Indexes: pkey(sandbox_id), origin_node_idx(origin_node_id), updated_at_idx(updated_at)
 Check: state IN ('publishing','paused','resuming','local_only','running')

$ ... -c 'SELECT state, count(*) FROM paused_sandboxes GROUP BY state;'
   state    | count
------------+-------
 local_only |     1
```

那一行：

```
sandbox_id  01a01853-30a6-75a3-8b25-eb87b3db0265
state       local_only      generation 3      origin_node_id aenv-worker-01
snapshot_id (NULL)          paused_at 2026-08-19 05:13:41Z
claimed_by_node_id (NULL)   lease_expires_at 2026-08-19 10:07:02Z
sandbox_expires_at 2026-08-19 05:43:11Z        ← 已过期
```

**它为什么是 `local_only`**（node 日志坐实，与 kb 里 rustfs 并发 503 那条一致）：

```
WARN pause_sandbox: agentenv::api::impls::paused_coordinator:
  failed to publish paused snapshot; it stays resumable on this node only
  error=backend error: upload managed layer '…/mem_overlaybd/overlaybd.commit'
  sandbox_id=01a01853-30a6-75a3-8b25-eb87b3db0265
```

⇒ **快照发布到 RustFS 失败 ⇒ 降级 `local_only`**。同一时段其它沙箱的
`oss file uploaded … artifact="memory_layer" size_bytes=126115840` 是成功的，
所以不是「OSS 后端没配好」，是**大层上传偶发失败**。

> 阶段 0/1 的取样含义：现在库里只有 1 行、且是降级态。要造出
> `paused` / `publishing` / `resuming` 各态样本，得跑真沙箱 pause/resume（见 §4）。

### 2.4 怎么连上它（可复现）

**A. 集群内 / 最省事（已实测通过，本次所有 SQL 都走这条）**

```bash
export KUBECONFIG=~/.kube/config-aenv-sg
kubectl exec -n agentenv-system agentenv-postgres-0 -- \
  psql -U aenv -d aenv -c 'SELECT state, count(*) FROM paused_sandboxes GROUP BY state;'
# Pod 内 psql 用 local socket + trust，无需密码
```

**B. 从本机（port-forward）—— TCP 通路已实测，客户端本机缺**

```bash
export KUBECONFIG=~/.kube/config-aenv-sg
kubectl port-forward -n agentenv-system svc/agentenv-postgres 15432:5432 &
PGPASSWORD="$(kubectl get secret agentenv-postgres -n agentenv-system \
  -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 -d)" \
  psql -h 127.0.0.1 -p 15432 -U aenv -d aenv -c '\dt'
```

已实测：port-forward 后发 PG SSLRequest ⇒ 服务端回 `N`（明文可用），
**证明通路是真 PG**。⚠️ 本机**没装** `psql` / `pgcli` / `pg8000`，
要走 B 得先装其中之一（或用 `kubectl run --rm` 起个带 psql 的临时 Pod）。

**C. 阶段 0 里 scheduler 要用的地址**（同 namespace，直接 ClusterIP）

```
postgres://<user>:<password>@agentenv-postgres.agentenv-system.svc.cluster.local:5432/aenv
```
凭据从**已存在的** `agentenv-postgres` Secret 的 `dsn` key 取即可，不必新建。

### 2.5 🔴 阶段 0 的真实缺口：不在 PG，在 **Go 侧没有任何数据库代码**

```
$ grep -rn "postgres|pgx|lib/pq|database/sql|paused_sandboxes" services/ --include=*.go --include=go.mod
（无任何命中）

$ head -15 services/go.mod
module agentenv/services
go 1.25.0
require (
  github.com/hashicorp/golang-lru/v2  github.com/prometheus/client_golang
  github.com/redis/go-redis/v9        go.uber.org/zap
  google.golang.org/grpc              google.golang.org/protobuf
  k8s.io/api  k8s.io/apimachinery  k8s.io/client-go
)
```

paused registry 的实现**只在 Rust 侧**（`src/orchestrator/paused_registry/postgres.rs`），
gateway/scheduler 这两个 Go 服务对 PG 一无所知。scheduler 当前启动日志：

```
scheduler gRPC server listening addr=:9090 strategy=round_robin binding_store=memory query_only=false
```

⇒ **阶段 0 要做的是给 `services/` 引入 PG driver（新依赖 + go.sum）**，
配一个 DSN env（复用现成 Secret），再加只读查询。PG 本身零工作量。

---

## 3. 发布 / 部署流程

### 3.1 目录职责

| 目录 | 归属 | 管什么 | 这套集群在用吗 |
|---|---|---|---|
| `apps/AgentENV/deploy/k8s/`（base + overlays/default、local-dev） | submodule | 上游 kustomize：ns / SA / role / 三个工作负载 / 四个 ConfigMap | ✅ 集群对象由它建出来 |
| `apps/AgentENV/deploy/docker/` | submodule | 三个 Dockerfile（`.agentenv` Rust / `.gateway` Go / `.scheduler` Go） | ✅ 构建走它 |
| **主仓** `deploy/agentenv-sg/` | uns-swe 主仓 | `rustfs.yaml`（RustFS 全套）+ `patch-node-config.sh`（把节点 ConfigMap 调到目标态） | ✅ 本环境专属增量 |

`agentenv-postgres`（StatefulSet + Service + Secret）与 `agentenv-gateway-nodeport`
**两边都没有清单文件** —— 是手工建的集群对象，只活在集群里。
（`kubectl get sts agentenv-postgres -o yaml` 是目前唯一的真相源。**建议补一份 yaml 进主仓 `deploy/agentenv-sg/`**。）

### 3.2 🔴 `make k8s-apply` 的回退风险：**还在，而且比备忘记的更严重（三条，不是一条）**

机制（`deploy/k8s/run.sh` 第 30 行 + `deploy/k8s/base/kustomization.yaml`）：

```bash
# run.sh
cp -R "${SCRIPT_DIR}" "${TEMP_DIR}/k8s"
cp "${REPO_ROOT}/config/default.toml" "${TEMP_DIR}/k8s/base/config/agentenv.toml"   # ← 关键
...
kubectl apply -k "${OVERLAY_PATH}"
```
```yaml
# base/kustomization.yaml
configMapGenerator:
  - name: agentenv-k8s-config
    files: [config/agentenv.toml]
generatorOptions:
  disableNameSuffixHash: true        # ← 同名覆盖，不是新建
```

`deploy/k8s/base/config/` 里**只有** `gateway.json` / `scheduler.json`，
`agentenv.toml` 是 run.sh 每次从 `config/default.toml` 现场拷进去的。
`disableNameSuffixHash: true` ⇒ 直接覆盖同名 ConfigMap。

对照 `config/default.toml`（HEAD `abe1bbd`），`make k8s-apply` 会打掉：

| # | 集群现值 | apply 后 | 后果 |
|---|---|---|---|
| 1 | `repository_backend = "oss"` + `[backend.oss]` 指 RustFS | `repository_backend = "posix_fs"`（default.toml:213） | 模板回到节点本地，两节点各自一份，约一半建沙箱请求落到没模板的节点 |
| 2 | `capacity_gb=24` / `remote_blocks.max_size_gb=12` / `oss.cache_max_size_gb=8`（合 44G） | `100` / `100`（合 200G+） | master 只有 90G 可用，GC 高水位 95G **永远触发不到** ⇒ 盘先满 |
| 3 | **`[orchestrator.paused_registry] backend = "postgres"`** | **整节消失** ⇒ 回落 `#[config(default = "local")]`（`src/cfg.rs:394`） | 🔴 **PG registry 静默关掉**，pause 变节点本地、跨节点恢复失效，**且不报错** |

第 3 条是这次新发现的 —— `config/default.toml` 里**根本没有** `[orchestrator.paused_registry]`
（`grep` 零命中），所以不是"改回旧值"，是"配置项消失、回落默认值"。**对阶段 0 是致命的**：
读侧还在读 PG，但写侧不再写了，症状是"表不再增长"，不是报错。

**规避方式（三选一，推荐前两条）**

1. **别在 submodule 里跑 `make k8s-apply`。** 改 Go 代码的发布路径完全不需要它（§3.3 全程只 `set image`）。
2. 万一必须 apply（改了 base 清单结构），**apply 完立刻**跑主仓的
   `deploy/agentenv-sg/patch-node-config.sh` 复原第 1、2 条，**并手工补回第 3 条**
   （🔴 该脚本**只处理 oss + 缓存预算，不认识 paused_registry**，见脚本文件头）。
   安全姿势：apply 前先备份
   `kubectl -n agentenv-system get cm agentenv-k8s-config -o yaml > /tmp/cm-backup.yaml`。
3. 根治：把这三条写进 `config/default.toml`（会污染上游 diff），或给主仓
   `deploy/agentenv-sg/` 加一个 overlay 承接 `agentenv.toml`。

### 3.3 改了 `services/` Go 代码后怎么发到这套集群（端到端）

**构建在哪**：`10.10.10.204`（16C，直连公网快；国内→10.10.10.x 只有 ~70KB/s，
**不能在本机构建再传**）。Go 服务是多阶段 Docker 构建，构建机不需要装 Go。

Dockerfile 形状（`deploy/docker/Dockerfile.gateway`，scheduler 同构 + 多一个 grpc-health-probe 阶段）：

```dockerfile
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY services/go.mod services/go.sum ./
RUN go mod download
COPY services/ .
RUN CGO_ENABLED=0 go build -o /out/gateway ./gateway/cmd
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/gateway /gateway
COPY deploy/docker/config/default.json /config/default.json
ENTRYPOINT ["/gateway"]
CMD ["-config", "/config/default.json"]
```

🔴 **构建 context 是仓库根**（要 `services/` 和 `deploy/docker/config/`），
`-f deploy/docker/Dockerfile.gateway .`，不能只送 `services/`。
Go 侧只有 42 个 `.go` 文件，构建远快于 Rust 的 `Dockerfile.agentenv`。

#### Runbook（命令级；`<TAG>` 用不可变 tag，例如 `pgread-$(git rev-parse --short HEAD)`）

```bash
# ── 0) 构建机对齐源码（🔴 tests/fixtures 是 root 属主，checkout 会静默漏文件）
ssh supos@10.10.10.204
cd /opt/AgentENV
sudo git -c safe.directory=/opt/AgentENV fetch fork dev
sudo git -c safe.directory=/opt/AgentENV checkout --detach FETCH_HEAD
sudo git -c safe.directory=/opt/AgentENV status -s        # 复核：缺文件就 checkout -- <path> 补
# 当前基线：HEAD = abe1bbd（detached），remotes: fork=suixinio, origin=kvcache-ai

# ── 1) 构建 + 推 registry（只建改了的那个；两个都改就都建）
TAG=pgread-$(sudo git -c safe.directory=/opt/AgentENV rev-parse --short HEAD)
cd /opt/AgentENV
sudo docker build -f deploy/docker/Dockerfile.gateway \
  -t 10.10.10.204:5000/agentenv-gateway:$TAG .
sudo docker build -f deploy/docker/Dockerfile.scheduler \
  -t 10.10.10.204:5000/agentenv-scheduler:$TAG .
sudo docker push 10.10.10.204:5000/agentenv-gateway:$TAG
sudo docker push 10.10.10.204:5000/agentenv-scheduler:$TAG
```

```bash
# ── 2) 发布：走 registry 路（两节点 registries.yaml 已信任，无需再 ctr import）
export KUBECONFIG=~/.kube/config-aenv-sg
kubectl -n agentenv-system set image deploy/agentenv-gateway \
  gateway=10.10.10.204:5000/agentenv-gateway:$TAG
kubectl -n agentenv-system set image deploy/agentenv-scheduler \
  scheduler=10.10.10.204:5000/agentenv-scheduler:$TAG
kubectl -n agentenv-system rollout status deploy/agentenv-gateway   --timeout=180s
kubectl -n agentenv-system rollout status deploy/agentenv-scheduler --timeout=180s

# 验证真的换了（比 imageID，别只看 image 名）
kubectl -n agentenv-system get pods -l app.kubernetes.io/name=agentenv-gateway \
  -o jsonpath='{.items[*].status.containerStatuses[*].imageID}{"\n"}'
```

🔴 **两条不许省的纪律**
- **不可变 tag，绝不用 `:latest`**：`imagePullPolicy: IfNotPresent` + 203 上已缓存一份
  过期的 `…/agentenv-gateway:latest`（manifest `4329f73c` ≠ registry 的 `18fd7541`）
  ⇒ 用 `:latest` 在 203 上会静默跑旧代码。
- **`set image` 不碰 ConfigMap** ⇒ 天然规避 §3.2 那三条回退。**这就是首选发布路径。**

```bash
# ── 3) 回滚（两种，任选）
kubectl -n agentenv-system rollout undo deploy/agentenv-gateway          # 回上一个 ReplicaSet
# 或显式钉回已知好版本（三个服务当前跑的都是它）：
kubectl -n agentenv-system set image deploy/agentenv-gateway \
  gateway=10.10.10.204:5000/agentenv-gateway:merge-abe1bbd
# 退路 tag（registry 里现存）：merge-abe1bbd（当前）/ isolation3（合并上游前）/ isolation2 / isolation
```

**改了 Rust（`src/`，即 node）时**：镜像是 `Dockerfile.agentenv`（cargo-chef，首次 ~5-8min），
同样 build → push → `kubectl set image ds/agentenv-node agentenv=…:$TAG`。
🔴 但 DaemonSet 的 `terminationGracePeriodSeconds = 3600`，节点上有 **running** 沙箱时旧 Pod
会长时间 `Terminating`，rollout 卡住 ⇒ **先把沙箱 pause 掉再滚**，且**绝不 `--force --grace-period=0`**
（强删只删 API 对象、容器还活着，新 Pod 抢不到 `records.db/LOCK` 会 CrashLoop）。
本次调研时刻两节点 `sandboxCount: 0`，是滚 node 的好窗口。

**备用路（registry 不可用时）**：`docker save` → 两台各
`sudo k3s ctr -n k8s.io images import`。`make k8s-load-dev` 就是干这个，但它只 import 到
**执行机本地**（204），**203 要单独做**。registry 路已验证可用，优先用它。

---

## 4. 验证手段

### 4.1 `aenv` CLI

- 位置：**只有 203 上有** —— `/home/supos/.local/bin/aenv`，`aenv 0.1.2`。
  🔴 **不在非交互 SSH 的 PATH 里**，必须写全路径（直接 `ssh supos@… aenv …` 会 `command not found`）。
  204 上没有 aenv，也没有 `~/.config/aenv/`。
- 凭据：`~/.config/aenv/credentials`，**TOML**（不是 env 文件），两行：
  ```toml
  url = "http://10.10.10.203:30800"
  api_key = "<REDACTED>"
  ```
  （gateway 鉴权是空壳：任意非空 key 都放行；只有节点 Pod `:8000` 不带 key 会 401。）
- 子命令（`aenv --help` 实测）：
  `auth / pull / build / start / exec / upload / download / connect / pause / resume /
   list(ls) / delete(rm) / timeout / snapshot(snap) / template`
  ⚠️ **没有 `sandbox` 这一级** —— 列沙箱是 `aenv list`，不是 `aenv sandbox list`。

```bash
ssh supos@10.10.10.203 '~/.local/bin/aenv template list'     # ✅ 实测通过
ssh supos@10.10.10.203 '~/.local/bin/aenv list'              # 沙箱列表
```

### 4.2 模板：两个都在，都 ready

```
$ curl -H 'X-API-Key: dummy' http://10.10.10.203:30800/templates
templateID                            names                    cpu mem   buildStatus spawnCount
01a008fd-3c39-7201-babe-5051df0453a4  uns-scaffold-v3          2   4096  ready       0
01a008fd-3c28-7853-8523-81f29b876816  uns-sandbox-runtime-v3   2   4096  ready       0
```

`diskSizeMB: 65536`，`envdVersion: 0.5.15`，`createdAt 2026-08-16T05:13Z`。
⚠️ `spawnCount: 0` / `lastSpawnedAt: null` —— 这两个计数器像是随 node 进程重启归零
（今天 02:52 / 03:09 两节点都重启过），**不能拿它推断"没人用过这两个模板"**。

### 4.3 端到端验证入口

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://10.10.10.203:30800/health   # 204 = 正常 ✅
curl -H 'X-API-Key: dummy' http://10.10.10.203:30800/nodes                  # 两节点 ready
curl -H 'X-API-Key: dummy' http://10.10.10.203:30800/sandboxes              # 当前 []
```

建沙箱做 pause/resume 取样时🔴**两个必传字段**（漏了不报错，但会把你带偏）：
`"timeout": <秒>`（不传吃 15s 默认 TTL，沙箱几十秒内自己消失）、
`"autoResume":{"enabled":true}`（不传则 paused 后数据面恒 410）。

RustFS 控制台 `http://10.10.10.204:30901/rustfs/console/` ⇒ **200** ✅（S3 `:30900` ⇒ 403 = 正常，未签名）。
Agent-Console 运维后台：`http://10.10.10.203:30895`（NodePort，无登录，只作本地排障用）。

### 4.4 日志与 metrics

```bash
export KUBECONFIG=~/.kube/config-aenv-sg
kubectl logs -n agentenv-system deploy/agentenv-gateway   -f --tail=100   # JSON 行
kubectl logs -n agentenv-system deploy/agentenv-scheduler -f --tail=100   # JSON 行
kubectl logs -n agentenv-system -l app.kubernetes.io/name=agentenv-node --tail=200 --prefix
# node 是 Rust tracing（带 ANSI 色码，grep 前建议 sed 去色）

# 阶段 0 最有用的一条：paused registry 是否接上 PG
kubectl logs -n agentenv-system -l app.kubernetes.io/name=agentenv-node --tail=500 --prefix \
  | grep -a "paused_registry\|paused sandbox registry\|stays resumable on this node only"
```

metrics：

| 端点 | 地址 | 现状 |
|---|---|---|
| 节点运行时 | node Pod `:8000/metrics`（要 `X-API-Key`） | ✅ 实测 200（集群内 curl） |
| ublk daemon | node Pod `:9103`，headless svc `agentenv-ublk-daemon-metrics` | Service 已暴露 |
| scheduler | Pod `:9101`（日志：`scheduler metrics server listening addr=:9101`） | 🔴 **Service 只开了 grpc 9090**，要 `kubectl port-forward` 才够得到 |
| gateway | `30800/metrics` ⇒ **404** | 上游 `abe1bbd` 有"gateway 代理沙箱路由的 `/metrics`"，但**不是 gateway 自身指标**；gateway 容器只声明了 8080/http |

```bash
kubectl port-forward -n agentenv-system deploy/agentenv-scheduler 9101:9101
curl -s localhost:9101/metrics | head
```

---

## 5. 阻塞项 / 待办

| # | 事项 | 严重度 | 说明 |
|---|---|---|---|
| B1 | **`services/` 无任何 DB 代码**（`go.mod` 无 pg driver） | 🔴 阻塞阶段 0 | 需引入 pgx/lib-pq + go.sum + DSN env（复用现成 `agentenv-postgres` Secret 的 `dsn` key）。PG 侧零工作量 |
| B2 | `make k8s-apply` 会**静默删掉** `[orchestrator.paused_registry]` ⇒ 回落 `local` | 🔴 会悄悄废掉阶段 0 | 见 §3.2。规避：Go 发布只用 `set image`；必须 apply 时先备份 ConfigMap，且 `patch-node-config.sh` **不认识**这一节，要手工补 |
| B3 | `agentenv-postgres` 与 `agentenv-gateway-nodeport` **无清单文件** | 🟡 | 只活在集群里，重建集群会漏。建议导出到主仓 `deploy/agentenv-sg/` |
| B4 | 唯一的 registry 行是 `local_only`（RustFS 大层上传失败降级） | 🟡 取样受限 | 造 `paused`/`publishing`/`resuming` 样本得跑真沙箱；大层 pause 可能再次降级 |
| B5 | `agentenv-postgres` PVC 是 `local-path` + 钉死 worker，**5Gi、无备份** | 🟡 | worker 挂了 registry 全丢。阶段 1 若把它当真相源，要先谈持久化 |
| B6 | 203 的 containerd 缓存着过期的 `…:latest` | 🟡 已有规避 | 发布一律用不可变 tag（§3.3） |
| B7 | scheduler `:9101` metrics 未进 Service | 🟢 | 要观测得先 port-forward，或补 Service 端口 |
| B8 | 构建机 `/opt/AgentENV` 有 12 个未跟踪文件（`*.prealign`、`build-*.log`、`.cargo-test/`） | 🟢 | 不影响构建（Dockerfile 只 COPY `services/`、`deploy/docker/`），但下次 checkout 前值得清 |

---

## 本次执行的动作清单（透明起见）

除以下一项外，全部是只读命令（`get` / `logs` / `exec … psql SELECT` / `curl GET` / `ssh` 读取）：

- **起过一个临时 Pod**：`kubectl run --rm -i --restart=Never --image=curlimages/curl … probe-ro-996874`，
  用途仅为从集群内 curl 节点 `:8000/metrics`（因该端口无 NodePort）。命令自带 `--rm`，
  已确认输出 `pod "probe-ro-996874" deleted`。**未改动任何既有集群对象**。
- 两次短暂 `kubectl port-forward`（本地代理，不改集群状态），用完即 kill。

未执行任何 `apply` / `delete` / `restart` / `scale` / `set image` / `edit`，
未对 `paused_sandboxes` 做过任何写操作。
