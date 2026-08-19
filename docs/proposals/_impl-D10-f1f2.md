# D10 实现记录：F1 / F2 两条静默失败缺口

> 2026-08-19 · 分支 `central-control-plane-phase2`（基线 HEAD `40a4526`）
> 上位：[`_verify-T3-phase2.md`](_verify-T3-phase2.md) §7 F1 / F2
> 相关裁决：[`_impl-D6-scheduler.md`](_impl-D6-scheduler.md) §5.2（"注册但留冷"，本轮**未推翻**）

---

## 0. 一页速览

| | F1 写面 cluster 作用域无人提供 | F2 `central` 后端无装配日志 |
|---|---|---|
| 根因 | 值待在一个**没人创建**的 Secret key 里 | 三个后端里只有 `postgres` 打了 ready 日志 |
| 修法 | 值搬进 base 层 `configMapGenerator`，**node 与 scheduler 读同一个 key** | 三个后端**合并成一条**装配日志，打在 `match` 之后 |
| 新增测试 | `TestOneClusterIdentityReachesBothSidesOfTheRegistry`（Go，读清单）+ `/healthz` 带 `cluster_id` | `every_backend_reports_which_one_it_is`（Rust）+ backend 值 fail-closed 断言 |
| 变异 | 4 条（M1–M4）+ 1 条（M5） | 3 条（M6–M8）+ 2 条（M9–M10） |
| D6 §5.2 | 不变：缺 cluster id 仍是 **注册但留冷 + UNAVAILABLE**，不 Fatal | —— |

改动 10 个文件，未连集群、未 `make k8s-apply`、未 commit / push。

---

## 1. F1 —— 写面的 cluster 作用域

### 1.1 改了什么

| 文件 | 改动 |
|---|---|
| `deploy/k8s/base/kustomization.yaml` | 新增 `configMapGenerator` 条目 `cluster-identity-config`，字面量 `CLUSTER_ID=00000000-0000-0000-0000-000000000000` |
| `deploy/k8s/base/scheduler-deployment.yaml` | `SCHEDULER_REGISTRY_CLUSTER_ID` 从 `secretKeyRef{agentenv-postgres, cluster_id}` 改成 `configMapKeyRef{cluster-identity-config, CLUSTER_ID}` |
| `deploy/k8s/base/agentenv-daemonset.yaml` | 新增 `AENV_CLUSTER_ID`，**读同一个 ConfigMap 的同一个 key** |
| `config/default.toml` | `[node_identity].cluster_id` 上方写清它与 ConfigMap 的关系，以及它是"ConfigMap 缺席时的回落值" |
| `services/scheduler/cmd/main.go:419` | `/healthz` 的 `registry_write` 里增加 `cluster_id` 字段 |
| `docs/src/deployment/kubernetes.md` | 新增 `### Cluster identity` 一节 + 装配日志说明 |

### 1.2 为什么这么改

**（a）cluster id 不是机密，Secret 是错的容器。** 它是集群的名字，不是凭据。放进 Secret 的直接后果就是 T3 撞到的那个：`agentenv-postgres` 这个 Secret 由数据库那一侧创建，没有任何清单 / 脚本 / helper 会往里加 `cluster_id` 这个 key，于是全新集群上这个值**永远不存在**，写面永久留冷。

**（b）"两边必须同一个值"这件事要在清单里看得出来。** 现在两个工作负载引用的是同一个 ConfigMap 名 + 同一个 key 名，`kustomize build` 里逐字可读：

```
$ kubectl kustomize deploy/k8s/overlays/default | grep -B3 -A3 cluster-identity-config
  CLUSTER_ID: 00000000-0000-0000-0000-000000000000
kind: ConfigMap
metadata:
  name: cluster-identity-config
...
        - name: SCHEDULER_REGISTRY_CLUSTER_ID     # scheduler
          valueFrom: {configMapKeyRef: {key: CLUSTER_ID, name: cluster-identity-config, optional: true}}
...
        - name: AENV_CLUSTER_ID                   # node
          valueFrom: {configMapKeyRef: {key: CLUSTER_ID, name: cluster-identity-config, optional: true}}
```

node 侧的 cluster id 来源已核实：`src/cfg.rs` `NodeIdentityConfig.cluster_id`（`#[config(env = "AENV_CLUSTER_ID")]`），无 env 时取 `config/default.toml` 的 `[node_identity].cluster_id`；解析失败或缺失落到 `Uuid::nil()`（`src/identity.rs:52`），恰好等于 toml 里那个全零值。所以**回落路径与 ConfigMap 值一致**，这个一致性由测试锁住（见 §1.3）。

**（c）没有从 heartbeat 推断。** 值只有一条来源：清单里那一行字面量。heartbeat 里的 `cluster_id` 是被管理对象的自述，方向反了，本轮一个字都没碰。

**（d）D6 §5.2 的裁决原样保留。** Go 侧 `registryWriteScoped` / "注册但留冷 + UNAVAILABLE" / `/healthz` 报 `cold` 全部未动；两处 `configMapKeyRef` 都保持 `optional: true`，因为一个**必需**的 keyRef 会让 Pod 直接 `CreateContainerConfigError` 起不来 —— 那正是 D6 §5.2 拒绝的那种爆炸半径（拿路由 / 发现 / binding 全停去换一个可选值）。

**（e）"缺了在部署时就看得见"用三个手段兑现，都不依赖运行日志：**

1. **默认值**：值现在由 base 层自己生成，`kubectl apply -k` 的正常路径下不可能缺。T3 那个"手工 `kubectl patch secret` 才跑起来"的现场不会重演。
2. **提交时就红**：`services/shared/config` 的清单测试把"两边同源 + 与 `default.toml` 同值"变成 CI 门禁。key 名写错、只改一边、literal 漂移，全部在 PR 上失败 —— 比部署时更早。
3. **部署后可自证**：`/healthz` 的 `registry_write.cluster_id`。`cold` 有两个成因（库连不上 / 没人给作用域）从外面长得一模一样，读回这个字段就能把"配了"和"没配"分开，不用重新发布任何东西。

### 1.3 对应测试

**`services/shared/config/manifest_test.go:150` `TestOneClusterIdentityReachesBothSidesOfTheRegistry`**
（放在这个文件是因为它已经是"清单不变量"的家：`TestMetricsListenersAreDeclaredAndExposed` 同款）。四条断言：

1. 两个工作负载的 cluster id 都来自 **ConfigMap key**，不是 Secret / 字面量 / fieldRef；
2. 两边的 **ConfigMap 名与 key 名完全相同**；
3. base 层的 `configMapGenerator` 真的生成了那个 ConfigMap，且该 key 非空；
4. 该字面量 == `config/default.toml` 的 `[node_identity].cluster_id`（锁住"回落值不会和 ConfigMap 分家"）。

**`services/scheduler/cmd/health_test.go`**：`serving` 用例带 cluster id，断言 `/healthz` 原样报回；其余相位断言为空。

### 1.4 变异验证

| # | 变异 | 命令 | 结果 |
|---|---|---|---|
| M1 | scheduler 改回 `secretKeyRef{agentenv-postgres, cluster_id}` | `go test -run TestOneClusterIdentityReachesBothSidesOfTheRegistry ./shared/config/` | **FAIL** `…reads SCHEDULER_REGISTRY_CLUSTER_ID from something other than a ConfigMap key` |
| M2 | daemonset 删掉 `AENV_CLUSTER_ID` | 同上 | **FAIL** `the node DaemonSet does not set AENV_CLUSTER_ID at all` |
| M3 | ConfigMap 字面量改成 `11111111-…` | 同上 | **FAIL** `…is "11111111-…" but config/default.toml's [node_identity].cluster_id is "00000000-…"` |
| M4 | 删掉整个 `configMapGenerator` 条目 | 同上 | **FAIL** `nothing in the base layer generates the cluster-identity-config ConfigMap` |
| M5 | `/healthz` 不再输出 `cluster_id` | `go test -run TestHealthReportsThePhaseWithoutGatingTheProcess ./scheduler/cmd/` | **FAIL** `cluster_id: got "", want "11111111-aaaa-…"` |

变异均在 scratchpad 副本（`/home/debian/.claude/jobs/ec286011/tmp/mut/orig/`）留底后就地打入，逐条跑完立即还原；还原后复跑全绿。

---

## 2. F2 —— 后端装配日志

### 2.1 改了什么

| 文件 | 改动 |
|---|---|
| `src/orchestrator/paused_registry/mod.rs:423` | `build_paused_registry` 重排成 `match` 产出 `(registry, scheduler_endpoint)`，**match 之后一条** `info!(backend, cluster_id, lease_ttl_secs, scheduler_endpoint, "paused sandbox registry ready")` |
| `src/orchestrator/paused_registry/postgres.rs` | 删掉 `connect()` 里那条重复的 `info!`（留注释指向新位置），`use tracing::{debug, warn}` |
| `src/cfg.rs:406` | 新增 `PausedRegistryBackendKind::as_str()` |
| `docs/src/deployment/kubernetes.md` | 把这条日志写进运维文档，说明它是"切换是否生效"的确认手段 |

日志形状（三个后端同一句，`local` / `postgres` 的 `scheduler_endpoint` 为空）：

```
INFO paused sandbox registry ready backend=central cluster_id=… lease_ttl_secs=90 scheduler_endpoint=http://agentenv-scheduler:9090
INFO paused sandbox registry ready backend=postgres cluster_id=… lease_ttl_secs=90 scheduler_endpoint=
INFO paused sandbox registry ready backend=local    cluster_id=… lease_ttl_secs=90 scheduler_endpoint=
```

### 2.2 为什么是"一条"而不是"三条"

任务书说三个后端各打一条。实现上做成**一个语句、三个后端共用**，理由是这条缺陷本身：

> 每个分支各写一行 ⇒ 就有一行是某个分支可以漏掉的。而漏掉的正是 `central`。

`match` 之后的单点是结构性的 —— 想让某个后端不打这条日志，必须让它 early-return，而三条 early-return 路径全是 `Err`（起不来）。`backend=` 字段承担了"是哪个"的分辨力，且它的取值来自 `as_str()`，与 `AENV_PAUSED_REGISTRY_BACKEND` 接受的字面量**是同一个词**（有断言锁住，见下），所以运维读回的东西和他刚写下去的东西可以逐字比对。

DSN **不进日志**（带凭据）；`postgres` 后端因此没有"往哪连"的字段，这是刻意的。

### 2.3 拼错的 backend 值：**已经是 fail-closed，本轮把它锁住**

结论：**拼错会拒绝启动，不是静默回落 local**，无需改行为。

机理（`confique-0.4.0/src/internal.rs:68` `from_env`）：

```rust
let s = get_env_var!(key, field);
let is_empty = s.is_empty();
match deserialize(...) {
    Ok(v)              => Ok(Some(v)),
    Err(_) if is_empty => Ok(None),          // 空串 == 未设置
    Err(e)             => Err(EnvDeserialization{..}),   // ← 非空的非法值走这里
}
```

非空非法值 ⇒ `AppConfig::builder().load()` 报错 ⇒ `ConfigManager::new_from_path` 返回 `Err` ⇒ `src/bin/server.rs:62` 用 `?` 接 ⇒ 进程起不来。方向与 D6 §5.2 不冲突：这是"有人显式写了一个非法值"，正是该 Fatal 的那类。

本轮把这个行为**变成断言**（`src/cfg.rs:1592`），覆盖 `postgress` / `Central`（大小写）/ `postgres `（尾空格）/ `node-local` 四种写法。

### 2.4 对应测试

| 测试 | 位置 | 断言什么 |
|---|---|---|
| `every_backend_reports_which_one_it_is` | `src/orchestrator/paused_registry/mod.rs:611` | `local` / `central` 两条真实装配路径各打出 `backend=…`、`cluster_id=…`、`lease_ttl_secs=90`、消息体；`scheduler_endpoint=` 只在 `central` 出现且值正确 |
| `the_paused_registry_backend_is_settable_from_the_environment`（扩写） | `src/cfg.rs` | ①三个后端都能从 env 选中（原有）②`as_str()` 与 env 接受值逐字相同（新）③四种拼错写法**必须 `Err`**（新） |

两点实现取舍：

- **`postgres` 臂没法单测**（要真库），它与另外两臂共用同一条语句，这是"一个语句"设计换来的覆盖；集成测试 `tests/paused_registry.rs` 走的是 `PostgresPausedSandboxRegistry::connect` 而非 `build_paused_registry`，够不到 `Recorder`（`pub(crate)`）。
- **拼错断言塞进已有那个测试而非新开一个**：`AENV_PAUSED_REGISTRY_BACKEND` 是进程级 env，cargo 默认并行跑测试，两个测试同时 set/remove 同一个变量会互相打架。原测试的注释本来就写着"这个变量本 crate 没有别处读写"，新开一个会让那句话失真。

### 2.5 变异验证

| # | 变异 | 命令 | 结果 |
|---|---|---|---|
| M6 | 整条 `info!` 删掉 | `cargo test -p agentenv --lib every_backend_reports_which_one_it_is` | **FAIL**（panic at mod.rs 断言处） |
| M7 | 日志去掉 `backend` 字段 | 同上 | **FAIL** |
| M8 | 日志去掉 `scheduler_endpoint` 字段 | 同上 | **FAIL** |
| M9 | `as_str()` 把 `Central` 印成 `"Central"` | `cargo test -p agentenv --lib the_paused_registry_backend_is_settable_from_the_environment` | **FAIL** `left: "Central" / right: "central"` |
| M10 | 给 `backend` 加 `parse_env`，未知值宽容地映射成 `Local` | 同上 | **FAIL** `backend="postgress" was accepted; a misspelled backend must not silently leave the node on 'local'` |

M10 是本组里唯一一条"真把行为改坏"的变异（M6–M9 是去掉信号），它证明 §2.3 的断言确实咬住了 fail-closed 这件事，而不是只咬住了 confique 的现状。

---

## 3. 门槛实际输出

Go（`GOWORK=off`，`services/`，DSN 见 §5 说明）：

```
### gofmt -l .
(空)
### go build ./...          rc=0
### go vet ./...            rc=0
### go test -count=1 ./...
?   	agentenv/services/api/proto	[no test files]
?   	agentenv/services/gateway/cmd	[no test files]
ok  	agentenv/services/gateway/internal	0.032s
ok  	agentenv/services/scheduler/cmd	0.013s
ok  	agentenv/services/scheduler/internal	0.453s
ok  	agentenv/services/scheduler/internal/registry	26.319s
ok  	agentenv/services/shared/config	0.012s
ok  	agentenv/services/shared/logging	0.004s
?   	agentenv/services/shared/observability	[no test files]
```

Rust：

```
### rustfmt --edition 2021 --check src/cfg.rs src/orchestrator/paused_registry/{mod,postgres}.rs
rc=0（三个改过的文件都干净；main 上本来就红的两个文件未触碰）
### cargo clippy --workspace --all-targets --all-features -- -D warnings
rc=0
### cargo test -p agentenv --lib
test result: ok. 853 passed; 0 failed; 4 ignored; 0 measured; 0 filtered out
### AENV_PAUSED_REGISTRY_TEST_DSN=… cargo test -p agentenv --test paused_registry
test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out      ← 0 skip
```

另：`kubectl kustomize deploy/k8s/overlays/default` rc=0，渲染结果里 ConfigMap 与两处 `configMapKeyRef` 逐字对上（§1.2 引文）。**未连任何集群，未 apply。**

---

## 4. 改动清单

```
config/default.toml                          |   9 +      注释：cluster_id 与 ConfigMap 的关系
deploy/k8s/base/agentenv-daemonset.yaml      |  16 +      AENV_CLUSTER_ID
deploy/k8s/base/kustomization.yaml           |  17 +      cluster-identity-config 生成器
deploy/k8s/base/scheduler-deployment.yaml    |  28 +-     Secret → ConfigMap
services/scheduler/cmd/health_test.go        |  23 +-     M5 的靶子
services/scheduler/cmd/main.go               |  15 +-     /healthz 带 cluster_id
services/shared/config/manifest_test.go      | 135 +      清单不变量测试
src/cfg.rs                                   |  52 +-     as_str + 拼错断言
src/orchestrator/paused_registry/mod.rs      | 175 +-     单点装配日志 + 测试
src/orchestrator/paused_registry/postgres.rs |   6 +-     删重复日志
```

阶段 0 / 1 / 2 的任何东西都没有回退。

---

## 5. 发现但没做的问题

### 5.1 🟡 `TestPostgresReaderListWithoutClusterFilterSeesEveryCluster` 在共享测试库上必红（非本轮引入）

本机 `postgres://…/aenv` 里 `paused_sandboxes` 有 **180 行残留**，全部是 Rust 集成测试留下的随机 cluster id（时间戳 15:56–16:48，本轮开工前就在）：

```
cluster=077f60dd-… rows=3 first=2026-08-19 15:56:55 …
cluster=2cd5e477-… rows=3 first=2026-08-19 16:48:42 …
（共 60+ 个 cluster）
```

该测试 `newTestReader(t, dsn, "")` 后断言 `len(listing.Sandboxes) != 6` —— 即"全表只有我自己那 6 行"。这和它上面 `setupRegistryDatabase` 的注释自相矛盾（"Each test owns its rows by cluster id and removes those instead"），也和 Rust 侧共用同一张表的事实冲突。

**取证**：新建空库 `aenv_f1f2` 后同一个包 `ok 26.5s` 全绿；本轮改动没有一个字落在 `services/scheduler/internal/registry/`。

**建议**（未做，超出本次范围）：这条断言改成"至少包含这 6 行、且 6 行的 cluster 分布正确"，或者给它自己的 schema / 库。
**副作用告知**：我在 PG 里建了一个 `aenv_f1f2` 库用于取证，没删（删库对并行的其它 agent 有风险）。共享库里的残行**一行没动**。

### 5.2 🟢 `AENV_PAUSED_REGISTRY_BACKEND=""`（显式空串）仍然回落 `local`，不报错

confique 的全局约定：空串一律当"未设置"（§2.3 引文里的 `Err(_) if is_empty => Ok(None)`）。一个 `configMapKeyRef` 指到值为空的 key 就会走到这里。

没有为这一项破例（那要写一个专用 `parse_env`，与全 crate 其它 env 项的行为分家）。理由是后果现在**可见**了：这种情况下装配日志会打 `backend=local`，正是 F2 要解决的那个分辨力。登记在此，若将来要求"空串也拒绝"，改动点是 `src/cfg.rs` 那个 `#[config(...)]`。

### 5.3 🟢 scheduler 侧 cluster id 仍是 `optional: true`，删掉 ConfigMap 在部署时不报错

刻意的，理由见 §1.2(d)。部署时的分辨力由 CI 清单测试（提交时）+ `/healthz`（部署后）承担，而不是靠让 Pod 起不来。如果将来判断"宁可 scheduler CrashLoop 也不要冷写面"，改法是把两处 `optional: true` 去掉 —— 但那等于推翻 D6 §5.2，应该另开裁决。

### 5.4 🟢 `docs/src/deployment/kubernetes.md` 里 `AENV_NODE_ID` 的来源写错了

文档写 "from Pod metadata name (`metadata.name`)"，清单实际是 `fieldRef: spec.nodeName`（而且 daemonset 里有一段 🔴 注释专门解释为什么必须是 nodeName 而不是 pod 名）。文档过期，与本轮无关，未改。

### 5.5 🟢 cluster id 没有 apply 期覆盖钩子

`deploy/k8s/run.sh` 给 `SANDBOX_PROXY_DOMAINS` 留了一个环境变量覆盖入口（`sed_in_place` 改 kustomization 的字面量），cluster id 没有对应的。**故意不加**：多一个覆盖入口就多一处能和 `config/default.toml` 的回落值分家的地方，而 §1.3 那条测试只看得住清单里的字面量。要改集群名，就改 `kustomization.yaml` 那一行，两边一起变。

### 5.6 T3 §7 的 F3 / F4 / F5 / F6 本轮未动

不在任务范围内（指标口径 F3/F4、`inferred_downtime` 高估 F5、`sandbox_expires_at` 孤儿行 F6）。
