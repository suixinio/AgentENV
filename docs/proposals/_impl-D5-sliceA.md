# 实施记录：中央控制面 阶段 2 · Slice A（前置加固）

> 2026-08-19 · 研发 agent D5-sliceA。分支 `central-control-plane-phase2`，**未 commit / 未 push**。
> 任务书：[`_impl-plan-control-plane-phase2.md`](_impl-plan-control-plane-phase2.md) §2 🅐
> 侦察：[`_recon-R4-rust-client.md`](_recon-R4-rust-client.md) §4.5 §5.1 §5.3 §6.3 §7、
> [`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md) §2.11
>
> 只动 Rust + 配置 + 清单 + CI + Go 测试。**没有碰**阶段 0/1 的任何生产代码
> （`services/scheduler/internal/registry/postgres.go` / `lookup.go` / `reconcile.go` 全部零改动，
> 该目录下只有 `postgres_integration_test.go` 被改，且只加了防假绿开关）。

---

## 0. 改动清单

```
 .github/workflows/ci.yml                              |  35 +      A3
 .github/workflows/services-ci.yml                     |  24 +      A3
 Makefile                                              |  11 +      A3
 config/default.toml                                   |  38 +      A4
 deploy/k8s/base/agentenv-daemonset.yaml               |  26 +      A4
 docs/src/configuration/env-vars.md                    |   2 +      A4
 docs/src/deployment/kubernetes.md                     |  18 +      A4
 services/scheduler/internal/registry/
     postgres_integration_test.go                      |  27 +-     A3
     metadata_golden_test.go                （新增）    | 217 +      A2
 src/api/impls/mod.rs                                  |   2 +-     A1
 src/api/impls/paused_coordinator.rs                   | 525 +      A1 A5
 src/api/impls/paused_recovery.rs                      | 199 +-     A1
 src/api/mod.rs                                        |   2 +-     A1
 src/bin/server.rs                                     |  15 +-     A1
 src/cfg.rs                                            |  84 +-     A4
 src/orchestrator/store/metadata.rs                    | 316 +      A2
 tests/fixtures/sandbox_metadata_full.json    （新增）  | 105 +      A2
 tests/fixtures/sandbox_metadata_minimal.json （新增）  |  38 +      A2
```

---

## 1. A1 —— `release_stale_node_holdings` 带围栏可重试 + metric

### 改了什么

**新增 `TakeoverWindow`（`paused_coordinator.rs:111`）** —— 一个 `tokio::sync::Mutex<bool>` 单调闩，
表示「本进程还没做过任何可能把本节点名字写进 `running` / `resuming` 行的事」。

- `enter()` 返回 `TakeoverAttempt` 守卫，**整个释放 RPC 期间锁一直握着**；
  拿不到（窗口已关）就返回 `None`。
- `close()` 只从「开」翻到「关」，永不翻回。
- 守卫 `settle()` 才关窗；**丢弃守卫（失败路径）窗口保持打开** —— 这就是可重试的来源。

**围栏在两个最早的点关闭**（都在写之前，不看写是否成功）：
| 位置 | 为什么是这里 |
|---|---|
| `paused_recovery.rs:169`，`claim_for_resume` **之前** | 认领一旦落库就写了 `resuming` + 本节点名；响应丢了也一样写了 |
| `paused_coordinator.rs:393`，`mark_running` **之前** | 同理，`running` + 本节点名 |

**`PausedSandboxCoordinator::release_stale_holdings`（`:335`）** 返回三态
`StaleReleaseOutcome::{Released, Fenced, Failed}`，并在失败时打
`agentenv_paused_registry_stale_release_failed_total`。

**`ApiImpl::release_stale_node_holdings`（`paused_recovery.rs:427`）** 保留原来的
`is_cluster_backed()` 短路，改为返回 `StaleReleaseOutcome`。
**`ApiImpl::retry_stale_node_holdings_release`（`:456`）** 是重试循环：每
`STALE_RELEASE_RETRY_INTERVAL = 5s`（`:216`，常量不开配置，理由写在注释里）重试一次，
`Released` 就 `info!` 返回；`Fenced` 打
`agentenv_paused_registry_stale_release_abandoned_total` + **`error!`** 返回
（这一刻是真的有行被搁浅到下次重启，值得告警而不是一条 debug）。

**`server.rs:176-192`**：启动期那一发的结果被接住，**只有 `Failed` 才 spawn 重试任务**，
句柄推进 `paused_upkeep`，随其它 upkeep 任务一起在 shutdown 时 abort。
启动三连的顺序和「listener 之前」的语义**完全没变**。

### 行为变化

| 场景 | 之前 | 之后 |
|---|---|---|
| 启动期释放成功 | 释放 | 不变，且窗口关闭（第二次调用是 `Fenced`，不再打 registry） |
| 启动期释放失败 | 一条 `warn!`，**永不重试**，那些沙箱的 resume 永久 409 直到人工重滚节点 | `warn!` + metric，5s 一次重试，落地即 `info!` 停 |
| 重试期间本节点接下第一台沙箱 | —— | 立刻 `Fenced` + `error!` + metric，**绝不会**再释放 |
| `local` 后端 | 无操作 | 不变（`Released`，且不 spawn 重试） |

### 对应测试

`src/api/impls/paused_coordinator.rs`（协调器层，全部 `#[tokio::test]`）：
- `a_registry_failure_leaves_the_window_open_for_another_attempt`
- `a_successful_release_closes_the_window`
- `taking_a_sandbox_live_fences_the_release`
- 🔴 `an_unconfirmed_mark_running_still_fences_the_release`
- `pruning_the_registrations_does_not_reopen_the_window`

`src/api/impls/paused_recovery.rs`（ApiImpl 层）：
- `a_node_local_registry_has_nothing_to_release`
- `the_retry_keeps_going_until_the_release_lands`（`start_paused` 虚拟时钟）
- 🔴 `the_retry_stops_once_this_node_holds_a_sandbox`

测试用的假 registry 在 `paused_coordinator.rs` 的 `test_support::CountingRegistry`
（cluster-backed，可编程失败次数、可数调用次数），两个测试模块共用。

> 两个重试循环测试都套了 `run_bounded`（`tokio::time::timeout(600s)`）。
> 不是装饰：围栏被去掉时循环**不会失败而会永远转**（`start_paused` 下虚拟时钟随 CPU 空转推进），
> 第一轮变异验证就把测试进程挂在 100% CPU 上跑了十分钟。有界之后同一个变异是干净的 FAIL。

---

## 2. A2 —— metadata golden fixture

### 改了什么

两份 fixture，由 Rust 从**真实结构体**序列化产出、Go 读**同一份文件**：

| 文件 | 形态 |
|---|---|
| `tests/fixtures/sandbox_metadata_full.json` | 所有可选字段都填满：两个 `skip_serializing_if` 字段（`image_configs` / `custom_extension_params`）**存在**，`CommandContext` 六个可选项全填，两个 camelCase 键（`mountPath` / `driveId`）都出现 |
| `tests/fixtures/sandbox_metadata_minimal.json` | 两个 `skip_serializing_if` 字段**缺失**（文档里连键都没有） |

**Rust 侧** `src/orchestrator/store/metadata.rs` 新增 `mod golden`（跑在 `make test-unit` 里）：
- `the_full_fixture_matches_what_sandbox_metadata_serialises_to` / `the_minimal_fixture_omits_...`
  —— 钉死文件内容；结构体一改就红，`UPDATE_METADATA_GOLDEN=1` 重生成
- `both_fixtures_round_trip_through_the_struct`
- 🔴 `dropping_any_required_field_makes_the_record_undecodable` —— 逐个删掉那 10 个必需字段，
  断言**必然解码失败**。这条是 fixture 的**自证**：没有它，fixture 少一个字段自己也发现不了
- `dropping_an_optional_field_still_decodes` —— 反向边界，5 个 `Option` 字段缺了仍然合法

**Go 侧** `services/scheduler/internal/registry/metadata_golden_test.go`：
- `TestMetadataSurvivesTheJSONBRoundTrip` —— `json.RawMessage` → JSONB → 读回，
  三重断言：① Go 侧 `json.Number` 规范化后 `reflect.DeepEqual`（不用默认 `any`，
  否则大整数塌成 float64 还会「通过」）② PG 自己的 `metadata = $2::jsonb` ③ 10 个必需键 +
  两个 camelCase 键都还在
- `TestMetadataFixturesAreReadableWithoutADatabase` —— 没有 PG 的 runner 上也能抓到 fixture 被挪走/改名

fixture 放在仓库根 `tests/fixtures/`（Go module 之外），Go 用相对路径 `../../../../tests/fixtures/`
读同一份 —— **`services/` 里放一份副本就等于制造第二个真相源**。

### 🔴 一个必须记下来的坑：R4 §3.2 的键序结论在本仓是错的

R4 写「`serde_json::to_value` 用 BTreeMap 建对象（Cargo.toml 没开 `preserve_order`），键序已排序」。
实际上 **`storage/overlaybd/Cargo.toml:28` 开了 `serde_json/preserve_order`**，
Cargo 的 feature 统一让 `agentenv` 也吃到 —— `serde_json::Map` 在本 workspace 是
**插入序（IndexMap）**，于是 `HashMap` 字段的迭代顺序（每进程随机）会直接漏进 JSON。
不处理的话 fixture 每次跑都不一样，根本钉不住。

处理：`canonical()` 递归按键排序后再渲染（`metadata.rs` 的 `sort_keys`）。
键序本来就不是契约（PG jsonb 自己会重排），两侧比的都是键集合 + 值。
连跑 5 次确认稳定。

---

## 3. A3 —— CI 补真 PG job

### 改了什么

**Rust**
- `Makefile` 新增 `test-paused-registry` 目标，**自带 `AENV_PAUSED_REGISTRY_TEST_REQUIRED=1`**
  （放进目标里而不是只放 CI，本地跑也不会假绿）
- `.github/workflows/ci.yml` 新增 job `paused-registry-tests`：`services: postgres`
  （`postgres:16-alpine` + `pg_isready` 健康检查），`AENV_PAUSED_REGISTRY_TEST_DSN` 指过去，
  跑 `make test-paused-registry`
- 单独一个 job 而不是塞进 `unit-tests`：`make test-unit` 只 build lib target，
  **结构上就够不到 `tests/paused_registry.rs`** —— 这正是那 29 个测试能躺在仓里从没被跑过的原因

**Go**
- `postgres_integration_test.go` 新增 `requireTestDSN(t)`：`SCHEDULER_REGISTRY_TEST_REQUIRED`
  非空而 DSN 缺失 ⇒ `t.Fatal`，否则 `t.Skip`。7 个既有集成测全部改走它
- `.github/workflows/services-ci.yml` 的 `go-services` job 加 `services: postgres`，
  `Test services` 步骤带上 `SCHEDULER_REGISTRY_TEST_DSN` + `SCHEDULER_REGISTRY_TEST_REQUIRED=1`

**两个 workflow 的 `paths:` 都加了 `tests/fixtures/**`** —— 否则改 fixture 两边 CI 都不触发，
安全绳自己没人守。

### 自证（对照实验，见 §6 变异表 A3-M1 / A3-M2）

不是只跑「有 DSN 时是绿的」就算数：
1. **没有 DSN 但设了 REQUIRED** ⇒ Rust 29 个全 FAIL、Go 7 个全 FAIL ✅
2. **反向对照**：把 `_REQUIRED` 判定从 Go helper 里删掉 ⇒ 同样的无 DSN 环境**变绿**；
   把 `AENV_PAUSED_REGISTRY_TEST_REQUIRED=1` 从 make 目标里删掉 ⇒
   `test result: ok. 29 passed`，而 29 个测试一行 SQL 都没执行。
   这两发证明「红」确实是那个开关造成的，不是别的原因

`actionlint` 对两个 workflow 文件都干净（exit 0）。

⚠️ **GitHub 上的实跑没有做** —— 本地没有 GitHub runner，也没连集群。
上面验证的是 job 里那条命令的行为和 workflow 的语法，不是 Actions 调度本身。

---

## 4. A4 —— `[orchestrator.paused_registry]` 进 `config/default.toml`

### 改了什么

1. **`config/default.toml:205-243`** 新增整节：`backend = "local"` + `max_connections` +
   `reconcile_interval_secs` + `lease_ttl_secs`，`dsn` 只留注释（凭据不进这个会被整份拷进
   ConfigMap 的文件）。节首一段 🔴 注释写清楚为什么这节必须在文件里：
   `deploy/k8s/run.sh:30` 每次 apply 都拿 `config/default.toml` 覆盖 `agentenv.toml`。
2. **`src/cfg.rs:394`** `backend` 加 `env = "AENV_PAUSED_REGISTRY_BACKEND"`。
3. **`deploy/k8s/base/agentenv-daemonset.yaml`** 加两个 env，**都是 `optional: true`**：
   - `AENV_PAUSED_REGISTRY_DSN` ← `secretKeyRef{agentenv-runtime-secrets, paused-registry-dsn}`
   - `AENV_PAUSED_REGISTRY_BACKEND` ← `configMapKeyRef{paused-registry-config, ...}`
4. `docs/src/configuration/env-vars.md` 补两行；`docs/src/deployment/kubernetes.md` 新增
   「Cluster-wide Paused Sandboxes」一节，给出开启所需的两条 `kubectl` 命令。

### 为什么 backend 也要走 env（这是我加的，任务书只要求 DSN secretKeyRef）

光把 `backend = "local"` 写进 `default.toml` **并不能修掉那颗地雷，只是把它显式化**：
`make k8s-apply` 照样会把集群 ConfigMap 里的 `postgres` 覆盖成 `local`。
只有让「选哪个后端」落在**文件覆盖不到的地方**（env），R3 §3.2 的根因才真的没了。
DSN 是 Secret、backend 是 ConfigMap，两个都 optional，所以**没有这两个对象的集群照常起来**
（回落 `local`，与今天一致）。

### 🔴 上线前必须做的一步（运维动作，不是代码）

本改动**不会**自动保住某个集群现有的 `postgres` 设置。如果 dev/test 集群今天是靠手改
`agentenv-k8s-config` ConfigMap 跑 `postgres` 的，那么**下一次 `make k8s-apply` 之前**要先建好：

```bash
kubectl -n agentenv-system create configmap paused-registry-config \
  --from-literal=AENV_PAUSED_REGISTRY_BACKEND=postgres
# 并把 paused-registry-dsn 加进 agentenv-runtime-secrets
```

否则那次 apply 会把节点静默降级成 `local`（与改动前同样的失败，只是现在有了正确的补法）。
我没连集群，无法确认现状，请在切换窗口前核对。

### 对应测试

`src/cfg.rs` 的 `mod tests`：
- `the_bundled_default_config_documents_the_paused_registry` —— 直接 parse
  `config/default.toml`，断言 `[orchestrator.paused_registry]` 表存在、四个键都在、
  **`dsn` 不在**（凭据不许提交）
- `the_paused_registry_backend_is_settable_from_the_environment` —— 设了 env 得 `Postgres`，
  不设得文件里的 `Local`

另外 `kubectl kustomize` 本地渲染通过，确认渲染出的 ConfigMap 里带上了新一节、
DaemonSet 里带上了两个 env（**只 render，没有 apply，没有连集群**）。

---

## 5. A5 —— `complete_pause` / `mark_local_only` 失败后先 `get` 再决定删不删快照

### 🔴 与任务书不一致：R4 §4.5 的前提在当前树上已经不成立

任务书说「今天这两个失败会**直接删快照**」。实际读代码：

- **`complete_pause` 失败这条已经修好了**，而且不是本轮修的 ——
  上游 commit `c08cf5f`（*fix(orchestrator): hold a paused sandbox's cluster record to what it promises*，
  2026-08-17，已在 HEAD 祖先里）已经把 `discard_unreferenced_snapshot` 改成
  「先 `get` 再决定」，并有纯函数 `orphan_verdict(readable, referenced)` 三态：
  `Delete` / `Referenced` / `Unknown`，**读不到就保留快照**。
  R4 §4.5 末尾引的那句「`paused_coordinator.rs:335` 已经有一个 `let reread = ...` 的先例可以照抄」，
  指的其实就是它想修的那个函数本身。
- **`mark_local_only` 失败根本不会删快照**：走到那条分支说明 `publish_captured` 失败了，
  压根没有新快照；`began.previous_snapshot_id` 在这条路上是**保留**的（只有 `complete_pause`
  成功后才删旧的）。所以这半边没有东西可修。

### 那我做了什么

原实现虽然正确，但**变异验证会假绿**：`orphan_verdict` 只有纯函数单测，
把 `discard_unreferenced_snapshot` 里的重读整段删掉、写死 `orphan_verdict(true, false)`，
原有 3 个测试**全都还是绿的**。而「读了行才删」和「没读就删」从仓库那边看是一样的，
只在写落库但响应丢了的时候分道扬镳 —— 那正是阶段 2 新增的失败形态。

所以补了一条可测的缝：
- `discard_unreferenced_snapshot`（`:489`）**返回它据以行动的 `OrphanVerdict`**，
  生产调用点 `let _ =` 忽略
- `CountingRegistry` 加 `get_calls()` 计数与可编程 `GetAnswer::{Missing, Referencing, Unreachable}`

新增三条测试断言「**读了一次**」+「据读到的东西下对了结论」：
- 🔴 `a_failed_complete_pause_rereads_the_row_before_touching_the_snapshot`（行仍指向该快照 ⇒ `Referenced`）
- `an_unreadable_registry_leaves_the_snapshot_alone`（读不到 ⇒ `Unknown`，保留）
- `a_snapshot_no_row_points_at_is_collected`（确证无人引用 ⇒ `Delete`）

变异（删掉重读）现在 3 条全红。

---

## 6. 变异验证表

变异全部打在 `/tmp/.../scratchpad/mut/` 的整树副本上（独立 `CARGO_TARGET_DIR`），
**工作区零改动**（每发跑完立刻还原；`git status` 全程只有本轮的预期改动）。
副本内的基线是绿的：`test result: ok. 45 passed`（`api::impls::paused`）。

| # | 项 | 变异 | 结果 |
|---|---|---|---|
| A1-M1 | 围栏 | 围栏只看 `running_registrations.is_empty()`，去掉单调闩 | **FAILED 5 条**：`an_unconfirmed_mark_running_still_fences_the_release`、`pruning_the_registrations_does_not_reopen_the_window`、`taking_a_sandbox_live_fences_the_release`、`a_successful_release_closes_the_window`、`the_retry_stops_once_this_node_holds_a_sandbox` |
| A1-M2 | 可重试 | 失败路径也 `attempt.settle()`（退回一次性语义） | **FAILED 2 条**：`a_registry_failure_leaves_the_window_open_for_another_attempt`、`the_retry_keeps_going_until_the_release_lands` |
| A2-M1 | fixture 完整性 | 从 full fixture 删掉必需字段 `timeout_action` | **Rust FAILED 4 条**（含 `dropping_any_required_field_...`）；**Go FAILED 2 个 Test**，报 `sandbox_metadata_full.json is missing required field "timeout_action"` |
| A2-M2 | camelCase 键 | 把 `mountPath`/`driveId` 整体蛇形化（模拟 Go 统一 tag 策略） | **Rust FAILED 3 条**；**Go FAILED**，报 `camelCase key "mountPath" did not survive the round trip` |
| A2-M3 | 不透明透传 | Go 侧把 metadata 过一遍手写 struct 再写回（R4 风险 3a 的原始场景） | **Go FAILED**，报 `required field "created_at" did not survive the round trip` |
| A3-M1 | 防假绿（Go，对照） | 删掉 `SCHEDULER_REGISTRY_TEST_REQUIRED` 判定 | 无 DSN + REQUIRED=1 下 **变绿**（`ok ... 0.004s`）—— 证明红是那个开关造成的 |
| A3-M1' | 防假绿（Go，正向） | 保留判定，REQUIRED=1 且不给 DSN | **FAIL**，7 个 Test 全红 |
| A3-M2 | 防假绿（Rust，对照） | 从 make 目标里删掉 `AENV_PAUSED_REGISTRY_TEST_REQUIRED=1` | 无 DSN 下 **`test result: ok. 29 passed`**，而一行 SQL 都没跑 —— 这就是那个假绿 |
| A3-M2' | 防假绿（Rust，正向） | 保留开关，不给 DSN | **FAILED. 0 passed; 29 failed** |
| A4-M1 | env 覆盖 | 去掉 `env = "AENV_PAUSED_REGISTRY_BACKEND"` | **FAILED**：`the_paused_registry_backend_is_settable_from_the_environment` |
| A4-M2 | 配置节 | 从 `config/default.toml` 删掉整个 `[orchestrator.paused_registry]` 节 | **FAILED 2 条**：`the_bundled_default_config_documents_the_paused_registry`、`the_paused_registry_backend_is_settable_from_the_environment` |
| A5-M1 | 重读 | 删掉重读，写死 `orphan_verdict(true, false)`（读都不读就删快照） | **FAILED 3 条**：`a_failed_complete_pause_rereads_the_row_before_touching_the_snapshot`、`an_unreadable_registry_leaves_the_snapshot_alone`、`a_snapshot_no_row_points_at_is_collected` |

---

## 7. 全绿门槛实测输出

### Rust

```
$ cargo fmt --all -- --check
Diff in .../src/api/generated/src/models.rs
Diff in .../src/orchestrator/paused_registry/postgres.rs
Diff in .../tests/paused_registry.rs
```
⚠️ **这三个是 HEAD 上就有的既存失败**，不是本轮引入。已 `git stash -u` 在干净树上复验过：
同样这三个文件（本轮新增的 `src/orchestrator/store/metadata.rs` 一度也在里面，已格式化掉）。
本轮改过的文件**全部 rustfmt 干净**。根因是当前工具链（rustc 1.97.1）的 rustfmt 比这三个文件
上次格式化时更严；我**没有**顺手格掉它们 —— 那会在本 slice 的 diff 里混进无关改动，
而其中一个是 `src/api/generated/`（机器生成，`make agentenv-server` 的产物）。
👉 **`make fmt` 目前在 main 上就是红的**，请单独处理。

```
$ make clippy                      # cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile ...
clippy exit=0
```

```
$ make test-unit PROFILE=debug
exit=0
test result: ok. 804 passed; 0 failed; 4 ignored; 0 measured; 0 filtered out
test result: ok. 0 passed; 0 failed; ...
test result: ok. 3 passed; 0 failed; ...
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 804 filtered out
test result: ok. 8 passed; 0 failed; ...
test result: ok. 46 passed; 0 failed; ...
```
（其中 4 个 ignored 是既有的 `-- --ignored` 那组，由 `test-unit` 的第二条命令单独跑。）

```
$ AENV_PAUSED_REGISTRY_TEST_DSN=postgres://aenv:verify@127.0.0.1:15499/aenv_registry?sslmode=disable \
      make test-paused-registry
exit=0
test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.98s
```
**0 skip**（目标自带 `AENV_PAUSED_REGISTRY_TEST_REQUIRED=1`，skip 会直接 FAIL）。

### Go（全部 `GOWORK=off`，在 `services/` 下）

```
$ go build ./...          exit=0
$ go vet ./...            exit=0
$ gofmt -l .              []            # 空
$ SCHEDULER_REGISTRY_TEST_DSN=postgres://aenv:verify@127.0.0.1:15499/aenv?sslmode=disable \
  SCHEDULER_REGISTRY_TEST_REQUIRED=1 go test -count=1 ./...
ok  	agentenv/services/gateway/internal            0.029s
ok  	agentenv/services/scheduler/internal          0.080s
ok  	agentenv/services/scheduler/internal/registry 0.210s
ok  	agentenv/services/shared/config               0.013s
ok  	agentenv/services/shared/logging              0.004s
exit=0
```

### 其它

```
$ actionlint .github/workflows/ci.yml .github/workflows/services-ci.yml     # exit 0，无输出
$ K8S_OVERLAY=default bash deploy/k8s/run.sh render                          # exit 0
  → 渲染出的 agentenv-k8s-config 里带上了 [orchestrator.paused_registry] 整节
  → 渲染出的 DaemonSet 里带上了 AENV_PAUSED_REGISTRY_BACKEND / _DSN 两个 optional env
```
（**只 render，没有 apply**；全程没连任何集群。）

---

## 8. 与任务书不一致的地方

### 8.1 🔴 A1 的围栏比任务书要求的更严（已 SendMessage 报备，未收到回复）

任务书：「围栏条件 = `running_registrations` 为空」。
我实现的是「`running_registrations` 为空 **且** 本进程从未**尝试**过让任何沙箱在本节点变活」。

理由：`running_registrations` 只记 **confirmed** 的 `mark_running`
（`paused_coordinator.rs` 的 `observe()` 里 `if !confirmed { return; }`），
而 A1 要覆盖的那个场景恰好会让它保持空：

1. 进程启动，registry 抖 ⇒ 释放失败（这就是要重试的那次）
2. 抖动期间来一发 resume ⇒ `claim_for_resume` 报错 ⇒ `Proceed`（fail-open，
   护栏 §3.5 是 Slice C 才收口）⇒ 沙箱在本进程**真跑起来了**
3. `mark_running` 也失败 ⇒ `confirmed = false` ⇒ **`running_registrations` 仍是空**
4. registry 恢复 ⇒ 重试看到围栏「开着」⇒ 把 `state='running' AND origin_node_id=me`
   （上一个进程留下、本进程此刻正在跑的那台）释放成 `paused`, generation+1
   ⇒ 另一节点可认领重建 ⇒ **双活**

这在 `postgres` 后端下也可达（`connect()` 成功后 PG 才抖，进程照样起来），
等于我用 A1 引入一条新的双活路径。另外 `retain_running_registrations` 会把那张表重新清空
（reconcile 每 30s 一次），单靠它做围栏还会二次开窗。

单调闩仍然**严格绑定「本进程是否登记/尝试登记过 running」，不是时间窗口**，
只是把「确认过」放宽成「尝试过」，方向更保守。A1-M1 变异表就是这条的证据。

### 8.2 A4 的 DaemonSet 我多加了 `AENV_PAUSED_REGISTRY_BACKEND`

任务书只要求 DSN secretKeyRef + cfg 里的 env 覆盖。但 cfg 有 env 覆盖而清单不引用它，
A4 想修的根因（`make k8s-apply` 把集群的 `postgres` 覆盖成文件里的 `local`）就还在。
新加的 `configMapKeyRef` 是 `optional: true`，指向仓库**不**提供的
`paused-registry-config` —— 没建它的集群完全不受影响。见 §4 的上线前动作。

### 8.3 A5 的前提已不成立（见 §5），我做的是补可测性而非补功能

---

## 9. 发现但没做的问题

1. **🔴 `make fmt` 在 main 上就是红的**（3 个文件，其中 1 个是 `src/api/generated/`）。
   本轮没顺手格式化 —— 会往这个 slice 的 diff 里混进无关改动。建议单独一个
   `chore: rustfmt` 提交，或把生成目录排除出 `cargo fmt --all`。
2. **R4 §3.2 关于 `serde_json` 键序的结论是错的**（`preserve_order` 经 feature 统一开着）。
   已在 A2 里绕开，但 Slice B 写 Go 侧时要知道：Rust 写出去的 JSON **键序不稳定**，
   任何按字节比对 metadata 的地方都会翻车（PG jsonb 侧无影响，它自己重排）。
3. **`superseded_by_cluster` 的静默不删仍然没有日志**（R4 §4.3 #5，`paused_recovery.rs:778-785`
   返回 `Err(())`，被 `:732-734` 的 `let Ok(Some(_)) else { return false }` 吃掉）。
   R4 建议补一条 `debug!`。不在 A 片范围内，但 Central 后端上线后这里会**变常见**，
   而排障时完全看不见。建议并入 Slice C。
4. **`renew_lease` 的连续失败计数 metric 没做**（R4 §4.4 建议的
   `agentenv_paused_registry_renew_consecutive_failures`）。它是 Central 后端才真正需要的
   （今天连续漏 3 次几乎不可能），归 Slice C 更合适。
5. **Go 侧那 10 个必需字段的清单是手抄的**（`metadata_golden_test.go` 的
   `requiredMetadataFields` vs `metadata.rs` 的 `REQUIRED_FIELDS`）。
   两边都有测试守着 fixture，所以清单抄错会被 fixture 抓到；但清单本身没有机器同步。
   与 `schemaDDL` 逐字复制是同一种取舍。
6. **`tests/paused_registry.rs` 不做任何清理**（不 DELETE / 不 DROP，靠每测试一个随机
   `cluster_id` 分区）。CI 里每次是全新容器所以无所谓，但本地反复跑会让表无限增长。
   本轮没改它的 harness —— 那是 Slice B 移植语义时更合适一并处理的事。
7. **CI 的两个 PG service 用的是 `postgres:16-alpine`**，与生产版本是否一致我无从确认。
   `SCHEMA_DDL` 只用到 `make_interval` / `jsonb` / advisory lock，PG 12+ 都有，风险低。
