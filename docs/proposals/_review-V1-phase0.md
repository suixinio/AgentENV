# V1：阶段 0（中央影子对账）对抗审查结论

> 只读审查产物。所有变异测试都在 `/tmp/.../scratchpad` 的副本里跑，工作区未做任何改动。
> 集成测试跑在本机 PG（`SCHEDULER_REGISTRY_TEST_DSN`），4 个用例真跑了，非 skip。
> 审查范围：`registry/*`、`reconcile.go`、`node_registry.go` roster 部分、`metrics.go` 新增部分、
> `shared/config` registry 部分、`gateway/internal/registry_list.go`、`scheduler/cmd/main.go`、
> `deploy/k8s/base/scheduler-{deployment,service}.yaml`。
> 判据：`_impl-plan-control-plane-phase01.md` §1/§2/§4、`_recon-R2-registry-spec.md` §1/§6、
> `2026-08-19-agentenv-control-plane-refactor.md` §3、Rust `src/orchestrator/paused_registry/`。

---

## P0

### 1. `services/scheduler/internal/reconcile.go:145-156` + `services/scheduler/internal/metrics.go:157-178`

**缺陷** — 节点整台从 `observed` 消失时，它的 per-node 序列被 `Reset()` 删光，而不是变成告警值。
`Rosters()`（`node_registry.go:481`）只遍历 `r.observed`，而 discovery 驱逐（`node_registry.go:202`）
和 `UnregisterObserved`（`node_registry.go:439`）都会把记录删掉。

**失败场景** — node-a 分区 → `registry_roster_stale{node-a}=1`，告警响 → k8s discovery 摘掉 pod →
下一轮 `recordRegistryReconcile` 的 `Reset()` 之后该序列**不存在** → 告警自动 resolve，
运维读作"节点恢复了"。同一时刻 `reconcile.go:194` 的 `!rosterFresh[origin] → continue`
让 node-a 名下所有 `running` 行也不计 ghost。即：问题变成永久性的那一刻，整台节点的搁浅行全部隐身。
实测探针（两台 discovery 已知节点，其中一台从未心跳）：

```
Rosters()   = [{node-alive [s1] ...}]      # node-never-booted 根本不在里面
rosterStale = map[node-alive:false]        # 没有 node-never-booted 这条
ghost       = map[node-alive:0]            # 它名下 1h 前的 running 行也不算 ghost
```

**建议修法** — 口径改从 registry 那一侧算，它不依赖节点是否还在 `observed` 里：
新增 `rows_without_roster{node}` = 登记表里 `Holder()` 不在任何 roster 中的行数；
并把 `roster_stale` 的 label 全集从 `Rosters()` 换成 `nodes.Snapshot()`（discovery 已知节点全集），
从未心跳过的那台才会渲染成 `1` 而不是消失。

---

## P1

### 2. `services/scheduler/internal/reconcile.go:284`

**缺陷** — `s.nodes.Rosters()` 不按 cluster 过滤，而 `registry/postgres.go:150-153` 的读带
`WHERE cluster_id = $1`，两侧作用域不一致（对比 `ListObserved(clusterID,…)`，`node_registry.go:321`，
是过滤的）。

**失败场景** — 一个 scheduler 观测两个集群时，实测探针（cluster A 的 registry + cluster B 的两台节点）：
`untracked = map[nodeA:0 nodeB1:3 nodeB2:1]`、`holderConflict = 1` —— 全部来自 B 集群自己
健康的跨节点接管过渡态，与 A 集群的健康度毫无关系。

**建议修法** — `computeRegistryReconcile` 收一个 `clusterID` 参数，roster 侧按
`record.node.GetClusterId()` 过滤；或给 `NodeRegistry` 加 `RostersInCluster(id)`。

### 3. `services/scheduler/internal/reconcile.go:155-167`

**缺陷** — `untracked` / `staleCopy` 拿陈 roster 当证据，而 `ghost`（`:194`）和
`holderConflict`（`:171`）都要求 roster 新鲜。同一份数据两套标准。

**失败场景** — node-a 已一小时无心跳，它最后那份 roster 仍被计入：
`staleCopy=map[node-a:2] untracked=map[node-a:1] rosterStale=map[node-a:true]`（实测）。
`stale_copy` 的 Help 写着 "Briefly non-zero during a cross-node takeover"，
实际会在任何静默但尚未被 discovery 驱逐的节点上永久卡住。

**建议修法** — `untracked` / `staleCopy` 同样加 `if !fresh { continue }`；`rosterStale` 仍照常上报。

### 4. `services/scheduler/internal/reconcile.go:131-136`

**缺陷** — `parkedLeaseExpiring` 把 `snapshot_id IS NULL` 的 `publishing`/`local_only` 行算进去，
但 Rust `claim_for_resume`（`src/orchestrator/paused_registry/postgres.rs:545`）的 WHERE 里有
`snapshot_id IS NOT NULL`，这些行谁也抢不走。

**失败场景** — 首次 pause 上传失败 → `local_only` 且 snapshot 仍为 NULL
（`postgres.rs:341` 的 INSERT 写 NULL，`begin_pause` 的 conflict 分支刻意不动 `snapshot_id`）→
节点下线后 `release_node_holdings`（`postgres.rs:893`）只管 live 状态也删不掉它 →
`parked_lease_expiring` 永久 ≥1。而它的 Help 声称"另一个节点可能接管并丢掉上次 pause 之后的工作"，
该结论对这类行不成立：这是一条**永远无法处置的假告警**。

**建议修法** — 加 `&& sandbox.SnapshotID != ""`；NULL-snapshot 那类另立一个
`stranded_rows` gauge（它们确实是坏状态，但坏在"谁也救不回"而不是"要被抢走"）。

### 5. `services/scheduler/internal/reconcile.go:206-221`

**缺陷** — `holderConflict` 与 `staleCopy` 实际不互斥，而 `:214-218` 的注释声称
"The attributable case is counted as staleCopy on the losing side instead"。

**失败场景** — 登记表把行归给**第三台**节点时，实测 `staleCopy=map[node-a:1 node-b:1]
holderConflict=1`，同一个事实计了三次。`TestReconcileHolderConflictNeeds…/"registry names
neither reporter"` 只断言 `holderConflict==1`，没钉住 staleCopy，所以这个行为无人守。

**建议修法** — 要么删掉那句注释（承认二者可叠加），要么在无法归属时跳过该 sandbox 的 staleCopy 计数。

### 6. `services/scheduler/internal/reconcile_test.go:308-334`（假绿，变异实测）

**缺陷** — `TestReconcileDoesNotMixTheTwoClocks` 只挡住 4 个时钟混用变异中的 1 个。
根因是 `:312` 的 `f.localNow = f.dbNow.Add(-90 * time.Minute)` 把进程时钟设在 DB 时钟**之后方**，
于是"是否已过期 / 行够不够老"两类比较在两个时钟下答案相同。

**失败场景** —

| 变异 | 结果 |
|---|---|
| `LeaseExpired(dbNow)` → `LeaseExpired(in.now)`（`reconcile.go:139`）| **PASS**（漏） |
| `dbNow.Sub(UpdatedAt)` → `in.now.Sub(UpdatedAt)`（`reconcile.go:200`）| **PASS**（漏） |
| `dbNow.Add(leaseWarnWindow)` → `in.now.Add(...)`（`reconcile.go:135`）| **PASS**（漏） |
| `in.now.Sub(LastSeen)` → `in.listing.Now.Sub(...)`（`reconcile.go:154`）| FAIL（挡住了） |

**建议修法（已验证）** — `:312` 改成 `f.localNow = f.dbNow.Add(90 * time.Minute)`：
正确实现下仍 PASS，第一个变异下 FAIL（`expected the live lease to be judged against the
database clock, got 1`）。

### 7. `services/scheduler/internal/metrics.go:152-181` + `reconcile_test.go:385-397`（假绿，变异实测）

**缺陷** — `metrics.go` 整个文件零测试覆盖，其中包括方案 §3.1 最重要那条不变式："读失败不清零"。

**失败场景** — 在 `reconcile.go:271-275` 的读失败分支插入
`recordRegistryReconcile(空 result, time.Now())`（正是该处注释明令禁止的行为）→
`go test ./scheduler/internal/` **ok**；把 `metrics.go:157/162/167/172` 四个 `Reset()` 全删 →
也 **ok**。`TestReconcileOnceKeepsGoingAfterAReadFailure` 的注释写
"and — critically — nothing is zeroed"，但只断言了返回值和调用次数。

**建议修法** — 用 `prometheus.NewRegistry()` + `testutil.ToFloat64` / `CollectAndCompare` 写三条：
成功轮后 gauge 有值；紧接一轮读失败后 gauge **不变**；节点消失后旧 label 消失。

### 8. `services/scheduler/internal/reconcile_test.go:289`（假绿）

**缺陷** — `Roster{NodeID: "node-never"}`（`LastSeen` 零值）是生产链路不可达的构造：
`lastSeen` 只在 `node_registry.go:251` 被赋成心跳时刻，`Rosters()` 永远不会产出零值 `LastSeen`。

**失败场景** — 该断言给 `LastSeen.IsZero()` 分支虚假的安全感，正好掩盖了 P0-1 ——
真正没心跳的节点根本进不了 `Rosters()`，那个分支在生产里一次都不会执行。

**建议修法** — 改成经 `Heartbeat` + discovery 驱逐来构造，断言那台节点**仍**出现在 `rosterStale` 里。

---

## P2

### 9. `services/scheduler/internal/registry/postgres.go:199-205`

**缺陷** — `Get` 遇 22P02（id 不是 uuid）时 `r.ready.Store(true)`，一次**失败**的读把 reader 标成 warm，
与 `registry.go` 里 "Ready reports whether this reader has ever completed a read" 的文档不符。

**失败场景** — `Ready()` 今天无消费方所以无害；阶段 1 要靠它区分"权威的无此行"与"我还不知道"，
届时一个畸形 id 的探测就能在从未读到过任何行的情况下提前解锁那条路径。

**建议修法** — 22P02 分支不动 `ready`。

### 10. `services/scheduler/internal/metrics.go:48-135`

**缺陷** — feature off 的集群与"从未成功过"在指标上不可区分：`promauto` 在 init 就注册了
`registry_last_success_timestamp_seconds` / `registry_invalid_rows` 等裸 Gauge，DSN 为空时恒为 0。

**失败场景** — 任何 `time() - last_success > N` 的告警，在每个不跑登记表的集群上永久触发；
而 `deploy` 里 `optional: true` 明确把"不跑"列为受支持配置。

**建议修法** — 加一个 `agentenv_scheduler_registry_enabled` 0/1 gauge，供告警表达式与门。

### 11. `services/scheduler/internal/metrics.go:157-178`

**缺陷** — Reset→Set 不是原子的，抓取正好落在两者之间会看到部分/缺失序列。

**失败场景** — `sum(registry_untracked)` 类告警少一个采样点。窗口微秒级，可接受。

**建议修法** — 可不修；若要修，改成先算全量再对差集逐个 `DeleteLabelValues`。

### 12. `deploy/k8s/base/scheduler-deployment.yaml:34-48`

**缺陷** — 复用节点自己的 `agentenv-postgres/dsn`（owner 权限），只读全靠会话 GUC
`default_transaction_read_only`；且 `cluster_id` 那条 `optional: true` 是 **fail-open**。
（注：GUC 保护本身有效 —— 删掉 `postgres.go` 的 `AfterConnect` 后 4 个写测试全 FAIL，已实测。）

**失败场景** — secret 里少 `cluster_id` 或 key 名写错 → 环境变量静默为空 →
`postgres.go:150` 不加 WHERE → 读全库所有集群的行，正是 R2 §1.2 点名的那个事故形状。

**建议修法** — 给 scheduler 单独一个 `pg_read_all_data` 只读角色；`cluster_id` 缺失时至少 WARN 一条。

### 13. `services/scheduler/internal/metrics.go:50-56`

**缺陷** — `schedulerRegistryRows` 从不 `Reset()`。

**失败场景** — 5 个已知 state 每轮都写所以今天不会陈旧；若 Rust 侧将来加第 6 个 state，
那个 label 在行消失后会永久留在 `/metrics` 上。

**建议修法** — 改成先 `Reset()` 再按固定 5 态写。

---

## 总体判断

**能上生产，但现在还不能拿这些指标去配告警。**

读路径本身是安全的：pool 在数据库层拒写（变异验证有效）、读失败 fail-closed 到 `Unavailable`
而非空集、`ErrDisabled` 与后端故障分得干净、DSN 为空是干净关闭而非 fatal。
对账算法扛住了我扔的全部 9 个语义变异（`Holder()` 忽略 resuming、`staleCopy` 退回 origin、
COALESCE 去掉、ghost 忽略 roster 新鲜度、ghost 算 resuming、reclaimable 把 NULL deadline
当过期、两个 multiplier）—— 口径对齐 R2 §6.4 是扎实的，不是碰巧绿。

但 P0-1 会让最该响的场景（整台节点没了）静默 resolve，P1-2/3/4 会给三个 gauge 带上永久噪声
或永久假阳。建议顺序：先修 P0-1 与 P1-4，补上 #6（一行、已验证）与 #7 两个测试；
P1-2/3 可随阶段 1 一起做。
