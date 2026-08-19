# 实施记录：中央控制面 阶段 2 · Slice B —— scheduler 侧 Go 写实现

> 2026-08-19 · 研发 agent D6-scheduler。分支 `central-control-plane-phase2`，**未 commit / 未 push**。
> 任务书：[`_impl-plan-control-plane-phase2.md`](_impl-plan-control-plane-phase2.md) §1 §3 §4 §5 §6
> 规格书（唯一真相源）：[`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md)
> 权威实现：`src/orchestrator/paused_registry/postgres.rs`（942 行）
>
> 冻结契约由主 agent 提供，本记录**未改动**其中任何一处签名：
> `services/scheduler/internal/registry/store.go`、`services/api/proto/scheduler.proto` 及生成的 pb。

---

## 0. 改动清单

| 文件 | 行数 | 内容 |
|---|---:|---|
| `services/scheduler/internal/registry/store_postgres.go` | 1363 | 新增。`Store` 的 PostgreSQL 实现（13 个方法）+ 行解码 + uuid 校验 + metadata 形状校验 |
| `services/scheduler/internal/registry/migrate.go` | 127 | 新增。`SchemaDDL`（逐字）+ advisory lock 串行化的 `Migrate` |
| `services/scheduler/internal/registry/grace.go` | 448 | 新增。护栏 §3.1 相位闸 + §3.2 重启 grace 期 + §3.3 丢弃熔断 |
| `services/scheduler/internal/registry_service.go` | 613 | 新增。5 个 RPC handler + 错误码映射 + 租约下限 + reclaim 定时器 |
| `services/scheduler/cmd/main.go` | +202 | 写面装配、迁移/grace 后台重试、`/healthz`、无 cluster 作用域时留冷 |
| `services/shared/config/config.go` | +197 | `scheduler.registry` 新增 7 个写面字段 + 默认值 + 校验 + env |
| `services/scheduler/internal/registry/store_postgres_test.go` | 1915 | 新增。61 个测试（多数需真 PG） |
| `services/scheduler/internal/registry/grace_test.go` | 387 | 新增。13 个测试 |
| `services/scheduler/internal/registry_service_test.go` | 957 | 新增。22 个测试（fake Store，无 DB） |
| `services/shared/config/registry_write_test.go` | 159 | 新增。5 个测试 |
| `services/scheduler/cmd/health_test.go` | 174 | 新增。4 个测试 |

**未碰**：`store.go`、`scheduler.proto` 与 pb、`contract*_test.go`、`postgres.go` / `registry.go` /
`lookup.go` / `reconcile.go`（阶段 0/1 已上线代码，零改动）、`src/` 下任何 Rust。

### 0.1 对外接线（契约测试 / Rust 侧要认的名字）

```go
func NewStore(ctx context.Context, cfg StoreConfig) (Store, error)   // 唯一构造器，不连库、不迁移
func Migrate(ctx context.Context, pool *pgxpool.Pool) error          // 也作为 Store 接口方法暴露
func (s *PostgresStore) WithGuards(*Grace, *DiscardBreaker) *PostgresStore
func (s *PostgresStore) ExtendLeases(ctx, clusterID string, ttl time.Duration) (time.Duration, int64, error)

func NewGrace(ttl time.Duration, log *zap.Logger) *Grace
func NewDiscardBreaker(maxRows int64, maxRatio float64, log *zap.Logger) *DiscardBreaker
func NewPausedRegistryService(log, store, grace, clusterID, defaultLeaseTTL, leaseTTLFloor) *PausedRegistryService
```

三处需要说明的设计取舍：

- **只有一个构造器**。logger 走 `StoreConfig.Logger`（主 agent 后加的字段）。
  claim 的 `claim_outcome = "rewound"` warn 是方案 §3.4 点名不能丢的
  （它是这条路径上**用户唯一会丢工作的事件**），`invariant_violation` 是谓词与 match 漂移的哨兵；
  它们既然是一等公民，就该进 config 而不是靠第二个构造器绕过去 —— 第二个构造器的存在本身
  就意味着"有条路没被契约测试覆盖"。
- **`WithGuards` / `ExtendLeases` 挂在具体类型上，不进 `Store`**。它们是 controller 对
  **自己缺席**的修复，不是任何节点能请求的操作；放进 `Store` 就是把它变成一个节点契约。
  `main.go` 因此对 `NewStore` 的返回值做一次类型断言，断言失败 `Fatal`（`NewStore` 只会返回这一个实现）。
- **`WithLeaseTTL` 返回的视图不拥有连接池**：视图是 per-request handler 手里的东西，
  它的 `Close()` 是 no-op。否则一个 handler 调 `Close()` 就把原 store 的池抽掉了。
  单测 `TestALeaseViewDoesNotOwnThePool` 钉死。

---

## 1. 与 `postgres.rs` 的逐条对齐表

**全局差异（对每条语句一致成立，下表不再重复）**：

| # | 差异 | 理由 |
|---|---|---|
| G-a | 所有 uuid 参数写成 `$n::uuid` / `$n::uuid[]`，Rust 直接绑 `Uuid` 类型 | Go 侧 id 从线上来是字符串。已实测 pgx 的 `string → uuid` / `[]string → uuid[]` / `[]*time.Time → timestamptz[]` / `json.RawMessage → jsonb` 四种编码均可用且逐字往返（含 `9007199254740993` 这种超 float64 精度的整数） |
| G-b | 读列表 = Rust 的 `ENTRY_COLUMNS` **再加** `lease_expires_at, sandbox_expires_at`，且 uuid 列 `::text AS 原名` | Go 的 `Entry`（冻结）内嵌 `Sandbox`，它**声明了**这两列，且 `Sandbox.LeaseExpired` 在 `LeaseExpiresAt == nil` 时回落到 `UpdatedAt`。只选 9 列会让每个 entry 都自称"租约在零时刻过期"。多读两列**不改变任何语句匹配哪些行**。变异 M23 验证 |
| G-c | 入参先做 uuid 形状校验，非法 ⇒ `ErrInvalidArgument`，语句根本不发 | Rust 的 `SandboxId` 是 `Uuid` 类型，非法值在类型层就不存在。Go 侧若交给 PG 的 cast，一个坏 id 会以 `22P02` 打挂整个 chunk 且错误里不含是哪个 id。**方向是 fail-closed**：错误 ≠ 空结果 |
| G-d | metadata 全程 `json.RawMessage` / `[]byte`，**不解码**，但写路径校验**顶层必须是 object** | 任务书要求不解码。副作用：Rust `decode` 里 "metadata is not a sandbox record" 那条 `InvalidRecord` 在 Go 侧**不存在** —— 那不是丢了检查，是搬回它该在的地方（节点收到 `metadata_json` 后自己 serde，同一条 `InvalidRecord` 在那里触发）。但**形状**必须在这里挡，见 §1.3 |
| G-e | 每条语句加 `context.WithTimeout`（默认 30s，可配） | Rust 无。是兜底，不是策略：`WithTimeout` 取调用方 deadline 与它的较早者，节点自己的 deadline 仍然说了算 |

### 1.1 十三个方法

| 方法 | Rust | SQL 差异 | 说明 |
|---|---|---|---|
| `begin_pause` | `:328-357` | **有一处刻意改动** + G-a/G-b/G-d | 见 §1.2 / §1.3 |
| `complete_pause` | `:396-402` | 无差异（仅 G-a） | CAS `generation = $2 AND state = 'publishing'`；0 行 ⇒ `ErrGenerationConflict` |
| `mark_local_only` | `:427-433` | 无差异（仅 G-a） | 同形 CAS；0 行**上报**不吞（`:443-446` 的理由逐字保留在注释里） |
| `get` | `:277-280` | G-a/G-b | 🔴 与阶段 0 的 `PostgresReader.Get` **刻意不同**：坏 id 在这里是 error，不是 "no row"。读面下游不删东西，写面下游会 |
| `get_many` | `:471-473` | G-a/G-b/G-c | 分块 1000 与 Rust 一致；任一块出错 / 任一行 decode 失败 / 任一 id 非法 ⇒ 整调用 `err` 且 **map 为 nil** |
| `claim_for_resume` | `:536-558` | 无差异（仅 G-a/G-b）+ grace 期第二形态 | 三分法逐字，含 `previous` CTE、`LEASE_EXPIRED` 逐字、四变体重读判定逐字。grace 期换用去掉租约接管臂的 `claimForResumeDurableOnlySQL`，见 §2.2 |
| `release_claim` | `:786-792` | 无差异（仅 G-a） | 🔴 **0 行静默成功**，与 `complete_pause`/`mark_local_only` 相反，与 Rust 一致 |
| `renew_lease` | `:670-681` | 无差异（仅 G-a） | 两个 `unnest` 数组、持有者谓词、`paused` 不在列表、空输入不发语句，全部保留 |
| `reclaim_expired_holdings` | `:725-754` | **加了一个 `count(*)`**（熔断用） | 两条件（租约过期 **且** deadline 过期）逐字，NULL deadline 永不匹配逐字，一个事务两条语句。熔断的计数在**同一事务内**，见 §2.3 |
| `mark_running` | `:816-825` | 无差异（仅 G-a） | 无状态前置、无 generation CAS、只有 claim guard；**永不建行**；0 行时重读只为打 warn，且重读的 error **向上传播**（Rust `:841` 的 `?`），因为 `false` 只能表示"集群不追踪" |
| `release_node_holdings` | `:869-899` | 无差异（仅 G-a） | `LIVE_HOLDINGS_OF_NODE` 逐字（含 `$2` 参数编号）、released/discarded 两出口、一个事务 |
| `remove` | `:929` | 无差异（仅 G-a） | 无 CAS、无状态前置、不看 rows_affected，与 Rust 一致（R2 §2.12 建议补 CAS，但冻结的 proto 明写 remove 不带 —— 见 §4） |
| `is_cluster_backed` | `:939-941` | **不移植** | 它是节点侧 trait 用来判"这个 registry 敢不敢让 reconciliation 删东西"的开关，中央侧的等价物是 `Grace`（cold ⇒ 一律 UNAVAILABLE）。Go 的 `Store` 接口里也没有它 |

### 1.2 唯一一处刻意的 SQL 改动：`begin_pause` 的时钟

```diff
- VALUES (..., $4, $5, $5, now() + make_interval(secs => $6::double precision))   -- $5 = 节点 Utc::now()
+ VALUES (..., $4::jsonb, now(), now(), now() + make_interval(secs => $5::double precision))
```

`ON CONFLICT DO UPDATE` 那段**一个字没改**（仍然是 `paused_at = EXCLUDED.paused_at` /
`updated_at = EXCLUDED.updated_at`）—— `EXCLUDED` 取的就是 VALUES 里的 `now()`，
所以改一处就够，两条路径同时切到 DB 时钟。

理由（R2 歧义 3 + 任务书 §3.2 的 ⚠️）：节点在这里绑自己的 `Utc::now()`，其余所有写路径绑 DB 的
`now()`，于是 `updated_at` 带着两个时钟源。以前没人读它；**现在 grace 期要从
`max(updated_at)` 推算停机时长**，节点时钟快就会把停机算短、grace 给不够 ——
正是这道护栏要防的批量抢占。`paused_at` 的调用方传值本来就被 Rust 自己忽略
（`paused_coordinator.rs:139` 传的值到不了 SQL）。

单测 `TestBeginPauseStampsTheDatabaseClock`：断言两个时间戳落在**两次 DB `now()` 之间**。
变异 M6（写回 `now() - interval '1 hour'` 模拟节点时钟漂移）被抓。

### 1.3 metadata 的形状必须挡在写路径上

写路径要求 `metadata` 是一个 **JSON object**：`null` / 数组 / 标量 / 空白全部
`ErrInvalidRecord`。**不看 object 里面有什么。**

🔴 `null` 是这条规则存在的理由，也是唯一一个能骗过"合法 JSON"检查的输入。
它是一份完全合法的 JSON 文档，JSONB 会老老实实存下来 —— 而节点侧的 `SandboxMetadata`
有 10 个必需字段，于是那一行此后**在节点上**解不出来。节点的 `decode` 被
`get` / `get_many` / `claim_for_resume` 三条路共用，所以**一行毒行会让那台机器的整批
`get_many` 报错、reconciliation 全线停摆**（R2 §6.4 末段，阶段 0 的 `invalid_rows`
指标就是为看见它而加的）。而且它只在下一次 resume 才暴露。

不看内容的理由同样明确：哪些字段属于那份文档是**节点的事**，这里定义一个 Go struct
去判断，就会静默丢掉这个 build 没听说过的字段（Rust 那侧不拒绝未知字段，所以不会报错）。

三种输入的含义（写在 `BeginPause` 的实现注释里）：

| 输入 | 含义 |
|---|---|
| `nil` / 空 / 空白 | 写方向的错误 ⇒ `ErrInvalidRecord` |
| 非 object 的合法 JSON（`null` / `[...]` / `"s"` / `42` / `true`） | 同上，**这是新加的** |
| 任意 object（含 `{}`、含未知键、含内层 `null`） | 接受，逐字入库 |
| 读方向的 `nil` | "本次响应没带" —— 批量读不带 metadata |

---

## 2. 三条护栏的落地

### 2.1 §3.1 全有或全无

| 位置 | 做法 |
|---|---|
| `Store.GetMany` | 任何后端错误 / 任何 decode 失败 / 任何非法 id ⇒ `return nil, err`。**返回的 map 是 nil，不是短 map** |
| `Grace` 三相位 | `PhaseCold`（migration 未成功）/ `PhaseGrace` / `PhaseServing`。cold ⇒ 每个 RPC `codes.Unavailable`，**store 一次都不碰**（`TestNothingIsAnsweredBeforeTheGateOpens` 断言 `store.calls` 为空） |
| `main.go` | migration + grace pass 在**后台带退避重试**，不 `Fatal`。scheduler 还要跑路由/发现/binding，因为 registry 库挂了就退出会把一个子系统的故障变成集群故障。闸门保证这样是安全的 |
| RPC 层 `fail()` | default 分支是 `Unavailable`，**不是 `Internal`，更不是 nil + 空响应** |
| 无 cluster 作用域 | 🔴 **注册但永远留冷**（见 §5.2）。写面被配置开了却没有 cluster id ⇒ grace pass 与 reclaim 定时器都不启动，每个 RPC 答 `UNAVAILABLE`，启动日志 error 级说明**后果**而不只是缺什么 |
| query-only 副本 | 🔴 **永不装配写面**。副本存在的意义是主 scheduler 重启时沙箱查询还能答；表的 shape 和那个会删行的 reclaim 定时器**只能有一个 owner**，副本也来一份就是第二个 owner，而且两者每次重启都会抢 bootstrap 的 advisory lock |

### 2.2 §3.2 租约冻结 grace 期

启动顺序：`Migrate` → `Grace.Enter`（跑续租延长）→ 相位转 `PhaseGrace` → TTL 后自动转 `PhaseServing`。

**续租延长用加法而不是绝对形式**：

```sql
SET lease_expires_at = COALESCE(p.lease_expires_at, p.updated_at)
                     + (SELECT downtime FROM observed)
                     + make_interval(secs => $2::double precision)
```

任务书写的是"把租约**延长** (停机时长 + lease_ttl)"，加法是它的字面实现，而且**自带上界**：
一行的租约最晚不会超过 (停机开始时刻 + ttl)，所以结果最晚不超过 `now + 2·ttl`，**与停机多长无关**；
反过来，停机**之前**就已经死透的行（比如某台机器一周前就没了）加同样的量之后**仍然是过期的**，
于是仍然可回收。绝对形式 `now() + downtime + ttl` 这两点都做不到：停机一周就会把所有行的租约
推后一周，把一台早已下线的机器的行硬冻一周。

- 停机时长 = `now() - max(updated_at)`，**取不到（表里没行）按一个完整 TTL 算**，不是 0
- `GREATEST(..., interval '0')` 兜住节点时钟跑到 DB 前面的存量行
- **不写 `updated_at`**：它正是这一趟读的证据，写了下次重启就是拿这次的清理时间当上次的停机时间
- grace 期内 `claim_for_resume` 换用去掉租约接管臂的语句 ⇒ 那些行落到重读分支、被答 `NotReady`
  （"停在 origin 上"，本来就是真话，而且节点已经会处理这个答案）。
  **`paused` 那一臂完全不受影响** —— 它本来就不看租约，冻它只会让每次重启后所有普通跨节点
  resume 白等一个租约
- `ReleaseNodeHoldings` **grace 期照常服务**：它的证据不是时钟，是"本机后继进程"这个因果事实，
  跟本进程离开过多久无关；withhold 它只会让刚重启的节点的沙箱多搁一个租约
- `reclaim` grace 期不跑（`RequireServing`）
- 可观测：`agentenv_scheduler_registry_write_phase` / `_grace_downtime_seconds` /
  `_grace_leases_extended` / `_grace_refusals_total` / `_grace_takeovers_withheld_total`；
  `/healthz` 返回 `{"registry_write":{"phase","ready","serving","grace_remaining_seconds","inferred_downtime_seconds"}}`

🔴 **`/healthz` 恒返回 200**，理由写在代码注释里：它说的是一个子系统的事，
挂成 readiness probe 会让 registry 库故障把路由和发现一起拖下线 —— 正是整条启动路径在避免的事。

#### 2.2.1 租约下限（`lease_ttl_floor`）

TTL 改成 per-call 之后，**"`ttl ≥ 3×reconcile_interval`（容忍两次漏续）"这条不变式没了归属**：
校验需要的两个量分居两个进程，controller 永远看不到节点的 `reconcile_interval`。

controller 侧因此加了一个下限：`effective = max(reported, floor)`，默认 30s，可配。

三条设计要点，都写进了 `clampLeaseTTL` 的注释（否则下一个人一定会想把它做"精确"）：

1. **它挡的是荒谬值，不是略短的值**。真正会发生的 bug 是报了 `0` 或 `1ms`
   （字段没填、毫秒当秒）。"比 3×interval 略短但非零"那种**已经被节点自己的配置校验挡住了**
   （`cfg.rs:455-458`），所以这个 floor 不需要精确 —— 要精确就得在这一侧猜一个它看不见的数
2. **抬高，不是拒绝**。租约更长 = 更难被抢 = fail-safe 方向；拒绝会把这个请求所属的
   pause / resume 整个打掉，而那个数字本来就只是建议性的。代价不对等
3. **只对"报了值"的请求生效**。没报 ⇒ 用 store 自己的默认值（那是已经过校验的 controller 配置），
   对它再 clamp 是自己怀疑自己

clamp 发生时打 warn（含"某节点报了 X，已抬到 Y"与后果一句话）+ metric
`agentenv_scheduler_registry_write_lease_ttl_clamped_total`。
默认 floor 必须**显著低于**真实租约，单测 `TestTheWriteSurfaceIsOffByDefault` 直接断言
`floor < lease_ttl`，否则它就从"挡荒谬值"变成"覆盖健康节点"。

### 2.3 §3.3 丢弃熔断

`DiscardBreaker{MaxRows, MaxRatio}`，**两条都判、取严**（任一超限即停手），默认 10 行 / 10%。

三处关键实现选择：

1. **计数在同一个事务里**（`countReclaimDiscardableSQL` 与 DELETE 共用事务快照），
   否则判的是一个快照、删的是另一个。变异 G13 验证
2. **超限时整个事务回滚，连 released 一起**。released 单独看是安全的，但丢弃数离谱说明
   两半共享的那个谓词（两个时钟 + 一个状态列）前提就不对，"跑一半没人信的 pass"比不跑更糟。
   变异 G11 验证
3. **只挂在 reclaim 上，不挂 `release_node_holdings`**：后者是节点启动驱动的、证据是因果的，
   给它加限流等于让一台机器的启动被限流卡住

---

## 3. 变异验证表

方法：把 `services/` 整树复制到 scratchpad（`.../scratchpad/mut/AgentENV/services`，
`deploy/` 与 `tests/` 用 symlink 保持相对路径），逐条打变异 → 跑指定测试 → 断言 FAIL → 还原。
驱动脚本 `.../scratchpad/mutate.py`，变异清单 `mut1..6.json`。
**主仓工作树全程未被变异污染。**

### 3.1 `store_postgres.go` / `migrate.go`（28 条）

| # | 变异 | 捕获它的测试 | 结果 |
|---|---|---|---|
| M1 | claim 的 `previous_state` 改从 RETURNING 的行读 | `TestAPausedSandboxIsClaimableImmediately` | ✅ |
| M2 | 租约接管臂加上 `running`/`resuming` | `TestALiveSandboxIsNeverTakenOverOnALapsedLease` | ✅ |
| M3 | `LEASE_EXPIRED` 去掉 `COALESCE` | `TestARowWrittenBeforeTheLeaseColumnExistedCountsAsExpired` | ✅ |
| M4 | `begin_pause` 的 upsert 清 `snapshot_id` | `TestAFailedPublishKeepsTheSnapshotTheSandboxAlreadyHad` | ✅ |
| M5 | `begin_pause` 去掉跨集群 `WHERE` | `TestBeginPauseRefusesToTakeOverAnotherClustersRow` | ✅ |
| M6 | `begin_pause` 改回节点时钟（漂移 1h） | `TestBeginPauseStampsTheDatabaseClock` | ✅ |
| M7 | `complete_pause` 去掉 generation CAS | `TestCompletePauseRefusesAStaleGeneration` | ✅ |
| M8 | `mark_local_only` 吞掉 0 行 | `TestADowngradeThatMatchesNothingIsReported` | ✅ |
| M9 | `release_claim` 把 0 行报成 conflict | `TestAReleaseThatMatchesNothingIsSilent` | ✅ |
| M10 | `mark_running` 去掉 claim guard | `TestMarkingRunningCannotEraseAnotherNodesClaim` | ✅ |
| M11 | `mark_running` 一律返回 tracked | `TestMarkingAnUntrackedSandboxRunningReportsThatItIsUntracked` | ✅ |
| M12 | `get_many` 跳过读不懂的行 | `TestABatchReadFailsWholeOnARowItCannotDecode` | ✅ |
| M13 | `get_many` 跳过非法 id | `TestABatchReadFailsRatherThanShorteningOnAMalformedID` | ✅ |
| M14 | `get` 把非法 id 答成 "no row" | `TestGetRefusesAMalformedIDRatherThanAnsweringNoRow` | ✅ |
| M15 | `renew_lease` 去掉持有者谓词 | `TestOnlyTheHolderCanRenewItsLease` | ✅ |
| M16 | `renew_lease` 把 `paused` 加进状态表 | `TestAParkedSandboxIsNotRenewed` | ✅ |
| M17 | reclaim 去掉 deadline 条件 | `TestReclamationNeedsBothClocks/still_within_its_deadline` | ✅ |
| M18 | reclaim 的 released 半边去掉租约条件 | `TestReclamationNeedsBothClocks/node_is_still_reporting` | ✅ |
| M19 | reclaim 把 NULL deadline 当过期 | `TestReclamationNeedsBothClocks/no_deadline_at_all` | ✅ |
| M20 | `release_node_holdings` 用 origin 判 resuming | `TestAnInterruptedResumeIsReleasedByTheNodeThatClaimedIt` | ✅ |
| M21 | DELETE 半边去掉 `snapshot_id IS NULL` | — | ⚠️ **等价变异**，见下 |
| M21b | UPDATE 半边把 `IS NOT NULL` 换成 `IS NULL`（两出口互换） | `TestASuccessorProcessReleasesWhatThePreviousOneWasRunning` | ✅ |
| M22 | `remove` 去掉集群作用域 | `TestRemoveIsScopedToItsCluster` | ✅ |
| M23 | 读列表去掉两个租约列 | `TestTheReadModelCarriesTheLeaseColumns` | ✅ |
| M24 | 解码器接受 `paused` 且无快照 | `TestGetRefusesARowItCannotDecode` | ✅ |
| M25 | 解码器接受未知 state | `TestAStateThisBuildDoesNotKnowIsRefusedRatherThanSkipped` | ✅ |
| M26 | migration 的 CHECK 多一个状态 | `TestMigrationIsTheNodesScriptVerbatim` + `TestMigrateCreatesTheStateConstraintTheNodeExpects` | ✅ |
| M27 | migration 换一个 advisory lock key | `TestTheSchemaLockIsTheOneTheNodesTake` | ✅ |
| M28 | migration 不释放 advisory lock | `TestTheSchemaLockIsReleased` | ✅ |

🔴 **M21 是等价变异，不是测试缺口。** 一个事务里 UPDATE 先跑，凡是有快照的行都已经被移出
`running`/`resuming`，所以后面那条 DELETE 的 `snapshot_id IS NULL` 在**当前语句顺序下**匹配集合不变。
保留它的理由有两条：与节点语句逐字一致；以及一旦有人调换两条语句的顺序，它是唯一挡住
"把可恢复的行删掉"的东西。M21b 从另一侧把两个出口钉死了。

### 3.2 `grace.go`（13 条）

| # | 变异 | 捕获它的测试 | 结果 |
|---|---|---|---|
| G1 | cold 闸门放行 | `TestNothingIsServedBeforeTheGateOpens` | ✅ |
| G2 | grace 期允许租约接管 | `TestTheGraceWindowWithholdsOnlyTheTakeoverArm` + `TestTheGateOpensInTwoStages` | ✅ |
| G3 | grace 期连普通 resume 一起拒 | `TestTheGraceWindowWithholdsOnlyTheTakeoverArm` | ✅ |
| G4 | grace 期永不结束 | `TestTheGateOpensInTwoStages` | ✅ |
| G5 | 续租延长改绝对形式 | `TestTheRestartPassLeavesARowThatWasAlreadyDeadAlone` | ✅ |
| G6 | 测不出停机时按 0 算 | `TestTheRestartPassAssumesAFullLeaseWhenItCannotMeasure` | ✅ |
| G7 | 续租延长顺手写 `updated_at` | `TestTheRestartPassDoesNotRewriteTheEvidenceItReads` | ✅ |
| G8 | 续租延长不带集群作用域 | `TestTheRestartPassIsScopedToItsCluster` | ✅ |
| G9 | 熔断改成两条都超才停（取宽） | `TestTheDiscardBreakerTakesTheStricterOfItsTwoLimits` | ✅ |
| G10 | 零值熔断器放行一切 | `TestTheDiscardBreakerFillsInWhateverWasLeftAtZero` | ✅ |
| G11 | 熔断触发仍提交 released | `TestTheDiscardBreakerAbandonsTheWholePass` | ✅ |
| G12 | reclaim 根本不接熔断器 | `TestTheDiscardBreakerAbandonsTheWholePass` | ✅ |
| G13 | 熔断计数跑到事务外 | `TestTheDiscardBreakerAbandonsTheWholePass` | ✅ |

### 3.3 `registry_service.go`（15 条）

| # | 变异 | 捕获它的测试 | 结果 |
|---|---|---|---|
| S1 | 读失败答成空列表 | `TestAFailedReadIsNeverAnEmptyList` | ✅ |
| S2 | `Aborted` 与 `FailedPrecondition` 合并 | `TestStoreErrorsKeepTheirDistinctions` | ✅ |
| S3 | 未分类后端错误映射成 `Internal` | `TestAFailedReadIsNeverAnEmptyList` | ✅ |
| S4 | 该拒的字段改成静默忽略 | `TestATransitionRefusesFieldsItsKindNeverWrites` | ✅ |
| S5 | 条件写缺 generation 时按 0 走 | `TestAConditionalTransitionMustQuoteAGeneration` | ✅ |
| S6 | `tracked=false` 报成错误 | `TestAnUntrackedSandboxIsASuccessfulAnswer` | ✅ |
| S7 | 缺席的 deadline 变成 epoch | `TestAnAbsentDeadlineStaysAbsent` | ✅ |
| S8 | 时间戳改用秒 | `TestTimesCrossTheWireAsMicroseconds` | ✅ |
| S9 | 忽略调用方的 lease TTL | `TestTheCallersLeaseLengthReachesTheStore` | ✅ |
| S10 | 服务别的集群的请求 | `TestRequestsForAnotherClusterAreRefused` | ✅ |
| S11 | reclaim 定时器 grace 期照跑 | `TestReclamationIsHeldBackUntilTheWindowCloses` | ✅ |
| S12 | `ReleaseNodeHoldings` grace 期被拒 | `TestTheGraceWindowStillServesANodeThatJustRestarted` | ✅ |
| S13 | 放行没有 entry 的 claimed | `TestAClaimWithNoEntryIsAFailure` | ✅ |
| S14 | 未知 claim outcome 被塞成别的变体 | `TestAllFourClaimOutcomesReachTheWire` | ✅ |
| S15 | begin_pause 无 metadata 也下发 | `TestBeginPauseWithoutMetadataIsRefused` | ✅ |

### 3.4 `config.go` / `main.go`（9 条）

| # | 变异 | 捕获它的测试 | 结果 |
|---|---|---|---|
| C1 | 写面接受缺失的 cluster 作用域 | `TestTheWriteSurfaceRequiresAClusterScope` | ✅ |
| C2 | 写面默认打开 | `TestTheWriteSurfaceIsOffByDefault` | ✅ |
| C3 | 丢弃比例可以大于 1 | `TestTheWriteSurfaceRejectsBadValues/discard_max_ratio_above_one` | ✅ |
| C4 | 写面的 duration 解码被丢 | `TestTheWriteBlockDecodesFromJSON` | ✅ |
| C5 | 写面跳过自己那段校验 | `TestTheWriteSurfaceRejectsBadValues` | ✅ |
| C6 | lease 默认值偏离节点默认 | `TestTheWriteSurfaceIsOffByDefault` | ✅ |
| H1 | `/healthz` 拿 registry 卡整个 pod | `TestHealthReportsThePhaseWithoutGatingTheProcess` | ✅ |
| H2 | `/healthz` 把 ready 与 serving 合成一个 | `TestHealthReportsThePhaseWithoutGatingTheProcess/grace` | ✅ |
| H3 | 只凭 flag 就起写面（没有 DSN） | `TestTheWriteSurfaceNeedsBothItsSwitches` | ✅ |
| H4 | query-only 副本也拿表的所有权 | `TestAQueryOnlyReplicaNeverOwnsTheTable` | ✅ |

### 3.5 契约二轮变更后新增的实现（17 条）

metadata 形状、migration 幂等硬门禁、租约下限、cluster 作用域运行时行为。

| # | 变异 | 捕获它的测试 | 结果 |
|---|---|---|---|
| N1 | metadata 检查退回 `json.Valid`（于是接受 `null`） | `TestBeginPauseRefusesMetadataThatIsNotAnObject` | ✅ |
| N2 | object 探针接受 nil map（即 `null`） | `.../json_null` | ✅ |
| N3 | object 探针拒绝空 object | `TestBeginPauseAcceptsAnyObject/empty_object` | ✅ |
| N4 | migration 的建表不再 `IF NOT EXISTS` | `TestMigrateIsIdempotentOverATableTheNodeCreated` + `TestATableTheControllerCreatedAcceptsTheNodesBootstrap` | ✅ |
| N5 | migration 的 `ADD COLUMN` 不再 `IF NOT EXISTS` | `TestMigrateIsIdempotentOverATableTheNodeCreated` | ✅ |
| N6 | migration 把 constraint 改名（drop 了不按原名加回） | 同上 + `TestMigrateCreatesTheStateConstraintTheNodeExpects` | ✅ |
| N7 | 租约下限方向反了（压低而不是抬高） | `TestALeaseTooShortToBeMeantIsRaisedNotRefused` | ✅ |
| N8 | 租约下限改成拒绝 | `.../one_millisecond` | ✅ |
| N9 | 零 floor 不回落默认 | `TestAZeroFloorFallsBackToTheDefault` | ✅ |
| N10 | 下限只在 begin_pause 上生效 | `TestTheFloorAppliesToEveryPathThatStampsALease` | ✅ |
| N11 | 没报 TTL 的请求也被 clamp | `TestAnUnreportedLeaseIsNotClamped` | ✅ |
| N12 | 缺 cluster id 变成启动致命错 | `TestAMissingClusterScopeDoesNotStopTheProcessStarting` | ✅ |
| N13 | 零 `lease_ttl_floor` 通过校验 | `TestTheWriteSurfaceRejectsBadValues/lease_ttl_floor` | ✅ |
| N14 | 默认 floor 与真实租约一样长 | `TestTheWriteSurfaceIsOffByDefault` | ✅ |
| N15 | `lease_ttl_floor` 解码被丢 | `TestTheWriteBlockDecodesFromJSON` | ✅ |
| N16 | 没有 cluster 作用域也开写面 | `TestAWriteSurfaceWithNoClusterScopeStaysCold` | ✅ |
| N17 | 全空白的 cluster id 算作有作用域 | 同上 | ✅ |

**合计 72 条变异 / 71 条被捕获 / 1 条等价变异（M21，已论证）。**

---

## 4. 门槛实际输出

主仓工作树，`services/` 目录下，**契约测试与阶段 0/1 测试全部在场**：

```
$ export GOWORK=off

$ go build ./...            → clean
$ go vet ./...              → clean
$ gofmt -l .                → clean
$ go mod tidy               → no drift

$ SCHEDULER_REGISTRY_TEST_REQUIRED=1 \
  SCHEDULER_REGISTRY_TEST_DSN='postgres://aenv:verify@127.0.0.1:15499/aenv?sslmode=disable' \
  go test -count=1 ./...
?   	agentenv/services/api/proto	[no test files]
?   	agentenv/services/gateway/cmd	[no test files]
ok  	agentenv/services/gateway/internal	0.038s
ok  	agentenv/services/scheduler/cmd	0.012s
ok  	agentenv/services/scheduler/internal	0.456s
ok  	agentenv/services/scheduler/internal/registry	26.343s
ok  	agentenv/services/shared/config	0.012s
ok  	agentenv/services/shared/logging	0.003s
?   	agentenv/services/shared/observability	[no test files]

$ # 同一次 -v 运行的计数
exit=0
PASS = 474
SKIP = 3
```

**那 3 个 SKIP 全部是既有的 Redis 集成测试**（`scheduler/internal/redis_store_test.go`，
本机没装 `redis-server`），与 registry 无关。
**`scheduler/internal/registry` 包 SKIP = 0**，`SCHEDULER_REGISTRY_TEST_REQUIRED=1` 已验过防假绿。

**测试规模**：本轮新增 105 个顶层测试 ——
`store_postgres_test.go` 61 / `grace_test.go` 13 / `registry_service_test.go` 22 /
`registry_write_test.go` 5 / `health_test.go` 4。
其中 65 个需要真 PG，其余 40 个纯逻辑、无 DB。

## 5. 与任务书不一致处

### 5.1 🔴 "4 个方法带 `WHERE generation = $n`" 实际是 **3 个**

任务书 §"你要做的" 第 1 条与 §3.3 都写 "只有 4 个方法是真 CAS"，proto 里
`TransitionSandboxRequest.expect_generation` 的注释也写 "the four transitions that are conditional"。

按 `postgres.rs` 逐行数，带 `generation = $2` 的只有 **3 条**：

| 方法 | 位置 |
|---|---|
| `complete_pause` | `postgres.rs:401` |
| `mark_local_only` | `postgres.rs:432` |
| `release_claim` | `postgres.rs:791` |

`claim_for_resume`（状态 + 租约 + 快照谓词）、`mark_running`（claim guard）、
`begin_pause`（无谓词）、`renew_lease`（持有者谓词）、`reclaim_*`（状态 + 双时钟）、
`release_node_holdings`（持有者 + 状态）、`remove`（仅集群）**都不带**。
R2 §3.4 的表格本身也只列出这 3 个。

**实现按 3 个走**：`COMPLETE_PAUSE` / `MARK_LOCAL_ONLY` / `RELEASE_CLAIM` 强制要求
`expect_generation`（缺 ⇒ `InvalidArgument`），另外 3 个 kind 带了 ⇒ 也是 `InvalidArgument`
（不是静默忽略，理由见 §5.3）。

✅ **已由主 agent 复核确认：是 3 不是 4，proto 注释已订正**，R2 §3.4 的"4"会在收尾时一并改。
（`postgres.rs` 那三处在主 agent 的行号里是 `:414` / `:445` / `:769`，与我数的同三条语句。）

### 5.2 写面需要 cluster 作用域，但缺它**不是启动致命错**

阶段 0 的读面允许 `ClusterID` 为空（= 读全库，`main.go` 只 warn）。写面不能照抄：
reclaim 会 **DELETE** 别的集群的行，grace 期又只延长一个集群的租约（另一个集群的写会在
"租约全是过期的"状态下被服务，正是护栏要防的）。

**第一版我把它做成了配置校验错误**（`write_enabled` 且 `cluster_id` 为空 ⇒ `LoadScheduler` 失败
⇒ `log.Fatalf`）。**已改掉**，理由是它把爆炸半径搞反了：

`cluster_id` 来自一个**可选的 Secret key**（阶段 0 的注释自己写着 "arrives from an optional
Secret key, so a missing key or a typo in its name leaves it empty"）。也就是说"缺"
是**发布时真的会发生**的事。而配置校验失败 ⇒ scheduler 进程根本起不来 ⇒ CrashLoop ⇒
**路由、发现、binding 全停** —— 用一个上周才打开的子系统，换掉整个集群的数据面。
这正是这条路径上所有其它决策（`New` 不连库、migration 后台重试、`/healthz` 恒 200）
都在避免的东西。

**现在的行为**：

| | |
|---|---|
| 进程 | 正常启动，路由 / 发现 / binding 不受影响 |
| PausedRegistry 服务 | **注册**，但闸门永远停在 `PhaseCold` ⇒ 每个 RPC `UNAVAILABLE` |
| grace pass / reclaim 定时器 | 不启动 |
| 日志 | error 级，且**说后果不只说缺什么**："…so it will stay cold: the restart grace pass and the reclamation timer will not run, and every registry RPC will be answered UNAVAILABLE until a cluster id is set" |
| `/healthz` | `phase: cold`（不是 `off` —— 它被配置开了，报 off 是撒谎） |

**为什么是"注册但留冷"而不是"不注册"**：不注册 ⇒ gRPC 答 `Unimplemented` ⇒ 读起来像
"这个 build 没有这个功能"，而事实是"配了但用不了"。`UNAVAILABLE` 两者都对，且是可重试语义。

**配置校验里其余的检查保留为致命错**，这个不对称是刻意的：那几项（`write_max_connections` /
`lease_ttl` / `lease_ttl_floor` / `reclaim_interval` / `discard_max_*`）**每一项都有默认值**，
所以要走到非法值必须有人显式写一个进去 —— 没有哪个缺失的 Secret 能造出它们。

RPC 层另有一道：请求的 `cluster_id` 与配置不符 ⇒ `InvalidArgument`。
每条 SQL 仍然逐字带 `WHERE cluster_id = $n`，这是**第二道**检查，不是把作用域降级成隐式。

### 5.3 严格于任务书的两处输入校验（都是 fail-closed 方向）

| 校验 | 为什么不是静默忽略 |
|---|---|
| kind 用不到的字段（generation / metadata / snapshot_id）带了就拒 | 调用方带 generation 是**以为这次写被 fence 了**，带 metadata 是**以为存进去了**。忽略的话响应是成功的（它确实成功了），错误从调用方那一侧完全不可见 |
| sandbox_id / cluster_id / snapshot_id 必须是规范 36 字符 uuid | 比 PG 自己的 uuid 解析严（PG 还认花括号和不带连字符的形式）。节点侧 id 来自 `Uuid` 的 `Display`，永远是规范形式；不规范的形式说明请求来自这个 build 不认识的调用方 |
| metadata 顶层必须是 JSON object（见 §1.3） | `null` 是合法 JSON，存得进去、读得出来，然后**在节点上**解不出来，一行毒死那台机器的整个对账 |

> ⚠️ 这两条可能影响 Slice C 的互通。若 Rust 客户端有任何一处会带上用不到的字段，
> 会拿到 `InvalidArgument`。**建议 Slice C 先按这个契约实现，不合适再回来放宽**。

---

## 6. 发现但没做的问题

### 6.1 ✅ 已解决：测试隔离冲突的真因是一个大小写陷阱

排查过程中我一度判断"契约测试用随机 cluster id + 不删行，与阶段 0 的
'表存在就 Fatal / cleanup DROP' 策略互斥"。**这个判断是错的**，主 agent 查出了真因：

契约测试**本来就是**私有 schema 实现的，但它的 schema 名带大写字母 ——
`CREATE SCHEMA "regtest_TestContractXxx"` 建的是大小写敏感的名字，而
`search_path=regtest_TestContractXxx` 里**未加引号的标识符会被 PostgreSQL 折叠成小写**，
于是建了一个 schema、去找另一个，40 个测试全部 `3F000: no schema has been selected to create in`
—— 表建不进私有 schema，行落进了 `public`。我看到的 `got 7` → `got 35`
是那些**跑挂的轮次**留下的历史垃圾，不是它正常运行的产物。

schema 名改全小写后 40/40 全绿、`regtest_%` 跑完剩 0 个。两套隔离本来就不见面。

> 留一条给下一个人：**私有 schema 的名字必须全小写**，否则 `search_path` 会静默指到
> 另一个不存在的 schema 上，而报错信息（"no schema has been selected to create in"）
> 完全不提大小写。本记录的 `testSchemaName` 走的是 `strings.ToLower(t.Name())`，
> 一开始就没踩到，所以也一开始就没发现这是个坑。

### 6.2 reclaim 的两条语句仍然是全表扫

`reclaim_expired_holdings` 在 `cluster_id + state + lease_expires_at + sandbox_expires_at`
上做条件扫描，**没有任何可用索引**（R2 §1.4 已点名）。中央定时器把它的频率从"每个节点各跑"
变成"一个进程跑"，规模小的时候无所谓。

**没做的原因**：任务书 §5.1 要求第一版 migration 是 `SCHEMA_DDL` **逐字复制、零新列**。
加一个部分索引虽然不是加列、节点的 `CREATE INDEX IF NOT EXISTS` 也不会跟它打架，
但那是对"逐字"的第一次破例，而破例的判断权在方案上而不在我这。
建议放进"删掉 `postgres` 后端那个 release"一起做：

```sql
CREATE INDEX IF NOT EXISTS paused_sandboxes_reclaim_idx
    ON paused_sandboxes (cluster_id, state)
 WHERE sandbox_expires_at IS NOT NULL;
```

### 6.3 `get_many` 仍然把 metadata 读回控制面

Rust 的 `get_many` 选 `ENTRY_COLUMNS`（含 metadata），Go 侧照抄了 —— 因为冻结的 `Entry`
类型有这个字段，只选一部分会让它变成一个静默的零值谎言。**它不上线**（`GetSandboxes`
的 proto 里没有 metadata 字段，方案 §3.1），所以 tonic 4 MiB 那个风险不存在；
但 DB → controller 这一跳仍然按 roster 大小拉全部 metadata。与今天的 Rust 完全同量，
不是回归。要优化的话是给 `Store` 加一个"不带 metadata 的批量读"，那要动冻结接口。

### 6.4 ✅ 已解决：lease TTL 归属与那条不变式的去处

TTL 归属已裁决为**属于 node**，实现走 `Store.WithLeaseTTL`；proto 让 `lease_ttl_millis`
跟着每一条会盖租约的请求走（除 `REMOVE`），所以**每条写 `lease_expires_at` 的路径
拿到的都是节点的值**。

原本随之丢失的那条不变式（`ttl ≥ 3×reconcile_interval`）已由 `lease_ttl_floor` 补上一个
**粗粒度**的兜底，见 §2.2.1 —— 它挡的是 `0` 和单位搞错，不试图复刻节点侧那条精确校验。

controller 配置里的 `lease_ttl` 现在只剩两个用途：请求没报时的回落，以及
**grace 期窗口长度与续租延长量**（那是进程级决定，节点值影响不到）。
⇒ 若某集群把节点的 `lease_ttl_secs` 调离 90s，**controller 的 `lease_ttl` 仍应跟着调**，
否则 grace 窗口与实际租约长度对不上。节点报的值与配置不一致时会按值打一次 warn
（`warnOnLeaseDisagreement`，per-value 去重，带锁）。

### 6.5 一个 controller 只能服务一个集群

见 §5.2。proto 的形状（每个请求带 cluster_id）允许多集群，实现按单集群 fail-closed
（配了 cluster id 就只服务那一个；没配就整个写面留冷）。
要放开需要：grace 期对表里出现的每个 cluster 各跑一趟延长、reclaim 定时器按 cluster 展开、
以及一个"这个 controller 负责哪些 cluster"的配置。**不建议在阶段 2 做** —— 它会让
grace 期的正确性依赖"发现所有 cluster"这件事本身不出错。

### 6.6 `remove` 仍然是无条件破坏性写

R2 §2.12 建议中央化之后给 `remove` 补 `expect_generation`（"否则中央化之后这是唯一一个
无条件破坏性写"）。冻结的 `Store.Remove` 与 proto 都没有它，`TRANSITION_KIND_REMOVE`
的注释明写 "Absent on mark_running and remove"。**按冻结契约实现**（与 Rust 一致）。
节点侧的守卫仍在它自己那边（`forget_sandbox` 先 `get`、`live_elsewhere` 就拒绝清行）。
如果要补，是一次 proto + `Store` 的同步变更，主 agent 定。

### 6.7 migration 的 advisory unlock 失败时会销毁连接

Rust 侧 unlock 失败只 `debug!` 一行，然后把连接还回池 —— 而 advisory lock 是 session 级的，
那条连接回池时还**握着集群级的锁**，后果是所有节点和 controller 的 bootstrap 一起卡住，
而且没有任何日志指向它。Go 侧改成：unlock 用**脱离调用方 cancel 的 context**跑
（`context.WithoutCancel`，因为最可能走到这里的原因就是调用方 ctx 被取消），
仍然失败就 `Hijack()` 把连接摘出池并 `Close()` —— 结束 session 就是释放锁。
`TestTheSchemaLockIsReleased` + 变异 M28 钉死。**这是 Go 侧比 Rust 严的一处，没有回移 Rust。**
