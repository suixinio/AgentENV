# R2：Rust `paused_registry` 语义规格书（供 Go 侧照做）

> 调研产物，只读不改代码。所有结论带 `file:line`，行号基于本 worktree
> `apps/AgentENV` 当前 HEAD。
>
> 覆盖范围：`src/orchestrator/paused_registry/{mod,types,postgres,disabled}.rs`、
> 消费方 `src/api/impls/{paused_recovery,paused_coordinator}.rs`、
> 配置 `src/cfg.rs:380-510`、装配 `src/bin/server.rs:147-182,292-322`、
> 测试 `tests/paused_registry.rs`。
>
> 服务于方案 `2026-08-19-agentenv-control-plane-refactor.md` 的 §3 四条护栏、
> §4 阶段 0/1/2、§5.1。

---

## 0. 一页速览

- **表**：`paused_sandboxes`，单表，主键 `sandbox_id`，节点自建（`postgres.rs:38-58`），
  advisory lock 串行化（`postgres.rs:151-181`）。
- **状态**：5 个（`publishing` / `paused` / `resuming` / `local_only` / `running`），
  CHECK 约束钉死（`postgres.rs:52-53`）。
- **仲裁令牌**：`generation`（BIGINT，CAS），**不是**所有方法都用它。
- **两个时钟**：`lease_expires_at`（节点还够不够得到 DB）与 `sandbox_expires_at`
  （沙箱自己的 deadline，**只由 `renew_lease` 写**，`postgres.rs:673`）。
- **最危险的契约**：`get` / `get_many` 的「行不存在」= **删除指令**
  （`mod.rs:120-121` → `paused_recovery.rs:691` / `:919-921`）。
- **最危险的 SQL**：`claim_for_resume` 三分法（`postgres.rs:548-552`）。
- **13 个 trait 方法**中，只有 **3 个**带 `WHERE generation = $n` 的真 CAS。

> 🔧 **勘误（2026-08-19，实施期核对）**：本行原写"4 个"，**是错的，实际 3 个** ——
> `complete_pause` / `mark_local_only` / `release_claim`（HEAD `224b70d` 上分别在
> `postgres.rs:416` / `:447` / `:771`）。`claim_for_resume` 与 `mark_running` 的谓词
> **只在 state 与 holder 上**，不带 generation。
> 本文 §3.4 的表格本身列的就是 3 个，错的只有这句速览。
> 取证见 [`_impl-D6-scheduler.md`](_impl-D6-scheduler.md) §5.1（Go 实现按 3 个走，
> 另外 3 个 kind 带了 `expect_generation` ⇒ `InvalidArgument`，不是静默忽略）。

---

## 1. 表结构权威版

### 1.1 完整 DDL（`postgres.rs:38-58`，逐字）

```sql
CREATE TABLE IF NOT EXISTS paused_sandboxes (
    sandbox_id     UUID        PRIMARY KEY,
    cluster_id     UUID        NOT NULL,
    state          TEXT        NOT NULL,
    generation     BIGINT      NOT NULL,
    origin_node_id TEXT        NOT NULL,
    snapshot_id    UUID,
    metadata       JSONB       NOT NULL,
    paused_at      TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS claimed_by_node_id TEXT;
ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_state_check;
ALTER TABLE paused_sandboxes ADD CONSTRAINT paused_sandboxes_state_check
    CHECK (state IN ('publishing', 'paused', 'resuming', 'local_only', 'running'));
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS sandbox_expires_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx ON paused_sandboxes (updated_at);
```

三条 ALTER 是**增量列**（`claimed_by_node_id` / `lease_expires_at` / `sandbox_expires_at`），
按注释（`postgres.rs:31-37`）它们的存在理由是 `CREATE TABLE IF NOT EXISTS` 对已存在的
表静默无操作。整段是一个隐式事务，节点要么看到旧形状要么看到新形状。

**Go 侧 migration 落地时**：DDL 语义可以逐字搬，但 `DROP CONSTRAINT IF EXISTS` +
`ADD CONSTRAINT` 这对必须保持顺序，否则重复运行会 42710。

### 1.2 每列含义

| 列 | 类型 | 含义 | 谁写 | 阶段 0 只读对账够不够 |
|---|---|---|---|---|
| `sandbox_id` | UUID PK | 沙箱 ID，跨节点唯一 | `begin_pause` 插入 | ✅ 只读 |
| `cluster_id` | UUID NN | 集群作用域。**不是装饰**：两个集群指同一个库时，没有它一个节点会认领另一个集群的沙箱、在自己的 repository 里找不到快照、然后把对方的行当 dangling 删掉（`postgres.rs:271-275`）| `begin_pause` 插入 | ✅ 只读（**每条查询都必须带**） |
| `state` | TEXT NN | 5 态之一，见 §3 | 6 个写方法 | ✅ 只读 |
| `generation` | BIGINT NN | 仲裁令牌。新插入 = 1，冲突 upsert = `+1`（`postgres.rs:339,343`）| 见 §3 bump 列 | ✅ 只读 |
| `origin_node_id` | TEXT NN | **当前持有沙箱的节点**：pause 它的那台，或最后 resume 它的那台。既是调度提示，也是「谁的本地副本是权威」的答案（`types.rs:67-71`）。claim 期间**故意不动**，仍指向持有本地 artifacts 的那台（`postgres.rs:496-498`）| `begin_pause` / `mark_running` | ✅ 只读 |
| `claimed_by_node_id` | TEXT | 取走认领权、正在把沙箱拉起来的节点。仅 `state = resuming` 时有值（`types.rs:72-75`）| `claim_for_resume` 置位；`begin_pause`/`mark_running`/`release_claim`/`release_node_holdings`/`reclaim_*` 清空 | ✅ 只读 |
| `snapshot_id` | UUID | 可重建的快照引用。`publishing` 期间为 NULL。**`Paused` 且为 NULL 是非法行**，读路径直接报 `InvalidRecord`（`postgres.rs:236-241`）| `complete_pause` 唯一写入者 | ✅ 只读 |
| `metadata` | JSONB NN | 与 node-local persister 同一份 `SandboxMetadata`，让异节点 resume 重建出同样的身份与配置（`types.rs:53-56`）| `begin_pause` | ✅ 只读（但注意它是 pause 那一刻的快照，**不是当前值**）|
| `paused_at` | TIMESTAMPTZ NN | 见下方 🔴 | `begin_pause` | ⚠️ 见 §6.4 时钟污染 |
| `updated_at` | TIMESTAMPTZ NN | 最后一次写的时刻 | 几乎所有写方法 | ⚠️ 见 §6.4 |
| `lease_expires_at` | TIMESTAMPTZ | 租约到期时刻。NULL 视为**已过期**（`LEASE_EXPIRED` 的 `COALESCE`，`postgres.rs:88`）| 见 §3 | ❗ **不在 `ENTRY_COLUMNS` 里**，阶段 0 必须显式 SELECT |
| `sandbox_expires_at` | TIMESTAMPTZ | 沙箱自己的 deadline，由持有者上报。**只由 `renew_lease` 写**（`postgres.rs:673`），别处一律不碰。NULL 永不匹配回收条件（`postgres.rs:717-719`）| `renew_lease` 唯一写入者 | ❗ 同上，不在 `ENTRY_COLUMNS` |

🔴 **`paused_at` 的实际写入值不是调用方传的值。** `begin_pause` 绑定 `$5 = Utc::now()`
（`postgres.rs:302,363`），而调用方在 `paused_coordinator.rs:139` 传的
`paused_at: DateTime::<Utc>::from(outcome.metadata.created_at)` **被完全忽略**。
同理被忽略的还有 `entry.state` / `entry.generation` / `entry.claimed_by_node_id` /
`entry.snapshot_id` / `entry.cluster_id` / `entry.updated_at`——`begin_pause` 只用
`sandbox_id`、`origin_node_id`、`metadata` 三个字段。Go 侧的 `TransitionSandbox`
接口不要照抄这个「传一整个 entry 但只用三个字段」的形状。

### 1.3 `ENTRY_COLUMNS`（读路径唯一的列清单，`postgres.rs:21-22`）

```
sandbox_id, cluster_id, state, generation, origin_node_id,
claimed_by_node_id, snapshot_id, metadata, paused_at, updated_at
```

**两个租约列不在其中** ⇒ `PausedSandboxEntry`（`types.rs:57-81`）里没有任何租约/deadline
字段 ⇒ **今天所有 Rust 消费方都看不见租约**。阶段 0 的只读对账要算 `lease_expiring`，
必须自己 SELECT `lease_expires_at, sandbox_expires_at`。

### 1.4 索引

| 索引 | 服务谁 |
|---|---|
| PK `sandbox_id` | `get` / `get_many`（`= ANY($2)`）/ 所有单行 UPDATE |
| `paused_sandboxes_origin_node_idx (origin_node_id)` | `release_node_holdings` 的两条语句、`renew_lease` |
| `paused_sandboxes_updated_at_idx (updated_at)` | 无代码消费方（`reclaim_expired_holdings` 走的是 `lease_expires_at` / `sandbox_expires_at`，都没索引）。**这个索引今天是空转的**；§3.2 那个「从 `max(updated_at)` 推断停机时长」的 grace 算法恰好会用上它 |

⚠️ `reclaim_expired_holdings` 的两条语句（`postgres.rs:731-735,749-753`）
在 `cluster_id + state + lease_expires_at + sandbox_expires_at` 上做条件扫描，
**没有任何可用索引**，是全表扫。规模小的时候无所谓，Go 侧中央定时器跑它之前
应当补一个部分索引。

### 1.5 只读对账 vs 写路径

- **只读对账够用**：`sandbox_id / cluster_id / state / generation / origin_node_id /
  claimed_by_node_id / snapshot_id / updated_at / lease_expires_at / sandbox_expires_at`
- **写路径才需要**：`metadata`（重建请求的全部载荷，见 `paused_recovery.rs:950-972`
  的 `restore_request`）、`paused_at`（无人消费，纯记录）

---

## 2. `PausedSandboxRegistry` 13 个方法逐个规格

trait 定义：`mod.rs:81-252`。错误类型 `PausedRegistryError`：`mod.rs:45-65`，三个变体
`Backend{operation, source}` / `InvalidRecord{sandbox_id, reason}` / `GenerationConflict{sandbox_id, expected}`。
`GenerationConflict` 明写「Never fatal by itself: the caller re-reads and decides」（`mod.rs:60-62`）。

---

### 2.1 `begin_pause(&entry) -> BeganPause`

`mod.rs:88` / `postgres.rs:294-387`

**前置条件**：调用方就是正在跑这台沙箱的节点。**无 CAS，无状态前置**——可以从任意状态
（含 `running` / `resuming` / `paused`）打进 `publishing`。注释给的理由：
「A sandbox is only ever paused from the one node running it, so nothing else can
be rewriting this row concurrently」（`postgres.rs:320-321`）。

**SQL**（`postgres.rs:328-357`）：两个 CTE。

```sql
WITH previous AS (
    SELECT snapshot_id FROM paused_sandboxes
     WHERE sandbox_id = $1 AND cluster_id = $2
),
upserted AS (
    INSERT INTO paused_sandboxes (...)
    VALUES ($1, $2, 'publishing', 1, $3, NULL, NULL, $4, $5, $5,
            now() + make_interval(secs => $6::double precision))
    ON CONFLICT (sandbox_id) DO UPDATE SET
        state              = 'publishing',
        generation         = paused_sandboxes.generation + 1,
        origin_node_id     = EXCLUDED.origin_node_id,
        claimed_by_node_id = NULL,
        metadata           = EXCLUDED.metadata,
        paused_at          = EXCLUDED.paused_at,
        updated_at         = EXCLUDED.updated_at,
        lease_expires_at   = EXCLUDED.lease_expires_at
    WHERE paused_sandboxes.cluster_id = EXCLUDED.cluster_id
    RETURNING generation
)
SELECT upserted.generation, previous.snapshot_id AS previous_snapshot_id
  FROM upserted LEFT JOIN previous ON TRUE
```

三个要点：

1. **`snapshot_id` 故意不在 UPDATE 列表里**（`postgres.rs:308-315`）：清掉它会让「pause 了
   但上传失败」的行指向 NULL，而一个完好的旧快照躺在 repository 里没人引用；此刻丢掉
   origin 节点就是沙箱直接消失——正是这个 registry 存在的理由。
2. **`previous` CTE 读的是语句开始时的快照**，这是唯一能知道「这次 pause 顶掉了哪个
   快照」的办法；`RETURNING` 只能给出新行（`postgres.rs:316-321`）。
3. **`WHERE paused_sandboxes.cluster_id = EXCLUDED.cluster_id`**：跨集群保护。不匹配时
   `upserted` 返回 0 行 → 外层 SELECT 返回 0 行 → `fetch_optional` → `None` →
   `InvalidRecord{reason: "registry already holds this sandbox for a different cluster"}`
   （`postgres.rs:368-372`）。

**返回**：`BeganPause { generation, previous_snapshot_id }`（`types.rs:84-95`）。
`previous_snapshot_id` 的删除责任在调用方，且**故意推迟到 `complete_pause` 成功之后**
才删（`paused_coordinator.rs:172-183`），保证沙箱运行期间始终有一个 durable 快照垫底。

**失败处理**（`paused_coordinator.rs:143-154`）：任何错误 → warn + `return None` →
本次 pause 不进集群，沙箱只在本节点可 resume。**pause 本身不失败**。

---

### 2.2 `complete_pause(&sandbox_id, generation, &snapshot_id) -> ()`

`mod.rs:91-96` / `postgres.rs:389-423`

**真 CAS**：`WHERE sandbox_id = $1 AND cluster_id = $5 AND generation = $2 AND state = 'publishing'`
（`postgres.rs:400-401`）。

写：`state = 'paused'`、`snapshot_id = $3`、`updated_at = now()`、
`lease_expires_at = now() + ttl`。**不 bump generation。**

`rows_affected() == 0` ⇒ `GenerationConflict`（`postgres.rs:413-418`）。

**调用方怎么处理 CAS 失败**（`paused_coordinator.rs:185-199`）：
快照已经 durable 但登记表不认——要么写从没落，要么发布期间别人把沙箱推进了别的状态。
无论哪种，刚上传的东西都不会有人引用 ⇒ 调 `discard_unreferenced_snapshot`
（`paused_coordinator.rs:330-359`），而它**先重读一次行**：只有确证行没引用它才删；
读不到就留在 repository 里并打 `error!` 让人工回收——「an operator can collect garbage
but cannot un-delete a referenced snapshot」（`paused_coordinator.rs:328-329`）。

---

### 2.3 `mark_local_only(&sandbox_id, generation) -> ()`

`mod.rs:98-105` / `postgres.rs:425-455`

**真 CAS**，谓词与 `complete_pause` 完全同形：`generation = $2 AND state = 'publishing'`。
写 `state = 'local_only'`、`updated_at = now()`、`lease_expires_at = now() + ttl`。
**不 bump generation。**

🔴 **行保留不删**（`mod.rs:100-104`）：沙箱确实 paused 了，只是除了 origin 节点谁都
resume 不了。删掉它会让「还停在自己节点上」和「已经在别处 resume 了 / 已经销毁了」
无法区分，随后 reconciliation 会把 origin 节点的本地记录扔掉——而那是**唯一的**副本。

`rows_affected() == 0` ⇒ `GenerationConflict`，而且注释明写这是**故意上报而不是吞掉**
（`postgres.rs:443-446`）：静默匹配不到会让行卡死在 `publishing`，此后每次异节点 resume
都会把一个早就放弃的上传报成「还在上传中」。调用方（`paused_coordinator.rs:213-219`）
只 warn，没有修复手段——「The caller cannot repair it, but it can say so」。

---

### 2.4 `get(&sandbox_id) -> Option<PausedSandboxEntry>`

`mod.rs:108` / `postgres.rs:457-459` → `fetch`（`postgres.rs:276-289`）

```sql
SELECT {ENTRY_COLUMNS} FROM paused_sandboxes WHERE sandbox_id = $1 AND cluster_id = $2
```

`decode`（`postgres.rs:205-267`）里有一条**读时不变量**：
`state == Paused && snapshot_id IS NULL` ⇒ `InvalidRecord`（`postgres.rs:236-241`）。
Go 侧必须保留：这类行不能被「跳过」，跳过 = 变成缺行 = 删除指令。

---

### 2.5 `get_many(&[SandboxId]) -> HashMap<SandboxId, PausedSandboxEntry>`

`mod.rs:110-125` / `postgres.rs:461-489`

**契约原文（`mod.rs:120-121`）**：

> A sandbox missing from the returned map has no row — the same answer
> `get` gives as `None`, and **never "we did not look"**.

分块 1000（`GET_MANY_CHUNK`，`postgres.rs:61`），每块
`WHERE cluster_id = $1 AND sandbox_id = ANY($2)`（`postgres.rs:472-473`）。
任一块出错 ⇒ 整个调用 `Err`；任一行 decode 失败 ⇒ 整个调用 `Err`（`postgres.rs:483`
的 `?`）。**这就是"全有或全无"的实现方式。**

#### 🔴 空结果 → `Superseded::Gone` → 删本地的两条确切代码路径

**路径 A：删本地 paused artifacts（不可逆）**

```
paused_recovery.rs:676   let rows = match self.paused.registry().get_many(&ids).await {
paused_recovery.rs:678-684   Err(err) => { warn!("registry unreadable; stopping paused-record
                             reconciliation"); return; }          ← 这是唯一的护栏
paused_recovery.rs:690-691   let superseded = match rows.get(&sandbox_id) {
                                 None => Superseded::Gone,        ← 空 = 删
paused_recovery.rs:698-700   self.orchestrator.discard_local_paused_record(sandbox_id).await
   ↓
service.rs:1173-1205    update_state_if_state(Paused → Killing)
service.rs:1192           self.store.remove(&sandbox_id)
service.rs:1193-1199      self.persister.delete_record_and_artifacts(&sandbox_id)   ← 删盘
service.rs:1200-1201      self.release_image_refs(RuntimeImageOwner::PausedSandbox(...))
```

进入这条路径的前置过滤（`paused_recovery.rs:657-673`）：只有
`paused_record_cluster_registration(id) != ClusterRegistration::Never` 的记录才被送进
`get_many`。也就是说「从没向集群报备过」的记录天然免疫（`persistence/mod.rs:70-79`）。

**路径 B：拆掉正在跑的沙箱（杀 VM）**

```
paused_recovery.rs:587   let rows = match self.paused.registry().get_many(&ids).await {
paused_recovery.rs:589-595   Err(err) => { warn!("registry unreadable; stopping running-sandbox
                             reconciliation"); return; }          ← 唯一护栏
paused_recovery.rs:599   let Some(superseded) = running_supersession(rows.get(&sandbox_id),
                                                                    &registered_as) else { continue };
   ↓
paused_recovery.rs:919-921   let Some(entry) = entry else { return Some(Superseded::Gone); };
paused_recovery.rs:610-613   self.orchestrator.discard_superseded_sandbox(sandbox_id).await
   ↓
service.rs:949-956       delete_sandbox_inner(id, ClusterDisposition::KeepClusterRecord)
```

前置过滤（`paused_recovery.rs:574-584`）：只有本进程通过 `mark_running` 拿到**确认**
（`paused_coordinator.rs:240-256`，只有 `Ok(true)` 才 `observe(confirmed=true)`）的沙箱
才进 `get_many`。`running_supersession` 的文档把这条写死（`paused_recovery.rs:910-914`）：
「`entry: None` is only reachable for a sandbox this process registered … For an
unregistered sandbox `None` means nothing at all, and reading it as "gone" would tear
down a sandbox seconds after it was created.」

**第三条（单点版）**：`superseded_by_cluster` 用 `get`（`paused_recovery.rs:778-790`）——
`Ok(None)` ⇒ `Superseded::Gone`；`Err` ⇒ `Err(())` ⇒ 调用方 `discard_if_superseded`
不删（`paused_recovery.rs:736-738`）。

> **§3.1 验收条件的直接推论**：Go 侧 `GetSandboxes` 只要有一次把「后端出错」映射成
> 「空 map」，上面三条路径立即变成批量删用户工作区。node 侧客户端必须把 gRPC 层任何
> 非 OK（含 deadline / EOF / 部分流）映射成 `PausedRegistryError::Backend`，才能落进
> `:589` / `:678` 那两个 `return` 分支。

---

### 2.6 `claim_for_resume(&sandbox_id, node_id) -> ResumeClaim`

`mod.rs:127-132` / `postgres.rs:491-644`

#### 三分法 SQL（`postgres.rs:536-558`，逐字）

```sql
WITH previous AS (
    SELECT state AS previous_state
      FROM paused_sandboxes
     WHERE sandbox_id = $1 AND cluster_id = $4
),
claimed AS (
    UPDATE paused_sandboxes
       SET state = 'resuming', claimed_by_node_id = $2,
           generation = generation + 1, updated_at = now(),
           lease_expires_at = now() + make_interval(secs => $3::double precision)
     WHERE sandbox_id = $1
       AND cluster_id = $4
       AND snapshot_id IS NOT NULL
       AND (state = 'paused'
         OR (state IN ('publishing', 'local_only') AND COALESCE(lease_expires_at, updated_at) < now()))
    RETURNING {ENTRY_COLUMNS}
)
SELECT claimed.*, previous.previous_state
  FROM claimed JOIN previous ON TRUE
```

（`{LEASE_EXPIRED}` 已就地展开，定义在 `postgres.rs:88`。）

**逐条件解释**

| 条件 | 为什么 |
|---|---|
| `snapshot_id IS NOT NULL` | 没有快照就无从重建，给出去的认领权调用方也用不了。测试 `a_sandbox_that_never_published_is_never_claimable`（`tests/paused_registry.rs:682`）钉死 |
| `state = 'paused'` **任意节点、不看租约** | 「nobody is holding the sandbox and its snapshot is durable」（`postgres.rs:505-506`）。让租约也管这一档会给每次普通跨节点 resume 平白加延迟，零收益（`tests/paused_registry.rs:506-518`） |
| `state IN ('publishing','local_only') AND LEASE_EXPIRED` | 这两态说明「某节点 pause 了它但快照始终没进 repository」。**VM 已经停了**，所以在别处重建**不可能造成双活**——只会回退到上一次 pause 留下的快照（`postgres.rs:508-514`）。这是真损失，所以要等满一个租约，并作为降级事件打 `warn` |
| `running` / `resuming` **永不可抢，任凭租约过期多久** | `postgres.rs:516-527` 逐字：「Their VM may still be up: a lapsed lease says the holder cannot reach this database, which a partitioned node — still running every sandbox it has, still being routed traffic — satisfies exactly as well as a dead one.」e2b 从同一事实的另一侧做同样的判断：resume 命中 store 里的沙箱直接 409，孤儿清扫只杀 store 里完全没记录的 |

`LEASE_EXPIRED` 里 `COALESCE(lease_expires_at, updated_at)` 的作用（`postgres.rs:85-87`）：
把这列出现之前写的行**视为已过期**，这是安全方向——活着的持有者一个 interval 内就会刷新，
死的永远不会。

#### `previous` CTE：为什么 `previous_state` 不能从 `entry` 读

`types.rs:154-168` 的 🔴 注释：

> It cannot be read off `entry`: the claim is a single conditional `UPDATE`, and
> `RETURNING` hands back the row as the statement left it — `state` is therefore
> always `Resuming` there, whatever it was a moment earlier. **Reading it from
> `entry` is how this claim came to report every ordinary resume as a lease
> takeover for months.**

两个 CTE 看的是同一个语句开始快照，所以 `previous` 看到的是 UPDATE **找到时**的行
（`postgres.rs:529-535`）。这条 bug 之所以能潜伏数月，是因为
「every assertion in this file looked only at the variant and the snapshot —
never at what the claim said it replaced」（`tests/paused_registry.rs:533` 上方文档）。

#### `ResumeClaim` 四个变体 ← 什么 SQL 结果

| 变体 | 产生条件 | 代码 |
|---|---|---|
| `Claimed { entry, previous_state }` | 上面的 UPDATE 匹配到 1 行 | `postgres.rs:609-612` |
| `NotFound` | UPDATE 0 行，且**重读**（`fetch`）也没有行 | `postgres.rs:619-620` |
| `NotReady { origin_node_id }` | UPDATE 0 行，重读到 `publishing` 或 `local_only`（即租约**还活着**，仍停在 origin 上） | `postgres.rs:624-628` |
| `Conflict { origin_node_id }` | 重读到 `resuming` / `running`（取 `claimed_by_node_id`，回退 `origin_node_id`）**或** `paused`（输给了另一个已释放的 claimer） | `postgres.rs:633-641` |

⚠️ `Conflict` 一个变体承担了两种事实：「活在别处」和「刚输掉一次竞态」。
Go 侧的 `AcquireSandbox` 如果要拆，这是拆点。

三个 `previous_state` 分支的日志（`postgres.rs:577-607`）：

- `Paused` ⇒ `debug!(claim_outcome = "durable")`
- `Publishing | LocalOnly` ⇒ **`warn!(claim_outcome = "rewound", previous_state, previous_holder)`**
  ——§3.4 验收条件点名不能丢的就是这条
- 其它（不可达）⇒ `error!(claim_outcome = "invariant_violation", "a live sandbox may now exist twice")`
  ——刻意留的哨兵，走到这里说明 WHERE 谓词和 match 已经漂移

#### 调用方

`arbitrate_resume`（`paused_recovery.rs:160-185`）：registry 报错 ⇒ **fail-open**
`ResumeArbitration::Proceed`（「a registry that cannot answer must not be able to stop
a node from resuming a sandbox sitting on its own disk」`paused_recovery.rs:157-159`）。

`arbitration()`（`paused_recovery.rs:820-835`）：**答案指向本节点自己就不算拒绝**——
`NotReady{origin == self}` 和 `Conflict{origin == self}` 都判 `Proceed`。

失败回滚：`restore_claimed_sandbox` 在三处调 `release_claim`
（`paused_recovery.rs:275, 306, 325`），唯独「快照在 repository 里没了」那条走
`registry.remove()` 而不是 release（`paused_recovery.rs:293-303`）。

---

### 2.7 `release_claim(&sandbox_id, generation) -> ()`

`mod.rs:135` / `postgres.rs:781-803`

CAS：`WHERE sandbox_id = $1 AND cluster_id = $4 AND generation = $2 AND state = 'resuming'`。
写 `state = 'paused'`、`claimed_by_node_id = NULL`、`updated_at = now()`、
`lease_expires_at = now() + ttl`。**不 bump generation。**

🔴 **不检查 `rows_affected`，0 行静默成功**——与 `complete_pause` / `mark_local_only`
的处理方式相反。调用方 `paused_recovery.rs:795-808` 只有在 `Err` 时才 warn。

⚠️ **Go 移植注意的不一致**：这里写 `lease_expires_at = now() + ttl`，而
`release_node_holdings`（`postgres.rs:874`）和 `reclaim_expired_holdings`
（`postgres.rs:730`）在同样落到 `paused` 时写的是 `lease_expires_at = now()`，
理由是「`paused` 意味着没人持有，留一个看起来还活着的租约只会让下一个读者困惑」
（`postgres.rs:866-868`）。三处应当一致；`paused` 态的租约本来就不参与可认领性判断，
所以这是观测口径问题不是正确性问题——但阶段 0 算 `lease_expiring` 时会被它污染。

---

### 2.8 `renew_lease(node_id, &[HeldSandbox]) -> u64`

`mod.rs:137-162` / `postgres.rs:646-696`

`held.is_empty()` ⇒ 直接 `Ok(0)`，不发语句（`postgres.rs:647-649`；
测试 `a_batch_read_of_nothing_asks_nothing` 是 `get_many` 的同款）。

**批量语义**：两个数组 `unnest` 成一张临时表再 join：

```sql
UPDATE paused_sandboxes AS p
   SET lease_expires_at   = now() + make_interval(secs => $1::double precision),
       sandbox_expires_at = v.expires_at,
       updated_at         = now()
  FROM (SELECT unnest($3::uuid[]) AS sandbox_id,
               unnest($4::timestamptz[]) AS expires_at) AS v
 WHERE p.sandbox_id = v.sandbox_id
   AND p.cluster_id = $2
   AND ((p.state IN ('running', 'publishing', 'local_only') AND p.origin_node_id = $5)
     OR (p.state = 'resuming' AND p.claimed_by_node_id = $5))
```

- **调用方传整个本地 roster，由谓词而不是调用方决定哪些有资格续**
  （`mod.rs:152-154`、`postgres.rs:654-657`）——一个节点不能靠列举一台它不持有的沙箱
  来延长别人的租约。测试 `only_the_holder_can_renew_its_lease`（`tests:460`）。
- **`paused` 故意不在列表里**（`postgres.rs:659-661`）：那个状态意味着没人持有，续了也只是噪音。
- 返回 `rows_affected`，即实际续上的行数。

#### `HeldSandbox.expires_at` 为什么走参数不走行内 metadata

`types.rs:99-109` 逐字：

> `metadata` is whatever the sandbox looked like when it was paused; a resume may set
> a different timeout, and callers extend timeouts on live sandboxes all the time.
> Reading the deadline out of the row would therefore reclaim sandboxes that still had
> hours to run.
>
> `None` means the sandbox has no deadline at all, which is not the same as
> "unknown": it is a sandbox that was asked never to expire, and reclamation leaves it
> alone forever.

而且两个事实**必须同一条语句写**（`postgres.rs:663-668`）：「when did the holder last
check in」和「how long was this sandbox supposed to live」是回收要比对的一对，分两个时刻
写会让一行拿着 A 时刻的 deadline 和 B 时刻的租约。

调用方 `renew_paused_leases`（`paused_recovery.rs:345-387`）：
`list_sandboxes_filtered(default)` 拿**全部**本地沙箱（不分状态），
`expires_at` 取自**活沙箱的当前元数据**（`paused_recovery.rs:368-374`），不是行里的。

---

### 2.9 `reclaim_expired_holdings() -> ReclaimedHoldings`

`mod.rs:164-194` / `postgres.rs:698-779`

**一个事务，两条语句**。

released（`postgres.rs:725-737`）：

```sql
UPDATE paused_sandboxes
   SET state = 'paused', claimed_by_node_id = NULL,
       generation = generation + 1, updated_at = now(), lease_expires_at = now()
 WHERE cluster_id = $1
   AND snapshot_id IS NOT NULL
   AND state IN ('running', 'resuming')
   AND COALESCE(lease_expires_at, updated_at) < now()
   AND sandbox_expires_at < now()
```

discarded（`postgres.rs:746-754`）：同样的 WHERE，但 `snapshot_id IS NULL` 且是 `DELETE`。

#### 两个条件的安全性论证（原文）

`mod.rs:173-179`：

> 🔴 The deadline, not the lease, is what makes this safe. A lapsed lease alone says
> only that the holder cannot reach the database; acting on it is the mistake
> `claim_for_resume` exists to avoid. But a sandbox that is *also* past the deadline its
> own user gave it has no claim on being kept alive: it should already have been
> evicted, and would have been if anyone could still reach the node. **Reclaiming it is
> enforcing the timeout, not guessing at the node's health.**

`mod.rs:190-193`：

> Both conditions are required, and the lease one is what keeps this out of the way of
> the normal path: a reachable node evicts its own expired sandboxes itself, pausing
> them properly and publishing a fresh snapshot. Only when nobody has renewed for a full
> lease does the cluster step in.

`postgres.rs:717-719`：`sandbox_expires_at` 为 NULL **永不匹配**，覆盖两种情况——
「被要求永不过期的沙箱」和「持有者自这列存在以来就没续过的行」，两者都是安全答案：别碰。

**为什么它不该进 RPC 面**（方案 §4 阶段 2）：`reclaim_expired_sandboxes`
（`paused_recovery.rs:464-486`）的文档自己写着「Unlike everything else in this module,
this pass is not about *this* node — any node runs it against the whole cluster, and
running it from several at once is harmless because the statement is a single
conditional `UPDATE`」（`paused_recovery.rs:449-454`）。它本来就是集群兜底。

**不在启动路径上跑**（`bin/server.rs:313-317`）：「the rows it collects have been stranded
for at least a sandbox lifetime already」。

---

### 2.10 `mark_running(&sandbox_id, node_id) -> bool`

`mod.rs:196-213` / `postgres.rs:805-852`

```sql
UPDATE paused_sandboxes
   SET state = 'running', origin_node_id = $2, claimed_by_node_id = NULL,
       generation = generation + 1, updated_at = now(),
       lease_expires_at = now() + make_interval(secs => $3::double precision)
 WHERE sandbox_id = $1
   AND cluster_id = $4
   AND (claimed_by_node_id IS NULL OR claimed_by_node_id = $2)
```

🔴 **没有状态前置、没有 generation CAS**，唯一的守卫是 claim guard。可以从任意状态打进
`running`。两条不变量：

1. **Never creates a row**（`mod.rs:204-205`、`postgres.rs:806-809`）：集群没听说过的沙箱
   要保持没听说过，否则每个在有 node-local 历史的节点上的 resume 都会为没有快照垫底的
   沙箱造行。
2. **claim guard**（`postgres.rs:810-815`）：没有它，盲写会在 claim 中途清掉
   `claimed_by_node_id`，两个节点都以为自己持有 ⇒ 第二个活副本。

**返回值 `bool` 的语义（`mod.rs:207-212`）**：`false` 同时覆盖「集群不追踪这台」和
「别人持有认领权」，调用方在**两种情况下都不得**认为 registry 对这台沙箱有任何发言权——
reconciliation 把缺行读成「集群已经越过这台沙箱」，对一台未追踪的沙箱那就是刚创建就被拆掉。

0 行时会重读一次，只为在「行确实存在但被别人认领」时打 warn（`postgres.rs:836-849`）。

调用方 `mark_sandbox_running`（`paused_coordinator.rs:240-256`）：
**只有确认（`Ok(true)`）才登记 `running_registrations`**；错误或拒绝**不清除**先前的确认
（`paused_coordinator.rs:232-239`）——「it remains true that the cluster once named this
node the holder, which is precisely the premise reconciliation needs」。

---

### 2.11 `release_node_holdings(node_id) -> ReleasedHoldings`

`mod.rs:215-237` / `postgres.rs:854-926`

**一个事务，两条语句**，共用谓词 `LIVE_HOLDINGS_OF_NODE`（`postgres.rs:94-95`）：

```sql
((state = 'running'  AND origin_node_id     = $2)
 OR (state = 'resuming' AND claimed_by_node_id = $2))
```

released（`postgres.rs:869-879`）：`snapshot_id IS NOT NULL` ⇒
`state='paused', claimed_by_node_id=NULL, generation=generation+1, updated_at=now(),
lease_expires_at=now()`。
discarded（`postgres.rs:892-899`）：`snapshot_id IS NULL` ⇒ `DELETE`。

#### 「本机后继进程」论证（原文，`mod.rs:218-230`）

> 🔴 **Only ever correct at process startup, before this node can hold anything.**
> It releases rows by node identity alone, so running it once the node is serving would
> hand this node's own live sandboxes to whoever resumes them next — the exact
> duplication the rest of this module exists to prevent.
>
> Why startup is nevertheless the strongest evidence in the system: a node's ID names
> the machine, not the process, so a row saying "`running` on this node" that is being
> read by a process which has just started and holds nothing can only have been written
> by a previous process on this same machine. **That process is gone, and its sandboxes
> went with it — the VMs are its children, in its PID namespace. No timeout can
> establish that; only being the successor can.**

`mod.rs:232-236` 解释两个出口：

> Rows naming a snapshot go back to `paused` and can be resumed anywhere. Rows without
> one are deleted: the sandbox was live, its local artifacts were consumed by the resume
> that started it, and nothing was ever published — there is nothing left to bring back,
> and a row that can never be claimed would just accumulate.

调用位置**唯一**：`bin/server.rs:176`，在 listener 打开之前，且是三步中的**第一步**
（`bin/server.rs:166-175` 明写「All three run before the listener opens, and the order is
load-bearing」）。

失败处理（`paused_recovery.rs:431-442`）：只 warn，沙箱保持 stranded 直到下次启动成功——
「That is the safe direction — the alternative is a timeout deciding it, which is the
thing this replaced.」

> **方案 §3B 的证据等级问题**：换成 `service_instance_id` 会把「同一台机器的后继进程」
> 这个**因果证据**降级成「一个不同的实例 id」这个**相关性证据**。`ObservedNode` 里
> `node_id` 与 `service_instance_id` 是并列两个字段（`scheduler.proto:158,161`），
> 说明系统里本来就区分机器身份与进程身份。

---

### 2.12 `remove(&sandbox_id) -> ()`

`mod.rs:239-241` / `postgres.rs:928-937`

```sql
DELETE FROM paused_sandboxes WHERE sandbox_id = $1 AND cluster_id = $2
```

**无 generation CAS、无状态前置、不检查 rows_affected。** 契约上靠调用方保证
「Only correct once the sandbox itself is gone」。

调用方 `forget_sandbox`（`paused_coordinator.rs:280-319`）在 remove 之前做了自己的守卫：
先 `get`，若 `live_elsewhere(&entry, &self.node_id)` 就**拒绝清行**
（`paused_coordinator.rs:294-306`）——删掉一台活在别的节点上的沙箱的本地副本，
对那台副本毫无影响，清行只会剥掉它可恢复的快照。remove 成功后才删快照。

另一处调用：`restore_claimed_sandbox` 里「registry 指的快照 repository 已经没了」
（`paused_recovery.rs:298-300`）。

> Go 侧的 `TransitionSandbox` 若要覆盖 `remove`，**必须补上 `expect_generation`**，
> 否则中央化之后这是唯一一个无条件破坏性写。

---

### 2.13 `is_cluster_backed() -> bool`

`mod.rs:243-251`，默认 `false`。Postgres 实现返回 `true`（`postgres.rs:939-941`），
Disabled 用默认（`disabled.rs` 不覆盖）。

> Reconciliation keys off this and must never run against a registry that does not: a
> disabled registry answers "no record" for everything, which reads as "every paused
> sandbox has moved on" and would discard all of them. Defaults to `false` so a new
> backend has to opt in deliberately.

守卫点：`paused_recovery.rs:161-163, 207-209, 346-348, 412-414, 465-467, 517-519, 732-734`。
单测 `disabled_registry_is_not_cluster_backed`（`orchestrator/tests.rs:4811-4816`）。

**Disabled 实现的返回值**（`disabled.rs:23-88`）：`begin_pause` 返回
`{generation: 0, previous_snapshot_id: None}`、`get`/`get_many` 返回空、
`claim_for_resume` 返回 `NotFound`、`mark_running` 返回 `false`。

---

## 3. 状态机全图

### 3.1 状态含义（`types.rs:15-33`）

| 状态 | 含义 | VM 在跑吗 | 谁能 resume |
|---|---|---|---|
| `publishing` | 本地已 paused，快照还没进 repository | 否 | 仅 origin |
| `paused` | 快照 durable | 否 | 任意节点 |
| `resuming` | 某节点已认领，正在拉起 | 起来中 | 仅 claimer |
| `local_only` | paused 但快照**永远**没进 repository | 否 | 仅 origin（除非租约过期被抢） |
| `running` | 活在 `origin_node_id` 上；`snapshot_id` 仍指向上次 resume 的来源快照 | 是 | 持有者 |

### 3.2 转换矩阵

行 = 起始状态，列 = 方法。`—` = 谓词不匹配（无效果）。

| from \ 方法 | `begin_pause` | `complete_pause` | `mark_local_only` | `claim_for_resume` | `release_claim` | `mark_running` | `renew_lease` | `reclaim_expired` | `release_node_holdings` | `remove` |
|---|---|---|---|---|---|---|---|---|---|---|
| `publishing` | →`publishing` | →`paused` (CAS) | →`local_only` (CAS) | →`resuming` **仅租约过期且有快照** | — | →`running` | 续（origin） | — | — | 删 |
| `paused` | →`publishing` | — | — | →`resuming` **无条件** | — | →`running` | — （`paused` 不续）| — | — | 删 |
| `resuming` | →`publishing` | — | — | — 🔴永不 | →`paused` (CAS) | →`running`（仅 claimer 或 NULL）| 续（claimer） | →`paused` / 删 | →`paused` / 删（claimer） | 删 |
| `local_only` | →`publishing` | — | — | →`resuming` **仅租约过期** | — | →`running` | 续（origin） | — | — | 删 |
| `running` | →`publishing` | — | — | — 🔴永不 | — | →`running`（刷新） | 续（origin） | →`paused` / 删 | →`paused` / 删（origin） | 删 |

### 3.3 每条转换写了什么

| 方法 | bump `generation`？ | 写 `lease_expires_at` | 写 `claimed_by_node_id` | 写 `snapshot_id` | 写 `origin_node_id` | 写 `sandbox_expires_at` |
|---|---|---|---|---|---|---|
| `begin_pause` | ✅ 新行=1，冲突=`+1` | `now()+ttl` | `NULL` | ❌ **故意保留** | ✅ 覆盖为调用者 | ❌ |
| `complete_pause` | ❌ | `now()+ttl` | ❌ | ✅ 写入 | ❌ | ❌ |
| `mark_local_only` | ❌ | `now()+ttl` | ❌ | ❌ | ❌ | ❌ |
| `claim_for_resume` | ✅ `+1` | `now()+ttl` | ✅ = claimer | ❌ | ❌ **故意不动** | ❌ |
| `release_claim` | ❌ | `now()+ttl` ⚠️见 §2.7 | `NULL` | ❌ | ❌ | ❌ |
| `mark_running` | ✅ `+1` | `now()+ttl` | `NULL` | ❌ | ✅ 覆盖为调用者 | ❌ |
| `renew_lease` | ❌ | `now()+ttl` | ❌ | ❌ | ❌ | ✅ **唯一写入者** |
| `reclaim_expired` (released) | ✅ `+1` | `now()`（立即过期） | `NULL` | ❌ | ❌ | ❌ |
| `reclaim_expired` (discarded) | — DELETE | — | — | — | — | — |
| `release_node_holdings` (released) | ✅ `+1` | `now()`（立即过期） | `NULL` | ❌ | ❌ | ❌ |
| `release_node_holdings` (discarded) | — DELETE | — | — | — | — | — |
| `remove` | — DELETE | — | — | — | — | — |

**所有方法都写 `updated_at`**（`begin_pause` 写节点时钟 `$5`，其余写 DB 的 `now()`）。

### 3.4 CAS 分布小结

| 类型 | 方法 |
|---|---|
| 真 `WHERE generation = $n` CAS，失败报 `GenerationConflict` | `complete_pause`、`mark_local_only` |
| 真 CAS，但失败**静默** | `release_claim` |
| 无 generation CAS，靠状态/持有者谓词 | `begin_pause`（无谓词）、`claim_for_resume`（状态+租约+快照）、`mark_running`（claim guard）、`renew_lease`（持有者）、`reclaim_expired`（状态+双时钟）、`release_node_holdings`（持有者+状态） |
| 完全无条件 | `remove`（仅 cluster 作用域） |

---

## 4. 租约时钟

### 4.1 配置（`cfg.rs:392-459`）

| 字段 | 默认 | 位置 |
|---|---|---|
| `backend` | `"local"` | `cfg.rs:394-395` |
| `dsn` | `None`，env `AENV_PAUSED_REGISTRY_DSN` | `cfg.rs:398-399` |
| `max_connections` | `8` | `cfg.rs:400-401` |
| `reconcile_interval_secs` | **`30`** | `cfg.rs:410-411` |
| `lease_ttl_secs` | **`90`** | `cfg.rs:434-435` |

### 4.2 两条强制校验（`cfg.rs:438-459`）

```rust
pub fn reconcile_interval(&self) -> Duration {
    Duration::from_secs(self.reconcile_interval_secs.max(1))     // cfg.rs:444-446
}

pub fn lease_ttl_secs(&self) -> u64 {
    self.lease_ttl_secs
        .max(self.reconcile_interval_secs.max(1).saturating_mul(3))   // cfg.rs:455-458
}
```

论证原文（`cfg.rs:448-454`）：

> A lease shorter than the cadence that renews it expires on a healthy node, so a
> sandbox parked on a node that is doing fine would be rebuilt elsewhere from an older
> snapshot for no reason. **Three intervals leaves room for two missed renewals** before
> the cluster concludes a node is gone.

`reconcile_interval` 的 floor 论证（`cfg.rs:439-443`）：零不只是无用而是致命——
`tokio::time::interval` 零周期会 panic，运维能用一个配置值把节点在启动时打挂。

`lease_ttl_secs` 的 🔴 论证（`cfg.rs:422-433`）：

> A live sandbox is never handed to another node on this alone. A lapsed lease only
> proves the holder cannot reach the database … So this value is **a floor on how long
> the cluster waits before either of those, never the thing that decides them.**
> Lowering it does not bring a dead node's sandboxes back sooner than their own timeouts
> allow.

### 4.3 TTL 传递链

`build_paused_registry`（`mod.rs:320`）→ `PostgresPausedSandboxRegistry::connect(..., lease_ttl_secs)`
→ 存成 `f64` 字段（`postgres.rs:106-110`），**作为参数绑定而不是烤进 SQL**：
「so all nodes agree on it through configuration, and expiry is always judged against
the database's clock rather than each node's own」。

### 4.4 续租循环跑在哪个 task

`spawn_paused_record_upkeep`（`bin/server.rs:292-322`）起 **两个** task：

```
task 1 (renew):     ticker(interval) → renew_paused_leases()
task 2 (reconcile): ticker(interval) → reconcile_local_records() → reclaim_expired_sandboxes()
```

🔴 **两个而不是一个**的理由（`bin/server.rs:286-291`）：

> Reconciliation tears sandboxes down, and a teardown waits on whatever operation
> currently holds the sandbox; one that drags on would, in a shared loop, stop the
> renewals as well. The node would then declare *all* of its own sandboxes abandoned
> while it was busy standing one of them down, and other nodes would take them over.
> **Renewal must not be able to starve behind anything.**

启动路径（`bin/server.rs:176-178`，顺序 load-bearing）：
`release_stale_node_holdings()` → `renew_paused_leases()` → `reconcile_local_records()`，
全部在 listener 打开之前。两个 ticker 都先 `tick()` 吞掉第一拍（`bin/server.rs:299-300, 309`）。

### 4.5 漏续几次才过期

默认 30s / 90s ⇒ 第 1、2 次漏续仍在 TTL 内，**第 3 次漏续之后**（t = 90s）
`COALESCE(lease_expires_at, updated_at) < now()` 才成立。即**容忍 2 次连续漏续**。

⚠️ **只对 `publishing` / `local_only` 有后果**（会被别的节点抢走并回退一个快照）；
对 `running` / `resuming` 租约过期本身**零后果**，只有和 `sandbox_expires_at < now()`
同时成立才会被 `reclaim_expired_holdings` 收走。

---

## 5. 现有 Rust 测试清单（Go 契约测试的源材料）

### 5.1 `tests/paused_registry.rs` —— 29 个集成测，**全部需要真 PG**

跳过机制（`tests/paused_registry.rs:48-63`）：无 `AENV_PAUSED_REGISTRY_TEST_DSN` 就 return；
设了 `AENV_PAUSED_REGISTRY_TEST_REQUIRED=1` 而没 DSN 则 assert 失败——防假绿。
`TEST_LEASE_SECS = 1.0`、`PAST_LEASE = 1600ms`（`:28-30`）。

| # | 行 | 测试 | 覆盖的语义 | 移植 |
|---|---|---|---|---|
| 1 | :129 | `a_failed_publish_keeps_the_snapshot_the_sandbox_already_had` | 🔴 `begin_pause` 不清 `snapshot_id` | **P0** |
| 2 | :187 | `a_live_holder_cannot_have_its_sandbox_taken_away` | 🔴 `running` 不可抢（第二副本 bug） | **P0** |
| 3 | :217 | `a_live_sandbox_is_never_taken_over_on_a_lapsed_lease` | 🔴 租约过期也不抢活行 | **P0** |
| 4 | :242 | `a_parked_sandbox_moves_on_once_its_holder_stops_renewing` | 三分法第二支：parked + 租约过期可接管 | **P0** |
| 5 | :286 | `a_successor_process_releases_what_the_previous_one_was_running` | `release_node_holdings` released 出口 | **P0** |
| 6 | :331 | `a_successor_process_discards_live_rows_that_never_published` | `release_node_holdings` discarded 出口 | **P0** |
| 7 | :370 | `releasing_holdings_touches_nothing_but_this_nodes_live_rows` | 作用域双重收窄（别人的活行 + 自己的 parked 行都不碰） | **P0** |
| 8 | :419 | `an_interrupted_resume_is_released_by_the_node_that_claimed_it` | `resuming` 按 `claimed_by` 而非 `origin` 判持有者 | **P0** |
| 9 | :460 | `only_the_holder_can_renew_its_lease` | `renew_lease` 持有者谓词 | **P0** |
| 10 | :506 | `a_paused_sandbox_is_claimable_immediately` | 三分法第一支不看租约 | **P0** |
| 11 | :533 | `an_ordinary_claim_reports_no_takeover` | 🔴 `previous_state` 必须来自 `previous` CTE | **P0** |
| 12 | :558 | `marking_a_sandbox_running_cannot_erase_another_nodes_claim` | 🔴 `mark_running` 的 claim guard | **P0** |
| 13 | :609 | `one_cluster_cannot_reach_anothers_sandboxes` | 🔴 `cluster_id` 作用域（写路径） | **P0** |
| 14 | :653 | `a_downgrade_that_matches_nothing_is_reported` | `mark_local_only` CAS 失败必须上报 | P1 |
| 15 | :682 | `a_sandbox_that_never_published_is_never_claimable` | `snapshot_id IS NOT NULL` 前置 | **P0** |
| 16 | :707 | `releasing_a_claim_puts_the_sandbox_back` | `release_claim` 语义 | P1 |
| 17 | :741 | `marking_an_untracked_sandbox_running_reports_that_it_is_untracked` | 🔴 `mark_running` never creates a row + `false` 语义 | **P0** |
| 18 | :757 | `marking_a_tracked_sandbox_running_reports_the_node_as_holder` | `true` 语义 | P1 |
| 19 | :774 | `marking_running_reports_a_refusal_when_another_node_holds_the_claim` | 拒绝必须可与成功区分 | **P0** |
| 20 | :799 | `a_batch_read_reports_only_the_sandboxes_that_have_rows` | 🔴 全有或全无 | **P0** |
| 21 | :835 | `a_batch_read_cannot_see_another_clusters_sandboxes` | 🔴 `get_many` 的 cluster 作用域 | **P0** |
| 22 | :852 | `a_batch_read_of_nothing_asks_nothing` | 空输入不发语句 | P2 |
| 23 | :874 | `a_sandbox_that_outlived_its_deadline_on_a_silent_node_is_reclaimed` | 🔴 回收两条件同时成立 | **P0** |
| 24 | :911 | `a_sandbox_still_within_its_deadline_survives_a_silent_node` | 只有租约过期不够 | **P0** |
| 25 | :952 | `an_expired_sandbox_stays_with_a_node_that_is_still_reporting` | 只有 deadline 过期不够 | **P0** |
| 26 | :983 | `a_sandbox_with_no_deadline_is_never_reclaimed` | NULL deadline 永不匹配 | **P0** |
| 27 | :1015 | `reclamation_leaves_parked_rows_alone` | 回收只作用于活行 | P1 |
| 28 | :1102 | `reclamation_discards_expired_rows_with_nothing_to_rebuild_from` | 回收 discarded 出口 | P1 |
| 29 | :1144 | `a_renewal_moves_the_deadline_the_row_is_judged_against` | deadline 来自持有者不是行 | **P0** |

**P0 共 20 个**——这 20 个是「Go 侧 SQL 与 Rust SQL 语义等价」的最小证明集。

> ⚠️ 这个文件里**没有**关于 `begin_pause` 跨集群 upsert 被拒（`InvalidRecord`）、
> `complete_pause` 的 `GenerationConflict`、`decode` 的 `Paused && snapshot IS NULL`
> 不变量的测试。Go 侧要补，**这三条都是 fail-closed 方向的**。
>
> 🔧 **勘误（2026-08-19）：实际只缺两条。** `begin_pause` 跨集群 upsert 被拒**是有测试的** ——
> `one_cluster_cannot_reach_anothers_sandboxes`（`tests/paused_registry.rs:609`，最后三分之一）
> 断言了 `matches!(hijack, Err(PausedRegistryError::InvalidRecord { .. }))`。
> 真正缺的是 `complete_pause` 的 generation 冲突与 `paused && snapshot IS NULL` 不变量两条，
> Go 侧契约测试已补齐。见 [`_impl-D7-contract-tests.md`](_impl-D7-contract-tests.md) §4 S0。

### 5.2 `src/cfg.rs:461-510` —— 4 个纯单测，**不需要 PG**

| 行 | 测试 | 断言 | 移植 |
|---|---|---|---|
| :478 | `a_zero_interval_never_reaches_the_timer` | `config(0,90).reconcile_interval() == 1s` | P1 |
| :489 | `a_lease_can_never_be_shorter_than_the_renewal_cadence` | `config(60,10)→180`；`config(0,0)→3` | **P0** |
| :497 | `a_generous_lease_is_left_alone` | `config(30,600)→600` | P1 |
| :504 | `the_defaults_leave_room_for_two_missed_renewals` | `config(30,90)→90` 且 `≥ 3×interval` | **P0** |

### 5.3 `src/api/impls/paused_coordinator.rs:462-612` —— 11 个纯单测，不需要 PG

`unreferenced_snapshot_is_collected`(:468) / `snapshot_the_registry_points_at_is_kept`(:476) /
`unreadable_registry_never_deletes`(:484) —— 孤儿快照三分法，**P0**（fail-closed）。
`an_unconfirmed_mark_leaves_the_sandbox_unregistered`(:495) /
`a_confirmed_mark_records_the_identity_it_was_confirmed_under`(:505) /
`a_refusal_does_not_clear_an_earlier_confirmation`(:518) /
`retain_drops_sandboxes_this_node_no_longer_has`(:529) —— running registration 语义，**P0**。
`a_sandbox_running_on_another_node_is_not_forgotten`(:565) /
`a_sandbox_claimed_by_another_node_is_not_forgotten`(:577) /
`our_own_sandbox_is_forgotten`(:589) /
`a_parked_sandbox_is_forgotten_from_any_node`(:601) —— `live_elsewhere` 守卫，P1。

### 5.4 `src/api/impls/paused_recovery.rs:974-1339` —— 23 个纯单测，不需要 PG

三组纯函数的穷举测试：`supersession`(11 个，:1011-:1152)、`arbitration`(4 个，:1167-:1241)、
`running_supersession` + `missing_local_verdict`(8 个，:1264-:1332)。

**这一组整体 P0**——它们是「什么时候删用户数据」的判定表，且已经抽成纯函数便于穷举。
Go 侧的中央裁决逻辑应当照抄这个形状（纯函数 + 表驱动），而不是把判断散进 RPC handler。

其中特别点名：`a_restart_under_a_new_node_id_does_not_supersede_our_own_records`(:1152) ——
node_id 在 K8s 下是 pod 名，会变；用当前 ID 比会让节点重启后把自己所有行读成别人的。

### 5.5 `src/orchestrator/tests.rs`

`disabled_registry_is_not_cluster_backed`(:4811) —— **P0**，一行断言挡住「全量删除」。
另有 `RecordingPublisher`(:4820-4867) 供编排层测试用，无需真 registry。

### 5.6 移植优先级汇总

- **P0（36 项）**：tests/paused_registry.rs 20 + cfg 2 + coordinator 7 + recovery 23 中的
  判定表（按组算 3 组）+ orchestrator 1。**这些是 §3 护栏的可执行版本。**
- **P1（约 12 项）**：语义正确但不是数据安全边界。
- **P2（1 项）**：`a_batch_read_of_nothing_asks_nothing`（优化性质）。

---

## 6. 阶段 0 只读对账：需要什么，以及歧义在哪

### 6.1 数据源

| 源 | 内容 | 位置 |
|---|---|---|
| 登记表 | `paused_sandboxes` 全表（**必须补 SELECT `lease_expires_at, sandbox_expires_at`**，不在 `ENTRY_COLUMNS` 里） | PG |
| Heartbeat roster | `HeartbeatRequest.sandbox_ids`（`scheduler.proto:177`），来源 = `NodeSnapshot.sandbox_ids` = `orchestrator.list_sandbox_ids()`（`observability/service.rs:82`）= **本地 store 全部记录，不分状态** | scheduler 内存 / `ListObservedNodes` |
| `ObservedNode` | `node_id` / `service_instance_id` / `last_seen_unix_ms` / `snapshot`（含 `sandbox_count` / `paused_sandbox_count`，但**没有 per-sandbox 状态**） | `scheduler.proto:157-167` |

🔴 **roster 只有 id 列表，没有 per-sandbox 状态。** scheduler 的 `BindingStore.ReconcileNode`
（`services/scheduler/internal/store.go:17,77`）也只维护 `sandbox_id → node`。
这个限制决定了下面四个口径里有两个今天算不准。

### 6.2 四个口径逐个拆

#### `orphan`（节点上跑着、登记表不认）

需要：roster（每 node）、登记表 `sandbox_id` 集合。
朴素算法 `roster(N) ∖ registry` — **算出来的不是 orphan。**

#### `ghost`（登记表说 running、节点 roster 里没有）

需要：`state`、`origin_node_id`、`claimed_by_node_id`、`updated_at`、
`ObservedNode.last_seen_unix_ms`。
算法：`state='running' AND origin_node_id = N AND sandbox_id ∉ roster(N)`。

**必须加的两个限定**：
1. 只算 `state='running'`，**不要算 `resuming`** —— resume 在飞的窗口里沙箱还没进 N 的 store。
2. 要求 `now - updated_at > 一个 heartbeat 周期 + margin`，且 `N` 的 `last_seen_unix_ms`
   足够新（否则 roster 本身就是陈的）。

#### `holder_conflict`（两个节点同时报同一个 sandbox）

需要：全部 node 的 roster。算法：`sandbox_id` 在 ≥2 个 roster 里出现。

🔴 **这个数在健康集群里也不是零**，因为 roster 含 paused 记录：跨节点接管期间
origin 仍持有 paused 记录（要等它自己的 reconcile 才丢弃，见 `paused_recovery.rs:636-722`），
而 claimer 已经有活的了。两条腿都上报同一个 id 是**预期中的过渡态**。

⇒ 必须拆成两类，而**光靠 roster 拆不开**（没有 per-sandbox 状态）。
可用的近似：用登记表的 `origin_node_id` 定位「谁是权威持有者」，
把 `roster ∋ id 但 origin_node_id ≠ 该节点` 的那一侧标成 `stale_copy`，
真正的 `holder_conflict` 留给「登记表也说不清是谁」的情况。

#### `lease_expiring`（租约将过期的 live 行数）

需要：`lease_expires_at`、`state`。

🔴 **一个数字会把两件完全不同的事混起来**：
- `publishing` / `local_only` 租约将过期 = **可被抢走、会丢最后一次 pause 的工作**，
  这是**可告警的数据损失前兆**（`postgres.rs:551-552` 的第二支）
- `running` / `resuming` 租约过期 = **本身零后果**，只说明该节点够不到 PG；
  只有再叠加 `sandbox_expires_at < now()` 才会被回收（`postgres.rs:731-735`）

⇒ 建议拆成 `parked_lease_expiring`（可告警）与 `live_lease_lapsed`（信息性），
外加 `reclaimable_now` = 两条件同时成立的行数（这是 `reclaim_expired_holdings`
下一拍会动的行，是最该盯的那个数）。

⚠️ 计算 `parked_lease_expiring` 时注意 §2.7 的不一致：`release_claim` 把 `paused` 行的租约
写成 `now()+ttl`，而 `release_node_holdings` / `reclaim` 写成 `now()`。`paused` 行不该进
任何租约口径（它的可认领性不看租约），过滤掉即可。

### 6.3 🔴 歧义点与我的判断

#### 歧义 1（任务点名的那个）：roster 里有、登记表里没有，算 orphan 吗

**判断：不算。而且阶段 0 根本算不出真正的 orphan，只能算 `untracked`。**

三条理由，都有代码支撑：

1. **`mark_running` 契约是 "Never creates a row"**（`mod.rs:204-205`、`postgres.rs:806-809`）。
   一台在本节点创建、从没 pause 过的沙箱**本来就没有行**。这在稳态下是**多数**情况，
   不是异常。
2. **roster = `list_ids()` = 全部本地记录，含 paused**（`observability/service.rs:82` →
   `store/in_memory.rs:248-250`）。里面还混着 `ClusterRegistration::Never` 的 paused 记录
   ——那些记录「the local copy is the only copy, so absence from the registry carries no
   information at all」（`paused_recovery.rs:759-763`）。
3. **让「缺行」变得有意义的那个前提，heartbeat 里根本没有**：Rust 侧敢用缺行下结论，
   靠的是两个 roster 之外的证据——进程内 `running_registrations`
   （`paused_coordinator.rs:264-266`，只在 `mark_running` 返回 `true` 时写）和盘上的
   `ClusterRegistration`（`persistence/mod.rs:70-79`）。中央观察者两个都看不到。

**建议**：阶段 0 把这个指标命名为 `untracked`（roster ∖ registry），并**明说它是 orphan 的
超集、在健康集群里非零**。要把它变成真 orphan 信号，唯一干净的做法是在 heartbeat 的
per-sandbox 条目上加一位「是否已向集群报备」（等价于 node 侧那个 registration 标记），
这是一次 proto 变更，属于阶段 0 的可选增量而不是必需项。

> 这条直接影响方案 §3.3 的丢弃熔断阈值怎么定：如果拿 `untracked` 当 orphan，
> 阈值会被稳态噪声顶得没有意义。

#### 歧义 2：`ENTRY_COLUMNS` 不含租约列

Rust 侧所有消费方**都看不到租约**。阶段 0 的只读 API 若照抄 `PausedSandboxEntry` 形状，
`lease_expiring` 就永远算不出来。**必须显式扩展读模型**（这是纯加列，零风险）。

#### 歧义 3：`updated_at` 混了两个时钟源

`begin_pause` 写的 `updated_at` 是**节点进程的 `Utc::now()`**（`postgres.rs:302,363`），
其余所有写路径写的是 **DB 的 `now()`**。

⇒ 方案 §3.2 那个「停机时长从 PG 里的 `max(updated_at)` 与当前时间推算」的 grace 算法，
在节点时钟漂移时会算错，且**方向不确定**（节点时钟快 ⇒ 低估停机 ⇒ grace 不够 ⇒ 正是
要防的批量抢占）。建议 Go 侧 migration 顺手把 `begin_pause` 的 `paused_at/updated_at`
改成 DB `now()`（语义上更对，且 `paused_at` 的调用方传值本来就被忽略，见 §1.2）。

#### 歧义 4：`resuming` 在 roster 里的可见性

`resuming` 行的 `origin_node_id` 仍指向**持有本地 artifacts 的旧节点**
（`postgres.rs:496-498`），`claimed_by_node_id` 才是正在拉起的节点。
任何按 `origin_node_id` 与 roster 比对的口径，对 `resuming` 行都会得出错误结论。
⇒ 对账时 `resuming` 必须按 `claimed_by_node_id` 归属（照抄
`running_supersession` 的 `PausedRegistryState::Resuming` 分支，`paused_recovery.rs:933-940`）。

#### 歧义 5：`answered` 与 `sync_ok` 分开计

方案要求照抄 e2b `nodemanager/sync.go`。**今天 AgentENV 侧没有这个区分**：
heartbeat 是 node → scheduler 单向推（`scheduler.proto:12`），没有「controller 问、node 答」
这一步。阶段 0 能做的只有 `last_seen_unix_ms` 的新鲜度（`scheduler.proto:166`），
那对应 e2b 的 `answered`；`sync_ok`（答了但内容与中央不一致）要等阶段 3 的中央 poll。
⇒ 阶段 0 只能上报 `roster_stale`（`now - last_seen > N×interval`），
并**明确标注 `sync_ok` 本阶段不可得**，别把它写进阶段 0 的验收清单。

### 6.4 阶段 0 建议的最小读模型

```sql
SELECT sandbox_id, cluster_id, state, generation,
       origin_node_id, claimed_by_node_id, snapshot_id,
       paused_at, updated_at,
       lease_expires_at, sandbox_expires_at
  FROM paused_sandboxes
 WHERE cluster_id = $1
```

派生指标：

| 指标 | 口径 |
|---|---|
| `untracked{node}` | `roster(N) ∖ registry`（**不叫 orphan**，见歧义 1） |
| `ghost{node}` | `state='running' AND origin_node_id=N AND id ∉ roster(N)`，且 N 的 `last_seen` 新鲜、行 `updated_at` 够老 |
| `stale_copy{node}` | `id ∈ roster(N)` 但行的权威持有者（running→`origin`，resuming→`claimed_by`）≠ N |
| `holder_conflict` | `id` 出现在 ≥2 个 roster **且** 无法用登记表归到单一持有者 |
| `parked_lease_expiring` | `state IN ('publishing','local_only') AND lease_expires_at < now() + 窗口` ← **可告警** |
| `live_lease_lapsed` | `state IN ('running','resuming') AND LEASE_EXPIRED` ← 信息性 |
| `reclaimable_now` | `state IN ('running','resuming') AND LEASE_EXPIRED AND sandbox_expires_at < now()` ← **最该盯的数** |
| `roster_stale{node}` | `now - last_seen_unix_ms > 3×reconcile_interval` |
| `invalid_rows` | `state='paused' AND snapshot_id IS NULL`（Rust 读路径会整批报错的行，`postgres.rs:236-241`）← **应当恒为 0** |

最后一条尤其值得加：它今天在 Rust 侧的表现是**整个 `get_many` 批次报错**，
也就是那个节点的 reconciliation 会全线停摆——一条坏行能静默冻结一台机器的对账。
中央只读对账是第一次能看见它的地方。
