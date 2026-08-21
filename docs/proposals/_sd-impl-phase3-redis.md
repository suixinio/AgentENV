# 阶段 3 存储半：Redis 活跃态 store 与四个原语的落地规格

> 2026-08-20 · **写给要照着敲键盘的人**。设计文档，不含生产代码。
>
> 上游：[`2026-08-20-module-responsibilities.md`](2026-08-20-module-responsibilities.md) §8 全节 ＋ D3 / D7 / D9 / D11；
> [`2026-08-20-service-decomposition.md`](2026-08-20-service-decomposition.md) §4.4 / §6 全节 / §7 阶段 3 第 2 条 / §8 陷阱 1 2 7 8；
> 同批的另一半：[`_sd-impl-phase3-role.md`](_sd-impl-phase3-role.md)（下文一律记作 **[S]**，结构半）；
> 前序：[`_sd-impl-phase1.md`](_sd-impl-phase1.md) §6.4 / §6.5（投影 TTL 与 `KEEPTTL` 缺陷）；
> 落地环境：[`_sd-recon-env.md`](_sd-recon-env.md) §2 / §8 / §9 SD-B1 SD-B5。
> 考古基准：e2b `/home/debian/e2b-infra`。**本文引用的每一条都自己核过**，核对结果在附录。
>
> **范围。** 阶段 3 有两半。本文只写**活跃态 store** 这一半：
> `MetadataStore` 的 Redis 实现、键布局、四个原语（闭包式 update / transition key 三件套 /
> 全局过期 ZSET ＋ healer / `Reserve` 三态）、`GetMany` 覆盖集、留下的两处 fencing、
> 真 Redis 测试、Redis 持久化与 HA、验证探针。
>
> **不写**另一半（`--role` 开关、`ListSandboxes`、trait 拆分、`proxy_routes` 与句柄分家、
> auto-resume 迁移、node 收窄 REST、启动残留回收、部署与回退）——那是 [S]。
> 本文在接缝处按名引用它，并在 §16 把**要回传给 [S] 的三条硬要求**单列出来。
>
> 🔴 **仓库边界（用户裁决，2026-08-20）**：全部工作只落在 `/home/debian/AgentENV`。
> 主仓的 pause-publish-durability 按**不可用**处理，因此记录必须带 `origin_node_id` ＋
> `published`（[S] §6.6 第 2 条对本半提的接缝要求，本文接受并落在 §4.3）。
>
> **可执行性**：本文自带全部证据行号，不需要回读父提案。

---

## 0. 三十秒版

**七个决定：**

| # | 问题 | 决定 |
|---|---|---|
| **A** | `update_if_state<F>` 的闭包契约怎么办 | **逐字保留**，锁换分布式锁。🔴 但**锁不承重** —— 承重的是写回那一步 Lua 里的 `rev` ＋ `execution_id` ＋ `state` 三重 CAS。锁只买吞吐与「闭包只跑一次」，锁失效降级成一次**响亮的 `ConcurrentUpdate`**，不是静默覆盖 |
| **B** | 闭包执行时限怎么落 | **三层**：① 闭包跑在本地副本上，超预算就**不写**（`FnOnce` 决定了不能重试）；② 写前查锁的剩余寿命，不够就放弃；③ 写本身带 CAS，前两层全漏了也覆盖不掉新化身。§5.3 |
| **C** | 键怎么布 | **扁平，前缀 `agentenv:api:`**。🔴 不抄 `SameSlot`（§4.4：无租户模型），不与路由投影 `agentenv:scheduler:bindings:*` 共用任何键（D11）。九个键，全表在 §4.1 |
| **D** | 记录键要不要 TTL | 🔴 **要，而 e2b 没有。** e2b 的沙箱键**根本不设过期**（`operations.go:42` 的 `SET` 无 `EX`），所以它的 `redis.KeepTTL` 保的是一个不存在的 TTL。我们把 TTL 设成**寿命上界 ＋ 宽限**做兜底，于是 `KEEPTTL` 才真正承重。§4.4 |
| **E** | 卡住的转换谁来解 | transition key 的 TTL 只让**下一次操作**能继续，不修状态。e2b 靠 `ExpiredItems` 的 stale-cutoff 分支兜底（`items.go:150-159`），🔴 **而那条对我们有洞**：我们允许 `timeout = None`，这种记录**根本不进过期 ZSET**。⇒ 加一条 e2b 没有的 `agentenv:api:txn:index` ZSET ＋ reaper。§6.7 |
| **F** | `Reserve` 三态的真实价值 | 🔴 **父提案对它的价值论证在我们这里不成立**：`NewSandbox` schema 里**没有 `sandboxID` 字段**（`src/api/openapi.yml:540-580`），id 由服务端在 `service.rs:368` 现铸 ⇒ **create 路径上「客户端重试撞同一个 id」这件事发生不了**。真正的触发点是 `restore_sandbox`（调用方带 id）与**「创建中窗口对集群不可见」**。三态照做，但**理由要换**，否则会按错误的理由做错误的取舍。§8 |
| **G** | Redis 丢了怎么办 | 这个 2 节点集群做不出真 HA（`_sd-recon-env.md` §2）。⇒ **把「全丢 ⇒ 重建」当主路设计并验证**（§13.3），HA 是生产要求不是本阶段前置。🔴 顺带：`redis.yaml` 里 `everysec` 与 `Recreate` 的**理由注释在阶段 3 之后是假的**，必须改写（§13.1） |

**🔴 读完代码之后，我认为父提案与同伴文档里有九处必须更正**（完整论证在 §16）：

1. §8.1「`redis.KeepTTL` 的等价物 —— 写回不能抹掉沙箱寿命 TTL」—— **误读**。e2b 的沙箱记录键没有 TTL。
2. §8 陷阱 7「这条在 PG 版本里就是遗留项，别原样继承」—— **事实错误**。`Rows.Covered` 与 `require_full_coverage` **两侧都已经实现了**。
3. §8.2「上 Redis 之后状态会永远卡住」—— **只在漏抄 `items.go:150-159` 时成立**；而我们有一个 e2b 没有的更狠的洞（`timeout = None`）。
4. §8.4 / 阶段 3 第 2 条对 `Reserve` 的价值论证 —— 在我们的 API 形状下**没有触发点**（服务端铸 id）。
5. D9 表格「靠锁的 `lockTimeout`」做崩溃恢复 —— 与 §8.2 自己的结论矛盾，且 §8.2 是对的。
6. 🔴 e2b 的 `Update`（`operations.go:203`）**自己就有 `scripts.go:33-40` 描述的那个洞** —— 裸 `SET`，无 execution 谓词。照抄就是把 e2b 的 bug 抄过来。
7. 🔴 `SandboxMetadata` **不可序列化**（`paused_state: Option<Arc<dyn PausedSandboxState>>`，`metadata.rs:121-122`）。「换个 store 后端」在第一次 `serde_json::to_vec` 就撞墙，而三份文档都没提。
8. 🔴 `store.update()` 是**无谓词的 LWW 全量写**，全仓 8 处。它在进程内被过渡态串行化掉了，跨副本不再。
9. 🔴 今天的驱逐**不在锁内重校验过期**（`service.rs:2201-2233`）—— 这是**已经存在的 bug**，不是上 Redis 才有的。

> 🔧 **2026-08-21 回填：本文写在阶段 2 开工之前，2a/2b/2c 落地之后有一节新增与五处订正 —— 见 §17。**
> 最要紧的三条：**读作用域是「面」的属性**（§17.1）、**「不存在」这个答案要三态，
> 而够不着的 store 以错误传播、绝不表现为「不存在」**（§17.2）、
> **本半十一个任务全是「先落地、后接线」，接线前要过一份审计清单**（§17.3）。

---

## 1. 事实基线

### 1.1 e2b 侧（逐条自证）

| 断言 | 位置 | 核对结果 |
|---|---|---|
| execution CAS 必须在 Lua 里，不能在 Go 里 | `packages/api/internal/sandbox/storage/redis/scripts.go:33-40` | ✅ 逐字：「it has to live in here rather than in Go: Add is lockless, so a resume can install a new incarnation between a Go-side comparison and this write, and the SET below would then overwrite the new live record with the stale one」 |
| 分布式锁：`redislock`，`NoRetry` ＋ pub/sub 唤醒 ＋ jitter backoff | `storage/redis/lock.go:21-31` `:50-87` | ✅ 🔴 `Obtain(ctx, key, timeout)` 里 **同一个 `timeout` 既是锁 TTL 又是获取超时**（`:51` `:90`） |
| `StartRemoving`：先拿锁，**进入等待前显式 `releaseFunc()`** | `state_change.go:41` `:110-115` | ✅ 逐字 `releaseErr := releaseFunc()` 在 `handleExistingTransition` 之前 |
| 完成回调在**另一把锁**下写 result ＋ 删 transition key ＋ publish | `state_change.go:180-233`（提案写 `:190`，落在 `Obtain` 那行） | ✅ |
| 闭包式 update：锁 ＋ GET ＋ 闭包 ＋ `SET KeepTTL` ＋ `EndTime` 变了才 `ZAdd` | `operations.go:159-215` | ✅ 逐行对上 |
| 🔴 但 `Update` 的写是**裸 `SET`，无任何谓词** | `operations.go:203` | ✅ `s.redisClient.Set(ctx, key, newData, redis.KeepTTL)` |
| 🔴 沙箱记录键**从不设过期** | `scripts.go:12-15`（`addSandboxScript` 只有 `SET`／`SADD`），全目录 `grep Expire` 零命中 | ✅ ⇒ `KeepTTL` 保的是空 |
| 单条全局 pub/sub 通道，路由键在 payload 内 | `storage/redis/utils.go:22-25` | ✅ 逐字「The per-event routing key is embedded in the message payload so one connection per API pod is sufficient」 |
| 过期索引 member 按 execution 作用域 ⇒ ZREM「structurally safe」 | `storage/redis/utils.go:31-38` | ✅ 逐字「removing a dead execution's member can never unindex a live one, even when a lockless Add for the same sandbox ID races a Remove or the evictor's stale sweep」 |
| team 分片是 Redis Cluster hash slot 的需求 | `storage/redis/utils.go:61-68`（`SameSlot(teamID)` 在 `:63`） | ✅ 🔴 **我们不适用**（§4.4） |
| `ExpiredItems` 每轮有界 256 ＋ 扫孤儿 ＋ 补重打分 ＋ **stale-cutoff 放行卡住的转换** | `items.go:16` `:20-32` `:104-112` `:126-132` `:134-147` `:149-159` | ✅ 提案只引了 `:20`，`:149-159` 那一段没人引过，而它是崩溃恢复的实际出口 |
| healer：每副本跑、`ZADD NX`、1 分钟 grace、feature flag 每轮重求值 | `heal.go:16-22` `:29` `:66-69` `:99-101` `:136-143` | ✅ TOCTOU 论证逐字一致 |
| evictor 无 leader election，`activeEvictions` 只是进程内去重 | `orchestrator/orchestrator.go:192-196`、`evictor/evict.go:36` `:109-111` | ✅ `go sandboxEvictor.Start(ctx)`；🔴 poll 间隔 **50ms**（`evict.go:23`） |
| `Reserve` 四态 ＋ `staleCutoff` ＋ `waitForStart` | `sandbox/store.go:157-171`、`reservations/redis/reservation.go:50-83`、`scripts.go:9-64` | ✅ `reserveResult{Reserved,AlreadyInStorage,AlreadyPending,LimitExceeded}` = 0/1/2/3 |
| `waitForStart`：订阅 ＋ 初探 ＋ 1s 兜底 ticker ＋ 「不在 pending 且无 result」= 失败 | `reservation.go:152-231` | ✅ |
| 🔴 孤儿判定只测 sandbox id 存在性 —— **已知 bug，不要抄** | `storage/redis/main.go:205-209`（提案写 `:205-217`） | ✅ 逐字 `if raw != nil { // Sandbox exists in store, not an orphan. continue }`。**但同一函数 `:191-200` 有一条值得抄的护栏**：pipeline 出错 ⇒ 整轮跳过，`return nil`，逐字「skip entirely to avoid mass kills」 |
| 测试跑 testcontainer 里的真 Redis | `packages/shared/pkg/redis/tests.go:14-48`（提案写 `:17`，落在 `ContainerRequest`） | ✅ `redis:8-alpine`，`wait.ForLog("Ready to accept connections")` |
| 常量 | `storage/redis/main.go:20-26` | `lockTimeout=1m` / `transitionKeyTTL=70s` / `transitionResultKeyTTL=30s` / `lockRetry{Min=200ms,Max=1s,Jitter=0.25}` / `pollInterval=1s`；`reservation` 侧 `resultTTL=30s` / `staleTTL=90s`（`reservations/redis/reservation.go:17-28`） |

### 1.2 AgentENV 侧

| 事实 | 位置 |
|---|---|
| `MetadataStore` trait 十二个方法 | `src/orchestrator/store/mod.rs:57-103` |
| 🔴 **行号漂移**：三份上游文档写 `:61` / `:73` / `:84` / `:96`，实际是 `:63` / `:75` / `:86` / `:98` | 阶段 1 的 `configured_max_sandbox_lifetime` / `NewTimeout` 导出使 `mod.rs` 上移了 2 行。引用时按实际行号 |
| 唯一的生产实现 | `src/orchestrator/store/in_memory.rs:95` |
| 🔴 `SandboxMetadata::paused_state: Option<Arc<dyn PausedSandboxState>>`，`#[serde(skip)]` | `src/orchestrator/store/metadata.rs:119-122` |
| `max_lifetime` / `lifetime_deadline()` / `projection_ttl_secs()` 已落地（阶段 1） | `metadata.rs:103-118` `:167-195` |
| `Orchestrator` 三泛型参数 | `src/orchestrator/service.rs:93-97` |
| `SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>`；句柄表 | `service.rs:41` `:101` |
| 八态状态机 | `src/orchestrator/types.rs:82-91` |
| `update_if_state` 六个调用点，闭包全部是纯同步字段赋值 | `service.rs:611` `:967` `:1922` `:2010` `:2110` `:2446` |
| 🔴 `service.rs:967` 的闭包**写外层局部变量** `timeout_updated` | `service.rs:965-986` ⇒ 乐观重试会让它多次置位，这是「不能改成重试循环」的第二条硬理由 |
| `update_state_if_state` 十四个调用点 | `service.rs:666` `:684` `:1055` `:1135` `:1260` `:1341` `:1395` `:1411` `:1461` `:1525` `:1657` `:1692` `:1771` `:1827` `:1844` `:2587` |
| 🔴 `store.update()` 八个调用点，**全部是无谓词 LWW 全量写** | `service.rs:1541` `:2935` `:2960` `:2974` `:3009` ＋ 测试辅助 |
| 驱逐：全表扫 ＋ 状态过滤，**无锁内重校验** | `service.rs:2201-2237`（提案写 `:2159`，已漂移） |
| `wait_while_in_states` 唯一调用点，外层 60 秒超时 | `service.rs:2075-2098`；`WAIT_TRANSITION_TIMEOUT` 在 `:46` |
| `create_sandbox` 现铸 id；`NewSandbox` 无 `sandboxID` 字段 | `service.rs:368`；`src/api/openapi.yml:540-580` |
| 🔴 `store.add` 发生在 VM 建好**之后** | `service.rs:2390-2408`（`launch_sandbox` 里 handle 先插表，`transitional_metadata` 后入库） |
| `restore_sandbox` 接受调用方给定 id | `service.rs:387-397` |
| 四个测试替身 `impl MetadataStore` | `src/orchestrator/tests.rs:200`（`ScriptedStore`）`:344`（`RaceBeforeUpdateStore`）`:451`（`ConflictOnUpdateStore`）`:564`（`ScriptedWaitStore`） |
| 已落地的 execution fencing（批次 A，**已合并，不要回退**） | `services/scheduler/internal/registry/store_postgres.go:462` `:501`；`services/gateway/internal/execution_fencing.go`；`src/api/proxy.rs:380` |
| `GetMany` 的 `Rows.Covered` **已实现** | `services/scheduler/internal/registry/store.go:50-63` `:195-198`、`store_postgres.go:321-352` `:395` |
| `require_full_coverage` **已实现** | `src/orchestrator/paused_registry/central.rs:274-345` |
| 路由投影键，**本文一律不碰** | `services/shared/routing/record.go:87` = `agentenv:scheduler:bindings`；`BindingKey` ⇒ `…:sandbox:<id>`，`NodeIndexKey` ⇒ `…:node:<id>` |
| 集群 Redis 清单（本会话另一位已放进树，未提交） | `deploy/k8s/base/redis.yaml`：单副本 / `appendonly yes` / `appendfsync everysec` / `noeviction` / `maxmemory 512mb` / `Recreate` / local-path PVC |
| 集群只有两台机器，PVC 全钉 204 | `_sd-recon-env.md` §2；SD-B5 |
| Rust 侧已有 testcontainers 基建 | `crates/test-support/Cargo.toml:10-11`（`testcontainers 0.23` ＋ `testcontainers-modules 0.11`，现开 `minio` feature）；`crates/test-support/src/minio.rs:22-37` 是模板 |
| `testcontainers-modules 0.11.6` 有 `redis` feature | `~/.cargo/registry/src/*/testcontainers-modules-0.11.6/Cargo.toml:305` ＋ `src/redis/` |
| Go 侧真 Redis 测试的纪律模板 | `services/Makefile:36-60`（`test-with-postgres` 的 `REDIS_SERVER_BIN` ＋ `SCHEDULER_REDIS_TEST_REQUIRED=1` ＋ 前置 `command -v` 检查） |

---

## 2. 🔴 三个在动手前必须解决的结构性障碍

三份上游文档都把这一半描述成「把 `S` 换成 `RedisMetadataStore`」。下面三条是换之前就会撞墙的东西。

### 2.1 `SandboxMetadata` 今天不可序列化

```rust
// src/orchestrator/store/metadata.rs:119-122
    /// Paused state produced by the sandbox backend during `pause`.
    /// Passed back to the backend factory when `resume_sandbox` is called.
    #[serde(skip)]
    pub paused_state: Option<Arc<dyn PausedSandboxState>>,
```

`#[serde(skip)]` 意味着**它能过 `serde`，但过完就没了**。而 `resume_sandbox` 逐字依赖它：

```rust
// src/orchestrator/service.rs:1699-1702
let paused_state = metadata.paused_state.as_ref().ok_or_else(|| {
    warn!("missing paused state while resuming");
    OrchestratorError::InternalError("missing paused state".to_string())
})?;
```

⇒ **一个天真的 Redis store 会让每一次 resume 都 500**，而且是在集成测试之后、真跑之前才发现。

**解法已经在仓里了。** `FileBackedSandboxPersister` 早就把这个 trait object 序列化过：

```rust
// src/orchestrator/persistence/file_backed.rs:419        编码
let state = paused_state.encode()?;                    // -> serde_json::Value
// :429-433 record { version, lifecycle, metadata, artifact_root, state, registered_as }
// :66-73                                                  解码
let paused_state = factory.decode_paused_state(self.artifact_root, self.state)?;
self.metadata.paused_state = Some(paused_state);
```

`PausedSandboxState::encode() -> Value`（`src/sandbox/backend.rs:32`）与
`SandboxBackendFactory::decode_paused_state(PathBuf, Value)`（`:335-339`）已经是**成对的可序列化边界**。

⇒ **本半的落法**（§4.3 有完整 schema）：

| 项 | 决定 |
|---|---|
| 记录里存什么 | 新增 `paused_state_ref: Option<PausedStateRef>`，`PausedStateRef { artifact_root: PathBuf, state: serde_json::Value }` —— 与 `PersistedPausedRecord` 的 `artifact_root` ＋ `state` **同形**，不是新格式 |
| `paused_state` 字段 | **保留**，仍是 `#[serde(skip)]`，仍是本机句柄。`--role all` / `--role node` 下由本机 factory 填 |
| 谁把 ref 变回句柄 | `--role api` 下**不变**：`api` 没有 factory，也不该有。它把 `PausedStateRef` **原样**塞进 node gRPC 的 `Resume` 请求，由 node 侧 `decode_paused_state` 还原。这正是 [S] §4.1「远程形态返回 id 与事实，不返回句柄」 |
| 🔴 `artifact_root` 是**节点本地路径** | 所以记录必须同时带 `origin_node_id`（§4.3），否则 `api` 拿着一条路径不知道该发给谁。这与 [S] §6.6 第 2 条对本半提的要求**是同一条要求**，两边独立推出来同一个结论 |

🔴 **顺带一条给 [S] 的更正**：[S] §3.4 说 `control_plane_config` 装
「`{execution_id, snapshot_id, timeout_action, expires_at, auto_resume, secure, user_metadata}` 的紧凑编码」。
**那不够。** 它的用途是「Redis 全丢之后 `api` 从 `ListSandboxes` 重建记录」（§13.3），
而上面七项重建不出 `resources` / `created_at` / `max_lifetime` / `network_policy` /
`custom_extension_params` / `runtime_versions` / `context` / `startup` / `image_configs` /
`virtualization_mode` / `snapshot_alias` / `origin_node_id` / `published`。
⇒ **`control_plane_config` 就应该是 `SandboxMetadata` 本体的版本化 serde 编码**（去掉 `paused_state`）。
它对 node 不透明，所以这不是跨语言契约，是 `api` 与**未来的自己**的契约 ——
版本化方案照抄 `PersistedPausedRecord` 的 `RECORD_VERSION` ＋ `ensure_supported_version`
（`file_backed.rs:63` 附近），滚动升级期间两个 `api` 副本读得懂彼此。

### 2.2 `update_if_state<F: FnOnce>` 在类型上排除了乐观重试 —— 而这是好事

D9 的表格说 Redis 实现「只能二选一：翻成 Lua，或改成 GET→改→CAS-SET 的乐观重试」。
§8.1 已经推翻了前半句。后半句也不成立，**而且是被类型系统排除的**：

```rust
// src/orchestrator/store/mod.rs:75-82
async fn update_if_state<F>(&self, sandbox_id, expected_states, update: F) -> Result<MetadataUpdateResult>
where F: FnOnce(&mut SandboxMetadata) + Send;
```

`FnOnce` **只能调用一次**。要做重试循环，签名必须改成 `FnMut` 或 `Fn`，那是 [S] §2.4 已经点名
反对的语义变更（「两半会在同一个签名上打架」）。

**第二条硬理由，比类型更硬**：

```rust
// src/orchestrator/service.rs:965-986   keep_alive
let mut timeout_updated = false;
… .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
        …
        if new_expire <= current_expire { return; }      // ← 提前返回，不置位
        metadata.set_timeout(Some(valid_timeout));
        timeout_updated = true;                          // ← 🔴 写的是闭包**外面**的变量
    })
```

闭包有**外部副作用**。重试循环会让 `timeout_updated` 在一次逻辑操作里被置位多次，
而调用方拿它决定要不要打 `info!` 与要不要重打投影分数。

⇒ **结论：闭包契约逐字保留，跑一次，跑在锁内。** §8.1 是对的，D9 的表格是错的（§16.5）。

### 2.3 `store.update()` 是八处无谓词的 LWW 全量写

```rust
// src/orchestrator/store/mod.rs:60
async fn update(&self, metadata: SandboxMetadata) -> Result<()>;
```

最要命的一处在 pause 的收尾：

```rust
// src/orchestrator/service.rs:1483-1541
let persisted_metadata = { let mut m = self.store.get(&sandbox_id).await? … ; m.state = Paused; m };
…                                    // ← persist_paused：一次落盘 I/O，几十到几百毫秒
self.store.update(persisted_metadata).await?;    // ← 把几百毫秒前读到的整条记录写回去
```

这是一个**跨越 I/O 的读-改-写，没有任何谓词**。今天它安全，靠的是 `Pausing` 这个过渡态
把并发写全挡在外面（`update_if_state` 的 `expected_states` 都要求 `Running`）。
**跨副本之后过渡态仍然挡得住并发的 `update_if_state`，但挡不住 `add`** ——
`add` 在 e2b 那里是 lockless 的（`scripts.go:36` 逐字），在我们这里也是（`in_memory.rs:96`）。
一次 `restore_sandbox` 用同 id 建一条新记录，正好落在这个窗口里，就会被这一次 `update` 覆盖回去。

⇒ **`update()` 的 Redis 契约必须带 execution 谓词**（§5.7）。这不是加固，是把
`scripts.go:33-40` 已经写明的理由用在它自己漏掉的那条路径上（§16.6）。

---

## 3. trait 的命运，逐方法

### 3.1 全表

| # | 方法（`store/mod.rs`） | 命运 | 说明 |
|---|---|---|---|
| 1 | `add(metadata)` :59 | **契约变** | 仍然 `SandboxAlreadyExists` 时报错，但实现变成一个 Lua：`SET NX` 记录 ＋ `SADD` 索引 ＋ `ZADD` 过期索引 ＋ `ZREM` pending，四步原子。🔴 加一条：写 TTL（§4.4） |
| 2 | `update(metadata)` :60 | 🔴 **契约变（加谓词）** | 变成 Lua CAS：仅当存储中 `execution_id` 与 `metadata.execution_id` 相同、且 `rev` 与调用方读到的相同时才写。新错误 `StoreError::ConcurrentUpdate`。§5.7 |
| 3 | `update_state_if_state(id, new, expected)` :63 | **保留，实现变** | 纯 Lua，**不取锁**：一次 GET-判断-SET 在脚本里就是原子的，加锁只是白付一次往返。§5.6 |
| 4 | `update_if_state(id, expected, F)` :75 | **契约逐字保留 ＋ 两条新增** | 分布式锁 ＋ 闭包执行时限 ＋ 写回 CAS。§5 全节 |
| 5 | `get(id)` :83 | 保留 | 一次 `GET` ＋ 反序列化。`None` = 键不存在 |
| 6 | `remove(id)` :84 | **实现变** | Lua：`GET` ＋ `DEL` ＋ `SREM` ＋ 按**读到的那个 execution** 做 `ZREM`，返回被删的 JSON。逐字抄 `removeSandboxScript`（`scripts.go:23-28`）＋ `operations.go:104-118` 的作用域清理 |
| 7 | `list()` :85 | 🔴 **契约变（全有或全无）** | `SMEMBERS` 索引 ＋ 分块 `MGET`。**任何一块失败就整体失败**，绝不返回短列表。§9 |
| 8 | `list_with_callback(F)` :86 | **保留签名，实现变** | 同上，但按块回调，不物化整个 `Vec`。🔴 见 §3.3 的性能警告 |
| 9 | `list_filtered(filter)` :89 | 保留 | `list()` ＋ Rust 侧过滤。Redis 侧不做二级索引（§4.5 论证） |
| 10 | `list_expired(now)` :90 | 🔴 **换掉** | 由 `expired_batch(now, limit)` 取代（新方法，见 3.3）。旧方法**保留为 default 实现**（`expired_batch(now, usize::MAX)`），这样四个测试替身和 in-memory 实现一行都不用改 |
| 11 | `list_ids()` :91 | 保留 | `SMEMBERS`。🔴 索引集合是权威成员集，不是 `SCAN` |
| 12 | `wait_while_in_states(id, transitional)` :98 | **契约逐字保留，实现全换** | pub/sub 唤醒 ＋ 1 秒兜底 ticker ＋ 每次唤醒重 `GET`。返回 `Ok(None)` 仍然只表示「记录没了」。§6.6 |

### 3.2 新增方法（六个）

| 方法 | 签名 | 为什么在 trait 上而不是在 Redis 实现上 |
|---|---|---|
| `expired_batch` | `async fn expired_batch(&self, now: SystemTime, limit: usize) -> Result<Vec<SandboxMetadata>>` | 驱逐器要「每轮有界」。in-memory 实现是 `by_expiry.range(..).take(limit)`，三行 |
| `start_transition` | `async fn start_transition(&self, id: &SandboxId, req: TransitionRequest) -> Result<TransitionOutcome>` | 原语二的入口。in-memory 实现退化成一次 `update_state_if_state` ＋ 一个空回调，见 §12 |
| `get_many` | `async fn get_many(&self, ids: &[SandboxId]) -> Result<MetadataRows>` | 陷阱 7。`MetadataRows { entries: HashMap<..>, covered: Vec<SandboxId> }` |
| `reserve` | `async fn reserve(&self, id: &SandboxId) -> Result<Reservation>` | 原语四 |
| `heal_expiry_index` | `async fn heal_expiry_index(&self) -> Result<usize>` | 让 healer 由 orchestrator 统一调度，且 in-memory 实现返回 `Ok(0)`（索引与记录同一把锁，结构上不会漂） |
| `reap_stuck_transitions` | `async fn reap_stuck_transitions(&self, now: SystemTime) -> Result<Vec<SandboxId>>` | §6.7。in-memory 返回 `Ok(vec![])` |

🔴 **六个全部给 default 实现**。理由是 [S] §14.6 已经裁定 `InMemoryMetadataStore` **保留为 node 的本地账本** ——
它不需要这六件事的真实语义，而 `src/orchestrator/tests.rs` 里那四个替身
（`:200` / `:344` / `:451` / `:564`）如果每加一个方法就要改四遍，
这一半的改动面会凭空多出几百行**没有任何断言价值**的转发代码。

### 3.3 🔴 一条不能不说的性能变形：`list_with_callback` 与 `metrics_snapshot`

```rust
// src/orchestrator/service.rs:2037-2046
pub async fn metrics_snapshot(&self) -> Result<OrchestratorMetrics> {
    self.store.list_with_callback(|metadata| { aggregate_resource_metrics(…); }).await?;
```

进程内它是一次 `RwLock::read` ＋ 一次遍历。在 Redis 上它是
**「`SMEMBERS` 全集 ＋ `MGET` 全部记录 ＋ 全部反序列化」**，
而 `metrics_snapshot` 挂在 `/metrics` 抓取路径与节点 API 上（`src/observability/`）。
N 个 `api` 副本 × Prometheus 抓取周期 × 全集 MGET = 一条谁都没设计过的负载。

⇒ **三条要求：**

1. `RedisMetadataStore` 内置一个 **1 秒 TTL 的聚合备忘**（`RwLock<(Instant, OrchestratorMetrics)>`），
   `list_with_callback` 的**聚合类**调用走它。这与 e2b 把 `sandboxcounts` 单独做成缓存包
   是同一个取舍（模块文档 §8 陷阱 3）。
2. 🔴 备忘的注释里必须写明「这是采样值，不是时点值」，
   并且**返回值里带上采样时刻**，否则运维会拿两个副本的两次采样去做差。
3. `list_with_callback` 的**非聚合**调用（如果将来出现）不得走备忘。
   ⇒ 现在只有一个调用点，所以更干净的做法是：**在 trait 上加 `metrics_snapshot` 本身**，
   由 store 决定怎么算。但那会把 orchestrator 的指标模型下沉进 store。
   **本文选 1+2，并把这条登记为已知代价**，不选下沉。

### 3.4 对象安全：与 [S] 的接缝

[S] §2.2 已经证明 `MetadataStore` **不是对象安全的**（`update_if_state<F>` / `list_with_callback<F>` 是泛型方法），
所以 `Box<dyn MetadataStore>` 编译不过，「换 `S`」只能在**单态**世界成立。
[S] §2.3 的决定是门面 trait `SandboxOrchestration` ＋ 两个装配函数：

```rust
ServerRole::Api  => assemble_remote(config).await?,   // Orchestrator<RedisMetadataStore, RemoteSandboxBackendFactory, DisabledSandboxPersister>
ServerRole::All | ServerRole::Node => assemble_local(config).await?,
```

🔴 **本半对这个决定的三条依赖，逐条确认：**

| 依赖 | 状态 |
|---|---|
| 本半**不需要** `MetadataStore` 对象安全 | ✅ `RedisMetadataStore` 是一个具体类型，直接填进 `Orchestrator<S,F,P>` 的 `S` |
| 本半**不得**为了让它对象安全而改 `update_if_state` 的签名 | ✅ §2.2 已论证不能改，[S] §2.4 也反对 —— **两半在这一点上是一致的，不是冲突的** |
| 本半新增的六个方法**不得**引入新的泛型方法 | 🔴 **这是一条约束，不是观察**。`start_transition` 的回调用 `Box<dyn FnOnce(...) + Send>` 或返回一个 `TransitionGuard` 结构体，不要写成 `fn start_transition<F: FnOnce>` —— 否则将来任何一次「让它对象安全」的尝试都要多改一处 |

### 3.5 不进 trait 的东西

| 项 | 落点 | 理由 |
|---|---|---|
| 分布式锁 | `RedisMetadataStore` 私有 | 调用方从来不该看见它。§8 陷阱 1 说的「不需要新造锁」指的是**调用面**不新造 |
| pub/sub 连接与订阅管理 | `RedisMetadataStore` 私有（`src/orchestrator/store/redis/notify.rs`） | 但见 §3.6：D3 的事件通道是**另一件事** |
| Lua 脚本 | `src/orchestrator/store/redis/scripts.rs` 的 `const &str` ＋ `OnceLock<Script>` | 与 e2b 同形（`scripts.go` 是独立文件） |
| healer / reaper 的**调度** | `Orchestrator` 的后台任务（与 `start_auto_evict_task` 同形，`service.rs:2239-2275`） | store 只提供 `heal_expiry_index()` / `reap_stuck_transitions()` 这两个**幂等的一轮** |

### 3.6 🔴 D3（生命周期事件走 Redis pub/sub）不在本半

D3 说 `sandbox_event_tx`（`service.rs:106`）要改走 Redis pub/sub。
**本半会引入一条 pub/sub 通道**（`agentenv:api:notify`），用途是唤醒 `wait_while_in_states`
与锁等待者 —— 那是**store 内部的唤醒信号**，不是生命周期事件。

⇒ **两条通道，不要合并。** 理由与 D11 让投影与 store 分家是同一条：
唤醒信号的 payload 是路由键（e2b `utils.go:22-25` 的形状），
生命周期事件的 payload 是 `SandboxLifecycleEvent` —— 一个随编排逻辑变，一个不变。
把 observability reporter 挂到 store 的唤醒通道上，等于让心跳的上报格式跟着状态机走。
**D3 排在阶段 3 之后**，本文只保证唤醒通道的实现可以被它复用（同一个 `notify.rs` 里的
`SubscriptionManager` 支持多通道），不实现它。

---

## 4. Redis 键布局

### 4.1 全表

前缀 `agentenv:api:`。🔴 **与路由投影的 `agentenv:scheduler:bindings:*` 完全不相交**（D11）。

| 键 | 类型 | TTL | 写者 | 读者 |
|---|---|---|---|---|
| `agentenv:api:sbx:<sandbox_id>` | string（JSON） | 🔴 `lifetime_deadline − now + record_ttl_grace`，无上界时**无 TTL**（§4.4） | `add` / `update` / `update_*_if_state` / `remove` | 全部 |
| `agentenv:api:index` | SET&lt;sandbox_id&gt; | 无 | `add` / `remove` | `list*` / `reserve` / healer |
| `agentenv:api:expiry` | ZSET，score = `expires_at` 的 unix 毫秒，member = `<sandbox_id>:<execution_id>` | 无 | `add` / `update*` / `remove` / healer | `expired_batch` / healer |
| `agentenv:api:txn:<sandbox_id>` | string = `transition_id` | `transition_key_ttl`（默认 90s） | `start_transition` / 完成回调 | `start_transition` / reaper |
| `agentenv:api:txn:<sandbox_id>:<transition_id>` | string = 结果（空串 = 成功） | `transition_result_ttl`（默认 30s） | 完成回调 | 等待者 |
| 🆕 `agentenv:api:txn:index` | ZSET，score = 转换截止毫秒，member = `<sandbox_id>:<execution_id>:<transition_id>` | 无 | `start_transition` / 完成回调 / reaper | reaper |
| `agentenv:api:lock:sbx:<sandbox_id>` | string = 持有者 token（uuid） | `lock_ttl`（默认 15s） | 锁 | 锁 |
| `agentenv:api:pending` | ZSET，score = 预留时刻 unix 秒，member = `<sandbox_id>` | 无（由 `staleCutoff` 的 `ZREMRANGEBYSCORE` 扫） | `reserve` / `finish_start` / `release` | `reserve` / 等待者 / 🔴 孤儿判定（§10.3） |
| `agentenv:api:reserve:<sandbox_id>` | string = 编码的创建结果 | `reserve_result_ttl`（默认 30s） | `finish_start` / `release` | `wait_for_start` |
| `agentenv:api:notify` | pub/sub 通道 | —— | 全部 | 全部 |

**分隔符。** `SandboxId` 与 `ExecutionId` 都是 `Uuid`（`src/types/id.rs:7` `:95`，`Uuid::now_v7()`），
序列化成带连字符的小写十六进制，**永远不含 `:`** ⇒ `:` 做 member 分隔符是安全的，
与 e2b `utils.go:46-59` 的 `parseExpirationMember` 同理（它还额外 `uuid.Parse` 校验第三段，
**这条要抄**：一个解析不出来的 member 应当被当成垃圾扫掉，而不是被当成某个沙箱）。

### 4.2 🔴 为什么扁平，以及不抄 `SameSlot`

e2b 的 `GetTeamPrefix` 用 `redis_utils.SameSlot(teamID)`（`storage/redis/utils.go:63`）把
一个 team 的所有键塞进同一个 Redis Cluster hash slot，这样一条 Lua 才能同时碰
sandbox 键与 team 索引键。**这是 Redis Cluster 的约束，不是数据模型。**

我们的处境（`grep -rn "team_id|TeamId" src/ --include=*.rs` 只命中
`src/api/generated/src/models.rs` 的 E2B 兼容 schema，`src/api/impls/auth.rs` 自述
"presence, not validity"）⇒ **没有租户模型，没有可分片的主体**。

⇒ 三条结论：

1. **键扁平，不带任何分片段。**
2. 🔴 **但 Lua 的多键约束不因此消失。** 部署形态今天是**单实例**（`deploy/k8s/base/redis.yaml`），
   所有键天然同 slot。**如果将来上 Redis Cluster**，`agentenv:api:sbx:<id>` 与
   `agentenv:api:index` / `agentenv:api:expiry` 会落在不同 slot，
   `add` 的那条四步 Lua 会直接报 `CROSSSLOT`。
   ⇒ **本文的选择：现在就给这三个共享结构加 hash tag**：
   `agentenv:api:{global}:index` / `agentenv:api:{global}:expiry` / `agentenv:api:{global}:pending`，
   并让 sandbox 键写成 `agentenv:api:{global}:sbx:<id>`。
   代价是全集群共享一个 slot（单实例下无差别），收益是**将来上 Cluster 时不用改键名**。
   🔴 **键名改不动**——这是父提案自己对记录结构说过的话，对键名同样成立。
3. 不实现 e2b 的 `globalTeamsSet` 与 `TeamsWithSandboxCount`（`operations.go:221`）。

### 4.3 记录 schema

存储格式 = `serde_json` 紧凑编码的 `StoredSandboxRecord`：

```rust
// src/orchestrator/store/redis/record.rs
#[derive(Serialize, Deserialize)]
pub struct StoredSandboxRecord {
    /// 🔴 版本号在最前面，且没有 `#[serde(default)]`。
    /// 一条读不懂版本的记录必须**失败**，不能被当成"新记录"。
    /// 与 `PersistedPausedRecord` 的 `ensure_supported_version` 同一套。
    version: u32,

    /// 每次写自增。update_if_state 的写回 CAS 比的就是它。
    /// 🔴 不用 execution_id 代替：一次 keep_alive 不换化身，但确实改了记录。
    rev: u64,

    /// `SandboxMetadata` 本体，除 `paused_state` 之外逐字段。
    #[serde(flatten)]
    metadata: SandboxMetadata,

    /// §2.1：`paused_state` 的可序列化替身。
    /// `artifact_root` 是 **origin_node_id 那台机器上的**本地路径。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    paused_state_ref: Option<PausedStateRef>,

    /// 🔴 [S] §6.6 第 2 条对本半提的硬要求，本文接受。
    /// 承载 sandbox 的节点。Running 时 = 正在跑它的节点；
    /// Paused 时 = 硬盘上有那份字节的节点。
    origin_node_id: Option<String>,

    /// 🔴 同上。false ⇒ 字节只在 origin 的盘上 ⇒ resume **硬钉 origin**，
    /// 不降级、不改写提示、显式失败（对标
    /// services/scheduler/internal/lookup.go:270-317 的 SANDBOX_LOCATION_PINNED）。
    published: bool,
}
```

**四条约束：**

| # | 约束 | 理由 |
|---|---|---|
| 1 | 🔴 `metadata` 用 `#[serde(flatten)]`，字段名与 `SandboxMetadata` 完全一致 | 让 Lua 里的 `cjson.decode(current)['execution_id']` 能直接取到，不用嵌一层。e2b 的 `scripts.go:54` 正是这么读 `decoded['executionID']` 的 |
| 2 | 🔴 `execution_id` **不得**加 `#[serde(default)]` | `metadata.rs:67-73` 已经逐字论证过：默认值是永久的 fail-open。同一条理由，同一条禁令 |
| 3 | 新字段一律 `#[serde(default)]` | 滚动升级期间旧副本写的记录要能被新副本读 |
| 4 | 🔴 `published` 的默认值是 **`false`** | fail-closed：来路不明的记录当作「未发布」⇒ 钉 origin ⇒ 最坏结果是 resume 失败，不是把沙箱放到一台没有它字节的机器上 |

### 4.4 🔴 记录键的 TTL：e2b 没有，我们要有

**e2b 的事实**（核过）：`addSandboxScript` 只有 `SET KEYS[1] ARGV[1]`（`scripts.go:12-15`），
整个 `storage/redis/` 目录里 `grep Expire` 对沙箱键**零命中**。
⇒ `operations.go:203` 的 `redis.KeepTTL` 保的是一个**从不存在的 TTL**。

⇒ 🔴 **模块文档 §8.1 第 1 条「`redis.KeepTTL` 的等价物 —— 写回不能抹掉沙箱寿命 TTL」是误读**（§16.1）。
e2b 的寿命住在 JSON 里的 `EndTime` 和过期 ZSET 的 score 里，不住在键的 TTL 里。

**但我们应该有，理由与 e2b 不同：**

| 理由 | 说明 |
|---|---|
| ① 阶段 1 已经引入了**硬寿命上界** | `config/default.toml:240` `max_sandbox_lifetime_secs = 86400`，`metadata.rs:167-171` 的 `lifetime_deadline()`。**一个记录活得比它描述的沙箱的硬上界还久，一定是泄漏** |
| ② 驱逐是唯一的删除路径，而它跑在 `api` 上 | 全部 `api` 副本挂掉的时段里，过期沙箱既不被驱逐也不被删记录。恢复之后 ZSET 会把它们扫出来 —— 前提是 ZSET 与记录都还在。TTL 是**第二道**，它不依赖任何进程活着 |
| ③ `noeviction` 让内存耗尽表现为**写失败** | 泄漏的记录会把这条护栏顶到线上（§13.1）。TTL 是让泄漏自愈的唯一手段 |

**具体：**

```
record_ttl = (lifetime_deadline − now) + record_ttl_grace     // 新 config，默认 3600s
max_sandbox_lifetime_secs == 0（上界关闭）  ⇒ 不设 TTL，与 e2b 一致
```

🔴 **三条硬规则**（照抄 `_sd-impl-phase1.md` §6.4 对投影 TTL 立的规矩，因为踩的是同一个坑）：

1. **向上取整到整秒，下限钳 1。** 绝不出现 0 或负数。
2. 🔴 **`grace` 必须显著大于最长的一次转换。** `record_ttl_grace` 默认 3600 秒
   （对比 `transition_key_ttl` 90 秒、`WAIT_TRANSITION_TIMEOUT` 60 秒）。
   记录在沙箱死透之前消失，等于让 node 上那台还在跑的 VM 变成孤儿，
   而孤儿的处置是**杀掉**（§10.3）。**这条比投影 TTL 那条更要命**：投影早死只是路由未命中。
3. 🔴 **每一条写路径都必须显式选 `KEEPTTL` 还是 `PX`。** 全表：

| 写路径 | 用什么 | 为什么 |
|---|---|---|
| `add` | `PX record_ttl` | 记录诞生，预算从此刻算 |
| `update` / `update_if_state` / `update_state_if_state` | 🔴 `KEEPTTL` | 状态变化不改寿命预算。用裸 `SET` 会**把 TTL 抹成永不过期** —— 这与 `_sd-impl-phase1.md` §6.5 抓到的心跳重置 TTL 是同一类缺陷的镜像（那边是把长 TTL 缩短，这边是把有限 TTL 变无限） |
| resume 后的最终写（`service.rs:2446` 的闭包） | 🔴 `PX record_ttl` **重算** | resume **不重置** `created_at`，所以 `lifetime_deadline` 不动，重算得到的是**更短**的 TTL。这是对的：预算在缩小 |
| fork 子记录 | `PX record_ttl` | `service.rs:713` 逐字 `metadata.created_at = now` ⇒ 全新预算 |
| healer / reaper | 🔴 **不碰记录键** | 它们只修索引 |

### 4.5 不建二级索引

`list_filtered` 支持按 `states` / `excluded_states` / `user_metadata` 过滤。
诱惑是给每个 state 建一个 SET。**不做**，三条理由：

1. 状态由三条不同的写路径改（Lua CAS × 2 ＋ 闭包 update），每条都要多维护一组 `SREM`/`SADD`，
   而**索引与记录不一致的窗口就是一次误判**；
2. e2b 不建（`TeamItems` 是 `SMembers` ＋ `MGET` ＋ **Go 侧过滤**，`operations.go:145-153`）；
3. `user_metadata` 是任意 KV，本来就建不了。

⇒ **过滤在 Rust 侧做，成本是一次全集 MGET**，与 §3.3 的备忘同一条代价，登记在案。

---

## 5. 原语一：闭包式 update ＋ 分布式锁 ＋ 执行时限

### 5.1 算法

```
update_if_state(id, expected_states, closure) :

  ① lock  = acquire(lock:sbx:<id>, ttl = lock_ttl, wait_budget = lock_wait)      // §5.2
     acquired_at = Instant::now()
  ② raw   = GET sbx:<id>                       → None ⇒ SandboxNotFound
     rec   = decode(raw)                       → 解不开 ⇒ Backend（🔴 不是 NotFound）
  ③ if rec.metadata.state ∉ expected_states ⇒ StateConflict{actual}
  ④ previous = rec.metadata.clone()
     t0 = Instant::now()
     closure(&mut rec.metadata)                // 🔴 同步、纯、跑一次
     closure_elapsed = t0.elapsed()
  ⑤ if closure_elapsed > closure_budget                       ⇒ 🔴 ClosureBudgetExceeded，**不写**
  ⑥ if acquired_at.elapsed() + write_budget >= lock_ttl        ⇒ 🔴 LockLapsed，**不写**
  ⑦ EVAL update_if_state.lua
        KEYS = [sbx:<id>, {global}:expiry]
        ARGV = [new_json, expected_rev, expected_execution_id,
                new_expiry_ms, old_member, new_member, rescore_flag]
     → 0  ⇒ ConcurrentUpdate（rev 或 execution 变了）
     → 1  ⇒ ok
  ⑧ release(lock)     // 无论成败，且用 Instant 记的持有者 token 做 CAS 释放
  ⑨ PUBLISH notify  <routing_key(id)>          // 尽力而为，丢了由 ticker 兜
  ⑩ Ok(MetadataUpdateResult { previous, current: rec.metadata })
```

### 5.2 锁

| 项 | 值 | 理由 |
|---|---|---|
| 实现 | `SET lock:sbx:<id> <token> NX PX <lock_ttl>` ＋ 释放时 Lua 比对 token 再 `DEL` | 单实例 Redis 上这就是正确的互斥。**不上 Redlock**：多实例 Redlock 的正确性本身有争议，而我们的正确性根本不靠锁（§5.5） |
| `lock_ttl` | **15 秒**（e2b 是 60 秒） | 🔴 **比 e2b 短，而且是故意的。** e2b 的 60 秒同时是获取超时，它要覆盖 `StartRemoving` 里那一长串往返；我们的 `update_if_state` 是「一次 GET ＋ 一个纯闭包 ＋ 一次 EVAL」，正常路径 &lt; 5ms。TTL 定成 15 秒意味着**一个被杀掉的副本最多把某个沙箱堵 15 秒**，而不是 60 秒 |
| 获取等待预算 | **独立配置** `lock_wait`，默认 10 秒 | 🔴 **不与 TTL 共用一个数**（e2b `lock.go:51` `:90` 共用）。共用之后想调短 TTL 就必然调短等待，两个目标互相绑架 |
| 等待策略 | 首次 `NX` 失败 ⇒ 订阅 `notify` 的锁路由键 ＋ 指数退避（200ms → 1s）＋ ±25% jitter | 逐字抄 `lock.go:62-86`、`jitterBackoff`（`:98-102`）与 `main.go:23-25` 的三个常量 |
| 释放后 publish | 是 | `lock.go:39-48` |
| 🔴 不做锁续期（watchdog） | —— | 续期会把「慢闭包」从一个**能被检测到的错误**变成一个**看不见的长持有**。§5.3 第 ② 层要的正是它不续期 |

### 5.3 🔴 闭包执行时限：三层，且第三层才是承重的

模块文档 §8.1 说「给闭包一个执行时限」，但没说超时之后怎么办。**说清楚：**

**第 ① 层 —— 闭包超预算就不写（`closure_budget`，默认 50ms）。**
关键在于**闭包改的是本地副本**（`rec.metadata`），不是 Redis 里的字节。
所以「跑完发现超时」是一个**可以无损丢弃**的结果：不写，报错，调用方拿到
`StoreError::ClosureBudgetExceeded`，映射成 500。
🔴 **不能重试**（§2.2：`FnOnce` ＋ 外部副作用）⇒ 这必须是一个**终态错误**，
而它同时是一条**告警**：50ms 的纯字段赋值超时，只可能是有人往闭包里塞了阻塞调用。
⇒ 配一个 counter `agentenv_store_closure_budget_exceeded_total`，**默认告警阈值是 &gt; 0**。

**第 ② 层 —— 写之前查锁的剩余寿命。**
真正的危险不是闭包慢，是「①GET ②闭包 ③EVAL」这一串加起来超过 `lock_ttl`。
⇒ 在 ⑦ 之前判 `acquired_at.elapsed() + write_budget >= lock_ttl`（`write_budget` 默认 2 秒，
按 Redis 往返的 p99.9 定）。不满足就放弃，报 `StoreError::LockLapsed`。
这一层抓的是**GC 停顿、调度饥饿、Redis 卡顿**，闭包本身可能一点不慢。

**第 ③ 层 —— 🔴 写本身带 CAS，前两层全漏了也覆盖不掉新化身。**

```lua
-- update_if_state.lua（节选）
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, cur = pcall(cjson.decode, raw)
if not ok then return 0 end
-- 🔴 两个谓词，缺一不可：
--   rev            —— 有人在我读到之后改过这条记录（可能是同一个化身）
--   execution_id   —— 有人换了化身（resume 装了新的一次运行）
if tostring(cur['rev']) ~= ARGV[2] then return 0 end
if ARGV[3] ~= '' and cur['execution_id'] ~= ARGV[3] then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'KEEPTTL')          -- 🔴 KEEPTTL，见 §4.4
if ARGV[7] == '1' then
  if ARGV[5] ~= '' then redis.call('ZREM', KEYS[2], ARGV[5]) end
  redis.call('ZADD', KEYS[2], ARGV[4], ARGV[6])
end
return 1
```

⇒ **锁失效的后果从「静默覆盖」降级成「一次响亮的 `ConcurrentUpdate`」。**
这比 e2b 强，因为 e2b 的 `Update` 写回是裸 `SET`（`operations.go:203`），
它自己在 `scripts.go:33-40` 描述的那个洞在 `Update` 这条路径上是**开着的**（§16.6）。

**一句话总结（要写进 `store/redis/mod.rs` 的模块注释）：**

> 锁买的是吞吐与「闭包只跑一次」；正确性由脚本里的 `rev` ＋ `execution_id` 双谓词承担。
> 把锁关掉，这个 store 仍然不会写坏数据 —— 它只会在高争用下不停返回 `ConcurrentUpdate`。
> 这正是 §14 的对照组能成立、而且能被区分的原因。

### 5.4 KeepTTL 等价物

见 §4.4 第 3 条的全表。补一条落地细节：
🔴 **`KEEPTTL` 是 Redis 6.0+ 的 `SET` 选项**，集群镜像是 `redis:7.4.10-alpine`（`redis.yaml:110`），
测试容器建议**同版本**而不是 e2b 的 `redis:8-alpine` —— 测试和生产跑同一个大版本，
否则「本地绿、线上红」的第一嫌疑人就是它。

### 5.5 ZSET 重打分

e2b 的判定是 `if !updatedSbx.EndTime.Equal(sbx.EndTime)`（`operations.go:209`）。
我们的对应量是 `expires_at: Option<SystemTime>`，四种迁移：

| previous → current | 动作 |
|---|---|
| `None` → `None` | 无 |
| `Some(a)` → `Some(a)` | 无 |
| `Some(a)` → `Some(b)`，a ≠ b | `ZREM` 旧 member ＋ `ZADD` 新 member（member 同名时等价于改分，但仍写两条，因为 execution 可能同时变了） |
| `Some(a)` → `None` / `None` → `Some(b)` | `ZREM` / `ZADD` |

🔴 **member 变了也要重打分**，哪怕 `expires_at` 没变：`update_if_state` 的闭包可以改
`execution_id`（`service.rs:2446` 的 resume 收尾就在改）。
⇒ 判定条件是 `previous.expires_at != current.expires_at || previous.execution_id != current.execution_id`。
**这一条 e2b 没有，因为它的 `Update` 从不换化身**（换化身走 `Add`）。
我们的 resume 收尾走的是 `update_if_state`（`service.rs:2440-2451`）⇒ 必须有。

### 5.6 `update_state_if_state`：纯 Lua，不取锁

```lua
-- update_state_if_state.lua
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, ''} end                      -- NotFound
local ok, cur = pcall(cjson.decode, raw); if not ok then return {-1, ''} end
local actual = cur['state']
local matched = false
for i = 3, #ARGV do if ARGV[i] == actual then matched = true break end end
if not matched then return {2, actual} end              -- StateConflict
cur['state'] = ARGV[1]
cur['rev']   = tonumber(cur['rev']) + 1
redis.call('SET', KEYS[1], cjson.encode(cur), 'KEEPTTL')
return {1, actual}
```

**不取锁的三条理由：**

1. 「GET-判断-SET」在一条 Lua 里**本来就是原子的**。加锁只是多两次往返；
2. e2b 之所以给 `StartRemoving` 加锁，是因为它要在读和写之间做**跨多次往返**的事
   （查 transition key、生成 uuid、算 `TransitionExpires` 的新 `EndTime`）——
   D9 逐字写了「复杂到写不进单个 Lua」。`update_state_if_state` 不是那种；
3. 🔴 **十四个调用点里有九个是回滚路径**（`service.rs:666` `:1135` `:1395` `:1411` `:1461`
   `:1525` `:1692` `:1827` `:2587`）。回滚必须尽可能不会失败 —— 让它去抢一把可能被别人
   持有 15 秒的锁，是把失败注入到「失败之后的补救」里。

🔴 **一条 `cjson` 陷阱**：`cjson.decode` → 改一个字段 → `cjson.encode` **不是保序的**，
而且会把 Lua 认不出的数字表示改掉（大整数精度、`1.0` 变 `1`）。
⇒ **不要在 Lua 里 re-encode 整条记录。** 正确做法：Rust 侧把**新的完整 JSON** 作为 `ARGV[1]` 传进去，
Lua 只负责「读旧的、比谓词、写新的」。上面那段示意里的 `cjson.encode(cur)` 要改掉 ——
签名变成先 `GET` 拿到 `rev` 与 `state`，Rust 侧构造新 JSON，再走 §5.3 的
`update_if_state.lua`。**即：`update_state_if_state` 在 Redis 实现里是
`update_if_state` 的一个特例**（闭包 = `|m| m.state = new_state`），共用一条脚本，
省掉一个只在 Lua 里改 JSON 的危险分支。
⇒ 但它**仍然不取锁**：特化版走「GET → 构造 → CAS-EVAL → 失败则重读重试（最多 3 次）」的
乐观循环。这里可以重试，因为它没有闭包（§2.2 的两条限制都不适用）。

### 5.7 `update()`：全量写 ＋ execution 谓词

```
update(metadata) :
  ① raw = GET sbx:<id>              → None ⇒ SandboxNotFound
  ② cur = decode(raw)
  ③ 🔴 if cur.execution_id != metadata.execution_id ⇒ StoreError::ExecutionSuperseded
  ④ EVAL update_if_state.lua  with expected_rev = cur.rev, expected_execution_id = metadata.execution_id
     → 0 ⇒ 重读重试（最多 3 次），仍失败 ⇒ ConcurrentUpdate
```

🔴 **第 ③ 步是新增的语义，八个调用点都要过一遍。**
它对现有代码是**零行为变化**：八处 `update` 全都写的是自己刚读出来或刚构造的同一化身的记录。
但它挡住了 §2.3 描述的那条路 —— pause 收尾的几百毫秒 I/O 窗口里被 `restore_sandbox` 换了化身。

新增错误变体：

```rust
StoreError::ExecutionSuperseded { sandbox_id, expected: ExecutionId, actual: ExecutionId },
StoreError::ConcurrentUpdate    { sandbox_id },
StoreError::ClosureBudgetExceeded { sandbox_id, elapsed: Duration },
StoreError::LockLapsed          { sandbox_id, held_for: Duration },
StoreError::TransitionInProgress{ sandbox_id, target: SandboxState },
StoreError::InvalidTransition   { sandbox_id, from: SandboxState, to: SandboxState },
```

`OrchestratorError` 侧的映射：前四个 → 409 Conflict（不是 500，它们都是「你输了一次竞争」），
后两个 → 409 / 400。🔴 **`ExecutionSuperseded` 要单独打一条带 `refusal_code` 的日志** ——
`_sd-recon-env.md` §8 的待办 (2) 逐字记着上一轮这条日志缺 `refusal_code`，别再欠一次。

---

## 6. 原语二：transition key 三件套

### 6.1 三件套 ＋ 我们多的第四件

| 件 | 键 | TTL | 谁写 | 谁读 |
|---|---|---|---|---|
| ① transition key | `agentenv:api:txn:<sandbox_id>` | `transition_key_ttl` = **90s** | `start_transition` 的 Lua ／ 完成回调删 | `start_transition` / reaper |
| ② result key | `agentenv:api:txn:<sandbox_id>:<txn_id>` | `transition_result_ttl` = 30s | 完成回调 | 等待者 |
| ③ 完成回调 | —— | —— | 调用方**必须**调 | —— |
| 🆕 ④ transition 索引 | `agentenv:api:txn:index`（ZSET） | 无 | ①③ 同一条 Lua | reaper（§6.7） |

**为什么 `transition_key_ttl` 是 90 秒而不是 e2b 的 70 秒**：
它必须严格大于「最长的一次转换」。我们最长的一次是 pause：
`sandbox.pause()` ＋ `persist_paused()` ＋ 可能的快照发布。
现有的 `WAIT_TRANSITION_TIMEOUT = 60s`（`service.rs:46`）是**等待方**的预算，
不是**执行方**的预算 —— 两者今天没有关系，上 Redis 之后必须有：

```
transition_key_ttl (90s)  >  WAIT_TRANSITION_TIMEOUT (60s)  >  lock_ttl (15s)
```

🔴 **这三个数的序关系是一条不变量，要写成一个 `const_assert!` 或启动断言。**
反过来（`transition_key_ttl < WAIT_TRANSITION_TIMEOUT`）意味着：
等待者还在等，transition key 已经过期，另一个副本以为没有在途转换就开始了第二次 ——
**双执行**，正是 §14 判据 3 要抓的那件事。

### 6.2 `start_transition` 的 Lua

```lua
-- start_transition.lua
-- KEYS = [sbx:<id>, txn:<id>, txn:<id>:<txn_id>, {global}:txn:index]
-- ARGV = [new_json, txn_id, txn_key_ttl_s, result_ttl_s,
--         expected_execution_id, txn_index_member, txn_deadline_ms,
--         eviction_flag, now_ms]
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, 'not_found', ''} end
local ok, cur = pcall(cjson.decode, raw)
if not ok then return {0, 'undecodable', ''} end

-- 🔴 execution CAS —— §6.3「必须留的」那一行，落点就是这里。
--    在脚本内，与下面的写原子。绝不是先查后写。
if ARGV[5] ~= '' and cur['execution_id'] ~= ARGV[5] then
  return {0, 'execution_superseded', cur['execution_id']}
end

-- 已有在途转换：不写任何东西，把 txn_id 还给调用方去等。
local inflight = redis.call('GET', KEYS[2])
if inflight then return {0, 'in_flight', inflight} end

-- 🔴 驱逐分支：在这里（而不是在 Rust 侧）重校验过期。
--    §8.3 的"锁内重校验"在我们这里升级成"脚本内重校验" —— 更强。
if ARGV[8] == '1' then
  local exp = cur['expires_at_ms']
  if (not exp) or tonumber(exp) > tonumber(ARGV[9]) then
    return {0, 'not_expired', ''}
  end
end

redis.call('SET', KEYS[1], ARGV[1], 'KEEPTTL')
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('SET', KEYS[3], '',      'EX', ARGV[4])
redis.call('ZADD', KEYS[4], ARGV[7], ARGV[6])
return {1, 'started', ARGV[2]}
```

🔴 **四条与 e2b 的差异，每条都有理由：**

| 差异 | e2b | 我们 | 理由 |
|---|---|---|---|
| 在途转换的处理 | 在 Go 里 GET transition key，**在脚本外** | **在脚本内**，返回 `in_flight` ＋ 在途的 txn_id | e2b 靠锁把这一段圈住；我们把它塞进脚本，于是 `start_transition` 也**不需要锁**。少一把锁，少一个 TTL 要对齐 |
| 驱逐的过期重校验 | 在锁内、脚本外（`state_change.go:95-105`） | **在脚本内** | 同上。而且这修掉了我们**今天就有的** bug（§16.9） |
| `expires_at_ms` | 从 JSON 的 `EndTime` 反序列化后在 Go 里比 | 记录里额外冗余一个 `expires_at_ms: Option<i64>` | Lua 比不了 RFC3339 字符串。🔴 **冗余字段必须与 `metadata.expires_at` 同源同写**，由 `StoredSandboxRecord` 的 `From<SandboxMetadata>` 单点派生，不许有第二处赋值 |
| transition 索引 | 无 | 有 | §6.7 |

### 6.3 完成回调

```rust
pub struct TransitionGuard { /* 不可 Clone，Drop 时若未 complete 则 warn! + 尽力而为地标失败 */ }
impl TransitionGuard {
    pub async fn complete(self, outcome: Result<(), String>);
}
```

回调做四件事，**顺序照抄 `state_change.go:181-232`**：

1. 若 `effect == Transient` 且成功 ⇒ 先 `restore_to_running`（一次 `update_if_state`，
   闭包 = `|m| if m.state == from { m.state = Running }`）；
2. `SET result_key <err_or_empty> EX result_ttl`；
3. `DEL txn_key` ＋ `ZREM txn:index <member>`（**同一条 Lua**，e2b 是两条命令，
   我们合并是因为多了索引，两者必须一起消失）；
4. `PUBLISH notify <txn_routing_key>`。

🔴 **两条比 e2b 严的地方：**

- **`TransitionGuard` 不可 `Clone`，且实现 `Drop`。** e2b 的回调是一个裸 `func`，
  忘了调就永久卡住（靠 TTL 兜）。Rust 能做得更好：`Drop` 里发现没 `complete` 过就
  `warn!` ＋ spawn 一个尽力而为的失败标记。**这不是替代 TTL，是让忘记调用变得可观测。**
- **回调不再取第二把锁。** e2b 在回调里 `Obtain(GetLockKey(transitionKey))`（`state_change.go:190`）。
  我们把 2+3 合成一条 Lua ⇒ 不需要。少一把锁，也少一个「回调里拿不到锁就直接 return，
  transition key 留到过期」的分支（`state_change.go:191-195` 逐字就是这么写的）。

### 6.4 `AllowedTransitions`：我们的八态表

`SandboxState`（`src/orchestrator/types.rs:82-91`）：
`Creating` / `Resuming` / `Running` / `Snapshotting` / `Forking` / `Pausing` / `Paused` / `Killing`。

从十四个 `update_state_if_state` 调用点 ＋ 六个 `update_if_state` 调用点逐条读出来的**实际**转换表：

| from ＼ to | Creating | Resuming | Running | Snapshotting | Forking | Pausing | Paused | Killing |
|---|---|---|---|---|---|---|---|---|
| **Creating** | — | — | ✅ `:2446` | — | — | — | — | ✅ `:1055` |
| **Resuming** | — | — | ✅ `:2446` | — | — | — | ✅ `:1692` `:2587`（回滚） | — |
| **Running** | — | ✅ `:1657` | — | ✅ `:1771` | ✅ `:611` | ✅ `:1341` | — | ✅ `:1055` |
| **Snapshotting** | — | — | ✅ `:1827` `:1844` | — | — | — | — | — |
| **Forking** | — | — | ✅ `:666` `:684` | — | — | — | — | — |
| **Pausing** | — | — | ✅ `:1395` `:1411` `:1461` `:1525`（回滚） | — | — | — | ✅ `:1541`（`update`） | — |
| **Paused** | — | ✅ `:1657` | — | — | — | — | — | ✅ `:1260` |
| **Killing** | — | — | ✅ `:1135`（回滚 → previous） | — | — | — | ✅ `:1135`（回滚 → previous） | — |

🔴 **五条从表里读出来的东西：**

1. **`Pausing → Paused` 走的是 `update()`，不是 CAS**（`service.rs:1541`）。
   ⇒ 上 Redis 之后它必须变成 transition 的**完成回调**，而不是一次裸写。这是本表最重要的一格。
2. **`Killing` 的回滚目标是 `previous_state`，可能是 `Running` 也可能是 `Paused`**（`:1135`）。
   ⇒ `AllowedTransitions[Killing]` 有两个出口，而 e2b 的 `Killing` 是终态。**不要照抄它的表。**
3. **`Creating → Killing` 是允许的**（`:1055` 的 `expected` 是 `[Running, Paused]`，
   但 `Creating` 走的是 `StateConflict` ⇒ 等待重试的循环）。
   ⇒ 表里 `Creating → Killing` 实际是「等 `Creating` 结束再 CAS」，**不是直接转换**。写表时要区分。
4. **没有任何一条 `X → Creating`。** `Creating` 只由 `add` 产生。
5. 🔴 **`Resuming → Paused` 有两个来源**（`:1692` 与 `:2587`），
   而 `:2587` 的 `expected_state` 是变量（`std::slice::from_ref(&expected_state)`）——
   写表的时候要把这个变量的取值域跟出来，不能按字面写。

**落法**：`src/orchestrator/store/transitions.rs`，一张 `const ALLOWED: [[bool; 8]; 8]`
＋ 一个 `pub fn is_allowed(from, to) -> bool` ＋ 🔴 **一个把上表逐格断言一遍的测试**。
非法转换返回 `StoreError::InvalidTransition`，**不是** `StateConflict`
（后者的意思是「你晚了一步」，前者的意思是「你要求的事情不存在」）。

### 6.5 `Transient` vs `Expires`

| 转换 | effect | 完成后 |
|---|---|---|
| `Running → Snapshotting` | **Transient** | 回调恢复 `Running`（今天由 `:1844` 显式做，改成回调） |
| `Running → Forking` | **Transient** | 回调恢复 `Running`（今天 `:684`） |
| `Running → Pausing` | **Terminal(Paused)** | 回调写 `Paused`。🔴 **不拨 `expires_at`** —— e2b 的 `TransitionExpires` 会把 `EndTime` 拨到现在（`state_change.go:134-139`），因为它的暂停 = 生命结束。**我们的暂停不是**：`Paused` 沙箱可以 resume，`expires_at` 仍然有意义（`timeout_action` 决定过期后是 pause 还是 delete）。⇒ **这一支不要照抄** |
| `Paused → Resuming` | **Terminal(Running)** | 回调写 `Running` ＋ 新 `execution_id` |
| `* → Killing` | **Terminal(removed)** | 回调删记录 |

🔴 **`TransitionExpires` 那一支我们不要**，理由在上表第三行。
父提案 §8.2 逐字写「终态转换则顺手把 `EndTime` 拨到现在」——
照抄会让每一个暂停的沙箱立刻进入过期 ZSET 的可驱逐区间，
然后被驱逐器按 `timeout_action` 处理一遍。`Pause` 的话是 no-op（已经 Paused），
`Delete` 的话是**把用户刚暂停的沙箱删掉**。登记在 §16.7。

### 6.6 `wait_while_in_states` 的新实现

**契约逐字不变**（`store/mod.rs:93-102`）：等到状态不在 `transitional_states` 里，
返回最新 metadata；记录没了返回 `Ok(None)`；已经不在过渡态则立即返回。

```
wait_while_in_states(id, transitional) :
  ① rx = subscribe(notify, routing_key(id))       // 先订阅，再初探，顺序不能反
  ② loop {
       raw = GET sbx:<id>
       if raw is None                 ⇒ Ok(None)
       if state ∉ transitional        ⇒ Ok(Some(decode(raw)))
       select {
         rx.recv()      => continue
         tick(1s)       => continue                // 兜底，抄 pollInterval
       }
     }
```

🔴 **三条与 e2b 的差异：**

1. **我们等的是「状态」，e2b 等的是「某个 transition id」**（`state_change.go:268-313`）。
   保留我们的语义，因为契约里写的是状态，而调用点
   （`service.rs:2081`，唯一一个）拿到的也是状态。
2. **不因为「transition key 不存在但状态还在过渡态」就报错。** 那个判断留给 reaper（§6.7）。
   等待者报错会让 `join_concurrent_pause`（`service.rs:2129`）把一次正常的并发 pause 变成失败。
3. **外层的 60 秒超时保留在 `wait_for_transition`（`service.rs:2082`），不下沉进 store。**
   store 不知道调用方的耐心。

### 6.7 🔴 崩溃恢复的真实边界 —— 以及 e2b 兜不住我们的那个洞

模块文档 §8.2 说「分布式锁只覆盖读-判断-写，跨越整个操作的是 `transitionKey`，
**它的 TTL 才是崩溃恢复的边界**」。**这句话对了一半。**

`transitionKey` 的 TTL 让**下一次操作能开始**（`start_transition` 发现 `inflight == nil`），
但它**不修状态**。一条卡在 `Pausing` 的记录，在 transition key 过期之后仍然是 `Pausing`。
谁把它变回可用状态？**e2b 的答案在 `items.go:149-159`**，那段谁都没引过：

```go
// Only evict running sandboxes
if sbx.State != sandboxtypes.StateRunning {
    // If the sandbox is in transitioning state for more than stale cutoff,
    // it's likely failed removal. Let it be cleaned up by the regular expiration process.
    if time.Since(sbx.EndTime) <= sandboxtypes.StaleCutoff { continue }
    …
}
result = append(result, sbx)     // ← 卡住的转换被放进驱逐清单
```

⇒ **e2b 的崩溃恢复是三件事的合力**：transition key TTL（让路）＋ `AllowedTransitions`
（从过渡态出发的转换是合法的）＋ **过期 ZSET 的 stale-cutoff 分支**（真正把它清掉）。

🔴 **第三件对我们有洞。** e2b 的每个沙箱都有 `EndTime`；
我们的 `expires_at` 是 `Option<SystemTime>`（`metadata.rs:81`），
而 `index_expiry` 对 `None` **直接跳过**（`in_memory.rs:81-85`）。
⇒ **一个 `timeout = None` 的沙箱卡在 `Pausing`，永远不在过期 ZSET 里，永远没人清。**
它会一直占着那个 id：resume 报 `InvalidSandboxState`，delete 走
`update_state_if_state(Killing, [Running, Paused])` 撞 `StateConflict` 然后进
`wait_for_transition` 等 60 秒再失败。**用户层面表现为「这个沙箱删不掉」。**

⇒ **本文的答案：第四件套 —— `agentenv:api:txn:index` ＋ reaper。**

| 项 | 定义 |
|---|---|
| member | `<sandbox_id>:<execution_id>:<txn_id>` —— 🔴 **三段全要**。只有 sandbox_id 的话，一次针对死转换的 `ZREM` 会把同 id 的活转换摘掉，与 §6.3「过期索引 member 按 execution 作用域」是**同一条理由的第二个落点** |
| score | `now_ms + transition_key_ttl_ms` |
| 写 | `start_transition.lua` 的 `ZADD`；完成回调的 `ZREM`（与 `DEL txn_key` 同一条 Lua） |
| reaper | 与 healer 同一个后台任务，间隔 30 秒 ＋ jitter，每副本跑 |
| reaper 一轮 | `ZRANGEBYSCORE txn:index -inf now LIMIT 0 128` ⇒ 对每个 member：GET 记录 → ① 记录没了 ⇒ `ZREM`；② `execution_id` 对不上 ⇒ `ZREM`（死化身）；③ `GET txn_key` 仍存在且 == txn_id ⇒ **不动**（TTL 还没到，说明索引的 score 算早了）；④ 否则 ⇒ 记录卡住，执行**恢复动作** |
| 恢复动作 | 🔴 **不是"改回 Running"。** 是把记录标成 `stuck`：`update_if_state` 把 `expires_at` 拨到 `now`（如果它是 `None`），并 `ZADD` 进过期 ZSET。**然后交给驱逐器按 `timeout_action` 处理** —— 与 e2b 的出口一致，只是我们要自己把它送进那个出口 |
| 可热关 | 是。`orchestrator.transition_reaper_enabled`，**每轮重新读**（抄 `heal.go:66-69` 的 kill switch） |

🔴 **为什么恢复动作不是「改回 Running」**：一个在 `Pausing` 中途死掉的副本，
可能已经调过 `sandbox.pause()`（VM 已停）也可能没调。
`api` 副本**不知道**，而唯一知道的是 node —— 它的 `ListSandboxes` 里这台 VM 在不在。
⇒ 改回 `Running` 是在猜。送进驱逐 ⇒ 走 pause/delete 的完整路径 ⇒ 那条路径会去问 node。
**把一个不知道答案的问题交给知道答案的人，而不是猜一个。**

### 6.8 🔴 `handleExistingTransition` 三分支，以及 e2b 缺的深度上限

三分支照搬（`state_change.go:343-384`）：

| 分支 | 动作 |
|---|---|
| 在途转换的目标态 == 我要的 | 等它，返回它的结果，`already_done = true`。**不报 409** |
| 在途转换的目标态 ≠ 我要的，但 `is_allowed(current, mine)` | 等它结束，**重试整个 `start_transition`** |
| 非法转换 | 立刻 `InvalidTransition` |

🔴 **e2b 的第二分支是裸递归**（`state_change.go:383`：`return s.StartRemoving(ctx, teamID, sbx.SandboxID, opts)`），
**没有深度上限**，唯一的界是 ctx 的 deadline。在一个被反复转换的沙箱上，
这是一条可以吃掉整个请求预算的路径，而且栈上没有任何东西说明它递归过几次。

⇒ **我们写成循环，带 `max_transition_retries = 3`**，超过返回
`StoreError::TransitionInProgress { target }` ⇒ 409。
🔴 **并且日志里带重试次数**（`_sd-recon-env.md` §8 待办 (1)：「被 fence 的操作零日志」，别再欠一次）。

**一条 e2b 逐字提醒，要抄进注释**（`state_change.go:336-342`）：

> 重试的是**调用方的那次操作，完整地**。等待期间丢掉的任何一个参数，
> 都是一条被静默丢弃的调用方指令 —— 它咬过 `FilesystemOnly` 和 `Reason`，
> 而**对 `ExpectExecutionID` 最要命，因为等待窗口正好是沙箱可能被换化身的那一段**。

对我们：重试时必须**重新读一次记录**再判 `expected_execution_id`，
不能沿用等待前读到的那份。

---

## 7. 原语三：全局过期 ZSET ＋ healer

### 7.1 member 形状

```
member = "<sandbox_id>:<execution_id>"          score = expires_at 的 unix 毫秒
```

🔴 **execution 作用域是 §6.3「必须留的」六条 fencing 里的第五条**，逐字理由在
`storage/redis/utils.go:31-35`：

> Scoping the member to the execution makes every ZREM **structurally safe**:
> removing a dead execution's member can never unindex a live one, even when a
> lockless Add for the same sandbox ID races a Remove or the evictor's stale sweep.

⇒ 三条落地规则：

1. `remove()` 的 `ZREM` **必须用它刚删掉的那条 JSON 里的 `execution_id`**，
   不是调用方手里的那个。逐字抄 `operations.go:104-118`：`removeSandboxScript` 返回被删的 JSON，
   Rust 侧从中取 execution 再 `ZREM`。
2. `update_if_state` 换化身时**先 ZREM 旧 member 再 ZADD 新 member**（§5.5）。
3. 解析不出三段 / 第二段不是合法 uuid 的 member ⇒ **当垃圾扫掉**并计数
   （抄 `items.go:53-62` ＋ `utils.go:46-59` 的 `uuid.Parse` 校验）。

### 7.2 `expired_batch(now, limit)`

```
① ZRANGEBYSCORE {global}:expiry -inf <now_ms> LIMIT 0 <limit>      // limit 默认 256，抄 e2b
② 解析 member；解析失败 ⇒ stale
③ 分块 MGET sbx:<id>                                              // 🔴 任一块出错 ⇒ 整轮放弃（§9）
④ raw == nil                         ⇒ stale（孤儿 member）
⑤ member.execution != record.execution ⇒ stale（死化身）
⑥ !record.is_expired(now)            ⇒ 🔴 重打分 ZADD **XX**，不入结果
⑦ record.state != Running:
     若 now − expires_at <= stale_cutoff ⇒ 跳过（正常的在途转换）
     否则                                ⇒ 入结果（卡住的转换，§6.7）
⑧ ZREM 全部 stale；ZADD XX 全部重打分；各自计数
```

🔴 **第 ⑥ 步的 `XX` 不能省。** `ZADD` 不带 `XX` 会把一个并发 `Remove` 刚删掉的 member
**复活**（`items.go:140` 逐字注释：「XX: never resurrect a member a concurrent Remove deleted」）。

🔴 **第 ⑦ 步 `stale_cutoff`** 我们定 **`transition_key_ttl × 2` = 180 秒**，
而不是 e2b 的常量 —— 它必须严格大于一次合法转换的最长时间，否则会把正在 pause 的沙箱
当成卡住的。

**指标**（四个 counter，抄 `items.go:170-178` 的分类）：
`expiry_index_swept_total{reason="orphan|dead_execution|invalid"}`、
`expiry_index_rescored_total`、`expiry_index_healed_total`、`transition_reaped_total`。

### 7.3 🔴 锁内重校验：我们做得比 §8.3 更强，因为今天有个 bug

**今天的驱逐**（`service.rs:2201-2237`）：

```rust
let expired = self.store.list_expired(SystemTime::now()).await?;
for metadata in expired {
    if metadata.state != SandboxState::Running { continue; }
    match metadata.timeout_action {
        Pause  => self.pause_sandbox_inner(metadata.id).await…,
        Delete => self.delete_sandbox_inner(metadata.id, Forget).await…,
    }
}
```

`pause_sandbox_inner` 做 `update_state_if_state(Pausing, [Running])`（`:1341`）——
那是一个**状态** CAS，**不是过期** CAS。
⇒ 在 `list_expired` 与那次 CAS 之间到达的一次 `keep_alive`（`:967`，它只要求 `Running`，
所以能成功）会把 `expires_at` 往后推，然后驱逐照样把沙箱暂停了。

🔴 **这是今天就存在的 bug，不是 Redis 引入的。** 只是今天两次 `await` 之间的窗口很窄，
上 Redis 之后窄不下去（两次网络往返）。

⇒ **落法**：驱逐走 `start_transition(..., TransitionRequest { eviction: true, .. })`，
过期重校验在 §6.2 的 Lua 里、与状态写**原子**完成。
三种返回：`not_expired` ⇒ `EvictionNotNeeded`（不是错误，debug 日志即可）；
`in_flight` ⇒ `EvictionInProgress`（抄 e2b 的 `ErrEvictionInProgress`，`state_change.go:98`）；
`started` ⇒ 正常执行。

### 7.4 healer

逐条抄 `heal.go`，四个设计点一个不少：

| 点 | 落法 |
|---|---|
| 每副本跑，无 leader election | `Orchestrator` 的后台任务，5 分钟 ＋ jitter（抄 `heal.go:17` `:30`） |
| `ZADD NX` 让并发轮次幂等无害 | `heal_expiry_index()` 内部用 `ZADD NX` |
| grace period 跳过刚起的沙箱 | `now − created_at < heal_grace`（默认 60s）⇒ 跳过。抄 `heal.go:99-101`。🔴 我们用 `created_at` 而不是 e2b 的 `StartTime`，两者同义（`metadata.rs:78`） |
| feature flag 每轮重新求值，可热关 | `orchestrator.expiry_healer_enabled`，**每轮读一次 config**，注释逐字抄「acts as a kill switch without redeploy」 |

**一轮的动作**：`SSCAN {global}:index` 分批（**不是 `SMEMBERS`** —— healer 扫全集，
用 SSCAN 才不会一次阻塞 Redis）⇒ 每批 `MGET` ⇒ 算出应有的 member 与 score ⇒
`ZMSCORE` 查缺（**score 为 0 即缺失**，抄 `heal.go:112-113` 的论证：合法 score 是 unix 毫秒，永不为 0）⇒
`ZADD NX` 补回。

🔴 **`expires_at == None` 的记录**：healer **也要把它们索引进去**，
score = `lifetime_deadline` 的毫秒；`max_lifetime` 也是 `None` 时**才**跳过。
这与 §6.7 的洞是同一件事的正面修法：让「永不过期」的记录仍然有一个可扫描的坐标。

**TOCTOU 论证逐字抄进注释**（`heal.go:136-140`）：

> 与并发 `Remove` 的 TOCTOU 只可能种下一个孤儿 member，
> 它会在 score 越过之后被 `expired_batch` 扫掉 —— 是垃圾，**永远不是误驱逐**
> （驱逐会重读记录并在脚本内重校验过期）。

🔴 **注意这条论证的成立前提是 §7.3 已经落地。** 顺序：先做过期重校验，再开 healer。
反过来就是在没有安全网的情况下往索引里种东西。

### 7.5 多副本驱逐的并发闸

e2b：50ms 轮询 ＋ `activeEvictions sync.Map` 进程内去重 ＋ `AdjustableSemaphore` 限流。

我们：`auto_evict_interval_ms = 1000`（`config/default.toml:230`），N 个副本。

| 项 | 决定 |
|---|---|
| leader election | **不做**。§6.1 逐字：「N 个副本跑 N 个 evictor 而不打架，靠的完全是 store 的原子状态转换」 |
| 进程内去重 | **做**，`DashMap<SandboxId, ()>`，与 `activeEvictions` 同形 |
| 跨副本去重 | **靠 transition key**。第二个副本拿到 `in_flight` ⇒ `EvictionInProgress` ⇒ debug 日志，不告警 |
| 并发上限 | 做一个固定的 `Semaphore(max_concurrent_evictions)`，默认 16。🔴 **不做 e2b 的可热调**（那需要 feature flag 基建，我们没有），但**要有这个信号量** —— 一次 200 个沙箱同时过期而没有闸，会把 node 的 gRPC 面打满 |
| 轮询间隔 | 保持 1 秒。🔴 **不抄 e2b 的 50ms**：那是 N × 20 次/秒的 `ZRANGEBYSCORE`，对单实例 Redis 是白付的负载，而我们的 SLA 没有要求 50ms 的驱逐精度 |

---

## 8. 原语四：`Reserve` 三态

### 8.1 🔴 先把价值论证改对

父提案 §8.4 与阶段 3 第 2 条说 `Reserve` 解决「并发 create 去重」，
`alreadyPending` 是「客户端重试」与「用户真的建了两次」的分水岭。

**在我们的 API 形状下，这个分水岭不存在：**

```yaml
# src/api/openapi.yml:540-543
NewSandbox:
  required:
    - templateID
  properties:
    templateID: …          # ← 没有 sandboxID
```

```rust
// src/orchestrator/service.rs:368
let sandbox_id = SandboxId::new();          // Uuid::now_v7()，服务端现铸
```

⇒ **客户端重试 `POST /sandboxes` 会得到一个新沙箱，不是一次冲突。**
`alreadyInStorage` / `alreadyPending` 在 create 路径上**永远不会返回**。

**但三态仍然要做，理由有两条，都不是父提案写的那条：**

| # | 真正的触发点 | 说明 |
|---|---|---|
| ① | 🔴 **`restore_sandbox(sandbox_id, …)` 接受调用方给定的 id**（`service.rs:387-397`） | 这是跨节点 resume 的落点。两个 `api` 副本同时收到同一个暂停沙箱的 resume ⇒ 两次 `restore_sandbox`，同一个 id。今天靠 `paused_registry` 的 `ClaimForResume` 挡（那是 PG 的活），阶段 3 之后活跃态在 Redis，**去重要在这里做一次** |
| ② | 🔴 **「创建中窗口」对集群不可见** | `store.add` 发生在 VM 建好**之后**（`service.rs:2390-2408`）。从 `SandboxId::new()` 到 `add` 之间可能是几秒到几十秒（拉镜像、开 VM、等 envd）。这段时间里记录不存在 ⇒ 任何拿 `ListSandboxes` 对账的东西都会把这台 VM 判成孤儿并杀掉。**pending ZSET 就是这个窗口的可见性** |

⇒ **①是并发去重，②是孤儿误判防护，而②在父提案里一个字都没有，它比①更容易出事。**

### 8.2 Lua

```lua
-- reserve.lua
-- KEYS = [{global}:index, {global}:pending, reserve:<id>]
-- ARGV = [sandbox_id, now_s, stale_cutoff_s]
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[3])   -- 陈旧 pending 自动失效
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 1 end   -- alreadyInStorage
if redis.call('ZSCORE',    KEYS[2], ARGV[1])      then return 2 end   -- alreadyPending
-- 3 = limitExceeded，见 §8.5：本阶段不产生
redis.call('DEL',  KEYS[3])                                -- 清掉上一次失败留下的 result
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return 0                                                   -- reserved
```

逐字对齐 `reservations/redis/scripts.go:35-64`，**只删掉配额那三行**（`SCARD` + `ZCARD` + 比较）。
`stale_cutoff = now − reserve_stale_ttl`，`reserve_stale_ttl` 默认 **300 秒**
（e2b 是 90 秒，注释说「well beyond any realistic sandbox creation time」；
我们的创建要拉 OCI 层、可能要转换 overlaybd，90 秒不够 —— 🔴 **这个数必须大于最慢的一次
冷启动**，定小了就是「建到一半被别人抢走 id」）。

### 8.3 三个分支的调用面

```rust
pub enum Reservation {
    /// 我抢到了。调用方**必须**在成功或失败后调 finish。
    Reserved(ReservationGuard),
    /// 已经建好了。
    AlreadyInStorage,
    /// 别人正在建 ⇒ 等它的结果，不是 409。
    AlreadyPending(WaitForStart),
    // 🔴 LimitExceeded 的位置留着，见 §8.5。
}
pub struct ReservationGuard { /* Drop 时未 finish ⇒ warn! + 尽力而为 release */ }
impl ReservationGuard { pub async fn finish(self, outcome: Result<SandboxMetadata, String>); }
pub struct WaitForStart { /* .await -> Result<SandboxMetadata> */ }
```

**接线：**

| 路径 | 怎么用 |
|---|---|
| `create_sandbox` | 🔴 **仍然调 `reserve`**，尽管 id 是现铸的、必然返回 `Reserved`。理由是 §8.1 的 ②：pending 条目是创建中窗口的**唯一**集群可见性 |
| `restore_sandbox` | 调 `reserve`。`AlreadyPending` ⇒ `wait_for_start().await` ⇒ 返回别人建好的那个沙箱（对调用方来说 resume 成功了，这是对的） |
| `fork_sandbox` 的每个子 | 调 `reserve`（子 id 也是现铸的，同 create） |
| `add` | 🔴 **必须在同一条 Lua 里 `ZREM` pending** —— 记录一旦入库，pending 条目就该消失。否则 `reserve` 会在 `SISMEMBER` 命中之后仍然留着一条 pending 垃圾，被 `stale_cutoff` 拖 300 秒 |

### 8.4 `AlreadyPending ⇒ wait_for_start`

抄 `reservation.go:152-231`：

```
① rx = subscribe(notify, reserve_routing_key(id))
② 初探 try_read_result()                        // 可能在订阅前就完成了
③ loop { select { rx.recv() | tick(1s) } → try_read_result() }

try_read_result:
   GET reserve:<id>          → 有 ⇒ 解码返回（成功或失败）
   ZSCORE pending <id>       → 不在 ⇒ 🔴 再 GET 一次 reserve:<id>（竞态窗口）
                                      仍然没有 ⇒ Err("no longer pending and has no result")
   在 ⇒ 继续等
```

🔴 **第三步那次「再 GET 一次」是 e2b 修过的一个真实竞态**（`reservation.go:211-214`
逐字注释：「Re-read the result in case finishStart or a new Release wrote it between
the initial GET and the legacy pending-set check」）。**不要因为看起来冗余就删掉。**

**等待预算**：由调用方的 ctx 决定，store 不设。但 🔴 `api` 侧的 HTTP handler
必须给它一个**小于反向代理超时**的预算，否则一次 pending 卡住会变成一个挂住的连接。

### 8.5 配额那一支的留位

```rust
// 🔴 Intentionally unreachable in phase 3. AgentENV has no tenant model
// (service-decomposition §4.4: `grep team_id src/` only hits the generated
// E2B-compatible schema, and src/api/impls/auth.rs is "presence, not
// validity"), so there is no quota subject to count against. The variant and
// the script's return code 3 are reserved so that adding a tenant model later
// is a change to the script's middle, not to this enum and every match on it.
LimitExceeded { subject: String, limit: u64 },
```

⇒ **枚举变体留、脚本返回码 3 留、`SCARD`/`ZCARD` 那三行不写。**
`match` 上写 `LimitExceeded { .. } => unreachable!("no tenant model in phase 3")`，
🔴 **不是 `_ => {}`** —— 将来接上租户模型时，编译器要能把每个漏掉的分支指出来。

---

## 9. `GetMany` 的覆盖集语义

> 🔧 **2026-08-21：本节的结论全部经复核仍然成立**，只有 `central.rs` 的行号漂了（§17.5 第 1 条）。
> 🔴 §9.2 第 3 条（报错 ⇒ 整轮跳过）现在有了一个仓内先例和一个更硬的名字：见 §17.2。

### 9.1 🔴 更正：这条**已经修好了**，不是遗留项

父提案 §8 陷阱 7 逐字：「⇒ 新 store 必须把『本次实际覆盖的 id 集合』带在响应里…
**这条在 PG 版本里就是遗留项，别原样继承。**」

**事实相反，两侧都已经实现：**

```go
// services/scheduler/internal/registry/store.go:50-63   —— 契约
// 🔴 A sandbox missing from the returned map has no row … The caller deletes
// local artifacts on the strength of that absence, so any failure at all must
// be an error rather than a shorter map.
//
// Rows.Covered is that guarantee made checkable.
GetMany(ctx, clusterID string, sandboxIDs []string) (Rows, error)
// store.go:195-198  Covered []string
// store_postgres.go:395  return Rows{Entries: entries, Covered: ids, Now: now}, nil
// registry_service.go:231  CoveredSandboxIds: rows.Covered,
```

```rust
// src/orchestrator/paused_registry/central.rs:305 -- 调用方**已经在断言**
Self::require_full_coverage(operation, chunk, &response.covered_sandbox_ids)?;
```

⇒ **本半要做的不是「修一个遗留 bug」，是「把已经存在的模式搬到 Redis store 上」。**
这个区别重要：前者会让人去改 Go 侧的 PG 代码（那是**阶段 3 删除批**里的东西，
[S] §11.3 论证过它是回退的前提），后者只碰新代码。

### 9.2 Redis 侧的落法

```rust
pub struct MetadataRows {
    pub entries: HashMap<SandboxId, SandboxMetadata>,
    /// 本次**实际查过**的 id，present 与 absent 都算。
    pub covered: Vec<SandboxId>,
}

async fn get_many(&self, ids: &[SandboxId]) -> Result<MetadataRows> {
    // 🔴 空批次不发请求。"我什么都没查"是一个事实，
    //    调用方拿它去对一个什么都没要的请求。抄 store_postgres.go:344-352。
    if ids.is_empty() { return Ok(MetadataRows { entries: HashMap::new(), covered: vec![] }); }
    let mut entries = HashMap::with_capacity(ids.len());
    for chunk in ids.chunks(GET_MANY_CHUNK) {          // 256
        // 🔴 MGET 的任何错误、任何一条解不开的 JSON ⇒ 整个调用失败。
        //    一份短了的 map 与"这些沙箱没有记录"长得一模一样，
        //    而调用方对后者的回答是删本地产物、拆掉正在跑的 VM。
        let raws: Vec<Option<Vec<u8>>> = mget(chunk).await?;
        for (id, raw) in chunk.iter().zip(raws) {
            if let Some(raw) = raw { entries.insert(*id, decode(&raw)?); }
        }
    }
    Ok(MetadataRows { entries, covered: ids.to_vec() })
}
```

**为什么 Redis 上仍然需要 `covered`**，尽管 `MGET` 是原子的：

1. 我们分块（256），块与块之间不原子 ⇒ 与 PG 的 chunk 同构；
2. `get_many` 的结果会跨 gRPC 面传（`api` 拿 node 的 `ListSandboxes` 去对 Redis），
   而**截断可能发生在那一跳上**，不在 Redis 这一跳；
3. 🔴 e2b 在同一个位置放的是另一条护栏，也要抄：
   `main.go:191-200`，pipeline 出错 ⇒ **整轮跳过、`return nil`**，
   逐字「Pipeline error — skip entirely to avoid mass kills」。
   ⇒ 我们的孤儿对账在 `get_many` 报错时**必须跳过整轮**，不是「按拿到的那部分处理」。

### 9.3 🔴 不要抄的那个 fail-open

```rust
// src/orchestrator/paused_registry/central.rs:317-330
fn require_full_coverage(...) -> RegistryResult<()> {
    if covered.is_empty() {
        return Ok(());          // ← 🔴 旧控制面不报覆盖集 ⇒ 放行
    }
```

那条豁免有它的理由（跨版本滚动，注释里写了），**但它不适用于本半**：
`RedisMetadataStore` 与它的调用方在**同一个进程、同一次编译**里。
⇒ **Redis store 的 `get_many` 调用方不得有「covered 为空就放行」的分支。**
`covered.len() != ids.len()` ⇒ `panic!`-级别的编程错误（用 `debug_assert!` ＋ 生产环境返回 `Backend`）。

---

## 10. 留下的两处 fencing，逐处

§6.3 的表说阶段 3 之后 fencing「从五处收到两处」，承重的两处是
**状态转换的 execution CAS** 与 **目录提交的 execution 谓词**，
外加**过期索引 member 按 execution 作用域**（§6.3 末尾单独点名的第六个落点）。
本半负责第一处和第三处。

### 10.1 execution CAS —— 落在三条 Lua 里，不是一条

父提案写「Redis Lua 脚本内，**一处**」。**实际是三处**，因为写路径有三条：

| 脚本 | 谓词 | 承重什么 |
|---|---|---|
| `start_transition.lua`（§6.2） | `ARGV[5] ~= '' and cur.execution_id ~= ARGV[5] ⇒ execution_superseded` | 逐字对应 e2b 的 `RemoveOpts.ExpectExecutionID`。pause / delete / snapshot / fork 的入口 |
| `update_if_state.lua`（§5.3） | `rev` ＋ `execution_id` 双谓词 | 🔴 **e2b 在这条路径上没有谓词**（`operations.go:203` 裸 `SET`）。这是我们比它强的一处，也是 §16.6 那条更正的落点 |
| `remove.lua` | 返回被删的 JSON，让 `ZREM` 用**实际删掉的那个** execution | `operations.go:96-118` |

🔴 **三条都在脚本内原子，绝不是先查后写。** 这是 `scripts.go:33-40` 的全部理由，
而它对我们更强一层：**`add` 在我们这里也是 lockless 的**（`in_memory.rs:96` 今天就是），
`restore_sandbox` 会在任何时刻装进一个新化身。

### 10.2 过期索引 member —— §7.1 已述

排期上单列一行，别只对着「状态 CAS」那一行做。

### 10.3 🔴 孤儿判定：`main.go:205-209` 的 bug 不要抄，而且我们的形状更危险

> 🔧 **2026-08-21：下面这张四行判定表要改成五行三态** —— 缺的那一行是
> 「`get_many` 报错 ⇒ **我不知道** ⇒ 整轮放弃」，它今天写在表下面的第 ④ 行里，
> 但和另外三行**不是同一类答案**，混在一张表里会被读成第四种判定。
> 重述后的表在 §17.2。

e2b 的缺口（已核）：

```go
// packages/api/internal/sandbox/storage/redis/main.go:205-209
if raw != nil {
    // Sandbox exists in store, not an orphan.
    continue
}
```

只测 sandbox id 的**存在性**。同 ID 在 B 节点重建之后，A 节点回来的旧化身查库命中的是
**新化身的记录** ⇒ 不判 orphan ⇒ 永远杀不掉 ⇒ 无路由僵尸。

**我们的判定必须是：**

```
node 报的 (sandbox_id, execution_id)：
  ① 记录不存在                        ⇒ 🔴 先查 pending：在 pending 里 ⇒ **不是孤儿**（创建中，§8.1 ②）
                                          不在 pending  ⇒ 孤儿
  ② 记录存在但 execution_id 不同      ⇒ 孤儿（旧化身）
  ③ 记录存在且 execution_id 相同      ⇒ 不是孤儿
  ④ get_many 报错 / 覆盖集不全        ⇒ 🔴 整轮放弃，不判任何东西（§9.2 第 3 条）
```

🔴 **第 ① 步查 pending 是我们独有的，因为我们的 `store.add` 在 VM 之后**（`service.rs:2390-2408`）。
e2b 不需要这一步（它的 `Reserve` 之后立刻 `Add`）。
**漏掉这一步 = 每一台正在冷启动的沙箱都是孤儿 = `api` 会去杀正在建的 VM。**

🔴 **同时提醒**：`KillOrphan` 本身**还没写**（§6.2 ② 逐字：全仓只有两处注释提到它，
`services/scheduler/internal/registry/store_postgres.go:1055` 把它登记为待办）。
所以这不是「要修的缺陷」，是**写它的时候按上表写**。它落在 [S]（`api` 的对账路径），
本半只负责把 `get_many` ＋ pending 查询这两个原料给足。

---

## 11. 测试：真 Redis，不是假的

### 11.1 harness

**用 testcontainers，不用本机 `redis-server` 二进制。** 三条理由：

1. 🔴 **Rust 侧已经有这套基建**：`crates/test-support/Cargo.toml:10-11`
   （`testcontainers 0.23` ＋ `testcontainers-modules 0.11`，现开 `minio`），
   `crates/test-support/src/minio.rs:22-37` 就是模板。加一个 feature 而已；
2. `testcontainers-modules 0.11.6` 有 `redis` feature（已核：`Cargo.toml:305` ＋ `src/redis/`）；
3. Go 侧的 `REDIS_SERVER_BIN` 走本机二进制，是因为 Go 那边没有 testcontainers 依赖；
   Rust 这边有，**而且版本可控** —— 本机 `redis-server` 是发行版给什么就是什么，
   容器可以钉 `redis:7.4.10-alpine`，与集群镜像（`redis.yaml:110`）**同版本**。

```rust
// crates/test-support/src/redis.rs
pub struct RedisFixture {
    pub url: String,                       // redis://127.0.0.1:<mapped>
    _container: ContainerAsync<Redis>,
}
impl RedisFixture {
    pub async fn start() -> Result<Self> { … }
    /// 每个测试自己的键空间，避免并行测试互相踩。
    /// 🔴 用 SELECT <db> 而不是键前缀：前缀会让 `{global}` hash tag 那条
    ///    Lua 的 KEYS 计算跟着测试变形，而那正是要测的东西。
    pub fn db(&self, n: u8) -> String { format!("{}/{n}", self.url) }
}
```

🔴 **`Cargo.toml` 的 feature 要写成 `features = ["minio", "redis"]`，不是新加一个 dep。**

### 11.2 三层测试

| 层 | 位置 | 内容 | 跑在哪 |
|---|---|---|---|
| **L1 契约套** | `src/orchestrator/store/contract.rs`（`#[cfg(test)]` ＋ `pub(crate)` 宏） | 一套断言，**参数化在 store 构造器上**，同时跑 `InMemoryMetadataStore` 与 `RedisMetadataStore`。覆盖十二个原有方法的全部现有语义 | in-memory 部分进 `make test-unit`；Redis 部分进 `make test-with-redis` |
| **L2 Redis 专属** | `tests/redis_store.rs` | 锁、transition 三件套、过期 ZSET、healer、reaper、`Reserve`、`get_many` 覆盖集、KEEPTTL、execution CAS | 只在 `make test-with-redis` |
| **L3 跨副本** | `tests/redis_store_multi.rs` | 🔴 **两个 `RedisMetadataStore` 实例 ＋ 一个 Redis**。这是唯一能证明「跨副本互斥」的层，也是 §14 判据 3 的载体 | 只在 `make test-with-redis` |

🔴 **L1 是回答 `CLAUDE.md` 那条疤的正面手段**（「a change made to the in-memory store and
forgotten for Redis is invisible everywhere else」）：一套断言两个后端，
改了一个忘了另一个 ⇒ **红**。
🔴 **但 L1 覆盖不到的东西要说清楚**：四个新原语在 in-memory 上是退化实现（§3.2 的 default impl），
L1 对它们只能断言「退化语义自洽」，不能断言「两个后端等价」——
**它们本来就不等价**（[S] §14.6 已经裁定 node 的账本不需要 CAS 正确性）。
⇒ **L1 的方法清单里要显式排除这六个，并在注释里说明为什么**，
否则将来有人看到 L1 里没有 `start_transition` 会以为是漏了。

### 11.3 CI 与 Makefile

> 🔧 **2026-08-21：下面这段抄的 `services/Makefile` 模板已经长出两件新东西** ——
> `-count=1`（在两个子 Makefile 的 `:19`）与 `report-skipped-suites`（`services/Makefile:46`），
> 各自都是被一次真事顶出来的。🔴 **并且「新增一个 CI job」要与代码同批，不是等它稳定了再进**：
> 理由与证据在 §17.4 ①②。

```make
# 🔴 与 services/Makefile:48-52 同一条纪律：先证明依赖在，再把 skip 变成 fail。
test-with-redis:
	@docker info >/dev/null 2>&1 || { \
	  echo "docker not available: install it, or the Redis store tests silently skip"; \
	  exit 1; }
	AENV_REDIS_TEST_REQUIRED=1 cargo test -p agentenv --test redis_store --test redis_store_multi
	AENV_REDIS_TEST_REQUIRED=1 cargo test -p agentenv --lib store::contract
```

| 项 | 决定 |
|---|---|
| 默认 `make test` | **不跑** L2/L3（需要 Docker）。L1 的 in-memory 部分跑 |
| `AENV_REDIS_TEST_REQUIRED=1` | 缺 Docker ⇒ **失败**，不是 skip。镜像 `SCHEDULER_REDIS_TEST_REQUIRED` |
| CI | 新增一个 job 跑 `make test-with-redis`。🔴 **与 `make test` 分开的 job**，因为它需要 Docker-in-Docker 或 socket 挂载，而 `make test` 不需要 |
| `cargo adev` | 在 `adev` 里加一个 `cargo adev ci` 的条目，与现有 CI 配置生成对齐 |

🔴 **一条纪律**：`make test-with-redis` 一旦进 CI，**`InMemoryMetadataStore` 的任何改动
都必须同时跑它**。写进 `CLAUDE.md` 的 "Build, Lint, Test Commands" 一节
（与那条已有的 `test-with-postgres` 说明并列）。

---

## 12. `InMemoryMetadataStore` 的去留

**不在这一批删。** 三条理由，逐条对应上游：

1. §7 的规矩：**删除不与切换同批**。阶段 3 的删除清单本身就写着
   「删除动作放下一个 release（观察期之后）」，`InMemoryMetadataStore`
   已经被 v3 从「同批必做」移到那里（服务拆分 §7 阶段 3 的 🔧 注）。
2. **回退 `--role all` 依赖它存在。** 两条 store 路径在这一批期间**都保留**是回退声明的内容。
3. 🔴 **[S] §14.6 的裁定把它从「暂缓删除」升格为「长期保留」**：
   `--role node` 仍然构造完整的 `Orchestrator`，用 `InMemoryMetadataStore` 作为
   **节点自己的账本**（不是集群权威）。

⇒ **本文对 D7 的收窄表述（要写回 D7）：**

> `InMemoryMetadataStore` 不再是**集群权威**，但**长期保留**为 node 角色的本地账本。
> 集群里唯一的权威活跃态 store 是 `RedisMetadataStore`，`--role api` 只用它。
> D7 的三条依据全部仍然成立 —— 它们论证的是「**权威**状态不要有两个后端」。
> 🔴 **代价诚实登记**：`update_if_state` 之类的语义会有两个实现，
> 正是 `CLAUDE.md` 那条疤的形状。缓解有两层：
> ① §11.2 的 L1 契约套让共有语义的漂移变成红；
> ② 两个实现服务两个不同的调用面（node 的账本从不并发决策）。
> **但这两层都不能覆盖新原语，所以这条代价不会归零，只会变小。**

**这一批要做的收窄动作**（不是删除）：

| 动作 | 说明 |
|---|---|
| `Orchestrator` 的默认类型参数（`service.rs:93-97`） | **不动**。改它等于让 `--role all` 变形 |
| `with_in_memory_store()`（`service.rs:145`）/ `with_file_backed_store_and_factory()`（`service.rs:155`） | **不动**，它们是 node/all 的装配入口 |
| 🔴 文档 | 在 `in_memory.rs` 的模块注释里加一段：**「这个 store 是节点本地账本，不是集群权威。`--role api` 从不构造它。」** 一句话，防止将来有人把它当成「另一个后端」去做双写 |

---

## 13. Redis 持久化与 HA

### 13.1 🔴 现有清单的两条注释在阶段 3 之后是**假的**

`deploy/k8s/base/redis.yaml` 已经在树里（本会话另一位所写，未提交），
它自己已经预见到了阶段 3（`:11-21` 逐字写了 `appendonly yes` 与 `noeviction` 的理由，
`:23-29` 把 HA 登记成 OPEN ITEM）。**这份文档质量很高**，但有两处理由注释会随阶段 3 失效：

| 位置 | 现注释 | 阶段 3 之后 |
|---|---|---|
| `:115-118` `appendfsync everysec` | 「a host-level crash can lose at most the last second of binding writes, **all of which the next heartbeat re-sends anyway**」 | 🔴 **后半句变假。** 活跃态记录没有任何 5 秒周期的写路径去重发它。丢一秒的写 = 丢掉那一秒内的状态转换 |
| `:80-85` `strategy: Recreate` | 「The cost is a short gap on every Redis restart; **the scheduler survives it because a binding that fails to write is re-sent by the next heartbeat**」 | 🔴 **同样变假。** 阶段 3 之后每一次 Redis 重启都是**整个控制面的写不可用窗口**（长度 = AOF 重放时间）。`api` 必须把它表现成 503，而不是静默降级 |

⇒ **本半的交付里包含一次对 `redis.yaml` 的注释改写**（不改配置值，只改理由），
外加一条新注释说明「写不可用窗口的处置见本文 §13.3」。
🔴 **配置值本身不改**：`everysec` 仍然是对的选择 —— `always` 会把一次 fsync 放进
每一次状态转换的关键路径，而这个卷是 local-path（`redis.yaml:47-49`），没有 NVMe 保证。
**代价用「重建路径必须可用」来买单，而不是用 fsync 买单。**

### 13.2 HA：这个集群做不出来，说清楚做得出什么

`_sd-recon-env.md` §2：集群只有两台（203 master / 204 worker），
所有 PVC 都是 `local-path` 且钉在 204（SD-B5）。

| 方案 | 在这个集群上 | 判断 |
|---|---|---|
| 托管 Redis（e2b 的做法） | 不可用 | 生产的正确答案，本阶段够不着 |
| Sentinel（1 primary + 1 replica + 3 sentinel） | **能跑，但 quorum 是假的**：3 个 sentinel 摊在 2 台机器上，承载 2 个的那台一挂就没多数 | ❌ **不做**。一个不能自动切换的自动切换机制比没有更糟 |
| **primary/replica ＋ 手动切换** | ✅ 能做：primary 放 **203**（那台没有 PG / RustFS / registry），replica 放 204，各自 local-path PVC，`replica-read-only yes` ＋ `appendonly yes` | 🟡 **建议做**，但**不是阶段 3 的前置** |
| 单实例 ＋ 重建路径 | ✅ 现状 | 🔴 **这是阶段 3 的设计契约**，见 §13.3 |

⇒ **primary/replica 的收益要说准**：它把「204 挂掉 ⇒ 登记表 ＋ 快照桶 ＋ **活跃态** 一起丢」
降级成「204 挂掉 ⇒ 登记表 ＋ 快照桶丢，活跃态还在 203」。
它**不**提供自动故障切换，RPO = 一次复制延迟，RTO = 一次人工 `REPLICAOF NO ONE` ＋ 改 Service selector。
**它是一份可恢复的副本，不是 HA。** 别在文档里把它写成 HA。

### 13.3 🔴「全丢 ⇒ 重建」：规格 ＋ 验证

**这是本阶段的设计契约，不是兜底。** 父提案 §8 陷阱 8 给了两个选项
（「要么同等对待，要么明确接受并**验证**这条路径」），本文选后者并把它写全。

> 🔧 **2026-08-21：下面的触发条件要按 §17.2 的三态补一句** —— 一台 `ListSandboxes`
> **调不通**的 node，既不满足「报了非空」也不该被当成「报了空」。见 §17.5 第 5 条。

**触发条件**（`api` 启动时 ＋ 运行中检测到）：

```
store 为空（SCARD {global}:index == 0）
  AND 至少一个 node 的 ListSandboxes 返回了非空的、带 control_plane_config 的沙箱
```

🔴 **两个条件缺一不可。** 只看第一个，会在**集群真的空**的时候跑一次无害但令人困惑的重建；
只看第二个，会在正常运行中被一次瞬时的 store 读失败触发。

**重建过程：**

| 阶段 | 来源 | 重建出什么 |
|---|---|---|
| ① `Running` 沙箱 | 每个 node 的 `ListSandboxes`（[S] §3.2）的 `control_plane_config` | 🔴 **完整的 `SandboxMetadata`**（§2.1 末尾对 [S] §3.4 的更正：这个 blob 必须是 metadata 本体的版本化编码，不是七字段子集） |
| ② `Paused` 沙箱 | PG 目录（阶段 2 的产物）／`paused_sandboxes` 表 | `state = Paused` ＋ `origin_node_id` ＋ `published` ＋ `paused_state_ref` |
| ③ 过期索引 | ①②的结果 | 一次 `ZADD`，不走 healer（healer 是稳态修复，不是冷启动） |
| ④ pending / transition / lock | **不重建** | 它们描述的是**在途操作**，而在途操作的那个副本已经跟着 Redis 一起没了 |

**四条硬规则：**

1. 🔴 **重建是 `SET NX`，不是 `SET`。** 重建期间可能有别的副本已经在写新沙箱了。
2. 🔴 **重建期间 `api` 拒绝写操作**（create / pause / resume / delete ⇒ 503 ＋ `Retry-After`），
   直到重建完成。理由：重建是一次全量对账，与并发写交叉会产生「谁覆盖谁」的问题，
   而这个路径按定义是罕见的 —— 让它慢且正确。
3. 🔴 **重建**不**从 node 推断 `expires_at`。** `control_plane_config` 里带的是原值。
   如果那个 blob 缺失或解不开，这条沙箱**按 `timeout = None` 重建并打告警**，
   而不是给它一个编出来的过期时间。
4. 🔴 **重建之后立刻跑一轮 healer ＋ 一轮 reaper**，把索引补齐。

**必须验证**（§14 R7）。**「设计了但没跑过」的重建路径与没有重建路径是同一件东西** ——
这正是 `_sd-recon-env.md` §8 第 2 条说的「某指标恒 0 本身不是证据，必须先把它顶起来一次」。

---

## 14. 验证探针 —— 每条自带控制面

> 🔧 **2026-08-21：阶段 2 又顶出五条验证手法上的教训，本节八组探针要逐条继承 —— 见 §17.4。**
> 其中 ③ 直接改写 R7 与 R8 的判据，⑤ 直接改写 R4 与 R5。

`_sd-recon-env.md` §8 第 1 条逐字：**每发探针必须自带对照面 —— 一个必然为假的输入，
证明这条探针有分辨力。** 上一轮翻过车：grace 期「拒绝接管」的第一发用了一行任何相位都
认领不了的合成行，`409` 看着像被拒，实则毫无分辨力。

### R1 —— 跨副本可见（父提案判据 1）

| | |
|---|---|
| 做 | `api` 副本数 2。向副本 A 建一个沙箱，从副本 B `GET /sandboxes/{id}` |
| 断言 | B 返回 200 且 `execution_id` 与 A 返回的一致 |
| 🔴 对照面 | **同一发，但把 B 的 `agentenv:api:` 前缀改掉**（指向一个空键空间）⇒ 必须 404。<br>排除「B 其实是通过反向代理转给 A 了」这种更弱的实现 |

### R2 —— 写入方死掉之后仍可 pause/resume（父提案判据 2）

| | |
|---|---|
| 做 | 副本 A 建沙箱 ⇒ `kubectl delete pod` 杀 A ⇒ 从 B `POST /sandboxes/{id}/pause` 再 `/resume` |
| 断言 | 两次都 200；`execution_id` 在 resume 后变了 |
| 🔴 对照面 | **先不杀 A，从 B 做同样的两次操作** ⇒ 也必须 200。<br>排除「B 只是在 A 死后接管了某种本地状态」——如果只有 A 死了才能从 B 操作，说明有一条没被发现的亲和 |

### R3 —— 🔴 相反操作产生一次成功、一次明确冲突（父提案判据 3，本阶段真正的风险）

| | |
|---|---|
| 做 | 同一个 Running 沙箱，副本 A 发 `POST /pause`、副本 B 发 `DELETE`，**同一毫秒发出**（客户端两个线程 ＋ 一个 barrier） |
| 断言 | 恰好一个 2xx，另一个 **409**，且 409 的 body 里带 `refusal_code`。**沙箱最终处在一个确定状态**（要么 Paused 要么不存在），不是两者的叠加 |
| 断言 2 | node 侧 `ListSandboxes` 与 store 的记录一致（不能出现「store 说没了、node 上还在跑」） |
| 🔴 **对照面** | **把分布式锁关掉重跑**（`orchestrator.store_distributed_lock_enabled = false`，一个只在测试与探针里用的开关）⇒ **必须出现双执行** |
| 🔴 **对照面的对照面** | 见下 |

🔴 **这里有一个陷阱，必须现在就说清楚，否则这条探针会没有分辨力。**

§5.5 的设计里，**正确性不靠锁，靠脚本内的 `rev` ＋ `execution_id` 双谓词**。
⇒ **把锁关掉，双执行也不会发生** —— 只会看到 `ConcurrentUpdate` 变多。
⇒ **「关掉锁必须出现双执行」这个判据，在我们的设计下是假的。**

**父提案的对照组设计是按「锁承重」写的，而我们的设计里锁不承重。**
照它的字面做，探针会得出「关了锁也没双执行 ⇒ 探针没分辨力 ⇒ 判据不通过」的结论，
而实际情况是**设计比判据设想的更强**。

⇒ **对照面改成两级，两级都要跑：**

| 级 | 关掉什么 | 预期 | 证明了什么 |
|---|---|---|---|
| **C1** | 分布式锁（`store_distributed_lock_enabled = false`） | 🔴 **不出现双执行**，但 `agentenv_store_concurrent_update_total` **必须 &gt; 0** | 锁确实在被使用（否则关它不会有任何变化），而且它不是正确性的最后一道 |
| **C2** | 脚本内的双谓词（`store_cas_predicates_enabled = false`，🔴 **只允许在测试构建里存在**，用 `#[cfg(any(test, feature = "unsafe-probe"))]` 门住） | 🔴 **必须出现双执行**：pause 和 delete 都报成功，node 上留下一个已经被 stop 但记录说 Paused 的沙箱，或者反过来 | 这才是真正的承重点 |

**C1 的「必须 &gt; 0」是 `_sd-recon-env.md` §8 第 2 条的直接应用**：
「某指标恒 0 本身不是证据 —— 必须先把它顶起来一次」。
关掉锁而 `concurrent_update` 仍然是 0，说明这一发根本没制造出竞争，探针本身没跑对。

🔴 **C2 的开关必须编译期门住，绝不能是一个运行时配置项。**
一个能在生产里关掉承重 CAS 的开关，本身就是这次重构要消灭的那类东西。

### R4 —— 闭包执行时限有牙

| | |
|---|---|
| 做 | 测试专用的 store 包装，往 `update_if_state` 的闭包里塞一个 200ms 的 `std::thread::sleep`（`closure_budget = 50ms`） |
| 断言 | 返回 `ClosureBudgetExceeded`，**且 Redis 里的记录未被修改**（`rev` 不变） |
| 🔴 对照面 | 同一发，sleep 改成 1ms ⇒ 必须成功且 `rev + 1`。<br>排除「这个断言其实是在测别的失败路径」 |

### R5 —— KEEPTTL 没有被抹掉

| | |
|---|---|
| 做 | 建一个 `max_sandbox_lifetime_secs = 3600` 的沙箱 ⇒ `PTTL` 记下 T0 ⇒ 做 20 次 `keep_alive` ⇒ 再 `PTTL` |
| 断言 | T1 ≈ T0 − 经过时间（**单调下降**），且 **T1 &gt; 0**（不是 −1「永不过期」） |
| 🔴 对照面 | 把 `update_if_state.lua` 里的 `'KEEPTTL'` 改成不带选项重跑 ⇒ `PTTL` 必须变成 **−1**。<br>这是唯一能抓到 §4.4 第 3 条的探针，形态照抄 `_sd-impl-phase1.md` §9 的 P5 |

### R6 —— 过期索引 member 按 execution 作用域

| | |
|---|---|
| 做 | 沙箱 X 化身 E1 ⇒ pause ⇒ resume 得到化身 E2 ⇒ 手工 `ZREM {global}:expiry "X:E1"`（模拟对死化身的清理）⇒ 等待 X 的 timeout 过去 |
| 断言 | X **仍然被驱逐**（`E2` 的 member 还在） |
| 🔴 对照面 | 把 member 改成只有 sandbox_id 的形状重跑 ⇒ X **永不过期**。<br>这条对照面证明的正是 §6.3 末尾那句「排期时别只对着前五行」 |

### R7 —— 🔴 Redis 全丢之后重建（§13.3）

| | |
|---|---|
| 做 | 集群里跑 3 个沙箱（2 Running 1 Paused）⇒ `FLUSHALL`（或删掉 Redis Pod ＋ PVC）⇒ 等 `api` 重建 |
| 断言 | ① 3 个沙箱全部回到 `GET /sandboxes`；② 2 个 Running 的仍然能被反代访问；③ 那个 Paused 的能 resume；④ **重建期间的写请求返回 503 而不是 500 或静默成功** |
| 🔴 对照面 1 | **在重建完成之前**发一个 `POST /sandboxes` ⇒ 必须 503。排除「重建其实没有拦住写」 |
| 🔴 对照面 2 | 把某一台 node 的 `control_plane_config` 置空重跑 ⇒ 那台上的沙箱**必须**被判成不属于控制面（不出现在列表里），**而不是被杀掉**。这一发同时验证 [S] §3.4 的所有权标记 |
| 射程边界 | 🔴 这条**不证明**「重建出来的 metadata 与丢失前逐字段相同」。要证明那个，需要在 FLUSHALL 之前 dump 一份并逐字段对比 —— **这一步要做，写进探针脚本**，否则只证明了「有三条记录」而不是「是原来那三条」 |

### R8 —— 卡住的转换会被 reap（§6.7 的那个洞）

| | |
|---|---|
| 做 | 建一个 **`timeout = None`** 的沙箱 ⇒ 从副本 A 发 pause ⇒ 在 `start_transition` 之后、完成回调之前 `SIGKILL` A ⇒ 等 `transition_key_ttl` ＋ reaper 周期 |
| 断言 | 沙箱最终离开 `Pausing`，并且**可以被 delete**（不再 409） |
| 🔴 对照面 | 把 reaper 关掉重跑 ⇒ 沙箱**永远卡在 `Pausing`**，delete 稳定 409。<br>这条对照面是本文 §16.3 那条更正的证据：不加第四件套，e2b 的三件套在我们这里兜不住 |

### 探针的射程边界（照实写）

- R1–R3 证明的是**控制面**的互斥，**不证明** node 侧的执行是幂等的。
  一次被 fence 掉的 pause，node 上有没有已经动过 VM，本组探针看不见。
- R7 的重建路径在 dev 集群上跑，**用的是 2 个 node**。
  「N 个 node 部分失联时的重建」没有覆盖，显式登记。
- 🔴 **`_sd-recon-env.md` §8 第 3 条（两侧互证）**：R3 的 409 计数必须与
  store 侧 `agentenv_store_execution_superseded_total` / `..._concurrent_update_total`
  的增量**逐条对得上**。对不上就是有一侧算错了。

---

## 15. 任务顺序与规模

### 15.1 依赖序

```
R0 crates/test-support 的 RedisFixture ＋ Makefile 目标      ← 🔴 第一个做，与一切无关
 │
R1 StoredSandboxRecord ＋ PausedStateRef ＋ 记录 schema（§2.1 §4.3）
 │   └─ 🔴 出口条件：把「control_plane_config = metadata 本体」这条更正传给 [S]
 │
R2 键布局 ＋ 连接管理 ＋ 脚本装载（§4）
 ├─ R3 CRUD ＋ get_many ＋ list*（§3.1 §9）
 │    └─ R4 L1 契约套（§11.2）                              ← 这里就能开始双后端跑
 ├─ R5 分布式锁 ＋ notify pub/sub（§5.2 §3.6）
 │    └─ R6 update_if_state ＋ update ＋ update_state_if_state（§5）
 │         └─ R7 wait_while_in_states（§6.6）
 ├─ R8 过期 ZSET ＋ expired_batch ＋ healer（§7.1 §7.2 §7.4）
 │    └─ R9 🔴 驱逐的过期重校验（§7.3）—— **可以先于一切在 in-memory 上修**
 ├─ R10 transition 三件套 ＋ AllowedTransitions 表（§6.1–§6.5）
 │    └─ R11 transition 索引 ＋ reaper（§6.7）
 ├─ R12 Reserve 三态 ＋ wait_for_start（§8）
 └─ R13 重建路径（§13.3）                                    ← 依赖 [S] 的 ListSandboxes
      └─ R14 探针脚本（§14）                                 ← 收口
```

### 15.2 哪些能在 [S] 就绪之前做

| 任务 | 能否先行 | 理由 |
|---|---|---|
| R0 测试基建 | ✅ **应该第一个做** | 与一切无关，且后面每一条都要用它 |
| 🔴 **R9 驱逐重校验** | ✅ **应该现在就做，独立提交** | §16.9：这是**今天就存在的 bug**，与 Redis 无关。在 in-memory 上修掉，等 Redis 来了自然继承。**不要打包进这一批**——一个可以独立验证的修复不该躲在一次大重构后面 |
| R1 记录 schema | ✅ | 纯类型 ＋ serde，`--role all` 下没人构造它 |
| R2–R8、R10–R12 | ✅ | `RedisMetadataStore` 是一个没人实例化的新类型。🔴 加一个 `#[allow(dead_code)]` 还是加一个只在测试里用的构造入口？**选后者**：L1/L2/L3 从第一天就在跑它 |
| R7 `wait_while_in_states` | ✅ | 同上 |
| R13 重建 | ❌ | 依赖 [S] 的 `ListSandboxes` 与 `control_plane_config` |
| R14 探针 | ❌ | 依赖 `--role api` 能起两个副本 |
| `--role api` 的装配 | ❌ | 那是 [S] 的 T9 |

⇒ **本半有十一个任务（R0–R12 除 R13/R14）可以在 [S] 完全没动之前做完并合入**，
且它们在 `--role all` 下**全部是零行为变化**（唯一例外是 R9，它是一个行为**修正**）。

### 15.3 规模估算（含测试）

| 模块 | 性质 | LOC |
|---|---|---|
| `crates/test-support/src/redis.rs` ＋ `Cargo.toml` feature | 🆕 | 90 |
| `src/orchestrator/store/redis/mod.rs`（连接、配置、装配、备忘） | 🆕 | 420 |
| `src/orchestrator/store/redis/record.rs`（`StoredSandboxRecord` / `PausedStateRef` / 版本化） | 🆕 | 340 |
| `src/orchestrator/store/redis/keys.rs`（九个键 ＋ member 编解码 ＋ 校验） | 🆕 | 200 |
| `src/orchestrator/store/redis/scripts.rs`（六条 Lua ＋ `OnceLock<Script>` ＋ 逐条注释） | 🆕 | 480 |
| `src/orchestrator/store/redis/lock.rs`（获取 / 退避 / token 释放） | 🆕 | 260 |
| `src/orchestrator/store/redis/notify.rs`（pub/sub 连接 ＋ `SubscriptionManager` ＋ publisher 队列） | 🆕 | 400 |
| `src/orchestrator/store/redis/crud.rs`（`MetadataStore` 十二方法实现） | 🆕 | 620 |
| `src/orchestrator/store/redis/transition.rs`（三件套 ＋ `TransitionGuard` ＋ 三分支） | 🆕 | 520 |
| `src/orchestrator/store/redis/expiry.rs`（`expired_batch` / healer / reaper） | 🆕 | 480 |
| `src/orchestrator/store/redis/reserve.rs`（三态 ＋ `wait_for_start`） | 🆕 | 320 |
| `src/orchestrator/store/transitions.rs`（`AllowedTransitions` 表 ＋ 逐格测试） | 🆕 | 260 |
| `src/orchestrator/store/mod.rs`：六个新方法 ＋ default impl ＋ 新错误变体 | 改 | +190 |
| `src/orchestrator/store/in_memory.rs`：`expired_batch` ＋ 五个退化实现 | 改 | +90 |
| `src/orchestrator/store/contract.rs`（L1 契约套 ＋ 宏） | 🆕 | 700 |
| `tests/redis_store.rs`（L2） | 🆕 | 1,100 |
| `tests/redis_store_multi.rs`（L3 跨副本） | 🆕 | 480 |
| `src/orchestrator/service.rs`：驱逐走 `start_transition` ＋ pause 收尾走回调 ＋ reserve 接线 ＋ healer/reaper 任务 | 改 | +320 / −120 |
| `src/orchestrator/store/metadata.rs`：`origin_node_id` / `published` / `expires_at_ms` 冗余 | 改 | +80 |
| `src/cfg.rs` ＋ `config/default.toml`：`[orchestrator.store]` 十三个新值 | 改 | +160 |
| `deploy/k8s/base/redis.yaml`：两处理由注释改写（§13.1） | 改 | +25 / −10 |
| `Makefile` / `adev` CI：`test-with-redis` | 改 | +40 |
| `CLAUDE.md`：测试命令一节 | 改 | +20 |
| **合计** | | **≈ 7,600 行**（新增 ≈ 7,450，删除 ≈ 130） |

**对照**：与 [S] 的 ≈ 6,700 行合起来，阶段 3 是 **≈ 14,300 行新增**。
父提案承诺的 −13,640 行在**下一个 release** 的删除批里。
🔴 **不要把 −13,640 写进本半或任何一半的验收判据**（[S] §13.3 已经论证过一次，
这里是它的第三个理由：其中 `services/scheduler/internal/registry` 的
pin/prefer 语义要**搬**不要删，[S] §14.8 末尾）。

**诚实对照**：e2b 的 Redis 沙箱 store 非测试约 2,100 行，我们的非测试部分约 4,300 行。
差距主要来自三处：① 六条 Lua 我们写得更严（`rev` 双谓词、脚本内在途检查、脚本内过期重校验）；
② 多一个 transition 索引 ＋ reaper（§6.7 那个 e2b 没有的洞）；
③ Rust 的错误类型与 `Guard` 语义比 Go 的 `func(ctx, error)` 冗长。
**①②是买来的正确性，③是语言税。**

---

## 16. 🔴 对抗性结论：我认为父提案与同伴文档写错的地方

> 🔧 **2026-08-21 复核：下面九条经本轮复核全部仍然成立**（§16.2 的「两侧都已实现」重新核过，
> 只是行号漂了）。**本文自己写错或需要补的，另立在 §17.5。**

### 16.1 §8.1「`redis.KeepTTL` 的等价物 —— 写回不能抹掉沙箱寿命 TTL」—— 误读

`operations.go:203` 确实写着 `redis.KeepTTL`。但沙箱记录键**从来没有被设过 TTL**：
`addSandboxScript`（`scripts.go:12-15`）是 `SET` ＋ `SADD`，无 `EX`；
`storage/redis/` 全目录对沙箱键 `grep Expire` 零命中。
⇒ **e2b 的 `KeepTTL` 保的是一个不存在的 TTL**，是防御性写法，不是「沙箱寿命 TTL」。

e2b 的寿命住在两个地方：JSON 里的 `EndTime`，和过期 ZSET 的 score。

**影响**：如果按 §8.1 的字面去找「e2b 是怎么给记录设寿命 TTL 的」，会找不到，
然后可能得出「那就不设 TTL」的结论 —— 而我们**应该**设（§4.4 给了三条与 e2b 不同的理由）。
⇒ **结论对（要 KeepTTL），理由错（不是因为 e2b 有）。** 理由错会导致数值定错：
如果以为是抄 e2b，就不会去想 `record_ttl_grace` 该多大，而 §4.4 第 2 条论证了它必须
显著大于最长转换，否则记录先死、VM 变孤儿、被杀。

### 16.2 §8 陷阱 7「这条在 PG 版本里就是遗留项」—— 事实错误

`Rows.Covered` 在 `services/scheduler/internal/registry/store.go:50-63` `:195-198`
定义并注释，在 `store_postgres.go:395` 返回，在 `registry_service.go:231` 上线；
Rust 侧 `src/orchestrator/paused_registry/central.rs:305` 的 `require_full_coverage`
在消费它，注释逐字论证了为什么。**两侧都实现了。**

**影响**：把它当遗留项，会有人去「修」阶段 3 删除批里的 Go 代码，
而 [S] §11.3 论证过那部分是回退的前提。⇒ 本半要做的是**搬**（§9.2），不是修。
🔴 顺带：陷阱 7 漏了一条真正该抄的东西 —— `main.go:191-200` 的
「pipeline 出错 ⇒ 整轮跳过以避免 mass kill」，那才是这一族里最能防灾的一行。

### 16.3 §8.2「上 Redis 之后状态会永远卡住」—— 只在漏抄的情况下成立，而我们有个更狠的洞

e2b 的崩溃恢复是**三件事的合力**，§8.2 只写了第一件：
transition key TTL（让路）＋ `AllowedTransitions` 允许从过渡态出发 ＋
🔴 **`items.go:149-159` 的 stale-cutoff 分支**（把卡住的记录送进驱逐清单）。
第三件没有任何一份文档引过，而它才是真正的出口。

🔴 **而第三件对我们有洞**：`expires_at: Option<SystemTime>`，`None` 时
`index_expiry` 直接跳过（`in_memory.rs:81-85`）⇒ 一个 `timeout = None` 的沙箱
卡在 `Pausing` 之后**根本不在过期 ZSET 里**，永远没人清，用户看到的是「这个沙箱删不掉」。
e2b 没这个洞，因为它的每个沙箱都有 `EndTime`。

⇒ §6.7 的第四件套（transition 索引 ＋ reaper）不是加固，是补一个 e2b 兜不住的洞。
探针 R8 就是为它写的。

### 16.4 §8.4 / 阶段 3 第 2 条对 `Reserve` 的价值论证 —— 在我们的 API 形状下没有触发点

「`alreadyPending` 那一支是关键：并发创建的第二个调用方等第一个的结果，而不是收到 409。
这是『客户端重试』与『用户真的建了两次』的分水岭。」

**我们的 `NewSandbox` schema 里没有 `sandboxID`**（`src/api/openapi.yml:540-580`），
id 在 `service.rs:368` 由服务端 `Uuid::now_v7()` 现铸。
⇒ 客户端重试得到的是**一个新沙箱**，不是一次冲突。这条分水岭在 create 路径上不存在。

**影响**：按错误的理由做，会把 `Reserve` 的重点放在 create 的去重上，
然后很自然地得出「id 是我们铸的，那这个原语可以不做」的结论 ——
而真正需要它的两处（`restore_sandbox` 的并发，以及**创建中窗口的集群可见性**）
就一起丢了。**第二处比第一处更危险**：它决定了孤儿判定会不会去杀正在冷启动的 VM（§10.3）。
⇒ 三态照做，理由换成 §8.1 的两条。

### 16.5 D9 表格「靠锁的 `lockTimeout`」做崩溃恢复 —— 与 §8.2 矛盾，且 §8.2 是对的

模块文档 D9 的「那还缺什么」表，第三行「崩溃恢复」的落点写「同上，**靠锁的 `lockTimeout`**」。
§8.2 后来（读实现之后）纠正成 transition key。**两处并存，且 D9 那一行没有被划掉。**

代码证据站在 §8.2 这边：`state_change.go:110-115` 在进入等待**之前**显式 `releaseFunc()`，
锁根本不跨越整个操作。⇒ **D9 那一行要划掉**，否则会有人按「把 `lockTimeout` 调大就行」去做，
而那只会让一个死副本堵住某个沙箱更久，不会让它自愈。

### 16.6 🔴 e2b 的 `Update` 自己就有 `scripts.go:33-40` 描述的那个洞

`scripts.go:33-40` 的论证逐字：Go 侧比较与写之间，一个 lockless 的 `Add` 能装进新化身，
所以 enforcement 必须在 Lua 里。**这条论证对 `Update` 同样成立**，
而 `operations.go:203` 是 `s.redisClient.Set(ctx, key, newData, redis.KeepTTL)` —— **裸 SET，无谓词**。

它靠锁挡（`operations.go:163`），但 `Add` 不取锁（`operations.go:21-56` 全程无锁）。
⇒ **一次 `Update` 与一次 `Add` 的竞争，正是那段注释描述的场景，而 `Update` 这条路上没有防护。**

**影响**：§8.1 让我们「逐字保留」这个形状，照做就是把 e2b 的洞抄过来。
而我们的形状**更危险**：`restore_sandbox`（`service.rs:387`）走的正是 `add`，
而 pause 收尾的 `store.update()`（`service.rs:1541`）跨着一次几百毫秒的落盘 I/O（§2.3）。
⇒ §5.3 的 `rev` ＋ `execution_id` 双谓词，以及 §5.7 给 `update()` 加谓词，是必须的。

### 16.7 §8.2「终态转换则顺手把 `EndTime` 拨到现在」—— 对我们是有害的

e2b 的 `TransitionExpires`（`state_change.go:134-139`）把 `EndTime` 拨到 now，
因为它的「移除」类转换等于生命结束。

**我们的 `Pausing → Paused` 不是生命结束**：`Paused` 沙箱可以 resume，
而 `expires_at` 仍然驱动 `timeout_action`。照抄的话，每个刚暂停的沙箱立刻进入可驱逐区间，
`timeout_action = Delete` 的那些会**被删掉**。⇒ §6.5 的表里这一支明确不抄。

### 16.8 🔴 `SandboxMetadata` 不可序列化 —— 三份文档都没提

`paused_state: Option<Arc<dyn PausedSandboxState>>`，`#[serde(skip)]`（`metadata.rs:119-122`），
而 `resume_sandbox` 逐字依赖它（`service.rs:1699-1702`）。
「换个 store 后端」在第一次 `serde_json::to_vec` 就把它丢了，
表现是**每一次 resume 都 500**，且只有在真跑 resume 时才暴露。

解法在仓里现成（`file_backed.rs:419` 的 `encode()` ＋ `:66-73` 的 `decode_paused_state`），
但**必须在 R1（记录 schema）就落**，不能等到 R6。
⇒ 顺带推出一条给 [S] 的更正：`artifact_root` 是节点本地路径，
所以记录必须带 `origin_node_id` —— 这与 [S] §6.6 第 2 条从完全不同的方向推出了同一个结论。
**两条独立推理指向同一个字段，是这个字段确实必要的一个不错的证据。**

### 16.9 🔴 驱逐不在锁内重校验过期 —— 这是**今天就有**的 bug

`service.rs:2201-2237` ⇒ `list_expired` ⇒ `pause_sandbox_inner` ⇒
`update_state_if_state(Pausing, [Running])`（`:1341`）。
那是状态 CAS，**不是过期 CAS**。中间到达的一次 `keep_alive`（`:967`，只要求 `Running`，会成功）
把 `expires_at` 往后推，驱逐照样把沙箱暂停了。

**它不是 Redis 引入的**，Redis 只是把窗口从「两次 `await`」拉宽到「两次网络往返」。
⇒ §15.2 建议 **R9 独立先行**：在 in-memory 上修掉并独立验证，
不要让一个可以单独证明的修复躲在一次大重构后面。

### 16.10 顺带：三处行号漂移

| 文档写的 | 实际 | 原因 |
|---|---|---|
| `store/mod.rs:61` `:73` `:84` `:96` | `:63` `:75` `:86` `:98` | 阶段 1 的 `configured_max_sandbox_lifetime` / `NewTimeout` 导出使文件上移 2 行 |
| `service.rs:2159`（evictor） | `:2201`（`evict_expired_sandboxes`）／`:2207`（`list_expired`） | 本会话的并行改动 |
| `main.go:205-217`（e2b 孤儿检查） | `:205-209` | 引多了 8 行，实际的判定只有 5 行 |

行号本身不重要，但**三份文档互相引用行号**，而这些文件正在被三个 agent 同时改。
⇒ 🔴 **引用时带上符号名**（函数名 / 常量名），不要只带行号。本文全部这么做了。

---

## 17. 🔧 阶段 2 落地之后回填的事实与订正（2026-08-21）

> 本文写在阶段 2 开工之前。2a（目录 schema ＋ 服务）、2b（trait 拆分、`stage` / `commit_staged`、双写）、
> 2c（`read = postgres`，今天在 dev 集群上跑着）在验收与 QA 里挖出的东西**改变了本文的若干前提**。
> 本节回填，**不重写前面**。与 [S] §15 是同一批回填的两半，交叉引用按名给出。

### 17.1 🔴 读作用域是**面**的属性 —— 本半有三处会踩同一个坑

**2c 的事故。** 中心目录 `SnapshotCatalog` 的**整个读面**被钉死成
`CatalogReadScope::Resolvable` ⇒ `status_group = 'ready'`
（`src/snapshot/repository/backends/central/mod.rs:942` `:947` `:973`；
SQL 侧 `services/scheduler/internal/catalog/queries_resolved.go:26` 的 `readyPredicate`，
用在 `:116` 与 `:236`）。**对快照这是对的**（挡住字节还在上传的快照被拿去开 VM），
套到别的面上炸了两次：模板从创建到首次构建提交为止一直是 `waiting`，**永远到不了 `ready`** ⇒
整个模板面 404；而更贵的一次是**跨节点 resume 读不到快照就删掉登记行** ——
在 resolvable 作用域下，「读不到」把**真的没了**和**行在、只是还没翻成 ready** 混成了同一个答案。

🔴 **规矩：读作用域是「面」的属性，不是后端的、也不是客户端的。**
一个装在后端上的默认谓词，会让每一个问别的问题的调用点悄悄拿到这个问题的答案。

**本半的三处落点，逐条对号：**

| 落点 | 这个面在问什么 | 🔴 不许拿别的面的谓词回答 |
|---|---|---|
| `list_filtered`（§3.1 第 9 行）／`list*` | 「符合这个过滤器的记录有哪些」 | **过滤在 Rust 侧做**（§4.5 已定），所以这里天然不会把某个状态谓词烧进 store。🔴 **这条要写进 `crud.rs` 的注释当理由**，否则将来「给每个 state 建一个 SET」的诱惑回来时，只剩性能这一条反对理由，而真正的理由是这一条 |
| `expired_batch`（§7.2）第 ⑦ 步 | 「这条记录该不该被驱逐」 | 它已经**分了三态**：未过期 ⇒ 重打分；过渡态且在 `stale_cutoff` 内 ⇒ **跳过**（不是「不该驱逐」）；超过 ⇒ 入结果。这正是本条规矩的正面样本，**保持** |
| 🔴 `get_many`（§9）→ 孤儿判定（§10.3） | 「node 报的这个 `(sandbox_id, execution_id)` 该不该被杀」 | §10.3 的四行判定表已经把「记录不存在」拆成了**pending / 不在 pending** 两支。🔴 **这就是本条规矩在本半唯一真正承重的地方** —— 它的否定答案后面挂着 `KillOrphan`。见 §17.2 |

### 17.2 🔴 「不存在」这个答案需要三态 —— 现成的样板就在仓里

**2c 的最终修法（`dbd6fa9`）不是把作用域调宽，是让那条销毁分支根本不问「读」。**
理由逐字：**两边都没有行的时候，任何读都分不清「一次还没落地的写」与「一个从来不存在的快照」。**

样板（`SnapshotCatalog::absence_of`，`src/snapshot/repository/interfaces.rs:719`；
语义类型 `SnapshotAbsence` 在 `:499-517`）：

```
absence_of(id) -> Settled                 // 不存在，而且这是定论
              -> Unsettled { because }    // 有东西反驳了它：队列里欠着一笔写，或另一个 store 持有
              -> Err(..)                  // 🔴 够不着的 store 以错误传播，绝不表现为「不存在」
```

🔴 **第三条是本半最该抄的那条**，trait 注释逐字：`An error is never an absence`。
它与 §9.2 第 3 条（`get_many` 报错 ⇒ 整轮跳过，抄 e2b `main.go:191-200` 的「skip entirely to avoid mass kills」）
**是同一条规矩** —— 本文已经写对了，这里给它一个更硬的名字和一个仓内先例。

**⇒ §10.3 的判定表补一行，并把整表重述成三态：**

```
node 报的 (sandbox_id, execution_id)：
  ① get_many 报错 / 覆盖集不全 / 任一块 MGET 失败   ⇒ 🔴 「我不知道」——整轮放弃，不判任何东西
  ② 记录不存在，但在 pending ZSET 里                ⇒ 「还没到」——不是孤儿（创建中，§8.1 ②）
  ③ 记录不存在，且不在 pending                      ⇒ 「从来没有」——孤儿
  ④ 记录存在但 execution_id 不同                    ⇒ 孤儿（旧化身）
  ⑤ 记录存在且 execution_id 相同                    ⇒ 不是孤儿
```

🔴 **①②③ 三支必须各自可观测**（三个不同的计数器 ／ 三条不同的日志），
否则「这一轮什么都没杀」在三种完全不同的原因下读起来一模一样 —— 正是 §17.4 ③ 那条教训。

**同样的形状还欠 [S] 一条**：`api` 对账读的是 node 的 `ListSandboxes`，
**一台 gRPC 调不通的 node 不等于这台 node 上没有沙箱**。已在 [S] §15.2 登记，两边是同一条。

### 17.3 🔴 惰性代码不是正确的代码 —— 本半整整十一个任务都是「先落地、后接线」

**2a 的实例，值得逐条看，因为本半的形状与它一模一样。**
2a 提前交付了 `StartBuild` / `RenewBuildLease` / `ReapExpiredBuilds`，
一道 QA 门**正确地**判定它们无害 —— 理由是**没有任何东西在调它们**。
2c 把驱动接上之后，这三样里有**三个真缺陷**，每一个都以别的样子出现：

| 缺陷 | 症状 | 现在的落点（以及为什么值得本半抄） |
|---|---|---|
| 心跳由**节点**盖戳、拿去和 **reaper 的时钟**比 | 一台走慢的节点，它跑的每一个构建都在半途被杀，错误说的是「心跳失效」—— 一件从没发生过的事 | 两端都取数据库的时钟，而且**两个输入类型根本没有地方放第二个时钟**（`services/scheduler/internal/catalog/store.go:364-378` `:380-392`；`ReapInput` 收的是**时长**不是时刻）。🔴 **本半有同形的三处**：`expired_batch(now, ..)`、`reap_stuck_transitions(now)`、`reserve` 的 `stale_cutoff` —— 它们的 `now` 今天由**调用方**给。单实例 Redis 上这不是跨机器比时钟，但一旦 `api` 有 N 个副本，就**是**了。⇒ 🔴 **要么把 `now` 挪进 Lua（`redis.call('TIME')`），要么在注释里写明「为什么这里可以由调用方给」** |
| **成功不把构建移出队列** | 每个**成功**的构建继续占着名额；攒够天花板那么多次成功之后，全集群拒绝一切新构建，而当时**没有任何东西在构建** | 🔴 **本半有两个同形的队列**：`agentenv:api:pending`（§8）与 `agentenv:api:txn:index`（§6.7）。§8.3 末行已经要求 `add` 在同一条 Lua 里 `ZREM` pending，§6.3 第 3 步要求完成回调与 `ZREM` 同一条 Lua ——**这两条不是整洁，是这条缺陷的正面修法**，注释里要这么写 |
| reaper **没有预热** | 一次滚动让所有心跳同时看起来陈旧（不是因为构建停了，是因为没人在听），第一轮把它们全杀了 | 🔴 **本半的 reaper（§6.7）与 healer（§7.4）都没有预热期。** healer 有 `heal_grace`（按 `created_at` 跳过刚起的沙箱），但那是**按记录**的，不是**按进程**的。⇒ **两个后台任务都要加一个「本进程启动后先按住一个周期」的预热**，且闸门（kill switch）关了再开要**重新计时** |

外加 `69c131d`：`start_build` 把同一个 id 同时当 `build_id` 和 `template_id` 发出去，
而 `builds.id` 是主键 ⇒ **一个模板一辈子只能构建一次**；
🔴 **而如果它没有撞上**，两个共用 id 的构建会**互相续对方的租约**。
⇒ **本半的直接对照**：§6.7 的 transition 索引 member 是 `<sandbox_id>:<execution_id>:<txn_id>` 三段，
§7.1 的过期索引 member 是 `<sandbox_id>:<execution_id>` 两段 ——
**「两个不同的东西共用一个 id」正是这两处三段／两段 member 要消灭的形态**，
本文已经写对了两次，这里给它第三个理由。

**⇒ 规矩，以及本半的接线闸：**

> 🔴 **一样东西「还没有人调它」，只证明它现在无害，不证明它是对的。**
> 提前落地的代码在**接上驱动之前**要过一次审计，而不是接的时候翻一个开关就算数。

§15.2 说本半有**十一个任务（R0–R12 除 R13/R14）可以在 [S] 完全没动之前做完并合入**，
而它们全部落在一个**没人实例化的新类型**上。⇒ **在 `--role api` 第一次装配 `RedisMetadataStore` 之前，
必须先跑完下面这份审计清单**，它不是「再跑一遍测试」：

| # | 审什么 | 为什么单元测试不够 |
|---|---|---|
| 1 | 🔴 六条 Lua 的**每一个 return 分支**都被至少一发探针执行过 | L2 里容易只覆盖 happy path；`execution_superseded` / `not_expired` / `in_flight` 三支是**只在竞争下发生**的 |
| 2 | 🔴 §5.7 给 `update()` 加的 execution 谓词，对**八个现有调用点**逐个过一遍 | 本文说它「零行为变化」——这句话要被验证，不是被声明 |
| 3 | 🔴 §6.4 那张八态转换表**逐格**跑一遍（表里已经要求一个逐格测试） | `Killing → previous_state` 有两个出口，`:2587` 的 `expected_state` 还是个变量 |
| 4 | reaper／healer 的预热与 kill switch（§17.3 第三行） | 它们只在**重启之后的第一轮**才会出事 |
| 5 | 🔴 §13.3 的重建路径**真的跑一次** | 本文自己写了：「设计了但没跑过的重建路径与没有重建路径是同一件东西」 |

🔴 **一条活样本，说明这不是杞人忧天**：`guard_read_side` 的三条拒绝分支
**除了单元测试之外从未执行过**；`(both, postgres)` 那一支里 `diverged > 0` 这个析取项
**至今仍未执行过**。一条从没跑过的拒绝分支，和一条不存在的拒绝分支，在事故当天是同一个东西。

### 17.4 🔧 验证手法：§14 要继承的五条

`_sd-recon-env.md` §8 的四条仍然全部适用。阶段 2 又加了五条：

**① 跳过的测试报成 `ok`，而且是整批。**
`make -C services test` 曾经**静默跳过 152 个测试**并报绿，
其中包括**唯一两个**抓到 `KEEPTTL` 被改回去的测试 —— 🔴 **和本半 R5 要抓的是同一件事**。
`-count=1` 现在写在 `services/gateway/Makefile:19` 与 `services/scheduler/Makefile:19`
（顶层 `services/Makefile:29-32` 靠委派继承它；`report-skipped-suites`（`:46`）在输出末尾
把**这一轮跳过了什么**说出来）。
🔴 **本半 §11.3 的 `make test-with-redis` 要照抄这三件，一件不少**：
`-count=1`（Rust 侧对应物是**不要**让测试在缺依赖时提前返回）、
`AENV_REDIS_TEST_REQUIRED=1` 把跳过变成失败、以及**一条把「这轮跳过了什么」说出来的收尾输出**。

**② 一个 1,100 行的套件可以从来没被 CI 跑过。**
`tests/snapshot_catalog.rs`（发现时 1,161 行，现在 2,673 行）是**唯一**演练双写的地方，
而它**不在任何一个 CI workflow 里**，且没有环境变量时**提前返回并打印 `ok`**。
现在挂在 `.github/workflows/integration-tests.yml:52-66`（该 job 的注释逐字写着
「This job is the only thing that runs `tests/snapshot_catalog.rs`」），
入口是 `make test-snapshot-catalog`（`Makefile:157`），它自己起 PG ＋ scheduler 再拆掉。
🔴 **本半的 L2／L3（`tests/redis_store.rs`、`tests/redis_store_multi.rs`）要在同一批里进 CI，
而不是「等它稳定了再进」** —— §11.3 已经写了要加 job，这里把它升格为**与代码同批**。

**③ 🔴 一个恒 0 的计数器，「做过了」和「从来没做」读起来一模一样。**
镜像回填在这套集群上约 **100 ms** 跑完 ⇒ 「切读侧之前先看 `mirror_lag` 归零」这条判据，
在「跑过并结清」与「根本没跑」两种相位下**给出完全相同的读数**（实测撞过两次；
第二次更隐蔽：清了 PG 没清节点本地镜像 store，「历史已入队」标记还在盘上 ⇒
回填立刻返回、什么都没入队 ⇒ 三个数字一致同意两个目录一致，而当时 PG 0 行、对象存储 32 行）。
**定下来的做法：判据钉在「直接总体比对」上（`src/snapshot/repository/mirror/population.rs:359`
的 `admit_read_side`：问两个目录各自持有什么、谁都不信、不一致就点名 id），
用 `mirror_repaired_total` 佐证，不拿 lag 当主判据。**
🔴 **集群上已证过它有分辨力**：PG 31 / 对象存储 32 时四个 gauge 全读 0，总体判据照样拒绝并点名。

⇒ **§14 里凡是「某计数器为 0」的判据，全部要回答：这个 0，是「做了且结清」还是「没发生」？**
逐条：R3 的 C1（`concurrent_update_total` **必须 > 0**）已经是正面写法，**保持**；
R7 的重建判据不能只看「三条记录回来了」，要按它自己的射程边界那一行 **dump 后逐字段对比**；
🔴 **R8 的「reaper 关掉 ⇒ 永远卡住」是本组里最像 mirror_lag 的一条** ——
加一发正面对照：reaper 开着时 `transition_reaped_total` **必须 +1**，而不是只看沙箱最后活了。

**④ 🔴 指标是每节点的，集群级判据要求和，而产品里没有任何东西在求这个和。**
一台节点离场，会把它那份欠账**从总和里带走，不留痕迹**。
⇒ §14 的射程边界那一节已经写了「R1–R3 不证明 node 侧执行幂等」，**再加一条**：
R3 的「409 计数与 `execution_superseded_total` / `concurrent_update_total` 逐条相等」
是一条**跨副本求和**的判据 —— 🔴 **必须显式说清楚谁在求和、求和那一刻有几个 `api` 副本在报**，
并且**在一个副本被杀掉的相位里（R2）这条互证不成立**，要单独标注。

**⑤ 两个落在同一毫秒里的时间戳，让一条测试变得不可能失败。**
本轮抓到一个断言，它要区分的两个时刻由两次紧邻的取时产生，在快机器上落在同一毫秒 ⇒ 恒绿。
🔴 **本半是重灾区**：`rev`、`staged_at_unix_ms`、`heartbeat`、`expires_at_ms`、
`ZSET` 的 score、`pending` 的 score —— **任何基于时间先后的断言，都要由测试自己制造一个
可靠大于时钟粒度的间隔**（显式推进时钟，或注入两个确定的时间戳）。
点名两条：R5（`PTTL` 单调下降）与 R4（50ms 预算 vs 1ms 对照）。

### 17.5 🔧 就地订正

| # | 位置 | 现在的事实 |
|---|---|---|
| **1** | §1.2 与 §9.1 引的 `src/orchestrator/paused_registry/central.rs:274-345` / `:305` | 实际：`require_full_coverage` 定义在 **`:324`**，调用点在 **`:306`**；§9.3 引的「`covered.is_empty()` ⇒ 放行」那条豁免在 **`:329-331`**（注释理由在 `:317-323`）。**结论全部不变**，§9.1「两侧都已经实现」经本轮复核**仍然成立**（`services/scheduler/internal/registry/store.go:58` `:195-198`、`store_postgres.go:395`、`registry_service.go:231`） |
| **2** | §1.2 引的 `services/Makefile:36-60`（真 Redis 测试的纪律模板） | 文件已改：`test:` 在 **`:29`**，`report-skipped-suites:` 在 **`:46`**，`test-with-postgres:` 在 **`:81`**（`SCHEDULER_REDIS_TEST_REQUIRED` / `REDIS_SERVER_BIN` 在 `:94-95`）。🔴 **模板里现在多了两件要抄的**：`-count=1`（在两个子 Makefile 的 `:19`）与 `report-skipped-suites`。见 §17.4 ① |
| **3** | §16.10 的行号漂移表 | 🔴 **本轮又漂了一批**（[S] §15.5 末尾给了 `src/bin/server.rs` 的对照）。§16.10 立的那条规矩——**引用时带符号名，不要只带行号**——本轮再次被验证，**升格为本文的硬约定** |
| **4** | 🔴 §13.3 重建路径的数据源 | 本文已经要求 `control_plane_config` 必须是 `SandboxMetadata` 本体的版本化编码（§2.1 末尾 ＋ 附录第一条）。✅ **[S] 已接受**，写在 [S] §15.5 第 6 条与 §3.4 表格的就地更正里。**这条回传闭环。** |
| **5** | 🔴 §13.3 的触发条件（store 为空 ＋ 至少一台 node 报出带标记的沙箱） | **不改，但要按 §17.2 补第三态**：一台 `ListSandboxes` **调不通**的 node，既不满足「报了非空」也不该被当成「报了空」。⇒ 触发条件里的第二个条件必须写成「**至少一台 node 成功应答且返回非空**」，并且 🔴 **只要有 node 应答失败，就不许在这一轮下「集群是空的」这个结论** |

### 17.6 🔧 当前集群基线（本半的探针从这里开始）

| 项 | 值 |
|---|---|
| 运行中的一批 | `sd2c-*` |
| `snapshot.catalog.write` / `read` | **`both`** / **`postgres`** |
| 目录总体 | 30 条 `p0-seed-*` ＋ 2 条模板 = **N = 32**（两侧一致） |
| `mirror_lag` / `mirror_diverged` | 两台 node 上**都是 0**（🔴 按 §17.4 ③，这不是一致的证据） |
| 三个路由投影开关 | **全 on** |
| F4 sweep 开关 | **off** |
| 镜像 store | **空** |

🔴 **两条会直接影响 §14 探针能不能跑的环境事实**（详见 `_sd-recon-env.md` §11.8）：

1. **删任何一个 node Pod 之前，`/v2/sandboxes` 必须读 0** —— preStop 的 drain 循环没有超时也没有逃生口，
   **任何一次删除**都会跑它。R2（`kubectl delete pod` 杀掉一个 `api` 副本）打的是 Deployment 不是 DaemonSet，
   不受这条约束；🔴 **但 R7（删 Redis Pod ＋ PVC）与任何要动 node 的相位受**。
2. **要造「中心不可达」，探针必须走 node 直连** —— `scheduler` 副本数归零会把 gateway 一起废掉，
   那一发测到的是 gateway 502，不是节点面对一个够不着的中心时的行为。
   🔴 R1 的对照面（「把 B 的键空间指到别处」）不受影响，**但任何「Redis 够不着」的相位要照这条设计取样点**。

---

## 附：要回传给别人的三条

| 给谁 | 内容 | 为什么现在就要传 |
|---|---|---|
| **[S]（结构半）** | 🔴 `control_plane_config` 必须是 `SandboxMetadata` 本体的**版本化 serde 编码**，不是 [S] §3.4 写的七字段子集 | 它是 §13.3 重建路径的唯一数据源。七字段重建不出 `resources` / `max_lifetime` / `network_policy` / `custom_extension_params` 等十二项。**proto 一旦定稿就改不动** |
| **[S]（结构半）** | ✅ 已收到并接受：记录带 `origin_node_id` ＋ `published`（[S] §6.6 第 2 条）。本文从 `paused_state` 的 `artifact_root` 是本地路径**独立推出了同一条** | 记录结构建起来改不动 |
| **母提案 / 模块文档** | §16 的九条更正，其中 §16.1 / §16.2 / §16.4 / §16.5 / §16.7 需要**改文档正文**（不是补注），因为它们会导致按字面执行的人做错事 | 三份文档正在被并行修改，越晚合并冲突越大 |

## 附：本文核对过的引用索引

**e2b（`/home/debian/e2b-infra`）** —— 见 §1.1 的全表，全部逐行核过，
其中任务要求逐条自证的八条：`scripts.go:33-40` ✅ / `lock.go` ＋ `state_change.go:41` `:190` ✅ /
`operations.go:159-215` ✅ / `utils.go:22-25` `:31-38` `:61-68` ✅ /
`evictor/evict.go` ＋ `orchestrator.go:192-196` ＋ `heal.go` ✅ /
`store.go:157` ＋ `reservation.go:50` ✅ / `main.go:205-217`（实际 `:205-209`）✅ /
`packages/shared/pkg/redis/tests.go:17`（实际 `:14-48`）✅。

**本文额外核出、而三份上游文档都没引过的四条：**

| 事实 | 位置 | 为什么重要 |
|---|---|---|
| 🔴 沙箱记录键从不设 TTL ⇒ `KeepTTL` 保的是空 | `scripts.go:12-15`、全目录 `grep Expire` | §16.1 |
| 🔴 `ExpiredItems` 的 stale-cutoff 分支才是崩溃恢复的实际出口 | `items.go:149-159` | §16.3 |
| 🔴 pipeline 出错 ⇒ 整轮跳过以避免 mass kill | `main.go:191-200` | §9.2 第 3 条，陷阱 7 漏掉的那一行 |
| 🔴 `handleExistingTransition` 是裸递归，无深度上限 | `state_change.go:383` | §6.8 |

**AgentENV 侧** —— 见 §1.2 的全表。其中本文新增的六条：
`metadata.rs:119-122`（`paused_state` 不可序列化）／
`service.rs:965-986`（闭包写外层变量）／
`service.rs:1483-1541`（跨 I/O 的无谓词读-改-写）／
`service.rs:2390-2408`（`add` 在 VM 之后）／
`src/api/openapi.yml:540-580`（`NewSandbox` 无 `sandboxID`）／
`service.rs:2201-2237` ＋ `:1341`（驱逐不重校验过期）。
