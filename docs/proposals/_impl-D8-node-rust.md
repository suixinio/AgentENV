# 实施记录：中央控制面 阶段 2 · Slice C（node 侧 Rust Central 后端）

> 2026-08-19 · 研发 agent D8（**接手第二棒**：前一个 agent 写完主体后进程终止，无交付报告、门槛未跑完）。
> 分支 `central-control-plane-phase2`，**未 commit / 未 push**。
> 任务书：[`_impl-plan-control-plane-phase2.md`](_impl-plan-control-plane-phase2.md) §2 🅒 + §1 §3 §6
> 侦察：[`_recon-R4-rust-client.md`](_recon-R4-rust-client.md) §3 §4 §7、[`_recon-R2-registry-spec.md`](_recon-R2-registry-spec.md) §2
> 对端：Go 侧 `339d1f2`（`services/` **本轮零改动**）
>
> 只动 `src/` + `build.rs` + `tests/`。proto 未改（冻结）。未连集群、未 `make k8s-apply`、未 commit。

---

## 0. 一页速览

| 项 | 结论 |
|---|---|
| C1~C6 | **全部落地**。接手时 C1/C2/C5 完整、C3/C4/C6 主体完整，本轮补齐三处缺口 + 5 个测试 |
| 契约 ① metadata 缺席编码 | ✅ 对得上，且已钉死（写路径不可能产出 `None`/`null`，两发测试） |
| 契约 ② `lease_ttl_millis` 覆盖面 | 🔴 **接手时是错的**：`remove` 也带了 TTL，且测试反向钉死了错误行为。已修 + 重写测试 |
| 变异验证 | **37 发全部 FAILED（有牙）**。含每一类 gRPC 失败各一发 |
| 门槛 | `make fmt` / `make clippy` / `make test-unit` / `make test-paused-registry`（0 skip）**全绿** |
| 本轮发现的既有缺陷 | 2 个：C4 的 HTTP 状态码**无任何测试**（变异存活）、日志断言测试**随机失败**（1/5） |

---

## 1. 现状盘点：C1~C6

| # | 接手时 | 本轮补了什么 |
|---|---|---|
| **C1** `Central` 变体 + `build_paused_registry` 分支 | ✅ 完整。`cfg.rs:390-396` 加变体，`mod.rs:356-372` 加分支（签名已收 `&ClusterConfig`），空 / 全空白端点 fail-closed，`config/default.toml` + `docs/src/` 已同步 | — |
| **C2** `central.rs` 13 个 trait 方法 | ✅ 13/13 齐（`begin_pause` `complete_pause` `mark_local_only` `get` `get_many` `claim_for_resume` `release_claim` `renew_lease` `reclaim_expired_holdings` `mark_running` `release_node_holdings` `remove` `is_cluster_backed`），映射到 5 个 RPC，`reclaim_expired_holdings` 按 B6 是本地 no-op | 逐条对着 Go `registry_service.go` 复核（见 §3.1），未发现映射错 |
| **C3** 错误映射 | ✅ 主体正确：`unreachable()` 一处收口，所有 `Status` ⇒ `Backend`；`malformed()` 处理"响应到了但没说该说的" | 补 **EOF/半开** 与 **解码失败** 两条路径的测试（见 §4）；补全变异表 |
| **C4** 护栏 §3.5 | ✅ `ResumeArbitration::Unavailable` + 收窄版（只有 `ClusterRegistration::As(_)` 才拒）+ 两条路径各自 metric + 500 已在 | 🔴 补 **HTTP 状态码测试**（原先零覆盖，变异 M25 存活）；补注释说明**为什么不是 503** |
| **C5** `build.rs` `build_server(true)` | ✅ 已翻，注释解释了为什么（传输层错误只能由 socket 另一端产生） | — |
| **C6a** `superseded_by_cluster` 静默不删补 `debug!` | ✅ 已在 `paused_recovery.rs:811-819` | — |
| **C6b** `renew_lease` 连续失败计数 metric | ✅ `agentenv_paused_registry_renew_consecutive_failures`（gauge，`paused_coordinator.rs:201-215`），成功即清零 | — |

### 本轮改动清单

```
 src/orchestrator/paused_registry/central.rs   remove 不再带 lease TTL；+3 测试；重写 1 测试
 src/orchestrator/paused_registry/types.rs     metadata: Option 的 None 语义写死进注释
 src/api/impls/sandbox.rs                      注释：为什么是 500 不是 503
 src/api/impls/paused_recovery.rs              +1 测试（C4 的 HTTP 状态码）
 src/logging.rs                                🔴 修日志断言测试的随机失败（见 §7.2）
 tests/paused_registry.rs                      回退一处顺手格式化（该文件在 main 上就是 fmt-红）
```

---

## 2. 两条契约核对（任务书第二步）

### ① metadata 缺席的双侧编码 —— ✅ 对得上

| 层 | "不携带" | "有值" | 现状 |
|---|---|---|---|
| wire（`bytes metadata_json`）| 空 bytes | 原始 JSON 字节逐字 | ✅ `transition()` 非 begin_pause 一律传 `Vec::new()` |
| Rust | `None` | `Some(..)` | ✅ `PausedSandboxEntry.metadata: Option<SandboxMetadata>` |
| Go | `nil` RawMessage | 非 nil 逐字存回 | ✅ `store.go:150` `json.RawMessage` |

**`None` 不可能变成 JSON `null`，有两道锁：**

1. **类型层**：`begin_pause` 先 `let Some(metadata) = entry.metadata.as_ref() else { return Err(InvalidRecord) }`，
   之后 `serde_json::to_vec(metadata)` 的入参是 `&SandboxMetadata` 而非 `&Option<_>` —— `null` 无法被构造出来。
2. **测试层**（本轮补）：`a_pause_never_writes_a_record_that_is_not_an_object` 断言线上字节
   `serde_json::from_slice::<Value>().is_object()`。它挡的不是今天的代码，是明天那个"顺手改成序列化
   `Option`"的重构 —— 那个改动**能编译通过**。
   配套的 `a_pause_without_a_record_is_refused_before_it_is_sent` 断言"连发都没发出去"。

变异 M12（把 `Some` 判空去掉、改成序列化 `Option`）与 M12b（无条件写 `null`）**都 FAILED**。

`Option` 定义处（`types.rs:78-96`）已写明：

> 🔴 `None` says *this answer did not carry the record*, never *this sandbox has no record*.
> Every row in the table has one — the write that creates the row is refused without it —
> so there is no such thing as a sandbox whose record is absent…

Go 侧对 `null` / 数组 / 标量 / 空白一律 `ErrInvalidRecord`（`store_postgres_test.go:558`
`TestBeginPauseRefusesMetadataThatIsNotAnObject`），两端同一条规则的两头。

### ② `lease_ttl_millis` 覆盖面 —— 🔴 接手时是错的，已修

**接手时**：`transition()` 无条件塞 `lease_ttl_millis: self.lease_ttl_millis`，`remove` 也带。
更糟的是测试 `the_lease_length_travels_with_every_write` 取的 `seen_transition[0]` **恰好就是那发
`remove`**，等于把错误行为反向钉死了。

**已改**（`central.rs:291-294`）：

```rust
let lease_ttl_millis = match kind {
    pb::TransitionKind::Remove => 0,
    _ => self.lease_ttl_millis,
};
```

测试重写成六种 transition 的 `(kind, ttl)` 全序列断言，`Remove` 那条是 `0`。
变异 M13a（remove 也带）与 M13b（一律不带）**都 FAILED**。

TTL 值取本节点 `config.lease_ttl_secs()`（**夹紧后**的方法值，非字段），
`AcquireSandboxRequest` 与 `RenewNodeLeaseRequest` 也各带一份。理由已写进字段注释
（`central.rs:67-73`）：node 按自己的 `reconcile_interval` 续租，`ttl ≥ 3×interval` 这条下限
在 node 侧校验（`cfg.rs:455-458`），controller 自选会让行在一台完全守规矩的节点底下过期。

> Go 侧对 `remove` 不检查该字段（`rejectFields` 只管 generation / metadata / snapshot），
> 所以带与不带今天**都不会被拒**。改它是为了让契约与实现一致 —— 下一次 Go 侧给
> `rejectFields` 加上 `fieldLeaseTTL`，就不必再回头改 node。

---

## 3. C3 错误映射对照表

### 3.1 gRPC 结果 → `PausedRegistryError` → 调用方行为

`central.rs` 只有两个出口把"外部世界"变成错误：`unreachable()`（收所有 `tonic::Status`）
与 `malformed()`（响应到了但没说该说的）。**没有第三条路径，也没有任何一条通向 `Ok(空)`。**

| gRPC 侧发生了什么 | 实测 `Status` | → `PausedRegistryError` | 钉死它的测试 |
|---|---|---|---|
| controller 主动报错（16 个 code 全量） | 对应 code | `Backend` | `no_grpc_failure_ever_arrives_as_an_empty_batch` / `..._as_a_missing_row` |
| 连不上（端口无人监听） | `Unavailable` | `Backend` | `a_controller_that_cannot_be_dialled_is_a_failure_not_an_answer` |
| **EOF / 半开**（连上后对端挂断） | 🔴 `Cancelled` | `Backend` | `a_controller_that_hangs_up_mid_call_is_a_failure_not_an_answer` |
| 超出本地调用预算（10s） | `Cancelled` | `Backend` | `a_call_that_outlives_its_budget_is_a_failure_not_an_answer` |
| **解码失败**（响应超过 tonic 4 MiB 上限） | `OutOfRange` | `Backend` | `an_answer_too_large_to_decode_is_a_failure_not_an_empty_registry` |
| 分片读某一片失败（部分流） | 任意 | `Backend`（整发失败） | `a_roster_split_across_requests_is_still_all_or_nothing` |
| 响应成功但 `outcome` 未设 | — | `Backend`（`malformed`） | `a_claim_with_no_outcome_is_a_failure_not_a_missing_sandbox` |
| 响应成功但 claimed 无 entry | — | `Backend`（`malformed`） | `a_granted_claim_with_no_row_is_a_failure` |
| 行读不懂（未知 state / 非 uuid / 时间戳越界 / `paused` 无 snapshot） | — | `InvalidRecord` | `a_row_this_build_cannot_read_is_reported_rather_than_skipped` |
| 行属于**别的 cluster** | — | `InvalidRecord` | `a_row_belonging_to_another_cluster_is_refused` |
| claim 带的 metadata 解不开 | — | `InvalidRecord` | `a_claim_carrying_an_unreadable_record_is_refused` |
| 成功响应里那行**确实不在** | — | `Ok(None)` / 短 map ← **唯一合法的"空"** | `an_absent_row_in_a_successful_answer_is_still_no_row` |

🔴 **`EOF` 落在 `Cancelled` 上是本轮实测出来的**（原以为是 `Unknown`/`Unavailable`）。
`Cancelled` 这个名字读起来像"调用方自己放弃了"，而实际情况是**请求很可能已经执行、只是回答丢了**。
catch-all 映射天然覆盖它，测试里也留了注释。

### 3.2 错误 → 调用方行为（R4 §4.2 的 16 个调用点，Central 后端下的实际后果）

| 调用点 | 出错时 | 方向 |
|---|---|---|
| `get_many` ×2（reconcile running / paused） | `warn!` + `return`，**不删任何本地 artifact，不拆任何运行中沙箱** | ✅ 停手 |
| `claim_for_resume`（`arbitrate_resume`） | 本地有 `As(_)` 记录 ⇒ `Unavailable` ⇒ **HTTP 500**；否则 `Proceed` | ✅ **本轮护栏 §3.5** |
| `get`（`resolve_missing_local_resume`） | `MissingLocalResume::Undecided` | ✅ 不下结论 |
| `get`（`superseded_by_cluster`） | `Err(())` ⇒ 不删本地副本，**并打 `debug!`** | ✅ 停手（C6a） |
| `begin_pause` | `warn!` + `return None` ⇒ 整个 publish 放弃 | ✅ 保守 |
| `complete_pause` / `mark_local_only` | 先 `get` 再决定删不删快照（Slice A5 已做） | ✅ |
| `mark_running` | `confirmed = false`，不登记 running registration | ✅ 保守 |
| `renew_lease` | `warn!` + **连续失败计数 gauge**，下个 tick 再来 | ⚠️ 可观测（C6b） |
| `release_node_holdings` | 三态 `StaleReleaseOutcome`，`Failed` ⇒ 带围栏重试（Slice A1 已做） | ✅ 可重试 |
| `release_claim` / `remove` | `warn!` 继续 | 中性 |

**注意 `Backend` vs `InvalidRecord` 的区别今天没有行为差异** —— 生产代码里没有任何一处按变体 match
（R4 §4.1）。两者都是 `Err`，都不是空结果，护栏成立。区分只是为了排障。

---

## 4. 变异验证表

方法：把修复/实现**退回去**（多数是退成"当作空结果"），确认对应测试 FAIL，然后还原。
每发变异从**原始副本**重新生成，跑完立即还原；收尾核对工作树无残留。

### 4.1 C3 —— 每一类 gRPC 失败各一发

| # | 变异 | 结果 | 被抓的测试 |
|---|---|---|---|
| M1 | `NotFound` ⇒ 空批 | ✅ FAILED | `no_grpc_failure_ever_arrives_as_an_empty_batch` / `..._as_a_missing_row` |
| M2 | `Unavailable` ⇒ 空批 | ✅ FAILED | 上述 + `a_controller_that_cannot_be_dialled` + `a_roster_split_across_requests` |
| M3 | `DeadlineExceeded` ⇒ 空批 | ✅ FAILED | 全量 code 扫描两发 |
| M4 | `Cancelled` ⇒ 空批 | ✅ FAILED | 上述 + `a_controller_that_hangs_up_mid_call` + `a_call_that_outlives_its_budget` |
| M5 | `OutOfRange`（解码失败）⇒ 空批 | ✅ FAILED | 上述 + `an_answer_too_large_to_decode` |
| M6 | 分片失败 `break` ⇒ 短 map | ✅ FAILED | 7 发全挂 |
| M15 | claim 传输失败 ⇒ `NotFound` | ✅ FAILED | `a_controller_that_cannot_be_dialled` / `..._hangs_up_mid_call` |
| M16 | renew 传输失败 ⇒ `renewed = 0` | ✅ FAILED | `a_controller_that_cannot_be_dialled` |
| M17 | release 传输失败 ⇒ 什么都没释放 | ✅ FAILED | 同上 |
| M18 | transition 传输失败 ⇒ 当作成功 | ✅ FAILED | 同上 |

> **M15 的第一版变异存活了，而且是好消息。** 最初把 claim 的错误映射换成
> `unwrap_or_default()`，测试仍全绿 —— 因为 `AcquireSandboxResponse::default()` 的 `outcome` 是
> `None`，紧接着就被 `malformed` 那道闸拦下了。**两道独立的闸**，改成显式伪造
> `NotFound` outcome 之后才失效。这条记下来：C3 在 claim 路径上不是单点。

### 4.2 C3 —— 响应到了但不可信

| # | 变异 | 结果 | 被抓的测试 |
|---|---|---|---|
| M7 | `outcome` 未设 ⇒ `NotFound` | ✅ FAILED | `a_claim_with_no_outcome_is_a_failure_not_a_missing_sandbox` |
| M8 | claimed 无 entry ⇒ `NotFound` | ✅ FAILED | `a_granted_claim_with_no_row_is_a_failure` |
| M9 | 读不懂的行 ⇒ 跳过 | ✅ FAILED | `a_row_this_build_cannot_read` + `a_row_belonging_to_another_cluster` |
| M10 | 别的 cluster 的行 ⇒ 接受 | ✅ FAILED | `a_row_belonging_to_another_cluster_is_refused` |
| M11 | metadata 解不开 ⇒ 默认值 | ✅ FAILED | `a_claim_carrying_an_unreadable_record_is_refused` |
| M24 | `get` 拿回什么就用什么（不按行 id 对齐） | ✅ FAILED | `a_row_for_a_different_sandbox_is_not_mistaken_for_this_one` |
| M33 | 批量读也塞一份 record | ✅ FAILED | `a_bulk_read_carries_no_sandbox_record` |

### 4.3 契约 ① ② + 线上表示

| # | 变异 | 结果 | 被抓的测试 |
|---|---|---|---|
| M12 | 序列化 `Option`（缺席 ⇒ `null`） | ✅ FAILED | `a_pause_without_a_record_is_refused_before_it_is_sent` |
| M12b | 无条件写字面量 `null` | ✅ FAILED | `a_pause_never_writes_a_record_that_is_not_an_object` + `the_sandbox_record_travels_byte_for_byte` |
| M13a | `remove` 也带 TTL | ✅ FAILED | `the_lease_length_travels_with_every_write` |
| M13b | 一条都不带 TTL | ✅ FAILED | 同上 |
| M14 | `previous_state` 从返回行读 | ✅ FAILED | `the_state_a_claim_replaced_is_not_read_off_the_row` |
| M28 | 时间戳按毫秒解 | ✅ FAILED | `timestamps_keep_their_microseconds` |
| M29 | 调用不带 deadline | ✅ FAILED | `every_call_carries_its_deadline` + `a_call_that_outlives_its_budget` |
| M31 | 所有 transition 发 `UNSPECIFIED` | ✅ FAILED | `every_transition_names_itself…` + `the_lease_length_travels…` |
| M32 | 条件写丢掉 `expect_generation` | ✅ FAILED | `every_transition_names_itself_and_says_whether_it_is_conditional` |
| M34 | 空 roster 也去打扰 controller | ✅ FAILED | `asking_about_nothing_asks_the_controller_nothing` |
| M35 | "永不过期"发成 1970 的 deadline | ✅ FAILED | `a_renewal_carries_every_holding_and_its_deadline` |

### 4.4 C1 / C4 / C6

| # | 变异 | 结果 | 被抓的测试 |
|---|---|---|---|
| M27 | 缺端点 ⇒ 静默兜底成 localhost | ✅ FAILED | `the_central_backend_without_an_endpoint_is_a_startup_failure` |
| M30 | `is_cluster_backed() = false` | ✅ FAILED | `the_central_registry_is_cluster_backed` + `the_central_backend_comes_up_against_an_endpoint` |
| M19 | 不可达 ⇒ 一律 `Proceed`（**退回护栏前**） | ✅ FAILED | `an_unreachable_registry_refuses_a_resume_for_a_copy_the_cluster_knows_about` 等 3 发 |
| M20 | 不可达 ⇒ 一律拒（一刀切，非收窄版） | ✅ FAILED | `..._still_proceeds_for_a_copy_the_cluster_never_saw` 等 3 发 |
| M25 | `Unavailable` 答成 **404** | ✅ FAILED（**补测试后**） | `a_resume_nobody_could_arbitrate_is_retryable_not_a_missing_sandbox` |
| M26 | `Unavailable` 落进本地 resume | ✅ FAILED（**补测试后**） | 同上 |
| M36 | 拒绝时不打日志 | ✅ FAILED | `an_unreachable_registry_refuses_a_resume_for_a_copy_the_cluster_knows_about` |
| M21 | 静默不删（去掉 C6a 的 `debug!`） | ✅ FAILED | `a_registry_that_cannot_be_asked_says_so_before_keeping_the_local_copy` |
| M22 | 续租成功不清零 | ✅ FAILED | `a_renewal_that_lands_ends_the_run` |
| M23 | 续租失败不计数 | ✅ FAILED | `consecutive_failed_renewals_are_counted` + 上一条 |
| M37 | "回退一个快照"的 claim 降级成 `debug!` | ✅ FAILED | `a_claim_that_cost_somebody_their_last_pause_is_a_warning` |
| M38 | 日志层过滤器移回 registry 之上（见 §7.2） | ✅ **12/12 run FAILED** | `api::impls::paused_recovery` 整组 |

**合计 37 发，全部 FAILED。** 其中 M25 / M26 是**接手时会存活**的（原先没有任何测试断言
C4 的 HTTP 状态码），补测试之后才有牙。

---

## 5. 门槛实际输出

```
$ make fmt        # cargo fmt --all -- --check
src/api/generated/src/models.rs:3351      ← main 上即红（生成物，工具链版本差异）
tests/paused_registry.rs:264              ← main 上即红（本轮改过的两行不在此处）
                                             本轮改过的所有文件 fmt 干净
                                             （postgres.rs 已由前一棒顺手修好，保留）

$ make clippy     # --workspace --all-targets --all-features -- -D warnings
exit 0，零 warning（唯一输出是 proc-macro-error2 的 future-incompat note，与本轮无关）

$ make test-unit
test result: ok. 852 passed; 0 failed; 4 ignored     ← agentenv --lib
test result: ok. 0 passed                            ← envd
test result: ok. 3 passed                            ← linux-cap
test result: ok. 4 passed                            ← agentenv --lib -- --ignored
test result: ok. 8 passed                            ← uvm-ublk
test result: ok. 46 passed                           ← uvm-ublk-daemon
verify-capability-runner.sh / verify-install-service.sh  ok
MAKE_EXIT=0

$ AENV_PAUSED_REGISTRY_TEST_DSN=… make test-paused-registry
running 29 tests
test result: ok. 29 passed; 0 failed; 0 ignored      ← 🔴 0 skip
EXIT=0
```

抗抖动：`cargo test -p agentenv --lib` 连跑 5 次全 852 绿；
先前随机失败的 `api::impls::paused_recovery` 子集连跑 12 次全绿（修复前 1/5 失败，
把修复退回去后 **12/12 失败**）。

分模块计数：`paused_registry::central::tests` 32（前一棒 29 + 本轮 3）、
`paused_registry` 全模块 39、`paused_recovery::tests` 35（前一棒 34 + 本轮 1）、
`paused_coordinator` 19。

---

## 6. 与任务书不一致处

### 6.1 🔴 `GenerationConflict` 在 Central 后端下**表达不出来**

任务书 C3 白纸黑字："gRPC 层任何非 `OK` ⇒ `PausedRegistryError::Backend`"。照做了。
但 Go 侧**刻意**给 CAS 失败留了独有的 code：

```go
// registry_service.go:513-517
// Aborted and FailedPrecondition are deliberately different codes. A generation
// conflict means somebody else wrote first and the caller's view is stale,
// which its own code handles by re-reading; …
case errors.Is(err, pausedregistry.ErrGenerationConflict): return codes.Aborted
```

而 Rust 侧 `PostgresPausedSandboxRegistry` 在 CAS 失败时返回
`PausedRegistryError::GenerationConflict`（`postgres.rs:427,461`），R2 §2.3 的测试
`a_downgrade_that_matches_nothing_is_reported` 钉死了这一点。

⇒ **同一个事件，两个后端返回不同变体。** 切到 `central` 之后，
`tests/paused_registry.rs` 里那条断言只在 `postgres` 后端下成立。

**为什么本轮没改**：
1. 任务书对 C3 的措辞是加粗的祈使句，没有"除 CAS 外"的例外；
2. **今天没有行为差异** —— 生产代码里没有任何一处按 `PausedRegistryError` 变体 match
   （`grep` 在 `src/` 内除定义文件外零命中），所有调用方一律 `Err(err) =>` 一把抓；
3. 护栏的硬约束是"不许变成空结果"，`Aborted ⇒ GenerationConflict` 也不是空结果，两种映射都不破护栏。

**建议**：阶段 3 若真要让节点按变体分流（比如 CAS 失败重读、`InvalidRecord` 不重试），
在 `unreachable()` 里加一条 `Code::Aborted ⇒ GenerationConflict` 即可 —— 一行，且 Go 侧已经备好了。
**要不要现在就加，请裁决。**

### 6.2 `docs/src/` 有改动，不在"只动 src/ + build.rs + tests/"之内

`docs/src/configuration/env-vars.md` 与 `docs/src/deployment/kubernetes.md` 在工作树里带改动，
**是前面两棒（Slice A A4 与 Slice C 首棒）留下的**，本轮一个字没动。内容是对的
（`central` 的 env 说明 + K8s 切换 runbook），不建议回退。

### 6.3 C4 的 metric 是"一个带标签的 counter"，不是"两个 metric"

任务书说"必须有 metric 区分这两条路径"。实现是
`agentenv_paused_registry_resume_unarbitrated_total{outcome="proceeded"|"refused"}`。
一个 counter 两个标签值 —— 语义上区分开了，PromQL 上也能分开画。按已满足处理。

---

## 7. 发现但没做完 / 需要下一棒知道的

### 7.1 ✅ 已修：C4 的 HTTP 状态码此前零覆盖

`ResumeArbitration::Unavailable` 走到 `sandbox.rs` 的 500 分支，**接手时没有任何测试碰过它**。
变异 M25（改成 404）**测试全绿存活**。这条很要命：404 在下游 agent-platform 是
"沙箱没了，重建"（`aenv/service.go:525-526` `errors.Is(err, ErrNotFound) => sandboxGone`），
会**清掉 external_id 从模板重建，用户工作区回到初始态** —— 正是护栏 §3.5 要避免的后果，
换了条路到达。

已补 `a_resume_nobody_could_arbitrate_is_retryable_not_a_missing_sandbox`（直接驱动
`sandboxes_sandbox_id_resume_post`，断言 500 + 独有文案），M25/M26 现在都 FAILED。

**顺带核实了"500 与 503 对唯一消费方逐字无差别"这条前提**：
`apps/agent-platform/internal/sandbox/aenv/client.go:296-298` 只特判 404 与 409，
其余全落 `default:` ⇒ `sandboxUncertain`。前提成立，注释里写进去了。

### 7.2 ✅ 已修：日志断言测试随机失败（1/5），根因是 tracing 的 callsite 兴趣缓存

`Recorder::install()` 原本用 `tracing::subscriber::set_default`（线程作用域）。
问题不在作用域，在于 **callsite 的"是否有人关心"是全进程决定一次的**：
哪个测试先跑到那条 `warn!`/`debug!`，就用当时的全局 dispatcher 定下它的 interest 并缓存；
如果那一刻没有全局 subscriber（或全局 subscriber 的 `EnvFilter` 是 `info`），
该 callsite 被缓存成"没人关心"，**后面所有装了 recorder 的测试都收不到自己的事件**。
测试顺序由 harness 随机决定 ⇒ 随机失败。

修法（`src/logging.rs`）：
- `init_for_tests()` 把 `EnvFilter` 从**整个 registry 之上**挪到**打印层自己身上**
  （`registry.with(fmt_layer.with_filter(filter))`），于是 registry 报出的 max level 是 TRACE，
  没有任何 callsite 会被缓存成"没人关心"；
- capture 层改成**进程级安装一次**（`#[cfg(test)]` 挂进 `init_for_tests` 的栈），
  只有**路由**是线程局部的（`thread_local! ACTIVE`）；
- `install()` 先调 `init_for_tests()`（`Once` 幂等），再登记本线程的 recorder，返回的 guard 负责摘除。

⚠️ **约束写进注释了**：capture 只对 `#[tokio::test]`（current-thread）成立；
`flavor = "multi_thread"` 的测试会什么都录不到。

### 7.3 未做：`reclaim_expired_holdings` 每 30s 打一条 `debug!`

Central 后端下它是 no-op，但每个 reconcile tick 都会打一条
`"skipping expired-holding reclamation…"`。debug 级别，不影响生产日志量，留着当"这个后端确实在跑"的证据。
若嫌吵可改成只在首次打一次。

### 7.4 未做：`central.rs` 与 `postgres.rs` 各有一份 `GET_MANY_CHUNK = 1_000`

两个模块各自定义，值相同、理由不同（一个是 SQL 参数数组上限，一个是消息体大小）。
没有合并，因为合并会让"为什么是 1000"这条注释失去它各自的语境。

### 7.5 未做（超出本轮范围）：切换与回退

- `backend` 无热加载（`OnceLock`），切 `central` 与回退 `postgres` **都要重启 node**；
- 混跑期两种 backend 写同一张表：generation CAS 仍能仲裁，但 `ensure_schema` 的 advisory lock
  会与 controller migration 抢 ⇒ **在无活沙箱窗口整批切**（已写进 `docs/src/deployment/kubernetes.md`）；
- 阶段 2 期间 controller 的 migration 必须是 `SCHEMA_DDL` 逐字复制（任务书 §5.1，Go 侧的事）。

### 7.6 未验证：真集群

按约束未连集群、未 `make k8s-apply`。本轮的全部验证都在
**in-process 假 controller（真 socket、真 HTTP/2、真 tonic 编解码）**  与本机 PG 上完成。
node ↔ 真 scheduler 的端到端仍待 Slice D。
