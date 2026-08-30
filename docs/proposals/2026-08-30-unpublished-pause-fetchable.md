# 未发布暂停：让字节可取、让恢复可等，然后收掉 `local_only`

**状态**：待对抗审查（v1）
**仓库基线**：AgentENV `55a3b6a`
**参照实现**：e2b-dev/infra `fdc33599b`（本地 `/home/debian/e2b-infra`）

---

## 1. 问题

一次暂停的字节，在进入共享存储之前，只存在于 origin 节点的盘上。这个窗口今天由控制面的两个
状态记录：`publishing`（同步上传进行中）和 `local_only`（同步上传失败后）。两者在所有承重处
语义相同——`LookupNode` 都答 `PINNED`，都被注释描述为 "exactly one copy of the bytes"
（`src/api/impls/resume_surface.rs:1063,1471`）。

代价是：**这个窗口内的沙箱被钉死在一台机器上**。origin 消失即不可恢复
（`resume_surface.rs:1149` 的 "eaten by a grue"）；而租约过期后
`claim_for_resume` 会把它交给任意节点，后者用**上一次成功发布的旧快照**重建并返回成功——
模块头把这件事说得很直白：

> waking it anywhere else does not fail. It *succeeds*, by rewinding the sandbox to whatever
> older snapshot did reach storage, and the user sees a workspace that has silently lost work.

E2B 没有等价状态。它的窗口**更宽**（见 K-4），但它把窗口做成了"可取 + 可等"，因此不需要记录
"取不到"。本方案把那两块机器补上，然后让状态失去存在理由。

## 2. 约束（全部已在代码中核实）

**K-1 post-commit 广告是刻意约定，不能简单提前。** `src/snapshot/manager.rs:331`：

> 🔴 After the commit, never before: publishing artifacts for a snapshot whose row was never
> written would advertise something no reader can resolve, and the convention that P2P only
> carries committed snapshots is what lets a peer treat a hit as authoritative.

把封层后的字节广告出去，必须**不破坏**"committed ⇒ authoritative"这条约定。

**K-2 删除未发布行不安全，且与路由无关。** `src/api/impls/paused_coordinator.rs` 失败分支：

> Keep the row and downgrade it instead of deleting it... A deleted row would be
> indistinguishable from "already resumed elsewhere", and reconciliation would then throw away
> the only copy that exists.

**K-3 `refuse_unhonourable_pin` 是唯一挡在静默倒带前面的东西**
（`resume_surface.rs:892-900`）。任何改动都不得让它在"字节确实取不到"时失效。

**K-4 改成异步 pause 会把丢失窗口从"仅失败时"扩大到"每次 pause"。** E2B 的 `Pause`
（`packages/orchestrator/pkg/server/sandboxes.go:789+`）先 `snapshotAndCacheSandbox` 落本地，
再 `s.uploadSnapshotAsync(...)`（注释原文 "Fire and forget - upload completes in the
background"），**上传还在飞就返回成功**。我们今天是 `publish_captured().await` 同步完成才算
暂停成功。E2B 在此处比我们激进。

**K-5 我们没有等待原语。** `resume_surface.rs` 的 `locate_for_resume` → `arbitrate_resume`
是"要么兑现 pin，要么拒绝"，没有任何等待/轮询。E2B 有 `Uploads.Wait`：Redis pub/sub 的
per-build 频道 + 轮询远端存储（`packages/orchestrator/pkg/sandbox/uploads.go:58-64,105-109`），
外加 `peerclient`/`peerserver` 直接取。

**K-6 发布本地层的零件已存在。** `/p2p-control/publish-layer`
（`crates/aenv-node/src/overlaybd/p2p/facade.rs:38,179,537`）已支持按引用发布完整本地层；
P2P 层键是内容寻址的 `overlaybd-layer/v1/sha256:<digest>`。

**K-7 `stage` 与 `commit_staged` 已经是分开的接缝**（`src/snapshot/manager.rs:234,298,322`），
节点 stage 字节、api 写行，异步化不需要新的拆分。

## 3. 范围

| | 交付 | 性质 |
|---|---|---|
| **S1** | 合并 `publishing` + `local_only` 为单一未发布态 | 纯简化，保留 pin，零行为变化 |
| **S2** | 封层即广告到**独立的 in-flight 键空间** | 严格更安全（多一条取字节的路） |
| **S3** | resume 遇未发布态改为**有界等待 + 尝试取件**，超时才走今天的拒绝 | 严格更安全（少一次不必要的拒绝） |
| **S4** | 异步返回 pause | 🔴 **拿持久性换延迟，需单独裁决，不在本期默认范围** |

S1–S3 之后，未发布态不再承担路由职责，只剩"这次上传还没完成"的重试含义；届时才谈收掉它。

## 4. 设计

### 4.1 S1 状态合并

`publishing` 与 `local_only` 合并为 `unpublished`。"在飞 / 已失败 / 重试次数 / 最后错误"
降级为列，不再是状态。`LookupNode` 对 `unpublished` 仍答 `PINNED`；
`refuse_unhonourable_pin` 行为不变（K-3）。

数据库变更走既有 ledger（`crates/aenv-api/src/snapshot/repository/backends/postgres/migrate.rs`
的版本表 + `paused_registry/postgres/schema.rs` 的 execution-axis preflight），**并需要一个
adopt 路径**处理存量 `local_only` 行。

### 4.2 S2 in-flight 键空间

- 已提交快照继续广告到 `snapshot/v1/artifacts/{snapshot_id}/...`，语义不变：**命中即权威**（K-1）。
- 封层完成、commit 之前，把同一批字节广告到**独立命名空间**
  `snapshot/v1/inflight/{sandbox_id}/{generation}/...`。
- 两个命名空间的消费者不同：in-flight 键的命中**不是**权威快照，只是"origin 上确实有这批字节"
  的证据，仅供 S3 的等待方取用；取到之后仍需等 commit 才认为快照存在。
- 键里带 `generation`，避免上一代失败的暂停被下一代误取。
- commit 成功后撤销对应的 in-flight 键（`unpublish`）。
- 复用 K-6 的 `/p2p-control/publish-layer` 通路，不新造发布机制。

### 4.3 S3 恢复端的等待

`arbitrate_resume` 命中未发布态时，不再立即拒绝，而是：

1. 查 in-flight 键。命中 → 从 origin 取层，取满后按已发布路径继续；
2. 未命中或取件失败 → **有界等待**（订阅 + 轮询，上限可配），期间 origin 可能完成上传；
3. 超时仍不可得 → 走今天的路径：兑现 pin（origin 还在）或 `refuse_unhonourable_pin`（origin 没了）。

🔴 第 3 步必须保留，它是 K-3 的兑现。等待只是在"拒绝"之前多给一次机会，不能取代拒绝。

### 4.4 S4 异步 pause（需裁决，本期不做）

若采纳：`publish_captured` 移出 pause 关键路径，暂停在字节落本地并完成 in-flight 广告后即返回
成功；上传在后台进行，完成后 `complete_pause`。

**代价必须写清楚**：暂停成功不再意味着字节已持久。origin 在后台上传完成前死亡 ⇒ 那次暂停的
增量丢失，沙箱回退到上一个已发布快照。今天这只在同步上传失败时发生，改后**每次暂停都经过这个
窗口**。E2B 接受这个代价（K-4）。

## 5. 测试要求

- **S1**：状态合并前后 `LookupNode` 对未发布行的答案逐值不变；存量 `local_only` 行的 adopt 路径
  有测试；`refuse_unhonourable_pin` 的触发条件不变。
- **S2**：in-flight 键与 committed 键**互不污染**——一个只在 in-flight 存在的快照，
  按 committed 键查询必须查不到（守住 K-1）；`generation` 不同的两代不得互相命中；
  commit 后 in-flight 键被撤销。
- **S3**：三条路径各有用例——命中取件成功、等待中上传完成、超时后仍正确拒绝；
  **且"origin 已消失且 in-flight 取不到"必须仍然拒绝而不是倒带**（守住 K-3）。
- **对账**：未发布行在 K-2 的语义下不被误删（"已在别处恢复"与"未发布"必须可区分）。
- 回归面同仓库既有要求：`make fmt` / `clippy` / `test-unit` / `test-with-redis` /
  `check-crate-boundaries` / `-C services test`。

## 6. 与既有工作的关系

- 与正在实施的《放置资源打分》方案**无耦合**，不同分支。
- 与 E-1（恢复路径两次独立放置）**相关但不重叠**：E-1 是放置权威问题，本方案是字节可得性问题。
  两者都指向"恢复路径需要一次单独的整理"，但可以分别落地。

## 7. 待裁决

**S4 是否纳入。** 不纳入：本方案全部内容都是严格更安全的改进，pause 延迟不变。
纳入：pause 延迟下降一次对象存储写入的时间，代价是每次暂停都存在一个"字节尚未持久"的窗口。
