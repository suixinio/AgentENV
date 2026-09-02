# 暂停模型一步对齐 e2b：pause 即死亡，resume 即 create

**日期**：2026-09-02（方向稿，未实施）
**基线**：`dev`@`7067ae2`
**参照实现**：e2b-dev/infra `fdc33599b`（本地 `/home/debian/e2b-infra`，只读引用，未复制代码）
**取代**：`2026-09-01-origin-as-hint-resume.md` 的阶段 A/B/C 与「刻意不照抄 e2b 的两处」。
A/B 已合入的代码不是本稿的前提，而是本稿删除清单的一部分。
**并行**：`2026-09-01-bidirectional-reconciler.md` 不受影响；`2026-09-01-reserve-before-runtime.md`
的占位机制是本稿单活不变量的载体。
**前提**：系统尚未上线。没有存量 `paused_sandboxes` 行需要迁移，没有过渡期，没有回退腿。

## 1. 决定

暂停沙箱不再是沙箱的一个状态，而是沙箱的死亡加一条快照目录行。恢复不再是重开同一谱系，
而是从那条行 create 一个新 execution。origin 是放置偏好，不是权威。`paused_sandboxes` 表、
它的五态、租约、回收、claim CAS、节点侧 paused 记录，整个删除。

这不是「按 e2b 抄一遍」。§3 是逐行核对 e2b 之后得到的事实，§5 是拿这些事实对本稿初版
（本轮对话里的四段口头方案）做的对抗审查，其中五处推翻了初版。

## 2. 为什么现在做而不是分三阶段

前篇把 A（capture 缺失降级）、B（origin 降为提示）、C（pause 即 publish）排成递进，理由是
零迁移与状态机逐步收窄。两个理由都随「未上线」失效：

- 零迁移承诺保护的是存量行。没有存量行。
- 逐步收窄保护的是并行的两种语义不出现窗口。一步到位没有窗口。

而分阶段的代价是真实的：A/B 已合入的代码（`missing_local_verdict` 的重建分支、
`RecordAbsent`、`lookup.rs` 的 `Resuming` 拆分、warm 判据改写）全部是终态下不存在的
状态上的修补。留着它们，就是给一个即将删除的状态机继续付维护费。

## 3. e2b 的暂停模型：逐行核过的事实

每条一处 file:line，按 `fdc33599b`。

**E1 pause 是 RemoveSandbox 的一个动作。** `handlers/sandbox_pause.go` 调
`orchestrator.RemoveSandbox(…, StateActionPause)`；`orchestrator/delete_instance.go:26`
`StartRemoving` 把活跃 store 记录 CAS 到 `pausing`（transition key 保护），
`:105` `defer o.sandboxStore.Remove(...)` **无条件**删记录，随后 `:106`
`removeSandboxFromNode` 才去打节点。`removeSandboxFromNode` 里 `:152`
`routingCatalog.DeleteSandbox` 先拆路由，再 `pauseSandbox`。

**E2 快照行先于节点动作写入，且是「模板 + 构建」。** `pause_instance.go:35`
`throttledUpsertSnapshot` 在打节点之前 upsert：`db/queries/snapshots/create_new_snapshot.sql`
以 `sandbox_id` 为冲突键，首次暂停建一个私有 env（`source='snapshot'`），之后每次暂停
复用同一 env 只写新 build；build 状态先 `snapshotting`（`pause_instance.go:161`），节点
Pause 成功后 `finishSnapshotBuild(success)`（`:69`），失败则 `failSnapshotBuild`（`:56`）。
行里带 `origin_node_id`（`:160`）、`config`（网络、autoResume 政策、volume 挂载，`:151-158`）、
`auto_pause`、`metadata`。

**E3 pause 失败即沙箱死亡，不回滚到 running。** 节点侧 `server/sandboxes.go`
`Pause`：`EnsurePausable` 失败则 `stopSandboxAsync` 杀掉并返回 Internal；成功路径
`defer s.stopSandboxAsync`。api 侧 E1 的 `Remove` 是 defer，无论节点答什么记录都没了。
唯一的例外是 `dberrors.IsForeignKeyViolation`（基础模板已删）时显式 kill
（`delete_instance.go:175`）。

**E4 节点 Pause 返回时上传未完成。** `snapshotAndCacheSandbox`（`server/sandboxes.go:1395`）
把快照加进本地 template cache 后立即返回，`uploadSnapshotAsync`（`:1510`）在
`uploadsWG` 里异步上传。优雅关闭 `Server.Close`（`server/main.go:292`）等 `uploadsWG`
排空。另一道缓解：`template/cache.go:182-186` 在 `PeerToPeerChunkTransferFlag` 下把
persistence 包成 `peerclient.NewRoutingProvider`，「allows pulling data directly from the
peer before GCS upload completes」。

**E5 resume 是 create，前置只查活跃 store。** `handlers/sandbox_resume.go:69-110`：活跃
store 有记录时按状态答——`pausing` 等 `WaitForStateChange` 后继续、`killing` 404、
`snapshotting` 409、`running` 409；无记录则 `snapshotCache.Get`（`:123`）取最后快照，
`get_last_snapshot.sql:8` 只接 `status_group='ready'` 的 build，然后 `CreateSandbox`
带 `isResume=true`、`nodeID = snap.OriginNodeID`（`:304`）。`create_instance.go:196`
`Reserve` 先于一切；同 id 并发撞 `ErrAlreadyExists` 则 `waitForStart` 等首个结果。

**E6 origin 是偏好，且自我纠正。** `create_instance.go:372-380`：origin 缺席或
`!CanAcceptNewRequests()` 则 `node = nil` 走正常放置；`:389-396` 放置超时且被暖的节点
不是 origin 时 `maybeRemapResumeOriginNode` 改写行（`update_snapshot_origin_node.sql`）。

**E7 快照行不随 resume 删除。** resume 全程没有删行的调用；行是「最后一次快照」，
下次暂停 upsert 覆盖。因此 running 沙箱可以同时有一条快照行，
`handlers/sandboxes_list.go:138-192` 列表时把 running id 从 paused 页排除并解释了
为什么必须这样（`:93-124`）。`handlers/sandbox_kill.go:39-104` DELETE 先 kill
running（不存在则记 debug 继续），再无条件 `deleteSnapshot`（软删 env、失效缓存），
两者都没有才 404。

**E8 autoResume 政策存在快照行，不存在路由。** `handlers/proxy_grpc.go:99-125`
`getAutoResumeSnapshot`：gateway 未命中来的 `ResumeSandbox` 读 `snap.Snapshot.Config.AutoResume`，
非 `Any` 答 NotFound。路由 catalog 在 E1 已拆。

**E9 节点关机不暂停沙箱。** `server/main.go:337` `DrainSandboxes`：等本机沙箱
「exit on their own」，只等不动手；`ctx` 到期即放弃。启动时 `startupreclaim` 扫进程表
杀孤儿 firecracker（`startupreclaim/firecracker.go:21`）。节点没有任何形式的沙箱持久化。

**E10 节点侧没有 paused 沙箱这个对象。** Pause 后 `sandboxFactory.Sandboxes` 里没有它；
本地留下的是 template cache 里以 build id 为键、带 TTL 的条目（`template/cache.go:219`
`AddSnapshot`），是缓存不是记录，丢了从存储层重拉。

## 4. AgentENV 终态

### 4.1 数据

**不建新表。** `crates/aenv-api/src/snapshot/repository/backends/postgres/migrations/0001_initial_schema.sql:90`
的 `snapshots` 已经是 E2 的形状：`source_kind='sandbox'` 加 `source_sandbox_id`（对应
e2b 的 `snapshots.sandbox_id`），`status_group`（对应 `env_builds.status_group`），
`published OR origin_node_id IS NOT NULL`（对应 `origin_node_id`），`sandbox_started_at_ms`，
`snapshots_source_sandbox_idx` 索引。「暂停沙箱 X」的定义就是：

> `source_sandbox_id = X AND deleted_at_ms IS NULL` 中 `created_at_ms` 最大的那一行，
> 且 `status_group = 'ready'` 时可恢复。

`paused_sandboxes` 表与 `paused_registry_grace` 表删除。需要补进 catalog 行的只有
sandbox 级配置：网络策略、`auto_resume`、custom extension params、attached drives、
资源规格。它们已在 `SandboxMetadata` 里，落到行的 `committed_payload` 或一个新的
`sandbox_config JSONB` 列，由实施时定。

**活跃 store（Redis）** 状态收为 e2b 的四个：`running`、`pausing`、`snapshotting`、
`killing`，外加我们已有的占位 `creating`。`Paused`、`Resuming` 删除。`Pausing` 是
transient-to-removal：结束即删记录。

**binding / 路由投影** 在 pause 时拆除（E1）。gateway 未命中走既有 `ResumeSandbox`
冷路径，api 侧改读 catalog 行的 `auto_resume`（E8）。

**节点** 不持有任何 paused 记录。`records/<id>.json` 删除。本地 capture 目录降级为
以 `snapshot_id` 为键的缓存（E10），可被容量或 TTL 清理，清掉只影响暖恢复速度。

### 4.2 流程

**pause**：api 在活跃 store 上 CAS `running → pausing`（既有 transition key）；
在 catalog 写一行 `source_kind='sandbox'`、`status='building'`、`origin_node_id=<node>`；
打节点 `Pause`；节点本地落 capture 后立刻应答，异步 `stage` 上传；api 收到应答即拆
binding、删活跃 store 记录；上传完成后 `commit_staged` 把行翻 `ready`。节点 Pause 失败
（含 seal 失败）一律 kill（E3），行翻 `error`，沙箱不回 running。

**resume**：先查活跃 store（E5 的四分支，`pausing` 走 `wait_while_in_states`）；无记录则
读 catalog 最新 ready 行，没有则 404；然后走 **create** 路径：占位（reserve-before-runtime
的 `Starting` binding）→ 放置（origin 可调度则偏好，否则任意）→ 节点 `Create` 带既有的
`SnapshotSource`（`node.proto:281`）→ 成功后若落点 ≠ origin 则改写行的
`origin_node_id`（E6）。resume 不删行（E7）。

**DELETE**：kill running（不存在则继续），再软删该 sandbox 的全部 catalog 行；两者都无
才 404（E7）。

**GET / list**：running 从活跃 store；paused 从 catalog 最新 ready 行，排除 running id（E7）。

**节点关机**：只 drain（E9）。运维需要清空节点时，通过 api 对该节点的沙箱逐个发起普通
pause，写行的是 api。节点重启后存量运行沙箱一律回收（`node_reclaim` 已有）。

**`/sandboxes-cold`**：语义变为「不启动，直接在 catalog 造一条 sandbox 来源的 ready 行，
其内容引用模板快照的层」。需要 catalog 支持引用型提交或复制 manifest，是本稿唯一需要
新设计的数据面点，见 §7。

### 4.3 单活不变量

执法者是活跃 store 的占位（`Starting` binding 的 incarnation 仲裁），不是 catalog 行。
resume 就是 create，同 id 并发被占位挡住。前篇「刻意不照抄」保留的 claim CAS 与
binding 都不再需要。

## 5. 对抗审查：初版方案 vs e2b 事实

初版指本轮对话里的口头方案。每条：攻击点 → e2b 事实 → 判决。

| # | 初版说法 | e2b 事实 | 判决 |
|---|---|---|---|
| R1 | PG 行收窄为 sandbox_id / snapshot_id / origin / metadata 加「一个上传状态」 | E2：快照就是模板加 build，可恢复性由 build 的 `status_group='ready'` 表达 | **推翻**。不建新表，catalog `snapshots` 已有全部轴（§4.1）。「上传状态」就是既有 `status_group` |
| R2 | 单活靠 Reserve，不靠 PG 行 CAS | E5：resume 先查活跃 store 再 `Reserve`，同 id 并发等首个 | **成立**。偏离一处：我们的占位撞上是拒绝（409），e2b 是等待；沿用 reserve-before-runtime 的裁决 |
| R3 | 未提 pause 失败的归宿 | E3：失败即死亡，记录无条件删除 | **补正**。`capture_snapshot` 的「可恢复失败回滚到 Running」只保留给 Checkpoint（e2b 的 `snapshotting → running`），pause 失败一律 kill |
| R4 | 上传窗口内节点死亡等于丢失，直接接受 | E4：两道缓解——关机等上传排空、P2P 从 origin 直拉 | **修正**。接受丢失仍是判决，但两道缓解都有同位物：优雅关闭等 stage 排空（节点已有 P2P/ublk 的分步关机预算，加一项）；iroh P2P 已能按 artifact 键拉层，resume 的 `SnapshotSource` 解析可选先问 P2P。后者列为加固项，不阻塞 |
| R5 | 隐含 resume 后行消失（否则 running 与行并存要有状态） | E7：行不删，list 排除 running id，DELETE 两步 | **推翻**。行是「最后一次快照」，下次 pause 覆盖。这正是我们 `running` 态在 PG 里想表达的东西，e2b 用「行 + 活跃 store 并存」表达，不需要状态 |
| R6 | 优雅关闭「暂停并持久化」要重画成 stage 加 commit | E9：节点关机不暂停，只 drain；持久化不存在 | **推翻**。`SandboxPersister` 的 paused 半边整个删除，不重画。清空节点是 api 侧动作 |
| R7 | binding 拆掉后 autoResume 唤醒失去载体 | E8：政策在快照行，路由在 pause 时已拆 | **成立且更简单**。`auto_resume` 已在 `SandboxMetadata`，随行落库。偏离清单 #10（autoResume:false 的 running 必须可路由）不受影响，running 由活跃 store 应答 |
| R8 | origin 落在别处就改写 | E6：只在放置超时且暖了别的节点时改写 | **等价**。我们改写条件取「resume 成功落点 ≠ origin」，比 e2b 宽但语义一致（字段跟随现实） |
| R9 | 节点「忘掉」沙箱 | E10：本地留 template cache，TTL，缓存语义 | **补正**。capture 目录不删，改为 snapshot_id 键的缓存，与正确性无关 |
| R10 | 未提 pausing 期间的并发 resume / 二次 pause | E5：`pausing` 等状态变更；E1 transition key 去重 | **已具备**。`store/redis/transition.rs` 与 `notify.rs` 是同构物，无新工作 |
| R11 | `/sandboxes-cold` 「从模板写一条快照行」 | e2b 无同位物 | **保留为设计项**。冷沙箱没有自己的字节，行必须引用模板层；catalog 今天没有引用型提交（§7） |
| R12 | 删除清单未提心跳 | 节点无 paused 沙箱可报 | **补正**。`scheduler.proto:149-151` 的 `paused_*` 计数、`:225` 的 `paused` 点名位、api 侧 `discard` 一族全部删除 |
| R13 | 2026-08-25 尽调说「删除批 = 在 Redis 重写分布式协议」，与本稿冲突？ | 该结论的前提是 `paused_sandboxes` 的写路径语义要保留 | **不冲突**。本稿删的是语义本身，不是搬家。那份尽调仍然正确地否定了「迁到 Redis」这条路 |
| R14 | resume 时 origin 不健康「不再等 90s 死亡判定」 | E6：`CanAcceptNewRequests()` 即时判定 | **成立**。判定源是 node registry 的观测状态，不是租约 |
| R15 | 未提团队校验 | E5 逐处校验 `TeamID` | **非目标**。无租户模型，见 `2026-09-01-client-proxy-api-alignment.md` §6 |

审查结论：初版的方向正确，但五处（R1、R3、R5、R6、R9）细节与 e2b 相反或缺失，
其中 R1 和 R5 改变了数据设计，R6 删掉了初版认为「唯一要新设计」的点。

## 6. 施工清单

按 crate，删除优先。行数为 2026-09-02 实测非空行，含文件内测试。

**删除**
- `src/orchestrator/paused_registry/`（1,021）；`crates/aenv-api/src/orchestrator/paused_registry/`（4,600，含 contract 测试 1,561）；`paused_sandboxes`、`paused_registry_grace` DDL。
- `src/api/impls/paused_recovery.rs`（2,681）、`paused_coordinator.rs`（2,383）整文件。
- `src/api/impls/resume_surface.rs`（2,118）中 `ResumePlacement::LocalOnly`、`Pinned`、`PinRefusal`、`WakeSite` 的本地/远端区分。终态 `ResumeWiring` 只剩「放置源 + 投影写回」。
- `src/orchestrator/persistence/file_backed.rs`（1,173）的 paused 记录半边；`records/` 目录约定。
- `src/node_client/paused_state.rs`（154）；`node.proto` 的 `Resume` RPC 与 `PausedSandboxCapture` 载荷（`Create` 带 `SnapshotSource` 已覆盖）。
- `SandboxState::{Paused, Resuming}`、`spends_lifetime`、`PausedHandle`、`PausedStateRef`。
- `scheduler.proto` 的 `paused_sandbox_count`、`paused_allocated_*`、`paused` 点名位；`grpc_service.rs` 的 `paused_entry` 与 discard 判定。
- 前篇 A/B 落地的 `RecordAbsent`、`missing_local_verdict` 重建分支、`lookup.rs` 的 `Resuming` 拆分。
- `config/default.toml` 的 `[orchestrator.paused_registry]` 整节与 `AENV_PAUSED_REGISTRY_BACKEND`，记入 `docs/src/configuration/env-vars.md`。

**改写**
- `src/orchestrator/service.rs` `pause_sandbox`：按 §4.2 的顺序；失败 kill。`capture_snapshot` 保留回滚。
- `src/api/impls/sandbox.rs` 的 resume / GET / list / DELETE：按 §4.2。resume 复用 create 的放置与占位，只是来源换成 `SnapshotSource`。
- `crates/aenv-api` 的 catalog：`latest_ready_for_sandbox(sandbox_id)` 一条查询；`origin_node_id` 改写一条写入；sandbox 级配置落列。
- `aenv-node` 的 `Pause`：本地落 capture 后应答，stage 异步；关机预算加「等 stage 排空」一步。capture 目录改 snapshot_id 键。
- gateway `resume` 客户端与 `apiproxy.proto`：应答不再区分 pinned，只剩「running 在哪 / 已唤醒在哪 / 拒绝」。
- e2e：暂停相关断言按新语义重写；`tests/fixtures/sandbox_metadata_full.json` 去掉 paused 字段。

**新增（唯一）**
- `/sandboxes-cold` 的引用型提交（§7）。

## 7. 开放设计项

1. **冷沙箱的 catalog 表达。** 选项 a：catalog 提交允许 `layers` 引用另一条快照的层而不复制（overlaybd 层本就内容寻址，物理上零拷贝）；选项 b：复制模板 manifest 成独立提交。倾向 a，但要核对 GC 的引用计数。
2. **catalog 增长。** paused 行不再有 `sandbox_expires_at` 之类的到期。e2b 同样没有自动清理，靠 DELETE。是否给暂停行一个集群级保留期，与租户模型一起裁决。
3. **P2P 直拉（R4）。** 加固项，独立排期。

## 8. 接受的取舍

- 上传完成前 origin 硬死，该沙箱丢失，resume 404，GET 报 error 状态的快照行。与 e2b 一致。
- 节点重启，存量运行沙箱死亡。pve-mf 已经接受过一次（`2026-08-31-node-embedded-db-removal.md`）。
- 没有「只能在 origin 恢复」的状态。用户感知的是 resume 慢一点（远端拉层）或 404，不再有 500。
- 并发 resume 第二个得 409 而不是等待。

## 9. 验收判据（pve-mf）

1. 暂停后 `paused_sandboxes` 不存在（表已删），catalog 出现 `source_sandbox_id` 行且 `status_group` 最终 `ready`；节点上 `records/` 不存在。
2. 人为删除 origin 节点的 capture 缓存目录后 resume 成功（终态的金判据，沿用前篇）。
3. 人为把 origin 置 draining 后 resume 落到另一节点，行的 `origin_node_id` 改写；再置 ready 后下一次 resume 回到暖节点。
4. 上传完成前杀 origin Pod：resume 404，GET 报不可恢复，DELETE 204。
5. 暂停途中并发 resume：等 pausing 结束后成功；并发 pause：第二个拿到第一个的结果。
6. `GET /sandboxes` 一个刚 resume 的沙箱只出现一次。
7. autoResume:true 的暂停沙箱，直接打数据面地址被唤醒；autoResume:false 的得 404。
8. e2e 基线：暂停套件全绿，其余套件不变。

## 10. 非目标

租户校验、快照保留期策略、filesystem-only 暂停、volume 挂载。前三项各有归属方案，
第四项 AgentENV 用 attached drives 表达，已在 `SandboxMetadata` 里随行落库。

## 11. 风险

- 这是拆分以来最大的一次删除，横跨两个 Rust 二进制、一个 Go 二进制、两份 proto、e2e。
  缓解：先删后改，先让 `make check-crate-boundaries`、`make test-unit`、`make -C services test` 在删除态全绿，再改写流程。
- `resume_surface.rs` 和 gateway 冷路径刚在 P4 收口（`790620b`），本稿再改一次应答形状。缓解：apiproxy 应答只减枚举值不加，Go 侧按 `2026-09-01-client-proxy-api-alignment.md` §4 的偏离清单同步更新第 1、3 条。
- `/sandboxes-cold` 的引用型提交若做成选项 b，会让冷沙箱复制模板层的 manifest；层本身仍零拷贝，风险仅在 GC 引用计数。
