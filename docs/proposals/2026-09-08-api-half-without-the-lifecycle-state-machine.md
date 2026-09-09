# api 半边不再跑沙箱生命周期状态机

**日期**：2026-09-08
**参照实现**：e2b-dev/infra（本地 `/home/debian/e2b-infra`，只读引用）
**批次**：C6（`_scratch` 审查报告 §2 F2、附录 `review-boundary.md` C3、
附录 `review-tests-abstractions.md` §2.4/§4.1）
**前置**：C0（mock 门控）、C2（proxy 出 core）、C3（配置分半）、C4（模块出 core，本稿步骤 2 完成其余量）

## 1. 问题

`src/orchestrator/service.rs` 是一台 4,247 行的沙箱生命周期状态机，
`Orchestrator<S, F>` 两个类型参数：`S: MetadataStore`、`F: SandboxBackendFactory`。
两个二进制各实例化一份：

- `crates/aenv-node/src/bin/aenv-node.rs:264`：`InMemoryMetadataStore` +
  `FirecrackerSandboxFactory`，`sandboxes` 表里是真 VM 句柄。
- `crates/aenv-api/src/bin/aenv-api.rs:209`：`RedisMetadataStore` +
  `RemoteSandboxBackendFactory`，`sandboxes` 表里是**一次 gRPC 调用**
  （`RemoteSandboxStub`，`crates/aenv-api/src/node_client/stub.rs:52,650`）。

这个"换一个类型参数就把本地编排变成集群编排"的选择写在
`git show 3347c0c`（2026-08-21，`feat(node): drive a sandbox on another machine`）的
提交体里。该提交自己就列出了这一步换不掉的三件事：`build` 是同步的所以选节点必须挪进
`start`；持久化半边必须一起换；冷创建那条臂"没有东西可发"，只能 `refuse`。
换言之，**类型参数换掉的是 backend，换不掉的是"api 半边不该有 backend"**。

今天 api 半边这份实例上挂着的东西：

| 字段 | `service.rs` | api 半边上是什么 |
|---|---|---|
| `sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>` | :130 | 一张网络桩表，句柄不代表任何进程内资源 |
| `proxy_routes: RwLock<ProxyRouteTable>` | :131 | 永不被服务（`server.rs` 的 `new_control_plane_only` 不挂数据面） |
| `launch_claims: Arc<LaunchClaims>` | :158 | 进程内单激活，而真正的单激活是 binding store 的 `Starting` 预留 |
| `image_refs: Arc<dyn RuntimeImageRefs>` | :142 | `DisabledRuntimeImageRefs`（`src/image/contract.rs:103`） |

以及四个"拒绝式"实现，它们是同一个诊断的四份副本 ——
把一个**编译期可判定**的角色差异，降级成**运行时才报错**的差异：

| no-op | 位置 |
|---|---|
| `NoSnapshotCatalog` | `src/snapshot/repository/no_catalog.rs:17` |
| `DisabledRuntimeImageRefs` | `src/image/contract.rs:103` |
| `UnknownRecordOwner` | `crates/aenv-api/src/node_client/record_owner.rs:22` |

三个，不是四个。`EmptyRuntimeArtifactLease`（`src/runtime_snapshot.rs:90-101`）
连同它的 `default_runtime_artifact_lease` 已经门控在
`#[cfg(any(test, feature = "test-support"))]` 之下，不在任何发布二进制里，
因此不是运行期的角色降级，也不该被步骤 3 的判据点名。

`NoSnapshotCatalog` 也不能整体删除：节点侧有四个生产调用方
（`src/snapshot/repository/backends/{oss/durable.rs:46,posixfs/durable.rs:14}`、
`crates/aenv-node/src/snapshot/repository/backends/{oss/backend.rs:65,posixfs/backend.rs:96}`）。
要删的是 api 半边**装配**里那一个，不是这个类型。

`NoSnapshotCatalog` 的直接后果写在 `src/snapshot/repository/backends/mod.rs:66-72`：
api 半边先用 `build_catalog_only_storage` 造一个塞了 `NoSnapshotCatalog` 的
`SnapshotRepository`，唯一用途是把 artifact store 装在一个必须要有 catalog 的类型里搬运一次，
然后 `build_snapshot_backend` 立刻把它丢掉、只取 `.artifacts()` 重建。

判据缺失，但不是"零测试"：`crates/aenv-api/src/node_client/tests.rs` 有 57 个
`#[tokio::test]`，构造的正是 `Orchestrator<InMemoryMetadataStore,
RemoteSandboxBackendFactory>` 并驱动 create/pause/跨副本账本。真正为零的是
**Redis 元数据存储参与的那个组合**，而步骤 1 的新测试用的也是内存存储
（`crates/aenv-api/src/control_path_tests.rs` 开头写明了这一点），所以这个缺口在
C6 完成后依然为零。另一半是对的：`src/orchestrator/tests.rs` 用
`MockBackendFactory`，它与 `RemoteSandboxBackendFactory` 独有的四个开关
（`build` / `build_from_snapshot` / `build_from_snapshot_record` / `build_from_image_ref`，
`crates/aenv-api/src/node_client/factory.rs:52,65,76,139`）不相交；冷创建入口
`UnresolvedImage` 在那 6,580 行里出现 0 次，所以编排器层的冷创建此前确实无覆盖。

`docs/` 里没有任何一处论证过"两个半边共享一台状态机"。本稿是那个裁决。

## 2. e2b 的形状

e2b 的 api 半边也有一个叫 `Orchestrator` 的类型
（`packages/api/internal/orchestrator/orchestrator.go`），但它不是状态机：
它持有 `sandboxStore`、`nodemanager` 节点表、`routingCatalog`、`sqlcDB`，
**没有 VM 句柄表、没有代理路由表、没有 backend 工厂**。每个操作是一条
"放置 → store 写 → gRPC → store 写"的直线：

- `create_instance.go:197`：先 `o.sandboxStore.Reserve(...)` 拿到
  `finishStart`/`waitForStart` 一对闭包（占位先于放置，键是 `(teamID, sandboxID)`，
  与节点无关，实现是一段 Lua 原子脚本，
  `packages/api/internal/sandbox/reservations/redis/reservation.go:50`），再
  `placement.PlaceSandbox(...)`（:388）。并发同 id 拿到 `waitForStart` 去 join
  （:221-239），不由进程内 map；`finishStart` 在 :263-265 一次性收口。
  **`client.Sandbox.Create` 在 `PlaceSandbox` 里面，不在它之后**：
  `placement/placement.go:166 node.SandboxCreate(...)` →
  `nodemanager/sandbox_create.go:12 client.Sandbox.Create(...)`，
  失败按 gRPC code 分流并**换节点重试**（`placement.go:185-208`：
  `ResourceExhausted` 记 refusal 不计 attempt，其余把节点加入 `nodesExcluded`
  并 `attempt++`）。**记录写在节点调用之后**：`create_instance.go:496
  o.sandboxStore.Add(...)`，写失败时异步 kill 掉已经起来的沙箱（:503-518）。
  我们的 `place_new` + `reserve_placement` + `RemoteSandboxStub::start` 是三步分开的，
  重试语义因此不同——目标形状必须写明照抄哪一种（见 §3 的裁决）。
- `pause_instance.go:35`：`throttledUpsertSnapshot(buildUpsertSnapshotParams(...))`
  写 PG 行 → `snapshotInstance(...)` 一次 `client.Sandbox.Pause` gRPC →
  `finishSnapshotBuild(...)`。
- `delete_instance.go:26`：`o.sandboxStore.StartRemoving(...)` 拿到状态迁移与它的
  收口闭包，`removeSandboxFromNode(...)` 里 `routingCatalog.DeleteSandbox(...)`
  （:152）+ `client.Sandbox.Delete(...)`。
- `checkpoint_instance.go:30`：同样是 `StartRemoving` 一行拿到 `finishSnapshotting`
  闭包，其余是 upsert + 一次 `client.Sandbox.Checkpoint`。

**状态迁移在 e2b 住在 store 里，以两阶段闭包表达，而不是住在一台泛型编排器里、
以 backend 句柄表达**——但它有回滚臂，而且是显式的：`StartRemoving` 返回
四元组 `(sbx, alreadyDone, finish, err)`（`delete_instance.go:26`、
`checkpoint_instance.go:30`），`finish(err)` 就是那条臂，注释逐字写明
"On success (nil) it restores the sandbox to Running. On error it leaves the
state as Snapshotting"（`checkpoint_instance.go:47-57`），并按 gRPC code 分别
决定恢复 Running（:92、:97、:112）还是留在 Snapshotting 后 kill。
`packages/api/internal/sandbox/sandboxtypes/states.go:95-99` 是一张显式的
`AllowedTransitions` 表，`:11-20` 的 `TransitionEffect{Expires, Transient}`
就是"终态 / 成功后回到原状态"。

这条必须写准，否则 `SandboxControl` 会被实现成一条没有回滚臂的直线，把
`OrchestratorError::PausePublicationFailed`（发布失败把 VM 放回 Running，
CLAUDE.md 与 `src/orchestrator/service.rs:2060` 的 `rollback_pause_to_running`
都依赖它）丢掉。VM 自身的状态机住在 orchestrator 进程
（`packages/orchestrator/`），api 进程根本不链接它——这一半成立。

## 3. 目标形状

```
aenv-core            两半都跑的：SandboxMetadata、SandboxState、is_allowed_transition、
                     MetadataStore trait、OrchestratorError、快照 catalog 模型、
                     生成的 HTTP 面、server::compose、cfg 的共享半
crates/aenv-node     Orchestrator<S, F> 全部（真 VM、句柄表、proxy_routes、launch_claims、
                     fork/capture 的回滚臂），自己的 HTTP 装配（/health + /nodes* + 数据面）
crates/aenv-api      SandboxControl（放置 → binding 预留 → node gRPC → 一次 store CAS）、
                     api/impls、node_registry、binding_store、node_client、PG
```

`SandboxControl` 的每个方法是 e2b 那条直线**加上它的回滚臂**（见 §2 的 `finish`
闭包）：`PausePublicationFailed` 必须活下来。构造参数直接接收今天靠三个
`set_*` 后置注入的协作者（`set_pause_publisher` / `set_grant_issuer` /
`set_runtime_routing`，`service.rs:1629,1636,1643`）—— 一个装配完就不可变的类型
不需要 `OnceCell`，而那三个 `OnceCell`（`service.rs:147,150,154`）今天正是
"我是不是节点半边"的运行期判定：`service.rs:1654` 与 `:1817` 都以
`runtime_routing.get().is_none()` 分流。删掉它们等于消掉三处运行期角色分支。

**单激活的顺序：预留先于放置。** 今天是 `place_new → reserve_placement →
节点 create → record_placement`（`crates/aenv-api/src/control_path_tests.rs`
的冷创建用例断言的正是这个顺序），预留在选节点之后且键里含节点
（`node_registry/grpc_service.rs` 写 `Binding{node, execution_id, state: Starting}`）。
e2b 相反：`Reserve` 在选节点之前，键是 `(teamID, sandboxID)`、与节点无关。
本稿裁决**与 e2b 收敛**：把预留提到放置之前，键只含沙箱 id，节点在
`record_placement` 时才写进去。理由有三条，都不是审美：

1. 今天"先放置再预留"意味着并发的同 id 创建会各自先跑一遍放置——两次调度决策、
   两次资源乐观扣减，然后其中一个才在预留上失败。预留先行让第二个请求在拿到
   任何节点之前就知道自己是后来者。
2. 单激活因此有**一个**真相源。今天有两个：进程内的 `launch_claims`
   （`service.rs:158`）与 binding store 的 `Starting` 预留，前者只在一个副本内成立。
3. 于是步骤 3 **删掉 `launch_claims`**：它今天是唯一"先于任何分配拿 id"的东西
   （dev 的 406bc6f 就是为此而来），而预留提前之后，它守的正是预留在守的东西，
   只是守在单个进程里。`LaunchHeldElsewhere` + `LAUNCH_ELSEWHERE_POLL`
   （`service.rs:52,468,573`）的轮询是我们替代 e2b `waitForStart` join 的方式，
   保留，但它等待的对象改成预留而不是进程内 map。

**预留住在自己的键空间里，不是路由键里。** 本仓库的 `Binding` 必带节点：
`crates/aenv-api/src/binding_store/record.rs:99-107` 的 `parse_record` 在
node_id 或 endpoint 为空时返回 `None`，而这套值住在 gateway 路由投影读的同一个
键空间（`binding_key`，`record.rs:111` 的 `{prefix}:sandbox:{id}`）。所以
"键里不含节点"**不能**实现成往路由键里写一条无节点的 `Starting` 记录——那条记录
对 gateway 和 `get` 都等于不存在，单激活反而没了。预留改走并列的新键
（`{prefix}:reservation:{id}`），只有 api 半边读写；放置成功后仍按今天的形状写
路由键，`Starting` 因此只描述路由记录的一个阶段，不再兼任单激活的真相源。
e2b 的分法一样：预留在 `getReservationPrefix(teamID)` 下，沙箱记录在
`.../sandboxes/{id}`（`packages/api/internal/sandbox/reservations/redis/utils.go:20-36`
与 `.../storage/redis/utils.go:66-68`），共享的只有那个用于配额的团队索引。

预留已按此落地（`crates/aenv-api/src/binding_store/reservation.rs`），三个结果
claimed / held-elsewhere / expired，窗口取 `LAUNCH_RESERVATION_EXCLUSIVE_TTL`。
与 e2b 的一处偏离：e2b 的预留键还存放启动结果（`resultKey`），join 者从那里读；
我们的 join 者读的仍是共享元数据记录（`await_launch_elsewhere_until` 的
`LAUNCH_ELSEWHERE_POLL`），预留只回答"谁在启动"。理由是这条记录本来就是 resume
与 connect 的判据，再存一份结果会多出一个可以互相矛盾的真相源。

**排他区间是整条 launch，不是其中一段。** `SandboxControl::launch` 的第一件事是
`reserve_launch`——在读"这个 id 是否已被记录"之前；最后一件事是 `release_launch`——在
`store.add` 写下记录之后，成败两路都放。节点会话不再自己预留，`RemoteSandboxStub::start`
收到的是一个已经被持有的 id。e2b 是同一形状：`reserveScript` 把 storage index 的
`SISMEMBER` 与 pending 的 `ZSCORE` 判进同一段 Lua（`reservations/redis/scripts.go:37-46`），
`finishStart` 在 `defer` 里、晚于 `sandboxStore.Add`（`create_instance.go:496`）。
于是"读到无记录"这件事只可能发生在持有 id 的那个 launch 里。

节点侧的 `launch_claims` 仍然必要，理由收窄成两条，两条都是"预留的窗口是有限的"：

1. **窗口过期。** 冷创建的镜像拉取与转换可以超过
   `LAUNCH_RESERVATION_EXCLUSIVE_TTL`（120s，`binding_store/mod.rs:84`）。A 的预留到点被
   Lua 的 `PX` 删掉，B 拿到 `ClaimedFromExpired`；B 的放置若又选到同一节点 N
   （resume 带 `origin_node_id` 提示时尤其容易），N 上就同时有 execA 与 execB。
2. **持有者中途死亡。** 写下预留的 api 副本消失，它发出的节点 create 仍在进行；预留在
   120s 后被清扫器或下一个 `ClaimedFromExpired` 收走，下一个 launch 同样可能落到同一节点。

两条都是同一节点上两个不同 execution id 的并发 create，`launch_claims.claim`
（`crates/aenv-node/src/orchestrator/launch_claim.rs`）在分配任何资源之前取 id，是唯一
答得对的一层。跨节点那一条不在此列：它靠的是预留本身，而预留现在覆盖整条 launch。

**与 e2b 的偏离**：e2b 的 keep-alive **有**一段向持有节点的转发
（`keep_alive.go:60` → `update_instance.go:34,40`：`getOrConnectNode` 后
`client.Sandbox.Update`），因为它的节点自己持 deadline。我们没有这段代码可删——
`src/orchestrator/service.rs:1336` 的 `keep_alive_for` 全文只有 store 读写与
`runtime_confirmed_gone`，没有任何节点转发。真正的偏离是反向的、且是物理约束：
远端 create 发 `Expiry::CallerKept`（`crates/aenv-api/src/node_client/factory.rs:95,177`，
注释原文"The orchestrator owns expiry; the node must not invent another deadline"），
节点因此不持 deadline，也就不需要被通知。这条要写在目标形状里，不是"收敛"。

## 4. 步骤与各步删除的东西

### 步骤 1：先给 api 半边的真实装配补测试

在 aenv-api 里按 `aenv-api.rs` 的方式装配（`RemoteSandboxBackendFactory` + 真
`NodePlacement`/node-client 替身 + binding store），覆盖冷创建、resume、pause、
delete，以及每条路径上的节点侧失败。**删除：无。** 这是步骤 2/3 的安全网，
必须在改动前后都通过。

### 步骤 2：拆掉 `ApiImpl` 互锁，收尾 C4

（已落地。）node 为了 `server::new` 造一个 `ApiImpl`，而它服务的生成路由只有三条
（`/health`、`GET /nodes`、`GET|POST /nodes/{id}`），其余 25 条被角色门 404。
那个 `ApiImpl` 是 `src/api/impls` 留在 core 里的唯一原因；角色门本身
（`src/api/role_gate.rs`）随本步一起删除。

- node 自建路由（`crates/aenv-node/src/api/`），不再构造 `ApiImpl`；
- `git mv` `src/api/impls`、`src/node_registry`、`src/binding_store`、
  `src/node_client` 进 crates/aenv-api；
- 根 `Cargo.toml` 去掉 core 不再使用的 `kube`/`k8s-openapi`/`tonic` 服务端件；
- `CORE_EXILED_PATHS` 守卫加上新移出的路径，带双向变异证据。

**删除**：node 二进制里的 `api/impls`（5,528 行）、`node_registry`（4,667）、
`binding_store`（1,728）、`node_client`（2,462）与它们拖来的 kube 依赖链；
`ApiImpl` 在 node 上的构造与 `role_gate` 对生成路由的整层包裹。

### 步骤 3：api 半边的控制路径

引入 `SandboxControl`，逐族切换 handler，最后把 `src/orchestrator/` 移进
crates/aenv-node（只留 REST 层要映射的 `SandboxMetadata`/状态枚举/
`OrchestratorError` 形状在 core）。

**删除**：api 半边装配里的 `DisabledRuntimeImageRefs` 与 `UnknownRecordOwner`
两个 no-op，以及那个塞了 `NoSnapshotCatalog` 的 `RoleStorage`（类型本身留给节点侧，
见 §1）；`build_catalog_only_storage` 的"造了再丢"（`backends/mod.rs:68-76`）；
api 侧的 `sandboxes` 桩表、`proxy_routes`；**`launch_claims`**（连同它在 node 侧的
用法——预留提前之后单激活只剩一个真相源，见 §3 的裁决）；`RemoteSandboxBackendFactory::build`
那条"拒绝式"实现（`crates/aenv-api/src/node_client/factory.rs:52-64`）随 backend
工厂一起消失。三个 `set_*` 变成构造参数，`service.rs:1654`、`:1817` 的
`OnceCell` 分流随之消失。

`ApiImpl::runs_sandbox_runtime` / `owns_sandboxes` / `WakeSite` 与 `role_gate`
已在步骤 2 删除（commit 270d858），不再是本步的工作。

**本步已落地**：§3 的预留（新键、两后端契约、清扫器回收）与它在创建路径上的
位置（`RemoteSandboxStub::start` 先取 id 再选节点，落定后归还）；`facade.rs`
的拆分——`SandboxOrchestration` 是 REST 层与 observability 两个消费者要的那一份，
`NodeOrchestration` 是只有跑沙箱的进程能回答的十一个方法；以及
`SandboxControl`（`crates/aenv-api/src/control/`）本身。随它删掉的有：api 半边
装配里的 `DisabledRuntimeImageRefs`、`UnknownRecordOwner` 与 `RoleStorage` 的
造了再丢（api 半边改为 `build_catalog_backed_backend` 直接在
`build_artifact_store` 上组合 PostgreSQL），api 侧的 `sandboxes` 桩表与
`proxy_routes`，`launch_claims` 在 api 半边的用法，以及
`RemoteSandboxBackendFactory` 整个类型——`RemoteSandboxStub` 现在只是"一个沙箱的
节点会话"，经 `NodeLaunchBuilder` 取得，不再实现 `SandboxBackend`。
三个 `set_*` 也没了：`pause_publisher` 与 `grants` 成为 `Orchestrator::new` 的参数，
`RuntimeRouting` 这一路整个从 `Orchestrator` 移除（api 半边不再构造它，节点半边
自己的句柄表就是答案）。

`launch_claims` 在**节点侧**必须保留：它守的是同一节点上两个不同 execution id
的并发 create（dev 的 406bc6f），预留的窗口有限，两条到得了那里的路径写在 §3。

**与本稿的两处偏离，各有理由**：

1. `SandboxControl` 的每个操作都从路由绑定解析节点（`place_existing`），而不是
   从本副本的记忆里取句柄。今天的 api 半边在创建它的那个副本上会命中句柄表，
   在别的副本上早已走这条路；删掉句柄表等于让所有副本走同一条，代价是 delete
   与 pause 多一次 `describe`。
2. `SandboxOrchestration` 的三个 `*_for_test` 种子改为带"拒绝"默认体：另一个 crate
   里的实现者读不到 `aenv-core` 的 `test-support` feature 来给覆盖加门，而
   `--all-targets` 会编出一个看得见门控 trait、自己却没有 `cfg(test)` 的 lib。

### 步骤 3 的余量：模块归位

（已落地。）`Orchestrator<S, F>` 连同只有跑沙箱的进程才有的东西——`launch_claims`、
`launch_plan`、`proxy_routes`、内存元数据存储与那 6,097 行套件——进
`crates/aenv-node/src/orchestrator/`；`RedisMetadataStore`（4,671 行）与
`RuntimeRouting` 进 `crates/aenv-api/src/orchestrator/`。core 留下两半都读的那份模型：
状态、`SandboxMetadata`、`MetadataStore` 与它的契约套件、`OrchestratorError`、
`grants` 与 `pause_publisher` 的 trait、`launch_parts`，以及
`RestoredSandbox`/`LaunchHeldElsewhere`（前者是每个 restore 调用方的返回值，后者由
api 半边的放置源抛出）。

`SandboxOrchestration` 的 trait 留在 core（`SandboxControl` 实现它，
`src/observability/service.rs` 消费它），所以它的签名清单变成一个宏：core 把清单交给
一个 emitter，core 的 emitter 写 trait，`aenv-node` 的 emitter 写转发到
`Orchestrator` 的实现。一份清单，两边不会漂。`NodeOrchestration` 同法，清单与 trait
都在 `aenv-node`。

`redis` 变成 core 的可选依赖，挂在 `test-support` 下——core 里唯一还讲这个协议的是
`redis_test_server`，它为兄弟 crate 的测试起一个服务端。因此节点二进制不再链接任何
Redis 客户端，`cargo tree -p aenv-node -e normal` 少了十个 crate。

判据（`crates/aenv-api` 的十处编排替身）不是阻塞：十处全在 `#[cfg(test)]` 下，而
`crates/aenv-api/Cargo.toml` 的 `[dev-dependencies]` 早在步骤 2 就有
`aenv-node`。`make check-crate-boundaries` 读的是 `cargo tree -e normal`，dev 边不影响它。

**删除**：node 二进制里的 `RedisMetadataStore` 与 Redis 客户端；api 二进制里的
`Orchestrator`、内存存储、`launch_claims`、`launch_plan`、代理路由表与 iptables 施加面。

### 步骤 4：文档

CLAUDE.md 的 aenv-api 条、Orchestrator 条、Workspace Crates，以及讲"哪一半是二进制
的常量"的那句（今天在编排器层还有三处 `OnceCell` 例外，见 §6 第 2 条）；
`docs/src/internals/architecture.md` 的子系统表与树状图。

## 5. 回滚

**这一节的作用域是 C6 本身，不是承载它的分支。** C6：零迁移、零 schema 变更、
零 feature flag、零双写腿。两个二进制的线上契约（REST 面、`scheduler.v1`、
node gRPC 面、Redis 与 PG 的键与列）都不变——§3 的预留是**新增**的一个键，
路由键的键名与值形状一个字节都不动，所以 gateway 与旧副本读到的东西不变；
回滚后最多留下每个在途 create 一个孤儿预留键，按它自己的 TTL 过期。
所以回滚手段是**镜像 digest**：
把 Deployment/DaemonSet 的镜像换回上一版即可，不需要开关。这与 CLAUDE.md
"Rolling this half back is an image digest change on its Deployment, no flags"
是同一条。

承载它的分支不是这样，读者不要把上一段读成对整个分支的背书：
`crates/aenv-api/src/snapshot/repository/backends/postgres/migrations/0004_one_pause_per_sandbox.sql`
加了 `is_pause` 列与唯一部分索引，**软删了每个沙箱除最新 ready 之外的全部 pause 行，
并硬删了它们的别名**。回滚镜像不会把它们带回来：旧构建读"最新 ready 行"仍找到
幸存者，所以每个沙箱仍可 resume，但它会列出的那段 pause 历史没了。
列与触发器留在库里是无害的——`snapshots_pause_axis_trg` 正是为了让不认识
`is_pause` 的构建（也就是回滚目标）继续写出可被识别的 pause 行，
它的退役步骤写在 `services/README.md`。

## 6. 剩余耦合清单

步骤 3 之后 core 上还留着的、只有一个半边用得上的东西。每条都在 HEAD 上核过。

| # | 耦合 | 位置 | 为什么它属于这份清单 |
|---|---|---|---|
| 1 | ~~`src/orchestrator/store/redis/`（4,671 行）~~ 已搬进 `crates/aenv-api/src/orchestrator/store/redis/`，`redis` 同时变成 core 的可选依赖 | — | `check-crate-boundaries` 现在直接拒绝 `cargo tree -p aenv-node -e normal` 里出现 Redis 客户端 |
| 2 | ~~三个 `OnceCell`~~ 已删：`pause_publisher` 与 `grants` 是构造参数，`RuntimeRouting` 整条路径不在 `Orchestrator` 里了 | `src/orchestrator/service.rs` | — |
| 3 | ~~`SandboxOrchestration` facade（38 个方法）~~ 已拆：27 个方法的 `SandboxOrchestration` + 11 个方法的 `NodeOrchestration` | `src/orchestrator/facade.rs` | `SandboxControl` 只需实现前者；`check-crate-boundaries` 盯住 `ApiImpl` 持有的是哪一个 |
| 4 | ~~`src/sandbox/network/iptables_util.rs`（290 行）~~ 已搬：`policy.rs` 的 366-714 行（施加面）连同六个只有它用的链名常量成为 `crates/aenv-node/src/sandbox/network/policy_apply.rs`，`iptables_util.rs` 随之 | — | 施加面对模型半边零引用，唯一消费者是 `slot.rs`；三十七个测试项按同一条线分成 14 + 23 |
| 5 | ~~`src/secrets/mod.rs`（1,728 行）~~ 已搬进 `crates/aenv-api/src/secrets/service.rs`；`SecretKind` 留在 core 的 `src/secret_kind.rs` | core 消费者 `orchestrator/grants.rs`、`sandbox/network/policy.rs` | `API_EXILED_PATHS` 新增 `src/secrets`，双向变异证据在提交里 |
| 6 | ~~`RemoteSandboxBackendFactory::build` 的拒绝式实现~~ 已删：冷创建的拒绝现在是 `SandboxControl::plan_launch` 里 `SandboxLaunchSource::Image` 那一臂，返回 `InvalidRequest` | `crates/aenv-api/src/control/mod.rs` | — |
| 7 | ~~`crates/aenv-api` 的测试以 `Orchestrator` + `MockBackendFactory` 作编排替身~~ 十处全在 `#[cfg(test)]` 下，经 `[dev-dependencies]` 的 `aenv-node` 取得 | — | 不得依赖 aenv-node 的是**生产**边：`make check-crate-boundaries` 读 `cargo tree -e normal`。`node_client/tests.rs::real_node()` 在 api 的测试里起一个节点半边，现在写明它起的是谁的 |

## 7. 验收判据

1. 步骤 1 的测试在步骤 2、3 之后**逐字不改**仍通过；任何必须改的测试要在
   提交信息里写明为什么它测的是被删掉的形状。
2. `make fmt`、`make clippy`、`make check-crate-boundaries`、`make test-unit`
   全绿；测试计数守恒（移动不减少用例，新增单独计）。
3. `make test-with-redis`、`make test-with-postgres` 零 `SKIPPED`。
4. `cargo tree -p aenv-node -e normal` 不再含 `kube`/`k8s-openapi`；
   `cargo tree -p aenv-api -e normal` 的依赖数下降。
5. 步骤 3 完成后，api 半边的装配（`crates/aenv-api/src/bin/aenv-api.rs`）不再构造
   `DisabledRuntimeImageRefs`（今天在 :213）、`UnknownRecordOwner`
   （今天经 `node_client/factory.rs:34` 的默认值）与带 `NoSnapshotCatalog` 的
   `RoleStorage`（今天经 `backends/mod.rs:68-76`）。
   **不是**"全树 grep 无命中"：`NoSnapshotCatalog` 有四个节点侧生产调用方，
   `EmptyRuntimeArtifactLease` 已门控在 `cfg(any(test, feature = "test-support"))`
   之下（见 §1）。
6. 新增的模块级守卫有双向变异证据（破坏 → 出现 FAILED 行；复原 → ok；
   `git status` 干净）。

判据现状：六条全部成立。1：步骤 1 的十四个用例逐字未改（模块归位也没动它们，
`InMemoryMetadataStore` 经 `crates/aenv-api/src/orchestrator/` 的一条
`#[cfg(test)] pub use` 到位），另新增九个。2：测试计数守恒逐 crate 核过——搬走的
用例在收方一个不少地出现。4：`cargo tree -p aenv-node -e normal` 不含
`kube`/`k8s-openapi`/`sqlx`/`redis`，677 → 667；`cargo tree -p aenv-api -e normal`
528 → 527。6：新增的三条守卫（core 的五条路径、api 的两条、每半边自有的
`orchestrator` 模块、node 树里的 Redis 客户端）各有双向变异证据。

§6 只剩第 5 条一类的既定分工，没有待办。
