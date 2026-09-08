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
  （`RemoteSandboxStub`，`src/node_client/stub.rs:650`）。

这个"换一个类型参数就把本地编排变成集群编排"的选择写在
`git show 3347c0c`（2026-08-21，`feat(node): drive a sandbox on another machine`）的
提交体里。该提交自己就列出了这一步换不掉的三件事：`build` 是同步的所以选节点必须挪进
`start`；持久化半边必须一起换；冷创建那条臂"没有东西可发"，只能 `refuse`。
换言之，**类型参数换掉的是 backend，换不掉的是"api 半边不该有 backend"**。

今天 api 半边这份实例上挂着的东西：

| 字段 | `service.rs` | api 半边上是什么 |
|---|---|---|
| `sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>` | :129 | 一张网络桩表，句柄不代表任何进程内资源 |
| `proxy_routes: RwLock<ProxyRouteTable>` | :130 | 永不被服务（`server.rs` 的 `new_control_plane_only` 不挂数据面） |
| `launch_claims: Arc<LaunchClaims>` | :158 | 进程内单激活，而真正的单激活是 binding store 的 `Starting` 预留 |
| `image_refs: Arc<dyn RuntimeImageRefs>` | :142 | `DisabledRuntimeImageRefs`（`src/image/contract.rs:103`） |

以及四个"拒绝式"实现，它们是同一个诊断的四份副本 ——
把一个**编译期可判定**的角色差异，降级成**运行时才报错**的差异：

| no-op | 位置 |
|---|---|
| `NoSnapshotCatalog` | `src/snapshot/repository/no_catalog.rs:17` |
| `DisabledRuntimeImageRefs` | `src/image/contract.rs:103` |
| `EmptyRuntimeArtifactLease` | `src/runtime_snapshot.rs:92` |
| `UnknownRecordOwner` | `src/node_client/record_owner.rs:22` |

`NoSnapshotCatalog` 的直接后果写在 `src/snapshot/repository/backends/mod.rs:66-72`：
api 半边先用 `build_catalog_only_storage` 造一个塞了 `NoSnapshotCatalog` 的
`SnapshotRepository`，唯一用途是把 artifact store 装在一个必须要有 catalog 的类型里搬运一次，
然后 `build_snapshot_backend` 立刻把它丢掉、只取 `.artifacts()` 重建。

判据缺失：`Orchestrator<RedisMetadataStore, RemoteSandboxBackendFactory>` 这个组合
全 workspace 零测试。`src/orchestrator/tests.rs` 的 6,580 行用的是
`MockBackendFactory`，它实现的工厂方法与 `RemoteSandboxBackendFactory` 独有的四个开关
（`build` / `build_from_snapshot` / `build_from_snapshot_record` / `build_from_image_ref`，
`src/node_client/factory.rs:52,65,76,139`）不相交；冷创建入口 `UnresolvedImage`
在那 6,580 行里出现 0 次。

`docs/` 里没有任何一处论证过"两个半边共享一台状态机"。本稿是那个裁决。

## 2. e2b 的形状

e2b 的 api 半边也有一个叫 `Orchestrator` 的类型
（`packages/api/internal/orchestrator/orchestrator.go`），但它不是状态机：
它持有 `sandboxStore`、`nodemanager` 节点表、`routingCatalog`、`sqlcDB`，
**没有 VM 句柄表、没有代理路由表、没有 backend 工厂**。每个操作是一条
"放置 → store 写 → gRPC → store 写"的直线：

- `create_instance.go:187`：先 `o.sandboxStore.Reserve(...)` 拿到
  `finishStart`/`waitForStart` 一对闭包（占位先于运行时），再
  `placement.PlaceSandbox(...)`（:365），再 `client.Sandbox.Create(...)`。
  并发同 id 由 `waitForStart` 等待，不由进程内 map。
- `pause_instance.go:31`：`throttledUpsertSnapshot(buildUpsertSnapshotParams(...))`
  写 PG 行 → `snapshotInstance(...)` 一次 `client.Sandbox.Pause` gRPC →
  `finishSnapshotBuild(...)`。
- `delete_instance.go:22`：`o.sandboxStore.StartRemoving(...)` 一次 store 调用完成状态迁移，
  `removeSandboxFromNode(...)` 里 `routingCatalog.DeleteSandbox(...)` +
  `client.Sandbox.Delete(...)`。
- `checkpoint_instance.go:30`：同样是 `StartRemoving` 一行拿到 `finishSnapshotting`
  闭包，其余是 upsert + 一次 `client.Sandbox.Checkpoint`。

**状态迁移在 e2b 是一次 store 调用**（`StartRemoving` / `Reserve`），
不是一台带回滚臂的状态机。VM 的状态机住在 orchestrator 进程
（`packages/orchestrator/`），api 进程根本不链接它。

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

`SandboxControl` 的每个方法是 e2b 那条直线，构造参数直接接收今天靠三个
`set_*` 后置注入的协作者（`set_pause_publisher` / `set_grant_issuer` /
`set_runtime_routing`，`service.rs:1629,1636,1643`）—— 一个装配完就不可变的类型
不需要 `OnceCell`。

**与 e2b 的偏离**：e2b 的 api 半边没有等价于我们 `keep_alive_for` 里那段
"记录不在本进程时向持有节点转发"的逻辑，因为它的 `sandboxStore` 就是集群真相；
我们的 `RedisMetadataStore` 同样是集群真相，所以这条转发在目标形状里消失，
不是移植过去。这是收敛，记录在此以免被读成漏移。

## 4. 步骤与各步删除的东西

### 步骤 1：先给 api 半边的真实装配补测试

在 aenv-api 里按 `aenv-api.rs` 的方式装配（`RemoteSandboxBackendFactory` + 真
`NodePlacement`/node-client 替身 + binding store），覆盖冷创建、resume、pause、
delete，以及每条路径上的节点侧失败。**删除：无。** 这是步骤 2/3 的安全网，
必须在改动前后都通过。

### 步骤 2：拆掉 `ApiImpl` 互锁，收尾 C4

node 今天为了 `server::new` 造一个 `ApiImpl`（`aenv-node.rs:355`），而它服务的
生成路由只有三条（`src/api/role_gate.rs:55-63`：`/health`、`GET /nodes`、
`GET|POST /nodes/{id}`），其余 25 条被 404。这个 `ApiImpl` 是
`src/api/impls` 留在 core 里的唯一原因。

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

**删除**：四个 no-op impl；`RoleStorage` 与 `build_catalog_only_storage` 的
"造了再丢"（`backends/mod.rs:66-72`）；api 侧的 `sandboxes` 桩表、`proxy_routes`、
`launch_claims`；`ApiImpl::runs_sandbox_runtime` / `owns_sandboxes` 与 handler 里
9 处运行期角色分支（角色成为二进制常量）；`WakeSite`（若无剩余读取方）。
三个 `set_*` 变成构造参数。

### 步骤 4：文档

CLAUDE.md 的 aenv-api 条、Orchestrator 条、Workspace Crates、
"runtime-role distinction survives in one place" 那句，
以及 `docs/src/internals/architecture.md`。

## 5. 回滚

零迁移、零 schema 变更、零 feature flag、零双写腿。两个二进制的线上契约
（REST 面、`scheduler.v1`、node gRPC 面、Redis 与 PG 的键与列）都不变，
所以回滚手段是**镜像 digest**：把 Deployment/DaemonSet 的镜像换回上一版即可，
不需要开关，也不存在"回滚后数据读不回来"的方向性。这与 CLAUDE.md
"Rolling this half back is an image digest change on its Deployment, no flags"
是同一条。

## 6. 验收判据

1. 步骤 1 的测试在步骤 2、3 之后**逐字不改**仍通过；任何必须改的测试要在
   提交信息里写明为什么它测的是被删掉的形状。
2. `make fmt`、`make clippy`、`make check-crate-boundaries`、`make test-unit`
   全绿；测试计数守恒（移动不减少用例，新增单独计）。
3. `make test-with-redis`、`make test-with-postgres` 零 `SKIPPED`。
4. `cargo tree -p aenv-node -e normal` 不再含 `kube`/`k8s-openapi`；
   `cargo tree -p aenv-api -e normal` 的依赖数下降。
5. `grep -rn "NoSnapshotCatalog\|DisabledRuntimeImageRefs\|EmptyRuntimeArtifactLease\|UnknownRecordOwner" src/ crates/`
   在步骤 3 完成后无生产命中。
6. 新增的模块级守卫有双向变异证据（破坏 → 出现 FAILED 行；复原 → ok；
   `git status` 干净）。
