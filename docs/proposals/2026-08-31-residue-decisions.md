# 过渡期残留 D1-D8 裁决记录

**日期**：2026-08-31
**仓库基线**：AgentENV `0e29426`（分支 `chore/residue-decisions`）
**参照实现**：e2b-dev/infra `fdc33599b`（本地 `/home/debian/e2b-infra`，只读引用，未复制代码）

这是一批清理决定的裁决记录。每节一条：裁决内容、依据、以及本轮从 e2b 取到的对照观察。

---

## D1 — 删除无调用者的 `ListNodes` RPC，保留 `Schedule`

`scheduler.v1.Scheduler.ListNodes` 全树零调用者：gateway 的 `GET /nodes*` 走
`ListObservedNodes`/`GetNode`，aenv-api 自己的放置走进程内接缝
`src/node_client/native_placement.rs`。`Schedule` 有真实调用者，保留。

删除范围：proto 里的 rpc 与它专属的两个 message（`Node` 是共享类型，保留）、
`src/node_registry/grpc_service.rs` 的 server 实现与测试、
`src/observability/reporter.rs` 的 trait stub、gateway 测试里的 stub 客户端方法、
`services/gateway/internal/execution_fencing_test.go` 冻结方法表里的表项。
Go binding 由 `make -C services proto` 重新生成；Rust proto 在 build 时生成。

顺带修正：`deploy/k8s/base/kustomization.yaml` 里那段注释原先声称 gateway 会发
`Schedule`。它不会——非生成 Go 代码里 `grep -rn '\.Schedule(' services/` 无命中。

**e2b 对照**：e2b 同类面上确实有清单 RPC——`packages/orchestrator/orchestrator.proto:261`
的 `rpc List(google.protobuf.Empty) returns (SandboxListResponse)`——但它有活的调用者
（`packages/api/internal/orchestrator/nodemanager/sandboxes.go:36`）。节点状态那条同形
（`packages/orchestrator/info.proto:87`，调用者
`packages/api/internal/orchestrator/nodemanager/status.go:146`）。e2b 的 proto 里没有
"无仓内调用者的清单 RPC"；我们有过一条。

---

## D2 — 折叠只有一个取值的 `{"upstream"}` 标签

`agentenv_gateway_rest_upstream_total` 的 `upstream` 标签只有一个可达取值：本包任何构建
都无法把面向用户的 REST 交给某个 node 处理。标签描述的维度不变化，因此折叠为普通
Counter，与本文件里另一条单维计数器 `gatewayColdLookupTimeout` 同形。

**指标名不变，标签维度消失。** 任何按
`agentenv_gateway_rest_upstream_total{upstream="api"}` 选择的看板/告警在折叠后匹配不到
任何序列，必须去掉该 selector；裸序列携带同样的数值。本批次里没有其它指标名或标签集变化。

**e2b 对照**：e2b 的单维计数器直接不带 attribute——
`packages/api/internal/orchestrator/evictor/evict.go:178`（`fsOnlyAutoPauseCounter.Add(ctx, 1)`）、
`packages/api/internal/orchestrator/create_instance.go:572`
（`resumeOriginNodeRemapCounter.Add(wctx, 1)`）——而同一文件里真正会变化的那条才传
attribute（`create_instance.go:410`，`metric.WithAttributes(attributes...)`）。同一条规则：
维度只在会变化的地方标注。

---

## D3 — 拆分前回滚叙述退役

回滚到 `e2734fd`（Go scheduler 删除）之前不再是受支持的操作。真实回滚目标是**当前架构的
某个更早镜像 digest**，2026-08-31 已在 pve-mf 上实操验证过。因此
`deploy/k8s/base/kustomization.yaml`、`deploy/docker-compose.yml`、`services/README.md`、
`docs/src/deployment/kubernetes.md` 里的拆分前回滚叙述缩成一句话，操作细节移到本文件。

### 退役掉的那套程序是什么

从 `deploy/k8s/base` 出发，回到"每台机器上一个进程同时服务面向用户的 REST"曾经要求：

1. DaemonSet 换成**拆分前的镜像 tag**（crate 拆分之前最后一个构建，那个二进制还认
   `--role all`）；
2. 同一次 apply 里把 `GATEWAY_REST_UPSTREAM_ADDR` 指回节点；
3. gateway **自己的镜像 tag 也要钉回**到 `Config.Validate` 拒绝空 REST upstream 之前的
   构建——否则当代 gateway 在该键为空时直接拒绝启动，而不是启动后每个 REST 调用 502；
4. `[cluster].scheduler_endpoint` 与 api Deployment 的
   `AENV_PAUSED_REGISTRY_BACKEND` 一起回滚；
5. 若要连 scheduler 一起复活，还要从 git 历史里恢复
   `scheduler-service.yaml`/`scheduler-deployment.yaml`/`scheduler-pdb.yaml` 以及一个
   兼容的 scheduler 镜像。

### 为什么退役

- 代价：一次串行 DaemonSet 滚动、每台机器一次 drain，**加上**两个互相钉死的镜像 tag。
  两个 tag 必须同一次 apply 落地，任何一半落空就是没人选择过的半迁移状态。
- `--role`/`AENV_ROLE` 已被参数解析直接拒绝，`services/scheduler` 与
  `deploy/docker/Dockerfile.scheduler` 是从树里删掉的，不是留着没人用。
- `http://agentenv-scheduler:9090` 不再解析到任何东西；把相关键指回去只会把心跳与
  P2P peer discovery（两者共用 `[cluster].scheduler_endpoint`）变成 DNS 失败。
- 真实使用的回滚——换一个更早的当前架构镜像 digest——不需要上面任何一步。

---

## D4 — shadow 打分：保留，并给一个 owner 和一个复查条件

保留 `src/node_registry/placement/` 的 shadow 打分，它是 e2b 对齐工作
（`docs/proposals/2026-08-30-e2b-alignment-placement-scoring.md`）的交付物，而不是过渡
残留。但 shadow 状态不再无限期开放：

- **Owner**：suixinio。
- **复查条件**（满足任一即复查是否翻默认）：集群规模超过 2 个节点；或者集群级
  pending-assignment 记账被排期。后者是 `src/node_registry/placement/mod.rs` 模块文档
  已经写下的前置条件——单靠心跳快照落后于并发 create，会把突发流量赶到同一个陈旧的
  最小值上。
- **2026-08-31 pve-mf 实测**：21 次 create 之后
  `agentenv_api_placement_shadow_pressure_spread` 直方图**一条都没记**。该直方图只在样本内
  `Scored` 候选 ≥2 时记录（对齐规格 §4，`scored_in_sample >= 2`），而两节点集群极少满足。
  在当前集群形状下这条指标对"打分分歧有多大"不给任何信号。
- **e2b 参照实现**：`packages/api/internal/orchestrator/placement/placement_best_of_K.go`——
  `Score` 在 `:33`，pending 资源在 `:37-43` 被加进已分配量，无放回采样在 `:145`。上面那条
  复查条件正对应 `:37-43`：e2b 的打分从一开始就读 pending 量，我们要翻默认就得先有它。

---

## D5 — "scheduler" 这个名字的约定

`scheduler` 指 aenv-api 服务的 **scheduler.v1 协议面**：config 键、proto、Role 名、指标名
一律保留这个词。描述**行为主体**的散文说 "the api half"。CLAUDE.md 的 Distributed Control
Plane 一节记下了这条约定。

配套修一处：`agentenv_scheduler_catalog_rpc_total`（名字按上述约定保留）此前没有 HELP，
补上 `describe_counter!`，名字与标签不变。

---

## D6 — `.tasks/` 进 gitignore

`.tasks/` 是工作单与本地跑出来的日志/指标快照，不进版本库。

---

## D7 — 守住 api 半边的控制面凭据投影

api 半边上，`api-gate-token` 这道闸是面向用户 REST 唯一的传输层检查，而此前没有任何东西
断言 api Deployment 真的挂载了它。新测试放在 `src/api/control_plane_gate.rs` 的测试模块里，
形状对齐 `the_gateway_and_the_node_read_different_keys_of_the_credential_secret`：
`include_str!` 读 `deploy/k8s/base/agentenv-api-deployment.yaml`，锚在 manifest 语法上而非
裸子串，并且把断言限定在那一个 volume 内，使兄弟投影无法替它满足条件。变异证据记在提交
信息里。

---

## D8 — api 半边回滚指针

CLAUDE.md 里"Rolling this half back is an image-tag change; see `services/README.md`"
此前指向一个不存在的小节。`services/README.md` 补一个两行小节：api 半边回滚是
Deployment 镜像改动（tag/digest），没有开关，与 node 半边同机制，并指向 digest 钉法。
