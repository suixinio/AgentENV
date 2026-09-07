# 出口凭据 broker 每节点化：执行计划（第三版）

**日期**：2026-09-06。**基线**：`dev`@a9ee98d（= `origin/dev`）。**替代**：
`2026-09-03-sandbox-egress-credential-brokering.md` 的 §1 决定 1 后半句、C2、§4.4、§4.10、§5.2、§5.8、§8。
**参照**：e2b-infra baab220（`/home/debian/e2b-infra`）、CubeSandbox v0.6（`/home/debian/CubeSandbox-latest`）。
本文是实现 agent 的唯一规格；行号按 a9ee98d 核实，以当前代码为准。

## 0. 裁决与不变量

四条裁决（用户，2026-09-06）：

1. **边界是 guest**：凭据值不进沙箱、沙箱内检测不到值；node 可以持有。root 进程不在威胁模型内。
2. **每节点 broker**，不再有集群级 `remote`。理由：节点级故障域、去掉传输 PKI 与 HMAC、容量随节点扩展。
3. **独立 DaemonSet**，不是 node Pod 的 sidecar：我们的 Firecracker 是 aenv-node 子进程，Pod 重建即杀沙箱，
   只有独立 DaemonSet 保得住"broker 重启不杀沙箱"。
4. **传输是同节点 Unix socket**，删 `remote`、HMAC、nonce。这是简化不是加固：它们防的横穿集群明文与
   重放在 UDS 上不存在，对 root 进程本来就不设防。

两个配套条件，v1 必做，第二条是准入门槛：

- **每节点一张短期中间 CA**，由 aenv-api 用根签发，根私钥只在 aenv-api；**guest 信任根**（不是本节点中间），
  否则 pause/resume 漂移后启动时一次性装载信任库的进程（Node.js、Go、JVM）会带着旧节点中间证书穿过内存快照。
- **取值身份按节点限定**：resolve 只回答绑定在该节点上的沙箱的 grant。目标集群落不下就不切换形态。

一个阻断级前置：guest 经 `veth_host_ip` 今天可达 node 的 8000/8001/9103，8001 gRPC 无凭据且含
`update_network`、`delete`、`pause`（`crates/aenv-node/src/node_server/service.rs:589-1001`），guest 无需逃逸就能
给自己去掉 rules。P0 先堵。

原样保留、不得退化：listener 建在沙箱 netns 内（`crates/aenv-node/src/sandbox/network/slot.rs:459-479`）、
accept 即身份、DNAT 端口粒度、SNI 分流、未命中不解密透传、UDP 443 拒绝、tap0 防伪
（`src/sandbox/network/policy.rs:538-545`）、G8（`crates/aenv-egress/src/policy.rs`）、grant 模型、CA 每次 init
下发（`crates/aenv-node/src/sandbox/firecracker/sandbox.rs:644,679-700`）、broker 不可达时"accept 即关"
（`crates/aenv-node/src/sandbox/egress/mod.rs:221-224`，**不改成关 listener**）、`requires_egress_broker` 放置过滤
（`src/binding_store/lookup.rs:94-125`，语义改为"本节点 broker 健康"，**过滤本身保留**）、
`check-crate-boundaries` 里 aenv-node 不链接 `tls`（`Makefile:138-145`）。

硬约束（违反即返工）：TLS 栈只有 openssl/native-tls；生成目录只重生成不手改；测试放它所属的 crate；
`CLAUDE.md` 注释规则；每阶段一次提交、Conventional Commit、不带 attribution 行；不 push。
Go 侧改了 proto 或 `config/default.toml` 后 `make -C services test` 带 `-count=1`。

## P0 前置加固（M，独立可合）

**P0.1 guest 到 node 的路径。** `src/sandbox/network/policy.rs:553-561` 的两条 `/32 ACCEPT`：

- `veth_host_ip/32`：改为只放行明确端口。brokered 路径不需要它（listener 在 `169.254.0.22`，nat PREROUTING 先于
  filter）。本期先加一条带 `-m comment` 的计数规则放在它前面并在 debug 日志里导出计数，规则本身改成
  `-p tcp -m multiport --dports <空>` 即等效删除；若集成测试或 e2e 发现合法消费者（自定义扩展的
  `hostInteractionIp` 是 host→guest 方向，不算），再按端口放行。
- `guest_dns_ip/32`：改为 `-p udp --dport 53` 与 `-p tcp --dport 53` 两条；`BaseSandboxNetworkPolicy::Deny`
  下同样只放 53。

`policy.rs:1002-1910` 的命令序列断言相应改写，加"53 以外端口不在 ACCEPT 里"的断言。

**P0.2 node gRPC 鉴权。** `crates/aenv-node/src/node_server/mod.rs:65-75` 挂 tonic interceptor：复用
`src/api/control_plane_gate.rs` 的 `node-gate-token` 语义（`agentenv-daemonset.yaml:556-580` 已投影该文件），
每个 RPC 检查 `x-agentenv-control-plane` metadata；文件不存在时保持今天的开放行为并 `warn!` 一次（与 REST 门禁的
"optional Secret ⇒ 门禁关"同形）。api 半边的 node client（`src/node_client/stub.rs`）带上同一令牌。
单元测试：有令牌通过、错令牌 `Unauthenticated`、无文件放行。

**P0.3 永久拒绝表。** `src/cfg/network.rs:24-32` 的 `always_denied_cidrs` 改为常量（现值已含
`10/8, 100.64/10, 127/8, 169.254/16, 172.16/12, 192.168/16`），补 `::1/128, fc00::/7, fe80::/10`；新增
`[network.egress].allow_internal_cidrs: Vec<String>`，只能是常量表的子网，启动时 `info!` 每一条；
`config/default.toml:355-366` 与 `docs/src/configuration/reference.md:281` 改写；`always_denied_cidrs` 进
`docs/src/configuration/env-vars.md` 的 removed 行（"设了会怎样：启动拒绝并指向 allow_internal_cidrs"）。
测试：`allowOut 10.0.0.0/8` 仍拒 `10.255.255.254`（`crates/aenv-node/tests/integration/fc.rs:584` 已有），
`allow_internal_cidrs` 不在常量表内 ⇒ 校验失败。

**P0.4 IPv6。** `src/sandbox/network/policy.rs:631-646` `try_normalize_ip_or_cidr` 拒绝 V6 并返回带文案的错误；
`slot.rs` 建 netns 时 `sysctl -w net.ipv6.conf.all.disable_ipv6=1`（`iptables_util.rs` 同样的能力抬升方式）；
guest 内核参数加 `ipv6.disable=1`（`slot.rs:433-442` 的 `ip=` 旁）。测试：V6 字面量 ⇒ 400。

**P0.5 broker 拒 TRACE。** `crates/aenv-egress/src/handlers/http.rs:195-205` 只拒 CONNECT；加 TRACE ⇒ 405，
`x-aenv-egress-reason: method-not-brokered`。单元测试。

**P0.6 文档化契约。** `src/api/openapi.yml:317,360` 的 allowOut 描述删掉域名宣称，写明"域名条目返回 400，见
v1.1"；`src/api/impls/sandbox.rs:175` 写死的 `allow_public_traffic: Some(true)` 改为：请求为 `false` ⇒ 400
"allowPublicTraffic=false is not supported yet"，直到 T1 落地。`make agentenv-server` 重生成。

**P0.7 Firecracker seccomp 断言。** `crates/aenv-node/src/sandbox/firecracker/instance.rs:99-129` 的参数构造抽成
可测函数，单元测试断言不含 `--no-seccomp`。

验收：`make test-unit`、`make clippy`、`make fmt`、`make check-crate-boundaries`、`make -C services test -count=1`
全绿；集成测试新增（本机跑不了，标"需 pve-mf"）：guest 对 `veth_host_ip` 的 8000/8001/9103 与对 DNS 服务器的
非 53 端口全部不可达；guest 直连 node gRPC `update_network` 得 `Unauthenticated`。

## P1 契约：`local` 传输（M）

`crates/aenv-egress`：

- `Cargo.toml`：删 `remote` feature 及 `native-tls`/`tokio-native-tls` 在 `remote` 下的引用；新增
  `local = []`（只需 `tokio` 的 `net` feature，已有）；`bin = ["tls", "resolver", "local", ...]`；
  `tls` 保留叶签发、`http` handler、上游 TLS，**删掉 broker 侧 TLS 监听**。
- `src/header.rs:31-48`：删 `nonce`、`hmac` 字段与 `base64_nonce`、`NONCE_LEN`、HMAC 签名/校验、
  `ReplayCache`；`IDENTITY_HEADER_VERSION` 升 2，broker 拒绝未知版本并回 `Ack{accepted:false, reason:"unsupported header version"}`。
  `v` 是帧版本，node 与 broker 谁先滚由它裁：**broker 先于 node 滚**。
- `src/transport.rs`：删 `RemoteTransport`；新增 `LocalTransport { socket_path }` 实现 `BrokerTransport::open`：
  `tokio::net::UnixStream::connect`，写帧，读 `Ack`，返回流。`EmbeddedTransport` 不动。
- `src/runtime.rs:23-37,123-149,160-165`：`Options` 去 keys；`admit` 去 verify 与 replay；`run` 改收
  `tokio::net::UnixListener`；accept 后 `peer_cred()` 取 uid/gid，与 `Options::expected_peer_uid`
  比较（node 的 uid，见 P2；配置项 `[listen].peer_uid`），不匹配立即关；`admission_full` 与每沙箱二级限额
  （新增 `Options::per_sandbox_connections`，按 `IdentityHeader.sandbox_id` 计数）。
- `src/main.rs:198-206`：删空 keys 即 bail；`[listen]` 改为 `socket_path`（默认 `/run/aenv-egress/broker.sock`）
  与 `peer_uid`；启动时 `unlink` 残留 socket，`bind` 后 `chmod 0660`。
- 单元测试：`header.rs`、`runtime.rs` 里签名/重放/TLS 监听的约 20 个测试删除或改写；新增：tempdir 里建 UDS，
  错误 uid 被关、版本不符被拒、每沙箱限额生效。**不写可执行文件**。

`crates/aenv-node`：

- `Cargo.toml:20` `features = ["local"]`；`src/sandbox/egress/mod.rs` 的 transport 构造按 `mode` 选
  `Embedded | Local`；删 `RemoteTransport` 分支与 `shared_secret`/`ca_cert_path` 的读取。
- `src/cfg/egress_broker.rs:78-115`：`mode` 枚举 `disabled | embedded | local`；`remote` ⇒ 启动拒绝，错误文案
  指向 `docs/src/configuration/env-vars.md` 的迁移条目（与 `AENV_SECRETS_BACKEND=vault` 同形）；
  `local` 需要 `socket_path`；`embedded` 校验条件不变。
- `Makefile:143` 的 feature 循环改为 `core resolver local tls`；`check-crate-boundaries` 加断言：aenv-node 不链
  `tls`（不变）且 aenv-egress 的 `local` 单独可编译。
- `docs/src/configuration/env-vars.md`：`AENV_EGRESS_BROKER_{ENDPOINT,CA_CERT_PATH,SHARED_SECRET}`、
  `AENV_EGRESS_{MAX_SKEW_MS,REPLAY_CAPACITY,TLS_CERT_PATH,TLS_KEY_PATH}` 各一条 removed 行；Go 守卫按
  `services/shared/config/gateway_removed_keys_manifest_test.go` 形制扫 `deploy/k8s/base` 与 `deploy/docker-compose.yml`
  （本期先写守卫，P2 改清单后它才能过；同一提交里把清单一起改掉）。

验收：四个 make 门禁全绿；`cargo test -p aenv-egress --features bin` 与默认 feature 各一遍。

## P2 节点与部署（M 到 L）

- **DaemonSet** `deploy/k8s/base/aenv-egress-daemonset.yaml`（新）：专用 SA `aenv-egress` + 投影 token
  （audience `aenv-api`）；`runAsUser: 65532`、`readOnlyRootFilesystem`、`drop: ALL`；`resources` requests 等于 limits
  （初值 cpu 500m / memory 512Mi，P4 后按压测改）；`priorityClassName` 与 node 相同；readiness 探
  `socket_path` 可连；`updateStrategy.rollingUpdate.maxUnavailable: 1`；volume：hostPath
  `/run/aenv-egress` `DirectoryOrCreate`。指标端口保留，`aenv-egress-networkpolicy.yaml` 改为只放行 Prometheus。
- **node 侧目录**：aenv-node 启动时建 `/run/aenv-egress`，`chown root:65532`、`chmod 0750`（node 是 root，
  `agentenv-daemonset.yaml:421-422`），DaemonSet 挂同一 hostPath。broker 只要能 `bind`，socket 文件 0660。
  peer_uid 在 broker 侧配置为 0（node 的 uid）；这只挡非 root 旁路进程，**不是对 root 的边界**，文档如实写。
- **健康与心跳**：`crates/aenv-node/src/sandbox/egress/mod.rs:156-204` 的探测改为周期性 `UnixStream::connect` +
  空帧握手；`src/observability/model.rs:20-22` 的 `EgressBrokerState` 加 `LocalOk | LocalUnreachable`；
  `services/api/proto/scheduler.proto:162-168` 加 `EGRESS_BROKER_STATE_LOCAL_OK = 5`、`..._LOCAL_UNREACHABLE = 6`，
  `make -C services proto` 重生成；`src/node_registry/filter.rs:41-43` 的 `can_broker()` 认 `LocalOk`；
  **api 先于 node 滚**（旧 api 读未知枚举按 0 处理 ⇒ 503）。
- **REST 可观测**：`/nodes` 响应加 `egressBroker` 字段（`src/api/openapi.yml` 的 node schema，重生成）；
  api 半边指标 `agentenv_node_egress_broker{node,state}` gauge。
- **清单连锁**：删 `aenv-egress-deployment.yaml`、其 Service；`agentenv-daemonset.yaml:253-272,489-508` 删
  `ENDPOINT/CA_CERT_PATH/SHARED_SECRET` env 与 `egress-ca`、`egress-hmac` 挂载，加 `/run/aenv-egress` hostPath；
  `deploy/k8s/base/kustomization.yaml:26-33,448-451` 与 `deploy/k8s/overlays/pve-mf/kustomization.yaml:49-52,74-81`
  同一提交改（overlay 里指向 Deployment 的 patch 必须删，否则 kustomize build 失败）；
  `services/shared/config/execution_switches_manifest_test.go:236-262` 的引用计数按实际结果调整；
  `deploy/docker-compose.yml:23` 加 `aenv-egress` 服务与共享 socket volume，node 用 `local`。
- **集成测试**：`crates/aenv-node/tests/integration/egress.rs:31-38` 的 `require_embedded_broker()` 改为
  `require_local_broker()`：Makefile 新增 `build-egress` 目标（`cargo build -p aenv-egress --features bin`），
  `AENV_EGRESS_BINARY_PATH` 指向产物，测试起 broker 子进程走 tempdir 里的 UDS（像 ublk daemon 那样注入）；
  `tests/fixtures/egress-local-overlay.toml` 新建，`make test-agent-integration` 的第二次 cargo 调用改用它。
  新增回归测试：`src/binding_store/lookup.rs:509` 附近加"preferred 节点无 broker 时被跳过"。

验收：四个门禁 + `make -C services test -count=1` + `kustomize build deploy/k8s/overlays/pve-mf` 成功；
集成测试编译通过，标"需 pve-mf"。

## P3 CA 与身份，准入门槛（L）

**P3.1 aenv-api 的 CA 模块**（`crates/aenv-api/src/egress_ca/`，新）：用 `openssl`（已在依赖树，
`crates/aenv-api/Cargo.toml:27`）实现 `issue_node_intermediate(node_id) -> (cert, key)`：7 天有效、
`basicConstraints CA:TRUE pathlen:0`、`nameConstraints` **排除集** `.svc`、`.cluster.local`、`.local`、`.internal`、
`10/8`、`172.16/12`、`192.168/16`、`100.64/10`、`169.254/16`；根从 Secret `egress-ca` 读（复用，作根）。
**不链接 aenv-egress 的 `tls` feature。**

**P3.2 鉴权端点**：`POST /internal/egress/intermediate` 与现有 `POST /internal/credentials/resolve`
（`crates/aenv-api/src/secrets/pg/resolve_route.rs:43-92`）共用一个鉴权层：请求带投影 SA token；api 用 `kube`
（`Cargo.toml:39`）调 `TokenReview`，取 `authentication.kubernetes.io/pod-name` extra，再 `get pods` 读
`spec.nodeName`，等于 `AENV_NODE_ID`（`agentenv-daemonset.yaml:149-152`）。RBAC：新增 ClusterRole
`tokenreviews create` + ClusterRoleBinding 给 `agentenv-api` SA（`deploy/k8s/base/role.yaml:11-27` 只有 namespaced）。
`static` 发现模式下这两个端点关闭（返回 503 `resolve disabled in static mode`），只允许 `embedded`。
resolve 只认投影 token：每个被放行的调用方都指名一台机器（收口批删掉了共享 bearer 的并存窗口）。

**P3.3 节点限定 resolve**：`resolve_route.rs` 接 `BindingStore::get(sandbox_id)`（`src/binding_store/mod.rs:87-92`）；
`Binding.node.id != caller_node_id` ⇒ 404；binding 的 `execution_id` 非空且不等 ⇒ 404；store 不可判定 ⇒ 503，
broker 读作 outage 合成 502（`crates/aenv-egress/src/resolver.rs:81,84` 的错误映射加 `Unavailable` 分支）。
撤销时主动清 broker 缓存：api 在 `revoke` 后不能推送（无反向通道），改为缓存 TTL 从 30s 降到 10s，文档如实写。

**P3.4 broker 自取与热替换**：`crates/aenv-egress/src/tls.rs:66-91,122-160` 的 `CaSigner` 改为
`ArcSwap<CaSigner>`（`arc-swap` crate，无 TLS 依赖）；`main.rs:208-222` 启动时向 api 取中间证书，取不到 ⇒ 规则域名
回 502 `no-intermediate`（不拒启）；剩余 1/3 时续期，换签清叶缓存；叶 `not_after = min(now + leaf_ttl,
issuer.not_after − 1h)`（`tls.rs:129,199-200`）；叶缓存键加签发者序列号；剩余不足 24h 打 `warn!` 并置指标
`egress_intermediate_expires_seconds`。握手链 = 叶 + 中间（`tls.rs:217-218` 已追加签发者证书，换成中间即可）。
`sandbox.rs:696-700` 的 `assert_guest_trusts` 匹配根 PEM 不变。

验收：单元测试覆盖签发（约束项存在、pathlen 0）、TokenReview 桩、绑定比对三种结果、叶 TTL 上限、缓存键；
e2e 三条（写好，标"需 pve-mf"）：pause 后 resume 到另一节点，resume 前启动的长驻 Node.js 进程仍能经规则域名
完成 TLS；节点 A 的 token 取节点 B 沙箱的 grant 得 404；中间轮换后已缓存的名字仍能握手。

## P4 审计与观测（S 到 M）

`crates/aenv-egress/src/handlers/http.rs` 每请求写一行 JSON 到 stdout（`tracing` 的独立 target `egress.audit`）：
`ts, node_id, sandbox_id, execution_id, dst_ip, dst_port, scheme, host, method, path(≤1 KiB，不含 query),
status, bytes_in, bytes_out, latency_ms, tls_version, cipher, upstream_addr, rule, injected_headers(只记名)`；
`security_event`（default-deny、host/SNI 不符、G8 拒绝、inject 触发）与 `tls_handshake`（握手失败）两类事件同 target。
`[audit].level = metadata | none`（节点级）。指标增删表进 `docs/src/concepts/egress-credentials.md`：
删 `egress_replay_rejected_total`，新增 `egress_intermediate_expires_seconds`、`egress_peer_rejected_total`、
`agentenv_node_egress_broker`。每沙箱出口计数 `egress_conns_total{sandbox}` 只在 debug 级别导出，避免基数爆炸。

## T1 入口令牌（M，与 P0 到 P5 并行，不依赖 broker 形态）

- `src/api/impls/sandbox.rs:175`：`allowPublicTraffic=false` ⇒ 生成 UUID v4 `traffic_access_token`，随创建响应
  返回（openapi `Sandbox` schema 加字段，与 e2b `spec/openapi.yml` 同名），持久化进 metadata 与
  `PausedSandboxConfig`；要求 `secure=true`，否则 400（e2b `sandbox_create.go:283-287` 同）。
- `src/api/proxy.rs`：非 envd 端口且沙箱有 token 时，检查 `e2b-traffic-access-token` 或
  `x-agentenv-traffic-access-token`，`subtle::ConstantTimeEq`，缺失/不符 ⇒ 403 带 `x-aenv-proxy-reason`；
  日志里 token 打码；预检请求同样 403（e2b `proxy.go:100-103` 的理由）。同一处屏蔽 envd 内部路径
  `/init /collapse /freeze /fsfreeze /fsthaw /unfreeze /upgrade`（e2b `internal_routes.gen.go`）。
  `services/gateway` 只透传这两个头，不做判断（`internal/server.go:29-31` 的注释改写）。
- 每沙箱入站连接上限 `[api.proxy].max_incoming_per_sandbox`，默认 0 = 不限。
- e2e：默认可达；锁定后无头 403、有头 200；envd 端口不受 token 约束但内部路径被拒。

## P5 收尾（M）

代码与文档部分（本分支完成）：

- `docs/proposals/2026-09-03-sandbox-egress-credential-brokering.md`：§1 决定 1 后半句、§4.4、§4.10、§5.2、§5.8、
  §8 各加一行"被第三版（`_egress-per-node-implementation.md`）推翻"，不删原文。
- `CLAUDE.md` 第 102 与 112 行改写为每节点形态（改写不追加，保持 200 行内）；
  `docs/src/concepts/egress-credentials.md` 部署章节、失败表、guest 可见差异（链长 2、issuer CN 为节点名）、
  排障顺序一节；`docs/src/configuration/reference.md` 的 `[egress_broker]`、`[network.egress]`。
- e2e：`scripts/tests/e2e/suites/15_egress_credentials.sh` 的 `rollout restart deploy/aenv-egress` 改
  DaemonSet；停机注入几经修正，最终形态是**把沙箱所在节点的 broker socket 文件挪走**
  （删 Pod 的窗口被 DaemonSet 十秒补回；`kill -STOP 1` 对容器 PID 1 是空操作）。沙箱所在节点不查
  `/registry/sandboxes`（那是暂停沙箱的注册表），改用"逐个 broker token 试 resolve，恰好一个 200"；
  `16_egress_postgres.sh:122-131` 的 503 重试注释改为"全部节点 broker 未就绪"；新增 T1 与 P3 的断言。
- 三张回滚表写进 `docs/src/internals/services.md`："Rolling the egress broker back"：node、broker、api 各自
  需要什么对象在场。

集群操作部分：**用户裁决 2026-09-07，从旧形态升级是破坏性变更，没有迁移路径**。
没有迁移脚本、没有中间 mode、没有共享 bearer 的回滚窗口。步骤是"清空沙箱 → 删旧对象族 → apply 终态 →
等三个 rollout"，全文在 `docs/src/internals/services.md` 的 "Bringing the per-node broker up"。
依赖顺序从源头拆掉了：broker 的 init container 自己准备 `/run/aenv-egress`，节点不必先滚一遍；
节点在 `local` 下等不到这个目录就启动失败，而不是静默 `local_unreachable`。

## 涉及文件清单

| 区域 | 文件 |
|---|---|
| 网络 | `src/sandbox/network/policy.rs`、`src/cfg/network.rs`、`crates/aenv-node/src/sandbox/network/slot.rs` |
| node gRPC | `crates/aenv-node/src/node_server/{mod,service}.rs`、`src/node_client/stub.rs`、`src/api/control_plane_gate.rs` |
| broker | `crates/aenv-egress/{Cargo.toml,src/header.rs,src/transport.rs,src/runtime.rs,src/main.rs,src/tls.rs,src/resolver.rs,src/handlers/http.rs}` |
| node 运行时 | `crates/aenv-node/{Cargo.toml,src/sandbox/egress/mod.rs,src/sandbox/firecracker/instance.rs}`、`src/cfg/egress_broker.rs` |
| api | `crates/aenv-api/src/{egress_ca/,secrets/pg/resolve_route.rs}`、`src/api/{openapi.yml,impls/sandbox.rs,proxy.rs}`、`src/observability/model.rs`、`src/node_registry/filter.rs` |
| proto | `services/api/proto/scheduler.proto` 及重生成产物 |
| 部署 | `deploy/k8s/base/{aenv-egress-daemonset.yaml,aenv-egress-networkpolicy.yaml,agentenv-daemonset.yaml,kustomization.yaml,role.yaml,rolebinding.yaml}`、`deploy/k8s/overlays/pve-mf/kustomization.yaml`、`deploy/docker-compose.yml` |
| 守卫 | `Makefile`、`services/shared/config/*_manifest_test.go` |
| 测试 | `crates/aenv-node/tests/integration/{egress,fc}.rs`、`tests/fixtures/egress-local-overlay.toml`、`scripts/tests/e2e/suites/{15,16,17}_*.sh` |
| 文档 | `CLAUDE.md`、`docs/src/concepts/egress-credentials.md`、`docs/src/configuration/{reference,env-vars}.md`、`docs/src/internals/services.md`、`docs/proposals/2026-09-03-*.md` |
