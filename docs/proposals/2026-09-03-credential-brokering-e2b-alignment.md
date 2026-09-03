# 沙箱零凭据接入：运行时拥有拦截与身份，集群级 broker 拥有协议与凭据

**日期**：2026-09-03（方向稿，未实施；本稿是同日四轮讨论的汇总，取代前几轮的口头结论）
**基线**：`refactor/pause-model-e2b`@`d27c600`
**参照实现**：e2b-dev/infra `fdc33599b`（`/home/debian/e2b-infra`）；TencentCloud/CubeSandbox
`50d9a3e7`（`/home/debian/CubeSandbox-latest`，v0.4.0 起的 CubeEgress；旧 checkout 没有该组件）；
uns-swe PR 475 `2de64ef1` 的 `apps/dab-node-agent`
**前提**：AgentENV 不持有任何用户密钥；broker 用 Rust 写在本工作区里；节点上不放任何
sidecar 或额外常驻进程；系统未上线，无存量。

## 1. 决定

三条不变量，其余都是推论：

1. **密钥不进 microVM，也不进 AgentENV。** 运行时、API、快照、metadata store 里只有标记与端口
   声明，没有值。值只在 broker 内存里，来源是集群里另一个服务。
2. **拦截点与身份归运行时。** 沙箱能连到什么、连上来的是谁，由 `aenv-node` 从拓扑决定：
   监听 socket 建在沙箱自己的 netns 内，accept 到即身份。不由任何进程自报，也不由 broker
   自己装规则。
3. **协议与凭据归一个集群级、可独立滚动的进程。** broker 是本工作区的 `aenv-egress` 二进制，
   以普通 Deployment 部署在集群里，节点上没有它的任何组件。运行时 accept 后把连接转发到
   broker，前置一段带 HMAC 的身份头。滚动 broker 不碰任何沙箱，滚动运行时不碰 broker。

形态上：接缝的形状学 e2b（trait + factory，链接期扩展），但接缝装在 broker 二进制上而不是
运行时二进制上；"运行时转发字节、外部组件解析凭据"的分工也是 e2b 的（tcpfirewall 在
orchestrator 内转发，EE 解析），只是 EE 从同进程搬到了集群；密钥不像 CubeSandbox 那样推进代理，
而像 e2b 那样按需在出口解析；身份学 PR 475（socket 在沙箱 netns 内），但建 socket 的是运行时
自己，节点上不再有第二个进程。

## 2. 目标与约束

目标沿用 PR 475 提炼出的六条：

- **G1** 密钥不进 microVM：env、MMDS、guest 文件系统、快照里都没有。
- **G2** 身份来自拓扑，不来自沙箱自报。
- **G3** fail-closed 来自拓扑：没有声明就没有路径。
- **G4** 跨 pause/resume、跨节点恢复、fork 后仍成立；运行中可换凭据。
- **G5** ~~不改 AgentENV~~ 放弃。PR 475 的全部脆弱性（`privileged`、`shareProcessNamespace`、
  按 argv[0] 找 `/aenv-node`、硬编码 `/run/aenv/netns` 与 169.254.0.22）都来自这条。
- **G6** 协议正确：Postgres SCRAM-SHA-256 由 broker 以客户端身份对上游完成，之后盲转发，
  上游的 `ErrorResponse` 原样给到沙箱。

AgentENV 特有的约束：

- **C1 滚动运行时等于杀沙箱。** `aenv-node` 优雅关闭时停掉所有沙箱，节点重启丢失其上
  运行中的沙箱（`CLAUDE.md`）；k8s 里改 Pod 模板的任何容器镜像都会重建 Pod，所以
  **sidecar 与运行时同 Pod 就等于同生命周期**。broker 的迭代节奏远快于运行时。
- **C2 节点上不放额外进程。** 不要 sidecar，也不要第二个 DaemonSet 与 hostPath 共享。
  节点只有 `aenv-node` 与它本来就有的 `uvm-ublk-daemon`。
- **C3 broker 可以是 Rust。** 运行时与 broker 共享一个协议 crate，broker 框架作为库被链接
  （e2b 的 `EgressFactory` 形态），第三方 handler 不必另起语言。
- **C4 AgentENV 不做密钥存储。** 与 e2b 一致：API 只存标记（`docs/ARCHITECTURE.md:130-132`）。

## 3. 参照事实

每条一处 file:line，只保留影响本稿决定的。

### 3.1 e2b

- **E1 平台只存标记。** `${e2b.secrets.NAME}` / `${e2b.identity.tokens.NAME}`
  （`packages/shared/pkg/networktransform/placeholders.go:11-12`），只出现在
  `SandboxNetworkRule.transform.headers`（`spec/openapi.yml:511-529`）；`/secrets` 是到外部
  `e2b.secretsstore.management.v1` 的元数据直通，仓库里只有管理面契约，没有 `Resolve`。
- **E2 OSS 存规则但不执行。** `GetRules()` 全仓只有 `pkg/server/sandboxes.go:612` 一处判空；
  `Firewall.ApplyRules` 不接 rules。
- **E3 orchestrator-ee 是同进程的链接期插件。** `pkg/factories/run.go:257-259`
  `type EgressFactory func(ctx, deps) -> EgressSetup{Proxy, NetworkAssignHook, Start, Close}`，
  `:293-294` 没传就 `log.Fatalf`；`packages/orchestrator/main.go:42-56` OSS 默认工厂返回
  `tcpfirewall.New`。接缝是 `network.EgressProxy{OnSlotCreate, OnSlotDelete, CABundle,
  SupportsBYOP}`（`pkg/sandbox/network/egressproxy.go:9-18`）与 `NetworkAssignHook`。
  EE 作为 Nomad service job 独立发布（`packages/nomad-nodepool-apm/README.md:12`），
  但它是整个 orchestrator 换了工厂。
- **E4 拦截、身份与转发都在 orchestrator 内。** `tcpfirewall/proxy.go:149-161` 每 slot
  `nat PREROUTING -i <veth> -p tcp -j REDIRECT`；`:266-269` 按源地址 `GetByHostPort`；
  握手后由 orchestrator 自己转发字节（`handlers.go` 的 `proxy()`）。orchestrator 里没有 `setns`。
- **E5 会进 guest 的东西。** MMDS 只放 `accessTokenHash`（`fc/mmds.go:6-12`）；envd token 与
  `envVars` 明文经 `POST /init`（`pkg/sandbox/envd.go:84-88`）。AgentENV 同构
  （`crates/aenv-node/src/sandbox/firecracker/mmds.rs:8`）。
- **E6 fork 不继承身份定义。** `orchestrator.proto:72-84`、`docs/ARCHITECTURE.md:109-116`。
- **E7 CA 进 guest 的管道在 OSS。** `EgressProxy.CABundle()` → `Sandbox.CABundle`
  （`pkg/sandbox/sandbox.go:814`）→ envd 装进系统信任库（`envd/internal/host/cacerts.go:20-28`）；
  OSS 传 `""`。

### 3.2 CubeSandbox

- **K1 独立的每节点进程。** CubeEgress 是 host-network 容器里的 OpenResty，
  `CubeEgress/scripts/cube-proxy-iptables-init.sh:90-96` `mangle PREROUTING -p tcp --dport
  80|443 -j TPROXY`，`docs/guide/security-proxy.md:1-8`。
- **K2 身份 = 源 IP。** `CubeEgress/lua/access_phase.lua:291` `sandbox_ip = ngx.var.remote_addr`；
  策略以沙箱 IP 为键（`policy.lua:4`）。
- **K3 密钥是明文值，推进代理。** 规则 `{match{sni,host,method,scheme,path},
  action{allow, inject[{header,secret,format}], audit}}`（`policy.lua:83-117`），`secret` 上限
  64 KiB；路径 CubeAPI → CubeMaster → Cubelet（`Cubelet/network/plugin_tap.go:600`）→
  network-agent → `PUT /admin/v1/policies/<ip>`。密钥经两个控制面进程。
- **K4 MITM。** `cert_signer.lua` 按 SNI 现签；根 CA 在模板构建时烧进 guest（`--with-cube-ca`）。
- **K5 没有工作负载身份、轮换、按请求签发。** env vars 照常明文进 VM
  （`Cubelet/services/cubebox/probe.go:195-258`）。

### 3.3 PR 475 的 dab-node-agent

- **D1 身份来自 socket 所在 netns。** `netns.go::listenInNetns` 用 `LockOSThread + setns`
  在每个沙箱 netns 内 bind 169.254.0.22:5432；"哪个 socket accept 到，就是哪个沙箱"。
- **D2 为进 netns 付出的代价。** `resolveNetnsPath` 经 `/proc/<pid>/root` 翻译路径，manifest
  要 `privileged: true` 与 `shareProcessNamespace: true`，按 argv[0] 找 `/aenv-node`。
- **D3 token 走 `customExtensionParams`。** 而 params 进 metadata store
  （`src/orchestrator/store/metadata.rs:213`）、快照 common config
  （`crates/aenv-node/src/sandbox/firecracker/config.rs:158`，resume 与模板继承
  `sandbox.rs:556-560`）、并可 `GET /sandboxes/{id}/custom-extension-params` 回读。
- **D4 协议部分是对的。** `relay.go::authenticateUpstream` 以节点持有的 token 对 DAB 做 SCRAM，
  `AuthenticationOk` 后纯转发，DAB 的 `ErrorResponse` 透传。

### 3.4 AgentENV 现状

- **A1** 每 slot 把 VM 流量 SNAT 成唯一的 `host_interaction_ip`
  （`crates/aenv-node/src/sandbox/network/slot.rs::configure_namespace_iptables_rules`）；
  出口链先 ACCEPT `veth_host_ip/32` 与 DNS，再 REJECT `always_denied_cidrs`
  （`src/sandbox/network/policy.rs:129-224`）。
- **A2** FORWARD 是无条件 `-i tap0 -o vpeer -j ACCEPT`，SNAT 只匹配 `-s <vm_ip>`，全工作区
  没有反伪造规则。
- **A3** 固定 tap 链路 `169.254.0.20/30`（`src/cfg/network.rs:11`），guest .21，宿主侧 .22。
- **A4** 运行时已经会进沙箱 netns 装规则（`slot.rs::set_egress_policy` 起线程 `setns`）。
- **A5** `SandboxNetworkPolicy` 随 metadata、`PausedSandboxConfig`（`src/snapshot/types/paused.rs`）
  与快照 common config 持久化，pause/resume/fork/模板全程携带。
- **A6** 节点已有一个集群级共享密钥的先例：`[sandbox].access_token_hash_seed`，k8s 里由
  `agentenv-runtime-secrets` Secret 下发到每个运行时 Pod（`docs/src/security/secure-sandboxes.md`）。
- **A7** 运行时已经是一个数据面转发者：沙箱代理（`src/api/proxy.rs`）在 `aenv-node` 内转发
  到 `host_interaction_ip`。再加一个方向的转发不是新的角色。

## 4. 方案

### 4.1 总体

```
guest ── 169.254.0.22:5432 ──▶ [listener, 建在沙箱 netns 内, 由 aenv-node 持有]
                                     │ accept；listener 即身份
                                     ▼
                              [aenv-node relay]  ── TCP ──▶  [aenv-egress Deployment, 集群内]
                                 先写身份头 {sandbox_id, execution_id, port, handler, params, hmac}
                                 再 copy_bidirectional             │ handler(port) → e.g. PostgresScram
                                                                   │ credential_source.get(sandbox, execution, name)
                                                                   ▼
                                                            [上游: DAB VIP / 任意 PG / 将来 HTTPS 目标]
```

运行时做三件事：建监听、认身份、转发。broker 做其余：验身份头、协议、凭据、连上游。
密钥值只在 broker 内存里，来源是 broker 的 `credential_source`（uns-swe 里是 agent-platform）。
节点上除了 `aenv-node` 没有任何新进程、新 hostPath、新权限。

### 4.2 运行时：监听在沙箱 netns 内，转发到集群

slot 建好网络后（A4 的同一处），对该沙箱 `network.brokers` 声明的每个端口，在沙箱 netns 内
bind `169.254.0.22:<port>` 并 listen，然后把 fd 带回运行时自己的 netns。socket 的 netns
归属在创建时固定（D1 的依据），之后运行时在任何线程上 accept 都行，tokio 直接包这个 fd。

每 accept 到一条连接：listener 属于哪个沙箱就是谁（G2），向 `[network.egress_broker].endpoint`
拨一条 TCP，写身份头（§4.4），然后 `copy_bidirectional` 直到任一方关闭。broker 不可达、
拒收身份头、上游拒绝：运行时关掉沙箱侧连接，沙箱看到 RST 或上游的 `ErrorResponse`（G3、G6）。

运行时不解析任何一个应用层字节；它对 Postgres、HTTP 一无所知。这与 e2b E4 的分工相同：
tcpfirewall 在 orchestrator 内转发，语义在别处。

不再需要：DNAT、宿主侧 REDIRECT、反伪造规则作为前置、`hostInteractionIp` 建表、
`state.json`、custom extension hook 参与本功能、任何节点侧 sidecar 或 socket 文件。
反伪造规则仍建议加（§6 W1），但与本稿无关。

stop / pause / 迁移：listener 随 slot 释放关闭，转发中的连接两端一起关。
resume 与 fork：新 slot、新 execution id，按持久化的 policy 重建 listener。

### 4.3 API：`network.brokers`

`SandboxNetworkConfig`（`src/api/openapi.yml:299`）与 `SandboxNetworkUpdateConfig` 增加：

```json
"brokers": [
  { "port": 5432, "handler": "postgres",
    "params": { "endpoint": "ep-abc", "role": "sbx_x", "database": "proj" } }
]
```

- `port`：guest 侧在 169.254.0.22 上看到的端口。同一沙箱内唯一。
- `handler`：broker 端 handler 名，运行时不解释，只透传。
- `params`：非密标记，opaque JSON，运行时不解释。**契约上不得含密钥**：它随
  `SandboxNetworkPolicy` 进 metadata、`PausedSandboxConfig`、快照与模板（A5），并可被
  `GET /sandboxes/{id}` 读到。这是 e2b E1 与 CubeSandbox K3 的分水岭，本稿站 e2b。

`brokers` 存进 `SandboxNetworkPolicy.egress`，与 `allowed_cidrs` 同级；于是 G4 的持久化
一分钱不花。`PATCH /sandboxes/{id}/network` 可增删条目，运行时对运行中的沙箱增删 listener。

`customExtensionParams` 与本功能脱钩：不改它，不给它加敏感通道，不靠 hook 传递任何东西。

### 4.4 运行时 ↔ broker 契约

传输：TCP，目标是 `[network.egress_broker].endpoint`（k8s 里就是 `aenv-egress` 的 ClusterIP
Service），每条沙箱连接对应一条 broker 连接。连接建立后运行时先写一段身份头：

```
u32 长度前缀 + JSON {
  "v": 1,
  "node_id": "...",
  "sandbox_id": "...", "execution_id": "...", "template_id": "...",
  "port": 5432, "handler": "postgres", "params": {...},
  "guest_addr": "169.254.0.21:41234",
  "issued_at_unix_ms": ...,
  "hmac": base64(HMAC-SHA256(key, 以上字段的规范序列化))
}
broker → runtime : u32 长度前缀 + JSON { "accepted": true } | { "accepted": false, "reason": "..." }
```

之后双向透明。身份头的 HMAC 密钥是集群级共享密钥 `[network.egress_broker].shared_secret`
（k8s 里放进 `agentenv-runtime-secrets`，与 A6 的 seed 同一个 Secret、不同的 key），
broker 与每个运行时都持有；`issued_at` 限制重放窗口，broker 拒绝时钟偏差超过阈值的头。
这解决了"任何能连到 broker 的 Pod 都能冒充节点"的问题，不需要 PKI，也不依赖 NetworkPolicy
（NetworkPolicy 可以再加一层，但不是契约的一部分）。

只有运行时能生成合法身份头，而运行时的身份来自 listener，所以链条是完整的：
guest → 拓扑 → 运行时 → HMAC → broker。broker 不需要任何特权，不需要在节点上，
不需要看见 veth 或 netns。

### 4.5 `crates/aenv-egress`：broker 框架与内置 handler

工作区新增 crate，lib + bin：

```rust
pub trait Handler: Send + Sync {
    fn name(&self) -> &str;
    async fn handle(&self, conn: TcpStream, ctx: ConnCtx, creds: &dyn CredentialSource) -> Result<()>;
}
pub trait CredentialSource: Send + Sync {
    async fn get(&self, sandbox_id, execution_id, name: &str) -> Result<Secret>; // 带 TTL，进程内缓存
}
pub struct Options { pub handlers: Vec<Box<dyn Handler>>, pub credential_source: Box<dyn CredentialSource>, .. }
pub async fn run(opts: Options) -> Result<()>;   // 即 e2b 的 factories::Run
```

`aenv-egress` 的 `main` 用配置装配内置 handler；第三方在自己的 `main` 里链接这个 lib 并塞进
自己的 handler，就是 e2b `orchestrator-ee` 对 `pkg/factories` 的关系，只是对象换成了
broker 二进制。身份头的解析与 HMAC 校验在 lib 里，handler 拿到的 `ConnCtx` 已经是可信的。

内置 handler（v1 两个，v2 一个）：

- `tcp`：按 `params.upstream` 直连并转发。用途：把一个内网目标以固定身份代理出去，
  不带凭据；也是集成测试的探针。
- `postgres`：读客户端 `StartupMessage` 原样前送（`user`、`database`、`options=endpoint=…`
  都由上游校验与路由，D4 的做法），以 `creds.get(sandbox, execution, "password")` 取到的口令
  对上游完成 SCRAM-SHA-256，`AuthenticationOk` 后纯转发；上游 `ErrorResponse` 透传后关闭。
  这是 D4 的 `relay.go` + `proto.go` 的 Rust 化，DAB 特有语义（principal、endpoint 路由）
  全在 uns-swe 的控制面，不在这里。
- `http`（v2，§7 P3）：TLS 终止 + header 注入 + 上游重连。需要 CA 进 guest（E7 与 K4 是
  同一件事），在 AgentENV 里就是 envd 的 `POST /init` 多带一个 `caBundle`。

`CredentialSource` 内置一个 HTTP 实现：`GET {url}/credentials?sandbox_id=&execution_id=&name=`
带 Bearer，响应 `{value, ttl_secs}`；缓存到 TTL。换凭据不需要任何 AgentENV 动作：下一条
新连接取到新值。uns-swe 只需在 agent-platform 上开这个端点（与现有
`/internal/v1/dab/wake_compute` 同一 Bearer 保护，`internalrt/routes.go:42-44`），
不需要再写任何代理代码。

### 4.6 生命周期

| 事件 | 运行时 | broker |
|---|---|---|
| create | 装 slot → 按 policy 建 listener → 起 VM | 无 |
| 沙箱首连 | accept → 拨 broker → 写身份头 → 转发 | 验头 → `creds.get` → 握手 → 转发 |
| PATCH network | 增删 listener | 无 |
| pause | listener 随 slot 释放；policy 进快照行 | 连接关闭 |
| resume（任意节点） | 新 slot 按 policy 重建 listener，新 execution_id | 新 execution 首连重新取凭据 |
| fork | 子沙箱按继承的 policy 建 listener | 子 execution 没被签发即拒（E6 语义） |
| 模板发布 | 标记随模板，无密钥可泄 | 无 |
| broker 滚动/崩溃 | 新连接被拒 → RST；沙箱不受影响 | 无状态，重启即恢复 |
| 运行时滚动 | 沙箱本来就没了 | 无 |

### 4.7 部署

- `aenv-egress`：普通 Deployment + ClusterIP Service，多副本无状态，不特权，不挂 hostPath。
  可以和 `aenv-api` 同一 namespace 但必须是不同的 Deployment：它是数据面。
- `aenv-node`：多一个配置段 `[network.egress_broker] { endpoint, shared_secret }` 与
  Secret 里多一个 key。运行时启动不依赖 broker 存在；没有 broker 时带 `brokers` 的沙箱能
  创建、连不上（fail-closed），并在节点心跳里上报 `egress_broker_reachable=false`。
- uns-swe 的 `40-node-agent.yaml`、`dab-node-agent` 目录整个删除；`DB_ACCESS_MODE=node_agent`
  下 `BuildNodeAgentDatabaseURL` 的 DSN 形状不变（`postgres://role@169.254.0.22:5432/db?options=…`）；
  沙箱创建请求多带 `network.brokers` 一条。`network-dab-overlay.toml` 的 VIP 放行不再需要，
  因为连 DAB 的是 broker 而不是沙箱。

## 5. 为什么不是另外三条路

**5.1 DNAT 到 Pod netns + sidecar broker（本稿第一版）。** 运行时改动最小（几条 iptables），
但 sidecar 与运行时同 Pod，违反 C1、C2；身份靠源 IP，A2 说明今天可伪造，必须先加反伪造规则；
broker 要在 Pod netns 里才收得到包，部署形态被锁死。

**5.2 节点本地独立 DaemonSet + unix socket 传 fd（本稿第二版）。** 零拷贝、无需 HMAC，
但节点上多一个常驻进程与共享 hostPath，违反 C2；fd 出不了主机，broker 永远只能是每节点一个，
不能按负载水平扩缩。语言约束解除后它相对 5.1 的优势消失，相对本稿只剩零拷贝一项。

**5.3 e2b 式同进程（handler 编进 `aenv-node`）。** C3 允许了，但 C1 把它否掉：协议
handler 与凭据客户端的每次改动都要滚动运行时，即杀沙箱；handler 的 panic 面也进了运行时。
e2b 自己把 EE 作为独立 job 发布，说明他们在意的也是独立滚动。本稿把 e2b 的接缝形态保留下来，
只是装在 broker 上。

**5.4 CubeSandbox 式 TPROXY 独立代理。** 进程边界比本稿更靠近节点（K1），且两点不学：密钥明文经
控制面推进代理（K3），与 C4 冲突；代理自己装拦截规则、按源 IP 键控（K1、K2），等于拦截点
不归运行时。本稿的 broker 完全不知道网络布局。

## 6. 对抗审查

**W1 反伪造规则仍然该加。** 本稿的身份不依赖源 IP，但 A2 的洞独立存在：guest 可以伪造别的
沙箱的 `host_interaction_ip` 出网。`FORWARD -i tap0 ! -s <vm_ip> -j DROP` 列为 P0，与本功能解耦。

**W2 运行时进了数据路径。** 每条 broker 化连接在运行时里占一对缓冲与一个任务；e2b 的
orchestrator 同样如此（E4）；本仓库的沙箱代理已经是这个角色（A7）。慢 broker 靠 TCP
背压自然限速，不会在运行时里积压。运行时重启本来就杀沙箱，不额外损失。

**W3 共享密钥是信任边界。** 泄露 `shared_secret` 的人可以向 broker 冒充任意沙箱。
与 A6 的 seed 同级别对待：只在 Secret 里、`--setup-host` 不碰、轮换时先加后删（broker 同时
接受两把）。不做 PKI 是刻意的：本仓库没有 PKI，为此引入一个的成本大于收益。

**W4 broker 无状态，但转发中的连接在滚动时会断。** e2b、CubeSandbox、PR 475 都如此；
Postgres 客户端会重连。不做热接管。

**W5 `params` 可能被误用来装密钥。** 契约禁止，但无法机器检测。缓解：文档醒目；`aenv-egress`
的内置 handler 一律不从 `params` 读凭据，只从 `CredentialSource` 读，于是塞进去也没用。

**W6 listener 在沙箱 netns 内绑 169.254.0.22，与 PR 475 的 `DAB_NODE_BIND_ADDR` 是同一地址。**
它在本稿里成了契约（写进 `docs/src/concepts/`），不再是硬编码的巧合。将来若改固定链路
（A3），随契约一起改。

**W7 HTTP handler 要求 CA 进 guest，改动面比 v1 大。** 这是 e2b E7 与 CubeSandbox K4 都付过
的成本；本稿只把它排进 P3，不让 v1 背。

**W8 fork 的授权语义交给了 broker 的控制面。** 运行时只保证子沙箱有 listener；子 execution
能否拿到凭据由 `CredentialSource` 决定。这与 E6 一致，但意味着一个"什么都放行"的
`CredentialSource` 会让 fork 静默继承父的权限。内置 HTTP 实现要求服务端按 execution_id 签发。

**W9 broker 到上游的出口身份变了。** 上游看到的源是 broker Pod 而不是节点。对 DAB 这是
改善（VIP 从集群内 Pod 可达，无需在沙箱出口链上开洞）；对按源 IP 白名单的上游，白名单
要从节点改成 broker。

## 7. 分期

1. **P0（可独立合入）**：W1 反伪造 FORWARD 规则 + 测试。
2. **P1（运行时 + API）**：`network.brokers` 进 openapi、`SandboxNetworkPolicy`、metadata、
   `PausedSandboxConfig`；slot 内建 listener 与 accept 循环；身份头 + HMAC + `copy_bidirectional`；
   `[network.egress_broker]` 配置与 Secret key；集成测试：带 `brokers` 的沙箱在无 broker 时得
   RST，对一个假 broker 时身份头到达、HMAC 可验、字节双向通。
3. **P2（`crates/aenv-egress`）**：框架 lib + bin、身份头校验、`tcp` 与 `postgres` handler、
   HTTP `CredentialSource`、Deployment + Service manifest；e2e：沙箱内 `psql` 无密码经 broker
   连到真 PG，错误口令时看到上游 `ErrorResponse`。
4. **P2'（uns-swe）**：agent-platform 开凭据端点；删 `dab-node-agent` 与 `40-node-agent.yaml`；
   创建请求带 `network.brokers`；`customExtensionParams.dab` 整个去掉。
5. **P3（可选）**：`http` handler、CA 经 envd `/init` 进 guest、按域名的 `rules` 与
   `${aenv.secrets.NAME}` 标记语法。届时 e2b E1 的完整形态才落地。

## 8. 不做的事

- 不把任何密钥值存进 AgentENV 的任何存储，不给 `customExtensionParams` 加敏感通道。
- 不在运行时里做协议解析或 TLS 终止；运行时只转发字节。
- 不在节点上放 sidecar、第二个 DaemonSet、hostPath socket；不让任何外部进程进沙箱 netns
  或装 iptables。
- 不引入 PKI；节点到 broker 的身份用共享密钥 HMAC。
- 不为 PR 475 的 `setns` sidecar 形态保留兼容；不保留 `DB_ACCESS_MODE=vip` 的 token 进 env 形态。
- 不做 broker 滚动时的连接热接管。

## 9. 四方对照

| 轴 | e2b | CubeSandbox | PR 475 | 本稿 |
|---|---|---|---|---|
| 谁装拦截 | orchestrator（`OnSlotCreate`） | 代理自己的 init 脚本 | sidecar `setns` | 运行时，listener 在沙箱 netns 内 |
| 身份 | 源 IP → slot | 源 IP → 策略键 | socket 所在 netns | listener 所属沙箱（同 PR 475） |
| 谁转发字节 | orchestrator | CubeEgress | sidecar | 运行时 → broker |
| 解析进程 | orchestrator-ee，同进程 | CubeEgress，每节点独立 | sidecar，同 Pod | `aenv-egress`，集群 Deployment |
| 节点上的额外进程 | 无 | 一个 | 一个 | 无 |
| 运行时↔解析器 | Go 接口，链接期 | admin HTTP + TPROXY | custom extension hook | TCP + HMAC 身份头 |
| 密钥在哪 | 外部 store，平台存标记 | 代理内存明文，经控制面推 | `customExtensionParams`，进快照 | broker 内存，按需向外取；平台存标记 |
| 扩展方式 | 私有 `main` 链接 `factories` | Lua 规则 | 改 sidecar | 私有 `main` 链接 `aenv-egress` lib |
| 独立滚动 / 水平扩缩 | 是 / 否 | 是 / 否 | 否 / 否 | 是 / 是 |
| Postgres SCRAM | 否（只有 HTTP header） | 否 | 是 | 是 |
