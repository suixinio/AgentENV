# 沙箱出口凭据代理：实现计划（第二版）

> 2026-09-03 · 配 `2026-09-03-sandbox-egress-credential-brokering.md` 第二版。那份是"做什么与为什么"，
> 这份是"改哪些文件、加哪些类型、测什么"。基线 `refactor/pause-model-e2b`@`d27c600`。
> 未实施；每个阶段一个 PR，按顺序合入。行号已按审查核实。

## 0. 全局约束

- 先读方案 §4.1 三层三接缝、§4.2 原语与透明拦截、§4.4 契约、§4.5 G8、§4.6 授权记录。
- TLS 栈：全线 openssl / native-tls（方案 C3）。`aenv-egress` 的叶证书签发用 `openssl` crate，
  不引 rustls / rcgen。
- `make check-crate-boundaries` 加一条：`cargo tree -p aenv-egress` 不得含 sqlx / overlaybd /
  uvm-ublk；`cargo tree -p aenv-node` 不得含 `aenv-egress` 的 `tls` feature 依赖（openssl 以外
  的 TLS 栈本来就被现有规则的精神禁止，这里把它写成检查）。
- `nix` 需要加 `socket` feature（`Cargo.toml:82` 今天没有），`sockopt::OriginalDst` 在 nix 0.31 由
  它门控。
- 注释规则按 `CLAUDE.md`；新增配置全部走 confique，段名 `[egress_broker]` 与 `[secrets]`，
  变更登记进 `docs/src/configuration/env-vars.md` 与 `reference.md`。

## P0 反伪造规则（独立 PR，先合）

`crates/aenv-node/src/sandbox/network/slot.rs::configure_namespace_iptables_rules`（`:476-511`）先
Append 基础规则，再调 `initialize_namespace_egress_chain`，后者把 `-j AGENTENV-EGRESS` Insert 到
FORWARD 位置 1（`src/sandbox/network/policy.rs:141-146`）。所以 DROP 必须在**那之后**再
`Insert position 1`：`FORWARD -i tap0 ! -s <vm_ip> -j DROP`。放在前面会被挤到位置 2，排在按目的地
ACCEPT 之后而失效。

**测试**：`policy.rs` 已有命令序列断言（`:359-474`），加一条断言 DROP 的 Insert 出现在 egress chain
的 Insert 之后且两者位置都是 1（即最终 DROP 在前）。集成：guest 内以别的地址为源发包，宿主侧不可见。

## P1 契约与 `crates/aenv-egress` core（纯单元测试）

新 crate，`members` 加入。features：`core`（默认关闭 TLS）、`tls`。依赖：`tokio`、`serde_json`、
`bytes`、`hmac`、`sha2`、`base64`、`rand`、`zeroize`（新增直接依赖）；`tls` feature 再加 `openssl`、
`hyper`、`hyper-util`、`hyper-openssl`。

```
crates/aenv-egress/src/
  header.rs      IdentityHeader { v, node_id, sandbox_id, execution_id, template_id, port, handler,
                                  params, original_dst: Option<SocketAddr>,
                                  egress: EgressPolicySummary { allow_internet, allowed_cidrs, denied_cidrs },
                                  guest_addr, issued_at_unix_ms, nonce: [u8;16], hmac }
                 canonical_bytes() / sign(key) / verify(keys, max_skew)
                 ReplayCache：按 nonce 去重，容量按 max_skew 内的预期连接数
  framing.rs     u32 LE 长度前缀 + JSON，上限 64 KiB；半帧 EOF 报错
  transport.rs   trait BrokerTransport { open(hdr) -> Box<dyn AsyncStream> }
                 EmbeddedTransport（tokio::io::duplex）；RemoteTransport（P3，openssl 客户端）
  handler.rs     ConnCtx { sandbox_id, execution_id, template_id, port, handler, params, original_dst, egress }
                 trait Handler
  credential.rs  Secret { value: Zeroizing<Vec<u8>>, expires_at }
                 trait CredentialSource { get(sandbox_id, execution_id, name) }
                 CredentialError::{Denied, Unavailable}；CachingSource<S>
  policy.rs      UpstreamGuard：resolve(name) -> ips；check(ip, &EgressPolicySummary, &BrokerDenyList)
                 -> Result<()>；connect_checked(addr) 只连检查过的 IP（G8，防 rebinding）
  dispatch.rs    Dispatcher { handlers, creds, guard }；未知 handler → Ack{false,"unknown_handler"}
  handlers/tcp.rs（core，仅 embedded 与测试用；生产配置默认不装）
  runtime.rs     Options、run()、dispatch()
```

不变量写进类型：`Secret` zeroize 且 `Debug` 打码；`ConnCtx` 无 `Slot`、路径、fd；handler 拿不到
`hmac` 与 `nonce`；`UpstreamGuard::connect_checked` 是 handler 连上游的唯一入口。

**单元测试**：签名改任一字段 verify 失败；skew 超限失败；两把 key 任一可验；nonce 重放被拒；
帧上限与半帧；未知 handler；`UpstreamGuard` 对 RFC1918、CGNAT、link-local、Service CIDR 拒绝，
对 `allow_internet=false` 全拒，解析结果多 IP 时任一命中即拒；`CachingSource` TTL 与 `Denied`
不缓存；`tcp` handler 经 `EmbeddedTransport` 回声全程不经网络。

## P2 运行时与 API（集成测试用 embedded）

### P2.1 策略模型

`src/sandbox/network/policy.rs`：

```rust
pub struct HeaderTransform { pub headers: BTreeMap<String, String> }      // 值含 ${aenv.secrets.NAME}
pub struct DomainRule { pub transform: HeaderTransform }
pub struct BrokeredEndpoint { pub port: u16, pub handler: String, pub params: serde_json::Value,
                              pub intercept: Option<Intercept { dports: Vec<u16> }> }   // 内部形态
pub struct SandboxNetworkEgressPolicy { allowed_cidrs, allowed_domains, denied_cidrs,
    #[serde(default)] pub rules: BTreeMap<String, Vec<DomainRule>>,   // 公开形态，持久化
    #[serde(default)] pub brokers: Vec<BrokeredEndpoint> }            // 由 rules 归一化，持久化
```

- `SandboxNetworkEgressPolicy::new(allow_out, deny_out, rules)`：域名键校验（精确或单前导通配，
  小写化）；`allowOut` 覆盖每个规则域名否则报错；标记解析（接受 `${aenv.secrets.NAME}` 与
  `${e2b.secrets.NAME}`，名字 `^[a-zA-Z0-9_-]{1,128}$`，每域 ≤ 32 名字，每值 ≤ 8 KiB）；
  归一化：任何 `rules` 非空 → `brokers = [{port: <运行时分配>, handler: "http",
  params: {rules}, intercept: {dports: [443]}}]`。
- `referenced_secret_names()` 供 aenv-api 签 grant。
- `has_runtime_egress_rules()` 加 `|| !self.egress.brokers.is_empty()`（`policy.rs:98-100`），否则
  `runtime_policy()` 返回 `None`（消费者 `src/orchestrator/service.rs:543,598,1799,3187`）。
- `has_explicit_rules()` 不含 brokers。
- `EgressPolicySummary::from(&SandboxNetworkPolicy)` 给身份头。
- golden：`src/orchestrator/store/contract.rs` 与 `src/snapshot/types/paused.rs:129` 各加一条带
  `rules` 的往返。

### P2.2 API

`src/api/openapi.yml`：`SandboxNetworkConfig`（`:299`）与 `SandboxNetworkUpdateConfig`（`:320`）加
`rules`，schema 与 E2B `SandboxNetworkRule` / `SandboxNetworkTransform` 字段级一致；`/secrets`
五个操作与 `Secret`、`NewSecret`、`SecretUpdate`、`SecretString`（独立类型，生成代码里做
`Debug` 打码的 newtype）。`make agentenv-server` 重新生成。

`src/api/impls/sandbox.rs::network_policy_from_create` / `_from_update`（`:400-432`）传 `rules`；
校验失败 400，文案不含标记值以外的任何东西。创建前：`secret_refs` 里名字必须存在（否则 400）；
向 store 写 grant（P2.4）；`node_registry` 只把带 `brokers` 的沙箱放到 `egress_broker` 上报为
`remote_ok` 的节点，一个都没有 → 503。`embedded` 只装 tcp 身份处理器，公开规则一律命名 `http`
处理器，所以它不算可放置。

`PUT /sandboxes/{id}/network`（`:1017-1050` → `replace_sandbox_network_policy` `:1744-1813`）：
重写 grant，再下发。

### P2.3 节点配置

`src/cfg/egress_broker.rs`（新段，不放在 `network` 下）：

```rust
pub struct EgressBrokerConfig {
    #[config(default = "disabled")] pub mode: EgressBrokerMode,   // disabled | embedded | remote
    pub endpoint: Option<String>,        // remote 必填
    pub ca_cert_path: Option<PathBuf>,   // remote 必填：校验 broker 服务端证书，也是下发给 guest 的 caBundle
    pub shared_secret: Option<String>,   // remote 必填，Secret 注入
    #[config(default = 30_000)] pub max_skew_ms: u64,
    #[config(default = 256)] pub per_sandbox_conns: u32,
    #[config(default = 20_000)] pub node_conns: u32,
    #[config(default = 3_000)] pub open_timeout_ms: u64,
}
```

校验（`src/cfg.rs`）：`remote` 缺项报错；`embedded` 只允许 `node_discovery_mode = "static"` 且
`static_discovery_nodes.len() <= 1`。`config/default.toml` 加注释段。

### P2.4 aenv-api：`/secrets` 与 grant

`crates/aenv-api/src/secrets/`：`SecretsBackend` trait `{ put, new_version, delete, grant, revoke }`，
v1 实现 `VaultKv2Backend`（`<mount>/secrets/<name>`；grant 写 `<mount>/grants/<execution_id>`
为名字列表）。PG 新表 `secret_refs(secret_id, name, current_version, created_at, updated_at)`；
值不进 PG。`/secrets` handler：`value` 反序列化为 `Zeroizing<String>`，直通后立即丢弃；
响应 `Cache-Control: no-store`；tracing 层禁止记录请求体。

grant 生命周期：create / resume / fork / PUT network → `grant(execution_id, names)`；
stop / pause / delete → `revoke(execution_id)`。fork 子沙箱由 api 显式发 grant。

### P2.5 运行时：listener、DNAT、accept

`crates/aenv-node/src/sandbox/network/slot.rs`：

- `listen_in_namespace(port) -> std::net::TcpListener`：与 `set_egress_policy`（`:451-468`）同一线程
  模型；地址由 `address_plan.rs` 提供。
- `install_intercept(port, dports)` / `remove_intercept`：`nat PREROUTING -i tap0 -p tcp --dport
  <d> -j DNAT --to-destination 169.254.0.22:<port>`；`AGENTENV-EGRESS` 链头 `-p udp --dport 443
  -j REJECT`。slot 复用前无条件 `nat -F PREROUTING`（`Slot::cleanup` `:779` 与 warm pool 归还处）。

`crates/aenv-node/src/sandbox/egress/mod.rs`（新）：

```rust
pub struct BrokeredEndpoints { tasks: Vec<(u16, JoinHandle<()>)>, permits: Arc<Semaphore> }
impl BrokeredEndpoints {
    pub fn spawn(slot, identity, endpoints, transport, limits) -> Result<Self>
    pub fn reconcile(&mut self, ..) -> Result<()>          // PUT network
    pub async fn shutdown(&mut self)                        // abort 并 await 全部任务结束
    pub fn close_listeners(&mut self) / reopen(..)          // broker 不可达期间
}
```

accept 任务：`TcpListener::from_std` → loop { accept → `permits.try_acquire()` 失败即 drop 并计数
→ `spawn(async { original_dst = getsockopt(OriginalDst); hdr = IdentityHeader{.., egress}; stream =
timeout(open_timeout, transport.open(hdr)); copy_bidirectional })` }。

`crates/aenv-node/src/sandbox/firecracker/sandbox.rs`：

- 字段 `brokered: Option<BrokeredEndpoints>`。
- `start_fresh`：`set_egress_policy`（`:1312-1315`）之后、VM 启动（`:1372`）之前 `spawn`。
- `start_resume`：warm 分支（`:1401-1420`）与新分配（`:1494-1515`）都在 `set_egress_policy`
  （`:1517`）之后 `spawn`。
- `update_network_policy`（`:413`）→ `reconcile`。
- `stop`（`:875-924`）：**先 `shutdown().await`，再 `release(slot)`**（`:916-922`），否则 fd 钉住
  旧 netns。`Drop`（`:1137`）同样顺序，尽力而为。
- `wait_for_ready` 的 `envd_instance.init`（`:628-634`，`src/sandbox/envd.rs:129-150`）：**每次**
  传 `ca_bundle`（有 brokers 传 `ca_cert_path` 内容，否则 `Some("")`），有 brokers 时再传默认 env
  `SSL_CERT_FILE` / `REQUESTS_CA_BUNDLE` / `CURL_CA_BUNDLE` / `NODE_EXTRA_CA_CERTS` /
  `GIT_SSL_CAINFO`（用户同名 env 优先）；随后经 envd 读 `/etc/ssl/certs/ca-certificates.crt`
  探测 CA 指纹存在，失败即返回创建错误。

心跳：`src/observability/` 每次心跳前对 broker endpoint 做 TLS 握手探测（不发帧），写
`NodeSnapshot.egress_broker`（`services/api/proto/scheduler.proto`，字段 17，枚举 `disabled |
embedded | remote_ok | remote_unreachable`）；`src/node_registry/filter.rs` 过滤；
`services/Makefile` 重生成 Go 侧。`remote_unreachable` 期间运行时 `close_listeners`，恢复后 `reopen`。

envd：`config/deps_manifest.toml` 与 `[envd].version` 升到支持 `caBundle` 的版本；
`docs/src/internals/sandbox-testing.md` 记录。

### P2.6 集成测试（`crates/aenv-node/tests/`，需 root 与 `/dev/kvm`，`embedded` 模式）

1. 带 `rules["fake.test"]` 的沙箱：guest `curl http://fake.test/`（假上游经 hosts 指向宿主回环，
   443 用假上游的 TLS）到达 `tcp` 回声 handler 且 `original_dst` 正确、`sandbox_id` 正确。
2. 未声明域名的 443 连接经透传到达真实目的地（本地假服务）。
3. `denyOut` 含假上游 IP 时，透传与规则命中都被 broker 拒绝。
4. `allow_internet_access=false` 时全部拒绝。
5. PUT network 去掉规则后新连接透传、在途连接不断。
6. pause → resume 后规则重建，`execution_id` 已变；fork 两个子沙箱各自身份正确。
7. 无 rules 的沙箱 init 传空 `caBundle`；有 rules 的 guest 内 `openssl verify` 通过。
8. 每沙箱连接超过 `per_sandbox_conns` 时新连接被关闭并计数。
9. warm slot 复用后 `nat PREROUTING` 为空。

## P3 生产形态

### P3.1 `RemoteTransport` 与 bin

`transport.rs::RemoteTransport::open`：openssl 客户端连接 `endpoint`，校验 `ca_cert_path`；写身份头，
读 Ack。`runtime.rs::run`：openssl 服务端（证书由集群 CA 签，Secret `egress-ca` 与
`egress-server`），每连接 read_frame → verify + replay cache → dispatch。`main.rs`：confique 读
`AENV_EGRESS_CONFIG_PATH`；只在这里初始化 tracing。指标：`egress_conns_total{handler,outcome}`、
`egress_active_conns{node_id}`、`egress_policy_denied_total{reason}`、`egress_intercept_no_sni_total`、
`egress_tls_leaf_minted_total`、`egress_replay_rejected_total`。

### P3.2 `tls` feature 与 `http` handler

`tls.rs`：`CaSigner { key, cert }` 读 `egress-ca`；`leaf_for(sni)` 先查规则命中再签，LRU 缓存 +
每沙箱每分钟签发上限；未命中 SNI 回 `unrecognized_name` alert。CA 生成脚本加 Name Constraints
（运营者配置的域名后缀列表）。

`handlers/http.rs`：peek ClientHello → 有 SNI 且命中 `ctx.params.rules` → 终止 TLS →
HTTP/1.1 服务端（hyper）→ 对每个请求按规则 `headers` 替换（同名一律替换；标记逐个 `creds.get`；
`Denied` → 403、`Unavailable` → 502，均带 `x-aenv-egress-reason`）→ `UpstreamGuard::connect_checked`
→ openssl 客户端连上游（SNI 为域名，校验系统信任库）→ 流式回写；`Upgrade: websocket` 走
`hyper::upgrade`；HTTP/2 → 505。未命中 SNI 或无 SNI → `UpstreamGuard` 检查 `original_dst` 后
`copy_bidirectional` 透传。

### P3.3 部署

- `deploy/k8s/base/aenv-egress-deployment.yaml` + Service + `NetworkPolicy`（ingress 只放
  node DaemonSet；egress 拒绝集群内除 Vault 以外目标）+ Secret 模板 `egress-ca`、
  `egress-server`、`egress-hmac`；`Dockerfile.egress`；`make k8s-build` 加目标。
- DaemonSet：`AENV_EGRESS_BROKER_MODE=remote`、`..._ENDPOINT`、`..._SHARED_SECRET`（Secret）、
  `..._CA_CERT_PATH`（ConfigMap 挂载）。
- aenv-api Deployment：`[secrets]` 的 Vault 配置。
- `deploy/docker-compose.yml` 与 `make start-server`：`embedded`（只有 `tcp`，用于烟测）。
- `docs/src/concepts/egress-credentials.md`（新页）、`docs/src/security/`、`configuration/*`。

### P3.4 e2e（`crates/e2e-tests`）

1. `POST /secrets` → 创建带 `rules` 的沙箱 → guest `curl https://<假上游域名>/anything`，回显含
   `Authorization: Bearer <value>`，沙箱 env 与 `GET /sandboxes/{id}` 无该值。
2. python `requests` 与 `git ls-remote https://…` 到未声明域名照常成功。
3. `allow_internet_access=false` 的沙箱经 broker 也出不去（403 / 关闭）。
4. fork 子沙箱有 grant，请求成功；手动 revoke 后 403。
5. `kubectl rollout restart deploy/aenv-egress` 期间沙箱不受影响，新连接在就绪后恢复。
6. `curl --http3` 回退 TCP 且仍被拦截。
7. broker 不可达时 guest 得 `ECONNREFUSED`，恢复后自动可用。

## v1.1 与 v2

见方案 §7。v1.1 的显式端点、`tcp` allowlist、`postgres`（钉死 upstream/user、上游 TLS 校验、
重写 `StartupMessage`）、外部 `http` 解析器与 `fallback`、`aenv-secrets` 服务、类型化内联值转存、
`allowedHosts`；v2 的多租户所有权、HTTP/2、ECH 与 DNS 层域名规则。各自立项时再写实现计划。

## 涉及文件清单（v1）

| 阶段 | 文件 |
|---|---|
| P0 | `crates/aenv-node/src/sandbox/network/slot.rs`、`src/sandbox/network/policy.rs`（测试） |
| P1 | `Cargo.toml`（members、`nix` socket feature、`zeroize`）、`Makefile`（crate 边界）、`crates/aenv-egress/**` |
| P2 | `src/sandbox/network/policy.rs`、`src/api/openapi.yml`、`src/api/generated`（生成）、`src/api/impls/sandbox.rs`、`crates/aenv-api/src/secrets/**`（新）、`crates/aenv-api/src/pg/`（`secret_refs`）、`src/cfg/egress_broker.rs`（新）、`src/cfg.rs`、`config/default.toml`、`config/deps_manifest.toml`、`crates/aenv-node/src/sandbox/network/{slot,address_plan,manager}.rs`、`crates/aenv-node/src/sandbox/egress/mod.rs`（新）、`crates/aenv-node/src/sandbox/firecracker/sandbox.rs`、`src/sandbox/envd.rs`、`src/observability/*`、`services/api/proto/scheduler.proto`、`src/node_registry/filter.rs`、`src/orchestrator/store/contract.rs`（测试）、`src/snapshot/types/paused.rs`（测试）、`crates/aenv-node/tests/`、`docs/src/**` |
| P3 | `crates/aenv-egress/src/{runtime,main,transport,tls}.rs`、`crates/aenv-egress/src/handlers/http.rs`、`deploy/k8s/base/*`、`deploy/docker-compose.yml`、`Dockerfile.egress`、`Makefile`、`crates/e2e-tests/*` |
