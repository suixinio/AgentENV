# 沙箱出口凭据代理：运行时拥有端点与身份，可替换的 broker 拥有协议与凭据

**日期**：2026-09-03，第二版（第一版经三路对抗审查后重写；审查记录与裁决见 §6）
**基线**：`refactor/pause-model-e2b`@`d27c600`
**参照实现**：e2b-dev/infra `fdc33599b`（`/home/debian/e2b-infra`）；TencentCloud/CubeSandbox
`50d9a3e7`（`/home/debian/CubeSandbox-latest`，v0.4.0 起的 CubeEgress）
**范围**：AgentENV 的一项通用能力，不针对任何一个上游协议或任何一个消费者。附录 A 用一个现有
消费者验证；附录 B 是接入指南。
**前提**：aenv-api 与 aenv-node 不落任何密钥值；节点上除运行时外不放常驻进程；系统未上线，
无存量；**当前部署是单租户**（§2 C5）。

## 1. 问题与决定

跑在沙箱里的 agent 需要访问受保护的资源：LLM API、代码托管、数据库、对象存储、内部服务。
今天唯一的路是把凭据放进 `envVars`，明文进 microVM，凭据的暴露面就是 agent 本身：它的代码、
它拉的第三方库、它的日志、它写进 LLM 上下文的东西、它发布出去的快照与模板。

四条决定：

1. **密钥不进 microVM，也不进 aenv-api / aenv-node。** 平台只持有名字、版本与授权记录，值在
   store 与 broker 内存里。
2. **端点与身份归运行时。** 监听 socket 建在沙箱自己的 netns 内，accept 到即身份。
3. **协议与凭据归一个可替换的组件，契约与放置无关。** 契约是"一条字节流加一段身份头"，
   生产走 TLS 到集群里的 `aenv-egress` Deployment，单机与开发把同一个 lib 的核心部分链接进
   `aenv-node`。
4. **公开面是 E2B 的 `network.rules` 与 `${aenv.secrets.NAME}` 标记；透明拦截是 v1 的原语。**
   产品跑未经改造的任意代码，硬编码目标的 SDK、git、包管理器不改一行配置也要拿到凭据。
   显式本地端点与非 HTTP 协议是同一机制上的扩展，排在 v1.1。

## 2. 目标与约束

- **G1** 凭据不进 microVM，也不进 aenv-api / aenv-node。
- **G2** 身份来自拓扑，不由沙箱自报，不由源地址推断。
- **G3** fail-closed 来自拓扑：没有声明就没有端点；broker 不可达就是拒绝，且沙箱能观察到拒绝。
- **G4** 跨 pause/resume、跨节点恢复、fork、模板发布后语义不变；轮换不需要重启沙箱。
- **G5** 协议无关：协议知识只在可替换的 handler 里。
- **G6** 凭据来源无关：来源只在可替换的 `CredentialSource` 里。
- **G7** 对未经改造的工作负载成立。
- **G8** 沙箱的出口策略对经 broker 的流量同样生效。broker 不是绕过 `always_denied_cidrs` 与
  `denyOut` 的第二条路。
- **G9** E2B SDK 用户按 E2B 的写法就能用。

约束：

- **C1** 滚动运行时等于杀沙箱（`CLAUDE.md`），迭代快于运行时的东西必须能独立发布。
- **C2** 节点上不放额外进程。
- **C3** 同语言 Rust，同一个 TLS 栈：工作区只允许 openssl/native-tls，rustls 是 `crates/aenv`
  的唯一例外（`Cargo.toml:128-135`）。
- **C4** aenv-api 与 aenv-node 不落值。
- **C5** 当前没有租户身份：`Claims` 是单元结构，鉴权只判断 header 非空
  （`src/api/impls/mod.rs:35`，`src/api/impls/auth.rs:31-36`）。所有权语义必须等它存在再做。

## 3. 参照事实

只列影响决定的，每条一处 file:line。

**e2b。** 平台只存标记，`${e2b.secrets.NAME}` 只出现在 `SandboxNetworkRule.transform.headers`
（`spec/openapi.yml:457-528`），`/secrets` 直通外部 store（`docs/ARCHITECTURE.md:130-132`）；
orchestrator-ee 是链接期插件（`pkg/factories/run.go:257-259`），做成 Nomad service job 只为滚动更新；
拦截是每 slot `REDIRECT`，身份是源 IP，**拦截该端口全部流量、按 SNI 分流、不匹配的按 CIDR 策略
透传**（`tcpfirewall/proxy.go:92-121,149-161,266-269`），并有按沙箱的连接上限（`:277-286`）；
CA 经 `CABundle()` → envd `/init` → 系统信任库（`envd/internal/api/init.go:296-297`，
`envd/internal/host/cacerts.go`）；envd 当前 0.7.0。

**CubeSandbox。** 每节点独立 host-network 代理，TPROXY 80/443，身份是源 IP，密钥明文经四个
控制面进程推到代理内存（`policy.lua:83-117`，`security-proxy.md`），按 SNI 现签，CA 烧进模板；
非 80/443 端口不经代理（`security-proxy.md:205-215`）。

**AgentENV。** 每 slot SNAT 到唯一 `host_interaction_ip`；出口策略只装在 FORWARD 链
（`src/sandbox/network/policy.rs:141-224`），到 netns 自身地址的连接走 INPUT，不经它；
运行时已会 `setns` 进沙箱 netns（`slot.rs::set_egress_policy`）；`SandboxNetworkPolicy` 随
metadata、`PausedSandboxConfig`、快照 common config 持久化；`runtime_policy()` 在没有出口规则时
返回 `None`（`policy.rs:90-100`）；envd 客户端已有 `caBundle`（`thirdparty/envd/http-client/
src/models/_init_post_request.rs:37`）但节点钉的 envd 是 0.5.15（`config/default.toml:354`）；
`deploy/` 无任何 NetworkPolicy；`Claims` 无身份（C5）。

## 4. 方案

### 4.1 三层与三条接缝

```
guest ──(真实目的地:443)──▶ [DNAT → listener, 沙箱 netns 内, aenv-node 持有]
                                  │ 接缝 1: TLS(集群 CA) + 身份头 + 字节流
                                  ▼
                        [aenv-egress, 集群 Deployment]
                                  │ SNI 匹配规则 → 终止 TLS → 替换 header → TLS 连上游
                                  │ 不匹配 → 按沙箱出口策略透传 original_dst
                                  │ 接缝 2: CredentialSource.get(grant)
                                  ▼
                        [store: Vault]  ◀── 接缝 3: aenv-api 写名字/版本与授权记录
```

- **运行时**：建 listener、装 DNAT、认身份、转发字节，不解析应用层。
- **broker**：验身份头、按规则分流、终止 TLS、替换 header、按沙箱策略连上游、按授权记录取值。
- **store**：值的唯一落点。aenv-api 通过接缝 3 写名字、版本与授权记录，不写值以外的任何东西
  到自己的 PG。

谁承接凭据工作：broker 是平台组件，部署一次，所有沙箱共用；单机与开发下核心部分内嵌在
`aenv-node`，不是新进程。`execution_id` 是身份头字段，不是服务。

### 4.2 原语：沙箱 netns 内的 listener，与它之上的透明拦截

**原语。** 运行时在 slot 建好网络后，进入沙箱 netns 在 169.254.0.22 上 bind 并 listen，把 fd
带回自己的 netns 交给 tokio。socket 的 netns 归属在创建时固定，所以 accept 到即身份：不依赖
iptables 状态、不依赖报文字段、没有 IP 到沙箱的映射表、slot 复用没有竞态。listener 随 slot
释放关闭；resume 与 fork 在新 slot 上按持久化的策略重建。

**为什么不用源 IP。** 构造出来的身份比推断出来的强；没有映射表；运行时本来就在那个 netns 里；
没有 listener 就没有路径，fail-closed 不靠任何一条可以忘记的规则。"源 IP 可伪造"是另一回事：
盲目伪造完不成 TCP 握手，对 UDP 与单向流量是真问题，反伪造规则是独立的 P0（§6 F-M）。

**透明拦截，v1 原语。** 沙箱声明了任何 `rules` 后，运行时在其 netns 内装：

```
nat PREROUTING -i tap0 -p tcp --dport 443 -j DNAT --to-destination 169.254.0.22:<port>
filter AGENTENV-EGRESS: -p udp --dport 443 -j REJECT     # HTTP/3 回退到 TCP
```

**拦截的是该端口的全部流量**，这是 DNAT 的粒度，与 e2b 相同，本稿不再宣称"只拦截声明的域名"。
运行时从 socket 取 `SO_ORIGINAL_DST`（conntrack 与 listener 同在沙箱 netns）写进身份头。
broker 读 ClientHello 的 SNI：

- SNI 命中 `rules` 的某个域名：**先匹配，再签发**该名字的叶证书应答，终止 TLS，替换 header，
  以 TLS 连真上游（名字来自 SNI，证书按系统信任库校验）。
- 未命中：**不签发、不解密**，作为不透明 TCP 流按该沙箱的出口策略（§4.5）透传到 `original_dst`。
  这正是 `pip`、`npm`、`git clone github.com` 走的路，它们照常工作。
- 没有 SNI（IP 直连、ECH）：只能走透传，不可能匹配域名规则；计数 `egress_intercept_no_sni_total`。

因此 broker 对每条 443 连接看到的是 SNI 与 `original_dst`；明文只在命中规则的域名上出现。

**CA 进 guest。** 集群 CA 的私钥只在 broker 的 Secret 里，带 Name Constraints 限制到运营者
配置的域名后缀；公钥证书作为节点配置下发，运行时在 `envd_instance.init` 里**每次都**经
`caBundle` 传：有 `rules` 传证书，没有则传空串让 envd 清理。这样 CA 不会随快照与模板扩散到
不该信任它的沙箱。系统信任库不够的语言，同一次 init 的默认 env 补 `SSL_CERT_FILE`、
`REQUESTS_CA_BUNDLE`、`CURL_CA_BUNDLE`、`NODE_EXTRA_CA_CERTS`、`GIT_SSL_CAINFO`；Java keystore
需模板里一条 RUN；证书固定的客户端在任何透明拦截方案下都失败，本稿不解。

guest 的 envd 必须是支持 `caBundle` 的版本。节点今天钉 0.5.15，支持与否未证实；P2 先升级
tools drive 里的 envd 并把版本写进 `docs/src/internals/sandbox-testing.md` 的升级清单；
有 `rules` 的沙箱在 `wait_for_ready` 里对 guest 做一次 CA 存在性探测，失败即创建失败。

**堵旁路与其边界。** UDP 443 REJECT 堵 HTTP/3；ECH 让域名规则失效，只剩透传，指标超阈值时立项
DNS 层对策（§7 v2）。

### 4.3 公开面：E2B 的 `rules` 与标记

v1 唯一的公开面，字段级对齐 E2B（`spec/openapi.yml:457-528`）：

```json
"network": {
  "rules": {
    "api.openai.com": [ { "transform": { "headers": { "Authorization": "Bearer ${aenv.secrets.openai}" } } } ],
    "*.github.com":   [ { "transform": { "headers": { "Authorization": "token ${aenv.secrets.gh}" } } } ]
  }
}
```

- 键是精确域名或单个前导通配符；`rules` 不授予网络访问，出口策略另配，与 E2B 相同。
- 出口策略在本实现里是 IP/CIDR 粒度的：`allowOut` 的域名条目被 api 侧和 netns 装配双双拒绝
  （"domain entries in allowOut require the TCP egress proxy"），两处拒绝都早于本方案。所以
  规则域名的可达性由 `allow_internet_access` 决定：默认 `true` 时可达，`false` 时沙箱只剩
  `allowOut` 的 CIDR，规则域名被 **broker 按 §4.5 的 G8 拒绝**（合成 403），而不是创建时报错。
  `rules` 与 `allow_internet_access: false` 同时出现是合法的创建，这正是 §7 P3 那条 e2e 断言
  的形状。
- `headers` 是 `map<string,string>`，值里的 `${aenv.secrets.NAME}` 在出口替换；同时接受
  `${e2b.secrets.NAME}` 作别名。一个值里可以有多个标记（Basic 的 `user:pass`）。
  **沙箱自带的同名 header 一律被替换**，不追加，不透传。
- 上限：每个域名最多 32 个不同名字（同 E2B `MaxMarkerNames`），每个 header 值 8 KiB。
- `${` 后不是两个已知前缀之一时按字面量处理，不做转义语法。
- `PUT /sandboxes/{id}/network` 整体替换 `rules`；在途连接不切断，新连接按新规则。
- 名字语法 `^[a-zA-Z0-9_-]{1,128}$`，与 E2B `/secrets` 的 `name` 一致；引用侧与定义侧同一校验。

创建时校验：标记语法、名字存在于 store（否则 400）、域名语法。不校验出口策略是否"覆盖"规则
域名——按上一条，那个覆盖在本实现里无法表达，且拒绝发生在 broker。

`rules` 归一化进 `SandboxNetworkEgressPolicy` 的内部形态 `brokers`（端口、handler、参数），
`brokers` **不是公开字段**。v1.1 的显式端点与非 HTTP handler 通过 `x-aenv-` 前缀的扩展字段进
公开面，仍归一化到同一内部形态。

### 4.4 接缝 1：运行时 ↔ broker

传输：**TLS**，不是明文 TCP。第一版写"不引入 PKI"在 CA 已经存在的前提下不成立：透明模式下这条
链路上跑的是被解密的上游请求体。broker 的服务端证书由同一集群 CA 签发，运行时用工作区已有的
openssl 栈校验（C3）。身份头仍带 HMAC：TLS 证明"这是 broker"，HMAC 证明"这是某个运行时"，
两者不是一回事。

身份头：

```
u32 长度前缀 + JSON {
  "v": 1, "node_id", "sandbox_id", "execution_id", "template_id",
  "port", "handler", "params", "original_dst",
  "egress": { "allow_internet": bool, "allowed_cidrs": [], "denied_cidrs": [] },
  "guest_addr", "issued_at_unix_ms", "nonce",
  "hmac": HMAC-SHA256(key, 规范序列化)
}
broker → runtime : { "accepted": true } | { "accepted": false, "reason": "..." }
```

- `nonce` 随机 16 字节；broker 在 `max_skew` 窗口内按 nonce 去重，重放被拒。
- `egress` 是该沙箱当时生效的出口策略，broker 用它做 G8（§4.5）。
- `guest_addr` 仅供观测，broker 不据此做任何判断。
- `v` 只增不改：broker 拒绝未知 `v`，运行时升级先于 broker 升级时得到明确的 reason。

运行时侧一个 trait：`BrokerTransport::open(hdr) -> AsyncStream`。两个实现：`remote`（生产）
与 `embedded`（`aenv-node` 链接 `aenv-egress` 的 `core` feature，只有 `tcp` handler，没有
TLS 栈；允许条件是 `[cluster].node_discovery_mode = "static"` 且节点数 ≤ 1，其他情况配置校验
拒绝）。契约里没有 `Slot`、iptables 句柄、netns 路径，这是与 e2b `OnSlotCreate(slot, tables)`
的刻意区别。

运行时的 accept 循环：每条 accept 立即 `spawn`；每沙箱一个 Semaphore（默认 256 条）与全节点
上限，超限直接关闭并计数；`open` 带超时。心跳探测 broker 不可达期间**关闭 listener**，
guest 得到 `ECONNREFUSED` 而不是挂起。

### 4.5 G8：broker 执行沙箱的出口策略

第一版最大的洞：broker 从 Pod 网络连上游，沙箱 netns 里的 `always_denied_cidrs` 与 `denyOut`
对它无效，一条 `rules` 就把 broker 变成进集群的 SSRF 跳板。修法三层：

1. **身份头带策略**（§4.4 `egress`）。broker 对每个上游连接：解析名字得到 IP，**对解析后的 IP**
   按 `denied_cidrs`、`allow_internet=false` 与 broker 自己的 `[upstream].denied_cidrs`
   （默认 = 节点 `always_denied_cidrs` 加 Service/Pod CIDR）逐条检查，然后连接**那个 IP**，
   不再二次解析（防 rebinding）。命中即 403（HTTP）或关闭。
2. **透传流同样受 1 约束。** 不匹配规则的 443 连接透传到 `original_dst` 前先过同一张表。
3. **NetworkPolicy。** `deploy/k8s/base/` 加 `aenv-egress` 的 egress 策略：拒绝到
   `aenv-api`、PG、Redis、Vault 之外的集群内目标；`ingress` 只允许来自 node DaemonSet。

`tcp` 透传 handler 与 `params.upstream` 在 v1 不存在；v1.1 引入时 `upstream` 必须命中运营者在
broker 配置里列出的 allowlist，默认空。

### 4.6 接缝 2 与 3：授权记录，而不是所有权

C5 说没有租户身份，所以"secret 归 tenant，tenant 的沙箱可读"没有锚点。改为**结构性授权**：

- aenv-api 在 create、resume、fork、`PUT /network` 时，对策略里引用的每个名字向 store 写一条
  授权记录 `grant(sandbox_id, execution_id, name)`；stop、pause、删除时撤销。
- broker 的 `CredentialSource::get(sandbox_id, execution_id, name)` 只在 grant 存在时返回值。
  这条约束的是**沙箱**：一个沙箱经由正常工作的 broker 能拿到的，恰好是它自己的 grant。
  它约束不了 broker 进程本身——检查在 broker 的代码里，而它那张 Vault 令牌读得到 mount 下
  的每一个值（KV v2 的 policy 表达不出"grants/E 里点名了才让读 secrets/X"）。被攻陷的
  broker 进程由进程外的东西兜底：只有 `read` 能力、写不了 grant 的令牌，只放行节点入站、
  只放行 Vault/DNS/443 出站的 NetworkPolicy，以及只读根文件系统的非 root Pod。把进程本身也
  关进 grant 里需要签发时就限定范围的令牌（scoped 或 response-wrapped），列在 §7 v1.1。
- fork 子沙箱得到父策略的副本，aenv-api 为子 execution 显式发新 grant：继承是 aenv-api 的
  决定，不是 store 的默认。W8 的"服务端自觉"变成结构。
- 多租户到来时（`Claims` 携带身份），grant 的签发处加所有权检查，broker 与 store 不变。

`CredentialSource` 的 v1 实现只有 `store`（Vault KV v2，路径 `<mount>/secrets/<name>`，
grant 在 `<mount>/grants/<execution_id>`）。外部 `http` 解析器、`fallback` 组合、自带的
`aenv-secrets` 服务都在 v1.1。名字语法与 §4.3 相同，broker 拒绝含 `/` 的名字。

### 4.7 `/secrets`：字段级对齐 E2B

`POST /secrets {name, value, metadata?}` → `201 Secret{secretID, name, currentVersion, metadata,
createdAt, updatedAt}`；`GET /secrets`（分页，不回值）；`GET /secrets/{secretID}`；
`POST /secrets/{secretID} {value}` 新版本；`DELETE /secrets/{secretID}`。名字 `^[a-zA-Z0-9_-]+$`，
`sec_` 前缀保留给 id。aenv-api 把 `value` 直通 store，自己的 PG 只有 `secret_refs(secretID,
name, currentVersion, createdAt)`。`value` 在 openapi 上是独立类型 `SecretString`，反序列化进
`Zeroizing`，`Debug` 打码，不进任何日志与 span；响应 `Cache-Control: no-store`。

创建请求内联值（第一版 §4.9 B）**从 v1 删除**：它走在 opaque JSON 里无法打码，转存失败成孤儿，
名字绑定沙箱 id 与 fork、模板冲突。v1.1 若重做，只能以类型化字段、随机名、引用计数重做。

### 4.8 `aenv-egress`：框架与 v1 的 handler

```rust
pub trait Handler { fn name(&self) -> &str;
                    async fn handle(&self, conn: Box<dyn AsyncStream>, ctx: ConnCtx, creds: &dyn CredentialSource) -> Result<()>; }
pub trait CredentialSource { async fn get(&self, sandbox_id, execution_id, name) -> Result<Secret>; }
pub async fn run(opts: Options) -> Result<()>;      // e2b 的 factories::Run
pub async fn dispatch(hdr, stream) -> Result<()>;   // embedded 用
```

crate 分 feature：`core`（身份头、帧、传输、分发、`tcp`）与 `tls`（openssl 叶证书签发、
`http` handler）。`aenv-node` 只链 `core`。第三方在自己的 `main` 里链接 lib 塞进 handler，
就是 e2b orchestrator-ee 对 `pkg/factories` 的关系。

v1 唯一的 handler 是 `http`：TLS 终止前端（透明模式）；HTTP/1.1 与 WebSocket 升级；
HTTP/2 拒绝 `505`；按 `rules` 替换 header；以 TLS 连上游并校验证书；`Denied` 合成
`403`、`Unavailable` 合成 `502`，都带 `x-aenv-egress-reason` 与固定文案，不含名字与值。
每个 handler 的参数有类型化 schema，创建时按 schema 400。

叶证书：先匹配规则再签发；缓存按 SNI，LRU 上限与每沙箱每分钟签发上限；未匹配的 SNI
不签发也不解密。CA 私钥不出 broker 的 Secret；轮换先加后删，运行时的 `caBundle` 可含两张。

v1.1 的 handler：`tcp`（受 §4.5 allowlist）、`postgres`（上游与 `user` 由声明钉死，broker
重写 `StartupMessage` 并剔除 `replication`，上游必须 TLS 且校验主机名，只接受 SCRAM-SHA-256）。

### 4.9 生命周期与用户可见的失败

| 事件 | 运行时 | broker / api |
|---|---|---|
| create | 装 slot → DNAT + UDP REJECT → 建 listener → init 传 caBundle → 起 VM | api 校验 rules、写 grant |
| 沙箱首连 | accept → TLS 到 broker → 身份头 → 转发 | 验头 → SNI 匹配 → 取值 → 替换 → 连上游 |
| PUT network | 增删 DNAT 与 listener，在途连接不动 | api 重写 grant |
| pause | listener 随 slot 释放；policy 进快照行 | grant 撤销 |
| resume（任意节点） | 新 slot 重建，新 execution | api 发新 grant |
| fork | 子沙箱按继承的 policy 重建 | api 为子 execution 发 grant |
| 模板发布 | 标记随模板；模板构建期没有 policy，RUN 步骤拿不到凭据（`template/runner.rs:163-166`） | 无 |
| broker 不可达 | 心跳探测失败即关闭 listener → guest ECONNREFUSED | 节点上报 `egress_broker: unreachable` |
| 无 broker 能力的节点 | 节点心跳上报 `egress_broker: disabled` | api 只把带 rules 的沙箱放到有能力的节点；一个都没有 → 503 |

用户看到的失败只有三种，都可归因：规则未命中或策略拒绝是 `403 + x-aenv-egress-reason`；
broker 或 store 不可用是 `502 + reason`；CA 未到达是创建失败，不是运行时的 TLS 错误。
`GET /sandboxes/{id}` 的 network 段回每个域名规则的状态。

### 4.10 部署

- `aenv-egress`：Deployment + Service，多副本无状态，不特权；Secret `egress-ca`（CA 私钥与证书）、
  Secret 里的 HMAC key；NetworkPolicy（§4.5）。
- `aenv-node`：`[egress_broker] { mode, endpoint, ca_cert_path, shared_secret, max_skew_ms,
  per_sandbox_conns }`；段名不带 `network.` 前缀，避免与 `AENV_NETWORK_EGRESS_*` 撞前缀。
  变更登记进 `docs/src/configuration/env-vars.md` 与 `reference.md`。
- `aenv-api`：`[secrets].backend = "vault"` 与 Vault 连接配置；`/secrets` 路由。
- 心跳：`NodeSnapshot` 加 `egress_broker` 枚举（`disabled | embedded | remote_ok |
  remote_unreachable`，用保留号之后的 17）；`node_registry/filter.rs` 按它过滤。

## 5. 否决的路

- **5.1 DNAT 到 Pod netns + sidecar。** 违反 C1、C2；身份靠源 IP。
- **5.2 节点 DaemonSet + unix socket 传 fd。** 违反 C2；不能水平扩缩。
- **5.3 同进程作为唯一形态。** 违反 C1；降为 `embedded`，且只有 `core`。
- **5.4 CubeSandbox 式 TPROXY 独立代理。** 密钥明文经控制面推进代理；拦截点不归运行时。
- **5.5 自造 `brokers` 作为公开面。** 与 E2B SDK 不兼容，`handler`/`params` 把实现泄进 API 且
  无法校验。降为内部形态。
- **5.6 "只拦截声明的域名"。** DNAT 是端口粒度，这句话在数据面上不成立，会 RST 掉所有其他
  HTTPS。改为拦截整端口、按 SNI 分流、不匹配透传。
- **5.7 只做显式端点。** 被 G7 否掉。
- **5.8 明文 TCP 到 broker 加"不引入 PKI"。** CA 已经存在，链路上是解密后的请求体。
- **5.9 租户所有权作为授权模型。** C5 说没有租户。改为授权记录。
- **5.10 创建请求内联值。** 无法打码、孤儿、名字绑定沙箱 id。移出 v1。

## 6. 对抗审查记录（第一版 → 第二版）

三路独立审查：安全与威胁模型（S）、对代码库的可行性（F）、产品与 API（P）。合并去重后 24 条，
每条给裁决。

| 编号 | 严重度 | 发现 | 裁决与落点 |
|---|---|---|---|
| S1/F1/P5 | 阻断 | broker 从 Pod 网络连上游，绕过沙箱全部出口策略，成 SSRF 跳板 | 采纳：G8，§4.5 三层；`tcp` 与 `upstream` 移出 v1 |
| S2/P2 | 阻断 | `tenant` 不存在，所有权授权为空 | 采纳：C5；§4.6 改为授权记录；多租户推 v2 |
| S3 | 阻断 | broker 单一身份可 Resolve 全库；沙箱可把任意密钥注入到自选上游外送 | 部分采纳：grant 粒度到 (sandbox, execution, name)，上游由规则域名决定而非 `params.upstream`；"broker 单一身份可读全库"这一半**未消除**——grant 是 broker 进程内的应用层检查，其令牌仍读得到 mount 下每个值（§4.6）。按 grant 限定范围的令牌推 v1.1 |
| P1 | 阻断 | 公开面与 E2B 不兼容，`/secrets` 也非 E2B 形状 | 采纳：§4.3、§4.7 字段级对齐；`brokers` 降为内部形态 |
| F2/S13 | 阻断 | "只拦截声明域名"与 DNAT 端口粒度矛盾，会 RST 其他 HTTPS | 采纳：§4.2 拦整端口按 SNI 分流；5.6 |
| P3 | 阻断 | v1 范围不可评审 | 采纳：§7 三段裁剪 |
| S4 | 重大 | 运行时↔broker 明文 TCP 横穿集群 | 采纳：§4.4 TLS，用 openssl 栈 |
| S5 | 重大 | 身份头可重放 | 采纳：nonce + 去重 |
| S6 | 重大 | 先签叶证书再匹配：DoS 与签名预言机 | 部分采纳：先匹配再签、每沙箱每分钟限速已实现；**Name Constraints 未实现**——同一张 CA 还要签 broker 自己的 `CN=aenv-egress`（SAN `*.svc`），排除集会作废那张服务端证书，允许集又枚举不完规则可能点名的公网域名。给服务端证书单独一张 CA 后才能加约束，推 v1.1 |
| S7 | 重大 | CA 随快照/模板扩散 | 采纳：每次 init 都传 caBundle，无规则传空 |
| S8/S9 | 重大 | 透明 Postgres 可被中继；角色由沙箱自选 | 采纳：postgres 移到 v1.1，上游与 user 钉死，上游 TLS 校验 |
| S10/F7 | 重大 | 无按沙箱连接上限；accept 循环内 await 卡死；fd 无预算 | 采纳：§4.4 spawn + Semaphore + 超时 |
| S11/P4/S12 | 重大 | 内联值在 opaque JSON 里无法打码；孤儿；名字绑定沙箱 id | 采纳：§4.7 移出 v1 |
| F3 | 重大 | api 半边不知道节点有无 broker，400 不可判定 | 采纳：心跳枚举 + 注册表过滤 + 503 |
| F4 | 重大 | P0 反伪造规则插入顺序错误即无效 | 采纳：在出口链插入之后再 Insert 到位置 1；断言相对顺序 |
| F5 | 重大 | envd 0.5.15 未证实支持 caBundle，旧版静默忽略 | 采纳：升级 envd + 创建期 CA 探测 |
| F6 | 重大 | embedded 把第二个 TLS 栈带进 aenv-node | 采纳：feature 拆分，`core` 无 TLS；全线 openssl |
| P6/P7 | 重大 | `handler/params` 不可校验；注入语法欠定义 | 采纳：E2B `headers` 语义、替换而非追加、上限、类型化 schema |
| P9/S14 | 重大 | 失败对用户不可见；broker 不可达时挂起而非 RST | 采纳：§4.9 合成 403/502 + reason；关闭 listener |
| P8 | 重大 | `169.254.0.22` 无友好名 | 部分采纳：随 v1.1 显式端点一起做（默认 env `AENV_EGRESS_HOST`，hosts 别名） |
| S15 | 次要 | embedded 校验条件不可判定；解析器 Bearer 进节点配置 | 采纳：只允许 static 且 ≤1 节点；`core` 无解析器 |
| S16 | 次要 | 名字路径注入 | 采纳：定义与引用同一正则，拒绝 `/` |
| S17/F9 | 次要 | warm slot 残留 nat 规则；abort 异步导致 fd 钉住旧 netns | 采纳：复用前 `nat -F`；stop 里 await 任务结束再释放 slot |
| F8/F10/F11/F12/P10/P11 | 次要 | nix `socket` feature；模板构建无 policy；§8 措辞与扩展契约冲突；crate 边界规则缺；命名与配置前缀；PUT/限制/版本未定义 | 采纳：分别落进 §4.9、§4.10、§8、实现计划 |

审查里核实为正确、本稿不改的：listener 在 setns 线程内建再交 tokio 可行，`SO_ORIGINAL_DST`
在该 socket 上可取；DNAT 到本地地址走 INPUT，与现有 SNAT/DNAT 不冲突；`#[serde(default)]`
足以让 `brokers` 通过 metadata、`PausedSandboxConfig`、快照往返；`PUT /network` 链路无需新端点；
fork 与 warm-pool 路径下 listener 早于 VM 运行，无窗口；`EnvdInstance::init` 可直接加字段。

## 7. 分期

**v1（满足 G1–G9 的最小集）**

1. **P0**：反伪造 FORWARD 规则，插在出口链之后的位置 1；独立合入。
2. **P1 契约 + `aenv-egress` core**：身份头（含 `egress`、`nonce`、`original_dst`）、帧、
   `BrokerTransport`、`Handler`、`CredentialSource`、grant 模型、`embedded`；纯单元测试。
3. **P2 运行时 + API**：`rules` 进 openapi 与 `SandboxNetworkPolicy`（内部归一化为 `brokers`）；
   listener、DNAT、UDP REJECT、`SO_ORIGINAL_DST`、Semaphore；`[egress_broker]`；envd 升级 +
   `caBundle` 每次下发 + 默认信任 env + CA 探测；心跳枚举与注册表过滤；`/secrets`（vault）
   与 grant 签发；集成测试用 `embedded` 跑通身份、透传、策略拒绝、pause/resume/fork 重建。
4. **P3 生产形态**：`remote` TLS 传输、`aenv-egress` bin、`tls` feature 的 `http` handler、
   叶证书策略、NetworkPolicy、Deployment；e2e：`curl https://api.openai.com`（假上游）带上
   `Authorization`、`pip`/`git` 到未声明域名照常通、`allow_internet_access=false` 的沙箱经
   broker 也出不去、fork 子沙箱有 grant、broker 滚动期间沙箱不受影响、`curl --http3` 回退。

**v1.1**：显式本地端点（`x-aenv-` 扩展字段，`AENV_EGRESS_HOST` 与 hosts 别名）、`tcp`
（allowlist）与 `postgres` handler、外部解析器（同时承接 api 的 `grant/revoke` 与 broker 的
`get`，见附录 A）与 `fallback`、`aenv-secrets` 自带 store、类型化的内联值转存、`allowedHosts`
每 secret 限定上游、**按 grant 限定范围的 broker 令牌**（S3 的另一半：签发 grant 时同时签发
只读得到该 grant 名字的 scoped/wrapped 令牌，把 §4.6 的应用层检查变成 store 层约束）、
**给 broker 服务端证书单独一张 CA**（S6 的前置：两张 CA 分开后，签叶证书那张才能加
Name Constraints）。

**v2**：多租户所有权（待 `Claims` 有身份）、HTTP/2、ECH 与 DNS 层域名规则。

## 8. 不做的事

- aenv-api 与 aenv-node 不落值；不给 `customExtensionParams` 加敏感通道。
- 生产形态下运行时不做协议解析或 TLS 终止；运行时只转发字节。
- 本功能不在节点上新增 sidecar、DaemonSet、hostPath socket，也不新增任何进入沙箱 netns 的
  外部进程（自定义扩展现有的 `networkNamespacePath` 契约不受影响）。
- 不提供"只拦截部分域名"的声明；DNAT 是端口粒度，分流在 broker。
- 不解证书固定的客户端；不支持透明 Postgres 的 `sslmode=verify-full`。
- 不做 broker 滚动时的连接热接管。

## 9. 对照

| 轴 | e2b | CubeSandbox | 本稿 v2 |
|---|---|---|---|
| 公开面 | `rules` + `${e2b.secrets}` | SDK `Rule/Inject`，值内联 | `rules` + `${aenv.secrets}`（兼容 e2b 标记） |
| 谁装拦截 | orchestrator | 代理自己 | 运行时（netns 内 DNAT + listener） |
| 身份 | 源 IP | 源 IP | listener 所属沙箱 |
| 拦截粒度 | 整端口，SNI 分流，透传按策略 | 整端口 80/443 | 同 e2b |
| 出口策略对代理流量 | orchestrator 内同一套 | CubeNet L3/L4 | 身份头带策略，broker 执行（G8） |
| 解析进程 | ee，同进程 | 每节点独立 | 集群 Deployment；单机 embedded core |
| 运行时↔解析器 | Go 接口 | admin HTTP + TPROXY | TLS + HMAC 身份头 + nonce |
| 密钥在哪 | 外部 store | 代理内存明文 | Vault；沙箱侧按 grant 取值，broker 进程本身是可信组件（§4.6） |
| 授权 | project 所有权 | 无 | 授权记录 (sandbox, execution, name) |
| 节点上的额外进程 | 无 | 一个 | 无 |
| 独立滚动 / 扩缩 | 是 / 否 | 是 / 否 | 是 / 是 |

## 附录 A：DB 场景的运作（v1.1）

场景来自 uns-swe：沙箱里的 dev server 用 `DATABASE_URL` 连租户 PG。PR 475 曾设计过一层 DAB
（Neon proxy fork）做 principal 到真实凭据的映射与按 endpoint 的路由，外加每节点一个 privileged
sidecar 代做 SCRAM；DAB 没有上线，本稿直接按目标形态写：**没有 DAB，broker 直连租户 PG，
身份是 PG 原生的短期角色。**

```
 控制面                                                  数据面
 ┌──────────────┐  ① create {x-aenv 端点 5432/postgres}   ┌─────────────┐ ④ connect helium:5432
 │ agent-platform│     envVars DATABASE_URL=              │ guest app   │    StartupMessage
 │  (uns-swe)   │     postgresql://postgres:password@helium/xxxxdb      │    user=postgres db=xxxxdb
 │              │ ───────────────────▶ ┌──────────┐       └──────┬──────┘
 │  解析器端点   │ ◀── ② grant(sandbox, │ aenv-api │              │
 │ grant/revoke │      execution,"db") └────┬─────┘              ▼
 │ get          │   → CREATE ROLE sbx_<exec>│ ③ 建 listener ┌─────────────────────┐
 │              │     VALID UNTIL <ttl>     ▼               │ listener + relay    │ 显式端点：无 DNAT、无 CA
 └──────┬───────┘                    ┌────────────┐        │ (沙箱 netns 内)      │ helium → 169.254.0.22
        │ ⑥ get(sandbox,execution,   │ aenv-node  │◀───────│                     │
        │   "db") → {host, port,     └────────────┘        └──────────┬──────────┘
        │   database, user, password}                                 │ ⑤ TLS + 身份头 {handler: postgres, egress}
        ▼                                                            ▼
 ┌──────────────┐  ⑦ TLS 连 host:port（须在 CIDR   ┌─────────────────────────────┐
 │   租户 PG     │     allowlist 内），重写 user/db，│ aenv-egress · postgres handler│
 │ sbx_<exec>   │ ◀────────────────────────────── │  不向 guest 发挑战；上游 SCRAM │
 │ VALID UNTIL  │  ⑧ SCRAM(sbx_<exec>) → AuthOk    │  成功后才回 AuthenticationOk   │
 └──────────────┘     → 给 guest AuthOk → 盲转发    └─────────────────────────────┘
```

1. agent-platform 创建沙箱：`network` 里一条 `x-aenv` 显式端点 `{ port: 5432, handler: "postgres",
   params: { credential: "db", upstream_tls: true } }`；`envVars` 里的 `DATABASE_URL` 是**常量**
   `postgresql://postgres:password@helium/xxxxdb`，所有沙箱相同，`helium` 由模板 `/etc/hosts` 或
   envd init 解析到 169.254.0.22，其中的 user、password、database 都是占位。
2. aenv-api 写 `grant(sandbox_id, execution_id, "db")`。uns-swe 用外部解析器，grant/revoke 就是对
   agent-platform 的两个 HTTP 调用；agent-platform 收到 grant 时在租户 PG 上
   `CREATE ROLE sbx_<execution> LOGIN PASSWORD '…' VALID UNTIL '<ttl>' IN ROLE rw_<db>`
   （它已经在那里跑 DDL，`tenant_pg/lifecycle.go`），收到 revoke 时 `DROP ROLE`。
3. 运行时在沙箱 netns 内建 listener 169.254.0.22:5432。显式端点：没有 DNAT，不需要 CA 进 guest。
4. dev server 连 `DATABASE_URL`，发 `StartupMessage`（占位的 user 与 database）。
5. accept 到即身份；relay 向 broker 开 TLS 连接，写身份头。
6. `postgres` handler 调 `get(sandbox_id, execution_id, "db")`，解析器按 grant 返回结构
   `{host, port, database, user: "sbx_<execution>", password, ttl}`；无 grant 回 403，handler 给
   guest 合成 `28000`。
7. handler 以 TLS 连 `host:port` 并校验主机名。**该地址必须落在运营者为 `postgres` handler 配置的
   CIDR allowlist 内**（租户 PG namespace 的网段），沙箱选不了上游，G8 成立。`StartupMessage`
   的 `user`、`database` 重写为解析器给的值，剔除 `replication`。handler **不向 guest 发 SCRAM 挑战**，
   guest 的占位 `password` 不会被用到。
8. 上游回 `AuthenticationSASL`，只接受 SCRAM-SHA-256；handler 以短期角色完成握手，
   `AuthenticationOk` 后才给 guest 回 `AuthenticationOk`，之后纯字节转发；上游 `ErrorResponse`
   原样给 guest。`CancelRequest` 是独立的新连接，handler 识别后转发到同一上游。

生命周期：pause/resume 后 execution 变了，aenv-api 重新 grant，agent-platform 建新角色、删旧角色；
fork 子沙箱得到父策略副本，aenv-api 为子 execution 请求 grant，agent-platform 不签发则子沙箱首连
得到 `28000`；吊销就是 `DROP ROLE`，是数据库层的事实，不依赖任何缓存 TTL，在途连接由 PG 切断。

租户 PG 的 NetworkPolicy 放行 broker 的 namespace（PR 475 里 `tenant-allow-dab-pg-ingress` 的同一
位置）。托管库没有建角色权限时的退路是解析器直接返回 rw_ 凭据，真实凭据按 grant 在 broker 内存里
存活，仍从不进沙箱。

边界：guest 到 `helium` 是 netns 内明文，handler 对 `SSLRequest` 回 `N`，统一 DSN 建议
`sslmode=disable` 或不写；`sslmode=require` 与 `verify-full` 需要给 `helium` 现签证书并把 CA
送进 guest。DSN 看起来像真凭据，文档要写明它是占位。

uns-swe 侧的变化：agent-platform 实现解析器端点（grant/revoke/get）与短期角色的建删；创建请求带
`x-aenv` 端点声明与常量 DSN；`apps/dab-node-agent`、`40-node-agent.yaml`、DAB 部署清单、
`db_access` 的 principal 签发、`customExtensionParams.dab` 全部不需要。AgentENV 侧没有为这个场景
增加任何专用面：`postgres` handler 对任何 Postgres 协议的上游一样工作。

## 附录 B：新增一种授权要动什么

以 LLM key 为例，v1 两步，都不改 AgentENV 与 broker 代码：

1. `POST /secrets {"name": "openai", "value": "sk-…"}`。
2. 创建沙箱时 `network.rules["api.openai.com"]` 加
   `{"transform": {"headers": {"Authorization": "Bearer ${aenv.secrets.openai}"}}}`。出口策略
   不用动：默认 `allow_internet_access: true` 已经放行，而 `allowOut` 按 §4.3 收不下域名。

代码照常调 `api.openai.com`。再加一家是再来一次这两步。什么时候才要改 broker：凭据的用法
不是"替换一个 header"时，query 参数是 `http` handler 的一个扩展点，SigV4 与 SCRAM 是新的
handler 类，都在 `aenv-egress` 里加，运行时、API、契约不变。
