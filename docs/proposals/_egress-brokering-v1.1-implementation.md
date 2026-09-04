# 沙箱出口凭据代理 v1.1：施工清单

> 2026-09-04 · 承接 `2026-09-03-sandbox-egress-credential-brokering.md` 的 §7 v1.1 与附录 A。
> v1 已完成并在 pve-mf 验证（分支 `feat/egress-credential-brokering`，基线 `2f45023`）。
> 这份是"改哪些文件、加哪些类型、测什么"，以及每阶段的验收判据。
>
> **实施状态（2026-09-04）**：A/B/C/D 已实施，E 除 E1 外已实施，F 属消费方仓库。
> 单元与守卫变异证据见各阶段提交；集群验收（A 阶段的 netns 判据、D 阶段的真实 PG）未做。
> E1 的结论见下面 E 表格里的那一行：Vault 那条路要给 api 半边 `sys/policies/acl/*` 写权限，
> 用一个更大的暴露换一个更小的，所以没走；`external_resolver` 从另一侧达到了同一个目的。

## 0. 起点：v1 留下的接缝

v1.1 不需要新的架构，四个接缝已经在位，postgres 与 tcp 是往里填实现：

- `crates/aenv-egress/src/handler.rs::Handler` —— `handle(conn, ctx, guard, creds)`。一种协议一个
  实现，上游只能经 `guard`，凭据只能经 `creds`。新增 handler 不动运行时、API 与契约。
- `crates/aenv-egress/src/policy.rs::UpstreamGuard` —— G8 的执行点。`connect_checked` 已做解析、
  逐地址检查、v4-mapped 规范化。v1.1 只需要往它上面加"每 handler 的运营者 CIDR allowlist"。
- `crates/aenv-egress/src/credential.rs::CredentialSource` —— 取值接缝。v1 只返回不透明字节，
  v1.1 要返回结构化凭据。
- `crates/aenv-egress/src/header.rs::IdentityHeader` —— 已带 `handler`、`params`、`egress`、
  `original_dst`。显式端点复用同一个头，不新增传输面。

**v1 没有而 v1.1 必须新建的**：公开面的显式端点声明、不带 DNAT/CA 的 listener、结构化解析器、
两个 handler 的真实实现。

## 0.1 全局约束（沿用 v1，违反即回退）

- **G8 不可让步**：沙箱永远不能选上游。任何 handler 的上游都必须过 `UpstreamGuard` 并落在运营者
  配置的 allowlist 内。第一版方案的 `params.upstream` 就是因为它是 SSRF 跳板才被删掉的（§6 S3）。
- 运行时不解析任何应用层字节，只转发。协议解析全部在 `aenv-egress` 内。
- 不在节点上新增 sidecar、DaemonSet、hostPath socket，也不新增进入沙箱 netns 的常驻进程。
- `make check-crate-boundaries` 必须保持绿：`aenv-egress` 不得引入数据库、字节半边、`aenv-core`
  或第二个 TLS 栈。结构化解析器是 HTTP 客户端，不是数据库客户端。
- 每个新守卫都要有变异证据（构造一次真实违规、确认守卫变红、复原）。

## 0.2 已定：附录 A 的两处修正（2026-09-04）

**不建短期角色。** 附录 A 原本要求发放授权时 `CREATE ROLE sbx_<execution>`、撤销时 `DROP ROLE`。
改为解析器直接返回消费方已有的每库凭据，不做任何 DDL、不新增存储。代价要写进文档：所有沙箱在
数据库侧仍是同一个角色，审计日志分不出是哪个沙箱。**代理消除的是"凭据在沙箱里"，它本身不提供
每沙箱的数据库身份。**

**字段选择性覆盖。** 解析器返回哪些字段就只覆盖哪些字段，**没有被选中的字段一律原样透传**。
以 `postgresql://user:pass@tier0/xxx` 为例，解析器给了 host、user、password，那么连接落到
真实地址与真实账号，而 `xxx` 这个库名、`options`、`application_name` 等保持 guest 写的值。
附录 A 原文"user、database 重写为解析器给的值"作废，database 只在解析器明确返回时才重写。

唯一的例外是 `replication`：它切换的是另一套协议模式，handler 的盲转发前提不成立，所以带
`replication` 的启动一律**拒绝**（不是静默剔除），理由写进错误文案。

## 0.3 已定的另外三条（2026-09-04）

**`sslmode` 只支持 `disable`。** handler 对 `SSLRequest` 回 `N`，沙箱侧 DSN 只能是 `sslmode=disable`
或不写。不为 `verify-full` 给本地名字现签证书、不把 CA 送进 guest，显式端点"无 CA"的前提保住。
消费方文档要写明：命名空间内那一段是明文，不出宿主机；真实的 TLS 与证书校验由 broker 对上游做。

**allowlist 每 handler 一份。** 不做全局表加标签。空表等于该 handler 不可用，而不是放行一切。

**显式端点仍按方案走 `x-aenv-` 扩展字段。** 见下面的对齐说明。

## 0.4 与 e2b 的对齐（2026-09-04 查证 `e2b-infra`）

**e2b 没有显式端点声明。** 它存储层的出口配置只有三组：`allowedAddresses` / `deniedAddresses`、
按域名的 `rules`（只做 HTTP 头替换）、以及 SOCKS5 的 `egressProxyAddress` / `Username` / `Password`。
全链路**没有任何按端口的配置**，也没有 handler 概念。

相邻的是 `egressProxy`：透明 SOCKS5 隧道，"出站 TCP 在放行过滤之后被隧道转发，沙箱对此无感知"。
实现上是 iptables REDIRECT 把**所有 TCP** 打到用户态代理，该代理接口同时提供 `CABundle()`，
所以 HTTPS 中间人与证书下发也归它。仓库里只有接口与空实现。

两条结论：

- **透明拦截是 E2B 形状的做法**，他们没有"声明本地端点"这个念头。这支持 A3 的端口拦截那一条，
  也是"guest 可以写任意 host"能成立的机制。
- **SOCKS5 转的是字节，选不了协议 handler、绑不了凭据，改写不了 `StartupMessage`。**
  所以显式端点声明是对上游的一处**有意偏离**，理由是我们需要"哪个端口用哪个 handler、配哪份凭据"，
  而 e2b 的面表达不了这件事。

一处我们更严：e2b 把 SOCKS5 口令明文存在沙箱网络配置里；我们的凭据在密钥存储或解析器后面，
配置里不落值。

## A 阶段：显式本地端点

没有它，非 HTTP 协议无处落脚。这是 v1.1 唯一的**新公开面**。

### A1 契约与校验（`src/sandbox/network/policy.rs`、`src/api/openapi.yml`）

- `network` 增加 `x-aenv-` 前缀的显式端点数组，元素形如
  `{ port: 5432, handler: "postgres", params: { credential: "db", upstream_tls: true } }`。
- 校验：`handler` 必须是已注册名之一；`port` 不得与 v1 的透明拦截端口冲突；`params` 的形状按
  handler 校验而不是自由 JSON；`credential` 命名的密钥必须在 `secret_refs` 里存在（与 v1 的
  `check_rule_secrets` 同一条路径）。
- **注意 v1 刚加的 32 KiB 上限**：`MAX_RULES_SERIALIZED_BYTES` 保护的是 64 KiB 的身份帧。显式端点
  会进同一个帧，上限要一起重算，否则会出现"校验通过但 remote 模式下帧超限"的老问题。

### A2 归一化与放置

- 显式端点归一化进内部 `brokers`，与 `rules` 派生的条目共存。
- `requires_egress_broker` 的放置判据要覆盖显式端点，否则会被放到没有 broker 的节点上。
- v1 刚把 `can_broker()` 收窄到只认 `RemoteOk`；embedded 依旧只服务 `tcp`，A 阶段之后
  `mode_serves_handler` 的表要同步更新。

### A3 运行时：两条到达路径（`crates/aenv-node/src/sandbox/egress/`、`src/sandbox/network/policy.rs`）

在沙箱 netns 内 bind `169.254.0.22:<port>`。**显式端点没有 DNAT，也不需要 CA 进 guest**，与 v1 的
透明拦截是两条独立路径，不要复用 `install_intercept`。但 guest 的 DSN 要真的走到这个 listener，
名字必须先解析得出来——一个解析不了的 host 连 TCP 都不会发起，也就没有东西可拦截。两条机制，
都要做，第二条可关：

- **hosts 别名（必须）**：模板或 envd init 写入 `169.254.0.22 <name>`，`<name>` 由
  `AENV_EGRESS_HOST` 给出（§6 P8）。这条让约定的名字精确生效，是消费方常量 DSN 的最小依赖。
- **端口透明拦截（可选开关）**：复用 v1 为 443 建的 `install_intercept`，把 netns 内所有出站
  该端口的流量 DNAT 到 listener，于是**任何能解析的 host** 都落到代理上。
  **代价必须写进文档**：开了它，沙箱就再也直连不到任何其它同协议服务（例如用户自己的外部库），
  会被静默改道。因此它是每沙箱可关的开关，不是全局默认。

v1 刚修的 `Drop for BrokeredEndpoints` 与 `replace()` 顺序对显式端点同样适用，复用现有实现。

**A 阶段验收**：只声明 `tcp` 显式端点的沙箱起来后，netns 内有 listener、guest 信任库没有新增 CA；
写约定名字的客户端能连上；打开端口拦截后，写任意可解析 host 的客户端也落到同一个 listener，
关掉后它恢复直连；身份头里 `handler`/`params` 与声明逐字一致；放到 broker 关闭的节点上会被拒绝
而不是静默无效。

## B 阶段：`tcp` handler 变成真的

今天的 `TcpEchoHandler` 只回一行身份横幅再回显，它是测试夹具，不是中继。

### B1 运营者 allowlist（`crates/aenv-egress/src/policy.rs`、`deploy/k8s/base/config/aenv-egress.toml`）

- 每 handler 一份 CIDR allowlist（0.3 已定），`UpstreamGuard` 在既有拒绝表之后、沙箱策略之前检查。
- 空 allowlist 意味着该 handler 不可用，而不是放行一切。这是 fail-closed 的默认。

### B2 中继实现（`crates/aenv-egress/src/handlers/tcp.rs`）

- 上游取自 `params.upstream`，**必须**过 allowlist；不在表内直接拒绝并记 metric。
- 用 `connect_checked` 建连，`copy_bidirectional` 转发。
- 保留身份横幅行为：要么保留 `TcpEchoHandler` 作为独立名字给集成测试用，要么把集成测试改成对
  夹具上游做真实中继。倾向后者，前者会让 embedded 与 remote 的行为继续分叉。

**B 阶段验收**：allowlist 内的上游能中继；表外的上游被拒且沙箱无法通过任何输入改变上游；
metric 能区分"被 allowlist 拒"和"被沙箱策略拒"。

## C 阶段：结构化凭据与外部解析器

`postgres` 需要的不是一串字节，是一组字段：`host`、`port`、`user`、`password` 为必填，
`database`、`options`、`ttl` 等按 0.2 的选择性覆盖规则可缺省，缺省即透传 guest 写的值。

### C1 `CredentialSource` 返回结构化值（`crates/aenv-egress/src/credential.rs`）

- 现有 `Secret`（不透明字节）保留给 `http` handler，新增结构化变体。改动要保持 `http` 路径
  逐字不变，v1 的缓存语义（TTL、有界、按 `(sandbox, execution, name)` 键）一并沿用。
- 结构化值同样是机密：不进日志、不进 `Debug`、过期即释放。

### C2 外部解析器后端（`crates/aenv-api/src/secrets/`、`crates/aenv-egress/src/`）

解析器同时承接两端，是同一个外部服务的三个调用：

- api 半边：`grant(sandbox, execution, name)` / `revoke(...)` 由写 Vault 改为调外部端点。
  v1 的 `GrantIssuer` 接缝已经在 orchestrator 上，加一个实现即可，**不要改调用点**
  （v1 刚把 `PUT /network` 与九条终态路径接上，改调用点等于把那批修复重做一遍）。
- broker：`get(sandbox, execution, name)` 返回结构化凭据；无 grant 回 403，与 Vault 后端同义。

### C3 后端选择（`config/default.toml`、`docs/src/configuration/`）

- `[secrets].backend` 增加 `external_resolver`，与 `disabled`/`vault` 并列。
- 端点、鉴权方式、超时进配置；文档同步 `env-vars.md` 与 `reference.md`。

**C 阶段验收**：grant/revoke 真的打到外部服务；`get` 返回结构且无 grant 时 403；
`http` handler 的行为与 v1 逐字一致（回归跑 15 套件）。

## D 阶段：`postgres` handler

附录 A 第 4 到第 8 步就是这一阶段的规格，逐条对着写。

### D1 协议前半（`crates/aenv-egress/src/handlers/postgres.rs`，新文件）

- `SSLRequest` 回 `N`，沙箱侧只支持 `sslmode=disable`（0.3）。
- 解析 `StartupMessage`，保留 guest 写的每个参数，等 D2 决定哪些被覆盖。
- `CancelRequest` 是独立新连接，识别后转发到同一上游。

### D2 上游与选择性覆盖

`get` 拿到结构化凭据后，按 0.2 的规则逐字段处理，**解析器没给的字段一律原样透传**：

| 字段 | 解析器给了 | 解析器没给 |
|---|---|---|
| host / port | 连它，且必须落在 B1 的 allowlist 内 | 该授权不可用，合成 `28000` |
| user | 重写 `StartupMessage` 的 user | 透传 guest 写的 user |
| password | 用它对上游完成认证 | 该授权不可用 |
| database | 重写 | **透传 guest 写的库名** |
| options、application_name 等 | 重写 | 透传 |
| replication | —— | 一律拒绝，见 0.2 |

透传 database 的后果要在文档里写明：guest 能点名同一实例上的任意库，但凭据是运营者给的角色，
越权的库会被数据库自己按权限拒绝，失败是 fail-closed 的。它会成为一个库名探测口，可接受。

- 以 TLS 连 `host:port` 并**校验主机名**；自签 CA 的上游把 CA 配给 broker，而不是降级校验。
- **不向 guest 发 SCRAM 挑战**，guest DSN 里那串口令永远不被使用。

### D3 认证与转发

- 上游只接受 `AuthenticationSASL` / SCRAM-SHA-256，以解析器返回的账号完成握手。
- 上游回 `AuthenticationOk` 之后才给 guest 回 `AuthenticationOk`，随后纯字节转发。
- 上游 `ErrorResponse` 原样透传；无 grant 或取值失败时给 guest 合成 `28000`。

**D 阶段验收**：带常量 DSN 的沙箱连上真实 PG 并能查询；把 DSN 里的用户名口令改成任意值，连接
照常成功且落到同一个账号；撤销授权后新连接被拒；解析器没返回的字段（库名、`options`）与 guest
写的逐字一致。

## E 阶段：v1 结转的加固项

这些与 DB 场景无关，但都在 v1.1 清单里，且都是 v1 审查明确推后的半条。

| 项 | 位置 | 内容 |
|---|---|---|
| E1 按 grant 限定范围的 broker 令牌 | api 半边 + broker | **不按原方案做。** 每 grant 一张 Vault 策略要给 api 半边 `sys/policies/acl/*` 写权限，即"能给自己写任意策略"，是更大的暴露。`external_resolver` 后端把这条检查放进了 store：broker 持有的令牌只能按 `(sandbox, execution, name)` 逐条问，问不出未授权的名字。Vault 后端的这条差距写进了 `concepts/egress-credentials.md` 的 Authorization |
| E2 broker 服务端证书单独一张 CA（已做） | `deploy/k8s/base/aenv-egress-secrets.example.yaml`、`tls.rs` | 两张 CA 分开后，签叶证书那张才能加 Name Constraints。这是 S6 的前置 |
| E3 每 secret 的 `allowedHosts`（已做） | `secret_refs` + broker | 把一个密钥能去的上游钉在密钥上，而不只是钉在规则上 |
| E4 叶证书缓存（已做） | `crates/aenv-egress/src/tls.rs` | 陈旧队列项会驱逐刚刷新的叶证书；并发未命中会重复消耗签发预算。修法是刷新时清理旧队列项、签发期间持锁 |
| E5 密钥值全程 Zeroizing（已做） | `src/api/generated` + `adev` | v1 已用 codegen 后处理去掉 XSS 校验与打印型 `Debug`，剩下的是让值从反序列化起就在 `Zeroizing` 里 |
| E6 e2e harness 严格模式（已做） | `scripts/tests/e2e/lib/` | 现在前置条件不满足记为通过是全 harness 的约定。加 `E2E_STRICT` 类开关让选定的套件硬失败，别在单个套件里做 |

## F 阶段：消费方（uns-swe，不在本仓库）

AgentENV 侧不为这个场景加任何专用面，消费方要做的是：

1. agent-platform 实现解析器三端点（`grant` / `revoke` / `get`）。**不做 DDL**：`get` 直接返回
   已有的每库凭据（现成的派生逻辑即可），`grant`/`revoke` 只维护授权记录。
2. 创建请求带 `x-aenv` 显式端点声明与**常量** DSN，DSN 里的用户名口令是占位；模板或 envd 把
   约定的主机名写进 `/etc/hosts`，否则客户端解析不出来就不会发起连接。
3. 停止把真实凭据拼进 `DATABASE_URL` 注入沙箱环境变量。这是本次改造的目的，两个注入点都要改。
4. 租户库的 NetworkPolicy 放行 broker 所在 namespace；自签 CA 的实例把 CA 配给 broker。
5. 删除节点侧代理及其部署清单、principal 签发、相关的扩展参数。

**顺带可拆掉的坑**：现在为了让 guest 连自签 CA 的共享实例，`sslmode` 被翻译成了"加密但不校验"，
而那个值不是 libpq 合法值，模板一旦引入走 libpq 的运行时就会直接报错。改造后 guest 用
`sslmode=disable` 明文连本地 listener，真实 TLS 与校验由 broker 做，这层翻译可以删掉。

## 生命周期（附录 A，实现时逐条对照）

- pause/resume 后 execution 变了，api 重新 grant，消费方按新 execution 记一条授权。
- fork 子沙箱得到父策略副本，api 为子 execution 请求 grant；消费方不签发则子沙箱首连得到 `28000`。
- 吊销是删授权记录，新连接立刻被拒。**不建短期角色的直接代价**：吊销不切断在途连接，
  且生效时间受 broker 凭据缓存的过期时间限制。两条都要写进面向消费方的文档。

## 边界与已知取舍

- guest 到本地 listener 是 netns 内明文（`sslmode=disable`，见 0.3）。这条链路不出宿主机，
  但要在面向消费方的文档里写明，真实 TLS 与校验由 broker 对上游做。
- DSN 长得像真凭据却是占位，文档必须显式说明，否则会被当成泄漏。
- `postgres` handler 对任何 Postgres 协议的上游一样工作，它不是为某一个消费方写的。

## 测试义务（每阶段都要，缺一不可）

1. 单元测试带变异证据。仓库有过守卫对着坏代码全绿的历史，新守卫一律要证明能变红。
2. 集成测试：A/B 阶段可用 `embedded`；D 阶段需要真实 PG，走 e2e。
3. e2e：新增一个套件覆盖 DB 场景，并入 `run_dev_cluster.sh`。基线是 15 套件 141 条通过 0 失败，
   新套件的条数要单独记进 `pve-mf-e2e-invocation` 的判读基线。
4. `make test-unit` 已覆盖 `aenv-egress --features bin`；新 handler 的测试要落在这条线上。
