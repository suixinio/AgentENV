# 沙箱出口凭据代理 v1.1：施工清单

> 2026-09-04 · 承接 `2026-09-03-sandbox-egress-credential-brokering.md` 的 §7 v1.1 与附录 A。
> v1 已完成并在 pve-mf 验证（分支 `feat/egress-credential-brokering`，基线 `2f45023`）。
> 这份是"改哪些文件、加哪些类型、测什么"，以及每阶段的验收判据。未实施。

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

## 0.2 动工前必须裁决的四件事

这四件事影响接口形状，先定再写，否则要返工：

1. **显式端点声明放在哪。** 方案说走 `x-aenv-` 前缀的扩展字段，挂在 `network` 上。要确认它与
   E2B 的 `network.rules` 并存时的校验顺序，以及 `brokers` 是否就此变成半公开。
2. **`sslmode` 的支持面。** handler 对 `SSLRequest` 回 `N`，所以统一 DSN 只能是 `sslmode=disable`
   或不写。若消费方需要 `require`/`verify-full`，就要给 `helium` 现签证书并把 CA 送进 guest，
   那等于把显式端点的"无 CA"前提推翻。默认建议：v1.1 只支持 `disable`，写进文档。
3. **托管库没有建角色权限时走哪条路。** 退路是解析器直接返回 `rw_` 凭据，真实凭据只在 broker
   内存里按 grant 存活。要确认这条退路是否算 v1.1 的一等场景（影响解析器契约的必填字段）。
4. **allowlist 的粒度。** 每 handler 一份，还是全局一份加 handler 标签。附录 A 写的是"运营者为
   `postgres` handler 配置的 CIDR allowlist"，倾向每 handler。

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

### A3 运行时（`crates/aenv-node/src/sandbox/egress/`、`src/sandbox/network/policy.rs`）

- 在沙箱 netns 内 bind `169.254.0.22:<port>`。**显式端点没有 DNAT，也不需要 CA 进 guest**，
  与 v1 的透明拦截是两条独立路径，不要复用 `install_intercept`。
- `helium` 之类的友好名：默认 env `AENV_EGRESS_HOST` 加模板 `/etc/hosts` 别名（§6 P8）。
- v1 刚修的 `Drop for BrokeredEndpoints` 与 `replace()` 顺序对显式端点同样适用，复用现有实现。

**A 阶段验收**：一个只声明 `tcp` 显式端点的沙箱起来后，netns 内有 listener、没有 DNAT 规则、
guest 信任库没有新增 CA；身份头里 `handler`/`params` 与声明逐字一致；放到 disabled 节点上会被
拒绝而不是静默无效。

## B 阶段：`tcp` handler 变成真的

今天的 `TcpEchoHandler` 只回一行身份横幅再回显，它是测试夹具，不是中继。

### B1 运营者 allowlist（`crates/aenv-egress/src/policy.rs`、`deploy/k8s/base/config/aenv-egress.toml`）

- 新增每 handler 的 CIDR allowlist 配置，`UpstreamGuard` 在既有拒绝表之后、沙箱策略之前检查。
- 空 allowlist 意味着该 handler 不可用，而不是放行一切。这是 fail-closed 的默认。

### B2 中继实现（`crates/aenv-egress/src/handlers/tcp.rs`）

- 上游取自 `params.upstream`，**必须**过 allowlist；不在表内直接拒绝并记 metric。
- 用 `connect_checked` 建连，`copy_bidirectional` 转发。
- 保留身份横幅行为：要么保留 `TcpEchoHandler` 作为独立名字给集成测试用，要么把集成测试改成对
  夹具上游做真实中继。倾向后者，前者会让 embedded 与 remote 的行为继续分叉。

**B 阶段验收**：allowlist 内的上游能中继；表外的上游被拒且沙箱无法通过任何输入改变上游；
metric 能区分"被 allowlist 拒"和"被沙箱策略拒"。

## C 阶段：结构化凭据与外部解析器

`postgres` 需要的不是一串字节，是 `{host, port, database, user, password, ttl}`。

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

- `SSLRequest` 回 `N`（见 0.2 第 2 条裁决）。
- 解析 `StartupMessage`，取出 guest 的占位 `user`/`database`。
- `CancelRequest` 是独立新连接，识别后转发到同一上游。

### D2 上游与重写

- `get` 拿到结构化凭据，`StartupMessage` 的 `user`/`database` **重写**为解析器给的值，
  剔除 `replication` 参数。
- 以 TLS 连 `host:port` 并校验主机名；地址必须落在 B1 的 allowlist 内。
- **不向 guest 发 SCRAM 挑战**，guest DSN 里的占位口令永远不被使用。

### D3 认证与转发

- 上游只接受 `AuthenticationSASL` / SCRAM-SHA-256，以短期角色完成握手。
- 上游回 `AuthenticationOk` 之后才给 guest 回 `AuthenticationOk`，随后纯字节转发。
- 上游 `ErrorResponse` 原样透传；无 grant 或取值失败时给 guest 合成 `28000`。

**D 阶段验收**：e2e 中一个带常量 DSN 的沙箱连上真实 PG 并能查询；撤销后角色被删、在途连接被
PG 切断；沙箱换任何 DSN 参数都不能改变实际连到的库与角色。

## E 阶段：v1 结转的加固项

这些与 DB 场景无关，但都在 v1.1 清单里，且都是 v1 审查明确推后的半条。

| 项 | 位置 | 内容 |
|---|---|---|
| E1 按 grant 限定范围的 broker 令牌 | api 半边 + broker | 签发 grant 时同签只读得到该 grant 名字的 scoped/wrapped 令牌，把 §4.6 的应用层检查变成 store 层约束。这是 S3 未消除的那一半 |
| E2 broker 服务端证书单独一张 CA | `deploy/k8s/base/aenv-egress-secrets.example.yaml`、`tls.rs` | 两张 CA 分开后，签叶证书那张才能加 Name Constraints。这是 S6 的前置 |
| E3 每 secret 的 `allowedHosts` | `secret_refs` + broker | 把一个密钥能去的上游钉在密钥上，而不只是钉在规则上 |
| E4 叶证书缓存 | `crates/aenv-egress/src/tls.rs` | 陈旧队列项会驱逐刚刷新的叶证书；并发未命中会重复消耗签发预算。修法是刷新时清理旧队列项、签发期间持锁 |
| E5 密钥值全程 Zeroizing | `src/api/generated` + `adev` | v1 已用 codegen 后处理去掉 XSS 校验与打印型 `Debug`，剩下的是让值从反序列化起就在 `Zeroizing` 里 |
| E6 e2e harness 严格模式 | `scripts/tests/e2e/lib/` | 现在前置条件不满足记为通过是全 harness 的约定。加 `E2E_STRICT` 类开关让选定的套件硬失败，别在单个套件里做 |

## F 阶段：消费方（uns-swe，不在本仓库）

AgentENV 侧不为这个场景加任何专用面，消费方要做的是：

1. agent-platform 实现解析器三端点（`grant` / `revoke` / `get`）。
2. 收到 grant 时在租户 PG 上 `CREATE ROLE sbx_<execution> LOGIN PASSWORD '…' VALID UNTIL '<ttl>'
   IN ROLE rw_<db>`，收到 revoke 时 `DROP ROLE`（它已经在 `tenant_pg/lifecycle.go` 跑 DDL）。
3. 创建请求带 `x-aenv` 显式端点声明与**常量** DSN；DSN 里的 user/password/database 都是占位，
   文档必须写明这一点。
4. 租户 PG 的 NetworkPolicy 放行 broker 所在 namespace。
5. 删除 `apps/dab-node-agent`、`40-node-agent.yaml`、DAB 部署清单、`db_access` 的 principal 签发、
   `customExtensionParams.dab`。

## 生命周期（附录 A，实现时逐条对照）

- pause/resume 后 execution 变了，api 重新 grant，消费方建新角色删旧角色。
- fork 子沙箱得到父策略副本，api 为子 execution 请求 grant；消费方不签发则子沙箱首连得到 `28000`。
- 吊销就是 `DROP ROLE`，是数据库层的事实，不依赖任何缓存 TTL，在途连接由 PG 切断。

## 边界与已知取舍

- guest 到 `helium` 是 netns 内明文。这条链路不出宿主机，但要在文档里写明。
- DSN 长得像真凭据却是占位，文档必须显式说明，否则会被当成泄漏。
- `postgres` handler 对任何 Postgres 协议的上游一样工作，它不是为某一个消费方写的。

## 测试义务（每阶段都要，缺一不可）

1. 单元测试带变异证据。仓库有过守卫对着坏代码全绿的历史，新守卫一律要证明能变红。
2. 集成测试：A/B 阶段可用 `embedded`；D 阶段需要真实 PG，走 e2e。
3. e2e：新增一个套件覆盖 DB 场景，并入 `run_dev_cluster.sh`。基线是 15 套件 141 条通过 0 失败，
   新套件的条数要单独记进 `pve-mf-e2e-invocation` 的判读基线。
4. `make test-unit` 已覆盖 `aenv-egress --features bin`；新 handler 的测试要落在这条线上。
