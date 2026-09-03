# 研发 prompt：沙箱出口凭据代理 v1

> 交给实现 agent 的工作指令。工作树 `/home/debian/.herdr/worktrees/AgentENV/feat-egress-credential-brokering`，
> 分支 `feat/egress-credential-brokering`（基于 `dev`@71a2d5f）。按阶段推进，每个阶段结束停下来等评审。

---

你在 Rust 工作区 `/home/debian/.herdr/worktrees/AgentENV/feat-egress-credential-brokering` 工作，
分支 `feat/egress-credential-brokering`。不要切分支、不要 stash、不要 `git checkout`/`reset` 到别的提交、
不要 push。先读 `CLAUDE.md`，再按顺序读这两份文档，它们是本任务的唯一规格：

1. `docs/proposals/2026-09-03-sandbox-egress-credential-brokering.md`（方案第二版）：§1 决定、§2 目标与
   约束、§4 方案、§6 审查裁决表、§7 分期。
2. `docs/proposals/_egress-brokering-implementation.md`（实现计划第二版）：P0、P1、P2、P3 的文件、类型、
   测试。行号已按代码核实，但以当前代码为准。

## 任务

实现方案 §7 的 v1，按 P0 → P1 → P2 → P3 推进。**每完成一个阶段就停下来**，给出该阶段的报告（见"报告
格式"），等评审通过再进入下一阶段。不要为了赶进度跳阶段，也不要把 v1.1 / v2 的内容（显式端点、tcp
allowlist、postgres handler、外部解析器、aenv-secrets、内联值转存、多租户）提前做进来。

## 每个阶段的验收

- **P0**：`crates/aenv-node/src/sandbox/network/slot.rs` 的反伪造 DROP 规则。它必须在
  `initialize_namespace_egress_chain` 把 `AGENTENV-EGRESS` Insert 到 FORWARD 位置 1 **之后**再 Insert 到
  位置 1；先插会被挤到位置 2 而失效。`src/sandbox/network/policy.rs` 已有命令序列断言，加一条断言相对
  顺序。`make test-unit` 绿。这是独立 PR，先合。
- **P1**：新 crate `crates/aenv-egress`，feature `core`（默认）与 `tls`（本阶段不实现 tls 内容，只留
  feature 门）。实现计划 P1 列出的模块：`header`（含 `original_dst`、`egress` 摘要、`nonce`、HMAC、
  ReplayCache）、`framing`、`transport`（`BrokerTransport` + `EmbeddedTransport`）、`handler`、
  `credential`（`Secret` 用 `zeroize`，`Debug` 打码）、`policy`（`UpstreamGuard`：解析 → 对 IP 检查
  → 只连检查过的 IP）、`dispatch`、`handlers/tcp`、`runtime`。`Cargo.toml` 加 members、`nix` 的
  `socket` feature、`zeroize`；`Makefile` 的 `check-crate-boundaries` 加 aenv-egress 规则。
  全部单元测试按实现计划 P1 的清单写，`make test-unit`、`make clippy`、`make fmt`、
  `make check-crate-boundaries` 全绿。**没有网络、没有 k8s。**
- **P2**：策略模型（`rules` 公开形态 + `brokers` 内部形态，都 `#[serde(default)]`；
  `has_runtime_egress_rules` 纳入 brokers；`has_explicit_rules` 不纳入）、openapi 的 `rules` 与
  `/secrets`（`make agentenv-server` 重生成，不手改生成目录）、aenv-api 的 `secrets/`（`SecretsBackend`
  + `VaultKv2Backend`、PG 表 `secret_refs`、grant 生命周期）、`[egress_broker]` 与 `[secrets]` 配置
  段及校验、`Slot::listen_in_namespace` / `install_intercept` / `remove_intercept`、
  `crates/aenv-node/src/sandbox/egress/mod.rs` 的 `BrokeredEndpoints`（accept 即 spawn、Semaphore、
  `SO_ORIGINAL_DST`、open 超时、`shutdown().await` 在 `release(slot)` 之前）、`start_fresh` /
  `start_resume` 两个分支 / `update_network_policy` / `stop` / `Drop` 的挂接、`wait_for_ready` 的
  init **每次**传 `ca_bundle`（无 rules 传空）与默认信任 env、CA 存在性探测、心跳 `NodeSnapshot.
  egress_broker` 枚举（proto 字段 17，`services/Makefile` 重生成 Go 侧）与注册表过滤、envd 版本升级
  并登记 `docs/src/internals/sandbox-testing.md`。golden 测试与集成测试按实现计划 P2.1 / P2.6。
  集成测试需要 root 与 `/dev/kvm`；本机跑不了的（见"环境"）写好并在报告里标明未执行。
- **P3**：`RemoteTransport`（openssl 客户端）、`aenv-egress` bin（openssl 服务端、verify、replay
  cache、指标）、`tls` feature 的 `CaSigner`（先匹配再签、LRU、限速、Name Constraints）与 `http`
  handler（peek ClientHello、命中终止 TLS、header 替换同名一律替换、`UpstreamGuard`、上游校验证书、
  WebSocket 升级、HTTP/2 → 505、Denied → 403 / Unavailable → 502 带 `x-aenv-egress-reason`、未命中透传）、
  部署清单（Deployment、Service、NetworkPolicy、Secret 模板、`Dockerfile.egress`、DaemonSet env、
  compose 用 embedded）、`docs/src/concepts/egress-credentials.md`、e2e 七条。

## 硬约束（违反即返工）

- **TLS 栈只有 openssl / native-tls**。工作区 `Cargo.toml:128-135` 明写禁止第二个栈，rustls 是
  `crates/aenv` 的唯一例外。`aenv-egress` 的叶证书签发用 `openssl` crate，不引 rustls、rcgen。
  `aenv-node` 只链 `aenv-egress` 的 `core` feature，`cargo tree -p aenv-node` 不得出现 TLS 相关新依赖。
- **aenv-api 与 aenv-node 不落任何密钥值**：值不进 PG、不进 Redis、不进日志、不进 span、不进错误
  文案。`SecretString` 是独立类型，`Debug` 打码。
- **公开面只有 E2B 形状的 `network.rules` 与 `/secrets`**，字段级对齐 `/home/debian/e2b-infra/spec/
  openapi.yml:457-528` 与 `:2231-2300`。`brokers`、`handler`、`params` 不进 openapi。
- **G8**：broker 侧连任何上游前必须经 `UpstreamGuard`，对解析后的 IP 检查并只连该 IP。不留第二个连接入口。
- **契约里没有进程内对象**：`IdentityHeader`、`ConnCtx` 不含 `Slot`、iptables 句柄、netns 路径、fd。
- `CLAUDE.md` 的注释规则：`///` 只写契约，一到三行；测试名即文档，测试上方不写段落；源码里不写历史、
  不写"第一版曾经…"、不写 emoji。
- 生成目录（`src/api/generated`、`thirdparty/*`、Go 侧 `.pb.go`）只能重生成，不能手改。
- 测试放它所属的 crate：`aenv-core` 的进 `src/`，节点的进 `crates/aenv-node`，集成测试进
  `crates/aenv-node/tests/`。`make test-unit` 跑 `--lib --bins`，不要把测试放到它看不见的地方。
- 每个阶段一次提交，Conventional Commit 前缀（`feat:` / `fix:` / `test:` / `docs:`），提交信息不带任何
  attribution 行。不 push。

## 环境

- 本机是构建机，内核 6.1，**没有 ublk，也跑不了需要 root + `/dev/kvm` 的集成测试**；能跑的是
  `make test-unit`、`make clippy`、`make fmt`、`make check-crate-boundaries`、`make -C services test`。
  需要真机的测试写好、编译通过、在报告里标"未执行，需 pve-mf"。
- Go 侧改了 proto 或 `config/default.toml` 后，`services/` 的测试要带 `-count=1`，否则命中缓存假绿。
- 长命令只用前台 Bash，等待写在同一条命令里；单条上限 600000 ms。**永远不要**用 `pgrep -f` / `pkill -f`
  找或杀自己启动的进程，用记下的 PID。
- 构建产物容易把盘打满；`cargo build` 报错但输出为空时先看 `df -h`。

## 报告格式（每阶段结束时）

1. 一句话：阶段是否完成，哪些验收项通过。
2. 改动文件清单，每个文件一行说明。
3. 跑过的命令与真实结果（贴关键输出，不要只说"全绿"）；未执行的测试逐条列出并说明原因。
4. 与方案或实现计划**不一致**的地方：你改了什么、为什么。没有就写"无"。
5. 你不确定、需要评审拍板的问题，每条一句。

不要在报告里复述方案。不要在阶段之间自行决定继续。
