# 研发 prompt：出口凭据 broker 每节点化（第三版）

> 交给实现 agent 的工作指令。工作树 `/home/debian/.herdr/worktrees/AgentENV/feat-egress-per-node-broker`，
> 分支 `feat/egress-per-node-broker`（基于 `dev`@a9ee98d）。按阶段推进，每阶段一次提交，不等评审，
> 全部完成后写总报告。

---

你在 Rust 工作区 `/home/debian/.herdr/worktrees/AgentENV/feat-egress-per-node-broker` 工作，分支
`feat/egress-per-node-broker`。不要切分支、不要 stash、不要 `git checkout`/`reset` 到别的提交、不要 push。
先读 `CLAUDE.md`，再读 `docs/proposals/_egress-per-node-implementation.md`，它是本任务的唯一规格；
`docs/proposals/2026-09-03-sandbox-egress-credential-brokering.md` 与 `_egress-brokering-implementation.md`
只作背景，与新规格冲突处以新规格为准。

## 任务

按 P0 → P1 → P2 → P3 → P4 → T1 → P5（代码与文档部分）推进。每完成一个阶段：跑该阶段的验收命令、
一次提交、在 `/home/debian/.herdr/reports/egress-per-node/P<n>.md` 写阶段报告（格式见下），然后**直接进入
下一阶段，不要停下来等评审**。全部完成后写 `/home/debian/.herdr/reports/egress-per-node/FINAL.md`，
内容是各阶段报告的汇总加"需 pve-mf 验证的清单"，然后停下。

不要把 v1.1 的内容（全端口 TCP 出口代理、域名 allowOut 实现、每规则审计级别、Firecracker 专用 uid）
提前做进来。P5 的集群操作部分不是你的事，只提供清单与脚本。

## 硬约束（违反即返工）

- 规格 §0 里"原样保留、不得退化"的每一条。
- TLS 栈只有 openssl / native-tls；`aenv-node` 不链接 `aenv-egress` 的 `tls`；`aenv-api` 不链接 `aenv-egress`。
- 值不进日志、span、错误文案；审计日志只记 header 名不记值。
- `IdentityHeader` 版本升 2 后 broker 拒绝旧版本；不要留兼容旧帧的分支。
- 生成目录（`src/api/generated`、`thirdparty/*`、`services/api/proto/*.pb.go`）只重生成，不手改。
- 测试放它所属的 crate；`make test-unit` 跑 `--lib --bins`。测试不得写出可执行文件再执行。
- 注释规则见 `CLAUDE.md`：`///` 只写契约，测试名即文档，源码里不写历史与"曾经"。
- 每阶段一次提交，Conventional Commit 前缀，提交信息不带 attribution 行。不 push。
- 遇到规格与代码不一致：以代码事实为准做最小偏离，并在阶段报告第 4 条写明；不要为此停下。
- 遇到需要用户拍板的问题：按规格里的默认值做，报告第 5 条列出；不要停下。

## 环境

- 本机是构建机，内核 6.1，没有 ublk，跑不了需要 root + `/dev/kvm` 的集成测试；能跑的是
  `make test-unit`、`make clippy`、`make fmt`、`make check-crate-boundaries`、`make -C services test -count=1`、
  `kustomize build deploy/k8s/overlays/pve-mf`。需要真机的测试写好、编译通过，报告里标"未执行，需 pve-mf"。
- Go 侧改了 proto 或 `config/default.toml` 后，`services/` 的测试带 `-count=1`。
- 长命令只用前台 Bash，单条上限 600000 ms。不要用 `pgrep -f` / `pkill -f`，用记下的 PID。
- `cargo build` 报错但输出为空时先看 `df -h`。
- 参照实现在 `/home/debian/e2b-infra` 与 `/home/debian/CubeSandbox-latest`，只读。

## 报告格式（每阶段）

1. 一句话：阶段是否完成，哪些验收项通过。
2. 改动文件清单，每个文件一行。
3. 跑过的命令与真实输出的关键行；未执行的测试逐条列出并说明原因。
4. 与规格不一致的地方：改了什么、为什么。没有就写"无"。
5. 需要评审拍板的问题，每条一句。

不要在报告里复述规格。
