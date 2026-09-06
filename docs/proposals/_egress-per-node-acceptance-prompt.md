# 验收 prompt：出口凭据 broker 每节点化（第三版），pve-mf

> 交给验收 agent 的工作指令。工作树 `/home/debian/.herdr/worktrees/AgentENV/feat-egress-per-node-broker`，
> 分支 `feat/egress-per-node-broker`。目标集群 pve-mf。验收不通过就写缺陷清单，不要自己修代码。

---

你在 `/home/debian/.herdr/worktrees/AgentENV/feat-egress-per-node-broker` 工作。先读 `CLAUDE.md`、
`docs/proposals/_egress-per-node-implementation.md`（规格）和 `/home/debian/.herdr/reports/egress-per-node/FINAL.md`
（研发报告）。不要改源码、不要提交、不要 push；发现缺陷写进报告。

## 集群入口

- `export KUBECONFIG=~/.kube/config-aenv-mf`，确认 `kubectl get nodes` 看到 `aenv-mf-master`、`aenv-mf-worker`。
- 网关 `http://10.1.0.200:30800`，`/health` 答 204；API 要 `X-API-Key: dummy`。
- 镜像仓库 `10.1.0.201:5000`。**构建在本机做**（201 无 buildx）：`make k8s-build` 后推到该仓库；
  部署用 `deploy/k8s/overlays/pve-mf`。**不要用 `make test-e2e-k8s`**（它会删整个 overlay）。
- 控制面门禁在此集群是 armed 的：直连 api 半边要带 `x-agentenv-control-plane: <token>`
  （Secret `agentenv-control-plane-token`）。
- e2e 驱动：
  ```bash
  KUBECONFIG=$HOME/.kube/config-aenv-mf \
  E2E_DEV_CLUSTER_GATEWAY_NODEPORT_URL=http://10.1.0.200:30800 \
  bash scripts/tests/e2e/run_dev_cluster.sh
  ```
  基线（含套件 15、16）：141 条真断言 / 9 条跳过 / 0 失败。套件 12 与 14 是空跑，exit 0 不携带信息。
- 集群目前跑的是 `remote` 形态。迁移按规格 P5 的五步做，第 4 步会清空存量沙箱，先确认集群上没有需要保留的沙箱。

## 验收顺序

1. **静态审查**：对 `git diff dev...HEAD` 做一遍对抗审查，重点核对规格 §0"原样保留"的每一条没有退化、
   `make check-crate-boundaries` 的守卫仍然成立、`cargo tree -p aenv-node` 无 TLS 新依赖、
   `IdentityHeader` 无 `hmac`/`nonce`。四个 make 门禁与 `make -C services test -count=1` 在本机重跑一遍。
2. **准入检查（P3 门槛）**：pve-mf 的 k8s 版本支持 `TokenReview` 且 api SA 能 `get pods`；
   不成立则在报告首行写"准入失败"，只做第 1 步与第 3 步，不部署。
3. **集成测试**：在 pve-mf 的 worker 节点上（root，`/dev/kvm`）跑 `make test-agent-integration`，
   含 `tests/fixtures/egress-local-overlay.toml` 那次调用。记录 P0 的三条隔离断言与 P2 的放置回归测试结果。
4. **部署迁移**：按 P5 五步，每步之间 `kubectl get pods -A -o wide` 与 `/nodes` 的 `egressBroker` 字段截图进报告。
5. **e2e**：全套跑一遍，与基线逐套件比对；新增断言（T1、P3 三条、15/16 的改写）逐条列出结果。
6. **手工探针**：
   - guest 内 `curl` node 的 `veth_host_ip` 8000/8001/9103 与 DNS 服务器的 8080 端口，全部失败；
   - guest 内 `curl -X TRACE https://<规则域名>` 得 405；
   - pause 后 resume 到另一节点（用 drain 让 origin 不可选），resume 前起的 `node -e` 长驻进程仍能访问规则域名；
   - 用节点 A broker 的 SA token 手工调 resolve 取节点 B 沙箱的 grant，得 404；
   - `kubectl rollout restart ds/aenv-egress`，滚动期间沙箱存活，规则域名短暂 ECONNREFUSED 后恢复；
   - `kubectl delete pod` 沙箱所在节点的 broker Pod，观察 `/nodes` 的 `egressBroker` 变为 `LOCAL_UNREACHABLE`，
     新建带规则沙箱不落到该节点。
7. **回滚演练**：按 `docs/src/internals/services.md` 的三张回滚表，把 broker 回滚一个版本再前滚，沙箱存活。

## 报告

写到 `/home/debian/.herdr/reports/egress-per-node/ACCEPTANCE.md`：

1. 首行：通过 / 不通过 / 准入失败。
2. 每个验收步骤的结果与证据（命令、关键输出、e2e 计数）。
3. 缺陷清单：每条含复现步骤、期望、实际、对应规格条目、严重度（阻断 / 重大 / 次要）。
4. 与基线的差异表（套件 × 断言数）。
5. 你没能验证的项与原因。

不要修代码。不要在报告里复述规格。
