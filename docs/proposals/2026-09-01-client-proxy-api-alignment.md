# gateway 对齐 e2b client-proxy、aenv-api 对齐 e2b api：实施方案

> 2026-09-01 · 基线 `dev`@`8248c8f`。配套
> [`2026-08-20-module-responsibilities.md`](2026-08-20-module-responsibilities.md)（§4.3 与连边表 A1/A2 是本方案的终态依据）、
> [`2026-08-31-residue-decisions.md`](2026-08-31-residue-decisions.md)（本方案落地后取代其 D2）、
> [`2026-09-01-bidirectional-reconciler.md`](2026-09-01-bidirectional-reconciler.md)（并行项，见 §7）。
> e2b 侧事实来自本地 `/home/debian/e2b-infra`@`fdc3359` 逐行核对 + DeepWiki（e2b-dev/infra）交叉确认，
> AgentENV 侧全部 file:line 已在 `8248c8f` 上验证；§4 与 P4 的行号已在 `feat/client-proxy-alignment` HEAD 上重新核对。

## 1. 目标与依据

分两层对齐：

- **职责对齐**（P1–P3）：`services/gateway` 收窄为 e2b `packages/client-proxy` 的同位物——
  纯沙箱数据面边缘；控制面 REST 入口收进 `aenv-api`，对齐 e2b `packages/api` 的入口职责。
- **连边对齐**（P4）：e2b client-proxy 的控制面连边恰好一条（resume gRPC）。我们的 gateway
  今天挂着两个面（apiproxy + scheduler.v1 的 LookupNode/RecordAssignment）。2026-08-20
  设计图的连边表 A1/A2 目标就是 e2b 形状——Redis 直读 + 一个恢复 RPC——P4 走完这一步。

这不是新方向。设计图 §4.3 给 gateway 的目标职责表只有 `proxy`、`resume`、`execution_fencing`
三项，`internal/{node,cluster,registry}_list` 标为删除。当前树上多出来的三块——REST 转发
（`rest_upstream.go`）、`/nodes*`（`node_list.go`）、`/registry/sandboxes`（`registry_list.go`）——
是拆分迁移期（3a 起）的产物，服务拆分完成（Go scheduler 已于 2026-08-28 裁决并完整下线）后没有回收。

一处成文立场随本方案作废，记录在此：`services/gateway/internal/rest_upstream.go` 的
「There is one position, not two」论证的是上游只能是 api 而非 node；它没有回答
「gateway 是否应该转发 REST」。本方案回答：不应该。

另一处成文论证在 P4 作废：`server.go` 冷路径注释「an api that is down costs latency and
not availability」写于 scheduler 还是独立进程的年代——如今 scheduler.v1 由同一个 aenv-api
进程服务，api 挂了 LookupNode 一样挂，第三级不再是进程级容灾。

`src/api/impls/auth.rs` 的「access control belongs at the network boundary」**继续有效**：
本方案不改变暴露面——gateway 对 REST 本就零鉴权只做转发。鉴权见 §6 的条件项。

## 2. 终态契约（部署无关）

对外契约从一个入口地址变为两个。**不预设 k8s Ingress 或任何入口产品**——
域名/端口如何映射到这两个地址是运维文档的事，仓库只保证两个地址各自完整：

| 地址 | 进程 | 承接 |
|---|---|---|
| REST 入口 | `aenv-api` | 全部用户 REST（sandbox/snapshot/template/nodes/registry） |
| 数据面入口 | `gateway` | Host（`{port}-{sandboxID}.<domain>`）或路由头（`x-agentenv-sandbox-id`/`x-agentenv-target-port`）流量 → `aenv-node /proxy` → VM |

e2b 把这条分流写在自己仓库的 `iac/provider-gcp/nomad-cluster/network/main.tf`
（`api.<domain>` → api backend、`*.<domain>` → session backend，:45-66,245-311）。
我们的对应物是 `deploy/k8s/base` 与 `deploy/docker-compose.yml` 两套参考部署——
它们是产品的一部分（有守卫测试逐文件扫描），随阶段同步改。

P4 后 gateway 的全部连边：Redis routing projection（直读）＋ `apiproxy.ResumeSandbox`
（一条 RPC）——与 e2b client-proxy 同形。scheduler.v1 只剩 node↔api 面。

## 3. 职责终态表

| 职责 | gateway（终态） | aenv-api（终态） | e2b 同位物 |
|---|---|---|---|
| 用户 REST 入口 | ✂ 删除转发（P3） | ✔ 唯一入口（现已实现全部端点） | LB 直达 `packages/api` |
| 客户鉴权 | 不做 | 不做——维持 `auth.rs` 网络边界立场（§6 条件项） | `packages/auth` 认证链（公网多租户 SaaS 的前提，我们没有） |
| `/nodes`、`/nodes/{id}` | ✂ 删 `node_list.go`（P3） | ✔ 接入 node registry 后才是 fleet 端点（P1）——原 `admin.rs` 实现只答本进程 observability 快照 | `AdminNodes`/`AdminNodeDetail`（api 内） |
| `/registry/sandboxes` | ✂ 删 `registry_list.go`（P3） | ➕ 新增 REST 端点（P1） | 无同名物；同类查询在 api 控制面 |
| Host/路由头解析、反代、WebSocket | ✔ 保留 | — | `client-proxy` 核心 |
| 合成响应的 CORS | ➕ 采纳 e2b 形制（P3） | — | `shared/pkg/proxy/cors` |
| 冷路径 | projection 直读 → resume（P4 后两级） | resume 内化「先查 running、再按门控 wake」的完整判定（P4） | catalog 直读 → resume，同为两级 |
| execution fencing、内部 header 加盖 | ✔ 保留（喂源 P4 后收为 projection + resume 响应） | — | 无同位物，永久偏离 |
| 路由修复 | ✂ RecordAssignment 移交（P4） | wake 与 running-miss 时自写投影；reconciler 只作命中率优化 | catalog 只由 api 写 |

## 4. 与 e2b 的偏离：HEAD 上的唯一完整清单

「对齐」指职责与连边，不是逐行抄写。下面是本分支 HEAD 上**全部**有意偏离——不在此表的差异
不是偏离，是待对齐项或疏漏。每条一行理由、两侧 file:line（AgentENV 侧按本分支 HEAD 核对，
e2b 侧按 `fdc3359`）。前五条是原「永久两项、过渡三项」：过渡三项在 P4 折叠，其余保留。

1. **execution fencing**（`services/gateway/internal/execution_fencing.go`）——拒旧 incarnation；
   e2b 无同位物。永久：暂停/占位/收口状态机更复杂。P4 后喂源收窄为 projection 记录
   （自带 `execution_id`，`services/shared/routing/record.go:71`）与 resume 响应
   （`services/api/proto/apiproxy/apiproxy.proto:90` 的 `execution_id`），不再依赖任何
   scheduler RPC。
2. **apiproxy 不加 OIDC edge 鉴权**（`services/gateway/internal/resume/client.go:224` 一条裸
   RPC）；e2b `packages/api/internal/handlers/proxy_grpc.go:34-41` 的
   `requireEdgeClientProxyAuth` 服务跨集群 edge，我们单集群内网。永久；「一个 RPC」纪律不变。
3. **LookupNode 第三级**（`services/gateway/internal/server.go:491` `lookupNodeColdPath`）；
   e2b 的 client-proxy 只有 catalog 直读 → resume 两级
   （`packages/client-proxy/internal/proxy/proxy.go:77`）。过渡，P4 折叠：剩余承重只有
   「Wake 答 Undecided 但 LookupNode 仍可答」的窗口（wake 需要 PG，LookupNode 可从 Redis
   binding/心跳清册应答）。
4. **RecordAssignment**（`server.go:882`，触发在 node 代理路径的 ModifyResponse，门控
   `assignment != None` + 2xx）；e2b 的 catalog 只由 api 写
   （`packages/api/internal/sandbox/store.go:43-44`）。过渡，P4 移交 api 自写投影；修复职责与
   reconciler 重叠。
5. **gateway → scheduler.v1 拨号本身**（`services/shared/config/config.go:164`
   `scheduler_addr`）；e2b client-proxy 的连边只有 Redis + apiproxy。过渡，上两项的载体。
6. **无沙箱的请求答 404**（`server.go:265-274`：无 Host 路由、无路由头 → 404）；e2b 没有
   proxy 域名允许表，任何不合形的 Host 一律 400 "Invalid host"
   （`packages/shared/pkg/proxy/host.go:74-88`、`handler.go:36-41`）。保留：
   `sandbox_proxy_domains` 允许表让「这不是数据面地址」成为一个真实、可区分的状态，
   残余 REST 调用方拿到的信号也更清楚。
7. **运维可设的节点状态只有 `ready`/`draining`**，在 `node.proto` 的
   `NodeStatusOverrideRequest` 里是一个 bool，409 表示「不可设」
   （`src/api/impls/admin.rs:106-115`）；e2b 转发 `ready|draining|unhealthy|standby`
   （`packages/api/internal/orchestrator/nodemanager/status.go:21-26`，:145-146 打
   `ServiceStatusOverride`），409 留给 orchestrator 对 draining→standby 的 FailedPrecondition
   （`packages/orchestrator/pkg/service/info.go:63-65`）。`lingering` 无 e2b 同位物。保留现状。
8. **api 半边只查 admin 头是否存在**（`src/api/impls/auth.rs:26-28`）；e2b 对 admin token
   做等值比较（`packages/api/main.go:176-183` 的 `NewAdminApiKeyAuthenticator(config.AdminToken)`）。
   按 §6 条件项保留。
9. **node 半边仍以 `x-agentenv-control-plane` 门禁 `/nodes*`**（`src/api/server.rs:144-148`
   只在不拥有用户 REST 的半边挂 `require_control_plane`，头名在
   `src/api/control_plane_gate.rs:23`）；e2b 的 orchestrator 没有 REST 面，其 gRPC 不鉴权。
   按 P3 实施修订保留。
10. **冷路径先查 running、再看 autoResume 门控**（P4 的 api 侧判定）；e2b
    `packages/api/internal/handlers/proxy_grpc.go:99-125` 的 `getAutoResumeSnapshot` 跑在
    `GetSandbox`（:179-208）之前，政策非 Any 时连从未暂停的 running 沙箱在 catalog miss 上
    也答 NotFound。保留（P4 明确项）：autoResume:false + running 必须可路由。
11. **running-miss 时 api 回写投影**（P4 的 api 侧）；e2b 的 edge 从不回写，
    `packages/client-proxy/internal/proxy/proxy.go:77` 是唯一一次 catalog 调用，miss 即重发
    RPC。保留（P4 明确项）：对命中率的既定改良。
12. **CLI 取两个显式地址**（`crates/aenv/src/auth.rs` 的 `url` + `proxy_url`，缺一拒绝运行）；
    e2b 从一个域名推导两个入口（`api.<domain>` / `<port>-<id>.<domain>`），外加每沙箱可选的
    `domain` 字段。按部署无关裁决（§2）保留：仓库不预设任何域名产品。

## 5. 分阶段实施

四个阶段 + 一个可选项，每阶段可独立合入、独立回滚（镜像 digest 回退，与 D3 的回滚口径一致；
**P3 例外**——它删配置也删门禁，纯镜像回退不完整，回滚口径见 P3 部署段的回滚窗口约定）。
顺序约束：P1 与 P2 互相独立；**P3 必须在 P2 之后**（客户端先搬家，转发再断）；
**P4 必须在 P3 上线稳定之后**（reconciler 不再卡 P4——2026-09-01 裁决降格为并行项，
依据与替代验收见 P4 验收段）。

### P1 — `/registry/sandboxes` 收进 REST 面，`/nodes` 形状对齐

- 先做形状对账：`node_list.go` 手搓的 `nodeListItem` JSON vs `openapi.yml` `/nodes` 模型，
  逐字段 diff；缺的字段扩进 openapi 模型（canonical = openapi），消费方（e2e 07 套件、运维脚本）
  按新形状核对断言。
- **数据源对账**（形状之外的一刀，2026-09-01 pve-mf 验收暴露）：`admin.rs` 的 `/nodes`、
  `/nodes/{id}` 原本只读本进程 `ObservabilityService` 快照——在 split 部署里那是**应答请求的
  那个 api 副本自己**，一行，不是集群清册；split 模式套件 11 因此等两个节点等到超时（网关版
  答 2 节点，api 版答 1 行自身）。api 半边持有进程内 node registry，接上它才是 fleet 端点。
  **双形态是刻意的**：`role_gate.rs` 的 `node_serves` 放行 node 侧的 `GET /nodes` 与
  `GET|POST /nodes/{id}`（其余生成路由一律拒），所以本地快照语义是 node 的自报面，必须留着；
  形态由句柄决定——持 registry 答集群、不持答自身。REST 渲染与 `ListObservedNodes` 走
  `node_registry::fleet` 同一读取路径，字段级 parity 测试钉住 `nodeListItem` 的 JSON 标签与
  `nodeStatusToString` 的拼写（含 `lingering`）。
- `nodes_node_id_post` 同走 fleet 视图：命中集群观测节点时经 `node.proto` 的
  `OverrideStatus` RPC 直达该节点（e2b 同形——api 对节点的 orchestrator 打 gRPC
  `ServiceStatusOverride`，不 HTTP 代理、不写共享状态），SelfReport 形态保留本地路径。
  两处 e2b 同款语义一并保留：draining 只挡新放置不动存量；观测状态在该节点下一次
  心跳才刷新，POST 后立读可能仍见旧值。缺此一刀，直连 api 能列出节点却 drain 不了
  任何一个，P3 删掉 `node_list.go` 的代理后维护面就断了。
- `openapi.yml` 新增 `GET /registry/sandboxes` → `make agentenv-server` → 实现在
  `src/api/impls/admin.rs`，直读 paused registry（与 `grpc_service.rs` 同数据源）。
  保留 `registry_list.go` 已经论证过的两条语义：lease 的 NULL 必须渲染为 JSON null
  （NULL lease=已过期、NULL deadline=永不过期，压成 0 都是错的）；`executionID` 只读、
  不可作过滤参数。gateway 版对未声明查询参数的主动 400 **不迁**：e2b 的 api 全面上
  spec 校验（已声明参数坏值 400、缺必填 400），但没有任何 HTTP 端点拒绝未声明参数——
  api 版与 e2b 同策，声明集内从严、集外忽略，契约文本按此措辞。畸形 `nextToken`
  （非 sandbox id）按 e2b 键集分页形制显式 400（文案泛化），绝不静默答空终页。
- 本阶段 gateway 的拦截先不动（直连 api 地址即可验证新端点），拆除并入 P3。

### P2 — 客户端双地址与参考部署

- `crates/aenv/src/auth.rs` 的 `Credentials { url, api_key }` 增加 `proxy_url: Option<String>`：
  `url` = REST 入口，`proxy_url` = 数据面入口。文件格式里它仍是可选项（P3 前写下的凭据
  文件必须还能解析），但**缺省回落到 `url` 的规则随 P3 失效**：P3 之后没有任何进程同时
  服务两个面（api 半边不挂数据面路由，gateway 不转发 REST），回落只会把数据面流量送到
  一个答 404 的地址。现行规则：`data_plane_url()` 在 `proxy_url` 缺失或为空时报错并指向
  `aenv auth`；`aenv auth` 的代理地址提示不带默认值，留空即拒绝，且**即使与 `url` 相同也
  原样保存**（一个 Ingress 名可以合法地同时前置两个面）。`client/files.rs`（envd 数据面，
  含 `bypass_proxy_for_base_url`）与 `grpc/` 改用 `proxy_url`。
- `deploy/docker-compose.yml`：api 已直发 8010（:234），把它扶正为文档化 REST 入口，
  改写 :220-233 的「gateway 8000 也当 API 入口」注释；gateway 端口标注数据面专用。
  compose 路径要实测——它是上一轮迁移中唯一烂掉且单测全盲的部署面。
- `deploy/k8s/base`：`agentenv-api-service` 标注为客户端 REST 入口；`gateway-service`
  标注为数据面。`docs/src/deployment/kubernetes.md` 相应改写（含 :193 一节，其语义在 P3 后消失）。
- e2e 套件切到双地址（REST 打 api、数据面打 gateway），在 pve-mf 全量跑过（基线 109+8）
  之后才允许进入 P3。

### P3 — 拆除 gateway 控制面（两拍 + 一个收尾提交）

拍序（先开新路，再拆旧路——armed 集群上普通客户端在门禁退役前**无法**直连 REST，
所以门禁必须先走，客户端迁移窗口从第一拍开始）：

1. **第一拍：退役 api 侧门禁**（下方裁决），直连 REST 即刻可用；
2. **观测排空**：`agentenv_gateway_rest_upstream_total` 持续归零，残余走转发的调用方
   在旧路还活着时被找出来迁走，而不是删完之后靠 404 发现；
3. **第二拍：按下方删除清单拆转发。**

删除清单（Go 侧）：

- `rest_upstream.go` 全文件及 `server.go` 的请求分类转发分支（:291-351 一带）、
  `ServerOptions.RestUpstreamAddr`（:166-170,198,225）；
- `node_list.go`、`registry_list.go` 全文件及各自测试、`rest_upstream_test.go`、
  `server_test.go` 中依赖转发的用例；
- `services/shared/config`：`ParseRestUpstream`（config.go:120-163）、`RestUpstreamAddr`
  字段（:218）、Validate 的必填校验、`TestTheRestUpstreamIsAlwaysSetBecauseNodesNeverServeRest`、
  `manifest_test.go:194` `TestGatewayRestUpstreamIsDeclaredAndRequired`、
  :263 `TestAnEmptyEnvironmentValueCannotTurnTheApiUpstreamSwitchOff` 中 rest_upstream 的一半
  （**resume_addr 的一半原样保留**——`apiproxy.ResumeSandbox` 是 client-proxy 的核心行为）；
  **实施修订**：`resume_addr` 在 `8248c8f` 上早已删除（唤醒 RPC 走 `gateway.scheduler_addr`
  的连接），该用例整条只剩 rest_upstream 一半。红线保护的属性——「空环境值不得清掉承载
  resume 的地址」——因此改钉在 `GATEWAY_SCHEDULER_ADDR` 上并更名
  `TestAnEmptyEnvironmentValueCannotClearTheResumeAddress`，而不是删掉；
- 指标 `agentenv_gateway_rest_upstream_total`（metrics.go）整条删除——D2 的「折叠标签」被
  「删指标」取代；`recordGatewaySchedulerRPC` 的 ListObservedNodes/ListRegistrySandboxes
  两个 label 取值随调用点消失。两者都要通知看板。

行为定义与 CORS（采纳 e2b `shared/pkg/proxy/cors` 形制）：

- 删除后 gateway 对「无 Host 路由、无路由头」的请求答 404（同 node 侧 role_gate 风格），
  健康与指标监听不受影响。
- 新增 gateway 内小型 cors 帮助包，**只**作用于 gateway 自己合成的响应（404 兜底、resume
  错误、scheduler/fencing 拒绝）：`Access-Control-Allow-Origin: *`、无 `Vary: Origin`、
  无 credentials；preflight 只在无上游可答处应答（`IsPreflight` = OPTIONS +
  `Access-Control-Request-Method`；应答 204，带 `Allow-Methods: *`、`Allow-Headers`
  回显请求值、`Max-Age: 86400`——与 e2b `HandlePreflight` 逐头一致，缺 `Allow-Methods`
  的 preflight 会让浏览器拒发非安全清单方法的正式请求）。
  **活沙箱的响应一律不碰**——CORS 属于 envd 或用户自己的服务器（e2b 的注释原文如此）。

裁决：**api 侧控制面 REST 门禁随转发一并退役**（按上方拍序，它走在删转发之前）。

> **实施修订（2026-09-01，落地时）**：`ControlPlaneGate` 在 `8248c8f` 上是**两个半边共用**的
> 一层，不是 api 专有——`assemble` 无条件挂它，node 半边的 `/nodes`、`/nodes/{id}`（含 preStop
> 排空）正是靠它校验 `node-gate-token`。整体删掉会连带把下面「node 侧的 gate token 不动」
> 一并作废。因此实际落地为：`assemble` **只在不拥有用户 REST 的那半边挂门**
> （与 `role_gate` 同一根轴），api 半边不再挂任何凭据层；`[api].control_plane_tokens` /
> `control_plane_token_file` 两个配置项**保留**（node 半边读它们）；
> `deploy/k8s/base/agentenv-api-deployment.yaml` 撤下 `api-gate-token` 投影与
> `AENV_API_CONTROL_PLANE_TOKEN_FILE`。武装机制确认为配置驱动（空凭据集 ⇒ Disabled ⇒ 放行），
> 因此回退 api 镜像时旧门在场但未武装，成立。

- 对象是 `src/api/server.rs:141` `assemble` 里加在生成路由上的 `require_control_plane`
  （`ControlPlaneGate`，头 `x-agentenv-control-plane`）。它的前提是「用户 REST 只经 gateway
  到达，gateway 替客户端盖这个内部头」；P3 删掉转发之后客户端直连 REST，这个前提不存在了，
  门在 armed 集群上只会把合法客户端挡成 403（本轮 pve-mf 直连验收即撞到）。
  参照 e2b：`packages/api` 对 REST 不设内部来源门，安全靠网络边界与真实鉴权（§6 的条件项）。
- **node 侧的 gate token 不动**：node 进程的 `role_gate` 与其凭据是另一件事，
  它挡的不是用户 REST。
- 连带删除：e2e 的 `AENV_CONTROL_PLANE_TOKEN` 注入（`lib/helpers.sh` 的
  `_e2e_control_plane_args` 及各调用点、`run_dev_cluster.sh` 的 preflight、
  `runtime.sh` 就绪轮询里的同款注入）。
- e2e 同批：`E2E_SPLIT_ADDRESSES` 默认翻为 1——转发删除后非 split 路径不复存在，
  开关与 `test-e2e-*-split` 目标随后收敛掉。
- 本轮（P1）**不动**任何门禁代码，只落这条裁决。

凭据轴迁移注记（消费方要改头，写进变更说明）：

- api 侧 `/nodes` 与 `/registry/sandboxes` 统一 `AdminApiKeyAuth` = **`X-Admin-Token`**
  （`openapi.yml` securitySchemes；与 e2b 的 admin 面同型）。
- 网关版两个端点的凭据要求与之不同，且**两者之间也不一致**：`registry_list.go:123` 要求
  非空 `X-API-Key`（否则 401），而 `node_list.go` 对 `/nodes` **不查任何凭据**。
  这两种行为都随 P3 删拦截一起消亡。
- 因此：从网关地址迁到 REST 地址的消费方（运维脚本、看板、e2e 07 套件）必须换头——
  `X-API-Key` → `X-Admin-Token`；原先裸调 `/nodes` 不带凭据的调用方则要**开始**带头。

部署与文档（同批）：

- **回滚窗口**：`deploy/k8s/base/config/gateway.json` 的 `rest_upstream_addr` 键与
  compose（:272）/k8s 的 `GATEWAY_REST_UPSTREAM_ADDR` 本批**保留不删**——旧 gateway
  把 rest_upstream 当必填校验（`TestTheRestUpstreamIsAlwaysSet…`），键在则 gateway 的
  纯镜像 digest 回退仍然成立（新二进制忽略不认识的键）；删除挪到收尾提交，并在
  收尾时录入 `docs/src/configuration/env-vars.md` 的 removed 清单（今日效果：被忽略）。
  **实施修订**：`api_manifest_test.go` 的 `TestTheGatewayCanBeFlippedToTheApiHalfWithoutEditingAManifest`
  断言的正是「gateway Deployment 声明该键且非空」——它就是回滚窗口的 manifest 侧守卫，
  因此**保留并改写理由**（更名 `TestTheGatewayKeepsTheRestUpstreamKeyForTheRollbackWindow`），
  由收尾提交连同键一起删除。§8 那条「断言任何工作负载不再声明 `GATEWAY_REST_UPSTREAM_ADDR`」
  的新守卫与回滚窗口直接冲突，**归入收尾提交**，本批不加。
  api 半边同批把门禁的武装配置（control-plane token 注入 Deployment 的那份）撤下：
  回退 api 镜像时旧门在场但未武装，已迁走的直连客户端不会集体 403（实施时验证
  武装机制确为配置驱动）。P3 的完整回滚因此是「镜像 + 本批未删的配置」成对回退。
- `services/README.md`、`CLAUDE.md` gateway 段、`docs/src/deployment/kubernetes.md` 改写；
- `2026-08-31-residue-decisions.md` D2 加一行「被本方案取代」。

收尾提交（gateway 不再调用、回滚窗口关闭后单独一个提交）：`scheduler.proto` 删
`ListObservedNodes`/`ListRegistrySandboxes` 两个 RPC，连同 `grpc_service.rs` 的实现、
`reporter.rs` 的测试桩、两侧生成码（`make -C services`、`build.rs`）；
`rest_upstream_addr` 键与 `GATEWAY_REST_UPSTREAM_ADDR` 至此才删。

### P4 — 连边折叠：gateway 掉线 scheduler.v1（前置：P3 上线稳定）

终点：gateway 连边 = Redis 直读 + apiproxy 一条 RPC，与 e2b client-proxy 同形（设计图 A1/A2）。

- **api 侧**：`resume_for_data_plane`（`src/api/impls/resume_surface.rs`）内化完整冷路径
  判定——先查 running（binding/心跳清册，今天 LookupNode 走的三步），再按 autoResume 门控
  决定 wake 或拒绝。响应继续用现有 `SandboxResumeResponse`（`node_address` + `execution_id`
  已是 e2b 形状）。**两趟读、门控不同的语义必须原样保住**：autoResume:false + running 可
  路由，autoResume:false + paused 拒绝——这正是当年 autoResume 静默失效修复的形状，
  折叠进一个 RPC 不许把它折没（e2b 此处更弱：政策非 Any 时 miss 连 running 都答
  NotFound，我们不跟，这是有据的既证偏离，§4 第 10 条）。wake 成功与 running-miss 修复时
  api 都自写投影（它拥有 Redis 写权），取代 gateway 的 RecordAssignment——e2b 的
  edge 不回写、每次 miss 重发 RPC，回写是我们对命中率的既定改良（§4 第 11 条）。
- **api 不可达时 gateway 答 502**：e2b 把 api 的 NotFound 与传输失败都渲染成 502
  （`packages/shared/pkg/proxy/template/sandbox_not_found.go:31`）；我们 `writeResumeError`
  的 default 分支已经答 502（`server.go:1478-1484`），P4 只需把 VerdictUndecided 收进这一
  分支，不另设状态码。
- **投影 TTL**：「覆盖沙箱剩余寿命」就是现有的 `src/orchestrator/store/metadata.rs:319-333`
  `projection_ttl_secs`——`max_sandbox_lifetime_secs` 上限的剩余量 + grace（SetTimeout 永远
  越不过这个上限），与 e2b `packages/api/internal/orchestrator/lifecycle.go:36-40`
  `lifetime = MaxLengthInHours` 同形；api 半边的 `resolve_projection_ttl`
  （`src/node_registry/grpc_service.rs:254`）也已经存在。P4 的 api 侧在 wake 与 running-miss
  写投影时调用它，不新实现任何东西。两条注记：
  (i) 上限为 0 时 TTL 回落到 `binding_ttl`（30 s，`src/cfg.rs:949`），P4 后每次 miss 花一次
  resume RPC——只影响命中率，正确性不受影响；e2b 的「不足一小时的最大寿命 → TTL 0 =
  永不过期」（`packages/shared/pkg/sandbox-catalog/catalog_redis.go:98` 的 `SET` 带 0 过期）
  **不得照抄**。(ii) e2b 在 store 插入回调里**同步**写 catalog
  （`packages/api/internal/sandbox/store.go:43-44`，"should be called sync to prevent race
  conditions"）——投影写入必须在 resume 响应返回之前完成。
- **transitioning 一腿**：e2b 的 api 对 Pausing/Snapshotting 最多等 60 s / 3 次重试
  （`packages/api/internal/orchestrator/autoresume.go:43-97`，预算 `proxy_grpc.go:97`）才答
  FailedPrecondition；本分支在边缘立即答 503 + `Retry-After: 1`（`server.go:1468-1475`）。
  裁决：保持立即 503 + Retry-After，api 不引入等待预算。
- **gateway 侧删除**：`lookupNodeColdPath`（`server.go:491`）与 VerdictUndecided 的第三级回落——
  Undecided 收窄为纯传输失败 → 请求失败（e2b 形状：api 不可答则数据面冷路径失败；
  PG 降级窗口内 running 沙箱的冷路由损失是接受的代价，冷路径命中率列为监控项）；
  `recordAssignmentFromResponse` 两腿（response_header /
  response_body；**实施时先验证 response_body 腿在 HEAD 是否已不可达**——fork REST 在
  回落分支删除后不再经 gateway 到 node）；`projectionTTLToRecord` 的 gateway 半边；
  对 scheduler.v1 的拨号与 `gateway.scheduler_addr` 配置、`GATEWAY_COLD_LOOKUP_TIMEOUT`。
- **fencing 改喂源**：`decideFencing` 只再消费 projection Synthesize 与 resume 响应两个来源
  （它消费的本就是响应类型而非 RPC，此处是删一个来源，不是重设计）。
- **指标**：`recordGatewaySchedulerRPC` 全系、`gatewayColdLookupTimeout`、
  route_resolution 的 scheduler 来源值消失；变更说明列全。
- **验收（对照 e2b 后修订）**：e2b 的 catalog 没有反熵重发布——一次写入、TTL 覆盖
  沙箱最大寿命，miss 的正确性来自 api 侧按请求修复（`autoresume.go` 的 StateRunning
  分支）。折叠后我们同形，投影缺失/陈旧不再是正确性问题，只是命中率。硬验收三条：
  (a) **投影丢失演练**——人为删除一个 running 沙箱的投影，请求必须经
  `resume_for_data_plane` 照常路由，且 api 回写投影恢复命中；(b) autoResume 双门控
  断言原样保住（上方 api 侧）；(c) 冷路径命中率与「api 不可达时 miss 即失败」的
  影响面进看板。reconciler 据此定性为**命中率优化 + 孤儿方向清理**（e2b 的
  `Store.Reconcile` 只做杀孤儿），不是折叠的正确性前提。**已裁决（2026-09-01）：
  降格为并行项**——P4 的前置只剩 P3 稳定，正确性由 (a) 的演练直接证明。
- **收尾提交**：`scheduler.proto` 删 `LookupNode`、`RecordAssignment`，连同
  `grpc_service.rs` 实现与生成码。scheduler.v1 至此只剩 node↔api 面。

> **实施修订（2026-09-01，落地时；四个提交，api 先行可独立滚集群）**
>
> - **api 侧**（`feat(api)`）：`resume_for_data_plane` 在 `locate_for_resume` 之后、
>   envd 鉴权与 autoResume 门控之前新增 running 短路：三步查找答 BOUND 且
>   `execution_authority = REGISTRY`（`ResumePlacement::Running`）即直接答
>   `Woken{node, execution_id}`——与 projection 命中给 gateway 的答案同形，token 由
>   envd 在节点上校验。PLACED/PINNED 不算 running；REGISTRY 但 id 不可解析退化为
>   Preferred。两道 autoResume 门控原样保留（false+paused → AutoResumeDisabled，
>   claim 前后各一道），变异证据：删短路 → 两个 running 用例红；删 paused 门 → 拒绝
>   用例红。投影由 api 同步写入（响应返回前）：wake 成功时用
>   `metadata.projection_ttl_secs` 经 `resolve_projection_ttl`（与 RecordAssignment
>   同一门与上限）；running-miss（答案来自心跳清册或 registry 而非 binding store）
>   时同样写入，用 api 半边元数据记录的预算；binding store 答的短路**不**回写。
>   写失败只 warn，不影响 wake（变异证据：删 wake 回写 → 两个用例红）。
>   `NodeRegistryGrpcService` 新增进程内入口 `lookup_sandbox`（带来源标签）与
>   `record_running`（source 标签 `resume`），gRPC 的 `LookupNode`/`RecordAssignment`
>   改为其薄包装。**偏离 P4 文本一处**：api 半边的远端后端在 resume 启动沙箱时本就
>   经 `announce_placement` 写一次 TTL=0 的投影，本批未动它，wake 路径因此写两次
>   （第二次带真实预算，同 incarnation 属 refresh）。transitioning 维持
>   FailedPrecondition，无等待预算；线上响应形状不变。
> - **gateway 侧**（`refactor(gateway)`）：删 `lookupNodeColdPath`、`writeSchedulerError`、
>   `recordAssignmentFromResponse` 两腿、`recordAssignment`、`assignmentRoute*`、
>   `projectionTTLToRecord*`、`ServerOptions.ProjectionAuthoritative`、
>   `SchedulerClient` 字段与 `NewServer` 参数、`cmd/main.go` 的 scheduler 客户端
>   （到 `gateway.scheduler_addr` 的连接保留承载 resume RPC）、`ColdLookupTimeout`
>   与 `services/shared/config` 的 `cold_lookup_timeout` 键/环境变量绑定。
>   VerdictUndecided 收进 `writeResumeError` 的 default 分支 → 502（变异证据：把
>   Undecided 映成 503 → 三个用例红）。fencing 保留 `LookupNodeResponse` 载体类型，
>   喂源只剩 projection Synthesize 与 resume `LookupResponse`；LookupNode 喂源的
>   fencing 用例改由同 incarnation 的 projection 记录喂入，PLACED/PINNED 与
>   UNSPECIFIED authority 两条用例因无喂源能产生而删除。**response_body 腿可达性
>   查证**：HEAD 上 `recordAssignmentFromResponse` 只在 PLACED/PINNED 时运行，而
>   只有冷路径 LookupNode 能答 PLACED/PINNED；节点的数据面 `/proxy` 响应从不盖
>   `x-agentenv-sandbox-id`，所以 header 腿在数据面上本就不可达，真正跑的是 body
>   腿——它把沙箱自己的 2xx 响应体当 JSON 找 `sandboxID`，找不到就什么也不写。冷
>   路径删除后两腿皆无喂源，整函数删除。`projectionAuthoritative` 在 gateway 内
>   唯一消费者是写侧 TTL 门控，读侧从未由它门控（读由 `projection_read` 门控），
>   故 gateway 内整个字段删除；配置键随回滚窗口保留。
> - **消失的指标**：`agentenv_gateway_scheduler_rpc_duration_seconds` 整条（rpc
>   标签值 LookupNode/RecordAssignment）、`agentenv_gateway_cold_lookup_timeout_total`
>   整条、`agentenv_gateway_route_resolution_total{source}` 的 `scheduler` 与
>   `resume_undecided` 两个值、`agentenv_gateway_sandbox_location_total{location}`
>   的 `placed`/`pinned`/`unspecified` 三个值（序列保留，只剩 `bound`）。
> - **回滚窗口**（`test(config)`）：`GATEWAY_COLD_LOOKUP_TIMEOUT` 与
>   `cold_lookup_timeout` 保留在 `deploy/k8s/base`（新二进制忽略，
>   `rollback_window_test.go` 与 manifest 守卫各加一项，删 manifest 键 → 守卫红），
>   `env-vars.md` 记为 P4 起忽略、收尾提交删除。`gateway.forward_response_size` 与
>   `gateway.routing.projection_authoritative` 同为「解析但无人读」，归收尾提交。
> - **未动**：`deploy/k8s/base` 与 compose 的 manifest 及其注释（回滚窗口）；
>   保留代码上仍指向 `writeSchedulerError`/回落的 🔴 注释四处（`writeResumeError`、
>   `executionSupersededStatusCode`、`resolveFromProjection`、resume client 的
>   Unavailable 分支）按「不改他处 🔴 注释」规则原样留下，收尾提交一并清。

### P5（可选，默认不做）— 改名 gateway → client-proxy

`agentenv_gateway_*` 全系指标名和 k8s Service 名都是序列/寻址身份，改名的代价是
全部看板与部署引用迁移。职责切干净之后名字的错位是纯外观问题，留给单独裁决。

## 6. 非目标

- **aenv-api 内置鉴权**（admin token 等值校验、API key、e2b 的 teams/`TeamApiKey` 形制）：
  当前部署单租户、内网，访问控制在网络边界——`auth.rs` 的成文立场，继续有效；
  本方案的分流不改变暴露面（gateway 对 REST 本就零鉴权）。**条件触发**：任何部署要把
  REST 入口暴露出受信网络之外时，先按 e2b 形制补鉴权（admin token 等值比较是最便宜的
  第一刀），再暴露。
- **活沙箱响应的 CORS**：不做——e2b 同样不做，那是 envd 或用户自己服务器的职责；
  P3 采纳的 cors 包只覆盖 gateway 合成的响应。
- apiproxy 的 OIDC edge 鉴权（§4）；
- placement 打分对齐（已有独立方案 `2026-08-30-e2b-alignment-placement-scoring.md`）；
- 多集群 edge 面（e2b `openapi-edge`/servicediscovery）、docker-reverse-proxy 同位物、
  泛域名 TLS 自动化。

## 7. 与在途事项的关系

- **取代 D2**（`agentenv_gateway_rest_upstream_total`）：从折叠标签升级为删除整条指标。
- **reconciler**（`2026-09-01-bidirectional-reconciler.md`）：**并行项，不卡 P4**
  （2026-09-01 裁决；此前一度升格为前置）。RecordAssignment 的修复职责由
  `resume_for_data_plane` 的按请求修复接收（e2b StateRunning 同形），reconciler
  收窄为命中率优化 + 杀孤儿，按它自己的方案推进。
- **discard 竞态**：不同子系统；P3/P4 与它不要同一批滚集群，避免归因混叠。
- **scheduler 面**：P3 净减两个 RPC，P4 再减两个（**待收尾提交**：gateway 已于
  2026-09-01 停止调用 `LookupNode`/`RecordAssignment`，proto、`grpc_service.rs`
  实现与生成码仍在，随回滚窗口关闭后的收尾提交删除）；此后仅剩 node↔api 的
  Heartbeat/ReportSandboxEvent/ListSandboxes/p2p hints 等。

## 8. 验收

- 每阶段通用：`make fmt clippy test-unit`、`make -C services test`（改到 Go 守卫扫描的文件后
  `-count=1` 重跑）、两套 contract suite 的 Redis 侧、新增守卫一律附变异证据。
- P1：api 的 `/nodes` 与 `/registry/sandboxes` 响应对 gateway 版本逐字段等价
  （lease NULL → null 显式断言）；`/nodes` 另需数据源断言——集群有 N 个节点时 api 直连答
  N 行而非 1 行自身。split 模式**套件 11 恢复 117/8/0**。
- P2/P3：pve-mf 全量 e2e（109+8 基线）双地址通过；数据面五项冒烟——Host 路由、路由头路由、
  暂停自动唤醒（resume curl 注意 Content-Type，历史 415 坑）、fencing 拒旧 incarnation、
  RecordAssignment 后 projection 收敛。
- P3 后：404 兜底行为 + 合成响应携带 CORS 头（preflight 在 gateway 合成失败的每一处应答，
  且只在无上游处应答）；
  gateway 访问日志观察一个发布周期，确认无残余 REST 流量。
- P3 后守卫：新增 manifest 测试断言任何工作负载不再声明 `GATEWAY_REST_UPSTREAM_ADDR`
  （沿 `snapshot_catalog_manifest_test.go` 形制；锚定语法而非裸子串）。
- P4：autoResume 双门控行为保持（false+running 可路由、false+paused 拒绝，两个用例
  显式断言）；wake 后投影由 api 写入并收敛；冷路径命中率与失败率上监控；
  fencing 在 projection/resume 两个喂源下拒旧 incarnation 的用例保持全绿；api 不可达答 502、
  transitioning 仍答 503 + `Retry-After: 1` 各一个显式用例；投影写入在 resume 响应返回前完成。

## 9. 风险

| 风险 | 缓解 |
|---|---|
| `/nodes` 形状漂移破坏消费方 | P1 先 diff 后动，openapi 为 canonical，e2e 断言同批改 |
| 客户端双地址切换窗口 | 窗口已随 P3 关闭：`proxy_url` 不再回落 `url`，只配一个地址的客户端拒绝运行并指向 `aenv auth`；P3 前 e2e 已在双地址上通过 |
| compose 面再次静默烂掉 | P2 实测 compose 全流程（历史事故单测全盲） |
| 指标/label 消失打断看板 | P3/P4 变更说明各列全部消失序列 |
| P3 后残余流量打到 gateway 的 REST 路径 | 定义 404 行为并在 gateway 访问日志观察一个发布周期 |
| P4 折没 autoResume 双门控（历史上删掉那趟读时 1397 个测试全绿） | 两个门控用例显式断言 + 变异证据；不许以「测试全绿」为删除依据 |
| P4 后 PG 降级窗口 running 沙箱冷路由失败 | e2b 同形的接受代价；投影 TTL = `projection_ttl_secs`（剩余寿命 + grace，e2b 同形）+ 冷路径命中率监控，恶化再议 |
