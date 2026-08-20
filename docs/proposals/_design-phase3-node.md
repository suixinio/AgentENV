# 阶段 3 · node 侧（Rust）设计：A1 ExecutionID 一等公民 / A4 node API 收窄 / A5 接收端 / A6 的 node 侧

> 2026-08-19 · **只出设计，不改源码**。任务书：[`_impl-plan-control-plane-phase3.md`](_impl-plan-control-plane-phase3.md) §3 批次 A。
> 权威方案：[`2026-08-19-agentenv-control-plane-refactor.md`](2026-08-19-agentenv-control-plane-refactor.md) §4 阶段 3 / 阶段 4、§8。
> 本文覆盖 **A1**（§2）、**A4**（§3）、**A5 的接收端**（§3.7，裁决 O8 追加）、**A6 的 node 侧**（§3.8，2026-08-19 追加）
> 与 **node 侧指标规范**（§3.9，2026-08-19 追加），范围是 `src/`（Rust node）+ `services/gateway`（注入端）+ `deploy/k8s/base/`。
> A2（登记表 schema）/ A3（SQL 事务内 fencing）/ A5 的**发端与路由决策**不在本文范围，
> 但本文为 A3 冻结的接口形状写在 §2.9。
> 🔴 **A6 的归属**：`src/api/openapi.yml` 的改动 + codegen + 响应体字段**全部在本文**；
> gateway 只负责它自己拼的三个 DTO（`_design-phase3-gateway.md` §9）。

---

## 0. 一句话

**A1**：把「本节点这一次 VM 运行」升成一个跨进程可比对的 `ExecutionId`，由 `LaunchPlan` 的两个变体**各自在构造函数内铸造**、
经 `SandboxBackendFactory` 的三个必填参数进 backend、经 `SandboxMetadata` 一个必填字段进持久化与登记表；
`SandboxInstanceId` 降级成它的派生物（wire 字段名与类型不变，**不动 `custom_extension_api/openapi.yml`、不动它的 generated**；
🔴 **A6 另说 —— 它要动 `src/api/openapi.yml` 并重新 codegen**，见 §3.8.1）。

**A4**：node 的用户级 REST **只接受带控制面凭证的调用**，凭证由 gateway 在它唯一的出向 `Rewrite` 钩子里注入；
实现层次是**路由层中间件挂在 generated router 上、`.merge(proxy)` 之前** —— 数据面因此在**装配顺序**上就不可能被覆盖，
而不是靠中间件里再判一次路径。

---

## 1. 输入事实（本轮已实证的锚点，直接引用不再复查）

| # | 事实 | 证据 |
|---|---|---|
| F1 | `SandboxInstanceId` = `Uuid::now_v7()`，**模块私有**、零持久化，仅在 `[custom_extension].url` 配置时生成 | `src/sandbox/custom_extension/client.rs:53-60`；`config/default.toml:130-136` 该项注释掉 |
| F2 | 它的换代语义已经对：start-fresh / start-resume 各铸一次 | `client.rs:271-272`、`client.rs:300-301` |
| F3 | snapshot / fork **不动**父沙箱的 guard（FC 原地 pause+resume） | `src/sandbox/firecracker/sandbox.rs:330-361`（`snapshot()`）、`:363-416`（`fork()`） |
| F4 | 🔴「从模板/快照创建新沙箱」走的是 `LaunchMode::Resume` | `src/sandbox/firecracker/sandbox.rs:551-561`（`from_snapshot` → `LaunchMode::Resume`） |
| F5 | 🟢 `LaunchPlan` 已经是 `Create` / `Resume` 两变体的枚举，且**只有三个构造函数** | `src/orchestrator/launch_plan.rs:36-90` |
| F6 | `launch_sandbox` 是两条路径**唯一**的汇流点 | `src/orchestrator/service.rs:2216`；调用点 `:482`（create-from-snapshot）`:542`（create-fresh）`:1606`（resume） |
| F7 | 仓内同构 fencing 先例：`NodeIdentity.service_instance_id`（UUID v7），scheduler 已校验并有测试钉住 | `src/identity.rs:19,41-45`；`services/scheduler/internal/service.go:345`；`service_test.go:644` |
| F8 | 🔴 resume 的登记表**首次写**（`claim_for_resume`）发生在 API 层、在 orchestrator 之前 | `src/api/impls/sandbox.rs:1254 discard_if_superseded`、`:1261 arbitrate_resume` |
| F9 | 🔴 数据面 auto-resume **跳过** F8 那两道闸，直接调 `orchestrator.resume_sandbox` | `src/api/proxy.rs:686` → `:800 try_auto_resume` |
| F10 | 但两条 resume 最终**都**进 `resume_sandbox_inner`，`mark_running` 在 `service.rs:1627-1634` 统一发出 | 同上 |
| F11 | `PauseOutcome` 携带整份 `SandboxMetadata`，`begin_pause` 把它序列化进 `metadata_json` | `src/orchestrator/types.rs:99-102`；`src/orchestrator/paused_registry/central.rs:476-497` |
| F12 | 🟢 `SnapshotPublishMetadata` 是逐字段构造的**独立**结构，不是 `SandboxMetadata` 的序列化 | `src/snapshot/types/snapshot.rs:22-35`；`src/api/impls/paused_coordinator.rs:685-686` |
| F13 | 控制面与数据面**共用一个 listener**，数据面是 `.fallback()` 兜底 | `src/api/server.rs:24-41`；`src/bin/server.rs:83`（`API_ADDR` 默认 `0.0.0.0:8000`）；`src/api/proxy.rs:139-148` |
| F14 | node REST **无 NodePort / hostPort**（headless ClusterIP），30800 是集群里手工 apply 的 out-of-band 资源 | `deploy/k8s/base/agentenv-daemonset.yaml:109-113`；`deploy/k8s/base/agentenv-headless-service.yaml`；`grep -rn nodePort deploy/` 零命中 |
| F15 | 现状鉴权是 presence-only，注释自认 | `src/api/impls/auth.rs:15-55`；`docs/src/deployment/kubernetes.md:174-199` |
| F16 | gateway 默认透传，只截 4 类路径自处理；控制面路径**不改写** | `services/gateway/internal/server.go:177-195`、`:674-687 upstreamTargetPath` |
| F17 | gateway 出向请求只有**一个** header 注入点 | `services/gateway/internal/server.go:368-380`（`ReverseProxy.Rewrite`）、`:747-757 injectForwardedHeaders` |
| F18 | scheduler **无下行通道**：五个 RPC 全是 node 当 client；`services/scheduler` 里 `net/http` 只出现在自身健康端口 | `services/api/proto/scheduler.proto:384-401`；`services/scheduler/cmd/main.go:10` |
| F19 | 🔴 `axum::serve(listener, app)` **未**用 `into_make_service_with_connect_info` ⇒ `ConnectInfo<SocketAddr>` 拿不到 | `src/bin/server.rs:252` |
| F20 | 🔴 preStop 钩子自己就是 node REST 的调用方：`POST /nodes/{id}` + `GET /sandboxes`，打 `http://localhost:8000` | `deploy/k8s/base/agentenv-daemonset.yaml:126-159` |
| F21 | `/health` 是 kubelet 三种探针的目标，且在 **generated router 内** | `agentenv-daemonset.yaml:177-193`；`src/api/generated/src/server/mod.rs:45` |
| F22 | Rust 配置支持 `#[config(env = "AENV_…")]` 逐字段环境变量覆盖；Go 侧 `services/shared/config` 也有 env 覆盖层 | `src/cfg.rs:78-140`；`services/shared/config/config.go:519-622` |

---

## 2. A1：ExecutionID 一等公民

### 2.1 类型定义

**放哪**：`src/types/id.rs`，与 `SandboxId` 并列，`src/types/mod.rs` 加一行 `pub use id::ExecutionId;`。

理由：`SandboxId` 就在那儿（`src/types/id.rs:6-29`），而 execution 与 sandbox 是同一层的身份概念，
它要同时被 `orchestrator` / `sandbox` / `api` / `paused_registry` 四个模块用；放进任何一个模块都会造成另外三个反向依赖。

```rust
/// 一次沙箱运行的身份。沙箱 id 命名的是「用户的这台机器」，
/// execution 命名的是「这台机器的这一次开机」。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionId(Uuid);
```

| 决策点 | 选择 | 对齐 / 理由 |
|---|---|---|
| 表示 | `Uuid`，`Uuid::now_v7()` | 与 `SandboxId::new()`（`id.rs:10-12`）、`NodeIdentity.service_instance_id`（`identity.rs:41-45`）、`SandboxInstanceId::new()`（`client.rs:56-58`）**三处**现有形状完全一致；v7 自带时间序，日志里两个 execution 谁先谁后可读 |
| 构造 | 只有 `new()`（铸造）/ `from_uuid()` / `parse_str()`（反序列化用） | 没有 `Default`。🔴 **刻意不实现 `Default`** —— `SandboxId` 有 `Default`（`id.rs:31-35`），而它被 `SandboxMetadata::default()` 用来造测试对象；execution 若也有 `Default` 就会出现「悄悄铸出一个谁也没授权的化身」，正是必填想禁止的事 |
| `Display` | 透传内层 `Uuid`（`self.0.fmt(f)`） | 与 `SandboxId`（`id.rs:59-62`）、`SandboxInstanceId`（`client.rs:62-66`）一致 |
| 序列化 | `Serialize`/`Deserialize` 走 `Uuid` 默认（JSON 上是带连字符的小写字符串） | proto / JSON 上都是 `string`。与 `service_instance_id` 在 proto 上是 `string` 一致（`scheduler.proto`），A3 的列类型建议 `uuid`，与 `generation`（`bigint`）分属两轴 |
| `Copy` | 是 | `SandboxId` 是 `Copy`，调用点全是按值传；execution 若不是 `Copy`，`launch_sandbox` 里那一串 `plan.sandbox_id()` 风格的读取全要改成借用 |

🟢 **与 F7 的对齐点逐条**：同为 UUID v7 / proto 与 JSON 上同为 string / 校验位置在**接收侧**（scheduler 校验 node 报的 instance，A3 校验 node 报的 execution）/ 错误语义同为「拒绝，不是静默改判」（`service.go:345-347` 那条走 ~~`FailedPrecondition`~~ **`codes.InvalidArgument`** —— 🔴 **2026-08-19 按源码订正：原文写错了**。`service.go:345` 只是 `strings.TrimSpace`，真正的校验与拒绝在 `:346-347`，返回的是 `InvalidArgument`。**这不影响 O5 的裁决**（A3 定的是 `PermissionDenied`，语义是「你不是有资格做这件事的那个实体」，与 `service_instance_id` 那条「参数不合法」本来就不是同一类），但**别照着这句去"对齐先例的错误码"**；A3 的三种破坏性操作也应落在一个可分辨的 gRPC code 上，建议沿用 D8 已备好的 `Code::Aborted ⇒ GenerationConflict` 旁边新增一个 execution 专用 code —— 见 §5 未决项 O5）。

> ✅ **裁决（2026-08-19 主 agent）**：O5 按「新开一个」定稿 —— `ErrExecutionFenced → codes.PermissionDenied → Rust `PausedRegistryError::ExecutionFenced`，语义**永不重试**，与既有 `GenerationConflict → codes.Aborted`（重读再试）**严格分开**。
> 理由：合并后节点会重读、拿到新化身的 generation 再发一次，**正好绕过 fencing**。详见 §5 O5 与 `_design-phase3-scheduler.md` §3.5。

### 2.2 铸造点与传播路径

#### 2.2.1 三个铸造点，一个也不能多

| 铸造点 | 函数 | 语义 |
|---|---|---|
| **B-1** | `LaunchPlan::for_create_from_snapshot` / `for_create_fresh`（`launch_plan.rs:42-74`）| 一次 Create = 一次新化身 |
| **B-2** | `LaunchPlan::for_resume`（`launch_plan.rs:76-90`）| 一次 Resume = 一次新化身。✅ **裁决后铸造点前移到 claim**，`for_resume` 由「铸造者」改成「消费者」，见下方裁决块 |
| **B-3** | `fork_sandbox_inner` 里造 `children_spec` 的那个 `map`（`service.rs:624-631`）| 每个 fork **子沙箱**是全新沙箱，各自一次新化身 |

> ### ✅ 裁决 D-1（2026-08-19 主 agent）：`resuming` 行走「预分配」E-A，B-2 的铸造点前移到 claim
>
> 对应 `_design-phase3-scheduler.md` §6-U1 与本文 §5-O1。**claim 时分配 execution，与 `claimed_by_node_id` 同一个 SQL 事务写入。**
> 理由三条：① 让 `mark_running` 成为真正的"校验 execution"而不只是"校验 claim 持有者"；
> ② 恒空（E-B）要求 B3 记得给 `resuming` 开孤儿判定特例，而"注释承诺别处会做、结果没做"正是本仓踩过的坑；
> ③ 它使 gateway 设计的 S5 可满足 —— resume 窗口内可**正常设防**，不必退化成"窗口内不设防"。
>
> 🔴 **对本设计的硬约束：铸造点前移，但「拿到 `LaunchPlan` ⟺ 已铸恰好一个新 execution」这条编译期保证一分都不许丢。**
> 做法（**这是裁决的一部分，不是建议**）：
>
> 1. 在 **claim 路径**铸造，并产出一个**只有 claim 能构造**的 token 类型，例如
>    ```rust
>    /// 一次成功的 resume 认领。内含本次认领预分配的化身。
>    /// 🔴 字段私有 + 无公开构造函数 + 不实现 Default/Clone ⇒ 只有 claim 路径能造出它，
>    ///    且一个 token 只能兑换一次启动。
>    pub struct ClaimedExecution(ExecutionId);
>    ```
>    构造函数只对 claim 所在模块可见（`pub(crate)` 或 `pub(super)` + 私有字段），
>    读取只经 `fn execution_id(&self) -> ExecutionId`。
> 2. `LaunchPlan::for_resume(…, claimed: ClaimedExecution)` **按值消费**它，把其中的 `ExecutionId`
>    装进 `ResumeLaunchPlan` 的私有字段。`for_resume` 自己**不再调** `ExecutionId::new()`。
> 3. ⇒ §2.2.2(a) 的三条论证原样成立，只是第 2 条换了措辞：调用方依然**没有办法**递一个旧化身进来 ——
>    它连一个能被 `for_resume` 接受的值都造不出来，除非走 claim。
>
> 🔴 **推论（必须写进实现，否则 E-A 有绕过通道）**：`for_resume` 只接受 token ⇒ **任何 resume 路径都必须先经过 claim 决策点**，
> 包括 F9 那条数据面 auto-resume（今天它跳过 `discard_if_superseded` / `arbitrate_resume`）。
> 这一条与 A3 在 `mark_running` SQL 谓词里的校验是**两道独立的闸**，不是二选一（见 §5-O1 的裁决）。
>
> 🟡 **落地注意（本轮实现必须回答，不许留白）**：`disabled.rs`（未配置中央登记表）与"本机自有 parked 行就地唤醒"这两条路径上没有集群 claim。
> 它们的 token **由同一个本地决策点铸造**（本地仲裁通过之后），
> 🔴 **绝不允许给 `for_resume` 开第二个"不需要 token"的构造函数** —— 那等于给预分配开一条静默旁路。
>
> 🔴 **顺带被这条裁决固定下来的三件事**：
> - claim 的 RPC（`AcquireSandbox` / `claim_for_resume`）**必须携带** execution（scheduler 侧 `AcquireSandboxRequest` 加 `string execution_id`）；
> - `mark_running` 送上去的**必须是 claim 时那一个**，不是启动时现铸的（变异验证见 §4.1 T-A1-6）；
> - `release_claim`（交还未 resume 的 claim）**必须清空**行上的 execution —— 与另外两条夺权路径同规格（§2.9、`_design-phase3-scheduler.md` §3.4）。

**不铸造的地方**（本设计的负空间，同样是验收对象）：
`capture_snapshot`（`service.rs:1655+`）、`SandboxBackend::snapshot()`（`sandbox.rs:330`）、
`SandboxBackend::fork()` 对**父**沙箱的处理（`sandbox.rs:363-382`：pause + resume 原地回来）、
`pause_sandbox_inner`、模板 build / rebuild、`update_custom_extension_params`。

#### 2.2.2 「由类型系统保证」到底能保证到哪一步 —— 逐条论证

任务书要求「不靠调用点自觉」。诚实的答案是：**三条里两条能靠编译器，一条只能靠结构 + 测试**。

**(a) 🟢 Create/Resume 各换一次 —— 编译期强制，方向成立。**

`LaunchPlan` 是 `pub(super)` 枚举，两个变体各裹一个 `Box<…LaunchPlan>` 结构体（`launch_plan.rs:36-39`）。
把 `execution_id` 加成这两个结构体的**私有字段**（去掉 `pub`）：

```rust
pub(super) struct CreateLaunchPlan {
    pub sandbox_id: SandboxId,
    execution_id: ExecutionId,     // ← 无 pub
    …
}
pub(super) struct ResumeLaunchPlan {
    pub sandbox_id: SandboxId,
    execution_id: ExecutionId,     // ← 无 pub
    …
}
```

于是：
1. 结构体字面量在 `launch_plan.rs` **之外**不可写（私有字段的结构体字面量是编译错误），
2. `launch_plan.rs` 内只有那三个 `for_*` 构造函数，每个各调一次 `ExecutionId::new()`，且**都不接受 execution 参数** ——
   调用方**没有办法**递一个旧的进来，
   （🔴 ✅ **裁决 D-1 改写本条**：`for_resume` 改成**接受一个 `ClaimedExecution` token 并按值消费**，自己不再 `new()`；
   由于 token 只有 claim 路径能构造，"调用方没办法递一个旧的进来"这条**结论不变**，保证依然是编译期的。见 §2.2.1 裁决块）
3. 读取只经新增的 `LaunchPlan::execution_id(&self) -> ExecutionId`（照 `sandbox_id()` 的形状，`launch_plan.rs:92-97`）。

⇒ 「拿到一个 `LaunchPlan` ⟺ 已经铸了恰好一个新 execution」是**类型层面**的，不是约定。
而 F6 说 `launch_sandbox` 是唯一消费者、且是两条启动路径唯一的汇流点 ⇒ 「启动而不换代」在 orchestrator 内**不可表达**。

🔴 **F4 的陷阱在这个形状下自动消失**：换代判定读的是 `LaunchPlan` 变体，
`LaunchPlan::for_create_from_snapshot` 明确是 Create，尽管它下游走 `LaunchMode::Resume`（`sandbox.rs:551-561`）。
**任何按 `LaunchMode` 或按 hook 类型（start-fresh / start-resume）判换代的写法都会把"从模板创建"误判成 resume** ——
今天的 `SandboxInstanceId` 恰好因为「两个 start hook 都铸」而躲过了这个坑，但它的换代**语义**是错的：
一次 create-from-snapshot 会发 `start-resume` hook。A1 之后 hook 类型与换代解耦，这个语义错误顺带修掉。

**(b) 🟢 必填穿透到 backend —— 编译期强制。**

`SandboxBackendFactory`（`src/sandbox/backend.rs:296-325`）三个方法各加一个 `execution_id: ExecutionId` 位置参数：

```rust
fn build(&self, build_spec: FreshSandboxBuildSpec, launch_config: SandboxLaunchConfig,
         execution_id: ExecutionId) -> Result<Box<dyn SandboxBackend>>;
fn build_from_snapshot(&self, snapshot: &RunnableSnapshot, launch_config: SandboxLaunchConfig,
         execution_id: ExecutionId) -> Result<Box<dyn SandboxBackend>>;
fn build_from_paused_state(&self, sandbox_id: SandboxId, execution_id: ExecutionId,
         state: &dyn PausedSandboxState, envd_access_token: Option<EnvdAccessToken>)
         -> Result<Box<dyn SandboxBackend>>;
```

trait 方法加必填参数 ⇒ **两个 impl（firecracker + mock）与全部调用点必须被编译器逐个访问**。
这就是 P1「内部必填」的机械保证：不存在「字段缺失」分支，也就不存在 fail-open 分支。
`decode_paused_state` **不加** —— 它解码的是磁盘上的旧化身状态，与「这一次要跑哪个化身」无关。

传播的下一跳：`FirecrackerSandbox::build(id, launch)`（`sandbox.rs:1182`）改成
`build(id, execution_id, launch)`，execution 落成 `FirecrackerSandbox` 的一个字段，与 `id` 同级
（CLAUDE.md 已写明 `firecracker/sandbox.rs` "now owns a stable `SandboxId` passed in by the orchestrator"，同一位置同一理由）。

🔴 **绝不能把 execution 放进 `FirecrackerCommonConfig`**：那份配置会 serde 进 paused state 与快照
（CLAUDE.md：`custom_extension_params` 就是这么 "persist through pause/resume (serde of the common config)" 的）。
放进去 ⇒ resume 时会把**上一次的 execution 从磁盘复活**，正好是 A1 要防的那件事，而且是静默的。

**(c) 🟡 snapshot / fork 父沙箱不换代 —— 只能结构保证 + 测试钉。**

`snapshot()` 与 `fork()` 都不构造 `LaunchPlan`、不调 factory，它们在 `FirecrackerSandbox` 里原地 pause+resume（F3），
`self.execution_id` 那个字段没有 setter ⇒ 没有代码路径能改它。这是**结构**保证（"没有 setter"），
不是类型保证（类型层面无法表达"这个字段永不重写"）。
⇒ 必须由测试钉住：`M-A1-3` / `M-A1-4`（§4）。

**(d) 🔴 fork 子沙箱：类型系统在这里会骗人，必须显式设计。**

`fork_sandbox_inner` 造子沙箱元数据的方式是 **clone 父的再改 id**：

```
service.rs:701-706
let mut metadata = source_metadata.clone();
metadata.id = sandbox_id;
metadata.state = SandboxState::Running;
metadata.created_at = now;
metadata.paused_state = None;
```

`SandboxMetadata` 是 `#[derive(Clone)]`（`metadata.rs:30`）。**只要 `execution_id` 是普通字段，
子沙箱就会继承父的 execution，而且编译零告警** —— 两台活着的 VM 共用一个化身身份，
A3 的 fencing 会把它们当成同一个化身，`begin_pause` 相互不拒。这是本设计里**唯一**一处类型系统帮不上忙的地方。

**处置**：`SandboxForkSpec`（`src/sandbox/backend.rs`，`service.rs:627-631` 构造）加一个必填字段 `execution_id: ExecutionId`。
该结构体无 `Default`，加必填字段 ⇒ 每个构造点编译期报错，被迫在 `SandboxId::new()` **同一个表达式**里
写出 `ExecutionId::new()`，两者共生。同时 `fork()` 里 `from_snapshot_config_with_override(…, child.sandbox_id, …)`
（`sandbox.rs:390-394`）多传一个 `child.execution_id`。
`service.rs:701` 那行 clone 之后紧跟 `metadata.execution_id = child.execution_id;`，
并由测试 `M-A1-5` 钉住"两个子沙箱的 execution 互不相同、且都不等于父"。

#### 2.2.3 传播路径全景

```
Create:  create_sandbox            → LaunchPlan::for_create_*     [铸 B-1]
                                   → launch_sandbox              (service.rs:2216)
                                   → build_sandbox               (service.rs:2388) → factory.build*(…, exec)
                                   → FirecrackerSandbox::build(id, exec, LaunchMode)
                                   → start_fresh/start_resume    → CustomExtensionHookGuard::new(client, id, exec)
                                   → transitional_metadata.execution_id = exec   (store.add, service.rs:2295)

Resume:  resume_sandbox_inner      → LaunchPlan::for_resume       [铸 B-2]   (service.rs:1606)
                                   → …同上…
                                   → final_metadata（update_if_state 内写 execution，service.rs:2336-2347）
                                   → mark_running(sandbox_id, node_id, exec, expires_at)  (service.rs:1627-1634)

Pause:   pause_sandbox_inner       → PauseOutcome{metadata}       (types.rs:99)  ← metadata 已带 exec
                                   → publish_paused → begin_pause(entry) → metadata_json  (central.rs:476-497)
                                   →（A3 在这里加 expect/install 语义）

Fork:    fork_sandbox_inner        → SandboxForkSpec{id, exec}    [铸 B-3]   (service.rs:624-631)
                                   → backend.fork(spec)          → 子 FirecrackerSandbox 各持己 exec
                                   → metadata.execution_id = child.exec       (service.rs:701-706)
```

### 2.3 持久化

| 载体 | 决策 | 依据 |
|---|---|---|
| `SandboxMetadata`（`src/orchestrator/store/metadata.rs:31-65`）| ✅ **加 `pub execution_id: ExecutionId`，必填、参与 serde、无 `#[serde(default)]`** | 它是 F11 那条链的载体：加在这里，`begin_pause` 的 `metadata_json` 与本地持久化**两条路一次到位**，零额外管道 |
| `SandboxMetadata::default()`（`metadata.rs:67+`）| 🟡 `execution_id: ExecutionId::new()` | `Default` 只被测试与 `set_metadata_state_for_test`（`service.rs:2800-2804`）用。这里铸一个新的是安全的：`Default` 造出来的对象**从来不代表一次真实运行** |
| 本地持久化文件（`src/orchestrator/persistence/file_backed.rs`）| ✅ **不新增字段**，随 `SandboxMetadata` 的 serde 一起落盘 | `persist_paused(metadata, …)`（`persistence/mod.rs:99-104`）本来就整份写 |
| 本地 RocksDB（`src/local_store.rs`）| ❌ **不用** | 沙箱持久化走的是 `file_backed.rs`，`LocalKvStore` 服务的是别的小目录（CLAUDE.md）。为一个字段引第二套存储是纯增熵 |
| committed snapshot | ❌ **不会进** | F12：`SnapshotPublishMetadata` 是逐字段构造的独立结构，不加就不进。🔴 **这是必须保持的性质**：快照是可被任意节点、任意时刻 launch 的模板，让它携带某一次运行的身份，等于给未来所有从它创建的沙箱发同一张身份证 |
| `FirecrackerCommonConfig` / paused state | ❌ **禁止** | §2.2.2(b) 已论证：会静默复活旧化身 |

**没有 `#[serde(default)]` 的后果**：老的持久化文件（dev 存量）反序列化会失败 ⇒ 该 paused 记录加载失败。
P1（未上生产、dev 存量可丢）覆盖这一点，**且这是刻意的**：一个 `#[serde(default)]` 就等于给"缺 execution 的行"
留了一条永久 fail-open 通道，任务书 §0 P1 明写要禁的就是它。
🟡 **但要在发布说明里点名**：升级前需清空节点上的 `$AENV_HOME/persisted-sandboxes`（或等价目录），
否则加载失败会以「沙箱不见了」的形态出现。见 §5 未决项 O2。

> ✅ **裁决（O2，2026-08-19 主 agent）：`serde(default)` 不加，两处"清干净"合并成同一个 runbook 步骤。**
>
> 1. **不加 `#[serde(default)]`** —— 那等于让"缺化身"静默通过，正是本轮要防的东西。
> 2. 🔴 **scheduler 侧的 `DROP TABLE paused_sandboxes`（`_design-phase3-scheduler.md` §2.7 方案 γ）
>    与本节的"清本地 paused 记录"是同一次破坏性操作的两半，必须写进 runbook 的同一个步骤**，
>    不许分成两条各自独立的指令 —— 分开写，就一定会有集群只做了其中一半，
>    症状是"中央说没有、节点说有"的半清状态，比两边都不清更难查。
> 3. 🔴 **加载失败必须响亮报错并指明处置命令**，不许静默丢弃记录：
>    错误信息里要同时出现**记录路径**、**失败原因（缺 `execution_id`）**、**处置命令**（清空该目录），
>    并计一个非零的启动期计数（形状照 `_design-phase3-scheduler.md` §2.8 那条 fail-fast 自检：
>    "一句中文加一条命令"，而不是把 serde 的原始错误抛给运维）。

**崩溃重启后怎么恢复 —— 结论：不恢复。**

| 崩溃时的沙箱状态 | 本地有什么 | A1 之后 |
|---|---|---|
| `Paused` | 有持久化记录 | 记录里带的是**产出该快照的那次 execution**，原样恢复。语义正确：那次运行确实是这份快照的作者 |
| `Running` / `Resuming` | 🔴 **什么都没有** —— 持久化只在 pause 时写（`persist_paused`），`launch_sandbox` 只写内存 store | 本地无从得知崩溃前的 execution。登记表那边是 `running` + 旧 execution 的孤儿行 |

⇒ **A1 明确不承担崩溃恢复**。收拾 `Running` 孤儿行的唯一手段仍是 `release_node_holdings`
（`central.rs:770-786`），即任务书 §4 B7。
理由与方案 §阶段 3-B 那段一字不改地成立：本机后继进程是这个系统里唯一能**证明**（而非推断）VM 已死的证据 ——
VM 是老进程的子进程，在它的 PID namespace 里。
🟡 `SandboxInstanceId` 今天的 Drop 兜底（`client.rs:330-347`）只在进程活着时有效，A1 继承这个局限，一分不多。

### 2.4 `SandboxInstanceId` 的处置

**结论：删除类型，wire 字段保留、改成 execution 的派生（其实是同一个值）。**

| 项 | 动作 |
|---|---|
| `struct SandboxInstanceId`（`client.rs:53-60`）+ `impl` + `Display`（`:62-66`）| **删除** |
| `CustomExtensionHookGuard::new(client, sandbox_id)`（`client.rs:250-256`）| 改签名 `new(client, sandbox_id, execution_id)`，字段 `sandbox_instance_id: Option<…>` 改为 `execution_id: ExecutionId` + `started: bool` |
| `start_fresh` / `start_resume` 里的 `SandboxInstanceId::new()`（`client.rs:271-272`、`:300-301`）| **删掉铸造**，改为 `self.started = true;`（记录时机不变：仍在 hook 投递**之前**置位，`client.rs:268-270` 那条注释的理由原样成立） |
| `hook_start_fresh` / `hook_start_resume` / `hook_stop` 的 `sandbox_instance_id: SandboxInstanceId` 形参 | 改成 `execution_id: ExecutionId` |
| wire 上的 `sandboxInstanceId` 字段（`models::StartFreshHookRequest` 等）| 🟢 **不动**。字段名不变、类型不变（string uuid），只换值来源 |
| `src/custom_extension_api/openapi.yml` | 🟢 **不改** |
| `src/custom_extension_api/generated/` | 🟢 **不重新生成**（`make custom-extension-client` 不用跑） |

**为什么保留 wire 字段名**：P2 要求本轮零跨仓改动。`[custom_extension].url` 今天在任何已部署环境里都没配（F1），
所以理论上改名也没人受影响 —— 但改名要动 openapi.yml + 重新 codegen + 手工清理孤儿 model 文件
（CLAUDE.md 明写生成器不删除已移除的 model 文件），换来的只是"名字更好听"。不做。
🟡 **代价**：wire 上的名字（`sandboxInstanceId`）与内部名字（execution）从此不一致。
处置：在 `client.rs` 的 doc 注释里写死这条对应关系，并在 openapi.yml 的 `description` 里加一句
（description 是纯文档字段，改它**不影响** generated 代码）—— 见 §5 未决项 O4。

> ✅ **裁决（O4）：不改名，按上面这个处置执行 —— openapi 的 `description` 与 Rust 侧各加一行注释说明内部名与 wire 名的关系。**
> 理由：A1 的一个明确收益就是**不动 `src/custom_extension_api/openapi.yml`、不重新 codegen**（还免了手工清孤儿 model 文件），改名会把这份收益吐回去；如无必要勿增实体。
>
> 🔴 **这句话的射程只到 `custom_extension_api` 那一份**：**A6 是要动 `src/api/openapi.yml` 并重新
> `make agentenv-server` 的**（§3.8.1 有对照表）。两份 openapi、两件事 ——
> 把本条读成"本轮全程不碰 openapi"会让 A6 的 node 侧整块消失，而它的失败症状是"字段恒空、零报错"。

**迁移步骤**（无数据迁移，纯代码）：
1. 先落 `ExecutionId` 类型 + `LaunchPlan` 字段 + factory 参数（编译期驱动，一次改完）；
2. `FirecrackerSandbox` 存字段、`start_fresh`/`start_resume` 造 guard 时传入；
3. 删 `SandboxInstanceId`，改 guard 与三个 hook 函数签名；
4. `client.rs:588-813` 的 9 个单测里凡是自己造 `SandboxInstanceId` 的，改造 `ExecutionId`；
   `start_guard_stop_carries_matching_instance_id`（`client.rs:589`）与
   `failed_start_still_stops_on_drop_with_matching_instance_id`（`client.rs:727`）
   的断言语义**不变**（start 与 stop 携带同一个值），只是这个值现在由外面给。
   🟢 这两个测试顺带变成了 A1 的免费回归：它们钉住"stop 报的是本次化身"。

### 2.5 mock backend（`src/sandbox/mock.rs`）

这是「内部必填」能否成立的关键：集成测试全用它，且 `MockBackendFactory` 是 `SandboxBackendFactory`
的第二个 impl（`mock.rs:398-441`）。

**结论：mock 不但要接这个参数，还要把它记下来。**

```rust
pub struct MockSandboxBackend {
    …,
    execution_id: ExecutionId,     // 由 factory 传入，无 Default
}
impl SandboxBackend for MockSandboxBackend {
    fn execution_id(&self) -> ExecutionId { self.execution_id }   // ← 新增 trait 方法
}
```

三个 factory 方法（`mock.rs:399/411/423`）签名跟着 trait 改；`MockSandboxBackend::new_with_host_ip`
多一个参数。为什么不能只 `_execution_id` 忽略掉：

- 🔴 **不记就没法在单元测试层面验证换代**。A1 的全部验收判据（"pause→resume 后 execution 变化 / snapshot 前后不变"）
  如果只能靠真 VM 的集成测试来验，就落进 `sudo -E cargo test --test orchestrator_integration` 那条需要 root + `/dev/kvm`
  的路径，CI 跑不了，变异验证也就没有牙。记下来之后，整套 A1 验收都能在 `cargo test -p agentenv --lib` 里完成。
- `SandboxBackend::execution_id()` 这个 trait 方法对 firecracker impl 是一行 `self.execution_id`，
  对 mock 是一行，成本为零，收益是让 orchestrator 单测能直接读到 backend 拿到了哪个化身
  （而不是只能读 `store` 里的 metadata —— 那样测的是"我写进去的那个值"，是自证）。

🟡 `tests/integration/orchestrator.rs` 与 `src/orchestrator/tests.rs:717-740` 那两个
`create_launch_plan_with_resources` / `resume_launch_plan` 辅助函数走的是 `LaunchPlan::for_*`，
构造函数内部铸造 ⇒ **零改动**。这是 §2.2.2(a) 那个形状的附带好处。

### 2.6 auto-resume 路径（`src/api/proxy.rs:800`）如何拿到 execution

> ✅ **裁决 D-1 之后本节要这样读**：铸造点移到 claim（§2.2.1），所以"两条 resume 在 `resume_sandbox_inner` 汇流后各自铸一次"变成
> "**两条 resume 都必须先走 claim 才拿得到 token**"。下文关于"铸造点放 API 层就会漏掉 auto-resume"的论证**结论不变、机理更强**：
> 今天靠"汇流点唯一"保证，裁决后靠"`for_resume` 只接受 claim 造出的 token"保证 —— 后者是编译期的。
> 🔴 **随之新增的工作**：auto-resume 必须接上 claim 决策点（它今天两道闸都跳，F9），否则它**根本调不出 `for_resume`**。

**答案：不需要额外做任何事，且这是设计选择的结果而不是巧合。**

`try_auto_resume`（`proxy.rs:800-830`）调的是 `orchestrator.resume_sandbox(sandbox_id, NewTimeout::…)`，
与 REST resume 走的是同一个 `resume_sandbox_inner`。铸造点 B-2 在 `LaunchPlan::for_resume`
（`service.rs:1606`），在**汇流之后**；`mark_running` 在 `service.rs:1627-1634`，也在汇流之后。
⇒ 两条路径拿到的都是本次化身的 execution。

🔴 **反面：铸造点如果放在 API 层就必然漏掉这条路径。** 一个看起来更"自然"的设计是
「在 `sandboxes_sandbox_id_resume_post` 里铸，一路传给 `arbitrate_resume` 和 `resume_sandbox`」——
那样 F9 这条路径（proxy → `try_auto_resume`）就得**自己再铸一次**，
于是"每次 resume 恰好铸一次"从类型保证退化成两处调用点的自觉，
而这两处正是历史上已经分叉过一次的地方（F9：auto-resume 跳过了 `discard_if_superseded` 与 `arbitrate_resume` 两道闸）。

**代价与它带来的顺序问题**：铸造点在汇流之后 ⇒ 它**晚于** `claim_for_resume`（F8，API 层 `arbitrate_resume` 内）。
所以本设计把两件事分开：

| 登记表操作 | 谓词依据 | 谁写 execution |
|---|---|---|
| `claim_for_resume`（`central.rs:588`）| 仍按 `generation`（**保留**，A2 决定两轴分工）| 不带 execution —— 它是**预约**，此刻化身还不存在 |
| `mark_running`（`central.rs:739`）| A3 定（今天裸奔）| ✅ **它是安装点**：把本次化身写进行里 |
| `begin_pause`（`central.rs:476`）| A3 定（今天裸奔）| ✅ **它是引用点**：quote 自己那份，行里不是自己的就拒 |

> ✅ **裁决 D-1 改写了上表的第一行**（E-A 预分配，见 §2.2.1 的裁决块）。定稿形状：
>
> | 登记表操作 | 谓词依据 | 谁写 execution |
> |---|---|---|
> | `claim_for_resume` | `generation`（不变）| 🔴 **它才是安装点**：把认领者**预分配**的化身与 `claimed_by_node_id` **同事务**写进行里 |
> | `mark_running` | A3：`state='resuming' AND claimed_by_node_id=$me AND execution_id=$me_exec` | **校验 + 确认同一个值**（不是换代）。送上去的必须是 claim 时那一个 |
> | `begin_pause` | A3：行内 execution ≠ 请求 execution ⇒ 拒 | ✅ 引用点，不变 |
> | `release_claim` | `generation` CAS（不加 execution 谓词，见 §5-O3 裁决）| 🔴 **清空**：交还 claim = 预分配作废 |
>
> 一句话理由：把"以后要记得给 `resuming` 开孤儿判定特例"这笔债，换成一次接口调整。



这与 e2b 的形状同构：`Add` 安装化身、`Remove(ExpectExecutionID)` 引用化身
（方案 §4 阶段 3-C 订正段：`storage/redis/scripts.go:33-39` 把 enforcement 放进 Lua，
因为 "Add is lockless, so a resume can install a new incarnation between a Go-side comparison and this write"）。

🔴 **本设计不解决、也不假装解决的**：auto-resume 路径**完全跳过** `arbitrate_resume`。
A1 保证了它写进去的是一个诚实的新 execution，但"该不该让它 resume"这个判断，
在这条路径上今天依然没有人做。**这必须由 A3 在 `mark_running` 的 SQL 谓词里补齐**
（行是 `running` 且 origin 是别人 ⇒ 拒，而不是覆盖）。见 §5 未决项 O1。

> ✅ **裁决（O1）：选 (a)，由 A3 在 `mark_running` 的 SQL 谓词里补齐** —— 事务内、单一裁判、与 A3 的形状一致；
> 不把仲裁逻辑复制成两份。
> 🔴 但 D-1（E-A）额外带来一条**结构性**约束：`for_resume` 只消费 claim 造出的 token ⇒ auto-resume 也必须经过 claim 才能启动。
> 两者不是二选一：SQL 谓词管"别人的 running 行抢不走"，token 管"没经过认领就根本起不来"。

### 2.7 与 envd access token 的关系（一句话划界）

`metadata.secure.then(|| self.access_tokens.generate(metadata.id))`（`service.rs:1613-1615`）——
token 由 **sandbox id** 派生，跨化身相同，与 e2b 一致（方案 §4 引 `sandbox_envd_secret.go:27-35`）。
A1 **不动它**。"envd token 绑 execution"是闸门 B 明确留给 `secure` 沙箱、seed 统一后再补的候选 2。

### 2.8 受影响文件清单

| 文件 | 改动性质 | 内容 |
|---|---|---|
| `src/types/id.rs` | 新增 | `ExecutionId` 类型 + `Display` + `TryFrom`/`From<…> for String`（照 `SandboxId` 的形状，`id.rs:37-62`） |
| `src/types/mod.rs` | 一行 | `pub use id::ExecutionId;` |
| `src/orchestrator/launch_plan.rs` | 结构改动 | 两个结构体各加私有 `execution_id`；三个 `for_*` 内铸造；新增 `execution_id()` 访问器 |
| `src/orchestrator/service.rs` | 传参 + 3 处赋值 | `build_sandbox`（`:2388-2403`）传 execution；`transitional_metadata` / `update_if_state` 闭包（`:2277-2281`、`:2336-2347`）写字段；`mark_running` 调用（`:1627-1634`）带上；fork 子沙箱（`:624-631`、`:701-706`） |
| `src/orchestrator/store/metadata.rs` | 加字段 | `pub execution_id: ExecutionId`（必填、无 serde default）；`Default` impl 补一行 |
| `src/sandbox/backend.rs` | trait 签名 | `SandboxBackendFactory` 三方法加参；`SandboxBackend` 加 `fn execution_id(&self) -> ExecutionId`；`SandboxForkSpec` 加必填字段 |
| `src/sandbox/firecracker/sandbox.rs` | 字段 + 传参 | `FirecrackerSandbox` 加 `execution_id` 字段（`:1195-1208` 附近初始化）；`build`（`:1182`）签名；`new_with_id`（`:498`）/ `from_snapshot_config_with_override`（`:521`）/ `from_snapshot`（`:549`）各多一参；两处造 guard（`:1339`、`:1545`）传入；`fork()`（`:390-394`）传 `child.execution_id` |
| `src/sandbox/custom_extension/client.rs` | 删类型 + 改签名 | §2.4；9 个单测同步 |
| `src/sandbox/mock.rs` | 加字段 + 记录 | §2.5 |
| `src/orchestrator/paused_registry/mod.rs` | trait 签名 | `mark_running` 加 `execution_id` 参数；`begin_pause` 从 `entry.metadata.execution_id` 取（**无需改签名**，F11）；`PausedSandboxPublisher::mark_running`（`:318`）同步 |
| `src/orchestrator/paused_registry/central.rs` | RPC 字段 | `mark_running`（`:739`）与 `transition_with_deadline`（`:370`）带上 execution → `TransitionSandboxRequest` 新字段 |
| `src/orchestrator/paused_registry/disabled.rs` | 签名 | `mark_running`（`:85`）跟签名 |
| `src/api/impls/paused_coordinator.rs` | 签名 | `mark_running`（`:625`）与 `mark_sandbox_running` 透传 |
| `services/api/proto/scheduler.proto` | 新增字段 | `TransitionSandboxRequest` 加 `string execution_id`（A2/A3 负责服务端语义；node 侧只负责发出）。✅ 裁决 D-1 追加：`AcquireSandboxRequest` 同样加 `string execution_id`（claim 预分配） |
| ✅ `src/orchestrator/launch_plan.rs`（追加）| 新类型 | 裁决 D-1：`ClaimedExecution` token 类型（私有字段 + 只有 claim 路径可构造），`for_resume` 按值消费它 |
| ✅ `src/api/impls/paused_recovery.rs` / `sandbox.rs`（claim 路径）| 铸造点 | 裁决 D-1：`arbitrate_resume` / claim 处铸 execution 并产出 token；`disabled.rs` 与本机 parked 就地唤醒同样只经这一个决策点 |
| ✅ `src/api/proxy.rs` | 新增比对 | 裁决 O8（§3.7）：`x-agentenv-expect-execution-id` 比对 + **412** 拒绝 + 始终回声 `x-agentenv-execution-id`；比对必须在 `try_auto_resume` 之前 |
| ✅ `src/api/openapi.yml` | 🔴 **改 + 重新 codegen** | **A6（§3.8）**：`Sandbox`（`:373`）/ `SandboxDetail`（`:407`）/ 🔴 `ListedSandbox`（`:469`）三个 schema 各加 `executionID`（非 `required`）；改完跑 `make agentenv-server`。🔴 **与本表下一行的"不改"不矛盾**：不改的是 `custom_extension_api` 那份（A1），要改的是 node 自己对外 API 这份（A6）—— §3.8.1 |
| ✅ `src/api/impls/sandbox.rs` | 三行赋值 | **A6（§3.8.3）**：`From<SandboxMetadata>` 的三个 impl（`:115` / `:134` / `:185`）各填 `execution_id: m.execution_id.to_string()` |
| ✅ `src/cfg.rs`（追加）| 新增配置 | **A4（§3.2）**：`[api] control_plane_tokens`（env，静态）**加** 🔴 `[api] control_plane_token_file`（挂载文件，热读、mtime 缓存、读失败沿用上次成功值）|
| ✅ `deploy/k8s/base/agentenv-daemonset.yaml` | 清单 | **A4**：preStop 两条 curl 加 `x-agentenv-control-plane` 头（`T-A4-6`）；🔴 **加一个 Secret 卷**承载 token 文件（`secretKeyRef` 注入的 env **不热更新**，只有卷会）|
| **不改** | — | `src/custom_extension_api/generated/` 与 `src/custom_extension_api/openapi.yml` 的**结构**（§2.4，**不重新 codegen**，只加 `description`）；`SnapshotPublishMetadata`（F12） |
| **只改注释** | 文档字段 | ✅ 裁决 O4：`src/custom_extension_api/openapi.yml` 的 `sandboxInstanceId` **`description` 加一行**说明它就是内部的 execution（description 不影响 generated 代码），`client.rs` 的 doc 注释同步一行 |

### 2.9 冻结给 A3 的接口（跨服务契约）

```
ExecutionId  = Uuid v7；proto / JSON 上是 string（小写带连字符）
```

| 调用 | 新签名（node 侧） | A3 要加的谓词 |
|---|---|---|
| `begin_pause(&entry)` | 不变，值从 `entry.metadata.execution_id` 取 | 行内 execution ≠ 请求 execution ⇒ 拒，**事务内**，零副作用 |
| `mark_running(sandbox_id, node_id, execution_id, expires_at)` | 加第 3 参 | 安装语义 + 「行是别人的 running ⇒ 拒」（补 F9 的裸奔） |
| `complete_pause` / `mark_local_only` / `release_claim` / `remove` | 🟡 建议同批带上 execution（今天只带 `generation`） | 见 §5 未决项 O3 |

> ✅ **裁决后本表定稿（两条改写、一条新增）**：
>
> | 调用 | 定稿 | 依据 |
> |---|---|---|
> | 🆕 `claim_for_resume(sandbox_id, node_id, execution_id, …)` | **加 execution 参数**：认领时预分配的化身，与 `claimed_by_node_id` 同事务写入 | D-1（E-A），§2.2.1 |
> | `mark_running(…, execution_id, …)` | 送的**必须是 claim 时那一个**；语义从"安装"变成"校验 + 确认"（跨节点 resume 分支）| D-1 |
> | `complete_pause` / `mark_local_only` / `remove` | 🔴 **本轮不加 execution**（`generation` CAS 已够）| O3 裁决 |
> | `release_claim` | **不加谓词**，但**必须清空**行上的 execution（夺权路径三条之一）| O3 裁决 + `_design-phase3-scheduler.md` §3.4 |

---

## 3. A4：node API 收窄

> **本项采用已裁决的新定义**：收窄成「node 用户级 REST 只接受来自 gateway/scheduler 的调用」，
> gateway 被认定为 controller 的前端。旧定义"只接受 controller"今天做不到 —— F18：controller 没有下行通道，
> 严格执行会把平台的 create/pause/resume/timeout/delete 全部拒掉。

### 3.1 实现层次：路由层中间件 vs 拆端口

**推荐：路由层中间件，挂在 generated router 上、`.merge(proxy::router(...))` 之前。**

```rust
// src/api/server.rs（现状 :24-41 → 目标形状）
agentenv_http_server::server::new::<I, A, E, C>(api_impl.clone())
    .layer(middleware::from_fn_with_state(          // ← 新增，只覆盖 25 条 generated 路由
        api_impl.clone(),
        control_plane_gate::require_control_plane::<I>,
    ))
    .merge(proxy::router(api_impl.clone()))         // ← 数据面在 gate 之后并入
    .route("/metrics", get(metrics_handler))
    .layer(… resume_isolation_gate …)
    .layer(… sandbox_proxy_classifier …)
    .layer(… http_metrics_middleware …)
```

**论证：**

| 维度 | 路由层中间件（pre-merge） | 拆端口 |
|---|---|---|
| 对数据面 `.fallback()` 的影响 | 🟢 **结构性零影响**。gate 是挂在 generated `Router` 值上的 layer；`proxy::router` 连同它的 `.fallback()`（`proxy.rs:148`）是**之后**并入的，gate 从未被挂到它上面 | 🟢 零影响（物理隔离） |
| 与现有三个 `.layer()` 的关系 | 🔴 现有三层是挂在**合并后**的 router 上（`server.rs:31-41`），所以它们**确实**会跑在 `/proxy/*` 和 fallback 上 —— 它们靠内部判路径躲开（`isolation.rs:53 resume_target`、`proxy.rs:152 sandbox_proxy_classifier`）。**新 gate 不能照抄这个写法**：靠"再判一次路径"就把 F13 那条"共用 listener"的风险原封不动搬进 gate 自己 | — |
| 平台侧改动 | 🟢 零。`http://<node-ip>:30800` → gateway → node:8000 的链路不变（F16 默认透传、控制面路径不改写） | 🔴 需要 scheduler 为每个 node 携带**第二个** endpoint/port（`services/api/proto` 的 `Node.endpoint` 是单值；k8s discovery 的 `Port` 是单个 `int32`，`services/shared/config` `SchedulerDiscoveryKubernetesConfig`），gateway 再按 routeSource 选端口 —— **跨 proto + 跨发现模式**，超出"零跨仓改动"的 P2 边界 |
| 回退 | 🟢 配置级：node 端 token 置空 ⇒ 全放行 | 🔴 端口不是配置级回退：改回单端口要改 Service / proto / gateway 三处并重滚 |
| 强度 | 🟡 逻辑隔离。一旦有人在 gate 之后又 `.merge()` 一个控制面 router，就漏 | 🟢 拓扑隔离，最强 |
| 可测性 | 🟢 装配顺序可用 `assemble()` 抽出来做单测（§4 `T-A4-1`） | 🟡 要起两个 listener |

⇒ **本轮做中间件，把拆端口列为长期形态**：等 B4 让 controller 真正拥有下行通道、
scheduler 需要为 node 记录第二个 endpoint 的时候，拆端口是顺手的；现在做是为了一个逻辑边界去改三处拓扑。

🔴 **`.merge()` 的 layer 保留语义是这个方案的地基，必须被测试钉住**，而不是被文档断言 ——
`T-A4-1`（§4）就是这一发；它同时是"探针自证"：先验证 gate **确实**拦住了假 generated 路由，
再验证它**确实没有**拦住 proxy 路由与 fallback。

> ✅ **裁决（O6）：接受现状（本轮做中间件，不拆端口），但 `T-A4-1` 必须被显式命名，并在测试的 doc 注释里写清它守的是哪条不变式** ——
> 逐字写明"本测试守的是 axum `Router::merge` 保留各自 layer 这一语义；它一旦被大版本升级悄悄改变，
> A4 的数据面豁免就从**结构性**退化成**巧合**"。
> 理由：它看起来像一发"测框架行为"的冗余测试，不写清楚就会在某次清理里被当噪声删掉，
> 而它是整个 pre-merge 方案唯一的守卫。

🟡 **不能靠关端口 / NetworkPolicy 实现**（F13 + F14）：控制面与数据面共用 `API_ADDR`，
关端口等于把数据面也关掉；dev 集群零 NetworkPolicy，而 `docs/src/deployment/kubernetes.md:174-199`
所依赖的"那张网"根本没铺。网络边界是**纵深**，不是机制。

### 3.2 「来自 gateway/scheduler」如何证明

**推荐：共享 secret header，node 端常数时间比较。**

| 方案 | 机制 | 回退面 | 判断 |
|---|---|---|---|
| **① 共享 secret header** | gateway 在唯一的 `Rewrite` 钩子（F17，`server.go:368-380`）无条件 `Set` 一个 `x-agentenv-control-plane: <token>`；node 在 gate 里与配置值常数时间比对 | 🟢 **两侧各一个配置项**，node 端置空 = 全放行 | ✅ **推荐** |
| ② mTLS | node 起 TLS listener、gateway 带客户端证书 | 🔴 证书签发/轮换/过期是一套独立生命周期；回退不是一个开关 | ❌ 本轮不做，长期项 |
| ③ 对端 IP 白名单 | 校验 peer address ∈ gateway Pod IP | 🔴 **今天拿不到**：F19，`axum::serve` 未用 `into_make_service_with_connect_info`；且 Pod IP 会漂 | ❌ |
| ④ K8s TokenReview / SPIFFE | node 校验 gateway 的 SA token | 🔴 引入 apiserver 依赖 + 缓存 + 失败模式；node 侧当前无 k8s client | ❌ |
| ⑤ NetworkPolicy | 网络层限制来源 | 🔴 dev 零 NetworkPolicy；30800 out-of-band（F14）；A4 的判据是"直连 node 被拒"，网络策略在 dev 上根本验不了 | ❌ 作为**纵深**保留，不作为机制 |

**方案 ① 的三条必须写死的实现约束：**

1. 🔴 **gateway 必须无条件覆盖入站同名 header，而不是"没有才加"**。
   否则外部客户端自带一个 `x-agentenv-control-plane: 随便什么`，经 gateway 透传就拿到了控制面全权 ——
   收窄反而变成了"把 presence-only 换了个 header 名"。
   `Rewrite` 钩子里：token 非空 ⇒ `req.Out.Header.Set(...)`；token **为空 ⇒ `req.Out.Header.Del(...)`**。
   后半条同样关键：gateway 关掉注入时如果只是"不设"，就成了攻击者 token 的透传管道。
2. 🔴 **常数时间比较**（`subtle`/手写等长异或），不用 `==`。这是个 secret，不是 flag。
3. 🟡 **接受一个集合而不是单值**：`Vec<String>`，便于轮换（新旧同时接受一个窗口），
   也便于 B4 之后 scheduler 直连 node 时携带**自己那把**。今天集合里通常只有一把。

**Header 名**：`x-agentenv-control-plane`。不复用 `X-Admin-Token` ——
F15 那条 presence-only 语义还在（`auth.rs:15-55` 那段注释是**准确的**，
它说的是"检查边界，不是这个函数"），而 A4 就是那个边界。两者共存、职责分离：
generated auth 层继续按 openapi 的 security 声明抽 claims，A4 的 gate 是它**之外**的一道。

**配置形状：**

| 侧 | 配置项 | 环境变量 / 载体 | 空值语义 |
|---|---|---|---|
| node | 新增 `[api] control_plane_tokens = []`（`src/cfg.rs` `AppConfig` 加 `#[config(nested)] pub api: ApiConfig`，`:78-140` 那一列）| `AENV_API_CONTROL_PLANE_TOKEN`（**静态**，进程启动时读一次）| 🟢 **空 = 关闭 gate，全放行**（= 今天的行为）⇒ 回退即置空 |
| 🔴 node（新增）| `[api] control_plane_token_file = ""` | **挂载文件**（Secret 卷），gate 取值时**热读**（mtime 缓存，命中不读盘）| 文件不存在 / 存在且为空 ⇒ 与上一行同义：不贡献任何 token |
| gateway | `gateway.control_plane_token`（`services/shared/config`）| `GATEWAY_CONTROL_PLANE_TOKEN`（照 `config.go:570/614/622` 的现成形状）| 空 = 不注入 **且** 删除入站同名 header |

> ### 🔴 为什么必须多一个"文件"形态（2026-08-19，随「node 只滚一次」的追认一起定）
>
> **生效值 = env 那份 ∪ 文件那份**（并集，便于轮换）；**两者都空 ⇒ 全放行**，回退语义一分不变。
>
> **理由是运维物理，不是洁癖**：node 是 DaemonSet，任何**进程重启**（换镜像**或只改 env**）都会走
> `run_shutdown_cleanup`，把该节点上所有非 Paused 沙箱 pause 掉（`src/orchestrator/service.rs:2604`→`:2636`），
> 而 `terminationGracePeriodSeconds: 3600` + `maxSurge: 0` 让它**逐节点串行**。
> ⇒ 若 A4 的启用点是 env，「node 只滚一次」省下的那次 pause 风暴会在**启用时**原样还回来，
> 回退演练（总 runbook §6.5 R1/R2 要求真的翻回来再翻回去）还要再各付一次。
> 走挂载文件则：**启用 = 写 Secret、回退 = 清空 Secret，两个方向都零重启**。
>
> 🔴 **必须是卷、不能是 `secretKeyRef` 的 env**：Secret 以 env 注入时 kubelet **不会**热更新，只有**卷**会
> （刷新有 ≤60s 延迟，验收时要等）。
>
> **读失败怎么办（三态，别写成两态）**：
> | 情形 | 行为 |
> |---|---|
> | 文件存在、可读 | 采用其内容（按行 split，trim，丢空行）|
> | 文件存在、**这次**读失败（IO 抖动）| 🟡 **沿用上一次成功读到的值**，打 warn + 计数（§3.9）。既不 fail-open 到全放行，也不 fail-closed 到拒绝一切 |
> | 启动时就读不到 / 未配置 | 视为空 ⇒ 全放行（= 今天的行为），启动日志打一行 info 说明 gate 未启用 |
>
> 🟡 **若主 agent 否决这条新增机制**：A4 的启用与回退各需一次 DaemonSet 滚动，
> 那么**每次翻转前必须先主动 drain**（按正常路径把全集群沙箱 pause 干净）才能把风暴压到可控 ——
> 这条替代路径必须写进 runbook，不许默认"翻个 env 而已"。

🔴 **顺序不可颠倒**：先滚 gateway（开始注入）→ 确认注入生效 → **再把 token 写进 node 的文件**（开始强制）。
反过来会有一个窗口，期间 node 拒掉所有平台流量。这条已写进总 runbook 的步骤 8 → 9。

### 3.3 收窄的路径清单

**纳入（generated router 全部 25 条路由中的 24 条，即 openapi.yml 里除 `/health` 外的全部 operation）：**

| 路径 | 方法 | 备注 |
|---|---|---|
| `/sandboxes` | ~~GET~~ / POST | 🔴 **GET 已按裁决 D-6 移出纳入清单**（见下方豁免表新增行）；**POST 仍纳入**。原备注：GET 被 preStop 用（F20），见 §3.4 |
| `/sandboxes-cold` | POST | |
| `/v2/sandboxes` | GET | |
| `/sandboxes/{id}` | GET / DELETE | |
| `/sandboxes/{id}/pause` | POST | 破坏性 |
| `/sandboxes/{id}/resume` | POST | 破坏性；gate 跑在 `resume_isolation_gate` 之内还是之外见下 |
| `/sandboxes/{id}/fork` | POST | |
| `/sandboxes/{id}/connect` | POST | |
| `/sandboxes/{id}/timeout` | POST | |
| `/sandboxes/{id}/network` | PUT | |
| `/sandboxes/{id}/custom-extension-params` | GET / PATCH | |
| `/sandboxes/{id}/refreshes` | POST | |
| `/sandboxes/{id}/snapshots` | POST | 破坏性（写快照链） |
| `/snapshots`、`/snapshots/{id}` | GET | |
| `/templates`、`/v2/templates`、`/v3/templates`、`/templates/{id}`、`/templates/aliases/{alias}`、`/v2/templates/{tid}/builds/{bid}`、`/templates/{tid}/builds/{bid}/status` | GET / POST / DELETE | |
| `/nodes`、`/nodes/{id}` | GET | |
| **`/nodes/{id}`** | **POST** | §3.4 |

**豁免（逐条给理由）：**

| 路径 | 为什么豁免 | 机制 |
|---|---|---|
| `/proxy/*` + fallback（数据面反代）| 外部沙箱流量的入口，本来就该直连 node；且它不吃 generated auth 层 | 🟢 **结构性**：在 `.merge()` 之后，gate 从未挂上（§3.1） |
| `/metrics` | Prometheus 抓取来自 kubelet/Prometheus，不经 gateway | 🟢 **结构性**：`.route("/metrics", …)` 在 merge 之后（`server.rs:30`）；ublk 的另一个 metrics 口在 9103，独立 listener |
| `/health` | 🔴 **kubelet 三种探针的目标**（F21），kubelet 不经 gateway、也不会带 token。挂上 gate = Pod 起不来 | 🔴 **必须在 gate 里显式豁免**（唯一一条路径级豁免） |
| ✅ **`GET /sandboxes`（含 `GET /v2/sandboxes`）** | 🔴 **裁决 D-6（对应 gateway 设计 D6 / R-9）**：gateway 的集群列表是**自聚合扇出**（`cluster_list.go` 的 `fetchNodeClusterList`），走的是 gateway 自己的 HTTP client，**不经 `ReverseProxy.Rewrite`**，因此拿不到 §3.2 注入的 token；而该端点是 **all-or-nothing**（`cluster_list.go:84-96`，任一 node 失败整体失败）⇒ 不放行就是**整个集群列表直接 502** | 🔴 **在 gate 里显式豁免，与 `/health` 同一处**。⚠️ **只豁免只读的 GET**，`POST /sandboxes`（创建）**不豁免** |

⇒ gate 的路径判断有**两条**：`/health` 与 `GET /sandboxes`（`GET /v2/sandboxes` 同）。
这是刻意的：豁免清单越短越可审。🟡 若将来 openapi 增加公开只读端点，必须同批更新这个清单**并加一发测试**。

> ✅ **裁决 D-6 的两条附带说明**：
> 1. 🟡 **打击面**：`GET /sandboxes` 是只读列举，泄露的是本节点上的沙箱 id 与资源信息 ——
>    与今天（presence-only，等于全公开）相比不劣化，且它本来就是 gateway 聚合出去的公开视图。
>    **收窄的价值集中在破坏性写**（pause / resume / delete / fork / `POST /nodes/{id}`），只读列举不是价值主体。
> 2. 🟢 **顺带解决了 preStop 的一半**：F20 那条 `curl … /sandboxes | jq 'length'` 因此不再需要 token；
>    但 §3.4 的 `POST /nodes/${AENV_NODE_ID}` **仍然需要**，那条 header 一条都不能少。
> 3. 🔴 **必须配一发测试**（✅ 2026-08-19 已在 §4.2 编号为 **`T-A4-7 the_cluster_list_fanout_is_never_gated`**，原先只在这里点名、编号表里没有）：
>    无 token 的 `GET /sandboxes` ⇒ 非 403；无 token 的 `POST /sandboxes` ⇒ 403。
>    后半句是对照面 —— 没有它，"把整个 `/sandboxes` 路径前缀豁免掉"这个变异会假绿。

**与 `resume_isolation_gate` 的相对位置**：A4 的 gate 在 generated router 内层，
`resume_isolation_gate` 在合并后的外层（`server.rs:33-36`）⇒ **isolation gate 先跑**。
🟡 后果：一个无 token 的直连 resume，在隔离节点上会先拿到 503 reroute、而不是 403。
这不影响安全（503 不泄露任何东西、也不执行任何动作），但会让 A4 的验收探针在隔离节点上读到 503。
**验收探针必须在非隔离节点上跑**，或显式断言 403。写进 §4 `T-A4-4` 的前置条件。

> ✅ **裁决（O7）：接受这条顺序，记为已知偏差**（不构成安全问题，仅隔离节点上的诊断信息略差）。
> 理由：把 gate 提到最外层就得在 gate 里再判一次路径，那正是 §3.1 表格第二行明确反对的写法 ——
> 为一点诊断可读性，去换回 F13「共用 listener」那条风险，不划算。
> 🔴 **代价必须留在验收 runbook 里**：探针在隔离节点上读到的 503 **不能**被读成"收窄没生效"。

### 3.4 `POST /nodes/{id}` 的处置

**纳入收窄，无豁免。** 它是当前最锋利的那把：gateway 无条件透传（`services/gateway/internal/node_list.go:175-220`）
+ node presence-only（F15）⇒ 任何能打到 30800 的人都能把节点置 DRAINING。无代码调用方（除 preStop），
收窄的打击面为零。

🔴 **但 preStop 是它的调用方，且打的是 localhost：**

```
deploy/k8s/base/agentenv-daemonset.yaml:126-159
curl -sf -X POST -H 'X-Admin-Token: preStop' … "http://localhost:8000/nodes/${AENV_NODE_ID}"
curl -sf -H 'X-API-Key: preStop' http://localhost:8000/sandboxes | jq 'length'
```

两条都会被 gate 拒。**处置：给 preStop 带上 token**，而不是豁免 loopback：

```yaml
curl -sf -X POST -H "X-Admin-Token: preStop" \
     -H "x-agentenv-control-plane: ${AENV_API_CONTROL_PLANE_TOKEN}" …
```

token 已经作为环境变量在同一个容器里（node server 自己要读它），preStop 是同一个 Pod 的同一个容器 ⇒ 零新增分发。

**为什么不豁免 loopback**：
- F19：`ConnectInfo` 今天拿不到，要豁免得先改 `axum::serve` 的 make-service 形态 —— 一个为了少写一行 curl header 的运行时改动；
- 更重要的是**它是个洞**：`privileged: true` 的容器里任何东西都能打 localhost:8000，
  而这台机器上跑着用户代码（VM 内的用户代码理论上打不到宿主 loopback，但 A4 的价值恰恰是不去依赖"理论上")。

🔴 **preStop 的失败是静默的**：`… || echo "preStop: failed to drain node, continuing"`（`:145`）——
token 忘了带，drain 就悄悄不生效，症状是"滚动升级期间沙箱被放到正在下线的节点上"。
⇒ 变异验证 `M-A4-3`（§4）专门钉这条。

### 3.5 🔴 A4 **不**负责节点自主路径

**声明：A4 收的是"外面打进来"，对"节点自己动手"完全无效。** 以下路径不经过任何 HTTP 请求，
A4 挡不住其中任何一条：

| 自主路径 | 触发者 | 代码位置 |
|---|---|---|
| TTL 到期自动 pause / delete | orchestrator 的 auto-eviction task | `src/orchestrator/service.rs`（`SandboxTimeoutAction::Pause`/`Delete`，`metadata.rs:17-20`）|
| 优雅关机 pause | `shutdown()` → 把 running 沙箱 pause 并持久化 | `src/bin/server.rs:214-250`；CLAUDE.md "On graceful shutdown, running sandboxes are paused and persisted" |
| Drop 兜底 teardown | `impl Drop for FirecrackerSandbox` | `src/sandbox/firecracker/sandbox.rs:1157-1176` |
| 后台 reconcile / reclaim upkeep | `paused_upkeep` 任务 | `src/bin/server.rs:183-192`（`:224` abort） |
| 数据面 auto-resume | proxy 的 `try_auto_resume` | `src/api/proxy.rs:686 → :800`（🔴 它**经过** listener，但走的是数据面路径，A4 结构性豁免它）|

⇒ **一个分区中的旧化身，即使 A4 全开，仍然可以自己把工作区 pause 掉并写进快照链。**
堵这条路的是 **A3（写路径 fencing，SQL 事务内校验 execution）**，不是 A4。
把 A4 读成"收窄了就安全了"是本阶段最容易犯的错，任务书 §7.3 已经写明我们在接管上比 e2b 激进。

🟡 特别点名最后一行：auto-resume 走数据面，A4 **有意**不管它，而它今天还跳过了 `arbitrate_resume`（F9）。
这一条在 A1 里已标注（§2.6），在 A3 那边闭合。

### 3.6 回退方案

| 层级 | 动作 | 生效范围 |
|---|---|---|
| **node**（推荐首选）| 🔴 **清空挂载的 control-plane token 文件**（§3.2）—— **热生效，不滚 DaemonSet** | gate 整体关闭，回到今天的 presence-only。**这是配置级回退，满足任务书 §6** |
| ~~node（env 形态）~~ | ~~`AENV_API_CONTROL_PLANE_TOKEN=""` + 滚 DaemonSet~~ | 🔴 **不要用这条做回退**：滚一次 DaemonSet = 一次全集群 pause 风暴（§3.2 的理由块）。env 只在"从一开始就没启用"时有意义 |
| **gateway** | `GATEWAY_CONTROL_PLANE_TOKEN=""`，滚 Deployment | 停止注入 **并**删除入站同名 header。🔴 单独做这一步会让 node 拒掉一切 —— 回退必须 **node 先** |
| 代码级 | 无 | 🟢 不需要：gate 的"空 token = 放行"分支本身就是回退路径，且它是**默认值**，会被每一次不配 token 的单测覆盖到 |

🔴 **30800 out-of-band 资源的注意事项**：那个 NodePort Service 在仓内清单里**不存在**（F14，`grep -rn nodePort deploy/` 零命中），
是集群里手工 apply 的。后果有三条，回退演练必须点名：

1. `kubectl apply -k deploy/k8s/…` **不会**动它 —— 既不会重建也不会删除。以为"重新部署一遍就恢复原状"的人会踩空。
2. A4 的验收判据是「直连 node 的破坏性调用被拒 / 经 gateway 的同一调用成功」。
   而 30800 打的是 **gateway**（F16），所以**用 30800 验不出"直连被拒"** ——
   探针必须 `kubectl exec` 进一个 Pod、或 `port-forward` 到 node Pod 的 8000 直连。
   🔴 这正是"探针必须自证"的适用场景：用 30800 打一发看到 200，说明的是 gateway 注入生效，**不是**收窄失效。
3. 回退演练结束后要单独确认 30800 Service 仍在（它是平台的入口），
   `kubectl -n <ns> get svc agentenv-gateway-nodeport`。

---

## 3.7 ✅ 新增工作：A5 的接收端（裁决 O8 / gateway 设计 D5）

> **本节是 2026-08-19 裁决新加进 node 侧工作清单的**。O8 原文是"本设计没有给 A5 预留 gateway → node 的化身传递通道"，
> gateway 设计已经把通道定死了（`_design-phase3-gateway.md` §3.1/§3.2/§6），**但接收端在 node，属于本设计**。
> 🔴 **它不许掉在两份文档中间** —— 那是"注释承诺别处会做、结果没做"的标准形状。

**契约（gateway 侧已冻结，node 侧照实现，不再讨论）：**

| 方向 | 头 | 语义 |
|---|---|---|
| gateway → node（请求）| `x-agentenv-expect-execution-id: <E>` | "我把这条请求发给你，是因为中央认为该沙箱当前的化身是 E"。**只在数据面 + `authority=REGISTRY` 时下发** |
| node → gateway（响应）| `x-agentenv-execution-id: <本机活化身>` | **始终回**（包括放行时）。它同时是 node "已装配 A5" 的能力信号 —— gateway 见不到它就计 `unfenced_node_silent` |
| node → gateway（拒绝）| **`412 Precondition Failed`** + `x-agentenv-refusal: sandbox_execution_superseded` | 🔴 **内部信号，绝不直通客户端**：gateway 收到这对组合后改写成 **409 + `code=sandbox_execution_superseded`** |

**实现要点（五条，逐条都是硬要求）：**

1. **落点在数据面反代**（`src/api/proxy.rs`），**不是** A4 的 gate。A4 的 gate 结构性地不覆盖 `/proxy` 与 fallback（§3.1），
   而 A5 要管的正是那一侧 —— 两者互补，代码上零交叉。
2. **头不存在 ⇒ 放行**。gateway 只在 `authority=REGISTRY` 时下发；把"缺头"当拒绝，等于让
   `UNKNOWN` / `PENDING` 的正常流量全线 412。**这是按契约的 fail-open，由 gateway 侧计数**（`unfenced_no_authority`）。
3. **比对对象是"本机该沙箱此刻活着的化身"**，即 A1 落在 backend / metadata 上的那个值（`SandboxBackend::execution_id()`，§2.5）——
   不是登记表、不是持久化文件。node 在这条路上是**事实的一侧**，中央是**权威的一侧**，两侧都不在客户端手上。

   > ### 🔴 ✅ 裁决 A5-U4（2026-08-19 主 agent）：**比对必须是有序的，不是等值的**
   >
   > 记本机活化身为 `live`、gateway 下发的 expect 为 `E`（两者都是**小写 canonical UUID v7**，字典序即时间序）：
   >
   > | 关系 | 动作 | 语义 |
   > |---|---|---|
   > | `live < E` | 🔴 **拒**（412）| "中央知道一个比我新的化身 ⇒ 我已被取代" |
   > | `live == E` | ✅ 放行 | 绝大多数请求 |
   > | `live > E` | ✅ **放行**，但**计一个 `execution_ahead` 计数**（不是静默）—— 🔴 **指标名与标签见 §3.9.2：`agentenv_proxy_execution_fencing_total{decision="pass_ahead"}`**，别另起名字 | "我比中央知道的新 ⇒ 中央只是落后了一个心跳，而它路由到的就是我，不存在第二份活的要防" |
   > | **本机没有这台沙箱** | ✅ **放行**，落回既有的 404 / auto-resume 路径 | 见下方第 3b 条 |
   >
   > 🔴 **为什么不能用等值**：`live > E` 是**常规事件**而非异常 —— TTL 自动 pause 默认 **1 秒一跳**
   > （`src/orchestrator/service.rs:2094` + `config/default.toml:198`）叠加数据面 auto-resume 铸新化身，
   > 「同机 pause→resume」每天都在发生。等值比对会在这条最常见的合法路径上**批量制造 412 → 409**，
   > 而误拒恰好发生在用户正等着唤醒的时刻。
   > （完整论证见 [`_design-phase3-scheduler-a5.md`](_design-phase3-scheduler-a5.md) §2.2：
   > 路由与 expect 来自同一个 lookup 答案 ⇒ `live < E` 只可能来自"节点比中央旧"这一种情形。）

3b. 🔴 **拒绝必须只凭正面证据**（同一条裁决的另一半，来自 scheduler-a5 §S5）：
   **只有"本机确有该沙箱的活化身，且它比 expect 旧"才拒**。
   「本机没有这台沙箱」**不得**当成拒绝 —— 跨节点 resume 的 `resuming` 窗口里，gateway 会带着 claim
   预分配的化身把数据面流量打到认领者 B，而 B 的 VM 可能**还没起来**；把"没有"读成"旧"，
   resume 窗口内每一个数据面请求都会被拒。
4. 🔴 **拒绝必须零副作用，尤其不许触发 auto-resume**：比对要发生在 `try_auto_resume`（F9，`proxy.rs:686` → `:800`）**之前**。
   顺序反了，一条陈旧化身的流量就能唤醒一台沙箱 —— 那正好是 A5 想挡的事被 A5 自己触发。
5. 🔴 **码位不许撞车**：`/proxy` 今天已经用 404 表示 sandbox not found、410 表示 not proxyable
   （`src/api/proxy.rs:851-890`，gateway 设计 §6.2 已核）。
   A5 的拒绝**必须**用 412（该码表上的空位），**绝不能**是 404（平台据此重建工作区）、**也不能复用 410**。

**必须配的测试（并入 §4.2 A4 那张表的编号序列）：**

| # | 测试 | 断言 | 🔴 变异 |
|---|---|---|---|
| T-A5N-1 | `a_proxy_request_naming_a_dead_execution_is_refused` | 带 `x-agentenv-expect-execution-id` 且 **`live < E`** ⇒ **412** + `x-agentenv-refusal: sandbox_execution_superseded`；**沙箱未被唤醒、无任何副作用** | 把比对挪到 `try_auto_resume` 之后 ⇒ 沙箱被唤醒 ⇒ FAIL |
| T-A5N-2 | `a_proxy_request_naming_the_live_execution_passes_through`（**对照组**）| `live == E` ⇒ 正常转发。没有它，"一律 412"的实现能让 T-A5N-1 全绿 | 让 node 一律拒 ⇒ FAIL |
| 🔴 T-A5N-6 | `a_node_ahead_of_the_control_plane_still_serves`（**裁决 A5-U4**）| `live > E`（本机刚做完一次同机 pause→resume，中央还落后一个心跳）⇒ **放行**，且 `agentenv_proxy_execution_fencing_total{decision="pass_ahead"}` +1（§3.9.2）| 把比对写成等值（`live != E ⇒ 拒`）⇒ **FAIL**。🔴 这一发防的是"每一次同机 pause→resume 都被 A5 自己拒死" |
| 🔴 T-A5N-7 | `a_sandbox_this_node_does_not_have_is_not_a_superseded_execution`（**只凭正面证据**）| 本机无该沙箱 + 带 expect 头 ⇒ **不是 412**（走既有 404 / auto-resume 路径）| 把"本机没有"当成 `live < E` ⇒ FAIL。🔴 这一发防的是"跨节点 resume 窗口内数据面全线被拒" |
| T-A5N-3 | `a_proxy_request_without_the_expect_header_is_not_refused` | 缺头 ⇒ 放行（契约 fail-open）| 把缺头当拒绝 ⇒ FAIL |
| T-A5N-4 | `the_refusal_is_never_four_oh_four_or_gone` | 拒绝的 status **≠ 404 且 ≠ 410**，且**是 412** | 把 412 改成 404/410 ⇒ FAIL（这一发防的是用户工作区蒸发，与 gateway 的 `TestFencingRefusalIsNeverFourOhFour` 成对）|
| T-A5N-5 | `every_proxy_response_names_the_execution_that_served_it` | 放行与拒绝**两种**响应上都带 `x-agentenv-execution-id` | 只在拒绝时回声 ⇒ gateway 的能力探测失效 ⇒ FAIL |

---

## 3.8 ✅ 新增工作：A6 的 node 侧（外部只读暴露 execution）

> **本节是 2026-08-19 补的无主工作项。** A6 在任务书 §3 是一行（"外部只读暴露 execution
> （`GET /sandboxes/{id}`、resume 响应）；字段存在且与内部一致；**不作为入参**"），
> gateway 设计 §9 把它按端点拆过一次，但那张表只判了"gateway 要不要改"，
> 🔴 **node 这一侧（openapi + codegen + 响应体）在三份设计里都没有归属**。现在归 **impl-node**。

### 3.8.1 🔴 先澄清一处最容易误读的地方：A1 不动 openapi，**A6 要动**

| | 哪份 openapi | 动不动 | 为什么 |
|---|---|---|---|
| **A1**（§2.4 / 裁决 O4）| `src/custom_extension_api/openapi.yml`（**hook 客户端**）| 🟢 **不动结构、不重新 codegen**，只在 `sandboxInstanceId` 的 `description` 加一行 | 保留 wire 字段名 = 免掉一次 codegen 与手工清孤儿 model |
| **A6**（本节）| `src/api/openapi.yml`（**node 自己的对外 API**）| 🔴 **要动，要 `make agentenv-server` 重新 codegen** | 新字段必须出现在 generated model 里，否则 `From<SandboxMetadata>` 根本没有字段可填 |

🔴 **这是两个不同的 openapi 文件、两件不同的事**。把 §2.4 / §0 里那句「不动 openapi.yml、不重新 codegen」
读成"本轮全程不碰 openapi"，是本设计最可能被误读的一处 —— 那句话的射程只到 `custom_extension_api` 那一份。
§2.8 的文件清单已按此改写：`src/api/openapi.yml` 现在是**独立一行的"改 + 重新 codegen"**，
"不改"那一行只剩 `custom_extension_api` 与 `SnapshotPublishMetadata`。
🔴 **漏读的后果不是编译错，是"字段恒空、零报错"** —— 而 gateway 那半照样改得完、照样全绿。

### 3.8.2 改哪三个 schema（字段名对齐同一份 schema 里的既有惯例）

`src/api/openapi.yml` 的 id 字段惯例是 **camelCase + `ID` 全大写后缀**：
`templateID` / `sandboxID` / `clientID` / `nodeID` / `buildID` / `snapshotID` / `accessTokenID`
（`components.parameters` 与三个 sandbox schema 里逐个可数）。
⇒ 🔴 **字段名定为 `executionID`**，不是 `execution_id`、不是 `executionId`。**不进 `required`**（additive，老客户端不受影响）。

| schema（行号）| 服务于哪些响应 | 加什么 |
|---|---|---|
| `SandboxDetail`（`openapi.yml:407`）| `GET /sandboxes/{sandboxID}`（`:1488`）| `executionID: {type: string, description: ...}` |
| `Sandbox`（`:373`）| `POST /sandboxes`（`:1371`）、`POST /sandboxes-cold`（`:1410`）、🔴 **`POST /sandboxes/{sandboxID}/resume`（`:1570`）**、`POST /sandboxes/{sandboxID}/connect`（`:1653`/`:1659`）、`SandboxForkResult.sandbox`（`:690`）| 同上 |
| 🔴 `ListedSandbox`（`:469`）| `GET /sandboxes`（`:1334`）、`GET /v2/sandboxes`（`:1461`）| 同上 —— **这一条最容易漏，而漏了 gateway 那半就永远是空的**（下方 3.8.4）|

🟡 `ResumedSandbox`（`:557`）是 resume 的**请求体**，**绝不加** —— A6 的红线是"响应字段，不作为入参"。

### 3.8.3 Rust 侧：三行赋值

三处 `From<SandboxMetadata>` 各填一行（值直接来自 A1 落在 metadata 上的那个化身，零新增管道）：

| impl | 位置 |
|---|---|
| `impl From<SandboxMetadata> for models::ListedSandbox` | `src/api/impls/sandbox.rs:115` |
| `impl From<SandboxMetadata> for models::Sandbox` | `:134` |
| `impl From<SandboxMetadata> for models::SandboxDetail` | `:185` |

```rust
execution_id: m.execution_id.to_string(),   // 小写 canonical UUID v7（任务书 §11.1(a)）
```

🟡 `sandbox_model()` / `sandbox_detail_model()`（`:221` / `:235`）**不用改** —— 它们只在 `From` 之后补
`envd_access_token` 与 `domain`。

### 3.8.4 🔴 与 gateway 的交界（两边都要做，做的不是同一件事）

| 谁 | 做什么 | 漏了的症状 |
|---|---|---|
| **node（本节）**| `openapi.yml` + codegen + 三个 schema + 三行赋值 | gateway 那半改了也没用：`listedSandbox.ExecutionID` 恒空 |
| **gateway**（`_design-phase3-gateway.md` §9）| 只改**它自己拼的**三个 DTO：`listedSandbox`（`cluster_list.go:23-36`）、`registrySandboxItem`（`registry_list.go:25-38`）、以及 `/v2/sandboxes` 的去重 | node 报了，`json.Decode` 到结构体时**未知字段被静默丢弃** |

🔴 **两种失败长得一模一样：字段恒空、零报错。** 所以两侧各配一发测试（下方 T-A6-1 与
gateway §11.6），并且在步骤 8 的验收里明写"此刻应当**看得见** `executionID`"（总 runbook 步骤 8 验证项 ④）。

🟢 `GET /registry/sandboxes` 的数据来源是 scheduler 的 `RegistrySandbox` proto（A2/A3 加字段），
**与 node 无关** —— node 不参与那条链。

### 3.8.5 测试

| # | 测试函数（建议名 / 位置） | 断言 | 🔴 变异 |
|---|---|---|---|
| T-A6-1 | `every_sandbox_response_names_its_execution`<br>`src/api/impls/sandbox.rs` 的 `#[cfg(test)]` | 同一份 `SandboxMetadata` 分别转成 `ListedSandbox` / `Sandbox` / `SandboxDetail`，三者的 `execution_id` **都非空且逐字相同** | 任意一处漏填（尤其 `ListedSandbox`）⇒ FAIL |
| T-A6-2 | `the_execution_is_never_an_input`<br>同上 / openapi 清单校验 | `ResumedSandbox` 与所有 `*Request` schema **不含** execution 字段 | 把 `executionID` 加进 `ResumedSandbox` ⇒ FAIL（这一发钉住 A6 的红线）|

---

## 3.9 指标规范（node 侧）

> **本节 2026-08-19 新增**：[`_design-phase3-gateway.md`](_design-phase3-gateway.md) §11.8 与
> [`_design-phase3-scheduler-a5.md`](_design-phase3-scheduler-a5.md) §9 两份设计各自都有封闭标签集的指标章节，
> 本设计原先**全文没有 metrics 章节**，而 §3.7 已经引入了"`execution_ahead` 计数"却没给指标名 ——
> 一个没有名字的计数，实现者要么不写、要么各写各的。

### 3.9.1 沿用 node 既有写法（不引新设施）

| 约定 | 依据（仓内现状）|
|---|---|
| 用 `metrics::counter!` / `gauge!` **在使用点直接声明**，**没有**集中注册表 | `src/api/impls/paused_recovery.rs:1067-1074`（`record_supersession`）、`src/api/impls/paused_coordinator.rs:382-385`、`src/image/resolver.rs:232-235` |
| 前缀 `agentenv_`，计数器以 `_total` 结尾 | 同上；`agentenv_paused_registry_*_total` / `agentenv_snapshot_oss_upload_bytes_total` |
| 标签值一律 `&'static str` 的**封闭集**；🔴 **绝不放 `sandbox_id` / `execution_id` / 路径 id** | `http_route_label`（`src/observability/prometheus.rs:259`）+ `dynamic_route_label`（`:275`）把所有 id 折成 `{sandbox_id}` 占位，并有 `route_labels_hide_ids`（`:307`）钉住 |
| 成败用 `result_status(ok)` ⇒ `"ok"` / `"error"` | `prometheus.rs:227` |

🔴 **高基数是本节唯一的硬红线**：下面五条 series 全部与**单个沙箱**的事件有关，
把 `sandbox_id` 或 `execution_id` 放进标签会让 series 数随沙箱数无界增长。
**要按沙箱查就查日志** —— 日志字段已按任务书 §11.1(g) 跨三段统一
（`sandbox_id` / `expected_execution_id` / `observed_execution_id` / `refusal_code` / `fencing_stage` / `node_id`）。

### 3.9.2 五条 series（A4 三条 / A5 接收端一条 / 本地持久化一条）

| 指标 | 标签（**封闭**）| 用途 |
|---|---|---|
| `agentenv_api_control_plane_gate_total`（A4，§3.3）| `decision` = `allowed` / `refused` / `exempt` / `disabled` | `refused` 在健康集群上恒 0（平台流量全经 gateway，都带 token）；🔴 **非 0 就是"平台有一条路径没经 gateway"**，比看 403 日志直接。`exempt` = `/health` 与 `GET /sandboxes` 两条豁免；`disabled` = 未配 token 的放行（回退态自证）|
| `agentenv_api_control_plane_gate_enabled`（A4）| —— | 常驻 gauge，`0` = 未配 token（全放行）/ `1` = gate 生效。照 scheduler-a5 §9 那条「关掉时必须是刺耳的」；总 runbook 步骤 9 的热翻转**就看它从 0 变 1** |
| `agentenv_api_control_plane_token_reload_total`（A4，§3.2 的热读）| `result` = `loaded` / `unchanged` / `error_kept_previous` | 🔴 `error_kept_previous` 涨 = 文件读不到、正在用上一次的值。没有它，"Secret 卷没刷新"与"翻转已生效"在外部看**完全一样** |
| `agentenv_proxy_execution_fencing_total`（A5 接收端，§3.7）| `decision` = `pass` / **`pass_ahead`** / `pass_no_expect` / `pass_absent` / `refused_stale` | 🔴 **§3.7 里那个"`execution_ahead` 计数"就是这里的 `decision="pass_ahead"`**，别再另起名字。与 gateway 的 `agentenv_gateway_execution_fencing_total`（gateway §11.8）**逐条对账**：node 的 `pass_ahead` 应约等于 gateway 的 `unfenced_node_ahead`，⚠️ ~~`refused_stale` 应约等于 gateway 的 `refused_echo` + `refused_preflight`~~ **已补适用条件（2026-08-20）：这条等式只在 gateway 处于 `enforce` 时成立**。🔴 **`observe` 期它必然对不上，且对不上是正常的**：observe 不下发 expect 头 ⇒ node 侧没有 expect 可比 ⇒ ① node 的 `refused_stale` **恒 0**，而 gateway 的 `refused_echo` **可以非 0**（闸 2 读的是 node 无条件回声，不需要 expect）；② node 侧的数据面流量会**全量落到 `pass_no_expect`**，而不是 `pass`。拿这条等式在 observe 期对账，只会得出“node 侧的闸坏了”这个错误结论 —— 它没坏，是没上膛。🔴 **`pass_ahead` ↔ `unfenced_node_ahead` 那半条在 observe 期同样不成立**：`pass_ahead` 也要先有 expect 才判得出来（`fencing_decision` 的第一道分支就是 `expected` 为空 ⇒ `PassNoExpect`），所以 observe 期 node 侧这五值里**只有 `pass_no_expect` 会动**，而 gateway 的 `unfenced_node_ahead` 照常非 0。✅ **observe 期 node 侧这个系列唯一的正确用法**：确认它**全落在 `pass_no_expect`** —— 那是“gateway 确实没在盖章”的 node 侧自证，也正是 runbook 步骤 10 判据②“`refused_preflight` 非 0 才是异常”的另一面 |
| `agentenv_persisted_sandbox_load_total`（§2.3 裁决 O2 要求的"非零启动期计数"）| `result` = `restored` / `rejected` | `rejected` 非 0 = 有本地 paused 记录缺 `execution_id`（runbook 步骤 2 的 node 那一半漏做）。🔴 裁决 O2 明写"加载失败必须响亮报错并指明处置命令"，**日志负责说人话，这个计数负责让它可告警** |

🟡 **`decision` 五值为什么这么切**（A5 接收端）：它必须与任务书 §11.1(d) 的比对表**一一对应**，
否则实现者会自己合并成"pass / refuse"两值，而那样 `pass_ahead`（同机换代，常规）与
`pass_absent`（跨节点 resume 窗口，也常规）就再也分不开 —— 这两者恰好是裁决 A5-U4 与"只凭正面证据"
两条最容易被写错的分支，它们的计数就是这两条裁决在生产上的唯一证据。

---

## 4. 测试计划与变异验证

**方法论**：任务书 §8.2 ——每条修复配一发变异，把修复退回去、确认指定测试**真的 FAIL**。
下表把任务书 §3 给的 A1/A4 变异方向具体化到**测试函数名**。命名沿用仓内既有的陈述句风格
（如 `client.rs:589 start_guard_stop_carries_matching_instance_id`、
`central.rs:1133 no_grpc_failure_ever_arrives_as_an_empty_batch`）。

### 4.1 A1

| # | 测试函数（建议名 / 位置） | 断言 | 🔴 变异（把修复退回去） | 必须 FAIL 的测试 |
|---|---|---|---|---|
| T-A1-1 | `a_resume_runs_under_a_new_execution`<br>`src/orchestrator/tests.rs` | mock backend：create → 记 e0 → pause → resume → 记 e1；`e1 != e0`，且 `store.get().execution_id == e1` | 让 `LaunchPlan::for_resume` 复用旧 execution（加参、从 metadata 取）| **T-A1-1** + A3 的拒绝用例（任务书 §3 A1 行原文）|
| T-A1-2 | `a_create_from_a_snapshot_is_a_new_execution_not_a_resume`<br>`src/orchestrator/tests.rs` | 从同一 snapshot 连开两个沙箱：两者 execution 互不相同；且**变体判定**走 `LaunchPlan::Create` | 把换代判定改成按 `LaunchMode` / 按 hook 类型判（F4 的坑）| **T-A1-2** |
| T-A1-3 | `a_snapshot_does_not_change_the_execution`<br>`src/orchestrator/tests.rs` | `capture_snapshot` 前后 `store.get().execution_id` 与 `backend.execution_id()` 都不变 | 在 `capture_snapshot_inner` 里铸一个新的 | **T-A1-3** |
| T-A1-4 | `forking_leaves_the_parent_execution_alone`<br>`src/orchestrator/tests.rs` | fork 前后父沙箱 execution 不变 | 同上，在 fork 父分支铸新的 | **T-A1-4** |
| T-A1-5 | `every_fork_child_gets_its_own_execution`<br>`src/orchestrator/tests.rs` | fork count=3：三个子 execution 两两不等，且都 ≠ 父 | 🔴 **删掉 `service.rs:701-706` 那行 `metadata.execution_id = child.execution_id`**（即回到"clone 继承父的"，§2.2.2(d) 那个坑）| **T-A1-5** |
| T-A1-6 | `an_auto_resume_reports_the_execution_it_actually_started`<br>`src/orchestrator/tests.rs`（配 `RecordingPublisher`，`tests.rs:4842-4867`）| 经 `try_auto_resume` 等价路径 resume 后，publisher 收到的 `mark_running` 携带的 execution == 新起来那次的 | 让 `mark_running` 用 `metadata` 读之前的旧值 / 传 `ExecutionId::new()` 现铸一个 | **T-A1-6** |
| T-A1-7 | `a_paused_record_carries_the_execution_that_produced_it`<br>`src/orchestrator/tests.rs` | pause 后 `PauseOutcome.metadata.execution_id` == 该次运行的 execution；重启（重建 orchestrator 从 persister 加载）后仍相等 | 在 pause 路径把 execution 清掉 / 改成 `Default` | **T-A1-7** |
| T-A1-8 | `the_stop_hook_names_the_execution_that_started`<br>`src/sandbox/custom_extension/client.rs`（= 改造后的 `client.rs:589`）| start 与 stop 的 `sandboxInstanceId` 逐字节相同，且等于外部传入的 execution | 让 guard 在 `start_*` 里自己现铸一个（= 回到 `SandboxInstanceId::new()`）| **T-A1-8** |
| T-A1-9 | `the_snapshot_a_sandbox_publishes_carries_no_execution`<br>`src/api/impls/paused_coordinator.rs` 或 `src/snapshot/` | `publish_metadata()` 产物的 JSON 里不含任何 execution 字段（F12 的性质要被钉住）| 把 `execution_id` 加进 `SnapshotPublishMetadata` | **T-A1-9** |

🟢 **T-A1-1..7 全部跑在 mock backend 上** ⇒ `cargo test -p agentenv --lib`，不需要 root / `/dev/kvm`，CI 可跑。
这是 §2.5 坚持"mock 要记下来"的兑现。

🟡 **集成层补一发**（`tests/integration/orchestrator.rs`，需 root）：
`T-A1-10 a_real_resume_changes_the_execution_end_to_end` —— 真 FC pause/resume 一轮，
读 `GET /sandboxes/{id}` 之外的内部状态确认换代。它不承担变异验证（跑不进 CI），只做一次落地确认。

### 4.2 A4

| # | 测试函数（建议名 / 位置） | 断言 | 🔴 变异 | 必须 FAIL 的测试 |
|---|---|---|---|---|
| T-A4-1 | `the_gate_covers_the_control_plane_router_and_nothing_merged_after_it`<br>`src/api/server.rs`（新建 `#[cfg(test)] mod tests`）| 把 router 装配抽成 `fn assemble(generated: Router, proxy: Router) -> Router`；测试传入一个**假的** generated router（两条 stand-in 路由）+ **真的** `proxy::router`：<br>① 假控制面路由无 token ⇒ 403；② `/proxy/xxx` 无 token ⇒ **不是 403**；③ 任意未匹配路径（fallback）无 token ⇒ **不是 403** | 把 `.layer(gate)` 挪到 `.merge(proxy)` **之后** | **T-A4-1**（②③ 转 FAIL）—— 🔴 这一发同时是"探针自证"：①必须先证明 gate 有分辨力，②③才有意义 |
| T-A4-2 | `a_call_without_the_control_plane_credential_is_refused`<br>同上 | 有效 token ⇒ 放行；空 header / 错 token / 大小写变体 ⇒ 403 | 把比较改成 presence-only（`!value.is_empty()`，= 今天 `auth.rs:15-55` 的语义）| **T-A4-2** |
| T-A4-3 | `an_empty_configured_token_lets_everything_through`<br>同上 | 配置为空 ⇒ 无 token 的控制面调用返回非 403（回退路径本身被测到）| 把空配置改成"一律拒" | **T-A4-3**（回退面失效会被立刻发现）|
| T-A4-4 | `health_is_never_gated`<br>同上 | 无 token 的 `GET /health` ⇒ 非 403 | 删掉 `/health` 豁免 | **T-A4-4**（等价于"Pod 起不来"的早期信号，F21）|
| T-A4-5 | `the_gateway_overwrites_a_client_supplied_control_plane_header`<br>`services/gateway/internal/server_test.go` | 客户端自带 `x-agentenv-control-plane: forged`：<br>① gateway 配了 token ⇒ 上游收到的是 gateway 的 token；<br>② gateway 未配 token ⇒ 上游收到的**没有**这个 header | ① 改成"没有才加"（`if Get()==""`）；② 改成"未配就不动" | **T-A4-5**（①②各自转 FAIL）—— 🔴 这是 §3.2 约束 1，漏了等于 A4 白做 |
| T-A4-6 | `the_prestop_drain_carries_the_control_plane_credential`<br>清单校验（`make` 里的 lint 或 `deploy/` 下一个 shell 断言）| `agentenv-daemonset.yaml` 的 preStop 两条 curl 都带 `x-agentenv-control-plane` header | 删掉 header | **T-A4-6** —— 🔴 因为 preStop 的失败是静默的（`\|\| echo … continuing`，`:145`），运行时抓不到 |
| 🔴 T-A4-7 | `the_cluster_list_fanout_is_never_gated`<br>同 `src/api/server.rs` 的 `mod tests`（**§3.3 裁决 D-6 早已点名要这一发，本表原先漏编号，2026-08-19 补**）| 无 token 的 `GET /sandboxes` ⇒ **非 403**，无 token 的 `GET /v2/sandboxes` ⇒ **非 403**；🔴 **对照面**：无 token 的 `POST /sandboxes` ⇒ **403** | ① 删掉 `GET /sandboxes` 豁免 ⇒ 前半 FAIL（= 集群列表整体 502 的早期信号）；② **把 `/sandboxes` 整条路径前缀豁免** ⇒ 后半 FAIL | **T-A4-7**（①②各自转 FAIL）—— 🔴 缺后半句，"前缀豁免"的变异会假绿 |
| 🔴 T-A4-8 | `the_gate_picks_up_a_token_written_after_startup`（**§3.2 的热读**）| 启动时 token 文件为空 ⇒ 调用非 403；**运行期**把 token 写进该文件 ⇒ 同一个 gate 实例开始 403；再清空 ⇒ 又非 403 | 把文件读成"启动时一次性加载" | **T-A4-8** —— 🔴 这一发是"步骤 9 不用滚 DaemonSet"的**唯一机械保证**；退化成启动期读取的症状是运维改了 Secret 却毫无变化，而那与"Secret 卷还没刷新"外观完全一致 |
| 🔴 T-A4-9 | `an_unreadable_token_file_keeps_the_last_known_value`（**§3.2 的三态**）| 先成功读到 token（gate 生效）→ 让文件读失败 ⇒ **仍然 403**（沿用上次成功值）+ `error_kept_previous` 计数 +1；而**文件存在且内容为空**才是显式关闭 ⇒ 非 403 | 把读失败降级成"视为空" | **T-A4-9** —— 🔴 变异后一次磁盘抖动就能把整个 A4 悄悄关掉 |

**集群验收探针（非单测，写进 runbook）**：

| 探针 | 做法 | 🔴 自证 |
|---|---|---|
| P-1「直连被拒」| `kubectl port-forward pod/<node-pod> 18000:8000` 后 `curl -X POST localhost:18000/sandboxes/{id}/pause` ⇒ 403 | **对照**：同一命令带上正确 token ⇒ 非 403。没有对照就分不清"403"与"路由不存在" |
| P-2「经 gateway 成功」| 经 30800 打同一个 pause ⇒ 200/204 | **对照**：临时把 gateway 的 token 改错 ⇒ 必须 403。不做这一步就分不清"gateway 注入生效"与"node 根本没开 gate" |
| P-3「数据面未受影响」| 经 `{port}-{sandboxID}.<domain>` 打一次沙箱内 HTTP ⇒ 正常 | **对照**：删掉沙箱后同一请求 ⇒ 404/502。证明探针确实打到了数据面 |
| 🔴 反面探针 | **不要用 30800 验"直连被拒"** —— 30800 落在 gateway（F16、§3.6 第 2 条），它永远会成功，看到 200 会被误读成"收窄失效" | |

---

## 5. 风险与未决项（✅ 2026-08-19 主 agent 已逐条裁决，见表后「裁决收口」）

| # | 事 | 我的倾向 | 为什么不自己定 |
|---|---|---|---|
| **O1** | 🔴 **auto-resume 跳过 `arbitrate_resume`**（F9）。A1 保证它写的是诚实的新 execution，但"该不该 resume"在这条路径上无人判。补在哪：(a) A3 的 `mark_running` SQL 谓词；(b) node 侧让 `try_auto_resume` 也走一遍仲裁 | **(a)** —— 事务内、单一裁判、与 A3 的形状一致；(b) 会把仲裁逻辑复制成两份 | 它跨 A1/A3 边界，且 (b) 会改变数据面 auto-resume 的延迟特征（多一次 RPC），是产品面取舍 |
| **O2** | 🟡 `SandboxMetadata.execution_id` **无 `#[serde(default)]`** ⇒ dev 节点上的存量 paused 记录加载失败。要不要为一次性升级加一个**只在这一版存在**的 default？ | **不加**。P1 允许丢 dev 存量；一个 serde default 就是永久 fail-open 分支（任务书 §0 P1 明写要禁）。改为发布前清空持久化目录 | 会让 dev 上正在用的 paused 沙箱消失，属于"删用户数据"，必须人裁 |
| **O3** | 🟡 `complete_pause` / `mark_local_only` / `release_claim` / `remove` 今天只带 `generation`。要不要**同批**都带上 execution？ | **同批带上**（作为 A3 的输入），否则"身份轴"只覆盖两条转换，另外四条仍只有版本轴 | 这是 A2「身份轴 vs 版本轴分工」的裁决内容（R3 的结论），不该由 node 侧设计文档定 |
| **O4** | 🟡 wire 字段名 `sandboxInstanceId` 与内部名 `execution` 不一致（§2.4）。改名要动 openapi.yml + codegen + 手工清孤儿 model | **不改名**，只在 openapi 的 `description` 里注明（description 不影响 generated 代码） | 若主 agent 认为命名一致性值那次 codegen，是个便宜的时机（F1：该功能零部署） |
| **O5** | 🟡 execution 不匹配时的错误码。今天有 D8 的 `Code::Aborted ⇒ GenerationConflict`。execution 冲突要复用它还是新开一个？ | **新开一个**：两轴分工的意义就在于"版本过期"和"化身被替换"是不同事实，合并成一个码，运维只能看到"冲突"却不知道工作区有没有易主 | 影响 proto 与 Go 侧映射，属 A3/B6 的接口面 |
| **O6** | 🟡 §3.1 的 pre-merge layer 方案依赖 **axum `Router::merge` 保留各自 layer** 这一语义。`T-A4-1` 会钉住它，但那是行为测试不是编译期保证；axum 大版本升级可能悄悄改变它 | 接受，靠 `T-A4-1` 钉。若主 agent 要更强保证，退路是拆端口（§3.1 已评估，代价是跨 proto 改动） | 是"强度 vs 范围"的取舍 |
| **O7** | 🟡 A4 的 gate 与 `resume_isolation_gate` 的相对顺序导致隔离节点上无 token 的 resume 拿到 503 而非 403（§3.3 末） | 接受现状（503 不泄露、不执行），只在验收 runbook 里写明前置条件 | 若认为"未鉴权请求不该先被业务逻辑处理"，就要把 A4 的 gate 提到最外层，那样又要在 gate 里判路径（§3.1 表格第二行明确反对的写法） |
| **O8** | 🟡 本设计**没有**给 A5（路由层拒旧 execution）预留 gateway → node 的 execution 传递通道 | 建议 A5 单独设计时复用 `LookupNode` 的应答携带 execution，node 侧在数据面 gate 里比对；A1 只保证"node 手里有本次化身的值" | 超出本文范围（A5 不在我这轮） |

### ✅ 裁决收口（2026-08-19 主 agent，逐条对应上表）

> 上表的问题陈述**一个字都不删**（它记录了"当初为什么是个问题"）；下表是最终裁决，**实现以下表为准**。

| # | ✅ 裁决 | 一句话理由 | 正文落点 |
|---|---|---|---|
| **O1** | **选 (a)：补在 A3 的 `mark_running` SQL 谓词里**；另加裁决 D-1 的结构性约束 —— `for_resume` 只消费 claim 造出的 token ⇒ auto-resume 也必须经过 claim | 事务内、单一裁判，不把仲裁逻辑复制成两份；token 那条是"起不来"，SQL 那条是"抢不走"，互补不重叠 | §2.2.1 裁决块、§2.6 末 |
| **O2** | **不加 `#[serde(default)]`**；与 scheduler 的 `DROP TABLE` **合并成同一个 runbook 步骤**；**加载失败必须响亮报错并指明处置命令** | serde default = 永久 fail-open 分支；两处"清干净"分开写必然只做一半；静默丢记录会伪装成"沙箱不见了" | §2.3 |
| **O3** | **本轮不加**（`complete_pause` / `mark_local_only` / `release_claim` / `remove` 保持 generation-only）；🔴 **但三条夺权路径（reclaim / releaseHoldings / releaseClaim）必须清空 execution** | generation CAS 对这四条已够；而不清空的话，`_design-phase3-scheduler.md` §1.4 那条"必然序列"**第一步就成立** | §2.9、`_design-phase3-scheduler.md` §3.4 |
| **O4** | **不改名**，openapi `description` 与 Rust 侧**各加一行注释**说明内部名与 wire 名的关系 | A1 的一个明确收益就是不动 **`custom_extension_api` 那份** openapi、不重新 codegen；改名把这收益吐回去。如无必要勿增实体。🔴 **不含 `src/api/openapi.yml`：A6 要动它**（§3.8.1）| §2.4、§2.8、§3.8.1 |
| **O5** | **新开一个**：`ErrExecutionFenced → codes.PermissionDenied → `PausedRegistryError::ExecutionFenced`，语义**永不重试** | 合并进 `Aborted` 会让节点重读拿新 generation 再发一次，**绕过 fencing** | §2.1、§2.9 |
| **O6** | **接受现状**（本轮中间件、不拆端口），但 `T-A4-1` 要**显式命名并加注释写清它守的不变式** | 它长得像"测框架行为"的冗余测试，不写清就会被后人清理掉，而它是 pre-merge 方案唯一的守卫 | §3.1、§4.2 |
| **O7** | **接受**，记为**已知偏差**（隔离节点上无 token 的 resume 得到 503 而非 403）| 不构成安全问题，只是诊断信息略差；把 gate 提到最外层就得在 gate 里再判路径，那是明确反对的写法 | §3.3 末 |
| **O8** | **通道已由 gateway 设计定死**，但**接收端是 node 的新增工作**：实现 `x-agentenv-expect-execution-id` 比对 + **412** 拒绝 + 始终回声 | 它正好落在两份设计的接缝上，不点名就会两边都以为是对方的活 | 🔴 **新增 §3.7** |

---

## 6. 与任务书 §6 顺序的对齐

> 🔴 **2026-08-19：任务书 §6 已整节重写成一条 13 步的线性总 runbook（命名裁决 N5）。**
> 本节只是「A1/A4 这两项自己的依赖方向」，**执行以
> [`_impl-plan-control-plane-phase3.md` §6](_impl-plan-control-plane-phase3.md#6-总发布-runbook-n5三份设计的顺序合成一条) 为准**。
> 🔴 **2026-08-19 追认：node 全程只滚一次。** 对应关系：
> **步骤 3 = node 唯一一次滚**，一次带上 A1 + A4 的 gate + A5 的接收端（§3.7）+ A6 的响应字段（§3.8）+ preStop 带头；
> A4 的 gateway 注入 = 步骤 8；**A4 的启用 = 步骤 9，是一次配置热翻转（写 token 文件），不滚 DaemonSet**；
> A5 的启用 = 步骤 10/11（gateway 侧开关）。
> 🟡 **为什么合并**：每滚一次 node 都会把该节点上所有非 Paused 沙箱 pause 掉（§3.5），而 pause 是不可逆点；
> 步骤 3 的滚发生在集群刚被清空之后（代价≈0），原步骤 9 的第二次滚发生在集群已用回满之后（真代价）。
> 🔴 **合并的前提是 token 可热读**（§3.2 配置形状表）：启用点若是 env，就要滚 DaemonSet，省下的风暴原样还回来。
> 完整论证见总 runbook [§6.1 的追认块](_impl-plan-control-plane-phase3.md#61-依赖图为什么是这个线性顺序)。

- A1 是 A2/A3 的**前置**：登记表 schema 要写 `execution_id NOT NULL`，得先有人能生成它。
  本设计里 A1 的**全部**改动都不依赖 A2/A3 落地 —— 新增字段先写进 `metadata_json`（F11 的 JSONB 是原样透传的，
  `central.rs:487-490` 那段注释明写"Nothing between this line and the JSONB column parses it"）⇒
  **A1 可以先合，服务端此时还不认这个字段，无害**。
- A4 与 A3 **并行开发、发布在 A3 之后**（任务书 §6.4）。本设计不与 A3 共享任何代码路径，
  唯一的交叉是 §3.5 那条"A4 不负责节点自主路径"的声明。
- 🔴 A1 落地**不放宽**「`running`/`resuming` 永不可抢」（任务书 §0）。本文没有任何一处触碰那条判定。
