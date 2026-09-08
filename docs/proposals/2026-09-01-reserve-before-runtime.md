# Reserve-before-runtime：占位先于运行时

**日期**：2026-09-01（方向稿，未实施）
**参照实现**：e2b-dev/infra `fdc33599b`（本地 `/home/debian/e2b-infra`，只读引用）
**触发**：孤儿 DELETE-500 的推断修法（warm 确定性 `NotFound` → `RuntimeConfirmedGone`）
被 Face 2 守卫（"a cluster nothing tells … must still refuse"）正确否决——registry
暖了只说明节点发现完成，不说明**这个沙箱**的 binding 已落库；刚创建、binding
未落的活沙箱会得到同样的 NotFound，按孤儿收割会误删活 runtime。区分"从未记录"
与"记录过且消失"在现有信号里做不到。
**前篇**：`2026-09-01-origin-as-hint-resume.md`（其 as-built 节记录了该修法的
实施与回退；本稿是它 DELETE 错误分类学遗留项的正解）。

## e2b 同位设计

e2b 让"从未记录"在结构上不可观测，于是不需要区分：

- **Reserve 先于一切启动**：`create_instance.go:196` 在向 orchestrator 发任何
  请求前先 `sandboxStore.Reserve`；`store.go:157` 返回 `finishStart`/`waitForStart`
  闭包对——成功 `finishStart(sbx, nil)` 落正身，失败 `finishStart(sbx, err)`
  撤占位；并发同 ID 撞 `ErrAlreadyExists` 则转为等待首个完成。
- **NotFound 全链是判决**：api store 不认识 → "already removed"，404 语义
  （`delete_instance.go:35-41`）；节点 RPC `codes.NotFound` → 幂等继续收尾
  （`:237`）。任何一层都不把"找不到"升级成 500。
- **反向安全网**：API 周期 list 节点上报 runtime，节点报而 API 不认的按 orphan
  杀（`sandboxes.go:633` 注释即为此约定）。

## 不变量与推论

**不变量：任何真实 runtime 必有先行的控制面记录。**
推论：占位不变量成立后，warm binding 视图的确定性 `NotFound` 就是"已消亡"的
判决——被回退的 DELETE 修法原样复活即安全。

## 设计

1. **create 占位**：api 半边选定节点后、发出节点 create RPC **前**，向 binding
   store 写占位绑定（状态 `Starting`、holder=选定节点、TTL≈create 超时预算）。
   节点创建成功 → 占位翻正为既有的正常绑定；失败/超时 → 撤销占位。api 副本
   中途崩死由 TTL 兜底。e2b 的预留同样落在 Redis 里
   （`packages/api/internal/sandbox/reservations/redis/`，`fdc33599b`：pending
   zset + Lua 原子脚本，`staleTTL = 90s` 专为"某个 api 副本创建途中崩死"清理
   pending 项）——占位存在 Redis 并以 TTL 兜底是与参照实现收敛，不是偏离。
2. **resume/重建路径已满足不变量**：pg 的 claim 行先于 restore 存在（claim CAS
   即该路径的 Reserve）——实施时核实并用测试钉住这一论断，而不是默认成立。
3. **并发同 ID create**：沙箱 ID 为服务端生成，该竞态理论不可达——核实 fork
   与全部创建入口后，若确认不可达则**不建** e2b 的 waitForStart 等待机制
   （比参照实现更简，理由记录于此）；若可达则按 e2b 语义建。
4. **DELETE 判决**：warm 确定性 `NotFound` → `RuntimeConfirmedGone`
   （`stub.rs`/`native_placement.rs` 那条被回退的链路复活）；同时对齐 e2b 的
   "每层 NotFound 幂等继续收尾"。
5. **Face 2 守卫语义反转（刻意契约变更）**：守卫从"cluster 没听说过的必须拒绝"
   改写为断言新不变量（活 runtime 必有先行记录 + 占位期 DELETE 不误删）。旧
   极性不得静默留存；新守卫要红→绿变异证据。

## D4-α：前任 origin 的幽灵记录（并入本轮）

复验实证（CLUSTER-VALIDATION.md 1796-2116）：origin 缺席期间发生 off-origin
重建后，**没有任何机制告知前任 origin 它已不是持有者**——它回来后用幸存的
`records/<id>.json` 恢复出幽灵 paused 沙箱（连重启都清不掉：记录有效，启动
清扫只删无记录的 artifacts），每次泄漏一份记录+artifacts+虚高 paused 计数，
理论无界。用户不可见（GET/DELETE/列表全走控制面，恒正确）。

e2b 同位答案是 reconciler 方向：节点持有态对账控制面真相。设计：

- 节点启动清扫扩展 + 周期对账：对每个本地 `records/<id>.json` 向控制面查询该
  沙箱的 paused 行；行不存在、或 holder ≠ 本节点 → 丢弃记录与 artifacts（复用
  既有 discard/orphan 机制）。α（记录幸存）与 β（记录已失）两变体同一机制覆盖。
- **fail-closed**：控制面不可达/不确定时不删，下一周期再试。
- 不新增 scheduler gRPC 面（阶段四裁决）；查询走既有 node↔api RPC 面扩展，
  具体形状实施时定并记录于此。

## 数据兼容与无过渡承诺（沿用前篇标准）

零迁移、零 schema 变更、零 feature flag、零双写回退腿。占位是 binding 值上的
新状态字段：Redis 值为 JSON，旧值缺省即"已确认"，向后兼容不构成迁移；语义
切换单批完成。回滚手段=镜像 digest。

## 验收判据（pve-mf）

1. **孤儿 DELETE**：复现上轮 F2 孤儿件 → DELETE 返回正确语义（非 500），
   pg/Redis/binding 三方清净。
2. **创建竞态负控（不变量的另一面）**：create 进行中（占位在、runtime 未落）
   的 DELETE 不得误删活沙箱——集群实测两面都要有。
3. **D4-α 幽灵**：复刻 B2 场景，对账机制在一个周期内清掉前任 origin 的记录、
   artifacts 与 paused 计数；β 变体同验。
4. 全套 e2e 基线不回归；指标系列身份零改动。
5. 顺带回补：D3b 备用路径（FailedPrecondition→OriginUnavailable）若能在集群
   构造则补实证。

## 依赖与顺序

栈于 `refactor/origin-as-hint-resume`@836432b（已集群复验 PASS）。dev 合并
裁决独立进行，不阻塞本稿实施。

## 实施纪要（as-built）

### 占位写点：前后对照

原先：`RemoteSandboxStub::start`（`src/node_client/stub.rs`）依次 `place_new`
→ `connect` → `client.create` → 校验 incarnation → `announce_placement`。绑定
写在**节点确认之后**，且是 best-effort（`record_placement` 失败只 warn，靠心跳
补写）。整条链路 `place_new → record_assignment → binding_store.record` 都在
api 进程内，`record_assignment` 只是 `NodeRegistryGrpcService` 上的一次进程内
调用，不走 socket。

现在：`place_new` 之后、`connect` 之前多一次 `reserve_placement`，且**硬失败**
——占位写不进去就不启动运行时。`connect`/`create` 失败与 incarnation 不符三条
臂都撤销占位；`announce_placement` 原地不动，成为占位的翻正。

### 方向稿没预见的一处：心跳对账会把占位删掉

`reconcile_node` 的既有语义是"节点花名册里没有的绑定就删"。占位期节点尚未
确认，花名册本来就没有它——不加围栏的话，创建途中的一次心跳就把刚写下的占位
抹掉，不变量当场失效。两个后端的对账都加了"`Starting` 只由 TTL 退休"这条围栏，
这是 `BindingState` 必须落在绑定值里、而不能另起一套键的真正原因。

### 与 e2b 的三处有据偏离

1. **不建 `waitForStart`**。沙箱 id 全部由服务端 `SandboxId::new()`（UUIDv7）
   铸造，REST 的 `NewSandbox`/`NewColdSandbox` 都不收 id，fork 子沙箱的 id 也
   由 api 半边铸造（`ForkChildren::Fresh`）——同 id 并发创建理论不可达。占位
   仍按 incarnation 仲裁，撞上就**拒绝而不是等待**，比参照实现简一层。
2. **占位与绑定同键**。e2b 的 pending zset 与 sandbox storage 是两套键空间；
   这里 DELETE 的判据读的正是绑定视图，占位若另起键空间就看不见，所以做成
   绑定值上的 `state` 字段。确认态不序列化该字段，旧值与新写的确认记录逐字节
   相同，Go 侧 `ParseRecord` 无感。
3. **`reserve` 硬失败**。e2b 的 Reserve 失败同样直接 500；这与 AgentENV 原先
   "记录是 best-effort"的取向相反，但正是这条把"找不到"变成判决的前提。

### 刻意未复用的既有机制

`src/orchestrator/store/redis/reserve.rs` 曾有一套 e2b 形状的预留实现
（`Reservation::Reserved/AlreadyPending/AlreadyInStorage` + `ReservationGuard`
+ `WaitForStart`），生产路径无调用方。它写在编排器元数据键空间（`agentenv:api`），
而 DELETE 的 500 出自绑定视图查询，元数据侧的预留改变不了 `absent_handle` 的
判断，故未复用。该残留已裁决删除（`chore/e2b-adjudication-closeout`）。

### 窗口内 DELETE 得 404：接受语义

集群实测：占位已写、节点尚未确认时对该 id 发 DELETE，得 404，而该沙箱随后
仍正常出现。**裁决为接受语义，不是缺陷。**

e2b 同位相同。`packages/api/internal/sandbox/store.go` 的 `Get` 只读 storage，
不读 reservations；kill 撞不存在的沙箱走 `delete_instance.go` 的 `ErrNotFound`
臂，记 "Sandbox not found, already removed" 并返回 `ErrSandboxNotFound`（404）。
占位期的沙箱在 storage 里尚不存在，所以 e2b 的 DELETE 对同一时刻同样答 404。

窗口有界，约等于一次 create 的时长：`announce_placement` 把占位翻正之后，
DELETE 立刻按正常绑定走。窗口内 DELETE 未能取消的那个 runtime 由沙箱超时回收
兜底，与任何其他未被显式删除的沙箱同路。

代价是这段窗口里 404 有两种含义（"没有这个沙箱"与"这个沙箱还没造出来"），
调用方无从区分。使其可区分需要 DELETE 读占位态并另答一个状态码——那正是
e2b 拒绝建的东西，它让 storage 成为删除路径的唯一判据，也是"NotFound 全链是
判决"这条不变量的前提。两者不能同时要。

### 不变量核实（W2）

- **claim 即 Reserve**：`cross_node_resume` 里 `restore_request(..., entry.execution_id)`
  消费 claim 已分配的 incarnation，重建在类型上就取不到一个不存在的 `entry`，
  顺序由编译期保证。行的另一面用穷举测试钉住：`paused/publishing/local_only/
  running/resuming` 五个状态 × origin 在场与否，warm 查询都不得答 `NotFound`。
- **并发同 id create**：见上"有据偏离 1"。不可达，故不建等待机制。

### DELETE 判据扩大的暴露面

绑定 TTL 与花名册新鲜度都是 30s，心跳间隔 5s。一台仅仅**静默**（尚未被发现
机制摘除）超过 30s 的节点，其运行中沙箱同样落进 warm 缺席，于是 DELETE 从
"500 拒绝"变成"收割记录"。这些沙箱此刻本就不可路由、网关对它们已答 404，改动
让 DELETE 与之一致；但 e2b 的反向安全网（节点上报而控制面不认的按 orphan 杀，
`sandboxes.go:633`）没有同步建，所以收割后节点上的 VM 无人回收。这是本轮已知
且刻意接受的代价，集群验收应实测。

### D4 对账的 RPC 形状

不新增 Scheduler 方法。`HeartbeatResponse` 增一个字段：

```proto
repeated string disowned_sandbox_ids = 2;
```

api 半边在 `heartbeat` 里，对本次心跳中 `paused=true` 的花名册项批量读
paused 注册表（`get_many` 的 `AnsweredRows`），逐条判定"行不存在"或
"`origin_node_id ≠ 本节点`"，把结论**显式点名**回给节点。fail-closed 是结构性的：
没有注册表、注册表非 cluster-backed、读失败、批次未覆盖该 id——四种情况都不点名，
节点听不到就什么都不删。节点永远不从自己观察到的缺席推导删除。

节点侧要求**连续两次**被点名才动手（`DisownedCandidates`）。理由：`begin_pause`
写行发生在节点已经把沙箱报成 paused 之后，中间落一次心跳就会看见"无行"，一次
点名与那个窗口不可区分；隔一整个心跳间隔的两次点名则不然。代价是每次真实回收
多等一个间隔。

**与方向稿的偏离**：方向稿写"启动清扫扩展 + 周期对账"，实施只做了后者。启动
时向控制面求证要么阻塞启动，要么在够不着控制面时按 fail-closed 什么都不删——
等价于不做；而第一次心跳本身就是启动后的第一次对账，一个间隔内即完成。α（记录
幸存）与 β（记录已失）两变体走同一条 `discard_local_paused_record`，差别只在
记录文件在不在，两侧都有测试。
