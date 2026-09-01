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
   中途崩死由 TTL 兜底（e2b 的 reservation 在内存里随进程消失，我们在 Redis，
   TTL 是等价物）。
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
