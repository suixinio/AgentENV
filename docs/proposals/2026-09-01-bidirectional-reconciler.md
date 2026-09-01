# 反向 reconciler：周期性双向全量对账

**日期**：2026-09-01（方向稿，立项占位，排期未定）
**优先级**：中低——见下"为什么不紧急"。
**参照实现**：e2b-dev/infra `fdc33599b`（本地 `/home/debian/e2b-infra`，只读引用）
**触发**：`2026-09-01-reserve-before-runtime.md` 的 as-built 节记下的两笔欠账：
DELETE 判据扩大后"收割记录、节点上 VM 无人回收"，以及 D4-α 幽灵记录只在
paused 一侧被点名。两笔都是同一个缺口的不同切面：控制面与节点的持有态没有
任何机制做全量互查。
**前篇**：`2026-09-01-reserve-before-runtime.md`（W4 的心跳点名是本家族第一个
成员）。

## e2b 同位设计

e2b 对这件事的答案是一个无聊的定时器，没有事件、没有推送、没有状态机。

`api/internal/orchestrator/cache.go:31` 的 `keepInSync`，函数注释写明用途
是 "to handle instances that died"：启动即跑一轮，之后按 `cacheSyncTime`
（同文件 `:23`，20s）定时。每轮对池中每个节点：

1. `nodemanager/sync.go:75` `GetOrphanCandidates` 向节点要它**实际在跑**的
   沙箱清单（节点的 `Sandbox.List` RPC）。
2. `sync.go:82` 把清单交给 `sandbox/store.go:140` 的 `Reconcile`——注释即为
   该约定："Redis is the source of truth — divergent sandboxes are orphans
   running on the node but not present in the store. Kill them."
3. 差集逐个 `KillOrphanSandbox`。

两条值得照抄的护栏，都在 `sandbox/storage/redis/main.go:142`：

- **grace period**：启动时间不足 `orphanGracePeriod` 的沙箱直接跳过，不进
  候选集——刚落地、store 还没看见的沙箱不是孤儿。
- **fail-closed 到整轮**：MGET 管道出错就整轮放弃（"skip entirely to avoid
  mass kills"），而不是逐条降级。判据读不到时不删，是这一族机制的通则。

**e2b 的这条回路只有一个方向**：节点报了而控制面不认的杀掉。另一个方向——
控制面有行而 runtime 已经不在——不由它覆盖，走独立的超时驱逐
（`orchestrator/evictor/evict.go:108` 的 `ExpiredItems`）。节点从池中摘除
（`client.go:106` `deregisterNode`）只动节点池，不动沙箱行。所以 e2b 的"双向"
是两套机制合起来的效果，不是一套机制的两条腿；本稿要建的比它多一点。

## 我们的现状

W4 的心跳点名（`HeartbeatResponse.disowned_sandbox_ids`）已经是这一族的第一个
成员，但覆盖面窄：

- **只覆盖 paused**：api 半边只对本次心跳里 `paused=true` 的花名册项查 paused
  注册表。running 沙箱不在判据内。
- **只有控制面→节点一个方向**：控制面告诉节点"这条你别留了"。节点报了而控制面
  三处存储（binding / paused 注册表 / 花名册）都不认的那一类，无人处理——正是
  e2b 的 orphan kill 覆盖的方向，也正是 reserve-before-runtime 的 as-built 记
  下"收割后节点上的 VM 无人回收"的那一类。

## 为什么不紧急（现有缓解）

集群实测得到的两条缓解，都不解决问题，但都把它压在低频：

- **liveness 探针约 60s 杀冻结节点**。节点进程冻死不会长期挂着一堆无人对账的
  runtime；Pod 被杀，VM 随宿主进程一起没了。
- **DELETE 对静默节点是等待而非收割**。节点只是静默（未被发现机制摘除）时，
  DELETE 走的是等待路径，不会一边收割控制面记录一边把 VM 留在机器上。

真正的敞口因此收窄成一句话：**节点永不回来**（宿主没了、Pod 被换掉、机器被
重装），而它上面的 runtime 状态与控制面的记录再也无法互相印证。

## 未覆盖场景

1. **节点永不回来**：它持有的 running 沙箱在控制面留下记录，无人收割；反过来，
   它本地 `records/<id>.json` 里的 paused 记录若随宿主一起消失，控制面的 paused
   行也再无对应字节——两侧都只能靠人。
2. **running 侧的 D4-α**：W4 只点名 paused。off-origin 重建后前任 origin 上
   幸存的 running 态残留不在点名范围内。
3. **占位期跑飞的 runtime**：窗口内 DELETE 得 404（见前篇 as-built），create
   随后成功，沙箱活到超时——超时兜底了它，但控制面从未确认过它的存在。

## 方向

把 W4 的单向点名扩为**周期性双向全量对账**：

- **判据三源互查**：binding 视图、paused 注册表、心跳花名册，与节点上报的实际
  持有清单做全量差集，两个方向都算。
- **节点报而控制面不认** → 按 orphan 收割（e2b 的方向）。
- **控制面有而节点不报** → 按已消亡处理（W4 点名的推广，从 paused 扩到 running）。
- **两条护栏照抄 e2b**：新沙箱有 grace period；任一判据源读失败则整轮放弃，
  不逐条降级。W4 已有的"连续两次点名才动手"是同一族护栏，保留。
- **不新增 Scheduler gRPC 面**（阶段四裁决）。载体是既有心跳的往返：节点已经
  在每次心跳里报花名册，控制面已经在响应里点名，双向对账要的是把两侧的集合
  都算全，而不是新开一条 RPC。

范围与顺序留待排期时定；本稿只立方向。

## 数据兼容与无过渡承诺（沿用前篇标准）

零迁移、零 schema 变更、零 feature flag、零双写回退腿。对账读的是已经存在的
三处判据与已经存在的心跳往返，不引入新的持久状态；点名字段
（`disowned_sandbox_ids`）已在 proto 里，扩大它的判据范围不改变线格式，旧节点
收到更长的列表照旧处理。回滚手段=镜像 digest。

硬约束：不得引入 enable/disable 开关。一个"对账开关"就是一条永久的回退腿，而
关掉它的集群与开着它的集群对同一份数据会给出不同的持有态判决。

## 依赖与顺序

栈于 `refactor/reserve-before-runtime`@a0511fb（已集群复验 PASS，已合入 dev）。
W4 的点名机制是本稿的既有基座，实施时扩写它而不是另起一套。
