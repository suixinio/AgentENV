# Resume 重构方向：origin 从锁降为提示

**日期**：2026-09-01（方向稿，未实施）
**参照实现**：e2b-dev/infra `fdc33599b`（本地 `/home/debian/e2b-infra`，只读引用）
**触发**：pve-mf 上 `refactor/node-embedded-db-removal` 的滚动验证实证——已发布
（有效 `snapshot_id`、仓库有字节）的暂停沙箱在 origin 丢失本地 capture 后 resume
确定性 500（`node_client/wire.rs:54` "is not holding a paused capture"），与未发布者
无差别。发布不保护暂停沙箱。
**前篇**：`2026-08-31-node-embedded-db-removal.md`（终局方向节）、
`2026-08-30-unpublished-pause-fetchable.md`（其 S3 第 3 步以"origin 活着就能恢复"
为前提，该前提已被实证否定，本方向稿取代其恢复路径部分）。

## 缺陷的结构（现状）

暂停沙箱的 pg 行不只是记录，是**钉死在 origin 上的排他声明**：

- `arbitration()`（`paused_recovery.rs:844`）：`NotReady { origin_node_id }` 的语义
  是 "the only node that can serve this resume"；claimant 是 api 进程身份，
  `origin_node_id == node_id` 分支在集群部署里结构性不可达。
- `missing_local_verdict()`（同文件 :43）：本地无 capture 时只产出
  Wait/Busy/Unknown——**没有任何分支考虑"行已发布，可以重建"**。
- 从仓库快照重建的完整机器**已经存在**：`cross_node_resume`（同文件 :237 起，
  持已授 claim + `entry.snapshot_id` + `entry.metadata` 即可重建），但仲裁只在
  origin 死透/租约过期后放行它。
- 后果三件套（pve-mf 实证）：capture 缺失 → 500 而无降级；GET 恒 200 "paused"
  （谎报可恢复）；孤儿清理散落 pg 与 Redis 两处且 Redis 形态 DELETE 500。

## 参照：e2b 同位设计

信息模型相同——e2b 的快照行同样记 `OriginNodeID`（`sandbox_resume.go:304`）——
语义相反：

- **pause = 沙箱死亡**：`RemoveSandbox(StateActionPause)` 当场拆除路由与运行态
  记录，Postgres 快照行成为沙箱唯一持久身份，节点只剩暖缓存。
- **resume = 从快照建新沙箱，origin 只是放置提示**：`create_instance.go:372-380`，
  origin 缺席或 `!CanAcceptNewRequests()` → `node = nil` → 正常放置，数据从存储层拉。
- **提示自我纠正**：放置超时时 `maybeRemapResumeOriginNode` 把快照行的 origin
  改写为实际被暖的节点——字段跟随现实，不要求现实服从字段。
- 控制面不持有任何"节点 X 有你在别处拿不到的字节"的断言；唯一暴露窗口是
  上传完成前，由优雅关闭等待上传（uploadsWG）兜底。

## 目标模型与不变量

1. **持久真相 = 仓库字节 + 控制面行；节点本地 capture 一律是缓存**——对
   `snapshot_id` 已发布的行，origin 只承载"暖恢复更快"，不承载正确性。
2. **单活不变量的执法者是 pg 行的 claim CAS（generation/execution_id 围栏），
   不是 origin 亲和**。谁持 claim 谁恢复，CAS 保证同刻只有一个持有者——
   origin 亲和从互斥机制降级为放置偏好。
3. **origin 权威仅保留给数据确实只在 origin 的状态**（publishing / local_only）；
   这与 e2b 的上传窗口暴露同构，由前篇的终局方向（pause 即 publish）逐步收窄。
4. 状态与错误码说真话：不可恢复不报 paused；永久拒绝不报 500。

## 分阶段方向

### 阶段 A：capture 缺失时降级重建（最小修，消灭 500）

闸门改判据：从"origin 之死"改成"capture 之缺"。

- `missing_local_verdict` 增加一个分支：行状态为已发布（有 `snapshot_id`）、
  本地无 capture、且 claim 可得 → 产出"可重建"判决，接入**既有**的
  `cross_node_resume` 路径；先在 origin 节点自身降级（路由不变，仅数据源从
  本地 capture 变为仓库拉取），把 discard/硬死重启这类"origin 活着但 capture
  没了"的场景整个消灭。
- `node_client/wire.rs:54` 的 capture-missing 错误从终态改为可降级信号。
- publishing/local_only 行为不变（NotReady 依旧成立——数据确实只在 origin）。
- 仲裁测试必须用生产形状的两种身份（api 进程 id 作 claimant、机器名作
  origin），禁止 SELF 双扮（历史假绿根因）。

### 阶段 B：origin 成为可纠正的放置提示

- resume 放置：origin 健康则优先（暖缓存），不健康/不可调度则任意节点持
  claim 重建——对齐 e2b 的 `node = nil` 回落，不再等 90s 死亡判定。
- **提示自我纠正**：恢复落在节点 Y ≠ origin 时，行的 `origin_node_id` 改写为 Y
  （对齐 `maybeRemapResumeOriginNode`）。注意：改写 origin 的写入方会同时改变
  每个拿它与自身身份比较的读取方——按身份轴清单逐个核对消费方。
- 错误分类学：capture 永久不可得且行不可重建 → 4xx 语义（Gone/Conflict）；
  GET 对不可恢复行不再报可恢复的 paused；DELETE 能收割 Redis-only 孤儿形态
  （binding 在、pg 行不在）。
- 阶段 B 后，pve-mf 验证的"25h 暖位过期才可能解锁"的开放问题失效——不再依赖
  过期。

### 阶段 C：pause 即 publish（终局，收窄 origin 权威到上传窗口）

前篇终局方向的执行篇：暂停默认发布，`records/` 降级为上传窗口 staging，
publishing/local_only 收窄为"上传在途"。优雅关闭等待上传预算 + 超时回退本地
staging 双轨。阶段 C 后整个模型与 e2b 同构，origin 权威只剩上传窗口内的一段。
独立排期，不阻塞 A/B。

## 实施纪要（as-built，A/B 已实施）

三个实施前调研项的结论，以及与上文方向的偏离：

**capture 缺失有两个发现点，方向稿只写了一个。** 方向稿把降级点定位在
`node_client/wire.rs` 的 capture-missing 错误。实测该错误只在节点**整个不认识**
这个沙箱时产生（硬死重启：节点内存态为空 → `SandboxNotFound` → NOT_FOUND）。
另一形态——节点活着、内存态仍是 Paused、只有 `records/<id>.json` 没了
（discard、磁盘清理）——走的是持久化层：`mark_resuming` 读不到记录，节点
`orchestrator_status` 把它归到 `other => Status::internal`，api 半边无从分类，
仍是 500。故 A 阶段新增 `SandboxPersistenceError::RecordAbsent` 把这一缺席变成
类型，节点将其映射为 NOT_FOUND，判据 `paused_resume_warrants_rebuild` 同时认
本地与远端两种形状。**未新增任何 scheduler gRPC 面，也未新增 node RPC**：只是
既有 resume RPC 的一个状态码。

**阶段 B 的前两条在实施前已经成立。** 放置回落：`binding_store/lookup.rs` 对
`Paused` 行早已用 `select_node(prefer = origin)`，而 `select_node` 只在 origin
落在可调度集合内才优先，否则回落到 round-robin——即方向稿要的 `node = nil`
语义。提示自我纠正：`MARK_RUNNING_SQL` 的 `SET origin_node_id = $7`（holder）
每次 resume 都在改写 origin，契约测试已钉住。`claim_for_resume` 对已发布
parked 行也无租约门槛，所以"等 90s"从来不在 claim 上。真正残留的锁是
`node_client/stub.rs::reopen`：placement 选中的机器 ≠ 记录里的 origin 时它
`bail!` 出一个未分类错误，origin 地址解析不到时同样。B 阶段把这两处改成
`RemoteResumeFailure::OriginUnavailable`，判据放宽为"origin 无法服务此次
reopen"（含 `NodeUnreachable`），**排除 `Refused`**——节点对该沙箱有明确意见时
重建属于推翻判断而非绕过缺席。

**租约缺陷与 B 无交互，且原描述基本被证伪，不修。** `replica_renewal.rs` 有
10s 心跳驱动的续租回路（roster 驱动，`RENEW_LIVE_LEASE_SQL`），running 行还
需要"租约过期 AND `sandbox_expires_at` 已过"两个条件才被回收，健康沙箱不会被
回收。回收路径完全不读 `origin_node_id`，所以改写 origin 不改变可回收性。改写
origin 唯一的租约后果是**换了哪台机器的心跳在续这一行**（续租 SQL 以
`origin_node_id = v.node_id` 匹配），而 origin 恒被改写为实际 holder，与心跳
身份同源，故自洽。两处残留另记：`sandbox_expires_at` 无后台续期，只随离散
事件更新；`sandbox_expires_at IS NULL` 的 running 行在节点真死后永不回收
(`NULL < now()` 为 NULL)——这才是"25h 暖位"那个开放问题的真身，独立于 A/B。

## 刻意不照抄 e2b 的两处

1. **pause 不拆 binding**。e2b 的 `RemoveSandbox` 连路由一起拆，但 AgentENV 的
   autoResume 靠 gateway 对暂停沙箱的数据面路由触发唤醒（`resume_surface.rs`
   的 `wakes_on_traffic`/WakeSite），binding 是唤醒路由的载体。保留 binding，
   只废除"binding 存活 = capture 在场"的隐含推断。
2. **claim CAS 保留**。e2b 用 execution_id CAS + sandbox lock 防双活，我们的
   pg generation/execution 围栏是同构且已有的——阶段 A/B 复用它，不新造互斥。

## 数据兼容与无过渡承诺

**零数据迁移**。三个阶段只改既有字段的解释，不动 schema：A 新增的判决分支读
本来就在行里的 `snapshot_id`；B 改写既有列 `origin_node_id`；行指向已被 GC 快照
的脏数据由既有的 `CrossNodeResume` 缺失变体处理；旧行的 `execution_id` 缺省
回退已存在。存量 Redis 孤儿的清理是一次性运维补救，不产生兼容代码；B 的
DELETE-孤儿处理器是永久健壮性修复（该形态未来仍会因崩溃窗口产生）。
`origin_node_id` 语义从不可变出生地变为可变提示：数据不动，但全部读取方在
B 阶段**单批一次性对齐**，不允许两种语义并行的窗口期。

**无过渡代码**。A ⊂ B ⊂ C 为单调递进的终态子集，每阶段都是最终行为的真子集，
无双写、无回退腿、无 feature flag——回滚手段是镜像 digest（api 半边回滚约定）。
两条硬约束：①任何阶段不得引入 enable/disable 开关；②阶段 C 收窄状态机时，
因收窄而不可达的状态与分支同批删除——候选核对项：90 秒死亡倒带路径届时是否
只剩 publishing/local_only 一个服务对象；若 `local_only` 因 C 不可达则连状态
一起删。

## 验收判据

- **金判据**：pve-mf 上复刻这次失败的专项——已发布沙箱，人为清除 origin 的
  `records/<id>.json` 后 resume 必须成功（阶段 A：origin 自身从仓库重建；
  阶段 B：origin 下线时任意节点重建），内容与暂停前一致。
- local_only 沙箱在 origin 活着时行为不变（对照组）。
- 双活压测：并发 resume 同一沙箱 N 次，恢复实例恒为 1（CAS 生效）。
- 全套 e2e 基线不回归；`strategy=round_robin` 等指标形状零改动。
- 不新增 scheduler gRPC 面（阶段四裁决约束）；若 A/B 需要节点↔api 新交互，
  走既有 node RPC 面扩展并在本文件记录。

## 顺序与依赖

1. 本方向依赖 `refactor/node-embedded-db-removal` 先合入 dev（其 records/
   JSON 化是阶段 A 测试注入 capture 缺失的手段，也已把 discard 语义定型）。
2. A → B 可同分支连续做，C 独立提案排期。
3. 三个实施前调研项已结，结论见"实施纪要（as-built）"。
