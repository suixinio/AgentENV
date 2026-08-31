# 节点去嵌入式 DB：移除 RocksDB / LocalKvStore

**日期**：2026-08-31（已实施，本文按落地实现校准）
**参照实现**：e2b-dev/infra `fdc33599b`（本地 `/home/debian/e2b-infra`，只读引用，未复制代码）
**背景**：`docs/proposals/` 无前篇；起因是测试反复重编译 RocksDB 的构建成本分析。

## 目标与教义

E2B 的节点（orchestrator）不拥有任何嵌入式数据库：全部 go.mod 无
bbolt/badger/pebble/sqlite 直接依赖。其对应物的处理方式——

| E2B 位置 | 机制 |
| --- | --- |
| 沙箱恢复记录 | 不存在。重启即杀孤儿 firecracker（`pkg/startupreclaim/`），优雅关闭 = drain + 等快照上传；持久真相在 GCS + 控制面 Postgres |
| 模板/层缓存索引 | hash 寻址 JSON blob 走 `storage.StorageProvider`（`template/build/storage/cache/cache.go`），孤儿容忍到重启 |
| peer 制品路由 | Redis + TTL（`sandbox/template/peerclient/registry.go`），Redis 缺席时 nop |

教义归纳：**节点侧元数据要么可从磁盘布局/运行时重导出，要么是独立的原子
JSON 文件，要么上推控制面；不为它配备嵌入式 DB。**

本方案把 AgentENV 节点侧完全对齐这一教义：删除 `rocksdb` 依赖与
`src/local_store.rs` 的 KV 抽象。构建收益（C++ 工具链、bindgen、五个压缩库
从所有编译图消失）是副产品；主收益是每类状态的真相源变得显式。

## 现状盘点（操作面）

`rocksdb::` 只出现在 `src/local_store.rs`。三个消费方：

1. **`src/orchestrator/persistence/file_backed.rs`**（暂停沙箱恢复记录）
   key = sandbox id，value = `PersistedPausedRecord` JSON（自带 `version` 字段）。
   操作：`get`/`put`/`delete`/`entries`（启动全量加载）。**零批写、零前缀扫描。**
2. **`src/p2p/iroh/catalog.rs`**（已发布 P2P 制品目录）
   启动时 `fold` 进内存 HashMap，此后读全走内存；DB 仅为写穿副本
   （契约：`published_catalog_survives_transport_restart`）。操作：`put`/`delete`/`fold`。
3. **`crates/aenv-node/src/image/cache/graph.rs`**（镜像缓存元数据图）
   六个 key 族。唯一用到 `write_batch` 跨 key 原子性的地方，但逐族看真相源：
   - `ref/config-to-hard/*`：**已有 `rebuild_from_configs`**，真相源是 configs
     目录（`<id>-image.json` 经 overlaybd config loader 解析）；KV 是物化缓存。
   - `hold/*` + `ref/hold-to-hard/*` + `ref/hard-to-hold/*`：namespace 仅
     `runtime`/`operation`（瞬时，启动无条件清）与 `paused`（durable，owner 即
     暂停沙箱，pin 的 config 路径来自该沙箱的 artifacts）。durable 侧可从暂停
     记录重导出；双向索引是 hold 集合的纯派生索引。
   - `object/hard-commit/*`（digest/file/size）：由 config 路径 seed
     （`commit_store_hard_commits_from_config_paths`）；`trusted_descriptor`
     导入是 `#[cfg(test)]`-only。
   - `config-last-used/*`：驱逐时间戳，只随 ref 写入更新。
   - `schema/version`：单 key。
   且 GC 已是 fail-closed（候选逐个在 operation hold 下复查）+ 启动
   reconcile（`cleanup_stale_runtime_holds`、`reconcile_namespace`）。

`aenv-api` 对 `local_store` 零真实使用（仅 `lib.rs:7` 的一揽子 re-export）。

## 设计

### 新原语：`JsonRecordDir`（替换 `LocalKvStore`）

一个目录、每记录一个 `<name>.json` 文件的小模块（放 `src/record_dir.rs`）：

- `load_all()` / `get(name)` / `put(name, value)` / `remove(name)`；无批写、无前缀
  （命名空间用子目录）。
- `put` = 写 `<name>.json.tmp` → 按 durability fsync → `rename` → 目录 fsync。
  durability 由 `RecordDurability` 三档表达：`Full` = fsync 文件+目录；
  `File` = fsync 文件；`Memory` = 不 fsync（测试）。
- 启动扫描忽略并清理 `.tmp` 残留（崩溃窗口 = 半个临时文件，从不损坏已有记录）。
- 文件名用可逆的 percent 转义（非 `[A-Za-z0-9._-]` 的字节写成 `%XX`，`%` 自身
  也转义），因此 `load_all` 总能从目录列表还原 key，文件内容就是记录 JSON
  本身、不套信封。

RocksDB 的 `close(timeout)`/`LocalKvCloseOutcome`/后台线程取消、
`close_shared_metadata_stores` 全部机制随之消失——文件没有后台工作。

### 消费方一：file_backed → `records/` JSON 目录

`records.db` → `records/<sandbox-id>.json`，内容即现有
`PersistedPausedRecord`（含 `version`，`ensure_supported_version` 原样保留）。
所有操作单记录，语义一比一。`cleanup_orphan_artifacts` 等 scrub 逻辑不变。

### 消费方二：P2P catalog → 纯内存 + 惰性重公告

e2b 的 peer 路由态刻意短命（TTL + 上传完成即注销）：字节到达持久层后 P2P
状态没有存在价值。AgentENV 同构：registry hint 已随节点注销/过期丢弃，
P2P 默认 Disabled 且 origin 永远在。目录是派生态：

- catalog 纯内存，进程内有效，不再有任何落盘；
- 重公告是惰性的，走既有的两条事件路径：ublk daemon 下载完一层后调
  `/p2p-control/publish-layer`，快照提交后 `commit_and_advertise`。启动不做
  枚举 pass——节点侧没有可枚举的真相源：iroh 保留 tag 存的是 `sha256(key)`
  而非 key，descriptor 的 `LayerMetadata`（`fetch_byte_range` 依赖其 `size`）
  也不在 tag 里；快照那一侧 `aenv-node` 持 `NoSnapshotCatalog`，枚举"本机写过
  字节的快照"要么新增节点本地持久化（正是本方案要删的东西），要么新增
  scheduler gRPC 面（阶段四裁决禁止）。空窗期 lookup miss 回落 origin。
- `published_catalog_survives_transport_restart` 契约改为
  `a_restarted_transport_serves_nothing_until_it_republishes`，断言
  "重启后不谎称可服务、重公告后可再服务"；
- `unpublish` 语义验证结论：生产零调用方（仅 transport 自身测试调用），且其
  实现即删除保留 tag 并交给 GC 回收字节，因此不存在"撤销发布但字节保留"的
  刻意状态，重公告无复活风险。

### 消费方三：镜像缓存图 → 纯派生内存图

落地为完全派生：图只在内存里，磁盘上没有接替 `graph.db` 的任何文件——既无侧车
也无版本文件。

- **内存图**成为唯一运行时结构：每轮维护由 `rebuild_from_configs` 重建 ref 族；
  hold 族全内存——`runtime`/`operation` 本来启动即清，`paused` 在启动时
  由暂停记录重导出（`Orchestrator::new` 对每条恢复的暂停记录调用现有
  `protect()`，再 `reconcile_paused`）。删除性 GC 以"paused hold 重导出完成"
  为类型化硬前置：`run_maintenance` 需要一枚 `ReclaimAuthority`，而
  `reconcile_namespace(Paused, ..)` 是它唯一的来源；未拿到即 `bail`，不删。
  config 解析失败时整轮 rebuild 报错、该轮保持不删（fail-closed）。
- **hard-commit 元数据**：全派生，未退回侧车。两个来源——已发布 config 的
  `hard_refs`（digest/file/size 都在 config 里），以及 `indexes/` 里的转换索引
  （`scan_indexed_hard_commits`），后者让"已落 commit 但尚未发布 config"的字节
  也被记账。rebuild 对 hard-commit 事实只增不删，对 config ref 与 last-used
  则整体替换，因此磁盘上消失的 config 会同时失去引用与配额。
  `trusted_descriptor` 导入是 `cfg(test)`-only。无记录的 commit 文件维持现有
  fail-closed 行为。
- **last-used**：全内存（e2b 的 diff cache 用 ttlcache 内存态 + TTL/磁盘压力
  双驱逐，零持久化）。驱逐顺序允许近似：启动以 config 文件 mtime 作冷启动
  种子，运行中在内存更新，不 touch 文件、不写盘。
- **schema/version** 随之消失：磁盘上不再有需要版本化的图，旧 `metadata/`
  目录按迁移表弃置删除。
- `write_batch` 的跨 key 原子性需求随"派生索引进内存"而消解：磁盘上不再存在
  需要一起变更的多个文件。

### 收尾

- 删 `src/local_store.rs` 的 RocksDB 实现、根 `Cargo.toml:79` 的 `rocksdb`、
  `[profile.*.package.{rocksdb,librocksdb-sys}]` 四段。
- `make check-crate-boundaries` 增加"全 workspace 无 rocksdb 依赖"守卫。落地实现
  读的是 `cargo tree --workspace -e normal,build,dev` 解析出的依赖图而非
  `Cargo.toml` 文本，因此比锚定依赖行更强：注释里写 `rocksdb` 不会误报，而任何
  真实解析到 `rocksdb`/`librocksdb-sys` 的边都会红。变异证据（红→绿）见提交。
- CLAUDE.md 的 "Local RocksDB helper" 段改为：节点本地元数据用派生重建或
  `JsonRecordDir` 原子 JSON，不引入嵌入式 DB。

## 迁移（存量集群：dev-sg、pve-mf）

三个存储统一按弃置处理：`aenv-node` 启动见到旧目录即 warn 并尽力删除，随后
以空状态起步。

| 存储 | 策略 |
| --- | --- |
| `records.db` | 用户裁决可丢弃。启动 warn + 删除旧目录，以空 `records/` 起步。代价见下。 |
| `catalog.db` | 弃置删除，无接替目录（纯内存 + 惰性重公告）。registry hint 本就随节点注销/过期丢弃，残余脏 hint 由"lookup 失败换下一个 peer"消化。 |
| `graph.db` | 弃置删除。启动 rebuild_from_configs 重建 ref 族，paused hold 由暂停记录重导出，commit 元数据由 seed 路径首轮补齐。 |

丢弃 `records.db` 的代价是 rollout runbook 事项：滚动到本版本时，存量节点上的
暂停沙箱全部不可恢复，api 半边 paused registry 里对应的行随之成为孤儿，需要在
滚动窗口内一并清理。节点侧不自动清理远端数据库。

## 终局方向（对照 e2b，不在本次范围）

e2b 的优雅关闭等的是"上传完成"（uploadsWG），不是"本地持久化完成"——它没有
"暂停但未发布"这个长期状态。AgentENV 允许 local_only 暂停沙箱长期驻留节点，
这既是 `records/` 必须"不可丢"的原因，也是 split 上未发布行 409 不可恢复问题
的根源。终局方向：优雅关闭从"pause + 本地持久化"演进为"pause + publish 到
repository + commit row"，`records/` 降级为仅覆盖上传窗口的 staging，丢失
半径缩到窗口内。代价是关机路径受上传带宽约束，DaemonSet 滚动需要上传预算 +
超时回退本地 staging 的双轨，故作为独立提案另行裁决。本方案的 1:1 JSON 化
不改变现行为，且与该方向兼容（staging 记录沿用同一格式）。

## redb 何时才是"必须"（当前均未命中）

按用户要求明确列出：若未来命中以下任一条，节点侧引入 redb（纯 Rust、单文件、
事务、`Eventual`/`Immediate` 两档 durability 与现语义对齐）是正确工具，届时
`JsonRecordDir` 的调用面即插入点：

1. **高频 durable 写**：某类记录需要每秒多次且每次都落盘（file-per-write 的
   fsync+rename 成本线性），WAL 批量提交才划算。现状：暂停记录随 pause/resume
   事件，hold 进内存后热路径零磁盘写。
2. **记录数量级**：单目录超过 ~10⁵ 条时 readdir/inode 压力显著。现状：每节点
   10¹–10³。
3. **无法用 reconcile 抵偿的跨文件不变量**：出现"两个文件必须原子地一起变、
   且启动扫描无法判定哪边是真相"的新状态。现状：唯一的跨 key 批写（缓存图）
   被派生化消解，且 GC fail-closed 本来就承担不一致窗口。
4. **写入并发下的一致性快照遍历**：需要在写入进行中取全店 point-in-time
   迭代。现状：全量加载只在启动、写入前发生。

## 实施顺序

构建时长的即期缓解（rocksdb 裁 feature 只留 snappy、sccache/缓存卫生）与本
方案独立，可先行。本方案分四步，**编译收益在第 4 步才兑现**：

1. `JsonRecordDir` 原语 + `file_backed` 切换 + 旧目录弃置；
2. P2P catalog 内存化（含 `unpublish` 语义验证）；
3. 镜像缓存图派生化（最大的一步，含 hold 内存化与暂停记录到 `protect()` 的
   启动接线）;
4. 删 rocksdb 依赖 + 边界守卫 + CLAUDE.md/配置文档收尾，随后按集群迁移
   runbook 滚动。

每步四门（fmt/clippy/test-unit/redis 合同套件）+ 第 3、4 步后 pve-mf 全套
e2e（117/8/0/14 基线)与 pause→滚动 node→resume 的专项验证。
