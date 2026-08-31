# 节点去嵌入式 DB：移除 RocksDB / LocalKvStore

**日期**：2026-08-31（设计稿，未实施）
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
- `put` = 写 `.<name>.json.tmp` → 按 durability fsync → `rename` → 目录 fsync。
  `LocalStoreDurability` 三档映射：`Full` = fsync 文件+目录；`Wal` = fsync 文件；
  `Memory` = 不 fsync（测试）。
- 启动扫描忽略并清理 `.tmp` 残留（崩溃窗口 = 半个临时文件，从不损坏已有记录）。
- 文件名做保守编码（非 `[A-Za-z0-9._-]` 的 key 用 hash 命名、原 key 存 JSON 内）。

RocksDB 的 `close(timeout)`/`LocalKvCloseOutcome`/后台线程取消、
`close_shared_metadata_stores` 全部机制随之消失——文件没有后台工作。

### 消费方一：file_backed → `records/` JSON 目录

`records.db` → `records/<sandbox-id>.json`，内容即现有
`PersistedPausedRecord`（含 `version`，`ensure_supported_version` 原样保留）。
所有操作单记录，语义一比一。`cleanup_orphan_artifacts` 等 scrub 逻辑不变。

### 消费方二：P2P catalog → 纯内存 + 启动重公告

e2b 的 peer 路由态刻意短命（TTL + 上传完成即注销）：字节到达持久层后 P2P
状态没有存在价值。AgentENV 同构：publish 走 `P2pPublishMode::Reference`
（引用本地既有文件），key 可由 digest/路径重导出（`artifact.rs` 的
`layer_key_from_digest`/`extract_sha256_digest`），registry hint 已随节点
注销/过期丢弃，P2P 默认 Disabled 且 origin 永远在。目录是派生态：

- catalog 纯内存，不再有任何落盘；
- aenv-node 启动加重公告 pass：扫描本地已缓存 artifact，重新
  `publish(Reference)` + `RecordP2pArtifact`，温启动目录与 hint；
- `published_artifact_catalog_survives_transport_restart` 契约改为断言
  "重公告后可再服务"，或随语义一并删除；
- 落地前验证项：`unpublish` 的调用点是否总伴随字节删除——若存在"撤销发布
  但字节保留"的刻意状态，重公告会复活它，需在删除字节的路径上收敛。

### 消费方三：镜像缓存图 → 派生内存图 + 侧车文件

- **内存图**成为唯一运行时结构：启动时 `rebuild_from_configs`（已存在）建
  ref 族；hold 族全内存——`runtime`/`operation` 本来启动即清，`paused` 在启动时
  由暂停记录重导出（orchestrator 加载 `records/` 后对每条调用现有 `protect()`）。
  首轮删除性 GC 以"paused hold 重导出完成"为类型化硬前置，而非调用顺序约定；
  记录解码失败时该轮保持不删（沿用 `!reconciled` fail-closed）。
- **hard-commit 元数据**：优先全派生——记录本就由 config seed
  （`commit_store_hard_commits_from_config_paths`），digest 在文件名、size 用
  stat，`trusted_descriptor` 导入是 `cfg(test)`-only。仅当实现中发现确有
  config 拿不到的字段时才退回 `commits/<digest>.meta.json` 侧车。无记录的
  commit 文件维持现有 fail-closed 行为。
- **last-used**：全内存（e2b 的 diff cache 用 ttlcache 内存态 + TTL/磁盘压力
  双驱逐，零持久化）。驱逐顺序允许近似：启动以 config 文件 mtime 作冷启动
  种子，运行中在内存更新，不 touch 文件、不写盘。
- **schema/version** → 缓存根下 `layout-version` 文件。
- `write_batch` 的跨 key 原子性需求随"派生索引进内存"而消解：磁盘上不再存在
  需要一起变更的多个文件。

### 收尾

- 删 `src/local_store.rs` 的 RocksDB 实现、根 `Cargo.toml:79` 的 `rocksdb`、
  `[profile.*.package.{rocksdb,librocksdb-sys}]` 四段。
- `make check-crate-boundaries` 增加"全 workspace 无 rocksdb 依赖"守卫，
  锚定 `Cargo.toml` 依赖行语法而非裸子串，落地前出变异证据（红→绿）。
- CLAUDE.md 的 "Local RocksDB helper" 段改为：节点本地元数据用派生重建或
  `JsonRecordDir` 原子 JSON，不引入嵌入式 DB。

## 迁移（存量集群：dev-sg、pve-mf）

| 存储 | 策略 |
| --- | --- |
| `records.db` | **唯一不可丢**。独立迁移工具 `tools/rocksdb-migrate/`，**不入 workspace**（自带 lockfile，日常构建图零 rocksdb），读旧库写 `records/*.json`。新 `aenv-node` 启动见到旧目录即拒绝启动并给出可执行指引（与 `--setup-host` 的校验哲学一致）；无暂停沙箱的节点直接删目录。 |
| `catalog.db` | 弃置删除，无接替目录（纯内存 + 启动重公告）。registry hint 本就随节点注销/过期丢弃，残余脏 hint 由"lookup 失败换下一个 peer"消化。 |
| `graph.db` | 不迁移。启动 rebuild_from_configs + 由迁移后的暂停记录重建 paused hold；commit 侧车由 seed 路径首轮补齐。重建成功后删除旧目录。 |

否决的替代：在 aenv-node 里留一个 off-default 的 rocksdb 只读 feature 做原地
迁移——CI/开发构建图照样背上编译成本，违背本方案动机。

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

1. `JsonRecordDir` 原语 + `file_backed` 切换 + 迁移工具；
2. P2P catalog 内存化 + 启动重公告 pass（含 `unpublish` 语义验证）；
3. 镜像缓存图派生化（最大的一步，含 hold 内存化与暂停记录到 `protect()` 的
   启动接线）;
4. 删 rocksdb 依赖 + 边界守卫 + CLAUDE.md/配置文档收尾，随后按集群迁移
   runbook 滚动。

每步四门（fmt/clippy/test-unit/redis 合同套件）+ 第 3、4 步后 pve-mf 全套
e2e（117/8/0/14 基线)与 pause→滚动 node→resume 的专项验证。
