# api 半边自持凭据值：PostgreSQL 是唯一的凭据存储

状态：三个决策点全部已裁决（§5.1 主密钥、§5.2 新鲜度、§5.3 隔离轴）。
P0–P4 已完成，回执见 §7 末尾的落地记录；P5（删除批）可以开始。
关联：`2026-09-03-sandbox-egress-credential-brokering.md`（v1/v1.1 已实现）、
`_egress-brokering-v1.1-implementation.md`

---

## 1. 问题与决定

`[secrets].backend` 今天有两个可用值，两个都要求值住在 AgentENV 之外：

- `vault`：值在 HashiCorp Vault 的 KV v2。要求部署并运维一套 Vault。`overlays/pve-mf`
  为此塞了一个 `vault-dev`（`server -dev`，存储全在内存），它每次 rollout 都会忘记
  policy、reader token 和写进去的每一个值。
- `external_resolver`：值留在运营方自己的服务里，broker 每次取值都向它发一次 HTTP。
  凭据一个字节都不进 AgentENV，代价是 broker 的取值路径挂在控制面之外的一个进程上。

两条路都成立，但都不满足"AgentENV 控制面自包含"这条纪律：一个要求引入第三方存储组件，
另一个把 broker 的在线依赖指向控制面之外。

**决定**：值加密后存进 aenv-api 已经在用的 PostgreSQL，aenv-api 自己实现 resolver 契约的
服务端。**这不是新增第三个后端，是用它替换现有的三选一** —— `vault` 与 `external_resolver`
连同它们的配置、部署清单、broker 侧的 Vault 凭据源一并移除，`[secrets].backend` 收敛为
`disabled | postgres`。

两条已裁决的边界：

- **§5.1 接受**：aenv-api 从"只写不读"变成"能解密"，主密钥在它的进程里。这是这条路的定价，
  不是可以绕开的实现细节。§5.1 给出把这个边界写进代码的做法。
- **移除是一次删除批，闸门是回执**：`vault` 路径今天是 pve-mf 上唯一被 e2e 跑过的路径。
  删除必须排在 PG 后端拿到同一组 e2e 回执之后（§7 的 P5），不能与切换同批。

---

## 2. 目标与约束

**目标**

- G1 只剩一条凭据存储路径。Vault 与 external_resolver 的代码、配置、部署清单、文档全部移除。
- G2 `/secrets` 与 brokered egress 的全部功能在新路径下不变。
- G3 值不进 microVM、不进 sandbox spec / 快照 / 模板的任何 JSON 字段、不进日志与 span。
- G4 值静态加密，密文与主密钥不在同一处。
- G5 默认仍是 `disabled`；不配主密钥的部署行为与今天完全一致。
- G6 数据库变更是加表，不改既有表。
- G7 租户数据库这类结构化凭据能通过公开面创建 —— 今天做不到，见 §3 末条。
- G8 删除批在回执之后，且可逐条对照（§7 P5 是一张清单，不是一次"清理"）。
- G9 "凭据的隔离轴是 control-plane 凭证"这条不变式是写下来的、有测试的、有 v2 接入点的，
  而不是从代码里推断出来的（§5.3）。

**约束**

- C1 `aenv-egress` 不许链数据库、`aenv-core`、字节半边或第二套 TLS 栈。这是
  `make check-crate-boundaries` 的门禁。**被禁的是 broker 直连 PG，不是 broker 调一个
  HTTP 接口。**
- C2 `secret_refs` 只存名字与版本号，建表注释写死了它不持有值。新表另立，不动它。
- C3 `Claims` 没有租户身份。grant 的 `names` 直接来自沙箱声明里引用的标记名
  （`src/sandbox/network/policy.rs:214`），AgentENV 不做归属校验。
- C4 aenv-api 是多副本、面向用户 REST 的那一半。它被攻陷的后果因 §5.1 而扩大。
- C5 `SecretsBackend` trait 保留。它的第二个角色是测试接缝 ——
  `InMemorySecretsBackend` 与 `NameOwningBackend`（`src/secrets/mod.rs:541,843`）
  让 `SecretsService` 不需要真实 PG 就能单测。删掉 trait 会把这些测试逼上数据库。
  留一个 trait 加一个生产实现，不是为了"以后可能还有别的后端"。

---

## 3. 参照事实

只列影响决定的，每条一处 file:line。

**后端契约。** `SecretsBackend` 五个方法：`put` / `delete` / `grant` / `revoke` /
`missing_names`（`src/secrets/mod.rs:115-140`）。`missing_names` 的默认实现返回 `None`，
把权威留给 ref 表；注释写着"a backend that owns the names as well as the values"可以返回
`Some` —— 值与名字同库时正是这种后端。

**装配点。** `crates/aenv-api/src/secrets/mod.rs:22-45`，一个三分支 match。

**配置枚举。** `SecretsBackendKind` 在 `src/cfg/egress_broker.rs:138-146`，
它的校验 match 在 `:240-250`。Rust 的穷尽性检查会点出这两处。

**broker 侧 resolver 契约。** `crates/aenv-egress/src/resolver.rs`：

- `RESOLVE_PATH = "credentials/resolve"`（`:18`），拼在配置的 base 之后。
- 请求 `POST` + `Authorization: Bearer <token>`，body `{sandboxId, executionId, name}`。
- 401 / 403 / 404 → `CredentialError::Denied`；其余非 2xx → `Unavailable`（`:79-88`）。
- `get` 读响应的 `value` 与 `allowedHosts`（`:106-115`）；`get_fields` 读 `fields` 对象，
  空对象是错误（`:118-137`）。
- 过期时间的键是 **`expiresAtUnix`**（`:93`），秒。不是 `expiresAt`。

**broker 侧缓存。** `CachingSource` 包住凭据源，TTL 由 `resolver.cache_ttl_secs` 决定
（`crates/aenv-egress/src/main.rs:293`）。撤销的生效延迟由它决定，见 §4.7。

**grant 的文档形状。** Vault 后端写 `grants/<execution_id>`，内容
`{sandbox_id, execution_id, names[]}`，整份替换（`crates/aenv-api/src/secrets/vault.rs:158-170`）；
revoke 按 `execution_id` 删，`sandbox_id` 不参与（`:172-176`）。broker 侧读同一份，校验
`sandbox_id` 相等且 `name` 在 `names` 里（`crates/aenv-egress/src/vault.rs:78-99`）。
这份形状即将随 Vault 一起删除，但它定义的语义要原样搬进 PG（§4.2）。

**api 服务器的接缝。** `compose` 在套 gate 之前 merge 一个
`extra_control_plane_routes: Router`（`src/api/server.rs:111`），而
`new_control_plane_only` 今天传的是空 Router（`:70`）。`/metrics` 就挂在这个层级之外
（`:116`）。新增一条不进 openapi 的内部路由有现成位置。

**路由门禁。** 生成路由 28 条，其中 25 条在 refused 一侧
（`src/api/role_gate.rs:172,196`）。任何进 openapi 的新路由都要改这两个断言。

**既有迁移。** `0002_secret_refs.sql:3-11`，`UNIQUE (name)` —— 名字是部署级全局唯一。

**broker 的 NetworkPolicy。** 出向只放 DNS、`vault` 命名空间的 8200、公网 443
（`deploy/k8s/base/aenv-egress-networkpolicy.yaml`）。到 aenv-api 的 8000 今天是**不通**的。

**六个部署 Secret。** `aenv-egress-secrets.example.yaml:100,111,122,131,139,148` 依次是
`egress-ca` / `egress-transport-ca` / `egress-server` / `egress-hmac` / `egress-vault` /
`secrets-vault-writer`。后两个是 Vault 专有。

**`--features resolver` 单独构建今天编译不过。** `resolver.rs:109` 调用
`crate::vault::allowed_hosts`，而 `vault` 是另一个 feature（`Cargo.toml` 的 `vault` /
`resolver` 各自独立，只有 `bin` 同时开）。默认构建是 `core`，两个都不开；`bin` 两个都开；
`resolver` 单开这个组合没有任何构建走过：

```
error[E0433]: cannot find `vault` in `crate`
   --> crates/aenv-egress/src/resolver.rs:109:44
note: found an item that was configured out — gated behind the `vault` feature
```

这是既有的潜在缺陷，删掉 `vault.rs` 时它会立刻变成真实的编译失败，所以是删除批的第一步
（§4.6）。

**公开面收不了结构化凭据。** `NewSecret` / `SecretUpdate` 只有 `value: SecretString`
（`src/api/openapi.yml:505-545`），而 broker 的 `get_fields` 要的是"除 `value` 与
`allowed_hosts` 之外的每个键"（`crates/aenv-egress/src/vault.rs:124-141`）。
**所以今天 `postgres` handler 要的凭据只能绕过 `/secrets`、由运营者直接 `vault kv put`
写进 Vault。** Vault 被移除后这扇侧门也没了，所以 §4.5 是必需项。

---

## 4. 方案

### 4.1 形状

三层不变。变的只有"值住在哪"和"broker 向谁要"。

```
agent-platform ──POST /secrets{name,value|fields,allowedHosts}──▶ aenv-api
                                                                    │ 加密
                                                                    ▼
                                                            PG: secret_values
                                                                secret_grants
                                                                secret_refs
沙箱创建/resume/fork ─────────────────────────────────────▶ aenv-api
                                                          GrantIssuer.grant()
                                                                    │
guest ──443/5432──▶ 运行时 listener ──TLS+身份头──▶ aenv-egress broker
                                                                    │
                              POST /internal/credentials/resolve    │
                              {sandboxId, executionId, name}  ◀─────┘
```

`aenv-api` 链 sqlx 是既有事实，不触碰 C1；broker 在这条路上仍然只说 HTTP。

**"broker 一行不改"只对新增那一半成立。** 把 base 指向 aenv-api 不需要动 broker；
但删掉 Vault 凭据源要动它（§4.6）。这两件事在 §7 里是不同批次。

### 4.2 数据模型（迁移 `0003_secret_values.sql`）

```sql
CREATE TABLE IF NOT EXISTS secret_values (
    name            TEXT        NOT NULL,
    version         BIGINT      NOT NULL,
    kind            TEXT        NOT NULL,          -- 'opaque' | 'fields'
    ciphertext      BYTEA       NOT NULL,
    nonce           BYTEA       NOT NULL,
    allowed_hosts   TEXT[]      NOT NULL DEFAULT '{}',
    created_at_ms   BIGINT      NOT NULL,
    PRIMARY KEY (name, version),
    CONSTRAINT secret_values_name_fk FOREIGN KEY (name)
        REFERENCES secret_refs(name) ON DELETE CASCADE,
    CONSTRAINT secret_values_kind CHECK (kind IN ('opaque', 'fields')),
    CONSTRAINT secret_values_version_positive CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS secret_grants (
    execution_id  TEXT        PRIMARY KEY,
    sandbox_id    TEXT        NOT NULL,
    names          TEXT[]     NOT NULL,
    granted_at_ms  BIGINT     NOT NULL
);

时间列用毫秒 `BIGINT`，与 `0001`/`0002` 既有的 `*_at_ms` 一致；外键显式命名，因为
`migrate.rs` 的约束黄金测试按名字断言。
```

`secret_grants` 的形状是**照抄即将被删掉的 Vault 文档形状**：一条记录对应一个 execution，
整份替换，按 `execution_id` 删、不看 `sandbox_id`。这不是怀旧，是因为 broker 侧的判定逻辑
（`sandbox_id` 相等且 `name` 在 `names` 里）不变，语义必须逐字段对齐 ——
否则删掉 Vault 的同时也悄悄改了授权语义，而现有 grant 测试无法分辨这两件事。

`allowed_hosts` 跟着版本走：broker 从取到值的同一处读到它的界。

### 4.3 加密

应用层 AES-256-GCM。主密钥 32 字节，base64 从 `[secrets.pg].key_file` 指的文件读。

- AAD 绑 `name || ":" || version`。防的是把 A 的密文行搬到 B 的名下 ——
  没有 AAD 的话数据库写权限就等于任意换值。
- 每条密文自带 12 字节随机 nonce，存在同一行。
- 不用 `pgcrypto`：密钥要出现在 SQL 文本里，于是进 `pg_stat_statements`、慢查询日志。
- 密钥缺失或不是 32 字节 → 后端拒绝装配，进程启动失败。

**主密钥不进 PG，密文不进 K8s Secret。** 这条写进迁移文件的注释。

### 4.4 内部 resolve 端点

`POST /internal/credentials/resolve`，通过 `extra_control_plane_routes` 挂载
（`src/api/server.rs:70` 今天传的空 Router 换成它）。

**它绝不能进 `src/api/openapi.yml`。** 进了就会被 `role_gate` 的 28/25 断言算进公开面、
生成进客户端、出现在用户能看到的 API 文档里。走 extra routes 是刻意的，注释里写明。

**认证。** `Authorization: Bearer <token>`，与 `[secrets.pg].resolver_token_file` 的内容做
常量时间比对。生成路由那套 API-key 认证不覆盖这条路径，必须自己做 ——
漏做就是一个无认证的取值端点。

**语义。**

1. 校验名字语法（拒绝含 `/` 的名字）与两个 id 非空。
2. 读 `secret_grants` 的 `execution_id` 行；不存在、`sandbox_id` 不等、`name` 不在
   `names` 里 → **404**。
3. 读该 `name` 的最大 `version`；不存在 → 404。
4. 解密，按 `kind` 组装响应。

**响应。**

```jsonc
// kind = 'opaque'
{"value": "sk-live-...", "allowedHosts": ["api.openai.com"]}
// kind = 'fields'
{"fields": {"host": "pg.internal", "port": 5432, "user": "app",
            "password": "...", "database": "app"}, "allowedHosts": []}
```

`expiresAtUnix` 本方案不发；到期由 grant 的撤销表达。

**失败映射必须精确。** 401/403/404 被 broker 读成 `Denied` → guest 收到合成的 403；
其余非 2xx 读成 `Unavailable` → 502。所以"没有 grant"必须是 404 而不是 500，
否则用户看到的是"上游挂了"而不是"你没有这个凭据"。

**卫生。** 响应带 `Cache-Control: no-store`；值全程 `SecretString` / `Zeroizing`；
span 上除 `name` 之外什么都不记，包括响应体长度。

### 4.5 结构化凭据：`/secrets` 必须能收 fields

Vault 被移除后，多字段凭据没有任何写入路径 —— 而给 PG 开一扇"运营者直接写表"的侧门等于
把加密和版本号的责任推给人手。所以 `NewSecret` 与 `SecretUpdate` 增加可选字段：

```yaml
fields:
  type: object
  description: >
    Structured credential for a handler that authenticates to the upstream
    itself (postgres). Mutually exclusive with value; each entry is a scalar.
  additionalProperties:
    $ref: "#/components/schemas/SecretString"
```

`value` 与 `fields` 二选一，两个都给或都不给是 400。`required` 从 `[name, value]` 放宽为
`[name]`，由实现校验二选一 —— OpenAPI 的 `oneOf` 在生成代码里会长出一个难用的枚举。

**这一步因为移除而变简单了**：原本要同时给 Vault 后端补 `put` 的多字段写入，现在没有第二个
后端需要对齐。

**这不是 e2b 的形状。** e2b 的 `/secrets` 只有不透明 `value`，因为它没有懂协议的 handler。
`fields` 与 `x-aenv-endpoints` 同源。

### 4.6 删除清单

删除批要逐条对照，不是一次"清理"。

**代码。**

| 位置 | 动作 |
|---|---|
| `crates/aenv-egress/src/credential.rs` | 先把 `allowed_hosts()` 从 `vault.rs:152` 搬过来（`pub(crate)`，ungated），修掉 §3 那个 `--features resolver` 编译失败。**这一步单独一个提交**，它本身就是缺陷修复 |
| `crates/aenv-egress/src/vault.rs` | 删除 |
| `crates/aenv-egress/Cargo.toml` | 删 `vault` feature；`bin` 不再列它 |
| `crates/aenv-egress/src/main.rs` | 删 `[vault]` 配置段、`:270-280` 的"两个都设了"警告分支、`:305-315` 的 Vault 装配；`:320` 的"两个都没设"错误改为只提 `resolver.url` |
| `crates/aenv-api/src/secrets/vault.rs` | 删除 |
| `crates/aenv-api/src/secrets/resolver.rs` | 删除 |
| `crates/aenv-api/src/secrets/mod.rs` | 装配 match 收敛为 `Disabled` / `Postgres` 两支 |
| `src/cfg/egress_broker.rs` | `SecretsBackendKind` 收敛为两值；删 `SecretsVaultConfig` / `SecretsResolverConfig`；`:240` 校验 match 随之收敛 |

**配置与文档。**

- `config/default.toml:181-205`：`[secrets]` 的注释重写，删 `[secrets.vault]` 与
  `[secrets.resolver]`，加 `[secrets.pg]`。
- `docs/src/configuration/reference.md`：同上。
- `docs/src/configuration/env-vars.md`：按仓库既有惯例，**为每个被删的环境变量留一条
  "设置它今天会发生什么"** —— `AENV_SECRETS_VAULT_*`、`AENV_EGRESS_VAULT_*`。
  这是这份文件存在的理由，不是可选的礼貌。
- `docs/src/concepts/egress-credentials.md`：重写存储那一节。
- `CLAUDE.md:23` 那句 "its TLS signer, Vault source and `http` handler exist only under
  `bin`" 要改（删掉 Vault source），以及 `aenv-egress` 那条 crate 描述。
  按 `CLAUDE.md` 自己的规矩：改写那句话，不要新增一段解释它没了。

**部署。**

- 删 `deploy/k8s/overlays/pve-mf/vault-dev.yaml`，以及 kustomization 里的
  `AENV_SECRETS_VAULT_ADDR`、`AENV_EGRESS_VAULT_ADDR` patch、vault-dev 的 NetworkPolicy patch、
  以及那段"reader token 要手工签发"的注释。
- `aenv-egress-networkpolicy.yaml`：删 `vault` 命名空间那条 egress，加到 `agentenv-api:8000`。
- `aenv-egress-secrets.example.yaml`：六个 Secret 变成 —— `egress-ca`、
  `egress-transport-ca`、`egress-server`、`egress-hmac` 四个不变；`egress-vault` 改名
  `egress-resolver`（内容从 Vault reader token 变成 broker 的 bearer，**必须改名**，
  否则同一个名字在两次部署间悄悄换了含义）；`secrets-vault-writer` 删除；
  新增 `agentenv-secrets-key`（主密钥）。净仍是六个。
  文件顶部那段 `vault policy write` 的 reader/writer 拆分说明整段删除。

**防回归守卫（可选，但如果做就要有变异证据）。** 仓库已有
`snapshot_catalog_manifest_test.go` 这类"扫 `deploy/k8s/base` 确认没有工作负载还声明被删变量"
的守卫。给 Vault 变量加一个同形的守卫是自然的，但按仓库的教训：**源码扫描类守卫的极性决定
它的安全含义，新守卫必须先证明它会因为一个故意的变异而变红**，否则它是一个假绿。

### 4.7 撤销与缓存

broker 用 `CachingSource` 缓存已解析的凭据，TTL 是 `resolver.cache_ttl_secs`。
所以 `revoke()` 删掉 grant 行之后，最坏还要等一个 TTL 才对新连接生效，**在途连接不切断**。

这与被删掉的 Vault 后端完全一样，不是新问题；但 revoke 现在是一条 `DELETE`、看起来像立即
生效，必须在文档里写清楚它不是。生产建议 `cache_ttl_secs` ≤ 60。

### 4.8 部署

- `secrets-store-config` 设 `AENV_SECRETS_BACKEND=postgres`；`[secrets.pg]` 的两个文件路径
  由 Deployment 挂载。
- broker 侧 `[resolver].url = http://agentenv-api:8000/internal`，
  `[resolver].token_file` 指向 `egress-resolver`。**broker 的配置段名 `[resolver]` 保留** ——
  它描述的是"向一个 HTTP 端点解析凭据"，这个描述在移除之后仍然准确，改名只会制造一次无收益的
  配置断裂。
- NetworkPolicy 两处（§4.6）。漏了到 `agentenv-api:8000` 那条，每次取值都超时而没有任何一处
  说明原因。

### 4.9 唤醒路径的存在性告警

`check_rule_secrets` 只挂在 `sandboxes_cold_post`、`sandboxes_post`、
`sandboxes_sandbox_id_network_put`（`src/api/impls/sandbox.rs:625,806,1231`），而四条 launch
路径全都会 `grant_secrets`。所以**唤醒一个 paused 沙箱不检查它引用的名字还在不在**：
grant 照发（grant 只写名字数组），失败只在 guest 侧变成合成 403，运营方看不到原因。

在 `launch_sandbox` 里加一次 `ensure_names_exist`，缺失就 `warn!` 带上 sandbox_id 与缺失的
名字，**不阻断唤醒** ——"因为 secret 被删了所以沙箱醒不来"比降级更糟。这是把这个缺口补成
可观测，不是补成拒绝。

这一项与后端无关，可独立于 P1–P5 合入。

---

## 5. 决策点

### 5.1 主密钥的位置 —— 已裁决：接受

Vault 后端下 aenv-api 是**只写不读**的。PG 后端要求它能解密才能回给 broker，于是主密钥必然
在 aenv-api 的进程里，**aenv-api 被攻陷 = 所有凭据泄漏**。而 aenv-api 是多副本、面向用户
REST 的那一半。这推翻了 `2026-09-03` 方案 §8 的"aenv-api 与 aenv-node 不落值"。

**裁决：接受。** 随之而来的实现约束（不是建议）：

- 主密钥只在 `postgres` 后端模块内读取和持有，不进 `AppConfig` 任何可 `Debug` 的字段。
- `/secrets` 的读路径（list / get）永不解密。**只有 `credentials/resolve` 这一条路径调用
  解密函数**，于是"谁能解密"是一处可审计的调用点，可以用一条测试钉住。
- 移除 Vault 之后，"aenv-api 不落值"这句话在仓库里不再成立。
  `2026-09-03` 方案 §8 的那一行要改写，而不是留着与代码矛盾。

### 5.2 值的新鲜度谁负责 —— 已裁决：写入方主动更新

静态凭据（LLM key）存进去很合适。租户 DSN 是 per-workspace 且会轮换的：走这条路，
写入方必须在轮换时主动 `POST /secrets/{secretID}` 推新版本。被移除的
`external_resolver` 恰恰是天然 always-fresh 的那条，所以这个代价是**移除决定本身买单的**。

**裁决：接受推送模型。新鲜度由写入方负责，AgentENV 不回源、不做过期检测、不发告警。**

随之而来的两条实现事实，写进 `/secrets` 的文档而不是留作口头约定：

- **新版本不需要重新签发 grant。** grant 记的是名字，`resolve` 取的是该名字的最大版本。
  推一个新版本之后，最坏一个 `resolver.cache_ttl_secs` 之内全部沙箱换到新值，
  在途连接不切换（§4.7）。
- **`GET /secrets/{secretID}` 回 `currentVersion` 与 `updatedAt`。** 写入方要自证没漏推，
  这两个字段是唯一依据。

### 5.3 隔离轴 —— 已裁决：写成不变式，不在 v1 造租户轴

`GrantIssuer::grant(sandbox_id, execution_id, names)` 的 `names` 直接来自沙箱声明里引用的
标记名，AgentENV 不校验"这个沙箱凭什么用 `tenant_db_ws99`"，而 `secret_refs.name` 是部署级
全局唯一。这条不变式必须写下来：

> 凭据的隔离轴是 control-plane 凭证。持有 API key 的任意主体可以引用任意 secret；
> `(sandbox_id, execution_id)` 约束的是"这次运行能读哪些"，不是"这个凭据归谁"。

**裁决：不在 v1 造租户轴（那需要 `Claims` 有身份），而是让"没有租户轴"这件事从隐含变成
显式、从静默变成有声。** 四件事：

1. **不变式进文档，并用一条正向测试钉住**：任意已存在的名字都能拿到 grant。它现在是绿的，
   将来有人加了归属检查它会红 —— 那正是更新不变式的时刻。不要写"断言检查不存在"的反向
   测试，那是假绿的温床。
2. **唤醒路径补一次存在性告警**（§4.9）。
3. **命名纪律进文档，不进代码**：名字自带来源前缀（`tenant_db_ws42`）。**不加校验** ——
   加了就是一道假边界。它的作用只是让越权在审计里一眼可见。
4. **在 `grant_secrets`（`src/orchestrator/service.rs:1394`）写一行注释点名 v2 的接入点**：
   `Claims` 有身份之后，归属检查加在这一个函数里，broker、store 与公开面都不动。

**不把 name 当秘密。** `GET /secrets` 没有作用域参数（`src/secrets/mod.rs:267`），任何持
API key 的主体都能列出全部名字，所以 `check_rule_secrets` 那条 400 里的名字回显**严格弱于
一个已经存在的端点**。把 name 当秘密需要同时废掉 list，而那是 e2b 也有的公开面 ——
结果会是用一道假边界换掉真诊断信息。错误消息与 `id_or_name` 的双寻址都保持原样。

e2b 敢把 name 叫 confidential selector，是因为在它那里 name 不是隔离机制，只是 project
scope 之上的一层纵深：它的 name 是 **project 内唯一**（`spec/openapi.yml:2261`），我们是
deployment 内唯一。抄标签不抄机制，比不抄更糟。

---

## 6. 否决的路

- **6.1 broker 直连 PG。** 违反 C1，且是 CI 门禁。broker 是 openssl 之上的叶子。
- **6.2 值写进 `secret_refs`。** 违反 C2；那张表的注释是契约的一部分。
- **6.3 值内联进创建请求（CubeSandbox 的形状）。** 值会跟着请求的每一份副本走 ——
  模板、快照、镜像任务、软删记录，再想收回来就没有边界。
- **6.4 `pgcrypto`。** 密钥进 SQL 文本，于是进日志与 `pg_stat_statements`。
- **6.5 把 resolve 端点放进 openapi。** 它会被算进公开面、生成进客户端、进用户文档。
- **6.6 复用 `/secrets` 的 API-key 认证给 resolve 端点。** broker 不是 API-key 的持有者，
  给它一把用户级 key 等于给它整个 REST 面。
- **6.7 删掉 `SecretsBackend` trait。** 只剩一个生产实现看起来该收敛，但 trait 的第二个角色
  是测试接缝（C5）。删了它，`SecretsService` 的单测全部要真实 PG。
- **6.8 移除批与切换批同批合入。** 违反 G8：Vault 是今天唯一有 e2e 回执的路径，
  在 PG 路径拿到同一组回执之前拆掉它，等于把回滚目标也一并删了。
- **6.9 把 broker 的 `[resolver]` 配置段改名。** 描述仍然准确，改名只制造一次配置断裂。
- **6.10 把 name 当秘密。** `GET /secrets` 无作用域，同一个主体已经能列出全部名字（§5.3）。
- **6.11 给 delete 加引用检查。** pause 走 `forget_sandbox` 并撤销 grant
  （`src/orchestrator/service.rs:1458`），所以基于 `secret_grants` 的检查只覆盖 running
  沙箱，漏掉全部 paused 沙箱 —— 而 paused 恰恰是冻着名字、活得最久、醒来无人看着的那一类。
  要覆盖它得全扫快照行里的 `PausedSandboxConfig`。**一个覆盖不全的守卫比没有守卫更坏**，
  因为它让 delete 看起来被保护了。保持现状：允许删除，运行时降级为 403（§4.9 让它可见）。
- **6.12 把策略里的 name 换成 secretID。** 公开面是 name（e2b 兼容），换 id 要动 grant 内容、
  resolve 契约、`allowedHosts` 查找与存量 paused 行的迁移。它防的"同名重建后语义漂移"在没有
  租户轴时本来就分不清"轮换"与"换租户"——那是 §5.3，不是一个独立问题。

---

## 7. 分期

**P0 不变式与告警（与后端无关，可先合入）**

§5.3 的四件事：不变式进文档、正向测试、命名纪律、`grant_secrets` 的 v2 接入点注释；
以及 §4.9 的唤醒期存在性告警。这一批不碰存储后端，也不依赖任何裁决之外的东西。

**P1 后端本体（无部署变更）**

`SecretsBackendKind` 加 `Postgres`（此时**先不删**另外两个）；迁移 `0003`；
`crates/aenv-api/src/secrets/pg_values.rs` 实现 `SecretsBackend` 五个方法
（`missing_names` 返回 `Some`）；加密模块与其单元测试（AAD 绑定、nonce 唯一、
错误密钥拒绝装配）。装配点加分支。`make test-with-postgres` 覆盖表行为。

**P2 resolve 端点**

`extra_control_plane_routes` 挂载；bearer 常量时间比对；四种失败的状态码映射；`no-store`。
断言：无 grant → 404、错 token → 401、`sandbox_id` 不匹配 → 404、名字含 `/` → 404、
响应体不出现在任何 span 上。**加一条跨后端等价测试**：同一组 grant/put 操作下，
`VaultKv2Backend` 与 PG 后端让 broker 的 `CredentialSource` 得到相同的接受/拒绝判定 ——
这条测试在 P5 随 Vault 一起删除，它的作用是**证明移除没有改变授权语义**。

**P3 结构化凭据**

`NewSecret` / `SecretUpdate` 的 `fields`；PG 后端支持它。
没有新增路由，`role_gate` 的 28 / 25 计数不变。

**P4 切换与回执**

pve-mf overlay 切到 `postgres` 后端，`vault-dev` **保留不动**（回滚目标）。
`15_egress_credentials` 与 postgres broker 两套 e2e 在新后端下跑通。
这两套今天跑在 Vault 后端上，是这个方案唯一的端到端证据。**P5 的闸门就是这两套的回执。**

**P5 删除批**

按 §4.6 的清单逐条执行。`allowed_hosts` 搬家那一步已经先做掉了（见下），所以
`cargo check -p aenv-egress --no-default-features --features resolver` 现在就通过，
而且 `make check-crate-boundaries` 会逐个 feature 单独构建，这条不会再退回去。

---

## 落地记录

按分期实现的提交，最新在下：

| 提交 | 内容 | 偏离方案之处 |
|---|---|---|
| `fix(egress): each credential source builds on its own` | §4.6 第一行的 `allowed_hosts` 搬家 | 提前到 P5 之前做，因为它是独立的缺陷修复；顺带给 `check-crate-boundaries` 加了逐 feature 构建守卫（有变异证据） |
| `feat(secrets): say what a grant does not bound…` | P0：§5.3 的不变式 + 正向测试 + v2 接入点注释，§4.9 的唤醒告警 | 存在性查询挂在 `GrantIssuer` 上（`unknown_names`，返回 `Option` 以区分"查不到"与"都在"），而不是在 `launch_sandbox` 里直接调 `SecretsService` —— 编排器只认识 `GrantIssuer` |
| `feat(secrets): a postgres credential store…` | P1 + P2 合并 | 合成一个提交：拆开的话两半各自都编译不过（`SecretsAssembly` 与 `new_control_plane_only` 的签名跨在两边）。`SecretValue` 提前到这一批引入，避免 P3 再改一次 `put` 的签名；顺手修了迁移错误信息里那条漏掉 `secret_refs` 的回滚命令 |
| `feat(secrets): /secrets can hold the structured credential…` | P3：`fields` | 还要改 `adev` 的脱敏补丁 —— 方案没预见到 `value` 变成可选会让它匹配不上，也没预见到 `fields` 的生成类型是会打印的 `models::SecretString` |
| `feat(deploy): the postgres credential store…` | P4 的清单部分 | e2e 回执还没拿到 |

### P4 回执（2026-09-04，pve-mf，镜像 `mf-egress-7`）

G8 的闸门已满足。

- 迁移 `0003` 在集群上应用：`catalog_schema_migrations` = `1,2,3`。
- **`15_egress_credentials`：All 23 tests passed (2 skipped)** —— 与切换前基线逐字相同，
  两条跳过是 guest 侧探针（模板里没有 `git`、`curl` 的 libcurl 不支持 `--http3`），与后端无关。
- **`16_egress_postgres`：All 9 tests passed，零跳过。** 这条路第一次完整跑通：凭据由套件
  自己通过 `/secrets` 的 `fields` 写入（§4.5 存在的理由），guest 用占位 DSN 连上，
  `current_user` 不是它写的那个，换一组占位落到同一账号，删除沙箱撤销 grant。
- 全量 16 套件：**135 PASS / 10 SKIP / 0 FAIL**。断言点 145 = 切换前 01–15 基线 141 + 套件 16 的 4。
  01–15 零回归。
- 存储侧留痕观察（写两个版本再查表）：`kind` 分别是 `fields` 与 `opaque`，nonce 各 12 字节，
  明文金丝雀不出现在 `ciphertext` 里，`allowed_hosts` 第一版是 `{pg.internal}`、第二版是 `{}`
  —— §4.2 说的"pin 跟着版本走"在真库上成立。

两点与本方案无关但在验收中暴露：

1. 套件 16 的凭据必须带 `sslmode`。handler 的 `upstream_tls` 默认开（对真租户库是对的默认），
   而验证用的上游是明文 postgres，缺这个字段时每次查询都是
   `ERR:28000:the upstream refused TLS`，**而套件把它报成 skip、整体仍然 exit 0**。
   已加 `E2E_PG_SSLMODE`。
2. pve-mf 上 `[handlers.postgres]` 的开关与 `allowed_cidrs`、以及 NetworkPolicy 的 5432 出向
   规则，是上一轮手工加的活体漂移，仓库里没有。`[handlers.*]` 无 env 绑定且 broker 配置是整文件
   generator，overlay 只能整份复制。本次 apply 采取"渲染 → 注入 → apply"避开它。
   **这条没修，是独立的一笔。**

集群上还留着 4 条 Vault 时代的 `secret_refs` 行（`e2e-egress-…`、`verify_tenant_db`、
`t7_db`、`t8_db`）：名字在、值不在新后端。引用它们的策略会被 400 挡下，不影响其它路径。

---

## 8. 不做的事

- 不改 broker 的凭据取值契约：`RESOLVE_PATH`、请求体、两种响应形状、状态码语义全部不动。
  P5 删的是另一个凭据源，不是这一个。
- 不动 `secret_refs`。
- 不做密钥轮换自动化。换主密钥是"读旧密钥解密全表、用新密钥重写"的一次性运维操作；
  `secret_values` 不加 `key_id` 列 —— 多密钥共存会让"谁能解密"不再是一处（§5.1）。
- 不给沙箱数据库身份。所有沙箱在上游审计里仍是凭据自己那个账号 —— 这是 brokered egress
  的既有边界，与值存在哪无关，e2b 的答案（`${e2b.identity.tokens.}`）我们没有跟。
- 不在 AgentENV 里做 workspace 归属校验（§5.3）；不把 name 当秘密（6.10）；
  不给 delete 加引用检查（6.11）；不把策略里的 name 换成 secretID（6.12）。
- 不在 AgentENV 侧做新鲜度告警或过期检测（§5.2 由写入方负责）。
- 不在 P5 之前删除任何 Vault 相关的东西（G8）。

---

## 9. 对照

| 轴 | `vault`（移除） | `external_resolver`（移除） | `postgres`（唯一保留） |
|---|---|---|---|
| 值住在哪 | Vault KV v2 | 运营方的服务 | aenv-api 的 PG，AES-256-GCM |
| 部署单元 | +1（Vault） | +0 | +0 |
| broker 的在线依赖 | Vault | 运营方的服务 | aenv-api |
| api 半边能否读回值 | 否 | 否 | **是**（§5.1，已接受） |
| 值的新鲜度 | 推送 | 回源，always-fresh | 推送（§5.2） |
| grant 检查在哪 | broker 进程内 | 运营方服务内 | **aenv-api 内** |
| 结构化凭据入口 | 绕过 `/secrets` 直写 Vault | 运营方服务 | `/secrets` 的 `fields`（§4.5） |
| 撤销生效 | 一个 cache TTL，不切在途 | 同 | 同 |

Vault 后端下 broker 的令牌读得到 mount 下每一个值，grant 只是 broker 进程内的应用层检查
（`crates/aenv-egress/src/vault.rs:78`）—— 这是原方案对抗审查里 S3 明确"未消除"的那一半。
移除之后 broker 手里只剩一个 bearer，被攻陷的 broker 从"整个存储"降到"它能说出三元组的
那些"。**这是附带收益，不构成闭合**：broker 在身份头里见过每一个活着的 execution。

同时要如实记下这次移除买走的东西：`external_resolver` 是三条路里唯一
**值不进 AgentENV** 的那条。移除它意味着"凭据留在运营方自己的服务里"不再是本仓库支持的
形态，§5.2 的新鲜度责任也因此没有退路。这是 §1 那条决定的一部分，不是它的副作用。
