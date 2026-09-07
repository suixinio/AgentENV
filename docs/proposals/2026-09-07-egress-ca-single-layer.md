# 出口 CA 改为单层：broker 直接用根签叶

2026-09-07，用户裁决。

## 裁决

去掉每节点中间 CA。每个 `aenv-egress` broker 挂载 `egress-ca` Secret 的两个文件，用根直接签
拦截域名的叶证书；握手链上只有一张叶证书。根仍只经 node 的 `[egress_broker].guest_ca_cert_path`
推进 guest。api 半边不再持有任何 CA 材料：`POST /internal/egress/intermediate` 与
`crates/aenv-api/src/egress_ca/` 一并删除。

## 依据

2026-09-06 的边界裁决把出口代理的安全边界定在"值不进 guest"：节点可以持有凭据值，root 与节点
被攻破都不在威胁模型内。中间 CA 那一层（7 天寿命、`nameConstraints` 排除内网名字）只在"节点被
攻破"时有价值——它防的正是一个已被排除的威胁。留着它的代价不是运行成本，而是读代码的人会以为
节点被攻破在防护范围内。

参照 CubeSandbox：`CubeEgress/lua/cert_signer.lua` 用一把根直接签叶，
`deploy/one-click/scripts/systemd/cube-egress-prepare.sh` 让全集群共用同一把根私钥。

## 去掉后的两条运维差别

- **换根要重启沙箱。** 以前换中间证书对 guest 无感（guest 只认根）；现在根就是签名钥匙，换根
  意味着运行中的沙箱在下次 envd 初始化（重启或恢复）之前握手会失败。做法是先把新证书追加进
  `ca.crt` 并滚 node，等沙箱轮换完再换 `ca.key`。
- **api 不可达不再影响签名。** 以前 broker 起不来时取不到中间证书，规则域名一律 502
  `no-intermediate`；现在签名钥匙在 Pod 自己的挂载里，api 挂了只影响凭据 resolve。反过来，
  broker 缺 `ca.crt` 或 `ca.key` 从"降级"变成"拒启"——签不了的 broker 什么也做不了。
