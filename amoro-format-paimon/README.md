<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Paimon Tag 管控

Tag 通过公共多湖契约接入 AMS。Paimon 的能力声明、保留策略、观察标记和回读校验位于 `org.apache.amoro.formats.paimon.tag`。实际变更直接调用 Paimon 1.4.2 的 `FileStoreTable` API；其他湖格式保持默认不支持写操作。

## 使用条件

写入要求 Paimon 1.4.2、可刷新直连 Catalog、main 作用域、file/HDFS 路径和 `MANAGE_TABLE` 权限；默认 admin 账号已有该权限。Linux 与 macOS 使用相同 Java API，无须检查客户端 POSIX 或扩展属性。

| 配置 | 位置 | 要求 |
| --- | --- | --- |
| `write-only` | Paimon 表属性 | `true` |
| `tag.automatic-creation` | Paimon 表属性 | `none` |

Tag 写入不要求 `tag-management.exclusive-maintenance` 声明，该历史属性不再参与准入；`self-optimizing.enabled` 是否启用也不影响 Tag 写能力。Tag 操作不修改这些配置，也不暂停自优化任务。

`tag.time-expire-enabled` 保留 Paimon 1.4.2 原生默认值 `true`；缺省、显式 `true` 或 `false` 均不影响手动 Tag 写能力。AMS 不改写该属性，本功能不新增自动过期调度或调用 `TagTimeExpire`。按该属性执行自动过期的 Amoro 维护任务留待后续需求；外部写入和维护的既有原生行为保持不变，配置为 `true` 不表示 AMS 已实现自动清理。依据：[Paimon 配置](https://paimon.apache.org/docs/1.4/maintenance/configurations/#tagtime-expire-enabled)、[TagAutoManager](https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/tag/TagAutoManager.java)。

同一 AMS 进程的 Tag 请求使用表级串行准入。JVM 锁和观察检查不能协调外部程序或其他 AMS 实例的并发操作，也不承诺完整 CAS；并发历史清理及同名 Tag 变更遵循原生 API 的语义和限制。远程管理 Catalog、其他存储和未验证 SDK 版本不开放写入。

## 原生操作

| 操作 | 原生表 API | 业务校验 |
| --- | --- | --- |
| 新增 | `createTag(name, snapshotId, ttl)` | 明确 Snapshot、同名检查、完整 Snapshot 与策略回读 |
| 同表复制 | `createTag(newName, sourceSnapshotId, ttl)` | 源 Tag 观察标记、同名检查、源 Snapshot 与策略回读 |
| 生命周期调整 | `replaceTag(name, originalSnapshotId, ttl)` | 源观察标记、计时语义、完整 Snapshot 与策略回读 |
| 删除 | `deleteTag(name)` | 源观察标记、引用消失回读；历史清理结果单独表达 |

新增及复制拒绝已存在的同名 Tag；原生存在检查与发布并非跨写者原子事务。复制只增加引用，不复制数据。原生 API 能从 Tag 找回已过期的普通 Snapshot。

生命周期调整始终传原 Tag 的 Snapshot ID，保持名称、作用域和完整 Snapshot。有限时长从修改时重新计时，相同时长表示续期；此操作不执行历史清理。

`NATIVE_DEFAULT` 采用当前 `tag.default-time-retained`；`AGE_LIMIT` 指定正的 JSON 安全整数毫秒；`NO_AGE_LIMIT` 显式传入 null TTL，不设置该年龄上限，不表示永久保留。复制、删除和更新需源观察标记；采用表默认还需默认配置标记。缺失或变化时要求刷新并重新确认。

页面依据能力展示操作。Snapshot ID 打开保留的 Snapshot 详情，复制、删除和调整生命周期均需确认。成功回执须匹配目标身份、作用域和实际策略；未知写结果与读取错误分别显示，不自动重发写请求。

## 内核边界与失败处理

文件发布、文件系统访问、回调及历史清理全部由 SDK 执行；Amoro 不包装 FileIO、不创建存储锁标记、不重写元数据、不接管回调。文件权限和发布原子性遵循实际 Catalog 的原生 FileIO，不额外保证属性保留或原子替换。SDK 在不能使用原子 rename 时可能回退为覆盖写入，失败可能留下不完整 Tag 元数据。

原生删除可能立即同步触发历史清理，其遍历与耗时由 SDK 决定。SDK 的 `deleteQuietly` 可能吞掉引用删除错误并继续清理；Amoro 回读引用仍存在时返回 `UNKNOWN`，不能声称已停止内核清理。回调通知失败会使 SDK 抛错；SDK 安静关闭回调时可能只记录关闭错误，业务层无法据此证明所有回调成功。

前置校验或读取失败返回 `NOT_APPLIED`；进入 SDK 后抛错返回 `UNKNOWN`；SDK 已返回但结果回读失败或不符合请求返回 `APPLIED_WITH_ERRORS`。这些状态不承诺还原 SDK 内部各个步骤；响应丢失不能证明未写入，回读也不能证明回调或历史清理全部完成。不自动重试或补偿。

原生语义依据：[AbstractFileStoreTable](https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java)、[TagManager](https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java)、[HadoopFileIO](https://github.com/apache/paimon/blob/release-1.4.2/paimon-common/src/main/java/org/apache/paimon/fs/hadoop/HadoopFileIO.java)。
