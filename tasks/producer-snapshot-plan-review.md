<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# 审查记录：Producer 分布式快照过期实施计划

- 日期：2026-10-20；分支：`hotifx/producer-snapshot-retention`；审查基线：`f43ca2191`。
- 范围：[Spec](../SPEC-producer-distributed-snapshot-expiration.md)、[Plan](plan.md)、[任务清单](todo.md)。本次无生产代码修改。
- 方法：按用户指定的 planning-and-task-breakdown 编写计划，再按 code-review-and-quality 先检查验收设计、后做五维审查。主代理自审后，由独立审查代理（gpt-6-astra）只读复核并复审修订。
- 结论：**计划文档变更可接受；最终剩余 Critical 0、Required 0、Optional 0。** 该结论不代表功能实现、构建、集群正确性或性能验收通过。

## 已关闭的问题

| 问题 | 发现来源/类别 | 处理 |
|---|---|---|
| Spec 允许未来“证明安全后的原生重试”，Plan 首版明确禁止；maxFailures 约束口径不同 | 自审，需同步 | 首版仅只读准备可重试；原生异常不重试、不重投；maxFailures=1 为模式校验条件 |
| 旧类名、Spec 锚点及“进入计划前确认”与当前任务不符 | 自审，需同步 | 对齐当前类职责/链接，环境输入只阻塞对应集群验收 |
| 每表一 Job 未区分 `$options` 读取可能生成的准备 Job | 自审，需澄清 | 明确一次清理 Job/action，准备使用独立标识 |
| 固定时钟试验假设不符合原生直接调用 System.currentTimeMillis 的源码 | 自审，需修正 | 使用固定快照时间戳、远离时间保留边界、停止写入的 fixture，并记录试验窗口 |
| UNKNOWN 后原生 Future 可能继续持有对象和执行 I/O | 独立审查，Optional，已采纳 | 对象有界承诺限定 Producer 管理的 Driver 引用；表在途上限不代表异常后存储请求上限 |
| “每表保留100条”易误读为同时改变 Driver 轮次明细 | 独立审查，Optional，已采纳 | 明确仅 Executor 模式截断，Driver 保持既有行为；累计统计独立于明细 |

## 五维审查

| 维度 | 结论与后续证据要求 |
|---|---|
| 正确性 | 用户的无额外状态存储、一 Executor 多 Task、一 Task 一表、同 Task 多轮均保留；策略共用，UNKNOWN/计数/停止派发有明确测试。T05/T11/T12 必须证明 attempt 门禁与故障路径 |
| 可读性 | 14 项任务、5 个检查点；每项不超过3条验收标准和5个预计文件；依赖有序。若实施超过一个聚焦会话，先进一步拆分 |
| 架构 | core 只提供可选批执行扩展点，Paimon 编排/策略/原生逻辑留在 Paimon 包。旧 Driver 和两个构建 profile 保留 |
| 安全 | 版本/类来源、FileIO/权限、序列化和敏感异常脱敏均有门槛；不把单 JVM 成功泛化到真实认证环境 |
| 性能 | 重量级在途对象和轮次明细有界，Long 累计数独立；每表 Job 开销、遗留 I/O、共享池和大表长尾均纳入对照。当前没有收益数据 |

## 已完成的文档验证

- 读取当前 Producer 的策略、编排、结果模型、日志渲染及既有测试，未把现有串行测试误认作新分布式覆盖。
- 从对应版本源码确认：Paimon 0.9.0/1.3.2 均提供 Serializable Table、newExpireSnapshots 和四个 snapshot 配置 setter；Spark 3.2.1/3.5.4 均提供 Task/Stage attempt API。
- 区分 Producer 默认 Spark3.5.4/Paimon1.3.2 与旧 profile Spark3.2.1/Paimon0.9.0，不套用根 POM 的其他 Paimon 版本。
- 检查任务编号唯一、所有前置依赖在先、5个检查点、全部实现任务保持未勾选、每项验收与文件数符合拆分约定。
- 检查本地 Markdown 目标存在、Apache 文档头、空白及 Git diff；仅提交本任务文档。

## 未执行的验证

没有运行 Maven 构建/测试、变异测试、Spark 清理、YARN 故障注入或性能试验。本次没有新增实现条件可做变异，故未对无关生产代码做变异操作。实际构建兼容、shaded 版本识别、跨 JVM FileIO/认证、Spark 失败重放和性能仍由 T01–T14 的后续证据证明；所有实现任务保留未完成状态。
