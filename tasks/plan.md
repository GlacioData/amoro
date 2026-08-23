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

# amoro-ams-v2 Process 模拟调度实施计划

权威规格：[`amoro-ams-v2-process-spec.md`](amoro-ams-v2-process-spec.md)。本计划只覆盖单个
`amoro-ams-v2` Spring Boot 应用。

## 1. 范围锁定

本轮只验证通用 Process 控制面的完整流程：

`手工/定时创建 -> 持久化 -> 调度 -> 模拟提交/观测/取消 -> 终态 -> release -> TTL`

本轮明确不接入、不依赖、不加载、不执行任何 Iceberg/Paimon Action；不加载真实表，不修改
元数据或文件，不提交真实 Spark 作业，也不实现真实 Remote Spark 协议。Local 与 Remote 只提供
格式中立的 SPI 和显式启用的 simulator。Iceberg `expire-snapshots`、`clean-orphans` 与 Paimon
`sync-table-meta` 必须由后续独立 Action Spec 设计和实现。

## 2. 固定约束

- 单实例内同一 `(tableId, canonicalAction)` 最多一个非最终 Process；手工和定时入口共用同一
  `ProcessCreationService`，持久化 projection 再做 fail-closed 防线。
- DB 是事实源；内存索引、调度信号和 release 队列都必须可从 Process 行重建。
- 所有变更使用 `resourceVersion` CAS；未知写结果必须 fence，不能假定失败并重试副作用。
- 提交身份在 durable `DISPATCHING` 中冻结；UNKNOWN/LOST 只能 resolve，不能盲目重投。
- 终态先持久化，再 release execution handle；release 成功后才允许 TTL 删除。
- 默认 `simulation.enabled=false`，Engine/Action catalog 为空；模拟结果必须携带
  `simulated=true`。

## 3. 已执行阶段

1. **域模型与持久化一致性**：建立统一 invariant validator，修复 outcome-unknown repair，
   原子重建 canonical/index/hook/event。
2. **统一创建准入**：手工与 scheduled scanner 共用创建服务、幂等语义与单活跃约束；表身份通过
   一次原子 `TableCatalogPort.resolve` 冻结。
3. **Engine/Action SPI**：以 Java SPI 装配格式中立的 Engine、Action 与 LocalAction seam；按
   durable `(tableFormat, action, engine)` 精确选择，provider 启动失败和关闭都有界清理。
4. **状态机收敛**：覆盖 RUN/CANCEL、DISPATCHING、UNKNOWN/LOST、retry、late result、人工消解、
   重启恢复以及结果 CAS 重试。
5. **索引与后台维护**：使用结构共享有序索引支持分页、active repair、release reaper 和 TTL，
   所有循环均有 batch/cursor/timeout 上限。
6. **Spring 与 REST**：装配生命周期和显式模拟 profile；实现创建、查询、列表、取消、提交消解、
   执行消解接口与稳定错误映射。
7. **验证与终审**：执行并发、故障、重启、Local/Remote dummy 生命周期、Derby/MySQL E2E；并由
   多 Agent 分别复审状态机/持久化、SPI/生命周期和 REST/API。

## 4. 最终验收门槛

- JDK 17 模块全量非 Docker 测试通过，无静默跳过；
- Testcontainers MySQL 5.7 的存储与 Process 全生命周期测试通过；
- Spotless、Checkstyle 与 `git diff --check` 通过；
- 源码和依赖扫描证明没有 Iceberg/Paimon Action、真实表加载或真实 Spark 提交；
- 默认 Spring context 不注册模拟 pair，显式模拟 context 才能跑通全流程；
- `ARCHITECTURE.md`、README、Spec、Plan、Todo 对范围和实际行为描述一致；
- 仅提交 `amoro-ams-v2` 与对应任务文档，不纳入用户的其他工作树变更。

通过以上门槛只证明 **Process 编排与模拟流程** 可用，不代表任何真实湖格式维护能力已完成。
