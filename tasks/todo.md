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

# amoro-ams-v2 Process 模拟调度交付清单

规格：[`amoro-ams-v2-process-spec.md`](amoro-ams-v2-process-spec.md)；计划：
[`plan.md`](plan.md)。

> 硬边界：只用 `simulated` format、`dummy-maintenance` action 和 Local/Remote simulator 跑通
> 控制流程。不接入、不依赖、不加载、不执行任何 Iceberg/Paimon Action；不访问真实表或提交真实
> Spark 作业。真实格式能力必须另开 Spec。

## A. 域与持久化

- [x] 对 spec 冻结、desired 单调、attempt identity、history、condition 与 finality 做统一校验。
- [x] outcome-unknown repair 原子修复 canonical、aggregate indexes、hook 与补偿事件。
- [x] 第二个相同 `(tableId, canonicalAction)` 非最终资源在 projection 层 fail-closed。
- [x] postStart 从 durable Process 行重建索引、调度和 cleanup 状态。

## B. 创建与触发

- [x] 手工 REST 和 scheduled scanner 统一委托 `ProcessCreationService`。
- [x] 单实例并发准入保证同表同 action 最多一个活跃 Process。
- [x] `Idempotency-Key` replay、hash conflict、in-progress 与 outcome-unknown fence 行为已覆盖。
- [x] 表目录只提供原子 `resolve -> TableIdentity(tableId, tableFormat)` seam。
- [x] scanner 只读取显式 simulated facts，不包含真实格式探测条件。

## C. Engine/Action SPI

- [x] Java SPI 支持独立 Engine、Action 与 LocalAction provider，无需修改 Reconciler/REST。
- [x] capability catalog 只接受已装配的 `(tableFormat, action, engine)` 精确组合。
- [x] provider identity/重名/mode fail-fast，部分启动失败会关闭已创建 adapter。
- [x] Dispatcher flight、timeout、边界校验和所有 Engine 关闭路径均有界。
- [x] Local submission 在 JVM 生命周期内幂等；重启缺 ledger 的遗留提交收敛为 LOST。
- [x] 默认 context 的 Engine/Action catalog 为空；simulator 仅显式开启。

## D. 状态机与维护

- [x] RUN/CANCEL 覆盖 DISPATCHING、SUBMITTED、RUNNING、UNKNOWN、LOST、retry 与终态。
- [x] late/stale Engine result 按 attempt identity 丢弃或只更新合法观测，不回退 phase。
- [x] 提交/执行人工消解绑定 attempt/generation/requestHash/externalId。
- [x] 结果持久化重试、active rescheduler、release reaper 和 TTL 均有界且可恢复。
- [x] TTL 只删除已 final 且 handle cleanup 完成的行，时间比较统一使用 `Instant`。
- [x] ordered indexes 支持稳定 cursor/rank/batch，不在热路径全量复制排序。

## E. REST 与 Spring

- [x] 实现创建、点查、列表、取消、提交消解与执行消解 API。
- [x] 缺字段、未知字段、非法 engine/action、冲突与 outcome-unknown 使用稳定错误语义。
- [x] `IDEMPOTENCY_IN_PROGRESS` 返回 `Retry-After`。
- [x] Spring 启动顺序、postStart replay 和 shutdown 顺序受统一超时预算约束。
- [x] 端口 1640，与 v1 默认 1630 不冲突。

## F. 模拟边界

- [x] Local/Remote simulator 均走正式 SPI、Dispatcher、Reconciler 与持久化链路。
- [x] 模拟结果显式包含 `simulated=true`。
- [x] 无 Iceberg/Paimon Action 实现或注册，无真实表加载，无 HTTP/Spark 提交。
- [x] README、ARCHITECTURE、Spec 和归档旧计划明确此边界。

## G. 最终 release gate

- [x] 状态机/持久化、SPI/生命周期、REST/API 三路多 Agent 复审无剩余 P0–P2 findings。
- [x] JDK 17 模块全量非 Docker 测试通过：283 tests，0 failure/error/skipped。
- [x] Testcontainers MySQL 5.7 存储与 Process 生命周期测试通过：9 tests，0
  failure/error/skipped。
- [x] Spotless、Checkstyle、`git diff --check` 与禁止依赖/调用扫描通过。
- [x] 文档事实状态与最终测试数字同步。
- [x] 相关改动按层次提交到当前本地分支，不 push。

通过本清单不等于完成任何真实 Iceberg/Paimon 维护 Action；它只证明模拟 Process 调度框架可用。
