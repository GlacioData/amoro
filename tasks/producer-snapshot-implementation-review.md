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

# Producer 分布式快照过期：实现审查

日期：2026-10-20；审查范围：本分支相对 `07f7791cc` 的 Producer 实现、测试和交付文档。使用用户指定的 `source-driven-development` 与先前指定的 `code-review-and-quality`；独立审查者为 gpt-6-astra，按技能要求与实现模型分开。审查者只读源码和官方版本源码，构建由主线程执行。

## 结论

最终复核：没有剩余 Critical / Required。该结论适用于本地实现；YARN、认证存储与生产性能仍未验收，不是上线批准。

## Required 闭环

| 问题 | 修复 | 回归证据 |
|---|---|---|
| A 表成功后 SparkContext 失效、B 未派发时，可能整体返回成功 | 未派发目标生成基础设施失败，已有成功结果不影响运行失败判定 | contextLossAfterSuccessStillFailsTheRun |
| cancelJobGroup 发生在 collect 真正提交之前，后到的清理 Job 可能继续启动 | 改为单次 collectAsync；同一内存监视器完成提交、FutureAction 登记、取消及永久关闭 | cancelBeforeSubmissionPreventsTheAction / cancellationDuringSubmissionCannotMissTheHandle / closeCancelsRegisteredJobsAndPermanentlyClosesSubmission |
| 过早恢复中断位使 FutureAction.cancel / JobGroup 的事件入队再次抛 InterruptedException，跳过 UNKNOWN 汇总与资源释放 | 协调器清位收尾后恢复；句柄和 group 共用逐项取消保护，单次失败不跳过其他取消，Listener 在 finally 释放 | interruptedCallerAndFailingCancelDoNotSkipRemainingHandles / handleInterruptDoesNotEscapeOrPreventSubsequentGroupCancellation / cancellationAfterSuccessCannotReportSuccessWithQueuedTargets |

取消保护只重发取消请求，绝不重试清理 action。取消返回仍不被解释成 FileIO 已全部停止。

## 五个审查维度

- 正确性：表策略优先、同 Task 全轮次、Task/Stage 门禁、UNKNOWN 和部分计数、别名去重、失败后派发停止；上述 Required 已修复。
- 可读性：保留解析器唯一；原生边界、任务循环、Driver 调度、Spark 提交、观测分别归入小文件；最大新增生产文件不足 300 行。
- 架构：批执行 SPI 为默认可选扩展，core 不引用 Paimon；Executor 不捕获 SparkSession/Catalog，不嵌套 Spark SQL。
- 安全：不将 FileIO options、完整异常消息或凭据写入新结果和日志；实际类来源不明则拒绝 Executor 模式。
- 性能：准备和清理共用在途上限；100 条明细上限与 Long 累计分离；没有实测整库收益，不宣称单张大表并行加速。

## 兼容测试修正复核

0.9 clean 矩阵中既有 1.3 压缩测试类错误地发送 `compact_strategy`，已仅为 `PaimonCompactIntegrationTest` 增加 profile 前置条件。整个 1.3 类在旧 profile 下跳过，含其辅助测试；`PaimonCompactLegacyIntegrationTest`、快照 Driver 和新快照纯单元测试仍执行。默认 1.3 profile 仍运行完整压缩类。独立复核认为该修正没有隐藏快照功能失败，无阻塞意见。

## 验证与限制

最终测试数、命令、构建产物和未完成验收见 [验证记录](producer-snapshot-validation-evidence.md)。三项变异均被测试捕获，恢复后的源码再跑完整矩阵。没有将计划审查结论当成实现或生产验收。

## 打包配置追加审查

直接加载 shaded 产物发现 Maven 版本元数据被父过滤删除，已修正 Producer 的 shade execution。独立审查逐项对照父子过滤器，确认唯一移除的排除规则是 META-INF/maven/**，签名过滤、其他规则和 transformer 保留，无新增 Critical / Required。最终两个产物的来源/版本门禁 smoke 见验证记录。
