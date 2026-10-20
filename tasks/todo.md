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

# 任务清单：Producer 分布式快照过期

状态：仅完成计划编写；以下实现/验证任务全部未执行。计划和决策见 [plan.md](plan.md)，A01–A20 对应 [Spec 第 10 节](../SPEC-producer-distributed-snapshot-expiration.md#10-验收与测试策略)。

路径约定（文件表中均为仓库相对路径）：
- `P/` = `amoro-producer/src/main/scala/org/apache/amoro/producer/paimon/`
- `C/` = `amoro-producer/src/main/scala/org/apache/amoro/producer/core/`
- `S/` = `amoro-producer/src/main/scala/org/apache/amoro/producer/spi/`
- `PT/` = `amoro-producer/src/test/scala/org/apache/amoro/producer/paimon/`
- `CT/` = `amoro-producer/src/test/scala/org/apache/amoro/producer/core/`

验证约定：各项列出的测试类实施后必须实际存在并在 Surefire XML 中有执行记录。单项命令为 `./mvnw -pl amoro-producer test -Pproducer-paimon-1.3.2 -Dtest=<该项测试类>`；首次及切换 profile 必须加 `clean`。检查点列出完整组合。新增类用 JUnit 5；真实集群试验单独记录，不能标成 Maven 自动测试已覆盖。

## T01：复用唯一保留策略解析器

- [ ] 完成 T01（M，3 个文件；依赖：无；覆盖 A01/A02/A03）。
- 内容：将现有 resolveConfig 迁入 PaimonSnapshotRetention，Driver Action 和新执行路径使用同一不可变最终策略；本任务不增加新执行入口。
- 验收：
  - [ ] 显式表 min/max/limit 优先；时间按表 → procedureOptions → 版本默认；完整/部分/缺失/非法配置行为不变。
  - [ ] 表 maxDeletes=1、CLI=550 的用例仍依据 1 续轮，旧时区及无时间策略保持。
  - [ ] 无重复解析器或静默回退；既有 Action 测试证明 Driver SQL/重试不变。
- 文件：`P/PaimonSnapshotRetention.scala`（新）、`P/PaimonExpireSnapshotsAction.scala`、`PT/PaimonExpireSnapshotsActionTest.scala`。
- 验证类：`PaimonExpireSnapshotsActionTest,PaimonExpireSnapshotsIntegrationTest`。

## T02：验证原生 API 与 SQL 语义等价

- [ ] 完成 T02（M，3 个文件；依赖 T01；覆盖 A02/A07/A15）。
- 内容：新增唯一的原生单轮调用边界及临时文件系统 fixture；共用最终策略，对真实 Paimon 表运行原生清理。只供后续 Task 使用，不切换用户入口。
- 验收：
  - [ ] 固定快照时间戳、远离保留时间边界且相同初始状态下，原生与 SQL 保留快照集合、返回数和存活数据一致，保留 runtime options 与 Catalog 生效属性。
  - [ ] 不额外覆盖 changelog defaults；1.3.2 和 0.9.0 两组合均能编译，0.9 Driver 回归通过。
  - [ ] 原生对象在调用 JVM 内创建；异常向上返回，不在该层重试删除。
- 文件：`P/PaimonSnapshotNativeExpire.scala`（新）、`PT/PaimonSnapshotNativeExpireTest.scala`（新）、`PT/PaimonExpireSnapshotsIntegrationTest.scala`。
- 验证：1.3.2 跑 `PaimonSnapshotNativeExpireTest,PaimonExpireSnapshotsIntegrationTest`；0.9.0 跑现有 `PaimonExpireSnapshotsActionTest,PaimonExpireSnapshotsIntegrationTest`，同时编译全部新增源码。1.3.2 专属用例在旧 profile 的跳过须记录原因和数量。

### C1：原生路线可行

- [ ] `./mvnw -pl amoro-producer clean test -Pproducer-paimon-1.3.2 -Dtest=PaimonSnapshotNativeExpireTest,PaimonExpireSnapshotsActionTest,PaimonExpireSnapshotsIntegrationTest`
- [ ] `./mvnw -pl amoro-producer clean test -Pproducer-paimon-0.9.0 -Dtest=PaimonExpireSnapshotsActionTest,PaimonExpireSnapshotsIntegrationTest`
- [ ] 核对实际测试报告和原生/SQL 对照证据；发现语义差异先修复，不进入调度实现。

## T03：建立可信运行组合校验

- [ ] 完成 T03（M，最多 4 个文件；依赖 T02；覆盖 A15/A17）。
- 内容：实现 Driver/Task 可复用的运行校验，实际来源不明时拒绝 Executor 模式；保留旧 Driver。
- 验收：
  - [ ] 识别真实 Spark/Paimon 版本、CodeSource 和必要 API，覆盖缺失、多版本、shaded 资源来源不明确；不能只凭 Procedure options 能力放行。
  - [ ] Executor 仅允许 3.5.4/1.3.2、speculation=false、maxFailures=1；Driver 默认兼容不变。
  - [ ] 默认/旧 profile 均编译；测试当前实际 fat JAR 中的版本资源，并证明 Driver 和 Task 可独立校验。
- 文件：`P/PaimonSnapshotRuntime.scala`（新）、`PT/PaimonSnapshotRuntimeTest.scala`（新）、必要时 `amoro-producer/pom.xml`、`amoro-producer/src/main/resources/producer-build.properties`（新，仅构建来源标识，不替代实际类版本）。
- 验证类：`PaimonSnapshotRuntimeTest`；分别打包两个 profile（`clean package -DskipTests`），检查产物来源，不把修改 POM 值当成已验证实际运行依赖。

## T04：准备可传输的单表工作单元

- [ ] 完成 T04（M，3 个文件；依赖 T01/T03；覆盖 A02/A03/A09/A16）。
- 内容：扩展现有 Spark Catalog 加载辅助方法，提取真实 FileStoreTable、显式属性、最终策略和物理身份，定义可序列化工作/结果 DTO。
- 验收：
  - [ ] 显式属性与生效属性来源分开，失败不回退；保存恢复 active session/SQLConf；不手工拼 warehouse 重建表。
  - [ ] Java/Spark 序列化往返后目标、FileIO 类型、策略和参数指纹一致，无 SparkSession/ProducerContext/Catalog 连接闭包。
  - [ ] 文件系统 URI 规范化和同路径别名判定有正反例，敏感配置不出现在 DTO 的日志/toString 中。
- 文件：`P/PaimonSqlRunner.scala`、`P/PaimonSnapshotWork.scala`（新）、`PT/PaimonSnapshotWorkTest.scala`（新）。
- 验证类：`PaimonSnapshotWorkTest,PaimonFormatAdapterTest,PaimonExpireSnapshotsIntegrationTest`；本项序列化测试不代替 T12 认证验收。

## T05：完成同一 Task 内的单表多轮清理

- [ ] 完成 T05（M，3 个文件；依赖 T02/T03/T04；覆盖 A05/A06/A09/A11/A13）。
- 内容：实现单分区入口、Task/Stage attempt 门禁、原生循环和表级返回；用测试入口直接派发一张表，不接入产品 CLI。
- 验收：
  - [ ] 分区仅一表；所有轮次 taskAttemptId 不变；达到有效上限才继续，不创建新 Job、不处理第二张表。
  - [ ] Task/Stage 非首次 attempt、无 TaskContext、分区数量不符都在原生调用前拒绝；正常退出能确认返回数量。
  - [ ] 中断/原生异常立刻停止续轮，不进入失败重试；已确认轮数保留，未知部分标记不完整。
- 文件：`P/PaimonSnapshotTask.scala`（新）、`PT/PaimonSnapshotTaskTest.scala`（新）、`PT/PaimonSnapshotExecutorIntegrationTest.scala`（新）。
- 验证类：`PaimonSnapshotTaskTest,PaimonSnapshotExecutorIntegrationTest`；本地测试需显式标为 local 证据，不能声称已证明跨 JVM。

### C2：工作单元和单表路径成立

- [ ] 1.3.2 `clean test -Dtest=PaimonSnapshotRuntimeTest,PaimonSnapshotWorkTest,PaimonSnapshotTaskTest,PaimonSnapshotExecutorIntegrationTest`；命令前缀同 C1。
- [ ] 分别反转 attempt 门禁条件和续轮比较条件，运行对应测试观察失败后恢复文件；保存缺陷被捕获的证据，不提交变异源码。
- [ ] 复查本任务无新状态存储、同 Task 多轮、未捕获 Driver 对象；旧 profile 编译仍通过。

## T06：让截断明细不影响累计统计

- [ ] 完成 T06（M，5 个文件；依赖 T05 的结果契约；覆盖 A10/A14）。
- 内容：在 ActionResult 增加可选累计任务计数，默认兼容；汇总改用 Long 并说明完整性。新的 Task 结果在后续编排中转换，不修改已有 action 的轮次意义。
- 验收：
  - [ ] 未提供累计数时旧 tasks 推导结果不变；提供时使用累计数，150 轮保留 100 条详情仍统计 150。
  - [ ] UNKNOWN 完全无返回和部分已确认两种情况均不显示精确零删除；汇总注明计数不完整。
  - [ ] 表 success/failed/skipped/notExecuted 守恒，Spark Task 个数不混入清理轮数。
- 文件：`C/ActionResult.scala`、`C/ProducerSummary.scala`、`C/ProducerApp.scala`、`CT/ActionResultTest.scala`、`CT/ProducerAppTest.scala`。
- 验证类：`ActionResultTest,ProducerAppTest`；复核现有 JSON 字段保持。

## T07：实现有界的表级调度器

- [ ] 完成 T07（M，3 个文件；依赖 T03/T04/T05/T06；覆盖 A04/A09/A10/A14）。
- 内容：Paimon 包内完成准备、提交、完成队列和结果转换；注入可控的提交边界测试，无产品入口变化。
- 验收：
  - [ ] 准备+在途占用不超过上限，重量级对象有界；每表一次单分区清理 Job/一次清理 action，去重键由协调方原子登记。
  - [ ] 首个失败被观察后，false 不补充新任务、true 可继续其他表；已提交但无结果为 UNKNOWN，无主动重投；别名仅输出跳过。
  - [ ] 每个目标仅回调一次，所有回调在协调线程；线程上下文恢复，准备失败与队列失败不遗漏结果、不泄漏线程。
- 文件：`P/PaimonSnapshotExecutor.scala`（新）、`PT/PaimonSnapshotExecutorTest.scala`（新）、`P/PaimonSnapshotWork.scala`。
- 验证类：`PaimonSnapshotExecutorTest`；用 latch 控制完成顺序与故障，不通过 sleep 断言并发。

## T08：接入通用批执行扩展点

- [ ] 完成 T08（M，4 个文件；依赖 T06；覆盖 A01/A03/A14）。
- 内容：为 action 提供可选批执行器和 runtime 校验；core 只分发和汇总，先用 fake action 验证，旧 Action 全部默认串行。
- 验收：
  - [ ] 默认方法保持原有串行、重试与日志行为；core 不引用任何 Paimon 类或硬编码 action 名。
  - [ ] 运行组合校验在目标解析/删除前，专用校验异常为退出码 2，其余失败为 1；零目标成功但不绕过配置校验。
  - [ ] 批执行回调与返回结果没有重复输出；退出前等待批执行器完成收尾，外层才调用 spark.stop。
- 文件：`S/ProducerAction.scala`、`S/ProducerBatchExecutor.scala`（新，含运行配置异常类型）、`C/ProducerApp.scala`、`CT/ProducerAppTest.scala`。
- 验证类：`ProducerAppTest,LakehouseProducerTest,ProducerCliParserTest`。

### C3：编排和框架边界可用

- [ ] 1.3.2 `clean test -Dtest=PaimonSnapshotExecutorTest,ActionResultTest,ProducerAppTest,LakehouseProducerTest,ProducerCliParserTest`。
- [ ] 检查失败/取消/队列异常下在途数量、结果数和线程收尾；并发器归属 Paimon，未为所有 action 新增调度平台。
- [ ] 复核默认 Driver 行为、累计统计和去重后表数；出现接口需求扩张则先修订计划。

## T09：开放 Executor CLI 的最小完整路径

- [ ] 完成 T09（M，3 个文件；依赖 T07/T08；覆盖 A01/A02/A04/A06/A14/A15）。
- 内容：新增 executionMode/tableConcurrency，按配置选择批执行器，连通实际多表本地测试。
- 验收：
  - [ ] 无新参数保持 Driver；只接受 driver/executor、并发正整数；Driver 显式传并发参数失败；其他 action 不接受这些专用参数。
  - [ ] Executor 完整路径从原有 TableResolver 到同 Task 多轮、逐表日志、总汇总/退出码可运行；实际重叠且在途不超限。
  - [ ] 表保留优先、读取失败、continueOnTableFailure、零表/单表和 0.9 拒绝场景可重复验证。
- 文件：`P/PaimonExpireSnapshotsAction.scala`、`PT/PaimonExpireSnapshotsActionTest.scala`、`PT/PaimonSnapshotExecutorIntegrationTest.scala`。
- 验证类：`PaimonExpireSnapshotsActionTest,PaimonSnapshotExecutorIntegrationTest,ProducerAppTest`。

## T10：完善观测字段与资源收尾

- [ ] 完成 T10（M，5 个文件；依赖 T09；覆盖 A10/A12/A13/A18）。
- 内容：关联 Job/Task/轮次事件，补充可信延迟、脱敏错误、100条详情上限、取消和 Listener/线程池生命周期。
- 验收：
  - [ ] 同 Executor 多 Task 的表、策略、轮次、job description 不串用；各 work 与 Job/Task 可追溯，缺失指标留空。
  - [ ] 明细截断不影响 Long 累计值，UNKNOWN 的部分结果不丢失；不使用 accumulator 证明副作用完成。
  - [ ] 成功/失败/取消均清理线程上下文及 Listener，异常消息不泄漏合成凭据；跨 JVM 耗时不直接相减墙钟。
- 文件：`P/PaimonSnapshotObservability.scala`（新）、`PT/PaimonSnapshotObservabilityTest.scala`（新）、`P/PaimonSnapshotTask.scala`、`P/PaimonSnapshotExecutor.scala`、`PT/PaimonSnapshotExecutorIntegrationTest.scala`。
- 验证类：`PaimonSnapshotObservabilityTest,PaimonSnapshotExecutorIntegrationTest,ProducerAppTest`。

## T11：补齐保护语义和故障回归

- [ ] 完成 T11（M，4 个文件；依赖 T10；覆盖 A07/A08/A09/A10/A11/A14/A18）。
- 内容：基于已连通路径做原生保护语义对照和确定性故障注入，验证错误处理不会重新进入删除。
- 验收：
  - [ ] Tag/Consumer/changelog/合并后存活文件与 SQL 对照一致；持续写入下同 Task 续轮，测试以受控停止写入收敛。
  - [ ] Future 未结束即抛错、返回丢失、Task/Stage 重放分别断言无额外 native 调用，未知计数和非零退出准确。
  - [ ] 取消、基础设施失败及 false 停止派发覆盖在途/未提交分类；变异 attempt 门禁、失败后补充条件能让测试失败。
- 文件：`PT/PaimonSnapshotNativeExpireTest.scala`、`PT/PaimonSnapshotTaskTest.scala`、`PT/PaimonSnapshotExecutorTest.scala`、`PT/PaimonSnapshotExecutorIntegrationTest.scala`。
- 验证类：以上四类；如果实际原生错误不可由本地 fixture 控制，保留未完成项交给 T12 真实故障，不能用 mock 结果代替外部验收。

### C4：本地实现验收

- [ ] `./mvnw -pl amoro-producer clean test -Pproducer-paimon-1.3.2`
- [ ] `./mvnw -pl amoro-producer clean test -Pproducer-paimon-0.9.0`
- [ ] 核对 A01–A18 的本地证据与剩余多 JVM 项，记录实际测试数和跳过项；没有把 local 模式包装成集群通过。

## T12：真实多 JVM 和目标后端故障验证

- [ ] 完成 T12（M，最多 5 个文件；依赖 T11；覆盖 A05/A09/A10/A11/A12/A13/A16/A17/A18/A19）。
- 内容：增加仅用于隔离环境的集群试验入口和事件核对脚本，在独立 Spark 进程及目标 YARN 环境执行受控场景；本任务依赖环境访问与试验数据授权，不推断生产清理授权。
- 验收：
  - [ ] 固定 2 个 Executor、每个至少 2 个 Task 槽，证明非 Driver 执行、同一 Executor 两表 Task 重叠、每表只一个 Task 及多轮不换 attempt；记录 JAR SHA256、实际配置和终态。
  - [ ] 实际终止 Executor、丢失返回和重放门禁、取消/Driver 崩溃覆盖原生调用与存储变化；重跑前确认旧执行结束，不恢复旧轮次。
  - [ ] Hive/HDFS/实际对象存储分别证明序列化、身份、权限和插件正确；不支持的环境明确记录，不能泛称已覆盖所有后端。
- 文件：`PT/PaimonSnapshotClusterTestJob.scala`（新）、`amoro-producer/src/test/scripts/verify_snapshot_executor_events.py`（新）、必要时 `amoro-producer/pom.xml`（测试 JAR 打包）、`tasks/producer-snapshot-cluster-validation.md`（新）、`tasks/producer-snapshot-validation-evidence.md`（新）。
- 验证：`./mvnw -pl amoro-producer clean package -Pproducer-paimon-1.3.2 -DskipTests`；根据实际集群模板在 validation 文档中生成完整 spark-submit 命令后执行。命令必须包含确切产物/依赖、固定资源、关闭 speculation、maxFailures=1、隔离表；专用重放 harness 例外需标清。读回终态、Driver/Executor 日志、事件及表数据，不能仅凭提交成功。

## T13：测量整库收益与存储代价

- [ ] 完成 T13（S，2 个文件；依赖 T12；覆盖 A20）。
- 内容：四类负载执行 Driver、并发2、并发4对照，每组至少五次独立初始状态，文件线程数及保留参数保持用户原值。
- 验收：
  - [ ] 报告整库耗时中位数/范围、排队/执行分布、慢表和小表 Job 开销，并区分冷/热缓存及写入速度。
  - [ ] 记录 RPC p95/错误/限流、实际 Executor 文件池规模、CPU/GC；不声称严格等于 tableConcurrency×32 的并发。
  - [ ] 与目标环境确认后的门槛比较；若收益不成立，记录保持默认 Driver 的结论，不因代码完成而启用。
- 文件：`tasks/producer-snapshot-performance.md`（新）、`tasks/producer-snapshot-validation-evidence.md`。
- 验证：运行 T12 固化的模板，核对每次初始快照集合和 JAR 指纹；候选门槛为整库中位数改善至少20%、超过噪声、RPC p95 劣化不超过10%且无新增错误/限流，未获 SLO 确认不得声称正式达标。

## T14：完成文档、构建和交付审查

- [ ] 完成 T14（M，3 个文件；依赖 T13；覆盖全部验收与交付边界）。
- 内容：将真实支持矩阵、CLI、重试/未知结果、取消和重跑流程写入 README；按证据更新 Spec 状态并完成实现审查。
- 验收：
  - [ ] 文档符合最终实现，无状态目录、同 Task 多轮、外部互斥边界明确；回退 Driver 前确认旧执行结束。
  - [ ] 两组合测试、格式检查和1.3.2打包完成，报告实际命令/JDK/测试数/产物；所有 Required/Critical 实现审查意见闭环。
  - [ ] 未运行或失败验收保留未勾选；仅提交明确任务文件，不混入工作区其他改动，不自动 push/上线。
- 文件：`amoro-producer/README.md`、`SPEC-producer-distributed-snapshot-expiration.md`、`tasks/producer-snapshot-validation-evidence.md`。
- 验证：两个 profile 的 `clean test`，`./mvnw -pl amoro-producer validate -Pproducer-paimon-1.3.2`，`./mvnw -pl amoro-producer clean package -Pproducer-paimon-1.3.2 -DskipTests`；实现提交前按项目要求格式化并检查实际 diff。仅在新增变化/故障时重复已通过测试。

### C5：最终验收

- [ ] A01–A20 全部有证据；不满足的后端/性能项明确阻止对应支持/上线声明。
- [ ] 独立代码审查与变异验证完成，Required/Critical 闭环，验收证据与最终提交一致。
- [ ] 人工审阅最终实现与部署边界；没有把本次计划审查视为上线批准。
