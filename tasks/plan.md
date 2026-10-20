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

# 实施计划：Producer 将单表快照过期任务下发到 Spark Executor

## 1. 状态、依据与交付范围

- 日期：2026-10-20；分支：`hotifx/producer-snapshot-retention`；计划基线：`f43ca2191`。
- 依据：[Spec](../SPEC-producer-distributed-snapshot-expiration.md)、当前 Producer 源码、Paimon `release-1.3.2`（`16faabbddbe30477a7e2c3bacde5e541c8f1cf9c`）以及本地 Spark 3.5.4 / Paimon 0.9.0、1.3.2 源码包。
- 本次交付为计划、任务清单及计划审查；**没有实现功能、运行清理或测量性能**。后续实施状态统一维护在 [todo.md](todo.md)。
- 首版按 Spec 的推荐方向规划：默认 `driver`；显式 `executor`；并发上限默认 4、首次验证 2；Executor 仅支持 Spark 3.5.4 / Paimon 1.3.2。旧 profile 是 Spark **3.2.1** / Paimon 0.9.0，仍需编译并通过 Driver 回归。
- 生产版本、权限、资源模板和性能 SLO 尚未取得，不阻碍编写计划；真实环境验证依赖这些输入。不得将计划评审通过等同于运行验收通过。

## 2. 不变的执行契约

1. 一个 Executor 可同时执行多个 Task；每个 Task 只绑定一张表；达到该表最终 `maxDeletes` 后，在同一个 Task 内继续下一轮。轮次之间不创建 Job，不换 Task，不向空闲 Task 转移剩余工作。
2. 每张表一个单分区清理 Job，只触发一次清理结果收集；Driver 最多维持 `tableConcurrency` 个在途工作单元。一次只有一个表的巨大积压不会因本设计自动提速。
3. Driver 读取元数据、准备任务、提交和汇总；Task 直接调用原生 Paimon API。禁止在 Task 中调用 Spark SQL 或创建 SparkSession。
4. 不增加执行状态目录、锁、租约、授权 RPC 或外部协调服务；runId/workId 仅用于本次内存关联和日志。
5. 保留显式表属性优先、时间保留规则、Tag/Consumer 保护。不同应用针对同表的互斥由既有外部调度保证；不声称 exactly-once。
6. 保留完整单表循环，不增加总轮数/总时长上限或固定启动快照边界。持续写入表可能长期占槽，这是本次需求接受的边界。

## 3. 源码事实与设计落点

| 已核对的事实 | 实施决定 |
|---|---|
| `ProducerApp.executeTables` 串行，统一负责日志和退出汇总 | 增加一个通用批执行扩展点；Paimon 调度器留在 `paimon` 包，core 不判断具体 action 名或引用 Paimon 类 |
| `PaimonExpireSnapshotsAction.resolveConfig` 已修复表属性优先 | 抽取成唯一的策略解析器，两种模式共用；旧 SQL 路径保持既有重试与时区行为 |
| `PaimonSqlRunner.loadTableMetadata` 从 Spark Catalog 加载 `SparkTable` | 复用其会话保护方式获取真实 `FileStoreTable`，不用 warehouse 路径拼装另一个表 |
| 两个 Paimon 版本的 `Table` 均为 Serializable，均提供 `newExpireSnapshots().config(...).expire()` | 使用编译期类型和两版共有的 `ExpireConfig.builder()` 四个 snapshot setter；是否能带正确 FileIO 跨 JVM 仍须验证 |
| 1.3.2 Procedure 对表应用 dynamic options，再填充 snapshot 配置 | 保持 `table.copy(options)` 与四个最终保留值的等价行为，不使用会额外改变 changelog 配置的便捷构建器 |
| Spark 3.5.4 `TaskContext` 提供 `attemptNumber`、`stageAttemptNumber`，首次均为 0 | 在删除前要求两者都为 0；attemptId 只用于追踪，不能只检查它是否为 0 |
| 原生文件删除可能在异常返回时仍有 Future 在运行 | 首版 Executor 的原生过期调用异常一律不自动重试；也不重投整表 |
| `ProducerSummary` 当前按 `result.tasks` 数组计数 | 增加可选的累计计数模型，避免截断明细造成少算；不把 Spark Task 数替代清理轮数 |
| `TableActionJsonLog` 已把缺失删除计数输出为 null | 继续复用渲染器及现有字段，UNKNOWN 不补造 0；优先用 metrics 增加观测字段 |

实施前再次核对工作区 HEAD 和当前改动。保留当前无关未跟踪文件；每次提交只 stage 本任务明确文件。

## 4. 具体实现方案

### 4.1 策略、工作单元和原生执行

- `PaimonSnapshotRetention.scala`：从现有 Action 抽取解析器和不可变最终策略；显式属性仍从 `$options` 判断，不能把填满默认值的属性当成显式值。
- `PaimonSnapshotNativeExpire.scala`：唯一原生调用边界；在 Task 内按最终 options 创建本表副本和 ExpireSnapshots，每轮读取当前快照状态。保留项只写入 snapshotRetainMin/Max、snapshotTimeRetain、snapshotMaxDeletes。
- `PaimonSnapshotWork.scala`：明确可序列化 DTO，含真实 FileStoreTable、最终策略、逻辑表名、规范化物理身份、runId/workId 和参数指纹。禁止捕获 ProducerContext、SparkSession、SparkTable 或活动 Catalog 连接。
- 原生 ExpireConfig、轮次状态和删除器在 Task 内创建，不传输共享的可变实例。敏感 options 仅按原生 FileIO 所需传输，不进入日志和结果；不复制 keytab 内容。
- 物理身份以 Catalog 加载的规范化表位置为主；显式处理 URI scheme/authority/path，不能用字符串包含关系去重。别名指向同一位置时保留首个清理工作单元，后续别名输出 `Skipped(duplicateTarget)`，保持目标汇总守恒。跨 scheme 别名、路径迁移和 DROP/重建属于首版运行前提，不能声称自动识别全部别名。

### 4.2 运行校验与单 Task 循环

- `PaimonSnapshotRuntime.scala` 在 Driver 预检和 Task 删除前分别验证 Spark/Paimon 实际版本、必要 API、FileIO 类型及本表身份；只识别 Procedure options 参数不能证明版本。
- 版本证据取自实际加载类的 CodeSource 和随该来源发布的版本元数据；检查 Paimon core/API/Spark 集成组件一致性。shaded JAR 的资源必须与实际来源对应；版本缺失、多来源冲突或无法确认都明确失败，禁止以 CLI/build 默认版本冒充运行版本。
- 如当前打包物无法提供可信版本信息，T03 只调整本模块构建元数据或明确的类来源校验；不得用宽泛反射、`Any` 或捕获所有 Throwable 后静默降级。
- Executor 模式严格要求 `spark.speculation=false`、`spark.task.maxFailures=1`。后者不保证不会发生所有重放；Task 与 Stage attempt 的双重检查仍必须执行。
- `PaimonSnapshotTask.scala` 在入口检查 TaskContext、首次 attempt、运行时和表身份。每个分区恰有一个 work；空或多 work 属于协议错误，删除前失败。
- 循环：检查取消/中断 → 原生单轮调用 → 记录确认结果 → 返回值小于有效 maxDeletes 则结束，否则在同一 Task 续轮。恰好等于上限时允许最后多一轮返回 0。
- 取消/中断不能被普通失败重试吞掉。原生调用开始后发生任何不确定异常，返回或汇总为 UNKNOWN，并停止本表循环；不能因上层返回就宣称底层异步删除已结束。
- `retryTimes` 在新模式仅重试删除前的只读准备；一旦进入原生 API，本版本不做单轮失败重试。此决定是对 Spec 的保守收敛，正常成功后的续轮不属于重试。旧 Driver 重试不变。

### 4.3 Driver 编排和框架接入

- `ProducerAction` 增加默认无操作的 `validateRuntime(context, config)`，以及默认返回 None 的 `batchExecutor(context, config)`；`ProducerBatchExecutor` 仅定义批执行和逐表结果回调的契约。
- runtime 校验在 Spark 创建后、目标解析前执行。专用参数/运行组合异常映射退出码 2，其他运行失败仍为 1；不要把所有运行阶段 IllegalArgumentException 都改成参数错误。
- `ProducerApp` 只选择“默认串行路径 / action 提供的批执行器”。默认不提供批执行器，原有所有 action 行为保持；core 不实现快照政策、Paimon 版本判断或专用调度算法。
- `PaimonSnapshotExecutor.scala` 持有固定大小提交线程池和完成队列。每个在途单元从准备开始占用一个名额，到收到可信结果/失败完成才释放；重量级表对象和等待提交缓冲总量均不超过上限，不预先创建整库 Future。
- 单一协调循环处理完成事件、别名去重、日志回调和补充任务。处理已到达的失败事件后再补充；停止标志与提交边界由同一协调方控制，避免并发 worker 自行拉下一张表。
- 准备阶段 `$options` 只读查询可产生额外 Spark Job，使用 preparation 标识；清理 Job 才使用 workId 的 cleanup 标识，验收按清理 Job 统计一次 action。
- 每个提交线程保存/恢复 active SparkSession、SQLConf、线程类加载器和 Spark local properties；job group 用 runId/workId，description 沿用当前保留参数格式。
- `continueOnTableFailure=false`：观察到失败后停止新派发，等待在途表收敛；true：普通单表失败后继续其他表。SparkContext 不可用等基础设施失败停止全局派发。
- 回调仅在协调线程调用，每个逻辑目标结果一次；execute 返回同一批结果给已有汇总，禁止再次输出。记录准备失败、别名跳过和已提交结果；尚未派发目标才是 notExecuted。
- 调度器 finally 中停止接收、处理线程池和在途 Job，再返回到外层 `spark.stop()`；取消时对已提交但没有可信返回的表输出 UNKNOWN。不承诺故障进程必然能写出最终日志，不把 cancelJobGroup/future.cancel 成功当作删除已停止。

### 4.4 结果、资源与失败分类

| 情况 | 对外结果 | 可否重试/续轮 |
|---|---|---|
| 配置/运行组合不支持 | 退出码 2（Driver 预检）；Task 发现不同组合则表失败 | 不删除，不降级 |
| 本表准备只读失败 | 耗尽 retryTimes 后 Failed | 仅准备可重试 |
| 正常单轮返回达到上限 | 记录本轮确认数量 | 同一 Task 下一轮 |
| 原生调用异常、结果丢失、Task/Executor 失联 | Failed + completionState=UNKNOWN + countsComplete=false | 不重试原生调用，不重投表 |
| 后续 Task/Stage attempt | 拒绝删除；此前进度不能确认时 UNKNOWN | 不重放 |
| 目标别名重复 | Skipped + duplicateTarget + 主 workId | 不创建第二个表 Job |

- Executor 模式每表最多保存最近 100 条轮次详情；所有轮次实时日志输出，累计成功/失败轮数使用 Long；截断标记和总数独立于明细，不增加新的用户配置。
- `ActionResult` 增加默认 None 的 `ActionTaskTotals(success, failed, complete)`；旧 action 从 tasks 推导，新路径提供累计计数。`ProducerSummary` 聚合 Long 并输出计数是否完整；UNKNOWN 的未知轮数不能作为精确 0 报告。
- `deletedSnapshotsTotal` 只累计可信返回；结果完全丢失则缺失/null；有已确认轮次但后续未知则保留已确认值并明确 countsComplete=false，含义为下界。不能通过 accumulator 恢复“精确完成量”。
- 已确认表结果按 workId 汇总，别名按逻辑目标关联；只保留 O(目标数量) 的轻量结果、去重键和计数，Producer 管理的 Driver 重量级 FileStoreTable 及闭包引用在在途结束后释放；Executor 中仍运行的原生 Future 可能继续持有表/FileIO 对象，不承诺全应用对象立即回收。
- Task 结果携带 application/stage/partition/attempt/Executor 信息；jobId 和 queueWait 优先从 SparkListener 按 job group 关联。Listener 迟到或丢失时留空，不阻塞退出等待精确指标；结束后移除 Listener 和临时关联项。
- executionMs 和 endToEndMs 分别在各自 JVM 用单调时钟测量；不得直接跨机器墙钟相减。所有异常消息也要脱敏，不能只清洗正常 options 日志。
- 在途上限约束准备/提交工作单元，不能限制失联或异常后遗留的底层 I/O；UNKNOWN 不意味着释放了全部存储压力。
- 同一 JVM 的 Paimon 文件线程池共享且可能扩容；表并发数不等于文件请求数。首轮保持线程数 32 和固定 Executor 配置，以测量实际存储压力。

## 5. 任务依赖与检查点

详细验收、文件范围和验证命令见 [任务清单](todo.md)。每项按一次聚焦实现会话拆分；若预计超过约两小时或五个文件，先细分该项再编码。

```text
T01 策略复用 → T02 原生等价验证 → C1
T03 运行组合校验 ─┐
T01 → T04 工作单元 ├→ T05 单表 Task → C2
T02 ──────────────┘
T06 有界统计 → T07 有界调度（依赖 T03/T04/T05）
T08 框架扩展点 → C3
T07/T08 → T09 CLI 端到端 → T10 观测与资源收尾 → T11 语义/故障覆盖 → C4
T11 → T12 多 JVM/YARN 验证 → T13 性能对照 → T14 交付检查 → C5
```

T03、T06、T08 可在接口契约明确后独立准备；它们共享 core/测试文件，默认顺序实施，不能并行改同一文件。检查点以实际证据判断，未通过时不推进依赖项。中间任务不开放新 CLI，T09 才连通用户入口，默认 Driver 始终可用。

## 6. 验证方法与证据标准

- 单元层验证策略、attempt 门禁、停止派发、未知结果和累计计数；并发测试使用 latch/barrier 和有界等待，不以 sleep 碰运气，也不要求固定 Executor 落点。
- 本地 Spark 验证原生 SQL 等价、同 attempt 多轮和表间参数隔离；`local[2]` 不能证明非 Driver 进程执行。保护规则覆盖 Tag、Consumer、changelog 和合并后的存活文件；原生代码直接读取系统时钟，测试固定快照时间戳并使其远离保留边界、停止写入，记录运行窗口以确认没有跨界，不假定存在可注入的原生时钟。
- 独立多 JVM 层使用固定两个 Executor（每个至少两个可用 Task 槽），验证非 Driver ID、同 Executor 的两表 Task 重叠和空槽由新 Task 复用。结合事件日志核对每个 work 的 Job/partition/action 次数。
- 故障注入至少覆盖 native Future 未结束时抛错、Task 已清理但返回丢失、Executor 被终止、取消、Driver 崩溃。通过原生调用计数/存储审计证明重放没有再次进入删除；仅检查失败状态不足够。
- A09 的纯单元测试分别注入 taskAttempt>0 和 stageAttempt>0；真实 Spark 重试至少实际触发并观察可达路径，不因配置 maxFailures=1 而跳过。测试可以在专用 harness 中放开重试来验证门禁，但产品入口仍要求 maxFailures=1。
- Hive/HDFS/对象存储逐种验收真实认证、插件、token 和权限；不把 filesystem local 的成功泛化到其他后端。测试只使用隔离数据，生产清理需要独立运行授权。
- 性能：四类负载、Driver/并发2/并发4，每组至少五个相同初始状态的独立副本；比较整库中位数和范围，记录表级排队与执行、RPC 延迟/限流、CPU/GC、Job 提交开销。20% 改善和 10% RPC p95 劣化上限仍是候选门槛，需目标环境 SLO 认可。
- 编译/测试按两个 Maven profile 分别 clean；检查 Surefire 报告确实执行目标类。没有测试的文档提交不跑清理/故障/变异测试；实现完成后对新增 attempt 门禁和续轮条件分别做条件反转，确保对应测试失败，再恢复源码。

## 7. 风险、处置与进入实现条件

| 风险 | 处置与停止条件 |
|---|---|
| 新 API 让 0.9.0 profile 无法编译 | T02/T03 在编排前验证双 profile；只使用明确共有 API，不能隐藏链接错误 |
| shaded 包无法可靠识别真实版本 | T03 以实际打包样本证明识别；未知则拒绝 Executor 模式，不能按默认值放行 |
| 表对象能序列化但权限/插件失效 | T04/T12 分开验证传输和真实存储访问；未通过的后端不列为受支持 |
| 失联 Task 可能仍在删除 | 禁重放和自动重投；UNKNOWN；外部调度确认旧执行终止后才能重跑。无持久化恢复 |
| 共享 FileIO/全局池导致参数串用或存储饱和 | 每 Task 独立状态；并发隔离回归；按2/4测量总压力，未达门槛保持 Driver |
| 持续写入导致长尾、在途无法收敛 | 明确同 Task 无限续轮边界；观测占槽；取消不宣称底层立即停止，不偷偷加公平轮转 |
| 截断/失败导致统计失真 | 100条有界明细、独立 Long 累计数、countsComplete，覆盖结果全部丢失和部分已知 |
| 日志或异常泄漏敏感配置 | options/凭据禁止输出；使用合成敏感值验证错误路径脱敏 |

计划方向明确，但绝对安全和收益必须靠实现、故障注入及真实环境证据证明。目前不作“100% 已验证”的承诺。实现前审阅本计划即可；不再重新询问已确认的无状态存储、每 Task 一表和同 Task 多轮决策。生产环境信息只阻塞对应的集群验收，不阻塞已授权的本地设计工作。
