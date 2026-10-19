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

# Spec：Producer 按表分发快照过期任务到 Spark Executor

## 1. 状态与版本边界

- 状态：**DRAFT / 待评审**。本文描述待实现能力；没有实现分布式执行，没有执行生产清理，也没有性能测试结果。
- 日期：2026-10-19（Asia/Singapore）。
- 工作分支：`hotifx/producer-snapshot-retention`，保留用户指定的分支拼写。
- Rebase 目标：本地 `dev`，提交 `6631bfe106af0540eb703f5166359bafeafd81a5`；本次没有 fetch 或修改远端。
- Rebase 后代码基线：`7b257cd8f0fc48bccb66df805d94799a2ef5d838`。
- 原保留策略修复：`cc01a5731d073bf3a971304150c5face44e54a58`，rebase 后为上述 `7b257cd8f`；`git range-diff` 显示补丁等价。
- 源码核对版本：Paimon `release-1.3.2`，提交 `16faabbddbe30477a7e2c3bacde5e541c8f1cf9c`。
- Producer 默认构建组合：Spark 3.5.4 / Paimon 1.3.2 / Scala 2.12。不能套用根 POM 的 Paimon 1.4.2 作为本模块运行版本。
- 用户已提供应用参数，尚未提供生产运行 JAR 指纹、Spark 资源参数、慢表日志和存储负载数据。

### 1.1 已确认需求

1. 分析并改善整库快照过期耗时。
2. 将各表实际清理工作下发到 Spark Executor Task，Driver 负责编排和汇总。
3. 在指定分支 rebase 本地 dev 后编写并保存 Spec；本轮不实现代码。

### 1.2 本文采用的设计假设

以下是建议契约，尚不是用户逐项确认的要求：

1. 首版 Executor 模式只覆盖 Spark 3.5.4 / Paimon 1.3.2；保留 0.9.0 Driver 模式。
2. 新模式显式启用，默认仍使用 Driver；新增表级并发上限默认 4，首次验证使用 2。
3. 首版一个表的完整清理循环属于一个逻辑工作单元，不实现按轮抢占或同表跨 Task 并发。
4. 为防止 Task 重放和重复作业清理同表，引入持久化占用及结果记录；建议首版使用独立 HDFS 状态目录，禁止按超时自动抢占。该运维成本需评审确认。
5. 保留当前分支的“显式表保留配置优先”规则。Executor 模式的失败重试受副作用安全约束，不能无条件复制旧 SQL 重试循环。

## 2. 问题、目标和非目标

用户当前参数：

```text
--format paimon --action expire-snapshots --catalogName paimon
--databaseName sl_oki_prod --retryTimes 3 --continueOnTableFailure true
--retainMax 20 --retainMin 10 --maxDeletes 550
--procedureOptions file-operation.thread-num=32,snapshot.expire.execution-mode=sync
```

现有 `ProducerApp.executeTables` 串行处理目标表。`PaimonExpireSnapshotsAction.execute` 对同表反复调用 Procedure，直到返回数量小于最终生效的单轮上限才结束。因此一张大表会推迟所有后续表；持续写入使每轮都达到上限时，单表循环没有固定结束时间。

Paimon 1.3.2 的 Procedure 同步调用原生 `ExpireSnapshots.expire()`。跨快照循环、部分元数据读取及快照文件删除仍串行，部分文件删除提交到 JVM 内共享线程池。仅增加 Executor 数或把 execution-mode 改为 async，不会使当前 Procedure 自动分布式执行。

目标：

- 不同表在多个 Executor JVM 上并发清理，降低整库耗时与后续表的等待时间。
- Driver 不执行 Executor 模式的快照／数据／Manifest 删除，只做元数据准备、调度和控制状态管理。
- 保持现有分支的保留策略、Tag／Consumer 原生保护规则、表筛选以及退出码语义。
- 输出可归因到具体表、工作单元和 Spark Task attempt 的执行证据。
- 对重复执行和结果未知采取明确的恢复协议，避免把 Spark 重试当成 exactly-once。

非目标：

- 不重写 Paimon 的过期算法，不拆分同表快照范围、bucket 或文件给多个清理 Task。
- 不修改 compact、compact-manifest、remove-orphan-files 或 AMS 的调度逻辑。
- 不修改表的保留属性、write-only、生产写入任务或认证配置。
- 不自动缩短保留时间／数量；不承诺单张大表加速或固定倍数提升。
- 不在首版实现轮转、公平抢占、固定启动快照清理范围、自动清理异常占用记录。
- 不把过期数量当作全部物理文件删除成功的证明；原生清理可能记录告警并跳过部分文件。

## 3. 保留策略兼容契约

必须复用或等价抽取现有 `resolveConfig`，不能退回此前 dev 中命令行参数直接覆盖表策略的实现。

| 配置 | 最终值及优先级 |
|---|---|
| retainMin | `$options` 显式 `snapshot.num-retained.min`；否则 CLI；CLI 默认 10 |
| retainMax | `$options` 显式 `snapshot.num-retained.max`；否则 CLI；CLI 默认 20 |
| maxDeletes | `$options` 显式 `snapshot.expire.limit`；否则 CLI；CLI 默认 550 |
| timeRetained | `$options` 显式 `snapshot.time-retained`；否则 procedureOptions 同名项；1.3.2 下再回退到原生默认 1h |
| 其余 options | 保持现有已加载表的生效属性，并叠加 CLI procedureOptions；四个保留项统一写回为上述最终值 |

- 数量配置不从 procedureOptions 反向覆盖 CLI／显式表属性；时间配置单独按上表解析。
- `$options` 用于判断显式配置，不能用已经填满默认值的属性替代这一判断。实际执行表仍需保留 Spark Catalog 加载时生效的配置。
- `retainMin >= 1`、`retainMax >= retainMin`、`1 <= maxDeletes <= Integer.MAX_VALUE`、时间非负。
- 读取失败或配置非法时该表失败，不能静默回退到更激进的清理策略。
- 每表准备时解析一次，Task 和安全重试使用同一份不可变配置。执行期间修改表保留属性不动态刷新；需要立即生效时停止并确认旧清理结束后重新提交。
- 每轮重新读取原生快照状态；保持当前 1.3.2 按调用时刻计算时间保留边界的行为，不隐式改成作业启动时刻。
- 循环判断使用最终生效的 maxDeletes。例如表配置为 50，则以返回值是否达到 50 决定续轮，而不是以 CLI 550 决定。
- Executor 直接构造与当前 Procedure 等价的 `ExpireConfig`；不能顺手使用含其他默认策略的构建器改变 changelog 等行为。
- 0.9.0 Driver 路径继续保留 options 能力识别、无时间策略时 UNLIMITED、UTC 偏移时间边界等现有行为；明确请求不支持的 Executor 组合必须在删除前报错，不静默降级。

## 4. 命令行与模式契约

以下参数均是**待实现**参数，当前二进制不支持。

| 参数 | 默认值 | 契约 |
|---|---|---|
| `--executionMode` | `driver` | `driver` 或 `executor`；仅 expire-snapshots 使用 |
| `--tableConcurrency` | `4` | Executor 模式同时在途的表工作单元上限，必须为正整数 |
| `--executionStatePath` | 无 | Executor 模式必填；所有参与该协议的 Producer 共用的独立 HDFS 控制目录 |

- 显式在 driver 模式使用 Executor 专用参数应报参数错误，避免看起来配置成功但不生效。
- `--procedureOptions file-operation.thread-num=32,...` 继续控制原生文件操作；它不是表并发度。
- 新模式在创建 SparkSession 后、任何删除前校验实际配置和依赖：首版只接受已验证的 1.3.2 / 3.5.4 组合、禁用 speculation、具备状态目录访问权限。
- 首版建议要求 `spark.task.maxFailures=1`，减少 Spark 自动重试；它不是互斥或 exactly-once 保证，持久化协议仍必须成立。
- 不通过 `snapshot.expire.execution-mode=async` 绕过任务完成确认；直接调用原生 API 仍同步等待。
- 无目标表直接成功；参数／模式校验失败返回 2；运行失败或结果未知返回 1；只有全部成功／正常跳过时返回 0。

## 5. 执行架构

### 5.1 Driver 职责

1. 使用原有 TableResolver 枚举并筛选表，生成稳定的目标集合。
2. 通过 Spark Catalog 加载真实 FileStoreTable，读取显式属性，解析最终保留配置。
3. 为每表生成 `runId + workId` 和规范化物理表标识；同一物理表在目标集合中只保留一个逻辑工作单元。Catalog 别名不能绕过互斥。
4. 使用独立有界线程池提交最多 tableConcurrency 个表 Job，某表完成后立即补充下一张表，不等待整批结束。
5. 每表一个 Spark Job、一个分区、一个清理 Task；只触发一次收集结果的 Action。Driver 线程只等待调度／结果，不运行过期 API。
6. 设置并恢复各提交线程的 Spark active session、类加载器及 job group／description 等本地属性；不能在线程间串用表描述。
7. 将 Task 结果与持久化工作记录对账，逐表输出日志并汇总退出码。Spark Job 成功但表结果失败时，应用仍失败。

表准备和提交使用有界缓冲，不在 Driver 同时保留整库所有表的重量级对象。持久化状态中的 workId 在 Spark attempt 重放时保持不变，不能在 Task 内重新生成随机 workId 绕过去重。

选择每表独立 Job，是为了保留该分支的逐表 Job 描述、控制在途数量和首个失败后的停止派发。它会增加 Job 提交开销，必须纳入小表场景基准测试。

不采用整库 `parallelize(tables, tableConcurrency)` 作为“一表一 Task”的实现：这种写法会让一个分区包含多张表，改变故障重放粒度及表级可观测性。

### 5.2 Executor 职责

1. 反序列化表工作单元，校验运行时版本、表身份、FileIO 和参数指纹。
2. 按第 6 节获取同表独占执行资格，再创建原生 ExpireSnapshots 对象。
3. 按最终配置同步执行单轮 API，记录轮次与返回数量；满足继续条件时在当前 Task 内进入下一轮。
4. 记录持久化终态，确认所有本次清理工作结束后才释放占用，返回结构化结果。
5. 不创建 SparkSession／SparkContext，不调用 spark.sql，不在 Task 内提交嵌套 Spark Job，不携带 Driver 的 ProducerContext 或活动 Catalog 连接。

表对象可序列化不代表所有部署的 FileIO／认证都已验证。优先采用原生 FileStoreTable 的传输方式，并以多 JVM 测试证明配置、插件、delegation token 和文件访问正确；禁止把 keytab 或明文凭据写入工作记录。

Driver 可在准备阶段和 Task 启动时做身份核验；对运行中 DROP／重建／路径迁移不能靠一次快照校验保证安全。首版要求运维期间不执行这些管理操作。

### 5.3 并发与资源

- 运行表数不超过在途上限、可用 Spark Task 槽位和待处理表数三者的最小值。
- 相同 Executor JVM 的表 Task 共享 Paimon 文件操作池，不同 JVM 的池独立。
- 原生池初始大小与可见 CPU 数有关，后续请求可能扩容而不缩容；配置 32 不能被当成全应用严格的 32 请求上限。
- 固定 Executor 数量做首轮验证；不要让动态扩容同时改变测试的存储请求压力。
- 一个 Task 占用一个 Spark 槽位，但内部可能产生多个 I/O 线程，不能用 executor cores 直接估计文件系统并发。
- 首版沿用完整单表循环。持续写入的大表仍可能长期占槽；本 Spec 不承诺最大等待时间或作业必然在固定时间内结束。

## 6. 副作用、重放与恢复协议

### 6.1 必须成立的安全条件

Spark Task 可能重放，Executor 失联也不代表进程和底层 I/O 已停止。关闭 speculation、设置 maxFailures=1、Java 锁或只用 Driver 内存去重都不足以构成同表互斥。

执行资格的粒度为规范化物理表根路径。首版保守地让该表不同 branch 也共享互斥键，避免把可能共享文件的分支误当成完全独立资源。URI 别名、不同 Catalog 指向相同路径的识别必须测试；不能证明唯一身份时拒绝执行。

### 6.2 建议首版控制存储

使用用户指定、位于所有被清理表目录之外的 HDFS 控制目录：

```text
<state-root>/owners/<physical-table-key>        当前占用声明
<state-root>/runs/<run-id>/<work-id>/...        参数指纹、轮次和不可变终态记录
```

- 全部 Producer 实例必须使用同一控制目录；不同目录不能提供跨作业互斥。
- 所有权使用经验证的原子“不覆盖创建”获取，记录唯一 owner token、applicationId、workId、taskAttemptId 和目标指纹。
- 读取损坏、不完整或未知格式的所有权记录时，按“仍被占用”处理，不能当成文件不存在。
- HDFS 实现的原子性需做竞争与故障测试；不能把普通 exists-then-create 当成原子操作，也不能把泛化 FileIO 接口直接当作所有后端均支持同样语义的证明。
- 控制目录不可在本作业运行时被外部清理。首版不提供自动 GC；保留记录，后续独立定义安全回收协议。

### 6.3 状态与重复执行

| 状态 | 含义 | 再次看到相同 workId 时 |
|---|---|---|
| PREPARED | Driver 已准备，无删除 | 可由唯一获得所有权的 attempt 执行 |
| RUNNING | 已获得占用，可能有删除副作用 | 其他 attempt 不得执行删除 |
| SUCCEEDED | 原生调用结束，结果已持久化 | 返回已保存的结果，不再次清理 |
| FAILED | 明确失败且确认本工作没有遗留在途删除 | 返回失败，不自动启动第二轮完整工作 |
| UNKNOWN | 失联、中断或无法确定副作用结束 | 保留占用，禁止自动接管，应用失败 |

执行顺序必须为：获取独占资格 → 记录运行身份 → 执行原生清理 → 等待清理完成 → 原子发布终态 → 释放自己的占用 → 返回结果。

- 同一 workId 的成功结果发布在释放占用之前；返回结果丢失后重放只读取记录。
- 获得所有权后必须再次检查该 workId 终态，关闭“先读无结果、旧 attempt 随后完成、再抢到占用”的窗口。
- 相同 workId 但参数／表身份指纹不同是协议错误，拒绝执行。
- 释放必须验证 owner token；存在不确定性时保留占用。异常恢复不得删除其他执行者的占用。
- 不提供 TTL 自动抢占。只有确认旧执行进程及其文件操作已停止，才允许人工核验并解除异常占用；仅心跳过期或租约过期不够。
- UNKNOWN 的人工恢复应终结旧 workId，保留不可变的恢复记录后再以新 workId 提交。迟到的旧 attempt 必须被拒绝；不能只删除占用文件后任由旧工作重新开始。
- 原生调用返回异常／Task 收到中断时，底层线程池可能仍有删除任务，不能在 finally 中无条件释放占用。
- 已落盘成功但未返回 Driver 的结果，通过工作记录恢复；未记录的删除数量不得猜测或补算成精确值。

这是一种保守的“结果未知时停止接管”策略，不宣称实现所有故障下的 exactly-once。若不接受持久化控制目录或人工恢复成本，必须另行批准等价的协调方案，不能退回无保护 Task 重试后直接上线。

### 6.4 与其他维护任务的边界

该协议只能协调遵守相同协议的 Producer。原有 Driver 模式、AMS 过期任务、写入端自动过期等不会自动识别此占用。

首版 Executor 运行前须由调度配置保证目标表没有其他过期执行者；不能同时运行覆盖相同表的 Driver 清理任务。普通写入和合并不因本功能自动停用，但保留策略必须满足这些作业的读取时间窗口，Consumer／Tag 保护和并发写入需要原生语义回归验证。不能把表级占用当作对其他系统的全局锁。

## 7. 重试、失败及取消

- `retryTimes` 仍表示包含首次尝试的次数上限，但 Executor 模式仅对读取／准备及已经证明没有遗留副作用的失败使用自动重试。
- 重试原生过期前必须确认前一次调用的文件任务全部结束，并重新读取快照状态。若无法证明，则记录 UNKNOWN，不能仅因为异常属于 NonFatal 就继续。
- 不嵌套“整表框架重试 × Spark Task 重试 × 单轮重试”；各层尝试次数分别记录。
- `continueOnTableFailure=true`：明确的单表错误转成失败结果，继续派发其他不冲突的表；最终只要有失败或未知结果，退出码为 1。
- `continueOnTableFailure=false`：Driver 观察到首个失败后不再派发新的表，等待已经在途的表收敛。竞态中此前已提交的 Job 算在途，不能承诺“一个失败瞬间其他 Task 都不再启动”。
- SparkContext 丢失、集群不可用或状态存储整体不可用时允许停止整个运行；不能承诺基础设施故障下仍执行所有表。
- 用户取消／超时不回滚已删文件。Driver 不在收到取消后立刻宣称底层删除全部停止；已提交但无可信终态的工作归为 UNKNOWN。
- 没有被提交的表统计为 notExecuted；已提交但结果不明的表不得统计为正常 skipped。

## 8. 结果与可观测性

保持现有表级 JSON 字段兼容，增加以下信息：

- executionMode、runId、workId、表逻辑名和物理身份指纹。
- 最终 retainMin、retainMax、timeRetainedMs、maxDeletes，以及参数来源。
- submittedAt、startedAt、finishedAt、queueWaitMs、executionMs、endToEndMs。
- Spark applicationId、jobId、stageId、partitionId、taskAttemptId、Executor ID；拿不到的字段明确为空，不伪造。
- 每轮 round、耗时、返回的过期数量、安全重试次数及异常阶段。
- completionState、计数是否完整、是否从持久化结果恢复；UNKNOWN 对旧 ActionStatus 映射为 Failed，并以新增字段区分。

现有 `tasks` 代表清理轮次，不改成 Spark Task 个数；Spark attempt 信息使用独立字段。结果统计按 workId 去重，不用 accumulator 作为成功数量或副作用恰好执行一次的依据。

每表 Job description 保留当前分支格式和最终保留参数；Executor 日志也输出表／轮次／attempt。不能假定同一 Spark Job 内每个 Task 能拥有不同的 Driver job description。

原生返回数量沿用 `deletedSnapshotsTotal` 字段，但注明它是被确认调用的返回值累计，不是精确文件删除审计。结果未知时标记计数不完整，不能把缺失的进度显示为 0 次删除。

日志与状态记录不保存完整敏感 options、token、keytab 或口令；逐轮日志实时输出，Driver 内存中的历史详情必须有界，并给出截断标志和累计计数。

本机耗时使用单调时钟计算；跨 Driver／Executor 的排队时间优先使用 Spark 调度事件。未经时钟同步验证，不直接用两台机器的墙上时钟相减生成精确延迟。

## 9. 项目结构和编码约束

| 位置 | 职责 |
|---|---|
| `amoro-producer/src/main/scala/org/apache/amoro/producer/core/ProducerApp.scala` | 接入专用执行路径、汇总语义；其他 action 不改变 |
| `.../paimon/PaimonExpireSnapshotsAction.scala` | CLI、保留策略解析及 Driver 兼容路径 |
| `.../paimon/PaimonSqlRunner.scala` | Driver 端原生表／Procedure 能力访问，不发送到 Executor |
| 拟新增 `.../paimon/PaimonSnapshotExpirationExecutor.scala` | 纯 Executor 原生清理逻辑 |
| 拟新增 `.../paimon/PaimonSnapshotExpirationCoordinator.scala` | Driver 有界提交与结果汇总 |
| 拟新增 `.../paimon/SnapshotExpirationStateStore.scala` | 占用、结果、重复执行及异常恢复契约 |
| `amoro-producer/src/test/scala/org/apache/amoro/producer/paimon/` | 策略、集成、多 JVM 和故障测试 |
| `amoro-producer/README.md` | 模式、参数、生效策略、失败和恢复说明 |

遵守 Java 8 源码兼容、Scala 2.12、Apache 许可证头和现有 scalafmt 规则。使用不可变、可序列化工作描述，示意：

```scala
// 类型仅为接口示意，不代表已实现。
final case class SnapshotExpirationWork(
    runId: String,
    workId: String,
    tableKey: String,
    table: FileStoreTable,
    policy: EffectiveRetentionPolicy)
  extends Serializable
```

原生过期调用只存在于 Executor runner；Driver mode 的旧路径独立保留。状态存储访问不能包在吞异常后继续删除的逻辑中。

## 10. 验收与测试策略

新测试使用 JUnit 5，沿用现有 Producer 测试布局。`local[2]` 可验证基本控制流，不能证明跨 JVM、序列化、认证和 Executor 丢失恢复正确。

| ID | 场景 | 必须满足的结果 |
|---|---|---|
| A01 | 未传 executionMode | 仍走原 Driver 路径，既有测试不变 |
| A02 | 表配置完整／部分／缺失 | 两种模式的最终保留策略相同；表 expire.limit 覆盖 CLI 550 |
| A03 | 配置非法或读取耗尽 | 删除前失败，不降低保留要求 |
| A04 | 多表、并发上限 2 | 至少两个不同表有真实执行重叠，最多两个表 Job 在途 |
| A05 | Executor 位置验证 | 原生清理日志含非 Driver Executor ID；Driver 无清理删除调用 |
| A06 | 多轮清理与持续写入 | 使用有效 maxDeletes 续轮；不隐式固定最新快照或时间边界 |
| A07 | 原生结果对照 | 相同初始表副本和固定时钟下与 SQL 路径保留快照集合一致，存活数据可读 |
| A08 | Tag、Consumer、changelog 及合并后文件 | 保护语义与当前 1.3.2 Procedure 一致，无新误删 |
| A09 | 同表重复 Task／重复 application | 只有一个获准进入原生清理；别名不绕过互斥 |
| A10 | 完成后丢失返回结果 | 重放读取持久化成功记录，不再次清理，结果不重复统计 |
| A11 | 删除中途失联／中断 | 其他 attempt 不接管，状态未知且占用保留，最终非零退出 |
| A12 | 状态文件部分写入／存储失败 | 不把损坏记录当作空闲；无可靠资格则不删除 |
| A13 | 原子占用竞争及终态发布竞态 | 多 JVM 压力下无双持有者；获取后复查 workId 终态 |
| A14 | continueOnTableFailure true／false | 分别继续新表／停止新派发，在途任务和 notExecuted 正确统计 |
| A15 | Paimon 0.9.0 | Driver 既有保留策略及时区用例通过，Executor 请求在删除前拒绝 |
| A16 | Hive Catalog、HDFS／对象存储权限 | Executor 与 Driver 目标和身份一致，无凭据泄漏；具体后端分别验收 |
| A17 | 类冲突、缺少依赖、不同运行版本 | 删除前明确失败，报告实际版本，不自动切换算法 |
| A18 | 用户取消／Driver 崩溃及重启 | 已知结果可回读，未知工作不自动恢复删除 |
| A19 | 真实 YARN 多 Executor 运行 | 核验 JAR 指纹、最终 application 状态及表数据，不能只看 UI 成功 |
| A20 | 性能与存储压力 | 达到第 11 节门槛，无新增错误、限流或明显 GC 问题 |

必须特别注入“底层删除 Future 尚未完成但上层已抛异常”的故障，验证不会错误释放占用或再次删除。

### 10.1 实施阶段命令

以下是后续实施时的验证命令，本次文档提交没有运行这些测试。profile 切换使用 clean；具体 JDK 及 Spark 模块访问参数需沿用已验证的对应运行环境。

```bash
# 仓库根目录：现有保留策略回归
./mvnw -pl amoro-producer clean test -Pproducer-paimon-1.3.2 \
  -Dtest=PaimonExpireSnapshotsActionTest,PaimonExpireSnapshotsIntegrationTest,ProducerAppTest,LakehouseProducerTest

# 实现完成后：模块全量测试，包含新增分布式测试
./mvnw -pl amoro-producer clean test -Pproducer-paimon-1.3.2

# 独立验证旧组合；构建产物会被覆盖
./mvnw -pl amoro-producer clean test -Pproducer-paimon-0.9.0

# 格式检查与打包
./mvnw -pl amoro-producer validate -Pproducer-paimon-1.3.2
./mvnw -pl amoro-producer clean package -Pproducer-paimon-1.3.2 -DskipTests
```

Surefire 配置允许指定测试不存在时不失败，必须检查报告实际执行的类、数量、失败和跳过，不能只凭命令退出码宣称通过。

实现后应用参数示例（不是当前可执行能力）：

```text
--format paimon --action expire-snapshots --catalogName paimon
--databaseName sl_oki_prod --retryTimes 3 --continueOnTableFailure true
--retainMax 20 --retainMin 10 --maxDeletes 550
--procedureOptions file-operation.thread-num=32,snapshot.expire.execution-mode=sync
--executionMode executor --tableConcurrency 2
--executionStatePath hdfs://<nameservice>/<shared-producer-control-directory>
```

Spark 提交侧显式禁用 speculation，首版使用 maxFailures=1 和固定 Executor 数；JAR、catalog、warehouse、HDFS 配置及认证沿用经核对的部署模板。本 Spec 未获得完整生产 spark-submit 模板，不伪造可直接用于生产的资源和身份参数。

## 11. 性能验证与发布边界

建立三组对照：现有 Driver 串行、Executor 并发 2、Executor 并发 4。第一轮不同时改动文件线程数、保留参数或 maxDeletes。资源配置、实际池规模和存储总请求量须记录；更多 JVM 产生的容量提升不能被描述为算法本身加速。

- 每组至少 5 次可比较运行，报告整库耗时的中位数和范围，不用少量样本声称可靠的运行级 p95。
- 删除测试必须使用相同初始文件／快照状态的独立副本或可重建数据集，不能拿已经清理后的第二次执行与第一次比较。
- 区分冷／热缓存，记录写入速率、积压快照量、Tag／Consumer 情况、Driver／Executor CPU、堆及 GC、文件系统延迟和限流。
- 覆盖大量小表、均衡积压表、少数大表加许多小表、持续写入四类负载。
- 收集表级排队时间及执行时间分布，单表最慢耗时，元数据读取与文件删除的瓶颈证据。

建议上线门槛：正确性和故障验收全部通过；代表性多表负载整库耗时中位数较串行下降至少 20%，收益超过基线波动；无新增存储错误／限流，存储 p95 RPC 延迟劣化不超过 10%。这些是拟定验收门槛，不是已测收益，需结合环境 SLO 评审。

若存储已饱和、均为极小任务或单个超大表占主导，允许性能假设不成立；此时不能仅因实现完成就启用 Executor 模式。

回退通过显式使用 driver 模式完成，但必须先确认旧 Executor 清理已停止、相关占用已核验，不能在 UNKNOWN 时启动旧 Driver 绕过互斥。持久化控制记录保留用于追溯，不随回退删除。

## 12. 操作边界与待确认事项

始终执行：版本和身份预检、参数策略一致性校验、同表互斥、结构化结果、日志脱敏、真实多 JVM 故障测试。

需要评审确认：引入 HDFS 控制目录和人工解除 UNKNOWN 的运维流程；扩展 Executor 模式到 0.9.0／其他版本；改变保留默认值；增加外部协调服务或运行时依赖。

禁止：未知状态自动抢占、同表并行分段过期、删除过程中盲目重试、伪造成功／精确删除数量、以过期租约证明旧进程已停、把 local 模式通过当作集群验收。

进入实现计划前需要确认：

1. 是否接受首版默认 driver、显式启用 executor，且只支持 1.3.2 / 3.5.4。
2. 是否接受共享 HDFS 控制目录、无自动超时接管及 UNKNOWN 人工恢复；若环境已有等价持久化协调设施，替换前需核实其原子性和失联行为。
3. 是否接受首版完整表循环、暂不做轮转；若要求最长等待时间，应另行定义每表工作预算和续调度语义。
4. 生产实际版本、认证、存储类型、可用 Executor 资源及可用于恢复的控制目录。
5. 性能验收的 20% 耗时改善与 10% RPC 延迟劣化上限是否符合目标环境要求。

本文已明确推荐方向和安全边界；仍需通过上述评审及原型／故障验证，不能宣称当前已达到生产执行的绝对安全保证。

## 13. 源码依据

本仓库路径以第 1 节 rebase 后提交为基线：

- [表级编排](amoro-producer/src/main/scala/org/apache/amoro/producer/core/ProducerApp.scala)：串行循环及退出汇总。
- [过期 Action](amoro-producer/src/main/scala/org/apache/amoro/producer/paimon/PaimonExpireSnapshotsAction.scala)：表优先策略、轮次、重试及描述。
- [SQL Runner](amoro-producer/src/main/scala/org/apache/amoro/producer/paimon/PaimonSqlRunner.scala)：Spark Catalog 访问与 collect。
- [现有集成测试](amoro-producer/src/test/scala/org/apache/amoro/producer/paimon/PaimonExpireSnapshotsIntegrationTest.scala)：真实保留策略、时区及描述验证。
- [模块 POM](amoro-producer/pom.xml)：Producer 独立版本与构建 profile。
- [Paimon Procedure](https://github.com/apache/paimon/blob/release-1.3.2/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExpireSnapshotsProcedure.java)：table.copy、ExpireConfig 和同步 expire 调用。
- [原生过期实现](https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/table/ExpireSnapshotsImpl.java)：保护规则及阶段顺序。
- [文件删除基类](https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/operation/FileDeletionBase.java)：删除 Future、等待以及失败边界。
- [文件操作线程池](https://github.com/apache/paimon/blob/release-1.3.2/paimon-common/src/main/java/org/apache/paimon/utils/FileOperationThreadPool.java)：JVM 内共享和扩容行为。
- [Spark 分布式孤儿文件清理](https://github.com/apache/paimon/blob/release-1.3.2/paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/procedure/SparkOrphanFilesClean.scala)：Executor 原生文件操作的参考，不构成过期算法可安全重试的证明。
- [Spark 3.5.4 配置源码](https://github.com/apache/spark/blob/v3.5.4/docs/configuration.md)：speculation、task.maxFailures 与调度参数。
