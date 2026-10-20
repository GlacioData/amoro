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

# Producer 分布式快照过期：实施与验证记录

日期：2026-10-20；分支：`hotifx/producer-snapshot-retention`。依据已审查的 Spec/Plan，实现代码基线为 `07f7791cc`。实现提交：`3ec863a60`（通用批执行与累计计数）、`037e7d3ca`（Paimon Executor 路径）。本记录区分本地功能验证与生产验收；未执行生产清理。

## 实现结果

- 保留默认 Driver SQL 路径及原有保留参数优先级；显式增加 `--executionMode executor`、`--tableConcurrency`（默认 4）。
- Driver 负责有界准备、按物理表去重、提交及汇总。每表只提交一个单分区清理 Job，一次 `collectAsync`；Task 调用 Paimon 原生 API，并在同一个 Task 内完成该表全部轮次。
- 没有 executionStatePath、持久化目录、跨作业锁或授权 RPC。提交门禁只存在于当前 Driver 内存，用于避免取消与迟到提交的竞态。
- Driver 与 Task 独立检查 Spark 3.5.4、Paimon 1.3.2 的实际类/JAR 来源、必要 API、speculation=false、maxFailures=1；Task/Stage 双重 attempt 门禁防止重复进入删除。
- 只有只读准备使用 retryTimes；原生异常不重试，失去确定性时返回 Failed/UNKNOWN，保留已确认下界。最近 100 轮明细与 Long 累计计数分开。

## 源码依据

| 依据 | 本次采用的规则 |
|---|---|
| [Paimon 1.3.2 ExpireSnapshotsProcedure](https://github.com/apache/paimon/blob/release-1.3.2/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExpireSnapshotsProcedure.java) | table.copy(options) 后创建原生过期对象，使用 snapshot 四个配置项，保持 changelog 默认行为 |
| [Paimon 1.3 快照维护](https://paimon.apache.org/docs/1.3/maintenance/manage-snapshots/) | 快照保留、Tag/Consumer 保护交由原生实现 |
| [Paimon 1.3.2 ExpireSnapshotsImpl](https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/table/ExpireSnapshotsImpl.java) | 每轮读取当前时间/快照；不增加启动时固定边界 |
| [Spark 3.5.4 应用内调度](https://github.com/apache/spark/blob/v3.5.4/docs/job-scheduling.md#scheduling-within-an-application) | 不同 Driver 线程可提交并发 Jobs |
| [TaskContext](https://github.com/apache/spark/blob/v3.5.4/core/src/main/scala/org/apache/spark/TaskContext.scala) | 首次 Task 和 Stage attempt 均为 0，不能将 taskAttemptId 当作重试次数 |
| [AsyncRDDActions](https://github.com/apache/spark/blob/v3.5.4/core/src/main/scala/org/apache/spark/rdd/AsyncRDDActions.scala)、[FutureAction](https://github.com/apache/spark/blob/v3.5.4/core/src/main/scala/org/apache/spark/FutureAction.scala) | collectAsync 单次提交，登记可取消句柄后在监视器外等待；不重新收集 RDD |
| [Spark 配置](https://github.com/apache/spark/blob/v3.5.4/docs/configuration.md) | 禁用推测、限制失败重试；仍需额外 attempt 门禁 |
| [Java CodeSource](https://docs.oracle.com/javase/8/docs/api/java/security/CodeSource.html#getLocation--) | 从定义实际类的同一个 JAR 读取 Maven 版本资源，避免随机 classpath 资源误判 |

本地 Paimon 源码按 `release-1.3.2` / `16faabbddbe30477a7e2c3bacde5e541c8f1cf9c` 读取，没有将另一个 Paimon 工作分支的修改视为运行事实。

## 测试与构建

本机 macOS arm64，Maven 3.9.11，JDK 11.0.28；源码目标 Java 8。JDK 11 是本次验证 JVM，不声明已覆盖全部 JDK。两个 profile 切换均执行 clean，避免混用 Scala 编译缓存。

```bash
# 旧 Driver 兼容矩阵：Spark 3.2.1 / Paimon 0.9.0
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
./mvnw -pl amoro-producer clean package -Pproducer-paimon-0.9.0

# 新矩阵：Spark 3.5.4 / Paimon 1.3.2，含独立 Executor JVM
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 SPARK_TESTING=1 SPARK_SCALA_VERSION=2.12 \
./mvnw -pl amoro-producer clean package -Pproducer-paimon-1.3.2 \
  -Dproducer.snapshot.clusterTest=true -Djacoco.skip=true
```

最终矩阵均为 BUILD SUCCESS：

| 组合 | 用例数 | 失败/错误 | 跳过 | 证据 |
|---|---:|---:|---:|---|
| Spark 3.5.4 / Paimon 1.3.2（含两个 Executor JVM） | 229 | 0 / 0 | 0 | [Surefire 摘要](producer-snapshot-validation/profile-1.3.2.json) |
| Spark 3.2.1 / Paimon 0.9.0 | 229 | 0 / 0 | 26 | [Surefire 摘要](producer-snapshot-validation/profile-0.9.0.json) |

0.9 的 26 项跳过包含整个 1.3 compaction 类的 18 项、Executor 专属集成 7 项、原生 1.3/SQL 对照 1 项。其余 203 项实际运行通过，包含旧版快照 Driver 与纯调度/循环单测。

单独不启用 clusterTest 的常规 1.3.2 全量测试首轮为 227 项、0 失败、0 错误、1 跳过；后续增加了 1 个取消回归和 1 个合并/changelog 对照用例。

多 JVM fixture 使用 Spark 源码测试启动约定 `SPARK_TESTING=1` / `SPARK_SCALA_VERSION=2.12`。它不依赖本机已安装 Spark 发行包。该测试禁用 JaCoCo：Driver 插桩而子 Executor JVM 不插桩时，曾出现 `GenericRowWithSchema` 序列化 UID 不一致；普通本地测试已独立通过 JaCoCo 配置。此设置仅用于测试启动，不是生产提交要求。

0.9 首次全量测试有 10 项失败，均来自既有 1.3 compaction 测试类向 0.9 发送 `compact_strategy`。已为整个 1.3 测试类增加 profile 前置条件；旧版 `PaimonCompactLegacyIntegrationTest` 仍执行。该前置条件也跳过 1.3 类中辅助测试，具体数量以最终矩阵为准；没有修改 compaction 生产逻辑。

按仓库要求执行 `dev/reformat`，后续变更另跑模块级 `spotless:apply`。包装脚本未将 MAVEN_ARGS 的 reactor 限制生效，但检查已跟踪文件 diff 未产生本任务之外的变更；脚本已有 spark-3.3 profile 不存在警告，实际旧版测试使用明确的 Producer 0.9 profile。随后检查 `git diff --check`，并执行 `./mvnw -pl amoro-producer validate -Pproducer-paimon-1.3.2`，均通过。

## 关键行为证据

| 断言 | 实际测试 |
|---|---|
| 默认 Driver、参数优先级、时间默认、时区和重试保持 | PaimonExpireSnapshotsActionTest / PaimonExpireSnapshotsIntegrationTest |
| 原生与 SQL 返回数、保留快照、存活数据一致 | nativeExpirationMatchesSqlAndRetainsReadableData |
| 单表所有轮次同 Task，不使用 CLI 550 替代表的 limit=1 | cliBatchPreservesPerTablePolicyAndSameAttemptAcrossRounds |
| 真实 FileStoreTable/FileIO Java 序列化往返及物理身份校验 | realTableAndFileIOSurviveSerializationWithoutDriverContext |
| 一个 Executor 两个 Task 同时执行，策略不串用 | twoTasksOverlapInOneExecutorAndKeepTheirOwnPolicies（latch） |
| 2 个独立 Executor JVM、每 JVM 2 个并发 Task、4 表各 3 轮 | multiJvmTasksExecuteOnTwoExecutors（测试专用临时 barrier） |
| Tag、Consumer 保护，以及 tag/snapshot/latest 读取 | consumerAndTagProtectOldDataAcrossOverwriteSnapshots |
| 主键表显式 compact 后，input changelog 配置下 SQL 与 Executor 保留集合和存活数据一致 | compactedPrimaryKeyAndChangelogTablesMatchDriverExpiration |
| 已清理后注入结果丢失，Spark 实际重试得到 UNKNOWN，原生门禁拒绝重入 | actualSparkReplayDoesNotReenterNativeCleanupAfterResultLoss；仅测试 master 使用 local[2,2] |
| 原生异常与仍在运行的文件 Future 不重试，取消停止续轮 | PaimonSnapshotTaskTest |
| 150+ 轮明细截断不影响累计值，超过 Int 的统计不溢出 | PaimonSnapshotTaskTest / ActionResultTest |
| 有界准备/执行、停止/继续、去重、基础设施失败、中断及单次回调 | PaimonSnapshotExecutorTest |
| 取消早于提交、提交中取消、关闭后拒绝提交、嵌套中断及 group 取消 | PaimonSnapshotSubmissionTest |
| Job/Task 关联、缺失/迟到事件、不同 Job 的排队时间 | PaimonSnapshotObservabilityTest |

不把模拟 Task/Stage attempt 单测称为真实 Stage 故障恢复；真实重试测试仅覆盖 Task 结果丢失后的重试。两个独立 JVM 证明本地文件系统的序列化与并发，不证明 Kerberos/token 或对象存储插件兼容。

## 变异验证与独立审查

[变异证据](producer-snapshot-validation/mutations.json)：分别反转 Task attempt 条件、将续轮 `>=` 改为 `>`、反转失败后停止补充条件，三个对应测试均出现断言失败。已逐次从原文恢复源码，最终测试使用恢复后的版本。

[实现审查](producer-snapshot-implementation-review.md) 完成多轮修复复核：部分成功后基础设施失败误报成功、取消与迟到提交、取消过程的中断传播均已闭环。审查者只做源码检查，未代替主线程执行测试。

## 尚未完成的验收

- 目标 YARN 下实际杀 Executor / Driver、存储故障和跨进程取消收敛。
- Hive、认证 HDFS、实际对象存储的身份、FileIO 插件、token 传递与权限。
- 持续写入且随后受控停写的真实表收敛；本地已覆盖同 Task 长循环与取消，但不声称覆盖持续写入集群行为。
- 四类负载下 Driver、并发 2/4 的重复测量及存储 RPC p95、限流、CPU/GC；不声明性能提升比例。

这些项目保留在任务清单中，阻止对应生产支持或性能达标声明，不阻止交付本次已授权的本地实现和单元测试。没有 push、上线或执行生产清理。

## 实际打包物校验

根 POM 原有 Shade 过滤会去掉 `META-INF/maven/**`；首次直接加载产物时检出版本元数据缺失。已仅在 Producer shade execution 覆盖此过滤，保留 Maven 元数据，同时保留父级签名、日志和其他排除项，未改变依赖版本。依据 [Shade 过滤规则](https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html) 与 [Maven 配置继承](https://maven.apache.org/pom.html)。

0.9 clean package 和测试先通过；修复过滤后运行 `package -Pproducer-paimon-0.9.0 -DskipTests` 重打包。1.3 最终 clean package 已包含该修复。独立 JVM 将 fat JAR 放在 classpath 首位，断言 Table.class 的 CodeSource 就是该产物，再调用实际版本读取和运行门禁：0.9.0 拒绝 Executor，1.3.2 接受。

产物 SHA256、大小和 smoke 输出分别见 [0.9.0](producer-snapshot-validation/package-0.9.0.json) / [1.3.2](producer-snapshot-validation/package-1.3.2.json)。该 smoke 不调用清理。最终 target 中保留 1.3.2 产物。

重复实际打包物 smoke（在对应 profile 完成 package 后）：

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
python3 tasks/producer-snapshot-validation/check_packaged_runtime.py 1.3.2
```
