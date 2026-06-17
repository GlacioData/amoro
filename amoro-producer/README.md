<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Amoro Producer Paimon 清理任务

`amoro-producer` 提供可通过 `spark-submit` 提交的 Paimon 清理任务：

- Snapshot 清理入口类为 `org.apache.amoro.producer.PaimonExpireSnapshots`，任务会通过 Spark SQL 执行 `CALL sys.expire_snapshots`。
- 孤儿文件清理入口类为 `org.apache.amoro.producer.PaimonRemoveOrphanFiles`，任务会通过 Spark SQL 执行 `CALL <catalog>.sys.remove_orphan_files`。

## 打包

```bash
./mvnw -pl amoro-producer package -DskipTests
```

打包产物：

```text
amoro-producer/target/amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar
```

## Snapshot 清理参数说明

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `--databaseName` | 否 | 无 | 数据库名。仅传该参数时，任务会扫描并清理该库下全部非临时表。 |
| `--tableName` | 否 | 无 | 表名。与 `--databaseName` 同时传入时，只清理指定表；单独传入时必须使用 `db.table` 格式。 |
| `--retainMax` | 否 | `20` | 传给 `expire_snapshots` 的 `retain_max`。 |
| `--retainMin` | 否 | `10` | 传给 `expire_snapshots` 的 `retain_min`。 |
| `--maxDeletes` | 否 | `550` | 单轮最多删除的 snapshot 数量。若返回删除数大于等于该值，则继续下一轮清理。 |
| `--retryTimes` | 否 | `3` | 单轮 `expire_snapshots` SQL 失败后的最大重试次数。 |
| `--continueOnTableFailure` | 否 | `true` | 单表清理失败后是否继续处理后续表。 |
| `--procedureOptions` | 否 | `file-operation.thread-num=32,snapshot.expire.execution-mode=sync` | 传给 `expire_snapshots` 的 `options` 字符串。传空字符串时不拼接 `options` 参数。 |
| `--help` / `-h` | 否 | 无 | 打印帮助信息。 |

参数组合规则：

| 参数组合 | 行为 |
|----------|------|
| `--databaseName db` | 清理 `db` 下所有非临时表。 |
| `--databaseName db --tableName t1` | 只清理 `db.t1`。 |
| `--tableName db.t1` | 只清理 `db.t1`。 |
| `--tableName t1` | 非法参数，任务打印 usage 并以退出码 `2` 结束。 |
| `--databaseName db --tableName db.t1` | 非法参数，任务打印 usage 并以退出码 `2` 结束。 |

## 孤儿文件清理参数说明

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `--catalogName` | 否 | `paimon` | Paimon catalog 名称。任务会使用该 catalog 扫表并调用 `remove_orphan_files`。 |
| `--databaseName` | 否 | 无 | 数据库名。仅传该参数时，任务会扫描并清理该库下全部非临时表。 |
| `--tableName` | 否 | 无 | 表名。与 `--databaseName` 同时传入时，只清理指定表；单独传入时必须使用 `db.table` 格式。 |
| `--parallelism` | 否 | `10` | 传给 `remove_orphan_files` 的 `parallelism`。 |
| `--mode` | 否 | `distributed` | 传给 `remove_orphan_files` 的 `mode`。 |
| `--retryTimes` | 否 | `3` | 单表 `remove_orphan_files` SQL 失败后的最大执行次数。 |
| `--continueOnTableFailure` | 否 | `true` | 单表清理失败后是否继续处理后续表。 |
| `--help` / `-h` | 否 | 无 | 打印帮助信息。 |

孤儿文件清理的目标表参数组合规则与 Snapshot 清理保持一致。

## Snapshot 清理 Spark Submit Demo

以下示例基于 YARN cluster 模式提交，主类替换为
`org.apache.amoro.producer.PaimonExpireSnapshots`，应用 JAR 替换为
`amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar`。示例中显式声明
Paimon catalog，并通过 `spark.sql.defaultCatalog=paimon` 让 `--databaseName`
按 Paimon catalog 下的库解析。

### 按数据库清理

```bash
./spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 8g \
    --driver-cores 4 \
    --executor-memory 4g \
    --executor-cores 4 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=2 \
    --conf spark.dynamicAllocation.initialExecutors=1 \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.driver.memoryOverhead=2g \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.cbo.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=512m \
    --conf spark.default.parallelism=200 \
    --conf spark.locality.wait=0s \
    --conf spark.sql.crossJoin.enabled=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true \
    --conf spark.sql.fuse.unionAllOnJoin.enabled=true \
    --conf spark.sql.optimizer.runtime.bloomFilter.enabled=true \
    --conf spark.sql.optimizer.enableMergeScalarAggsInInnerJoin=true \
    --conf spark.sql.optimizer.pushdownAggregateBelowJoin=true \
    --conf spark.sql.optimizer.inferDistinctFromIntersect=true \
    --conf spark.sql.optimizer.groupSplitsByLocation=false \
    --conf spark.sql.adaptive.amend.join.selection.enabled=true \
    --conf spark.sql.mergeScalaSubquery.pullupAggFilter=true \
    --conf spark.sql.execution.optimizeExpand=true \
    --conf spark.sql.execution.optimizeExpand.ratio=5 \
    --conf spark.sql.legacy.ctePrecedencePolicy=LEGACY \
    --conf spark.sql.auto.reused.cte.enabled=true \
    --conf spark.sql.auto.clear.cte.cache.enabled=true \
    --conf spark.locality.wait.node=0s \
    --conf spark.sql.sources.ignoreDataLocality=true \
    --conf spark.driver.host=10.89.56.103 \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
    --conf spark.sql.catalog.paimon.type=hive \
    --conf spark.sql.catalog.paimon.warehouse=hdfs://slcluster01/hive_warehouse \
    --conf spark.hadoop.net.topology.script.file.name=/dev/null \
    --conf spark.sql.defaultCatalog=paimon \
    --keytab /data/work/hadoop-shopline/etc/hadoop/sljdp.keytab \
    --jars /tmp/paimon-hive-connector-3.1-1.5-SNAPSHOT.jar,/tmp/paimon-spark-3.5_2.12-1.5-SNAPSHOT.jar \
    --principal sljdp@SLCLUSTER.COM \
    --class org.apache.amoro.producer.PaimonExpireSnapshots \
    /data/work/frame/amoro-0.9-SNAPSHOT/plugin/producer/amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar \
    --databaseName sl_oki_test \
    --retainMax 20 \
    --retainMin 10 \
    --maxDeletes 550 \
    --retryTimes 3 \
    --continueOnTableFailure true \
    --procedureOptions file-operation.thread-num=32,snapshot.expire.execution-mode=sync
```

### Snapshot 按单表清理

```bash
./spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 8g \
    --driver-cores 4 \
    --executor-memory 4g \
    --executor-cores 4 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=2 \
    --conf spark.dynamicAllocation.initialExecutors=1 \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.driver.memoryOverhead=2g \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.cbo.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=512m \
    --conf spark.default.parallelism=200 \
    --conf spark.locality.wait=0s \
    --conf spark.sql.crossJoin.enabled=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true \
    --conf spark.sql.fuse.unionAllOnJoin.enabled=true \
    --conf spark.sql.optimizer.runtime.bloomFilter.enabled=true \
    --conf spark.sql.optimizer.enableMergeScalarAggsInInnerJoin=true \
    --conf spark.sql.optimizer.pushdownAggregateBelowJoin=true \
    --conf spark.sql.optimizer.inferDistinctFromIntersect=true \
    --conf spark.sql.optimizer.groupSplitsByLocation=false \
    --conf spark.sql.adaptive.amend.join.selection.enabled=true \
    --conf spark.sql.mergeScalaSubquery.pullupAggFilter=true \
    --conf spark.sql.execution.optimizeExpand=true \
    --conf spark.sql.execution.optimizeExpand.ratio=5 \
    --conf spark.sql.legacy.ctePrecedencePolicy=LEGACY \
    --conf spark.sql.auto.reused.cte.enabled=true \
    --conf spark.sql.auto.clear.cte.cache.enabled=true \
    --conf spark.locality.wait.node=0s \
    --conf spark.sql.sources.ignoreDataLocality=true \
    --conf spark.driver.host=10.89.56.103 \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
    --conf spark.sql.catalog.paimon.type=hive \
    --conf spark.sql.catalog.paimon.warehouse=hdfs://slcluster01/hive_warehouse \
    --conf spark.hadoop.net.topology.script.file.name=/dev/null \
    --conf spark.sql.defaultCatalog=paimon \
    --keytab /data/work/hadoop-shopline/etc/hadoop/sljdp.keytab \
    --jars /tmp/paimon-hive-connector-3.1-1.5-SNAPSHOT.jar,/tmp/paimon-spark-3.5_2.12-1.5-SNAPSHOT.jar \
    --principal sljdp@SLCLUSTER.COM \
    --class org.apache.amoro.producer.PaimonExpireSnapshots \
    /data/work/frame/amoro-0.9-SNAPSHOT/plugin/producer/amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar \
    --databaseName sl_oki_test \
    --tableName t_order \
    --retainMax 20 \
    --retainMin 10 \
    --maxDeletes 550 \
    --retryTimes 3 \
    --continueOnTableFailure true \
    --procedureOptions file-operation.thread-num=32,snapshot.expire.execution-mode=sync
```

## 孤儿文件清理 Spark Submit Demo

孤儿文件清理入口显式接收 `--catalogName`，示例中使用 `paimon`。如果生产环境
catalog 名称不同，只需要调整 `--catalogName` 以及对应的
`spark.sql.catalog.<catalog>` 配置。

### 孤儿文件按数据库清理

```bash
./spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 8g \
    --driver-cores 4 \
    --executor-memory 4g \
    --executor-cores 4 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=2 \
    --conf spark.dynamicAllocation.initialExecutors=1 \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.driver.memoryOverhead=2g \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.cbo.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=512m \
    --conf spark.default.parallelism=200 \
    --conf spark.locality.wait=0s \
    --conf spark.sql.crossJoin.enabled=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true \
    --conf spark.sql.fuse.unionAllOnJoin.enabled=true \
    --conf spark.sql.optimizer.runtime.bloomFilter.enabled=true \
    --conf spark.sql.optimizer.enableMergeScalarAggsInInnerJoin=true \
    --conf spark.sql.optimizer.pushdownAggregateBelowJoin=true \
    --conf spark.sql.optimizer.inferDistinctFromIntersect=true \
    --conf spark.sql.optimizer.groupSplitsByLocation=false \
    --conf spark.sql.adaptive.amend.join.selection.enabled=true \
    --conf spark.sql.mergeScalaSubquery.pullupAggFilter=true \
    --conf spark.sql.execution.optimizeExpand=true \
    --conf spark.sql.execution.optimizeExpand.ratio=5 \
    --conf spark.sql.legacy.ctePrecedencePolicy=LEGACY \
    --conf spark.sql.auto.reused.cte.enabled=true \
    --conf spark.sql.auto.clear.cte.cache.enabled=true \
    --conf spark.locality.wait.node=0s \
    --conf spark.sql.sources.ignoreDataLocality=true \
    --conf spark.driver.host=10.89.56.103 \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
    --conf spark.sql.catalog.paimon.type=hive \
    --conf spark.sql.catalog.paimon.warehouse=hdfs://slcluster01/hive_warehouse \
    --conf spark.hadoop.net.topology.script.file.name=/dev/null \
    --conf spark.sql.defaultCatalog=paimon \
    --keytab /data/work/hadoop-shopline/etc/hadoop/sljdp.keytab \
    --jars /tmp/paimon-hive-connector-3.1-1.5-SNAPSHOT.jar,/tmp/paimon-spark-3.5_2.12-1.5-SNAPSHOT.jar \
    --principal sljdp@SLCLUSTER.COM \
    --class org.apache.amoro.producer.PaimonRemoveOrphanFiles \
    /data/work/frame/amoro-0.9-SNAPSHOT/plugin/producer/amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar \
    --catalogName paimon \
    --databaseName sl_oki_test \
    --parallelism 10 \
    --mode distributed \
    --retryTimes 3 \
    --continueOnTableFailure true
```

### 孤儿文件按单表清理

```bash
./spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 8g \
    --driver-cores 4 \
    --executor-memory 4g \
    --executor-cores 4 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=2 \
    --conf spark.dynamicAllocation.initialExecutors=1 \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.driver.memoryOverhead=2g \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.cbo.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=512m \
    --conf spark.default.parallelism=200 \
    --conf spark.locality.wait=0s \
    --conf spark.sql.crossJoin.enabled=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true \
    --conf spark.sql.fuse.unionAllOnJoin.enabled=true \
    --conf spark.sql.optimizer.runtime.bloomFilter.enabled=true \
    --conf spark.sql.optimizer.enableMergeScalarAggsInInnerJoin=true \
    --conf spark.sql.optimizer.pushdownAggregateBelowJoin=true \
    --conf spark.sql.optimizer.inferDistinctFromIntersect=true \
    --conf spark.sql.optimizer.groupSplitsByLocation=false \
    --conf spark.sql.adaptive.amend.join.selection.enabled=true \
    --conf spark.sql.mergeScalaSubquery.pullupAggFilter=true \
    --conf spark.sql.execution.optimizeExpand=true \
    --conf spark.sql.execution.optimizeExpand.ratio=5 \
    --conf spark.sql.legacy.ctePrecedencePolicy=LEGACY \
    --conf spark.sql.auto.reused.cte.enabled=true \
    --conf spark.sql.auto.clear.cte.cache.enabled=true \
    --conf spark.locality.wait.node=0s \
    --conf spark.sql.sources.ignoreDataLocality=true \
    --conf spark.driver.host=10.89.56.103 \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
    --conf spark.sql.catalog.paimon.type=hive \
    --conf spark.sql.catalog.paimon.warehouse=hdfs://slcluster01/hive_warehouse \
    --conf spark.hadoop.net.topology.script.file.name=/dev/null \
    --conf spark.sql.defaultCatalog=paimon \
    --keytab /data/work/hadoop-shopline/etc/hadoop/sljdp.keytab \
    --jars /tmp/paimon-hive-connector-3.1-1.5-SNAPSHOT.jar,/tmp/paimon-spark-3.5_2.12-1.5-SNAPSHOT.jar \
    --principal sljdp@SLCLUSTER.COM \
    --class org.apache.amoro.producer.PaimonRemoveOrphanFiles \
    /data/work/frame/amoro-0.9-SNAPSHOT/plugin/producer/amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar \
    --catalogName paimon \
    --databaseName sl_oki_test \
    --tableName t_order \
    --parallelism 10 \
    --mode distributed \
    --retryTimes 3 \
    --continueOnTableFailure true
```

## 运行注意事项

Spark 运行环境必须提前配置好 Paimon catalog，否则 `SHOW TABLES` 或
`CALL sys.expire_snapshots`、`CALL <catalog>.sys.remove_orphan_files` 会在运行期失败。

如果运行环境已经通过 Spark 安装目录或平台层提供 Paimon 相关 JAR，可以直接提交
`amoro-producer-0.9-SNAPSHOT.jar`；如果希望单 JAR 提交，使用
`amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar`。

`spark.dynamicAllocation.initialExecutors` 建议不要大于
`spark.dynamicAllocation.maxExecutors`。上面的示例保留了原始提交命令中的资源参数，实际提交前请按集群策略调整。
