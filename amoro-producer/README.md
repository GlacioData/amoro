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

# Amoro Lakehouse Producer

`amoro-producer` 提供统一的 Lakehouse Producer 入口：
`org.apache.amoro.producer.LakehouseProducer`。

当前支持 Paimon format，提交任务时必须指定：

- `--format paimon`
- `--action <action>`
- `--catalogName paimon`

`--catalogName` 的默认值是 `spark_catalog`。Paimon 生产脚本必须显式传入
`--catalogName paimon`，该参数用于整库扫描、`$options` 读取和 catalog-qualified
procedure 调用。

## 打包

```bash
./mvnw -pl amoro-producer package -DskipTests
```

打包产物：

```text
amoro-producer/target/amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar
```

## 通用参数

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `--format` | 是 | 无 | 表格式。Paimon 使用 `paimon`。 |
| `--action` | 是 | 无 | action 名称：`compact`、`compact-manifest`、`expire-snapshots`、`remove-orphan-files`。 |
| `--catalogName` | 否 | `spark_catalog` | Spark catalog 名称。Paimon 生产脚本应显式传 `paimon`。 |
| `--databaseName` | 否 | 无 | 数据库名。仅传该参数时，扫描该库下全部非临时表。 |
| `--tableName` | 否 | 无 | 表名。与 `--databaseName` 同时传入时只允许简单表名；单独传入时必须是 `db.table`。 |
| `--tableNameRegex` | 否 | 无 | 整库模式表名过滤正则，仅支持 `--databaseName` 且未指定 `--tableName`。 |
| `--retryTimes` | 否 | `3` | 重试次数。 |
| `--continueOnTableFailure` | 否 | `true` | 单表失败后是否继续处理后续表。 |
| `--help` / `-h` | 否 | 无 | 打印帮助信息。 |

## compact

执行 Paimon `compact` procedure。

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `--startBucket` | 否 | `0` | 仅 `--version 1.3` 生效；从哪个 bucket 下标开始执行。 |
| `--step` | 否 | `20` | 仅 `--version 1.3` 生效；每次 compact 的 bucket 数量，生成闭区间 bucket range，例如 `0-19`。 |
| `--compactStrategy` | 否 | `full` | 仅 `--version 1.3` 生效；传给 `compact` 的 `compact_strategy`。 |
| `--procedureOptions` | 否 | `target-file-size=256m` | 仅 `--version 1.3` 生效；传给 `compact` 的 `options` 字符串，空字符串表示不拼接。 |
| `--version` | 否 | `0.9` | 仅支持 `0.9` 和 `1.3`。`0.9` 使用 `partition_idle_time` SQL；`1.3` 使用分桶 SQL。 |
| `--partitionIdleTime` | 否 | `1d` | `--version 0.9` 时传给 `compact` 的 `partition_idle_time`。 |

示例：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.defaultCatalog=paimon \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=hdfs://warehouse/paimon \
  --class org.apache.amoro.producer.LakehouseProducer \
  amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar \
  --format paimon \
  --action compact \
  --version 1.3 \
  --catalogName paimon \
  --databaseName ods \
  --tableNameRegex '^ods_.*' \
  --startBucket 0 \
  --step 20 \
  --compactStrategy full \
  --procedureOptions target-file-size=256m
```

compact 按 `--version` 选择 SQL：

- `--version 0.9`（默认）不读取目标表 `$options`，不执行分桶逻辑，只执行：
  `CALL sys.compact(table => 'db.table', partition_idle_time => '1d')`。
- `--version 1.3` 时先读取目标表 `$options` 中的 `bucket` 配置；有效正整数 bucket 时，按 `--startBucket` 和 `--step`
  生成 bucket range，并调用带 `buckets`、`compact_strategy`、`options` 的
  `compact` SQL。
- `--version 1.3` 下 bucket 缺失、非整数、`<= 0` 或读取失败时，执行
  `CALL sys.compact(table => 'db.table')`。

其中 0.9 compact 和 1.3 的非 bucket compact 使用
`CALL sys.compact(...)`，依赖 Spark 的 `spark.sql.defaultCatalog` 指向 Paimon catalog。
生产提交时应设置 `--conf spark.sql.defaultCatalog=paimon`，并与 `--catalogName paimon`
保持一致。

## compact-manifest

执行 Paimon `compact_manifest` procedure。

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `--procedureOptions` | 否 | 空字符串 | 传给 `compact_manifest` 的 `options` 字符串，空字符串表示不拼接。 |

示例：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=hdfs://warehouse/paimon \
  --class org.apache.amoro.producer.LakehouseProducer \
  amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar \
  --format paimon \
  --action compact-manifest \
  --catalogName paimon \
  --databaseName ods \
  --procedureOptions file-operation.thread-num=16
```

## expire-snapshots

执行 Paimon `expire_snapshots` procedure。

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `--retainMax` | 否 | `20` | 传给 `expire_snapshots` 的 `retain_max`。 |
| `--retainMin` | 否 | `10` | 传给 `expire_snapshots` 的 `retain_min`。 |
| `--maxDeletes` | 否 | `550` | 单轮最多删除的 snapshot 数量；删除数达到该值会继续下一轮。 |
| `--procedureOptions` | 否 | `file-operation.thread-num=32,snapshot.expire.execution-mode=sync` | 传给 `expire_snapshots` 的 `options` 字符串，空字符串表示不拼接。 |

示例：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=hdfs://warehouse/paimon \
  --class org.apache.amoro.producer.LakehouseProducer \
  amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar \
  --format paimon \
  --action expire-snapshots \
  --catalogName paimon \
  --databaseName ods \
  --retainMax 20 \
  --retainMin 10 \
  --maxDeletes 550 \
  --procedureOptions file-operation.thread-num=32,snapshot.expire.execution-mode=sync
```

## remove-orphan-files

执行 Paimon `remove_orphan_files` procedure。

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `--parallelism` | 否 | `10` | 传给 `remove_orphan_files` 的 `parallelism`。 |
| `--mode` | 否 | `distributed` | 传给 `remove_orphan_files` 的 `mode`。 |

示例：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=hdfs://warehouse/paimon \
  --class org.apache.amoro.producer.LakehouseProducer \
  amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar \
  --format paimon \
  --action remove-orphan-files \
  --catalogName paimon \
  --databaseName ods \
  --parallelism 10 \
  --mode distributed
```

## 生产 spark-submit 示例

以下示例以 Paimon catalog `paimon`、数据库 `sl_oki_prod` 为例。`spark.driver.host`、
`spark.sql.catalog.paimon.warehouse`、`--keytab`、`--principal`、`--jars` 和 producer jar
路径需要替换为实际生产环境路径。

如果目标库表数量很多，建议在整库模式下增加 `--tableNameRegex`，只处理命中正则的表。
例如 `--tableNameRegex '^orders_.*'` 只处理 `sl_oki_prod` 下表名匹配该正则的表。

### expire-snapshots

整库快照过期：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --driver-cores 4 \
  --executor-memory 8g \
  --executor-cores 2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=2 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.yarn.queue=real_time_merge \
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
  --conf spark.driver.host=REPLACE_DRIVER_HOST \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=REPLACE_PAIMON_WAREHOUSE \
  --conf spark.hadoop.net.topology.script.file.name=/dev/null \
  --conf spark.sql.defaultCatalog=paimon \
  --keytab REPLACE_KEYTAB_FILE \
  --jars REPLACE_PAIMON_HIVE_CONNECTOR_JAR,REPLACE_PAIMON_SPARK_JAR \
  --principal REPLACE_PRINCIPAL \
  --class org.apache.amoro.producer.LakehouseProducer \
  REPLACE_AMORO_PRODUCER_JAR \
  --format paimon \
  --action expire-snapshots \
  --catalogName paimon \
  --databaseName sl_oki_prod \
  --retryTimes 3 \
  --continueOnTableFailure true \
  --retainMax 20 \
  --retainMin 10 \
  --maxDeletes 550 \
  --procedureOptions file-operation.thread-num=32,snapshot.expire.execution-mode=sync
```

只处理命中正则的表：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --driver-cores 4 \
  --executor-memory 8g \
  --executor-cores 2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=2 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.yarn.queue=real_time_merge \
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
  --conf spark.driver.host=REPLACE_DRIVER_HOST \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=REPLACE_PAIMON_WAREHOUSE \
  --conf spark.hadoop.net.topology.script.file.name=/dev/null \
  --conf spark.sql.defaultCatalog=paimon \
  --keytab REPLACE_KEYTAB_FILE \
  --jars REPLACE_PAIMON_HIVE_CONNECTOR_JAR,REPLACE_PAIMON_SPARK_JAR \
  --principal REPLACE_PRINCIPAL \
  --class org.apache.amoro.producer.LakehouseProducer \
  REPLACE_AMORO_PRODUCER_JAR \
  --format paimon \
  --action expire-snapshots \
  --catalogName paimon \
  --databaseName sl_oki_prod \
  --tableNameRegex '^orders_.*' \
  --retryTimes 3 \
  --continueOnTableFailure true \
  --retainMax 20 \
  --retainMin 10 \
  --maxDeletes 550 \
  --procedureOptions file-operation.thread-num=32,snapshot.expire.execution-mode=sync
```

### compact

整库小文件合并：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --driver-cores 4 \
  --executor-memory 8g \
  --executor-cores 2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=2 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.yarn.queue=real_time_merge \
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
  --conf spark.driver.host=REPLACE_DRIVER_HOST \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=REPLACE_PAIMON_WAREHOUSE \
  --conf spark.hadoop.net.topology.script.file.name=/dev/null \
  --conf spark.sql.defaultCatalog=paimon \
  --keytab REPLACE_KEYTAB_FILE \
  --jars REPLACE_PAIMON_HIVE_CONNECTOR_JAR,REPLACE_PAIMON_SPARK_JAR \
  --principal REPLACE_PRINCIPAL \
  --class org.apache.amoro.producer.LakehouseProducer \
  REPLACE_AMORO_PRODUCER_JAR \
  --format paimon \
  --action compact \
  --version 1.3 \
  --catalogName paimon \
  --databaseName sl_oki_prod \
  --tableNameRegex '^orders_.*' \
  --retryTimes 3 \
  --continueOnTableFailure true \
  --startBucket 0 \
  --step 20 \
  --compactStrategy full \
  --procedureOptions target-file-size=256m
```

Paimon 0.9 compact（默认；也可显式指定版本）使用 `partition_idle_time`，不读取或按 bucket 分片：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --driver-cores 4 \
  --executor-memory 8g \
  --executor-cores 2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=2 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.yarn.queue=real_time_merge \
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
  --conf spark.driver.host=REPLACE_DRIVER_HOST \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=REPLACE_PAIMON_WAREHOUSE \
  --conf spark.hadoop.net.topology.script.file.name=/dev/null \
  --conf spark.sql.defaultCatalog=paimon \
  --keytab REPLACE_KEYTAB_FILE \
  --jars REPLACE_PAIMON_HIVE_CONNECTOR_JAR,REPLACE_PAIMON_SPARK_JAR \
  --principal REPLACE_PRINCIPAL \
  --class org.apache.amoro.producer.LakehouseProducer \
  REPLACE_AMORO_PRODUCER_JAR \
  --format paimon \
  --action compact \
  --catalogName paimon \
  --databaseName sl_oki_prod \
  --tableNameRegex '^orders_.*' \
  --retryTimes 3 \
  --continueOnTableFailure true \
  --version 0.9 \
  --partitionIdleTime 1d
```

### compact-manifest

整库 manifest 合并：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --driver-cores 4 \
  --executor-memory 8g \
  --executor-cores 2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=2 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.yarn.queue=real_time_merge \
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
  --conf spark.driver.host=REPLACE_DRIVER_HOST \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=REPLACE_PAIMON_WAREHOUSE \
  --conf spark.hadoop.net.topology.script.file.name=/dev/null \
  --conf spark.sql.defaultCatalog=paimon \
  --keytab REPLACE_KEYTAB_FILE \
  --jars REPLACE_PAIMON_HIVE_CONNECTOR_JAR,REPLACE_PAIMON_SPARK_JAR \
  --principal REPLACE_PRINCIPAL \
  --class org.apache.amoro.producer.LakehouseProducer \
  REPLACE_AMORO_PRODUCER_JAR \
  --format paimon \
  --action compact-manifest \
  --catalogName paimon \
  --databaseName sl_oki_prod \
  --tableNameRegex '^orders_.*' \
  --retryTimes 3 \
  --continueOnTableFailure true \
  --procedureOptions file-operation.thread-num=16
```

### remove-orphan-files

整库删除孤儿文件：

```bash
./spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --driver-cores 4 \
  --executor-memory 8g \
  --executor-cores 2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=2 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.yarn.queue=real_time_merge \
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
  --conf spark.driver.host=REPLACE_DRIVER_HOST \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon.type=hive \
  --conf spark.sql.catalog.paimon.warehouse=REPLACE_PAIMON_WAREHOUSE \
  --conf spark.hadoop.net.topology.script.file.name=/dev/null \
  --conf spark.sql.defaultCatalog=paimon \
  --keytab REPLACE_KEYTAB_FILE \
  --jars REPLACE_PAIMON_HIVE_CONNECTOR_JAR,REPLACE_PAIMON_SPARK_JAR \
  --principal REPLACE_PRINCIPAL \
  --class org.apache.amoro.producer.LakehouseProducer \
  REPLACE_AMORO_PRODUCER_JAR \
  --format paimon \
  --action remove-orphan-files \
  --catalogName paimon \
  --databaseName sl_oki_prod \
  --tableNameRegex '^orders_.*' \
  --retryTimes 3 \
  --continueOnTableFailure true \
  --parallelism 10 \
  --mode distributed
```
