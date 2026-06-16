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

# Amoro Producer Snapshot 清理任务

`amoro-producer` 提供可通过 `spark-submit` 提交的 Paimon Snapshot 清理任务。入口类为
`org.apache.amoro.producer.PaimonExpireSnapshots`，任务会通过 Spark SQL 执行
`CALL sys.expire_snapshots`。

## 打包

```bash
./mvnw -pl amoro-producer package -DskipTests
```

打包产物：

```text
amoro-producer/target/amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar
```

## 参数说明

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

## Spark Submit Demo

以下示例基于 YARN cluster 模式提交，主类替换为
`org.apache.amoro.producer.PaimonExpireSnapshots`，应用 JAR 替换为
`amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar`。

### 按数据库清理

```bash
./spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 8g \
    --driver-cores 4 \
    --executor-memory 16g \
    --executor-cores 4 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=2 \
    --conf spark.dynamicAllocation.initialExecutors=40 \
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
    --conf spark.hadoop.net.topology.script.file.name=/dev/null \
    --keytab /data/work/hadoop-shopline/etc/hadoop/sljdp.keytab \
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

### 按单表清理

```bash
./spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 8g \
    --driver-cores 4 \
    --executor-memory 16g \
    --executor-cores 4 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=2 \
    --conf spark.dynamicAllocation.initialExecutors=40 \
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
    --conf spark.hadoop.net.topology.script.file.name=/dev/null \
    --keytab /data/work/hadoop-shopline/etc/hadoop/sljdp.keytab \
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

## 运行注意事项

Spark 运行环境必须提前配置好 Paimon catalog，否则 `SHOW TABLES` 或
`CALL sys.expire_snapshots` 会在运行期失败。

如果运行环境已经通过 Spark 安装目录或平台层提供 Paimon 相关 JAR，可以直接提交
`amoro-producer-0.9-SNAPSHOT.jar`；如果希望单 JAR 提交，使用
`amoro-producer-0.9-SNAPSHOT-jar-with-dependencies.jar`。

`spark.dynamicAllocation.initialExecutors` 建议不要大于
`spark.dynamicAllocation.maxExecutors`。上面的示例保留了原始提交命令中的资源参数，实际提交前请按集群策略调整。
