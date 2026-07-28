---
title: "Paimon"
url: paimon-format
aliases:
    - "formats/paimon"
menu:
    main:
        parent: Formats
        weight: 200
---
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
# Paimon Format

Paimon format refers to [Apache Paimon](https://paimon.apache.org/) table.
Paimon is a streaming data lake platform with high-speed data ingestion, changelog tracking and efficient real-time analytics.

By registering Paimon's catalog with Amoro, users can view information such as Schema, Options, Files, Snapshots, DDLs, Compaction information, and more for Paimon tables.
Furthermore, they can operate on Paimon tables using Spark SQL in the Terminal. The current supported catalog types and file system types for Paimon are all supported.

For registering catalog operation steps, please refer to [Managing Catalogs](../managing-catalogs/).

{{< hint info >}}
If you want to use S3 or OSS, please download the 
[S3](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/0.5.0-incubating/paimon-s3-0.5.0-incubating.jar), 
[OSS](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/0.5.0-incubating/paimon-oss-0.5.0-incubating.jar) 
package and put it in the 'lib' directory of the Amoro installation package.
{{< /hint >}}

## Self-optimizing for BUCKET_UNAWARE tables

Amoro can automatically merge small files for Paimon AppendOnly tables whose bucket
is set to `-1` (the `BUCKET_UNAWARE` mode). This AppendOnly planner does not handle
primary-key tables or tables with a fixed positive bucket count. Eligible primary-key
tables use the independent planner described below.

### Enable the optimizer plugin

The Paimon optimizer plugin is **disabled by default**. To turn it on, add the
following property to the plugin-properties section of the Paimon `ProcessFactory`
in your AMS configuration:

```yaml
optimizing-plugins:
  paimon:
    paimon-optimizer.enabled: true
```

When this flag is off, `PaimonProcessFactory.supportedFormats()` returns an empty
set, so the AMS optimizer queue cannot route any Paimon table into this factory —
it is a clean kill-switch for grey-scale rollout.

### Watching progress

Once enabled, every registered Paimon AppendOnly table is refreshed periodically
by the same `TableRuntimeRefreshExecutor` that drives Iceberg. Each optimizing
cycle transitions the table runtime state through:

```
IDLE → PLANNING → OPTIMIZING → COMMITTING → IDLE
```

You can observe this progression on the Dashboard `Optimizing` tab, or by querying
`GET /api/ams/v1/optimize/tables` via the REST API. Every task shows up with
`OptimizingType.MINOR` and the `task-executor-factory-impl` property set to
`PaimonCompactionExecutorFactory`.

The `summary()` of each finished task carries four keys (all byte-level counts):

```
compacted-files, compacted-bytes, produced-files, produced-bytes
```

### Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| Table stays `IDLE` even though many small files exist | Check `paimon-optimizer.enabled=true` is in AMS config; check the table is `AppendOnly + bucket=-1`; primary-key tables are skipped by design. |
| `NoClassDefFoundError: org.apache.paimon.append.AppendCompactCoordinator` in the Optimizer log | `paimon-bundle` jar is missing from the Optimizer distribution `lib/` directory. Rebuild the distribution or drop the jar in manually. |
| `OptimizingCommitException: Paimon commit failed … RuntimeException` | An external writer committed concurrently between plan and commit; AMS will mark this process failed and re-plan automatically on the next tick. No action needed unless it repeats indefinitely. |
| `IllegalStateException: missing required fields (table / taskBytes)` in Optimizer | The task input was truncated or the Planner shipped an empty `PaimonCompactionInput`; usually an upgrade-skew issue. Check that AMS and Optimizer are on the same Amoro version. |
| Table goes `PLANNING → IDLE` immediately | The planner ran `AppendCompactCoordinator.run()` and found zero candidate files. Lower `target-file-size` or `compaction.min.file-num` on the table if you expect more aggressive compaction. |

### Limitations in the first version

* This AppendOnly path only supports AppendOnly tables (`bucket=-1`, no primary key).
  Primary-key HASH_FIXED and HASH_DYNAMIC tables use a separate path and configuration.
* REST Catalog is not tested; FileSystem and Hive Metastore catalogs are the
  tested code paths.
* Paimon-specific metrics (compaction lag, rewrite throughput) are not yet
  exposed to Dashboard — the first version reuses the standard `optimizing_*`
  metric family.

## Paimon 主键表单次规划与执行语义

Paimon 主键表优化仅覆盖同时满足以下条件的表：

* 表属性 `paimon-optimizer.primary-key.enabled=true`；
* 存在主键；
* bucket 模式为 `HASH_FIXED` 或 `HASH_DYNAMIC`；
* 未开启 `pk-clustering-override`。

AppendOnly 优化路径、Iceberg 及其他表格式不使用本节策略。

### MINOR、MAJOR 与 FULL

规划器固定读取一次目标 snapshot 的全部 live data files，并为每个
`partition + bucket` 重建 Paimon `Levels`：

```text
R = L0 文件数 + 非空高 level 数
C = num-sorted-run.compaction-trigger
S = num-sorted-run.stop-trigger
N = Paimon 1.4.2 官方 normal CompactStrategy 的 pick() 存在候选
M = N && R > S
```

其中 `N` 不是文件数阈值的近似值。它完整沿用 Paimon 1.4.2 的
Universal、early-full、off-peak、force-up-L0 与 lookup 优先级；规划端按真实执行链最多探测两次 `pick()`。

单次决策顺序如下：

1. 存在 `M`：只规划 MAJOR，并忽略 MINOR interval；
2. 不存在 `M`、存在 `N` 且 MINOR interval 到期：规划全部 `N` 为 MINOR；
3. 否则继续判断既有 FULL interval 与 partition idle 条件；
4. 仍不满足则本轮不规划。

执行映射固定为：

```text
MINOR -> compact(false)
MAJOR -> compact(false)
FULL  -> compact(true)
```

MAJOR 是 Amoro 对“存在 normal compaction 候选且 `R>S`”的高压分类，不是
Paimon FULL compaction。Paimon 的 normal strategy 可能因 size amplification、size ratio、early-full
等规则选择较大的高 level 文件，所以 MAJOR 记录中出现大文件不等于执行了 `compact(true)`。

### MAJOR 单次 bucket 上限

配置项：

```text
paimon-optimizer.primary-key.major.max-bucket-ratio
```

默认有效值为 `0.33`。配置使用十进制精确计算，先按 `RoundingMode.DOWN`
截取两位，再将低于 `0.33` 的值钳制为 `0.33`。例如：

```text
0.302 -> 0.30 -> 0.33
0.339 -> 0.33
0.341 -> 0.34
```

blank、非十进制、`NaN`、无穷或原始值大于 `1.00` 时，本轮主键表规划拒绝执行。
若固定 snapshot 中共有 `A` 个 active buckets，则单次 MAJOR 最多选择：

```text
B = ceil(A * effectiveRatio)
```

候选按以下顺序确定性排序后取前 `B` 个：

```text
R DESC
physical file count DESC
file size DESC
serialized partition bytes unsigned ASC
bucket ASC
```

已废弃的 `paimon-optimizer.primary-key.major.file-count-threshold` 只记录 WARN 并忽略，
即使其值无法解析也不会改变规划结果。

### 升级说明

该修正不提供旧 Paimon 主键 PROCESS payload 的无损接管。升级前应先 drain 或终止旧版本正在运行的
Paimon 主键表优化 PROCESS，再统一升级 AMS 与 Optimizer。重启后每轮直接基于当时捕获的新 snapshot
重新规划，不持久化 bucket cursor、候选文件列表或跨轮公平状态。
