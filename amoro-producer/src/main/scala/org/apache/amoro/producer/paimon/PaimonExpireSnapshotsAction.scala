/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.producer.paimon

import java.time.{Duration, Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.paimon.CoreOptions
import org.apache.paimon.utils.ParameterUtils
import org.apache.spark.sql.Row

import org.apache.amoro.producer.core.{ActionOption, ActionResult, ActionStatus, ActionTaskResult, CommonProducerConfig, ProducerContext, ProducerParsedOptions, RetryMode, TargetTable}
import org.apache.amoro.producer.paimon.PaimonSqlUtils.{fieldOpt, quoteIdent, sqlString, valueAt}
import org.apache.amoro.producer.spi.{ActionConfig, ProducerAction, ProducerBatchExecutor}

final case class PaimonExpireSnapshotsConfig(
    retainMax: Int,
    retainMin: Int,
    maxDeletes: Long,
    procedureOptions: String,
    timeRetained: Option[Duration] = None,
    executionMode: String = "driver",
    tableConcurrency: Int = 4,
    tableConcurrencyExplicit: Boolean = false)
  extends ActionConfig

class PaimonExpireSnapshotsAction(runner: PaimonSqlRunner = SparkPaimonSqlRunner)
  extends ProducerAction {

  override def name: String = "expire-snapshots"

  override def retryMode: RetryMode = RetryMode.ActionManagedRetry

  override def options: Seq[ActionOption] = {
    Seq(
      ActionOption("retainMax", description = "Paimon expire_snapshots retain max"),
      ActionOption("retainMin", description = "Paimon expire_snapshots retain min"),
      ActionOption("maxDeletes", description = "Paimon expire_snapshots max deletes"),
      ActionOption("procedureOptions", description = "Paimon expire_snapshots procedure options"),
      ActionOption(
        "executionMode",
        description = "Snapshot cleanup location: driver (default) or executor"),
      ActionOption(
        "tableConcurrency",
        description = "Executor mode in-flight table limit (default 4)"))
  }

  override def parse(commandLine: ProducerParsedOptions): ActionConfig = {
    PaimonExpireSnapshotsConfig(
      retainMax =
        parseInt(
          commandLine.getOptionValue(
            "retainMax",
            PaimonExpireSnapshotsAction.DefaultRetainMax.toString),
          "--retainMax"),
      retainMin =
        parseInt(
          commandLine.getOptionValue(
            "retainMin",
            PaimonExpireSnapshotsAction.DefaultRetainMin.toString),
          "--retainMin"),
      maxDeletes =
        parseLong(
          commandLine.getOptionValue(
            "maxDeletes",
            PaimonExpireSnapshotsAction.DefaultMaxDeletes.toString),
          "--maxDeletes"),
      procedureOptions =
        commandLine
          .getOptionValue(
            "procedureOptions",
            PaimonExpireSnapshotsAction.DefaultProcedureOptions)
          .trim,
      executionMode =
        commandLine.getOptionValue("executionMode", "driver").trim.toLowerCase(Locale.ROOT),
      tableConcurrency =
        parseInt(commandLine.getOptionValue("tableConcurrency", "4"), "--tableConcurrency"),
      tableConcurrencyExplicit = commandLine.hasOption("tableConcurrency"))
  }

  override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {
    val config = asExpireSnapshotsConfig(actionConfig)
    validateRetryTimes(common.retryTimes)
    require(
      Set("driver", "executor").contains(config.executionMode),
      "--executionMode must be driver or executor")
    require(config.tableConcurrency > 0, "--tableConcurrency must be positive")
    require(
      config.executionMode == "executor" || !config.tableConcurrencyExplicit,
      "--tableConcurrency is only supported in executor mode")
    if (config.retainMax < 0) {
      throw new IllegalArgumentException("--retainMax 不能小于 0")
    }
    if (config.retainMin < 0) {
      throw new IllegalArgumentException("--retainMin 不能小于 0")
    }
    if (config.maxDeletes <= 0) {
      throw new IllegalArgumentException("--maxDeletes 必须大于 0")
    }
    require(config.maxDeletes <= Int.MaxValue, "--maxDeletes 不能超过 Integer.MAX_VALUE")
  }

  override def validateRuntime(context: ProducerContext, actionConfig: ActionConfig): Unit = {
    if (asExpireSnapshotsConfig(actionConfig).executionMode == "executor") {
      PaimonSnapshotRuntime.validate(context.spark.version, context.spark.sparkContext.getConf)
    }
  }

  override def batchExecutor(
      context: ProducerContext,
      actionConfig: ActionConfig): Option[ProducerBatchExecutor] = {
    val config = asExpireSnapshotsConfig(actionConfig)
    if (config.executionMode == "driver") None
    else {
      validate(context.common, actionConfig)
      validateRuntime(context, actionConfig)
      Some(new PaimonSnapshotExecutor(
        config.tableConcurrency,
        context.common.continueOnTableFailure,
        new PaimonSnapshotSparkBackend(context, config, runner)))
    }
  }

  override def execute(
      context: ProducerContext,
      table: TargetTable,
      actionConfig: ActionConfig): ActionResult = {
    validate(context.common, actionConfig)
    val startNs = System.nanoTime()
    val requested = asExpireSnapshotsConfig(actionConfig)
    require(requested.executionMode == "driver", "Executor mode requires the batch execution path")
    val (properties, _) = withRetry(context, table, "读取 snapshot 配置") {
      runner.readExplicitProperties(context.spark, context.common.catalogName, table)
    }
    val (supportsOptions, _) = withRetry(context, table, "读取 expire_snapshots 参数") {
      runner.expireSnapshotsSupportsOptions(context.spark, context.common.catalogName)
    }
    val config = PaimonSnapshotRetention.resolveConfig(requested, properties, supportsOptions)
    if (!supportsOptions) {
      val options = ParameterUtils.parseCommaSeparatedKeyValues(config.procedureOptions).asScala
      val defaults = ParameterUtils.parseCommaSeparatedKeyValues(
        PaimonExpireSnapshotsAction.DefaultProcedureOptions).asScala
      val unsupported = options.filter { case (key, value) =>
        !PaimonExpireSnapshotsAction.RetentionKeys.contains(key) && !defaults.get(key).contains(
          value)
      }
      require(
        unsupported.isEmpty,
        s"当前 expire_snapshots 不支持 options: ${unsupported.keys.mkString(",")}")
    }
    var round = 1
    var shouldContinue = true
    var tasks = Seq.empty[ActionTaskResult]
    var totalDeletedCount = 0L

    while (shouldContinue) {
      val (deletedCount, attempts) =
        runRoundWithRetry(context, table, config, round, supportsOptions)
      totalDeletedCount += deletedCount
      tasks :+= ActionTaskResult(
        name = s"round-$round",
        status = ActionStatus.Success,
        message = None,
        metrics =
          Map(
            "table" -> table.raw,
            "deletedCount" -> deletedCount.toString,
            "attempts" -> attempts.toString))
      shouldContinue = deletedCount >= config.maxDeletes
      round += 1
    }

    val metrics =
      Map(
        "table" -> table.raw,
        "rounds" -> tasks.size.toString,
        "totalDeletedCount" -> totalDeletedCount.toString,
        "deletedSnapshotsTotal" -> totalDeletedCount.toString,
        "maxDeletes" -> config.maxDeletes.toString,
        "retainMin" -> config.retainMin.toString,
        "retainMax" -> config.retainMax.toString,
        "timeRetainedMs" -> config.timeRetained.get.toMillis.toString)
    ActionResult(
      status = ActionStatus.Success,
      tasks = tasks,
      message = None,
      metrics = metrics,
      costMs = (System.nanoTime() - startNs) / 1e6)
  }

  private def runRoundWithRetry(
      context: ProducerContext,
      table: TargetTable,
      config: PaimonExpireSnapshotsConfig,
      round: Int,
      supportsOptions: Boolean): (Long, Int) = {
    val sqlText =
      PaimonExpireSnapshotsAction.buildExpireSnapshotsSql(
        context.common.catalogName,
        config,
        table,
        supportsOptions)
    val sparkContext = Option(context.spark).map(_.sparkContext)
    val previous = sparkContext.map(_.getLocalProperty("spark.job.description"))
    // setJobDescription propagates this thread's description to submitted Spark tasks.
    // https://github.com/apache/spark/blob/v3.5.4/core/src/main/scala/org/apache/spark/SparkContext.scala#L708-L736
    sparkContext.foreach(_.setJobDescription(
      s"Expire snapshots: ${context.common.catalogName}.${table.database}.${table.table} " +
        s"| retained_min=${config.retainMin} | retained_max=${config.retainMax} " +
        s"| retained_time=${PaimonExpireSnapshotsAction.retainedTimeDescription(config)} " +
        s"| max_deletes=${config.maxDeletes}"))
    try {
      withRetry(context, table, s"expire_snapshots 第 $round 轮") {
        parseDeletedCount(runner.run(context.spark, sqlText))
      }
    } finally sparkContext.foreach(_.setJobDescription(previous.orNull))
  }

  private def withRetry[T](
      context: ProducerContext,
      table: TargetTable,
      operation: String)(body: => T): (T, Int) = {
    var attempt = 1
    var lastError: Throwable = null

    while (attempt <= context.common.retryTimes) {
      try {
        return (body, attempt)
      } catch {
        case NonFatal(t) =>
          lastError = t
          if (attempt == context.common.retryTimes) {
            throw new RuntimeException(
              s"表 ${table.raw} $operation 连续失败 ${context.common.retryTimes} 次",
              lastError)
          }
          attempt += 1
      }
    }
    throw new RuntimeException("不应到达此分支")
  }

  private def parseDeletedCount(rows: Seq[Row]): Long = {
    rows.headOption.map(parseDeletedCount).getOrElse {
      throw new IllegalStateException("expire_snapshots 返回空结果")
    }
  }

  private def parseDeletedCount(row: Row): Long = {
    val deletedCount =
      fieldOpt(row, Set("deletedcount", "deletedsnapshots", "deletedsnapshotscount", "result"))
        .orElse(valueAt(row, 0))

    deletedCount.flatMap(toLongOpt).getOrElse {
      throw new IllegalStateException(
        s"无法从返回结果解析删除数量，row=${row.mkString("[", ", ", "]")}")
    }
  }

  private def asExpireSnapshotsConfig(actionConfig: ActionConfig): PaimonExpireSnapshotsConfig = {
    actionConfig match {
      case config: PaimonExpireSnapshotsConfig => config
      case other =>
        throw new IllegalArgumentException(
          s"Action expire-snapshots requires PaimonExpireSnapshotsConfig, but got ${configType(other)}")
    }
  }

  private def configType(actionConfig: ActionConfig): String = {
    Option(actionConfig).map(_.getClass.getName).getOrElse("null")
  }

  private def parseInt(value: String, optionName: String): Int = {
    try {
      value.trim.toInt
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$optionName 必须是整数: $value")
    }
  }

  private def parseLong(value: String, optionName: String): Long = {
    try {
      value.trim.toLong
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$optionName 必须是长整数: $value")
    }
  }

  private def validateRetryTimes(retryTimes: Int): Unit = {
    if (retryTimes <= 0) {
      throw new IllegalArgumentException("--retryTimes 必须大于 0")
    }
  }

  private def toLongOpt(value: Any): Option[Long] = {
    value match {
      case null => None
      case n: java.lang.Number => Some(n.longValue())
      case s: String if s.trim.matches("-?\\d+") => Some(s.trim.toLong)
      case _ => None
    }
  }

}

object PaimonExpireSnapshotsAction {

  private val UnlimitedRetention = Duration.ofMillis(Long.MaxValue)

  private val RetentionKeys = Set(
    CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(),
    CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(),
    CoreOptions.SNAPSHOT_TIME_RETAINED.key(),
    CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key())

  private val DefaultRetainMax = 20
  private val DefaultRetainMin = 10
  private val DefaultMaxDeletes = 550L
  private val DefaultProcedureOptions =
    "file-operation.thread-num=32,snapshot.expire.execution-mode=sync"

  private def retainedTimeDescription(config: PaimonExpireSnapshotsConfig): String = {
    val duration = config.timeRetained.get
    if (duration == UnlimitedRetention) "UNLIMITED" else s"${duration.toMillis}ms"
  }

  private[paimon] def buildExpireSnapshotsSql(
      catalogName: String,
      config: PaimonExpireSnapshotsConfig,
      table: TargetTable,
      supportsOptions: Boolean = true): String = {
    val optionsClause =
      if (!supportsOptions || config.procedureOptions.trim.isEmpty) {
        ""
      } else {
        s",\n  options => ${sqlString(config.procedureOptions.trim)}"
      }
    // Paimon 0.9 accepts a timestamp cutoff and does not inherit table retention in this procedure.
    // https://github.com/apache/paimon/blob/release-0.9.0/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExpireSnapshotsProcedure.java
    val olderThanClause = if (supportsOptions) ""
    else config.timeRetained.filterNot(_ == UnlimitedRetention).map { duration =>
      val cutoff = Instant.ofEpochMilli(System.currentTimeMillis() - duration.toMillis)
      // An explicit UTC offset avoids ambiguous local timestamps during daylight-saving overlap.
      // https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala
      val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXXX")
        .withZone(ZoneOffset.UTC).format(cutoff)
      s",\n  older_than => TIMESTAMP ${sqlString(timestamp)}"
    }.getOrElse("")

    s"""
       |CALL ${quoteIdent(catalogName)}.sys.expire_snapshots(
       |  table => ${sqlString(table.raw)},
       |  retain_max => ${config.retainMax},
       |  retain_min => ${config.retainMin},
       |  max_deletes => ${config.maxDeletes}$olderThanClause$optionsClause
       |)
       |""".stripMargin
  }
}
