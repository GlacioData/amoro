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

import scala.util.control.NonFatal

import org.apache.spark.sql.Row

import org.apache.amoro.producer.core.{ActionOption, ActionResult, ActionStatus, ActionTaskResult, CommonProducerConfig, ProducerContext, ProducerParsedOptions, RetryMode, TargetTable}
import org.apache.amoro.producer.paimon.PaimonSqlUtils.{fieldOpt, quoteIdent, sqlString, valueAt}
import org.apache.amoro.producer.spi.{ActionConfig, ProducerAction}

final case class PaimonExpireSnapshotsConfig(
    retainMax: Int,
    retainMin: Int,
    maxDeletes: Long,
    procedureOptions: String)
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
      ActionOption("procedureOptions", description = "Paimon expire_snapshots procedure options"))
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
          .trim)
  }

  override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {
    val config = asExpireSnapshotsConfig(actionConfig)
    validateRetryTimes(common.retryTimes)
    if (config.retainMax < 0) {
      throw new IllegalArgumentException("--retainMax 不能小于 0")
    }
    if (config.retainMin < 0) {
      throw new IllegalArgumentException("--retainMin 不能小于 0")
    }
    if (config.maxDeletes <= 0) {
      throw new IllegalArgumentException("--maxDeletes 必须大于 0")
    }
  }

  override def execute(
      context: ProducerContext,
      table: TargetTable,
      actionConfig: ActionConfig): ActionResult = {
    validate(context.common, actionConfig)
    val config = asExpireSnapshotsConfig(actionConfig)
    val startNs = System.nanoTime()
    var round = 1
    var shouldContinue = true
    var tasks = Seq.empty[ActionTaskResult]
    var totalDeletedCount = 0L

    while (shouldContinue) {
      val (deletedCount, attempts) = runRoundWithRetry(context, table, config, round)
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
        "maxDeletes" -> config.maxDeletes.toString)
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
      round: Int): (Long, Int) = {
    val sqlText =
      PaimonExpireSnapshotsAction.buildExpireSnapshotsSql(
        context.common.catalogName,
        config,
        table)
    var attempt = 1
    var lastError: Throwable = null

    while (attempt <= context.common.retryTimes) {
      try {
        val deletedCount = parseDeletedCount(runner.run(context.spark, sqlText))
        return (deletedCount, attempt)
      } catch {
        case NonFatal(t) =>
          lastError = t
          if (attempt == context.common.retryTimes) {
            throw new RuntimeException(
              s"表 ${table.raw} expire_snapshots 第 $round 轮连续失败 ${context.common.retryTimes} 次",
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

  private val DefaultRetainMax = 20
  private val DefaultRetainMin = 10
  private val DefaultMaxDeletes = 550L
  private val DefaultProcedureOptions =
    "file-operation.thread-num=32,snapshot.expire.execution-mode=sync"

  private[paimon] def buildExpireSnapshotsSql(
      catalogName: String,
      config: PaimonExpireSnapshotsConfig,
      table: TargetTable): String = {
    val optionsClause =
      if (config.procedureOptions.trim.isEmpty) {
        ""
      } else {
        s",\n  options => ${sqlString(config.procedureOptions.trim)}"
      }

    s"""
       |CALL ${quoteIdent(catalogName)}.sys.expire_snapshots(
       |  table => ${sqlString(table.raw)},
       |  retain_max => ${config.retainMax},
       |  retain_min => ${config.retainMin},
       |  max_deletes => ${config.maxDeletes}$optionsClause
       |)
       |""".stripMargin
  }
}
