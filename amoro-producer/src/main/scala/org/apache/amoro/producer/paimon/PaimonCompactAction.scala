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

import org.apache.amoro.producer.core.{ActionOption, ActionResult, ActionStatus, ActionTaskResult, CommonProducerConfig, ProducerContext, ProducerParsedOptions, RetryMode, TargetTable}
import org.apache.amoro.producer.paimon.PaimonSqlUtils.{fieldOpt, quoteIdent, sqlString, valueAt}
import org.apache.amoro.producer.spi.{ActionConfig, ProducerAction}

final case class PaimonCompactConfig(
    startBucket: Int,
    step: Int,
    compactStrategy: String,
    procedureOptions: String,
    version: String,
    partitionIdleTime: String)
  extends ActionConfig

class PaimonCompactAction(runner: PaimonSqlRunner = SparkPaimonSqlRunner) extends ProducerAction {

  override def name: String = "compact"

  override def retryMode: RetryMode = RetryMode.ActionManagedRetry

  override def options: Seq[ActionOption] = {
    Seq(
      ActionOption("startBucket", description = "Start bucket id"),
      ActionOption("step", description = "Bucket range step"),
      ActionOption("compactStrategy", description = "Paimon compact strategy"),
      ActionOption("procedureOptions", description = "Paimon compact procedure options"),
      ActionOption("version", description = "Paimon version"),
      ActionOption("partitionIdleTime", description = "Paimon 0.9 partition idle time"))
  }

  override def parse(commandLine: ProducerParsedOptions): ActionConfig = {
    PaimonCompactConfig(
      startBucket = parseInt(optionValue(commandLine, "startBucket", "0"), "--startBucket"),
      step = parseInt(optionValue(commandLine, "step", "20"), "--step"),
      compactStrategy = optionValue(commandLine, "compactStrategy", "full"),
      procedureOptions = optionValue(commandLine, "procedureOptions", "target-file-size=256m"),
      version = optionValue(commandLine, "version", "0.9"),
      partitionIdleTime = optionValue(commandLine, "partitionIdleTime", "1d"))
  }

  override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {
    val config = asCompactConfig(actionConfig)
    validateRetryTimes(common.retryTimes)
    if (!PaimonCompactAction.isSupportedVersion(config)) {
      throw new IllegalArgumentException("--version 目前仅支持 0.9 或 1.3")
    }
    if (PaimonCompactAction.isPaimon13(config)) {
      if (config.startBucket < 0) {
        throw new IllegalArgumentException("--startBucket 不能小于 0")
      }
      if (config.step <= 0) {
        throw new IllegalArgumentException("--step 必须大于 0")
      }
      if (config.compactStrategy.trim.isEmpty) {
        throw new IllegalArgumentException("--compactStrategy 的值不能为空")
      }
    } else if (config.partitionIdleTime.trim.isEmpty) {
      throw new IllegalArgumentException("--partitionIdleTime 的值不能为空")
    }
  }

  override def execute(
      context: ProducerContext,
      table: TargetTable,
      actionConfig: ActionConfig): ActionResult = {
    val config = asCompactConfig(actionConfig)
    validateRetryTimes(context.common.retryTimes)
    val startNs = System.nanoTime()

    val tasks: Seq[ActionTaskResult] =
      if (PaimonCompactAction.isPaimon09(config)) {
        val taskName = s"partition_idle_time=${config.partitionIdleTime.trim}"
        Seq(
          runTask(
            context,
            table,
            taskName,
            PaimonCompactAction.buildCompactSql(
              context.common.catalogName,
              config,
              table,
              bucketRange = "")))
      } else {
        readBucketNum(context, table) match {
          case Left(_) =>
            Seq(runTask(
              context,
              table,
              "non-bucket",
              PaimonCompactAction.buildNonBucketCompactSql(table)))

          case Right(bucketNum) if config.startBucket > bucketNum - 1 =>
            val reason = s"startBucket(${config.startBucket}) > 最大 bucket id(${bucketNum - 1})"
            return result(
              status = ActionStatus.Skipped,
              tasks =
                Seq(
                  ActionTaskResult(
                    name = "skipped",
                    status = ActionStatus.Skipped,
                    message = Some(reason),
                    metrics = Map("table" -> table.raw))),
              message = Some(reason),
              table = table,
              startNs = startNs)

          case Right(bucketNum) =>
            PaimonCompactAction
              .buildBucketRanges(config.startBucket, config.step, bucketNum)
              .map { bucketRange =>
                runTask(
                  context,
                  table,
                  bucketRange,
                  PaimonCompactAction.buildCompactSql(
                    context.common.catalogName,
                    config,
                    table,
                    bucketRange))
              }
        }
      }

    val status =
      if (tasks.exists(_.status == ActionStatus.Failed)) {
        ActionStatus.Failed
      } else {
        ActionStatus.Success
      }

    result(status, tasks, None, table, startNs)
  }

  private[paimon] def buildBucketRanges(
      startBucket: Int,
      step: Int,
      bucketNum: Int): Seq[String] = {
    PaimonCompactAction.buildBucketRanges(startBucket, step, bucketNum)
  }

  private[paimon] def buildCompactSql(
      catalogName: String,
      config: PaimonCompactConfig,
      table: TargetTable,
      bucketRange: String): String = {
    PaimonCompactAction.buildCompactSql(catalogName, config, table, bucketRange)
  }

  private[paimon] def buildNonBucketCompactSql(table: TargetTable): String = {
    PaimonCompactAction.buildNonBucketCompactSql(table)
  }

  private[paimon] def buildNonBucketCompactSql(
      config: PaimonCompactConfig,
      table: TargetTable): String = {
    PaimonCompactAction.buildNonBucketCompactSql(config, table)
  }

  private[paimon] def isPaimon09(config: PaimonCompactConfig): Boolean = {
    PaimonCompactAction.isPaimon09(config)
  }

  private def readBucketNum(context: ProducerContext, table: TargetTable): Either[String, Int] = {
    val optionsTable =
      s"${quoteIdent(context.common.catalogName)}.${quoteIdent(table.database)}.${quoteIdent(table.table + "$options")}"
    val sqlText =
      s"""
         |SELECT `value`
         |FROM $optionsTable
         |WHERE lower(trim(`key`)) = 'bucket'
         |LIMIT 1
         |""".stripMargin

    try {
      runner.run(context.spark, sqlText).headOption match {
        case None =>
          Left("no bucket option in $options")

        case Some(row) =>
          fieldOpt(row, Set("value")).orElse(valueAt(row, 0)).map(_.toString.trim) match {
            case None =>
              Left("bucket value is null")

            case Some(bucketText) =>
              try {
                val bucketNum = bucketText.toInt
                if (bucketNum <= 0) {
                  Left(s"bucket = $bucketNum (非正数，无法按 bucket 范围 compact)")
                } else {
                  Right(bucketNum)
                }
              } catch {
                case NonFatal(_) =>
                  Left(s"bucket 不是整数: $bucketText")
              }
          }
      }
    } catch {
      case NonFatal(t) =>
        Left(s"read bucket error: ${t.getMessage}")
    }
  }

  private def runTask(
      context: ProducerContext,
      table: TargetTable,
      taskName: String,
      sqlText: String): ActionTaskResult = {
    var attempt = 1
    var lastError: Throwable = null

    while (attempt <= context.common.retryTimes) {
      try {
        runner.run(context.spark, sqlText)
        return ActionTaskResult(
          name = taskName,
          status = ActionStatus.Success,
          message = None,
          metrics = Map("table" -> table.raw, "attempts" -> attempt.toString))
      } catch {
        case NonFatal(t) =>
          lastError = t
          attempt += 1
      }
    }

    ActionTaskResult(
      name = taskName,
      status = ActionStatus.Failed,
      message = Some(errorMessage(lastError)),
      metrics = Map("table" -> table.raw, "attempts" -> context.common.retryTimes.toString))
  }

  private def result(
      status: ActionStatus,
      tasks: Seq[ActionTaskResult],
      message: Option[String],
      table: TargetTable,
      startNs: Long): ActionResult = {
    ActionResult(
      status = status,
      tasks = tasks,
      message = message,
      metrics = Map("table" -> table.raw),
      costMs = (System.nanoTime() - startNs) / 1e6)
  }

  private def asCompactConfig(actionConfig: ActionConfig): PaimonCompactConfig = {
    actionConfig match {
      case config: PaimonCompactConfig => config
      case other =>
        throw new IllegalArgumentException(
          s"Action compact requires PaimonCompactConfig, but got ${configType(other)}")
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

  private def optionValue(
      commandLine: ProducerParsedOptions,
      optionName: String,
      defaultValue: String): String = {
    commandLine.getOptionValue(optionName, defaultValue).trim
  }

  private def validateRetryTimes(retryTimes: Int): Unit = {
    if (retryTimes <= 0) {
      throw new IllegalArgumentException("--retryTimes 必须大于 0")
    }
  }

  private def errorMessage(error: Throwable): String = {
    Option(error)
      .map(t => s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}")
      .getOrElse("unknown error")
  }
}

object PaimonCompactAction {

  private val Paimon09 = "0.9"
  private val Paimon13 = "1.3"

  private[paimon] def buildBucketRanges(
      startBucket: Int,
      step: Int,
      bucketNum: Int): Seq[String] = {
    var start = startBucket
    var ranges = Seq.empty[String]
    while (start < bucketNum) {
      val end = math.min(start + step - 1, bucketNum - 1)
      ranges :+= s"$start-$end"
      start = end + 1
    }
    ranges
  }

  private[paimon] def buildCompactSql(
      catalogName: String,
      config: PaimonCompactConfig,
      table: TargetTable,
      bucketRange: String): String = {
    if (isPaimon09(config)) {
      return s"""
                |CALL sys.compact(
                |  table => ${sqlString(table.raw)},
                |  partition_idle_time => ${sqlString(config.partitionIdleTime.trim)}
                |)
                |""".stripMargin
    }

    val optionsClause =
      if (config.procedureOptions.trim.isEmpty) {
        ""
      } else {
        s",\n  options => ${sqlString(config.procedureOptions.trim)}"
      }

    s"""
       |CALL ${quoteIdent(catalogName)}.sys.compact(
       |  table => ${sqlString(table.raw)},
       |  buckets => ${sqlString(bucketRange)},
       |  compact_strategy => ${sqlString(config.compactStrategy.trim)}$optionsClause
       |)
       |""".stripMargin
  }

  private[paimon] def buildNonBucketCompactSql(table: TargetTable): String = {
    s"""
       |CALL sys.compact(
       |  table => ${sqlString(table.raw)}
       |)
       |""".stripMargin
  }

  private[paimon] def buildNonBucketCompactSql(
      config: PaimonCompactConfig,
      table: TargetTable): String = {
    buildNonBucketCompactSql(table)
  }

  private[paimon] def isPaimon09(config: PaimonCompactConfig): Boolean = {
    config.version.trim == Paimon09
  }

  private[paimon] def isPaimon13(config: PaimonCompactConfig): Boolean = {
    config.version.trim == Paimon13
  }

  private[paimon] def isSupportedVersion(config: PaimonCompactConfig): Boolean = {
    isPaimon09(config) || isPaimon13(config)
  }
}
