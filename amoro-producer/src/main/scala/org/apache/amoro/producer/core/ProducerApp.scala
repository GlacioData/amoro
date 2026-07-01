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

package org.apache.amoro.producer.core

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession

import org.apache.amoro.producer.spi.{ActionConfig, LakeFormatAdapter, ProducerAction}

object ProducerApp {

  def run(args: Array[String], adapters: Seq[LakeFormatAdapter]): Unit = {
    val code = runInternal(args, adapters)
    if (code != 0) {
      sys.exit(code)
    }
  }

  def runInternal(
      args: Array[String],
      adapters: Seq[LakeFormatAdapter],
      sparkBuilder: () => SparkSession = () => SparkSession.builder().getOrCreate()): Int = {
    val peek = ProducerCliParser.peek(args)
    if (peek.help) {
      printUsage(adapters, peek)
      return 0
    }

    val (adapter, action, actionConfig, common) =
      try {
        val format =
          peek.format.getOrElse {
            throw new IllegalArgumentException("--format must be specified")
          }
        val actionName =
          peek.action.getOrElse {
            throw new IllegalArgumentException("--action must be specified")
          }
        val resolvedAdapter = resolveAdapter(format, adapters)
        val resolvedAction = resolveAction(resolvedAdapter, actionName)
        val commandLine = ProducerCliParser.parseMerged(resolvedAction.options, args)
        val parsedCommon = ProducerCliParser.parseCommon(commandLine)
        val parsedActionConfig = resolvedAction.parse(commandLine)
        resolvedAction.validate(parsedCommon, parsedActionConfig)
        TableResolver.validateTargetOptions(parsedCommon)
        (resolvedAdapter, resolvedAction, parsedActionConfig, parsedCommon)
      } catch {
        case exception: ProducerCliParseException =>
          printUsage(adapters, peek)
          println(s"参数错误: ${exception.getMessage}")
          return 2
        case exception: IllegalArgumentException =>
          printUsage(adapters, peek)
          println(s"参数错误: ${exception.getMessage}")
          return 2
      }

    var spark: SparkSession = null
    try {
      spark = sparkBuilder()
      val targets = TableResolver.resolveTargets(spark, adapter, common)
      printTargets(common, targets)
      val context = ProducerContext(spark, common, adapter)
      val results = executeTables(context, action, actionConfig, targets)
      val summary = printSummary(common, results)
      if (summary.failedTables.nonEmpty && !common.continueOnTableFailure) 1 else 0
    } catch {
      case NonFatal(exception) =>
        println(s"执行失败: ${exception.getClass.getSimpleName}: ${exception.getMessage}")
        1
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  def resolveAdapter(format: String, adapters: Seq[LakeFormatAdapter]): LakeFormatAdapter = {
    val normalizedFormat = normalize(format)
    requireUniqueNormalizedKeys(
      values = adapters.map(_.format),
      duplicatedMessage =
        duplicate => s"Duplicate lakehouse format after normalization: $duplicate")
    adapters
      .find(adapter => normalize(adapter.format) == normalizedFormat)
      .getOrElse {
        throw new IllegalArgumentException(
          s"Unsupported lakehouse format: $format. Available formats: ${adapters.map(_.format).mkString(", ")}")
      }
  }

  def resolveAction(adapter: LakeFormatAdapter, actionName: String): ProducerAction = {
    val normalizedAction = normalize(actionName)
    val actions = adapter.actions
    val availableActions = actions.map(_.name).mkString(", ")
    requireUniqueNormalizedKeys(
      values = actions.map(_.name),
      duplicatedMessage = duplicate =>
        s"Duplicate action name after normalization for format '${adapter.format}': $duplicate")
    actions
      .find(action => normalize(action.name) == normalizedAction)
      .getOrElse {
        throw new IllegalArgumentException(
          s"Unsupported action '$actionName' for format '${adapter.format}'. Available actions: $availableActions")
      }
  }

  def executeOneTableWithRetry(
      context: ProducerContext,
      action: ProducerAction,
      actionConfig: ActionConfig,
      table: TargetTable): ActionResult = {
    action.retryMode match {
      case RetryMode.ActionManagedRetry =>
        action.execute(context, table, actionConfig)

      case RetryMode.FrameworkTableRetry =>
        executeWithFrameworkRetry(context, action, actionConfig, table)
    }
  }

  def executeTables(
      context: ProducerContext,
      action: ProducerAction,
      actionConfig: ActionConfig,
      targets: Seq[TargetTable]): Seq[(TargetTable, ActionResult)] = {
    validateRetryTimesForFrameworkRetry(context, action)
    targets.map { table =>
      val startNs = System.nanoTime()
      val execution =
        try {
          Right(executeOneTableWithRetry(context, action, actionConfig, table))
        } catch {
          case NonFatal(exception) => Left(exception)
        }

      execution match {
        case Right(actionResult) =>
          val result = actionResult.copy(costMs = elapsedMs(startNs))
          printTableAction(context.common, table, result)
          if (result.status == ActionStatus.Failed && !context.common.continueOnTableFailure) {
            throw new RuntimeException(failedStatusMessage(action, table, result))
          }
          table -> result

        case Left(exception) =>
          val result = failedResult(table, exception).copy(costMs = elapsedMs(startNs))
          printTableAction(context.common, table, result)
          if (context.common.continueOnTableFailure) {
            table -> result
          } else {
            throw exception
          }
      }
    }
  }

  private def executeWithFrameworkRetry(
      context: ProducerContext,
      action: ProducerAction,
      actionConfig: ActionConfig,
      table: TargetTable): ActionResult = {
    validateRetryTimesForFrameworkRetry(context, action)
    var attempt = 1
    var lastFailure: Throwable = null
    while (attempt <= context.common.retryTimes) {
      try {
        return action.execute(context, table, actionConfig)
      } catch {
        case NonFatal(exception) =>
          lastFailure = exception
          if (attempt == context.common.retryTimes) {
            throw exception
          }
      }
      attempt += 1
    }
    throw lastFailure
  }

  private def failedResult(table: TargetTable, exception: Throwable): ActionResult = {
    ActionResult(
      status = ActionStatus.Failed,
      tasks = Seq.empty,
      message = Some(Option(exception.getMessage).getOrElse(exception.getClass.getName)),
      metrics = Map("table" -> table.raw),
      costMs = 0.0d)
  }

  private def failedStatusMessage(
      action: ProducerAction,
      table: TargetTable,
      result: ActionResult): String = {
    val message = result.message.getOrElse("action returned failed status")
    s"Action '${action.name}' failed for table ${table.raw}: $message"
  }

  private def printTableAction(
      common: CommonProducerConfig,
      table: TargetTable,
      result: ActionResult): Unit = {
    println(TableActionJsonLog.render(common, table, result, result.costMs))
  }

  private def elapsedMs(startNs: Long): Double = {
    (System.nanoTime() - startNs) / 1e6
  }

  private def validateRetryTimesForFrameworkRetry(
      context: ProducerContext,
      action: ProducerAction): Unit = {
    if (action.retryMode == RetryMode.FrameworkTableRetry && context.common.retryTimes <= 0) {
      throw new IllegalArgumentException("retryTimes must be greater than 0")
    }
  }

  private def requireUniqueNormalizedKeys(
      values: Seq[String],
      duplicatedMessage: String => String): Unit = {
    val duplicateKeys =
      values
        .map(normalize)
        .groupBy(identity)
        .collect {
          case (key, groupedValues) if groupedValues.size > 1 => key
        }
        .toSeq
        .sorted

    if (duplicateKeys.nonEmpty) {
      throw new IllegalArgumentException(duplicatedMessage(duplicateKeys.mkString(", ")))
    }
  }

  private def normalize(value: String): String = {
    Option(value).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
  }

  private def printUsage(adapters: Seq[LakeFormatAdapter], peek: CliPeek): Unit = {
    val options =
      peek.format
        .flatMap(format => adapters.find(adapter => normalize(adapter.format) == normalize(format)))
        .flatMap { adapter =>
          peek.action.flatMap(actionName =>
            adapter.actions.find(action => normalize(action.name) == normalize(actionName)))
        }
        .map(_.options)
        .getOrElse(Seq.empty)
    println(ProducerCliParser.renderUsage(options, adapters.map(_.format)))
  }

  private def printTargets(common: CommonProducerConfig, targets: Seq[TargetTable]): Unit = {
    if (targets.size > 1 || common.tableName.isEmpty) {
      println(
        s"Lakehouse producer started, format=${common.format}, action=${common.action}, " +
          s"catalog=${common.catalogName}, tables=${targets.size}")
      targets.foreach(table => println(s"  - ${table.raw}"))
    } else if (targets.nonEmpty) {
      println(
        s"Lakehouse producer started, format=${common.format}, action=${common.action}, " +
          s"catalog=${common.catalogName}, table=${targets.head.raw}")
    }
  }

  private def printSummary(
      common: CommonProducerConfig,
      results: Seq[(TargetTable, ActionResult)]): ProducerSummary = {
    val summary = ProducerSummary.fromResults(results)
    println("=========== PRODUCER SUMMARY ===========")
    println(s"format             = ${common.format}")
    println(s"action             = ${common.action}")
    println(s"catalog            = ${common.catalogName}")
    println(s"tables found       = ${summary.tableCount}")
    println(s"tables success     = ${summary.successTables.size}")
    println(s"tables failed      = ${summary.failedTables.size}")
    println(s"tables skipped     = ${summary.skippedTables.size}")
    println(s"task success count = ${summary.taskSuccessCount}")
    println(s"task failed count  = ${summary.taskFailedCount}")
    if (summary.failedTableSummaries.nonEmpty) {
      println("failed tables:")
      summary.failedTableSummaries.foreach { failedTable =>
        println(s"  - ${failedTable.table.raw} | ${failedTable.message}")
      }
    }
    println("========================================")
    summary
  }
}
