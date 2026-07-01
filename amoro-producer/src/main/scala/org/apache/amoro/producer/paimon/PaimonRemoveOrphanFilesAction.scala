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

import org.apache.amoro.producer.core.{ActionOption, ActionResult, ActionStatus, ActionTaskResult, CommonProducerConfig, ProducerContext, ProducerParsedOptions, RetryMode, TargetTable}
import org.apache.amoro.producer.paimon.PaimonSqlUtils.{fieldOpt, quoteIdent, sqlString, valueAt}
import org.apache.amoro.producer.spi.{ActionConfig, ProducerAction}

final case class PaimonRemoveOrphanFilesConfig(parallelism: Int, mode: String)
  extends ActionConfig

class PaimonRemoveOrphanFilesAction(runner: PaimonSqlRunner = SparkPaimonSqlRunner)
  extends ProducerAction {

  override def name: String = "remove-orphan-files"

  override def retryMode: RetryMode = RetryMode.FrameworkTableRetry

  override def options: Seq[ActionOption] = {
    Seq(
      ActionOption("parallelism", description = "Paimon remove_orphan_files parallelism"),
      ActionOption("mode", description = "Paimon remove_orphan_files mode"))
  }

  override def parse(commandLine: ProducerParsedOptions): ActionConfig = {
    PaimonRemoveOrphanFilesConfig(
      parallelism = parseInt(commandLine.getOptionValue("parallelism", "10"), "--parallelism"),
      mode = commandLine.getOptionValue("mode", "distributed").trim)
  }

  override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {
    val config = asRemoveOrphanFilesConfig(actionConfig)
    if (config.parallelism <= 0) {
      throw new IllegalArgumentException("--parallelism 必须大于 0")
    }
    if (config.mode.trim.isEmpty) {
      throw new IllegalArgumentException("--mode 的值不能为空")
    }
  }

  override def execute(
      context: ProducerContext,
      table: TargetTable,
      actionConfig: ActionConfig): ActionResult = {
    val config = asRemoveOrphanFilesConfig(actionConfig)
    val startNs = System.nanoTime()
    val sqlText =
      PaimonRemoveOrphanFilesAction.buildRemoveOrphanFilesSql(
        context.common.catalogName,
        config,
        table)
    val (deletedFileCount, deletedFileTotalLenInBytes) = parseMetrics(
      runner.run(context.spark, sqlText))
    val metrics =
      Map(
        "table" -> table.raw,
        "deletedFileCount" -> deletedFileCount.toString,
        "deletedFileTotalLenInBytes" -> deletedFileTotalLenInBytes.toString)
    ActionResult(
      status = ActionStatus.Success,
      tasks =
        Seq(
          ActionTaskResult(
            name = "remove_orphan_files",
            status = ActionStatus.Success,
            message = None,
            metrics = metrics)),
      message = None,
      metrics = metrics,
      costMs = (System.nanoTime() - startNs) / 1e6)
  }

  private def asRemoveOrphanFilesConfig(actionConfig: ActionConfig)
      : PaimonRemoveOrphanFilesConfig = {
    actionConfig match {
      case config: PaimonRemoveOrphanFilesConfig => config
      case other =>
        throw new IllegalArgumentException(
          s"Action remove-orphan-files requires PaimonRemoveOrphanFilesConfig, but got ${configType(other)}")
    }
  }

  private def configType(actionConfig: ActionConfig): String = {
    Option(actionConfig).map(_.getClass.getName).getOrElse("null")
  }

  private def parseMetrics(rows: Seq[org.apache.spark.sql.Row]): (Long, Long) = {
    var deletedFileCount = 0L
    var deletedFileTotalLenInBytes = 0L

    rows.foreach { row =>
      val countValue =
        fieldOpt(row, Set("deletedfilecount")).orElse(valueAt(row, 0)).getOrElse(0L)
      val bytesValue =
        fieldOpt(row, Set("deletedfiletotalleninbytes")).orElse(valueAt(row, 1)).getOrElse(0L)

      deletedFileCount += toLong(countValue)
      deletedFileTotalLenInBytes += toLong(bytesValue)
    }

    (deletedFileCount, deletedFileTotalLenInBytes)
  }

  private def parseInt(value: String, optionName: String): Int = {
    try {
      value.trim.toInt
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$optionName 必须是整数: $value")
    }
  }

  private def toLong(value: Any): Long = {
    value match {
      case null => 0L
      case n: java.lang.Number => n.longValue()
      case s: String =>
        val trimmed = s.trim
        if (trimmed.isEmpty) {
          0L
        } else {
          trimmed.toLong
        }
      case other => other.toString.toLong
    }
  }
}

object PaimonRemoveOrphanFilesAction {

  private[paimon] def buildRemoveOrphanFilesSql(
      catalogName: String,
      config: PaimonRemoveOrphanFilesConfig,
      table: TargetTable): String = {
    s"""
       |CALL ${quoteIdent(catalogName)}.sys.remove_orphan_files(
       |  table => ${sqlString(table.raw)},
       |  parallelism => ${config.parallelism},
       |  mode => ${sqlString(config.mode.trim)}
       |)
       |""".stripMargin
  }
}
