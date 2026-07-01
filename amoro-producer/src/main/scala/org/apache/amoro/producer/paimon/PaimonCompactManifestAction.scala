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

final case class PaimonCompactManifestConfig(procedureOptions: String) extends ActionConfig

class PaimonCompactManifestAction(runner: PaimonSqlRunner = SparkPaimonSqlRunner)
  extends ProducerAction {

  override def name: String = "compact-manifest"

  override def retryMode: RetryMode = RetryMode.FrameworkTableRetry

  override def options: Seq[ActionOption] = {
    Seq(ActionOption("procedureOptions", description = "Paimon compact_manifest procedure options"))
  }

  override def parse(commandLine: ProducerParsedOptions): ActionConfig = {
    PaimonCompactManifestConfig(procedureOptions =
      commandLine.getOptionValue("procedureOptions", "").trim)
  }

  override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {
    asCompactManifestConfig(actionConfig)
  }

  override def execute(
      context: ProducerContext,
      table: TargetTable,
      actionConfig: ActionConfig): ActionResult = {
    val config = asCompactManifestConfig(actionConfig)
    val startNs = System.nanoTime()
    val sqlText =
      PaimonCompactManifestAction.buildCompactManifestSql(
        context.common.catalogName,
        config,
        table)
    val rows = runner.run(context.spark, sqlText)
    val result = parseResult(rows)
    val metrics = Map("table" -> table.raw, "result" -> result.toString)
    ActionResult(
      status = ActionStatus.Success,
      tasks =
        Seq(
          ActionTaskResult(
            name = "compact_manifest",
            status = ActionStatus.Success,
            message = None,
            metrics = metrics)),
      message = None,
      metrics = metrics,
      costMs = (System.nanoTime() - startNs) / 1e6)
  }

  private def asCompactManifestConfig(actionConfig: ActionConfig): PaimonCompactManifestConfig = {
    actionConfig match {
      case config: PaimonCompactManifestConfig => config
      case other =>
        throw new IllegalArgumentException(
          s"Action compact-manifest requires PaimonCompactManifestConfig, but got ${configType(other)}")
    }
  }

  private def configType(actionConfig: ActionConfig): String = {
    Option(actionConfig).map(_.getClass.getName).getOrElse("null")
  }

  private def parseResult(rows: Seq[org.apache.spark.sql.Row]): Boolean = {
    rows.headOption
      .flatMap(row => fieldOpt(row, Set("result")).orElse(valueAt(row, 0)))
      .exists(toBoolean)
  }

  private def toBoolean(value: Any): Boolean = {
    value match {
      case null => false
      case b: java.lang.Boolean => b.booleanValue()
      case s: String => s.trim.equalsIgnoreCase("true") || s.trim == "1"
      case i: java.lang.Integer => i == 1
      case l: java.lang.Long => l == 1L
      case other => other.toString.trim.equalsIgnoreCase("true")
    }
  }
}

object PaimonCompactManifestAction {

  private[paimon] def buildCompactManifestSql(
      catalogName: String,
      config: PaimonCompactManifestConfig,
      table: TargetTable): String = {
    val optionsClause =
      if (config.procedureOptions.trim.isEmpty) {
        ""
      } else {
        s",\n  options => ${sqlString(config.procedureOptions.trim)}"
      }

    s"""
       |CALL ${quoteIdent(catalogName)}.sys.compact_manifest(
       |  table => ${sqlString(table.raw)}$optionsClause
       |)
       |""".stripMargin
  }
}
