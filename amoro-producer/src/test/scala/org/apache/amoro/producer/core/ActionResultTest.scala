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

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.spi.{ActionConfig, LakeFormatAdapter, ProducerAction}

class ActionResultTest {

  @Test
  def taskResultUsesSpecDefinedFields(): Unit = {
    val task =
      ActionTaskResult(
        name = "db.tbl",
        status = ActionStatus.Success,
        message = Some("compact finished"),
        metrics = Map("table" -> "db.tbl"))

    assertEquals("db.tbl", task.name)
    assertEquals(ActionStatus.Success, task.status)
    assertEquals(Some("compact finished"), task.message)
    assertEquals("db.tbl", task.metrics("table"))
  }

  @Test
  def actionResultUsesSpecDefinedFields(): Unit = {
    val result =
      ActionResult(
        status = ActionStatus.Success,
        tasks =
          Seq(ActionTaskResult("db.tbl", ActionStatus.Success, None, Map("table" -> "db.tbl"))),
        message = Some("all tasks completed"),
        metrics = Map("table" -> "db.tbl"),
        costMs = 12.5d)

    assertEquals(ActionStatus.Success, result.status)
    assertEquals("db.tbl", result.tasks.head.name)
    assertEquals(Some("all tasks completed"), result.message)
    assertEquals("db.tbl", result.metrics("table"))
    assertEquals(12.5d, result.costMs)
  }

  @Test
  def producerCoreModelUsesSpecDefinedShape(): Unit = {
    val config =
      CommonProducerConfig(
        format = "paimon",
        action = "compact",
        catalogName = "catalog",
        databaseName = Some("db"),
        tableName = Some("tbl"),
        tableNameRegex = None,
        retryTimes = 3,
        continueOnTableFailure = true)
    val table = TargetTable(raw = "db.tbl", database = "db", table = "tbl")
    val adapter = new DummyAdapter
    val context = ProducerContext(spark = null, common = config, adapter = adapter)
    val action = new DummyAction

    assertEquals("paimon", config.format)
    assertEquals("compact", config.action)
    assertEquals("db.tbl", table.raw)
    assertEquals(config, context.common)
    assertEquals(adapter, context.adapter)
    assertEquals(Seq(action.name), adapter.actions.map(_.name))
    assertEquals(RetryMode.FrameworkTableRetry, action.retryMode)
  }

  final private class DummyConfig extends ActionConfig

  final private class DummyAction extends ProducerAction {
    override def name: String = "compact"

    override def retryMode: RetryMode = RetryMode.FrameworkTableRetry

    override def options: Seq[ActionOption] = Seq.empty

    override def parse(commandLine: ProducerParsedOptions): ActionConfig = new DummyConfig

    override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {}

    override def execute(
        context: ProducerContext,
        table: TargetTable,
        actionConfig: ActionConfig): ActionResult = {
      ActionResult(
        status = ActionStatus.Success,
        tasks =
          Seq(ActionTaskResult(table.raw, ActionStatus.Success, None, Map("table" -> table.raw))),
        message = None,
        metrics = Map("table" -> table.raw),
        costMs = 0.0d)
    }
  }

  final private class DummyAdapter extends LakeFormatAdapter {
    private val action = new DummyAction

    override def format: String = "paimon"

    override def actions: Seq[ProducerAction] = Seq(action)

    override def listTables(
        spark: SparkSession,
        config: CommonProducerConfig,
        database: String): Seq[TargetTable] = {
      Seq(TargetTable("db.tbl", database, "tbl"))
    }
  }
}
