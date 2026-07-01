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

package org.apache.amoro.producer

import java.io.{ByteArrayOutputStream, PrintStream}

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNull, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core.{ActionOption, ActionResult, ActionStatus, ActionTaskResult, CommonProducerConfig, ProducerApp, ProducerContext, ProducerParsedOptions, RetryMode, TargetTable}
import org.apache.amoro.producer.spi.{ActionConfig, LakeFormatAdapter, ProducerAction}

class LakehouseProducerTest {

  @Test
  def helpReturnsZeroAndDoesNotCreateSpark(): Unit = {
    var sparkCreated = false

    val exitCode =
      ProducerApp.runInternal(
        Array("--help"),
        Seq(new FakeAdapter("paimon", new FakeAction("compact"))),
        () => {
          sparkCreated = true
          null
        })

    assertEquals(0, exitCode)
    assertFalse(sparkCreated)
  }

  @Test
  def missingFormatOrActionReturnsTwoAndDoesNotCreateSpark(): Unit = {
    var sparkCreated = false
    val adapter = new FakeAdapter("paimon", new FakeAction("compact"))
    val builder = () => {
      sparkCreated = true
      null
    }

    assertEquals(2, ProducerApp.runInternal(Array("--action", "compact"), Seq(adapter), builder))
    assertEquals(2, ProducerApp.runInternal(Array("--format", "paimon"), Seq(adapter), builder))
    assertFalse(sparkCreated)
  }

  @Test
  def invalidTargetOptionsReturnTwoAndDoNotCreateSpark(): Unit = {
    var sparkCreated = false
    val adapter = new FakeAdapter("paimon", new FakeAction("compact"))
    val builder = () => {
      sparkCreated = true
      null
    }

    assertEquals(
      2,
      ProducerApp.runInternal(
        Array("--format", "paimon", "--action", "compact"),
        Seq(adapter),
        builder))
    assertEquals(
      2,
      ProducerApp.runInternal(
        Array("--format", "paimon", "--action", "compact", "--tableName", "t1"),
        Seq(adapter),
        builder))
    assertEquals(
      2,
      ProducerApp.runInternal(
        Array(
          "--format",
          "paimon",
          "--action",
          "compact",
          "--databaseName",
          "db",
          "--tableNameRegex",
          "["),
        Seq(adapter),
        builder))
    assertFalse(sparkCreated)
  }

  @Test
  def unsupportedFormatOrActionReturnsTwoAndDoesNotCreateSpark(): Unit = {
    var sparkCreated = false
    val adapter = new FakeAdapter("paimon", new FakeAction("compact"))
    val builder = () => {
      sparkCreated = true
      null
    }

    assertEquals(
      2,
      ProducerApp.runInternal(
        Array("--format", "iceberg", "--action", "compact"),
        Seq(adapter),
        builder))
    assertEquals(
      2,
      ProducerApp.runInternal(
        Array("--format", "paimon", "--action", "missing"),
        Seq(adapter),
        builder))
    assertFalse(sparkCreated)
  }

  @Test
  def sparkBuilderIllegalArgumentExceptionReturnsOne(): Unit = {
    val exitCode =
      ProducerApp.runInternal(
        Array("--format", "paimon", "--action", "compact", "--databaseName", "db"),
        Seq(new FakeAdapter("paimon", new FakeAction("compact"))),
        () => throw new IllegalArgumentException("builder failed"))

    assertEquals(1, exitCode)
  }

  @Test
  def actionRuntimeIllegalArgumentExceptionReturnsOne(): Unit = {
    val exitCode =
      ProducerApp.runInternal(
        Array(
          "--format",
          "paimon",
          "--action",
          "compact",
          "--databaseName",
          "db",
          "--continueOnTableFailure",
          "false"),
        Seq(new FakeAdapter("paimon", new FakeAction("compact", failExecution = true))),
        () => null)

    assertEquals(1, exitCode)
  }

  @Test
  def actionReturnedFailedStatusReturnsZeroWhenContinueTrue(): Unit = {
    val exitCode =
      ProducerApp.runInternal(
        Array("--format", "paimon", "--action", "compact", "--databaseName", "db"),
        Seq(
          new FakeAdapter(
            "paimon",
            new FakeAction(
              "compact",
              resultStatusByTable = Map("db.t1" -> ActionStatus.Failed)))),
        () => null)

    assertEquals(0, exitCode)
  }

  @Test
  def partialReturnedFailedStatusReturnsZeroAndContinuesWhenConfigured(): Unit = {
    val action =
      new FakeAction(
        "compact",
        resultStatusByTable = Map("db.t1" -> ActionStatus.Failed))
    val adapter =
      new FakeAdapter(
        "paimon",
        action,
        tables = Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2")))

    val (output, exitCode) =
      captureStdout {
        ProducerApp.runInternal(
          Array("--format", "paimon", "--action", "compact", "--databaseName", "db"),
          Seq(adapter),
          () => null)
      }

    assertEquals(0, exitCode)
    assertTrue(output.contains("failed tables:"))
    assertTrue(output.contains("  - db.t1 | failed table db.t1"))
    assertEquals(
      Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2")),
      action.executedTables)
  }

  @Test
  def returnedFailedStatusStopsAndReturnsOneWhenContinueFalse(): Unit = {
    val action =
      new FakeAction(
        "compact",
        resultStatusByTable = Map("db.t1" -> ActionStatus.Failed))
    val adapter =
      new FakeAdapter(
        "paimon",
        action,
        tables = Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2")))

    val exitCode =
      ProducerApp.runInternal(
        Array(
          "--format",
          "paimon",
          "--action",
          "compact",
          "--databaseName",
          "db",
          "--continueOnTableFailure",
          "false"),
        Seq(adapter),
        () => null)

    assertEquals(1, exitCode)
    assertEquals(Seq(TargetTable("db.t1", "db", "t1")), action.executedTables)
  }

  @Test
  def runInternalPrintsTableActionJsonForEachTableAndKeepsSummary(): Unit = {
    val action =
      new FakeAction(
        "expire-snapshots",
        metricsByTable =
          Map(
            "db.t1" -> Map("deletedSnapshotsTotal" -> "3", "rounds" -> "1"),
            "db.t2" -> Map("deletedSnapshotsTotal" -> "5", "rounds" -> "2")))
    val adapter =
      new FakeAdapter(
        "paimon",
        action,
        tables = Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2")))

    val (output, exitCode) =
      captureStdout {
        ProducerApp.runInternal(
          Array("--format", "paimon", "--action", "expire-snapshots", "--databaseName", "db"),
          Seq(adapter),
          () => null)
      }

    assertEquals(0, exitCode)
    val jsonLines = tableActionJsonLines(output)
    assertEquals(2, jsonLines.size)
    assertTrue(jsonLines.head.contains("\"action\":\"expire-snapshots\""))
    assertTrue(jsonLines.head.contains("\"target\":\"db.t1\""))
    assertTrue(jsonLines.head.contains("\"deletedSnapshotsTotal\":3"))
    assertTrue(jsonLines(1).contains("\"target\":\"db.t2\""))
    assertTrue(jsonLines(1).contains("\"deletedSnapshotsTotal\":5"))
    assertTrue(output.contains("=========== PRODUCER SUMMARY ==========="))
    assertEquals(
      Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2")),
      action.executedTables)
  }

  @Test
  def resolveAdapterAndActionSuccessfullyBeforeValidation(): Unit = {
    var sparkCreated = false
    val action = new FakeAction("compact", failValidation = true)
    val adapter = new FakeAdapter("paimon", action)

    val exitCode =
      ProducerApp.runInternal(
        Array("--format", "PAIMON", "--action", "COMPACT"),
        Seq(adapter),
        () => {
          sparkCreated = true
          null
        })

    assertEquals(2, exitCode)
    assertEquals(1, action.parseCalls)
    assertEquals(1, action.validateCalls)
    assertFalse(sparkCreated)
  }

  @Test
  def runInternalResolvesTargetsAndExecutesTables(): Unit = {
    val action = new FakeAction("compact")
    val adapter = new FakeAdapter("paimon", action)
    var sparkCreated = false

    val exitCode =
      ProducerApp.runInternal(
        Array("--format", "paimon", "--action", "compact", "--databaseName", "db"),
        Seq(adapter),
        () => {
          sparkCreated = true
          null
        })

    assertEquals(0, exitCode)
    assertTrue(sparkCreated)
    assertEquals(Seq("db"), adapter.listedDatabases)
    assertEquals(Seq(TargetTable("db.t1", "db", "t1")), action.executedTables)
    assertNull(action.contextSpark)
  }

  @Test
  def lakehouseProducerUsesPaimonAdapter(): Unit = {
    assertEquals(Seq("paimon"), LakehouseProducer.adapters.map(_.format))
  }

  private object NoopConfig extends ActionConfig

  private def captureStdout[T](block: => T): (String, T) = {
    val output = new ByteArrayOutputStream()
    val printStream = new PrintStream(output, true, "UTF-8")
    val result = Console.withOut(printStream) {
      block
    }
    printStream.flush()
    output.toString("UTF-8") -> result
  }

  private def tableActionJsonLines(output: String): Seq[String] = {
    output.linesIterator.filter(_.contains("\"event\":\"lakehouse_producer_table_action\"")).toSeq
  }

  final private class FakeAdapter(
      formatName: String,
      action: ProducerAction,
      tables: Seq[TargetTable] = Seq(TargetTable("db.t1", "db", "t1")))
    extends LakeFormatAdapter {
    var listedDatabases: Seq[String] = Seq.empty

    override def format: String = formatName

    override def actions: Seq[ProducerAction] = Seq(action)

    override def listTables(
        spark: SparkSession,
        config: CommonProducerConfig,
        database: String): Seq[TargetTable] = {
      listedDatabases = listedDatabases :+ database
      tables.map(table => table.copy(raw = s"$database.${table.table}", database = database))
    }
  }

  final private class FakeAction(
      actionName: String,
      failValidation: Boolean = false,
      failExecution: Boolean = false,
      resultStatusByTable: Map[String, ActionStatus] = Map.empty,
      metricsByTable: Map[String, Map[String, String]] = Map.empty)
    extends ProducerAction {
    var parseCalls = 0
    var validateCalls = 0
    var executedTables: Seq[TargetTable] = Seq.empty
    var contextSpark: SparkSession = _

    override def name: String = actionName

    override def retryMode: RetryMode = RetryMode.ActionManagedRetry

    override def options: Seq[ActionOption] = Seq.empty

    override def parse(commandLine: ProducerParsedOptions): ActionConfig = {
      parseCalls += 1
      NoopConfig
    }

    override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {
      validateCalls += 1
      if (failValidation) {
        throw new IllegalArgumentException("validation failed")
      }
    }

    override def execute(
        context: ProducerContext,
        table: TargetTable,
        actionConfig: ActionConfig): ActionResult = {
      contextSpark = context.spark
      executedTables = executedTables :+ table
      if (failExecution) {
        throw new IllegalArgumentException("execution failed")
      }
      val status = resultStatusByTable.getOrElse(table.raw, ActionStatus.Success)
      ActionResult(
        status = status,
        tasks = Seq(ActionTaskResult(table.raw, status, None, Map.empty)),
        message = if (status == ActionStatus.Failed) Some(s"failed table ${table.raw}") else None,
        metrics = metricsByTable.getOrElse(table.raw, Map.empty) + ("table" -> table.raw),
        costMs = 0.0d)
    }
  }
}
