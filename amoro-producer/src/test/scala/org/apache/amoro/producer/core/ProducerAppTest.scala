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

import java.io.{ByteArrayOutputStream, PrintStream}

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.spi.{ActionConfig, LakeFormatAdapter, ProducerAction}

class ProducerAppTest {

  @Test
  def frameworkTableRetryRetriesThrownAction(): Unit = {
    val action = new CountingAction(RetryMode.FrameworkTableRetry, failTimes = 2)
    val result =
      ProducerApp.executeOneTableWithRetry(
        context = fakeContext(action),
        action = action,
        actionConfig = NoopConfig,
        table = TargetTable("db.t1", "db", "t1"))

    assertEquals(ActionStatus.Success, result.status)
    assertEquals(3, action.calls)
  }

  @Test
  def actionManagedRetryDoesNotRetryInFramework(): Unit = {
    val action = new CountingAction(RetryMode.ActionManagedRetry, failTimes = 1)

    assertThrows(
      classOf[RuntimeException],
      () =>
        ProducerApp.executeOneTableWithRetry(
          fakeContext(action),
          action,
          NoopConfig,
          TargetTable("db.t1", "db", "t1")))
    assertEquals(1, action.calls)
  }

  @Test
  def executeTablesContinuesAfterFailedTableWhenConfigured(): Unit = {
    val action = new FailFirstTableAction
    val (output, results) =
      captureStdout {
        ProducerApp.executeTables(
          context = fakeContext(action, continueOnTableFailure = true),
          action = action,
          actionConfig = NoopConfig,
          targets = Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2")))
      }

    assertEquals(2, results.size)
    assertEquals(ActionStatus.Failed, results.head._2.status)
    assertEquals(ActionStatus.Success, results(1)._2.status)
    assertEquals(Some("failed table db.t1"), results.head._2.message)
    assertEquals(Seq("db.t1", "db.t2"), action.seenTables)
    val jsonLines = tableActionJsonLines(output)
    assertEquals(2, jsonLines.size)
    assertTrue(jsonLines.head.contains("\"status\":\"failed\""))
    assertTrue(jsonLines.head.contains("\"message\":\"failed table db.t1\""))
    assertTrue(jsonLines(1).contains("\"status\":\"success\""))
  }

  @Test
  def executeTablesLogsSuccessfulTableActionJson(): Unit = {
    val action =
      new CountingAction(
        RetryMode.ActionManagedRetry,
        failTimes = 0,
        actionName = "expire-snapshots",
        metrics = Map("deletedSnapshotsTotal" -> "7", "rounds" -> "2"))
    val (output, results) =
      captureStdout {
        ProducerApp.executeTables(
          context = fakeContext(action),
          action = action,
          actionConfig = NoopConfig,
          targets = Seq(TargetTable("db.t1", "db", "t1")))
      }

    assertEquals(1, results.size)
    val json = tableActionJsonLines(output).head
    assertTrue(json.contains("\"event\":\"lakehouse_producer_table_action\""))
    assertTrue(json.contains("\"format\":\"paimon\""))
    assertTrue(json.contains("\"catalog\":\"spark_catalog\""))
    assertTrue(json.contains("\"database\":\"db\""))
    assertTrue(json.contains("\"table\":\"t1\""))
    assertTrue(json.contains("\"target\":\"db.t1\""))
    assertTrue(json.contains("\"action\":\"expire-snapshots\""))
    assertTrue(json.contains("\"status\":\"success\""))
    assertTrue(json.contains("\"costMs\":"))
    assertTrue(json.contains("\"deletedSnapshotsTotal\":7"))
    assertTrue(json.contains(
      "\"metrics\":{\"deletedSnapshotsTotal\":\"7\",\"rounds\":\"2\",\"table\":\"db.t1\"}"))
    assertTrue(json.contains("\"message\":null"))
  }

  @Test
  def executeTablesLogsReturnedFailedJsonBeforeStopping(): Unit = {
    val action = new ReturnFailedFirstTableAction
    val (output, thrown) =
      captureStdout {
        assertThrows(
          classOf[RuntimeException],
          () =>
            ProducerApp.executeTables(
              context = fakeContext(action, continueOnTableFailure = false),
              action = action,
              actionConfig = NoopConfig,
              targets =
                Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2"))))
      }

    assertEquals(Seq("db.t1"), action.seenTables)
    assertTrue(thrown.getMessage.contains("failed table db.t1"))
    val jsonLines = tableActionJsonLines(output)
    assertEquals(1, jsonLines.size)
    assertTrue(jsonLines.head.contains("\"status\":\"failed\""))
    assertTrue(jsonLines.head.contains("\"message\":\"failed table db.t1\""))
  }

  @Test
  def tableActionJsonEscapesStringsAndUsesNullSnapshotCountWhenMissing(): Unit = {
    val table = TargetTable("db.t\"1", "d\\b", "t\n1")
    val json =
      TableActionJsonLog.render(
        common =
          fakeContext(new CountingAction(RetryMode.ActionManagedRetry, failTimes = 0))
            .common
            .copy(
              format = "pai\"mon",
              action = "expire\\snapshots",
              catalogName = "spark\ncatalog"),
        table = table,
        result =
          ActionResult(
            status = ActionStatus.Failed,
            tasks = Seq.empty,
            message = Some("bad \"message\"\\line\nnext"),
            metrics = Map("key\"1" -> "value\\1\nnext"),
            costMs = 0.0d),
        measuredCostMs = 12.5d)

    assertTrue(json.contains("\"format\":\"pai\\\"mon\""))
    assertTrue(json.contains("\"catalog\":\"spark\\ncatalog\""))
    assertTrue(json.contains("\"database\":\"d\\\\b\""))
    assertTrue(json.contains("\"table\":\"t\\n1\""))
    assertTrue(json.contains("\"action\":\"expire\\\\snapshots\""))
    assertTrue(json.contains("\"deletedSnapshotsTotal\":null"))
    assertTrue(json.contains("\"metrics\":{\"key\\\"1\":\"value\\\\1\\nnext\"}"))
    assertTrue(json.contains("\"message\":\"bad \\\"message\\\"\\\\line\\nnext\""))
  }

  @Test
  def tableActionJsonUsesNullSnapshotCountForNonNumericMetric(): Unit = {
    val json =
      TableActionJsonLog.render(
        common =
          fakeContext(new CountingAction(RetryMode.ActionManagedRetry, failTimes = 0)).common,
        table = TargetTable("db.t1", "db", "t1"),
        result =
          ActionResult(
            status = ActionStatus.Success,
            tasks = Seq.empty,
            message = None,
            metrics = Map("deletedSnapshotsTotal" -> "unknown"),
            costMs = 0.0d),
        measuredCostMs = 1.0d)

    assertTrue(json.contains("\"deletedSnapshotsTotal\":null"))
    assertTrue(json.contains("\"metrics\":{\"deletedSnapshotsTotal\":\"unknown\"}"))
  }

  @Test
  def tableActionJsonIgnoresNullMetricKeysAndRendersNullMetricValues(): Unit = {
    val metrics =
      Map(
        null.asInstanceOf[String] -> "ignored",
        "deletedSnapshotsTotal" -> null.asInstanceOf[String],
        "empty" -> null.asInstanceOf[String])
    val json =
      TableActionJsonLog.render(
        common =
          fakeContext(new CountingAction(RetryMode.ActionManagedRetry, failTimes = 0)).common,
        table = TargetTable("db.t1", "db", "t1"),
        result =
          ActionResult(
            status = ActionStatus.Success,
            tasks = Seq.empty,
            message = None,
            metrics = metrics,
            costMs = 0.0d),
        measuredCostMs = 1.0d)

    assertTrue(json.contains("\"deletedSnapshotsTotal\":null"))
    assertTrue(json.contains("\"metrics\":{\"deletedSnapshotsTotal\":null,\"empty\":null}"))
    assertFalse(json.contains("ignored"))
  }

  @Test
  def executeTablesContinuesAfterReturnedFailedTableWhenConfigured(): Unit = {
    val action = new ReturnFailedFirstTableAction
    val (output, results) =
      captureStdout {
        ProducerApp.executeTables(
          context = fakeContext(action, continueOnTableFailure = true),
          action = action,
          actionConfig = NoopConfig,
          targets = Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2")))
      }

    assertEquals(2, results.size)
    assertEquals(ActionStatus.Failed, results.head._2.status)
    assertEquals(ActionStatus.Success, results(1)._2.status)
    assertEquals(Seq("db.t1", "db.t2"), action.seenTables)
    val jsonLines = tableActionJsonLines(output)
    assertEquals(2, jsonLines.size)
    assertTrue(jsonLines.head.contains("\"status\":\"failed\""))
    assertTrue(jsonLines(1).contains("\"status\":\"success\""))
  }

  @Test
  def executeTablesStopsAfterFailedTableWhenConfigured(): Unit = {
    val action = new FailFirstTableAction
    val (output, thrown) =
      captureStdout {
        assertThrows(
          classOf[RuntimeException],
          () =>
            ProducerApp.executeTables(
              context = fakeContext(action, continueOnTableFailure = false),
              action = action,
              actionConfig = NoopConfig,
              targets =
                Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2"))))
      }

    assertEquals(Seq("db.t1"), action.seenTables)
    assertTrue(thrown.getMessage.contains("failed table db.t1"))
    val jsonLines = tableActionJsonLines(output)
    assertEquals(1, jsonLines.size)
    assertTrue(jsonLines.head.contains("\"status\":\"failed\""))
    assertTrue(jsonLines.head.contains("\"message\":\"failed table db.t1\""))
  }

  @Test
  def executeTablesStopsAfterReturnedFailedTableWhenConfigured(): Unit = {
    val action = new ReturnFailedFirstTableAction
    val (output, _) =
      captureStdout {
        assertThrows(
          classOf[RuntimeException],
          () =>
            ProducerApp.executeTables(
              context = fakeContext(action, continueOnTableFailure = false),
              action = action,
              actionConfig = NoopConfig,
              targets =
                Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2"))))
      }

    assertEquals(Seq("db.t1"), action.seenTables)
    assertEquals(1, tableActionJsonLines(output).size)
  }

  @Test
  def executeTablesFailsFastWhenFrameworkRetryTimesIsInvalid(): Unit = {
    val action = new CountingAction(RetryMode.FrameworkTableRetry, failTimes = 0)
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () =>
          ProducerApp.executeTables(
            context = fakeContext(action, retryTimes = 0, continueOnTableFailure = true),
            action = action,
            actionConfig = NoopConfig,
            targets = Seq(TargetTable("db.t1", "db", "t1"), TargetTable("db.t2", "db", "t2"))))

    assertTrue(exception.getMessage.contains("retryTimes"))
    assertEquals(0, action.calls)
  }

  @Test
  def resolveAdapterMatchesCaseInsensitiveFormat(): Unit = {
    val paimon = new FakeAdapter("paimon", Seq.empty)
    val iceberg = new FakeAdapter("iceberg", Seq.empty)

    assertEquals(paimon, ProducerApp.resolveAdapter("PAIMON", Seq(iceberg, paimon)))
  }

  @Test
  def resolveAdapterRejectsMissingFormat(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => ProducerApp.resolveAdapter("hudi", Seq(new FakeAdapter("paimon", Seq.empty))))
  }

  @Test
  def resolveAdapterRejectsDuplicateNormalizedFormats(): Unit = {
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () =>
          ProducerApp.resolveAdapter(
            "paimon",
            Seq(new FakeAdapter("paimon", Seq.empty), new FakeAdapter(" PAIMON ", Seq.empty))))

    assertTrue(exception.getMessage.contains("paimon"))
  }

  @Test
  def resolveActionMatchesCaseInsensitiveName(): Unit = {
    val compact =
      new CountingAction(RetryMode.FrameworkTableRetry, failTimes = 0, actionName = "compact")
    val expire =
      new CountingAction(RetryMode.FrameworkTableRetry, failTimes = 0, actionName = "expire")
    val adapter = new FakeAdapter("paimon", Seq(expire, compact))

    assertEquals(compact, ProducerApp.resolveAction(adapter, "COMPACT"))
  }

  @Test
  def resolveActionRejectsMissingName(): Unit = {
    val adapter =
      new FakeAdapter(
        "paimon",
        Seq(new CountingAction(RetryMode.FrameworkTableRetry, failTimes = 0)))

    assertThrows(
      classOf[IllegalArgumentException],
      () => ProducerApp.resolveAction(adapter, "missing"))
  }

  @Test
  def resolveActionRejectsDuplicateNormalizedNames(): Unit = {
    val adapter =
      new FakeAdapter(
        "paimon",
        Seq(
          new CountingAction(
            RetryMode.FrameworkTableRetry,
            failTimes = 0,
            actionName = "compact"),
          new CountingAction(
            RetryMode.FrameworkTableRetry,
            failTimes = 0,
            actionName = " COMPACT ")))
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () => ProducerApp.resolveAction(adapter, "compact"))

    assertTrue(exception.getMessage.contains("compact"))
  }

  @Test
  def producerSummaryCountsTablesAndTasksByStatus(): Unit = {
    val successTable = TargetTable("db.success", "db", "success")
    val failedTable = TargetTable("db.failed", "db", "failed")
    val skippedTable = TargetTable("db.skipped", "db", "skipped")
    val results =
      Seq(
        successTable ->
          ActionResult(
            status = ActionStatus.Success,
            tasks =
              Seq(
                ActionTaskResult("success-1", ActionStatus.Success, None, Map.empty),
                ActionTaskResult("success-2", ActionStatus.Success, None, Map.empty)),
            message = None,
            metrics = Map.empty,
            costMs = 1.0d),
        failedTable ->
          ActionResult(
            status = ActionStatus.Failed,
            tasks =
              Seq(
                ActionTaskResult("failed-1", ActionStatus.Failed, None, Map.empty),
                ActionTaskResult("failed-skipped", ActionStatus.Skipped, None, Map.empty)),
            message = Some("failed"),
            metrics = Map.empty,
            costMs = 2.0d),
        skippedTable ->
          ActionResult(
            status = ActionStatus.Skipped,
            tasks = Seq.empty,
            message = Some("skipped"),
            metrics = Map.empty,
            costMs = 0.0d))

    val summary = ProducerSummary.fromResults(results)

    assertEquals(3, summary.tableCount)
    assertEquals(Seq(successTable), summary.successTables)
    assertEquals(Seq(failedTable), summary.failedTables)
    assertEquals(Seq(skippedTable), summary.skippedTables)
    assertEquals(Seq(FailedTableSummary(failedTable, "failed")), summary.failedTableSummaries)
    assertEquals(2, summary.taskSuccessCount)
    assertEquals(1, summary.taskFailedCount)
  }

  @Test
  def producerSummaryUsesFailedTaskMessageWhenResultMessageIsMissing(): Unit = {
    val failedTable = TargetTable("db.failed", "db", "failed")
    val summary =
      ProducerSummary.fromResults(
        Seq(
          failedTable ->
            ActionResult(
              status = ActionStatus.Failed,
              tasks =
                Seq(
                  ActionTaskResult(
                    "round-1",
                    ActionStatus.Failed,
                    Some("task failed"),
                    Map.empty)),
              message = None,
              metrics = Map.empty,
              costMs = 2.0d)))

    assertEquals(Seq(FailedTableSummary(failedTable, "task failed")), summary.failedTableSummaries)
  }

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

  private def fakeContext(
      action: ProducerAction,
      retryTimes: Int = 3,
      continueOnTableFailure: Boolean = true): ProducerContext = {
    ProducerContext(
      spark = null,
      common =
        CommonProducerConfig(
          format = "paimon",
          action = action.name,
          catalogName = "spark_catalog",
          databaseName = Some("db"),
          tableName = None,
          tableNameRegex = None,
          retryTimes = retryTimes,
          continueOnTableFailure = continueOnTableFailure),
      adapter = new FakeAdapter("paimon", Seq(action)))
  }

  private object NoopConfig extends ActionConfig

  final private class CountingAction(
      mode: RetryMode,
      failTimes: Int,
      actionName: String = "compact",
      metrics: Map[String, String] = Map.empty)
    extends ProducerAction {
    var calls: Int = 0

    override def name: String = actionName

    override def retryMode: RetryMode = mode

    override def options: Seq[ActionOption] = Seq.empty

    override def parse(commandLine: ProducerParsedOptions): ActionConfig = NoopConfig

    override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {}

    override def execute(
        context: ProducerContext,
        table: TargetTable,
        actionConfig: ActionConfig): ActionResult = {
      calls += 1
      if (calls <= failTimes) {
        throw new RuntimeException(s"failed attempt $calls")
      }
      ActionResult(
        status = ActionStatus.Success,
        tasks = Seq(ActionTaskResult(table.raw, ActionStatus.Success, None, Map.empty)),
        message = None,
        metrics = metrics + ("table" -> table.raw),
        costMs = calls.toDouble)
    }
  }

  final private class FailFirstTableAction extends ProducerAction {
    var seenTables: Seq[String] = Seq.empty

    override def name: String = "compact"

    override def retryMode: RetryMode = RetryMode.ActionManagedRetry

    override def options: Seq[ActionOption] = Seq.empty

    override def parse(commandLine: ProducerParsedOptions): ActionConfig = NoopConfig

    override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {}

    override def execute(
        context: ProducerContext,
        table: TargetTable,
        actionConfig: ActionConfig): ActionResult = {
      seenTables = seenTables :+ table.raw
      if (seenTables.size == 1) {
        throw new RuntimeException(s"failed table ${table.raw}")
      }
      ActionResult(
        status = ActionStatus.Success,
        tasks = Seq(ActionTaskResult(table.raw, ActionStatus.Success, None, Map.empty)),
        message = None,
        metrics = Map("table" -> table.raw),
        costMs = 0.0d)
    }
  }

  final private class ReturnFailedFirstTableAction extends ProducerAction {
    var seenTables: Seq[String] = Seq.empty

    override def name: String = "compact"

    override def retryMode: RetryMode = RetryMode.ActionManagedRetry

    override def options: Seq[ActionOption] = Seq.empty

    override def parse(commandLine: ProducerParsedOptions): ActionConfig = NoopConfig

    override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {}

    override def execute(
        context: ProducerContext,
        table: TargetTable,
        actionConfig: ActionConfig): ActionResult = {
      seenTables = seenTables :+ table.raw
      val status = if (seenTables.size == 1) ActionStatus.Failed else ActionStatus.Success
      ActionResult(
        status = status,
        tasks = Seq(ActionTaskResult(table.raw, status, None, Map.empty)),
        message = if (status == ActionStatus.Failed) Some(s"failed table ${table.raw}") else None,
        metrics = Map("table" -> table.raw),
        costMs = 0.0d)
    }
  }

  final private class FakeAdapter(formatName: String, adapterActions: Seq[ProducerAction])
    extends LakeFormatAdapter {
    override def format: String = formatName

    override def actions: Seq[ProducerAction] = adapterActions

    override def listTables(
        spark: SparkSession,
        config: CommonProducerConfig,
        database: String): Seq[TargetTable] = Seq.empty
  }
}
