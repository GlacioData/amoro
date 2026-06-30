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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core.{ActionStatus, CommonProducerConfig, ProducerCliParser, ProducerContext, RetryMode, TargetTable}
import org.apache.amoro.producer.spi.{ActionConfig, LakeFormatAdapter, ProducerAction}

class PaimonExpireSnapshotsActionTest {

  @Test
  def parseUsesDefaultsAndTrimsArguments(): Unit = {
    val action = new PaimonExpireSnapshotsAction()

    assertEquals("expire-snapshots", action.name)
    assertEquals(RetryMode.ActionManagedRetry, action.retryMode)
    assertEquals(
      PaimonExpireSnapshotsConfig(
        retainMax = 20,
        retainMin = 10,
        maxDeletes = 550L,
        procedureOptions = "file-operation.thread-num=32,snapshot.expire.execution-mode=sync"),
      parse(action))
    assertEquals(
      PaimonExpireSnapshotsConfig(2, 1, 9L, "k=v"),
      parse(
        action,
        Array(
          "--retainMax",
          " 2 ",
          "--retainMin",
          " 1 ",
          "--maxDeletes",
          " 9 ",
          "--procedureOptions",
          " k=v ")))
  }

  @Test
  def buildSqlIncludesCatalogTableRetainLimitsMaxDeletesAndOptions(): Unit = {
    val sql =
      PaimonExpireSnapshotsAction.buildExpireSnapshotsSql(
        catalogName = "pa`imon",
        config = PaimonExpireSnapshotsConfig(20, 10, 550L, "k='v'"),
        table = table)

    assertTrue(sql.contains("CALL `pa``imon`.sys.expire_snapshots("))
    assertTrue(sql.contains("table => 'db.t1'"))
    assertTrue(sql.contains("retain_max => 20"))
    assertTrue(sql.contains("retain_min => 10"))
    assertTrue(sql.contains("max_deletes => 550"))
    assertTrue(sql.contains("options => 'k=''v'''"))
  }

  @Test
  def buildSqlOmitsOptionsWhenProcedureOptionsIsEmpty(): Unit = {
    val sql =
      PaimonExpireSnapshotsAction.buildExpireSnapshotsSql(
        catalogName = "paimon",
        config = PaimonExpireSnapshotsConfig(20, 10, 550L, " "),
        table = table)

    assertTrue(sql.contains("CALL `paimon`.sys.expire_snapshots("))
    assertTrue(sql.contains("max_deletes => 550"))
    assertFalse(sql.contains("options =>"))
  }

  @Test
  def validateRejectsInvalidArguments(): Unit = {
    val action = new PaimonExpireSnapshotsAction()
    val common = commonConfig()

    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonExpireSnapshotsConfig(-1, 10, 550L, "")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonExpireSnapshotsConfig(20, -1, 550L, "")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonExpireSnapshotsConfig(20, 10, -1L, "")))
    val error =
      assertThrows(
        classOf[IllegalArgumentException],
        () => action.validate(common, PaimonExpireSnapshotsConfig(20, 10, 0L, "")))
    assertEquals("--maxDeletes 必须大于 0", error.getMessage)
  }

  @Test
  def validateRejectsWrongActionConfigType(): Unit = {
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () => new PaimonExpireSnapshotsAction().validate(commonConfig(), WrongConfig))

    assertTrue(exception.getMessage.contains("expire-snapshots"))
    assertTrue(exception.getMessage.contains("PaimonExpireSnapshotsConfig"))
  }

  @Test
  def executeRejectsZeroMaxDeletesBeforeRunningSql(): Unit = {
    val runner = new FakeRunner(Seq(success(Row(20L))))
    val error =
      assertThrows(
        classOf[IllegalArgumentException],
        () =>
          new PaimonExpireSnapshotsAction(runner)
            .execute(context(), table, defaultConfig.copy(maxDeletes = 0L)))

    assertEquals("--maxDeletes 必须大于 0", error.getMessage)
    assertTrue(runner.sqls.isEmpty)
  }

  @Test
  def executeRejectsNonPositiveRetryTimesBeforeRunningSql(): Unit = {
    val runner = new FakeRunner(Seq(success(Row(20L))))

    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        new PaimonExpireSnapshotsAction(runner)
          .execute(context(retryTimes = 0), table, defaultConfig))
    assertTrue(runner.sqls.isEmpty)
  }

  @Test
  def executeRepeatsRoundsUntilDeletedCountIsLessThanMaxDeletes(): Unit = {
    val runner = new FakeRunner(Seq(success(Row(550L)), success(Row(550L)), success(Row(20L))))
    val result =
      new PaimonExpireSnapshotsAction(runner)
        .execute(context(), table, defaultConfig)

    assertEquals(ActionStatus.Success, result.status)
    assertEquals(3, runner.sqls.size)
    assertEquals(Seq("success", "success", "success"), runner.outcomes)
    assertEquals(Seq("round-1", "round-2", "round-3"), result.tasks.map(_.name))
    assertEquals(
      Seq(ActionStatus.Success, ActionStatus.Success, ActionStatus.Success),
      result.tasks.map(_.status))
    assertEquals("3", result.metrics("rounds"))
    assertEquals("1120", result.metrics("totalDeletedCount"))
    assertEquals("1120", result.metrics("deletedSnapshotsTotal"))
    assertEquals("550", result.metrics("maxDeletes"))
  }

  @Test
  def executeRetriesOnlyTheCurrentRoundWhenSqlFails(): Unit = {
    val runner =
      new FakeRunner(
        Seq(
          success(Row(550L)),
          failure(new RuntimeException("round 2 failed")),
          success(Row(20L))))
    val result =
      new PaimonExpireSnapshotsAction(runner)
        .execute(context(retryTimes = 2), table, defaultConfig)

    assertEquals(ActionStatus.Success, result.status)
    assertEquals(3, runner.sqls.size)
    assertEquals(Seq("success", "failure", "success"), runner.outcomes)
    assertEquals(Seq("round-1", "round-2"), result.tasks.map(_.name))
    assertEquals("1", result.tasks.head.metrics("attempts"))
    assertEquals("2", result.tasks(1).metrics("attempts"))
  }

  @Test
  def executeThrowsWhenCurrentRoundRetryIsExhausted(): Unit = {
    val runner =
      new FakeRunner(
        Seq(
          success(Row(550L)),
          failure(new RuntimeException("round 2 failed 1")),
          failure(new RuntimeException("round 2 failed 2"))))

    assertThrows(
      classOf[RuntimeException],
      () =>
        new PaimonExpireSnapshotsAction(runner)
          .execute(context(retryTimes = 2), table, defaultConfig))
    assertEquals(3, runner.sqls.size)
    assertEquals(Seq("success", "failure", "failure"), runner.outcomes)
  }

  @Test
  def executeParsesDeletedCountFromKnownFieldsAndFirstColumnFallback(): Unit = {
    assertEquals(
      "1",
      executeOnce(
        new GenericRowWithSchema(
          Array[Any](1L),
          StructType(Seq(StructField("deleted_count", LongType))))).metrics("totalDeletedCount"))
    assertEquals(
      "1",
      executeOnce(
        new GenericRowWithSchema(
          Array[Any](1L),
          StructType(Seq(StructField("deleted_count", LongType))))).metrics(
        "deletedSnapshotsTotal"))
    assertEquals(
      "2",
      executeOnce(
        new GenericRowWithSchema(
          Array[Any](2L),
          StructType(Seq(StructField("deleted_snapshots", LongType))))).metrics(
        "totalDeletedCount"))
    assertEquals(
      "3",
      executeOnce(
        new GenericRowWithSchema(
          Array[Any](3L),
          StructType(Seq(StructField("result", LongType))))).metrics("totalDeletedCount"))
    assertEquals(
      "4",
      executeOnce(
        new GenericRowWithSchema(
          Array[Any](4L),
          StructType(Seq(StructField("deleted_snapshots_count", LongType))))).metrics(
        "totalDeletedCount"))
    assertEquals("5", executeOnce(Row(5L)).metrics("totalDeletedCount"))
  }

  private def executeOnce(row: Row): org.apache.amoro.producer.core.ActionResult = {
    new PaimonExpireSnapshotsAction(new FakeRunner(Seq(success(row))))
      .execute(context(), table, defaultConfig)
  }

  private def parse(
      action: PaimonExpireSnapshotsAction,
      args: Array[String] = Array.empty): PaimonExpireSnapshotsConfig = {
    val commandLine = ProducerCliParser.parseMerged(action.options, args)
    action.parse(commandLine).asInstanceOf[PaimonExpireSnapshotsConfig]
  }

  private def context(retryTimes: Int = 3): ProducerContext = {
    ProducerContext(null.asInstanceOf[SparkSession], commonConfig(retryTimes), DummyAdapter)
  }

  private def commonConfig(retryTimes: Int = 3): CommonProducerConfig = {
    CommonProducerConfig(
      format = "paimon",
      action = "expire-snapshots",
      catalogName = "paimon",
      databaseName = Some("db"),
      tableName = Some("t1"),
      tableNameRegex = None,
      retryTimes = retryTimes,
      continueOnTableFailure = true)
  }

  private val table = TargetTable("db.t1", "db", "t1")

  private object WrongConfig extends ActionConfig

  private val defaultConfig =
    PaimonExpireSnapshotsConfig(
      retainMax = 20,
      retainMin = 10,
      maxDeletes = 550L,
      procedureOptions = "file-operation.thread-num=32,snapshot.expire.execution-mode=sync")

  private object DummyAdapter extends LakeFormatAdapter {
    override def format: String = "paimon"

    override def actions: Seq[ProducerAction] = Seq.empty

    override def listTables(
        spark: SparkSession,
        config: CommonProducerConfig,
        database: String): Seq[TargetTable] = Seq.empty
  }

  private def success(row: Row): Either[RuntimeException, Row] = Right(row)

  private def failure(error: RuntimeException): Either[RuntimeException, Row] = Left(error)

  private class FakeRunner(results: Seq[Either[RuntimeException, Row]]) extends PaimonSqlRunner {

    var sqls: Seq[String] = Seq.empty
    var outcomes: Seq[String] = Seq.empty
    private var remainingResults = results

    override def run(spark: SparkSession, sqlText: String): Seq[Row] = {
      sqls :+= sqlText
      if (remainingResults.isEmpty) {
        throw new AssertionError("FakeRunner result queue exhausted")
      }
      val nextResult = remainingResults.head
      remainingResults = remainingResults.tail
      nextResult match {
        case Left(error) =>
          outcomes :+= "failure"
          throw error
        case Right(row) =>
          outcomes :+= "success"
          Seq(row)
      }
    }
  }
}
