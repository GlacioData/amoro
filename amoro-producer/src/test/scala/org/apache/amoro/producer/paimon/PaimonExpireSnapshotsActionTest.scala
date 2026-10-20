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
  def executorOptionsAreOptInAndValidated(): Unit = {
    val action = new PaimonExpireSnapshotsAction()
    assertEquals("driver", parse(action).executionMode)
    val executor = parse(action, Array("--executionMode", " executor ", "--tableConcurrency", "2"))
    assertEquals("executor", executor.executionMode)
    assertEquals(2, executor.tableConcurrency)
    action.validate(commonConfig(), executor)
    Seq(
      Array("--executionMode", "unknown"),
      Array("--tableConcurrency", "2"),
      Array("--executionMode", "executor", "--tableConcurrency", "0")).foreach { args =>
      assertThrows(
        classOf[IllegalArgumentException],
        () => action.validate(commonConfig(), parse(action, args)))
    }
  }

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

  @Test
  def tableRetentionOverridesArgumentsAndProcedureOptionsInEveryRound(): Unit = {
    val runner = new FakeRunner(Seq(success(Row(2L)), success(Row(1L))))
    runner.properties = Map(
      "snapshot.num-retained.min" -> "50",
      "snapshot.num-retained.max" -> "300",
      "snapshot.time-retained" -> "24 h",
      "snapshot.expire.limit" -> "2")
    val config = defaultConfig.copy(procedureOptions = "snapshot.time-retained=1 ms")
    val result = new PaimonExpireSnapshotsAction(runner).execute(context(), table, config)

    assertEquals(1, runner.optionReads)
    assertEquals(2, runner.sqls.size)
    runner.sqls.foreach { sql =>
      assertTrue(sql.contains("retain_min => 50"), sql)
      assertTrue(sql.contains("retain_max => 300"), sql)
      assertTrue(sql.contains("max_deletes => 2"), sql)
      assertTrue(sql.contains("snapshot.time-retained=86400000 ms"), sql)
    }
    assertEquals("2", result.metrics("maxDeletes"))
    assertEquals("50", result.metrics("retainMin"))
    assertEquals("300", result.metrics("retainMax"))
    assertEquals("86400000", result.metrics("timeRetainedMs"))
  }

  @Test
  def missingTableOptionsFallBackIndividuallyToArguments(): Unit = {
    val runner = new FakeRunner(Seq(success(Row(0L))))
    runner.properties = Map("snapshot.num-retained.min" -> "15")
    val config = PaimonExpireSnapshotsConfig(80, 2, 9L, "snapshot.time-retained=2 h")
    val result = new PaimonExpireSnapshotsAction(runner).execute(context(), table, config)

    assertTrue(runner.sqls.head.contains("retain_min => 15"))
    assertTrue(runner.sqls.head.contains("retain_max => 80"))
    assertTrue(runner.sqls.head.contains("max_deletes => 9"))
    assertEquals("7200000", result.metrics("timeRetainedMs"))
  }

  @Test
  def invalidTableRetentionNeverFallsBackOrCallsExpire(): Unit = {
    Seq(
      Map("snapshot.num-retained.min" -> "0"),
      Map("snapshot.num-retained.max" -> "1"),
      Map("snapshot.num-retained.min" -> "invalid"),
      Map("snapshot.time-retained" -> "invalid"),
      Map("snapshot.time-retained" -> "-1 s"),
      Map("snapshot.expire.limit" -> "0"))
      .foreach { properties =>
        val runner = new FakeRunner(Seq.empty)
        runner.properties = properties
        assertThrows(
          classOf[IllegalArgumentException],
          () => new PaimonExpireSnapshotsAction(runner).execute(context(), table, defaultConfig))
        assertTrue(runner.sqls.isEmpty, properties.toString)
      }
  }

  @Test
  def optionReadRetriesWithoutReplayingSuccessfulExpiration(): Unit = {
    val runner = new FakeRunner(Seq(success(Row(0L))))
    runner.optionReadFailures = 1
    new PaimonExpireSnapshotsAction(runner).execute(context(), table, defaultConfig)
    assertEquals(2, runner.optionReads)
    assertEquals(1, runner.sqls.size)
  }

  @Test
  def exhaustedOptionReadDoesNotExpireWithDefaults(): Unit = {
    val runner = new FakeRunner(Seq.empty)
    runner.optionReadFailures = 3
    assertThrows(
      classOf[RuntimeException],
      () => new PaimonExpireSnapshotsAction(runner).execute(context(), table, defaultConfig))
    assertEquals(3, runner.optionReads)
    assertTrue(runner.sqls.isEmpty)
  }

  @Test
  def invalidOptionsRowsDoNotPermitExpiration(): Unit = {
    Seq(Seq(Row("key")), Seq(Row(null, "1")), Seq(Row("key", "1"), Row("key", "2")))
      .foreach { rows =>
        val runner = new FakeRunner(Seq.empty) {
          override def optionRows: Seq[Row] = rows
        }
        assertThrows(
          classOf[RuntimeException],
          () => new PaimonExpireSnapshotsAction(runner).execute(context(), table, defaultConfig))
        assertTrue(runner.sqls.isEmpty)
      }
  }

  @Test
  def legacyTimeRetentionIsUnlimitedOnlyWhenNoTimeWasConfigured(): Unit = {
    val absent = new FakeRunner(Seq(success(Row(0L))))
    absent.supportsOptions = false
    val result = new PaimonExpireSnapshotsAction(absent).execute(context(), table, defaultConfig)
    assertEquals(Long.MaxValue.toString, result.metrics("timeRetainedMs"))
    assertFalse(absent.sqls.head.contains("older_than"))
    assertFalse(absent.sqls.head.contains("options =>"))

    val configured = new FakeRunner(Seq(success(Row(0L))))
    configured.supportsOptions = false
    configured.properties = Map("snapshot.time-retained" -> "24 h")
    new PaimonExpireSnapshotsAction(configured).execute(context(), table, defaultConfig)
    assertTrue(configured.sqls.head.contains("older_than => TIMESTAMP"))
    assertFalse(configured.sqls.head.contains("options =>"))
  }

  @Test
  def unsupportedLegacyOptionsAndIntegerOverflowNeverCallExpire(): Unit = {
    val legacy = new FakeRunner(Seq.empty)
    legacy.supportsOptions = false
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        new PaimonExpireSnapshotsAction(legacy)
          .execute(
            context(),
            table,
            defaultConfig.copy(procedureOptions = "file-operation.thread-num=16")))
    assertTrue(legacy.sqls.isEmpty)
    val oversized = new FakeRunner(Seq.empty)
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        new PaimonExpireSnapshotsAction(oversized)
          .execute(context(), table, defaultConfig.copy(maxDeletes = Int.MaxValue.toLong + 1)))
    assertTrue(oversized.sqls.isEmpty)
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

    var supportsOptions = true
    override def expireSnapshotsSupportsOptions(spark: SparkSession, catalogName: String): Boolean =
      supportsOptions

    var sqls: Seq[String] = Seq.empty
    var outcomes: Seq[String] = Seq.empty
    var properties = Map.empty[String, String]
    var optionReads = 0
    var optionReadFailures = 0
    private var remainingResults = results

    def optionRows: Seq[Row] = properties.toSeq.map { case (key, value) => Row(key, value) }

    override def run(spark: SparkSession, sqlText: String): Seq[Row] = {
      if (sqlText.startsWith("SELECT")) {
        optionReads += 1
        if (optionReads <= optionReadFailures) throw new RuntimeException("options unavailable")
        return optionRows
      }
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
