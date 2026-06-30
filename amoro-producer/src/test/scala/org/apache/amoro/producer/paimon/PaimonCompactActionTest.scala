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
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core.{ActionStatus, CommonProducerConfig, ProducerCliParser, ProducerContext, RetryMode, TargetTable}
import org.apache.amoro.producer.spi.{ActionConfig, LakeFormatAdapter, ProducerAction}

class PaimonCompactActionTest {

  @Test
  def parseUsesScriptDefaults(): Unit = {
    val action = new PaimonCompactAction()
    val config = parse(action)

    assertEquals("compact", action.name)
    assertEquals(RetryMode.ActionManagedRetry, action.retryMode)
    assertEquals(0, config.startBucket)
    assertEquals(20, config.step)
    assertEquals("full", config.compactStrategy)
    assertEquals("target-file-size=256m", config.procedureOptions)
    assertEquals("", config.version)
    assertEquals("1d", config.partitionIdleTime)
  }

  @Test
  def validateRejectsInvalidActionArguments(): Unit = {
    val action = new PaimonCompactAction()
    val common = commonConfig()

    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonCompactConfig(-1, 20, "full", "", "", "1d")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonCompactConfig(0, 0, "full", "", "", "1d")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonCompactConfig(0, 20, " ", "", "", "1d")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonCompactConfig(0, 20, "full", "", "1.0", "1d")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonCompactConfig(0, 20, "full", "", "0.9", " ")))
  }

  @Test
  def validateRejectsWrongActionConfigType(): Unit = {
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () => new PaimonCompactAction().validate(commonConfig(), WrongConfig))

    assertTrue(exception.getMessage.contains("compact"))
    assertTrue(exception.getMessage.contains("PaimonCompactConfig"))
  }

  @Test
  def executeRejectsNonPositiveRetryTimesBeforeRunningSql(): Unit = {
    val runner = new FakeRunner()

    assertThrows(
      classOf[IllegalArgumentException],
      () => new PaimonCompactAction(runner).execute(context(retryTimes = 0), table, defaultConfig))
    assertTrue(runner.sqls.isEmpty)
  }

  @Test
  def parseTrimsActionArguments(): Unit = {
    val action = new PaimonCompactAction()
    val config =
      parse(
        action,
        Array(
          "--startBucket",
          " 1 ",
          "--step",
          " 2 ",
          "--compactStrategy",
          " full ",
          "--procedureOptions",
          " target-file-size=256m ",
          "--version",
          " 0.9 ",
          "--partitionIdleTime",
          " 1d "))

    assertEquals(1, config.startBucket)
    assertEquals(2, config.step)
    assertEquals("full", config.compactStrategy)
    assertEquals("target-file-size=256m", config.procedureOptions)
    assertEquals("0.9", config.version)
    assertEquals("1d", config.partitionIdleTime)
  }

  @Test
  def buildCompactSqlUsesTrimmedActionArguments(): Unit = {
    val action = new PaimonCompactAction()
    val config =
      parse(
        action,
        Array(
          "--startBucket",
          " 1 ",
          "--step",
          " 2 ",
          "--compactStrategy",
          " full ",
          "--procedureOptions",
          " target-file-size=256m ",
          "--version",
          " 0.9 ",
          "--partitionIdleTime",
          " 1d "))

    val bucketSql =
      PaimonCompactAction.buildCompactSql(
        catalogName = "paimon",
        config = config.copy(version = ""),
        table = table,
        bucketRange = "1-2")
    val paimon09Sql =
      PaimonCompactAction.buildCompactSql(
        catalogName = "paimon",
        config = config,
        table = table,
        bucketRange = "1-2")

    assertTrue(bucketSql.contains("compact_strategy => 'full'"))
    assertTrue(bucketSql.contains("options => 'target-file-size=256m'"))
    assertFalse(bucketSql.contains("compact_strategy => ' full '"))
    assertFalse(bucketSql.contains("options => ' target-file-size=256m '"))
    assertTrue(paimon09Sql.contains("partition_idle_time => '1d'"))
    assertFalse(paimon09Sql.contains("partition_idle_time => ' 1d '"))
  }

  @Test
  def buildBucketRangesUsesClosedIntervals(): Unit = {
    assertEquals(
      Seq("0-19", "20-39", "40-40"),
      PaimonCompactAction.buildBucketRanges(startBucket = 0, step = 20, bucketNum = 41))
  }

  @Test
  def buildCompactSqlIncludesCatalogBucketsStrategyAndOptions(): Unit = {
    val config =
      PaimonCompactConfig(
        startBucket = 0,
        step = 20,
        compactStrategy = "full's",
        procedureOptions = "target-file-size=256m,path='x'",
        version = "",
        partitionIdleTime = "1d")

    val sql =
      PaimonCompactAction.buildCompactSql(
        catalogName = "pa`imon",
        config = config,
        table = table,
        bucketRange = "0-19")

    assertTrue(sql.contains("CALL `pa``imon`.sys.compact("))
    assertTrue(sql.contains("table => 'db.t1'"))
    assertTrue(sql.contains("buckets => '0-19'"))
    assertTrue(sql.contains("compact_strategy => 'full''s'"))
    assertTrue(sql.contains("options => 'target-file-size=256m,path=''x'''"))
  }

  @Test
  def buildCompactSqlOmitsOptionsWhenProcedureOptionsIsEmpty(): Unit = {
    val sql =
      PaimonCompactAction.buildCompactSql(
        catalogName = "paimon",
        config = PaimonCompactConfig(0, 20, "full", "", "", "1d"),
        table = table,
        bucketRange = "0-19")

    assertTrue(sql.contains("compact_strategy => 'full'"))
    assertFalse(sql.contains("options =>"))
  }

  @Test
  def buildCompactSqlUsesPaimon09PartitionIdleTimeOnly(): Unit = {
    val sql =
      PaimonCompactAction.buildCompactSql(
        catalogName = "paimon",
        config = PaimonCompactConfig(0, 20, "full", "target-file-size=256m", "0.9", "1d"),
        table = table,
        bucketRange = "0-19")

    assertTrue(sql.contains("CALL sys.compact("))
    assertTrue(sql.contains("table => 'db.t1'"))
    assertTrue(sql.contains("partition_idle_time => '1d'"))
    assertFalse(sql.contains("buckets =>"))
    assertFalse(sql.contains("compact_strategy =>"))
    assertFalse(sql.contains("options =>"))
  }

  @Test
  def buildNonBucketCompactSqlUsesSimpleSysCompact(): Unit = {
    val sql = PaimonCompactAction.buildNonBucketCompactSql(table)

    assertTrue(sql.contains("CALL sys.compact("))
    assertTrue(sql.contains("table => 'db.t1'"))
    assertFalse(sql.contains("partition_idle_time =>"))
    assertFalse(sql.contains("buckets =>"))
    assertFalse(sql.contains("compact_strategy =>"))
    assertFalse(sql.contains("options =>"))
  }

  @Test
  def executeUsesNonBucketSimpleCompactWhenBucketReadFailsOrMissing(): Unit = {
    val failingBucketRunner = new FakeRunner(bucketFailure = true)
    val missingBucketRunner = new FakeRunner(bucketRows = Seq.empty)

    new PaimonCompactAction(failingBucketRunner).execute(context(), table, defaultConfig)
    new PaimonCompactAction(missingBucketRunner).execute(context(), table, defaultConfig)

    assertEquals(Seq("CALL sys.compact(\ntable => 'db.t1'\n)"), compactSqls(failingBucketRunner))
    assertEquals(Seq("CALL sys.compact(\ntable => 'db.t1'\n)"), compactSqls(missingBucketRunner))
  }

  @Test
  def executeUsesNonBucketSimpleCompactWhenBucketValueIsInvalid(): Unit = {
    val nonIntegerBucketRunner = new FakeRunner(bucketRows = Seq(Row("abc")))
    val nonPositiveBucketRunner = new FakeRunner(bucketRows = Seq(Row("0")))

    new PaimonCompactAction(nonIntegerBucketRunner).execute(context(), table, defaultConfig)
    new PaimonCompactAction(nonPositiveBucketRunner).execute(context(), table, defaultConfig)

    assertEquals(Seq("CALL sys.compact(\ntable => 'db.t1'\n)"), compactSqls(nonIntegerBucketRunner))
    assertEquals(
      Seq("CALL sys.compact(\ntable => 'db.t1'\n)"),
      compactSqls(nonPositiveBucketRunner))
  }

  @Test
  def executeSkipsWhenStartBucketIsGreaterThanMaxBucketId(): Unit = {
    val runner = new FakeRunner(bucketRows = Seq(Row("3")))
    val result =
      new PaimonCompactAction(runner)
        .execute(context(), table, defaultConfig.copy(startBucket = 3))

    assertEquals(ActionStatus.Skipped, result.status)
    assertTrue(compactSqls(runner).isEmpty)
  }

  @Test
  def executeRetriesFailedRangeUntilSuccess(): Unit = {
    val runner = new FakeRunner(bucketRows = Seq(Row("41")), failAttemptsByRange = Map("0-19" -> 1))
    val result =
      new PaimonCompactAction(runner).execute(context(retryTimes = 2), table, defaultConfig)

    assertEquals(ActionStatus.Success, result.status)
    assertEquals(2, compactSqls(runner).count(_.contains("buckets => '0-19'")))
    assertEquals(ActionStatus.Success, result.tasks.find(_.name == "0-19").get.status)
  }

  @Test
  def executeContinuesAfterRangeRetryExhaustedAndReturnsFailedResult(): Unit = {
    val runner = new FakeRunner(bucketRows = Seq(Row("41")), failAttemptsByRange = Map("0-19" -> 2))
    val result =
      new PaimonCompactAction(runner).execute(context(retryTimes = 2), table, defaultConfig)

    assertEquals(ActionStatus.Failed, result.status)
    assertEquals(ActionStatus.Failed, result.tasks.find(_.name == "0-19").get.status)
    assertTrue(compactSqls(runner).exists(_.contains("buckets => '20-39'")))
    assertTrue(compactSqls(runner).exists(_.contains("buckets => '40-40'")))
  }

  @Test
  def executePaimon09BucketTableRunsSinglePartitionIdleTimeTask(): Unit = {
    val runner = new FakeRunner(bucketRows = Seq(Row("41")))
    val result =
      new PaimonCompactAction(runner)
        .execute(context(), table, defaultConfig.copy(version = "0.9", partitionIdleTime = "1d"))

    assertEquals(ActionStatus.Success, result.status)
    assertEquals(Seq("partition_idle_time=1d"), result.tasks.map(_.name))
    assertEquals(1, compactSqls(runner).size)
    assertTrue(compactSqls(runner).head.contains("partition_idle_time => '1d'"))
    assertFalse(compactSqls(runner).head.contains("buckets =>"))
    assertFalse(compactSqls(runner).head.contains("compact_strategy =>"))
    assertFalse(compactSqls(runner).head.contains("options =>"))
  }

  private def parse(
      action: PaimonCompactAction,
      args: Array[String] = Array.empty): PaimonCompactConfig = {
    val commandLine = ProducerCliParser.parseMerged(action.options, args)
    action.parse(commandLine).asInstanceOf[PaimonCompactConfig]
  }

  private def compactSqls(runner: FakeRunner): Seq[String] = {
    runner.sqls.filterNot(_.contains("$options")).map(normalizeSql)
  }

  private def normalizeSql(sqlText: String): String = {
    sqlText.stripMargin.trim.linesIterator.map(_.trim).mkString("\n")
  }

  private def context(retryTimes: Int = 3): ProducerContext = {
    ProducerContext(null.asInstanceOf[SparkSession], commonConfig(retryTimes), DummyAdapter)
  }

  private def commonConfig(retryTimes: Int = 3): CommonProducerConfig = {
    CommonProducerConfig(
      format = "paimon",
      action = "compact",
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
    PaimonCompactConfig(
      startBucket = 0,
      step = 20,
      compactStrategy = "full",
      procedureOptions = "target-file-size=256m",
      version = "",
      partitionIdleTime = "1d")

  private object DummyAdapter extends LakeFormatAdapter {
    override def format: String = "paimon"

    override def actions: Seq[ProducerAction] = Seq.empty

    override def listTables(
        spark: SparkSession,
        config: CommonProducerConfig,
        database: String): Seq[TargetTable] = Seq.empty
  }

  private class FakeRunner(
      bucketRows: Seq[Row] = Seq(Row("41")),
      bucketFailure: Boolean = false,
      failAttemptsByRange: Map[String, Int] = Map.empty)
    extends PaimonSqlRunner {

    var sqls: Seq[String] = Seq.empty
    private var attemptsByRange: Map[String, Int] = Map.empty

    override def run(spark: SparkSession, sqlText: String): Seq[Row] = {
      sqls :+= sqlText
      if (sqlText.contains("$options")) {
        if (bucketFailure) {
          throw new RuntimeException("bucket read failed")
        }
        bucketRows
      } else {
        val range = bucketRange(sqlText)
        range.foreach { value =>
          val nextAttempt = attemptsByRange.getOrElse(value, 0) + 1
          attemptsByRange += value -> nextAttempt
          if (nextAttempt <= failAttemptsByRange.getOrElse(value, 0)) {
            throw new RuntimeException(s"failed range $value")
          }
        }
        Seq.empty
      }
    }

    private def bucketRange(sqlText: String): Option[String] = {
      "'([0-9]+-[0-9]+)'".r.findFirstMatchIn(sqlText).map(_.group(1))
    }
  }
}
