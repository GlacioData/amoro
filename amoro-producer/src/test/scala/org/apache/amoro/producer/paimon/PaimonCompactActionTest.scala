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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core._
import org.apache.amoro.producer.spi.ActionConfig

class PaimonCompactActionTest {

  private val table = TargetTable("db.t1", "db", "t1")
  private val managed = Map("self-optimizing.enabled" -> "true", "write-only" -> "true")

  @Test
  def warnsOnceWithCatalogDatabaseAndTableWhenSkipped(): Unit = {
    val events = ArrayBuffer.empty[org.apache.logging.log4j.core.LogEvent]
    val logger = org.apache.logging.log4j.LogManager.getLogger(classOf[PaimonCompactAction])
      .asInstanceOf[org.apache.logging.log4j.core.Logger]
    val originalLevel = logger.getLevel
    val appender = new org.apache.logging.log4j.core.appender.AbstractAppender(
      "compact-skip-test",
      null,
      org.apache.logging.log4j.core.layout.PatternLayout.createDefaultLayout(),
      false,
      org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
      override def append(event: org.apache.logging.log4j.core.LogEvent): Unit =
        events += event.toImmutable
    }
    appender.start()
    logger.addAppender(appender)
    logger.setLevel(org.apache.logging.log4j.Level.WARN)
    try {
      Seq((Map.empty[String, String], "1.3"), (managed, "0.9")).foreach {
        case (properties, version) =>
          events.clear()
          val runner = new FakeRunner(properties)
          assertEquals(ActionStatus.Skipped, execute(runner, "--version", version).status)
          assertEquals(1, events.size)
          assertEquals(org.apache.logging.log4j.Level.WARN, events.head.getLevel)
          val message = events.head.getMessage.getFormattedMessage
          Seq("catalog=paimon", "database=db", "table=t1", "reason=").foreach(part =>
            assertTrue(message.contains(part)))
          assertTrue(runner.compacts.isEmpty)
      }
    } finally {
      logger.removeAppender(appender)
      logger.setLevel(originalLevel)
      appender.stop()
    }
  }

  @Test
  def rejectsInvalidRetryLimitAndWrongConfigBeforeReadingMetadata(): Unit = {
    val runner = new FakeRunner(managed)
    val action = new PaimonCompactAction(runner)
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        action.execute(
          ProducerContext(null, common.copy(retryTimes = 0), PaimonFormatAdapter),
          table,
          parsed(Array.empty)))
    assertTrue(runner.compacts.isEmpty)
    assertEquals(0, runner.reads)
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, new ActionConfig {}))
  }

  @Test
  def validatesOnlyTheSelectedPathAndKeepsPredicateUntrimmed(): Unit = {
    val modern = new FakeRunner(managed)
    assertEquals(
      ActionStatus.Success,
      execute(modern, "--version", "1.3", "--partitionIdleTime", "", "--where", " dt = 1 ").status)
    assertTrue(modern.compacts.head.contains("where => ' dt = 1 '"))
    val legacy = new FakeRunner(Map.empty)
    assertEquals(ActionStatus.Success, execute(legacy, "--compactStrategy", "bad").status)
    assertEquals(
      ActionStatus.Failed,
      execute(new FakeRunner(Map.empty), "--partitionIdleTime", " ").status)
  }

  @Test
  def routesEverySwitchCombinationOnlyWhenVersionMatches(): Unit = {
    for {
      self <- Seq(None, Some("false"), Some("true"), Some(" TRUE "))
      write <- Seq(None, Some("false"), Some("true"), Some(" True "))
      version <- Seq("0.9", "1.3")
    } {
      val props = self.map("self-optimizing.enabled" -> _).toMap ++ write.map("write-only" -> _)
      val runner = new FakeRunner(props)
      val result = execute(runner, "--version", version)
      val isManaged = self.exists(_.trim.equalsIgnoreCase("true")) &&
        write.exists(_.trim.equalsIgnoreCase("true"))
      if (isManaged == (version == "1.3")) {
        assertEquals(ActionStatus.Success, result.status)
        assertEquals(1, runner.compacts.size)
        assertTrue(runner.compacts.head.startsWith("CALL `paimon`.sys.compact("))
        assertEquals(isManaged, runner.compacts.head.contains("compact_strategy"))
        assertEquals(!isManaged, runner.compacts.head.contains("partition_idle_time"))
        assertEquals(if (isManaged) "1.3" else "0.9", result.metrics("effectiveVersion"))
        assertFalse(runner.compacts.head.contains("buckets"))
      } else {
        assertEquals(ActionStatus.Skipped, result.status)
        assertTrue(runner.compacts.isEmpty)
        assertTrue(result.message.isDefined)
      }
      assertEquals(1, runner.reads)
    }
  }

  @Test
  def managedTableRequiresExplicitModernVersion(): Unit = {
    val runner = new FakeRunner(managed)
    assertEquals(ActionStatus.Skipped, execute(runner).status)
    assertTrue(runner.compacts.isEmpty)
    assertEquals(ActionStatus.Success, execute(runner, "--version", "1.3").status)
    assertTrue(runner.compacts.head.contains("compact_strategy => 'full'"))
    assertTrue(runner.compacts.head.contains("target-file-size=256m"))
  }

  @Test
  def legacyIdleTimeDefaultsToOneHourAndExplicitValuesWin(): Unit = {
    Seq(
      Seq.empty[String] -> "1h",
      Seq("--partitionIdleTime", "0s") -> "0s",
      Seq("--partitionIdleTime", "2h") -> "2h").foreach { case (args, expected) =>
      val runner = new FakeRunner(Map.empty)
      assertEquals(ActionStatus.Success, execute(runner, args: _*).status)
      assertTrue(runner.compacts.head.contains(s"partition_idle_time => '$expected'"))
    }
  }

  @Test
  def loadsRequiredMetadataEvenWithoutWhereAndDoesNotFallbackOnFailure(): Unit = {
    var attempts = 0
    val runner = new FakeRunner(managed) {
      override def loadTableMetadata(
          spark: SparkSession,
          catalog: String,
          table: TargetTable): PaimonTableMetadata = {
        attempts += 1
        throw new IllegalStateException("metadata unavailable")
      }
    }
    val result = execute(runner, "--version", "1.3")
    assertEquals(ActionStatus.Failed, result.status)
    assertEquals(3, attempts)
    assertTrue(runner.compacts.isEmpty)
  }

  @Test
  def rejectsRemovedBucketArgumentsAndEmptyWhereBeforeExecution(): Unit = {
    Seq(
      Array("--startBucket", "0"),
      Array("--step", "20"),
      Array("--where", ""),
      Array("--where", "   "),
      Array("--version", "1.4")).foreach { args =>
      assertThrows(classOf[IllegalArgumentException], () => parsed(args))
    }
  }

  @Test
  def whereIsRetainedOnBothExecutionPaths(): Unit = {
    Seq(managed, Map.empty[String, String]).foreach { props =>
      val runner = new FakeRunner(props)
      execute(
        runner,
        "--version",
        if (props == managed) "1.3" else "0.9",
        "--where",
        "dt > 10 AND hour < 20")
      assertTrue(runner.compacts.head.contains("where => 'dt > 10 AND hour < 20'"))
    }
  }

  @Test
  def rejectsUnicodeWhitespaceWhereBeforeExecution(): Unit = {
    Seq("\u3000", "\u2003", " \n\t\u3000").foreach { condition =>
      assertThrows(
        classOf[IllegalArgumentException],
        () => parsed(Array("--where", condition)))
    }
  }

  @Test
  def rejectsUnicodeWhitespaceIdleTimeOnLegacyPath(): Unit = {
    val runner = new FakeRunner(Map.empty)
    assertEquals(ActionStatus.Failed, execute(runner, "--partitionIdleTime", "\u3000").status)
    assertTrue(runner.compacts.isEmpty)
  }

  @Test
  def metadataFailureNeverFallsBackToCompaction(): Unit = {
    val runner = new FakeRunner(managed, readFailures = 5)
    val result = execute(runner)
    assertEquals(ActionStatus.Failed, result.status)
    assertEquals(3, runner.reads)
    assertTrue(runner.compacts.isEmpty)
    assertTrue(result.message.get.contains("metadata unavailable"))
    assertEquals("3", result.tasks.head.metrics("attempts"))
  }

  @Test
  def effectiveOptionsFailuresAreRetriedWithoutCompactingOrReReadingRawOptions(): Unit = {
    var attempts = 0
    val runner = new FakeRunner(managed) {
      override def loadTableMetadata(
          spark: SparkSession,
          catalogName: String,
          table: TargetTable): PaimonTableMetadata = {
        attempts += 1
        throw new IllegalStateException("effective options unavailable")
      }
    }
    val result = execute(runner, "--version", "1.3", "--where", "dt = 1")
    assertEquals(ActionStatus.Failed, result.status)
    assertEquals(3, attempts)
    assertEquals(1, runner.reads)
    assertEquals("3", result.metrics("effectiveOptionsAttempts"))
    assertTrue(runner.compacts.isEmpty)
  }

  @Test
  def rejectsVariableReferencesBeforeReadingIdentifiersOrCallingLegacyProcedure(): Unit = {
    Seq(
      (common.copy(catalogName = "${catalog}"), table),
      (common, TargetTable("${database}.t1", "${database}", "t1")),
      (common, TargetTable("db.${table}", "db", "${table}"))).foreach {
      case (config, target) =>
        val runner = new FakeRunner(managed)
        val result = new PaimonCompactAction(runner).execute(
          ProducerContext(null, config, PaimonFormatAdapter),
          target,
          parsed(Array.empty))
        assertEquals(ActionStatus.Failed, result.status)
        assertTrue(result.message.get.contains("variable.substitute"))
        assertEquals(0, runner.reads)
        assertTrue(runner.compacts.isEmpty)
    }
    val legacy = new FakeRunner(Map.empty)
    val result = execute(legacy, "--partitionIdleTime", "${idle}")
    assertEquals(ActionStatus.Failed, result.status)
    assertTrue(result.message.get.contains("variable.substitute"))
    assertTrue(legacy.compacts.isEmpty)
  }

  @Test
  def metadataRetryThenCompactionRetryDoNotReplaySuccessfulReads(): Unit = {
    val runner = new FakeRunner(managed, readFailures = 1, compactFailures = 1)
    val result = execute(runner, "--version", "1.3", "--where", "dt = 1")
    assertEquals(ActionStatus.Success, result.status)
    assertEquals(2, runner.reads)
    assertEquals(2, runner.compacts.size)
    assertEquals(1, runner.compacts.distinct.size)
    assertEquals("2", result.tasks.last.metrics("attempts"))
  }

  @Test
  def exhaustedCompactFailureRetainsConditionAndCause(): Unit = {
    val runner = new FakeRunner(managed, compactFailures = 5)
    val result = execute(runner, "--version", "1.3", "--where", "dt = 1")
    assertEquals(ActionStatus.Failed, result.status)
    assertEquals(3, runner.compacts.size)
    assertTrue(runner.compacts.forall(_.contains("where => 'dt = 1'")))
    assertTrue(result.message.get.contains("compact unavailable"))
  }

  @Test
  def validatesActualModernPathAndProtectsWriteOnly(): Unit = {
    Seq(
      Array("--compactStrategy", "bad"),
      Array("--compactStrategy", " "),
      Array("--procedureOptions", "write-only=true"),
      Array("--procedureOptions", "write-only=false,write-only=TRUE"),
      Array("--procedureOptions", "malformed")).foreach { args =>
      val runner = new FakeRunner(managed)
      val result = execute(runner, (Seq("--version", "1.3") ++ args): _*)
      assertEquals(ActionStatus.Failed, result.status)
      assertTrue(runner.compacts.isEmpty)
    }
  }

  @Test
  def allowsMinorAndEmptyDynamicOptions(): Unit = {
    val runner = new FakeRunner(managed)
    val result =
      execute(runner, "--version", "1.3", "--compactStrategy", " minor ", "--procedureOptions", "")
    assertEquals(ActionStatus.Success, result.status)
    assertTrue(runner.compacts.head.contains("compact_strategy => 'minor'"))
    assertFalse(runner.compacts.head.contains("options =>"))
  }

  @Test
  def rejectsIncrementalClusteringWhereFromTableOrDynamicOptions(): Unit = {
    val runner =
      new FakeRunner(managed ++ Map("bucket" -> "-1", "clustering.incremental" -> "true"))
    assertEquals(
      ActionStatus.Failed,
      execute(runner, "--version", "1.3", "--where", "dt = 1").status)
    assertTrue(runner.compacts.isEmpty)
    val dynamic = new FakeRunner(managed ++ Map("bucket" -> "-1"))
    assertEquals(
      ActionStatus.Failed,
      execute(
        dynamic,
        "--version",
        "1.3",
        "--where",
        "dt = 1",
        "--procedureOptions",
        "clustering.incremental=true").status)
    assertTrue(dynamic.compacts.isEmpty)
  }

  @Test
  def incrementalRangeGuardUsesTheFinalPredicate(): Unit = {
    Seq(Seq.empty[String], Seq("preset_pt")).foreach { partitions =>
      val props = managed ++ Map("bucket" -> "-1", "clustering.incremental" -> "true")
      val runner = new FakeRunner(props) {
        override def loadTableMetadata(
            spark: SparkSession,
            catalog: String,
            table: TargetTable): PaimonTableMetadata = PaimonTableMetadata(
          Seq.empty,
          partitions,
          new org.apache.spark.sql.types.StructType(),
          props)
      }
      val result = execute(runner, "--version", "1.3", "--where", "preset_dt = '2026-10-08'")
      assertEquals(
        if (partitions.isEmpty) ActionStatus.Success else ActionStatus.Failed,
        result.status)
      assertEquals(if (partitions.isEmpty) 1 else 0, runner.compacts.size)
    }
  }

  @Test
  def dynamicOptionsAreCheckedAfterApplyingOverrides(): Unit = {
    val runner =
      new FakeRunner(managed ++ Map("bucket" -> "-1", "clustering.incremental" -> "true"))
    assertEquals(
      ActionStatus.Success,
      execute(
        runner,
        "--version",
        "1.3",
        "--where",
        "dt = 1",
        "--procedureOptions",
        "clustering.incremental=false").status)
  }

  @Test
  def rejectsFalseAndMalformedProcedureResults(): Unit = {
    Seq(
      Seq(Row(false)),
      Seq.empty,
      Seq(Row(null)),
      Seq(Row("true")),
      Seq(Row(true), Row(true)),
      Seq(Row(true, "extra"))).foreach { rows =>
      val runner = new FakeRunner(managed, resultRows = rows)
      assertEquals(ActionStatus.Failed, execute(runner, "--version", "1.3").status)
    }
    assertEquals(ActionStatus.Success, execute(new FakeRunner(managed), "--version", "1.3").status)
  }

  @Test
  def invalidMetadataRowsAreFailuresInsteadOfLegacyTables(): Unit = {
    val runner = new FakeRunner(managed) {
      override def metadata: Seq[Row] = Seq(Row("write-only"))
    }
    assertEquals(ActionStatus.Failed, execute(runner, "--version", "1.3").status)
    assertTrue(runner.compacts.isEmpty)
  }

  private def common =
    CommonProducerConfig("paimon", "compact", "paimon", None, Some("db.t1"), None, 3, true)

  private def parsed(args: Array[String]): ActionConfig = {
    val action = new PaimonCompactAction()
    val config = action.parse(ProducerCliParser.parseMerged(action.options, args))
    action.validate(common, config)
    config
  }

  private def execute(runner: FakeRunner, args: String*): ActionResult = {
    new PaimonCompactAction(runner).execute(
      ProducerContext(null.asInstanceOf[SparkSession], common, PaimonFormatAdapter),
      table,
      parsed(args.toArray))
  }

  private class FakeRunner(
      properties: Map[String, String],
      readFailures: Int = 0,
      compactFailures: Int = 0,
      resultRows: Seq[Row] = Seq(Row(true))) extends PaimonSqlRunner {
    var reads = 0
    val compacts = ArrayBuffer.empty[String]
    override def loadTableMetadata(
        spark: SparkSession,
        catalogName: String,
        table: TargetTable): PaimonTableMetadata = PaimonTableMetadata(
      Seq.empty,
      Seq("dt", "hour"),
      new org.apache.spark.sql.types.StructType(),
      properties)
    def metadata: Seq[Row] = properties.toSeq.map { case (key, value) => Row(key, value) }
    override def run(spark: SparkSession, sqlText: String): Seq[Row] = {
      if (sqlText.trim.startsWith("SELECT")) {
        reads += 1
        if (reads <= readFailures) throw new RuntimeException("metadata unavailable")
        metadata
      } else {
        compacts += sqlText.trim
        if (compacts.size <= compactFailures) throw new RuntimeException("compact unavailable")
        resultRows
      }
    }
  }
}
