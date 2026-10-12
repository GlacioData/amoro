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

import java.time.Instant

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core._

class PaimonCompactionCompletionTest {
  private val managed = Map("self-optimizing.enabled" -> "true", "write-only" -> "true")
  private val common =
    CommonProducerConfig("paimon", "compact", "paimon", Some("db"), None, None, 3, true)
  private val context = ProducerContext(null, common, PaimonFormatAdapter)
  private val table = TargetTable("db.t1", "db", "t1")

  @Test
  def notifiesOncePerTableAfterAllRetriesAndKeepsExactNonSensitiveSql(): Unit = {
    val events = ArrayBuffer.empty[PaimonCompactionCompletion]
    val runner = new FakeRunner(failures = 1)
    val action = new PaimonCompactAction(runner, event => { events += event; () })
    val before = Instant.now()
    val result = execute(action, table, "--where", "dt = '2026-10-01'")
    val after = Instant.now()
    assertEquals(ActionStatus.Success, result.status)
    assertEquals(1, events.size)
    val event = events.head
    assertTrue(event.success)
    assertEquals(2, event.attempts)
    assertEquals("1.3", event.effectiveVersion)
    assertEquals("paimon", event.catalog)
    assertEquals("db", event.database)
    assertEquals("t1", event.table)
    assertEquals(runner.compacts.head, event.sql)
    assertEquals(1, runner.compacts.distinct.size)
    assertFalse(event.startTime.isBefore(before))
    assertFalse(event.startTime.isBefore(runner.metadataFinishedAt))
    assertFalse(event.endTime.isBefore(event.startTime))
    assertFalse(event.endTime.isAfter(after))
    assertTrue(event.durationMs >= 0)
    assertEquals(None, event.error)
    execute(action, TargetTable("db.t2", "db", "t2"))
    assertEquals(Seq("t1", "t2"), events.map(_.table).toSeq)
  }

  @Test
  def notifiesFinalFailureOnceForExceptionsFalseAndMalformedResults(): Unit = {
    Seq(
      new FakeRunner(failures = 3),
      new FakeRunner(rows = Seq(Row(false))),
      new FakeRunner(rows = Seq(Row("true")))).foreach { runner =>
      val events = ArrayBuffer.empty[PaimonCompactionCompletion]
      val action = new PaimonCompactAction(runner, event => { events += event; () })
      assertEquals(ActionStatus.Failed, execute(action, table).status)
      assertEquals(1, events.size)
      assertFalse(events.head.success)
      assertEquals(3, events.head.attempts)
      assertEquals(3, runner.compacts.size)
      assertTrue(events.head.error.isDefined)
    }
  }

  @Test
  def callbackFailureNeverRetriesOrChangesCompactionOutcome(): Unit = {
    Seq(true, false).foreach { procedureResult =>
      var notifications = 0
      val runner = new FakeRunner(rows = Seq(Row(procedureResult)))
      val action = new PaimonCompactAction(
        runner,
        _ => {
          notifications += 1
          throw new IllegalStateException("callback failed")
        })
      val result = execute(action, table)
      assertEquals(
        if (procedureResult) ActionStatus.Success else ActionStatus.Failed,
        result.status)
      assertEquals(if (procedureResult) 1 else 3, runner.compacts.size)
      assertEquals(1, notifications)
      if (!procedureResult) assertTrue(result.message.get.contains("returned false"))
    }
  }

  @Test
  def doesNotReportCompactionForSkippedOrPreExecutionFailedTables(): Unit = {
    val events = ArrayBuffer.empty[PaimonCompactionCompletion]
    val skipped = new PaimonCompactAction(
      new FakeRunner(properties = Map.empty),
      event => { events += event; () })
    assertEquals(ActionStatus.Skipped, execute(skipped, table, "--version", "1.3").status)
    val managedSkipped = new PaimonCompactAction(
      new FakeRunner(),
      event => { events += event; () })
    assertEquals(ActionStatus.Skipped, execute(managedSkipped, table, "--version", "0.9").status)
    val invalid = new PaimonCompactAction(new FakeRunner(), event => { events += event; () })
    assertEquals(ActionStatus.Failed, execute(invalid, table, "--compactStrategy", "bad").status)
    val unreadable = new PaimonCompactAction(
      new FakeRunner(metadataFails = true),
      event => { events += event; () })
    assertEquals(ActionStatus.Failed, execute(unreadable, table).status)
    assertTrue(events.isEmpty)
  }

  @Test
  def completionAndRetriesUseTheAdaptedScope(): Unit = {
    Seq(
      (Seq("id"), Seq("preset_dt"), "removed-primary-key"),
      (Seq.empty[String], Seq("preset_hr"), "removed-partition-mismatch"),
      (Seq.empty[String], Seq("preset_pt"), "replaced-preset-dt"))
      .foreach { case (keys, partitions, decision) =>
        val runner = new FakeRunner(failures = 1, keys = keys, partitions = partitions)
        val events = ArrayBuffer.empty[PaimonCompactionCompletion]
        val action = new PaimonCompactAction(runner, event => { events += event; () })
        val result = execute(action, table, "--where", "preset_dt = '2026-10-08'")
        assertEquals(ActionStatus.Success, result.status)
        assertEquals(1, events.size)
        assertEquals(2, events.head.attempts)
        assertEquals(decision, events.head.scope.decision)
        assertEquals(decision, result.metrics("whereDecision"))
        assertEquals(keys, events.head.scope.primaryKeys)
        assertEquals(partitions, events.head.scope.partitionKeys)
        assertEquals(runner.compacts.head, events.head.sql)
        assertEquals(1, runner.compacts.distinct.size)
        assertEquals(decision == "replaced-preset-dt", events.head.sql.contains("where =>"))
      }
  }

  @Test
  def removingWhereWarnsOnceWithEscapedScopeAndDoesNotLeakSensitiveValues(): Unit = {
    captureLogs(
      logs => {
        val runner = new FakeRunner(failures = 1, keys = Seq("id"), partitions = Seq("dt"))
        val result = execute(
          new PaimonCompactAction(runner),
          table,
          "--where",
          "dt = 'fake-secret'",
          "--procedureOptions",
          "password=fake-secret")
        assertEquals(ActionStatus.Success, result.status)
        val warnings = logs.filter(_.getLevel == org.apache.logging.log4j.Level.WARN)
        assertEquals(1, warnings.size)
        val message = warnings.head.getMessage.getFormattedMessage
        assertTrue(message.startsWith("Compaction scope adjusted;"))
        assertFalse(message.contains("fake-secret"))
        val json = new ObjectMapper().readTree(message.substring(message.indexOf('{')))
        assertTrue(json.get("effectiveWhere").isNull)
        assertEquals("id", json.get("primaryKeys").get(0).asText())
        assertEquals("removed-primary-key", json.get("whereDecision").asText())
        assertFalse(result.metrics("requestedWhere").contains("fake-secret"))
      },
      classOf[PaimonCompactAction])
  }

  @Test
  def defaultStrategyUsesMinorOnlyForModernUnpartitionedPrimaryKeys(): Unit = {
    for {
      version <- Seq("0.9", "1.3")
      primary <- Seq(true, false)
      partitioned <- Seq(false, true)
      explicit <- Seq(None, Some("full"), Some("minor"))
    } {
      val events = ArrayBuffer.empty[PaimonCompactionCompletion]
      val runner = new FakeRunner(
        properties = if (version == "1.3") managed else Map.empty,
        failures = 1,
        keys = if (primary) Seq("id") else Seq.empty,
        partitions = if (partitioned) Seq("dt") else Seq.empty)
      val action = new PaimonCompactAction(runner, event => { events += event; () })
      val args =
        Seq("--version", version) ++ explicit.toSeq.flatMap(v => Seq("--compactStrategy", v))
      val result = execute(action, table, args: _*)
      assertEquals(ActionStatus.Success, result.status)
      assertEquals(2, runner.compacts.size)
      assertEquals(1, runner.compacts.distinct.size)
      assertEquals(1, events.size)
      assertEquals(runner.compacts.last, events.head.sql)
      if (version == "1.3") {
        val expected = explicit.getOrElse(if (primary && !partitioned) "minor" else "full")
        assertTrue(
          runner.compacts.last.contains(s"compact_strategy => '$expected'"),
          s"primary=$primary, partitioned=$partitioned, explicit=$explicit")
        assertEquals(expected, result.metrics("effectiveCompactStrategy"))
      } else {
        assertFalse(runner.compacts.last.contains("compact_strategy"))
        assertTrue(runner.compacts.last.contains("partition_idle_time => '1h'"))
      }
    }
  }

  @Test
  def unpartitionedPrimaryKeyStrategyStillValidatesInputAndReportsFailureOnce(): Unit = {
    Seq("", "invalid").foreach { strategy =>
      val runner = new FakeRunner(keys = Seq("id"), partitions = Seq.empty)
      val events = ArrayBuffer.empty[PaimonCompactionCompletion]
      val action = new PaimonCompactAction(runner, e => { events += e; () })
      assertEquals(
        ActionStatus.Failed,
        execute(action, table, "--compactStrategy", strategy).status)
      assertTrue(runner.compacts.isEmpty)
      assertTrue(events.isEmpty)
    }
    val runner = new FakeRunner(failures = 3, keys = Seq("id"), partitions = Seq.empty)
    val events = ArrayBuffer.empty[PaimonCompactionCompletion]
    val action = new PaimonCompactAction(runner, e => { events += e; () })
    assertEquals(ActionStatus.Failed, execute(action, table).status)
    assertEquals(1, events.size)
    assertFalse(events.head.success)
    assertEquals(3, events.head.attempts)
    assertEquals(runner.compacts.last, events.head.sql)
    assertTrue(runner.compacts.forall(_.contains("compact_strategy => 'minor'")))
  }

  @Test
  def logsTheMinorDefaultWithoutOverridingAnExplicitFull(): Unit = {
    captureLogs(
      logs => {
        val runner = new FakeRunner(keys = Seq("id"), partitions = Seq.empty)
        val action = new PaimonCompactAction(runner)
        assertEquals(ActionStatus.Success, execute(action, table).status)
        val selected =
          logs.filter(_.getMessage.getFormattedMessage.startsWith("Compaction defaults to minor"))
        assertEquals(1, selected.size)
        assertEquals(org.apache.logging.log4j.Level.INFO, selected.head.getLevel)
        val message = selected.head.getMessage.getFormattedMessage
        Seq("catalog=paimon", "database=db", "table=t1").foreach(part =>
          assertTrue(message.contains(part)))
        logs.clear()
        assertEquals(
          ActionStatus.Success,
          execute(action, table, "--compactStrategy", "full").status)
        assertTrue(runner.compacts.last.contains("compact_strategy => 'full'"))
        assertFalse(
          logs.exists(_.getMessage.getFormattedMessage.startsWith("Compaction defaults to minor")))
      },
      classOf[PaimonCompactAction])
  }

  @Test
  def legacyPathAlsoReportsCompletion(): Unit = {
    val events = ArrayBuffer.empty[PaimonCompactionCompletion]
    val action = new PaimonCompactAction(
      new FakeRunner(properties = Map.empty),
      event => { events += event; () })
    assertEquals(
      ActionStatus.Success,
      execute(action, table, "--version", "0.9", "--where", "dt = 1").status)
    assertEquals("0.9", events.head.effectiveVersion)
    assertTrue(events.head.sql.contains("partition_idle_time"))
  }

  @Test
  def defaultCallbackLogsEnglishInfoWithEscapedSingleLineJson(): Unit = {
    captureLogs { logs =>
      val runner = new FakeRunner()
      val target = TargetTable("db.t1", "db\nforged", "t1\rforged")
      assertEquals(ActionStatus.Success, execute(new PaimonCompactAction(runner), target).status)
      assertEquals(1, logs.size)
      assertEquals(org.apache.logging.log4j.Level.INFO, logs.head.getLevel)
      val text = logs.head.getMessage.getFormattedMessage
      assertTrue(text.startsWith("Compaction completed: "))
      assertFalse(text.contains("\n"))
      assertFalse(text.contains("\r"))
      val json = new ObjectMapper().readTree(text.stripPrefix("Compaction completed: "))
      assertEquals(target.database, json.get("database").asText())
      assertEquals(target.table, json.get("table").asText())
      assertEquals(runner.compacts.head, json.get("sql").asText())
      assertTrue(json.get("success").asBoolean())
      assertEquals(1, json.get("attempts").asInt())
      assertNotNull(Instant.parse(json.get("startTime").asText()))
      assertNotNull(Instant.parse(json.get("endTime").asText()))
      assertTrue(json.get("durationMs").asDouble() >= 0)
    }
  }

  @Test
  def callbackAndInfoRedactSensitiveOptionsAndFailureSqlWithoutChangingExecution(): Unit = {
    captureLogs { logs =>
      val secret = "s3cr'et\\credential"
      val token = "token-value-123"
      val runner = new FakeRunner(error = Some(s"Invalid options: $secret $token"))
      val events = ArrayBuffer.empty[PaimonCompactionCompletion]
      val action = new PaimonCompactAction(
        runner,
        event => {
          events += event
          PaimonCompactionCompletion.log(event)
        })
      val result = execute(
        action,
        table,
        "--procedureOptions",
        s"password=$secret,fs.s3a.access.key=$token,target-file-size=256m")
      assertEquals(ActionStatus.Failed, result.status)
      assertFalse(result.message.get.contains(secret))
      assertFalse(result.message.get.contains(token))
      assertFalse(result.tasks.head.message.get.contains(token))
      assertTrue(runner.compacts.head.contains(token))
      assertTrue(runner.compacts.head.contains(
        PaimonCompactAction.procedureString(secret).drop(1).dropRight(1)))
      assertEquals(1, events.size)
      assertTrue(events.head.sql.contains("password=[REDACTED]"))
      assertTrue(events.head.sql.contains("fs.s3a.access.key=[REDACTED]"))
      assertTrue(events.head.sql.contains("target-file-size=256m"))
      assertFalse(events.head.error.get.contains(secret))
      assertFalse(events.head.error.get.contains(token))
      assertEquals(1, logs.size)
      assertFalse(logs.head.getMessage.getFormattedMessage.contains(token))
      assertFalse(logs.head.getMessage.getFormattedMessage.contains("s3cr"))
    }
  }

  @Test
  def redactsAzureAccountKeysIncludingAccountSpecificKeys(): Unit = {
    captureLogs { logs =>
      Seq("fs.azure.account.key", "fs.azure.account.key.example.blob.core.windows.net")
        .foreach { key =>
          val secret = "fake-azure-account-key"
          val runner = new FakeRunner(error = Some(s"Invalid credential: $secret"))
          val action = new PaimonCompactAction(runner)
          val result = execute(action, table, "--procedureOptions", s"$key=$secret")
          assertEquals(ActionStatus.Failed, result.status)
          assertTrue(runner.compacts.head.contains(secret))
          assertFalse(result.message.get.contains(secret))
        }
      assertEquals(2, logs.size)
      logs.foreach { event =>
        assertFalse(event.getMessage.getFormattedMessage.contains("fake-azure-account-key"))
        assertTrue(event.getMessage.getFormattedMessage.contains("[REDACTED]"))
      }
    }
  }

  private def execute(
      action: PaimonCompactAction,
      target: TargetTable,
      args: String*): ActionResult = {
    val versionedArgs = if (args.contains("--version")) args else Seq("--version", "1.3") ++ args
    val config = action.parse(ProducerCliParser.parseMerged(action.options, versionedArgs.toArray))
    action.execute(context, target, config)
  }

  private def captureLogs(
      check: ArrayBuffer[org.apache.logging.log4j.core.LogEvent] => Unit,
      loggerClass: Class[_] = classOf[PaimonCompactionCompletion]): Unit = {
    val logs = ArrayBuffer.empty[org.apache.logging.log4j.core.LogEvent]
    val logger = org.apache.logging.log4j.LogManager.getLogger(loggerClass)
      .asInstanceOf[org.apache.logging.log4j.core.Logger]
    val level = logger.getLevel
    val appender = new org.apache.logging.log4j.core.appender.AbstractAppender(
      "compaction-completion-test",
      null,
      org.apache.logging.log4j.core.layout.PatternLayout.createDefaultLayout(),
      false,
      org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
      override def append(event: org.apache.logging.log4j.core.LogEvent): Unit =
        logs += event.toImmutable
    }
    appender.start()
    logger.addAppender(appender)
    logger.setLevel(org.apache.logging.log4j.Level.INFO)
    try check(logs)
    finally {
      logger.removeAppender(appender)
      logger.setLevel(level)
      appender.stop()
    }
  }

  private class FakeRunner(
      properties: Map[String, String] = managed,
      failures: Int = 0,
      rows: Seq[Row] = Seq(Row(true)),
      metadataFails: Boolean = false,
      error: Option[String] = None,
      keys: Seq[String] = Seq.empty,
      partitions: Seq[String] = Seq("dt", "hour")) extends PaimonSqlRunner {
    val compacts = ArrayBuffer.empty[String]
    var metadataFinishedAt: Instant = _
    override def loadTableMetadata(
        spark: SparkSession,
        catalogName: String,
        table: TargetTable): PaimonTableMetadata = PaimonTableMetadata(
      keys,
      partitions,
      new org.apache.spark.sql.types.StructType(),
      properties)
    override def run(spark: SparkSession, sql: String): Seq[Row] = {
      if (sql.startsWith("SELECT")) {
        if (metadataFails) throw new IllegalStateException("metadata unavailable")
        metadataFinishedAt = Instant.now()
        properties.toSeq.map { case (key, value) => Row(key, value) }
      } else {
        compacts += sql
        error.foreach(message => throw new IllegalStateException(message + "\n" + sql))
        if (compacts.size <= failures) throw new IllegalStateException("temporary failure")
        rows
      }
    }
  }
}
