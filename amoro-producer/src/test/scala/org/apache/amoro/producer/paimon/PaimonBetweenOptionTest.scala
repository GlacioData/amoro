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
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core._

class PaimonBetweenOptionTest {
  private val common =
    CommonProducerConfig("paimon", "compact", "paimon", Some("db"), Some("t1"), None, 3, true)
  private val predicate = "preset_dt BETWEEN '2026-10-09' AND '2026-10-10'"

  private def parsed(args: String*): PaimonCompactConfig = {
    val action = new PaimonCompactAction()
    val config = action.parse(ProducerCliParser.parseMerged(action.options, args.toArray))
    action.validate(common, config)
    config.asInstanceOf[PaimonCompactConfig]
  }

  @Test
  def generatesStrictCalendarRangesIncludingMonthYearAndLeapBoundaries(): Unit = {
    Seq(
      ("2026-10-10", "2026-10-09", "2026-10-10"),
      (" 2026-10-10 ", "2026-10-09", "2026-10-10"),
      ("2026-10-01", "2026-09-30", "2026-10-01"),
      ("2026-01-01", "2025-12-31", "2026-01-01"),
      ("2024-03-01", "2024-02-29", "2024-03-01"),
      ("2026-03-01", "2026-02-28", "2026-03-01"),
      ("0001-01-02", "0001-01-01", "0001-01-02"),
      ("9999-12-31", "9999-12-30", "9999-12-31"))
      .foreach { case (input, start, end) =>
        assertEquals(
          Some(s"preset_dt BETWEEN '$start' AND '$end'"),
          parsed("--between", input).where)
      }
    assertEquals(Some(predicate), parsed("--between=2026-10-10").where)
    assertEquals(None, parsed().where)
    assertEquals(Some(" preset_dt = 'value' "), parsed("--where", " preset_dt = 'value' ").where)
  }

  @Test
  def rejectsInvalidDatesWithTheParameterName(): Unit = {
    Seq(
      "",
      " ",
      "\u3000",
      "2026-02-30",
      "2026-02-29",
      "2026-13-01",
      "2026-1-01",
      "20261010",
      "2026-10-10T00:00:00",
      "0000-01-01",
      "0001-01-01",
      "10000-01-01",
      "today").foreach { input =>
      val error =
        assertThrows(classOf[IllegalArgumentException], () => { parsed("--between", input); () })
      assertTrue(error.getMessage.contains("--between"), error.getMessage)
    }
  }

  @Test
  def rejectsBothExplicitOptionsRegardlessOfOrderOrBlankValues(): Unit = {
    for (where <- Seq(predicate, "", " "); between <- Seq("2026-10-10", "", " ")) {
      Seq(
        Seq("--where", where, "--between", between),
        Seq("--between", between, "--where", where),
        Seq(s"--where=$where", s"--between=$between"))
        .foreach { args =>
          val error =
            assertThrows(classOf[IllegalArgumentException], () => { parsed(args: _*); () })
          assertTrue(error.getMessage.contains("--where") && error.getMessage.contains("--between"))
        }
    }
  }

  @Test
  def invalidInputsExitBeforeCreatingSpark(): Unit = {
    Seq(
      Seq("--between", "2026-02-30"),
      Seq("--between"),
      Seq("--between="),
      Seq("--between", "2026-10-10", "--where="),
      Seq("--where", predicate, "--between", "2026-10-10"))
      .foreach { args =>
        var created = false
        val code = ProducerApp.runInternal(
          (Seq(
            "--format",
            "paimon",
            "--action",
            "compact",
            "--databaseName",
            "db") ++ args).toArray,
          Seq(PaimonFormatAdapter),
          () => { created = true; null })
        assertEquals(2, code)
        assertFalse(created)
      }
  }

  @Test
  def generatedWhereUsesTheSameRoutingScopeSqlAndRetryAsExplicitWhere(): Unit = {
    for {
      version <- Seq("0.9", "1.3")
      (keys, partitions, decision) <- Seq(
        (Seq("id"), Seq("preset_dt"), "removed-primary-key"),
        (Seq.empty[String], Seq("preset_dt", "preset_hr"), "retained"),
        (Seq.empty[String], Seq("preset_pt"), "replaced-preset-dt"),
        (Seq.empty[String], Seq("preset_hr"), "removed-partition-mismatch"),
        (Seq.empty[String], Seq.empty[String], "removed-partition-mismatch"))
    } {
      val properties = if (version == "1.3")
        Map("self-optimizing.enabled" -> "true", "write-only" -> "true")
      else Map.empty[String, String]
      def execute(range: Seq[String])
          : (ActionResult, Seq[String], Seq[PaimonCompactionCompletion]) = {
        val sqls = ArrayBuffer.empty[String]
        val events = ArrayBuffer.empty[PaimonCompactionCompletion]
        val runner = new PaimonSqlRunner {
          override def loadTableMetadata(
              spark: SparkSession,
              catalogName: String,
              table: TargetTable): PaimonTableMetadata =
            PaimonTableMetadata(keys, partitions, new StructType(), properties)
          override def run(spark: SparkSession, sql: String): Seq[Row] = {
            if (sql.startsWith("SELECT")) properties.toSeq.map { case (k, v) => Row(k, v) }
            else {
              sqls += sql
              if (sqls.size == 1) throw new IllegalStateException("retry once")
              Seq(Row(true))
            }
          }
        }
        val action = new PaimonCompactAction(runner, event => { events += event; () })
        val result = action.execute(
          ProducerContext(null, common, PaimonFormatAdapter),
          TargetTable("db.t1", "db", "t1"),
          parsed((Seq("--version", version) ++ range): _*))
        (result, sqls.toSeq, events.toSeq)
      }
      val (result, sqls, events) = execute(Seq("--between", "2026-10-10"))
      val (_, explicitSqls, _) = execute(Seq("--where", predicate))
      assertEquals(ActionStatus.Success, result.status)
      assertEquals(explicitSqls, sqls)
      assertEquals(2, sqls.size)
      assertEquals(1, sqls.distinct.size)
      assertEquals(1, events.size)
      assertEquals(Some(predicate), events.head.scope.requestedWhere)
      assertEquals(decision, events.head.scope.decision)
      assertEquals(sqls.last, events.head.sql)
      assertEquals(predicate, result.metrics("requestedWhere"))
      if (version == "0.9") assertTrue(sqls.last.contains("partition_idle_time => '1h'"))
    }
  }

  @Test
  def betweenDoesNotBypassTheVersionSkip(): Unit = {
    var metadataLoaded = false
    var compactCalled = false
    var callbackCalled = false
    val runner = new PaimonSqlRunner {
      override def loadTableMetadata(
          spark: SparkSession,
          catalogName: String,
          table: TargetTable): PaimonTableMetadata = {
        metadataLoaded = true
        throw new AssertionError("Skipped tables must not load metadata")
      }
      override def run(spark: SparkSession, sql: String): Seq[Row] = {
        if (sql.startsWith("SELECT"))
          Seq(Row("self-optimizing.enabled", "true"), Row("write-only", "true"))
        else { compactCalled = true; Seq(Row(true)) }
      }
    }
    val action = new PaimonCompactAction(runner, _ => { callbackCalled = true })
    val result = action.execute(
      ProducerContext(null, common, PaimonFormatAdapter),
      TargetTable("db.t1", "db", "t1"),
      parsed("--between", "2026-10-10"))
    assertEquals(ActionStatus.Skipped, result.status)
    assertFalse(metadataLoaded || compactCalled || callbackCalled)
  }
}
