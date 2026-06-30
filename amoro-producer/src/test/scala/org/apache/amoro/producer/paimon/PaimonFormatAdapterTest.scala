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
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core.{CommonProducerConfig, ProducerApp, ProducerCliParser, ProducerContext, TargetTable}

class PaimonFormatAdapterTest {

  @Test
  def adapterExposesFourActions(): Unit = {
    assertEquals("paimon", PaimonFormatAdapter.format)
    assertEquals(
      Seq("compact", "compact-manifest", "expire-snapshots", "remove-orphan-files"),
      PaimonFormatAdapter.actions.map(_.name))
  }

  @Test
  def adapterActionsUseInjectedRunner(): Unit = {
    val runner = new FakeRunner(Seq(Row("1")))
    val adapter = new PaimonFormatAdapter(runner)
    val context = ProducerContext(null, commonConfig.copy(retryTimes = 1), adapter)
    val table = TargetTable("db.orders", "db", "orders")

    adapter.actions.foreach { action =>
      val commandLine = ProducerCliParser.parseMerged(action.options, Array.empty[String])
      action.execute(context, table, action.parse(commandLine))
    }

    assertTrue(runner.sqls.exists(_.contains("sys.compact(")))
    assertTrue(runner.sqls.exists(_.contains("sys.compact_manifest(")))
    assertTrue(runner.sqls.exists(_.contains("sys.expire_snapshots(")))
    assertTrue(runner.sqls.exists(_.contains("sys.remove_orphan_files(")))
  }

  @Test
  def producerAppResolvesRealPaimonAdapterAndAction(): Unit = {
    val adapter = ProducerApp.resolveAdapter("paimon", Seq(PaimonFormatAdapter))
    val action = ProducerApp.resolveAction(PaimonFormatAdapter, "compact")

    assertEquals("paimon", adapter.format)
    assertEquals("compact", action.name)
  }

  @Test
  def producerAppRejectsMissingRealPaimonActionWithAvailableActions(): Unit = {
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () => ProducerApp.resolveAction(PaimonFormatAdapter, "missing"))

    assertTrue(exception.getMessage.contains("compact"))
    assertTrue(exception.getMessage.contains("compact-manifest"))
    assertTrue(exception.getMessage.contains("expire-snapshots"))
    assertTrue(exception.getMessage.contains("remove-orphan-files"))
  }

  @Test
  def producerAppWithRealPaimonAdapterRejectsMissingFormatOrActionBeforeSpark(): Unit = {
    var sparkCreated = false

    val exitCode =
      ProducerApp.runInternal(
        Array("--databaseName", "db"),
        Seq(PaimonFormatAdapter),
        () => {
          sparkCreated = true
          null
        })

    assertEquals(2, exitCode)
    assertTrue(!sparkCreated)
  }

  @Test
  def producerAppWithRealPaimonAdapterRejectsUnsupportedFormatOrActionBeforeSpark(): Unit = {
    var sparkCreated = false
    val builder = () => {
      sparkCreated = true
      null
    }

    assertEquals(
      2,
      ProducerApp.runInternal(
        Array("--format", "iceberg", "--action", "compact", "--databaseName", "db"),
        Seq(PaimonFormatAdapter),
        builder))
    assertEquals(
      2,
      ProducerApp.runInternal(
        Array("--format", "paimon", "--action", "missing", "--databaseName", "db"),
        Seq(PaimonFormatAdapter),
        builder))
    assertTrue(!sparkCreated)
  }

  @Test
  def listTablesFiltersTemporaryTables(): Unit = {
    val runner =
      new FakeRunner(
        Seq(
          Row("db", "orders", false),
          Row("db", "tmp_orders", true),
          Row("db", "customers", "false"),
          Row("db", "tmp_customers", "1")))
    val adapter = new PaimonFormatAdapter(runner)

    val targets = adapter.listTables(null, commonConfig, "db")

    assertEquals(
      Seq(TargetTable("db.orders", "db", "orders"), TargetTable("db.customers", "db", "customers")),
      targets)
    assertEquals(Seq("SHOW TABLES IN `paimon`.`db`"), runner.sqls)
  }

  @Test
  def listTablesAcceptsTableColumnFallback(): Unit = {
    val runner = new FakeRunner(Seq(Row("database_ignored", "events", false)))
    val adapter = new PaimonFormatAdapter(runner)

    val targets = adapter.listTables(null, commonConfig, "db")

    assertEquals(Seq(TargetTable("db.events", "db", "events")), targets)
  }

  @Test
  def listTablesIgnoresEmptyTableNames(): Unit = {
    val runner = new FakeRunner(Seq(Row("db", "  ", false), Row("db", null, false)))
    val adapter = new PaimonFormatAdapter(runner)

    assertTrue(adapter.listTables(null, commonConfig, "db").isEmpty)
  }

  private def commonConfig: CommonProducerConfig = {
    CommonProducerConfig(
      format = "paimon",
      action = "compact",
      catalogName = "paimon",
      databaseName = Some("db"),
      tableName = None,
      tableNameRegex = None,
      retryTimes = 3,
      continueOnTableFailure = true)
  }

  final private class FakeRunner(rows: Seq[Row]) extends PaimonSqlRunner {
    var sqls: Seq[String] = Seq.empty

    override def run(spark: SparkSession, sqlText: String): Seq[Row] = {
      sqls = sqls :+ sqlText
      rows
    }
  }
}
