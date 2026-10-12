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

import java.nio.file.Path
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir

import org.apache.amoro.producer.core._

abstract class CompactIntegrationSupport {
  @TempDir var directory: Path = _
  protected var spark: SparkSession = _
  protected def defaultCompactionVersion: String = "1.3"

  @BeforeEach
  def startSpark(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("producer-compact-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", directory.resolve("spark").toUri.toString)
      .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.paimon.type", "filesystem")
      .config("spark.sql.catalog.paimon.warehouse", directory.resolve("paimon").toUri.toString)
      .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .config("spark.sql.defaultCatalog", "spark_catalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE paimon.db")
  }

  @AfterEach
  def stopSpark(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  protected def createTable(name: String, primary: Boolean, managed: Boolean): Unit = {
    val bucket = if (primary) "1" else "-1"
    val key = if (primary) ", 'primary-key'='id,dt'" else ""
    val managedOption = if (managed) ", 'self-optimizing.enabled'='true'" else ""
    spark.sql(s"""CREATE TABLE paimon.db.$name (id INT, value STRING, dt STRING)
      USING paimon PARTITIONED BY (dt)
      TBLPROPERTIES ('bucket'='$bucket', 'write-only'='true',
        'compaction.min.file-num'='2', 'compaction.max.file-num'='3'$key$managedOption)""")
    (1 to 3).foreach { id =>
      spark.sql(s"INSERT INTO paimon.db.$name VALUES ($id, 'v$id', '2026-10-01'), ($id, 'v$id', '2026-10-02')")
    }
  }

  protected def compact(name: String, args: String*): ActionResult = {
    compactWith(new PaimonCompactAction(), name, args: _*)
  }

  protected def compactWith(
      action: PaimonCompactAction,
      name: String,
      args: String*): ActionResult = {
    val common =
      CommonProducerConfig("paimon", "compact", "paimon", Some("db"), Some(name), None, 1, true)
    val versionedArgs =
      if (args.contains("--version")) args else Seq("--version", defaultCompactionVersion) ++ args
    val config = action.parse(ProducerCliParser.parseMerged(action.options, versionedArgs.toArray))
    action.execute(
      ProducerContext(spark, common, PaimonFormatAdapter),
      TargetTable(s"db.$name", "db", name),
      config)
  }

  protected def files(name: String): Map[String, Set[String]] = {
    spark.sql(s"SELECT partition, file_path FROM paimon.db.`${name}$$files`")
      .collect().toSeq.groupBy(_.getString(0)).map { case (partition, rows) =>
        partition -> rows.map(_.getString(1)).toSet
      }
  }

  protected def data(name: String): Seq[Row] =
    spark.sql(s"SELECT * FROM paimon.db.$name ORDER BY dt, id").collect().toSeq

  @Test
  def resolvesPrefixRegexUsingRealCatalogAndCli(): Unit = {
    spark.sql("CREATE DATABASE paimon.sl_oki_prod")
    Seq("tidb_orders", "tidb_users", "other_tidb_orders").foreach { name =>
      spark.sql(s"CREATE TABLE paimon.sl_oki_prod.$name (id INT) USING paimon " +
        "TBLPROPERTIES ('bucket'='-1', 'write-only'='true')")
    }
    val action = new PaimonCompactAction()
    val commandLine = ProducerCliParser.parseMerged(
      action.options,
      Array(
        "--format",
        "paimon",
        "--action",
        "compact",
        "--catalogName",
        "paimon",
        "--databaseName",
        "sl_oki_prod",
        "--tableNameRegex",
        "^tidb_.*",
        "--retryTimes",
        "3",
        "--continueOnTableFailure",
        "true",
        "--where",
        "preset_dt = '2026-10-08'",
        "--version",
        "0.9"))
    val common = ProducerCliParser.parseCommon(commandLine)
    assertEquals(Some("^tidb_.*"), common.tableNameRegex)
    assertEquals("spark_catalog", spark.sessionState.catalogManager.currentCatalog.name())
    val listed = PaimonFormatAdapter.listTables(spark, common, "sl_oki_prod")
    assertEquals(Set("tidb_orders", "tidb_users", "other_tidb_orders"), listed.map(_.table).toSet)
    val targets = TableResolver.resolveTargets(spark, PaimonFormatAdapter, common)
    assertEquals(Set("sl_oki_prod.tidb_orders", "sl_oki_prod.tidb_users"), targets.map(_.raw).toSet)
  }

  @Test
  def legacySelectionSkipsManagedTablesBeforeProcedureAndRangeValidation(): Unit = {
    createTable("managed_skipped", primary = true, managed = true)
    val before = files("managed_skipped")
    var calls = 0
    var callbacks = 0
    val runner = new PaimonSqlRunner {
      override def run(session: SparkSession, sql: String): Seq[Row] = {
        if (sql.startsWith("CALL")) calls += 1
        SparkPaimonSqlRunner.run(session, sql)
      }
      override def loadTableMetadata(
          session: SparkSession,
          catalog: String,
          table: TargetTable): PaimonTableMetadata = {
        throw new AssertionError("Skipped tables must not load metadata")
      }
    }
    val action = new PaimonCompactAction(runner, _ => { callbacks += 1; () })
    val common = CommonProducerConfig(
      "paimon",
      "compact",
      "paimon",
      Some("db"),
      None,
      None,
      3,
      true)
    Seq(
      Array.empty[String],
      Array("--version", "0.9", "--where", "missing = 1", "--compactStrategy", "invalid"))
      .foreach { args =>
        val result = action.execute(
          ProducerContext(spark, common, PaimonFormatAdapter),
          TargetTable("db.managed_skipped", "db", "managed_skipped"),
          action.parse(ProducerCliParser.parseMerged(action.options, args)))
        assertEquals(ActionStatus.Skipped, result.status)
        assertTrue(result.message.get.contains("requires --version 1.3"))
      }
    assertEquals(0, calls)
    assertEquals(0, callbacks)
    assertEquals(before, files("managed_skipped"))
  }

  private def createAdaptiveTable(name: String, partitions: Seq[String]): Unit = {
    val partitionSql =
      if (partitions.isEmpty) "" else partitions.mkString("PARTITIONED BY (", ",", ")")
    val managed =
      if (defaultCompactionVersion == "1.3") ", 'self-optimizing.enabled'='true'" else ""
    spark.sql(s"""CREATE TABLE paimon.db.$name
      (id INT, preset_dt STRING, preset_pt STRING, preset_hr STRING)
      USING paimon $partitionSql TBLPROPERTIES ('bucket'='-1', 'write-only'='true',
        'compaction.min.file-num'='2', 'compaction.max.file-num'='3'$managed)""")
    (1 to 3).foreach { id =>
      spark.sql(s"""INSERT INTO paimon.db.$name VALUES
        ($id, '2026-10-01', '2026-10-08', '08'),
        ($id, '2026-10-01', '2026-10-09', '09')""")
    }
  }

  private def adaptiveData(name: String): Seq[Row] =
    spark.sql(s"SELECT * FROM paimon.db.$name ORDER BY id, preset_pt").collect().toSeq

  @Test
  def adaptsPresetPtWithoutChangingValuesOrBroadeningAnEmptyMatch(): Unit = {
    val name = "adaptive_pt"
    createAdaptiveTable(name, Seq("preset_pt", "preset_hr"))
    val before = files(name)
    val rows = adaptiveData(name)
    val events = scala.collection.mutable.ArrayBuffer.empty[PaimonCompactionCompletion]
    val action = new PaimonCompactAction(onCompactionCompleted = event => { events += event; () })
    assertEquals(
      ActionStatus.Success,
      compactWith(
        action,
        name,
        "--where",
        "preset_dt = 'not-present'",
        "--partitionIdleTime",
        "0s").status)
    assertEquals(before, files(name), "A valid zero-match predicate must never be removed")
    val result = compactWith(
      action,
      name,
      "--where",
      "preset_dt = '2026-10-08' AND preset_hr = '08'",
      "--partitionIdleTime",
      "0s")
    assertEquals(ActionStatus.Success, result.status, result.toString)
    assertEquals("replaced-preset-dt", result.metrics("whereDecision"))
    val after = files(name)
    before.keys.foreach { partition =>
      if (partition.contains("2026-10-08")) assertNotEquals(before(partition), after(partition))
      else assertEquals(before(partition), after(partition))
    }
    assertEquals(rows, adaptiveData(name))
    assertEquals(2, events.size)
    assertEquals(Seq("preset_pt", "preset_hr"), events.last.scope.partitionKeys)
    assertEquals(result.metrics("effectiveWhere"), events.last.scope.effectiveWhere.get)
    assertFalse(events.last.sql.contains("preset_dt"))
    assertTrue(events.last.sql.contains("preset_pt"))
  }

  @Test
  def removesTheEntireUnsupportedPredicateBeforeCallingCompaction(): Unit = {
    Seq(Seq.empty[String], Seq("preset_pt")).zipWithIndex.foreach { case (partitions, index) =>
      val name = s"adaptive_removed_$index"
      createAdaptiveTable(name, partitions)
      val before = files(name)
      val rows = adaptiveData(name)
      var event: PaimonCompactionCompletion = null
      val action = new PaimonCompactAction(onCompactionCompleted = value => { event = value })
      // preset_hr is a normal column here, so replacing preset_dt alone cannot make this fit.
      val result = compactWith(
        action,
        name,
        "--where",
        "preset_dt = '2026-10-08' AND preset_hr = '08'",
        "--partitionIdleTime",
        "0s")
      assertEquals(ActionStatus.Success, result.status, result.toString)
      assertEquals("removed-partition-mismatch", result.metrics("whereDecision"))
      val after = files(name)
      before.keys.foreach(partition => assertNotEquals(before(partition), after(partition)))
      assertEquals(rows, adaptiveData(name))
      assertEquals(None, event.scope.effectiveWhere)
      assertTrue(event.scope.requestedWhere.isDefined)
      assertFalse(event.sql.contains("where =>"))
    }
  }

  @Test
  def betweenCompactsBothEndpointsAndKeepsOutsideAndZeroMatchFiles(): Unit = {
    Seq("preset_dt", "preset_pt").foreach { partitionKey =>
      val name = s"between_$partitionKey"
      val managed =
        if (defaultCompactionVersion == "1.3") ", 'self-optimizing.enabled'='true'" else ""
      spark.sql(s"""CREATE TABLE paimon.db.$name (id INT, preset_dt STRING, preset_pt STRING)
        USING paimon PARTITIONED BY ($partitionKey)
        TBLPROPERTIES ('bucket'='-1', 'write-only'='true',
          'compaction.min.file-num'='2', 'compaction.max.file-num'='3'$managed)""")
      (1 to 3).foreach { id =>
        spark.sql(s"""INSERT INTO paimon.db.$name VALUES
          ($id, '2026-10-08', '2026-10-08'), ($id, '2026-10-09', '2026-10-09'),
          ($id, '2026-10-10', '2026-10-10'), ($id, '2026-10-11', '2026-10-11')""")
      }
      val before = files(name)
      val rows = adaptiveData(name)
      assertEquals(4, before.size)
      assertTrue(before.values.forall(_.size >= 3))
      val events = scala.collection.mutable.ArrayBuffer.empty[PaimonCompactionCompletion]
      val action = new PaimonCompactAction(onCompactionCompleted = e => { events += e; () })
      val empty = compactWith(action, name, "--between", "2030-01-01", "--partitionIdleTime", "0s")
      assertEquals(ActionStatus.Success, empty.status, empty.toString)
      assertEquals(before, files(name), "Zero match must retain its generated predicate")
      val result = compactWith(action, name, "--between", "2026-10-10", "--partitionIdleTime", "0s")
      assertEquals(ActionStatus.Success, result.status, result.toString)
      val after = files(name)
      before.keys.foreach { partition =>
        if (partition.contains("2026-10-09") || partition.contains("2026-10-10")) {
          assertNotEquals(before(partition), after(partition), partition)
        } else assertEquals(before(partition), after(partition), partition)
      }
      assertEquals(rows, adaptiveData(name))
      assertEquals(2, events.size)
      assertEquals(
        Some("preset_dt BETWEEN '2026-10-09' AND '2026-10-10'"),
        events.last.scope.requestedWhere)
      assertEquals(Seq(partitionKey), events.last.scope.partitionKeys)
      assertEquals(
        if (partitionKey == "preset_dt") "retained" else "replaced-preset-dt",
        events.last.scope.decision)
      assertTrue(events.last.sql.contains("where =>"))
      assertTrue(events.last.sql.contains(partitionKey))
      assertEquals(result.metrics("effectiveWhere"), events.last.scope.effectiveWhere.get)
    }
  }

  @Test
  def betweenIsRemovedForRealPrimaryKeyAndUnpartitionedTables(): Unit = {
    val managed = defaultCompactionVersion == "1.3"
    createTable("between_pk", primary = true, managed = managed)
    createAdaptiveTable("between_unpartitioned", Seq.empty)
    Seq("between_pk", "between_unpartitioned").foreach { name =>
      val before = files(name)
      val order = if (name == "between_pk") "id, dt" else "id, preset_pt, preset_dt"
      val rows = spark.sql(s"SELECT * FROM paimon.db.$name ORDER BY $order").collect().toSeq
      var event: PaimonCompactionCompletion = null
      val action = new PaimonCompactAction(onCompactionCompleted = e => { event = e })
      val result = compactWith(action, name, "--between", "2026-10-10", "--partitionIdleTime", "0s")
      assertEquals(ActionStatus.Success, result.status, result.toString)
      val after = files(name)
      before.keys.foreach(partition => assertNotEquals(before(partition), after(partition)))
      assertEquals(
        rows,
        spark.sql(s"SELECT * FROM paimon.db.$name ORDER BY $order").collect().toSeq)
      assertEquals(None, event.scope.effectiveWhere)
      assertFalse(event.sql.contains("where =>"))
      assertEquals(
        if (name == "between_pk") "removed-primary-key" else "removed-partition-mismatch",
        event.scope.decision)
    }
  }

  @Test
  def descriptionUsesFinalScopeAndStrategyAndRedactsSecrets(): Unit = {
    val condition = "preset_dt BETWEEN '2026-10-09' AND '2026-10-10'"
    for {
      (keys, partitions) <- Seq(
        (Seq("id"), Seq.empty[String]),
        (Seq.empty[String], Seq("preset_dt")),
        (Seq.empty[String], Seq("preset_pt")),
        (Seq.empty[String], Seq.empty[String]))
      explicit <- Seq(None, Some("full"))
    } {
      val properties = if (defaultCompactionVersion == "1.3")
        Map("self-optimizing.enabled" -> "true", "write-only" -> "true")
      else Map.empty[String, String]
      var observed: String = null
      var completion: PaimonCompactionCompletion = null
      val runner = new PaimonSqlRunner {
        override def loadTableMetadata(
            session: SparkSession,
            catalog: String,
            target: TargetTable): PaimonTableMetadata =
          PaimonTableMetadata(
            keys,
            partitions,
            new org.apache.spark.sql.types.StructType(),
            properties)
        override def run(session: SparkSession, sql: String): Seq[Row] = {
          assertEquals("outer job", session.sparkContext.getLocalProperty("spark.job.description"))
          properties.toSeq.map { case (k, v) => Row(k, v) }
        }
      }
      // Observe the description while CALL is executing, before the callback restores context.
      val observingRunner = new PaimonSqlRunner {
        override def loadTableMetadata(
            session: SparkSession,
            catalog: String,
            target: TargetTable): PaimonTableMetadata =
          runner.loadTableMetadata(session, catalog, target)
        override def run(session: SparkSession, sql: String): Seq[Row] = {
          if (sql.startsWith("SELECT")) runner.run(session, sql)
          else {
            observed = session.sparkContext.getLocalProperty("spark.job.description")
            Seq(Row(true))
          }
        }
      }
      val action = new PaimonCompactAction(observingRunner, e => { completion = e })
      val common =
        CommonProducerConfig("paimon", "compact", "paimon", Some("db"), None, None, 1, true)
      val args =
        Seq("--version", defaultCompactionVersion, "--where", condition) ++ explicit.toSeq.flatMap(
          v => Seq("--compactStrategy", v))
      spark.sparkContext.setJobDescription("outer job")
      try {
        val result = action.execute(
          ProducerContext(spark, common, PaimonFormatAdapter),
          TargetTable("db.description_scope", "db", "description_scope"),
          action.parse(ProducerCliParser.parseMerged(action.options, args.toArray)))
        assertEquals(ActionStatus.Success, result.status)
        val strategy = if (defaultCompactionVersion == "0.9") "native"
        else explicit.getOrElse(if (keys.nonEmpty && partitions.isEmpty) "minor" else "full")
        assertEquals(
          s"Compact table: paimon.db.description_scope | strategy=$strategy | where=${completion.scope.effectiveWhere.getOrElse("ALL")}",
          observed)
        assertEquals("outer job", spark.sparkContext.getLocalProperty("spark.job.description"))
      } finally spark.sparkContext.setJobDescription(null)
    }
    if (defaultCompactionVersion == "1.3") {
      var observed: String = null
      val runner = new PaimonSqlRunner {
        override def loadTableMetadata(
            session: SparkSession,
            catalog: String,
            target: TargetTable): PaimonTableMetadata =
          PaimonTableMetadata(
            Seq.empty,
            Seq("preset_dt"),
            new org.apache.spark.sql.types.StructType(),
            Map.empty)
        override def run(session: SparkSession, sql: String): Seq[Row] = {
          if (sql.startsWith("SELECT"))
            Seq(Row("self-optimizing.enabled", "true"), Row("write-only", "true"))
          else {
            assertTrue(sql.contains("test-secret-value"), "Actual SQL must retain the user's value")
            observed = session.sparkContext.getLocalProperty("spark.job.description")
            Seq(Row(true))
          }
        }
      }
      val action = new PaimonCompactAction(runner)
      val common =
        CommonProducerConfig("paimon", "compact", "paimon", Some("db"), None, None, 1, true)
      val args = Array(
        "--version",
        "1.3",
        "--where",
        "preset_dt = 'test-secret-value'",
        "--procedureOptions",
        "password=test-secret-value")
      assertEquals(
        ActionStatus.Success,
        action.execute(
          ProducerContext(spark, common, PaimonFormatAdapter),
          TargetTable("db.description_secret", "db", "description_secret"),
          action.parse(ProducerCliParser.parseMerged(action.options, args))).status)
      assertFalse(observed.contains("test-secret-value"))
      assertTrue(observed.contains("[REDACTED]"))
    }
  }

  protected def assertPartitionCompaction(primary: Boolean, managed: Boolean): Unit = {
    val name = if (primary) "pk" else "append_table"
    createTable(name, primary, managed)
    val beforeFiles = files(name)
    val beforeData = data(name)
    assertEquals(2, beforeFiles.size)
    assertTrue(beforeFiles.values.forall(_.size >= 2), beforeFiles.toString)
    val args = if (managed) Seq("--where", "dt = '2026-10-01'")
    else
      Seq("--where", "dt = '2026-10-01'", "--partitionIdleTime", "0s", "--version", "0.9")
    val description = new AtomicReference[String]()
    val jobStarted = new CountDownLatch(1)
    val listener = new SparkListener {
      override def onJobStart(event: SparkListenerJobStart): Unit = {
        if (event.stageInfos.exists(_.name.contains("CompactProcedure"))) {
          description.set(event.properties.getProperty("spark.job.description"))
          jobStarted.countDown()
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    spark.sparkContext.setJobDescription("parent job")
    val result =
      try {
        val outcome = compact(name, args: _*)
        assertTrue(jobStarted.await(10, TimeUnit.SECONDS), "Expected a real CompactProcedure job")
        val strategy = if (managed) "full" else "native"
        val where = if (primary) "ALL" else "dt = '2026-10-01'"
        assertEquals(
          s"Compact table: paimon.db.$name | strategy=$strategy | where=$where",
          description.get())
        assertEquals("parent job", spark.sparkContext.getLocalProperty("spark.job.description"))
        outcome
      } finally {
        spark.sparkContext.removeSparkListener(listener)
        spark.sparkContext.setJobDescription(null)
      }
    assertEquals(ActionStatus.Success, result.status, result.toString)
    assertEquals(if (managed) "1.3" else "0.9", result.metrics("effectiveVersion"))
    val afterFiles = files(name)
    val selected = beforeFiles.keys.find(_.contains("2026-10-01")).get
    val untouched = beforeFiles.keys.find(_.contains("2026-10-02")).get
    assertNotEquals(
      beforeFiles(selected),
      afterFiles(selected),
      "Selected partition must actually be rewritten")
    if (primary) {
      assertNotEquals(
        beforeFiles(untouched),
        afterFiles(untouched),
        "Primary-key compaction must not retain the supplied partition predicate")
      assertEquals("removed-primary-key", result.metrics("whereDecision"))
    } else {
      assertEquals(
        beforeFiles(untouched),
        afterFiles(untouched),
        "Unselected partition must not change")
    }
    assertEquals(beforeData, data(name))
    val properties = spark.sql(s"SELECT `key`, `value` FROM paimon.db.`${name}$$options`")
      .collect().map(row => row.getString(0) -> row.getString(1)).toMap
    assertEquals("true", properties("write-only"), "Persistent table options must remain unchanged")
  }
}

class PaimonCompactIntegrationTest extends CompactIntegrationSupport {
  @Test
  def unpartitionedPrimaryKeyDefaultMinorAndExplicitStrategiesReachNativeProcedure(): Unit = {
    Seq(None, Some("full"), Some("minor")).zipWithIndex.foreach { case (explicit, index) =>
      val name = s"unpartitioned_pk_strategy_$index"
      spark.sql(s"""CREATE TABLE paimon.db.$name (id INT, value STRING) USING paimon
        TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true',
          'self-optimizing.enabled'='true', 'compaction.min.file-num'='2',
          'num-sorted-run.compaction-trigger'='2', 'compaction.max.file-num'='3')""")
      (1 to 3).foreach(id => spark.sql(s"INSERT INTO paimon.db.$name VALUES ($id, 'v$id')"))
      val before = files(name)
      assertEquals(1, before.size)
      assertTrue(before.values.head.size >= 3)
      val rows = spark.sql(s"SELECT * FROM paimon.db.$name ORDER BY id").collect().toSeq
      var event: PaimonCompactionCompletion = null
      val action = new PaimonCompactAction(onCompactionCompleted = e => { event = e })
      val args = Seq("--between", "2026-10-10") ++ explicit.toSeq.map(v => s"--compactStrategy=$v")
      val result = compactWith(action, name, args: _*)
      val expected = explicit.getOrElse("minor")
      assertEquals(ActionStatus.Success, result.status, result.toString)
      assertEquals(expected, result.metrics("effectiveCompactStrategy"))
      assertEquals(Seq("id"), event.scope.primaryKeys)
      assertTrue(event.scope.partitionKeys.isEmpty)
      assertFalse(event.sql.contains("where =>"))
      assertTrue(event.sql.contains(s"compact_strategy => '$expected'"))
      assertNotEquals(before, files(name), "The native Procedure must actually compact the fixture")
      assertEquals(rows, spark.sql(s"SELECT * FROM paimon.db.$name ORDER BY id").collect().toSeq)
      assertEquals(
        "true",
        spark.sql(
          s"SELECT value FROM paimon.db.`${name}$$options` WHERE key='write-only'").head().getString(
          0))
    }
  }

  @Test def compactsAllPrimaryKeyPartitionsIgnoringWhere(): Unit =
    assertPartitionCompaction(true, true)
  @Test def compactsOnlySelectedAppendPartition(): Unit = assertPartitionCompaction(false, true)

  @Test
  def restoresJobDescriptionAfterRetriesAndBeforeCompletionCallback(): Unit = {
    Seq(null, "outer job").foreach { previous =>
      Seq(true, false).foreach { succeeds =>
        var attempts = 0
        var callbacks = 0
        val runner = new PaimonSqlRunner {
          override def loadTableMetadata(
              session: SparkSession,
              catalog: String,
              target: TargetTable): PaimonTableMetadata = PaimonTableMetadata(
            Seq.empty,
            Seq.empty,
            new org.apache.spark.sql.types.StructType(),
            Map.empty)
          override def run(session: SparkSession, sql: String): Seq[Row] = {
            if (sql.startsWith("CALL")) {
              assertEquals(
                "Compact table: paimon.db.description_test | strategy=full | where=ALL",
                session.sparkContext.getLocalProperty("spark.job.description"))
              attempts += 1
              if (attempts == 1 || !succeeds) throw new IllegalStateException("compact failed")
              Seq(Row(true))
            } else {
              assertEquals(previous, session.sparkContext.getLocalProperty("spark.job.description"))
              Seq(Row("self-optimizing.enabled", "true"), Row("write-only", "true"))
            }
          }
        }
        val action = new PaimonCompactAction(
          runner,
          _ => {
            assertEquals(previous, spark.sparkContext.getLocalProperty("spark.job.description"))
            callbacks += 1
          })
        val common = CommonProducerConfig(
          "paimon",
          "compact",
          "paimon",
          Some("db"),
          None,
          None,
          2,
          true)
        spark.sparkContext.setJobDescription(previous)
        try {
          val result = action.execute(
            ProducerContext(spark, common, PaimonFormatAdapter),
            TargetTable("db.description_test", "db", "description_test"),
            action.parse(ProducerCliParser.parseMerged(action.options, Array("--version", "1.3"))))
          assertEquals(if (succeeds) ActionStatus.Success else ActionStatus.Failed, result.status)
          assertEquals(2, attempts)
          assertEquals(1, callbacks)
          assertEquals(previous, spark.sparkContext.getLocalProperty("spark.job.description"))
        } finally spark.sparkContext.setJobDescription(null)
      }
    }
  }

  @Test
  def preservesLiteralsWithTheRealSparkParser(): Unit = {
    Seq(
      "dt = '2026-10-01'",
      "地区 = '广州' AND dt > '2026-10-01'",
      "a\\b\\'c\"d\n",
      "x'; DROP TABLE t; --")
      .foreach { value =>
        val decoded =
          spark.sql(s"SELECT ${PaimonCompactAction.procedureString(value)}").head().getString(0)
        assertEquals(value, decoded)
        val scope = PaimonCompactionScope.resolve(
          spark,
          PaimonTableMetadata(
            Seq.empty,
            Seq("preset_pt"),
            new org.apache.spark.sql.types.StructType(),
            Map.empty),
          Some("preset_dt = " + PaimonCompactAction.procedureString(value)))
        val finalPredicate = scope.effectiveWhere.get
        val restored = spark.sql("SELECT " + PaimonCompactAction.procedureString(finalPredicate))
          .head().getString(0)
        assertEquals(finalPredicate, restored)
        assertEquals(
          Seq(value),
          spark.sessionState.sqlParser.parseExpression(restored)
            .collect { case literal: org.apache.spark.sql.catalyst.expressions.Literal =>
              literal.value.toString
            })
      }
  }

  @Test
  def invalidWhereNeverChangesAnyPartitionAndEmptySelectionSucceeds(): Unit = {
    createTable("invalid_where", primary = false, managed = true)
    val before = files("invalid_where")
    Seq("unknown_function(dt) = 1", "dt =", "dt IN (SELECT 1)").foreach { predicate =>
      val result = compact("invalid_where", "--where", predicate)
      assertEquals(ActionStatus.Failed, result.status)
      assertEquals(before, files("invalid_where"))
    }
    assertEquals(
      ActionStatus.Success,
      compact("invalid_where", "--where", "dt = 'not-present'").status)
    assertEquals(before, files("invalid_where"))
  }

  @Test
  def unicodeWhitespaceWhereNeverTriggersWholeTableCompaction(): Unit = {
    createTable("blank_where", primary = false, managed = true)
    val before = files("blank_where")
    Seq("\u3000", "\u2003").foreach { condition =>
      assertThrows(
        classOf[IllegalArgumentException],
        () => compact("blank_where", "--where", condition))
      assertEquals(before, files("blank_where"))
    }
  }

  @Test
  def rejectsSqlVariableSubstitutionBeforeItCanChangeTheRange(): Unit = {
    createTable("variable_range", primary = false, managed = true)
    val before = files("variable_range")
    spark.conf.set("producer.incremental", "true")
    val result = compact(
      "variable_range",
      "--where",
      "dt = '2026-10-01'",
      "--procedureOptions",
      "clustering.incremental=${producer.incremental}")
    assertEquals(ActionStatus.Failed, result.status)
    assertTrue(result.message.get.contains("variable.substitute"), result.toString)
    assertEquals(before, files("variable_range"))
    spark.conf.set("producer.partition", "2026-10-01")
    val condition = "dt = '${producer.partition}'"
    assertEquals(ActionStatus.Failed, compact("variable_range", "--where", condition).status)
    spark.conf.set("spark.sql.variable.substitute", "false")
    assertEquals(ActionStatus.Success, compact("variable_range", "--where", condition).status)
    assertEquals(
      before,
      files("variable_range"),
      "Variable references remain literal when substitution is disabled")
  }

  @Test
  def sessionOptionsCannotBypassPartitionRangeProtection(): Unit = {
    createTable("session_range", primary = false, managed = true)
    val before = files("session_range")
    var calls = 0
    val runner = new PaimonSqlRunner {
      override def run(session: SparkSession, sql: String): Seq[Row] = {
        if (sql.startsWith("CALL")) calls += 1
        SparkPaimonSqlRunner.run(session, sql)
      }
    }
    val action = new PaimonCompactAction(runner)
    Seq(
      "spark.paimon.clustering.incremental",
      "spark.paimon.paimon.db.session_range.clustering.incremental",
      "spark.paimon.*.db.*.clustering.incremental").foreach { key =>
      spark.conf.set(key, "true")
      try {
        val result = compactWith(action, "session_range", "--where", "dt = '2026-10-01'")
        assertEquals(ActionStatus.Failed, result.status)
        assertTrue(result.message.get.contains("--where is unsupported"), result.toString)
        assertEquals(0, calls, "Unsafe branch must be rejected before submitting CALL")
        assertEquals(before, files("session_range"))
      } finally {
        spark.conf.unset(key)
      }
    }
    spark.conf.set("spark.paimon.clustering.incremental", "true")
    val result = compactWith(
      action,
      "session_range",
      "--where",
      "dt = '2026-10-01'",
      "--procedureOptions",
      "clustering.incremental=false")
    assertEquals(ActionStatus.Success, result.status, result.toString)
    assertEquals(1, calls)
    val after = files("session_range")
    val untouched = before.keys.find(_.contains("2026-10-02")).get
    assertEquals(before(untouched), after(untouched))
  }

  @Test
  def effectiveOptionsUseTheProvidedSessionAndRestoreTheActiveSession(): Unit = {
    createTable("session_context", primary = false, managed = true)
    val other = spark.newSession()
    other.conf.set("spark.paimon.clustering.incremental", "true")
    SparkSession.setActiveSession(spark)
    val target = TargetTable("db.session_context", "db", "session_context")
    val metadata = SparkPaimonSqlRunner.loadTableMetadata(other, "paimon", target)
    assertEquals("true", metadata.properties("clustering.incremental"))
    assertEquals(Seq.empty, metadata.primaryKeys)
    assertEquals(Seq("dt"), metadata.partitionKeys)
    assertTrue(metadata.schema.fieldNames.contains("id"))
    assertSame(spark, SparkSession.getActiveSession.get)
    assertThrows(
      classOf[Exception],
      () =>
        SparkPaimonSqlRunner.loadTableMetadata(
          other,
          "paimon",
          TargetTable("db.missing", "db", "missing")))
    assertSame(spark, SparkSession.getActiveSession.get)
    SparkSession.clearActiveSession()
    SparkPaimonSqlRunner.loadTableMetadata(other, "paimon", target)
    assertEquals(None, SparkSession.getActiveSession)
  }

  @Test
  def legacyStringParserModeIsRejectedBeforeCompaction(): Unit = {
    createTable("legacy_parser", primary = true, managed = true)
    val before = files("legacy_parser")
    spark.conf.set("spark.sql.parser.escapedStringLiterals", "true")
    val result = compact("legacy_parser", "--where", "dt = '2026-10-01'")
    assertEquals(ActionStatus.Failed, result.status)
    assertTrue(result.message.get.contains("escapedStringLiterals=false"))
    assertEquals(before, files("legacy_parser"))
  }
}

class PaimonCompactLegacyIntegrationTest extends CompactIntegrationSupport {
  override protected def defaultCompactionVersion: String = "0.9"
  @Test def legacyPrimaryKeyRange(): Unit = assertPartitionCompaction(true, false)
  @Test def legacyAppendRange(): Unit = assertPartitionCompaction(false, false)
}
