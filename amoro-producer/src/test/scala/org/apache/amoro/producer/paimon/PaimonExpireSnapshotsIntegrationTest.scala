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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.{Duration, Instant}

import scala.collection.JavaConverters._

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.FileStoreTable
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir

import org.apache.amoro.producer.core._

class PaimonExpireSnapshotsIntegrationTest {
  @TempDir var directory: Path = _
  private var spark: SparkSession = _

  @BeforeEach
  def startSpark(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("producer-expire-test")
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

  @Test
  def realExpirationRespectsTableRetentionAndPerRoundLimit(): Unit = {
    createTable(
      "table_policy",
      ",'snapshot.num-retained.min'='3','snapshot.num-retained.max'='4'," +
        "'snapshot.time-retained'='24 h','snapshot.expire.limit'='1'")
    val before = data("table_policy")
    val expected = "Expire snapshots: paimon.db.table_policy | retained_min=3 | retained_max=4 " +
      "| retained_time=86400000ms | max_deletes=1"
    var calls = 0
    val runner = new PaimonSqlRunner {
      override def run(session: SparkSession, sql: String): Seq[Row] = {
        if (sql.trim.startsWith("CALL")) {
          calls += 1
          assertEquals(expected, session.sparkContext.getLocalProperty("spark.job.description"))
          val taskDescriptions = session.sparkContext.parallelize(Seq(1), 1).map { _ =>
            TaskContext.get().getLocalProperty("spark.job.description")
          }.collect().toSeq
          assertEquals(Seq(expected), taskDescriptions)
        } else {
          assertEquals("outer job", session.sparkContext.getLocalProperty("spark.job.description"))
        }
        SparkPaimonSqlRunner.run(session, sql)
      }
    }
    spark.sparkContext.setJobDescription("outer job")
    val result = expire(
      "table_policy",
      runner,
      "--retainMin",
      "1",
      "--retainMax",
      "2",
      "--procedureOptions",
      "snapshot.time-retained=1 ms")
    assertEquals(ActionStatus.Success, result.status)
    assertEquals(3, calls, "Two full deletion rounds must be followed by a zero-delete round")
    assertEquals("2", result.metrics("deletedSnapshotsTotal"))
    assertEquals(4, snapshots("table_policy"))
    assertEquals(before, data("table_policy"))
    assertEquals("outer job", spark.sparkContext.getLocalProperty("spark.job.description"))
  }

  @Test
  def absentAndPartialTablePoliciesFallBackPerParameter(): Unit = {
    createTable("no_policy", "")
    val absent = expire("no_policy", SparkPaimonSqlRunner, "--retainMin", "1", "--retainMax", "2")
    assertEquals(ActionStatus.Success, absent.status)
    assertEquals(2, snapshots("no_policy"))

    createTable("time_policy", ",'snapshot.time-retained'='0 ms'")
    val partial =
      expire("time_policy", SparkPaimonSqlRunner, "--retainMin", "3", "--retainMax", "5")
    assertEquals(ActionStatus.Success, partial.status)
    assertEquals("0", partial.metrics("timeRetainedMs"))
    assertEquals(3, snapshots("time_policy"))
    assertNull(spark.sparkContext.getLocalProperty("spark.job.description"))
  }

  @Test
  def descriptionIsRestoredAfterFailedRetriesAndDoesNotContainOptions(): Unit = {
    createTable("failed_policy", ",'snapshot.num-retained.min'='2','snapshot.num-retained.max'='3'")
    var calls = 0
    val runner = new PaimonSqlRunner {
      // This test observes a failing CALL rather than executing a native procedure.
      override def expireSnapshotsSupportsOptions(session: SparkSession, catalog: String): Boolean =
        true
      override def run(session: SparkSession, sql: String): Seq[Row] = {
        if (sql.trim.startsWith("CALL")) {
          calls += 1
          val description = session.sparkContext.getLocalProperty("spark.job.description")
          assertTrue(
            description.contains("paimon.db.failed_policy | retained_min=2 | retained_max=3"))
          assertFalse(description.contains("test-secret-value"))
          assertTrue(sql.contains("test-secret-value"))
          throw new IllegalStateException("procedure failed")
        }
        SparkPaimonSqlRunner.run(session, sql)
      }
    }
    spark.sparkContext.setJobDescription("outer job")
    assertThrows(
      classOf[RuntimeException],
      () => expire("failed_policy", runner, "--procedureOptions", "password=test-secret-value"))
    assertEquals(2, calls)
    assertEquals(6, snapshots("failed_policy"))
    assertEquals("outer job", spark.sparkContext.getLocalProperty("spark.job.description"))
  }

  @Test
  def legacyCutoffUsesSparkSessionTimeZone(): Unit = {
    createTable("timezone_policy", ",'snapshot.time-retained'='24 h'")
    spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
    val result =
      expire("timezone_policy", SparkPaimonSqlRunner, "--retainMin", "1", "--retainMax", "6")
    assertEquals(ActionStatus.Success, result.status)
    assertEquals(
      6,
      snapshots("timezone_policy"),
      "Recent snapshots must survive the 24-hour cutoff")
  }

  @Test
  def missingTimePolicyPreservesTheNativeProcedureDefaultForOldSnapshots(): Unit = {
    createTable("old_snapshots", "")
    val paths = Files.walk(directory.resolve("paimon"))
    var agedCount = 0
    try {
      paths.iterator().asScala.filter(_.getFileName.toString.matches("snapshot-\\d+")).foreach {
        path =>
          val json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
          val aged = json.replaceAll("\"timeMillis\"\\s*:\\s*\\d+", "\"timeMillis\":0")
          assertNotEquals(json, aged)
          Files.write(path, aged.getBytes(StandardCharsets.UTF_8))
          agedCount += 1
      }
    } finally paths.close()
    assertEquals(6, agedCount)
    // Directly changing test metadata must invalidate 1.3's snapshot cache. 0.9 has no such cache.
    val catalog = spark.sessionState.catalogManager.catalog("paimon").asInstanceOf[TableCatalog]
    val loaded = catalog.loadTable(Identifier.of(Array("db"), "old_snapshots"))
      .asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable].snapshotManager()
    loaded.getClass.getMethods.find(_.getName == "invalidateCache").foreach(_.invoke(loaded))
    val supportsOptions = SparkPaimonSqlRunner.expireSnapshotsSupportsOptions(spark, "paimon")
    val result = expire(
      "old_snapshots",
      SparkPaimonSqlRunner,
      "--retainMin",
      "1",
      "--retainMax",
      "2",
      "--procedureOptions",
      "")
    assertEquals(ActionStatus.Success, result.status)
    assertEquals(if (supportsOptions) 1 else 2, snapshots("old_snapshots"))
    if (!supportsOptions) assertEquals(Long.MaxValue.toString, result.metrics("timeRetainedMs"))
  }

  @Test
  def legacyTimestampPreservesTheExactInstantAcrossTimeZonesAndDaylightSavingOverlap(): Unit = {
    val cutoff = Instant.parse("2020-11-01T06:30:00Z")
    Seq("America/New_York", "+8:00", "+08:0", "UTC").foreach { zone =>
      spark.conf.set("spark.sql.session.timeZone", zone)
      val config = PaimonExpireSnapshotsConfig(
        20,
        10,
        550L,
        "",
        Some(Duration.ofMillis(System.currentTimeMillis() - cutoff.toEpochMilli)))
      val sql = PaimonExpireSnapshotsAction.buildExpireSnapshotsSql(
        "paimon",
        config,
        TargetTable("db.test", "db", "test"),
        supportsOptions = false)
      val literal = sql.split("older_than => ")(1).split("\n")(0).trim
      val parsed = spark.sql(s"SELECT unix_timestamp($literal)").head().getLong(0) * 1000L
      assertTrue(Math.abs(parsed - cutoff.toEpochMilli) < 1000L, s"$zone: $literal -> $parsed")
    }
  }

  @Test
  def runtimeProcedureInspectionRestoresThePreviouslyActiveSession(): Unit = {
    val other = spark.newSession()
    SparkSession.setActiveSession(other)
    val supported = SparkPaimonSqlRunner.expireSnapshotsSupportsOptions(spark, "paimon")
    assertEquals(Some(other), SparkSession.getActiveSession)
    assertEquals(spark.version.startsWith("3.5"), supported)
  }

  @Test
  def nativeExpirationMatchesSqlAndRetainsReadableData(): Unit = {
    org.junit.jupiter.api.Assumptions.assumeTrue(
      SparkPaimonSqlRunner.expireSnapshotsSupportsOptions(spark, "paimon"))
    val properties = Map(
      "snapshot.num-retained.min" -> "3",
      "snapshot.num-retained.max" -> "4",
      "snapshot.time-retained" -> "24 h",
      "snapshot.expire.limit" -> "1")
    val ddl = properties.map { case (k, v) => s",'$k'='$v'" }.mkString
    createTable("native_policy", ddl)
    createTable("sql_policy", ddl)
    val before = data("native_policy")
    val policy = PaimonSnapshotRetention.resolveConfig(
      PaimonExpireSnapshotsConfig(20, 10, 550, "file-operation.thread-num=2"),
      properties)
    val loaded = spark.sessionState.catalogManager.catalog("paimon")
      .asInstanceOf[TableCatalog].loadTable(Identifier.of(Array("db"), "native_policy"))
      .asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
    val native = PaimonSnapshotNativeExpire.create(loaded, policy)
    assertEquals(1, native.expire())
    assertEquals(1, native.expire())
    assertEquals(0, native.expire())
    val sql = expire("sql_policy", SparkPaimonSqlRunner)
    assertEquals("2", sql.metrics("deletedSnapshotsTotal"))
    assertEquals(snapshots("sql_policy"), snapshots("native_policy"))
    assertEquals(before, data("native_policy"))
  }

  private def createTable(name: String, options: String): Unit = {
    spark.sql(s"CREATE TABLE paimon.db.$name (id INT) USING paimon " +
      s"TBLPROPERTIES ('bucket'='-1','write-only'='true'$options)")
    (1 to 6).foreach { id => spark.sql(s"INSERT INTO paimon.db.$name VALUES ($id)") }
    assertEquals(6, snapshots(name))
  }

  private def expire(name: String, runner: PaimonSqlRunner, args: String*): ActionResult = {
    val common = CommonProducerConfig(
      "paimon",
      "expire-snapshots",
      "paimon",
      Some("db"),
      Some(name),
      None,
      2,
      true)
    val action = new PaimonExpireSnapshotsAction(runner)
    val config = action.parse(ProducerCliParser.parseMerged(action.options, args.toArray))
    action.execute(
      ProducerContext(spark, common, PaimonFormatAdapter),
      TargetTable(s"db.$name", "db", name),
      config)
  }

  private def snapshots(name: String): Int =
    spark.sql(s"SELECT snapshot_id FROM paimon.db.`${name}$$snapshots`").collect().length

  private def data(name: String): Seq[Row] =
    spark.sql(s"SELECT * FROM paimon.db.$name ORDER BY id").collect().toSeq
}
