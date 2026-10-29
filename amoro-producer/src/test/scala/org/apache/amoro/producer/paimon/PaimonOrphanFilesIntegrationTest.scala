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

import java.io.{ByteArrayOutputStream, IOException, PrintStream}
import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.paimon.table.FileStoreTable
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskStart}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.io.TempDir

import org.apache.amoro.producer.core._

class PaimonOrphanFilesIntegrationTest {
  @TempDir var directory: Path = _
  private var spark: SparkSession = _
  private val target = TargetTable("db.a", "db", "a")
  private def config: PaimonRemoveOrphanFilesConfig =
    PaimonRemoveOrphanFilesConfig(2, "distributed", 2, true)
  private def common: CommonProducerConfig =
    CommonProducerConfig("paimon", "remove-orphan-files", "paimon", Some("db"), None, None, 3, true)
  private def context: ProducerContext = ProducerContext(spark, common, PaimonFormatAdapter)

  @BeforeEach def start(): Unit = {
    assumeTrue(
      PaimonOrphanFilesRuntime.supported,
      "Modern orphan scheduling only supports Spark 3.5.4 / Paimon 1.3.2")
    spark = session("local[4]")
  }
  @AfterEach def stop(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    Thread.interrupted()
  }

  @Test def parallelNativeProceduresDeleteOnlyOldOrphansInIndependentExecutors(): Unit = {
    assumeTrue(
      java.lang.Boolean.getBoolean("producer.orphan.clusterTest"),
      "Opt-in local-cluster test")
    val old = Seq("a", "b").map(name => name -> create(name)).toMap
    val fresh = old.map { case (name, path) =>
      val file = path.resolveSibling(s"fresh-$name.bin")
      Files.write(file, Array[Byte](4, 5))
      name -> file
    }
    // The native dry-run gives a read-only baseline before the actual concurrent cleanup.
    old.foreach { case (name, path) =>
      val result =
        spark.sql(s"CALL paimon.sys.remove_orphan_files(table => 'db.$name', parallelism => 2, mode => 'distributed', dry_run => true)").collect()
      assertEquals(1L, result.head.getLong(0))
      assertTrue(Files.exists(path))
    }
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark =
      session("local-cluster[2,2,1024]", serializer = "org.apache.spark.serializer.KryoSerializer")
    val both = new CountDownLatch(2)
    val observations = new Observation
    spark.sparkContext.addSparkListener(observations)
    val calls = new AtomicInteger()
    val sessions = ConcurrentHashMap.newKeySet[SparkSession]()
    val runner = new PaimonSqlRunner {
      override def run(s: SparkSession, sql: String): Seq[Row] = {
        if (!sql.trim.startsWith("CALL")) return SparkPaimonSqlRunner.run(s, sql)
        sessions.add(s)
        assertTrue(SparkSession.getActiveSession.contains(s))
        assertEquals("inherited-value", SQLConf.get.getConfString("spark.sql.orphan.test.marker"))
        assertTrue(
          s.sparkContext.getLocalProperty("spark.scheduler.pool").startsWith("producer-orphan-"))
        calls.incrementAndGet()
        both.countDown()
        assertTrue(
          both.await(10, TimeUnit.SECONDS),
          "Native procedures must be submitted by separate Driver workers")
        SparkPaimonSqlRunner.run(s, sql)
      }
    }
    spark.conf.set("spark.sql.orphan.test.marker", "inherited-value")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "caller-pool")
    spark.sparkContext.setJobDescription("caller-description")
    val action = new PaimonRemoveOrphanFilesAction(runner)
    val output = new ByteArrayOutputStream()
    val code = Console.withOut(new PrintStream(output)) {
      ProducerApp.runInternal(
        Array(
          "--format",
          "paimon",
          "--action",
          "remove-orphan-files",
          "--catalogName",
          "paimon",
          "--databaseName",
          "db",
          "--parallelism",
          "2",
          "--tableConcurrency",
          "2"),
        Seq(new PaimonFormatAdapter(runner)))
    }
    assertEquals(0, code, output.toString("UTF-8"))
    assertEquals(2, calls.get())
    assertEquals(2, sessions.size())
    assertFalse(sessions.contains(spark))
    assertFalse(spark.sparkContext.isStopped, "Borrowed SparkContext must remain running")
    assertEquals(
      "org.apache.spark.serializer.KryoSerializer",
      org.apache.spark.SparkEnv.get.serializer.getClass.getName)
    assertEquals("caller-pool", spark.sparkContext.getLocalProperty("spark.scheduler.pool"))
    assertEquals("caller-description", spark.sparkContext.getLocalProperty("spark.job.description"))
    val events = output.toString("UTF-8").split("\n").filter(_.startsWith(
      "{\"event\":\"lakehouse_producer_table_action\""))
    assertEquals(2, events.length)
    events.foreach { event =>
      val json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(event)
      assertEquals("success", json.get("status").asText(), event)
      assertEquals("1", json.get("metrics").get("deletedFileCount").asText(), event)
      assertEquals("COMPLETED", json.get("metrics").get("completionState").asText(), event)
    }
    old.foreach { case (name, path) =>
      assertFalse(Files.exists(path))
      assertTrue(Files.exists(fresh(name)))
      assertEquals(
        Seq(1, 2),
        spark.sql(s"SELECT id FROM paimon.db.$name ORDER BY id").collect().map(_.getInt(0)).toSeq)
      assertEquals(
        1,
        spark.sql(s"SELECT snapshot_id FROM paimon.db.`$name$$snapshots`").collect().length)
    }
    assertTrue(observations.executors.size() >= 2, observations.executors.toString)
    assertTrue(observations.peakGroups.get() >= 2, "Different table JobGroups must overlap")
    spark.sparkContext.removeSparkListener(observations)
  }

  @Test def preparationCanRetryButProcedureFailureNeverReplays(): Unit = {
    create("a")
    val attempts = new AtomicInteger()
    val calls = new AtomicInteger()
    val runner = new PaimonSqlRunner {
      override def loadFileStoreTable(
          s: SparkSession,
          catalog: String,
          t: TargetTable): FileStoreTable = {
        if (attempts.incrementAndGet() == 1) throw new IOException("temporary catalog failure")
        SparkPaimonSqlRunner.loadFileStoreTable(s, catalog, t)
      }
      override def run(s: SparkSession, sql: String): Seq[Row] = {
        calls.incrementAndGet()
        throw new IOException("password=secret-value", new NullPointerException("native failure"))
      }
    }
    val result = execute(runner, Seq(target)).head._2
    assertEquals(2, attempts.get())
    assertEquals(1, calls.get())
    assertEquals("UNKNOWN", result.metrics("completionState"))
    assertEquals("procedure", result.metrics("failurePhase"))
    assertFalse(result.metrics.contains("deletedFileCount"))
    assertTrue(result.message.get.contains("java.lang.NullPointerException: native failure"))
    assertFalse(result.message.get.contains("secret-value"))
  }

  @Test def failedPreparationDoesNotEnterProcedureAndMissingResultsStayUnknown(): Unit = {
    val calls = new AtomicInteger()
    val unavailable = new PaimonSqlRunner {
      override def loadFileStoreTable(
          s: SparkSession,
          catalog: String,
          t: TargetTable): FileStoreTable =
        throw new IOException("catalog unavailable")
      override def run(s: SparkSession, sql: String): Seq[Row] = {
        calls.incrementAndGet(); Seq.empty
      }
    }
    val failure = execute(unavailable, Seq(target)).head._2
    assertEquals(0, calls.get())
    assertEquals("FAILED", failure.metrics("completionState"))
    create("a")
    val empty =
      new PaimonSqlRunner { override def run(s: SparkSession, sql: String): Seq[Row] = Seq.empty }
    val unknown = execute(empty, Seq(target)).head._2
    assertEquals("UNKNOWN", unknown.metrics("completionState"))
    assertFalse(unknown.metrics.contains("deletedFileCount"))
  }

  @Test def physicalAliasesAreOnlyCleanedOnceAndThreadContextIsRestored(): Unit = {
    create("a")
    val calls = new AtomicInteger()
    val runner = new PaimonSqlRunner {
      override def loadFileStoreTable(
          s: SparkSession,
          catalog: String,
          t: TargetTable): FileStoreTable =
        SparkPaimonSqlRunner.loadFileStoreTable(s, catalog, target)
      override def run(s: SparkSession, sql: String): Seq[Row] = {
        calls.incrementAndGet(); Seq(Row(1L, 3L))
      }
    }
    val results = execute(runner, Seq(target, TargetTable("db.alias", "db", "alias")))
    assertEquals(1, calls.get())
    assertEquals(Set(ActionStatus.Success, ActionStatus.Skipped), results.map(_._2.status).toSet)
    val action = new PaimonRemoveOrphanFilesAction(runner)
    val backend = new PaimonOrphanFilesSparkBackend(context, config, runner, action)
    val sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", "original")
    sc.setJobDescription("original description")
    try {
      assertEquals(ActionStatus.Success, backend.clean(target, "direct-test", 0).status)
      assertTrue(SparkSession.getActiveSession.contains(spark))
      assertEquals("original", sc.getLocalProperty("spark.scheduler.pool"))
      assertEquals("original description", sc.getLocalProperty("spark.job.description"))
      assertFalse(sc.isStopped)
    } finally backend.close()
  }

  @Test def reusedUnsafeContextIsRejectedBeforeSelectingTables(): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = session("local[2]", safe = false)
    val adapter = new PaimonFormatAdapter() {
      override def listTables(
          s: SparkSession,
          c: CommonProducerConfig,
          d: String): Seq[TargetTable] =
        throw new AssertionError("Unsafe runtime must not select tables")
    }
    val code = ProducerApp.runInternal(
      Array(
        "--format",
        "paimon",
        "--action",
        "remove-orphan-files",
        "--databaseName",
        "db",
        "--tableConcurrency",
        "2"),
      Seq(adapter))
    assertEquals(2, code)
    assertFalse(spark.sparkContext.isStopped)
  }

  @Test def reusedFifoContextRejectsConcurrentTablesBeforeSelectionOrCleanup(): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = session("local[2]", schedulingMode = "FIFO")
    val calls = new AtomicInteger()
    val runner = new PaimonSqlRunner {
      override def run(s: SparkSession, sql: String): Seq[Row] = {
        calls.incrementAndGet()
        throw new AssertionError("FIFO context must not enter cleanup")
      }
    }
    val adapter = new PaimonFormatAdapter(runner) {
      override def listTables(
          s: SparkSession,
          c: CommonProducerConfig,
          d: String): Seq[TargetTable] =
        throw new AssertionError("FIFO context must be rejected before table selection")
    }
    val output = new ByteArrayOutputStream()
    val code = Console.withOut(new PrintStream(output)) {
      ProducerApp.runInternal(
        Array(
          "--format",
          "paimon",
          "--action",
          "remove-orphan-files",
          "--catalogName",
          "paimon",
          "--databaseName",
          "db",
          "--tableConcurrency",
          "2"),
        Seq(adapter))
    }
    assertEquals(2, code, output.toString("UTF-8"))
    assertTrue(output.toString("UTF-8").contains("requires spark.scheduler.mode=FAIR"))
    assertEquals(0, calls.get())
    assertEquals("FIFO", spark.sparkContext.getSchedulingMode.toString)
    assertFalse(spark.sparkContext.isStopped)
  }

  @Test def reusedFifoContextAllowsOneTableAndKeepsTheSharedContextRunning(): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = session("local[2]", schedulingMode = "FIFO")
    create("a")
    val calls = new AtomicInteger()
    val runner = new PaimonSqlRunner {
      override def run(s: SparkSession, sql: String): Seq[Row] = {
        calls.incrementAndGet()
        Seq(Row(0L, 0L))
      }
    }
    val code = ProducerApp.runInternal(
      Array(
        "--format",
        "paimon",
        "--action",
        "remove-orphan-files",
        "--catalogName",
        "paimon",
        "--databaseName",
        "db",
        "--tableName",
        "a",
        "--tableConcurrency",
        "1"),
      Seq(new PaimonFormatAdapter(runner)))
    assertEquals(0, code)
    assertEquals(1, calls.get())
    assertEquals("FIFO", spark.sparkContext.getSchedulingMode.toString)
    assertFalse(spark.sparkContext.isStopped)
  }

  @Test def cancellationAlsoCancelsJobsSubmittedAfterTheInitialCancel(): Unit = {
    create("a")
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val runner = new PaimonSqlRunner {
      override def run(s: SparkSession, sql: String): Seq[Row] = {
        entered.countDown()
        assertTrue(release.await(5, TimeUnit.SECONDS))
        // Simulate native Driver work submitting its first Job after cancelJobGroup returned.
        s.sparkContext.parallelize(Seq(1, 2), 2).map { value =>
          Thread.sleep(30000)
          value
        }.collect()
        Seq(Row(0L, 0L))
      }
    }
    val action = new PaimonRemoveOrphanFilesAction(runner)
    val backend = new PaimonOrphanFilesSparkBackend(context, config, runner, action)
    val driver = java.util.concurrent.Executors.newSingleThreadExecutor()
    val future = driver.submit(new java.util.concurrent.Callable[ActionResult] {
      override def call(): ActionResult = backend.clean(target, "late-job", 0)
    })
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS))
      assertTrue(backend.cancel("late-job"))
      backend.close()
      release.countDown()
      val result = future.get(10, TimeUnit.SECONDS)
      assertEquals(ActionStatus.Failed, result.status)
      assertEquals("UNKNOWN", result.metrics("completionState"))
      assertFalse(result.metrics.contains("deletedFileCount"))
    } finally { release.countDown(); backend.close(); driver.shutdownNow() }
  }

  private def execute(
      runner: PaimonSqlRunner,
      targets: Seq[TargetTable]): Seq[(TargetTable, ActionResult)] = {
    val action = new PaimonRemoveOrphanFilesAction(runner)
    ProducerApp.executeTables(context, action, config, targets)
  }

  private def create(name: String): Path = {
    spark.sql(s"CREATE TABLE paimon.db.$name (id INT) USING paimon TBLPROPERTIES ('bucket'='-1','write-only'='true')")
    spark.sql(s"INSERT INTO paimon.db.$name VALUES (1),(2)")
    val path = directory.resolve(s"paimon/db.db/$name")
    val walk = Files.walk(path)
    val bucket =
      try walk.iterator().asScala.find(p =>
          Files.isDirectory(p) && p.getFileName.toString.startsWith("bucket-")).get
      finally walk.close()
    val orphan = bucket.resolve(s"old-orphan-$name.bin")
    Files.write(orphan, Array[Byte](1, 2, 3))
    Files.setLastModifiedTime(
      orphan,
      FileTime.fromMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2)))
    orphan
  }

  private def session(
      master: String,
      safe: Boolean = true,
      serializer: String = "org.apache.spark.serializer.JavaSerializer",
      schedulingMode: String = "FAIR"): SparkSession = {
    val builder = SparkSession.builder().master(master).appName("orphan-test")
      .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1").config(
        "spark.driver.bindAddress",
        "127.0.0.1")
      .config("spark.task.maxFailures", if (safe) "1" else "4").config("spark.speculation", "false")
      .config("spark.serializer", serializer)
      .config("spark.scheduler.mode", schedulingMode).config("spark.sql.shuffle.partitions", "2")
      .config("spark.executor.memory", "512m").config("spark.executor.cores", "2")
      .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.paimon.warehouse", directory.resolve("paimon").toUri.toString)
      .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
    if (master.startsWith("local-cluster")) builder.config(
      "spark.executor.extraClassPath",
      System.getProperty("surefire.test.class.path"))
    val result = builder.getOrCreate()
    result.sparkContext.setLogLevel("WARN")
    result.sql("CREATE DATABASE IF NOT EXISTS paimon.db")
    result
  }

  private class Observation extends SparkListener {
    val executors = ConcurrentHashMap.newKeySet[String]()
    val peakGroups = new AtomicInteger()
    private val groups = scala.collection.mutable.Map.empty[Int, String]
    override def onJobStart(event: SparkListenerJobStart): Unit = {
      Option(event.properties.getProperty("spark.jobGroup.id")).foreach { group =>
        groups.put(event.jobId, group)
        peakGroups.accumulateAndGet(
          groups.values.toSet.size,
          new java.util.function.IntBinaryOperator {
            override def applyAsInt(a: Int, b: Int): Int = math.max(a, b)
          })
      }
    }
    override def onJobEnd(event: SparkListenerJobEnd): Unit = { groups.remove(event.jobId); () }
    override def onTaskStart(event: SparkListenerTaskStart): Unit = {
      if (event.taskInfo.executorId != "driver") executors.add(event.taskInfo.executorId)
    }
  }
}
