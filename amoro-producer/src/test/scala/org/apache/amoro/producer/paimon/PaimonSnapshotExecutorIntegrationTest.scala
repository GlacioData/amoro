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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.locks.LockSupport

import org.apache.paimon.consumer.{Consumer, ConsumerManager}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.io.TempDir

import org.apache.amoro.producer.core._

class PaimonSnapshotExecutorIntegrationTest {
  @TempDir var directory: Path = _
  private var spark: SparkSession = _

  @BeforeEach def start(): Unit = {
    assumeTrue(org.apache.spark.SPARK_VERSION == "3.5.4", "Executor mode only supports Spark 3.5.4")
    spark = session("local[4]")
  }

  @AfterEach def stop(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  @Test def cliBatchPreservesPerTablePolicyAndSameAttemptAcrossRounds(): Unit = {
    create("a", 4)
    create("b", 3)
    val action = new PaimonExpireSnapshotsAction()
    val config = action.parse(ProducerCliParser.parseMerged(
      action.options,
      Array("--executionMode", "executor", "--tableConcurrency", "2")))
    val context = ProducerContext(spark, common, PaimonFormatAdapter)
    val before = Seq("a", "b").map(name => name -> data(name)).toMap
    spark.sparkContext.setJobDescription("outer description")
    val results = ProducerApp.executeTables(context, action, config, Seq(target("a"), target("b")))
    assertEquals(2, results.size)
    assertTrue(results.forall(_._2.status == ActionStatus.Success), results.map(_._2).toString)
    val byName = results.map { case (table, result) => table.table -> result }.toMap
    assertEquals("2", byName("a").metrics("deletedSnapshotsTotal"))
    assertEquals("3", byName("b").metrics("deletedSnapshotsTotal"))
    assertEquals(3L, byName("a").taskTotals.get.success)
    assertEquals(4L, byName("b").taskTotals.get.success)
    assertEquals(2, results.map(_._2.metrics("taskAttemptId")).distinct.size)
    results.foreach { case (table, result) =>
      assertEquals("1", result.metrics("maxDeletes"))
      assertEquals("table", result.metrics("maxDeletesSource"))
      assertEquals("0", result.metrics("taskAttemptNumber"))
      assertEquals(before(table.table), data(table.table))
      assertEquals(if (table.table == "a") 4 else 3, snapshotCount(table.table))
    }
    assertEquals("outer description", spark.sparkContext.getLocalProperty("spark.job.description"))
  }

  @Test def realTableAndFileIOSurviveSerializationWithoutDriverContext(): Unit = {
    create("serialized", 4)
    val backend = new PaimonSnapshotSparkBackend(
      ProducerContext(spark, common, PaimonFormatAdapter),
      PaimonExpireSnapshotsConfig(20, 10, 550, ""),
      SparkPaimonSqlRunner)
    try {
      val work = backend.prepare(target("serialized"), "run", "work")
      val bytes = new ByteArrayOutputStream()
      val output = new ObjectOutputStream(bytes)
      output.writeObject(work)
      output.close()
      val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
      val copy =
        try input.readObject().asInstanceOf[PaimonSnapshotWork]
        finally input.close()
      copy.validate()
      assertEquals(work.physicalKey, copy.physicalKey)
      assertEquals(work.fileIOClass, copy.fileIOClass)
      assertFalse(copy.toString.contains("file-operation"))
      assertThrows(
        classOf[IllegalArgumentException],
        () => copy.copy(physicalKey = "file:/wrong").validate())
      assertThrows(
        classOf[IllegalArgumentException],
        () => copy.copy(policyFingerprint = "wrong").validate())
      val result = backend.execute(copy)
      assertEquals(ActionStatus.Success, result.status, result.toString)
      assertEquals(4, snapshotCount("serialized"))
    } finally backend.close()
  }

  @Test def twoTasksOverlapInOneExecutorAndKeepTheirOwnPolicies(): Unit = {
    create("a", 4)
    create("b", 3)
    SnapshotTaskBarrier.reset()
    val delegate = new PaimonSnapshotSparkBackend(
      ProducerContext(spark, common, PaimonFormatAdapter),
      PaimonExpireSnapshotsConfig(20, 10, 550, ""),
      SparkPaimonSqlRunner)
    val sessionContext = spark.sparkContext
    val backend = new PaimonSnapshotBackend {
      override def prepare(t: TargetTable, r: String, w: String): PaimonSnapshotWork =
        delegate.prepare(t, r, w)
      override def execute(work: PaimonSnapshotWork): ActionResult = {
        sessionContext.parallelize(Seq(work), 1).mapPartitions { input =>
          SnapshotTaskBarrier.arrive()
          PaimonSnapshotTask.runPartition(input)
        }.collect().head
      }
      override def available: Boolean = delegate.available
      override def cancel(id: String): Unit = delegate.cancel(id)
      override def close(): Unit = delegate.close()
    }
    val results = new PaimonSnapshotExecutor(2, true, backend).execute(
      Seq(target("a"), target("b")),
      (_, _) => ())
    assertTrue(results.forall(_._2.status == ActionStatus.Success), results.toString)
    assertEquals(1, results.map(_._2.metrics("executorId")).distinct.size)
    assertEquals(2, results.map(_._2.metrics("taskAttemptId")).distinct.size)
    assertEquals(4, snapshotCount("a"))
    assertEquals(3, snapshotCount("b"))
  }

  @Test def consumerAndTagProtectOldDataAcrossOverwriteSnapshots(): Unit = {
    spark.sql("CREATE TABLE paimon.db.protected_table (id INT) USING paimon " +
      "TBLPROPERTIES ('bucket'='-1','write-only'='true','snapshot.num-retained.min'='1'," +
      "'snapshot.num-retained.max'='2','snapshot.time-retained'='0 ms','snapshot.expire.limit'='1')")
    spark.sql("INSERT INTO paimon.db.protected_table VALUES (1)")
    val table = SparkPaimonSqlRunner.loadFileStoreTable(spark, "paimon", target("protected_table"))
    table.createTag("kept", 1L)
    (2 to 6).foreach(i => spark.sql(s"INSERT OVERWRITE paimon.db.protected_table VALUES ($i)"))
    new ConsumerManager(table.fileIO(), table.location()).resetConsumer("reader", new Consumer(3L))
    val action = new PaimonExpireSnapshotsAction()
    val config = action.parse(ProducerCliParser.parseMerged(
      action.options,
      Array("--executionMode", "executor")))
    val result = ProducerApp.executeTables(
      ProducerContext(spark, common, PaimonFormatAdapter),
      action,
      config,
      Seq(target("protected_table"))).head._2
    assertEquals(ActionStatus.Success, result.status, result.toString)
    assertEquals(4, snapshotCount("protected_table"))
    // Official time travel syntax: https://paimon.apache.org/docs/1.3/spark/sql-query/#batch-time-travel
    assertEquals(
      1,
      spark.sql("SELECT id FROM paimon.db.protected_table VERSION AS OF 'kept'").head().getInt(0))
    assertEquals(
      3,
      spark.sql("SELECT id FROM paimon.db.protected_table VERSION AS OF 3").head().getInt(0))
    assertEquals(6, spark.sql("SELECT id FROM paimon.db.protected_table").head().getInt(0))
  }

  @Test def compactedPrimaryKeyAndChangelogTablesMatchDriverExpiration(): Unit = {
    Seq("pk_driver", "pk_executor").foreach { name =>
      spark.sql(s"CREATE TABLE paimon.db.$name (id INT, value INT) USING paimon " +
        "TBLPROPERTIES ('primary-key'='id','bucket'='1','write-only'='true'," +
        "'changelog-producer'='input','changelog.num-retained.max'='10'," +
        "'snapshot.num-retained.min'='1','snapshot.num-retained.max'='3'," +
        "'snapshot.time-retained'='24 h','snapshot.expire.limit'='1')")
      (1 to 6).foreach(i => spark.sql(s"INSERT INTO paimon.db.$name VALUES (1, $i), (2, $i)"))
      spark.sql(s"CALL paimon.sys.compact(table => 'db.$name')")
    }
    val action = new PaimonExpireSnapshotsAction()
    val context = ProducerContext(spark, common, PaimonFormatAdapter)
    val driver = action.execute(
      context,
      target("pk_driver"),
      action.parse(
        ProducerCliParser.parseMerged(action.options, Array.empty[String])))
    val executor = ProducerApp.executeTables(
      context,
      action,
      action.parse(
        ProducerCliParser.parseMerged(action.options, Array("--executionMode", "executor"))),
      Seq(target("pk_executor"))).head._2
    assertEquals(ActionStatus.Success, driver.status, driver.toString)
    assertEquals(ActionStatus.Success, executor.status, executor.toString)
    assertEquals(driver.metrics("deletedSnapshotsTotal"), executor.metrics("deletedSnapshotsTotal"))
    def rows(name: String) = spark.sql(s"SELECT * FROM paimon.db.$name ORDER BY id").collect().toSeq
    assertEquals(rows("pk_driver"), rows("pk_executor"))
    assertEquals(Seq(6, 6), rows("pk_executor").map(_.getInt(1)))
    def snapshots(name: String) = spark.sql(
      s"SELECT snapshot_id FROM paimon.db.`$name$$snapshots` ORDER BY snapshot_id").collect().toSeq
    assertEquals(snapshots("pk_driver"), snapshots("pk_executor"))
    // Preserve the procedure's changelog behavior; do not override changelog settings when
    // building the native snapshot ExpireConfig.
    def changelogFiles(name: String): Long = {
      val root = directory.resolve("paimon/db.db").resolve(name)
      val files = Files.walk(root)
      try files.filter(path => path.getFileName.toString.startsWith("changelog-")).count()
      finally files.close()
    }
    assertEquals(changelogFiles("pk_driver"), changelogFiles("pk_executor"))
  }

  @Test def actualSparkReplayDoesNotReenterNativeCleanupAfterResultLoss(): Unit = {
    spark.stop()
    // Test harness only: local[N,maxFailures] overrides the scheduler's retry limit so a real
    // retry can be injected while the Task still sees the production maxFailures=1 setting.
    spark = session("local[2,2]")
    create("replay", 4)
    val backend = new PaimonSnapshotSparkBackend(
      ProducerContext(spark, common, PaimonFormatAdapter),
      PaimonExpireSnapshotsConfig(20, 10, 550, ""),
      SparkPaimonSqlRunner)
    try {
      val work = backend.prepare(target("replay"), "replay-run", "replay-work")
      val result = spark.sparkContext.parallelize(Seq(work), 1).mapPartitions { input =>
        val output = PaimonSnapshotTask.runPartition(input).toVector
        if (TaskContext.get().attemptNumber() == 0) {
          require(output.head.status == ActionStatus.Success)
          throw new java.io.IOException("Injected loss of completed cleanup result")
        }
        output.iterator
      }.collect().head
      assertEquals(ActionStatus.Failed, result.status)
      assertEquals("1", result.metrics("taskAttemptNumber"))
      assertEquals("replay", result.metrics("failurePhase"))
      assertEquals("UNKNOWN", result.metrics("completionState"))
      assertFalse(result.metrics.contains("deletedSnapshotsTotal"))
      assertEquals(4, snapshotCount("replay"))
      assertEquals(Seq(1, 2, 3, 4, 5, 6), data("replay"))
    } finally backend.close()
  }

  @Test def multiJvmTasksExecuteOnTwoExecutors(): Unit = {
    assumeTrue(
      java.lang.Boolean.getBoolean("producer.snapshot.clusterTest"),
      "Opt-in local-cluster test")
    spark.stop()
    spark = session("local-cluster[2,2,1024]")
    (0 until 4).foreach(i => create(s"cluster_$i", 4))
    val barrierPath = directory.resolve("barrier")
    Files.createDirectory(barrierPath)
    val barrier = barrierPath.toString
    val sc = spark.sparkContext
    val delegate = new PaimonSnapshotSparkBackend(
      ProducerContext(spark, common, PaimonFormatAdapter),
      PaimonExpireSnapshotsConfig(20, 10, 550, ""),
      SparkPaimonSqlRunner)
    val backend = new PaimonSnapshotBackend {
      override def prepare(t: TargetTable, r: String, w: String): PaimonSnapshotWork =
        delegate.prepare(t, r, w)
      override def execute(work: PaimonSnapshotWork): ActionResult = sc.parallelize(Seq(work), 1)
        .mapPartitions(SnapshotTaskBarrier.afterProcessBarrier(barrier, 4)).collect().head
      override def available: Boolean = delegate.available
      override def cancel(id: String): Unit = delegate.cancel(id)
      override def close(): Unit = delegate.close()
    }
    val results = new PaimonSnapshotExecutor(4, true, backend).execute(
      (0 until 4).map(i => target(s"cluster_$i")),
      (_, _) => ())
    assertTrue(results.forall(_._2.status == ActionStatus.Success), results.toString)
    assertFalse(results.exists(_._2.metrics("executorId") == "driver"))
    assertEquals(2, results.map(_._2.metrics("executorId")).distinct.size)
    assertEquals(4, results.map(_._2.metrics("taskAttemptId")).distinct.size)
    results.foreach { case (table, result) =>
      assertEquals(3L, result.taskTotals.get.success)
      assertEquals(4, snapshotCount(table.table))
    }
  }

  private def session(master: String): SparkSession = {
    val builder = SparkSession.builder().master(master).appName("producer-snapshot-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.speculation", "false").config("spark.task.maxFailures", "1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.executor.memory", "512m").config("spark.executor.cores", "2")
      .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.paimon.type", "filesystem")
      .config("spark.sql.catalog.paimon.warehouse", directory.resolve("paimon").toUri.toString)
      .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
    if (master.startsWith("local-cluster")) {
      builder.config(
        "spark.executor.extraClassPath",
        System.getProperty("surefire.test.class.path"))
    }
    val session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    session.sql("CREATE DATABASE IF NOT EXISTS paimon.db")
    session
  }

  private def create(name: String, retain: Int): Unit = {
    spark.sql(s"CREATE TABLE paimon.db.$name (id INT) USING paimon " +
      s"TBLPROPERTIES ('bucket'='-1','write-only'='true','snapshot.num-retained.min'='1'," +
      s"'snapshot.num-retained.max'='$retain','snapshot.time-retained'='24 h','snapshot.expire.limit'='1')")
    (1 to 6).foreach(i => spark.sql(s"INSERT INTO paimon.db.$name VALUES ($i)"))
  }
  private def target(name: String): TargetTable = TargetTable(s"db.$name", "db", name)
  private def data(name: String): Seq[Int] =
    spark.sql(s"SELECT id FROM paimon.db.$name ORDER BY id")
      .collect().map(_.getInt(0)).toSeq
  private def snapshotCount(name: String): Int =
    spark.sql(s"SELECT snapshot_id FROM paimon.db.`${name}$$snapshots`").collect().length
  private def common: CommonProducerConfig =
    CommonProducerConfig("paimon", "expire-snapshots", "paimon", Some("db"), None, None, 3, true)
}

/** Test-only coordination; no state directory or coordination service exists in production code. */
private[paimon] object SnapshotTaskBarrier {
  @volatile private var arrivals = new CountDownLatch(2)
  def reset(): Unit = { arrivals = new CountDownLatch(2) }
  def arrive(): Unit = {
    arrivals.countDown()
    require(arrivals.await(20, TimeUnit.SECONDS), "Two tasks did not overlap")
  }
  def afterProcessBarrier(
      directory: String,
      parties: Int): Iterator[PaimonSnapshotWork] => Iterator[ActionResult] = input => {
    awaitProcesses(directory, parties)
    PaimonSnapshotTask.runPartition(input)
  }
  def awaitProcesses(directory: String, parties: Int): Unit = {
    val path = Paths.get(directory)
    Files.createFile(path.resolve(TaskContext.get().taskAttemptId().toString))
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    var ready = false
    while (!ready && System.nanoTime() < deadline) {
      val files = Files.list(path)
      try ready = files.count() == parties
      finally files.close()
      if (!ready) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10))
    }
    require(ready, "Independent Executor tasks did not overlap")
  }
}
