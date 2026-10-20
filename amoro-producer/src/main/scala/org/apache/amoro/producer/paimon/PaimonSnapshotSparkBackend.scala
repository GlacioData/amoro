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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.paimon.utils.ParameterUtils
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import org.apache.amoro.producer.core.{ActionResult, ProducerContext, TargetTable}

/** Driver-only adapter. No method closure capturing this class is sent to an Executor. */
final private[paimon] class PaimonSnapshotSparkBackend(
    context: ProducerContext,
    requested: PaimonExpireSnapshotsConfig,
    runner: PaimonSqlRunner) extends PaimonSnapshotBackend {
  private val spark = context.spark
  private val sc = spark.sparkContext
  private val submissions = new PaimonSnapshotSubmission()
  private val observer = new PaimonSnapshotObservability()
  private val loader = Thread.currentThread().getContextClassLoader
  sc.addSparkListener(observer)

  override def available: Boolean = !sc.isStopped

  override def prepare(target: TargetTable, runId: String, workId: String): PaimonSnapshotWork = {
    withContext(workId + ":preparation", s"Prepare snapshot expiration: ${target.raw}") {
      var attempt = 1
      var prepared: Option[PaimonSnapshotWork] = None
      while (prepared.isEmpty) {
        if (Thread.currentThread().isInterrupted) throw new InterruptedException()
        try {
          val properties = runner.readExplicitProperties(spark, context.common.catalogName, target)
          val table = runner.loadFileStoreTable(spark, context.common.catalogName, target)
          val policy = PaimonSnapshotRetention.resolveConfig(requested, properties)
          val options =
            ParameterUtils.parseCommaSeparatedKeyValues(requested.procedureOptions).asScala
          val sources = Seq(
            "retainMin" -> "snapshot.num-retained.min",
            "retainMax" -> "snapshot.num-retained.max",
            "maxDeletes" -> "snapshot.expire.limit",
            "timeRetained" -> "snapshot.time-retained")
            .map { case (name, key) =>
              val source = if (properties.contains(key)) "table"
              else if (name == "timeRetained") {
                if (options.contains(key)) "procedureOptions" else "native-default"
              } else "cli-or-default"
              s"${name}Source" -> source
            }.toMap
          val work = PaimonSnapshotWork(
            runId,
            workId,
            target,
            table,
            policy,
            PaimonSnapshotWork.physicalKey(table),
            table.fileIO().getClass.getName,
            PaimonSnapshotWork.fingerprint(policy),
            sources)
          work.validate()
          prepared = Some(work)
        } catch {
          case NonFatal(error) =>
            if (attempt >= context.common.retryTimes || !available) throw error
            attempt += 1
        }
      }
      prepared.get.copy(policySources =
        prepared.get.policySources + ("preparationAttempts" -> attempt.toString))
    }
  }

  override def execute(work: PaimonSnapshotWork): ActionResult = {
    val policy = work.policy
    val description =
      s"Expire snapshots: ${context.common.catalogName}.${work.target.database}.${work.target.table} " +
        s"| retained_min=${policy.retainMin} | retained_max=${policy.retainMax} " +
        s"| retained_time=${policy.timeRetained.get.toMillis}ms | max_deletes=${policy.maxDeletes}"
    withContext(work.workId + ":cleanup", description) {
      if (Thread.currentThread().isInterrupted) throw new InterruptedException()
      val group = work.workId + ":cleanup"
      observer.track(group)
      try {
        // One table, one partition and exactly one action. Do not count/cache/recompute this RDD.
        // Spark supports concurrent actions submitted by different Driver threads:
        // https://github.com/apache/spark/blob/v3.5.4/docs/job-scheduling.md#scheduling-within-an-application
        val action = submissions.submit(work.workId) {
          sc.parallelize(Seq(work), 1)
            .mapPartitions(input => PaimonSnapshotTask.runPartition(input)).collectAsync()
        }
        val result = action.get()
        require(result.length == 1, "Snapshot job must return exactly one table result")
        result.head.copy(metrics = result.head.metrics ++ observer.take(group))
      } finally { submissions.completed(work.workId); observer.take(group) }
    }
  }

  override def cancel(workId: String): Unit = {
    // Cancellation is best effort and does not establish that native FileIO has stopped.
    // Treat handles and groups as one interrupt-safe sequence, including late read-only Jobs.
    PaimonSnapshotCancellation.run(Seq(
      () => submissions.cancel(workId),
      () => if (available) sc.cancelJobGroup(workId + ":preparation"),
      () => if (available) sc.cancelJobGroup(workId + ":cleanup")))
  }

  override def close(): Unit = {
    try submissions.close()
    finally sc.removeSparkListener(observer)
  }

  private def withContext[T](group: String, description: String)(body: => T): T = {
    val thread = Thread.currentThread()
    val previousLoader = thread.getContextClassLoader
    val previousSession = SparkSession.getActiveSession
    val keys = Seq("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
    val previousProperties = keys.map(key => key -> sc.getLocalProperty(key))
    thread.setContextClassLoader(loader)
    SparkSession.setActiveSession(spark)
    // Restore the submitting thread's context; thread pools inherit local properties unpredictably.
    // https://github.com/apache/spark/blob/v3.5.4/core/src/main/scala/org/apache/spark/SparkContext.scala
    sc.setJobGroup(group, description, interruptOnCancel = true)
    try SQLConf.withExistingConf(spark.sessionState.conf)(body)
    finally {
      previousProperties.foreach { case (key, value) => sc.setLocalProperty(key, value) }
      previousSession match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
      thread.setContextClassLoader(previousLoader)
    }
  }
}
