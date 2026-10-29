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
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import org.apache.amoro.producer.core.{ActionResult, ActionStatus, ProducerContext, TargetTable}

/** Each slot owns a session, while native distributed procedures share the application's executors. */
final private[paimon] class PaimonOrphanFilesSparkBackend(
    context: ProducerContext,
    config: PaimonRemoveOrphanFilesConfig,
    runner: PaimonSqlRunner,
    action: PaimonRemoveOrphanFilesAction) extends PaimonOrphanFilesBackend {
  private val spark = context.spark
  private val sc = spark.sparkContext
  private val loader = Thread.currentThread().getContextClassLoader
  private val sessions = new Array[SparkSession](config.tableConcurrency)
  private val entries = mutable.Map.empty[String, Entry]
  private val cancelled = mutable.Set.empty[String]
  private val seen = mutable.Map.empty[String, String]
  private var closing = false
  private var detached = false
  private val listener = new SparkListener {
    override def onJobStart(event: SparkListenerJobStart): Unit = {
      val group = Option(event.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id")))
      // cancelJobGroup only cancels current jobs; catch jobs submitted after cancellation too.
      if (group.exists(id => PaimonOrphanFilesSparkBackend.this.synchronized(cancelled(id))))
        sc.cancelJob(event.jobId)
    }
  }
  sc.addSparkListener(listener)

  override val sensitiveValues: Seq[String] = PaimonSnapshotErrors.sensitiveValues(
    sc.getConf.getAll.toSeq ++ sc.hadoopConfiguration.asScala.map(entry =>
      entry.getKey -> entry.getValue).toSeq)
  override def available: Boolean = !sc.isStopped

  override def clean(target: TargetTable, workId: String, slot: Int): ActionResult = {
    val entry = new Entry
    var secrets = sensitiveValues
    var fingerprint = ""
    val start = System.nanoTime()
    val pool = s"producer-orphan-$slot"
    def metrics: Map[String, String] = Map(
      "effectiveVersion" -> "1.3.2",
      "parallelism" -> config.parallelism.toString,
      "schedulerPool" -> pool,
      "jobGroup" -> workId,
      "physicalTableFingerprint" -> fingerprint,
      "executionMs" -> ((System.nanoTime() - start) / 1e6).toString)
    def failed(error: Throwable): ActionResult = {
      val entered = synchronized(entry.entered)
      val phase = if (entered) "procedure" else "preparation"
      val result = PaimonOrphanFilesExecutor.failure(
        phase,
        PaimonSnapshotErrors.format(
          s"Orphan cleaning failed (phase=$phase); no replay",
          error,
          secrets),
        entered)
      result.copy(metrics = result.metrics ++ metrics)
    }
    try {
      synchronized {
        if (closing || cancelled(workId))
          throw new InterruptedException("Cleanup cancelled before preparation")
        entries.put(workId, entry)
      }
      withSession(slot, workId, pool, target) { session =>
        var attempt = 0
        var table: org.apache.paimon.table.FileStoreTable = null
        while (table == null) {
          if (Thread.currentThread().isInterrupted)
            throw new InterruptedException("Preparation interrupted")
          attempt += 1
          try {
            val loaded = runner.loadFileStoreTable(session, context.common.catalogName, target)
            require(loaded != null, "Missing Paimon table")
            table = loaded
          } catch { case NonFatal(error) => if (attempt >= context.common.retryTimes) throw error }
        }
        val physical = PaimonSnapshotWork.physicalKey(table)
        fingerprint = PaimonSnapshotWork.digest(physical)
        secrets =
          (secrets ++ PaimonSnapshotErrors.sensitiveValues(table.options().asScala)).distinct
        val primary = synchronized {
          seen.get(physical) match {
            case existing @ Some(_) => existing
            case None => seen.put(physical, workId); None
          }
        }
        primary match {
          case Some(id) =>
            ActionResult(
              ActionStatus.Skipped,
              Seq.empty,
              Some("Duplicate physical table; no second cleanup submitted"),
              metrics ++ Map(
                "duplicateTarget" -> "true",
                "primaryWorkId" -> id,
                "completionState" -> "SKIPPED",
                "countsComplete" -> "true"),
              0)
          case None =>
            synchronized {
              if (closing || cancelled(workId) || Thread.currentThread().isInterrupted)
                throw new InterruptedException("Cleanup cancelled before procedure")
              entry.entered = true
            }
            // The native procedure can already delete files on the Driver before it submits jobs.
            // Never retry the CALL, including failures while constructing its distributed plan.
            val result =
              action.executeOnce(context.copy(spark = session), target, config, strict = true)
            result.copy(metrics = result.metrics ++ metrics ++ Map(
              "completionState" -> "COMPLETED",
              "countsComplete" -> "true",
              "metricSemantics" -> "paimon-reported",
              "preparationAttempts" -> attempt.toString))
        }
      }
    } catch {
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        failed(error)
      case NonFatal(error) => failed(error)
      case error: LinkageError => failed(error)
    } finally synchronized {
        entries.remove(workId)
        detachIfFinished()
      }

  }

  override def cancel(workId: String): Boolean = {
    val unknown = synchronized {
      cancelled.add(workId)
      // An absent worker may have completed without its Future result reaching the coordinator.
      entries.get(workId).forall(_.entered)
    }
    if (available) sc.cancelJobGroup(workId)
    unknown
  }

  override def close(): Unit = {
    val ids = synchronized { closing = true; entries.keys.toVector }
    PaimonSnapshotCancellation.run(ids.map(id => () => { cancel(id); () }))
    synchronized { detachIfFinished() }
  }

  private def detachIfFinished(): Unit = {
    if (closing && entries.isEmpty && !detached) {
      sc.removeSparkListener(listener)
      detached = true
    }
  }

  private def withSession[T](
      slot: Int,
      group: String,
      pool: String,
      target: TargetTable)(body: SparkSession => T): T = {
    val thread = Thread.currentThread()
    val oldLoader = thread.getContextClassLoader
    val oldSession = SparkSession.getActiveSession
    val keys = Seq(
      "spark.jobGroup.id",
      "spark.job.description",
      "spark.job.interruptOnCancel",
      "spark.scheduler.pool")
    val oldProperties = keys.map(key => key -> sc.getLocalProperty(key))
    thread.setContextClassLoader(loader)
    try {
      if (sessions(slot) == null) {
        val session = spark.newSession()
        spark.sessionState.conf.getAllConfs.foreach { case (key, value) =>
          session.sessionState.conf.setConfString(key, value)
        }
        sessions(slot) = session
      }
      val session = sessions(slot)
      SparkSession.setActiveSession(session)
      sc.setJobGroup(
        group,
        s"Remove orphan files: ${context.common.catalogName}.${target.raw} | parallelism=${config.parallelism}",
        interruptOnCancel = true)
      sc.setLocalProperty("spark.scheduler.pool", pool)
      // Sessions share the caller's SparkContext and must never be stopped here.
      SQLConf.withExistingConf(session.sessionState.conf)(body(session))
    } finally {
      oldProperties.foreach { case (key, value) => sc.setLocalProperty(key, value) }
      oldSession match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
      thread.setContextClassLoader(oldLoader)
    }
  }

  private class Entry { var entered = false }
}
