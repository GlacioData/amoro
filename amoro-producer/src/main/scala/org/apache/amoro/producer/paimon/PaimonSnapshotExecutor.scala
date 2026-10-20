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

import java.util.UUID
import java.util.concurrent.{Callable, ExecutorCompletionService, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.amoro.producer.core.{ActionResult, ActionStatus, ActionTaskTotals, TargetTable}
import org.apache.amoro.producer.spi.ProducerBatchExecutor

/** Paimon-owned, two-phase scheduler: preparation and cleanup share the same bounded slots. */
final private[paimon] class PaimonSnapshotExecutor(
    concurrency: Int,
    continueOnFailure: Boolean,
    backend: PaimonSnapshotBackend) extends ProducerBatchExecutor {

  override def execute(
      targets: Seq[TargetTable],
      onResult: (TargetTable, ActionResult) => Unit): Seq[(TargetTable, ActionResult)] = {
    require(concurrency > 0, "tableConcurrency must be positive")
    val runId = UUID.randomUUID().toString
    val names = new AtomicInteger()
    val pool = Executors.newFixedThreadPool(
      concurrency,
      new ThreadFactory {
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable, s"producer-snapshot-${names.incrementAndGet()}")
          thread.setDaemon(true)
          thread
        }
      })
    val completion = new ExecutorCompletionService[Event](pool)
    val active = mutable.LinkedHashMap.empty[String, Pending]
    val seen = mutable.Map.empty[String, String]
    val results = mutable.ArrayBuffer.empty[(TargetTable, ActionResult)]
    val remaining = targets.iterator
    var stop = false
    var cancelled = false
    var restoreInterrupt = false

    def finish(pending: Pending, raw: ActionResult): Unit = {
      active.remove(pending.workId)
      val elapsed = (System.nanoTime() - pending.startNs) / 1e6
      val result = raw.copy(
        costMs = elapsed,
        metrics = raw.metrics ++ Map(
          "executionMode" -> "executor",
          "runId" -> runId,
          "workId" -> pending.workId,
          "table" -> pending.target.raw,
          "submittedAt" -> pending.submittedAt.toString,
          "driverFinishedAt" -> System.currentTimeMillis().toString,
          "endToEndMs" -> elapsed.toString))
      if (result.status == ActionStatus.Failed && !continueOnFailure) stop = true
      results += pending.target -> result
      onResult(pending.target, result)
    }

    def submitPreparation(): Unit = {
      val target = remaining.next()
      val pending = Pending(
        target,
        s"$runId-${names.incrementAndGet()}",
        System.nanoTime(),
        System.currentTimeMillis())
      active.put(pending.workId, pending)
      completion.submit(new Callable[Event] {
        override def call(): Event = {
          try Prepared(pending, backend.prepare(target, runId, pending.workId))
          catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              FailedPreparation(pending, "Preparation interrupted")
            case NonFatal(error) =>
              FailedPreparation(pending, s"Preparation failed (${error.getClass.getSimpleName})")
            case _: LinkageError => FailedPreparation(pending, "Preparation linkage failure")
          }
        }
      })
    }

    def handle(event: Event): Unit = event match {
      case Prepared(pending, work) =>
        seen.get(work.physicalKey) match {
          case Some(primary) =>
            finish(
              pending,
              ActionResult(
                ActionStatus.Skipped,
                Seq.empty,
                Some("Duplicate physical table; no second cleanup submitted"),
                PaimonSnapshotTask.baseMetrics(work) ++ Map(
                  "duplicateTarget" -> "true",
                  "primaryWorkId" -> primary),
                0.0,
                Some(ActionTaskTotals(0, 0, complete = true))))
          case None if stop =>
            // Preparation was already admitted, but failure stopped dispatch before its cleanup.
            active.remove(pending.workId)
          case None =>
            seen.put(work.physicalKey, pending.workId)
            active.update(pending.workId, pending.copy(cleanupSubmitted = true))
            completion.submit(new Callable[Event] {
              override def call(): Event = {
                val result =
                  try backend.execute(work)
                  catch {
                    case _: InterruptedException =>
                      Thread.currentThread().interrupt()
                      PaimonSnapshotTask.failure(
                        PaimonSnapshotTask.baseMetrics(work),
                        "job",
                        "Cleanup interrupted; result unknown",
                        unknown = true)
                    case NonFatal(error) =>
                      PaimonSnapshotTask.failure(
                        PaimonSnapshotTask.baseMetrics(work),
                        "job",
                        s"Cleanup result unavailable (${error.getClass.getSimpleName}); no replay",
                        unknown = true)
                    case _: LinkageError =>
                      PaimonSnapshotTask.failure(
                        PaimonSnapshotTask.baseMetrics(work),
                        "job",
                        "Cleanup linkage failure; result unknown",
                        unknown = true)
                  }
                Completed(pending, result)
              }
            })
        }
      case FailedPreparation(pending, message) =>
        finish(
          pending,
          PaimonSnapshotTask.failure(Map.empty, "preparation", message, unknown = false))
      case Completed(pending, result) => finish(pending, result)
    }

    try {
      while (active.nonEmpty || (remaining.hasNext && !stop)) {
        if (Thread.currentThread().isInterrupted) throw new InterruptedException()
        // Observe every queued failure before admitting another target.
        var ready = completion.poll()
        while (ready != null) {
          if (Thread.currentThread().isInterrupted) throw new InterruptedException()
          handle(ready.get())
          ready = completion.poll()
        }
        if (Thread.currentThread().isInterrupted) throw new InterruptedException()
        if (!backend.available) {
          cancelled = true
          stop = true
          active.values.toVector.foreach { pending =>
            backend.cancel(pending.workId)
            finish(
              pending,
              PaimonSnapshotTask.failure(
                Map.empty,
                "infrastructure",
                "Spark context unavailable",
                unknown = pending.cleanupSubmitted))
          }
          // Infrastructure loss is a failure even when earlier targets already succeeded.
          if (!results.exists(_._2.status == ActionStatus.Failed) && remaining.hasNext) {
            val target = remaining.next()
            finish(
              Pending(target, s"$runId-unavailable", System.nanoTime(), System.currentTimeMillis()),
              PaimonSnapshotTask.failure(
                Map.empty,
                "infrastructure",
                "Spark context unavailable",
                unknown = false))
          }
        }
        if (!stop && remaining.hasNext && active.size < concurrency) {
          submitPreparation()
        } else if (active.nonEmpty) {
          val next = completion.poll(1, TimeUnit.SECONDS)
          if (next != null) handle(next.get())
        }
      }
    } catch {
      case _: InterruptedException =>
        Thread.interrupted()
        restoreInterrupt = true
        cancelled = true
        active.values.toVector.foreach { pending =>
          backend.cancel(pending.workId)
          finish(
            pending,
            PaimonSnapshotTask.failure(
              Map.empty,
              "cancelled",
              "Run interrupted; cleanup termination is not confirmed",
              unknown = pending.cleanupSubmitted))
        }
        if (!results.exists(_._2.status == ActionStatus.Failed) && remaining.hasNext) {
          finish(
            Pending(
              remaining.next(),
              s"$runId-cancelled",
              System.nanoTime(),
              System.currentTimeMillis()),
            PaimonSnapshotTask.failure(
              Map.empty,
              "cancelled",
              "Run interrupted before submission",
              unknown = false))
        }
      case NonFatal(error) =>
        cancelled = true
        active.values.toVector.foreach { pending =>
          backend.cancel(pending.workId)
          finish(
            pending,
            PaimonSnapshotTask.failure(
              Map.empty,
              "coordination",
              s"Coordinator failed (${error.getClass.getSimpleName}); no replay",
              unknown = pending.cleanupSubmitted))
        }
        if (!results.exists(_._2.status == ActionStatus.Failed)) {
          throw new IllegalStateException("Snapshot coordinator failed")
        }
    } finally {
      restoreInterrupt = Thread.interrupted() || restoreInterrupt
      if (cancelled || active.nonEmpty) {
        active.values.foreach(p => backend.cancel(p.workId))
        pool.shutdownNow()
      } else pool.shutdown()
      try backend.close()
      finally if (restoreInterrupt) Thread.currentThread().interrupt()
    }
    results.toVector
  }

  private case class Pending(
      target: TargetTable,
      workId: String,
      startNs: Long,
      submittedAt: Long,
      cleanupSubmitted: Boolean = false)
  sealed private trait Event
  private case class Prepared(pending: Pending, work: PaimonSnapshotWork) extends Event
  private case class FailedPreparation(pending: Pending, message: String) extends Event
  private case class Completed(pending: Pending, result: ActionResult) extends Event
}

private[paimon] trait PaimonSnapshotBackend extends AutoCloseable {
  def prepare(target: TargetTable, runId: String, workId: String): PaimonSnapshotWork
  def execute(work: PaimonSnapshotWork): ActionResult
  def available: Boolean
  def cancel(workId: String): Unit
  override def close(): Unit = ()
}
