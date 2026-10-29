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
import java.util.concurrent.{Callable, ExecutorCompletionService, Executors, Future, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.amoro.producer.core.{ActionResult, ActionStatus, ActionTaskTotals, TargetTable}
import org.apache.amoro.producer.spi.ProducerBatchExecutor

/** Bounded Driver submissions; each native CALL keeps its own distributed Spark execution. */
final private[paimon] class PaimonOrphanFilesExecutor(
    concurrency: Int,
    continueOnFailure: Boolean,
    backend: PaimonOrphanFilesBackend) extends ProducerBatchExecutor {
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
          val thread = new Thread(runnable, s"producer-orphan-${names.incrementAndGet()}")
          thread.setDaemon(true)
          thread
        }
      })
    val completion = new ExecutorCompletionService[(String, ActionResult)](pool)
    val active = mutable.LinkedHashMap.empty[String, Pending]
    val slots = mutable.Queue((0 until concurrency): _*)
    val remaining = targets.iterator
    val results = mutable.ArrayBuffer.empty[(TargetTable, ActionResult)]
    var sequence = 0
    var stopped = false
    var interrupted = false

    def finish(id: String, raw: ActionResult): Unit = {
      val pending = active.remove(id).get
      slots.enqueue(pending.slot)
      val elapsed = (System.nanoTime() - pending.startNs) / 1e6
      val result = raw.copy(
        costMs = elapsed,
        metrics = raw.metrics ++ Map(
          "runId" -> runId,
          "workId" -> id,
          "table" -> pending.target.raw,
          "executionMode" -> "distributed",
          "tableConcurrency" -> concurrency.toString,
          "endToEndMs" -> elapsed.toString))
      if (result.status == ActionStatus.Failed && !continueOnFailure) stopped = true
      results += pending.target -> result
      onResult(pending.target, result)
    }

    def submit(): Unit = {
      val target = remaining.next()
      sequence += 1
      val id = s"$runId-$sequence"
      val slot = slots.dequeue()
      val start = System.nanoTime()
      val future = completion.submit(new Callable[(String, ActionResult)] {
        override def call(): (String, ActionResult) = {
          val result =
            try backend.clean(target, id, slot)
            catch {
              case error: InterruptedException =>
                Thread.currentThread().interrupt()
                failure("worker", error, unknown = true)
              case NonFatal(error) => failure("worker", error, unknown = true)
              case error: LinkageError => failure("worker", error, unknown = true)
            }
          id -> result
        }
      })
      active.put(id, Pending(target, slot, start, future))
    }

    def failure(phase: String, error: Throwable, unknown: Boolean): ActionResult =
      PaimonOrphanFilesExecutor.failure(
        phase,
        PaimonSnapshotErrors.format(
          s"Orphan cleaning failed (phase=$phase); no replay",
          error,
          backend.sensitiveValues),
        unknown)

    def cancelAll(phase: String, error: Throwable): Unit = {
      stopped = true
      active.toVector.foreach { case (id, pending) =>
        var unknown = true
        PaimonSnapshotCancellation.run(Seq(
          () => { unknown = backend.cancel(id) },
          () => { pending.future.cancel(true); () }))
        finish(id, failure(phase, error, unknown))
      }
      if (!results.exists(_._2.status == ActionStatus.Failed) && remaining.hasNext) {
        val target = remaining.next()
        val result = failure(phase, error, unknown = false)
        results += target -> result
        onResult(target, result)
      }
      if (!results.exists(_._2.status == ActionStatus.Failed))
        throw new IllegalStateException(PaimonSnapshotErrors.format(
          "Orphan cleaning coordinator failed",
          error,
          backend.sensitiveValues))
    }

    try {
      while (active.nonEmpty || (remaining.hasNext && !stopped)) {
        if (Thread.currentThread().isInterrupted) throw new InterruptedException("Run interrupted")
        var ready = completion.poll()
        while (ready != null) {
          val (id, result) = ready.get()
          finish(id, result)
          ready = completion.poll()
        }
        if ((active.nonEmpty || remaining.hasNext) && !backend.available)
          throw new IllegalStateException("Spark context unavailable")
        if (!stopped && remaining.hasNext && slots.nonEmpty) submit()
        else if (active.nonEmpty) {
          val next = completion.poll(1, TimeUnit.SECONDS)
          if (next != null) {
            val (id, result) = next.get()
            finish(id, result)
          }
        }
      }
    } catch {
      case error: InterruptedException =>
        Thread.interrupted()
        interrupted = true
        cancelAll("cancelled", error)
      case NonFatal(error) => cancelAll("coordination", error)
    } finally {
      interrupted = Thread.interrupted() || interrupted
      pool.shutdownNow()
      try backend.close()
      finally if (interrupted) Thread.currentThread().interrupt()
    }
    results.toVector
  }

  private case class Pending(
      target: TargetTable,
      slot: Int,
      startNs: Long,
      future: Future[(String, ActionResult)])
}

private[paimon] object PaimonOrphanFilesExecutor {
  def failure(phase: String, message: String, unknown: Boolean): ActionResult =
    ActionResult(
      ActionStatus.Failed,
      Seq.empty,
      Some(message),
      Map(
        "failurePhase" -> phase,
        "completionState" -> (if (unknown) "UNKNOWN" else "FAILED"),
        "countsComplete" -> (!unknown).toString),
      0,
      Some(ActionTaskTotals(0, if (unknown) 1 else 0, complete = !unknown)))
}

private[paimon] trait PaimonOrphanFilesBackend extends AutoCloseable {
  def clean(target: TargetTable, workId: String, slot: Int): ActionResult
  def available: Boolean
  def cancel(workId: String): Boolean
  def sensitiveValues: Seq[String] = Seq.empty
  override def close(): Unit = ()
}
