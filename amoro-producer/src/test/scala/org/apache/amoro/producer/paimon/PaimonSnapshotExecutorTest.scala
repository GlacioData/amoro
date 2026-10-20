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

import java.io.IOException
import java.time.Duration
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.mutable

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core._

class PaimonSnapshotExecutorTest {
  @Test def boundsPreparationAndExecutionAndReusesSlots(): Unit = {
    val started = new CountDownLatch(2)
    val release = new CountDownLatch(1)
    val prepared = new AtomicInteger()
    val running = new AtomicInteger()
    val peak = new AtomicInteger()
    val backend = new Backend {
      override def prepare(target: TargetTable, run: String, id: String): PaimonSnapshotWork = {
        prepared.incrementAndGet()
        super.prepare(target, run, id)
      }
      override def execute(work: PaimonSnapshotWork): ActionResult = {
        val count = running.incrementAndGet()
        peak.accumulateAndGet(
          count,
          new java.util.function.IntBinaryOperator {
            override def applyAsInt(left: Int, right: Int): Int = math.max(left, right)
          })
        try {
          started.countDown()
          assertTrue(release.await(5, TimeUnit.SECONDS))
          success
        } finally running.decrementAndGet()
      }
    }
    val driver = Executors.newSingleThreadExecutor()
    val callbacks = mutable.ArrayBuffer.empty[(String, Long)]
    val future = driver.submit(new Callable[Seq[(TargetTable, ActionResult)]] {
      override def call(): Seq[(TargetTable, ActionResult)] =
        new PaimonSnapshotExecutor(2, true, backend).execute(
          targets(8),
          (table, _) =>
            callbacks += table.raw -> Thread.currentThread().getId)
    })
    try {
      assertTrue(started.await(5, TimeUnit.SECONDS))
      assertEquals(2, prepared.get())
      assertEquals(2, peak.get())
      release.countDown()
      assertEquals(8, future.get(10, TimeUnit.SECONDS).size)
      assertEquals(8, callbacks.map(_._1).distinct.size)
      assertEquals(1, callbacks.map(_._2).distinct.size)
      assertEquals(2, peak.get())
      assertEquals(1, backend.closed.get())
    } finally { release.countDown(); driver.shutdownNow() }
  }

  @Test def stopsAfterFailureOrContinuesAccordingToPolicy(): Unit = {
    Seq(false, true).foreach { continue =>
      val calls = new AtomicInteger()
      val backend = new Backend {
        override def execute(work: PaimonSnapshotWork): ActionResult = {
          calls.incrementAndGet()
          if (work.target.table == "t0") throw new IOException("password=secret-value")
          success
        }
      }
      val results =
        new PaimonSnapshotExecutor(1, continue, backend).execute(targets(3), (_, _) => ())
      assertEquals(if (continue) 3 else 1, calls.get())
      assertEquals(if (continue) 3 else 1, results.size)
      assertEquals("UNKNOWN", results.head._2.metrics("completionState"))
      assertFalse(results.head._2.metrics.contains("deletedSnapshotsTotal"))
      assertFalse(results.head._2.message.get.contains("secret-value"))
    }
  }

  @Test def deduplicatesPhysicalTargetsEvenAfterFailure(): Unit = {
    val calls = new AtomicInteger()
    val backend = new Backend {
      override def prepare(target: TargetTable, run: String, id: String): PaimonSnapshotWork =
        super.prepare(target, run, id).copy(physicalKey = "file:/same-table")
      override def execute(work: PaimonSnapshotWork): ActionResult = {
        calls.incrementAndGet()
        throw new IOException("result lost")
      }
    }
    val results = new PaimonSnapshotExecutor(2, true, backend).execute(targets(3), (_, _) => ())
    assertEquals(1, calls.get())
    assertEquals(1, results.count(_._2.status == ActionStatus.Failed))
    assertEquals(2, results.count(_._2.status == ActionStatus.Skipped))
    val skipped = results.find(_._2.status == ActionStatus.Skipped).get._2
    assertTrue(skipped.metrics.contains("primaryWorkId"))
  }

  @Test def reportsPreparationFailureWithoutUnknownDeletes(): Unit = {
    val backend = new Backend {
      override def prepare(target: TargetTable, run: String, id: String): PaimonSnapshotWork = {
        if (target.table == "t0") throw new IOException("s3://password=secret-value")
        super.prepare(target, run, id)
      }
    }
    val results = new PaimonSnapshotExecutor(1, true, backend).execute(targets(2), (_, _) => ())
    assertEquals(2, results.size)
    assertEquals("FAILED", results.head._2.metrics("completionState"))
    assertEquals("true", results.head._2.metrics("countsComplete"))
    assertFalse(results.head._2.message.get.contains("secret-value"))
  }

  @Test def unavailableContextDoesNotSucceedOrSubmitWork(): Unit = {
    val backend = new Backend {
      override def available: Boolean = false
      override def execute(work: PaimonSnapshotWork): ActionResult =
        throw new AssertionError("submitted")
    }
    val results = new PaimonSnapshotExecutor(2, true, backend).execute(targets(2), (_, _) => ())
    assertEquals(1, results.size)
    assertEquals(ActionStatus.Failed, results.head._2.status)
    assertEquals(1, backend.closed.get())
  }

  @Test def contextLossAfterSuccessStillFailsTheRun(): Unit = {
    val online = new AtomicBoolean(true)
    val backend = new Backend { override def available: Boolean = online.get() }
    val results =
      new PaimonSnapshotExecutor(1, true, backend).execute(targets(2), (_, _) => online.set(false))
    assertEquals(Seq(ActionStatus.Success, ActionStatus.Failed), results.map(_._2.status))
    assertEquals("infrastructure", results.last._2.metrics("failurePhase"))
  }

  @Test def cancellationAfterSuccessCannotReportSuccessWithQueuedTargets(): Unit = {
    try {
      val backend = new Backend {
        override def close(): Unit = {
          assertFalse(
            Thread.currentThread().isInterrupted,
            "Close must run before restoring interrupt")
          super.close()
        }
      }
      val results = new PaimonSnapshotExecutor(1, true, backend).execute(
        targets(3),
        (_, _) => Thread.currentThread().interrupt())
      assertEquals(Seq(ActionStatus.Success, ActionStatus.Failed), results.map(_._2.status))
      assertTrue(Thread.currentThread().isInterrupted)
    } finally Thread.interrupted()
  }

  @Test def cancellationMarksActiveCleanupUnknownAndStopsAdmission(): Unit = {
    val entered = new CountDownLatch(1)
    val exited = new CountDownLatch(1)
    val cancelled = new AtomicInteger()
    val result = new AtomicReference[Seq[(TargetTable, ActionResult)]]()
    val backend = new Backend {
      override def execute(work: PaimonSnapshotWork): ActionResult = {
        entered.countDown()
        try { new CountDownLatch(1).await(); success }
        finally exited.countDown()
      }
      override def cancel(id: String): Unit = { cancelled.incrementAndGet(); () }
    }
    val thread = new Thread(() =>
      result.set(new PaimonSnapshotExecutor(1, true, backend)
        .execute(targets(3), (_, _) => ())))
    thread.start()
    assertTrue(entered.await(5, TimeUnit.SECONDS))
    thread.interrupt()
    thread.join(5000)
    assertFalse(thread.isAlive)
    assertTrue(exited.await(5, TimeUnit.SECONDS))
    assertEquals(1, result.get().size)
    assertEquals("UNKNOWN", result.get().head._2.metrics("completionState"))
    assertEquals(1, cancelled.get())
    assertEquals(1, backend.closed.get())
  }

  @Test def emptyTargetsCloseBackendAndSucceed(): Unit = {
    val backend = new Backend
    assertTrue(new PaimonSnapshotExecutor(2, true, backend).execute(
      Seq.empty,
      (_, _) => ()).isEmpty)
    assertEquals(1, backend.closed.get())
  }

  private def targets(count: Int): Seq[TargetTable] =
    (0 until count).map(i => TargetTable(s"db.t$i", "db", s"t$i"))
  private def success: ActionResult =
    ActionResult(ActionStatus.Success, Seq.empty, None, Map.empty, 0)

  private class Backend extends PaimonSnapshotBackend {
    val closed = new AtomicInteger()
    override def available: Boolean = true
    override def cancel(id: String): Unit = ()
    override def close(): Unit = { closed.incrementAndGet(); () }
    override def execute(work: PaimonSnapshotWork): ActionResult = success
    override def prepare(target: TargetTable, run: String, id: String): PaimonSnapshotWork = {
      val policy = PaimonExpireSnapshotsConfig(4, 3, 1, "", Some(Duration.ofHours(24)))
      PaimonSnapshotWork(
        run,
        id,
        target,
        null,
        policy,
        s"file:/${target.table}",
        "test.FileIO",
        PaimonSnapshotWork.fingerprint(policy),
        Map.empty)
    }
  }
}
