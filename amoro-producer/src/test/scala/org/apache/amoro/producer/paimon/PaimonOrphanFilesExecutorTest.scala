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
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core._

class PaimonOrphanFilesExecutorTest {
  @Test def boundsInFlightTablesAndRefillsSlotsBeforeTheSlowestTableFinishes(): Unit = {
    val release = new CountDownLatch(1)
    val refilled = new CountDownLatch(1)
    val running = new AtomicInteger()
    val peak = new AtomicInteger()
    val callbacks = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
    val backend = new Backend {
      override def clean(target: TargetTable, id: String, slot: Int): ActionResult = {
        val count = running.incrementAndGet()
        peak.accumulateAndGet(
          count,
          new java.util.function.IntBinaryOperator {
            override def applyAsInt(a: Int, b: Int): Int = math.max(a, b)
          })
        try {
          if (target.table == "t0") assertTrue(release.await(10, TimeUnit.SECONDS))
          if (target.table == "t2") refilled.countDown()
          success
        } finally running.decrementAndGet()
      }
    }
    val driver = Executors.newSingleThreadExecutor()
    val future = driver.submit(new java.util.concurrent.Callable[Seq[(TargetTable, ActionResult)]] {
      override def call(): Seq[(TargetTable, ActionResult)] =
        new PaimonOrphanFilesExecutor(2, true, backend).execute(
          targets(6),
          (_, _) => callbacks.add(Thread.currentThread().getId))
    })
    try {
      assertTrue(
        refilled.await(5, TimeUnit.SECONDS),
        "A completed slot must not wait for the slow table")
      assertFalse(future.isDone)
      assertEquals(2, peak.get())
      release.countDown()
      assertEquals(6, future.get(10, TimeUnit.SECONDS).size)
      assertEquals(6, callbacks.size())
      assertEquals(1, callbacks.toArray.distinct.length)
      assertTrue(backend.closed.get())
    } finally { release.countDown(); driver.shutdownNow() }
  }

  @Test def failureIsNotReplayedAndContinueOnFailureControlsFurtherAdmission(): Unit = {
    Seq(false, true).foreach { continue =>
      val calls = new AtomicInteger()
      val backend = new Backend {
        override def clean(target: TargetTable, id: String, slot: Int): ActionResult = {
          calls.incrementAndGet()
          if (target.table == "t0") throw new IOException("password=secret-value")
          success
        }
      }
      val results =
        new PaimonOrphanFilesExecutor(1, continue, backend).execute(targets(3), (_, _) => ())
      assertEquals(if (continue) 3 else 1, calls.get())
      val failed = results.head._2
      assertEquals(ActionStatus.Failed, failed.status)
      assertEquals("UNKNOWN", failed.metrics("completionState"))
      assertFalse(failed.metrics.contains("deletedFileCount"))
      assertTrue(failed.message.get.contains("java.io.IOException"))
      assertTrue(failed.message.get.contains("no replay"))
      assertFalse(failed.message.get.contains("secret-value"))
    }
  }

  @Test def cancellationStopsAdmissionAndPreservesUnknownResultAndInterrupt(): Unit = {
    val entered = new CountDownLatch(1)
    val cancelled = new AtomicInteger()
    val calls = new AtomicInteger()
    val results = new AtomicReference[Seq[(TargetTable, ActionResult)]]()
    val interrupted = new AtomicBoolean()
    val backend = new Backend {
      override def clean(target: TargetTable, id: String, slot: Int): ActionResult = {
        calls.incrementAndGet()
        entered.countDown()
        new CountDownLatch(1).await()
        success
      }
      override def cancel(id: String): Boolean = { cancelled.incrementAndGet(); true }
    }
    val driver = new Thread(() => {
      results.set(new PaimonOrphanFilesExecutor(1, true, backend).execute(targets(3), (_, _) => ()))
      interrupted.set(Thread.currentThread().isInterrupted)
    })
    driver.start()
    assertTrue(entered.await(5, TimeUnit.SECONDS))
    driver.interrupt()
    driver.join(5000)
    assertFalse(driver.isAlive)
    assertTrue(interrupted.get())
    assertTrue(backend.closed.get())
    assertEquals(1, calls.get())
    assertEquals(1, cancelled.get())
    assertEquals("UNKNOWN", results.get().head._2.metrics("completionState"))
  }

  @Test def infrastructureLossCannotReportSuccessWithQueuedTables(): Unit = {
    val online = new AtomicBoolean(true)
    val backend = new Backend { override def available: Boolean = online.get() }
    val results = new PaimonOrphanFilesExecutor(1, true, backend)
      .execute(targets(3), (_, _) => online.set(false))
    assertEquals(Seq(ActionStatus.Success, ActionStatus.Failed), results.map(_._2.status))
    assertEquals("FAILED", results.last._2.metrics("completionState"))
  }

  @Test def emptyTargetsCloseTheBackend(): Unit = {
    val backend = new Backend
    assertTrue(new PaimonOrphanFilesExecutor(2, true, backend).execute(
      Seq.empty,
      (_, _) => ()).isEmpty)
    assertTrue(backend.closed.get())
  }

  @Test def aCompletedRunDoesNotFailIfTheContextStopsAfterItsLastResult(): Unit = {
    val online = new AtomicBoolean(true)
    val backend = new Backend { override def available: Boolean = online.get() }
    val results = new PaimonOrphanFilesExecutor(1, true, backend)
      .execute(targets(1), (_, _) => online.set(false))
    assertEquals(Seq(ActionStatus.Success), results.map(_._2.status))
  }

  private def targets(count: Int): Seq[TargetTable] =
    (0 until count).map(i => TargetTable(s"db.t$i", "db", s"t$i"))
  private def success: ActionResult =
    ActionResult(ActionStatus.Success, Seq.empty, None, Map.empty, 0)
  private class Backend extends PaimonOrphanFilesBackend {
    val closed = new AtomicBoolean()
    override def available: Boolean = true
    override def clean(target: TargetTable, id: String, slot: Int): ActionResult = success
    override def cancel(id: String): Boolean = true
    override def close(): Unit = { closed.set(true) }
  }
}
