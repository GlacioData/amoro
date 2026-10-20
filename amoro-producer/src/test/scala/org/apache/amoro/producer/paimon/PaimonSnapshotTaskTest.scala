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
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.Assertions._

import org.apache.amoro.producer.core._

class PaimonSnapshotTaskTest {
  @AfterEach def clearInterrupt(): Unit = { Thread.interrupted(); () }

  @Test def loopsInOneAttemptWithEffectiveTableLimit(): Unit = {
    val calls = new AtomicInteger()
    val result = run(work(2), () => if (calls.incrementAndGet() <= 2) 2 else 1)
    assertEquals(ActionStatus.Success, result.status)
    assertEquals(3, calls.get())
    assertEquals("5", result.metrics("deletedSnapshotsTotal"))
    assertEquals("42", result.metrics("taskAttemptId"))
    assertEquals(3L, result.taskTotals.get.success)
  }

  @Test def rejectsBothTaskAndStageReplayBeforeCreatingTheNativeObject(): Unit = {
    Seq((1, 0), (0, 1), (2, 3)).foreach { case (task, stage) =>
      var created = false
      val result = PaimonSnapshotTask.execute(
        work(2),
        task,
        stage,
        Map.empty,
        () => false,
        () => {
          created = true
          () => 0
        })
      assertFalse(created)
      assertEquals(ActionStatus.Failed, result.status)
      assertEquals("UNKNOWN", result.metrics("completionState"))
      assertFalse(result.metrics.contains("deletedSnapshotsTotal"))
    }
  }

  @Test def capsDetailsButPreservesLongCumulativeCounts(): Unit = {
    var rounds = 0
    val result = run(work(1), () => { rounds += 1; if (rounds <= 150) 1 else 0 })
    assertEquals(100, result.tasks.size)
    assertEquals("round-52", result.tasks.head.name)
    assertEquals(151L, result.taskTotals.get.success)
    assertEquals("150", result.metrics("deletedSnapshotsTotal"))
    assertEquals("true", result.metrics("detailsTruncated"))
    val summary = ProducerSummary.fromResults(Seq(work(1).target -> result))
    assertEquals(151L, summary.taskSuccessCount)
    assertTrue(summary.taskCountsComplete)
  }

  @Test def nativeFailurePreservesConfirmedLowerBoundAndNeverRetries(): Unit = {
    var calls = 0
    val result = run(
      work(2),
      () => {
        calls += 1
        if (calls == 1) 2 else throw new IOException("password=secret-value")
      })
    assertEquals(2, calls)
    assertEquals(ActionStatus.Failed, result.status)
    assertEquals("UNKNOWN", result.metrics("completionState"))
    assertEquals("false", result.metrics("countsComplete"))
    assertEquals("2", result.metrics("deletedSnapshotsTotal"))
    assertEquals(1L, result.taskTotals.get.success)
    assertEquals(1L, result.taskTotals.get.failed)
    assertFalse(result.message.get.contains("secret-value"))
  }

  @Test def failureWithOutstandingFileWorkDoesNotStartAnotherCleanup(): Unit = {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val pool = Executors.newSingleThreadExecutor()
    val future = pool.submit(new Runnable {
      override def run(): Unit = { started.countDown(); release.await() }
    })
    try {
      assertTrue(started.await(5, TimeUnit.SECONDS))
      var calls = 0
      val result = run(work(1), () => { calls += 1; throw new IOException("incomplete FileIO") })
      assertEquals(1, calls)
      assertFalse(future.isDone)
      assertEquals("UNKNOWN", result.metrics("completionState"))
      assertFalse(result.metrics.contains("deletedSnapshotsTotal"))
    } finally { release.countDown(); pool.shutdownNow() }
  }

  @Test def cancellationStopsBeforeTheNextRound(): Unit = {
    var calls = 0
    val result = PaimonSnapshotTask.execute(
      work(1),
      0,
      0,
      Map.empty,
      () => calls > 0,
      () => () => { calls += 1; 1 })
    assertEquals(1, calls)
    assertEquals(ActionStatus.Failed, result.status)
    assertTrue(Thread.currentThread().isInterrupted)
    assertEquals("1", result.metrics("deletedSnapshotsTotal"))
  }

  @Test def preparationFailureHasNoUncertainSideEffects(): Unit = {
    val result = PaimonSnapshotTask.execute(
      work(1),
      0,
      0,
      Map.empty,
      () => false,
      () => throw new IllegalArgumentException("credential=secret-value"))
    assertEquals("FAILED", result.metrics("completionState"))
    assertEquals("true", result.metrics("countsComplete"))
    assertEquals(0L, result.taskTotals.get.failed)
    assertFalse(result.message.get.contains("secret-value"))
  }

  @Test def requiresOneWorkUnitAndATaskContext(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => PaimonSnapshotTask.runPartition(Iterator.empty))
    assertThrows(
      classOf[IllegalArgumentException],
      () => PaimonSnapshotTask.runPartition(Iterator(work(1), work(1))))
    assertThrows(
      classOf[IllegalArgumentException],
      () => PaimonSnapshotTask.runPartition(Iterator(work(1))))
  }

  private def run(work: PaimonSnapshotWork, expire: () => Int): ActionResult =
    PaimonSnapshotTask.execute(work, 0, 0, Map("taskAttemptId" -> "42"), () => false, () => expire)

  private def work(limit: Int): PaimonSnapshotWork = {
    val policy = PaimonExpireSnapshotsConfig(4, 3, limit, "", Some(Duration.ofHours(24)))
    PaimonSnapshotWork(
      "run",
      "work",
      TargetTable("db.t", "db", "t"),
      null,
      policy,
      "file:/test",
      "test.FileIO",
      PaimonSnapshotWork.fingerprint(policy),
      Map.empty)
  }
}
