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

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import org.apache.spark.FutureAction
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.amoro.producer.core.ActionResult

class PaimonSnapshotSubmissionTest {
  @Test def cancelBeforeSubmissionPreventsTheAction(): Unit = {
    val submissions = new PaimonSnapshotSubmission()
    submissions.cancel("table")
    assertThrows(
      classOf[InterruptedException],
      () =>
        submissions.submit("table") {
          fail("Cancelled work submitted a Spark Job")
        })
    submissions.close()
  }

  @Test def cancellationDuringSubmissionCannotMissTheHandle(): Unit = {
    val submissions = new PaimonSnapshotSubmission()
    val action = mock(classOf[FutureAction[Seq[ActionResult]]])
    val submitting = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val cancelling = new CountDownLatch(1)
    val threads = Executors.newFixedThreadPool(2)
    try {
      val submitted = threads.submit(new Callable[FutureAction[Seq[ActionResult]]] {
        override def call(): FutureAction[Seq[ActionResult]] = submissions.submit("table") {
          submitting.countDown()
          assertTrue(release.await(5, TimeUnit.SECONDS))
          action
        }
      })
      assertTrue(submitting.await(5, TimeUnit.SECONDS))
      val cancelled = threads.submit(new Runnable {
        override def run(): Unit = { cancelling.countDown(); submissions.cancel("table") }
      })
      assertTrue(cancelling.await(5, TimeUnit.SECONDS))
      release.countDown()
      assertSame(action, submitted.get(5, TimeUnit.SECONDS))
      cancelled.get(5, TimeUnit.SECONDS)
      verify(action).cancel()
      submissions.completed("table")
    } finally { release.countDown(); threads.shutdownNow(); submissions.close() }
  }

  @Test def interruptedCallerAndFailingCancelDoNotSkipRemainingHandles(): Unit = {
    val submissions = new PaimonSnapshotSubmission()
    val first = mock(classOf[FutureAction[Seq[ActionResult]]])
    val second = mock(classOf[FutureAction[Seq[ActionResult]]])
    var calls = 0
    doAnswer(new Answer[Void] {
      override def answer(invocation: InvocationOnMock): Void = {
        assertFalse(
          Thread.currentThread().isInterrupted,
          "Cancellation must clear caller interrupt temporarily")
        calls += 1
        if (calls == 1) throw new InterruptedException("interrupt during Spark event submission")
        null
      }
    }).when(first).cancel()
    submissions.submit("a")(first)
    submissions.submit("b")(second)
    Thread.currentThread().interrupt()
    try {
      submissions.close()
      assertTrue(Thread.currentThread().isInterrupted)
      verify(first, times(2)).cancel()
      verify(second).cancel()
    } finally Thread.interrupted()
  }

  @Test def handleInterruptDoesNotEscapeOrPreventSubsequentGroupCancellation(): Unit = {
    val submissions = new PaimonSnapshotSubmission()
    val action = mock(classOf[FutureAction[Seq[ActionResult]]])
    var attempts = 0
    doAnswer(new Answer[Void] {
      override def answer(invocation: InvocationOnMock): Void = {
        attempts += 1
        if (attempts == 1) throw new InterruptedException("Spark event interrupted")
        null
      }
    }).when(action).cancel()
    submissions.submit("a")(action)
    var groups = 0
    val groupCancel = () => {
      if (Thread.interrupted()) throw new InterruptedException("Group cancellation interrupted")
      groups += 1
    }
    try {
      PaimonSnapshotCancellation.run(Seq(() => submissions.cancel("a"), groupCancel, groupCancel))
      assertEquals(2, groups)
      assertTrue(Thread.currentThread().isInterrupted)
      verify(action, times(2)).cancel()
    } finally { Thread.interrupted(); submissions.completed("a"); submissions.close() }
  }

  @Test def closeCancelsRegisteredJobsAndPermanentlyClosesSubmission(): Unit = {
    val submissions = new PaimonSnapshotSubmission()
    val action = mock(classOf[FutureAction[Seq[ActionResult]]])
    submissions.submit("a")(action)
    submissions.close()
    verify(action).cancel()
    assertThrows(
      classOf[InterruptedException],
      () =>
        submissions.submit("b") {
          fail("Closed backend submitted a Spark Job")
        })
  }
}
