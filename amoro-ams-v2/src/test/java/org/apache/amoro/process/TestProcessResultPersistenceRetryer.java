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

package org.apache.amoro.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.FakeEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight;
import org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandInFlightException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Timeout(10)
public class TestProcessResultPersistenceRetryer {

  @Test
  public void flightIsHeldUntilTransientPersistenceFailureRecovers() throws Exception {
    try (ProcessEngineDispatcher dispatcher =
            new ProcessEngineDispatcher(new FakeEngineAdapter(), 1_000L);
        ProcessResultPersistenceRetryer retryer = new ProcessResultPersistenceRetryer(4, 2, 10L)) {
      CommandFlight<SubmissionOutcome> first =
          dispatcher.submit("p1", "p1:0:0", "sha256:req", new byte[0]);
      first.toCompletableFuture().join();
      AtomicInteger writes = new AtomicInteger();

      retryer.handle(
          "p1|submit|p1:0:0|sha256:req", first, () -> writes.incrementAndGet() >= 2, () -> {});

      assertEquals(1, retryer.pendingCount());
      assertThrows(
          CommandInFlightException.class,
          () -> dispatcher.submit("p1", "p1:0:0", "sha256:req", new byte[0]));

      long deadline = System.nanoTime() + 2_000_000_000L;
      while (retryer.pendingCount() != 0 && System.nanoTime() < deadline) {
        Thread.sleep(5L);
      }
      assertEquals(0, retryer.pendingCount());

      CommandFlight<SubmissionOutcome> replay =
          dispatcher.submit("p1", "p1:0:0", "sha256:req", new byte[0]);
      replay.toCompletableFuture().join();
      replay.markDurablyHandled();
    }
  }

  @Test
  public void boundedCapacityFailsClosedAndCloseStopsFurtherReservations() {
    try (ProcessEngineDispatcher dispatcher =
        new ProcessEngineDispatcher(new FakeEngineAdapter(), 1_000L)) {
      ProcessResultPersistenceRetryer retryer = new ProcessResultPersistenceRetryer(1, 1, 60_000L);
      CommandFlight<SubmissionOutcome> retained =
          dispatcher.submit("capacity-a", "capacity-a:0:0", "sha256:req-a", new byte[0]);
      CommandFlight<SubmissionOutcome> saturated =
          dispatcher.submit("capacity-b", "capacity-b:0:0", "sha256:req-b", new byte[0]);
      retained.toCompletableFuture().join();
      saturated.toCompletableFuture().join();
      AtomicInteger saturatedWrites = new AtomicInteger();

      retryer.handle("capacity-a", retained, () -> false, () -> {});
      retryer.handle(
          "capacity-b", saturated, () -> saturatedWrites.incrementAndGet() > 0, () -> {});

      assertEquals(1, retryer.pendingCount());
      assertEquals(0, saturatedWrites.get(), "saturation must not run an unreserved durable apply");
      assertThrows(
          CommandInFlightException.class,
          () -> dispatcher.submit("capacity-a", "capacity-a:0:0", "sha256:req-a", new byte[0]));
      assertThrows(
          CommandInFlightException.class,
          () -> dispatcher.submit("capacity-b", "capacity-b:0:0", "sha256:req-b", new byte[0]));

      retryer.close();
      assertEquals(0, retryer.pendingCount());
      assertNull(retryer.tryReserve(), "a closed retry lane must reject new engine commands");
      assertThrows(
          CommandInFlightException.class,
          () -> dispatcher.submit("capacity-a", "capacity-a:0:0", "sha256:req-a", new byte[0]));
      assertThrows(
          CommandInFlightException.class,
          () -> dispatcher.submit("capacity-b", "capacity-b:0:0", "sha256:req-b", new byte[0]));
      retryer.close();
    }
  }

  @Test
  public void failedHeadRotatesBehindOtherPendingResults() {
    try (ProcessEngineDispatcher dispatcher =
            new ProcessEngineDispatcher(new FakeEngineAdapter(), 1_000L);
        ProcessResultPersistenceRetryer retryer =
            new ProcessResultPersistenceRetryer(2, 1, 60_000L)) {
      CommandFlight<SubmissionOutcome> alwaysFailing =
          dispatcher.submit("fair-a", "fair-a:0:0", "sha256:req-a", new byte[0]);
      CommandFlight<SubmissionOutcome> eventuallySuccessful =
          dispatcher.submit("fair-b", "fair-b:0:0", "sha256:req-b", new byte[0]);
      alwaysFailing.toCompletableFuture().join();
      eventuallySuccessful.toCompletableFuture().join();
      AtomicInteger firstAttempts = new AtomicInteger();
      AtomicInteger secondAttempts = new AtomicInteger();

      retryer.handle("fair-a", alwaysFailing, () -> firstAttempts.incrementAndGet() < 0, () -> {});
      retryer.handle(
          "fair-b", eventuallySuccessful, () -> secondAttempts.incrementAndGet() >= 2, () -> {});
      assertEquals(2, retryer.pendingCount());

      retryer.drainOnce();
      assertEquals(2, firstAttempts.get());
      assertEquals(1, secondAttempts.get());
      assertEquals(2, retryer.pendingCount());

      retryer.drainOnce();
      assertEquals(2, firstAttempts.get(), "the failing head must rotate to the queue tail");
      assertEquals(2, secondAttempts.get());
      assertEquals(1, retryer.pendingCount());

      CommandFlight<SubmissionOutcome> released =
          dispatcher.submit("fair-b", "fair-b:0:0", "sha256:req-b", new byte[0]);
      released.toCompletableFuture().join();
      released.markDurablyHandled();
      assertThrows(
          CommandInFlightException.class,
          () -> dispatcher.submit("fair-a", "fair-a:0:0", "sha256:req-a", new byte[0]));
    }
  }

  @Test
  public void shutdownWaitsForDirectResultApplyOwnedByTheRetryLane() throws Exception {
    try (ProcessEngineDispatcher dispatcher =
        new ProcessEngineDispatcher(new FakeEngineAdapter(), 1_000L)) {
      ProcessResultPersistenceRetryer retryer = new ProcessResultPersistenceRetryer(1, 1, 60_000L);
      CommandFlight<SubmissionOutcome> flight =
          dispatcher.submit("closing", "closing:0:0", "sha256:req", new byte[0]);
      flight.toCompletableFuture().join();
      CountDownLatch applyEntered = new CountDownLatch(1);
      CountDownLatch releaseApply = new CountDownLatch(1);
      CompletableFuture<Void> callback =
          CompletableFuture.runAsync(
              () ->
                  retryer.handle(
                      "closing",
                      flight,
                      () -> {
                        applyEntered.countDown();
                        try {
                          return releaseApply.await(2, TimeUnit.SECONDS);
                        } catch (InterruptedException interrupted) {
                          Thread.currentThread().interrupt();
                          return false;
                        }
                      },
                      () -> {}));
      assertTrue(applyEntered.await(1, TimeUnit.SECONDS));

      CompletableFuture<Void> shutdown = CompletableFuture.runAsync(() -> retryer.shutdown(1_000L));
      assertFalse(shutdown.isDone(), "shutdown must wait for the owned durable apply");

      releaseApply.countDown();
      callback.get(1, TimeUnit.SECONDS);
      shutdown.get(1, TimeUnit.SECONDS);
      assertNull(retryer.tryReserve());
    }
  }
}
