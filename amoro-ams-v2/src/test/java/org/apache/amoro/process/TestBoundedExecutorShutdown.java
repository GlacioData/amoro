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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestBoundedExecutorShutdown {

  @Test
  public void gracefulShutdownWaitsForTheCurrentMaintenanceRound() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch shutdownStarted = new CountDownLatch(1);
    executor.execute(
        () -> {
          entered.countDown();
          try {
            release.await();
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
          }
        });
    assertTrue(entered.await(1, TimeUnit.SECONDS));

    CompletableFuture<Void> shutdown =
        CompletableFuture.runAsync(
            () -> {
              shutdownStarted.countDown();
              BoundedExecutorShutdown.shutdown(executor, 1_000L, "test maintenance");
            });
    assertTrue(shutdownStarted.await(1, TimeUnit.SECONDS));
    assertFalse(shutdown.isDone(), "dependency shutdown must wait for the active round");

    release.countDown();
    shutdown.get(1, TimeUnit.SECONDS);
    assertTrue(executor.isTerminated());
  }

  @Test
  public void timedOutMaintenanceRoundIsInterrupted() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    executor.execute(
        () -> {
          entered.countDown();
          try {
            new CountDownLatch(1).await();
          } catch (InterruptedException expected) {
            interrupted.countDown();
            Thread.currentThread().interrupt();
          }
        });
    assertTrue(entered.await(1, TimeUnit.SECONDS));

    BoundedExecutorShutdown.shutdown(executor, 100L, "test maintenance");

    assertTrue(interrupted.await(1, TimeUnit.SECONDS));
    assertTrue(executor.isTerminated());
  }
}
