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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Shared bounded shutdown used by Process maintenance executors. */
public final class BoundedExecutorShutdown {

  private static final Logger LOG = LoggerFactory.getLogger(BoundedExecutorShutdown.class);

  private BoundedExecutorShutdown() {}

  public static boolean shutdown(
      ExecutorService executor, long timeoutMillis, String componentName) {
    Objects.requireNonNull(executor, "executor");
    Objects.requireNonNull(componentName, "componentName");
    if (timeoutMillis <= 0) {
      throw new IllegalArgumentException("timeoutMillis must be > 0");
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    executor.shutdown();
    try {
      if (awaitUntil(executor, deadline)) {
        return true;
      }
      executor.shutdownNow();
      if (!awaitUntil(executor, deadline)) {
        LOG.warn("{} executor did not terminate after bounded shutdown.", componentName);
        return false;
      }
      return true;
    } catch (InterruptedException interrupted) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
      return executor.isTerminated();
    }
  }

  private static boolean awaitUntil(ExecutorService executor, long deadlineNanos)
      throws InterruptedException {
    long remaining = deadlineNanos - System.nanoTime();
    return remaining <= 0L
        ? executor.isTerminated()
        : executor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
  }
}
