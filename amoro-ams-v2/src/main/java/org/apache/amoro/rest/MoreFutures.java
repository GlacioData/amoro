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

package org.apache.amoro.rest;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Dereferences {@link CompletionStage} results from the service layer with a uniform backstop
 * timeout (appmanager's {@code MoreFutures.derefUsingDefaultTimeout} port). The service contract
 * is asynchronous; the MVC layer stays synchronous by unwrapping here.
 *
 * <p>The default timeout is a safety net above the repository facade's own bound ({@code
 * amoro.control.repository.timeout-ms}, default 10s): service calls currently complete on the
 * caller thread, so the deref can only outrun the underlying bounded waits.
 */
public final class MoreFutures {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30L);

  private MoreFutures() {}

  /**
   * Waits at most the default timeout for the stage. The stage's original {@link RuntimeException}
   * is rethrown unchanged so the {@code @ExceptionHandler} path stays identical to the synchronous
   * calling convention; checked causes are wrapped in {@link IllegalStateException}.
   */
  public static <T> T derefUsingDefaultTimeout(CompletionStage<T> stage) {
    try {
      return stage.toCompletableFuture().get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while awaiting the service stage", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new IllegalStateException("service stage failed", e.getCause());
    } catch (TimeoutException e) {
      throw new IllegalStateException(
          "service stage did not complete within " + DEFAULT_TIMEOUT.toMillis() + "ms", e);
    }
  }
}
