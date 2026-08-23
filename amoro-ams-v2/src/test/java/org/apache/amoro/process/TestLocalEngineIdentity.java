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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.apache.amoro.process.engine.LocalEngineAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Process-local submission identity and conservative restart semantics for the local simulator. */
@Timeout(60)
public class TestLocalEngineIdentity {

  @Test
  public void concurrentSameKeyAndHashDispatchesExactlyOnce() throws Exception {
    CountDownLatch releaseAction = new CountDownLatch(1);
    AtomicInteger runs = new AtomicInteger();
    LocalEngineAdapter adapter =
        new LocalEngineAdapter(
            1,
            16,
            (payload, summary, canceled) -> {
              runs.incrementAndGet();
              releaseAction.await();
            });
    try {
      CountDownLatch ready = new CountDownLatch(16);
      CountDownLatch start = new CountDownLatch(1);
      List<SubmissionOutcome> outcomes =
          java.util.Collections.synchronizedList(new ArrayList<>());
      List<Thread> callers = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        Thread caller =
            new Thread(
                () -> {
                  ready.countDown();
                  await(start);
                  outcomes.add(
                      adapter
                          .submit("p:0:0", "sha256:req", new byte[] {1})
                          .toCompletableFuture()
                          .join());
                });
        callers.add(caller);
        caller.start();
      }
      ready.await(5, TimeUnit.SECONDS);
      start.countDown();
      for (Thread caller : callers) {
        caller.join(5_000L);
      }

      assertEquals(16, outcomes.size());
      Set<String> externalIds = new HashSet<>();
      for (SubmissionOutcome outcome : outcomes) {
        assertEquals(SubmissionOutcome.Kind.ACKNOWLEDGED, outcome.kind());
        externalIds.add(outcome.externalId());
      }
      assertEquals(1, externalIds.size());
      assertTrue(externalIds.iterator().next().startsWith("local-"));
      assertEquals(1, runs.get());
      assertEquals(1, adapter.submissionCount());
    } finally {
      releaseAction.countDown();
      adapter.shutdown(5_000L);
    }
  }

  @Test
  public void sameKeyDifferentHashConflictsWithoutSecondDispatch() throws Exception {
    AtomicInteger runs = new AtomicInteger();
    LocalEngineAdapter adapter =
        new LocalEngineAdapter(1, 4, (payload, summary, canceled) -> runs.incrementAndGet());
    try {
      SubmissionOutcome first =
          adapter
              .submit("p:0:0", "sha256:first", "one".getBytes(StandardCharsets.UTF_8))
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);
      SubmissionOutcome conflict =
          adapter
              .submit("p:0:0", "sha256:other", "two".getBytes(StandardCharsets.UTF_8))
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);

      assertEquals(SubmissionOutcome.Kind.ACKNOWLEDGED, first.kind());
      assertEquals(SubmissionOutcome.Kind.CONFLICT, conflict.kind());
      assertEquals(1, adapter.submissionCount());
    } finally {
      adapter.shutdown(5_000L);
    }
  }

  @Test
  public void missingSubmissionLedgerIsLostRatherThanAuthoritativeNotFound() throws Exception {
    LocalEngineAdapter restarted =
        new LocalEngineAdapter(1, 4, (payload, summary, canceled) -> {});
    try {
      SubmissionResolution resolution =
          restarted
              .resolveSubmission("durable-dispatching:0:0", "sha256:req")
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);

      assertEquals(SubmissionResolution.Kind.LOST, resolution.kind());
      assertTrue(resolution.reason().contains("ledger"));
    } finally {
      restarted.shutdown(5_000L);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
