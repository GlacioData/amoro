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

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Durable command concurrency: a single CAS winner and an identity-bound replay. */
@Timeout(20)
public class TestProcessCommandService {

  @Test
  public void twoConcurrentIdenticalCommandsProduceOneWriteAndOneReplay() throws Exception {
    DefaultScheduler scheduler = DefaultScheduler.create(1, 1000L);
    ProcessDomainAssembly assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            scheduler,
            128,
            10_000L,
            65536);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      assembly.repository().create(unresolvedSubmission("concurrent-command"));
      ProcessCommandService service =
          new ProcessCommandService(
              assembly.repository(),
              Clock.fixed(Instant.parse("2026-08-24T01:00:00Z"), ZoneOffset.UTC));
      ManualResolutionTransition.Command command =
          new ManualResolutionTransition.Command(
              ManualResolutionTransition.Kind.SUBMISSION,
              "same-resolution-key",
              "concurrent-command:0:0",
              "sha256:req",
              "ACKNOWLEDGED",
              "dummy-execution",
              null,
              "simulated ledger verified the submission",
              "test-operator");
      CountDownLatch ready = new CountDownLatch(2);
      CountDownLatch start = new CountDownLatch(1);

      java.util.concurrent.Callable<ProcessCommandService.CommandResult> call =
          () -> {
            ready.countDown();
            start.await();
            return service.resolve(command, "concurrent-command");
          };
      Future<ProcessCommandService.CommandResult> first = callers.submit(call);
      Future<ProcessCommandService.CommandResult> second = callers.submit(call);
      ready.await();
      start.countDown();

      List<ProcessCommandService.CommandResult> results = List.of(first.get(), second.get());
      assertEquals(
          1, results.stream().filter(ProcessCommandService.CommandResult::replayed).count());
      assertEquals(
          1, results.stream().filter(result -> !result.replayed()).count(), "exactly one CAS wins");
      assertTrue(results.stream().allMatch(result -> result.resource().resourceVersion() == 2L));

      ProcessResource durable = assembly.repository().get("concurrent-command");
      assertEquals(2L, durable.resourceVersion(), "the command is durably written only once");
      assertEquals("SUBMITTED", durable.status().phase());
      assertEquals("dummy-execution", durable.status().attempt().externalId());
      assertEquals(
          "ACKNOWLEDGED", durable.status().attempt().manualResolutions().submission().outcome());
    } finally {
      callers.shutdownNow();
      assembly.persistence().shutdown(Duration.ofSeconds(5));
      scheduler.shutdown(Duration.ofSeconds(5));
    }
  }

  private static ProcessResource unresolvedSubmission(String name) {
    String dispatchedAt = "2026-08-24T00:59:00Z";
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            name + ":0:0",
            "sha256:req",
            "UNKNOWN",
            null,
            dispatchedAt,
            "submission outcome is unknown",
            "AUTO",
            null,
            Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    return new ProcessResource(
        name,
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("sim", "db", name, name, "simulated"),
            "dummy-maintenance",
            "local",
            "MANUAL",
            "2026-08-24T00:00:00Z",
            "RUN",
            new ProcessResource.RequestIdentity("sha256:create-key", "sha256:create-request"),
            Collections.singletonMap("simulated", true),
            new ProcessResource.RetryPolicy(3, 2, 30)),
        new ProcessResource.ProcessStatus(
            "UNKNOWN",
            0,
            attempt,
            Collections.emptyList(),
            null,
            null,
            "2026-08-24T01:00:00Z",
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            ProcessConditions.set(
                Collections.emptyList(),
                ProcessConditions.SUBMISSION_UNRESOLVED,
                "UNKNOWN",
                "simulated submission is unresolved",
                dispatchedAt,
                null),
            null,
            null,
            null,
            null,
            null));
  }
}
