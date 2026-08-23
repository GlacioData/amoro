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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/** P8: the TTL cleaner — only old final resources leave, in bounded batches. */
@Timeout(60)
public class TestProcessTtlCleaner {

  private DefaultScheduler scheduler;
  private ProcessDomainAssembly assembly;
  private ProcessTtlCleaner cleaner;

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(1, 1000L);
    assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            scheduler,
            128,
            10_000L,
            65536);
    cleaner = new ProcessTtlCleaner(assembly);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
  }

  private String createTerminal(String key, String phase) {
    ProcessResource created = createActive(key);
    ProcessTestFixtures.forceTerminal(assembly, created.name(), phase);
    return created.name();
  }

  @Test
  public void deletesOnlyFinalResourcesPastRetention() {
    String old = createTerminal("old", "SUCCESS");
    String fresh = createTerminal("fresh", "SUCCESS");
    String active = createActive("live").name(); // PENDING: never eligible

    // force `old` past the retention window by faking the clock 40 days ahead
    int deleted = cleaner.cleanOnce(Instant.now().plus(Duration.ofDays(40)), 30, 100);

    assertEquals(2, deleted, "old and fresh terminals both pass the 30d cutoff at clock+40d");
    assertFalse(assembly.indexProjection().current().find(old).isPresent());
    assertFalse(assembly.indexProjection().current().find(fresh).isPresent());
    assertTrue(
        assembly.indexProjection().current().find(active).isPresent(),
        "the active process is never touched");

    // at the real clock neither is old enough
    String kept = createTerminal("kept", "SUCCESS");
    int none = cleaner.cleanOnce(Instant.now(), 30, 100);
    assertEquals(0, none);
    assertTrue(assembly.indexProjection().current().find(kept).isPresent());
  }

  @Test
  public void batchesAreBounded() {
    for (int i = 0; i < 5; i++) {
      createTerminal("batch-" + i, "SUCCESS");
    }
    int deleted = cleaner.cleanOnce(Instant.now().plus(Duration.ofDays(40)), 30, 2);
    assertEquals(2, deleted, "the batch cap limits one round to two deletes");
    // the remaining terminals still stand; the next round picks them up
    int deletedAgain = cleaner.cleanOnce(Instant.now().plus(Duration.ofDays(40)), 30, 10);
    assertEquals(3, deletedAgain);
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> assembly.indexProjection().current().expiryOrder().isEmpty());
  }

  @Test
  public void ttlOrdersOffsetsAndFractionalInstantsByTimeRatherThanText() {
    ProcessResource offset = createActive("offset");
    forceTerminalAt(offset.name(), "2026-08-31T01:00:00+02:00");
    ProcessResource cutoff = createActive("cutoff");
    forceTerminalAt(cutoff.name(), "2026-08-31T00:00:00Z");
    ProcessResource fractional = createActive("fractional");
    forceTerminalAt(fractional.name(), "2026-08-31T00:00:00.001Z");

    int deleted = cleaner.cleanOnce(Instant.parse("2026-09-30T00:00:00Z"), 30, 10);

    assertEquals(2, deleted);
    assertFalse(assembly.indexProjection().current().find(offset.name()).isPresent());
    assertFalse(assembly.indexProjection().current().find(cutoff.name()).isPresent());
    assertTrue(assembly.indexProjection().current().find(fractional.name()).isPresent());
  }

  @Test
  public void pendingDurableHandleReleaseGatesTtlDeletion() {
    ProcessResource created = createActive("release");
    forceTerminalWithHandle(created.name(), "dummy-handle-1");
    Instant future = Instant.now().plus(Duration.ofDays(40));

    assertTrue(assembly.releaseIndex().hasPendingForProcess(created.name()));
    assertEquals(0, cleaner.cleanOnce(future, 30, 10));
    assertTrue(
        assembly.indexProjection().current().find(created.name()).isPresent(),
        "TTL must retain the durable Process while its engine handle still needs release");

    java.util.List<org.apache.amoro.process.engine.ExecutionHandleReleaseIndex.ReleaseEntry>
        claimed = assembly.releaseIndex().claimDue(future, 10);
    assertEquals(1, claimed.size());
    assembly.releaseIndex().releaseSucceeded(claimed.get(0));

    assertFalse(assembly.releaseIndex().hasPendingForProcess(created.name()));
    assertEquals(1, cleaner.cleanOnce(future, 30, 10));
    assertFalse(assembly.indexProjection().current().find(created.name()).isPresent());
  }

  private void forceTerminalWithHandle(String processName, String externalId) {
    ProcessResource current = assembly.repository().get(processName);
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    String now = Instant.now().toString();
    ProcessResource.ProcessAttempt closed =
        new ProcessResource.ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            "ACKNOWLEDGED",
            externalId,
            now,
            null,
            "FINAL",
            now,
            Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    ProcessResource.ProcessStatus terminal =
        new ProcessResource.ProcessStatus(
            "SUCCESS",
            status.retryNumber(),
            closed,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            status.conditions(),
            status.summary(),
            null,
            now,
            null,
            now);
    assembly
        .repository()
        .modify(processName, current.resourceVersion(), resource -> resource.withStatus(terminal));
  }

  private void forceTerminalAt(String processName, String finishedAt) {
    ProcessResource current = assembly.repository().get(processName);
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    ProcessResource.ProcessAttempt closed =
        new ProcessResource.ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            attempt.submitState(),
            attempt.externalId(),
            attempt.dispatchedAt(),
            null,
            "FINAL",
            finishedAt,
            attempt.submissionHistory(),
            attempt.manualResolutions());
    ProcessResource.ProcessStatus terminal =
        new ProcessResource.ProcessStatus(
            "SUCCESS",
            status.retryNumber(),
            closed,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            status.conditions(),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            finishedAt);
    assembly
        .repository()
        .modify(processName, current.resourceVersion(), resource -> resource.withStatus(terminal));
  }

  private ProcessResource createActive(String key) {
    String name = "ttl-" + key;
    String createdAt = Instant.now().toString();
    String requestHash = "sha256:request-" + key;
    ProcessResource.ProcessSpec spec =
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("prod", "db", "t-" + key, "table-" + key, "simulated"),
            "dummy-maintenance",
            "local",
            "MANUAL",
            createdAt,
            "RUN",
            new ProcessResource.RequestIdentity("sha256:idempotency-" + key, requestHash),
            Collections.singletonMap("simulated", true),
            new ProcessResource.RetryPolicy(3, 2, 30));
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            name + ":0:0",
            requestHash,
            "CREATED",
            null,
            null,
            null,
            "AUTO",
            null,
            Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    ProcessResource.ProcessStatus status =
        new ProcessResource.ProcessStatus(
            "PENDING",
            0,
            attempt,
            Collections.emptyList(),
            null,
            null,
            createdAt,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            null);
    return assembly.repository().create(new ProcessResource(name, spec, status));
  }
}
