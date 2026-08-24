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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/** Attempt-bound resolution, audit and replay semantics independent of the REST transport. */
public class TestManualResolutionTransition {

  private static final String NOW = "2026-08-24T01:00:00Z";

  @Test
  public void submissionNotFoundRotatesGenerationAndArchivedCommandReplays() {
    ProcessResource original = resource("submission", false);
    ManualResolutionTransition.Command command =
        command(
            ManualResolutionTransition.Kind.SUBMISSION,
            "resolution-key",
            "submission:0:0",
            "NOT_FOUND",
            null,
            null,
            "remote ledger checked");

    ManualResolutionTransition.Result resolved =
        ManualResolutionTransition.apply(original, command, NOW);

    assertEquals("PENDING", resolved.status().phase());
    assertEquals(1, resolved.status().attempt().dispatchGeneration());
    assertEquals("submission:0:1", resolved.status().attempt().submissionKey());
    ProcessResource.ManualResolution audit =
        resolved.status().attempt().submissionHistory().get(0).manualResolution();
    assertNotEquals("resolution-key", audit.idempotencyKeyHash());
    assertTrue(audit.idempotencyKeyHash().startsWith("sha256:"));

    ProcessResource rotated = original.withStatus(resolved.status());
    assertTrue(ManualResolutionTransition.apply(rotated, command, NOW).replayed());
  }

  @Test
  public void idempotencyKeyCannotChangeCommandAndIdentityCannotTakeAnotherKey() {
    ProcessResource original = resource("conflict", false);
    ManualResolutionTransition.Command first =
        command(
            ManualResolutionTransition.Kind.SUBMISSION,
            "one-key",
            "conflict:0:0",
            "ACKNOWLEDGED",
            "dummy-1",
            null,
            "verified");
    ProcessResource resolved =
        original.withStatus(ManualResolutionTransition.apply(original, first, NOW).status());

    ProcessCommandException changedPayload =
        assertThrows(
            ProcessCommandException.class,
            () ->
                ManualResolutionTransition.apply(
                    resolved,
                    command(
                        ManualResolutionTransition.Kind.SUBMISSION,
                        "one-key",
                        "conflict:0:0",
                        "ACKNOWLEDGED",
                        "dummy-2",
                        null,
                        "verified"),
                    NOW));
    assertEquals(ProcessCommandException.Code.IDEMPOTENCY_KEY_REUSED, changedPayload.code());

    ProcessCommandException differentKey =
        assertThrows(
            ProcessCommandException.class,
            () ->
                ManualResolutionTransition.apply(
                    resolved,
                    command(
                        ManualResolutionTransition.Kind.SUBMISSION,
                        "another-key",
                        "conflict:0:0",
                        "ACKNOWLEDGED",
                        "dummy-1",
                        null,
                        "verified"),
                    NOW));
    assertEquals(ProcessCommandException.Code.SUBMISSION_RESOLUTION_CONFLICT, differentKey.code());
  }

  @Test
  public void retryableExecutionFailureClosesOnlyTheAttempt() {
    ProcessResource original = resource("execution", true);

    ManualResolutionTransition.Result result =
        ManualResolutionTransition.apply(
            original,
            command(
                ManualResolutionTransition.Kind.EXECUTION,
                "execution-key",
                "execution:0:0",
                "FAILED",
                null,
                true,
                "dummy execution lost"),
            NOW);

    assertEquals("FAILED", result.status().phase());
    assertEquals("ALLOW", result.status().attempt().retryDisposition());
    assertEquals(NOW, result.status().attempt().finishedAt());
    assertNull(result.status().failure());
    assertNull(result.status().finishedAt());
    assertFalse(
        ProcessConditions.isTrue(
            result.status().conditions(), ProcessConditions.EXECUTION_UNRESOLVED));
  }

  private static ManualResolutionTransition.Command command(
      ManualResolutionTransition.Kind kind,
      String idempotencyKey,
      String submissionKey,
      String outcome,
      String externalId,
      Boolean retryAllowed,
      String reason) {
    return new ManualResolutionTransition.Command(
        kind,
        idempotencyKey,
        submissionKey,
        "sha256:req",
        outcome,
        externalId,
        retryAllowed,
        reason,
        "test-operator");
  }

  private static ProcessResource resource(String name, boolean executionUnresolved) {
    String externalId = executionUnresolved ? "dummy-execution" : null;
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            name + ":0:0",
            "sha256:req",
            executionUnresolved ? "ACKNOWLEDGED" : "DISPATCHING",
            externalId,
            "2026-08-24T00:59:00Z",
            null,
            "AUTO",
            null,
            Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    String condition =
        executionUnresolved
            ? ProcessConditions.EXECUTION_UNRESOLVED
            : ProcessConditions.SUBMISSION_UNRESOLVED;
    return new ProcessResource(
        name,
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("sim", "db", name, name, "simulated"),
            "dummy-maintenance",
            "local",
            "MANUAL",
            "2026-08-24T00:00:00Z",
            "RUN",
            new ProcessResource.RequestIdentity("sha256:create-key", "sha256:req"),
            Collections.emptyMap(),
            new ProcessResource.RetryPolicy(1, 2, 1)),
        new ProcessResource.ProcessStatus(
            executionUnresolved ? "SUBMITTED" : "PENDING",
            0,
            attempt,
            Collections.emptyList(),
            null,
            null,
            NOW,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            ProcessConditions.set(
                Collections.emptyList(),
                condition,
                "Test",
                "simulated unresolved state",
                NOW,
                null),
            null,
            null,
            executionUnresolved ? NOW : null,
            null,
            null));
  }
}
