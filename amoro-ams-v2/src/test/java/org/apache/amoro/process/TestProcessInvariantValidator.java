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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.persistence.PersistenceChange;
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Domain-boundary validation must reject corrupt Process documents before durable I/O. */
public class TestProcessInvariantValidator {

  private final ProcessInvariantValidator validator = new ProcessInvariantValidator();

  @Test
  public void acceptsCreationAndTheOnlyLegalSpecMutation() {
    ProcessResource created = resource("p1", spec("RUN"), status("PENDING", 0, null, null));
    ProcessResource canceled = created.withSpec(spec("CANCEL"));

    assertDoesNotThrow(() -> validator.prepare(PersistenceChange.created(created)));
    assertDoesNotThrow(() -> validator.prepare(PersistenceChange.modified(created, canceled)));
  }

  @Test
  public void rejectsFrozenSpecMutationAndDesiredStateRegression() {
    ProcessResource created = resource("p1", spec("RUN"), status("PENDING", 0, null, null));
    ProcessResource.ProcessSpec changedAction =
        new ProcessResource.ProcessSpec(
            created.spec().table(),
            "dummy-secondary",
            created.spec().executionEngine(),
            created.spec().triggerSource(),
            created.spec().createdAt(),
            created.spec().desiredState(),
            created.spec().request(),
            created.spec().parameters(),
            created.spec().retryPolicy());
    ProcessResource canceled = created.withSpec(spec("CANCEL"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            validator.prepare(
                PersistenceChange.modified(created, created.withSpec(changedAction))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            validator.prepare(
                PersistenceChange.modified(canceled, canceled.withSpec(spec("RUN")))));
  }

  @Test
  public void rejectsInvalidPhaseRetryBudgetAndEngineBackoff() {
    assertInvalid(status("NOT_A_PHASE", 0, null, null));
    assertInvalid(status("PENDING", -1, null, null));
    assertInvalid(status("PENDING", 4, null, null));
    assertInvalid(
        new ProcessResource.ProcessStatus(
            "PENDING",
            0,
            null,
            null,
            null,
            null,
            new ProcessResource.EngineBackoff(8, 0, 0, 0),
            null,
            null,
            null,
            null,
            null,
            null));
  }

  @Test
  public void rejectsMismatchedAttemptIdentityAndHistoryOverflow() {
    ProcessResource.ProcessAttempt mismatched =
        new ProcessResource.ProcessAttempt(
            1,
            "other:0:1",
            "sha256:attempt",
            "DISPATCHING",
            null,
            "2026-08-22T10:00:01Z",
            "AUTO",
            null,
            null,
            new ProcessResource.ManualResolutions(null, null));
    assertInvalid(status("PENDING", 0, mismatched, null));

    List<ProcessResource.AttemptSummary> tooMany = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      tooMany.add(
          new ProcessResource.AttemptSummary(
              i,
              0,
              "p1:" + i + ":0",
              "sha256:" + i,
              "FAILED",
              null,
              "AUTO",
              null,
              null,
              "2026-08-22T10:00:01Z",
              "simulated"));
    }
    assertInvalid(status("PENDING", 0, null, tooMany));
  }

  @Test
  public void rejectsDuplicateOrMalformedConditions() {
    ProcessResource.Condition first =
        new ProcessResource.Condition(
            "EngineUnreachable",
            "True",
            "Unavailable",
            "simulated",
            "2026-08-22T10:00:00Z",
            "2026-08-22T10:00:00Z");
    ProcessResource.Condition duplicate =
        new ProcessResource.Condition(
            "EngineUnreachable",
            "False",
            "Recovered",
            "",
            "2026-08-22T10:00:01Z",
            "2026-08-22T10:00:01Z");
    assertInvalid(statusWithConditions(Arrays.asList(first, duplicate)));

    ProcessResource.Condition malformed =
        new ProcessResource.Condition(
            "SubmissionUnresolved",
            "maybe",
            "Unknown",
            "simulated",
            "2026-08-22T10:00:00Z",
            "2026-08-22T10:00:00Z");
    assertInvalid(statusWithConditions(java.util.Collections.singletonList(malformed)));
  }

  @Test
  public void rejectsInconsistentAttemptAndTopLevelFinalityMarkers() {
    ProcessResource.ProcessAttempt open =
        new ProcessResource.ProcessAttempt(
            0,
            "p1:0:0",
            "sha256:attempt",
            "ACKNOWLEDGED",
            "external-1",
            "2026-08-22T10:00:01Z",
            "ALLOW",
            null,
            null,
            new ProcessResource.ManualResolutions(null, null));
    ProcessResource partiallyMarkedLegacyTerminal =
        resource(
            "p1",
            spec("RUN"),
            new ProcessResource.ProcessStatus(
                "SUCCESS",
                0,
                open,
                null,
                null,
                null,
                new ProcessResource.EngineBackoff(0, 0, 0, 0),
                null,
                null,
                null,
                null,
                null,
                "2026-08-22T10:00:02Z"));
    assertDoesNotThrow(
        () -> validator.prepare(PersistenceChange.created(partiallyMarkedLegacyTerminal)),
        "postStart must load a terminal row whose only defect is a missing finality marker");

    ProcessResource.ProcessAttempt retryableFailed =
        new ProcessResource.ProcessAttempt(
            0,
            "p1:0:0",
            "sha256:attempt",
            "ACKNOWLEDGED",
            "external-1",
            "2026-08-22T10:00:01Z",
            "simulated failure",
            "ALLOW",
            "2026-08-22T10:00:02Z",
            null,
            new ProcessResource.ManualResolutions(null, null));
    assertInvalid(
        new ProcessResource.ProcessStatus(
            "FAILED",
            0,
            retryableFailed,
            null,
            null,
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            null,
            null,
            "must-not-be-top-level-yet",
            null,
            null,
            "2026-08-22T10:00:02Z"));
  }

  @Test
  public void legacyTerminalAllowsOnlyExactFinalityMarkerRepair() {
    ProcessResource legacy = resource("p1", spec("RUN"), status("SUCCESS", 0, null, null));
    ProcessResource repaired = exactLegacyRepair(legacy, "2026-08-22T10:00:03Z");

    assertDoesNotThrow(
        () -> validator.prepare(PersistenceChange.created(legacy)),
        "postStart must admit the narrowly repairable legacy terminal shape");
    assertDoesNotThrow(
        () -> validator.prepare(PersistenceChange.modified(legacy, repaired)),
        "the controller may fill only the missing finality markers and DataRepaired audit");

    ProcessResource legacyWithStaleRepairAudit =
        legacy.withStatus(
            new ProcessResource.ProcessStatus(
                "SUCCESS",
                0,
                null,
                null,
                null,
                null,
                null,
                new ProcessResource.EngineBackoff(0, 0, 0, 0),
                java.util.Collections.singletonList(
                    new ProcessResource.Condition(
                        "DataRepaired",
                        "False",
                        "LegacyImport",
                        "repair is still pending",
                        "2026-08-22T10:00:01Z",
                        "2026-08-22T10:00:01Z",
                        null)),
                null,
                null,
                null,
                null,
                null));
    assertDoesNotThrow(
        () -> validator.prepare(PersistenceChange.created(legacyWithStaleRepairAudit)));
    assertDoesNotThrow(
        () -> validator.prepare(PersistenceChange.modified(legacyWithStaleRepairAudit, repaired)),
        "an incomplete legacy terminal may canonically update its DataRepaired condition");

    ProcessResource.ProcessStatus repairedStatus = repaired.status();
    Map<String, Object> forgedResult = new LinkedHashMap<>();
    forgedResult.put("simulated", false);
    ProcessResource forged =
        repaired.withStatus(
            new ProcessResource.ProcessStatus(
                repairedStatus.phase(),
                repairedStatus.retryNumber(),
                repairedStatus.attempt(),
                repairedStatus.attemptHistory(),
                repairedStatus.lastObservedAt(),
                repairedStatus.lastCancelAttemptAt(),
                repairedStatus.nextReconcileAt(),
                repairedStatus.engineBackoffAttempts(),
                repairedStatus.conditions(),
                new ProcessResource.Summary(null, forgedResult),
                repairedStatus.failure(),
                repairedStatus.submittedAt(),
                repairedStatus.startedAt(),
                repairedStatus.finishedAt()));
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.prepare(PersistenceChange.modified(legacy, forged)),
        "DataRepaired must not authorize a simultaneous business-result mutation");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.prepare(PersistenceChange.modified(repaired, forged)),
        "a fully valid final Process is immutable");
    ProcessResource finalSpecMutation =
        repaired.withSpec(repaired.spec().withDesiredState("CANCEL"));
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.prepare(PersistenceChange.modified(repaired, finalSpecMutation)),
        "final immutability also covers desiredState and the complete spec");
  }

  @Test
  public void dataRepairedCannotHideAttemptHistoryRewrite() {
    ProcessResource.AttemptSummary originalSummary = archivedFailure("original failure");
    ProcessResource previous =
        resource(
            "p1",
            spec("RUN"),
            retriedStatus(java.util.Collections.singletonList(originalSummary), null));
    ProcessResource.AttemptSummary rewrittenSummary = archivedFailure("rewritten failure");
    List<ProcessResource.Condition> forgedAudit =
        java.util.Collections.singletonList(
            new ProcessResource.Condition(
                "DataRepaired",
                "True",
                "FinalityMarkersRepaired",
                "missing finality markers were reconstructed",
                "2026-08-22T10:00:03Z",
                "2026-08-22T10:00:03Z",
                null));
    ProcessResource current =
        resource(
            "p1",
            spec("RUN"),
            retriedStatus(java.util.Collections.singletonList(rewrittenSummary), forgedAudit));

    assertDoesNotThrow(() -> validator.prepare(PersistenceChange.created(previous)));
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.prepare(PersistenceChange.modified(previous, current)),
        "history is append-only and DataRepaired is not a general repair capability");

    ProcessResource.AttemptSummary wrongIdentity =
        new ProcessResource.AttemptSummary(
            0,
            0,
            "another-process:0:0",
            "sha256:attempt-0",
            "FAILED",
            null,
            "ALLOW",
            java.util.Collections.emptyList(),
            null,
            "2026-08-22T10:00:01Z",
            "original failure");
    assertInvalid(retriedStatus(java.util.Collections.singletonList(wrongIdentity), null));

    assertInvalid(
        statusWithConditions(
            java.util.Collections.singletonList(
                new ProcessResource.Condition(
                    "DataRepaired",
                    "True",
                    "FinalityMarkersRepaired",
                    "missing finality markers were reconstructed",
                    "2026-08-22T10:00:03Z",
                    "2026-08-22T10:00:03Z",
                    null))));
  }

  private static ProcessResource exactLegacyRepair(ProcessResource legacy, String repairedAt) {
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            legacy.name() + ":0:0",
            ProcessRequestHashes.actionAttempt(legacy.name(), 0, legacy.spec()),
            "CREATED",
            null,
            null,
            null,
            "FINAL",
            repairedAt,
            java.util.Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    ProcessResource.Condition dataRepaired =
        new ProcessResource.Condition(
            "DataRepaired",
            "True",
            "FinalityMarkersRepaired",
            "missing finality markers were reconstructed",
            repairedAt,
            repairedAt,
            null);
    return legacy.withStatus(
        new ProcessResource.ProcessStatus(
            "SUCCESS",
            0,
            attempt,
            java.util.Collections.emptyList(),
            null,
            null,
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            java.util.Collections.singletonList(dataRepaired),
            null,
            null,
            null,
            null,
            repairedAt));
  }

  private static ProcessResource.ProcessStatus retriedStatus(
      List<ProcessResource.AttemptSummary> history, List<ProcessResource.Condition> conditions) {
    ProcessResource.ProcessAttempt currentAttempt =
        new ProcessResource.ProcessAttempt(
            0,
            "p1:1:0",
            "sha256:attempt-1",
            "CREATED",
            null,
            null,
            null,
            "AUTO",
            null,
            java.util.Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    return new ProcessResource.ProcessStatus(
        "PENDING",
        1,
        currentAttempt,
        history,
        null,
        null,
        "2026-08-22T10:00:02Z",
        new ProcessResource.EngineBackoff(0, 0, 0, 0),
        conditions,
        null,
        null,
        null,
        null,
        null);
  }

  private static ProcessResource.AttemptSummary archivedFailure(String reason) {
    return new ProcessResource.AttemptSummary(
        0,
        0,
        "p1:0:0",
        "sha256:attempt-0",
        "FAILED",
        null,
        "ALLOW",
        java.util.Collections.emptyList(),
        null,
        "2026-08-22T10:00:01Z",
        reason);
  }

  private void assertInvalid(ProcessResource.ProcessStatus invalidStatus) {
    ProcessResource invalid = resource("p1", spec("RUN"), invalidStatus);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.prepare(PersistenceChange.created(invalid)));
  }

  private static ProcessResource resource(
      String name, ProcessResource.ProcessSpec spec, ProcessResource.ProcessStatus status) {
    return new ProcessResource(name, spec, status);
  }

  private static ProcessResource.ProcessSpec spec(String desiredState) {
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("simulated", true);
    return new ProcessResource.ProcessSpec(
        new ProcessResource.TableRef("prod", "db", "table", "42", "simulated"),
        "dummy-maintenance",
        "local",
        "MANUAL",
        "2026-08-22T10:00:00Z",
        desiredState,
        new ProcessResource.RequestIdentity("sha256:key", "sha256:request"),
        parameters,
        new ProcessResource.RetryPolicy(3, 2, 30));
  }

  private static ProcessResource.ProcessStatus status(
      String phase,
      int retryNumber,
      ProcessResource.ProcessAttempt attempt,
      List<ProcessResource.AttemptSummary> history) {
    return new ProcessResource.ProcessStatus(
        phase,
        retryNumber,
        attempt,
        history,
        null,
        null,
        new ProcessResource.EngineBackoff(0, 0, 0, 0),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static ProcessResource.ProcessStatus statusWithConditions(
      List<ProcessResource.Condition> conditions) {
    return new ProcessResource.ProcessStatus(
        "PENDING",
        0,
        null,
        null,
        null,
        null,
        new ProcessResource.EngineBackoff(0, 0, 0, 0),
        conditions,
        null,
        null,
        null,
        null,
        null);
  }
}
