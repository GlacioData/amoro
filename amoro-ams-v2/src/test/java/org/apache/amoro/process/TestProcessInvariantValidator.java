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
    assertDoesNotThrow(
        () -> validator.prepare(PersistenceChange.modified(created, canceled)));
  }

  @Test
  public void rejectsFrozenSpecMutationAndDesiredStateRegression() {
    ProcessResource created = resource("p1", spec("RUN"), status("PENDING", 0, null, null));
    ProcessResource.ProcessSpec changedAction =
        new ProcessResource.ProcessSpec(
            created.spec().table(),
            "clean-orphans",
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

  private void assertInvalid(ProcessResource.ProcessStatus invalidStatus) {
    ProcessResource invalid = resource("p1", spec("RUN"), invalidStatus);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.prepare(PersistenceChange.created(invalid)));
  }

  private static ProcessResource resource(
      String name,
      ProcessResource.ProcessSpec spec,
      ProcessResource.ProcessStatus status) {
    return new ProcessResource(name, spec, status);
  }

  private static ProcessResource.ProcessSpec spec(String desiredState) {
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("simulated", true);
    return new ProcessResource.ProcessSpec(
        new ProcessResource.TableRef("prod", "db", "table", "42"),
        "expire-snapshots",
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
