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

import org.apache.amoro.persistence.DurableStateProjection;
import org.apache.amoro.persistence.PersistenceChange;
import org.apache.amoro.persistence.PreparedProjectionUpdate;
import org.apache.amoro.resources.ProcessResource;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Fail-closed Process domain validation. It participates in persistence projection preparation so
 * every create/modify is validated on the mutation lane before any durable I/O or index update.
 */
public final class ProcessInvariantValidator implements DurableStateProjection<ProcessResource> {

  private static final PreparedProjectionUpdate NO_OP = () -> {};
  private static final Set<String> PHASES =
      Set.of(
          "UNKNOWN",
          "PENDING",
          "SUBMITTED",
          "RUNNING",
          "CANCELING",
          "SUCCESS",
          "CANCELED",
          "CLOSED",
          "KILLED",
          "FAILED");
  private static final Set<String> SUBMIT_STATES =
      Set.of(
          "CREATED",
          "DISPATCHING",
          "ACKNOWLEDGED",
          "REJECTED",
          "UNKNOWN",
          "CONFLICT",
          "UNAVAILABLE");
  private static final Set<String> RETRY_DISPOSITIONS = Set.of("AUTO", "ALLOW", "FINAL");

  @Override
  public PreparedProjectionUpdate prepare(PersistenceChange<ProcessResource> change) {
    Objects.requireNonNull(change, "change");
    ProcessResource current = change.current();
    if (current == null) {
      return NO_OP;
    }
    validateResource(current);
    if (change.previous() != null) {
      require(change.previous().name().equals(current.name()), "process name is immutable");
      validateSpecTransition(change.previous().spec(), current.spec());
      if (ProcessFinality.isFinal(change.previous())) {
        require(
            change.previous().spec().equals(current.spec()), "a final Process spec is immutable");
      }
      validateStatusTransition(change.previous(), current);
    }
    return NO_OP;
  }

  private static void validateResource(ProcessResource resource) {
    require(ProcessResource.API_VERSION.equals(resource.apiVersion()), "invalid apiVersion");
    require(ProcessResource.COLLECTION.equals(resource.collection()), "invalid collection");
    require(!isBlank(resource.name()), "process name must not be blank");

    ProcessResource.ProcessSpec spec = resource.spec();
    require(!isBlank(spec.table().catalog()), "table catalog must not be blank");
    require(!isBlank(spec.table().database()), "table database must not be blank");
    require(!isBlank(spec.table().table()), "table name must not be blank");
    require(!isBlank(spec.table().tableId()), "tableId must not be blank");
    require(
        spec.table().tableFormat().matches("[a-z][a-z0-9]*(?:-[a-z0-9]+)*"),
        "tableFormat must be canonical lower-kebab-case");
    require(
        spec.action().matches("[a-z][a-z0-9]*(?:-[a-z0-9]+)*"),
        "action must be canonical lower-kebab-case");
    require(!isBlank(spec.executionEngine()), "executionEngine must not be blank");
    require(
        "MANUAL".equals(spec.triggerSource()) || "SCHEDULED".equals(spec.triggerSource()),
        "triggerSource must be MANUAL or SCHEDULED");
    require(
        "RUN".equals(spec.desiredState()) || "CANCEL".equals(spec.desiredState()),
        "desiredState must be RUN or CANCEL");
    require(!isBlank(spec.createdAt()), "createdAt must not be blank");
    requireInstant(spec.createdAt(), "createdAt");
    require(!isBlank(spec.request().idempotencyKeyHash()), "idempotencyKeyHash must not be blank");
    require(
        spec.request().requestHash() != null && spec.request().requestHash().startsWith("sha256:"),
        "creation requestHash must be a sha256 identity");
    require(
        spec.request().idempotencyKeyHash().startsWith("sha256:"),
        "idempotencyKeyHash must be a sha256 identity");

    ProcessResource.RetryPolicy policy = spec.retryPolicy();
    require(inRange(policy.maxRetries(), 0, 3), "maxRetries must be in [0,3]");
    require(inRange(policy.maxSubmissionRetries(), 0, 2), "maxSubmissionRetries must be in [0,2]");
    require(
        inRange(policy.retryDelaySeconds(), 1, 86_400), "retryDelaySeconds must be in [1,86400]");

    validateStatus(resource);
  }

  private static void validateSpecTransition(
      ProcessResource.ProcessSpec previous, ProcessResource.ProcessSpec current) {
    require(previous.table().equals(current.table()), "table is immutable");
    require(previous.action().equals(current.action()), "action is immutable");
    require(
        previous.executionEngine().equals(current.executionEngine()),
        "executionEngine is immutable");
    require(previous.triggerSource().equals(current.triggerSource()), "triggerSource is immutable");
    require(previous.createdAt().equals(current.createdAt()), "createdAt is immutable");
    require(previous.request().equals(current.request()), "request identity is immutable");
    require(previous.parameters().equals(current.parameters()), "parameters are immutable");
    require(previous.retryPolicy().equals(current.retryPolicy()), "retryPolicy is immutable");

    String before = previous.desiredState();
    String after = current.desiredState();
    require(
        before.equals(after) || ("RUN".equals(before) && "CANCEL".equals(after)),
        "desiredState is monotonic and only allows RUN -> CANCEL");
  }

  private static void validateStatus(ProcessResource resource) {
    ProcessResource.ProcessSpec spec = resource.spec();
    ProcessResource.ProcessStatus status = resource.status();
    require(PHASES.contains(status.phase()), "unknown process phase " + status.phase());
    require(
        inRange(status.retryNumber(), 0, spec.retryPolicy().maxRetries()),
        "retryNumber exceeds retry policy");
    require(
        status.attemptHistory().size() <= spec.retryPolicy().maxRetries(),
        "attempt history exceeds retry policy");
    require(
        status.attemptHistory().size() == status.retryNumber(),
        "attempt history size must equal retryNumber");
    for (int retry = 0; retry < status.attemptHistory().size(); retry++) {
      validateAttemptSummary(resource, status.attemptHistory().get(retry), retry);
    }

    ProcessResource.EngineBackoff backoff = status.engineBackoffAttempts();
    require(inRange(backoff.submit(), 0, 7), "submit backoff must be in [0,7]");
    require(inRange(backoff.resolve(), 0, 7), "resolve backoff must be in [0,7]");
    require(inRange(backoff.observe(), 0, 7), "observe backoff must be in [0,7]");
    require(inRange(backoff.cancel(), 0, 7), "cancel backoff must be in [0,7]");

    require(status.conditions().size() <= 8, "conditions must contain at most 8 entries");
    requireOptionalInstant(status.lastObservedAt(), "lastObservedAt");
    requireOptionalInstant(status.lastCancelAttemptAt(), "lastCancelAttemptAt");
    requireOptionalInstant(status.nextReconcileAt(), "nextReconcileAt");
    requireOptionalInstant(status.submittedAt(), "submittedAt");
    requireOptionalInstant(status.startedAt(), "startedAt");
    requireOptionalInstant(status.finishedAt(), "finishedAt");
    Set<String> conditionTypes = new HashSet<>();
    for (ProcessResource.Condition condition : status.conditions()) {
      require(conditionTypes.add(condition.type()), "condition types must be unique");
      require(
          "True".equals(condition.status()) || "False".equals(condition.status()),
          "condition status must be True or False");
      requireOptionalInstant(condition.lastTransitionTime(), "condition.lastTransitionTime");
      requireOptionalInstant(condition.lastUpdateTime(), "condition.lastUpdateTime");
    }

    ProcessResource.ProcessAttempt attempt = status.attempt();
    boolean repairableLegacyFinal = isRepairableLegacyFinal(resource);
    validatePersistedDataRepair(resource, repairableLegacyFinal);
    if (attempt == null) {
      if (repairableLegacyFinal) {
        return;
      }
      require(
          !ProcessFinality.isFinal(resource), "a final Process requires a closed current attempt");
      require(status.failure() == null, "a non-final Process must not carry top-level failure");
      require(status.finishedAt() == null, "a non-final Process must not carry finishedAt");
      return;
    }
    require(
        inRange(attempt.dispatchGeneration(), 0, spec.retryPolicy().maxSubmissionRetries()),
        "dispatchGeneration exceeds submission retry policy");
    require(
        attempt.submissionHistory().size() <= spec.retryPolicy().maxSubmissionRetries(),
        "submission history exceeds retry policy");
    require(
        attempt.submissionHistory().size() == attempt.dispatchGeneration(),
        "submission history size must equal dispatchGeneration");
    validateSubmissionHistory(
        resource.name(), status.retryNumber(), attempt.requestHash(), attempt.submissionHistory());
    require(
        (resource.name() + ":" + status.retryNumber() + ":" + attempt.dispatchGeneration())
            .equals(attempt.submissionKey()),
        "submissionKey does not match process/retry/generation identity");
    require(
        attempt.requestHash() != null && attempt.requestHash().startsWith("sha256:"),
        "attempt requestHash must be a sha256 identity");
    require(SUBMIT_STATES.contains(attempt.submitState()), "invalid submitState");
    require(RETRY_DISPOSITIONS.contains(attempt.retryDisposition()), "invalid retryDisposition");
    requireOptionalInstant(attempt.dispatchedAt(), "attempt.dispatchedAt");
    requireOptionalInstant(attempt.finishedAt(), "attempt.finishedAt");
    if (attempt.externalId() != null) {
      require(
          "ACKNOWLEDGED".equals(attempt.submitState()),
          "externalId requires ACKNOWLEDGED submitState");
    }
    if ("CREATED".equals(attempt.submitState())) {
      require(attempt.dispatchedAt() == null, "CREATED must not carry dispatchedAt");
    }
    if ("DISPATCHING".equals(attempt.submitState())
        || "ACKNOWLEDGED".equals(attempt.submitState())
        || "REJECTED".equals(attempt.submitState())
        || "UNKNOWN".equals(attempt.submitState())
        || "CONFLICT".equals(attempt.submitState())) {
      require(attempt.dispatchedAt() != null, attempt.submitState() + " requires dispatchedAt");
    }
    if ("ACKNOWLEDGED".equals(attempt.submitState())) {
      require(!isBlank(attempt.externalId()), "ACKNOWLEDGED requires externalId");
    }
    if ("SUBMITTED".equals(status.phase()) || "RUNNING".equals(status.phase())) {
      require(
          "ACKNOWLEDGED".equals(attempt.submitState()) && !isBlank(attempt.externalId()),
          status.phase() + " requires an acknowledged external execution");
    }
    validateManualResolutions(attempt);

    boolean fixedTerminal = ProcessFinality.isFixedTerminal(status.phase());
    boolean finalProcess = ProcessFinality.isFinal(resource);
    if (fixedTerminal) {
      if (repairableLegacyFinal) {
        return;
      }
      require(attempt.finishedAt() != null, "fixed terminal requires attempt.finishedAt");
      require(status.finishedAt() != null, "fixed terminal requires status.finishedAt");
      require(status.failure() == null, "fixed terminal must not carry failure");
    } else if ("FAILED".equals(status.phase())) {
      if (repairableLegacyFinal) {
        return;
      }
      require(attempt.finishedAt() != null, "FAILED requires attempt.finishedAt");
      require(!isBlank(attempt.lastError()), "FAILED requires attempt.lastError");
      if (finalProcess) {
        require(!isBlank(status.failure()), "final FAILED requires top-level failure");
        require(status.finishedAt() != null, "final FAILED requires status.finishedAt");
      } else {
        require(status.failure() == null, "retryable FAILED must not carry top-level failure");
        require(status.finishedAt() == null, "retryable FAILED must not carry status.finishedAt");
      }
    } else {
      require(attempt.finishedAt() == null, "active phase must not close the current attempt");
      require(status.failure() == null, "active phase must not carry top-level failure");
      require(status.finishedAt() == null, "active phase must not carry status.finishedAt");
    }
  }

  private static void validateStatusTransition(ProcessResource previous, ProcessResource current) {
    ProcessResource.ProcessStatus before = previous.status();
    ProcessResource.ProcessStatus after = current.status();
    require(
        after.retryNumber() == before.retryNumber()
            || after.retryNumber() == before.retryNumber() + 1,
        "retryNumber may only remain stable or increment by one");
    if (ProcessFinality.isFinal(previous)) {
      if (!before.equals(after)) {
        require(isRepairableLegacyFinal(previous), "a valid final Process is immutable");
        validateExactFinalityRepair(previous, current);
      }
      return;
    }
    if (after.retryNumber() == before.retryNumber() + 1) {
      require("FAILED".equals(before.phase()), "retry may only archive a FAILED attempt");
      require(before.attempt() != null, "retry requires a current attempt");
      require(before.attempt().finishedAt() != null, "retry requires a closed attempt");
      require(
          !"FINAL".equals(before.attempt().retryDisposition()),
          "FINAL retryDisposition cannot be retried");
      require("RUN".equals(previous.spec().desiredState()), "CANCEL desired cannot retry");
      require(!ProcessFinality.isFinal(previous), "a final FAILED Process cannot retry");
      require("PENDING".equals(after.phase()), "retry must open PENDING");
      require(after.attempt() != null, "retry must create a new attempt");
      require(
          "CREATED".equals(after.attempt().submitState())
              && after.attempt().dispatchGeneration() == 0,
          "retry must open generation zero in CREATED");
      require(
          after.attemptHistory().size() == before.attemptHistory().size() + 1,
          "retry must append exactly one attempt summary");
      require(
          after
              .attemptHistory()
              .subList(0, before.attemptHistory().size())
              .equals(before.attemptHistory()),
          "retry must preserve prior attempt history");
      require(
          archiveOf(previous).equals(after.attemptHistory().get(after.attemptHistory().size() - 1)),
          "retry summary must exactly archive the closed attempt");
    } else {
      require(
          before.attemptHistory().equals(after.attemptHistory()),
          "attempt history may only change during retry");
    }
    if (before.attempt() != null && before.attempt().finishedAt() != null) {
      boolean archivedAndRetried = after.retryNumber() == before.retryNumber() + 1;
      require(
          archivedAndRetried || (after.attempt() != null && after.attempt().finishedAt() != null),
          "a closed attempt cannot be reopened");
    }
  }

  private static void validateAttemptSummary(
      ProcessResource resource, ProcessResource.AttemptSummary summary, int expectedRetry) {
    require(summary.retryNumber() == expectedRetry, "attempt history retryNumber is not ordered");
    require(
        inRange(
            summary.dispatchGeneration(), 0, resource.spec().retryPolicy().maxSubmissionRetries()),
        "archived dispatchGeneration exceeds policy");
    require(
        (resource.name() + ":" + expectedRetry + ":" + summary.dispatchGeneration())
            .equals(summary.submissionKey()),
        "archived submissionKey identity mismatch");
    require(
        summary.requestHash() != null && summary.requestHash().startsWith("sha256:"),
        "archived requestHash must be sha256");
    require("FAILED".equals(summary.outcome()), "only FAILED attempts may be archived for retry");
    require(
        RETRY_DISPOSITIONS.contains(summary.retryDisposition()),
        "invalid archived retryDisposition");
    require(
        !"FINAL".equals(summary.retryDisposition()), "FINAL attempt cannot enter retry history");
    require(!isBlank(summary.reason()), "archived FAILED attempt requires reason");
    requireOptionalInstant(summary.finishedAt(), "attemptHistory.finishedAt");
    require(summary.finishedAt() != null, "archived attempt requires finishedAt");
    require(
        summary.submissionHistory().size() == summary.dispatchGeneration(),
        "archived submission history size must equal dispatchGeneration");
    validateSubmissionHistory(
        resource.name(), expectedRetry, summary.requestHash(), summary.submissionHistory());
    if (summary.manualResolutions() != null) {
      validateManualResolutionIdentity(
          summary.manualResolutions().submission(), summary.submissionKey(), summary.requestHash());
      validateManualResolutionIdentity(
          summary.manualResolutions().execution(), summary.submissionKey(), summary.requestHash());
    }
  }

  private static void validateSubmissionHistory(
      String processName,
      int retryNumber,
      String requestHash,
      java.util.List<ProcessResource.SubmissionSummary> history) {
    for (int generation = 0; generation < history.size(); generation++) {
      ProcessResource.SubmissionSummary summary = history.get(generation);
      require(
          summary.dispatchGeneration() == generation,
          "submission history generations must be ordered from zero");
      require(
          (processName + ":" + retryNumber + ":" + generation).equals(summary.submissionKey()),
          "submission history key identity mismatch");
      require(requestHash.equals(summary.requestHash()), "submission history requestHash mismatch");
      require(
          "NOT_FOUND".equals(summary.outcome()), "archived submission outcome must be NOT_FOUND");
      requireOptionalInstant(summary.finishedAt(), "submissionHistory.finishedAt");
      require(summary.finishedAt() != null, "archived submission requires finishedAt");
      validateManualResolutionIdentity(
          summary.manualResolution(), summary.submissionKey(), summary.requestHash());
    }
  }

  private static void validateManualResolutionIdentity(
      ProcessResource.ManualResolution resolution, String submissionKey, String requestHash) {
    if (resolution == null) {
      return;
    }
    require(
        submissionKey.equals(resolution.submissionKey())
            && requestHash.equals(resolution.requestHash()),
        "archived manual resolution identity mismatch");
    validateManualResolutionShape(resolution);
  }

  private static ProcessResource.AttemptSummary archiveOf(ProcessResource resource) {
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    return new ProcessResource.AttemptSummary(
        status.retryNumber(),
        attempt.dispatchGeneration(),
        attempt.submissionKey(),
        attempt.requestHash(),
        "FAILED",
        attempt.externalId(),
        attempt.retryDisposition(),
        attempt.submissionHistory(),
        attempt.manualResolutions(),
        attempt.finishedAt(),
        attempt.lastError());
  }

  private static void validateManualResolutions(ProcessResource.ProcessAttempt attempt) {
    ProcessResource.ManualResolutions resolutions = attempt.manualResolutions();
    if (resolutions == null) {
      return;
    }
    validateManualResolution(resolutions.submission(), attempt, true);
    validateManualResolution(resolutions.execution(), attempt, false);
    for (ProcessResource.SubmissionSummary summary : attempt.submissionHistory()) {
      if (summary.manualResolution() != null) {
        require(
            summary.submissionKey().equals(summary.manualResolution().submissionKey())
                && summary.requestHash().equals(summary.manualResolution().requestHash()),
            "archived submission audit identity mismatch");
        validateManualResolutionShape(summary.manualResolution());
      }
    }
  }

  private static void validateManualResolution(
      ProcessResource.ManualResolution resolution,
      ProcessResource.ProcessAttempt attempt,
      boolean submission) {
    if (resolution == null) {
      return;
    }
    require(
        attempt.submissionKey().equals(resolution.submissionKey())
            && attempt.requestHash().equals(resolution.requestHash()),
        "manual resolution identity mismatch");
    if (submission) {
      require(
          "ACKNOWLEDGED".equals(resolution.outcome()) || "NOT_FOUND".equals(resolution.outcome()),
          "invalid manual submission outcome");
    } else {
      require(
          ProcessFinality.isFixedTerminal(resolution.outcome())
              || "FAILED".equals(resolution.outcome()),
          "invalid manual execution outcome");
    }
    validateManualResolutionShape(resolution);
  }

  private static void validateManualResolutionShape(ProcessResource.ManualResolution resolution) {
    require(
        resolution.idempotencyKeyHash().startsWith("sha256:"),
        "manual idempotencyKeyHash must be sha256");
    require(resolution.commandHash().startsWith("sha256:"), "manual commandHash must be sha256");
    require(!isBlank(resolution.reason()), "manual resolution reason is required");
    require(!isBlank(resolution.operatorContext()), "manual operator context is required");
    requireInstant(resolution.resolvedAt(), "manualResolution.resolvedAt");
  }

  private static boolean isRepairableLegacyFinal(ProcessResource resource) {
    ProcessResource.ProcessStatus status = resource.status();
    if (!ProcessFinality.isFinal(resource)) {
      return false;
    }
    if (ProcessFinality.isFixedTerminal(status.phase()) && status.failure() != null) {
      return false;
    }
    ProcessResource.ProcessAttempt attempt = status.attempt();
    return attempt == null
        || attempt.finishedAt() == null
        || status.finishedAt() == null
        || ("FAILED".equals(status.phase())
            && (attempt.lastError() == null || status.failure() == null));
  }

  private static void validatePersistedDataRepair(
      ProcessResource resource, boolean repairableLegacyFinal) {
    java.util.Optional<ProcessResource.Condition> marker =
        ProcessConditions.find(resource.status().conditions(), ProcessConditions.DATA_REPAIRED);
    if (marker.isEmpty() || repairableLegacyFinal) {
      return;
    }
    ProcessResource.Condition repaired = marker.get();
    require(ProcessFinality.isFinal(resource), "DataRepaired is only valid on a final Process");
    require("True".equals(repaired.status()), "DataRepaired must be True");
    require(
        "FinalityMarkersRepaired".equals(repaired.reason()),
        "DataRepaired reason must identify finality marker repair");
    require(
        "missing finality markers were reconstructed".equals(repaired.message()),
        "DataRepaired message must identify finality marker repair");
    require(
        repaired.observedCapabilityVersion() == null,
        "DataRepaired must not carry a capability version");
    require(
        Objects.equals(repaired.lastTransitionTime(), repaired.lastUpdateTime()),
        "DataRepaired timestamps must identify one atomic repair");
    require(
        Objects.equals(resource.status().finishedAt(), repaired.lastUpdateTime()),
        "DataRepaired timestamp must match the final Process timestamp");
  }

  private static void validateExactFinalityRepair(
      ProcessResource previous, ProcessResource current) {
    ProcessResource.ProcessStatus before = previous.status();
    ProcessResource.ProcessStatus after = current.status();
    require(before.phase().equals(after.phase()), "finality repair must preserve phase");
    require(
        before.retryNumber() == after.retryNumber(), "finality repair must preserve retryNumber");
    require(
        before.attemptHistory().equals(after.attemptHistory()),
        "finality repair must preserve attempt history");
    require(
        Objects.equals(before.lastObservedAt(), after.lastObservedAt()),
        "finality repair must preserve lastObservedAt");
    require(
        Objects.equals(before.lastCancelAttemptAt(), after.lastCancelAttemptAt()),
        "finality repair must preserve lastCancelAttemptAt");
    require(after.nextReconcileAt() == null, "finality repair must clear nextReconcileAt");
    require(
        before.engineBackoffAttempts().equals(after.engineBackoffAttempts()),
        "finality repair must preserve engine backoff");
    require(
        Objects.equals(before.summary(), after.summary()),
        "finality repair must preserve result summary");
    require(
        Objects.equals(before.submittedAt(), after.submittedAt()),
        "finality repair must preserve submittedAt");
    require(
        Objects.equals(before.startedAt(), after.startedAt()),
        "finality repair must preserve startedAt");

    ProcessResource.Condition repaired =
        ProcessConditions.find(after.conditions(), ProcessConditions.DATA_REPAIRED)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "invalid Process resource: finality repair requires DataRepaired"));
    require("True".equals(repaired.status()), "DataRepaired must be True");
    require(
        "FinalityMarkersRepaired".equals(repaired.reason()),
        "DataRepaired reason must identify finality marker repair");
    require(
        "missing finality markers were reconstructed".equals(repaired.message()),
        "DataRepaired message must identify finality marker repair");
    require(
        repaired.observedCapabilityVersion() == null,
        "DataRepaired must not carry a capability version");
    require(
        Objects.equals(repaired.lastTransitionTime(), repaired.lastUpdateTime()),
        "DataRepaired timestamps must identify one atomic repair");
    require(
        withoutDataRepaired(before.conditions()).equals(withoutDataRepaired(after.conditions())),
        "finality repair must preserve all other conditions");

    String repairedAt = repaired.lastUpdateTime();
    String expectedRepairTimestamp =
        before.finishedAt() != null
            ? before.finishedAt()
            : before.attempt() != null && before.attempt().finishedAt() != null
                ? before.attempt().finishedAt()
                : repairedAt;
    require(
        expectedRepairTimestamp.equals(repairedAt),
        "DataRepaired timestamp must equal the reconstructed terminal timestamp");

    String expectedAttemptFinishedAt =
        before.attempt() != null && before.attempt().finishedAt() != null
            ? before.attempt().finishedAt()
            : before.finishedAt() != null ? before.finishedAt() : repairedAt;
    String expectedStatusFinishedAt =
        before.finishedAt() != null ? before.finishedAt() : expectedAttemptFinishedAt;

    ProcessResource.ProcessAttempt expectedAttempt =
        expectedRepairedAttempt(previous, expectedAttemptFinishedAt);
    require(
        expectedAttempt.equals(after.attempt()),
        "finality repair may only complete the current attempt markers");
    String expectedFailure =
        "FAILED".equals(before.phase())
            ? before.failure() != null ? before.failure() : expectedAttempt.lastError()
            : null;
    require(
        Objects.equals(expectedFailure, after.failure()),
        "finality repair may only reconstruct the final failure");
    require(
        expectedStatusFinishedAt.equals(after.finishedAt()),
        "finality repair must reconstruct status.finishedAt");
  }

  private static ProcessResource.ProcessAttempt expectedRepairedAttempt(
      ProcessResource resource, String finishedAt) {
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt before = status.attempt();
    if (before == null) {
      return new ProcessResource.ProcessAttempt(
          0,
          resource.name() + ":" + status.retryNumber() + ":0",
          ProcessRequestHashes.actionAttempt(
              resource.name(), status.retryNumber(), resource.spec()),
          "CREATED",
          null,
          null,
          "FAILED".equals(status.phase()) ? "FAILED" : null,
          "FINAL",
          finishedAt,
          java.util.Collections.emptyList(),
          new ProcessResource.ManualResolutions(null, null));
    }
    return new ProcessResource.ProcessAttempt(
        before.dispatchGeneration(),
        before.submissionKey(),
        before.requestHash(),
        before.submitState(),
        before.externalId(),
        before.dispatchedAt(),
        "FAILED".equals(status.phase()) && before.lastError() == null
            ? status.failure() != null ? status.failure() : "FAILED"
            : before.lastError(),
        before.retryDisposition(),
        finishedAt,
        before.submissionHistory(),
        before.manualResolutions());
  }

  private static java.util.List<ProcessResource.Condition> withoutDataRepaired(
      java.util.List<ProcessResource.Condition> conditions) {
    java.util.List<ProcessResource.Condition> retained = new java.util.ArrayList<>();
    for (ProcessResource.Condition condition : conditions) {
      if (!ProcessConditions.DATA_REPAIRED.equals(condition.type())) {
        retained.add(condition);
      }
    }
    return retained;
  }

  private static boolean inRange(int value, int minimum, int maximum) {
    return value >= minimum && value <= maximum;
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static void requireOptionalInstant(String value, String label) {
    if (value != null) {
      requireInstant(value, label);
    }
  }

  private static void requireInstant(String value, String label) {
    try {
      java.time.Instant.parse(value);
    } catch (RuntimeException malformed) {
      throw new IllegalArgumentException(
          "invalid Process resource: " + label + " must be an RFC 3339 instant", malformed);
    }
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalArgumentException("invalid Process resource: " + message);
    }
  }
}
