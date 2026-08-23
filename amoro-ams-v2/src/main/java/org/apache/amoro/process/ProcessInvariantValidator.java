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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Fail-closed Process domain validation. It participates in persistence projection preparation so
 * every create/modify is validated on the mutation lane before any durable I/O or index update.
 */
public final class ProcessInvariantValidator
    implements DurableStateProjection<ProcessResource> {

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
      validateSpecTransition(change.previous().spec(), current.spec());
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
    require(
        !isBlank(spec.request().idempotencyKeyHash()), "idempotencyKeyHash must not be blank");
    require(!isBlank(spec.request().requestHash()), "creation requestHash must not be blank");

    ProcessResource.RetryPolicy policy = spec.retryPolicy();
    require(inRange(policy.maxRetries(), 0, 3), "maxRetries must be in [0,3]");
    require(
        inRange(policy.maxSubmissionRetries(), 0, 2),
        "maxSubmissionRetries must be in [0,2]");
    require(
        inRange(policy.retryDelaySeconds(), 1, 86_400),
        "retryDelaySeconds must be in [1,86400]");

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

    ProcessResource.EngineBackoff backoff = status.engineBackoffAttempts();
    require(inRange(backoff.submit(), 0, 7), "submit backoff must be in [0,7]");
    require(inRange(backoff.resolve(), 0, 7), "resolve backoff must be in [0,7]");
    require(inRange(backoff.observe(), 0, 7), "observe backoff must be in [0,7]");
    require(inRange(backoff.cancel(), 0, 7), "cancel backoff must be in [0,7]");

    require(status.conditions().size() <= 8, "conditions must contain at most 8 entries");
    Set<String> conditionTypes = new HashSet<>();
    for (ProcessResource.Condition condition : status.conditions()) {
      require(conditionTypes.add(condition.type()), "condition types must be unique");
      require(
          "True".equals(condition.status()) || "False".equals(condition.status()),
          "condition status must be True or False");
    }

    ProcessResource.ProcessAttempt attempt = status.attempt();
    if (attempt == null) {
      return;
    }
    require(
        inRange(
            attempt.dispatchGeneration(), 0, spec.retryPolicy().maxSubmissionRetries()),
        "dispatchGeneration exceeds submission retry policy");
    require(
        attempt.submissionHistory().size() <= spec.retryPolicy().maxSubmissionRetries(),
        "submission history exceeds retry policy");
    require(
        (resource.name()
                + ":"
                + status.retryNumber()
                + ":"
                + attempt.dispatchGeneration())
            .equals(attempt.submissionKey()),
        "submissionKey does not match process/retry/generation identity");
    require(
        attempt.requestHash() != null && attempt.requestHash().startsWith("sha256:"),
        "attempt requestHash must be a sha256 identity");
    require(SUBMIT_STATES.contains(attempt.submitState()), "invalid submitState");
    require(
        RETRY_DISPOSITIONS.contains(attempt.retryDisposition()), "invalid retryDisposition");
    if (attempt.externalId() != null) {
      require(
          "ACKNOWLEDGED".equals(attempt.submitState()),
          "externalId requires ACKNOWLEDGED submitState");
    }
  }

  private static boolean inRange(int value, int minimum, int maximum) {
    return value >= minimum && value <= maximum;
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalArgumentException("invalid Process resource: " + message);
    }
  }
}
