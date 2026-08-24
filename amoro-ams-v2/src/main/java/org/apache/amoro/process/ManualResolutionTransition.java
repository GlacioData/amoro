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

import org.apache.amoro.process.ProcessCommandException.Code;
import org.apache.amoro.resources.ProcessResource;
import org.apache.amoro.resources.ProcessResource.AttemptSummary;
import org.apache.amoro.resources.ProcessResource.EngineBackoff;
import org.apache.amoro.resources.ProcessResource.ManualResolution;
import org.apache.amoro.resources.ProcessResource.ManualResolutions;
import org.apache.amoro.resources.ProcessResource.ProcessAttempt;
import org.apache.amoro.resources.ProcessResource.ProcessStatus;
import org.apache.amoro.resources.ProcessResource.SubmissionSummary;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Pure, attempt-bound state derivation for both kinds of manual resolution. */
public final class ManualResolutionTransition {

  private static final int REASON_LIMIT = 512;

  private ManualResolutionTransition() {}

  public enum Kind {
    SUBMISSION,
    EXECUTION
  }

  public static final class Command {
    private final Kind kind;
    private final String idempotencyKey;
    private final String submissionKey;
    private final String requestHash;
    private final String outcome;
    private final String externalId;
    private final Boolean retryAllowed;
    private final String reason;
    private final String operatorContext;

    public Command(
        Kind kind,
        String idempotencyKey,
        String submissionKey,
        String requestHash,
        String outcome,
        String externalId,
        Boolean retryAllowed,
        String reason,
        String operatorContext) {
      this.kind = Objects.requireNonNull(kind, "kind");
      this.idempotencyKey = idempotencyKey;
      this.submissionKey = submissionKey;
      this.requestHash = requestHash;
      this.outcome = outcome;
      this.externalId = externalId;
      this.retryAllowed = retryAllowed;
      this.reason = reason;
      this.operatorContext = operatorContext == null ? "api" : operatorContext;
    }
  }

  public static final class Result {
    private final ProcessStatus status;
    private final boolean replayed;

    private Result(ProcessStatus status, boolean replayed) {
      this.status = status;
      this.replayed = replayed;
    }

    public ProcessStatus status() {
      return status;
    }

    public boolean replayed() {
      return replayed;
    }
  }

  public static Result apply(ProcessResource current, Command command, String now) {
    validateCommon(command);
    Instant.parse(now);
    String reason = normalizeReason(command.reason);
    String keyHash = sha256(command.idempotencyKey);
    String commandHash = commandHash(command, reason);

    ManualResolution replay = findAudit(current, command);
    if (replay != null) {
      if (!replay.idempotencyKeyHash().equals(keyHash)) {
        throw conflict(command.kind, "this attempt already has a different manual resolution");
      }
      if (!replay.commandHash().equals(commandHash)) {
        throw new ProcessCommandException(
            Code.IDEMPOTENCY_KEY_REUSED,
            "the Idempotency-Key was already used with a different resolution command");
      }
      return new Result(current.status(), true);
    }
    if (findAuditByIdempotencyKey(current, keyHash) != null) {
      throw new ProcessCommandException(
          Code.IDEMPOTENCY_KEY_REUSED,
          "the Idempotency-Key was already used for a different Process resolution identity");
    }

    ProcessAttempt attempt = current.status().attempt();
    if (attempt == null
        || !command.submissionKey.equals(attempt.submissionKey())
        || !command.requestHash.equals(attempt.requestHash())) {
      throw new ProcessCommandException(
          Code.PROCESS_ATTEMPT_STALE, "the command does not target the current Process attempt");
    }

    ManualResolution audit =
        new ManualResolution(
            keyHash,
            commandHash,
            command.submissionKey,
            command.requestHash,
            command.outcome,
            command.externalId,
            command.retryAllowed,
            reason,
            boundedOperator(command.operatorContext),
            now);
    return command.kind == Kind.SUBMISSION
        ? submission(current, command, audit, now)
        : execution(current, command, audit, now);
  }

  private static Result submission(
      ProcessResource current, Command command, ManualResolution audit, String now) {
    ProcessStatus status = current.status();
    ProcessAttempt attempt = status.attempt();
    if (!ProcessConditions.isTrue(status.conditions(), ProcessConditions.SUBMISSION_UNRESOLVED)
        || !("DISPATCHING".equals(attempt.submitState())
            || "UNKNOWN".equals(attempt.submitState())
            || "CONFLICT".equals(attempt.submitState()))) {
      throw conflict(
          Kind.SUBMISSION,
          "submission resolution requires the current unresolved submission generation");
    }
    if (!("ACKNOWLEDGED".equals(command.outcome) || "NOT_FOUND".equals(command.outcome))) {
      throw validation("submission resolution must be ACKNOWLEDGED or NOT_FOUND");
    }
    if ("ACKNOWLEDGED".equals(command.outcome) && isBlank(command.externalId)) {
      throw validation("ACKNOWLEDGED requires externalId");
    }
    if ("NOT_FOUND".equals(command.outcome) && command.externalId != null) {
      throw validation("NOT_FOUND must not carry externalId");
    }

    EngineBackoff backoff = resetResolve(status.engineBackoffAttempts());
    List<ProcessResource.Condition> conditions =
        clearEngineWhenRecovered(
            ProcessConditions.remove(status.conditions(), ProcessConditions.SUBMISSION_UNRESOLVED),
            backoff);
    if ("ACKNOWLEDGED".equals(command.outcome)) {
      ProcessAttempt acknowledged =
          copyAttempt(
              attempt,
              "ACKNOWLEDGED",
              command.externalId,
              attempt.dispatchedAt() == null ? now : attempt.dispatchedAt(),
              null,
              attempt.retryDisposition(),
              null,
              new ManualResolutions(audit, currentExecutionAudit(attempt)));
      return result(
          status,
          "CANCEL".equals(current.spec().desiredState()) ? "CANCELING" : "SUBMITTED",
          acknowledged,
          status.attemptHistory(),
          now,
          backoff,
          conditions,
          null,
          status.submittedAt() == null ? now : status.submittedAt(),
          status.startedAt(),
          null);
    }

    if ("CANCEL".equals(current.spec().desiredState())) {
      ProcessAttempt closed =
          copyAttempt(
              attempt,
              attempt.submitState(),
              null,
              attempt.dispatchedAt(),
              null,
              "FINAL",
              now,
              new ManualResolutions(audit, currentExecutionAudit(attempt)));
      return result(
          status,
          "CANCELED",
          closed,
          status.attemptHistory(),
          null,
          backoff,
          conditions,
          null,
          status.submittedAt(),
          status.startedAt(),
          now);
    }

    List<SubmissionSummary> history = new ArrayList<>(attempt.submissionHistory());
    history.add(
        new SubmissionSummary(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            "NOT_FOUND",
            audit,
            now));
    if (attempt.dispatchGeneration() < current.spec().retryPolicy().maxSubmissionRetries()) {
      int generation = attempt.dispatchGeneration() + 1;
      ProcessAttempt next =
          new ProcessAttempt(
              generation,
              current.name() + ":" + status.retryNumber() + ":" + generation,
              attempt.requestHash(),
              "CREATED",
              null,
              null,
              null,
              "AUTO",
              null,
              history,
              new ManualResolutions(null, null));
      return result(
          status,
          "PENDING",
          next,
          status.attemptHistory(),
          now,
          backoff,
          conditions,
          null,
          status.submittedAt(),
          status.startedAt(),
          null);
    }

    String failure = "SUBMISSION_NOT_ACCEPTED";
    ProcessAttempt failed =
        copyAttempt(
            attempt,
            attempt.submitState(),
            null,
            attempt.dispatchedAt(),
            failure,
            "ALLOW",
            now,
            new ManualResolutions(audit, currentExecutionAudit(attempt)));
    boolean finalProcess = status.retryNumber() >= current.spec().retryPolicy().maxRetries();
    return result(
        status,
        "FAILED",
        failed,
        status.attemptHistory(),
        finalProcess ? null : now,
        backoff,
        conditions,
        finalProcess ? failure : null,
        status.submittedAt(),
        status.startedAt(),
        finalProcess ? now : null);
  }

  private static Result execution(
      ProcessResource current, Command command, ManualResolution audit, String now) {
    ProcessStatus status = current.status();
    ProcessAttempt attempt = status.attempt();
    if (!ProcessConditions.isTrue(status.conditions(), ProcessConditions.EXECUTION_UNRESOLVED)) {
      throw conflict(Kind.EXECUTION, "execution resolution requires ExecutionUnresolved=True");
    }
    boolean failed = "FAILED".equals(command.outcome);
    boolean fixed = ProcessFinality.isFixedTerminal(command.outcome);
    if (!(failed || fixed)) {
      throw validation("execution resolution must be SUCCESS, FAILED, CANCELED, KILLED or CLOSED");
    }
    if (failed && command.retryAllowed == null) {
      throw validation("FAILED requires retryAllowed");
    }
    if (!failed && command.retryAllowed != null) {
      throw validation("retryAllowed is valid only for FAILED");
    }
    if (command.externalId != null) {
      throw validation("execution resolution must not carry externalId");
    }

    String lastError = failed ? audit.reason() : null;
    String disposition = failed && Boolean.TRUE.equals(command.retryAllowed) ? "ALLOW" : "FINAL";
    ProcessAttempt closed =
        copyAttempt(
            attempt,
            attempt.submitState(),
            attempt.externalId(),
            attempt.dispatchedAt(),
            lastError,
            disposition,
            now,
            new ManualResolutions(currentSubmissionAudit(attempt), audit));
    boolean finalProcess =
        fixed
            || "CANCEL".equals(current.spec().desiredState())
            || status.retryNumber() >= current.spec().retryPolicy().maxRetries()
            || "FINAL".equals(disposition);
    List<ProcessResource.Condition> conditions =
        ProcessConditions.remove(
            status.conditions(),
            ProcessConditions.SUBMISSION_UNRESOLVED,
            ProcessConditions.EXECUTION_UNRESOLVED,
            ProcessConditions.ENGINE_UNREACHABLE,
            ProcessConditions.CANCELLATION_UNSUPPORTED);
    return result(
        status,
        command.outcome,
        closed,
        status.attemptHistory(),
        finalProcess ? null : now,
        new EngineBackoff(0, 0, 0, 0),
        conditions,
        finalProcess && failed ? lastError : null,
        status.submittedAt(),
        status.startedAt(),
        finalProcess ? now : null);
  }

  private static ManualResolution findAudit(ProcessResource current, Command command) {
    ProcessAttempt attempt = current.status().attempt();
    if (attempt != null) {
      if (command.kind == Kind.SUBMISSION) {
        ManualResolution audit = currentSubmissionAudit(attempt);
        if (matches(audit, command)) {
          return audit;
        }
        for (SubmissionSummary summary : attempt.submissionHistory()) {
          if (command.submissionKey.equals(summary.submissionKey())
              && command.requestHash.equals(summary.requestHash())) {
            return summary.manualResolution();
          }
        }
      } else if (matches(currentExecutionAudit(attempt), command)) {
        return currentExecutionAudit(attempt);
      }
    }
    for (AttemptSummary summary : current.status().attemptHistory()) {
      if (command.kind == Kind.EXECUTION
          && command.submissionKey.equals(summary.submissionKey())
          && command.requestHash.equals(summary.requestHash())) {
        return summary.manualResolutions() == null ? null : summary.manualResolutions().execution();
      }
      if (command.kind == Kind.SUBMISSION) {
        if (command.submissionKey.equals(summary.submissionKey())
            && command.requestHash.equals(summary.requestHash())) {
          return summary.manualResolutions() == null
              ? null
              : summary.manualResolutions().submission();
        }
        for (SubmissionSummary generation : summary.submissionHistory()) {
          if (command.submissionKey.equals(generation.submissionKey())
              && command.requestHash.equals(generation.requestHash())) {
            return generation.manualResolution();
          }
        }
      }
    }
    return null;
  }

  private static boolean matches(ManualResolution audit, Command command) {
    return audit != null
        && command.submissionKey.equals(audit.submissionKey())
        && command.requestHash.equals(audit.requestHash());
  }

  private static ManualResolution findAuditByIdempotencyKey(
      ProcessResource current, String keyHash) {
    ProcessAttempt attempt = current.status().attempt();
    if (attempt != null) {
      ManualResolution found = matchingKey(currentSubmissionAudit(attempt), keyHash);
      if (found != null) {
        return found;
      }
      found = matchingKey(currentExecutionAudit(attempt), keyHash);
      if (found != null) {
        return found;
      }
      for (SubmissionSummary summary : attempt.submissionHistory()) {
        found = matchingKey(summary.manualResolution(), keyHash);
        if (found != null) {
          return found;
        }
      }
    }
    for (AttemptSummary summary : current.status().attemptHistory()) {
      if (summary.manualResolutions() != null) {
        ManualResolution found = matchingKey(summary.manualResolutions().submission(), keyHash);
        if (found != null) {
          return found;
        }
        found = matchingKey(summary.manualResolutions().execution(), keyHash);
        if (found != null) {
          return found;
        }
      }
      for (SubmissionSummary generation : summary.submissionHistory()) {
        ManualResolution found = matchingKey(generation.manualResolution(), keyHash);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static ManualResolution matchingKey(ManualResolution audit, String keyHash) {
    return audit != null && keyHash.equals(audit.idempotencyKeyHash()) ? audit : null;
  }

  private static Result result(
      ProcessStatus source,
      String phase,
      ProcessAttempt attempt,
      List<AttemptSummary> history,
      String nextReconcileAt,
      EngineBackoff backoff,
      List<ProcessResource.Condition> conditions,
      String failure,
      String submittedAt,
      String startedAt,
      String finishedAt) {
    return new Result(
        new ProcessStatus(
            phase,
            source.retryNumber(),
            attempt,
            history,
            source.lastObservedAt(),
            source.lastCancelAttemptAt(),
            nextReconcileAt,
            backoff,
            conditions,
            source.summary(),
            failure,
            submittedAt,
            startedAt,
            finishedAt),
        false);
  }

  private static ProcessAttempt copyAttempt(
      ProcessAttempt source,
      String submitState,
      String externalId,
      String dispatchedAt,
      String lastError,
      String retryDisposition,
      String finishedAt,
      ManualResolutions resolutions) {
    return new ProcessAttempt(
        source.dispatchGeneration(),
        source.submissionKey(),
        source.requestHash(),
        submitState,
        externalId,
        dispatchedAt,
        lastError,
        retryDisposition,
        finishedAt,
        source.submissionHistory(),
        resolutions);
  }

  private static ManualResolution currentSubmissionAudit(ProcessAttempt attempt) {
    return attempt.manualResolutions() == null ? null : attempt.manualResolutions().submission();
  }

  private static ManualResolution currentExecutionAudit(ProcessAttempt attempt) {
    return attempt.manualResolutions() == null ? null : attempt.manualResolutions().execution();
  }

  private static EngineBackoff resetResolve(EngineBackoff current) {
    return new EngineBackoff(current.submit(), 0, current.observe(), current.cancel());
  }

  private static List<ProcessResource.Condition> clearEngineWhenRecovered(
      List<ProcessResource.Condition> conditions, EngineBackoff backoff) {
    return backoff.submit() == 0
            && backoff.resolve() == 0
            && backoff.observe() == 0
            && backoff.cancel() == 0
        ? ProcessConditions.remove(conditions, ProcessConditions.ENGINE_UNREACHABLE)
        : conditions;
  }

  private static void validateCommon(Command command) {
    if (isBlank(command.idempotencyKey)
        || command.idempotencyKey.length() > 128
        || !command.idempotencyKey.matches("[\\x20-\\x7E]+")) {
      throw new ProcessCommandException(
          Code.IDEMPOTENCY_KEY_REQUIRED,
          "Idempotency-Key must contain 1..128 printable ASCII characters");
    }
    if (isBlank(command.submissionKey) || isBlank(command.requestHash)) {
      throw validation("submissionKey and requestHash are required");
    }
    if (command.submissionKey.length() > 512 || containsControl(command.submissionKey)) {
      throw validation("submissionKey must be at most 512 characters without controls");
    }
    if (!command.requestHash.startsWith("sha256:") || command.requestHash.length() > 128) {
      throw validation("requestHash must be a bounded sha256 identity");
    }
    if (isBlank(command.outcome)) {
      throw validation("resolution is required");
    }
    if (isBlank(command.reason)) {
      throw validation("reason is required");
    }
    if (command.externalId != null
        && (command.externalId.getBytes(StandardCharsets.UTF_8).length > 512
            || containsControl(command.externalId))) {
      throw validation("externalId must be at most 512 UTF-8 bytes without controls");
    }
  }

  private static ProcessCommandException conflict(Kind kind, String message) {
    return new ProcessCommandException(
        kind == Kind.SUBMISSION
            ? Code.SUBMISSION_RESOLUTION_CONFLICT
            : Code.EXECUTION_RESOLUTION_CONFLICT,
        message);
  }

  private static ProcessCommandException validation(String message) {
    return new ProcessCommandException(Code.VALIDATION_FAILED, message);
  }

  private static String normalizeReason(String value) {
    String normalized = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim();
    if (normalized.getBytes(StandardCharsets.UTF_8).length > REASON_LIMIT) {
      throw validation("reason must not exceed 512 UTF-8 bytes");
    }
    return normalized;
  }

  private static String boundedOperator(String value) {
    String normalized = value.replace('\n', ' ').replace('\r', ' ').trim();
    return normalized.length() <= 256 ? normalized : normalized.substring(0, 256);
  }

  private static String commandHash(Command command, String normalizedReason) {
    StringBuilder canonical = new StringBuilder();
    appendField(canonical, command.kind.name());
    appendField(canonical, command.submissionKey);
    appendField(canonical, command.requestHash);
    appendField(canonical, command.outcome);
    appendField(canonical, command.externalId);
    appendField(canonical, command.retryAllowed == null ? null : command.retryAllowed.toString());
    appendField(canonical, normalizedReason);
    return sha256(canonical.toString());
  }

  private static void appendField(StringBuilder target, String value) {
    if (value == null) {
      target.append("-1:");
    } else {
      target.append(value.length()).append(':').append(value);
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static boolean containsControl(String value) {
    for (int index = 0; index < value.length(); index++) {
      if (Character.isISOControl(value.charAt(index))) {
        return true;
      }
    }
    return false;
  }

  private static String sha256(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return "sha256:"
          + HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception unavailable) {
      throw new IllegalStateException("SHA-256 is unavailable", unavailable);
    }
  }
}
