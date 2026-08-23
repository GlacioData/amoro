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

package org.apache.amoro.process.rest;

import org.apache.amoro.persistence.exception.PreconditionFailedException;
import org.apache.amoro.persistence.exception.ResourceDoesNotExist;
import org.apache.amoro.process.ProcessAdmissionException;
import org.apache.amoro.process.ProcessCreateIntent;
import org.apache.amoro.process.ProcessCreationResult;
import org.apache.amoro.process.ProcessCreationService;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.ProcessFinality;
import org.apache.amoro.process.ProcessResource;
import org.apache.amoro.process.ProcessResource.ProcessAttempt;
import org.apache.amoro.process.ProcessResource.ProcessStatus;
import org.apache.amoro.process.ProcessResource.SubmissionSummary;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The service layer behind {@code /api/ams/v2/processes} (process spec §8). Idempotent create with
 * single-active admission, point read, one-snapshot filtered listing, monotonic cancel (merging
 * retryable-FAILED finality in the same CAS) and the two manual-resolution commands whose audit
 * records land in the same durable CAS as the state change. REST never calls engines and never
 * mutates resources except through the repository's version-CAS modify.
 */
public final class ProcessRestSupport {

  private static final int REASON_LIMIT = 512;

  private final ProcessDomainAssembly assembly;
  private final TableCatalogPort tableCatalog;
  private final ProcessCreationService creationService;
  private final ProcessActionCatalog actionCatalog;

  /** P6 (ManagedTablePort) replaces this minimal catalog port. */
  public interface TableCatalogPort {
    boolean exists(String catalog, String database, String table);

    String tableId(String catalog, String database, String table);
  }

  public ProcessRestSupport(ProcessDomainAssembly assembly) {
    this(
        assembly,
        defaultCatalog(),
        new ProcessCreationService(assembly),
        ProcessActionCatalog.simulatedRoutingFixtures());
  }

  public ProcessRestSupport(ProcessDomainAssembly assembly, TableCatalogPort tableCatalog) {
    this(
        assembly,
        tableCatalog,
        new ProcessCreationService(assembly),
        ProcessActionCatalog.simulatedRoutingFixtures());
  }

  public ProcessRestSupport(
      ProcessDomainAssembly assembly, ProcessCreationService creationService) {
    this(
        assembly,
        defaultCatalog(),
        creationService,
        ProcessActionCatalog.simulatedRoutingFixtures());
  }

  public ProcessRestSupport(
      ProcessDomainAssembly assembly,
      TableCatalogPort tableCatalog,
      ProcessCreationService creationService) {
    this(
        assembly,
        tableCatalog,
        creationService,
        ProcessActionCatalog.simulatedRoutingFixtures());
  }

  public ProcessRestSupport(
      ProcessDomainAssembly assembly,
      TableCatalogPort tableCatalog,
      ProcessCreationService creationService,
      ProcessActionCatalog actionCatalog) {
    this.assembly = assembly;
    this.tableCatalog = tableCatalog;
    this.creationService = creationService;
    this.actionCatalog = actionCatalog;
  }

  // ------------------------------------------------------------------ create

  /** Create outcome: the resource plus whether it satisfied the call as an idempotent replay. */
  public static final class CreateResult {
    public final ProcessResource resource;
    public final boolean replay;

    public CreateResult(ProcessResource resource, boolean replay) {
      this.resource = resource;
      this.replay = replay;
    }
  }

  public CreateResult create(
      String catalog,
      String database,
      String table,
      String idempotencyKey,
      String action,
      String engine,
      Map<String, Object> parameters) {
    return create(
        catalog, database, table, idempotencyKey, action, engine, parameters, "MANUAL", "iceberg");
  }

  public CreateResult create(
      String catalog,
      String database,
      String table,
      String idempotencyKey,
      String action,
      String engine,
      Map<String, Object> parameters,
      String triggerSource) {
    return create(
        catalog,
        database,
        table,
        idempotencyKey,
        action,
        engine,
        parameters,
        triggerSource,
        "iceberg");
  }

  public CreateResult create(
      String catalog,
      String database,
      String table,
      String idempotencyKey,
      String action,
      String engine,
      Map<String, Object> parameters,
      String triggerSource,
      String tableFormat) {
    requireIdempotencyKey(idempotencyKey);
    if (!actionCatalog.isKnownAction(action)) {
      throw ApiError.of("INVALID_ACTION", "unknown action '" + action + "'");
    }
    if (!actionCatalog.supports(tableFormat, action, engine)) {
      throw ApiError.of(
          "INVALID_ENGINE", "action '" + action + "' does not support engine '" + engine + "'");
    }
    if (!tableCatalog.exists(catalog, database, table)) {
      throw ApiError.of("TABLE_NOT_FOUND", catalog + "." + database + "." + table);
    }

    String tableId = tableCatalog.tableId(catalog, database, table);
    try {
      ProcessCreationResult result =
          creationService.create(
              ProcessCreateIntent.resolve(
                  new ProcessResource.TableRef(catalog, database, table, tableId),
                  action,
                  engine,
                  triggerSource,
                  idempotencyKey,
                  parameters));
      return new CreateResult(result.resource(), result.replayed());
    } catch (ProcessAdmissionException admission) {
      switch (admission.code()) {
        case ACTIVE_PROCESS_EXISTS:
          throw ApiError.of("ACTIVE_PROCESS_EXISTS", admission.getMessage());
        case IDEMPOTENCY_KEY_REUSED:
          throw ApiError.of("IDEMPOTENCY_KEY_REUSED", admission.getMessage());
        case ADMISSION_IN_PROGRESS:
          throw ApiError.of("IDEMPOTENCY_IN_PROGRESS", admission.getMessage());
        default:
          throw new AssertionError("unknown admission code " + admission.code());
      }
    }
  }

  private static ProcessAttempt firstAttempt(String name, int retryNumber) {
    return new ProcessAttempt(
        0,
        name + ":" + retryNumber + ":0",
        "sha256:initial",
        "CREATED",
        null,
        null,
        "AUTO",
        null,
        new ArrayList<SubmissionSummary>(),
        new ProcessResource.ManualResolutions(null, null));
  }

  // ------------------------------------------------------------------ read

  public ProcessResource get(String name) {
    try {
      return assembly.repository().get(name);
    } catch (ResourceDoesNotExist e) {
      throw ApiError.of("PROCESS_NOT_FOUND", "no process named '" + name + "'");
    }
  }

  /** One page of a filtered listing plus the total, both from ONE index snapshot. */
  public static final class PageResult {
    public final List<ProcessResource> items;
    public final int total;

    public PageResult(List<ProcessResource> items, int total) {
      this.items = items;
      this.total = total;
    }
  }

  public PageResult list(
      String catalog,
      String database,
      String table,
      String action,
      String status,
      int page,
      int pageSize) {
    if (!tableCatalog.exists(catalog, database, table)) {
      throw ApiError.of("TABLE_NOT_FOUND", catalog + "." + database + "." + table);
    }
    if (page < 1 || pageSize < 1 || pageSize > 50) {
      throw ApiError.of("VALIDATION_FAILED", "page must be >= 1 and pageSize within 1..50");
    }
    String tableId = tableCatalog.tableId(catalog, database, table);
    List<ProcessResource> matches = new ArrayList<ProcessResource>();
    for (ProcessResource resource :
        assembly.indexProjection().current().resourcesByName().values()) {
      if (!tableId.equals(resource.spec().table().tableId())) {
        continue;
      }
      if (action != null && !action.equals(resource.spec().action())) {
        continue;
      }
      if (status != null && !status.equals(resource.status().phase())) {
        continue;
      }
      matches.add(resource);
    }
    // spec §8.1: fixed ordering createdAt DESC, name DESC
    matches.sort(
        (a, b) -> {
          int byCreated = b.spec().createdAt().compareTo(a.spec().createdAt());
          return byCreated != 0 ? byCreated : b.name().compareTo(a.name());
        });
    int fromIndex = Math.min((page - 1) * pageSize, matches.size());
    int toIndex = Math.min(fromIndex + pageSize, matches.size());
    return new PageResult(new ArrayList<>(matches.subList(fromIndex, toIndex)), matches.size());
  }

  // ------------------------------------------------------------------ cancel

  public ProcessResource cancel(String name) {
    ProcessResource current = get(name);
    if (ProcessFinality.isFinal(current)) {
      return current; // terminal: return the current state unchanged
    }
    if ("CANCEL".equals(current.spec().desiredState())) {
      return current; // idempotent repeat
    }
    ProcessStatus status = current.status();
    String now = now();
    ProcessStatus next;
    if ("FAILED".equals(status.phase())) {
      // retryable FAILED + desired=CANCEL becomes final in this CAS: merge failure and stamp
      // both finishedAt layers so the expiry index can admit it (TTL would never see it else)
      ProcessAttempt closed = closedAttempt(status.attempt(), "FINAL", now);
      next =
          new ProcessStatus(
              "FAILED",
              status.retryNumber(),
              closed,
              status.attemptHistory(),
              status.lastObservedAt(),
              now,
              status.engineBackoffAttempts(),
              status.conditions(),
              status.summary(),
              status.failure() != null ? status.failure() : "canceled while retryable-failed",
              status.submittedAt(),
              status.startedAt(),
              now);
    } else {
      // business gating: the next reconcile round must fire immediately, not at the old wait
      next =
          new ProcessStatus(
              status.phase(),
              status.retryNumber(),
              status.attempt(),
              status.attemptHistory(),
              status.lastObservedAt(),
              now,
              status.engineBackoffAttempts(),
              status.conditions(),
              status.summary(),
              status.failure(),
              status.submittedAt(),
              status.startedAt(),
              status.finishedAt());
    }
    try {
      return assembly
          .repository()
          .modify(
              name,
              current.resourceVersion(),
              r -> r.withSpec(r.spec().withDesiredState("CANCEL")).withStatus(next));
    } catch (PreconditionFailedException raced) {
      return get(name); // level-triggered: the caller sees the fresh state
    }
  }

  // ------------------------------------------------------------------ resolutions

  public ProcessResource submissionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      String externalId,
      String reason) {
    requireIdempotencyKey(idempotencyKey);
    requireReason(reason);
    ProcessResource current = get(name);
    ProcessAttempt attempt = requireIdentityMatch(current, submissionKey, requestHash);
    requireUnresolvedSubmission(attempt);
    String audit = auditJson(idempotencyKey, submissionKey, resolution, reason);
    switch (resolution) {
      case "ACKNOWLEDGED":
        if (externalId == null) {
          throw ApiError.of("VALIDATION_FAILED", "ACKNOWLEDGED requires an externalId");
        }
        String phase = "CANCEL".equals(current.spec().desiredState()) ? "CANCELING" : "SUBMITTED";
        return casWrite(
            current, statusWithAttempt(withAck(attempt, externalId), phase, current, audit));
      case "NOT_FOUND":
        if (externalId != null) {
          throw ApiError.of("VALIDATION_FAILED", "NOT_FOUND must not carry an externalId");
        }
        if ("CANCEL".equals(current.spec().desiredState())) {
          return casWrite(current, canceledNow(current));
        }
        return casWrite(current, nextGenerationOrFailed(current, attempt));
      default:
        throw ApiError.of("VALIDATION_FAILED", "resolution must be ACKNOWLEDGED or NOT_FOUND");
    }
  }

  public ProcessResource executionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      Boolean retryAllowed,
      String reason) {
    requireIdempotencyKey(idempotencyKey);
    requireReason(reason);
    ProcessResource current = get(name);
    if (ProcessFinality.isFinal(current)) {
      throw ApiError.of(
          "EXECUTION_RESOLUTION_CONFLICT",
          "the process is already final; execution resolution targets unresolved attempts");
    }
    ProcessAttempt attempt = requireIdentityMatch(current, submissionKey, requestHash);
    String audit = auditJson(idempotencyKey, submissionKey, resolution, reason);
    String now = now();
    ProcessStatus status = current.status();
    if ("FAILED".equals(resolution)) {
      if (retryAllowed == null) {
        throw ApiError.of(
            "VALIDATION_FAILED", "resolution FAILED must carry retryAllowed (true/false)");
      }
      boolean fin = !retryAllowed;
      ProcessAttempt closed = closedAttempt(attempt, fin ? "FINAL" : "ALLOW", now);
      boolean finalNow =
          fin
              || "CANCEL".equals(current.spec().desiredState())
              || status.retryNumber() >= current.spec().retryPolicy().maxRetries();
      return casWrite(
          current,
          withExecutionAudit(
              new ProcessStatus(
                  "FAILED",
                  status.retryNumber(),
                  closed,
                  status.attemptHistory(),
                  status.lastObservedAt(),
                  now,
                  zeroedBackoff(),
                  status.conditions(),
                  status.summary(),
                  truncate(reason),
                  status.submittedAt(),
                  status.startedAt(),
                  finalNow ? now : null),
              attempt,
              audit));
    }
    if ("SUCCESS".equals(resolution)
        || "CANCELED".equals(resolution)
        || "KILLED".equals(resolution)
        || "CLOSED".equals(resolution)) {
      ProcessAttempt closed = closedAttempt(attempt, "FINAL", now);
      return casWrite(
          current,
          withExecutionAudit(
              new ProcessStatus(
                  resolution,
                  status.retryNumber(),
                  closed,
                  status.attemptHistory(),
                  status.lastObservedAt(),
                  now,
                  zeroedBackoff(),
                  status.conditions(),
                  status.summary(),
                  null,
                  status.submittedAt(),
                  status.startedAt(),
                  now),
              attempt,
              audit));
    }
    throw ApiError.of(
        "VALIDATION_FAILED", "resolution must be one of SUCCESS/FAILED/CANCELED/KILLED/CLOSED");
  }

  // ------------------------------------------------------------------ test helpers (also used by
  // P8 demos)

  /** Stages a DISPATCHING attempt for manual-resolution testing. */
  public void forceDispatching(String name) {
    ProcessResource current = get(name);
    ProcessAttempt attempt =
        current.status().attempt() != null
            ? current.status().attempt()
            : firstAttempt(name, current.status().retryNumber());
    ProcessAttempt dispatching =
        new ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            "DISPATCHING",
            attempt.externalId(),
            attempt.dispatchedAt(),
            attempt.retryDisposition(),
            attempt.finishedAt(),
            attempt.submissionHistory(),
            attempt.manualResolutions());
    casWrite(current, statusWithAttempt(dispatching, current.status().phase(), current, null));
  }

  /** Drives the resource into a fixed terminal phase for lifecycle testing. */
  public void forceTerminal(String name, String phase) {
    ProcessResource current = get(name);
    String now = now();
    ProcessStatus status = current.status();
    ProcessAttempt closed = closedAttempt(status.attempt(), "FINAL", now);
    casWrite(
        current,
        new ProcessStatus(
            phase,
            status.retryNumber(),
            closed,
            status.attemptHistory(),
            status.lastObservedAt(),
            now,
            zeroedBackoff(),
            status.conditions(),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            now));
  }

  // ------------------------------------------------------------------ internals

  private static void requireIdempotencyKey(String idempotencyKey) {
    if (idempotencyKey == null || idempotencyKey.trim().isEmpty()) {
      throw ApiError.of("IDEMPOTENCY_KEY_REQUIRED", "the Idempotency-Key header is required");
    }
    if (idempotencyKey.length() > 128 || !idempotencyKey.matches("\\p{Print}+")) {
      throw ApiError.of(
          "IDEMPOTENCY_KEY_REQUIRED",
          "the Idempotency-Key must be 1..128 printable ASCII characters");
    }
  }

  private static void requireReason(String reason) {
    if (reason == null || reason.trim().isEmpty()) {
      throw ApiError.of("VALIDATION_FAILED", "reason is required for manual resolutions");
    }
  }

  private static ProcessAttempt requireIdentityMatch(
      ProcessResource current, String submissionKey, String requestHash) {
    ProcessAttempt attempt = current.status().attempt();
    if (attempt == null || !attempt.submissionKey().equals(submissionKey)) {
      throw ApiError.of(
          "PROCESS_ATTEMPT_STALE", "submissionKey does not match the current attempt");
    }
    if (requestHash != null && !requestHash.equals(attempt.requestHash())) {
      throw ApiError.of("PROCESS_ATTEMPT_STALE", "requestHash does not match the current attempt");
    }
    return attempt;
  }

  private static void requireUnresolvedSubmission(ProcessAttempt attempt) {
    String state = attempt.submitState();
    if (!"DISPATCHING".equals(state) && !"UNKNOWN".equals(state) && !"CONFLICT".equals(state)) {
      throw ApiError.of(
          "SUBMISSION_RESOLUTION_CONFLICT",
          "submission resolution targets a DISPATCHING/UNKNOWN/CONFLICT attempt, not '"
              + state
              + "'");
    }
  }

  /**
   * Applies the write through version-CAS; the status passed in already carries its audit (the
   * submission audit via {@link #statusWithAttempt}, the execution audit via {@link
   * #withExecutionAudit}) so record and state change land in the same durable CAS.
   */
  private ProcessResource casWrite(ProcessResource current, ProcessStatus next) {
    try {
      return assembly
          .repository()
          .modify(current.name(), current.resourceVersion(), r -> r.withStatus(next));
    } catch (PreconditionFailedException raced) {
      throw ApiError.of(
          "PRECONDITION_FAILED", "the resource changed concurrently; re-read and retry");
    }
  }

  private ProcessStatus statusWithAttempt(
      ProcessAttempt attempt, String phase, ProcessResource current, String submissionAudit) {
    ProcessStatus status = current.status();
    ProcessAttempt audited =
        submissionAudit == null
            ? attempt
            : new ProcessAttempt(
                attempt.dispatchGeneration(),
                attempt.submissionKey(),
                attempt.requestHash(),
                attempt.submitState(),
                attempt.externalId(),
                attempt.dispatchedAt(),
                attempt.retryDisposition(),
                attempt.finishedAt(),
                attempt.submissionHistory(),
                new ProcessResource.ManualResolutions(
                    submissionAudit,
                    attempt.manualResolutions() != null
                        ? attempt.manualResolutions().execution()
                        : null));
    return new ProcessStatus(
        phase,
        status.retryNumber(),
        audited,
        status.attemptHistory(),
        status.lastObservedAt(),
        now(),
        status.engineBackoffAttempts(),
        status.conditions(),
        status.summary(),
        status.failure(),
        status.submittedAt(),
        status.startedAt(),
        status.finishedAt());
  }

  private ProcessStatus withExecutionAudit(
      ProcessStatus next, ProcessAttempt attempt, String executionAudit) {
    ProcessAttempt audited =
        new ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            attempt.submitState(),
            attempt.externalId(),
            attempt.dispatchedAt(),
            attempt.retryDisposition(),
            attempt.finishedAt(),
            attempt.submissionHistory(),
            new ProcessResource.ManualResolutions(
                attempt.manualResolutions() != null
                    ? attempt.manualResolutions().submission()
                    : null,
                executionAudit));
    return new ProcessStatus(
        next.phase(),
        next.retryNumber(),
        audited,
        next.attemptHistory(),
        next.lastObservedAt(),
        next.nextReconcileAt(),
        next.engineBackoffAttempts(),
        next.conditions(),
        next.summary(),
        next.failure(),
        next.submittedAt(),
        next.startedAt(),
        next.finishedAt());
  }

  private ProcessAttempt withAck(ProcessAttempt attempt, String externalId) {
    return new ProcessAttempt(
        attempt.dispatchGeneration(),
        attempt.submissionKey(),
        attempt.requestHash(),
        "ACKNOWLEDGED",
        externalId,
        attempt.dispatchedAt() != null ? attempt.dispatchedAt() : now(),
        attempt.retryDisposition(),
        attempt.finishedAt(),
        attempt.submissionHistory(),
        attempt.manualResolutions());
  }

  private ProcessAttempt closedAttempt(ProcessAttempt attempt, String disposition, String now) {
    if (attempt == null) {
      return null;
    }
    return new ProcessAttempt(
        attempt.dispatchGeneration(),
        attempt.submissionKey(),
        attempt.requestHash(),
        attempt.submitState(),
        attempt.externalId(),
        attempt.dispatchedAt(),
        disposition,
        now,
        attempt.submissionHistory(),
        attempt.manualResolutions());
  }

  private ProcessStatus canceledNow(ProcessResource current) {
    ProcessStatus status = current.status();
    String now = now();
    return new ProcessStatus(
        "CANCELED",
        status.retryNumber(),
        closedAttempt(status.attempt(), "FINAL", now),
        status.attemptHistory(),
        status.lastObservedAt(),
        now,
        zeroedBackoff(),
        status.conditions(),
        status.summary(),
        null,
        status.submittedAt(),
        status.startedAt(),
        now);
  }

  private ProcessStatus nextGenerationOrFailed(ProcessResource current, ProcessAttempt attempt) {
    ProcessStatus status = current.status();
    String now = now();
    int maxGenerations = current.spec().retryPolicy().maxSubmissionRetries();
    if (attempt.dispatchGeneration() >= maxGenerations) {
      ProcessAttempt closed = closedAttempt(attempt, "FINAL", now);
      return new ProcessStatus(
          "FAILED",
          status.retryNumber(),
          closed,
          status.attemptHistory(),
          status.lastObservedAt(),
          now,
          status.engineBackoffAttempts(),
          status.conditions(),
          status.summary(),
          "submission not accepted (generations exhausted)",
          status.submittedAt(),
          status.startedAt(),
          now);
    }
    int nextGeneration = attempt.dispatchGeneration() + 1;
    ProcessAttempt next =
        new ProcessAttempt(
            nextGeneration,
            current.name() + ":" + status.retryNumber() + ":" + nextGeneration,
            attempt.requestHash(),
            "CREATED",
            null,
            null,
            "AUTO",
            null,
            archiveSubmission(attempt),
            // the audit belongs to the resolved generation only; a fresh generation starts clean
            new ProcessResource.ManualResolutions(null, null));
    return new ProcessStatus(
        "PENDING",
        status.retryNumber(),
        next,
        status.attemptHistory(),
        status.lastObservedAt(),
        now,
        status.engineBackoffAttempts(),
        status.conditions(),
        status.summary(),
        status.failure(),
        status.submittedAt(),
        status.startedAt(),
        status.finishedAt());
  }

  private static List<SubmissionSummary> archiveSubmission(ProcessAttempt attempt) {
    List<SubmissionSummary> history = new ArrayList<SubmissionSummary>(attempt.submissionHistory());
    history.add(
        new SubmissionSummary(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            "NOT_FOUND",
            null,
            now()));
    return history;
  }

  private static ProcessResource.EngineBackoff zeroedBackoff() {
    return new ProcessResource.EngineBackoff(0, 0, 0, 0);
  }

  private static String now() {
    return java.time.Instant.now().toString();
  }

  private static String truncate(String reason) {
    if (reason == null) {
      return null;
    }
    return reason.length() <= REASON_LIMIT ? reason : reason.substring(0, REASON_LIMIT);
  }

  private static String auditJson(
      String idempotencyKey, String submissionKey, String resolution, String reason) {
    return "{\"idempotencyKeyHash\":\""
        + sha256(idempotencyKey)
        + "\",\"submissionKey\":\""
        + submissionKey
        + "\",\"resolution\":\""
        + resolution
        + "\",\"reason\":\""
        + (reason == null ? "" : reason.replace("\"", "'"))
        + "\",\"resolvedAt\":\""
        + now()
        + "\"}";
  }

  /** Canonical JSON with recursively sorted keys and string escaping (no hash collisions). */
  private static String canonical(Object value) {
    StringBuilder builder = new StringBuilder();
    appendCanonical(value, builder);
    return builder.toString();
  }

  private static void appendCanonical(Object value, StringBuilder builder) {
    if (value == null) {
      builder.append("null");
    } else if (value instanceof String) {
      builder
          .append('"')
          .append(((String) value).replace("\\", "\\\\").replace("\"", "\\\""))
          .append('"');
    } else if (value instanceof Number || value instanceof Boolean) {
      builder.append(value);
    } else if (value instanceof Map) {
      Map<String, Object> sorted = new TreeMap<String, Object>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        sorted.put(String.valueOf(entry.getKey()), entry.getValue());
      }
      builder.append('{');
      boolean first = true;
      for (Map.Entry<String, Object> entry : sorted.entrySet()) {
        if (!first) {
          builder.append(',');
        }
        first = false;
        appendCanonical(entry.getKey(), builder);
        builder.append(':');
        appendCanonical(entry.getValue(), builder);
      }
      builder.append('}');
    } else if (value instanceof List) {
      builder.append('[');
      boolean first = true;
      for (Object item : (List<?>) value) {
        if (!first) {
          builder.append(',');
        }
        first = false;
        appendCanonical(item, builder);
      }
      builder.append(']');
    } else {
      appendCanonical(String.valueOf(value), builder);
    }
  }

  private static String sha256(String input) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static TableCatalogPort defaultCatalog() {
    return new TableCatalogPort() {
      @Override
      public boolean exists(String catalog, String database, String table) {
        return !"ghost-table".equals(table) && !"ghost".equals(database);
      }

      @Override
      public String tableId(String catalog, String database, String table) {
        return sha256(catalog + "|" + database + "|" + table).substring(0, 12);
      }
    };
  }
}
