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

import org.apache.amoro.persistence.exception.ResourceDoesNotExist;
import org.apache.amoro.process.ManualResolutionTransition;
import org.apache.amoro.process.ProcessAdmissionException;
import org.apache.amoro.process.ProcessCommandException;
import org.apache.amoro.process.ProcessCommandService;
import org.apache.amoro.process.ProcessCreateIntent;
import org.apache.amoro.process.ProcessCreationResult;
import org.apache.amoro.process.ProcessCreationService;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.resources.ProcessResource;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The service layer behind {@code /api/ams/v2/processes} (process spec §8). Idempotent create with
 * single-active admission, point read, one-snapshot filtered listing, monotonic cancel (merging
 * retryable-FAILED finality in the same CAS) and the two manual-resolution commands whose audit
 * records land in the same durable CAS as the state change. REST never calls engines and never
 * mutates resources except through the repository's version-CAS modify.
 */
public final class ProcessRestSupport {

  private static final Pattern WIRE_NAME = Pattern.compile("[a-z][a-z0-9-]{0,63}");

  private final ProcessDomainAssembly assembly;
  private final TableCatalogPort tableCatalog;
  private final ProcessCreationService creationService;
  private final ProcessCommandService commandService;
  private final ProcessActionCatalog actionCatalog;

  /** Format-neutral lookup seam; this Spec wires only an exact simulated-table fixture. */
  public interface TableCatalogPort {
    /** Resolves one atomic identity snapshot, or returns null when the table is not managed. */
    TableIdentity resolve(String catalog, String database, String table);
  }

  /** Immutable table identity used for the entire create/list operation. */
  public static final class TableIdentity {
    private final String tableId;
    private final String tableFormat;

    public TableIdentity(String tableId, String tableFormat) {
      this.tableId = Objects.requireNonNull(tableId, "tableId");
      this.tableFormat = Objects.requireNonNull(tableFormat, "tableFormat");
    }

    public String tableId() {
      return tableId;
    }

    public String tableFormat() {
      return tableFormat;
    }
  }

  public ProcessRestSupport(ProcessDomainAssembly assembly) {
    this(
        assembly,
        defaultCatalog(),
        new ProcessCreationService(assembly),
        new ProcessCommandService(assembly.repository()),
        ProcessActionCatalog.empty());
  }

  public ProcessRestSupport(ProcessDomainAssembly assembly, TableCatalogPort tableCatalog) {
    this(
        assembly,
        tableCatalog,
        new ProcessCreationService(assembly),
        new ProcessCommandService(assembly.repository()),
        ProcessActionCatalog.empty());
  }

  public ProcessRestSupport(
      ProcessDomainAssembly assembly, ProcessCreationService creationService) {
    this(
        assembly,
        defaultCatalog(),
        creationService,
        new ProcessCommandService(assembly.repository()),
        ProcessActionCatalog.empty());
  }

  public ProcessRestSupport(
      ProcessDomainAssembly assembly,
      TableCatalogPort tableCatalog,
      ProcessCreationService creationService) {
    this(
        assembly,
        tableCatalog,
        creationService,
        new ProcessCommandService(assembly.repository()),
        ProcessActionCatalog.empty());
  }

  public ProcessRestSupport(
      ProcessDomainAssembly assembly,
      TableCatalogPort tableCatalog,
      ProcessCreationService creationService,
      ProcessActionCatalog actionCatalog) {
    this(
        assembly,
        tableCatalog,
        creationService,
        new ProcessCommandService(assembly.repository()),
        actionCatalog);
  }

  public ProcessRestSupport(
      ProcessDomainAssembly assembly,
      TableCatalogPort tableCatalog,
      ProcessCreationService creationService,
      ProcessCommandService commandService,
      ProcessActionCatalog actionCatalog) {
    this.assembly = assembly;
    this.tableCatalog = tableCatalog;
    this.creationService = creationService;
    this.commandService = commandService;
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
    TableIdentity tableIdentity = requireExistingTable(catalog, database, table);
    return createResolved(
        catalog,
        database,
        table,
        idempotencyKey,
        action,
        engine,
        parameters,
        "MANUAL",
        tableIdentity);
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
    TableIdentity tableIdentity = requireExistingTable(catalog, database, table);
    return createResolved(
        catalog,
        database,
        table,
        idempotencyKey,
        action,
        engine,
        parameters,
        triggerSource,
        tableIdentity);
  }

  private CreateResult createResolved(
      String catalog,
      String database,
      String table,
      String idempotencyKey,
      String action,
      String engine,
      Map<String, Object> parameters,
      String triggerSource,
      TableIdentity tableIdentity) {
    requireIdempotencyKey(idempotencyKey);
    requireWireName(action, "action");
    requireWireName(engine, "executionEngine");
    if (!actionCatalog.isKnownAction(action)) {
      throw ApiError.of("INVALID_ACTION", "unknown action '" + action + "'");
    }
    if (!actionCatalog.supports(tableIdentity.tableFormat(), action, engine)) {
      throw ApiError.of(
          "INVALID_ENGINE", "action '" + action + "' does not support engine '" + engine + "'");
    }
    try {
      Map<String, Object> frozenParameters =
          actionCatalog.freezeManual(tableIdentity.tableFormat(), action, engine, parameters);
      ProcessCreationResult result =
          creationService.create(
              ProcessCreateIntent.resolve(
                  new ProcessResource.TableRef(
                      catalog,
                      database,
                      table,
                      tableIdentity.tableId(),
                      tableIdentity.tableFormat()),
                  action,
                  engine,
                  triggerSource,
                  idempotencyKey,
                  frozenParameters));
      return new CreateResult(result.resource(), result.replayed());
    } catch (IllegalArgumentException invalidParameters) {
      throw ApiError.of("VALIDATION_FAILED", invalidParameters.getMessage());
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
    TableIdentity tableIdentity = requireExistingTable(catalog, database, table);
    if (page < 1 || pageSize < 1 || pageSize > 50) {
      throw ApiError.of("VALIDATION_FAILED", "page must be >= 1 and pageSize within 1..50");
    }
    org.apache.amoro.process.ProcessIndexSnapshot snapshot = assembly.indexProjection().current();
    long offset = Math.multiplyExact((long) page - 1L, (long) pageSize);
    int total = snapshot.listTotal(tableIdentity.tableId(), action, status);
    return new PageResult(
        snapshot.list(tableIdentity.tableId(), action, status, offset, pageSize), total);
  }

  // ------------------------------------------------------------------ cancel

  public ProcessResource cancel(String name, String reason) {
    try {
      return commandService.requestCancel(name, reason);
    } catch (ProcessCommandException rejected) {
      throw ApiError.of(rejected.code().name(), rejected.getMessage());
    } catch (ResourceDoesNotExist missing) {
      throw ApiError.of("PROCESS_NOT_FOUND", "no process named '" + name + "'");
    }
  }

  // ------------------------------------------------------------------ resolutions

  /** Manual-resolution outcome including the idempotency replay marker exposed by HTTP. */
  public static final class ResolutionResult {
    public final ProcessResource resource;
    public final boolean replay;

    public ResolutionResult(ProcessResource resource, boolean replay) {
      this.resource = resource;
      this.replay = replay;
    }
  }

  public ProcessResource submissionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      String externalId,
      String reason) {
    return submissionResolutionResult(
            name, idempotencyKey, submissionKey, requestHash, resolution, externalId, reason)
        .resource;
  }

  public ResolutionResult submissionResolutionResult(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      String externalId,
      String reason) {
    return resolveCommand(
        name,
        new ManualResolutionTransition.Command(
            ManualResolutionTransition.Kind.SUBMISSION,
            idempotencyKey,
            submissionKey,
            requestHash,
            resolution,
            externalId,
            null,
            reason,
            "api"));
  }

  public ProcessResource executionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      Boolean retryAllowed,
      String reason) {
    return executionResolutionResult(
            name, idempotencyKey, submissionKey, requestHash, resolution, retryAllowed, reason)
        .resource;
  }

  public ResolutionResult executionResolutionResult(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      Boolean retryAllowed,
      String reason) {
    return resolveCommand(
        name,
        new ManualResolutionTransition.Command(
            ManualResolutionTransition.Kind.EXECUTION,
            idempotencyKey,
            submissionKey,
            requestHash,
            resolution,
            null,
            retryAllowed,
            reason,
            "api"));
  }

  private ResolutionResult resolveCommand(String name, ManualResolutionTransition.Command command) {
    try {
      ProcessCommandService.CommandResult result = commandService.resolve(command, name);
      return new ResolutionResult(result.resource(), result.replayed());
    } catch (ProcessCommandException rejected) {
      throw ApiError.of(rejected.code().name(), rejected.getMessage());
    } catch (ResourceDoesNotExist missing) {
      throw ApiError.of("PROCESS_NOT_FOUND", "no process named '" + name + "'");
    }
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

  private static void requireWireName(String value, String field) {
    if (value == null || !WIRE_NAME.matcher(value).matches()) {
      throw ApiError.of("VALIDATION_FAILED", field + " must match [a-z][a-z0-9-]{0,63}");
    }
  }

  private TableIdentity requireExistingTable(String catalog, String database, String table) {
    TableIdentity identity = tableCatalog.resolve(catalog, database, table);
    if (identity == null) {
      throw ApiError.of("TABLE_NOT_FOUND", catalog + "." + database + "." + table);
    }
    return identity;
  }

  private static TableCatalogPort defaultCatalog() {
    return new TableCatalogPort() {
      @Override
      public TableIdentity resolve(String catalog, String database, String table) {
        return null;
      }
    };
  }
}
