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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.amoro.service;

import org.apache.amoro.persistence.exception.ResourceDoesNotExist;
import org.apache.amoro.process.ManualResolutionTransition;
import org.apache.amoro.process.ProcessAdmissionException;
import org.apache.amoro.process.ProcessCommandException;
import org.apache.amoro.process.ProcessCommandService;
import org.apache.amoro.process.ProcessCreateIntent;
import org.apache.amoro.process.ProcessCreationResult;
import org.apache.amoro.process.ProcessCreationService;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.ProcessIndexSnapshot;
import org.apache.amoro.process.rest.ApiError;
import org.apache.amoro.process.rest.ProcessActionCatalog;
import org.apache.amoro.resources.ProcessResource;
import org.apache.amoro.resources.ResourceList;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/**
 * The service layer behind {@code /api/ams/v2/processes} (process spec §8). Idempotent create with
 * single-active admission, point read, one-snapshot filtered listing, monotonic cancel (merging
 * retryable-FAILED finality in the same CAS) and the two manual-resolution commands whose audit
 * records land in the same durable CAS as the state change. The service never calls engines and
 * never mutates resources except through the repository's version-CAS modify.
 *
 * <p>Every method completes on the caller thread and wraps the result in an already-completed
 * stage, honoring the {@link ProcessService} asynchronous contract without moving any
 * validation/admission/persistence logic; exceptions propagate unchanged to the caller thread.
 */
@Service
public class ProcessServiceImpl implements ProcessService {

  private static final Pattern WIRE_NAME = Pattern.compile("[a-z][a-z0-9-]{0,63}");

  private final ProcessDomainAssembly assembly;
  private final ProcessService.TableCatalogPort tableCatalog;
  private final ProcessCreationService creationService;
  private final ProcessCommandService commandService;
  private final ProcessActionCatalog actionCatalog;

  public ProcessServiceImpl(
      ProcessDomainAssembly assembly,
      ProcessService.TableCatalogPort tableCatalog,
      ProcessCreationService creationService,
      ProcessActionCatalog actionCatalog) {
    this.assembly = Objects.requireNonNull(assembly, "assembly");
    this.tableCatalog = Objects.requireNonNull(tableCatalog, "tableCatalog");
    this.creationService = Objects.requireNonNull(creationService, "creationService");
    this.commandService = new ProcessCommandService(assembly.repository());
    this.actionCatalog = Objects.requireNonNull(actionCatalog, "actionCatalog");
  }

  // ------------------------------------------------------------------ create

  @Override
  public CompletionStage<ProcessResource> create(
      String catalog,
      String database,
      String table,
      String idempotencyKey,
      String action,
      String engine,
      Map<String, Object> parameters) {
    TableIdentity tableIdentity = requireExistingTable(catalog, database, table);
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
                  "MANUAL",
                  idempotencyKey,
                  frozenParameters));
      return CompletableFuture.completedFuture(result.resource());
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

  @Override
  public CompletionStage<ProcessResource> get(String name) {
    try {
      return CompletableFuture.completedFuture(assembly.repository().get(name));
    } catch (ResourceDoesNotExist e) {
      throw ApiError.of("PROCESS_NOT_FOUND", "no process named '" + name + "'");
    }
  }

  @Override
  public CompletionStage<ResourceList<ProcessResource>> list(
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
    ProcessIndexSnapshot snapshot = assembly.indexProjection().current();
    long offset = Math.multiplyExact((long) page - 1L, (long) pageSize);
    int total = snapshot.listTotal(tableIdentity.tableId(), action, status);
    return CompletableFuture.completedFuture(
        ResourceList.<ProcessResource>builder()
            .apiVersion(ProcessResource.API_VERSION)
            .kind("ProcessResourceList")
            .metadata(
                ResourceList.ResourceListMetadata.builder()
                    .total(total)
                    .page(page)
                    .pageSize(pageSize)
                    .build())
            .items(snapshot.list(tableIdentity.tableId(), action, status, offset, pageSize))
            .build());
  }

  // ------------------------------------------------------------------ cancel

  @Override
  public CompletionStage<ProcessResource> cancel(String name, String reason) {
    try {
      return CompletableFuture.completedFuture(commandService.requestCancel(name, reason));
    } catch (ProcessCommandException rejected) {
      throw ApiError.of(rejected.code().name(), rejected.getMessage());
    } catch (ResourceDoesNotExist missing) {
      throw ApiError.of("PROCESS_NOT_FOUND", "no process named '" + name + "'");
    }
  }

  // ------------------------------------------------------------------ resolutions

  @Override
  public CompletionStage<ProcessResource> submissionResolution(
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

  @Override
  public CompletionStage<ProcessResource> executionResolution(
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

  private CompletionStage<ProcessResource> resolveCommand(
      String name, ManualResolutionTransition.Command command) {
    try {
      ProcessCommandService.CommandResult result = commandService.resolve(command, name);
      return CompletableFuture.completedFuture(result.resource());
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
}
