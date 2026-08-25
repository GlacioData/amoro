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

import org.apache.amoro.resources.ProcessResource;
import org.apache.amoro.resources.ResourceList;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * The asynchronous REST-facing process contract (appmanager service-layer style): every method
 * returns a {@link CompletionStage} so the MVC layer can choose its own dereference strategy
 * without touching the contract, and every payload is a resource-family object — a single {@link
 * ProcessResource} or a {@link ResourceList} page. The default implementation completes on the
 * caller thread; the asynchronous boundary exists for future non-blocking migrations.
 *
 * <p>Idempotent commands (create and the two manual resolutions) return the resulting resource
 * uniformly, whether the call created it or replayed an existing intent; validation, admission and
 * persistence semantics live in the underlying layers and are not part of this contract.
 */
public interface ProcessService {

  /** Format-neutral lookup seam; resolves one atomic identity snapshot for create/list. */
  interface TableCatalogPort {

    /** Resolves one atomic identity snapshot, or returns null when the table is not managed. */
    TableIdentity resolve(String catalog, String database, String table);
  }

  /** Immutable table identity used for the entire create/list operation. */
  record TableIdentity(String tableId, String tableFormat) {}

  /** Creates (or idempotently replays) a process for the given table and returns the resource. */
  CompletionStage<ProcessResource> create(
      String catalog,
      String database,
      String table,
      String idempotencyKey,
      String action,
      String executionEngine,
      Map<String, Object> parameters);

  /** Fetches one process by name. */
  CompletionStage<ProcessResource> get(String name);

  /** Pages through the processes of one table; one snapshot serves the page and the total. */
  CompletionStage<ResourceList<ProcessResource>> list(
      String catalog,
      String database,
      String table,
      String action,
      String status,
      int page,
      int pageSize);

  /** Transitions the desired state RUN→CANCEL. */
  CompletionStage<ProcessResource> cancel(String name, String reason);

  /** Records a manual submission resolution (UNKNOWN/CONFLICT fallback). */
  CompletionStage<ProcessResource> submissionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      String externalId,
      String reason);

  /** Records a manual execution resolution (LOST/UNRESOLVED fallback). */
  CompletionStage<ProcessResource> executionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      Boolean retryAllowed,
      String reason);
}
