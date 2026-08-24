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

package org.apache.amoro.service;

import org.apache.amoro.process.rest.ProcessRestSupport;
import org.apache.amoro.resources.ProcessResource;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * The asynchronous REST-facing process contract (appmanager service-layer style): every method
 * returns a {@link CompletionStage} so the MVC layer can choose its own dereference strategy
 * without touching the contract. The default implementation completes on the caller thread by
 * delegating to {@link ProcessRestSupport}; the asynchronous boundary exists for future
 * non-blocking migrations.
 *
 * <p>Validation, admission and persistence semantics live in the underlying layers and are not
 * part of this contract's responsibility.
 */
public interface ProcessService {

  /**
   * Creates (or idempotently replays) a process for the given table; see {@link
   * ProcessRestSupport#create}.
   */
  CompletionStage<ProcessRestSupport.CreateResult> create(
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
  CompletionStage<ProcessRestSupport.PageResult> list(
      String catalog,
      String database,
      String table,
      String action,
      String status,
      int page,
      int pageSize);

  /** Transitions the desired state RUN→CANCEL. */
  CompletionStage<ProcessResource> cancel(String name, String reason);

  /** Records a manual submission resolution; see {@code ProcessRestSupport}. */
  CompletionStage<ProcessRestSupport.ResolutionResult> submissionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      String externalId,
      String reason);

  /** Records a manual execution resolution; see {@code ProcessRestSupport}. */
  CompletionStage<ProcessRestSupport.ResolutionResult> executionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      Boolean retryAllowed,
      String reason);
}
