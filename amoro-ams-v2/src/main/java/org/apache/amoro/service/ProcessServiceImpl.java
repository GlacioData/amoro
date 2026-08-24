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
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Thin synchronous-backed implementation: delegates every call to {@link ProcessRestSupport} and
 * wraps the result in an already-completed stage, honoring the {@link ProcessService} contract
 * without moving any validation/admission/persistence logic. Exceptions propagate unchanged to
 * the caller thread.
 */
@Service
public class ProcessServiceImpl implements ProcessService {

  private final ProcessRestSupport support;

  public ProcessServiceImpl(ProcessRestSupport support) {
    this.support = Objects.requireNonNull(support, "support");
  }

  @Override
  public CompletionStage<ProcessRestSupport.CreateResult> create(
      String catalog,
      String database,
      String table,
      String idempotencyKey,
      String action,
      String executionEngine,
      Map<String, Object> parameters) {
    return CompletableFuture.completedFuture(
        support.create(
            catalog, database, table, idempotencyKey, action, executionEngine, parameters));
  }

  @Override
  public CompletionStage<ProcessResource> get(String name) {
    return CompletableFuture.completedFuture(support.get(name));
  }

  @Override
  public CompletionStage<ProcessRestSupport.PageResult> list(
      String catalog,
      String database,
      String table,
      String action,
      String status,
      int page,
      int pageSize) {
    return CompletableFuture.completedFuture(
        support.list(catalog, database, table, action, status, page, pageSize));
  }

  @Override
  public CompletionStage<ProcessResource> cancel(String name, String reason) {
    return CompletableFuture.completedFuture(support.cancel(name, reason));
  }

  @Override
  public CompletionStage<ProcessRestSupport.ResolutionResult> submissionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      String externalId,
      String reason) {
    return CompletableFuture.completedFuture(
        support.submissionResolutionResult(
            name, idempotencyKey, submissionKey, requestHash, resolution, externalId, reason));
  }

  @Override
  public CompletionStage<ProcessRestSupport.ResolutionResult> executionResolution(
      String name,
      String idempotencyKey,
      String submissionKey,
      String requestHash,
      String resolution,
      Boolean retryAllowed,
      String reason) {
    return CompletableFuture.completedFuture(
        support.executionResolutionResult(
            name, idempotencyKey, submissionKey, requestHash, resolution, retryAllowed, reason));
  }
}
