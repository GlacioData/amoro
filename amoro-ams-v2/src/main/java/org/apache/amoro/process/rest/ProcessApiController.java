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

import org.apache.amoro.process.ProcessResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/** {@code /api/ams/v2} process endpoints (process spec §8.2). */
@RestController
@RequestMapping("/api/ams/v2")
public class ProcessApiController {

  private final ProcessRestSupport support;

  public ProcessApiController(ProcessRestSupport support) {
    this.support = support;
  }

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = false)
  public static final class CreateRequest {
    public String action;
    public String executionEngine;
    public Map<String, Object> parameters;
  }

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = false)
  public static final class CancelRequest {
    public String desiredState;
    public String reason;
  }

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = false)
  public static final class SubmissionResolutionRequest {
    public String submissionKey;
    public String requestHash;
    public String resolution;
    public String externalId;
    public String reason;
  }

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = false)
  public static final class ExecutionResolutionRequest {
    public String submissionKey;
    public String requestHash;
    public String resolution;
    public Boolean retryAllowed;
    public String reason;
  }

  @PostMapping("/tables/{catalog}/{db}/{table}/processes")
  public ResponseEntity<ProcessResource> create(
      @PathVariable("catalog") String catalog,
      @PathVariable("db") String db,
      @PathVariable("table") String table,
      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
      @RequestBody CreateRequest request) {
    ProcessRestSupport.CreateResult result =
        support.create(
            catalog,
            db,
            table,
            idempotencyKey,
            request.action,
            request.executionEngine,
            request.parameters);
    return ResponseEntity.status(result.replay ? HttpStatus.OK : HttpStatus.CREATED)
        .header("Idempotency-Replayed", String.valueOf(result.replay))
        .body(result.resource);
  }

  @GetMapping("/processes/{name}")
  public ProcessResource get(@PathVariable("name") String name) {
    return support.get(name);
  }

  @GetMapping("/tables/{catalog}/{db}/{table}/processes")
  public Map<String, Object> listProcesses(
      @PathVariable("catalog") String catalog,
      @PathVariable("db") String db,
      @PathVariable("table") String table,
      @RequestParam(value = "action", required = false) String action,
      @RequestParam(value = "status", required = false) String status,
      @RequestParam(value = "page", defaultValue = "1") int page,
      @RequestParam(value = "pageSize", defaultValue = "20") int pageSize) {
    // one snapshot serves both the page and the total (no cross-version inconsistency)
    ProcessRestSupport.PageResult pageResult =
        support.list(catalog, db, table, action, status, page, pageSize);
    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("items", new ArrayList<>(pageResult.items));
    body.put("total", pageResult.total);
    body.put("page", page);
    body.put("pageSize", pageSize);
    return body;
  }

  @PatchMapping("/processes/{name}")
  public ProcessResource cancel(
      @PathVariable("name") String name, @RequestBody CancelRequest request) {
    if (request == null || !"CANCEL".equals(request.desiredState)) {
      throw ApiError.of(
          "VALIDATION_FAILED", "PATCH accepts only {\"desiredState\":\"CANCEL\"} in this version");
    }
    return support.cancel(name);
  }

  @PostMapping("/processes/{name}/submission-resolutions")
  public ProcessResource submissionResolution(
      @PathVariable("name") String name,
      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
      @RequestBody SubmissionResolutionRequest request) {
    return support.submissionResolution(
        name,
        idempotencyKey,
        request.submissionKey,
        request.requestHash,
        request.resolution,
        request.externalId,
        request.reason);
  }

  @PostMapping("/processes/{name}/execution-resolutions")
  public ProcessResource executionResolution(
      @PathVariable("name") String name,
      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
      @RequestBody ExecutionResolutionRequest request) {
    return support.executionResolution(
        name,
        idempotencyKey,
        request.submissionKey,
        request.requestHash,
        request.resolution,
        request.retryAllowed,
        request.reason);
  }
}
