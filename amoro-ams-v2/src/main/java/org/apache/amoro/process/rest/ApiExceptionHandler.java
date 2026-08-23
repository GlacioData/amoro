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

import org.apache.amoro.persistence.exception.PersistenceException;
import org.apache.amoro.persistence.exception.PersistenceOutcomeUnknownException;
import org.apache.amoro.persistence.exception.PostCommitCleanupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Unified error body {@code {code,message,timestamp,traceId}} for /api/ams/v2 (spec §8.8). */
@RestControllerAdvice
public class ApiExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);

  @ExceptionHandler(ApiError.class)
  public ResponseEntity<Map<String, Object>> apiError(ApiError error) {
    if ("IDEMPOTENCY_IN_PROGRESS".equals(error.code())) {
      return ResponseEntity.status(error.httpStatus())
          .header("Retry-After", "1")
          .body(errorBody(error.code(), error.getMessage(), UUID.randomUUID().toString()));
    }
    return respond(HttpStatus.valueOf(error.httpStatus()), error.code(), error.getMessage());
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<Map<String, Object>> malformed(HttpMessageNotReadableException e) {
    return respond(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "malformed request body");
  }

  @ExceptionHandler({
    org.springframework.web.method.annotation.MethodArgumentTypeMismatchException.class,
    IllegalArgumentException.class
  })
  public ResponseEntity<Map<String, Object>> badArguments(Exception e) {
    return respond(
        HttpStatus.BAD_REQUEST,
        "VALIDATION_FAILED",
        "invalid query or path argument: " + e.getMessage());
  }

  @ExceptionHandler(PersistenceException.class)
  public ResponseEntity<Map<String, Object>> persistenceDown(PersistenceException e) {
    LOG.warn("Persistence unavailable for a /api/ams/v2 request: {}", e.getMessage());
    return respond(
        HttpStatus.SERVICE_UNAVAILABLE,
        "PERSISTENCE_UNAVAILABLE",
        "the durable store is unavailable; retry later");
  }

  @ExceptionHandler(PersistenceOutcomeUnknownException.class)
  public ResponseEntity<Map<String, Object>> outcomeUnknown(PersistenceOutcomeUnknownException e) {
    return respond(
        HttpStatus.SERVICE_UNAVAILABLE,
        "PERSISTENCE_OUTCOME_UNKNOWN",
        "the durable outcome is unknown; the key is fenced — do not retry with a new id");
  }

  @ExceptionHandler(PostCommitCleanupException.class)
  public ResponseEntity<Map<String, Object>> cleanupFenced(PostCommitCleanupException e) {
    return respond(
        HttpStatus.CONFLICT, "PRECONDITION_FAILED", "the name is fenced pending cleanup repair");
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<Map<String, Object>> internal(Exception e) {
    String traceId = UUID.randomUUID().toString();
    LOG.error("Unhandled /api/ams/v2 failure; traceId={}", traceId, e);
    return respond(
        HttpStatus.INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "internal error (no stack or SQL details are exposed)",
        traceId);
  }

  private static ResponseEntity<Map<String, Object>> respond(
      HttpStatus status, String code, String message) {
    return respond(status, code, message, UUID.randomUUID().toString());
  }

  private static ResponseEntity<Map<String, Object>> respond(
      HttpStatus status, String code, String message, String traceId) {
    return ResponseEntity.status(status).body(errorBody(code, message, traceId));
  }

  private static Map<String, Object> errorBody(String code, String message, String traceId) {
    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("code", code);
    body.put("message", message);
    body.put("timestamp", Instant.now().toString());
    body.put("traceId", traceId);
    return body;
  }
}
