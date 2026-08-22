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

/**
 * A process-plane error carrying a machine-readable code (process spec §8.8). The handler maps it
 * to the right HTTP status; the serialized body is the unified error shape.
 */
public final class ApiError extends RuntimeException {

  private final String code;

  private ApiError(String code, String message) {
    super(message);
    this.code = code;
  }

  public static ApiError of(String code, String message) {
    return new ApiError(code, message);
  }

  public String code() {
    return code;
  }

  public int httpStatus() {
    switch (code) {
      case "VALIDATION_FAILED":
      case "INVALID_ACTION":
      case "INVALID_ENGINE":
      case "IDEMPOTENCY_KEY_REQUIRED":
        return 400;
      case "TABLE_NOT_FOUND":
      case "PROCESS_NOT_FOUND":
        return 404;
      case "ACTIVE_PROCESS_EXISTS":
      case "IDEMPOTENCY_KEY_REUSED":
      case "IDEMPOTENCY_IN_PROGRESS":
      case "PROCESS_ATTEMPT_STALE":
      case "SUBMISSION_RESOLUTION_CONFLICT":
      case "EXECUTION_RESOLUTION_CONFLICT":
      case "PRECONDITION_FAILED":
        return 409;
      case "PERSISTENCE_UNAVAILABLE":
      case "PERSISTENCE_OUTCOME_UNKNOWN":
      case "ENGINE_CONTROL_UNAVAILABLE":
        return 503;
      default:
        return 500;
    }
  }
}
