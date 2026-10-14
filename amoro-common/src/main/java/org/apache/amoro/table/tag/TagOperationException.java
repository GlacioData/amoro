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

package org.apache.amoro.table.tag;

/** Explicit side-effect outcome; HTTP adapters must never automatically replay a mutation. */
public class TagOperationException extends RuntimeException {
  private final int status;
  private final String reason;
  private final String outcome;

  public TagOperationException(int status, String reason, String outcome, String message) {
    super(message);
    this.status = status;
    this.reason = reason;
    this.outcome = outcome;
  }

  public TagOperationException(
      int status, String reason, String outcome, String message, Throwable cause) {
    super(message, cause);
    this.status = status;
    this.reason = reason;
    this.outcome = outcome;
  }

  public int getStatus() {
    return status;
  }

  public String getReason() {
    return reason;
  }

  public String getOutcome() {
    return outcome;
  }

  public static TagOperationException invalid(String message) {
    return new TagOperationException(400, "INVALID_ARGUMENT", "NOT_APPLIED", message);
  }

  public static TagOperationException unsupported(String action) {
    return new TagOperationException(
        422, "UNSUPPORTED_OPERATION", "NOT_APPLIED", "Tag operation is unavailable: " + action);
  }
}
