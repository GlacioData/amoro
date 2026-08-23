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

package org.apache.amoro.process;

/** Typed single-instance Process creation admission failure. */
public final class ProcessAdmissionException extends IllegalStateException {

  public enum Code {
    ACTIVE_PROCESS_EXISTS,
    IDEMPOTENCY_KEY_REUSED,
    ADMISSION_IN_PROGRESS
  }

  private final Code code;

  public ProcessAdmissionException(Code code, String message) {
    super(message);
    this.code = java.util.Objects.requireNonNull(code, "code");
  }

  public Code code() {
    return code;
  }
}
