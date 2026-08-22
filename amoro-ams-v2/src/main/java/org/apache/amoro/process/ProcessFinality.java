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

/**
 * The finality predicate of the Process domain (process spec §7.1): fixed terminal phases are
 * always final; FAILED is final only when the desired state is CANCEL, the retry budget is
 * exhausted or the current attempt carries a FINAL disposition — otherwise it is a retryable
 * decision point that stays schedulable.
 */
public final class ProcessFinality {

  public static final String SUCCESS = "SUCCESS";
  public static final String CANCELED = "CANCELED";
  public static final String KILLED = "KILLED";
  public static final String CLOSED = "CLOSED";
  public static final String FAILED = "FAILED";

  private ProcessFinality() {}

  public static boolean isFixedTerminal(String phase) {
    return SUCCESS.equals(phase)
        || CANCELED.equals(phase)
        || KILLED.equals(phase)
        || CLOSED.equals(phase);
  }

  public static boolean isFinal(ProcessResource resource) {
    String phase = resource.status().phase();
    if (isFixedTerminal(phase)) {
      return true;
    }
    if (!FAILED.equals(phase)) {
      return false;
    }
    if ("CANCEL".equals(resource.spec().desiredState())) {
      return true;
    }
    if (resource.status().retryNumber() >= resource.spec().retryPolicy().maxRetries()) {
      return true;
    }
    return "FINAL"
        .equals(
            resource.status().attempt() != null
                ? resource.status().attempt().retryDisposition()
                : null);
  }

  /** Active for scheduling: neither final nor a retryable FAILED still within budget. */
  public static boolean isActive(ProcessResource resource) {
    return !isFinal(resource);
  }
}
