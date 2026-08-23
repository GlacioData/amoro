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

import org.apache.amoro.process.ProcessResource.EngineBackoff;
import org.apache.amoro.process.ProcessResource.ProcessAttempt;
import org.apache.amoro.process.ProcessResource.ProcessStatus;

/** Pure transition for the monotonic operator intent {@code desiredState: RUN -> CANCEL}. */
public final class ToCancelTransition {

  private ToCancelTransition() {}

  public static ProcessResource requestCancel(ProcessResource current, String now) {
    if (ProcessFinality.isFinal(current) || "CANCEL".equals(current.spec().desiredState())) {
      return current;
    }
    ProcessStatus status = current.status();
    ProcessStatus next;
    if ("FAILED".equals(status.phase())) {
      String failure =
          status.attempt() != null && status.attempt().lastError() != null
              ? status.attempt().lastError()
              : "FAILED";
      ProcessAttempt closed =
          status.attempt() == null
              ? null
              : new ProcessAttempt(
                  status.attempt().dispatchGeneration(),
                  status.attempt().submissionKey(),
                  status.attempt().requestHash(),
                  status.attempt().submitState(),
                  status.attempt().externalId(),
                  status.attempt().dispatchedAt(),
                  failure,
                  "FINAL",
                  status.attempt().finishedAt() == null ? now : status.attempt().finishedAt(),
                  status.attempt().submissionHistory(),
                  status.attempt().manualResolutions());
      next =
          new ProcessStatus(
              "FAILED",
              status.retryNumber(),
              closed,
              status.attemptHistory(),
              status.lastObservedAt(),
              status.lastCancelAttemptAt(),
              null,
              new EngineBackoff(0, 0, 0, 0),
              ProcessConditions.remove(
                  status.conditions(),
                  ProcessConditions.SUBMISSION_UNRESOLVED,
                  ProcessConditions.EXECUTION_UNRESOLVED,
                  ProcessConditions.ENGINE_UNREACHABLE,
                  ProcessConditions.CANCELLATION_UNSUPPORTED),
              status.summary(),
              failure,
              status.submittedAt(),
              status.startedAt(),
              now);
    } else {
      next =
          new ProcessStatus(
              status.phase(),
              status.retryNumber(),
              status.attempt(),
              status.attemptHistory(),
              status.lastObservedAt(),
              status.lastCancelAttemptAt(),
              now,
              status.engineBackoffAttempts(),
              status.conditions(),
              status.summary(),
              status.failure(),
              status.submittedAt(),
              status.startedAt(),
              status.finishedAt());
    }
    return current.withSpec(current.spec().withDesiredState("CANCEL")).withStatus(next);
  }
}
