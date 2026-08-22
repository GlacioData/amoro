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

package org.apache.amoro.process.engine;

import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.EngineCapabilities;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;

import java.util.concurrent.CompletionStage;

/**
 * The v2 engine contract (process spec §6.1). Strict classification rules:
 *
 * <ul>
 *   <li>NOT_FOUND is only for an authoritative "this key/id does not exist" protocol answer —
 *       timeouts, 5xx and parse failures are UNAVAILABLE;
 *   <li>submit is stricter than the rest: only provably-never-sent failures are UNAVAILABLE;
 *       anything with undetermined side effects is UNKNOWN;
 *   <li>LOST means the local execution registry/intent was lost while side effects may exist — it
 *       is never equivalent to NOT_FOUND and never auto-resubmitted.
 * </ul>
 */
public interface ProcessEnginePort {

  /** Immutable local snapshot; implementations must not perform I/O here. */
  EngineCapabilities capabilities();

  /**
   * @param submissionKey idempotent identity: processName:retryNumber:dispatchGeneration
   * @param requestHash the attempt's canonical submission hash
   */
  CompletionStage<SubmissionOutcome> submit(
      String submissionKey, String requestHash, byte[] submissionPayload);

  CompletionStage<SubmissionResolution> resolveSubmission(String submissionKey, String requestHash);

  CompletionStage<ProcessObservation> observe(String externalId);

  CompletionStage<CancellationOutcome> cancel(String externalId);

  /** Idempotent resource release after the execution's terminal result is durable. */
  CompletionStage<Void> release(String externalId);
}
