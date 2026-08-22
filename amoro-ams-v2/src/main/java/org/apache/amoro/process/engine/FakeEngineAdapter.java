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
import org.apache.amoro.process.engine.EngineTypes.EngineFailure;
import org.apache.amoro.process.engine.EngineTypes.EngineObservation;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Scriptable in-memory engine for unit tests and the remote-spark simulation (the user decision:
 * remote submission is simulated in tests, no real Spark service). Maintains a submission registry
 * and an execution registry keyed by submissionKey/externalId; behavior hooks let each test stage
 * outcomes, failures and lost-registry windows deterministically.
 */
public class FakeEngineAdapter implements ProcessEnginePort {

  /** Optional per-call behavior override; returning null falls through to the registry. */
  public interface Behavior {
    default SubmissionOutcome onSubmit(String submissionKey) {
      return null;
    }

    default SubmissionResolution onResolve(String submissionKey) {
      return null;
    }

    default ProcessObservation onObserve(String externalId) {
      return null;
    }

    default CancellationOutcome onCancel(String externalId) {
      return null;
    }
  }

  private final Map<String, String> acknowledgedBySubmissionKey =
      new ConcurrentHashMap<String, String>(); // submissionKey -> externalId
  private final Map<String, EngineObservation> executionsByExternalId =
      new ConcurrentHashMap<String, EngineObservation>();
  private volatile Behavior behavior = new Behavior() {};

  public void setBehavior(Behavior behavior) {
    this.behavior = behavior == null ? new Behavior() {} : behavior;
  }

  @Override
  public EngineCapabilities capabilities() {
    return new EngineCapabilities(true, true, "fake-v1");
  }

  @Override
  public CompletionStage<SubmissionOutcome> submit(
      String submissionKey, String requestHash, byte[] submissionPayload) {
    SubmissionOutcome override = behavior.onSubmit(submissionKey);
    if (override != null) {
      if (override.kind() == SubmissionOutcome.Kind.ACKNOWLEDGED) {
        acknowledgedBySubmissionKey.put(submissionKey, override.externalId());
        executionsByExternalId.put(
            override.externalId(), new EngineObservation("SUBMITTED", null, null, null));
      }
      return CompletableFuture.completedFuture(override);
    }
    String externalId = "fake-app-" + Math.abs(submissionKey.hashCode());
    acknowledgedBySubmissionKey.put(submissionKey, externalId);
    executionsByExternalId.put(externalId, new EngineObservation("SUBMITTED", null, null, null));
    return CompletableFuture.completedFuture(SubmissionOutcome.acknowledged(externalId));
  }

  @Override
  public CompletionStage<SubmissionResolution> resolveSubmission(
      String submissionKey, String requestHash) {
    SubmissionResolution override = behavior.onResolve(submissionKey);
    if (override != null) {
      return CompletableFuture.completedFuture(override);
    }
    String externalId = acknowledgedBySubmissionKey.get(submissionKey);
    return CompletableFuture.completedFuture(
        externalId == null
            ? SubmissionResolution.notFound()
            : SubmissionResolution.acknowledged(externalId));
  }

  @Override
  public CompletionStage<ProcessObservation> observe(String externalId) {
    ProcessObservation override = behavior.onObserve(externalId);
    if (override != null) {
      return CompletableFuture.completedFuture(override);
    }
    EngineObservation observation = executionsByExternalId.get(externalId);
    return CompletableFuture.completedFuture(
        observation == null
            ? ProcessObservation.notFound()
            : ProcessObservation.known(observation));
  }

  @Override
  public CompletionStage<CancellationOutcome> cancel(String externalId) {
    CancellationOutcome override = behavior.onCancel(externalId);
    if (override != null) {
      return CompletableFuture.completedFuture(override);
    }
    EngineObservation observation = executionsByExternalId.get(externalId);
    if (observation == null) {
      return CompletableFuture.completedFuture(CancellationOutcome.notFound());
    }
    executionsByExternalId.put(
        externalId,
        new EngineObservation(
            "CANCELED", observation.trackUri(), observation.summaryDelta(), null));
    return CompletableFuture.completedFuture(CancellationOutcome.accepted());
  }

  @Override
  public CompletionStage<Void> release(String externalId) {
    executionsByExternalId.remove(externalId);
    return CompletableFuture.completedFuture(null);
  }

  /** Test staging helpers. */
  public void stageExecution(String externalId, String remotePhase, boolean retryableFailure) {
    Map<String, Object> summary = new LinkedHashMap<String, Object>();
    summary.put("staged", true);
    executionsByExternalId.put(
        externalId,
        retryableFailure
            ? new EngineObservation(
                "FAILED", null, summary, new EngineFailure("E_STAGED", "staged failure", true))
            : new EngineObservation(remotePhase, null, summary, null));
  }

  public void dropExecution(String externalId) {
    executionsByExternalId.remove(externalId);
  }

  public void dropSubmission(String submissionKey) {
    acknowledgedBySubmissionKey.remove(submissionKey);
  }
}
