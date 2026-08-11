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

package org.apache.amoro.server.table;

import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** Atomic in-process Paimon health result and evaluation-attempt state. */
final class RuntimeHealthState {

  private static final String SNAPSHOT_SCAN_FAILED = "SNAPSHOT_SCAN_FAILED";
  private static final State EMPTY = new State(null, null);

  private final AtomicReference<State> state = new AtomicReference<>(EMPTY);

  Optional<RuntimeHealthSnapshot> snapshot() {
    return Optional.ofNullable(state.get().snapshot);
  }

  boolean shouldEvaluate(TableAnalysisKey key) {
    EvaluationAttempt attempt = state.get().attempt;
    return attempt == null
        || !key.encoded().equals(attempt.evaluationKey)
        || attempt.outcome == EvaluationOutcome.RETRYABLE_FAILURE;
  }

  /** Records an evaluation and returns whether its summary should replace the current Gauges. */
  boolean update(FormatTableAnalysis analysis) {
    TableHealthDetails details = analysis.healthDetails();
    int healthScore = analysis.pendingInput().getHealthScore();
    boolean successful = healthScore >= 0 && healthScore <= 100;
    EvaluationOutcome outcome =
        successful
            ? EvaluationOutcome.SUCCESS
            : details.getReasonCodes().contains(SNAPSHOT_SCAN_FAILED)
                ? EvaluationOutcome.RETRYABLE_FAILURE
                : EvaluationOutcome.TERMINAL_FAILURE;
    EvaluationAttempt attempt = new EvaluationAttempt(details.getEvaluationKey(), outcome);

    while (true) {
      State current = state.get();
      boolean replaceSnapshot =
          successful || current.snapshot == null || !current.snapshot.isSuccessful();
      RuntimeHealthSnapshot nextSnapshot =
          replaceSnapshot ? new RuntimeHealthSnapshot(healthScore, details) : current.snapshot;
      if (state.compareAndSet(current, new State(nextSnapshot, attempt))) {
        return replaceSnapshot;
      }
    }
  }

  void clear() {
    state.set(EMPTY);
  }

  private enum EvaluationOutcome {
    SUCCESS,
    RETRYABLE_FAILURE,
    TERMINAL_FAILURE
  }

  private static final class EvaluationAttempt {
    private final String evaluationKey;
    private final EvaluationOutcome outcome;

    private EvaluationAttempt(String evaluationKey, EvaluationOutcome outcome) {
      this.evaluationKey = Objects.requireNonNull(evaluationKey, "Evaluation key must not be null");
      this.outcome = Objects.requireNonNull(outcome, "Evaluation outcome must not be null");
    }
  }

  private static final class State {
    private final RuntimeHealthSnapshot snapshot;
    private final EvaluationAttempt attempt;

    private State(RuntimeHealthSnapshot snapshot, EvaluationAttempt attempt) {
      this.snapshot = snapshot;
      this.attempt = attempt;
    }
  }
}
