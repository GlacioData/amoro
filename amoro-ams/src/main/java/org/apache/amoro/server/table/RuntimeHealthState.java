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
import org.apache.amoro.table.health.TableHealthEvaluation;
import org.apache.amoro.table.health.TableHealthSupport;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Atomic in-process format health result and evaluation-attempt state. */
final class RuntimeHealthState {

  private final TableHealthSupport support;
  private static final State EMPTY = new State(null, null, 0);

  private final AtomicReference<State> state = new AtomicReference<>(EMPTY);
  private final LongSupplier clock;
  private long issuedAttempt;
  private long generationFloor;

  synchronized long beginAttempt() {
    return ++issuedAttempt;
  }

  RuntimeHealthState(TableHealthSupport support) {
    this(support, System::currentTimeMillis);
  }

  RuntimeHealthState(TableHealthSupport support, LongSupplier clock) {
    this.support = support;
    this.clock = clock;
  }

  Optional<RuntimeHealthSnapshot> snapshot(TableAnalysisKey observed) {
    State current = state.get();
    if (current.snapshot == null) {
      return Optional.empty();
    }
    RuntimeHealthSnapshot result = current.snapshot;
    EvaluationAttempt attempt = current.attempt;
    boolean stale =
        observed == null
            || !support.sameInput(observed, result.getHealthDetails())
            || attempt.outcome != EvaluationOutcome.SUCCESS;
    String status =
        attempt.outcome == EvaluationOutcome.SUCCESS
            ? (stale ? "STALE" : "SUCCESS")
            : result.getHealthDetails().getStructure() != null && !result.isSuccessful()
                ? "PARTIAL"
                : "FAILED";
    TableHealthEvaluation evaluation =
        new TableHealthEvaluation(
            status,
            stale,
            observed == null || observed.getSnapshotId() < 0
                ? null
                : Long.toString(observed.getSnapshotId()),
            attempt.details.getSnapshotId() == null
                ? null
                : attempt.details.getSnapshotId().toString(),
            attempt.atMillis,
            attempt.outcome == EvaluationOutcome.RETRYABLE_FAILURE,
            attempt.details.getReasonCodes());
    return Optional.of(
        new RuntimeHealthSnapshot(result.getHealthScore(), result.getHealthDetails(), evaluation));
  }

  Optional<RuntimeHealthSnapshot> snapshot() {
    return Optional.ofNullable(state.get().snapshot);
  }

  boolean shouldEvaluate(TableAnalysisKey key) {
    EvaluationAttempt attempt = state.get().attempt;
    return attempt == null
        || !support.sameInput(key, attempt.details)
        || (attempt.outcome == EvaluationOutcome.RETRYABLE_FAILURE
            && clock.getAsLong() >= attempt.retryAtMillis);
  }

  /** Records an evaluation and returns whether its summary should replace the current Gauges. */
  synchronized boolean update(FormatTableAnalysis analysis) {
    return update(analysis, beginAttempt());
  }

  /** Reject completions from an older attempt or a cleared runtime generation. */
  synchronized boolean update(FormatTableAnalysis analysis, long sequence) {
    return update(analysis.healthDetails(), analysis.pendingInput().getHealthScore(), sequence);
  }

  synchronized void fail(TableAnalysisKey key, long sequence) {
    if (key != null) {
      update(
          new TableHealthDetails(
              key.getFormulaVersion(),
              key.getSnapshotId(),
              key.getChangeSnapshotId(),
              key.getSchemaId(),
              key.getScoringConfigFingerprint(),
              key.encoded(),
              null,
              null,
              java.util.Collections.singletonList(support.readFailureReason())),
          -1,
          sequence);
    }
  }

  private boolean update(TableHealthDetails details, int healthScore, long sequence) {
    if (sequence <= generationFloor || sequence < state.get().sequence) {
      return false;
    }
    boolean successful = healthScore >= 0 && healthScore <= 100;
    EvaluationOutcome outcome =
        successful
            ? EvaluationOutcome.SUCCESS
            : support.retryable(details)
                ? EvaluationOutcome.RETRYABLE_FAILURE
                : EvaluationOutcome.TERMINAL_FAILURE;
    while (true) {
      State current = state.get();
      int failures =
          current.attempt != null
                  && details.getEvaluationKey().equals(current.attempt.evaluationKey)
                  && current.attempt.outcome == EvaluationOutcome.RETRYABLE_FAILURE
              ? current.attempt.failures + 1
              : 1;
      EvaluationAttempt attempt =
          new EvaluationAttempt(details, outcome, clock.getAsLong(), Math.min(5, failures));
      boolean replaceSnapshot =
          successful || current.snapshot == null || !current.snapshot.isSuccessful();
      RuntimeHealthSnapshot nextSnapshot =
          replaceSnapshot ? new RuntimeHealthSnapshot(healthScore, details) : current.snapshot;
      if (state.compareAndSet(current, new State(nextSnapshot, attempt, sequence))) {
        return replaceSnapshot;
      }
    }
  }

  synchronized void clear() {
    generationFloor = ++issuedAttempt;
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
    private final TableHealthDetails details;
    private final long atMillis;
    private final long retryAtMillis;
    private final int failures;

    private EvaluationAttempt(
        TableHealthDetails details, EvaluationOutcome outcome, long atMillis, int failures) {
      this.details = details;
      this.atMillis = atMillis;
      this.failures = failures;
      this.retryAtMillis = atMillis + Math.min(60000L, 5000L << (failures - 1));
      this.evaluationKey = details.getEvaluationKey();
      this.outcome = Objects.requireNonNull(outcome, "Evaluation outcome must not be null");
    }
  }

  private static final class State {
    private final RuntimeHealthSnapshot snapshot;
    private final EvaluationAttempt attempt;
    private final long sequence;

    private State(RuntimeHealthSnapshot snapshot, EvaluationAttempt attempt, long sequence) {
      this.sequence = sequence;
      this.snapshot = snapshot;
      this.attempt = attempt;
    }
  }
}
