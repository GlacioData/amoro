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

package org.apache.amoro.optimizing;

import org.apache.amoro.table.FormatPendingInput;

import java.util.Optional;

/** Result of a format-specific pending input evaluation. */
public class PendingInputResult {

  private final FormatPendingInput pendingInput;
  private final FormatPendingInput optimizingPendingInput;
  private final boolean optimizingNecessary;
  private final FormatTableAnalysis tableAnalysis;
  private final long nextEvaluationTime;
  private final boolean planningRetry;

  public PendingInputResult(FormatPendingInput pendingInput, boolean optimizingNecessary) {
    this(pendingInput, pendingInput, optimizingNecessary, null);
  }

  public PendingInputResult(
      FormatPendingInput pendingInput,
      boolean optimizingNecessary,
      FormatTableAnalysis tableAnalysis) {
    this(pendingInput, pendingInput, optimizingNecessary, tableAnalysis);
  }

  public PendingInputResult(
      FormatPendingInput pendingInput,
      FormatPendingInput optimizingPendingInput,
      boolean optimizingNecessary) {
    this(pendingInput, optimizingPendingInput, optimizingNecessary, null);
  }

  public PendingInputResult(
      FormatPendingInput pendingInput,
      FormatPendingInput optimizingPendingInput,
      boolean optimizingNecessary,
      FormatTableAnalysis tableAnalysis) {
    this(pendingInput, optimizingPendingInput, optimizingNecessary, tableAnalysis, 0L);
  }

  public PendingInputResult(
      FormatPendingInput pendingInput,
      FormatPendingInput optimizingPendingInput,
      boolean optimizingNecessary,
      FormatTableAnalysis tableAnalysis,
      long nextEvaluationTime) {
    this(
        pendingInput,
        optimizingPendingInput,
        optimizingNecessary,
        tableAnalysis,
        nextEvaluationTime,
        false);
  }

  /** Keep completed health facts while retrying an inconclusive planning evaluation. */
  public static PendingInputResult retryPlanning(FormatTableAnalysis analysis, long retryAtMillis) {
    if (retryAtMillis <= 0L) {
      throw new IllegalArgumentException("Planning retry requires a deadline");
    }
    return new PendingInputResult(
        analysis.pendingInput(), null, false, analysis, retryAtMillis, true);
  }

  private PendingInputResult(
      FormatPendingInput pendingInput,
      FormatPendingInput optimizingPendingInput,
      boolean optimizingNecessary,
      FormatTableAnalysis tableAnalysis,
      long nextEvaluationTime,
      boolean planningRetry) {
    this.pendingInput = pendingInput;
    this.optimizingPendingInput =
        optimizingPendingInput == null ? pendingInput : optimizingPendingInput;
    this.optimizingNecessary = optimizingNecessary;
    this.tableAnalysis = tableAnalysis;
    this.nextEvaluationTime = nextEvaluationTime;
    this.planningRetry = planningRetry;
  }

  public FormatPendingInput pendingInput() {
    return pendingInput;
  }

  public FormatPendingInput optimizingPendingInput() {
    return optimizingPendingInput;
  }

  public boolean optimizingNecessary() {
    return optimizingNecessary;
  }

  /** Earliest requested reevaluation time in epoch milliseconds; zero means no deadline. */
  public long nextEvaluationTime() {
    return nextEvaluationTime;
  }

  public boolean planningRetry() {
    return planningRetry;
  }

  public Optional<FormatTableAnalysis> tableAnalysis() {
    return Optional.ofNullable(tableAnalysis);
  }
}
