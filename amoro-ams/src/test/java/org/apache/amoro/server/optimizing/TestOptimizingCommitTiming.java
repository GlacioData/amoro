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

package org.apache.amoro.server.optimizing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TestOptimizingCommitTiming {
  @Test
  void reportsMissingWallClockAndOnlyDurableCompletionAsEndToEnd() {
    List<String> events = new ArrayList<>();
    Logger logger =
        mock(
            Logger.class,
            call -> {
              if ("info".equals(call.getMethod().getName())) {
                events.add(
                    MessageFormatter.arrayFormat(
                            call.getArgument(0),
                            Arrays.copyOfRange(call.getArguments(), 1, call.getArguments().length))
                        .getMessage());
              }
              return RETURNS_DEFAULTS.answer(call);
            });
    when(logger.isInfoEnabled()).thenReturn(true);
    long start = System.nanoTime();
    OptimizingCommitObservation.record(
        logger,
        "table",
        7L,
        "FULL",
        "NORMAL",
        "process_lock_wait",
        start,
        "acquired",
        -1,
        false,
        -1L);
    OptimizingCommitObservation.record(
        logger,
        "table",
        7L,
        "FULL",
        "NORMAL",
        "persist_completion",
        start,
        "persistence_failed",
        System.currentTimeMillis() - 1000,
        false,
        -1L);
    OptimizingCommitObservation.record(
        logger,
        "table",
        7L,
        "FULL",
        "NORMAL",
        "persist_completion",
        start,
        "SUCCESS",
        System.currentTimeMillis() - 1000,
        true,
        -1L);
    assertTrue(events.get(0).contains("committing_status_age_ms=unavailable"));
    assertTrue(events.get(1).contains("committing_end_to_end_ms=unavailable"));
    assertFalse(events.get(2).contains("committing_end_to_end_ms=unavailable"));
    assertTrue(events.get(2).contains("processId=7"));
  }

  @Test
  void observationFailureCannotReplaceSqlFailure() {
    Logger logger =
        mock(
            Logger.class,
            call -> {
              throw new IllegalStateException("logger unavailable");
            });
    assertDoesNotThrow(
        () ->
            OptimizingCommitObservation.record(
                logger,
                "table",
                7L,
                "FULL",
                "NORMAL",
                "persist_completion",
                System.nanoTime(),
                "persistence_failed",
                -1,
                false,
                -1L));
  }
}
