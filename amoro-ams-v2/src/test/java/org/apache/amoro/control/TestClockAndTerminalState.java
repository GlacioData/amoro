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

package org.apache.amoro.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestClockAndTerminalState {

  @Test
  public void terminalStateIsASingletonWithoutStackTrace() {
    assertTrue(TerminalState.INSTANCE == TerminalState.INSTANCE);
    TerminalState thrown =
        assertThrows(
            TerminalState.class,
            () -> {
              throw TerminalState.INSTANCE;
            });
    // writableStackTrace=false: capturing the stack is skipped, so the trace is empty
    assertEquals(0, thrown.getStackTrace().length);
    assertTrue(thrown instanceof RuntimeException);
  }

  @Test
  public void monotonicClockIsNonDecreasingAndAddsDelay() {
    Clock clock = MonotonicClock.INSTANCE;
    long first = clock.currentTimeMillisPlus(0L);
    long second = clock.currentTimeMillisPlus(0L);
    assertTrue(second >= first, "monotonic clock must never go backwards");

    long now = clock.currentTimeMillisPlus(0L);
    long later = clock.currentTimeMillisPlus(1500L);
    assertTrue(later >= now + 1500L, "delay must be added on top of the current reading");
  }

  @Test
  public void monotonicClockRejectsNegativeDelay() {
    assertThrows(
        IllegalArgumentException.class, () -> MonotonicClock.INSTANCE.currentTimeMillisPlus(-1L));
  }
}
