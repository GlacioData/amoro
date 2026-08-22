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

public class TestBackoffPolicy {

  private final BackoffPolicy policy = new BackoffPolicy();

  /** Deterministic random source pinned to the smallest legal value (jitter = 0). */
  private static final class ZeroRandom implements RandomSupplier {
    @Override
    public long nextNonNegative(long upperBound) {
      return 0L;
    }
  }

  /** Deterministic random source pinned to the largest legal value (jitter = 249). */
  private static final class MaxRandom implements RandomSupplier {
    @Override
    public long nextNonNegative(long upperBound) {
      return upperBound - 1L;
    }
  }

  @Test
  public void emittedBackoffSequenceMatchesSpecExactly() {
    // Spec §4.2: the emitted sequence is {3,3,5,8,13,21,34,55}s. The value is taken for the
    // current attempt first and the attempt counter is incremented afterwards, so unlike the
    // reference implementation the leading 3s is emitted (fidelity ledger: off-by-one fix).
    long[] expectedMillis = {
      3000L, 3000L, 5000L, 8000L, 13000L, 21000L, 34000L, 55000L, 55000L, 55000L, 55000L, 55000L
    };
    for (int attempt = 0; attempt < expectedMillis.length; attempt++) {
      long actual = policy.nextBackoffDelayMillis(attempt, new ZeroRandom());
      assertEquals(
          expectedMillis[attempt],
          actual,
          "emitted backoff for attempt " + attempt + " must match the spec sequence");
    }
  }

  @Test
  public void tailIsCappedAtFiftyFiveSeconds() {
    for (int attempt = 7; attempt < 100; attempt++) {
      assertEquals(55000L, policy.nextBackoffDelayMillis(attempt, new ZeroRandom()));
    }
  }

  @Test
  public void jitterBoundsAreZeroInclusiveAndTwoHundredFiftyExclusive() {
    assertEquals(3000L, policy.nextBackoffDelayMillis(0, new ZeroRandom()));
    assertEquals(3249L, policy.nextBackoffDelayMillis(0, new MaxRandom()));
    // 250ms is unreachable: RandomSupplier contract is [0, upperBound).
    assertEquals(55000L, policy.nextBackoffDelayMillis(20, new ZeroRandom()));
    assertEquals(55249L, policy.nextBackoffDelayMillis(20, new MaxRandom()));
  }

  @Test
  public void productionRandomSupplierStaysWithinHalfOpenJitterBound() {
    RandomSupplier random = ThreadLocalRandomSupplier.INSTANCE;
    for (int i = 0; i < 10_000; i++) {
      long jitter = random.nextNonNegative(BackoffPolicy.MAX_JITTER_MILLIS);
      assertTrue(jitter >= 0L && jitter < BackoffPolicy.MAX_JITTER_MILLIS, "jitter in [0,250)");
      long delay = policy.nextBackoffDelayMillis(0, random);
      assertTrue(delay >= 3000L && delay < 3250L, "delay in [3000,3250)");
    }
  }

  @Test
  public void defensiveBranchesRejectIllegalArguments() {
    assertThrows(
        IllegalArgumentException.class, () -> policy.nextBackoffDelayMillis(-1, new ZeroRandom()));
    assertThrows(
        IllegalArgumentException.class,
        () -> ThreadLocalRandomSupplier.INSTANCE.nextNonNegative(0L));
  }
}
