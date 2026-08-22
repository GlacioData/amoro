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

/**
 * Fixed backoff sequence plus jitter (framework spec §4.2): the emitted sequence is exactly
 * {3,3,5,8,13,21,34,55} seconds, capped at 55s beyond the eighth failure, plus jitter uniform in
 * [0,250) ms. Retries are unlimited.
 *
 * <p>Stateless: the attempt counter lives in the scheduled controller, which must take the value
 * for the current attempt first and increment afterwards — the reference implementation
 * pre-increments and thereby silently drops the leading 3s entry, a defect this port fixes
 * (fidelity ledger, backoff off-by-one).
 */
public final class BackoffPolicy {

  static final long[] BACKOFF_MILLIS = {3000L, 3000L, 5000L, 8000L, 13000L, 21000L, 34000L, 55000L};

  public static final long MAX_JITTER_MILLIS = 250L;

  /**
   * @param backOffAttempts the attempt counter value before this failure is accounted
   * @param random injectable jitter source
   * @return base backoff for the given attempt plus jitter in [0, 250) ms
   */
  public long nextBackoffDelayMillis(int backOffAttempts, RandomSupplier random) {
    if (backOffAttempts < 0) {
      throw new IllegalArgumentException("backOffAttempts must be >= 0, got " + backOffAttempts);
    }
    int index = Math.min(backOffAttempts, BACKOFF_MILLIS.length - 1);
    return BACKOFF_MILLIS[index] + random.nextNonNegative(MAX_JITTER_MILLIS);
  }
}
