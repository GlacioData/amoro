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

import java.util.concurrent.TimeUnit;

/** Production {@link Clock} backed by {@link System#nanoTime()}. */
public enum MonotonicClock implements Clock {
  INSTANCE;

  /** Captured once so that wraparound of the raw nanoTime reading never enters the math. */
  private final long startNanos = System.nanoTime();

  @Override
  public long currentTimeMillisPlus(long delayInMillis) {
    if (delayInMillis < 0L) {
      throw new IllegalArgumentException("delayInMillis must be >= 0, got " + delayInMillis);
    }
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    return Math.addExact(elapsedMillis, delayInMillis);
  }
}
