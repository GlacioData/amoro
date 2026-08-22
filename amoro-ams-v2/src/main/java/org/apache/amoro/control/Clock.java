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
 * Monotonic scheduling clock. Deliberately not {@code java.time.Clock}, which is wall-clock based
 * and therefore subject to NTP jumps; scheduler deadlines and backoff consume this injectable
 * monotonic time base only. Business timestamps (created/finished at) use {@code java.time.Clock}
 * instead — the two time bases must not be mixed.
 */
public interface Clock {

  /**
   * Returns the current monotonic millisecond reading plus {@code delayInMillis}.
   *
   * @throws IllegalArgumentException if {@code delayInMillis} is negative
   * @throws ArithmeticException if the sum overflows
   */
  long currentTimeMillisPlus(long delayInMillis);
}
