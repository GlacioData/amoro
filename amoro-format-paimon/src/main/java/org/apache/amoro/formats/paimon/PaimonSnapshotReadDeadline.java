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

package org.apache.amoro.formats.paimon;

import static org.apache.amoro.table.descriptor.SnapshotReadException.Reason.READ_FAILED;
import static org.apache.amoro.table.descriptor.SnapshotReadException.Reason.TIMEOUT;
import static org.apache.amoro.table.descriptor.SnapshotReadException.Reason.UNAVAILABLE;

import org.apache.amoro.table.descriptor.SnapshotReadException;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Cooperative request budget. Blocking FileIO still needs finite client timeouts. */
final class PaimonSnapshotReadDeadline {
  private final LongSupplier clock;
  private final long started;
  private final long budget;

  PaimonSnapshotReadDeadline() {
    this(System::nanoTime, TimeUnit.SECONDS.toNanos(30));
  }

  PaimonSnapshotReadDeadline(LongSupplier clock, long budget) {
    this.clock = clock;
    this.started = clock.getAsLong();
    this.budget = budget;
  }

  void check() {
    if (Thread.currentThread().isInterrupted()) {
      throw new SnapshotReadException(UNAVAILABLE, "Snapshot read interrupted");
    }
    if (clock.getAsLong() - started >= budget) {
      throw new SnapshotReadException(TIMEOUT, "Snapshot read exceeded its request budget");
    }
  }

  <T> T call(Callable<T> action) {
    check();
    try {
      T value = action.call();
      check();
      return value;
    } catch (SnapshotReadException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      check();
      throw new SnapshotReadException(READ_FAILED, "Failed to read snapshot metadata", e);
    }
  }
}
