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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.server.persistence.OptimizingPayloadException;
import org.apache.amoro.server.persistence.OptimizingPayloadException.FailureReason;
import org.junit.jupiter.api.Test;

import java.sql.SQLTransientException;

class TestOptimizingRecoveryPolicy {

  @Test
  void usesBoundedExponentialBackoff() {
    assertEquals(30_000L, OptimizingQueue.recoveryRetryDelayMillis(1));
    assertEquals(60_000L, OptimizingQueue.recoveryRetryDelayMillis(2));
    assertEquals(120_000L, OptimizingQueue.recoveryRetryDelayMillis(3));
    assertEquals(240_000L, OptimizingQueue.recoveryRetryDelayMillis(4));
    assertEquals(300_000L, OptimizingQueue.recoveryRetryDelayMillis(5));
    assertEquals(300_000L, OptimizingQueue.recoveryRetryDelayMillis(32));
  }

  @Test
  void classifiesPayloadFailuresAsPermanentThroughPersistenceWrapper() {
    OptimizingPayloadException payloadFailure =
        new OptimizingPayloadException(FailureReason.CORRUPT_GZIP, "safe metadata only");
    PersistenceException wrapped = new PersistenceException("load failed", payloadFailure);

    assertSame(payloadFailure, OptimizingQueue.permanentRecoveryFailure(wrapped));
  }

  @Test
  void classifiesLegacyDescriptorMismatchAsPermanent() {
    OptimizingQueue.CorruptedRecoveryDataException mismatch =
        new OptimizingQueue.CorruptedRecoveryDataException(
            "descriptor type mismatch", new ClassCastException("wrong descriptor"));

    assertSame(mismatch, OptimizingQueue.permanentRecoveryFailure(mismatch));
  }

  @Test
  void keepsSqlAndUnclassifiedRuntimeFailuresRetryable() {
    assertNull(
        OptimizingQueue.permanentRecoveryFailure(
            new PersistenceException("temporary", new SQLTransientException("lock timeout"))));
    assertNull(OptimizingQueue.permanentRecoveryFailure(new IllegalStateException("temporary")));
    assertNull(OptimizingQueue.permanentRecoveryFailure(new IllegalArgumentException("unknown")));
  }

  @Test
  void failedSingleFlightEntryDoesNotReleaseActiveRecovery() {
    OptimizingQueue.RecoveryGate gate = new OptimizingQueue.RecoveryGate();

    OptimizingQueue.RecoveryKey active = gate.tryEnter(42L, 1001L);
    assertNotNull(active);
    assertNull(gate.tryEnter(42L, 1001L));
    assertNull(gate.tryEnter(42L, 1001L));
    assertNotNull(gate.tryEnter(42L, 1002L));
    assertNotNull(gate.tryEnter(43L, 1001L));

    gate.exit(active);
    assertNotNull(gate.tryEnter(42L, 1001L));
  }
}
