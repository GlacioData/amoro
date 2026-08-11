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

package org.apache.amoro.formats.paimon.optimizing.primary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.optimizing.OptimizingType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

@DisplayName("Paimon primary-key optimizing evaluation")
class TestPaimonPrimaryKeyOptimizingEvaluation {

  @Test
  @DisplayName("empty evaluation keeps target snapshot id and has no candidates")
  void emptyEvaluationKeepsTargetSnapshotId() {
    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluation.empty(12L);

    assertFalse(evaluation.necessary());
    assertEquals(12L, evaluation.targetSnapshotId());
    assertTrue(evaluation.units().isEmpty());
    assertFalse(evaluation.analysis().isPresent());
    assertFalse(evaluation.fullCompaction());
  }

  @Test
  @DisplayName("non-empty evaluation exposes type, full flag and units")
  void nonEmptyEvaluationExposesDecision() {
    PaimonBucketCompactionUnit unit =
        new PaimonBucketCompactionUnit(new byte[0], 1, 5, 100, 20, 10);
    List<PaimonBucketCompactionUnit> units = new ArrayList<>();
    units.add(unit);
    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluation.of(units, OptimizingType.MAJOR, true, 20L);
    units.clear();

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.MAJOR, evaluation.optimizingType());
    assertTrue(evaluation.fullCompaction());
    assertEquals(20L, evaluation.targetSnapshotId());
    assertEquals(1, evaluation.units().size());
    assertEquals(1, evaluation.units().get(0).getBucket());
    assertThrows(UnsupportedOperationException.class, () -> evaluation.units().add(unit));
  }
}
