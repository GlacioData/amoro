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

package org.apache.amoro.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TestScoreUtil {

  @Test
  void clampsIntegralHealthScoresToTheInclusiveRange() {
    assertEquals(0, ScoreUtil.clampScore(-1L));
    assertEquals(0, ScoreUtil.clampScore(0L));
    assertEquals(57, ScoreUtil.clampScore(57L));
    assertEquals(100, ScoreUtil.clampScore(100L));
    assertEquals(100, ScoreUtil.clampScore(101L));
  }

  @Test
  void preservesLegacyFloatingPointClampSemantics() {
    assertEquals(0.0D, ScoreUtil.clampPercentage(Double.NaN));
    assertEquals(0.0D, ScoreUtil.clampPercentage(Double.NEGATIVE_INFINITY));
    assertEquals(37.5D, ScoreUtil.clampPercentage(37.5D));
    assertEquals(100.0D, ScoreUtil.clampPercentage(Double.POSITIVE_INFINITY));
  }
}
