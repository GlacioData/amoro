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

package org.apache.amoro.formats.paimon.optimizing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.paimon.CoreOptions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestPaimonOptimizingEligibility {

  @Test
  void testNullAndEmptyPropertiesAreIneligible() {
    assertFalse(PaimonOptimizingEligibility.isEligible((Map<String, String>) null));
    assertFalse(PaimonOptimizingEligibility.isEligible(Collections.emptyMap()));
  }

  @Test
  void testWriteOnlyUsesFalseDefault() {
    Map<String, String> properties = new HashMap<>();
    properties.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");

    assertFalse(PaimonOptimizingEligibility.isEligible(properties));
  }

  @Test
  void testSelfOptimizingMustBeExplicitlyEnabled() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CoreOptions.WRITE_ONLY.key(), "true");

    assertFalse(PaimonOptimizingEligibility.isEligible(properties));
  }

  @Test
  void testBothEffectiveValuesMustBeTrue() {
    assertTrue(PaimonOptimizingEligibility.isEligible(true, true));
    assertFalse(PaimonOptimizingEligibility.isEligible(true, false));
    assertFalse(PaimonOptimizingEligibility.isEligible(false, true));
    assertFalse(PaimonOptimizingEligibility.isEligible(false, false));
  }

  @Test
  void testBooleanTextIsCaseInsensitive() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CoreOptions.WRITE_ONLY.key(), "TRUE");
    properties.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "TrUe");

    assertTrue(PaimonOptimizingEligibility.isEligible(properties));
  }

  @Test
  void testMalformedBooleanIsIneligible() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CoreOptions.WRITE_ONLY.key(), "not-a-boolean");
    properties.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");
    assertFalse(PaimonOptimizingEligibility.isEligible(properties));

    properties.put(CoreOptions.WRITE_ONLY.key(), "true");
    properties.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, " true ");
    assertFalse(PaimonOptimizingEligibility.isEligible(properties));
  }
}
