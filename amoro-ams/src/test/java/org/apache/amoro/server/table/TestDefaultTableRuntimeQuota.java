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

package org.apache.amoro.server.table;

import static org.apache.amoro.server.optimizing.sorter.QuotaTestSupport.runtime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.server.AmoroServiceConstants;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class TestDefaultTableRuntimeQuota {
  @Test
  void preservesFormulaAndLazyBranchForSpecialValues() {
    double[] quotas = {
      0.5, 1, 1.1, 2.9, 0, -0.5, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY
    };
    for (double quota : quotas) {
      for (long occupied : new long[] {0, AmoroServiceConstants.QUOTA_LOOK_BACK_TIME}) {
        AtomicInteger reads = new AtomicInteger();
        DefaultTableRuntime runtime = runtime("formula", "group", quota, occupied);
        double actual =
            runtime.calculateQuotaOccupy(
                () -> {
                  reads.incrementAndGet();
                  return 3;
                });
        int limit = quota > 1 ? (int) quota : (int) Math.ceil(quota * 3);
        double expected = (double) occupied / AmoroServiceConstants.QUOTA_LOOK_BACK_TIME / limit;
        assertEquals(expected, actual, "quota=" + quota);
        assertEquals(quota > 1 ? 0 : 1, reads.get());
      }
    }
  }

  @Test
  void absoluteQuotaDoesNotReadAndNoArgEntryRemainsUsable() {
    DefaultTableRuntime runtime =
        runtime("absolute", "group", 2.9, AmoroServiceConstants.QUOTA_LOOK_BACK_TIME);
    assertEquals(
        0.5,
        runtime.calculateQuotaOccupy(
            () -> {
              throw new AssertionError("eager read");
            }));
    assertEquals(0.5, runtime.calculateQuotaOccupy());
  }

  @Test
  void resourceFailurePropagatesForProportionalQuota() {
    DefaultTableRuntime runtime = runtime("failure", "group", 0.5, 0);
    assertThrows(
        IllegalStateException.class,
        () ->
            runtime.calculateQuotaOccupy(
                () -> {
                  throw new IllegalStateException("DB");
                }));
  }
}
