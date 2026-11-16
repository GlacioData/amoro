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

package org.apache.amoro.server.optimizing.sorter;

import static org.apache.amoro.server.optimizing.sorter.QuotaTestSupport.optimizer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.server.resource.OptimizerInstance;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class TestOptimizerThreadCountSnapshot {
  @Test
  void loadsOnceAndPreservesExactAndNullGroups() {
    AtomicInteger reads = new AtomicInteger();
    OptimizerThreadCountSnapshot snapshot =
        new OptimizerThreadCountSnapshot(
            () -> {
              reads.incrementAndGet();
              return Arrays.asList(
                  optimizer("a", 2),
                  optimizer("a", 3),
                  optimizer("A", 7),
                  optimizer("a ", 9),
                  optimizer(null, 4));
            });
    assertEquals(0, reads.get());
    assertEquals(5, snapshot.getThreadCount("a"));
    assertEquals(7, snapshot.getThreadCount("A"));
    assertEquals(9, snapshot.getThreadCount("a "));
    assertEquals(4, snapshot.getThreadCount(null));
    assertEquals(1, snapshot.getThreadCount("missing"));
    assertEquals(1, reads.get());
  }

  @Test
  void emptyAndNullResultsAreLoadedSnapshots() {
    for (List<OptimizerInstance> result :
        Arrays.asList(Collections.<OptimizerInstance>emptyList(), null)) {
      AtomicInteger reads = new AtomicInteger();
      OptimizerThreadCountSnapshot snapshot =
          new OptimizerThreadCountSnapshot(
              () -> {
                reads.incrementAndGet();
                return result;
              });
      assertEquals(1, snapshot.getThreadCount("a"));
      assertEquals(1, snapshot.getThreadCount("b"));
      assertEquals(1, reads.get());
    }
  }

  @Test
  void clampsOnlyAfterIntSumIncludingOverflow() {
    OptimizerThreadCountSnapshot snapshot =
        new OptimizerThreadCountSnapshot(
            () ->
                Arrays.asList(
                    optimizer("negative", -2),
                    optimizer("negative", 4),
                    optimizer("overflow", Integer.MAX_VALUE),
                    optimizer("overflow", 1),
                    optimizer("zero", 0)));
    assertEquals(2, snapshot.getThreadCount("negative"));
    assertEquals(1, snapshot.getThreadCount("overflow"));
    assertEquals(1, snapshot.getThreadCount("zero"));
  }

  @Test
  void failureDoesNotPublishPartialOrEmptySnapshot() {
    AtomicInteger reads = new AtomicInteger();
    OptimizerThreadCountSnapshot snapshot =
        new OptimizerThreadCountSnapshot(
            () -> {
              if (reads.incrementAndGet() == 1) {
                throw new IllegalStateException("database unavailable");
              }
              return Collections.singletonList(optimizer("a", 8));
            });
    assertThrows(IllegalStateException.class, () -> snapshot.getThreadCount("a"));
    assertEquals(8, snapshot.getThreadCount("a"));
    assertEquals(2, reads.get());
  }

  @Test
  void nullElementIsNotSilentlySkipped() {
    OptimizerThreadCountSnapshot snapshot =
        new OptimizerThreadCountSnapshot(() -> Arrays.asList(optimizer("a", 3), null));
    assertThrows(NullPointerException.class, () -> snapshot.getThreadCount("a"));
  }
}
