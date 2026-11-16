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

import static org.apache.amoro.server.optimizing.sorter.QuotaTestSupport.runtime;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.amoro.server.AmoroServiceConstants;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class TestQuotaOccupySorter {
  @Test
  void absoluteQuotasSortWithoutDatabaseAndKeepEqualInputOrder() {
    long occupied = AmoroServiceConstants.QUOTA_LOOK_BACK_TIME;
    DefaultTableRuntime first = runtime("first", "g", 4, occupied);
    DefaultTableRuntime second = runtime("second", "g", 2, occupied);
    DefaultTableRuntime equal = runtime("equal", "g", 4, occupied);
    List<DefaultTableRuntime> tables = Arrays.asList(second, first, equal);
    tables.sort(new QuotaOccupySorter().createComparator());
    assertEquals(Arrays.asList(first, equal, second), tables);
  }

  @Test
  void runtimeExtensionsKeepOriginalDynamicDispatchAndPerTableCaching() {
    AtomicInteger calls = new AtomicInteger();
    DefaultTableRuntime extension =
        new DefaultTableRuntime(QuotaTestSupport.store("extension", "g", 0.5), () -> null) {
          @Override
          public double calculateQuotaOccupy() {
            calls.incrementAndGet();
            return 0.75;
          }
        };
    DefaultTableRuntime other =
        runtime("other", "g", 2, AmoroServiceConstants.QUOTA_LOOK_BACK_TIME);
    java.util.Comparator<DefaultTableRuntime> comparator =
        new QuotaOccupySorter().createComparator();
    assertEquals(1, comparator.compare(extension, other));
    assertEquals(-1, comparator.compare(other, extension));
    assertEquals(1, calls.get());
  }
}
