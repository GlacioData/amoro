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

package org.apache.amoro.server.dashboard.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TestParamSignatureCalculator {

  @Test
  public void testGenerateParamStringWithValueList() {
    // Prepare input and mock classes
    Map<String, List<String>> testMap = new HashMap<>();
    testMap.put("name", Collections.singletonList(""));
    testMap.put("value", Collections.singletonList("111"));
    testMap.put("age", Collections.singletonList("11"));
    testMap.put("sex", Collections.singletonList("1"));
    testMap.put("high", Collections.singletonList("180"));
    testMap.put("nick", Collections.singletonList(""));

    // Call the actual method
    String result = ParamSignatureCalculator.generateParamStringWithValueList(testMap);

    // Assert the result
    Assertions.assertEquals("age11high180sex1value111", result);
  }

  @Test
  void generateTablePageToken() {
    Map<String, String> map = new HashMap<>();
    map.put("age", "11");
    map.put("value", "");
    map.put("high", "180");
    map.put("sex", "1");
    map.put("name", "111");

    String result = ParamSignatureCalculator.generateParamStringWithValue(map);

    // The expected string is: age11high180sex1value111
    Assertions.assertEquals("age11high180name111sex1", result);
  }

  /**
   * The fixed-moment equivalent of {@link ParamSignatureCalculator#currentDateString()}: both sides
   * format the same instant with the same {@code yyyyMMdd} pattern in the system default zone and
   * locale, the left via a per-call SimpleDateFormat, the right via a thread-confined formatter
   * mirroring the one owned by ParamSignatureCalculator.
   */
  @Test
  public void testYyyyMMddPatternEquivalenceAtFixedMoments() {
    List<LocalDate> fixedDates =
        List.of(LocalDate.of(2026, 10, 29), LocalDate.of(2024, 2, 29)); // includes a leap day

    ThreadLocal<SimpleDateFormat> threadConfined =
        ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));

    for (LocalDate fixed : fixedDates) {
      Date fixedDate = Date.from(fixed.atStartOfDay(ZoneId.systemDefault()).toInstant());
      String legacy = new SimpleDateFormat("yyyyMMdd").format(fixedDate);
      String current = threadConfined.get().format(fixedDate);
      Assertions.assertEquals(legacy, current);
    }
  }

  /**
   * currentDateString() must agree with a legacy per-call SimpleDateFormat formatting of the
   * current moment. Repeated 100 times to strengthen the check; a flake would require the two calls
   * of an iteration to straddle local midnight.
   */
  @Test
  public void testCurrentDateStringMatchesLegacySimpleDateFormat() {
    for (int i = 0; i < 100; i++) {
      String legacy = new SimpleDateFormat("yyyyMMdd").format(new Date());
      Assertions.assertEquals(legacy, ParamSignatureCalculator.currentDateString());
    }
  }

  /**
   * Regression guard for locales whose calendar or digits differ from ISO Gregorian/ASCII (e.g.
   * Thai Buddhist era, where 2026 formats as 2569): currentDateString() must keep following the
   * JVM-default locale exactly like a locally constructed SimpleDateFormat, so the signature bytes
   * are unchanged.
   *
   * <p>The comparison runs on a fresh thread because the thread-confined formatter captures the
   * default locale at first use per thread, mirroring how the legacy static instance captured it at
   * class-load time; a JUnit worker thread already warmed under the original locale would keep that
   * locale, which would make this check depend on test execution order.
   */
  @Test
  public void testCurrentDateStringKeepsJvmDefaultLocaleCalendar() throws InterruptedException {
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("th", "TH")); // Buddhist calendar: 2026 CE formats as 2569
      AtomicReference<String> legacy = new AtomicReference<>();
      AtomicReference<String> current = new AtomicReference<>();
      Thread thread =
          new Thread(
              () -> {
                legacy.set(new SimpleDateFormat("yyyyMMdd").format(new Date()));
                current.set(ParamSignatureCalculator.currentDateString());
              });
      thread.start();
      thread.join();
      Assertions.assertEquals(legacy.get(), current.get());
    } finally {
      Locale.setDefault(original);
    }
  }
}
