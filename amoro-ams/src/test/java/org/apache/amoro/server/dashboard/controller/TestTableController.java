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

package org.apache.amoro.server.dashboard.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.properties.HiveTableProperties;
import org.apache.amoro.server.dashboard.ServerTableProperties;
import org.apache.amoro.server.dashboard.utils.AmsUtil;
import org.apache.amoro.table.TableProperties;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

public class TestTableController {

  @Test
  void testMatchesSearchKeywordIgnoringCase() {
    assertTrue(TableController.matchesSearchKeyword("Orders", "orders"));
    assertTrue(TableController.matchesSearchKeyword("orders", "ORDERS"));
    assertFalse(TableController.matchesSearchKeyword("customers", "orders"));
  }

  @Test
  void testBlankSearchKeywordMatchesEveryTable() {
    assertTrue(TableController.matchesSearchKeyword("orders", null));
    assertTrue(TableController.matchesSearchKeyword("orders", ""));
    assertTrue(TableController.matchesSearchKeyword("orders", "  "));
  }

  /**
   * The cached hive-upgrade properties must equal what the original per-request computation
   * produced; they are pure functions of static constants fixed for the process lifetime. The
   * toString comparison additionally pins the TreeMap ordering that JSON serialization observes.
   */
  @Test
  void testUpgradeHiveTablePropertiesMatchPerRequestComputation() throws Exception {
    Map<String, String> expected = computeUpgradeHiveTablePropertiesPerRequest();
    Map<String, String> actual = loadUpgradeHiveTableProperties();

    assertEquals(expected, actual);
    assertEquals(expected.toString(), actual.toString());
  }

  /** Verbatim copy of the pre-cache per-request logic, used as the oracle. */
  private static Map<String, String> computeUpgradeHiveTablePropertiesPerRequest()
      throws IllegalAccessException {
    Map<String, String> keyValues = new TreeMap<>();
    Map<String, String> tableProperties =
        AmsUtil.getNotDeprecatedAndNotInternalStaticFields(TableProperties.class);
    tableProperties.keySet().stream()
        .filter(key -> !key.endsWith("_DEFAULT"))
        .forEach(
            key -> keyValues.put(tableProperties.get(key), tableProperties.get(key + "_DEFAULT")));
    ServerTableProperties.HIDDEN_EXPOSED.forEach(keyValues::remove);
    Map<String, String> hiveProperties =
        AmsUtil.getNotDeprecatedAndNotInternalStaticFields(HiveTableProperties.class);

    hiveProperties.keySet().stream()
        .filter(key -> HiveTableProperties.EXPOSED.contains(hiveProperties.get(key)))
        .filter(key -> !key.endsWith("_DEFAULT"))
        .forEach(
            key -> keyValues.put(hiveProperties.get(key), hiveProperties.get(key + "_DEFAULT")));
    return keyValues;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> loadUpgradeHiveTableProperties() throws Exception {
    Method load = TableController.class.getDeclaredMethod("loadUpgradeHiveTableProperties");
    load.setAccessible(true);
    return (Map<String, String>) load.invoke(null);
  }
}
