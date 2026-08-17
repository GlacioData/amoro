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

import org.apache.paimon.CoreOptions;

import java.util.Map;

/** Determines whether a Paimon table is eligible for Amoro optimizing and maintenance. */
public final class PaimonOptimizingEligibility {

  public static final String SELF_OPTIMIZING_ENABLED = "self-optimizing.enabled";

  private static final boolean WRITE_ONLY_DEFAULT = false;
  private static final boolean SELF_OPTIMIZING_ENABLED_DEFAULT = false;

  private PaimonOptimizingEligibility() {}

  public static boolean isEligible(Map<String, String> tableProperties) {
    return isEligible(writeOnly(tableProperties), selfOptimizingEnabled(tableProperties));
  }

  public static boolean isEligible(boolean writeOnly, boolean selfOptimizingEnabled) {
    return writeOnly && selfOptimizingEnabled;
  }

  public static boolean writeOnly(Map<String, String> tableProperties) {
    return booleanValue(tableProperties, CoreOptions.WRITE_ONLY.key(), WRITE_ONLY_DEFAULT);
  }

  public static boolean selfOptimizingEnabled(Map<String, String> tableProperties) {
    return booleanValue(tableProperties, SELF_OPTIMIZING_ENABLED, SELF_OPTIMIZING_ENABLED_DEFAULT);
  }

  private static boolean booleanValue(
      Map<String, String> tableProperties, String key, boolean defaultValue) {
    if (tableProperties == null || !tableProperties.containsKey(key)) {
      return defaultValue;
    }
    String value = tableProperties.get(key);
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }
    return false;
  }
}
