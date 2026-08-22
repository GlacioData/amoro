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

package org.apache.amoro.process.rest;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The first-version (tableFormat, action, executionEngine) allowlist (process spec §6.2). The L2
 * business decision on the exact first-version scope is still open; until it closes, the catalog
 * below mirrors the spec's candidate matrix, and create-time validation rejects any unlisted pair
 * with INVALID_ACTION/INVALID_ENGINE instead of failing later at runtime.
 */
public final class ProcessActionCatalog {

  /** action -> engine -> true. */
  private static final Map<String, Map<String, Boolean>> SUPPORTED;

  static {
    Map<String, Map<String, Boolean>> matrix = new LinkedHashMap<String, Map<String, Boolean>>();
    Map<String, Boolean> expire = new LinkedHashMap<String, Boolean>();
    expire.put("local", true);
    expire.put("remote-spark", true);
    matrix.put("expire-snapshots", Collections.unmodifiableMap(expire));
    Map<String, Boolean> orphans = new LinkedHashMap<String, Boolean>();
    orphans.put("local", true);
    orphans.put("remote-spark", true);
    matrix.put("clean-orphans", Collections.unmodifiableMap(orphans));
    Map<String, Boolean> sync = new LinkedHashMap<String, Boolean>();
    sync.put("local", true);
    matrix.put("sync-table-meta", Collections.unmodifiableMap(sync));
    SUPPORTED = Collections.unmodifiableMap(matrix);
  }

  private ProcessActionCatalog() {}

  public static boolean isKnownAction(String action) {
    return SUPPORTED.containsKey(action);
  }

  public static boolean supports(String action, String engine) {
    Map<String, Boolean> engines = SUPPORTED.get(action);
    return engines != null && engines.containsKey(engine);
  }
}
