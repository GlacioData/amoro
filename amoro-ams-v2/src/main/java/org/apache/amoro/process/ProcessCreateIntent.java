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

package org.apache.amoro.process;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Canonical, already-authorized creation intent shared by manual and scheduled entry points. */
public final class ProcessCreateIntent {

  private final ProcessResource.TableRef table;
  private final String action;
  private final String executionEngine;
  private final String triggerSource;
  private final String idempotencyKeyHash;
  private final String requestHash;
  private final Map<String, Object> parameters;

  public ProcessCreateIntent(
      ProcessResource.TableRef table,
      String action,
      String executionEngine,
      String triggerSource,
      String idempotencyKeyHash,
      String requestHash,
      Map<String, Object> parameters) {
    this.table = Objects.requireNonNull(table, "table");
    this.action = Objects.requireNonNull(action, "action");
    this.executionEngine = Objects.requireNonNull(executionEngine, "executionEngine");
    this.triggerSource = Objects.requireNonNull(triggerSource, "triggerSource");
    this.idempotencyKeyHash =
        Objects.requireNonNull(idempotencyKeyHash, "idempotencyKeyHash");
    this.requestHash = Objects.requireNonNull(requestHash, "requestHash");
    this.parameters =
        Collections.unmodifiableMap(
            new LinkedHashMap<>(Objects.requireNonNull(parameters, "parameters")));
  }

  public ProcessResource.TableRef table() {
    return table;
  }

  public String action() {
    return action;
  }

  public String executionEngine() {
    return executionEngine;
  }

  public String triggerSource() {
    return triggerSource;
  }

  public String idempotencyKeyHash() {
    return idempotencyKeyHash;
  }

  public String requestHash() {
    return requestHash;
  }

  public Map<String, Object> parameters() {
    return parameters;
  }

  String admissionScope() {
    return table.tableId() + "|" + action;
  }
}
