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

import org.apache.amoro.resources.ProcessResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Pure helpers for the type-unique persisted Process condition set. */
final class ProcessConditions {

  static final String SUBMISSION_UNRESOLVED = "SubmissionUnresolved";
  static final String EXECUTION_UNRESOLVED = "ExecutionUnresolved";
  static final String ENGINE_UNREACHABLE = "EngineUnreachable";
  static final String CANCELLATION_UNSUPPORTED = "CancellationUnsupported";
  static final String DATA_REPAIRED = "DataRepaired";

  private ProcessConditions() {}

  static Optional<ProcessResource.Condition> find(
      List<ProcessResource.Condition> conditions, String type) {
    return conditions.stream().filter(condition -> type.equals(condition.type())).findFirst();
  }

  static boolean isTrue(List<ProcessResource.Condition> conditions, String type) {
    return find(conditions, type).map(condition -> "True".equals(condition.status())).orElse(false);
  }

  static List<ProcessResource.Condition> set(
      List<ProcessResource.Condition> conditions,
      String type,
      String reason,
      String message,
      String now,
      String capabilityVersion) {
    List<ProcessResource.Condition> next = remove(conditions, type);
    next.add(
        new ProcessResource.Condition(type, "True", reason, message, now, now, capabilityVersion));
    return next;
  }

  static List<ProcessResource.Condition> remove(
      List<ProcessResource.Condition> conditions, String... types) {
    java.util.Set<String> removed = new java.util.HashSet<>(java.util.Arrays.asList(types));
    List<ProcessResource.Condition> next = new ArrayList<>();
    for (ProcessResource.Condition condition : conditions) {
      if (!removed.contains(condition.type())) {
        next.add(condition);
      }
    }
    return next;
  }
}
