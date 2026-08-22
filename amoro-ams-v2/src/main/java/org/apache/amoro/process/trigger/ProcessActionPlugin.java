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

package org.apache.amoro.process.trigger;

import java.time.Instant;
import java.util.Map;

/**
 * The per-action gate and intent freezer (process spec §6.3): the scanner asks whether a table is
 * due for the action at this logical fire time; a positive answer hands back the canonical
 * parameters that are frozen into the Process spec at create time.
 */
public interface ProcessActionPlugin {

  String action();

  boolean supports(String tableFormat, String executionEngine);

  ScheduledEvaluation evaluateScheduled(
      ManagedTablePort.TableSnapshot table, Instant logicalFireTime);

  /** One of: create with frozen parameters, or skip this window. */
  final class ScheduledEvaluation {
    private final Object parameters;
    private final boolean create;

    private ScheduledEvaluation(Map<String, Object> parameters, boolean create) {
      this.parameters = parameters;
      this.create = create;
    }

    public static ScheduledEvaluation create(Map<String, Object> parameters) {
      return new ScheduledEvaluation(parameters, true);
    }

    public static ScheduledEvaluation skip() {
      return new ScheduledEvaluation(null, false);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> parameters() {
      return (Map<String, Object>) parameters;
    }

    public boolean shouldCreate() {
      return create;
    }
  }
}
