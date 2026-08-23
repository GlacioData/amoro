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

package org.apache.amoro.process.trigger.simulated;

import org.apache.amoro.process.ProcessCreateIntent;
import org.apache.amoro.process.ProcessResource;
import org.apache.amoro.process.engine.ProviderMode;
import org.apache.amoro.process.trigger.ManagedTablePort;
import org.apache.amoro.process.trigger.ProcessActionPlugin;
import org.apache.amoro.process.trigger.ProcessActionPluginFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Pure in-memory action used only when {@code amoro.process.simulation.enabled=true}. */
public final class SimulatedDummyMaintenanceActionFactory implements ProcessActionPluginFactory {

  public static final String ACTION = "dummy-maintenance";
  public static final String TABLE_FORMAT = "simulated";

  @Override
  public String action() {
    return ACTION;
  }

  @Override
  public ProviderMode mode() {
    return ProviderMode.SIMULATED;
  }

  @Override
  public Set<String> tableFormats() {
    return Collections.singleton(TABLE_FORMAT);
  }

  @Override
  public ProcessActionPlugin create(Context context) {
    return new DummyMaintenanceAction(context.instanceId());
  }

  private static final class DummyMaintenanceAction implements ProcessActionPlugin {
    private final String instanceId;

    private DummyMaintenanceAction(String instanceId) {
      this.instanceId = instanceId;
    }

    @Override
    public String action() {
      return ACTION;
    }

    @Override
    public boolean supports(String tableFormat, String executionEngine) {
      return TABLE_FORMAT.equals(tableFormat)
          && ("local".equals(executionEngine) || "remote-spark".equals(executionEngine));
    }

    @Override
    public Map<String, Object> validateAndFreezeManual(Map<String, Object> parameters) {
      return ProcessCreateIntent.freezeParameters(
          parameters == null ? Collections.emptyMap() : parameters);
    }

    @Override
    public byte[] buildSubmission(
        ProcessResource.ProcessSpec frozenSpec, Map<String, Object> simulationProfile) {
      Map<String, Object> profile =
          validateAndFreezeManual(
              simulationProfile == null ? Collections.emptyMap() : simulationProfile);
      return (ACTION
              + "|"
              + instanceId
              + "|"
              + frozenSpec.action()
              + "|"
              + ProcessCreateIntent.canonicalParameters(frozenSpec.parameters())
              + "|"
              + ProcessCreateIntent.canonicalParameters(profile))
          .getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ScheduledEvaluation evaluateScheduled(
        ManagedTablePort.TableSnapshot table, Instant logicalFireTime) {
      if (!TABLE_FORMAT.equals(table.tableFormat())) {
        return ScheduledEvaluation.skip();
      }
      Map<String, Object> parameters = new LinkedHashMap<>();
      parameters.put("simulated", true);
      parameters.put("logicalFireTime", logicalFireTime.toString());
      return ScheduledEvaluation.create("local", validateAndFreezeManual(parameters));
    }
  }
}
