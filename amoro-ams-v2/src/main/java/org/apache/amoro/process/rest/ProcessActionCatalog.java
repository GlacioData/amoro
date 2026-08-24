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

import org.apache.amoro.process.engine.ProcessEngineRegistry;
import org.apache.amoro.process.trigger.ProcessActionPlugin;
import org.apache.amoro.process.trigger.ProcessActionPluginFactory;
import org.apache.amoro.process.trigger.ProcessActionRegistry;
import org.apache.amoro.process.trigger.simulated.SimulatedDummyMaintenanceActionFactory;
import org.apache.amoro.resources.ProcessResource;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable create-admission catalog derived only from selected Engine and Action providers. */
public final class ProcessActionCatalog {

  private final Map<Pair, ProcessActionPlugin> plugins;
  private final Set<String> actions;

  private ProcessActionCatalog(Map<Pair, ProcessActionPlugin> plugins) {
    this.plugins = Collections.unmodifiableMap(new LinkedHashMap<>(plugins));
    Set<String> known = new LinkedHashSet<>();
    for (Pair pair : plugins.keySet()) {
      known.add(pair.action);
    }
    this.actions = Collections.unmodifiableSet(known);
  }

  public static ProcessActionCatalog from(
      ProcessEngineRegistry engines, ProcessActionRegistry actions) {
    Objects.requireNonNull(engines, "engines");
    Objects.requireNonNull(actions, "actions");
    Map<Pair, ProcessActionPlugin> deployed = new LinkedHashMap<>();
    for (ProcessActionRegistry.Entry entry : actions.entries()) {
      for (String format : entry.tableFormats()) {
        for (String engine : engines.engines().keySet()) {
          if (entry.plugin().supports(format, engine)) {
            deployed.put(new Pair(format, entry.action(), engine), entry.plugin());
          }
        }
      }
    }
    return new ProcessActionCatalog(deployed);
  }

  /** Explicit pure-simulation routing fixture for isolated framework tests. */
  public static ProcessActionCatalog simulatedRoutingFixtures() {
    SimulatedDummyMaintenanceActionFactory factory = new SimulatedDummyMaintenanceActionFactory();
    ProcessActionPlugin plugin =
        factory.create(new ProcessActionPluginFactory.Context("test-simulation"));
    Map<Pair, ProcessActionPlugin> fixtures = new LinkedHashMap<>();
    fixtures.put(new Pair("simulated", "dummy-maintenance", "local"), plugin);
    fixtures.put(new Pair("simulated", "dummy-maintenance", "remote-spark"), plugin);
    return new ProcessActionCatalog(fixtures);
  }

  public static ProcessActionCatalog empty() {
    return new ProcessActionCatalog(Collections.emptyMap());
  }

  public boolean isKnownAction(String action) {
    return actions.contains(action);
  }

  public boolean supports(String tableFormat, String action, String engine) {
    return plugins.containsKey(new Pair(tableFormat, action, engine));
  }

  public Map<String, Object> freezeManual(
      String tableFormat, String action, String engine, Map<String, Object> parameters) {
    ProcessActionPlugin plugin = plugins.get(new Pair(tableFormat, action, engine));
    if (plugin == null) {
      throw new IllegalArgumentException(
          "no deployed action plugin for " + tableFormat + "/" + action + "/" + engine);
    }
    return plugin.validateAndFreezeManual(parameters);
  }

  /** Selects the exact deployed format/action/engine plugin without resolving a table. */
  public byte[] buildSubmission(
      ProcessResource.ProcessSpec frozenSpec, Map<String, Object> simulationProfile) {
    ProcessActionPlugin selected =
        plugins.get(
            new Pair(
                frozenSpec.table().tableFormat(),
                frozenSpec.action(),
                frozenSpec.executionEngine()));
    if (selected == null) {
      throw new IllegalStateException(
          "no deployed submission builder for action "
              + frozenSpec.action()
              + ", table format "
              + frozenSpec.table().tableFormat()
              + " and engine "
              + frozenSpec.executionEngine());
    }
    return selected.buildSubmission(frozenSpec, simulationProfile);
  }

  public Set<String> actions() {
    return actions;
  }

  private static final class Pair {
    private final String tableFormat;
    private final String action;
    private final String engine;

    private Pair(String tableFormat, String action, String engine) {
      this.tableFormat = Objects.requireNonNull(tableFormat, "tableFormat");
      this.action = Objects.requireNonNull(action, "action");
      this.engine = Objects.requireNonNull(engine, "engine");
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Pair)) {
        return false;
      }
      Pair that = (Pair) other;
      return tableFormat.equals(that.tableFormat)
          && action.equals(that.action)
          && engine.equals(that.engine);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableFormat, action, engine);
    }
  }
}
