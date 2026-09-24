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

package org.apache.amoro.server.table;

import org.apache.amoro.Action;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.process.ActionCoordinator;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.FormatPendingInput;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableRuntimeFactory;
import org.apache.amoro.table.TableRuntimeStore;
import org.apache.amoro.table.TableRuntimeSupport;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Default {@link TableRuntimeFactory} implementation used by AMS.
 *
 * <p>Creates {@link DefaultTableRuntime} instances for all supported formats, with format-specific
 * pending-input state keys. Also aggregates {@link ProcessFactory} declarations to expose {@link
 * ActionCoordinator} plugins for different {@link TableFormat}/{@link Action} combinations.
 */
public class DefaultTableRuntimeFactory implements TableRuntimeFactory {

  /** Mapping from table format to its supported actions and corresponding process factory. */
  private final Map<TableFormat, Map<Action, ProcessFactory>> factoriesByFormat = new HashMap<>();

  /** Coordinators derived from all installed process factories. */
  private final List<ActionCoordinator> supportedCoordinators = Lists.newArrayList();

  private Function<ServerTableIdentifier, AmoroTable<?>> loader;
  private final TableFormatSupports formatSupports;

  public DefaultTableRuntimeFactory() {
    this(TableFormatSupports.instance());
  }

  DefaultTableRuntimeFactory(TableFormatSupports formatSupports) {
    this.formatSupports = formatSupports;
  }

  @Override
  public List<ActionCoordinator> supportedCoordinators() {
    return Collections.unmodifiableList(supportedCoordinators);
  }

  @Override
  public void initialize(List<ProcessFactory> factories) {
    factoriesByFormat.clear();
    supportedCoordinators.clear();

    for (ProcessFactory factory : factories) {
      Map<TableFormat, Set<Action>> supported = factory.supportedActions();
      if (supported == null || supported.isEmpty()) {
        continue;
      }

      for (Map.Entry<TableFormat, Set<Action>> entry : supported.entrySet()) {
        TableFormat format = entry.getKey();
        Map<Action, ProcessFactory> byAction =
            factoriesByFormat.computeIfAbsent(format, k -> new HashMap<>());

        for (Action action : entry.getValue()) {
          ProcessFactory existed = byAction.get(action);
          if (existed != null && existed != factory) {
            throw new IllegalArgumentException(
                String.format(
                    "ProcessFactory conflict for format %s and action %s, existing: %s, new: %s",
                    format, action, existed.name(), factory.name()));
          }
          byAction.put(action, factory);
          supportedCoordinators.add(new DefaultActionCoordinator(format, action, factory));
        }
      }
    }
  }

  @Override
  public void withTableLoader(Function<ServerTableIdentifier, AmoroTable<?>> loader) {
    this.loader = loader;
  }

  @Override
  public Optional<TableRuntimeCreator> accept(
      ServerTableIdentifier tableIdentifier, Map<String, String> tableProperties) {
    return formatSupports
        .runtimeSupport(tableIdentifier.getFormat())
        .map(support -> new TableRuntimeCreatorImpl(tableIdentifier.getFormat(), support));
  }

  private class TableRuntimeCreatorImpl implements TableRuntimeFactory.TableRuntimeCreator {

    private final TableFormat format;
    private final TableRuntimeSupport support;
    private final StateKey<? extends FormatPendingInput> pendingInputKey;

    private TableRuntimeCreatorImpl(TableFormat format, TableRuntimeSupport support) {
      this.format = format;
      this.support = support;
      this.pendingInputKey = support.pendingInputKey();
    }

    @Override
    public List<StateKey<?>> requiredStateKeys() {
      Map<String, StateKey<?>> merged = new LinkedHashMap<>();
      DefaultTableRuntime.REQUIRED_STATES.forEach(key -> merged.put(key.getKey(), key));
      addState(merged, pendingInputKey, "format " + format);
      support.requiredStates().forEach(key -> addState(merged, key, "format " + format));
      formatSupports
          .healthSupport(format)
          .requiredStates()
          .forEach(key -> addState(merged, key, "health support " + format));

      Map<Action, ProcessFactory> byAction = factoriesByFormat.get(format);
      if (byAction != null) {
        Map<String, ProcessFactory> deFactories = new HashMap<>();
        byAction.forEach((a, f) -> deFactories.putIfAbsent(f.name(), f));
        deFactories
            .values()
            .forEach(
                factory ->
                    factory
                        .requiredStates()
                        .forEach(
                            stateKey ->
                                addState(merged, stateKey, "process-factory " + factory.name())));
      }

      return Lists.newArrayList(merged.values());
    }

    @Override
    public TableRuntime create(TableRuntimeStore store) {
      return new DefaultTableRuntime(
          store, () -> loader.apply(store.getTableIdentifier()), pendingInputKey);
    }
  }

  private static void addState(Map<String, StateKey<?>> states, StateKey<?> key, String owner) {
    if (states.putIfAbsent(key.getKey(), key) != null) {
      throw new IllegalStateException(
          "State key " + key.getKey() + " declared by " + owner + " has already been defined");
    }
  }
}
