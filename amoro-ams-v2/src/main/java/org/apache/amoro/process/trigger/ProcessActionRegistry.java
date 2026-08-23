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

import org.apache.amoro.process.engine.ProviderMode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Immutable selected-mode action registry. */
public final class ProcessActionRegistry {

  private static final Pattern WIRE_NAME = Pattern.compile("[a-z][a-z0-9-]{0,63}");

  private final List<Entry> entries;

  private ProcessActionRegistry(List<Entry> entries) {
    this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
  }

  public static ProcessActionRegistry fromFactories(
      Collection<? extends ProcessActionPluginFactory> factories,
      ProviderMode activeMode,
      ProcessActionPluginFactory.Context context) {
    Objects.requireNonNull(factories, "factories");
    Objects.requireNonNull(activeMode, "activeMode");
    Objects.requireNonNull(context, "context");
    Map<String, String> identities = new LinkedHashMap<>();
    List<Entry> selected = new ArrayList<>();
    for (ProcessActionPluginFactory factory : factories) {
      Objects.requireNonNull(factory, "action factory");
      String action = requireWireName(factory.action(), "action");
      ProviderMode mode = Objects.requireNonNull(factory.mode(), "action factory mode");
      Set<String> formats =
          new LinkedHashSet<>(Objects.requireNonNull(factory.tableFormats(), "tableFormats"));
      if (formats.isEmpty()) {
        throw new IllegalArgumentException(
            "action factory " + action + " declares no table format");
      }
      for (String format : formats) {
        requireWireName(format, "table format");
        String identity = mode + "|" + format + "|" + action;
        String previous = identities.putIfAbsent(identity, factory.getClass().getName());
        if (previous != null) {
          throw new IllegalArgumentException(
              "duplicate action factory identity "
                  + identity
                  + ": "
                  + previous
                  + " and "
                  + factory.getClass().getName());
        }
      }
      if (mode == activeMode) {
        ProcessActionPlugin plugin =
            Objects.requireNonNull(factory.create(context), "action factory result");
        if (!action.equals(plugin.action())) {
          throw new IllegalArgumentException(
              "action factory " + action + " created plugin named " + plugin.action());
        }
        selected.add(new Entry(action, formats, plugin, mode));
      }
    }
    return new ProcessActionRegistry(selected);
  }

  public List<Entry> entries() {
    return entries;
  }

  private static String requireWireName(String value, String label) {
    if (value == null || !WIRE_NAME.matcher(value).matches()) {
      throw new IllegalArgumentException(label + " is not a canonical wire name: " + value);
    }
    return value;
  }

  /** One selected factory and its declared table formats. */
  public static final class Entry {
    private final String action;
    private final Set<String> tableFormats;
    private final ProcessActionPlugin plugin;
    private final ProviderMode mode;

    private Entry(
        String action, Set<String> tableFormats, ProcessActionPlugin plugin, ProviderMode mode) {
      this.action = action;
      this.tableFormats = Collections.unmodifiableSet(new LinkedHashSet<>(tableFormats));
      this.plugin = plugin;
      this.mode = mode;
    }

    public String action() {
      return action;
    }

    public Set<String> tableFormats() {
      return tableFormats;
    }

    public ProcessActionPlugin plugin() {
      return plugin;
    }

    public ProviderMode mode() {
      return mode;
    }
  }
}
