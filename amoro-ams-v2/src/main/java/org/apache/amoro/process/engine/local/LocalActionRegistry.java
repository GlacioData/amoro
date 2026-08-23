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

package org.apache.amoro.process.engine.local;

import org.apache.amoro.process.engine.ProviderMode;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** Immutable action selector for the Local Engine. */
public final class LocalActionRegistry {
  private static final Pattern WIRE_NAME = Pattern.compile("[a-z][a-z0-9-]{0,63}");

  private final Map<String, LocalAction> actions;

  private LocalActionRegistry(Map<String, LocalAction> actions) {
    this.actions = Collections.unmodifiableMap(new LinkedHashMap<>(actions));
  }

  public static LocalActionRegistry fromFactories(
      Collection<? extends LocalActionFactory> factories, ProviderMode selectedMode) {
    Objects.requireNonNull(factories, "factories");
    Objects.requireNonNull(selectedMode, "selectedMode");
    Map<String, String> identities = new LinkedHashMap<>();
    Map<String, LocalAction> selected = new LinkedHashMap<>();
    for (LocalActionFactory factory : factories) {
      String action = Objects.requireNonNull(factory, "local action factory").action();
      if (action == null || !WIRE_NAME.matcher(action).matches()) {
        throw new IllegalArgumentException("local action is not a canonical wire name: " + action);
      }
      ProviderMode mode = Objects.requireNonNull(factory.mode(), "local action mode");
      String identity = mode + "|" + action;
      String previous = identities.putIfAbsent(identity, factory.getClass().getName());
      if (previous != null) {
        throw new IllegalArgumentException(
            "duplicate local action factory "
                + identity
                + ": "
                + previous
                + " and "
                + factory.getClass().getName());
      }
      if (mode == selectedMode) {
        selected.put(action, Objects.requireNonNull(factory.create(), "local action"));
      }
    }
    return new LocalActionRegistry(selected);
  }

  public static LocalActionRegistry empty() {
    return new LocalActionRegistry(Collections.emptyMap());
  }

  public Optional<LocalAction> action(String action) {
    return Optional.ofNullable(actions.get(action));
  }

  public Map<String, LocalAction> actions() {
    return actions;
  }
}
