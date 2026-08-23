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
import org.apache.amoro.process.trigger.ProcessActionRegistry;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Immutable create-admission catalog derived from deployed Engine and Action providers. */
public final class ProcessActionCatalog {

  private final Set<Pair> pairs;
  private final Set<String> actions;

  private ProcessActionCatalog(Set<Pair> pairs) {
    this.pairs = Collections.unmodifiableSet(new LinkedHashSet<>(pairs));
    Set<String> known = new LinkedHashSet<>();
    for (Pair pair : pairs) {
      known.add(pair.action);
    }
    this.actions = Collections.unmodifiableSet(known);
  }

  public static ProcessActionCatalog from(
      ProcessEngineRegistry engines, ProcessActionRegistry actions) {
    Objects.requireNonNull(engines, "engines");
    Objects.requireNonNull(actions, "actions");
    Set<Pair> deployed = new LinkedHashSet<>();
    for (ProcessActionRegistry.Entry entry : actions.entries()) {
      for (String format : entry.tableFormats()) {
        for (String engine : engines.engines().keySet()) {
          if (entry.plugin().supports(format, engine)) {
            deployed.add(new Pair(format, entry.action(), engine));
          }
        }
      }
    }
    return new ProcessActionCatalog(deployed);
  }

  /** Explicit routing fixtures retained only for isolated framework tests until Spring SPI wiring. */
  public static ProcessActionCatalog simulatedRoutingFixtures() {
    Set<Pair> fixtures = new LinkedHashSet<>();
    fixtures.add(new Pair("iceberg", "expire-snapshots", "local"));
    fixtures.add(new Pair("iceberg", "expire-snapshots", "remote-spark"));
    fixtures.add(new Pair("iceberg", "clean-orphans", "local"));
    fixtures.add(new Pair("iceberg", "clean-orphans", "remote-spark"));
    fixtures.add(new Pair("paimon", "sync-table-meta", "local"));
    return new ProcessActionCatalog(fixtures);
  }

  public static ProcessActionCatalog empty() {
    return new ProcessActionCatalog(Collections.emptySet());
  }

  public boolean isKnownAction(String action) {
    return actions.contains(action);
  }

  public boolean supports(String tableFormat, String action, String engine) {
    return pairs.contains(new Pair(tableFormat, action, engine));
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
