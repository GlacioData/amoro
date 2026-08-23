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

package org.apache.amoro.process.engine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Engine selection by {@code spec.executionEngine} (process spec §6.1): each engine name maps to
 * its own {@link ProcessEnginePort} behind its own {@link ProcessEngineDispatcher} (single-flight
 * identities are scoped per adapter, so two engines never share one flight map). Adding an engine —
 * local, remote Spark, a future Flink submitter — means implementing the port and registering it
 * here; the reconciler, REST validation and the dispatcher semantics stay untouched.
 */
public final class ProcessEngineRegistry {

  private final Map<String, ProcessEngineDispatcher> dispatchersByEngine;

  private ProcessEngineRegistry(Map<String, ProcessEngineDispatcher> dispatchersByEngine) {
    this.dispatchersByEngine = Collections.unmodifiableMap(dispatchersByEngine);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** One-engine convenience used by tests that wire a single dispatcher. */
  public static ProcessEngineRegistry single(
      String executionEngine, ProcessEngineDispatcher dispatcher) {
    return builder().register(executionEngine, dispatcher).build();
  }

  /** The dispatcher serving the given engine name; empty when the engine is not deployed. */
  public Optional<ProcessEngineDispatcher> dispatcherFor(String executionEngine) {
    return Optional.ofNullable(dispatchersByEngine.get(executionEngine));
  }

  /** The engine names available in this deployment (REST create validation consults this). */
  public Map<String, ProcessEngineDispatcher> engines() {
    return dispatchersByEngine;
  }

  /** Registry builder; duplicate engine names fail fast. */
  public static final class Builder {
    private final Map<String, ProcessEngineDispatcher> engines =
        new LinkedHashMap<String, ProcessEngineDispatcher>();

    public Builder register(String executionEngine, ProcessEngineDispatcher dispatcher) {
      Objects.requireNonNull(executionEngine, "executionEngine");
      Objects.requireNonNull(dispatcher, "dispatcher");
      if (engines.putIfAbsent(executionEngine, dispatcher) != null) {
        throw new IllegalArgumentException(
            "engine '" + executionEngine + "' is already registered");
      }
      return this;
    }

    public Builder registerPort(
        String executionEngine, ProcessEnginePort adapter, long commandTimeoutMillis) {
      return register(executionEngine, new ProcessEngineDispatcher(adapter, commandTimeoutMillis));
    }

    public ProcessEngineRegistry build() {
      return new ProcessEngineRegistry(engines);
    }
  }
}
