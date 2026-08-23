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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Engine selection by {@code spec.executionEngine} (process spec §6.1): each engine name maps to
 * its own {@link ProcessEnginePort} behind its own {@link ProcessEngineDispatcher} (single-flight
 * identities are scoped per adapter, so two engines never share one flight map). Adding an engine —
 * local, a remote submitter or another future engine — means implementing the port and registering
 * it here; the reconciler, REST validation and the dispatcher semantics stay untouched.
 */
public final class ProcessEngineRegistry implements AutoCloseable {

  private static final Pattern ENGINE_NAME = Pattern.compile("[a-z][a-z0-9-]{0,63}");

  private final Map<String, ProcessEngineDispatcher> dispatchersByEngine;
  private final AtomicBoolean closed = new AtomicBoolean();

  private ProcessEngineRegistry(Map<String, ProcessEngineDispatcher> dispatchersByEngine) {
    this.dispatchersByEngine = Collections.unmodifiableMap(dispatchersByEngine);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Selects one deployment mode after validating every factory's (engineName, mode) identity. */
  public static ProcessEngineRegistry fromFactories(
      Collection<? extends ProcessEngineFactory> factories,
      ProviderMode activeMode,
      ProcessEngineFactory.Context context,
      long commandTimeoutMillis) {
    Objects.requireNonNull(factories, "factories");
    Objects.requireNonNull(activeMode, "activeMode");
    Objects.requireNonNull(context, "context");
    Map<String, String> identities = new LinkedHashMap<>();
    List<SelectedFactory> selected = new ArrayList<>();
    for (ProcessEngineFactory factory : factories) {
      Objects.requireNonNull(factory, "engine factory");
      String engineName = requireEngineName(factory.engineName());
      ProviderMode mode = Objects.requireNonNull(factory.mode(), "engine factory mode");
      String identity = engineName + "|" + mode;
      String previous = identities.putIfAbsent(identity, factory.getClass().getName());
      if (previous != null) {
        throw new IllegalArgumentException(
            "duplicate engine factory identity "
                + identity
                + ": "
                + previous
                + " and "
                + factory.getClass().getName());
      }
      if (mode == activeMode) {
        selected.add(new SelectedFactory(engineName, factory));
      }
    }
    Builder builder = builder();
    try {
      for (SelectedFactory selection : selected) {
        builder.registerPort(
            selection.engineName,
            Objects.requireNonNull(selection.factory.create(context), "engine factory result"),
            commandTimeoutMillis);
      }
      return builder.build();
    } catch (RuntimeException | Error startupFailure) {
      builder.closeRegistered(startupFailure);
      throw startupFailure;
    }
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

  /** Closes every selected dispatcher and its owned adapter. */
  @Override
  public void close() {
    shutdown(5_000L);
  }

  /** Closes selected dispatchers in reverse order within one lifecycle budget. */
  public void shutdown(long timeoutMillis) {
    if (timeoutMillis <= 0) {
      throw new IllegalArgumentException("timeoutMillis must be > 0");
    }
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    long deadline =
        System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    RuntimeException failure = null;
    List<ProcessEngineDispatcher> dispatchers = new ArrayList<>(dispatchersByEngine.values());
    Collections.reverse(dispatchers);
    for (ProcessEngineDispatcher dispatcher : dispatchers) {
      try {
        dispatcher.shutdown(remainingMillis(deadline));
      } catch (RuntimeException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /** Registry builder; duplicate engine names fail fast. */
  public static final class Builder {
    private final Map<String, ProcessEngineDispatcher> engines =
        new LinkedHashMap<String, ProcessEngineDispatcher>();

    public Builder register(String executionEngine, ProcessEngineDispatcher dispatcher) {
      requireEngineName(executionEngine);
      Objects.requireNonNull(dispatcher, "dispatcher");
      if (engines.putIfAbsent(executionEngine, dispatcher) != null) {
        throw new IllegalArgumentException(
            "engine '" + executionEngine + "' is already registered");
      }
      return this;
    }

    public Builder registerPort(
        String executionEngine, ProcessEnginePort adapter, long commandTimeoutMillis) {
      ProcessEngineDispatcher dispatcher =
          new ProcessEngineDispatcher(adapter, commandTimeoutMillis);
      try {
        return register(executionEngine, dispatcher);
      } catch (RuntimeException | Error registrationFailure) {
        try {
          dispatcher.close();
        } catch (RuntimeException closeFailure) {
          registrationFailure.addSuppressed(closeFailure);
        }
        throw registrationFailure;
      }
    }

    public ProcessEngineRegistry build() {
      return new ProcessEngineRegistry(engines);
    }

    private void closeRegistered(Throwable startupFailure) {
      List<ProcessEngineDispatcher> dispatchers = new ArrayList<>(engines.values());
      Collections.reverse(dispatchers);
      for (ProcessEngineDispatcher dispatcher : dispatchers) {
        try {
          dispatcher.close();
        } catch (RuntimeException closeFailure) {
          startupFailure.addSuppressed(closeFailure);
        }
      }
      engines.clear();
    }
  }

  private static long remainingMillis(long deadlineNanos) {
    return Math.max(
        1L, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
  }

  private static String requireEngineName(String executionEngine) {
    if (executionEngine == null || !ENGINE_NAME.matcher(executionEngine).matches()) {
      throw new IllegalArgumentException(
          "executionEngine is not a canonical wire name: " + executionEngine);
    }
    return executionEngine;
  }

  /** Freezes the validated factory identity before invoking provider-controlled construction. */
  private static final class SelectedFactory {
    private final String engineName;
    private final ProcessEngineFactory factory;

    private SelectedFactory(String engineName, ProcessEngineFactory factory) {
      this.engineName = engineName;
      this.factory = factory;
    }
  }
}
