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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.process.engine.EngineTypes;
import org.apache.amoro.process.engine.ProcessEngineFactory;
import org.apache.amoro.process.engine.ProcessEnginePort;
import org.apache.amoro.process.engine.ProcessEngineRegistry;
import org.apache.amoro.process.engine.ProcessPluginLoader;
import org.apache.amoro.process.engine.ProviderMode;
import org.apache.amoro.process.rest.ProcessActionCatalog;
import org.apache.amoro.process.trigger.ManagedTablePort;
import org.apache.amoro.process.trigger.ProcessActionPlugin;
import org.apache.amoro.process.trigger.ProcessActionPluginFactory;
import org.apache.amoro.process.trigger.ProcessActionRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Java SPI loading and capability-derived action admission without any format implementation. */
public class TestProcessPluginSpi {

  @TempDir Path servicesRoot;

  @Test
  public void discoversExternalFactoriesWithoutCoreRegistration() throws Exception {
    writeService(ProcessEngineFactory.class, ExternalEngineFactory.class);
    writeService(ProcessActionPluginFactory.class, ExternalActionFactory.class);
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {servicesRoot.toUri().toURL()}, getClass().getClassLoader())) {
      assertEquals(
          ExternalEngineFactory.class,
          ProcessPluginLoader.loadEngineFactories(loader).get(0).getClass());
      assertEquals(
          ExternalActionFactory.class,
          ProcessPluginLoader.loadActionFactories(loader).get(0).getClass());
    }
  }

  @Test
  public void selectsOneModeAndDerivesOnlyDeployedPairs() {
    ProcessEngineRegistry engines =
        ProcessEngineRegistry.fromFactories(
            Arrays.asList(new ExternalEngineFactory(), new SimulatedSameNameEngineFactory()),
            ProviderMode.SIMULATED,
            new ProcessEngineFactory.Context("test-instance"),
            5_000L);
    ProcessActionRegistry actions =
        ProcessActionRegistry.fromFactories(
            Collections.singletonList(new ExternalActionFactory()),
            ProviderMode.SIMULATED,
            new ProcessActionPluginFactory.Context("test-instance"));
    ProcessActionCatalog catalog = ProcessActionCatalog.from(engines, actions);

    assertTrue(engines.dispatcherFor("dummy-local").isPresent());
    assertTrue(catalog.isKnownAction("dummy-maintenance"));
    assertTrue(catalog.supports("test-format", "dummy-maintenance", "dummy-local"));
    assertFalse(catalog.supports("test-format", "dummy-maintenance", "missing-engine"));
    assertFalse(catalog.supports("other-format", "dummy-maintenance", "dummy-local"));
  }

  @Test
  public void duplicateFactoryIdentityFailsFast() {
    IllegalArgumentException duplicateEngine =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ProcessEngineRegistry.fromFactories(
                    Arrays.asList(new ExternalEngineFactory(), new ExternalEngineFactory()),
                    ProviderMode.SIMULATED,
                    new ProcessEngineFactory.Context("test-instance"),
                    5_000L));
    assertTrue(duplicateEngine.getMessage().contains("dummy-local"));

    IllegalArgumentException duplicateAction =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ProcessActionRegistry.fromFactories(
                    Arrays.asList(new ExternalActionFactory(), new ExternalActionFactory()),
                    ProviderMode.SIMULATED,
                    new ProcessActionPluginFactory.Context("test-instance")));
    assertTrue(duplicateAction.getMessage().contains("dummy-maintenance"));
  }

  private void writeService(Class<?> service, Class<?> implementation) throws Exception {
    Path serviceFile = servicesRoot.resolve("META-INF/services/" + service.getName());
    Files.createDirectories(serviceFile.getParent());
    Files.writeString(serviceFile, implementation.getName() + System.lineSeparator());
  }

  public static final class ExternalEngineFactory implements ProcessEngineFactory {
    @Override
    public String engineName() {
      return "dummy-local";
    }

    @Override
    public ProviderMode mode() {
      return ProviderMode.SIMULATED;
    }

    @Override
    public ProcessEnginePort create(Context context) {
      return new NoopEngine();
    }
  }

  public static final class SimulatedSameNameEngineFactory implements ProcessEngineFactory {
    @Override
    public String engineName() {
      return "dummy-local";
    }

    @Override
    public ProviderMode mode() {
      return ProviderMode.REAL;
    }

    @Override
    public ProcessEnginePort create(Context context) {
      return new NoopEngine();
    }
  }

  public static final class ExternalActionFactory implements ProcessActionPluginFactory {
    @Override
    public String action() {
      return "dummy-maintenance";
    }

    @Override
    public ProviderMode mode() {
      return ProviderMode.SIMULATED;
    }

    @Override
    public java.util.Set<String> tableFormats() {
      return Collections.singleton("test-format");
    }

    @Override
    public ProcessActionPlugin create(Context context) {
      return new ProcessActionPlugin() {
        @Override
        public String action() {
          return "dummy-maintenance";
        }

        @Override
        public boolean supports(String tableFormat, String executionEngine) {
          return "test-format".equals(tableFormat) && "dummy-local".equals(executionEngine);
        }

        @Override
        public ScheduledEvaluation evaluateScheduled(
            ManagedTablePort.TableSnapshot table, Instant logicalFireTime) {
          return ScheduledEvaluation.create("dummy-local", Collections.emptyMap());
        }
      };
    }
  }

  private static final class NoopEngine implements ProcessEnginePort {
    @Override
    public EngineTypes.EngineCapabilities capabilities() {
      return new EngineTypes.EngineCapabilities(true, true, "noop-v1");
    }

    @Override
    public java.util.concurrent.CompletionStage<EngineTypes.SubmissionOutcome> submit(
        String submissionKey, String requestHash, byte[] submissionPayload) {
      return CompletableFuture.completedFuture(EngineTypes.SubmissionOutcome.rejected("noop"));
    }

    @Override
    public java.util.concurrent.CompletionStage<EngineTypes.SubmissionResolution> resolveSubmission(
        String submissionKey, String requestHash) {
      return CompletableFuture.completedFuture(EngineTypes.SubmissionResolution.notFound());
    }

    @Override
    public java.util.concurrent.CompletionStage<EngineTypes.ProcessObservation> observe(
        String externalId) {
      return CompletableFuture.completedFuture(EngineTypes.ProcessObservation.notFound());
    }

    @Override
    public java.util.concurrent.CompletionStage<EngineTypes.CancellationOutcome> cancel(
        String externalId) {
      return CompletableFuture.completedFuture(EngineTypes.CancellationOutcome.notFound());
    }

    @Override
    public java.util.concurrent.CompletionStage<Void> release(String externalId) {
      return CompletableFuture.completedFuture(null);
    }
  }
}
