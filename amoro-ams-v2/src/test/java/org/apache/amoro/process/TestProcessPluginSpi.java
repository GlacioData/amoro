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
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/** Java SPI loading and capability-derived action admission without any format implementation. */
public class TestProcessPluginSpi {

  @TempDir Path servicesRoot;

  @Test
  public void discoversExternalFactoriesWithoutCoreRegistration() throws Exception {
    writeService(ProcessEngineFactory.class, ExternalEngineFactory.class);
    writeService(ProcessActionPluginFactory.class, ExternalActionFactory.class);
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {servicesRoot.toUri().toURL()}, getClass().getClassLoader())) {
      assertTrue(
          ProcessPluginLoader.loadEngineFactories(loader).stream()
              .anyMatch(factory -> factory.getClass() == ExternalEngineFactory.class));
      assertTrue(
          ProcessPluginLoader.loadActionFactories(loader).stream()
              .anyMatch(factory -> factory.getClass() == ExternalActionFactory.class));
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
  public void submissionBuilderUsesDurableTableFormatForExactPluginSelection() {
    ProcessEngineRegistry engines =
        ProcessEngineRegistry.fromFactories(
            Collections.singletonList(new ExternalEngineFactory()),
            ProviderMode.SIMULATED,
            new ProcessEngineFactory.Context("test-instance"),
            5_000L);
    ProcessActionRegistry actions =
        ProcessActionRegistry.fromFactories(
            Arrays.asList(new ExternalActionFactory(), new OtherFormatActionFactory()),
            ProviderMode.SIMULATED,
            new ProcessActionPluginFactory.Context("test-instance"));
    ProcessActionCatalog catalog = ProcessActionCatalog.from(engines, actions);

    assertEquals(
        "test-format",
        new String(
            catalog.buildSubmission(spec("test-format"), Collections.emptyMap()),
            java.nio.charset.StandardCharsets.UTF_8));
    assertEquals(
        "other-format",
        new String(
            catalog.buildSubmission(spec("other-format"), Collections.emptyMap()),
            java.nio.charset.StandardCharsets.UTF_8));
    engines.close();
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

  @Test
  public void factoryStartupFailureClosesAlreadyCreatedEngines() {
    AtomicInteger closes = new AtomicInteger();
    ProcessEngineFactory created =
        new ProcessEngineFactory() {
          @Override
          public String engineName() {
            return "created-first";
          }

          @Override
          public ProviderMode mode() {
            return ProviderMode.SIMULATED;
          }

          @Override
          public ProcessEnginePort create(Context context) {
            return new ClosingEngine(closes);
          }
        };
    ProcessEngineFactory failing =
        new ProcessEngineFactory() {
          @Override
          public String engineName() {
            return "fails-second";
          }

          @Override
          public ProviderMode mode() {
            return ProviderMode.SIMULATED;
          }

          @Override
          public ProcessEnginePort create(Context context) {
            throw new IllegalStateException("dummy startup failure");
          }
        };

    assertThrows(
        IllegalStateException.class,
        () ->
            ProcessEngineRegistry.fromFactories(
                Arrays.asList(created, failing),
                ProviderMode.SIMULATED,
                new ProcessEngineFactory.Context("test-instance"),
                100L));
    assertEquals(1, closes.get());
  }

  @Test
  public void freezesValidatedEngineNameBeforeProviderConstruction() {
    AtomicInteger reads = new AtomicInteger();
    ProcessEngineFactory unstable =
        new ProcessEngineFactory() {
          @Override
          public String engineName() {
            return reads.getAndIncrement() == 0 ? "validated-name" : "changed-name";
          }

          @Override
          public ProviderMode mode() {
            return ProviderMode.SIMULATED;
          }

          @Override
          public ProcessEnginePort create(Context context) {
            return new NoopEngine();
          }
        };

    ProcessEngineRegistry engines =
        ProcessEngineRegistry.fromFactories(
            Collections.singletonList(unstable),
            ProviderMode.SIMULATED,
            new ProcessEngineFactory.Context("test-instance"),
            100L);

    assertTrue(engines.dispatcherFor("validated-name").isPresent());
    assertFalse(engines.dispatcherFor("changed-name").isPresent());
    assertEquals(1, reads.get());
    engines.close();
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

        @Override
        public byte[] buildSubmission(
            ProcessResource.ProcessSpec frozenSpec,
            java.util.Map<String, Object> simulationProfile) {
          return "test-format".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
      };
    }
  }

  public static final class OtherFormatActionFactory implements ProcessActionPluginFactory {
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
      return Collections.singleton("other-format");
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
          return "other-format".equals(tableFormat) && "dummy-local".equals(executionEngine);
        }

        @Override
        public ScheduledEvaluation evaluateScheduled(
            ManagedTablePort.TableSnapshot table, Instant logicalFireTime) {
          return ScheduledEvaluation.skip();
        }

        @Override
        public byte[] buildSubmission(
            ProcessResource.ProcessSpec frozenSpec,
            java.util.Map<String, Object> simulationProfile) {
          return "other-format".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
      };
    }
  }

  private static ProcessResource.ProcessSpec spec(String tableFormat) {
    return new ProcessResource.ProcessSpec(
        new ProcessResource.TableRef("catalog", "database", "table", "42", tableFormat),
        "dummy-maintenance",
        "dummy-local",
        "MANUAL",
        "2026-08-22T10:00:00Z",
        "RUN",
        new ProcessResource.RequestIdentity("sha256:key", "sha256:request"),
        Collections.emptyMap(),
        new ProcessResource.RetryPolicy(0, 0, 1));
  }

  private static class NoopEngine implements ProcessEnginePort {
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

  private static final class ClosingEngine extends NoopEngine implements AutoCloseable {
    private final AtomicInteger closes;

    private ClosingEngine(AtomicInteger closes) {
      this.closes = closes;
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }
}
