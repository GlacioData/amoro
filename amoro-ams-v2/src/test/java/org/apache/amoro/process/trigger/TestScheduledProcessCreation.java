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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.process.ProcessCreationService;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.ProcessFinality;
import org.apache.amoro.process.TestProcessDomain;
import org.apache.amoro.process.rest.ApiError;
import org.apache.amoro.process.rest.ProcessRestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Manual and scheduled callers race through the same singleton admission transaction. */
@Timeout(60)
public class TestScheduledProcessCreation {

  private DefaultScheduler scheduler;
  private ProcessDomainAssembly assembly;

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(1, 1000L);
    scheduler.start();
    assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            scheduler,
            128,
            10_000L,
            65536);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
  }

  @Test
  public void restAndScannerPersistExactlyOneActiveProcess() throws Exception {
    ProcessCreationService creationService = new ProcessCreationService(assembly);
    ProcessRestSupport rest =
        new ProcessRestSupport(
            assembly,
            new ProcessRestSupport.TableCatalogPort() {
              @Override
              public boolean exists(String catalog, String database, String table) {
                return true;
              }

              @Override
              public String tableId(String catalog, String database, String table) {
                return "42";
              }
            },
            creationService);
    ManagedTablePort tables =
        new SimulatedManagedTablePort(
            Collections.singletonList(
                new ManagedTablePort.TableSnapshot(
                    "prod", "db1", "orders", "42", "iceberg", Instant.EPOCH.toString())));
    ProcessActionPlugin dummy =
        new ProcessActionPlugin() {
          @Override
          public String action() {
            return "expire-snapshots";
          }

          @Override
          public boolean supports(String tableFormat, String executionEngine) {
            return "iceberg".equals(tableFormat) && "local".equals(executionEngine);
          }

          @Override
          public ScheduledEvaluation evaluateScheduled(
              ManagedTablePort.TableSnapshot table, Instant logicalFireTime) {
            return ScheduledEvaluation.create("local", Collections.emptyMap());
          }
        };
    ProcessTriggerScanner scanner =
        new ProcessTriggerScanner(
            creationService,
            tables,
            dummy,
            "scheduled-race",
            Clock.fixed(Instant.parse("2026-08-23T12:34:56Z"), ZoneOffset.UTC),
            10);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> manualFailure = new AtomicReference<>();
    Thread scheduled =
        new Thread(
            () -> {
              awaitStart(ready, start);
              scanner.scanOnce();
            },
            "scheduled-create");
    Thread manual =
        new Thread(
            () -> {
              awaitStart(ready, start);
              try {
                rest.create(
                    "prod",
                    "db1",
                    "orders",
                    "manual-race",
                    "expire-snapshots",
                    "local",
                    new LinkedHashMap<>());
              } catch (ApiError expectedRaceLoser) {
                if (!"ACTIVE_PROCESS_EXISTS".equals(expectedRaceLoser.code())) {
                  manualFailure.set(expectedRaceLoser);
                }
              } catch (Throwable unexpected) {
                manualFailure.set(unexpected);
              }
            },
            "manual-create");
    scheduled.start();
    manual.start();
    ready.await();
    start.countDown();
    scheduled.join(10_000L);
    manual.join(10_000L);

    assertNull(manualFailure.get());
    long active =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(resource -> !ProcessFinality.isFinal(resource))
            .count();
    assertEquals(1, active);
    assertEquals(1, assembly.indexProjection().current().resourcesByName().size());
  }

  private static void awaitStart(CountDownLatch ready, CountDownLatch start) {
    ready.countDown();
    try {
      start.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
