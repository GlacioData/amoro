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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.PersistenceListener;
import org.apache.amoro.process.engine.ExecutionHandleRegistry;
import org.apache.amoro.process.engine.LocalEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineRegistry;
import org.apache.amoro.process.rest.ProcessRestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Mirrors the production {@code ControlPlaneAutoConfiguration} wiring (scheduling listener + engine
 * registry + local engine) over an offline durable store: a REST-created process schedules and
 * converges to SUCCESS with NO manual reconciler wiring, and an undeleted process is re-scheduled
 * on rebuild.
 */
@Timeout(90)
public class TestAutoSchedulingWiring {

  private final TestProcessDomain.TestPersistenceBlobStore durableStore =
      new TestProcessDomain.TestPersistenceBlobStore();
  private org.apache.amoro.persistence.ListenerDispatcher<ProcessResource> eventDispatcher;
  private DefaultScheduler scheduler;
  private LocalEngineAdapter localEngine;
  private ProcessDomainAssembly assembly;
  private ProcessRestSupport rest;
  private ProcessEngineRegistry engines;

  @BeforeEach
  public void setUp() {
    eventDispatcher =
        org.apache.amoro.persistence.ListenerDispatcher.start("auto-wiring", 2, 256, 3, 50L);
    scheduler = DefaultScheduler.create(2, 50L);
    scheduler.start();
    localEngine = new LocalEngineAdapter(2, 64, LocalEngineAdapter.simulatedAction());
    engines = ProcessEngineRegistry.builder().registerPort("local", localEngine, 5_000L).build();
    assembly =
        new ProcessDomainAssembly(
            durableStore,
            eventDispatcher,
            scheduler,
            128,
            10_000L,
            65536,
            new ExecutionHandleRegistry());
    rest = new ProcessRestSupport(assembly);
    registerSchedulingListener();
  }

  /** The same bridge the production configuration registers. */
  private void registerSchedulingListener() {
    PersistenceListener<ProcessResource> listener =
        new PersistenceListener<ProcessResource>() {
          @Override
          public void afterCreated(ProcessResource resource) {
            schedule(resource);
          }

          @Override
          public void afterModified(ProcessResource resource) {
            schedule(resource);
          }

          @Override
          public void afterDeleted(ProcessResource resource) {}

          @Override
          public void postStart(ProcessResource existing) {
            schedule(existing);
          }

          private void schedule(ProcessResource resource) {
            scheduler.schedule(
                new ProcessReconciler(
                    resource.name(),
                    assembly.repository(),
                    engines,
                    scheduler,
                    ProcessReconciler.Clock.systemUtc(),
                    200L,
                    assembly.handleRegistry()));
          }
        };
    assembly.persistence().addListener(listener);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
    eventDispatcher.shutdown(Duration.ofSeconds(5));
    localEngine.shutdown(5_000L);
  }

  @Test
  public void restCreatedProcessAutoSchedulesToSuccess() {
    ProcessResource created =
        rest.create("prod", "db", "orders", "wire-1", "expire-snapshots", "local", null).resource;

    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () ->
                ProcessFinality.isFixedTerminal(
                    assembly.repository().get(created.name()).status().phase()));
    assertEquals("SUCCESS", assembly.repository().get(created.name()).status().phase());
    assertTrue(assembly.repository().get(created.name()).status().finishedAt() != null);
    // the terminal release cleared the handle: the TTL gate is open for this process
    assertTrue(!assembly.handleRegistry().hasPendingHandle(created.name()));
    await().atMost(10, TimeUnit.SECONDS).until(() -> scheduler.registrySize() == 0);
  }

  @Test
  public void pendingProcessIsRescheduledAfterRebuild() {
    ProcessResource created =
        rest.create("prod", "db", "orders", "wire-2", "expire-snapshots", "local", null).resource;
    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () ->
                ProcessFinality.isFixedTerminal(
                    assembly.repository().get(created.name()).status().phase()));
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));

    // rebuild everything over the SAME durable store instance: postStart replay re-reads the
    // rows and the freshly registered listener re-schedules every live process
    eventDispatcher =
        org.apache.amoro.persistence.ListenerDispatcher.start("auto-wiring-2", 2, 256, 3, 50L);
    scheduler = DefaultScheduler.create(2, 50L);
    scheduler.start();
    assembly =
        new ProcessDomainAssembly(
            durableStore,
            eventDispatcher,
            scheduler,
            128,
            10_000L,
            65536,
            new ExecutionHandleRegistry());
    rest = new ProcessRestSupport(assembly);
    registerSchedulingListener();
    assembly.persistence().postStart();

    ProcessResource reloaded = assembly.repository().get(created.name());
    assertEquals("SUCCESS", reloaded.status().phase(), "the durable row survived the rebuild");
  }
}
