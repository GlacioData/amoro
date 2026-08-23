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

package org.apache.amoro.process.e2e;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.persistence.blob.MyBatisBlobStore;
import org.apache.amoro.persistence.blob.ResourceBlobMapper;
import org.apache.amoro.process.ProcessCreationService;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.ProcessFinality;
import org.apache.amoro.process.ProcessReconciler;
import org.apache.amoro.process.ProcessResource;
import org.apache.amoro.process.ProcessTtlCleaner;
import org.apache.amoro.process.engine.ExecutionHandleReaper;
import org.apache.amoro.process.engine.LocalEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.apache.amoro.process.engine.ProcessEngineRegistry;
import org.apache.amoro.process.engine.ProviderMode;
import org.apache.amoro.process.rest.ApiError;
import org.apache.amoro.process.rest.ProcessActionCatalog;
import org.apache.amoro.process.rest.ProcessRestSupport;
import org.apache.amoro.process.trigger.ManagedTablePort;
import org.apache.amoro.process.trigger.ProcessActionPlugin;
import org.apache.amoro.process.trigger.ProcessActionPluginFactory;
import org.apache.amoro.process.trigger.ProcessActionRegistry;
import org.apache.amoro.test.IsolatedMysql;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * MySQL release gate over one Testcontainers-owned database: simulated REST create → dummy local
 * execution → durable terminal result → full context rebuild/postStart → release-gated TTL. No
 * format action, real table access, Spark submission, fixed host database or destructive cleanup is
 * involved.
 */
@Tag("docker-mysql")
@ExtendWith(IsolatedMysql.class)
@Timeout(180)
public class TestProcessE2EMysql {

  private static final String MYSQL_DATABASE = "amoro_process_e2e";
  private static final String ACTION = "dummy-maintenance";
  private static final String FORMAT = "simulated";
  private static final String ENGINE = "dummy-local";
  private static final String CATALOG = "simulated";
  private static final String DATABASE = "dummy_db";
  private static final String TABLE = "dummy_table";
  private static final String TABLE_ID = "dummy-table-42";

  private static SqlSessionFactory sqlFactory;

  @BeforeAll
  public static void initializeIsolatedSchema() {
    IsolatedMysql.initializeControlPlane(MYSQL_DATABASE);
    sqlFactory = IsolatedMysql.sqlSessionFactory(MYSQL_DATABASE, "process-testcontainer");
  }

  /** One complete control-plane context over the container-owned durable store. */
  private static final class Stack implements AutoCloseable {
    private final DefaultScheduler scheduler = DefaultScheduler.create(2, 50L);
    private final LocalEngineAdapter engine =
        new LocalEngineAdapter(2, 64, LocalEngineAdapter.simulatedAction());
    private final ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(engine, 5_000L);
    private final ProcessEngineRegistry engines = ProcessEngineRegistry.single(ENGINE, dispatcher);
    private final SqlSession blobSession = sqlFactory.openSession(true);
    private final ProcessDomainAssembly assembly;
    private final ProcessRestSupport rest;
    private final ProcessTtlCleaner cleaner;
    private final ExecutionHandleReaper reaper;

    private Stack() {
      assembly =
          new ProcessDomainAssembly(
              new MyBatisBlobStore(
                  ProcessDomainAssembly.DOMAIN, blobSession.getMapper(ResourceBlobMapper.class)),
              event -> HandoffResult.ACCEPTED,
              scheduler,
              128,
              10_000L,
              65_536);
      ProcessActionRegistry actions =
          ProcessActionRegistry.fromFactories(
              List.of(new DummyActionFactory()),
              ProviderMode.SIMULATED,
              new ProcessActionPluginFactory.Context("mysql-e2e"));
      rest =
          new ProcessRestSupport(
              assembly,
              new DummyTableCatalog(),
              new ProcessCreationService(assembly),
              ProcessActionCatalog.from(engines, actions));
      cleaner = new ProcessTtlCleaner(assembly, assembly.handleRegistry());
      reaper = new ExecutionHandleReaper(assembly.releaseIndex(), engines, 8, 1_000L);

      // Rebuild all projections before any worker is allowed to run.
      assembly.persistence().postStart();
      scheduler.start();
    }

    private ProcessResource create(String idempotencyKey) {
      return rest.create(
              CATALOG,
              DATABASE,
              TABLE,
              idempotencyKey,
              ACTION,
              ENGINE,
              Collections.singletonMap("simulated", true),
              "MANUAL")
          .resource;
    }

    private void schedule(String processName) {
      scheduler.schedule(
          new ProcessReconciler(
              processName,
              assembly.repository(),
              engines,
              scheduler,
              ProcessReconciler.Clock.systemUtc(),
              50L,
              assembly.handleRegistry()));
    }

    private int durableProcessCount() {
      return blobSession
          .getMapper(ResourceBlobMapper.class)
          .selectAll(ProcessDomainAssembly.DOMAIN.table(), ProcessResource.COLLECTION)
          .size();
    }

    @Override
    public void close() {
      reaper.close();
      scheduler.shutdown(Duration.ofSeconds(5));
      assembly.persistence().shutdown(Duration.ofSeconds(5));
      dispatcher.close();
      engine.shutdown(5_000L);
      blobSession.close();
    }
  }

  @Test
  public void dummyLifecycleSurvivesRestartAndTtlWaitsForRelease() {
    String processName;
    String finishedAt;
    try (Stack stack = new Stack()) {
      ProcessResource created = stack.create("mysql-dummy-e2e-1");
      processName = created.name();
      assertEquals(ACTION, created.spec().action());
      assertEquals(ENGINE, created.spec().executionEngine());
      assertEquals("PENDING", created.status().phase());

      // Same request replays, while another key cannot create a second active durable row.
      assertEquals(processName, stack.create("mysql-dummy-e2e-1").name());
      ApiError conflict =
          assertThrows(ApiError.class, () -> stack.create("mysql-dummy-e2e-conflict"));
      assertEquals("ACTIVE_PROCESS_EXISTS", conflict.code());
      assertEquals(1, stack.durableProcessCount());

      stack.schedule(processName);
      await()
          .atMost(30, TimeUnit.SECONDS)
          .until(
              () ->
                  ProcessFinality.isFixedTerminal(
                      stack.assembly.repository().get(processName).status().phase()));
      ProcessResource terminal = stack.assembly.repository().get(processName);
      assertEquals("SUCCESS", terminal.status().phase());
      assertEquals(Boolean.TRUE, terminal.status().summary().result().get("simulated"));
      assertTrue(terminal.status().finishedAt() != null);
      finishedAt = terminal.status().finishedAt();
      await().atMost(10, TimeUnit.SECONDS).until(() -> stack.scheduler.registrySize() == 0);
      assertTrue(stack.assembly.releaseIndex().hasPendingForProcess(processName));
    }

    // A wholly new context rebuilds canonical/read/release indexes only from the durable row.
    try (Stack rebuilt = new Stack()) {
      ProcessResource reloaded = rebuilt.assembly.repository().get(processName);
      assertEquals("SUCCESS", reloaded.status().phase());
      assertEquals(finishedAt, reloaded.status().finishedAt());
      assertTrue(rebuilt.assembly.indexProjection().current().find(processName).isPresent());
      assertTrue(rebuilt.assembly.releaseIndex().hasPendingForProcess(processName));
      assertEquals(1, rebuilt.durableProcessCount());

      Instant ttlNow = Instant.parse(finishedAt).plus(Duration.ofDays(40));
      assertEquals(0, rebuilt.cleaner.cleanOnce(ttlNow, 30, 10));
      assertTrue(rebuilt.assembly.indexProjection().current().find(processName).isPresent());

      assertEquals(1, rebuilt.reaper.runOnce(ttlNow));
      await()
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> !rebuilt.assembly.releaseIndex().hasPendingForProcess(processName));
      assertEquals(0, rebuilt.engine.submissionCount());

      assertEquals(1, rebuilt.cleaner.cleanOnce(ttlNow, 30, 10));
      assertFalse(rebuilt.assembly.indexProjection().current().find(processName).isPresent());
      assertEquals(0, rebuilt.durableProcessCount());

      ProcessResource fresh = rebuilt.create("mysql-dummy-e2e-2");
      assertEquals("PENDING", fresh.status().phase());
      assertEquals(ACTION, fresh.spec().action());
    }
  }

  private static final class DummyTableCatalog implements ProcessRestSupport.TableCatalogPort {
    @Override
    public ProcessRestSupport.TableIdentity resolve(String catalog, String database, String table) {
      return CATALOG.equals(catalog) && DATABASE.equals(database) && TABLE.equals(table)
          ? new ProcessRestSupport.TableIdentity(TABLE_ID, FORMAT)
          : null;
    }
  }

  private static final class DummyActionFactory implements ProcessActionPluginFactory {
    @Override
    public String action() {
      return ACTION;
    }

    @Override
    public ProviderMode mode() {
      return ProviderMode.SIMULATED;
    }

    @Override
    public Set<String> tableFormats() {
      return Set.of(FORMAT);
    }

    @Override
    public ProcessActionPlugin create(Context context) {
      return new ProcessActionPlugin() {
        @Override
        public String action() {
          return ACTION;
        }

        @Override
        public boolean supports(String tableFormat, String executionEngine) {
          return FORMAT.equals(tableFormat) && ENGINE.equals(executionEngine);
        }

        @Override
        public ScheduledEvaluation evaluateScheduled(
            ManagedTablePort.TableSnapshot table, Instant logicalFireTime) {
          return ScheduledEvaluation.create(
              ENGINE, Map.of("simulated", true, "logicalFireTime", logicalFireTime.toString()));
        }
      };
    }
  }
}
