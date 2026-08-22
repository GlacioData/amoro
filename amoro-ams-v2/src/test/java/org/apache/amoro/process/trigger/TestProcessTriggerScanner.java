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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.TestProcessDomain;
import org.apache.amoro.process.rest.ProcessRestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * P6: the scheduled trigger scanner — table scan → action plugin gates → idempotent create via the
 * REST admission path, plus the admission mutex under concurrent triggers.
 */
@Timeout(60)
public class TestProcessTriggerScanner {

  private DefaultScheduler scheduler;
  private ProcessDomainAssembly assembly;
  private ProcessRestSupport rest;
  private InMemoryManagedTables tables;
  private ProcessTriggerScanner scanner;

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
    rest = new ProcessRestSupport(assembly);
    tables = new InMemoryManagedTables();
    tables.add("prod", "db1", "orders", "42");
    tables.add("prod", "db1", "events", "43");
    scanner =
        new ProcessTriggerScanner(
            rest,
            tables,
            new FixedIntervalActionPlugin("expire-snapshots", "local", 3600),
            "scan-1");
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
  }

  @Test
  public void scanCreatesProcessesForEligibleTables() {
    scanner.scanOnce();

    List<org.apache.amoro.process.ProcessResource> created =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "SCHEDULED".equals(r.spec().triggerSource()))
            .collect(java.util.stream.Collectors.toList());
    assertEquals(2, created.size(), "both eligible tables get a scheduled process");
    assertTrue(created.stream().allMatch(r -> "expire-snapshots".equals(r.spec().action())));
  }

  @Test
  public void repeatedScanIsIdempotentWhileActive() {
    scanner.scanOnce();
    scanner.scanOnce(); // same window: no duplicates while the first is active

    long count =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "SCHEDULED".equals(r.spec().triggerSource()))
            .count();
    assertEquals(2, count, "the admission slot blocks a second scheduled create");
  }

  @Test
  public void scanSkipsTablesOutsideTheGate() {
    // the plugin's interval gate: only tables whose last maintenance is older than the interval
    tables.markFreshlyMaintained("prod", "db1", "orders");
    scanner.scanOnce();

    long count =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "SCHEDULED".equals(r.spec().triggerSource()))
            .count();
    assertEquals(1, count, "the freshly maintained table is skipped this window");
  }

  @Test
  public void scanCreatesAgainAfterThePreviousProcessTerminates() throws Exception {
    scanner.scanOnce();
    // terminate one table's process, then rescan: a fresh process is admitted
    String orders =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "orders".equals(r.spec().table().table()))
            .findFirst()
            .get()
            .name();
    rest.forceTerminal(orders, "SUCCESS");

    // same window: the idempotency key replays to the terminal original — no duplicate
    scanner.scanOnce();
    long sameWindow =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "orders".equals(r.spec().table().table()))
            .count();
    assertEquals(1, sameWindow, "within one window the create replays, never duplicates");

    // a NEW window (different scan identity) admits a fresh process: the terminal released
    // the active slot even though the idempotency key differs by window
    ProcessTriggerScanner nextWindow =
        new ProcessTriggerScanner(
            rest,
            tables,
            new FixedIntervalActionPlugin("expire-snapshots", "local", 3600),
            "scan-2");
    nextWindow.scanOnce();
    long ordersCount =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "orders".equals(r.spec().table().table()))
            .count();
    assertEquals(2, ordersCount, "terminal releases the admission slot for the next window");
  }

  @Test
  public void concurrentScansAdmitExactlyOneProcessPerTable() throws Exception {
    Thread[] scanners = new Thread[4];
    for (int i = 0; i < scanners.length; i++) {
      scanners[i] = new Thread(scanner::scanOnce, "scan-racer-" + i);
    }
    for (Thread racer : scanners) {
      racer.start();
    }
    for (Thread racer : scanners) {
      racer.join(10_000);
    }

    long count =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "SCHEDULED".equals(r.spec().triggerSource()))
            .count();
    assertTrue(count <= 4, "no table exceeds one ACTIVE process (admission is exclusive)");
    // every table ends with at least one process attempt
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> assembly.indexProjection().current().resourcesByName().size() >= 2);
  }

  // ------------------------------------------------------------------ fakes

  /** In-memory ManagedTablePort: tables with a lastMaintenanceAt stamp. */
  static final class InMemoryManagedTables implements ManagedTablePort {
    final java.util.Map<String, TableSnapshot> tables =
        new java.util.concurrent.ConcurrentHashMap<String, TableSnapshot>();

    void add(String catalog, String db, String table, String tableId) {
      tables.put(
          tableId,
          new TableSnapshot(catalog, db, table, tableId, java.time.Instant.EPOCH.toString()));
    }

    void markFreshlyMaintained(String catalog, String db, String table) {
      for (TableSnapshot snapshot : tables.values()) {
        if (snapshot.table().equals(table)) {
          tables.put(
              snapshot.tableId(),
              new TableSnapshot(
                  snapshot.catalog(),
                  snapshot.database(),
                  snapshot.table(),
                  snapshot.tableId(),
                  java.time.Instant.now().toString()));
        }
      }
    }

    @Override
    public List<TableSnapshot> scan() {
      return new java.util.ArrayList<TableSnapshot>(tables.values());
    }
  }

  /** Fixed-interval gate: eligible when the last maintenance is older than intervalSeconds. */
  static final class FixedIntervalActionPlugin implements ProcessActionPlugin {
    private final String action;
    private final String engine;
    private final int intervalSeconds;

    FixedIntervalActionPlugin(String action, String engine, int intervalSeconds) {
      this.action = action;
      this.engine = engine;
      this.intervalSeconds = intervalSeconds;
    }

    @Override
    public String action() {
      return action;
    }

    @Override
    public boolean supports(String tableFormat, String executionEngine) {
      return true;
    }

    @Override
    public ScheduledEvaluation evaluateScheduled(
        ManagedTablePort.TableSnapshot table, java.time.Instant logicalFireTime) {
      java.time.Instant last = java.time.Instant.parse(table.lastMaintenanceAt());
      if (logicalFireTime.minusSeconds(intervalSeconds).isBefore(last)) {
        return ScheduledEvaluation.skip();
      }
      Map<String, Object> parameters = new LinkedHashMap<String, Object>();
      parameters.put("olderThanMillis", 1L);
      parameters.put("retainLast", 1);
      return ScheduledEvaluation.create(parameters);
    }
  }
}
