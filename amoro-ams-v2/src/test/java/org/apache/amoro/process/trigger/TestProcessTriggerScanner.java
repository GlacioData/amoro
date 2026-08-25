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
import org.apache.amoro.process.ProcessCreationService;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.ProcessTestFixtures;
import org.apache.amoro.process.TestProcessDomain;
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
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
  private ProcessCreationService creationService;
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
    creationService = new ProcessCreationService(assembly);
    tables = new InMemoryManagedTables();
    tables.add("prod", "db1", "orders", "42");
    tables.add("prod", "db1", "events", "43");
    scanner =
        new ProcessTriggerScanner(
            creationService,
            tables,
            new FixedIntervalActionPlugin(
                "dummy-maintenance", "local", 3600, tables.observedFireTimes),
            "scan-1",
            Clock.fixed(Instant.parse("2026-08-23T12:34:56Z"), ZoneOffset.UTC),
            1);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
  }

  @Test
  public void scanCreatesProcessesForEligibleTables() {
    scanner.scanOnce();

    List<ProcessResource> created =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "SCHEDULED".equals(r.spec().triggerSource()))
            .collect(java.util.stream.Collectors.toList());
    assertEquals(2, created.size(), "both eligible tables get a scheduled process");
    assertTrue(created.stream().allMatch(r -> "dummy-maintenance".equals(r.spec().action())));
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
  public void oneBrokenSimulatedFactDoesNotAbortTheRound() {
    tables.tables.put(
        "42",
        new ManagedTablePort.TableSnapshot(
            "prod", "db1", "orders", "42", "simulated", "not-an-instant"));

    scanner.scanOnce();

    assertEquals(1, assembly.indexProjection().current().resourcesByName().size());
    assertEquals(
        "events",
        assembly
            .indexProjection()
            .current()
            .resourcesByName()
            .values()
            .iterator()
            .next()
            .spec()
            .table()
            .table());
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
    ProcessTestFixtures.forceTerminal(assembly, orders, "SUCCESS");

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
            creationService,
            tables,
            new FixedIntervalActionPlugin("dummy-maintenance", "local", 3600),
            "scan-2",
            Clock.fixed(Instant.parse("2026-08-23T12:35:56Z"), ZoneOffset.UTC),
            1);
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

    // exactly one ACTIVE process per table: the admission mutex spans check AND durable
    // create for both scanner and REST entries (spec §5.2, review Critical-1)
    for (String tableName : new String[] {"orders", "events"}) {
      long activeForTable =
          assembly.indexProjection().current().resourcesByName().values().stream()
              .filter(r -> tableName.equals(r.spec().table().table()))
              .filter(r -> !org.apache.amoro.process.ProcessFinality.isFinal(r))
              .count();
      assertEquals(1, activeForTable, "table " + tableName + " has exactly one active process");
    }
    long total =
        assembly.indexProjection().current().resourcesByName().values().stream()
            .filter(r -> "SCHEDULED".equals(r.spec().triggerSource()))
            .count();
    assertEquals(2, total, "two tables, one scheduled process each — no duplicates");
    // every table ends with at least one process attempt
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> assembly.indexProjection().current().resourcesByName().size() >= 2);
  }

  @Test
  public void independentScannerInstancesShareCreationAdmission() throws Exception {
    ProcessTriggerScanner second =
        new ProcessTriggerScanner(
            creationService,
            tables,
            new FixedIntervalActionPlugin("dummy-maintenance", "local", 3600),
            "scan-2",
            Clock.fixed(Instant.parse("2026-08-23T12:35:56Z"), ZoneOffset.UTC),
            1);
    Thread first = new Thread(scanner::scanOnce, "first-scanner");
    Thread other = new Thread(second::scanOnce, "second-scanner");
    first.start();
    other.start();
    first.join(10_000L);
    other.join(10_000L);

    assertEquals(2, assembly.indexProjection().current().resourcesByName().size());
  }

  @Test
  public void unsupportedEnginePairIsSkippedWithoutGuessingRemote() {
    ProcessTriggerScanner unsupported =
        new ProcessTriggerScanner(
            creationService,
            tables,
            new FixedIntervalActionPlugin("dummy-maintenance", "not-deployed", 3600) {
              @Override
              public boolean supports(String tableFormat, String executionEngine) {
                return false;
              }
            },
            "unsupported",
            Clock.fixed(Instant.parse("2026-08-23T12:34:56Z"), ZoneOffset.UTC),
            2);

    unsupported.scanOnce();

    assertTrue(assembly.indexProjection().current().resourcesByName().isEmpty());
  }

  @Test
  public void scannerUsesStableCursorBatchAndInjectedLogicalTime() {
    scanner.scanOnce();

    assertEquals(java.util.Arrays.asList(null, "42"), tables.requestedCursors);
    assertEquals(java.util.Arrays.asList(1, 1), tables.requestedBatchSizes);
    assertEquals(
        java.util.Arrays.asList(
            Instant.parse("2026-08-23T12:34:56Z"), Instant.parse("2026-08-23T12:34:56Z")),
        tables.observedFireTimes);
  }

  @Test
  public void boundedRoundsPersistCursorWithoutScanningAllPages() {
    scanner.scanBatchOnce();
    assertEquals(java.util.Collections.singletonList((String) null), tables.requestedCursors);
    assertEquals(1, assembly.indexProjection().current().resourcesByName().size());

    scanner.scanBatchOnce();
    assertEquals(java.util.Arrays.asList(null, "42"), tables.requestedCursors);
    assertEquals(2, assembly.indexProjection().current().resourcesByName().size());

    scanner.scanBatchOnce();
    assertEquals(java.util.Arrays.asList(null, "42", null), tables.requestedCursors);
    assertEquals(2, assembly.indexProjection().current().resourcesByName().size());
  }

  // ------------------------------------------------------------------ fakes

  /** In-memory ManagedTablePort: tables with a lastMaintenanceAt stamp. */
  static final class InMemoryManagedTables implements ManagedTablePort {
    final java.util.Map<String, TableSnapshot> tables =
        new java.util.concurrent.ConcurrentHashMap<String, TableSnapshot>();
    final List<String> requestedCursors = new java.util.concurrent.CopyOnWriteArrayList<>();
    final List<Integer> requestedBatchSizes = new java.util.concurrent.CopyOnWriteArrayList<>();
    final List<Instant> observedFireTimes = new java.util.concurrent.CopyOnWriteArrayList<>();

    void add(String catalog, String db, String table, String tableId) {
      tables.put(
          tableId,
          new TableSnapshot(
              catalog, db, table, tableId, "simulated", java.time.Instant.EPOCH.toString()));
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
                  snapshot.tableFormat(),
                  java.time.Instant.now().toString()));
        }
      }
    }

    @Override
    public TablePage scanAfter(String cursor, int batchSize) {
      requestedCursors.add(cursor);
      requestedBatchSizes.add(batchSize);
      List<TableSnapshot> ordered = new java.util.ArrayList<>(tables.values());
      ordered.sort(java.util.Comparator.comparing(TableSnapshot::tableId));
      List<TableSnapshot> page =
          ordered.stream()
              .filter(snapshot -> cursor == null || snapshot.tableId().compareTo(cursor) > 0)
              .limit(batchSize)
              .collect(java.util.stream.Collectors.toList());
      String next =
          !page.isEmpty()
                  && ordered.stream()
                      .anyMatch(
                          snapshot ->
                              snapshot.tableId().compareTo(page.get(page.size() - 1).tableId()) > 0)
              ? page.get(page.size() - 1).tableId()
              : null;
      return new TablePage(page, next);
    }
  }

  /** Fixed-interval gate: eligible when the last maintenance is older than intervalSeconds. */
  static class FixedIntervalActionPlugin implements ProcessActionPlugin {
    private final String action;
    private final String engine;
    private final int intervalSeconds;
    private final List<Instant> observedFireTimes;

    FixedIntervalActionPlugin(String action, String engine, int intervalSeconds) {
      this(action, engine, intervalSeconds, new java.util.concurrent.CopyOnWriteArrayList<>());
    }

    FixedIntervalActionPlugin(
        String action, String engine, int intervalSeconds, List<Instant> observedFireTimes) {
      this.action = action;
      this.engine = engine;
      this.intervalSeconds = intervalSeconds;
      this.observedFireTimes = observedFireTimes;
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
      observedFireTimes.add(logicalFireTime);
      java.time.Instant last = java.time.Instant.parse(table.lastMaintenanceAt());
      if (logicalFireTime.minusSeconds(intervalSeconds).isBefore(last)) {
        return ScheduledEvaluation.skip();
      }
      Map<String, Object> parameters = new LinkedHashMap<String, Object>();
      parameters.put("olderThanMillis", 1L);
      parameters.put("retainLast", 1);
      return ScheduledEvaluation.create(engine, parameters);
    }
  }
}
