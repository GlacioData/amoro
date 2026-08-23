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

import org.apache.amoro.process.ProcessAdmissionException;
import org.apache.amoro.process.ProcessCreateIntent;
import org.apache.amoro.process.ProcessCreationResult;
import org.apache.amoro.process.ProcessCreationService;
import org.apache.amoro.process.ProcessResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The scheduled trigger (process spec §6.3): scans the managed tables, asks each action plugin
 * whether the table is due, and routes eligible tables through the SAME admission path as the REST
 * create (idempotent key, single-active slot). The scanner owns no admission lock: all manual and
 * scheduled callers share the singleton creation service. Table failures are isolated so one broken
 * simulated fact never aborts the round.
 */
public final class ProcessTriggerScanner {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessTriggerScanner.class);

  private final ProcessCreationService creationService;
  private final ManagedTablePort tables;
  private final ProcessActionPlugin plugin;
  private final String scanIdentity;
  private final Clock clock;
  private final int batchSize;
  private final Set<String> batchVisitedCursors = new HashSet<>();
  private String batchCursor;
  private Instant batchFireTime;

  public ProcessTriggerScanner(
      ProcessCreationService creationService,
      ManagedTablePort tables,
      ProcessActionPlugin plugin,
      String scanIdentity,
      Clock clock,
      int batchSize) {
    this.creationService = Objects.requireNonNull(creationService, "creationService");
    this.tables = Objects.requireNonNull(tables, "tables");
    this.plugin = Objects.requireNonNull(plugin, "plugin");
    this.scanIdentity = Objects.requireNonNull(scanIdentity, "scanIdentity");
    this.clock = Objects.requireNonNull(clock, "clock");
    if (batchSize < 1 || batchSize > 1000) {
      throw new IllegalArgumentException("batchSize must be in [1, 1000]");
    }
    this.batchSize = batchSize;
  }

  /** One trigger round: scan → gate → idempotent create for each eligible table. */
  public synchronized void scanOnce() {
    do {
      scanBatchOnce();
    } while (batchCursor != null);
  }

  /** Processes at most one configured page and retains its cursor for the next bounded round. */
  public synchronized int scanBatchOnce() {
    if (batchFireTime == null) {
      batchFireTime = Instant.now(clock);
    }
    String currentCursor = batchCursor;
    ManagedTablePort.TablePage page = tables.scanAfter(currentCursor, batchSize);
    for (ManagedTablePort.TableSnapshot table : page.tables()) {
      try {
        evaluateAndMaybeCreate(table, batchFireTime);
      } catch (RuntimeException isolated) {
        LOG.warn(
            "Trigger for {}.{}.{} isolated and skipped this round.",
            table.catalog(),
            table.database(),
            table.table(),
            isolated);
      }
    }
    String next = page.nextCursor();
    if (next != null && (next.equals(currentCursor) || !batchVisitedCursors.add(next))) {
      resetBatchTraversal();
      throw new IllegalStateException("ManagedTablePort returned a repeated cursor: " + next);
    }
    if (next == null) {
      resetBatchTraversal();
    } else {
      batchCursor = next;
    }
    return page.tables().size();
  }

  private void resetBatchTraversal() {
    batchCursor = null;
    batchFireTime = null;
    batchVisitedCursors.clear();
  }

  private void evaluateAndMaybeCreate(ManagedTablePort.TableSnapshot table, Instant fireTime) {
    ProcessActionPlugin.ScheduledEvaluation evaluation = plugin.evaluateScheduled(table, fireTime);
    if (!evaluation.shouldCreate()) {
      return;
    }
    String engine = evaluation.executionEngine();
    String lockKey = table.tableId() + "|" + plugin.action();
    if (!plugin.supports(table.tableFormat(), engine)) {
      LOG.warn(
          "Scheduled pair is not deployed; skipping format={}, action={}, engine={}.",
          table.tableFormat(),
          plugin.action(),
          engine);
      return;
    }
    String idempotencyKey = "scan|" + scanIdentity + "|" + lockKey + "|" + windowOf(fireTime);
    try {
      ProcessCreationResult result =
          creationService.create(
              ProcessCreateIntent.resolve(
                  new ProcessResource.TableRef(
                      table.catalog(),
                      table.database(),
                      table.table(),
                      table.tableId(),
                      table.tableFormat()),
                  plugin.action(),
                  engine,
                  "SCHEDULED",
                  idempotencyKey,
                  evaluation.parameters()));
      LOG.info(
          "Scheduled process {} for {}.{}.{} action {}.",
          result.replayed() ? "replayed" : "created",
          table.catalog(),
          table.database(),
          table.table(),
          plugin.action());
    } catch (ProcessAdmissionException admissionRejected) {
      LOG.debug("Scheduled create for {} rejected: {}", lockKey, admissionRejected.getMessage());
    }
  }

  private static String windowOf(Instant fireTime) {
    // one-minute window: a scan retrying within the window replays, the next window is new
    return fireTime.toString().substring(0, 16);
  }
}
