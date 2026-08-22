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

import org.apache.amoro.process.rest.ProcessRestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The scheduled trigger (process spec §6.3): scans the managed tables, asks each action plugin
 * whether the table is due, and routes eligible tables through the SAME admission path as the REST
 * create (idempotent key, single-active slot). A per-(tableId,action) mutex serializes concurrent
 * scans inside this process; table failures are isolated so one broken table never aborts the
 * round.
 */
public final class ProcessTriggerScanner {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessTriggerScanner.class);

  private final ProcessRestSupport rest;
  private final ManagedTablePort tables;
  private final ProcessActionPlugin plugin;
  private final String scanIdentity;

  /** Admission mutexes per (tableId|action); single-instance only (spec §5.2 note). */
  private final ConcurrentHashMap<String, Object> admissionLocks =
      new ConcurrentHashMap<String, Object>();

  public ProcessTriggerScanner(
      ProcessRestSupport rest,
      ManagedTablePort tables,
      ProcessActionPlugin plugin,
      String scanIdentity) {
    this.rest = rest;
    this.tables = tables;
    this.plugin = plugin;
    this.scanIdentity = scanIdentity;
  }

  /** One trigger round: scan → gate → idempotent create for each eligible table. */
  public void scanOnce() {
    Instant fireTime = Instant.now();
    for (ManagedTablePort.TableSnapshot table : tables.scan()) {
      try {
        evaluateAndMaybeCreate(table, fireTime);
      } catch (RuntimeException isolated) {
        LOG.warn(
            "Trigger for {}.{}.{} isolated and skipped this round.",
            table.catalog(),
            table.database(),
            table.table(),
            isolated);
      }
    }
  }

  private void evaluateAndMaybeCreate(ManagedTablePort.TableSnapshot table, Instant fireTime) {
    ProcessActionPlugin.ScheduledEvaluation evaluation = plugin.evaluateScheduled(table, fireTime);
    if (!evaluation.shouldCreate()) {
      return;
    }
    String lockKey = table.tableId() + "|" + plugin.action();
    Object admission = admissionLocks.computeIfAbsent(lockKey, key -> new Object());
    synchronized (admission) {
      // the idempotency key is stable per (table, action, scan window): replays in the same
      // window return the original resource instead of duplicating
      String idempotencyKey = "scan|" + scanIdentity + "|" + lockKey + "|" + windowOf(fireTime);
      try {
        rest.create(
            table.catalog(),
            table.database(),
            table.table(),
            idempotencyKey,
            plugin.action(),
            engineOf(plugin, table.tableFormat()),
            evaluation.parameters(),
            "SCHEDULED",
            table.tableFormat());
        LOG.info(
            "Scheduled process created for {}.{}.{} action {}.",
            table.catalog(),
            table.database(),
            table.table(),
            plugin.action());
      } catch (org.apache.amoro.process.rest.ApiError admissionRejected) {
        // ACTIVE_PROCESS_EXISTS and same-window replay are normal scheduled outcomes
        LOG.debug("Scheduled create for {} rejected: {}", lockKey, admissionRejected.getMessage());
      }
    }
  }

  private static String engineOf(ProcessActionPlugin plugin, String tableFormat) {
    // the (format, action, engine) pair is decided by the plugin against the table's real
    // format — never guessed from the action alone (spec §6.2/§6.3)
    return plugin.supports(tableFormat, "local") ? "local" : "remote-spark";
  }

  private static String windowOf(Instant fireTime) {
    // one-minute window: a scan retrying within the window replays, the next window is new
    return fireTime.toString().substring(0, 16);
  }
}
