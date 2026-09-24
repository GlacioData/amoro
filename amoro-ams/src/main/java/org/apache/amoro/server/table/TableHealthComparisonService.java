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

package org.apache.amoro.server.table;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.table.descriptor.TableHealthComparisonView;
import org.apache.amoro.table.health.TableHealthSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Coordinates optional format diagnostics without participating in data commits. */
public final class TableHealthComparisonService extends PersistentBase {
  private static final Logger LOG = LoggerFactory.getLogger(TableHealthComparisonService.class);
  private static final TableHealthComparisonService INSTANCE = new TableHealthComparisonService();
  private final Semaphore buildBudget = new Semaphore(1);
  private final Set<Long> inFlight = ConcurrentHashMap.newKeySet();
  private static final long RECONCILE_NANOS = TimeUnit.MINUTES.toNanos(5);
  private final LongSupplier clock;
  // Runtime identity is the lifetime boundary; removed/reloaded tables cannot retain hints.
  private final Map<DefaultTableRuntime, Observation> observations =
      Collections.synchronizedMap(new WeakHashMap<>());

  private TableHealthComparisonService() {
    this(System::nanoTime);
  }

  TableHealthComparisonService(LongSupplier clock) {
    this.clock = clock;
  }

  public static TableHealthComparisonService instance() {
    return INSTANCE;
  }

  public void capture(
      DefaultTableRuntime runtime,
      AmoroTable<?> table,
      OptimizingPlanResult<?> plan,
      FormatTableAnalysis analysis) {
    TableHealthSupport support = TableFormatSupports.instance().healthSupport(runtime.getFormat());
    if (!support.enabled()) {
      return;
    }
    try {
      observeLatest(runtime, support);
      support.capture(runtime.store(), table, plan, analysis);
    } catch (RuntimeException e) {
      LOG.warn("Unable to capture health comparison for process {}", plan.getProcessId(), e);
    }
  }

  /**
   * Work runs on the existing background refresh executor, never on dashboard or commit threads.
   */
  public void refresh(DefaultTableRuntime runtime, AmoroTable<?> table) {
    TableHealthSupport support = TableFormatSupports.instance().healthSupport(runtime.getFormat());
    if (!support.enabled()) {
      return;
    }
    long id = runtime.getTableIdentifier().getId();
    if (!inFlight.add(id)) {
      return;
    }
    if (!buildBudget.tryAcquire()) {
      inFlight.remove(id);
      return;
    }
    try {
      observeIfNeeded(runtime, support);
      runtime.runHealthComparison(
          () -> {
            RuntimeHealthSnapshot current = runtime.getRuntimeHealthSnapshot().orElse(null);
            support.refreshComparison(
                runtime.store(),
                table,
                current == null ? null : current.getHealthDetails(),
                current == null ? -1 : current.getHealthScore());
          });
    } catch (RuntimeException e) {
      LOG.warn("Unable to build health comparison for {}", runtime.getTableIdentifier(), e);
    } finally {
      buildBudget.release();
      inFlight.remove(id);
    }
  }

  private void observeIfNeeded(DefaultTableRuntime runtime, TableHealthSupport support) {
    long now = clock.getAsLong();
    long[] revision = {
      runtime.getProcessId(), runtime.getLastPlanTime(),
      runtime.getLastMinorOptimizingTime(), runtime.getLastMajorOptimizingTime(),
      runtime.getLastFullOptimizingTime(), runtime.getLastOptimizedSnapshotId(),
      runtime.getLastOptimizedChangeSnapshotId()
    };
    Observation previous = observations.get(runtime);
    if (previous != null
        && Arrays.equals(previous.revision, revision)
        && now - previous.checkedAt < RECONCILE_NANOS) {
      return;
    }
    observeLatest(runtime, support);
    // Failed lookups remain retryable on the next refresh; never advance this hint on failure.
    observations.put(runtime, new Observation(now, revision));
  }

  private static final class Observation {
    private final long checkedAt;
    private final long[] revision;

    private Observation(long checkedAt, long[] revision) {
      this.checkedAt = checkedAt;
      this.revision = revision;
    }
  }

  void observeLatest(DefaultTableRuntime runtime, TableHealthSupport support) {
    TableProcessMeta process =
        getAs(
            TableProcessMapper.class,
            mapper ->
                mapper.latestSuccessfulOptimizingProcess(runtime.getTableIdentifier().getId()));
    if (process != null) {
      support.observeSuccess(
          runtime.store(),
          process.getProcessId(),
          process.getProcessType(),
          process.getFinishTime());
    }
  }

  public TableHealthComparisonView view(DefaultTableRuntime runtime) {
    return runtime == null
        ? null
        : TableFormatSupports.instance()
            .healthSupport(runtime.getFormat())
            .comparisonView(runtime.store());
  }
}
