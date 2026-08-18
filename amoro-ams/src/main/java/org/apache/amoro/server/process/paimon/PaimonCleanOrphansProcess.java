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

package org.apache.amoro.server.process.paimon;

import org.apache.amoro.Action;
import org.apache.amoro.PaimonActions;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.TableSnapshot;
import org.apache.amoro.process.ExecuteEngine;
import org.apache.amoro.process.HttpRemoteSparkStandAloneSubmit;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.TableProcess;
import org.apache.amoro.process.TableProcessStore;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Remote table process that submits Paimon remove_orphan_files procedure to Spark via HTTP. */
public class PaimonCleanOrphansProcess extends TableProcess {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonCleanOrphansProcess.class);
  private static final String TRIGGER_TIME_SUMMARY_KEY = "clean-orphans-trigger-time";

  private final int sparkVersion;
  private final long triggerTime;

  public PaimonCleanOrphansProcess(
      TableRuntime tableRuntime, ExecuteEngine engine, int sparkVersion) {
    this(tableRuntime, engine, sparkVersion, System.currentTimeMillis());
  }

  private PaimonCleanOrphansProcess(
      TableRuntime tableRuntime, ExecuteEngine engine, int sparkVersion, long triggerTime) {
    super(tableRuntime, engine);
    this.sparkVersion = sparkVersion;
    this.triggerTime = triggerTime;
  }

  public static Optional<PaimonCleanOrphansProcess> trigger(
      TableRuntime tableRuntime,
      ExecuteEngine engine,
      int sparkVersion,
      Duration interval,
      Duration staticTableThreshold) {
    long now = System.currentTimeMillis();
    long lastExecuteTime =
        tableRuntime.getState(DefaultTableRuntime.CLEANUP_STATE_KEY).getLastOrphanFilesCleanTime();
    // Preserve the existing interval gate and avoid loading Paimon metadata before it is due.
    if (now - lastExecuteTime < interval.toMillis()) {
      LOG.debug(
          "Skip clean orphans for table {}, last execute time: {}",
          tableRuntime.getTableIdentifier(),
          lastExecuteTime);
      return Optional.empty();
    }

    long lastCommitTime = getLastSnapshotCommitTime(tableRuntime);
    // lastOrphanFilesCleanTime stores both successful cleanup trigger and static-table decision
    // times. Evaluate static status first so data committed after a static decision, but stale by
    // the next scheduler run, is not incorrectly submitted to Spark.
    if (now - lastCommitTime >= staticTableThreshold.toMillis()) {
      tableRuntime.updateState(
          DefaultTableRuntime.CLEANUP_STATE_KEY,
          cleanUp -> cleanUp.setLastOrphanFilesCleanTime(now));
      LOG.info(
          "Skip clean orphans for static table {}, last snapshot commit time: {}, "
              + "static table threshold: {}",
          tableRuntime.getTableIdentifier(),
          lastCommitTime,
          staticTableThreshold);
      return Optional.empty();
    }

    // Submit a new cleanup task only when the Snapshot was committed after the last successful
    // cleanup trigger or static-table decision.
    if (lastCommitTime <= lastExecuteTime) {
      LOG.debug(
          "Skip clean orphans for table {}, no new snapshot since last clean or static check time: {}",
          tableRuntime.getTableIdentifier(),
          lastExecuteTime);
      return Optional.empty();
    }
    return Optional.of(new PaimonCleanOrphansProcess(tableRuntime, engine, sparkVersion, now));
  }

  static PaimonCleanOrphansProcess recover(
      TableRuntime tableRuntime, ExecuteEngine engine, int sparkVersion, TableProcessStore store) {
    return new PaimonCleanOrphansProcess(
        tableRuntime, engine, sparkVersion, restoreTriggerTime(store));
  }

  private static long getLastSnapshotCommitTime(TableRuntime tableRuntime) {
    TableSnapshot snapshot;
    try {
      snapshot = tableRuntime.loadTable().currentSnapshot();
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Cannot read latest Paimon snapshot for table " + tableRuntime.getTableIdentifier(), e);
    }
    if (snapshot == null) {
      throw new IllegalStateException(
          "Cannot clean orphans for table "
              + tableRuntime.getTableIdentifier()
              + " because no Paimon snapshot exists");
    }
    return snapshot.commitTime();
  }

  private static long restoreTriggerTime(TableProcessStore store) {
    Map<String, String> summary = store.getSummary();
    String triggerTime = summary == null ? null : summary.get(TRIGGER_TIME_SUMMARY_KEY);
    if (triggerTime != null) {
      try {
        long parsedTriggerTime = Long.parseLong(triggerTime);
        if (parsedTriggerTime > 0) {
          return parsedTriggerTime;
        }
      } catch (NumberFormatException e) {
        LOG.warn(
            "Ignore invalid clean orphans trigger time {} for process {}",
            triggerTime,
            store.getProcessId());
      }
    }

    long createTime = store.getCreateTime();
    return createTime > 0 ? createTime : System.currentTimeMillis();
  }

  @Override
  public Action getAction() {
    return PaimonActions.CLEAN_ORPHANS;
  }

  @Override
  public Map<String, String> getProcessParameters() {
    String executeUser = getExecutionUser();
    Map<String, String> params = new HashMap<>();
    params.put("hql", buildCleanOrphansSql());
    params.put("curUser", executeUser);
    params.put("logUser", executeUser);
    params.put("group", executeUser);
    params.put("userId", "470");
    params.put("sparkVersion", String.valueOf(sparkVersion));
    params.put("sourceTag", "AMORO");
    params.put(
        "conf",
        "{\"sparkVersion\":\"" + sparkVersion + "\",\"spark.custom.paimon.version\":\"1.3\"}");
    return params;
  }

  private String getExecutionUser() {
    if (executeEngine instanceof HttpRemoteSparkStandAloneSubmit) {
      return ((HttpRemoteSparkStandAloneSubmit) executeEngine).configuredExecuteUser();
    }
    return "sljdp";
  }

  @Override
  public Map<String, String> getSummary() {
    Map<String, String> summary = new HashMap<>();
    summary.put("table", getTableIdentifier().toString());
    summary.put("action", getAction().getName());
    summary.put(TRIGGER_TIME_SUMMARY_KEY, String.valueOf(triggerTime));
    return summary;
  }

  @Override
  public void afterComplete(ProcessStatus status) {
    if (status == ProcessStatus.SUCCESS) {
      tableRuntime.updateState(
          DefaultTableRuntime.CLEANUP_STATE_KEY,
          cleanUp -> cleanUp.setLastOrphanFilesCleanTime(triggerTime));
      LOG.info(
          "Updated lastOrphanFilesCleanTime for table {} to clean orphans trigger time {} after "
              + "successful execution",
          getTableIdentifier(),
          triggerTime);
    }
  }

  String buildCleanOrphansSql() {
    ServerTableIdentifier tableId = getTableIdentifier();
    String fullTableName = String.format("%s.%s", tableId.getDatabase(), tableId.getTableName());
    String sql = String.format("CALL sys.remove_orphan_files(table => '%s')", fullTableName);

    LOG.info("Built clean orphans SQL for table {}: {}", tableId, sql);
    return sql;
  }
}
