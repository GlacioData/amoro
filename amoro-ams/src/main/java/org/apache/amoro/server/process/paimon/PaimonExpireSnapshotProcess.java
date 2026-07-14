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
import org.apache.amoro.AmoroTable;
import org.apache.amoro.PaimonActions;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.process.ExecuteEngine;
import org.apache.amoro.process.HttpRemoteSparkStandAloneSubmit;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.TableProcess;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Remote table process that submits Paimon expire_snapshots procedure to Spark via HTTP. The SQL is
 * assembled from table properties and submitted through {@link HttpRemoteSparkStandAloneSubmit}.
 */
public class PaimonExpireSnapshotProcess extends TableProcess {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonExpireSnapshotProcess.class);

  private static final Duration DEFAULT_SNAPSHOT_TIME_RETAINED =
      CoreOptions.SNAPSHOT_TIME_RETAINED.defaultValue();
  private static final int DEFAULT_SNAPSHOT_NUM_RETAINED_MAX = 10;

  private static final String DEFAULT_SNAPSHOT_EXPIRE_LIMIT = "500";
  private static final String DEFAULT_SNAPSHOT_EXPIRE_EXECUTION_MODE = "async";
  private static final String DEFAULT_SNAPSHOT_IGNORE_EMPTY_COMMIT = "true";
  private static final String DEFAULT_SNAPSHOT_CLEAN_EMPTY_DIRECTORIES = "true";
  private final int sparkVersion;

  public PaimonExpireSnapshotProcess(
      TableRuntime tableRuntime, ExecuteEngine engine, int sparkVersion) {
    super(tableRuntime, engine);
    this.sparkVersion = sparkVersion;
  }

  public static Optional<PaimonExpireSnapshotProcess> trigger(
      TableRuntime tableRuntime, ExecuteEngine engine, int sparkVersion, Duration interval) {
    long lastExecuteTime =
        tableRuntime.getState(DefaultTableRuntime.CLEANUP_STATE_KEY).getLastSnapshotsExpiringTime();
    if (System.currentTimeMillis() - lastExecuteTime < interval.toMillis()) {
      LOG.debug(
          "Skip expire snapshots for table {}, last execute time: {}",
          tableRuntime.getTableIdentifier(),
          lastExecuteTime);
      return Optional.empty();
    }
    if (!hasSnapshotsExceedingRetainMax(tableRuntime)) {
      return Optional.empty();
    }
    return Optional.of(new PaimonExpireSnapshotProcess(tableRuntime, engine, sparkVersion));
  }

  private static boolean hasSnapshotsExceedingRetainMax(TableRuntime tableRuntime) {
    try {
      AmoroTable<?> amoroTable = tableRuntime.loadTable();
      Object originalTable = amoroTable.originalTable();
      if (!(originalTable instanceof Table)) {
        LOG.warn(
            "Skip expire snapshots for table {} because original table is not Paimon Table: {}",
            tableRuntime.getTableIdentifier(),
            originalTable == null ? "null" : originalTable.getClass().getName());
        return false;
      }

      Table paimonTable = (Table) originalTable;
      if (!(paimonTable instanceof FileStoreTable)) {
        LOG.warn(
            "Skip expire snapshots for table {} because Paimon Table is not FileStoreTable: {}",
            tableRuntime.getTableIdentifier(),
            paimonTable.getClass().getName());
        return false;
      }

      FileStoreTable fileStoreTable = (FileStoreTable) paimonTable;

      long snapshotCount = fileStoreTable.store().snapshotManager().snapshotCount();
      int retainMax = parseRetainMax(tableRuntime.getTableConfig());
      if (snapshotCount <= retainMax) {
        LOG.info(
            "Skip expire snapshots for table {} because snapshot count {} does not exceed retain max {}",
            tableRuntime.getTableIdentifier(),
            snapshotCount,
            retainMax);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.warn(
          "Skip expire snapshots for table {} because snapshot metadata cannot be read",
          tableRuntime.getTableIdentifier(),
          e);
      return false;
    }
  }

  @Override
  public Action getAction() {
    return PaimonActions.EXPIRE_SNAPSHOTS;
  }

  @Override
  public Map<String, String> getProcessParameters() {
    String executeUser = getExecutionUser();
    Map<String, String> params = new HashMap<>();
    params.put("hql", buildExpireSnapshotsSql());
    params.put("curUser", executeUser);
    params.put("logUser", executeUser);
    params.put("group", executeUser);
    params.put("userId", "470");
    params.put("sparkVersion", String.valueOf(sparkVersion));
    params.put("sourceTag", "AMORO");
    params.put("conf", "{\"sparkVersion\":\"" + sparkVersion + "\",\"paimon.version\":\"1.3\"}");
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
    return summary;
  }

  @Override
  public void afterComplete(ProcessStatus status) {
    if (status == ProcessStatus.SUCCESS) {
      tableRuntime.updateState(
          DefaultTableRuntime.CLEANUP_STATE_KEY,
          cleanUp -> cleanUp.setLastSnapshotsExpiringTime(System.currentTimeMillis()));
      LOG.info(
          "Updated lastSnapshotsExpiringTime for table {} after successful expire snapshots",
          getTableIdentifier());
    }
  }

  String buildExpireSnapshotsSql() {
    Map<String, String> tableConfig = tableRuntime.getTableConfig();
    ServerTableIdentifier tableId = getTableIdentifier();

    Duration timeRetained = parseTimeRetained(tableConfig);
    int retainMax = parseRetainMax(tableConfig);

    long olderThanTimestampMillis = System.currentTimeMillis() - timeRetained.toMillis();
    String olderThanTimestamp = new Timestamp(olderThanTimestampMillis).toString();

    String fullTableName = String.format("%s.%s", tableId.getDatabase(), tableId.getTableName());

    String sql =
        String.format(
            "CALL sys.expire_snapshots(table => '%s', retain_max => %d, older_than => '%s', options => '%s')",
            fullTableName, retainMax, olderThanTimestamp, buildSnapshotOptions(tableConfig));

    LOG.info("Built expire snapshots SQL for table {}: {}", tableId, sql);
    return sql;
  }

  private static String buildSnapshotOptions(Map<String, String> tableConfig) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put(
        CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key(),
        getSnapshotOption(
            tableConfig, CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key(), DEFAULT_SNAPSHOT_EXPIRE_LIMIT));
    options.put(
        CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE.key(),
        getSnapshotOption(
            tableConfig,
            CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE.key(),
            DEFAULT_SNAPSHOT_EXPIRE_EXECUTION_MODE));
    options.put(
        CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT.key(),
        getSnapshotOption(
            tableConfig,
            CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT.key(),
            DEFAULT_SNAPSHOT_IGNORE_EMPTY_COMMIT));
    options.put(
        CoreOptions.SNAPSHOT_CLEAN_EMPTY_DIRECTORIES.key(),
        getSnapshotOption(
            tableConfig,
            CoreOptions.SNAPSHOT_CLEAN_EMPTY_DIRECTORIES.key(),
            DEFAULT_SNAPSHOT_CLEAN_EMPTY_DIRECTORIES));
    return options.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining(","));
  }

  private static String getSnapshotOption(
      Map<String, String> tableConfig, String key, String defaultValue) {
    String value = tableConfig.get(key);
    return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
  }

  private Duration parseTimeRetained(Map<String, String> tableConfig) {
    String value = tableConfig.get(CoreOptions.SNAPSHOT_TIME_RETAINED.key());
    if (value == null || value.isEmpty()) {
      return DEFAULT_SNAPSHOT_TIME_RETAINED;
    }
    try {
      return TimeUtils.parseDuration(value);
    } catch (Exception e) {
      LOG.warn(
          "Failed to parse {} value '{}', using default {}",
          CoreOptions.SNAPSHOT_TIME_RETAINED.key(),
          value,
          DEFAULT_SNAPSHOT_TIME_RETAINED);
      return DEFAULT_SNAPSHOT_TIME_RETAINED;
    }
  }

  private static int parseRetainMax(Map<String, String> tableConfig) {
    String value = tableConfig.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key());
    if (value == null || value.isEmpty()) {
      return DEFAULT_SNAPSHOT_NUM_RETAINED_MAX;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      LOG.warn(
          "Failed to parse {} value '{}', using default {}",
          CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(),
          value,
          DEFAULT_SNAPSHOT_NUM_RETAINED_MAX);
      return DEFAULT_SNAPSHOT_NUM_RETAINED_MAX;
    }
  }

  static Duration resolveTriggerInterval(
      Map<String, String> tableConfig, Duration defaultInterval) {
    String value = tableConfig.get(CoreOptions.SNAPSHOT_TIME_RETAINED.key());
    if (value == null || value.trim().isEmpty()) {
      return defaultInterval;
    }

    try {
      Duration timeRetained = TimeUtils.parseDuration(value);
      if (timeRetained.compareTo(DEFAULT_SNAPSHOT_TIME_RETAINED) > 0) {
        return timeRetained;
      }
      if (timeRetained.compareTo(DEFAULT_SNAPSHOT_TIME_RETAINED) < 0) {
        LOG.warn(
            "{} value '{}' is less than one hour, using default expire snapshots interval {}",
            CoreOptions.SNAPSHOT_TIME_RETAINED.key(),
            value,
            defaultInterval);
      }
      return defaultInterval;
    } catch (Exception e) {
      LOG.warn(
          "Failed to parse {} value '{}', using default expire snapshots interval {}",
          CoreOptions.SNAPSHOT_TIME_RETAINED.key(),
          value,
          defaultInterval);
      return defaultInterval;
    }
  }
}
