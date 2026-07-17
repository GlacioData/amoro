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
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.config.ConfigOption;
import org.apache.amoro.config.ConfigOptions;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.process.ExecuteEngine;
import org.apache.amoro.process.HttpRemoteSparkStandAloneSubmit;
import org.apache.amoro.process.LocalExecutionEngine;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.process.ProcessTriggerStrategy;
import org.apache.amoro.process.RecoverProcessFailedException;
import org.apache.amoro.process.TableProcess;
import org.apache.amoro.process.TableProcessStore;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Process factory for Paimon table <em>maintenance</em> actions, including metadata
 * synchronization, remote snapshot expiration, and remote orphan file cleaning.
 *
 * <p>Renamed from {@code PaimonProcessFactory} in AMORO-4200 to disambiguate from the optimizing
 * factory {@code org.apache.amoro.formats.paimon.process.PaimonProcessFactory} in the {@code
 * amoro-format-paimon} module. This factory does <b>not</b> declare any {@code supportedFormats} so
 * {@code ProcessFactoryRouter} never picks it up for optimizing routing — it only feeds the
 * action-triggered scheduler.
 */
public class PaimonMaintainProcessFactory implements ProcessFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonMaintainProcessFactory.class);

  public static final String PLUGIN_NAME = "paimon-maintain";

  public static final ConfigOption<Boolean> SYNC_TABLE_META_ENABLED =
      ConfigOptions.key("sync-table-meta.enabled").booleanType().defaultValue(true);

  public static final ConfigOption<Duration> SYNC_TABLE_META_INTERVAL =
      ConfigOptions.key("sync-table-meta.interval")
          .durationType()
          .defaultValue(Duration.ofHours(1));

  public static final ConfigOption<Integer> SYNC_TABLE_META_TRIGGER_PARALLELISM =
      ConfigOptions.key("sync-table-meta.trigger-parallelism").intType().defaultValue(1);

  public static final ConfigOption<Boolean> SNAPSHOT_EXPIRE_ENABLED =
      ConfigOptions.key("expire-snapshots.enabled").booleanType().defaultValue(false);

  public static final ConfigOption<Duration> SNAPSHOT_EXPIRE_INTERVAL =
      ConfigOptions.key("expire-snapshots.interval")
          .durationType()
          .defaultValue(Duration.ofHours(24));

  public static final ConfigOption<Boolean> CLEAN_ORPHANS_ENABLED =
      ConfigOptions.key("clean-orphans.enabled").booleanType().defaultValue(true);

  public static final ConfigOption<Duration> CLEAN_ORPHANS_INTERVAL =
      ConfigOptions.key("clean-orphans.interval").durationType().defaultValue(Duration.ofHours(48));

  public static final ConfigOption<Duration> CLEAN_ORPHANS_STATIC_TABLE_THRESHOLD =
      ConfigOptions.key("clean-orphans.static-table-threshold")
          .durationType()
          .defaultValue(Duration.ofDays(6));

  public static final ConfigOption<Integer> SPARK_VERSION =
      ConfigOptions.key("spark-version").intType().defaultValue(354);

  private final Map<Action, ProcessTriggerStrategy> actions = Maps.newHashMap();
  private ExecuteEngine remoteEngine;
  private int sparkVersion = SPARK_VERSION.defaultValue();
  private Duration cleanOrphansStaticTableThreshold =
      CLEAN_ORPHANS_STATIC_TABLE_THRESHOLD.defaultValue();

  @Override
  public void availableExecuteEngines(Collection<ExecuteEngine> allAvailableEngines) {
    this.remoteEngine = null;
    if (allAvailableEngines == null) {
      return;
    }
    for (ExecuteEngine engine : allAvailableEngines) {
      if (engine instanceof HttpRemoteSparkStandAloneSubmit
          || HttpRemoteSparkStandAloneSubmit.ENGINE_NAME.equals(engine.name())) {
        this.remoteEngine = engine;
        return;
      }
    }
  }

  @Override
  public Map<TableFormat, Set<Action>> supportedActions() {
    return Collections.singletonMap(TableFormat.PAIMON, actions.keySet());
  }

  @Override
  public ProcessTriggerStrategy triggerStrategy(TableFormat format, Action action) {
    return actions.getOrDefault(action, ProcessTriggerStrategy.METADATA_TRIGGER);
  }

  @Override
  public Duration getTriggerInterval(TableRuntime tableRuntime, TableFormat format, Action action) {
    Duration defaultInterval = triggerStrategy(format, action).getTriggerInterval();
    if (TableFormat.PAIMON.equals(format) && PaimonActions.EXPIRE_SNAPSHOTS.equals(action)) {
      return PaimonExpireSnapshotProcess.resolveTriggerInterval(
          tableRuntime.getTableConfig(), defaultInterval);
    }
    return defaultInterval;
  }

  @Override
  public Optional<TableProcess> trigger(TableRuntime tableRuntime, Action action) {
    if (!actions.containsKey(action)) {
      return Optional.empty();
    }

    if (PaimonActions.SYNC_TABLE_META.equals(action)) {
      return Optional.of(new PaimonTableMetaSyncProcess(tableRuntime));
    }

    if (PaimonActions.EXPIRE_SNAPSHOTS.equals(action)) {
      return triggerExpireSnapshots(tableRuntime);
    }

    if (PaimonActions.CLEAN_ORPHANS.equals(action)) {
      return triggerCleanOrphans(tableRuntime);
    }

    return Optional.empty();
  }

  @Override
  public TableProcess recover(TableRuntime tableRuntime, TableProcessStore store)
      throws RecoverProcessFailedException {
    if (PaimonActions.SYNC_TABLE_META.equals(store.getAction())) {
      return new PaimonTableMetaSyncProcess(tableRuntime);
    }
    if (PaimonActions.EXPIRE_SNAPSHOTS.equals(store.getAction())) {
      if (remoteEngine == null) {
        throw new RecoverProcessFailedException(
            "Cannot recover Paimon expire snapshots process without "
                + HttpRemoteSparkStandAloneSubmit.ENGINE_NAME
                + " execute engine");
      }
      return new PaimonExpireSnapshotProcess(tableRuntime, remoteEngine, sparkVersion);
    }
    if (PaimonActions.CLEAN_ORPHANS.equals(store.getAction())) {
      if (remoteEngine == null) {
        throw new RecoverProcessFailedException(
            "Cannot recover Paimon clean orphans process without "
                + HttpRemoteSparkStandAloneSubmit.ENGINE_NAME
                + " execute engine");
      }
      return PaimonCleanOrphansProcess.recover(tableRuntime, remoteEngine, sparkVersion, store);
    }
    throw new RecoverProcessFailedException(
        "Unsupported action for PaimonMaintainProcessFactory: " + store.getAction());
  }

  @Override
  public void open(Map<String, String> properties) {
    resetConfiguredState();
    Map<String, String> safeProperties = properties == null ? Collections.emptyMap() : properties;
    Configurations configs = Configurations.fromMap(safeProperties);
    this.sparkVersion = configs.getInteger(SPARK_VERSION);
    this.cleanOrphansStaticTableThreshold =
        configs.getDuration(CLEAN_ORPHANS_STATIC_TABLE_THRESHOLD);
    if (cleanOrphansStaticTableThreshold.isZero()
        || cleanOrphansStaticTableThreshold.isNegative()) {
      throw new IllegalArgumentException(
          CLEAN_ORPHANS_STATIC_TABLE_THRESHOLD.key() + " must be greater than zero");
    }

    if (configs.getBoolean(SYNC_TABLE_META_ENABLED)) {
      Duration interval = configs.getDuration(SYNC_TABLE_META_INTERVAL);
      int parallelism = configs.getInteger(SYNC_TABLE_META_TRIGGER_PARALLELISM);
      this.actions.put(
          PaimonActions.SYNC_TABLE_META,
          new ProcessTriggerStrategy(interval, false, Math.max(parallelism, 1)));
    }
    if (configs.getBoolean(SNAPSHOT_EXPIRE_ENABLED)) {
      Duration interval = configs.getDuration(SNAPSHOT_EXPIRE_INTERVAL);
      this.actions.put(
          PaimonActions.EXPIRE_SNAPSHOTS, ProcessTriggerStrategy.triggerAtFixRate(interval));
    }
    if (configs.getBoolean(CLEAN_ORPHANS_ENABLED)) {
      Duration interval = configs.getDuration(CLEAN_ORPHANS_INTERVAL);
      this.actions.put(
          PaimonActions.CLEAN_ORPHANS, ProcessTriggerStrategy.triggerAtFixRate(interval));
    }
    LOG.info(
        "Apache Paimon Process Factory initialized actions {}.",
        this.actions.keySet().stream().map(Action::getName).collect(Collectors.toList()));
  }

  @Override
  public void close() {
    resetConfiguredState();
  }

  @Override
  public String name() {
    return PLUGIN_NAME;
  }

  public String executionEngine() {
    return LocalExecutionEngine.ENGINE_NAME;
  }

  private void resetConfiguredState() {
    actions.clear();
    remoteEngine = null;
    sparkVersion = SPARK_VERSION.defaultValue();
    cleanOrphansStaticTableThreshold = CLEAN_ORPHANS_STATIC_TABLE_THRESHOLD.defaultValue();
  }

  private Optional<TableProcess> triggerExpireSnapshots(TableRuntime tableRuntime) {
    if (remoteEngine == null) {
      LOG.warn(
          "Skip Paimon expire snapshots for table {} because execute engine {} is not installed.",
          tableRuntime.getTableIdentifier(),
          HttpRemoteSparkStandAloneSubmit.ENGINE_NAME);
      return Optional.empty();
    }
    Duration interval =
        getTriggerInterval(tableRuntime, TableFormat.PAIMON, PaimonActions.EXPIRE_SNAPSHOTS);
    return PaimonExpireSnapshotProcess.trigger(tableRuntime, remoteEngine, sparkVersion, interval)
        .map(process -> (TableProcess) process);
  }

  private Optional<TableProcess> triggerCleanOrphans(TableRuntime tableRuntime) {
    if (remoteEngine == null) {
      LOG.warn(
          "Skip Paimon clean orphans for table {} because execute engine {} is not installed.",
          tableRuntime.getTableIdentifier(),
          HttpRemoteSparkStandAloneSubmit.ENGINE_NAME);
      return Optional.empty();
    }
    ProcessTriggerStrategy strategy = actions.get(PaimonActions.CLEAN_ORPHANS);
    return PaimonCleanOrphansProcess.trigger(
            tableRuntime,
            remoteEngine,
            sparkVersion,
            strategy.getTriggerInterval(),
            cleanOrphansStaticTableThreshold)
        .map(process -> (TableProcess) process);
  }
}
