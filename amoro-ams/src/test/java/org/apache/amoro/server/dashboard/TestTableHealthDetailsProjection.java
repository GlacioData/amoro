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

package org.apache.amoro.server.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.http.Context;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.TableIdentifier;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.controller.TableController;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.RuntimeHealthSnapshot;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.server.table.TableService;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TableDetailObservation;
import org.apache.amoro.table.descriptor.TableHealthDetailsView;
import org.apache.amoro.table.descriptor.TableSummary;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableFileStatisticsSnapshot;
import org.apache.amoro.table.health.TableHealthComponent;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.table.health.TableHealthEvaluation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

class TestTableHealthDetailsProjection {

  private static final long TABLE_ID = 17L;
  private static final long SNAPSHOT_ID = Long.MAX_VALUE;

  private CatalogManager catalogManager;
  private TableManager tableManager;
  private ServerTableDescriptor tableDescriptor;
  private TableService tableService;
  private Context context;
  private ServerTableIdentifier identifier;
  private ServerTableMeta serverTableMeta;
  private TableRuntimeMeta runtimeMeta;

  @BeforeEach
  void setUp() {
    catalogManager = mock(CatalogManager.class);
    tableManager = mock(TableManager.class);
    tableDescriptor = mock(ServerTableDescriptor.class);
    tableService = mock(TableService.class);
    context = mock(Context.class);
    identifier =
        ServerTableIdentifier.of(TABLE_ID, "catalog", "database", "table", TableFormat.PAIMON);

    serverTableMeta = new ServerTableMeta();
    serverTableMeta.setTableSummary(new TableSummary());
    runtimeMeta = new TableRuntimeMeta();
    runtimeMeta.setStatusCode(OptimizingStatus.IDLE.getCode());

    when(context.pathParam("catalog")).thenReturn("catalog");
    when(context.pathParam("db")).thenReturn("database");
    when(context.pathParam("table")).thenReturn("table");
    when(catalogManager.catalogExist("catalog")).thenReturn(true);
    when(tableDescriptor.getTableDetail(any())).thenReturn(serverTableMeta);
    when(tableManager.getServerTableIdentifier(any(TableIdentifier.class))).thenReturn(identifier);
    when(tableManager.getTableRuntimeMata(identifier)).thenReturn(runtimeMeta);
  }

  @Test
  void projectsRuntimePaimonDetailsInsteadOfPersistedHealthAndKeepsLegacyScores() {
    TableAnalysisKey key = analysisKey(SNAPSHOT_ID);
    org.apache.amoro.table.TableSummary stored =
        storedSummary(95, details(analysisKey(SNAPSHOT_ID - 1), SNAPSHOT_ID - 1));
    stored.setSmallFileScore(11);
    stored.setEqualityDeleteScore(22);
    stored.setPositionalDeleteScore(33);
    runtimeMeta.setTableSummary(stored);
    DefaultTableRuntime runtime = runtimeWithHealth(0, details(key, SNAPSHOT_ID));
    when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);

    controllerWithRuntime().getTableDetail(context);

    TableSummary projected = serverTableMeta.getTableSummary();
    assertEquals(0, projected.getHealthScore());
    assertEquals(11, projected.getSmallFileScore());
    assertEquals(22, projected.getEqualityDeleteScore());
    assertEquals(33, projected.getPositionalDeleteScore());
    TableHealthDetailsView view = projected.getHealthDetails();
    assertEquals(Long.toString(SNAPSHOT_ID), view.getSnapshotId());
    assertNull(view.getChangeSnapshotId());
    assertEquals(Long.toString(Long.MAX_VALUE - 2), view.getSchemaId());
    assertEquals(key.encoded(), view.getEvaluationKey());
    assertEquals("9223372036854775806", view.getMetrics().get("totalFileSize"));
    assertEquals("SORTED_RUN", view.getComponents().get(0).getCode());
    assertEquals(72, view.getComponents().get(0).getScore());
    assertEquals("9223372036854775805", view.getComponents().get(0).getMetrics().get("fileSize"));
    assertEquals(Integer.valueOf(100), view.getComponents().get(0).getWeight());
    assertEquals("WEIGHTED", view.getComponents().get(0).getCombination());
  }

  @Test
  void keepsCurrentUnavailablePaimonDetailsForDiagnosis() {
    TableAnalysisKey key = analysisKey(SNAPSHOT_ID);
    runtimeMeta.setTableSummary(storedSummary(87, details(key, SNAPSHOT_ID)));
    DefaultTableRuntime runtime = runtimeWithHealth(-1, details(key, SNAPSHOT_ID));
    when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);

    controllerWithRuntime().getTableDetail(context);

    assertEquals(-1, serverTableMeta.getTableSummary().getHealthScore());
    assertEquals(
        Collections.singletonList("SNAPSHOT_SCAN_FAILED"),
        serverTableMeta.getTableSummary().getHealthDetails().getReasonCodes());
  }

  @Test
  void projectsRuntimePaimonHealthWhenPersistedSummaryIsMissing() {
    TableAnalysisKey key = analysisKey(SNAPSHOT_ID);
    runtimeMeta.setTableSummary(null);
    DefaultTableRuntime runtime = runtimeWithHealth(72, details(key, SNAPSHOT_ID));
    when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);

    controllerWithRuntime().getTableDetail(context);

    assertEquals(72, serverTableMeta.getTableSummary().getHealthScore());
    assertEquals(
        key.encoded(), serverTableMeta.getTableSummary().getHealthDetails().getEvaluationKey());
  }

  @Test
  void resolvesTableServiceAfterControllerConstruction() {
    TableAnalysisKey key = analysisKey(SNAPSHOT_ID);
    runtimeMeta.setTableSummary(storedSummary(31, details(key, SNAPSHOT_ID)));
    DefaultTableRuntime runtime = runtimeWithHealth(72, details(key, SNAPSHOT_ID));
    when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);
    AtomicReference<TableService> serviceReference = new AtomicReference<>();
    TableController controller =
        new TableController(
            catalogManager,
            tableManager,
            tableDescriptor,
            mock(Configurations.class),
            serviceReference::get);

    serviceReference.set(tableService);
    controller.getTableDetail(context);

    assertEquals(72, serverTableMeta.getTableSummary().getHealthScore());
    assertEquals(
        key.encoded(), serverTableMeta.getTableSummary().getHealthDetails().getEvaluationKey());

    TableService replacementService = mock(TableService.class);
    when(replacementService.getRuntime(TABLE_ID)).thenReturn(runtime);
    serviceReference.set(replacementService);
    controller.getTableDetail(context);

    assertEquals(72, serverTableMeta.getTableSummary().getHealthScore());
    verify(replacementService).getRuntime(TABLE_ID);
  }

  @Test
  void keepsRuntimePaimonSnapshotWhenCurrentSnapshotAndKeyAdvance() {
    TableAnalysisKey successfulKey = analysisKey(SNAPSHOT_ID);
    runtimeMeta.setTableSummary(
        storedSummary(-1, details(analysisKey(SNAPSHOT_ID - 1), SNAPSHOT_ID - 1)));
    DefaultTableRuntime runtime = runtimeWithHealth(72, details(successfulKey, SNAPSHOT_ID));
    when(runtime.getCurrentSnapshotId()).thenReturn(SNAPSHOT_ID + 1);
    when(runtime.getCurrentAnalysisKey()).thenReturn(Optional.of(analysisKey(SNAPSHOT_ID + 1)));
    when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);

    controllerWithRuntime().getTableDetail(context);

    assertEquals(72, serverTableMeta.getTableSummary().getHealthScore());
    assertEquals(
        successfulKey.encoded(),
        serverTableMeta.getTableSummary().getHealthDetails().getEvaluationKey());
  }

  @Test
  void doesNotRestorePersistedPaimonHealthWhenRuntimeOrRuntimeSnapshotIsMissing() {
    TableAnalysisKey key = analysisKey(SNAPSHOT_ID);
    runtimeMeta.setTableSummary(storedSummary(72, details(key, SNAPSHOT_ID)));
    when(tableService.getRuntime(TABLE_ID)).thenReturn(null);

    controllerWithRuntime().getTableDetail(context);

    assertEquals(-1, serverTableMeta.getTableSummary().getHealthScore());
    assertNull(serverTableMeta.getTableSummary().getHealthDetails());

    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    when(runtime.getRuntimeHealthSnapshot()).thenReturn(Optional.empty());
    when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);

    controllerWithRuntime().getTableDetail(context);

    assertEquals(-1, serverTableMeta.getTableSummary().getHealthScore());
    assertNull(serverTableMeta.getTableSummary().getHealthDetails());
  }

  @Test
  void doesNotApplyPaimonCurrentnessGateToIcebergOrLegacyController() {
    identifier.setFormat(TableFormat.ICEBERG);
    TableAnalysisKey key =
        new TableAnalysisKey(
            "17",
            TableFormat.ICEBERG,
            SNAPSHOT_ID,
            TableAnalysisKey.NO_CHANGE_SNAPSHOT,
            5L,
            "fingerprint",
            "iceberg-legacy-v1",
            TableAnalysisKey.NO_BASELINE,
            TableAnalysisKey.NO_BASELINE_TIME);
    runtimeMeta.setTableSummary(storedSummary(91, details(key, SNAPSHOT_ID)));

    controllerWithoutRuntime().getTableDetail(context);

    assertEquals(91, serverTableMeta.getTableSummary().getHealthScore());
    assertEquals(
        Long.toString(SNAPSHOT_ID),
        serverTableMeta.getTableSummary().getHealthDetails().getSnapshotId());
  }

  @Test
  void projectsFormulaFingerprintAndReasonCodes() {
    TableAnalysisKey key = analysisKey(SNAPSHOT_ID);
    TableHealthDetails source = details(key, SNAPSHOT_ID);

    TableHealthDetailsView view = TableHealthDetailsView.from(source);

    assertEquals(Collections.singletonList("SNAPSHOT_SCAN_FAILED"), view.getReasonCodes());
    assertEquals("paimon-primary-key-health-v2", view.getFormulaVersion());
    assertEquals("fingerprint", view.getScoringConfigFingerprint());
  }

  @Test
  void fileStatisticsKeepHealthAndLatestCommitIndependentAcrossRepeatedReads() throws Exception {
    serverTableMeta.setTableType("PAIMON");
    serverTableMeta.setDetailObservation(new TableDetailObservation(102L, 1));
    Map<String, Object> base = new LinkedHashMap<>();
    base.put("lastCommitTime", 123456L);
    base.put("baseWatermark", 120000L);
    serverTableMeta.setBaseMetrics(base);
    DefaultTableRuntime runtime = runtimeWithHealth(88, details(analysisKey(100), 100L));
    TableHealthEvaluation evaluation =
        new TableHealthEvaluation(
            "FAILED",
            true,
            "102",
            "102",
            2000,
            true,
            Collections.singletonList("SNAPSHOT_SCAN_FAILED"));
    when(runtime.getRuntimeHealthSnapshot())
        .thenReturn(
            Optional.of(
                new RuntimeHealthSnapshot(
                    88,
                    details(analysisKey(100), 100L),
                    evaluation,
                    new TableFileStatisticsSnapshot(101L, 1, 4, 80, 1000))));
    when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);
    TableController controller = controllerWithRuntime();
    for (int i = 0; i < 100; i++) {
      controller.getTableDetail(context);
    }
    assertEquals(Long.valueOf(4), base.get("fileCount"));
    assertEquals("80.00B", base.get("totalSize"));
    assertEquals("20.00B", base.get("averageFileSize"));
    assertEquals(123456L, base.get("lastCommitTime"));
    assertEquals(120000L, base.get("baseWatermark"));
    assertEquals(Long.valueOf(4), serverTableMeta.getTableSummary().getFile());
    assertEquals("80.00B", serverTableMeta.getTableSummary().getSize());
    assertEquals("20.00B", serverTableMeta.getTableSummary().getAverageFile());
    assertEquals(88, serverTableMeta.getTableSummary().getHealthScore());
    assertEquals("100", serverTableMeta.getTableSummary().getHealthDetails().getSnapshotId());
    assertEquals("101", serverTableMeta.getFileStatistics().getSnapshotId());
    assertEquals("102", serverTableMeta.getFileStatistics().getObservedSnapshotId());
    verify(runtime, times(100)).getRuntimeHealthSnapshot();
    verify(tableManager, times(100)).getTableRuntimeMata(identifier);
    verify(runtime, never()).takeTableAnalysis(any());
    org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode json =
        new ObjectMapper().valueToTree(serverTableMeta);
    assertFalse(json.get("fileStatistics").has("evaluation"));
    assertEquals(
        "102", json.at("/tableSummary/healthDetails/evaluation/attemptedSnapshotId").asText());
  }

  @Test
  void collectionFlagsMatchExistingOptimizingOrSummaryConfiguration() {
    serverTableMeta.setTableType("PAIMON");
    serverTableMeta.setDetailObservation(new TableDetailObservation(1L, 0));
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    when(runtime.getRuntimeHealthSnapshot()).thenReturn(Optional.empty());
    when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);
    Map<String, String> config = new LinkedHashMap<>();
    // Unrelated invalid numeric configuration must not affect this display-only projection.
    config.put(TableProperties.SELF_OPTIMIZING_MAX_FILE_CNT, "invalid");
    when(runtime.getTableConfig()).thenReturn(config);
    for (boolean optimize : new boolean[] {false, true}) {
      for (boolean summary : new boolean[] {false, true}) {
        config.put(TableProperties.ENABLE_SELF_OPTIMIZING, Boolean.toString(optimize));
        config.put(TableProperties.TABLE_SUMMARY_ENABLED, Boolean.toString(summary));
        controllerWithRuntime().getTableDetail(context);
        assertEquals(
            optimize || summary, serverTableMeta.getFileStatistics().getCollectionEnabled());
        assertEquals(
            optimize || summary ? "PENDING" : "UNAVAILABLE",
            serverTableMeta.getFileStatistics().getStatus().name());
        assertNull(serverTableMeta.getBaseMetrics().get("fileCount"));
      }
    }
  }

  @Test
  void detailResponseScenariosPreserveLegacyFieldsAndCanBeReplayedInBrowser() throws Exception {
    String[] scenarios = {
      "current", "stale", "partial", "failed", "unknown", "pending", "empty", "disabled", "iceberg"
    };
    String[] statuses = {
      "CURRENT", "STALE", "CURRENT", "STALE", "UNKNOWN", "PENDING", "CURRENT", "STALE", null
    };
    for (int i = 0; i < scenarios.length; i++) {
      setUp();
      String scenario = scenarios[i];
      boolean empty = "empty".equals(scenario);
      boolean pending = "pending".equals(scenario);
      boolean failed = "failed".equals(scenario) || "partial".equals(scenario);
      boolean legacy = "iceberg".equals(scenario);
      long observed =
          "current".equals(scenario) || "partial".equals(scenario)
              ? 101
              : "unknown".equals(scenario) ? 100 : 102;
      serverTableMeta.setBoundedSnapshotRead(true);
      serverTableMeta.setTableType(legacy ? "ICEBERG" : "PAIMON");
      serverTableMeta.setTableIdentifier(
          org.apache.amoro.table.TableIdentifier.of("catalog", "database", scenario));
      serverTableMeta.setComment("Local file statistics verification");
      serverTableMeta.setProperties(Collections.singletonMap("owner", "local-test"));
      serverTableMeta.setSchema(Collections.emptyList());
      serverTableMeta.setDetailObservation(new TableDetailObservation(empty ? null : observed, 1));
      serverTableMeta.setTableSummary(
          new TableSummary(3, "3.00KB", "1.00KB", 1200, legacy ? "iceberg" : "paimon"));
      Map<String, Object> base = new LinkedHashMap<>();
      base.put("lastCommitTime", empty ? null : 1790000600000L);
      base.put("baseWatermark", empty ? null : 1790000500000L);
      base.put("fileCount", 3L);
      base.put("totalSize", "3.00KB");
      base.put("averageFileSize", "1.00KB");
      serverTableMeta.setBaseMetrics(base);
      DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
      when(runtime.getTableConfig())
          .thenReturn(
              Collections.singletonMap(
                  TableProperties.ENABLE_SELF_OPTIMIZING,
                  Boolean.toString(!"disabled".equals(scenario))));
      Map<String, String> flags = new LinkedHashMap<>(runtime.getTableConfig());
      flags.put(
          TableProperties.TABLE_SUMMARY_ENABLED, Boolean.toString(!"disabled".equals(scenario)));
      when(runtime.getTableConfig()).thenReturn(flags);
      TableHealthEvaluation evaluation =
          new TableHealthEvaluation(
              failed ? "FAILED" : "SUCCESS",
              failed,
              Long.toString(observed),
              Long.toString(observed),
              1790000400000L,
              failed,
              failed
                  ? Collections.singletonList("DELETE_METADATA_READ_FAILED")
                  : Collections.emptyList());
      when(runtime.getRuntimeHealthSnapshot())
          .thenReturn(
              pending || empty
                  ? Optional.empty()
                  : Optional.of(
                      new RuntimeHealthSnapshot(
                          failed ? -1 : 90,
                          details(analysisKey(101), 101L),
                          evaluation,
                          new TableFileStatisticsSnapshot(
                              101L, 1, 100, 104857600, 1790000000000L))));
      when(tableService.getRuntime(TABLE_ID)).thenReturn(runtime);
      if (legacy) {
        identifier.setFormat(TableFormat.ICEBERG);
      }
      controllerWithRuntime().getTableDetail(context);
      if (legacy) {
        assertNull(serverTableMeta.getFileStatistics());
        assertEquals(3L, base.get("fileCount"));
      } else {
        assertEquals(statuses[i], serverTableMeta.getFileStatistics().getStatus().name());
        assertEquals(
            pending ? null : empty ? "0" : "100",
            serverTableMeta.getFileStatistics().getFileCount());
        assertEquals(serverTableMeta.getTableSummary().getFile(), base.get("fileCount"));
      }
      assertEquals(empty ? null : 1790000600000L, base.get("lastCommitTime"));
      assertEquals(1200, serverTableMeta.getTableSummary().getRecords());
      String evidence = System.getProperty("fileStatistics.evidenceDirectory");
      if (evidence != null) {
        java.nio.file.Path directory = java.nio.file.Paths.get(evidence);
        java.nio.file.Files.createDirectories(directory);
        new ObjectMapper()
            .writeValue(
                directory.resolve(scenario + ".json").toFile(),
                org.apache.amoro.server.dashboard.response.OkResponse.of(serverTableMeta));
      }
    }
  }

  private TableController controllerWithRuntime() {
    return new TableController(
        catalogManager, tableManager, tableDescriptor, mock(Configurations.class), tableService);
  }

  private TableController controllerWithoutRuntime() {
    return new TableController(
        catalogManager, tableManager, tableDescriptor, mock(Configurations.class));
  }

  private DefaultTableRuntime runtimeWithHealth(int healthScore, TableHealthDetails details) {
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    when(runtime.getRuntimeHealthSnapshot())
        .thenReturn(Optional.of(new RuntimeHealthSnapshot(healthScore, details)));
    return runtime;
  }

  private org.apache.amoro.table.TableSummary storedSummary(
      int healthScore, TableHealthDetails details) {
    org.apache.amoro.table.TableSummary summary = new org.apache.amoro.table.TableSummary();
    summary.setHealthScore(healthScore);
    summary.setHealthDetails(details);
    return summary;
  }

  private TableAnalysisKey analysisKey(long snapshotId) {
    return new TableAnalysisKey(
        "17",
        TableFormat.PAIMON,
        snapshotId,
        TableAnalysisKey.NO_CHANGE_SNAPSHOT,
        Long.MAX_VALUE - 2,
        "fingerprint",
        "paimon-primary-key-health-v2",
        TableAnalysisKey.NO_BASELINE,
        TableAnalysisKey.NO_BASELINE_TIME);
  }

  private TableHealthDetails details(TableAnalysisKey key, Long snapshotId) {
    Map<String, String> componentMetrics = new LinkedHashMap<>();
    componentMetrics.put("fileSize", "9223372036854775805");
    Map<String, String> metrics = new LinkedHashMap<>();
    metrics.put("totalFileSize", "9223372036854775806");
    return new TableHealthDetails(
        key.getFormulaVersion(),
        snapshotId,
        null,
        key.getSchemaId(),
        key.getScoringConfigFingerprint(),
        key.encoded(),
        Collections.singletonList(
            new TableHealthComponent("SORTED_RUN", 72, 100, "WEIGHTED", componentMetrics)),
        metrics,
        Collections.singletonList("SNAPSHOT_SCAN_FAILED"));
  }
}
