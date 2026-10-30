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

package org.apache.amoro.server.persistence.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionExecutorFactory;
import org.apache.amoro.formats.paimon.optimizing.PaimonMetricsSummary;
import org.apache.amoro.optimizing.IcebergRewriteExecutorFactory;
import org.apache.amoro.optimizing.MetricsSummary;
import org.apache.amoro.optimizing.TaskMetricsSummary;
import org.apache.ibatis.executor.result.ResultMapException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

class TestOptimizingTaskSummaryConverter {
  private final OptimizingTaskSummaryConverter history = new OptimizingTaskSummaryConverter();
  private static final String ICEBERG =
      "{\"rewriteDataFileCnt\":3,\"rewriteDataSize\":300,"
          + "\"rewriteDataRecordCnt\":30,\"newDataFileCnt\":1,\"newDataSize\":100,"
          + "\"newDataRecordCnt\":10,\"eqDeleteFileCnt\":2,\"eqDeleteRecordCnt\":4}";

  @Test
  void preservesIcebergMixedAndLegacyCounters() throws Exception {
    for (String factory :
        new String[] {
          null,
          "org.apache.amoro.optimizing.IcebergRewriteExecutorFactory",
          "org.apache.amoro.optimizing.MixedIcebergRewriteExecutorFactory",
          "org.apache.amoro.hive.optimizing.MixedHiveRewriteExecutorFactory"
        }) {
      ResultSet row = row(ICEBERG, factory);
      MetricsSummary expected =
          new JsonObjectConverter<>(MetricsSummary.class).getNullableResult(row, "metrics_summary");
      assertEquals(
          expected.summaryAsMap(false),
          history.getResult(row, "metrics_summary").summaryAsMap(false));
      assertEquals(expected.summaryAsMap(false), history.getResult(row, 1).summaryAsMap(false));
    }
  }

  @Test
  void unknownFactoryKeepsHistoryReadableButRecoveryStrict() throws Exception {
    ResultSet row = row(ICEBERG, "custom.RewriteFactory");
    assertEquals(30, history.getResult(row, "metrics_summary").getRewriteDataRecordCnt());
    assertEquals(10, history.getResult(row, 1).getNewDataRecordCnt());
    assertThrows(
        IllegalArgumentException.class,
        () -> new TaskDescriptorSummaryConverter().getResult(row, "metrics_summary"));
  }

  @Test
  void restoresBothPaimonFactoriesWithoutFormatSpecificCast() throws Exception {
    String json =
        "{\"compactedFileCount\":7,\"compactedFileSize\":700,\"compactedRecordCount\":70,"
            + "\"producedFileCount\":2,\"producedFileSize\":200,\"producedRecordCount\":20}";
    for (String factory :
        new String[] {
          "org.apache.amoro.formats.paimon.optimizing.PaimonCompactionExecutorFactory",
          "org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionExecutorFactory"
        }) {
      MetricsSummary value = history.getResult(row(json, factory), "metrics_summary");
      assertEquals(7, value.getRewriteDataFileCnt());
      assertEquals(700, value.getRewriteDataSize());
      assertEquals(70, value.getRewriteDataRecordCnt());
      assertEquals(2, value.getNewDataFileCnt());
      assertEquals(200, value.getNewDataSize());
      assertEquals(20, value.getNewDataRecordCnt());
    }
  }

  @Test
  void preservesEmptyAndMalformedJsonBehavior() throws Exception {
    for (String json : new String[] {null, "", " "}) {
      assertNull(history.getResult(row(json, "custom.RewriteFactory"), "metrics_summary"));
    }
    ResultMapException error =
        assertThrows(
            ResultMapException.class,
            () -> history.getResult(row("{invalid", "custom.RewriteFactory"), "metrics_summary"));
    assertTrue(error.getCause() instanceof SQLException);
  }

  @Test
  void missingOrMalformedPropertiesKeepLegacyHistoryReadable() throws Exception {
    for (String properties : new String[] {null, "", "{}", "{invalid"}) {
      ResultSet row = row(ICEBERG, null);
      when(row.getString("properties")).thenReturn(properties);
      assertEquals(30, history.getResult(row, "metrics_summary").getRewriteDataRecordCnt());
    }
  }

  @Test
  void goldenLegacyRowDeserializesToExpectedSummaryMap() throws Exception {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put(MetricsSummary.INPUT_DATA_FILES, "3");
    expected.put(MetricsSummary.INPUT_DATA_SIZE, "300");
    expected.put(MetricsSummary.INPUT_DATA_RECORDS, "30");
    expected.put(MetricsSummary.INPUT_EQ_DELETE_FILES, "2");
    expected.put(MetricsSummary.INPUT_EQ_DELETE_RECORDS, "4");
    expected.put(MetricsSummary.OUTPUT_DATA_FILES, "1");
    expected.put(MetricsSummary.OUTPUT_DATA_SIZE, "100");
    expected.put(MetricsSummary.OUTPUT_DATA_RECORDS, "10");

    assertEquals(
        expected, history.getResult(row(ICEBERG, null), "metrics_summary").summaryAsMap(false));
  }

  @Test
  void goldenPaimonRowWithUnknownFieldDeserializesToExpectedSummary() throws Exception {
    // PaimonMetricsSummary has no @JsonIgnoreProperties, so tolerating "futureField"
    // pins the shared ObjectMapper's disabled FAIL_ON_UNKNOWN_PROPERTIES feature.
    String json =
        "{\"compactedFileCount\":7,\"compactedFileSize\":700,\"compactedRecordCount\":70,"
            + "\"producedFileCount\":2,\"producedFileSize\":200,\"producedRecordCount\":20,"
            + "\"futureField\":{\"nested\":true}}";
    PaimonMetricsSummary expected = new PaimonMetricsSummary();
    expected.setCompactedFileCount(7);
    expected.setCompactedFileSize(700);
    expected.setCompactedRecordCount(70);
    expected.setProducedFileCount(2);
    expected.setProducedFileSize(200);
    expected.setProducedRecordCount(20);

    assertEquals(
        expected,
        new JsonObjectConverter<>(PaimonMetricsSummary.class)
            .getNullableResult(row(json, null), "metrics_summary"));

    MetricsSummary adapted =
        history.getResult(
            row(json, PaimonCompactionExecutorFactory.class.getName()), "metrics_summary");
    assertEquals(7, adapted.getRewriteDataFileCnt());
    assertEquals(700, adapted.getRewriteDataSize());
    assertEquals(70, adapted.getRewriteDataRecordCnt());
    assertEquals(2, adapted.getNewDataFileCnt());
    assertEquals(200, adapted.getNewDataSize());
    assertEquals(20, adapted.getNewDataRecordCnt());
  }

  @Test
  void reusesConverterInstancePerSummaryType() throws Exception {
    // summaryConverter is private, so it is invoked reflectively to pin instance
    // reuse without widening the collaborator's visibility. This failed before the
    // converter cache (a fresh JsonObjectConverter was built per row) and must stay
    // green now that converters are cached per resolved summary type.
    Method summaryConverter =
        OptimizingTaskSummaryConverter.class.getDeclaredMethod("summaryConverter", ResultSet.class);
    summaryConverter.setAccessible(true);

    String factory = IcebergRewriteExecutorFactory.class.getName();
    assertSame(
        summaryConverter.invoke(history, row(ICEBERG, factory)),
        summaryConverter.invoke(history, row(ICEBERG, factory)));

    // Legacy rows without a factory impl must reuse a cached instance too.
    assertSame(
        summaryConverter.invoke(history, row(ICEBERG, null)),
        summaryConverter.invoke(history, row(ICEBERG, null)));

    // A factory with a different summary type resolves to its own converter instance.
    assertNotSame(
        summaryConverter.invoke(history, row(ICEBERG, factory)),
        summaryConverter.invoke(
            history, row(ICEBERG, PaimonCompactionExecutorFactory.class.getName())));
  }

  @Test
  void unknownFactoryNamesReuseTheLegacySummaryConverter() throws Exception {
    Method summaryConverter =
        OptimizingTaskSummaryConverter.class.getDeclaredMethod("summaryConverter", ResultSet.class);
    summaryConverter.setAccessible(true);
    Object legacyConverter = summaryConverter.invoke(history, row(ICEBERG, null));

    for (int i = 0; i < 2_000; i++) {
      assertSame(
          legacyConverter,
          summaryConverter.invoke(history, row(ICEBERG, "custom.RewriteFactory" + i)));
    }
  }

  @Test
  void registeredFactoriesWithTheSameSummaryTypeShareAConverter() throws Exception {
    Method summaryConverter =
        OptimizingTaskSummaryConverter.class.getDeclaredMethod("summaryConverter", ResultSet.class);
    summaryConverter.setAccessible(true);
    Object legacyConverter = summaryConverter.invoke(history, row(ICEBERG, null));
    for (String factory :
        new String[] {
          "",
          IcebergRewriteExecutorFactory.class.getName(),
          "org.apache.amoro.optimizing.MixedIcebergRewriteExecutorFactory",
          "org.apache.amoro.hive.optimizing.MixedHiveRewriteExecutorFactory"
        }) {
      assertSame(legacyConverter, summaryConverter.invoke(history, row(ICEBERG, factory)));
    }

    assertSame(
        summaryConverter.invoke(
            history, row(ICEBERG, PaimonCompactionExecutorFactory.class.getName())),
        summaryConverter.invoke(
            history,
            row(
                ICEBERG,
                "org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionExecutorFactory")));
  }

  @Test
  void acceptsAnySummaryImplementingTheCommonContract() {
    TaskMetricsSummary custom =
        humanReadable -> {
          Map<String, String> values = new HashMap<>();
          values.put(TaskMetricsSummary.AGG_INPUT_DATA_FILES, "3");
          values.put(TaskMetricsSummary.AGG_INPUT_DATA_RECORDS, "30");
          values.put(TaskMetricsSummary.AGG_OUTPUT_DATA_RECORDS, "10");
          return values;
        };
    MetricsSummary adapted = OptimizingTaskSummaryConverter.adapt(custom);
    assertEquals(3, adapted.getRewriteDataFileCnt());
    assertEquals(30, adapted.getRewriteDataRecordCnt());
    assertEquals(10, adapted.getNewDataRecordCnt());
  }

  private static ResultSet row(String json, String factory) throws Exception {
    ResultSet row = mock(ResultSet.class);
    when(row.getString("metrics_summary")).thenReturn(json);
    when(row.getString(1)).thenReturn(json);
    when(row.getString("properties"))
        .thenReturn(
            factory == null ? "{}" : "{\"task-executor-factory-impl\":\"" + factory + "\"}");
    return row;
  }
}
