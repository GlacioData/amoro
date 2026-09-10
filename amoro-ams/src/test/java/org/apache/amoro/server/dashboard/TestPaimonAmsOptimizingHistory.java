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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.formats.paimon.optimizing.PaimonMetricsSummary;
import org.apache.amoro.optimizing.MetricsSummary;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.server.optimizing.OptimizingTaskMeta;
import org.apache.amoro.server.optimizing.TaskRuntime;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.server.persistence.converter.TaskDescriptorSummaryConverter;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.table.DerbyPersistence;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;

public class TestPaimonAmsOptimizingHistory {
  private static final long PROCESS_ID = 202L;
  private static final TableIdentifier TABLE_IDENTIFIER =
      TableIdentifier.of("catalog", "database", "table");

  @Rule public DerbyPersistence derbyPersistence = new DerbyPersistence();

  @Test
  public void testPaimonOptimizingPageUsesAmsHistoryAndLazyLoadsError() {
    String error = "Paimon optimizing failed";
    TestPersistence persistence = new TestPersistence();
    persistence.insertTableAndFailedProcess(error);
    TableProcessMeta persistedProcess = persistence.getProcess();
    Assert.assertNotNull(persistedProcess);
    Assert.assertEquals(error, persistedProcess.getFailMessage());
    Assert.assertEquals(1, persistence.listProcesses().size());

    AmoroTable<?> table = mock(AmoroTable.class);
    when(table.id()).thenReturn(TABLE_IDENTIFIER);
    when(table.format()).thenReturn(TableFormat.PAIMON);
    ServerCatalog catalog = mock(ServerCatalog.class);
    doReturn(table).when(catalog).loadTable("database", "table");
    CatalogManager catalogManager = mock(CatalogManager.class);
    when(catalogManager.getServerCatalog("catalog")).thenReturn(catalog);
    ServerTableDescriptor descriptor =
        new ServerTableDescriptor(
            catalogManager, mock(TableManager.class), mock(Configurations.class));

    Pair<List<OptimizingProcessInfo>, Integer> listWithoutError =
        descriptor.getOptimizingProcessesInfo(
            TABLE_IDENTIFIER.buildTableIdentifier(), null, null, 20, 0, null, false);
    Assert.assertEquals(1, listWithoutError.getLeft().size());
    Assert.assertNull(listWithoutError.getLeft().get(0).getFailReason());

    Pair<List<OptimizingProcessInfo>, Integer> listWithError =
        descriptor.getOptimizingProcessesInfo(
            TABLE_IDENTIFIER.buildTableIdentifier(), null, null, 20, 0, null, true);
    Assert.assertEquals(error, listWithError.getLeft().get(0).getFailReason());
  }

  @Test
  public void testTaskHistoryIsPagedInDatabaseAndDoesNotLoadFailureText() throws Exception {
    TestPersistence persistence = new TestPersistence();
    persistence.insertTableAndFailedProcess("process failure");
    persistence.insertFailedTasks("task failure");

    AmoroTable<?> table = mock(AmoroTable.class);
    when(table.id()).thenReturn(TABLE_IDENTIFIER);
    when(table.format()).thenReturn(TableFormat.PAIMON);
    ServerCatalog catalog = mock(ServerCatalog.class);
    doReturn(table).when(catalog).loadTable("database", "table");
    CatalogManager catalogManager = mock(CatalogManager.class);
    when(catalogManager.getServerCatalog("catalog")).thenReturn(catalog);
    ServerTableDescriptor descriptor =
        new ServerTableDescriptor(
            catalogManager, mock(TableManager.class), mock(Configurations.class));

    Pair<List<OptimizingTaskInfo>, Integer> page =
        descriptor.getOptimizingProcessTaskInfos(
            TABLE_IDENTIFIER.buildTableIdentifier(), Long.toString(PROCESS_ID), 1, 1, false);

    Assert.assertEquals(Integer.valueOf(3), page.getRight());
    Assert.assertEquals(1, page.getLeft().size());
    Assert.assertEquals(2, page.getLeft().get(0).getTaskId());
    Assert.assertNull(page.getLeft().get(0).getFailReason());
  }

  @Test
  public void testNonZeroPaimonSummarySurvivesTaskHistoryRead() throws Exception {
    TestPersistence persistence = new TestPersistence();
    persistence.insertTableAndFailedProcess("test");
    persistence.insertFailedTasks("test");
    try (SqlSession session = SqlSessionFactoryProvider.getInstance().get().openSession(true);
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "UPDATE task_runtime SET status = 'SUCCESS', metrics_summary = ?, properties = ? WHERE process_id = ?")) {
      PaimonMetricsSummary collected = new PaimonMetricsSummary(7, 700, 2, 200);
      collected.setCompactedRecordCount(70);
      collected.setProducedRecordCount(20);
      new TaskDescriptorSummaryConverter().setParameter(statement, 1, collected, null);
      statement.setString(
          2,
          "{\"task-executor-factory-impl\":\"org.apache.amoro.formats.paimon.optimizing.PaimonCompactionExecutorFactory\"}");
      statement.setLong(3, PROCESS_ID);
      statement.executeUpdate();
      OptimizingTaskMeta task =
          session
              .getMapper(OptimizingProcessMapper.class)
              .selectOptimizeTaskMeta(persistence.tableId, PROCESS_ID, 1);
      Assert.assertEquals(7, task.getMetricsSummary().getRewriteDataFileCnt());
      Assert.assertEquals(700, task.getMetricsSummary().getRewriteDataSize());
      Assert.assertEquals(2, task.getMetricsSummary().getNewDataFileCnt());
      Assert.assertEquals(200, task.getMetricsSummary().getNewDataSize());
      Assert.assertEquals(70, task.getMetricsSummary().getRewriteDataRecordCnt());
      Assert.assertEquals(20, task.getMetricsSummary().getNewDataRecordCnt());
      MetricsSummary restored =
          MetricsSummary.fromMap(
              MetricsSummary.aggregate(Collections.singletonList(collected.toMetricsSummary()))
                  .summaryAsMap(false));
      Assert.assertEquals(70, restored.getRewriteDataRecordCnt());
      Assert.assertEquals(20, restored.getNewDataRecordCnt());
      Assert.assertEquals(6, restored.summaryAsMap(false).size());
    }
  }

  private static class TestPersistence extends PersistentBase {
    private long tableId;

    private void insertTableAndFailedProcess(String error) {
      ServerTableIdentifier identifier =
          ServerTableIdentifier.of("catalog", "database", "table", TableFormat.PAIMON);
      doAs(TableMetaMapper.class, mapper -> mapper.insertTable(identifier));
      tableId = identifier.getId();
      MetricsSummary metricsSummary = new MetricsSummary();
      doAs(
          TableProcessMapper.class,
          mapper -> {
            mapper.insertProcess(
                tableId,
                PROCESS_ID,
                "application-test",
                ProcessStatus.RUNNING,
                "MAJOR",
                "EXECUTING",
                "SPARK",
                0,
                1L,
                Collections.emptyMap(),
                metricsSummary.summaryAsMap(false));
            mapper.updateProcess(
                tableId,
                PROCESS_ID,
                "application-test",
                ProcessStatus.FAILED,
                "EXECUTING",
                0,
                2L,
                error,
                Collections.emptyMap(),
                metricsSummary.summaryAsMap(false));
          });
    }

    private TableProcessMeta getProcess() {
      return getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(PROCESS_ID));
    }

    private List<TableProcessMeta> listProcesses() {
      return getAs(
          TableProcessMapper.class,
          mapper -> mapper.listProcessMetaWithoutFailMessage(tableId, null, null));
    }

    private void insertFailedTasks(String error) throws Exception {
      try (SqlSession session = SqlSessionFactoryProvider.getInstance().get().openSession(true);
          PreparedStatement statement =
              session
                  .getConnection()
                  .prepareStatement(
                      "INSERT INTO task_runtime "
                          + "(process_id, task_id, retry_num, table_id, partition_data, status, "
                          + "fail_reason, metrics_summary, properties) "
                          + "VALUES (?, ?, 1, ?, ?, ?, ?, '{}', '{}')")) {
        for (int taskId = 1; taskId <= 3; taskId++) {
          statement.setLong(1, PROCESS_ID);
          statement.setInt(2, taskId);
          statement.setLong(3, tableId);
          statement.setString(4, "dt=2026-09-0" + taskId);
          statement.setString(5, TaskRuntime.Status.FAILED.name());
          statement.setString(6, error + taskId);
          statement.addBatch();
        }
        statement.executeBatch();
      }
    }
  }
}
