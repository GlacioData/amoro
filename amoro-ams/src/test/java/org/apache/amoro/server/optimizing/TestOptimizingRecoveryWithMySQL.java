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

package org.apache.amoro.server.optimizing;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.OptimizingTaskId;
import org.apache.amoro.api.OptimizingTaskResult;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionExecutorFactory;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionTask;
import org.apache.amoro.optimizing.BaseOptimizingInput;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.RewriteFilesInput;
import org.apache.amoro.optimizing.RewriteFilesOutput;
import org.apache.amoro.optimizing.TableOptimizingCommitter;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.optimizing.TaskProperties;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.StagedTaskDescriptor;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.persistence.DataSourceFactory;
import org.apache.amoro.server.persistence.OptimizingPayloadCodec;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.TaskFilesPersistence;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.process.iceberg.IcebergProcessFactory;
import org.apache.amoro.server.resource.OptimizerThread;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.DefaultTableRuntimeStore;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Testcontainers
class TestOptimizingRecoveryWithMySQL {

  @Container
  private static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>("mysql:8.0")
          .withDatabaseName("amoro")
          .withUsername("amoro")
          .withPassword("amoro");

  private static final Gson GSON = new Gson();
  private static final Persistence PERSISTENCE = new Persistence();
  private static DataSource dataSource;

  @BeforeAll
  static void initializeMySQLPersistence() throws Exception {
    Configurations configurations = new Configurations();
    configurations.set(AmoroManagementConf.DB_TYPE, AmoroManagementConf.DB_TYPE_MYSQL);
    configurations.set(AmoroManagementConf.DB_CONNECTION_URL, MYSQL.getJdbcUrl());
    configurations.set(AmoroManagementConf.DB_DRIVER_CLASS_NAME, MYSQL.getDriverClassName());
    configurations.set(AmoroManagementConf.DB_USER_NAME, MYSQL.getUsername());
    configurations.set(AmoroManagementConf.DB_PASSWORD, MYSQL.getPassword());
    configurations.set(AmoroManagementConf.DB_AUTO_CREATE_TABLES, true);
    dataSource = DataSourceFactory.createDataSource(configurations);
    SqlSessionFactoryProvider.getInstance().init(dataSource);
  }

  @AfterAll
  static void closeDataSource() throws Exception {
    if (dataSource instanceof AutoCloseable) {
      ((AutoCloseable) dataSource).close();
    }
  }

  @BeforeEach
  void clearFixtureRows() throws Exception {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("DELETE FROM optimizing_task_quota");
      statement.executeUpdate("DELETE FROM task_runtime");
      statement.executeUpdate("DELETE FROM optimizing_process_state");
      statement.executeUpdate("DELETE FROM table_process");
      statement.executeUpdate("DELETE FROM table_runtime_state");
      statement.executeUpdate("DELETE FROM table_runtime");
      connection.commit();
    }
  }

  @Test
  void readsRawAndGzipInputsAndOutputsFromMySqlLongBlob() throws Exception {
    long tableId = 101L;
    RewriteFilesInput input = icebergInput();
    Map<Integer, BaseOptimizingInput> inputs = Collections.singletonMap(1, input);

    PERSISTENCE.insertProcessState(tableId, 1001L);
    PERSISTENCE.writeInput(1001L, inputs);
    assertGzipMagic("optimizing_process_state", "rewrite_input", "process_id", 1001L);
    RewriteFilesInput gzipInput =
        (RewriteFilesInput) TaskFilesPersistence.loadTaskInputs(1001L).get(1);
    assertEquals(0, gzipInput.rewrittenDataFiles().length);

    PERSISTENCE.insertProcessState(tableId, 1002L);
    updateBlob(
        "optimizing_process_state",
        "rewrite_input",
        "process_id",
        1002L,
        SerializationUtil.simpleSerialize(inputs).array());
    RewriteFilesInput rawInput =
        (RewriteFilesInput) TaskFilesPersistence.loadTaskInputs(1002L).get(1);
    assertEquals(0, rawInput.rewrittenDeleteFiles().length);

    RewriteFilesOutput output =
        new RewriteFilesOutput(
            new DataFile[0], new org.apache.iceberg.DeleteFile[0], new HashMap<>());
    insertTask(
        tableId, 1003L, 1, TaskRuntime.Status.SUCCESS, OptimizingPayloadCodec.encode(output), null);
    insertTask(
        tableId,
        1003L,
        2,
        TaskRuntime.Status.SUCCESS,
        SerializationUtil.simpleSerialize(output).array(),
        null);

    List<TaskRuntime<? extends StagedTaskDescriptor<? extends BaseOptimizingInput, ?, ?>>> tasks =
        PERSISTENCE.taskRuntimes(tableId, 1003L);
    assertEquals(2, tasks.size());
    RewriteFilesOutput gzipOutput =
        (RewriteFilesOutput) tasks.get(0).getTaskDescriptor().getOutput();
    RewriteFilesOutput rawOutput =
        (RewriteFilesOutput) tasks.get(1).getTaskDescriptor().getOutput();
    assertArrayEquals(new DataFile[0], gzipOutput.getDataFiles());
    assertArrayEquals(new DataFile[0], rawOutput.getDataFiles());
  }

  @Test
  void storesCompleteUnicodeFailureMessagesInMySqlLongText() throws Exception {
    long tableId = 151L;
    long processId = 1501L;
    String error =
        "java.lang.IllegalStateException: 顶层异常-🚀\n"
            + repeat("at 测试.完整错误上下文(PaimonCompaction.java:319)\n", 10_000)
            + "Caused by: snapshot-319 不可达，完整尾部-😃";

    PERSISTENCE.insertProcess(tableId, processId);
    PERSISTENCE.failProcess(tableId, processId, error);
    insertTask(tableId, processId, 1, TaskRuntime.Status.PLANNED, null, null);
    Map<String, String> taskProperties = new HashMap<>();
    taskProperties.put(
        TaskProperties.TASK_EXECUTOR_FACTORY_IMPL, PaimonCompactionExecutorFactory.class.getName());
    TaskRuntime<PaimonCompactionTask> taskRuntime =
        new TaskRuntime<>(
            new OptimizingTaskId(processId, 1),
            new PaimonCompactionTask(tableId, "dt=2026-09-03", null, taskProperties));
    OptimizerThread optimizerThread =
        new OptimizerThread(29, null) {
          @Override
          public String getToken() {
            return "optimizer-token";
          }
        };
    taskRuntime.schedule(optimizerThread);
    taskRuntime.ack(optimizerThread);
    taskRuntime.complete(
        optimizerThread,
        new OptimizingTaskResult(new OptimizingTaskId(processId, 1), 29).setErrorMessage(error));

    assertTrue(error.length() > 4_096);
    assertEquals(error, PERSISTENCE.process(processId).getFailMessage());
    assertEquals(error, PERSISTENCE.taskMetas(processId).get(0).getFailReason());
  }

  @Test
  void corruptPaimonInputAtomicallyClosesProcessAndNeverBuildsCommitter() throws Exception {
    long tableId = 201L;
    long processId = 2001L;
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    DefaultTableRuntime runtime = runtime(tableId, processId, TableFormat.PAIMON, false, 0L);
    insertProcess(runtime, processId, new byte[] {0x1f, (byte) 0x8b, 0x00});
    insertTask(tableId, processId, 1, TaskRuntime.Status.ACKED, null, paimonTaskProperties());
    insertTask(tableId, processId, 2, TaskRuntime.Status.SUCCESS, null, paimonTaskProperties());

    OptimizingQueue queue = queue(runtime, catalogManager, router(TableFormat.PAIMON), null);
    try {
      TableProcessMeta process = PERSISTENCE.process(processId);
      List<OptimizingTaskMeta> tasks = PERSISTENCE.taskMetas(processId);
      assertEquals(ProcessStatus.CLOSED, process.getStatus());
      assertNotNull(process.getFailMessage());
      assertTrue(process.getFailMessage().contains("CORRUPT_GZIP"));
      assertTrue(process.getFinishTime() > 0L);
      assertEquals(TaskRuntime.Status.CANCELED, tasks.get(0).getStatus());
      assertEquals(TaskRuntime.Status.SUCCESS, tasks.get(1).getStatus());
      assertEquals(0L, runtime.getProcessId());
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
      assertNull(runtime.getOptimizingProcess());
      Mockito.verify(catalogManager, Mockito.never()).loadTable(Mockito.any());
    } finally {
      queue.dispose();
    }
  }

  @Test
  void corruptOutputUsesPayloadFreeRowsToCloseProcess() throws Exception {
    long tableId = 301L;
    long processId = 3001L;
    DefaultTableRuntime runtime = runtime(tableId, processId, TableFormat.ICEBERG, false, 0L);
    insertProcess(
        runtime,
        processId,
        OptimizingPayloadCodec.encode(Collections.singletonMap(1, icebergInput())));
    insertTask(
        tableId,
        processId,
        1,
        TaskRuntime.Status.SUCCESS,
        new byte[] {0x1f, (byte) 0x8b, 0x00},
        null);

    OptimizingQueue queue =
        queue(runtime, Mockito.mock(CatalogManager.class), router(TableFormat.ICEBERG), null);
    try {
      assertEquals(ProcessStatus.CLOSED, PERSISTENCE.process(processId).getStatus());
      assertEquals(TaskRuntime.Status.SUCCESS, PERSISTENCE.taskMetas(processId).get(0).getStatus());
      assertEquals(0L, runtime.getProcessId());
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    } finally {
      queue.dispose();
    }
  }

  @Test
  void strictOwnerCasFailureRollsBackProcessAndTaskChanges() throws Exception {
    long tableId = 401L;
    long processId = 4001L;
    long competingProcessId = 4999L;
    DefaultTableRuntime runtime =
        runtime(tableId, processId, TableFormat.ICEBERG, true, competingProcessId);
    insertProcess(runtime, processId, new byte[] {0x1f, (byte) 0x8b, 0x00});
    insertTask(tableId, processId, 1, TaskRuntime.Status.ACKED, null, null);

    OptimizingQueue queue =
        queue(runtime, Mockito.mock(CatalogManager.class), router(TableFormat.ICEBERG), null);
    try {
      assertEquals(ProcessStatus.RUNNING, PERSISTENCE.process(processId).getStatus());
      assertEquals(TaskRuntime.Status.ACKED, PERSISTENCE.taskMetas(processId).get(0).getStatus());
      assertEquals(competingProcessId, runtime.getProcessId());
      assertEquals(OptimizingStatus.MAJOR_OPTIMIZING, runtime.getOptimizingStatus());
      assertNull(runtime.getOptimizingProcess());
      assertEquals(0, queue.pendingRecoveryCount());
    } finally {
      queue.dispose();
    }
  }

  @Test
  void transientFailureLeavesProcessUntouchedAndPeriodicRetryRecoversIt() throws Exception {
    long tableId = 501L;
    long processId = 5001L;
    DefaultTableRuntime runtime = runtime(tableId, processId, TableFormat.ICEBERG, false, 0L);
    insertProcess(
        runtime,
        processId,
        OptimizingPayloadCodec.encode(Collections.singletonMap(1, icebergInput())));
    insertTask(tableId, processId, 1, TaskRuntime.Status.PLANNED, null, null);
    AtomicInteger ownershipChecks = new AtomicInteger();

    OptimizingQueue queue =
        queue(
            runtime,
            Mockito.mock(CatalogManager.class),
            router(TableFormat.ICEBERG),
            ignored -> {
              if (ownershipChecks.getAndIncrement() == 0) {
                throw new PersistenceException(
                    "temporary SQL failure", new SQLTransientException("lock wait timeout"));
              }
              return OptimizingOwnership.OWNED;
            });
    try {
      assertEquals(1, queue.pendingRecoveryCount());
      assertEquals(ProcessStatus.RUNNING, PERSISTENCE.process(processId).getStatus());
      assertEquals(TaskRuntime.Status.PLANNED, PERSISTENCE.taskMetas(processId).get(0).getStatus());
      assertNull(runtime.getOptimizingProcess());

      queue.retryPendingRecoveries(Long.MAX_VALUE);

      assertEquals(0, queue.pendingRecoveryCount());
      assertNotNull(runtime.getOptimizingProcess());
      assertEquals(processId, runtime.getOptimizingProcess().getProcessId());
      assertEquals(ProcessStatus.RUNNING, PERSISTENCE.process(processId).getStatus());
    } finally {
      queue.dispose();
    }
  }

  @Test
  void processOwnerChangeDiscardsPendingRecovery() throws Exception {
    long tableId = 601L;
    long processId = 6001L;
    long newProcessId = 6002L;
    DefaultTableRuntime runtime = runtime(tableId, processId, TableFormat.ICEBERG, false, 0L);
    insertProcess(
        runtime,
        processId,
        OptimizingPayloadCodec.encode(Collections.singletonMap(1, icebergInput())));
    insertTask(tableId, processId, 1, TaskRuntime.Status.PLANNED, null, null);

    OptimizingQueue queue =
        queue(
            runtime,
            Mockito.mock(CatalogManager.class),
            router(TableFormat.ICEBERG),
            ignored -> {
              throw new PersistenceException(
                  "temporary SQL failure", new SQLTransientException("connection reset"));
            });
    try {
      assertEquals(1, queue.pendingRecoveryCount());
      replaceOwner(tableId, newProcessId);

      queue.retryPendingRecoveries(Long.MAX_VALUE);

      assertEquals(0, queue.pendingRecoveryCount());
      assertEquals(ProcessStatus.RUNNING, PERSISTENCE.process(processId).getStatus());
      assertEquals(newProcessId, runtime.getProcessId());
      assertNull(runtime.getOptimizingProcess());
    } finally {
      queue.dispose();
    }
  }

  @Test
  void committingPaimonProcessReplaysWithPersistedIdentityAfterRestart() throws Exception {
    long tableId = 701L;
    long processId = 7001L;
    String commitUser = "persisted-recovery-user";
    DefaultTableRuntime runtime = runtime(tableId, processId, TableFormat.PAIMON, false, 0L);
    runtime
        .store()
        .begin()
        .updateStatusCode(code -> OptimizingStatus.COMMITTING.getCode())
        .commit();
    PaimonCompactionInput input =
        new PaimonCompactionInput(
            null, new byte[] {1, 2, 3}, 1, commitUser, "dt=2026-08-27", 10L, processId);
    insertProcess(
        runtime, processId, OptimizingPayloadCodec.encode(Collections.singletonMap(1, input)));
    insertTask(tableId, processId, 1, TaskRuntime.Status.SUCCESS, null, paimonTaskProperties());

    RecordingCommitter committer = new RecordingCommitter();
    ProcessFactory factory = Mockito.mock(ProcessFactory.class);
    Mockito.when(factory.supportedFormats()).thenReturn(Collections.singleton(TableFormat.PAIMON));
    Mockito.when(
            factory.createCommitter(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyCollection(),
                Mockito.anyMap(),
                Mockito.anyMap()))
        .thenAnswer(
            invocation -> {
              Collection<?> tasks = invocation.getArgument(3);
              PaimonCompactionTask task = (PaimonCompactionTask) tasks.iterator().next();
              committer.commitUser = task.getInput().getCommitUser();
              committer.commitIdentifier = task.getInput().getCommitIdentifier();
              return committer;
            });
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    Mockito.when(catalogManager.loadTable(Mockito.any()))
        .thenReturn(Mockito.mock(AmoroTable.class));

    OptimizingQueue queue =
        queue(
            runtime,
            catalogManager,
            new ProcessFactoryRouter(Collections.singletonList(factory)),
            null);
    try {
      OptimizingProcess recovered = runtime.getOptimizingProcess();
      assertNotNull(recovered);

      recovered.commit();

      assertEquals(CommitMode.RECOVERY_REPLAY, committer.commitMode);
      assertEquals(0, committer.normalCommitCount);
      assertEquals(commitUser, committer.commitUser);
      assertEquals(processId, committer.commitIdentifier);
      assertEquals(ProcessStatus.SUCCESS, PERSISTENCE.process(processId).getStatus());
      assertEquals(0L, runtime.getProcessId());
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    } finally {
      queue.dispose();
    }
  }

  private static OptimizingQueue queue(
      DefaultTableRuntime runtime,
      CatalogManager catalogManager,
      ProcessFactoryRouter router,
      java.util.function.Function<DefaultTableRuntime, OptimizingOwnership> ownershipGuard) {
    ResourceGroup group =
        new ResourceGroup.Builder("mysql-recovery-" + runtime.getTableIdentifier().getId(), "local")
            .build();
    if (ownershipGuard == null) {
      return new OptimizingQueue(
          catalogManager,
          group,
          ignored -> 1,
          Runnable::run,
          Collections.singletonList(runtime),
          1,
          router);
    }
    return new OptimizingQueue(
        catalogManager,
        group,
        ignored -> 1,
        Runnable::run,
        Collections.singletonList(runtime),
        1,
        router,
        ownershipGuard);
  }

  private static ProcessFactoryRouter router(TableFormat format) {
    if (format == TableFormat.ICEBERG) {
      return new ProcessFactoryRouter(Collections.singletonList(new IcebergProcessFactory()));
    }
    ProcessFactory factory = Mockito.mock(ProcessFactory.class);
    Mockito.when(factory.supportedFormats()).thenReturn(Collections.singleton(format));
    return new ProcessFactoryRouter(Collections.singletonList(factory));
  }

  private static DefaultTableRuntime runtime(
      long tableId,
      long processId,
      TableFormat format,
      boolean loseOwnerOnStrictCompletion,
      long competingProcessId) {
    ServerTableIdentifier identifier =
        ServerTableIdentifier.of("mysql", "db", "table_" + tableId, format);
    identifier.setId(tableId);
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(tableId);
    meta.setGroupName("mysql-recovery-" + tableId);
    meta.setStatusCode(OptimizingStatus.MAJOR_OPTIMIZING.getCode());
    PERSISTENCE.insertRuntime(meta);

    List<StateKey<?>> requiredStates = new ArrayList<>(DefaultTableRuntime.REQUIRED_STATES);
    requiredStates.add(DefaultTableRuntime.DEFAULT_PENDING_INPUT_KEY);
    DefaultTableRuntimeStore store =
        new DefaultTableRuntimeStore(identifier, meta, requiredStates, Collections.emptyList());
    DefaultTableRuntime runtime =
        loseOwnerOnStrictCompletion
            ? new CasLosingTableRuntime(store, tableId, competingProcessId)
            : new DefaultTableRuntime(store, () -> null);
    store.setTableRuntime(runtime);
    assertTrue(runtime.tryAcquireProcessOwner(processId));
    return runtime;
  }

  private static void insertProcess(
      DefaultTableRuntime runtime, long processId, byte[] rewriteInput) throws Exception {
    long tableId = runtime.getTableIdentifier().getId();
    PERSISTENCE.insertProcess(tableId, processId);
    PERSISTENCE.insertProcessState(tableId, processId);
    updateBlob("optimizing_process_state", "rewrite_input", "process_id", processId, rewriteInput);
  }

  private static RewriteFilesInput icebergInput() {
    return new RewriteFilesInput(
        new DataFile[0], new DataFile[0], new ContentFile<?>[0], new ContentFile<?>[0], null);
  }

  private static String paimonTaskProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        TaskProperties.TASK_EXECUTOR_FACTORY_IMPL, PaimonCompactionExecutorFactory.class.getName());
    return GSON.toJson(properties);
  }

  private static void insertTask(
      long tableId,
      long processId,
      int taskId,
      TaskRuntime.Status status,
      byte[] output,
      String properties)
      throws Exception {
    String sql =
        "INSERT INTO task_runtime "
            + "(process_id, task_id, retry_num, table_id, partition_data, start_time, end_time, "
            + "status, optimizer_token, thread_id, rewrite_output, metrics_summary, properties) "
            + "VALUES (?, ?, 0, ?, 'dt=2026-08-27', CURRENT_TIMESTAMP, "
            + "CASE WHEN ? IN ('SUCCESS', 'FAILED', 'CANCELED') THEN CURRENT_TIMESTAMP ELSE NULL END, "
            + "?, 'optimizer-token', 29, ?, '{}', ?)";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, processId);
      statement.setInt(2, taskId);
      statement.setLong(3, tableId);
      statement.setString(4, status.name());
      statement.setString(5, status.name());
      statement.setBytes(6, output);
      statement.setString(7, properties == null ? "{}" : properties);
      statement.executeUpdate();
      connection.commit();
    }
  }

  private static void updateBlob(
      String table, String column, String idColumn, long id, byte[] content) throws Exception {
    String sql = "UPDATE " + table + " SET " + column + " = ? WHERE " + idColumn + " = ?";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setBytes(1, content);
      statement.setLong(2, id);
      statement.executeUpdate();
      connection.commit();
    }
  }

  private static String repeat(String value, int count) {
    StringBuilder builder = new StringBuilder(value.length() * count);
    for (int i = 0; i < count; i++) {
      builder.append(value);
    }
    return builder.toString();
  }

  private static void assertGzipMagic(String table, String column, String idColumn, long id)
      throws Exception {
    String sql = "SELECT " + column + " FROM " + table + " WHERE " + idColumn + " = ?";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, id);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        byte[] bytes = resultSet.getBytes(1);
        assertEquals(0x1f, bytes[0] & 0xff);
        assertEquals(0x8b, bytes[1] & 0xff);
      }
    }
  }

  private static void replaceOwner(long tableId, long processId) throws Exception {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "UPDATE table_runtime_state SET state_value = ?, state_version = state_version + 1 "
                    + "WHERE table_id = ? AND state_key = 'process_id'")) {
      statement.setString(1, Long.toString(processId));
      statement.setLong(2, tableId);
      assertEquals(1, statement.executeUpdate());
      connection.commit();
    }
  }

  private static final class CasLosingTableRuntime extends DefaultTableRuntime {
    private final long tableId;
    private final long competingProcessId;
    private boolean ownerReplaced;

    private CasLosingTableRuntime(
        DefaultTableRuntimeStore store, long tableId, long competingProcessId) {
      super(store, () -> null);
      this.tableId = tableId;
      this.competingProcessId = competingProcessId;
    }

    @Override
    public void completeProcessStrict(OptimizingProcess process, boolean success) {
      if (!ownerReplaced) {
        ownerReplaced = true;
        try {
          replaceOwner(tableId, competingProcessId);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      super.completeProcessStrict(process, success);
    }
  }

  private static final class RecordingCommitter implements TableOptimizingCommitter {
    private CommitMode commitMode;
    private int normalCommitCount;
    private String commitUser;
    private long commitIdentifier;

    @Override
    public void commit() {
      normalCommitCount++;
    }

    @Override
    public void commit(CommitMode mode) {
      commitMode = mode;
    }
  }

  private static final class Persistence extends PersistentBase {
    private void insertRuntime(TableRuntimeMeta meta) {
      doAs(TableRuntimeMapper.class, mapper -> mapper.insertRuntime(meta));
    }

    private void insertProcess(long tableId, long processId) {
      doAs(
          TableProcessMapper.class,
          mapper ->
              mapper.insertProcess(
                  tableId,
                  processId,
                  "",
                  ProcessStatus.RUNNING,
                  OptimizingType.MAJOR.name(),
                  "planning",
                  "AMORO",
                  0,
                  System.currentTimeMillis(),
                  new HashMap<>(),
                  new HashMap<>()));
    }

    private void failProcess(long tableId, long processId, String error) {
      doAs(
          TableProcessMapper.class,
          mapper ->
              mapper.updateProcess(
                  tableId,
                  processId,
                  "",
                  ProcessStatus.FAILED,
                  "failed",
                  0,
                  System.currentTimeMillis(),
                  error,
                  new HashMap<>(),
                  new HashMap<>()));
    }

    private void insertProcessState(long tableId, long processId) {
      doAs(
          OptimizingProcessMapper.class,
          mapper ->
              mapper.insertInternalProcessState(
                  tableId, processId, 10L, 0L, Collections.emptyMap(), Collections.emptyMap()));
    }

    private void writeInput(long processId, Map<Integer, BaseOptimizingInput> input) {
      doAs(
          OptimizingProcessMapper.class,
          mapper -> mapper.updateProcessInputFiles(processId, input));
    }

    private List<TaskRuntime<? extends StagedTaskDescriptor<? extends BaseOptimizingInput, ?, ?>>>
        taskRuntimes(long tableId, long processId) {
      return getAs(
          OptimizingProcessMapper.class, mapper -> mapper.selectTaskRuntimes(tableId, processId));
    }

    private TableProcessMeta process(long processId) {
      return getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(processId));
    }

    private List<OptimizingTaskMeta> taskMetas(long processId) {
      return getAs(
          OptimizingProcessMapper.class,
          mapper -> mapper.selectOptimizeTaskMetas(Collections.singletonList(processId)));
    }
  }
}
