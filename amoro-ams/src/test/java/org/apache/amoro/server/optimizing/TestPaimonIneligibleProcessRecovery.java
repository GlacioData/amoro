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

import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.OptimizingTaskResult;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionExecutorFactory;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionOutput;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionTask;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.process.PaimonProcessFactory;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.optimizing.TaskProperties;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.resource.OptimizerThread;
import org.apache.amoro.server.table.AMSTableTestBase;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.utils.IdGenerator;
import org.apache.amoro.utils.SerializationUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class TestPaimonIneligibleProcessRecovery extends AMSTableTestBase {

  private static final String GROUP_NAME = "paimon-ineligible-recovery";
  private static final long MAX_POLLING_TIME = 5000L;

  private final ExecutorService planExecutor = Executors.newSingleThreadExecutor();
  private final Persistency persistency = new Persistency();
  private final OptimizerThread optimizerThread =
      new OptimizerThread(19, null) {
        @Override
        public String getToken() {
          return "orphan-token";
        }
      };

  public TestPaimonIneligibleProcessRecovery() {
    super(
        new BasicCatalogTestHelper(TableFormat.ICEBERG),
        new BasicTableTestHelper(false, true),
        true);
  }

  @Before
  public void clearMetrics() {
    MetricRegistry registry = MetricManager.getInstance().getGlobalRegistry();
    List<MetricKey> keys = new ArrayList<>();
    for (MetricKey key : registry.getMetrics().keySet()) {
      if (GROUP_NAME.equals(key.valueOfTag(OptimizerGroupMetrics.GROUP_TAG))) {
        keys.add(key);
      }
    }
    keys.forEach(registry::unregister);
  }

  @After
  public void shutdownExecutor() {
    planExecutor.shutdownNow();
  }

  @Test
  public void startupClosesOwnedIneligibleProcessAndPreservesSuccessfulTask() {
    Incident incident = createIncidentWithSuccessAndPlannedTasks();
    makePaimonIneligible(incident.runtime);

    OptimizingQueue recoveryQueue = recoveryQueue(incident.runtime, paimonFactoryRouter());

    TableProcessMeta process = persistency.process(incident.processId);
    List<OptimizingTaskMeta> tasks = persistency.tasks(incident.processId);
    Assert.assertEquals(ProcessStatus.CLOSED, process.getStatus());
    Assert.assertEquals(1, countTasks(tasks, TaskRuntime.Status.SUCCESS));
    Assert.assertTrue(countTasks(tasks, TaskRuntime.Status.CANCELED) >= 1);
    OptimizingTaskMeta successful =
        tasks.stream()
            .filter(task -> task.getStatus() == TaskRuntime.Status.SUCCESS)
            .findFirst()
            .orElseThrow(AssertionError::new);
    Assert.assertEquals("orphan-token", successful.getOptimizerToken());
    Assert.assertEquals(19, successful.getThreadId());
    Assert.assertEquals(0L, incident.runtime.getProcessId());
    Assert.assertEquals(OptimizingStatus.IDLE, incident.runtime.getOptimizingStatus());
    Assert.assertNull(incident.runtime.getOptimizingProcess());
    Assert.assertTrue(recoveryQueue.collectTasks().isEmpty());

    recoveryQueue.dispose();
    OptimizingQueue repeatedRecovery = recoveryQueue(incident.runtime, paimonFactoryRouter());
    Assert.assertEquals(ProcessStatus.CLOSED, persistency.process(incident.processId).getStatus());
    Assert.assertEquals(0L, incident.runtime.getProcessId());
    Assert.assertTrue(repeatedRecovery.collectTasks().isEmpty());
    repeatedRecovery.dispose();
  }

  @Test
  public void startupKeepsIneligiblePaimonProcessAlreadyCommitting() {
    DefaultTableRuntime runtime = createCommittingProcess();
    long processId = runtime.getProcessId();
    makePaimonIneligible(runtime);

    OptimizingQueue recoveryQueue = recoveryQueue(runtime, paimonFactoryRouter());

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(processId).getStatus());
    Assert.assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
    Assert.assertEquals(processId, runtime.getProcessId());
    Assert.assertNotNull(runtime.getOptimizingProcess());
    Assert.assertEquals(ProcessStatus.RUNNING, runtime.getOptimizingProcess().getStatus());
    recoveryQueue.dispose();
  }

  @Test
  public void startupDoesNotCloseWhenOwnershipIsUnknown() {
    Incident incident = createIncidentWithSuccessAndPlannedTasks();
    makePaimonIneligible(incident.runtime);

    OptimizingQueue recoveryQueue =
        recoveryQueue(
            incident.runtime, paimonFactoryRouter(), ignored -> OptimizingOwnership.UNKNOWN);

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(incident.processId).getStatus());
    Assert.assertEquals(incident.processId, incident.runtime.getProcessId());
    Assert.assertFalse(
        persistency.tasks(incident.processId).stream()
            .anyMatch(task -> task.getStatus() == TaskRuntime.Status.CANCELED));
    recoveryQueue.dispose();
  }

  @Test
  public void startupDoesNotCloseWhenEligibilityCannotBeEvaluated() {
    Incident incident = createIncidentWithSuccessAndPlannedTasks();
    ProcessFactory failingFactory = Mockito.mock(ProcessFactory.class);
    Mockito.when(failingFactory.supportedFormats())
        .thenReturn(Collections.singleton(TableFormat.PAIMON));
    Mockito.when(failingFactory.isOptimizingEligible(incident.runtime))
        .thenThrow(new IllegalStateException("eligibility unavailable"));

    OptimizingQueue recoveryQueue =
        recoveryQueue(
            incident.runtime,
            new ProcessFactoryRouter(Collections.singletonList(failingFactory)),
            ignored -> OptimizingOwnership.OWNED);

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(incident.processId).getStatus());
    Assert.assertEquals(incident.processId, incident.runtime.getProcessId());
    Assert.assertFalse(
        persistency.tasks(incident.processId).stream()
            .anyMatch(task -> task.getStatus() == TaskRuntime.Status.CANCELED));
    recoveryQueue.dispose();
  }

  @Test
  public void startupNormalizesIneligiblePlanningTableWithoutProcess() {
    DefaultTableRuntime runtime =
        Mockito.spy(getDefaultTableRuntime(serverTableIdentifier().getId()));
    Mockito.doReturn(TableFormat.PAIMON).when(runtime).getFormat();
    runtime
        .store()
        .begin()
        .updateStatusCode(ignored -> OptimizingStatus.PLANNING.getCode())
        .updateTableConfig(
            config -> {
              config.put("write-only", "true");
              config.remove(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED);
            })
        .commit();
    Assert.assertEquals(0L, runtime.getProcessId());

    OptimizingQueue recoveryQueue = recoveryQueue(runtime, paimonFactoryRouter());

    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    Assert.assertEquals(0L, runtime.getProcessId());
    recoveryQueue.dispose();
  }

  @Test
  public void liveConfigReconciliationClosesForEveryPaimonEligibilityRevocation() {
    assertLiveConfigRevocationCloses(
        config -> config.remove(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED));
    assertLiveConfigRevocationCloses(config -> config.remove("write-only"));
    assertLiveConfigRevocationCloses(config -> config.put("write-only", "false"));
    assertLiveConfigRevocationCloses(
        config -> config.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "false"));
  }

  @Test
  public void liveConfigReconciliationKeepsProcessAlreadyCommitting() {
    LiveIncident incident = createLiveCommittingProcess();
    updateConfig(
        incident.runtime,
        config -> config.remove(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED));

    incident.queue.reconcileTableConfig(incident.runtime);

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(incident.processId).getStatus());
    Assert.assertEquals(OptimizingStatus.COMMITTING, incident.runtime.getOptimizingStatus());
    Assert.assertEquals(incident.processId, incident.runtime.getProcessId());
    incident.queue.dispose();
  }

  @Test
  public void liveConfigReconciliationDoesNotCloseWhenOwnershipIsUnknown() {
    AtomicReference<OptimizingOwnership> ownership =
        new AtomicReference<>(OptimizingOwnership.OWNED);
    LiveIncident incident = createLiveIncident(ignored -> ownership.get());
    updateConfig(
        incident.runtime,
        config -> config.remove(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED));
    ownership.set(OptimizingOwnership.UNKNOWN);

    incident.queue.reconcileTableConfig(incident.runtime);

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(incident.processId).getStatus());
    Assert.assertEquals(incident.processId, incident.runtime.getProcessId());
    incident.queue.dispose();
  }

  @Test
  public void liveConfigReconciliationDoesNotCloseWhenEligibilityIsUnknown() {
    PlannedPaimonProcess planned = plannedPaimonProcess();
    ProcessFactory factory = sourceFactory(planned);
    OptimizingQueue queue = sourceQueue(planned, factory, ignored -> OptimizingOwnership.OWNED);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    long processId = planned.runtime.getProcessId();
    Mockito.when(factory.isOptimizingEligible(planned.runtime))
        .thenThrow(new IllegalStateException("eligibility unavailable"));

    queue.reconcileTableConfig(planned.runtime);

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(processId).getStatus());
    Assert.assertEquals(processId, planned.runtime.getProcessId());
    queue.dispose();
  }

  @Test
  public void repeatedRecoveryReconcilesAlreadyAttachedProcess() {
    LiveIncident incident = createLiveIncident(ignored -> OptimizingOwnership.OWNED);
    makePaimonIneligible(incident.runtime);

    incident.queue.recoverOwnedTable(incident.runtime);

    Assert.assertEquals(ProcessStatus.CLOSED, persistency.process(incident.processId).getStatus());
    Assert.assertEquals(0L, incident.runtime.getProcessId());
    Assert.assertEquals(OptimizingStatus.IDLE, incident.runtime.getOptimizingStatus());
    Assert.assertNull(incident.runtime.getOptimizingProcess());
    incident.queue.dispose();
  }

  @Test
  public void ownershipHandoffDetachesOldOwnerAndNewOwnerClosesSameProcess() {
    AtomicReference<OptimizingOwnership> ownership =
        new AtomicReference<>(OptimizingOwnership.OWNED);
    LiveIncident incident = createLiveIncident(ignored -> ownership.get());
    makePaimonIneligible(incident.runtime);

    ownership.set(OptimizingOwnership.NOT_OWNED);
    incident.queue.detachTable(incident.runtime);

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(incident.processId).getStatus());
    Assert.assertEquals(incident.processId, incident.runtime.getProcessId());
    Assert.assertNull(incident.runtime.getOptimizingProcess());
    Assert.assertTrue(incident.queue.collectTasks().isEmpty());
    incident.queue.dispose();

    OptimizingQueue newOwner = recoveryQueue(incident.runtime, paimonFactoryRouter());

    Assert.assertEquals(ProcessStatus.CLOSED, persistency.process(incident.processId).getStatus());
    Assert.assertEquals(0L, incident.runtime.getProcessId());
    Assert.assertEquals(OptimizingStatus.IDLE, incident.runtime.getOptimizingStatus());
    Assert.assertNull(incident.runtime.getOptimizingProcess());
    newOwner.dispose();
  }

  private void assertLiveConfigRevocationCloses(Consumer<Map<String, String>> revocation) {
    LiveIncident incident = createLiveIncident(ignored -> OptimizingOwnership.OWNED);
    updateConfig(incident.runtime, revocation);

    incident.queue.reconcileTableConfig(incident.runtime);

    Assert.assertEquals(ProcessStatus.CLOSED, persistency.process(incident.processId).getStatus());
    Assert.assertTrue(
        persistency.tasks(incident.processId).stream()
            .allMatch(task -> task.getStatus() == TaskRuntime.Status.CANCELED));
    Assert.assertEquals(OptimizingStatus.IDLE, incident.runtime.getOptimizingStatus());
    Assert.assertEquals(0L, incident.runtime.getProcessId());
    Assert.assertTrue(incident.queue.collectTasks().isEmpty());
    incident.queue.dispose();
  }

  private LiveIncident createLiveIncident(
      java.util.function.Function<DefaultTableRuntime, OptimizingOwnership> ownershipGuard) {
    PlannedPaimonProcess planned = plannedPaimonProcess();
    OptimizingQueue queue = sourceQueue(planned, sourceFactory(planned), ownershipGuard);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    long processId = planned.runtime.getProcessId();
    Assert.assertTrue(processId > 0L);
    return new LiveIncident(planned.runtime, queue, processId);
  }

  private LiveIncident createLiveCommittingProcess() {
    PlannedPaimonProcess planned = plannedPaimonProcess();
    OptimizingQueue queue = sourceQueue(planned);
    for (int i = 0; i < planned.tasks.size(); i++) {
      TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
      Assert.assertNotNull(task);
      queue.ackTask(task.getTaskId(), optimizerThread);
      queue.completeTask(optimizerThread, successfulResult(task));
    }
    Assert.assertEquals(OptimizingStatus.COMMITTING, planned.runtime.getOptimizingStatus());
    return new LiveIncident(planned.runtime, queue, planned.runtime.getProcessId());
  }

  private void updateConfig(
      DefaultTableRuntime runtime, Consumer<Map<String, String>> configUpdater) {
    runtime.store().begin().updateTableConfig(configUpdater).commit();
  }

  private Incident createIncidentWithSuccessAndPlannedTasks() {
    PlannedPaimonProcess planned = plannedPaimonProcess();
    DefaultTableRuntime runtime = planned.runtime;
    OptimizingQueue queue = sourceQueue(planned);
    TaskRuntime<?> successfulTask = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(successfulTask);
    queue.ackTask(successfulTask.getTaskId(), optimizerThread);
    queue.completeTask(optimizerThread, successfulResult(successfulTask));
    Assert.assertEquals(TaskRuntime.Status.SUCCESS, successfulTask.getStatus());
    Assert.assertTrue(
        "Fixture must retain at least one PLANNED task",
        queue.collectTasks(task -> task.getStatus() == TaskRuntime.Status.PLANNED).size() >= 1);
    long processId = runtime.getProcessId();
    Assert.assertTrue(processId > 0L);
    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(processId).getStatus());
    queue.dispose();
    runtime.detachOptimizingProcess(runtime.getOptimizingProcess());
    return new Incident(runtime, processId);
  }

  private DefaultTableRuntime createCommittingProcess() {
    PlannedPaimonProcess planned = plannedPaimonProcess();
    DefaultTableRuntime runtime = planned.runtime;
    OptimizingQueue queue = sourceQueue(planned);
    for (int i = 0; i < planned.tasks.size(); i++) {
      TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
      Assert.assertNotNull(task);
      queue.ackTask(task.getTaskId(), optimizerThread);
      queue.completeTask(optimizerThread, successfulResult(task));
    }
    Assert.assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
    queue.dispose();
    runtime.detachOptimizingProcess(runtime.getOptimizingProcess());
    return runtime;
  }

  private PlannedPaimonProcess plannedPaimonProcess() {
    long processId = IdGenerator.randomId();
    DefaultTableRuntime runtime =
        Mockito.spy(getDefaultTableRuntime(serverTableIdentifier().getId()));
    Mockito.doReturn(TableFormat.PAIMON).when(runtime).getFormat();
    Mockito.doReturn(runtime).when(runtime).refresh(Mockito.any());
    Mockito.doReturn(1L).when(runtime).getCurrentSnapshotId();
    Mockito.doReturn(0L).when(runtime).getLastOptimizedSnapshotId();
    runtime
        .store()
        .begin()
        .updateStatusCode(ignored -> OptimizingStatus.PENDING.getCode())
        .updateTableConfig(
            config -> {
              config.put("write-only", "true");
              config.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");
            })
        .commit();
    Map<String, String> properties = new HashMap<>();
    properties.put(
        TaskProperties.TASK_EXECUTOR_FACTORY_IMPL, PaimonCompactionExecutorFactory.class.getName());
    List<PaimonCompactionTask> tasks = new ArrayList<>();
    tasks.add(paimonTask(runtime, processId, 1, properties));
    tasks.add(paimonTask(runtime, processId, 2, properties));
    OptimizingPlanResult<PaimonCompactionTask> planResult =
        new OptimizingPlanResult<>(
            processId,
            OptimizingType.MAJOR,
            System.currentTimeMillis(),
            1L,
            -1L,
            tasks,
            Collections.emptyMap(),
            Collections.emptyMap());
    return new PlannedPaimonProcess(runtime, tasks, planResult);
  }

  private PaimonCompactionTask paimonTask(
      DefaultTableRuntime runtime, long processId, int taskId, Map<String, String> properties) {
    String partition = "p=" + taskId;
    PaimonCompactionInput input =
        new PaimonCompactionInput(
            null, new byte[] {(byte) taskId}, 2, "commit-user", partition, 1L, processId);
    return new PaimonCompactionTask(
        runtime.getTableIdentifier().getId(), partition, input, new HashMap<>(properties));
  }

  private OptimizingTaskResult successfulResult(TaskRuntime<?> task) {
    PaimonCompactionOutput output =
        new PaimonCompactionOutput(new byte[] {1}, 2, 2L, 200L, 1L, 100L);
    return new OptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId())
        .setTaskOutput(SerializationUtil.simpleSerialize(output));
  }

  private void makePaimonIneligible(DefaultTableRuntime runtime) {
    long processId = runtime.getProcessId();
    ProcessStatus processStatus = persistency.process(processId).getStatus();
    OptimizingStatus optimizingStatus = runtime.getOptimizingStatus();
    runtime
        .store()
        .begin()
        .updateTableConfig(
            config -> {
              config.put("write-only", "true");
              config.remove(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED);
            })
        .commit();
    Assert.assertEquals("true", runtime.getTableConfig().get("write-only"));
    Assert.assertFalse(
        runtime.getTableConfig().containsKey(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED));
    Assert.assertTrue(runtime.getOptimizingConfig().isEnabled());
    Assert.assertEquals(processId, runtime.getProcessId());
    Assert.assertEquals(processStatus, persistency.process(processId).getStatus());
    Assert.assertEquals(optimizingStatus, runtime.getOptimizingStatus());
  }

  private OptimizingQueue sourceQueue(PlannedPaimonProcess planned) {
    return sourceQueue(planned, sourceFactory(planned), ignored -> OptimizingOwnership.OWNED);
  }

  private ProcessFactory sourceFactory(PlannedPaimonProcess planned) {
    TableOptimizingPlanner planner = Mockito.mock(TableOptimizingPlanner.class);
    Mockito.when(planner.isNecessary()).thenReturn(true);
    Mockito.doReturn(planned.planResult).when(planner).plan();
    ProcessFactory factory = Mockito.mock(ProcessFactory.class);
    Mockito.when(factory.supportedFormats()).thenReturn(Collections.singleton(TableFormat.PAIMON));
    Mockito.when(factory.isOptimizingEligible(planned.runtime))
        .thenAnswer(
            ignored -> PaimonOptimizingEligibility.isEligible(planned.runtime.getTableConfig()));
    Mockito.when(
            factory.createPlanner(
                Mockito.eq(planned.runtime), Mockito.any(), Mockito.anyDouble(), Mockito.anyLong()))
        .thenReturn(planner);
    return factory;
  }

  private OptimizingQueue sourceQueue(
      PlannedPaimonProcess planned,
      ProcessFactory factory,
      java.util.function.Function<DefaultTableRuntime, OptimizingOwnership> ownershipGuard) {
    return new OptimizingQueue(
        catalogManager(),
        resourceGroup(),
        ignored -> 1,
        planExecutor,
        Collections.singletonList(planned.runtime),
        1,
        new ProcessFactoryRouter(Collections.singletonList(factory)),
        ownershipGuard);
  }

  private OptimizingQueue recoveryQueue(
      DefaultTableRuntime runtime, ProcessFactoryRouter factoryRouter) {
    return recoveryQueue(runtime, factoryRouter, ignored -> OptimizingOwnership.OWNED);
  }

  private OptimizingQueue recoveryQueue(
      DefaultTableRuntime runtime,
      ProcessFactoryRouter factoryRouter,
      java.util.function.Function<DefaultTableRuntime, OptimizingOwnership> ownershipGuard) {
    return new OptimizingQueue(
        catalogManager(),
        resourceGroup(),
        ignored -> 1,
        planExecutor,
        Collections.singletonList(runtime),
        1,
        factoryRouter,
        ownershipGuard);
  }

  private ProcessFactoryRouter paimonFactoryRouter() {
    return new ProcessFactoryRouter(Collections.singletonList(new PaimonProcessFactory()));
  }

  private ResourceGroup resourceGroup() {
    return new ResourceGroup.Builder(GROUP_NAME, "local").build();
  }

  private static long countTasks(List<OptimizingTaskMeta> tasks, TaskRuntime.Status status) {
    return tasks.stream().filter(task -> task.getStatus() == status).count();
  }

  private static class PlannedPaimonProcess {
    private final DefaultTableRuntime runtime;
    private final List<PaimonCompactionTask> tasks;
    private final OptimizingPlanResult<PaimonCompactionTask> planResult;

    private PlannedPaimonProcess(
        DefaultTableRuntime runtime,
        List<PaimonCompactionTask> tasks,
        OptimizingPlanResult<PaimonCompactionTask> planResult) {
      this.runtime = runtime;
      this.tasks = tasks;
      this.planResult = planResult;
    }
  }

  private static class Incident {
    private final DefaultTableRuntime runtime;
    private final long processId;

    private Incident(DefaultTableRuntime runtime, long processId) {
      this.runtime = runtime;
      this.processId = processId;
    }
  }

  private static class LiveIncident {
    private final DefaultTableRuntime runtime;
    private final OptimizingQueue queue;
    private final long processId;

    private LiveIncident(DefaultTableRuntime runtime, OptimizingQueue queue, long processId) {
      this.runtime = runtime;
      this.queue = queue;
      this.processId = processId;
    }
  }

  private static class Persistency extends PersistentBase {
    private TableProcessMeta process(long processId) {
      return getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(processId));
    }

    private List<OptimizingTaskMeta> tasks(long processId) {
      return getAs(
          OptimizingProcessMapper.class,
          mapper -> mapper.selectOptimizeTaskMetas(Collections.singletonList(processId)));
    }
  }
}
