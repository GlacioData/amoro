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

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.optimizing.MetricsSummary;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.ProcessTaskStatus;
import org.apache.amoro.server.optimizing.OptimizingTaskMeta;
import org.apache.amoro.server.optimizing.TaskRuntime;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Reads the optimizing history recorded by AMS, independently of table-format commit history. */
class AmsOptimizingHistory extends PersistentBase {
  private static final Logger LOG = LoggerFactory.getLogger(AmsOptimizingHistory.class);

  Pair<List<OptimizingProcessInfo>, Integer> getProcesses(
      AmoroTable<?> amoroTable,
      String type,
      ProcessStatus status,
      int limit,
      int offset,
      boolean includeError) {
    TableIdentifier tableIdentifier = amoroTable.id();
    ServerTableIdentifier identifier =
        getAs(
            TableMetaMapper.class,
            mapper ->
                mapper.selectTableIdentifier(
                    tableIdentifier.getCatalog(),
                    tableIdentifier.getDatabase(),
                    tableIdentifier.getTableName()));
    if (identifier == null) {
      return Pair.of(Collections.emptyList(), 0);
    }

    int pageNumber = (offset / limit) + 1;
    List<TableProcessMeta> processMetaList;
    int total;
    try (Page<?> ignored = PageHelper.startPage(pageNumber, limit, true)) {
      processMetaList =
          getAs(
              TableProcessMapper.class,
              mapper ->
                  includeError
                      ? mapper.listProcessMeta(identifier.getId(), type, status)
                      : mapper.listProcessMetaWithoutFailMessage(identifier.getId(), type, status));
      PageInfo<TableProcessMeta> pageInfo = new PageInfo<>(processMetaList);
      total = (int) pageInfo.getTotal();
      LOG.info(
          "Get optimizing processes total: {}, pageNumber: {}, limit: {}, offset: {}",
          total,
          pageNumber,
          limit,
          offset);
      if (pageInfo.getSize() == 0) {
        return Pair.of(Collections.emptyList(), total);
      }
    }

    List<Long> processIds =
        processMetaList.stream().map(TableProcessMeta::getProcessId).collect(Collectors.toList());
    List<OptimizingTaskMeta> taskMetas =
        getAs(
            OptimizingProcessMapper.class,
            mapper -> mapper.selectOptimizeTaskMetasWithoutFailReason(processIds));
    Map<Long, List<OptimizingTaskMeta>> optimizingTasks =
        taskMetas.stream().collect(Collectors.groupingBy(OptimizingTaskMeta::getProcessId));

    return Pair.of(
        processMetaList.stream()
            .map(
                process ->
                    buildOptimizingProcessInfo(
                        identifier, process, optimizingTasks.get(process.getProcessId())))
            .collect(Collectors.toList()),
        total);
  }

  List<OptimizingTaskInfo> getTasks(
      AmoroTable<?> amoroTable, String processId, boolean includeError) {
    long id = Long.parseLong(processId);
    List<Long> processIds = Collections.singletonList(id);
    List<OptimizingTaskMeta> taskMetas =
        getAs(
            OptimizingProcessMapper.class,
            mapper ->
                includeError
                    ? mapper.selectOptimizeTaskMetas(processIds)
                    : mapper.selectOptimizeTaskMetasWithoutFailReason(processIds));
    if (CollectionUtils.isEmpty(taskMetas)) {
      return Collections.emptyList();
    }
    long tableId = serverTableId(amoroTable);
    return taskMetas.stream()
        .filter(task -> task.getTableId() == tableId)
        .map(AmsOptimizingHistory::buildOptimizingTaskInfo)
        .collect(Collectors.toList());
  }

  Pair<List<OptimizingTaskInfo>, Integer> getTaskPage(
      AmoroTable<?> amoroTable, String processId, int limit, int offset, boolean includeError) {
    long tableId = serverTableId(amoroTable);
    if (tableId == Long.MIN_VALUE) {
      return Pair.of(Collections.emptyList(), 0);
    }

    long id = Long.parseLong(processId);
    int pageNumber = (offset / limit) + 1;
    List<OptimizingTaskMeta> taskMetas;
    int total;
    try (Page<?> ignored = PageHelper.startPage(pageNumber, limit, true)) {
      taskMetas =
          getAs(
              OptimizingProcessMapper.class,
              mapper ->
                  includeError
                      ? mapper.selectOptimizeTaskMetasByProcess(tableId, id)
                      : mapper.selectOptimizeTaskMetasWithoutFailReasonByProcess(tableId, id));
      PageInfo<OptimizingTaskMeta> pageInfo = new PageInfo<>(taskMetas);
      total = (int) pageInfo.getTotal();
    }

    return Pair.of(
        taskMetas.stream()
            .map(AmsOptimizingHistory::buildOptimizingTaskInfo)
            .collect(Collectors.toList()),
        total);
  }

  Optional<OptimizingTaskMeta> findTask(long tableId, long processId, int taskId) {
    return Optional.ofNullable(
        getAs(
            OptimizingProcessMapper.class,
            mapper -> mapper.selectOptimizeTaskMeta(tableId, processId, taskId)));
  }

  private long serverTableId(AmoroTable<?> amoroTable) {
    TableIdentifier tableIdentifier = amoroTable.id();
    ServerTableIdentifier identifier =
        getAs(
            TableMetaMapper.class,
            mapper ->
                mapper.selectTableIdentifier(
                    tableIdentifier.getCatalog(),
                    tableIdentifier.getDatabase(),
                    tableIdentifier.getTableName()));
    return identifier == null ? Long.MIN_VALUE : identifier.getId();
  }

  private static OptimizingTaskInfo buildOptimizingTaskInfo(OptimizingTaskMeta taskMeta) {
    MetricsSummary metricsSummary = taskMeta.getMetricsSummary();
    if (metricsSummary == null) {
      metricsSummary = new MetricsSummary();
    }
    return new OptimizingTaskInfo(
        taskMeta.getTableId(),
        String.valueOf(taskMeta.getProcessId()),
        taskMeta.getTaskId(),
        taskMeta.getPartitionData(),
        ProcessTaskStatus.valueOf(taskMeta.getStatus().name()),
        taskMeta.getRetryNum(),
        taskMeta.getOptimizerToken(),
        taskMeta.getThreadId(),
        taskMeta.getStartTime(),
        taskMeta.getEndTime(),
        taskMeta.getCostTime(),
        taskMeta.getFailReason(),
        metricsSummary.getInputFilesStatistics(),
        metricsSummary.getOutputFilesStatistics(),
        metricsSummary.summaryAsMap(true),
        taskMeta.getProperties());
  }

  private static OptimizingProcessInfo buildOptimizingProcessInfo(
      ServerTableIdentifier identifier,
      TableProcessMeta meta,
      List<OptimizingTaskMeta> optimizingTaskStats) {
    OptimizingProcessInfo result = new OptimizingProcessInfo();
    if (optimizingTaskStats != null) {
      int successTasks = 0;
      int runningTasks = 0;
      for (OptimizingTaskMeta task : optimizingTaskStats) {
        TaskRuntime.Status taskStatus = task.getStatus();
        if (taskStatus == TaskRuntime.Status.SUCCESS) {
          successTasks++;
        } else if (taskStatus == TaskRuntime.Status.SCHEDULED
            || taskStatus == TaskRuntime.Status.ACKED) {
          runningTasks++;
        }
      }
      result.setTotalTasks(optimizingTaskStats.size());
      result.setSuccessTasks(successTasks);
      result.setRunningTasks(runningTasks);
    }
    MetricsSummary summary = MetricsSummary.fromMap(meta.getSummary());
    result.setInputFiles(summary.getInputFilesStatistics());
    result.setOutputFiles(summary.getOutputFilesStatistics());
    result.setTableId(meta.getTableId());
    result.setCatalogName(identifier.getCatalog());
    result.setDbName(identifier.getDatabase());
    result.setTableName(identifier.getTableName());
    result.setProcessId(String.valueOf(meta.getProcessId()));
    result.setStartTime(meta.getCreateTime());
    result.setOptimizingType(meta.getProcessType());
    result.setStatus(meta.getStatus());
    result.setFailReason(meta.getFailMessage());
    result.setDuration(
        meta.getFinishTime() > 0
            ? meta.getFinishTime() - meta.getCreateTime()
            : System.currentTimeMillis() - meta.getCreateTime());
    result.setFinishTime(meta.getFinishTime());
    result.setSummary(meta.getSummary());
    return result;
  }
}
