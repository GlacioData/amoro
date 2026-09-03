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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.TableIdentifier;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.server.optimizing.OptimizingTaskMeta;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.ConsumerInfo;
import org.apache.amoro.table.descriptor.DDLInfo;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.amoro.table.descriptor.PartitionBaseInfo;
import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.util.ThreadPools;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;

public class ServerTableDescriptor extends PersistentBase {

  private final Map<TableFormat, FormatTableDescriptor> formatDescriptorMap = new HashMap<>();

  private final CatalogManager catalogManager;
  private final TableManager tableManager;
  private final AmsOptimizingHistory optimizingHistory = new AmsOptimizingHistory();

  public ServerTableDescriptor(
      CatalogManager catalogManager, TableManager tableManager, Configurations serviceConfig) {
    this.tableManager = tableManager;
    this.catalogManager = catalogManager;

    // All table formats will jointly reuse the work thread pool named iceberg-worker-pool-%d
    ExecutorService executorService = ThreadPools.getWorkerPool();
    ServiceLoader<FormatTableDescriptor> tableDescriptorLoader =
        ServiceLoader.load(FormatTableDescriptor.class);
    for (FormatTableDescriptor descriptor : tableDescriptorLoader) {
      for (TableFormat format : descriptor.supportFormat()) {
        formatDescriptorMap.put(format, descriptor);
      }
      descriptor.withIoExecutor(executorService);
    }
  }

  public ServerTableMeta getTableDetail(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableDetail(amoroTable);
  }

  public Pair<List<AmoroSnapshotsOfTable>, Long> getSnapshots(
      TableIdentifier tableIdentifier,
      String ref,
      OperationType operationType,
      int limit,
      int offset,
      String lastSnapshot) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getSnapshots(
        amoroTable, ref, operationType, limit, offset, lastSnapshot);
  }

  public List<PartitionFileBaseInfo> getSnapshotDetail(
      TableIdentifier tableIdentifier, String snapshotId, String ref) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getSnapshotDetail(amoroTable, snapshotId, ref);
  }

  public List<DDLInfo> getTableOperations(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableOperations(amoroTable);
  }

  public List<PartitionBaseInfo> getTablePartition(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTablePartitions(amoroTable);
  }

  public List<PartitionFileBaseInfo> getTableFile(
      TableIdentifier tableIdentifier, String partition, Integer specId) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableFiles(amoroTable, partition, specId);
  }

  public List<TagOrBranchInfo> getTableTags(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableTags(amoroTable);
  }

  public List<TagOrBranchInfo> getTableBranches(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableBranches(amoroTable);
  }

  public List<ConsumerInfo> getTableConsumersInfos(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableConsumerInfos(amoroTable);
  }

  public Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfo(
      TableIdentifier tableIdentifier,
      String type,
      ProcessStatus status,
      int limit,
      int offset,
      String lastSnapshot) {
    return getOptimizingProcessesInfo(
        tableIdentifier, type, status, limit, offset, lastSnapshot, true);
  }

  public Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfo(
      TableIdentifier tableIdentifier,
      String type,
      ProcessStatus status,
      int limit,
      int offset,
      String lastSnapshot,
      boolean includeError) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    if (usesAmsOptimizingHistory(amoroTable.format())) {
      return optimizingHistory.getProcesses(amoroTable, type, status, limit, offset, includeError);
    }
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    Pair<List<OptimizingProcessInfo>, Integer> result =
        formatTableDescriptor.getOptimizingProcessesInfo(
            amoroTable, type, status, limit, offset, lastSnapshot);
    if (!includeError) {
      result.getLeft().forEach(process -> process.setFailReason(null));
    }
    return result;
  }

  public List<OptimizingTaskInfo> getOptimizingProcessTaskInfos(
      TableIdentifier tableIdentifier, String processId) {
    return getOptimizingProcessTaskInfos(tableIdentifier, processId, true);
  }

  public List<OptimizingTaskInfo> getOptimizingProcessTaskInfos(
      TableIdentifier tableIdentifier, String processId, boolean includeError) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    if (usesAmsOptimizingHistory(amoroTable.format())) {
      return optimizingHistory.getTasks(amoroTable, processId, includeError);
    }
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    List<OptimizingTaskInfo> result =
        formatTableDescriptor.getOptimizingTaskInfos(amoroTable, processId);
    if (!includeError) {
      result.forEach(task -> task.setFailReason(null));
    }
    return result;
  }

  public Pair<List<OptimizingTaskInfo>, Integer> getOptimizingProcessTaskInfos(
      TableIdentifier tableIdentifier,
      String processId,
      int limit,
      int offset,
      boolean includeError) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    if (usesAmsOptimizingHistory(amoroTable.format())) {
      return optimizingHistory.getTaskPage(amoroTable, processId, limit, offset, includeError);
    }

    List<OptimizingTaskInfo> tasks =
        formatDescriptorMap.get(amoroTable.format()).getOptimizingTaskInfos(amoroTable, processId);
    if (!includeError) {
      tasks.forEach(task -> task.setFailReason(null));
    }
    int total = tasks.size();
    if (offset >= total) {
      return Pair.of(Collections.emptyList(), total);
    }
    int toIndex = (int) Math.min((long) offset + limit, total);
    return Pair.of(tasks.subList(offset, toIndex), total);
  }

  public Optional<OptimizingTaskMeta> findOptimizingTask(long tableId, long processId, int taskId) {
    return optimizingHistory.findTask(tableId, processId, taskId);
  }

  public Map<String, String> getTableOptimizingTypes(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableOptimizingTypes(amoroTable);
  }

  private AmoroTable<?> loadTable(TableIdentifier identifier) {
    ServerCatalog catalog = catalogManager.getServerCatalog(identifier.getCatalog());
    return catalog.loadTable(identifier.getDatabase(), identifier.getTableName());
  }

  private static boolean usesAmsOptimizingHistory(TableFormat format) {
    return format == TableFormat.PAIMON
        || format == TableFormat.ICEBERG
        || format == TableFormat.MIXED_ICEBERG
        || format == TableFormat.MIXED_HIVE;
  }
}
