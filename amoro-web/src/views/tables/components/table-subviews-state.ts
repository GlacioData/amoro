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

export interface PartitionSearchState {
  searchKey: string
  current: number
}

export interface AdaptiveTableColumn {
  title: string
  dataIndex: string
  width?: number | string
  minWidth?: number
  ellipsis?: boolean
}

export const TABLE_EXPAND_COLUMN_WIDTH = 36

export function computeTableMinWidth(
  columns: ReadonlyArray<{ width?: number | string, minWidth?: number }>,
  expandColumnWidth = 0,
): number {
  return columns.reduce(
    (sum, col) => sum + Number(typeof col.width === 'number' ? col.width : (col.minWidth ?? 80)),
    expandColumnWidth,
  )
}

export function buildOptimizingProcessColumns(t: (key: string) => string): AdaptiveTableColumn[] {
  return [
    { title: t('processId'), dataIndex: 'processId', minWidth: 96 },
    { title: t('startTime'), dataIndex: 'startTime', minWidth: 96 },
    { title: t('type'), dataIndex: 'optimizingType', minWidth: 72 },
    { title: t('status'), dataIndex: 'status', minWidth: 96 },
    { title: t('duration'), dataIndex: 'duration', minWidth: 64 },
    { title: t('tasks'), dataIndex: 'tasks', minWidth: 64 },
    { title: t('finishTime'), dataIndex: 'finishTime', minWidth: 96 },
    { title: t('input'), dataIndex: 'inputFiles', minWidth: 76 },
    { title: t('output'), dataIndex: 'outputFiles', minWidth: 76 },
  ]
}

export function buildOptimizingTaskColumns(t: (key: string) => string): AdaptiveTableColumn[] {
  return [
    { title: t('taskId'), dataIndex: 'taskId', minWidth: 64 },
    { title: t('partition'), dataIndex: 'partitionData', minWidth: 112, ellipsis: true },
    { title: t('startTime'), dataIndex: 'startTime', minWidth: 96 },
    { title: t('status'), dataIndex: 'status', minWidth: 108 },
    { title: t('costTime'), dataIndex: 'formatCostTime', minWidth: 72 },
    { title: t('finishTime'), dataIndex: 'endTime', minWidth: 96 },
    { title: t('input'), dataIndex: 'inputFilesDesc', minWidth: 82 },
    { title: t('output'), dataIndex: 'outputFilesDesc', minWidth: 82 },
  ]
}

export function buildProcessColumns(t: (key: string) => string): AdaptiveTableColumn[] {
  return [
    { title: t('processId'), dataIndex: 'processId', minWidth: 120 },
    { title: t('status'), dataIndex: 'status', minWidth: 102 },
    { title: t('processType'), dataIndex: 'processType', minWidth: 80 },
    { title: t('processStage'), dataIndex: 'processStage', minWidth: 92 },
    { title: t('executionEngine'), dataIndex: 'executionEngine', minWidth: 84 },
    { title: t('externalProcessId'), dataIndex: 'externalProcessIdentifier', minWidth: 94, ellipsis: true },
    { title: t('retry'), dataIndex: 'retryNumber', minWidth: 56 },
    { title: t('startTime'), dataIndex: 'createTimeFormatted', minWidth: 96 },
    { title: t('finishTime'), dataIndex: 'finishTimeFormatted', minWidth: 96 },
  ]
}

export function applyPartitionSearch(query: string): PartitionSearchState {
  return {
    searchKey: query,
    current: 1,
  }
}

export function buildOperationRowKey(
  record: { ts?: string | number | null, operation?: string | null },
  page: number,
  index: number,
): string {
  const ts = record.ts ?? ''
  const op = record.operation ?? ''
  return `${page}:${index}:${ts}:${op}`
}
