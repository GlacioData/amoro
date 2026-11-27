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

import assert from 'node:assert/strict'
import test from 'node:test'

import {
  TABLE_EXPAND_COLUMN_WIDTH,
  applyPartitionSearch,
  buildOperationRowKey,
  buildOptimizingProcessColumns,
  buildOptimizingTaskColumns,
  buildProcessColumns,
  computeTableMinWidth,
} from '../src/views/tables/components/table-subviews-state.ts'

test('applyPartitionSearch resets page to 1 for both non-empty and cleared searches', () => {
  assert.deepEqual(applyPartitionSearch('dt=2026-01-01'), {
    searchKey: 'dt=2026-01-01',
    current: 1,
  })
  assert.deepEqual(applyPartitionSearch(''), {
    searchKey: '',
    current: 1,
  })
})

test('buildOperationRowKey generates stable unique keys even when timestamps collide or are empty', () => {
  const keyA = buildOperationRowKey({ ts: '2026-01-01 00:00:00', operation: 'ALTER TABLE t ADD COLUMN a INT' }, 1, 0)
  const keyB = buildOperationRowKey({ ts: '2026-01-01 00:00:00', operation: 'ALTER TABLE t ADD COLUMN a INT' }, 1, 1)
  const keyEmpty = buildOperationRowKey({ ts: '', operation: '' }, 2, 0)

  assert.ok(keyA.length > 0)
  assert.notEqual(keyA, keyB)
  assert.notEqual(keyA, keyEmpty)
  assert.equal(
    keyA,
    buildOperationRowKey({ ts: '2026-01-01 00:00:00', operation: 'ALTER TABLE t ADD COLUMN a INT' }, 1, 0),
  )
})

test('buildOptimizingProcessColumns adapts to desktop workspace without fixed-width overflow or clipping', () => {
  const cols = buildOptimizingProcessColumns(key => key)
  assert.deepEqual(
    cols.map(col => col.dataIndex),
    ['processId', 'startTime', 'optimizingType', 'status', 'duration', 'tasks', 'finishTime', 'inputFiles', 'outputFiles'],
  )

  for (const col of cols) {
    assert.equal(col.width, undefined, `Column ${col.dataIndex} should be flexible (use minWidth, not fixed width)`)
    assert.ok(typeof col.minWidth === 'number' && col.minWidth > 0, `Column ${col.dataIndex} must declare a positive minWidth`)
  }

  const byIndex = Object.fromEntries(cols.map(col => [col.dataIndex, col]))
  assert.ok((byIndex.processId.minWidth ?? 0) >= 96, 'processId needs enough width for 10+ digit IDs')
  assert.ok((byIndex.status.minWidth ?? 0) >= 96, 'status needs enough width for indicator dot and label')

  const totalMinWidth = computeTableMinWidth(cols, TABLE_EXPAND_COLUMN_WIDTH)
  assert.ok(totalMinWidth <= 840, `Expected Optimizing process table min-width <= 840px for 1440px desktop, got ${totalMinWidth}px`)
})

test('buildOptimizingTaskColumns adapts to desktop workspace and preserves partition ellipsis', () => {
  const cols = buildOptimizingTaskColumns(key => key)
  assert.deepEqual(
    cols.map(col => col.dataIndex),
    ['taskId', 'partitionData', 'startTime', 'status', 'formatCostTime', 'endTime', 'inputFilesDesc', 'outputFilesDesc'],
  )

  for (const col of cols) {
    assert.equal(col.width, undefined, `Task column ${col.dataIndex} should be flexible (use minWidth, not fixed width)`)
    assert.ok(typeof col.minWidth === 'number' && col.minWidth > 0, `Task column ${col.dataIndex} must declare a positive minWidth`)
  }

  const byIndex = Object.fromEntries(cols.map(col => [col.dataIndex, col]))
  assert.equal(byIndex.partitionData.ellipsis, true)
  assert.ok((byIndex.status.minWidth ?? 0) >= 100)

  const totalMinWidth = computeTableMinWidth(cols, TABLE_EXPAND_COLUMN_WIDTH)
  assert.ok(totalMinWidth <= 840, `Expected Optimizing task table min-width <= 840px for 1440px desktop, got ${totalMinWidth}px`)
})

test('buildProcessColumns adapts to 1440px desktop workspace without mid-word clipping or overflow', () => {
  const cols = buildProcessColumns(key => key)
  assert.deepEqual(
    cols.map(col => col.dataIndex),
    ['processId', 'status', 'processType', 'processStage', 'executionEngine', 'externalProcessIdentifier', 'retryNumber', 'createTimeFormatted', 'finishTimeFormatted'],
  )

  for (const col of cols) {
    assert.equal(col.width, undefined, `Process column ${col.dataIndex} should be flexible (use minWidth, not fixed width)`)
    assert.ok(typeof col.minWidth === 'number' && col.minWidth > 0, `Process column ${col.dataIndex} must declare a positive minWidth`)
  }

  const byIndex = Object.fromEntries(cols.map(col => [col.dataIndex, col]))
  assert.ok((byIndex.processId.minWidth ?? 0) >= 120)
  assert.ok((byIndex.status.minWidth ?? 0) >= 100)
  assert.equal(byIndex.externalProcessIdentifier.ellipsis, true)

  const totalMinWidth = computeTableMinWidth(cols, TABLE_EXPAND_COLUMN_WIDTH)
  assert.ok(totalMinWidth <= 860, `Expected Process table min-width <= 860px for 1440px desktop, got ${totalMinWidth}px`)
})
