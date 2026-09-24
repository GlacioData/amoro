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
// eslint-disable-next-line test/no-import-node-test
import test from 'node:test'
import type { TableFileStatistics, TableHealthEvaluation } from '../src/types/common.type.ts'
import { fileStatisticsPresentation } from '../src/views/tables/components/file-statistics-view.ts'

function statistics(overrides: Partial<TableFileStatistics> = {}): TableFileStatistics {
  return {
    status: 'CURRENT', source: 'BACKGROUND', snapshotId: '103', schemaId: '7',
    observedSnapshotId: '105', observedSchemaId: '7', fileCount: '9007199254740993',
    totalSizeBytes: '9223372036854775807', averageFileSizeBytes: '1023',
    evaluatedAtMillis: 1000, collectionEnabled: true, reasonCodes: [], ...overrides,
  }
}

function evaluation(overrides: Partial<TableHealthEvaluation> = {}): TableHealthEvaluation {
  return {
    status: 'FAILED', stale: true, currentSnapshotId: '104', attemptedSnapshotId: '104',
    attemptedAtMillis: 2000, retryable: true, reasonCodes: ['SNAPSHOT_SCAN_FAILED'], ...overrides,
  }
}

test('other formats receive no Paimon presentation and old responses retain the legacy fallback', () => {
  for (const format of ['ICEBERG', 'MIXED_HIVE', 'HUDI', ''])
    assert.equal(fileStatisticsPresentation(format, statistics(), evaluation()), null)
  for (const missing of [undefined, null]) {
    const view = fileStatisticsPresentation('PAIMON', missing)
    assert.ok(view)
    assert.equal(view.count, null)
    assert.equal(view.statusKey, 'fileStatisticsUnknown')
    assert.equal(view.evaluatedAtMillis, null)
    assert.equal(view.attemptedAtMillis, null)
  }
})

test('exact count strings including zero survive without numeric conversion', () => {
  for (const count of ['0', '2147483648', '9007199254740993', '9223372036854775807'])
    assert.equal(fileStatisticsPresentation('PAIMON', statistics({ fileCount: count }))?.count, count)
  assert.equal(fileStatisticsPresentation('PAIMON', statistics({ fileCount: null }))?.count, null)
})

test('backend status is authoritative even when snapshot identities would suggest another status', () => {
  const statuses = {
    CURRENT: 'fileStatisticsCurrent', STALE: 'fileStatisticsStale', UNKNOWN: 'fileStatisticsUnknown',
    PENDING: 'fileStatisticsPending', UNAVAILABLE: 'fileStatisticsUnavailable', FUTURE: 'fileStatisticsUnknown',
  }
  for (const [status, statusKey] of Object.entries(statuses)) {
    const view = fileStatisticsPresentation('PAIMON', statistics({ status, snapshotId: '106' }))
    assert.equal(view?.statusKey, statusKey)
    assert.equal(view?.sourceSnapshotId, '106')
    assert.equal(view?.observedSnapshotId, '105')
  }
})

test('empty snapshots preserve zero and never invent a background statistics time', () => {
  const view = fileStatisticsPresentation('PAIMON', statistics({
    source: 'EMPTY_SNAPSHOT', fileCount: '0', totalSizeBytes: '0', averageFileSizeBytes: '0',
    snapshotId: null, observedSnapshotId: null, evaluatedAtMillis: null,
  }))
  assert.ok(view)
  assert.equal(view.count, '0')
  assert.equal(view.evaluatedAtMillis, null)
  assert.equal(view.sourceSnapshotId, null)
  assert.ok(['fileStatisticsCurrent', 'fileStatisticsEmpty'].includes(view.statusKey))
})

test('valid statistics and failed health keep their own status, times, and diagnostic reasons', () => {
  for (const status of ['FAILED', 'PARTIAL']) {
    const view = fileStatisticsPresentation('PAIMON', statistics(), evaluation({
      status, reasonCodes: ['SNAPSHOT_SCAN_FAILED', 'DELETE_METADATA_READ_FAILED'],
    }))
    assert.ok(view)
    assert.equal(view.statusKey, 'fileStatisticsCurrent')
    assert.equal(view.count, '9007199254740993')
    assert.equal(view.evaluatedAtMillis, 1000)
    assert.equal(view.attemptedAtMillis, 2000)
    for (const reason of ['fileStatisticsHealthUnavailable', 'healthReasonSnapshotScanFailed', 'healthReasonDeleteReadFailed'])
      assert.ok(view.reasons.includes(reason))
  }
})

test('collection and management reasons remain visible while retry requires explicitly enabled collection', () => {
  for (const collectionEnabled of [true, false, null]) {
    for (const retryable of [true, false]) {
      const view = fileStatisticsPresentation('PAIMON', statistics({
        collectionEnabled, reasonCodes: ['COLLECTION_DISABLED', 'NOT_MANAGED'],
      }), evaluation({ retryable }))
      assert.ok(view)
      assert.equal(view.count, '9007199254740993')
      assert.ok(view.reasons.includes('fileStatisticsCollectionDisabled'))
      assert.ok(view.reasons.includes('fileStatisticsNotManaged'))
      assert.equal(view.reasons.includes('healthRetryNotice'), collectionEnabled === true && retryable)
    }
  }
})


test('all backend statistics reasons have explanations independent of health success', () => {
  const view = fileStatisticsPresentation('PAIMON', statistics({
    status: 'UNAVAILABLE', reasonCodes: ['NO_VALID_STATISTICS', 'WAITING_FOR_STATISTICS', 'SNAPSHOT_OBSERVATION_MISMATCH'],
  }), evaluation({ status: 'SUCCESS', reasonCodes: [] }))
  assert.deepEqual(view?.reasons, ['fileStatisticsNoValidStatistics', 'fileStatisticsWaiting', 'fileStatisticsObservationMismatch'])
})
