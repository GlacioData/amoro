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

import type { TableFileStatistics, TableHealthEvaluation } from '../../../types/common.type'

const statusKeys: Record<string, string> = {
  CURRENT: 'fileStatisticsCurrent',
  STALE: 'fileStatisticsStale',
  UNKNOWN: 'fileStatisticsUnknown',
  PENDING: 'fileStatisticsPending',
  UNAVAILABLE: 'fileStatisticsUnavailable',
}
const reasonKeys: Record<string, string> = {
  COLLECTION_DISABLED: 'fileStatisticsCollectionDisabled',
  NOT_MANAGED: 'fileStatisticsNotManaged',
  NO_VALID_STATISTICS: 'fileStatisticsNoValidStatistics',
  WAITING_FOR_STATISTICS: 'fileStatisticsWaiting',
  SNAPSHOT_OBSERVATION_MISMATCH: 'fileStatisticsObservationMismatch',
  SNAPSHOT_SCAN_FAILED: 'healthReasonSnapshotScanFailed',
  DELETE_METADATA_READ_FAILED: 'healthReasonDeleteReadFailed',
}

export function fileStatisticsPresentation(
  tableFormat: string,
  statistics?: TableFileStatistics | null,
  evaluation?: TableHealthEvaluation | null,
) {
  if (tableFormat.toLowerCase() !== 'paimon')
    return null
  const failed = evaluation?.status === 'FAILED' || evaluation?.status === 'PARTIAL'
  const reasons = (statistics?.reasonCodes ?? []).map(code => reasonKeys[code]).filter(Boolean)
  if (failed) {
    reasons.push('fileStatisticsHealthUnavailable')
    reasons.push(...(evaluation?.reasonCodes ?? []).map(code => reasonKeys[code]).filter(Boolean))
    if (statistics?.collectionEnabled === true && evaluation?.retryable)
      reasons.push('healthRetryNotice')
  }
  return {
    count: statistics?.fileCount ?? null,
    statusKey: statistics?.source === 'EMPTY_SNAPSHOT'
      ? 'fileStatisticsEmpty'
      : statusKeys[statistics?.status ?? 'UNKNOWN'] ?? 'fileStatisticsUnknown',
    reasons: [...new Set(reasons)],
    evaluatedAtMillis: statistics?.evaluatedAtMillis ?? null,
    attemptedAtMillis: failed ? evaluation?.attemptedAtMillis ?? null : null,
    sourceSnapshotId: statistics?.snapshotId ?? null,
    observedSnapshotId: statistics?.observedSnapshotId ?? null,
  }
}
