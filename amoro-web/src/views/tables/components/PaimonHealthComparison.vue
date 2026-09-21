<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
/ -->

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { TableHealthComparison } from '@/types/common.type'
import { bytesToSize } from '@/utils'

const props = defineProps<{ comparison?: TableHealthComparison | null }>()
const { t, locale } = useI18n()
const rows = computed(() => {
  const before = props.comparison?.before
  const after = props.comparison?.after
  const values = ['totalFileCount', 'totalFileSize', 'maxSortedRunCount', 'reducibleFileCount', 'tombstoneRecordCount', 'deletionVectorRecordCount', 'deleteRecordCount']
  return values.filter(key => before?.metrics[key] != null || after?.metrics[key] != null)
    .map(key => ({ key, before: before?.metrics[key], after: after?.metrics[key] }))
})
const components = computed(() => (props.comparison?.before?.components || [])
  .filter(item => item.score >= 0)
  .map(item => ({ ...item, after: props.comparison?.after?.components.find(other => other.code === item.code)?.score ?? -1 })))
const componentLabels: Record<string, string> = { SORTED_RUN: 'sortedRunScore', MATERIALIZED_DELETE: 'materializedDeleteScore', FILE_ORGANIZATION: 'fileOrganizationScore' }
const levels = computed(() => {
  const left = props.comparison?.before?.structure
  const right = props.comparison?.after?.structure
  if (left?.tableShape !== 'PRIMARY_KEY' || right?.tableShape !== 'PRIMARY_KEY')
    return []
  return [...new Set([...left.bands, ...right.bands].map(band => band.code))].map(code => ({
    code,
    before: left.bands.find(band => band.code === code),
    after: right.bands.find(band => band.code === code),
  }))
})
const labels: Record<string, string> = {
  totalFileCount: 'healthMetricTotalFileCount',
  totalFileSize: 'healthMetricTotalFileSize',
  maxSortedRunCount: 'healthMetricMaxSortedRunCount',
  reducibleFileCount: 'healthMetricReducibleFileCount',
  tombstoneRecordCount: 'healthMetricTombstoneRecordCount',
  deletionVectorRecordCount: 'healthMetricDeletionVectorRecordCount',
  deleteRecordCount: 'healthMetricDeleteRecordCount',
}
function value(key: string, raw?: string): string {
  if (raw == null || raw === 'N/A')
    return '—'
  return key === 'totalFileSize' ? bytesToSize(Number(raw)) : raw
}
function score(raw: number): string {
  return raw < 0 ? '—' : `${raw}/100`
}
</script>

<template>
  <section class="health-comparison">
    <h3>{{ t('healthComparisonTitle') }}</h3>
    <p v-if="!comparison">
      {{ t('healthComparisonEmpty') }}
    </p>
    <template v-else>
      <p>{{ comparison.optimizingType }} · {{ t('healthComparisonProcess') }} {{ comparison.processId }}<span v-if="comparison.completedAtMillis"> · {{ new Date(comparison.completedAtMillis).toLocaleString(locale) }}</span></p>
      <p v-if="comparison.status === 'PREPARING'" role="status">
        {{ t('healthComparisonPreparing') }}
      </p>
      <p v-else-if="comparison.status !== 'READY'" role="status">
        {{ t('healthComparisonUnavailable') }} {{ t(`healthComparisonReason_${comparison.reason || 'MISSING_INPUT'}`) }}
      </p>
      <template v-if="comparison.before || comparison.after">
        <div class="comparison-scroll">
          <table>
            <thead><tr><th>{{ t('healthMetrics') }}</th><th>{{ t('healthBefore') }}</th><th>{{ t('healthAfter') }}</th></tr></thead>
            <tbody>
              <tr><th>{{ t('healthSnapshotId') }}</th><td>{{ comparison.before?.snapshotId || '—' }}</td><td>{{ comparison.after?.snapshotId || '—' }}</td></tr>
              <tr><th>{{ t('healthScore') }}</th><td>{{ score(comparison.beforeScore) }}</td><td>{{ score(comparison.afterScore) }}</td></tr>
              <tr v-for="component in components" :key="component.code">
                <th>{{ t(componentLabels[component.code] || 'healthComponents') }}</th><td>{{ score(component.score) }}</td><td>{{ score(component.after) }}</td>
              </tr>
              <tr v-for="row in rows" :key="row.key">
                <th>{{ t(labels[row.key]) }}</th><td>{{ value(row.key, row.before) }}</td><td>{{ value(row.key, row.after) }}</td>
              </tr>
              <template v-for="level in levels" :key="level.code">
                <tr><th>{{ level.code }} · {{ t('healthMetricTotalFileCount') }}</th><td>{{ level.before?.fileCount ?? '—' }}</td><td>{{ level.after?.fileCount ?? '—' }}</td></tr>
                <tr><th>{{ level.code }} · {{ t('healthMetricTotalFileSize') }}</th><td>{{ value('totalFileSize', level.before?.fileSize) }}</td><td>{{ value('totalFileSize', level.after?.fileSize) }}</td></tr>
              </template>
            </tbody>
          </table>
        </div>
        <p v-if="comparison.status === 'READY'">
          {{ t('healthScoreChange') }}: {{ comparison.afterScore - comparison.beforeScore > 0 ? '+' : '' }}{{ comparison.afterScore - comparison.beforeScore }}
        </p>
      </template>
      <p v-if="comparison.concurrentChanges" class="comparison-warning">
        {{ t('healthComparisonConcurrent') }}
      </p>
      <p>{{ t('healthComparisonObservation') }}</p>
    </template>
  </section>
</template>

<style scoped>
.health-comparison { margin-top: 24px; padding-top: 16px; border-top: 1px solid #e8eaef; }
p { color: #667085; margin: 10px 0; }
.comparison-scroll { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
th, td { padding: 10px 8px; border-bottom: 1px solid #e8eaef; text-align: right; white-space: nowrap; }
th:first-child { text-align: left; }
.comparison-warning { color: #ad6800; }
</style>
