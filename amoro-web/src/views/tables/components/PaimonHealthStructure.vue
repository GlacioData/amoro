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
import { QuestionCircleOutlined } from '@ant-design/icons-vue'
import { Tooltip as ATooltip } from 'ant-design-vue'
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { TableHealthBand, TableHealthStructure } from '@/types/common.type'
import { bytesToSize } from '@/utils'

const props = defineProps<{ structure: TableHealthStructure }>()
const { t } = useI18n()
const primaryKey = computed(() => props.structure.tableShape === 'PRIMARY_KEY')
const totalBytes = computed(() => props.structure.bands.reduce((sum, band) => sum + Number(band.fileSize), 0))
function size(value: string): string {
  return bytesToSize(Number(value))
}
function bandLabel(band: TableHealthBand): string {
  if (primaryKey.value)
    return band.code
  return band.upperBound == null ? `≥ ${size(band.lowerBound || '0')}` : `${size(band.lowerBound || '0')} – < ${size(band.upperBound)}`
}
function width(band: TableHealthBand): string {
  return `${totalBytes.value > 0 ? 100 * Number(band.fileSize) / totalBytes.value : 0}%`
}
function problemLabel(code: string): string {
  return code === 'SORTED_RUN'
    ? t('sortedRunScore')
    : code === 'FILE_ORGANIZATION'
      ? t('fileOrganizationScore')
      : code === 'MATERIALIZED_DELETE'
        ? t('materializedDeleteScore')
        : `${code.startsWith('SORTED_RUN') ? t('sortedRunScore') : t('fileOrganizationScore')} / ${t('materializedDeleteScore')}`
}
</script>

<template>
  <section class="paimon-structure">
    <h3>
      {{ primaryKey ? t('healthLsmTitle') : t('healthSizeDistribution') }}
      <ATooltip :title="primaryKey ? t('healthLsmHelp') : t('healthSizeHelp')" :trigger="['hover', 'focus']">
        <button type="button" class="health-help" :aria-label="t('healthMetricHelp', { metric: primaryKey ? t('healthLsmTitle') : t('healthSizeDistribution') })">
          <QuestionCircleOutlined />
        </button>
      </ATooltip>
    </h3>
    <p v-if="structure.availability !== 'COMPLETE'" role="status">
      {{ t('healthPartialStructure') }}
    </p>
    <div class="health-table-scroll">
      <table class="health-structure-table">
        <thead>
          <tr>
            <th>{{ primaryKey ? t('healthLevel') : t('healthSizeRange') }}</th><th>{{ t('healthMetricTotalFileCount') }}</th><th>{{ t('healthMetricTotalFileSize') }}</th><th v-if="primaryKey">
              Sorted Runs
            </th><th>{{ t('healthByteShare') }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="band in structure.bands" :key="band.code">
            <th scope="row">
              {{ bandLabel(band) }}
            </th><td>{{ band.fileCount }}</td><td>{{ size(band.fileSize) }}</td><td v-if="primaryKey">
              {{ band.sortedRuns }}
            </td>
            <td class="share-cell">
              <span class="share-bar" aria-hidden="true"><span :style="{ width: width(band) }" /></span>{{ totalBytes > 0 ? (100 * Number(band.fileSize) / totalBytes).toFixed(1) : '0.0' }}%
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <h3>{{ t('healthProblemUnits') }}</h3>
    <p :class="{ 'health-severe': structure.severeUnits !== '0' }" role="status">
      {{ t('healthProblemSummary', { affected: structure.affectedUnits, total: structure.totalUnits, severe: structure.severeUnits }) }}
    </p>
    <p v-if="!structure.problems.length">
      {{ t('healthNoProblems') }}
    </p>
    <template v-else>
      <p>{{ t('healthTopProblems', { shown: structure.problems.length, total: structure.affectedUnits }) }}</p>
      <article v-for="problem in structure.problems" :key="`${problem.partition}:${problem.bucket}`" class="health-problem">
        <div class="problem-title">
          <strong>{{ problem.partition || t('healthUnpartitioned') }}<span v-if="problem.bucket != null"> · Bucket {{ problem.bucket }}</span></strong><span :class="{ 'health-severe': problem.severity === 'SEVERE' }">{{ problem.severity === 'SEVERE' ? t('healthSevere') : t('healthAttention') }}</span>
        </div>
        <p>{{ problemLabel(problem.code) }} · {{ t('healthFileCountValue', { count: problem.fileCount }) }} · {{ size(problem.fileSize) }}<span v-if="primaryKey"> · {{ problem.sortedRuns }} Sorted Runs</span></p>
        <details v-if="primaryKey && problem.bands.length">
          <summary>{{ t('healthBucketLevels') }}</summary>
          <div class="health-table-scroll">
            <table class="health-structure-table">
              <thead><tr><th>{{ t('healthLevel') }}</th><th>{{ t('healthMetricTotalFileCount') }}</th><th>{{ t('healthMetricTotalFileSize') }}</th><th>Sorted Runs</th></tr></thead><tbody>
                <tr v-for="band in problem.bands" :key="band.code">
                  <th scope="row">
                    {{ band.code }}
                  </th><td>{{ band.fileCount }}</td><td>{{ size(band.fileSize) }}</td><td>{{ band.sortedRuns }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </details>
      </article>
    </template>
  </section>
</template>

<style scoped>
.paimon-structure { margin-top: 24px; }
h3 { display: flex; align-items: center; gap: 8px; margin: 20px 0 12px; }
.health-help { border: 0; background: none; color: #667085; cursor: help; }
.health-help:focus-visible, summary:focus-visible { outline: 2px solid #1677ff; outline-offset: 3px; }
.health-table-scroll { overflow-x: auto; }
.health-structure-table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
th, td { padding: 10px 8px; border-bottom: 1px solid #e8eaef; text-align: right; white-space: nowrap; }
th:first-child { text-align: left; }
thead { color: #667085; background: #fafbfc; }
.share-cell { min-width: 140px; }
.share-bar { display: inline-flex; width: 70px; height: 7px; background: #f0f2f5; margin-right: 8px; border-radius: 4px; overflow: hidden; }
.share-bar span { background: #1677ff; }
.health-problem { padding: 12px; margin: 10px 0; border: 1px solid #e8eaef; border-radius: 6px; }
.problem-title { display: flex; justify-content: space-between; gap: 12px; overflow-wrap: anywhere; }
.problem-title > strong { flex: 1; min-width: 0; }
.problem-title > span { flex-shrink: 0; white-space: nowrap; }
.health-severe { color: #cf1322; font-weight: 600; }
summary { cursor: pointer; color: #1677ff; }
p { color: #667085; margin: 8px 0; }
</style>
