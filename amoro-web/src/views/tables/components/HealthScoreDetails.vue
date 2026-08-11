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
import { Modal as AModal, Progress as AProgress } from 'ant-design-vue'
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import type { IBaseDetailInfo, TableHealthComponent } from '@/types/common.type'

interface DisplayComponent extends TableHealthComponent {
  maxScore: number
}

const props = defineProps<{
  baseInfo: IBaseDetailInfo
}>()

const { t } = useI18n()
const modalVisible = ref(false)

const ICEBERG_TABLE_TYPES = new Set(['ICEBERG', 'MIXED_HIVE', 'MIXED_ICEBERG'])
const COMPONENT_LABEL_KEYS: Record<string, string> = {
  SMALL_FILE: 'smallFileScore',
  EQUALITY_DELETE: 'equalityDeleteScore',
  POSITIONAL_DELETE: 'positionalDeleteScore',
  FILE_ORGANIZATION: 'fileOrganizationScore',
  MATERIALIZED_DELETE: 'materializedDeleteScore',
  SORTED_RUN: 'sortedRunScore',
  FILE_SIZE_AUXILIARY: 'fileSizeAuxiliary',
  SNAPSHOT_ACTIVITY: 'snapshotActivityScore',
}
const COMPONENT_DESCRIPTION_KEYS: Record<string, string> = {
  SMALL_FILE: 'smallFileScoreDescription',
  EQUALITY_DELETE: 'equalityDeleteScoreDescription',
  POSITIONAL_DELETE: 'positionalDeleteScoreDescription',
  FILE_ORGANIZATION: 'fileOrganizationScoreDescription',
  MATERIALIZED_DELETE: 'materializedDeleteScoreDescription',
  SORTED_RUN: 'sortedRunScoreDescription',
  FILE_SIZE_AUXILIARY: 'fileSizeAuxiliaryDescription',
  SNAPSHOT_ACTIVITY: 'snapshotActivityScoreDescription',
}
const REASON_LABEL_KEYS: Record<string, string> = {
  UNSUPPORTED_BUCKET_MODE: 'healthReasonUnsupportedBucketMode',
  PK_CLUSTERING_OVERRIDE_UNSUPPORTED: 'healthReasonPkClusteringOverrideUnsupported',
  UNSUPPORTED_TABLE_SHAPE: 'healthReasonUnsupportedTableShape',
  EMPTY_TABLE: 'healthReasonEmptyTable',
  INVALID_SCORING_CONFIG: 'healthReasonInvalidScoringConfig',
  SNAPSHOT_SCAN_FAILED: 'healthReasonSnapshotScanFailed',
  DELETE_METADATA_INCOMPLETE: 'healthReasonDeleteMetadataIncomplete',
  SUCCESS_BASELINE_UNAVAILABLE: 'healthReasonSuccessBaselineUnavailable',
  SUCCESS_BASELINE_INVALID: 'healthReasonSuccessBaselineInvalid',
  KEY_DYNAMIC_OPTIMIZING_UNSUPPORTED: 'healthReasonKeyDynamicOptimizingUnsupported',
  KEY_DYNAMIC_LOCAL_INDEX_NOT_EVALUATED: 'healthReasonKeyDynamicLocalIndexNotEvaluated',
}
const METRIC_LABEL_KEYS: Record<string, string> = {
  tableShape: 'healthMetricTableShape',
  bucketMode: 'healthMetricBucketMode',
  totalFileCount: 'healthMetricTotalFileCount',
  totalFileSize: 'healthMetricTotalFileSize',
  averageFileSize: 'healthMetricAverageFileSize',
  targetFileSize: 'healthMetricTargetFileSize',
  smallFileBoundary: 'healthMetricSmallFileBoundary',
  smallFileCount: 'healthMetricSmallFileCount',
  smallFileSize: 'healthMetricSmallFileSize',
  undersizedFileCount: 'healthMetricUndersizedFileCount',
  undersizedFileSize: 'healthMetricUndersizedFileSize',
  reducibleFileCount: 'healthMetricReducibleFileCount',
  expectedOutputFileCount: 'healthMetricExpectedOutputFileCount',
  totalRecordCount: 'healthMetricTotalRecordCount',
  deleteRecordCount: 'healthMetricDeleteRecordCount',
  tombstoneRecordCount: 'healthMetricTombstoneRecordCount',
  deletionVectorRecordCount: 'healthMetricDeletionVectorRecordCount',
  effectiveUnitCount: 'healthMetricEffectiveUnitCount',
  activePartitionCount: 'healthMetricActivePartitionCount',
  maxSortedRunCount: 'healthMetricMaxSortedRunCount',
  compactionTrigger: 'healthMetricCompactionTrigger',
  stopTrigger: 'healthMetricStopTrigger',
  numLevels: 'healthMetricNumLevels',
  sortedRunDistribution: 'healthMetricSortedRunDistribution',
  worstBuckets: 'healthMetricWorstBuckets',
  compactionFileSize: 'healthMetricCompactionFileSize',
  baselineSnapshotId: 'healthMetricBaselineSnapshotId',
  baselineSnapshotTimeMillis: 'healthMetricBaselineSnapshotTime',
  latestSnapshotTimeMillis: 'healthMetricLatestSnapshotTime',
  newSnapshotCount: 'healthMetricNewSnapshotCount',
  snapshotTimeDistanceMillis: 'healthMetricSnapshotTimeDistance',
  timeThresholdMillis: 'healthMetricTimeThreshold',
  snapshotPressure: 'healthMetricSnapshotPressure',
  timePressure: 'healthMetricTimePressure',
  activityPressure: 'healthMetricActivityPressure',
}

const tableType = computed(() => (props.baseInfo.tableType || '').toUpperCase())
const details = computed(() => props.baseInfo.healthDetails)
const isIcebergCompatible = computed(() => ICEBERG_TABLE_TYPES.has(tableType.value))
const isPaimon = computed(() => tableType.value === 'PAIMON')
const canOpen = computed(() =>
  (props.baseInfo.healthScore != null && props.baseInfo.healthScore >= 0) || details.value != null,
)
const displayScore = computed(() => formatScore(props.baseInfo.healthScore))
const cardTitle = computed(() => {
  if (isIcebergCompatible.value)
    return t('icebergHealthScore')
  if (isPaimon.value && props.baseInfo.hasPrimaryKey)
    return t('paimonPrimaryKeyHealthScore')
  if (isPaimon.value)
    return t('paimonAppendHealthScore')
  return t('healthScore')
})
const scoreDescription = computed(() =>
  isIcebergCompatible.value ? t('healthScoreDescription') : t('paimonHealthScoreDescription'),
)

const legacyIcebergComponents = computed<DisplayComponent[]>(() => [
  legacyComponent('SMALL_FILE', props.baseInfo.smallFileScore, 40),
  legacyComponent('EQUALITY_DELETE', props.baseInfo.equalityDeleteScore, 40),
  legacyComponent('POSITIONAL_DELETE', props.baseInfo.positionalDeleteScore, 20),
])

const displayComponents = computed<DisplayComponent[]>(() => {
  if (details.value) {
    return details.value.components.map(component => ({
      ...component,
      maxScore: isIcebergCompatible.value ? (component.weight ?? 100) : 100,
    }))
  }
  return isIcebergCompatible.value ? legacyIcebergComponents.value : []
})

const healthMetrics = computed(() => metricEntries(details.value?.metrics))
const reasonCodes = computed(() => details.value?.reasonCodes ?? [])

function legacyComponent(code: string, score: number, weight: number): DisplayComponent {
  return {
    code,
    score,
    weight,
    combination: 'SUM',
    metrics: {},
    maxScore: weight,
  }
}

function formatScore(score: number | null | undefined): string {
  return score == null || score < 0 ? 'N/A' : String(score)
}

function scoreWithMax(score: number | null | undefined, maxScore: number): string {
  const formatted = formatScore(score)
  return formatted === 'N/A' ? formatted : `${formatted}/${maxScore}`
}

function scorePercent(score: number, maxScore: number): number {
  if (score < 0 || maxScore <= 0)
    return 0
  return Math.min(Math.max((score / maxScore) * 100, 0), 100)
}

function componentLabel(code: string): string {
  const key = COMPONENT_LABEL_KEYS[code]
  return key ? t(key) : code
}

function componentDescription(code: string): string {
  const key = COMPONENT_DESCRIPTION_KEYS[code]
  return key ? t(key) : t('healthComponentProvidedByBackend')
}

function reasonLabel(code: string): string {
  const key = REASON_LABEL_KEYS[code]
  return key ? t(key) : code
}

function metricLabel(code: string): string {
  const key = METRIC_LABEL_KEYS[code]
  return key ? t(key) : code
}

function metricEntries(metrics?: Record<string, string>): Array<[string, string]> {
  return Object.entries(metrics ?? {})
}

function showHealthScoreDetail() {
  if (canOpen.value)
    modalVisible.value = true
}
</script>

<template>
  <button
    type="button"
    class="health-score-trigger text-color"
    :class="{ 'clickable-score': canOpen }"
    :disabled="!canOpen"
    :aria-label="t('openHealthScoreDetails', { score: displayScore })"
    @click="showHealthScoreDetail"
  >
    {{ displayScore }}
  </button>

  <AModal
    v-model:open="modalVisible"
    :footer="null"
    class="health-score-modal"
    :title="t('healthScoreDetails')"
    width="min(800px, calc(100vw - 32px))"
    centered
  >
    <div class="health-score-modal-content">
      <section class="health-score-section" :aria-label="cardTitle">
        <div class="health-score-row">
          <strong class="module-info">{{ cardTitle }}</strong>
          <span class="module-score">{{ scoreWithMax(baseInfo.healthScore, 100) }}</span>
        </div>
        <AProgress
          v-if="baseInfo.healthScore >= 0"
          :percent="scorePercent(baseInfo.healthScore, 100)"
          :show-info="false"
          stroke-color="#7CB305"
          :stroke-width="8"
        />
        <p class="score-description">
          {{ scoreDescription }}
        </p>
        <p v-if="isIcebergCompatible" class="formula-row">
          {{ `${t('healthScore')} = ${t('smallFileScore')} + ${t('equalityDeleteScore')} + ${t('positionalDeleteScore')}` }}
        </p>
        <dl v-if="details" class="summary-metadata">
          <div>
            <dt>{{ t('healthFormulaVersion') }}</dt>
            <dd>{{ details.formulaVersion }}</dd>
          </div>
          <div v-if="details.snapshotId != null">
            <dt>{{ t('healthSnapshotId') }}</dt>
            <dd>{{ details.snapshotId }}</dd>
          </div>
          <div v-if="details.changeSnapshotId != null">
            <dt>{{ t('healthChangeSnapshotId') }}</dt>
            <dd>{{ details.changeSnapshotId }}</dd>
          </div>
          <div v-if="details.schemaId != null">
            <dt>{{ t('healthSchemaId') }}</dt>
            <dd>{{ details.schemaId }}</dd>
          </div>
        </dl>
      </section>

      <section v-if="reasonCodes.length" class="details-section" aria-live="polite">
        <h3>{{ t('healthReasons') }}</h3>
        <ul class="reason-list">
          <li v-for="reason in reasonCodes" :key="reason">
            {{ reasonLabel(reason) }}
          </li>
        </ul>
      </section>

      <section v-if="displayComponents.length" class="details-section">
        <h3>{{ t('healthComponents') }}</h3>
        <article
          v-for="component in displayComponents"
          :key="component.code"
          class="submodule-item"
        >
          <div class="submodule-row">
            <strong class="module-info">{{ componentLabel(component.code) }}</strong>
            <span class="module-score">
              {{ scoreWithMax(component.score, component.maxScore) }}
            </span>
          </div>
          <AProgress
            v-if="component.score >= 0"
            :percent="scorePercent(component.score, component.maxScore)"
            :show-info="false"
            stroke-color="#1890ff"
            :stroke-width="8"
          />
          <p class="score-description">
            {{ componentDescription(component.code) }}
          </p>
          <p v-if="component.weight != null || component.combination" class="component-rule">
            <span v-if="component.weight != null">
              {{ `${t('healthWeight')}: ${component.weight}` }}
            </span>
            <span v-if="component.combination">
              {{ `${t('healthCombination')}: ${component.combination}` }}
            </span>
          </p>
          <dl v-if="metricEntries(component.metrics).length" class="metric-list">
            <div v-for="([code, value]) in metricEntries(component.metrics)" :key="code">
              <dt>{{ metricLabel(code) }}</dt>
              <dd>{{ value }}</dd>
            </div>
          </dl>
        </article>
      </section>

      <section v-if="healthMetrics.length" class="details-section">
        <h3>{{ t('healthMetrics') }}</h3>
        <dl class="metric-list">
          <div v-for="([code, value]) in healthMetrics" :key="code">
            <dt>{{ metricLabel(code) }}</dt>
            <dd>{{ value }}</dd>
          </div>
        </dl>
      </section>

      <p
        v-if="!details && !isIcebergCompatible"
        class="empty-details"
        role="status"
      >
        {{ t('healthDetailsUnavailable') }}
      </p>
    </div>
  </AModal>
</template>

<style lang="less">
.health-score-trigger {
  appearance: none;
  margin: 0;
  padding: 0;
  border: 0;
  background: transparent;
  font: inherit;

  &:disabled {
    cursor: default;
  }

  &.clickable-score {
    color: #7CB305;
    cursor: pointer;
    text-decoration: underline;
    text-underline-offset: 2px;

    &:focus-visible {
      border-radius: 2px;
      outline: 2px solid #1890ff;
      outline-offset: 2px;
    }
  }
}

.health-score-modal {
  .ant-modal-body {
    max-height: calc(100vh - 160px);
    overflow-x: hidden;
    overflow-y: auto;
  }

  .health-score-modal-content {
    box-sizing: border-box;
    width: 100%;
    min-width: 0;
    color: #102048;
  }

  .health-score-section,
  .details-section {
    min-width: 0;
    margin-bottom: 16px;
  }

  .ant-progress-line {
    margin-inline-end: 0;
  }

  .health-score-row,
  .submodule-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    margin-bottom: 4px;
  }

  .module-info,
  .module-score {
    color: #102048;
    font-size: 14px;
  }

  .module-info {
    min-width: 0;
    overflow-wrap: anywhere;
  }

  .module-score {
    flex: none;
  }

  .score-description,
  .component-rule,
  .empty-details {
    margin: 6px 0;
    color: #4f5d75;
    font-size: 12px;
    line-height: 1.5;
    overflow-wrap: anywhere;
  }

  .formula-row {
    margin: 8px 0;
    padding: 6px 8px;
    overflow-wrap: anywhere;
    border-radius: 4px;
    background-color: #f5f7fa;
    color: #102048;
    font-family: monospace;
    font-size: 12px;
    text-align: center;
  }

  h3 {
    margin: 0 0 8px;
    color: #102048;
    font-size: 14px;
    line-height: 20px;
  }

  .submodule-item {
    padding: 10px 0;
    border-top: 1px solid #e8e8e8;

    &:last-child {
      padding-bottom: 0;
    }
  }

  .component-rule {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
  }

  .summary-metadata,
  .metric-list {
    margin: 8px 0 0;

    > div {
      display: grid;
      grid-template-columns: minmax(128px, 1fr) minmax(0, 2fr);
      gap: 12px;
      padding: 4px 0;
    }

    dt {
      color: #667085;
    }

    dd {
      min-width: 0;
      margin: 0;
      overflow-wrap: anywhere;
      color: #102048;
      text-align: right;
    }
  }

  .reason-list {
    margin: 0;
    padding-left: 20px;
    color: #4f5d75;
    font-size: 12px;
    line-height: 1.6;
    overflow-wrap: anywhere;
  }

  @media (max-width: 480px) {
    .summary-metadata > div,
    .metric-list > div {
      grid-template-columns: 1fr;
      gap: 2px;
    }

    .summary-metadata dd,
    .metric-list dd {
      text-align: left;
    }
  }
}
</style>
