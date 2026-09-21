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
import { Modal as AModal, Progress as AProgress, Tooltip as ATooltip } from 'ant-design-vue'
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import PaimonHealthStructure from './PaimonHealthStructure.vue'
import PaimonHealthComparison from './PaimonHealthComparison.vue'
import type { IBaseDetailInfo, TableHealthComponent } from '@/types/common.type'
import { bytesToSize } from '@/utils'

interface DisplayComponent extends TableHealthComponent {
  maxScore: number
}

const props = defineProps<{
  baseInfo: IBaseDetailInfo
}>()

const { t, locale } = useI18n()
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
  DELETE_METADATA_READ_FAILED: 'healthReasonDeleteReadFailed',
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

// These legacy activity fields are no longer evaluated, and worstBuckets is a diagnostic payload.
const PAIMON_HIDDEN_METRICS = new Set([
  'baselineSnapshotTimeMillis',
  'snapshotTimeDistanceMillis',
  'timeThresholdMillis',
  'timePressure',
  'activityPressure',
  'worstBuckets',
  'snapshotPressure',
  'sortedRunDistribution',
  'baselineSnapshotId',
  'newSnapshotCount',
  'tableShape',
  'bucketMode',
  'numLevels',
])
const FILE_SIZE_METRICS = new Set([
  'totalFileCount',
  'totalFileSize',
  'averageFileSize',
  'targetFileSize',
  'compactionFileSize',
  'smallFileCount',
  'smallFileSize',
])
const BYTE_METRICS = new Set([
  'totalFileSize',
  'averageFileSize',
  'targetFileSize',
  'compactionFileSize',
  'smallFileBoundary',
  'smallFileSize',
  'undersizedFileSize',
])

const tableType = computed(() => (props.baseInfo.tableType || '').toUpperCase())
const details = computed(() => props.baseInfo.healthDetails)
const isIcebergCompatible = computed(() => ICEBERG_TABLE_TYPES.has(tableType.value))
const isPaimon = computed(() => tableType.value === 'PAIMON')
const isPaimonV3 = computed(() => !!details.value?.formulaVersion?.includes('health-v3'))
const canOpen = computed(() =>
  isPaimon.value || (props.baseInfo.healthScore != null && props.baseInfo.healthScore >= 0) || details.value != null,
)
const isHistorical = computed(() => isPaimon.value && props.baseInfo.healthScore >= 0
  && !!details.value?.evaluation?.stale)
const displayScore = computed(() => isHistorical.value
  ? t('historicalHealthScore', { score: formatScore(props.baseInfo.healthScore) })
  : formatScore(props.baseInfo.healthScore))
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
  isIcebergCompatible.value ? t('healthScoreDescription') : t(isPaimonV3.value ? 'paimonHealthScoreDescription' : 'healthComponentProvidedByBackend'),
)

const legacyIcebergComponents = computed<DisplayComponent[]>(() => [
  legacyComponent('SMALL_FILE', props.baseInfo.smallFileScore, 40),
  legacyComponent('EQUALITY_DELETE', props.baseInfo.equalityDeleteScore, 40),
  legacyComponent('POSITIONAL_DELETE', props.baseInfo.positionalDeleteScore, 20),
])

const displayComponents = computed<DisplayComponent[]>(() => {
  if (details.value) {
    return details.value.components.filter(component => !isPaimon.value
      || (component.code !== 'SNAPSHOT_ACTIVITY' && !(details.value?.structure && component.code === 'FILE_SIZE_AUXILIARY'))).map(component => ({
      ...component,
      maxScore: isIcebergCompatible.value ? (component.weight ?? 100) : 100,
    }))
  }
  return isIcebergCompatible.value ? legacyIcebergComponents.value : []
})

const hasFileSizeComponent = computed(() =>
  isPaimon.value && displayComponents.value.some(component => component.code === 'FILE_SIZE_AUXILIARY'),
)
const healthMetrics = computed(() => metricEntries(details.value?.metrics).filter(([code]) =>
  !hasFileSizeComponent.value || !FILE_SIZE_METRICS.has(code),
))
const reasonCodes = computed(() => (details.value?.evaluation?.reasonCodes ?? details.value?.reasonCodes ?? [])
  .filter(code => !isPaimon.value || !['SUCCESS_BASELINE_UNAVAILABLE', 'SUCCESS_BASELINE_INVALID'].includes(code)))
const scoreColor = computed(() => !isPaimon.value
  ? '#7CB305'
  : details.value?.evaluation?.stale ? '#8c8c8c' : props.baseInfo.healthScore >= 80 ? '#389e0d' : props.baseInfo.healthScore >= 60 ? '#d48806' : '#cf1322')
const scoreGrade = computed(() => details.value?.evaluation?.stale
  ? t('healthStatus_STALE')
  : props.baseInfo.healthScore < 0
    ? t('healthStatus_PENDING')
    : props.baseInfo.healthScore >= 80
      ? t('healthGradeHealthy')
      : props.baseInfo.healthScore >= 60 ? t('healthGradeAttention') : t('healthGradeOptimize'))
function dateTime(value: number): string {
  return new Date(value).toLocaleString(locale.value)
}

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
  if (isPaimon.value && !isPaimonV3.value)
    return t('healthComponentProvidedByBackend')
  if (isPaimon.value && code === 'MATERIALIZED_DELETE' && !props.baseInfo.hasPrimaryKey)
    return t('appendMaterializedDeleteScoreDescription')
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
  return Object.entries(metrics ?? {}).filter(([code]) =>
    !isPaimon.value || !PAIMON_HIDDEN_METRICS.has(code),
  )
}

function componentMetrics(component: TableHealthComponent): Array<[string, string]> {
  if (isPaimon.value && component.code === 'FILE_SIZE_AUXILIARY') {
    return metricEntries({ ...details.value?.metrics, ...component.metrics })
      .filter(([code]) => FILE_SIZE_METRICS.has(code))
  }
  return metricEntries(component.metrics)
}

function metricDescription(code: string): string {
  return METRIC_LABEL_KEYS[code] ? t(`${METRIC_LABEL_KEYS[code]}Description`) : ''
}

function metricValue(code: string, value: string): string {
  if (!isPaimon.value || value === 'N/A')
    return value
  const number = Number(value)
  if (!Number.isFinite(number) || number < 0)
    return value
  if (BYTE_METRICS.has(code))
    return number === 0 ? '0 B' : bytesToSize(number)
  if (code === 'snapshotPressure')
    return `${Math.round(number * 100)}%`
  if (code === 'latestSnapshotTimeMillis') {
    const date = new Date(number)
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString(locale.value)
  }
  return value
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
    :style="isPaimon ? { color: baseInfo.healthScore < 0 ? '#667085' : scoreColor } : undefined"
    :aria-label="t(isHistorical ? 'openHistoricalHealthScoreDetails' : 'openHealthScoreDetails', { score: displayScore })"
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
          :stroke-color="scoreColor"
          :stroke-width="8"
        />
        <p v-if="isPaimon && isPaimonV3 && baseInfo.healthScore >= 0" :style="{ color: scoreColor }">
          {{ scoreGrade }}
        </p>
        <div v-if="isPaimon" class="evaluation-status" role="status">
          <strong>{{ t(`healthStatus_${details?.evaluation?.status || (!details ? 'PENDING' : baseInfo.healthScore < 0 ? (details.structure ? 'PARTIAL' : 'FAILED') : 'LEGACY')}`) }}</strong>
          <p v-if="details?.evaluation?.stale">
            {{ t('healthHistoricalWarning') }}
          </p>
          <p v-if="details?.evaluation?.retryable">
            {{ t('healthRetryNotice') }}
          </p>
          <p v-if="details?.structure?.evaluatedAtMillis">
            {{ t('healthEvaluatedAt') }}: {{ dateTime(details.structure.evaluatedAtMillis) }}
          </p>
          <p v-if="details?.evaluation?.attemptedAtMillis">
            {{ t('healthAttemptedAt') }}: {{ dateTime(details.evaluation.attemptedAtMillis) }}
          </p>
          <p v-if="details?.evaluation?.currentSnapshotId">
            {{ t('healthCurrentSnapshot') }}: {{ details.evaluation.currentSnapshotId }}
          </p>
        </div>
        <p class="score-description">
          {{ scoreDescription }}
        </p>
        <p v-if="isIcebergCompatible" class="formula-row">
          {{ `${t('healthScore')} = ${t('smallFileScore')} + ${t('equalityDeleteScore')} + ${t('positionalDeleteScore')}` }}
        </p>
        <dl v-if="details" class="summary-metadata">
          <div v-if="!isPaimon">
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
          <div v-if="!isPaimon && details.schemaId != null">
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
            <div class="component-heading">
              <strong class="module-info">{{ componentLabel(component.code) }}</strong>
              <ATooltip v-if="isPaimon" :title="componentDescription(component.code)" :trigger="['hover', 'focus']" overlay-class-name="health-score-tooltip">
                <button type="button" class="health-help" :aria-label="t('healthMetricHelp', { metric: componentLabel(component.code) })">
                  <QuestionCircleOutlined />
                </button>
              </ATooltip>
            </div>
            <span class="module-score">
              {{ isPaimon && component.code === 'FILE_SIZE_AUXILIARY' ? t('healthNotScored') : scoreWithMax(component.score, component.maxScore) }}
            </span>
          </div>
          <AProgress
            v-if="component.score >= 0 && !(isPaimon && component.code === 'FILE_SIZE_AUXILIARY')"
            :percent="scorePercent(component.score, component.maxScore)"
            :show-info="false"
            stroke-color="#1890ff"
            :stroke-width="8"
          />
          <p v-if="!isPaimon" class="score-description">
            {{ componentDescription(component.code) }}
          </p>
          <p v-if="!isPaimon && (component.weight != null || component.combination)" class="component-rule">
            <span v-if="component.weight != null">
              {{ `${t('healthWeight')}: ${component.weight}` }}
            </span>
            <span v-if="component.combination">
              {{ `${t('healthCombination')}: ${component.combination}` }}
            </span>
          </p>
          <dl v-if="componentMetrics(component).length" class="metric-list">
            <div v-for="([code, value]) in componentMetrics(component)" :key="code">
              <dt>
                {{ metricLabel(code) }}
                <ATooltip v-if="isPaimon && metricDescription(code)" :title="metricDescription(code)" :trigger="['hover', 'focus']" overlay-class-name="health-score-tooltip">
                  <button type="button" class="health-help" :aria-label="t('healthMetricHelp', { metric: metricLabel(code) })">
                    <QuestionCircleOutlined />
                  </button>
                </ATooltip>
              </dt>
              <dd>{{ metricValue(code, value) }}</dd>
            </div>
          </dl>
        </article>
      </section>

      <PaimonHealthStructure v-if="isPaimon && details?.structure" :structure="details.structure" />

      <PaimonHealthComparison v-if="isPaimon" :comparison="baseInfo.healthComparison" />

      <component :is="isPaimon ? 'details' : 'section'" v-if="healthMetrics.length" class="details-section">
        <component :is="isPaimon ? 'summary' : 'h3'">
          {{ t('healthMetrics') }}
        </component>
        <dl class="metric-list">
          <div v-for="([code, value]) in healthMetrics" :key="code">
            <dt>
              {{ metricLabel(code) }}
              <ATooltip v-if="isPaimon && metricDescription(code)" :title="metricDescription(code)" :trigger="['hover', 'focus']" overlay-class-name="health-score-tooltip">
                <button type="button" class="health-help" :aria-label="t('healthMetricHelp', { metric: metricLabel(code) })">
                  <QuestionCircleOutlined />
                </button>
              </ATooltip>
            </dt>
            <dd>{{ metricValue(code, value) }}</dd>
          </div>
        </dl>
      </component>

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
.evaluation-status { margin: 12px 0; padding: 12px; background: #fafbfc; border-left: 3px solid #91caff; }
.health-score-tooltip {
  max-width: min(360px, calc(100vw - 32px));
}

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

  .component-heading {
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
  }

  .health-help {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex: none;
    padding: 3px;
    border: 0;
    background: transparent;
    color: #667085;
    font-size: 14px;
    vertical-align: middle;
    cursor: help;

    &:focus-visible {
      border-radius: 2px;
      outline: 2px solid #1890ff;
      outline-offset: 2px;
    }
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
