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
import { QuestionFilled } from '@element-plus/icons-vue'
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import PaimonHealthStructure from './PaimonHealthStructure.vue'
import PaimonHealthComparison from './PaimonHealthComparison.vue'
import type { IBaseDetailInfo, TableHealthComponent } from '@/types/common.type'
import { bytesToSize, dateFormat } from '@/utils'

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
    return Number.isNaN(date.getTime()) ? value : dateFormat(number)
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

  <el-dialog
    v-model="modalVisible"
    class="health-score-modal"
    :title="t('healthScoreDetails')"
    width="min(800px, calc(100vw - 32px))"
    align-center
  >
    <div class="health-score-modal-content">
      <section class="health-score-section" :aria-label="cardTitle">
        <div class="score-overview">
          <div class="health-score-row">
            <strong class="module-info">{{ cardTitle }}</strong>
            <span class="module-score">{{ scoreWithMax(baseInfo.healthScore, 100) }}</span>
          </div>
          <el-progress
            v-if="baseInfo.healthScore >= 0"
            :percentage="scorePercent(baseInfo.healthScore, 100)"
            :show-text="false"
            :color="scoreColor"
            :stroke-width="8"
          />
          <p v-if="isPaimon && isPaimonV3 && baseInfo.healthScore >= 0" class="score-grade" :style="{ color: scoreColor }">
            {{ scoreGrade }}
          </p>
        </div>
        <div v-if="isPaimon" class="evaluation-status" role="status">
          <strong>{{ t(`healthStatus_${details?.evaluation?.status || (!details ? 'PENDING' : baseInfo.healthScore < 0 ? (details.structure ? 'PARTIAL' : 'FAILED') : 'LEGACY')}`) }}</strong>
          <p v-if="details?.evaluation?.stale">
            {{ t('healthHistoricalWarning') }}
          </p>
          <p v-if="details?.evaluation?.retryable">
            {{ t('healthRetryNotice') }}
          </p>
          <p v-if="details?.structure?.evaluatedAtMillis">
            {{ t('healthEvaluatedAt') }}: {{ dateFormat(details.structure.evaluatedAtMillis) }}
          </p>
          <p v-if="details?.evaluation?.attemptedAtMillis">
            {{ t('healthAttemptedAt') }}: {{ dateFormat(details.evaluation.attemptedAtMillis) }}
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
        <dl v-if="details && (!isPaimon || details.snapshotId != null || details.changeSnapshotId != null)" class="summary-metadata">
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
              <el-tooltip v-if="isPaimon" :content="componentDescription(component.code)" :trigger="['hover', 'focus']" popper-class="health-score-tooltip">
                <button type="button" class="health-help" :aria-label="t('healthMetricHelp', { metric: componentLabel(component.code) })">
                  <QuestionFilled class="amoro-icon" />
                </button>
              </el-tooltip>
            </div>
            <span class="module-score">
              {{ isPaimon && component.code === 'FILE_SIZE_AUXILIARY' ? t('healthNotScored') : scoreWithMax(component.score, component.maxScore) }}
            </span>
          </div>
          <el-progress
            v-if="component.score >= 0 && !(isPaimon && component.code === 'FILE_SIZE_AUXILIARY')"
            :percentage="scorePercent(component.score, component.maxScore)"
            :show-text="false"
            color="#1890ff"
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
                <el-tooltip v-if="isPaimon && metricDescription(code)" :content="metricDescription(code)" :trigger="['hover', 'focus']" popper-class="health-score-tooltip">
                  <button type="button" class="health-help" :aria-label="t('healthMetricHelp', { metric: metricLabel(code) })">
                    <QuestionFilled class="amoro-icon" />
                  </button>
                </el-tooltip>
              </dt>
              <dd>{{ metricValue(code, value) }}</dd>
            </div>
          </dl>
        </article>
      </section>

      <PaimonHealthStructure v-if="isPaimon && details?.structure" :structure="details.structure" />

      <PaimonHealthComparison v-if="isPaimon" :comparison="baseInfo.healthComparison" />

      <component :is="isPaimon ? 'details' : 'section'" v-if="healthMetrics.length" class="details-section health-metrics-section">
        <component :is="isPaimon ? 'summary' : 'h3'">
          {{ t('healthMetrics') }}
        </component>
        <dl class="metric-list">
          <div v-for="([code, value]) in healthMetrics" :key="code">
            <dt>
              {{ metricLabel(code) }}
              <el-tooltip v-if="isPaimon && metricDescription(code)" :content="metricDescription(code)" :trigger="['hover', 'focus']" popper-class="health-score-tooltip">
                <button type="button" class="health-help" :aria-label="t('healthMetricHelp', { metric: metricLabel(code) })">
                  <QuestionFilled class="amoro-icon" />
                </button>
              </el-tooltip>
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
  </el-dialog>
</template>

<style lang="less">
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

.health-score-modal.el-dialog {
  --health-dialog-gutter: 24px;

  display: flex;
  flex-direction: column;
  max-height: calc(100dvh - 32px);
  padding: 0;
  white-space: normal;

  .el-dialog__header {
    flex: none;
    margin: 0;
    padding: 16px var(--health-dialog-gutter);
    padding-inline-end: 48px;
    border-bottom: 1px solid #e8eaef;
  }

  .el-dialog__title {
    color: #102048;
    font-weight: 600;
  }

  .el-dialog__body {
    flex: 1 1 auto;
    min-height: 0;
    padding: 16px var(--health-dialog-gutter) 20px;
    overflow: auto;
    overscroll-behavior: contain;
  }

  .health-score-modal-content {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 12px;
    color: #102048;
    line-height: 1.5;
    overflow-wrap: anywhere;

    > * + * {
      padding-block-start: 12px;
      border-block-start: 1px solid #e8eaef;
    }
  }

  .health-score-section,
  .score-overview,
  .submodule-item {
    display: grid;
    min-width: 0;
    gap: 8px;
  }

  .health-score-section {
    gap: 12px;
  }

  .score-overview {
    gap: 4px;
  }

  .score-grade {
    font-size: 13px;
    font-weight: 600;
  }

  .evaluation-status {
    display: grid;
    gap: 2px;
    padding: 12px;
    border-inline-start: 3px solid #91caff;
    border-radius: 4px;
    background: #f5f7fa;
    font-size: 13px;

    strong {
      font-weight: 600;
    }
  }

  .el-progress {
    margin-inline-end: 0;
  }

  .health-score-row,
  .submodule-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .module-info,
  .module-score {
    color: #102048;
    font-size: 14px;
  }

  .module-info {
    min-width: 0;
    font-weight: 600;
  }

  .module-score {
    flex: none;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
  }

  .health-score-row .module-score {
    font-size: 24px;
    line-height: 1.2;
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
    margin: 0;
    color: #4f5d75;
    font-size: 13px;
  }

  .formula-row {
    margin: 0;
    padding: 6px 8px;
    border-radius: 4px;
    background-color: #f5f7fa;
    color: #102048;
    font-family: monospace;
    font-size: 12px;
    text-align: center;
  }

  .details-section > h3,
  .details-section > summary {
    font-size: 14px;
    font-weight: 600;
  }

  .details-section > h3 {
    margin-block-end: 8px;
  }

  .details-section > summary {
    padding-block: 4px;
    cursor: pointer;

    &:focus-visible {
      outline: 2px solid #1890ff;
      outline-offset: 2px;
    }
  }

  .submodule-item {
    padding-block: 8px;

    & + .submodule-item {
      border-top: 1px solid #e8eaef;
    }
  }

  .component-rule {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
  }

  .summary-metadata,
  .metric-list {
    margin: 0;

    > div {
      display: grid;
      grid-template-columns: minmax(0, 1fr) fit-content(45%);
      align-items: baseline;
      min-width: 0;
      gap: 8px;
      padding-block: 6px;
    }

    dt {
      color: #667085;
    }

    dd {
      min-width: 0;
      margin: 0;
      overflow-wrap: anywhere;
      color: #102048;
      text-align: end;
      font-variant-numeric: tabular-nums;
    }
  }

  .summary-metadata > div {
    padding-block: 2px;
  }

  .metric-list > div {
    border-block-end: 1px solid #eef0f2;
  }

  .health-metrics-section > .metric-list {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 300px), 1fr));
    column-gap: 24px;
    margin-block-start: 4px;
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
    --health-dialog-gutter: 16px;

    .health-score-row .module-score {
      font-size: 20px;
    }
  }
}
</style>
