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
import { computed, onBeforeUnmount, reactive, shallowReactive } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute, useRouter } from 'vue-router'
import { QuestionCircleOutlined } from '@ant-design/icons-vue'
import type { ColumnProps } from 'ant-design-vue/es/table'
import { requestEpoch, snapshotTableKey } from './snapshot-page-state'
import { fileStatisticsPresentation } from './file-statistics-view'
import type { DetailColumnItem, IBaseDetailInfo, IColumns, IMap, PartitionColumnItem, TableFileStatistics } from '@/types/common.type'
import { getTableDetail } from '@/services/table.service'
import { dateFormat } from '@/utils'

const emit = defineEmits<{
  (e: 'initializationError', message: string): void
  (e: 'setBaseDetailInfo', data: IBaseDetailInfo): void
  (e: 'tableNotFound', info: { catalog: string, db: string, table: string }): void
}>()
const { t } = useI18n()
const route = useRoute()
const router = useRouter()

const requests = requestEpoch()
let loadingIdentity = ''
const tableIdentity = () => snapshotTableKey(route.query)
onBeforeUnmount(() => requests.invalidate())

const STORAGE_TABLE_KEY = 'easylake-menu-catalog-db-table'

const params = computed(() => {
  return {
    ...route.query,
  }
})

const commonMetricMap = {
  fileCount: 'File Count',
  totalSize: 'Total Size',
  averageFileSize: 'Average File Size',
  lastCommitTime: 'Last Commit Time',
}

const baseMetricsMap: IMap<string | number> = {
  ...commonMetricMap,
  baseWatermark: 'Base Watermark',
}
const changeMetricsMap: IMap<string | number> = {
  ...commonMetricMap,
  tableWatermark: 'Table Watermark',
}

const state = reactive({
  detailLoading: false,
  detailRefreshFailed: false,
  fileStatistics: null as TableFileStatistics | null,
  baseDetailInfo: {
    boundedSnapshotRead: null,
    optimizingStatus: '',
    records: '',
    tableType: '',
    tableName: '',
    createTime: '',
    size: '',
    file: '',
    averageFile: '',
    tableFormat: '',
    hasPartition: false, // Whether there is a partition, if there is no partition, the file list will be displayed
    hasPrimaryKey: false,
    healthScore: -1,
    smallFileScore: -1,
    equalityDeleteScore: -1,
    positionalDeleteScore: -1,
    healthDetails: null,
    comment: '',
  } as IBaseDetailInfo,
  pkList: [] as DetailColumnItem[],
  partitionColumnList: [] as PartitionColumnItem[],
  properties: [] as IMap<string>[],
  changeMetrics: [] as IMap<string | number>[],
  baseMetrics: [] as IMap<string | number>[],
  schema: [] as DetailColumnItem[],
})

const fileStatisticsView = computed(() => fileStatisticsPresentation(
  state.baseDetailInfo.tableFormat || state.baseDetailInfo.tableType,
  state.fileStatistics,
  state.baseDetailInfo.healthDetails?.evaluation,
))
const fileStatisticsNotices = computed(() => fileStatisticsView.value?.reasons.filter(key => [
  'fileStatisticsCollectionDisabled',
  'fileStatisticsNotManaged',
  'fileStatisticsNoValidStatistics',
  'fileStatisticsHealthUnavailable',
].includes(key)) ?? [])

async function getTableDetails() {
  const identity = tableIdentity()
  if (state.detailLoading && loadingIdentity === identity) {
    return
  }
  const requestParams = { ...params.value }
  const { catalog, db, table } = requestParams
  if (!catalog || !db || !table) {
    return
  }
  const ticket = requests.begin()
  loadingIdentity = identity
  const isCurrent = () => requests.current(ticket) && tableIdentity() === identity
  // Vue 3.5 async watchers need stale-result guards even when cancellation cannot stop server IO.
  // https://vuejs.org/guide/essentials/watchers.html#side-effect-cleanup
  try {
    state.detailLoading = true
    const result = await getTableDetail({
      ...requestParams,
    })
    if (!isCurrent())
      return
    if (typeof result.boundedSnapshotRead !== 'boolean')
      throw new Error(t('snapshotVersionMismatch'))
    emit('initializationError', '')
    state.detailRefreshFailed = false
    state.fileStatistics = result.fileStatistics ?? null
    const { pkList = [], tableType, partitionColumnList = [], properties, changeMetrics, schema, createTime, tableIdentifier, baseMetrics, tableSummary, comment } = result
    const primaryKeys = pkList || []
    state.baseDetailInfo = {
      ...tableSummary,
      boundedSnapshotRead: result.boundedSnapshotRead,
      tableType,
      tableName: `${tableIdentifier?.catalog || ''}.${tableIdentifier?.database || ''}.${tableIdentifier?.tableName || ''}`,
      createTime: createTime ? dateFormat(createTime) : '',
      hasPartition: !!(partitionColumnList?.length),
      hasPrimaryKey: primaryKeys.length > 0,
      healthDetails: tableSummary?.healthDetails ?? null,
      comment: comment || '',
    }

    state.pkList = primaryKeys
    state.partitionColumnList = partitionColumnList || []
    state.schema = schema || []

    state.changeMetrics = Object.keys(changeMetricsMap || {}).map((key) => {
      return {
        metric: changeMetricsMap[key],
        value: key === 'lastCommitTime' || key === 'tableWatermark' ? ((changeMetrics || {})[key] ? dateFormat((changeMetrics || {})[key]) : '') : (changeMetrics || {})[key],
      }
    }).filter(ele => ele.value)

    state.baseMetrics = Object.keys(baseMetricsMap || {}).map((key) => {
      return {
        metric: baseMetricsMap[key],
        value: key === 'lastCommitTime' || key === 'baseWatermark'
          ? ((baseMetrics || {})[key] ? dateFormat((baseMetrics || {})[key]) : '—')
          : (key === 'fileCount' && fileStatisticsView.value && state.fileStatistics
              ? (fileStatisticsView.value.count ?? '—')
              : ((baseMetrics || {})[key] ?? '—')),
      }
    })
    state.properties = Object.keys(properties || {}).map((key) => {
      return {
        key,
        value: properties[key],
      }
    })
    setBaseDetailInfo()
  }
  catch (error) {
    if (!isCurrent())
      return
    state.detailRefreshFailed = true
    emit('initializationError', (error as Error)?.message || t('snapshotInitializationFailed'))
    const errorMessage = (error as Error)?.message || ''
    const isNotFoundError = /not exist|not found/i.test(errorMessage)

    if (isNotFoundError) {
      localStorage.removeItem(STORAGE_TABLE_KEY)

      emit('tableNotFound', {
        catalog: catalog as string,
        db: db as string,
        table: table as string,
      })

      router.replace({ path: '/tables', query: {} })
    }
  }
  finally {
    if (isCurrent())
      state.detailLoading = false
  }

  function setBaseDetailInfo() {
    emit('setBaseDetailInfo', state.baseDetailInfo)
  }
}

defineExpose({ getTableDetails })

const primaryColumns: ColumnProps[] = shallowReactive([
  { title: t('field'), dataIndex: 'field', width: '30%' },
  { title: t('type'), dataIndex: 'type', width: '20%' },
  { title: t('required'), dataIndex: 'required', width: '20%', customRender: text => String(text?.value) },
  { title: t('description'), dataIndex: 'comment', ellipsis: true },
])
const partitionColumns: IColumns[] = shallowReactive([
  { title: t('field'), dataIndex: 'field', width: '30%' },
  { title: t('sourceField'), dataIndex: 'sourceField', width: '30%' },
  { title: t('transform'), dataIndex: 'transform', ellipsis: true },
])
const metricsColumns: IColumns[] = shallowReactive([
  { title: t('metric'), dataIndex: 'metric', width: '50%', ellipsis: true },
  { title: t('value'), dataIndex: 'value', ellipsis: true },
])
const propertiesColumns: IColumns[] = shallowReactive([
  { title: t('key'), dataIndex: 'key', width: '50%', ellipsis: true },
  { title: t('value'), dataIndex: 'value', ellipsis: true },
])
</script>

<template>
  <div class="table-detail g-flex">
    <div class="left-content">
      <div v-if="state.pkList && state.pkList.length" class="table-attrs">
        <p class="attr-title">
          {{ $t('primaryKey') }}
        </p>
        <a-table
          row-key="field"
          :columns="primaryColumns"
          :data-source="state.pkList"
          :pagination="false"
        />
      </div>
      <div v-if="state.partitionColumnList && state.partitionColumnList.length" class="table-attrs">
        <p class="attr-title">
          {{ $t('partitionKey') }}
        </p>
        <a-table
          row-key="field"
          :columns="partitionColumns"
          :data-source="state.partitionColumnList"
          :pagination="false"
        />
      </div>
      <div class="table-attrs">
        <p class="attr-title">
          {{ $t('schema') }}
        </p>
        <a-table
          row-key="field"
          :columns="primaryColumns"
          :data-source="state.schema"
          :pagination="false"
        />
      </div>
    </div>
    <div class="right-content">
      <div v-if="state.changeMetrics && state.changeMetrics.length" class="table-attrs">
        <p class="attr-title">
          {{ $t('changeTableMetrics') }}
        </p>
        <a-table
          :columns="metricsColumns"
          :data-source="state.changeMetrics"
          :pagination="false"
        />
      </div>
      <div class="table-attrs">
        <p class="attr-title">
          {{ $t('baseTableMetrics') }}
        </p>
        <a-table
          :columns="metricsColumns"
          :data-source="state.baseMetrics"
          :pagination="false"
        />
        <div v-if="fileStatisticsView" class="file-statistics-note" role="status" aria-live="polite" data-testid="file-statistics">
          <div class="file-statistics-header">
            <span
              class="file-statistics-status"
              :class="{
                'is-current': state.fileStatistics?.status === 'CURRENT',
                'is-awaiting': state.fileStatistics?.status === 'STALE' || state.fileStatistics?.status === 'PENDING',
              }"
            >{{ $t(fileStatisticsView.statusKey) }}</span>
            <a-tooltip :trigger="['hover', 'focus']">
              <template #title>
                <p>{{ $t('fileStatisticsScope') }}</p>
                <p v-if="fileStatisticsView.evaluatedAtMillis">
                  {{ $t('fileStatisticsAsOf', { time: dateFormat(fileStatisticsView.evaluatedAtMillis) }) }}
                </p>
                <p>{{ $t('fileStatisticsSourceSnapshot') }}: {{ fileStatisticsView.sourceSnapshotId ?? '—' }}</p>
                <p>{{ $t('fileStatisticsObservedSnapshot') }}: {{ fileStatisticsView.observedSnapshotId ?? '—' }}</p>
                <p v-for="reason in fileStatisticsView.reasons" :key="reason">
                  {{ $t(reason) }}
                </p>
              </template>
              <button type="button" class="file-statistics-explanation" :aria-label="$t('fileStatisticsExplanation')">
                <QuestionCircleOutlined />
              </button>
            </a-tooltip>
          </div>
          <p v-for="notice in fileStatisticsNotices" :key="notice">
            {{ $t(notice) }}
          </p>
          <p v-if="fileStatisticsView.attemptedAtMillis">
            {{ $t('fileStatisticsFailedAt', { time: dateFormat(fileStatisticsView.attemptedAtMillis) }) }}
          </p>
          <p v-if="state.detailRefreshFailed">
            {{ $t('fileStatisticsRefreshFailed') }}
          </p>
        </div>
      </div>
      <div class="table-attrs">
        <p class="attr-title">
          {{ $t('properties') }}
        </p>
        <a-table
          :columns="propertiesColumns"
          :data-source="state.properties"
          :pagination="false"
        />
      </div>
    </div>
    <u-loading v-if="state.detailLoading" />
  </div>
</template>

<style lang="less" scoped>
.file-statistics-note {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin-top: 12px;
  font-size: 12px;
  line-height: 1.7;

  p { margin: 0; }
}
.file-statistics-status {
  font-weight: 500;

  &.is-current { color: #237804; }
  &.is-awaiting { color: #ad6800; }
}
.file-statistics-explanation {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  margin-left: 4px;
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

.table-detail {
  display: flex;
  column-gap: 24px;

  .left-content,
  .right-content {
    padding: 0 0 12px;
    flex-shrink: 0;
    flex-direction: column;
  }

  .left-content {
    flex: 2;
  }

  .right-content {
    flex: 1;
  }

  .table-attrs {
    margin-top: 16px;
  }

  .attr-title {
    font-size: 16px;
    line-height: 24px;
    font-weight: bold;
    color: #102048;
    padding-bottom: 12px;
  }
}
</style>
