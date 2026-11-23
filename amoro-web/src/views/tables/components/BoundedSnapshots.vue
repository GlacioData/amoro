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

<script lang="ts" setup>
import { CaretBottom, CaretRight } from '@element-plus/icons-vue'
import { computed, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import axios from 'axios'
import Selector from './Selector.vue'
import { cursorPages, requestEpoch, snapshotRowKey } from './snapshot-page-state'
import { snapshotChartData } from './snapshot-chart-data'
import SnapshotCharts from './SnapshotCharts.vue'
import DashboardPagination from '@/components/DashboardPagination.vue'
import { tableColumnProps } from '@/utils/table'
import { usePagination } from '@/hooks/usePagination'
import { getSnapshotDetailPage, getSnapshots } from '@/services/table.service'
import type { BreadcrumbSnapshotItem, SnapshotItem, SnapshotItemSummary } from '@/types/common.type'
import { dateFormat } from '@/utils'
import { generateLineChartOption } from '@/utils/chart'

const { t } = useI18n()
const route = useRoute()
const router = useRouter()
const selectedRefType = ref<string | undefined>()
const fixedTag = ref(false)
const selectedScope = reactive({ kind: 'BRANCH', branch: 'main' as string | undefined })
// The parent keys this component by table identity; no state is shared between tables.
const source = {
  catalog: String(route.query.catalog),
  db: String(route.query.db),
  table: String(route.query.table),
}
const requests = requestEpoch()
const rows = ref<SnapshotItem[]>([])
const detailRows = ref<(BreadcrumbSnapshotItem & { rowKey: string })[]>([])
const pagination = reactive({ ...usePagination(), pageSizeOptions: ['25'], showSizeChanger: false })
const cursors = reactive(cursorPages())
const selectedSnapshot = ref<string | null>(null)
const selectedRef = ref('main')
const operation = ref('all')
const consumerSelected = ref(false)
const loading = ref(false)
const selectorLoading = ref(false)
const error = ref('')
const showCharts = ref(false)
let pageRenderId = 0
let retry: () => void = () => {}

const columns = computed(() => [
  { title: t('snapshotId'), dataIndex: 'snapshotId', minWidth: 112, ellipsis: true },
  { title: t('operation'), dataIndex: 'operation', minWidth: 112 },
  { title: t('records'), dataIndex: 'records', minWidth: 96 },
  { title: t('fileCount'), dataIndex: 'fileCount', minWidth: 112 },
  { title: t('commitTime'), dataIndex: 'commitTime', minWidth: 164 },
])
const detailColumns = computed(() => [
  { title: t('operation'), dataIndex: 'operation', width: 120, ellipsis: true },
  { title: t('file'), dataIndex: 'file', minWidth: 240, ellipsis: true },
  { title: t('partition'), dataIndex: 'partition', width: 120 },
  { title: t('fileType'), dataIndex: 'fileType', width: 120, ellipsis: true },
  { title: t('size'), dataIndex: 'size', width: 120 },
  { title: t('commitTime'), dataIndex: 'commitTime', width: 200, ellipsis: true },
  { title: t('path'), dataIndex: 'path', minWidth: 280, ellipsis: true },
])
const recordChart = computed(() => {
  const data: Record<string, SnapshotItemSummary> = {}
  rows.value.forEach((row) => {
    data[row.commitTime] = row.recordsSummaryForChart || {}
  })
  return generateLineChartOption(t('recordChartTitle'), snapshotChartData(data))
})
// Vue 3 caches this projection until current-page rows change; no additional request.
// Source: https://vuejs.org/guide/essentials/computed.html#computed-caching-vs-methods
const fileChart = computed(() => {
  const data: Record<string, SnapshotItemSummary> = {}
  rows.value.forEach((row) => {
    data[row.commitTime] = row.filesSummaryForChart || {}
  })
  return generateLineChartOption(t('fileChangeChartTitle'), snapshotChartData(data))
})

function errorMessage(failure: unknown) {
  if (axios.isAxiosError(failure) && typeof failure.response?.data?.message === 'string')
    return failure.response.data.message
  return failure instanceof Error ? failure.message : t('snapshotReadFailed')
}

function clearDetail() {
  requests.invalidate()
  selectedSnapshot.value = null
  detailRows.value = []
  cursors.reset()
  loading.value = false
  error.value = ''
  retry = () => {}
}

function selectRef(selection: { ref: string, operation: string, refType?: string, targetId?: string }) {
  clearDetail()
  rows.value = []
  consumerSelected.value = false
  selectedRef.value = selection.ref
  selectedScope.kind = 'BRANCH'
  selectedScope.branch = 'main'
  selectedRefType.value = selection.refType
  if (selection.refType === 'TAG' && selection.targetId) {
    selectedSnapshot.value = selection.targetId
    void loadDetail(0, null)
    return
  }
  operation.value = selection.operation
  pagination.current = 1
  pagination.total = 0
  void loadList(1)
}

function selectConsumer(selection: { amoroCurrentSnapshotsItem: SnapshotItem | null }) {
  if (!selection.amoroCurrentSnapshotsItem)
    return
  clearDetail()
  // Paimon 1.4.2 ConsumerManager is main-scoped here; consumerId is not a branch/tag.
  // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java
  selectedRef.value = 'main'
  consumerSelected.value = true
  rows.value = [{ ...selection.amoroCurrentSnapshotsItem }]
  pagination.current = 1
  pagination.total = 1
}

async function loadList(page: number) {
  if (loading.value || selectorLoading.value)
    return
  const ticket = requests.begin()
  loading.value = true
  error.value = ''
  retry = () => {
    void loadList(page)
  }
  try {
    const result = await getSnapshots({ ...source, ref: selectedRef.value, operation: operation.value, page, pageSize: 25 }, false)
    if (!requests.current(ticket))
      return
    rows.value = result.list
    pagination.current = page
    pagination.total = result.total
  }
  catch (failure) {
    if (requests.current(ticket))
      error.value = errorMessage(failure)
  }
  finally {
    if (requests.current(ticket))
      loading.value = false
  }
}

async function loadDetail(index: number, cursor: string | null) {
  if (loading.value || selectorLoading.value || selectedSnapshot.value == null)
    return
  const ticket = requests.begin()
  loading.value = true
  error.value = ''
  retry = () => {
    void loadDetail(index, cursor)
  }
  try {
    const result = await getSnapshotDetailPage({
      ...source,
      snapshotId: selectedSnapshot.value,
      ref: selectedRef.value,
      refType: selectedRefType.value,
      ...(selectedRefType.value === 'TAG' ? { scopeKind: selectedScope.kind, branch: selectedScope.branch } : {}),
      pageSize: 25,
      ...(cursor === null ? {} : { cursor }),
    })
    if (!requests.current(ticket))
      return
    if (!Array.isArray(result.list) || !Object.hasOwn(result, 'nextCursor')
      || (result.nextCursor !== null && typeof result.nextCursor !== 'string')) {
      throw new Error(t('snapshotVersionMismatch'))
    }
    const renderId = ++pageRenderId
    detailRows.value = result.list.map((row: BreadcrumbSnapshotItem, index: number) => ({
      ...row,
      rowKey: snapshotRowKey(renderId, index),
    }))
    cursors.commit(index, cursor, result.nextCursor)
  }
  catch (failure) {
    if (requests.current(ticket))
      error.value = errorMessage(failure)
  }
  finally {
    if (requests.current(ticket))
      loading.value = false
  }
}

function openSnapshot(id: string) {
  if (loading.value || selectorLoading.value)
    return
  clearDetail()
  selectedSnapshot.value = id
  void loadDetail(0, null)
}

// Restore the complete Tag coordinate before starting any main-branch request.
// Vue 3.5: https://vuejs.org/guide/essentials/watchers.html#side-effect-cleanup
watch(() => [route.query.snapshotId, route.query.refType, route.query.ref, route.query.scopeKind, route.query.branch], () => {
  if (route.query.refType === 'TAG' && typeof route.query.snapshotId === 'string' && typeof route.query.ref === 'string') {
    clearDetail()
    fixedTag.value = true
    selectedScope.kind = typeof route.query.scopeKind === 'string' ? route.query.scopeKind : 'DEFAULT'
    selectedScope.branch = typeof route.query.branch === 'string' ? route.query.branch : undefined
    selectedRef.value = route.query.ref
    selectedRefType.value = 'TAG'
    selectedSnapshot.value = route.query.snapshotId
    void loadDetail(0, null)
  }
  else if (fixedTag.value) {
    clearDetail()
    fixedTag.value = false
    selectedRef.value = 'main'
    selectedRefType.value = undefined
  }
}, { immediate: true })
function returnToList() {
  clearDetail()
  if (fixedTag.value) {
    fixedTag.value = false
    const { snapshotId, refType, ref, scopeKind, branch, ...query } = route.query
    void router.push({ query: { ...query, tab: 'Tags' } })
  }
}

onBeforeUnmount(() => {
  clearDetail()
  rows.value = []
})
</script>

<template>
  <div class="table-snapshots">
    <el-alert v-if="error" type="error" :title="error" show-icon class="g-mb-8" :closable="false">
      <template #default>
        <el-button :disabled="loading || selectorLoading" @click="retry">
          {{ $t('snapshotRetry') }}
        </el-button>
      </template>
    </el-alert>
    <div v-if="selectedSnapshot === null">
      <Selector
        v-bind="source" bounded :disabled="loading"
        @ref-change="selectRef" @consumer-change="selectConsumer"
        @loading-change="selectorLoading = $event"
      >
        <template #extra>
          <button type="button" class="snapshots-charts-header" :aria-expanded="showCharts" @click="showCharts = !showCharts">
            <span class="snapshots-charts-title">{{ $t('charts') }}</span>
            <span class="snapshots-charts-icon">
              <CaretRight v-if="!showCharts" class="amoro-icon" />
              <CaretBottom v-else class="amoro-icon" />
            </span>
          </button>
        </template>
      </Selector>
      <SnapshotCharts v-if="showCharts" :loading="loading" :record-options="recordChart" :file-options="fileChart" />
      <el-table v-loading="loading" scrollbar-always-on class="dashboard-table" :class="{ 'snapshot-table-ready': !loading }" row-key="snapshotId" :data="rows">
        <el-table-column type="expand">
          <template #default="{ row: record }">
            <dl class="snapshot-summary">
              <template v-for="(value, key) in record.summary" :key="key">
                <dt>{{ key }} :</dt>
                <dd>{{ value ?? '—' }}</dd>
              </template>
            </dl>
          </template>
        </el-table-column>
        <el-table-column v-for="column in columns" :key="column.dataIndex" v-bind="tableColumnProps(column, columns)">
          <template v-if="column.dataIndex === 'commitTime' || column.dataIndex === 'fileCount' || column.dataIndex === 'operation' || column.dataIndex === 'snapshotId'" #default="{ row: record }">
            <el-button v-if="column.dataIndex === 'snapshotId'" link type="primary" :disabled="loading || selectorLoading" @click="openSnapshot(record.snapshotId)">
              {{ record.snapshotId }}
            </el-button>
            <template v-else-if="column.dataIndex === 'fileCount'">
              {{ record.fileCount ?? '—' }}
            </template>
            <template v-else-if="column.dataIndex === 'operation'">
              {{ record.operation }}{{ record.producer === 'OPTIMIZE' ? '(optimizing)' : '' }}
            </template>
            <template v-else-if="column.dataIndex === 'commitTime'">
              {{ record.commitTime ? dateFormat(record.commitTime) : '—' }}
            </template>
          </template>
        </el-table-column>
      </el-table>
      <DashboardPagination v-if="!consumerSelected" :pagination="pagination" @change="({ current }) => loadList(current)" />
    </div>
    <div v-if="selectedSnapshot !== null">
      <el-breadcrumb separator=">">
        <el-breadcrumb-item><a @click="returnToList">{{ $t(fixedTag ? 'tags' : 'all') }}</a></el-breadcrumb-item>
        <el-breadcrumb-item>{{ $t('snapshotId') }} {{ selectedSnapshot }}</el-breadcrumb-item>
      </el-breadcrumb>
      <el-table v-loading="loading" scrollbar-always-on :class="{ 'snapshot-table-ready': !loading }" row-key="rowKey" class="dashboard-table g-mt-8" :tooltip-options="{ popperClass: 'snapshot-file-tooltip' }" :data="detailRows">
        <el-table-column v-for="column in detailColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, detailColumns)">
          <template v-if="column.dataIndex === 'commitTime'" #default="{ row: record }">
            {{ record.commitTime ? dateFormat(record.commitTime) : '—' }}
          </template>
        </el-table-column>
      </el-table>
      <el-space class="snapshot-detail-pagination">
        <el-button :disabled="loading || cursors.index === 0" @click="loadDetail(cursors.index - 1, cursors.starts[cursors.index - 1])">
          {{ $t('snapshotPrevious') }}
        </el-button>
        <span>{{ cursors.index + 1 }}</span>
        <el-button :disabled="loading || cursors.next === null" @click="loadDetail(cursors.index + 1, cursors.next)">
          {{ $t('snapshotNext') }}
        </el-button>
      </el-space>
    </div>
  </div>
</template>

<style lang="less" scoped>
// Match LegacySnapshots presentation without changing the bounded read protocol.
.table-snapshots {
  padding: 18px 0;

  :deep(.el-button.is-link) {
    padding: 0;
  }

  :deep(.el-table__expand-icon) {
    border-radius: 0 !important;
  }

  :deep(td.el-table__expanded-cell) {
    background-color: #f7f7f7;
  }

  .el-table {
    margin-top: 18px;
  }

  // Release the controls when the data is ready, even while Element Plus unmounts its loading mask.
  .snapshot-table-ready :deep(.el-loading-mask) {
    visibility: hidden;
    pointer-events: none;
    transition: none;
  }

  .snapshots-charts-header {
    display: flex;
    align-items: center;
    padding: 0;
    border: 0;
    background: none;
    color: inherit;
    font: inherit;
    cursor: pointer;
  }

  .snapshots-charts-title {
    margin-right: 4px;
  }

  .snapshots-charts-icon {
    font-size: 10px;
    color: #999;
    display: inline-flex;
    align-items: center;
  }

  .snapshot-detail-pagination {
    display: flex;
    justify-content: flex-end;
    margin: 16px 0;
  }
}
</style>
