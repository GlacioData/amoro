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
import { computed, onMounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import Selector from './Selector.vue'
import { snapshotChartData } from './snapshot-chart-data'
import SnapshotCharts from './SnapshotCharts.vue'
import DashboardPagination from '@/components/DashboardPagination.vue'
import { tableColumnProps } from '@/utils/table'
import { usePagination } from '@/hooks/usePagination'
import type { BreadcrumbSnapshotItem, IColumns, SnapshotItem, SnapshotItemSummary } from '@/types/common.type'
import { getDetailBySnapshotId, getSnapshots } from '@/services/table.service'
import { dateFormat } from '@/utils'
import type { ECOption } from '@/components/echarts'
import { generateLineChartOption } from '@/utils/chart'

const hasBreadcrumb = ref<boolean>(false)
const { t } = useI18n()
const columns = computed<IColumns[]>(() => [
  { title: t('snapshotId'), dataIndex: 'snapshotId', minWidth: 112, ellipsis: true },
  { title: t('operation'), dataIndex: 'operation', minWidth: 112 },
  { title: t('records'), dataIndex: 'records', minWidth: 96 },
  { title: t('fileCount'), dataIndex: 'fileCount', minWidth: 112 },
  { title: t('commitTime'), dataIndex: 'commitTime', minWidth: 164 },
])
const breadcrumbColumns = computed(() => [
  { title: t('operation'), dataIndex: 'operation', width: 120, ellipsis: true },
  { title: t('file'), dataIndex: 'file', minWidth: 240, ellipsis: true },
  { title: t('partition'), dataIndex: 'partition', width: 120 },
  { title: t('fileType'), dataIndex: 'fileType', width: 120, ellipsis: true },
  { title: t('size'), dataIndex: 'size', width: 120 },
  { title: t('commitTime'), dataIndex: 'commitTime', width: 200, ellipsis: true },
  { title: t('path'), dataIndex: 'path', minWidth: 280, ellipsis: true },
])
const dataSource = reactive<SnapshotItem[]>([])
const breadcrumbDataSource = reactive<BreadcrumbSnapshotItem[]>([])
const snapshotId = ref<string>('')
const loading = ref<boolean>(false)
const pagination = reactive(usePagination())
const breadcrumbPagination = reactive(usePagination())
const lastSnapshot = ref<string | null>(null) // Store last snapshot ID for cursor-based pagination
const route = useRoute()
const query = route.query
const sourceData = reactive({
  catalog: '',
  db: '',
  table: '',
  ...query,
})

const recordChartOption = ref<ECOption>({})
const fileChartOption = ref<ECOption>({})
const showCharts = ref(false)
const tblRef = ref<string>('')
const operation = ref<string>('')

function onRefChange(params: { ref: string, operation: string }) {
  tblRef.value = params.ref
  operation.value = params.operation
  // Reset cursor-based pagination when ref or operation changes
  lastSnapshot.value = null
  pagination.current = 1
  getTableInfo()
}
function onConsumerChange(params: {
  ref: string
  operation: string
  amoroCurrentSnapshotsItem: SnapshotItem | null
}) {
  if (!params.amoroCurrentSnapshotsItem)
    return
  tblRef.value = params.ref
  operation.value = params.operation
  dataSource.length = 0
  params.amoroCurrentSnapshotsItem.commitTime = params.amoroCurrentSnapshotsItem
    .commitTime
    ? dateFormat(params.amoroCurrentSnapshotsItem.commitTime)
    : '-'
  dataSource.push(params.amoroCurrentSnapshotsItem)
  pagination.total = 1
}

async function getTableInfo() {
  try {
    loading.value = true
    dataSource.length = 0
    const result = await getSnapshots({
      ...sourceData,
      ref: tblRef.value,
      operation: operation.value,
      page: pagination.current,
      pageSize: pagination.pageSize,
      lastSnapshot: lastSnapshot.value ?? undefined,
    })
    const { list = [], total } = result
    const rcData: Record<string, SnapshotItemSummary> = {}
    const fcData: Record<string, SnapshotItemSummary> = {}
    list.forEach((p: SnapshotItem) => {
      // Assume that the time will not conflict and use the time as the unique key without formatting it.
      const { recordsSummaryForChart, filesSummaryForChart, commitTime } = p
      rcData[commitTime] = recordsSummaryForChart || {}
      fcData[commitTime] = filesSummaryForChart || {}
      if (p.producer === 'OPTIMIZE') {
        p.operation = `${p.operation}(optimizing)`
      }
      p.commitTime = p.commitTime ? dateFormat(p.commitTime) : '-'
      dataSource.push(p)
    })

    // Update lastSnapshot with the last item's snapshotId from current page for cursor-based pagination
    if (list && list.length > 0) {
      lastSnapshot.value = list[list.length - 1].snapshotId || null
    }
    else {
      lastSnapshot.value = null
    }

    recordChartOption.value = generateLineChartOption(t('recordChartTitle'), snapshotChartData(rcData))
    fileChartOption.value = generateLineChartOption(t('fileChartTitle'), snapshotChartData(fcData))
    pagination.total = total
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function change({ current = 1, pageSize = 25 }) {
  if (hasBreadcrumb.value) {
    breadcrumbPagination.current = current
    if (pageSize !== breadcrumbPagination.pageSize) {
      breadcrumbPagination.current = 1
    }
    breadcrumbPagination.pageSize = pageSize
  }
  else {
    const prevCurrent = pagination.current
    // Reset cursor-based pagination when:
    //   - returning to first page
    //   - page size changed
    //   - non-sequential jump (user skipped pages or went backward)
    // Cursor (lastSnapshot) is only valid for a sequential prev → prev+1 step.
    if (
      current === 1
      || pageSize !== pagination.pageSize
      || current !== prevCurrent + 1
    ) {
      lastSnapshot.value = null
    }
    pagination.current = current
    if (pageSize !== pagination.pageSize) {
      pagination.current = 1
    }
    pagination.pageSize = pageSize
  }
  refresh()
}

function refresh() {
  if (hasBreadcrumb.value) {
    getBreadcrumbTable()
  }
  else {
    getTableInfo()
  }
}

async function getBreadcrumbTable() {
  try {
    breadcrumbDataSource.length = 0
    loading.value = true
    const params = {
      ...sourceData,
      snapshotId: snapshotId.value,
      ref: tblRef.value,
      page: breadcrumbPagination.current,
      pageSize: breadcrumbPagination.pageSize,
    }
    const result = await getDetailBySnapshotId(params)
    const { list, total } = result
    breadcrumbPagination.total = total
    list.forEach((p: BreadcrumbSnapshotItem) => {
      p.commitTime = p.commitTime ? dateFormat(p.commitTime) : ''
      breadcrumbDataSource.push(p)
    })
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function openSnapshotDetail(record: SnapshotItem) {
  snapshotId.value = record.snapshotId
  hasBreadcrumb.value = true
  breadcrumbPagination.current = 1
  getBreadcrumbTable()
}

function returnToList() {
  snapshotId.value = ''
  hasBreadcrumb.value = false
  // Reset cursor-based pagination when returning to main view
  lastSnapshot.value = null
  pagination.current = 1
}

function toggleCharts() {
  showCharts.value = !showCharts.value
}

onMounted(() => {
  hasBreadcrumb.value = false
})
</script>

<template>
  <div class="table-snapshots">
    <template v-if="!hasBreadcrumb">
      <Selector
        :catalog="sourceData.catalog"
        :db="sourceData.db"
        :table="sourceData.table"
        :disabled="loading"
        @consumer-change="onConsumerChange"
        @ref-change="onRefChange"
      >
        <template #extra>
          <button type="button" class="snapshots-charts-header" :aria-expanded="showCharts" @click="toggleCharts">
            <span class="snapshots-charts-title">{{ $t('charts') }}</span>
            <span class="snapshots-charts-icon">
              <CaretRight v-if="!showCharts" class="amoro-icon" />
              <CaretBottom v-else class="amoro-icon" />
            </span>
          </button>
        </template>
      </Selector>
      <SnapshotCharts v-if="showCharts" :loading="loading" :record-options="recordChartOption" :file-options="fileChartOption" />
      <el-table v-loading="loading" class="dashboard-table" scrollbar-always-on row-key="snapshotId" :data="dataSource">
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
          <template v-if="column.dataIndex === 'snapshotId'" #default="{ row: record }">
            <template v-if="column.dataIndex === 'snapshotId'">
              <el-button link type="primary" @click="openSnapshotDetail(record)">
                {{ record.snapshotId }}
              </el-button>
            </template>
          </template>
        </el-table-column>
      </el-table>
      <DashboardPagination :pagination="pagination" @change="change" />
    </template>
    <template v-else>
      <el-breadcrumb separator=">">
        <el-breadcrumb-item class="text-active" @click="returnToList">
          {{ $t('all') }}
        </el-breadcrumb-item>
        <el-breadcrumb-item>{{ `${$t('snapshotId')} ${snapshotId}` }}</el-breadcrumb-item>
      </el-breadcrumb>
      <el-table v-loading="loading" scrollbar-always-on row-key="file" class="dashboard-table g-mt-8" :tooltip-options="{ popperClass: 'snapshot-file-tooltip' }" :data="breadcrumbDataSource">
        <el-table-column v-for="column in breadcrumbColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, breadcrumbColumns)" />
      </el-table>
      <DashboardPagination :pagination="breadcrumbPagination" @change="change" />
    </template>
  </div>
</template>

<style lang="less" scoped>
.table-snapshots {
  padding: 18px 0;

  .text-active {
    color: #1890ff;
    cursor: pointer;
  }

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

  .snapshots-charts-header {
    border: 0;
    background: none;
    padding: 0;
    color: inherit;
    font: inherit;
    display: flex;
    align-items: center;
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
}
</style>
