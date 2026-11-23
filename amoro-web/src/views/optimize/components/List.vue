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
import { computed, onMounted, reactive, ref, shallowReactive } from 'vue'
import { useI18n } from 'vue-i18n'
import type { TableInstance } from 'element-plus'
import DashboardPagination from '@/components/DashboardPagination.vue'
import { tableColumnProps } from '@/utils/table'
import { confirmAction } from '@/utils/ui-feedback'
import type { IIOptimizeGroupItem, ILableAndValue, IOptimizeResourceTableItem, IOptimizeTableItem } from '@/types/common.type'
import { getOptimizerAction, getOptimizerTableList, getResourceGroupsListAPI, releaseResource } from '@/services/optimize.service'
import { useMediaQuery } from '@/hooks/useMediaQuery'
import { usePagination } from '@/hooks/usePagination'
import { usePlaceholder } from '@/hooks/usePlaceholder'
import { bytesToSize, formatMS2DisplayTime, formatMS2Time } from '@/utils'

const { t } = useI18n()
const compact = useMediaQuery('(max-width: 760px)')

const STATUS_CONFIG = shallowReactive({
  pending: { title: 'pending', color: '#ffcc00' },
  planning: { title: 'planning', color: '#076de3' },
  idle: { title: 'idle', color: '#c9cdd4' },
  minor: { title: 'minor', color: '#0ad787' },
  major: { title: 'major', color: '#0ad787' },
  full: { title: 'full', color: '#0ad787' },
  committing: { title: 'committing', color: '#0ad787' },
})

const loading = ref<boolean>(false)
const releaseLoading = ref<boolean>(false)
const optimizerGroupList = ref<ILableAndValue[]>([])

type ServerSortField = 'duration' | 'quota'
type ServerSortOrder = 'asc' | 'desc'

const tableRef = ref<TableInstance>()
const sortBy = ref<ServerSortField>()
const sortOrder = ref<ServerSortOrder>()

const columns = computed(() => [
  { dataIndex: 'tableName', title: t('table'), minWidth: compact.value ? 208 : 300 },
  { dataIndex: 'groupName', title: t('optimizerGroup'), minWidth: 128, ellipsis: true },
  { dataIndex: 'healthScore', title: t('healthScore'), minWidth: 108, align: 'right' as const, ellipsis: true },
  { dataIndex: 'optimizeStatus', title: t('optimizingStatus'), minWidth: 140, ellipsis: true },
  {
    dataIndex: 'duration',
    key: 'duration',
    title: t('duration'),
    minWidth: 104,
    align: 'right' as const,
    ellipsis: true,
    sorter: true,
  },
  { dataIndex: 'fileCount', title: t('fileCount'), minWidth: 88, align: 'right' as const, ellipsis: true },
  { dataIndex: 'fileSizeDesc', title: t('fileSize'), minWidth: 88, align: 'right' as const, ellipsis: true },
  {
    dataIndex: 'quota',
    key: 'quota',
    title: t('quota'),
    minWidth: 84,
    align: 'right' as const,
    ellipsis: true,
    sorter: true,
  },
  { dataIndex: 'quotaOccupation', title: t('occupation'), minWidth: 104, align: 'right' as const, ellipsis: true },
])

const pagination = reactive(usePagination())
const dataSource = ref<IOptimizeTableItem[]>([])
const optimizerGroup = ref<string>()
const actions = ref<string[]>()
const dbSearchInput = ref<string>()
const tableSearchInput = ref<string>()
const healthScoreMin = ref<number>()
const healthScoreMax = ref<number>()
const durationMinMinutes = ref<number>()
const placeholder = reactive(usePlaceholder())

const actionOptions = ref<ILableAndValue[]>([])
async function fetchOptimizerAction() {
  try {
    const res = await getOptimizerAction()
    actionOptions.value = (res || []).map((value: string) => ({ label: value, value }))
  }
  catch (error) {
  }
}

async function getOptimizerGroupList() {
  const res = await getResourceGroupsListAPI()
  const list = (res || []).map((item: IIOptimizeGroupItem) => ({ label: item.resourceGroup.name, value: item.resourceGroup.name }))
  optimizerGroupList.value = list
}

function refresh(resetPage?: boolean) {
  if (resetPage) {
    pagination.current = 1
  }
  getTableList()
}

async function getTableList() {
  try {
    loading.value = true
    const params = {
      optimizerGroup: optimizerGroup.value ? encodeURIComponent(optimizerGroup.value) : 'all',
      dbSearchInput: dbSearchInput.value || '',
      tableSearchInput: tableSearchInput.value || '',
      page: pagination.current,
      pageSize: pagination.pageSize,
      actions: actions.value,
      healthScoreMin: healthScoreMin.value,
      healthScoreMax: healthScoreMax.value,
      durationMinMinutes: durationMinMinutes.value,
      sortBy: sortBy.value,
      sortOrder: sortOrder.value,
    }
    const result = await getOptimizerTableList(params)
    const { list, total } = result
    pagination.total = total
    dataSource.value = (list || []).map((p: IOptimizeTableItem) => {
      return {
        ...p,
        quotaOccupationDesc: p.quotaOccupation - 0.0005 > 0 ? `${(p.quotaOccupation * 100).toFixed(1)}%` : '0',
        durationDesc: p.duration ? formatMS2DisplayTime(p.duration) : '-',
        fileSizeDesc: bytesToSize(p.fileSize),
      }
    })
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function releaseModal(record: any) {
  if (record.container === 'external') {
    return
  }
  confirmAction({
    title: t('releaseOptModalTitle'),
    onOk: () => {
      releaseJob(record)
    },
  })
}
async function releaseJob(record: IOptimizeResourceTableItem) {
  try {
    releaseLoading.value = true
    await releaseResource({
      optimizerGroup: record.groupName,
      jobId: record.jobId as unknown as string,
    })
    refresh(true)
  }
  finally {
    releaseLoading.value = false
  }
}
function changeTable({ current, pageSize }: { current: number, pageSize: number }) {
  pagination.current = pageSize !== pagination.pageSize ? 1 : current
  pagination.pageSize = pageSize
  getTableList()
}

function changeSort({ prop, order }: { prop: string, order: 'ascending' | 'descending' | null }) {
  const field = order && (prop === 'duration' || prop === 'quota') ? prop : undefined
  const direction = field ? (order === 'ascending' ? 'asc' : 'desc') : undefined
  if (field === sortBy.value && direction === sortOrder.value)
    return
  sortBy.value = field
  sortOrder.value = direction
  pagination.current = 1
  getTableList()
}

function tableDetailLocation(record: IOptimizeTableItem) {
  const { catalog, database, tableName } = record.tableIdentifier
  return {
    path: '/tables',
    query: {
      catalog,
      db: database,
      table: tableName,
    },
  }
}

function reset() {
  optimizerGroup.value = undefined
  dbSearchInput.value = undefined
  tableSearchInput.value = undefined
  actions.value = undefined
  healthScoreMin.value = undefined
  healthScoreMax.value = undefined
  durationMinMinutes.value = undefined
  sortBy.value = undefined
  sortOrder.value = undefined
  tableRef.value?.clearSort()
  refresh(true)
}

onMounted(async () => {
  refresh()
  await getOptimizerGroupList()
  await fetchOptimizerAction()
})
</script>

<template>
  <div class="list-wrap">
    <div class="filter-form" role="search" :aria-label="t('search')">
      <el-select
        v-model="optimizerGroup"
        clearable
        :placeholder="t('optimizerGroup')"
        :options="optimizerGroupList"
        class="filter-control"
      />

      <el-input
        v-model="dbSearchInput"
        :placeholder="placeholder.filterDBPh"
        class="filter-control"
      />

      <el-input
        v-model="tableSearchInput"
        :placeholder="placeholder.filterTablePh"
        class="filter-control"
      />

      <el-select
        v-model="actions"
        clearable
        :placeholder="t('action')"
        :options="actionOptions"
        multiple
        class="filter-control"
      />

      <el-input-number
        v-model="healthScoreMin"
        controls-position="right"
        :min="0"
        :max="100"
        :precision="0"
        :placeholder="t('healthScoreMin')"
        :aria-label="t('healthScoreMin')"
        class="filter-control"
      />

      <el-input-number
        v-model="healthScoreMax"
        controls-position="right"
        :min="0"
        :max="100"
        :precision="0"
        :placeholder="t('healthScoreMax')"
        :aria-label="t('healthScoreMax')"
        class="filter-control"
      />

      <el-input-number
        v-model="durationMinMinutes"
        controls-position="right"
        :min="0"
        :precision="0"
        :placeholder="t('durationMinMinutes')"
        :aria-label="t('durationMinMinutes')"
        class="filter-control"
      />

      <div class="filter-actions">
        <el-button type="primary" @click="refresh(true)">
          {{ t('search') }}
        </el-button>
        <el-button @click="reset">
          {{ t('reset') }}
        </el-button>
      </div>
    </div>
    <el-table ref="tableRef" v-loading="loading" class="dashboard-table" scrollbar-always-on :data="dataSource" @sort-change="changeSort">
      <template #empty>
        <el-empty :image-size="48" />
      </template>
      <el-table-column v-for="column in columns" :key="column.dataIndex" v-bind="tableColumnProps(column, columns)" :align="column.align">
        <template v-if="column.dataIndex === 'duration' || column.dataIndex === 'healthScore' || column.dataIndex === 'operation' || column.dataIndex === 'optimizeStatus' || column.dataIndex === 'quotaOccupation' || column.dataIndex === 'tableName'" #default="{ row: record }">
          <template v-if="column.dataIndex === 'tableName'">
            <router-link :to="tableDetailLocation(record)" :title="record.tableName" class="table-name-link">
              {{ record.tableName }}
            </router-link>
          </template>
          <template v-if="column.dataIndex === 'duration'">
            <span :title="record.durationDesc">
              {{ formatMS2Time(record.duration || 0) }}
            </span>
          </template>
          <template v-if="column.dataIndex === 'healthScore'">
            {{ record.healthScore >= 0 ? record.healthScore : '-' }}
          </template>
          <template v-if="column.dataIndex === 'quotaOccupation'">
            <span :title="record.quotaOccupationDesc">
              {{ record.quotaOccupationDesc }}
            </span>
          </template>
          <template v-if="column.dataIndex === 'optimizeStatus'">
            <span
              :style="{ 'background-color': (STATUS_CONFIG[record.optimizeStatus as keyof typeof STATUS_CONFIG] as any)?.color }"
              class="status-icon"
            />
            <span>{{ record.optimizeStatus }}</span>
          </template>
          <template v-if="column.dataIndex === 'operation'">
            <span class="primary-link" :class="{ disabled: record.container === 'external' }" @click="releaseModal(record)">
              {{ t('release') }}
            </span>
          </template>
        </template>
      </el-table-column>
    </el-table>
    <DashboardPagination v-if="pagination.total > 0" :pagination="pagination" @change="changeTable" />
  </div>
  <u-loading v-if="releaseLoading" />
</template>

<style lang="less" scoped>
.list-wrap {

  .filter-form {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 220px), 1fr));
    align-items: start;
    gap: 8px;
    width: 100%;
    margin-bottom: 16px;
  }

  .filter-control {
    width: 100%;
    min-width: 0;
  }

  :deep(.el-input-number .el-input__inner) {
    text-align: left;
  }

  .filter-actions {
    display: flex;
    gap: 8px;

    .el-button {
      flex: 1;
      min-width: 0;
    }

    .el-button + .el-button {
      margin-inline-start: 0;
    }
  }

  :deep(.el-table .cell) {
    padding-inline: 8px;
  }

  .table-name-link {
    display: block;
    overflow: hidden;
    color: @primary-color;
    text-overflow: ellipsis;
    white-space: nowrap;

    &:hover,
    &:focus-visible {
      text-decoration: underline;
    }
  }

  .primary-link {
    color: @primary-color;

    &:hover {
      cursor: pointer;
    }

    &.disabled {
      color: #999;

      &:hover {
        cursor: not-allowed;
      }
    }
  }

  .status-icon {
    width: 8px;
    height: 8px;
    border-radius: 8px;
    background-color: #c9cdd4;
    display: inline-block;
    margin-right: 8px;
  }
}
</style>
