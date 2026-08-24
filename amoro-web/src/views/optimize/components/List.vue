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
import { useRouter } from 'vue-router'
import { Modal } from 'ant-design-vue'
import type { TableProps } from 'ant-design-vue'
import type { IIOptimizeGroupItem, ILableAndValue, IOptimizeResourceTableItem, IOptimizeTableItem } from '@/types/common.type'
import { getOptimizerAction, getOptimizerTableList, getResourceGroupsListAPI, releaseResource } from '@/services/optimize.service'
import { usePagination } from '@/hooks/usePagination'
import { usePlaceholder } from '@/hooks/usePlaceholder'
import { bytesToSize, formatMS2DisplayTime, formatMS2Time } from '@/utils'
import { getTableMaxWidth } from '@/utils/table'

const { t } = useI18n()
const router = useRouter()

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

const sortBy = ref<ServerSortField>()
const sortOrder = ref<ServerSortOrder>()

function columnSortOrder(field: ServerSortField) {
  if (sortBy.value !== field) {
    return null
  }
  return sortOrder.value === 'asc' ? 'ascend' : 'descend'
}

const columns = computed(() => [
  { dataIndex: 'tableName', title: t('table'), width: 300, scopedSlots: { customRender: 'tableName' } },
  { dataIndex: 'groupName', title: t('optimizerGroup'), width: 180, ellipsis: true },
  { dataIndex: 'healthScore', title: t('healthScore'), width: 140, ellipsis: true },
  { dataIndex: 'optimizeStatus', title: t('optimizingStatus'), width: 150, ellipsis: true },
  {
    dataIndex: 'duration',
    key: 'duration',
    title: t('duration'),
    width: 150,
    ellipsis: true,
    sorter: true,
    sortDirections: ['descend', 'ascend'],
    sortOrder: columnSortOrder('duration'),
  },
  { dataIndex: 'fileCount', title: t('fileCount'), width: 150, ellipsis: true },
  { dataIndex: 'fileSizeDesc', title: t('fileSize'), width: 150, ellipsis: true },
  {
    dataIndex: 'quota',
    key: 'quota',
    title: t('quota'),
    width: 150,
    ellipsis: true,
    sorter: true,
    sortDirections: ['descend', 'ascend'],
    sortOrder: columnSortOrder('quota'),
  },
  { dataIndex: 'quotaOccupation', title: t('occupation'), width: 120, ellipsis: true },
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
  Modal.confirm({
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
const changeTable: TableProps<IOptimizeTableItem>['onChange'] = (
  { current = pagination.current, pageSize = pagination.pageSize },
  _filters,
  sorter,
) => {
  const activeSorter = Array.isArray(sorter) ? sorter[0] : sorter
  const nextSortBy
    = activeSorter?.columnKey === 'duration' || activeSorter?.columnKey === 'quota'
      ? activeSorter.columnKey
      : undefined
  const nextSortOrder
    = activeSorter?.order === 'ascend'
      ? 'asc'
      : activeSorter?.order === 'descend'
        ? 'desc'
        : undefined
  const normalizedSortBy = nextSortOrder ? nextSortBy : undefined
  const pageSizeChanged = pageSize !== pagination.pageSize
  const sortChanged = normalizedSortBy !== sortBy.value || nextSortOrder !== sortOrder.value

  sortBy.value = normalizedSortBy
  sortOrder.value = nextSortOrder
  pagination.pageSize = pageSize
  pagination.current = pageSizeChanged || sortChanged ? 1 : current
  getTableList()
}

function goTableDetail(record: IOptimizeTableItem) {
  const { catalog, database, tableName } = record.tableIdentifier
  router.push({
    path: '/tables',
    query: {
      catalog,
      db: database,
      table: tableName,
    },
  })
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
    <a-space class="filter-form" wrap>
      <a-select
        v-model:value="optimizerGroup" allow-clear :placeholder="t('optimizerGroup')" :options="optimizerGroupList"
        style="min-width: 150px;"
      />

      <a-input
        v-model:value="dbSearchInput"
        :placeholder="placeholder.filterDBPh"
      />

      <a-input
        v-model:value="tableSearchInput"
        :placeholder="placeholder.filterTablePh"
      />

      <a-select
        v-model:value="actions" allow-clear :placeholder="t('action')" :options="actionOptions" mode="multiple"
        style="min-width: 150px;"
      />

      <a-input-number
        v-model:value="healthScoreMin"
        :min="0"
        :max="100"
        :precision="0"
        :placeholder="t('healthScoreMin')"
        :aria-label="t('healthScoreMin')"
        class="number-filter"
      />

      <a-input-number
        v-model:value="healthScoreMax"
        :min="0"
        :max="100"
        :precision="0"
        :placeholder="t('healthScoreMax')"
        :aria-label="t('healthScoreMax')"
        class="number-filter"
      />

      <a-input-number
        v-model:value="durationMinMinutes"
        :min="0"
        :precision="0"
        :placeholder="t('durationMinMinutes')"
        :aria-label="t('durationMinMinutes')"
        class="duration-filter"
      />

      <a-button type="primary" @click="refresh(true)">
        {{ t('search') }}
      </a-button>
      <a-button @click="reset">
        {{ t('reset') }}
      </a-button>
    </a-space>
    <a-table
      :columns="columns"
      :data-source="dataSource"
      :pagination="pagination"
      :loading="loading"
      :scroll="{ x: getTableMaxWidth(columns) }"
      class="ant-table-common"
      @change="changeTable"
    >
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'tableName'">
          <a-typography-text
            style="display: block;"
            :ellipsis="{
              tooltip: record.tableName,
            }"
            class="primary-link"
            :content="record.tableName"
            @click="goTableDetail(record)"
          />
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
    </a-table>
  </div>
  <u-loading v-if="releaseLoading" />
</template>

<style lang="less" scoped>
.list-wrap {

  .filter-form {
    width: 100%;
    margin-bottom: 16px;
  }

  .number-filter {
    width: 165px;
  }

  .duration-filter {
    width: 220px;
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
