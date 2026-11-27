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
import { QuestionFilled } from '@element-plus/icons-vue'
import { computed, onMounted, reactive, ref, shallowReactive } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import {
  TABLE_EXPAND_COLUMN_WIDTH,
  buildOptimizingProcessColumns,
  buildOptimizingTaskColumns,
} from './table-subviews-state'
import DashboardPagination from '@/components/DashboardPagination.vue'
import { copyText } from '@/utils/clipboard'
import { tableColumnProps } from '@/utils/table'
import { confirmAction, message } from '@/utils/ui-feedback'
import { usePagination } from '@/hooks/usePagination'
import type { BreadcrumbOptimizingItem, IColumns, ILableAndValue } from '@/types/common.type'
import { cancelOptimizingProcess, getOptimizingProcessError, getOptimizingProcesses, getOptimizingTaskError, getTableOptimizingTypes, getTasksByOptimizingProcessId } from '@/services/table.service'
import { bytesToSize, dateFormat, formatMS2Time } from '@/utils/index'
import { canManageTable } from '@/utils/permission'

const hasBreadcrumb = ref<boolean>(false)

const statusMap = {
  PENDING: { title: 'PENDING', color: '#ffcc00' },
  RUNNING: { title: 'RUNNING', color: '#1890ff' },
  SUBMITTED: { title: 'SUBMITTED', color: '#4169E1' },
  CLOSED: { title: 'CLOSED', color: '#c9cdd4' },
  SUCCESS: { title: 'SUCCESS', color: '#0ad787' },
  FAILED: { title: 'FAILED', color: '#f5222d' },
}
const STATUS_CONFIG = shallowReactive(statusMap)

const TASK_STATUS_CONFIG = shallowReactive({
  PLANNED: { title: 'PLANNED', color: '#ffcc00' },
  SCHEDULED: { title: 'SCHEDULED', color: '#4169E1' },
  ACKED: { title: 'ACKED', color: '#1890ff' },
  FAILED: { title: 'FAILED', color: '#f5222d' },
  SUCCESS: { title: 'SUCCESS', color: '#0ad787' },
  CANCELED: { title: 'CANCELED', color: '#c9cdd4' },
})

const { t } = useI18n()
const columns = computed<IColumns[]>(() => buildOptimizingProcessColumns(t))

const breadcrumbColumns = computed<IColumns[]>(() => buildOptimizingTaskColumns(t))

const dataSource = reactive<any[]>([])
const processId = ref<string>('')
const breadcrumbDataSource = reactive<BreadcrumbOptimizingItem[]>([])
const errorModal = reactive({
  open: false,
  loading: false,
  title: '',
  content: '',
  canCopy: false,
})
let errorRequestSequence = 0

const loading = ref<boolean>(false)
const cancelDisabled = ref(true)
const writable = ref<boolean>(canManageTable())
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
const actionType = ref<ILableAndValue>()
const actionTypeList = ref<ILableAndValue[]>([])
const statusType = ref<ILableAndValue>()
const statusTypeList = ref<ILableAndValue[]>([])

async function getQueryDataDictList() {
  const tableProcessTypes = await getTableOptimizingTypes({ ...sourceData })
  const typesList = Object.entries(tableProcessTypes).map(([typeName, displayName]) => ({ label: displayName as string, value: typeName }))
  const status = Object.entries(statusMap).map(([key, value]) => ({ label: value.title, value: key }))

  actionTypeList.value = typesList
  statusTypeList.value = status
}

async function refreshOptimizingProcesses() {
  try {
    loading.value = true
    dataSource.length = 0
    const result = await getOptimizingProcesses({
      ...sourceData,
      type: actionType.value || '',
      status: statusType.value || '',
      page: pagination.current,
      pageSize: pagination.pageSize,
      lastSnapshot: lastSnapshot.value,
    } as any)
    const { list, total = 0 } = result
    pagination.total = total

    // Update lastSnapshot with the last item's processId from current page
    if (list && list.length > 0) {
      lastSnapshot.value = list[list.length - 1].processId?.toString() || null
    }
    else {
      lastSnapshot.value = null
    }

    dataSource.push(...[...list || []].map((item) => {
      const { inputFiles = {}, outputFiles = {} } = item
      return {
        ...item,
        // recordId,
        // startTime: item.commitTime ? d(new Date(item.commitTime), 'long') : '',
        // commitTime: item.commitTime ? dateFormat(item.commitTime) : '',
        startTime: item.startTime ? dateFormat(item.startTime) : '-',
        finishTime: item.finishTime ? dateFormat(item.finishTime) : '-',
        optimizingType: item.optimizingType ? item.optimizingType : '-',
        duration: item.duration ? formatMS2Time(item.duration) : '-',
        inputFiles: `${bytesToSize(inputFiles.totalSize)} / ${inputFiles.fileCnt}`,
        outputFiles: `${bytesToSize(outputFiles.totalSize)} / ${outputFiles.fileCnt}`,
        tasks: `${item.successTasks || '0'} / ${item.totalTasks || '0'}${item.runningTasks ? ` (${item.runningTasks} running)` : ''}`,
      }
    }))
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

async function cancel() {
  confirmAction({
    title: t('cancelOptimizingProcessOptModalTitle'),
    onOk: async () => {
      try {
        loading.value = true
        await cancelOptimizingProcess({
          ...sourceData,
          processId: processId.value?.toString(),
        })
        cancelDisabled.value = true
        refresh()
      }
      catch (error) {
      }
      finally {
        loading.value = false
      }
    },
  })
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
    refreshOptimizingTasks()
  }
  else {
    refreshOptimizingProcesses()
  }
}

function formatPartition(partition: string): string {
  return partition.replace(/^Unpartitioned(?= \[bucket=)/, t('unpartitioned'))
}

async function refreshOptimizingTasks() {
  try {
    breadcrumbDataSource.length = 0
    loading.value = true
    const params = {
      ...sourceData,
      processId: processId.value,
      page: breadcrumbPagination.current,
      pageSize: breadcrumbPagination.pageSize,
    }
    const result = await getTasksByOptimizingProcessId(params)
    const { list, total } = result
    breadcrumbPagination.total = total
    list.forEach((p: BreadcrumbOptimizingItem) => {
      p.startTime = p.startTime ? dateFormat(p.startTime) : '-'
      p.endTime = p.endTime ? dateFormat(p.endTime) : '-'
      p.formatCostTime = p.costTime ? formatMS2Time(p.costTime) : '-'
      p.thread = p.optimizerToken ? `(${p.threadId})${p.optimizerToken}` : '-'
      p.partitionData = p.partitionData ? p.partitionData : '-'
      p.inputFilesDesc = `${bytesToSize(p.inputFiles.totalSize)} / ${p.inputFiles.fileCnt}`
      p.outputFilesDesc = `${bytesToSize(p.outputFiles.totalSize)} / ${p.outputFiles.fileCnt}`
      breadcrumbDataSource.push(p)
    })
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function handleFilterChange() {
  // Reset cursor-based pagination when filters change
  lastSnapshot.value = null
  pagination.current = 1
  refresh()
}

function openProcessDetail(rowProcessId: string, status: string) {
  processId.value = rowProcessId
  cancelDisabled.value = status !== 'RUNNING'
  hasBreadcrumb.value = true
  breadcrumbPagination.current = 1
  refresh()
}

function returnToList() {
  processId.value = ''
  cancelDisabled.value = true
  hasBreadcrumb.value = false
  // Reset cursor-based pagination when returning to main view
  lastSnapshot.value = null
  pagination.current = 1
  refresh()
}

async function showProcessError(record: { processId: string }) {
  await showError(
    t('processErrorTitle'),
    () => getOptimizingProcessError({
      catalog: sourceData.catalog,
      db: sourceData.db,
      table: sourceData.table,
      processId: record.processId,
    }),
  )
}

async function showTaskError(record: BreadcrumbOptimizingItem) {
  await showError(
    t('taskErrorTitle'),
    () => getOptimizingTaskError({
      catalog: sourceData.catalog,
      db: sourceData.db,
      table: sourceData.table,
      processId: processId.value,
      taskId: record.taskId,
    }),
  )
}

async function showError(title: string, loadError: () => Promise<string>) {
  const requestSequence = ++errorRequestSequence
  errorModal.open = true
  errorModal.loading = true
  errorModal.title = title
  errorModal.content = ''
  errorModal.canCopy = false
  try {
    const content = await loadError()
    if (requestSequence !== errorRequestSequence) {
      return
    }
    errorModal.content = content || t('errorMessageUnavailable')
    errorModal.canCopy = Boolean(content)
  }
  catch (error) {
    if (requestSequence === errorRequestSequence) {
      errorModal.content = t('loadErrorFailed')
    }
  }
  finally {
    if (requestSequence === errorRequestSequence) {
      errorModal.loading = false
    }
  }
}

function closeError() {
  errorRequestSequence++
  errorModal.open = false
  errorModal.loading = false
}

async function copyError() {
  try {
    await copyText(errorModal.content)
    message.success(t('copySuccess'))
  }
  catch (error) {
    message.error(t('copyFailed'))
  }
}

onMounted(() => {
  hasBreadcrumb.value = false
  refresh()
  getQueryDataDictList()
})
</script>

<template>
  <div class="table-optimizing">
    <template v-if="!hasBreadcrumb">
      <el-space wrap class="filter-form">
        <el-select
          v-model="actionType"
          clearable
          :placeholder="t('type')"
          :options="actionTypeList"
          style="min-width: 150px;"
          @change="handleFilterChange"
        />
        <el-select
          v-model="statusType"
          clearable
          :placeholder="t('status')"
          :options="statusTypeList"
          style="min-width: 150px;"
          @change="handleFilterChange"
        />
      </el-space>
      <el-table v-loading="loading" table-layout="auto" scrollbar-always-on row-key="processId" :data="dataSource">
        <el-table-column type="expand" :width="TABLE_EXPAND_COLUMN_WIDTH">
          <template #default="{ row: record }">
            <dl class="optimizing-summary">
              <template v-for="(value, key) in record.summary" :key="key">
                <dt>{{ key }} :</dt>
                <dd>{{ value ?? '—' }}</dd>
              </template>
            </dl>
          </template>
        </el-table-column>
        <el-table-column v-for="column in columns" :key="column.dataIndex" v-bind="tableColumnProps(column, columns)">
          <template v-if="column.dataIndex === 'inputFiles' || column.dataIndex === 'outputFiles' || column.dataIndex === 'tasks'" #header>
            <template v-if="column.dataIndex === 'tasks'">
              <div class="">
                {{ column.title }}
              </div>
              <div class="">
                {{ t('successSlashTotal') }}
              </div>
            </template>
            <template v-if="column.dataIndex === 'inputFiles'">
              <div class="">
                {{ column.title }}
              </div>
              <div class="">
                {{ t('sizeSlashCount') }}
              </div>
            </template>
            <template v-if="column.dataIndex === 'outputFiles'">
              <div class="">
                {{ column.title }}
              </div>
              <div class="">
                {{ t('sizeSlashCount') }}
              </div>
            </template>
          </template>
          <template v-if="column.dataIndex === 'processId' || column.dataIndex === 'status'" #default="{ row: record }">
            <template v-if="column.dataIndex === 'processId'">
              <el-button link type="primary" @click="openProcessDetail(record.processId, record.status)">
                {{ record.processId }}
              </el-button>
            </template>
            <template v-if="column.dataIndex === 'status'">
              <div class="g-flex-ac status-cell">
                <span
                  :style="{ 'background-color': (STATUS_CONFIG[record.status as keyof typeof STATUS_CONFIG] || {} as any).color }"
                  class="status-icon"
                />
                <span>{{ record.status }}</span>
                <el-button
                  v-if="record.status === 'FAILED' || record.status === 'CLOSED'"
                  link
                  class="error-detail-button g-ml-4"
                  :aria-label="t('viewError')"
                  :title="t('viewError')"
                  @click.stop="showProcessError(record)"
                >
                  <QuestionFilled class="amoro-icon" />
                </el-button>
              </div>
            </template>
          </template>
        </el-table-column>
      </el-table>
      <DashboardPagination :pagination="pagination" @change="change" />
    </template>
    <template v-else>
      <el-row>
        <el-col :span="18">
          <el-breadcrumb separator=">">
            <el-breadcrumb-item class="text-active" @click="returnToList">
              {{ t('all') }}
            </el-breadcrumb-item>
            <el-breadcrumb-item>{{ `${$t('processId')} ${processId}` }}</el-breadcrumb-item>
          </el-breadcrumb>
        </el-col>
        <el-col :span="6">
          <el-button
            v-if="writable"
            :disabled="cancelDisabled"
            type="primary"
            class="g-mb-16"
            style="float: right"
            @click="cancel"
          >
            {{ t("cancelProcess") }}
          </el-button>
        </el-col>
      </el-row>
      <el-table v-loading="loading" scrollbar-always-on row-key="taskId" class="g-mt-8" :data="breadcrumbDataSource">
        <el-table-column type="expand" :width="TABLE_EXPAND_COLUMN_WIDTH">
          <template #default="{ row: record }">
            <dl class="optimizing-summary">
              <template v-for="(value, key) in record.summary" :key="key">
                <dt>{{ key }} :</dt>
                <dd>{{ value ?? '—' }}</dd>
              </template>
            </dl>
          </template>
        </el-table-column>
        <el-table-column v-for="column in breadcrumbColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, breadcrumbColumns)">
          <template v-if="column.dataIndex === 'inputFilesDesc' || column.dataIndex === 'outputFilesDesc'" #header>
            <template v-if="column.dataIndex === 'inputFilesDesc'">
              <div class="">
                {{ column.title }}
              </div>
              <div class="">
                {{ t('sizeSlashCount') }}
              </div>
            </template>
            <template v-if="column.dataIndex === 'outputFilesDesc'">
              <div class="">
                {{ column.title }}
              </div>
              <div class="">
                {{ t('sizeSlashCount') }}
              </div>
            </template>
          </template>
          <template v-if="column.dataIndex === 'partitionData' || column.dataIndex === 'status'" #default="{ row: record }">
            <template v-if="column.dataIndex === 'partitionData'">
              <span>{{ formatPartition(record.partitionData) }}</span>
            </template>
            <template v-if="column.dataIndex === 'status'">
              <div class="g-flex-ac status-cell">
                <span
                  :style="{ 'background-color': (TASK_STATUS_CONFIG[record.status as keyof typeof TASK_STATUS_CONFIG] || {} as any).color }"
                  class="status-icon"
                />
                <span>{{ record.status }}</span>
                <el-button
                  v-if="record.status === 'FAILED'"
                  link
                  class="error-detail-button g-ml-4"
                  :aria-label="t('viewError')"
                  :title="t('viewError')"
                  @click.stop="showTaskError(record)"
                >
                  <QuestionFilled class="amoro-icon" />
                </el-button>
              </div>
            </template>
          </template>
        </el-table-column>
      </el-table>
      <DashboardPagination :pagination="breadcrumbPagination" @change="change" />
    </template>
    <el-dialog v-model="errorModal.open" :title="errorModal.title" :width="960" @update:model-value="(open: boolean) => { if (!open) closeError() }">
      <div v-loading="errorModal.loading">
        <pre class="error-stack">{{ errorModal.content }}</pre>
      </div>
      <template #footer>
        <el-button :disabled="!errorModal.canCopy" @click="copyError">
          {{ t('copy') }}
        </el-button>
        <el-button type="primary" @click="closeError">
          {{ t('close') }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style lang="less" scoped>
.table-optimizing {
  padding: 18px 0;

  :deep(.el-table .cell) {
    padding-inline: 8px;
  }

  :deep(.el-table__expand-column .cell) {
    padding-inline: 4px;
    text-align: center;
  }

  :deep(.el-table__expand-icon) {
    border-radius: 0 !important;
  }
}

.optimizing-summary {
  display: grid;
  grid-template-columns: minmax(0, min(38%, 260px)) minmax(0, 1fr);
  gap: 6px 16px;
  margin: 0;
  padding: 8px 16px;

  dt {
    margin: 0;
    text-align: right;
    color: #5a6577;
    overflow-wrap: anywhere;
  }

  dd {
    margin: 0;
    overflow-wrap: anywhere;
  }
}

@media (max-width: 760px) {
  .optimizing-summary {
    grid-template-columns: minmax(0, 1fr);

    dt {
      text-align: left;
      font-weight: 500;
    }
  }
}

.status-cell {
  white-space: nowrap;
}

.status-icon {
  flex: none;
  width: 8px;
  height: 8px;
  border-radius: 8px;
  background-color: #c9cdd4;
  display: inline-block;
  margin-right: 6px;
}

.error-detail-button {
  height: auto;
  line-height: 1;
}

.error-stack {
  min-height: 120px;
  max-height: 65vh;
  margin: 0;
  padding: 12px;
  overflow: auto;
  color: #1d2129;
  background: #f7f8fa;
  border: 1px solid #e5e6eb;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
</style>

<style lang="less">
.table-optimizing {

  .filter-form {
    width: 100%;
    margin-bottom: 16px;
  }

  .text-active {
    color: #1890ff;
    cursor: pointer;
  }

  .el-button.is-link {
    padding: 0;
  }
}
</style>
