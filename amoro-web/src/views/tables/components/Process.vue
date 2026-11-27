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
import { TABLE_EXPAND_COLUMN_WIDTH, buildProcessColumns } from './table-subviews-state'
import DashboardPagination from '@/components/DashboardPagination.vue'
import { tableColumnProps } from '@/utils/table'
import { usePagination } from '@/hooks/usePagination'
import type { IColumns, ILableAndValue } from '@/types/common.type'
import { getTableProcesses } from '@/services/table.service'
import { dateFormat } from '@/utils/index'

const statusMap = {
  UNKNOWN: { title: 'UNKNOWN', color: '#c9cdd4' },
  PENDING: { title: 'PENDING', color: '#ffcc00' },
  SUBMITTED: { title: 'SUBMITTED', color: '#4169E1' },
  RUNNING: { title: 'RUNNING', color: '#1890ff' },
  CANCELING: { title: 'CANCELING', color: '#faad14' },
  SUCCESS: { title: 'SUCCESS', color: '#0ad787' },
  CANCELED: { title: 'CANCELED', color: '#c9cdd4' },
  CLOSED: { title: 'CLOSED', color: '#c9cdd4' },
  KILLED: { title: 'KILLED', color: '#c9cdd4' },
  FAILED: { title: 'FAILED', color: '#f5222d' },
}
const STATUS_CONFIG = shallowReactive(statusMap)

const { t } = useI18n()
const columns = computed<IColumns[]>(() => buildProcessColumns(t))

const dataSource = reactive<any[]>([])
const loading = ref<boolean>(false)
const pagination = reactive(usePagination())
const route = useRoute()
const query = route.query
const sourceData = reactive({
  catalog: '',
  db: '',
  table: '',
  ...query,
})

const processTypeFilter = ref<string>('')
const statusType = ref<string>()
const statusTypeList = ref<ILableAndValue[]>([])

function initFilters() {
  const statusList = Object.entries(statusMap).map(([key, value]) => ({
    label: value.title,
    value: key,
  }))
  statusTypeList.value = statusList
}

async function refreshProcesses() {
  try {
    loading.value = true
    dataSource.length = 0
    const result = await getTableProcesses({
      ...sourceData,
      type: processTypeFilter.value || '',
      status: statusType.value || '',
      page: pagination.current,
      pageSize: pagination.pageSize,
    } as any)
    const { list, total = 0 } = result
    pagination.total = total

    dataSource.push(
      ...[...(list || [])].map((item) => {
        return {
          ...item,
          createTimeFormatted: item.createTime ? dateFormat(item.createTime) : '-',
          finishTimeFormatted: item.finishTime ? dateFormat(item.finishTime) : '-',
        }
      }),
    )
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function change({ current = 1, pageSize = 25 }) {
  pagination.current = current
  if (pageSize !== pagination.pageSize) {
    pagination.current = 1
  }
  pagination.pageSize = pageSize
  refreshProcesses()
}

function handleFilterChange() {
  pagination.current = 1
  refreshProcesses()
}

function isHttpTrackUri(value: unknown) {
  if (typeof value !== 'string' || !value)
    return false

  try {
    const uri = new URL(value)
    return uri.protocol === 'http:' || uri.protocol === 'https:'
  }
  catch {
    return false
  }
}

onMounted(() => {
  initFilters()
  refreshProcesses()
})
</script>

<template>
  <div class="table-process">
    <el-space wrap class="filter-form">
      <el-input
        v-model="processTypeFilter"
        clearable
        :placeholder="t('processType')"
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
    <el-table v-loading="loading" scrollbar-always-on row-key="processId" :data="dataSource">
      <el-table-column type="expand" :width="TABLE_EXPAND_COLUMN_WIDTH">
        <template #default="{ row: record }">
          <div class="expanded-content">
            <template v-if="record.processParameters && Object.keys(record.processParameters).length">
              <h4 class="expanded-section-title">
                {{ t('processParameters') }}
              </h4>
              <div class="kv-list">
                <div v-for="(value, key) in record.processParameters" :key="key" class="kv-row">
                  <span class="kv-key">{{ key }}</span>
                  <span class="kv-value">{{ value }}</span>
                </div>
              </div>
            </template>
            <template v-if="record.summary && Object.keys(record.summary).length">
              <h4 class="expanded-section-title">
                {{ t('summary') }}
              </h4>
              <div class="kv-list">
                <div v-for="(value, key) in record.summary" :key="key" class="kv-row">
                  <span class="kv-key">{{ key }}</span>
                  <span class="kv-value">
                    <a
                      v-if="key === 'trackUri' && isHttpTrackUri(value)" :href="value"
                      target="_blank" rel="noopener noreferrer"
                    >{{ value }}</a>
                    <template v-else>
                      {{ value }}
                    </template>
                  </span>
                </div>
              </div>
            </template>
          </div>
        </template>
      </el-table-column>
      <el-table-column v-for="column in columns" :key="column.dataIndex" v-bind="tableColumnProps(column, columns)">
        <template v-if="column.dataIndex === 'status'" #default="{ row: record }">
          <template v-if="column.dataIndex === 'status'">
            <div class="g-flex-ac status-cell">
              <span
                :style="{ 'background-color': (STATUS_CONFIG[record.status as keyof typeof STATUS_CONFIG] || {} as any).color }"
                class="status-icon"
              />
              <span>{{ record.status }}</span>
              <el-tooltip
                v-if="record.status === 'FAILED' && record.failMessage"
                placement="top-end"
                popper-class="table-failed-tip"
              >
                <template #content>
                  <div class="tip-title">
                    {{ record.failMessage }}
                  </div>
                </template>
                <QuestionFilled class="amoro-icon g-ml-4" />
              </el-tooltip>
            </div>
          </template>
        </template>
      </el-table-column>
    </el-table>
    <DashboardPagination :pagination="pagination" @change="change" />
  </div>
</template>

<style lang="less" scoped>
.table-process {
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

.expanded-content {
  padding: 4px 0;
}

.expanded-section-title {
  margin: 0 0 8px 0;
  font-size: 14px;
  font-weight: 600;

  & + .kv-list {
    margin-bottom: 16px;
  }
}

.kv-list {
  display: table;
  border-spacing: 0;
}

.kv-row {
  display: table-row;
  line-height: 28px;
}

.kv-key {
  display: table-cell;
  text-align: right;
  padding-right: 8px;
  white-space: nowrap;
  color: rgba(0, 0, 0, 0.65);
  font-weight: 500;

  &::after {
    content: ' :';
  }
}

.kv-value {
  display: table-cell;
  text-align: left;
  padding-left: 8px;
  word-break: break-all;
}
</style>

<style lang="less">
.table-process {

  .filter-form {
    width: 100%;
    margin-bottom: 16px;
  }
}

.el-popper.table-failed-tip {
  max-width: min(800px, calc(100vw - 32px));

  .tip-title {
    display: block;
    max-height: min(700px, 70dvh);
    overflow: auto;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
}
</style>
