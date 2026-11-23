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
import { CircleClose, Search } from '@element-plus/icons-vue'
import { computed, onMounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import DashboardPagination from '@/components/DashboardPagination.vue'
import { tableColumnProps } from '@/utils/table'
import { usePagination } from '@/hooks/usePagination'
import type { BreadcrumbPartitionItem, IColumns, PartitionItem } from '@/types/common.type'
import { getPartitionFiles, getPartitionTable } from '@/services/table.service'
import { dateFormat, utcDateFormat } from '@/utils'

const props = defineProps<{ hasPartition: boolean, tableFormat?: string }>()

// Paimon reports commit times as raw epoch millis with no server-side timezone; rendering them in
// UTC keeps the Files page deterministic for Paimon. Every other lake format keeps using local
// time via dateFormat, so this is scoped strictly to tableFormat === 'PAIMON'.
function formatTime(val: number | string) {
  return props.tableFormat?.toUpperCase() === 'PAIMON' ? utcDateFormat(val) : dateFormat(val)
}
const hasBreadcrumb = ref<boolean>(false)
const { t } = useI18n()
const columns = computed<IColumns[]>(() => [
  { title: t('partition'), dataIndex: 'partition', minWidth: 220, ellipsis: true },
  { title: t('fileCount'), dataIndex: 'fileCount', width: 100, ellipsis: true },
  { title: t('size'), dataIndex: 'size', width: 88, ellipsis: true },
  { title: t('lastCommitTime'), dataIndex: 'lastCommitTime', width: 164, ellipsis: true },
])
const breadcrumbColumns = computed<IColumns[]>(() => [
  { title: t('file'), dataIndex: 'file', minWidth: 220, ellipsis: true },
  { title: t('fileType'), dataIndex: 'fileType', width: 108, ellipsis: true },
  { title: t('size'), dataIndex: 'size', width: 84, ellipsis: true },
  { title: t('commitTime'), dataIndex: 'commitTime', width: 164, ellipsis: true },
  { title: t('commitId'), dataIndex: 'commitId', width: 108, ellipsis: true },
  { title: t('path'), dataIndex: 'path', minWidth: 176, ellipsis: true },
])

const dataSource = reactive<PartitionItem[]>([])
const breadcrumbDataSource = reactive<BreadcrumbPartitionItem[]>([])
const partitionId = ref<string>('')
const partitionSegments = computed(() => partitionId.value.split('/')
  .map((segment, index, segments) => index < segments.length - 1 ? `${segment}/` : segment))
const specId = ref<number>(0)
const loading = ref<boolean>(false)
const pagination = reactive(usePagination())
const breadcrumbPagination = reactive(usePagination())
const route = useRoute()
const query = route.query
const sourceData = reactive({
  catalog: '',
  db: '',
  table: '',
  ...query,
})
const searchKey = ref<string>('')

async function handleSearch(val: string) {
  searchKey.value = val
  await getTableInfo()
}

async function getTableInfo() {
  try {
    loading.value = true
    dataSource.length = 0
    const result = await getPartitionTable({
      ...sourceData,
      filter: searchKey.value,
      page: pagination.current,
      pageSize: pagination.pageSize,
    })
    const { list, total } = result
    pagination.total = total;
    (list || []).forEach((p: PartitionItem) => {
      p.lastCommitTime = p.lastCommitTime ? formatTime(p.lastCommitTime) : ''
      dataSource.push(p)
    })
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}
function change({ current = 1, pageSize = 25 }) {
  if (!hasBreadcrumb.value && props.hasPartition) {
    pagination.current = current
    if (pageSize !== pagination.pageSize) {
      pagination.current = 1
    }
    pagination.pageSize = pageSize
  }
  else {
    breadcrumbPagination.current = current
    if (pageSize !== breadcrumbPagination.pageSize) {
      breadcrumbPagination.current = 1
    }
    breadcrumbPagination.pageSize = pageSize
  }
  refresh()
}

function refresh() {
  if (!props.hasPartition) {
    getFiles()
    return
  }
  if (hasBreadcrumb.value) {
    getFiles()
  }
  else {
    getTableInfo()
  }
}

async function getFiles() {
  try {
    breadcrumbDataSource.length = 0
    loading.value = true
    const params = {
      ...sourceData,
      partition: props.hasPartition ? encodeURIComponent(partitionId.value) : null,
      specId: specId.value,
      page: breadcrumbPagination.current,
      pageSize: breadcrumbPagination.pageSize,
    }
    const result = await getPartitionFiles(params)
    const { list, total } = result
    breadcrumbPagination.total = total;
    (list || []).forEach((p: BreadcrumbPartitionItem) => {
      p.commitTime = p.commitTime && p.commitTime !== -1 ? formatTime(p.commitTime) : ''
      breadcrumbDataSource.push(p)
    })
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function openPartition(record: PartitionItem) {
  partitionId.value = record.partition
  specId.value = record.specId
  hasBreadcrumb.value = true
  breadcrumbPagination.current = 1
  void getFiles()
}

onMounted(() => {
  hasBreadcrumb.value = false

  if (props.hasPartition) {
    getTableInfo()
  }
  else {
    getFiles()
  }
})
</script>

<template>
  <div class="table-files">
    <template v-if="!hasBreadcrumb && hasPartition">
      <div class="filter-wrap">
        <el-input
          v-model="searchKey"
          :placeholder="$t('fileSearchPlaceholder')"
          style="width: 350px; max-width: 100%"
          @keydown.enter="(event: KeyboardEvent) => { if (!event.isComposing) ((val: string) => handleSearch(val))((event.target as HTMLInputElement).value) }"
        >
          <template #prefix>
            <Search class="amoro-icon" />
          </template>
          <template v-if="searchKey" #suffix>
            <CircleClose class="amoro-icon input-clear-icon" @click="handleSearch('')" />
          </template>
          <template #append>
            <el-button :aria-label="$t('search')" @click="handleSearch(searchKey)">
              <Search class="amoro-icon" />
            </el-button>
          </template>
        </el-input>
      </div>
      <el-table v-loading="loading" scrollbar-always-on :row-key="(record: PartitionItem) => `${record.specId}:${record.partition}`" :data="dataSource">
        <el-table-column
          v-for="column in columns"
          :key="column.dataIndex"
          v-bind="tableColumnProps(column, columns)"
          :align="column.dataIndex === 'fileCount' || column.dataIndex === 'size' ? 'right' : 'left'"
        >
          <template v-if="column.dataIndex === 'partition'" #default="{ row: record }">
            <el-button link type="primary" class="partition-link" @click="openPartition(record)">
              {{ record.partition }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
      <DashboardPagination :pagination="pagination" @change="change" />
    </template>
    <template v-else>
      <el-breadcrumb v-if="hasPartition" class="files-breadcrumb" separator=">" :aria-label="$t('files')">
        <el-breadcrumb-item>
          <el-button link type="primary" class="breadcrumb-back" @click="hasBreadcrumb = false">
            {{ $t('all') }}
          </el-button>
        </el-breadcrumb-item>
        <el-breadcrumb-item>
          {{ $t('partition') }}{{ ' ' }}
          <span v-for="(segment, index) in partitionSegments" :key="index" class="partition-segment">{{ segment }}</span>
        </el-breadcrumb-item>
      </el-breadcrumb>
      <el-table v-loading="loading" scrollbar-always-on row-key="file" :data="breadcrumbDataSource">
        <el-table-column
          v-for="column in breadcrumbColumns"
          :key="column.dataIndex"
          v-bind="tableColumnProps(column, breadcrumbColumns)"
          :align="column.dataIndex === 'size' ? 'right' : 'left'"
        />
      </el-table>
      <DashboardPagination :pagination="breadcrumbPagination" @change="change" />
    </template>
  </div>
</template>

<style lang="less" scoped>
.table-files {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 12px;
  padding-block: 16px 20px;

  .dashboard-pagination {
    margin-block: 0;
  }

  .files-breadcrumb {
    display: flex;
    align-items: baseline;
    line-height: 1.5;

    :deep(.el-breadcrumb__item) {
      display: flex;
      align-items: baseline;
    }

    :deep(.el-breadcrumb__item:last-child) {
      min-width: 0;
      white-space: normal;

      .el-breadcrumb__inner {
        min-width: 0;
      }
    }
  }

  .partition-segment {
    display: inline-block;
    max-width: 100%;
    overflow-wrap: anywhere;
    vertical-align: top;
  }

  .breadcrumb-back {
    height: auto;
    padding: 0;
    line-height: inherit;
  }

  .partition-link {
    max-width: 100%;

    :deep(> span) {
      display: block;
      overflow: hidden;
      text-overflow: ellipsis;
    }
  }

  .input-clear-icon {
    font-size: 12px;
  }

  :deep(.el-table .cell) {
    padding-inline: 8px;
  }

  :deep(.el-input-group__append) {
    display: none;
  }
}
</style>
