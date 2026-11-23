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
import { computed, onMounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import DashboardPagination from '@/components/DashboardPagination.vue'
import { tableColumnProps } from '@/utils/table'
import { copyText } from '@/utils/clipboard'
import { message } from '@/utils/ui-feedback'
import { usePagination } from '@/hooks/usePagination'
import type { IColumns, OperationItem } from '@/types/common.type'
import { getOperations } from '@/services/table.service'
import { dateFormat } from '@/utils'

const { t } = useI18n()
const columns = computed<IColumns[]>(() => [
  { title: t('time'), dataIndex: 'ts', width: '30%' },
  { title: t('operation'), dataIndex: 'operation' },
])
const visible = ref<boolean>(false)
const activeCopyText = ref<string>('')

const dataSource = reactive<OperationItem[]>([])

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

async function getOperationInfo() {
  try {
    loading.value = true
    dataSource.length = 0
    const result = await getOperations({
      ...sourceData,
      page: pagination.current,
      pageSize: pagination.pageSize,
    })
    const { total, list } = result
    pagination.total = total;
    (list || []).forEach((ele: OperationItem) => {
      ele.ts = ele.ts ? dateFormat(ele.ts) : ''
      dataSource.push(ele)
    })
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function change({ current, pageSize }: { current: number, pageSize: number }) {
  pagination.current = current
  if (pageSize !== pagination.pageSize) {
    pagination.current = 1
  }
  pagination.pageSize = pageSize
  getOperationInfo()
}

function viewDetail(record: OperationItem) {
  visible.value = true
  activeCopyText.value = record.operation
}

function cancel() {
  visible.value = false
}

async function onCopy() {
  try {
    await copyText(activeCopyText.value)
    message.success(t('copySuccess'))
    cancel()
  }
  catch (error) {}
}

onMounted(() => {
  getOperationInfo()
})
</script>

<template>
  <div class="table-operations">
    <el-table v-loading="loading" row-key="partiton" :data="dataSource">
      <el-table-column v-for="column in columns" :key="column.dataIndex" v-bind="tableColumnProps(column, columns)">
        <template v-if="column.dataIndex === 'operation'" #default="{ row: record }">
          <template v-if="column.dataIndex === 'operation'">
            <span class="text-active g-max-line-3" @click="viewDetail(record)">
              {{ record.operation }}
            </span>
          </template>
        </template>
      </el-table-column>
    </el-table>
    <DashboardPagination :pagination="pagination" @change="change" />
  </div>
  <el-dialog
    :model-value="visible"
    :width="560"
    :title="$t('operationDetails')"
    class="operation-wrap"
    @update:model-value="(value: boolean) => { if (!value) cancel() }"
  >
    {{ activeCopyText }}
    <template #footer>
      <el-button type="primary" @click="onCopy">
        {{ $t('copy') }}
      </el-button>
    </template>
  </el-dialog>
</template>

<style lang="less">
.table-operations {
  padding: 12px 0;

  .el-table__body > tr > td {
    white-space: pre;
  }

  .text-active {
    color: #1890ff;
    cursor: pointer;
  }
}

.operation-wrap .el-dialog__body {
  max-height: 360px;
  overflow-y: auto;
  white-space: pre;
}
</style>
