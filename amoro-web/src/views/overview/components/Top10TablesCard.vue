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
import { useRouter } from 'vue-router'
import { tableColumnProps } from '@/utils/table'
import type { ITopTableItem } from '@/types/common.type'
import { getTop10TableList } from '@/services/overview.service'
import { bytesToSize } from '@/utils'

const { t } = useI18n()
const router = useRouter()
const orderBy = ref('healthScore')
const loading = ref<boolean>(false)
const dataSource = reactive<ITopTableItem[]>([])

const columns = computed(() => [
  {
    title: t('tableName'),
    dataIndex: 'tableName',
    width: 300,
    ellipsis: true,
  },
  {
    title: t('tableSize'),
    dataIndex: 'tableSize',
    width: 100,
    ellipsis: true,
  },
  {
    title: t('fileCount'),
    dataIndex: 'fileCount',
    width: 100,
    ellipsis: true,
  },
  {
    title: t('averageFileSize'),
    dataIndex: 'averageFileSize',
    width: 140,
    ellipsis: true,
  },
  {
    title: t('healthScore'),
    dataIndex: 'healthScore',
    width: 110,
    ellipsis: true,
  },
])

function goTableDetail(record: ITopTableItem) {
  try {
    const table = (record.tableName || '').split('.')
    router.push({
      path: '/tables',
      query: {
        catalog: table[0],
        db: table[1],
        table: table[2],
      },
    })
  }
  catch (error) {
  }
}

async function getTop10Tables() {
  try {
    loading.value = true
    dataSource.length = 0
    const params = {
      order: orderBy.value === 'healthScore' ? 'asc' : 'desc',
      orderBy: orderBy.value,
    }
    const result = await getTop10TableList(params)
    result.forEach((ele: ITopTableItem) => {
      dataSource.push(ele)
    })
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

onMounted(() => {
  getTop10Tables()
})
</script>

<template>
  <el-card class="top-tables-card">
    <template #header>
      <el-row justify="space-between">
        <span class="card-title" v-text="t('top10Tables')" />

        <div style="display: flex; justify-content: space-between; align-items: center;">
          <el-select v-model="orderBy" style="width: 150px" @change="getTop10Tables">
            <el-option :label="t('tableSize')" value="tableSize" />
            <el-option :label="t('fileCount')" value="fileCount" />
            <el-option :label="t('healthScore')" value="healthScore" />
          </el-select>
        </div>
      </el-row>
    </template>

    <div class="list-wrap">
      <el-table v-loading="loading" class="dashboard-table" :data="dataSource" :max-height="390">
        <el-table-column v-for="column in columns" :key="column.dataIndex" v-bind="tableColumnProps(column, columns)">
          <template v-if="column.dataIndex === 'averageFileSize' || column.dataIndex === 'healthScore' || column.dataIndex === 'tableName' || column.dataIndex === 'tableSize'" #default="{ row: record }">
            <template v-if="column.dataIndex === 'tableName'">
              <span :title="record.tableName" class="primary-link" @click="goTableDetail(record)">
                {{ record.tableName }}
              </span>
            </template>
            <template v-if="column.dataIndex === 'tableSize'">
              {{ bytesToSize(record.tableSize) }}
            </template>
            <template v-if="column.dataIndex === 'averageFileSize'">
              {{ bytesToSize(record.averageFileSize) }}
            </template>
            <template v-if="column.dataIndex === 'healthScore'">
              {{ record.healthScore == null || record.healthScore < 0 ? 'N/A' : record.healthScore }}
            </template>
          </template>
        </el-table-column>
      </el-table>
    </div>
  </el-card>
</template>

<style lang="less" scoped>
.card-title {
  font-size: 18px;
}

.top-tables-card {
  height: 500px;
}

.list-wrap {
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
}
</style>
