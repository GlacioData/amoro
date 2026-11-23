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
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { tableColumnProps } from '@/utils/table'
import type { DetailColumnItem, IColumns } from '@/types/common.type'

const props = defineProps<{ schema: DetailColumnItem[], partitionColumnList: DetailColumnItem[] }>()

const { t } = useI18n()

const primaryColumns = computed<IColumns[]>(() => [
  { title: t('field'), dataIndex: 'field', width: '30%' },
  { title: t('type'), dataIndex: 'type', width: '30%' },
  { title: t('description'), dataIndex: 'comment', ellipsis: true },
])
const partitionColumns = computed<IColumns[]>(() => [
  { title: t('field'), dataIndex: 'field', width: '30%' },
  { title: t('type'), dataIndex: 'type', width: '30%' },
  { title: t('description'), dataIndex: 'comment', ellipsis: true },
])
</script>

<template>
  <div class="hive-table-detail g-flex">
    <div class="left-content">
      <div v-if="props.partitionColumnList && props.partitionColumnList.length" class="table-attrs">
        <p class="attr-title">
          {{ $t('partitionKey') }}
        </p>
        <el-table row-key="field" :data="props.partitionColumnList">
          <el-table-column v-for="column in partitionColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, partitionColumns)" />
        </el-table>
      </div>
      <div class="table-attrs">
        <p class="attr-title">
          {{ $t('schema') }}
        </p>
        <el-table row-key="field" :data="props.schema">
          <el-table-column v-for="column in primaryColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, primaryColumns)" />
        </el-table>
      </div>
    </div>
  </div>
</template>

<style lang="less" scoped>
.hive-table-detail {
  .left-content {
    padding: 0 24px 12px;
    width: 66%;
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
