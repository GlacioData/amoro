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
import { onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import SingleDataCard from './components/SingleDataCard.vue'
import MultipleDataCard from './components/MultipleDataCard.vue'
import Top10TablesCard from './components/Top10TablesCard.vue'
import ResourceUsageCard from './components/ResourceUsageCard.vue'
import DataSizeCard from './components/DataSizeCard.vue'
import PieChartCard from './components/PieChartCard.vue'
import { usePageScroll } from '@/hooks/usePageScroll'
import { getOverviewOptimizingStatus, getOverviewSummary } from '@/services/overview.service'
import { bytesToSize } from '@/utils'
import type { IKeyAndValue } from '@/types/common.type'

const { t } = useI18n()
const { pageScrollRef } = usePageScroll()

const singleData = ref<IKeyAndValue[]>([])
const multipleData = ref<IKeyAndValue[]>([])
const optimizingStatusData = ref<{ value: number, name: string }[]>([])

async function getCurOverviewData() {
  const summaryResult = await getOverviewSummary()
  const tableSize = bytesToSize(summaryResult.tableTotalSize)
  const memorySize = bytesToSize(summaryResult.totalMemory)
  singleData.value.push({ key: 'catalog', value: summaryResult.catalogCnt })
  singleData.value.push({ key: 'table', value: summaryResult.tableCnt })
  singleData.value.push({ key: 'data', value: tableSize })
  multipleData.value.push({ key: 'cpu', value: summaryResult.totalCpu })
  multipleData.value.push({ key: 'memory', value: memorySize })

  optimizingStatusData.value = await getOverviewOptimizingStatus()
}

onMounted(() => {
  getCurOverviewData()
})
</script>

<template>
  <div :ref="el => { pageScrollRef = el as HTMLElement | null }" class="page-scroll">
    <div :style="{ background: '#F8F7F8', padding: '24px', minHeight: '900px' }" class="overview-content">
      <el-row :gutter="16" class="overview-grid">
        <el-col v-for="(card, index) in singleData" :key="index" :span="6" :xs="12">
          <SingleDataCard :title="t(card.key)" :value="card.value" />
        </el-col>
        <el-col :span="6" :xs="12">
          <MultipleDataCard :title="t('resource')" :data="multipleData" />
        </el-col>
        <el-col :span="12" :xs="24">
          <ResourceUsageCard />
        </el-col>
        <el-col :span="12" :xs="24">
          <DataSizeCard />
        </el-col>
        <el-col :span="12" :xs="24">
          <PieChartCard :title="t('optimizingStatus')" :data="optimizingStatusData" />
        </el-col>
        <el-col :span="12" :xs="24">
          <Top10TablesCard />
        </el-col>
      </el-row>
    </div>
  </div>
</template>

<style lang="less" scoped>
.page-scroll {
  height: 100%;
  overflow-y: auto;
}

.overview-grid {
  row-gap: 8px;

  :deep(.el-card) {
    --el-card-padding: 24px;
    --el-card-border-color: #ededed;
    border-radius: 8px;
    box-shadow: none;
  }

  :deep(.el-card__header) {
    min-height: 56px;
    padding: 12px 24px;
    display: flex;
    align-items: center;
    font-weight: 600;
    color: rgba(0, 0, 0, 0.88);
  }

  @media (max-width: 760px) {
    :deep(.el-card) { --el-card-padding: 12px; }
    :deep(.el-card__header) { padding: 8px 12px; }
    :deep(.card-title) { font-size: 18px; }
    :deep(.el-card__header > .el-row) { gap: 8px; }
  }

  :deep(.el-card__header > .el-row) {
    width: 100%;
    align-items: center;
  }
}
</style>
