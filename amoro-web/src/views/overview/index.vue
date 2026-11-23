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

const singleData = ref<IKeyAndValue<string | number>[]>([])
const multipleData = ref<IKeyAndValue<string | number>[]>([])
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
    <div class="overview-content">
      <div class="overview-summary">
        <SingleDataCard v-for="card in singleData" :key="card.key" :title="t(card.key)" :value="card.value" />
        <MultipleDataCard :title="t('resource')" :data="multipleData" />
      </div>
      <div class="overview-panels">
        <ResourceUsageCard />
        <DataSizeCard />
        <PieChartCard :title="t('optimizingStatus')" :data="optimizingStatusData" />
        <Top10TablesCard />
      </div>
    </div>
  </div>
</template>

<style lang="less" scoped>
.page-scroll {
  height: 100%;
  overflow-y: auto;
}

.overview-content {
  container-type: inline-size;
  box-sizing: border-box;
  min-height: 100%;
  padding: 24px;
  background: #f8f7f8;

  :deep(.el-card) {
    --el-card-padding: 24px;
    --el-card-border-color: #ededed;
    min-width: 0;
    border-radius: 8px;
    box-shadow: none;
  }

  :deep(.el-card__header) {
    display: flex;
    flex: none;
    align-items: center;
    min-height: 56px;
    padding: 12px var(--el-card-padding);
    font-weight: 600;
    color: rgba(0, 0, 0, 0.88);
  }

  :deep(.el-card__header > .el-row) {
    width: 100%;
    align-items: center;
    gap: 8px;
  }

  :deep(.el-card__body) {
    overflow: visible;
  }

  @container (max-width: 1280px) {
    :deep(.el-card) {
      --el-card-padding: 16px;
    }
  }

  @media (max-width: 760px) {
    padding: 12px;

    :deep(.el-card) {
      --el-card-padding: 12px;
    }
  }
}

.overview-summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 16px;
  margin-bottom: 16px;

  :deep(.el-card) {
    min-height: 150px;
  }

  :deep(.el-card__body) {
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding-block: 16px;
  }

  :deep(.card-title),
  :deep(.el-statistic) {
    font-size: clamp(20px, 2.25cqi, 28px);
    overflow-wrap: anywhere;
  }

  :deep(.el-statistic) {
    --el-statistic-content-font-size: 1em;
    --el-statistic-content-color: #07a7f0;
  }

  @container (max-width: 960px) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  @container (max-width: 460px) {
    grid-template-columns: minmax(0, 1fr);

    :deep(.el-card) {
      min-height: 112px;
    }

    :deep(.el-card__header) {
      min-height: 44px;
      padding-block: 8px;
    }

    :deep(.el-card__body) {
      padding-block: 12px;
    }
  }
}

.overview-panels {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 16px;

  // Allow for the ranking table's minimum column widths and card padding.
  @container (width < 1184px) {
    grid-template-columns: minmax(0, 1fr);
  }
}
</style>
