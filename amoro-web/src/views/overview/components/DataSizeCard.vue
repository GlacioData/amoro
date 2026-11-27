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
import { buildDataSizeOption } from '../overview-state'
import { getDataSizeList } from '@/services/overview.service'
import { bytesToSize, dateFormat } from '@/utils'
import type { DataSizeItem } from '@/types/common.type'
import Chart from '@/components/echarts/Chart.vue'

const { t } = useI18n()
const timeRange = ref('24')
const loading = ref<boolean>(false)
const seriesData = reactive({
  times: [] as string[],
  values: [] as number[],
})

const dataSizeChartOption = computed(() => buildDataSizeOption(
  seriesData,
  t('dataSize'),
  bytesToSize,
))

async function updateData() {
  try {
    loading.value = true
    const times: string[] = []
    const dataSizeList: number[] = []

    const startTime = Date.now() - Number.parseFloat(timeRange.value) * 60 * 60 * 1000
    const result: DataSizeItem[] = await getDataSizeList(startTime)
    ;(result || []).forEach((item) => {
      times.push(dateFormat(item.ts))
      dataSizeList.push(item.dataSize)
    })
    seriesData.times = times
    seriesData.values = dataSizeList
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

onMounted(() => {
  updateData()
})
</script>

<template>
  <el-card>
    <template #header>
      <el-row justify="space-between">
        <span class="card-title" v-text="t('dataSize')" />
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <el-select v-model="timeRange" style="width: 120px" @change="updateData">
            <el-option :label="t('last1h')" value="1" />
            <el-option :label="t('last12h')" value="12" />
            <el-option :label="t('last24h')" value="24" />
            <el-option :label="t('last7day')" value="168" />
          </el-select>
        </div>
      </el-row>
    </template>

    <Chart :loading="loading" :options="dataSizeChartOption" />
  </el-card>
</template>

<style scoped>
.card-title {
  font-size: 18px;
}
</style>
