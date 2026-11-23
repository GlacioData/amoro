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
import { defineAsyncComponent, defineComponent, h } from 'vue'
import { useI18n } from 'vue-i18n'
import type { ECOption } from '@/components/echarts'
import Loading from '@/components/loading/loading.vue'

defineProps<{ recordOptions: ECOption, fileOptions: ECOption, loading: boolean }>()

const Chart = defineAsyncComponent({
  loader: () => import('@/components/echarts/Chart.vue'),
  loadingComponent: defineComponent({
    inheritAttrs: false,
    setup() {
      const { t } = useI18n()
      return () => h(Loading, { loadingText: t('loading') })
    },
  }),
  delay: 0,
})
</script>

<template>
  <div class="snapshot-chart-grid">
    <div v-for="(options, index) in [recordOptions, fileOptions]" :key="index" class="snapshot-chart">
      <Chart height="300px" :loading="loading" :options="options" />
    </div>
  </div>
</template>

<style scoped>
.snapshot-chart {
  position: relative;
  min-width: 0;
  min-height: 300px;
}
</style>
