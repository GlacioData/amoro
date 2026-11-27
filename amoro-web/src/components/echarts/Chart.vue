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

<script lang="ts">
import { defineComponent, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import echarts from './index'

export default defineComponent({
  props: {
    width: {
      type: String,
      default: 'auto',
    },
    height: {
      type: String,
      default: '350px',
    },
    loading: {
      type: Boolean,
      default: false,
    },
    options: {
      type: Object,
      default: () => ({}),
    },
  },
  setup(props) {
    const echart = ref<HTMLDivElement>()
    let chart: ReturnType<typeof echarts.init> | undefined
    let observer: ResizeObserver | undefined

    watch(
      () => props.options,
      (value) => {
        if (value)
          chart?.setOption(value, { notMerge: true })
      },
      {
        deep: true,
      },
    )

    onMounted(() => {
      const container = echart.value
      if (!container)
        return
      chart = echarts.init(container, 'v5')
      chart.setOption(props.options ?? {})
      // The table sidebar can change the chart area without resizing the viewport.
      observer = new ResizeObserver(() => {
        const { clientWidth, clientHeight } = container
        if (chart && clientWidth > 0 && clientHeight > 0
          && (clientWidth !== chart.getWidth() || clientHeight !== chart.getHeight())) {
          chart.resize()
        }
      })
      observer.observe(container)
    })

    onBeforeUnmount(() => {
      observer?.disconnect()
      chart?.dispose()
      chart = undefined
    })

    return {
      echart,
    }
  },
})
</script>

<template>
  <div v-loading="loading" class="echarts-loading">
    <div ref="echart" :style="{ width, height }" class="timeline-echarts" />
  </div>
</template>

<style lang="less">
.echarts-loading {
  width: 100% !important;
}
.timeline-echarts {
  .echarts-tooltip-dark {
    background-color: rgba(0,0,0,.7) !important;
    line-height: 20px !important;
    border: 1px solid #E9EBF1 !important;
  }
}
</style>
