/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import assert from 'node:assert/strict'
import test from 'node:test'
import {
  buildDataSizeOption,
  buildResourceUsageOption,
  formatDataSizeTooltip,
  formatResourceTooltip,
  loadOverviewSections,
} from '../src/views/overview/overview-state.ts'

const stubBytes = (n: number) => `${n}B`

test('formatResourceTooltip handles hidden legend series without crashing or mislabeling', () => {
  const labels = { cpu: 'CPU', memory: 'Memory' }
  assert.equal(formatResourceTooltip([], labels, stubBytes), '')

  const onlyMemory = formatResourceTooltip([
    { seriesId: 'memory', seriesName: 'Memory', axisValue: '10:00', color: '#0f0', value: 2048 },
  ], labels, stubBytes)
  assert.match(onlyMemory, /10:00/)
  assert.match(onlyMemory, /Memory: 2048B/)
  assert.doesNotMatch(onlyMemory, /Core/)

  const onlyCpu = formatResourceTooltip([
    { seriesId: 'cpu', seriesName: 'CPU', axisValue: '10:00', color: '#00f', value: 8 },
  ], labels, stubBytes)
  assert.match(onlyCpu, /CPU: 8 Core/)
  assert.doesNotMatch(onlyCpu, /Memory/)
})

test('formatDataSizeTooltip returns empty string for empty params and formats valid point', () => {
  assert.equal(formatDataSizeTooltip([], 'Data Size', stubBytes), '')
  const html = formatDataSizeTooltip([
    { seriesName: 'Data Size', axisValue: '12:00', color: '#1890ff', value: 4096 },
  ], 'Data Size', stubBytes)
  assert.match(html, /12:00/)
  assert.match(html, /Data Size: 4096B/)
})

test('chart option builders start empty (no -1 sentinel) and reflect updated locale labels', () => {
  const enOption = buildResourceUsageOption(
    { times: [], cpu: [], memory: [] },
    { cpu: 'CPU', cpuCores: 'CPU Cores', memory: 'Memory' },
    stubBytes,
  )
  assert.deepEqual(enOption.xAxis.data, [])
  assert.deepEqual(enOption.series[0].data, [])
  assert.deepEqual(enOption.series[1].data, [])
  assert.deepEqual(enOption.legend.data, ['CPU', 'Memory'])

  const zhOption = buildResourceUsageOption(
    { times: ['10:00'], cpu: [4], memory: [1024] },
    { cpu: 'CPU', cpuCores: 'CPU 核数', memory: '内存' },
    stubBytes,
  )
  assert.deepEqual(zhOption.legend.data, ['CPU', '内存'])
  assert.equal(zhOption.yAxis[1].name, '内存')

  const dataOption = buildDataSizeOption({ times: [], values: [] }, '数据规模', stubBytes)
  assert.deepEqual(dataOption.xAxis.data, [])
  assert.deepEqual(dataOption.series[0].data, [])
  assert.deepEqual(dataOption.legend.data, ['数据规模'])
})

test('loadOverviewSections isolates summary and optimizing status failures', async () => {
  const result = await loadOverviewSections(
    async () => { throw new Error('summary timeout') },
    async () => [{ name: 'Idle', value: 5 }],
  )
  assert.equal(result.summaryError, 'summary timeout')
  assert.equal(result.summary, null)
  assert.equal(result.statusError, '')
  assert.deepEqual(result.optimizingStatus, [{ name: 'Idle', value: 5 }])
})

test('tooltip formatters escape HTML metacharacters to prevent XSS injection', () => {
  const malicious = formatResourceTooltip([
    {
      seriesId: 'cpu',
      seriesName: '<img src=x onerror=alert(1)>',
      axisValue: '<script>evil()</script>',
      color: '#00f;background:url(javascript:alert(1))',
      value: 4,
    },
  ], { cpu: 'CPU', memory: 'Memory' }, stubBytes)

  assert.doesNotMatch(malicious, /<script>|<img\b|javascript:/i)
  assert.match(malicious, /&lt;script&gt;evil\(\)&lt;\/script&gt;/)
  assert.match(malicious, /&lt;img src=x onerror=alert\(1\)&gt;/)

  const maliciousData = formatDataSizeTooltip([
    { axisValue: '<b>12:00</b>', color: 'red" onclick="alert(1)', value: 10 },
  ], 'Size <1>', stubBytes)
  assert.doesNotMatch(maliciousData, /<b>|onclick=/i)
  assert.match(maliciousData, /&lt;b&gt;12:00&lt;\/b&gt;/)
  assert.match(maliciousData, /Size &lt;1&gt;/)
})
