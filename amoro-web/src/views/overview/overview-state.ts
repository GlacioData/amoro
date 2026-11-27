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

export interface ChartTooltipItem {
  seriesId?: string
  seriesName?: string
  axisValue?: string | number
  color?: string
  value?: number
}

export interface ResourceSeriesData {
  times: string[]
  cpu: number[]
  memory: number[]
}

export interface DataSizeSeriesData {
  times: string[]
  values: number[]
}

export interface OverviewSummary {
  catalogCnt: number
  tableCnt: number
  tableTotalSize: number
  totalCpu: number
  totalMemory: number
}

export interface OverviewOptimizingStatusItem {
  name: string
  value: number
}

function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function sanitizeCssColor(value: unknown): string {
  if (typeof value !== 'string')
    return ''
  const trimmed = value.trim()
  if (/^#(?:[0-9a-f]{3}|[0-9a-f]{4}|[0-9a-f]{6}|[0-9a-f]{8})$/i.test(trimmed))
    return trimmed
  if (/^(?:rgb|rgba|hsl|hsla)\([0-9.,%\s/]+\)$/i.test(trimmed))
    return trimmed
  if (/^[a-z]+$/i.test(trimmed))
    return trimmed
  return ''
}

export function formatResourceTooltip(
  params: readonly ChartTooltipItem[] | undefined,
  labels: { cpu: string, memory: string },
  formatBytes: (size: number | null) => string,
): string {
  if (!params?.length)
    return ''

  const axisValue = escapeHtml(params[0]?.axisValue ?? '')
  let html = `<span style="font-size: 12px">${axisValue}</span><br/>`
  for (const item of params) {
    const isMemory = item.seriesId === 'memory' || item.seriesName === labels.memory
    const formatted = escapeHtml(isMemory
      ? formatBytes(item.value ?? null)
      : `${item.value ?? 0} Core`)
    const name = escapeHtml(item.seriesName || (isMemory ? labels.memory : labels.cpu))
    const color = sanitizeCssColor(item.color)
    html += `<span style="display: inline-block;background-color:${color}; margin-right: 6px; width: 6px;height: 6px;"></span>${name}: ${formatted}<br/>`
  }
  return html
}

export function formatDataSizeTooltip(
  params: readonly ChartTooltipItem[] | undefined,
  label: string,
  formatBytes: (size: number | null) => string,
): string {
  if (!params?.length)
    return ''

  const point = params[0]
  const dataSize = escapeHtml(formatBytes(point?.value ?? null))
  const axisValue = escapeHtml(point?.axisValue ?? '')
  const color = sanitizeCssColor(point?.color)
  let html = `<span style="font-size: 12px">${axisValue}</span><br/>`
  html += `<span style="display: inline-block;background-color:${color}; margin-right: 6px; width: 6px;height: 6px;"></span>${escapeHtml(label)}: ${dataSize}<br/>`
  return html
}

export function buildResourceUsageOption(
  seriesData: ResourceSeriesData,
  labels: { cpu: string, cpuCores: string, memory: string },
  formatBytes: (size: number | null) => string,
) {
  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: ChartTooltipItem[]) => formatResourceTooltip(params, labels, formatBytes),
    },
    legend: { data: [labels.cpu, labels.memory] },
    xAxis: { type: 'category', data: seriesData.times },
    yAxis: [
      { type: 'value', name: labels.cpuCores, axisLabel: { formatter: '{value}' } },
      { type: 'value', name: labels.memory, axisLabel: { formatter: (value: number) => `${formatBytes(value)}` } },
    ],
    series: [
      { id: 'cpu', name: labels.cpu, type: 'line', yAxisIndex: 0, data: seriesData.cpu },
      { id: 'memory', name: labels.memory, type: 'line', yAxisIndex: 1, data: seriesData.memory },
    ],
  }
}

export function buildDataSizeOption(
  seriesData: DataSizeSeriesData,
  label: string,
  formatBytes: (size: number | null) => string,
) {
  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: ChartTooltipItem[]) => formatDataSizeTooltip(params, label, formatBytes),
    },
    legend: { data: [label] },
    xAxis: {
      type: 'category',
      data: seriesData.times,
    },
    yAxis: {
      type: 'value',
      axisLabel: { formatter: (value: number) => `${formatBytes(value)}` },
    },
    series: [
      {
        id: 'dataSize',
        name: label,
        type: 'line',
        data: seriesData.values,
      },
    ],
  }
}

export async function loadOverviewSections<TSummary, TStatus>(
  fetchSummary: () => Promise<TSummary>,
  fetchOptimizingStatus: () => Promise<TStatus[]>,
): Promise<{
  summary: TSummary | null
  summaryError: string
  optimizingStatus: TStatus[]
  statusError: string
}> {
  const [summaryResult, statusResult] = await Promise.allSettled([
    fetchSummary(),
    fetchOptimizingStatus(),
  ])

  const summary = summaryResult.status === 'fulfilled' ? summaryResult.value : null
  const summaryError = summaryResult.status === 'rejected'
    ? (summaryResult.reason instanceof Error ? summaryResult.reason.message : 'Failed to load overview summary')
    : ''

  const optimizingStatus = statusResult.status === 'fulfilled' ? (statusResult.value || []) : []
  const statusError = statusResult.status === 'rejected'
    ? (statusResult.reason instanceof Error ? statusResult.reason.message : 'Failed to load optimizing status')
    : ''

  return { summary, summaryError, optimizingStatus, statusError }
}
