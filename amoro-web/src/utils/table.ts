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

interface DashboardColumn {
  dataIndex: string
  title: string
  width?: number | string
  minWidth?: number
  ellipsis?: boolean
  sorter?: boolean
}

const descendingFirst: ('descending' | 'ascending' | null)[] = ['descending', 'ascending', null]

// Element Plus fits flexible columns by the declared minimum widths. Convert
// percentage widths to relative weights; retain fixed numeric widths as pixels.
// https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/table.md
export function tableColumnProps(column: DashboardColumn, columns: readonly DashboardColumn[]) {
  let minWidth = column.minWidth
  if (typeof column.width === 'string' && column.width.endsWith('%')) {
    minWidth = Number.parseFloat(column.width) * 5
  }
  else if (minWidth === undefined && column.width === undefined) {
    const percentages = columns.filter(item => typeof item.width === 'string' && item.width.endsWith('%'))
    if (percentages.length) {
      const remaining = 100 - percentages.reduce((sum, item) => sum + Number.parseFloat(String(item.width)), 0)
      minWidth = Math.max(80, 5 * remaining / columns.filter(item => item.width === undefined).length)
    }
  }
  return {
    prop: column.dataIndex,
    label: column.title,
    width: typeof column.width === 'number' ? column.width : undefined,
    minWidth,
    showOverflowTooltip: column.ellipsis ?? false,
    sortable: column.sorter ? 'custom' as const : false,
    sortOrders: column.sorter ? descendingFirst : undefined,
  }
}
