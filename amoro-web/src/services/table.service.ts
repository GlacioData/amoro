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

// import { IOptions } from '@/types/common.type'
import type { ICatalogItem, IMap, SnapshotItem } from '@/types/common.type'
import request from '@/utils/request'
import { deletedTag, savedTag, updatedTag, tagMutation } from './tag-mutation'

export function getCatalogList(): Promise<ICatalogItem[]> {
  return request.get('api/ams/v1/catalogs')
}
export function getDatabaseList(params: {
  catalog: string
  keywords: string
}): Promise<string[]> {
  const { catalog, keywords } = params
  return request.get(`api/ams/v1/catalogs/${catalog}/databases`, { params: { keywords } })
}

export function getTableList(params: {
  catalog: string
  db: string
  keywords: string
}) {
  const { catalog, db, keywords } = params
  return request.get(`api/ams/v1/catalogs/${catalog}/databases/${db}/tables`, { params: { keywords } })
}

// get tables detail
export function getTableDetail(
  { catalog = '' as string, db = '' as string, table = '' as string, token = '' as string },
) {
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/details`, { params: { token } })
}

export function getHiveTableDetail(
  { catalog = '' as string, db = '' as string, table = '' as string },
) {
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/hive/details`)
}

export function getUpgradeStatus(
  { catalog = '' as string, db = '' as string, table = '' as string },
) {
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/upgrade/status`)
}
// get partions table
export function getPartitionTable(
  params: {
    catalog: string
    db: string
    table: string
    filter: string
    page: number
    pageSize: number
    token?: string
  },
) {
  const { catalog, db, table, filter, page, pageSize, token } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/partitions`, { params: { filter, page, pageSize, token } })
}

// get partions
export function getPartitions(
  params: {
    catalog: string
    db: string
    table: string
    page: number
    pageSize: number
    token: string
  },
) {
  const { catalog, db, table, page, pageSize, token } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/partitions`, { params: { page, pageSize, token } })
}
// get partions-files
export function getPartitionFiles(
  params: {
    catalog: string
    db: string
    table: string
    partition: string | null
    specId: number
    page: number
    pageSize: number
    token?: string
  },
) {
  const { catalog, db, table, partition, specId, page, pageSize, token } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/partitions/${partition}/files`, { params: { specId, page, pageSize, token } })
}
// get snapshots
export function getSnapshots(
  params: {
    catalog: string
    db: string
    table: string
    page: number
    pageSize: number
    token?: string
    ref: string
    operation: string
    lastSnapshot?: string
  },
  handleError = true,
): Promise<{ list: SnapshotItem[], total: number }> {
  const { catalog, db, table, page, pageSize, token, ref, operation, lastSnapshot } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/snapshots`, { handleError, params: { page, pageSize, token, ref, operation, lastSnapshot } })
}

// get Snapshot detail
export function getDetailBySnapshotId(
  params: {
    catalog: string
    db: string
    table: string
    ref: string
    snapshotId: string
    page: number
    pageSize: number
    token?: string
  },
) {
  const { catalog, db, table, snapshotId, page, pageSize, ref, token } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/snapshots/${snapshotId}/detail`, { params: { page, pageSize, ref, token } })
}
// The bounded protocol deliberately has no page/offset field.
export function getSnapshotDetailPage(params: {
  catalog: string, db: string, table: string, ref: string, snapshotId: string,
  pageSize: number, cursor?: string, refType?: string, scopeKind?: string, branch?: string,
}): Promise<{ list: any[], nextCursor: string | null }> {
  const { catalog, db, table, snapshotId, ...query } = params
  return request.get(`${tagTablePath({ catalog, db, table })}/snapshots/${encodeURIComponent(snapshotId)}/detail`, { handleError: false, params: query })
}

// get operations
export function getOperations(
  params: {
    catalog: string
    db: string
    table: string
    page: number
    pageSize: number
    token?: string
  },
) {
  const { catalog, db, table, page, pageSize, token } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/operations`, { params: { page, pageSize, token } })
}
// get optimizing processes
export function getOptimizingProcesses(
  params: {
    catalog: string
    db: string
    table: string
    type: string
    status: string
    page: number
    pageSize: number
    token?: string
    lastSnapshot?: string
  },
) {
  const { catalog, db, table, type, status, page, pageSize, token, lastSnapshot } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/optimizing-processes`, { params: { page, pageSize, token, type, status, lastSnapshot } })
}

// get optimizing process types
export function getTableOptimizingTypes(
  params: {
    catalog: string
    db: string
    table: string
    token?: string
  },
) {
  const { catalog, db, table, token } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/optimizing-types`, { params: { token } })
}

// get optimizing tasks
export function getTasksByOptimizingProcessId(
  params: {
    catalog: string
    db: string
    table: string
    processId: string
    page: number
    pageSize: number
    token?: string
  },
) {
  const { catalog, db, table, processId, page, pageSize, token } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/optimizing-processes/${processId}/tasks`, { params: { page, pageSize, token } })
}

export function getOptimizingProcessError(params: {
  catalog: string
  db: string
  table: string
  processId: string
}): Promise<string> {
  const { catalog, db, table, processId } = params
  return request.get(
    `api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/optimizing-processes/${processId}/error`,
    { rawData: true, responseType: 'text' },
  )
}

export function getOptimizingTaskError(params: {
  catalog: string
  db: string
  table: string
  processId: string
  taskId: number
}): Promise<string> {
  const { catalog, db, table, processId, taskId } = params
  return request.get(
    `api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/optimizing-processes/${processId}/tasks/${taskId}/error`,
    { rawData: true, responseType: 'text' },
  )
}

// get table processes
export function getTableProcesses(
  params: {
    catalog: string
    db: string
    table: string
    type: string
    status: string
    page: number
    pageSize: number
    token?: string
  },
) {
  const { catalog, db, table, type, status, page, pageSize, token } = params
  return request.get(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/processes`, { params: { page, pageSize, token, type, status } })
}

export function upgradeHiveTable(
  { catalog = '' as string, db = '' as string, table = '' as string, properties = {} as IMap<string>, pkList = [] as IMap<string>[] },
) {
  return request.post(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/upgrade`, {
    properties,
    pkList,
  })
}

export function getUpgradeProperties() {
  return request.get('api/ams/v1/upgrade/properties')
}

export function cancelOptimizingProcess(
  { catalog = '' as string, db = '' as string, table = '' as string, processId = '' as string },
) {
  return request.post(`api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/optimizing-processes/${processId}/cancel`)
}

export function getBranches(params: { catalog: string, db: string, table: string }) {
  const { catalog, db, table } = params
  return request.get(`/api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/branches`)
}

export function tagTablePath(source: { catalog: string, db: string, table: string }) {
  return `/api/ams/v1/tables/catalogs/${encodeURIComponent(source.catalog)}/dbs/${encodeURIComponent(source.db)}/tables/${encodeURIComponent(source.table)}`
}
export function getTags(params: { catalog: string, db: string, table: string, page?: number, pageSize?: number, keyword?: string, scopeKind?: string, branch?: string }): Promise<{ list: import('@/types/tag.type').TableTag[], total: number }> {
  const { catalog, db, table, ...query } = params
  return request.get(`${tagTablePath({ catalog, db, table })}/tags`, { params: query, handleError: false })
}
export function createTag(source: { catalog: string, db: string, table: string }, body: import('@/types/tag.type').TagWriteRequest, expected: import('@/types/tag.type').TagRetention) {
  return tagMutation(() => request.post(`${tagTablePath(source)}/tags`, body, { handleError: false, returnCode: true }), savedTag(body.name || '', body.target?.id, body.scope, expected))
}
export function copyTag(source: { catalog: string, db: string, table: string }, name: string, body: import('@/types/tag.type').TagWriteRequest, expectedVersion: string, expected: import('@/types/tag.type').TagRetention) {
  return tagMutation(() => request.post(`${tagTablePath(source)}/tags/${encodeURIComponent(name)}/copy`, body, { handleError: false, returnCode: true }), savedTag(body.newName || '', expectedVersion, body.scope, expected))
}
export function deleteTag(source: { catalog: string, db: string, table: string }, row: import('@/types/tag.type').TableTag) {
  return tagMutation(() => request.delete(`${tagTablePath(source)}/tags/${encodeURIComponent(row.name)}`, { params: { scopeKind: row.scope.kind, branch: row.scope.branch }, headers: { 'If-Match': `"${row.observationToken}"` }, handleError: false, returnCode: true }), deletedTag(row.name, row.scope))
}
export function updateTagRetention(source: { catalog: string, db: string, table: string }, row: import('@/types/tag.type').TableTag, body: import('@/types/tag.type').TagRetentionUpdateRequest, expected: import('@/types/tag.type').TagRetention) {
  return tagMutation(() => request.post(`${tagTablePath(source)}/tags/${encodeURIComponent(row.name)}/retention`, body, { headers: { 'If-Match': `"${row.observationToken}"` }, handleError: false, returnCode: true }), updatedTag(row, expected))
}
export function getConsumers(params: { catalog: string, db: string, table: string }) {
  const { catalog, db, table } = params
  return request.get(`/api/ams/v1/tables/catalogs/${catalog}/dbs/${db}/tables/${table}/consumers`)
}
