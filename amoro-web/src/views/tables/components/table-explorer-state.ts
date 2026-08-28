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

export interface TableCache<T> {
  values: Record<string, T[]>
  pending: Map<string, Promise<T[]>>
  generation: number
}

export interface TableSearchContext {
  catalog: string
  database: string
}

export interface TableSearchTarget {
  catalog: string
  database: string
  keyword: string
}

export interface TableSearchPlan {
  filterKeyword: string
  target?: TableSearchTarget
}

function tableCacheKey(catalog: string, database: string): string {
  return JSON.stringify([catalog, database])
}

export function createTableCache<T>(): TableCache<T> {
  return {
    values: {},
    pending: new Map(),
    generation: 0,
  }
}

export async function loadTablesWithCache<T>(
  cache: TableCache<T>,
  catalog: string,
  database: string,
  fetchTables: () => Promise<T[]>,
): Promise<T[]> {
  const key = tableCacheKey(catalog, database)
  if (Object.prototype.hasOwnProperty.call(cache.values, key)) {
    return cache.values[key]
  }

  const pendingRequest = cache.pending.get(key)
  if (pendingRequest) {
    return pendingRequest
  }

  const generation = cache.generation
  const request = fetchTables()
    .then((tables) => {
      if (cache.generation === generation) {
        cache.values[key] = tables
      }
      return tables
    })
    .finally(() => {
      if (cache.pending.get(key) === request) {
        cache.pending.delete(key)
      }
    })

  cache.pending.set(key, request)
  return request
}

export function clearTableCache<T>(cache: TableCache<T>): void {
  cache.generation += 1
  cache.pending.clear()
  Object.keys(cache.values).forEach(key => delete cache.values[key])
}

export function getTableCacheGeneration<T>(cache: TableCache<T>): number {
  return cache.generation
}

export function getCachedTables<T>(
  cache: TableCache<T>,
  catalog: string,
  database: string,
): T[] | undefined {
  return cache.values[tableCacheKey(catalog, database)]
}

export function createTableSearchPlan(
  rawKeyword: string,
  context: TableSearchContext,
): TableSearchPlan {
  const filterKeyword = rawKeyword.trim()
  const parts = filterKeyword.split('.').map(part => part.trim())
  if (!filterKeyword || parts.some(part => !part)) {
    return { filterKeyword }
  }

  if (parts.length >= 3) {
    return {
      filterKeyword,
      target: {
        catalog: parts[0],
        database: parts[1],
        keyword: parts.slice(2).join('.'),
      },
    }
  }

  if (parts.length === 2) {
    return { filterKeyword }
  }

  const catalog = context.catalog.trim()
  const database = context.database.trim()
  if (!catalog || !database) {
    return { filterKeyword }
  }

  return {
    filterKeyword,
    target: { catalog, database, keyword: parts[0] },
  }
}

export function getSelectedPathExpandedKeys(
  currentExpandedKeys: string[],
  catalog: string,
): string[] {
  const safeCatalog = catalog.trim()
  if (!safeCatalog) {
    return currentExpandedKeys
  }

  const catalogKey = `catalog:${safeCatalog}`
  return currentExpandedKeys.includes(catalogKey)
    ? currentExpandedKeys
    : [...currentExpandedKeys, catalogKey]
}

export function getStoredCatalogExpandedKeys(storedExpandedKeys: string[]): string[] {
  return storedExpandedKeys.filter(
    key => key.startsWith('catalog:') && !key.includes('/db:'),
  )
}

export function getExpandedDatabaseKeys(expandedKeys: string[]): string[] {
  return expandedKeys.filter(
    key => key.startsWith('catalog:') && key.includes('/db:') && !key.includes('/table:'),
  )
}

export async function refreshExpandedDatabaseNodes<T extends { key: string }>(
  databaseNodes: T[],
  expandedKeys: string[],
  clearCollapsedNode: (node: T) => void,
  reloadExpandedNode: (node: T) => Promise<void>,
): Promise<void> {
  const expandedDatabaseKeys = new Set(getExpandedDatabaseKeys(expandedKeys))
  const reloads: Promise<void>[] = []

  databaseNodes.forEach((node) => {
    if (expandedDatabaseKeys.has(node.key)) {
      reloads.push(Promise.resolve().then(() => reloadExpandedNode(node)))
    }
    else {
      clearCollapsedNode(node)
    }
  })

  await Promise.allSettled(reloads)
}
