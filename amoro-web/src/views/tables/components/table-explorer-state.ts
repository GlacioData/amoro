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

export function createBranchLoader<T>() {
  const pending = new Map<string, { revision: number, result: Promise<T> }>()

  return (key: string, revision: number, load: () => Promise<T>) => {
    const active = pending.get(key)
    if (active?.revision === revision) {
      return active.result
    }

    const result = Promise.resolve().then(load).finally(() => {
      if (pending.get(key)?.result === result) {
        pending.delete(key)
      }
    })
    pending.set(key, { revision, result })
    return result
  }
}

export function reconcileTreeNodes<T extends { key: string, tableType?: string }>(
  previous: T[] | undefined,
  incoming: T[],
): T[] {
  if (!previous) {
    return incoming
  }
  if (previous.length === incoming.length && incoming.every((node, index) =>
    node.key === previous[index].key && node.tableType === previous[index].tableType,
  )) {
    return previous
  }

  const byKey = new Map(previous.map(node => [node.key, node]))
  return incoming.map((node) => {
    const existing = byKey.get(node.key)
    return existing && existing.tableType === node.tableType ? existing : node
  })
}

export function mergeExpandedKeys(defaults: string[], overrides: ReadonlyMap<string, boolean>): string[] {
  const expanded = new Set(defaults)
  for (const [key, open] of overrides) {
    if (open) {
      expanded.add(key)
    }
    else {
      expanded.delete(key)
    }
  }
  return [...expanded]
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

export function getStoredCatalogExpandedKeys(storedExpandedKeys: string[]): string[] {
  return storedExpandedKeys.filter(
    key => key.startsWith('catalog:') && !key.includes('/db:'),
  )
}
