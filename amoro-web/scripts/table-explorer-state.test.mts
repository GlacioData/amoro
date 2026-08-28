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
// These build-time tests intentionally use the Node test runner to avoid another test dependency.
// eslint-disable-next-line test/no-import-node-test
import test from 'node:test'
import {
  clearTableCache,
  createTableCache,
  createTableSearchPlan,
  getCachedTables,
  getExpandedDatabaseKeys,
  getSelectedPathExpandedKeys,
  getStoredCatalogExpandedKeys,
  getTableCacheGeneration,
  loadTablesWithCache,
  refreshExpandedDatabaseNodes,
} from '../src/views/tables/components/table-explorer-state.ts'

test('selected detail path expands only its catalog without expanding the database', () => {
  const expandedKeys = getSelectedPathExpandedKeys(
    ['catalog:another'],
    'sl',
  )

  assert.deepEqual(expandedKeys, ['catalog:another', 'catalog:sl'])
  assert.equal(expandedKeys.some(key => key.includes('/db:')), false)
})

test('restores catalog expansion only so mounting a detail page cannot load tables', () => {
  const expandedKeys = getStoredCatalogExpandedKeys([
    'catalog:sl',
    'catalog:sl/db:sl_oki_test',
    'catalog:sl/db:sl_oki_test/table:orders',
  ])

  assert.deepEqual(expandedKeys, ['catalog:sl'])
})

test('caches table lists, including empty lists, until explicitly cleared', async () => {
  const cache = createTableCache<{ name: string, type: string }>()
  let requestCount = 0
  const fetchTables = async () => {
    requestCount += 1
    return []
  }

  assert.deepEqual(await loadTablesWithCache(cache, 'sl', 'empty_db', fetchTables), [])
  assert.deepEqual(await loadTablesWithCache(cache, 'sl', 'empty_db', fetchTables), [])
  assert.equal(requestCount, 1)

  clearTableCache(cache)
  assert.deepEqual(await loadTablesWithCache(cache, 'sl', 'empty_db', fetchTables), [])
  assert.equal(requestCount, 2)
})

test('marks an in-flight result stale when a global refresh clears the cache', async () => {
  const cache = createTableCache<{ name: string, type: string }>()
  let resolveRequest: ((tables: { name: string, type: string }[]) => void) | undefined
  const generation = getTableCacheGeneration(cache)
  const pending = loadTablesWithCache(cache, 'sl', 'sl_oki_test', () =>
    new Promise((resolve) => {
      resolveRequest = resolve
    }))

  clearTableCache(cache)
  resolveRequest?.([{ name: 'stale_orders', type: 'PAIMON' }])
  await pending

  assert.notEqual(getTableCacheGeneration(cache), generation)
  assert.equal(getCachedTables(cache, 'sl', 'sl_oki_test'), undefined)
})

test('deduplicates concurrent table loads for the same database', async () => {
  const cache = createTableCache<{ name: string, type: string }>()
  let requestCount = 0
  const fetchTables = async () => {
    requestCount += 1
    await Promise.resolve()
    return [{ name: 'orders', type: 'PAIMON' }]
  }

  const [first, second] = await Promise.all([
    loadTablesWithCache(cache, 'sl', 'sl_oki_test', fetchTables),
    loadTablesWithCache(cache, 'sl', 'sl_oki_test', fetchTables),
  ])

  assert.deepEqual(first, [{ name: 'orders', type: 'PAIMON' }])
  assert.strictEqual(first, second)
  assert.equal(requestCount, 1)
})

test('global refresh targets only expanded database nodes', () => {
  assert.deepEqual(
    getExpandedDatabaseKeys([
      'catalog:sl',
      'catalog:sl/db:sl_oki_test',
      'catalog:other',
      'catalog:other/db:archived/table:events',
    ]),
    ['catalog:sl/db:sl_oki_test'],
  )
})

test('keeps a bare table search global while targeting the current database', () => {
  assert.deepEqual(
    createTableSearchPlan('orders', { catalog: 'sl', database: 'sl_oki_test' }),
    {
      filterKeyword: 'orders',
      target: { catalog: 'sl', database: 'sl_oki_test', keyword: 'orders' },
    },
  )
})

test('keeps two-part input as a catalog.database filter without loading tables', () => {
  assert.deepEqual(
    createTableSearchPlan('sl.sl_oki_test', { catalog: 'other', database: 'archive' }),
    { filterKeyword: 'sl.sl_oki_test' },
  )
})

test('targets an explicitly qualified table without global database fan-out', () => {
  assert.deepEqual(
    createTableSearchPlan('other.analytics.sessions', { catalog: 'sl', database: 'sl_oki_test' }),
    {
      filterKeyword: 'other.analytics.sessions',
      target: { catalog: 'other', database: 'analytics', keyword: 'sessions' },
    },
  )
  assert.deepEqual(
    createTableSearchPlan('orders', { catalog: '', database: '' }),
    { filterKeyword: 'orders' },
  )
})

test('exposes a fully loaded database cache to search without another request', async () => {
  const cache = createTableCache<{ name: string, type: string }>()
  const tables = [{ name: 'orders', type: 'PAIMON' }]

  await loadTablesWithCache(cache, 'sl', 'sl_oki_test', async () => tables)

  assert.strictEqual(getCachedTables(cache, 'sl', 'sl_oki_test'), tables)
  assert.equal(getCachedTables(cache, 'sl', 'missing'), undefined)
})

test('refresh keeps expanded nodes visible and waits for every reload to settle', async () => {
  const nodes = [
    { key: 'catalog:sl/db:orders', value: 'old-orders' },
    { key: 'catalog:sl/db:users', value: 'old-users' },
    { key: 'catalog:sl/db:archive', value: 'old-archive' },
  ]
  let resolveUsers: (() => void) | undefined
  let refreshSettled = false

  const refresh = refreshExpandedDatabaseNodes(
    nodes,
    ['catalog:sl/db:orders', 'catalog:sl/db:users'],
    node => node.value = '',
    (node) => {
      if (node.key.endsWith('orders')) {
        return Promise.reject(new Error('orders unavailable'))
      }
      return new Promise<void>((resolve) => {
        resolveUsers = resolve
      })
    },
  )
  refresh.then(() => refreshSettled = true)

  await Promise.resolve()
  assert.equal(refreshSettled, false)
  assert.equal(nodes[0].value, 'old-orders')
  assert.equal(nodes[1].value, 'old-users')
  assert.equal(nodes[2].value, '')

  resolveUsers?.()
  await refresh
  assert.equal(refreshSettled, true)
})
