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
  createBranchLoader,
  createTableSearchPlan,
  getRevealScrollKey,
  getSelectedPathExpandedKeys,
  getStoredCatalogExpandedKeys,
  mergeExpandedKeys,
  reconcileTreeNodes,
} from '../src/views/tables/components/table-explorer-state.ts'

test('selected detail path expands only its catalog without expanding the database', () => {
  const expandedKeys = getSelectedPathExpandedKeys(['catalog:another'], 'sl')
  assert.deepEqual(expandedKeys, ['catalog:another', 'catalog:sl'])
  assert.equal(expandedKeys.some(key => key.includes('/db:')), false)

  // Already-expanded catalog is preserved without duplication; blank catalog is a no-op.
  assert.deepEqual(getSelectedPathExpandedKeys(expandedKeys, 'sl'), expandedKeys)
  assert.deepEqual(getSelectedPathExpandedKeys(expandedKeys, '   '), expandedKeys)
})

test('getRevealScrollKey targets database row when collapsed and table row only when database is already expanded', () => {
  assert.equal(
    getRevealScrollKey(['catalog:sl'], 'sl', 'sl_oki_test', 'orders'),
    'catalog:sl/db:sl_oki_test',
  )
  assert.equal(
    getRevealScrollKey(['catalog:sl', 'catalog:sl/db:sl_oki_test'], 'sl', 'sl_oki_test', 'orders'),
    'catalog:sl/db:sl_oki_test/table:orders',
  )
  assert.equal(getRevealScrollKey(['catalog:sl'], 'sl', '', 'orders'), null)
})

test('restores catalog expansion and leaves unrelated databases unloaded', () => {
  const expandedKeys = getStoredCatalogExpandedKeys([
    'catalog:sl',
    'catalog:sl/db:sl_oki_test',
    'catalog:sl/db:sl_oki_test/table:orders',
  ])

  assert.deepEqual(expandedKeys, ['catalog:sl'])
})

test('shares pending requests for the same branch and refresh revision', async () => {
  const load = createBranchLoader<string[]>()
  let requests = 0
  const fetchChildren = async () => {
    requests += 1
    return ['orders']
  }

  const first = load('catalog:sl', 0, fetchChildren)
  const second = load('catalog:sl', 0, fetchChildren)
  assert.strictEqual(first, second)
  assert.deepEqual(await second, ['orders'])
  assert.equal(requests, 1)
})

test('allows a new revision to supersede a request and keeps its promise registered', async () => {
  const load = createBranchLoader<string>()
  let finishOld!: (value: string) => void
  let finishNew!: (value: string) => void
  const old = load('db', 0, () => new Promise(resolve => finishOld = resolve))
  const next = load('db', 1, () => new Promise(resolve => finishNew = resolve))
  await Promise.resolve()
  finishOld('old')
  await old
  assert.strictEqual(load('db', 1, async () => 'duplicate'), next)
  finishNew('fresh')
  assert.equal(await next, 'fresh')
})

test('allows a failed branch request to be retried', async () => {
  const load = createBranchLoader<string>()
  await assert.rejects(load('db', 0, async () => {
    throw new Error('unavailable')
  }))
  assert.equal(await load('db', 0, async () => 'fresh'), 'fresh')
})

test('keeps existing nodes and children when refresh returns equivalent data', () => {
  const orders = { key: 'orders', tableType: 'PAIMON', children: ['unchanged'] }
  const nodes = [orders]
  assert.strictEqual(reconcileTreeNodes(nodes, [{ key: 'orders', tableType: 'PAIMON' }]), nodes)
  const added = { key: 'new', tableType: 'ICEBERG' }
  const updated = reconcileTreeNodes(nodes, [added, { key: 'orders', tableType: 'PAIMON' }])
  assert.strictEqual(updated[0], added)
  assert.strictEqual(updated[1], orders)
  const changed = { key: 'orders', tableType: 'ICEBERG' }
  assert.strictEqual(reconcileTreeNodes(updated, [changed])[0], changed)
})

test('applies explicit search toggles without changing the defaults', () => {
  const autoExpanded = ['catalog:sl', 'db:open']
  const toggles = new Map([['db:open', false], ['db:closed', true]])
  assert.deepEqual(mergeExpandedKeys(autoExpanded, toggles), ['catalog:sl', 'db:closed'])
  assert.deepEqual(autoExpanded, ['catalog:sl', 'db:open'])
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
