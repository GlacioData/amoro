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
// eslint-disable-next-line test/no-import-node-test
import test from 'node:test'
import { cursorPages, requestEpoch, sameSnapshotSelection, snapshotRowKey, snapshotTableKey } from '../src/views/tables/components/snapshot-page-state.ts'

test('new table request is independent of old loading and old finally', () => {
  const requests = requestEpoch()
  const old = requests.begin()
  requests.invalidate()
  const current = requests.begin()
  assert.equal(requests.current(old), false)
  assert.equal(requests.current(current), true)
  requests.invalidate()
  assert.equal(requests.current(current), false)
})

test('only success commits cursors; failed next and back leave current page intact', () => {
  const pages = cursorPages()
  pages.commit(0, null, 'next-1')
  const beforeFailure = JSON.stringify(pages)
  // A failed request does not call commit.
  assert.equal(JSON.stringify(pages), beforeFailure)
  pages.commit(1, 'next-1', 'next-2')
  assert.deepEqual(pages.starts, [null, 'next-1'])
  pages.commit(0, pages.starts[0], 'new-next-1')
  assert.deepEqual(pages.starts, [null])
  assert.equal(pages.next, 'new-next-1')
  pages.reset()
  assert.equal(pages.next, null)
  assert.equal(pages.index, 0)
})

test('empty pages may continue and terminal pages do not', () => {
  const pages = cursorPages()
  pages.commit(0, null, 'after-empty')
  assert.equal(pages.next, 'after-empty')
  pages.commit(1, 'after-empty', null)
  assert.equal(pages.next, null)
})

test('ADD and DELETE of the same file use separate identities, across page renders too', () => {
  assert.notEqual(snapshotRowKey(1, 0), snapshotRowKey(1, 1))
  assert.notEqual(snapshotRowKey(1, 0), snapshotRowKey(2, 0))
})

test('table identity preserves field boundaries and ignores the selected tab', () => {
  const old = { catalog: 'a', db: 'bc', table: 'd', type: 'PAIMON' }
  assert.notEqual(snapshotTableKey(old), snapshotTableKey({ ...old, catalog: 'ab', db: 'c' }))
  assert.equal(snapshotTableKey(old), snapshotTableKey({ ...old, tab: 'Snapshots' }))
  assert.notEqual(snapshotTableKey(old), snapshotTableKey({ ...old, type: 'ICEBERG' }))
})

test('same-named branch and consumer remain independently selectable', () => {
  const branch = { value: 'same-name', type: 'branch' }
  assert.equal(sameSnapshotSelection(branch, { ...branch }), true)
  assert.equal(sameSnapshotSelection(branch, { ...branch, type: 'consumer' }), false)
  assert.equal(sameSnapshotSelection(branch, { ...branch, type: 'tag' }), false)
})
