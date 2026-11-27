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
  buildGroupSubmitPayload,
  clampPage,
  isValidParallelism,
  paginateItems,
  runExclusiveSubmit,
} from '../src/views/resource/resource-state.ts'

test('paginateItems slices client-side resource groups by current page and pageSize', () => {
  const items = Array.from({ length: 12 }, (_, idx) => `g-${idx + 1}`)
  assert.deepEqual(paginateItems(items, 1, 5), ['g-1', 'g-2', 'g-3', 'g-4', 'g-5'])
  assert.deepEqual(paginateItems(items, 2, 5), ['g-6', 'g-7', 'g-8', 'g-9', 'g-10'])
  assert.deepEqual(paginateItems(items, 3, 5), ['g-11', 'g-12'])
})

test('clampPage falls back to the last valid page when total shrinks or becomes empty', () => {
  assert.equal(clampPage(2, 5, 5), 1)
  assert.equal(clampPage(3, 0, 10), 1)
  assert.equal(clampPage(0, 20, 10), 1)
  assert.equal(clampPage(2, 20, 10), 2)
})

test('isValidParallelism allows only positive integers', () => {
  assert.equal(isValidParallelism(1), true)
  assert.equal(isValidParallelism(4), true)
  assert.equal(isValidParallelism(64), true)

  assert.equal(isValidParallelism(0), false)
  assert.equal(isValidParallelism(-1), false)
  assert.equal(isValidParallelism(1.5), false)
  assert.equal(isValidParallelism(Number.NaN), false)
  assert.equal(isValidParallelism(Number.POSITIVE_INFINITY), false)
  assert.equal(isValidParallelism(''), false)
  assert.equal(isValidParallelism(null), false)
  assert.equal(isValidParallelism(undefined), false)
})

test('buildGroupSubmitPayload aborts submission when properties validation returns false', () => {
  assert.equal(buildGroupSubmitPayload({ name: 'g1', container: 'local', properties: false }), null)
  assert.deepEqual(
    buildGroupSubmitPayload({ name: 'g1', container: 'local', properties: {} }),
    { name: 'g1', container: 'local', properties: {} },
  )
  assert.deepEqual(
    buildGroupSubmitPayload({ name: 'g1', container: 'local', properties: { memory: '1024' } }),
    { name: 'g1', container: 'local', properties: { memory: '1024' } },
  )
})

test('runExclusiveSubmit blocks overlapping async submissions even before the first await resolves and always resets busy state', async () => {
  let busy = false
  let invocations = 0
  let releaseFirst!: () => void
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve
  })

  const first = runExclusiveSubmit(
    () => busy,
    (next) => { busy = next },
    async () => {
      invocations += 1
      await firstGate
    },
  )

  // Busy must be set synchronously before the first microtask yields.
  assert.equal(busy, true)

  const duplicate = await runExclusiveSubmit(
    () => busy,
    (next) => { busy = next },
    async () => {
      invocations += 1
    },
  )
  assert.equal(duplicate, false)
  assert.equal(invocations, 1)

  releaseFirst()
  assert.equal(await first, true)
  assert.equal(busy, false)

  // Rejection still resets busy so the user can retry.
  await assert.rejects(
    runExclusiveSubmit(
      () => busy,
      (next) => { busy = next },
      async () => {
        throw new Error('submit failed')
      },
    ),
    /submit failed/,
  )
  assert.equal(busy, false)
})
