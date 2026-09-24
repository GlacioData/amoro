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
// Match the existing dependency-free frontend regression suite.
// eslint-disable-next-line test/no-import-node-test
import test from 'node:test'
import { normalizeTableModel, tableModelDescription } from '../src/utils/tableModel.ts'

test('new models retain native and future codes without a shared enum', () => {
  for (const code of ['HASH_FIXED', 'HASH_DYNAMIC', 'KEY_DYNAMIC', 'BUCKET_UNAWARE', 'POSTPONE_MODE', 'FUTURE']) {
    const model = { kind: 'BUCKET_MODE', code }
    assert.deepEqual(normalizeTableModel(model, 'PAIMON', 'OLD'), model)
  }
  for (const code of ['V1', 'V2', 'V3', 'V4']) {
    const model = { kind: 'FORMAT_VERSION', code }
    assert.deepEqual(normalizeTableModel(model, 'ICEBERG'), model)
  }
})

test('legacy fallback is Paimon-only and never overrides explicit null or invalid new fields', () => {
  assert.deepEqual(normalizeTableModel(undefined, 'PAIMON', 'HASH_FIXED'), { kind: 'BUCKET_MODE', code: 'HASH_FIXED' })
  for (const model of [null, {}, '', [], { kind: '', code: 'V2' }, { kind: 'FORMAT_VERSION', code: 2 }, { kind: ' ', code: 'V2' }])
    assert.equal(normalizeTableModel(model, 'PAIMON', 'HASH_FIXED'), null)
  for (const format of ['ICEBERG', 'MIXED_ICEBERG', 'MIXED_HIVE', 'HUDI', 'FUTURE'])
    assert.equal(normalizeTableModel(undefined, format, 'V2'), null)
  assert.equal(normalizeTableModel(undefined, 'PAIMON', ' '), null)
})

test('tooltip semantics require the exact format and model kind', () => {
  for (const code of ['V1', 'V2', 'V3'])
    assert.equal(tableModelDescription('ICEBERG', { kind: 'FORMAT_VERSION', code }), `icebergModel${code}`)
  const keys = ['HASH_FIXED', 'HASH_DYNAMIC', 'KEY_DYNAMIC', 'BUCKET_UNAWARE', 'POSTPONE_MODE']
    .map(code => tableModelDescription('PAIMON', { kind: 'BUCKET_MODE', code }))
  assert.equal(new Set(keys).size, 5)
  assert.equal(keys.includes('tableModelUnknown'), false)
  for (const format of ['PAIMON', 'MIXED_ICEBERG', 'MIXED_HIVE', 'FUTURE'])
    assert.equal(tableModelDescription(format, { kind: 'FORMAT_VERSION', code: 'V2' }), 'tableModelUnknown')
  assert.equal(tableModelDescription('ICEBERG', { kind: 'OTHER', code: 'V2' }), 'tableModelUnknown')
  assert.equal(tableModelDescription('ICEBERG', { kind: 'FORMAT_VERSION', code: 'V4' }), 'tableModelUnknown')
  assert.equal(tableModelDescription('PAIMON', null), 'tableModelUnavailable')
})
