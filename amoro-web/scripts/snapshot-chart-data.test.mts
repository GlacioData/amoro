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
import { snapshotChartData } from '../src/views/tables/components/snapshot-chart-data.ts'

test('Paimon and Iceberg string counters preserve zero and negative values', () => {
  assert.deepEqual(snapshotChartData({
    1: { 'total-records': '53248', 'delta-records': '-1632' },
    2: { 'total-records': '0', 'total-data-files': '32' },
  }), {
    1: { 'total-records': 53248, 'delta-records': -1632, 'total-data-files': null },
    2: { 'total-records': 0, 'delta-records': null, 'total-data-files': 32 },
  })
})

test('unknown and unsafe counters are gaps, never zero or rounded counts', () => {
  const values = ['—', '', ' ', 'NaN', 'Infinity', '1x', '1.2', '9007199254740993', null]
  for (const value of values)
    assert.deepEqual(snapshotChartData({ 1: { count: value } }), { 1: { count: null } })
  assert.deepEqual(snapshotChartData({ 1: { count: '9007199254740991' } }), { 1: { count: 9007199254740991 } })
})

test('missing chart points keep time alignment for every series', () => {
  assert.deepEqual(snapshotChartData({ 1: { a: '1' }, 2: {}, 3: { b: '2' } }), {
    1: { a: 1, b: null },
    2: { a: null, b: null },
    3: { a: null, b: 2 },
  })
})

test('empty charts stay empty and source values are unchanged', () => {
  const source = { 1: { count: '42' } }
  snapshotChartData(source)
  assert.deepEqual(source, { 1: { count: '42' } })
  assert.deepEqual(snapshotChartData({}), {})
})

test('delta file trends preserve ADD, DELETE and negative net counts without inventing totals', () => {
  assert.deepEqual(snapshotChartData({
    1: { addedFiles: '32', deletedFiles: '0', netFileChange: '32' },
    2: { addedFiles: '32', deletedFiles: '1664', netFileChange: '-1632' },
    3: {},
  }), {
    1: { addedFiles: 32, deletedFiles: 0, netFileChange: 32 },
    2: { addedFiles: 32, deletedFiles: 1664, netFileChange: -1632 },
    3: { addedFiles: null, deletedFiles: null, netFileChange: null },
  })
})
