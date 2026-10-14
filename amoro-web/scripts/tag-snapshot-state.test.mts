/* Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See NOTICE. Licensed under the Apache License, Version 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { snapshotPicker } from '../src/views/tables/components/tag-snapshot-state.ts'

const row = (snapshotId: string) => ({ snapshotId, commitTime: '1791884637973' })

test('snapshot choices preserve opaque and large IDs without duplicate options', async () => {
  const id = 'bda791ee-daa7-4f9e-90c4-591fa91359e1'
  const picker = snapshotPicker(async () => ({ list: [row(id), row('9007199254740993'), row(id)], total: 3 }))
  await picker.load()
  assert.deepEqual(picker.state.rows.map(value => value.snapshotId), [id, '9007199254740993'])
  assert.equal(picker.state.rows[0].commitTime, '1791884637973')
  assert.equal(picker.state.hasMore, false)
})

test('snapshot pagination keeps existing choices after failure and retries the same page', async () => {
  const calls: number[] = []
  const first = Array.from({ length: 25 }, (_, index) => row(String(index)))
  const failure = new Error('read failed')
  const picker = snapshotPicker(async (page, size) => {
    calls.push(page)
    assert.equal(size, 25)
    if (calls.length === 2) throw failure
    return page === 1 ? { list: first, total: 26 } : { list: [row('24'), row('25')], total: 26 }
  })
  await picker.load()
  assert.equal(picker.state.hasMore, true)
  await picker.load()
  assert.equal(picker.state.page, 1)
  assert.equal(picker.state.rows.length, 25)
  assert.equal(picker.state.error, failure)
  await picker.load()
  assert.deepEqual(calls, [1, 2, 2])
  assert.equal(picker.state.rows.length, 26)
  assert.equal(picker.state.hasMore, false)
  assert.equal(picker.state.error, null)
  await picker.load()
  assert.deepEqual(calls, [1, 2, 2])
})

test('rapid repeated loads send only one snapshot request', async () => {
  let calls = 0
  let finish!: (result: { list: ReturnType<typeof row>[], total: number }) => void
  const picker = snapshotPicker(() => {
    calls++
    return new Promise(resolve => { finish = resolve })
  })
  const pending = picker.load()
  await picker.load()
  assert.equal(calls, 1)
  assert.equal(picker.state.loading, true)
  finish({ list: [row('selected')], total: 1 })
  await pending
  assert.equal(picker.state.loading, false)
})

test('short or empty candidate pages still allow access to later retained snapshots', async () => {
  for (const first of [[], [row('first')]]) {
    const calls: number[] = []
    const picker = snapshotPicker(async (page) => {
      calls.push(page)
      return { list: page === 1 ? first : [row('later-retained')], total: 50 }
    })
    await picker.load()
    assert.equal(picker.state.hasMore, true)
    await picker.load()
    assert.deepEqual(calls, [1, 2])
    assert.ok(picker.state.rows.some(value => value.snapshotId === 'later-retained'))
    assert.equal(picker.state.hasMore, false)
  }
})

test('closing or changing scope rejects old responses and their finally handlers', async () => {
  const pending: ((result: { list: ReturnType<typeof row>[], total: number }) => void)[] = []
  const picker = snapshotPicker(() => new Promise(resolve => pending.push(resolve)))
  const old = picker.load()
  picker.reset()
  const current = picker.load()
  pending[0]({ list: [row('old-scope')], total: 1 })
  await old
  assert.equal(picker.state.loading, true)
  assert.deepEqual(picker.state.rows, [])
  pending[1]({ list: [row('current-scope')], total: 1 })
  await current
  assert.deepEqual(picker.state.rows.map(value => value.snapshotId), ['current-scope'])
  picker.reset()
  assert.deepEqual(picker.state.rows, [])
  assert.equal(picker.state.page, 0)
})

test('empty snapshot results terminate pagination and malformed numeric IDs cannot be selected', async () => {
  const empty = snapshotPicker(async () => ({ list: [], total: 0 }))
  await empty.load()
  assert.deepEqual(empty.state.rows, [])
  assert.equal(empty.state.hasMore, false)
  const invalid = snapshotPicker(async () => ({ list: [row(123 as unknown as string)], total: 1 }))
  await invalid.load()
  assert.deepEqual(invalid.state.rows, [])
  assert.ok(invalid.state.error)
  assert.equal(invalid.state.page, 0)
})
