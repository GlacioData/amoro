/* Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See NOTICE. Licensed under the Apache License, Version 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 */
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import { canDeleteTag, tagErrorMessages, validTagForm } from '../src/views/tables/components/tag-management-state.ts'

test('deletion requires an exact nonempty name and no in-flight mutation', () => {
  assert.equal(canDeleteTag('中文 release%', '中文 release%', false), true)
  for (const typed of ['', '中文 release', '中文 release% ']) assert.equal(canDeleteTag('中文 release%', typed, false), false)
  assert.equal(canDeleteTag('release', 'release', true), false)
  assert.equal(canDeleteTag('', '', false), false)
})
test('form supports opaque version IDs and rejects invalid age conversions', () => {
  assert.equal(validTagForm('release', 'bda791ee-daa7-4f9e-90c4-591fa91359e1', 'NO_AGE_LIMIT', null), true)
  for (const hours of [null, 0, -1, Infinity, Number.MAX_SAFE_INTEGER, 0.0000001]) assert.equal(validTagForm('release', '3', 'AGE_LIMIT', hours), false)
  assert.equal(validTagForm('release', '3', 'AGE_LIMIT', 0.001), true)
  for (const name of ['', '  ', 'a\u0000b', 'x'.repeat(256)]) assert.equal(validTagForm(name, '3', 'NO_AGE_LIMIT', null), false)
  assert.equal(validTagForm('release', '', 'NO_AGE_LIMIT', null), false)
})


// Exercise the same receipt gate used by create/copy/delete, including HTTP 200 / code 403.
import { TagMutationError, deletedTag, savedTag, tagMutation, tagMutationFailureMessage } from '../src/services/tag-mutation.ts'
const receipt = { name: 'release', type: 'tag', target: { id: 'bda791ee-daa7-4f9e-90c4-591fa91359e1' }, scope: { kind: 'TABLE' }, retention: { state: 'UNKNOWN', model: 'UNKNOWN', ageLimit: 'UNKNOWN' } }
test('valid Tag receipts preserve opaque versions and accept both success codes', async () => {
  for (const code of [0, 200]) {
    assert.deepEqual(await tagMutation(async () => ({ code, result: receipt }), savedTag('release', receipt.target.id, receipt.scope as TagScope, receipt.retention)), receipt)
  }
  const result = { name: 'release', scope: { kind: 'TABLE' }, referenceState: 'ABSENT', historyCleanup: 'UNKNOWN' }
  assert.deepEqual(await tagMutation(async () => ({ code: 200, result }), deletedTag('release', receipt.scope as TagScope)), result)
})
test('expired login, missing receipts and mismatches cannot run success effects or retry', async () => {
  const responses = [
    undefined, { code: 403, message: 'Please login first' },
    { code: 200 }, { code: 200, result: {} },
    { code: 200, result: { ...receipt, name: 'wrong' } },
    { code: 200, result: { ...receipt, target: { id: 3 } } },
    { code: 200, result: { ...receipt, target: { id: 'wrong-version' } } },
  ]
  for (const response of responses) {
    let calls = 0; let saved = false
    await assert.rejects(async () => {
      await tagMutation(async () => { calls++; return response }, savedTag('release', receipt.target.id, receipt.scope as TagScope, receipt.retention))
      saved = true
    }, TagMutationError)
    assert.equal(calls, 1); assert.equal(saved, false)
  }
})
test('Tag error envelopes keep uncertainty and transport failures are never retried', async () => {
  for (const outcome of ['UNKNOWN', 'APPLIED_WITH_ERRORS', 'NOT_APPLIED']) {
    await assert.rejects(tagMutation(async () => ({ code: 500, message: 'callback failed', outcome }), savedTag('release', undefined, receipt.scope as TagScope, receipt.retention)), error => error instanceof TagMutationError && error.outcome === outcome)
  }
  const transport = new Error('connection lost')
  let calls = 0
  await assert.rejects(tagMutation(async () => { calls++; throw transport }, savedTag('release', undefined, receipt.scope as TagScope, receipt.retention)), error => error === transport)
  assert.equal(calls, 1)
  await assert.rejects(tagMutation(async () => ({ code: 200, result: { name: 'release', referenceState: 'PRESENT' } }), deletedTag('release', receipt.scope as TagScope)), TagMutationError)
})

test('mutation failures without a NOT_APPLIED receipt always explain uncertainty', () => {
  const unknown = 'Refresh to verify the Tag before retrying'
  for (const failure of [new Error('Network error'), { isAxiosError: true, response: { data: { message: 'gateway failure' } } }, { isAxiosError: true }, new TagMutationError('callback failed', 'APPLIED_WITH_ERRORS')]) {
    assert.equal(tagMutationFailureMessage(failure, unknown), unknown)
  }
  assert.equal(tagMutationFailureMessage(new TagMutationError('Please login first', 'NOT_APPLIED'), unknown), 'Please login first')
  assert.equal(tagMutationFailureMessage({ isAxiosError: true, response: { data: { outcome: 'NOT_APPLIED', message: 'name exists' } } }, unknown), 'name exists')
})

test('real local Paimon HTTP create receipt satisfies the same frontend gate', async () => {
  // Captured from the isolated AMS after create/copy/delete and permission verification.
  const response = JSON.parse(readFileSync(new URL('./fixtures/tag-created.json', import.meta.url), 'utf8'))
  const row = response.result
  assert.equal(row.type, 'tag')
  assert.deepEqual(await tagMutation(async () => response, savedTag(row.name, row.target.id, row.scope as TagScope, row.retention)), row)
})

test('unknown mutation warning survives failed verification reads and later refreshes', async () => {
  const errors = { mutation: '', read: '' }
  const unknown = 'The operation may have applied; verify the Tag before retrying'
  for (const outcome of ['UNKNOWN', 'APPLIED_WITH_ERRORS']) {
    try {
      await tagMutation(async () => ({ code: 500, outcome }), savedTag('release', undefined, receipt.scope as TagScope, receipt.retention))
    }
    catch (failure) { errors.mutation = tagMutationFailureMessage(failure, unknown) }
    try { await Promise.reject(new Error('Unable to refresh Tags')) }
    catch (failure) { errors.read = (failure as Error).message }
    assert.deepEqual(tagErrorMessages(errors), [
      { kind: 'mutation', message: unknown }, { kind: 'read', message: 'Unable to refresh Tags' },
    ])
    errors.read = ''
    assert.deepEqual(tagErrorMessages(errors), [{ kind: 'mutation', message: unknown }])
  }
})

import { updatedTag } from '../src/services/tag-mutation.ts'
import type { TableTag, TagRetention } from '../src/types/tag.type.ts'
test('retention receipt must match original identity, scope and resolved policy', () => {
  const policy: TagRetention = { state: 'KNOWN', model: 'TAG_CREATION_AGE', ageLimit: 'SET', maxAgeMillis: 3600000 }
  const row: TableTag = { ...receipt, scope: { kind: 'TABLE' }, observationToken: 'old' }
  const result = { ...row, retention: policy, observationToken: 'current' }
  const valid = updatedTag(row, policy)
  assert.equal(valid(result), true)
  for (const wrong of [
    { ...result, scope: { kind: 'BRANCH', branch: 'wrong' } },
    { ...result, retention: { ...policy, maxAgeMillis: 7200000 } },
    { ...result, retention: { ...policy, model: 'TARGET_COMMIT_AGE' } },
    { ...result, retention: { ...policy, state: 'UNKNOWN' } },
    { ...result, retention: { ...policy, ageLimit: 'UNSET' } },
    { ...result, observationToken: undefined },
  ]) assert.equal(valid(wrong), false)
})

import { canUpdateTagRetention, resolvedTagRetention, sameRetentionSource } from '../src/views/tables/components/tag-management-state.ts'
import type { TagCapabilities } from '../src/types/tag.type.ts'
const updateCapabilities: TagCapabilities = { actions: ['UPDATE_RETENTION'], supportedScopes: [{ kind: 'TABLE' }], retention: { model: 'TAG_CREATION_AGE', updateClock: 'RESET_ON_UPDATE', supportedModes: ['AGE_LIMIT', 'NATIVE_DEFAULT', 'NO_AGE_LIMIT'], nativeDefault: { state: 'KNOWN', model: 'TAG_CREATION_AGE', ageLimit: 'SET', maxAgeMillis: 7200000 }, configObservationToken: 'defaults' }, deleteEffect: 'UNKNOWN', preconditionGuarantee: 'OBSERVATION_CHECK' }
test('retention choices require explicit exact safe milliseconds and a resolved default', () => {
  for (const value of [null, 0, -1, Infinity, Number.MAX_SAFE_INTEGER, 0.0000001]) assert.equal(resolvedTagRetention('AGE_LIMIT', value, updateCapabilities), null)
  assert.equal(resolvedTagRetention('', null, updateCapabilities), null)
  assert.equal(resolvedTagRetention('AGE_LIMIT', 0.001, updateCapabilities)?.maxAgeMillis, 3600)
  assert.equal(resolvedTagRetention('NATIVE_DEFAULT', null, updateCapabilities)?.maxAgeMillis, 7200000)
  assert.equal(resolvedTagRetention('NO_AGE_LIMIT', null, updateCapabilities)?.ageLimit, 'UNSET')
  assert.equal(resolvedTagRetention('NATIVE_DEFAULT', null, { ...updateCapabilities, retention: { ...updateCapabilities.retention, configObservationToken: undefined } }), null)
})
test('update requires explicit capability, known clock and current source marker', () => {
  const row: TableTag = { ...receipt, scope: { kind: 'TABLE' }, observationToken: 'tag' }
  assert.equal(canUpdateTagRetention(row, updateCapabilities, false), true)
  assert.equal(canUpdateTagRetention(row, updateCapabilities, true), false)
  assert.equal(canUpdateTagRetention(row, { ...updateCapabilities, supportedScopes: [{ kind: 'BRANCH', branch: 'main' }] }, false), false)
  assert.equal(canUpdateTagRetention(row, { ...updateCapabilities, retention: { ...updateCapabilities.retention, supportedModes: [] } }, false), false)
  assert.equal(canUpdateTagRetention(row, { ...updateCapabilities, actions: [] }, false), false)
  assert.equal(canUpdateTagRetention(row, { ...updateCapabilities, retention: { ...updateCapabilities.retention, updateClock: 'UNKNOWN' } }, false), false)
  assert.equal(sameRetentionSource(row, { ...row, observationToken: 'changed' }), false)
  assert.equal(sameRetentionSource(row, undefined), false)
  assert.equal(sameRetentionSource(row, { ...row, target: { id: 'changed' } }), false)
  assert.equal(sameRetentionSource(row, row), true)
})

import type { TagScope } from '../src/types/tag.type.ts'
for (const operation of ['create', 'copy']) {
  const name = operation === 'create' ? 'release' : 'release-copy'
  const expectedScope: TagScope = { kind: 'BRANCH', branch: 'main' }
  const expectedRetention: TagRetention = { state: 'KNOWN', model: 'TAG_CREATION_AGE', ageLimit: 'SET', maxAgeMillis: 3600000 }
  const result = { ...receipt, name, scope: expectedScope, target: { ...receipt.target, branch: 'main' }, retention: expectedRetention }

  test(`${operation} receipts accept the requested scope, target branch and resolved retention`, async () => {
    for (const scope of [{ kind: 'TABLE' }, expectedScope] as TagScope[]) {
      for (const retention of [expectedRetention, { state: 'KNOWN', model: 'TAG_CREATION_AGE', ageLimit: 'UNSET' }]) {
        const row = { ...result, scope, target: { ...receipt.target, branch: scope.branch }, retention }
        for (const code of [0, 200]) {
          assert.deepEqual(await tagMutation(async () => ({ code, result: row }), savedTag(name, row.target.id, scope, retention)), row)
        }
      }
    }
  })

  const mismatches = [
    ['a different scope kind', { ...result, scope: { kind: 'TABLE' }, target: receipt.target }],
    ['a different scope branch', { ...result, scope: { kind: 'BRANCH', branch: 'other' }, target: { ...receipt.target, branch: 'other' } }],
    ['a target branch outside the requested scope', { ...result, target: { ...receipt.target, branch: 'other' } }],
    ['a missing target branch for the requested branch scope', { ...result, target: receipt.target }],
    ['a different retention model', { ...result, retention: { ...expectedRetention, model: 'TARGET_COMMIT_AGE' } }],
    ['a different retention duration', { ...result, retention: { ...expectedRetention, maxAgeMillis: 7200000 } }],
    ['a different retention age limit', { ...result, retention: { state: 'KNOWN', model: 'TAG_CREATION_AGE', ageLimit: 'UNSET' } }],
    ['unknown retention when a known policy was requested', { ...result, retention: { state: 'UNKNOWN', model: 'UNKNOWN', ageLimit: 'UNKNOWN' } }],
  ] as const
  for (const [mismatch, row] of mismatches) {
    test(`${operation} receipts reject ${mismatch} without success effects or retry`, async () => {
      let calls = 0; let saved = false
      await assert.rejects(async () => {
        await tagMutation(async () => { calls++; return { code: 200, result: row } }, savedTag(name, receipt.target.id, expectedScope, expectedRetention))
        saved = true
      }, error => error instanceof TagMutationError && error.outcome === 'UNKNOWN')
      assert.equal(calls, 1); assert.equal(saved, false)
    })
  }

  test(`${operation} receipts reject a target branch for the requested table scope`, async () => {
    const scope: TagScope = { kind: 'TABLE' }
    const row = { ...result, scope }
    await assert.rejects(tagMutation(async () => ({ code: 200, result: row }), savedTag(name, receipt.target.id, scope, expectedRetention)), error => error instanceof TagMutationError && error.outcome === 'UNKNOWN')
  })
}

test('delete receipts accept the requested table or branch scope', async () => {
  for (const scope of [{ kind: 'TABLE' }, { kind: 'BRANCH', branch: 'main' }] as TagScope[]) {
    const result = { name: 'release', scope, referenceState: 'ABSENT', historyCleanup: 'UNKNOWN' }
    for (const code of [0, 200]) {
      assert.deepEqual(await tagMutation(async () => ({ code, result }), deletedTag('release', scope)), result)
    }
  }
})
for (const [mismatch, scope] of [
  ['a different scope kind', { kind: 'TABLE' }],
  ['a different scope branch', { kind: 'BRANCH', branch: 'other' }],
] as const) {
  test(`delete receipts reject ${mismatch} without success effects or retry`, async () => {
    const expectedScope: TagScope = { kind: 'BRANCH', branch: 'main' }
    const result = { name: 'release', scope, referenceState: 'ABSENT', historyCleanup: 'UNKNOWN' }
    let calls = 0; let deleted = false
    await assert.rejects(async () => {
      await tagMutation(async () => { calls++; return { code: 200, result } }, deletedTag('release', expectedScope))
      deleted = true
    }, error => error instanceof TagMutationError && error.outcome === 'UNKNOWN')
    assert.equal(calls, 1); assert.equal(deleted, false)
  })
}
