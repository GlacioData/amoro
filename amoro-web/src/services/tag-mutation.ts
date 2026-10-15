/* Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See NOTICE. Licensed under the Apache License, Version 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 */
import axios from 'axios'
import type { TableTag, TagDeleteResult, TagScope, TagRetention } from '../types/tag.type'

export class TagMutationError extends Error {
  readonly outcome: string
  constructor(message: string, outcome: string) {
    super(message)
    this.name = 'TagMutationError'
    this.outcome = outcome
  }
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function scope(value: unknown): value is TagScope {
  return record(value) && (value.kind === 'DEFAULT' || value.kind === 'TABLE'
    || (value.kind === 'BRANCH' && typeof value.branch === 'string' && value.branch.length > 0))
  && (value.kind === 'BRANCH' || value.branch === undefined)
}

function matchesScope(value: unknown, expected: TagScope): value is TagScope {
  return scope(value) && value.kind === expected.kind && value.branch === expected.branch
}

function matchesRetention(value: unknown, expected: TagRetention): value is TagRetention {
  return record(value)
    && ['KNOWN', 'UNKNOWN'].includes(String(value.state))
    && typeof value.model === 'string'
    && ['SET', 'UNSET', 'UNKNOWN'].includes(String(value.ageLimit))
    && value.state === expected.state && value.model === expected.model
    && value.ageLimit === expected.ageLimit && value.maxAgeMillis === expected.maxAgeMillis
}

function failureOutcome(response: Record<string, unknown>): string {
  if (typeof response.outcome === 'string') return response.outcome
  if (response.code === 403 || response.code === 401) return 'NOT_APPLIED'
  return 'UNKNOWN'
}

// Axios resolves HTTP 2xx even when the AMS business envelope rejects the operation.
// Source: https://axios-http.com/docs/handling_errors
// A router.push Promise is navigation completion, never a mutation receipt:
// https://router.vuejs.org/guide/advanced/navigation-failures.html#detecting-navigation-failures
// Callers use the existing returnCode option, so login redirects cannot masquerade as success.
export async function tagMutation<T>(send: () => Promise<unknown>, valid: (value: unknown) => value is T): Promise<T> {
  const response = await send() // Exactly one attempt; an unknown outcome must never be retried.
  if (!record(response)) throw new TagMutationError('Invalid Tag operation response', 'UNKNOWN')
  if (response.code !== 0 && response.code !== 200) {
    throw new TagMutationError(typeof response.message === 'string' ? response.message : 'Tag operation failed', failureOutcome(response))
  }
  if (!valid(response.result)) throw new TagMutationError('Invalid Tag operation receipt; refresh before trying again', 'UNKNOWN')
  return response.result
}

export function savedTag(name: string, version: string | undefined, expectedScope: TagScope, expectedRetention: TagRetention): (value: unknown) => value is TableTag {
  return (value): value is TableTag => record(value) && value.name === name && value.type === 'tag'
    && record(value.target) && typeof value.target.id === 'string' && value.target.id.length > 0
    && (version === undefined || value.target.id === version)
    && matchesScope(value.scope, expectedScope) && value.target.branch === expectedScope.branch
    && matchesRetention(value.retention, expectedRetention)
}

export function deletedTag(name: string, expectedScope: TagScope): (value: unknown) => value is TagDeleteResult {
  return (value): value is TagDeleteResult => record(value) && value.name === name
    && value.referenceState === 'ABSENT' && matchesScope(value.scope, expectedScope)
    && ['NOT_REQUIRED', 'BEST_EFFORT_UNVERIFIED', 'VERIFIED', 'UNKNOWN'].includes(String(value.historyCleanup))
}

// Only an explicit rejection proves that no mutation was applied. Lost/partial responses do not.
export function tagMutationFailureMessage(failure: unknown, unknownMessage: string): string {
  if (failure instanceof TagMutationError) return failure.outcome === 'NOT_APPLIED' ? failure.message : unknownMessage
  if (axios.isAxiosError(failure)) {
    const body: unknown = failure.response?.data
    if (record(body) && body.outcome === 'NOT_APPLIED' && typeof body.message === 'string') return body.message
  }
  return unknownMessage
}

export function updatedTag(row: TableTag, expected: TagRetention): (value: unknown) => value is TableTag {
  const valid = savedTag(row.name, row.target.id, row.scope, expected)
  return (value): value is TableTag => valid(value)
    && value.target.branch === row.target.branch
    && typeof value.observationToken === 'string' && value.observationToken.length > 0
    && value.retention.state === 'KNOWN'
}
