/* Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See NOTICE. Licensed under the Apache License, Version 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 */
export function validTagForm(name: string, version: string, mode: string, hours: number | null) {
  return name.length > 0 && name.length <= 255 && name.trim().length > 0 && !/[\u0000-\u001F\u007F]/u.test(name)
    && version.trim().length > 0 && (mode !== 'AGE_LIMIT' || (hours != null && Number.isSafeInteger(hours * 3600000) && hours > 0 && hours * 3600000 <= Number.MAX_SAFE_INTEGER))
}
export function canDeleteTag(actual: string, typed: string, pending: boolean) {
  return !pending && actual.length > 0 && actual === typed
}

// Keep a failed verification read separate from the preceding mutation outcome.
export function tagErrorMessages(errors: { mutation: string, read: string }) {
  return [{ kind: 'mutation', message: errors.mutation }, { kind: 'read', message: errors.read }]
    .filter(entry => entry.message.length > 0)
}

import type { TableTag, TagCapabilities, TagRetention } from '../../../types/tag.type'

export function canUpdateTagRetention(row: TableTag, capabilities: TagCapabilities | null, pending: boolean) {
  return !pending && !!row.observationToken && !!capabilities?.actions.includes('UPDATE_RETENTION')
    && ['RESET_ON_UPDATE', 'PRESERVE_ANCHOR'].includes(capabilities.retention.updateClock || '')
    && capabilities.retention.model !== 'UNKNOWN'
    && capabilities.supportedScopes.some(scope => scope.kind === row.scope.kind && scope.branch === row.scope.branch)
    && capabilities.retention.supportedModes.some(mode => ['AGE_LIMIT', 'NATIVE_DEFAULT', 'NO_AGE_LIMIT'].includes(mode))
}

export function resolvedTagRetention(mode: string, hours: number | null, capabilities: TagCapabilities | null): TagRetention | null {
  if (!capabilities?.retention.supportedModes.includes(mode)) return null
  const model = capabilities.retention.model
  if (mode === 'NO_AGE_LIMIT') return { state: 'KNOWN', model, ageLimit: 'UNSET' }
  if (mode === 'NATIVE_DEFAULT') {
    const value = capabilities.retention.nativeDefault
    if (value?.state !== 'KNOWN' || value.model !== model || !capabilities.retention.configObservationToken) return null
    if (value.ageLimit === 'UNSET' && value.maxAgeMillis === undefined) return { ...value }
    if (value.ageLimit === 'SET' && Number.isSafeInteger(value.maxAgeMillis) && value.maxAgeMillis! > 0) return { ...value }
    return null
  }
  if (mode === 'AGE_LIMIT' && hours != null && hours > 0 && Number.isSafeInteger(hours * 3600000)) return { state: 'KNOWN', model, ageLimit: 'SET', maxAgeMillis: hours * 3600000 }
  return null
}

export function sameRetentionSource(observed: TableTag, current?: TableTag) {
  return !!current && observed.name === current.name && observed.observationToken === current.observationToken
    && observed.target.id === current.target.id && observed.target.branch === current.target.branch
    && observed.scope.kind === current.scope.kind && observed.scope.branch === current.scope.branch
}
