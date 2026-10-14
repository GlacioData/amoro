/* Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See NOTICE. Licensed under the Apache License, Version 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 */
export interface TagScope { kind: 'DEFAULT' | 'TABLE' | 'BRANCH', branch?: string }
export interface TagRetention { state: string, model: string, ageLimit: string, maxAgeMillis?: number }
export interface TableTag { name: string, type: string, scope: TagScope, target: { id: string, branch?: string }, versionCommittedAtMillis?: number, retention: TagRetention, observationToken?: string }
export interface TagCapabilities { actions: string[], supportedScopes: TagScope[], retention: { supportedModes: string[], model: string, updateClock?: 'RESET_ON_UPDATE' | 'PRESERVE_ANCHOR' | 'UNKNOWN', nativeDefault?: TagRetention, configObservationToken?: string }, deleteEffect: string, preconditionGuarantee: string, unavailableReason?: string }
export interface TagWriteRequest { name?: string, newName?: string, scope: TagScope, target?: { id: string, branch?: string }, retention: { mode: string, maxAgeMillis?: number }, expectedRetentionModel: string, expectedRetentionConfigToken?: string, expectedSourceToken?: string }

export interface TagDeleteResult { name: string, scope: TagScope, referenceState: 'ABSENT', historyCleanup: 'NOT_REQUIRED' | 'BEST_EFFORT_UNVERIFIED' | 'VERIFIED' | 'UNKNOWN' }

export interface TagRetentionUpdateRequest {
  scope: TagScope
  retention: { mode: string, maxAgeMillis?: number }
  expectedRetentionModel: string
  expectedUpdateClock: string
  expectedRetentionConfigToken?: string
}
