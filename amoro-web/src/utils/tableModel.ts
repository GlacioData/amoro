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

import type { ITableModel } from '../types/common.type'

/** Normalize at the response boundary; only absent fields may use the legacy Paimon contract. */
export function normalizeTableModel(model: unknown, format: string, legacyBucketMode?: unknown): ITableModel | null {
  if (model === undefined && format === 'PAIMON' && typeof legacyBucketMode === 'string' && legacyBucketMode.trim()) {
    return { kind: 'BUCKET_MODE', code: legacyBucketMode }
  }
  if (!model || typeof model !== 'object')
    return null
  const { kind, code } = model as Partial<ITableModel>
  if (typeof kind !== 'string' || !kind.trim() || typeof code !== 'string' || !code.trim())
    return null
  return { kind, code }
}

// Format owns the semantics: identical codes in different formats/kinds are not interchangeable.
// https://paimon.apache.org/docs/1.4/primary-key-table/data-distribution/
// https://iceberg.apache.org/spec/#format-versioning
const descriptions = new Map([
  ['PAIMON.BUCKET_MODE.HASH_FIXED', 'paimonBucketHashFixed'],
  ['PAIMON.BUCKET_MODE.HASH_DYNAMIC', 'paimonBucketHashDynamic'],
  ['PAIMON.BUCKET_MODE.KEY_DYNAMIC', 'paimonBucketKeyDynamic'],
  ['PAIMON.BUCKET_MODE.BUCKET_UNAWARE', 'paimonBucketUnaware'],
  ['PAIMON.BUCKET_MODE.POSTPONE_MODE', 'paimonBucketPostpone'],
  ['ICEBERG.FORMAT_VERSION.V1', 'icebergModelV1'],
  ['ICEBERG.FORMAT_VERSION.V2', 'icebergModelV2'],
  ['ICEBERG.FORMAT_VERSION.V3', 'icebergModelV3'],
])

export function tableModelDescription(format: string, model: ITableModel | null): string {
  if (!model)
    return 'tableModelUnavailable'
  return descriptions.get(`${format}.${model.kind}.${model.code}`) || 'tableModelUnknown'
}
