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

export function clampPage(current: number, total: number, pageSize: number): number {
  const safeSize = pageSize > 0 ? Math.floor(pageSize) : 10
  const maxPage = Math.max(1, Math.ceil(Math.max(0, total) / safeSize))
  const safeCurrent = Number.isFinite(current) && current >= 1 ? Math.floor(current) : 1
  return Math.min(safeCurrent, maxPage)
}

export function paginateItems<T>(items: readonly T[], current: number, pageSize: number): T[] {
  const safeSize = pageSize > 0 ? Math.floor(pageSize) : 10
  const safePage = clampPage(current, items.length, safeSize)
  const start = (safePage - 1) * safeSize
  return items.slice(start, start + safeSize)
}

export function isValidParallelism(value: unknown): boolean {
  if (typeof value === 'string' && value.trim() === '')
    return false
  if (value === null || value === undefined || typeof value === 'boolean')
    return false
  const num = Number(value)
  return Number.isInteger(num) && num >= 1
}

export interface GroupSubmitInput {
  name: string
  container: string
  properties: Record<string, string> | false
}

export function buildGroupSubmitPayload(input: GroupSubmitInput): {
  name: string
  container: string
  properties: Record<string, string>
} | null {
  if (!input.properties)
    return null
  return {
    name: input.name,
    container: input.container,
    properties: input.properties,
  }
}

export async function runExclusiveSubmit(
  isBusy: () => boolean,
  setBusy: (busy: boolean) => void,
  task: () => Promise<void>,
): Promise<boolean> {
  if (isBusy())
    return false
  setBusy(true)
  try {
    await task()
    return true
  }
  finally {
    setBusy(false)
  }
}
