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

/**
 * Normalize only Snapshot chart counters, not the displayed summary.
 * The common Java DTO exposes string maps for every lake format.
 * https://www.typescriptlang.org/docs/handbook/2/objects.html#index-signatures
 */
export function snapshotChartData(data: Record<string, Record<string, string | null>>) {
  const keys = [...new Set(Object.values(data).flatMap(point => Object.keys(point)))]
  return Object.fromEntries(Object.entries(data).map(([time, point]) => [
    time,
    Object.fromEntries(keys.map((key) => {
      const raw = point[key]
      // Counts must remain exact. Unknown, fractional and unsafe integers are chart gaps.
      const value = typeof raw === 'string' && /^[+-]?\d+$/.test(raw) ? Number(raw) : Number.NaN
      return [key, Number.isSafeInteger(value) ? value : null]
    })),
  ]))
}
