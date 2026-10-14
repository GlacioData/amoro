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
import { reactive } from 'vue'
import { requestEpoch } from './snapshot-page-state.ts'

interface SnapshotChoice {
  snapshotId: string
  commitTime: string | null
}

/** Read retained snapshots in bounded pages; IDs remain opaque strings. */
export function snapshotPicker(readPage: (page: number, pageSize: number) => Promise<{ list: SnapshotChoice[], total: number }>) {
  const requests = requestEpoch()
  const pageSize = 25
  const state = reactive({ rows: [] as SnapshotChoice[], page: 0, loading: false, error: null as unknown, hasMore: false })
  return {
    state,
    reset() {
      requests.invalidate()
      state.rows = []
      state.page = 0
      state.loading = false
      state.error = null
      state.hasMore = false
    },
    async load() {
      if (state.loading || (state.page > 0 && !state.hasMore))
        return
      const ticket = requests.begin()
      const page = state.page + 1
      state.loading = true
      state.error = null
      try {
        const result = await readPage(page, pageSize)
        if (!requests.current(ticket))
          return
        if (!Array.isArray(result.list) || !Number.isSafeInteger(result.total) || result.total < 0
          || result.list.some(row => typeof row.snapshotId !== 'string' || !row.snapshotId)) {
          throw new Error('Invalid snapshot list response')
        }
        state.rows = [...new Map([...state.rows, ...result.list].map(row => [row.snapshotId, row])).values()]
        state.page = page
        // Native readers may skip expired entries without filling the candidate page.
        state.hasMore = page * pageSize < result.total
      }
      catch (error) {
        if (requests.current(ticket))
          state.error = error
      }
      finally {
        if (requests.current(ticket))
          state.loading = false
      }
    },
  }
}
