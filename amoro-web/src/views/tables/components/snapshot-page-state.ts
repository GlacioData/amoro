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

/** Reject stale asynchronous results, including their finally handlers. */
export function requestEpoch() {
  let generation = 0
  return {
    begin: () => ++generation,
    current: (ticket: number) => ticket === generation,
    invalidate: () => { generation++ },
  }
}

/** Stores page-start cursors only, never file rows or server reader sessions. */
export function cursorPages() {
  return {
    starts: [null] as (string | null)[],
    index: 0,
    next: null as string | null,
    commit(index: number, start: string | null, next: string | null) {
      this.starts = this.starts.slice(0, index)
      this.starts.push(start)
      this.index = index
      this.next = next
    },
    reset() {
      this.starts = [null]
      this.index = 0
      this.next = null
    },
  }
}

export function snapshotRowKey(pageRenderId: number, index: number) {
  return `${pageRenderId}:${index}`
}

/** Preserve field boundaries: concatenated catalog/database names can collide. */
export function snapshotTableKey(query: Record<string, unknown>) {
  return JSON.stringify([query.catalog, query.db, query.table, query.type])
}

/** Branches, tags and consumers have independent name spaces. */
export function sameSnapshotSelection(left: { type: string, value: string }, right: { type: string, value: string }) {
  return left.type === right.type && left.value === right.value
}
