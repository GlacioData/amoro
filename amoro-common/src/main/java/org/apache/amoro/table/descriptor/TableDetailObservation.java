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

package org.apache.amoro.table.descriptor;

/** Snapshot and schema already read for this request; null snapshot explicitly means empty. */
public final class TableDetailObservation {
  private final Long snapshotId;
  private final long schemaId;

  public TableDetailObservation(Long snapshotId, long schemaId) {
    if ((snapshotId != null && snapshotId <= 0) || schemaId < 0) {
      throw new IllegalArgumentException("Invalid table detail observation");
    }
    this.snapshotId = snapshotId;
    this.schemaId = schemaId;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  public long getSchemaId() {
    return schemaId;
  }

  public boolean isEmptySnapshot() {
    return snapshotId == null;
  }
}
