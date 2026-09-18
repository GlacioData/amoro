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

import javax.annotation.Nullable;

public class ConsumerInfo {

  private final String consumerId;
  private final long nextSnapshotId;
  @Nullable private final AmoroSnapshotsOfTable amoroCurrentSnapshotsOfTable;

  public ConsumerInfo(
      String consumerId,
      long nextSnapshotId,
      @Nullable AmoroSnapshotsOfTable amoroCurrentSnapshotsOfTable) {
    this.consumerId = consumerId;
    this.nextSnapshotId = nextSnapshotId;
    this.amoroCurrentSnapshotsOfTable = amoroCurrentSnapshotsOfTable;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public long getNextSnapshotId() {
    return nextSnapshotId;
  }

  /** Missing metadata does not change consumer progress or imply a specific expiration cause. */
  @Nullable
  public AmoroSnapshotsOfTable getAmoroCurrentSnapshotsOfTable() {
    return amoroCurrentSnapshotsOfTable;
  }
}
