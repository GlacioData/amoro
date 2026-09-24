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

package org.apache.amoro.formats.paimon.optimizing.plan;

import org.apache.paimon.data.BinaryRow;

/** Physical Append unit; totalBuckets is a checked layout fact, not a different unit identity. */
public final class PaimonAppendUnitKey {
  private final BinaryRow partition;
  private final int bucket;

  public PaimonAppendUnitKey(BinaryRow partition, int bucket) {
    // Paimon ac223f47, manifest/FileEntry.java:49-55: never infer bucket from a path string.
    this.partition = partition.copy();
    this.bucket = bucket;
  }

  public BinaryRow partition() {
    return partition.copy();
  }

  public int bucket() {
    return bucket;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PaimonAppendUnitKey)) {
      return false;
    }
    PaimonAppendUnitKey that = (PaimonAppendUnitKey) other;
    return bucket == that.bucket && partition.equals(that.partition);
  }

  @Override
  public int hashCode() {
    return 31 * partition.hashCode() + bucket;
  }
}
