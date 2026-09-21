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

package org.apache.amoro.formats.paimon.optimizing.health;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.source.DeletionFile;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Read-only fixed-snapshot DV counts; metadata counts do not verify bitmap readability. */
public final class PaimonDeletionVectorCounts {
  private PaimonDeletionVectorCounts() {}

  public static Map<String, Long> read(
      IndexFileHandler handler, BinaryRow partition, int bucket, List<IndexFileMeta> files) {
    Map<String, Long> counts = new HashMap<>();
    DeletionVectorsIndexFile index = null;
    Set<String> resolved = new HashSet<>();
    // Paimon 1.4.2 stores nullable cardinalities and projects them without reading bitmaps.
    // Visit newer entries first so overwritten legacy bitmaps never trigger unnecessary reads.
    // Batch only the winning ranges with missing counts from each legacy file.
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/SnapshotReaderImpl.java#L686-L704
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/deletionvectors/DeletionVectorsIndexFile.java#L98-L143
    for (int i = files.size() - 1; i >= 0; i--) {
      IndexFileMeta file = files.get(i);
      if (file.dvRanges() == null) {
        throw new IllegalArgumentException("Missing deletion-vector ranges");
      }
      Map<String, DeletionFile> missing = new HashMap<>();
      for (DeletionVectorMeta range : file.dvRanges().values()) {
        if (range == null || range.dataFileName() == null || range.dataFileName().isEmpty()) {
          throw new IllegalArgumentException("Invalid deletion-vector file identity");
        }
        if (!resolved.add(range.dataFileName())) {
          continue;
        }
        if (range.offset() < 0
            || range.length() <= 0
            || (range.cardinality() != null && range.cardinality() < 0)) {
          throw new IllegalArgumentException("Invalid deletion-vector metadata");
        }
        if (range.cardinality() != null) {
          counts.put(range.dataFileName(), range.cardinality());
        } else {
          if (index == null) {
            index = handler.dvIndex(partition, bucket);
          }
          missing.put(
              range.dataFileName(),
              new DeletionFile(index.path(file).toString(), range.offset(), range.length(), null));
        }
      }
      if (!missing.isEmpty()) {
        Map<String, DeletionVector> vectors = index.readDeletionVector(missing);
        for (String name : missing.keySet()) {
          counts.put(
              name,
              Objects.requireNonNull(vectors.get(name), "Missing deletion vector")
                  .getCardinality());
        }
      }
    }
    return counts;
  }
}
