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

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.amoro.utils.DigestUtil;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** Capability checks for Amoro's Append optimizing paths. */
public final class PaimonAppendCapabilities {
  private PaimonAppendCapabilities() {}

  public static Optional<String> appendRejection(FileStoreTable table) {
    if (table instanceof AppendOnlyFileStoreTable
        && table.bucketMode() == BucketMode.BUCKET_UNAWARE) {
      return dataEvolutionRejection(table.coreOptions());
    }
    return fixedBucketRejection(table);
  }

  public static Optional<String> dataEvolutionRejection(CoreOptions options) {
    // Paimon 1.4.2 requires a different read/write path for Data Evolution:
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/append/dataevolution/DataEvolutionCompactTask.java#L79-L117
    // Amoro's ordinary Append task does not support that path. This capability gate applies
    // to optimizing only; table health and maintenance retain their own eligibility rules.
    return options.dataEvolutionEnabled()
        ? Optional.of("APPEND_DATA_EVOLUTION_UNSUPPORTED")
        : Optional.empty();
  }

  public static Optional<String> fixedBucketRejection(FileStoreTable table) {
    if (!(table instanceof AppendOnlyFileStoreTable)) {
      return Optional.of("FIXED_APPEND_REQUIRED");
    }
    if (table.bucketMode() != BucketMode.HASH_FIXED || table.coreOptions().bucket() <= 0) {
      return Optional.of("HASH_FIXED_REQUIRED");
    }
    CoreOptions options = table.coreOptions();
    if (options.deletionVectorsEnabled()) {
      return Optional.of("FIXED_APPEND_DV_UNSUPPORTED");
    }
    // Source:
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L3654-L3658
    // Match the effective native writer mode, not merely the presence of clustering options.
    if (options.bucketClusterEnabled()) {
      return Optional.of("FIXED_APPEND_CLUSTERING_UNSUPPORTED");
    }
    // Same SHA, paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java:647.
    // Row tracking and data evolution are not fixed-bucket rewrite capabilities.
    if (options.rowTrackingEnabled() || options.dataEvolutionEnabled()) {
      return Optional.of("FIXED_APPEND_SPECIAL_FIELDS_UNSUPPORTED");
    }
    return Optional.empty();
  }

  /**
   * Fingerprint writer constraints only. Scheduling limits belong to the planning key; structural
   * health retains its existing fingerprint. Schema and snapshot identity travel separately.
   */
  public static String rewriteFingerprint(AppendOnlyFileStoreTable table) {
    Map<String, String> constraints = new TreeMap<>();
    table
        .options()
        .forEach(
            (key, value) -> {
              if (key.equals("bucket")
                  || key.equals("bucket-key")
                  || key.equals("bucket-function.type")
                  || key.equals("bucket-append-ordered")
                  || key.equals("write-only")
                  || key.equals("deletion-vectors.enabled")
                  || key.equals("row-tracking.enabled")
                  || key.equals("data-evolution.enabled")
                  || key.equals("target-file-size")
                  || key.equals("file.format")
                  || key.equals("file.compression")
                  || key.startsWith("file-index.")
                  || key.startsWith("clustering.")
                  || key.startsWith("sink.clustering.")
                  || key.startsWith("data-file.")) {
                constraints.put(key, value);
              }
            });
    // Source: same SHA, operation/BaseAppendFileStoreWrite.java:187 compactRewrite.
    // The bound table creates the writer; index and layout options cannot be swapped at execution.
    StringBuilder encoded = new StringBuilder("fixed-append-rewrite-v1");
    constraints.forEach(
        (key, value) -> {
          encoded.append(key.length()).append(':').append(key);
          encoded.append(value.length()).append(':').append(value);
        });
    return DigestUtil.sha256Hex(encoded.toString());
  }
}
