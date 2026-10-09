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

import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Metadata consistency checks before a fixed rewrite can enter native commit or recovery. */
final class PaimonFixedBucketOutputValidator {
  private PaimonFixedBucketOutputValidator() {}

  static void validate(
      AppendOnlyFileStoreTable table,
      PaimonFixedBucketTask payload,
      PaimonCompactionOutput output,
      CommitMessageImpl message) {
    require(output != null, "Missing fixed rewrite output");
    PaimonRewriteMetrics metrics = PaimonRewriteMetrics.collect(Collections.singletonList(message));
    require(
        metrics.inputFiles() == output.getCompactedFileCount()
            && metrics.inputBytes() == output.getCompactedFileSize()
            && metrics.inputRecords() == output.getCompactedRecordCount()
            && metrics.outputFiles() == output.getProducedFileCount()
            && metrics.outputBytes() == output.getProducedFileSize()
            && metrics.outputRecords() == output.getProducedRecordCount(),
        "Fixed rewrite metrics differ from commit message");
    require(
        metrics.inputRecords() == metrics.outputRecords(),
        "Fixed non-DV rewrite must preserve every input row");

    long requiredSchema =
        payload.files().stream().mapToLong(DataFileMeta::schemaId).max().getAsLong();
    long outputSchema = message.compactIncrement().compactAfter().get(0).schemaId();
    PaimonCompactionSchema.validateRewriteSchema(table, requiredSchema, outputSchema);
    boolean external =
        table
            .store()
            .pathFactory()
            .createDataFilePathFactory(payload.partition(), payload.bucket())
            .isExternalPath();
    long nextSequence = payload.files().get(0).minSequenceNumber();
    Set<String> names = new HashSet<>();
    // Paimon ac223f47, BaseAppendFileStoreWrite.java:198-201 and
    // io/RowDataFileWriter.java:70-72,99-123: each rolled COMPACT file restarts at the first
    // input's min sequence. Unordered tasks retain that native shape; ordered tasks use
    // PaimonFixedBucketRewrite's metadata-only adaptation to preserve their continuous range.
    for (DataFileMeta file : message.compactIncrement().compactAfter()) {
      require(
          safeName(file.fileName())
              && names.add(file.fileName())
              && file.fileSource().orElse(null) == FileSource.COMPACT
              && file.fileSize() >= 0
              && file.rowCount() > 0
              && file.schemaId() == outputSchema
              && file.deleteRowCount().orElse(0L) == 0
              && file.firstRowId() == null
              && file.writeCols() == null,
          "Invalid fixed rewrite output metadata");
      require(
          file.externalPath().isPresent() == external
              && (!external || validExternalPath(table, payload, file)),
          "Fixed output path differs from the bound writer layout");
      for (String extra : file.extraFiles()) {
        require(
            safeName(extra) && names.add(extra), "Invalid or reused fixed output index filename");
      }
      require(file.minSequenceNumber() == nextSequence, "Fixed output sequence start mismatch");
      long endSequence = Math.addExact(nextSequence, file.rowCount());
      require(
          file.maxSequenceNumber() == endSequence - 1,
          "Fixed output sequence does not cover its row count");
      if (payload.ordered()) {
        nextSequence = endSequence;
      }
    }
  }

  private static boolean validExternalPath(
      AppendOnlyFileStoreTable table, PaimonFixedBucketTask payload, DataFileMeta file) {
    FileStorePathFactory paths = table.store().pathFactory();
    Path actual = new Path(file.externalPath().get());
    // Paimon ac223f47, FileStorePathFactory.java:216-224 and fs/ExternalPathProvider.java:37-62.
    // Reuse native layout generation per allowed root, including entropy directories. A singleton
    // root removes placement randomness without changing the physical path within that root.
    for (Path root : paths.getExternalPaths()) {
      ExternalPathProvider provider =
          ExternalPathProvider.create(
              table.coreOptions().externalPathStrategy(),
              Collections.singletonList(root),
              paths.relativeBucketPath(payload.partition(), payload.bucket()),
              null);
      if (provider != null && actual.equals(provider.getNextExternalDataPath(file.fileName()))) {
        return true;
      }
    }
    return false;
  }

  private static boolean safeName(String name) {
    return name != null
        && !name.isEmpty()
        && !name.equals(".")
        && !name.equals("..")
        && !name.contains("/")
        && !name.contains("\\")
        && !name.contains(":")
        && name.indexOf(0) < 0;
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
