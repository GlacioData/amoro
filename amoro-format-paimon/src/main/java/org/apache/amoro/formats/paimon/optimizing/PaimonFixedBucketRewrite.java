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

import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exact fixed-bucket rewrite; candidate selection and ordered-scope proof belong to the planner.
 */
final class PaimonFixedBucketRewrite {
  private PaimonFixedBucketRewrite() {}

  static CommitMessage execute(PaimonCompactionInput input) {
    try {
      PaimonFixedBucketTask task =
          new PaimonFixedBucketTaskSerializer()
              .deserialize(input.getSerializerVersion(), input.getTaskBytes());
      task.validateBinding(input.getTable());
      if (task.snapshotId() != input.getTargetSnapshotId()) {
        throw new IllegalArgumentException("Fixed task snapshot binding mismatch");
      }
      AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) input.getTable().originalTable();
      long requiredSchemaId =
          task.files().stream().mapToLong(DataFileMeta::schemaId).max().orElse(task.schemaId());
      // Paimon ac223f47, table/AbstractFileStoreTable.java:382 preserves original options.
      // Reuse Amoro's compatibility guard only when a prepared file requires a newer schema.
      table =
          (AppendOnlyFileStoreTable)
              PaimonCompactionSchema.refreshFixed(table, requiredSchemaId, task.snapshotId());
      List<DataFileMeta> after;
      // Source:
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/BaseAppendFileStoreWrite.java#L187-L229
      // Rewrite precisely the supplied files. Never rescan latest or invoke a second native picker.
      // Keep write-only unchanged: compactRewrite is an explicit rewrite, not background
      // compaction.
      BaseAppendFileStoreWrite write = table.store().newWrite(input.getCommitUser());
      Throwable rewriteFailure = null;
      try {
        after = write.compactRewrite(task.partition(), task.bucket(), null, task.files());
      } catch (Exception | Error failure) {
        rewriteFailure = failure;
        throw failure;
      } finally {
        // FileStoreWrite is not AutoCloseable in 1.4.2; preserve the primary native failure.
        try {
          write.close();
        } catch (Exception closeFailure) {
          if (rewriteFailure == null) {
            throw closeFailure;
          }
          rewriteFailure.addSuppressed(closeFailure);
        }
      }
      if (task.ordered()) {
        // Paimon ac223f47, BaseAppendFileStoreWrite.java:198-201 creates a NEW counter
        // per rolled file. RawFileSplitRead.java:214-224 preserves supplied input order;
        // RollingFileWriterImpl.java:119-133 preserves output close order. Adapt only the
        // metadata to preserve our ordered task range across AppendOnlySplitGenerator's
        // min-sequence sort (table/source/AppendOnlySplitGenerator.java:52-58).
        // PojoDataFileMeta.java:333-356 preserves file contents and index metadata.
        List<DataFileMeta> orderedAfter = new ArrayList<>(after.size());
        long nextSequence = task.files().get(0).minSequenceNumber();
        for (DataFileMeta file : after) {
          long endSequence = Math.addExact(nextSequence, file.rowCount());
          orderedAfter.add(file.assignSequenceNumber(nextSequence, endSequence - 1));
          nextSequence = endSequence;
        }
        after = orderedAfter;
      }
      // Same SHA, table/sink/CommitMessageImpl.java:51 carries real bucket and totalBuckets.
      return new CommitMessageImpl(
          task.partition(),
          task.bucket(),
          task.totalBuckets(),
          DataIncrement.emptyIncrement(),
          new CompactIncrement(task.files(), after, Collections.emptyList()));
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to execute Paimon fixed-bucket task for partition " + input.getPartitionPath(),
          e);
    }
  }
}
