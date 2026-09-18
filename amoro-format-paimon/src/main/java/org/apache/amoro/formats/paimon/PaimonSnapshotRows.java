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

package org.apache.amoro.formats.paimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.avro.AvroRowDatumReader;
import org.apache.paimon.format.avro.SeekableInputStreamWrapper;
import org.apache.paimon.fs.Path;
import org.apache.paimon.shade.org.apache.avro.Schema;
import org.apache.paimon.shade.org.apache.avro.file.DataFileReader;
import org.apache.paimon.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.paimon.shade.org.apache.avro.io.DatumReader;
import org.apache.paimon.shade.org.apache.avro.io.Decoder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.UriReader;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Request-owned rows for both manifest levels. Never caches decoded rows or keeps a reader session.
 */
abstract class PaimonSnapshotRows implements CloseableIterator<InternalRow> {
  final PaimonSnapshotReadDeadline deadline;

  PaimonSnapshotRows(PaimonSnapshotReadDeadline deadline) {
    this.deadline = deadline;
  }

  static PaimonSnapshotRows open(
      ObjectsFile<?> file,
      Path path,
      Long size,
      RowType type,
      boolean avro,
      PaimonSnapshotReadDeadline deadline)
      throws IOException {
    deadline.check();
    if (!avro) {
      // Paimon 1.4.2 ObjectsFile.createIterator is streaming, unlike read(), which collects a List.
      // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/utils/ObjectsFile.java
      return new IteratorRows(file.createIterator(path, size), deadline);
    }
    long length = size == null ? deadline.call(() -> file.fileSize(path)) : size;
    if (length < 0) {
      throw new IOException("Negative manifest file size");
    }
    SkippingDatumReader datum = new SkippingDatumReader(type, UriReader.fromFile(file.fileIO()));
    // Follow Paimon 1.4.2 AvroBulkFormat with the bundle's relocated Avro 1.11.4.
    // DataFileReader initializes at the first block; no physical seek or client offset is used.
    // https://github.com/apache/paimon/blob/release-1.4.2/paimon-format/src/main/java/org/apache/paimon/format/avro/AvroBulkFormat.java
    deadline.check();
    SeekableInputStreamWrapper input =
        new SeekableInputStreamWrapper(file.fileIO().newInputStream(path), length);
    try {
      deadline.check();
      DataFileReader<InternalRow> reader = new DataFileReader<>(input, datum);
      deadline.check();
      return new AvroRows(reader, datum, length, deadline);
    } catch (Throwable failure) {
      // The constructor does not close caller-owned input on failure. Preserve the primary cause.
      // https://github.com/apache/avro/blob/release-1.11.4/lang/java/avro/src/main/java/org/apache/avro/file/DataFileReader.java
      try {
        input.close();
      } catch (Throwable closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  /** Returns the number skipped; a short result means EOF. Does not look ahead after the budget. */
  long skip(long count) {
    if (count < 0) {
      throw new IllegalArgumentException("Negative row skip");
    }
    long skipped = 0;
    while (skipped < count && hasNext()) {
      next();
      skipped++;
    }
    return skipped;
  }

  private static final class IteratorRows extends PaimonSnapshotRows {
    private final CloseableIterator<InternalRow> rows;

    private IteratorRows(CloseableIterator<InternalRow> rows, PaimonSnapshotReadDeadline deadline) {
      super(deadline);
      this.rows = rows;
    }

    @Override
    public boolean hasNext() {
      return deadline.call(rows::hasNext);
    }

    @Override
    public InternalRow next() {
      return deadline.call(rows::next);
    }

    @Override
    public void close() throws Exception {
      rows.close();
    }
  }

  private static final class AvroRows extends PaimonSnapshotRows {
    private final DataFileReader<InternalRow> reader;
    private final SkippingDatumReader datum;
    private final long length;
    private long blockRemaining;

    private AvroRows(
        DataFileReader<InternalRow> reader,
        SkippingDatumReader datum,
        long length,
        PaimonSnapshotReadDeadline deadline) {
      super(deadline);
      this.reader = reader;
      this.datum = datum;
      this.length = length;
    }

    @Override
    public boolean hasNext() {
      return deadline.call(
          () -> {
            if (blockRemaining > 0) {
              return true;
            }
            if (!reader.hasNext()) {
              // Avro 1.11.4 hasNext catches EOFException, including a truncated block.
              // Only a completed block/header boundary at the declared file end is clean EOF.
              // https://github.com/apache/avro/blob/release-1.11.4/lang/java/avro/src/main/java/org/apache/avro/file/DataFileStream.java
              if (reader.previousSync() != length) {
                throw new IOException("Manifest ended outside an Avro block boundary");
              }
              return false;
            }
            blockRemaining = reader.getBlockCount();
            if (blockRemaining <= 0) {
              throw new IOException("Invalid Avro block count");
            }
            return true;
          });
    }

    @Override
    public InternalRow next() {
      return deadline.call(
          () -> {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            InternalRow row = reader.next();
            blockRemaining--;
            return row;
          });
    }

    @Override
    long skip(long count) {
      if (count < 0) {
        throw new IllegalArgumentException("Negative row skip");
      }
      long skipped = 0;
      while (skipped < count && hasNext()) {
        long remaining = count - skipped;
        if (blockRemaining == reader.getBlockCount() && remaining >= blockRemaining) {
          // Public nextBlock() requires a block boundary. It still reads/decompresses that block;
          // it avoids row graphs, not prefix IO or the largest-block memory allocation.
          // https://github.com/apache/avro/blob/release-1.11.4/lang/java/avro/src/main/java/org/apache/avro/file/DataFileStream.java
          deadline.call(reader::nextBlock);
          skipped += blockRemaining;
          blockRemaining = 0;
        } else {
          datum.skip = true;
          try {
            next();
          } finally {
            datum.skip = false;
          }
          skipped++;
        }
      }
      return skipped;
    }

    @Override
    public void close() throws IOException {
      // Do not check the deadline before cleanup: an expired request must still release input.
      reader.close();
    }
  }

  private static final class SkippingDatumReader implements DatumReader<InternalRow> {
    private final AvroRowDatumReader delegate;
    private Schema schema;
    private boolean skip;

    private SkippingDatumReader(RowType type, UriReader uriReader) {
      delegate = new AvroRowDatumReader(type, uriReader);
    }

    @Override
    public void setSchema(Schema schema) {
      this.schema = schema;
      delegate.setSchema(schema);
    }

    @Override
    public InternalRow read(InternalRow reuse, Decoder decoder) throws IOException {
      if (skip) {
        // Skip using the writer schema; only selected rows use Paimon's version-aware conversion.
        // https://github.com/apache/avro/blob/release-1.11.4/lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumReader.java
        GenericDatumReader.skip(schema, decoder);
        return null;
      }
      return delegate.read(reuse, decoder);
    }
  }
}
