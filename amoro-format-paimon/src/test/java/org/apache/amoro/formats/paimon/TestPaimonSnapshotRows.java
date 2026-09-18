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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.avro.SeekableInputStreamWrapper;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.shade.org.apache.avro.Schema;
import org.apache.paimon.shade.org.apache.avro.file.CodecFactory;
import org.apache.paimon.shade.org.apache.avro.file.DataFileReader;
import org.apache.paimon.shade.org.apache.avro.file.DataFileWriter;
import org.apache.paimon.shade.org.apache.avro.generic.GenericData;
import org.apache.paimon.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.paimon.shade.org.apache.avro.generic.GenericDatumWriter;
import org.apache.paimon.shade.org.apache.avro.generic.GenericRecord;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ObjectsFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

class TestPaimonSnapshotRows {
  @TempDir java.nio.file.Path directory;
  private final FileIO io = LocalFileIO.create();
  private final RowType type = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
  private final Schema schema =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"test\",\"fields\":["
                  + "{\"name\":\"id\",\"type\":\"int\"},"
                  + "{\"name\":\"unknown\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}]}]}");

  private Path fixture(String codec, int records) throws Exception {
    Path path = new Path(directory.resolve(codec + "-" + records + ".avro").toUri());
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      writer.setCodec(CodecFactory.fromString(codec));
      writer.create(schema, io.newOutputStream(path, false));
      for (int i = 0; i < records; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", i);
        record.put("unknown", i % 2 == 0 ? null : Collections.singletonList("extra schema field"));
        writer.append(record);
        if ((i + 1) % 7 == 0) {
          writer.sync();
        }
      }
    }
    return path;
  }

  private ObjectsFile<?> source(FileIO fileIO) {
    ObjectsFile<?> source = mock(ObjectsFile.class);
    when(source.fileIO()).thenReturn(fileIO);
    return source;
  }

  private PaimonSnapshotRows open(Path path) throws Exception {
    return PaimonSnapshotRows.open(
        source(io), path, io.getFileSize(path), type, true, new PaimonSnapshotReadDeadline());
  }

  @ParameterizedTest
  @ValueSource(strings = {"null", "deflate", "snappy", "zstandard"})
  void exactPositionAcrossBlocksAndCodecs(String codec) throws Exception {
    Path path = fixture(codec, 100);
    for (long offset : new long[] {0, 1, 6, 7, 8, 14, 24, 25, 69, 75, 99, 100, 101}) {
      try (PaimonSnapshotRows rows = open(path)) {
        assertEquals(Math.min(offset, 100), rows.skip(offset));
        for (int id = (int) offset; id < Math.min(offset + 25, 100); id++) {
          assertTrue(rows.hasNext());
          assertTrue(rows.hasNext(), "hasNext must not consume a row");
          assertEquals(id, rows.next().getInt(0));
        }
        if (offset + 25 >= 100) {
          assertFalse(rows.hasNext());
        }
      }
    }
  }

  @Test
  void skipWorksAfterPartialBlockAndDoesNotReadNextBlockAfterBudget() throws Exception {
    Path path = fixture("null", 21);
    try (PaimonSnapshotRows rows = open(path)) {
      assertEquals(0, rows.skip(0));
      assertEquals(0, rows.next().getInt(0));
      assertEquals(8, rows.skip(8));
      assertEquals(9, rows.next().getInt(0));
      assertEquals(4, rows.skip(4));
      assertEquals(14, rows.next().getInt(0));
      assertThrows(IllegalArgumentException.class, () -> rows.skip(-1));
    }
    // Append garbage after complete blocks: stopping exactly at the page budget must not inspect
    // it.
    java.nio.file.Path local = java.nio.file.Paths.get(path.toUri());
    Files.write(local, new byte[] {1}, java.nio.file.StandardOpenOption.APPEND);
    try (PaimonSnapshotRows rows = open(path)) {
      assertEquals(21, rows.skip(21));
      // Avro may defer a malformed partial block-header failure until next(), not hasNext().
      assertThrows(SnapshotReadException.class, rows::next);
    }
  }

  @Test
  void emptyFileUnknownSizeAndOlderProjectedSchema() throws Exception {
    Path empty = fixture("null", 0);
    ObjectsFile<?> source = source(io);
    when(source.fileSize(empty)).thenReturn(io.getFileSize(empty));
    try (PaimonSnapshotRows rows =
        PaimonSnapshotRows.open(
            source, empty, null, type, true, new PaimonSnapshotReadDeadline())) {
      assertFalse(rows.hasNext());
      assertEquals(0, rows.skip(100));
    }
    Path path = fixture("null", 14);
    // New projected fields absent from an older writer schema stay null, as in Paimon's reader.
    RowType evolved =
        DataTypes.ROW(
            DataTypes.FIELD(0, "id", DataTypes.INT()),
            DataTypes.FIELD(1, "later", DataTypes.STRING()));
    try (PaimonSnapshotRows rows =
        PaimonSnapshotRows.open(
            source, path, io.getFileSize(path), evolved, true, new PaimonSnapshotReadDeadline())) {
      assertEquals(8, rows.skip(8));
      InternalRow row = rows.next();
      assertEquals(8, row.getInt(0));
      assertTrue(row.isNullAt(1));
    }
  }

  @Test
  void truncatedBlockAndCorruptHeaderAreNotCleanEof() throws Exception {
    Path path = fixture("null", 7);
    java.nio.file.Path local = java.nio.file.Paths.get(path.toUri());
    byte[] bytes = Files.readAllBytes(local);
    int headerEnd;
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(
            new SeekableInputStreamWrapper(io.newInputStream(path), bytes.length),
            new GenericDatumReader<>())) {
      headerEnd = (int) reader.previousSync();
    }
    // Truncate the block header, its body, and the final sync marker independently.
    for (int end : new int[] {headerEnd + 1, headerEnd + 5, bytes.length - 1}) {
      Files.write(local, java.util.Arrays.copyOf(bytes, end));
      try (PaimonSnapshotRows rows = open(path)) {
        SnapshotReadException error = assertThrows(SnapshotReadException.class, rows::next);
        assertEquals(SnapshotReadException.Reason.READ_FAILED, error.getReason());
      }
    }
    Files.write(local, new byte[] {0, 1, 2, 3});
    FileIO trackedIO = spy(io);
    SeekableInputStream stream = spy(io.newInputStream(path));
    doReturn(stream).when(trackedIO).newInputStream(path);
    doAnswer(
            i -> {
              i.callRealMethod();
              throw new IOException("close failure");
            })
        .when(stream)
        .close();
    IOException failure =
        assertThrows(
            IOException.class,
            () ->
                PaimonSnapshotRows.open(
                    source(trackedIO), path, 4L, type, true, new PaimonSnapshotReadDeadline()));
    assertEquals(1, failure.getSuppressed().length);
    assertEquals("close failure", failure.getSuppressed()[0].getMessage());
    verify(stream).close();
  }

  @Test
  void deadlineCoversOpenSkipAndCleanupWithoutLeakingInput() throws Exception {
    Path path = fixture("null", 100);
    AtomicLong clock = new AtomicLong();
    FileIO trackedIO = spy(io);
    SeekableInputStream stream = spy(io.newInputStream(path));
    doAnswer(
            i -> {
              clock.set(100);
              return stream;
            })
        .when(trackedIO)
        .newInputStream(path);
    assertEquals(
        SnapshotReadException.Reason.TIMEOUT,
        assertThrows(
                SnapshotReadException.class,
                () ->
                    PaimonSnapshotRows.open(
                        source(trackedIO),
                        path,
                        io.getFileSize(path),
                        type,
                        true,
                        new PaimonSnapshotReadDeadline(clock::get, 100)))
            .getReason());
    verify(stream).close();

    clock.set(0);
    try (PaimonSnapshotRows rows =
        PaimonSnapshotRows.open(
            source(io),
            path,
            io.getFileSize(path),
            type,
            true,
            new PaimonSnapshotReadDeadline(clock::incrementAndGet, 25))) {
      assertEquals(
          SnapshotReadException.Reason.TIMEOUT,
          assertThrows(SnapshotReadException.class, () -> rows.skip(99)).getReason());
    }
  }

  @Test
  void nonAvroUsesStreamingIteratorAndPropagatesCloseFailure() throws Exception {
    ObjectsFile<?> source = source(io);
    CloseableIterator<InternalRow> iterator = mock(CloseableIterator.class);
    Path path = new Path(directory.resolve("other-format").toUri());
    when(source.createIterator(path, 10L)).thenReturn(iterator);
    when(iterator.hasNext()).thenReturn(true, true, false);
    doThrow(new IOException("close failure")).when(iterator).close();
    PaimonSnapshotRows rows =
        PaimonSnapshotRows.open(source, path, 10L, type, false, new PaimonSnapshotReadDeadline());
    assertEquals(2, rows.skip(3));
    verify(iterator, times(2)).next();
    assertThrows(IOException.class, rows::close);
    verify(source, never()).read(any(String.class));
    verify(source, never()).fileIO();
  }
}
