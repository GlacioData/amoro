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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.MemorySize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

class TestPaimonDeletionVectorCounts {
  @TempDir java.nio.file.Path warehouse;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void metadataAvoidsBitmapIoAndLegacyFallbackReadsOnlyMissingCounts(boolean bitmap64)
      throws Exception {
    FileIO io = spy(LocalFileIO.create());
    IndexPathFactory paths =
        new IndexPathFactory() {
          @Override
          public Path toPath(String name) {
            return new Path(warehouse.toUri().toString(), name);
          }

          @Override
          public Path newPath() {
            return toPath(UUID.randomUUID().toString());
          }

          @Override
          public boolean isExternalPath() {
            return false;
          }
        };
    DeletionVectorsIndexFile index =
        spy(new DeletionVectorsIndexFile(io, paths, MemorySize.ofBytes(Long.MAX_VALUE), bitmap64));
    Map<String, DeletionVector> vectors = new LinkedHashMap<>();
    DeletionVector first = bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
    first.delete(1);
    first.delete(7);
    DeletionVector second = bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
    second.delete(3);
    vectors.put("first", first);
    vectors.put("second", second);
    IndexFileMeta original = index.writeSingleFile(vectors);
    IndexFileHandler handler = mock(IndexFileHandler.class);
    clearInvocations(io, index);
    Map<String, Long> expected = new HashMap<>();
    expected.put("first", 2L);
    expected.put("second", 1L);
    assertEquals(
        expected,
        PaimonDeletionVectorCounts.read(
            handler, BinaryRow.EMPTY_ROW, 0, Collections.singletonList(original)));
    verifyNoInteractions(handler, io, index);

    when(handler.dvIndex(BinaryRow.EMPTY_ROW, 0)).thenReturn(index);
    LinkedHashMap<String, DeletionVectorMeta> ranges = new LinkedHashMap<>(original.dvRanges());
    DeletionVectorMeta range = ranges.get("first");
    ranges.put("first", new DeletionVectorMeta("first", range.offset(), range.length(), null));
    IndexFileMeta legacy =
        new IndexFileMeta(
            original.indexType(),
            original.fileName(),
            original.fileSize(),
            original.rowCount(),
            ranges,
            null);
    assertEquals(
        expected,
        PaimonDeletionVectorCounts.read(
            handler, BinaryRow.EMPTY_ROW, 0, Arrays.asList(original, legacy)));
    verify(io).newInputStream(index.path(original));
    verify(index)
        .readDeletionVector(
            org.mockito.ArgumentMatchers.argThat(
                (Map<String, org.apache.paimon.table.source.DeletionFile> missing) ->
                    missing.size() == 1 && missing.containsKey("first")));

    index.delete(original);
    clearInvocations(io, index, handler);
    assertEquals(
        expected,
        PaimonDeletionVectorCounts.read(
            handler, BinaryRow.EMPTY_ROW, 0, Collections.singletonList(original)));
    verifyNoInteractions(handler, io, index);
    assertEquals(
        expected,
        PaimonDeletionVectorCounts.read(
            handler, BinaryRow.EMPTY_ROW, 0, Arrays.asList(legacy, original)));
    verifyNoInteractions(handler, io, index);
    assertThrows(
        RuntimeException.class,
        () ->
            PaimonDeletionVectorCounts.read(
                handler, BinaryRow.EMPTY_ROW, 0, Collections.singletonList(legacy)));
  }

  @Test
  void preservesNativeLastFileWinsAndRejectsInvalidRanges() {
    IndexFileHandler handler = mock(IndexFileHandler.class);
    IndexFileMeta first = file("one", 1L);
    IndexFileMeta last = file("two", 7L);
    assertEquals(
        Collections.singletonMap("data", 7L),
        PaimonDeletionVectorCounts.read(
            handler, BinaryRow.EMPTY_ROW, 0, Arrays.asList(first, last)));
    verifyNoInteractions(handler);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PaimonDeletionVectorCounts.read(
                handler, BinaryRow.EMPTY_ROW, 0, Collections.singletonList(file("invalid", -1L))));
    IndexFileMeta missing =
        new IndexFileMeta(
            "DELETION_VECTORS",
            "no-ranges",
            1L,
            1L,
            (LinkedHashMap<String, DeletionVectorMeta>) null,
            null);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PaimonDeletionVectorCounts.read(
                handler, BinaryRow.EMPTY_ROW, 0, Collections.singletonList(missing)));
  }

  @Test
  void ignoresOnlyShadowedInvalidRanges() {
    IndexFileHandler handler = mock(IndexFileHandler.class);
    IndexFileMeta invalid = file("old", -1L);
    IndexFileMeta valid = file("new", 7L);
    assertEquals(
        Collections.singletonMap("data", 7L),
        PaimonDeletionVectorCounts.read(
            handler, BinaryRow.EMPTY_ROW, 0, Arrays.asList(invalid, valid)));
    invalid.dvRanges().put("data", new DeletionVectorMeta("data", -1, 0, 1L));
    assertEquals(
        Collections.singletonMap("data", 7L),
        PaimonDeletionVectorCounts.read(
            handler, BinaryRow.EMPTY_ROW, 0, Arrays.asList(invalid, valid)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PaimonDeletionVectorCounts.read(
                handler, BinaryRow.EMPTY_ROW, 0, Arrays.asList(valid, invalid)));
    invalid.dvRanges().put("other", new DeletionVectorMeta("other", -1, 0, 1L));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PaimonDeletionVectorCounts.read(
                handler, BinaryRow.EMPTY_ROW, 0, Arrays.asList(invalid, valid)));
    verifyNoInteractions(handler);
  }

  private static IndexFileMeta file(String name, Long count) {
    LinkedHashMap<String, DeletionVectorMeta> ranges = new LinkedHashMap<>();
    ranges.put("data", new DeletionVectorMeta("data", 1, 20, count));
    return new IndexFileMeta("DELETION_VECTORS", name, 21L, 1L, ranges, null);
  }
}
