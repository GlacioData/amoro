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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class TestPaimonFixedBucketPublication {
  // Source:
  // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/io/DataFileIndexWriter.java#L159-L175
  // Native embedded metadata is independent of physical input bytes. Exercise the full codec,
  // including header/checksum overhead, not a file-count proxy for its 32 MiB envelope.
  @ParameterizedTest
  @CsvSource({"true,4", "false,4", "true,5", "false,5", "true,7", "false,7"})
  void splitsAggregatedMetadataWithoutChangingOwnershipOrOrder(boolean ordered, int fileCount) {
    byte[] index = new byte[8 * 1024 * 1024];
    List<DataFileMeta> files = new ArrayList<>();
    for (int i = 0; i < fileCount; i++) {
      files.add(file(i, index));
    }
    PaimonFixedBucketTask task = task(files, ordered);
    PaimonFixedBucketTaskSerializer codec = new PaimonFixedBucketTaskSerializer();
    assertThrows(IllegalArgumentException.class, () -> codec.serialize(task));
    List<PaimonFixedBucketTask> parts = codec.splitForPublication(task);
    assertEquals(
        fileCount == 4
            ? Arrays.asList(2, 2)
            : fileCount == 5 ? Arrays.asList(3, 2) : Arrays.asList(3, 2, 2),
        parts.stream().map(p -> p.files().size()).collect(Collectors.toList()));
    assertEquals(
        files, parts.stream().flatMap(p -> p.files().stream()).collect(Collectors.toList()));
    for (PaimonFixedBucketTask part : parts) {
      byte[] bytes = codec.serialize(part);
      assertTrue(bytes.length <= PaimonFixedBucketTaskSerializer.MAX_PAYLOAD_BYTES);
      PaimonFixedBucketTask restored = codec.deserialize(codec.getVersion(), bytes);
      assertEquals(part.files(), restored.files());
      assertEquals(task.partition(), restored.partition());
      assertEquals(task.bucket(), restored.bucket());
      assertEquals(task.totalBuckets(), restored.totalBuckets());
      assertEquals(task.snapshotId(), restored.snapshotId());
      assertEquals(task.ordered(), restored.ordered());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rebalancesTailsOnEachSideOfAnUnencodableBarrier(boolean ordered) {
    byte[] index = new byte[8 * 1024 * 1024];
    List<DataFileMeta> files = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      files.add(file(i, i == 4 ? new byte[index.length + 1] : index));
    }
    List<PaimonFixedBucketTask> parts =
        new PaimonFixedBucketTaskSerializer().splitForPublication(task(files, ordered));
    assertEquals(4, parts.size());
    assertEquals(files.subList(0, 2), parts.get(0).files());
    assertEquals(files.subList(2, 4), parts.get(1).files());
    assertEquals(files.subList(5, 7), parts.get(2).files());
    assertEquals(files.subList(7, 9), parts.get(3).files());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void carriesTheBoundaryAcrossTwoFileGroups(boolean ordered) {
    List<DataFileMeta> files = new ArrayList<>();
    int[] sizesMiB = {9, 9, 12, 12, 18, 12};
    for (int i = 0; i < sizesMiB.length; i++) {
      files.add(fileWithMetadataMiB(i, sizesMiB[i]));
    }
    PaimonFixedBucketTaskSerializer codec = new PaimonFixedBucketTaskSerializer();
    List<PaimonFixedBucketTask> parts = codec.splitForPublication(task(files, ordered));
    assertEquals(3, parts.size());
    for (int i = 0; i < parts.size(); i++) {
      assertEquals(files.subList(i * 2, i * 2 + 2), parts.get(i).files());
      byte[] bytes = codec.serialize(parts.get(i));
      assertTrue(bytes.length <= PaimonFixedBucketTaskSerializer.MAX_PAYLOAD_BYTES);
      assertEquals(parts.get(i).files(), codec.deserialize(codec.getVersion(), bytes).files());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void retainsOriginalGroupsWhenTailCannotBePaired(boolean ordered) {
    PaimonFixedBucketTaskSerializer codec = new PaimonFixedBucketTaskSerializer();
    List<DataFileMeta> files =
        Arrays.asList(
            file(0, null), file(1, null), fileWithMetadataMiB(2, 24), fileWithMetadataMiB(3, 12));
    assertThrows(
        IllegalArgumentException.class, () -> codec.serialize(task(files.subList(2, 4), ordered)));
    List<PaimonFixedBucketTask> parts = codec.splitForPublication(task(files, ordered));
    assertEquals(1, parts.size());
    assertEquals(files.subList(0, 3), parts.get(0).files());

    List<DataFileMeta> triple =
        Arrays.asList(
            fileWithMetadataMiB(0, 12), fileWithMetadataMiB(1, 12), fileWithMetadataMiB(2, 12));
    parts = codec.splitForPublication(task(triple, ordered));
    assertEquals(1, parts.size());
    assertEquals(triple.subList(0, 2), parts.get(0).files());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void neverBorrowsAcrossRejectedInput(boolean ordered) {
    List<DataFileMeta> files =
        Arrays.asList(
            file(0, null),
            file(1, null),
            file(2, null),
            file(3, new byte[8 * 1024 * 1024 + 1]),
            file(4, null));
    List<PaimonFixedBucketTask> parts =
        new PaimonFixedBucketTaskSerializer().splitForPublication(task(files, ordered));
    assertEquals(1, parts.size());
    assertEquals(files.subList(0, 3), parts.get(0).files());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void unencodableFileIsABarrierAndDoesNotFailOtherGroups(boolean ordered) {
    List<DataFileMeta> files =
        Arrays.asList(
            file(0, null),
            file(1, null),
            file(2, new byte[8 * 1024 * 1024 + 1]),
            file(3, null),
            file(4, null));
    List<PaimonFixedBucketTask> parts =
        new PaimonFixedBucketTaskSerializer().splitForPublication(task(files, ordered));
    assertEquals(2, parts.size());
    assertEquals(files.subList(0, 2), parts.get(0).files());
    assertEquals(files.subList(3, 5), parts.get(1).files());
    assertTrue(
        new PaimonFixedBucketTaskSerializer()
            .splitForPublication(task(files.subList(1, 4), ordered))
            .isEmpty());
  }

  private static DataFileMeta file(int i, byte[] index) {
    return DataFileMeta.forAppend(
        "f" + i,
        1024,
        10,
        SimpleStats.EMPTY_STATS,
        i * 10L,
        i * 10L + 9,
        0,
        Collections.emptyList(),
        index,
        null,
        null,
        null,
        null,
        null);
  }

  // Synthetic metadata only: each field fits its segment cap, while the aggregate can exceed
  // one segment. These are codec inputs, not real paths or an index-rebuild integration fixture.
  private static DataFileMeta fileWithMetadataMiB(int i, int sizeMiB) {
    List<String> extraFiles = new ArrayList<>();
    for (int remaining = sizeMiB - 8; remaining > 0; remaining -= 8) {
      char[] chars = new char[Math.min(remaining, 8) * 1024 * 1024];
      Arrays.fill(chars, 'x');
      extraFiles.add(new String(chars));
    }
    return file(i, new byte[Math.min(sizeMiB, 8) * 1024 * 1024]).copy(extraFiles);
  }

  private static PaimonFixedBucketTask task(List<DataFileMeta> files, boolean ordered) {
    return new PaimonFixedBucketTask(
        "c.d.t",
        "file:/review",
        BinaryRow.singleColumn(7),
        1,
        2,
        1,
        0,
        ordered,
        false,
        OptimizingType.MINOR,
        PaimonAppendRewriteOptions.Source.AMORO_DEFAULT,
        "fingerprint",
        files);
  }
}
