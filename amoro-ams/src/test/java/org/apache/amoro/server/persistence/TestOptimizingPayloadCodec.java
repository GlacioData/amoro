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

package org.apache.amoro.server.persistence;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonBucketCompactionUnit;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionInput;
import org.apache.amoro.optimizing.BaseOptimizingInput;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.RewriteFilesInput;
import org.apache.amoro.server.persistence.OptimizingPayloadException.FailureReason;
import org.apache.amoro.server.utils.CompressUtil;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

class TestOptimizingPayloadCodec {

  @Test
  void writesGzipAndRoundTripsCurrentPayload() {
    PaimonCompactionInput input = paimonInput();

    byte[] encoded = OptimizingPayloadCodec.encode(input);
    PaimonCompactionInput restored = OptimizingPayloadCodec.decode(encoded);

    assertEquals(0x1f, encoded[0] & 0xff);
    assertEquals(0x8b, encoded[1] & 0xff);
    assertArrayEquals(input.getTaskBytes(), restored.getTaskBytes());
    assertEquals(input.getCommitUser(), restored.getCommitUser());
    assertEquals(input.getCommitIdentifier(), restored.getCommitIdentifier());
  }

  @Test
  void readsLegacyRawJavaSerialization() {
    PaimonCompactionInput input = paimonInput();
    byte[] raw = SerializationUtil.simpleSerialize(input).array();

    PaimonCompactionInput restored = OptimizingPayloadCodec.decode(raw);

    assertArrayEquals(input.getTaskBytes(), restored.getTaskBytes());
    assertEquals(input.getCommitUser(), restored.getCommitUser());
    assertEquals(input.getCommitIdentifier(), restored.getCommitIdentifier());
  }

  @Test
  void readsLegacyRawIcebergPayload() {
    RewriteFilesInput input =
        new RewriteFilesInput(
            new DataFile[0], new DataFile[0], new ContentFile<?>[0], new ContentFile<?>[0], null);
    byte[] raw = SerializationUtil.simpleSerialize(input).array();

    RewriteFilesInput restored = OptimizingPayloadCodec.decode(raw);

    assertEquals(0, restored.rewrittenDataFiles().length);
    assertEquals(0, restored.rePosDeletedDataFiles().length);
    assertEquals(0, restored.readOnlyDeleteFiles().length);
    assertEquals(0, restored.rewrittenDeleteFiles().length);
  }

  @Test
  void rejectsUnknownPayloadHeader() {
    OptimizingPayloadException failure =
        assertThrows(
            OptimizingPayloadException.class,
            () -> OptimizingPayloadCodec.decode(new byte[] {1, 2, 3, 4}));

    assertEquals(FailureReason.UNKNOWN_FORMAT, failure.getReason());
  }

  @Test
  void classifiesTruncatedGzipWithoutRawFallback() {
    byte[] encoded = OptimizingPayloadCodec.encode(paimonInput());
    byte[] truncated = new byte[] {encoded[0], encoded[1], encoded[2]};

    OptimizingPayloadException failure =
        assertThrows(
            OptimizingPayloadException.class, () -> OptimizingPayloadCodec.decode(truncated));

    assertEquals(FailureReason.CORRUPT_GZIP, failure.getReason());
  }

  @Test
  void classifiesGzipCrcFailure() {
    byte[] encoded = OptimizingPayloadCodec.encode(paimonInput());
    encoded[encoded.length - 1] ^= 0x01;

    OptimizingPayloadException failure =
        assertThrows(
            OptimizingPayloadException.class, () -> OptimizingPayloadCodec.decode(encoded));

    assertEquals(FailureReason.CORRUPT_GZIP, failure.getReason());
  }

  @Test
  void classifiesInvalidSerializedObjectInsideValidGzip() {
    byte[] encoded = CompressUtil.gzip(new byte[] {(byte) 0xac, (byte) 0xed, 0, 5, 1});

    OptimizingPayloadException failure =
        assertThrows(
            OptimizingPayloadException.class, () -> OptimizingPayloadCodec.decode(encoded));

    assertEquals(FailureReason.DESERIALIZATION_FAILED, failure.getReason());
  }

  @Test
  void preservesNullSemantics() {
    assertNull(OptimizingPayloadCodec.encode(null));
    assertNull(OptimizingPayloadCodec.decode(null));
  }

  @Test
  void failureMessageContainsOnlySafePayloadMetadata() {
    String secret = "secret-commit-user";
    byte[] unknown = secret.getBytes(StandardCharsets.UTF_8);

    OptimizingPayloadException failure =
        assertThrows(
            OptimizingPayloadException.class, () -> OptimizingPayloadCodec.decode(unknown));

    assertEquals(
        "Unknown optimizing payload format (length=18, prefix=73 65 63 72)", failure.getMessage());
    assertFalse(failure.getMessage().contains(secret));
  }

  @Test
  void streamedEncodeMatchesLegacyByteFormat() {
    PaimonCompactionInput input = paimonInput();

    byte[] legacy = CompressUtil.gzip(SerializationUtil.simpleSerialize(input).array());
    byte[] streamed = OptimizingPayloadCodec.encode(input);

    assertArrayEquals(legacy, streamed);
  }

  @Test
  void roundTripsMultiTaskInputMap() {
    Map<Integer, BaseOptimizingInput> tasks = new HashMap<>();
    for (int taskId = 1; taskId <= 3; taskId++) {
      tasks.put(taskId, paimonInput("user-" + taskId, new byte[] {(byte) taskId}));
    }

    @SuppressWarnings("unchecked")
    Map<Integer, BaseOptimizingInput> restored =
        OptimizingPayloadCodec.decode(OptimizingPayloadCodec.encode(tasks));

    assertEquals(3, restored.size());
    for (int taskId = 1; taskId <= 3; taskId++) {
      PaimonCompactionInput task = (PaimonCompactionInput) restored.get(taskId);
      assertEquals("user-" + taskId, task.getCommitUser());
      assertArrayEquals(new byte[] {(byte) taskId}, task.getTaskBytes());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 1023, 1024, 1025, 1048593})
  void streamedEncodePreservesPayloadsAcrossBufferBoundaries(int size) {
    byte[] taskBytes = new byte[size];
    new Random(42).nextBytes(taskBytes);
    PaimonCompactionInput input = paimonInput("large-input-user", taskBytes);

    byte[] encoded = OptimizingPayloadCodec.encode(input);

    assertArrayEquals(CompressUtil.gzip(SerializationUtil.simpleSerialize(input).array()), encoded);
    PaimonCompactionInput restored = OptimizingPayloadCodec.decode(encoded);
    assertArrayEquals(taskBytes, restored.getTaskBytes());
    assertEquals(input.getCommitIdentifier(), restored.getCommitIdentifier());
  }

  @Test
  void roundTripsLargePrimaryKeyTaskMapWithLegacyByteFormat() {
    Map<Integer, BaseOptimizingInput> tasks = new HashMap<>();
    for (int taskId = 1; taskId <= 10000; taskId++) {
      List<PaimonBucketCompactionUnit> units = new ArrayList<>();
      units.add(
          new PaimonBucketCompactionUnit(new byte[] {1, 2, 3, 4}, taskId, 200, 100, 8192, 1000, 1));
      tasks.put(
          taskId,
          new PaimonPrimaryKeyCompactionInput(
              null, units, OptimizingType.MINOR, false, 9876L, "primary-key-user", 73L));
    }

    byte[] encoded = OptimizingPayloadCodec.encode(tasks);

    assertArrayEquals(CompressUtil.gzip(SerializationUtil.simpleSerialize(tasks).array()), encoded);
    Map<Integer, BaseOptimizingInput> restored = OptimizingPayloadCodec.decode(encoded);
    assertEquals(tasks.size(), restored.size());
    for (int taskId = 1; taskId <= 10000; taskId++) {
      PaimonPrimaryKeyCompactionInput input =
          assertInstanceOf(PaimonPrimaryKeyCompactionInput.class, restored.get(taskId));
      assertEquals(OptimizingType.MINOR, input.getOptimizingType());
      assertFalse(input.isFullCompaction());
      assertEquals(9876L, input.getTargetSnapshotId());
      assertEquals("primary-key-user", input.getCommitUser());
      assertEquals(73L, input.getCommitIdentifier());
      assertEquals(1, input.getUnits().size());
      PaimonBucketCompactionUnit unit = input.getUnits().get(0);
      assertArrayEquals(new byte[] {1, 2, 3, 4}, unit.getPartitionBytes());
      assertEquals(taskId, unit.getBucket());
      assertEquals(200, unit.getFileCount());
      assertEquals(100, unit.getSortedRunCount());
      assertEquals(8192, unit.getFileSizeInBytes());
      assertEquals(1000, unit.getRecordCount());
      assertEquals(1, unit.getLastFileCreationTime());
    }
  }

  private static PaimonCompactionInput paimonInput() {
    return paimonInput("recovery-user", new byte[] {1, 2, 3, 4});
  }

  private static PaimonCompactionInput paimonInput(String commitUser, byte[] taskBytes) {
    return new PaimonCompactionInput(
        null, taskBytes, 1, commitUser, "dt=2026-08-27", 9876L, 1464516326809600L);
  }
}
