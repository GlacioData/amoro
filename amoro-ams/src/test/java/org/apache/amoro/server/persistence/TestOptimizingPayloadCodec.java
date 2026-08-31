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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput;
import org.apache.amoro.optimizing.RewriteFilesInput;
import org.apache.amoro.server.persistence.OptimizingPayloadException.FailureReason;
import org.apache.amoro.server.utils.CompressUtil;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

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

  private static PaimonCompactionInput paimonInput() {
    return new PaimonCompactionInput(
        null,
        new byte[] {1, 2, 3, 4},
        1,
        "recovery-user",
        "dt=2026-08-27",
        9876L,
        1464516326809600L);
  }
}
