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

import static org.apache.amoro.server.persistence.OptimizingPayloadException.FailureReason.CORRUPT_GZIP;
import static org.apache.amoro.server.persistence.OptimizingPayloadException.FailureReason.DESERIALIZATION_FAILED;
import static org.apache.amoro.server.persistence.OptimizingPayloadException.FailureReason.UNKNOWN_FORMAT;

import org.apache.amoro.server.utils.CompressUtil;
import org.apache.amoro.utils.SerializationUtil;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/** Codec for optimizing inputs and outputs persisted as database BLOBs. */
public final class OptimizingPayloadCodec {

  private static final int SERIALIZATION_BUFFER_SIZE = 1024;
  private static final int GZIP_MAGIC_FIRST = 0x1f;
  private static final int GZIP_MAGIC_SECOND = 0x8b;
  private static final int JAVA_MAGIC_FIRST = 0xac;
  private static final int JAVA_MAGIC_SECOND = 0xed;
  private static final int JAVA_VERSION_FIRST = 0x00;
  private static final int JAVA_VERSION_SECOND = 0x05;

  private OptimizingPayloadCodec() {}

  /**
   * New writes retain the existing Java-serialization plus GZIP wire format. Serialization streams
   * through a bounded buffer into GZIP so the uncompressed byte array never resides in memory as a
   * whole.
   */
  public static byte[] encode(Object value) {
    if (value == null) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out);
        BufferedOutputStream buffered = new BufferedOutputStream(gzip, SERIALIZATION_BUFFER_SIZE)) {
      // Batch small object-field writes before compression without retaining the full payload.
      SerializationUtil.simpleSerialize(value, buffered);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  /**
   * Reads the current GZIP representation and the raw Java representation written before GZIP was
   * introduced. A corrupt GZIP stream never falls back to raw decoding.
   */
  public static <T> T decode(byte[] content) {
    if (content == null) {
      return null;
    }
    if (isGzip(content)) {
      byte[] serialized;
      try {
        serialized = CompressUtil.unGzip(content);
      } catch (RuntimeException e) {
        throw new OptimizingPayloadException(
            CORRUPT_GZIP, "Failed to decompress optimizing payload (" + describe(content) + ")", e);
      }
      return deserialize(serialized, content);
    }
    if (isJavaSerialization(content)) {
      return deserialize(content, content);
    }
    throw new OptimizingPayloadException(
        UNKNOWN_FORMAT, "Unknown optimizing payload format (" + describe(content) + ")");
  }

  public static String describe(byte[] content) {
    if (content == null) {
      return "length=null, prefix=null";
    }
    StringBuilder prefix = new StringBuilder();
    int prefixLength = Math.min(content.length, 4);
    for (int index = 0; index < prefixLength; index++) {
      if (index > 0) {
        prefix.append(' ');
      }
      int value = content[index] & 0xff;
      if (value < 0x10) {
        prefix.append('0');
      }
      prefix.append(Integer.toHexString(value));
    }
    return "length=" + content.length + ", prefix=" + prefix;
  }

  private static <T> T deserialize(byte[] serialized, byte[] persistedContent) {
    try {
      return SerializationUtil.simpleDeserialize(serialized);
    } catch (RuntimeException e) {
      throw new OptimizingPayloadException(
          DESERIALIZATION_FAILED,
          "Failed to deserialize optimizing payload (" + describe(persistedContent) + ")",
          e);
    }
  }

  private static boolean isGzip(byte[] content) {
    return content.length >= 2
        && (content[0] & 0xff) == GZIP_MAGIC_FIRST
        && (content[1] & 0xff) == GZIP_MAGIC_SECOND;
  }

  private static boolean isJavaSerialization(byte[] content) {
    return content.length >= 4
        && (content[0] & 0xff) == JAVA_MAGIC_FIRST
        && (content[1] & 0xff) == JAVA_MAGIC_SECOND
        && (content[2] & 0xff) == JAVA_VERSION_FIRST
        && (content[3] & 0xff) == JAVA_VERSION_SECOND;
  }
}
