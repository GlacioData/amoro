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

package org.apache.amoro.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

class TestSerializationUtilStream {

  private static final class CloseTrackingOutputStream extends ByteArrayOutputStream {
    private boolean closed;

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }

  private static Map<String, String> sample() {
    Map<String, String> map = new HashMap<>();
    map.put("commit-user", "recovery-user");
    map.put("partition", "dt=2026-08-27");
    return map;
  }

  @Test
  void streamedSerializeEqualsByteArrayVersion() {
    Map<String, String> obj = sample();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SerializationUtil.simpleSerialize(obj, out);

    assertArrayEquals(SerializationUtil.simpleSerialize(obj).array(), out.toByteArray());
  }

  @Test
  void streamedSerializeRoundTrips() {
    Map<String, String> obj = sample();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SerializationUtil.simpleSerialize(obj, out);

    assertEquals(obj, SerializationUtil.simpleDeserialize(out.toByteArray()));
  }

  @Test
  void streamedSerializeLeavesStreamOpen() throws IOException {
    Map<String, String> obj = sample();

    CloseTrackingOutputStream out = new CloseTrackingOutputStream();
    SerializationUtil.simpleSerialize(obj, out);
    out.write(0x42);

    byte[] bytes = out.toByteArray();
    assertEquals(0x42, bytes[bytes.length - 1]);
    assertFalse(out.closed);
  }

  @Test
  void streamedSerializeWrapsIoException() {
    OutputStream failing =
        new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            throw new IOException("boom");
          }
        };

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> SerializationUtil.simpleSerialize(sample(), failing));

    assertTrue(failure.getMessage().startsWith("serialization error of "));
  }
}
