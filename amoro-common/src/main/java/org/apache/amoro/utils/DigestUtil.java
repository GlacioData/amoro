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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Digest helpers with a stable lowercase hexadecimal representation. */
public class DigestUtil {

  private DigestUtil() {}

  public static String sha256Hex(String value) {
    Objects.requireNonNull(value, "value");
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      char[] encoded = new char[digest.length * 2];
      char[] alphabet = "0123456789abcdef".toCharArray();
      for (int index = 0; index < digest.length; index++) {
        int byteValue = digest[index] & 0xff;
        encoded[index * 2] = alphabet[byteValue >>> 4];
        encoded[index * 2 + 1] = alphabet[byteValue & 0x0f];
      }
      return new String(encoded);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
    }
  }
}
