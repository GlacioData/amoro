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

import org.apache.amoro.utils.SerializationUtil;

import java.nio.file.Files;
import java.nio.file.Paths;

/** Standalone child-JVM probe; its executor/input classes must come from the old baseline. */
public final class PaimonLegacyTaskCompatibilityProbe {
  private PaimonLegacyTaskCompatibilityProbe() {}

  public static void main(String[] args) throws Exception {
    String executorSource =
        PaimonCompactionExecutor.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toURI()
            .getPath();
    String inputSource =
        PaimonCompactionInput.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toURI()
            .getPath();
    System.out.println("executor CodeSource=" + executorSource);
    System.out.println("input CodeSource=" + inputSource);
    if (!Files.isSameFile(Paths.get(executorSource), Paths.get(args[1]))
        || !Files.isSameFile(Paths.get(inputSource), Paths.get(args[1]))) {
      throw new AssertionError("Legacy classes were not loaded from isolated baseline classes");
    }
    PaimonCompactionInput input =
        SerializationUtil.simpleDeserialize(Files.readAllBytes(Paths.get(args[0])));
    try {
      new PaimonCompactionExecutor(input).execute();
      throw new AssertionError("Legacy executor accepted fixed-bucket input");
    } catch (IllegalStateException expected) {
      if (!(expected.getCause() instanceof UnsupportedOperationException)
          || !expected.getCause().getMessage().contains("found 1001")) {
        throw expected;
      }
      System.out.println("LEGACY_REJECTED_BEFORE_WRITER version=1001");
    }
  }
}
