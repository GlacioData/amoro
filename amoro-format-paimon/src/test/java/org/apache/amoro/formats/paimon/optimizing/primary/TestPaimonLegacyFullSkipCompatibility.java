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

package org.apache.amoro.formats.paimon.optimizing.primary;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.TableOptimizingCommitter;
import org.apache.amoro.utils.SerializationUtil;
import org.junit.jupiter.api.Test;

import java.io.ObjectInputStream;
import java.net.URL;
import java.net.URLClassLoader;

class TestPaimonLegacyFullSkipCompatibility {
  @Test
  void precompiledOldPluginLinksToCurrentDefaultMethods() throws Exception {
    URL jar = getClass().getResource("/paimon-full-skip-legacy/plugin.jar");
    try (URLClassLoader loader = new URLClassLoader(new URL[] {jar}, getClass().getClassLoader())) {
      Object plugin = loader.loadClass("org.apache.amoro.compat.LegacyPlugin").newInstance();
      ((TableOptimizingCommitter) plugin)
          .commit(TableOptimizingCommitter.CommitMode.RECOVERY_REPLAY);
      assertEquals(1, plugin.getClass().getField("commits").get(plugin));
      assertEquals(0L, ((OptimizationContext) plugin).getFullRetryNotBefore());
    }
  }

  @Test
  void oldSerializedOutputHasNoSkipAndNewMarkerRoundTrips() throws Exception {
    try (ObjectInputStream stream =
        new ObjectInputStream(
            getClass().getResourceAsStream("/paimon-full-skip-legacy/output.bin"))) {
      PaimonPrimaryKeyCompactionOutput old = (PaimonPrimaryKeyCompactionOutput) stream.readObject();
      assertNull(old.getSkipReason());
      assertEquals(2, old.getCompactedFileCount());
      assertEquals(5, old.getProducedFileCount());
      assertArrayEquals(new byte[] {1, 2, 3}, old.getCommitMessageBytesList().get(0));
    }
    java.nio.ByteBuffer serialized =
        SerializationUtil.simpleSerialize(PaimonPrimaryKeyCompactionOutput.staleFull());
    byte[] bytes = new byte[serialized.remaining()];
    serialized.get(bytes);
    PaimonPrimaryKeyCompactionOutput current = SerializationUtil.simpleDeserialize(bytes);
    assertEquals(
        PaimonPrimaryKeyCompactionOutput.STALE_FULL_BEFORE_EXECUTE, current.getSkipReason());
    assertEquals(0, current.commitMessageCount());
  }
}
