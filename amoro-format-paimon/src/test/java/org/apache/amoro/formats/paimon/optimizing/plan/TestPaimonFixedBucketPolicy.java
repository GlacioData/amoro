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

package org.apache.amoro.formats.paimon.optimizing.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendCapabilities;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendRewriteOptions;
import org.apache.amoro.table.TableIdentifier;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestPaimonFixedBucketPolicy {
  @Test
  void fixedFullPolicyDoesNotMutateLegacyContextOrBestEffortTargets() {
    PaimonPlanContext legacy =
        PaimonPlanContext.forOptions(
            CoreOptions.fromMap(Collections.emptyMap()),
            new OptimizingConfig().setFullRewriteAllFiles(false).setMaxTaskSize(1024),
            1,
            2,
            3,
            4,
            512,
            6);
    PaimonPlanContext fixed =
        legacy.withFixedRewriteOptions(PaimonAppendRewriteOptions.from(Collections.emptyMap()));
    assertFalse(legacy.fullRewriteAllFiles());
    assertTrue(fixed.fullRewriteAllFiles());
    assertEquals(512, fixed.effectiveTaskInputLimit());
    assertEquals(legacy.targetSize(), fixed.targetSize());
    assertEquals(legacy.planTime(), fixed.planTime());
    assertEquals(legacy.lastFullOptimizingTime(), fixed.lastFullOptimizingTime());
  }

  @ParameterizedTest
  @CsvSource({
    "true,false,false,false,false,true",
    "false,false,false,false,false,true",
    "true,true,false,false,false,false",
    "false,false,true,false,false,false",
    "true,false,true,false,false,true",
    "true,false,false,true,false,false",
    "true,false,false,false,true,false"
  })
  void capabilityMatchesNativeEffectiveMode(
      boolean ordered,
      boolean dv,
      boolean clustering,
      boolean tracking,
      boolean evolution,
      boolean supported) {
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    options.put("bucket-append-ordered", String.valueOf(ordered));
    options.put("deletion-vectors.enabled", String.valueOf(dv));
    options.put("clustering.incremental", String.valueOf(clustering));
    options.put("row-tracking.enabled", String.valueOf(tracking));
    options.put("data-evolution.enabled", String.valueOf(evolution));
    // Paimon ac223f47: CoreOptions.java:3654 bucketClusterEnabled.
    // An inactive clustering option is not an effective clustering writer.
    assertEquals(
        supported, !PaimonAppendCapabilities.fixedBucketRejection(fixed(options)).isPresent());
  }

  @Test
  void refusesOtherShapesWithoutChangingTheirExistingPaths() {
    AppendOnlyFileStoreTable unaware = fixed(Collections.singletonMap("bucket", "-1"));
    when(unaware.bucketMode()).thenReturn(BucketMode.BUCKET_UNAWARE);
    assertTrue(PaimonAppendCapabilities.fixedBucketRejection(unaware).isPresent());
    assertTrue(
        PaimonAppendCapabilities.fixedBucketRejection(mock(PrimaryKeyFileStoreTable.class))
            .isPresent());
  }

  @Test
  void explicitFullPrecedencePreservesFalseAndCatalogInheritance() {
    String amoro = PaimonAppendRewriteOptions.AMORO_FULL_REWRITE;
    String nativeKey = CoreOptions.COMPACTION_FORCE_REWRITE_ALL_FILES.key();
    Map<String, String> options = new HashMap<>();
    PaimonAppendRewriteOptions defaults = PaimonAppendRewriteOptions.from(options);
    assertTrue(defaults.fullRewriteAllFiles());
    assertEquals(PaimonAppendRewriteOptions.Source.AMORO_DEFAULT, defaults.source());
    options.put(nativeKey, "false");
    assertFalse(PaimonAppendRewriteOptions.from(options).fullRewriteAllFiles());
    assertEquals(
        PaimonAppendRewriteOptions.Source.PAIMON_EXPLICIT,
        PaimonAppendRewriteOptions.from(options).source());
    PaimonTable inherited =
        new PaimonTable(
            TableIdentifier.of("test", "db", "table"),
            fixed(options),
            Collections.singletonMap("table." + amoro, "true"));
    assertTrue(PaimonAppendRewriteOptions.from(inherited.properties()).fullRewriteAllFiles());
    options.put(nativeKey, "true");
    options.put(amoro, "false");
    PaimonTable overridden =
        new PaimonTable(
            inherited.id(), fixed(options), Collections.singletonMap("table." + amoro, "true"));
    assertFalse(PaimonAppendRewriteOptions.from(overridden.properties()).fullRewriteAllFiles());
    assertEquals(
        PaimonAppendRewriteOptions.Source.AMORO_EXPLICIT,
        PaimonAppendRewriteOptions.from(overridden.properties()).source());
    options.put(amoro, "TRUE");
    assertTrue(PaimonAppendRewriteOptions.from(options).fullRewriteAllFiles());
  }

  @ParameterizedTest
  @CsvSource({"self-optimizing.full.rewrite-all-files,yes", "compaction.force-rewrite-all-files,1"})
  void rejectsMalformedSelectedBoolean(String key, String value) {
    assertThrows(
        IllegalArgumentException.class,
        () -> PaimonAppendRewriteOptions.from(Collections.singletonMap(key, value)));
  }

  @Test
  void constraintFingerprintIsIndependentOfSchedulingButTracksWriterOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    String first = PaimonAppendCapabilities.rewriteFingerprint(fixed(options));
    options.put("self-optimizing.full.trigger.interval", "1234");
    options.put("compaction.file-num-limit", "100");
    assertEquals(first, PaimonAppendCapabilities.rewriteFingerprint(fixed(options)));
    for (String key :
        new String[] {
          "bucket-append-ordered",
          "file-index.range-bitmap.columns",
          "bucket-key",
          "clustering.incremental"
        }) {
      Map<String, String> changed = new HashMap<>(options);
      changed.put(key, key.endsWith("columns") || key.equals("bucket-key") ? "hm" : "false");
      assertNotEquals(first, PaimonAppendCapabilities.rewriteFingerprint(fixed(changed)), key);
    }
    Map<String, String> layout = new HashMap<>(options);
    layout.put("bucket", "4");
    assertNotEquals(first, PaimonAppendCapabilities.rewriteFingerprint(fixed(layout)));
    Map<String, String> bucketFunction = new HashMap<>(options);
    bucketFunction.put("bucket-function.type", "MOD");
    assertNotEquals(first, PaimonAppendCapabilities.rewriteFingerprint(fixed(bucketFunction)));
  }

  private static AppendOnlyFileStoreTable fixed(Map<String, String> options) {
    AppendOnlyFileStoreTable table = mock(AppendOnlyFileStoreTable.class);
    Map<String, String> copy = new HashMap<>(options);
    when(table.options()).thenReturn(copy);
    when(table.coreOptions()).thenReturn(CoreOptions.fromMap(copy));
    when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    return table;
  }
}
