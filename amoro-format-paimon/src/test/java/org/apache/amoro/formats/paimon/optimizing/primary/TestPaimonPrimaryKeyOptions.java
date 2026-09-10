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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.mergetree.compact.EarlyFullCompaction;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestPaimonPrimaryKeyOptions {

  @Test
  void defaultsKeepSupportedPrimaryKeyOptions() {
    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(new HashMap<>());

    assertFalse(options.partitionIdleTime().isPresent());
    assertEquals(new BigDecimal("0.33"), options.majorMaxBucketRatio());
  }

  @Test
  void parsesPrimaryKeySpecificOptions() {
    Map<String, String> props = new HashMap<>();
    props.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "PT30M");
    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "0.302");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertEquals(
        Duration.ofMinutes(30), options.partitionIdleTime().orElseThrow(AssertionError::new));
    assertEquals(new BigDecimal("0.33"), options.majorMaxBucketRatio());
  }

  @Test
  void clampsMajorMaxBucketRatioBelowMinimum() {
    Map<String, String> props = new HashMap<>();
    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "0.329");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertEquals(new BigDecimal("0.33"), options.majorMaxBucketRatio());
  }

  @Test
  void truncatesMajorMaxBucketRatioWithoutFloatingPointArithmetic() {
    Map<String, String> props = new HashMap<>();
    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "0.341");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertEquals(new BigDecimal("0.34"), options.majorMaxBucketRatio());

    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "0.339");
    assertEquals(new BigDecimal("0.33"), PaimonPrimaryKeyOptions.from(props).majorMaxBucketRatio());

    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "1.000");
    assertEquals(new BigDecimal("1.00"), PaimonPrimaryKeyOptions.from(props).majorMaxBucketRatio());
  }

  @Test
  void rejectsInvalidMajorMaxBucketRatios() {
    for (String value : new String[] {"", "not-a-number", "NaN", "Infinity", "1.001"}) {
      Map<String, String> props = new HashMap<>();
      props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, value);

      assertThrows(
          IllegalArgumentException.class, () -> PaimonPrimaryKeyOptions.from(props), value);
    }
  }

  @Test
  void parsesPaimonStylePartitionIdleTime() {
    Map<String, String> props = new HashMap<>();
    props.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "10s");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertEquals(
        Duration.ofSeconds(10), options.partitionIdleTime().orElseThrow(AssertionError::new));
  }

  @Test
  void ignoresRemovedMaxBucketsPerTaskOption() {
    Map<String, String> props = new HashMap<>();
    props.put("paimon-optimizer.primary-key.max-buckets-per-task", "0");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertFalse(options.partitionIdleTime().isPresent());
    assertEquals(new BigDecimal("0.33"), options.majorMaxBucketRatio());
  }

  @Test
  void onlyRawDoubleGateRemovesIntervalAndPreservesOtherOptions() {
    for (String writeOnly : Arrays.asList(null, "false", "true", "TRUE", "invalid")) {
      for (String enabled : Arrays.asList(null, "false", "true", "TrUe", "invalid")) {
        Map<String, String> raw = new HashMap<>();
        raw.put("compaction.optimization-interval", "1h");
        raw.put("compaction.total-size-threshold", "64mb");
        raw.put("compaction.incremental-size-threshold", "128mb");
        raw.put("lookup-compact", "gentle");
        raw.put("test.unrelated", "keep");
        if (writeOnly != null) {
          raw.put("write-only", writeOnly);
        }
        if (enabled != null) {
          raw.put("self-optimizing.enabled", enabled);
        }
        Map<String, String> before = new HashMap<>(raw);
        CoreOptions original = CoreOptions.fromMap(raw);
        CoreOptions effective = PaimonPrimaryKeyOptions.compactionStrategyOptions(original);
        Map<String, String> overrides = PaimonPrimaryKeyOptions.compactionWriteOverrides(raw);
        boolean managed = "true".equalsIgnoreCase(writeOnly) && "true".equalsIgnoreCase(enabled);
        Map<String, String> expected = new HashMap<>(raw);
        if (managed) {
          expected.remove("compaction.optimization-interval");
        }
        assertEquals(expected, effective.toMap());
        assertEquals(before, original.toMap());
        assertEquals(before, raw);
        assertNotSame(original, effective);
        assertEquals("false", overrides.get("write-only"));
        assertEquals(managed, overrides.containsKey("compaction.optimization-interval"));
        assertEquals(managed ? 2 : 1, overrides.size());
        assertNull(overrides.get("compaction.optimization-interval"));
        overrides.put("test.unrelated", "changed");
        assertEquals(before, raw);
      }
    }
  }

  @Test
  void absentIntervalDoesNotDisableSizeEarlyFull() {
    Map<String, String> raw = PaimonPrimaryKeyIntervalTestSupport.options(false);
    CoreOptions effective =
        PaimonPrimaryKeyOptions.compactionStrategyOptions(CoreOptions.fromMap(raw));
    assertNull(EarlyFullCompaction.create(effective));
    for (String key :
        Arrays.asList("compaction.total-size-threshold", "compaction.incremental-size-threshold")) {
      Map<String, String> sized = new HashMap<>(raw);
      sized.put(key, "64mb");
      sized.put("compaction.optimization-interval", "1h");
      effective = PaimonPrimaryKeyOptions.compactionStrategyOptions(CoreOptions.fromMap(sized));
      assertNull(effective.optimizedCompactionInterval());
      assertTrue(EarlyFullCompaction.create(effective) != null);
      assertEquals("64mb", effective.toMap().get(key));
    }
  }

  @Test
  void writerCopyLeavesCatalogAndOriginalSchemaUntouched(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      FileStoreTable original =
          PaimonPrimaryKeyIntervalTestSupport.table(
              catalog, "isolated", PaimonPrimaryKeyIntervalTestSupport.options(true));
      Map<String, String> before = new HashMap<>(original.options());
      long schemaId = original.schema().id();
      FileStoreTable effective =
          original.copy(PaimonPrimaryKeyOptions.compactionWriteOverrides(original.options()));
      assertNull(effective.coreOptions().optimizedCompactionInterval());
      assertFalse(effective.coreOptions().writeOnly());
      assertEquals(before, original.options());
      FileStoreTable reloaded =
          (FileStoreTable) catalog.getTable(Identifier.create("db", "isolated"));
      assertEquals(before, reloaded.options());
      assertEquals(schemaId, reloaded.schema().id());
      assertEquals(schemaId, effective.schema().id());
      FileStoreTable unmanaged =
          original.copy(Collections.singletonMap("self-optimizing.enabled", "false"));
      assertEquals(
          Duration.ofHours(1),
          PaimonPrimaryKeyOptions.compactionStrategyOptions(unmanaged.coreOptions())
              .optimizedCompactionInterval());
      assertEquals(
          Duration.ofHours(1),
          unmanaged
              .copy(PaimonPrimaryKeyOptions.compactionWriteOverrides(unmanaged.options()))
              .coreOptions()
              .optimizedCompactionInterval());
    }
  }
}
