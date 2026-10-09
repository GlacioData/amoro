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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TestPaimonFixedBucketNativeRewrite {
  @BeforeAll
  static void reportNativeRuntime() {
    System.out.println(
        "PAIMON_NATIVE_CODESOURCE="
            + BaseAppendFileStoreWrite.class.getProtectionDomain().getCodeSource().getLocation());
    System.out.println("TEST_JAVA_VERSION=" + System.getProperty("java.version"));
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void contiguousRewritePreservesBucketRowsAndSequences(
      boolean partitioned, boolean ordered, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, partitioned, ordered)) {
      fixture.append(0, 1);
      fixture.append(1, 64);
      fixture.append(65, 1);
      Map<String, List<String>> before = fixture.rowsByUnit();
      Map<String, List<ManifestEntry>> units = fixture.units();
      assertEquals(partitioned ? 4 : 2, units.size());
      long snapshot = fixture.table.latestSnapshot().get().id();
      for (List<ManifestEntry> files : units.values()) {
        assertEquals(3, files.size());
        for (ManifestEntry file : files) {
          assertEquals(2, file.totalBuckets());
          assertTrue(file.bucket() == 0 || file.bucket() == 1);
        }
        assertTrue(
            files.stream().mapToLong(f -> f.file().fileSize()).max().getAsLong()
                > files.stream().mapToLong(f -> f.file().fileSize()).min().getAsLong());
        // Source: https://paimon.apache.org/docs/1.4/append-table/bucketed/#bucketed-streaming
        // Native rewriting receives a safe ordered range; it does not select or sort files.
        CommitMessageImpl message = fixture.rewrite(files);
        assertEquals(files.get(0).bucket(), message.bucket());
        assertEquals(files.get(0).partition(), message.partition());
        assertEquals(2, message.totalBuckets());
        List<DataFileMeta> output = message.compactIncrement().compactAfter();
        assertEquals(1, output.size());
        assertEquals(files.get(0).file().minSequenceNumber(), output.get(0).minSequenceNumber());
        long records = files.stream().mapToLong(f -> f.file().rowCount()).sum();
        assertEquals(
            output.get(0).minSequenceNumber() + records - 1, output.get(0).maxSequenceNumber());
        if (ordered) {
          assertEquals(files.get(2).file().maxSequenceNumber(), output.get(0).maxSequenceNumber());
        } else {
          // Paimon ac223f47, operation/BucketedAppendFileStoreWrite.java:75,84:
          // Unordered write-only writers skip restored files; overlapping sequences are valid.
          files.forEach(file -> assertEquals(0, file.file().minSequenceNumber()));
        }
        assertEquals(
            files.stream().mapToLong(f -> f.file().rowCount()).sum(), output.get(0).rowCount());
        assertEquals(snapshot, fixture.table.latestSnapshot().get().id());
        fixture.commit(message);
        snapshot++;
      }
      assertEquals(before, fixture.rowsByUnit());
      assertEquals(
          before.values().stream().flatMap(List::stream).sorted().collect(Collectors.toList()),
          fixture.snapshotRows());
      fixture.units().values().forEach(files -> assertEquals(1, files.size()));
    }
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void nativeRewriteDoesNotSortInputs(boolean partitioned, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, partitioned, true)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      List<ManifestEntry> files = fixture.units().values().iterator().next();
      List<ManifestEntry> reversed = new ArrayList<>(files);
      Collections.reverse(reversed);
      List<String> expected =
          fixture.rows(
              files.get(0),
              reversed.stream().map(ManifestEntry::file).collect(Collectors.toList()));
      CommitMessageImpl output = fixture.rewrite(reversed);
      assertEquals(expected, fixture.rows(files.get(0), output.compactIncrement().compactAfter()));
      assertNotEquals(
          fixture.rows(
              files.get(0), files.stream().map(ManifestEntry::file).collect(Collectors.toList())),
          expected);
      assertEquals(
          reversed.get(0).file().minSequenceNumber(),
          output.compactIncrement().compactAfter().get(0).minSequenceNumber());
      // The unsafe native probe is never committed to the table.
      assertEquals(2, fixture.table.latestSnapshot().get().id());
    }
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void skippingRetainedFileCreatesOverlappingOutput(boolean partitioned, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, partitioned, true)) {
      fixture.append(0, 1);
      fixture.append(1, 64);
      fixture.append(65, 1);
      List<ManifestEntry> files = fixture.units().values().iterator().next();
      DataFileMeta retained = files.get(1).file();
      CommitMessageImpl message = fixture.rewrite(Arrays.asList(files.get(0), files.get(2)));
      DataFileMeta output = message.compactIncrement().compactAfter().get(0);
      // BaseAppendFileStoreWrite#compactRewrite (pinned source L187) renumbers A+C contiguously.
      // The output overlaps retained B: equal row counts alone cannot prove ordered safety.
      assertTrue(output.minSequenceNumber() <= retained.maxSequenceNumber());
      assertTrue(output.maxSequenceNumber() >= retained.minSequenceNumber());
      assertEquals(3, fixture.table.latestSnapshot().get().id());
    }
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void contiguousPartialRewriteKeepsOtherFilesAndUnits(boolean partitioned, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, partitioned, true)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      fixture.append(2, 1);
      Map<String, List<String>> before = fixture.rowsByUnit();
      Map<String, List<ManifestEntry>> units = fixture.units();
      String selected = units.keySet().iterator().next();
      List<ManifestEntry> files = units.get(selected);
      DataFileMeta retained = files.get(2).file();
      CommitMessageImpl message = fixture.rewrite(files.subList(0, 2));
      assertTrue(
          message.compactIncrement().compactAfter().get(0).maxSequenceNumber()
              < retained.minSequenceNumber());
      fixture.commit(message);
      Map<String, List<ManifestEntry>> after = fixture.units();
      assertEquals(before, fixture.rowsByUnit());
      assertEquals(
          before.values().stream().flatMap(List::stream).sorted().collect(Collectors.toList()),
          fixture.snapshotRows());
      assertEquals(2, after.get(selected).size());
      assertTrue(
          after.get(selected).stream()
              .anyMatch(f -> f.file().fileName().equals(retained.fileName())));
      for (String other : units.keySet()) {
        if (!other.equals(selected)) {
          assertEquals(units.get(other), after.get(other));
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void nativeRewriteDoesNotRejectMalformedOrderedRanges(
      boolean partitioned, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, partitioned, true)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      List<ManifestEntry> files = fixture.units().values().iterator().next();
      ManifestEntry second = files.get(1);
      DataFileMeta first = files.get(0).file();
      DataFileMeta overlap =
          second
              .file()
              .assignSequenceNumber(
                  first.minSequenceNumber(),
                  first.minSequenceNumber() + second.file().rowCount() - 1);
      ManifestEntry malformed =
          ManifestEntry.create(
              second.kind(), second.partition(), second.bucket(), second.totalBuckets(), overlap);
      // Only the supplied metadata is corrupted; real input files and table state are untouched.
      // Native compactRewrite performs no ordered-range validation, so Amoro must do it.
      CommitMessageImpl result = fixture.rewrite(Arrays.asList(files.get(0), malformed));
      assertEquals(
          first.rowCount() + overlap.rowCount(),
          result.compactIncrement().compactAfter().stream()
              .mapToLong(DataFileMeta::rowCount)
              .sum());
      assertEquals(2, fixture.table.latestSnapshot().get().id());
    }
  }
}
