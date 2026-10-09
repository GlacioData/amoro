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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.DigestUtil;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;

import java.io.File;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TestPaimonFixedBucketTaskSerialization {
  @Test
  void rejectsNestedRowAndStatsBoundsBeforeNativeDecode(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      PaimonFixedBucketTaskSerializer serializer = new PaimonFixedBucketTaskSerializer();
      byte[] payload =
          serializer.serialize(task(fixture, fixture.units().values().iterator().next()));
      ByteBuffer cursor = ByteBuffer.wrap(payload);
      cursor.position(43);
      skipSegment(cursor); // table identity
      skipSegment(cursor); // table path
      skipSegment(cursor); // options fingerprint
      int partitionLength = cursor.position();
      int partitionArity = partitionLength + 4;
      skipSegment(cursor);
      skipSegment(cursor); // file name
      cursor.position(cursor.position() + 16); // file size and row count
      int minKeyLength = cursor.position();
      skipSegment(cursor);
      skipSegment(cursor); // max key
      skipSegment(cursor); // key stats minimum
      skipSegment(cursor); // key stats maximum
      int nullCount = cursor.position();
      // Source: Paimon ac223f47, utils/ObjectSerializer.java:100 and
      // utils/SerializationUtils.java:97. Native decode must not allocate from these raw counts.
      for (int offset : new int[] {partitionLength, partitionArity, minKeyLength, nullCount}) {
        byte[] damaged = payload.clone();
        ByteBuffer.wrap(damaged).putInt(offset, Integer.MAX_VALUE);
        reseal(damaged); // exercise structural checks independently of checksum detection
        assertThrows(
            IllegalArgumentException.class,
            () -> serializer.deserialize(serializer.getVersion(), damaged),
            "offset " + offset);
      }
    }
  }

  private static void skipSegment(ByteBuffer cursor) {
    int length = cursor.getInt();
    cursor.position(cursor.position() + length);
  }

  @Test
  void rejectsMismatchedOuterTableAndDisabledFrozenOptions(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, false)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      List<ManifestEntry> entries = fixture.units().values().iterator().next();
      PaimonFixedBucketTask valid = task(fixture, entries);
      PaimonTable wrongTable =
          new PaimonTable(TableIdentifier.of("test", "db", "another"), fixture.table);
      assertThrows(
          IllegalArgumentException.class,
          () -> PaimonCompactionInput.forFixedBucket(wrongTable, valid, "bound", "", 10));
      PaimonTable disabled =
          new PaimonTable(
              TableIdentifier.of("test", "db", "fixed_append"),
              fixture.table.copy(Collections.singletonMap("write-only", "false")));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PaimonFixedBucketTask.from(
                  disabled,
                  entries,
                  valid.snapshotId(),
                  OptimizingType.MINOR,
                  PaimonAppendRewriteOptions.from(disabled.properties())));
    }
  }

  @Test
  void legacyExecutorRejectsBeforeWritingInIsolatedJvm(@TempDir Path temporary) throws Exception {
    String baseline = "7fadd00db6c092b2b015f79049973611007a83b4";
    Path warehouse = temporary.resolve("warehouse");
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      PaimonTable table =
          new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), fixture.table);
      PaimonCompactionInput input =
          PaimonCompactionInput.forFixedBucket(
              table,
              task(fixture, fixture.units().values().iterator().next()),
              "legacy-probe",
              "",
              99);
      Path serialized = temporary.resolve("input.bin");
      ByteBuffer buffer = SerializationUtil.simpleSerialize(input);
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      Files.write(serialized, bytes);
      Path classes = Files.createDirectories(temporary.resolve("legacy-classes"));
      List<String> compilerArguments = new ArrayList<>();
      String classpath = System.getProperty("java.class.path");
      compilerArguments.add("-classpath");
      compilerArguments.add(classpath);
      compilerArguments.add("-d");
      compilerArguments.add(classes.toString());
      // Pin the original source bytes rather than requiring Git history in a source release.
      // Paimon ac223f47, table/sink/AppendCompactTaskSerializer.java:92 rejects version 1001.
      // Compile the real baseline classes, not replacements that mimic this rejection.
      for (String[] baselineSource :
          new String[][] {
            {
              "PaimonCompactionExecutor",
              "6a6e0a086f2bb697c8d3f5ce95b9c5daeb07bc3b9ab8ec23b68b436b57de826d"
            },
            {
              "PaimonCompactionInput",
              "29344607ca754b8d37cf1046b88a2b8f7c060eab01800d9b8dd17357d6825715"
            }
          }) {
        String name = baselineSource[0];
        Path source = temporary.resolve(name + ".java");
        String resource = "/paimon-fixed-bucket-legacy/" + baseline + "/" + name + ".java.fixture";
        try (InputStream sourceStream = getClass().getResourceAsStream(resource)) {
          assertNotNull(sourceStream, "Missing pinned legacy source: " + resource);
          Files.copy(sourceStream, source);
        }
        // Normalize checkout line endings only; all source content must match the pinned SHA.
        String sourceText =
            new String(Files.readAllBytes(source), StandardCharsets.UTF_8).replace("\r\n", "\n");
        assertEquals(baselineSource[1], DigestUtil.sha256Hex(sourceText), resource);
        compilerArguments.add(source.toString());
      }
      assertEquals(
          0,
          ToolProvider.getSystemJavaCompiler()
              .run(null, null, null, compilerArguments.toArray(new String[0])));
      List<String> before = warehouseFiles(warehouse);
      Path log = temporary.resolve("legacy-probe.log");
      List<String> childCommand = new ArrayList<>();
      childCommand.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
      // Match JaCoCo instrumentation: a transformed native interface has a different default UID.
      // The old Amoro classes remain isolated and their actual CodeSources are asserted in the
      // child.
      ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
          .filter(argument -> argument.startsWith("-javaagent:") && argument.contains("jacoco"))
          .forEach(childCommand::add);
      childCommand.add("-cp");
      childCommand.add(classes + File.pathSeparator + classpath);
      childCommand.add(PaimonLegacyTaskCompatibilityProbe.class.getName());
      childCommand.add(serialized.toString());
      childCommand.add(classes.toString());
      Process child =
          new ProcessBuilder(childCommand)
              .redirectErrorStream(true)
              .redirectOutput(log.toFile())
              .start();
      boolean finished = child.waitFor(30, TimeUnit.SECONDS);
      if (!finished) {
        child.destroyForcibly();
      }
      assertTrue(finished, "Legacy probe timeout");
      String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
      assertEquals(0, child.exitValue(), output);
      assertTrue(output.contains("LEGACY_REJECTED_BEFORE_WRITER"), output);
      assertEquals(before, warehouseFiles(warehouse));
      System.out.println("Legacy baseline=" + baseline + "\n" + output);
      System.out.println(
          "Paimon DataFileMeta schema digest="
              + DigestUtil.sha256Hex(DataFileMeta.SCHEMA.toString()));
    }
  }

  private static List<String> warehouseFiles(Path warehouse) throws Exception {
    try (Stream<Path> files = Files.walk(warehouse)) {
      return files
          .filter(Files::isRegularFile)
          .map(Path::toString)
          .sorted()
          .collect(Collectors.toList());
    }
  }

  @Test
  void roundTripKeepsExactManifestMetadata(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, false)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      List<ManifestEntry> files = fixture.units().values().iterator().next();
      PaimonFixedBucketTask task = task(fixture, files);
      PaimonFixedBucketTaskSerializer serializer = new PaimonFixedBucketTaskSerializer();
      PaimonFixedBucketTask restored =
          serializer.deserialize(serializer.getVersion(), serializer.serialize(task));
      assertEquals(task.partition(), restored.partition());
      assertEquals(task.bucket(), restored.bucket());
      assertEquals(task.totalBuckets(), restored.totalBuckets());
      assertEquals(task.snapshotId(), restored.snapshotId());
      assertEquals(task.schemaId(), restored.schemaId());
      assertEquals(task.tableId(), restored.tableId());
      assertEquals(task.tablePath(), restored.tablePath());
      assertEquals(task.rewriteFingerprint(), restored.rewriteFingerprint());
      assertEquals(task.files(), restored.files());
      assertEquals(OptimizingType.MINOR, restored.optimizingType());
      assertTrue(!restored.ordered());
    }
  }

  @Test
  void rejectsCrossBucketAndDuplicateInputs(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      List<List<ManifestEntry>> units = new ArrayList<>(fixture.units().values());
      List<ManifestEntry> mixed = new ArrayList<>(units.get(0));
      mixed.add(units.get(1).get(0));
      assertThrows(IllegalArgumentException.class, () -> task(fixture, mixed));
      List<ManifestEntry> duplicate = new ArrayList<>(units.get(0));
      duplicate.add(duplicate.get(0));
      assertThrows(IllegalArgumentException.class, () -> task(fixture, duplicate));
    }
  }

  @Test
  void rejectsUnknownVersionsAndBoundsBeforeNativeDecode(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, false, true)) {
      fixture.append(0, 1);
      PaimonFixedBucketTaskSerializer serializer = new PaimonFixedBucketTaskSerializer();
      byte[] bytes =
          serializer.serialize(task(fixture, fixture.units().values().iterator().next()));
      assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(-1, bytes));
      for (int offset : new int[] {0, 4, 8, 12, 16, 43}) {
        byte[] corrupted = bytes.clone();
        ByteBuffer.wrap(corrupted).putInt(offset, Integer.MAX_VALUE);
        reseal(corrupted);
        assertThrows(
            IllegalArgumentException.class,
            () -> serializer.deserialize(serializer.getVersion(), corrupted),
            "offset " + offset);
      }
      byte[] trailing = new byte[bytes.length + 1];
      System.arraycopy(bytes, 0, trailing, 0, bytes.length);
      reseal(trailing);
      assertThrows(
          IllegalArgumentException.class,
          () -> serializer.deserialize(serializer.getVersion(), trailing));
      assertThrows(
          IllegalArgumentException.class,
          () -> serializer.deserialize(serializer.getVersion(), new byte[1]));
      byte[] oversized = new byte[PaimonFixedBucketTaskSerializer.MAX_PAYLOAD_BYTES + 1];
      assertThrows(
          IllegalArgumentException.class,
          () -> serializer.deserialize(serializer.getVersion(), oversized));
      byte[] damaged = bytes.clone();
      damaged[damaged.length - 40] ^= 1;
      assertThrows(
          IllegalArgumentException.class,
          () -> serializer.deserialize(serializer.getVersion(), damaged));
    }
  }

  static PaimonFixedBucketTask task(
      PaimonFixedBucketTestSupport fixture, List<ManifestEntry> files) {
    PaimonTable table =
        new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), fixture.table);
    return PaimonFixedBucketTask.from(
        table,
        files,
        fixture.table.latestSnapshot().get().id(),
        OptimizingType.MINOR,
        PaimonAppendRewriteOptions.from(table.properties()));
  }

  private static void reseal(byte[] payload) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    digest.update(payload, 0, payload.length - 32);
    System.arraycopy(digest.digest(), 0, payload, payload.length - 32, 32);
  }
}
