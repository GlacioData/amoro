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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.tag.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TestPaimonPrimaryKeyCommitMaintenance {
  @ParameterizedTest
  @CsvSource({
    "false,default",
    "false,true",
    "false,false",
    "true,default",
    "true,true",
    "true,false"
  })
  void isolatesCommitOptions(boolean dynamic, String original, @TempDir Path warehouse)
      throws Exception {
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, dynamic, Collections.emptyMap())) {
      FileStoreTable table =
          fixture
              .table()
              .copy(
                  Collections.singletonMap(
                      CoreOptions.WRITE_ONLY.key(), "default".equals(original) ? null : original));
      Map<String, String> before = new HashMap<>(table.options());
      FileStoreTable observed = spy(table);
      new PaimonPrimaryKeyTableCommit(observed, Collections.emptyList());
      ArgumentCaptor<Map<String, String>> overrides = ArgumentCaptor.forClass(Map.class);
      verify(observed).copy(overrides.capture());
      assertTrue(table.copy(overrides.getValue()).coreOptions().writeOnly());
      assertEquals(before, table.options());
      assertFalse(
          table
              .copy(PaimonPrimaryKeyOptions.compactionWriteOverrides(table.options()))
              .coreOptions()
              .writeOnly());
    }
  }

  @ParameterizedTest
  @CsvSource({"false,NORMAL", "true,NORMAL", "false,RECOVERY_REPLAY", "true,RECOVERY_REPLAY"})
  void preparedCompactionSkipsSnapshotExpiry(
      boolean dynamic, CommitMode mode, @TempDir Path warehouse) throws Exception {
    assertSnapshotExpiry(warehouse.resolve("candidate"), dynamic, mode, true);
    assertSnapshotExpiry(warehouse.resolve("control"), dynamic, mode, false);
  }

  @ParameterizedTest
  @CsvSource({
    "false,TAG_CREATE",
    "true,TAG_CREATE",
    "false,TAG_EXPIRE",
    "true,TAG_EXPIRE",
    "false,PARTITION",
    "true,PARTITION",
    "false,CONSUMER",
    "true,CONSUMER",
    "false,CHANGELOG",
    "true,CHANGELOG"
  })
  void preparedCompactionSkipsOtherMaintenance(
      boolean dynamic, String kind, @TempDir Path warehouse) throws Exception {
    assertMaintenance(warehouse.resolve("candidate"), dynamic, kind, true);
    assertMaintenance(warehouse.resolve("control"), dynamic, kind, false);
  }

  private void assertMaintenance(Path warehouse, boolean dynamic, String kind, boolean candidate)
      throws Exception {
    Map<String, String> options = new HashMap<>();
    switch (kind) {
      case "TAG_CREATE":
        options.put(CoreOptions.TAG_AUTOMATIC_CREATION.key(), "process-time");
        options.put(CoreOptions.TAG_CREATION_PERIOD.key(), "hourly");
        break;
      case "PARTITION":
        options.put(CoreOptions.PARTITION_EXPIRATION_TIME.key(), "1 d");
        options.put(CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL.key(), "0 s");
        options.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$dt");
        break;
      case "CONSUMER":
        options.put(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 s");
        break;
      case "CHANGELOG":
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "input");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        options.put(CoreOptions.CHANGELOG_NUM_RETAINED_MIN.key(), "1");
        options.put(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), "2");
        break;
      default:
        break;
    }
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, dynamic, options)) {
      for (int generation = 0; generation < 4; generation++) {
        fixture.writeGeneration(generation);
      }
      FileStoreTable original = fixture.table();
      List<String> expected = PaimonPrimaryKeyCommitTestSupport.rows(original);
      if ("TAG_EXPIRE".equals(kind)) {
        // A real native tag with a deterministic expired TTL, without sleeps.
        original
            .fileIO()
            .overwriteFileUtf8(
                original.tagManager().tagPath("expired"),
                Tag.fromSnapshotAndTagTtl(
                        original.snapshotManager().snapshot(1),
                        Duration.ofSeconds(1),
                        LocalDateTime.of(2000, 1, 1, 0, 0))
                    .toJson());
        assertTrue(original.tagManager().tagExists("expired"));
      } else if ("CONSUMER".equals(kind)) {
        original.consumerManager().resetConsumer("expired", new Consumer(Long.MAX_VALUE));
        Files.setLastModifiedTime(
            Paths.get(
                new org.apache.paimon.fs.Path(original.location(), "consumer/consumer-expired")
                    .toUri()),
            FileTime.from(Instant.parse("2000-01-01T00:00:00Z")));
        assertTrue(original.consumerManager().consumer("expired").isPresent());
      } else if ("CHANGELOG".equals(kind)) {
        // Separate lifecycle: snapshot expiration first converts snapshots to retained changelogs.
        original
            .newExpireSnapshots()
            .config(ExpireConfig.builder().snapshotRetainMax(1).changelogRetainMax(10).build())
            .expire();
        assertTrue(original.changelogManager().longLivedChangelogExists(1));
      } else if ("TAG_CREATE".equals(kind)) {
        assertTrue(original.tagManager().allTagNames().isEmpty());
      }
      List<PaimonPrimaryKeyCompactionTask> tasks = fixture.prepareTasks();
      FileIO io = spy(original.fileIO());
      FileStoreTable observed =
          FileStoreTableFactory.create(io, original.location(), original.schema());
      Mockito.clearInvocations(io);
      if (candidate) {
        new PaimonPrimaryKeyTableCommit(observed, tasks).commit();
      } else {
        PaimonPrimaryKeyCommitTestSupport.nativeCommit(
            observed.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false")),
            tasks,
            CommitMode.NORMAL);
      }
      FileStoreTable after = fixture.table();
      String deletedPath = null;
      switch (kind) {
        case "TAG_CREATE":
          assertEquals(candidate, after.tagManager().allTagNames().isEmpty());
          boolean tagWrite =
              Mockito.mockingDetails(io).getInvocations().stream()
                  .anyMatch(
                      call ->
                          call.getMethod().getName().equals("newOutputStream")
                              && String.valueOf(call.getArguments()[0]).contains("/tag/tag-"));
          assertEquals(!candidate, tagWrite);
          break;
        case "TAG_EXPIRE":
          assertEquals(candidate, after.tagManager().tagExists("expired"));
          deletedPath = "/tag/tag-expired";
          break;
        case "CONSUMER":
          assertEquals(candidate, after.consumerManager().consumer("expired").isPresent());
          deletedPath = "/consumer/consumer-expired";
          break;
        case "CHANGELOG":
          assertEquals(candidate, after.changelogManager().longLivedChangelogExists(1));
          deletedPath = "/changelog/changelog-1";
          break;
        case "PARTITION":
          assertEquals(
              candidate ? expected : Collections.emptyList(),
              PaimonPrimaryKeyCommitTestSupport.rows(after));
          assertEquals(
              candidate ? Snapshot.CommitKind.COMPACT : Snapshot.CommitKind.OVERWRITE,
              after.snapshotManager().latestSnapshot().commitKind());
          break;
        default:
          throw new AssertionError(kind);
      }
      if (deletedPath != null) {
        String path = deletedPath;
        assertEquals(
            !candidate,
            Mockito.mockingDetails(io).getInvocations().stream()
                .anyMatch(
                    call ->
                        call.getMethod().getName().startsWith("delete")
                            && call.getArguments().length > 0
                            && String.valueOf(call.getArguments()[0]).contains(path)));
      }
      if (!"PARTITION".equals(kind)) {
        assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(after));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mixedMessagesKeepNativeAppendAndCompactSnapshots(boolean dynamic, @TempDir Path warehouse)
      throws Exception {
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, dynamic, Collections.emptyMap())) {
      fixture.writeGeneration(0);
      fixture.writeGeneration(1);
      List<PaimonPrimaryKeyCompactionTask> tasks = fixture.prepareTasks();
      List<String> expected =
          new ArrayList<>(PaimonPrimaryKeyCommitTestSupport.rows(fixture.table()));
      List<byte[]> messages = new ArrayList<>(tasks.get(0).getOutput().getCommitMessageBytesList());
      try (BatchTableWrite writer = fixture.table().newBatchWriteBuilder().newWrite()) {
        GenericRow row =
            GenericRow.of(
                99,
                BinaryString.fromString("new"),
                BinaryString.fromString(PaimonPrimaryKeyCommitTestSupport.PARTITION));
        if (dynamic) {
          writer.write(row, 1);
        } else {
          writer.write(row);
        }
        messages.addAll(CommitMessageSerializer.serializeAll(writer.prepareCommit()));
      }
      tasks.get(0).setOutput(new PaimonPrimaryKeyCompactionOutput(messages, 0, 0, 0, 0, 0, 0));
      new PaimonPrimaryKeyTableCommit(fixture.table(), tasks).commit();
      expected.add("99|new|" + PaimonPrimaryKeyCommitTestSupport.PARTITION);
      Collections.sort(expected);
      assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(fixture.table()));
      assertEquals(
          Snapshot.CommitKind.APPEND, fixture.table().snapshotManager().snapshot(3).commitKind());
      assertEquals(
          Snapshot.CommitKind.COMPACT, fixture.table().snapshotManager().snapshot(4).commitKind());
      for (long id = 3; id <= 4; id++) {
        assertEquals(
            tasks.get(0).getInput().getCommitUser(),
            fixture.table().snapshotManager().snapshot(id).commitUser());
        assertEquals(
            tasks.get(0).getInput().getCommitIdentifier(),
            fixture.table().snapshotManager().snapshot(id).commitIdentifier());
      }
      new PaimonPrimaryKeyTableCommit(fixture.table(), tasks).commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(4L, fixture.table().snapshotManager().latestSnapshotId());
      assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(fixture.table()));
    }
  }

  private void assertSnapshotExpiry(
      Path warehouse, boolean dynamic, CommitMode mode, boolean candidate) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
    options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, dynamic, options)) {
      fixture.writeGeneration(0);
      fixture.writeGeneration(1);
      fixture.writeGeneration(2);
      List<String> expected = PaimonPrimaryKeyCommitTestSupport.rows(fixture.table());
      List<PaimonPrimaryKeyCompactionTask> tasks = fixture.prepareTasks();
      FileStoreTable original = fixture.table();
      assertTrue(original.snapshotManager().snapshotExists(1));
      FileIO io = spy(original.fileIO());
      FileStoreTable observed =
          FileStoreTableFactory.create(io, original.location(), original.schema());
      Mockito.clearInvocations(io);
      if (candidate) {
        new PaimonPrimaryKeyTableCommit(observed, tasks).commit(mode);
      } else {
        PaimonPrimaryKeyCommitTestSupport.nativeCommit(
            observed.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false")),
            tasks,
            mode);
      }
      boolean deletedSnapshot =
          Mockito.mockingDetails(io).getInvocations().stream()
              .anyMatch(
                  call ->
                      call.getMethod().getName().startsWith("delete")
                          && call.getArguments().length > 0
                          && String.valueOf(call.getArguments()[0])
                              .contains("/snapshot/snapshot-1"));
      assertEquals(!candidate, deletedSnapshot);
      FileStoreTable after = fixture.table();
      assertEquals(candidate, after.snapshotManager().snapshotExists(1));
      Snapshot latest = after.snapshotManager().latestSnapshot();
      assertNotNull(latest);
      assertEquals(Snapshot.CommitKind.COMPACT, latest.commitKind());
      assertEquals(tasks.get(0).getInput().getCommitUser(), latest.commitUser());
      assertEquals(tasks.get(0).getInput().getCommitIdentifier(), latest.commitIdentifier());
      assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(after));
    }
  }
}
