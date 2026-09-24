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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class TestPaimonFixedBucketOutputPaths {
  private static final String COMMIT_USER = "fixed-output-paths";
  private static final long COMMIT_IDENTIFIER = 401L;

  // Paimon ac223f47, fs/ExternalPathProvider.java:37-62 and FileStorePathFactory.java:216-224:
  // placement may choose a root, but each strategy still fixes the layout within that root.
  @ParameterizedTest
  @ValueSource(strings = {"round-robin", "weight-robin", "entropy-inject", "specific-fs"})
  void acceptsNativeExternalPathAndRejectsSameNameUnderWrongRoot(
      String strategy, @TempDir Path warehouse) throws Exception {
    Path external = warehouse.resolve("external");
    Path wrongRoot = warehouse.resolve("wrong-root");
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(
            warehouse.resolve("table"), true, true, externalOptions(external, strategy))) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      Prepared prepared = prepare(fixture.table, fixture.units().get("0:0"));
      PaimonCompactionOutput output = new PaimonCompactionExecutor(prepared.input).execute();
      CommitMessageImpl nativeMessage = message(output);

      assertTrue(
          nativeMessage.compactIncrement().compactAfter().stream()
              .allMatch(file -> file.externalPath().isPresent()));
      assertTrue(
          nativeMessage.compactIncrement().compactAfter().stream()
              .allMatch(
                  file -> Files.exists(Paths.get(java.net.URI.create(file.externalPath().get())))));
      PaimonFixedBucketOutputValidator.validate(
          fixture.table, prepared.payload, output, nativeMessage);

      DataFileMeta nativeFile = nativeMessage.compactIncrement().compactAfter().get(0);
      org.apache.paimon.fs.Path wrongExternal =
          ExternalPathProvider.create(
                  externalPathStrategy(strategy),
                  Collections.singletonList(
                      new org.apache.paimon.fs.Path(wrongRoot.toUri().toString())),
                  fixture
                      .table
                      .store()
                      .pathFactory()
                      .relativeBucketPath(prepared.payload.partition(), prepared.payload.bucket()),
                  null)
              .getNextExternalDataPath(nativeFile.fileName());
      DataFileMeta wrongFile = copyWithExternalPath(nativeFile, wrongExternal.toString());
      CommitMessageImpl wrongMessage = replaceFirstAfter(nativeMessage, wrongFile);
      PaimonCompactionOutput wrongOutput = withMessage(output, wrongMessage);

      assertEquals(nativeFile.fileName(), wrongExternal.getName());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PaimonFixedBucketOutputValidator.validate(
                  fixture.table, prepared.payload, wrongOutput, wrongMessage));
    }
  }

  @Test
  void rejectsSourceSnapshotAliasAfterConcurrentCompactionRemovedItFromLatest(
      @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      fixture.append(2, 1);
      AppendOnlyFileStoreTable observedTable = spy(fixture.table);
      List<ManifestEntry> unit = fixture.units().get("0:0");
      List<ManifestEntry> selected = unit.subList(0, 2);
      ManifestEntry retained = unit.get(2);
      Prepared prepared = prepare(observedTable, selected);
      PaimonCompactionOutput output = new PaimonCompactionExecutor(prepared.input).execute();
      CommitMessageImpl nativeMessage = message(output);
      long sourceSnapshot = fixture.table.latestSnapshot().get().id();

      // Paimon ac223f47, BaseAppendFileStoreWrite.java:187-222 rewrites even a singleton.
      // Its old path remains in the source snapshot, although latest no longer references it.
      fixture.commit(fixture.rewrite(Collections.singletonList(retained)));
      assertTrue(
          fixture.units().get("0:0").stream()
              .noneMatch(entry -> entry.fileName().equals(retained.fileName())));

      DataFileMeta aliased =
          nativeMessage.compactIncrement().compactAfter().get(0).rename(retained.fileName());
      CommitMessageImpl aliasedMessage = replaceFirstAfter(nativeMessage, aliased);
      PaimonCompactionOutput aliasedOutput = withMessage(output, aliasedMessage);
      PaimonCompactionTask task =
          new PaimonCompactionTask(1L, "", prepared.input, Collections.emptyMap());
      setOutput(task, aliasedOutput);
      long latestSnapshot = fixture.table.latestSnapshot().get().id();
      Set<String> latestFiles = liveFileNames(fixture);

      assertTrue(latestSnapshot > sourceSnapshot);
      assertThrows(
          OptimizingCommitException.class,
          () ->
              new PaimonTableCommit(
                      observedTable,
                      Collections.singletonList(task),
                      COMMIT_USER,
                      COMMIT_IDENTIFIER)
                  .commit());
      verify(observedTable, never()).newCommit(anyString());
      assertEquals(latestSnapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(latestFiles, liveFileNames(fixture));
    }
  }

  private static Map<String, String> externalOptions(Path external, String strategy) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("data-file.external-paths", external.toUri().toString());
    options.put("data-file.external-paths.strategy", strategy);
    if ("weight-robin".equals(strategy)) {
      options.put("data-file.external-paths.weights", "1");
    } else if ("specific-fs".equals(strategy)) {
      options.put("data-file.external-paths.specific-fs", "file");
    }
    return options;
  }

  private static ExternalPathStrategy externalPathStrategy(String strategy) {
    switch (strategy) {
      case "round-robin":
        return ExternalPathStrategy.ROUND_ROBIN;
      case "weight-robin":
        return ExternalPathStrategy.WEIGHTED;
      case "entropy-inject":
        return ExternalPathStrategy.ENTROPY_INJECT;
      case "specific-fs":
        return ExternalPathStrategy.SPECIFIC_FS;
      default:
        throw new IllegalArgumentException("Unknown external path strategy " + strategy);
    }
  }

  private static Prepared prepare(AppendOnlyFileStoreTable table, List<ManifestEntry> selected) {
    PaimonTable wrapper = new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), table);
    PaimonFixedBucketTask payload =
        PaimonFixedBucketTask.from(
            wrapper,
            selected,
            table.latestSnapshot().get().id(),
            OptimizingType.MINOR,
            PaimonAppendRewriteOptions.from(wrapper.properties()));
    return new Prepared(
        payload,
        PaimonCompactionInput.forFixedBucket(wrapper, payload, COMMIT_USER, "", COMMIT_IDENTIFIER));
  }

  private static CommitMessageImpl replaceFirstAfter(
      CommitMessageImpl original, DataFileMeta replacement) {
    List<DataFileMeta> after =
        new java.util.ArrayList<>(original.compactIncrement().compactAfter());
    after.set(0, replacement);
    return new CommitMessageImpl(
        original.partition(),
        original.bucket(),
        original.totalBuckets(),
        DataIncrement.emptyIncrement(),
        new CompactIncrement(
            original.compactIncrement().compactBefore(), after, Collections.emptyList()));
  }

  private static DataFileMeta copyWithExternalPath(DataFileMeta file, String externalPath) {
    return DataFileMeta.create(
        file.fileName(),
        file.fileSize(),
        file.rowCount(),
        file.minKey(),
        file.maxKey(),
        file.keyStats(),
        file.valueStats(),
        file.minSequenceNumber(),
        file.maxSequenceNumber(),
        file.schemaId(),
        file.level(),
        file.extraFiles(),
        file.creationTime(),
        file.deleteRowCount().orElse(null),
        file.embeddedIndex(),
        file.fileSource().orElse(null),
        file.valueStatsCols(),
        externalPath,
        file.firstRowId(),
        file.writeCols());
  }

  private static PaimonCompactionOutput withMessage(
      PaimonCompactionOutput original, CommitMessageImpl message) throws Exception {
    CommitMessageSerializer serializer = new CommitMessageSerializer();
    PaimonCompactionOutput replacement =
        new PaimonCompactionOutput(
            serializer.serialize(message),
            serializer.getVersion(),
            original.getCompactedFileCount(),
            original.getCompactedFileSize(),
            original.getProducedFileCount(),
            original.getProducedFileSize());
    replacement.setRewriteRecords(
        original.getCompactedRecordCount(), original.getProducedRecordCount());
    return replacement;
  }

  private static CommitMessageImpl message(PaimonCompactionOutput output) throws Exception {
    return (CommitMessageImpl)
        new CommitMessageSerializer()
            .deserialize(output.getCommitMessageVersion(), output.getCommitMessageBytes());
  }

  private static void setOutput(PaimonCompactionTask task, PaimonCompactionOutput output) {
    ByteBuffer buffer = SerializationUtil.simpleSerialize(output);
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    task.setOutputBytes(bytes);
  }

  private static Set<String> liveFileNames(PaimonFixedBucketTestSupport fixture) {
    return fixture.units().values().stream()
        .flatMap(List::stream)
        .map(ManifestEntry::fileName)
        .collect(Collectors.toSet());
  }

  private static final class Prepared {
    private final PaimonFixedBucketTask payload;
    private final PaimonCompactionInput input;

    private Prepared(PaimonFixedBucketTask payload, PaimonCompactionInput input) {
      this.payload = payload;
      this.input = input;
    }
  }
}
