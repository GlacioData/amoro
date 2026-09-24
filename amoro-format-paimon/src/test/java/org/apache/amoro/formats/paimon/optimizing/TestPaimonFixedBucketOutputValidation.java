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
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TestPaimonFixedBucketOutputValidation {
  private static final String COMMIT_USER = "fixed-output-validation";
  private static final long COMMIT_IDENTIFIER = 301L;

  @ParameterizedTest
  @EnumSource(Tamper.class)
  void rejectsTamperedExecutorOutputBeforeCreatingNativeCommit(
      Tamper tamper, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      AppendOnlyFileStoreTable observedTable = spy(fixture.table);
      List<ManifestEntry> unit = fixture.units().get("0:0");
      List<ManifestEntry> selected = unit.subList(0, 2);
      DataFileMeta retained = unit.get(2).file();
      PaimonCompactionInput input = input(observedTable, selected);
      PaimonCompactionTask task = new PaimonCompactionTask(1L, "", input, Collections.emptyMap());
      PaimonCompactionOutput genuine = new PaimonCompactionExecutor(input).execute();
      CommitMessageImpl genuineMessage = message(genuine);
      DataFileMeta genuineAfter = genuineMessage.compactIncrement().compactAfter().get(0);

      assertTrue(
          files(warehouse).stream().anyMatch(file -> file.endsWith(genuineAfter.fileName())),
          "the executor output must remain physically present while validation rejects metadata");

      PaimonCompactionOutput tampered = tamper(genuine, genuineMessage, retained, tamper);
      CommitMessageImpl tamperedMessage = message(tampered);
      Set<String> physicalFiles = files(warehouse);
      assertTrue(
          tamperedMessage.compactIncrement().compactAfter().stream()
              .allMatch(
                  file ->
                      physicalFiles.stream()
                          .anyMatch(physical -> physical.endsWith(file.fileName()))),
          "tampering changes only declared metadata and keeps every output path present");
      setOutput(task, tampered);
      long snapshot = fixture.table.latestSnapshot().get().id();
      Set<String> referencedFiles = liveFileNames(fixture);

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
      assertEquals(snapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(referencedFiles, liveFileNames(fixture));
    }
  }

  private static PaimonFixedBucketTestSupport fixture(Path warehouse) throws Exception {
    PaimonFixedBucketTestSupport fixture = new PaimonFixedBucketTestSupport(warehouse, true, true);
    fixture.append(0, 1);
    fixture.append(1, 1);
    fixture.append(2, 1);
    return fixture;
  }

  private static PaimonCompactionInput input(
      AppendOnlyFileStoreTable table, List<ManifestEntry> selected) {
    PaimonTable wrapper = new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), table);
    PaimonFixedBucketTask fixed =
        PaimonFixedBucketTask.from(
            wrapper,
            selected,
            table.latestSnapshot().get().id(),
            OptimizingType.MINOR,
            PaimonAppendRewriteOptions.from(wrapper.properties()));
    return PaimonCompactionInput.forFixedBucket(wrapper, fixed, COMMIT_USER, "", COMMIT_IDENTIFIER);
  }

  private static PaimonCompactionOutput tamper(
      PaimonCompactionOutput genuine,
      CommitMessageImpl genuineMessage,
      DataFileMeta retained,
      Tamper tamper)
      throws Exception {
    List<DataFileMeta> before = genuineMessage.compactIncrement().compactBefore();
    List<DataFileMeta> afterFiles =
        new ArrayList<>(genuineMessage.compactIncrement().compactAfter());
    DataFileMeta after = afterFiles.get(0);
    DataFileMeta changedAfter = after;
    switch (tamper) {
      case AFTER_SCHEMA:
        changedAfter = copy(after, after.schemaId() + 1, after.rowCount(), FileSource.COMPACT);
        break;
      case AFTER_SOURCE:
        changedAfter = copy(after, after.schemaId(), after.rowCount(), FileSource.APPEND);
        break;
      case AFTER_ROW_COUNT:
        changedAfter = copy(after, after.schemaId(), after.rowCount() + 1, FileSource.COMPACT);
        break;
      case AFTER_SEQUENCE:
        changedAfter =
            after.assignSequenceNumber(
                after.minSequenceNumber() + 1, after.maxSequenceNumber() + 1);
        break;
      case AFTER_PATH:
        changedAfter = after.rename(retained.fileName());
        break;
      case EXTRA_ALIASES_RETAINED_DATA:
        // Paimon ac223f47, io/DataFileMeta.java:332-336 collects aligned extra paths too.
        changedAfter = after.copy(Collections.singletonList(retained.fileName()));
        break;
      case EXTRA_ALIASES_OUTPUT_DATA:
        changedAfter = after.copy(Collections.singletonList(after.fileName()));
        break;
      case DUPLICATE_EXTRA:
        changedAfter =
            after.copy(java.util.Arrays.asList(retained.fileName(), retained.fileName()));
        break;
      default:
        // Metric-only cases retain the genuine native message.
    }

    afterFiles.set(0, changedAfter);
    CommitMessageImpl changedMessage =
        new CommitMessageImpl(
            genuineMessage.partition(),
            genuineMessage.bucket(),
            genuineMessage.totalBuckets(),
            DataIncrement.emptyIncrement(),
            new CompactIncrement(before, afterFiles, Collections.emptyList()));
    CommitMessageSerializer serializer = new CommitMessageSerializer();
    long compactedFiles = genuine.getCompactedFileCount();
    long compactedBytes = genuine.getCompactedFileSize();
    long producedFiles = genuine.getProducedFileCount();
    long producedBytes = genuine.getProducedFileSize();
    long compactedRecords = genuine.getCompactedRecordCount();
    long producedRecords =
        tamper == Tamper.AFTER_ROW_COUNT
            ? afterFiles.stream().mapToLong(DataFileMeta::rowCount).sum()
            : genuine.getProducedRecordCount();
    switch (tamper) {
      case COMPACTED_FILE_COUNT:
        compactedFiles++;
        break;
      case COMPACTED_FILE_SIZE:
        compactedBytes++;
        break;
      case PRODUCED_FILE_COUNT:
        producedFiles++;
        break;
      case PRODUCED_FILE_SIZE:
        producedBytes++;
        break;
      case COMPACTED_RECORD_COUNT:
        compactedRecords++;
        break;
      case PRODUCED_RECORD_COUNT:
        producedRecords++;
        break;
      default:
        // Metadata-only cases preserve metrics, except row count remains internally consistent.
    }
    PaimonCompactionOutput result =
        new PaimonCompactionOutput(
            serializer.serialize(changedMessage),
            serializer.getVersion(),
            compactedFiles,
            compactedBytes,
            producedFiles,
            producedBytes);
    result.setRewriteRecords(compactedRecords, producedRecords);
    return result;
  }

  private static DataFileMeta copy(
      DataFileMeta file, long schemaId, long rowCount, FileSource source) {
    return DataFileMeta.create(
        file.fileName(),
        file.fileSize(),
        rowCount,
        file.minKey(),
        file.maxKey(),
        file.keyStats(),
        file.valueStats(),
        file.minSequenceNumber(),
        file.maxSequenceNumber(),
        schemaId,
        file.level(),
        file.extraFiles(),
        file.creationTime(),
        file.deleteRowCount().orElse(null),
        file.embeddedIndex(),
        source,
        file.valueStatsCols(),
        file.externalPath().orElse(null),
        file.firstRowId(),
        file.writeCols());
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

  private static Set<String> files(Path warehouse) throws Exception {
    try (Stream<Path> paths = Files.walk(warehouse)) {
      return paths
          .filter(Files::isRegularFile)
          .map(path -> warehouse.relativize(path).toString())
          .collect(Collectors.toSet());
    }
  }

  private enum Tamper {
    AFTER_SCHEMA,
    AFTER_SOURCE,
    AFTER_ROW_COUNT,
    AFTER_SEQUENCE,
    AFTER_PATH,
    EXTRA_ALIASES_RETAINED_DATA,
    EXTRA_ALIASES_OUTPUT_DATA,
    DUPLICATE_EXTRA,
    COMPACTED_FILE_COUNT,
    COMPACTED_FILE_SIZE,
    PRODUCED_FILE_COUNT,
    PRODUCED_FILE_SIZE,
    COMPACTED_RECORD_COUNT,
    PRODUCED_RECORD_COUNT
  }
}
