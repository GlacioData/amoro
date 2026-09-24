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

import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/** Real fixed-bucket files shared by native-contract and later Amoro adaptation tests. */
final class PaimonFixedBucketTestSupport implements AutoCloseable {
  final Catalog catalog;
  final Identifier identifier = Identifier.create("db", "fixed_append");
  final AppendOnlyFileStoreTable table;
  private final boolean partitioned;

  PaimonFixedBucketTestSupport(Path warehouse, boolean partitioned, boolean ordered)
      throws Exception {
    this(warehouse, partitioned, ordered, Collections.emptyMap());
  }

  PaimonFixedBucketTestSupport(
      Path warehouse, boolean partitioned, boolean ordered, Map<String, String> options)
      throws Exception {
    this.partitioned = partitioned;
    catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration());
    catalog.createDatabase("db", true);
    Schema.Builder schema =
        Schema.newBuilder()
            .column("dt", DataTypes.INT())
            .column("hm", DataTypes.STRING().notNull())
            .column("reserve7", DataTypes.INT())
            .column("seq", DataTypes.INT())
            .column("payload", DataTypes.STRING())
            .option("bucket", "2")
            .option("bucket-key", "hm,reserve7")
            .option("bucket-append-ordered", String.valueOf(ordered))
            .option("write-only", "true")
            .option("self-optimizing.enabled", "true")
            .option("target-file-size", "64 mb")
            .option("file-index.range-bitmap.columns", "hm");
    options.forEach(schema::option);
    if (partitioned) {
      schema.partitionKeys("dt");
    }
    catalog.createTable(identifier, schema.build(), false);
    table = (AppendOnlyFileStoreTable) catalog.getTable(identifier);
  }

  void append(int firstRound, int rounds) throws Exception {
    // Source: https://paimon.apache.org/docs/1.4/program-api/java-api/#batch-write
    // Two equal rows per value make a multiset assertion detect accidental deduplication.
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    Random random = new Random(firstRound);
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      for (int dt = 0; dt < (partitioned ? 2 : 1); dt++) {
        for (int round = firstRound; round < firstRound + rounds; round++) {
          for (int key = 0; key < 16; key++) {
            StringBuilder payload = new StringBuilder();
            for (int i = 0; i < 16; i++) {
              payload.append(Long.toHexString(random.nextLong()));
            }
            GenericRow row =
                GenericRow.of(
                    dt,
                    BinaryString.fromString(String.format("%04d", key)),
                    key % 3,
                    round * 16 + key,
                    BinaryString.fromString(payload.toString()));
            write.write(row);
            write.write(row);
          }
        }
      }
      commit.commit(write.prepareCommit());
    }
  }

  Map<String, List<ManifestEntry>> units() {
    Map<String, List<ManifestEntry>> result = new LinkedHashMap<>();
    // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
    // paimon-core/src/main/java/org/apache/paimon/manifest/FileEntry.java:49
    // Manifest metadata, not directory parsing, supplies the real partition and bucket.
    for (ManifestEntry entry : table.store().newScan().plan().files()) {
      String key = (partitioned ? entry.partition().getInt(0) : 0) + ":" + entry.bucket();
      result.computeIfAbsent(key, ignored -> new ArrayList<>()).add(entry);
    }
    result
        .values()
        .forEach(files -> files.sort(Comparator.comparingLong(f -> f.file().minSequenceNumber())));
    return result;
  }

  CommitMessageImpl rewrite(List<ManifestEntry> inputs) throws Exception {
    ManifestEntry first = inputs.get(0);
    List<DataFileMeta> before =
        inputs.stream().map(ManifestEntry::file).collect(Collectors.toList());
    List<DataFileMeta> after;
    // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
    // paimon-core/src/main/java/org/apache/paimon/operation/BaseAppendFileStoreWrite.java:187
    // compactRewrite preserves supplied order and starts at the FIRST input sequence.
    // This raw native probe deliberately does not add the future Amoro range guard.
    BaseAppendFileStoreWrite write = table.store().newWrite("fixed-native-probe");
    try {
      after = write.compactRewrite(first.partition(), first.bucket(), null, before);
    } finally {
      write.close();
    }
    // Paimon reference: same SHA, paimon-core/src/main/java/org/apache/paimon/
    // table/sink/CommitMessageImpl.java:51. Never use the unaware task's synthetic bucket 0.
    return new CommitMessageImpl(
        first.partition(),
        first.bucket(),
        first.totalBuckets(),
        DataIncrement.emptyIncrement(),
        new CompactIncrement(before, after, Collections.emptyList()));
  }

  void commit(CommitMessageImpl message) throws Exception {
    try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
      commit.commit(Collections.singletonList(message));
    }
  }

  List<String> rows(ManifestEntry unit, List<DataFileMeta> files) throws Exception {
    DataSplit split =
        DataSplit.builder()
            .withPartition(unit.partition())
            .withBucket(unit.bucket())
            .withTotalBuckets(unit.totalBuckets())
            .withBucketPath(
                table.store().pathFactory().bucketPath(unit.partition(), unit.bucket()).toString())
            .withDataFiles(files)
            .build();
    List<String> result = new ArrayList<>();
    // Source: https://paimon.apache.org/docs/1.4/program-api/java-api/#batch-read
    try (RecordReader<InternalRow> reader = table.newReadBuilder().newRead().createReader(split)) {
      reader.forEachRemaining(
          row ->
              result.add(
                  row.getInt(0)
                      + "|"
                      + row.getString(1)
                      + "|"
                      + row.getInt(2)
                      + "|"
                      + row.getInt(3)
                      + "|"
                      + row.getString(4)));
    }
    return result;
  }

  Map<String, List<String>> rowsByUnit() throws Exception {
    Map<String, List<String>> result = new LinkedHashMap<>();
    for (Map.Entry<String, List<ManifestEntry>> entry : units().entrySet()) {
      List<ManifestEntry> files = entry.getValue();
      result.put(
          entry.getKey(),
          rows(files.get(0), files.stream().map(ManifestEntry::file).collect(Collectors.toList())));
    }
    return result;
  }

  List<String> snapshotRows() throws Exception {
    List<String> result = new ArrayList<>();
    ReadBuilder read = table.newReadBuilder();
    // Use the public snapshot scan too; direct file reads alone cannot verify publication.
    try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
      reader.forEachRemaining(
          row ->
              result.add(
                  row.getInt(0)
                      + "|"
                      + row.getString(1)
                      + "|"
                      + row.getInt(2)
                      + "|"
                      + row.getInt(3)
                      + "|"
                      + row.getString(4)));
    }
    Collections.sort(result);
    return result;
  }

  @Override
  public void close() throws Exception {
    catalog.close();
  }
}
