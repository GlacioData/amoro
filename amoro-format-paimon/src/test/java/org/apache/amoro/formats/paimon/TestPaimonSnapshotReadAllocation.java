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

package org.apache.amoro.formats.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.SnapshotDetailPage;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.avro.AvroRowDatumReader;
import org.apache.paimon.format.avro.SeekableInputStreamWrapper;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaSerializer;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.shade.org.apache.avro.file.DataFileReader;
import org.apache.paimon.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.paimon.shade.org.apache.avro.io.DatumReader;
import org.apache.paimon.shade.org.apache.avro.io.Decoder;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.UriReader;
import org.apache.paimon.utils.VersionedObjectSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Opt-in, same-process comparison of the old full detail materialization and bounded pages. */
@EnabledIfSystemProperty(named = "snapshot.benchmark", matches = "true")
class TestPaimonSnapshotReadAllocation {
  @TempDir Path warehouse;

  @Test
  void compareEqualAvroDatasetWithoutReaderProbes() throws Exception {
    System.out.println("SNAPSHOT_BENCH_MAX_HEAP_BYTES," + Runtime.getRuntime().maxMemory());
    Map<String, String> options = new HashMap<>();
    boolean suppliedCacheProfile =
        Boolean.parseBoolean(System.getProperties().getProperty("snapshot.supplied-cache-profile"));
    if (suppliedCacheProfile) {
      options.put("case-sensitive", "true");
      options.put("ams.uri", "thrift://127.0.0.1:1260");
      options.put("clients", "30");
      options.put("client-pool-size", "10");
      options.put("cache.manifest.small-file-threshold", "2mb");
      options.put("cache.snapshot.max-num-per-table", "10");
      options.put("database-filter", "^(?!test_spark$).*$");
      options.put("cache.expire-after-access", "30min");
      options.put("cache.manifest.max-memory", "512mb");
      options.put("cache.manifest.small-file-memory", "256mb");
    }
    // Keep all fixture IO local; the AMS URI is not used by the filesystem catalog.
    options.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    try (Catalog catalog = PaimonCatalogFactory.paimonCatalog(options, new Configuration())) {
      assertTrue(catalog instanceof CachingCatalog);
      CachingCatalog cachingCatalog = (CachingCatalog) catalog;
      System.out.println("SNAPSHOT_BENCH_SUPPLIED_CACHE_PROFILE," + suppliedCacheProfile);
      catalog.createDatabase("db", true);
      Identifier id = Identifier.create("db", "bench");
      catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
      FileStoreTable table = (FileStoreTable) catalog.getTable(id);
      FileStore<?> store = table.store();
      ManifestFile files = store.manifestFileFactory().create();
      ManifestList list = store.manifestListFactory().create();
      DataFileMeta data =
          DataFileMeta.forAppend(
              "same-file",
              128,
              1,
              SimpleStats.EMPTY_STATS,
              0,
              0,
              0,
              Collections.emptyList(),
              null,
              null,
              null,
              null,
              null,
              null);
      List<ManifestEntry> entries = new ArrayList<>();
      for (int i = 0; i < 100_000; i++) {
        entries.add(
            ManifestEntry.create(
                i % 2 == 0 ? FileKind.ADD : FileKind.DELETE, BinaryRow.EMPTY_ROW, 0, 1, data));
      }
      List<ManifestFileMeta> metas = files.write(entries);
      entries.clear();
      // Use one large manifest so the deep cursor is inside it, not behind unopened files.
      assertEquals(1, metas.size());
      org.apache.paimon.utils.Pair<String, Long> location = list.write(metas);
      Snapshot snapshot =
          new Snapshot(
              1,
              0,
              location.getLeft(),
              location.getRight(),
              location.getLeft(),
              location.getRight(),
              null,
              null,
              null,
              "fixture",
              1,
              Snapshot.CommitKind.APPEND,
              0,
              100_000,
              100_000,
              null,
              null,
              null,
              null,
              null);
      long cacheBefore = cachingCatalog.estimatedCacheSizes().manifestCacheBytes();
      String firstCursor = bounded(store, snapshot, null).getNextCursor();
      // Paimon 1.4.2 ObjectsFile.createIterator bypasses ObjectsFile.cache.read.
      // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/utils/ObjectsFile.java
      assertEquals(cacheBefore, cachingCatalog.estimatedCacheSizes().manifestCacheBytes());
      byte[] bytes = Base64.getUrlDecoder().decode(firstCursor);
      ByteBuffer.wrap(bytes).putLong(41, 90_000);
      String deepCursor = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
      Map<String, Supplier<List<PartitionFileBaseInfo>>> cases = new java.util.LinkedHashMap<>();
      cases.put("legacy-first", () -> legacy(store, snapshot, 0));
      cases.put("bounded-first", () -> bounded(store, snapshot, null).getList());
      cases.put("raw-prefix-first", () -> rawPrefix(store, snapshot, null).getList());
      cases.put("legacy-deep-90000", () -> legacy(store, snapshot, 90_000));
      cases.put("bounded-deep-90000", () -> bounded(store, snapshot, deepCursor).getList());
      cases.put("raw-prefix-deep-90000", () -> rawPrefix(store, snapshot, deepCursor).getList());
      cases.put("skip-prototype-first", () -> skipPrototype(table.fileIO(), store, snapshot, 0));
      cases.put(
          "skip-prototype-deep-90000",
          () -> skipPrototype(table.fileIO(), store, snapshot, 90_000));
      // Correctness comparison is outside the measured interval. This prototype only supports
      // the single-manifest fixture, not the production cursor or error-handling contract.
      for (int offset : new int[] {0, 1, 25, 90_000, 99_975}) {
        List<PartitionFileBaseInfo> expected = legacy(store, snapshot, offset);
        List<PartitionFileBaseInfo> actual = skipPrototype(table.fileIO(), store, snapshot, offset);
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
          assertEquals(expected.get(i).getOperation(), actual.get(i).getOperation());
          assertEquals(expected.get(i).getPath(), actual.get(i).getPath());
          assertEquals(expected.get(i).getFileSize(), actual.get(i).getFileSize());
        }
      }
      for (Supplier<List<PartitionFileBaseInfo>> read : cases.values()) {
        for (int warmup = 0; warmup < 3; warmup++) {
          assertEquals(25, read.get().size());
        }
      }
      long warmCacheBytes = cachingCatalog.estimatedCacheSizes().manifestCacheBytes();
      System.out.println(
          "SNAPSHOT_BENCH_MANIFEST_CACHE_BYTES," + cacheBefore + "," + warmCacheBytes);
      if (suppliedCacheProfile) {
        assertTrue(
            warmCacheBytes > cacheBefore, "The legacy warmup must actually populate the cache");
      }
      com.sun.management.ThreadMXBean threads =
          (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
      threads.setThreadAllocatedMemoryEnabled(true);
      long rawPrefixBytes = 0;
      long boundedBytes = 0;
      for (int iteration = 0; iteration < 3; iteration++) {
        for (Map.Entry<String, Supplier<List<PartitionFileBaseInfo>>> item : cases.entrySet()) {
          for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
              pool.resetPeakUsage();
            }
          }
          long gc = gcCount();
          long beforeBytes = threads.getThreadAllocatedBytes(Thread.currentThread().getId());
          long start = System.nanoTime();
          List<PartitionFileBaseInfo> page = item.getValue().get();
          long elapsed = System.nanoTime() - start;
          long allocated =
              threads.getThreadAllocatedBytes(Thread.currentThread().getId()) - beforeBytes;
          if (item.getKey().equals("raw-prefix-deep-90000")) {
            rawPrefixBytes += allocated;
          } else if (item.getKey().equals("bounded-deep-90000")) {
            boundedBytes += allocated;
          }
          long peak =
              ManagementFactory.getMemoryPoolMXBeans().stream()
                  .filter(pool -> pool.getType() == MemoryType.HEAP)
                  .mapToLong(pool -> pool.getPeakUsage().getUsed())
                  .sum();
          System.out.printf(
              "SNAPSHOT_BENCH,%s,%d,%d,%d,%d,%d%n",
              item.getKey(), iteration, elapsed / 1000, allocated, peak, gcCount() - gc);
          assertEquals(25, page.size());
        }
      }
      // Fixture-only allocation ratchet, not a production heap bound. Previous experiments showed
      // 30-36% of raw-prefix allocation; allow 60% to absorb JIT variation, but require a clear
      // win.
      assertTrue(
          boundedBytes < rawPrefixBytes * 0.60,
          "Avro prefix skipping must materially reduce deep-page allocations");
    }
  }

  private static SnapshotDetailPage bounded(FileStore<?> store, Snapshot snapshot, String cursor) {
    return new PaimonSnapshotDetailReader(new PaimonSnapshotReadDeadline())
        .read(store, snapshot, "bench", "main", 25, cursor, TestPaimonSnapshotReadAllocation::dto);
  }

  private static SnapshotDetailPage rawPrefix(
      FileStore<?> store, Snapshot snapshot, String cursor) {
    PaimonSnapshotReadDeadline deadline = new PaimonSnapshotReadDeadline();
    return new PaimonSnapshotDetailReader(deadline) {
      @Override
      PaimonSnapshotRows openRows(
          ObjectsFile<?> file,
          org.apache.paimon.fs.Path path,
          Long size,
          org.apache.paimon.types.RowType type,
          boolean avro)
          throws IOException {
        return PaimonSnapshotRows.open(file, path, size, type, false, deadline);
      }
    }.read(store, snapshot, "bench", "main", 25, cursor, TestPaimonSnapshotReadAllocation::dto);
  }

  /** Test-only experiment: retain logical offsets but avoid decoding prefix rows. */
  private static List<PartitionFileBaseInfo> skipPrototype(
      FileIO fileIO, FileStore<?> store, Snapshot snapshot, int offset) {
    PaimonSnapshotReadDeadline deadline = new PaimonSnapshotReadDeadline();
    return deadline.call(
        () -> {
          try (CloseableIterator<InternalRow> metas =
              store
                  .manifestListFactory()
                  .create()
                  .createIterator(
                      store.pathFactory().toManifestListPath(snapshot.deltaManifestList()),
                      snapshot.deltaManifestListSize())) {
            ManifestFileMeta meta = new ManifestFileMetaSerializer().fromRow(metas.next());
            PrefixSkippingDatumReader datum = new PrefixSkippingDatumReader(fileIO);
            // Follow Paimon 1.4.2 AvroBulkFormat, using the bundle's relocated Avro 1.11.4.
            // https://github.com/apache/paimon/blob/release-1.4.2/paimon-format/src/main/java/org/apache/paimon/format/avro/AvroBulkFormat.java
            try (SeekableInputStreamWrapper input =
                    new SeekableInputStreamWrapper(
                        fileIO.newInputStream(
                            store.pathFactory().toManifestFilePath(meta.fileName())),
                        meta.fileSize());
                DataFileReader<InternalRow> reader = new DataFileReader<>(input, datum)) {
              long remaining = offset;
              while (remaining > 0) {
                deadline.check();
                if (!reader.hasNext()) {
                  throw new IOException("Prefix ended before the fixture offset");
                }
                long count = reader.getBlockCount();
                if (count <= 0) {
                  throw new IOException("Invalid Avro block count");
                }
                if (remaining >= count) {
                  // nextBlock consumes a whole DECOMPRESSED block, not a physical seek.
                  // https://github.com/apache/avro/blob/release-1.11.4/lang/java/avro/src/main/java/org/apache/avro/file/DataFileStream.java
                  reader.nextBlock();
                  remaining -= count;
                } else {
                  datum.skip = true;
                  while (remaining > 0) {
                    deadline.check();
                    reader.next();
                    remaining--;
                  }
                  datum.skip = false;
                }
              }
              List<PartitionFileBaseInfo> result = new ArrayList<>(25);
              ManifestEntrySerializer serializer = new ManifestEntrySerializer();
              while (result.size() < 25 && reader.hasNext()) {
                deadline.check();
                result.add(dto(serializer.fromRow(reader.next())));
              }
              return result;
            }
          }
        });
  }

  private static final class PrefixSkippingDatumReader implements DatumReader<InternalRow> {
    private final AvroRowDatumReader delegate;
    private org.apache.paimon.shade.org.apache.avro.Schema schema;
    private boolean skip;

    private PrefixSkippingDatumReader(FileIO fileIO) {
      delegate =
          new AvroRowDatumReader(
              VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA),
              UriReader.fromFile(fileIO));
    }

    @Override
    public void setSchema(org.apache.paimon.shade.org.apache.avro.Schema schema) {
      this.schema = schema;
      delegate.setSchema(schema);
    }

    @Override
    public InternalRow read(InternalRow reuse, Decoder decoder) throws IOException {
      if (skip) {
        // Consume the writer schema without constructing InternalRow or file-stat graphs.
        // https://github.com/apache/avro/blob/release-1.11.4/lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumReader.java
        GenericDatumReader.skip(schema, decoder);
        return null;
      }
      return delegate.read(reuse, decoder);
    }
  }

  private static List<PartitionFileBaseInfo> legacy(
      FileStore<?> store, Snapshot snapshot, int offset) {
    List<PartitionFileBaseInfo> rows = new ArrayList<>();
    ManifestFile files = store.manifestFileFactory().create();
    for (ManifestFileMeta meta :
        store.manifestListFactory().create().readDeltaManifests(snapshot)) {
      for (ManifestEntry entry : files.read(meta.fileName())) {
        rows.add(dto(entry));
      }
    }
    return new ArrayList<>(rows.subList(offset, offset + 25));
  }

  private static PartitionFileBaseInfo dto(ManifestEntry entry) {
    return new PartitionFileBaseInfo(
        null,
        "BASE_FILE",
        0L,
        "bucket-0",
        entry.file().fileName(),
        entry.file().fileSize(),
        entry.kind().name());
  }

  private static long gcCount() {
    return ManagementFactory.getGarbageCollectorMXBeans().stream()
        .mapToLong(bean -> Math.max(0, bean.getCollectionCount()))
        .sum();
  }
}
