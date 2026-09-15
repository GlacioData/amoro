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

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.append;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies catalog cache behavior with independently deserialized tasks and actual data manifest
 * opens.
 */
class TestPaimonPrimaryKeyManifestCache {

  @ParameterizedTest
  @CsvSource({
    "true,128 mb,1 mb,,2",
    "false,128 mb,1 mb,,4",
    "true,0 b,1 mb,,4",
    "true,1 mb,0 b,512 kb,4",
    "true,1 mb,0 b,1 mb,4",
    "true,1 mb,0 b,2 mb,2",
    "true,0 b,0 b,1 mb,2",
    "true,1 b,1 mb,,4"
  })
  void catalogOptionsControlRealReads(
      boolean enabled,
      String budget,
      String threshold,
      String maximum,
      int expected,
      @TempDir Path dir)
      throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      Map<String, String> props = new HashMap<>();
      props.put("cache-enabled", String.valueOf(enabled));
      props.put("cache.manifest.small-file-memory", budget);
      props.put("cache.manifest.small-file-threshold", threshold);
      if (maximum != null) {
        props.put("cache.manifest.max-memory", maximum);
      }
      RestoreFiles first = restore(fixture.taskTable(props), 0);
      RestoreFiles second = restore(fixture.taskTable(props), 0);
      assertRestoreEquals(first, second);
      assertEquals(expected, fixture.counter().opens.get());
    }
  }

  @Test
  void rawCatalogOptionsSurviveSerializationAndRemainReadOnly(@TempDir Path dir) throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      Map<String, String> props = Collections.singletonMap("cache-enabled", "false");
      PaimonTable table = fixture.taskTable(props);
      assertEquals(props, table.catalogProperties());
      assertFalse(table.properties().containsKey("cache-enabled"));
      assertThrows(
          UnsupportedOperationException.class,
          () -> table.catalogProperties().put("cache-enabled", "true"));
      assertNull(((FileStoreTable) table.originalTable()).getManifestCache());
    }
  }

  @Test
  void nativeDefaultsAndThresholdBoundary(@TempDir Path dir) throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      restore(fixture.taskTable(Collections.emptyMap()), 0);
      restore(fixture.taskTable(Collections.emptyMap()), 0);
      assertEquals(2, fixture.counter().opens.getAndSet(0));
      long maximumSize =
          fixture.manifests().stream().mapToLong(ManifestFileMeta::fileSize).max().getAsLong();
      Map<String, String> props = new HashMap<>();
      props.put("cache.manifest.small-file-threshold", maximumSize + " b");
      restore(fixture.taskTable(props), 0);
      restore(fixture.taskTable(props), 0);
      assertEquals(2, fixture.counter().opens.getAndSet(0));
      props.put("cache.manifest.small-file-threshold", (maximumSize - 1) + " b");
      restore(fixture.taskTable(props), 0);
      restore(fixture.taskTable(props), 0);
      assertTrue(fixture.counter().opens.get() > 2);
    }
  }

  @Test
  void independentBucketExecutorsReduceThirtyTwoReadsToTwo(@TempDir Path dir) throws Exception {
    try (Fixture fixture = new Fixture(dir, 16)) {
      List<PaimonPrimaryKeyCompactionTask> baseline = executeBuckets(fixture, false);
      assertEquals(32, fixture.counter().opens.getAndSet(0));
      List<PaimonPrimaryKeyCompactionTask> cached = executeBuckets(fixture, true);
      assertEquals(2, fixture.counter().opens.getAndSet(0));
      assertEquals(16, baseline.size());
      assertEquals(16, cached.size());
      for (int i = 0; i < 16; i++) {
        PaimonPrimaryKeyCompactionOutput before = baseline.get(i).getOutput();
        PaimonPrimaryKeyCompactionOutput after = cached.get(i).getOutput();
        assertEquals(before.getCompactedFileCount(), after.getCompactedFileCount());
        assertEquals(before.getCompactedRecordCount(), after.getCompactedRecordCount());
        assertEquals(before.getProducedRecordCount(), after.getProducedRecordCount());
      }
      List<String> expected = rows(fixture.raw);
      new PaimonPrimaryKeyTableCommit(fixture.raw, cached).commit();
      assertEquals(expected, rows(fixture.raw));
      System.out.println("F5_EXECUTOR_IO buckets=16 uncached=32 cached=2 rows=" + expected.size());
    }
  }

  @Test
  void busyLoaderDoesNotBlockOrPopulateButCanReadWarmCache(@TempDir Path dir) throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      PaimonTable wrapper = fixture.taskTable(Collections.emptyMap());
      FileStoreTable table = (FileStoreTable) wrapper.originalTable();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try (PaimonPrimaryKeyManifestCache.ScanLease loading =
          PaimonPrimaryKeyManifestCache.acquire(wrapper, table)) {
        assertEquals(Integer.valueOf(1), loading.scan().parallelism());
        Future<RestoreFiles> cold =
            executor.submit(() -> restore(fixture.taskTable(Collections.emptyMap()), 0));
        cold.get(10, TimeUnit.SECONDS);
        assertEquals(2, fixture.counter().opens.getAndSet(0));
        prepared(table, loading, 0);
        assertEquals(
            2,
            fixture.counter().opens.getAndSet(0),
            "Read-only misses must not populate the cache");
        Future<RestoreFiles> hot =
            executor.submit(() -> restore(fixture.taskTable(Collections.emptyMap()), 0));
        assertRestoreEquals(cold.get(), hot.get(10, TimeUnit.SECONDS));
        assertEquals(0, fixture.counter().opens.get());
      } finally {
        executor.shutdownNow();
      }
      assertNull(table.getManifestCache());
      assertEquals("5", table.options().get("scan.manifest.parallelism"));
    }
  }

  @Test
  void failureReleasesLoaderAndDoesNotReturnPartialRestore(@TempDir Path dir) throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      fixture.counter().fail = true;
      assertThrows(
          RuntimeException.class, () -> restore(fixture.taskTable(Collections.emptyMap()), 0));
      fixture.counter().fail = false;
      fixture.counter().opens.set(0);
      restore(fixture.taskTable(Collections.emptyMap()), 0);
      restore(fixture.taskTable(Collections.emptyMap()), 0);
      assertEquals(2, fixture.counter().opens.get());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void workerOutlivingFailureOrInterruptionKeepsLoaderPermit(boolean interrupted, @TempDir Path dir)
      throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      List<DataFileMeta> files = new ArrayList<>();
      fixture.raw.store().newScan().plan().files().forEach(entry -> files.add(entry.file()));
      List<ManifestFileMeta> deletes = new ArrayList<>();
      for (DataFileMeta file : files) {
        deletes.addAll(
            fixture
                .raw
                .store()
                .manifestFileFactory()
                .create()
                .write(
                    Collections.singletonList(
                        ManifestEntry.create(FileKind.DELETE, BinaryRow.EMPTY_ROW, 0, 1, file))));
      }
      assertEquals(2, deletes.size());
      Pair<String, Long> base = fixture.raw.store().manifestListFactory().create().write(deletes);
      Pair<String, Long> delta =
          fixture.raw.store().manifestListFactory().create().write(Collections.emptyList());
      // Build scan inputs without publishing the failing snapshot; use native discovery, DELETE
      // reads and pools.
      Snapshot snapshot =
          new Snapshot(
              2,
              fixture.raw.schema().id(),
              base.getLeft(),
              base.getRight(),
              delta.getLeft(),
              delta.getRight(),
              null,
              null,
              null,
              "failure",
              1,
              Snapshot.CommitKind.COMPACT,
              System.currentTimeMillis(),
              0,
              0,
              null,
              null,
              null,
              null,
              null);
      Counter counter = fixture.counter();
      counter.failName = interrupted ? null : deletes.get(0).fileName();
      counter.blockName = deletes.get(interrupted ? 0 : 1).fileName();
      PaimonTable wrapper = fixture.taskTable(Collections.emptyMap());
      FileStoreTable table = (FileStoreTable) wrapper.originalTable();
      ExecutorService caller = Executors.newSingleThreadExecutor();
      PaimonPrimaryKeyManifestCache.ScanLease lease =
          PaimonPrimaryKeyManifestCache.acquire(wrapper, table);
      try {
        Future<?> read = caller.submit(() -> lease.readFiles(snapshot, BinaryRow.EMPTY_ROW, 0));
        assertTrue(counter.entered.await(10, TimeUnit.SECONDS));
        if (interrupted) {
          assertTrue(read.cancel(true));
        } else {
          assertThrows(ExecutionException.class, () -> read.get(10, TimeUnit.SECONDS));
        }
        lease.close();
        lease.close();
        try (PaimonPrimaryKeyManifestCache.ScanLease other =
            PaimonPrimaryKeyManifestCache.acquire(wrapper, table)) {
          assertEquals(
              Integer.valueOf(5),
              other.scan().parallelism(),
              "The loading permit must remain held while a native worker is still loading");
        }
        counter.release.countDown();
        boolean acquired = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
          try (PaimonPrimaryKeyManifestCache.ScanLease other =
              PaimonPrimaryKeyManifestCache.acquire(wrapper, table)) {
            if (Integer.valueOf(1).equals(other.scan().parallelism())) {
              acquired = true;
              break;
            }
          }
          Thread.sleep(5);
        }
        assertTrue(acquired, "The permit must be returned after loading finishes");
      } finally {
        lease.close();
        counter.release.countDown();
        caller.shutdownNow();
        assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void cacheContextSeparatesCatalogConfigurationAndFileIO(@TempDir Path dir) throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      restore(fixture.taskTable(Collections.emptyMap()), 0);
      restore(fixture.taskTable(Collections.emptyMap()), 0);
      assertEquals(2, fixture.counter().opens.getAndSet(0));
      PaimonTable differentCatalog =
          new PaimonTable(TableIdentifier.of("other", "db", "t"), fixture.raw);
      restore(roundTrip(differentCatalog), 0);
      assertEquals(2, fixture.counter().opens.getAndSet(0));
      restore(fixture.taskTable(Collections.singletonMap("catalog-marker", "new-context")), 0);
      assertEquals(2, fixture.counter().opens.getAndSet(0));
      CountingFileIO differentIO = new CountingFileIO();
      FileStoreTable other =
          new PrimaryKeyFileStoreTable(
              differentIO,
              fixture.raw.location(),
              fixture.raw.schema(),
              CatalogEnvironment.empty());
      restore(roundTrip(new PaimonTable(TableIdentifier.of("test", "db", "t"), other)), 0);
      assertEquals(2, differentIO.counter().opens.get());
    }
  }

  @Test
  void nativeExistingCacheAndInputOptionsAreUntouched(@TempDir Path dir) throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      SegmentsCache<org.apache.paimon.fs.Path> existing =
          SegmentsCache.create(MemorySize.ofMebiBytes(2), 1_000_000);
      fixture.raw.setManifestCache(existing);
      PaimonTable wrapper = new PaimonTable(TableIdentifier.of("test", "db", "t"), fixture.raw);
      try (PaimonPrimaryKeyManifestCache.ScanLease lease =
          PaimonPrimaryKeyManifestCache.acquire(wrapper, fixture.raw)) {
        assertEquals(Integer.valueOf(5), lease.scan().parallelism());
        prepared(fixture.raw, lease, 0);
      }
      assertSame(existing, fixture.raw.getManifestCache());
      assertEquals("5", fixture.raw.options().get("scan.manifest.parallelism"));
    }
  }

  @Test
  void warmManifestDoesNotPinExecutionSnapshot(@TempDir Path dir) throws Exception {
    try (Fixture fixture = new Fixture(dir, 1)) {
      PaimonTable oldInput = fixture.taskTable(Collections.emptyMap());
      long planned = fixture.raw.latestSnapshot().get().id();
      RestoreFiles oldFiles = restore(oldInput, 0);
      append(fixture.raw, GenericRow.of(777, 3, 4, 5L));
      FileStoreTable oldTable = (FileStoreTable) oldInput.originalTable();
      try (PaimonPrimaryKeyManifestCache.ScanLease lease =
          PaimonPrimaryKeyManifestCache.acquire(oldInput, oldTable)) {
        PaimonPrimaryKeySnapshotWriteRestore restored =
            new PaimonPrimaryKeySnapshotWriteRestore(
                oldTable, planned, Collections.singletonList(unit(0)), lease);
        RestoreFiles result = restored.restoreFiles(BinaryRow.EMPTY_ROW, 0, false, false);
        assertEquals(planned + 1, result.snapshot().id());
        assertEquals(oldFiles.dataFiles().size() + 1, result.dataFiles().size());
      }
    }
  }

  private static List<PaimonPrimaryKeyCompactionTask> executeBuckets(
      Fixture fixture, boolean enabled) {
    List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>();
    for (int bucket = 0; bucket < 16; bucket++) {
      PaimonPrimaryKeyCompactionInput input =
          roundTrip(
              new PaimonPrimaryKeyCompactionInput(
                  fixture.taskTable(
                      Collections.singletonMap("cache-enabled", String.valueOf(enabled))),
                  Collections.singletonList(unit(bucket)),
                  OptimizingType.MINOR,
                  false,
                  fixture.raw.latestSnapshot().get().id(),
                  "f5-cache",
                  1));
      PaimonPrimaryKeyCompactionExecutorFactory factory =
          new PaimonPrimaryKeyCompactionExecutorFactory();
      factory.initialize(Collections.emptyMap());
      PaimonPrimaryKeyCompactionOutput output = factory.createExecutor(input).execute();
      assertNull(((FileStoreTable) input.getTable().originalTable()).getManifestCache());
      PaimonPrimaryKeyCompactionTask task =
          new PaimonPrimaryKeyCompactionTask(1, "bucket=" + bucket, input, Collections.emptyMap());
      task.setOutput(output);
      tasks.add(task);
    }
    return tasks;
  }

  private static RestoreFiles restore(PaimonTable wrapper, int bucket) {
    FileStoreTable table = (FileStoreTable) wrapper.originalTable();
    try (PaimonPrimaryKeyManifestCache.ScanLease lease =
        PaimonPrimaryKeyManifestCache.acquire(wrapper, table)) {
      return prepared(table, lease, bucket);
    }
  }

  private static RestoreFiles prepared(
      FileStoreTable table, PaimonPrimaryKeyManifestCache.ScanLease lease, int bucket) {
    return new PaimonPrimaryKeySnapshotWriteRestore(
            table,
            table.latestSnapshot().get().id(),
            Collections.singletonList(unit(bucket)),
            lease)
        .restoreFiles(BinaryRow.EMPTY_ROW, bucket, false, false);
  }

  private static PaimonBucketCompactionUnit unit(int bucket) {
    return new PaimonBucketCompactionUnit(
        SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW), bucket, 0, 0, 0, 0);
  }

  private static void assertRestoreEquals(RestoreFiles expected, RestoreFiles actual) {
    assertEquals(expected.snapshot().id(), actual.snapshot().id());
    assertEquals(expected.totalBuckets(), actual.totalBuckets());
    assertEquals(expected.dataFiles(), actual.dataFiles());
    assertEquals(expected.dynamicBucketIndex(), actual.dynamicBucketIndex());
    assertEquals(expected.deleteVectorsIndex(), actual.deleteVectorsIndex());
  }

  private static <T> T roundTrip(T value) {
    ByteBuffer buffer = SerializationUtil.simpleSerialize(value);
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return SerializationUtil.simpleDeserialize(bytes);
  }

  private static final class Fixture implements AutoCloseable {
    private final Catalog catalog;
    private final CountingFileIO io = new CountingFileIO();
    private final FileStoreTable raw;

    private Fixture(Path directory, int buckets) throws Exception {
      catalog = catalog(directory);
      Map<String, String> options = options(false);
      options.put("bucket", String.valueOf(buckets));
      options.put("scan.manifest.parallelism", "5");
      options.put("num-sorted-run.compaction-trigger", "2");
      FileStoreTable original = table(catalog, "cache", options);
      for (int batch = 0; batch < 2; batch++) {
        GenericRow[] rows = new GenericRow[256];
        for (int i = 0; i < rows.length; i++) {
          rows[i] = GenericRow.of(i, batch, i, 1L);
        }
        append(original, rows);
      }
      raw =
          new PrimaryKeyFileStoreTable(
              io, original.location(), original.schema(), CatalogEnvironment.empty());
    }

    private PaimonTable taskTable(Map<String, String> props) {
      return roundTrip(new PaimonTable(TableIdentifier.of("test", "db", "cache"), raw, props));
    }

    private Counter counter() {
      return io.counter();
    }

    private List<ManifestFileMeta> manifests() {
      return raw.store()
          .newScan()
          .manifestsReader()
          .read(raw.latestSnapshot().get(), ScanMode.ALL)
          .filteredManifests;
    }

    @Override
    public void close() throws Exception {
      catalog.close();
    }
  }

  private static final class Counter {
    private final AtomicInteger opens = new AtomicInteger();
    private volatile boolean fail;
    private volatile String failName;
    private volatile String blockName;
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
  }

  private static final class CountingFileIO extends LocalFileIO {
    private static final long serialVersionUID = 1L;
    private static final Map<String, Counter> COUNTERS = new ConcurrentHashMap<>();
    private final String identity = UUID.randomUUID().toString();

    private CountingFileIO() {
      COUNTERS.put(identity, new Counter());
    }

    private Counter counter() {
      return COUNTERS.get(identity);
    }

    @Override
    public SeekableInputStream newInputStream(org.apache.paimon.fs.Path path) throws IOException {
      if (path.getName().startsWith("manifest-") && !path.getName().startsWith("manifest-list-")) {
        counter().opens.incrementAndGet();
        if (counter().fail || path.getName().equals(counter().failName)) {
          throw new IOException("Injected manifest read failure");
        }
      }
      if (path.getName().equals(counter().blockName)) {
        counter().entered.countDown();
        try {
          if (!counter().release.await(15, TimeUnit.SECONDS)) {
            throw new IOException("Blocked manifest timed out");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      return super.newInputStream(path);
    }
  }
}
