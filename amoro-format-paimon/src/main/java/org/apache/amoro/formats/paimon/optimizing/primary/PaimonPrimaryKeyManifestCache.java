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

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.shade.guava32.com.google.common.hash.Hashing;
import org.apache.amoro.shade.guava32.com.google.common.hash.HashingOutputStream;
import org.apache.amoro.shade.guava32.com.google.common.io.ByteStreams;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Segments;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SegmentsCache;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

/**
 * Shares native manifest caches by catalog context for Optimizer primary-key restore scans only.
 */
final class PaimonPrimaryKeyManifestCache {
  private static final Cache<String, SharedCache> CACHES =
      Caffeine.newBuilder().softValues().executor(Runnable::run).build();
  private static final Semaphore CACHE_LOADER = new Semaphore(1);

  private PaimonPrimaryKeyManifestCache() {}

  static ScanLease acquire(PaimonTable wrapper, FileStoreTable table) {
    Options options = Options.fromMap(wrapper.catalogProperties());
    if (table.getManifestCache() != null
        || table.fileIO() == null
        || !options.get(CatalogOptions.CACHE_ENABLED)) {
      return new ScanLease(table, null, false);
    }
    // Match CachingCatalog: max-memory overrides the budget and removes the size limit only when
    // larger.
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/catalog/CachingCatalog.java#L76-L99
    MemorySize budget = options.get(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY);
    long threshold = options.get(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD).getBytes();
    Optional<MemorySize> maximum = options.getOptional(CatalogOptions.CACHE_MANIFEST_MAX_MEMORY);
    if (maximum.isPresent() && maximum.get().compareTo(budget) > 0) {
      budget = maximum.get();
      threshold = Long.MAX_VALUE;
    }
    if (budget.getBytes() == 0) {
      return new ScanLease(table, null, false);
    }
    final String context;
    try {
      context = contextFingerprint(wrapper, table);
    } catch (IOException e) {
      // Keep the original read behavior when FileIO cannot be serialized for cross-task reuse.
      return new ScanLease(table, null, false);
    }
    final MemorySize cacheBudget = budget;
    final long cacheThreshold = threshold;
    SharedCache shared = CACHES.get(context, key -> new SharedCache(cacheBudget, cacheThreshold));
    boolean loading = CACHE_LOADER.tryAcquire();
    try {
      return new ScanLease(table, shared, loading);
    } catch (RuntimeException | Error e) {
      if (loading) {
        CACHE_LOADER.release();
      }
      throw e;
    }
  }

  private static String contextFingerprint(PaimonTable wrapper, FileStoreTable table)
      throws IOException {
    // Hash sorted options directly without retaining credentials or allocating
    // per-bucket/task/table budgets.
    HashingOutputStream digest =
        new HashingOutputStream(Hashing.sha256(), ByteStreams.nullOutputStream());
    try (ObjectOutputStream output = new ObjectOutputStream(digest)) {
      output.writeObject(wrapper.id().getCatalog());
      output.writeObject(new TreeMap<>(wrapper.catalogProperties()));
      output.writeObject(table.fileIO());
    }
    return digest.hash().toString();
  }

  private static final class SharedCache {
    private final SegmentsCache<Path> cache;
    private final SegmentsCache<Path> readOnly;

    private SharedCache(MemorySize budget, long threshold) {
      cache = SegmentsCache.create(budget, threshold);
      readOnly = new ReadOnlyCache(cache);
    }
  }

  private static class ReadOnlyCache extends SegmentsCache<Path> {
    protected final SegmentsCache<Path> delegate;

    private ReadOnlyCache(SegmentsCache<Path> delegate) {
      super(delegate.pageSize(), MemorySize.ofBytes(0), -1L);
      this.delegate = delegate;
    }

    @Override
    public Segments getIfPresents(Path path) {
      return delegate.getIfPresents(path);
    }

    @Override
    public void put(Path path, Segments segments) {
      // ObjectsCache checks for a hit first; on a miss, fileSize > -1 uses the original iterator.
      // Use -1 rather than 0 to prevent segment creation even for empty files.
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/utils/ObjectsCache.java#L65-L94
      throw new UnsupportedOperationException("Read-only manifest cache");
    }
  }

  /** Loads synchronously in native workers and retains the permit until actual loading finishes. */
  private static final class LoadingCache extends ReadOnlyCache {
    private final ScanLease lease;
    private final Map<String, Long> fileSizes = new HashMap<>();

    private LoadingCache(ScanLease lease, SegmentsCache<Path> delegate) {
      super(delegate);
      this.lease = lease;
    }

    @Override
    public Segments getIfPresents(Path path) {
      Segments hit = delegate.getIfPresents(path);
      Long fileSize = fileSizes.get(path.getName());
      if (hit != null
          || fileSize == null
          || fileSize > delegate.maxElementSize()
          || !lease.beginLoad()) {
        return hit;
      }
      try {
        // Native loading creates partition/bucket segments; false filters avoid an extra full entry
        // list.
        // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestEntryCache.java#L88-L127
        CapturingCache capture = new CapturingCache(delegate);
        FileStoreTable loader = lease.table.copy(lease.table.schema());
        loader.setManifestCache(capture);
        loader
            .store()
            .manifestFileFactory()
            .create()
            .read(path.getName(), fileSize, null, null, row -> false, entry -> false);
        return capture.loaded;
      } finally {
        lease.endLoad();
      }
    }
  }

  /**
   * Returns loaded segments to the scan even after immediate eviction, avoiding a second storage
   * read.
   */
  private static final class CapturingCache extends SegmentsCache<Path> {
    private final SegmentsCache<Path> delegate;
    private Segments loaded;

    private CapturingCache(SegmentsCache<Path> delegate) {
      super(delegate.pageSize(), MemorySize.ofBytes(0), delegate.maxElementSize());
      this.delegate = delegate;
    }

    @Override
    public Segments getIfPresents(Path path) {
      loaded = delegate.getIfPresents(path);
      return loaded;
    }

    @Override
    public void put(Path path, Segments segments) {
      loaded = segments;
      delegate.put(path, segments);
    }
  }

  static final class ScanLease implements AutoCloseable {
    private final FileStoreTable table;
    private final FileStoreScan scan;
    // Retain the container during restore; active workers also retain the lease through
    // LoadingCache.
    private final SharedCache shared;
    private final LoadingCache loadingCache;
    private boolean loading;
    private boolean closed;
    private int activeLoads;

    private ScanLease(FileStoreTable table, SharedCache shared, boolean loading) {
      this.table = table;
      this.shared = shared;
      this.loading = loading;
      this.loadingCache = loading ? new LoadingCache(this, shared.cache) : null;
      if (shared == null) {
        scan = table.store().newScan();
      } else {
        // copy(schema) does not query latest. Only scans use this copy; writers and indexes use the
        // original.
        // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java#L396-L414
        Map<String, String> scanOptions = new HashMap<>(table.options());
        if (loading) {
          scanOptions.put(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1");
        }
        FileStoreTable scanTable = table.copy(table.schema().copy(scanOptions));
        scanTable.setManifestCache(loading ? loadingCache : shared.readOnly);
        scan = scanTable.store().newScan();
      }
    }

    FileStoreScan scan() {
      return scan;
    }

    Iterator<ManifestEntry> readFiles(Snapshot snapshot, BinaryRow partition, int bucket) {
      scan.withSnapshot(snapshot).withPartitionBucket(partition, bucket);
      if (loadingCache == null) {
        return scan.readFileIterator();
      }
      // Reuse the native ALL scan filteredManifests without reading the manifest list twice.
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreScan.java#L390-L402
      List<ManifestFileMeta> manifests =
          scan.manifestsReader().read(snapshot, ScanMode.ALL).filteredManifests;
      loadingCache.fileSizes.clear();
      for (ManifestFileMeta manifest : manifests) {
        if (manifest.fileSize() <= shared.cache.maxElementSize()) {
          loadingCache.fileSizes.put(manifest.fileName(), manifest.fileSize());
        }
      }
      if (loadingCache.fileSizes.isEmpty()) {
        // When every file exceeds the native threshold, preserve scan concurrency and reuse the
        // manifest list.
        FileStoreScan uncached =
            table.store().newScan().withSnapshot(snapshot).withPartitionBucket(partition, bucket);
        if (CoreOptions.fromMap(table.options()).manifestDeleteFileDropStats()) {
          uncached.dropStats();
        }
        return uncached.readFileIterator(manifests);
      }
      return scan.readFileIterator(manifests);
    }

    private synchronized boolean beginLoad() {
      if (closed) {
        return false;
      }
      activeLoads++;
      return true;
    }

    private synchronized void endLoad() {
      activeLoads--;
      releaseIfFinished();
    }

    @Override
    public synchronized void close() {
      closed = true;
      releaseIfFinished();
    }

    private void releaseIfFinished() {
      if (closed && activeLoads == 0 && loading) {
        loading = false;
        CACHE_LOADER.release();
      }
    }
  }
}
