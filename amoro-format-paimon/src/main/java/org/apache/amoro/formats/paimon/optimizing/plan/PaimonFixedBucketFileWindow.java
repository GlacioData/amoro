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

package org.apache.amoro.formats.paimon.optimizing.plan;

import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Predicate;

/** Bounded unit metadata; finish only after exhausting its fixed-snapshot scope scan. */
final class PaimonFixedBucketFileWindow {
  private static final Comparator<ManifestEntry> ORDER =
      Comparator.comparingLong((ManifestEntry e) -> e.file().minSequenceNumber())
          .thenComparingLong(e -> e.file().maxSequenceNumber())
          .thenComparing(ManifestEntry::fileName);

  private final PaimonAppendUnitKey unit;
  private final int totalBuckets;
  private final int capacity;
  private final boolean ordered;
  private final PriorityQueue<RankedFile> retained;
  private final Predicate<ManifestEntry> problemFile;
  private final ManifestEntry lowerExclusive;
  private long remainingProblemCount;
  private int retainedProblemCount;
  private final Map<String, ManifestEntry> retainedByName = new HashMap<>();
  private final Set<String> verifiedNames = new HashSet<>();
  private final Set<String> barriers = new HashSet<>();
  private long verifiedFileCount;
  private boolean scopeProven;
  private long totalFileCount;
  private long minDiscardedSequence = Long.MAX_VALUE;
  private boolean finished;
  private boolean invalid;
  private boolean provenPrefix;
  private String reason = "";
  private List<ManifestEntry> files;

  PaimonFixedBucketFileWindow(
      PaimonAppendUnitKey unit, int totalBuckets, int capacity, boolean ordered) {
    this(unit, totalBuckets, capacity, ordered, entry -> true);
  }

  PaimonFixedBucketFileWindow(
      PaimonAppendUnitKey unit,
      int totalBuckets,
      int capacity,
      boolean ordered,
      Predicate<ManifestEntry> problemFile) {
    this(unit, totalBuckets, capacity, ordered, problemFile, null);
  }

  private PaimonFixedBucketFileWindow(
      PaimonAppendUnitKey unit,
      int totalBuckets,
      int capacity,
      boolean ordered,
      Predicate<ManifestEntry> problemFile,
      ManifestEntry lowerExclusive) {
    if (capacity <= 0 || totalBuckets <= 0) {
      throw new IllegalArgumentException("Fixed window needs a positive capacity and bucket count");
    }
    this.unit = unit;
    this.totalBuckets = totalBuckets;
    this.capacity = capacity;
    this.ordered = ordered;
    this.problemFile = problemFile;
    this.lowerExclusive = lowerExclusive;
    // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/append/BucketedAppendCompactManager.java#L204-L229
    // Native selection advances beyond large files. Keep problem candidates before healthy
    // padding, otherwise a bounded healthy prefix can permanently hide a mergeable suffix.
    Comparator<RankedFile> selection =
        Comparator.comparing((RankedFile file) -> !file.problem)
            .thenComparing(file -> file.entry, ORDER);
    this.retained = new PriorityQueue<>(selection.reversed());
  }

  void add(ManifestEntry entry) {
    if (finished) {
      throw new IllegalStateException("Fixed window is already finished");
    }
    totalFileCount++;
    validateEntry(entry);
    if (lowerExclusive != null && ORDER.compare(entry, lowerExclusive) <= 0) {
      minDiscardedSequence = Math.min(minDiscardedSequence, entry.file().minSequenceNumber());
      return;
    }
    // Cache classification once per streamed entry, not once per heap comparison.
    boolean problem = problemFile.test(entry);
    if (problem) {
      remainingProblemCount++;
    }
    retained.add(new RankedFile(entry, problem));
    if (retained.size() > capacity) {
      ManifestEntry discarded = retained.remove().entry;
      minDiscardedSequence = Math.min(minDiscardedSequence, discarded.file().minSequenceNumber());
    }
  }

  private void validateEntry(ManifestEntry entry) {
    if (entry.kind() != FileKind.ADD
        || !unit.equals(new PaimonAppendUnitKey(entry.partition(), entry.bucket()))
        || entry.bucket() < 0
        || entry.bucket() >= totalBuckets
        || entry.totalBuckets() != totalBuckets) {
      invalidate("INVALID_BUCKET_LAYOUT");
    }
    if (entry.file().minSequenceNumber() < 0
        || entry.file().maxSequenceNumber() < entry.file().minSequenceNumber()) {
      invalidate("INVALID_FILE_SEQUENCE");
    }
  }

  void finish() {
    if (finished) {
      return;
    }
    List<ManifestEntry> sorted = new ArrayList<>(retained.size());
    for (RankedFile file : retained) {
      sorted.add(file.entry);
      if (file.problem) {
        retainedProblemCount++;
      }
    }
    sorted.sort(ORDER);
    retained.clear();
    Set<String> names = new HashSet<>();
    long previousMax = -1L;
    for (ManifestEntry file : sorted) {
      retainedByName.put(file.fileName(), file);
      if (!names.add(file.fileName())) {
        invalidate("DUPLICATE_INPUT_FILE");
      }
      if (ordered && file.file().minSequenceNumber() <= previousMax) {
        invalidate("OVERLAPPING_ORDERED_SEQUENCE");
      }
      previousMax = file.file().maxSequenceNumber();
    }
    files = Collections.unmodifiableList(sorted);
    finished = true;
    if (files.isEmpty()) {
      invalidate("EMPTY_UNIT");
    }
    // Paimon ac223f47, append/BucketedAppendCompactManager.java:87 orders by min sequence;
    // operation/BaseAppendFileStoreWrite.java:187-222 starts output at the first input sequence.
    // A bounded heap is NOT a complete bucket. Prove that every omitted file starts strictly
    // after this retained prefix before allowing ordered rewrites; no iterator order is assumed.
    provenPrefix = ordered && !invalid && !complete() && previousMax < minDiscardedSequence;
    if (!invalid && !complete()) {
      reason = needsScopeProof() ? "UNPROVEN_ORDERED_BOUNDARY" : "CANDIDATE_WINDOW_LIMIT";
    }
  }

  boolean needsScopeProof() {
    requireFinished();
    return ordered && !invalid && !complete() && !provenPrefix && !scopeProven;
  }

  void verifyScopeEntry(ManifestEntry entry) {
    requireFinished();
    if (scopeProven || complete() || provenPrefix || !ordered) {
      throw new IllegalStateException("Scope proof is not required for this window");
    }
    verifiedFileCount++;
    validateEntry(entry);
    ManifestEntry selected = retainedByName.get(entry.fileName());
    if (selected != null) {
      if (!selected.file().equals(entry.file()) || !verifiedNames.add(entry.fileName())) {
        invalidate("DUPLICATE_OR_CHANGED_SCOPE_FILE");
      }
      return;
    }
    // Paimon reference: same SHA, BaseAppendFileStoreWrite.java:187-222.
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/BaseAppendFileStoreWrite.java#L187-L222
    // Rewriting concatenates exact inputs from the first sequence. Prove both boundaries
    // against EVERY omitted file; preserve an omitted middle file as a packing barrier.
    int low = 0;
    int high = files.size();
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (files.get(middle).file().minSequenceNumber() < entry.file().minSequenceNumber()) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    if ((low > 0
            && files.get(low - 1).file().maxSequenceNumber() >= entry.file().minSequenceNumber())
        || (low < files.size()
            && entry.file().maxSequenceNumber() >= files.get(low).file().minSequenceNumber())) {
      invalidate("OVERLAPPING_OMITTED_SEQUENCE");
    }
    if (low > 0 && low < files.size()) {
      barriers.add(files.get(low).fileName());
    }
  }

  void finishScopeProof() {
    requireFinished();
    if (verifiedFileCount != totalFileCount || verifiedNames.size() != files.size()) {
      invalidate("INCOMPLETE_SCOPE_PROOF");
    }
    scopeProven = !invalid;
    if (scopeProven) {
      reason = "CANDIDATE_WINDOW_LIMIT";
    }
  }

  boolean hasBarrierBefore(String fileName) {
    requireFinished();
    return barriers.contains(fileName);
  }

  boolean canAdvance() {
    return safe() && ordered && capacity > 1 && remainingProblemCount > retainedProblemCount;
  }

  PaimonFixedBucketFileWindow advance() {
    if (!canAdvance()) {
      throw new IllegalStateException("Fixed window cannot advance");
    }
    // Paimon reference: ac223f47, BucketedAppendCompactManager.java:204-229 (URL above).
    // Native picking moves right past an unusable prefix. Preserve the trailing contiguous
    // run so a legal group crossing our metadata-window boundary is not skipped. If no
    // barrier exists, drop just one file. The exclusive cursor strictly increases each time.
    int discardThrough = 0;
    for (int i = 1; i < files.size(); i++) {
      if (hasBarrierBefore(files.get(i).fileName())) {
        discardThrough = i - 1;
      }
    }
    return new PaimonFixedBucketFileWindow(
        unit, totalBuckets, capacity, ordered, problemFile, files.get(discardThrough));
  }

  private static final class RankedFile {
    private final ManifestEntry entry;
    private final boolean problem;

    private RankedFile(ManifestEntry entry, boolean problem) {
      this.entry = entry;
      this.problem = problem;
    }
  }

  private void invalidate(String issue) {
    if (!invalid) {
      reason = issue;
    }
    invalid = true;
  }

  PaimonAppendUnitKey unit() {
    return unit;
  }

  int totalBuckets() {
    return totalBuckets;
  }

  boolean ordered() {
    return ordered;
  }

  List<ManifestEntry> files() {
    requireFinished();
    return files;
  }

  long totalFileCount() {
    requireFinished();
    return totalFileCount;
  }

  long omittedFileCount() {
    requireFinished();
    return totalFileCount - files.size();
  }

  boolean complete() {
    return omittedFileCount() == 0;
  }

  boolean provenPrefix() {
    requireFinished();
    return provenPrefix;
  }

  boolean safe() {
    requireFinished();
    return !invalid && (!ordered || complete() || provenPrefix || scopeProven);
  }

  String reason() {
    requireFinished();
    return reason;
  }

  private void requireFinished() {
    if (!finished) {
      throw new IllegalStateException("Fixed scope scan has not finished");
    }
  }
}
