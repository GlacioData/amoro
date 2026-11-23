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

import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.utils.DigestUtil;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.compact.EarlyFullCompaction;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.OffPeakHours;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarFile;

/** Fixed-snapshot summary. No native file metadata or Levels may escape synchronous work. */
public final class PaimonPrimaryKeySnapshotAnalysis implements FormatTableAnalysis {
  private static final boolean VERIFIED_NATIVE = verifyNativeStrategies();
  private final TableAnalysisKey key;
  private final PaimonPrimaryKeyPendingInput pendingInput;
  private final TableHealthDetails healthDetails;
  private final List<BucketFacts> bucketFacts;
  private final int activePartitionCount;
  private final boolean validForPlanning;
  private final boolean reusableCandidates;
  private final boolean candidateDecisionFailed;
  private final String candidateFingerprint;

  private PaimonPrimaryKeySnapshotAnalysis(
      PaimonPrimaryKeySnapshotWork work,
      PaimonHealthEvaluationContext context,
      boolean retainPlannerFacts) {
    this.key = work.key();
    this.pendingInput = work.pendingInput();
    this.healthDetails = work.healthDetails();
    this.activePartitionCount = work.activePartitionCount();
    this.validForPlanning = retainPlannerFacts && work.validForPlanning();
    List<BucketFacts> facts = new ArrayList<>();
    boolean reusable = false;
    boolean failed = false;
    String fingerprint = null;
    if (validForPlanning && context.bucketMode() != BucketMode.KEY_DYNAMIC) {
      try {
        CoreOptions options =
            PaimonPrimaryKeyOptions.compactionStrategyOptions(context.coreOptions().get());
        fingerprint = candidateFingerprint(options, context.offPeakRatioNow());
        // Only managed, stateless strategies can share the native decision across calls.
        // EarlyFull's interval and GENTLE's counter remain on the fresh native path.
        reusable =
            VERIFIED_NATIVE
                && PaimonOptimizingEligibility.isEligible(context.coreOptions().get().toMap())
                && options.optimizedCompactionInterval() == null
                && (!options.needLookup()
                    || options.lookupCompact() == CoreOptions.LookupCompactMode.RADICAL);
        for (PaimonPrimaryKeySnapshotWork.BucketFacts bucket : work.bucketFacts()) {
          if (bucket.candidate() < 0) {
            failed = true;
            reusable = false;
          }
          facts.add(
              new BucketFacts(bucket.unit(), bucket.ownedPartitionBytes(), bucket.candidate() > 0));
        }
      } catch (RuntimeException failure) {
        // Candidate failure must not replace an independently completed health result.
        failed = true;
        reusable = false;
      }
    }
    // Keep every bucket, including non-candidates and buckets outside diagnostic top-20.
    if (retainPlannerFacts && facts.size() != work.bucketFacts().size()) {
      facts.clear();
      for (PaimonPrimaryKeySnapshotWork.BucketFacts bucket : work.bucketFacts()) {
        facts.add(new BucketFacts(bucket.unit(), bucket.ownedPartitionBytes(), false));
      }
    }
    this.bucketFacts = Collections.unmodifiableList(facts);
    this.reusableCandidates = reusable;
    this.candidateDecisionFailed = failed;
    this.candidateFingerprint = fingerprint;
  }

  private static boolean verifyNativeStrategies() {
    // Inspect the actual code sources, not split-package implementation version metadata.
    // An unverified runtime still computes fresh native decisions but does not retain them for
    // reuse.
    Set<URL> verified = new HashSet<>();
    for (Class<?> type :
        new Class<?>[] {
          CoreOptions.class,
          Levels.class,
          UniversalCompaction.class,
          EarlyFullCompaction.class,
          ForceUpLevel0Compaction.class,
          OffPeakHours.class
        }) {
      try {
        URL location = type.getProtectionDomain().getCodeSource().getLocation();
        if (!verified.add(location)) {
          continue;
        }
        if (!"file".equals(location.getProtocol())) {
          return false;
        }
        try (JarFile jar = new JarFile(new File(location.toURI()))) {
          if (jar.getManifest() == null
              || !"1.4.2"
                  .equals(
                      jar.getManifest().getMainAttributes().getValue("Implementation-Version"))) {
            return false;
          }
        }
      } catch (Exception unavailableVersion) {
        return false;
      }
    }
    return true;
  }

  public static PaimonPrimaryKeySnapshotAnalysis analyze(
      FileStoreTable table, PaimonHealthEvaluationContext context) {
    return analyze(table, context, true);
  }

  public static PaimonPrimaryKeySnapshotAnalysis analyzeHealth(
      FileStoreTable table, PaimonHealthEvaluationContext context) {
    return analyze(table, context, false);
  }

  private static PaimonPrimaryKeySnapshotAnalysis analyze(
      FileStoreTable table, PaimonHealthEvaluationContext context, boolean retainPlannerFacts) {
    try (PaimonPrimaryKeySnapshotWork work =
        PaimonPrimaryKeySnapshotWork.analyze(table, context, retainPlannerFacts)) {
      return new PaimonPrimaryKeySnapshotAnalysis(work, context, retainPlannerFacts);
    }
  }

  @Override
  public TableAnalysisKey key() {
    return key;
  }

  @Override
  public PaimonPrimaryKeyPendingInput pendingInput() {
    return pendingInput;
  }

  @Override
  public TableHealthDetails healthDetails() {
    return healthDetails;
  }

  List<BucketFacts> bucketFacts() {
    return bucketFacts;
  }

  int activePartitionCount() {
    return activePartitionCount;
  }

  public boolean validForPlanning() {
    return validForPlanning;
  }

  boolean reusableCandidates(CoreOptions options, int offPeakRatio) {
    return reusableCandidates
        && candidateFingerprint.equals(candidateFingerprint(options, offPeakRatio));
  }

  private static String candidateFingerprint(CoreOptions options, int offPeakRatio) {
    // Health identity intentionally omits some native EarlyFull options. Keep the complete
    // strategy configuration separate so equal health keys cannot reuse stale candidates.
    StringBuilder encoded = new StringBuilder().append(offPeakRatio).append(':');
    new TreeMap<>(options.toMap())
        .forEach(
            (key, value) -> {
              encoded.append(key.length()).append(':').append(key);
              String text = String.valueOf(value);
              encoded.append(text.length()).append(':').append(text);
            });
    return DigestUtil.sha256Hex(encoded.toString());
  }

  boolean candidateDecisionFailed() {
    return candidateDecisionFailed;
  }

  static final class BucketFacts {
    private final PaimonBucketCompactionUnit unit;
    private final byte[] partitionBytes;
    private final boolean normalCandidate;

    private BucketFacts(
        PaimonBucketCompactionUnit unit, byte[] partitionBytes, boolean normalCandidate) {
      this.unit = unit;
      this.partitionBytes = partitionBytes;
      this.normalCandidate = normalCandidate;
    }

    byte[] partitionBytes() {
      return partitionBytes.clone();
    }

    int bucket() {
      return unit.getBucket();
    }

    PaimonBucketCompactionUnit unit() {
      return unit;
    }

    boolean normalCandidate() {
      return normalCandidate;
    }

    long sortedRunCount() {
      return unit.getSortedRunCount();
    }

    long fileCount() {
      return unit.getFileCount();
    }

    long fileSize() {
      return unit.getFileSizeInBytes();
    }
  }
}
