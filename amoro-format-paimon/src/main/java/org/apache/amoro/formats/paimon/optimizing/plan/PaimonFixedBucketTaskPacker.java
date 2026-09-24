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

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.shade.guava32.com.google.common.math.LongMath;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Exact file groups only; the caller keeps the window's native bucket identity. */
final class PaimonFixedBucketTaskPacker {
  private static final Logger LOG = LoggerFactory.getLogger(PaimonFixedBucketTaskPacker.class);
  private final PaimonPlanContext context;

  PaimonFixedBucketTaskPacker(PaimonPlanContext context) {
    this.context = context;
  }

  List<List<DataFileMeta>> pack(
      PaimonFixedBucketFileWindow window, PaimonPartitionEvaluation evaluation) {
    Map<String, PaimonFileCandidate> selected = validateSelection(window, evaluation);
    if (!evaluation.necessary()) {
      return Collections.emptyList();
    }
    List<List<DataFileMeta>> groups = new ArrayList<>();
    if (!window.ordered()) {
      // Paimon ac223f47, operation/BucketedAppendFileStoreWrite.java:75-85 allows overlapping
      // sequences in unordered write-only files. Reuse size-based Amoro selection, but never
      // execute or serialize its unaware task (AppendCompactTask.java:doCompact uses bucket -1).
      new PaimonAppendTaskPacker(context)
          .pack(evaluation)
          .forEach(task -> groups.add(new ArrayList<>(task.compactBefore())));
    } else {
      List<PaimonFileCandidate> run = new ArrayList<>();
      for (ManifestEntry entry : window.files()) {
        if (window.hasBarrierBefore(entry.fileName())) {
          packRun(run, evaluation.optimizingType(), groups);
          run.clear();
        }
        PaimonFileCandidate file = selected.get(entry.fileName());
        if (file == null) {
          packRun(run, evaluation.optimizingType(), groups);
          run.clear();
        } else {
          run.add(file);
        }
      }
      packRun(run, evaluation.optimizingType(), groups);
    }

    Set<String> emitted = new HashSet<>();
    List<List<DataFileMeta>> result = new ArrayList<>();
    for (List<DataFileMeta> group : groups) {
      // Native AppendCompactTask.java:71-74 requires >1 ordinary input. FULL does not override
      // that rule, even if compaction.min.file-num is configured as one.
      if (group.size() < 2) {
        continue;
      }
      for (DataFileMeta file : group) {
        PaimonFileCandidate candidate = selected.get(file.fileName());
        if (candidate == null || !candidate.file().equals(file) || !emitted.add(file.fileName())) {
          throw new IllegalStateException("Fixed packer produced a duplicate or out-of-scope file");
        }
      }
      result.add(Collections.unmodifiableList(new ArrayList<>(group)));
    }
    return Collections.unmodifiableList(result);
  }

  private Map<String, PaimonFileCandidate> validateSelection(
      PaimonFixedBucketFileWindow window, PaimonPartitionEvaluation evaluation) {
    if (!window.safe() || !window.unit().partition().equals(evaluation.partition())) {
      throw new IllegalArgumentException("Unsafe or mismatched fixed-bucket window");
    }
    Map<String, DataFileMeta> retained = new HashMap<>();
    window.files().forEach(entry -> retained.put(entry.fileName(), entry.file()));
    Map<String, PaimonFileCandidate> selected = new HashMap<>();
    for (PaimonFileCandidate file : evaluation.selectedFiles()) {
      if (!window.unit().partition().equals(file.partition())
          || file.hasDeletionVector()
          || !file.file().equals(retained.get(file.fileName()))
          || selected.put(file.fileName(), file) != null) {
        throw new IllegalArgumentException("Fixed selection is not an exact unique window subset");
      }
    }
    return selected;
  }

  private void packRun(
      List<PaimonFileCandidate> run, OptimizingType type, List<List<DataFileMeta>> output) {
    if (run.size() < 2) {
      return;
    }
    // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187-222 reads the supplied order
    // and starts sequence allocation at the FIRST input. Never bridge an unselected active file.
    List<List<PaimonFileCandidate>> bins =
        type == OptimizingType.MINOR ? packMinor(run) : packMajorOrFull(run);
    for (List<PaimonFileCandidate> bin : bins) {
      if (bin.size() > 1 && PaimonAppendCompactBenefit.shouldKeep(type, bin, context)) {
        output.add(bin.stream().map(PaimonFileCandidate::file).collect(Collectors.toList()));
      }
    }
  }

  private List<List<PaimonFileCandidate>> packMinor(List<PaimonFileCandidate> run) {
    List<List<PaimonFileCandidate>> bins = new ArrayList<>();
    List<PaimonFileCandidate> bin = new ArrayList<>();
    long total = 0L;
    for (PaimonFileCandidate file : run) {
      bin.add(file);
      total += file.fileSize() + context.openFileCost();
      if (bin.size() > 1 && total >= context.targetSize() * 2) {
        bins.add(bin);
        bin = new ArrayList<>();
        total = 0L;
      }
    }
    if (bin.size() >= context.minorMinFileNum()
        || (bin.size() > 1 && context.reachMinorInterval())) {
      bins.add(bin);
    }
    return bins;
  }

  private List<List<PaimonFileCandidate>> packMajorOrFull(List<PaimonFileCandidate> run) {
    long limit = context.effectiveTaskInputLimit();
    List<List<PaimonFileCandidate>> bins = new ArrayList<>();
    List<PaimonFileCandidate> bin = new ArrayList<>();
    long total = 0L;
    int lastBinEnd = -2;
    for (int i = 0; i < run.size(); i++) {
      PaimonFileCandidate file = run.get(i);
      if (file.fileSize() > limit) {
        // Preserve Amoro's non-oversized partner rule without reordering a native ordered run.
        // A skipped oversized file is a barrier, not permission to join its two neighbours.
        if (bin.size() >= 2 && i + 1 < run.size() && run.get(i + 1).fileSize() <= limit) {
          // Keep an already legal prefix separate when the oversized file has a following
          // partner. Otherwise a singleton tail could unnecessarily inflate the oversized bin.
          bins.add(bin);
          bin = new ArrayList<>();
          bin.add(file);
          bin.add(run.get(++i));
          bins.add(bin);
          lastBinEnd = i;
        } else if (!bin.isEmpty()) {
          if (bin.size() >= 3) {
            bins.add(new ArrayList<>(bin.subList(0, bin.size() - 1)));
            bin = new ArrayList<>(bin.subList(bin.size() - 1, bin.size()));
          }
          bin.add(file);
          bins.add(bin);
          lastBinEnd = i;
        } else if (i + 1 < run.size() && run.get(i + 1).fileSize() <= limit) {
          bin.add(file);
          bin.add(run.get(++i));
          bins.add(bin);
          lastBinEnd = i;
        } else {
          lastBinEnd = -2;
          LOG.warn(
              "Skip ordered Paimon file {} with size={} bytes, limit={} bytes: "
                  + "no adjacent non-oversized partner.",
              file.fileName(),
              file.fileSize(),
              limit);
        }
        bin = new ArrayList<>();
        total = 0L;
        continue;
      }
      // The limit is a soft target. Do not strand a singleton tail; the next normal file can
      // start a legal pair only when at least one adjacent selected file remains in this run.
      if (bin.size() > 1 && file.fileSize() > limit - total && i + 1 < run.size()) {
        bins.add(bin);
        lastBinEnd = i - 1;
        bin = new ArrayList<>();
        total = 0L;
      }
      bin.add(file);
      total = LongMath.saturatedAdd(total, file.fileSize());
    }
    if (bin.size() > 1) {
      bins.add(bin);
    } else if (bin.size() == 1 && lastBinEnd == run.size() - 2) {
      bins.get(bins.size() - 1).addAll(bin);
    }
    return bins;
  }
}
