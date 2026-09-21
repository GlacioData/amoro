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

package org.apache.amoro.formats.paimon.optimizing.health;

import org.apache.amoro.table.health.TableHealthStructure;
import org.apache.amoro.table.health.TableHealthStructure.Band;
import org.apache.amoro.table.health.TableHealthStructure.Problem;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.TreeMap;

/** Collects only metadata aggregates during the existing fixed-snapshot scan. */
public final class PaimonHealthStructureCollector {
  public static final int MAX_PROBLEMS = 20;
  private final FileStoreTable table;
  private final PaimonHealthEvaluationContext context;
  private final boolean primaryKey;
  private final Map<UnitKey, Unit> units = new HashMap<>();
  private final Map<Integer, Counts> bands = new TreeMap<>();

  public PaimonHealthStructureCollector(
      FileStoreTable table, PaimonHealthEvaluationContext context) {
    this.table = table;
    this.context = context;
    this.primaryKey = context.tableShape() == PaimonHealthEvaluationContext.TableShape.PRIMARY_KEY;
    int count = primaryKey ? context.numLevels() : 3;
    for (int i = 0; i < count; i++) {
      bands.put(i, new Counts());
    }
  }

  public void addFile(BinaryRow partition, int bucket, DataFileMeta file, Long deleted) {
    if (file.fileSize() < 0 || file.rowCount() < 0 || (primaryKey && file.level() < 0)) {
      throw new IllegalArgumentException("Invalid structural file metadata");
    }
    Unit unit = units.computeIfAbsent(new UnitKey(partition, bucket), ignored -> new Unit());
    unit.files = Math.addExact(unit.files, 1);
    unit.bytes = Math.addExact(unit.bytes, file.fileSize());
    unit.rows = Math.addExact(unit.rows, file.rowCount());
    if (deleted == null || deleted < 0 || deleted > file.rowCount()) {
      unit.deleteKnown = false;
    } else {
      unit.deletes = Math.addExact(unit.deletes, deleted);
    }
    int band =
        primaryKey
            ? file.level()
            : file.fileSize() < context.smallFileBoundary()
                ? 0
                : file.fileSize() < context.targetFileSize() ? 1 : 2;
    Counts total = bands.computeIfAbsent(band, ignored -> new Counts());
    total.add(file.fileSize());
    if (primaryKey) {
      // Paimon 1.4.2 Levels.numberOfSortedRuns: each L0 file, each nonempty higher level.
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/mergetree/Levels.java
      Counts level = unit.levels.computeIfAbsent(band, ignored -> new Counts());
      if (band == 0 || level.files == 0) {
        total.runs++;
        unit.runs++;
      }
      level.add(file.fileSize());
      level.runs = band == 0 ? level.files : 1;
    } else if (file.fileSize() < context.targetFileSize()) {
      unit.undersizedFiles++;
      unit.undersizedBytes = Math.addExact(unit.undersizedBytes, file.fileSize());
    }
  }

  public void observeDeletionVector(BinaryRow partition, int bucket, long count) {
    Unit unit = units.get(new UnitKey(partition, bucket));
    if (unit != null) {
      unit.dvDeletes = Math.addExact(unit.dvDeletes, count);
    }
  }

  /** Finish without sorting all units or retaining their file metadata in the published result. */
  public TableHealthStructure finish(boolean complete) {
    Comparator<Problem> order =
        Comparator.comparing((Problem p) -> !"SEVERE".equals(p.getSeverity()))
            .thenComparing(
                Comparator.comparingLong((Problem p) -> Long.parseLong(p.getSortedRuns()))
                    .reversed())
            .thenComparing(
                Comparator.comparingLong((Problem p) -> Long.parseLong(p.getFileCount()))
                    .reversed())
            .thenComparing(Problem::getPartition)
            .thenComparing(p -> p.getBucket() == null ? -1 : p.getBucket());
    PriorityQueue<Problem> top = new PriorityQueue<>(MAX_PROBLEMS, order.reversed());
    FileStorePathFactory pathFactory = null;
    long affected = 0;
    long severe = 0;
    for (Map.Entry<UnitKey, Unit> entry : units.entrySet()) {
      Unit unit = entry.getValue();
      boolean runProblem = primaryKey && unit.runs >= context.compactionTrigger();
      long outputs =
          unit.undersizedBytes == 0 ? 0 : 1 + (unit.undersizedBytes - 1) / context.targetFileSize();
      boolean fileProblem = !primaryKey && unit.undersizedFiles > Math.max(1, outputs);
      boolean deleteProblem = (unit.deleteKnown && unit.deletes > 0) || unit.dvDeletes > 0;
      if (!runProblem && !fileProblem && !deleteProblem) {
        continue;
      }
      affected++;
      boolean serious =
          (primaryKey && unit.runs >= context.stopTrigger())
              || (unit.rows > 0
                  && ((unit.deleteKnown && unit.deletes >= unit.rows - unit.deletes)
                      || unit.dvDeletes >= unit.rows - unit.dvDeletes));
      if (serious) {
        severe++;
      }
      String code =
          runProblem ? "SORTED_RUN" : fileProblem ? "FILE_ORGANIZATION" : "MATERIALIZED_DELETE";
      if (deleteProblem && (runProblem || fileProblem)) {
        code += "_AND_DELETE";
      }
      UnitKey key = entry.getKey();
      if (pathFactory == null) {
        // Paimon creates a new factory (including a UUID) on every pathFactory() call.
        // Reuse it within this result without changing native partition encoding or ordering.
        // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/AbstractFileStore.java#L126-L145
        pathFactory = table.store().pathFactory();
      }
      String partition = pathFactory.getPartitionString(key.partition);
      Problem problem =
          new Problem(
              partition,
              key.bucket < 0 ? null : key.bucket,
              code,
              serious ? "SEVERE" : "ATTENTION",
              Long.toString(unit.files),
              Long.toString(unit.bytes),
              Long.toString(unit.runs),
              Collections.emptyList());
      if (top.size() < MAX_PROBLEMS || order.compare(problem, top.peek()) < 0) {
        // Expand level details only for candidates retained by the bounded problem heap.
        if (primaryKey) {
          problem =
              new Problem(
                  partition,
                  key.bucket < 0 ? null : key.bucket,
                  code,
                  serious ? "SEVERE" : "ATTENTION",
                  Long.toString(unit.files),
                  Long.toString(unit.bytes),
                  Long.toString(unit.runs),
                  levelBands(unit.levels));
        }
        if (top.size() == MAX_PROBLEMS) {
          top.poll();
        }
        top.add(problem);
      }
    }
    List<Problem> problems = new ArrayList<>(top);
    problems.sort(order);
    List<Band> distribution;
    if (primaryKey) {
      distribution = levelBands(bands);
    } else {
      distribution = new ArrayList<>();
      String[] lower = {
        "0", Long.toString(context.smallFileBoundary()), Long.toString(context.targetFileSize())
      };
      String[] upper = {
        Long.toString(context.smallFileBoundary()), Long.toString(context.targetFileSize()), null
      };
      for (int i = 0; i < 3; i++) {
        Counts values = bands.get(i);
        distribution.add(
            new Band(
                "SIZE_" + i,
                lower[i],
                upper[i],
                Long.toString(values.files),
                Long.toString(values.bytes),
                null));
      }
    }
    return new TableHealthStructure(
        context.tableShape().name(),
        complete ? "COMPLETE" : "PARTIAL",
        System.currentTimeMillis(),
        context.snapshotTimeMillis(),
        Integer.toString(units.size()),
        Long.toString(affected),
        Long.toString(severe),
        distribution,
        problems);
  }

  private List<Band> levelBands(Map<Integer, Counts> values) {
    List<Band> result = new ArrayList<>();
    int count =
        Math.max(context.numLevels(), values.isEmpty() ? 0 : Collections.max(values.keySet()) + 1);
    for (int i = 0; i < count; i++) {
      Counts value = values.getOrDefault(i, new Counts());
      result.add(
          new Band(
              "L" + i,
              null,
              null,
              Long.toString(value.files),
              Long.toString(value.bytes),
              Long.toString(value.runs)));
    }
    return result;
  }

  private static final class Counts {
    private long files;
    private long bytes;
    private long runs;

    private void add(long size) {
      files = Math.addExact(files, 1);
      bytes = Math.addExact(bytes, size);
    }
  }

  private static final class Unit {
    private long files;
    private long bytes;
    private long rows;
    private long deletes;
    private long dvDeletes;
    private long runs;
    private long undersizedFiles;
    private long undersizedBytes;
    private boolean deleteKnown = true;
    private final Map<Integer, Counts> levels = new TreeMap<>();
  }

  private static final class UnitKey {
    private final BinaryRow partition;
    private final int bucket;

    private UnitKey(BinaryRow partition, int bucket) {
      this.partition = partition.copy();
      this.bucket = bucket;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof UnitKey)) {
        return false;
      }
      UnitKey that = (UnitKey) other;
      return bucket == that.bucket && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partition, bucket);
    }
  }
}
