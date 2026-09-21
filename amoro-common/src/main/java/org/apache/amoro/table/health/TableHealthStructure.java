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

package org.apache.amoro.table.health;

import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Bounded snapshot diagnostics. Counts and sizes are decimal strings to preserve REST precision.
 */
public final class TableHealthStructure {
  private final String tableShape;
  private final String availability;
  private final long evaluatedAtMillis;
  private final long snapshotTimeMillis;
  private final String totalUnits;
  private final String affectedUnits;
  private final String severeUnits;
  private final List<Band> bands;
  private final List<Problem> problems;

  @JsonCreator
  public TableHealthStructure(
      @JsonProperty("tableShape") String tableShape,
      @JsonProperty("availability") String availability,
      @JsonProperty("evaluatedAtMillis") long evaluatedAtMillis,
      @JsonProperty("snapshotTimeMillis") long snapshotTimeMillis,
      @JsonProperty("totalUnits") String totalUnits,
      @JsonProperty("affectedUnits") String affectedUnits,
      @JsonProperty("severeUnits") String severeUnits,
      @JsonProperty("bands") List<Band> bands,
      @JsonProperty("problems") List<Problem> problems) {
    this.tableShape = tableShape;
    this.availability = availability;
    this.evaluatedAtMillis = evaluatedAtMillis;
    this.snapshotTimeMillis = snapshotTimeMillis;
    this.totalUnits = totalUnits;
    this.affectedUnits = affectedUnits;
    this.severeUnits = severeUnits;
    this.bands = immutable(bands);
    this.problems = immutable(problems);
  }

  public String getTableShape() {
    return tableShape;
  }

  public String getAvailability() {
    return availability;
  }

  public long getEvaluatedAtMillis() {
    return evaluatedAtMillis;
  }

  public long getSnapshotTimeMillis() {
    return snapshotTimeMillis;
  }

  public String getTotalUnits() {
    return totalUnits;
  }

  public String getAffectedUnits() {
    return affectedUnits;
  }

  public String getSevereUnits() {
    return severeUnits;
  }

  public List<Band> getBands() {
    return bands;
  }

  public List<Problem> getProblems() {
    return problems;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TableHealthStructure)) {
      return false;
    }
    TableHealthStructure that = (TableHealthStructure) other;
    return Objects.equals(tableShape, that.tableShape)
        && Objects.equals(availability, that.availability)
        && Objects.equals(evaluatedAtMillis, that.evaluatedAtMillis)
        && Objects.equals(snapshotTimeMillis, that.snapshotTimeMillis)
        && Objects.equals(totalUnits, that.totalUnits)
        && Objects.equals(affectedUnits, that.affectedUnits)
        && Objects.equals(severeUnits, that.severeUnits)
        && Objects.equals(bands, that.bands)
        && Objects.equals(problems, that.problems);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        tableShape,
        availability,
        evaluatedAtMillis,
        snapshotTimeMillis,
        totalUnits,
        affectedUnits,
        severeUnits,
        bands,
        problems);
  }

  private static <T> List<T> immutable(List<T> values) {
    if (values == null) {
      return Collections.emptyList();
    }
    ArrayList<T> copy = new ArrayList<>(values);
    for (T value : copy) {
      Objects.requireNonNull(value, "Diagnostic element must not be null");
    }
    return Collections.unmodifiableList(copy);
  }

  /** One LSM level or half-open file-size interval; null bounds mean unbounded. */
  public static final class Band {
    private final String code;
    private final String lowerBound;
    private final String upperBound;
    private final String fileCount;
    private final String fileSize;
    private final String sortedRuns;

    @JsonCreator
    public Band(
        @JsonProperty("code") String code,
        @JsonProperty("lowerBound") String lowerBound,
        @JsonProperty("upperBound") String upperBound,
        @JsonProperty("fileCount") String fileCount,
        @JsonProperty("fileSize") String fileSize,
        @JsonProperty("sortedRuns") String sortedRuns) {
      this.code = code;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.fileCount = fileCount;
      this.fileSize = fileSize;
      this.sortedRuns = sortedRuns;
    }

    public String getCode() {
      return code;
    }

    public String getLowerBound() {
      return lowerBound;
    }

    public String getUpperBound() {
      return upperBound;
    }

    public String getFileCount() {
      return fileCount;
    }

    public String getFileSize() {
      return fileSize;
    }

    public String getSortedRuns() {
      return sortedRuns;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Band)) {
        return false;
      }
      Band that = (Band) other;
      return Objects.equals(code, that.code)
          && Objects.equals(lowerBound, that.lowerBound)
          && Objects.equals(upperBound, that.upperBound)
          && Objects.equals(fileCount, that.fileCount)
          && Objects.equals(fileSize, that.fileSize)
          && Objects.equals(sortedRuns, that.sortedRuns);
    }

    @Override
    public int hashCode() {
      return Objects.hash(code, lowerBound, upperBound, fileCount, fileSize, sortedRuns);
    }
  }

  /** One affected partition/bucket with its already collected structure. */
  public static final class Problem {
    private final String partition;
    private final Integer bucket;
    private final String code;
    private final String severity;
    private final String fileCount;
    private final String fileSize;
    private final String sortedRuns;
    private final List<Band> bands;

    @JsonCreator
    public Problem(
        @JsonProperty("partition") String partition,
        @JsonProperty("bucket") Integer bucket,
        @JsonProperty("code") String code,
        @JsonProperty("severity") String severity,
        @JsonProperty("fileCount") String fileCount,
        @JsonProperty("fileSize") String fileSize,
        @JsonProperty("sortedRuns") String sortedRuns,
        @JsonProperty("bands") List<Band> bands) {
      this.partition = partition;
      this.bucket = bucket;
      this.code = code;
      this.severity = severity;
      this.fileCount = fileCount;
      this.fileSize = fileSize;
      this.sortedRuns = sortedRuns;
      this.bands = immutable(bands);
    }

    public String getPartition() {
      return partition;
    }

    public Integer getBucket() {
      return bucket;
    }

    public String getCode() {
      return code;
    }

    public String getSeverity() {
      return severity;
    }

    public String getFileCount() {
      return fileCount;
    }

    public String getFileSize() {
      return fileSize;
    }

    public String getSortedRuns() {
      return sortedRuns;
    }

    public List<Band> getBands() {
      return bands;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Problem)) {
        return false;
      }
      Problem that = (Problem) other;
      return Objects.equals(partition, that.partition)
          && Objects.equals(bucket, that.bucket)
          && Objects.equals(code, that.code)
          && Objects.equals(severity, that.severity)
          && Objects.equals(fileCount, that.fileCount)
          && Objects.equals(fileSize, that.fileSize)
          && Objects.equals(sortedRuns, that.sortedRuns)
          && Objects.equals(bands, that.bands);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          partition, bucket, code, severity, fileCount, fileSize, sortedRuns, bands);
    }
  }
}
