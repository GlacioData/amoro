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
import org.apache.paimon.CoreOptions;
import org.apache.paimon.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PaimonPrimaryKeyOptions {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonPrimaryKeyOptions.class);

  private static final BigDecimal DEFAULT_MAJOR_MAX_BUCKET_RATIO = new BigDecimal("0.33");
  private static final BigDecimal MIN_MAJOR_MAX_BUCKET_RATIO = new BigDecimal("0.33");

  public static final String PARTITION_IDLE_TIME =
      "paimon-optimizer.primary-key.partition-idle-time";
  public static final String MAJOR_MAX_BUCKET_RATIO =
      "paimon-optimizer.primary-key.major.max-bucket-ratio";

  private final Duration partitionIdleTime;
  private final BigDecimal majorMaxBucketRatio;

  private PaimonPrimaryKeyOptions(Duration partitionIdleTime, BigDecimal majorMaxBucketRatio) {
    this.partitionIdleTime = partitionIdleTime;
    this.majorMaxBucketRatio = majorMaxBucketRatio;
  }

  public static PaimonPrimaryKeyOptions from(Map<String, String> properties) {
    Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
    Duration partitionIdleTime =
        props.containsKey(PARTITION_IDLE_TIME)
            ? parseDuration(props.get(PARTITION_IDLE_TIME))
            : null;
    BigDecimal majorMaxBucketRatio = parseMajorMaxBucketRatio(props);

    return new PaimonPrimaryKeyOptions(partitionIdleTime, majorMaxBucketRatio);
  }

  static CoreOptions compactionStrategyOptions(CoreOptions originalOptions) {
    Map<String, String> options = new HashMap<>(originalOptions.toMap());
    if (PaimonOptimizingEligibility.isEligible(options)) {
      // Paimon 1.4.2：EarlyFullCompaction#create/tryFullCompact 的计时状态属于实例，
      // interval 非空且 lastFullCompaction 为空时，多 run 可在首次选择成为全量候选。
      // Amoro 双开关托管时由自身控制周期 FULL，仅移除 interval，保留原生大小阈值。
      // 源码：paimon-core/src/main/java/org/apache/paimon/mergetree/compact/EarlyFullCompaction.java
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/EarlyFullCompaction.java#L57-L81
      options.remove(CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL.key());
    }
    return CoreOptions.fromMap(options);
  }

  static Map<String, String> compactionWriteOverrides(Map<String, String> originalOptions) {
    Map<String, String> overrides = new HashMap<>();
    // 必须依据原始双开关判定，不能在 write-only 已改为 false 的副本上判断托管资格。
    if (PaimonOptimizingEligibility.isEligible(originalOptions)) {
      // Paimon 1.4.2：AbstractFileStoreTable#copy(Map)/copyInternal 将 null 解释为移除副本的键；
      // 仅省略覆盖项会保留旧值。此 Map 专供 table.copy 使用，不是完整 CoreOptions 配置。
      // 源码：paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java#L322-L378
      overrides.put(CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL.key(), null);
    }
    overrides.put(CoreOptions.WRITE_ONLY.key(), "false");
    return overrides;
  }

  private static Duration parseDuration(String value) {
    try {
      return TimeUtils.parseDuration(value);
    } catch (RuntimeException paimonStyleFailure) {
      try {
        return Duration.parse(value);
      } catch (RuntimeException isoFailure) {
        paimonStyleFailure.addSuppressed(isoFailure);
        throw paimonStyleFailure;
      }
    }
  }

  private static BigDecimal parseMajorMaxBucketRatio(Map<String, String> properties) {
    if (!properties.containsKey(MAJOR_MAX_BUCKET_RATIO)) {
      return DEFAULT_MAJOR_MAX_BUCKET_RATIO;
    }

    String rawValue = properties.get(MAJOR_MAX_BUCKET_RATIO);
    if (rawValue == null || rawValue.trim().isEmpty()) {
      throw new IllegalArgumentException(MAJOR_MAX_BUCKET_RATIO + " must be a decimal value.");
    }

    final BigDecimal configured;
    try {
      configured = new BigDecimal(rawValue.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          MAJOR_MAX_BUCKET_RATIO + " must be a decimal value, but was: " + rawValue, e);
    }
    if (configured.compareTo(BigDecimal.ONE) > 0) {
      throw new IllegalArgumentException(
          MAJOR_MAX_BUCKET_RATIO + " must not be greater than 1.00, but was: " + rawValue);
    }

    BigDecimal truncated = configured.setScale(2, RoundingMode.DOWN);
    if (truncated.compareTo(MIN_MAJOR_MAX_BUCKET_RATIO) < 0) {
      LOG.warn(
          "Paimon primary-key option [{}]={} is below the minimum {}; use {}.",
          MAJOR_MAX_BUCKET_RATIO,
          rawValue,
          MIN_MAJOR_MAX_BUCKET_RATIO,
          MIN_MAJOR_MAX_BUCKET_RATIO);
      return MIN_MAJOR_MAX_BUCKET_RATIO;
    }
    return truncated;
  }

  public Optional<Duration> partitionIdleTime() {
    return Optional.ofNullable(partitionIdleTime);
  }

  public BigDecimal majorMaxBucketRatio() {
    return majorMaxBucketRatio;
  }
}
