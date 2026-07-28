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

import org.apache.paimon.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class PaimonPrimaryKeyOptions {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonPrimaryKeyOptions.class);

  private static final BigDecimal DEFAULT_MAJOR_MAX_BUCKET_RATIO = new BigDecimal("0.33");
  private static final BigDecimal MIN_MAJOR_MAX_BUCKET_RATIO = new BigDecimal("0.33");

  public static final String ENABLED = "paimon-optimizer.primary-key.enabled";
  public static final String PARTITION_IDLE_TIME =
      "paimon-optimizer.primary-key.partition-idle-time";
  public static final String MAJOR_FILE_COUNT_THRESHOLD =
      "paimon-optimizer.primary-key.major.file-count-threshold";
  public static final String MAJOR_MAX_BUCKET_RATIO =
      "paimon-optimizer.primary-key.major.max-bucket-ratio";

  private final boolean enabled;
  private final Duration partitionIdleTime;
  private final BigDecimal majorMaxBucketRatio;

  private PaimonPrimaryKeyOptions(
      boolean enabled, Duration partitionIdleTime, BigDecimal majorMaxBucketRatio) {
    this.enabled = enabled;
    this.partitionIdleTime = partitionIdleTime;
    this.majorMaxBucketRatio = majorMaxBucketRatio;
  }

  public static PaimonPrimaryKeyOptions from(Map<String, String> properties) {
    Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
    boolean enabled = enabled(properties);
    Duration partitionIdleTime =
        props.containsKey(PARTITION_IDLE_TIME)
            ? parseDuration(props.get(PARTITION_IDLE_TIME))
            : null;
    if (props.containsKey(MAJOR_FILE_COUNT_THRESHOLD)) {
      LOG.warn(
          "Paimon primary-key option [{}] is deprecated and ignored.", MAJOR_FILE_COUNT_THRESHOLD);
    }
    BigDecimal majorMaxBucketRatio = parseMajorMaxBucketRatio(props);

    return new PaimonPrimaryKeyOptions(enabled, partitionIdleTime, majorMaxBucketRatio);
  }

  public static boolean enabled(Map<String, String> properties) {
    Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
    return Boolean.parseBoolean(props.getOrDefault(ENABLED, "false"));
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

  public boolean enabled() {
    return enabled;
  }

  public Optional<Duration> partitionIdleTime() {
    return Optional.ofNullable(partitionIdleTime);
  }

  public BigDecimal majorMaxBucketRatio() {
    return majorMaxBucketRatio;
  }
}
