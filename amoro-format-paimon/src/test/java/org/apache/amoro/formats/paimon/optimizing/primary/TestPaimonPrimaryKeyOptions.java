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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

class TestPaimonPrimaryKeyOptions {

  @Test
  void defaultsKeepSupportedPrimaryKeyOptions() {
    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(new HashMap<>());

    assertFalse(options.partitionIdleTime().isPresent());
    assertEquals(new BigDecimal("0.33"), options.majorMaxBucketRatio());
  }

  @Test
  void parsesPrimaryKeySpecificOptions() {
    Map<String, String> props = new HashMap<>();
    props.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "PT30M");
    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "0.302");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertEquals(
        Duration.ofMinutes(30), options.partitionIdleTime().orElseThrow(AssertionError::new));
    assertEquals(new BigDecimal("0.33"), options.majorMaxBucketRatio());
  }

  @Test
  void clampsMajorMaxBucketRatioBelowMinimum() {
    Map<String, String> props = new HashMap<>();
    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "0.329");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertEquals(new BigDecimal("0.33"), options.majorMaxBucketRatio());
  }

  @Test
  void truncatesMajorMaxBucketRatioWithoutFloatingPointArithmetic() {
    Map<String, String> props = new HashMap<>();
    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "0.341");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertEquals(new BigDecimal("0.34"), options.majorMaxBucketRatio());

    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "0.339");
    assertEquals(new BigDecimal("0.33"), PaimonPrimaryKeyOptions.from(props).majorMaxBucketRatio());

    props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "1.000");
    assertEquals(new BigDecimal("1.00"), PaimonPrimaryKeyOptions.from(props).majorMaxBucketRatio());
  }

  @Test
  void rejectsInvalidMajorMaxBucketRatios() {
    for (String value : new String[] {"", "not-a-number", "NaN", "Infinity", "1.001"}) {
      Map<String, String> props = new HashMap<>();
      props.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, value);

      assertThrows(
          IllegalArgumentException.class, () -> PaimonPrimaryKeyOptions.from(props), value);
    }
  }

  @Test
  void parsesPaimonStylePartitionIdleTime() {
    Map<String, String> props = new HashMap<>();
    props.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "10s");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertEquals(
        Duration.ofSeconds(10), options.partitionIdleTime().orElseThrow(AssertionError::new));
  }

  @Test
  void ignoresRemovedMaxBucketsPerTaskOption() {
    Map<String, String> props = new HashMap<>();
    props.put("paimon-optimizer.primary-key.max-buckets-per-task", "0");

    PaimonPrimaryKeyOptions options = PaimonPrimaryKeyOptions.from(props);

    assertFalse(options.partitionIdleTime().isPresent());
    assertEquals(new BigDecimal("0.33"), options.majorMaxBucketRatio());
  }
}
