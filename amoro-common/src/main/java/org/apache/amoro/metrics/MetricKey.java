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

package org.apache.amoro.metrics;

import org.apache.amoro.shade.guava32.com.google.common.base.Joiner;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Immutable identity and fixed labels of a registered metric. */
public class MetricKey {

  private final MetricDefine define;
  private final Map<String, String> valueOfTags;
  private final List<String> orderedTagValues;
  private final int hash;

  public MetricKey(MetricDefine define, Map<String, String> tagValues) {
    Preconditions.checkNotNull(define);
    this.valueOfTags = snapshotTags(tagValues);
    if (define.getTags().size() != this.valueOfTags.size()) {
      throw new IllegalArgumentException(
          "Tag value is miss-match with metric define. MetricDefine{"
              + define
              + "} given tags:"
              + Joiner.on(",").join(this.valueOfTags.keySet().stream().sorted().iterator()));
    }

    define
        .getTags()
        .forEach(
            t ->
                Preconditions.checkArgument(
                    this.valueOfTags.containsKey(t), "The value of tag: %s is missed", t));
    this.define = define;
    List<String> orderedValues = Lists.newArrayListWithCapacity(define.getTags().size());
    define.getTags().forEach(tag -> orderedValues.add(valueOfTag(tag)));
    this.orderedTagValues = Collections.unmodifiableList(orderedValues);
    this.hash = Objects.hash(define, this.valueOfTags);
  }

  private static Map<String, String> snapshotTags(Map<String, String> tags) {
    if (tags == null) {
      return ImmutableMap.of();
    }
    if (tags instanceof ImmutableMap) {
      return tags;
    }
    // Keep the existing representation of null values while isolating mutable caller maps.
    return Collections.unmodifiableMap(new HashMap<>(tags));
  }

  public MetricDefine getDefine() {
    return define;
  }

  public String valueOfTag(String tag) {
    return valueOfTags.getOrDefault(tag, "");
  }

  public List<String> valueOfTags() {
    return orderedTagValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricKey that = (MetricKey) o;
    return Objects.equals(this.define, that.define)
        && Objects.equals(this.valueOfTags, that.valueOfTags);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    String desc = define.getName() + ":" + define.getType().name();
    if (!define.getTags().isEmpty()) {
      String tagDesc =
          define.getTags().stream()
              .map(t -> t + "=" + valueOfTag(t))
              .collect(Collectors.joining(","));
      desc = "<" + tagDesc + ">";
    }
    return desc;
  }
}
