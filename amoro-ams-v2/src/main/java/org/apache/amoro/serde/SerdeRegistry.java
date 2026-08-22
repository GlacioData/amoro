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

package org.apache.amoro.serde;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The validated converter chain of one resource type (framework spec §5.4). Registration
 * self-checks run eagerly: a duplicate (resource type, fromVersion) link fails construction, and
 * every historical version must walk adjacent links to the latest version — a missing link or a
 * cycle fails construction. Spring classpath scanning fills these registries in T10; unit tests and
 * programmatic assembly construct them directly.
 */
public final class SerdeRegistry {

  private final String latestVersion;
  private final Map<String, VersionedResourceConverter> linksByFromVersion = new LinkedHashMap<>();

  public SerdeRegistry(String latestVersion, List<VersionedResourceConverter> converters) {
    this.latestVersion = Objects.requireNonNull(latestVersion, "latestVersion").trim();
    if (this.latestVersion.isEmpty()) {
      throw new IllegalArgumentException("latestVersion must not be blank");
    }
    for (VersionedResourceConverter converter : converters) {
      Objects.requireNonNull(converter, "converter");
      String from = requireVersion(converter.fromVersion(), "fromVersion");
      String to = requireVersion(converter.toVersion(), "toVersion");
      if (from.equals(to)) {
        throw new IllegalArgumentException(
            "converter " + from + " -> " + to + " must link two different versions");
      }
      if (from.equals(this.latestVersion)) {
        // the latest version must not have an outgoing link: it is never reachable from any
        // walk, and accepting it would silently tolerate a dead converter
        throw new IllegalArgumentException(
            "converter "
                + from
                + " -> "
                + to
                + " leaves the latest version; the latest"
                + " version must not have outgoing links");
      }
      if (linksByFromVersion.put(from, converter) != null) {
        throw new IllegalArgumentException(
            "duplicate converter registered for input version '" + from + "'");
      }
    }
    validateChainReachability();
  }

  private static String requireVersion(String version, String field) {
    Objects.requireNonNull(version, field);
    String trimmed = version.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return trimmed;
  }

  private void validateChainReachability() {
    for (String start : linksByFromVersion.keySet()) {
      String current = start;
      int hops = 0;
      while (!current.equals(latestVersion)) {
        VersionedResourceConverter link = linksByFromVersion.get(current);
        if (link == null) {
          throw new IllegalArgumentException(
              "converter chain is incomplete: version '"
                  + current
                  + "' (reachable from '"
                  + start
                  + "') has no upgrade link towards '"
                  + latestVersion
                  + "'");
        }
        current = link.toVersion().trim();
        if (++hops > linksByFromVersion.size()) {
          throw new IllegalArgumentException(
              "converter chain from '"
                  + start
                  + "' cycles and never reaches '"
                  + latestVersion
                  + "'");
        }
      }
    }
  }

  public String latestVersion() {
    return latestVersion;
  }

  /** The upgrade link leaving {@code version}; null when the version is the latest. */
  public VersionedResourceConverter linkFrom(String version) {
    return linksByFromVersion.get(version);
  }

  public int linkCount() {
    return linksByFromVersion.size();
  }
}
