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

package org.apache.amoro.table.tag;

import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

/** Format-neutral Tag contract; version IDs remain opaque strings. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableTagInfo {
  private String name;
  private String type = "tag";
  private TagScope scope = new TagScope();
  private TableVersionRef target;
  private Long versionCommittedAtMillis;
  private String createdAt;
  private TagRetentionInfo retention = new TagRetentionInfo();
  private String observationToken;
  private Long snapshotId;
  private Integer minSnapshotsToKeep;
  private Long maxSnapshotAgeMs;
  private Long maxRefAgeMs;

  public String getName() {
    return name;
  }

  public void setName(String value) {
    name = value;
  }

  public String getType() {
    return type;
  }

  public void setType(String value) {
    type = value;
  }

  public TagScope getScope() {
    return scope;
  }

  public void setScope(TagScope value) {
    scope = value;
  }

  public TableVersionRef getTarget() {
    return target;
  }

  public void setTarget(TableVersionRef value) {
    target = value;
  }

  public Long getVersionCommittedAtMillis() {
    return versionCommittedAtMillis;
  }

  public void setVersionCommittedAtMillis(Long value) {
    versionCommittedAtMillis = value;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(String value) {
    createdAt = value;
  }

  public TagRetentionInfo getRetention() {
    return retention;
  }

  public void setRetention(TagRetentionInfo value) {
    retention = value;
  }

  public String getObservationToken() {
    return observationToken;
  }

  public void setObservationToken(String value) {
    observationToken = value;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(Long value) {
    snapshotId = value;
  }

  public Integer getMinSnapshotsToKeep() {
    return minSnapshotsToKeep;
  }

  public void setMinSnapshotsToKeep(Integer value) {
    minSnapshotsToKeep = value;
  }

  public Long getMaxSnapshotAgeMs() {
    return maxSnapshotAgeMs;
  }

  public void setMaxSnapshotAgeMs(Long value) {
    maxSnapshotAgeMs = value;
  }

  public Long getMaxRefAgeMs() {
    return maxRefAgeMs;
  }

  public void setMaxRefAgeMs(Long value) {
    maxRefAgeMs = value;
  }
}
