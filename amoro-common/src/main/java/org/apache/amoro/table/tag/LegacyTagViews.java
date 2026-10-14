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

import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Compatibility bridge for existing read-only plugins, preserving their legacy fields. */
public final class LegacyTagViews {
  private LegacyTagViews() {}

  public static Pair<List<TableTagInfo>, Long> page(List<TagOrBranchInfo> legacy, TagQuery query) {
    int offset = query.offset();
    if (query.getScope() == null
        || !"DEFAULT".equals(query.getScope().getKind())
        || query.getScope().getBranch() != null) {
      throw TagOperationException.unsupported("SCOPE");
    }
    List<TableTagInfo> rows = new ArrayList<>();
    for (TagOrBranchInfo old : legacy) {
      if (query.getKeyword() != null && !old.getName().contains(query.getKeyword())) {
        continue;
      }
      TableTagInfo row = new TableTagInfo();
      row.setName(old.getName());
      row.setType(old.getType());
      row.setSnapshotId(old.getSnapshotId());
      row.setTarget(TableVersionRef.of(Long.toString(old.getSnapshotId()), null));
      row.setMinSnapshotsToKeep(old.getMinSnapshotsToKeep());
      row.setMaxSnapshotAgeMs(old.getMaxSnapshotAgeMs());
      row.setMaxRefAgeMs(old.getMaxRefAgeMs());
      rows.add(row);
    }
    rows.sort(Comparator.comparing(TableTagInfo::getName));
    long total = rows.size();
    return Pair.of(
        new ArrayList<>(
            rows.subList(
                Math.min(offset, rows.size()),
                (int) Math.min((long) offset + query.getPageSize(), rows.size()))),
        total);
  }
}
