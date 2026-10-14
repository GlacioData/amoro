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

package org.apache.amoro.formats.paimon.tag;

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.apache.amoro.table.tag.TableTagInfo;
import org.apache.amoro.table.tag.TableVersionRef;
import org.apache.amoro.table.tag.TagOperationException;
import org.apache.amoro.table.tag.TagQuery;
import org.apache.amoro.table.tag.TagRetentionInfo;
import org.apache.amoro.table.tag.TagScope;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.TagManager;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;

/** Native Tag reads and observation projection. */
public final class PaimonTagReader {
  private static final long JSON_SAFE_INTEGER = 9007199254740991L;

  private PaimonTagReader() {}

  public static Pair<List<TableTagInfo>, Long> page(
      PaimonTable owner, FileStoreTable table, TagQuery query) throws java.io.IOException {
    int offset = query.offset();
    validateScope(query.getScope());
    TagManager manager = manager(table);
    List<String> names =
        manager.tagPaths(path -> true).stream()
            .map(path -> path.getName().substring("tag-".length()))
            .filter(name -> query.getKeyword() == null || name.contains(query.getKeyword()))
            .sorted()
            .collect(Collectors.toList());
    List<TableTagInfo> rows = new ArrayList<>();
    for (int i = Math.min(offset, names.size());
        i < Math.min((long) offset + query.getPageSize(), names.size());
        i++) {
      String name = names.get(i);
      Tag tag =
          manager
              .get(name)
              .orElseThrow(
                  () ->
                      new TagOperationException(
                          409,
                          "CAPABILITY_CHANGED",
                          "NOT_APPLIED",
                          "Tag list changed; refresh the page"));
      rows.add(view(owner, table, name, tag));
    }
    return Pair.of(rows, (long) names.size());
  }

  public static List<TagOrBranchInfo> legacy(FileStoreTable table) {
    SortedMap<Snapshot, List<String>> tags = table.tagManager().tags();
    List<TagOrBranchInfo> rows = new ArrayList<>();
    tags.forEach(
        (snapshot, names) ->
            names.forEach(
                name ->
                    rows.add(
                        new TagOrBranchInfo(name, snapshot.id(), 0, 0L, 0L, TagOrBranchInfo.TAG))));
    return rows;
  }

  public static void validateScope(TagScope scope) {
    if (scope == null
        || !("DEFAULT".equals(scope.getKind()) && scope.getBranch() == null
            || "BRANCH".equals(scope.getKind()) && "main".equals(scope.getBranch()))) {
      throw new TagOperationException(
          422, "UNSUPPORTED_SCOPE", "NOT_APPLIED", "Only the main Tag scope is supported");
    }
  }

  // Native metadata can contain values outside the dashboard's write contract. Project those
  // without rejecting a read or silently rounding an unsafe JSON integer.
  static TagRetentionInfo retention(Duration duration) {
    TagRetentionInfo info = new TagRetentionInfo();
    info.setModel("TAG_CREATION_AGE");
    info.setAgeLimit(duration == null ? "UNSET" : "SET");
    if (duration == null) {
      info.setState("KNOWN");
      return info;
    }
    try {
      long millis = duration.toMillis();
      if (millis >= -JSON_SAFE_INTEGER
          && millis <= JSON_SAFE_INTEGER
          && duration.equals(Duration.ofMillis(millis))) {
        info.setState("KNOWN");
        info.setMaxAgeMillis(millis);
      }
    } catch (ArithmeticException ignored) {
      // UNKNOWN retains the model and SET indication; do not fabricate a duration.
    }
    return info;
  }

  static String digest(String... values) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (String value : values) {
        byte[] b = value.getBytes(StandardCharsets.UTF_8);
        digest.update(java.nio.ByteBuffer.allocate(4).putInt(b.length).array());
        digest.update(b);
      }
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  static TableTagInfo view(PaimonTable owner, FileStoreTable table, String name, Tag tag) {
    TableTagInfo info = new TableTagInfo();
    info.setName(name);
    info.setScope(TagScope.main());
    info.setTarget(TableVersionRef.of(Long.toString(tag.id()), "main"));
    info.setSnapshotId(tag.id());
    info.setMinSnapshotsToKeep(0);
    info.setMaxSnapshotAgeMs(0L);
    info.setMaxRefAgeMs(0L);
    info.setVersionCommittedAtMillis(tag.timeMillis());
    info.setRetention(retention(tag.getTagTimeRetained()));
    // Native LocalDateTime has no offset. Never invent createdAt using snapshot time or mtime.
    info.setObservationToken(
        digest(owner.id().toString(), table.location().toString(), "main", name, tag.toJson()));
    return info;
  }

  static TagManager manager(FileStoreTable table) {
    return new TagManager(table.fileIO(), table.location(), "main", table.coreOptions());
  }
}
