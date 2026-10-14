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

import static org.apache.amoro.formats.paimon.tag.PaimonTagReader.digest;
import static org.apache.amoro.formats.paimon.tag.PaimonTagReader.manager;
import static org.apache.amoro.formats.paimon.tag.PaimonTagReader.retention;
import static org.apache.amoro.formats.paimon.tag.PaimonTagReader.validateScope;
import static org.apache.amoro.formats.paimon.tag.PaimonTagReader.view;

import org.apache.amoro.formats.paimon.PaimonSnapshotMetadata;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.amoro.table.tag.CopyTagRequest;
import org.apache.amoro.table.tag.CreateTagRequest;
import org.apache.amoro.table.tag.DeleteTagRequest;
import org.apache.amoro.table.tag.TableTagInfo;
import org.apache.amoro.table.tag.TableTagOperations;
import org.apache.amoro.table.tag.TableVersionRef;
import org.apache.amoro.table.tag.TagDeleteResult;
import org.apache.amoro.table.tag.TagOperationException;
import org.apache.amoro.table.tag.TagRetentionInfo;
import org.apache.amoro.table.tag.TagRetentionRequest;
import org.apache.amoro.table.tag.TagScope;
import org.apache.amoro.table.tag.TagWriteCapabilities;
import org.apache.amoro.table.tag.UpdateTagRetentionRequest;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Paimon Tag adapter using native table APIs independently of Amoro optimizing configuration. JVM
 * locks and observation checks do not coordinate external writers. Source:
 * https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java
 */
public final class PaimonTagOperations implements TableTagOperations {
  private static final long JSON_SAFE_INTEGER = 9007199254740991L;
  private static final ReentrantLock[] LOCKS = new ReentrantLock[128];

  static {
    for (int i = 0; i < LOCKS.length; i++) {
      LOCKS[i] = new ReentrantLock();
    }
  }

  private final PaimonTable owner;

  private final Supplier<Table> refresh;
  private final boolean remoteCatalog;

  public PaimonTagOperations(PaimonTable owner, Supplier<Table> refresh, boolean remoteCatalog) {
    this.owner = owner;
    this.refresh = refresh;
    this.remoteCatalog = remoteCatalog;
  }

  @Override
  public TagWriteCapabilities capabilities() {
    return owner.doAs(() -> capabilities(fresh()));
  }

  private FileStoreTable fresh() {
    Table loaded = refresh == null ? owner.originalTable() : refresh.get();
    if (!(loaded instanceof FileStoreTable)) {
      throw TagOperationException.unsupported("TABLE_SHAPE");
    }
    return (FileStoreTable) loaded;
  }

  private TagWriteCapabilities capabilities(FileStoreTable table) {
    TagWriteCapabilities result = new TagWriteCapabilities();
    result.setSupportedScopes(Collections.singletonList(TagScope.main()));
    result.setDeleteEffect("MAY_CLEAN_HISTORY");
    result.setPreconditionGuarantee("OBSERVATION_CHECK");
    result.getRetention().setModel("TAG_CREATION_AGE");
    result.getRetention().setUpdateClock("RESET_ON_UPDATE");
    Duration ttl = table.coreOptions().tagDefaultTimeRetained();
    String reason = unavailable(table);
    if (reason == null && !validWriteRetention(ttl)) {
      reason = "INVALID_NATIVE_RETENTION";
    }
    result.setUnavailableReason(reason);
    if (reason == null) {
      result.setActions(
          new LinkedHashSet<>(Arrays.asList("CREATE", "COPY", "DELETE", "UPDATE_RETENTION")));
    }
    TagRetentionInfo retention = retention(ttl);
    result.getRetention().setNativeDefault(retention);
    result.getRetention().setConfigObservationToken(configToken(table, ttl));
    result
        .getRetention()
        .setSupportedModes(
            new LinkedHashSet<>(Arrays.asList("NATIVE_DEFAULT", "AGE_LIMIT", "NO_AGE_LIMIT")));
    return result;
  }

  private String unavailable(FileStoreTable table) {
    if (!"1.4.2".equals(version())) {
      return "UNVERIFIED_PAIMON_VERSION";
    }
    if (refresh == null || remoteCatalog) {
      return "UNSUPPORTED_CATALOG";
    }
    String scheme = table.location().toUri().getScheme();
    if (scheme != null && !"file".equals(scheme) && !"hdfs".equals(scheme)) {
      return "UNVERIFIED_STORAGE";
    }
    if (!table.coreOptions().writeOnly()) {
      return "WRITE_ONLY_REQUIRED";
    }
    // Time-based expiry configures native maintenance, not the manual Tag API prerequisites.
    // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/tag/TagAutoManager.java
    if (table.coreOptions().tagCreationMode() != CoreOptions.TagCreationMode.NONE) {
      return "AUTOMATIC_TAG_MAINTENANCE_ENABLED";
    }
    if (!"main".equals(table.coreOptions().branch())) {
      return "UNSUPPORTED_SCOPE";
    }
    return null;
  }

  static String version() {
    try (InputStream in =
        FileStoreTable.class
            .getClassLoader()
            .getResourceAsStream("META-INF/maven/org.apache.paimon/paimon-bundle/pom.properties")) {
      if (in == null) {
        return "UNKNOWN";
      }
      Properties p = new Properties();
      p.load(in);
      return p.getProperty("version");
    } catch (IOException e) {
      return "UNKNOWN";
    }
  }

  static void name(String name) {
    if (name == null) {
      throw TagOperationException.invalid("Tag name is required");
    }
    try {
      PaimonSnapshotMetadata.ref(name);
    } catch (SnapshotReadException invalid) {
      throw TagOperationException.invalid(
          "Tag name must be a safe path component (maximum 255 characters)");
    }
  }

  private static void safePath(TagManager manager, String name) {
    Path path = manager.tagPath(name);
    if (!path.getParent().equals(manager.tagPath("placeholder").getParent())
        || !path.getName().equals("tag-" + name)) {
      throw TagOperationException.invalid(
          "Tag path must be a direct child of the native Tag directory");
    }
  }

  private static void boundedTagCount(TagManager manager) throws IOException {
    if (manager.tagPaths(path -> true).size() > 100) {
      throw new TagOperationException(
          503,
          "UNAVAILABLE",
          "NOT_APPLIED",
          "Historical Tag traversal exceeds the synchronous operation budget");
    }
  }

  private static boolean validWriteRetention(Duration duration) {
    if (duration == null) {
      return true;
    }
    try {
      long millis = duration.toMillis();
      return millis >= 1
          && millis <= JSON_SAFE_INTEGER
          && duration.equals(Duration.ofMillis(millis));
    } catch (ArithmeticException ignored) {
      return false;
    }
  }

  private Duration duration(
      FileStoreTable table, TagRetentionRequest request, String model, String config) {
    if (!"TAG_CREATION_AGE".equals(model)) {
      throw new TagOperationException(
          412,
          "PRECONDITION_FAILED",
          "NOT_APPLIED",
          "Retention model changed; refresh and confirm again");
    }
    if (request == null || request.getMode() == null) {
      throw TagOperationException.invalid("Retention mode is required");
    }
    switch (request.getMode()) {
      case "NATIVE_DEFAULT":
        if (request.getMaxAgeMillis() != null) {
          throw TagOperationException.invalid("Default retention does not accept maxAgeMillis");
        }
        Duration ttl = table.coreOptions().tagDefaultTimeRetained();
        if (!validWriteRetention(ttl)) {
          throw TagOperationException.invalid("Tag retention is outside the supported range");
        }
        checkToken(config, configToken(table, ttl));
        return ttl;
      case "NO_AGE_LIMIT":
        if (request.getMaxAgeMillis() != null) {
          throw TagOperationException.invalid("No-age-limit does not accept maxAgeMillis");
        }
        return null;
      case "AGE_LIMIT":
        Long ms = request.getMaxAgeMillis();
        if (ms == null || ms < 1 || ms > JSON_SAFE_INTEGER) {
          throw TagOperationException.invalid("maxAgeMillis must be a positive JSON-safe integer");
        }
        return Duration.ofMillis(ms);
      default:
        throw new TagOperationException(
            422, "UNSUPPORTED_RETENTION", "NOT_APPLIED", "Unsupported retention mode");
    }
  }

  private String configToken(FileStoreTable table, Duration ttl) {
    return digest(
        owner.id().toString(),
        table.location().toString(),
        Long.toString(table.schema().id()),
        "main",
        String.valueOf(ttl));
  }

  static void checkToken(String expected, String actual) {
    if (expected == null || expected.isEmpty()) {
      throw new TagOperationException(
          428,
          "PRECONDITION_REQUIRED",
          "NOT_APPLIED",
          "Refresh metadata before confirming this operation");
    }
    if (!expected.equals(actual)) {
      throw new TagOperationException(
          412, "PRECONDITION_FAILED", "NOT_APPLIED", "Metadata changed; refresh and confirm again");
    }
  }

  private Tag required(TagManager manager, String name) {
    safePath(manager, name);
    return manager
        .get(name)
        .orElseThrow(
            () ->
                new TagOperationException(
                    404, "TAG_NOT_FOUND", "NOT_APPLIED", "Tag no longer exists"));
  }

  @Override
  public TableTagInfo createTag(CreateTagRequest request) {
    if (request == null) {
      throw TagOperationException.invalid("Tag request is required");
    }
    name(request.getName());
    validateScope(request.getScope());
    TableVersionRef target = request.getTarget();
    if (target == null
        || target.getId() == null
        || target.getBranch() != null && !"main".equals(target.getBranch())) {
      throw TagOperationException.invalid("A main Snapshot target is required");
    }
    final long id;
    try {
      id = Long.parseLong(target.getId());
    } catch (NumberFormatException e) {
      throw TagOperationException.invalid("Paimon Snapshot ID must be a positive long");
    }
    // parseLong accepts aliases such as +1 and 01; do not publish a receipt with a different ID.
    // https://docs.oracle.com/javase/8/docs/api/java/lang/Long.html#parseLong-java.lang.String-
    if (id < 1 || !Long.toString(id).equals(target.getId())) {
      throw TagOperationException.invalid("Paimon Snapshot ID must be a canonical positive long");
    }
    return mutate(
        (table, manager, write) -> {
          Duration ttl =
              duration(
                  table,
                  request.getRetention(),
                  request.getExpectedRetentionModel(),
                  request.getExpectedRetentionConfigToken());
          Snapshot snapshot;
          try {
            snapshot = table.snapshotManager().copyWithBranch("main").tryGetSnapshot(id);
          } catch (java.io.FileNotFoundException expired) {
            snapshot = null;
          }
          if (snapshot == null) {
            boundedTagCount(manager);
            snapshot =
                manager.tags().keySet().stream()
                    .filter(candidate -> candidate.id() == id)
                    .findFirst()
                    .orElse(null);
          }
          if (snapshot == null) {
            throw new TagOperationException(
                404, "VERSION_NOT_FOUND", "NOT_APPLIED", "Snapshot no longer exists");
          }
          return create(table, manager, write, snapshot, request.getName(), ttl);
        });
  }

  @Override
  public TableTagInfo copyTag(CopyTagRequest request) {
    if (request == null) {
      throw TagOperationException.invalid("Tag request is required");
    }
    name(request.getSourceName());
    name(request.getNewName());
    validateScope(request.getScope());
    return mutate(
        (table, manager, write) -> {
          Tag source = required(manager, request.getSourceName());
          checkToken(
              request.getExpectedSourceToken(),
              view(owner, table, request.getSourceName(), source).getObservationToken());
          Duration ttl =
              duration(
                  table,
                  request.getRetention(),
                  request.getExpectedRetentionModel(),
                  request.getExpectedRetentionConfigToken());
          return create(table, manager, write, source.trimToSnapshot(), request.getNewName(), ttl);
        });
  }

  private TableTagInfo create(
      FileStoreTable table,
      TagManager manager,
      NativeWrite write,
      Snapshot snapshot,
      String name,
      Duration ttl) {
    safePath(manager, name);
    if (manager.tagExists(name)) {
      throw new TagOperationException(
          409, "TAG_ALREADY_EXISTS", "NOT_APPLIED", "Tag name already exists");
    }
    // The explicit Duration overload preserves null as no age limit, including when a default
    // exists. Native findSnapshot also resolves snapshots retained only by a Tag.
    // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java
    write.run(() -> table.createTag(name, snapshot.id(), ttl));
    return verified(table, manager, name, snapshot, ttl, null);
  }

  @Override
  public TagDeleteResult deleteTag(DeleteTagRequest request) {
    if (request == null) {
      throw TagOperationException.invalid("Tag request is required");
    }
    name(request.getName());
    validateScope(request.getScope());
    return mutate(
        (table, manager, write) -> {
          Tag tag = required(manager, request.getName());
          checkToken(
              request.getExpectedToken(),
              view(owner, table, request.getName(), tag).getObservationToken());
          boolean noCleanup =
              table.snapshotManager().copyWithBranch("main").snapshotExists(tag.id());
          if (!noCleanup) {
            boundedTagCount(manager);
            noCleanup =
                manager.tags().values().stream()
                    .anyMatch(names -> names.contains(request.getName()) && names.size() > 1);
          }
          write.run(() -> table.deleteTag(request.getName()));
          // Native deleteQuietly can swallow a failed reference deletion. Never report success
          // unless the reference is absent; native history cleanup remains best effort.
          if (manager.tagExists(request.getName())) {
            throw new TagOperationException(
                500, "OUTCOME_UNKNOWN", "UNKNOWN", "Tag deletion could not be verified");
          }
          TagDeleteResult result = new TagDeleteResult();
          result.setName(request.getName());
          result.setScope(TagScope.main());
          result.setHistoryCleanup(noCleanup ? "NOT_REQUIRED" : "BEST_EFFORT_UNVERIFIED");
          return result;
        });
  }

  @Override
  public TableTagInfo updateTagRetention(UpdateTagRetentionRequest request) {
    if (request == null) {
      throw TagOperationException.invalid("Tag request is required");
    }
    name(request.getName());
    validateScope(request.getScope());
    return mutate(
        (table, manager, write) -> {
          if (!capabilities(table).getActions().contains("UPDATE_RETENTION")) {
            throw TagOperationException.unsupported("UPDATE_RETENTION");
          }
          checkToken(request.getExpectedUpdateClock(), "RESET_ON_UPDATE");
          Tag source = required(manager, request.getName());
          checkToken(
              request.getExpectedToken(),
              view(owner, table, request.getName(), source).getObservationToken());
          Duration ttl =
              duration(
                  table,
                  request.getRetention(),
                  request.getExpectedRetentionModel(),
                  request.getExpectedRetentionConfigToken());
          // Passing null for the Snapshot ID selects latest in Paimon. Always keep the observed
          // Tag's ID; the SDK resolves retained snapshots and owns TTL, publication and callbacks.
          // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java
          write.run(() -> table.replaceTag(request.getName(), source.id(), ttl));
          return verified(table, manager, request.getName(), source.trimToSnapshot(), ttl, source);
        });
  }

  private TableTagInfo verified(
      FileStoreTable table,
      TagManager manager,
      String name,
      Snapshot snapshot,
      Duration ttl,
      Tag previous) {
    Tag actual = required(manager, name);
    if (!snapshot.toJson().equals(actual.trimToSnapshot().toJson())
        || !Objects.equals(ttl, actual.getTagTimeRetained())
        || (ttl == null ? actual.getTagCreateTime() != null : actual.getTagCreateTime() == null)
        || (ttl != null
            && previous != null
            && Objects.equals(previous.getTagCreateTime(), actual.getTagCreateTime()))) {
      throw new TagOperationException(
          500, "PARTIAL_FAILURE", "APPLIED_WITH_ERRORS", "Native Tag result could not be verified");
    }
    return view(owner, table, name, actual);
  }

  /** Tracks the SDK call boundary, without intercepting its file I/O or side effects. */
  private static final class NativeWrite {
    private boolean started;
    private boolean returned;

    private void run(Runnable operation) {
      started = true;
      operation.run();
      returned = true;
    }
  }

  private interface Mutation<T> {
    T run(FileStoreTable table, TagManager manager, NativeWrite write) throws Exception;
  }

  private <T> T mutate(Mutation<T> mutation) {
    return owner.doAs(
        () -> {
          FileStoreTable initial = fresh();
          String reason = unavailable(initial);
          if (reason != null) {
            throw new TagOperationException(
                422,
                "UNSUPPORTED_OPERATION",
                "NOT_APPLIED",
                "Tag writing is unavailable: " + reason);
          }
          String identity = initial.location().toString();
          ReentrantLock lock = LOCKS[(identity.hashCode() & Integer.MAX_VALUE) % LOCKS.length];
          if (!lock.tryLock()) {
            throw new TagOperationException(
                429, "BUSY", "NOT_APPLIED", "Another Tag mutation is running");
          }
          NativeWrite write = new NativeWrite();
          try {
            FileStoreTable table = fresh();
            if (!identity.equals(table.location().toString()) || unavailable(table) != null) {
              throw new TagOperationException(
                  409,
                  "CAPABILITY_CHANGED",
                  "NOT_APPLIED",
                  "Table capability changed; refresh before retrying");
            }
            return mutation.run(table, manager(table), write);
          } catch (TagOperationException typed) {
            if (write.started && "NOT_APPLIED".equals(typed.getOutcome())) {
              throw failure(write, typed);
            }
            throw typed;
          } catch (Exception failure) {
            throw failure(write, failure);
          } finally {
            lock.unlock();
          }
        });
  }

  private static TagOperationException failure(NativeWrite write, Exception cause) {
    if (!write.started) {
      return new TagOperationException(
          503, "READ_FAILED", "NOT_APPLIED", "Tag metadata could not be read", cause);
    }
    return new TagOperationException(
        500,
        write.returned ? "PARTIAL_FAILURE" : "OUTCOME_UNKNOWN",
        write.returned ? "APPLIED_WITH_ERRORS" : "UNKNOWN",
        "Tag operation could not be confirmed; refresh metadata before taking further action",
        cause);
  }
}
