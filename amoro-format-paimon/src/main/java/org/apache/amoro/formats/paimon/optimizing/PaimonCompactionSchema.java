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

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** 在创建合并 writer 前，为已固定的输入选择兼容的 schema。 */
public final class PaimonCompactionSchema {
  private static final int MAX_REFRESH_ATTEMPTS = 3;

  private PaimonCompactionSchema() {}

  static Set<Long> fileSchemaIds(Collection<DataFileMeta> files) {
    return files.stream().map(DataFileMeta::schemaId).collect(Collectors.toSet());
  }

  static void validateRewriteSchema(
      FileStoreTable original, long requiredSchemaId, long outputSchemaId) {
    validateRewriteSchema(original, Collections.singleton(requiredSchemaId), outputSchemaId);
  }

  static void validateRewriteSchema(
      FileStoreTable original, Collection<Long> inputSchemaIds, long outputSchemaId) {
    long requiredSchemaId = requiredSchemaId(inputSchemaIds);
    if (requiredSchemaId <= original.schema().id()) {
      if (outputSchemaId != original.schema().id()) {
        throw new IllegalArgumentException("Rewrite output changed the bound writer schema");
      }
      return;
    }
    if (outputSchemaId < requiredSchemaId) {
      throw new IllegalArgumentException("Rewrite output schema is behind prepared inputs");
    }
    // Validate the immutable schema used by the output, not a later concurrent latest schema.
    FileStoreTable output = copyWithSchema(original, outputSchemaId);
    validateCompatibility(original, output);
    validateInputSchemas(original, output, inputSchemaIds);
  }

  public static FileStoreTable refresh(
      FileStoreTable original, long requiredSchemaId, long snapshotId) {
    return refresh(original, Collections.singleton(requiredSchemaId), snapshotId);
  }

  public static FileStoreTable refresh(
      FileStoreTable original, Collection<Long> inputSchemaIds, long snapshotId) {
    long requiredSchemaId = requiredSchemaId(inputSchemaIds);
    FileStoreTable current = original;
    int attempts = 0;
    try {
      // Paimon 1.4.2 CompactRefresher#tryRefresh 按实际文件最大 schemaId 更新 writer 以避免丢列。
      // paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CompactRefresher.java
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CompactRefresher.java#L61-L85
      while (current.schema().id() < requiredSchemaId && attempts < MAX_REFRESH_ATTEMPTS) {
        attempts++;
        current = original.copyWithLatestSchema();
        if (current.schema().id() != original.schema().id()) {
          validateCompatibility(original, current);
        }
      }
      if (current.schema().id() < requiredSchemaId) {
        throw new IllegalStateException("Schema remains behind prepared input files");
      }
      validateInputSchemas(original, current, inputSchemaIds);
      return current;
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Unsafe Paimon compaction schema: table="
              + original.name()
              + ", snapshot="
              + snapshotId
              + ", requiredSchemaId="
              + requiredSchemaId
              + ", currentSchemaId="
              + current.schema().id()
              + ", refreshAttempts="
              + attempts
              + "; replan with a fresh table after resolving schema compatibility",
          e);
    }
  }

  private static long requiredSchemaId(Collection<Long> schemaIds) {
    return schemaIds.stream().mapToLong(Long::longValue).max().orElse(-1L);
  }

  private static FileStoreTable copyWithSchema(FileStoreTable original, long schemaId) {
    TableSchema schema = original.schemaManager().schema(schemaId);
    if (schema == null) {
      throw new IllegalStateException("Missing Paimon compaction schema id=" + schemaId);
    }
    return original.copy(schema.copy(original.options()));
  }

  private static void validateInputSchemas(
      FileStoreTable original, FileStoreTable output, Collection<Long> inputSchemaIds) {
    // Paimon 1.4.2 RawFileSplitRead#createFileReader builds a mapping for each file schema ID.
    // The maximum ID chooses the writer version but can hide columns in another input version.
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java#L227-L245
    for (long schemaId : inputSchemaIds) {
      if (schemaId > original.schema().id() && schemaId != output.schema().id()) {
        FileStoreTable input = copyWithSchema(original, schemaId);
        validateCompatibility(original, input);
        validateCompatibility(input, output);
      }
    }
  }

  private static void validateCompatibility(FileStoreTable original, FileStoreTable refreshed) {
    TableSchema before = original.schema();
    TableSchema after = refreshed.schema();
    if (original.getClass() != refreshed.getClass()
        || original.bucketMode() != refreshed.bucketMode()
        || !before.partitionKeys().equals(after.partitionKeys())
        || !before.primaryKeys().equals(after.primaryKeys())
        || !before.bucketKeys().equals(after.bucketKeys())
        || !original.options().equals(refreshed.options())) {
      throw new IllegalStateException(
          "Table shape or execution options changed during schema refresh");
    }
    Map<Integer, DataField> fields = new HashMap<>();
    for (DataField field : after.fields()) {
      fields.put(field.id(), field);
    }
    // Paimon 1.4.2 SchemaEvolutionUtil#createIndexMapping 按 field id 映射。
    // 仅接受顶层新增/重排列；不在旧任务里推断 DROP、RENAME、类型或默认值变更的语义。
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/schema/SchemaEvolutionUtil.java#L79-L100
    for (DataField field : before.fields()) {
      DataField next = fields.get(field.id());
      if (next == null
          || !field.name().equals(next.name())
          || !field.type().equals(next.type())
          || !Objects.equals(field.defaultValue(), next.defaultValue())) {
        throw new IllegalStateException("Incompatible existing field id=" + field.id());
      }
    }
    // copyWithLatestSchema 保留执行选项；新增列若同时设置 sum，旧 options 缺该配置时会回退
    // last_non_null_value。必须比较两代持久化选项，不能把 Amoro runtime overrides 当作 DDL。
    // Paimon 1.4.2
    // AbstractFileStoreTable#copyWithLatestSchema、AggregateMergeFunction#getAggFuncName：
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java#L382-L391
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/aggregate/AggregateMergeFunction.java#L193-L202
    if (!original
        .schemaManager()
        .schema(before.id())
        .options()
        .equals(original.schemaManager().schema(after.id()).options())) {
      throw new IllegalStateException("Persisted table options changed across compaction schemas");
    }
  }
}
