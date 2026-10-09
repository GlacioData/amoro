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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Collections;

class TestPaimonFixedBucketRewriteSchema {

  // Paimon ac223f47, schema/SchemaManager.java:252-275 persists immutable schema versions;
  // table/AbstractFileStoreTable.java:382-393 preserves execution options during refresh.
  @ParameterizedTest
  @CsvSource({"false, false", "false, true", "true, false", "true, true"})
  void acceptsCompatibleOutputAtOrAfterRequiredWithoutFollowingLaterDdl(
      boolean newerOutput, boolean runtimeOverride, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      FileStoreTable bound =
          runtimeOverride
              ? fixture.table.copy(Collections.singletonMap("target-file-size", "32 mb"))
              : fixture.table;
      SchemaManager manager = bound.schemaManager();
      TableSchema required =
          manager.commitChanges(
              SchemaChange.addColumn("added", DataTypes.INT()),
              SchemaChange.updateColumnPosition(SchemaChange.Move.last("seq")));
      TableSchema output =
          newerOutput
              ? manager.commitChanges(SchemaChange.addColumn("later_added", DataTypes.STRING()))
              : required;
      manager.commitChanges(SchemaChange.setOption("file-index.range-bitmap.columns", "payload"));

      assertTrue(manager.latest().get().id() > output.id());
      assertDoesNotThrow(
          () -> PaimonCompactionSchema.validateRewriteSchema(bound, required.id(), output.id()));
      assertEquals(0L, bound.schema().id());
      assertEquals(runtimeOverride ? "32 mb" : "64 mb", bound.options().get("target-file-size"));
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, 1L})
  void rejectsOutputBehindThePreparedInputSchema(long outputOffset, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      SchemaManager manager = fixture.table.schemaManager();
      manager.commitChanges(SchemaChange.addColumn("first_added", DataTypes.INT()));
      TableSchema required =
          manager.commitChanges(SchemaChange.addColumn("second_added", DataTypes.INT()));
      long outputSchemaId = fixture.table.schema().id() + outputOffset;

      assertTrue(outputSchemaId < required.id());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PaimonCompactionSchema.validateRewriteSchema(
                  fixture.table, required.id(), outputSchemaId));
    }
  }

  // Paimon ac223f47, schema/SchemaManager.java:1091-1092 and 1329-1343 loads the requested
  // schema file. Missing immutable metadata must not silently fall back to latest or bound.
  @ParameterizedTest
  @ValueSource(strings = {"output", "bound", "required"})
  void rejectsMissingPersistedSchemaFiles(String missing, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      SchemaManager manager = fixture.table.schemaManager();
      TableSchema required =
          manager.commitChanges(SchemaChange.addColumn("added", DataTypes.INT()));
      TableSchema output =
          manager.commitChanges(SchemaChange.addColumn("later_added", DataTypes.INT()));
      long missingId =
          "output".equals(missing)
              ? output.id()
              : "required".equals(missing) ? required.id() : fixture.table.schema().id();
      manager.deleteSchema(missingId);

      assertFalse(manager.schemaExists(missingId));
      RuntimeException failure =
          assertThrows(
              RuntimeException.class,
              () ->
                  PaimonCompactionSchema.validateRewriteSchema(
                      fixture.table, required.id(), output.id()));
      assertTrue(failure.getCause() instanceof FileNotFoundException);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "target-file-size, 32 mb",
    "bucket-append-ordered, false",
    "file-index.range-bitmap.columns, payload"
  })
  void rejectsPersistedExecutionOptionDriftDespiteBoundOptionCopies(
      String option, String value, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      TableSchema output =
          fixture
              .table
              .schemaManager()
              .commitChanges(
                  SchemaChange.addColumn("added", DataTypes.INT()),
                  SchemaChange.setOption(option, value));

      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  PaimonCompactionSchema.validateRewriteSchema(
                      fixture.table, output.id(), output.id()));

      assertTrue(failure.getMessage().contains("Persisted table options changed"));
    }
  }

  // Paimon ac223f47, schema/SchemaEvolutionUtil.java:79-100 maps by field ID. Amoro's
  // compatibility boundary additionally rejects changed name, type, default or missing fields.
  @ParameterizedTest
  @ValueSource(strings = {"drop", "rename", "type", "default"})
  void rejectsIncompatibleExistingFieldsInTheOutputSchema(String change, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      TableSchema output =
          fixture
              .table
              .schemaManager()
              .commitChanges(
                  SchemaChange.addColumn("added", DataTypes.INT()), incompatibleChange(change));

      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  PaimonCompactionSchema.validateRewriteSchema(
                      fixture.table, output.id(), output.id()));

      assertTrue(failure.getMessage().contains("Incompatible existing field"));
    }
  }

  // Paimon ac223f47, schema/SchemaEvolutionUtil.java:88-95 maps absent fields to NULL_FIELD_INDEX.
  // Compatibility with the stale bound schema alone cannot protect a column present in inputs.
  @Test
  void rejectsNewerOutputThatDropsAColumnPresentInTheRequiredInputSchema(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      SchemaManager manager = fixture.table.schemaManager();
      TableSchema required =
          manager.commitChanges(SchemaChange.addColumn("input_added", DataTypes.INT()));
      TableSchema output = manager.commitChanges(SchemaChange.dropColumn("input_added"));

      assertTrue(required.fieldNames().contains("input_added"));
      assertTrue(output.id() > required.id());
      assertThrows(
          IllegalStateException.class,
          () ->
              PaimonCompactionSchema.validateRewriteSchema(
                  fixture.table, required.id(), output.id()));
    }
  }

  @Test
  void refreshRejectsLatestSchemaThatDropsAColumnPresentInTheRequiredInputs(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      SchemaManager manager = fixture.table.schemaManager();
      TableSchema required =
          manager.commitChanges(SchemaChange.addColumn("input_added", DataTypes.INT()));
      TableSchema latest = manager.commitChanges(SchemaChange.dropColumn("input_added"));

      assertTrue(latest.id() > required.id());
      assertFalse(latest.fieldNames().contains("input_added"));
      assertThrows(
          IllegalStateException.class,
          () -> PaimonCompactionSchema.refreshFixed(fixture.table, required.id(), 1L));
    }
  }

  private static SchemaChange incompatibleChange(String change) {
    switch (change) {
      case "drop":
        return SchemaChange.dropColumn("seq");
      case "rename":
        return SchemaChange.renameColumn("seq", "renamed_seq");
      case "type":
        return SchemaChange.updateColumnType("seq", DataTypes.BIGINT());
      case "default":
        return SchemaChange.updateColumnDefaultValue(new String[] {"seq"}, "7");
      default:
        throw new IllegalArgumentException("Unknown schema change: " + change);
    }
  }
}
