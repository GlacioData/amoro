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

package org.apache.amoro.server.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.amoro.server.dashboard.component.reverser.IcebergTableMetaExtract;
import org.apache.amoro.table.descriptor.TableMetaExtract.InternalSchema;
import org.apache.amoro.table.descriptor.TableMetaExtract.InternalTableMeta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class TestIcebergSchemaDisplay {
  @TempDir Path warehouse;

  @Test
  void retainsNestedSchemaAndDecimalPrecisionWithoutTerminal() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get(), "business id"),
            Types.NestedField.optional(2, "amount", Types.DecimalType.of(20, 8)),
            Types.NestedField.optional(
                3,
                "profile",
                Types.StructType.of(
                    Types.NestedField.optional(4, "name", Types.StringType.get()),
                    Types.NestedField.required(5, "active", Types.BooleanType.get()))),
            Types.NestedField.optional(
                6, "tags", Types.ListType.ofOptional(7, Types.StringType.get())),
            Types.NestedField.optional(
                8,
                "attributes",
                Types.MapType.ofOptional(
                    9, 10, Types.StringType.get(), Types.DecimalType.of(18, 4))));
    // Real local table metadata:
    // https://iceberg.apache.org/javadoc/1.6.1/org/apache/iceberg/hadoop/HadoopTables.html
    Table table =
        new HadoopTables()
            .create(
                schema,
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                Collections.singletonMap("comment", "schema regression"),
                warehouse.resolve("nested").toString());
    List<InternalTableMeta> history = new IcebergTableMetaExtract().extractTable(table);
    assertEquals(1, history.size());
    assertEquals("schema regression", history.get(0).getProperties().get("comment"));
    Map<String, InternalSchema> fields =
        history.get(0).getInternalSchema().stream()
            .collect(Collectors.toMap(InternalSchema::getName, Function.identity()));
    assertEquals(10, fields.size());
    assertField(fields, table.schema(), "id", null, "bigint", true);
    assertEquals("business id", fields.get("id").getComment());
    assertField(fields, table.schema(), "amount", null, "decimal(20,8)", false);
    assertField(
        fields, table.schema(), "profile", null, "struct<name:string,active:boolean>", false);
    assertField(fields, table.schema(), "profile.name", "profile", "string", false);
    assertField(fields, table.schema(), "profile.active", "profile", "boolean", true);
    assertField(fields, table.schema(), "tags", null, "array<string>", false);
    assertField(fields, table.schema(), "tags.element", "tags", "string", false);
    assertField(fields, table.schema(), "attributes", null, "map<string,decimal(18,4)>", false);
    assertField(fields, table.schema(), "attributes.key", "attributes", "string", false);
    assertField(fields, table.schema(), "attributes.value", "attributes", "decimal(18,4)", false);
  }

  private static void assertField(
      Map<String, InternalSchema> fields,
      Schema schema,
      String name,
      String parent,
      String type,
      boolean required) {
    InternalSchema field = fields.get(name);
    assertEquals(schema.findField(name).fieldId(), field.getId(), name);
    assertEquals(type, field.getType(), name);
    assertEquals(required, field.isRequired(), name);
    if (parent == null) {
      assertNull(field.getParentId(), name);
    } else {
      assertEquals(Integer.valueOf(schema.findField(parent).fieldId()), field.getParentId(), name);
    }
  }
}
