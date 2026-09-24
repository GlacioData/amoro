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

package org.apache.amoro.formats.paimon;

import org.apache.amoro.TableRuntime;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

/** Schema-derived creation time, populated only by background metadata synchronization. */
public class PaimonTableCreationTime {
  public static final StateKey<PaimonTableCreationTime> KEY =
      StateKey.stateKey("paimon_table_creation_time")
          .jsonType(PaimonTableCreationTime.class)
          .defaultValue(new PaimonTableCreationTime())
          .withOptimisticConcurrency();

  private long createTime;
  private long schemaId;
  private String source;

  /** Must run in the Paimon table's authentication context, never on a dashboard request. */
  public static PaimonTableCreationTime read(FileStoreTable table) {
    SchemaManager manager = table.schemaManager();
    long firstId =
        manager.schemaExists(0)
            ? 0
            : manager.listAllIds().stream()
                .mapToLong(Long::longValue)
                .min()
                .orElseThrow(() -> new IllegalStateException("No retained Paimon schema"));
    TableSchema schema = manager.schema(firstId);
    PaimonTableCreationTime result = new PaimonTableCreationTime();
    result.schemaId = firstId;
    result.createTime = schema.timeMillis();
    result.source = "SCHEMA_TIME";
    if (result.createTime <= 0) {
      // Paimon 1.4.2 SchemaManager.earliestCreationTime uses the earliest schema file's mtime.
      // Source:
      // https://paimon.apache.org/docs/master/api/java/org/apache/paimon/schema/SchemaManager.html
      result.createTime = manager.earliestCreationTime();
      result.source = "SCHEMA_FILE_MTIME";
    }
    if (result.createTime <= 0) {
      throw new IllegalStateException("Paimon schema creation time is unavailable");
    }
    return result;
  }

  /** Reads only the runtime's already restored in-memory state. */
  public static void project(TableRuntime runtime, ServerTableMeta target) {
    if (runtime != null) {
      PaimonTableCreationTime state = runtime.getState(KEY);
      if (state != null && state.createTime > 0) {
        target.setCreateTime(state.createTime);
      }
    }
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getSchemaId() {
    return schemaId;
  }

  public void setSchemaId(long schemaId) {
    this.schemaId = schemaId;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }
}
