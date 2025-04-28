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

package org.apache.amoro.maintainer.iceberg;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.optimizing.maintainer.IcebergTableMaintainerV2;
import org.apache.amoro.table.MixedTable;
import org.apache.iceberg.Table;

public class StandaloneMaintainerStarter {

  public static IcebergTableMaintainerV2 ofTable(AmoroTable<?> amoroTable) {
    TableFormat format = amoroTable.format();
    if (format.in(TableFormat.MIXED_HIVE, TableFormat.MIXED_ICEBERG)) {
      throw new RuntimeException("Unsupported table type" + amoroTable.originalTable().getClass());
    } else if (TableFormat.ICEBERG.equals(format)) {
      return new StandaloneIcebergMaintainer((Table) amoroTable.originalTable());
    } else {
      throw new RuntimeException("Unsupported table type" + amoroTable.originalTable().getClass());
    }
  }

  public static Table asIcebergTable(AmoroTable<?> amoroTable) {
    TableFormat format = amoroTable.format();
    if (TableFormat.ICEBERG.equals(format)) {
      return (Table) amoroTable.originalTable();
    }
    throw new RuntimeException("Unsupported table type" + amoroTable.originalTable().getClass());
  }

  public static MixedTable asMixedTable(AmoroTable<?> amoroTable) {
    TableFormat format = amoroTable.format();
    if (format.in(TableFormat.MIXED_HIVE, TableFormat.MIXED_ICEBERG)) {
      return (MixedTable) amoroTable.originalTable();
    }
    throw new RuntimeException("Unsupported table type" + amoroTable.originalTable().getClass());
  }
}
