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

package org.apache.amoro.process.trigger.simulated;

import org.apache.amoro.process.trigger.ManagedTablePort;

/** Single immutable simulated table fact shared by REST resolution and scheduled scanning. */
public final class SimulatedProcessFixture {
  public static final String CATALOG = "simulated";
  public static final String DATABASE = "demo";
  public static final String TABLE = "table";
  public static final String TABLE_ID = "simulated-demo-table";
  public static final String TABLE_FORMAT = "simulated";

  private SimulatedProcessFixture() {}

  public static boolean matches(String catalog, String database, String table) {
    return CATALOG.equals(catalog) && DATABASE.equals(database) && TABLE.equals(table);
  }

  public static ManagedTablePort.TableSnapshot tableSnapshot() {
    return new ManagedTablePort.TableSnapshot(
        CATALOG, DATABASE, TABLE, TABLE_ID, TABLE_FORMAT, "1970-01-01T00:00:00Z");
  }
}
