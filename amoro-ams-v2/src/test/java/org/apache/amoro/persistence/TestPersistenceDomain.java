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

package org.apache.amoro.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.junit.jupiter.api.Test;

public class TestPersistenceDomain {

  @Test
  public void whitelistedTablesBindSuccessfully() {
    PersistenceDomain resource =
        new PersistenceDomain("resource", "amoro_resource", SerdeFormat.JSON);
    assertEquals("amoro_resource", resource.table());
    assertEquals("resource", resource.domainName());
    assertEquals(SerdeFormat.JSON, resource.serdeFormat());

    PersistenceDomain process =
        new PersistenceDomain("process", "amoro_process_v2", SerdeFormat.YAML);
    assertEquals(SerdeFormat.YAML, process.serdeFormat());

    // the enum overload binds the deployed single table directly
    PersistenceDomain viaEnum =
        new PersistenceDomain(
            "process", PersistenceDomain.Table.AMORO_PROCESS_V2, SerdeFormat.YAML);
    assertEquals("amoro_process_v2", viaEnum.table());
  }

  @Test
  public void unknownTableNameIsRejectedAtConstruction() {
    // table names feed MyBatis SQL, so only whitelisted enum-backed names are legal
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PersistenceDomain("evil", "amoro_process_v2; DROP TABLE users", SerdeFormat.JSON));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PersistenceDomain("evil", "appmanager", SerdeFormat.JSON));
  }

  @Test
  public void nullArgumentsAreRejected() {
    assertThrows(
        NullPointerException.class,
        () -> new PersistenceDomain(null, "amoro_resource", SerdeFormat.JSON));
    assertThrows(
        NullPointerException.class,
        () -> new PersistenceDomain("resource", (String) null, SerdeFormat.JSON));
    assertThrows(
        NullPointerException.class,
        () -> new PersistenceDomain("resource", "amoro_resource", null));
  }
}
