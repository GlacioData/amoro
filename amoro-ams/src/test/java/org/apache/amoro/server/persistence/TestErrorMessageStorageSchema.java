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

package org.apache.amoro.server.persistence;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class TestErrorMessageStorageSchema {

  @Test
  public void testMySqlErrorColumnsAndUpgradeUseLongText() throws IOException {
    String initSql = normalizedResource("mysql/ams-mysql-init.sql");
    Assertions.assertTrue(initSql.contains("`fail_message` longtext character set utf8mb4"));
    Assertions.assertEquals(
        3, occurrences(initSql, "`fail_reason` longtext character set utf8mb4"));

    String upgradeSql = normalizedResource("mysql/upgrade.sql");
    Assertions.assertTrue(
        upgradeSql.contains("modify column `fail_message` longtext character set utf8mb4"));
    Assertions.assertEquals(
        3, occurrences(upgradeSql, "modify column `fail_reason` longtext character set utf8mb4"));
  }

  @Test
  public void testPostgresErrorColumnsAndUpgradeUseText() throws IOException {
    String initSql = normalizedResource("postgres/ams-postgres-init.sql");
    Assertions.assertEquals(3, occurrences(initSql, "fail_reason text"));

    String upgradeSql = normalizedResource("postgres/upgrade.sql");
    Assertions.assertEquals(3, occurrences(upgradeSql, "alter column fail_reason type text"));
  }

  private static String normalizedResource(String name) throws IOException {
    try (InputStream inputStream =
        TestErrorMessageStorageSchema.class.getClassLoader().getResourceAsStream(name)) {
      Assertions.assertNotNull(inputStream, "Missing test resource " + name);
      byte[] buffer = new byte[8_192];
      ByteArrayOutputStream content = new ByteArrayOutputStream();
      int read;
      while ((read = inputStream.read(buffer)) >= 0) {
        content.write(buffer, 0, read);
      }
      return new String(content.toByteArray(), StandardCharsets.UTF_8)
          .toLowerCase()
          .replaceAll("\\s+", " ");
    }
  }

  private static int occurrences(String value, String expected) {
    int count = 0;
    int fromIndex = 0;
    while ((fromIndex = value.indexOf(expected, fromIndex)) >= 0) {
      count++;
      fromIndex += expected.length();
    }
    return count;
  }
}
