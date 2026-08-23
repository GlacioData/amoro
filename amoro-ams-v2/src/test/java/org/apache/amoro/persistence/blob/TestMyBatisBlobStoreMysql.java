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

package org.apache.amoro.persistence.blob;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.persistence.PersistenceDomain;
import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.persistence.exception.ResourceAlreadyExists;
import org.apache.amoro.test.IsolatedMysql;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real-MySQL integration (framework spec §8/§9, tag {@code docker-mysql}, activated by {@code
 * -Pdocker-it}). The class owns an isolated database inside the suite's disposable MySQL 5.7
 * container and never connects to or cleans a fixed host database.
 */
@Tag("docker-mysql")
@ExtendWith(IsolatedMysql.class)
@Timeout(120)
public class TestMyBatisBlobStoreMysql {

  private static final String MYSQL_DATABASE = "amoro_blob_store_e2e";

  private static final List<SqlSession> SESSIONS = new ArrayList<>();
  private static SqlSessionFactory sqlFactory;
  private static MyBatisBlobStore resourceStore;
  private static MyBatisBlobStore processStore;

  @BeforeAll
  public static void initializeIsolatedSchema() {
    IsolatedMysql.initializeControlPlane(MYSQL_DATABASE);
    IsolatedMysql.initializeGenericResourceDomain(MYSQL_DATABASE);
    sqlFactory = IsolatedMysql.sqlSessionFactory(MYSQL_DATABASE, "blob-store-testcontainer");
    resourceStore =
        store(
            new PersistenceDomain(
                "resource",
                PersistenceDomain.Table.AMORO_RESOURCE.physicalName(),
                SerdeFormat.JSON));
    processStore = store(new PersistenceDomain("process", "amoro_process_v2", SerdeFormat.YAML));
  }

  @AfterAll
  public static void closeSessions() {
    for (SqlSession session : SESSIONS) {
      session.close();
    }
  }

  private static MyBatisBlobStore store(PersistenceDomain domain) {
    SqlSession session = sqlFactory.openSession(true);
    SESSIONS.add(session);
    return new MyBatisBlobStore(domain, session.getMapper(ResourceBlobMapper.class));
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  // ------------------------------------------------------------------ five SQL semantics

  @Test
  public void insertFindUpdateDeleteOnRealMysql() {
    resourceStore.insert("fake", "r1", bytes("{\"doc\":\"one\"}"));
    assertArrayEquals(bytes("{\"doc\":\"one\"}"), resourceStore.find("fake", "r1").orElse(null));

    assertTrue(resourceStore.update("fake", "r1", bytes("{\"doc\":\"two\"}")));
    assertArrayEquals(bytes("{\"doc\":\"two\"}"), resourceStore.find("fake", "r1").orElse(null));

    assertTrue(resourceStore.delete("fake", "r1"));
    assertEquals(Optional.empty(), resourceStore.find("fake", "r1"));
    assertFalse(resourceStore.delete("fake", "r1"));
    assertFalse(resourceStore.update("fake", "ghost", bytes("x")));
  }

  @Test
  public void duplicateInsertTranslatesToResourceAlreadyExists() {
    resourceStore.insert("fake", "dup", bytes("a"));
    assertThrows(
        ResourceAlreadyExists.class, () -> resourceStore.insert("fake", "dup", bytes("b")));
  }

  @Test
  public void mediumTextSurvivesBase64OfAFullSizeDocument() {
    // a 64KiB raw JSON document Base64-encodes to ~87KB: proves MEDIUMTEXT (TEXT would truncate)
    StringBuilder json = new StringBuilder("{\"padding\":\"");
    while (json.length() < 64 * 1024) {
      json.append("abcdefghijklmnopqrstuvwxyz0123456789");
    }
    json.append("\"}");
    byte[] document = bytes(json.toString());
    assertTrue(
        Base64.getEncoder().encodeToString(document).length() > 65_535,
        "the encoded value must exceed the TEXT limit for the test to mean anything");

    resourceStore.insert("fake", "big", document);
    assertArrayEquals(document, resourceStore.find("fake", "big").orElse(null));
  }

  @Test
  public void forEachScansAndRestartReloadsExactlyTheDurableBytes() throws Exception {
    // a dedicated collection keeps the scan independent of rows left by sibling tests
    final String collection = "scan-it";
    resourceStore.insert(collection, "reload", bytes("{\"v\":7}"));

    AtomicInteger seen = new AtomicInteger();
    resourceStore.forEach(collection, (name, value) -> seen.incrementAndGet());
    assertEquals(1, seen.get());

    MyBatisBlobStore restarted =
        store(
            new PersistenceDomain(
                "resource",
                PersistenceDomain.Table.AMORO_RESOURCE.physicalName(),
                SerdeFormat.JSON));
    assertArrayEquals(bytes("{\"v\":7}"), restarted.find(collection, "reload").orElse(null));
  }

  @Test
  public void doubleDomainDoubleTableIsolationOnRealMysql() {
    resourceStore.insert("fake", "same-name", bytes("from-resource"));
    processStore.insert("process", "same-name", bytes("from-process"));

    assertArrayEquals(bytes("from-resource"), resourceStore.find("fake", "same-name").orElse(null));
    assertArrayEquals(
        bytes("from-process"), processStore.find("process", "same-name").orElse(null));
    assertTrue(processStore.delete("process", "same-name"));
    assertTrue(resourceStore.find("fake", "same-name").isPresent());
  }

  @Test
  public void schemaInitializationIsIdempotent() {
    IsolatedMysql.initializeControlPlane(MYSQL_DATABASE);
    IsolatedMysql.initializeGenericResourceDomain(MYSQL_DATABASE);
    resourceStore.insert("fake", "after-rerun", bytes("ok"));
    assertArrayEquals(bytes("ok"), resourceStore.find("fake", "after-rerun").orElse(null));
  }
}
