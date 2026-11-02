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

package org.apache.amoro.server.persistence.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

class TestObject2ByteArrayConvert {

  private final Object2ByteArrayConvert<String> converter = new Object2ByteArrayConvert<>();

  private PreparedStatement ps;
  private Connection connection;
  private DatabaseMetaData metaData;

  @BeforeEach
  void setUp() throws Exception {
    resetDatabaseProductNameCache();
    ps = mock(PreparedStatement.class);
    connection = mock(Connection.class);
    metaData = mock(DatabaseMetaData.class);
    when(ps.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metaData);
  }

  @AfterEach
  void clearDatabaseProductNameCache() throws Exception {
    resetDatabaseProductNameCache();
  }

  /**
   * The resolved product name is cached in a static field shared across test methods; clear it so
   * every test observes its own mocked metadata lookup.
   */
  private static void resetDatabaseProductNameCache() throws Exception {
    Field cache = Object2ByteArrayConvert.class.getDeclaredField("databaseProductName");
    cache.setAccessible(true);
    cache.set(null, null);
  }

  @Test
  void resolvesDatabaseProductNameOnlyOncePerProcess() throws SQLException {
    when(metaData.getDatabaseProductName()).thenReturn("mysql");

    converter.setParameter(ps, 1, null, null);
    converter.setParameter(ps, 2, "first", null);
    converter.setParameter(ps, 3, "second", null);

    verify(connection, times(1)).getMetaData();
  }

  @Test
  void nonNullParametersNeverTouchDatabaseMetadata() throws SQLException {
    when(metaData.getDatabaseProductName()).thenReturn("mysql");

    ArgumentCaptor<InputStream> streams = ArgumentCaptor.forClass(InputStream.class);
    converter.setParameter(ps, 1, "first", null);
    converter.setParameter(ps, 2, "second", null);

    verify(connection, never()).getMetaData();
    verify(ps, times(2)).setBinaryStream(anyInt(), streams.capture());
    assertEquals(2, streams.getAllValues().size());
    for (InputStream stream : streams.getAllValues()) {
      assertNotNull(stream);
    }
  }

  @Test
  void nullParameterBindsPostgresBinaryType() throws SQLException {
    when(metaData.getDatabaseProductName()).thenReturn("postgresql");

    converter.setParameter(ps, 1, null, null);

    verify(ps).setNull(1, Types.BINARY);
  }

  @Test
  void nullParameterBindsBlobTypeForOtherDatabases() throws SQLException {
    when(metaData.getDatabaseProductName()).thenReturn("mysql");

    converter.setParameter(ps, 1, null, null);

    verify(ps).setNull(1, Types.BLOB);
  }

  @Test
  void lowerCasesMixedCaseProductNameBeforeMatchingPostgresPrefix() throws SQLException {
    when(metaData.getDatabaseProductName()).thenReturn("PostgreSQL");

    converter.setParameter(ps, 1, null, null);

    verify(ps).setNull(1, Types.BINARY);
  }
}
