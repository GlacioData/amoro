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

package org.apache.amoro.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Idempotent schema initialization for the framework's domain tables (framework spec §5.3). Dialect
 * scripts with {@code CREATE TABLE IF NOT EXISTS} (MySQL, PostgreSQL) run as-is; Derby (no such
 * syntax in 10.14) runs per-table under a metadata guard, tolerating only the table-exists SQLState
 * of a concurrent create.
 */
public final class ControlPlaneSchemaInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(ControlPlaneSchemaInitializer.class);

  private final DataSource dataSource;

  public ControlPlaneSchemaInitializer(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  /** Runs {@code schema-<platform>.sql} idempotently; safe to call on every boot. */
  public void initialize() {
    try (Connection connection = dataSource.getConnection()) {
      String platform = resolvePlatform(connection);
      String script = readScript(platform);
      if (isDerby(connection)) {
        runPerTableWithGuard(connection, script);
      } else {
        runWholeScript(connection, script);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("failed to initialize the control-plane schema", e);
    }
  }

  private String resolvePlatform(Connection connection) throws SQLException {
    String product = connection.getMetaData().getDatabaseProductName().toLowerCase();
    if (product.contains("mysql")) {
      return "mysql";
    }
    if (product.contains("postgres")) {
      return "postgres";
    }
    if (product.contains("derby")) {
      return "derby";
    }
    throw new IllegalStateException("unsupported database product " + product);
  }

  private boolean isDerby(Connection connection) throws SQLException {
    return connection.getMetaData().getDatabaseProductName().toLowerCase().contains("derby");
  }

  private String readScript(String platform) {
    try {
      return new String(
          getClass().getResourceAsStream("/schema-" + platform + ".sql").readAllBytes(),
          StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IllegalStateException("missing schema-" + platform + ".sql on the classpath", e);
    }
  }

  private void runWholeScript(Connection connection, String script) throws SQLException {
    String withoutComments = script.replaceAll("--[^\n]*", "");
    try (Statement statement = connection.createStatement()) {
      for (String piece : withoutComments.split(";")) {
        String sql = piece.trim();
        if (!sql.isEmpty()) {
          statement.execute(sql);
        }
      }
    }
  }

  private void runPerTableWithGuard(Connection connection, String script) throws SQLException {
    String withoutComments = script.replaceAll("--[^\n]*", "");
    List<String> creates = new ArrayList<String>();
    for (String piece : withoutComments.split(";")) {
      String sql = piece.trim();
      if (sql.toUpperCase().startsWith("CREATE TABLE ")) {
        creates.add(sql);
      }
    }
    DatabaseMetaData metadata = connection.getMetaData();
    java.util.regex.Matcher matcher;
    for (String create : creates) {
      matcher =
          java.util.regex.Pattern.compile("CREATE\\s+TABLE\\s+([A-Za-z0-9_]+)").matcher(create);
      if (!matcher.find()) {
        throw new IllegalStateException("cannot parse table name from: " + create);
      }
      String table = matcher.group(1);
      boolean exists;
      try (ResultSet found = metadata.getTables(null, null, table.toUpperCase(), null)) {
        exists = found.next();
      }
      if (exists) {
        continue;
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute(create);
      } catch (SQLException concurrentCreate) {
        if (!"X0Y32".equals(concurrentCreate.getSQLState())) {
          throw concurrentCreate;
        }
        LOG.info("Table {} was created concurrently; continuing.", table);
      }
    }
  }
}
