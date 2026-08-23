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

package org.apache.amoro.test;

import org.apache.amoro.config.ControlPlaneSchemaInitializer;
import org.apache.amoro.persistence.blob.ResourceBlobMapper;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

/**
 * JUnit root extension owning one MySQL 5.7 container for the docker-mysql suite.
 *
 * <p>Every test class uses a separate database inside the container. The root-store resource is
 * stopped only after the complete JUnit plan, avoiding three image boot cycles without sharing any
 * durable row or issuing destructive cleanup.
 */
public final class IsolatedMysql implements BeforeAllCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(IsolatedMysql.class);
  private static final String RESOURCE_KEY = "mysql-5.7";
  private static final Pattern DATABASE_NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");
  private static volatile SharedContainer shared;

  @Override
  public void beforeAll(ExtensionContext context) {
    shared =
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(
                RESOURCE_KEY, ignored -> new SharedContainer(), SharedContainer.class);
  }

  /** Runs the production control-plane DDL against one class-isolated database. */
  public static void initializeControlPlane(String databaseName) {
    createDatabase(databaseName);
    new ControlPlaneSchemaInitializer(dataSource(databaseName)).initialize();
  }

  /** Creates the generic framework test domain without dropping or cleaning any table. */
  public static void initializeGenericResourceDomain(String databaseName) {
    createDatabase(databaseName);
    try (Connection connection = dataSource(databaseName).getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS amoro_resource ("
              + "name VARCHAR(256) NOT NULL, "
              + "collection CHAR(50) NOT NULL, "
              + "value MEDIUMTEXT NOT NULL, "
              + "last_updated DATETIME NOT NULL, "
              + "PRIMARY KEY (name)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    } catch (SQLException e) {
      throw new IllegalStateException("failed to initialize isolated generic resource schema", e);
    }
  }

  public static SqlSessionFactory sqlSessionFactory(String databaseName, String environment) {
    createDatabase(databaseName);
    Environment myBatisEnvironment =
        new Environment(environment, new JdbcTransactionFactory(), dataSource(databaseName));
    Configuration configuration = new Configuration(myBatisEnvironment);
    configuration.addMapper(ResourceBlobMapper.class);
    return new SqlSessionFactoryBuilder().build(configuration);
  }

  private static synchronized void createDatabase(String databaseName) {
    if (!DATABASE_NAME.matcher(databaseName).matches()) {
      throw new IllegalArgumentException("invalid isolated database name " + databaseName);
    }
    MySQLContainer container = container();
    try (Connection connection =
            new UnpooledDataSource(
                    "com.mysql.cj.jdbc.Driver",
                    container.getJdbcUrl(),
                    "root",
                    container.getPassword())
                .getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + databaseName);
    } catch (SQLException e) {
      throw new IllegalStateException("failed to create isolated database " + databaseName, e);
    }
  }

  private static UnpooledDataSource dataSource(String databaseName) {
    MySQLContainer container = container();
    String jdbcUrl =
        "jdbc:mysql://"
            + container.getHost()
            + ":"
            + container.getMappedPort(MySQLContainer.MYSQL_PORT)
            + "/"
            + databaseName
            + "?useSSL=false&characterEncoding=utf8&allowPublicKeyRetrieval=true";
    return new UnpooledDataSource(
        "com.mysql.cj.jdbc.Driver", jdbcUrl, "root", container.getPassword());
  }

  private static MySQLContainer container() {
    SharedContainer resource = shared;
    if (resource == null) {
      throw new IllegalStateException("IsolatedMysql extension has not started");
    }
    return resource.container;
  }

  private static final class SharedContainer implements ExtensionContext.Store.CloseableResource {
    private final MySQLContainer container;

    private SharedContainer() {
      container =
          new MySQLContainer(DockerImageName.parse("mysql:5.7.44"))
              .withDatabaseName("amoro_testcontainers")
              .withUsername("amoro_test")
              .withPassword("amoro_test_password");
      container.start();
    }

    @Override
    public void close() {
      container.stop();
      shared = null;
    }
  }
}
