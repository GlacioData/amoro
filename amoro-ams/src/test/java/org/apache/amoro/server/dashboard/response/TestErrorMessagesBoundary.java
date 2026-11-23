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

package org.apache.amoro.server.dashboard.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.exception.AlreadyExistsException;
import org.apache.amoro.exception.ObjectNotExistsException;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.server.PlainFailures;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.ws.rs.BadRequestException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/** Pins the boundary of {@link ErrorMessages}: what a client may read and what it may not. */
class TestErrorMessagesBoundary {

  /** Messages the REST layer raises on purpose. Every one of them must stay visible. */
  static Stream<String> validationMessages() {
    return Stream.of(
        "Duplicate catalog name!",
        "Invalid storage type S9",
        "Invalid table format list, FOO",
        "page[0] must > 0",
        "pageSize[200] must <= 100",
        "offset[100] exceeds supported range",
        "Catalog type:hadoop require property:warehouse.",
        "Table property x is not allowed to set",
        "Catalog property y is not allowed to set",
        "Catalog auth config must not be null",
        "Group name:bad name must match ^[A-Za-z0-9_-]{1,50}$.",
        "Optimizer group:g already existed.",
        "Group name can not be empty, please provide a valid group name.",
        "Unsupported sort field: name",
        "Unsupported sort order: up",
        "Page and page size must be greater than zero",
        "Page offset is too large",
        "Invalid orderBy: size",
        "invalid operation: x, only support all/optimizing/non-optimizing",
        "unknown snapshot 1 of catalog.db.tbl",
        "invalid catalog!",
        "Catalog missing not exists",
        "invalid user admin or password!",
        "Login failed due to a server error during role resolution",
        "Please login first",
        "No permission",
        "For input string: \"abc\"",
        "BasicAuthCredentials must not be null",
        "Database: db doesn't exists",
        "Table does not exist: c.d.t",
        "rename is not supported now.");
  }

  /** Text raised by infrastructure, or wrapping another failure. None of it may be returned. */
  static Stream<String> infrastructureTexts() {
    return Stream.of(
        "java.net.UnknownHostException: hdfs-nn",
        "java.net.ConnectException: Connection refused to hms-host:9083",
        "org.apache.thrift.transport.TTransportException: java.net.SocketTimeoutException:"
            + " Read timed out",
        "### Error querying database. Cause: jdbc:mysql://db:3306/ams: Access denied"
            + "\n### SQL: select 1",
        "Wrong FS: hdfs://nn:8020/a, expected: file:///",
        "Caused by: java.io.IOException: broken pipe",
        "Cannot invoke \"java.util.Map.get(Object)\" because \"properties\" is null",
        "Cannot create pool for jdbc:mysql://db:3306/ams",
        "failed to load table: s3a://bucket/warehouse/db/t/metadata/00001.metadata.json",
        "at org.apache.amoro.server.table.DefaultTableService.load(DefaultTableService.java:12)");
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("validationMessages")
  void validationMessagesStayVisible(String message) {
    assertTrue(ErrorMessages.isClientFacing(new IllegalArgumentException(message)), message);
    assertEquals(message, ErrorMessages.of(new IllegalStateException(message)));
    assertEquals(message, ErrorMessages.of(new NullPointerException(message)));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("infrastructureTexts")
  void infrastructureTextStaysHiddenWhateverTheType(String text) {
    assertTrue(ErrorMessages.carriesInternalDetail(text), text);
    assertEquals(
        ErrorMessages.INTERNAL_ERROR, ErrorMessages.of(new IllegalArgumentException(text)));
    assertEquals(ErrorMessages.INTERNAL_ERROR, ErrorMessages.of(new IllegalStateException(text)));
    assertEquals(ErrorMessages.INTERNAL_ERROR, ErrorMessages.of(new NullPointerException(text)));
  }

  @Test
  void typesRaisedOnPurposeAreClientFacing() {
    List<Exception> raisedForTheClient =
        Arrays.asList(
            new ObjectNotExistsException("Catalog demo"),
            new AlreadyExistsException("Catalog demo"),
            new BadRequestException("Page offset is too large"),
            new NoSuchTableException("Table does not exist: c.d.t"),
            new NoSuchNamespaceException("Database: db doesn't exists"));

    for (Exception failure : raisedForTheClient) {
      assertTrue(ErrorMessages.isClientFacing(failure), String.valueOf(failure));
    }
  }

  @Test
  void otherTypesAreNotClientFacing() {
    List<Exception> notRaisedForTheClient =
        Arrays.asList(
            new PersistenceException("Table runtime meta can not be found"),
            new UncheckedIOException(new IOException("Unable to load the metadata file")),
            new ClassCastException("CatalogMeta cannot be cast to ServerCatalog"),
            new UnsupportedOperationException("x"),
            new IndexOutOfBoundsException("Index 3 out of bounds for length 1"),
            // A plain RuntimeException raised below the dashboard layer
            PlainFailures.plain("Duplicate catalog name!"));

    for (Exception failure : notRaisedForTheClient) {
      assertFalse(ErrorMessages.isClientFacing(failure), String.valueOf(failure));
      assertEquals(ErrorMessages.INTERNAL_ERROR, ErrorMessages.of(failure));
    }
  }

  @Test
  void aMessageWithoutTextIsNotClientFacing() {
    assertFalse(ErrorMessages.isClientFacing(new IllegalArgumentException()));
    assertFalse(ErrorMessages.isClientFacing(new NullPointerException()));
    assertEquals(ErrorMessages.INTERNAL_ERROR, ErrorMessages.of(new NullPointerException()));
  }

  @Test
  void aVeryLongMessageIsNeitherScannedNorReturned() {
    StringBuilder longText = new StringBuilder("Unsupported sort field: ");
    while (longText.length() <= 1000) {
      longText.append('a');
    }
    assertFalse(ErrorMessages.carriesInternalDetail("Unsupported sort field: aaa"));
    assertTrue(ErrorMessages.carriesInternalDetail(longText.toString()));
    assertFalse(ErrorMessages.isClientFacing(new IllegalArgumentException(longText.toString())));
    assertEquals(
        ErrorMessages.INTERNAL_ERROR,
        ErrorMessages.of(new IllegalArgumentException(longText.toString())));
  }
}
