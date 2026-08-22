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

import org.apache.amoro.persistence.PersistenceDomain;
import org.apache.amoro.persistence.exception.ResourceAlreadyExists;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * L7: {@link BlobStore} over the domain-bound MyBatis table (framework spec §5.3). Values are
 * Base64 of the domain's document bytes (JSON or YAML) — the {@code value} column therefore carries
 * ~33% more bytes than the raw document, which the MEDIUMTEXT/TEXT/CLOB dialect types accommodate.
 * A duplicate-key database error is translated to {@link ResourceAlreadyExists}.
 */
public final class MyBatisBlobStore implements BlobStore {

  private final String table;
  private final ResourceBlobMapper mapper;

  public MyBatisBlobStore(PersistenceDomain domain, ResourceBlobMapper mapper) {
    Objects.requireNonNull(domain, "domain");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    // the whitelist validation inside PersistenceDomain makes this name safe for ${} use
    this.table = domain.table();
  }

  @Override
  public void insert(String collection, String name, byte[] value) {
    try {
      mapper.insert(
          table,
          name,
          collection,
          encode(value),
          Timestamp.valueOf(LocalDateTime.now(ZoneOffset.UTC)));
    } catch (RuntimeException e) {
      // Spring-wired sessions already translate to DuplicateKeyException; raw MyBatis sessions
      // wrap the driver error — accept both so the store behaves identically in tests
      if (isDuplicateKey(e)) {
        throw new ResourceAlreadyExists(collection, name);
      }
      throw e;
    }
  }

  /** SQL-standard unique-violation state plus the MySQL vendor code, across cause chains. */
  private static boolean isDuplicateKey(Throwable error) {
    Throwable current = error;
    for (int depth = 0; current != null && depth < 8; depth++) {
      String name = current.getClass().getName();
      if (name.endsWith("DuplicateKeyException")) {
        return true; // Spring-translated sessions
      }
      if (current instanceof java.sql.SQLException) {
        java.sql.SQLException sql = (java.sql.SQLException) current;
        // the type alone covers NOT NULL/FK/CHECK violations too, so require the unique
        // violation state or the MySQL vendor code alongside it
        if (name.endsWith("SQLIntegrityConstraintViolationException")
            && ("23505".equals(sql.getSQLState()) || sql.getErrorCode() == 1062)) {
          return true;
        }
        if ("23505".equals(sql.getSQLState()) || sql.getErrorCode() == 1062) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  @Override
  public boolean update(String collection, String name, byte[] value) {
    return mapper.update(
            table,
            name,
            collection,
            encode(value),
            Timestamp.valueOf(LocalDateTime.now(ZoneOffset.UTC)))
        > 0;
  }

  @Override
  public boolean delete(String collection, String name) {
    return mapper.delete(table, name, collection) > 0;
  }

  @Override
  public Optional<byte[]> find(String collection, String name) {
    String encoded = mapper.find(table, name, collection);
    return encoded == null ? Optional.empty() : Optional.of(decode(encoded));
  }

  @Override
  public void forEach(String collection, BiConsumer<String, byte[]> action) {
    for (BlobRow row : mapper.selectAll(table, collection)) {
      // String-typed properties read CLOB/VARCHAR columns portably across dialects
      action.accept(row.getName(), decode(row.getValue()));
    }
  }

  private static String encode(byte[] value) {
    return Base64.getEncoder().encodeToString(value);
  }

  private static byte[] decode(String encoded) {
    return Base64.getDecoder().decode(encoded.getBytes(StandardCharsets.US_ASCII));
  }
}
