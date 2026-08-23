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

import java.util.Objects;

/**
 * Declarative binding of one {@link PersistenceService} instance to its physical table and serde
 * format (framework spec §5.3, per-domain tables). Each domain gets its own homogeneous KV table so
 * domains can carry independent retention/cleanup lifecycles; the default framework domain binds
 * {@code amoro_resource}.
 *
 * <p>Table names feed MyBatis SQL, so only the whitelisted enum constants below are accepted —
 * construction with anything else fails fast (injection defense).
 */
public final class PersistenceDomain {

  /**
   * Whitelist of physical tables the blob layer may target. The deployed schema creates only {@code
   * amoro_process_v2} (the single process table doing persistence AND state tracking); {@code
   * amoro_resource} stays available for framework-generic domains that opt in, with their table
   * created by that domain's own setup rather than the shipped DDL.
   */
  public enum Table {
    AMORO_PROCESS_V2("amoro_process_v2"),
    AMORO_RESOURCE("amoro_resource");

    private final String physicalName;

    Table(String physicalName) {
      this.physicalName = physicalName;
    }

    public String physicalName() {
      return physicalName;
    }
  }

  /** Wire format of the serialized resource blob; Base64-encoded before storage either way. */
  public enum SerdeFormat {
    JSON,
    YAML
  }

  private final String domainName;
  private final Table table;
  private final SerdeFormat serdeFormat;

  public PersistenceDomain(String domainName, String tableName, SerdeFormat serdeFormat) {
    this.domainName = requireNonBlankDomain(domainName);
    Objects.requireNonNull(tableName, "tableName");
    Objects.requireNonNull(serdeFormat, "serdeFormat");
    Table resolved = null;
    for (Table candidate : Table.values()) {
      if (candidate.physicalName.equals(tableName)) {
        resolved = candidate;
        break;
      }
    }
    if (resolved == null) {
      throw new IllegalArgumentException(
          "table '" + tableName + "' is not a whitelisted persistence table");
    }
    this.table = resolved;
    this.serdeFormat = serdeFormat;
  }

  public PersistenceDomain(String domainName, Table table, SerdeFormat serdeFormat) {
    this.domainName = requireNonBlankDomain(domainName);
    this.table = Objects.requireNonNull(table, "table");
    this.serdeFormat = Objects.requireNonNull(serdeFormat, "serdeFormat");
  }

  private static String requireNonBlankDomain(String domainName) {
    Objects.requireNonNull(domainName, "domainName");
    if (domainName.trim().isEmpty()) {
      throw new IllegalArgumentException("domainName must not be blank");
    }
    return domainName;
  }

  /** Default framework domain: {@code amoro_resource}, JSON. */
  public static PersistenceDomain defaultResourceDomain() {
    return new PersistenceDomain("resource", Table.AMORO_RESOURCE.physicalName(), SerdeFormat.JSON);
  }

  public String domainName() {
    return domainName;
  }

  public String table() {
    return table.physicalName();
  }

  public SerdeFormat serdeFormat() {
    return serdeFormat;
  }

  @Override
  public String toString() {
    return "PersistenceDomain{"
        + domainName
        + " -> "
        + table.physicalName()
        + " ("
        + serdeFormat
        + ")}";
  }
}
