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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.table.DerbyPersistence;
import org.apache.amoro.server.table.TableMetadata;
import org.apache.ibatis.session.SqlSession;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Tests that the slim table-metadata select used by the thrift {@code listTables} path returns
 * {@link TableMetadata} indistinguishable from the wide select on every field consumed by {@link
 * TableMetadata#buildTableMeta()}.
 */
public class TestTableMetaMapperSlimSelect {

  @Rule public DerbyPersistence derbyPersistence = new DerbyPersistence();

  private static final String CATALOG = "slim_catalog";
  private static final String DB = "slim_db";

  @Test
  public void testSlimSelectMatchesWideSelectOnConsumedFields() {
    try (SqlSession session = SqlSessionFactoryProvider.getInstance().get().openSession(true)) {
      TableMetaMapper mapper = session.getMapper(TableMetaMapper.class);

      insertTable(
          mapper,
          "tbl_with_pk",
          "id,name",
          TableFormat.MIXED_ICEBERG,
          bigValue("{\"type\":\"struct\",\"fields\":[", 4000));
      insertTable(
          mapper,
          "tbl_without_pk",
          null,
          TableFormat.ICEBERG,
          bigValue("{\"type\":\"struct\",", 2000));

      List<TableMetadata> wide = mapper.selectTableMetasByDb(CATALOG, DB);
      List<TableMetadata> slim = mapper.selectTableMetaSummariesByDb(CATALOG, DB);

      Assert.assertEquals(2, wide.size());
      Assert.assertEquals(2, slim.size());

      Map<String, TableMetadata> wideByName = byTableName(wide);
      Map<String, TableMetadata> slimByName = byTableName(slim);
      Assert.assertEquals(wideByName.keySet(), slimByName.keySet());
      for (Map.Entry<String, TableMetadata> entry : wideByName.entrySet()) {
        TableMetadata wideMeta = entry.getValue();
        TableMetadata slimMeta = slimByName.get(entry.getKey());
        assertConsumedFieldsEquals(wideMeta, slimMeta);
        Assert.assertEquals(wideMeta.buildTableMeta(), slimMeta.buildTableMeta());
      }

      // the wide select loads the heavy columns, the slim select leaves them unmapped
      for (TableMetadata metadata : wide) {
        Assert.assertNotNull(metadata.getSchema());
        Assert.assertNotNull(metadata.getMetaStoreSite());
        Assert.assertNotNull(metadata.getHdfsSite());
        Assert.assertNotNull(metadata.getCoreSite());
        Assert.assertNotNull(metadata.getKrbKeytab());
      }
      for (TableMetadata metadata : slim) {
        Assert.assertNull(metadata.getSchema());
        Assert.assertNull(metadata.getMetaStoreSite());
        Assert.assertNull(metadata.getHdfsSite());
        Assert.assertNull(metadata.getCoreSite());
        Assert.assertNull(metadata.getKrbKeytab());
      }
    }
  }

  private static void assertConsumedFieldsEquals(TableMetadata expected, TableMetadata actual) {
    Assert.assertEquals(expected.getTableIdentifier().getId(), actual.getTableIdentifier().getId());
    Assert.assertEquals(
        expected.getTableIdentifier().getCatalog(), actual.getTableIdentifier().getCatalog());
    Assert.assertEquals(
        expected.getTableIdentifier().getDatabase(), actual.getTableIdentifier().getDatabase());
    Assert.assertEquals(
        expected.getTableIdentifier().getTableName(), actual.getTableIdentifier().getTableName());
    Assert.assertEquals(
        expected.getTableIdentifier().getFormat(), actual.getTableIdentifier().getFormat());
    Assert.assertEquals(expected.getPrimaryKey(), actual.getPrimaryKey());
    Assert.assertEquals(expected.getTableLocation(), actual.getTableLocation());
    Assert.assertEquals(expected.getBaseLocation(), actual.getBaseLocation());
    Assert.assertEquals(expected.getChangeLocation(), actual.getChangeLocation());
    Assert.assertEquals(expected.getProperties(), actual.getProperties());
  }

  private static Map<String, TableMetadata> byTableName(List<TableMetadata> metadataList) {
    return metadataList.stream()
        .collect(Collectors.toMap(m -> m.getTableIdentifier().getTableName(), Function.identity()));
  }

  private static void insertTable(
      TableMetaMapper mapper,
      String tableName,
      String primaryKey,
      TableFormat format,
      String schema) {
    ServerTableIdentifier identifier = ServerTableIdentifier.of(CATALOG, DB, tableName, format);
    mapper.insertTable(identifier);

    Map<String, String> properties = new HashMap<>();
    properties.put("owner", "slim-test");
    properties.put("self-optimizing.group", "slim-group");
    TableMetadata metadata =
        TableMetadata.fromExternalTable(
            identifier,
            "file:///tmp/slim/" + tableName,
            "file:///tmp/slim/" + tableName + "/base",
            "file:///tmp/slim/" + tableName + "/change",
            primaryKey,
            properties,
            schema,
            "HASH_FIXED",
            4,
            "snapshot-" + tableName,
            "comment of " + tableName);
    metadata.setMetaStoreSite(bigValue("<configuration><name>hive.metastore.uris</name>", 500));
    metadata.setHdfsSite(bigValue("<configuration><name>dfs.replication</name>", 400));
    metadata.setCoreSite(bigValue("<configuration><name>fs.defaultFS</name>", 300));
    metadata.setAuthMethod("SIMPLE");
    metadata.setHadoopUsername("hadoop");
    metadata.setKrbKeytab(bigValue("krb5-keytab-bytes-", 200));
    metadata.setKrbConf(bigValue("[libdefaults]\ndefault_realm = EXAMPLE.COM\n", 50));
    metadata.setKrbPrincipal("slim/_HOST@EXAMPLE.COM");
    mapper.insertTableMeta(metadata);
  }

  private static String bigValue(String prefix, int repetitions) {
    return prefix + String.valueOf("x".repeat(24) + "\n").repeat(Math.max(1, repetitions));
  }
}
