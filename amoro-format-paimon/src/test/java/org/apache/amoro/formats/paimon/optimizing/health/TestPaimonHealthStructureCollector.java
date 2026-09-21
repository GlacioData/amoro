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

package org.apache.amoro.formats.paimon.optimizing.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.table.health.TableHealthStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

class TestPaimonHealthStructureCollector {
  @TempDir Path warehouse;

  @Test
  void levelRunsAreCountedPerBucketAndAllLevelsConserveBytes() throws Exception {
    PaimonHealthStructureCollector collector = collector(true);
    BinaryRow partition = BinaryRow.EMPTY_ROW;
    collector.addFile(partition, 0, file(10, 0), 0L);
    collector.addFile(partition, 0, file(20, 0), 0L);
    collector.addFile(partition, 0, file(30, 2), 0L);
    collector.addFile(partition, 0, file(40, 2), 0L);
    collector.addFile(partition, 1, file(50, 2), 0L);
    TableHealthStructure structure = collector.finish(true);
    assertEquals("2", structure.getTotalUnits());
    assertEquals(6, structure.getBands().size());
    assertEquals("2", structure.getBands().get(0).getSortedRuns());
    assertEquals("2", structure.getBands().get(2).getSortedRuns());
    assertEquals("0", structure.getBands().get(1).getFileCount());
    assertEquals(
        5, structure.getBands().stream().mapToLong(b -> Long.parseLong(b.getFileCount())).sum());
    assertEquals(
        150, structure.getBands().stream().mapToLong(b -> Long.parseLong(b.getFileSize())).sum());
  }

  @Test
  void appendIntervalsAreHalfOpenAndDoNotCreateLsmRuns() throws Exception {
    PaimonHealthStructureCollector collector = collector(false);
    long[] sizes = {0, 699, 700, 999, 1000, 1001};
    for (long size : sizes) {
      collector.addFile(BinaryRow.EMPTY_ROW, -1, file(size, 0), 0L);
    }
    TableHealthStructure structure = collector.finish(true);
    assertEquals(3, structure.getBands().size());
    for (TableHealthStructure.Band band : structure.getBands()) {
      assertEquals("2", band.getFileCount());
      assertNull(band.getSortedRuns());
    }
    assertNull(structure.getProblems().get(0).getBucket());
  }

  @Test
  void topProblemsAreBoundedStableAndRetainBucketLevels() throws Exception {
    PaimonHealthStructureCollector forward = collector(true);
    PaimonHealthStructureCollector reverse = collector(true);
    for (int i = 0; i < 100; i++) {
      for (int f = 0; f < 8; f++) {
        forward.addFile(BinaryRow.EMPTY_ROW, i, file(10, 0), 0L);
        reverse.addFile(BinaryRow.EMPTY_ROW, 99 - i, file(10, 0), 0L);
      }
    }
    TableHealthStructure structure = forward.finish(true);
    assertEquals("100", structure.getAffectedUnits());
    assertEquals("100", structure.getSevereUnits());
    assertEquals(20, structure.getProblems().size());
    assertEquals(structure.getProblems(), reverse.finish(true).getProblems());
    assertEquals(Integer.valueOf(0), structure.getProblems().get(0).getBucket());
    assertEquals("8", structure.getProblems().get(0).getBands().get(0).getSortedRuns());
  }

  @Test
  void deletionVectorsRemainVisibleWithoutAddingTombstonesAndDvCounts() throws Exception {
    PaimonHealthStructureCollector collector = collector(true);
    collector.addFile(BinaryRow.EMPTY_ROW, 0, file(100, 2), 0L);
    collector.observeDeletionVector(BinaryRow.EMPTY_ROW, 0, 60);
    assertEquals("1", collector.finish(true).getSevereUnits());
    assertEquals("MATERIALIZED_DELETE", collector.finish(true).getProblems().get(0).getCode());
  }

  @Test
  void createsOnePathFactoryOnlyWhenProblemsExist() throws Exception {
    for (boolean pk : new boolean[] {false, true}) {
      FileStoreTable table = table(pk);
      org.apache.paimon.FileStore<?> store = org.mockito.Mockito.spy(table.store());
      FileStoreTable observed = org.mockito.Mockito.spy(table);
      org.mockito.Mockito.doReturn(store).when(observed).store();
      PaimonHealthEvaluationContext context =
          PaimonHealthEvaluationContext.capture(table, "db.t", null);
      PaimonHealthStructureCollector empty = new PaimonHealthStructureCollector(observed, context);
      empty.finish(true);
      org.mockito.Mockito.verify(store, org.mockito.Mockito.never()).pathFactory();
      PaimonHealthStructureCollector collector =
          new PaimonHealthStructureCollector(observed, context);
      PaimonHealthStructureCollector reference = new PaimonHealthStructureCollector(table, context);
      for (int i = 0; i < 1000; i++) {
        collector.addFile(BinaryRow.EMPTY_ROW, i, file(100, 0), 60L);
        reference.addFile(BinaryRow.EMPTY_ROW, i, file(100, 0), 60L);
      }
      TableHealthStructure result = collector.finish(true);
      assertEquals("1000", result.getAffectedUnits());
      assertEquals(20, result.getProblems().size());
      assertEquals(reference.finish(true).getProblems(), result.getProblems());
      org.mockito.Mockito.verify(store).pathFactory();
    }
  }

  private PaimonHealthStructureCollector collector(boolean pk) throws Exception {
    FileStoreTable table = table(pk);
    return new PaimonHealthStructureCollector(
        table, PaimonHealthEvaluationContext.capture(table, "db.t", null));
  }

  private FileStoreTable table(boolean pk) throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      Schema.Builder schema =
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .option("target-file-size", "1000 b")
              .option("compaction.small-file-ratio", "0.7")
              .option("num-levels", "6");
      if (pk) {
        schema.primaryKey("id").option("bucket", "1");
      }
      Identifier id = Identifier.create("db", pk ? "pk" : "append");
      catalog.createTable(id, schema.build(), true);
      return (FileStoreTable) catalog.getTable(id);
    }
  }

  private static DataFileMeta file(long bytes, int level) {
    DataFileMeta file = mock(DataFileMeta.class);
    when(file.fileSize()).thenReturn(bytes);
    when(file.level()).thenReturn(level);
    when(file.rowCount()).thenReturn(100L);
    return file;
  }
}
