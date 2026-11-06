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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.PlannerFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DeletionFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;

class TestPaimonAppendPlannerFileFactory {
  @ParameterizedTest
  @ValueSource(strings = {"empty", "null", "long", "composite"})
  void sharesOnlyPartitionStorageAndPreservesEveryFileFact(String shape) throws Exception {
    BinaryRow partition = partition(shape);
    PlannerFile.Factory factory = new PlannerFile.Factory();
    DataFileMeta firstMeta = mock(DataFileMeta.class);
    DataFileMeta secondMeta = mock(DataFileMeta.class);
    DeletionFile deletion = new DeletionFile("dv", 3L, 5L, 1L);
    PlannerFile first = factory.create(partition, 0, 4, firstMeta, null, null);
    PlannerFile second = factory.create(partition.copy(), 3, 4, secondMeta, deletion, "group");

    assertSame(
        storedPartition(first),
        storedPartition(second),
        "Equal partitions must share storage inside one scan");
    assertNotSame(partition, storedPartition(first));
    assertNotSame(first, second);
    assertEquals(partition, first.partition());
    assertEquals(partition, second.partition());
    assertEquals(0, first.bucket());
    assertEquals(3, second.bucket());
    assertEquals(4, second.totalBuckets());
    assertSame(firstMeta, first.file());
    assertSame(secondMeta, second.file());
    assertSame(deletion, second.deletionFile());
    assertEquals("group", second.dvGroupKey());
  }

  @Test
  void inputAndAccessorMutationCannotChangeSharedStorageOrIndex() throws Exception {
    PlannerFile.Factory factory = new PlannerFile.Factory();
    BinaryRow input = BinaryRow.singleColumn(7);
    DataFileMeta meta = mock(DataFileMeta.class);
    PlannerFile first = factory.create(input, 0, 1, meta, null, null);
    input.setInt(0, 8);
    first.partition().setInt(0, 9);
    PlannerFile same = factory.create(BinaryRow.singleColumn(7), 0, 1, meta, null, null);
    PlannerFile different = factory.create(input, 0, 1, meta, null, null);

    assertSame(storedPartition(first), storedPartition(same));
    assertNotSame(storedPartition(first), storedPartition(different));
    assertEquals(7, first.partition().getInt(0));
    assertEquals(7, same.partition().getInt(0));
    assertEquals(8, different.partition().getInt(0));
    assertNotSame(first.partition(), first.partition());
  }

  @Test
  void separateFactoriesAndPublicConstructorsKeepTheirOwnSnapshots() throws Exception {
    BinaryRow input = BinaryRow.singleColumn(7);
    DataFileMeta meta = mock(DataFileMeta.class);
    PlannerFile first = new PlannerFile.Factory().create(input, 0, 1, meta, null, null);
    PlannerFile second = new PlannerFile.Factory().create(input, 0, 1, meta, null, null);
    PlannerFile publicFile = new PlannerFile(input, meta, null, null);
    assertNotSame(storedPartition(first), storedPartition(second));
    assertNotSame(storedPartition(first), storedPartition(publicFile));
    input.clear();
    assertEquals(BinaryRow.singleColumn(7), publicFile.partition());
    assertEquals(publicFile.partition(), first.partition());
  }

  private static BinaryRow storedPartition(PlannerFile file) throws Exception {
    // A memory ownership assertion must inspect storage without weakening the public copy boundary.
    Field field = PlannerFile.class.getDeclaredField("partition");
    field.setAccessible(true);
    return (BinaryRow) field.get(file);
  }

  private static BinaryRow partition(String shape) {
    if (shape.equals("empty")) {
      return BinaryRow.EMPTY_ROW.copy();
    }
    if (shape.equals("null")) {
      return BinaryRow.singleColumn((Integer) null);
    }
    BinaryRow row = new BinaryRow(shape.equals("composite") ? 2 : 1);
    BinaryRowWriter writer = new BinaryRowWriter(row);
    writer.writeString(
        0,
        BinaryString.fromString(String.join("", java.util.Collections.nCopies(300, "partition/"))));
    if (shape.equals("composite")) {
      writer.setNullAt(1);
    }
    writer.complete();
    return row;
  }
}
