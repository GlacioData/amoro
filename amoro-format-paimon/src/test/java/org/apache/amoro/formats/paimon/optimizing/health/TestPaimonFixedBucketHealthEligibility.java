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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendCapabilities;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonAppendFileScanner;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestPaimonFixedBucketHealthEligibility {
  @TempDir Path warehouse;

  // Paimon ac223f47, CoreOptions.java:3654-3658 computes clustering from current unordered,
  // non-DV options. Capability refusal is not a manifest/index read failure or a stale score.
  @Test
  void clusteringRefusalFollowsCurrentOptionsWithoutPollutingReusableHealth() throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("d", false);
      Identifier id = Identifier.create("d", "fixed");
      catalog.createTable(
          id,
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .option("bucket", "2")
              .option("bucket-key", "id")
              .option("write-only", "true")
              .option("bucket-append-ordered", "false")
              .option("clustering.columns", "id")
              .option("clustering.incremental", "false")
              .build(),
          false);
      AppendOnlyFileStoreTable original = (AppendOnlyFileStoreTable) catalog.getTable(id);
      BatchWriteBuilder builder = original.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(1));
        commit.commit(write.prepareCommit());
      }
      PaimonHealthEvaluationContext before =
          PaimonHealthEvaluationContext.capture(original, "t", null);
      FormatTableAnalysis healthy = PaimonAppendFileScanner.analyze(original, before, null);
      PaimonHealthSupport support = new PaimonHealthSupport();
      AppendOnlyFileStoreTable current = original;
      for (boolean clustering : new boolean[] {true, false, true, false}) {
        current =
            (AppendOnlyFileStoreTable)
                current.copy(
                    Collections.singletonMap(
                        "clustering.incremental", Boolean.toString(clustering)));
        PaimonHealthEvaluationContext context =
            PaimonHealthEvaluationContext.capture(current, "t", null);
        FormatTableAnalysis analysis = PaimonAppendFileScanner.analyze(current, context, null);

        assertEquals(
            clustering ? "FIXED_APPEND_CLUSTERING_UNSUPPORTED" : "",
            PaimonAppendCapabilities.fixedBucketRejection(current).orElse(""));
        if (clustering) {
          assertNotEquals(before.key(), context.key());
        } else {
          assertEquals(before.key(), context.key());
        }
        assertTrue(support.sameInput(context.key(), healthy.healthDetails()));
        assertEquals(
            healthy.pendingInput().getHealthScore(), analysis.pendingInput().getHealthScore());
        assertEquals("COMPLETE", analysis.healthDetails().getStructure().getAvailability());
        assertFalse(support.retryable(analysis.healthDetails()));
        assertFalse(
            analysis.healthDetails().getReasonCodes().contains(support.readFailureReason()));
        assertFalse(
            analysis
                .healthDetails()
                .getReasonCodes()
                .contains("FIXED_APPEND_CLUSTERING_UNSUPPORTED"));
      }
      Map<String, String> inactiveOptions = new HashMap<>(original.options());
      inactiveOptions.put("clustering.incremental", "true");
      inactiveOptions.put("bucket-append-ordered", "true");
      // A schema-options copy tests the effective capability rule, not ALTER validation.
      AppendOnlyFileStoreTable inactive =
          (AppendOnlyFileStoreTable) original.copy(original.schema().copy(inactiveOptions));
      assertFalse(PaimonAppendCapabilities.fixedBucketRejection(inactive).isPresent());
    }
  }
}
