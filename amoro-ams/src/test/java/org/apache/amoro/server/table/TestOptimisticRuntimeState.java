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

package org.apache.amoro.server.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableRuntimeStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class TestOptimisticRuntimeState extends PersistentBase {
  private final DerbyPersistence derby = new DerbyPersistence();
  private final StateKey<Long> key =
      StateKey.stateKey("diagnostic_counter")
          .longType()
          .defaultValue(0L)
          .withOptimisticConcurrency();

  @AfterEach
  void cleanup() {
    derby.after();
  }

  private TableRuntimeMeta create(ServerTableIdentifier id) {
    doAs(TableMetaMapper.class, mapper -> mapper.insertTable(id));
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("default");
    meta.setStatusCode(0);
    doAs(TableRuntimeMapper.class, mapper -> mapper.insertRuntime(meta));
    return meta;
  }

  @Test
  void staleStoreUpdatesThePersistedValueInsteadOfOverwritingIt() {
    ServerTableIdentifier id = ServerTableIdentifier.of("c", "d", "t", TableFormat.ICEBERG);
    TableRuntimeMeta meta = create(id);
    TableRuntimeStore first =
        new DefaultTableRuntimeStore(
            id, meta, Collections.singletonList(key), Collections.emptyList());
    TableRuntimeStore second =
        new DefaultTableRuntimeStore(
            id, meta, Collections.singletonList(key), Collections.emptyList());
    first.begin().updateState(key, value -> value + 1).commit();
    assertEquals(0L, second.getState(key).longValue());
    second.begin().updateState(key, value -> value + 1).commit();
    assertEquals(2L, second.getState(key).longValue());
    assertEquals(
        "2",
        getAs(TableRuntimeMapper.class, mapper -> mapper.getState(id.getId(), key.getKey()))
            .getStateValue());
    assertEquals(
        2L,
        getAs(TableRuntimeMapper.class, mapper -> mapper.getState(id.getId(), key.getKey()))
            .getStateVersion());
  }

  @Test
  void revisionMismatchRollsBackWithoutPublishingTheCandidateValue() {
    ServerTableIdentifier id = ServerTableIdentifier.of("c", "d", "t", TableFormat.ICEBERG);
    TableRuntimeStore store =
        new DefaultTableRuntimeStore(
            id, create(id), Collections.singletonList(key), Collections.emptyList());
    assertThrows(
        org.apache.amoro.exception.PersistenceException.class,
        () ->
            store
                .begin()
                .updateState(
                    key,
                    value -> {
                      // Deterministically change the revision after the operation read it, before
                      // its CAS write.
                      doAs(
                          TableRuntimeMapper.class,
                          mapper -> mapper.lockCommitState(id.getId(), key.getKey()));
                      return 99L;
                    })
                .commit());
    assertEquals(0L, store.getState(key).longValue());
    assertEquals(
        "0",
        getAs(TableRuntimeMapper.class, mapper -> mapper.getState(id.getId(), key.getKey()))
            .getStateValue());
    store.begin().updateState(key, value -> value + 1).commit();
    assertEquals(1L, store.getState(key).longValue());
  }

  @Test
  void legacyKeysKeepTheirExistingWritePolicy() {
    assertFalse(
        StateKey.stateKey("legacy").longType().defaultValue(0L).usesOptimisticConcurrency());
    assertFalse(DefaultTableRuntime.DEFAULT_PENDING_INPUT_KEY.usesOptimisticConcurrency());
    assertFalse(OptimizingCommitGuard.STATE_KEY.usesOptimisticConcurrency());
  }
}
