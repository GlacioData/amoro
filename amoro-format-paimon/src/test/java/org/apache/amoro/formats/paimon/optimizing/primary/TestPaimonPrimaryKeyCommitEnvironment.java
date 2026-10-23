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

package org.apache.amoro.formats.paimon.optimizing.primary;

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyProtectedCommit.committer;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyProtectedCommit.prepare;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyReadDependencies.fixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

class TestPaimonPrimaryKeyCommitEnvironment {
  @Test
  void allNativeEnvironmentOperationsDelegateIncludingCopyAndClose() throws Exception {
    TestPaimonPrimaryKeyCommitGuard.Harness h =
        new TestPaimonPrimaryKeyCommitGuard.Harness(1, false);
    h.guard.prepare(h.messages);
    CatalogEnvironment original = mock(CatalogEnvironment.class, RETURNS_DEEP_STUBS);
    CatalogEnvironment wrapped = new PaimonPrimaryKeyCommitEnvironment(original, h.guard);
    // Enumerate the pinned native API so a later Paimon upgrade cannot silently omit delegation.
    for (Method method : CatalogEnvironment.class.getDeclaredMethods()) {
      if (!Modifier.isPublic(method.getModifiers())
          || Modifier.isStatic(method.getModifiers())
          || method.getName().equals("snapshotCommit")
          || method.getName().equals("copy")) {
        continue;
      }
      Object[] args =
          method.getParameterCount() == 0
              ? new Object[0]
              : new Object[] {CoreOptions.fromMap(Collections.emptyMap())};
      assertEquals(method.invoke(original, args), method.invoke(wrapped, args), method.getName());
      method.invoke(verify(original, times(2)), args);
    }
    Identifier branch = new Identifier("db", "table", "other");
    CatalogEnvironment copied = wrapped.copy(branch);
    assertTrue(copied instanceof PaimonPrimaryKeyCommitEnvironment);
    SnapshotCommit delegate = original.copy(branch).snapshotCommit(h.manager);
    when(delegate.commit(any(), anyString(), anyList())).thenReturn(true);
    try (SnapshotCommit commit = copied.snapshotCommit(h.manager)) {
      assertThrows(
          PaimonPrimaryKeyCommitGuard.ReadConflict.class,
          () -> commit.commit(h.candidate(11), "other", Collections.emptyList()));
    }
    verify(delegate).close();
  }

  @Test
  void originalCatalogLockIsUsedAndClosed(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "lookup");
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.FULL);
      CatalogLock lock = mock(CatalogLock.class);
      CatalogLockFactory factory = mock(CatalogLockFactory.class);
      CatalogLockContext context = mock(CatalogLockContext.class);
      when(factory.createLock(context)).thenReturn(lock);
      when(lock.runWithLock(eq("db"), eq(t.name()), any()))
          .thenAnswer(call -> call.getArgument(2, Callable.class).call());
      CatalogEnvironment original =
          new CatalogEnvironment(
              Identifier.create("db", t.name()),
              t.uuid(),
              null,
              factory,
              context,
              null,
              false,
              false);
      FileStoreTable locked =
          new PrimaryKeyFileStoreTable(t.fileIO(), t.location(), t.schema(), original);
      List<String> expected = rows(t);
      committer(locked, task).commit();
      verify(factory).createLock(context);
      verify(lock).runWithLock(eq("db"), eq(t.name()), any());
      verify(lock).close();
      assertEquals(expected, rows(t));
      assertEquals(5L, t.snapshotManager().latestSnapshotId());
    }
  }

  @Test
  void realBranchPublishesOnlyToItsOwnHistoryAndRejectsCrossBranchEvidence(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "lookup");
      t.createTag("base", 4L);
      t.createBranch("review", "base");
      FileStoreTable branch = t.switchToBranch("review");
      PaimonPrimaryKeyCompactionTask task = prepare(branch, OptimizingType.FULL);
      List<String> expected = rows(branch);
      committer(branch, task).commit();
      assertEquals(5L, branch.snapshotManager().latestSnapshotId());
      assertEquals(4L, t.snapshotManager().latestSnapshotId());
      assertEquals(expected, rows(branch));
      assertEquals(expected, rows(t));
      committer(branch, task).commit(CommitMode.RECOVERY_REPLAY);
      assertThrows(OptimizingCommitException.class, () -> committer(t, task).commit());
      assertFalse(t.snapshotManager().snapshotExists(5L));
    }
  }
}
