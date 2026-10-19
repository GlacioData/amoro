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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.exception.OptimizingCommitSkippedException;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class TestPaimonPrimaryKeyCommitObservability {
  @ParameterizedTest
  @CsvSource({"false,true", "true,true", "false,false", "true,false"})
  void publishedCloseFailureUsesMaintenanceFreeFallbackAndLogsAttempts(
      boolean dynamic, boolean infoEnabled, @TempDir Path warehouse) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
    options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, dynamic, options)) {
      fixture.writeGeneration(0);
      fixture.writeGeneration(1);
      List<PaimonPrimaryKeyCompactionTask> tasks = fixture.prepareTasks();
      List<String> expected = PaimonPrimaryKeyCommitTestSupport.rows(fixture.table());
      FileStoreTable source = spy(fixture.table());
      FileStoreTable copy =
          spy(fixture.table().copy(Collections.singletonMap("write-only", "true")));
      doAnswer(
              call -> {
                assertEquals("true", ((Map<?, ?>) call.getArgument(0)).get("write-only"));
                return copy;
              })
          .when(source)
          .copy(anyMap());
      AtomicBoolean authenticated = new AtomicBoolean();
      PaimonTable authenticatedTable = mock(PaimonTable.class);
      when(authenticatedTable.doAs(any()))
          .thenAnswer(
              call -> {
                authenticated.set(true);
                try {
                  return ((Callable<?>) call.getArgument(0)).call();
                } finally {
                  authenticated.set(false);
                }
              });
      AtomicInteger attempts = new AtomicInteger();
      IOException closeFailure = new IOException("lost close response after publication");
      doAnswer(
              call -> {
                assertTrue(authenticated.get());
                assertTrue(copy.coreOptions().writeOnly());
                TableCommitImpl commit = spy((TableCommitImpl) call.callRealMethod());
                if (attempts.getAndIncrement() == 0) {
                  doAnswer(
                          close -> {
                            assertTrue(authenticated.get());
                            close.callRealMethod();
                            throw closeFailure;
                          })
                      .when(commit)
                      .close();
                }
                return commit;
              })
          .when(copy)
          .newCommit(anyString());
      List<String> events = new ArrayList<>();
      List<Throwable> warnings = new ArrayList<>();
      Logger log = logger(events, warnings, false);
      when(log.isInfoEnabled()).thenReturn(infoEnabled);
      for (PaimonPrimaryKeyCompactionTask task : tasks) {
        task.setOutput(spy(task.getOutput()));
        clearInvocations(task.getOutput());
      }
      PaimonPrimaryKeyTableCommit committer =
          new PaimonPrimaryKeyTableCommit(authenticatedTable, source, tasks, log);
      committer.commit();
      assertEquals(2, attempts.get());
      assertTrue(fixture.table().snapshotManager().snapshotExists(1));
      assertEquals(3L, fixture.table().snapshotManager().latestSnapshotId());
      assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(fixture.table()));
      assertEquals(1, warnings.size());
      assertSame(closeFailure, warnings.get(0));
      if (infoEnabled) {
        assertEvent(events, "stage=native_attempt phase=direct:commit outcome=failed");
        assertEvent(
            events,
            "stage=native_attempt phase=fallback:filterAndCommit outcome=already_committed");
        assertEvent(events, "stage=committer_total phase=commit outcome=already_committed");
        assertEvent(events, "stage=prepare_messages phase=prepare outcome=returned");
        assertEvent(
            events,
            "stage=full_inspection phase=preflight outcome=not_applicable elapsedMs=unavailable");
        assertEvent(events, "unitCount=" + (dynamic ? 2 : 1));
      } else {
        assertEquals(1, events.size(), "WARN must survive when INFO timing is disabled");
        for (PaimonPrimaryKeyCompactionTask task : tasks) {
          verify(task.getOutput(), never()).commitMessageBytesSize();
        }
      }
      assertTrue(events.stream().allMatch(event -> event.contains("maintenancePolicy=external")));
      events.clear();
      committer.commit(CommitMode.RECOVERY_REPLAY);
      if (infoEnabled) {
        assertEvent(events, "phase=recovery:filterAndCommit outcome=already_committed");
      } else {
        assertTrue(events.isEmpty());
      }
      assertEquals(1, warnings.size(), "Successful recovery must not repeat the direct warning");
      assertEquals(3L, fixture.table().snapshotManager().latestSnapshotId());
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void warningFailureDoesNotChangeFallbackOrExceptionChain(boolean fallbackFails) throws Exception {
    FileStoreTable table = mock(FileStoreTable.class);
    when(table.copy(anyMap())).thenReturn(table);
    TableCommitImpl direct = mock(TableCommitImpl.class);
    TableCommitImpl fallback = mock(TableCommitImpl.class);
    when(table.newCommit("user")).thenReturn(direct, fallback);
    List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
    IOException directFailure = new IOException("direct close failed");
    IOException fallbackFailure = new IOException("fallback close failed");
    doThrow(directFailure).when(direct).close();
    if (fallbackFails) {
      doThrow(fallbackFailure).when(fallback).close();
    }
    Logger log = logger(new ArrayList<>(), true);
    when(log.isInfoEnabled()).thenReturn(false);
    PaimonPrimaryKeyTableCommit committer =
        new PaimonPrimaryKeyTableCommit(null, table, Collections.emptyList(), log);
    if (fallbackFails) {
      OptimizingCommitException failure =
          assertThrows(
              OptimizingCommitException.class,
              () -> committer.commitMessages(messages, "user", 7L, CommitMode.NORMAL));
      assertSame(fallbackFailure, failure.getCause());
      assertEquals(1, failure.getSuppressed().length);
      assertSame(directFailure, failure.getSuppressed()[0]);
    } else {
      committer.commitMessages(messages, "user", 7L, CommitMode.NORMAL);
    }
    verify(log).warn(anyString(), any(Object[].class));
    verify(fallback).filterAndCommit(Collections.singletonMap(7L, messages));
    verify(direct).close();
    verify(fallback).close();
  }

  @Test
  void loggingFailureDoesNotTurnPublicationIntoRetry(@TempDir Path warehouse) throws Exception {
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, false, Collections.emptyMap())) {
      fixture.writeGeneration(0);
      fixture.writeGeneration(1);
      new PaimonPrimaryKeyTableCommit(
              null, fixture.table(), fixture.prepareTasks(), logger(new ArrayList<>(), true))
          .commit();
      assertEquals(3L, fixture.table().snapshotManager().latestSnapshotId());
      assertEquals(4, PaimonPrimaryKeyCommitTestSupport.rows(fixture.table()).size());
    }
  }

  @Test
  void recordsNoOpInvalidOutputStaleFullAndReadOnlyReconcile(@TempDir Path warehouse)
      throws Exception {
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, false, Collections.emptyMap())) {
      fixture.writeGeneration(0);
      List<String> events = new ArrayList<>();
      Logger log = logger(events, false);
      new PaimonPrimaryKeyTableCommit(null, fixture.table(), Collections.emptyList(), log).commit();
      assertEvent(events, "outcome=no_tasks");
      PaimonPrimaryKeyCompactionTask task =
          new PaimonPrimaryKeyCompactionTask(
              100L,
              "p",
              new PaimonPrimaryKeyCompactionInput(
                  null, Collections.emptyList(), OptimizingType.FULL, true, 1L, "user", 7L),
              new HashMap<>());
      task.setOutput(new PaimonPrimaryKeyCompactionOutput());
      PaimonPrimaryKeyTableCommit committer =
          new PaimonPrimaryKeyTableCommit(
              null, fixture.table(), Collections.singletonList(task), log);
      committer.commit();
      assertEvent(events, "outcome=no_messages");
      fixture.writeGeneration(1);
      assertThrows(OptimizingCommitSkippedException.class, committer::commit);
      assertEvent(events, "stage=committer_total phase=commit outcome=skipped");
      committer.reconcile();
      assertEvent(events, "stage=committer_total phase=reconcile outcome=replan");
      assertEquals(2L, fixture.table().snapshotManager().latestSnapshotId());
      task.setOutput((PaimonPrimaryKeyCompactionOutput) null);
      assertThrows(OptimizingCommitException.class, committer::commit);
      assertEvent(events, "stage=prepare_messages phase=prepare outcome=failed");
    }
  }

  private static Logger logger(List<String> events, boolean fail) {
    return logger(events, new ArrayList<>(), fail);
  }

  private static Logger logger(List<String> events, List<Throwable> warnings, boolean fail) {
    Logger logger =
        mock(
            Logger.class,
            call -> {
              String level = call.getMethod().getName();
              if ("info".equals(level) || "warn".equals(level)) {
                if (fail) {
                  throw new IllegalStateException("logging unavailable");
                }
                FormattingTuple event =
                    MessageFormatter.arrayFormat(
                        call.getArgument(0),
                        Arrays.copyOfRange(call.getArguments(), 1, call.getArguments().length));
                events.add(event.getMessage());
                if ("warn".equals(level)) {
                  warnings.add(event.getThrowable());
                }
              }
              return RETURNS_DEFAULTS.answer(call);
            });
    when(logger.isInfoEnabled()).thenReturn(true);
    when(logger.isWarnEnabled()).thenReturn(true);
    return logger;
  }

  private static void assertEvent(List<String> events, String fragment) {
    assertTrue(
        events.stream().anyMatch(event -> event.contains(fragment)),
        () -> fragment + " missing in " + events);
  }
}
