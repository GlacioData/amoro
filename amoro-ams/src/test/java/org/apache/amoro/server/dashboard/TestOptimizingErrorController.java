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

package org.apache.amoro.server.dashboard;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.core.validation.Validator;
import io.javalin.http.Context;
import io.javalin.http.NotFoundResponse;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.controller.TableController;
import org.apache.amoro.server.optimizing.OptimizingTaskMeta;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

public class TestOptimizingErrorController {
  private static final long TABLE_ID = 11L;
  private static final long PROCESS_ID = 22L;
  private static final int TASK_ID = 3;

  private final CatalogManager catalogManager = mock(CatalogManager.class);
  private final TableManager tableManager = mock(TableManager.class);
  private final ServerTableDescriptor descriptor = mock(ServerTableDescriptor.class);
  private final Context context = mock(Context.class);
  private final TableProcessMeta process = new TableProcessMeta();
  private TableController controller;

  @BeforeEach
  public void beforeEach() {
    controller =
        new TableController(catalogManager, tableManager, descriptor, mock(Configurations.class));
    when(context.pathParam("catalog")).thenReturn("catalog");
    when(context.pathParam("db")).thenReturn("database");
    when(context.pathParam("table")).thenReturn("table");
    when(context.pathParam("processId")).thenReturn(Long.toString(PROCESS_ID));
    when(context.pathParam("taskId")).thenReturn(Integer.toString(TASK_ID));
    when(tableManager.getServerTableIdentifier(any()))
        .thenReturn(
            ServerTableIdentifier.of(TABLE_ID, "catalog", "database", "table", TableFormat.PAIMON));
    process.setProcessId(PROCESS_ID);
    process.setTableId(TABLE_ID);
    process.setFailMessage(null);
    when(tableManager.getTableProcessMeta(PROCESS_ID)).thenReturn(process);
  }

  @Test
  public void testProcessErrorReturnsCompleteUtf8PlainText() {
    String error = longUnicodeError();
    process.setFailMessage(error);

    controller.getOptimizingProcessError(context);

    verify(context).header("Cache-Control", "no-store");
    verify(context).contentType("text/plain; charset=UTF-8");
    verify(context).result(error);
  }

  @Test
  public void testProcessWithoutErrorReturnsNoContent() {
    controller.getOptimizingProcessError(context);

    verify(context).header("Cache-Control", "no-store");
    verify(context).status(204);
    verify(context, never()).result(any(String.class));
  }

  @Test
  public void testTaskErrorReturnsCompleteUtf8PlainText() {
    String error = longUnicodeError();
    OptimizingTaskMeta task = new OptimizingTaskMeta();
    task.setTableId(TABLE_ID);
    task.setProcessId(PROCESS_ID);
    task.setTaskId(TASK_ID);
    task.setFailReason(error);
    when(descriptor.findOptimizingTask(TABLE_ID, PROCESS_ID, TASK_ID))
        .thenReturn(Optional.of(task));

    controller.getOptimizingTaskError(context);

    verify(context).header("Cache-Control", "no-store");
    verify(context).contentType("text/plain; charset=UTF-8");
    verify(context).result(error);
  }

  @Test
  public void testProcessListNeverReturnsInlineErrorEvenWhenRequested() {
    stubPageParameters("1", "20", "true");
    when(context.queryParam("type")).thenReturn(null);
    when(context.queryParam("status")).thenReturn(null);
    when(descriptor.getOptimizingProcessesInfo(
            any(), any(), any(), anyInt(), anyInt(), any(), eq(false)))
        .thenReturn(Pair.of(Collections.<OptimizingProcessInfo>emptyList(), 0));

    controller.getOptimizingProcesses(context);

    verify(descriptor)
        .getOptimizingProcessesInfo(any(), any(), any(), eq(20), eq(0), any(), eq(false));
  }

  @Test
  public void testTaskListUsesBoundedDatabasePageWithoutInlineError() {
    stubPageParameters("2", "20", "true");
    when(descriptor.getOptimizingProcessTaskInfos(any(), anyString(), eq(20), eq(20), eq(false)))
        .thenReturn(Pair.of(Collections.<OptimizingTaskInfo>emptyList(), 0));

    controller.getOptimizingProcessTasks(context);

    verify(descriptor)
        .getOptimizingProcessTaskInfos(
            any(), eq(Long.toString(PROCESS_ID)), eq(20), eq(20), eq(false));
  }

  @Test
  public void testOptimizingHistoryRejectsZeroAndOversizedPages() {
    stubPageParameters("1", "0", "false");
    assertThrows(IllegalArgumentException.class, () -> controller.getOptimizingProcesses(context));

    stubPageParameters("1", "101", "false");
    assertThrows(
        IllegalArgumentException.class, () -> controller.getOptimizingProcessTasks(context));
  }

  @Test
  public void testCrossTableProcessIsNotFound() {
    process.setTableId(TABLE_ID + 1);

    assertThrows(NotFoundResponse.class, () -> controller.getOptimizingProcessError(context));
    verify(context, never()).result(any(String.class));
  }

  @Test
  public void testMissingTaskIsNotFound() {
    when(descriptor.findOptimizingTask(TABLE_ID, PROCESS_ID, TASK_ID)).thenReturn(Optional.empty());

    assertThrows(NotFoundResponse.class, () -> controller.getOptimizingTaskError(context));
  }

  private static String longUnicodeError() {
    StringBuilder error = new StringBuilder("异常-🚀\n");
    for (int i = 0; i < 2_000; i++) {
      error.append("at 测试.完整堆栈(").append(i).append(")\n");
    }
    return error.append("尾部-😃").toString();
  }

  private void stubPageParameters(String page, String pageSize, String includeError) {
    when(context.queryParamAsClass("page", Integer.class))
        .thenReturn(new Validator<>(page, Integer.class, "page"));
    when(context.queryParamAsClass("pageSize", Integer.class))
        .thenReturn(new Validator<>(pageSize, Integer.class, "pageSize"));
    when(context.queryParamAsClass("includeError", Boolean.class))
        .thenReturn(new Validator<>(includeError, Boolean.class, "includeError"));
  }
}
