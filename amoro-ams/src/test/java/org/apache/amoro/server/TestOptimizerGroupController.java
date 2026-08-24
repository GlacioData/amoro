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

package org.apache.amoro.server;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.core.validation.Validator;
import io.javalin.http.Context;
import org.apache.amoro.OptimizerProperties;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.dashboard.controller.OptimizerGroupController;
import org.apache.amoro.server.resource.OptimizerManager;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.server.table.TableOptimizingQuery;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import javax.ws.rs.BadRequestException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class TestOptimizerGroupController {

  private TableManager tableManager;
  private DefaultOptimizingService optimizingService;
  private OptimizerManager optimizerManager;
  private OptimizerGroupController controller;
  private Context ctx;

  @BeforeEach
  void setUp() {
    tableManager = mock(TableManager.class);
    optimizingService = mock(DefaultOptimizingService.class);
    optimizerManager = mock(OptimizerManager.class);
    controller = new OptimizerGroupController(tableManager, optimizerManager);
    ctx = mock(Context.class);
  }

  private static Stream<String> invalidGroupNames() {
    return Stream.of(
        "invalid group name!",
        "invalid@group!",
        "",
        null,
        "invalidGroupName\t",
        "invalidGroupName\n",
        "无效组名",
        "invalid.group.name",
        "invalidinvalidinvalidinvalidinvalidinvalidinvalidin");
  }

  @ParameterizedTest
  @MethodSource("invalidGroupNames")
  void createInvalidGroupName(String groupName) {
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("name", groupName);
    requestBody.put("container", "Container");
    requestBody.put("properties", new HashMap<String, String>());

    when(ctx.bodyAsClass(Map.class)).thenReturn(requestBody);
    assertThrows(BadRequestException.class, () -> controller.createResourceGroup(ctx));
  }

  private Map<String, Object> groupRequest(String name, Map<String, String> properties) {
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("name", name);
    requestBody.put("container", "flink");
    requestBody.put("properties", properties);
    return requestBody;
  }

  @Test
  void createWithInvalidDynamicAllocationIsRejected() {
    Map<String, String> properties = new HashMap<>();
    // enabled without the required max-parallelism
    properties.put(OptimizerProperties.DYNAMIC_ALLOCATION_ENABLED, "true");

    when(ctx.bodyAsClass(Map.class)).thenReturn(groupRequest("group1", properties));
    assertThrows(BadRequestException.class, () -> controller.createResourceGroup(ctx));
  }

  @Test
  void updateWithInvalidDynamicAllocationIsRejected() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerProperties.DYNAMIC_ALLOCATION_ENABLED, "true");
    properties.put(OptimizerProperties.DYNAMIC_ALLOCATION_MAX_PARALLELISM, "2048");

    when(ctx.bodyAsClass(Map.class)).thenReturn(groupRequest("group1", properties));
    assertThrows(BadRequestException.class, () -> controller.updateResourceGroup(ctx));
  }

  @Test
  void createWithValidDynamicAllocationSucceeds() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerProperties.DYNAMIC_ALLOCATION_ENABLED, "true");
    properties.put(OptimizerProperties.DYNAMIC_ALLOCATION_MAX_PARALLELISM, "16");

    when(ctx.bodyAsClass(Map.class)).thenReturn(groupRequest("group1", properties));
    controller.createResourceGroup(ctx);
    verify(optimizerManager).createResourceGroup(any(ResourceGroup.class));
  }

  @Test
  void optimizerTableQueryMapsDerivedFiltersAndDefaultsSortToDescending() {
    stubDefaultTableQuery();
    when(ctx.queryParam("healthScoreMin")).thenReturn("30");
    when(ctx.queryParam("healthScoreMax")).thenReturn("60");
    when(ctx.queryParam("durationMinMinutes")).thenReturn("30");
    when(ctx.queryParam("sortBy")).thenReturn("quota");
    when(ctx.queryParamAsClass("healthScoreMin", Integer.class))
        .thenReturn(new Validator<>("30", Integer.class, "healthScoreMin"));
    when(ctx.queryParamAsClass("healthScoreMax", Integer.class))
        .thenReturn(new Validator<>("60", Integer.class, "healthScoreMax"));
    when(ctx.queryParamAsClass("durationMinMinutes", Long.class))
        .thenReturn(new Validator<>("30", Long.class, "durationMinMinutes"));

    controller.getOptimizerTables(ctx);

    ArgumentCaptor<TableOptimizingQuery> queryCaptor =
        ArgumentCaptor.forClass(TableOptimizingQuery.class);
    verify(tableManager).queryTableOptimizingInfo(queryCaptor.capture());
    TableOptimizingQuery query = queryCaptor.getValue();
    assertEquals(Integer.valueOf(30), query.getHealthScoreMin());
    assertEquals(Integer.valueOf(60), query.getHealthScoreMax());
    assertEquals(Long.valueOf(30), query.getDurationMinMinutes());
    assertEquals(TableOptimizingQuery.SortField.QUOTA, query.getSortField());
    assertEquals(TableOptimizingQuery.SortOrder.DESC, query.getSortOrder());
  }

  @Test
  void optimizerTableQueryRejectsUnsupportedSortField() {
    stubDefaultTableQuery();
    when(ctx.queryParam("sortBy")).thenReturn("fileCount");

    assertThrows(BadRequestException.class, () -> controller.getOptimizerTables(ctx));
  }

  private void stubDefaultTableQuery() {
    when(ctx.pathParam("optimizerGroup")).thenReturn("all");
    when(ctx.queryParamAsClass("page", Integer.class))
        .thenReturn(new Validator<>("1", Integer.class, "page"));
    when(ctx.queryParamAsClass("pageSize", Integer.class))
        .thenReturn(new Validator<>("25", Integer.class, "pageSize"));
    when(ctx.queryParams("actions[]")).thenReturn(Collections.emptyList());
    when(tableManager.queryTableOptimizingInfo(any(TableOptimizingQuery.class)))
        .thenReturn(Pair.of(Collections.emptyList(), 0));
  }
}
