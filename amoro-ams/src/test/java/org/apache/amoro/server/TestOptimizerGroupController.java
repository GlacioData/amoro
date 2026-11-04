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
import org.apache.amoro.api.OptimizerRegisterInfo;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.dashboard.controller.OptimizerGroupController;
import org.apache.amoro.server.dashboard.model.OptimizerInstanceInfo;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.server.dashboard.response.PageResult;
import org.apache.amoro.server.resource.OptimizerInstance;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

  private static final String OPTIMIZER_CONTAINER = "localContainer";
  private static final long START_TIME_BASE = 1_700_000_000_000L;

  /**
   * Builds an optimizer whose every field is derived from {@code index}: startTime is {@code
   * START_TIME_BASE + index * 7000}, so a bigger index means a later start time and must come first
   * in the descending-start-time sort the endpoint applies.
   */
  private OptimizerInstance newOptimizerInstance(int index) {
    OptimizerRegisterInfo registerInfo =
        new OptimizerRegisterInfo()
            .setResourceId("job-" + index)
            .setGroupName("group1")
            .setThreadCount(index + 1)
            .setMemoryMb(1024 + index)
            .setStartTime(START_TIME_BASE + index * 7_000L);
    return new OptimizerInstance(registerInfo, OPTIMIZER_CONTAINER);
  }

  /** Instances ordered by ascending start time, so the endpoint's sort has to reverse them. */
  private List<OptimizerInstance> newAscendingOptimizerInstances(int count) {
    List<OptimizerInstance> instances = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      instances.add(newOptimizerInstance(i));
    }
    return instances;
  }

  /**
   * The DTO the endpoint must expose for the instance built with {@code index}; token and touchTime
   * are generated inside {@link OptimizerInstance} and asserted as passthrough values.
   */
  private OptimizerInstanceInfo expectedInfo(OptimizerInstance instance, int index) {
    return OptimizerInstanceInfo.builder()
        .token(instance.getToken())
        .startTime(START_TIME_BASE + index * 7_000L)
        .touchTime(instance.getTouchTime())
        .jobId("job-" + index)
        .groupName("group1")
        .coreNumber(index + 1)
        .memory(1024 + index)
        .jobStatus("RUNNING")
        .container(OPTIMIZER_CONTAINER)
        .build();
  }

  private List<OptimizerInstanceInfo> expectedInfos(
      List<OptimizerInstance> instances, int... indexes) {
    List<OptimizerInstanceInfo> expected = new ArrayList<>();
    for (int index : indexes) {
      expected.add(expectedInfo(instances.get(index), index));
    }
    return expected;
  }

  private void stubOptimizerListQuery(String group, String page, String pageSize) {
    when(ctx.pathParam("optimizerGroup")).thenReturn(group);
    when(ctx.queryParamAsClass("page", Integer.class))
        .thenReturn(new Validator<>(page, Integer.class, "page"));
    when(ctx.queryParamAsClass("pageSize", Integer.class))
        .thenReturn(new Validator<>(pageSize, Integer.class, "pageSize"));
  }

  private PageResult<?> getOptimizersAndCapturePageResult() {
    controller.getOptimizers(ctx);

    ArgumentCaptor<OkResponse<PageResult<?>>> captor = ArgumentCaptor.forClass(OkResponse.class);
    verify(ctx).json(captor.capture());
    return captor.getValue().getResult();
  }

  @Test
  void getOptimizersReturnsFirstPageSortedByStartTimeDescending() {
    List<OptimizerInstance> ascending = newAscendingOptimizerInstances(25);
    stubOptimizerListQuery("group1", "1", "20");
    when(optimizerManager.listOptimizers("group1")).thenReturn(new ArrayList<>(ascending));

    PageResult<?> result = getOptimizersAndCapturePageResult();

    assertEquals(25, result.getTotal());
    assertEquals(
        expectedInfos(
            ascending, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5),
        result.getList());
  }

  @Test
  void getOptimizersReturnsSecondPageWithRemainingInstancesAndFullTotal() {
    List<OptimizerInstance> ascending = newAscendingOptimizerInstances(25);
    stubOptimizerListQuery("all", "2", "20");
    when(optimizerManager.listOptimizers()).thenReturn(new ArrayList<>(ascending));

    PageResult<?> result = getOptimizersAndCapturePageResult();

    assertEquals(25, result.getTotal());
    assertEquals(expectedInfos(ascending, 4, 3, 2, 1, 0), result.getList());
  }

  @Test
  void getOptimizersWithOffsetBeyondTotalReturnsEmptyListAndFullTotal() {
    List<OptimizerInstance> ascending = newAscendingOptimizerInstances(25);
    stubOptimizerListQuery("group1", "4", "20");
    when(optimizerManager.listOptimizers("group1")).thenReturn(new ArrayList<>(ascending));

    PageResult<?> result = getOptimizersAndCapturePageResult();

    assertEquals(25, result.getTotal());
    assertEquals(Collections.emptyList(), result.getList());
  }
}
