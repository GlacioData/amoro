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

package org.apache.amoro.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.amoro.process.rest.ApiError;
import org.apache.amoro.process.rest.ProcessRestSupport;
import org.apache.amoro.resources.ProcessResource;
import org.apache.amoro.rest.MoreFutures;
import org.apache.amoro.rest.ResourceList;
import org.apache.amoro.service.ProcessService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Objects;

/** {@code /api/ams/v2} process endpoints (process spec §8.2), appmanager controller style. */
@RestController
@RequestMapping("/api/ams/v2")
@Tag(name = "Processes")
@ApiResponses({
  @ApiResponse(
      responseCode = "default",
      content = @Content(schema = @Schema(implementation = ApiError.class))),
  @ApiResponse(responseCode = "200", useReturnTypeSchema = true)
})
public class ProcessController {

  private final ProcessService processService;

  public ProcessController(ProcessService processService) {
    this.processService = Objects.requireNonNull(processService, "processService");
  }

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = false)
  @Schema(description = "创建进程请求")
  public static final class CreateRequest {
    @Schema(description = "动作名，lower-kebab，如 dummy-maintenance", example = "dummy-maintenance")
    public String action;

    @Schema(description = "执行引擎：local 或 remote-spark", example = "local")
    public String executionEngine;

    @Schema(description = "动作参数（动作级校验后冻结）")
    public Map<String, Object> parameters;
  }

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = false)
  @Schema(description = "取消进程请求")
  public static final class CancelRequest {
    @Schema(description = "期望状态，本版本仅接受 CANCEL", example = "CANCEL")
    public String desiredState;

    @Schema(description = "取消原因（审计用）")
    public String reason;
  }

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = false)
  @Schema(description = "提交解析请求（UNKNOWN/CONFLICT 人工兜底）")
  public static final class SubmissionResolutionRequest {
    @Schema(description = "提交键，processName:retryNumber:dispatchGeneration")
    public String submissionKey;

    @Schema(description = "提交请求哈希")
    public String requestHash;

    @Schema(description = "解析结论")
    public String resolution;

    @Schema(description = "外部执行 ID（结论为 ACKNOWLEDGED 时必填）")
    public String externalId;

    @Schema(description = "操作原因")
    public String reason;
  }

  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = false)
  @Schema(description = "执行解析请求（LOST/UNRESOLVED 人工兜底）")
  public static final class ExecutionResolutionRequest {
    @Schema(description = "提交键")
    public String submissionKey;

    @Schema(description = "提交请求哈希")
    public String requestHash;

    @Schema(description = "解析结论")
    public String resolution;

    @Schema(description = "是否允许重试")
    public Boolean retryAllowed;

    @Schema(description = "操作原因")
    public String reason;
  }

  @PostMapping("/tables/{catalog}/{db}/{table}/processes")
  @Operation(summary = "创建进程", description = "幂等：同 Idempotency-Key + 同请求内容重放返回原进程")
  public ResponseEntity<ProcessResource> create(
      @Parameter(
              name = "catalog",
              in = ParameterIn.PATH,
              description = "目录名",
              schema = @Schema(implementation = String.class))
          @PathVariable("catalog")
          String catalog,
      @Parameter(name = "db", in = ParameterIn.PATH, description = "库名")
          @PathVariable("db")
          String db,
      @Parameter(name = "table", in = ParameterIn.PATH, description = "表名")
          @PathVariable("table")
          String table,
      @Parameter(name = "Idempotency-Key", in = ParameterIn.HEADER, description = "幂等键")
          @org.springframework.web.bind.annotation.RequestHeader(
              value = "Idempotency-Key",
              required = false)
          String idempotencyKey,
      @RequestBody(description = "创建进程请求")
          @org.springframework.web.bind.annotation.RequestBody
          CreateRequest request) {
    if (request == null) {
      throw ApiError.of("VALIDATION_FAILED", "request body is required");
    }
    ProcessRestSupport.CreateResult result =
        MoreFutures.derefUsingDefaultTimeout(
            processService.create(
                catalog,
                db,
                table,
                idempotencyKey,
                request.action,
                request.executionEngine,
                request.parameters));
    return ResponseEntity.status(result.replay ? HttpStatus.OK : HttpStatus.CREATED)
        .header("Idempotency-Replayed", String.valueOf(result.replay))
        .body(result.resource);
  }

  @GetMapping("/processes/{name}")
  @Operation(summary = "获取单个进程")
  public ProcessResource get(
      @Parameter(name = "name", in = ParameterIn.PATH, description = "进程名")
          @PathVariable("name")
          String name) {
    return MoreFutures.derefUsingDefaultTimeout(processService.get(name));
  }

  @GetMapping("/tables/{catalog}/{db}/{table}/processes")
  @Operation(summary = "进程列表", description = "分页查询指定表的进程；同一快照同时产出当页数据与总数")
  public ResourceList<ProcessResource> listProcesses(
      @Parameter(
              name = "catalog",
              in = ParameterIn.PATH,
              description = "目录名",
              schema = @Schema(implementation = String.class))
          @PathVariable("catalog")
          String catalog,
      @Parameter(name = "db", in = ParameterIn.PATH, description = "库名")
          @PathVariable("db")
          String db,
      @Parameter(name = "table", in = ParameterIn.PATH, description = "表名")
          @PathVariable("table")
          String table,
      @Parameter(name = "action", description = "按动作过滤")
          @RequestParam(value = "action", required = false)
          String action,
      @Parameter(name = "status", description = "按状态过滤")
          @RequestParam(value = "status", required = false)
          String status,
      @Parameter(name = "page", description = "页码，从 1 开始", example = "1")
          @RequestParam(value = "page", defaultValue = "1")
          int page,
      @Parameter(name = "pageSize", description = "每页条数，1..50", example = "20")
          @RequestParam(value = "pageSize", defaultValue = "20")
          int pageSize) {
    ProcessRestSupport.PageResult pageResult =
        MoreFutures.derefUsingDefaultTimeout(
            processService.list(catalog, db, table, action, status, page, pageSize));
    return ResourceList.<ProcessResource>builder()
        .apiVersion(ProcessResource.API_VERSION)
        .kind("ProcessResourceList")
        .metadata(
            ResourceList.ResourceListMetadata.builder()
                .total(pageResult.total)
                .page(page)
                .pageSize(pageSize)
                .build())
        .items(pageResult.items)
        .build();
  }

  @PatchMapping("/processes/{name}")
  @Operation(summary = "取消进程", description = "期望状态仅支持 RUN→CANCEL 单向变更")
  public ProcessResource cancel(
      @Parameter(name = "name", in = ParameterIn.PATH, description = "进程名")
          @PathVariable("name")
          String name,
      @org.springframework.web.bind.annotation.RequestBody CancelRequest request) {
    if (request == null || !"CANCEL".equals(request.desiredState)) {
      throw ApiError.of(
          "VALIDATION_FAILED", "PATCH accepts only {\"desiredState\":\"CANCEL\"} in this version");
    }
    return MoreFutures.derefUsingDefaultTimeout(processService.cancel(name, request.reason));
  }

  @PostMapping("/processes/{name}/submission-resolutions")
  @Operation(summary = "提交解析", description = "对提交结果未知的尝试记录人工结论")
  public ResponseEntity<ProcessResource> submissionResolution(
      @Parameter(name = "name", in = ParameterIn.PATH, description = "进程名")
          @PathVariable("name")
          String name,
      @Parameter(name = "Idempotency-Key", in = ParameterIn.HEADER, description = "幂等键")
          @org.springframework.web.bind.annotation.RequestHeader(
              value = "Idempotency-Key",
              required = false)
          String idempotencyKey,
      @RequestBody(description = "提交解析请求")
          @org.springframework.web.bind.annotation.RequestBody
          SubmissionResolutionRequest request) {
    if (request == null) {
      throw ApiError.of("VALIDATION_FAILED", "request body is required");
    }
    ProcessRestSupport.ResolutionResult result =
        MoreFutures.derefUsingDefaultTimeout(
            processService.submissionResolution(
                name,
                idempotencyKey,
                request.submissionKey,
                request.requestHash,
                request.resolution,
                request.externalId,
                request.reason));
    return ResponseEntity.ok()
        .header("Idempotency-Replayed", String.valueOf(result.replay))
        .body(result.resource);
  }

  @PostMapping("/processes/{name}/execution-resolutions")
  @Operation(summary = "执行解析", description = "对执行结果未知的尝试记录人工结论")
  public ResponseEntity<ProcessResource> executionResolution(
      @Parameter(name = "name", in = ParameterIn.PATH, description = "进程名")
          @PathVariable("name")
          String name,
      @Parameter(name = "Idempotency-Key", in = ParameterIn.HEADER, description = "幂等键")
          @org.springframework.web.bind.annotation.RequestHeader(
              value = "Idempotency-Key",
              required = false)
          String idempotencyKey,
      @RequestBody(description = "执行解析请求")
          @org.springframework.web.bind.annotation.RequestBody
          ExecutionResolutionRequest request) {
    if (request == null) {
      throw ApiError.of("VALIDATION_FAILED", "request body is required");
    }
    ProcessRestSupport.ResolutionResult result =
        MoreFutures.derefUsingDefaultTimeout(
            processService.executionResolution(
                name,
                idempotencyKey,
                request.submissionKey,
                request.requestHash,
                request.resolution,
                request.retryAllowed,
                request.reason));
    return ResponseEntity.ok()
        .header("Idempotency-Replayed", String.valueOf(result.replay))
        .body(result.resource);
  }
}
