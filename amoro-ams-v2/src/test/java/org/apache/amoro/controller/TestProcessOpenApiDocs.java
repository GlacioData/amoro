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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Verifies the springdoc OpenAPI surface: the documented tag, the create path and the K8s-style
 * list envelope with pagination metadata are all present in {@code /v3/api-docs}.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.MOCK,
    properties = {
      "spring.datasource.url=jdbc:derby:memory:amoroV2OpenApi;create=true",
      "spring.datasource.driver-class-name=org.apache.derby.iapi.jdbc.AutoloadedDriver",
      "spring.sql.init.mode=never"
    })
@AutoConfigureMockMvc
class TestProcessOpenApiDocs {

  @Autowired private MockMvc mvc;

  @Test
  void openApiDocumentsProcessTagAndCreatePath() throws Exception {
    mvc.perform(get("/v3/api-docs"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.paths['/api/ams/v2/tables/{catalog}/{db}/{table}/processes']")
            .exists())
        .andExpect(jsonPath("$.paths['/api/ams/v2/processes/{name}']").exists());
  }

  @Test
  void openApiDocumentsResourceListSchemaWithPaginationMetadata() throws Exception {
    mvc.perform(get("/v3/api-docs"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.components.schemas.ResourceListProcessResource").exists())
        .andExpect(jsonPath("$.components.schemas.ResourceListMetadata").exists())
        .andExpect(jsonPath("$.components.schemas.ResourceListMetadata.properties.total").exists())
        .andExpect(jsonPath("$.components.schemas.ResourceListMetadata.properties.page").exists())
        .andExpect(
            jsonPath("$.components.schemas.ResourceListMetadata.properties.pageSize").exists());
  }
}
