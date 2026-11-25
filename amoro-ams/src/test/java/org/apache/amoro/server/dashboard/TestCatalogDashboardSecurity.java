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

import static org.apache.amoro.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_KEYTAB;
import static org.apache.amoro.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_KRB5;
import static org.apache.amoro.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_PRINCIPAL;
import static org.apache.amoro.properties.CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE;
import static org.apache.amoro.properties.CatalogMetaProperties.CATALOG_TYPE_FILESYSTEM;
import static org.apache.amoro.properties.CatalogMetaProperties.KEY_WAREHOUSE;
import static org.apache.amoro.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_CORE_SITE;
import static org.apache.amoro.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_HDFS_SITE;
import static org.apache.amoro.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_HIVE_SITE;
import static org.apache.amoro.properties.CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE;
import static org.apache.amoro.properties.CatalogMetaProperties.STORAGE_CONFIGS_VALUE_TYPE_HADOOP;
import static org.apache.amoro.properties.CatalogMetaProperties.TABLE_FORMATS;
import static org.apache.amoro.server.dashboard.utils.DesensitizationUtil.COMMON_SECRET_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.http.Context;
import io.javalin.http.UploadedFile;
import jakarta.servlet.http.Part;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.controller.CatalogController;
import org.apache.amoro.server.dashboard.controller.PlatformFileInfoController;
import org.apache.amoro.server.dashboard.model.CatalogRegisterInfo;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.utils.JacksonUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestCatalogDashboardSecurity {
  private static final String CATALOG = "fixture";
  private static final String PRINCIPAL = "account123@EXAMPLE.TEST";
  private static final String MASKED_PRINCIPAL = "****nt123@EXAMPLE.TEST";

  private final CatalogManager catalogs = mock(CatalogManager.class);
  private final PlatformFileManager files = mock(PlatformFileManager.class);
  private final CatalogController controller = new CatalogController(catalogs, files);
  private final Context context = mock(Context.class);

  @ParameterizedTest
  @CsvSource({
    "account123@EXAMPLE.TEST, ****nt123@EXAMPLE.TEST",
    "short@EXAMPLE.TEST, ****@EXAMPLE.TEST",
    "ab/node@EXAMPLE.TEST, ****/node@EXAMPLE.TEST",
    "person123, ****n123",
    "a\\@bcde@EXAMPLE.TEST, ****e@EXAMPLE.TEST"
  })
  void detailsExposeOnlyMaskedPrincipalAndFileNames(String principal, String masked) {
    CatalogMeta catalog = existingCatalog(principal);

    controller.getCatalogDetail(context);

    JsonNode result = result();
    JsonNode auth = result.path("authConfig");
    assertEquals(masked, auth.path(AUTH_CONFIGS_KEY_PRINCIPAL).asText());
    assertFileName(auth.path(AUTH_CONFIGS_KEY_KEYTAB), CATALOG + ".keytab");
    assertFileName(auth.path(AUTH_CONFIGS_KEY_KRB5), "krb5.conf");
    JsonNode storage = result.path("storageConfig");
    assertFileName(storage.path(STORAGE_CONFIGS_KEY_CORE_SITE), "core-site.xml");
    assertFileName(storage.path(STORAGE_CONFIGS_KEY_HDFS_SITE), "hdfs-site.xml");
    assertFileName(storage.path(STORAGE_CONFIGS_KEY_HIVE_SITE), "hive-site.xml");
    assertEquals(principal, catalog.getAuthConfigs().get(AUTH_CONFIGS_KEY_PRINCIPAL));
  }

  @Test
  void detailsMaskSensitivePropertyValuesWithoutChangingStoredMetadata() {
    CatalogMeta catalog = existingCatalog(PRINCIPAL);
    catalog.setCatalogProperties(
        ImmutableMap.<String, String>builder()
            .put(TABLE_FORMATS, TableFormat.ICEBERG.name())
            .put(KEY_WAREHOUSE, "/warehouse")
            .put(S3FileIOProperties.SSE_KEY, "fixture-sse-value")
            .put("oauth2.credential", "fixture-oauth-value")
            .put("rest.token-refresh-enabled", "true")
            .put("table.jdbc.password", "fixture-jdbc-value")
            .put("table.write.format.default", "parquet")
            .build());
    CatalogMeta before = catalog.deepCopy();

    controller.getCatalogDetail(context);

    JsonNode settings = result();
    JsonNode properties = settings.path("properties");
    assertEquals(COMMON_SECRET_FORMAT, properties.path(S3FileIOProperties.SSE_KEY).asText());
    assertEquals(COMMON_SECRET_FORMAT, properties.path("oauth2.credential").asText());
    assertEquals("/warehouse", properties.path(KEY_WAREHOUSE).asText());
    assertEquals("true", properties.path("rest.token-refresh-enabled").asText());
    JsonNode tableProperties = settings.path("tableProperties");
    assertEquals(COMMON_SECRET_FORMAT, tableProperties.path("jdbc.password").asText());
    assertEquals("parquet", tableProperties.path("write.format.default").asText());
    assertEquals(before, catalog);
  }

  @Test
  void editingPropertiesKeepsMaskedValuesAndAppliesExplicitReplacementsAndRemovals() {
    CatalogMeta catalog = existingCatalog(PRINCIPAL);
    catalog.setCatalogProperties(
        ImmutableMap.of(
            TABLE_FORMATS,
            TableFormat.ICEBERG.name(),
            S3FileIOProperties.SSE_KEY,
            "fixture-sse-value",
            "oauth2.credential",
            "fixture-oauth-value",
            "table.jdbc.password",
            "fixture-jdbc-value",
            "table.client.secret",
            "fixture-removed-value"));
    CatalogRegisterInfo request = registerInfo(MASKED_PRINCIPAL);
    request.setProperties(
        ImmutableMap.of(
            KEY_WAREHOUSE,
            "/warehouse",
            S3FileIOProperties.SSE_KEY,
            COMMON_SECRET_FORMAT,
            "oauth2.credential",
            "fixture-replacement",
            "custom.value",
            COMMON_SECRET_FORMAT));
    request.setTableProperties(ImmutableMap.of("jdbc.password", COMMON_SECRET_FORMAT));

    controller.updateCatalog(context);

    Map<String, String> properties = updatedCatalog().getCatalogProperties();
    assertEquals("fixture-sse-value", properties.get(S3FileIOProperties.SSE_KEY));
    assertEquals("fixture-replacement", properties.get("oauth2.credential"));
    assertEquals("fixture-jdbc-value", properties.get("table.jdbc.password"));
    assertFalse(properties.containsKey("table.client.secret"));
    assertEquals(COMMON_SECRET_FORMAT, properties.get("custom.value"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aMaskCannotBeSavedForAPropertyWithoutAnExistingValue(boolean update) {
    if (update) {
      CatalogMeta catalog = existingCatalog(PRINCIPAL);
      catalog.setCatalogProperties(
          ImmutableMap.of(
              TABLE_FORMATS,
              TableFormat.ICEBERG.name(),
              S3FileIOProperties.SSE_KEY,
              "fixture-sse-value"));
    }
    CatalogRegisterInfo request = registerInfo(PRINCIPAL);
    request.setTableProperties(ImmutableMap.of(S3FileIOProperties.SSE_KEY, COMMON_SECRET_FORMAT));

    if (update) {
      assertThrows(IllegalArgumentException.class, () -> controller.updateCatalog(context));
    } else {
      assertThrows(IllegalArgumentException.class, () -> controller.createCatalog(context));
    }
    verify(catalogs, never()).updateCatalog(any(CatalogMeta.class));
    verify(catalogs, never()).createCatalog(any(CatalogMeta.class));
  }

  @Test
  void updatingCatalogPreservesThePrincipalAndAcceptsUploadedFiles() {
    CatalogMeta original = existingCatalog(PRINCIPAL);
    CatalogRegisterInfo request = registerInfo(MASKED_PRINCIPAL);
    request.getAuthConfig().put(AUTH_CONFIGS_KEY_KEYTAB, "7");
    request.setStorageConfig(
        ImmutableMap.of(
            STORAGE_CONFIGS_KEY_TYPE,
            STORAGE_CONFIGS_VALUE_TYPE_HADOOP,
            STORAGE_CONFIGS_KEY_HDFS_SITE,
            "8"));
    when(files.getFileContentB64ById(7)).thenReturn("AwQ=");
    String uploadedSite =
        Base64.getEncoder().encodeToString("<configuration/>\n".getBytes(StandardCharsets.UTF_8));
    when(files.getFileContentB64ById(8)).thenReturn(uploadedSite);

    controller.updateCatalog(context);

    CatalogMeta update = updatedCatalog();
    assertEquals(PRINCIPAL, update.getAuthConfigs().get(AUTH_CONFIGS_KEY_PRINCIPAL));
    assertEquals("AwQ=", update.getAuthConfigs().get(AUTH_CONFIGS_KEY_KEYTAB));
    assertEquals(uploadedSite, update.getStorageConfigs().get(STORAGE_CONFIGS_KEY_HDFS_SITE));
    assertEquals(
        original.getAuthConfigs().get(AUTH_CONFIGS_KEY_KRB5),
        update.getAuthConfigs().get(AUTH_CONFIGS_KEY_KRB5));
    assertEquals(
        original.getStorageConfigs().get(STORAGE_CONFIGS_KEY_CORE_SITE),
        update.getStorageConfigs().get(STORAGE_CONFIGS_KEY_CORE_SITE));
  }

  @Test
  void anExplicitNewPrincipalCanReplaceTheStoredOne() {
    existingCatalog(PRINCIPAL);
    String replacement = "replacement@EXAMPLE.TEST";
    registerInfo(replacement);

    controller.updateCatalog(context);

    assertEquals(replacement, updatedCatalog().getAuthConfigs().get(AUTH_CONFIGS_KEY_PRINCIPAL));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aMaskWithoutMatchingStoredPrincipalCannotBePersisted(boolean update) {
    if (update) {
      existingCatalog(PRINCIPAL);
    }
    registerInfo("****other@EXAMPLE.TEST");

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          if (update) {
            controller.updateCatalog(context);
          } else {
            controller.createCatalog(context);
          }
        });
    verify(catalogs, never()).updateCatalog(any(CatalogMeta.class));
    verify(catalogs, never()).createCatalog(any(CatalogMeta.class));
  }

  @Test
  void uploadReturnsAnIdWithoutADownloadUrl() throws Exception {
    byte[] content = {1, 2};
    Part part = mock(Part.class);
    when(part.getInputStream()).thenReturn(new ByteArrayInputStream(content));
    when(part.getSubmittedFileName()).thenReturn("fixture.keytab");
    when(context.uploadedFile("file")).thenReturn(new UploadedFile(part));
    when(files.addFile("fixture.keytab", "AQI=")).thenReturn(7);

    new PlatformFileInfoController(files).uploadFile(context);

    JsonNode result = result();
    assertEquals("7", result.path("id").asText());
    assertEquals(1, result.size());
  }

  private CatalogMeta existingCatalog(String principal) {
    CatalogMeta catalog = new CatalogMeta();
    catalog.setCatalogName(CATALOG);
    catalog.setCatalogType(CATALOG_TYPE_FILESYSTEM);
    catalog.setCatalogProperties(ImmutableMap.of(TABLE_FORMATS, TableFormat.ICEBERG.name()));
    catalog.setAuthConfigs(
        ImmutableMap.of(
            AUTH_CONFIGS_KEY_TYPE, "kerberos",
            AUTH_CONFIGS_KEY_PRINCIPAL, principal,
            AUTH_CONFIGS_KEY_KEYTAB, "AQI=",
            AUTH_CONFIGS_KEY_KRB5, "YWJj"));
    catalog.setStorageConfigs(
        ImmutableMap.of(
            STORAGE_CONFIGS_KEY_TYPE,
            STORAGE_CONFIGS_VALUE_TYPE_HADOOP,
            STORAGE_CONFIGS_KEY_CORE_SITE,
            "PGNvbmZpZ3VyYXRpb24vPg=="));
    when(catalogs.catalogExist(CATALOG)).thenReturn(true);
    when(catalogs.getCatalogMeta(CATALOG)).thenReturn(catalog);
    when(context.pathParam("catalogName")).thenReturn(CATALOG);
    return catalog;
  }

  private CatalogRegisterInfo registerInfo(String principal) {
    CatalogRegisterInfo info = new CatalogRegisterInfo();
    info.setName(CATALOG);
    info.setType(CATALOG_TYPE_FILESYSTEM);
    info.setOptimizerGroup("local");
    info.setTableFormatList(Collections.singletonList(TableFormat.ICEBERG.name()));
    info.setProperties(Collections.singletonMap(KEY_WAREHOUSE, "/warehouse"));
    info.setTableProperties(Collections.emptyMap());
    info.setStorageConfig(
        Collections.singletonMap(STORAGE_CONFIGS_KEY_TYPE, STORAGE_CONFIGS_VALUE_TYPE_HADOOP));
    info.setAuthConfig(
        new HashMap<>(
            ImmutableMap.of(
                AUTH_CONFIGS_KEY_TYPE, "KERBEROS", AUTH_CONFIGS_KEY_PRINCIPAL, principal)));
    when(context.bodyAsClass(CatalogRegisterInfo.class)).thenReturn(info);
    return info;
  }

  private JsonNode result() {
    ArgumentCaptor<Object> response = ArgumentCaptor.forClass(Object.class);
    verify(context).json(response.capture());
    return JacksonUtil.fromObjects(response.getValue()).path("result");
  }

  private CatalogMeta updatedCatalog() {
    ArgumentCaptor<CatalogMeta> update = ArgumentCaptor.forClass(CatalogMeta.class);
    verify(catalogs).updateCatalog(update.capture());
    return update.getValue();
  }

  private static void assertFileName(JsonNode file, String name) {
    assertTrue(file.isObject());
    assertEquals(name, file.path("fileName").asText());
    assertEquals(1, file.size());
  }
}
