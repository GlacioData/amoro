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

package org.apache.amoro.server.dashboard.controller;

import io.javalin.http.Context;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.config.shade.utils.ConfigShadeUtils;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.server.resource.ContainerMetadata;
import org.apache.amoro.server.resource.Containers;
import org.apache.amoro.server.resource.OptimizerManager;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** The controller that handles setting requests. */
public class SettingController {
  private static final String MASK_STRING = "******";
  private static final Set<String> MASK_CONFIGURATION_SET = Sets.newHashSet();

  static {
    MASK_CONFIGURATION_SET.add(AmoroManagementConf.DB_PASSWORD.key());
    MASK_CONFIGURATION_SET.add(AmoroManagementConf.ADMIN_PASSWORD.key());
    MASK_CONFIGURATION_SET.add(
        AmoroManagementConf.AUTHORIZATION_LDAP_ROLE_MAPPING_BIND_PASSWORD.key());
  }

  /**
   * Setting names that hold a credential. Settings are open-ended (plugins may add their own
   * options), so names are matched in addition to the fixed set above and to the keys the operator
   * declared in shade.sensitive-keywords.
   */
  private static final Pattern SENSITIVE_SETTING =
      Pattern.compile(
          "password|passwd|secret|token|credential|api[-_.]?key|access[-_.]?key"
              + "|private[-_.]?key|account[-_.]?key|session[-_.]?key",
          Pattern.CASE_INSENSITIVE);

  private final OptimizerManager optimizerManager;
  private final Configurations serviceConfig;
  private final Set<String> declaredSensitiveKeys;

  public SettingController(Configurations serviceConfig, OptimizerManager optimizerManager) {
    this.optimizerManager = optimizerManager;
    this.serviceConfig = serviceConfig;
    // The operator marks these keys as credentials when having their values encrypted at rest.
    this.declaredSensitiveKeys =
        new HashSet<>(serviceConfig.get(ConfigShadeUtils.SHADE_SENSITIVE_KEYWORDS));
  }

  /** Get system settings. */
  public void getSystemSetting(Context ctx) {
    LinkedHashMap<String, String> result = new LinkedHashMap<>();
    serviceConfig.toMap().entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEachOrdered(entry -> putSetting(result, entry.getKey(), entry.getValue()));
    ctx.json(OkResponse.of(result));
  }

  private void putSetting(Map<String, String> settingMap, String key, Object value) {
    if (isSensitiveSetting(key)) {
      value = MASK_STRING;
    }
    settingMap.put(key, String.valueOf(value));
  }

  private boolean isSensitiveSetting(String key) {
    return MASK_CONFIGURATION_SET.contains(key)
        || declaredSensitiveKeys.contains(key)
        || SENSITIVE_SETTING.matcher(key).find();
  }

  /** Get container settings. */
  public void getContainerSetting(Context ctx) {
    List<ContainerMetadata> containerMetas = Containers.getMetadataList();
    List<Map<String, Object>> result = new ArrayList<>();
    Objects.requireNonNull(containerMetas)
        .forEach(
            container -> {
              List<ResourceGroup> optimizeGroups =
                  optimizerManager.listResourceGroups(container.getName());
              Map<String, Object> obj = new HashMap<>();
              obj.put("name", container.getName());
              obj.put("classpath", container.getImplClass());
              obj.put("properties", container.getProperties());
              obj.put("optimizeGroup", optimizeGroups);
              result.add(obj);
            });

    ctx.json(OkResponse.of(result));
  }
}
