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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class TestSelfOptimizingGrafanaDashboard {

  private static JsonNode dashboard;

  @BeforeAll
  static void loadDashboard() throws IOException {
    Path path = Paths.get("grafana/dashboard.json");
    if (!Files.exists(path)) {
      path = Paths.get("../grafana/dashboard.json");
    }
    assertTrue(Files.exists(path), "Expected grafana/dashboard.json to exist");
    dashboard = new ObjectMapper().readTree(path.toFile());
  }

  @Test
  void selfOptimizingTablesStatShowsTotalEnabledDisabledAndRatioFromSingleGauge() {
    JsonNode panel = findTopLevelPanelByTitle("Self Optimizing Tables");
    assertNotNull(panel, "Missing 'Self Optimizing Tables' stat panel");
    assertEquals("stat", panel.path("type").asText());
    assertEquals("none", panel.path("fieldConfig").path("defaults").path("unit").asText());

    Map<String, String> exprByLegend = targetsByLegend(panel);
    assertEquals(
        Arrays.asList("Total", "Enabled", "Disabled", "Enabled Ratio"),
        new ArrayList<>(exprByLegend.keySet()));

    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\"})",
        exprByLegend.get("Total"));
    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\", enabled=\"true\"})",
        exprByLegend.get("Enabled"));
    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\", enabled=\"false\"})",
        exprByLegend.get("Disabled"));
    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\", enabled=\"true\"})"
            + " / (sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\"}) > 0)",
        exprByLegend.get("Enabled Ratio"));

    exprByLegend
        .values()
        .forEach(
            expr -> {
              assertFalse(
                  expr.contains("amoro_table_optimizing_process_total_count_total"),
                  "Must not mix per-table counter with catalog/format gauge: " + expr);
              assertFalse(expr.contains("$Database"), "Gauge has no database label: " + expr);
              assertFalse(expr.contains("$Table"), "Gauge has no table label: " + expr);
            });
  }

  @Test
  void selfOptimizingTablesTrendShowsExactTotalEnabledAndDisabledCounts() {
    JsonNode panel = findTopLevelPanelByTitle("Self Optimizing Tables Trend");
    assertNotNull(panel, "Missing 'Self Optimizing Tables Trend' panel");
    assertEquals("timeseries", panel.path("type").asText());
    assertEquals(
        "none",
        panel.path("fieldConfig").path("defaults").path("unit").asText(),
        "Count trend must use exact integer unit instead of 'short' rounding (e.g. 6722 -> 7 K)");
    assertEquals(0, panel.path("fieldConfig").path("defaults").path("decimals").asInt());
    assertEquals(
        "stepAfter",
        panel
            .path("fieldConfig")
            .path("defaults")
            .path("custom")
            .path("lineInterpolation")
            .asText());

    Map<String, String> exprByLegend = targetsByLegend(panel);
    assertEquals(
        Arrays.asList("Total", "Enabled", "Disabled"), new ArrayList<>(exprByLegend.keySet()));
    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\"})",
        exprByLegend.get("Total"));
    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\", enabled=\"true\"})",
        exprByLegend.get("Enabled"));
    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\", enabled=\"false\"})",
        exprByLegend.get("Disabled"));
  }

  @Test
  void selfOptimizingEnabledTrendAutoScalesEnabledCountAndShowsRatioWithoutClippedLabel() {
    JsonNode panel = findTopLevelPanelByTitle("Self Optimizing Enabled Trend");
    assertNotNull(panel, "Missing dedicated 'Self Optimizing Enabled Trend' panel");
    assertEquals("timeseries", panel.path("type").asText());

    JsonNode defaults = panel.path("fieldConfig").path("defaults");
    assertEquals("none", defaults.path("unit").asText());
    assertEquals(0, defaults.path("decimals").asInt());
    assertEquals("stepAfter", defaults.path("custom").path("lineInterpolation").asText());
    assertTrue(
        defaults.path("min").isMissingNode() || defaults.path("min").isNull(),
        "Enabled trend left axis must auto-scale instead of locking min=0");

    Map<String, String> exprByLegend = targetsByLegend(panel);
    assertEquals(Arrays.asList("Enabled", "Enabled Ratio"), new ArrayList<>(exprByLegend.keySet()));
    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\", enabled=\"true\"})",
        exprByLegend.get("Enabled"));
    assertEquals(
        "sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\", enabled=\"true\"})"
            + " / (sum(amoro_ams_self_optimizing_tables{instance=~\"$Instance\", catalog=~\"$Catalog\"}) > 0)",
        exprByLegend.get("Enabled Ratio"));

    JsonNode ratioOverride = findOverrideByName(panel, "Enabled Ratio");
    assertNotNull(ratioOverride, "Missing field override for 'Enabled Ratio'");
    Map<String, JsonNode> props = overrideProperties(ratioOverride);
    assertEquals("percentunit", props.get("unit").asText());
    assertEquals("right", props.get("custom.axisPlacement").asText());
    if (props.containsKey("custom.axisLabel")) {
      assertEquals("", props.get("custom.axisLabel").asText());
    }
  }

  @Test
  void instanceFilterOffersInstancesThatOnlyReportTheTableGauge() {
    String selector =
        "label_values({__name__=~\"amoro_ams_jvm_cpu_load|"
            + "amoro_ams_self_optimizing_tables\"},instance)";
    JsonNode instance = findVariable("Instance");
    assertEquals(selector, instance.path("query").path("query").asText());
    assertEquals(selector, instance.path("definition").asText());
  }

  @Test
  void catalogFilterOffersCatalogsFromBothTableMetrics() {
    String selector =
        "label_values({__name__=~\"amoro_ams_self_optimizing_tables|"
            + "amoro_table_optimizing_process_total_count_total\",instance=~\"$Instance\"},catalog)";
    JsonNode catalog = findVariable("Catalog");
    assertEquals(selector, catalog.path("query").path("query").asText());
    assertEquals(selector, catalog.path("definition").asText());
  }

  private static JsonNode findVariable(String name) {
    for (JsonNode variable : dashboard.path("templating").path("list")) {
      if (name.equals(variable.path("name").asText())) {
        return variable;
      }
    }
    throw new AssertionError("Missing dashboard variable " + name);
  }

  @Test
  void preservesUnrelatedPanelsVariablesUniqueIdsAndNonOverlappingLayout() {
    Set<Integer> ids = new HashSet<>();
    collectAndAssertUniqueIds(dashboard.path("panels"), ids);

    List<String> expectedTopTitles =
        Arrays.asList(
            "Overview",
            "Self Optimizing",
            "Optimizer Group",
            "Planing Table Count",
            "Pending Table Count",
            "Executing Table Count",
            "Pending Task Count",
            "Executing Task Count",
            "Debug",
            "Top Failed Count",
            "Top Failed Count (Window)",
            "Failed Count Trend (Cumulative)",
            "Self Optimizing Tables",
            "Self Optimizing Tables Trend",
            "Self Optimizing Enabled Trend",
            "AMS JVM");
    for (String title : expectedTopTitles) {
      assertNotNull(
          findTopLevelPanelByTitle(title), "Expected top-level panel to remain: " + title);
    }

    List<String> variables = new ArrayList<>();
    for (JsonNode varNode : dashboard.path("templating").path("list")) {
      variables.add(varNode.path("name").asText());
    }
    assertEquals(
        Arrays.asList("Datasource", "Instance", "Catalog", "Database", "Table", "TopN"), variables);

    List<JsonNode> topPanels = new ArrayList<>();
    dashboard.path("panels").forEach(topPanels::add);
    for (int i = 0; i < topPanels.size(); i++) {
      JsonNode a = topPanels.get(i).path("gridPos");
      int ax1 = a.path("x").asInt();
      int ay1 = a.path("y").asInt();
      int ax2 = ax1 + a.path("w").asInt();
      int ay2 = ay1 + a.path("h").asInt();
      for (int j = i + 1; j < topPanels.size(); j++) {
        JsonNode b = topPanels.get(j).path("gridPos");
        int bx1 = b.path("x").asInt();
        int by1 = b.path("y").asInt();
        int bx2 = bx1 + b.path("w").asInt();
        int by2 = by1 + b.path("h").asInt();
        boolean overlaps = ax1 < bx2 && ax2 > bx1 && ay1 < by2 && ay2 > by1;
        assertFalse(
            overlaps,
            "Overlapping gridPos between '"
                + topPanels.get(i).path("title").asText()
                + "' and '"
                + topPanels.get(j).path("title").asText()
                + "'");
      }
    }
  }

  private static void collectAndAssertUniqueIds(JsonNode panels, Set<Integer> ids) {
    for (JsonNode panel : panels) {
      int id = panel.path("id").asInt();
      assertTrue(ids.add(id), "Duplicate panel id: " + id);
      if (panel.has("panels")) {
        collectAndAssertUniqueIds(panel.path("panels"), ids);
      }
    }
  }

  private static JsonNode findTopLevelPanelByTitle(String title) {
    for (JsonNode panel : dashboard.path("panels")) {
      if (title.equals(panel.path("title").asText())) {
        return panel;
      }
    }
    return null;
  }

  private static Map<String, String> targetsByLegend(JsonNode panel) {
    Map<String, String> map = new LinkedHashMap<>();
    for (JsonNode target : panel.path("targets")) {
      map.put(target.path("legendFormat").asText(), target.path("expr").asText());
    }
    return map;
  }

  private static JsonNode findOverrideByName(JsonNode panel, String name) {
    for (JsonNode override : panel.path("fieldConfig").path("overrides")) {
      JsonNode matcher = override.path("matcher");
      if ("byName".equals(matcher.path("id").asText())
          && name.equals(matcher.path("options").asText())) {
        return override;
      }
    }
    return null;
  }

  private static Map<String, JsonNode> overrideProperties(JsonNode override) {
    Map<String, JsonNode> props = new LinkedHashMap<>();
    for (JsonNode prop : override.path("properties")) {
      props.put(prop.path("id").asText(), prop.path("value"));
    }
    return props;
  }
}
