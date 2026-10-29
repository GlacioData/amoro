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

package org.apache.amoro.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class TestMetricMetadataSnapshot {

  @Test
  void definitionSnapshotsDeclaredTagOrderAndKeepsCanonicalIdentity() {
    List<String> tags = new ArrayList<>(Arrays.asList("table", "catalog"));
    MetricDefine definition = new MetricDefine("records", tags, MetricType.Gauge, "help");
    int hash = definition.hashCode();
    tags.set(0, "changed");

    assertEquals(Arrays.asList("table", "catalog"), definition.getTags());
    assertEquals("records,type=Gauge<catalog,table>", definition.toString());
    assertEquals(Objects.hash("records,type=Gauge<catalog,table>"), hash);
    assertEquals(hash, definition.hashCode());
    assertThrows(UnsupportedOperationException.class, () -> definition.getTags().clear());
  }

  @Test
  void canonicalDescriptionIsReusedWithoutChangingEquality() {
    MetricDefine first =
        MetricDefine.defineGauge("records")
            .withTags("table", "catalog")
            .withDescription("one")
            .build();
    MetricDefine second =
        MetricDefine.defineGauge("records")
            .withTags("catalog", "table")
            .withDescription("two")
            .build();

    assertSame(first.toString(), first.toString());
    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertEquals("one", first.getDescription());
    assertEquals("two", second.getDescription());
  }

  @Test
  void callerMutationCannotChangeRegisteredLabelsOrBreakUnregister() {
    MetricDefine definition = MetricDefine.defineGauge("records").withTags("table").build();
    Map<String, String> labels = new HashMap<>();
    labels.put("table", "original");
    MetricRegistry registry = new MetricRegistry();
    Gauge<Long> gauge = () -> 42L;
    MetricKey key = registry.register(definition, labels, gauge);
    int hash = key.hashCode();
    labels.put("table", "changed");
    labels.put("extra", "unexpected");

    assertEquals("original", key.valueOfTag("table"));
    assertEquals(Collections.singletonList("original"), key.valueOfTags());
    assertEquals(hash, key.hashCode());
    assertSame(gauge, registry.getMetrics().get(key));
    registry.unregister(key);
    assertEquals(0, registry.getMetrics().size());
    assertEquals(0, registry.metricDefineCount("records"));
  }

  @Test
  void orderedLabelValuesAreReusedAndImmutable() {
    MetricDefine definition =
        MetricDefine.defineGauge("records").withTags("table", "catalog").build();
    Map<String, String> labels = new HashMap<>();
    labels.put("catalog", "lake");
    labels.put("table", "orders");
    MetricKey key = new MetricKey(definition, labels);

    assertEquals(Arrays.asList("orders", "lake"), key.valueOfTags());
    assertSame(key.valueOfTags(), key.valueOfTags());
    assertEquals(Objects.hash(definition, labels), key.hashCode());
    assertEquals("", key.valueOfTag("unknown"));
    assertThrows(UnsupportedOperationException.class, () -> key.valueOfTags().set(0, "changed"));
  }

  @Test
  void nullLabelValuesKeepTheirExistingRepresentation() {
    MetricDefine definition = MetricDefine.defineGauge("records").withTags("table").build();
    Map<String, String> labels = new HashMap<>();
    labels.put("table", null);
    MetricKey key = new MetricKey(definition, labels);

    assertNull(key.valueOfTag("table"));
    assertEquals(Collections.singletonList(null), key.valueOfTags());
    assertEquals(Objects.hash(definition, labels), key.hashCode());
  }

  @Test
  void emptyLabelsAcceptNullAndKeepTheirExistingIdentity() {
    MetricDefine definition = new MetricDefine("records", null, MetricType.Gauge, "help");
    MetricKey key = new MetricKey(definition, null);

    assertEquals(Collections.emptyList(), definition.getTags());
    assertEquals(Collections.emptyList(), key.valueOfTags());
    assertSame(key.valueOfTags(), key.valueOfTags());
    assertEquals(Objects.hash("records,type=Gauge<>"), definition.hashCode());
    assertEquals(Objects.hash(definition, Collections.emptyMap()), key.hashCode());
  }

  @Test
  void missingExtraAndWrongLabelNamesAreStillRejected() {
    MetricDefine definition = MetricDefine.defineGauge("records").withTags("table").build();
    assertThrows(IllegalArgumentException.class, () -> new MetricKey(definition, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new MetricKey(definition, Collections.singletonMap("wrong", "orders")));
    Map<String, String> labels = new HashMap<>();
    labels.put("table", "orders");
    labels.put("extra", "value");
    assertThrows(IllegalArgumentException.class, () -> new MetricKey(definition, labels));
  }
}
