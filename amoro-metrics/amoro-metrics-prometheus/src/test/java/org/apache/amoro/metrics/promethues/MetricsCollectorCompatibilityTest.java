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

package org.apache.amoro.metrics.promethues;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.prometheus.client.Collector;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.amoro.metrics.Counter;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.Metric;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.metrics.MetricSet;
import org.apache.amoro.metrics.MetricType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class MetricsCollectorCompatibilityTest {

  @ParameterizedTest
  @ValueSource(strings = {TextFormat.CONTENT_TYPE_004, TextFormat.CONTENT_TYPE_OPENMETRICS_100})
  void expositionKeepsNamesLabelsHelpTypesAndEscaping(String format) throws Exception {
    Map<MetricKey, Metric> metrics = new LinkedHashMap<>();
    MetricDefine first =
        MetricDefine.defineGauge("table_records")
            .withTags("table", "catalog")
            .withDescription("records\\help\nnext")
            .build();
    MetricDefine reordered =
        MetricDefine.defineGauge("table_records")
            .withTags("catalog", "table")
            .withDescription("second description")
            .build();
    Map<String, String> tags = new HashMap<>();
    tags.put("table", "订单\"\\\n");
    tags.put("catalog", "lake");
    metrics.put(new MetricKey(first, tags), (Gauge<Double>) () -> 12.5);
    tags = new HashMap<>();
    tags.put("table", "returns");
    tags.put("catalog", "other");
    metrics.put(new MetricKey(reordered, tags), (Gauge<Double>) () -> -2.5);
    Counter counter = new Counter();
    counter.inc(7);
    metrics.put(
        new MetricKey(
            MetricDefine.defineCounter("completed_count").withDescription("completed").build(),
            null),
        counter);
    metrics.put(
        new MetricKey(
            MetricDefine.defineGauge("non_finite").withDescription("non-finite values").build(),
            null),
        (Gauge<Double>) () -> Double.NaN);

    List<Collector.MetricFamilySamples> actual = new MetricsCollector(() -> metrics).collect();
    assertEquals(exposition(format, legacyCollect(metrics)), exposition(format, actual));
    Collector.MetricFamilySamples records =
        actual.stream().filter(f -> f.name.equals("amoro_table_records")).findFirst().get();
    assertEquals(2, records.samples.size());
    assertEquals(first.getDescription(), records.help);
    assertEquals(first.getTags(), records.samples.get(0).labelNames);
  }

  @Test
  void gaugeAndCounterAreReadAgainWhileFixedLabelsAreReused() {
    MetricRegistry registry = new MetricRegistry();
    AtomicLong value = new AtomicLong(10);
    AtomicInteger reads = new AtomicInteger();
    MetricKey key =
        registry.register(
            MetricDefine.defineGauge("records").withTags("table").build(),
            Collections.singletonMap("table", "orders"),
            (Gauge<Long>)
                () -> {
                  reads.incrementAndGet();
                  return value.get();
                });
    Counter counter = new Counter();
    registry.register(MetricDefine.defineCounter("completed").build(), null, counter);
    MetricsCollector collector = new MetricsCollector(registry);
    Collector.MetricFamilySamples.Sample first = sample(collector.collect(), "amoro_records");
    value.set(20);
    counter.inc(3);
    List<Collector.MetricFamilySamples> next = collector.collect();
    Collector.MetricFamilySamples.Sample second = sample(next, "amoro_records");

    assertEquals(10D, first.value);
    assertEquals(20D, second.value);
    assertEquals(3D, sample(next, "amoro_completed").value);
    assertEquals(2, reads.get());
    assertNotSame(first, second);
    assertSame(key.valueOfTags(), first.labelValues);
    assertSame(first.labelValues, second.labelValues);
  }

  @Test
  void rejectedDefinitionsAreCheckedOnceAndNeverRead() {
    Map<MetricKey, Metric> metrics = new HashMap<>();
    MetricDefine accepted = MetricDefine.defineGauge("accepted").withTags("table").build();
    MetricDefine rejected = MetricDefine.defineGauge("rejected").withTags("table").build();
    Gauge<Long> unreadable =
        () -> {
          throw new AssertionError("Filtered Gauge must not be read");
        };
    for (int i = 0; i < 10; i++) {
      Map<String, String> tags = Collections.singletonMap("table", "table_" + i);
      metrics.put(new MetricKey(accepted, tags), (Gauge<Long>) () -> 1L);
      metrics.put(new MetricKey(rejected, tags), unreadable);
    }
    metrics.put(new MetricKey(MetricDefine.defineGauge("invalid-name").build(), null), unreadable);
    metrics.put(
        new MetricKey(
            MetricDefine.defineGauge("invalid_label").withTags("invalid-label").build(),
            Collections.singletonMap("invalid-label", "value")),
        unreadable);
    AtomicInteger checks = new AtomicInteger();
    MetricFilter filter =
        new MetricFilter(null, null) {
          @Override
          public boolean matches(String name) {
            checks.incrementAndGet();
            return name.equals("accepted");
          }
        };
    MetricsCollector collector = new MetricsCollector(() -> metrics, filter);

    assertEquals(10, collector.collect().get(0).samples.size());
    assertEquals(2, checks.get());
    assertEquals(10, collector.collect().get(0).samples.size());
    assertEquals(4, checks.get());
  }

  @Test
  void registrationUnregisterAndReregistrationAreVisibleOnNextScrape() {
    MetricRegistry registry = new MetricRegistry();
    MetricsCollector collector = new MetricsCollector(registry);
    assertTrue(collector.collect().isEmpty());
    MetricKey key =
        registry.register(
            MetricDefine.defineGauge("records").withDescription("old help").build(),
            null,
            (Gauge<Long>) () -> 1L);
    assertEquals(1D, sample(collector.collect(), "amoro_records").value);
    registry.unregister(key);
    assertTrue(collector.collect().isEmpty());
    Counter replacement = new Counter();
    replacement.inc(2);
    registry.register(
        MetricDefine.defineCounter("records").withDescription("new help").build(),
        null,
        replacement);
    List<Collector.MetricFamilySamples> result = collector.collect();
    assertEquals(2D, sample(result, "amoro_records").value);
    assertEquals("new help", result.get(0).help);
    assertEquals(Collector.Type.COUNTER, result.get(0).type);
  }

  @Test
  void arbitraryMetricSetCanReplaceItsMapBetweenScrapes() {
    AtomicReference<Map<MetricKey, Metric>> current = new AtomicReference<>(Collections.emptyMap());
    MetricSet metricSet = current::get;
    MetricsCollector collector = new MetricsCollector(metricSet);
    assertTrue(collector.collect().isEmpty());
    MetricKey key = new MetricKey(MetricDefine.defineGauge("records").build(), null);
    current.set(Collections.singletonMap(key, (Gauge<Long>) () -> 3L));
    assertEquals(3D, sample(collector.collect(), "amoro_records").value);
    current.set(Collections.emptyMap());
    assertTrue(collector.collect().isEmpty());
  }

  @Test
  void unregisterDuringCollectionDoesNotLookUpARemovedMetric() {
    Map<MetricKey, Metric> metrics = new ConcurrentHashMap<>();
    MetricDefine definition = MetricDefine.defineGauge("records").withTags("table").build();
    metrics.put(
        new MetricKey(definition, Collections.singletonMap("table", "one")),
        (Gauge<Long>) () -> 1L);
    metrics.put(
        new MetricKey(definition, Collections.singletonMap("table", "two")),
        (Gauge<Long>) () -> 2L);
    List<MetricKey> order = new ArrayList<>(metrics.keySet());
    metrics.put(
        order.get(0),
        (Gauge<Long>)
            () -> {
              metrics.remove(order.get(1));
              return 1L;
            });
    MetricsCollector collector = new MetricsCollector(() -> metrics);

    assertDoesNotThrow(
        () -> {
          collector.collect();
        });
    assertEquals(1, metrics.size());
    assertEquals(1, collector.collect().get(0).samples.size());
  }

  @Test
  void concurrentScrapesKeepTheirSamplesAndValuesSeparate() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    AtomicInteger value = new AtomicInteger();
    registry.register(
        MetricDefine.defineGauge("records").withTags("table").build(),
        Collections.singletonMap("table", "orders"),
        (Gauge<Integer>) value::incrementAndGet);
    MetricsCollector collector = new MetricsCollector(registry);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<List<Collector.MetricFamilySamples>> first =
          executor.submit(
              () -> {
                start.await();
                return collector.collect();
              });
      Future<List<Collector.MetricFamilySamples>> second =
          executor.submit(
              () -> {
                start.await();
                return collector.collect();
              });
      start.countDown();
      Collector.MetricFamilySamples.Sample a =
          sample(first.get(5, TimeUnit.SECONDS), "amoro_records");
      Collector.MetricFamilySamples.Sample b =
          sample(second.get(5, TimeUnit.SECONDS), "amoro_records");
      assertEquals(
          new HashSet<>(Arrays.asList(1D, 2D)), new HashSet<>(Arrays.asList(a.value, b.value)));
      assertNotSame(a, b);
      assertSame(a.labelValues, b.labelValues);
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  private static Collector.MetricFamilySamples.Sample sample(
      List<Collector.MetricFamilySamples> families, String name) {
    return families.stream().filter(f -> f.name.equals(name)).findFirst().get().samples.get(0);
  }

  private static List<String> exposition(
      String format, List<Collector.MetricFamilySamples> families) throws Exception {
    StringWriter writer = new StringWriter();
    TextFormat.writeFormat(format, writer, Collections.enumeration(families));
    return Arrays.stream(writer.toString().split("\n")).sorted().collect(Collectors.toList());
  }

  /** The pre-optimization collector, retained as an exposition compatibility oracle. */
  private static List<Collector.MetricFamilySamples> legacyCollect(Map<MetricKey, Metric> metrics) {
    Map<MetricDefine, List<MetricKey>> grouped =
        metrics.keySet().stream().collect(Collectors.groupingBy(MetricKey::getDefine));
    List<Collector.MetricFamilySamples> result = new ArrayList<>();
    grouped.forEach(
        (define, keys) -> {
          List<Collector.MetricFamilySamples.Sample> samples = new ArrayList<>();
          for (MetricKey key : keys) {
            Metric metric = metrics.get(key);
            double value =
                metric instanceof Counter
                    ? ((Counter) metric).getCount()
                    : ((Gauge<?>) metric).getValue().doubleValue();
            samples.add(
                new Collector.MetricFamilySamples.Sample(
                    "amoro_" + define.getName(), define.getTags(), key.valueOfTags(), value));
          }
          result.add(
              new Collector.MetricFamilySamples(
                  "amoro_" + define.getName(),
                  define.getType() == MetricType.Counter
                      ? Collector.Type.COUNTER
                      : Collector.Type.GAUGE,
                  define.getDescription(),
                  samples));
        });
    return result;
  }
}
