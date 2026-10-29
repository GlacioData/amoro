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

import io.prometheus.client.Collector;
import org.apache.amoro.metrics.Counter;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.Metric;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricSet;
import org.apache.amoro.metrics.MetricType;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Metric type converter for prometheus api */
public class MetricsCollector extends Collector {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);
  private static final String PREFIX = "amoro_";
  private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
  private static final Pattern LABEL_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
  MetricSet metrics;
  private final MetricFilter metricFilter;

  public MetricsCollector(MetricSet metrics) {
    this(metrics, MetricFilter.ACCEPT_ALL);
  }

  public MetricsCollector(MetricSet metrics, MetricFilter metricFilter) {
    this.metrics = metrics;
    this.metricFilter = metricFilter != null ? metricFilter : MetricFilter.ACCEPT_ALL;
  }

  @Override
  public List<MetricFamilySamples> collect() {
    Map<MetricDefine, MetricFamilyBuilder> families = new HashMap<>();
    metrics
        .getMetrics()
        .forEach(
            (key, metric) -> {
              MetricDefine define = key.getDefine();
              MetricFamilyBuilder family = families.get(define);
              // Null marks a rejected definition, so validation and filtering run once per scrape.
              if (family == null && !families.containsKey(define)) {
                family = isValidMetric(define) ? new MetricFamilyBuilder(define) : null;
                families.put(define, family);
              }
              if (family != null) {
                family.addSample(key, metric);
              }
            });
    return families.values().stream()
        .filter(Objects::nonNull)
        .map(MetricFamilyBuilder::build)
        .collect(Collectors.toList());
  }

  private boolean isValidMetric(MetricDefine define) {
    boolean nameIsValid = NAME_PATTERN.matcher(define.getName()).matches();
    boolean labelIsValid = true;
    for (String tag : define.getTags()) {
      if (!LABEL_PATTERN.matcher(tag).matches()) {
        labelIsValid = false;
        break;
      }
    }
    boolean valid = nameIsValid && labelIsValid;
    if (!valid) {
      LOGGER.warn("Metric {} is not a valid prometheus metric.", define);
      return false;
    }

    if (!metricFilter.matches(define.getName())) {
      return false;
    }

    return true;
  }

  private static final class MetricFamilyBuilder {
    private final MetricDefine define;
    private final String name;
    private final List<MetricFamilySamples.Sample> samples = Lists.newArrayList();

    private MetricFamilyBuilder(MetricDefine define) {
      this.define = define;
      this.name = PREFIX + define.getName();
    }

    private void addSample(MetricKey key, Metric metric) {
      samples.add(
          new MetricFamilySamples.Sample(
              name, define.getTags(), key.valueOfTags(), covertValue(metric)));
    }

    private MetricFamilySamples build() {
      return new MetricFamilySamples(
          name, covertType(define.getType()), define.getDescription(), samples);
    }
  }

  private static Type covertType(MetricType metricType) {
    switch (metricType) {
      case Counter:
        return Type.COUNTER;
      case Gauge:
        return Type.GAUGE;
      default:
        throw new IllegalStateException("unknown type:" + metricType);
    }
  }

  private static double covertValue(Metric metric) {
    if (metric instanceof Counter) {
      return ((Counter) metric).getCount();
    } else if (metric instanceof Gauge) {
      return ((Gauge<?>) metric).getValue().doubleValue();
    } else {
      throw new IllegalStateException(
          "unknown metric implement class:" + metric.getClass().getName());
    }
  }
}
