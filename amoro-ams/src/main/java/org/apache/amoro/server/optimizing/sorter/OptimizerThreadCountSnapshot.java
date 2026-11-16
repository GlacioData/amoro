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

package org.apache.amoro.server.optimizing.sorter;

import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.OptimizerMapper;
import org.apache.amoro.server.resource.OptimizerInstance;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** A lazy resource snapshot owned by one serial quota comparator, never shared across rounds. */
final class OptimizerThreadCountSnapshot extends PersistentBase {
  private final Supplier<List<OptimizerInstance>> loader;
  private Map<String, Integer> threadCounts;

  OptimizerThreadCountSnapshot() {
    this.loader = () -> getAs(OptimizerMapper.class, OptimizerMapper::selectAll);
  }

  OptimizerThreadCountSnapshot(Supplier<List<OptimizerInstance>> loader) {
    this.loader = loader;
  }

  int getThreadCount(String group) {
    if (threadCounts == null) {
      // HashMap permits the null group supported by the original Objects.equals filter.
      // https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html
      Map<String, Integer> counts = new HashMap<>();
      List<OptimizerInstance> instances = loader.get();
      if (instances != null) {
        for (OptimizerInstance instance : instances) {
          counts.merge(instance.getGroupName(), instance.getThreadCount(), Integer::sum);
        }
      }
      // Publish only after successful loading/aggregation; an empty map is a loaded snapshot.
      threadCounts = Collections.unmodifiableMap(counts);
    }
    return Math.max(threadCounts.getOrDefault(group, 0), 1);
  }
}
