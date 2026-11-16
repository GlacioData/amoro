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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.OptimizerRegisterInfo;
import org.apache.amoro.server.optimizing.OptimizingProcess;
import org.apache.amoro.server.resource.OptimizerInstance;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.table.TableRuntimeStore;

import java.lang.reflect.Field;
import java.util.Collections;

/**
 * Real runtimes with fixed quota time, so result comparisons do not depend on wall-clock timing.
 */
public final class QuotaTestSupport {
  private QuotaTestSupport() {}

  public static TableRuntimeStore store(String name, String group, double quota) {
    TableRuntimeStore store = mock(TableRuntimeStore.class);
    when(store.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "database", name, TableFormat.ICEBERG));
    when(store.getGroupName()).thenReturn(group);
    when(store.getTableConfig())
        .thenReturn(Collections.singletonMap("self-optimizing.quota", Double.toString(quota)));
    return store;
  }

  public static DefaultTableRuntime runtime(
      String name, String group, double quota, long occupied) {
    DefaultTableRuntime runtime = new DefaultTableRuntime(store(name, group, quota), () -> null);
    OptimizingProcess process = mock(OptimizingProcess.class);
    when(process.getRunningQuotaTime(anyLong(), anyLong())).thenReturn(occupied);
    try {
      Field field = DefaultTableRuntime.class.getDeclaredField("optimizingProcess");
      field.setAccessible(true);
      field.set(runtime, process);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
    return runtime;
  }

  public static OptimizerInstance optimizer(String group, int threads) {
    return new OptimizerInstance(
        new OptimizerRegisterInfo()
            .setGroupName(group)
            .setThreadCount(threads)
            .setMemoryMb(1024)
            .setStartTime(System.currentTimeMillis())
            .setResourceId("resource"),
        "local");
  }
}
