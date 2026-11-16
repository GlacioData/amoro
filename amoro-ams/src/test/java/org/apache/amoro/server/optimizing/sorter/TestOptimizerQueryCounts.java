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

import static org.apache.amoro.server.optimizing.sorter.QuotaTestSupport.optimizer;
import static org.apache.amoro.server.optimizing.sorter.QuotaTestSupport.runtime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.core.validation.Validator;
import io.javalin.http.Context;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.OptimizerRegisterInfo;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.resource.Resource;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.resource.ResourceType;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.AmoroServiceConstants;
import org.apache.amoro.server.DefaultOptimizingService;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.OverviewManager;
import org.apache.amoro.server.dashboard.controller.OptimizerController;
import org.apache.amoro.server.dashboard.controller.OptimizerGroupController;
import org.apache.amoro.server.manager.AbstractOptimizerContainer;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.DataSourceFactory;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.OptimizerMapper;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.resource.Containers;
import org.apache.amoro.server.resource.DefaultOptimizerManager;
import org.apache.amoro.server.resource.OptimizerInstance;
import org.apache.amoro.server.table.DefaultTableManager;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.server.table.TableOptimizingQuery;
import org.apache.amoro.server.table.TableService;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import javax.sql.DataSource;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercises the production persistence entry points and counts delegated JDBC executions. */
public class TestOptimizerQueryCounts extends PersistentBase {
  private final AtomicBoolean closeNextSelect = new AtomicBoolean();
  private final AtomicInteger selects = new AtomicInteger();
  private final AtomicInteger succeeded = new AtomicInteger();
  private final AtomicInteger failed = new AtomicInteger();
  private final AtomicInteger filteredSelects = new AtomicInteger();

  @Test
  void queryBudgetsInIsolatedJvm() throws Exception {
    // SqlSessionFactoryProvider is process-wide and DerbyPersistence may already have initialized
    // it.
    // A child JVM also keeps this test safe under the default reuseForks=true test suite.
    Path log = Files.createTempFile("optimizer-query-counts", ".log");
    List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("-javaagent:") && argument.contains("byte-buddy-agent")) {
        command.add(argument);
      }
    }
    command.addAll(
        Arrays.asList("-cp", System.getProperty("java.class.path"), getClass().getName()));
    Process process =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(120, TimeUnit.SECONDS), "isolated query test timed out: " + log);
      String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
      assertEquals(0, process.exitValue(), output);
      assertTrue(output.contains("QUERY_BUDGETS_PASS"), output);
      for (String line : output.split("\\R")) {
        if (line.startsWith("QUERY_BUDGETS")
            || line.startsWith("quota tables=")
            || line.startsWith("resource groups=")
            || line.startsWith("snapshot freshness")
            || line.startsWith("concurrent comparators=")
            || line.startsWith("manager lists")
            || line.startsWith("retained U1")) {
          System.out.println(line);
        }
      }
    } finally {
      process.destroyForcibly();
      Files.deleteIfExists(log);
    }
  }

  public static void main(String[] args) throws Exception {
    new TestOptimizerQueryCounts().runScenarios();
  }

  private void runScenarios() throws Exception {
    Configurations config = new Configurations();
    config.set(AmoroManagementConf.DB_CONNECTION_URL, "jdbc:derby:memory:query_counts;create=true");
    config.set(AmoroManagementConf.DB_TYPE, AmoroManagementConf.DB_TYPE_DERBY);
    config.set(AmoroManagementConf.DB_DRIVER_CLASS_NAME, "org.apache.derby.jdbc.EmbeddedDriver");
    DataSource source = DataSourceFactory.createDataSource(config);
    try {
      DataSource counted = countingDataSource(source);
      SqlSessionFactoryProvider.getInstance().init(counted);
      assertSame(
          counted,
          SqlSessionFactoryProvider.getInstance()
              .get()
              .getConfiguration()
              .getEnvironment()
              .getDataSource());
      getAs(OptimizerMapper.class, OptimizerMapper::selectAll);
      assertEquals(1, selects.get(), "counter calibration");
      assertEquals(1, succeeded.get());
      System.out.println("QUERY_BUDGETS_BEGIN");
      quotaRoundsAndLegacyComparison();
      freshSnapshotsAndFailureRecovery();
      parallelRounds();
      resourceGroupsAndRetainedManagerPaths(config);
      retainedServiceAndDisplayPaths(config);
      assertEquals(0, failed.get());
      System.out.println("QUERY_BUDGETS_PASS");
    } finally {
      ((AutoCloseable) source).close();
    }
  }

  private void quotaRoundsAndLegacyComparison() {
    clearOptimizers();
    insert(optimizer("g", 8));
    List<DefaultTableRuntime> input = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      input.add(
          runtime("table" + i, "g", 0.5, (i % 7) * AmoroServiceConstants.QUOTA_LOOK_BACK_TIME));
    }
    List<DefaultTableRuntime> legacy = new ArrayList<>(input);
    Map<DefaultTableRuntime, Double> weights = new HashMap<>();
    selects.set(0);
    // The original comparator, using the retained public no-arg production method.
    legacy.sort(
        Comparator.comparingDouble(
            r -> weights.computeIfAbsent(r, DefaultTableRuntime::calculateQuotaOccupy)));
    assertEquals(1000, selects.get());
    List<DefaultTableRuntime> optimized = new ArrayList<>(input);
    selects.set(0);
    optimized.sort(new QuotaOccupySorter().createComparator());
    assertEquals(1, selects.get());
    assertEquals(legacy, optimized, "ordering and equal-weight stability");
    System.out.println("quota tables=1000: legacy=1000 optimized=1; identical order");
    selects.set(0);
    Collections.<DefaultTableRuntime>emptyList().sort(new QuotaOccupySorter().createComparator());
    new ArrayList<>(Collections.singletonList(input.get(0)))
        .sort(new QuotaOccupySorter().createComparator());
    Arrays.asList(runtime("abs1", "g", 2, 1), runtime("abs2", "g", 3, 1))
        .sort(new QuotaOccupySorter().createComparator());
    assertEquals(0, selects.get());
    clearOptimizers();
    selects.set(0);
    new ArrayList<>(input).sort(new QuotaOccupySorter().createComparator());
    assertEquals(1, selects.get(), "empty snapshot is still cached");
  }

  private void freshSnapshotsAndFailureRecovery() throws Exception {
    clearOptimizers();
    OptimizerInstance initial = optimizer("g", 2);
    insert(initial);
    long occupied = AmoroServiceConstants.QUOTA_LOOK_BACK_TIME;
    DefaultTableRuntime proportional = runtime("proportional", "g", 0.5, occupied);
    DefaultTableRuntime absolute = runtime("absolute", "g", 2, occupied);
    Comparator<DefaultTableRuntime> first = new QuotaOccupySorter().createComparator();
    assertTrue(first.compare(proportional, absolute) > 0);
    doAs(OptimizerMapper.class, mapper -> mapper.deleteOptimizer(initial.getToken()));
    insert(optimizer("g", 8));
    // A different table in the same comparator sees the original denominator.
    assertTrue(first.compare(runtime("sameRound", "g", 0.5, occupied), absolute) > 0);
    assertTrue(new QuotaOccupySorter().createComparator().compare(proportional, absolute) < 0);
    // Real JDBC failure (missing relation), followed by restoration and a new sorting round.
    try (Connection connection =
        SqlSessionFactoryProvider.getInstance()
            .get()
            .getConfiguration()
            .getEnvironment()
            .getDataSource()
            .getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("RENAME TABLE optimizer TO optimizer_saved");
      }
      connection.commit();
      try {
        assertThrows(
            RuntimeException.class,
            () -> new QuotaOccupySorter().createComparator().compare(proportional, absolute));
        assertEquals(0, failed.get(), "missing table fails during prepare, before execution");
      } finally {
        try (Statement statement = connection.createStatement()) {
          statement.execute("RENAME TABLE optimizer_saved TO optimizer");
        }
        connection.commit();
      }
    }
    // Derby can reject during prepare before execute, which is deliberately not an execution count.
    closeNextSelect.set(true);
    assertThrows(
        RuntimeException.class,
        () -> new QuotaOccupySorter().createComparator().compare(proportional, absolute));
    assertEquals(1, failed.get(), "closed JDBC delegate fails during execute");
    failed.set(0);
    assertTrue(new QuotaOccupySorter().createComparator().compare(proportional, absolute) < 0);
    System.out.println(
        "snapshot freshness, zero-read branches, empty result and DB recovery passed");
  }

  private void parallelRounds() throws Exception {
    selects.set(0);
    int rounds = 16;
    ExecutorService executor = Executors.newFixedThreadPool(rounds);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < rounds; i++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  List<DefaultTableRuntime> tables =
                      Arrays.asList(runtime("p1", "g", 0.5, 1), runtime("p2", "g", 0.5, 2));
                  tables.sort(new QuotaOccupySorter().createComparator());
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
      assertEquals(rounds, selects.get());
      System.out.println("concurrent comparators=16: optimized reads=16");
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private void resourceGroupsAndRetainedManagerPaths(Configurations config) {
    CatalogManager catalogs = mock(CatalogManager.class);
    when(catalogs.listCatalogMetas()).thenReturn(Collections.emptyList());
    DefaultOptimizerManager manager = new DefaultOptimizerManager(config, catalogs);
    for (int i = 0; i < 100; i++) {
      manager.createResourceGroup(new ResourceGroup.Builder("g" + i, "local").build());
    }
    selects.set(0);
    for (ResourceGroup group : manager.listResourceGroups()) {
      manager.listOptimizers(group.getName());
      manager.getResourceGroup(group.getName());
    }
    assertEquals(100, selects.get());
    selects.set(0);
    new OptimizerGroupController(mock(TableManager.class), manager)
        .getResourceGroup(mock(Context.class));
    assertEquals(1, selects.get());
    System.out.println("resource groups=100: legacy=100 optimized=1");
    selects.set(0);
    assertEquals(1, manager.listOptimizers().size());
    assertEquals(1, manager.listOptimizers("g").size());
    assertEquals(2, selects.get());
    assertTrue(manager.canDeleteResourceGroup("g0"));
    assertEquals(3, selects.get());
    manager.deleteResourceGroup("g0");
    assertEquals(4, selects.get(), "actual delete rechecks independently");
    manager.deleteOptimizerAndResource("g", "resource");
    assertEquals(1, filteredSelects.get());
    assertEquals(4, selects.get(), "resourceId query is a separate SQL fingerprint");
    assertEquals(0, manager.listOptimizers().size());
    System.out.println("manager lists, delete guards and transactional resourceId query passed");
  }

  @SuppressWarnings("unchecked")
  private void retainedServiceAndDisplayPaths(Configurations config) throws Exception {
    CatalogManager catalogs = mock(CatalogManager.class);
    when(catalogs.listCatalogMetas()).thenReturn(Collections.emptyList());
    DefaultOptimizerManager manager = new DefaultOptimizerManager(config, catalogs);
    ResourceGroup group = new ResourceGroup.Builder("g", "local").build();
    manager.createResourceGroup(group);
    OptimizerInstance initial = optimizer("g", 8);
    insert(initial);
    DefaultOptimizingService service =
        new DefaultOptimizingService(
            config,
            catalogs,
            manager,
            mock(TableService.class),
            null,
            null,
            Collections.emptyList());
    try {
      selects.set(0);
      call(service, "loadOptimizingQueues", new Class<?>[] {List.class}, Collections.emptyList());
      assertEquals(1, selects.get(), "U1 startup");
      Object keeper = field(service, "optimizerKeeper");
      Map<String, OptimizerInstance> registered =
          (Map<String, OptimizerInstance>) field(service, "authOptimizers");
      OptimizerInstance loaded = registered.get(initial.getToken());
      assertTrue(loaded != null, "startup restores persistent tokens");
      selects.set(0);
      call(keeper, "loadOptimizersFromDatabase", new Class<?>[0]);
      assertEquals(1, selects.get(), "U2 follower sync");
      assertSame(loaded, registered.get(initial.getToken()), "existing token identity preserved");
      OptimizerInstance added = optimizer("g", 2);
      insert(added);
      doAs(OptimizerMapper.class, m -> m.deleteOptimizer(initial.getToken()));
      selects.set(0);
      call(keeper, "loadOptimizersFromDatabase", new Class<?>[0]);
      assertEquals(1, selects.get());
      assertTrue(registered.containsKey(added.getToken()));
      assertTrue(!registered.containsKey(initial.getToken()));
      Class<?> taskClass =
          Arrays.stream(DefaultOptimizingService.class.getDeclaredClasses())
              .filter(c -> c.getSimpleName().equals("OptimizerGroupKeepingTask"))
              .findFirst()
              .get();
      Constructor<?> constructor =
          taskClass.getDeclaredConstructor(DefaultOptimizingService.class, String.class, int.class);
      constructor.setAccessible(true);
      Object task = constructor.newInstance(service, "g", 1);
      selects.set(0);
      call(task, "tryKeeping", new Class<?>[] {ResourceGroup.class}, group);
      assertEquals(1, selects.get(), "U3 group keeper check");
    } finally {
      service.dispose();
    }
    OverviewManager overview = new OverviewManager(10, Duration.ZERO);
    selects.set(0);
    call(overview, "refreshResourceUsage", new Class<?>[] {long.class}, System.currentTimeMillis());
    assertEquals(1, selects.get(), "U4 overview refresh");
    assertEquals(2, overview.getTotalCpu());
    assertEquals(1024L * 1024 * 1024, overview.getTotalMemory());
    DefaultTableManager tables = new DefaultTableManager(config, catalogs);
    for (int i = 0; i < 3; i++) {
      ServerTableIdentifier id =
          ServerTableIdentifier.of("catalog", "database", "query" + i, TableFormat.ICEBERG);
      doAs(TableMetaMapper.class, m -> m.insertTable(id));
      TableRuntimeMeta meta = new TableRuntimeMeta();
      meta.setTableId(id.getId());
      meta.setGroupName("g");
      meta.setStatusCode(OptimizingStatus.IDLE.getCode());
      meta.setTableConfig(Collections.singletonMap("self-optimizing.group", "g"));
      doAs(TableRuntimeMapper.class, m -> m.insertRuntime(meta));
    }
    selects.set(0);
    assertEquals(
        3,
        tables
            .queryTableOptimizingInfo(TableOptimizingQuery.builder(10, 0).build())
            .getLeft()
            .size());
    assertEquals(1, selects.get(), "U6 table list remains batched");
    OptimizerGroupController controller = new OptimizerGroupController(tables, manager);
    for (String selectedGroup : Arrays.asList("all", "g")) {
      Context ctx = mock(Context.class);
      when(ctx.pathParam("optimizerGroup")).thenReturn(selectedGroup);
      when(ctx.queryParamAsClass("page", Integer.class))
          .thenReturn(new Validator<>("1", Integer.class, "page"));
      when(ctx.queryParamAsClass("pageSize", Integer.class))
          .thenReturn(new Validator<>("20", Integer.class, "pageSize"));
      selects.set(0);
      controller.getOptimizers(ctx);
      assertEquals(1, selects.get(), "U7 optimizer list");
      controller.getOptimizerGroupInfo(ctx);
      assertEquals(2, selects.get(), "U8 group info");
    }
    Resource resource = new Resource.Builder("local", "g", ResourceType.OPTIMIZER).build();
    manager.createResource(resource);
    insert(
        new OptimizerInstance(
            new OptimizerRegisterInfo()
                .setGroupName("g")
                .setThreadCount(1)
                .setStartTime(System.currentTimeMillis())
                .setResourceId(resource.getResourceId())
                .setProperties(Collections.emptyMap()),
            "local"));
    Context release = mock(Context.class);
    when(release.pathParam("jobId")).thenReturn(resource.getResourceId());
    AbstractOptimizerContainer container = mock(AbstractOptimizerContainer.class);
    selects.set(0);
    filteredSelects.set(0);
    try (MockedStatic<Containers> containers = mockStatic(Containers.class)) {
      containers.when(() -> Containers.get("local")).thenReturn(container);
      new OptimizerController(manager).releaseOptimizer(release);
      verify(container).releaseResource(org.mockito.ArgumentMatchers.any(Resource.class));
    }
    assertEquals(1, selects.get(), "U10 release guard");
    assertEquals(1, filteredSelects.get(), "release resource transaction");
    System.out.println("retained U1/U2/U3/U4/U6/U7/U8/U10 executed with expected JDBC budgets");
  }

  private static Object field(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static Object call(Object target, String name, Class<?>[] types, Object... args)
      throws Exception {
    Method method = target.getClass().getDeclaredMethod(name, types);
    method.setAccessible(true);
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw new AssertionError(e.getCause());
    }
  }

  private void insert(OptimizerInstance optimizer) {
    doAs(OptimizerMapper.class, mapper -> mapper.insertOptimizer(optimizer));
  }

  private void clearOptimizers() {
    for (OptimizerInstance instance : getAs(OptimizerMapper.class, OptimizerMapper::selectAll)) {
      doAs(OptimizerMapper.class, mapper -> mapper.deleteOptimizer(instance.getToken()));
    }
  }

  private DataSource countingDataSource(DataSource delegate) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              Object result = invoke(delegate, method, args);
              return result instanceof Connection
                  ? countingConnection((Connection) result)
                  : result;
            });
  }

  private Connection countingConnection(Connection delegate) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              Object result = invoke(delegate, method, args);
              if (method.getName().equals("prepareStatement")) {
                String sql =
                    ((String) args[0]).replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);
                return countingStatement((PreparedStatement) result, sql);
              }
              return result;
            });
  }

  private PreparedStatement countingStatement(PreparedStatement delegate, String sql) {
    boolean all = sql.startsWith("select ") && sql.endsWith(" from optimizer");
    boolean filtered =
        sql.startsWith("select ") && sql.contains(" from optimizer where resource_id");
    return (PreparedStatement)
        Proxy.newProxyInstance(
            PreparedStatement.class.getClassLoader(),
            new Class<?>[] {PreparedStatement.class},
            (proxy, method, args) -> {
              boolean execute =
                  method.getName().equals("execute") || method.getName().equals("executeQuery");
              if (execute && all) {
                selects.incrementAndGet();
              }
              if (execute && filtered) {
                filteredSelects.incrementAndGet();
              }
              try {
                if (execute && all && closeNextSelect.compareAndSet(true, false)) {
                  delegate.close();
                }
                Object result = invoke(delegate, method, args);
                if (execute && all) {
                  succeeded.incrementAndGet();
                }
                return result;
              } catch (SQLException e) {
                if (execute && all) {
                  failed.incrementAndGet();
                }
                throw e;
              }
            });
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
