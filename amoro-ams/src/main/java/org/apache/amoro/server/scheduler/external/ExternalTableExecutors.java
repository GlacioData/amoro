package org.apache.amoro.server.scheduler.external;

import static org.apache.amoro.IcebergActions.DELETE_ORPHANS;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.resource.ResourceManager;
import org.apache.amoro.server.manager.StandaloneTableMaintainerExternalResource;
import org.apache.amoro.server.table.TableService;

/**
 * ExternalTableExecutors Submit Jobs to Clusters
 */
public class ExternalTableExecutors {

  private static final ExternalTableExecutors instance = new ExternalTableExecutors();
  private JavaProcessExecutor javaProcessExecutor;

  public static ExternalTableExecutors getInstance() {
    return instance;
  }

  public void setup(ResourceManager resourceManager,TableService tableService, Configurations conf) {
    StandaloneTableMaintainerExternalResource standaloneTableMaintainerExternalResource =
        new StandaloneTableMaintainerExternalResource();
    this.javaProcessExecutor = new JavaProcessExecutor(
          resourceManager,standaloneTableMaintainerExternalResource,DELETE_ORPHANS,tableService,2);
  }

  public JavaProcessExecutor getJavaProcessExecutor() {
    return javaProcessExecutor;
  }
}
