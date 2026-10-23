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

package org.apache.amoro.formats.paimon.optimizing.primary;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.catalog.TableRollback;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.PartitionMarkDone;
import org.apache.paimon.table.PartitionModification;
import org.apache.paimon.table.SchemaModification;
import org.apache.paimon.table.source.TableQueryAuth;
import org.apache.paimon.utils.SnapshotLoader;
import org.apache.paimon.utils.SnapshotManager;

import java.util.function.LongConsumer;

/** Keeps catalog behavior while guarding each native atomic publication attempt. */
final class PaimonPrimaryKeyCommitEnvironment extends CatalogEnvironment {
  private static final long serialVersionUID = 1L;
  private final CatalogEnvironment delegate;
  private final PaimonPrimaryKeyCommitGuard guard;

  PaimonPrimaryKeyCommitEnvironment(
      CatalogEnvironment delegate, PaimonPrimaryKeyCommitGuard guard) {
    super(null, null, null, null, null, null, false, false);
    this.delegate = delegate;
    this.guard = guard;
  }

  @Override
  public SnapshotCommit snapshotCommit(SnapshotManager manager) {
    SnapshotCommit commit = delegate.snapshotCommit(manager);
    // Match AbstractFileStore.newCommit's fallback, preserving configured catalog locks otherwise.
    return guard.wrap(
        commit == null ? new RenamingSnapshotCommit(manager, Lock.empty()) : commit, manager);
  }

  @Override
  public CatalogEnvironment copy(Identifier identifier) {
    return new PaimonPrimaryKeyCommitEnvironment(delegate.copy(identifier), guard);
  }

  @Override
  public Identifier identifier() {
    return delegate.identifier();
  }

  @Override
  public String uuid() {
    return delegate.uuid();
  }

  @Override
  public PartitionModification partitionModification() {
    return delegate.partitionModification();
  }

  @Override
  public PartitionMarkDone partitionMarkDone() {
    return delegate.partitionMarkDone();
  }

  @Override
  public boolean supportsVersionManagement() {
    return delegate.supportsVersionManagement();
  }

  @Override
  public SchemaModification schemaModification() {
    return delegate.schemaModification();
  }

  @Override
  public TableRollback catalogTableRollback() {
    return delegate.catalogTableRollback();
  }

  @Override
  public LongConsumer catalogSchemaRollback() {
    return delegate.catalogSchemaRollback();
  }

  @Override
  public SnapshotLoader snapshotLoader() {
    return delegate.snapshotLoader();
  }

  @Override
  public CatalogLockFactory lockFactory() {
    return delegate.lockFactory();
  }

  @Override
  public CatalogLockContext lockContext() {
    return delegate.lockContext();
  }

  @Override
  public CatalogLoader catalogLoader() {
    return delegate.catalogLoader();
  }

  @Override
  public CatalogContext catalogContext() {
    return delegate.catalogContext();
  }

  @Override
  public TableQueryAuth tableQueryAuth(CoreOptions options) {
    return delegate.tableQueryAuth(options);
  }
}
