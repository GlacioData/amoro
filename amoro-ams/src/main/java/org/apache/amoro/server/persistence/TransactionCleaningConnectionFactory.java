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

package org.apache.amoro.server.persistence;

import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;

/** Adds read-only transaction cleanup to DBCP's configured connection lifecycle. */
final class TransactionCleaningConnectionFactory extends PoolableConnectionFactory {
  private final PoolableConnectionFactory delegate;

  TransactionCleaningConnectionFactory(PoolableConnectionFactory delegate) {
    super(delegate.getConnectionFactory(), delegate.getDataSourceJmxObjectName());
    this.delegate = delegate;
  }

  @Override
  public PooledObject<PoolableConnection> makeObject() throws Exception {
    return delegate.makeObject();
  }

  @Override
  public void activateObject(PooledObject<PoolableConnection> pooled) throws Exception {
    delegate.activateObject(pooled);
  }

  @Override
  public boolean validateObject(PooledObject<PoolableConnection> pooled) {
    return delegate.validateObject(pooled);
  }

  @Override
  public void passivateObject(PooledObject<PoolableConnection> pooled) throws Exception {
    PoolableConnection connection = pooled.getObject();
    // DBCP skips read-only connections, but even a SELECT can leave a transaction open.
    if (delegate.isRollbackOnReturn() && !connection.getAutoCommit() && connection.isReadOnly()) {
      connection.rollback();
    }
    delegate.passivateObject(pooled);
  }

  @Override
  public void destroyObject(PooledObject<PoolableConnection> pooled) throws Exception {
    delegate.destroyObject(pooled);
  }

  @Override
  public void destroyObject(PooledObject<PoolableConnection> pooled, DestroyMode mode)
      throws Exception {
    delegate.destroyObject(pooled, mode);
  }

  @Override
  public ObjectPool<PoolableConnection> getPool() {
    return delegate.getPool();
  }

  @Override
  public void setPool(ObjectPool<PoolableConnection> pool) {
    delegate.setPool(pool);
  }
}
