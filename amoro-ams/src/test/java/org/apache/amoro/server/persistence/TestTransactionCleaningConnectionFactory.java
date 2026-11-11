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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.PooledObject;
import org.junit.jupiter.api.Test;

class TestTransactionCleaningConnectionFactory {
  @Test
  void abandonedConnectionsRetainTheAbortDestructionMode() throws Exception {
    PoolableConnectionFactory delegate = mock(PoolableConnectionFactory.class);
    @SuppressWarnings("unchecked")
    PooledObject<PoolableConnection> pooled = mock(PooledObject.class);
    TransactionCleaningConnectionFactory factory =
        new TransactionCleaningConnectionFactory(delegate);
    factory.destroyObject(pooled, DestroyMode.ABANDONED);
    verify(delegate).destroyObject(pooled, DestroyMode.ABANDONED);
  }

  @Test
  void readOnlyHintDoesNotCauseRollbackForAutoCommitConnections() throws Exception {
    PoolableConnectionFactory delegate = mock(PoolableConnectionFactory.class);
    PoolableConnection connection = mock(PoolableConnection.class);
    @SuppressWarnings("unchecked")
    PooledObject<PoolableConnection> pooled = mock(PooledObject.class);
    when(pooled.getObject()).thenReturn(connection);
    when(delegate.isRollbackOnReturn()).thenReturn(true);
    when(connection.isReadOnly()).thenReturn(true);
    when(connection.getAutoCommit()).thenReturn(true);
    new TransactionCleaningConnectionFactory(delegate).passivateObject(pooled);
    verify(connection, never()).rollback();
    verify(connection, never()).isReadOnly();
    verify(delegate).passivateObject(pooled);
  }
}
