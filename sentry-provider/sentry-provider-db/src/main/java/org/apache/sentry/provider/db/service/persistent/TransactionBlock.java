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

package org.apache.sentry.provider.db.service.persistent;

import javax.jdo.PersistenceManager;

/**
 * TransactionBlock wraps the code that is executed inside a single
 * transaction. The {@link #execute(PersistenceManager)} method returns the
 * result of the transaction.
 */
@FunctionalInterface
public interface TransactionBlock<T> {
  /**
   * Execute some code as a single transaction, the code should not start new
   * transaction or manipulate transactions with the PersistenceManager.
   *
   * @param pm PersistenceManager for the current transaction
   * @return Object with the result of execute()
   * @throws Exception
   */
  T execute(PersistenceManager pm) throws Exception;
}