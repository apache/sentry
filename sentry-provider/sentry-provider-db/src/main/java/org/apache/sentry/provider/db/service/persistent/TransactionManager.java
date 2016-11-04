/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

/**
 * TransactionManager is used for executing the database transaction, it supports
 * the transaction with retry mechanism for the unexpected exceptions, except SentryUserExceptions,
 * eg, SentryNoSuchObjectException, SentryAlreadyExistsException etc.
 */
public class TransactionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionManager.class);

  final private PersistenceManagerFactory pmf;

  // Maximum number of retries per call
  final private int transactionRetryMax;

  // Delay (in milliseconds) between retries
  final private int retryWaitTimeMills;

  public TransactionManager(PersistenceManagerFactory pmf, Configuration conf) {
    this.pmf = pmf;
    this.transactionRetryMax = conf.getInt(
        ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY,
        ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_DEFAULT);
    this.retryWaitTimeMills = conf.getInt(
        ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_WAIT_TIME_MILLIS,
        ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_WAIT_TIME_MILLIS_DEFAULT);
  }


  /**
   * Execute some code as a single transaction, the code in tb.execute() should not start new
   * transaction or manipulate transactions with the PersistenceManager.
   * @param tb transaction block with code to execute
   * @return Object with the result of tb.execute()
   * @throws Exception
   */
  public Object executeTransaction(TransactionBlock tb) throws Exception {
    try (CloseablePersistenceManager cpm =
        new CloseablePersistenceManager(pmf.getPersistenceManager())) {
      Transaction transaction = cpm.pm.currentTransaction();
      transaction.begin();
      try {
        Object result = tb.execute(cpm.pm);
        transaction.commit();
        return result;
      } finally {
        if (transaction.isActive()) {
          transaction.rollback();
        }
      }
    }
  }

  /**
   * Execute some code as a single transaction with retry mechanism
   * @param tb transaction block with code to execute
   * @return Object with the result of tb.execute()
   * @throws Exception
   */
  public Object executeTransactionWithRetry(TransactionBlock tb) throws Exception {
    int retryNum = 0;
    while (retryNum < transactionRetryMax) {
      try {
        return executeTransaction(tb);
      } catch (Exception e) {
        // throw the sentry exception without retry
        if (e instanceof SentryUserException) {
          throw e;
        }
        retryNum++;
        if (retryNum >= transactionRetryMax) {
          String message = "The transaction has reached max retry number, will not retry again.";
          LOGGER.error(message, e);
          throw new Exception(message, e);
        }
        LOGGER.warn("Exception is thrown, retry the transaction, current retry num is:"
            + retryNum + ", the max retry num is: " + transactionRetryMax, e);
        try {
          Thread.sleep(retryWaitTimeMills);
        } catch (InterruptedException ex) {
          throw ex;
        }
      }
    }
    return null;
  }

  /**
   * CloseablePersistenceManager is a wrapper around PersistenceManager that
   * implements AutoCloseable interface. It is needed because Apache jdo doesn't
   * implement AutoCloseable (Datanucleus version does).
   */
  private class CloseablePersistenceManager implements AutoCloseable {
    private final PersistenceManager pm;

    public CloseablePersistenceManager(PersistenceManager pm) {
      this.pm = pm;
    }

    @Override
    public void close() throws Exception {
      pm.close();
    }
  }
}