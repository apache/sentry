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

import com.codahale.metrics.Counter;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.apache.sentry.provider.db.service.thrift.SentryMetrics;


/**
 * TransactionManager is used for executing the database transaction, it supports
 * the transaction with retry mechanism for the unexpected exceptions,
 * except <em>SentryUserExceptions</em>, eg, <em>SentryNoSuchObjectException</em>,
 * <em>SentryAlreadyExistsException</em> etc. <p>
 *
 * The purpose of the class is to separate all transaction housekeeping (opening
 * transaction, rolling back failed transactions) from the actual transaction
 * business logic.<p>
 *
 * TransactionManager creates an instance of PersistenceManager for each
 * transaction.<p>
 *
 * TransactionManager exposes several metrics:
 * <ul>
 *     <li>Timer metric for all transactions</li>
 *     <li>Counter for failed transactions</li>
 *     <li>Counter for each exception thrown by transaction</li>
 * </ul>
 */
public class TransactionManager {

  private static final Logger LOGGER =
          LoggerFactory.getLogger(TransactionManager.class);

  private final PersistenceManagerFactory pmf;

  // Maximum number of retries per call
  private final int transactionRetryMax;

  // Delay (in milliseconds) between retries
  private final int retryWaitTimeMills;

  // Transaction timer measures time distribution for all transactions
  private final Timer transactionTimer =
          SentryMetrics.getInstance().
                  getTimer(name(TransactionManager.class,
                           "transactions"));

  // Counter for failed transactions
  private final Counter failedTransactionsCount =
          SentryMetrics.getInstance().
                  getCounter(name(TransactionManager.class,
                             "transactions", "failed"));

  private final Counter retryCount =
          SentryMetrics.getInstance().getCounter(name(TransactionManager.class,
                  "transactions", "retry"));

  TransactionManager(PersistenceManagerFactory pmf, Configuration conf) {
    this.pmf = pmf;
    this.transactionRetryMax = conf.getInt(
        ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY,
        ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_DEFAULT);
    this.retryWaitTimeMills = conf.getInt(
        ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_WAIT_TIME_MILLIS,
        ServiceConstants.ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_WAIT_TIME_MILLIS_DEFAULT);
  }


  /**
   * Execute some code as a single transaction, the code in tb.execute()
   * should not start new transaction or manipulate transactions with the
   * PersistenceManager.
   * @param tb transaction block with code to execute
   * @return Object with the result of tb.execute()
   */
  public <T> T executeTransaction(TransactionBlock<T> tb) throws Exception {
    final Timer.Context context = transactionTimer.time();
    try (PersistenceManager pm = pmf.getPersistenceManager()) {
      Transaction transaction = pm.currentTransaction();
      transaction.begin();
      try {
        T result = tb.execute(pm);
        transaction.commit();
        return result;
      } catch (Exception e) {
        // Count total failed transactions
        failedTransactionsCount.inc();
        // Count specific exceptions
        SentryMetrics.getInstance().getCounter(name(TransactionManager.class,
                "exception", e.getClass().getSimpleName())).inc();
        // Re-throw the exception
        throw e;
      } finally {
        context.stop();
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
   */
  public <T> T executeTransactionWithRetry(TransactionBlock<T> tb)
          throws Exception {
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
        retryCount.inc();
        LOGGER.warn("Exception is thrown, retry the transaction, current retry num is:"
            + retryNum + ", the max retry num is: " + transactionRetryMax, e);
        Thread.sleep(retryWaitTimeMills);
      }
    }
    return null;
  }
}
