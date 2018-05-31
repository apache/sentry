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

import com.codahale.metrics.Timer.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.apache.sentry.api.service.thrift.SentryMetrics;

import java.util.Random;
import java.util.concurrent.Callable;

/**
 * TransactionManager is used for executing the database transaction, it supports
 * the transaction with retry mechanism for the unexpected exceptions,
 * except <em>SentryUserExceptions</em>, eg, <em>SentryNoSuchObjectException</em>,
 * <em>SentryAlreadyExistsException</em> etc. For <em>SentryUserExceptions</em>,
 * will simply throw the exception without retry<p>
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
@SuppressWarnings("NestedTryStatement")
public final class TransactionManager {

  private static final Logger LOGGER =
          LoggerFactory.getLogger(TransactionManager.class);

  /** Random number generator for exponential backoff */
  private static final Random random = new Random();

  private final PersistenceManagerFactory pmf;

  // Maximum number of retries per call
  private final int transactionRetryMax;

  // Delay (in milliseconds) between retries
  private final int retryWaitTimeMills;

  /** Name for metrics */
  private static final String TRANSACTIONS = "transactions";

  // Transaction timer measures time distribution for all transactions
  private final Timer transactionTimer =
          SentryMetrics.getInstance().
                  getTimer(name(TransactionManager.class,
                           TRANSACTIONS));

  // Counter for failed transactions
  private final Counter failedTransactionsCount =
          SentryMetrics.getInstance().
                  getCounter(name(TransactionManager.class,
                             TRANSACTIONS, "failed"));

  private final Counter retryCount =
          SentryMetrics.getInstance().getCounter(name(TransactionManager.class,
                  TRANSACTIONS, "retry"));

  TransactionManager(PersistenceManagerFactory pmf, Configuration conf) {
    this.pmf = pmf;
    transactionRetryMax = conf.getInt(
        ServerConfig.SENTRY_STORE_TRANSACTION_RETRY,
        ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_DEFAULT);
    retryWaitTimeMills = conf.getInt(
        ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_WAIT_TIME_MILLIS,
        ServerConfig.SENTRY_STORE_TRANSACTION_RETRY_WAIT_TIME_MILLIS_DEFAULT);
  }


  /**
   * Execute some code as a single transaction, the code in tb.execute()
   * should not start new transaction or manipulate transactions with the
   * PersistenceManager.
   *
   * @param tb transaction block with code to be executed
   * @return Object with the result of tb.execute()
   */
  public <T> T executeTransaction(TransactionBlock<T> tb) throws Exception {
    try (Context context = transactionTimer.time();
         PersistenceManager pm = pmf.getPersistenceManager()) {
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
        if (transaction.isActive()) {
          transaction.rollback();
        }
      }
    }
  }

  /**
   * Execute a list of TransactionBlock code as a single transaction.
   * The code in tb.execute() should not start new transaction or
   * manipulate transactions with the PersistenceManager. It returns
   * the result of the last transaction block execution.
   *
   * @param tbs transaction blocks with code to be executed
   * @return the result of the last result of tb.execute()
   */
  private <T> T executeTransaction(Iterable<TransactionBlock<T>> tbs) throws Exception {
    try (Context context = transactionTimer.time();
         PersistenceManager pm = pmf.getPersistenceManager()) {
      Transaction transaction = pm.currentTransaction();
      transaction.begin();
      try {
        T result = null;
        for (TransactionBlock<T> tb : tbs) {
          result = tb.execute(pm);
        }
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
        if (transaction.isActive()) {
          transaction.rollback();
        }
      }
    }
  }

  /**
   * Execute some code as a single transaction with retry mechanism.
   *
   * @param tb transaction block with code to execute
   * @return Object with the result of tb.execute()
   */
  @SuppressWarnings("squid:S00112")
  public <T> T executeTransactionWithRetry(final TransactionBlock<T> tb)
          throws Exception {
    return new ExponentialBackoff().execute(
            new Callable<T>() {
              @Override
              public T call() throws Exception {
                return executeTransaction(tb);
              }
            }
    );
  }

  /**
   * Execute a list of TransactionBlock code as a single transaction.
   * If any of the TransactionBlock fail, all the TransactionBlocks would
   * retry. It returns the result of the last transaction block
   * execution.
   *
   * @param tbs a list of transaction blocks with code to be executed.
   */
  @SuppressWarnings("squid:S00112")
  <T> void executeTransactionBlocksWithRetry(final Iterable<TransactionBlock<T>> tbs)
          throws Exception {
    new ExponentialBackoff().execute(
            new Callable<T>() {
              @Override
              public T call() throws Exception {
                return executeTransaction(tbs);
              }
            }
    );
  }

  /**
   * Implementation of exponential backoff with random fuzziness.
   * On each iteration the backoff time is 1.5 the previous amount plus the
   * random fuzziness factor which is up to half of the previous amount.
   */
  private class ExponentialBackoff {

    @SuppressWarnings("squid:S00112")
    <T> T execute(Callable<T> arg) throws Exception {
      Exception ex = null;
      long sleepTime = retryWaitTimeMills;

      for (int retryNum = 1; retryNum <= transactionRetryMax; retryNum++) {
        try {
          return arg.call();
        } catch (SentryUserException e) {
          // throw the sentry exception without retry
          LOGGER.warn("Transaction manager encountered non-retriable exception", e);
          throw e;
        } catch (Exception e) {
          ex = e;
          retryCount.inc();
          LOGGER.warn("Transaction execution encountered exception", e);
          LOGGER.warn("Retrying transaction {}/{} times",
                  retryNum, transactionRetryMax);
          // Introduce some randomness in the backoff time.
          LOGGER.warn("Sleeping for {} milliseconds before retrying", sleepTime);
          Thread.sleep(sleepTime);
          int fuzz = random.nextInt((int)sleepTime / 2);
          sleepTime *= 3;
          sleepTime /= 2;
          sleepTime += fuzz;
        }
      }
      assert(ex != null);
      String message = "The transaction has reached max retry number, "
              + ex.getMessage();
      LOGGER.error(message, ex);
      throw new Exception(message, ex);
    }
  }
}
