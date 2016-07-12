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

import java.util.List;

import com.google.common.base.Joiner;

import javax.jdo.JDOException;
import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fences the SQL database.<p/>
 *
 * Fencing ensures that any SQL requests that were sent by a previously active
 * (but now standby) sentry daemon will not be honored.  It also ensures that if
 * users start up multiple non-HA sentry daemons, only one can become
 * active.<p/>
 *
 * The fencer uses a special SQL table, the SENTRY_FENCE table.  When a sentry
 * process becomes active, it renames this table so that the name contains the
 * current "incarnation ID."  The incarnation ID is a randomly generated 128-bit
 * ID, which changes each time the process is restarted.  From that point
 * forward, the sentry process includes a SELECT query for the SENTRY_FENCE
 * table in all update transactions.  This ensures that if the SENTRY_FENCE
 * table is subsequently renamed again, those update transactions will not
 * succeed.<p/>
 *
 * It is important to distinguish between fencing and leader election.
 * ZooKeeper is responsible for leader election and ensures that there is only
 * ever one active sentry daemon at any one time.  However, sentry exists in an
 * asynchronous network where requests from a previously active daemon may be
 * arbitrarily delayed before reaching the SQL databse.  There is also a delay
 * between a process being "de-leadered" by ZooKeeper, and the process itself
 * becoming aware of this situation.  Java's garbage collection pauses tend to
 * expose these kinds of race conditions.  The SQL database must be prepared to
 * reject these stale updates.<p/>
 *
 * Given that we need this SQL fencing, why bother with ZooKeeper at all?
 * ZooKeeper detects when nodes have stopped responding, and elects a new
 * leader.  The SQL fencing code cannot do that.<p/>
 */
public class Fencer {
  private static final Logger LOGGER = LoggerFactory
          .getLogger(Fencer.class);

  /**
   * The base name of the sentry fencer table.<p/>
   *
   * We will append the incarnation ID on to this base name to make the final
   * table name.
   */
  private final static String SENTRY_FENCE_TABLE_BASE = "SENTRY_FENCE";

  /**
   * The update log table name, including the incarnation ID.
   */
  private final String tableIncarnationName;

  /**
   * The SQL accessor that we're using.
   */
  private final SqlAccessor sql;

  /**
   * Create an accessor for the update log.
   *
   * @param incarnationId     The ID of the current sentry daemon incarnation.
   * @param pmf               The PersistenceManagerFactory to use.
   */
  public Fencer(String incarnationId, PersistenceManagerFactory pmf) {
    this.tableIncarnationName = String.
        format("%s_%s", SENTRY_FENCE_TABLE_BASE, incarnationId);
    this.sql = SqlAccessor.get(pmf);
  }

  /**
   * Finds the name of the fencing table.<p/>
   *
   * The name of the update log table will always begin with SENTRY_UPDATE_LOG,
   * but it may have the ID of a previous sentry incarnation tacked on to it.
   *
   * @return the current name of the update log table, or null if there is none.
   *
   * @throws JDOFatalDataStoreException    If there is more than one sentry
   *                                       fencing table.
   *         JDOException                  If there was a JDO error.
   */
  private String findFencingTable(PersistenceManagerFactory pmf) {
    // Perform a SQL query to find the name of the update log table.
    PersistenceManager pm = pmf.getPersistenceManager();
    Query query = pm.newQuery(SqlAccessor.JDO_SQL_ESCAPE,
        sql.getFindTableByPrefixSql(SENTRY_FENCE_TABLE_BASE));
    Transaction tx = pm.currentTransaction();
    try {
      tx.begin();
      List<Object> results = (List<Object>) query.execute();
      if (results.isEmpty()) {
        return null;
      } else if (results.size() != 1) {
        throw new JDOFatalDataStoreException(
            "Found more than one table whose name begins with " +
            "SENTRY_UPDATE_LOG: " + Joiner.on(",").join(results));
      }
      String tableName = (String)results.get(0);
      if (!tableName.startsWith(SENTRY_FENCE_TABLE_BASE)) {
        throw new JDOFatalDataStoreException(
            "The result of our attempt to locate the update log table was " +
            "a table name which did not begin with " +
            SENTRY_FENCE_TABLE_BASE + ", named " + tableName);
      }
      LOGGER.info("Found sentry update log table named " + tableName);
      tx.commit();
      return tableName;
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
      query.closeAll();
    }
  }

  /**
   * Creates the fencing table.
   *
   * @param pmf                 The PersistenceManagerFactory to use.
   *
   * @throws  JDOException      If there was a JDO error.
   */
  private void createFenceTable(PersistenceManagerFactory pmf) {
    PersistenceManager pm = pmf.getPersistenceManager();
    Transaction tx = pm.currentTransaction();
    Query query = null;
    try {
      tx.begin();
      query = pm.newQuery(SqlAccessor.JDO_SQL_ESCAPE,
          sql.getCreateTableSql(tableIncarnationName));
      query.execute();
      tx.commit();
    } finally {
      if (query != null) {
        query.closeAll();
      }
      if (tx.isActive()) {
        tx.rollback();
      }
      pm.close();
    }
  }

  /**
   * Renames one table to another.
   *
   * @param pmf                 The PersistenceManagerFactory to use.
   * @param src                 The table to rename
   * @param dst                 The new name of the table.
   *
   * @throws  JDOException      If there was a JDO error.
   */
  private void renameTable(PersistenceManagerFactory pmf, String src,
          String dst) {
    boolean success = false;
    PersistenceManager pm = pmf.getPersistenceManager();
    Transaction tx = pm.currentTransaction();
    Query query = null;
    try {
      tx.begin();
      query = pm.newQuery(SqlAccessor.JDO_SQL_ESCAPE,
          sql.getRenameTableSql(src, dst));
      query.execute();
      tx.commit();
      success = true;
    } finally {
      if (query != null) {
        query.closeAll();
      }
      if (!success) {
        LOGGER.info("Failed to rename table " + src + " to " + dst);
        tx.rollback();
      }
      pm.close();
    }
  }

  /**
   * Renames the update log table so that only this incarnation can modify it.
   *
   * @param pmf                 The PersistenceManagerFactory to use.
   *
   * @throws  JDOException      If there was a JDO error.
   */
  public void fence(PersistenceManagerFactory pmf) {
    String curTableName = findFencingTable(pmf);
    if (curTableName == null) {
      createFenceTable(pmf);
      LOGGER.info("Created sentry fence table.");
    } else if (curTableName.equals(tableIncarnationName)) {
      LOGGER.info("Sentry fence table is already named " +
          tableIncarnationName);
    } else {
      renameTable(pmf, curTableName, tableIncarnationName);
      LOGGER.info("Renamed sentry fence table " + curTableName + " to " +
          tableIncarnationName);
    }
  }

  /**
   * Attempt to append an UpdateLogEntry to the update log.
   */
  void verify(PersistenceManager pm) {
    Query query = pm.newQuery(SqlAccessor.JDO_SQL_ESCAPE,
        sql.getFetchAllRowsSql(tableIncarnationName));
    query.execute();
  }

  String getTableIncarnationName() {
    return tableIncarnationName;
  }
}
