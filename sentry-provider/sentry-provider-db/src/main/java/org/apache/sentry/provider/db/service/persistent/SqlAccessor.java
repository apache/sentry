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

import java.sql.Connection;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.datastore.JDOConnection;

/**
 * An accessor for a SQL database.
 *
 * SqlAccessor objects generate raw SQL statements in a variety of dialects.
 * We use this to do stuff that the DataNucleus architects didn't anticipate,
 * like rename tables or search for tables by a prefix name.<p/>
 *
 * This class exists only to implement fencing.  While it's theoretically
 * possible to do other things with it, it is almost always better to use the
 * functionality provided by DataNucleus if it is at all possible.<p/>
 *
 * Note: do NOT pass any untrusted user input into these functions.  You must
 * NOT create SQL statements from unsanitized user input because they may expose
 * you to SQL injection attacks.  Use prepared statements if you need to do that
 * (yes, it's possible via DataNucleus.)<p/>
 */
abstract class SqlAccessor {
  /**
   * The string which we can use with PersistenceManager#newQuery to perform raw
   * SQL operations.
   */
  final static String JDO_SQL_ESCAPE = "javax.jdo.query.SQL";

  /**
   * Get an accessor for the SQL database that we're using.
   *
   * @return The singleton accessor instance for the SQL database we are using.
   *
   * @throws RuntimeException     If there was an error loading the SqlAccessor.
   *                              This could happen because we don't know the
   *                              type of database that we're using.  In theory
   *                              it could also happen because JDO is being run
   *                              against something that is not a SQL databse at
   *                              all.
   */
  static SqlAccessor get(PersistenceManagerFactory pmf) {
    String productName = getProductNameString(pmf).toLowerCase();
    if (productName.contains("postgresql")) {
      return PostgresSqlAccessor.INSTANCE;
    } else if (productName.contains("mysql")) {
      return MySqlSqlAccessor.INSTANCE;
    } else if (productName.contains("oracle")) {
      return OracleSqlAccessor.INSTANCE;
    } else if (productName.contains("derby")) {
      return DerbySqlAccessor.INSTANCE;
    } else if (productName.contains("db2")) {
      return Db2SqlAccessor.INSTANCE;
    } else {
      throw new RuntimeException("Unknown database type " +
      "'" + productName + "'.  Supported database types are " +
      "postgres, mysql, oracle, mssql, and derby.");
    }
  }

  /**
   * @return    An string describing the type of database that we're using.
   */
  static private String getProductNameString(PersistenceManagerFactory pmf) {
    PersistenceManager pm = pmf.getPersistenceManager();
    JDOConnection jdoConn = pm.getDataStoreConnection();
    try {
      return ((Connection)jdoConn.getNativeConnection()).getMetaData().
          getDatabaseProductName();
    } catch (Throwable t) {
      throw new RuntimeException("Error retrieving database product " +
          "name", t);
    } finally {
      // We must release the connection before we call other pm methods.
      jdoConn.close();
    }
  }

  /**
   * Get the name of this database.
   *
   * @return          The name of this databse.
   */
  abstract String getDatabaseName();

  /**
   * Get the SQL for finding a table that starts with the given prefix.
   *
   * @param prefix    The prefix of the table to find.
   * @return          The SQL.
   */
  abstract String getFindTableByPrefixSql(String prefix);

  /**
   * Get the SQL for creating a table with the given name.
   *
   * @param name      The name of the table to create.
   * @return          The SQL.
   */
  abstract String getCreateTableSql(String name);

  /**
   * Get the SQL for renaming a table.
   *
   * @param src       The name of the table to rename.
   * @param dst       The new name to give to the table.
   * @return          The SQL.
   */
  abstract String getRenameTableSql(String src, String dst);

  /**
   * Get the SQL for fetching all rows from the given table.
   *
   * @param name      The table name.
   * @return          The SQL.
   */
  abstract String getFetchAllRowsSql(String name);

  /**
   * The postgres database type.<p/>
   *
   * Postgres is case-senstitive, but will translate all identifiers to
   * lowercase unless you quote them.  So we quote all identifiers when using
   * postgres.
   */
  private static class PostgresSqlAccessor extends SqlAccessor {
    static final PostgresSqlAccessor INSTANCE = new PostgresSqlAccessor();

    @Override
    String getDatabaseName() {
      return "postgres";
    }

    @Override
    String getFindTableByPrefixSql(String prefix) {
      return "SELECT table_name FROM information_schema.tables " +
          "WHERE table_name LIKE '" + prefix + "%'";
    }

    @Override
    String getCreateTableSql(String name) {
      return "CREATE TABLE \"" + name + "\" (\"VAL\" VARCHAR(512))";
    }

    @Override
    String getRenameTableSql(String src, String dst) {
      return "ALTER TABLE \"" + src + "\" RENAME TO \"" + dst + "\"";
    }

    @Override
    String getFetchAllRowsSql(String tableName) {
      return "SELECT * FROM \"" + tableName + "\"";
    }
  }

  /**
   * The MySQL database type.<p/>
   *
   * MySQL can't handle quotes unless specifically configured to accept them.
   */
  private static class MySqlSqlAccessor extends SqlAccessor {
    static final MySqlSqlAccessor INSTANCE = new MySqlSqlAccessor();

    @Override
    String getDatabaseName() {
      return "mysql";
    }

    @Override
    String getFindTableByPrefixSql(String prefix) {
      return "SELECT table_name FROM information_schema.tables " +
          "WHERE table_name LIKE " + prefix + "%";
    }

    @Override
    String getCreateTableSql(String name) {
      return "CREATE TABLE " + name + " (VAL VARCHAR(512))";
    }

    @Override
    String getRenameTableSql(String src, String dst) {
      return "RENAME TABLE " + src + " TO " + dst;
    }

    @Override
    String getFetchAllRowsSql(String tableName) {
      return "SELECT * FROM " + tableName;
    }
  }

  /**
   * The Oracle database type.<p/>
   */
  private static class OracleSqlAccessor extends SqlAccessor {
    static final OracleSqlAccessor INSTANCE = new OracleSqlAccessor();

    @Override
    String getDatabaseName() {
      return "oracle";
    }

    @Override
    String getFindTableByPrefixSql(String prefix) {
      return "SELECT table_name FROM all_tables " +
          "WHERE table_name LIKE " + prefix + "%";
    }

    @Override
    String getCreateTableSql(String name) {
      return "CREATE TABLE " + name + " (VAL VARCHAR(512))";
    }

    @Override
    String getRenameTableSql(String src, String dst) {
      return "RENAME TABLE " + src + " TO " + dst;
    }

    @Override
    String getFetchAllRowsSql(String tableName) {
      return "SELECT * FROM " + tableName;
    }
  }

  /**
   * The Derby database type.</p>
   */
  private static class DerbySqlAccessor extends SqlAccessor {
    static final DerbySqlAccessor INSTANCE = new DerbySqlAccessor();

    @Override
    String getFindTableByPrefixSql(String prefix) {
      return "SELECT tablename FROM SYS.SYSTABLES " +
          "WHERE tablename LIKE '" + prefix + "%'";
    }

    @Override
    String getCreateTableSql(String name) {
      return "CREATE TABLE " + name + " (VAL VARCHAR(512))";
    }

    @Override
    String getRenameTableSql(String src, String dst) {
      return "RENAME TABLE " + src + " TO " + dst;
    }

    @Override
    String getDatabaseName() {
      return "derby";
    }

    @Override
    String getFetchAllRowsSql(String tableName) {
      return "SELECT * FROM " + tableName;
    }
  }

  /**
   * The DB2 database type.</p>
   */
  private static class Db2SqlAccessor extends SqlAccessor {
    static final Db2SqlAccessor INSTANCE = new Db2SqlAccessor();

    @Override
    String getFindTableByPrefixSql(String prefix) {
      return "SELECT tablename FROM SYS.SYSTABLES " +
          "WHERE tablename LIKE '" + prefix + "%'";
    }

    @Override
    String getCreateTableSql(String name) {
      return "CREATE TABLE " + name + " (VAL VARCHAR(512))";
    }

    @Override
    String getRenameTableSql(String src, String dst) {
      return "RENAME TABLE " + src + " TO " + dst;
    }

    @Override
    String getDatabaseName() {
      return "db2";
    }

    @Override
    String getFetchAllRowsSql(String tableName) {
      return "SELECT * FROM " + tableName;
    }
  }
}
