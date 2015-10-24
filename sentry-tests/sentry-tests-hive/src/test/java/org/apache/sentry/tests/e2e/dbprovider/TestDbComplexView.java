/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.tests.e2e.dbprovider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDbComplexView extends AbstractTestWithStaticConfiguration {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(TestDbComplexView.class);

    private static final String TEST_VIEW_DB = "test_complex_view_database";
    private static final String TEST_VIEW_TB = "test_complex_view_table";
    private static final String TEST_VIEW_TB2 = "test_complex_view_table_2";
    private static final String TEST_VIEW = "test_complex_view";
    private static final String TEST_VIEW_ROLE = "test_complex_view_role";
    private PolicyFile policyFile;

    /**
     * Run query and validate one column with given column name
     * @param user
     * @param sql
     * @param db
     * @param colName
     * @param colVal
     * @return
     * @throws Exception
     */
    private static boolean execValidate(String user, String sql, String db,
                                        String colName, String colVal) throws Exception {
        boolean status = false;
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = context.createConnection(user);
            stmt = context.createStatement(conn);
            LOGGER.info("Running [USE " + db + ";" + sql + "] to validate column " +  colName + " = " + colVal);
            stmt.execute("USE " + db);
            ResultSet rset = stmt.executeQuery(sql);
            while (rset.next()) {
                String val = rset.getString(colName);
                if (val.equalsIgnoreCase(colVal)) {
                    LOGGER.info("found [" + colName + "] = " + colVal);
                    status = true;
                    break;
                } else {
                    LOGGER.warn("[" + colName + "] = " + val + " not equal to " + colVal);
                }
            }
            rset.close();
        } catch (SQLException ex) {
            LOGGER.error("SQLException: ", ex);
        } catch (Exception ex) {
            LOGGER.error("Exception: ", ex);
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (Exception ex) {
                LOGGER.error("failed to close connection and statement: " + ex);
            }
            return status;
        }
    }

    @BeforeClass
    public static void setupTestStaticConfiguration() throws Exception {
        useSentryService = true;
        AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
    }

    @Override
    @Before
    public void setup() throws Exception {
        super.setupAdmin();
        super.setup();
        policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);

        // prepare test db and base table
        List<String> sqls = new ArrayList<String>();
        sqls.add("USE DEFAULT");
        sqls.add("DROP DATABASE IF EXISTS " + TEST_VIEW_DB + " CASCADE");
        sqls.add("CREATE DATABASE IF NOT EXISTS " + TEST_VIEW_DB);
        sqls.add("USE " + TEST_VIEW_DB);
        sqls.add("CREATE TABLE " + TEST_VIEW_TB + " (userid VARCHAR(64), link STRING, source STRING) "
            + "PARTITIONED BY (datestamp STRING) CLUSTERED BY (userid) INTO 256 BUCKETS STORED AS ORC");
        sqls.add("INSERT INTO TABLE " + TEST_VIEW_TB + " PARTITION (datestamp = '2014-09-23') VALUES "
            + "('tlee', " + "'mail.com', 'sports.com'), ('jdoe', 'mail.com', null)");
        sqls.add("SELECT userid FROM " + TEST_VIEW_TB);
        sqls.add("CREATE TABLE " + TEST_VIEW_TB2 + " (userid VARCHAR(64), name VARCHAR(64), age INT, "
            + "gpa DECIMAL(3, 2)) CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC");
        sqls.add("INSERT INTO TABLE " + TEST_VIEW_TB2 + " VALUES ('rgates', 'Robert Gates', 35, 1.28), "
            + "('tlee', 'Tod Lee', 32, 2.32)");
        sqls.add("SELECT * FROM " + TEST_VIEW_TB2);
        execBatch(ADMIN1, sqls);
    }

    private void createTestRole(String user, String roleName) throws Exception {
        Connection conn = context.createConnection(user);
        Statement stmt = conn.createStatement();
        try {
            exec(stmt, "DROP ROLE " + roleName);
        } catch (Exception ex) {
            LOGGER.info("test role doesn't exist, but it's ok");
        } finally {
            exec(stmt, "CREATE ROLE " + roleName);
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    private void grantAndValidatePrivilege(String testView, String testRole, String testGroup,
                                           String user, boolean revoke) throws Exception {
        createTestRole(ADMIN1, testRole);
        List<String> sqls = new ArrayList<String>();

        // grant privilege
        sqls.add("USE " + TEST_VIEW_DB);
        sqls.add("GRANT SELECT ON TABLE " + testView + " TO ROLE " + testRole);
        sqls.add("GRANT ROLE " + testRole + " TO GROUP " + testGroup);
        execBatch(ADMIN1, sqls);

        // show grant should pass and could list view
        assertTrue("can not find select privilege from " + testRole,
                execValidate(ADMIN1, "SHOW GRANT ROLE " + testRole + " ON TABLE " + testView,
                        TEST_VIEW_DB, "privilege", "select"));
        assertTrue("can not find " + testView,
            execValidate(user, "SHOW TABLES", TEST_VIEW_DB, "tab_name", testView));

        // select from view should pass
        sqls.clear();
        sqls.add("USE " + TEST_VIEW_DB);
        sqls.add("SELECT * FROM " + testView);
        execBatch(user, sqls);

        if (revoke) {
            // revoke privilege
            sqls.clear();
            sqls.add("USE " + TEST_VIEW_DB);
            sqls.add("REVOKE SELECT ON TABLE " + testView + " FROM ROLE " + testRole);
            execBatch(ADMIN1, sqls);

            // shouldn't be able to show grant
            assertFalse("should not find select from " + testRole,
                execValidate(ADMIN1, "SHOW GRANT ROLE " + testRole + " ON TABLE " + testView,
                    TEST_VIEW_DB, "privilege", "select"));

            // select from view should fail
            sqls.clear();
            sqls.add("USE " + TEST_VIEW_DB);
            sqls.add("SELECT * FROM " + testView);
            try {
                execBatch(user, sqls);
            } catch (SQLException ex) {
                LOGGER.info("Expected SQLException here", ex);
            }
        }
    }

    private void grantAndValidatePrivilege(String testView, String testRole,
                                           String testGroup, String user) throws Exception {
        grantAndValidatePrivilege(testView, testRole, testGroup, user, true);
    }
    /**
     * Create view1 and view2 from view1
     * Grant and validate select privileges to both views
     * @throws Exception
     */
    @Test
    public void testDbViewFromView() throws Exception {
        List<String> sqls = new ArrayList<String>();
        // create a simple view
        sqls.add("USE " + TEST_VIEW_DB);
        sqls.add("CREATE VIEW " + TEST_VIEW +
                "(userid,link) AS SELECT userid,link from " + TEST_VIEW_TB);

        // create another view from the previous view
        String testView2 = "view1_from_" + TEST_VIEW;
        String testRole2 = testView2 + "_test_role";
        sqls.add(String.format("CREATE VIEW %s AS SELECT userid,link from %s",
            testView2, TEST_VIEW));

        String testView3 = "view2_from_" + TEST_VIEW;
        String testRole3 = testView3 + "_test_role";
        sqls.add(String.format("CREATE VIEW %s(userid,link) AS SELECT userid,link from %s",
            testView3, TEST_VIEW));

        execBatch(ADMIN1, sqls);

        // validate privileges
        grantAndValidatePrivilege(TEST_VIEW, TEST_VIEW_ROLE, USERGROUP1, USER1_1);
        grantAndValidatePrivilege(testView2, testRole2, USERGROUP2, USER2_1);

        // Disabled because of SENTRY-745, also need to backport HIVE-10875
        //grantAndValidatePrivilege(testView3, testRole3, USERGROUP3, USER3_1);
    }

    /**
     * Create a view by join two tables
     * Grant and verify select privilege
     * @throws Exception
     */
    @Test
    public void TestDbViewWithJoin() throws Exception {
        List<String> sqls = new ArrayList<String>();
        // create a joint view
        sqls.add("USE " + TEST_VIEW_DB);
        sqls.add(String.format("create view %s as select name,age,gpa from %s join %s on "
                + "(%s.userid=%s.userid) where name='Tod Lee'", TEST_VIEW, TEST_VIEW_TB2,
            TEST_VIEW_TB, TEST_VIEW_TB2, TEST_VIEW_TB));
        execBatch(ADMIN1, sqls);

        // validate privileges
        grantAndValidatePrivilege(TEST_VIEW, TEST_VIEW_ROLE, USERGROUP1, USER1_1);
    }

    /**
     * Create a view with nested query
     * Grant and verify select privilege
     * @throws Exception
     * SENTRY-716: Hive plugin does not correctly enforce
     * privileges for new in case of nested queries
     * Once backport HIVE-10875 to Sentry repo, will enable this test.
     */
    @Ignore ("After SENTRY-716 is fixed, turn on this test")
    @Test
    public void TestDbViewWithNestedQuery() throws Exception {
        List<String> sqls = new ArrayList<String>();
        // create a joint view
        sqls.add("USE " + TEST_VIEW_DB);
        sqls.add("CREATE VIEW " + TEST_VIEW + " AS SELECT * FROM " + TEST_VIEW_TB);
        execBatch(ADMIN1, sqls);
        grantAndValidatePrivilege(TEST_VIEW, TEST_VIEW_ROLE, USERGROUP1, USER1_1, false);

        sqls.clear();
        sqls.add("USE " + TEST_VIEW_DB);
        sqls.add("SELECT * FROM (SELECT * FROM " + TEST_VIEW + ") v2");
        execBatch(USER1_1, sqls);
    }

    /**
     * Create a view with union two tables
     * Grant and verify select privilege
     * @throws Exception
     * SENTRY-747: Create a view by union tables, grant select
     * then select from view encounter errors
     * Once backport HIVE-10875 to Sentry repo, will enable this test.
     */
    @Ignore ("After SENTRY-747 is fixed, turn on this test")
    @Test
    public void TestDbViewWithUnion() throws Exception {
        List<String> sqls = new ArrayList<String>();
        String testTable = "test_user_info";
        sqls.add("USE " + TEST_VIEW_DB);
        sqls.add("DROP TABLE IF EXISTS " + testTable);
        sqls.add("CREATE TABLE " + testTable + " (userid VARCHAR(64), name STRING, address STRING, tel STRING) ");
        sqls.add("INSERT INTO TABLE " + testTable + " VALUES "
                + "('tlee', " + "'Tod Lee', '1234 23nd Ave SFO, CA', '123-456-7890')");
        sqls.add("SELECT * FROM " + testTable);
        sqls.add(String.format("CREATE VIEW " + TEST_VIEW + " AS "
                        + "SELECT u.userid, u.name, u.address, res.uid "
                        + "FROM ("
                        + "SELECT t1.userid AS uid "
                        + "FROM %s t1 "
                        + "UNION ALL "
                        + "SELECT t2.userid AS uid "
                        + "FROM %s t2 "
                        + ") res JOIN %s u ON (u.userid = res.uid)",
                TEST_VIEW_TB, TEST_VIEW_TB2, testTable));
        execBatch(ADMIN1, sqls);
        grantAndValidatePrivilege(TEST_VIEW, TEST_VIEW_ROLE, USERGROUP1, USER1_1);
    }
}