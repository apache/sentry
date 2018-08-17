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
package org.apache.sentry.tests.e2e.hive;

import com.google.common.collect.Sets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType.Server;
import static org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType.Db;
import static org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType.Table;
import static org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType.Column;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestAllowedGrantPrivileges extends AbstractTestWithStaticConfiguration  {
  private static Connection adminCon;
  private static Statement adminStmt;
  private static final String SERVER1 = "server1";

  private DBModelAction action;
  private Set<AuthorizableType> allowedScope;

  @Parameterized.Parameters
  public static Collection getPrivileges() {
    return Arrays.asList(new Object[][] {
      { DBModelAction.ALL,     Sets.newHashSet(Server, Db, Table) },
      { DBModelAction.CREATE,  Sets.newHashSet(Server, Db) },
      { DBModelAction.SELECT,  Sets.newHashSet(Server, Db, Table, Column) },
      { DBModelAction.INSERT,  Sets.newHashSet(Server, Db, Table) },
      { DBModelAction.ALTER,   Sets.newHashSet(Server, Db, Table) },
      { DBModelAction.DROP,    Sets.newHashSet(Server, Db, Table) },
      { DBModelAction.INDEX,   Sets.newHashSet(Server, Db, Table) },
      { DBModelAction.LOCK,    Sets.newHashSet(Server, Db, Table) }
    });
  }

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception{
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
    setupAdmin();

    adminCon = context.createConnection(ADMIN1);
    adminStmt = context.createStatement(adminCon);
  }

  @AfterClass
  public static void destroy() throws SQLException {
    adminStmt.close();
    adminCon.close();
  }

  public TestAllowedGrantPrivileges(DBModelAction action, Set<AuthorizableType> allowedScope) {
    this.action = action;
    this.allowedScope = allowedScope;
  }

  @Before
  public void setup() throws Exception {
    adminStmt.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    adminStmt.execute("CREATE DATABASE " + DB1);
    adminStmt.execute("CREATE TABLE " + DB1 + "." + TBL1 + " (id int)");
    adminStmt.execute("CREATE ROLE role1");
  }

  private void verifyGrantCommand(String command, AuthorizableType scope) {
    try {
      adminStmt.execute(command);
      if (!allowedScope.contains(scope)) {
        fail(String.format("GRANT %s should NOT be allowed on the %s scope.", action, scope));
      }
    } catch (Exception e) {
      if (allowedScope.contains(scope)) {
        fail(String.format("GRANT %s should be allowed on the %s scope.", action, scope));
      }
    }
  }

  @Test
  public void testGrantOnServer() {
    verifyGrantCommand("GRANT " + action + " ON SERVER " + SERVER1 + " TO ROLE role1", Server);
  }

  @Test
  public void testGrantOnDatabase() {
    verifyGrantCommand("GRANT " + action + " ON DATABASE " + DB1 + " TO ROLE role1", Db);
  }

  @Test
  public void testGrantOnTable() {
    verifyGrantCommand("GRANT " + action + " ON TABLE " + DB1 + "." + TBL1 + " TO ROLE role1", Table);
  }

  @Test
  public void testGrantOnColumn() {
    verifyGrantCommand("GRANT " + action + "(id) ON TABLE " + DB1 + "." + TBL1 + " TO ROLE role1", Column);
  }
}
