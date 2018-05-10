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

package org.apache.sentry.api.service.thrift;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sentry.provider.db.log.appender.AuditLoggerTestAppender;
import org.apache.sentry.provider.db.log.util.CommandUtil;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.codehaus.jettison.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestAuthorizingDDLAuditLogWithKerberos extends SentryServiceIntegrationBase {

  @BeforeClass
  public static void setupLog4j() throws Exception {
    Logger logger = Logger.getLogger("sentry.hive.authorization.ddl.logger");
    AuditLoggerTestAppender testAppender = new AuditLoggerTestAppender();
    logger.addAppender(testAppender);
    logger.setLevel(Level.INFO);
  }

  @Test
  public void testBasic() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        String roleName = "testRole";
        String errorRoleName = "errorRole";
        String serverName = "server1";
        String groupName = "testGroup";
        String dbName = "dbTest";
        String tableName = "tableTest";
        Map<String, String> fieldValueMap = new HashMap<String, String>();

        // for successful audit log
      client.createRole(requestorUserName, roleName);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_CREATE_ROLE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "CREATE ROLE " + roleName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
        // for ip address, there is another logic to test the result
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);

        client.grantRoleToGroup(requestorUserName, groupName, roleName);
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_ADD_ROLE);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ROLE " + roleName
            + " TO GROUP " + groupName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);

        client.grantDatabasePrivilege(requestorUserName, roleName, serverName, dbName, "ALL");
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ALL ON DATABASE " + dbName
            + " TO ROLE " + roleName);
        fieldValueMap.put(Constants.LOG_FIELD_DATABASE_NAME, dbName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);

        client.grantTablePrivilege(requestorUserName, roleName, serverName, dbName, tableName,
            "SELECT", true);
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT SELECT ON TABLE " + tableName
            + " TO ROLE " + roleName + " WITH GRANT OPTION");
        fieldValueMap.put(Constants.LOG_FIELD_TABLE_NAME, tableName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);

        // for error audit log
        try {
          client.createRole(requestorUserName, roleName);
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_CREATE_ROLE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "CREATE ROLE " + roleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }
        try {
          client.grantRoleToGroup(requestorUserName, groupName, errorRoleName);
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_ADD_ROLE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ROLE " + errorRoleName
              + " TO GROUP " + groupName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }
        try {
          client
              .grantDatabasePrivilege(requestorUserName, errorRoleName, serverName, dbName, "ALL");
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ALL ON DATABASE " + dbName
              + " TO ROLE " + errorRoleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }
        try {
          client.grantDatabasePrivilege(requestorUserName, errorRoleName, serverName, dbName,
              "INSERT");
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT INSERT ON DATABASE "
              + dbName + " TO ROLE " + errorRoleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }
        try {
          client.grantDatabasePrivilege(requestorUserName, errorRoleName, serverName, dbName,
              "SELECT");
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT SELECT ON DATABASE "
              + dbName + " TO ROLE " + errorRoleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }
        try {
          client.grantTablePrivilege(requestorUserName, errorRoleName, serverName, dbName,
              tableName, "SELECT");
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT SELECT ON TABLE "
              + tableName + " TO ROLE " + errorRoleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }

        client.revokeTablePrivilege(requestorUserName, roleName, serverName, dbName, tableName,
          "SELECT");
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE SELECT ON TABLE " + tableName
            + " FROM ROLE " + roleName);
        fieldValueMap.put(Constants.LOG_FIELD_TABLE_NAME, tableName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);

        client.revokeDatabasePrivilege(requestorUserName, roleName, serverName, dbName, "ALL");
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ALL ON DATABASE " + dbName
            + " FROM ROLE " + roleName);
        fieldValueMap.put(Constants.LOG_FIELD_DATABASE_NAME, dbName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);

        client.revokeRoleFromGroup(requestorUserName, groupName, roleName);
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DELETE_ROLE);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ROLE " + roleName
          + " FROM GROUP " + groupName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);

        client.dropRole(requestorUserName, roleName);
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DROP_ROLE);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "DROP ROLE " + roleName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);

        // for error audit log
        try {
          client.revokeTablePrivilege(requestorUserName, errorRoleName, serverName, dbName,
              tableName, "SELECT");
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE SELECT ON TABLE "
              + tableName + " FROM ROLE " + errorRoleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }

        try {
          client.revokeDatabasePrivilege(requestorUserName, errorRoleName, serverName, dbName,
              "ALL");
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ALL ON DATABASE " + dbName
              + " FROM ROLE " + errorRoleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }

        try {
          client.revokeRoleFromGroup(requestorUserName, groupName, errorRoleName);
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DELETE_ROLE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ROLE " + errorRoleName
              + " FROM GROUP " + groupName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }

        try {
          client.dropRole(requestorUserName, errorRoleName);
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DROP_ROLE);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "DROP ROLE " + errorRoleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }
      }
    });
  }

  private void assertAuditLog(Map<String, String> fieldValueMap) throws Exception {
    assertThat(AuditLoggerTestAppender.getLastLogLevel(), is(Level.INFO));
    JSONObject jsonObject = new JSONObject(AuditLoggerTestAppender.getLastLogEvent());
    if (fieldValueMap != null) {
      for (Map.Entry<String, String> entry : fieldValueMap.entrySet()) {
        String entryKey = entry.getKey();
        if (Constants.LOG_FIELD_IP_ADDRESS.equals(entryKey)) {
          assertTrue(CommandUtil.assertIPInAuditLog(jsonObject.get(entryKey).toString()));
        } else {
          assertTrue(entry.getValue().equalsIgnoreCase(jsonObject.get(entryKey).toString()));
        }
      }
    }
  }
}
