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

package org.apache.sentry.provider.db.generic.service.thrift;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.PrivilegedExceptionAction;
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
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestAuditLogForSentryGenericService extends SentryServiceIntegrationBase {

  private SentryGenericServiceClient client;
  private static final String COMPONENT = "SOLR";
  private static final org.slf4j.Logger LOGGER = LoggerFactory
      .getLogger(TestAuditLogForSentryGenericService.class);

  @BeforeClass
  public static void setup() throws Exception {
    SentryServiceIntegrationBase.setup();
    Logger logger = Logger.getLogger("sentry.generic.authorization.ddl.logger");
    AuditLoggerTestAppender testAppender = new AuditLoggerTestAppender();
    logger.addAppender(testAppender);
    logger.setLevel(Level.INFO);
  }

  @Override
  @After
  public void after() {
    try {
      runTestAsSubject(new TestOperation() {
        @Override
        public void runTestAsSubject() throws Exception {
          Set<TSentryRole> tRoles = client.listAllRoles(ADMIN_USER, COMPONENT);
          for (TSentryRole tRole : tRoles) {
            client.dropRole(ADMIN_USER, tRole.getRoleName(), COMPONENT);
          }
          if (client != null) {
            client.close();
          }
        }
      });
    } catch (Exception e) {
      // log the exception
      LOGGER.warn("Exception happened after test case.", e);
    } finally {
      policyFilePath.delete();
    }
  }

  /**
   * use the generic client to connect sentry service
   */
  @Override
  public void connectToSentryService() throws Exception {
    if (kerberos) {
      this.client = clientUgi.doAs(new PrivilegedExceptionAction<SentryGenericServiceClient>() {
            @Override
            public SentryGenericServiceClient run() throws Exception {
              return SentryGenericServiceClientFactory.create(conf);
            }
          });
    } else {
      this.client = SentryGenericServiceClientFactory.create(conf);
    }
  }

  @Test
  public void testAuditLogForGenericModel() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_r";
        String testGroupName = "g1";
        String action = "*";
        String service = "sentryService";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        // test the audit log for create role, success
        client.createRole(requestorUserName, roleName, COMPONENT);
        Map<String, String> fieldValueMap = new HashMap<String, String>();
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_CREATE_ROLE);
        //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "CREATE ROLE " + roleName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
        fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
        assertAuditLog(fieldValueMap);

        // test the audit log for create role, failed
        try {
          client.createRole(requestorUserName, roleName, COMPONENT);
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_CREATE_ROLE);
          //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "CREATE ROLE " + roleName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }

        // test the audit log for add role to group, success
        client.addRoleToGroups(requestorUserName, roleName, COMPONENT,
            Sets.newHashSet(testGroupName));
        fieldValueMap.clear();
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_ADD_ROLE);
        //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ROLE " + roleName
            + " TO GROUP " + testGroupName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
        fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
        assertAuditLog(fieldValueMap);

        // test the audit log for add role to group, failed
        try {
          client.addRoleToGroups(requestorUserName, "invalidRole", COMPONENT,
              Sets.newHashSet(testGroupName));
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_ADD_ROLE);
          //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ROLE invalidRole TO GROUP "
              + testGroupName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }

        // test the audit log for grant privilege, success
        TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT, service, Lists.newArrayList(
            new TAuthorizable("resourceType1", "resourceName1"), new TAuthorizable("resourceType2",
                "resourceName2")), action);
        client.grantPrivilege(requestorUserName, roleName, COMPONENT, privilege);
        fieldValueMap.clear();
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
        //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT,
            "GRANT ALL ON resourceType1 resourceName1 resourceType2 resourceName2 TO ROLE "
                + roleName + " ON COMPONENT " + COMPONENT);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
        fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
        assertAuditLog(fieldValueMap);

        // for error audit log
        TSentryPrivilege invalidPrivilege = new TSentryPrivilege(COMPONENT, service,
            Lists.newArrayList(new TAuthorizable("resourceType1", "resourceName1")),
            "invalidAction");
        // test the audit log for grant privilege, failed
        try {
          client.grantPrivilege(requestorUserName, roleName, COMPONENT, invalidPrivilege);
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
          //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT,
              "GRANT INVALIDACTION ON resourceType1 resourceName1 TO ROLE " + roleName
                  + " ON COMPONENT " + COMPONENT);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }

        // test the audit log for revoke privilege, success
        client.revokePrivilege(requestorUserName, roleName, COMPONENT, privilege);
        fieldValueMap.clear();
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
        //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT,
            "REVOKE ALL ON resourceType1 resourceName1 resourceType2 resourceName2 FROM ROLE "
                + roleName + " ON COMPONENT " + COMPONENT);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
        fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
        assertAuditLog(fieldValueMap);

        // test the audit log for revoke privilege, failed
        try {
          client.revokePrivilege(requestorUserName, "invalidRole", COMPONENT, invalidPrivilege);
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
          //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT,
              "REVOKE INVALIDACTION ON resourceType1 resourceName1 FROM ROLE invalidRole"
                  + " ON COMPONENT " + COMPONENT);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }

        // test the audit log for delete role from group, success
        client.deleteRoleToGroups(requestorUserName, roleName, COMPONENT,
            Sets.newHashSet(testGroupName));
        fieldValueMap.clear();
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DELETE_ROLE);
        //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ROLE " + roleName
            + " FROM GROUP " + testGroupName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
        fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
        assertAuditLog(fieldValueMap);
        // test the audit log for delete role from group, failed
        try {
          client.deleteRoleToGroups(requestorUserName, "invalidRole", COMPONENT,
              Sets.newHashSet(testGroupName));
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DELETE_ROLE);
          //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT,
              "REVOKE ROLE invalidRole FROM GROUP " + testGroupName);
          fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
          fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
          assertAuditLog(fieldValueMap);
        }
        // test the audit log for drop role, success
        client.dropRole(requestorUserName, roleName, COMPONENT);
        fieldValueMap.clear();
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DROP_ROLE);
        //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
        fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "DROP ROLE " + roleName);
        fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
        fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
        assertAuditLog(fieldValueMap);
        // test the audit log for drop role, failed
        try {
          client.dropRole(requestorUserName, roleName, COMPONENT);
          fail("Exception should have been thrown");
        } catch (Exception e) {
          fieldValueMap.clear();
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DROP_ROLE);
          //fieldValueMap.put(Constants.LOG_FIELD_COMPONENT, COMPONENT);
          fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "DROP ROLE " + roleName);
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
