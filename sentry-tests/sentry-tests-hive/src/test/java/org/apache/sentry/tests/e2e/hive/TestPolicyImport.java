/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.authz.SentryConfigTool;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPolicyImport extends AbstractTestWithStaticConfiguration {

  private static String prefix;
  private PolicyFile policyFile;
  private SentryConfigTool configTool;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception{
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Before
  public void setup() throws Exception {
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
    policyFile.addGroupsToUser("hive", ADMINGROUP);
    policyFile.addGroupsToUser(ADMIN1, ADMINGROUP);

    configTool = new SentryConfigTool();
    String hiveServer2 = System.getProperty("sentry.e2etest.hiveServer2Type",
        "InternalHiveServer2");
    String policyOnHDFS = System.getProperty(
        "sentry.e2etest.hive.policyOnHDFS", "true");
    if (policyOnHDFS.trim().equalsIgnoreCase("true")
        && (hiveServer2.equals("UnmanagedHiveServer2"))) {
      String policyLocation = System.getProperty(
          "sentry.e2etest.hive.policy.location", "/user/hive/sentry");
      prefix = "hdfs://" + policyLocation + "/";
    } else {
      prefix = "file://" + context.getPolicyFile().getParent() + "/";
    }

  }

    @Test
  public void testImportPolicy() throws Exception {
    policyFile.addRolesToGroup("analyst", "analyst_role", "customers_select_role");
    policyFile.addRolesToGroup("jranalyst", "junior_analyst_role");
    policyFile.addRolesToGroup("manager", "analyst_role",  "junior_analyst_role",
        "customers_insert_role", "customers_select_role");
    policyFile.addRolesToGroup("customers_admin", "customers_admin_role");

    policyFile.addPermissionsToRole("analyst_role", "server=server1->db=analyst_db",
        "server=server1->db=jranalyst_db->table=*->action=select");
    policyFile.addPermissionsToRole("junior_analyst_role", "server=server1->db=jranalyst_db");
    policyFile.addPermissionsToRole("customers_admin_role", "server=server1->db=customers");
    policyFile.addPermissionsToRole("customers_insert_role", "server=server1->db=customers->table=*->action=insert");
    policyFile.addPermissionsToRole("customers_select_role", "server=server1->db=customers->table=*->action=select");

    policyFile.write(context.getPolicyFile());

    configTool.setImportPolicy(true);
    configTool.setPolicyFile(context.getPolicyFile().getPath());
    configTool.setupConfig();

    configTool.importPolicy();

    SentryPolicyServiceClient client = new SentryPolicyServiceClient(configTool.getAuthzConf());
    verifyRoles(client, "analyst", "analyst_role", "customers_select_role");
    verifyRoles(client, "jranalyst", "junior_analyst_role");
    verifyRoles(client, "manager", "analyst_role", "junior_analyst_role",
        "customers_insert_role", "customers_select_role");
    verifyRoles(client, "customers_admin", "customers_admin_role");

    verifyPrivileges(client, "analyst_role",
        createPrivilege(AccessConstants.ALL, "analyst_db", null, null),
        createPrivilege(AccessConstants.SELECT, "jranalyst_db", null, null));
    verifyPrivileges(client, "junior_analyst_role",
        createPrivilege(AccessConstants.ALL, "jranalyst_db", null, null));
    verifyPrivileges(client, "customers_admin_role",
        createPrivilege(AccessConstants.ALL, "customers", null, null));
    verifyPrivileges(client, "customers_insert_role",
        createPrivilege(AccessConstants.INSERT, "customers", null, null));
    verifyPrivileges(client, "customers_select_role",
        createPrivilege(AccessConstants.SELECT, "customers", null, null));
  }

  private void verifyRoles(SentryPolicyServiceClient client, String group, String ... roles) throws SentryUserException {
    Set<String> expectedRoles = new HashSet<String>(Arrays.asList(roles));
    Set<String> actualRoles = new HashSet<String>();

    Set<TSentryRole> groupRoles = client.listRolesByGroupName("hive", group);
    for (TSentryRole role : groupRoles) {
      actualRoles.add(role.getRoleName());
    }

    assertEquals("Expected roles don't match.", expectedRoles, actualRoles);
  }

  private void verifyPrivileges(SentryPolicyServiceClient client, String role, TSentryPrivilege ... privileges) throws SentryUserException {
    Set<TSentryPrivilege> expectedPrivileges = new HashSet<TSentryPrivilege>(Arrays.asList(privileges));
    Set<TSentryPrivilege> actualPrivileges = client.listAllPrivilegesByRoleName("hive", role);
    for (TSentryPrivilege privilege : actualPrivileges) {
      privilege.unsetCreateTime();
      privilege.unsetGrantorPrincipal();
    }

    assertEquals("Expected privileges don't match.", expectedPrivileges, actualPrivileges);
  }

  private TSentryPrivilege createPrivilege(String action, String dbName, String tableName, String uri) {
    String scope = "SERVER";
    if (uri != null) {
      scope = "URI";
    } else if (dbName != null) {
      if (tableName != null) {
        scope = "TABLE";
      } else  {
        scope = "DATABASE";
      }
    }

    TSentryPrivilege privilege = new TSentryPrivilege(scope, "server1", action);
    if (dbName != null) {
      privilege.setDbName(dbName);
    }

    if (tableName != null) {
      privilege.setDbName(tableName);
    }

    if (uri != null) {
      privilege.setURI(uri);
    }

    return privilege;
  }

}
