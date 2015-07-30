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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Set;

import org.apache.sentry.binding.hive.SentryPolicyFileFormatFactory;
import org.apache.sentry.binding.hive.SentryPolicyFileFormatter;
import org.apache.sentry.binding.hive.authz.SentryConfigTool;
import org.apache.sentry.provider.common.PolicyFileConstants;
import org.apache.sentry.provider.common.ProviderConstants;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;

public class TestPolicyImportExport extends AbstractTestWithStaticConfiguration {

  // resources/testPolicyImport.ini is used for the import test and all the following
  // privileges(PRIVILIEGE1...8) are defined the same as in testPolicyImport.ini, used for verifying
  // the test result.
  public static String PRIVILIEGE1 = "server=server1";
  public static String PRIVILIEGE2 = "server=server1->action=select->grantoption=false";
  public static String PRIVILIEGE3 = "server=server1->db=db2->action=insert->grantoption=true";
  public static String PRIVILIEGE4 = "server=server1->db=db1->table=tbl1->action=insert";
  public static String PRIVILIEGE5 = "server=server1->db=db1->table=tbl2->column=col1->action=insert";
  public static String PRIVILIEGE6 = "server=server1->db=db1->table=tbl3->column=col1->action=*->grantoption=true";
  public static String PRIVILIEGE7 = "server=server1->db=db1->table=tbl4->column=col1->action=all->grantoption=true";
  public static String PRIVILIEGE8 = "server=server1->uri=hdfs://testserver:9999/path2->action=insert";

  private SentryConfigTool configTool;
  private Map<String, Map<String, Set<String>>> policyFileMappingData;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception{
    useSentryService = true;
    // add current user to admin group to get the permission for import/export
    String requestorUserName = System.getProperty("user.name", "");
    StaticUserGroup.getStaticMapping().put(requestorUserName, ADMINGROUP);
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Before
  public void setup() throws Exception {
    configTool = new SentryConfigTool();
    configTool.setPolicyFile(context.getPolicyFile().getPath());
    configTool.setupConfig();
    importAdminPrivilege();
  }

  private void importAdminPrivilege() throws Exception {
    prepareForImport("testPolicyImportAdmin.ini");
    configTool.importPolicy();
  }

  private void prepareExceptedData() {
    // test data for:
    // [groups]
    // group1=roleImport1,roleImport2
    // group2=roleImport1,roleImport2,roleImport3
    // group3=roleImport2,roleImport3
    // [roles]
    // roleImport1=privilege1,privilege2,privilege3,privilege4
    // roleImport2=privilege3,privilege4,privilege5,privilege6
    // roleImport3=privilege5,privilege6,privilege7,privilege8
    policyFileMappingData = Maps.newHashMap();
    Map<String, Set<String>> groupRolesMap = Maps.newHashMap();
    Map<String, Set<String>> rolePrivilegesMap = Maps.newHashMap();
    groupRolesMap.put("group1", Sets.newHashSet("roleimport1", "roleimport2"));
    groupRolesMap.put("group2", Sets.newHashSet("roleimport1", "roleimport2", "roleimport3"));
    groupRolesMap.put("group3", Sets.newHashSet("roleimport2", "roleimport3"));
    // the adminrole is defined in testPolicyImportAdmin.ini
    groupRolesMap.put("admin", Sets.newHashSet("adminrole"));
    rolePrivilegesMap.put("roleimport1",
        Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4));
    rolePrivilegesMap.put("roleimport2",
        Sets.newHashSet(PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6));
    rolePrivilegesMap.put("roleimport3",
        Sets.newHashSet(PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
    // the adminrole is defined in testPolicyImportAdmin.ini
    rolePrivilegesMap.put("adminrole", Sets.newHashSet(PRIVILIEGE1));
    policyFileMappingData.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

  }

  @Test
  public void testImportExportPolicy() throws Exception {
    String importFileName = "testPolicyImport.ini";
    String exportFileName = "testPolicyExport.ini";
    File importFile = new File(dataDir, importFileName);
    File exportFile = new File(dataDir, exportFileName);
    FileOutputStream to = new FileOutputStream(importFile);
    Resources.copy(Resources.getResource(importFileName), to);
    to.close();
    configTool.setImportPolicyFilePath(importFile.getAbsolutePath());
    configTool.importPolicy();

    configTool.setExportPolicyFilePath(exportFile.getAbsolutePath());
    configTool.exportPolicy();

    SentryPolicyFileFormatter sentryPolicyFileFormatter = SentryPolicyFileFormatFactory
        .createFileFormatter(configTool.getAuthzConf());
    Map<String, Map<String, Set<String>>> exportMappingData = sentryPolicyFileFormatter.parse(
        exportFile.getAbsolutePath(), configTool.getAuthzConf());

    prepareExceptedData();
    validateSentryMappingData(exportMappingData, policyFileMappingData);
  }

  @Test
  public void testImportExportPolicyForError() throws Exception {
    prepareForImport("testPolicyImportError.ini");
    try {
      configTool.importPolicy();
      fail("IllegalArgumentException should be thrown for: Invalid key value: server [server]");
    } catch (IllegalArgumentException ex) {
      // ignore
    }
  }

  private void prepareForImport(String resorceName) throws Exception {
    File importFile = new File(dataDir, resorceName);
    FileOutputStream to = new FileOutputStream(importFile);
    Resources.copy(Resources.getResource(resorceName), to);
    to.close();
    configTool.setImportPolicyFilePath(importFile.getAbsolutePath());
  }

  // verify the mapping data
  public void validateSentryMappingData(Map<String, Map<String, Set<String>>> actualMappingData,
      Map<String, Map<String, Set<String>>> expectedMappingData) {
    validateGroupRolesMap(actualMappingData.get(PolicyFileConstants.GROUPS),
        expectedMappingData.get(PolicyFileConstants.GROUPS));
    validateRolePrivilegesMap(actualMappingData.get(PolicyFileConstants.ROLES),
        expectedMappingData.get(PolicyFileConstants.ROLES));
  }

  // verify the mapping data for [group,role]
  private void validateGroupRolesMap(Map<String, Set<String>> actualMap,
      Map<String, Set<String>> expectedMap) {
    assertEquals(expectedMap.keySet().size(), actualMap.keySet().size());
    for (String groupName : actualMap.keySet()) {
      Set<String> actualRoles = actualMap.get(groupName);
      Set<String> expectedRoles = expectedMap.get(groupName);
      assertEquals(actualRoles.size(), expectedRoles.size());
      assertTrue(actualRoles.equals(expectedRoles));
    }
  }

  // verify the mapping data for [role,privilege]
  private void validateRolePrivilegesMap(Map<String, Set<String>> actualMap,
      Map<String, Set<String>> expectedMap) {
    assertEquals(expectedMap.keySet().size(), actualMap.keySet().size());
    for (String roleName : actualMap.keySet()) {
      Set<String> actualPrivileges = actualMap.get(roleName);
      Set<String> exceptedPrivileges = expectedMap.get(roleName);
      assertEquals(exceptedPrivileges.size(), actualPrivileges.size());
      for (String actualPrivilege : actualPrivileges) {
        boolean isFound = exceptedPrivileges.contains(actualPrivilege);
        if (!isFound) {
          String withOptionPrivilege = ProviderConstants.AUTHORIZABLE_JOINER.join(actualPrivilege,
              ProviderConstants.KV_JOINER.join(PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME,
                  "false"));
          isFound = exceptedPrivileges.contains(withOptionPrivilege);
        }
        assertTrue(isFound);
      }
    }
  }
}
