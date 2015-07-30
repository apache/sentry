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

package org.apache.sentry.binding.hive;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.provider.common.PolicyFileConstants;
import org.apache.sentry.provider.common.ProviderConstants;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestSentryIniPolicyFileFormatter {

  private static final String RESOURCE_PATH = "testImportExportPolicy.ini";
  // define the privileges
  public static String PRIVILIEGE1 = "server=server1";
  public static String PRIVILIEGE2 = "server=server1->action=select->grantoption=false";
  public static String PRIVILIEGE3 = "server=server1->db=db2->action=insert->grantoption=true";
  public static String PRIVILIEGE4 = "server=server1->db=db1->table=tbl1->action=insert";
  public static String PRIVILIEGE5 = "server=server1->db=db1->table=tbl2->column=col1->action=insert";
  public static String PRIVILIEGE6 = "server=server1->db=db1->table=tbl3->column=col1->action=*->grantoption=true";
  public static String PRIVILIEGE7 = "server=server1->db=db1->table=tbl4->column=col1->action=all->grantoption=true";
  public static String PRIVILIEGE8 = "server=server1->uri=hdfs://testserver:9999/path2->action=insert";

  private Map<String, Map<String, Set<String>>> policyFileMappingData1;
  private Map<String, Map<String, Set<String>>> policyFileMappingData2;
  private Map<String, Map<String, Set<String>>> policyFileMappingData3;
  private Map<String, Map<String, Set<String>>> policyFileMappingData4;
  private Map<String, Map<String, Set<String>>> policyFileMappingData5;

  private void prepareTestData() {
    // test data for:
    // [groups]
    // group1=role1,role2,role3
    // group2=role1,role2,role3
    // group3=role1,role2,role3
    // [roles]
    // role1=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
    // role2=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
    // role3=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
    policyFileMappingData1 = Maps.newHashMap();
    Map<String, Set<String>> groupRolesMap = Maps.newHashMap();
    Map<String, Set<String>> rolePrivilegesMap = Maps.newHashMap();
    Set<String> roles = Sets.newHashSet("role1", "role2", "role3");
    groupRolesMap.put("group1", roles);
    groupRolesMap.put("group2", roles);
    groupRolesMap.put("group3", roles);
    for (String roleName : roles) {
      rolePrivilegesMap.put(roleName, Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3,
          PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
    }
    policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

    // test data for:
    // [groups]
    // group1=role1
    // group2=role2
    // group3=role3
    // [roles]
    // role1=privilege1,privilege2,privilege3
    // role2=privilege4,privilege5,privilege6
    // role3=privilege7,privilege8
    policyFileMappingData2 = Maps.newHashMap();
    groupRolesMap = Maps.newHashMap();
    rolePrivilegesMap = Maps.newHashMap();
    groupRolesMap.put("group1", Sets.newHashSet("role1"));
    groupRolesMap.put("group2", Sets.newHashSet("role2"));
    groupRolesMap.put("group3", Sets.newHashSet("role3"));
    rolePrivilegesMap.put("role1", Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3));
    rolePrivilegesMap.put("role2", Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6));
    rolePrivilegesMap.put("role3", Sets.newHashSet(PRIVILIEGE7, PRIVILIEGE8));
    policyFileMappingData2.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData2.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

    // test data for:
    // [groups]
    // group1=role1,role2
    // group2=role1,role2,role3
    // group3=role2,role3
    // [roles]
    // role1=privilege1,privilege2,privilege3,privilege4
    // role2=privilege3,privilege4,privilege5,privilege6
    // role3=privilege5,privilege6,privilege7,privilege8
    policyFileMappingData3 = Maps.newHashMap();
    groupRolesMap = Maps.newHashMap();
    rolePrivilegesMap = Maps.newHashMap();
    groupRolesMap.put("group1", Sets.newHashSet("role1", "role2"));
    groupRolesMap.put("group2", Sets.newHashSet("role1", "role2", "role3"));
    groupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    rolePrivilegesMap.put("role1",
        Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4));
    rolePrivilegesMap.put("role2",
        Sets.newHashSet(PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6));
    rolePrivilegesMap.put("role3",
        Sets.newHashSet(PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
    policyFileMappingData3.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData3.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

    // test data for groups only
    policyFileMappingData4 = Maps.newHashMap();
    groupRolesMap = Maps.newHashMap();
    rolePrivilegesMap = Maps.newHashMap();
    groupRolesMap.put("group1", Sets.newHashSet("role1", "role2"));
    policyFileMappingData4.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData4.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

    // test empty data
    policyFileMappingData5 = Maps.newHashMap();
    groupRolesMap = Maps.newHashMap();
    rolePrivilegesMap = Maps.newHashMap();
    policyFileMappingData5.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData5.put(PolicyFileConstants.ROLES, rolePrivilegesMap);
  }

  @Test
  public void testImportExport() throws Exception {
    prepareTestData();
    File baseDir = Files.createTempDir();
    String resourcePath = (new File(baseDir, RESOURCE_PATH)).getAbsolutePath();
    HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
    SentryIniPolicyFileFormatter iniFormatter = new SentryIniPolicyFileFormatter();

    // test data1
    iniFormatter.write(resourcePath, policyFileMappingData1);
    Map<String, Map<String, Set<String>>> parsedMappingData = iniFormatter.parse(resourcePath,
        authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData1);

    // test data2
    iniFormatter.write(resourcePath, policyFileMappingData2);
    parsedMappingData = iniFormatter.parse(resourcePath, authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData2);

    // test data3
    iniFormatter.write(resourcePath, policyFileMappingData3);
    parsedMappingData = iniFormatter.parse(resourcePath, authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData3);

    // test data4
    iniFormatter.write(resourcePath, policyFileMappingData4);
    parsedMappingData = iniFormatter.parse(resourcePath, authzConf);
    assertTrue(parsedMappingData.get(PolicyFileConstants.GROUPS).isEmpty());
    assertTrue(parsedMappingData.get(PolicyFileConstants.ROLES).isEmpty());

    // test data5
    iniFormatter.write(resourcePath, policyFileMappingData5);
    parsedMappingData = iniFormatter.parse(resourcePath, authzConf);
    assertTrue(parsedMappingData.get(PolicyFileConstants.GROUPS).isEmpty());
    assertTrue(parsedMappingData.get(PolicyFileConstants.ROLES).isEmpty());
    (new File(baseDir, RESOURCE_PATH)).delete();
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
