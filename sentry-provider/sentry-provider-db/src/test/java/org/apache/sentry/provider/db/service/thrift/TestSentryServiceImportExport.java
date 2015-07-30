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

package org.apache.sentry.provider.db.service.thrift;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Set;

import org.apache.sentry.provider.common.PolicyFileConstants;
import org.apache.sentry.provider.common.ProviderConstants;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TestSentryServiceImportExport extends SentryServiceIntegrationBase {

  // define the privileges
  public static String PRIVILIEGE1 = "server=server1";
  public static String PRIVILIEGE2 = "server=server1->action=select->grantoption=false";
  public static String PRIVILIEGE3 = "server=server1->db=db2->action=insert->grantoption=true";
  public static String PRIVILIEGE4 = "server=server1->db=db1->table=tbl1->action=insert";
  public static String PRIVILIEGE5 = "server=server1->db=db1->table=tbl2->column=col1->action=insert";
  public static String PRIVILIEGE6 = "server=server1->db=db1->table=tbl3->column=col1->action=*->grantoption=true";
  public static String PRIVILIEGE7 = "server=server1->db=db1->table=tbl4->column=col1->action=all->grantoption=true";
  public static String PRIVILIEGE8 = "server=server1->uri=hdfs://testserver:9999/path2->action=insert";

  @BeforeClass
  public static void setup() throws Exception {
    kerberos = false;
    setupConf();
    startSentryService();
  }

  @Before
  public void preparePolicyFile() throws Exception {
    super.before();
    String requestorUserName = ADMIN_USER;
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
    writePolicyFile();
  }

  // Befor import, database is empty.
  // The following information is imported:
  // group1=role1,role2,role3
  // group2=role1,role2,role3
  // group3=role1,role2,role3
  // role1=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
  // role2=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
  // role3=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
  // Both import API importPolicy and export API exportPoicy are tested.
  @Test
  public void testImportExportPolicy1() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        Map<String, Map<String, Set<String>>> policyFileMappingData = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap = Maps.newHashMap();
        Set<String> roles = Sets.newHashSet("role1", "role2", "role3");
        groupRolesMap.put("group1", roles);
        groupRolesMap.put("group2", roles);
        groupRolesMap.put("group3", roles);
        Map<String, Set<String>> rolePrivilegesMap = Maps.newHashMap();
        for (String roleName : roles) {
          rolePrivilegesMap.put(roleName, Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3,
              PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        }
        policyFileMappingData.put(PolicyFileConstants.GROUPS, groupRolesMap);
        policyFileMappingData.put(PolicyFileConstants.ROLES, rolePrivilegesMap);
        client.importPolicy(policyFileMappingData, ADMIN_USER, false);

        Map<String, Map<String, Set<String>>> sentryMappingData = client.exportPolicy(ADMIN_USER);
        validateSentryMappingData(sentryMappingData,
            policyFileMappingData);
      }
    });
  }

  // call import twice, and there has no duplicate data:
  // The data for 1st import:
  // group1=role1
  // role1=privilege1,privilege2,privilege3,privilege4
  // The data for 2nd import:
  // group2=role2,role3
  // group3=role2,role3
  // role2=privilege5,privilege6,privilege7,privilege8
  // role3=privilege5,privilege6,privilege7,privilege8
  // Both import API importPolicy and export API exportPoicy are tested.
  @Test
  public void testImportExportPolicy2() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        Map<String, Map<String, Set<String>>> policyFileMappingData1 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap1 = Maps.newHashMap();
        groupRolesMap1.put("group1", Sets.newHashSet("role1"));
        Map<String, Set<String>> rolePrivilegesMap1 = Maps.newHashMap();
        rolePrivilegesMap1.put("role1",
            Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4));
        policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap1);
        policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap1);
        client.importPolicy(policyFileMappingData1, ADMIN_USER, false);

        Map<String, Map<String, Set<String>>> policyFileMappingData2 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap2 = Maps.newHashMap();
        groupRolesMap2.put("group2", Sets.newHashSet("role2", "role3"));
        groupRolesMap2.put("group3", Sets.newHashSet("role2", "role3"));
        Map<String, Set<String>> rolePrivilegesMap2 = Maps.newHashMap();
        rolePrivilegesMap2.put("role2",
            Sets.newHashSet(PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        rolePrivilegesMap2.put("role3",
            Sets.newHashSet(PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        policyFileMappingData2.put(PolicyFileConstants.GROUPS, groupRolesMap2);
        policyFileMappingData2.put(PolicyFileConstants.ROLES, rolePrivilegesMap2);
        client.importPolicy(policyFileMappingData2, ADMIN_USER, false);

        Map<String, Map<String, Set<String>>> exceptedMappingData = Maps.newHashMap();
        // for exceptedMappingData, combine policyFileMappingData1 and policyFileMappingData2
        exceptedMappingData.put(PolicyFileConstants.GROUPS,
            policyFileMappingData1.get(PolicyFileConstants.GROUPS));
        exceptedMappingData.get(PolicyFileConstants.GROUPS).putAll(
            policyFileMappingData2.get(PolicyFileConstants.GROUPS));
        exceptedMappingData.put(PolicyFileConstants.ROLES,
            policyFileMappingData1.get(PolicyFileConstants.ROLES));
        exceptedMappingData.get(PolicyFileConstants.ROLES).putAll(
            policyFileMappingData2.get(PolicyFileConstants.ROLES));

        Map<String, Map<String, Set<String>>> sentryMappingData = client.exportPolicy(ADMIN_USER);
        validateSentryMappingData(sentryMappingData, exceptedMappingData);
      }
    });
  }

  // Call import twice, and there has overlapping groups
  // The data for 1st import:
  // group1=role1, role2
  // group2=role1, role2
  // group3=role1, role2
  // role1=privilege1,privilege2,privilege3,privilege4,privilege5
  // role2=privilege1,privilege2,privilege3,privilege4,privilege5
  // The data for 2nd import:
  // group1=role2,role3
  // group2=role2,role3
  // group3=role2,role3
  // role2=privilege4,privilege5,privilege6,privilege7,privilege8
  // role3=privilege4,privilege5,privilege6,privilege7,privilege8
  // Both import API importPolicy and export API exportPoicy are tested.
  @Test
  public void testImportExportPolicy3() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        Map<String, Map<String, Set<String>>> policyFileMappingData1 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap1 = Maps.newHashMap();
        groupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
        groupRolesMap1.put("group2", Sets.newHashSet("role1", "role2"));
        groupRolesMap1.put("group3", Sets.newHashSet("role1", "role2"));
        Map<String, Set<String>> rolePrivilegesMap1 = Maps.newHashMap();
        rolePrivilegesMap1.put("role1",
            Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5));
        rolePrivilegesMap1.put("role2",
            Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5));
        policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap1);
        policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap1);
        client.importPolicy(policyFileMappingData1, ADMIN_USER, false);

        Map<String, Map<String, Set<String>>> policyFileMappingData2 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap2 = Maps.newHashMap();
        groupRolesMap2.put("group1", Sets.newHashSet("role2", "role3"));
        groupRolesMap2.put("group2", Sets.newHashSet("role2", "role3"));
        groupRolesMap2.put("group3", Sets.newHashSet("role2", "role3"));
        Map<String, Set<String>> rolePrivilegesMap2 = Maps.newHashMap();
        rolePrivilegesMap2.put("role2",
            Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        rolePrivilegesMap2.put("role3",
            Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        policyFileMappingData2.put(PolicyFileConstants.GROUPS, groupRolesMap2);
        policyFileMappingData2.put(PolicyFileConstants.ROLES, rolePrivilegesMap2);
        client.importPolicy(policyFileMappingData2, ADMIN_USER, false);

        Map<String, Map<String, Set<String>>> exceptedMappingData = Maps.newHashMap();
        Map<String, Set<String>> exceptedRolesMap = Maps.newHashMap();
        exceptedRolesMap.put("group1", Sets.newHashSet("role1", "role2", "role3"));
        exceptedRolesMap.put("group2", Sets.newHashSet("role1", "role2", "role3"));
        exceptedRolesMap.put("group3", Sets.newHashSet("role1", "role2", "role3"));
        Map<String, Set<String>> exceptedPrivilegesMap = Maps.newHashMap();
        exceptedPrivilegesMap.put("role1",
            Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5));
        exceptedPrivilegesMap.put("role2", Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3,
            PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        exceptedPrivilegesMap.put("role3",
            Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        exceptedMappingData.put(PolicyFileConstants.GROUPS, exceptedRolesMap);
        exceptedMappingData.put(PolicyFileConstants.ROLES, exceptedPrivilegesMap);

        Map<String, Map<String, Set<String>>> sentryMappingData = client.exportPolicy(ADMIN_USER);
        validateSentryMappingData(sentryMappingData, exceptedMappingData);
      }
    });
  }

  // Only mapping data for [group,role] is imported:
  // group1=role1,role2
  @Test
  public void testImportExportPolicy4() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        Map<String, Map<String, Set<String>>> policyFileMappingData = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap = Maps.newHashMap();
        Set<String> roles = Sets.newHashSet("role1", "role2");
        groupRolesMap.put("group1", roles);
        Map<String, Set<String>> rolePrivilegesMap = Maps.newHashMap();
        policyFileMappingData.put(PolicyFileConstants.GROUPS, groupRolesMap);
        policyFileMappingData.put(PolicyFileConstants.ROLES, rolePrivilegesMap);
        client.importPolicy(policyFileMappingData, ADMIN_USER, false);

        Map<String, Map<String, Set<String>>> sentryMappingData = client.exportPolicy(ADMIN_USER);
        validateSentryMappingData(sentryMappingData,
            policyFileMappingData);
      }
    });
  }

  // call import twice, and there has no duplicate data, the import will be with the overwrite mode:
  // The data for 1st import:
  // group1=role1
  // role1=privilege1
  // The data for 2nd import:
  // group2=role2,role3
  // group3=role2,role3
  // role2=privilege2
  // role3=privilege2
  // Both import API importSentryMetaData and export APIs getRolesMap, getGroupsMap,
  // getPrivilegesList are tested.
  @Test
  public void testImportExportPolicy5() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        Map<String, Map<String, Set<String>>> policyFileMappingData1 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap1 = Maps.newHashMap();
        groupRolesMap1.put("group1", Sets.newHashSet("role1"));
        Map<String, Set<String>> rolePrivilegesMap1 = Maps.newHashMap();
        rolePrivilegesMap1.put("role1", Sets.newHashSet(PRIVILIEGE1));
        policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap1);
        policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap1);
        client.importPolicy(policyFileMappingData1, ADMIN_USER, true);

        Map<String, Map<String, Set<String>>> policyFileMappingData2 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap2 = Maps.newHashMap();
        groupRolesMap2.put("group2", Sets.newHashSet("role2", "role3"));
        groupRolesMap2.put("group3", Sets.newHashSet("role2", "role3"));
        Map<String, Set<String>> rolePrivilegesMap2 = Maps.newHashMap();
        rolePrivilegesMap2.put("role2", Sets.newHashSet(PRIVILIEGE2));
        rolePrivilegesMap2.put("role3", Sets.newHashSet(PRIVILIEGE2));
        policyFileMappingData2.put(PolicyFileConstants.GROUPS, groupRolesMap2);
        policyFileMappingData2.put(PolicyFileConstants.ROLES, rolePrivilegesMap2);
        client.importPolicy(policyFileMappingData2, ADMIN_USER, true);

        Map<String, Map<String, Set<String>>> exceptedMappingData = Maps.newHashMap();
        Map<String, Set<String>> exceptedRolesMap = Maps.newHashMap();
        exceptedRolesMap.put("group1", Sets.newHashSet("role1"));
        exceptedRolesMap.put("group2", Sets.newHashSet("role2", "role3"));
        exceptedRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
        Map<String, Set<String>> exceptedPrivilegesMap = Maps.newHashMap();
        exceptedPrivilegesMap.put("role1", Sets.newHashSet(PRIVILIEGE1));
        exceptedPrivilegesMap.put("role2", Sets.newHashSet(PRIVILIEGE2));
        exceptedPrivilegesMap.put("role3", Sets.newHashSet(PRIVILIEGE2));
        exceptedMappingData.put(PolicyFileConstants.GROUPS, exceptedRolesMap);
        exceptedMappingData.put(PolicyFileConstants.ROLES, exceptedPrivilegesMap);

        Map<String, Map<String, Set<String>>> sentryMappingData = client.exportPolicy(ADMIN_USER);
        validateSentryMappingData(sentryMappingData, exceptedMappingData);
      }
    });
  }

  // call import twice, and there has data overlap, the import will be with the overwrite mode:
  // The data for 1st import:
  // group1=role1, role2
  // group2=role1, role2
  // group3=role1, role2
  // role1=privilege1,privilege2,privilege3,privilege4,privilege5
  // role2=privilege1,privilege2,privilege3,privilege4,privilege5
  // The data for 2nd import:
  // group1=role2,role3
  // group2=role2,role3
  // group3=role2,role3
  // role2=privilege4,privilege5,privilege6,privilege7,privilege8
  // role3=privilege4,privilege5,privilege6,privilege7,privilege8
  // Both import API importSentryMetaData and export APIs getRolesMap, getGroupsMap,
  // getPrivilegesList are tested.
  @Test
  public void testImportExportPolicy6() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        Map<String, Map<String, Set<String>>> policyFileMappingData1 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap1 = Maps.newHashMap();
        groupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
        groupRolesMap1.put("group2", Sets.newHashSet("role1", "role2"));
        groupRolesMap1.put("group3", Sets.newHashSet("role1", "role2"));
        Map<String, Set<String>> rolePrivilegesMap1 = Maps.newHashMap();
        rolePrivilegesMap1.put("role1",
            Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5));
        rolePrivilegesMap1.put("role2",
            Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5));
        policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap1);
        policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap1);
        client.importPolicy(policyFileMappingData1, ADMIN_USER, true);

        Map<String, Map<String, Set<String>>> policyFileMappingData2 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap2 = Maps.newHashMap();
        groupRolesMap2.put("group1", Sets.newHashSet("role2", "role3"));
        groupRolesMap2.put("group2", Sets.newHashSet("role2", "role3"));
        groupRolesMap2.put("group3", Sets.newHashSet("role2", "role3"));
        Map<String, Set<String>> rolePrivilegesMap2 = Maps.newHashMap();
        rolePrivilegesMap2.put("role2",
            Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        rolePrivilegesMap2.put("role3",
            Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        policyFileMappingData2.put(PolicyFileConstants.GROUPS, groupRolesMap2);
        policyFileMappingData2.put(PolicyFileConstants.ROLES, rolePrivilegesMap2);
        client.importPolicy(policyFileMappingData2, ADMIN_USER, true);

        Map<String, Map<String, Set<String>>> exceptedMappingData = Maps.newHashMap();
        Map<String, Set<String>> exceptedRolesMap = Maps.newHashMap();
        exceptedRolesMap.put("group1", Sets.newHashSet("role1", "role2", "role3"));
        exceptedRolesMap.put("group2", Sets.newHashSet("role1", "role2", "role3"));
        exceptedRolesMap.put("group3", Sets.newHashSet("role1", "role2", "role3"));
        Map<String, Set<String>> exceptedPrivilegesMap = Maps.newHashMap();
        exceptedPrivilegesMap.put("role1",
            Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5));
        exceptedPrivilegesMap.put("role2",
            Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        exceptedPrivilegesMap.put("role3",
            Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
        exceptedMappingData.put(PolicyFileConstants.GROUPS, exceptedRolesMap);
        exceptedMappingData.put(PolicyFileConstants.ROLES, exceptedPrivilegesMap);

        Map<String, Map<String, Set<String>>> sentryMappingData = client.exportPolicy(ADMIN_USER);
        validateSentryMappingData(sentryMappingData, exceptedMappingData);
      }
    });
  }

  // test the import privileges with the action: All, *, select, insert
  // All and * should replace the select and insert
  // The data for import:
  // group1=role1, role2
  // role1=testPrivilege1,testPrivilege2,testPrivilege3,testPrivilege4
  // role2=testPrivilege5, testPrivilege6,testPrivilege7,testPrivilege8
  @Test
  public void testImportExportPolicy7() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String testPrivilege1 = "server=server1->db=db1->table=tbl1->action=select->grantoption=true";
        String testPrivilege2 = "server=server1->db=db1->table=tbl1->action=insert->grantoption=false";
        String testPrivilege3 = "server=server1->db=db1->table=tbl1->action=all->grantoption=true";
        String testPrivilege4 = "server=server1->db=db1->table=tbl1->action=insert->grantoption=true";
        String testPrivilege5 = "server=server1->db=db1->table=tbl2->action=select->grantoption=true";
        String testPrivilege6 = "server=server1->db=db1->table=tbl2->action=insert->grantoption=false";
        String testPrivilege7 = "server=server1->db=db1->table=tbl2->action=*->grantoption=true";
        String testPrivilege8 = "server=server1->db=db1->table=tbl2->action=insert->grantoption=true";

        Map<String, Map<String, Set<String>>> policyFileMappingData1 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap1 = Maps.newHashMap();
        groupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
        Map<String, Set<String>> rolePrivilegesMap1 = Maps.newHashMap();
        rolePrivilegesMap1.put("role1",
            Sets.newHashSet(testPrivilege1, testPrivilege2, testPrivilege3, testPrivilege4));
        rolePrivilegesMap1.put("role2",
            Sets.newHashSet(testPrivilege5, testPrivilege6, testPrivilege7, testPrivilege8));
        policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap1);
        policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap1);
        client.importPolicy(policyFileMappingData1, ADMIN_USER, true);

        Map<String, Map<String, Set<String>>> exceptedMappingData = Maps.newHashMap();
        Map<String, Set<String>> exceptedRolesMap = Maps.newHashMap();
        exceptedRolesMap.put("group1", Sets.newHashSet("role1", "role2"));
        Map<String, Set<String>> exceptedPrivilegesMap = Maps.newHashMap();
        exceptedPrivilegesMap.put("role1", Sets.newHashSet(testPrivilege2, testPrivilege3));
        exceptedPrivilegesMap.put("role2", Sets.newHashSet(testPrivilege6, testPrivilege7));
        exceptedMappingData.put(PolicyFileConstants.GROUPS, exceptedRolesMap);
        exceptedMappingData.put(PolicyFileConstants.ROLES, exceptedPrivilegesMap);

        Map<String, Map<String, Set<String>>> sentryMappingData = client.exportPolicy(ADMIN_USER);
        validateSentryMappingData(sentryMappingData, exceptedMappingData);
      }
    });
  }

  // Call import twice, and there has overlapping actions, all and * should replace the select and
  // insert
  // The data for 1st import:
  // group1=role1, role2
  // role1=privilege1(with select action),privilege2(with insert action)
  // role2=privilege4(with select action),privilege5(with insert action)
  // The data for 2nd import:
  // group1=role1, role2
  // role1=privilege3(with all action)
  // role2=privilege6(with * action)
  @Test
  public void testImportExportPolicy8() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String testPrivilege1 = "server=server1->db=db1->table=tbl1->action=select->grantoption=true";
        String testPrivilege2 = "server=server1->db=db1->table=tbl1->action=insert->grantoption=true";
        String testPrivilege3 = "server=server1->db=db1->table=tbl1->action=all->grantoption=true";
        String testPrivilege4 = "server=server1->db=db1->table=tbl2->action=select->grantoption=true";
        String testPrivilege5 = "server=server1->db=db1->table=tbl2->action=insert->grantoption=true";
        String testPrivilege6 = "server=server1->db=db1->table=tbl2->action=*->grantoption=true";

        Map<String, Map<String, Set<String>>> policyFileMappingData1 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap1 = Maps.newHashMap();
        groupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
        Map<String, Set<String>> rolePrivilegesMap1 = Maps.newHashMap();
        rolePrivilegesMap1.put("role1", Sets.newHashSet(testPrivilege1, testPrivilege2));
        rolePrivilegesMap1.put("role2", Sets.newHashSet(testPrivilege4, testPrivilege5));
        policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap1);
        policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap1);
        client.importPolicy(policyFileMappingData1, ADMIN_USER, false);

        Map<String, Map<String, Set<String>>> policyFileMappingData2 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap2 = Maps.newHashMap();
        groupRolesMap2.put("group1", Sets.newHashSet("role1", "role2"));
        Map<String, Set<String>> rolePrivilegesMap2 = Maps.newHashMap();
        rolePrivilegesMap2.put("role1", Sets.newHashSet(testPrivilege3));
        rolePrivilegesMap2.put("role2", Sets.newHashSet(testPrivilege6));
        policyFileMappingData2.put(PolicyFileConstants.GROUPS, groupRolesMap2);
        policyFileMappingData2.put(PolicyFileConstants.ROLES, rolePrivilegesMap2);
        client.importPolicy(policyFileMappingData2, ADMIN_USER, false);

        Map<String, Map<String, Set<String>>> exceptedMappingData = policyFileMappingData2;
        Map<String, Map<String, Set<String>>> sentryMappingData = client.exportPolicy(ADMIN_USER);
        // all and * should replace the select and insert
        validateSentryMappingData(sentryMappingData, exceptedMappingData);
      }
    });
  }

  // test the user not in the admin group can't do the import/export
  @Test
  public void testImportExportPolicy9() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        Map<String, Map<String, Set<String>>> policyFileMappingData1 = Maps.newHashMap();
        Map<String, Set<String>> groupRolesMap1 = Maps.newHashMap();
        Map<String, Set<String>> rolePrivilegesMap1 = Maps.newHashMap();
        policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap1);
        policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap1);
        try {
          client.importPolicy(policyFileMappingData1, "no-admin-user", false);
          fail("non-admin can't do the import.");
        } catch (Exception e) {
          // excepted exception
        }

        try {
          client.exportPolicy("no-admin-user");
          fail("non-admin can't do the export.");
        } catch (Exception e) {
          // excepted exception
        }
      }
    });
  }

  // verify the mapping data
  public void validateSentryMappingData(
      Map<String, Map<String, Set<String>>> actualMappingData,
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
