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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.model.MSentryUser;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.service.thrift.TSentryGrantOption;
import org.apache.sentry.api.service.thrift.TSentryMappingData;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.api.common.SentryServiceUtil;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestSentryStoreImportExport {

  private static File dataDir;
  private static String[] adminGroups = { "adminGroup1" };
  private static PolicyFile policyFile;
  private static File policyFilePath;
  private static SentryStore sentryStore;
  private TSentryPrivilege tSentryPrivilege1;
  private TSentryPrivilege tSentryPrivilege2;
  private TSentryPrivilege tSentryPrivilege3;
  private TSentryPrivilege tSentryPrivilege4;
  private TSentryPrivilege tSentryPrivilege5;
  private TSentryPrivilege tSentryPrivilege6;
  private TSentryPrivilege tSentryPrivilege7;
  private TSentryPrivilege tSentryPrivilege8;
  private TSentryPrivilege tSentryPrivilege9;

  @BeforeClass
  public static void setupEnv() throws Exception {
    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    Configuration conf = new Configuration(true);
    conf.set(ServerConfig.SENTRY_VERIFY_SCHEM_VERSION, "false");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL, "jdbc:derby:;databaseName=" + dataDir.getPath()
        + ";create=true");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_PASS, "sentry");
    conf.setStrings(ServerConfig.ADMIN_GROUPS, adminGroups);
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, ServerConfig.SENTRY_STORE_LOCAL_GROUP_MAPPING);
    policyFilePath = new File(dataDir, "local_policy_file.ini");
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING_RESOURCE, policyFilePath.getPath());
    policyFile = new PolicyFile();
    boolean hdfsSyncEnabled = SentryServiceUtil.isHDFSSyncEnabled(conf);
    sentryStore = new SentryStore(conf);
    sentryStore.setPersistUpdateDeltas(hdfsSyncEnabled);

    String adminUser = "g1";
    addGroupsToUser(adminUser, adminGroups);
    writePolicyFile();
  }

  @Before
  public void setupPrivilege() {
    preparePrivilege();
  }

  @After
  public void clearStore() {
    sentryStore.clearAllTables();
  }

  // create the privileges instance for test case:
  // privilege1=[server=server1]
  // privilege2=[server=server1, action=select, grantOption=false]
  // privilege3=[server=server1, db=db2, action=insert, grantOption=true]
  // privilege4=[server=server1, db=db1, table=tbl1, action=insert, grantOption=false]
  // privilege5=[server=server1, db=db1, table=tbl2, column=col1, action=insert, grantOption=false]
  // privilege6=[server=server1, db=db1, table=tbl3, column=col1, action=*, grantOption=true]
  // privilege7=[server=server1, db=db1, table=tbl4, column=col1, action=all, grantOption=true]
  // privilege8=[server=server1, uri=hdfs://testserver:9999/path1, action=insert, grantOption=false]
  // privilege9=[server=server1, db=db2, table=tbl1, action=insert, grantOption=false]
  private void preparePrivilege() {
    tSentryPrivilege1 = createTSentryPrivilege(PrivilegeScope.SERVER.name(), "server1", "", "", "",
        "", "", TSentryGrantOption.UNSET);
    tSentryPrivilege2 = createTSentryPrivilege(PrivilegeScope.SERVER.name(), "server1", "", "", "",
        "", AccessConstants.SELECT, TSentryGrantOption.FALSE);
    tSentryPrivilege3 = createTSentryPrivilege(PrivilegeScope.DATABASE.name(), "server1", "db2",
        "", "", "", AccessConstants.INSERT, TSentryGrantOption.TRUE);
    tSentryPrivilege4 = createTSentryPrivilege(PrivilegeScope.TABLE.name(), "server1", "db1",
        "tbl1", "", "", AccessConstants.INSERT, TSentryGrantOption.FALSE);
    tSentryPrivilege5 = createTSentryPrivilege(PrivilegeScope.COLUMN.name(), "server1", "db1",
        "tbl2", "col1", "", AccessConstants.INSERT, TSentryGrantOption.FALSE);
    tSentryPrivilege6 = createTSentryPrivilege(PrivilegeScope.COLUMN.name(), "server1", "db1",
        "tbl3", "col1", "", AccessConstants.ALL, TSentryGrantOption.TRUE);
    tSentryPrivilege7 = createTSentryPrivilege(PrivilegeScope.COLUMN.name(), "server1", "db1",
        "tbl4", "col1", "", AccessConstants.ACTION_ALL, TSentryGrantOption.TRUE);
    tSentryPrivilege8 = createTSentryPrivilege(PrivilegeScope.URI.name(), "server1", "", "", "",
        "hdfs://testserver:9999/path1", AccessConstants.INSERT, TSentryGrantOption.FALSE);
    tSentryPrivilege9 = createTSentryPrivilege(PrivilegeScope.TABLE.name(), "server1", "db2",
         "tbl1", "", "", AccessConstants.INSERT, TSentryGrantOption.FALSE);
  }

  @AfterClass
  public static void teardown() {
    if (sentryStore != null) {
      sentryStore.stop();
    }
    if (dataDir != null) {
      FileUtils.deleteQuietly(dataDir);
    }
  }

  protected static void addGroupsToUser(String user, String... groupNames) {
    policyFile.addGroupsToUser(user, groupNames);
  }

  protected static void writePolicyFile() throws Exception {
    policyFile.write(policyFilePath);
  }

  // Befor import, database is empty.
  // The following information is imported:
  // group1=role1,role2,role3
  // group2=role1,role2,role3
  // group3=role1,role2,role3
  // role1=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
  // role2=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
  // role3=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
  // Both import API importSentryMetaData and export APIs getRolesMap, getGroupsMap,
  // getPrivilegesList are tested.
  @Test
  public void testImportExportPolicy1() throws Exception {
    TSentryMappingData tSentryMappingData = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap = Maps.newHashMap();
    sentryGroupRolesMap.put("group1", Sets.newHashSet("Role1", "role2", "role3"));
    sentryGroupRolesMap.put("group2", Sets.newHashSet("Role1", "role2", "role3"));
    sentryGroupRolesMap.put("group3", Sets.newHashSet("Role1", "role2", "role3"));
    sentryRolePrivilegesMap.put("Role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    sentryRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    sentryRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    tSentryMappingData.setGroupRolesMap(sentryGroupRolesMap);
    tSentryMappingData.setRolePrivilegesMap(sentryRolePrivilegesMap);
    sentryStore.importSentryMetaData(tSentryMappingData, false);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2", "role3"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1", "group2", "group3"));

    // test the result data for the privilege
    verifyPrivileges(privilegesList, Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1", "role2", "role3"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2", "role3"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role1", "role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));

    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);
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
  // Both import API importSentryMetaData and export APIs getRolesMap, getGroupsMap,
  // getPrivilegesList are tested.
  @Test
  public void testImportExportPolicy2() throws Exception {
    TSentryMappingData tSentryMappingData1 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap1 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap1 = Maps.newHashMap();
    sentryGroupRolesMap1.put("group1", Sets.newHashSet("role1"));
    sentryRolePrivilegesMap1
        .put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2, tSentryPrivilege3,
        tSentryPrivilege4));
    tSentryMappingData1.setGroupRolesMap(sentryGroupRolesMap1);
    tSentryMappingData1.setRolePrivilegesMap(sentryRolePrivilegesMap1);
    sentryStore.importSentryMetaData(tSentryMappingData1, false);

    TSentryMappingData tSentryMappingData2 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap2 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap2 = Maps.newHashMap();
    sentryGroupRolesMap2.put("group2", Sets.newHashSet("role2", "role3"));
    sentryGroupRolesMap2.put("group3", Sets.newHashSet("role2", "role3"));
    sentryRolePrivilegesMap2
        .put("role2", Sets.newHashSet(tSentryPrivilege5, tSentryPrivilege6, tSentryPrivilege7,
        tSentryPrivilege8));
    sentryRolePrivilegesMap2
        .put("role3", Sets.newHashSet(tSentryPrivilege5, tSentryPrivilege6, tSentryPrivilege7,
        tSentryPrivilege8));
    tSentryMappingData2.setGroupRolesMap(sentryGroupRolesMap2);
    tSentryMappingData2.setRolePrivilegesMap(sentryRolePrivilegesMap2);
    sentryStore.importSentryMetaData(tSentryMappingData2, false);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2", "role3"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1", "group2", "group3"));

    // test the result data for the privilege
    verifyPrivileges(privilegesList, Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role2", "role3"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap
        .put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2, tSentryPrivilege3,
            tSentryPrivilege4));
    exceptedRolePrivilegesMap
        .put("role2", Sets.newHashSet(tSentryPrivilege5, tSentryPrivilege6, tSentryPrivilege7,
            tSentryPrivilege8));
    exceptedRolePrivilegesMap
        .put("role3", Sets.newHashSet(tSentryPrivilege5, tSentryPrivilege6, tSentryPrivilege7,
            tSentryPrivilege8));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);
  }

  // call import twice, and there has data overlap:
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
  public void testImportExportPolicy3() throws Exception {
    TSentryMappingData tSentryMappingData1 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap1 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap1 = Maps.newHashMap();
    sentryGroupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
    sentryGroupRolesMap1.put("group2", Sets.newHashSet("role1", "role2"));
    sentryGroupRolesMap1.put("group3", Sets.newHashSet("role1", "role2"));
    sentryRolePrivilegesMap1.put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5));
    sentryRolePrivilegesMap1.put("role2", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5));
    tSentryMappingData1.setGroupRolesMap(sentryGroupRolesMap1);
    tSentryMappingData1.setRolePrivilegesMap(sentryRolePrivilegesMap1);
    sentryStore.importSentryMetaData(tSentryMappingData1, false);

    TSentryMappingData tSentryMappingData2 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap2 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap2 = Maps.newHashMap();
    sentryGroupRolesMap2.put("group1", Sets.newHashSet("role2", "role3"));
    sentryGroupRolesMap2.put("group2", Sets.newHashSet("role2", "role3"));
    sentryGroupRolesMap2.put("group3", Sets.newHashSet("role2", "role3"));
    sentryRolePrivilegesMap2.put("role2", Sets.newHashSet(tSentryPrivilege4, tSentryPrivilege5,
        tSentryPrivilege6, tSentryPrivilege7, tSentryPrivilege8));
    sentryRolePrivilegesMap2.put("role3", Sets.newHashSet(tSentryPrivilege4, tSentryPrivilege5,
        tSentryPrivilege6, tSentryPrivilege7, tSentryPrivilege8));
    tSentryMappingData2.setGroupRolesMap(sentryGroupRolesMap2);
    tSentryMappingData2.setRolePrivilegesMap(sentryRolePrivilegesMap2);
    sentryStore.importSentryMetaData(tSentryMappingData2, false);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2", "role3"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1", "group2", "group3"));

    // test the result data for the privilege
    verifyPrivileges(privilegesList, Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1", "role2", "role3"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2", "role3"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role1", "role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege4, tSentryPrivilege5,
        tSentryPrivilege6, tSentryPrivilege7, tSentryPrivilege8));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);
  }

  // call import twice, and there has one role without group.
  // The data for 1st import:
  // group1=role1, role2
  // role1=privilege1,privilege2
  // role2=privilege3,privilege4
  // The data for 2nd import:
  // group2=role2
  // role2=privilege5,privilege6
  // role3=privilege7,privilege8
  // role3 is without group, will be imported also
  @Test
  public void testImportExportPolicy4() throws Exception {
    TSentryMappingData tSentryMappingData1 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap1 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap1 = Maps.newHashMap();
    sentryGroupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
    sentryRolePrivilegesMap1.put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2));
    sentryRolePrivilegesMap1.put("role2", Sets.newHashSet(tSentryPrivilege3, tSentryPrivilege4));
    tSentryMappingData1.setGroupRolesMap(sentryGroupRolesMap1);
    tSentryMappingData1.setRolePrivilegesMap(sentryRolePrivilegesMap1);
    sentryStore.importSentryMetaData(tSentryMappingData1, false);

    TSentryMappingData tSentryMappingData2 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap2 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap2 = Maps.newHashMap();
    sentryGroupRolesMap2.put("group2", Sets.newHashSet("role2"));
    sentryRolePrivilegesMap2.put("role2", Sets.newHashSet(tSentryPrivilege5, tSentryPrivilege6));
    sentryRolePrivilegesMap2.put("role3", Sets.newHashSet(tSentryPrivilege7, tSentryPrivilege8));
    tSentryMappingData2.setGroupRolesMap(sentryGroupRolesMap2);
    tSentryMappingData2.setRolePrivilegesMap(sentryRolePrivilegesMap2);
    sentryStore.importSentryMetaData(tSentryMappingData2, false);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2", "role3"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1", "group2"));

    // test the result data for the privilege
    verifyPrivileges(privilegesList, Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1", "role2"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role2"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2));
    exceptedRolePrivilegesMap
        .put("role2", Sets.newHashSet(tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5,
        tSentryPrivilege6));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege7, tSentryPrivilege8));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);
  }

  // test for import mapping data for [group,role] only:
  // group1=role1, role2
  @Test
  public void testImportExportPolicy5() throws Exception {
    TSentryMappingData tSentryMappingData1 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap1 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap1 = Maps.newHashMap();
    sentryGroupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
    tSentryMappingData1.setGroupRolesMap(sentryGroupRolesMap1);
    tSentryMappingData1.setRolePrivilegesMap(sentryRolePrivilegesMap1);
    sentryStore.importSentryMetaData(tSentryMappingData1, false);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1"));

    // test the result data for the privilege
    assertTrue(privilegesList.isEmpty());

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1", "role2"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    assertTrue(actualRolePrivilegesMap.isEmpty());
  }

  // test for filter the orphaned group:
  // group1=role1, role2
  // group2=role2
  @Test
  public void testImportExportPolicy6() throws Exception {
    TSentryMappingData tSentryMappingData1 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap1 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap1 = Maps.newHashMap();
    sentryGroupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
    sentryGroupRolesMap1.put("group2", Sets.newHashSet("role2"));
    tSentryMappingData1.setGroupRolesMap(sentryGroupRolesMap1);
    tSentryMappingData1.setRolePrivilegesMap(sentryRolePrivilegesMap1);
    sentryStore.importSentryMetaData(tSentryMappingData1, false);

    // drop the role2, the group2 is orphaned group
    sentryStore.dropSentryRole("role2");

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1", "group2"));

    // test the result data for the privilege
    assertTrue(privilegesList.isEmpty());

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    assertTrue(actualRolePrivilegesMap.isEmpty());
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
  public void testImportExportPolicy7() throws Exception {
    TSentryMappingData tSentryMappingData1 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap1 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap1 = Maps.newHashMap();
    sentryGroupRolesMap1.put("group1", Sets.newHashSet("role1"));
    sentryRolePrivilegesMap1.put("role1", Sets.newHashSet(tSentryPrivilege1));
    tSentryMappingData1.setGroupRolesMap(sentryGroupRolesMap1);
    tSentryMappingData1.setRolePrivilegesMap(sentryRolePrivilegesMap1);
    // the import with overwrite mode
    sentryStore.importSentryMetaData(tSentryMappingData1, true);

    TSentryMappingData tSentryMappingData2 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap2 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap2 = Maps.newHashMap();
    sentryGroupRolesMap2.put("group2", Sets.newHashSet("role2", "role3"));
    sentryGroupRolesMap2.put("group3", Sets.newHashSet("role2", "role3"));
    sentryRolePrivilegesMap2.put("role2", Sets.newHashSet(tSentryPrivilege2));
    sentryRolePrivilegesMap2.put("role3", Sets.newHashSet(tSentryPrivilege2));
    tSentryMappingData2.setGroupRolesMap(sentryGroupRolesMap2);
    tSentryMappingData2.setRolePrivilegesMap(sentryRolePrivilegesMap2);
    // the import with overwrite mode
    sentryStore.importSentryMetaData(tSentryMappingData2, true);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2", "role3"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1", "group2", "group3"));

    // test the result data for the privilege
    verifyPrivileges(privilegesList, Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2));

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role2", "role3"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege1));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege2));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege2));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);
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
  public void testImportExportPolicy8() throws Exception {
    TSentryMappingData tSentryMappingData1 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap1 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap1 = Maps.newHashMap();
    sentryGroupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
    sentryGroupRolesMap1.put("group2", Sets.newHashSet("role1", "role2"));
    sentryGroupRolesMap1.put("group3", Sets.newHashSet("role1", "role2"));
    sentryRolePrivilegesMap1.put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5));
    sentryRolePrivilegesMap1.put("role2", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5));
    tSentryMappingData1.setGroupRolesMap(sentryGroupRolesMap1);
    tSentryMappingData1.setRolePrivilegesMap(sentryRolePrivilegesMap1);
    // the import with overwrite mode
    sentryStore.importSentryMetaData(tSentryMappingData1, true);

    TSentryMappingData tSentryMappingData2 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap2 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap2 = Maps.newHashMap();
    sentryGroupRolesMap2.put("group1", Sets.newHashSet("role2", "role3"));
    sentryGroupRolesMap2.put("group2", Sets.newHashSet("role2", "role3"));
    sentryGroupRolesMap2.put("group3", Sets.newHashSet("role2", "role3"));
    sentryRolePrivilegesMap2.put("role2", Sets.newHashSet(tSentryPrivilege4, tSentryPrivilege5,
        tSentryPrivilege6, tSentryPrivilege7, tSentryPrivilege8));
    sentryRolePrivilegesMap2.put("role3", Sets.newHashSet(tSentryPrivilege4, tSentryPrivilege5,
        tSentryPrivilege6, tSentryPrivilege7, tSentryPrivilege8));
    tSentryMappingData2.setGroupRolesMap(sentryGroupRolesMap2);
    tSentryMappingData2.setRolePrivilegesMap(sentryRolePrivilegesMap2);
    // the import with overwrite mode
    sentryStore.importSentryMetaData(tSentryMappingData2, true);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2", "role3"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1", "group2", "group3"));

    // test the result data for the privilege
    verifyPrivileges(privilegesList, Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1", "role2", "role3"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2", "role3"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role1", "role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5));
    // role2 should be overwrite
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege4, tSentryPrivilege5,
        tSentryPrivilege6, tSentryPrivilege7, tSentryPrivilege8));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege4, tSentryPrivilege5,
        tSentryPrivilege6, tSentryPrivilege7, tSentryPrivilege8));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);
  }

  // test the import privileges with the action: All, *, select, insert
  // All and * should replace the select and insert
  // The data for import:
  // group1=role1, role2
  // role1=testPrivilege1,testPrivilege2,testPrivilege3,testPrivilege4
  // role2=testPrivilege5, testPrivilege6,testPrivilege7,testPrivilege8
  @Test
  public void testImportExportPolicy9() throws Exception {
    TSentryPrivilege testPrivilege1 = createTSentryPrivilege(PrivilegeScope.TABLE.name(),
        "server1", "db1", "tbl1", "", "", AccessConstants.SELECT, TSentryGrantOption.TRUE);
    TSentryPrivilege testPrivilege2 = createTSentryPrivilege(PrivilegeScope.TABLE.name(),
        "server1", "db1", "tbl1", "", "", AccessConstants.INSERT, TSentryGrantOption.FALSE);
    TSentryPrivilege testPrivilege3 = createTSentryPrivilege(PrivilegeScope.TABLE.name(),
        "server1", "db1", "tbl1", "", "", AccessConstants.ACTION_ALL, TSentryGrantOption.TRUE);
    TSentryPrivilege testPrivilege4 = createTSentryPrivilege(PrivilegeScope.TABLE.name(),
        "server1", "db1", "tbl1", "", "", AccessConstants.INSERT, TSentryGrantOption.TRUE);
    TSentryPrivilege testPrivilege5 = createTSentryPrivilege(PrivilegeScope.TABLE.name(),
        "server1", "db1", "tbl2", "", "", AccessConstants.SELECT, TSentryGrantOption.TRUE);
    TSentryPrivilege testPrivilege6 = createTSentryPrivilege(PrivilegeScope.TABLE.name(),
        "server1", "db1", "tbl2", "", "", AccessConstants.INSERT, TSentryGrantOption.FALSE);
    TSentryPrivilege testPrivilege7 = createTSentryPrivilege(PrivilegeScope.TABLE.name(),
        "server1", "db1", "tbl2", "", "", AccessConstants.ALL, TSentryGrantOption.TRUE);
    TSentryPrivilege testPrivilege8 = createTSentryPrivilege(PrivilegeScope.TABLE.name(),
        "server1", "db1", "tbl2", "", "", AccessConstants.INSERT, TSentryGrantOption.TRUE);

    TSentryMappingData tSentryMappingData1 = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap1 = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap1 = Maps.newHashMap();
    sentryGroupRolesMap1.put("group1", Sets.newHashSet("role1", "role2"));
    // after import there should be only testPrivilege2, testPrivilege3
    sentryRolePrivilegesMap1.put("role1",
        Sets.newHashSet(testPrivilege1, testPrivilege2, testPrivilege3, testPrivilege4));
    // after import there should be only testPrivilege6,testPrivilege7
    sentryRolePrivilegesMap1.put("role2",
        Sets.newHashSet(testPrivilege5, testPrivilege6, testPrivilege7, testPrivilege8));
    tSentryMappingData1.setGroupRolesMap(sentryGroupRolesMap1);
    tSentryMappingData1.setRolePrivilegesMap(sentryRolePrivilegesMap1);
    // the import with overwrite mode
    sentryStore.importSentryMetaData(tSentryMappingData1, true);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1"));

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1", "role2"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(testPrivilege2, testPrivilege3));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(testPrivilege6, testPrivilege7));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);
  }

  // The following data is imported:
  // group1=role1
  // group2=role1,role2
  // group3=role2,role3
  // group4=role1,role2,role3
  // role1=privilege3,privilege4,privilege9
  // role2=privilege3,privilege4,privilege5,privilege6,privilege7
  // role3=privilege4,privilege5,privilege6,privilege7,privilege8
  // Export APIs getRoleNameTPrivilegesMap, getGroupNameRoleNamesMap are tested.
  @Test
  public void testExportPolicyWithSpecificObject() throws Exception {
    // import the data for test
    TSentryMappingData tSentryMappingData = new TSentryMappingData();
    Map<String, Set<String>> sentryGroupRolesMap = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap = Maps.newHashMap();
    sentryGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    sentryGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2"));
    sentryGroupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    sentryGroupRolesMap.put("group4", Sets.newHashSet("role1", "role2", "role3"));
    sentryRolePrivilegesMap.put("role1", Sets.newHashSet(
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege9));
    sentryRolePrivilegesMap.put("role2", Sets.newHashSet(
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7));
    sentryRolePrivilegesMap.put("role3", Sets.newHashSet(
        tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    tSentryMappingData.setGroupRolesMap(sentryGroupRolesMap);
    tSentryMappingData.setRolePrivilegesMap(sentryRolePrivilegesMap);
    sentryStore.importSentryMetaData(tSentryMappingData, false);

    // verify the rolePrivilegesMap and groupRolesMap for db=db1
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap =
            sentryStore.getRoleNameTPrivilegesMap("db1", "");
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege4));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege4,
        tSentryPrivilege5, tSentryPrivilege6, tSentryPrivilege7));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege4,
        tSentryPrivilege5, tSentryPrivilege6, tSentryPrivilege7));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);

    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(
        actualRolePrivilegesMap.keySet());
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    exceptedGroupRolesMap.put("group4", Sets.newHashSet("role1", "role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // verify the rolePrivilegesMap and groupRolesMap for db=db2
    actualRolePrivilegesMap = sentryStore.getRoleNameTPrivilegesMap("db2", "");
    exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege3, tSentryPrivilege9));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege3));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);

    mapList = sentryStore.getGroupUserRoleMapList(actualRolePrivilegesMap.keySet());
    actualGroupRolesMap = mapList.get(SentryConstants.INDEX_GROUP_ROLES_MAP);
    exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role2"));
    exceptedGroupRolesMap.put("group4", Sets.newHashSet("role1", "role2"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // verify the rolePrivilegesMap and groupRolesMap for db=db1 and table=tbl1
    actualRolePrivilegesMap = sentryStore.getRoleNameTPrivilegesMap("db1", "tbl1");
    exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege4));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege4));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege4));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);

    mapList = sentryStore.getGroupUserRoleMapList(actualRolePrivilegesMap.keySet());
    actualGroupRolesMap = mapList.get(SentryConstants.INDEX_GROUP_ROLES_MAP);
    exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    exceptedGroupRolesMap.put("group4", Sets.newHashSet("role1", "role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // verify the rolePrivilegesMap and groupRolesMap for db=db1 and table=tbl2
    actualRolePrivilegesMap = sentryStore.getRoleNameTPrivilegesMap("db1", "tbl2");
    exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege5));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege5));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);

    mapList = sentryStore.getGroupUserRoleMapList(actualRolePrivilegesMap.keySet());
    actualGroupRolesMap = mapList.get(SentryConstants.INDEX_GROUP_ROLES_MAP);
    exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role2"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    exceptedGroupRolesMap.put("group4", Sets.newHashSet("role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // verify the rolePrivilegesMap and groupRolesMap for table=tbl1
    actualRolePrivilegesMap = sentryStore.getRoleNameTPrivilegesMap("", "tbl1");
    exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege4, tSentryPrivilege9));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege4));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege4));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);

    mapList = sentryStore.getGroupUserRoleMapList(actualRolePrivilegesMap.keySet());
    actualGroupRolesMap = mapList.get(SentryConstants.INDEX_GROUP_ROLES_MAP);
    exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    exceptedGroupRolesMap.put("group4", Sets.newHashSet("role1", "role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    // verify the rolePrivilegesMap and groupRolesMap for empty parameter
    actualRolePrivilegesMap = sentryStore.getRoleNameTPrivilegesMap("", "");
    exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege3,
        tSentryPrivilege4, tSentryPrivilege9));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege3,
        tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6, tSentryPrivilege7));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege4,
        tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);

    mapList = sentryStore.getGroupUserRoleMapList(actualRolePrivilegesMap.keySet());
    actualGroupRolesMap = mapList.get(SentryConstants.INDEX_GROUP_ROLES_MAP);
    exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1"));
    exceptedGroupRolesMap.put("group2", Sets.newHashSet("role1", "role2"));
    exceptedGroupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    exceptedGroupRolesMap.put("group4", Sets.newHashSet("role1", "role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);
  }

  // Befor import, database is empty.
  // The following information is imported:
  // group1=role1,role2,role3
  // user1=role1,role2
  // user2=role2,role3
  // role1=privilege1,privilege2,privilege3,privilege4
  // role2=privilege5,privilege6,privilege7,privilege8
  // role3=privilege3,privilege4,privilege5,privilege6
  // Both import API importSentryMetaData and export APIs getRolesMap, getGroupsMap,
  // getUsersMap getPrivilegesList are tested.
  @Test
  public void testImportExportWithUser() throws Exception {
    TSentryMappingData tSentryMappingData = new TSentryMappingData();
    Map<String, Set<String>> groupRolesMap = Maps.newHashMap();
    Map<String, Set<String>> userRolesMap = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap = Maps.newHashMap();
    groupRolesMap.put("group1", Sets.newHashSet("Role1", "role2", "role3"));
    userRolesMap.put("user1", Sets.newHashSet("Role1", "role2"));
    userRolesMap.put("user2", Sets.newHashSet("role2", "role3"));
    sentryRolePrivilegesMap.put("Role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4));
    sentryRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    sentryRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege3,
        tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6));
    tSentryMappingData.setGroupRolesMap(groupRolesMap);
    tSentryMappingData.setRolePrivilegesMap(sentryRolePrivilegesMap);
    tSentryMappingData.setUserRolesMap(userRolesMap);
    sentryStore.importSentryMetaData(tSentryMappingData, false);

    Map<String, MSentryRole> rolesMap = sentryStore.getRolesMap();
    Map<String, MSentryGroup> groupsMap = sentryStore.getGroupNameToGroupMap();
    Map<String, MSentryUser> usersMap = sentryStore.getUserNameToUserMap();
    List<MSentryPrivilege> privilegesList = sentryStore.getPrivilegesList();

    // test the result data for the role
    verifyRoles(rolesMap, Sets.newHashSet("role1", "role2", "role3"));

    // test the result data for the group
    verifyGroups(groupsMap, Sets.newHashSet("group1"));

    // test the result data for the user
    verifyUsers(usersMap, Sets.newHashSet("user1", "user2"));

    // test the result data for the privilege
    verifyPrivileges(privilegesList, Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));

    // test the mapping data for group and role
    List<Map<String, Set<String>>> mapList = sentryStore.getGroupUserRoleMapList(null);
    Map<String, Set<String>> actualGroupRolesMap = mapList.get(
      SentryConstants.INDEX_GROUP_ROLES_MAP);
    Map<String, Set<String>> exceptedGroupRolesMap = Maps.newHashMap();
    exceptedGroupRolesMap.put("group1", Sets.newHashSet("role1", "role2", "role3"));
    verifyUserGroupRolesMap(actualGroupRolesMap, exceptedGroupRolesMap);

    Map<String, Set<String>> actualUserRolesMap = mapList.get(
      SentryConstants.INDEX_USER_ROLES_MAP);
    Map<String, Set<String>> exceptedUserRolesMap = Maps.newHashMap();
    exceptedUserRolesMap.put("user1", Sets.newHashSet("role1", "role2"));
    exceptedUserRolesMap.put("user2", Sets.newHashSet("role2", "role3"));
    verifyUserGroupRolesMap(actualUserRolesMap, exceptedUserRolesMap);

    // test the mapping data for role and privilege
    Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap = sentryStore
        .getRoleNameTPrivilegesMap();
    Map<String, Set<TSentryPrivilege>> exceptedRolePrivilegesMap = Maps.newHashMap();
    exceptedRolePrivilegesMap.put("role1", Sets.newHashSet(tSentryPrivilege1, tSentryPrivilege2,
        tSentryPrivilege3, tSentryPrivilege4));
    exceptedRolePrivilegesMap.put("role2", Sets.newHashSet(tSentryPrivilege5, tSentryPrivilege6,
        tSentryPrivilege7, tSentryPrivilege8));
    exceptedRolePrivilegesMap.put("role3", Sets.newHashSet(tSentryPrivilege3,
        tSentryPrivilege4, tSentryPrivilege5, tSentryPrivilege6));

    verifyRolePrivilegesMap(actualRolePrivilegesMap, exceptedRolePrivilegesMap);
  }

  private void verifyRoles(Map<String, MSentryRole> actualRoleMap, Set<String> expectedRoleNameSet) {
    assertEquals(expectedRoleNameSet.size(), actualRoleMap.keySet().size());
    for (String roleName : actualRoleMap.keySet()) {
      assertTrue(expectedRoleNameSet.contains(roleName));
    }
  }

  private void verifyGroups(Map<String, MSentryGroup> actualGroupsMap,
      Set<String> expectedGroupNameSet) {
    assertEquals(expectedGroupNameSet.size(), actualGroupsMap.keySet().size());
    for (String groupName : actualGroupsMap.keySet()) {
      assertTrue(expectedGroupNameSet.contains(groupName));
    }
  }

  private void verifyUsers(Map<String, MSentryUser> actualUsersMap,
                            Set<String> expectedUserNameSet) {
    assertEquals(expectedUserNameSet.size(), actualUsersMap.keySet().size());
    for (String userName : actualUsersMap.keySet()) {
      assertTrue(expectedUserNameSet.contains(userName));
    }
  }

  private void verifyPrivileges(List<MSentryPrivilege> actualPrivileges,
      Set<TSentryPrivilege> expectedTSentryPrivilegeSet) {
    assertEquals(expectedTSentryPrivilegeSet.size(), actualPrivileges.size());
    for (MSentryPrivilege mSentryPrivilege : actualPrivileges) {
      boolean isFound = false;
      for (TSentryPrivilege tSentryPrivilege : expectedTSentryPrivilegeSet) {
        isFound = compareTSentryPrivilege(sentryStore.convertToTSentryPrivilege(mSentryPrivilege),
            tSentryPrivilege);
        if (isFound) {
          break;
        }
      }
      assertTrue(isFound);
    }
  }

  private void verifyUserGroupRolesMap(Map<String, Set<String>> actualMap,
      Map<String, Set<String>> exceptedMap) {
    assertEquals(exceptedMap.keySet().size(), actualMap.keySet().size());
    for (String name : actualMap.keySet()) {
      Set<String> exceptedRoles = exceptedMap.get(name);
      Set<String> actualRoles = actualMap.get(name);
      assertEquals(actualRoles.size(), exceptedRoles.size());
      assertTrue(actualRoles.equals(exceptedRoles));
    }
  }

  private void verifyRolePrivilegesMap(Map<String, Set<TSentryPrivilege>> actualRolePrivilegesMap,
      Map<String, Set<TSentryPrivilege>> expectedRolePrivilegesMap) {
    assertEquals(expectedRolePrivilegesMap.keySet().size(), actualRolePrivilegesMap.keySet().size());
    for (String roleName : expectedRolePrivilegesMap.keySet()) {
      Set<TSentryPrivilege> exceptedTSentryPrivileges = expectedRolePrivilegesMap.get(roleName);
      Set<TSentryPrivilege> actualTSentryPrivileges = actualRolePrivilegesMap.get(roleName);
      assertEquals(exceptedTSentryPrivileges.size(), actualTSentryPrivileges.size());
      for (TSentryPrivilege actualPrivilege : actualTSentryPrivileges) {
        boolean isFound = false;
        for (TSentryPrivilege expectedPrivilege : exceptedTSentryPrivileges) {
          isFound = compareTSentryPrivilege(expectedPrivilege, actualPrivilege);
          if (isFound) {
            break;
          }
        }
        assertTrue(isFound);
      }
    }
  }

  private TSentryPrivilege createTSentryPrivilege(String scope, String server, String dbName,
      String tableName, String columnName, String uri, String action, TSentryGrantOption grantOption) {
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    tSentryPrivilege.setPrivilegeScope(scope);
    tSentryPrivilege.setServerName(server);
    tSentryPrivilege.setDbName(dbName);
    tSentryPrivilege.setTableName(tableName);
    tSentryPrivilege.setColumnName(columnName);
    tSentryPrivilege.setURI(uri);
    tSentryPrivilege.setAction(action);
    tSentryPrivilege.setGrantOption(grantOption);
    return tSentryPrivilege;
  }

  // compare the TSentryPrivilege without the create time
  private boolean compareTSentryPrivilege(TSentryPrivilege tSentryPrivilege1,
      TSentryPrivilege tSentryPrivilege2) {
    if (tSentryPrivilege1 == null) {
      if (tSentryPrivilege2 == null) {
        return true;
      } else {
        return false;
      }
    } else {
      if (tSentryPrivilege2 == null) {
        return false;
      }
    }

    boolean this_present_privilegeScope = true && tSentryPrivilege1.isSetPrivilegeScope();
    boolean that_present_privilegeScope = true && tSentryPrivilege2.isSetPrivilegeScope();
    if (this_present_privilegeScope || that_present_privilegeScope) {
      if (!(this_present_privilegeScope && that_present_privilegeScope)) {
        return false;
      }
      if (!tSentryPrivilege1.getPrivilegeScope().equalsIgnoreCase(
          tSentryPrivilege2.getPrivilegeScope())) {
        return false;
      }
    }

    boolean this_present_serverName = true && tSentryPrivilege1.isSetServerName();
    boolean that_present_serverName = true && tSentryPrivilege2.isSetServerName();
    if (this_present_serverName || that_present_serverName) {
      if (!(this_present_serverName && that_present_serverName)) {
        return false;
      }
      if (!tSentryPrivilege1.getServerName().equalsIgnoreCase(tSentryPrivilege2.getServerName())) {
        return false;
      }
    }

    boolean this_present_dbName = true && tSentryPrivilege1.isSetDbName();
    boolean that_present_dbName = true && tSentryPrivilege2.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName)) {
        return false;
      }
      if (!tSentryPrivilege1.getDbName().equalsIgnoreCase(tSentryPrivilege2.getDbName())) {
        return false;
      }
    }

    boolean this_present_tableName = true && tSentryPrivilege1.isSetTableName();
    boolean that_present_tableName = true && tSentryPrivilege2.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName)) {
        return false;
      }
      if (!tSentryPrivilege1.getTableName().equalsIgnoreCase(tSentryPrivilege2.getTableName())) {
        return false;
      }
    }

    boolean this_present_URI = true && tSentryPrivilege1.isSetURI();
    boolean that_present_URI = true && tSentryPrivilege2.isSetURI();
    if (this_present_URI || that_present_URI) {
      if (!(this_present_URI && that_present_URI)) {
        return false;
      }
      if (!tSentryPrivilege1.getURI().equalsIgnoreCase(tSentryPrivilege2.getURI())) {
        return false;
      }
    }

    boolean this_present_action = true && tSentryPrivilege1.isSetAction();
    boolean that_present_action = true && tSentryPrivilege2.isSetAction();
    if (this_present_action || that_present_action) {
      if (!(this_present_action && that_present_action)) {
        return false;
      }
      if (!tSentryPrivilege1.getAction().equalsIgnoreCase(tSentryPrivilege2.getAction())) {
        return false;
      }
    }

    boolean this_present_grantOption = true && tSentryPrivilege1.isSetGrantOption();
    boolean that_present_grantOption = true && tSentryPrivilege2.isSetGrantOption();
    if (this_present_grantOption || that_present_grantOption) {
      if (!(this_present_grantOption && that_present_grantOption)) {
        return false;
      }
      if (!tSentryPrivilege1.getGrantOption().equals(tSentryPrivilege2.getGrantOption())) {
        return false;
      }
    }

    boolean this_present_columnName = true && tSentryPrivilege1.isSetColumnName();
    boolean that_present_columnName = true && tSentryPrivilege2.isSetColumnName();
    if (this_present_columnName || that_present_columnName) {
      if (!(this_present_columnName && that_present_columnName)) {
        return false;
      }
      if (!tSentryPrivilege1.getColumnName().equalsIgnoreCase(tSentryPrivilege2.getColumnName())) {
        return false;
      }
    }

    return true;
  }
}
