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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.utils.PolicyFileConstants;

import org.junit.*;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestSentryIniPolicyFileFormatter {

  private static final String RESOURCE_FILE = "testImportExportPolicy.ini";
  private static File baseDir;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem fileSystem;
  private static Path root;
  private static Path etc;

  // define the privileges
  public static String PRIVILIEGE1 = "server=server1";
  public static String PRIVILIEGE2 = "server=server1->action=select->grantoption=false";
  public static String PRIVILIEGE3 = "server=server1->db=db2->action=insert->grantoption=true";
  public static String PRIVILIEGE4 = "server=server1->db=db1->table=tbl1->action=insert";
  public static String PRIVILIEGE5 = "server=server1->db=db1->table=tbl2->column=col1->action=insert";
  public static String PRIVILIEGE6 = "server=server1->db=db1->table=tbl3->column=col1->action=*->grantoption=true";
  public static String PRIVILIEGE7 = "server=server1->db=db1->table=tbl4->column=col1->action=all->grantoption=true";
  public static String PRIVILIEGE8 = "server=server1->uri=hdfs://testserver:9999/path2->action=insert";

  private static Map<String, Map<String, Set<String>>> policyFileMappingData1;
  private static Map<String, Map<String, Set<String>>> policyFileMappingData2;
  private static Map<String, Map<String, Set<String>>> policyFileMappingData3;
  private static Map<String, Map<String, Set<String>>> policyFileMappingData4;
  private static Map<String, Map<String, Set<String>>> policyFileMappingData5;

  @BeforeClass
  public static void setup() throws IOException {
    baseDir = Files.createTempDir();
    Assert.assertNotNull(baseDir);
    File dfsDir = new File(baseDir, "dfs");
    assertTrue(dfsDir.isDirectory() || dfsDir.mkdirs());
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getPath());
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    fileSystem = dfsCluster.getFileSystem();
    root = new Path(fileSystem.getUri().toString());
    etc = new Path(root, "/etc");
    fileSystem.mkdirs(etc);
    prepareTestData();
  }

  @AfterClass
  public static void teardownLocalClazz() throws Exception{
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
    if(dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  private static void prepareTestData() {
    // test data for:
    // [users]
    // user1=role1,role2,role3
    // user2=role1,role2,role3
    // user3=role1,role2,role3
    // [groups]
    // group1=role1,role2,role3
    // group2=role1,role2,role3
    // group3=role1,role2,role3
    // [roles]
    // role1=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
    // role2=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
    // role3=privilege1,privilege2,privilege3,privilege4,privilege5,privilege6,privilege7,privilege8
    policyFileMappingData1 = Maps.newHashMap();
    Map<String, Set<String>> userRolesMap = Maps.newHashMap();
    Map<String, Set<String>> groupRolesMap = Maps.newHashMap();
    Map<String, Set<String>> rolePrivilegesMap = Maps.newHashMap();
    Set<String> roles = Sets.newHashSet("role1", "role2", "role3");
    userRolesMap.put("user1", roles);
    userRolesMap.put("user2", roles);
    userRolesMap.put("user3", roles);
    groupRolesMap.put("group1", roles);
    groupRolesMap.put("group2", roles);
    groupRolesMap.put("group3", roles);
    for (String roleName : roles) {
      rolePrivilegesMap.put(roleName, Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3,
          PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
    }
    policyFileMappingData1.put(PolicyFileConstants.USER_ROLES, userRolesMap);
    policyFileMappingData1.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData1.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

    // test data for:
    // [users]
    // user1=role1
    // user2=role2
    // user3=role3
    // [groups]
    // group1=role1
    // group2=role2
    // group3=role3
    // [roles]
    // role1=privilege1,privilege2,privilege3
    // role2=privilege4,privilege5,privilege6
    // role3=privilege7,privilege8
    policyFileMappingData2 = Maps.newHashMap();
    userRolesMap = Maps.newHashMap();
    groupRolesMap = Maps.newHashMap();
    rolePrivilegesMap = Maps.newHashMap();
    userRolesMap.put("user1", Sets.newHashSet("role1"));
    userRolesMap.put("user2", Sets.newHashSet("role2"));
    userRolesMap.put("user3", Sets.newHashSet("role3"));
    groupRolesMap.put("group1", Sets.newHashSet("role1"));
    groupRolesMap.put("group2", Sets.newHashSet("role2"));
    groupRolesMap.put("group3", Sets.newHashSet("role3"));
    rolePrivilegesMap.put("role1", Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3));
    rolePrivilegesMap.put("role2", Sets.newHashSet(PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6));
    rolePrivilegesMap.put("role3", Sets.newHashSet(PRIVILIEGE7, PRIVILIEGE8));
    policyFileMappingData2.put(PolicyFileConstants.USER_ROLES, userRolesMap);
    policyFileMappingData2.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData2.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

    // test data for:
    // [users]
    // user1=role1,role2
    // user2=role1,role2,role3
    // user3=role2,role3
    // [groups]
    // group1=role1,role2
    // group2=role1,role2,role3
    // group3=role2,role3
    // [roles]
    // role1=privilege1,privilege2,privilege3,privilege4
    // role2=privilege3,privilege4,privilege5,privilege6
    // role3=privilege5,privilege6,privilege7,privilege8
    policyFileMappingData3 = Maps.newHashMap();
    userRolesMap = Maps.newHashMap();
    groupRolesMap = Maps.newHashMap();
    rolePrivilegesMap = Maps.newHashMap();
    userRolesMap.put("user1", Sets.newHashSet("role1", "role2"));
    userRolesMap.put("user2", Sets.newHashSet("role1", "role2", "role3"));
    userRolesMap.put("user3", Sets.newHashSet("role2", "role3"));
    groupRolesMap.put("group1", Sets.newHashSet("role1", "role2"));
    groupRolesMap.put("group2", Sets.newHashSet("role1", "role2", "role3"));
    groupRolesMap.put("group3", Sets.newHashSet("role2", "role3"));
    rolePrivilegesMap.put("role1",
        Sets.newHashSet(PRIVILIEGE1, PRIVILIEGE2, PRIVILIEGE3, PRIVILIEGE4));
    rolePrivilegesMap.put("role2",
        Sets.newHashSet(PRIVILIEGE3, PRIVILIEGE4, PRIVILIEGE5, PRIVILIEGE6));
    rolePrivilegesMap.put("role3",
        Sets.newHashSet(PRIVILIEGE5, PRIVILIEGE6, PRIVILIEGE7, PRIVILIEGE8));
    policyFileMappingData3.put(PolicyFileConstants.USER_ROLES, userRolesMap);
    policyFileMappingData3.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData3.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

    // test data for users, groups only
    policyFileMappingData4 = Maps.newHashMap();
    userRolesMap = Maps.newHashMap();
    groupRolesMap = Maps.newHashMap();
    rolePrivilegesMap = Maps.newHashMap();
    userRolesMap.put("user1", Sets.newHashSet("role1", "role2"));
    groupRolesMap.put("group1", Sets.newHashSet("role1", "role2"));
    policyFileMappingData4.put(PolicyFileConstants.USER_ROLES, userRolesMap);
    policyFileMappingData4.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData4.put(PolicyFileConstants.ROLES, rolePrivilegesMap);

    // test empty data
    policyFileMappingData5 = Maps.newHashMap();
    userRolesMap = Maps.newHashMap();
    groupRolesMap = Maps.newHashMap();
    rolePrivilegesMap = Maps.newHashMap();
    policyFileMappingData5.put(PolicyFileConstants.USER_ROLES, userRolesMap);
    policyFileMappingData5.put(PolicyFileConstants.GROUPS, groupRolesMap);
    policyFileMappingData5.put(PolicyFileConstants.ROLES, rolePrivilegesMap);
  }


  @Before
  public void  setupBeforeTest() throws IOException {
    fileSystem.delete(etc, true);
    fileSystem.mkdirs(etc);
  }

  /**
   * Test to verify import and export into local file system.
   * @throws Exception
   */
  @Test
  public void testImportExportFromLocalFilesystem() throws Exception {

    File baseDir = Files.createTempDir();
    String resourcePath = (new File(baseDir, RESOURCE_FILE)).getAbsolutePath();
    HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
    SentryIniPolicyFileFormatter iniFormatter = new SentryIniPolicyFileFormatter();

    // test data1
    iniFormatter.write(resourcePath, authzConf, policyFileMappingData1);
    Map<String, Map<String, Set<String>>> parsedMappingData = iniFormatter.parse(resourcePath,
        authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData1);

    // test data2
    iniFormatter.write(resourcePath, authzConf, policyFileMappingData2);
    parsedMappingData = iniFormatter.parse(resourcePath, authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData2);

    // test data3
    iniFormatter.write(resourcePath, authzConf, policyFileMappingData3);
    parsedMappingData = iniFormatter.parse(resourcePath, authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData3);

    // test data4
    iniFormatter.write(resourcePath, authzConf, policyFileMappingData4);
    parsedMappingData = iniFormatter.parse(resourcePath, authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData4);

    // test data5
    iniFormatter.write(resourcePath, authzConf, policyFileMappingData5);
    parsedMappingData = iniFormatter.parse(resourcePath, authzConf);
    assertTrue(parsedMappingData.get(PolicyFileConstants.USER_ROLES).isEmpty());
    assertTrue(parsedMappingData.get(PolicyFileConstants.GROUPS).isEmpty());
    assertTrue(parsedMappingData.get(PolicyFileConstants.ROLES).isEmpty());
    (new File(baseDir, RESOURCE_FILE)).delete();
  }

  /**
   * Test to verify import and export into a hdfs file
   * @throws Exception
   */
  @Test
  public void testImportExportFromHDFS() throws Exception {
    Path resourcePath = new Path(etc, RESOURCE_FILE);
    String resourcePathStr = resourcePath.toUri().toString();
    HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
    SentryIniPolicyFileFormatter iniFormatter = new SentryIniPolicyFileFormatter();

    // test data1
    iniFormatter.write(resourcePathStr, authzConf, policyFileMappingData1);
    Map<String, Map<String, Set<String>>> parsedMappingData = iniFormatter.parse(resourcePath.toUri().toString(),
            authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData1);

    // test data2
    iniFormatter.write(resourcePathStr, authzConf, policyFileMappingData2);
    parsedMappingData = iniFormatter.parse(resourcePath.toUri().toString(), authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData2);

    // test data3
    iniFormatter.write(resourcePathStr, authzConf, policyFileMappingData3);
    parsedMappingData = iniFormatter.parse(resourcePath.toUri().toString(), authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData3);

    // test data4
    iniFormatter.write(resourcePathStr, authzConf, policyFileMappingData4);
    parsedMappingData = iniFormatter.parse(resourcePath.toUri().toString(), authzConf);
    validateSentryMappingData(parsedMappingData, policyFileMappingData4);

    // test data5
    iniFormatter.write(resourcePathStr, authzConf, policyFileMappingData5);
    parsedMappingData = iniFormatter.parse(resourcePath.toUri().toString(), authzConf);
    assertTrue(parsedMappingData.get(PolicyFileConstants.USER_ROLES).isEmpty());
    assertTrue(parsedMappingData.get(PolicyFileConstants.GROUPS).isEmpty());
    assertTrue(parsedMappingData.get(PolicyFileConstants.ROLES).isEmpty());
  }

  // verify the mapping data
  public void validateSentryMappingData(Map<String, Map<String, Set<String>>> actualMappingData,
      Map<String, Map<String, Set<String>>> expectedMappingData) {
    validateGroupRolesMap(actualMappingData.get(PolicyFileConstants.USER_ROLES),
        expectedMappingData.get(PolicyFileConstants.USER_ROLES));
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
          String withOptionPrivilege = SentryConstants.AUTHORIZABLE_JOINER.join(actualPrivilege,
              SentryConstants.KV_JOINER.join(PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME,
                  "false"));
          isFound = exceptedPrivileges.contains(withOptionPrivilege);
        }
        assertTrue(isFound);
      }
    }
  }
}
