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

package org.apache.sentry.cli.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceIntegrationBase;
import org.apache.sentry.api.generic.thrift.TAuthorizable;
import org.apache.sentry.api.generic.thrift.TSentryPrivilege;
import org.apache.sentry.api.generic.thrift.TSentryRole;
import org.apache.sentry.api.tools.GenericPrivilegeConverter;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Files;

public class TestPermissionsMigrationToolSolr extends SentryGenericServiceIntegrationBase {
  private File confDir;
  private File confPath;
  private String requestorName = "";
  private String service = "service1";

  @Before
  public void prepareForTest() throws Exception {
    confDir = Files.createTempDir();
    confPath = new File(confDir, "sentry-site.xml");
    if (confPath.createNewFile()) {
      FileOutputStream to = new FileOutputStream(confPath);
      conf.writeXml(to);
      to.close();
    }
    requestorName = clientUgi.getShortUserName();//System.getProperty("user.name", "");
    Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
    setLocalGroupMapping(requestorName, requestorUserGroupNames);
    // add ADMIN_USER for the after() in SentryServiceIntegrationBase
    setLocalGroupMapping(ADMIN_USER, requestorUserGroupNames);
    setLocalGroupMapping("dev", Sets.newHashSet("dev_group"));
    setLocalGroupMapping("user", Sets.newHashSet("user_group"));
    writePolicyFile();
  }

  @After
  public void clearTestData() throws Exception {
    FileUtils.deleteQuietly(confDir);

    // clear roles and privileges
    Set<TSentryRole> tRoles = client.listAllRoles(requestorName, SOLR);
    for (TSentryRole tRole : tRoles) {
      String role = tRole.getRoleName();
      Set<TSentryPrivilege> privileges = client.listAllPrivilegesByRoleName(
          requestorName, role, SOLR, service);
      for (TSentryPrivilege privilege : privileges) {
        client.revokePrivilege(requestorName, role, SOLR, privilege);
      }
      client.dropRole(requestorName, role, SOLR);
    }
  }

  @Test
  public void testPermissionsMigrationFromSentrySvc_v1() throws Exception {
    initializeSentryService();

    String[] args = { "-s", "1.8.0", "-c", confPath.getAbsolutePath()};
    PermissionsMigrationToolSolr sentryTool = new PermissionsMigrationToolSolr();
    sentryTool.executeConfigTool(args);

    Map<String, Set<String>> groupMapping = new HashMap<String, Set<String>>();
    groupMapping.put("admin_role", Sets.newHashSet("admin_group"));
    groupMapping.put("dev_role", Sets.newHashSet("dev_group"));
    groupMapping.put("user_role", Sets.newHashSet("user_group"));

    Map<String, Set<String>> privilegeMapping = new HashMap<String, Set<String>>();
    privilegeMapping.put("admin_role",
        Sets.newHashSet("admin=collections->action=*", "admin=cores->action=*"));
    privilegeMapping.put("dev_role",
        Sets.newHashSet("collection=*->action=*", "admin=collections->action=*", "admin=cores->action=*"));
    privilegeMapping.put("user_role",
        Sets.newHashSet("collection=foo->action=*"));

    verifySentryServiceState(groupMapping, privilegeMapping);
  }

  @Test
  public void testPermissionsMigrationFromSentryPolicyFile_v1() throws Exception {
    Path policyFilePath = initializeSentryPolicyFile();
    Path outputFilePath = Paths.get(confDir.getAbsolutePath(), "sentry-provider_migrated.ini");

    String[] args = { "-s", "1.8.0", "-p", policyFilePath.toFile().getAbsolutePath(),
                      "-o", outputFilePath.toFile().getAbsolutePath() };
    PermissionsMigrationToolSolr sentryTool = new PermissionsMigrationToolSolr();
    assertTrue(sentryTool.executeConfigTool(args));

    Set<String> groups = new HashSet<>();
    groups.add("admin_group");
    groups.add("dev_group");
    groups.add("user_group");

    Map<String, Set<String>> privilegeMapping = new HashMap<String, Set<String>>();
    privilegeMapping.put("admin_role",
        Sets.newHashSet("admin=collections->action=*", "admin=cores->action=*"));
    privilegeMapping.put("dev_role",
        Sets.newHashSet("collection=*->action=*", "admin=collections->action=*", "admin=cores->action=*"));
    privilegeMapping.put("user_role",
        Sets.newHashSet("collection=foo->action=*"));

    verifySentryPolicyFile(groups, privilegeMapping, outputFilePath);
  }

  @Test
  // For permissions created with Sentry 2.x, no migration necessary
  public void testPermissionsMigrationFromSentrySvc_v2() throws Exception {
    initializeSentryService();

    String[] args = { "-s", "2.0.0", "-c", confPath.getAbsolutePath()};
    PermissionsMigrationToolSolr sentryTool = new PermissionsMigrationToolSolr();
    sentryTool.executeConfigTool(args);

    Map<String, Set<String>> groupMapping = new HashMap<String, Set<String>>();
    groupMapping.put("admin_role", Sets.newHashSet("admin_group"));
    groupMapping.put("dev_role", Sets.newHashSet("dev_group"));
    groupMapping.put("user_role", Sets.newHashSet("user_group"));

    Map<String, Set<String>> privilegeMapping = new HashMap<String, Set<String>>();
    privilegeMapping.put("admin_role",
        Sets.newHashSet("collection=admin->action=*"));
    privilegeMapping.put("dev_role",
        Sets.newHashSet("collection=*->action=*"));
    privilegeMapping.put("user_role",
        Sets.newHashSet("collection=foo->action=*"));

    verifySentryServiceState(groupMapping, privilegeMapping);
  }

  @Test
  // For permissions created with Sentry 2.x, no migration necessary
  public void testPermissionsMigrationFromSentryPolicyFile_v2() throws Exception {
    Path policyFilePath = initializeSentryPolicyFile();
    Path outputFilePath = Paths.get(confDir.getAbsolutePath(), "sentry-provider_migrated.ini");

    String[] args = { "-s", "2.0.0", "-p", policyFilePath.toFile().getAbsolutePath(),
                      "-o", outputFilePath.toFile().getAbsolutePath() };
    PermissionsMigrationToolSolr sentryTool = new PermissionsMigrationToolSolr();
    assertTrue(sentryTool.executeConfigTool(args));

    Set<String> groups = new HashSet<>();
    groups.add("admin_group");
    groups.add("dev_group");
    groups.add("user_group");

    Map<String, Set<String>> privilegeMapping = new HashMap<String, Set<String>>();
    privilegeMapping.put("admin_role",
        Sets.newHashSet("collection=admin->action=*"));
    privilegeMapping.put("dev_role",
        Sets.newHashSet("collection=*->action=*"));
    privilegeMapping.put("user_role",
        Sets.newHashSet("collection=foo->action=*"));

    verifySentryPolicyFile(groups, privilegeMapping, outputFilePath);
  }

  @Test
  public void testDryRunOption() throws Exception {
    initializeSentryService();

    String[] args = { "-s", "1.8.0", "-c", confPath.getAbsolutePath(), "--dry_run"};
    PermissionsMigrationToolSolr sentryTool = new PermissionsMigrationToolSolr();
    sentryTool.executeConfigTool(args);

    Map<String, Set<String>> groupMapping = new HashMap<String, Set<String>>();
    groupMapping.put("admin_role", Sets.newHashSet("admin_group"));
    groupMapping.put("dev_role", Sets.newHashSet("dev_group"));
    groupMapping.put("user_role", Sets.newHashSet("user_group"));

    // No change in the privileges
    Map<String, Set<String>> privilegeMapping = new HashMap<String, Set<String>>();
    privilegeMapping.put("admin_role",
        Sets.newHashSet("collection=admin->action=*"));
    privilegeMapping.put("dev_role",
        Sets.newHashSet("collection=*->action=*"));
    privilegeMapping.put("user_role",
        Sets.newHashSet("collection=foo->action=*"));

    verifySentryServiceState(groupMapping, privilegeMapping);
  }

  @Test
  public void testInvalidToolArguments() throws Exception {
    PermissionsMigrationToolSolr sentryTool = new PermissionsMigrationToolSolr();

    {
      String[] args = { "-c", confPath.getAbsolutePath()};
      assertFalse("The execution should have failed due to missing source version",
          sentryTool.executeConfigTool(args));
    }

    {
      String[] args = { "-s", "1.8.0" };
      sentryTool.executeConfigTool(args);
      assertFalse("The execution should have failed due to missing Sentry config file"
          + " (or policy file) path",
          sentryTool.executeConfigTool(args));
    }

    {
      String[] args = { "-s", "1.8.0", "-p", "/test/path" };
      sentryTool.executeConfigTool(args);
      assertFalse("The execution should have failed due to missing Sentry config output file path",
          sentryTool.executeConfigTool(args));
    }

    {
      String[] args = { "-s", "1.8.0", "-c", "/test/path1", "-p", "/test/path2" };
      sentryTool.executeConfigTool(args);
      assertFalse("The execution should have failed due to providing both Sentry config file"
          + " as well as policy file params",
          sentryTool.executeConfigTool(args));
    }
  }

  private void initializeSentryService() throws SentryUserException {
    // Define an admin role
    client.createRoleIfNotExist(requestorName, "admin_role", SOLR);
    client.grantRoleToGroups(requestorName, "admin_role", SOLR, Sets.newHashSet("admin_group"));

    // Define a developer role
    client.createRoleIfNotExist(requestorName, "dev_role", SOLR);
    client.grantRoleToGroups(requestorName, "dev_role", SOLR, Sets.newHashSet("dev_group"));

    // Define a user role
    client.createRoleIfNotExist(requestorName, "user_role", SOLR);
    client.grantRoleToGroups(requestorName, "user_role", SOLR, Sets.newHashSet("user_group"));

    // Grant permissions
    client.grantPrivilege(requestorName, "admin_role", SOLR,
        new TSentryPrivilege(SOLR, "service1",
            Arrays.asList(new TAuthorizable("collection", "admin")), "*"));
    client.grantPrivilege(requestorName, "dev_role", SOLR,
        new TSentryPrivilege(SOLR, "service1",
            Arrays.asList(new TAuthorizable("collection", "*")), "*"));
    client.grantPrivilege(requestorName, "user_role", SOLR,
        new TSentryPrivilege(SOLR, "service1",
            Arrays.asList(new TAuthorizable("collection", "foo")), "*"));
  }

  private void verifySentryServiceState(Map<String, Set<String>> groupMapping,
      Map<String, Set<String>> privilegeMapping) throws SentryUserException {
    // check roles
    Set<TSentryRole> tRoles = client.listAllRoles(requestorName, SOLR);
    assertEquals("Unexpected number of roles", groupMapping.keySet().size(), tRoles.size());
    Set<String> roles = new HashSet<String>();
    for (TSentryRole tRole : tRoles) {
      roles.add(tRole.getRoleName());
    }

    for (String expectedRole : groupMapping.keySet()) {
      assertTrue("Didn't find expected role: " + expectedRole, roles.contains(expectedRole));
    }

    // check groups
    for (TSentryRole tRole : tRoles) {
      Set<String> expectedGroups = groupMapping.get(tRole.getRoleName());
      assertEquals("Group size doesn't match for role: " + tRole.getRoleName(),
          expectedGroups.size(), tRole.getGroups().size());
      assertTrue("Group does not contain all expected members for role: " + tRole.getRoleName(),
          tRole.getGroups().containsAll(expectedGroups));
    }

    // check privileges
    GenericPrivilegeConverter convert = new GenericPrivilegeConverter(SOLR, service);
    for (String role : roles) {
      Set<TSentryPrivilege> privileges = client.listAllPrivilegesByRoleName(
          requestorName, role, SOLR, service);
      Set<String> expectedPrivileges = privilegeMapping.get(role);
      assertEquals("Privilege set size doesn't match for role: " + role + " Actual permissions : " + privileges,
          expectedPrivileges.size(), privileges.size());

      Set<String> privilegeStrs = new HashSet<String>();
      for (TSentryPrivilege privilege : privileges) {
        privilegeStrs.add(convert.toString(privilege).toLowerCase());
      }

      for (String expectedPrivilege : expectedPrivileges) {
        assertTrue("Did not find expected privilege: " + expectedPrivilege + " in " + privilegeStrs,
            privilegeStrs.contains(expectedPrivilege));
      }
    }
  }

  private Path initializeSentryPolicyFile() throws Exception {
    PolicyFile file = new PolicyFile();

    file.addRolesToGroup("admin_group", "admin_role");
    file.addRolesToGroup("dev_group", "dev_role");
    file.addRolesToGroup("user_group", "user_role");

    file.addPermissionsToRole("admin_role", "collection=admin->action=*");
    file.addPermissionsToRole("dev_role", "collection=*->action=*");
    file.addPermissionsToRole("user_role", "collection=foo->action=*");

    Path policyFilePath = Paths.get(confDir.getAbsolutePath(), "sentry-provider.ini");
    file.write(policyFilePath.toFile());

    return policyFilePath;
  }

  private void verifySentryPolicyFile (Set<String> groups, Map<String, Set<String>> privilegeMapping,
      Path policyFilePath) throws IOException {
    SimpleFileProviderBackend policyFileBackend = new SimpleFileProviderBackend(conf,
        new org.apache.hadoop.fs.Path(policyFilePath.toUri()));
    policyFileBackend.initialize(new ProviderBackendContext());
    Table<String, String, Set<String>> groupRolePrivilegeTable =
        policyFileBackend.getGroupRolePrivilegeTable();

    assertEquals(groups, groupRolePrivilegeTable.rowKeySet());
    assertEquals(privilegeMapping.keySet(), groupRolePrivilegeTable.columnKeySet());

    for (String groupName : groupRolePrivilegeTable.rowKeySet()) {
      for (String roleName : groupRolePrivilegeTable.columnKeySet()) {
        if (groupRolePrivilegeTable.contains(groupName, roleName)) {
          Set<String> privileges = groupRolePrivilegeTable.get(groupName, roleName);
          assertEquals(privilegeMapping.get(roleName), privileges);
        }
      }
    }
  }
}
