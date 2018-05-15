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

import com.google.common.io.Files;
import com.google.common.collect.Sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceIntegrationBase;
import org.apache.sentry.api.generic.thrift.TSentryRole;
import org.apache.sentry.api.generic.thrift.TSentryPrivilege;
import org.apache.sentry.core.common.exception.SentryConfigurationException;

import org.apache.sentry.api.tools.GenericPrivilegeConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSentryConfigToolSolr extends SentryGenericServiceIntegrationBase {
  private static String VALID_POLICY_INI = TestSentryConfigToolSolr.class.getClassLoader().getResource("solr_config_import_tool.ini").getPath();
  private static String INVALID_POLICY_INI = TestSentryConfigToolSolr.class.getClassLoader().getResource("solr_invalid.ini").getPath();
  private static String CASE_POLICY_INI = TestSentryConfigToolSolr.class.getClassLoader().getResource("solr_case.ini").getPath();
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
  public void testConvertIni() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String[] args = { "-p", VALID_POLICY_INI, "-conf", confPath.getAbsolutePath(), "-v", "-i"};
        SentryConfigToolSolr sentryTool = new SentryConfigToolSolr();
        sentryTool.executeConfigTool(args);

        Map<String, Set<String>> groupMapping = new HashMap<String, Set<String>>();
        groupMapping.put("corporal_role", Sets.newHashSet("corporal", "sergeant", "general", "commander_in_chief"));
        groupMapping.put("sergeant_role", Sets.newHashSet("sergeant", "general", "commander_in_chief"));
        groupMapping.put("general_role", Sets.newHashSet("general", "commander_in_chief"));
        groupMapping.put("commander_in_chief_role", Sets.newHashSet("commander_in_chief"));

        Map<String, Set<String>> privilegeMapping = new HashMap<String, Set<String>>();
        privilegeMapping.put("corporal_role",
            Sets.newHashSet("Collection=info->action=query", "Collection=info->action=update"));
        privilegeMapping.put("sergeant_role",
            Sets.newHashSet("Collection=info->action=update"));
        privilegeMapping.put("general_role",
            Sets.newHashSet("Collection=info->action=*"));
        privilegeMapping.put("commander_in_chief_role",
            Sets.newHashSet("Collection=*->action=*"));

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
          assertEquals("Privilege set size doesn't match for role: " + role,
              expectedPrivileges.size(), privileges.size());

          Set<String> privilegeStrs = new HashSet<String>();
          for (TSentryPrivilege privilege : privileges) {
            privilegeStrs.add(convert.toString(privilege));
          }

          for (String expectedPrivilege : expectedPrivileges) {
            assertTrue("Did not find expected privilege: " + expectedPrivilege + " in " + privilegeStrs,
                privilegeStrs.contains(expectedPrivilege));
          }
        }
      }
    });
  }

  @Test
  public void testNoPolicyFile() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String[] args = { "-p", INVALID_POLICY_INI + "Foobar", "-conf", confPath.getAbsolutePath(), "-v", "-i"};
        SentryConfigToolSolr sentryTool = new SentryConfigToolSolr();
        try {
          sentryTool.executeConfigTool(args);
          fail("Exception should be thrown for nonexistant ini");
        } catch (SentryConfigurationException e) {
          // expected exception
        }
      }
    });
  }

  @Test
  public void testNoValidateNorImport() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String[] args = { "-p", INVALID_POLICY_INI, "-conf", confPath.getAbsolutePath()};
        SentryConfigToolSolr sentryTool = new SentryConfigToolSolr();
        try {
          sentryTool.executeConfigTool(args);
          fail("Exception should be thrown for validating invalid ini");
        } catch (IllegalArgumentException e) {
          // expected exception
        }
      }
    });
  }

  @Test
  public void testConvertInvalidIni() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        // test: validate an invalid ini
        String[] args = { "-p", INVALID_POLICY_INI, "-conf", confPath.getAbsolutePath(), "-v", "-i"};
        SentryConfigToolSolr sentryTool = new SentryConfigToolSolr();
        try {
          sentryTool.executeConfigTool(args);
          fail("Exception should be thrown for validating invalid ini");
        } catch (SentryConfigurationException e) {
          // expected exception
        }

        // test without validating, should not error
        args = new String[] { "-p", INVALID_POLICY_INI, "-conf", confPath.getAbsolutePath(), "-i"};
        sentryTool = new SentryConfigToolSolr();
        sentryTool.executeConfigTool(args);
      }
    });
  }

  @Test
  public void testCompatCheck() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        // test: validate an invalid ini
        String[] args = { "-p", CASE_POLICY_INI, "-conf", confPath.getAbsolutePath(), "-v", "-i", "-c"};
        SentryConfigToolSolr sentryTool = new SentryConfigToolSolr();
        try {
          sentryTool.executeConfigTool(args);
          fail("Exception should be thrown for validating invalid ini");
        } catch (SentryConfigurationException e) {
          assertEquals("Expected error", 1, e.getConfigErrors().size());
          String error = e.getConfigErrors().get(0);
          assertCasedRoleNamesInMessage(error, "RoLe1", "rOlE1");
          String warning = e.getConfigWarnings().get(0);
          assertCasedRoleNamesInMessage(warning, "ROLE2", "RoLe1", "rOlE1");
          assertEquals("Expected warning", 1, e.getConfigWarnings().size());
        }

        // test without compat checking
        args = new String[] { "-p", CASE_POLICY_INI, "-conf", confPath.getAbsolutePath(), "-i", "-v"};
        sentryTool = new SentryConfigToolSolr();
        sentryTool.executeConfigTool(args);
      }
    });
  }

  // Test that a valid compat check doesn't throw an exception
  @Test
  public void testCompatCheckValid() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String[] args = { "-p", VALID_POLICY_INI, "-conf", confPath.getAbsolutePath(), "-v", "-i", "-c"};
        SentryConfigToolSolr sentryTool = new SentryConfigToolSolr();
        sentryTool.executeConfigTool(args);
      }
    });
  }

  private void assertCasedRoleNamesInMessage(String message, String ... casedRoleNames) {
    for (String casedRoleName : casedRoleNames) {
      assertTrue("Expected cased role name: " + casedRoleName, message.contains(casedRoleName));
    }
  }
}
