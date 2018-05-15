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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceIntegrationBase;
import org.apache.sentry.api.generic.thrift.TSentryRole;
import org.apache.sentry.api.generic.thrift.TSentryPrivilege;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSentryShellSolr extends SentryGenericServiceIntegrationBase {
  private File confDir;
  private File confPath;
  private static String TEST_ROLE_NAME_1 = "testRole1";
  private static String TEST_ROLE_NAME_2 = "testRole2";
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
  }

  @Test
  public void testCreateDropRole() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        // test: create role with -cr
        String[] args = { "-cr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);
        // test: create role with --create_role
        args = new String[] { "--create_role", "-r", TEST_ROLE_NAME_2, "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);

        // validate the result, list roles with -lr
        args = new String[] { "-lr", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric sentryShell = new SentryShellGeneric();
        Set<String> roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        validateRoleNames(roleNames, TEST_ROLE_NAME_1, TEST_ROLE_NAME_2);

        // validate the result, list roles with --list_role
        args = new String[] { "--list_role", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        validateRoleNames(roleNames, TEST_ROLE_NAME_1, TEST_ROLE_NAME_2);

        // test: drop role with -dr
        args = new String[] { "-dr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);
        // test: drop role with --drop_role
        args = new String[] { "--drop_role", "-r", TEST_ROLE_NAME_2, "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);

        // validate the result
        Set<TSentryRole> roles = client.listAllRoles(requestorName, SOLR);
        assertEquals("Incorrect number of roles", 0, roles.size());
      }
    });
  }

  @Test
  public void testAddDeleteRoleForGroup() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        // Group names are case sensitive - mixed case names should work
        String TEST_GROUP_1 = "testGroup1";
        String TEST_GROUP_2 = "testGroup2";
        String TEST_GROUP_3 = "testGroup3";

        // create the role for test
        client.createRole(requestorName, TEST_ROLE_NAME_1, SOLR);
        client.createRole(requestorName, TEST_ROLE_NAME_2, SOLR);
        // test: add role to group with -arg
        String[] args = { "-arg", "-r", TEST_ROLE_NAME_1, "-g", TEST_GROUP_1, "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);
        // test: add role to multiple groups
        args = new String[] { "-arg", "-r", TEST_ROLE_NAME_1, "-g", TEST_GROUP_2 + "," + TEST_GROUP_3,
            "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);
        // test: add role to group with --add_role_group
        args = new String[] { "--add_role_group", "-r", TEST_ROLE_NAME_2, "-g", TEST_GROUP_1,
            "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);

        // validate the result list roles with -lr and -g
        args = new String[] { "-lr", "-g", TEST_GROUP_1, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric sentryShell = new SentryShellGeneric();
        Set<String> roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        validateRoleNames(roleNames, TEST_ROLE_NAME_1, TEST_ROLE_NAME_2);

        // list roles with --list_role and -g
        args = new String[] { "--list_role", "-g", TEST_GROUP_2, "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        validateRoleNames(roleNames, TEST_ROLE_NAME_1);

        args = new String[] { "--list_role", "-g", TEST_GROUP_3, "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        validateRoleNames(roleNames, TEST_ROLE_NAME_1);

        // List the groups and roles via listGroups
        args = new String[] { "--list_group", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        Set<String> groups = getShellResultWithOSRedirect(sentryShell, args, true);
        assertEquals(3, groups.size());
        assertTrue(groups.contains("testGroup3 = testrole1"));
        assertTrue(groups.contains("testGroup2 = testrole1"));
        assertTrue(groups.contains("testGroup1 = testrole2, testrole1"));

        // test: delete role from group with -drg
        args = new String[] { "-drg", "-r", TEST_ROLE_NAME_1, "-g", TEST_GROUP_1, "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);
        // test: delete role to multiple groups
        args = new String[] { "-drg", "-r", TEST_ROLE_NAME_1, "-g", TEST_GROUP_2 + "," + TEST_GROUP_3,
            "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);
        // test: delete role from group with --delete_role_group
        args = new String[] { "--delete_role_group", "-r", TEST_ROLE_NAME_2, "-g", TEST_GROUP_1,
            "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);

        // validate the result
        Set<TSentryRole> roles = client.listRolesByGroupName(requestorName, TEST_GROUP_1, SOLR);
        assertEquals("Incorrect number of roles", 0, roles.size());
        roles = client.listRolesByGroupName(requestorName, TEST_GROUP_2, SOLR);
        assertEquals("Incorrect number of roles", 0, roles.size());
        roles = client.listRolesByGroupName(requestorName, TEST_GROUP_3, SOLR);
        assertEquals("Incorrect number of roles", 0, roles.size());
        // clear the test data
        client.dropRole(requestorName, TEST_ROLE_NAME_1, SOLR);
        client.dropRole(requestorName, TEST_ROLE_NAME_2, SOLR);
      }
    });
  }

  @Test
  public void testCaseSensitiveGroupName() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {

        // create the role for test
        client.createRole(requestorName, TEST_ROLE_NAME_1, SOLR);
        // add role to a group (lower case)
        String[] args = { "-arg", "-r", TEST_ROLE_NAME_1, "-g", "group1", "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric.main(args);

        // validate the roles when group name is same case as above
        args = new String[] { "-lr", "-g", "group1", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric sentryShell = new SentryShellGeneric();
        Set<String> roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        validateRoleNames(roleNames, TEST_ROLE_NAME_1);

        // roles should be empty when group name is different case than above
        args = new String[] { "-lr", "-g", "GROUP1", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        validateRoleNames(roleNames);
      }
      });
    }

  public static String grant(boolean shortOption) {
    return shortOption ? "-gpr" : "--grant_privilege_role";
  }

  public static String revoke(boolean shortOption) {
    return shortOption ? "-rpr" : "--revoke_privilege_role";
  }

  public static String list(boolean shortOption) {
    return shortOption ? "-lp" : "--list_privilege";
  }

  private void assertGrantRevokePrivilege(final boolean shortOption) throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        // create the role for test
        client.createRole(requestorName, TEST_ROLE_NAME_1, SOLR);
        client.createRole(requestorName, TEST_ROLE_NAME_2, SOLR);

        String [] privs = {
          "Collection=*->action=*",
          "Collection=collection2->action=update",
          "Collection=collection3->action=query",
        };
        for (int i = 0; i < privs.length; ++i) {
          // test: grant privilege to role
          String [] args = new String [] { grant(shortOption), "-r", TEST_ROLE_NAME_1, "-p",
            privs[ i ],
            "-conf", confPath.getAbsolutePath(), "-t", "solr" };
          SentryShellGeneric.main(args);
        }

        // test the list privilege
        String [] args = new String[] { list(shortOption), "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric sentryShell = new SentryShellGeneric();
        Set<String> privilegeStrs = getShellResultWithOSRedirect(sentryShell, args, true);
        assertEquals("Incorrect number of privileges", privs.length, privilegeStrs.size());
        for (int i = 0; i < privs.length; ++i) {
          assertTrue("Expected privilege: " + privs[ i ], privilegeStrs.contains(privs[ i ]));
        }

        for (int i = 0; i < privs.length; ++i) {
          args = new String[] { revoke(shortOption), "-r", TEST_ROLE_NAME_1, "-p",
            privs[ i ], "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
          SentryShellGeneric.main(args);
          Set<TSentryPrivilege> privileges = client.listAllPrivilegesByRoleName(requestorName,
            TEST_ROLE_NAME_1, SOLR, service);
          assertEquals("Incorrect number of privileges", privs.length - (i + 1), privileges.size());
        }

        // clear the test data
        client.dropRole(requestorName, TEST_ROLE_NAME_1, SOLR);
        client.dropRole(requestorName, TEST_ROLE_NAME_2, SOLR);
      }
    });
  }


  @Test
  public void testGrantRevokePrivilegeWithShortOption() throws Exception {
    assertGrantRevokePrivilege(true);
  }

  @Test
  public void testGrantRevokePrivilegeWithLongOption() throws Exception {
    assertGrantRevokePrivilege(false);
  }


  @Test
  public void testNegativeCaseWithInvalidArgument() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        client.createRole(requestorName, TEST_ROLE_NAME_1, SOLR);
        // test: create duplicate role with -cr
        String[] args = { "-cr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        SentryShellGeneric sentryShell = new SentryShellGeneric();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for creating duplicate role");
        } catch (SentryUserException e) {
          // expected exception
        }

        // test: drop non-exist role with -dr
        args = new String[] { "-dr", "-r", TEST_ROLE_NAME_2, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for dropping non-exist role");
        } catch (SentryUserException e) {
          // excepted exception
        }

        // test: add non-exist role to group with -arg
        args = new String[] { "-arg", "-r", TEST_ROLE_NAME_2, "-g", "testGroup1", "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for granting non-exist role to group");
        } catch (SentryUserException e) {
          // excepted exception
        }

        // test: drop group from non-exist role with -drg
        args = new String[] { "-drg", "-r", TEST_ROLE_NAME_2, "-g", "testGroup1", "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for drop group from non-exist role");
        } catch (SentryUserException e) {
          // excepted exception
        }

        // test: grant privilege to role with the error privilege format
        args = new String[] { "-gpr", "-r", TEST_ROLE_NAME_1, "-p", "serverserver1->action=*",
            "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for the error privilege format, invalid key value.");
        } catch (IllegalArgumentException e) {
          // excepted exception
        }

        // test: grant privilege to role with the error privilege hierarchy
        args = new String[] { "-gpr", "-r", TEST_ROLE_NAME_1, "-p",
            "server=server1->table=tbl1->column=col2->action=insert", "-conf",
            confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for the error privilege format, invalid key value.");
        } catch (IllegalArgumentException e) {
          // expected exception
        }

        // clear the test data
        client.dropRole(requestorName, TEST_ROLE_NAME_1, SOLR);
      }
    });
  }

  @Test
  public void testNegativeCaseWithoutRequiredArgument() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String strOptionConf = "conf";
        client.createRole(requestorName, TEST_ROLE_NAME_1, SOLR);
        // test: the conf is required argument
        String[] args = { "-cr", "-r", TEST_ROLE_NAME_1, "-t", "solr" };
        SentryShellGeneric sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + strOptionConf);

        // test: -r is required when create role
        args = new String[] { "-cr", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -r is required when drop role
        args = new String[] { "-dr", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -r is required when add role to group
        args = new String[] { "-arg", "-g", "testGroup1", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -g is required when add role to group
        args = new String[] { "-arg", "-r", TEST_ROLE_NAME_2, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_GROUP_NAME);

        // test: -r is required when delete role from group
        args = new String[] { "-drg", "-g", "testGroup1", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -g is required when delete role from group
        args = new String[] { "-drg", "-r", TEST_ROLE_NAME_2, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_GROUP_NAME);

        // test: -r is required when grant privilege to role
        args = new String[] { "-gpr", "-p", "server=server1", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -p is required when grant privilege to role
        args = new String[] { "-gpr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_PRIVILEGE);

        // test: action is required in privilege
        args = new String[] { "-gpr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath(), "-p", "collection=collection1", "-t", "solr" };
        sentryShell = new SentryShellGeneric();
         try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
          assertEquals("Privilege is invalid: action required but not specified.", e.getMessage());
        }

        // test: -r is required when revoke privilege from role
        args = new String[] { "-rpr", "-p", "server=server1", "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -p is required when revoke privilege from role
        args = new String[] { "-rpr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath(), "-t", "solr" };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_PRIVILEGE);

        // test: command option is required for shell
        args = new String[] {"-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellGeneric();
        validateMissingParameterMsgsContains(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + "[",
                "-arg Add role to group",
                "-cr Create role",
                "-rpr Revoke privilege from role",
                "-drg Delete role from group",
                "-lr List role",
                "-lp List privilege",
                "-gpr Grant privilege to role",
                "-dr Drop role");

        // clear the test data
        client.dropRole(requestorName, TEST_ROLE_NAME_1, SOLR);
      }
    });
  }

  // redirect the System.out to ByteArrayOutputStream, then execute the command and parse the result.
  private Set<String> getShellResultWithOSRedirect(SentryShellGeneric sentryShell,
      String[] args, boolean expectedExecuteResult) throws Exception {
    PrintStream oldOut = System.out;
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    assertEquals(expectedExecuteResult, sentryShell.executeShell(args));
    Set<String> resultSet = Sets.newHashSet(outContent.toString().split("\n"));
    System.setOut(oldOut);
    return resultSet;
  }

  private void validateRoleNames(Set<String> roleNames, String ... expectedRoleNames) {
    if (expectedRoleNames != null && expectedRoleNames.length > 0) {
      assertEquals("Found: " + roleNames.size() + " roles, expected: " + expectedRoleNames.length,
          expectedRoleNames.length, roleNames.size());
      Set<String> lowerCaseRoles = new HashSet<String>();
      for (String role : roleNames) {
        lowerCaseRoles.add(role.toLowerCase());
      }

      for (String expectedRole : expectedRoleNames) {
        assertTrue("Expected role: " + expectedRole,
            lowerCaseRoles.contains(expectedRole.toLowerCase()));
      }
    }
  }

  private void validateMissingParameterMsg(SentryShellGeneric sentryShell, String[] args,
      String expectedErrorMsg) throws Exception {
    Set<String> errorMsgs = getShellResultWithOSRedirect(sentryShell, args, false);
    assertTrue("Expected error message: " + expectedErrorMsg, errorMsgs.contains(expectedErrorMsg));
  }

  private void validateMissingParameterMsgsContains(SentryShellGeneric sentryShell, String[] args,
      String ... expectedErrorMsgsContains) throws Exception {
    Set<String> errorMsgs = getShellResultWithOSRedirect(sentryShell, args, false);
    boolean foundAllMessages = false;
    Iterator<String> it = errorMsgs.iterator();
    while (it.hasNext()) {
      String errorMessage = it.next();
      boolean missingExpected = false;
      for (String expectedContains : expectedErrorMsgsContains) {
        if (!errorMessage.contains(expectedContains)) {
          missingExpected = true;
          break;
        }
      }
      if (!missingExpected) {
        foundAllMessages = true;
        break;
      }
    }
    assertTrue(foundAllMessages);
  }
}
