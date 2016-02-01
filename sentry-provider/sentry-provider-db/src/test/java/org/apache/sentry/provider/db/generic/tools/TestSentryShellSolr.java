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

package org.apache.sentry.provider.db.generic.tools;

import com.google.common.io.Files;
import com.google.common.collect.Sets;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Set;
import javax.security.auth.Subject;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceIntegrationBase;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.tools.SentryShellCommon;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSentryShellSolr extends SentryGenericServiceIntegrationBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestSentryShellSolr.class);
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
    requestorName = System.getProperty("user.name", "");
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
        String[] args = { "-cr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath() };
        SentryShellSolr.main(args);
        // test: create role with --create_role
        args = new String[] { "--create_role", "-r", TEST_ROLE_NAME_2, "-conf",
            confPath.getAbsolutePath() };
        SentryShellSolr.main(args);

        // validate the result, list roles with -lr
        args = new String[] { "-lr", "-conf", confPath.getAbsolutePath() };
        SentryShellSolr sentryShell = new SentryShellSolr();
        Set<String> roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        assertEquals("Incorrect number of roles", 2, roleNames.size());
        for (String roleName : roleNames) {
          assertTrue(TEST_ROLE_NAME_1.equalsIgnoreCase(roleName)
              || TEST_ROLE_NAME_2.equalsIgnoreCase(roleName));
        }

        // validate the result, list roles with --list_role
        args = new String[] { "--list_role", "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        roleNames = getShellResultWithOSRedirect(sentryShell, args, true);
        assertEquals("Incorrect number of roles", 2, roleNames.size());
        for (String roleName : roleNames) {
          assertTrue(TEST_ROLE_NAME_1.equalsIgnoreCase(roleName)
              || TEST_ROLE_NAME_2.equalsIgnoreCase(roleName));
        }

        // test: drop role with -dr
        args = new String[] { "-dr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath() };
        SentryShellSolr.main(args);
        // test: drop role with --drop_role
        args = new String[] { "--drop_role", "-r", TEST_ROLE_NAME_2, "-conf",
            confPath.getAbsolutePath() };
        SentryShellSolr.main(args);

        // validate the result
        Set<TSentryRole> roles = client.listAllRoles(requestorName, SOLR);
        assertEquals("Incorrect number of roles", 0, roles.size());
      }
    });
  }

  // this is not supported, just check that all the permutations
  // give a reasonable error
  @Test
  public void testAddDeleteRoleForGroup() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
         // test: add role to multiple groups
        String[] args = new String[] { "-arg", "-r", TEST_ROLE_NAME_1, "-g", "testGroup2,testGroup3",
            "-conf",
            confPath.getAbsolutePath() };
        SentryShellSolr sentryShell = new SentryShellSolr();
        try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
          // expected
        }

        // test: add role to group with --add_role_group
        args = new String[] { "--add_role_group", "-r", TEST_ROLE_NAME_2, "-g", "testGroup1",
            "-conf",
            confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
          // expected
        }

        args = new String[] { "-lr", "-g", "testGroup1", "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
          // expected
        }

        // list roles with --list_role and -g
        args = new String[] { "--list_role", "-g", "testGroup2", "-conf",
            confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
          // expected
        }

        // test: delete group from role with -drg
        args = new String[] { "-drg", "-r", TEST_ROLE_NAME_1, "-g", "testGroup1", "-conf",
            confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
          // expected
        }

        args = new String[] { "-drg", "-r", TEST_ROLE_NAME_1, "-g", "testGroup2,testGroup3",
            "-conf",
            confPath.getAbsolutePath() };
        try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
          // expected
        }

        // test: delete group from role with --delete_role_group
        args = new String[] { "--delete_role_group", "-r", TEST_ROLE_NAME_2, "-g", "testGroup1",
            "-conf", confPath.getAbsolutePath() };
        try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
          // expected
        }
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
            "-conf", confPath.getAbsolutePath() };
          SentryShellSolr.main(args);
        }

        // test the list privilege
        String [] args = new String[] { list(shortOption), "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath() };
        SentryShellSolr sentryShell = new SentryShellSolr();
        Set<String> privilegeStrs = getShellResultWithOSRedirect(sentryShell, args, true);
        assertEquals("Incorrect number of privileges", privs.length, privilegeStrs.size());
        for (int i = 0; i < privs.length; ++i) {
          assertTrue("Expected privilege: " + privs[ i ], privilegeStrs.contains(privs[ i ]));
        }

        for (int i = 0; i < privs.length; ++i) {
          args = new String[] { revoke(shortOption), "-r", TEST_ROLE_NAME_1, "-p",
            privs[ i ], "-conf",
            confPath.getAbsolutePath() };
          SentryShellSolr.main(args);
          Set<TSentryPrivilege> privileges = client.listPrivilegesByRoleName(requestorName,
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
        String[] args = { "-cr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath() };
        SentryShellSolr sentryShell = new SentryShellSolr();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for creating duplicate role");
        } catch (SentryUserException e) {
          // expected exception
        }

        // test: drop non-exist role with -dr
        args = new String[] { "-dr", "-r", TEST_ROLE_NAME_2, "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for dropping non-exist role");
        } catch (SentryUserException e) {
          // excepted exception
        }

        // test: grant privilege to role with the error privilege format
        args = new String[] { "-gpr", "-r", TEST_ROLE_NAME_1, "-p", "serverserver1->action=*",
            "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        try {
          sentryShell.executeShell(args);
          fail("Exception should be thrown for the error privilege format, invalid key value.");
        } catch (IllegalArgumentException e) {
          // excepted exception
        }

        // test: grant privilege to role with the error privilege hierarchy
        args = new String[] { "-gpr", "-r", TEST_ROLE_NAME_1, "-p",
            "server=server1->table=tbl1->column=col2->action=insert", "-conf",
            confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
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
        String[] args = { "-cr", "-r", TEST_ROLE_NAME_1 };
        SentryShellSolr sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + strOptionConf);

        // test: -r is required when create role
        args = new String[] { "-cr", "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -r is required when drop role
        args = new String[] { "-dr", "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -r is required when add group to role
        args = new String[] { "-arg", "-g", "testGroup1", "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -g is required when add group to role
        args = new String[] { "-arg", "-r", TEST_ROLE_NAME_2, "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_GROUP_NAME);

        // test: -r is required when delete group from role
        args = new String[] { "-drg", "-g", "testGroup1", "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -g is required when delete group from role
        args = new String[] { "-drg", "-r", TEST_ROLE_NAME_2, "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_GROUP_NAME);

        // test: -r is required when grant privilege to role
        args = new String[] { "-gpr", "-p", "server=server1", "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -p is required when grant privilege to role
        args = new String[] { "-gpr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_PRIVILEGE);

        // test: action is required in privilege
        args = new String[] { "-gpr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath(), "-p", "collection=collection1" };
        sentryShell = new SentryShellSolr();
         try {
          getShellResultWithOSRedirect(sentryShell, args, false);
          fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
          assert("Privilege is invalid: action required but not specified.".equals(e.getMessage()));
        }

        // test: -r is required when revoke privilege from role
        args = new String[] { "-rpr", "-p", "server=server1", "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_ROLE_NAME);

        // test: -p is required when revoke privilege from role
        args = new String[] { "-rpr", "-r", TEST_ROLE_NAME_1, "-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsg(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + SentryShellCommon.OPTION_DESC_PRIVILEGE);

        // test: command option is required for shell
        args = new String[] {"-conf", confPath.getAbsolutePath() };
        sentryShell = new SentryShellSolr();
        validateMissingParameterMsgsContains(sentryShell, args,
                SentryShellCommon.PREFIX_MESSAGE_MISSING_OPTION + "[",
                "-arg Add group to role",
                "-cr Create role",
                "-rpr Revoke privilege from role",
                "-drg Delete group from role",
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
  private Set<String> getShellResultWithOSRedirect(SentryShellSolr sentryShell,
      String[] args, boolean expectedExecuteResult) throws Exception {
    PrintStream oldOut = System.out;
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    assertEquals(expectedExecuteResult, sentryShell.executeShell(args));
    Set<String> resultSet = Sets.newHashSet(outContent.toString().split("\n"));
    System.setOut(oldOut);
    return resultSet;
  }

  private void validateMissingParameterMsg(SentryShellSolr sentryShell, String[] args,
      String expectedErrorMsg) throws Exception {
    Set<String> errorMsgs = getShellResultWithOSRedirect(sentryShell, args, false);
    assertTrue("Expected error message: " + expectedErrorMsg, errorMsgs.contains(expectedErrorMsg));
  }

  private void validateMissingParameterMsgsContains(SentryShellSolr sentryShell, String[] args,
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
