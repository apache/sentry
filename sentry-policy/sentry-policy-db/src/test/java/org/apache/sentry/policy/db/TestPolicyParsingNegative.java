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
package org.apache.sentry.policy.db;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.file.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestPolicyParsingNegative {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestPolicyParsingNegative.class);

  private File baseDir;
  private File globalPolicyFile;
  private File otherPolicyFile;

  @Before
  public void setup() {
    baseDir = Files.createTempDir();
    globalPolicyFile = new File(baseDir, "global.ini");
    otherPolicyFile = new File(baseDir, "other.ini");
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  private void append(String from, File to) throws IOException {
    Files.append(from + "\n", to, Charsets.UTF_8);
  }

  @Test
  public void testUnauthorizedDbSpecifiedInDBPolicyFile() throws Exception {
    append("[databases]", globalPolicyFile);
    append("other_group_db = " + otherPolicyFile.getPath(), globalPolicyFile);
    append("[groups]", otherPolicyFile);
    append("other_group = malicious_role", otherPolicyFile);
    append("[roles]", otherPolicyFile);
    append("malicious_role = server=server1->db=customers->table=purchases->action=select", otherPolicyFile);
    PolicyEngine policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPrivileges(Sets.newHashSet("other_group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }
  @Test
  public void testPerDbFileCannotContainUsersOrDatabases() throws Exception {
    PolicyEngine policy;
    ImmutableSet<String> permissions;
    PolicyFile policyFile;
    // test sanity
    policyFile = PolicyFile.setAdminOnServer1("admin");
    policyFile.addGroupsToUser("admin1", "admin");
    policyFile.write(globalPolicyFile);
    policyFile.write(otherPolicyFile);
    policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());
    permissions = policy.getPrivileges(Sets.newHashSet("admin"), ActiveRoleSet.ALL);
    Assert.assertEquals(permissions.toString(), "[server=server1]");
    // test to ensure [users] fails parsing of per-db file
    policyFile.addDatabase("other", otherPolicyFile.getPath());
    policyFile.write(globalPolicyFile);
    policyFile.write(otherPolicyFile);
    policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());
    permissions = policy.getPrivileges(Sets.newHashSet("admin"), ActiveRoleSet.ALL);
    Assert.assertEquals(permissions.toString(), "[server=server1]");
    // test to ensure [databases] fails parsing of per-db file
    // by removing the user mapping from the per-db policy file
    policyFile.removeGroupsFromUser("admin1", "admin")
      .write(otherPolicyFile);
    policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());
    permissions = policy.getPrivileges(Sets.newHashSet("admin"), ActiveRoleSet.ALL);
    Assert.assertEquals(permissions.toString(), "[server=server1]");
  }

  @Test
  public void testDatabaseRequiredInRole() throws Exception {
    append("[databases]", globalPolicyFile);
    append("other_group_db = " + otherPolicyFile.getPath(), globalPolicyFile);
    append("[groups]", otherPolicyFile);
    append("other_group = malicious_role", otherPolicyFile);
    append("[roles]", otherPolicyFile);
    append("malicious_role = server=server1", otherPolicyFile);
    PolicyEngine policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPrivileges(Sets.newHashSet("other_group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  @Test
  public void testServerAll() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = server=*", globalPolicyFile);
    PolicyEngine policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPrivileges(Sets.newHashSet("group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  @Test
  public void testServerIncorrect() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = server=server2", globalPolicyFile);
    PolicyEngine policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPrivileges(Sets.newHashSet("group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  @Test
  public void testAll() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = *", globalPolicyFile);
    PolicyEngine policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPrivileges(Sets.newHashSet("group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  /**
   * Create policy file with multiple per db files.
   * Verify that a file with bad format is the only one that's ignored
   * @throws Exception
   */
  @Test
  public void testMultiDbWithErrors() throws Exception {
    File db1PolicyFile = new File(baseDir, "db1.ini");
    File db2PolicyFile = new File(baseDir, "db2.ini");

    // global policy file
    append("[databases]", globalPolicyFile);
    append("db1 = " + db1PolicyFile.getPath(), globalPolicyFile);
    append("db2 = " + db2PolicyFile.getPath(), globalPolicyFile);
    append("[groups]", globalPolicyFile);
    append("db3_group = db3_rule", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("db3_rule = server=server1->db=db3->table=sales->action=select", globalPolicyFile);

    //db1 policy file with badly formatted rule
    append("[groups]", db1PolicyFile);
    append("db1_group = bad_rule", db1PolicyFile);
    append("[roles]", db1PolicyFile);
    append("bad_rule = server=server1->db=customers->=purchases->action=", db1PolicyFile);

    //db2 policy file with proper rule
    append("[groups]", db2PolicyFile);
    append("db2_group = db2_rule", db2PolicyFile);
    append("[roles]", db2PolicyFile);
    append("db2_rule = server=server1->db=db2->table=purchases->action=select", db2PolicyFile);

    PolicyEngine policy = new DBPolicyFileBackend("server1", globalPolicyFile.getPath());

    // verify that the db1 rule is empty
    ImmutableSet<String> permissions = policy.getPrivileges(Sets.newHashSet("db1_group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());

    permissions = policy.getPrivileges(Sets.newHashSet("db2_group"), ActiveRoleSet.ALL);
    Assert.assertEquals(permissions.toString(), 1, permissions.size());
  }
}
