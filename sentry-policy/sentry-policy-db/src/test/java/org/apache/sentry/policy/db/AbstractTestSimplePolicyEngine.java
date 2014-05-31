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
import java.util.Set;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.policy.common.PolicyEngine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public abstract class AbstractTestSimplePolicyEngine {
  private static final String PERM_SERVER1_CUSTOMERS_SELECT = "server=server1->db=customers->table=purchases->action=select";
  private static final String PERM_SERVER1_CUSTOMERS_DB_CUSTOMERS_PARTIAL_SELECT = "server=server1->db=customers->table=purchases_partial->action=select";
  private static final String PERM_SERVER1_ANALYST_ALL = "server=server1->db=analyst1";
  private static final String PERM_SERVER1_JUNIOR_ANALYST_ALL = "server=server1->db=jranalyst1";
  private static final String PERM_SERVER1_JUNIOR_ANALYST_READ = "server=server1->db=jranalyst1->table=*->action=select";
  private static final String PERM_SERVER1_OTHER_GROUP_DB_CUSTOMERS_SELECT = "server=server1->db=other_group_db->table=purchases->action=select";

  private static final String PERM_SERVER1_ADMIN = "server=server1";
  private PolicyEngine policy;
  private static File baseDir;

  @BeforeClass
  public static void setupClazz() throws IOException {
    baseDir = Files.createTempDir();
  }

  @AfterClass
  public static void teardownClazz() throws IOException {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  protected void setPolicy(PolicyEngine policy) {
    this.policy = policy;
  }
  protected static File getBaseDir() {
    return baseDir;
  }
  @Before
  public void setup() throws IOException {
    afterSetup();
  }
  @After
  public void teardown() throws IOException {
    beforeTeardown();
  }
  protected void afterSetup() throws IOException {

  }

  protected void beforeTeardown() throws IOException {

  }

  @Test
  public void testManager() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        PERM_SERVER1_CUSTOMERS_SELECT, PERM_SERVER1_ANALYST_ALL,
        PERM_SERVER1_JUNIOR_ANALYST_ALL, PERM_SERVER1_JUNIOR_ANALYST_READ,
        PERM_SERVER1_CUSTOMERS_DB_CUSTOMERS_PARTIAL_SELECT
        ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getAllPrivileges(set("manager"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        PERM_SERVER1_CUSTOMERS_SELECT, PERM_SERVER1_ANALYST_ALL,
        PERM_SERVER1_JUNIOR_ANALYST_READ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getAllPrivileges(set("analyst"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testJuniorAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets
        .newHashSet(PERM_SERVER1_JUNIOR_ANALYST_ALL,
            PERM_SERVER1_CUSTOMERS_DB_CUSTOMERS_PARTIAL_SELECT));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getAllPrivileges(set("jranalyst"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testAdmin() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(PERM_SERVER1_ADMIN));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getAllPrivileges(set("admin"), ActiveRoleSet.ALL))
        .toString());
  }


  @Test
  public void testOtherGroup() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        PERM_SERVER1_OTHER_GROUP_DB_CUSTOMERS_SELECT));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getAllPrivileges(set("other_group"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testDbAll() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets
        .newHashSet(PERM_SERVER1_JUNIOR_ANALYST_ALL,
            PERM_SERVER1_CUSTOMERS_DB_CUSTOMERS_PARTIAL_SELECT));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getAllPrivileges(set("jranalyst"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testDbAllforOtherGroup() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        PERM_SERVER1_OTHER_GROUP_DB_CUSTOMERS_SELECT));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getAllPrivileges(set("other_group"), ActiveRoleSet.ALL))
        .toString());
  }

  private static Set<String> set(String... values) {
    return Sets.newHashSet(values);
  }
}
