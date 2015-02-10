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
package org.apache.sentry.policy.indexer;

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

public abstract class AbstractTestIndexerPolicyEngine {
  private static final String ANALYST_PURCHASES_WRITE = "indexer=purchases->action=write";
  private static final String ANALYST_ANALYST1_ALL = "indexer=analyst1";
  private static final String ANALYST_JRANALYST1_ACTION_ALL = "indexer=jranalyst1->action=*";
  private static final String ANALYST_TMPINDEXER_WRITE = "indexer=tmpindexer->action=write";
  private static final String ANALYST_TMPINDEXER_READ = "indexer=tmpindexer->action=read";
  private static final String JRANALYST_JRANALYST1_ALL = "indexer=jranalyst1";
  private static final String JRANALYST_PURCHASES_PARTIAL_READ = "indexer=purchases_partial->action=read";
  private static final String ADMIN_INDEXER_ALL = "indexer=*";

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
        ANALYST_PURCHASES_WRITE, ANALYST_ANALYST1_ALL,
        ANALYST_JRANALYST1_ACTION_ALL, ANALYST_TMPINDEXER_WRITE,
        ANALYST_TMPINDEXER_READ, JRANALYST_JRANALYST1_ALL,
        JRANALYST_PURCHASES_PARTIAL_READ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("manager"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        ANALYST_PURCHASES_WRITE, ANALYST_ANALYST1_ALL,
        ANALYST_JRANALYST1_ACTION_ALL, ANALYST_TMPINDEXER_WRITE,
        ANALYST_TMPINDEXER_READ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("analyst"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testJuniorAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets
        .newHashSet(JRANALYST_JRANALYST1_ALL,
            JRANALYST_PURCHASES_PARTIAL_READ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("jranalyst"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testAdmin() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(ADMIN_INDEXER_ALL));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("admin"), ActiveRoleSet.ALL))
        .toString());
  }

  private static Set<String> set(String... values) {
    return Sets.newHashSet(values);
  }
}
