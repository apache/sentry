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
package org.apache.sentry.policy.search;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.policy.common.PolicyEngine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public abstract class AbstractTestSearchPolicyEngine {
  private static final String ANALYST_PURCHASES_UPDATE = "collection=purchases->action=update";
  private static final String ANALYST_ANALYST1_ALL = "collection=analyst1";
  private static final String ANALYST_JRANALYST1_ACTION_ALL = "collection=jranalyst1->action=*";
  private static final String ANALYST_TMPCOLLECTION_UPDATE = "collection=tmpcollection->action=update";
  private static final String ANALYST_TMPCOLLECTION_QUERY = "collection=tmpcollection->action=query";
  private static final String JRANALYST_JRANALYST1_ALL = "collection=jranalyst1";
  private static final String JRANALYST_PURCHASES_PARTIAL_QUERY = "collection=purchases_partial->action=query";
  private static final String ADMIN_COLLECTION_ALL = "collection=*";

  private PolicyEngine policy;
  private static File baseDir;
  private List<Authorizable> authorizables = Lists.newArrayList();

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
        ANALYST_PURCHASES_UPDATE, ANALYST_ANALYST1_ALL,
        ANALYST_JRANALYST1_ACTION_ALL, ANALYST_TMPCOLLECTION_UPDATE,
        ANALYST_TMPCOLLECTION_QUERY, JRANALYST_JRANALYST1_ALL,
        JRANALYST_PURCHASES_PARTIAL_QUERY));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPermissions(authorizables, list("manager")).values())
        .toString());
  }

  @Test
  public void testAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        ANALYST_PURCHASES_UPDATE, ANALYST_ANALYST1_ALL,
        ANALYST_JRANALYST1_ACTION_ALL, ANALYST_TMPCOLLECTION_UPDATE,
        ANALYST_TMPCOLLECTION_QUERY));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPermissions(authorizables, list("analyst")).values())
        .toString());
  }

  @Test
  public void testJuniorAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets
        .newHashSet(JRANALYST_JRANALYST1_ALL,
            JRANALYST_PURCHASES_PARTIAL_QUERY));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPermissions(authorizables, list("jranalyst")).values())
        .toString());
  }

  @Test
  public void testAdmin() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(ADMIN_COLLECTION_ALL));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPermissions(authorizables, list("admin")).values())
        .toString());
  }

  private static List<String> list(String... values) {
    return Lists.newArrayList(values);
  }
}
