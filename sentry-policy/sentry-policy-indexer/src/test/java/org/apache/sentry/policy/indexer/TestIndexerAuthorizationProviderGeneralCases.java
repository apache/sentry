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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.indexer.Indexer;
import org.apache.sentry.core.model.indexer.IndexerModelAction;
import org.apache.sentry.provider.common.MockGroupMappingServiceProvider;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.provider.file.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;


public class TestIndexerAuthorizationProviderGeneralCases {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestIndexerAuthorizationProviderGeneralCases.class);

  private static final Multimap<String, String> USER_TO_GROUP_MAP = HashMultimap
      .create();

  private static final Subject SUB_ADMIN = new Subject("admin1");
  private static final Subject SUB_MANAGER = new Subject("manager1");
  private static final Subject SUB_ANALYST = new Subject("analyst1");
  private static final Subject SUB_JUNIOR_ANALYST = new Subject("jranalyst1");

  private static final Indexer IND_PURCHASES = new Indexer("purchases");
  private static final Indexer IND_ANALYST1 = new Indexer("analyst1");
  private static final Indexer IND_JRANALYST1 = new Indexer("jranalyst1");
  private static final Indexer IND_TMP = new Indexer("tmpindexer");
  private static final Indexer IND_PURCHASES_PARTIAL = new Indexer("purchases_partial");

  private static final IndexerModelAction ALL = IndexerModelAction.ALL;
  private static final IndexerModelAction READ = IndexerModelAction.READ;
  private static final IndexerModelAction WRITE = IndexerModelAction.WRITE;

  static {
    USER_TO_GROUP_MAP.putAll(SUB_ADMIN.getName(), Arrays.asList("admin"));
    USER_TO_GROUP_MAP.putAll(SUB_MANAGER.getName(), Arrays.asList("manager"));
    USER_TO_GROUP_MAP.putAll(SUB_ANALYST.getName(), Arrays.asList("analyst"));
    USER_TO_GROUP_MAP.putAll(SUB_JUNIOR_ANALYST.getName(),
        Arrays.asList("jranalyst"));
  }

  private final ResourceAuthorizationProvider authzProvider;
  private File baseDir;

  public TestIndexerAuthorizationProviderGeneralCases() throws IOException {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, "test-authz-provider.ini");
    authzProvider = new HadoopGroupResourceAuthorizationProvider(
        new IndexerPolicyFileBackend(new File(baseDir, "test-authz-provider.ini").getPath()),
        new MockGroupMappingServiceProvider(USER_TO_GROUP_MAP));

  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  private void doTestAuthProviderOnIndexer(Subject subject,
      Indexer indexer, Set<? extends Action> expectedPass) throws Exception {
    Set<IndexerModelAction> allActions = EnumSet.of(IndexerModelAction.ALL, IndexerModelAction.READ, IndexerModelAction.WRITE);
    for(IndexerModelAction action : allActions) {
      doTestResourceAuthorizationProvider(subject, indexer,
        EnumSet.of(action), expectedPass.contains(action));
    }
  }

  private void doTestResourceAuthorizationProvider(Subject subject,
      Indexer indexer,
      Set<? extends Action> privileges, boolean expected) throws Exception {
    List<Authorizable> authzHierarchy = Arrays.asList(new Authorizable[] {
        indexer
    });
    Objects.ToStringHelper helper = Objects.toStringHelper("TestParameters");
    helper.add("Subject", subject).add("Indexer", indexer)
      .add("Privileges", privileges).add("authzHierarchy", authzHierarchy);
    LOGGER.info("Running with " + helper.toString());
    Assert.assertEquals(helper.toString(), expected,
        authzProvider.hasAccess(subject, authzHierarchy, privileges, ActiveRoleSet.ALL));
    LOGGER.info("Passed " + helper.toString());
  }

  @Test
  public void testAdmin() throws Exception {
    Set<IndexerModelAction> allActions = EnumSet.allOf(IndexerModelAction.class);
    doTestAuthProviderOnIndexer(SUB_ADMIN, IND_PURCHASES, allActions);
    doTestAuthProviderOnIndexer(SUB_ADMIN, IND_ANALYST1, allActions);
    doTestAuthProviderOnIndexer(SUB_ADMIN, IND_JRANALYST1, allActions);
    doTestAuthProviderOnIndexer(SUB_ADMIN, IND_TMP, allActions);
    doTestAuthProviderOnIndexer(SUB_ADMIN, IND_PURCHASES_PARTIAL, allActions);
  }

  @Test
  public void testManager() throws Exception {
    Set<IndexerModelAction> writeOnly = EnumSet.of(IndexerModelAction.WRITE);
    doTestAuthProviderOnIndexer(SUB_MANAGER, IND_PURCHASES, writeOnly);

    Set<IndexerModelAction> allActions = EnumSet.allOf(IndexerModelAction.class);
    doTestAuthProviderOnIndexer(SUB_MANAGER, IND_ANALYST1, allActions);
    doTestAuthProviderOnIndexer(SUB_MANAGER, IND_JRANALYST1, allActions);

    Set<IndexerModelAction> readWriteOnly = EnumSet.of(READ, WRITE);
    doTestAuthProviderOnIndexer(SUB_MANAGER, IND_TMP, readWriteOnly);

    Set<IndexerModelAction> readOnly = EnumSet.of(IndexerModelAction.READ);
    doTestAuthProviderOnIndexer(SUB_MANAGER, IND_PURCHASES_PARTIAL, readOnly);
  }

  @Test
  public void testAnalyst() throws Exception {
    Set<IndexerModelAction> writeOnly = EnumSet.of(IndexerModelAction.WRITE);
    doTestAuthProviderOnIndexer(SUB_ANALYST, IND_PURCHASES, writeOnly);

    Set<IndexerModelAction> allActions = EnumSet.allOf(IndexerModelAction.class);
    doTestAuthProviderOnIndexer(SUB_ANALYST, IND_ANALYST1, allActions);
    doTestAuthProviderOnIndexer(SUB_ANALYST, IND_JRANALYST1, allActions);

    Set<IndexerModelAction> readWriteOnly = EnumSet.of(READ, WRITE);
    doTestAuthProviderOnIndexer(SUB_ANALYST, IND_TMP, readWriteOnly);

    Set<IndexerModelAction> noActions = EnumSet.noneOf(IndexerModelAction.class);
    doTestAuthProviderOnIndexer(SUB_ANALYST, IND_PURCHASES_PARTIAL, noActions);
  }

  @Test
  public void testJuniorAnalyst() throws Exception {
     Set<IndexerModelAction> allActions = EnumSet.allOf(IndexerModelAction.class);
     doTestAuthProviderOnIndexer(SUB_JUNIOR_ANALYST, IND_JRANALYST1, allActions);

    Set<IndexerModelAction> readOnly = EnumSet.of(IndexerModelAction.READ);
    doTestAuthProviderOnIndexer(SUB_JUNIOR_ANALYST, IND_PURCHASES_PARTIAL, readOnly);

    Set<IndexerModelAction> noActions = EnumSet.noneOf(IndexerModelAction.class);
    doTestAuthProviderOnIndexer(SUB_JUNIOR_ANALYST, IND_PURCHASES, noActions);
    doTestAuthProviderOnIndexer(SUB_JUNIOR_ANALYST, IND_ANALYST1, noActions);
    doTestAuthProviderOnIndexer(SUB_JUNIOR_ANALYST, IND_TMP, noActions);
  }
}
