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
package org.apache.sentry.policy.solr;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.solr.Collection;
import org.apache.sentry.core.model.solr.SolrModelAction;
import org.apache.sentry.core.model.solr.SolrPrivilegeModel;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.provider.file.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;


public class TestSolrAuthorizationProviderGeneralCases {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestSolrAuthorizationProviderGeneralCases.class);

  private static final Multimap<String, String> USER_TO_GROUP_MAP = HashMultimap
      .create();

  private static final Subject SUB_ADMIN = new Subject("admin1");
  private static final Subject SUB_MANAGER = new Subject("manager1");
  private static final Subject SUB_ANALYST = new Subject("analyst1");
  private static final Subject SUB_JUNIOR_ANALYST = new Subject("jranalyst1");

  private static final Collection COLL_PURCHASES = new Collection("purchases");
  private static final Collection COLL_ANALYST1 = new Collection("analyst1");
  private static final Collection COLL_JRANALYST1 = new Collection("jranalyst1");
  private static final Collection COLL_TMP = new Collection("tmpcollection");
  private static final Collection COLL_PURCHASES_PARTIAL = new Collection("purchases_partial");

  private static final SolrModelAction QUERY = SolrModelAction.QUERY;
  private static final SolrModelAction UPDATE = SolrModelAction.UPDATE;

  static {
    USER_TO_GROUP_MAP.putAll(SUB_ADMIN.getName(), Arrays.asList("admin"));
    USER_TO_GROUP_MAP.putAll(SUB_MANAGER.getName(), Arrays.asList("manager"));
    USER_TO_GROUP_MAP.putAll(SUB_ANALYST.getName(), Arrays.asList("analyst"));
    USER_TO_GROUP_MAP.putAll(SUB_JUNIOR_ANALYST.getName(),
        Arrays.asList("jranalyst"));
  }

  private final ResourceAuthorizationProvider authzProvider;
  private File baseDir;

  public TestSolrAuthorizationProviderGeneralCases() throws IOException {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, "solr-policy-test-authz-provider.ini");
    authzProvider = new HadoopGroupResourceAuthorizationProvider(
        SolrPolicyTestUtil.createPolicyEngineForTest(new File(baseDir, "solr-policy-test-authz-provider.ini").getPath()),
        new MockGroupMappingServiceProvider(USER_TO_GROUP_MAP), SolrPrivilegeModel.getInstance());

  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  private void doTestAuthProviderOnCollection(Subject subject,
      Collection collection, Set<? extends Action> expectedPass) throws Exception {
    Set<SolrModelAction> allActions =
        EnumSet.of(SolrModelAction.ALL, SolrModelAction.QUERY, SolrModelAction.UPDATE);
    for (SolrModelAction action : allActions) {
      doTestResourceAuthorizationProvider(subject, collection,
          EnumSet.of(action), expectedPass.contains(action));
    }
  }

  private void doTestResourceAuthorizationProvider(Subject subject,
      Collection collection,
      Set<? extends Action> privileges, boolean expected) throws Exception {
    List<Authorizable> authzHierarchy = Arrays.asList(new Authorizable[] {
        collection
    });
    Objects.ToStringHelper helper = Objects.toStringHelper("TestParameters");
    helper.add("Subject", subject)
          .add("Collection", collection)
          .add("Privileges", privileges)
          .add("authzHierarchy", authzHierarchy);
    LOGGER.info("Running with " + helper.toString());
    Assert.assertEquals(helper.toString(), expected,
        authzProvider.hasAccess(subject, authzHierarchy, privileges, ActiveRoleSet.ALL));
    LOGGER.info("Passed " + helper.toString());
  }

  @Test
  public void testAdmin() throws Exception {
    Set<SolrModelAction> allActions = EnumSet.allOf(SolrModelAction.class);
    doTestAuthProviderOnCollection(SUB_ADMIN, COLL_PURCHASES, allActions);
    doTestAuthProviderOnCollection(SUB_ADMIN, COLL_ANALYST1, allActions);
    doTestAuthProviderOnCollection(SUB_ADMIN, COLL_JRANALYST1, allActions);
    doTestAuthProviderOnCollection(SUB_ADMIN, COLL_TMP, allActions);
    doTestAuthProviderOnCollection(SUB_ADMIN, COLL_PURCHASES_PARTIAL, allActions);
  }

  @Test
  public void testManager() throws Exception {
    Set<SolrModelAction> updateOnly = EnumSet.of(SolrModelAction.UPDATE);
    doTestAuthProviderOnCollection(SUB_MANAGER, COLL_PURCHASES, updateOnly);

    Set<SolrModelAction> allActions = EnumSet.allOf(SolrModelAction.class);
    doTestAuthProviderOnCollection(SUB_MANAGER, COLL_ANALYST1, allActions);
    doTestAuthProviderOnCollection(SUB_MANAGER, COLL_JRANALYST1, allActions);

    Set<SolrModelAction> queryUpdateOnly = EnumSet.of(QUERY, UPDATE);
    doTestAuthProviderOnCollection(SUB_MANAGER, COLL_TMP, queryUpdateOnly);

    Set<SolrModelAction> queryOnly = EnumSet.of(SolrModelAction.QUERY);
    doTestAuthProviderOnCollection(SUB_MANAGER, COLL_PURCHASES_PARTIAL, queryOnly);
  }

  @Test
  public void testAnalyst() throws Exception {
    Set<SolrModelAction> updateOnly = EnumSet.of(SolrModelAction.UPDATE);
    doTestAuthProviderOnCollection(SUB_ANALYST, COLL_PURCHASES, updateOnly);

    Set<SolrModelAction> allActions = EnumSet.allOf(SolrModelAction.class);
    doTestAuthProviderOnCollection(SUB_ANALYST, COLL_ANALYST1, allActions);
    doTestAuthProviderOnCollection(SUB_ANALYST, COLL_JRANALYST1, allActions);

    Set<SolrModelAction> queryUpdateOnly = EnumSet.of(QUERY, UPDATE);
    doTestAuthProviderOnCollection(SUB_ANALYST, COLL_TMP, queryUpdateOnly);

    Set<SolrModelAction> noActions = EnumSet.noneOf(SolrModelAction.class);
    doTestAuthProviderOnCollection(SUB_ANALYST, COLL_PURCHASES_PARTIAL, noActions);
  }

  @Test
  public void testJuniorAnalyst() throws Exception {
    Set<SolrModelAction> allActions = EnumSet.allOf(SolrModelAction.class);
    doTestAuthProviderOnCollection(SUB_JUNIOR_ANALYST, COLL_JRANALYST1, allActions);

    Set<SolrModelAction> queryOnly = EnumSet.of(SolrModelAction.QUERY);
    doTestAuthProviderOnCollection(SUB_JUNIOR_ANALYST, COLL_PURCHASES_PARTIAL, queryOnly);

    Set<SolrModelAction> noActions = EnumSet.noneOf(SolrModelAction.class);
    doTestAuthProviderOnCollection(SUB_JUNIOR_ANALYST, COLL_PURCHASES, noActions);
    doTestAuthProviderOnCollection(SUB_JUNIOR_ANALYST, COLL_ANALYST1, noActions);
    doTestAuthProviderOnCollection(SUB_JUNIOR_ANALYST, COLL_TMP, noActions);
  }

  public class MockGroupMappingServiceProvider implements GroupMappingService {
    private final Multimap<String, String> userToGroupMap;

    public MockGroupMappingServiceProvider(Multimap<String, String> userToGroupMap) {
      this.userToGroupMap = userToGroupMap;
    }

    @Override
    public Set<String> getGroups(String user) {
      return Sets.newHashSet(userToGroupMap.get(user));
    }
  }
}
