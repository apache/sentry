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
package org.apache.sentry.binding.solr;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.binding.solr.authz.SentrySolrAuthorizationException;
import org.apache.sentry.binding.solr.authz.SolrAuthzBinding;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Test for solr authz binding
 */
public class TestSolrAuthzBinding {
  private static final String RESOURCE_PATH = "test-authz-provider.ini";
  private SolrAuthzConf authzConf = new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
  private File baseDir;

  private Collection infoCollection = new Collection("info");
  private Collection generalInfoCollection = new Collection("generalInfo");

  private Subject corporal1 = new Subject("corporal1");
  private Subject sergeant1 = new Subject("sergeant1");
  private Subject general1 = new Subject("general1");

  private EnumSet<SearchModelAction> querySet = EnumSet.of(SearchModelAction.QUERY);
  private EnumSet<SearchModelAction> updateSet = EnumSet.of(SearchModelAction.UPDATE);
  private EnumSet<SearchModelAction> allSet = EnumSet.of(SearchModelAction.ALL);
  private EnumSet<SearchModelAction> allOfSet = EnumSet.allOf(SearchModelAction.class);
  private EnumSet<SearchModelAction> emptySet = EnumSet.noneOf(SearchModelAction.class);

  @Before
  public void setUp() throws Exception {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), new File(baseDir, RESOURCE_PATH).getPath());
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  private void setUsableAuthzConf(SolrAuthzConf conf) {
    conf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(), "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    conf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), new File(baseDir, RESOURCE_PATH).getPath());
    conf.set(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(), AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getDefault());
    conf.set(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar(), AuthzConfVars.AUTHZ_POLICY_ENGINE.getDefault());
  }

  /**
   * Test that incorrect specification of classes for
   * AUTHZ_PROVIDER, AUTHZ_PROVIDER_BACKEND, and AUTHZ_POLICY_ENGINE
   * correctly throw ClassNotFoundExceptions
   */
  @Test
  public void testClassNotFound() throws Exception {
    SolrAuthzConf solrAuthzConf =
      new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(solrAuthzConf);
    // verify it is usable
    new SolrAuthzBinding(solrAuthzConf);

    // give a bogus provider
    solrAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(), "org.apache.sentry.provider.BogusProvider");
    try {
      new SolrAuthzBinding(solrAuthzConf);
      Assert.fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {}

    setUsableAuthzConf(solrAuthzConf);
    // give a bogus provider backend
    solrAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(), "org.apache.sentry.provider.file.BogusProviderBackend");
    try {
      new SolrAuthzBinding(solrAuthzConf);
      Assert.fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {}

    setUsableAuthzConf(solrAuthzConf);
    // give a bogus policy enine
    solrAuthzConf.set(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar(), "org.apache.sentry.provider.solr.BogusPolicyEngine");
    try {
      new SolrAuthzBinding(solrAuthzConf);
      Assert.fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {}
  }

  /**
   * Test that incorrect specification of the provider resource
   * throws an exception
   */
  @Test
  public void testResourceNotFound() throws Exception {
    SolrAuthzConf solrAuthzConf =
      new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(solrAuthzConf);

    // bogus specification
    solrAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), new File(baseDir, "test-authz-bogus-provider.ini").getPath());
    try {
      new SolrAuthzBinding(solrAuthzConf);
      Assert.fail("Expected InvocationTargetException");
    } catch (InvocationTargetException e) {
      assertTrue(e.getTargetException() instanceof FileNotFoundException);
    }

    // missing specification
    solrAuthzConf.unset(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
    try {
      new SolrAuthzBinding(solrAuthzConf);
      Assert.fail("Expected InvocationTargetException");
    } catch (InvocationTargetException e) {
      assertTrue(e.getTargetException() instanceof IllegalArgumentException);
    }
  }

  /**
   * Verify that an definition of only the AuthorizationProvider
   * (not ProviderBackend or PolicyEngine) works.
   */
  @Test
  public void testAuthProviderOnlySolrAuthzConfs() throws Exception {
    new SolrAuthzBinding(authzConf);
  }

  /**
   * Test for group mapping
   */
  @Test
  public void testGroupMapping() throws Exception {
    SolrAuthzConf solrAuthzConf =
      new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(solrAuthzConf);
    SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf);
    Set<String> emptyList = Collections.emptySet();

    // check non-existant users
    assertEquals(binding.getGroups(null), emptyList);
    assertEquals(binding.getGroups("nonExistantUser"), emptyList);

    // check group names don't map to user names
    assertEquals(binding.getGroups("corporal"), emptyList);
    assertEquals(binding.getGroups("sergeant"), emptyList);
    assertEquals(binding.getGroups("general"), emptyList);
    assertEquals(binding.getGroups("othergeneralgroup"), emptyList);

    // check valid group names
    assertEquals(binding.getGroups("corporal1"), Sets.newHashSet("corporal"));
    assertEquals(binding.getGroups("sergeant1"), Sets.newHashSet("sergeant"));
    assertEquals(binding.getGroups("general1"), Sets.newHashSet("general", "othergeneralgroup"));
  }

  /**
   * Test for role mapping
   */
  @Test
  public void testGetRoles() throws Exception {
    SolrAuthzConf solrAuthzConf =
      new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(solrAuthzConf);
    SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf);
    Set<String> emptySet = Collections.emptySet();

    // check non-existant users
    assertEquals(binding.getRoles(null), emptySet);
    assertEquals(binding.getRoles("nonExistantUser"), emptySet);

    // check user with undefined group
    assertEquals(binding.getRoles("undefinedGroupUser"), emptySet);
    // check group with undefined role
    assertEquals(binding.getRoles("undefinedRoleUser"), emptySet);

    // check role names don't map in the other direction
    assertEquals(binding.getRoles("corporal_role"), emptySet);
    assertEquals(binding.getRoles("sergeant_role"), emptySet);
    assertEquals(binding.getRoles("general_role"), emptySet);

    // check valid users
    assertEquals(binding.getRoles("corporal1"), Sets.newHashSet("corporal_role"));
    assertEquals(binding.getRoles("sergeant1"), Sets.newHashSet("corporal_role", "sergeant_role"));
    assertEquals(binding.getRoles("general1"), Sets.newHashSet("corporal_role", "sergeant_role", "general_role"));

    // check user whos groups have overlapping roles
    assertEquals(binding.getRoles("overlappingUser"), Sets.newHashSet("corporal_role", "sergeant_role", "general_role"));
  }

  /**
   * Test that a full sentry-site definition works.
   */
  @Test
  public void testSolrAuthzConfs() throws Exception {
     SolrAuthzConf solrAuthzConf =
       new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
     setUsableAuthzConf(solrAuthzConf);
     new SolrAuthzBinding(solrAuthzConf);
  }

  private void expectAuthException(SolrAuthzBinding binding, Subject subject,
      Collection collection, EnumSet<SearchModelAction> action) throws Exception {
     try {
       binding.authorizeCollection(subject, collection, action);
       Assert.fail("Expected SentrySolrAuthorizationException");
     } catch(SentrySolrAuthorizationException e) {
     }
  }

  /**
   * Test that a user that doesn't exist throws an exception
   * when trying to authorize
   */
  @Test
  public void testNoUser() throws Exception {
     SolrAuthzConf solrAuthzConf =
       new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
     setUsableAuthzConf(solrAuthzConf);
     SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf);
     expectAuthException(binding, new Subject("bogus"), infoCollection, querySet);
  }

  /**
   * Test that a bogus collection name throws an exception
   */
  @Test
  public void testNoCollection() throws Exception {
     SolrAuthzConf solrAuthzConf =
       new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
     setUsableAuthzConf(solrAuthzConf);
     SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf);
     expectAuthException(binding, corporal1, new Collection("bogus"), querySet);
  }

  /**
   * Test if no action is attempted an exception is thrown
   */
  @Test
  public void testNoAction() throws Exception {
    SolrAuthzConf solrAuthzConf =
      new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(solrAuthzConf);
    SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf);
    try {
      binding.authorizeCollection(corporal1, infoCollection, emptySet);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Test that standard unauthorized attempts fail
   */
  @Test
  public void testAuthException() throws Exception {
    SolrAuthzConf solrAuthzConf =
       new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
     setUsableAuthzConf(solrAuthzConf);
     SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf);
     expectAuthException(binding, corporal1, infoCollection, updateSet);
     expectAuthException(binding, corporal1, infoCollection, allSet);
     expectAuthException(binding, corporal1, generalInfoCollection, querySet);
     expectAuthException(binding, corporal1, generalInfoCollection, updateSet);
     expectAuthException(binding, corporal1, generalInfoCollection, allSet);
     expectAuthException(binding, sergeant1, infoCollection, allSet);
     expectAuthException(binding, sergeant1, generalInfoCollection, querySet);
     expectAuthException(binding, sergeant1, generalInfoCollection, updateSet);
     expectAuthException(binding, sergeant1, generalInfoCollection, allSet);
  }

  /**
   * Test that standard authorized attempts succeed
   */
  @Test
  public void testAuthAllowed() throws Exception {
     SolrAuthzConf solrAuthzConf =
       new SolrAuthzConf(Resources.getResource("sentry-site.xml"));
     setUsableAuthzConf(solrAuthzConf);
     SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf);
     binding.authorizeCollection(corporal1, infoCollection, querySet);
     binding.authorizeCollection(sergeant1, infoCollection, querySet);
     binding.authorizeCollection(sergeant1, infoCollection, updateSet);
     binding.authorizeCollection(general1, infoCollection, querySet);
     binding.authorizeCollection(general1, infoCollection, updateSet);
     binding.authorizeCollection(general1, infoCollection, allSet);
     binding.authorizeCollection(general1, infoCollection, allOfSet);
     binding.authorizeCollection(general1, generalInfoCollection, querySet);
     binding.authorizeCollection(general1, generalInfoCollection, updateSet);
     binding.authorizeCollection(general1, generalInfoCollection, allSet);
     binding.authorizeCollection(general1, generalInfoCollection, allOfSet);
  }
}
