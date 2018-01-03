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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.sentry.binding.solr.authz.SolrAuthzBinding;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.exception.SentryGroupNotFoundException;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.apache.sentry.core.model.solr.Collection;
import org.apache.sentry.core.model.solr.SolrModelAction;
import org.apache.solr.security.AuthorizationResponse;
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
  private SolrAuthzConf authzConf = new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
  private File baseDir;

  private Collection infoCollection = new Collection("info");
  private Collection generalInfoCollection = new Collection("generalInfo");

  private Subject corporal1 = new Subject("corporal1");
  private Subject sergeant1 = new Subject("sergeant1");
  private Subject general1 = new Subject("general1");

  private EnumSet<SolrModelAction> querySet = EnumSet.of(SolrModelAction.QUERY);
  private EnumSet<SolrModelAction> updateSet = EnumSet.of(SolrModelAction.UPDATE);
  private EnumSet<SolrModelAction> allSet = EnumSet.of(SolrModelAction.ALL);
  private EnumSet<SolrModelAction> allOfSet = EnumSet.allOf(SolrModelAction.class);
  private EnumSet<SolrModelAction> emptySet = EnumSet.noneOf(SolrModelAction.class);

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
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);
    // verify it is usable
    {
      SolrAuthzBinding binding = null;
      try {
        binding = new SolrAuthzBinding(solrAuthzConf);
      } finally {
        if (binding != null) {
          binding.close();
        }
      }
    }

    // give a bogus provider
    solrAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(), "org.apache.sentry.provider.BogusProvider");
    try (SolrAuthzBinding b = new SolrAuthzBinding(solrAuthzConf)) {
      Assert.fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {}

    setUsableAuthzConf(solrAuthzConf);
    // give a bogus provider backend
    solrAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(), "org.apache.sentry.provider.file.BogusProviderBackend");
    try (SolrAuthzBinding b = new SolrAuthzBinding(solrAuthzConf)) {
      Assert.fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {}

    setUsableAuthzConf(solrAuthzConf);
    // give a bogus policy enine
    solrAuthzConf.set(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar(), "org.apache.sentry.provider.solr.BogusPolicyEngine");
    try (SolrAuthzBinding b = new SolrAuthzBinding(solrAuthzConf)) {
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
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);

    // bogus specification
    solrAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), new File(baseDir, "test-authz-bogus-provider.ini").getPath());
    try (SolrAuthzBinding b = new SolrAuthzBinding(solrAuthzConf)) {
      Assert.fail("Expected InvocationTargetException");
    } catch (InvocationTargetException e) {
      assertTrue(e.getTargetException() instanceof FileNotFoundException);
    }

    // missing specification
    solrAuthzConf.unset(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
    try (SolrAuthzBinding b = new SolrAuthzBinding(solrAuthzConf)) {
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
    SolrAuthzBinding binding = null;
    try {
      binding = new SolrAuthzBinding(authzConf);
    } finally {
      if (binding != null) {
        binding.close();
      }
    }
  }

  /**
   * Test for group mapping
   */
  @Test
  public void testGroupMapping() throws Exception {
    SolrAuthzConf solrAuthzConf =
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);

    try (SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf)) {
      // check non-existant users
      try {
        binding.getGroups(null);
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }
      try {
        binding.getGroups("nonExistantUser");
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }

      // check group names don't map to user names
      try {
        binding.getGroups("corporal");
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }
      try {
        binding.getGroups("sergeant");
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }
      try {
        binding.getGroups("general");
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }
      try {
        binding.getGroups("othergeneralgroup");
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }

      // check valid group names
      assertEquals(binding.getGroups("corporal1"), Sets.newHashSet("corporal"));
      assertEquals(binding.getGroups("sergeant1"), Sets.newHashSet("sergeant"));
      assertEquals(binding.getGroups("general1"), Sets.newHashSet("general", "othergeneralgroup"));
    }
  }

  /**
   * Test for role mapping
   */
  @Test
  public void testGetRoles() throws Exception {
    SolrAuthzConf solrAuthzConf =
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);
    try (SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf)) {
      Set<String> emptySet = Collections.emptySet();

      // check user with undefined group
      assertEquals(binding.getRoles("undefinedGroupUser"), emptySet);
      // check group with undefined role
      assertEquals(binding.getRoles("undefinedRoleUser"), emptySet);

      // check role names don't map in the other direction
      try {
        binding.getRoles("corporal_role");
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }
      try {
        binding.getRoles("sergeant_role");
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }
      try {
        binding.getRoles("general_role");
        Assert.fail("Expected SentryGroupNotFoundException");
      } catch (SentryGroupNotFoundException e) {
      }

      // check valid users
      System.out.println("---" + binding.providerBackend.toString());

      assertEquals(Sets.newHashSet("corporal"), binding.getGroups("corporal1"));
      assertEquals(Sets.newHashSet("corporal_role"),
          binding.providerBackend.getRoles(Sets.newHashSet("corporal"), ActiveRoleSet.ALL));
      assertEquals(Sets.newHashSet("corporal_role"), binding.getRoles("corporal1"));
      assertEquals(Sets.newHashSet("corporal_role", "sergeant_role"),
          binding.getRoles("sergeant1"));
      assertEquals(Sets.newHashSet("corporal_role", "sergeant_role", "general_role"),
          binding.getRoles("general1"));

      // check user whos groups have overlapping roles
      assertEquals(Sets.newHashSet("corporal_role", "sergeant_role", "general_role"),
          binding.getRoles("overlappingUser"));
    }
  }

  /**
   * Test that a full sentry-site definition works.
   */
  @Test
  public void testSolrAuthzConfs() throws Exception {
    SolrAuthzConf solrAuthzConf =
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);

    SolrAuthzBinding binding = null;
    try {
      binding = new SolrAuthzBinding(solrAuthzConf);
    } finally {
      if (binding != null) {
        binding.close();
      }
    }
  }

  private void expectAuthException(SolrAuthzBinding binding, Subject subject,
      Collection collection, Set<SolrModelAction> action) throws Exception {
    assertEquals(AuthorizationResponse.FORBIDDEN,
        binding.authorizeCollection(subject, collection, action));
  }

  /**
   * Test that a user that doesn't exist gets an AuthException
   * when trying to authorize
   */
  @Test
  public void testNoUser() throws Exception {
    SolrAuthzConf solrAuthzConf =
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
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
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
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
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);
    try (SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf)) {
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
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);
    try (SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf)) {
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
  }

  /**
   * Test that standard authorized attempts succeed
   */
  @Test
  public void testAuthAllowed() throws Exception {
    SolrAuthzConf solrAuthzConf =
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);
    try (SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf)) {
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

  /**
   * Test that when the resource is put on  HDFS and the scheme of the resource is not set,
   * the resouce can be found if fs.defaultFS is specified
   */
  @Test
  public void testResourceWithSchemeNotSet() throws Exception {
    SolrAuthzConf solrAuthzConf =
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);

    MiniDFSCluster dfsCluster =  HdfsTestUtil.setupClass(new File(Files.createTempDir(),
        TestSolrAuthzBinding.class.getName() + "_"
            + System.currentTimeMillis()).getAbsolutePath());
    String resourceOnHDFS = "/hdfs" + File.separator + UUID.randomUUID() + File.separator + "test-authz-provider.ini";
    SolrAuthzBinding binding = null;
    try {
      // Copy resource to HDFSS
      dfsCluster.getFileSystem().copyFromLocalFile(false,
          new Path(baseDir.getPath(), RESOURCE_PATH),
          new Path(resourceOnHDFS));
      solrAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), resourceOnHDFS);
      // set HDFS as the defaultFS so the resource will be found
      solrAuthzConf.set("fs.defaultFS", dfsCluster.getFileSystem().getConf().get("fs.defaultFS"));
      binding = new SolrAuthzBinding(solrAuthzConf);
    } finally {
      if (binding != null) {
        binding.close();
      }
      if (dfsCluster != null) {
        HdfsTestUtil.teardownClass(dfsCluster);
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testCustomGroupMapping() throws Exception {
    SolrAuthzConf solrAuthzConf =
        new SolrAuthzConf(Collections.singletonList(Resources.getResource("sentry-site.xml")));
    setUsableAuthzConf(solrAuthzConf);
    solrAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(), "org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider");
    solrAuthzConf.set("hadoop.security.group.mapping",
        FoobarGroupMappingServiceProvider.class.getName());
    try (SolrAuthzBinding binding = new SolrAuthzBinding(solrAuthzConf)) {
      final String user = "userTestSolrAuthzBinding";
      assertEquals(1, binding.getGroups(user).size());
      assertTrue(binding.getGroups(user).contains("foobar"));
    }
  }

  /**
   * GroupMappingServiceProvider that returns "foobar" for any group
   */
  private static class FoobarGroupMappingServiceProvider implements GroupMappingServiceProvider {
    @Override
    public List<String> getGroups(String user) throws IOException {
      return Arrays.asList("foobar");
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {}

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {}
  }
}
