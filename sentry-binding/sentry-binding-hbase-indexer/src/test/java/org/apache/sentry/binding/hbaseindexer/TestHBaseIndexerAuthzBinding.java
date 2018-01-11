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
package org.apache.sentry.binding.hbaseindexer;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.sentry.binding.hbaseindexer.authz.HBaseIndexerAuthzBinding;
import org.apache.sentry.binding.hbaseindexer.conf.HBaseIndexerAuthzConf;
import org.apache.sentry.binding.hbaseindexer.conf.HBaseIndexerAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.apache.sentry.core.model.indexer.Indexer;
import org.apache.sentry.core.model.indexer.IndexerModelAction;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedList;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.apache.sentry.provider.common.AuthorizationComponent.HBASE_INDEXER;


/**
 * Test for hbaseindexer authz binding
 */
public class TestHBaseIndexerAuthzBinding {
  private static final String RESOURCE_PATH = "test-authz-provider.ini";
  private HBaseIndexerAuthzConf authzConf = new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
  private File baseDir;

  private Indexer infoIndexer = new Indexer("info");
  private Indexer generalInfoIndexer = new Indexer("generalInfo");

  private Subject corporal1 = new Subject("corporal1");
  private Subject sergeant1 = new Subject("sergeant1");
  private Subject general1 = new Subject("general1");

  private EnumSet<IndexerModelAction> readSet = EnumSet.of(IndexerModelAction.READ);
  private EnumSet<IndexerModelAction> writeSet = EnumSet.of(IndexerModelAction.WRITE);
  private EnumSet<IndexerModelAction> allSet = EnumSet.of(IndexerModelAction.ALL);
  private EnumSet<IndexerModelAction> allOfSet = EnumSet.allOf(IndexerModelAction.class);
  private EnumSet<IndexerModelAction> emptySet = EnumSet.noneOf(IndexerModelAction.class);

  @Before
  public void setUp() throws Exception {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), new File(baseDir, RESOURCE_PATH).getPath());
  }

  @After
  public void teardown() {
    if (baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  private void setUsableAuthzConf(HBaseIndexerAuthzConf conf) {
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
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);
    // verify it is usable
    new HBaseIndexerAuthzBinding(indexerAuthzConf);

    // give a bogus provider
    indexerAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(), "org.apache.sentry.provider.BogusProvider");
    try {
      new HBaseIndexerAuthzBinding(indexerAuthzConf);
      Assert.fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {
    }

    setUsableAuthzConf(indexerAuthzConf);
    // give a bogus provider backend
    indexerAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(), "org.apache.sentry.provider.file.BogusProviderBackend");
    try {
      new HBaseIndexerAuthzBinding(indexerAuthzConf);
      Assert.fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {
    }

    setUsableAuthzConf(indexerAuthzConf);
    // give a bogus policy enine
    indexerAuthzConf.set(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar(), "org.apache.sentry.provider.hbaseindexer.BogusPolicyEngine");
    try {
      new HBaseIndexerAuthzBinding(indexerAuthzConf);
      Assert.fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) {
    }
  }

  /**
   * Test that incorrect specification of the provider resource
   * throws an exception
   */
  @Test
  public void testResourceNotFound() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);

    // bogus specification
    indexerAuthzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), new File(baseDir, "test-authz-bogus-provider.ini").getPath());
    try {
      new HBaseIndexerAuthzBinding(indexerAuthzConf);
      Assert.fail("Expected InvocationTargetException");
    } catch (InvocationTargetException e) {
      assertTrue(e.getTargetException() instanceof FileNotFoundException);
    }

    // missing specification
    indexerAuthzConf.unset(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
    try {
      new HBaseIndexerAuthzBinding(indexerAuthzConf);
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
  public void testAuthProviderOnlyHBaseIndexerAuthzConfs() throws Exception {
    new HBaseIndexerAuthzBinding(authzConf);
  }

  /**
   * Test that a full sentry-site definition works.
   */
  @Test
  public void testHBaseIndexerAuthzConfs() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);
    new HBaseIndexerAuthzBinding(indexerAuthzConf);
  }

  private void expectAuthException(HBaseIndexerAuthzBinding binding, Subject subject,
                                   Indexer indexer, EnumSet<IndexerModelAction> action) throws Exception {
    try {
      binding.authorize(subject, indexer, action);
      Assert.fail("Expected SentryHBaseIndexerAuthorizationException");
    } catch (SentryAccessDeniedException e) {
    }
  }

  /**
   * Test that a user that doesn't exist throws an exception
   * when trying to authorize
   */
  @Test
  public void testNoUser() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);
    HBaseIndexerAuthzBinding binding = new HBaseIndexerAuthzBinding(indexerAuthzConf);
    expectAuthException(binding, new Subject("bogus"), infoIndexer, readSet);
  }

  /**
   * Test that a bogus indexer name throws an exception
   */
  @Test
  public void testNoIndexer() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);
    HBaseIndexerAuthzBinding binding = new HBaseIndexerAuthzBinding(indexerAuthzConf);
    expectAuthException(binding, corporal1, new Indexer("bogus"), readSet);
  }

  /**
   * Test if no action is attempted an exception is thrown
   */
  @Test
  public void testNoAction() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);
    HBaseIndexerAuthzBinding binding = new HBaseIndexerAuthzBinding(indexerAuthzConf);
    try {
      binding.authorize(corporal1, infoIndexer, emptySet);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Test that standard unauthorized attempts fail
   */
  @Test
  public void testAuthException() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);
    HBaseIndexerAuthzBinding binding = new HBaseIndexerAuthzBinding(indexerAuthzConf);
    expectAuthException(binding, corporal1, infoIndexer, writeSet);
    expectAuthException(binding, corporal1, infoIndexer, allSet);
    expectAuthException(binding, corporal1, generalInfoIndexer, readSet);
    expectAuthException(binding, corporal1, generalInfoIndexer, writeSet);
    expectAuthException(binding, corporal1, generalInfoIndexer, allSet);
    expectAuthException(binding, sergeant1, infoIndexer, allSet);
    expectAuthException(binding, sergeant1, generalInfoIndexer, readSet);
    expectAuthException(binding, sergeant1, generalInfoIndexer, writeSet);
    expectAuthException(binding, sergeant1, generalInfoIndexer, allSet);
  }

  /**
   * Test that standard authorized attempts succeed
   */
  @Test
  public void testAuthAllowed() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);
    HBaseIndexerAuthzBinding binding = new HBaseIndexerAuthzBinding(indexerAuthzConf);
    binding.authorize(corporal1, infoIndexer, readSet);
    binding.authorize(sergeant1, infoIndexer, readSet);
    binding.authorize(sergeant1, infoIndexer, writeSet);
    binding.authorize(general1, infoIndexer, readSet);
    binding.authorize(general1, infoIndexer, writeSet);
    binding.authorize(general1, infoIndexer, allSet);
    binding.authorize(general1, infoIndexer, allOfSet);
    binding.authorize(general1, generalInfoIndexer, readSet);
    binding.authorize(general1, generalInfoIndexer, writeSet);
    binding.authorize(general1, generalInfoIndexer, allSet);
    binding.authorize(general1, generalInfoIndexer, allOfSet);
  }

  @Test
  public void testFilterIndexers() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
        new HBaseIndexerAuthzConf(Resources.getResource("sentry-site.xml"));
    setUsableAuthzConf(indexerAuthzConf);
    HBaseIndexerAuthzBinding binding = new HBaseIndexerAuthzBinding(indexerAuthzConf);
    LinkedList<Indexer> list = new LinkedList<Indexer>();
    list.add(infoIndexer);
    list.add(generalInfoIndexer);

    Collection<Indexer> corpFilter = binding.filterIndexers(corporal1, list);
    assertEquals(1, corpFilter.size());
    assertTrue(corpFilter.contains(infoIndexer));

    Collection<Indexer> sergFilter = binding.filterIndexers(sergeant1, list);
    assertEquals(1, sergFilter.size());
    assertTrue(corpFilter.contains(infoIndexer));

    Collection<Indexer> genFilter = binding.filterIndexers(general1, list);
    assertEquals(2, genFilter.size());
    assertTrue(genFilter.contains(infoIndexer));
    assertTrue(genFilter.contains(generalInfoIndexer));
  }

  @Test
  public void testSentryGenericProviderBackendConfig() throws Exception {
    HBaseIndexerAuthzConf indexerAuthzConf =
      new HBaseIndexerAuthzConf(Resources.getResource("sentry-site-service.xml"));

    HBaseIndexerAuthzBinding binding = new HBaseIndexerAuthzBinding(indexerAuthzConf);
    Field f = binding.getClass().getDeclaredField("providerBackend"); //NoSuchFieldException
    f.setAccessible(true);
    SentryGenericProviderBackend providerBackend = (SentryGenericProviderBackend) f.get(binding);
    assertEquals(HBASE_INDEXER, providerBackend.getComponentType());
    assertEquals("MyService", providerBackend.getServiceName());
  }

}
