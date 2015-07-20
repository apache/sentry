package org.apache.solr.sentry;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Test for SentryIndexAuthorizationSingleton
 */
public class SentryIndexAuthorizationSingletonTest extends SentryTestBase {

  private static SolrCore core;
  private static CloudDescriptor cloudDescriptor;
  private static SentryIndexAuthorizationSingleton sentryInstance;

  private static final String OPERATION_NAME = "myOperation";

  @BeforeClass
  public static void beforeClass() throws Exception {
    core = createCore("solrconfig.xml", "schema-minimal.xml");
    // store the CloudDescriptor, because we will overwrite it with a mock
    // and restore it later
    cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    sentryInstance = SentrySingletonTestInstance.getInstance().getSentryInstance();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    closeCore(core, cloudDescriptor);
    core = null;
    cloudDescriptor = null;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp(core);
  }

  /**
   * Expect an unauthorized SolrException with a message that contains
   * msgContains.
   */
  private void doExpectUnauthorized(SolrQueryRequest request,
      Set<SearchModelAction> actions, String msgContains) throws Exception {
    doExpectUnauthorized(sentryInstance, request, actions, msgContains);
  }

  private void doExpectUnauthorized(SentryIndexAuthorizationSingleton singleton, SolrQueryRequest request,
      Set<SearchModelAction> actions, String msgContains) throws Exception {
    try {
      singleton.authorizeCollectionAction(request, actions, OPERATION_NAME);
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(msgContains));
    }
  }

  @Test
  public void testNoBinding() throws Exception {
    // Use reflection to construct a non-singleton version of SentryIndexAuthorizationSingleton
    // in order to get an instance without a binding
    Constructor ctor =
      SentryIndexAuthorizationSingleton.class.getDeclaredConstructor(String.class);
    ctor.setAccessible(true);
    SentryIndexAuthorizationSingleton nonSingleton =
      (SentryIndexAuthorizationSingleton)ctor.newInstance("");
    doExpectUnauthorized(nonSingleton, null, null, "binding");

    // test getUserName
    try {
      nonSingleton.getUserName(null);
      Assert.fail("Expected Solr exception");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
    }

    Collection<String> groups = nonSingleton.getRoles("junit");
    assertEquals(null, groups);
  }

  @Test
  public void testNoHttpRequest() throws Exception {
    SolrQueryRequest request = getRequest();
    doExpectUnauthorized(request, null, "HttpServletRequest");
  }

 @Test
  public void testNullUserName() throws Exception {
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(core, request, "collection1", null);
    doExpectUnauthorized(request, EnumSet.of(SearchModelAction.ALL),
      "User null does not have privileges for collection1");
  }

  @Test
  public void testEmptySuperUser() throws Exception {
    System.setProperty("solr.authorization.superuser", "");
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(core, request, "collection1", "solr");
    doExpectUnauthorized(request, EnumSet.of(SearchModelAction.ALL),
      "User solr does not have privileges for collection1");
  }

  /**
   * User name matches super user, should have access otherwise
   */
  @Test
  public void testSuperUserAccess() throws Exception {
    System.setProperty("solr.authorization.superuser", "junit");
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(core, request, "collection1", "junit");

    sentryInstance.authorizeCollectionAction(
      request, EnumSet.of(SearchModelAction.ALL), OPERATION_NAME);
  }

  /**
   * User name matches super user, should not have access otherwise
   */
  @Test
  public void testSuperUserNoAccess() throws Exception {
    System.setProperty("solr.authorization.superuser", "junit");
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(core, request, "bogusCollection", "junit");

    sentryInstance.authorizeCollectionAction(
      request, EnumSet.of(SearchModelAction.ALL), OPERATION_NAME);
  }

  /**
   * Test getting the user name.
   */
  @Test
  public void testUserName() throws Exception {
    SolrQueryRequest request = null;
    try {
      // no http request
      request = new SolrQueryRequestBase( core, new ModifiableSolrParams() ) {};
      try {
        sentryInstance.getUserName(request);
        Assert.fail("Expected SolrException");
      } catch (SolrException ex) {
        assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      }

      // no http request, but LocalSolrQueryRequest
      LocalSolrQueryRequest localRequest = null;
      try {
        localRequest = new LocalSolrQueryRequest(null, new ModifiableSolrParams());
        String superUser = (System.getProperty("solr.authorization.superuser", "solr"));
        String localName = sentryInstance.getUserName(localRequest);
        assertEquals(superUser, localName);
      } finally {
        if (localRequest != null) localRequest.close();
      }

      // null userName
      SolrQueryRequest sqr = getRequest();
      prepareCollAndUser(core, sqr, "collection", null, true);
      String nullName = sentryInstance.getUserName(sqr);
      assertEquals(null, nullName);

      // standard userName
      String userName = "foobar";
      prepareCollAndUser(core, sqr, "collection", userName, true);
      String returnedName = sentryInstance.getUserName(sqr);
      assertEquals(userName, returnedName);
    } finally {
      if (request != null) request.close();
    }
  }

  /**
   * Test getting the roles from user name
   */
  @Test
  public void testGetRoles() throws Exception {
    Collection<String> emptyCollection = ImmutableSet.<String>of();

    // null user
    Collection<String> roles = sentryInstance.getRoles(null);
    assertTrue(CollectionUtils.isEqualCollection(emptyCollection, roles));

    // no group
    roles = sentryInstance.getRoles("bogusUser");
    assertTrue(CollectionUtils.isEqualCollection(emptyCollection, roles));

    // no role
    roles = sentryInstance.getRoles("undefinedRoleUser");
    assertTrue(CollectionUtils.isEqualCollection(emptyCollection, roles));

    // single member
    Collection<String> singleRole = ImmutableSet.<String>of("junit_role");
    roles = sentryInstance.getRoles("junit");
    assertTrue(CollectionUtils.isEqualCollection(singleRole, roles));

    // multiple members
    Collection<String> multipleRoles = ImmutableSet.<String>of("junit_role", "queryOnlyAdmin_role", "updateOnlyAdmin_role");
    roles = sentryInstance.getRoles("multiGroupUser");
    assertTrue(CollectionUtils.isEqualCollection(multipleRoles, roles));
  }
}
