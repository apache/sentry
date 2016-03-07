package org.apache.solr.handler.component;
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.sentry.SentryIndexAuthorizationSingleton;
import org.apache.solr.sentry.SentrySingletonTestInstance;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for QueryIndexAuthorizationComponent
 */
public class QueryDocAuthorizationComponentTest extends SentryTestBase {
  private static SolrCore core;
  private static SentryIndexAuthorizationSingleton sentryInstance;

  @BeforeClass
  public static void beforeClass() throws Exception {
    core = createCore("solrconfig.xml", "schema-minimal.xml");
    // store the CloudDescriptor, because we will overwrite it with a mock
    // and restore it later
    sentryInstance = SentrySingletonTestInstance.getInstance().getSentryInstance();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    closeCore(core, null);
    core = null;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp(core);
  }

  private String getClause(String authField, String value) {
    StringBuilder builder = new StringBuilder();
    builder.append(" {!raw f=").append(authField)
      .append(" v=").append(value).append("}");
    return builder.toString();
  }

  private ResponseBuilder getResponseBuilder() {
    SolrQueryRequest request = getRequest();
    return new ResponseBuilder(request, null, null);
  }

  private ResponseBuilder runComponent(String user, NamedList args, SolrParams params)
  throws Exception {
    ResponseBuilder builder = getResponseBuilder();
    prepareCollAndUser(core, builder.req, "collection1", user);

    if (params != null) {
      builder.req.setParams(params);
    } else {
      builder.req.setParams(new ModifiableSolrParams());
    }

    QueryDocAuthorizationComponent component =
      new QueryDocAuthorizationComponent(sentryInstance);
    component.init(args);
    component.prepare(builder);
    return builder;
  }

  // Clauses are treated as OR, so order does not matter.
  private void assertEqualClausesOrderIndependent(String expected, String actual) {
    Set<String> expectedSet = new HashSet<String>(Arrays.asList(expected.split("}")));
    Set<String> actualSet = new HashSet<String>(Arrays.asList(actual.split("}")));
    assertEquals(expectedSet, actualSet);
  }

  private void checkParams(String[] expected, ResponseBuilder builder) {
    final String fieldName = "fq";
    final String [] params = builder.req.getParams().getParams(fieldName);
    if (expected == null) {
      assertEquals(null, params);
    } else {
      assertNotNull(params);
      assertEquals(expected.length, params.length);
      for (int i = 0; i < params.length; ++i) {
        assertEqualClausesOrderIndependent(expected[ i ], params[ i ]);
      }
    }
  }

  @Test
  public void testSimple() throws Exception {
    NamedList args = new NamedList();
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
    ResponseBuilder builder = runComponent("junit", args, null);

    String expect = getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "junit_role");
    checkParams(new String[] {expect}, builder);
  }

  @Test
  public void testEnabled() throws Exception {
    // Test empty args
    NamedList args = new NamedList();
    ResponseBuilder builder = runComponent("junit", args, null);
    checkParams(null, builder);

    // Test enabled false
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "false");
    builder = runComponent("junit", args, null);
    checkParams(null, builder);
  }

  @Test
  public void testAuthFieldNonDefault() throws Exception {
    String authField = "nonDefaultAuthField";
    NamedList args = new NamedList();
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
    args.add(QueryDocAuthorizationComponent.AUTH_FIELD_PROP, authField);
    ResponseBuilder builder = runComponent("junit", args, null);

    String expect = getClause(authField, "junit_role");
    checkParams(new String[] {expect}, builder);
  }

  @Test
  public void testSuperUser() throws Exception {
    String superUser = (System.getProperty("solr.authorization.superuser", "solr"));
    NamedList args = new NamedList();
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
    ResponseBuilder builder = runComponent(superUser, args, null);
    prepareCollAndUser(core, builder.req, "collection1", superUser);

    checkParams(null, builder);
  }

  @Test
  public void testExistingFilterQuery() throws Exception {
    ModifiableSolrParams newParams = new ModifiableSolrParams();
    String existingFq = "bogusField:(bogusUser)";
    newParams.add("fq", existingFq);
    NamedList args = new NamedList();
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
    ResponseBuilder builder = runComponent("junit", args, newParams);

    String expect = getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "junit_role");
    checkParams(new String[] {existingFq, expect} , builder);
  }

  /**
   * Test a request from a user coming from an empty group.
   * This request should be rejected because otherwise document-level
   * filtering will be skipped.
   */
  @Test
  public void testEmptyGroup() throws Exception {
    String user = "bogusUser";
    try {
      NamedList args = new NamedList();
      args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
      ResponseBuilder builder = runComponent(user, args, null);

      checkParams(null, builder);
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(
        user + " rejected because user is not associated with any roles"));
    }
  }

  /**
   * Test a request from a user coming from an empty role.
   * This request should be rejected because otherwise document-level
   * filtering will be skipped.
   */
  @Test
  public void testEmptyRole() throws Exception {
    String user = "undefinedRoleUser";
    try {
      NamedList args = new NamedList();
      args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
      ResponseBuilder builder = runComponent(user, args, null);

      checkParams(null, builder);
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
      assertTrue(ex.getMessage().contains(
        user + " rejected because user is not associated with any roles"));
    }
  }

  @Test
  public void testMultipleRoles() throws Exception {
    NamedList args = new NamedList();
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
    ResponseBuilder builder = runComponent("multiGroupUser", args, null);

    String expect = getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "junit_role")
      + getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "queryOnlyAdmin_role")
      + getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "updateOnlyAdmin_role");
    checkParams(new String[] {expect}, builder);
  }

  @Test
  public void testAllRolesToken() throws Exception {
    // test no arg
    NamedList args = new NamedList();
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
    ResponseBuilder builder = runComponent("junit", args, null);
    String expect = getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "junit_role");
    checkParams(new String[] {expect}, builder);

    // test empty string arg
    args = new NamedList();
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
    args.add(QueryDocAuthorizationComponent.ALL_ROLES_TOKEN_PROP, "");
    builder = runComponent("junit", args, null);
    expect = getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "junit_role");
    checkParams(new String[] {expect}, builder);

    String allRolesToken = "specialAllRolesToken";
    args = new NamedList();
    args.add(QueryDocAuthorizationComponent.ENABLED_PROP, "true");
    args.add(QueryDocAuthorizationComponent.ALL_ROLES_TOKEN_PROP, allRolesToken);

    // test valid single group
    builder = runComponent("junit", args, null);
    expect = getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "junit_role")
      + getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, allRolesToken);
    checkParams(new String[] {expect}, builder);

    // test valid multiple group
    builder = runComponent("multiGroupUser", args, null);
    expect = getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "junit_role")
      + getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "queryOnlyAdmin_role")
      + getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, "updateOnlyAdmin_role")
      + getClause(QueryDocAuthorizationComponent.DEFAULT_AUTH_FIELD, allRolesToken);
    checkParams(new String[] {expect}, builder);
  }
}
