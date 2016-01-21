package org.apache.solr.update.processor;
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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.sentry.SentrySingletonTestInstance;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for UpdateIndexAuthorizationProcessor
 */
public class UpdateIndexAuthorizationProcessorTest extends SentryTestBase {

  private List<String> methodNames = Arrays.asList("processAdd", "processDelete",
    "processMergeIndexes","processCommit", "processRollback", "finish");

  private static SolrCore core;
  private static CloudDescriptor cloudDescriptor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    core = createCore("solrconfig.xml", "schema-minimal.xml");
    // store the CloudDescriptor, because we will overwrite it with a mock
    // and restore it later
    cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
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

  private void verifyAuthorized(String collection, String user) throws Exception {
    SolrQueryRequestBase req = new SolrQueryRequestBase(core, new MapSolrParams(new HashMap())) {};
    getProcessor(collection, user).processAdd(new AddUpdateCommand(req));
    getProcessor(collection, user).processDelete(new DeleteUpdateCommand(req));
    DeleteUpdateCommand deleteByQueryCommand = new DeleteUpdateCommand(req);
    deleteByQueryCommand.setQuery("*:*");
    getProcessor(collection, user).processDelete(deleteByQueryCommand);
    getProcessor(collection, user).processMergeIndexes(new MergeIndexesCommand(null, req));
    getProcessor(collection, user).processCommit(new CommitUpdateCommand(req, false));
    getProcessor(collection, user).processRollback(new RollbackUpdateCommand(req));
    getProcessor(collection, user).finish();
  }

  private void verifyUnauthorizedException(SolrException ex, String exMsgContains, MutableInt numExceptions) {
    assertEquals(ex.code(), SolrException.ErrorCode.UNAUTHORIZED.code);
    assertTrue(ex.getMessage().contains(exMsgContains));
    numExceptions.add(1);
  }

  private void verifyUnauthorized(String collection, String user) throws Exception {
    MutableInt numExceptions = new MutableInt(0);
    String contains = "User " + user + " does not have privileges for " + collection;
    SolrQueryRequestBase req = new SolrQueryRequestBase(core, new MapSolrParams(new HashMap())) {};

    try {
      getProcessor(collection, user).processAdd(new AddUpdateCommand(req));
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).processDelete(new DeleteUpdateCommand(req));
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).processMergeIndexes(new MergeIndexesCommand(null, req));
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).processCommit(new CommitUpdateCommand(req, false));
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).processRollback(new RollbackUpdateCommand(req));
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }
    try {
      getProcessor(collection, user).finish();
    } catch(SolrException ex) {
      verifyUnauthorizedException(ex, contains, numExceptions);
    }

    assertEquals(methodNames.size(), numExceptions.intValue());
  }

  private UpdateIndexAuthorizationProcessor getProcessor(String collection, String user)
      throws Exception {
    SolrQueryRequest request = getRequest();
    prepareCollAndUser(core, request, collection, user);
    return new UpdateIndexAuthorizationProcessor(
      SentrySingletonTestInstance.getInstance().getSentryInstance(), request, null);
  }

 /**
  * Test the UpdateIndexAuthorizationComponent on a collection that
  * the user has ALL access
  */
  @Test
  public void testUpdateComponentAccessAll() throws Exception {
    verifyAuthorized("collection1", "junit");
  }

 /**
  * Test the UpdateIndexAuthorizationComponent on a collection that
  * the user has UPDATE only access
  */
  @Test
  public void testUpdateComponentAccessUpdate() throws Exception {
    verifyAuthorized("updateCollection", "junit");
  }

 /**
  * Test the UpdateIndexAuthorizationComponent on a collection that
  * the user has QUERY only access
  */
  @Test
  public void testUpdateComponentAccessQuery() throws Exception {
    verifyUnauthorized("queryCollection", "junit");
  }

 /**
  * Test the UpdateIndexAuthorizationComponent on a collection that
  * the user has no access
  */
  @Test
  public void testUpdateComponentAccessNone() throws Exception {
    verifyUnauthorized("noAccessCollection", "junit");
  }

  /**
   * Ensure no new methods have been added to base class that are not invoking
   * Sentry
   */
  @Test
  public void testAllMethodsChecked() throws Exception {
    Method [] methods = UpdateRequestProcessor.class.getDeclaredMethods();
    TreeSet<String> foundNames = new TreeSet<String>();
    for (Method method : methods) {
      if (Modifier.isPublic(method.getModifiers())) {
        foundNames.add(method.getName());
      }
    }
    assertEquals(methodNames.size(), foundNames.size());
    assertTrue(foundNames.containsAll(methodNames));
  }
}
