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

import javax.servlet.http.HttpServletRequest;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;

import java.lang.reflect.Field;

import org.junit.Assert;

/**
 * Base class for Sentry tests
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    QuickPatchThreadsFilter.class,
    HadoopThreadFilter.class
})
public abstract class SentryTestBase extends SolrTestCaseJ4 {

  private static final String USER_NAME = "solr.user.name";

  private SolrQueryRequest request;

  public void setUp(SolrCore core) throws Exception {
    super.setUp();
    request = new SolrQueryRequestBase( core, new ModifiableSolrParams() ) { };
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    request.close();
  }

  public static SolrCore createCore(String solrconfig, String schema) throws Exception {
    initCore(solrconfig, schema, "sentry-handlers/solr");
    return h.getCoreContainer().getCore("collection1");
  }

  public static void closeCore(SolrCore coreToClose, CloudDescriptor cloudDescriptor)
      throws Exception {
    if (cloudDescriptor != null) {
      CoreDescriptor coreDescriptor = coreToClose.getCoreDescriptor();
      Field cloudDescField = CoreDescriptor.class.getDeclaredField("cloudDesc");
      cloudDescField.setAccessible(true);
      cloudDescField.set(coreDescriptor, cloudDescriptor);
    }
    coreToClose.close();
  }

  protected SolrQueryRequest getRequest() {
    return request;
  }

  protected SolrQueryRequest prepareCollAndUser(SolrCore core, SolrQueryRequest request,
      String collection, String user) throws Exception {
    return prepareCollAndUser(core, request, collection, user, true);
  }

  protected SolrQueryRequest prepareCollAndUser(SolrCore core, SolrQueryRequest request,
      String collection, String user, boolean onlyOnce) throws Exception {
    CloudDescriptor mCloudDescriptor = EasyMock.createMock(CloudDescriptor.class);
    IExpectationSetters getCollNameExpect = EasyMock.expect(mCloudDescriptor.getCollectionName()).andReturn(collection);
    getCollNameExpect.anyTimes();
    IExpectationSetters getShardIdExpect = EasyMock.expect(mCloudDescriptor.getShardId()).andReturn("shard1");
    getShardIdExpect.anyTimes();
    EasyMock.replay(mCloudDescriptor);
    CoreDescriptor coreDescriptor = core.getCoreDescriptor();
    Field cloudDescField = CoreDescriptor.class.getDeclaredField("cloudDesc");
    cloudDescField.setAccessible(true);
    cloudDescField.set(coreDescriptor, mCloudDescriptor);

    return prepareUser(request, user, onlyOnce);
  }

  protected SolrQueryRequest prepareUser(SolrQueryRequest request, String user, boolean onlyOnce) throws Exception {
    HttpServletRequest httpServletRequest = EasyMock.createMock(HttpServletRequest.class);
    IExpectationSetters getAttributeExpect =
        EasyMock.expect(httpServletRequest.getAttribute(USER_NAME)).andReturn(user);
    if(!onlyOnce) getAttributeExpect.anyTimes();
    EasyMock.replay(httpServletRequest);
    request.getContext().put("httpRequest", httpServletRequest);
    return request;
  }

  private void verifyAuthorized(SolrRequestHandler handler,
      RequestHandlerBase handlerBase, SolrQueryRequest req) throws Exception {
    assert((handler == null && handlerBase != null)
        || (handler != null && handlerBase == null));
    SolrQueryResponse rsp = new SolrQueryResponse();
    // just ensure we don't get an unauthorized exception
    try {
      if (handler != null) {
        handler.handleRequest(req, rsp);
      } else {
        handlerBase.handleRequestBody(req, rsp);
      }
    } catch (SolrException ex) {
      assertFalse(ex.code() == SolrException.ErrorCode.UNAUTHORIZED.code);
    } catch (Exception ex) {
      // okay, we only want to verify we didn't get an Unauthorized exception,
      // going to treat each handler as a black box.
    }
  }

  protected void verifyAuthorized(RequestHandlerBase handlerBase,
      SolrQueryRequest req) throws Exception {
    verifyAuthorized(null, handlerBase, req);
  }


  protected void verifyAuthorized(SolrRequestHandler handler,
      SolrQueryRequest req) throws Exception {
    verifyAuthorized(handler, null, req);
  }

  protected void verifyUnauthorized(SolrRequestHandler handler,
      RequestHandlerBase handlerBase, SolrQueryRequest req, String collection, String user, boolean shouldFailAdmin)
          throws Exception {
    assert((handler == null && handlerBase != null)
        || (handler != null && handlerBase == null));
    String exMsgContains = "User " + user + " does not have privileges for " + (shouldFailAdmin?"admin":collection);
    SolrQueryResponse rsp = new SolrQueryResponse();
    try {
      if (handler!= null) {
        handler.handleRequest(req, rsp);
        if (rsp.getException() != null) {
          throw rsp.getException();
        }
      } else {
        handlerBase.handleRequestBody(req, rsp);
        if (rsp.getException() != null) {
          throw rsp.getException();
        }
      }
      Assert.fail("Expected SolrException");
    } catch (SolrException ex) {
      assertEquals(SolrException.ErrorCode.UNAUTHORIZED.code, ex.code());
      assertTrue(ex.getMessage().contains(exMsgContains));
    } catch (Exception ex) {
      Assert.fail("Expected SolrException");
    }
  }

  protected void verifyUnauthorized(RequestHandlerBase handlerBase,
      SolrQueryRequest req, String collection, String user, boolean shouldFailAdmin) throws Exception {
    verifyUnauthorized(null, handlerBase, req, collection, user, shouldFailAdmin);
  }

  protected void verifyUnauthorized(RequestHandlerBase handlerBase,
      SolrQueryRequest req, String collection, String user) throws Exception {
    verifyUnauthorized(null, handlerBase, req, collection, user, false);
  }

  protected void verifyUnauthorized(SolrRequestHandler handler,
      SolrQueryRequest req, String collection, String user) throws Exception {
    verifyUnauthorized(handler, null, req, collection, user, false);
  }
}
