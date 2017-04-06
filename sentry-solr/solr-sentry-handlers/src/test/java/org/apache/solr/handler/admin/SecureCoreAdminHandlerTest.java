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
package org.apache.solr.handler.admin;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.sentry.SentryTestBase;
import org.apache.solr.sentry.SentrySingletonTestInstance;
import org.eclipse.jetty.util.log.Log;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecureCoreAdminHandlerTest extends SentryTestBase {

  private static SolrCore core;
  private static CloudDescriptor cloudDescriptor;

  public final static List<CoreAdminAction> QUERY_ACTIONS = Arrays.asList(
      CoreAdminAction.STATUS,
      CoreAdminAction.REQUESTSTATUS,
      CoreAdminAction.LISTSNAPSHOTS);
  public final static List<CoreAdminAction> UPDATE_ACTIONS = Arrays.asList(
      CoreAdminAction.LOAD,
      CoreAdminAction.UNLOAD,
      CoreAdminAction.CREATE,
      CoreAdminAction.PERSIST,
      CoreAdminAction.SWAP,
      CoreAdminAction.RENAME,
      CoreAdminAction.MERGEINDEXES,
      CoreAdminAction.SPLIT,
      CoreAdminAction.PREPRECOVERY,
      CoreAdminAction.REQUESTRECOVERY,
      CoreAdminAction.REQUESTSYNCSHARD,
      CoreAdminAction.CREATEALIAS,
      CoreAdminAction.DELETEALIAS,
      CoreAdminAction.REQUESTAPPLYUPDATES,
      CoreAdminAction.REQUESTBUFFERUPDATES,
      CoreAdminAction.LOAD_ON_STARTUP,
      CoreAdminAction.TRANSIENT,
      CoreAdminAction.OVERSEEROP,
      CoreAdminAction.BACKUPCORE,
      CoreAdminAction.RESTORECORE,
      CoreAdminAction.CREATESNAPSHOT,
      CoreAdminAction.DELETESNAPSHOT,
      // RELOAD needs to go last, because our bogus calls leaves things in a bad state for later calls.
      // We could handle this more cleanly at the cost of a lot more creating and deleting cores.
      CoreAdminAction.RELOAD
      );

  // These actions require that the collection is specified on the request.
  public final static List<CoreAdminAction> REQUIRES_COLLECTION = Arrays.asList(
      CoreAdminAction.CREATE
      );

  // actions which don't check the actual collection
  public final static List<CoreAdminAction> NO_CHECK_COLLECTIONS = Arrays.asList(
      CoreAdminAction.STATUS,
      CoreAdminAction.REQUESTSTATUS,
      CoreAdminAction.LOAD,
      CoreAdminAction.PERSIST,
      CoreAdminAction.CREATEALIAS,
      CoreAdminAction.DELETEALIAS,
      CoreAdminAction.LOAD_ON_STARTUP,
      CoreAdminAction.REQUESTBUFFERUPDATES,
      CoreAdminAction.OVERSEEROP,
      CoreAdminAction.TRANSIENT
      );

  @BeforeClass
  public static void beforeClass() throws Exception {
    core = createCore("solrconfig-secureadmin.xml", "schema-minimal.xml");
    // store the CloudDescriptor, because we will overwrite it with a mock
    // and restore it later
    cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    // ensure the SentrySingletonTestInstance is initialized
    SentrySingletonTestInstance.getInstance();
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

  private SolrQueryRequest getCoreAdminRequest(String collection, String user,
      CoreAdminAction action) throws Exception {
    SolrQueryRequest req = getRequest();
    prepareCollAndUser(core, req, collection, user, false);
    ModifiableSolrParams modParams = new ModifiableSolrParams(req.getParams());
    modParams.set(CoreAdminParams.ACTION, action.name());
    modParams.set(CoreAdminParams.COLLECTION, "");
    modParams.set(CoreAdminParams.CORE, "");
    modParams.set(CoreAdminParams.NAME, "");
    for (SolrCore core : h.getCoreContainer().getCores()) {
      if(core.getCoreDescriptor().getCloudDescriptor().getCollectionName().equals(collection)) {
        modParams.set(CoreAdminParams.CORE, core.getName());
        modParams.set(CoreAdminParams.NAME, core.getName());
        break;
      }
    }
    if (REQUIRES_COLLECTION.contains(action)) {
      modParams.set(CoreAdminParams.COLLECTION, collection);
      modParams.set(CoreAdminParams.CORE, core.getName());
      modParams.set(CoreAdminParams.NAME, core.getName());
    }
    req.setParams(modParams);
    return req;
  }

  private void verifyQueryAccess(CoreAdminAction action, boolean checkCollection) throws Exception {
    CoreContainer cc = getCleanCoreContainer(action, h.getCoreContainer());
    CoreAdminHandler handler = new SecureCoreAdminHandler(cc);
    verifyAuthorized(handler, getCoreAdminRequest("collection1", "junit", action));
    verifyAuthorized(handler, getCoreAdminRequest("queryCollection", "junit", action));
    if (!checkCollection) {
      verifyAuthorized(handler, getCoreAdminRequest("bogusCollection", "junit", action));
      verifyAuthorized(handler, getCoreAdminRequest("updateCollection", "junit", action));
    } else {
      verifyUnauthorized(handler, getCoreAdminRequest("bogusCollection", "junit", action), "bogusCollection", "junit");
      verifyUnauthorized(handler, getCoreAdminRequest("updateCollection", "junit", action), "updateCollection", "junit");
    }
  }

  private void verifyUpdateAccess(CoreAdminAction action, boolean checkCollection) throws Exception {
    CoreContainer cc = getCleanCoreContainer(action, h.getCoreContainer());
    CoreAdminHandler handler = new SecureCoreAdminHandler(cc);
    verifyAuthorized(handler, getCoreAdminRequest("collection1", "junit", action));
    verifyAuthorized(handler, getCoreAdminRequest("updateCollection", "junit", action));
    verifyUnauthorized(handler, getCoreAdminRequest("bogusCollection", "bogusUser", action), "bogusCollection", "bogusUser", true);
    if (checkCollection) {
      verifyUnauthorized(handler, getCoreAdminRequest("queryCollection", "junit", action), "queryCollection", "junit");
    }
  }

  private CoreContainer getZkAwareCoreContainer(final CoreContainer cc) {
    Enhancer e = new Enhancer();
    e.setClassLoader(cc.getClass().getClassLoader());
    e.setSuperclass(CoreContainer.class);
    e.setCallback(new MethodInterceptor() {
      public Object intercept(Object obj, Method method, Object [] args, MethodProxy proxy) throws Throwable {
        if (method.getName().equals("isZooKeeperAware")) {
          return Boolean.TRUE;
        }
        return method.invoke(cc, args);
      }
    });
    return (CoreContainer)e.create();
  }

  private CoreContainer getCleanCoreContainer(CoreAdminAction action, CoreContainer cc) {
    // Ensure CoreContainer is empty
    for (String coreName : h.getCoreContainer().getCoreNames()) {
      h.getCoreContainer().unload(coreName);
    }
    for (Map.Entry entry : h.getCoreContainer().getCoreInitFailures().entrySet()) {
      String coreName = entry.getKey().toString();
      h.getCoreContainer().unload(coreName);
    }
    // actions that require the collection attempt to read the collection off the CloudDescriptor, which is only
    // present when the CoreContainer is ZkAware.
    return REQUIRES_COLLECTION.contains(action) ? getZkAwareCoreContainer(h.getCoreContainer()) : h.getCoreContainer();
  }

  @Test
  public void testSecureAdminHandler() throws Exception {
    for (CoreAdminAction action : QUERY_ACTIONS) {
      verifyQueryAccess(action, !NO_CHECK_COLLECTIONS.contains(action));
    }
    for (CoreAdminAction action : UPDATE_ACTIONS) {
      verifyUpdateAccess(action, !NO_CHECK_COLLECTIONS.contains(action));
    }
  }

  @Test
  public void testAllActionsChecked() throws Exception {
    for (CoreAdminAction action : CoreAdminAction.values()) {
      assertTrue(QUERY_ACTIONS.contains(action) || UPDATE_ACTIONS.contains(action));
    }
  }
}
