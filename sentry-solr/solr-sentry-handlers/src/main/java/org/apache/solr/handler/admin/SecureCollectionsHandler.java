package org.apache.solr.handler.admin;

import java.util.Arrays;

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

import java.util.EnumSet;
import java.util.List;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.solr.handler.SecureRequestHandlerUtil;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.core.CoreContainer;

/**
 * Secure (sentry-aware) version of CollectionsHandler
 */
public class SecureCollectionsHandler extends CollectionsHandler {
  public final static List<CollectionAction> QUERY_ACTIONS = Arrays.asList(
      CollectionAction.LISTSNAPSHOTS);

  public SecureCollectionsHandler() {
    super();
  }

  public SecureCollectionsHandler(final CoreContainer coreContainer) {
    super(coreContainer);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Pick the action
    SolrParams params = req.getParams();
    CollectionAction action = null;
    String a = params.get(CoreAdminParams.ACTION);
    String collection = null;
    if (a != null) {
      action = CollectionAction.get(a);
    }
    if (action != null) {
      switch (action) {
        case CREATE:
        case DELETE:
        case RELOAD:
        case CREATEALIAS: // FixMe: do we need to check the underlying "collections" as well?
        case DELETEALIAS:
        {
          collection = req.getParams().required().get("name");
          break;
        }
        case SYNCSHARD:
        case SPLITSHARD:
        case DELETESHARD:
        case BACKUP:
        case RESTORE:
        case CREATESNAPSHOT:
        case DELETESNAPSHOT:
        case LISTSNAPSHOTS: {
          collection = req.getParams().required().get("collection");
          break;
        }
        default: {
          collection = null;
          break;
        }
      }
    }

    if ((action != null) && QUERY_ACTIONS.contains(action)) {
      SecureRequestHandlerUtil.checkSentryAdminCollection(req, SecureRequestHandlerUtil.QUERY_ONLY,
                (action != null ? "CollectionAction." + action.toString() : getClass().getName() + "/" + a), true, collection);
    } else {
      // all actions require UPDATE privileges
      SecureRequestHandlerUtil.checkSentryAdminCollection(req, SecureRequestHandlerUtil.UPDATE_ONLY,
          (action != null ? "CollectionAction." + action.toString() : getClass().getName() + "/" + a), true, collection);
    }

    super.handleRequestBody(req, rsp);

    /**
     * Attempt to sync collection privileges with Sentry when the metadata has changed.
     * ex: When the collection has been deleted, the privileges related to the collection
     * were also needed to drop.
     */
    if (action.equals(CollectionAction.DELETE)) {
      SecureRequestHandlerUtil.syncDeleteCollection(collection);
    }

  }
}
