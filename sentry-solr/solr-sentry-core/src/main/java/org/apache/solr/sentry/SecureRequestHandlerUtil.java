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
package org.apache.solr.sentry;

import java.util.EnumSet;
import java.util.Set;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Utility functions for Secure (sentry-aware) versions of RequestHandlers
 */
public class SecureRequestHandlerUtil {
  public static final Set<SearchModelAction> QUERY_ONLY = EnumSet.of(SearchModelAction.QUERY);
  public static final Set<SearchModelAction> UPDATE_ONLY = EnumSet.of(SearchModelAction.UPDATE);
  public static final Set<SearchModelAction> QUERY_AND_UPDATE = EnumSet.of(SearchModelAction.QUERY, SearchModelAction.UPDATE);

  // Hack to provide a test-only version of SentryIndexAuthorizationSingleton
  public static SentryIndexAuthorizationSingleton testOverride = null;

  /**
   * Attempt to authorize an administrative action.
   *
   * @param req request to check
   * @param andActions set of actions to check
   * @param checkCollection check the collection the action is on, or only "admin"?
   * @param collection only relevant if checkCollection==true,
   *   use collection (if non-null) instead pulling collection name from req (if null)
   */
  public static void checkSentryAdmin(SolrQueryRequest req, Set<SearchModelAction> andActions,
      String operation, boolean checkCollection, String collection) {
    checkSentry(req, andActions, operation, true, checkCollection, collection);
  }

  /**
   * Attempt to authorize a collection action.  The collection
   * name will be pulled from the request.
   */
  public static void checkSentryCollection(SolrQueryRequest req, Set<SearchModelAction> andActions, String operation) {
    checkSentry(req, andActions, operation, false, false, null);
   }

  /**
   * Attempt to sync collection privileges with Sentry when the metadata has changed.
   * ex: When the collection has been deleted, the privileges related to the collection
   * were also needed to drop. When the collection has been renamed, the privileges must been
   * renamed too.
   */
  public static void syncDeleteCollection(String collection) {
    final SentryIndexAuthorizationSingleton sentryInstance =
        (testOverride == null)?SentryIndexAuthorizationSingleton.getInstance():testOverride;
    sentryInstance.deleteCollection(collection);
  }

  private static void checkSentry(SolrQueryRequest req, Set<SearchModelAction> andActions,
      String operation, boolean admin, boolean checkCollection, String collection) {
    // Sentry currently does have AND support for actions; need to check
    // actions one at a time
    final SentryIndexAuthorizationSingleton sentryInstance =
      (testOverride == null)?SentryIndexAuthorizationSingleton.getInstance():testOverride;
    for (SearchModelAction action : andActions) {
      if (admin) {
        sentryInstance.authorizeAdminAction(req, EnumSet.of(action), operation, checkCollection, collection);
      } else {
        sentryInstance.authorizeCollectionAction(req, EnumSet.of(action), operation);
      }
    }
  }
}
