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

package org.apache.solr.handler.component;

import org.apache.solr.common.util.StrUtils;
import org.apache.solr.sentry.SentryIndexAuthorizationSingleton;
import org.apache.sentry.core.model.search.SearchModelAction;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

public class QueryIndexAuthorizationComponent extends SearchComponent
{
  private static final String OPERATION_NAME = "query";
  private SentryIndexAuthorizationSingleton sentryInstance;

  public QueryIndexAuthorizationComponent() {
    this(SentryIndexAuthorizationSingleton.getInstance());
  }

  @VisibleForTesting
  public QueryIndexAuthorizationComponent(SentryIndexAuthorizationSingleton sentryInstance) {
    super();
    this.sentryInstance = sentryInstance;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    sentryInstance.authorizeCollectionAction(
      rb.req, EnumSet.of(SearchModelAction.QUERY), OPERATION_NAME);
    String collections = rb.req.getParams().get("collection");
    if (collections != null) {
      List<String> collectionList = StrUtils.splitSmart(collections, ",", true);
      // this may be an alias request: check each collection listed in "collections".
      // NOTE1: this may involve checking a collection twice, because the collection
      // on which this component is running may also be in the collections list,
      // but this simplifies the logic.  We don't want to only check the collections in the
      // list, because the user can spoof this by adding collections to non-alias requests.

      // NOTE2: we only need to do this for queries, not for updates, because updates are only
      // written to the first alias in the collection list, so are guaranteed to undergo the
      // correct sentry check
      for (String coll : collectionList) {
        sentryInstance.authorizeCollectionAction(rb.req, EnumSet.of(SearchModelAction.QUERY),
          OPERATION_NAME, coll, true);
      }
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
  }

  @Override
  public String getDescription() {
    return "Handle Query Index Authorization";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
