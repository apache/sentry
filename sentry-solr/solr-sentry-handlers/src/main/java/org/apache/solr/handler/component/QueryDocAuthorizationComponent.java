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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.sentry.SentryIndexAuthorizationSingleton;
import org.apache.solr.request.LocalSolrQueryRequest;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.net.URLEncoder;

public class QueryDocAuthorizationComponent extends SearchComponent
{
  private static Logger log =
    LoggerFactory.getLogger(QueryDocAuthorizationComponent.class);
  public static String AUTH_FIELD_PROP = "sentryAuthField";
  public static String DEFAULT_AUTH_FIELD = "sentry_auth";
  public static String ALL_ROLES_TOKEN_PROP = "allRolesToken";
  public static String ENABLED_PROP = "enabled";
  private SentryIndexAuthorizationSingleton sentryInstance;
  private String authField;
  private String allRolesToken;
  private boolean enabled;

  public QueryDocAuthorizationComponent() {
    this(SentryIndexAuthorizationSingleton.getInstance());
  }

  @VisibleForTesting
  public QueryDocAuthorizationComponent(SentryIndexAuthorizationSingleton sentryInstance) {
    super();
    this.sentryInstance = sentryInstance;
  }

  @Override
  public void init(NamedList args) {
    SolrParams params = SolrParams.toSolrParams(args);
    this.authField = params.get(AUTH_FIELD_PROP, DEFAULT_AUTH_FIELD);
    log.info("QueryDocAuthorizationComponent authField: " + this.authField);
    this.allRolesToken = params.get(ALL_ROLES_TOKEN_PROP, "");
    log.info("QueryDocAuthorizationComponent allRolesToken: " + this.allRolesToken);
    this.enabled = params.getBool(ENABLED_PROP, false);
    log.info("QueryDocAuthorizationComponent enabled: " + this.enabled);
  }

  private void addRawClause(StringBuilder builder, String authField, String value) {
    // requires a space before the first term, so the
    // default lucene query parser will be used
    builder.append(" {!raw f=").append(authField).append(" v=")
      .append(value).append("}");
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (!enabled) return;

    String userName = sentryInstance.getUserName(rb.req);
    String superUser = (System.getProperty("solr.authorization.superuser", "solr"));
    if (superUser.equals(userName)) {
      return;
    }
    Set<String> roles = sentryInstance.getRoles(userName);
    if (roles != null && roles.size() > 0) {
      StringBuilder builder = new StringBuilder();
      for (String role : roles) {
        addRawClause(builder, authField, role);
      }
      if (allRolesToken != null && !allRolesToken.isEmpty()) {
        addRawClause(builder, authField, allRolesToken);
      }
      ModifiableSolrParams newParams = new ModifiableSolrParams(rb.req.getParams());
      String result = builder.toString();
      newParams.add("fq", result);
      rb.req.setParams(newParams);
    } else {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
        "Request from user: " + userName +
        " rejected because user is not associated with any roles");
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
  }

  @Override
  public String getDescription() {
    return "Handle Query Document Authorization";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
