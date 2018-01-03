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

import org.apache.sentry.binding.solr.authz.SentrySolrPluginImpl;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.AuthorizationPlugin;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

public class QueryDocAuthorizationComponent extends SearchComponent
{
  private static final Logger LOG =
    LoggerFactory.getLogger(QueryDocAuthorizationComponent.class);
  public static final String AUTH_FIELD_PROP = "sentryAuthField";
  public static final String DEFAULT_AUTH_FIELD = "sentry_auth";
  public static final String ALL_ROLES_TOKEN_PROP = "allRolesToken";
  public static final String ENABLED_PROP = "enabled";
  private static final String superUser = System.getProperty("solr.authorization.superuser", "solr");
  private String authField;
  private String allRolesToken;
  private boolean enabled;

  @Override
  public void init(NamedList args) {
    SolrParams params = SolrParams.toSolrParams(args);
    this.authField = params.get(AUTH_FIELD_PROP, DEFAULT_AUTH_FIELD);
    LOG.info("QueryDocAuthorizationComponent authField: " + this.authField);
    this.allRolesToken = params.get(ALL_ROLES_TOKEN_PROP, "");
    LOG.info("QueryDocAuthorizationComponent allRolesToken: " + this.allRolesToken);
    this.enabled = params.getBool(ENABLED_PROP, false);
    LOG.info("QueryDocAuthorizationComponent enabled: " + this.enabled);
  }

  private void addRawClause(StringBuilder builder, String authField, String value) {
    // requires a space before the first term, so the
    // default lucene query parser will be used
    builder.append(" {!raw f=").append(authField).append(" v=")
      .append(value).append("}");
  }

  public String getFilterQueryStr(Set<String> roles) {
    if (roles != null && roles.size() > 0) {
      StringBuilder builder = new StringBuilder();
      for (String role : roles) {
        addRawClause(builder, authField, role);
      }
      if (allRolesToken != null && !allRolesToken.isEmpty()) {
        addRawClause(builder, authField, allRolesToken);
      }
      return builder.toString();
    }
    return null;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (!enabled) {
      return;
    }

    String userName = getUserName(rb.req);
    if (superUser.equals(userName)) {
      return;
    }

    Set<String> roles = getRoles(rb.req, userName);
    if (roles != null && !roles.isEmpty()) {
      String filterQuery = getFilterQueryStr(roles);

      ModifiableSolrParams newParams = new ModifiableSolrParams(rb.req.getParams());
      newParams.add("fq", filterQuery);
      rb.req.setParams(newParams);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding filter query {} for user {} with roles {}", new Object[] {filterQuery, userName, roles});
      }

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

  public boolean getEnabled() {
    return enabled;
  }

  /**
   * This method return the user name from the provided {@linkplain SolrQueryRequest}
   */
  private String getUserName (SolrQueryRequest req) {
    // If a local request, treat it like a super user request; i.e. it is equivalent to an
    // http request from the same process.
    if (req instanceof LocalSolrQueryRequest) {
      return superUser;
    }

    SolrCore solrCore = req.getCore();

    HttpServletRequest httpServletRequest = (HttpServletRequest)req.getContext().get("httpRequest");
    if (httpServletRequest == null) {
      StringBuilder builder = new StringBuilder("Unable to locate HttpServletRequest");
      if (solrCore != null && solrCore.getSolrConfig().getBool(
        "requestDispatcher/requestParsers/@addHttpRequestToContext", true) == false) {
        builder.append(", ensure requestDispatcher/requestParsers/@addHttpRequestToContext is set to true in solrconfig.xml");
      }
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, builder.toString());
    }

    String userName = httpServletRequest.getRemoteUser();
    if (userName == null) {
      userName = SentrySolrPluginImpl.getShortUserName(httpServletRequest.getUserPrincipal());
    }
    if (userName == null) {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, "This request is not authenticated.");
    }

    return userName;
  }

  /**
   * This method returns the roles associated with the specified <code>userName</code>
   */
  private Set<String> getRoles (SolrQueryRequest req, String userName) {
    SolrCore solrCore = req.getCore();

    AuthorizationPlugin plugin = solrCore.getCoreContainer().getAuthorizationPlugin();
    if (!(plugin instanceof SentrySolrPluginImpl)) {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, getClass().getSimpleName() +
          " can only be used with Sentry authorization plugin for Solr");
    }
    try {
      return ((SentrySolrPluginImpl)plugin).getRoles(userName);
    } catch (SentryUserException e) {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
        "Request from user: " + userName +
        " rejected due to SentryUserException: ", e);
    }
  }

}
