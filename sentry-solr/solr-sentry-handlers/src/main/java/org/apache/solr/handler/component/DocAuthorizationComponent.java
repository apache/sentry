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

import org.apache.sentry.binding.solr.authz.SentrySolrPluginImpl;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.AuthorizationPlugin;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Set;

public abstract class DocAuthorizationComponent extends SearchComponent {
  public static final String SUPERUSER = System.getProperty("solr.authorization.superuser", "solr");

  public abstract boolean getEnabled();

  public abstract void prepare(ResponseBuilder rb, String userName) throws IOException;

  /**
   * This method returns the roles associated with the specified <code>userName</code>
   */
  protected final Set<String> getRoles(SolrQueryRequest req, String userName) {
    SolrCore solrCore = req.getCore();

    AuthorizationPlugin plugin = solrCore.getCoreContainer().getAuthorizationPlugin();
    if (!(plugin instanceof SentrySolrPluginImpl)) {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
          getClass().getSimpleName() + " can only be used with Sentry authorization plugin for Solr");
    }
    try {
      return ((SentrySolrPluginImpl) plugin).getRoles(userName);
    } catch (SentryUserException e) {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
          "Request from user: " + userName + " rejected due to SentryUserException: ", e);
    }
  }

  /**
   * This method return the user name from the provided {@linkplain SolrQueryRequest}
   */
  protected final String getUserName(SolrQueryRequest req) {
    // If a local request, treat it like a super user request; i.e. it is equivalent to an
    // http request from the same process.
    if (req instanceof LocalSolrQueryRequest) {
      return SUPERUSER;
    }

    SolrCore solrCore = req.getCore();

    HttpServletRequest httpServletRequest = (HttpServletRequest) req.getContext().get("httpRequest");
    if (httpServletRequest == null) {
      StringBuilder builder = new StringBuilder("Unable to locate HttpServletRequest");
      if (solrCore != null && !solrCore.getSolrConfig().getBool("requestDispatcher/requestParsers/@addHttpRequestToContext", true)) {
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

  @Override
  public final void prepare(ResponseBuilder rb) throws IOException {
    if (!getEnabled()) {
      return;
    }

    String userName = getUserName(rb.req);
    if (SUPERUSER.equals(userName)) {
      return;
    }

    prepare(rb, userName);
  }
}
