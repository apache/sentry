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

import java.net.URL;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.sentry.binding.solr.authz.SentrySolrAuthorizationException;
import org.apache.sentry.binding.solr.authz.SolrAuthzBinding;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryIndexAuthorizationSingleton {

  private static final Logger LOG =
    LoggerFactory.getLogger(SentryIndexAuthorizationSingleton.class);

  /**
   * Java system property for specifying location of sentry-site.xml
   */
  public static final String propertyName = "solr.authorization.sentry.site";

  /**
   * {@link HttpServletRequest} attribute for requesting user name
   */
  public static final String USER_NAME = "solr.user.name";

  /**
   * {@link HttpServletRequest} attribute for requesting do as user.
   */
  public static final String DO_AS_USER_NAME = "solr.do.as.user.name";

  private static final SentryIndexAuthorizationSingleton INSTANCE =
    new SentryIndexAuthorizationSingleton(System.getProperty(propertyName));

  private final SolrAuthzBinding binding;
  private final AuditLogger auditLogger = new AuditLogger();

  private SentryIndexAuthorizationSingleton(String sentrySiteLocation) {
    SolrAuthzBinding tmpBinding = null;
    try {
      if (sentrySiteLocation != null && sentrySiteLocation.length() > 0) {
        tmpBinding =
          new SolrAuthzBinding(new SolrAuthzConf(new URL("file://" + sentrySiteLocation)));
        LOG.info("SolrAuthzBinding created successfully");
      } else {
        LOG.info("SolrAuthzBinding not created because " + propertyName
          + " not set, sentry not enabled");
      }
    } catch (Exception ex) {
      LOG.error("Unable to create SolrAuthzBinding", ex);
    }
    binding = tmpBinding;
  }

  public static SentryIndexAuthorizationSingleton getInstance() {
    return INSTANCE;
  }

  /**
   * Returns true iff Sentry index authorization checking is enabled
   */
  public boolean isEnabled() {
    return binding != null;
  }

  /**
   * Attempt to authorize an administrative action.
   *
   * @param req request to check
   * @param actions set of actions to check
   * @param checkCollection check the collection the action is on, or only "admin"?
   * @param collection only relevant if checkCollection==true,
   *   use collection (if non-null) instead pulling collection name from req (if null)
   */
  public void authorizeAdminAction(SolrQueryRequest req,
      Set<SearchModelAction> actions, String operation, boolean checkCollection, String collection)
      throws SolrException {
    authorizeCollectionAction(req, actions, operation, "admin", true);
    if (checkCollection) {
      // Let's not error out if we can't find the collection associated with an
      // admin action, it's pretty complicated to get all the possible administrative
      // actions correct.  Instead, let's warn in the log and address any issues we
      // find.
      authorizeCollectionAction(req, actions, operation, collection, false);
    }
  }

  /**
   * Attempt to authorize a collection action.  The collection
   * name will be pulled from the request.
   */
  public void authorizeCollectionAction(SolrQueryRequest req,
      Set<SearchModelAction> actions, String operation) throws SolrException {
    authorizeCollectionAction(req, actions, operation, null, true);
  }

  /**
   * Attempt to authorize a collection action.
   *
   * @param req request to check
   * @param actions set of actions to check
   * @param collectionName the collection to check.  If null, the collection
   *   name is pulled from the request
   * @param errorIfNoCollection is true, throw an exception if collection
   *   cannot be located
   */
  public void authorizeCollectionAction(SolrQueryRequest req,
      Set<SearchModelAction> actions, String operation, String collectionName,
      boolean errorIfNoCollection)
      throws SolrException {

    Subject superUser = new Subject(System.getProperty("solr.authorization.superuser", "solr"));
    Subject userName = new Subject(getUserName(req));
    long eventTime = req.getStartTime();
    String paramString = req.getParamString();
    String impersonator = getImpersonatorName(req);

    String ipAddress = null;
    HttpServletRequest sreq = (HttpServletRequest) req.getContext().get("httpRequest");
    if (sreq != null) {
      try {
        ipAddress = sreq.getRemoteAddr();
      } catch (AssertionError e) {
        // ignore
        // This is a work-around for "Unexpected method call getRemoteAddr()"
        // exception during unit test mocking at
        // com.sun.proxy.$Proxy28.getRemoteAddr(Unknown Source)
      }
    }

    String newCollectionName = collectionName;
    if (newCollectionName == null) {
      SolrCore solrCore = req.getCore();
      if (solrCore == null) {
        String msg = "Unable to locate collection for sentry to authorize because "
          + "no SolrCore attached to request";
        if (errorIfNoCollection) {
          auditLogger.log(userName.getName(), impersonator, ipAddress,
              operation, paramString, eventTime, AuditLogger.UNAUTHORIZED, "");
          throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, msg);
        } else { // just warn
          LOG.warn(msg);
          auditLogger.log(userName.getName(), impersonator, ipAddress,
              operation, paramString, eventTime, AuditLogger.ALLOWED, "");
          return;
        }
      }
      newCollectionName = solrCore.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    }

    Collection collection = new Collection(newCollectionName);
    try {
      if (!superUser.getName().equals(userName.getName())) {
        binding.authorizeCollection(userName, collection, actions);
      }
    } catch (SentrySolrAuthorizationException ex) {
      auditLogger.log(userName.getName(), impersonator, ipAddress,
          operation, paramString, eventTime, AuditLogger.UNAUTHORIZED, newCollectionName);
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, ex);
    }

    auditLogger.log(userName.getName(), impersonator, ipAddress,
        operation, paramString, eventTime, AuditLogger.ALLOWED, newCollectionName);
  }

  /**
   * Get the roles associated with the user
   * @param userName to get roles for
   * @return The roles associated with the user
   */
  public Set<String> getRoles(String userName) {
    if (binding == null) {
      return null;
    }
    return binding.getRoles(userName);
  }

  /**
   * Get the user name associated with the request
   *
   * @param req the request
   * @return the user name associated with the request
   */
  public String getUserName(SolrQueryRequest req) throws SolrException {
    if (binding == null) {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
        "Solr binding was not created successfully.  Defaulting to no access");
    }
    SolrCore solrCore = req.getCore();
    HttpServletRequest httpServletRequest = (HttpServletRequest)req.getContext().get("httpRequest");

    // LocalSolrQueryRequests won't have the HttpServletRequest because there is no
    // http request associated with it.
    if (httpServletRequest == null && !(req instanceof LocalSolrQueryRequest)) {
      StringBuilder builder = new StringBuilder("Unable to locate HttpServletRequest");
      if (solrCore != null && !solrCore.getSolrConfig().getBool(
        "requestDispatcher/requestParsers/@addHttpRequestToContext", true)) {
        builder.append(", ensure requestDispatcher/requestParsers/@addHttpRequestToContext is set to true");
      }
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, builder.toString());
    }

    String superUser = System.getProperty("solr.authorization.superuser", "solr");
    // If a local request, treat it like a super user request; i.e. it is equivalent to an
    // http request from the same process.
    return req instanceof LocalSolrQueryRequest?
      superUser : (String)httpServletRequest.getAttribute(USER_NAME);
  }

  private String getImpersonatorName(SolrQueryRequest req) {
    HttpServletRequest httpServletRequest = (HttpServletRequest)req.getContext().get("httpRequest");
    if (httpServletRequest != null) {
      return (String)httpServletRequest.getAttribute(DO_AS_USER_NAME);
    }
    return null;
  }

  /**
   * Attempt to notify the Sentry service when deleting collection happened
   * @param collection
   * @throws SolrException
   */
  public void deleteCollection(String collection) throws SolrException {
    try {
      binding.deleteCollectionPrivilege(collection);
    } catch (SentrySolrAuthorizationException ex) {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED, ex);
    }
  }
}
