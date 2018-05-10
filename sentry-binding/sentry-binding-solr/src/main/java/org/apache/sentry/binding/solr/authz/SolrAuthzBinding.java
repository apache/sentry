/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.binding.solr.authz;

import static org.apache.sentry.core.model.solr.SolrConstants.SENTRY_SOLR_SERVICE_DEFAULT;
import static org.apache.sentry.core.model.solr.SolrConstants.SENTRY_SOLR_SERVICE_KEY;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.model.solr.SolrPrivilegeModel;
import org.apache.sentry.core.model.solr.AdminOperation;
import org.apache.sentry.core.model.solr.Collection;
import org.apache.sentry.core.model.solr.SolrModelAction;
import org.apache.sentry.core.model.solr.SolrModelAuthorizable;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClient;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.api.common.ApiConstants;
import org.apache.sentry.api.tools.GenericPrivilegeConverter;
import org.apache.solr.security.AuthorizationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides functionality to initialize the Sentry (specifically
 * {@linkplain AuthorizationProvider} and {@linkplain ProviderBackend}) as
 * well as query the permissions for a given user and a specific set of
 * entities.
 */
public class SolrAuthzBinding implements Closeable {
  private static final Logger LOG = LoggerFactory
      .getLogger(SolrAuthzBinding.class);

  static final Set<SolrModelAction> QUERY = new HashSet<>(Arrays.asList(SolrModelAction.QUERY));
  static final Set<SolrModelAction> UPDATE = new HashSet<>(Arrays.asList(SolrModelAction.UPDATE));

  private final SolrAuthzConf authzConf;
  public final AuthorizationProvider authProvider;
  private final GroupMappingService groupMapping;
  public ProviderBackend providerBackend;

  /**
   * The constructor.
   *
   * @param authzConf The Sentry/Solr authorization configuration
   * @throws Exception in case of errors.
   */
  public SolrAuthzBinding(SolrAuthzConf authzConf) throws Exception {
    this.authzConf = authzConf;
    this.authProvider = getAuthProvider();
    this.groupMapping = authProvider.getGroupMapping();
  }

  @Override
  public void close() throws IOException {
    if (this.authProvider != null) {
      this.authProvider.close();
    }
  }

  // Instantiate the configured authz provider
  private AuthorizationProvider getAuthProvider() throws Exception {
    // get the provider class and resources from the authz config
    String authProviderName = authzConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar());
    String resourceName =
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
    String providerBackendName =
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar());
    String policyEngineName =
        authzConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar(), AuthzConfVars.AUTHZ_POLICY_ENGINE.getDefault());
    String serviceName = authzConf.get(SENTRY_SOLR_SERVICE_KEY, SENTRY_SOLR_SERVICE_DEFAULT);

    LOG.debug("Using authorization provider " + authProviderName +
        " with resource " + resourceName + ", policy engine "
        + policyEngineName + ", provider backend " + providerBackendName);

    // for convenience, set the PrivilegeConverter.
    if (authzConf.get(ApiConstants.ClientConfig.PRIVILEGE_CONVERTER) == null) {
      authzConf.set(ApiConstants.ClientConfig.PRIVILEGE_CONVERTER,
                       GenericPrivilegeConverter.class.getName());
    }

    Constructor<?> providerBackendConstructor =
        Class.forName(providerBackendName).getDeclaredConstructor(Configuration.class, String.class);
    providerBackendConstructor.setAccessible(true);

    providerBackend =
        (ProviderBackend) providerBackendConstructor.newInstance(new Object[] {authzConf, resourceName});

    if (providerBackend instanceof SentryGenericProviderBackend) {
      ((SentryGenericProviderBackend) providerBackend)
      .setComponentType(AuthorizationComponent.Search);
      ((SentryGenericProviderBackend) providerBackend).setServiceName(serviceName);
    }

    // Create backend context
    ProviderBackendContext context = new ProviderBackendContext();
    context.setAllowPerDatabase(false);
    context.setValidators(SolrPrivilegeModel.getInstance().getPrivilegeValidators());
    providerBackend.initialize(context);

    // load the policy engine class
    Constructor<?> policyConstructor =
        Class.forName(policyEngineName).getDeclaredConstructor(ProviderBackend.class);
    policyConstructor.setAccessible(true);
    PolicyEngine policyEngine =
        (PolicyEngine) policyConstructor.newInstance(new Object[] {providerBackend});

    // if unset, set the hadoop auth provider to use new groups, so we don't
    // conflict with the group mappings that may already be set up
    if (authzConf.get(HadoopGroupResourceAuthorizationProvider.USE_NEW_GROUPS) == null) {
      authzConf.setBoolean(HadoopGroupResourceAuthorizationProvider.USE_NEW_GROUPS ,true);
    }

    // load the authz provider class
    Constructor<?> constrctor =
        Class.forName(authProviderName).getDeclaredConstructor(Configuration.class,
            String.class, PolicyEngine.class, Model.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {authzConf, resourceName,
        policyEngine, SolrPrivilegeModel.getInstance()});
  }

  /**
   * Authorize access to a Solr operation
   * @param subject The user invoking the SOLR operation
   * @param admin The {@linkplain SolrModelAuthorizable} associated with the operation
   * @param actions The action performed as part of the operation (query or update)
   * @return {@linkplain AuthorizationResponse#OK} If the authorization is successful
   *         {@linkplain AuthorizationResponse#FORBIDDEN} if the authorization fails.
   */
  public AuthorizationResponse authorize (Subject subject,
      java.util.Collection<? extends SolrModelAuthorizable> authorizables, Set<SolrModelAction> actions) {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Going to authorize " + authorizables +
          " for subject " + subject.getName());
      LOG.debug("Actions: " + actions);
    }

    for (SolrModelAuthorizable a : authorizables) {
      if (!authProvider.hasAccess(subject, Arrays.asList(new Authorizable[] { a }),
          actions, ActiveRoleSet.ALL)) {
        return AuthorizationResponse.FORBIDDEN;
      }
    }

    return AuthorizationResponse.OK;
  }

  /**
   * Authorize access to an index/collection
   * @param subject The user invoking the SOLR collection related operation
   * @param collection The Solr collection associated with the operation
   * @param actions The action performed as part of the operation (query or update)
   * @return {@linkplain AuthorizationResponse#OK} If the authorization is successful
   *         {@linkplain AuthorizationResponse#FORBIDDEN} if the authorization fails.
   */
  public AuthorizationResponse authorizeCollection(Subject subject, Collection collection,
      Set<SolrModelAction> actions) {
    return authorize(subject, Collections.singleton(collection), actions);
  }

  /**
   * Authorize access to an admin operation
   * @param subject The user invoking the SOLR admin operation
   * @param admin The type of the admin operation
   * @param actions The action performed as part of the operation (query or update)
   * @return {@linkplain AuthorizationResponse#OK} If the authorization is successful
   *         {@linkplain AuthorizationResponse#FORBIDDEN} if the authorization fails.
   */
  public AuthorizationResponse authorizeAdminAction(Subject subject, AdminOperation admin,
      Set<SolrModelAction> actions) {
    return authorize(subject, Collections.singleton(admin), actions);
  }

  /**
   * Get the list of groups the user belongs to
   * @param user
   * @return list of groups the user belongs to
   * @deprecated use getRoles instead
   */
  @Deprecated
  public Set<String> getGroups(String user) throws SentryUserException {
    return groupMapping.getGroups(user);
  }

  /**
   * Get the roles associated with the user
   * @param user
   * @return The roles associated with the user
   * @throws SentryUserException
   */
  public Set<String> getRoles(String user) throws SentryUserException {
    return providerBackend.getRoles(getGroups(user), ActiveRoleSet.ALL);
  }

  public SentryGenericServiceClient getClient() throws Exception {
    return SentryGenericServiceClientFactory.create(authzConf);
  }
}
