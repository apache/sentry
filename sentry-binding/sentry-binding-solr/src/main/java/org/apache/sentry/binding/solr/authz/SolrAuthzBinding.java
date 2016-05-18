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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.sentry.core.model.search.SearchConstants.SENTRY_SEARCH_SERVICE_DEFAULT;
import static org.apache.sentry.core.model.search.SearchConstants.SENTRY_SEARCH_SERVICE_KEY;
import static org.apache.sentry.core.model.search.SearchModelAuthorizable.AuthorizableType.Collection;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.Config;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.generic.tools.SolrTSentryPrivilegeConverter;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class SolrAuthzBinding {
  private static final Logger LOG = LoggerFactory
      .getLogger(SolrAuthzBinding.class);
  private static final String[] HADOOP_CONF_FILES = {"core-site.xml",
    "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml"};
  public static final String KERBEROS_ENABLED = "solr.hdfs.security.kerberos.enabled";
  public static final String KERBEROS_KEYTAB = "solr.hdfs.security.kerberos.keytabfile";
  public static final String KERBEROS_PRINCIPAL = "solr.hdfs.security.kerberos.principal";
  private static final String kerberosEnabledProp = Strings.nullToEmpty(System.getProperty(KERBEROS_ENABLED)).trim();
  private static final String keytabProp = Strings.nullToEmpty(System.getProperty(KERBEROS_KEYTAB)).trim();
  private static final String principalProp = Strings.nullToEmpty(System.getProperty(KERBEROS_PRINCIPAL)).trim();
  private static Boolean kerberosInit;

  private final SolrAuthzConf authzConf;
  private final AuthorizationProvider authProvider;
  private final GroupMappingService groupMapping;
  private ProviderBackend providerBackend;
  private Subject bindingSubject;
  private boolean syncEnabled;

  public SolrAuthzBinding (SolrAuthzConf authzConf) throws Exception {
    this.authzConf = addHdfsPropsToConf(authzConf);
    this.authProvider = getAuthProvider();
    this.groupMapping = authProvider.getGroupMapping();
    /**
     * The Solr server principal will use the binding
     */
    this.bindingSubject = new Subject(UserGroupInformation.getCurrentUser()
        .getShortUserName());
    this.syncEnabled = authzConf.getBoolean("sentry.service.solr.hidden.syncEnabled", false);
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
      authzConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar());
    String serviceName = authzConf.get(SENTRY_SEARCH_SERVICE_KEY, SENTRY_SEARCH_SERVICE_DEFAULT);

    LOG.debug("Using authorization provider " + authProviderName +
      " with resource " + resourceName + ", policy engine "
      + policyEngineName + ", provider backend " + providerBackendName);
    // load the provider backend class
    if (kerberosEnabledProp.equalsIgnoreCase("true")) {
      initKerberos(keytabProp, principalProp);
    } else {
      // set configuration so that group mappings are properly setup even if
      // we don't use kerberos, for testing
      UserGroupInformation.setConfiguration(authzConf);
    }

    // for convenience, set the PrivilegeConverter.
    if (authzConf.get(ServiceConstants.ClientConfig.PRIVILEGE_CONVERTER) == null) {
      authzConf.set(ServiceConstants.ClientConfig.PRIVILEGE_CONVERTER, SolrTSentryPrivilegeConverter.class.getName());
    }

    // the SearchProviderBackend is deleted in SENTRY-828, this is for the compatible with the
    // previous Sentry.
    if ("org.apache.sentry.provider.db.generic.service.thrift.SearchProviderBackend"
        .equals(providerBackendName)) {
      providerBackendName = SentryGenericProviderBackend.class.getName();
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
      Class.forName(authProviderName).getDeclaredConstructor(Configuration.class, String.class, PolicyEngine.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {authzConf, resourceName, policyEngine});
  }


  /**
   * Authorize access to an index/collection
   * @param subject
   * @param collection
   * @param actions
   * @throws SentrySolrAuthorizationException
   */
  public void authorizeCollection(Subject subject, Collection collection, Set<SearchModelAction> actions) throws SentrySolrAuthorizationException {
    boolean isDebug = LOG.isDebugEnabled();
    if(isDebug) {
      LOG.debug("Going to authorize collection " + collection.getName() +
          " for subject " + subject.getName());
      LOG.debug("Actions: " + actions);
    }

    if (!authProvider.hasAccess(subject, Arrays.asList(new Collection[] {collection}), actions,
        ActiveRoleSet.ALL)) {
      throw new SentrySolrAuthorizationException("User " + subject.getName() +
        " does not have privileges for " + collection.getName());
    }
  }

  /**
   * Authorize access to an config
   * @param subject
   * @param config
   * @throws SentrySolrAuthorizationException
   */
  public void authorizeConfig(Subject subject, Config config) throws SentrySolrAuthorizationException {
    Set<SearchModelAction> actions = EnumSet.of(SearchModelAction.ALL);
    boolean isDebug = LOG.isDebugEnabled();
    if(isDebug) {
      LOG.debug("Going to authorize config " + config.getName() +
          " for subject " + subject.getName());
      LOG.debug("Actions: " + actions);
    }

    if (!authProvider.hasAccess(subject, Arrays.asList(new Config[] {config}), actions,
        ActiveRoleSet.ALL)) {
      throw new SentrySolrAuthorizationException("User " + subject.getName() +
        " does not have privileges for " + config.getName());
    }
  }

  /**
   * Get the list of groups the user belongs to
   * @param user
   * @return list of groups the user belongs to
   * @deprecated use getRoles instead
   */
  @Deprecated
  public Set<String> getGroups(String user) {
    return groupMapping.getGroups(user);
  }

  /**
   * Get the roles associated with the user
   * @param user
   * @return The roles associated with the user
   */
  public Set<String> getRoles(String user) {
    return providerBackend.getRoles(getGroups(user), ActiveRoleSet.ALL);
  }

  private SolrAuthzConf addHdfsPropsToConf(SolrAuthzConf conf) throws IOException {
    String confDir = System.getProperty("solr.hdfs.confdir");
    if (confDir != null && confDir.length() > 0) {
      File confDirFile = new File(confDir);
      if (!confDirFile.exists()) {
        throw new IOException("Resource directory does not exist: " + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.isDirectory()) {
        throw new IOException("Specified resource directory is not a directory" + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.canRead()) {
        throw new IOException("Resource directory must be readable by the Solr process: " + confDirFile.getAbsolutePath());
      }
      for (String file : HADOOP_CONF_FILES) {
        if (new File(confDirFile, file).exists()) {
          conf.addResource(new Path(confDir, file));
        }
      }
    }
    return conf;
  }

  /**
   * Initialize kerberos via UserGroupInformation.  Will only attempt to login
   * during the first request, subsequent calls will have no effect.
   */
  public void initKerberos(String keytabFile, String principal) {
    if (keytabFile == null || keytabFile.length() == 0) {
      throw new IllegalArgumentException("keytabFile required because kerberos is enabled");
    }
    if (principal == null || principal.length() == 0) {
      throw new IllegalArgumentException("principal required because kerberos is enabled");
    }
    synchronized (SolrAuthzBinding.class) {
      if (kerberosInit == null) {
        kerberosInit = new Boolean(true);
        final String authVal = authzConf.get(HADOOP_SECURITY_AUTHENTICATION);
        final String kerberos = "kerberos";
        if (authVal != null && !authVal.equals(kerberos)) {
          throw new IllegalArgumentException(HADOOP_SECURITY_AUTHENTICATION
              + " set to: " + authVal + ", not kerberos, but attempting to "
              + " connect to HDFS via kerberos");
        }
        // let's avoid modifying the supplied configuration, just to be conservative
        final Configuration ugiConf = new Configuration(authzConf);
        ugiConf.set(HADOOP_SECURITY_AUTHENTICATION, kerberos);
        UserGroupInformation.setConfiguration(ugiConf);
        LOG.info(
            "Attempting to acquire kerberos ticket with keytab: {}, principal: {} ",
            keytabFile, principal);
        try {
          UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        LOG.info("Got Kerberos ticket");
      }
    }
  }

  /**
   * SENTRY-478
   * If the binding uses the searchProviderBackend, it can sync privilege with Sentry Service
   */
  public boolean isSyncEnabled() {
    return (syncEnabled && providerBackend instanceof SentryGenericProviderBackend);
  }

  public SentryGenericServiceClient getClient() throws Exception {
    return SentryGenericServiceClientFactory.create(authzConf);
  }

  /**
   * Attempt to notify the Sentry service when deleting collection happened
   * @param collection
   * @throws SolrException
   */
  public void deleteCollectionPrivilege(String collection) throws SentrySolrAuthorizationException {
    if (!isSyncEnabled()) {
      return;
    }
    SentryGenericServiceClient client = null;
    try {
      client = getClient();
      TSentryPrivilege tPrivilege = new TSentryPrivilege();
      tPrivilege.setComponent(AuthorizationComponent.Search);
      tPrivilege.setServiceName(authzConf.get(SENTRY_SEARCH_SERVICE_KEY,
          SENTRY_SEARCH_SERVICE_DEFAULT));
      tPrivilege.setAction(Action.ALL);
      tPrivilege.setGrantOption(TSentryGrantOption.UNSET);
      List<TAuthorizable> authorizables = Lists.newArrayList(new TAuthorizable(Collection.name(),
          collection));
      tPrivilege.setAuthorizables(authorizables);
      client.dropPrivilege(bindingSubject.getName(), AuthorizationComponent.Search, tPrivilege);
    } catch (SentryUserException ex) {
      throw new SentrySolrAuthorizationException("User " + bindingSubject.getName() +
          " can't delete privileges for collection " + collection);
    } catch (Exception ex) {
      throw new SentrySolrAuthorizationException("Unable to obtain client:" + ex.getMessage());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
