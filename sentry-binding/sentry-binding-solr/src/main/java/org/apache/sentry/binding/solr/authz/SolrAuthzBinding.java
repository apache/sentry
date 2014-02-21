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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf.AuthzConfVars;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.common.ProviderBackend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

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

  public SolrAuthzBinding (SolrAuthzConf authzConf) throws Exception {
    this.authzConf = authzConf;
    this.authProvider = getAuthProvider();
    this.groupMapping = authProvider.getGroupMapping();
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

    LOG.debug("Using authorization provider " + authProviderName +
      " with resource " + resourceName + ", policy engine "
      + policyEngineName + ", provider backend " + providerBackendName);
    // load the provider backend class
    Constructor<?> providerBackendConstructor =
      Class.forName(providerBackendName).getDeclaredConstructor(Configuration.class, String.class);
    providerBackendConstructor.setAccessible(true);

    if (kerberosEnabledProp.equalsIgnoreCase("true")) {
      initKerberos(keytabProp, principalProp);
    }
    Configuration conf = getConf();
    ProviderBackend providerBackend =
      (ProviderBackend) providerBackendConstructor.newInstance(new Object[] {conf, resourceName});

    // load the policy engine class
    Constructor<?> policyConstructor =
      Class.forName(policyEngineName).getDeclaredConstructor(ProviderBackend.class);
    policyConstructor.setAccessible(true);
    PolicyEngine policyEngine =
      (PolicyEngine) policyConstructor.newInstance(new Object[] {providerBackend});

    // load the authz provider class
    Constructor<?> constrctor =
      Class.forName(authProviderName).getDeclaredConstructor(String.class, PolicyEngine.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {resourceName, policyEngine});
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

    if (!authProvider.hasAccess(subject, Arrays.asList(new Collection[] {collection}), actions)) {
      throw new SentrySolrAuthorizationException("User " + subject.getName() +
        " does not have privileges for " + collection.getName());
    }
  }

  /**
   * Get the list of groups the user belongs to
   * @param user
   * @return list of groups the user belongs to
   */
  public List<String> getGroups(String user) {
    return groupMapping.getGroups(user);
  }

  private Configuration getConf() throws IOException {
    Configuration conf = new Configuration();
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
  public static void initKerberos(String keytabFile, String principal) {
    if (keytabFile == null || keytabFile.length() == 0) {
      throw new IllegalArgumentException("keytabFile required because kerberos is enabled");
    }
    if (principal == null || principal.length() == 0) {
      throw new IllegalArgumentException("principal required because kerberos is enabled");
    }
    synchronized (SolrAuthzBinding.class) {
      if (kerberosInit == null) {
        kerberosInit = new Boolean(true);
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
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
}
