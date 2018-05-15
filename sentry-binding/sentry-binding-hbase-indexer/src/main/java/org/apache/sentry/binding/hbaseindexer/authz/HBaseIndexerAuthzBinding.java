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
package org.apache.sentry.binding.hbaseindexer.authz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.binding.hbaseindexer.conf.HBaseIndexerAuthzConf;
import org.apache.sentry.binding.hbaseindexer.conf.HBaseIndexerAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.model.indexer.Indexer;
import org.apache.sentry.core.model.indexer.IndexerModelAction;
import org.apache.sentry.core.model.indexer.IndexerPrivilegeModel;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.sentry.provider.common.AuthorizationComponent.HBASE_INDEXER;

/**
 * This class provides functionality to initialize the Sentry as
 * well as query the permissions for a given user over indexers for HBase-Indexer.
 */
public class HBaseIndexerAuthzBinding {
  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseIndexerAuthzBinding.class);
  private static final String[] HADOOP_HBASE_CONF_FILES = {"core-site.xml",
    "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml", "hbase-site.xml"};
  public static final String HBASE_REGIONSERVER_KEYTAB_FILE = "hbase.regionserver.keytab.file";
  public static final String HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
  private static final AtomicBoolean kerberosInit = new AtomicBoolean(false);

  private final HBaseIndexerAuthzConf authzConf;
  private final AuthorizationProvider authProvider;
  private ProviderBackend providerBackend;

  public HBaseIndexerAuthzBinding (HBaseIndexerAuthzConf authzConf) throws Exception {
    this.authzConf = addHdfsPropsToConf(authzConf);
    this.authProvider = getAuthProvider();
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

    LOG.debug("Using HBase indexer authorization provider " + authProviderName +
      " with resource " + resourceName + ", policy engine "
      + policyEngineName + ", provider backend " + providerBackendName);
    // load the provider backend class
    Constructor<?> providerBackendConstructor =
      Class.forName(providerBackendName).getDeclaredConstructor(Configuration.class, String.class);
    providerBackendConstructor.setAccessible(true);

    if ("kerberos".equals(authzConf.get(HADOOP_SECURITY_AUTHENTICATION))) {
      // let's just reuse the hadoop/hbase properties since the HBase Indexer is
      // essentially running as an HBase RegionServer
      String keytabProp = authzConf.get(HBASE_REGIONSERVER_KEYTAB_FILE);
      String principalProp = authzConf.get(HBASE_REGIONSERVER_KERBEROS_PRINCIPAL);
      if (keytabProp != null && principalProp != null) {
        // if necessary, translate _HOST in principal specification
        String actualHost = authzConf.get(AuthzConfVars.PRINCIPAL_HOSTNAME.getVar());
        if (actualHost != null) {
          principalProp = SecurityUtil.getServerPrincipal(principalProp, actualHost);
        }
        initKerberos(keytabProp, principalProp);
      }
    }

    // For SentryGenericProviderBackend
    authzConf.set(ServiceConstants.ClientConfig.COMPONENT_TYPE, HBASE_INDEXER);

    providerBackend =
      (ProviderBackend) providerBackendConstructor.newInstance(new Object[] {authzConf, resourceName});

    // create backendContext
    ProviderBackendContext context = new ProviderBackendContext();
    context.setAllowPerDatabase(true);
    context.setValidators(IndexerPrivilegeModel.getInstance().getPrivilegeValidators());
    // initialize the backend with the context
    providerBackend.initialize(context);

    // load the policy engine class
    Constructor<?> policyConstructor =
      Class.forName(policyEngineName).getDeclaredConstructor(ProviderBackend.class);
    policyConstructor.setAccessible(true);
    PolicyEngine policyEngine =
      (PolicyEngine) policyConstructor.newInstance(new Object[] {providerBackend});

    // load the authz provider class
    Constructor<?> constrctor =
      Class.forName(authProviderName).getDeclaredConstructor(String.class, PolicyEngine.class, Model.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {resourceName, policyEngine,
        IndexerPrivilegeModel.getInstance()});
  }

  public void authorize(Subject subject, Indexer indexer, Set<IndexerModelAction> actions)
      throws SentryAccessDeniedException {
    if (!authProvider.hasAccess(subject, Arrays.asList(new Indexer[] {indexer}), actions,
        ActiveRoleSet.ALL)) {
      throw new SentryAccessDeniedException(String.format("User '%s' does not have privileges for indexer '%s'",
        (subject != null?subject.getName():""),
        (indexer != null?indexer.getName():"")
      ));
    }
  }

  public Collection<Indexer> filterIndexers(Subject subject,
      Collection<Indexer> indexers) {
    ArrayList<Indexer> filteredIndexers = new ArrayList<Indexer>();
    Set<IndexerModelAction> actions = EnumSet.of(IndexerModelAction.READ);
    for (Indexer def : indexers) {
      if (authProvider.hasAccess(subject, Arrays.asList(new Indexer[] {new Indexer(def.getName())}), actions,
          ActiveRoleSet.ALL)) {
        filteredIndexers.add(def);
      }
    }
    return filteredIndexers;
  }

  private HBaseIndexerAuthzConf addHdfsPropsToConf(HBaseIndexerAuthzConf conf) throws IOException {
    final String hdfsConfdirKey = "hbaseindxer.hdfs.confdir";
    String confDir = System.getProperty(hdfsConfdirKey, ".");
    if (confDir != null && confDir.length() > 0) {
      File confDirFile = new File(confDir);
      if (!confDirFile.exists()) {
        throw new IOException(String.format("The HBase indexer resource '%s' does not exist. Use the '%s' configuration variable to specify an existing resource directory.",
            confDirFile.getAbsolutePath(), hdfsConfdirKey));
      }
      if (!confDirFile.isDirectory()) {
        throw new IOException(String.format("The HBase indexer resource '%s' is not a directory. Use the '%s' configuration variable to specify an existing resource directory.",
            confDirFile.getAbsolutePath(), hdfsConfdirKey));
      }
      if (!confDirFile.canRead()) {
        throw new IOException(String.format("The HBase indexer resource '%s' does not have read permissions. Change the directory\n" +
                "permissions to allow the HBase indexer process to read or use the %s' configuration variable to specify an existing resource directory with read permissions.",
            confDirFile.getAbsolutePath(), hdfsConfdirKey));
      }
      for (String file : HADOOP_HBASE_CONF_FILES) {
        if (new File(confDirFile, file).exists()) {
          conf.addResource(new Path(confDir, file), true);
        }
      }
    }
    return conf;
  }

  /**
   * Initialize kerberos via UserGroupInformation.  Will only attempt to login
   * during the first request, subsequent calls will have no effect.
   * @param keytabFile path to keytab file
   * @param principal principal used for authentication
   * @throws IllegalArgumentException
   */
  private void initKerberos(String keytabFile, String principal) {
    if (keytabFile == null || keytabFile.length() == 0) {
      throw new IllegalArgumentException(String.format("Setting keytab file path required when kerberos is enabled. Use %s configuration entry to define keytab file.", HBASE_REGIONSERVER_KEYTAB_FILE));
    }
    if (principal == null || principal.length() == 0) {
      throw new IllegalArgumentException(String.format("Setting kerberos principal is required when kerberos is enabled. Use %s configuration entry to define principal.", HBASE_REGIONSERVER_KERBEROS_PRINCIPAL));
    }
    if(kerberosInit.compareAndSet(false, true)) { // init kerberos if kerberosInit is false, then set it to true
      // let's avoid modifying the supplied configuration, just to be conservative
      final Configuration ugiConf = new Configuration(authzConf);
      UserGroupInformation.setConfiguration(ugiConf);
      LOG.info(
          "Attempting to acquire kerberos ticket for HBase Indexer binding with keytab: {}, principal: {} ",
          keytabFile, principal);
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
      } catch (IOException ioe) {
        kerberosInit.set(false);
        throw new RuntimeException(ioe);
      }
      LOG.info("Got Kerberos ticket for HBase Indexer binding");
    }

  }
}
