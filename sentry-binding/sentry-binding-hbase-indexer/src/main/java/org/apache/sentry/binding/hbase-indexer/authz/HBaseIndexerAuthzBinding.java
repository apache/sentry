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

import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.servlet.IndexerServerException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.binding.hbaseindexer.conf.HBaseIndexerAuthzConf;
import org.apache.sentry.binding.hbaseindexer.conf.HBaseIndexerAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.indexer.Indexer;
import org.apache.sentry.core.model.indexer.IndexerModelAction;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.common.ProviderBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseIndexerAuthzBinding {
  public static final int SC_UNAUTHORIZED = 401;
  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseIndexerAuthzBinding.class);
  private static final String[] HADOOP_HBASE_CONF_FILES = {"core-site.xml",
    "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml", "hbase-site.xml"};
  private static Boolean kerberosInit;

  private final HBaseIndexerAuthzConf authzConf;
  private final AuthorizationProvider authProvider;
  private final GroupMappingService groupMapping;
  private ProviderBackend providerBackend;

  public HBaseIndexerAuthzBinding (HBaseIndexerAuthzConf authzConf) throws Exception {
    this.authzConf = addHdfsPropsToConf(authzConf);
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

    if ("kerberos".equals(authzConf.get(HADOOP_SECURITY_AUTHENTICATION))) {
      // let's just reuse the hadoop/hbase properties since the HBase Indexer is
      // essentially running as an HBase RegionServer
      String keytabProp = authzConf.get("hbase.regionserver.keytab.file");
      String principalProp = authzConf.get("hbase.regionserver.kerberos.principal");
      if (keytabProp != null && principalProp != null) {
        initKerberos(keytabProp, principalProp);
      }
    }
    providerBackend =
      (ProviderBackend) providerBackendConstructor.newInstance(new Object[] {authzConf, resourceName});

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

  public void authorizeIndexerAction(Subject subject, Indexer indexer, Set<IndexerModelAction> actions)
  throws IndexerServerException {
    if (!authProvider.hasAccess(subject, Arrays.asList(new Indexer[] {indexer}), actions,
        ActiveRoleSet.ALL)) {
      throw new IndexerServerException(SC_UNAUTHORIZED,
        new SentryHBaseIndexerAuthorizationException("User \'"
          + (subject != null?subject.getName():"") +
          "\' does not have privileges for indexer \'"
          + (indexer != null?indexer.getName():"") + "\'"));
    }
  }

  public Collection<IndexerDefinition> filterIndexers(Subject subject,
      Collection<IndexerDefinition> indexers) {
    ArrayList<IndexerDefinition> filteredIndexers = new ArrayList<IndexerDefinition>();
    Set<IndexerModelAction> actions = EnumSet.of(IndexerModelAction.READ);
    for (IndexerDefinition def : indexers) {
      if (authProvider.hasAccess(subject, Arrays.asList(new Indexer[] {new Indexer(def.getName())}), actions,
          ActiveRoleSet.ALL)) {
        filteredIndexers.add(def);
      }
    }
    return filteredIndexers;
  }

  private HBaseIndexerAuthzConf addHdfsPropsToConf(HBaseIndexerAuthzConf conf) throws IOException {
    String confDir = System.getProperty("hbaseindxer.hdfs.confdir", ".");
    if (confDir != null && confDir.length() > 0) {
      File confDirFile = new File(confDir);
      if (!confDirFile.exists()) {
        throw new IOException("Resource directory does not exist: " + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.isDirectory()) {
        throw new IOException("Specified resource directory is not a directory" + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.canRead()) {
        throw new IOException("Resource directory must be readable by the Hbase Indexer process: " + confDirFile.getAbsolutePath());
      }
      for (String file : HADOOP_HBASE_CONF_FILES) {
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
    synchronized (HBaseIndexerAuthzBinding.class) {
      if (kerberosInit == null) {
        kerberosInit = new Boolean(true);
        // let's avoid modifying the supplied configuration, just to be conservative
        final Configuration ugiConf = new Configuration(authzConf);
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
}
