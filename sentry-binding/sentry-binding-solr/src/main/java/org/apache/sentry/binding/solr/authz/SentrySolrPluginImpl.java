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
import static org.apache.sentry.binding.solr.authz.SolrAuthzBinding.QUERY;
import static org.apache.sentry.binding.solr.authz.SolrAuthzBinding.UPDATE;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.sentry.binding.solr.conf.SolrAuthzConf;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.model.solr.AdminOperation;
import org.apache.sentry.core.model.solr.Collection;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.sentry.core.model.solr.SolrModelAction;
import org.apache.sentry.core.model.solr.SolrModelAuthorizable;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.security.PermissionNameProvider.Name;
import org.apache.solr.sentry.AuditLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A concrete implementation of Solr {@linkplain AuthorizationPlugin} backed by Sentry.
 *
 */
public class SentrySolrPluginImpl implements AuthorizationPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(SentrySolrPluginImpl.class);

  /**
   * A property specifies the value of the prefix to be used to define Java system property
   * for configuring the authentication mechanism. The name of the Java system property is
   * defined by appending the configuration parmeter namne to this prefix value e.g. if prefix
   * is 'solr' then the Java system property 'solr.kerberos.principal' defines the value of
   * configuration parameter 'kerberos.principal'.
   */
  private static final String SYSPROP_PREFIX_PROPERTY = "sysPropPrefix";

  /**
   * A property specifying the configuration parameters required by the Sentry authorization
   * plugin.
   */
  private static final String AUTH_CONFIG_NAMES_PROPERTY = "authConfigs";

  /**
   * A property specifying the default values for the configuration parameters specified by the
   * {@linkplain #AUTH_CONFIG_NAMES_PROPERTY} property. The default values are specified as a
   * collection of key-value pairs (i.e. property-name : default_value).
   */
  private static final String DEFAULT_AUTH_CONFIGS_PROPERTY = "defaultConfigs";

  /**
   * A configuration property specifying location of sentry-site.xml
   */
  public static final String SNTRY_SITE_LOCATION_PROPERTY = "authorization.sentry.site";

  /**
   * A configuration property specifying the Solr super-user name. The Sentry permissions
   * check will be skipped if the request is authenticated with this user name.
   */
  public static final String SENTRY_SOLR_AUTH_SUPERUSER = "authorization.superuser";

  /**
   * A configuration property to enable audit log for the Solr operations. Please note that
   * audit log is available only for operations handled by the Solr authorization framework.
   */
  public static final String SENTRY_ENABLE_SOLR_AUDITLOG = "authorization.enable.auditlog";

  /**
   * A configuration property to specify the location of Hadoop configuration files (specifically
   * core-site.xml) required to properly setup Hadoop {@linkplain UserGroupInformation} context.
   */
  public static final String SENTRY_HADOOP_CONF_DIR_PROPERTY = "authorization.sentry.hadoop.conf";

  /**
   * A configuration property to specify the kerberos principal to be used for communicating with
   * HDFS. This is required only in case of {@linkplain SimpleFileProviderBackend} when the policy
   * file is stored on HDFS.
   */
  public static final String SENTRY_HDFS_KERBEROS_PRINCIPAL = "authorization.hdfs.kerberos.principal";

  /**
   * A configuration property to specify the kerberos keytab file to be used for communicating with
   * HDFS. This is required only in case of {@linkplain SimpleFileProviderBackend} when the policy
   * file is stored on HDFS.
   */
  public static final String SENTRY_HDFS_KERBEROS_KEYTAB = "authorization.hdfs.kerberos.keytabfile";

  private String solrSuperUser;
  private SolrAuthzBinding binding;
  private Optional<AuditLogger> auditLog = Optional.empty();

  @SuppressWarnings("unchecked")
  @Override
  public void init(Map<String, Object> pluginConfig) {
    Map<String, String> params = new HashMap<>();

    String sysPropPrefix = (String) pluginConfig.getOrDefault(SYSPROP_PREFIX_PROPERTY, "solr.");
    java.util.Collection<String> authConfigNames = (java.util.Collection<String>) pluginConfig.
        getOrDefault(AUTH_CONFIG_NAMES_PROPERTY, Collections.emptyList());
    Map<String,String> authConfigDefaults = (Map<String,String>) pluginConfig
        .getOrDefault(DEFAULT_AUTH_CONFIGS_PROPERTY, Collections.emptyMap());

    for ( String configName : authConfigNames) {
      String systemProperty = sysPropPrefix + configName;
      String defaultConfigVal = authConfigDefaults.get(configName);
      String configVal = System.getProperty(systemProperty, defaultConfigVal);
      if (configVal != null) {
        params.put(configName, configVal);
      }
    }

    initializeSentry(params);
  }

  @Override
  public void close() throws IOException {
    if (this.binding != null) {
      this.binding.close();
    }
  }

  @Override
  public AuthorizationResponse authorize(AuthorizationContext authCtx) {
    if (authCtx.getUserPrincipal() == null) { // Request not authenticated.
      return AuthorizationResponse.PROMPT;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Authorizing a request with authorization context {} ", SolrAuthzUtil.toString(authCtx));
    }

    String userNameStr = getShortUserName(authCtx.getUserPrincipal());

    if (this.solrSuperUser.equals(userNameStr)) {
      return AuthorizationResponse.OK;
    }

    if (authCtx.getHandler() instanceof PermissionNameProvider) {
      Subject userName = new Subject(userNameStr);
      Name perm = ((PermissionNameProvider) authCtx.getHandler()).getPermissionName(authCtx);
      switch (perm) {
        case READ_PERM:
        case UPDATE_PERM: {
          AuthorizationResponse resp = AuthorizationResponse.FORBIDDEN;
          Set<SolrModelAction> actions = (perm == Name.READ_PERM) ? QUERY : UPDATE;
          for (CollectionRequest req : authCtx.getCollectionRequests()) {
            resp = binding.authorizeCollection(userName,
                new Collection(req.collectionName), actions);
            if (!AuthorizationResponse.OK.equals(resp)) {
              break;
            }
          }

          audit (perm, authCtx, resp);
          return resp;
        }
        case SECURITY_EDIT_PERM: {
          return binding.authorize(userName, Collections.singleton(AdminOperation.SECURITY), UPDATE);
        }
        case SECURITY_READ_PERM: {
          return binding.authorize(userName, Collections.singleton(AdminOperation.SECURITY), QUERY);
        }
        case CORE_READ_PERM:
        case CORE_EDIT_PERM:
        case COLL_READ_PERM:
        case COLL_EDIT_PERM: {
          AuthorizationResponse resp = AuthorizationResponse.FORBIDDEN;
          SolrModelAuthorizable auth = (perm == Name.COLL_READ_PERM || perm == Name.COLL_EDIT_PERM)
              ? AdminOperation.COLLECTIONS : AdminOperation.CORES;
          Set<SolrModelAction> actions = (perm == Name.COLL_READ_PERM || perm == Name.CORE_READ_PERM)
              ? QUERY : UPDATE;
          resp = binding.authorize(userName, Collections.singleton(auth), actions);
          audit (perm, authCtx, resp);
          if (AuthorizationResponse.OK.equals(resp)) {
            // Apply collection/core-level permissions check as well.
            for (Map.Entry<String, SolrModelAction> entry :
              SolrAuthzUtil.getCollectionsForAdminOp(authCtx).entrySet()) {
              resp = binding.authorizeCollection(userName,
                  new Collection(entry.getKey()), Collections.singleton(entry.getValue()));
              Name p = entry.getValue().equals(SolrModelAction.UPDATE) ? Name.UPDATE_PERM : Name.READ_PERM;
              audit(p, authCtx, resp);
              if (!AuthorizationResponse.OK.equals(resp)) {
                break;
              }
            }
          }
          return resp;
        }
        case CONFIG_EDIT_PERM: {
          return binding.authorize(userName, SolrAuthzUtil.getConfigAuthorizables(authCtx), UPDATE);
        }
        case CONFIG_READ_PERM: {
          return binding.authorize(userName, SolrAuthzUtil.getConfigAuthorizables(authCtx), QUERY);
        }
        case SCHEMA_EDIT_PERM: {
          return binding.authorize(userName, SolrAuthzUtil.getSchemaAuthorizables(authCtx), UPDATE);
        }
        case SCHEMA_READ_PERM: {
          return binding.authorize(userName, SolrAuthzUtil.getSchemaAuthorizables(authCtx), QUERY);
        }
        case METRICS_READ_PERM: {
          return binding.authorize(userName, Collections.singleton(AdminOperation.METRICS), QUERY);
        }
        case AUTOSCALING_READ_PERM:
        case AUTOSCALING_HISTORY_READ_PERM: {
          return binding.authorize(userName, Collections.singleton(AdminOperation.AUTOSCALING), QUERY);
        }
        case AUTOSCALING_WRITE_PERM: {
          return binding.authorize(userName, Collections.singleton(AdminOperation.AUTOSCALING), UPDATE);
        }
        case ALL: {
          return AuthorizationResponse.OK;
        }
      }
    }

    /*
     * The switch-case statement above handles all possible permission types. Some of the request handlers
     * in SOLR do not implement PermissionNameProvider interface and hence are incapable to providing the
     * type of permission to be enforced for this request. This is a design limitation (or a bug) on the SOLR
     * side. Until that issue is resolved, Solr/Sentry plugin needs to return OK for such requests.
     * Ref: SOLR-11623
     */
    return AuthorizationResponse.OK;
  }

  /**
   * This method returns the roles associated with the specified user name.
   */
  public Set<String> getRoles (String userName) throws SentryUserException {
    return binding.getRoles(userName);
  }

  private void initializeSentry(Map<String, String> config) {
    String sentrySiteLoc =
        Preconditions.checkNotNull(config.get(SNTRY_SITE_LOCATION_PROPERTY),
            "The authorization plugin configuration is missing " + SNTRY_SITE_LOCATION_PROPERTY
            + " property");
    String sentryHadoopConfLoc = (String)config.get(SENTRY_HADOOP_CONF_DIR_PROPERTY);

    try {
      List<URL> configFiles = getHadoopConfigFiles(sentryHadoopConfLoc);
      configFiles.add((new File(sentrySiteLoc)).toURI().toURL());

      SolrAuthzConf conf = new SolrAuthzConf(configFiles);
      if (shouldInitializeKereberos(conf)) {
        String princ = Preconditions.checkNotNull(config.get(SENTRY_HDFS_KERBEROS_PRINCIPAL),
            "The authorization plugin is missing the " + SENTRY_HDFS_KERBEROS_PRINCIPAL + " property.");
        String keytab = Preconditions.checkNotNull(config.get(SENTRY_HDFS_KERBEROS_KEYTAB),
            "The authorization plugin is missing the " + SENTRY_HDFS_KERBEROS_KEYTAB + " property.");
        initKerberos(conf, keytab, princ);
      }

      binding = new SolrAuthzBinding(conf);
      LOG.info("SolrAuthzBinding created successfully");
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to create SolrAuthzBinding", e);
    }

    this.solrSuperUser = Preconditions.checkNotNull(config.get(SENTRY_SOLR_AUTH_SUPERUSER));
    boolean enableAuditLog = Boolean.parseBoolean(
        Preconditions.checkNotNull(config.get(SENTRY_ENABLE_SOLR_AUDITLOG)));
    if (enableAuditLog) {
      this.auditLog = Optional.of(new AuditLogger());
    }
  }

  private void audit (Name perm, AuthorizationContext ctx, AuthorizationResponse resp) {
    if (!auditLog.isPresent() || !auditLog.get().isLogEnabled()) {
      return;
    }

    String userName = getShortUserName(ctx.getUserPrincipal());
    String ipAddress = ctx.getRemoteAddr();
    long eventTime = System.currentTimeMillis();
    int allowed = (resp.statusCode == AuthorizationResponse.OK.statusCode)
        ? AuditLogger.ALLOWED : AuditLogger.UNAUTHORIZED;
    String operationParams = ctx.getParams().toString();

    switch (perm) {
      case COLL_EDIT_PERM:
      case COLL_READ_PERM: {
        String collectionName = "admin";
        String actionName = ctx.getParams().get(CoreAdminParams.ACTION);
        String operationName = (actionName != null) ?
            "CollectionAction." + ctx.getParams().get(CoreAdminParams.ACTION)
            : ctx.getHandler().getClass().getName();
        auditLog.get().log (userName, null, ipAddress,
            operationName, operationParams, eventTime, allowed, collectionName);
        break;
      }

      case CORE_EDIT_PERM:
      case CORE_READ_PERM: {
        String collectionName = "admin";
        String operationName = "CoreAdminAction.STATUS";
        if (ctx.getParams().get(CoreAdminParams.ACTION) != null) {
          operationName = "CoreAdminAction." + ctx.getParams().get(CoreAdminParams.ACTION);
        }

        auditLog.get().log (userName, null, ipAddress,
            operationName, operationParams, eventTime, allowed, collectionName);
        break;
      }

      case READ_PERM:
      case UPDATE_PERM: {
        List<String> names = new ArrayList<>();
        for (CollectionRequest r : ctx.getCollectionRequests()) {
          names.add(r.collectionName);
        }
        String collectionName = String.join(",", names);
        String operationName = (perm == Name.READ_PERM) ? SolrConstants.QUERY : SolrConstants.UPDATE;
        auditLog.get().log (userName, null, ipAddress,
            operationName, operationParams, eventTime, allowed, collectionName);
        break;
      }

      default: {
        // Do nothing.
        break;
      }
    }
  }

  /**
   * Workaround until SOLR-10814 is fixed. This method allows extracting short user-name from
   * Solr provided {@linkplain Principal} instance.
   *
   * @param ctx The Solr provided authorization context
   * @return The short name of the authenticated user for this request
   */
  public static String getShortUserName (Principal princ) {
    if (princ instanceof BasicUserPrincipal) {
      return  princ.getName();
    }

    KerberosName name = new KerberosName(princ.getName());
    try {
      return name.getShortName();
    } catch (IOException e) {
      LOG.error("Error converting kerberos name. principal = {}, KerberosName.rules = {}",
                                princ, KerberosName.getRules());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unexpected error converting a kerberos name", e);
    }
  }

  /**
   * This method provides the path(s) of various Hadoop configuration files required
   * by the Sentry/Solr plugin.
   * @param confDir Location of a folder (on local file-system) storing Sentry Hadoop
   *                configuration files
   * @return A list of URLs containing the Sentry Hadoop
   *                configuration files
   */
  private List<URL> getHadoopConfigFiles(String confDir) {
    List<URL> result = new ArrayList<>();

    if (confDir != null && !confDir.isEmpty()) {
      File confDirFile = new File(confDir);
      if (!confDirFile.exists()) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Specified Sentry hadoop config directory does not exist: "
               + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.isDirectory()) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Specified Sentry hadoop config directory path is not a directory: "
               + confDirFile.getAbsolutePath());
      }
      if (!confDirFile.canRead()) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Specified Sentry hadoop config directory must be readable by the Solr process: "
               + confDirFile.getAbsolutePath());
      }

      for (String file : Arrays.asList("core-site.xml",
          "hdfs-site.xml", "ssl-client.xml")) {
        File f = new File(confDirFile, file);
        if (f.exists()) {
          try {
            result.add(f.toURI().toURL());
          } catch (MalformedURLException e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, e.getMessage(), e);
          }
        }
      }
    }

    return result;
  }

  /**
   * Initialize kerberos via UserGroupInformation.  Will only attempt to login
   * during the first request, subsequent calls will have no effect.
   */
  private void initKerberos(SolrAuthzConf authzConf, String keytabFile, String principal) {
    synchronized (SentrySolrPluginImpl.class) {
      UserGroupInformation.setConfiguration(authzConf);
      LOG.info(
          "Attempting to acquire kerberos ticket with keytab: {}, principal: {} ",
          keytabFile, principal);
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
      } catch (IOException ioe) {
        throw new SolrException(ErrorCode.SERVER_ERROR, ioe);
      }
      LOG.info("Got Kerberos ticket");
    }
  }

  private boolean shouldInitializeKereberos(SolrAuthzConf conf) {
    String providerBackend = conf.get(SolrAuthzConf.AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar());
    String authVal = conf.get(HADOOP_SECURITY_AUTHENTICATION);
    return SimpleFileProviderBackend.class.getName().equals(providerBackend)
           && "kerberos".equalsIgnoreCase(authVal);
  }

}
