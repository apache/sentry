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
package org.apache.sentry.kafka.binding;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import kafka.security.auth.Acl;
import kafka.security.auth.Allow;
import kafka.security.auth.Allow$;
import kafka.security.auth.Operation$;
import kafka.security.auth.ResourceType$;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Sets;
import kafka.network.RequestChannel;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.kafka.KafkaActionFactory;
import org.apache.sentry.core.model.kafka.KafkaActionFactory.KafkaAction;
import org.apache.sentry.core.model.kafka.KafkaAuthorizable;
import org.apache.sentry.core.model.kafka.KafkaPrivilegeModel;
import org.apache.sentry.kafka.ConvertUtil;
import org.apache.sentry.kafka.conf.KafkaAuthConf.AuthzConfVars;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.generic.tools.GenericPrivilegeConverter;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.immutable.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

public class KafkaAuthBinding {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAuthBinding.class);
  private static final String COMPONENT_TYPE = AuthorizationComponent.KAFKA;
  private static final String COMPONENT_NAME = COMPONENT_TYPE;

  private static Boolean kerberosInit;

  private final Configuration authConf;
  private final AuthorizationProvider authProvider;
  private final KafkaActionFactory actionFactory = KafkaActionFactory.getInstance();

  private ProviderBackend providerBackend;
  private String instanceName;
  private String requestorName;
  private java.util.Map<String, ?> kafkaConfigs;


  public KafkaAuthBinding(String instanceName, String requestorName, Configuration authConf, java.util.Map<String, ?> kafkaConfigs) throws Exception {
    this.instanceName = instanceName;
    this.requestorName = requestorName;
    this.authConf = authConf;
    this.kafkaConfigs = kafkaConfigs;
    this.authProvider = createAuthProvider();
  }

  /**
   * Instantiate the configured authz provider
   *
   * @return {@link AuthorizationProvider}
   */
  private AuthorizationProvider createAuthProvider() throws Exception {
    /**
     * get the authProvider class, policyEngine class, providerBackend class and resources from the
     * kafkaAuthConf config
     */
    String authProviderName =
        authConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
            AuthzConfVars.AUTHZ_PROVIDER.getDefault());
    String resourceName =
        authConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
            AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getDefault());
    String providerBackendName =
        authConf.get(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(),
            AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getDefault());
    String policyEngineName =
        authConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar(),
            AuthzConfVars.AUTHZ_POLICY_ENGINE.getDefault());
    if (resourceName != null && resourceName.startsWith("classpath:")) {
      String resourceFileName = resourceName.substring("classpath:".length());
      resourceName = AuthorizationProvider.class.getClassLoader().getResource(resourceFileName).getPath();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using authorization provider " + authProviderName + " with resource "
          + resourceName + ", policy engine " + policyEngineName + ", provider backend "
          + providerBackendName);
    }

    // Initiate kerberos via UserGroupInformation if required
    if (ServiceConstants.ServerConfig.SECURITY_MODE_KERBEROS.equals(authConf.get(ServiceConstants.ServerConfig.SECURITY_MODE))
            && kafkaConfigs != null) {
      String keytabProp = kafkaConfigs.get(AuthzConfVars.AUTHZ_KEYTAB_FILE_NAME.getVar()).toString();
      String principalProp = kafkaConfigs.get(AuthzConfVars.AUTHZ_PRINCIPAL_NAME.getVar()).toString();
      if (keytabProp != null && principalProp != null) {
          String actualHost = kafkaConfigs.get(AuthzConfVars.AUTHZ_PRINCIPAL_HOSTNAME.getVar()).toString();
          if (actualHost != null) {
              principalProp = SecurityUtil.getServerPrincipal(principalProp, actualHost);
          }
          initKerberos(keytabProp, principalProp);
      } else {
          LOG.debug("Could not initialize Kerberos.\n" +
                  AuthzConfVars.AUTHZ_KEYTAB_FILE_NAME.getVar() + " set to " + kafkaConfigs.get(AuthzConfVars.AUTHZ_KEYTAB_FILE_NAME.getVar()).toString() + "\n" +
                  AuthzConfVars.AUTHZ_PRINCIPAL_NAME.getVar() + " set to " + kafkaConfigs.get(AuthzConfVars.AUTHZ_PRINCIPAL_NAME.getVar()).toString());
      }
    } else {
      LOG.debug("Could not initialize Kerberos as no kafka config provided. " +
              AuthzConfVars.AUTHZ_KEYTAB_FILE_NAME.getVar() + " and " + AuthzConfVars.AUTHZ_PRINCIPAL_NAME.getVar() +
              " are required configs to be able to initialize Kerberos");
    }

    // Pass sentry privileges caching settings from kafka conf to sentry's auth conf
    final Object enableCachingConfig = kafkaConfigs.get(AuthzConfVars.AUTHZ_CACHING_ENABLE_NAME.getVar());
    if (enableCachingConfig != null) {
      String enableCaching = enableCachingConfig.toString();
      if (Boolean.parseBoolean(enableCaching)) {
        authConf.set(ServiceConstants.ClientConfig.ENABLE_CACHING, enableCaching);

        final Object cacheTtlMsConfig = kafkaConfigs
            .get(AuthzConfVars.AUTHZ_CACHING_TTL_MS_NAME.getVar());
        if (cacheTtlMsConfig != null) {
          authConf.set(ServiceConstants.ClientConfig.CACHE_TTL_MS, cacheTtlMsConfig.toString());
        }

        final Object cacheUpdateFailuresCountConfig = kafkaConfigs
            .get(AuthzConfVars.AUTHZ_CACHING_UPDATE_FAILURES_COUNT_NAME.getVar());
        if (cacheUpdateFailuresCountConfig != null) {
          authConf.set(ServiceConstants.ClientConfig.CACHE_UPDATE_FAILURES_BEFORE_PRIV_REVOKE,
              cacheUpdateFailuresCountConfig.toString());
        }

        if (authConf.get(ServiceConstants.ClientConfig.PRIVILEGE_CONVERTER) == null) {
          authConf.set(ServiceConstants.ClientConfig.PRIVILEGE_CONVERTER,
              GenericPrivilegeConverter.class.getName());
        }
      }
    }

    // Instantiate the configured providerBackend
    Constructor<?> providerBackendConstructor =
        Class.forName(providerBackendName)
            .getDeclaredConstructor(Configuration.class, String.class);
    providerBackendConstructor.setAccessible(true);
    providerBackend =
        (ProviderBackend) providerBackendConstructor.newInstance(new Object[]{authConf,
            resourceName});
    if (providerBackend instanceof SentryGenericProviderBackend) {
      ((SentryGenericProviderBackend) providerBackend).setComponentType(COMPONENT_TYPE);
      ((SentryGenericProviderBackend) providerBackend).setServiceName(instanceName);
    }

    // Create backend context
    ProviderBackendContext context = new ProviderBackendContext();
    context.setAllowPerDatabase(false);
    context.setValidators(KafkaPrivilegeModel.getInstance().getPrivilegeValidators());
    providerBackend.initialize(context);

    // Instantiate the configured policyEngine
    Constructor<?> policyConstructor =
        Class.forName(policyEngineName).getDeclaredConstructor(ProviderBackend.class);
    policyConstructor.setAccessible(true);
    PolicyEngine policyEngine =
        (PolicyEngine) policyConstructor.newInstance(new Object[]{providerBackend});

    // Instantiate the configured authProvider
    Constructor<?> constructor =
        Class.forName(authProviderName).getDeclaredConstructor(Configuration.class, String.class,
            PolicyEngine.class, Model.class);
    constructor.setAccessible(true);
    return (AuthorizationProvider) constructor.newInstance(new Object[]{authConf, resourceName,
        policyEngine, KafkaPrivilegeModel.getInstance()});
  }

  /**
   * Authorize access to a Kafka privilege
   */
  public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {
      List<Authorizable> authorizables = ConvertUtil.convertResourceToAuthorizable(session.clientAddress().getHostAddress(), resource);
      Set<KafkaAction> actions = Sets.newHashSet(actionFactory.getActionByName(operation.name()));
      return authProvider.hasAccess(new Subject(getName(session)), authorizables, actions, ActiveRoleSet.ALL);
  }

  public void addAcls(scala.collection.immutable.Set<Acl> acls, final Resource resource) {
    verifyAcls(acls);
    LOG.info("Adding Acl: acl->" + acls + " resource->" + resource);

    final Iterator<Acl> iterator = acls.iterator();
    while (iterator.hasNext()) {
      final Acl acl = iterator.next();
      final String role = getRole(acl);
      if (!roleExists(role)) {
        throw new KafkaException("Can not add Acl for non-existent Role: " + role);
      }
      execute(new Command<Void>() {
        @Override
        public Void run(SentryGenericServiceClient client) throws Exception {
          client.grantPrivilege(
              requestorName, role, COMPONENT_NAME, toTSentryPrivilege(acl, resource));
          return null;
        }
      });
    }
  }

  public boolean removeAcls(scala.collection.immutable.Set<Acl> acls, final Resource resource) {
    verifyAcls(acls);
    LOG.info("Removing Acl: acl->" + acls + " resource->" + resource);
    final Iterator<Acl> iterator = acls.iterator();
    while (iterator.hasNext()) {
      final Acl acl = iterator.next();
      final String role = getRole(acl);
      try {
        execute(new Command<Void>() {
          @Override
          public Void run(SentryGenericServiceClient client) throws Exception {
            client.dropPrivilege(
                    requestorName, role, toTSentryPrivilege(acl, resource));
            return null;
          }
        });
      } catch (KafkaException kex) {
        LOG.error("Failed to remove acls.", kex);
        return false;
      }
    }

    return true;
  }

  public void addRole(final String role) {
    if (roleExists(role)) {
      throw new KafkaException("Can not create an existing role, " + role + ", again.");
    }

    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.createRole(
            requestorName, role, COMPONENT_NAME);
        return null;
      }
    });
  }

  public void addRoleToGroups(final String role, final Set<String> groups) {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.grantRoleToGroups(
            requestorName, role, COMPONENT_NAME, groups);
        return null;
      }
    });
  }

  public void dropAllRoles() {
    final List<String> roles = getAllRoles();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (String role : roles) {
          client.dropRole(requestorName, role, COMPONENT_NAME);
        }
        return null;
      }
    });
  }

  private List<String> getRolesforGroup(final String groupName) {
    final List<String> roles = new ArrayList<>();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (TSentryRole tSentryRole : client.listRolesByGroupName(requestorName, groupName, COMPONENT_NAME)) {
          roles.add(tSentryRole.getRoleName());
        }
        return null;
      }
    });

    return roles;
  }

  private SentryGenericServiceClient getClient() throws Exception {
    return SentryGenericServiceClientFactory.create(this.authConf);
  }

  public boolean removeAcls(final Resource resource) {
    LOG.info("Removing Acls for Resource: resource->" + resource);
    List<String> roles = getAllRoles();
    final List<TSentryPrivilege> tSentryPrivileges = getAllPrivileges(roles);
    try {
      execute(new Command<Void>() {
        @Override
        public Void run(SentryGenericServiceClient client) throws Exception {
          for (TSentryPrivilege tSentryPrivilege : tSentryPrivileges) {
            if (isPrivilegeForResource(tSentryPrivilege, resource)) {
              client.dropPrivilege(
                        requestorName, COMPONENT_NAME, tSentryPrivilege);
            }
          }
          return null;
        }
      });
    } catch (KafkaException kex) {
      LOG.error("Failed to remove acls.", kex);
      return false;
    }

    return true;
  }

  public scala.collection.immutable.Set<Acl> getAcls(final Resource resource) {
    final Option<scala.collection.immutable.Set<Acl>> acls = getAcls().get(resource);
    if (acls.nonEmpty()) {
      return acls.get();
    }
    return new scala.collection.immutable.HashSet<Acl>();
  }

  public Map<Resource, scala.collection.immutable.Set<Acl>> getAcls(KafkaPrincipal principal) {
    if (principal.getPrincipalType().toLowerCase().equals("group")) {
      List<String> roles = getRolesforGroup(principal.getName());
      return getAclsForRoles(roles);
    } else {
      LOG.info("Did not recognize Principal type: " + principal.getPrincipalType() + ". Returning Acls for all principals.");
      return getAcls();
    }
  }

  public Map<Resource, scala.collection.immutable.Set<Acl>> getAcls() {
    final List<String> roles = getAllRoles();
    return getAclsForRoles(roles);
  }

  /**
   * A Command is a closure used to pass a block of code from individual
   * functions to execute, which centralizes connection error
   * handling. Command is parameterized on the return type of the function.
   */
  private interface Command<T> {
    T run(SentryGenericServiceClient client) throws Exception;
  }

  private <T> T execute(Command<T> cmd) throws KafkaException {
    try (SentryGenericServiceClient client  = getClient()){
      return cmd.run(client);
    } catch (SentryUserException ex) {
      String msg = "Unable to excute command on sentry server: " + ex.getMessage();
      LOG.error(msg, ex);
      throw new KafkaException(msg, ex);
    } catch (Exception ex) {
      String msg = "Unable to obtain client:" + ex.getMessage();
      LOG.error(msg, ex);
      throw new KafkaException(msg, ex);
    }
  }

  private TSentryPrivilege toTSentryPrivilege(Acl acl, Resource resource) {
    final List<Authorizable> authorizables = ConvertUtil.convertResourceToAuthorizable(acl.host(), resource);
    final List<TAuthorizable> tAuthorizables = new ArrayList<>();
    for (Authorizable authorizable : authorizables) {
      tAuthorizables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
    }
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege(COMPONENT_NAME, instanceName, tAuthorizables, acl.operation().name());
    return tSentryPrivilege;
  }

  private String getRole(Acl acl) {
    return acl.principal().getName();
  }

  private boolean isPrivilegeForResource(TSentryPrivilege tSentryPrivilege, Resource resource) {
    final java.util.Iterator<TAuthorizable> authorizablesIterator = tSentryPrivilege.getAuthorizablesIterator();
    while (authorizablesIterator.hasNext()) {
      TAuthorizable tAuthorizable = authorizablesIterator.next();
      if (tAuthorizable.getType().equals(resource.resourceType().name())) {
        return true;
      }
    }
    return false;
  }

  private List<TSentryPrivilege> getAllPrivileges(final List<String> roles) {
    final List<TSentryPrivilege> tSentryPrivileges = new ArrayList<>();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (String role : roles) {
          tSentryPrivileges.addAll(client.listAllPrivilegesByRoleName(
                requestorName, role, COMPONENT_NAME, instanceName));
        }
        return null;
      }
    });

    return tSentryPrivileges;
  }

  private List<String> getAllRoles() {
    final List<String> roles = new ArrayList<>();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (TSentryRole tSentryRole : client.listAllRoles(requestorName, COMPONENT_NAME)) {
          roles.add(tSentryRole.getRoleName());
        }
        return null;
      }
    });

    return roles;
  }

  private Map<Resource, scala.collection.immutable.Set<Acl>> getAclsForRoles(final List<String> roles) {
    return scala.collection.JavaConverters.mapAsScalaMapConverter(
              rolePrivilegesToResourceAcls(getRoleToPrivileges(roles)))
              .asScala().toMap(Predef.<Tuple2<Resource, scala.collection.immutable.Set<Acl>>>conforms());
  }

  private java.util.Map<Resource, scala.collection.immutable.Set<Acl>> rolePrivilegesToResourceAcls(java.util.Map<String, scala.collection.immutable.Set<TSentryPrivilege>> rolePrivilegesMap) {
    final java.util.Map<Resource, scala.collection.immutable.Set<Acl>> resourceAclsMap = new HashMap<>();
    for (java.util.Map.Entry<String, scala.collection.immutable.Set<TSentryPrivilege>> rolePrivilege : rolePrivilegesMap.entrySet()) {
      scala.collection.immutable.Set<TSentryPrivilege> privileges = rolePrivilege.getValue();
      final Iterator<TSentryPrivilege> iterator = privileges.iterator();
      while (iterator.hasNext()) {
        TSentryPrivilege privilege = iterator.next();
        final List<TAuthorizable> authorizables = privilege.getAuthorizables();
        String host = null;
        String operation = privilege.getAction();
        for (TAuthorizable tAuthorizable : authorizables) {
          if (tAuthorizable.getType().equals(KafkaAuthorizable.AuthorizableType.HOST.name())) {
            host = tAuthorizable.getName();
          } else {
            Resource resource = new Resource(ResourceType$.MODULE$.fromString(tAuthorizable.getType()), tAuthorizable.getName());
            if (operation.equals("*")) {
              operation = "All";
            }
            Acl acl = new Acl(new KafkaPrincipal("role", rolePrivilege.getKey()), Allow$.MODULE$, host, Operation$.MODULE$.fromString(operation));
            Set<Acl> newAclsJava = new HashSet<Acl>();
            newAclsJava.add(acl);
            addExistingAclsForResource(resourceAclsMap, resource, newAclsJava);
            final scala.collection.mutable.Set<Acl> aclScala = JavaConversions.asScalaSet(newAclsJava);
            resourceAclsMap.put(resource, aclScala.<Acl>toSet());
          }
        }
      }
    }

    return resourceAclsMap;
  }

  private java.util.Map<String, scala.collection.immutable.Set<TSentryPrivilege>> getRoleToPrivileges(final List<String> roles) {
    final java.util.Map<String, scala.collection.immutable.Set<TSentryPrivilege>> rolePrivilegesMap = new HashMap<>();
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        for (String role : roles) {
          final Set<TSentryPrivilege> rolePrivileges = client.listAllPrivilegesByRoleName(
              requestorName, role, COMPONENT_NAME, instanceName);
          final scala.collection.immutable.Set<TSentryPrivilege> rolePrivilegesScala =
              scala.collection.JavaConverters.asScalaSetConverter(rolePrivileges).asScala().toSet();
          rolePrivilegesMap.put(role, rolePrivilegesScala);
        }
        return null;
      }
    });

    return rolePrivilegesMap;
  }

  private void addExistingAclsForResource(java.util.Map<Resource, scala.collection.immutable.Set<Acl>> resourceAclsMap, Resource resource, Set<Acl> newAclsJava) {
    final scala.collection.immutable.Set<Acl> existingAcls = resourceAclsMap.get(resource);
    if (existingAcls != null) {
      final Iterator<Acl> aclsIter = existingAcls.iterator();
      while (aclsIter.hasNext()) {
        Acl curAcl = aclsIter.next();
        newAclsJava.add(curAcl);
      }
    }
  }

  private boolean roleExists(String role) {
      return getAllRoles().contains(role);
  }

  private void verifyAcls(scala.collection.immutable.Set<Acl> acls) {
    final Iterator<Acl> iterator = acls.iterator();
    while (iterator.hasNext()) {
      final Acl acl = iterator.next();
      assert acl.principal().getPrincipalType().toLowerCase().equals("role") : "Only Acls with KafkaPrincipal of type \"role;\" is supported.";
      assert acl.permissionType().name().equals(Allow.name()) : "Only Acls with Permission of type \"Allow\" is supported.";
    }
  }

  /*
  * For SSL session's Kafka creates user names with "CN=" prepended to the user name.
  * "=" is used as splitter by Sentry to parse key value pairs and so it is required to strip off "CN=".
  * */
  private String getName(RequestChannel.Session session) {
    final String principalName = session.principal().getName();
    int start = principalName.indexOf("CN=");
    if (start >= 0) {
      String tmpName, name = "";
      tmpName = principalName.substring(start + 3);
      int end = tmpName.indexOf(",");
      if (end > 0) {
          name = tmpName.substring(0, end);
      } else {
          name = tmpName;
      }
      return name;
    } else {
      return principalName;
    }
  }

  /**
   * Initialize kerberos via UserGroupInformation.  Will only attempt to login
   * during the first request, subsequent calls will have no effect.
   */
  private void initKerberos(String keytabFile, String principal) {
    if (keytabFile == null || keytabFile.length() == 0) {
      throw new IllegalArgumentException("keytabFile required because kerberos is enabled");
    }
    if (principal == null || principal.length() == 0) {
      throw new IllegalArgumentException("principal required because kerberos is enabled");
    }
    synchronized (KafkaAuthBinding.class) {
      if (kerberosInit == null) {
        kerberosInit = Boolean.TRUE;
        // let's avoid modifying the supplied configuration, just to be conservative
        final Configuration ugiConf = new Configuration();
        ugiConf.set(HADOOP_SECURITY_AUTHENTICATION, ServiceConstants.ServerConfig.SECURITY_MODE_KERBEROS);
        UserGroupInformation.setConfiguration(ugiConf);
        LOG.info(
                "Attempting to acquire kerberos ticket with keytab: {}, principal: {} ",
                keytabFile, principal);
        try {
          UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        } catch (IOException ioe) {
          throw new RuntimeException("Failed to login user with Principal: " + principal +
                    " and Keytab file: " + keytabFile, ioe);
        }
        LOG.info("Got Kerberos ticket");
      }
    }
  }
}
