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
package org.apache.sentry.sqoop.binding;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.sqoop.Server;
import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sentry.core.model.sqoop.SqoopActionFactory;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.db.generic.SentryGenericProviderBackend;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryRole;
import org.apache.sentry.sqoop.conf.SqoopAuthConf.AuthzConfVars;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.SecurityError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SqoopAuthBinding {
  private static final Logger LOG = LoggerFactory.getLogger(SqoopAuthBinding.class);
  private static final String COMPONENT_TYPE = AuthorizationComponent.SQOOP;

  private final Configuration authConf;
  private final AuthorizationProvider authProvider;
  private final Server sqoopServer;
  private final Subject bindingSubject;
  private ProviderBackend providerBackend;

  private final SqoopActionFactory actionFactory = new SqoopActionFactory();

  public SqoopAuthBinding(Configuration authConf, String serverName) throws Exception {
    this.authConf = authConf;
    this.authConf.set(AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), serverName);
    this.sqoopServer = new Server(serverName);
    this.authProvider = createAuthProvider();
    /** The Sqoop server principal will use the binding */
    this.bindingSubject = new Subject(UserGroupInformation.getCurrentUser()
        .getShortUserName());
  }

  /**
   * Instantiate the configured authz provider
   * @return {@link AuthorizationProvider}
   */
  private AuthorizationProvider createAuthProvider() throws Exception {
    /**
     * get the authProvider class, policyEngine class, providerBackend class and resources from the sqoopAuthConf config
     */
    String authProviderName = authConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar(),AuthzConfVars.AUTHZ_PROVIDER.getDefault());
    String resourceName = authConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getDefault());
    String providerBackendName = authConf.get(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar(), AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getDefault());
    String policyEngineName = authConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar(), AuthzConfVars.AUTHZ_POLICY_ENGINE.getDefault());
    String serviceName = authConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using authorization provider " + authProviderName +
          " with resource " + resourceName + ", policy engine "
          + policyEngineName + ", provider backend " + providerBackendName);
    }

    // the SqoopProviderBackend is deleted in SENTRY-828, this is for the compatible with the
    // previous Sentry.
    if ("org.apache.sentry.sqoop.binding.SqoopProviderBackend".equals(providerBackendName)) {
      providerBackendName = SentryGenericProviderBackend.class.getName();
    }

    //Instantiate the configured providerBackend
    Constructor<?> providerBackendConstructor = Class.forName(providerBackendName)
        .getDeclaredConstructor(Configuration.class, String.class);
    providerBackendConstructor.setAccessible(true);
    providerBackend = (ProviderBackend) providerBackendConstructor.newInstance(new Object[] {
        authConf, resourceName });
    if (providerBackend instanceof SentryGenericProviderBackend) {
      ((SentryGenericProviderBackend) providerBackend).setComponentType(COMPONENT_TYPE);
      ((SentryGenericProviderBackend) providerBackend).setServiceName(serviceName);
    }

    //Instantiate the configured policyEngine
    Constructor<?> policyConstructor =
        Class.forName(policyEngineName).getDeclaredConstructor(String.class, ProviderBackend.class);
    policyConstructor.setAccessible(true);
    PolicyEngine policyEngine =
        (PolicyEngine) policyConstructor.newInstance(new Object[] {sqoopServer.getName(), providerBackend});

    //Instantiate the configured authProvider
    Constructor<?> constrctor =
        Class.forName(authProviderName).getDeclaredConstructor(Configuration.class, String.class, PolicyEngine.class);
    constrctor.setAccessible(true);
    return (AuthorizationProvider) constrctor.newInstance(new Object[] {authConf, resourceName, policyEngine});
  }

  /**
   * Authorize access to a Sqoop privilege
   * @param subject
   * @param authorizable
   * @param action
   * @return true or false
   */
  public boolean authorize(Subject subject, MPrivilege privilege) {
    List<Authorizable> authorizables = toAuthorizable(privilege.getResource());
    if (!hasServerInclude(authorizables)) {
      authorizables.add(0, sqoopServer);
    }
    return authProvider.hasAccess(subject,
        authorizables,
        Sets.newHashSet(actionFactory.getActionByName(privilege.getAction())), ActiveRoleSet.ALL);
  }

  public boolean hasServerInclude(List<Authorizable> authorizables) {
    for (Authorizable authorizable : authorizables) {
      if (authorizable.getTypeName().equalsIgnoreCase(sqoopServer.getTypeName())) {
        return true;
      }
    }
    return false;
  }

  /**
   *  The Sentry-296(generate client for connection pooling) has already finished development and reviewed by now. When it
   *  was committed to master, the getClient method was needed to refactor using the connection pool
   */
  private SentryGenericServiceClient getClient() throws Exception {
    return SentryGenericServiceClientFactory.create(authConf);
  }

  public void createRole(final Subject subject, final String role) throws SqoopException {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.createRole(subject.getName(), role, COMPONENT_TYPE);
        return null;
      }
    });
  }

  public void dropRole(final Subject subject, final String role) throws SqoopException {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.dropRole(subject.getName(), role, COMPONENT_TYPE);
        return null;
      }
    });
  }

  public List<MRole> listAllRoles(final Subject subject) throws SqoopException {
    Set<TSentryRole> tSentryRoles = execute(new Command<Set<TSentryRole>>() {
      @Override
      public Set<TSentryRole> run(SentryGenericServiceClient client)
          throws Exception {
        return client.listAllRoles(subject.getName(), COMPONENT_TYPE);
      }
    });

    List<MRole> roles = Lists.newArrayList();
    for (TSentryRole tRole : tSentryRoles) {
      roles.add(new MRole(tRole.getRoleName()));
    }
    return roles;
  }

  public List<MRole> listRolesByGroup(final Subject subject, final String groupName) throws SqoopException {
    Set<TSentryRole> tSentryRoles = execute(new Command<Set<TSentryRole>>() {
      @Override
      public Set<TSentryRole> run(SentryGenericServiceClient client)
          throws Exception {
        return client.listRolesByGroupName(subject.getName(), groupName, COMPONENT_TYPE);
      }
    });

    List<MRole> roles = Lists.newArrayList();
    for (TSentryRole tSentryRole : tSentryRoles) {
      roles.add(new MRole(tSentryRole.getRoleName()));
    }
    return roles;
  }

  public List<MPrivilege> listPrivilegeByRole(final Subject subject, final String role, final MResource resource) throws SqoopException {
    Set<TSentryPrivilege> tSentryPrivileges = execute(new Command<Set<TSentryPrivilege>>() {
      @Override
      public Set<TSentryPrivilege> run(SentryGenericServiceClient client)
          throws Exception {
        if (resource == null) {
          return client.listPrivilegesByRoleName(subject.getName(), role, COMPONENT_TYPE, sqoopServer.getName());
        } else if (resource.getType().equalsIgnoreCase(MResource.TYPE.SERVER.name())) {
          return client.listPrivilegesByRoleName(subject.getName(), role, COMPONENT_TYPE, resource.getName());
        } else {
          return client.listPrivilegesByRoleName(subject.getName(), role, COMPONENT_TYPE, sqoopServer.getName(), toAuthorizable(resource));
        }
      }
    });

    List<MPrivilege> privileges = Lists.newArrayList();
    for (TSentryPrivilege tSentryPrivilege : tSentryPrivileges) {
      privileges.add(toSqoopPrivilege(tSentryPrivilege));
    }
    return privileges;
  }

  public void grantPrivilege(final Subject subject, final String role, final MPrivilege privilege) throws SqoopException {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.grantPrivilege(subject.getName(), role, COMPONENT_TYPE, toTSentryPrivilege(privilege));
        return null;
      }
    });
  }

  public void revokePrivilege(final Subject subject, final String role, final MPrivilege privilege) throws SqoopException {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.revokePrivilege(subject.getName(), role, COMPONENT_TYPE, toTSentryPrivilege(privilege));
        return null;
      }
    });
  }

  public void grantGroupToRole(final Subject subject, final String group, final MRole role) throws SqoopException {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.addRoleToGroups(subject.getName(), role.getName(), COMPONENT_TYPE, Sets.newHashSet(group));
        return null;
      }
    });
  }

  public void revokeGroupfromRole(final Subject subject, final String group, final MRole role) throws SqoopException {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.deleteRoleToGroups(subject.getName(), role.getName(), COMPONENT_TYPE, Sets.newHashSet(group));
        return null;
      }
    });
  }

  public void renamePrivilege(final Subject subject, final MResource srcResource, final MResource dstResource) throws SqoopException {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        client.renamePrivilege(subject.getName(), COMPONENT_TYPE, sqoopServer.getName(),
            toAuthorizable(srcResource), toAuthorizable(dstResource));
        return null;
      }
    });
  }

  public void dropPrivilege(final MResource resource) throws SqoopException {
    execute(new Command<Void>() {
      @Override
      public Void run(SentryGenericServiceClient client) throws Exception {
        TSentryPrivilege privilege = new TSentryPrivilege();
        privilege.setComponent(COMPONENT_TYPE);
        privilege.setServiceName(sqoopServer.getName());
        privilege.setAuthorizables(toTSentryAuthorizable(resource));
        privilege.setAction(SqoopActionConstant.ALL);
        client.dropPrivilege(bindingSubject.getName(), COMPONENT_TYPE, privilege);
        return null;
      }
    });
  }

  private MPrivilege toSqoopPrivilege(TSentryPrivilege tPrivilege) {
    //construct a sqoop resource
    boolean grantOption = false;
    if (tPrivilege.getGrantOption() == TSentryGrantOption.TRUE) {
      grantOption = true;
    }
    //construct a sqoop privilege
    return new MPrivilege(
        toSqoopResource(tPrivilege.getAuthorizables()),
        tPrivilege.getAction().equalsIgnoreCase(SqoopActionConstant.ALL) ? SqoopActionConstant.ALL_NAME
            : tPrivilege.getAction(), grantOption);
  }

  private MResource toSqoopResource(List<TAuthorizable> authorizables) {
    if ((authorizables == null) || authorizables.isEmpty()) {
      //server resource
      return new MResource(sqoopServer.getName(), MResource.TYPE.SERVER);
    } else {
      //currently Sqoop only has one-level hierarchy authorizable resource
      return new MResource(authorizables.get(0).getName(), authorizables.get(0).getType());
    }
  }

  /**
   * construct a Sentry privilege to call by the thrift API
   * @param privilege
   * @return {@link TSentryPrivilege}
   */
  private TSentryPrivilege toTSentryPrivilege(MPrivilege privilege) {
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    tSentryPrivilege.setComponent(COMPONENT_TYPE);
    tSentryPrivilege.setServiceName(sqoopServer.getName());
    tSentryPrivilege.setAction(privilege.getAction().equalsIgnoreCase(
        SqoopActionConstant.ALL_NAME) ? SqoopActionConstant.ALL : privilege
        .getAction());
    if (privilege.isWith_grant_option()) {
      tSentryPrivilege.setGrantOption(TSentryGrantOption.TRUE);
    } else {
      tSentryPrivilege.setGrantOption(TSentryGrantOption.FALSE);
    }
    tSentryPrivilege.setAuthorizables(toTSentryAuthorizable(privilege.getResource()));
    return tSentryPrivilege;
  }


  private List<TAuthorizable> toTSentryAuthorizable(MResource resource) {
    List<TAuthorizable> tAuthorizables = Lists.newArrayList();
    /**
     * Currently Sqoop supports grant privileges on server object, but the server name must be equaled the configuration
     * of org.apache.sqoop.security.authorization.server_name in the Sqoop.properties.
     */
    if (resource.getType().equalsIgnoreCase(MResource.TYPE.SERVER.name())) {
      if (!resource.getName().equalsIgnoreCase(sqoopServer.getName())) {
        throw new IllegalArgumentException( resource.getName() + " must be equal to " + sqoopServer.getName() + "\n" +
            " Currently Sqoop supports grant/revoke privileges on server object, but the server name must be equal to the configuration " +
            "of org.apache.sqoop.security.authorization.server_name in the Sqoop.properties");
      }
    } else {
      tAuthorizables.add(new TAuthorizable(resource.getType(), resource.getName()));
    }
    return tAuthorizables;
  }

  private List<Authorizable> toAuthorizable(final MResource resource) {
    List<Authorizable> authorizables = Lists.newArrayList();
    if (resource == null) {
      return authorizables;
    }
    authorizables.add(new Authorizable() {
      @Override
      public String getTypeName() {
        return resource.getType();
      }

      @Override
      public String getName() {
        return resource.getName();
      }
    });
    return authorizables;
  }

  /**
   * A Command is a closure used to pass a block of code from individual
   * functions to execute, which centralizes connection error
   * handling. Command is parameterized on the return type of the function.
   */
  private static interface Command<T> {
    T run(SentryGenericServiceClient client) throws Exception;
  }

  private <T> T execute(Command<T> cmd) throws SqoopException {
    SentryGenericServiceClient client = null;
    try {
      client = getClient();
      return cmd.run(client);
    } catch (SentryUserException ex) {
      String msg = "Unable to excute command on sentry server: " + ex.getMessage();
      LOG.error(msg, ex);
      throw new SqoopException(SecurityError.AUTH_0014, msg, ex);
    } catch (Exception ex) {
      String msg = "Unable to obtain client:" + ex.getMessage();
      LOG.error(msg, ex);
      throw new SqoopException(SecurityError.AUTH_0014, msg, ex);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
