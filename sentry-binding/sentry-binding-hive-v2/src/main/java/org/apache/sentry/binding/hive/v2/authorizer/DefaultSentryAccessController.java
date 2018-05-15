/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.binding.hive.v2.authorizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.SentryHiveConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.sentry.binding.hive.SentryOnFailureHookContext;
import org.apache.sentry.binding.hive.SentryOnFailureHookContextImpl;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding.HiveHook;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.binding.hive.v2.util.SentryAuthorizerUtil;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class DefaultSentryAccessController extends SentryHiveAccessController {

  public static final Logger LOG = LoggerFactory.getLogger(DefaultSentryAccessController.class);

  public static final String REQUIRED_AUTHZ_SERVER_NAME = "Config "
      + AuthzConfVars.AUTHZ_SERVER_NAME.getVar() + " is required";

  private HiveAuthenticationProvider authenticator;
  private String serverName;
  private HiveConf conf;
  private HiveAuthzConf authzConf;
  private HiveAuthzSessionContext ctx;

  private HiveHook hiveHook;
  private HiveAuthzBinding hiveAuthzBinding;
  protected SentryPolicyServiceClient sentryClient;


  public DefaultSentryAccessController(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) throws Exception {
    initilize(conf, authzConf, authenticator, ctx);
    this.hiveHook = HiveHook.HiveServer2;
  }

  public DefaultSentryAccessController(HiveHook hiveHook, HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) throws Exception {
    initilize(conf, authzConf, authenticator, ctx);
    this.hiveHook = hiveHook;
  }

  /**
   * initialize authenticator and hiveAuthzBinding.
   */
  protected void initilize(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) throws Exception {
    Preconditions.checkNotNull(conf, "HiveConf cannot be null");
    Preconditions.checkNotNull(authzConf, "HiveAuthzConf cannot be null");
    Preconditions.checkNotNull(authenticator, "Hive authenticator provider cannot be null");
    Preconditions.checkNotNull(ctx, "HiveAuthzSessionContext cannot be null");

    this.conf = conf;
    this.authzConf = authzConf;
    this.authenticator = authenticator;
    this.ctx = ctx;
    this.serverName =
        Preconditions.checkNotNull(authzConf.get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()),
            REQUIRED_AUTHZ_SERVER_NAME);
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException {
    if (AccessConstants.RESERVED_ROLE_NAMES.contains(roleName.toUpperCase())) {
      String msg =
          "Roles cannot be one of the reserved roles: " + AccessConstants.RESERVED_ROLE_NAMES;
      throw new HiveAccessControlException(msg);
    }
    try {
      sentryClient = getSentryClient();
      sentryClient.createRole(authenticator.getUserName(), roleName);
    } catch (SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.CREATEROLE;
      executeOnFailureHooks(hiveOp, e);
    } catch (SentryUserException e) {
      String msg = "Sentry failed to create role: " + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
    }
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    if (AccessConstants.RESERVED_ROLE_NAMES.contains(roleName.toUpperCase())) {
      String msg =
          "Roles cannot be one of the reserved roles: " + AccessConstants.RESERVED_ROLE_NAMES;
      throw new HiveAccessControlException(msg);
    }
    try {
      sentryClient = getSentryClient();
      sentryClient.dropRole(authenticator.getUserName(), roleName);
    } catch (SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.DROPROLE;
      executeOnFailureHooks(hiveOp, e);
    } catch (SentryUserException e) {
      String msg = "Sentry failed to drop role: " + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
    }
  }

  @Override
  public List<String> getAllRoles() throws HiveAccessControlException, HiveAuthzPluginException {
    List<String> roles = new ArrayList<String>();
    try {
      sentryClient = getSentryClient();
      roles = convert2RoleList(sentryClient.listAllRoles(authenticator.getUserName()));
    } catch (SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.SHOW_ROLES;
      executeOnFailureHooks(hiveOp, e);
    } catch (SentryUserException e) {
      String msg = "Error when sentryClient listRoles: " + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
    }
    return roles;
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException,
      HiveAccessControlException {
    grantOrRevokePrivlegeOnRole(hivePrincipals, hivePrivileges, hivePrivObject, grantOption, true);
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException,
      HiveAccessControlException {
    grantOrRevokePrivlegeOnRole(hivePrincipals, hivePrivileges, hivePrivObject, grantOption, false);
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
      HiveAccessControlException {
    grantOrRevokeRoleOnGroup(hivePrincipals, roles, grantorPrinc, true);
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
      HiveAccessControlException {
    grantOrRevokeRoleOnGroup(hivePrincipals, roles, grantorPrinc, false);
  }


  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException, HiveAccessControlException {
    if (principal.getType() != HivePrincipalType.ROLE) {
      String msg =
          SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principal.getType();
      throw new HiveAuthzPluginException(msg);
    }
    List<HivePrivilegeInfo> infoList = new ArrayList<HivePrivilegeInfo>();
    try {
      sentryClient = getSentryClient();
      List<List<DBModelAuthorizable>> authorizables =
          SentryAuthorizerUtil.getAuthzHierarchy(new Server(serverName), privObj);
      Set<TSentryPrivilege> tPrivilges = new HashSet<TSentryPrivilege>();
      if (authorizables != null && !authorizables.isEmpty()) {
        for (List<? extends Authorizable> authorizable : authorizables) {
          tPrivilges.addAll(sentryClient.listPrivilegesByRoleName(authenticator.getUserName(),
              principal.getName(), authorizable));
        }
      } else {
        tPrivilges.addAll(sentryClient.listPrivilegesByRoleName(authenticator.getUserName(),
            principal.getName(), null));
      }

      if (tPrivilges != null && !tPrivilges.isEmpty()) {
        for (TSentryPrivilege privilege : tPrivilges) {
          infoList.add(SentryAuthorizerUtil.convert2HivePrivilegeInfo(privilege, principal));
        }
      }
    } catch (SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.SHOW_GRANT;
      executeOnFailureHooks(hiveOp, e);
    } catch (SentryUserException e) {
      String msg = "Error when sentryClient listPrivilegesByRoleName: " + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
    }
    return infoList;
  }

  @Override
  public void setCurrentRole(String roleName) throws HiveAccessControlException,
      HiveAuthzPluginException {
    try {
      sentryClient = getSentryClient();
      hiveAuthzBinding = new HiveAuthzBinding(hiveHook, conf, authzConf);
      hiveAuthzBinding.setActiveRoleSet(roleName,
          sentryClient.listUserRoles(authenticator.getUserName()));
    } catch (SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.GRANT_ROLE;
      executeOnFailureHooks(hiveOp, e);
    } catch (Exception e) {
      String msg = "Error when sentryClient setCurrentRole: " + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
      if (hiveAuthzBinding != null) {
        hiveAuthzBinding.close();
      }
    }
  }

  @Override
  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
    List<String> roles = new ArrayList<String>();
    try {
      sentryClient = getSentryClient();
      hiveAuthzBinding = new HiveAuthzBinding(hiveHook, conf, authzConf);
      ActiveRoleSet roleSet = hiveAuthzBinding.getActiveRoleSet();
      if (roleSet.isAll()) {
        roles = convert2RoleList(sentryClient.listUserRoles(authenticator.getUserName()));
      } else {
        roles.addAll(roleSet.getRoles());
      }
    } catch (Exception e) {
      String msg = "Error when sentryClient listUserRoles: " + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
      if (hiveAuthzBinding != null) {
        hiveAuthzBinding.close();
      }
    }
    return roles;
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName)
      throws HiveAuthzPluginException {
    // TODO we will support in future
    throw new HiveAuthzPluginException("Not supported of SHOW_ROLE_PRINCIPALS in Sentry");
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal)
      throws HiveAccessControlException, HiveAuthzPluginException {
    List<HiveRoleGrant> hiveRoleGrants = new ArrayList<HiveRoleGrant>();
    try {
      sentryClient = getSentryClient();
      Set<TSentryRole> roles = null;
      if (principal.getType() == HivePrincipalType.GROUP) {
        roles = sentryClient.listRolesByGroupName(authenticator.getUserName(), principal.getName());
      } else if (principal.getType() == HivePrincipalType.USER) {
        roles = sentryClient.listRolesByUserName(authenticator.getUserName(), principal.getName());
      } else {
        String msg =
            SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principal.getType();
        throw new HiveAuthzPluginException(msg);
      }
      if (roles != null && !roles.isEmpty()) {
        for (TSentryRole role : roles) {
          hiveRoleGrants.add(SentryAuthorizerUtil.convert2HiveRoleGrant(role));
        }
      }
    } catch (SentryAccessDeniedException e) {
      HiveOperation hiveOp = HiveOperation.SHOW_ROLE_GRANT;
      executeOnFailureHooks(hiveOp, e);
    } catch (SentryUserException e) {
      String msg = "Error when sentryClient listRolesByGroupName: " + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
    }
    return hiveRoleGrants;
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
    // Apply rest of the configuration only to HiveServer2
    if (ctx.getClientType() != CLIENT_TYPE.HIVESERVER2
        || !hiveConf.getBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      throw new HiveAuthzPluginException("Sentry only supports hiveserver2");
    }
  }

  /**
   * Grant(isGrant is true) or revoke(isGrant is false) db privileges to/from role via sentryClient,
   * which is a instance of SentryPolicyServiceClientV2
   *
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantOption
   * @param isGrant
   */
  private void grantOrRevokePrivlegeOnRole(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject, boolean grantOption,
      boolean isGrant) throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      sentryClient = getSentryClient();

      for (HivePrincipal principal : hivePrincipals) {
        // Sentry only support grant privilege to ROLE
        if (principal.getType() != HivePrincipalType.ROLE) {
          String msg =
              SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principal.getType();
          throw new HiveAuthzPluginException(msg);
        }
        for (HivePrivilege privilege : hivePrivileges) {
          String grantorName = authenticator.getUserName();
          String roleName = principal.getName();
          String action = SentryAuthorizerUtil.convert2SentryAction(privilege);
          List<String> columnNames = privilege.getColumns();
          Boolean grantOp = null;
          if (isGrant) {
            grantOp = grantOption;
          }

          switch (hivePrivObject.getType()) {
            case GLOBAL:
              if (isGrant) {
                sentryClient.grantServerPrivilege(grantorName, roleName,
                    hivePrivObject.getObjectName(), action, grantOp);
              } else {
                sentryClient.revokeServerPrivilege(grantorName, roleName,
                    hivePrivObject.getObjectName(), action, grantOp);
              }
              break;
            case DATABASE:
              if (isGrant) {
                sentryClient.grantDatabasePrivilege(grantorName, roleName, serverName,
                    hivePrivObject.getDbname(), action, grantOp);
              } else {
                sentryClient.revokeDatabasePrivilege(grantorName, roleName, serverName,
                    hivePrivObject.getDbname(), action, grantOp);
              }
              break;
            case TABLE_OR_VIEW:
              // For column level security
              if (columnNames != null && !columnNames.isEmpty()) {
                if (action.equalsIgnoreCase(AccessConstants.INSERT)
                    || action.equalsIgnoreCase(AccessConstants.ALL)) {
                  String msg =
                      SentryHiveConstants.PRIVILEGE_NOT_SUPPORTED + privilege.getName()
                          + " on Column";
                  throw new HiveAuthzPluginException(msg);
                }
                if (isGrant) {
                  sentryClient.grantColumnsPrivileges(grantorName, roleName, serverName,
                      hivePrivObject.getDbname(), hivePrivObject.getObjectName(), columnNames,
                      action, grantOp);
                } else {
                  sentryClient.revokeColumnsPrivilege(grantorName, roleName, serverName,
                      hivePrivObject.getDbname(), hivePrivObject.getObjectName(), columnNames,
                      action, grantOp);
                }
              } else {
                if (isGrant) {
                  sentryClient.grantTablePrivilege(grantorName, roleName, serverName,
                      hivePrivObject.getDbname(), hivePrivObject.getObjectName(), action, grantOp);
                } else {
                  sentryClient.revokeTablePrivilege(grantorName, roleName, serverName,
                      hivePrivObject.getDbname(), hivePrivObject.getObjectName(), action, grantOp);
                }
              }
              break;
            case LOCAL_URI:
            case DFS_URI:
              String uRIString = hivePrivObject.getObjectName().replace("'", "").replace("\"", "");
              if (isGrant) {
                sentryClient.grantURIPrivilege(grantorName, roleName, serverName,
                    uRIString, grantOp);
              } else {
                sentryClient.revokeURIPrivilege(grantorName, roleName, serverName,
                    uRIString, grantOp);
              }
              break;
            case FUNCTION:
            case PARTITION:
            case COLUMN:
            case COMMAND_PARAMS:
              // not support these type
              throw new HiveAuthzPluginException(hivePrivObject.getType().name()
                  + " are not supported in sentry");
            default:
              break;
          }
        }
      }
    } catch (SentryAccessDeniedException e) {
      HiveOperation hiveOp =
          isGrant ? HiveOperation.GRANT_PRIVILEGE : HiveOperation.REVOKE_PRIVILEGE;
      executeOnFailureHooks(hiveOp, e);
    } catch (SentryUserException e) {
      String msg = "Error when sentryClient grant/revoke privilege:" + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
    }
  }
  /**
   * Grant(isGrant is true) or revoke(isGrant is false) role to/from group via sentryClient, which
   * is a instance of SentryPolicyServiceClientV2
   *
   * @param hivePrincipals
   * @param roles
   * @param grantorPrinc
   * @param isGrant
   */
  private void grantOrRevokeRoleOnGroup(List<HivePrincipal> hivePrincipals, List<String> roles,
      HivePrincipal grantorPrinc, boolean isGrant) throws HiveAuthzPluginException,
      HiveAccessControlException {
    try {
      sentryClient = getSentryClient();
      // get principals
      Set<String> groups = Sets.newHashSet();
      Set<String> users = Sets.newHashSet();
      for (HivePrincipal principal : hivePrincipals) {
        if (principal.getType() == HivePrincipalType.GROUP) {
          groups.add(principal.getName());
        } else if (principal.getType() == HivePrincipalType.USER) {
          users.add(principal.getName());
        } else {
          String msg =
              SentryHiveConstants.GRANT_REVOKE_NOT_SUPPORTED_FOR_PRINCIPAL + principal.getType();
          throw new HiveAuthzPluginException(msg);

        }
      }

      // grant/revoke role to/from principals
      for (String roleName : roles) {
        if (isGrant) {
          if (groups.size() > 0) {
            sentryClient.grantRoleToGroups(grantorPrinc.getName(), roleName, groups);
          }
          if (users.size() > 0) {
            sentryClient.grantRoleToUsers(grantorPrinc.getName(), roleName, users);
          }
        } else {
          if (groups.size() > 0) {
            sentryClient.revokeRoleFromGroups(grantorPrinc.getName(), roleName, groups);
          }
          if (users.size() > 0) {
            sentryClient.revokeRoleFromUsers(grantorPrinc.getName(), roleName, users);
          }
        }
      }

    } catch (SentryAccessDeniedException e) {
      HiveOperation hiveOp = isGrant ? HiveOperation.GRANT_ROLE : HiveOperation.REVOKE_ROLE;
      executeOnFailureHooks(hiveOp, e);
    } catch (SentryUserException e) {
      String msg = "Error when sentryClient grant/revoke role:" + e.getMessage();
      executeOnErrorHooks(msg, e);
    } finally {
      closeClient();
    }
  }

  private void executeOnFailureHooks(HiveOperation hiveOp, SentryAccessDeniedException e)
      throws HiveAccessControlException {

    // With Hive 2.x cmd information is not available from SessionState. More over cmd information
    // is not used in SentryOnFailureHookContextImpl. If this information is really needed an issue
    // should be raised with  Hive community to update HiveAccessController interface to pass
    // HiveSemanticAnalyzerHookContext, which has cmd information. For now, empty string is used for
    // cmd.
    SentryOnFailureHookContext hookCtx =
        new SentryOnFailureHookContextImpl("", null, null, hiveOp, null,
            null, null, null, authenticator.getUserName(), null, new AuthorizationException(e),
            authzConf);
    SentryAuthorizerUtil.executeOnFailureHooks(hookCtx, authzConf);
    throw new HiveAccessControlException(e.getMessage(), e);
  }

  private void executeOnErrorHooks(String msg, Exception e) throws HiveAuthzPluginException {
    LOG.error(msg, e);
    throw new HiveAuthzPluginException(msg, e);
  }

  private List<String> convert2RoleList(Set<TSentryRole> roleSet) {
    List<String> roles = new ArrayList<String>();
    if (roleSet != null && !roleSet.isEmpty()) {
      for (TSentryRole tRole : roleSet) {
        roles.add(tRole.getRoleName());
      }
    }
    return roles;
  }

  private SentryPolicyServiceClient getSentryClient() throws HiveAuthzPluginException {
    try {
      Preconditions.checkNotNull(authzConf, "HiveAuthConf cannot be null");
      return SentryServiceClientFactory.create(authzConf);
    } catch (Exception e) {
      String msg = "Error occurred when creating Sentry client: " + e.getMessage();
      throw new HiveAuthzPluginException(msg, e);
    }
  }
  private void closeClient() {
    if (sentryClient != null) {
      try {
        sentryClient.close();
      } catch (Exception e) {
        LOG.error("Error while closing the connection with sentry server", e);
      }
    }
  }

}
