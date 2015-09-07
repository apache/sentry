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

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SentryHivePrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.sentry.binding.hive.v2.SentryHivePrivilegeObject;

/**
 * Convenience implementation of HiveAuthorizer. You can customize the behavior by passing different
 * implementations of {@link SentryHiveAccessController} and
 * {@link SentryHiveAuthorizationValidator} to constructor.
 */
public abstract class SentryHiveAuthorizer implements HiveAuthorizer {

  private SentryHiveAccessController accessController;
  private SentryHiveAuthorizationValidator authValidator;

  public SentryHiveAuthorizer(SentryHiveAccessController accessController,
      SentryHiveAuthorizationValidator authValidator) {
    this.accessController = accessController;
    this.authValidator = authValidator;
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException,
      HiveAccessControlException {
    accessController.grantPrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException,
      HiveAccessControlException {
    accessController.revokePrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.createRole(roleName, adminGrantor);
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.dropRole(roleName);
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
      HiveAccessControlException {
    accessController.grantRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
      HiveAccessControlException {
    accessController.revokeRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    authValidator.checkPrivileges(hiveOpType, inputHObjs, outputHObjs, context);
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.getAllRoles();
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.showPrivileges(principal, privObj);
  }

  @Override
  public VERSION getVersion() {
    return VERSION.V1;
  }

  @Override
  public void setCurrentRole(String roleName) throws HiveAccessControlException,
      HiveAuthzPluginException {
    accessController.setCurrentRole(roleName);
  }

  @Override
  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
    return accessController.getCurrentRoleNames();
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.getPrincipalGrantInfoForRole(roleName);
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.getRoleGrantInfoForPrincipal(principal);
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
    accessController.applyAuthorizationConfigPolicy(hiveConf);
  }

  @Override
  public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs,
      HiveAuthzContext context) throws HiveAuthzPluginException, HiveAccessControlException {
    return authValidator.filterListCmdObjects(listObjs, context);
  }

  @Override
  public List<HivePrincipal> getHivePrincipals(List<PrincipalDesc> principals) throws HiveException {
    return AuthorizationUtils.getHivePrincipals(principals);
  }

  @Override
  public List<HivePrivilege> getHivePrivileges(List<PrivilegeDesc> privileges) {
    return AuthorizationUtils.getHivePrivileges(privileges);
  }

  @Override
  public HivePrivilegeObject getHivePrivilegeObject(PrivilegeObjectDesc privSubjectDesc)
      throws HiveException {
    SentryHivePrivilegeObjectDesc sPrivSubjectDesc = null;
    if (privSubjectDesc instanceof SentryHivePrivilegeObjectDesc) {
      sPrivSubjectDesc = (SentryHivePrivilegeObjectDesc) privSubjectDesc;
    }
    if (sPrivSubjectDesc != null && sPrivSubjectDesc.isSentryPrivObjectDesc()) {
      HivePrivilegeObjectType objectType = getPrivObjectType(sPrivSubjectDesc);
      return new SentryHivePrivilegeObject(objectType, privSubjectDesc.getObject());
    } else {
      return AuthorizationUtils.getHivePrivilegeObject(privSubjectDesc);
    }
  }

  protected static HivePrivilegeObjectType getPrivObjectType(
      SentryHivePrivilegeObjectDesc privSubjectDesc) {
    if (privSubjectDesc.getObject() == null) {
      return null;
    }
    if (privSubjectDesc.getServer()) {
      return HivePrivilegeObjectType.GLOBAL;
    } else if (privSubjectDesc.getUri()) {
      return HivePrivilegeObjectType.LOCAL_URI;
    } else {
      return privSubjectDesc.getTable() ? HivePrivilegeObjectType.TABLE_OR_VIEW
          : HivePrivilegeObjectType.DATABASE;
    }
  }

}
