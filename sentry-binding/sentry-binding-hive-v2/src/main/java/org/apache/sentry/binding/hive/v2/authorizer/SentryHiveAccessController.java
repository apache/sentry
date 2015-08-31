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
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;

/**
 * Abstract class to do access control commands, e.g. grant/revoke privileges, grant/revoke role,
 * create/drop role.
 */
public abstract class SentryHiveAccessController implements HiveAccessController {

  /**
   * Hive statement: Grant privilege GRANT priv_type [, priv_type ] ... ON table_or_view_name TO
   * principal_specification [, principal_specification] ... [WITH GRANT OPTION];
   * 
   * principal_specification : USER user | ROLE role
   * 
   * priv_type : INSERT | SELECT | UPDATE | DELETE | ALL
   * 
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException,
      HiveAccessControlException;

  /**
   * Hive statement: Revoke privilege REVOKE priv_type [, priv_type ] ... ON table_or_view_name FROM
   * principal_specification [, principal_specification] ... ;
   * 
   * principal_specification : USER user | ROLE role
   * 
   * priv_type : INSERT | SELECT | UPDATE | DELETE | ALL
   * 
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException,
      HiveAccessControlException;

  /**
   * Hive statement: Create role CREATE ROLE role_name;
   * 
   * @param roleName
   * @param adminGrantor
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Hive statement: Drop role DROP ROLE role_name;
   * 
   * @param roleName
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract void dropRole(String roleName) throws HiveAuthzPluginException,
      HiveAccessControlException;

  /**
   * Hive statement: Grant role GRANT role_name [, role_name] ... TO principal_specification [,
   * principal_specification] ... [ WITH ADMIN OPTION ];
   * 
   * principal_specification : USER user | ROLE role
   * 
   * @param hivePrincipals
   * @param roles
   * @param grantOption
   * @param grantorPrinc
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
      HiveAccessControlException;


  /**
   * Hive statement: Revoke role REVOKE [ADMIN OPTION FOR] role_name [, role_name] ... FROM
   * principal_specification [, principal_specification] ... ;
   * 
   * principal_specification : USER user | ROLE role
   * 
   * @param hivePrincipals
   * @param roles
   * @param grantOption
   * @param grantorPrinc
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
      HiveAccessControlException;

  /**
   * Hive statement: Show roles SHOW ROLES;
   * 
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract List<String> getAllRoles() throws HiveAuthzPluginException,
      HiveAccessControlException;

  /**
   * Hive statement: Show grant SHOW GRANT [principal_name] ON (ALL| ([TABLE] table_or_view_name);
   * 
   * @param principal
   * @param privObj
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
      HivePrivilegeObject privObj) throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Hive statement: Set role SET ROLE (role_name|ALL);
   * 
   * @param roleName
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract void setCurrentRole(String roleName) throws HiveAuthzPluginException,
      HiveAccessControlException;

  /**
   * Hive statement: Show current roles SHOW CURRENT ROLES;
   * 
   * @throws HiveAuthzPluginException
   */
  @Override
  public abstract List<String> getCurrentRoleNames() throws HiveAuthzPluginException;

  /**
   * Hive statement: Set role privileges SHOW PRINCIPALS role_name;
   * 
   * @param roleName
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Hive statement: Set role grant SHOW ROLE GRANT (USER|ROLE) principal_name;
   * 
   * @param principal
   * @throws HiveAuthzPluginException, HiveAccessControlException
   */
  @Override
  public abstract List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Apply configuration files for authorization V2
   * 
   * @param hiveConf
   * @throws HiveAuthzPluginException
   */
  @Override
  public abstract void applyAuthorizationConfigPolicy(HiveConf hiveConf)
      throws HiveAuthzPluginException;

}
