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
package org.apache.sentry.binding.hive.authz;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SentryHivePrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationTranslator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.AbstractHiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationTranslator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.sentry.binding.hive.SentryHivePrivilegeObject;
import org.apache.sentry.binding.metastore.SentryMetaStoreFilterHook;

/**
 * This is a HiveAuthorizer implementation, and it is used by HiveServer2 to check privileges
 * of an object, execute GRANT/REVOKE DDL statements and filter HMS metadata. This class is
 * part of the Hive authorization V2.
 * <p>
 * NOTE: For this first version of the class, only the HMS metadata filtering
 * and grant/revoke privileges are implemented.
 * The rest of the authorization is still using Hive authorization V1 API.
 */
public class SentryHiveAuthorizerImpl extends AbstractHiveAuthorizer {
  private SentryHiveAccessController accessController;
  static private HiveAuthorizationTranslator hiveTranslator =
      new SentryHiveAuthorizationTranslator();

  private SentryMetaStoreFilterHook filterHook;

  public SentryHiveAuthorizerImpl(SentryHiveAccessController accessController) {
    this.accessController = accessController;
    this.filterHook = new SentryMetaStoreFilterHook(null);
  }

  @Override
  public VERSION getVersion() {
    return VERSION.V1;
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption)
      throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.grantPrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption)
      throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.revokePrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.createRole(roleName, adminGrantor);
  }

  @Override
  public void dropRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.dropRole(roleName);
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
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc) throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.grantRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc)
      throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.revokeRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputsHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    // Nothing to do there. Privileges are checked on the Semantic hooks
  }

  @Override
  public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs,
      HiveAuthzContext context) throws HiveAuthzPluginException, HiveAccessControlException {
    if (listObjs == null || listObjs.size() == 0) {
      return listObjs;
    }

    switch (listObjs.get(0).getType()) {
      case DATABASE:
        return filterDbs(listObjs);
      case TABLE_OR_VIEW:
        return filterTables(listObjs);
      default:
        return listObjs;
    }
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.getAllRoles();
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
      HivePrivilegeObject privObj) throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.showPrivileges(principal, privObj);
  }

  @Override
  public void setCurrentRole(String roleName)
      throws HiveAccessControlException, HiveAuthzPluginException {
    accessController.setCurrentRole(roleName);
  }

  @Override
  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
    return accessController.getCurrentRoleNames();
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
    accessController.applyAuthorizationConfigPolicy(hiveConf);
  }

  @Override
  public HiveAuthorizationTranslator getHiveAuthorizationTranslator() throws HiveAuthzPluginException {
    return hiveTranslator;
  }

  private List<HivePrivilegeObject> filterDbs(List<HivePrivilegeObject> listObjs) {
    List<String> dbList = new ArrayList<>(listObjs.size());
    for (HivePrivilegeObject o : listObjs) {
      dbList.add(o.getDbname());
    }

    List<String> filterDbList = filterHook.filterDatabases(dbList);
    List<HivePrivilegeObject> filterObjs = new ArrayList<>(filterDbList.size());
    for (String db : filterDbList) {
      filterObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, db, db));
    }

    return filterObjs;
  }

  private List<HivePrivilegeObject> filterTables(List<HivePrivilegeObject> listObjs) {
    if (listObjs == null || listObjs.size() == 0) {
      return listObjs;
    }

    List<String> tableList = new ArrayList<>(listObjs.size());
    for (HivePrivilegeObject o : listObjs) {
      tableList.add(o.getObjectName());
    }

    String db = listObjs.get(0).getDbname();

    List<String> filterTableList =
        filterHook.filterTableNames(db, tableList);

    List<HivePrivilegeObject> filterObjs = new ArrayList<>(filterTableList.size());
    for (String table : filterTableList) {
      filterObjs.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, db, table));
    }

    return filterObjs;
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

  private static class SentryHiveAuthorizationTranslator extends
      DefaultHiveAuthorizationTranslator {

    @Override
    public HivePrivilegeObject getHivePrivilegeObject(PrivilegeObjectDesc privSubjectDesc)
        throws HiveException {
      if (privSubjectDesc != null && privSubjectDesc instanceof SentryHivePrivilegeObjectDesc) {
        SentryHivePrivilegeObjectDesc sPrivSubjectDesc =
            (SentryHivePrivilegeObjectDesc) privSubjectDesc;
        if (sPrivSubjectDesc.isSentryPrivObjectDesc()) {
          HivePrivilegeObjectType objectType = getPrivObjectType(sPrivSubjectDesc);
          return new SentryHivePrivilegeObject(objectType, privSubjectDesc.getObject());
        }
      }
      return super.getHivePrivilegeObject(privSubjectDesc);
    }
  }
}
