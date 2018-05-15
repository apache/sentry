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
package org.apache.sentry.binding.hive.v2.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.SentryOnFailureHook;
import org.apache.sentry.binding.hive.SentryOnFailureHookContext;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

public class SentryAuthorizerUtil {
  public static final Logger LOG = LoggerFactory.getLogger(SentryAuthorizerUtil.class);
  public static String UNKONWN_GRANTOR = "--";

  /**
   * Convert string to URI
   *
   * @param uri
   * @param isLocal
   * @throws SemanticException
   * @throws URISyntaxException
   */
  public static AccessURI parseURI(String uri, boolean isLocal) throws URISyntaxException {
    HiveConf conf = SessionState.get().getConf();
    String warehouseDir = conf.getVar(ConfVars.METASTOREWAREHOUSE);
    return new AccessURI(PathUtils.parseURI(warehouseDir, uri, isLocal));
  }

  /**
   * Convert HivePrivilegeObject to DBModelAuthorizable list Now hive 0.13 don't support column
   * level
   *
   * @param server
   * @param privilege
   */
  public static List<List<DBModelAuthorizable>> getAuthzHierarchy(Server server,
      HivePrivilegeObject privilege) {
    List<DBModelAuthorizable> baseHierarchy = new ArrayList<DBModelAuthorizable>();
    List<List<DBModelAuthorizable>> objectHierarchy = new ArrayList<List<DBModelAuthorizable>>();
    boolean isLocal = false;
    if (privilege.getType() != null) {
      switch (privilege.getType()) {
        case GLOBAL:
          baseHierarchy.add(new Server(privilege.getObjectName()));
          objectHierarchy.add(baseHierarchy);
          break;
        case DATABASE:
          baseHierarchy.add(server);
          baseHierarchy.add(new Database(privilege.getDbname()));
          objectHierarchy.add(baseHierarchy);
          break;
        case TABLE_OR_VIEW:
          baseHierarchy.add(server);
          baseHierarchy.add(new Database(privilege.getDbname()));
          baseHierarchy.add(new Table(privilege.getObjectName()));
          if (privilege.getColumns() != null) {
            for (String columnName : privilege.getColumns()) {
              List<DBModelAuthorizable> columnHierarchy =
                  new ArrayList<DBModelAuthorizable>(baseHierarchy);
              columnHierarchy.add(new Column(columnName));
              objectHierarchy.add(columnHierarchy);
            }
          } else {
            objectHierarchy.add(baseHierarchy);
          }
          break;
        case LOCAL_URI:
          isLocal = true;
        case DFS_URI:
          if (privilege.getObjectName() == null) {
            break;
          }
          try {
            baseHierarchy.add(server);
            baseHierarchy.add(parseURI(privilege.getObjectName(), isLocal));
            objectHierarchy.add(baseHierarchy);
          } catch (Exception e) {
            throw new AuthorizationException("Failed to get File URI", e);
          }
          break;
        case FUNCTION:
        case PARTITION:
        case COLUMN:
        case COMMAND_PARAMS:
          // not support these type
          break;
        default:
          break;
      }
    }
    return objectHierarchy;
  }

  /**
   * Convert HivePrivilegeObject list to List<List<DBModelAuthorizable>>
   *
   * @param server
   * @param privilges
   */
  public static List<List<DBModelAuthorizable>> convert2SentryPrivilegeList(Server server,
      List<HivePrivilegeObject> privilges) {
    List<List<DBModelAuthorizable>> hierarchyList = new ArrayList<List<DBModelAuthorizable>>();
    if (privilges != null && !privilges.isEmpty()) {
      for (HivePrivilegeObject p : privilges) {
        hierarchyList.addAll(getAuthzHierarchy(server, p));
      }
    }
    return hierarchyList;
  }

  /**
   * Convert HiveOperationType to HiveOperation
   *
   * @param type
   */
  public static HiveOperation convert2HiveOperation(String typeName) {
    try {
      return HiveOperation.valueOf(typeName);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Convert HivePrivilege to Sentry Action
   *
   * @param hivePrivilege
   */
  public static String convert2SentryAction(HivePrivilege hivePrivilege) {
    if (PrivilegeType.ALL.name().equals(hivePrivilege.getName())) {
      return AccessConstants.ALL;
    } else {
      return hivePrivilege.getName();
    }
  }

  /**
   * Convert Sentry Action to HivePrivilege
   *
   * @param hivePrivilege
   */
  public static HivePrivilege convert2HivePrivilege(String action) {
    return new HivePrivilege(action, null);
  }

  /**
   * Convert TSentryRole Set to String List
   *
   * @param roleSet
   */
  public static List<String> convert2RoleList(Set<TSentryRole> roleSet) {
    List<String> roles = new ArrayList<String>();
    if (roleSet != null && !roleSet.isEmpty()) {
      for (TSentryRole tRole : roleSet) {
        roles.add(tRole.getRoleName());
      }
    }
    return roles;
  }

  /**
   * Convert TSentryPrivilege to HivePrivilegeInfo
   *
   * @param tPrivilege
   * @param principal
   */
  public static HivePrivilegeInfo convert2HivePrivilegeInfo(TSentryPrivilege tPrivilege,
      HivePrincipal principal) {
    HivePrivilege hivePrivilege = convert2HivePrivilege(tPrivilege.getAction());
    HivePrivilegeObject hivePrivilegeObject = convert2HivePrivilegeObject(tPrivilege);
    // now sentry don't show grantor of a privilege
    HivePrincipal grantor = new HivePrincipal(UNKONWN_GRANTOR, HivePrincipalType.ROLE);
    boolean grantOption =
        tPrivilege.getGrantOption().equals(TSentryGrantOption.TRUE) ? true : false;
    return new HivePrivilegeInfo(principal, hivePrivilege, hivePrivilegeObject, grantor,
        grantOption, (int) tPrivilege.getCreateTime());
  }

  /**
   * Convert TSentryPrivilege to HivePrivilegeObject
   *
   * @param tSentryPrivilege
   */
  public static HivePrivilegeObject convert2HivePrivilegeObject(TSentryPrivilege tSentryPrivilege) {
    HivePrivilegeObject privilege = null;
    switch (PrivilegeScope.valueOf(tSentryPrivilege.getPrivilegeScope())) {
      case SERVER:
        privilege = new HivePrivilegeObject(HivePrivilegeObjectType.GLOBAL, "*", null);
        break;
      case DATABASE:
        privilege =
            new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, tSentryPrivilege.getDbName(),
                null);
        break;
      case TABLE:
        privilege =
            new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW,
                tSentryPrivilege.getDbName(), tSentryPrivilege.getTableName());
        break;
      case COLUMN:
        privilege =
            new HivePrivilegeObject(HivePrivilegeObjectType.COLUMN, tSentryPrivilege.getDbName(),
                tSentryPrivilege.getTableName(), null, tSentryPrivilege.getColumnName());
        break;
      case URI:
        String uriString = tSentryPrivilege.getURI();
        try {
          uriString = uriString.replace("'", "").replace("\"", "");
          HivePrivilegeObjectType type =
              isLocalUri(uriString) ? HivePrivilegeObjectType.LOCAL_URI
                  : HivePrivilegeObjectType.DFS_URI;
          privilege = new HivePrivilegeObject(type, uriString, null);
        } catch (URISyntaxException e1) {
          throw new RuntimeException(uriString + "is not a URI");
        }
      default:
        LOG.warn("Unknown PrivilegeScope: "
            + PrivilegeScope.valueOf(tSentryPrivilege.getPrivilegeScope()));
        break;
    }
    return privilege;
  }

  public static boolean isLocalUri(String uriString) throws URISyntaxException {
    URI uri = new URI(uriString);
    if (uri.getScheme().equalsIgnoreCase("file")) {
      return true;
    }

    return false;
  }

  /**
   * Convert TSentryRole to HiveRoleGrant
   *
   * @param role
   */
  public static HiveRoleGrant convert2HiveRoleGrant(TSentryRole role) {
    HiveRoleGrant hiveRoleGrant = new HiveRoleGrant();
    hiveRoleGrant.setRoleName(role.getRoleName());
    hiveRoleGrant.setPrincipalName(role.getRoleName());
    hiveRoleGrant.setPrincipalType(PrincipalType.ROLE.name());
    hiveRoleGrant.setGrantOption(false);
    hiveRoleGrant.setGrantor(role.getGrantorPrincipal());
    hiveRoleGrant.setGrantorType(PrincipalType.USER.name());
    return hiveRoleGrant;
  }

  /**
   * Execute on failure hooks for e2e tests
   *
   * @param context
   * @param conf
   * @param hiveOp
   */
  public static void executeOnFailureHooks(SentryOnFailureHookContext hookCtx, Configuration conf) {
    String csHooks =
        conf.get(HiveAuthzConf.AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(), "").trim();

    try {
      for (Hook aofh : SentryAuthorizerUtil.getHooks(csHooks)) {
        ((SentryOnFailureHook) aofh).run(hookCtx);
      }
    } catch (Exception ex) {
      LOG.error("Error executing hook:", ex);
    }
  }

  /**
   * Returns a set of hooks specified in a configuration variable.
   *
   * See getHooks(HiveAuthzConf.AuthzConfVars hookConfVar, Class<T> clazz)
   *
   * @param hookConfVar
   * @return
   * @throws Exception
   */
  public static List<Hook> getHooks(String csHooks) throws Exception {
    return getHooks(csHooks, Hook.class);
  }

  /**
   * Returns the hooks specified in a configuration variable. The hooks are returned in a list in
   * the order they were specified in the configuration variable.
   *
   * @param hookConfVar The configuration variable specifying a comma separated list of the hook
   *        class names.
   * @param clazz The super type of the hooks.
   * @return A list of the hooks cast as the type specified in clazz, in the order they are listed
   *         in the value of hookConfVar
   * @throws Exception
   */
  public static <T extends Hook> List<T> getHooks(String csHooks, Class<T> clazz) throws Exception {

    List<T> hooks = new ArrayList<T>();
    if (csHooks.isEmpty()) {
      return hooks;
    }
    for (String hookClass : Splitter.on(",").omitEmptyStrings().trimResults().split(csHooks)) {
      try {
        @SuppressWarnings("unchecked")
        T hook = (T) Class.forName(hookClass, true, JavaUtils.getClassLoader()).newInstance();
        hooks.add(hook);
      } catch (ClassNotFoundException e) {
        LOG.error(hookClass + " Class not found:" + e.getMessage());
        throw e;
      }
    }

    return hooks;
  }
}
