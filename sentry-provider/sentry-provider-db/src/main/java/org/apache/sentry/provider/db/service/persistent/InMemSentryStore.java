/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.persistent;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.utils.PathUtils;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessor;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilegeMap;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.service.thrift.TStoreAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TStorePrivilege;
import org.apache.sentry.provider.db.service.thrift.TStoreSnapshot;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * In memory implementation of the SentryStore. The privilege model is
 * implemented as a Tree of {@link Authorizable} objects, this serves two
 * purposes :
 * 1) Since all objects have a parent <-> child relationship, using a tree
 *    data-structure makes it easier to traverse and aggregate privilege
 *    information for a request
 * 2) Improve in-memory storage efficiency since names of the parents are
 *    not duplicated for large number of leaf node.
 * Each {@link Authorizable} object, in addition to maintaining a list of
 * children, also maintains a map of all roles and the associated Privileges
 * Granted to the role for the Authorizable object.
 * 
 * The Store also maintains a reverse-mapping of role To {@link Authorizable}
 * to eliminate the need to traverse the tree for requests
 * that are keyed to a role.
 *
 */
public class InMemSentryStore implements SentryStore {

  static enum GrantOption {
    UNSET, TRUE, FALSE
  }

  /**
   * The Privilege is maintained as a bitset which makes it easier to
   * handle addition / removal and aggregation of privileges for a role
   *
   */
  static enum Privilege {
    ALL((short)63),
    CREATE((short)32),
    DROP((short)16),
    INDEX((short)8),
    LOCK((short)4),
    SELECT((short)2),
    INSERT((short)1),
    NONE((short)0);

    private short code;
    private Privilege(short code) {
      this.code = code;
    }

    static boolean includedIn(Privilege priv, short bitset) {
      return (bitset & ((short) priv.code)) == priv.code;
    }

    static short addTo(Privilege priv, short bitset) {
      return (short)(bitset | ((short) priv.code));
    }

    static short removeFrom(Privilege priv, short bitset) {
      return (short)(bitset & ~((short) priv.code));
    }

    static Privilege fromTSentryPrivilege(TSentryPrivilege priv) {
      // TODO: maybe better way of doing this ?
      if (AccessConstants.ACTION_ALL.equalsIgnoreCase(
          Strings.nullToEmpty(priv.getAction()))) {
        return ALL;
      } else if (AccessConstants.ALL.equalsIgnoreCase(
          Strings.nullToEmpty(priv.getAction()))) {
        return ALL;
      } else if (AccessConstants.CREATE.equalsIgnoreCase(
          Strings.nullToEmpty(priv.getAction()))) {
        return CREATE;
      } else if (AccessConstants.DROP.equalsIgnoreCase(
          Strings.nullToEmpty(priv.getAction()))) {
        return DROP;
      } else if (AccessConstants.INDEX.equalsIgnoreCase(
          Strings.nullToEmpty(priv.getAction()))) {
        return INDEX;
      } else if (AccessConstants.LOCK.equalsIgnoreCase(
          Strings.nullToEmpty(priv.getAction()))) {
        return LOCK;
      } else if (AccessConstants.SELECT.equalsIgnoreCase(
          Strings.nullToEmpty(priv.getAction()))) {
        return SELECT;
      } else if (AccessConstants.INSERT.equalsIgnoreCase(
          Strings.nullToEmpty(priv.getAction()))) {
        return INSERT;
      } else {
        return NONE;
      }
    }
  };

  static class AuthPrivilege {
    private GrantOption grantOption;
    private short privilege;
    AuthPrivilege(short privilege) {
      this.grantOption = GrantOption.UNSET;
      this.privilege = privilege;
    }
    public GrantOption getGrantOption() {
      return grantOption;
    }
    public void setGrantOption(GrantOption grantOption) {
      this.grantOption = grantOption;
    }
    public short getPrivilege() {
      return privilege;
    }
    public void setPrivilege(short privilege) {
      this.privilege = privilege;
    }
    public boolean implies(Authorizable auth, TSentryPrivilege priv) {
      if (checkHierarchy(auth, priv)) {
        if (!Privilege.includedIn(Privilege.ALL, privilege)
            && !Privilege.includedIn(Privilege.fromTSentryPrivilege(priv), privilege)) {
          return false;
        }
        return true;
      }
      return false;
    }
  }

  static enum AuthType {
    SERVER, DB, URI, TABLE, COLUMN;
  };

  static Map<AuthType, Set<AuthType>> permissableChildren =
      ImmutableMap.<AuthType, Set<AuthType>>builder()
      .put(AuthType.SERVER, Sets.newHashSet(AuthType.DB, AuthType.URI))
      .put(AuthType.URI, Collections.<AuthType>emptySet())
      .put(AuthType.DB, Sets.newHashSet(AuthType.TABLE))
      .put(AuthType.TABLE, Sets.newHashSet(AuthType.COLUMN))
      .put(AuthType.COLUMN, Collections.<AuthType>emptySet())
      .build();

  static class Authorizable {
    private String name;
    private final AuthType type;
    private Map<String, AuthPrivilege> privileges = new HashMap<String, AuthPrivilege>();
    private volatile Map<String, Authorizable> children = new HashMap<String, Authorizable>();
    private Authorizable parent;

    Authorizable(String name, AuthType type, Authorizable parent) {
      this.name = name;
      this.type = type;
      this.parent = parent;
    }

    Authorizable addChild(String name, AuthType authType) {
      Authorizable child = children.get(name);
      if (child == null) {
        child = new Authorizable(name, authType, this);
        children.put(name, child);
      }
      return child;
    }

    Authorizable getChild(String name) {
      return children.get(name);
    }

    Authorizable getChild(String name, AuthType type) {
      if (type == AuthType.URI) {
        for (Authorizable child : children.values()) {
          if (child.getAuthType() == AuthType.URI) {
            if (PathUtils.impliesURI(child.getName(), name)) {
              return child;
            }
          }
        }
      }
      return children.get(name);
    }

    String getName() {
      return name;
    }

    // Needed for rename
    void setName(String name) {
      this.name = name;
    }

    Authorizable getParent() {
      return parent;
    }

    AuthType getAuthType() {
      return type;
    }

    void addPrivilege(String role, Privilege priv) {
      AuthPrivilege authPriv = privileges.get(role);
      if (authPriv == null) {
        authPriv = new AuthPrivilege(Privilege.NONE.code);
        privileges.put(role, authPriv);
      }
      authPriv.setPrivilege(Privilege.addTo(priv, authPriv.privilege));
    }

    void setPrivilege(String role, short privBits) {
      AuthPrivilege authPriv = privileges.get(role);
      if (authPriv == null) {
        authPriv = new AuthPrivilege(Privilege.NONE.code);
        privileges.put(role, authPriv);
      }
      authPriv.setPrivilege(privBits);
    }

    boolean delPrivilege(String role, Privilege priv) {
      AuthPrivilege authPriv = privileges.get(role);
      if (authPriv != null) {
        short newPriv = Privilege.removeFrom(priv, authPriv.privilege);
        if (newPriv == Privilege.NONE.code) {
          privileges.remove(role);
          return true;
        } else {
          authPriv.setPrivilege(newPriv);
        }
      }
      return false;
    }

    AuthPrivilege getPrivilege(String role) {
      return privileges.get(role);
    }

    Map<String, AuthPrivilege> getAllPrivileges() {
      return privileges;
    }

    Map<String, Authorizable> getChildren() {
      return children;
    }
  }

  
  static class GroupMapper {
    final Configuration conf;

    @VisibleForTesting
    GroupMapper(Configuration conf) {
      this.conf = conf;
    }

    protected Set<String> getGroupsForUser(String user)
        throws SentryUserException {
      return SentryPolicyStoreProcessor.getGroupsFromUserName(conf, user);
    }
    
    // get adminGroups from conf
    protected Set<String> getAdminGroups() {
      return Sets.newHashSet(conf.getStrings(
          ServerConfig.ADMIN_GROUPS, new String[]{}));
    }

    // is Admin group
    protected boolean isInAdminGroup(Set<String> groups)
        throws SentryUserException {
      Set<String> admins = getAdminGroups();
      if (admins != null && !admins.isEmpty()) {
        for (String g : groups) {
          if (admins.contains(g)) {
            return true;
          }
        }
      }
      return false;
    }
  }
  
//  private static String LOG_FILE = "inmem.log";
//  private final OutputStream logStream;
  private final UUID serverId = UUID.randomUUID();
  private final AtomicLong seqId = new AtomicLong(0);
  private final Configuration conf;
  private final GroupMapper groupMapper;
  private String schemaVersion;

  private Map<String, Authorizable> rootAuthrizables =
      new HashMap<String, Authorizable>();
  private Map<String, Set<Authorizable>> roleToAuthorizable =
      new HashMap<String, Set<Authorizable>>();
  private Map<String, Set<String>> roleToGroups =
      new HashMap<String, Set<String>>();
  private Map<String, Set<String>> groupToRoles =
      new HashMap<String, Set<String>>();

  InMemSentryStore(Configuration conf) throws SentryAccessDeniedException{
    this(conf, new GroupMapper(conf));
  }

  @VisibleForTesting
  InMemSentryStore(Configuration conf, GroupMapper groupMapper)
      throws SentryAccessDeniedException{
    this.conf = conf;
    this.groupMapper = groupMapper;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public CommitContext createSentryRole(String roleName)
      throws SentryAlreadyExistsException {
    if (!doesRoleExists(roleName)) {
      roleToGroups.put(roleName, new HashSet<String>());
      roleToAuthorizable.put(roleName, new HashSet<Authorizable>());
      return new CommitContext(serverId, seqId.getAndIncrement());
    }
    throw new SentryAlreadyExistsException(
        "Role [" + roleName + "] already exists !!");
  }

  private boolean doesRoleExists(String roleName) {
    return roleToGroups.containsKey(roleName)
        || roleToAuthorizable.containsKey(roleName);
  }

  private static boolean checkHierarchy(Authorizable auth, TSentryPrivilege tPriv) {
    if (auth == null) return true;
    if ((auth.getAuthType() == AuthType.COLUMN)
        &&(!auth.getName().equals(Strings.nullToEmpty(tPriv.getColumnName())))) {
      return false;
    }
    if ((auth.getAuthType() == AuthType.TABLE)
        &&(!auth.getName().equals(Strings.nullToEmpty(tPriv.getTableName())))) {
      return false;
    }
    if ((auth.getAuthType() == AuthType.DB)
        &&(!auth.getName().equals(Strings.nullToEmpty(tPriv.getDbName())))) {
      return false;
    }
    if ((auth.getAuthType() == AuthType.URI)
        &&(!Strings.isNullOrEmpty(tPriv.getURI()))) {
      if (!PathUtils.impliesURI(auth.getName(), tPriv.getURI())) {
        return false;
      }
    }
    if ((auth.getAuthType() == AuthType.SERVER)
        &&(!auth.getName().equals(Strings.nullToEmpty(tPriv.getServerName())))) {
      return false;
    }
    return checkHierarchy(auth.getParent(), tPriv);
  }

  private LinkedHashMap<AuthType, String> toAuthorizableHierarchy(
      TSentryAuthorizable tAuthHier) {
    // TODO : fix this for generic model
    TSentryPrivilege temp = new TSentryPrivilege("", tAuthHier.getServer(), "");
    temp.setDbName(tAuthHier.getDb());
    temp.setURI(tAuthHier.getUri());
    temp.setTableName(tAuthHier.getTable());
    temp.setColumnName(tAuthHier.getColumn());
    return toAuthorizableHierarchy(temp);
  }

  private LinkedHashMap<AuthType, String> toAuthorizableHierarchy(
      TSentryPrivilege tPriv) {
    // TODO : fix this for generic model
    LinkedHashMap<AuthType, String> map = new LinkedHashMap<AuthType, String>();
    map.put(AuthType.SERVER, tPriv.getServerName().toLowerCase());
    if (Strings.nullToEmpty(tPriv.getDbName()) != "") {
      map.put(AuthType.DB, tPriv.getDbName().toLowerCase());
      if (Strings.nullToEmpty(tPriv.getTableName()) != "") {
        map.put(AuthType.TABLE, tPriv.getTableName().toLowerCase());
        if (Strings.nullToEmpty(tPriv.getColumnName()) != "") {
          map.put(AuthType.COLUMN, tPriv.getColumnName().toLowerCase());
        }
      }
    } else {
      if (Strings.nullToEmpty(tPriv.getURI()) != "") {
        map.put(AuthType.URI, tPriv.getURI());
      }
    }
    return map;
  }

  private void fillPrivMap(HashMap<String, String> pUpdate, Authorizable auth) {
    for (Map.Entry<String, AuthPrivilege> e : auth.getAllPrivileges().entrySet()) {
      String outPriv = "";
      if (Privilege.includedIn(Privilege.SELECT, e.getValue().privilege)) {
        outPriv = Privilege.SELECT.toString();
      }
      if (Privilege.includedIn(Privilege.INSERT, e.getValue().privilege)) {
        outPriv = (outPriv.equals("") ? outPriv : ",");
        outPriv = outPriv + Privilege.INSERT.toString();
      }
      pUpdate.put(e.getKey(), outPriv);
    }
  }

  private void grantOptionCheck(String grantorPrincipal, TSentryPrivilege inPriv)
      throws SentryUserException {
    if (grantorPrincipal == null) {
      throw new SentryInvalidInputException("grantorPrincipal should not be null");
    }
    Set<String> groups = groupMapper.getGroupsForUser(grantorPrincipal);
    if (groups == null || groups.isEmpty()) {
      throw new SentryGrantDeniedException(grantorPrincipal
          + " has no grant!");
    }
    // if grantor is in adminGroup, don't need to do check
    if (!groupMapper.isInAdminGroup(groups)) {
      boolean hasGrant = false;
      Set<String> roles = getRoleNamesForGroups(groups);
      for (String role : roles) {
        Set<Authorizable> authorizables = roleToAuthorizable.get(role);
        for (Authorizable auth : authorizables) {
          AuthPrivilege authPriv = auth.getPrivilege(role);
          if ((authPriv.getGrantOption() == GrantOption.TRUE)
              && authPriv.implies(auth, inPriv)) {
            hasGrant = true;
            break;
          }
        }
      }
      if (!hasGrant) {
        throw new SentryGrantDeniedException(grantorPrincipal
            + " has no grant!");
      }
    }
  }

  private Authorizable getLeaf(TSentryPrivilege inPriv, boolean createNodes)
      throws SentryUserException {
    LinkedHashMap<AuthType,String> authHierarchy =
        toAuthorizableHierarchy(inPriv);
    return getLeafCore(createNodes, false, authHierarchy);
  }

  private Authorizable getLeafCore(boolean createNodes, boolean isParentOk,
      LinkedHashMap<AuthType, String> authHierarchy) throws SentryUserException {
    Authorizable parent = null;
    Authorizable current = null;
    for (Map.Entry<AuthType, String> e : authHierarchy.entrySet()) {
      if (e.getKey() == AuthType.SERVER) {
        current = rootAuthrizables.get(e.getValue());
        if (current == null) {
          if (createNodes) {
            current = new Authorizable(e.getValue(), e.getKey(), null);
            rootAuthrizables.put(e.getValue(), current);
          } else {
            if (isParentOk) {
              return parent;
            }
            throw new SentryUserException(
                "Invalid authHierarchy [" + authHierarchy + "]");
          } 
        }
      } else {
        // parent cannot be null here
        if (parent == null) {
          throw new SentryUserException(
              "Invalid authHierarchy [" + authHierarchy + "]");
        } else {
          current = parent.getChild(e.getValue(), e.getKey());
          if (current == null) {
            if (createNodes) {
              current = parent.addChild(e.getValue(), e.getKey());
            } else {
              if (isParentOk) {
                return parent;
              }
              throw new SentryUserException(
                  "Invalid authHierarchy [" + authHierarchy + "]");
            }
          }
        }
      }
      parent = current;
    }
    return current;
  }

  public Set<String> getGroupsForRole(String roleName) {
    return roleToGroups.get(roleName);
  }

  private Map<String, Set<TSentryPrivilege>> collectPrivileges(Set<String> roleSet,
      TSentryAuthorizable inAuthHier, boolean isAdmin)
      throws SentryUserException {
    // This collects all privileges applicable for the role for
    // 1) All child objects from the leaf of authHierarchy
    // 2) All nodes in parent chain of the leaf of authHierarchy
    Map<String, Set<TSentryPrivilege>> resultMap =
        new HashMap<String, Set<TSentryPrivilege>>();
    if (inAuthHier != null) {
      LinkedHashMap<AuthType,String> authHierarchy =
          toAuthorizableHierarchy(inAuthHier);
      Authorizable current = getLeafCore(false, true, authHierarchy);
      if (current != null) {
        // If returned node is a parent.. we should not
        // recurse down..
        collectPrivileges(current, roleSet, isAdmin, resultMap,
            !isAParent(authHierarchy, current));
        Authorizable parent = current.parent;
        while (parent != null) {
          collectPrivileges(parent, roleSet, isAdmin, resultMap, false);
          parent = parent.getParent();
        }
        
      }
    } else {
      for (Authorizable root : rootAuthrizables.values()) {
        collectPrivileges(root, roleSet, isAdmin, resultMap, true);
      }
    }
    return resultMap;
  }

  private boolean isAParent(LinkedHashMap<AuthType, String> authHierarchy,
      Authorizable current) {
    Iterator<Entry<AuthType, String>> iterator = authHierarchy.entrySet().iterator();
    Entry<AuthType, String> ent = null;
    // get last element
    while (iterator.hasNext()) {
      ent = iterator.next();
    }
    return (ent.getKey() != current.getAuthType()); 
  }

  private void collectPrivileges(Authorizable current, Set<String> roleSet,
      boolean isAdmin, Map<String, Set<TSentryPrivilege>> resultMap, boolean recurse)
      throws SentryUserException {
    addToPrivSet(current, roleSet, isAdmin, resultMap);
    if (recurse) {
      for (Authorizable child : current.getChildren().values()) {
        collectPrivileges(child, roleSet, isAdmin, resultMap, recurse);
      }
    }
  }

  private void addToPrivSet(Authorizable current, Set<String> roleSet,
      boolean isAdmin, Map<String, Set<TSentryPrivilege>> resultMap) {
    for (Map.Entry<String, AuthPrivilege> e : current.getAllPrivileges().entrySet()) {
      String roleName = e.getKey();
      if (isAdmin || (roleSet.contains(roleName))) {
        Set<TSentryPrivilege> pSet = resultMap.get(roleName);
        if (pSet == null) {
          pSet = new HashSet<TSentryPrivilege>();
          resultMap.put(roleName, pSet);
        }
        pSet.addAll(convertToTSentryPrivileges(current, e.getValue()));
      }
    }
  }

  private Set<TSentryPrivilege> convertToTSentryPrivileges(Authorizable auth, AuthPrivilege authPriv) {
    Set<TSentryPrivilege> retPrivs = new HashSet<TSentryPrivilege>();
    for (Privilege privToTest : Privilege.values()) {
      // These Apply only to DB
      if (Sets.newHashSet(
          Privilege.CREATE, Privilege.DROP,
          Privilege.INDEX, Privilege.LOCK).contains(privToTest)
          && (auth.getAuthType() != AuthType.DB)) {
        continue;
      }
      if (privToTest == Privilege.NONE) {
        continue;
      }
      if (Privilege.includedIn(privToTest, authPriv.privilege)) {
        TSentryPrivilege tPriv = new TSentryPrivilege(); 
        tPriv.setAction(
            (privToTest == Privilege.ALL)
            && (auth.getAuthType() != AuthType.URI) ? 
                "*" : privToTest.toString().toLowerCase());
        tPriv.setPrivilegeScope(auth.getAuthType().toString());
        if (auth.getAuthType() == AuthType.SERVER) {
          tPriv.setServerName(auth.getName());
        } else if (auth.getAuthType() == AuthType.DB) {
          tPriv.setServerName(auth.getParent().getName());
          tPriv.setDbName(auth.getName());
        } else if (auth.getAuthType() == AuthType.URI) {
          tPriv.setServerName(auth.getParent().getName());
          tPriv.setURI(auth.getName());
        } else if (auth.getAuthType() == AuthType.TABLE) {
          tPriv.setServerName(auth.getParent().getParent().getName());
          tPriv.setDbName(auth.getParent().getName());
          tPriv.setTableName(auth.getName());
        } else if (auth.getAuthType() == AuthType.COLUMN) {
          tPriv.setServerName(auth.getParent().getParent().getParent().getName());
          tPriv.setDbName(auth.getParent().getParent().getName());
          tPriv.setTableName(auth.getParent().getName());
          tPriv.setColumnName(auth.getName());
        }
        tPriv.setGrantOption(
            TSentryGrantOption.valueOf(authPriv.getGrantOption().toString()));
        // Not storing this for the timebeing
        tPriv.setCreateTime(0);
        if (privToTest == Privilege.ALL) {
          // Remove all other privilege objects if ALL
          retPrivs.clear();
          retPrivs.add(tPriv);
          break;
        }
        retPrivs.add(tPriv);
      }
    }
    return retPrivs;
  }

  private void recursiveRevoke(String roleName, Privilege priv,
      Authorizable authorizable, GrantOption inGrantOption) {
    AuthPrivilege authPriv = authorizable.getPrivilege(roleName);
    if (authPriv != null) {
      boolean doRevoke = true;
      if (authPriv.getGrantOption() != GrantOption.UNSET) {
        doRevoke = authPriv.getGrantOption() == inGrantOption;
        if (inGrantOption == GrantOption.UNSET) {
          doRevoke = true;
        }
      }
      if (doRevoke) {
        boolean delPriv = authorizable.delPrivilege(roleName, priv);
        // Remove from roleToAuth mapping if no privileges
        if (delPriv) {
          Set<Authorizable> authSet = roleToAuthorizable.get(roleName);
          authSet.remove(authorizable);
        }
      }
    }
    for(Authorizable child : authorizable.getChildren().values()) {
      recursiveRevoke(roleName, priv, child, inGrantOption);
    }
  }

  private Set<String> getRolesToQuery(Set<String> groups,
      TSentryActiveRoleSet roleSet) {
    if (roleSet == null) {
      roleSet = new TSentryActiveRoleSet(true, null);
    }
    Set<String> activeRoleNames = StoreUtils.toTrimedLower(roleSet.getRoles());
    Set<String> roleNamesForGroups = StoreUtils.toTrimedLower(getRoleNamesForGroups(groups));
    Set<String> rolesToQuery = roleSet.isAll() ? roleNamesForGroups : Sets.intersection(activeRoleNames, roleNamesForGroups);
    return rolesToQuery;
  }

  @Override
  public CommitContext alterSentryRoleGrantPrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege inPriv) throws SentryUserException {
    if (!doesRoleExists(roleName)) {
      throw new SentryNoSuchObjectException("Role: " + roleName);
    }
    grantOptionCheck(grantorPrincipal, inPriv);
    Authorizable current = getLeaf(inPriv, true);
    current.addPrivilege(roleName, Privilege.fromTSentryPrivilege(inPriv));
    if (inPriv.getGrantOption() != null) {
      current.getPrivilege(roleName).setGrantOption(
          GrantOption.valueOf(inPriv.getGrantOption().toString()));
    }
    Set<Authorizable> authSet = roleToAuthorizable.get(roleName);
    authSet.add(current);
    return new CommitContext(serverId, seqId.getAndIncrement());
  }

  
  @Override
  public CommitContext alterSentryRoleGrantPrivileges(String grantorPrincipal,
      String roleName, Set<TSentryPrivilege> privileges)
      throws SentryUserException {
    CommitContext ctx = null;
    for (TSentryPrivilege inPriv : privileges) {
      ctx = alterSentryRoleGrantPrivilege(grantorPrincipal, roleName, inPriv);
    }
    return ctx;
  }

  @Override
  public CommitContext alterSentryRoleRevokePrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege inPriv) throws SentryUserException {
    if (!doesRoleExists(roleName)) {
      throw new SentryNoSuchObjectException("Role: " + roleName);
    }
    grantOptionCheck(grantorPrincipal, inPriv);
    Authorizable current = getLeaf(inPriv, false);
    // NOTE : look how much simpler this is !!
    recursiveRevoke(roleName, Privilege.fromTSentryPrivilege(inPriv), current,
        GrantOption.valueOf(inPriv.getGrantOption().toString()));
    return new CommitContext(serverId, seqId.getAndIncrement());
  }

  @Override
  public CommitContext alterSentryRoleRevokePrivileges(String grantorPrincipal,
      String roleName, Set<TSentryPrivilege> privileges)
      throws SentryUserException {
    CommitContext ctx = null;
    for (TSentryPrivilege inPriv : privileges) {
      ctx = alterSentryRoleRevokePrivilege(grantorPrincipal, roleName, inPriv);
    }
    return ctx;
  }

  @Override
  public CommitContext dropSentryRole(String roleName)
      throws SentryNoSuchObjectException {
    if (doesRoleExists(roleName)) {
      Set<Authorizable> authSet = roleToAuthorizable.get(roleName);
      for (Authorizable authorizable : authSet) {
        authorizable.delPrivilege(roleName, Privilege.ALL);
      }
      roleToAuthorizable.remove(roleName);
      Set<String> groups = roleToGroups.get(roleName);
      for (String group : groups) {
        Set<String> roleSet = groupToRoles.get(group);
        if (roleSet != null) {
          roleSet.remove(roleName);
        }
      }
      roleToGroups.remove(roleName);
      return new CommitContext(serverId, seqId.getAndIncrement());
    } else {
      throw new SentryNoSuchObjectException("Role [" + roleName + "] does not exist !!");
    }
  }

  @Override
  public CommitContext alterSentryRoleAddGroups(String grantorPrincipal,
      String roleName, Set<TSentryGroup> inGroups)
      throws SentryNoSuchObjectException {
    if (!doesRoleExists(roleName)) {
      throw new SentryNoSuchObjectException(
          "Role [" + roleName + "] does not exist !!");
    }
    Set<String> groups = roleToGroups.get(roleName);
    for (TSentryGroup inGroup : inGroups) {
      groups.add(inGroup.getGroupName());
      Set<String> rSet = groupToRoles.get(inGroup.getGroupName());
      if (rSet == null) {
        rSet = new HashSet<String>();
        groupToRoles.put(inGroup.getGroupName(), rSet);
      }
      rSet.add(roleName);
    }
    return new CommitContext(serverId, seqId.getAndIncrement());
  }

  @Override
  public CommitContext alterSentryRoleDeleteGroups(String roleName,
      Set<TSentryGroup> inGroups) throws SentryNoSuchObjectException {
    if (!doesRoleExists(roleName)) {
      throw new SentryNoSuchObjectException(
          "Role [" + roleName + "] does not exist !!");
    }
    Set<String> groups = roleToGroups.get(roleName);
    for (TSentryGroup inGroup : inGroups) {
      groups.remove(inGroup.getGroupName());
      Set<String> rSet = groupToRoles.get(inGroup.getGroupName());
      if (rSet != null) {
        rSet.remove(roleName);
      }
    }
    return new CommitContext(serverId, seqId.getAndIncrement());
  }

  @Override
  public TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(
      Set<String> groups, TSentryActiveRoleSet activeRoles,
      TSentryAuthorizable tAuthHierarchy, boolean isAdmin)
      throws SentryInvalidInputException {
    Map<String, Set<TSentryPrivilege>> resultPrivilegeMap = Maps.newTreeMap();
    Set<String> roles = Sets.newHashSet();
    if (groups != null && !groups.isEmpty()) {
      roles.addAll(getRolesToQuery(groups, activeRoles));
    }
    if (activeRoles != null && !activeRoles.isAll()) {
      // need to check/convert to lowercase here since this is from user input
      for (String aRole : activeRoles.getRoles()) {
        roles.add(aRole.toLowerCase());
      }
    }
    try {
      resultPrivilegeMap = collectPrivileges(roles, tAuthHierarchy, isAdmin);
    } catch (SentryUserException e) {
      // Do this quietly
//      throw new SentryInvalidInputException(e.getMessage());
    }
 
    return new TSentryPrivilegeMap(resultPrivilegeMap);
  }

  @Override
  public Set<TSentryPrivilege> getAllTSentryPrivilegesByRoleName(
      String roleName) throws SentryNoSuchObjectException {
    Set<TSentryPrivilege> resultSet = new HashSet<TSentryPrivilege>();
    Set<Authorizable> auths = roleToAuthorizable.get(roleName);
    if (auths != null) {
      for (Authorizable auth : auths) {
        resultSet.addAll(
            convertToTSentryPrivileges(auth, auth.getPrivilege(roleName)));
      }
    }
    return resultSet;
  }

  @Override
  public Set<TSentryPrivilege> getTSentryPrivileges(Set<String> roleNames,
      TSentryAuthorizable authHierarchy) throws SentryInvalidInputException {
    Set<TSentryPrivilege> resultSet = new HashSet<TSentryPrivilege>();
    try {
      Map<String, Set<TSentryPrivilege>> allPrivs =
          collectPrivileges(roleNames, authHierarchy, false);
      for (Set<TSentryPrivilege> pivSet : allPrivs.values()) {
        resultSet.addAll(pivSet);
      }
    } catch (SentryUserException e) {
      throw new SentryInvalidInputException(e.getMessage());
    }
    return resultSet;
  }

  @Override
  public Set<TSentryRole> getTSentryRolesByGroupName(Set<String> groupNames,
      boolean checkAllGroups) throws SentryNoSuchObjectException {
    Set<TSentryRole> resultSet = new HashSet<TSentryRole>();
    Set<String> roleSet = new HashSet<String>();
    if ((groupNames == null)||(groupNames.size() == 0)) {
      roleSet.addAll(roleToGroups.keySet());
    } else {
      for (String group : groupNames) {
        if (group == null) {
          roleSet.addAll(roleToGroups.keySet());
          break;
        }
        Set<String> rSet = groupToRoles.get(group);
        if (rSet != null) {
          for (String role : rSet) {
            roleSet.add(role);
          }
        } else {
          throw new SentryNoSuchObjectException("Group [" + group + "] does not exist !!");
        }
      }
    }
    for (String role : roleSet) {
      TSentryRole tRole = new TSentryRole();
      tRole.setRoleName(role);
      tRole.setGrantorPrincipal("--");
      Set<TSentryGroup> tGroups = new HashSet<TSentryGroup>();
      for (String groupName : roleToGroups.get(role)) {
        TSentryGroup tSentryGroup = new TSentryGroup();
        tSentryGroup.setGroupName(groupName);
        tGroups.add(tSentryGroup);
      }
      tRole.setGroups(tGroups);
      resultSet.add(tRole);
    }
    return resultSet;
  }

  @Override
  public Set<String> getRoleNamesForGroups(Set<String> groups) {
    Set<String> roles = new HashSet<String>();
    for (String group : groups) {
      Set<String> rs = groupToRoles.get(group);
      if (rs != null) {
        roles.addAll(rs);
      }
    }
    return roles;
  }

  @Override
  public Set<String> listAllSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet) throws SentryInvalidInputException {
    return listSentryPrivilegesForProvider(groups, roleSet, null);
  }

  @Override
  public Set<String> listSentryPrivilegesForProvider(Set<String> groups,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy)
      throws SentryInvalidInputException {
    Set<String> resultSet = new HashSet<String>();
    TSentryPrivilegeMap privMap =
        listSentryPrivilegesByAuthorizable(groups, roleSet, authHierarchy, false);
    for (Set<TSentryPrivilege> privSet : privMap.getPrivilegeMap().values()) {
      for (TSentryPrivilege tPriv : privSet) {
        String authorizable =
            StoreUtils.toAuthorizable(tPriv.getServerName(), tPriv.getDbName(),
            tPriv.getURI(), tPriv.getTableName(),
            tPriv.getColumnName(), tPriv.getAction());
        resultSet.add(authorizable);
      }
    }
    return resultSet;
  }

  @Override
  public boolean hasAnyServerPrivileges(Set<String> groups,
      TSentryActiveRoleSet roleSet, String server) {
    Set<String> rolesToQuery = getRolesToQuery(groups, roleSet);
    Authorizable serverAuth = rootAuthrizables.get(server);
    if (serverAuth != null) {
      // Do breadth first search and return true immediately
      LinkedList<Authorizable> lst = new LinkedList<Authorizable>();
      lst.add(serverAuth);
      while (!lst.isEmpty()) {
        Authorizable auth = lst.removeFirst();
        for (String roleName : rolesToQuery) {
          if (auth.getPrivilege(roleName) != null) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public String getSentryVersion() throws SentryNoSuchObjectException,
      SentryAccessDeniedException {
    return this.schemaVersion;
  }

  @Override
  public void setSentryVersion(String newVersion, String verComment)
      throws SentryNoSuchObjectException, SentryAccessDeniedException {
    this.schemaVersion = newVersion;
  }

  @Override
  public void dropPrivilege(TSentryAuthorizable tAuthorizable)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    LinkedHashMap<AuthType,String> authHierarchy =
        toAuthorizableHierarchy(tAuthorizable);
    try {
      Authorizable node = null;
      try {
        node = getLeafCore(false, false, authHierarchy);
      } catch (Exception e) {
        node = null;
      }
      if (node != null) {
        if ((node.getChildren().size() == 0)
            &&(node.getAllPrivileges().size() == 0)
            &&(node.getParent() != null)) {
          node.getParent().getChildren().remove(node.getName());
        }
      }
    } catch (Exception e) {
      // Ignore for the timebeing
    }
  }

  @Override
  public void renamePrivilege(TSentryAuthorizable tAuthorizable,
      TSentryAuthorizable newTAuthorizable) throws SentryNoSuchObjectException,
      SentryInvalidInputException {
    LinkedHashMap<AuthType, String> oldHier = toAuthorizableHierarchy(tAuthorizable);
    LinkedHashMap<AuthType, String> newHier = toAuthorizableHierarchy(newTAuthorizable);
    Iterator<String> iter = newHier.values().iterator();
    Authorizable parent = null;
    Authorizable current = null;
    for (Map.Entry<AuthType, String> e : oldHier.entrySet()) {
      String newAuthName = iter.next();
      if (e.getKey() == AuthType.SERVER) {
        current = rootAuthrizables.get(e.getValue());
        if (current == null) {
          throw new SentryNoSuchObjectException(
              "Invalid authHierarchy [" + oldHier + "]");
        }
      } else {
        // parent cannot be null here
        if (parent == null) {
          throw new SentryNoSuchObjectException(
              "Invalid authHierarchy [" + oldHier + "]");
        } else {
          current = parent.getChild(e.getValue());
          if (current == null) {
            throw new SentryNoSuchObjectException(
                "Invalid authHierarchy [" + oldHier + "]");
          }
        }
      }
      if (!newAuthName.equals(e.getValue())) {
        if (parent != null) {
          parent.getChildren().remove(e.getValue());
          current.setName(newAuthName);
        }
      }
      parent = current;
    }
  }

  @Override
  public Map<String, HashMap<String, String>> retrieveFullPrivilegeImage() {
    Map<String, HashMap<String, String>> retVal =
        new HashMap<String, HashMap<String,String>>();
    for (Authorizable rootA : rootAuthrizables.values()) {
      // We need jut two levels here (DB and Table)..
      for (Authorizable db : rootA.getChildren().values()) {
        HashMap<String, String> pUpdate = new HashMap<String, String>();
        retVal.put(db.name, pUpdate);
        fillPrivMap(pUpdate, db);
        for (Authorizable table : db.getChildren().values()) {
          pUpdate = new HashMap<String, String>();
          retVal.put(db.name + "." + table.name, pUpdate);
          fillPrivMap(pUpdate, table);
        }
      }
    }
    return retVal;
  }

  @Override
  public Map<String, LinkedList<String>> retrieveFullRoleImage() {
    Map<String, LinkedList<String>> retVal =
        new HashMap<String, LinkedList<String>>();
    for (Map.Entry<String, Set<String>> e : roleToGroups.entrySet()) {
      retVal.put(e.getKey(), new LinkedList<String>(e.getValue()));
    }
    return retVal;
  }

  @Override
  public long getRoleCount() {
    return (long)roleToAuthorizable.size();
  }

  @Override
  public long getPrivilegeCount() {
    long count = 0;
    for (Set<Authorizable> auths : roleToAuthorizable.values()) {
      count += auths.size();
    }
    return count;
  }

  @Override
  public long getGroupCount() {
    return (long)groupToRoles.size();
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public TStoreSnapshot toSnapshot() {
    AtomicInteger counter = new AtomicInteger(0);
    TStoreSnapshot snapshot = 
        new TStoreSnapshot(
            new HashMap<String, TStoreAuthorizable>(),
            new HashMap<String, Set<String>>(),
            new HashMap<Integer, TStoreAuthorizable>());
    Map<String, TStoreAuthorizable> tRootAuths = snapshot.getRootAuthorizable();
    for (Map.Entry<String, Authorizable> e : rootAuthrizables.entrySet()) {
      TStoreAuthorizable tAuthorizabe =
          new TStoreAuthorizable(e.getKey(), AuthType.SERVER.toString());
      tAuthorizabe.setChildren(new HashSet<Integer>());
      tAuthorizabe.setPrivileges(new HashMap<String, TStorePrivilege>());
      for (Map.Entry<String, AuthPrivilege> priv : e.getValue().getAllPrivileges().entrySet()) {
        tAuthorizabe.getPrivileges().put(priv.getKey(),
            new TStorePrivilege(
                TSentryGrantOption.valueOf(priv.getValue().grantOption.toString()),
                priv.getValue().privilege));
      }
      snapshot.getObjIds().put(counter.getAndIncrement(), tAuthorizabe);
      cloneTStoreAuthorizable(
              e.getValue(), tAuthorizabe, counter, snapshot.getObjIds());
      tRootAuths.put(e.getKey(), tAuthorizabe);
    }
    for (Map.Entry<String, Set<String>> e : roleToGroups.entrySet()) {
      snapshot.getRoleToGroups().put(e.getKey(), new HashSet<String>(e.getValue()));
    }
    return snapshot;
  }

  private void cloneTStoreAuthorizable(Authorizable parent,
      TStoreAuthorizable tParent, AtomicInteger counter,
      Map<Integer, TStoreAuthorizable> objIds) {
    for (Authorizable child : parent.getChildren().values()) {
      TStoreAuthorizable tChild =
          new TStoreAuthorizable(child.getName(),
              child.getAuthType().toString());
      tChild.setChildren(new HashSet<Integer>());
      tChild.setPrivileges(new HashMap<String, TStorePrivilege>());
      for (Map.Entry<String, AuthPrivilege> priv : child.getAllPrivileges().entrySet()) {
        tChild.getPrivileges().put(priv.getKey(),
            new TStorePrivilege(
                TSentryGrantOption.valueOf(priv.getValue().grantOption.toString()),
                priv.getValue().privilege));
      }
      int objId = counter.getAndIncrement();
      objIds.put(objId, tChild);
      tParent.addToChildren(objId);
      cloneTStoreAuthorizable(child, tChild, counter, objIds);
    }
  }

  @Override
  public void fromSnapshot(TStoreSnapshot snapshot) {
    initializeRolesAndGroups(snapshot);
    // Should be called only after initializeRolesAndGroups()
    initializeAuthorizables(snapshot);
  }

  private void initializeAuthorizables(TStoreSnapshot snapshot) {
    roleToAuthorizable.clear();
    for (String role : roleToGroups.keySet()) {
      roleToAuthorizable.put(role, new HashSet<Authorizable>());
    }
    rootAuthrizables.clear();
    for(Map.Entry<String, TStoreAuthorizable> e : snapshot.getRootAuthorizable().entrySet()) {
      Authorizable auth =
          new Authorizable(e.getValue().getName(),
              AuthType.valueOf(e.getValue().getType()), null);
      setPrivileges(e.getValue(), auth);
      createChildren(e.getValue(), auth, snapshot.getObjIds());
    }
  }

  private void setPrivileges(TStoreAuthorizable tAuth, Authorizable auth) {
    for (Map.Entry<String, TStorePrivilege> privEntry : tAuth.getPrivileges().entrySet()) {
      auth.setPrivilege(privEntry.getKey(), privEntry.getValue().getPrivilege());
      if (privEntry.getValue().getGrantOption() != null) {
        auth.getPrivilege(privEntry.getKey()).setGrantOption(
            GrantOption.valueOf(privEntry.getValue().getGrantOption().toString()));
      }
      Set<Authorizable> authSet = roleToAuthorizable.get(privEntry.getKey());
      if (authSet == null) {
        authSet = new HashSet<Authorizable>();
        roleToAuthorizable.put(privEntry.getKey(), authSet);
      }
      authSet.add(auth);
    }
  }

  private void createChildren(TStoreAuthorizable tParent, Authorizable parent,
      Map<Integer, TStoreAuthorizable> objIds) {
    for (Integer id : tParent.getChildren()) {
      TStoreAuthorizable tChild = objIds.get(id);
      Authorizable child =
          parent.addChild(tChild.getName(), AuthType.valueOf(tChild.getType()));
      setPrivileges(tChild, child);
      createChildren(tChild, child, objIds);
    }
  }

  private void initializeRolesAndGroups(TStoreSnapshot snapshot) {
    roleToGroups.clear();
    groupToRoles.clear();
    for (Map.Entry<String, Set<String>> e : snapshot.getRoleToGroups().entrySet()) {
      for (String groupName : e.getValue()) {
        Set<String> roleSet = groupToRoles.get(groupName);
        if (roleSet == null) {
          roleSet = new HashSet<String>();
          groupToRoles.put(groupName, roleSet);
        }
        roleSet.add(e.getKey());
      }
      roleToGroups.put(e.getKey(), new HashSet<String>(e.getValue()));
    }
  }
}
