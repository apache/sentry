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
package org.apache.sentry.provider.db.generic.service.thrift;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.provider.common.AuthorizationComponent;

import com.google.common.collect.Lists;

import static org.apache.sentry.core.model.search.SearchModelAuthorizable.AuthorizableType.Collection;
import static org.apache.sentry.core.model.search.SearchConstants.SENTRY_SEARCH_CLUSTER_KEY;
import static org.apache.sentry.core.model.search.SearchConstants.SENTRY_SEARCH_CLUSTER_DEFAULT;

/**
 * This search policy client will be used in the solr component to communicate with Sentry service.
 *
 */
public class SearchPolicyServiceClient {
  private static final String COMPONENT_TYPE = AuthorizationComponent.Search;

  private String searchClusterName;
  private SentryGenericServiceClient client;

  public SearchPolicyServiceClient(Configuration conf) throws Exception {
    this.searchClusterName = conf.get(SENTRY_SEARCH_CLUSTER_KEY, SENTRY_SEARCH_CLUSTER_DEFAULT);
    this.client = new SentryGenericServiceClient(conf);
  }

  public void createRole(final String requestor, final String roleName)
      throws SentryUserException {
    client.createRole(requestor, roleName, COMPONENT_TYPE);
  }

  public void createRoleIfNotExist(final String requestor,
      final String roleName) throws SentryUserException {
    client.createRoleIfNotExist(requestor, roleName, COMPONENT_TYPE);
  }

  public void dropRole(final String requestor, final String roleName)
      throws SentryUserException {
    client.dropRole(requestor, roleName, COMPONENT_TYPE);
  }

  public void dropRoleIfExists(final String requestor, final String roleName)
      throws SentryUserException {
    client.dropRoleIfExists(requestor, roleName, COMPONENT_TYPE);
  }

  public void addRoleToGroups(final String requestor, final String roleName,
      final Set<String> groups) throws SentryUserException {
    client.addRoleToGroups(requestor, roleName, COMPONENT_TYPE, groups);
  }

  public void deleteRoleFromGroups(final String requestor, final String roleName,
      final Set<String> groups) throws SentryUserException {
    client.deleteRoleToGroups(requestor, roleName, COMPONENT_TYPE, groups);
  }

  public void grantCollectionPrivilege(final String collection, final String requestor,
      final String roleName,final String action) throws SentryUserException {
    grantCollectionPrivilege(collection, requestor, roleName, action, false);
  }

  public void grantCollectionPrivilege(final String collection, final String requestor,
      final String roleName, final String action, final Boolean grantOption) throws SentryUserException {
    TSentryPrivilege tPrivilege = toTSentryPrivilege(collection, action, grantOption);
    client.grantPrivilege(requestor, roleName, COMPONENT_TYPE, tPrivilege);
  }

  public void revokeCollectionPrivilege(final String collection, final String requestor, final String roleName,
      final String action) throws SentryUserException {
    revokeCollectionPrivilege(collection, requestor, roleName, action, false);
  }

  public void revokeCollectionPrivilege(final String collection, final String requestor, final String roleName,
      final String action, final Boolean grantOption) throws SentryUserException {
    TSentryPrivilege tPrivilege = toTSentryPrivilege(collection, action, grantOption);
    client.revokePrivilege(requestor, roleName, COMPONENT_TYPE, tPrivilege);
  }

  public void renameCollectionPrivilege(final String oldCollection, final String newCollection, final String requestor)
      throws SentryUserException {
    client.renamePrivilege(requestor, COMPONENT_TYPE, searchClusterName, Lists.newArrayList(new Collection(oldCollection)),
        Lists.newArrayList(new Collection(newCollection)));
  }

  public void dropCollectionPrivilege(final String collection, final String requestor) throws SentryUserException {
    final TSentryPrivilege tPrivilege = toTSentryPrivilege(collection, Action.ALL, null);
    client.dropPrivilege(requestor, COMPONENT_TYPE, tPrivilege);
  }

  public Set<TSentryRole> listAllRoles(final String user) throws SentryUserException {
    return client.listAllRoles(user, COMPONENT_TYPE);
  }

  public Set<TSentryRole> listRolesByGroupName(final String requestor, final String groupName) throws SentryUserException {
    return client.listRolesByGroupName(requestor, groupName, COMPONENT_TYPE);
  }

  public Set<TSentryPrivilege> listPrivilegesByRoleName(
      final String requestor, final String roleName,
      final List<? extends Authorizable> authorizables) throws SentryUserException {
    return client.listPrivilegesByRoleName(requestor, roleName, COMPONENT_TYPE, searchClusterName, authorizables);
  }

  public Set<String> listPrivilegesForProvider(final ActiveRoleSet roleSet, final Set<String> groups,
      final List<? extends Authorizable> authorizables) throws SentryUserException {
    return client.listPrivilegesForProvider(COMPONENT_TYPE, searchClusterName, roleSet, groups, authorizables);
  }

  private TSentryPrivilege toTSentryPrivilege(String collection, String action,
      Boolean grantOption) {
    TSentryPrivilege tPrivilege = new TSentryPrivilege();
    tPrivilege.setComponent(COMPONENT_TYPE);
    tPrivilege.setServiceName(searchClusterName);
    tPrivilege.setAction(action);

    if (grantOption == null) {
      tPrivilege.setGrantOption(TSentryGrantOption.UNSET);
    } else if (grantOption) {
      tPrivilege.setGrantOption(TSentryGrantOption.TRUE);
    } else {
      tPrivilege.setGrantOption(TSentryGrantOption.FALSE);
    }

    List<TAuthorizable> authorizables = Lists.newArrayList(new TAuthorizable(Collection.name(), collection));
    tPrivilege.setAuthorizables(authorizables);
    return tPrivilege;
  }

  public void close() {
    if (client != null) {
      client.close();
    }
  }
}
