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
package org.apache.sentry.provider.db;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.provider.common.PermissionFactory;
import org.apache.sentry.provider.common.PolicyEngine;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.Roles;
import org.apache.sentry.provider.common.RoleValidator;
import org.apache.sentry.provider.file.SimpleFileProviderBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;

public class SimpleDBPolicyEngine implements PolicyEngine {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleDBPolicyEngine.class);

  public final static String ACCESS_ALLOW_URI_PER_DB_POLICYFILE = "sentry.allow.uri.db.policyfile";

  private ProviderBackend providerBackend;

  public SimpleDBPolicyEngine(String serverName, ProviderBackend providerBackend) {
    List<? extends RoleValidator> validators =
      Lists.newArrayList(new ServersAllIsInvalid(), new DatabaseMustMatch(),
        new DatabaseRequiredInRole(), new ServerNameMustMatch(serverName));
    this.providerBackend = providerBackend;
    this.providerBackend.process(validators);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PermissionFactory getPermissionFactory() {
    return new DBWildcardPermission.DBWildcardPermissionFactory();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSetMultimap<String, String> getPermissions(List<? extends Authorizable> authorizables, List<String> groups) {
    String database = null;
    Boolean isURI = false;
    for(Authorizable authorizable : authorizables) {
      if(authorizable instanceof Database) {
        database = authorizable.getName();
      }
      if (authorizable instanceof AccessURI) {
        isURI = true;
      }
    }

    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Getting permissions for {} via {}", groups, database);
    }
    ImmutableSetMultimap.Builder<String, String> resultBuilder = ImmutableSetMultimap.builder();
    for(String group : groups) {
      resultBuilder.putAll(group, getDBRoles(database, group, isURI, providerBackend.getRoles()));
    }
    ImmutableSetMultimap<String, String> result = resultBuilder.build();
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("result = " + result);
    }
    return result;
  }

  private ImmutableSet<String> getDBRoles(@Nullable String database,
      String group, Boolean isURI, Roles roles) {
    ImmutableSetMultimap<String, String> globalRoles = roles.getGlobalRoles();
    ImmutableMap<String, ImmutableSetMultimap<String, String>> perDatabaseRoles = roles.getPerDatabaseRoles();
    ImmutableSet.Builder<String> resultBuilder = ImmutableSet.builder();
    String allowURIPerDbFile =
        System.getProperty(SimpleDBPolicyEngine.ACCESS_ALLOW_URI_PER_DB_POLICYFILE);
    Boolean consultPerDbRolesForURI = isURI && ("true".equalsIgnoreCase(allowURIPerDbFile));

    // handle Database.ALL
    if (Database.ALL.getName().equals(database)) {
      for(Entry<String, ImmutableSetMultimap<String, String>> dbListEntry : perDatabaseRoles.entrySet()) {
        if (dbListEntry.getValue().containsKey(group)) {
          resultBuilder.addAll(dbListEntry.getValue().get(group));
        }
      }
    } else if(database != null) {
      ImmutableSetMultimap<String, String> dbPolicies =  perDatabaseRoles.get(database);
      if(dbPolicies != null && dbPolicies.containsKey(group)) {
        resultBuilder.addAll(dbPolicies.get(group));
      }
    }

    if (consultPerDbRolesForURI) {
      for(String db : perDatabaseRoles.keySet()) {
        ImmutableSetMultimap<String, String> dbPolicies =  perDatabaseRoles.get(db);
        if(dbPolicies != null && dbPolicies.containsKey(group)) {
          resultBuilder.addAll(dbPolicies.get(group));
        }
      }
    }

    if(globalRoles.containsKey(group)) {
      resultBuilder.addAll(globalRoles.get(group));
    }
    ImmutableSet<String> result = resultBuilder.build();
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Database {}, Group {}, Result {}",
          new Object[]{ database, group, result});
    }
    return result;
  }
}
