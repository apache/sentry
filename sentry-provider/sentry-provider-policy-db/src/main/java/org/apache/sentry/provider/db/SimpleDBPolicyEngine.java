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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.core.AccessURI;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.Database;
import org.apache.sentry.provider.file.PolicyEngine;
import org.apache.sentry.provider.file.SimplePolicyParser;
import org.apache.sentry.provider.file.Roles;
import org.apache.sentry.provider.file.RoleValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;

public class SimpleDBPolicyEngine implements PolicyEngine {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleDBPolicyEngine.class);

  public final static String ACCESS_ALLOW_URI_PER_DB_POLICYFILE = "sentry.allow.uri.db.policyfile";

  private SimplePolicyParser parser;

  public SimpleDBPolicyEngine(String resourcePath, String serverName) throws IOException {
    this(new Configuration(), new Path(resourcePath), serverName);
  }

  @VisibleForTesting
  public SimpleDBPolicyEngine(Configuration conf, Path resourcePath, String serverName) throws IOException {
    List<? extends RoleValidator> validators =
      Lists.newArrayList(new ServersAllIsInvalid(), new DatabaseMustMatch(),
        new DatabaseRequiredInRole(), new ServerNameMustMatch(serverName));
    parser = new SimplePolicyParser(conf, resourcePath, new DBRoles.DBRolesFactory(), validators);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSetMultimap<String, String> getPermissions(List<Authorizable> authorizables, List<String> groups) {
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
      resultBuilder.putAll(group, parser.getRoles(database, group, isURI));
    }
    ImmutableSetMultimap<String, String> result = resultBuilder.build();
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("result = " + result);
    }
    return result;
  }
}
