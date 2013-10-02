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

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.AccessURI;
import org.apache.sentry.core.Action;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.AuthorizationProvider;
import org.apache.sentry.core.Server;
import org.apache.sentry.core.Subject;
import org.apache.sentry.provider.db.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

public class TestResourceAuthorizationProviderSpecialCases {
  private AuthorizationProvider authzProvider;
  private PolicyFile policyFile;
  private File baseDir;
  private File iniFile;
  private String initResource;
  @Before
  public void setup() throws IOException {
    baseDir = Files.createTempDir();
    iniFile = new File(baseDir, "policy.ini");
    initResource = "file://" + iniFile.getPath();
    policyFile = new PolicyFile();
  }

  @After
  public void teardown() throws IOException {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  @Test
  public void testDuplicateEntries() throws Exception {
    Subject user1 = new Subject("user1");
    Server server1 = new Server("server1");
    AccessURI uri = new AccessURI("file:///path/to/");
    EnumSet<Action> actions = EnumSet.of(Action.ALL, Action.SELECT, Action.INSERT);
    policyFile.addGroupsToUser(user1.getName(), true, "group1", "group1")
      .addRolesToGroup("group1",  true, "role1", "role1")
      .addPermissionsToRole("role1", true, "server=" + server1.getName() + "->uri=" + uri.getName(),
          "server=" + server1.getName() + "->uri=" + uri.getName());
    policyFile.write(iniFile);
    authzProvider = new LocalGroupResourceAuthorizationProvider(initResource, server1.getName());
    List<Authorizable> authorizableHierarchy = ImmutableList.of(server1, uri);
    Assert.assertTrue(authorizableHierarchy.toString(),
        authzProvider.hasAccess(user1, authorizableHierarchy, actions));
  }
  @Test
  public void testNonAbolutePath() throws Exception {
    Subject user1 = new Subject("user1");
    Server server1 = new Server("server1");
    AccessURI uri = new AccessURI("file:///path/to/");
    EnumSet<Action> actions = EnumSet.of(Action.ALL, Action.SELECT, Action.INSERT);
    policyFile.addGroupsToUser(user1.getName(), "group1")
      .addRolesToGroup("group1", "role1")
      .addPermissionsToRole("role1", "server=" + server1.getName() + "->uri=" + uri.getName());
    policyFile.write(iniFile);
    authzProvider = new LocalGroupResourceAuthorizationProvider(initResource, server1.getName());
    // positive test
    List<Authorizable> authorizableHierarchy = ImmutableList.of(server1, uri);
    Assert.assertTrue(authorizableHierarchy.toString(),
        authzProvider.hasAccess(user1, authorizableHierarchy, actions));
    // negative tests
    // TODO we should support the case of /path/to/./ but let's to that later
    uri = new AccessURI("file:///path/to/./");
    authorizableHierarchy = ImmutableList.of(server1, uri);
    Assert.assertFalse(authorizableHierarchy.toString(),
        authzProvider.hasAccess(user1, authorizableHierarchy, actions));
    uri = new AccessURI("file:///path/to/../");
    authorizableHierarchy = ImmutableList.of(server1, uri);
    Assert.assertFalse(authorizableHierarchy.toString(),
        authzProvider.hasAccess(user1, authorizableHierarchy, actions));
    uri = new AccessURI("file:///path/to/../../");
    authorizableHierarchy = ImmutableList.of(server1, uri);
    Assert.assertFalse(authorizableHierarchy.toString(),
        authzProvider.hasAccess(user1, authorizableHierarchy, actions));
    uri = new AccessURI("file:///path/to/dir/../../");
    authorizableHierarchy = ImmutableList.of(server1, uri);
    Assert.assertFalse(authorizableHierarchy.toString(),
        authzProvider.hasAccess(user1, authorizableHierarchy, actions));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidPath() throws Exception {
    new AccessURI(":invaliduri");
  }
}
