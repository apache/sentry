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
package org.apache.sentry.policy.sqoop;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.policy.common.PolicyEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestSqoopPolicyNegative {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestSqoopPolicyNegative.class);

  private File baseDir;
  private File globalPolicyFile;

  @Before
  public void setup() {
    baseDir = Files.createTempDir();
    globalPolicyFile = new File(baseDir, "global.ini");
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  private void append(String from, File to) throws IOException {
    Files.append(from + "\n", to, Charsets.UTF_8);
  }

  @Test
  public void testauthorizedSqoopInPolicyFile() throws Exception {
    append("[groups]", globalPolicyFile);
    append("other_group = other_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("other_role = server=server1->connector=c1->action=read, server=server1->link=l1->action=read", globalPolicyFile);
    PolicyEngine policy = new SqoopPolicyFileProviderBackend("server1", globalPolicyFile.getPath());
    //malicious_group has no privilege
    ImmutableSet<String> permissions = policy.getAllPrivileges(Sets.newHashSet("malicious_group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
    //other_group has two privileges
    permissions = policy.getAllPrivileges(Sets.newHashSet("other_group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.size() == 2);
  }

  @Test
  public void testNoServerNameConfig() throws Exception {
    append("[groups]", globalPolicyFile);
    append("other_group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = connector=c1->action=read,link=l1->action=read", globalPolicyFile);
    PolicyEngine policy = new SqoopPolicyFileProviderBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getAllPrivileges(Sets.newHashSet("other_group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  @Test
  public void testServerAllName() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = server=*", globalPolicyFile);
    PolicyEngine policy = new SqoopPolicyFileProviderBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getAllPrivileges(Sets.newHashSet("group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  @Test
  public void testServerIncorrect() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = server=server2", globalPolicyFile);
    PolicyEngine policy = new SqoopPolicyFileProviderBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getAllPrivileges(Sets.newHashSet("group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  @Test
  public void testAll() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = *", globalPolicyFile);
    PolicyEngine policy = new SqoopPolicyFileProviderBackend("server1", globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getAllPrivileges(Sets.newHashSet("group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }
}
