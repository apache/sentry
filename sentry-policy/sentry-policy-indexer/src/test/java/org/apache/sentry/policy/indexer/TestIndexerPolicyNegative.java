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
package org.apache.sentry.policy.indexer;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

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

public class TestIndexerPolicyNegative {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestIndexerPolicyNegative.class);

  private File baseDir;
  private File globalPolicyFile;
  private File otherPolicyFile;

  @Before
  public void setup() {
    baseDir = Files.createTempDir();
    globalPolicyFile = new File(baseDir, "global.ini");
    otherPolicyFile = new File(baseDir, "other.ini");
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
  public void testPerDbFileException() throws Exception {
    append("[databases]", globalPolicyFile);
    append("other_group_db = " + otherPolicyFile.getPath(), globalPolicyFile);
    append("[groups]", otherPolicyFile);
    append("other_group = some_role", otherPolicyFile);
    append("[roles]", otherPolicyFile);
    append("some_role = indexer=i1", otherPolicyFile);
    IndexerPolicyFileBackend policy = new IndexerPolicyFileBackend(globalPolicyFile.getPath());
    Assert.assertEquals(Collections.emptySet(),
        policy.getPrivileges(Sets.newHashSet("other_group"), ActiveRoleSet.ALL));
  }

  @Test
  public void testIndexerRequiredInRole() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = some_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("some_role = action=read", globalPolicyFile);
    PolicyEngine policy = new IndexerPolicyFileBackend(globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPrivileges(Sets.newHashSet("group"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  @Test
  public void testGroupIncorrect() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = indexer=*", globalPolicyFile);
    PolicyEngine policy = new IndexerPolicyFileBackend(globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPrivileges(Sets.newHashSet("incorrectGroup"), ActiveRoleSet.ALL);
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }
}
