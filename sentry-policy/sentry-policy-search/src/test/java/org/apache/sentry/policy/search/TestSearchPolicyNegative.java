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
package org.apache.sentry.policy.search;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.shiro.config.ConfigurationException;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.file.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestSearchPolicyNegative {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestSearchPolicyNegative.class);

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
    append("other_group = malicious_role", otherPolicyFile);
    append("[roles]", otherPolicyFile);
    append("malicious_role = collection=*", otherPolicyFile);
    try {
      PolicyEngine policy = new SearchPolicyFileBackend(globalPolicyFile.getPath());
      Assert.fail("Excepted ConfigurationException");
    } catch (ConfigurationException ce) {}
  }

  @Test
  public void testCollectionRequiredInRole() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = action=query", globalPolicyFile);
    PolicyEngine policy = new SearchPolicyFileBackend(globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPermissions(
        Arrays.asList(new Authorizable[] {
            new Collection("collection1"),
    }), Lists.newArrayList("group")).get("group");
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }

  @Test
  public void testGroupIncorrect() throws Exception {
    append("[groups]", globalPolicyFile);
    append("group = malicious_role", globalPolicyFile);
    append("[roles]", globalPolicyFile);
    append("malicious_role = collection=*", globalPolicyFile);
    PolicyEngine policy = new SearchPolicyFileBackend(globalPolicyFile.getPath());
    ImmutableSet<String> permissions = policy.getPermissions(
        Arrays.asList(new Authorizable[] {
            Collection.ALL
    }), Lists.newArrayList("incorrectGroup")).get("incorrectGroup");
    Assert.assertTrue(permissions.toString(), permissions.isEmpty());
  }
}
