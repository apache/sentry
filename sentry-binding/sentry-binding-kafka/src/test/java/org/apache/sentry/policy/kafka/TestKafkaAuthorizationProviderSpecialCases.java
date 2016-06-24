/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.policy.kafka;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.kafka.KafkaActionConstant;
import org.apache.sentry.core.model.kafka.KafkaActionFactory.KafkaAction;
import org.apache.sentry.core.model.kafka.Host;
import org.apache.sentry.core.model.kafka.KafkaPrivilegeModel;
import org.apache.sentry.core.model.kafka.Topic;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider;
import org.apache.sentry.core.common.utils.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestKafkaAuthorizationProviderSpecialCases {
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
    Host host1 = new Host("host1");
    Topic topic1 = new Topic("t1");
    Set<? extends Action> actions = Sets.newHashSet(new KafkaAction(KafkaActionConstant.READ));
    policyFile.addGroupsToUser(user1.getName(), true, "group1", "group1")
      .addRolesToGroup("group1",  true, "role1", "role1")
      .addPermissionsToRole("role1", true, "host=host1->topic=t1->action=read",
          "host=host1->topic=t1->action=read");
    policyFile.write(iniFile);
    PolicyEngine policy = KafkaPolicyTestUtil.createPolicyEngineForTest(initResource);
    authzProvider = new LocalGroupResourceAuthorizationProvider(initResource,
        policy, KafkaPrivilegeModel.getInstance());
    List<? extends Authorizable> authorizableHierarchy = ImmutableList.of(host1, topic1);
    Assert.assertTrue(authorizableHierarchy.toString(),
        authzProvider.hasAccess(user1, authorizableHierarchy, actions, ActiveRoleSet.ALL));
  }

}
