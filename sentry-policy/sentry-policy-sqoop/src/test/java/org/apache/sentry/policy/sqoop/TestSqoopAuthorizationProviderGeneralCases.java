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
package org.apache.sentry.policy.sqoop;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.sqoop.Connector;
import org.apache.sentry.core.model.sqoop.Job;
import org.apache.sentry.core.model.sqoop.Link;
import org.apache.sentry.core.model.sqoop.Server;
import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sentry.core.model.sqoop.SqoopActionFactory.SqoopAction;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.file.PolicyFiles;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestSqoopAuthorizationProviderGeneralCases {
  private static final Multimap<String, String> USER_TO_GROUP_MAP = HashMultimap.create();

  private static final Subject SUB_ADMIN = new Subject("admin1");
  private static final Subject SUB_DEVELOPER = new Subject("developer1");
  private static final Subject SUB_ANALYST = new Subject("analyst1");
  private static final Subject SUB_JOB_OPERATOR = new Subject("job_operator1");
  private static final Subject SUB_LINK_OPERATOR = new Subject("link_operator1");
  private static final Subject SUB_CONNECTOR_OPERATOR = new Subject("connector_operator1");



  private static final Server server1 = new Server("server1");
  private static final Connector jdbc_connector = new Connector("generic-jdbc-connector");
  private static final Connector hdfs_connector = new Connector("hdfs-connector");
  private static final Connector kafka_connector = new Connector("kafka-connector");
  private static final Connector kite_connector = new Connector("kite-connector");
  private static final Link link1 = new Link("link1");
  private static final Link link2 = new Link("link2");
  private static final Job job1 = new Job("job1");
  private static final Job job2 = new Job("job2");

  private static final SqoopAction ALL = new SqoopAction(SqoopActionConstant.ALL);
  private static final SqoopAction READ = new SqoopAction(SqoopActionConstant.READ);
  private static final SqoopAction WRITE = new SqoopAction(SqoopActionConstant.WRITE);

  private static final String ADMIN = "admin";
  private static final String DEVELOPER = "developer";
  private static final String ANALYST = "analyst";
  private static final String JOB_OPERATOR = "job1_2_operator";
  private static final String LINK_OPERATOR ="link1_2_operator";
  private static final String CONNECTOR_OPERATOR = "connectors_operator";

  static {
    USER_TO_GROUP_MAP.putAll(SUB_ADMIN.getName(), Arrays.asList(ADMIN));
    USER_TO_GROUP_MAP.putAll(SUB_DEVELOPER.getName(), Arrays.asList(DEVELOPER));
    USER_TO_GROUP_MAP.putAll(SUB_ANALYST.getName(), Arrays.asList(ANALYST));
    USER_TO_GROUP_MAP.putAll(SUB_JOB_OPERATOR.getName(),Arrays.asList(JOB_OPERATOR));
    USER_TO_GROUP_MAP.putAll(SUB_LINK_OPERATOR.getName(),Arrays.asList(LINK_OPERATOR));
    USER_TO_GROUP_MAP.putAll(SUB_CONNECTOR_OPERATOR.getName(),Arrays.asList(CONNECTOR_OPERATOR));
  }

  private final ResourceAuthorizationProvider authzProvider;
  private File baseDir;

  public TestSqoopAuthorizationProviderGeneralCases() throws IOException {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, "test-authz-provider.ini");
    authzProvider = new HadoopGroupResourceAuthorizationProvider(
        new SqoopPolicyFileProviderBackend(server1.getName(), new File(baseDir, "test-authz-provider.ini").getPath()),
        new MockGroupMappingServiceProvider(USER_TO_GROUP_MAP));
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  private void doTestResourceAuthorizationProvider(Subject subject, List<? extends Authorizable> authorizableHierarchy,
      Set<? extends Action> actions, boolean expected) throws Exception {
    Objects.ToStringHelper helper = Objects.toStringHelper("TestParameters");
    helper.add("Subject", subject).add("authzHierarchy", authorizableHierarchy).add("action", actions);
    Assert.assertEquals(helper.toString(), expected,
        authzProvider.hasAccess(subject, authorizableHierarchy, actions, ActiveRoleSet.ALL));
  }

  @Test
  public void testAdmin() throws Exception {
    Set<? extends Action> allActions = Sets.newHashSet(ALL, READ, WRITE);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1,hdfs_connector), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1,jdbc_connector), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1,kafka_connector), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1,kite_connector), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1,link1), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1,link2), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1,job1), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(server1,job2), allActions, true);
  }

  @Test
  public void testDeveloper() throws Exception {
    Set<SqoopAction> allActions = Sets.newHashSet(ALL, READ, WRITE);
    for (SqoopAction action : allActions) {
      //developer only has the read action on all connectors
      for (Connector connector : Sets.newHashSet(jdbc_connector, hdfs_connector, kafka_connector, kite_connector))
      doTestResourceAuthorizationProvider(SUB_DEVELOPER, Arrays.asList(server1, connector), Sets.newHashSet(action), READ.equals(action));
    }

    for (Link link : Sets.newHashSet(link1, link2)) {
      //developer has the all action on all links
      doTestResourceAuthorizationProvider(SUB_DEVELOPER, Arrays.asList(server1, link), allActions, true);
    }

    for (Job job : Sets.newHashSet(job1,job2)) {
      //developer has the all action on all jobs
      doTestResourceAuthorizationProvider(SUB_DEVELOPER, Arrays.asList(server1, job), allActions, true);
    }
  }

  @Test
  public void testAnalyst() throws Exception {
    Set<SqoopAction> allActions = Sets.newHashSet(ALL, READ, WRITE);
    for (SqoopAction action : allActions) {
      //analyst has not the any action on all connectors
      for (Connector connector : Sets.newHashSet(jdbc_connector, hdfs_connector, kafka_connector, kite_connector))
      doTestResourceAuthorizationProvider(SUB_ANALYST, Arrays.asList(server1, connector), Sets.newHashSet(action), false);
    }

    for (Link link : Sets.newHashSet(link1, link2)) {
      //analyst has the all action on all links
      doTestResourceAuthorizationProvider(SUB_ANALYST, Arrays.asList(server1, link), allActions, true);
    }

    for (Job job : Sets.newHashSet(job1,job2)) {
      //analyst has the all action on all jobs
      doTestResourceAuthorizationProvider(SUB_ANALYST, Arrays.asList(server1, job), allActions, true);
    }
  }

  @Test
  public void testJobOperator() throws Exception {
    Set<SqoopAction> allActions = Sets.newHashSet(ALL, READ, WRITE);
    for (SqoopAction action : allActions) {
      for (Job job : Sets.newHashSet(job1,job2)) {
        //Job operator has the read action on all jobs
        doTestResourceAuthorizationProvider(SUB_JOB_OPERATOR, Arrays.asList(server1, job), Sets.newHashSet(action), READ.equals(action));
      }
      for (Link link : Sets.newHashSet(link1, link2)) {
        doTestResourceAuthorizationProvider(SUB_JOB_OPERATOR, Arrays.asList(server1, link), Sets.newHashSet(action), false);
      }
      for (Connector connector : Sets.newHashSet(jdbc_connector, hdfs_connector, kafka_connector, kite_connector)) {
        doTestResourceAuthorizationProvider(SUB_JOB_OPERATOR, Arrays.asList(server1, connector), Sets.newHashSet(action), false);
      }
    }
  }

  @Test
  public void testLinkOperator() throws Exception {
    Set<SqoopAction> allActions = Sets.newHashSet(ALL, READ, WRITE);
    for (SqoopAction action : allActions) {
      for (Link link : Sets.newHashSet(link1, link2)) {
        //Link operator has the read action on all links
        doTestResourceAuthorizationProvider(SUB_LINK_OPERATOR, Arrays.asList(server1, link), Sets.newHashSet(action), READ.equals(action));
      }
      for (Job job : Sets.newHashSet(job1,job2)) {
        doTestResourceAuthorizationProvider(SUB_LINK_OPERATOR, Arrays.asList(server1, job), Sets.newHashSet(action), false);
      }
      for (Connector connector : Sets.newHashSet(jdbc_connector, hdfs_connector, kafka_connector, kite_connector)) {
        doTestResourceAuthorizationProvider(SUB_LINK_OPERATOR, Arrays.asList(server1, connector), Sets.newHashSet(action), false);
      }
    }
  }

  @Test
  public void testConnectorOperator() throws Exception {
    Set<SqoopAction> allActions = Sets.newHashSet(ALL, READ, WRITE);
    for (SqoopAction action : allActions) {
      for (Connector connector : Sets.newHashSet(jdbc_connector, hdfs_connector, kafka_connector, kite_connector)) {
        doTestResourceAuthorizationProvider(SUB_CONNECTOR_OPERATOR, Arrays.asList(server1, connector), Sets.newHashSet(action), READ.equals(action));
      }
      for (Job job : Sets.newHashSet(job1,job2)) {
        doTestResourceAuthorizationProvider(SUB_CONNECTOR_OPERATOR, Arrays.asList(server1, job), Sets.newHashSet(action), false);
      }
      for (Link link : Sets.newHashSet(link1, link2)) {
        doTestResourceAuthorizationProvider(SUB_CONNECTOR_OPERATOR, Arrays.asList(server1, link), Sets.newHashSet(action), false);
      }
    }
  }
}
