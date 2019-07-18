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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.kafka.Cluster;
import org.apache.sentry.core.model.kafka.ConsumerGroup;
import org.apache.sentry.core.model.kafka.KafkaActionConstant;
import org.apache.sentry.core.model.kafka.KafkaActionFactory.KafkaAction;
import org.apache.sentry.core.model.kafka.Host;
import org.apache.sentry.core.model.kafka.KafkaPrivilegeModel;
import org.apache.sentry.core.model.kafka.Topic;
import org.apache.sentry.core.model.kafka.TransactionalId;        ;
import org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestKafkaAuthorizationProviderGeneralCases {
  private static final Multimap<String, String> USER_TO_GROUP_MAP = HashMultimap.create();

  private static final Host HOST_1 = new Host("host1");
  private static final Host HOST_2 = new Host("host2");
  private static final Cluster cluster1 = new Cluster();
  private static final Topic topic1 = new Topic("t1");
  private static final Topic topic2 = new Topic("t2");
  private static final ConsumerGroup cgroup1 = new ConsumerGroup("cg1");
  private static final ConsumerGroup cgroup2 = new ConsumerGroup("cg2");
  private static final TransactionalId transactionalId1 = new TransactionalId("ti1");
  private static final TransactionalId transactionalId2 = new TransactionalId("ti2");

  private static final KafkaAction ALL = new KafkaAction(KafkaActionConstant.ALL);
  private static final KafkaAction READ = new KafkaAction(KafkaActionConstant.READ);
  private static final KafkaAction WRITE = new KafkaAction(KafkaActionConstant.WRITE);
  private static final KafkaAction CREATE = new KafkaAction(KafkaActionConstant.CREATE);
  private static final KafkaAction DELETE = new KafkaAction(KafkaActionConstant.DELETE);
  private static final KafkaAction ALTER = new KafkaAction(KafkaActionConstant.ALTER);
  private static final KafkaAction DESCRIBE = new KafkaAction(KafkaActionConstant.DESCRIBE);
  private static final KafkaAction CLUSTER_ACTION = new KafkaAction(KafkaActionConstant.CLUSTER_ACTION);
  private static final KafkaAction ALTER_CONFIGS = new KafkaAction(KafkaActionConstant.ALTER_CONFIGS);
  private static final KafkaAction DESCRIBE_CONFIGS = new KafkaAction(KafkaActionConstant.DESCRIBE_CONFIGS);
  private static final KafkaAction IDEMPOTENT_WRITE = new KafkaAction(KafkaActionConstant.IDEMPOTENT_WRITE);

  private static final Set<KafkaAction> allActions = Sets.newHashSet(ALL, READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
    CLUSTER_ACTION, ALTER_CONFIGS, DESCRIBE_CONFIGS, IDEMPOTENT_WRITE);

  private static final Subject ADMIN = new Subject("admin1");
  private static final Subject SUB_ADMIN = new Subject("subadmin1");
  private static final Subject CONSUMER0 = new Subject("consumer0");
  private static final Subject CONSUMER1 = new Subject("consumer1");
  private static final Subject CONSUMER2 = new Subject("consumer2");
  private static final Subject PRODUCER0 = new Subject("producer0");
  private static final Subject PRODUCER1 = new Subject("producer1");
  private static final Subject PRODUCER2 = new Subject("producer2");
  private static final Subject PRODUCER3 = new Subject("producer3");
  private static final Subject PRODUCER4 = new Subject("producer4");
  private static final Subject PRODUCER5 = new Subject("producer5");
  private static final Subject CONFIG_ADMIN1 = new Subject("config_admin1");
  private static final Subject CONFIG_ADMIN2 = new Subject("config_admin2");
  private static final Subject CONSUMER_PRODUCER0 = new Subject("consumer_producer0");

  private static final String ADMIN_GROUP  = "admin_group";
  private static final String SUBADMIN_GROUP  = "subadmin_group1";
  private static final String CONSUMER_GROUP0 = "consumer_group0";
  private static final String CONSUMER_GROUP1 = "consumer_group1";
  private static final String CONSUMER_GROUP2 = "consumer_group2";
  private static final String PRODUCER_GROUP0 = "producer_group0";
  private static final String PRODUCER_GROUP1 = "producer_group1";
  private static final String PRODUCER_GROUP2 = "producer_group2";
  private static final String PRODUCER_GROUP3 = "producer_group3";
  private static final String PRODUCER_GROUP4 = "producer_group4";
  private static final String PRODUCER_GROUP5 = "producer_group5";
  private static final String CONFIG_ADMIN_GROUP1 = "config_admin_group1";
  private static final String CONFIG_ADMIN_GROUP2 = "config_admin_group2";

  private static final String CONSUMER_PRODUCER_GROUP0 = "consumer_producer_group0";

  static {
    USER_TO_GROUP_MAP.putAll(ADMIN.getName(), Arrays.asList(ADMIN_GROUP));
    USER_TO_GROUP_MAP.putAll(SUB_ADMIN.getName(),  Arrays.asList(SUBADMIN_GROUP ));
    USER_TO_GROUP_MAP.putAll(CONSUMER0.getName(),  Arrays.asList(CONSUMER_GROUP0));
    USER_TO_GROUP_MAP.putAll(CONSUMER1.getName(),  Arrays.asList(CONSUMER_GROUP1));
    USER_TO_GROUP_MAP.putAll(CONSUMER2.getName(),  Arrays.asList(CONSUMER_GROUP2));
    USER_TO_GROUP_MAP.putAll(PRODUCER0.getName(),  Arrays.asList(PRODUCER_GROUP0));
    USER_TO_GROUP_MAP.putAll(PRODUCER1.getName(),  Arrays.asList(PRODUCER_GROUP1));
    USER_TO_GROUP_MAP.putAll(PRODUCER2.getName(),  Arrays.asList(PRODUCER_GROUP2));
    USER_TO_GROUP_MAP.putAll(PRODUCER3.getName(),  Arrays.asList(PRODUCER_GROUP3));
    USER_TO_GROUP_MAP.putAll(PRODUCER4.getName(),  Arrays.asList(PRODUCER_GROUP4));
    USER_TO_GROUP_MAP.putAll(PRODUCER5.getName(),  Arrays.asList(PRODUCER_GROUP5));
    USER_TO_GROUP_MAP.putAll(CONFIG_ADMIN1.getName(),  Arrays.asList(CONFIG_ADMIN_GROUP1));
    USER_TO_GROUP_MAP.putAll(CONFIG_ADMIN2.getName(),  Arrays.asList(CONFIG_ADMIN_GROUP2));
    USER_TO_GROUP_MAP.putAll(CONSUMER_PRODUCER0.getName(),  Arrays.asList(CONSUMER_PRODUCER_GROUP0));
  }

  private final ResourceAuthorizationProvider authzProvider;
  private File baseDir;

  public TestKafkaAuthorizationProviderGeneralCases() throws IOException {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, "kafka-policy-test-authz-provider.ini");
    authzProvider = new HadoopGroupResourceAuthorizationProvider(
        KafkaPolicyTestUtil.createPolicyEngineForTest(new File(baseDir,
            "kafka-policy-test-authz-provider.ini").getPath()),
        new MockGroupMappingServiceProvider(USER_TO_GROUP_MAP), KafkaPrivilegeModel.getInstance());
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
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_1,cluster1), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_1,topic1), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_1,topic2), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_1,cgroup1), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_1,cgroup2), allActions, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_1), allActions, true);

    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_2,cluster1), allActions, false);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_2,topic1), allActions, false);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_2,topic2), allActions, false);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_2,cgroup1), allActions, false);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_2,cgroup2), allActions, false);
    doTestResourceAuthorizationProvider(SUB_ADMIN, Arrays.asList(HOST_2), allActions, false);

    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_1,cluster1), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_1,topic1), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_1,topic2), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_1,cgroup1), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_1,cgroup2), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_1), allActions, true);

    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_2,cluster1), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_2,topic1), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_2,topic2), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_2,cgroup1), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_2,cgroup2), allActions, true);
    doTestResourceAuthorizationProvider(ADMIN, Arrays.asList(HOST_2), allActions, true);
  }

  @Test
  public void testConsumer() throws Exception {
    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(CONSUMER0, Arrays.asList(host, topic1),
            Sets.newHashSet(action), READ.equals(action));
      }
    }

    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(CONSUMER1, Arrays.asList(host, topic1),
            Sets.newHashSet(action), HOST_1.equals(host) && READ.equals(action));
      }
    }

    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(CONSUMER2, Arrays.asList(host, topic2),
            Sets.newHashSet(action), HOST_2.equals(host) && READ.equals(action));
      }
    }
  }

  @Test
  public void testProducer() throws Exception {
    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(PRODUCER0, Arrays.asList(host, topic1),
            Sets.newHashSet(action), WRITE.equals(action));
      }
    }

    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(PRODUCER1, Arrays.asList(host, topic1),
            Sets.newHashSet(action), HOST_1.equals(host) && WRITE.equals(action));
      }
    }

    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(PRODUCER2, Arrays.asList(host, topic2),
            Sets.newHashSet(action), HOST_2.equals(host) && WRITE.equals(action));
      }
    }

    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(PRODUCER3, Arrays.asList(host, transactionalId1),
                Sets.newHashSet(action), HOST_1.equals(host) && WRITE.equals(action));
      }
    }

    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(PRODUCER4, Arrays.asList(host, transactionalId2),
                Sets.newHashSet(action), HOST_2.equals(host) && WRITE.equals(action));
      }
    }

    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(PRODUCER5, Arrays.asList(host, cluster1),
                Sets.newHashSet(action), HOST_1.equals(host) && IDEMPOTENT_WRITE.equals(action));
      }
    }
  }

  @Test
  public void testConfigAdmin() throws Exception {
    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(CONFIG_ADMIN1, Arrays.asList(host, cluster1),
                Sets.newHashSet(action), HOST_1.equals(host) && DESCRIBE_CONFIGS.equals(action));
      }
    }

    for (KafkaAction action : allActions) {
      for (Host host : Sets.newHashSet(HOST_1, HOST_2)) {
        doTestResourceAuthorizationProvider(CONFIG_ADMIN2, Arrays.asList(host, topic1),
                Sets.newHashSet(action), HOST_2.equals(host) && ALTER_CONFIGS.equals(action));
      }
    }
  }

  @Test
  public void testConsumerProducer() throws Exception {
    for (KafkaAction action : allActions) {
      doTestResourceAuthorizationProvider(CONSUMER_PRODUCER0, Arrays.asList(HOST_1, topic1),
          Sets.newHashSet(action), true);
    }
  }

}
