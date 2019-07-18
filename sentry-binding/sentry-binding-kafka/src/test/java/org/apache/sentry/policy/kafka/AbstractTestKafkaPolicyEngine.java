/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.policy.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.policy.common.PolicyEngine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public abstract class AbstractTestKafkaPolicyEngine {

  private static final String ADMIN = "host=*->action=all";
  private static final String ADMIN_HOST1 = "host=host1->action=all";
  private static final String CONSUMER_T1_ALL = "host=*->topic=t1->action=read";
  private static final String CONSUMER_T1_HOST1 = "host=host1->topic=t1->action=read";
  private static final String CONSUMER_T2_HOST2 = "host=host2->topic=t2->action=read";
  private static final String PRODUCER_T1_ALL = "host=*->topic=t1->action=write";
  private static final String PRODUCER_T1_HOST1 = "host=host1->topic=t1->action=write";
  private static final String PRODUCER_T2_HOST2 = "host=host2->topic=t2->action=write";
  private static final String PRODUCER_TI1_HOST1 = "host=host1->transactionalid=ti1->action=write";
  private static final String PRODUCER_TI2_HOST2 = "host=host2->transactionalid=ti2->action=write";
  private static final String PRODUCER_IDEMPOTENTWRITE = "host=host1->cluster=kafka-cluster->action=idempotentwrite";
  private static final String CONFIG_ADMIN_HOST1 = "host=host1->cluster=kafka-cluster->action=describeconfigs";
  private static final String CONFIG_ADMIN_T1_HOST2 = "host=host2->topic=t1->action=alterconfigs";
  private static final String CONSUMER_PRODUCER_T1 = "host=host1->topic=t1->action=all";

  private PolicyEngine policy;
  private static File baseDir;

  @BeforeClass
  public static void setupClazz() throws IOException {
    baseDir = Files.createTempDir();
  }

  @AfterClass
  public static void teardownClazz() throws IOException {
    if (baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  protected void setPolicy(PolicyEngine policy) {
    this.policy = policy;
  }

  protected static File getBaseDir() {
    return baseDir;
  }

  @Before
  public void setup() throws IOException {
    afterSetup();
  }

  @After
  public void teardown() throws IOException {
    beforeTeardown();
  }

  protected void afterSetup() throws IOException {}

  protected void beforeTeardown() throws IOException {}


  @Test
  public void testConsumer0() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(CONSUMER_T1_ALL));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("consumer_group0"), ActiveRoleSet.ALL))
            .toString());
  }

  @Test
  public void testConsumer1() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(CONSUMER_T1_HOST1));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("consumer_group1"), ActiveRoleSet.ALL))
            .toString());
  }

  @Test
  public void testConsumer2() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(CONSUMER_T2_HOST2));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("consumer_group2"), ActiveRoleSet.ALL))
            .toString());
  }

  @Test
  public void testProducer0() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(PRODUCER_T1_ALL));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("producer_group0"), ActiveRoleSet.ALL))
            .toString());
  }

  @Test
  public void testProducer1() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(PRODUCER_T1_HOST1));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("producer_group1"), ActiveRoleSet.ALL))
            .toString());
  }


  @Test
  public void testProducer2() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(PRODUCER_T2_HOST2));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("producer_group2"), ActiveRoleSet.ALL))
            .toString());
  }

  @Test
  public void testProducer3() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(PRODUCER_TI1_HOST1));
    Assert.assertEquals(expected.toString(),
            new TreeSet<String>(policy.getPrivileges(set("producer_group3"), ActiveRoleSet.ALL))
                    .toString());
  }

  @Test
  public void testProducer4() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(PRODUCER_TI2_HOST2));
    Assert.assertEquals(expected.toString(),
            new TreeSet<String>(policy.getPrivileges(set("producer_group4"), ActiveRoleSet.ALL))
                    .toString());
  }

  @Test
  public void testProducer5() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(PRODUCER_IDEMPOTENTWRITE));
    Assert.assertEquals(expected.toString(),
            new TreeSet<String>(policy.getPrivileges(set("producer_group5"), ActiveRoleSet.ALL))
                    .toString());
  }

  @Test
  public void testConfigAdmin1() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(CONFIG_ADMIN_HOST1));
    Assert.assertEquals(expected.toString(),
            new TreeSet<String>(policy.getPrivileges(set("config_admin_group1"), ActiveRoleSet.ALL))
                    .toString());
  }

  @Test
  public void testConfigAdmin2() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(CONFIG_ADMIN_T1_HOST2));
    Assert.assertEquals(expected.toString(),
            new TreeSet<String>(policy.getPrivileges(set("config_admin_group2"), ActiveRoleSet.ALL))
                    .toString());
  }

  @Test
  public void testConsumerProducer0() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(CONSUMER_PRODUCER_T1));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("consumer_producer_group0"), ActiveRoleSet.ALL))
            .toString());
  }

  @Test
  public void testSubAdmin() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(ADMIN_HOST1));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("subadmin_group1"), ActiveRoleSet.ALL))
            .toString());
  }

  @Test
  public void testAdmin() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(ADMIN));
    Assert
        .assertEquals(expected.toString(),
            new TreeSet<String>(policy.getPrivileges(set("admin_group"), ActiveRoleSet.ALL))
                .toString());
  }

  private static Set<String> set(String... values) {
    return Sets.newHashSet(values);
  }
}
