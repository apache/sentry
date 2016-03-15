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

public abstract class AbstractTestSqoopPolicyEngine {
  private static final String OPERATOR_JDBC_CONNECTORS_READ = "server=server1->connector=generic-jdbc-connector->action=read";
  private static final String OPERATOR_HDFS_CONNECTORS_READ = "server=server1->connector=hdfs-connector->action=read";
  private static final String OPERATOR_KAFKA_CONNECTORS_READ = "server=server1->connector=kafka-connector->action=read";
  private static final String OPERATOR_KITE_CONNECTORS_READ = "server=server1->connector=kite-connector->action=read";
  private static final String ANALYST_JOBS_ALL = "server=server1->job=all->action=*";
  private static final String OPERATOR_JOB1_READ = "server=server1->job=job1->action=read";
  private static final String OPERATOR_JOB2_READ = "server=server1->job=job2->action=read";
  private static final String ANALYST_LINKS_ALL = "server=server1->link=all->action=*";
  private static final String OPERATOR_LINK1_READ = "server=server1->link=link1->action=read";
  private static final String OPERATOR_LINK2_READ = "server=server1->link=link2->action=read";
  private static final String ADMIN = "server=server1->action=*";

  private PolicyEngine policy;
  private static File baseDir;

  protected String sqoopServerName = "server1";

  @BeforeClass
  public static void setupClazz() throws IOException {
    baseDir = Files.createTempDir();
  }

  @AfterClass
  public static void teardownClazz() throws IOException {
    if(baseDir != null) {
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
  protected void afterSetup() throws IOException {

  }

  protected void beforeTeardown() throws IOException {

  }

  @Test
  public void testDeveloper() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        OPERATOR_JDBC_CONNECTORS_READ, OPERATOR_HDFS_CONNECTORS_READ,
        OPERATOR_KAFKA_CONNECTORS_READ, OPERATOR_KITE_CONNECTORS_READ,
        ANALYST_JOBS_ALL, ANALYST_LINKS_ALL));
    Assert.assertEquals(expected.toString(),
        Sets.newTreeSet(policy.getPrivileges(set("developer"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(ANALYST_JOBS_ALL, ANALYST_LINKS_ALL));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("analyst"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testConnectorOperator() throws Exception {

  }

  @Test
  public void testJobOperator() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets
        .newHashSet(OPERATOR_JOB1_READ,OPERATOR_JOB2_READ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("job1_2_operator"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testLinkOperator() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets
        .newHashSet(OPERATOR_LINK1_READ, OPERATOR_LINK2_READ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("link1_2_operator"), ActiveRoleSet.ALL))
        .toString());
  }

  @Test
  public void testAdmin() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(ADMIN));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policy.getPrivileges(set("admin"), ActiveRoleSet.ALL))
        .toString());
  }

  private static Set<String> set(String... values) {
    return Sets.newHashSet(values);
  }
}
