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
package org.apache.sentry.tests.e2e.kafka;

import kafka.security.auth.Acl;
import kafka.security.auth.Allow$;
import kafka.security.auth.Operation$;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType$;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.sentry.kafka.authorizer.SentryKafkaAuthorizer;
import org.apache.sentry.kafka.conf.KafkaAuthConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.immutable.Map;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class TestAclsCrud extends AbstractKafkaSentryTestBase {
  private SentryKafkaAuthorizer sentryKafkaAuthorizer;

  @After
  public void cleanUp() throws Exception {
    sentryKafkaAuthorizer.dropAllRoles();
    if (sentryKafkaAuthorizer != null) {
      sentryKafkaAuthorizer.close();
      sentryKafkaAuthorizer = null;
    }
  }


  @Test
  public void testAddAclsForNonExistentRole() {
    sentryKafkaAuthorizer = new SentryKafkaAuthorizer();
    java.util.Map<String, String> configs = new HashMap<>();
    configs.put(KafkaAuthConf.SENTRY_KAFKA_SITE_URL, "file://" + sentrySitePath.getAbsolutePath());
    sentryKafkaAuthorizer.configure(configs);

    final String role1 = "role1";
    Set<Acl> acls = new HashSet<>();
    final Acl acl = new Acl(new KafkaPrincipal("role", role1),
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("READ"));
    acls.add(acl);
    scala.collection.immutable.Set<Acl> aclsScala = scala.collection.JavaConversions.asScalaSet(acls).toSet();
    Resource resource = new Resource(ResourceType$.MODULE$.fromString("TOPIC"), "test-topic");
    try {
      sentryKafkaAuthorizer.addAcls(aclsScala, resource);
    } catch (Exception ex) {
      assertCausedMessage(ex, "Can not add Acl for non-existent Role: role1");
    }
  }

  @Test
  public void testAddRole() {
    sentryKafkaAuthorizer = new SentryKafkaAuthorizer();
    java.util.Map<String, String> configs = new HashMap<>();
    configs.put(KafkaAuthConf.SENTRY_KAFKA_SITE_URL, "file://" + sentrySitePath.getAbsolutePath());
    sentryKafkaAuthorizer.configure(configs);

    final String role1 = "role1";
    try {
      sentryKafkaAuthorizer.addRole(role1);
    } catch (Exception ex) {
      Assert.fail("Failed to create role.");
    }
  }

  @Test
  public void testAddExistingRole() {
    sentryKafkaAuthorizer = new SentryKafkaAuthorizer();
    java.util.Map<String, String> configs = new HashMap<>();
    configs.put(KafkaAuthConf.SENTRY_KAFKA_SITE_URL, "file://" + sentrySitePath.getAbsolutePath());
    sentryKafkaAuthorizer.configure(configs);

    // Add role the first time
    final String role1 = "role1";
    try {
      sentryKafkaAuthorizer.addRole(role1);
    } catch (Exception ex) {
      Assert.fail("Failed to create role.");
    }

    // Try adding same role again
    try {
      sentryKafkaAuthorizer.addRole(role1);
    } catch (Exception ex) {
      assertCausedMessage(ex, "Can not create an existing role, role1, again.");
    }
  }

  @Test
  public void testAddAcls() {
    sentryKafkaAuthorizer = new SentryKafkaAuthorizer();
    java.util.Map<String, String> configs = new HashMap<>();
    configs.put(KafkaAuthConf.SENTRY_KAFKA_SITE_URL, "file://" + sentrySitePath.getAbsolutePath());
    sentryKafkaAuthorizer.configure(configs);

    final String role1 = "role1";
    Set<Acl> acls = new HashSet<>();
    Acl acl = new Acl(new KafkaPrincipal("role", role1),
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("READ"));
    acls.add(acl);
    acl = new Acl(new KafkaPrincipal("role", role1),
            Allow$.MODULE$,
            "127.0.0.1",
            Operation$.MODULE$.fromString("WRITE"));
    acls.add(acl);
    scala.collection.immutable.Set<Acl> aclsScala = scala.collection.JavaConversions.asScalaSet(acls).toSet();
    Resource resource = new Resource(ResourceType$.MODULE$.fromString("TOPIC"), "test-Topic");

    // Add role
    try {
      sentryKafkaAuthorizer.addRole(role1);
    } catch (Exception ex) {
      Assert.fail("Failed to create role.");
    }

    // Add acl
    try {
      sentryKafkaAuthorizer.addAcls(aclsScala, resource);
    } catch (Exception ex) {
      Assert.fail("Failed to add acl.");
    }

    final scala.collection.immutable.Set<Acl> obtainedAcls = sentryKafkaAuthorizer.getAcls(resource);
    Assert.assertTrue("Obtained acls did not match expected Acls", obtainedAcls.contains(acl));
  }

  @Test
  public void testAddRoleToGroups() {
    sentryKafkaAuthorizer = new SentryKafkaAuthorizer();
    java.util.Map<String, String> configs = new HashMap<>();
    configs.put(KafkaAuthConf.SENTRY_KAFKA_SITE_URL, "file://" + sentrySitePath.getAbsolutePath());
    sentryKafkaAuthorizer.configure(configs);

    final String role1 = "role1";
    Set<Acl> acls = new HashSet<>();
    final Acl acl = new Acl(new KafkaPrincipal("role", role1),
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("READ"));
    acls.add(acl);
    scala.collection.immutable.Set<Acl> aclsScala = scala.collection.JavaConversions.asScalaSet(acls).toSet();
    Resource resource = new Resource(ResourceType$.MODULE$.fromString("TOPIC"), "test-topic");

    // Add role
    try {
      sentryKafkaAuthorizer.addRole(role1);
    } catch (Exception ex) {
      Assert.fail("Failed to create role.");
    }

    // Add acl
    try {
      sentryKafkaAuthorizer.addAcls(aclsScala, resource);
    } catch (Exception ex) {
      Assert.fail("Failed to add acl.");
    }

    // Add role to group
    Set<String> groups = new HashSet<>();
    String group1 = "group1";
    groups.add(group1);
    try {
      sentryKafkaAuthorizer.addRoleToGroups(role1, groups);
    } catch (Exception ex) {
      throw ex;
    }

    final scala.collection.immutable.Set<Acl> obtainedAcls = sentryKafkaAuthorizer.getAcls(new KafkaPrincipal("group", group1)).get(resource).get();
    Assert.assertTrue("Obtained acls did not match expected Acls", obtainedAcls.contains(acl));
  }

  @Test
  public void testRemoveAclsByResource() {
    sentryKafkaAuthorizer = new SentryKafkaAuthorizer();
    java.util.Map<String, String> configs = new HashMap<>();
    configs.put(KafkaAuthConf.SENTRY_KAFKA_SITE_URL, "file://" + sentrySitePath.getAbsolutePath());
    sentryKafkaAuthorizer.configure(configs);

    final String role1 = "role1";
    Set<Acl> acls = new HashSet<>();
    final KafkaPrincipal principal1 = new KafkaPrincipal("role", role1);
    final Acl acl = new Acl(principal1,
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("READ"));
    acls.add(acl);
    scala.collection.immutable.Set<Acl> aclsScala = scala.collection.JavaConversions.asScalaSet(acls).toSet();
    Resource resource = new Resource(ResourceType$.MODULE$.fromString("TOPIC"), "test-topic");

    // Add role
    try {
      sentryKafkaAuthorizer.addRole(role1);
    } catch (Exception ex) {
      Assert.fail("Failed to create role.");
    }

    // Add acl
    try {
      sentryKafkaAuthorizer.addAcls(aclsScala, resource);
    } catch (Exception ex) {
      Assert.fail("Failed to add acl.");
    }

    // Add acl for different resource
    Set<Acl> acls2 = new HashSet<>();
    final Acl acl2 = new Acl(principal1,
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("WRITE"));
    acls2.add(acl2);
    scala.collection.immutable.Set<Acl> aclsScala2 = scala.collection.JavaConversions.asScalaSet(acls2).toSet();
    Resource resource2 = new Resource(ResourceType$.MODULE$.fromString("CLUSTER"), "test-cluster");
    try {
      sentryKafkaAuthorizer.addAcls(aclsScala2, resource2);
    } catch (Exception ex) {
      Assert.fail("Failed to add second acl.");
    }

    try {
      sentryKafkaAuthorizer.removeAcls(resource);
    } catch (Exception ex) {
      Assert.fail("Failed to remove acls for resource.");
    }

    final Map<Resource, scala.collection.immutable.Set<Acl>> obtainedAcls = sentryKafkaAuthorizer.getAcls(principal1);
    Assert.assertTrue("Obtained acls must not contain acl for removed resource's acls.", !obtainedAcls.keySet().contains(resource));
    Assert.assertTrue("Obtained acls must contain acl for resource2.", obtainedAcls.keySet().contains(resource2));
    Assert.assertTrue("Obtained acl does not match expected acl.", obtainedAcls.get(resource2).get().contains(acl2));
  }

  @Test
  public void testRemoveAclsByAclsAndResource() {
    sentryKafkaAuthorizer = new SentryKafkaAuthorizer();
    java.util.Map<String, String> configs = new HashMap<>();
    configs.put(KafkaAuthConf.SENTRY_KAFKA_SITE_URL, "file://" + sentrySitePath.getAbsolutePath());
    sentryKafkaAuthorizer.configure(configs);

    final String role1 = "role1";
    Set<Acl> acls = new HashSet<>();
    final KafkaPrincipal principal1 = new KafkaPrincipal("role", role1);
    final Acl acl = new Acl(principal1,
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("READ"));
    acls.add(acl);
    scala.collection.immutable.Set<Acl> aclsScala = scala.collection.JavaConversions.asScalaSet(acls).toSet();
    Resource resource = new Resource(ResourceType$.MODULE$.fromString("TOPIC"), "test-topic");

    // Add role
    try {
      sentryKafkaAuthorizer.addRole(role1);
    } catch (Exception ex) {
      Assert.fail("Failed to create role.");
    }

    // Add acl
    try {
      sentryKafkaAuthorizer.addAcls(aclsScala, resource);
    } catch (Exception ex) {
      Assert.fail("Failed to add acl.");
    }

    // Add another acl to same resource
    Set<Acl> acls01 = new HashSet<>();
    final Acl acl01 = new Acl(principal1,
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("DESCRIBE"));
    acls01.add(acl01);
    scala.collection.immutable.Set<Acl> aclsScala01 = scala.collection.JavaConversions.asScalaSet(acls01).toSet();
    try {
      sentryKafkaAuthorizer.addAcls(aclsScala01, resource);
    } catch (Exception ex) {
      Assert.fail("Failed to add acl.");
    }


    // Add acl for different resource
    Set<Acl> acls2 = new HashSet<>();
    final Acl acl2 = new Acl(principal1,
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("WRITE"));
    acls2.add(acl2);
    scala.collection.immutable.Set<Acl> aclsScala2 = scala.collection.JavaConversions.asScalaSet(acls2).toSet();
    Resource resource2 = new Resource(ResourceType$.MODULE$.fromString("CLUSTER"), "test-cluster");
    try {
      sentryKafkaAuthorizer.addAcls(aclsScala2, resource2);
    } catch (Exception ex) {
      Assert.fail("Failed to add second acl.");
    }

    // Remove acls
    try {
      sentryKafkaAuthorizer.removeAcls(aclsScala, resource);
    } catch (Exception ex) {
      Assert.fail("Failed to remove acls for resource.");
    }

    final Map<Resource, scala.collection.immutable.Set<Acl>> obtainedAcls = sentryKafkaAuthorizer.getAcls(principal1);
    Assert.assertTrue("Obtained acls must contain acl for resource.", obtainedAcls.keySet().contains(resource));
    Assert.assertTrue("Obtained acls must contain acl for resource2.", obtainedAcls.keySet().contains(resource2));
    Assert.assertTrue("Obtained acl must not contain removed acl for resource.", !obtainedAcls.get(resource).get().contains(acl));
    Assert.assertTrue("Obtained acl does not match expected acl for resource.", obtainedAcls.get(resource).get().contains(acl01));
    Assert.assertTrue("Obtained acl does not match expected acl for resource2.", obtainedAcls.get(resource2).get().contains(acl2));
  }

  @Test
  public void testGetAcls() {
    sentryKafkaAuthorizer = new SentryKafkaAuthorizer();
    java.util.Map<String, String> configs = new HashMap<>();
    configs.put(KafkaAuthConf.SENTRY_KAFKA_SITE_URL, "file://" + sentrySitePath.getAbsolutePath());
    sentryKafkaAuthorizer.configure(configs);

    final String role1 = "role1";
    final Resource topicResource = new Resource(ResourceType$.MODULE$.fromString("TOPIC"), "test-topic");
    final Resource consumerGroupResource = new Resource(ResourceType$.MODULE$.fromString("GROUP"), "test-consumergroup");
    final Resource transactionalIdResource = new Resource(ResourceType$.MODULE$.fromString("TRANSACTIONALID"), "test-transactionalId");
    final Resource clusterResource = new Resource(ResourceType$.MODULE$.fromString("CLUSTER"), "test-cluster");

    // Add role
    try {
      sentryKafkaAuthorizer.addRole(role1);
    } catch (Exception ex) {
      Assert.fail("Failed to create role.");
    }

    // Add acl for topic
    Set<Acl> acls = new HashSet<>();
    final KafkaPrincipal principal1 = new KafkaPrincipal("role", role1);
    final Acl acl = new Acl(principal1,
        Allow$.MODULE$,
        "127.0.0.1",
        Operation$.MODULE$.fromString("READ"));
    acls.add(acl);
    scala.collection.immutable.Set<Acl> aclsScala = scala.collection.JavaConversions.asScalaSet(acls).toSet();

    try {
      sentryKafkaAuthorizer.addAcls(aclsScala, topicResource);
      sentryKafkaAuthorizer.addAcls(aclsScala, consumerGroupResource);
      sentryKafkaAuthorizer.addAcls(aclsScala, transactionalIdResource);
      sentryKafkaAuthorizer.addAcls(aclsScala, clusterResource);
    } catch (Exception ex) {
      Assert.fail("Failed to add acls.");
    }

    final Map<Resource, scala.collection.immutable.Set<Acl>> obtainedAcls = sentryKafkaAuthorizer.getAcls(principal1);
    Assert.assertTrue("Obtained acls must contain acl for topic resource.", obtainedAcls.keySet().contains(topicResource));
    Assert.assertTrue("Obtained acls must contain acl for consumer group resource.", obtainedAcls.keySet().contains(consumerGroupResource));
    Assert.assertTrue("Obtained acls must contain acl for cluster resource.", obtainedAcls.keySet().contains(clusterResource));
    Assert.assertTrue("Obtained acls must contain acl for transactionalid resource.", obtainedAcls.keySet().contains(transactionalIdResource));
  }
}