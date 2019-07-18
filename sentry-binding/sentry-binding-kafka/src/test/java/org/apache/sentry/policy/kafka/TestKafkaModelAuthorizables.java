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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;

import org.apache.sentry.core.model.kafka.Cluster;
import org.apache.sentry.core.model.kafka.ConsumerGroup;
import org.apache.sentry.core.model.kafka.Host;
import org.apache.sentry.core.model.kafka.KafkaModelAuthorizables;
import org.apache.sentry.core.model.kafka.Topic;
import org.apache.sentry.core.model.kafka.TransactionalId;
import org.apache.shiro.config.ConfigurationException;
import org.junit.Test;

public class TestKafkaModelAuthorizables {

  @Test
  public void testHost() throws Exception {
    Host host1 = (Host) KafkaModelAuthorizables.from("HOST=host1");
    assertEquals("host1", host1.getName());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNoKV() throws Exception {
    System.out.println(KafkaModelAuthorizables.from("nonsense"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyKey() throws Exception {
    System.out.println(KafkaModelAuthorizables.from("=host1"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(KafkaModelAuthorizables.from("HOST="));
  }

  @Test
  public void testNotAuthorizable() throws Exception {
    assertNull(KafkaModelAuthorizables.from("k=v"));
  }

  @Test
  public void testResourceNameIsCaseSensitive() throws Exception {
    Host host1 = (Host)KafkaModelAuthorizables.from("HOST=Host1");
    assertEquals("Host1", host1.getName());

    Cluster cluster1 = (Cluster)KafkaModelAuthorizables.from("Cluster=kafka-cluster");
    assertEquals("kafka-cluster", cluster1.getName());

    Topic topic1 = (Topic)KafkaModelAuthorizables.from("topic=topiC1");
    assertEquals("topiC1", topic1.getName());

    ConsumerGroup consumergroup1 = (ConsumerGroup)KafkaModelAuthorizables.from("ConsumerGroup=CG1");
    assertEquals("CG1", consumergroup1.getName());

    TransactionalId transactionalId1 = (TransactionalId) KafkaModelAuthorizables.from("TransactionalId=tRaNs1");
    assertEquals("tRaNs1", transactionalId1.getName());
  }

  @Test
  public void testClusterResourceNameIsRestricted() throws Exception {
    try {
      KafkaModelAuthorizables.from("Cluster=cluster1");
      fail("Cluster with name other than " + Cluster.NAME + " must not have been created.");
    } catch (ConfigurationException cex) {
      assertEquals("Exception message is not as expected.", "Kafka's cluster resource can only have name " + Cluster.NAME, cex.getMessage());
    } catch (Exception ex) {
      fail("Configuration exception was expected for invalid Cluster name.");
    }
  }
}
