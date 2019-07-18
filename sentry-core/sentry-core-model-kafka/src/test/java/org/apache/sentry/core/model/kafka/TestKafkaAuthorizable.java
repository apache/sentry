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

package org.apache.sentry.core.model.kafka;

import junit.framework.Assert;

import org.apache.sentry.core.model.kafka.KafkaAuthorizable.AuthorizableType;
import org.junit.Test;

/**
 * Test proper KafkaAuthorizable is created for various Kafka resources.
 */
public class TestKafkaAuthorizable {

  @Test
  public void testName() throws Exception {
    String name = "simple";
    Host host = new Host(name);
    Assert.assertEquals(host.getName(), name);

    Cluster cluster = new Cluster();
    Assert.assertEquals(cluster.getName(), Cluster.NAME);

    Topic topic = new Topic(name);
    Assert.assertEquals(topic.getName(), name);

    ConsumerGroup consumerGroup = new ConsumerGroup(name);
    Assert.assertEquals(consumerGroup.getName(), name);

    TransactionalId transactionalId = new TransactionalId(name);
    Assert.assertEquals(transactionalId.getName(), name);
  }

  @Test
  public void testAuthType() throws Exception {
    Host host = new Host("host1");
    Assert.assertEquals(host.getAuthzType(), AuthorizableType.HOST);

    Cluster cluster = new Cluster();
    Assert.assertEquals(cluster.getAuthzType(), AuthorizableType.CLUSTER);

    Topic topic = new Topic("topic1");
    Assert.assertEquals(topic.getAuthzType(), AuthorizableType.TOPIC);

    ConsumerGroup consumerGroup = new ConsumerGroup("consumerGroup1");
    Assert.assertEquals(consumerGroup.getAuthzType(), AuthorizableType.CONSUMERGROUP);

    TransactionalId transactionalId = new TransactionalId("transactionalId1");
    Assert.assertEquals(transactionalId.getAuthzType(), AuthorizableType.TRANSACTIONALID);
  }
}
