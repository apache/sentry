/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.kafka.authorizer;

import junit.framework.Assert;
import kafka.security.auth.Resource;
import kafka.security.auth.Resource$;
import kafka.security.auth.ResourceType$;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.model.kafka.KafkaAuthorizable;
import org.apache.sentry.kafka.ConvertUtil;
import org.junit.Test;

import java.util.List;

public class ConvertUtilTest {

  @Test
  public void testCluster() {
    String hostname = "localhost";
    String clusterName = Resource$.MODULE$.ClusterResourceName();
    Resource clusterResource = new Resource(ResourceType$.MODULE$.fromString("cluster"), clusterName);
    List<Authorizable> authorizables = ConvertUtil.convertResourceToAuthorizable(hostname, clusterResource);
    for (Authorizable auth : authorizables) {
      if (auth.getTypeName().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.CLUSTER.name())) {
        Assert.assertEquals(auth.getName(), clusterName);
      } else if (auth.getTypeName().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.HOST.name())) {
        Assert.assertEquals(auth.getName(), hostname);
      } else {
        Assert.fail("Unexpected type found: " + auth.getTypeName());
      }
    }
    Assert.assertEquals(authorizables.size(), 2);
  }

  @Test
  public void testTopic() {
    String hostname = "localhost";
    String topicName = "t1";
    Resource topicResource = new Resource(ResourceType$.MODULE$.fromString("topic"), topicName);
    List<Authorizable> authorizables = ConvertUtil.convertResourceToAuthorizable(hostname, topicResource);
    for (Authorizable auth : authorizables) {
      if (auth.getTypeName().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.TOPIC.name())) {
        Assert.assertEquals(auth.getName(), topicName);
      } else if (auth.getTypeName().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.HOST.name())) {
        Assert.assertEquals(auth.getName(), hostname);
      } else {
        Assert.fail("Unexpected type found: " + auth.getTypeName());
      }
    }
    Assert.assertEquals(authorizables.size(), 2);
  }

  @Test
  public void testConsumerGroup() {
    String hostname = "localhost";
    String consumerGroup = "g1";
    Resource consumerGroupResource = new Resource(ResourceType$.MODULE$.fromString("group"), consumerGroup);
    List<Authorizable> authorizables = ConvertUtil.convertResourceToAuthorizable(hostname, consumerGroupResource);
    for (Authorizable auth : authorizables) {
      if (auth.getTypeName().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.CONSUMERGROUP.name())) {
        Assert.assertEquals(auth.getName(),consumerGroup);
      } else if (auth.getTypeName().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.HOST.name())) {
        Assert.assertEquals(auth.getName(),hostname);
      } else {
        Assert.fail("Unexpected type found: " + auth.getTypeName());
      }
    }
    Assert.assertEquals(authorizables.size(), 2);
  }

  @Test
  public void testTransactionalId() {
    String hostname = "localhost";
    String transactionalId = "t1";
    Resource transactionalIdResource = new Resource(ResourceType$.MODULE$.fromString("transactionalId"), transactionalId);
    List<Authorizable> authorizables = ConvertUtil.convertResourceToAuthorizable(hostname, transactionalIdResource);
    for (Authorizable auth : authorizables) {
      if (auth.getTypeName().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.TRANSACTIONALID.name())) {
        Assert.assertEquals(auth.getName(), transactionalId);
      } else if (auth.getTypeName().equalsIgnoreCase(KafkaAuthorizable.AuthorizableType.HOST.name())) {
        Assert.assertEquals(auth.getName(), hostname);
      } else {
        Assert.fail("Unexpected type found: " + auth.getTypeName());
      }
    }
    Assert.assertEquals(authorizables.size(), 2);
  }
}
