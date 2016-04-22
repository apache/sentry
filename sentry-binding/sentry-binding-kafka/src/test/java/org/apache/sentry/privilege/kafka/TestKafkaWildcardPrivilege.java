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
package org.apache.sentry.privilege.kafka;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.kafka.KafkaActionConstant;
import org.apache.sentry.core.model.kafka.KafkaPrivilegeModel;
import org.apache.sentry.policy.common.CommonPrivilege;
import org.apache.sentry.policy.common.Privilege;
import org.junit.Before;
import org.junit.Test;

public class TestKafkaWildcardPrivilege {

  private Model kafkaPrivilegeModel;

  private static final CommonPrivilege KAFKA_HOST1_ALL =
      create(new KeyValue("HOST", "host1"), new KeyValue("action", KafkaActionConstant.ALL));
  private static final CommonPrivilege KAFKA_HOST1_READ =
      create(new KeyValue("HOST", "host1"), new KeyValue("action", KafkaActionConstant.READ));
  private static final CommonPrivilege KAFKA_HOST1_WRITE =
      create(new KeyValue("HOST", "host1"), new KeyValue("action", KafkaActionConstant.WRITE));

  private static final CommonPrivilege KAFKA_HOST1_TOPIC1_ALL =
      create(new KeyValue("HOST", "host1"), new KeyValue("TOPIC", "topic1"), new KeyValue("action", KafkaActionConstant.ALL));
  private static final CommonPrivilege KAFKA_HOST1_TOPIC1_READ =
      create(new KeyValue("HOST", "host1"), new KeyValue("TOPIC", "topic1"), new KeyValue("action", KafkaActionConstant.READ));
  private static final CommonPrivilege KAFKA_HOST1_TOPIC1_WRITE =
      create(new KeyValue("HOST", "host1"), new KeyValue("TOPIC", "topic1"), new KeyValue("action", KafkaActionConstant.WRITE));

  private static final CommonPrivilege KAFKA_HOST1_CLUSTER1_ALL =
      create(new KeyValue("HOST", "host1"), new KeyValue("CLUSTER", "cluster1"), new KeyValue("action", KafkaActionConstant.ALL));
  private static final CommonPrivilege KAFKA_HOST1_CLUSTER1_READ =
      create(new KeyValue("HOST", "host1"), new KeyValue("CLUSTER", "cluster1"), new KeyValue("action", KafkaActionConstant.READ));
  private static final CommonPrivilege KAFKA_HOST1_CLUSTER1_WRITE =
      create(new KeyValue("HOST", "host1"), new KeyValue("CLUSTER", "cluster1"), new KeyValue("action", KafkaActionConstant.WRITE));

  private static final CommonPrivilege KAFKA_HOST1_GROUP1_ALL =
      create(new KeyValue("HOST", "host1"), new KeyValue("GROUP", "cgroup1"), new KeyValue("action", KafkaActionConstant.ALL));
  private static final CommonPrivilege KAFKA_HOST1_GROUP1_READ =
      create(new KeyValue("HOST", "host1"), new KeyValue("GROUP", "cgroup1"), new KeyValue("action", KafkaActionConstant.READ));
  private static final CommonPrivilege KAFKA_HOST1_GROUP1_WRITE =
      create(new KeyValue("HOST", "host1"), new KeyValue("GROUP", "cgroup1"), new KeyValue("action", KafkaActionConstant.WRITE));

  @Before
  public void prepareData() {
    kafkaPrivilegeModel = KafkaPrivilegeModel.getInstance();
  }

  @Test
  public void testSimpleAction() throws Exception {
    //host
    assertFalse(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_READ, kafkaPrivilegeModel));
    assertFalse(KAFKA_HOST1_READ.implies(KAFKA_HOST1_WRITE, kafkaPrivilegeModel));
    //consumer group
    assertFalse(KAFKA_HOST1_GROUP1_WRITE.implies(KAFKA_HOST1_GROUP1_READ, kafkaPrivilegeModel));
    assertFalse(KAFKA_HOST1_GROUP1_READ.implies(KAFKA_HOST1_GROUP1_WRITE, kafkaPrivilegeModel));
    //topic
    assertFalse(KAFKA_HOST1_TOPIC1_READ.implies(KAFKA_HOST1_TOPIC1_WRITE, kafkaPrivilegeModel));
    assertFalse(KAFKA_HOST1_TOPIC1_WRITE.implies(KAFKA_HOST1_TOPIC1_READ, kafkaPrivilegeModel));
    //cluster
    assertFalse(KAFKA_HOST1_CLUSTER1_READ.implies(KAFKA_HOST1_CLUSTER1_WRITE, kafkaPrivilegeModel));
    assertFalse(KAFKA_HOST1_CLUSTER1_WRITE.implies(KAFKA_HOST1_CLUSTER1_READ, kafkaPrivilegeModel));
  }

  @Test
  public void testShorterThanRequest() throws Exception {
    //topic
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_TOPIC1_ALL, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_TOPIC1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_TOPIC1_WRITE, kafkaPrivilegeModel));

    assertFalse(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_READ.implies(KAFKA_HOST1_TOPIC1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_TOPIC1_WRITE, kafkaPrivilegeModel));

    //cluster
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_CLUSTER1_ALL, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_CLUSTER1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_CLUSTER1_WRITE, kafkaPrivilegeModel));

    assertTrue(KAFKA_HOST1_READ.implies(KAFKA_HOST1_CLUSTER1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_CLUSTER1_WRITE, kafkaPrivilegeModel));

    //consumer group
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_GROUP1_ALL, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_GROUP1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_GROUP1_WRITE, kafkaPrivilegeModel));

    assertTrue(KAFKA_HOST1_READ.implies(KAFKA_HOST1_GROUP1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_GROUP1_WRITE, kafkaPrivilegeModel));
  }

  @Test
  public void testActionAll() throws Exception {
    //host
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_WRITE, kafkaPrivilegeModel));

    //topic
    assertTrue(KAFKA_HOST1_TOPIC1_ALL.implies(KAFKA_HOST1_TOPIC1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_TOPIC1_ALL.implies(KAFKA_HOST1_TOPIC1_WRITE, kafkaPrivilegeModel));

    //cluster
    assertTrue(KAFKA_HOST1_CLUSTER1_ALL.implies(KAFKA_HOST1_CLUSTER1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_CLUSTER1_ALL.implies(KAFKA_HOST1_CLUSTER1_WRITE, kafkaPrivilegeModel));

    //consumer group
    assertTrue(KAFKA_HOST1_GROUP1_ALL.implies(KAFKA_HOST1_GROUP1_READ, kafkaPrivilegeModel));
    assertTrue(KAFKA_HOST1_GROUP1_ALL.implies(KAFKA_HOST1_GROUP1_WRITE, kafkaPrivilegeModel));
  }

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p, Model model) {
        return false;
      }
    };
    Privilege topic1 = create(new KeyValue("HOST", "host"), new KeyValue("TOPIC", "topic1"));
    assertFalse(topic1.implies(null, kafkaPrivilegeModel));
    assertFalse(topic1.implies(p, kafkaPrivilegeModel));
    assertFalse(topic1.equals(null));
    assertFalse(topic1.equals(p));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNullString() throws Exception {
    System.out.println(create((String)null));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyString() throws Exception {
    System.out.println(create(""));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyKey() throws Exception {
    System.out.println(create(SentryConstants.KV_JOINER.join("", "host1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(SentryConstants.KV_JOINER.join("HOST", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(SentryConstants.AUTHORIZABLE_JOINER.
        join(SentryConstants.KV_JOINER.join("HOST", "host1"), "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testOnlySeperators() throws Exception {
    System.out.println(create(SentryConstants.AUTHORIZABLE_JOINER.
        join(SentryConstants.KV_SEPARATOR, SentryConstants.KV_SEPARATOR,
        SentryConstants.KV_SEPARATOR)));
  }

  static CommonPrivilege create(KeyValue... keyValues) {
    return create(SentryConstants.AUTHORIZABLE_JOINER.join(keyValues));

  }
  static CommonPrivilege create(String s) {
    return new CommonPrivilege(s);
  }
}
