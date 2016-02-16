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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.KV_JOINER;
import static org.apache.sentry.provider.common.ProviderConstants.KV_SEPARATOR;

import org.apache.sentry.core.model.kafka.KafkaActionConstant;
import org.apache.sentry.policy.common.Privilege;
import org.apache.sentry.policy.kafka.KafkaWildcardPrivilege;
import org.apache.sentry.provider.common.KeyValue;
import org.junit.Test;

public class TestKafkaWildcardPrivilege {
  private static final Privilege KAFKA_HOST1_ALL =
      create(new KeyValue("HOST", "host1"), new KeyValue("action", KafkaActionConstant.ALL));
  private static final Privilege KAFKA_HOST1_READ =
      create(new KeyValue("HOST", "host1"), new KeyValue("action", KafkaActionConstant.READ));
  private static final Privilege KAFKA_HOST1_WRITE =
      create(new KeyValue("HOST", "host1"), new KeyValue("action", KafkaActionConstant.WRITE));

  private static final Privilege KAFKA_HOST1_TOPIC1_ALL =
      create(new KeyValue("HOST", "host1"), new KeyValue("TOPIC", "topic1"), new KeyValue("action", KafkaActionConstant.ALL));
  private static final Privilege KAFKA_HOST1_TOPIC1_READ =
      create(new KeyValue("HOST", "host1"), new KeyValue("TOPIC", "topic1"), new KeyValue("action", KafkaActionConstant.READ));
  private static final Privilege KAFKA_HOST1_TOPIC1_WRITE =
      create(new KeyValue("HOST", "host1"), new KeyValue("TOPIC", "topic1"), new KeyValue("action", KafkaActionConstant.WRITE));

  private static final Privilege KAFKA_HOST1_CLUSTER1_ALL =
      create(new KeyValue("HOST", "host1"), new KeyValue("CLUSTER", "cluster1"), new KeyValue("action", KafkaActionConstant.ALL));
  private static final Privilege KAFKA_HOST1_CLUSTER1_READ =
      create(new KeyValue("HOST", "host1"), new KeyValue("CLUSTER", "cluster1"), new KeyValue("action", KafkaActionConstant.READ));
  private static final Privilege KAFKA_HOST1_CLUSTER1_WRITE =
      create(new KeyValue("HOST", "host1"), new KeyValue("CLUSTER", "cluster1"), new KeyValue("action", KafkaActionConstant.WRITE));

  private static final Privilege KAFKA_HOST1_GROUP1_ALL =
      create(new KeyValue("HOST", "host1"), new KeyValue("GROUP", "cgroup1"), new KeyValue("action", KafkaActionConstant.ALL));
  private static final Privilege KAFKA_HOST1_GROUP1_READ =
      create(new KeyValue("HOST", "host1"), new KeyValue("GROUP", "cgroup1"), new KeyValue("action", KafkaActionConstant.READ));
  private static final Privilege KAFKA_HOST1_GROUP1_WRITE =
      create(new KeyValue("HOST", "host1"), new KeyValue("GROUP", "cgroup1"), new KeyValue("action", KafkaActionConstant.WRITE));


  private static final Privilege KAFKA_CLUSTER1_HOST1_ALL =
      create(new KeyValue("CLUSTER", "cluster1"), new KeyValue("HOST", "host1"), new KeyValue("action", KafkaActionConstant.ALL));


  @Test
  public void testSimpleAction() throws Exception {
    //host
    assertFalse(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_READ));
    assertFalse(KAFKA_HOST1_READ.implies(KAFKA_HOST1_WRITE));
    //consumer group
    assertFalse(KAFKA_HOST1_GROUP1_WRITE.implies(KAFKA_HOST1_GROUP1_READ));
    assertFalse(KAFKA_HOST1_GROUP1_READ.implies(KAFKA_HOST1_GROUP1_WRITE));
    //topic
    assertFalse(KAFKA_HOST1_TOPIC1_READ.implies(KAFKA_HOST1_TOPIC1_WRITE));
    assertFalse(KAFKA_HOST1_TOPIC1_WRITE.implies(KAFKA_HOST1_TOPIC1_READ));
    //cluster
    assertFalse(KAFKA_HOST1_CLUSTER1_READ.implies(KAFKA_HOST1_CLUSTER1_WRITE));
    assertFalse(KAFKA_HOST1_CLUSTER1_WRITE.implies(KAFKA_HOST1_CLUSTER1_READ));
  }

  @Test
  public void testShorterThanRequest() throws Exception {
    //topic
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_TOPIC1_ALL));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_TOPIC1_READ));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_TOPIC1_WRITE));

    assertFalse(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_READ));
    assertTrue(KAFKA_HOST1_READ.implies(KAFKA_HOST1_TOPIC1_READ));
    assertTrue(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_TOPIC1_WRITE));

    //cluster
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_CLUSTER1_ALL));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_CLUSTER1_READ));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_CLUSTER1_WRITE));

    assertTrue(KAFKA_HOST1_READ.implies(KAFKA_HOST1_CLUSTER1_READ));
    assertTrue(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_CLUSTER1_WRITE));

    //consumer group
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_GROUP1_ALL));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_GROUP1_READ));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_GROUP1_WRITE));

    assertTrue(KAFKA_HOST1_READ.implies(KAFKA_HOST1_GROUP1_READ));
    assertTrue(KAFKA_HOST1_WRITE.implies(KAFKA_HOST1_GROUP1_WRITE));
  }

  @Test
  public void testActionAll() throws Exception {
    //host
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_READ));
    assertTrue(KAFKA_HOST1_ALL.implies(KAFKA_HOST1_WRITE));

    //topic
    assertTrue(KAFKA_HOST1_TOPIC1_ALL.implies(KAFKA_HOST1_TOPIC1_READ));
    assertTrue(KAFKA_HOST1_TOPIC1_ALL.implies(KAFKA_HOST1_TOPIC1_WRITE));

    //cluster
    assertTrue(KAFKA_HOST1_CLUSTER1_ALL.implies(KAFKA_HOST1_CLUSTER1_READ));
    assertTrue(KAFKA_HOST1_CLUSTER1_ALL.implies(KAFKA_HOST1_CLUSTER1_WRITE));

    //consumer group
    assertTrue(KAFKA_HOST1_GROUP1_ALL.implies(KAFKA_HOST1_GROUP1_READ));
    assertTrue(KAFKA_HOST1_GROUP1_ALL.implies(KAFKA_HOST1_GROUP1_WRITE));
  }

  @Test
  public void testUnexpected() throws Exception {
    Privilege p = new Privilege() {
      @Override
      public boolean implies(Privilege p) {
        return false;
      }
    };
    Privilege topic1 = create(new KeyValue("HOST", "host"), new KeyValue("TOPIC", "topic1"));
    assertFalse(topic1.implies(null));
    assertFalse(topic1.implies(p));
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
    System.out.println(create(KV_JOINER.join("", "host1")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyValue() throws Exception {
    System.out.println(create(KV_JOINER.join("HOST", "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testEmptyPart() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_JOINER.join("HOST", "host1"), "")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testOnlySeperators() throws Exception {
    System.out.println(create(AUTHORIZABLE_JOINER.
        join(KV_SEPARATOR, KV_SEPARATOR, KV_SEPARATOR)));
  }

  static KafkaWildcardPrivilege create(KeyValue... keyValues) {
    return create(AUTHORIZABLE_JOINER.join(keyValues));

  }
  static KafkaWildcardPrivilege create(String s) {
    return new KafkaWildcardPrivilege(s);
  }
}
