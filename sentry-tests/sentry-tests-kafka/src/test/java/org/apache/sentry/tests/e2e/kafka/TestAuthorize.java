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

import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.sentry.core.model.kafka.Cluster;
import org.apache.sentry.core.model.kafka.ConsumerGroup;
import org.apache.sentry.core.model.kafka.KafkaActionConstant;
import org.apache.sentry.core.model.kafka.Host;
import org.apache.sentry.core.model.kafka.Topic;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class TestAuthorize extends AbstractKafkaSentryTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestAuthorize.class);
  private static final String TOPIC_NAME = "tOpIc1";

  @Test
  public void testProduceConsumeForSuperuser() {
    try {
      final String SuperuserName = "test";
      testProduce(SuperuserName);
      testConsume(SuperuserName);
    } catch (Exception ex) {
      Assert.fail("Superuser must have been allowed to perform any and all actions. \nException: \n" + ex);
    }
  }

  @Test
  public void testProduceConsumeCycle() throws Exception {
    final String localhost = InetAddress.getLocalHost().getHostAddress();

    // START TESTING PRODUCER
    try {
      testProduce("user1");
      Assert.fail("user1 must not have been authorized to create topic " + TOPIC_NAME + ".");
    } catch (ExecutionException ex) {
      assertCausedMessage(ex, "Not authorized to access topics: [" + TOPIC_NAME + "]");
    }

    final String role = StaticUserGroupRole.ROLE_1;
    final String group = StaticUserGroupRole.GROUP_1;

    // Allow HOST=localhost->Topic=tOpIc1->action=describe
    ArrayList<TAuthorizable> authorizables = new ArrayList<TAuthorizable>();
    Host host = new Host(localhost);
    authorizables.add(new TAuthorizable(host.getTypeName(), host.getName()));
    Topic topic = new Topic(TOPIC_NAME); // Topic name is case sensitive.
    authorizables.add(new TAuthorizable(topic.getTypeName(), topic.getName()));
    addPermissions(role, group, KafkaActionConstant.DESCRIBE, authorizables);
    try {
      testProduce("user1");
      Assert.fail("user1 must not have been authorized to create topic " + TOPIC_NAME + ".");
    } catch (ExecutionException ex) {
      assertCausedMessage(ex, "Not authorized to access topics: [" + TOPIC_NAME + "]");
    }

    // Allow HOST=localhost->Cluster=kafka-cluster->action=create
    authorizables = new ArrayList<TAuthorizable>();
    authorizables.add(new TAuthorizable(host.getTypeName(), host.getName()));
    Cluster cluster = new Cluster();
    authorizables.add(new TAuthorizable(cluster.getTypeName(), cluster.getName()));
    addPermissions(role, group, KafkaActionConstant.CREATE, authorizables);
    try {
      testProduce("user1");
      Assert.fail("user1 must not have been authorized to create topic " + TOPIC_NAME + ".");
    } catch (ExecutionException ex) {
      assertCausedMessage(ex, "Not authorized to access topics: [" + TOPIC_NAME + "]");
    }

    // Allow HOST=localhost->Topic=tOpIc1->action=write
    authorizables = new ArrayList<TAuthorizable>();
    authorizables.add(new TAuthorizable(host.getTypeName(), host.getName()));
    authorizables.add(new TAuthorizable(topic.getTypeName(), topic.getName()));
    addPermissions(role, group, KafkaActionConstant.WRITE, authorizables);
    try {
      testProduce("user1");
    } catch (Exception ex) {
      Assert.fail("user1 should have been able to successfully produce to topic " + TOPIC_NAME + ". \n Exception: " + ex);
    }

    // START TESTING CONSUMER
    try {
      testConsume("user1");
      Assert.fail("user1 must not have been authorized to describe consumer group sentrykafkaconsumer.");
    } catch (Exception ex) {
      assertCausedMessage(ex, "Not authorized to access group: sentrykafkaconsumer");
    }

    // HOST=localhost->Group=SentryKafkaConsumer->action=describe
    authorizables = new ArrayList<TAuthorizable>();
    authorizables.add(new TAuthorizable(host.getTypeName(), host.getName()));
    ConsumerGroup consumerGroup = new ConsumerGroup("sentrykafkaconsumer");
    authorizables.add(new TAuthorizable(consumerGroup.getTypeName(), consumerGroup.getName()));
    addPermissions(role, group, KafkaActionConstant.DESCRIBE, authorizables);
    try {
      testConsume("user1");
      Assert.fail("user1 must not have been authorized to read consumer group sentrykafkaconsumer.");
    } catch (Exception ex) {
      assertCausedMessage(ex, "Not authorized to access group: sentrykafkaconsumer");
    }

    // HOST=localhost->Group=SentryKafkaConsumer->action=read
    authorizables = new ArrayList<TAuthorizable>();
    authorizables.add(new TAuthorizable(host.getTypeName(), host.getName()));
    authorizables.add(new TAuthorizable(consumerGroup.getTypeName(), consumerGroup.getName()));
    addPermissions(role, group, KafkaActionConstant.READ, authorizables);
    try {
      testConsume("user1");
      Assert.fail("user1 must not have been authorized to read from topic " + TOPIC_NAME + ".");
    } catch (Exception ex) {
      assertCausedMessage(ex, "Not authorized to access topics: [" + TOPIC_NAME + "]");
    }

    // HOST=localhost->Topic=tOpIc1->action=read
    authorizables = new ArrayList<TAuthorizable>();
    authorizables.add(new TAuthorizable(host.getTypeName(), host.getName()));
    authorizables.add(new TAuthorizable(topic.getTypeName(), topic.getName()));
    addPermissions(role, group, KafkaActionConstant.READ, authorizables);
    testConsume("user1");
  }

  private void addPermissions(String role, String group, String action, ArrayList<TAuthorizable> authorizables) throws Exception {
    SentryGenericServiceClient sentryClient = getSentryClient();
    try {
      sentryClient.createRoleIfNotExist(ADMIN_USER, role, COMPONENT);
      sentryClient.addRoleToGroups(ADMIN_USER, role, COMPONENT, Sets.newHashSet(group));

      sentryClient.grantPrivilege(ADMIN_USER, role, COMPONENT,
          new TSentryPrivilege(COMPONENT, "kafka", authorizables,
              action));
    } finally {
      if (sentryClient != null) {
        sentryClient.close();
        sentryClient = null;
      }
    }
    sleepIfCachingEnabled();
  }

  private void testProduce(String producerUser) throws Exception {
    final KafkaProducer<String, String> kafkaProducer = createKafkaProducer(producerUser);
    try {
      final String msg = "message1";
      ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TOPIC_NAME, msg);
      kafkaProducer.send(producerRecord).get();
      LOGGER.debug("Sent message: " + producerRecord);
    } finally {
      kafkaProducer.close();
    }
  }

  private void testConsume(String consumerUser) throws Exception {
    final KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(consumerUser);
    try {
      final String msg = "message1";
      kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME), new CustomRebalanceListener(kafkaConsumer));
      waitTillTrue("Did not receive expected message.", 60, 2, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
          if (records.isEmpty())
            LOGGER.debug("No record received from consumer.");
          for (ConsumerRecord<String, String> record : records) {
            if (record.value().equals(msg)) {
              LOGGER.debug("Received message: " + record);
              return true;
            }
          }
          return false;
        }
      });
    } finally {
      kafkaConsumer.close();
    }
  }

  private KafkaProducer<String, String> createKafkaProducer(String user) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "SentryKafkaProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KafkaTestServer.class.getResource("/" + user + ".keystore.jks").getPath());
    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, user + "-ks-passwd");
    props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, user + "-key-passwd");
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, KafkaTestServer.class.getResource("/" + user + ".truststore.jks").getPath());
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, user + "-ts-passwd");

    return new KafkaProducer<String, String>(props);
  }

  private KafkaConsumer<String, String> createKafkaConsumer(String user) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sentrykafkaconsumer");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KafkaTestServer.class.getResource("/" + user + ".keystore.jks").getPath());
    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, user + "-ks-passwd");
    props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, user + "-key-passwd");
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, KafkaTestServer.class.getResource("/" + user + ".truststore.jks").getPath());
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, user + "-ts-passwd");

    return new KafkaConsumer<String, String>(props);
  }

  /**
   * Wait for a condition to succeed up to specified time.
   *
   * @param failureMessage Message to be displayed on failure.
   * @param maxWaitTime    Max waiting time for success in seconds.
   * @param loopInterval   Wait time between checks in seconds.
   * @param testFunc       Check to be performed for success, should return boolean.
   * @throws Exception
   */
  private void waitTillTrue(
      String failureMessage, long maxWaitTime, long loopInterval, Callable<Boolean> testFunc)
      throws Exception {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime <= maxWaitTime * 1000L) {
      if (testFunc.call()) {
        return; // Success
      }
      Thread.sleep(loopInterval * 1000L);
    }

    Assert.fail(failureMessage);
  }

  class CustomRebalanceListener implements ConsumerRebalanceListener {

    KafkaConsumer consumer = null;

    CustomRebalanceListener(KafkaConsumer kafkaConsumer) {
      consumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
      for (TopicPartition tp : collection) {
        consumer.seekToBeginning(tp);
      }
    }
  }
}