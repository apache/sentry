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

import kafka.network.RequestChannel;
import kafka.security.auth.Operation$;
import kafka.security.auth.Resource;
import kafka.security.auth.Resource$;
import kafka.security.auth.ResourceType$;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.sentry.kafka.conf.KafkaAuthConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class SentryKafkaAuthorizerTest {

  private SentryKafkaAuthorizer authorizer;
  private InetAddress testHostName1;
  private InetAddress testHostName2;
  private String resourceName;
  private Resource clusterResource;
  private Resource topic1Resource;
  private Resource transactionalIdResource;
  private KafkaConfig config;

  public SentryKafkaAuthorizerTest() throws UnknownHostException {
    authorizer = new SentryKafkaAuthorizer();
    testHostName1 = InetAddress.getByAddress("host1", new byte[] {1, 2, 3, 4});
    testHostName2 = InetAddress.getByAddress("host2", new byte[] {2, 3, 4, 5});
    resourceName = Resource$.MODULE$.ClusterResourceName();
    clusterResource = new Resource(ResourceType$.MODULE$.fromString("cluster"), resourceName);
    topic1Resource = new Resource(ResourceType$.MODULE$.fromString("topic"), "t1");
    transactionalIdResource = new Resource(ResourceType$.MODULE$.fromString("transactionalId"), "tid1");
  }

  @Before
  public void  setUp() {
    Properties props = new Properties();
    String sentry_site_path = SentryKafkaAuthorizerTest.class.getClassLoader().getResource(KafkaAuthConf.AUTHZ_SITE_FILE).getPath();
    // Kafka check this prop when creating a config instance
    props.put("zookeeper.connect", "test");
    props.put("sentry.kafka.site.url", "file://" + sentry_site_path);

    config = KafkaConfig.fromProps(props);
    authorizer.configure(config.originals());
  }

  @Test
  public void testAdmin() {

    KafkaPrincipal admin = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin_group");
    RequestChannel.Session host1Session = new RequestChannel.Session(admin, testHostName1);
    RequestChannel.Session host2Session = new RequestChannel.Session(admin, testHostName2);

    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Create"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Describe"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("ClusterAction"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Read"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Write"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Create"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Delete"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Alter"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Describe"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("ClusterAction"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("IdempotentWrite"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("AlterConfigs"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("DescribeConfigs"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Write"), transactionalIdResource));


    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Create"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Describe"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("ClusterAction"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Read"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Write"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Create"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Delete"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Alter"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Describe"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("ClusterAction"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("IdempotentWrite"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("AlterConfigs"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("DescribeConfigs"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Write"), transactionalIdResource));
  }

  @Test
  public void testSubAdmin() {
    KafkaPrincipal admin = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "subadmin_group2");
    RequestChannel.Session host1Session = new RequestChannel.Session(admin, testHostName1);
    RequestChannel.Session host2Session = new RequestChannel.Session(admin, testHostName2);

    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Create"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Describe"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("ClusterAction"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Read"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Write"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Create"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Delete"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Alter"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Describe"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("ClusterAction"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("IdempotentWrite"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("AlterConfigs"), topic1Resource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("DescribeConfigs"), clusterResource));
    Assert.assertTrue("Test failed.", authorizer.authorize(host1Session, Operation$.MODULE$.fromString("Write"), transactionalIdResource));

    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Create"), clusterResource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Describe"), clusterResource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("ClusterAction"), clusterResource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Read"), topic1Resource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Write"), topic1Resource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Create"), topic1Resource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Delete"), topic1Resource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Alter"), topic1Resource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Describe"), topic1Resource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("ClusterAction"), topic1Resource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("IdempotentWrite"), clusterResource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("AlterConfigs"), topic1Resource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("DescribeConfigs"), clusterResource));
    Assert.assertFalse("Test failed.", authorizer.authorize(host2Session, Operation$.MODULE$.fromString("Write"), transactionalIdResource));
  }
}
