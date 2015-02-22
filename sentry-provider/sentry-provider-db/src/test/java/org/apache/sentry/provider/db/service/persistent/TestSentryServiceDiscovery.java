/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.persistent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;

public class TestSentryServiceDiscovery {

  private HAContext haContext;
  private TestingServer server;

  @Before
  public void setup() throws Exception {
    server = new TestingServer();
    // HA conf
    Configuration conf = new Configuration(false);
    conf.set(ServerConfig.SENTRY_HA_ENABLED, "true");
    conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE, "sentry-test");
    conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM, server.getConnectString());
    haContext = HAContext.getHAContext(conf);
  }

  @After
  public void teardown() {
    HAContext.clearServerContext();
    if (server != null) {
      try {
        server.stop();
      } catch (IOException e) {
      }
    }
  }

  @Test
  public void testRegisterOneService() throws Exception {
    final String hostname = "localhost1";
    final Integer port = 123;
    ServiceRegister register = new ServiceRegister(haContext);
    register.regService(hostname, port);
    ServiceManager manager = new ServiceManager(haContext);
    ServiceInstance<Void> instance = manager.getServiceInstance();
    assertEquals("'hostname' doesn't match.", hostname, instance.getAddress());
    assertEquals("'port' doesn't match.", port, instance.getPort());
  }

  @Test
  public void testRegisterMultiService() throws Exception {

    final String hostname1 = "localhost1";
    final Integer port1 = 123;
    final String hostname2 = "localhost2";
    final Integer port2 = 456;
    final String hostname3 = "localhost3";
    final Integer port3 = 789;

    Map<String, Integer> servicesMap = new HashMap<String, Integer>();
    servicesMap.put(hostname1, port1);
    servicesMap.put(hostname2, port2);
    servicesMap.put(hostname3, port3);

    ServiceRegister register1 = new ServiceRegister(haContext);
    register1.regService(hostname1, port1);
    ServiceRegister register2 = new ServiceRegister(haContext);
    register2.regService(hostname2, port2);
    ServiceRegister register3 = new ServiceRegister(haContext);
    register3.regService(hostname3, port3);

    ServiceManager manager = new ServiceManager(haContext);
    ServiceInstance<Void> instance = manager.getServiceInstance();
    assertEquals("'instance' doesn't match.", instance.getPort(), servicesMap.get(instance.getAddress()));
    instance = manager.getServiceInstance();
    assertEquals("'instance' doesn't match.", instance.getPort(), servicesMap.get(instance.getAddress()));
    instance = manager.getServiceInstance();
    assertEquals("'instance' doesn't match.", instance.getPort(), servicesMap.get(instance.getAddress()));
  }

  @Test
  public void testReportError() throws Exception {
    final String hostname1 = "localhost1";
    final Integer port1 = 123;

    ServiceRegister register1 = new ServiceRegister(haContext);
    register1.regService(hostname1, port1);

    ServiceManager manager = new ServiceManager(haContext);
    ServiceInstance<Void> instance = manager.getServiceInstance();
    manager.reportError(instance);
    // report twice, manager will not return temporarily
    instance = manager.getServiceInstance();
    manager.reportError(instance);
    instance = manager.getServiceInstance();
    assertEquals("'instance' should be null.", null, instance);
  }

}
